//! Database queries for the memories system.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::diesel_models::{DbMemory, DbMemoryTrigger, NewMemory, NewMemoryTrigger};
use crate::models::{Memory, MemoryConfidence, MemoryTrigger};
use crate::schema::{memories, memory_triggers};

/// Convert a DbMemory + DbMemoryTriggers into a domain Memory.
fn to_memory(db_memory: DbMemory, db_triggers: Vec<DbMemoryTrigger>) -> Memory {
    Memory {
        id: db_memory.id,
        project_id: db_memory.project_id,
        content: db_memory.content,
        confidence: db_memory
            .confidence
            .parse()
            .unwrap_or(MemoryConfidence::Tentative),
        source_issue: db_memory.source_issue,
        created_at: db_memory.created_at as i64,
        updated_at: db_memory.updated_at as i64,
        surfaced_count: db_memory.surfaced_count,
        last_surfaced_at: db_memory.last_surfaced_at.map(|t| t as i64),
        active: db_memory.active != 0,
        triggers: db_triggers
            .into_iter()
            .map(|t| MemoryTrigger {
                id: t.id,
                memory_id: t.memory_id,
                trigger_index: t.trigger_index,
                json_path: t.json_path,
                pattern: t.pattern,
            })
            .collect(),
    }
}

/// Load all active memories + triggers for a project (+ globals where project_id IS NULL).
pub fn load_active_memories(
    conn: &mut SqliteConnection,
    project_id: Option<&str>,
) -> Result<Vec<Memory>, String> {
    // Load active memories matching the project or global
    let db_memories: Vec<DbMemory> = match project_id {
        Some(pid) => memories::table
            .filter(memories::active.eq(1))
            .filter(
                memories::project_id
                    .eq(pid)
                    .or(memories::project_id.is_null()),
            )
            .load(conn),
        None => memories::table
            .filter(memories::active.eq(1))
            .filter(memories::project_id.is_null())
            .load(conn),
    }
    .map_err(|e| format!("Failed to load memories: {}", e))?;

    if db_memories.is_empty() {
        return Ok(vec![]);
    }

    // Load all triggers for these memories
    let memory_ids: Vec<&str> = db_memories.iter().map(|l| l.id.as_str()).collect();
    let db_triggers: Vec<DbMemoryTrigger> = memory_triggers::table
        .filter(memory_triggers::memory_id.eq_any(&memory_ids))
        .load(conn)
        .map_err(|e| format!("Failed to load triggers: {}", e))?;

    // Group triggers by memory_id
    let mut trigger_map: std::collections::HashMap<String, Vec<DbMemoryTrigger>> =
        std::collections::HashMap::new();
    for trigger in db_triggers {
        trigger_map
            .entry(trigger.memory_id.clone())
            .or_default()
            .push(trigger);
    }

    // Assemble memories with their triggers
    Ok(db_memories
        .into_iter()
        .map(|memory| {
            let triggers = trigger_map.remove(&memory.id).unwrap_or_default();
            to_memory(memory, triggers)
        })
        .collect())
}

/// Load all memories (active and inactive) for listing, optionally filtered by project.
pub fn load_all_memories(
    conn: &mut SqliteConnection,
    project_id: Option<&str>,
) -> Result<Vec<Memory>, String> {
    let db_memories: Vec<DbMemory> = match project_id {
        Some(pid) => memories::table
            .filter(
                memories::project_id
                    .eq(pid)
                    .or(memories::project_id.is_null()),
            )
            .order(memories::created_at.desc())
            .load(conn),
        None => memories::table
            .order(memories::created_at.desc())
            .load(conn),
    }
    .map_err(|e| format!("Failed to load memories: {}", e))?;

    if db_memories.is_empty() {
        return Ok(vec![]);
    }

    let memory_ids: Vec<&str> = db_memories.iter().map(|l| l.id.as_str()).collect();
    let db_triggers: Vec<DbMemoryTrigger> = memory_triggers::table
        .filter(memory_triggers::memory_id.eq_any(&memory_ids))
        .load(conn)
        .map_err(|e| format!("Failed to load triggers: {}", e))?;

    let mut trigger_map: std::collections::HashMap<String, Vec<DbMemoryTrigger>> =
        std::collections::HashMap::new();
    for trigger in db_triggers {
        trigger_map
            .entry(trigger.memory_id.clone())
            .or_default()
            .push(trigger);
    }

    Ok(db_memories
        .into_iter()
        .map(|memory| {
            let triggers = trigger_map.remove(&memory.id).unwrap_or_default();
            to_memory(memory, triggers)
        })
        .collect())
}

/// Load a single memory by ID with its triggers.
pub fn load_memory(conn: &mut SqliteConnection, memory_id: &str) -> Result<Memory, String> {
    let db_memory: DbMemory = memories::table
        .find(memory_id)
        .first(conn)
        .map_err(|e| format!("Memory not found: {}", e))?;

    let db_triggers: Vec<DbMemoryTrigger> = memory_triggers::table
        .filter(memory_triggers::memory_id.eq(memory_id))
        .load(conn)
        .map_err(|e| format!("Failed to load triggers: {}", e))?;

    Ok(to_memory(db_memory, db_triggers))
}

/// Create a new memory with its triggers.
pub fn create_memory(
    conn: &mut SqliteConnection,
    id: &str,
    content: &str,
    project_id: Option<&str>,
    confidence: &str,
    source_issue: Option<&str>,
    triggers: &[(i32, &str, &str)], // (trigger_index, json_path, pattern)
) -> Result<Memory, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let new_memory = NewMemory {
        id,
        project_id,
        content,
        confidence,
        source_issue,
        created_at: now,
        updated_at: now,
        surfaced_count: 0,
        last_surfaced_at: None,
        active: 1,
    };

    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::insert_into(memories::table)
            .values(&new_memory)
            .execute(conn)?;

        for (trigger_index, json_path, pattern) in triggers {
            let new_trigger = NewMemoryTrigger {
                memory_id: id,
                trigger_index: *trigger_index,
                json_path,
                pattern,
            };
            diesel::insert_into(memory_triggers::table)
                .values(&new_trigger)
                .execute(conn)?;
        }

        Ok(())
    })
    .map_err(|e| format!("Failed to create memory: {}", e))?;

    load_memory(conn, id)
}

/// Update a memory's fields.
pub fn update_memory(
    conn: &mut SqliteConnection,
    id: &str,
    content: Option<&str>,
    confidence: Option<&str>,
    active: Option<bool>,
) -> Result<Memory, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    // Build dynamic update
    let mut updated = false;

    if let Some(content) = content {
        diesel::update(memories::table.find(id))
            .set((memories::content.eq(content), memories::updated_at.eq(now)))
            .execute(conn)
            .map_err(|e| format!("Failed to update memory: {}", e))?;
        updated = true;
    }

    if let Some(confidence) = confidence {
        diesel::update(memories::table.find(id))
            .set((
                memories::confidence.eq(confidence),
                memories::updated_at.eq(now),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to update memory: {}", e))?;
        updated = true;
    }

    if let Some(active) = active {
        diesel::update(memories::table.find(id))
            .set((
                memories::active.eq(if active { 1 } else { 0 }),
                memories::updated_at.eq(now),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to update memory: {}", e))?;
        updated = true;
    }

    if !updated {
        return Err("No fields to update".to_string());
    }

    load_memory(conn, id)
}

/// Delete a memory and its triggers (cascade).
pub fn delete_memory(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    // Triggers are cascade-deleted by FK
    diesel::delete(memories::table.find(id))
        .execute(conn)
        .map_err(|e| format!("Failed to delete memory: {}", e))?;
    Ok(())
}

/// Increment surfaced_count and set last_surfaced_at for matched memories.
pub fn record_surfacing(conn: &mut SqliteConnection, memory_ids: &[&str]) -> Result<(), String> {
    if memory_ids.is_empty() {
        return Ok(());
    }

    let now = chrono::Utc::now().timestamp() as i32;

    for id in memory_ids {
        diesel::update(memories::table.find(id))
            .set((
                memories::surfaced_count.eq(memories::surfaced_count + 1),
                memories::last_surfaced_at.eq(now),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to record surfacing: {}", e))?;
    }

    Ok(())
}

/// Replace all triggers for a memory.
pub fn replace_triggers(
    conn: &mut SqliteConnection,
    memory_id: &str,
    triggers: &[(i32, &str, &str)], // (trigger_index, json_path, pattern)
) -> Result<(), String> {
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        // Delete existing triggers
        diesel::delete(memory_triggers::table.filter(memory_triggers::memory_id.eq(memory_id)))
            .execute(conn)?;

        // Insert new triggers
        for (trigger_index, json_path, pattern) in triggers {
            let new_trigger = NewMemoryTrigger {
                memory_id,
                trigger_index: *trigger_index,
                json_path,
                pattern,
            };
            diesel::insert_into(memory_triggers::table)
                .values(&new_trigger)
                .execute(conn)?;
        }

        Ok(())
    })
    .map_err(|e| format!("Failed to replace triggers: {}", e))?;

    Ok(())
}
