//! Memory command orchestration logic.
//!
//! Business logic for create/update/delete memory operations.
//! Callers (Tauri commands, MCP handlers) are responsible for event emission.

use diesel::sqlite::SqliteConnection;

use crate::memories::db as memory_db;
use crate::models::{CreateMemory, Memory, UpdateMemory};

/// Helper to convert CreateMemoryTrigger to the tuple format memory_db expects.
fn trigger_tuples(triggers: &[crate::models::CreateMemoryTrigger]) -> Vec<(i32, &str, &str)> {
    triggers
        .iter()
        .map(|t| (t.trigger_index, t.json_path.as_str(), t.pattern.as_str()))
        .collect()
}

/// Create a new memory with triggers.
///
/// Generates a UUID, normalizes confidence, converts trigger format,
/// and delegates to `memory_db::create_memory`.
pub fn create(conn: &mut SqliteConnection, input: CreateMemory) -> Result<Memory, String> {
    let id = uuid::Uuid::new_v4().to_string();
    let confidence = input
        .confidence
        .as_ref()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "tentative".to_string());

    let triggers = trigger_tuples(&input.triggers);

    memory_db::create_memory(
        conn,
        &id,
        &input.content,
        input.project_id.as_deref(),
        &confidence,
        input.source_issue.as_deref(),
        &triggers,
    )
}

/// Update a memory's scalar fields and/or triggers.
///
/// Only updates fields that are provided (Some). Returns the updated memory.
pub fn update(conn: &mut SqliteConnection, input: UpdateMemory) -> Result<Memory, String> {
    // Update scalar fields if any provided
    if input.content.is_some() || input.confidence.is_some() || input.active.is_some() {
        memory_db::update_memory(
            conn,
            &input.id,
            input.content.as_deref(),
            input.confidence.as_ref().map(|c| c.to_string()).as_deref(),
            input.active,
        )?;
    }

    // Replace triggers if provided
    if let Some(triggers) = &input.triggers {
        // Validate regex patterns before writing to DB
        for trigger in triggers {
            if regex::Regex::new(&trigger.pattern).is_err() {
                return Err(format!("Invalid regex pattern: {}", trigger.pattern));
            }
        }

        let tuples = trigger_tuples(triggers);
        memory_db::replace_triggers(conn, &input.id, &tuples)?;
    }

    memory_db::load_memory(conn, &input.id)
}

/// Delete a memory and its triggers.
pub fn delete(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    memory_db::delete_memory(conn, id)
}
