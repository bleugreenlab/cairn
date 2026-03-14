//! Memory-related MCP handlers.
//!
//! Handles: list_memories, create_memory, update_memory, deactivate_memory

use serde::Deserialize;

use super::lookup_project_context;
use crate::mcp::types::McpCallbackRequest;
use crate::memories::db as memory_db;
use crate::orchestrator::Orchestrator;

// ============================================================================
// Payload Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMemoriesPayload {
    #[serde(default = "default_true")]
    pub active_only: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerCondition {
    pub trigger_index: i32,
    pub json_path: String,
    pub pattern: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMemoryPayload {
    pub content: String,
    pub confidence: Option<String>,
    pub source_issue: Option<String>,
    pub triggers: Vec<TriggerCondition>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMemoryPayload {
    pub id: String,
    pub content: Option<String>,
    pub confidence: Option<String>,
    pub active: Option<bool>,
    pub triggers: Option<Vec<TriggerCondition>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeactivateMemoryPayload {
    pub id: String,
}

// ============================================================================
// Handlers
// ============================================================================

/// Handle list_memories tool call
pub async fn handle_list_memories(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: ListMemoriesPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get project context
    let project_id = match lookup_project_context(&mut conn, request) {
        Ok(ctx) => Some(ctx.project_id),
        Err(_) => None, // Fall back to global-only memories
    };

    let memories = if payload.active_only {
        memory_db::load_active_memories(&mut conn, project_id.as_deref())
    } else {
        memory_db::load_all_memories(&mut conn, project_id.as_deref())
    };

    match memories {
        Ok(memories) => {
            if memories.is_empty() {
                "No memories found.".to_string()
            } else {
                let mut output = format!("Found {} memory(s):\n\n", memories.len());
                for memory in &memories {
                    let scope = match &memory.project_id {
                        Some(_) => "project",
                        None => "global",
                    };
                    let status = if memory.active { "" } else { " [inactive]" };
                    output.push_str(&format!(
                        "- **{}** ({}{}): {}\n  Confidence: {} | Surfaced: {} times | Triggers: {}\n\n",
                        memory.id,
                        scope,
                        status,
                        memory.content,
                        memory.confidence,
                        memory.surfaced_count,
                        memory.triggers.len(),
                    ));
                }
                output
            }
        }
        Err(e) => format!("Failed to list memories: {}", e),
    }
}

/// Handle create_memory tool call
pub async fn handle_create_memory(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: CreateMemoryPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    if payload.content.is_empty() {
        return "Memory content cannot be empty".to_string();
    }

    if payload.triggers.is_empty() {
        return "At least one trigger condition is required".to_string();
    }

    // Validate regex patterns
    for trigger in &payload.triggers {
        if regex::Regex::new(&trigger.pattern).is_err() {
            return format!("Invalid regex pattern: {}", trigger.pattern);
        }
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get project context for scoping
    let project_id = match lookup_project_context(&mut conn, request) {
        Ok(ctx) => Some(ctx.project_id),
        Err(_) => None,
    };

    let id = uuid::Uuid::new_v4().to_string();
    let confidence = payload.confidence.as_deref().unwrap_or("tentative");

    let triggers: Vec<(i32, &str, &str)> = payload
        .triggers
        .iter()
        .map(|t| (t.trigger_index, t.json_path.as_str(), t.pattern.as_str()))
        .collect();

    match memory_db::create_memory(
        &mut conn,
        &id,
        &payload.content,
        project_id.as_deref(),
        confidence,
        payload.source_issue.as_deref(),
        &triggers,
    ) {
        Ok(memory) => {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "memories", "action": "insert"}),
            );
            format!(
                "Created memory {} ({}, {} triggers): {}",
                memory.id,
                memory.confidence,
                memory.triggers.len(),
                memory.content
            )
        }
        Err(e) => format!("Failed to create memory: {}", e),
    }
}

/// Handle update_memory tool call
pub async fn handle_update_memory(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: UpdateMemoryPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    if payload.content.is_none()
        && payload.confidence.is_none()
        && payload.active.is_none()
        && payload.triggers.is_none()
    {
        return "No fields to update".to_string();
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Update scalar fields if any provided
    if payload.content.is_some() || payload.confidence.is_some() || payload.active.is_some() {
        if let Err(e) = memory_db::update_memory(
            &mut conn,
            &payload.id,
            payload.content.as_deref(),
            payload.confidence.as_deref(),
            payload.active,
        ) {
            return format!("Failed to update memory: {}", e);
        }
    }

    // Replace triggers if provided
    if let Some(triggers) = &payload.triggers {
        // Validate regex patterns
        for trigger in triggers {
            if regex::Regex::new(&trigger.pattern).is_err() {
                return format!("Invalid regex pattern: {}", trigger.pattern);
            }
        }

        let trigger_tuples: Vec<(i32, &str, &str)> = triggers
            .iter()
            .map(|t| (t.trigger_index, t.json_path.as_str(), t.pattern.as_str()))
            .collect();
        if let Err(e) = memory_db::replace_triggers(&mut conn, &payload.id, &trigger_tuples) {
            return format!("Failed to update triggers: {}", e);
        }
    }

    match memory_db::load_memory(&mut conn, &payload.id) {
        Ok(memory) => {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "memories", "action": "update"}),
            );
            format!("Updated memory {}: {}", memory.id, memory.content)
        }
        Err(e) => format!("Failed to load updated memory: {}", e),
    }
}

/// Handle deactivate_memory tool call
pub async fn handle_deactivate_memory(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: DeactivateMemoryPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    match memory_db::update_memory(&mut conn, &payload.id, None, None, Some(false)) {
        Ok(memory) => {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "memories", "action": "update"}),
            );
            format!("Deactivated memory {}: {}", memory.id, memory.content)
        }
        Err(e) => format!("Failed to deactivate memory: {}", e),
    }
}
