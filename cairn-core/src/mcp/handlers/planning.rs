//! Communication-related MCP handlers.
//!
//! Handles: ask_user

use crate::claude::process::graceful_stop;
use crate::diesel_models::{NewPrompt, UpdateIssueChangeset, UpdateRunChangeset};
use crate::mcp::types::{AskUserPayload, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::schema::{issues, prompts, runs};
use diesel::prelude::*;

use super::lookup_run;

// ============================================================================
// Handlers
// ============================================================================

/// Handle ask_user tool call
/// Stores prompt in DB, sets status to paused, kills Claude synchronously
pub async fn handle_ask_user(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: AskUserPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!(
        "Prompting user for cwd={} with {} questions",
        request.cwd,
        payload.questions.len()
    );

    let services = &orch.services;

    // Look up the run and store prompt in DB
    let run_id = {
        let db_state = &orch.db;

        let mut conn = match db_state.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Failed to lock database: {}", e),
        };

        let diesel_conn = &mut *conn;

        let ctx = match lookup_run(diesel_conn, request) {
            Ok(ctx) => ctx,
            Err(e) => return e,
        };

        let now = chrono::Utc::now().timestamp() as i32;
        let prompt_id = uuid::Uuid::new_v4().to_string();
        let questions_json = serde_json::to_string(&payload.questions).unwrap_or_default();

        // Insert prompt
        let new_prompt = NewPrompt {
            id: &prompt_id,
            run_id: &ctx.run_id,
            questions: &questions_json,
            response: None,
            created_at: now,
            answered_at: None,
        };

        if let Err(e) = diesel::insert_into(prompts::table)
            .values(&new_prompt)
            .execute(diesel_conn)
        {
            log::error!("Failed to insert prompt: {}", e);
            return format!("Failed to store prompt: {}", e);
        }

        // Update run status to paused
        let run_update = UpdateRunChangeset {
            status: Some("paused"),
            updated_at: Some(now),
            ..Default::default()
        };
        let _ = diesel::update(runs::table.find(&ctx.run_id))
            .set(&run_update)
            .execute(diesel_conn);

        // Set status='waiting' with wait_state='prompt' on the issue (if this is an issue run)
        if let Some(issue_id) = ctx.issue_id {
            let issue_update = UpdateIssueChangeset {
                status: Some("waiting"),
                wait_state: Some(Some("prompt")),
                updated_at: Some(now),
                ..Default::default()
            };
            let _ = diesel::update(issues::table.find(&issue_id))
                .set(&issue_update)
                .execute(diesel_conn);
        }

        ctx.run_id
    };

    // Emit run-paused event for frontend to show prompt UI
    let _ = services
        .emitter
        .emit("run-paused", serde_json::json!(&run_id));

    // Kill Claude synchronously - MCP connection close is expected and fine
    {
        let process_state = &orch.process_state;
        if let Ok(mut processes) = process_state.processes.lock() {
            if let Some(active_process) = processes.remove(&run_id) {
                if let Ok(mut child_guard) = active_process.child.lock() {
                    if let Some(ref mut child) = *child_guard {
                        log::info!("Killing Claude process for run_id={} (ask_user)", run_id);
                        graceful_stop(child.as_mut());
                    }
                }
            }
        }
    }

    // Emit db-change events for tables updated via Diesel
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "insert"}),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "update"}),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );

    "Prompt stored. Session will resume when user responds.".to_string()
}
