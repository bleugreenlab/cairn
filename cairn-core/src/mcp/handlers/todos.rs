//! MCP handler for todo_write tool.

use crate::mcp::types::{McpCallbackRequest, TodoWritePayload};
use crate::orchestrator::Orchestrator;

use super::lookup_run;

/// Handle todo_write request — replaces all todos for the calling job.
pub fn handle_todo_write(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: TodoWritePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database error: {}", e),
    };

    let run_ctx = match lookup_run(&mut conn, request) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    match crate::todos::replace_todos(&mut conn, &run_ctx.job_id, &payload.todos) {
        Ok(todos) => {
            drop(conn);

            // Emit db-change so frontend updates
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "todos", "action": "replace"}),
            );

            let completed = todos.iter().filter(|t| t.status == "completed").count();
            format!("Updated {} todos ({} completed)", todos.len(), completed)
        }
        Err(e) => format!("Failed to write todos: {}", e),
    }
}
