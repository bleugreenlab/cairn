//! Permission prompt handling for Claude CLI --permission-prompt-tool
//!
//! Handles: permission_prompt
//!
//! When Claude wants to use a tool not in --allowedTools, it calls this MCP tool.
//! We store the request in the database, emit an event for the frontend, and
//! wait (blocking) for the user to approve or deny via the Orchestrator's
//! permission_responses broadcast channel.

use crate::diesel_models::NewPermissionRequest;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::schema::permission_requests;
use diesel::prelude::*;

/// Tools that are always allowed without user confirmation.
/// These tools are safe to auto-approve because they are non-destructive
/// or are standard Claude Code utilities (e.g., built-in /bug reporting).
const ALWAYS_ALLOWED_TOOLS: &[&str] = &["bug_report"];

/// Payload for permission_prompt tool
/// Note: Fields are snake_case to match what cairn-mcp sends
#[derive(Debug, serde::Deserialize)]
pub struct PermissionPromptPayload {
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: serde_json::Value,
}

/// Handle permission_prompt tool call
/// Stores request in DB, emits event, waits for user response via broadcast channel
pub async fn handle_permission_prompt(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let services = &orch.services;
    let payload: PermissionPromptPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return deny_response(&format!("Invalid payload: {}", e));
        }
    };

    log::info!(
        "Permission request: tool={}, tool_use_id={}",
        payload.tool_name,
        payload.tool_use_id
    );

    // Auto-allow tools that don't need user confirmation
    if ALWAYS_ALLOWED_TOOLS.contains(&payload.tool_name.as_str()) {
        log::info!("Auto-allowing tool: {}", payload.tool_name);
        return allow_response(&payload.input);
    }

    // Auto-allow tools previously approved via "Allow for Session"
    if let Ok(allowed) = orch.session_allowed_tools.lock() {
        if allowed.contains(&payload.tool_name) {
            log::info!("Auto-allowing session-approved tool: {}", payload.tool_name);
            return allow_response(&payload.input);
        }
    }

    // Get run_id - use request's run_id directly, or generate one for project chat
    let run_id = request
        .run_id
        .clone()
        .unwrap_or_else(|| format!("project-chat-{}", uuid::Uuid::new_v4()));

    // Store permission request
    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return deny_response(&format!("DB lock failed: {}", e)),
        };

        let tool_input_json = serde_json::to_string(&payload.input).unwrap_or_default();

        let new_request = NewPermissionRequest {
            id: &request_id,
            run_id: &run_id,
            tool_use_id: &payload.tool_use_id,
            tool_name: &payload.tool_name,
            tool_input: &tool_input_json,
            status: "pending",
            created_at: now,
        };

        if let Err(e) = diesel::insert_into(permission_requests::table)
            .values(&new_request)
            .execute(&mut *conn)
        {
            return deny_response(&format!("Failed to store request: {}", e));
        }
    }

    // Emit event for frontend
    let _ = services.emitter.emit(
        "permission-request",
        serde_json::json!({
            "requestId": request_id,
            "runId": run_id,
            "toolUseId": payload.tool_use_id,
            "toolName": payload.tool_name,
            "input": payload.input,
        }),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "insert"}),
    );

    // Subscribe to permission responses broadcast channel
    let mut rx = orch.permission_responses.subscribe();
    let request_id_clone = request_id.clone();

    // Wait for matching response with 5 minute timeout
    let result = tokio::time::timeout(std::time::Duration::from_secs(300), async {
        loop {
            match rx.recv().await {
                Ok((resp_request_id, response_json)) => {
                    if resp_request_id == request_id_clone {
                        return Ok(response_json);
                    }
                    // Not our request, keep waiting
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err("Channel closed");
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Missed some messages, keep going
                    continue;
                }
            }
        }
    })
    .await;

    match result {
        Ok(Ok(response)) => response,
        Ok(Err(msg)) => deny_response(msg),
        Err(_) => {
            // Timeout - update the request status to denied
            if let Ok(mut conn) = orch.db.conn.lock() {
                let now = chrono::Utc::now().timestamp() as i32;
                let _ = diesel::update(permission_requests::table.find(&request_id))
                    .set((
                        permission_requests::status.eq("denied"),
                        permission_requests::response.eq(Some(
                            r#"{"behavior":"deny","message":"Request timed out after 5 minutes"}"#,
                        )),
                        permission_requests::responded_at.eq(Some(now)),
                    ))
                    .execute(&mut *conn);
            }
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "permission_requests", "action": "update"}),
            );
            deny_response("Permission request timed out after 5 minutes")
        }
    }
}

pub fn allow_response(original_input: &serde_json::Value) -> String {
    serde_json::json!({
        "behavior": "allow",
        "updatedInput": original_input
    })
    .to_string()
}

pub fn deny_response(message: &str) -> String {
    serde_json::json!({
        "behavior": "deny",
        "message": message
    })
    .to_string()
}
