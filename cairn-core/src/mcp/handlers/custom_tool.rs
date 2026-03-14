//! Custom tool execution handler.
//!
//! Handles execution of user-defined custom tools by looking up tool code
//! from the execution snapshot (or files as fallback), checking required
//! tools against the agent's allowed tools, and running the code via the
//! execute runtime.

use std::io::Write as IoWrite;
use std::path::PathBuf;

use diesel::prelude::*;
use serde::Deserialize;
use tempfile::NamedTempFile;

use crate::config::tools as config_tools;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

use super::execute::{execute_code, format_tool_response, ExecutePayload};
use super::{lookup_run, RunContext};

#[derive(Debug, Deserialize)]
pub struct CustomToolPayload {
    pub tool_id: String,
    pub inputs: serde_json::Value,
}

/// Handle custom tool execution synchronously. Must be called from a dedicated
/// thread (same pattern as execute handler) because the Bun process may make
/// MCP callbacks that need tokio to process.
pub fn handle_custom_tool_sync(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: CustomToolPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid custom tool payload: {}", e),
    };

    log::info!("custom_tool called: {}", payload.tool_id);

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Connection error: {}", e),
    };

    let ctx = match lookup_run(&mut conn, request) {
        Ok(c) => c,
        Err(e) => return e,
    };

    let config_dir = orch.config_dir.clone();

    // Look up tool: snapshot first, then files
    let (tool_code, required_tools) =
        match lookup_tool_code(&mut conn, &ctx, &payload.tool_id, &config_dir) {
            Ok(result) => result,
            Err(e) => return e,
        };

    // Get agent's allowed tools
    let allowed_tools = super::execute::get_allowed_tools_for_run(&mut conn, &ctx, &config_dir);

    // Check required_tools ⊆ allowed_tools
    let normalized_allowed = super::execute::normalize_tool_names(&allowed_tools);
    let missing: Vec<&String> = required_tools
        .iter()
        .filter(|t| !normalized_allowed.contains(t.as_str()))
        .collect();

    if !missing.is_empty() {
        return format!(
            "Custom tool '{}' requires tools {:?} but agent only has access to {:?}",
            payload.tool_id,
            missing,
            allowed_tools.iter().collect::<Vec<_>>()
        );
    }

    let mcp_secret = orch.mcp_auth.get_secret_for_mcp().unwrap_or_default();

    let project_path: Option<String> = crate::schema::projects::table
        .find(&ctx.project_id)
        .select(crate::schema::projects::repo_path)
        .first(&mut *conn)
        .ok();

    drop(conn);

    let cwd = project_path.as_deref().unwrap_or(&request.cwd);

    // Write tool code to a temp .ts file for import-based execution
    let mut tool_file = match NamedTempFile::with_suffix(".ts") {
        Ok(f) => f,
        Err(e) => return format_error(&format!("Failed to create temp tool file: {}", e)),
    };
    if let Err(e) = tool_file.write_all(tool_code.as_bytes()) {
        return format_error(&format!("Failed to write tool file: {}", e));
    }
    let tool_path = tool_file.path().to_string_lossy().to_string();

    let inputs_json = serde_json::to_string(&payload.inputs).unwrap_or_else(|_| "{}".to_string());
    let combined_code = format!(
        r#"const __m = await import("file://{}");
return __m.default({{ inputs: {} as const, mcp, read, CWD, RUN_ID, PROJECT_ID }});"#,
        tool_path, inputs_json
    );

    let execute_payload = ExecutePayload {
        code: combined_code,
        timeout: Some(300),
    };

    // tool_file lives on the stack — stays alive through synchronous execute_code()
    match execute_code(
        &ctx,
        cwd,
        &execute_payload,
        &mcp_secret,
        &allowed_tools,
        orch.mcp_callback_port,
    ) {
        Ok(result) => format_tool_response(&result),
        Err(e) => format_error(&e),
    }
}

/// Look up tool code from snapshot (preferred) or files (fallback).
/// Returns (code, required_tools) on success.
fn lookup_tool_code(
    conn: &mut diesel::SqliteConnection,
    ctx: &RunContext,
    tool_id: &str,
    config_dir: &std::path::Path,
) -> Result<(String, Vec<String>), String> {
    // Try snapshot first (if execution_id exists)
    if let Some(ref execution_id) = ctx.execution_id {
        if let Ok(snapshot) = crate::jobs::queries::load_execution_snapshot(conn, execution_id) {
            if let Some(tool) = snapshot.tools.get(tool_id) {
                return Ok((tool.code.clone(), tool.required_tools.clone()));
            }
        }
    }

    // Fallback to files
    let project_path: Option<PathBuf> = crate::schema::projects::table
        .find(&ctx.project_id)
        .select(crate::schema::projects::repo_path)
        .first::<String>(conn)
        .ok()
        .map(PathBuf::from);

    match config_tools::get_tool(config_dir, tool_id, project_path.as_deref()) {
        Ok(Some(tool)) => Ok((tool.code, tool.required_tools)),
        Ok(None) => Err(format!("Custom tool '{}' not found", tool_id)),
        Err(e) => Err(format!("Error loading custom tool '{}': {}", tool_id, e)),
    }
}

fn format_error(message: &str) -> String {
    serde_json::to_string(&super::execute::ToolResponse {
        output: message.to_string(),
        is_error: true,
    })
    .unwrap_or_else(|_| format!(r#"{{"output":"{}","isError":true}}"#, message))
}
