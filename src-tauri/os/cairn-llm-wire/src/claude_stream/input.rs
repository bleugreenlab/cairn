//! Outbound stream-json: the messages written to a running Claude CLI's stdin
//! under `--input-format stream-json`.

use base64::Engine;
use serde_json::{json, Value};
use std::io::Write;

use crate::replay::{MessageContent, EMPTY_MESSAGE_REFUSAL};

/// Serialize common content into Claude stream-json's native content blocks.
///
/// Never emits an empty text block. The provider refuses one outright (`text
/// content blocks must be non-empty`), and a harness that persists it converts a
/// single bad turn into a permanently unresumable session (CAIRN-3263), so blank
/// text is dropped when images carry the message and refused when nothing else
/// does.
pub fn build_message_content(content: &MessageContent) -> Result<Value, String> {
    let has_text = !content.text.trim().is_empty();
    if !has_text && content.images.is_empty() {
        return Err(EMPTY_MESSAGE_REFUSAL.to_string());
    }
    if content.images.is_empty() {
        return Ok(json!(content.text));
    }

    let mut blocks = Vec::with_capacity(content.images.len() + 1);
    if has_text {
        blocks.push(json!({ "type": "text", "text": content.text }));
    }
    blocks.extend(content.images.iter().map(|image| {
        json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": image.mime_type,
                "data": base64::engine::general_purpose::STANDARD.encode(&image.bytes),
            }
        })
    }));
    Ok(Value::Array(blocks))
}

/// Send a user message to Claude via stdin using pre-resolved native parts.
pub fn send_user_message(
    stdin: &mut dyn Write,
    session_id: &str,
    content: &MessageContent,
    parent_tool_use_id: Option<&str>,
) -> Result<(), String> {
    let message_content = build_message_content(content)?;

    let message = json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": message_content
        },
        "session_id": session_id,
        "parent_tool_use_id": parent_tool_use_id
    });

    writeln!(stdin, "{}", message).map_err(|e| format!("Failed to write to stdin: {}", e))?;
    stdin
        .flush()
        .map_err(|e| format!("Failed to flush stdin: {}", e))?;

    log::info!(
        "Sent user message via stdin to session {}: {} chars",
        &session_id[..session_id.len().min(8)],
        content.text.len()
    );

    Ok(())
}

/// Send a control response to Claude via stdin (for permission prompts).
///
/// Format:
/// ```json
/// {
///   "type": "control_response",
///   "request_id": "...",
///   "response": {
///     "subtype": "success",
///     "response": {"behavior": "allow"|"deny", "message": "..."}
///   }
/// }
/// ```
///
/// Cairn answers permissions through its MCP callback today, so nothing sends
/// this yet; it is the stdin half of the protocol for when that changes.
pub fn send_control_response(
    stdin: &mut dyn Write,
    request_id: &str,
    allow: bool,
    message: Option<&str>,
) -> Result<(), String> {
    let behavior = if allow { "allow" } else { "deny" };

    let response = json!({
        "type": "control_response",
        "request_id": request_id,
        "response": {
            "subtype": "success",
            "response": {
                "behavior": behavior,
                "message": message
            }
        }
    });

    writeln!(stdin, "{}", response).map_err(|e| format!("Failed to write to stdin: {}", e))?;
    stdin
        .flush()
        .map_err(|e| format!("Failed to flush stdin: {}", e))?;

    log::info!(
        "Sent control response via stdin: request_id={}, behavior={}",
        &request_id[..request_id.len().min(8)],
        behavior
    );

    Ok(())
}

/// Send a control request to Claude via stdin.
///
/// Control requests allow runtime control of the Claude CLI:
/// - `interrupt`: Gracefully interrupt the current turn
/// - `set_model`: Change the model for subsequent turns
/// - `set_permission_mode`: Change permission handling mode
///
/// Format:
/// ```json
/// {
///   "type": "control_request",
///   "request_id": "...",
///   "request": { "subtype": "interrupt" | "set_model" | "set_permission_mode", ... }
/// }
/// ```
fn send_control_request(
    stdin: &mut dyn Write,
    request_id: &str,
    request: serde_json::Value,
) -> Result<(), String> {
    let message = json!({
        "type": "control_request",
        "request_id": request_id,
        "request": request
    });

    writeln!(stdin, "{}", message)
        .map_err(|e| format!("Failed to write control request: {}", e))?;
    stdin
        .flush()
        .map_err(|e| format!("Failed to flush stdin: {}", e))?;

    log::info!(
        "Sent control request via stdin: request_id={}, subtype={}",
        &request_id[..request_id.len().min(8)],
        request
            .get("subtype")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
    );

    Ok(())
}

/// Set environment variables inside a running CLI.
///
/// Format:
/// ```json
/// {"type": "update_environment_variables", "variables": {"NAME": "value"}, "request_id": "..."}
/// ```
///
/// A top-level message, not a `control_request`, but acknowledged with a
/// `control_response` carrying `request_id`. The CLI accepts only the names on
/// its allowlist (`CLAUDE_CODE_OAUTH_TOKEN`, `CLAUDE_CODE_SESSION_ACCESS_TOKEN`);
/// a new OAuth token also clears its credential caches, so the next request,
/// including one the CLI is retrying after a 401, carries it.
pub fn send_update_environment_variables(
    stdin: &mut dyn Write,
    request_id: &str,
    variables: &[(&str, &str)],
) -> Result<(), String> {
    let variables: serde_json::Map<String, Value> = variables
        .iter()
        .map(|(name, value)| ((*name).to_string(), Value::from(*value)))
        .collect();
    let message = json!({
        "type": "update_environment_variables",
        "variables": variables,
        "request_id": request_id,
    });
    writeln!(stdin, "{}", message)
        .map_err(|e| format!("Failed to write environment update: {}", e))?;
    stdin
        .flush()
        .map_err(|e| format!("Failed to flush stdin: {}", e))?;
    log::info!(
        "Sent environment update via stdin: request_id={}, variables={:?}",
        request_id,
        variables.keys().collect::<Vec<_>>()
    );
    Ok(())
}

/// Send an interrupt control request to gracefully stop the current turn.
/// The process stays alive and can receive new messages for follow-up.
pub fn send_interrupt_request(stdin: &mut dyn Write, request_id: &str) -> Result<(), String> {
    send_control_request(stdin, request_id, json!({ "subtype": "interrupt" }))
}

/// Send a set_model control request to change the model for subsequent turns.
pub fn send_set_model_request(
    stdin: &mut dyn Write,
    request_id: &str,
    model: &str,
) -> Result<(), String> {
    send_control_request(
        stdin,
        request_id,
        json!({ "subtype": "set_model", "model": model }),
    )
}

/// Send a set_permission_mode control request to change permission handling.
pub fn send_set_permission_mode_request(
    stdin: &mut dyn Write,
    request_id: &str,
    mode: &str,
) -> Result<(), String> {
    send_control_request(
        stdin,
        request_id,
        json!({ "subtype": "set_permission_mode", "mode": mode }),
    )
}
