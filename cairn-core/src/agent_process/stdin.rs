//! Stdin communication for bidirectional Claude CLI streaming.
//!
//! This module provides functions to send messages to Claude CLI via stdin
//! when using `--input-format stream-json` mode.

use base64::Engine;
use regex::Regex;
use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;

/// Resolver function that converts an image path to base64-encoded data.
type Base64Resolver<'a> = Option<&'a dyn Fn(&Path) -> Option<String>>;

/// Get MIME type for an image extension
fn get_mime_type(ext: &str) -> &'static str {
    match ext.to_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

/// Extract local file paths from text that look like images.
/// Matches patterns like:
/// - /absolute/path/to/image.png
/// - ./relative/path/to/image.jpg
/// - file:///path/to/image.png
fn extract_image_paths(text: &str) -> Vec<String> {
    let mut paths = Vec::new();

    // Match file:// URLs
    let file_url_re = Regex::new(r"file://(/[^\s\)>\]]+\.(png|jpg|jpeg|gif|webp))").unwrap();
    for cap in file_url_re.captures_iter(text) {
        if let Some(path) = cap.get(1) {
            paths.push(path.as_str().to_string());
        }
    }

    // Match absolute paths (starting with /)
    let abs_path_re =
        Regex::new(r"(?:^|[\s\(\[<])(/[^\s\)>\]]+\.(png|jpg|jpeg|gif|webp))").unwrap();
    for cap in abs_path_re.captures_iter(text) {
        if let Some(path) = cap.get(1) {
            let p = path.as_str().to_string();
            if !paths.contains(&p) {
                paths.push(p);
            }
        }
    }

    // Match relative paths (starting with ./)
    let rel_path_re =
        Regex::new(r"(?:^|[\s\(\[<])(\./[^\s\)>\]]+\.(png|jpg|jpeg|gif|webp))").unwrap();
    for cap in rel_path_re.captures_iter(text) {
        if let Some(path) = cap.get(1) {
            let p = path.as_str().to_string();
            if !paths.contains(&p) {
                paths.push(p);
            }
        }
    }

    paths
}

/// Build message content with embedded images.
/// Returns a content array if images are found, or a simple string if not.
///
/// If `resolve_base64` is provided, it will be called for each image path
/// before falling back to reading from disk. This allows hosts to provide
/// pre-computed base64 data (e.g. from a cache).
pub(crate) fn build_message_content(
    text: &str,
    working_dir: Option<&str>,
    resolve_base64: Base64Resolver<'_>,
) -> Value {
    let image_paths = extract_image_paths(text);

    if image_paths.is_empty() {
        // No images - return simple text content
        return json!(text);
    }

    // Build content array with text and images
    let mut content_blocks: Vec<Value> = Vec::new();

    // Add text block first
    content_blocks.push(json!({
        "type": "text",
        "text": text
    }));

    // Try to embed each image
    for path_str in &image_paths {
        let path = if let Some(relative) = path_str.strip_prefix("./") {
            // Resolve relative path against working directory
            if let Some(wd) = working_dir {
                Path::new(wd).join(relative)
            } else {
                Path::new(path_str).to_path_buf()
            }
        } else {
            Path::new(path_str).to_path_buf()
        };

        if path.exists() {
            // Try host-provided resolver first (e.g. cache), then fall back to disk read
            let base64_data = resolve_base64.and_then(|f| f(&path)).or_else(|| {
                // Fallback: read and encode from disk
                std::fs::read(&path).ok().map(|data| {
                    log::debug!(
                        "Reading image from disk: {} ({} bytes)",
                        path.display(),
                        data.len()
                    );
                    base64::engine::general_purpose::STANDARD.encode(&data)
                })
            });

            if let Some(base64_data) = base64_data {
                let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("png");
                let mime_type = get_mime_type(ext);

                log::info!("Embedding image {} ({})", path.display(), mime_type);

                content_blocks.push(json!({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": mime_type,
                        "data": base64_data
                    }
                }));
            } else {
                log::warn!("Failed to read image {}", path.display());
            }
        } else {
            log::debug!("Image path not found: {}", path.display());
        }
    }

    // If we only have the text block (no images loaded), return simple string
    if content_blocks.len() == 1 {
        return json!(text);
    }

    json!(content_blocks)
}

/// Send a user message to Claude via stdin with image embedding.
/// Images referenced in the content (markdown image syntax) will be embedded as base64.
/// The working_dir is only needed for relative paths; absolute paths work without it.
/// If `resolve_base64` is provided, it will be called for each image path before
/// falling back to reading from disk.
pub(crate) fn send_user_message_with_images(
    stdin: &mut dyn Write,
    session_id: &str,
    content: &str,
    parent_tool_use_id: Option<&str>,
    working_dir: Option<&str>,
    resolve_base64: Base64Resolver<'_>,
) -> Result<(), String> {
    let message_content = build_message_content(content, working_dir, resolve_base64);

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
        content.len()
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
/// Note: Currently unused - permissions use MCP callback. Will be used when
/// stdin-based permission handling is implemented.
#[allow(dead_code)]
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

/// Send an interrupt control request to gracefully stop the current turn.
/// The process stays alive and can receive new messages for follow-up.
pub(crate) fn send_interrupt_request(
    stdin: &mut dyn Write,
    request_id: &str,
) -> Result<(), String> {
    send_control_request(stdin, request_id, json!({ "subtype": "interrupt" }))
}

/// Send a set_model control request to change the model for subsequent turns.
pub(crate) fn send_set_model_request(
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
pub(crate) fn send_set_permission_mode_request(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse_buffer(buffer: Cursor<Vec<u8>>) -> serde_json::Value {
        let output = String::from_utf8(buffer.into_inner()).unwrap();
        serde_json::from_str(output.trim()).unwrap()
    }

    #[test]
    fn test_send_user_message() {
        let mut buffer = Cursor::new(Vec::new());

        send_user_message_with_images(
            &mut buffer,
            "session-123",
            "Hello, Claude!",
            None,
            None,
            None,
        )
        .unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "user");
        assert_eq!(parsed["session_id"], "session-123");
        assert_eq!(parsed["message"]["role"], "user");
        assert_eq!(parsed["message"]["content"], "Hello, Claude!");
        assert!(parsed["parent_tool_use_id"].is_null());
    }

    #[test]
    fn test_send_user_message_with_parent_tool_use_id() {
        let mut buffer = Cursor::new(Vec::new());

        send_user_message_with_images(
            &mut buffer,
            "session-456",
            "Subagent message",
            Some("toolu_abc123"),
            None,
            None,
        )
        .unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "user");
        assert_eq!(parsed["parent_tool_use_id"], "toolu_abc123");
    }

    #[test]
    fn test_send_control_response_allow() {
        let mut buffer = Cursor::new(Vec::new());

        send_control_response(&mut buffer, "req-789", true, Some("Approved by user")).unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "control_response");
        assert_eq!(parsed["request_id"], "req-789");
        assert_eq!(parsed["response"]["subtype"], "success");
        assert_eq!(parsed["response"]["response"]["behavior"], "allow");
        assert_eq!(
            parsed["response"]["response"]["message"],
            "Approved by user"
        );
    }

    #[test]
    fn test_send_control_response_deny() {
        let mut buffer = Cursor::new(Vec::new());

        send_control_response(&mut buffer, "req-abc", false, None).unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "control_response");
        assert_eq!(parsed["response"]["response"]["behavior"], "deny");
        assert!(parsed["response"]["response"]["message"].is_null());
    }

    #[test]
    fn test_send_interrupt_request() {
        let mut buffer = Cursor::new(Vec::new());

        send_interrupt_request(&mut buffer, "req-int-1").unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "control_request");
        assert_eq!(parsed["request_id"], "req-int-1");
        assert_eq!(parsed["request"]["subtype"], "interrupt");
    }

    #[test]
    fn test_send_set_model_request() {
        let mut buffer = Cursor::new(Vec::new());

        send_set_model_request(&mut buffer, "req-model-1", "claude-sonnet-4-20250514").unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "control_request");
        assert_eq!(parsed["request_id"], "req-model-1");
        assert_eq!(parsed["request"]["subtype"], "set_model");
        assert_eq!(parsed["request"]["model"], "claude-sonnet-4-20250514");
    }

    #[test]
    fn test_send_set_permission_mode_request() {
        let mut buffer = Cursor::new(Vec::new());

        send_set_permission_mode_request(&mut buffer, "req-perm-1", "bypassPermissions").unwrap();

        let parsed = parse_buffer(buffer);

        assert_eq!(parsed["type"], "control_request");
        assert_eq!(parsed["request_id"], "req-perm-1");
        assert_eq!(parsed["request"]["subtype"], "set_permission_mode");
        assert_eq!(parsed["request"]["mode"], "bypassPermissions");
    }
}
