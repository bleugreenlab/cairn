//! Readers for the items a Codex turn produces.

use serde_json::Value;

/// The text of a streamed delta, under whichever key this server version uses.
pub fn extract_app_server_delta(msg: &Value) -> Option<&str> {
    msg.pointer("/params/delta")
        .and_then(|v| v.as_str())
        .or_else(|| msg.pointer("/params/delta/text").and_then(|v| v.as_str()))
        .or_else(|| msg.pointer("/params/textDelta").and_then(|v| v.as_str()))
}

/// A `commandExecution` item's command as display text and as argv. Codex sends
/// either a single string or an argument array.
pub fn extract_command_execution(command: &Value) -> (String, Vec<String>) {
    if let Some(command_str) = command.as_str() {
        return (command_str.to_string(), vec![command_str.to_string()]);
    }

    let command_vec: Vec<String> = command
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|val| val.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let command_display = if command_vec.is_empty() {
        String::new()
    } else {
        command_vec.join(" ")
    };
    (command_display, command_vec)
}

/// Extract display text from a Codex MCP CallToolResult.
///
/// The wire format is `Result<CallToolResult, String>` serialized as:
///   `{"Ok": {"content": [{"type":"text","text":"..."}], "is_error": false}}`
///   or `{"Err": "message"}`
///
/// Returns (display_text, is_error, optional raw value for storage).
pub fn extract_mcp_result(result: &Option<Value>) -> (String, bool, Option<Value>) {
    let Some(val) = result else {
        return ("Completed".to_string(), false, None);
    };

    // Handle Result::Err
    if let Some(err_str) = val.get("Err").and_then(|v| v.as_str()) {
        return (format!("Error: {}", err_str), true, Some(val.clone()));
    }

    // Handle Result::Ok -> CallToolResult
    let call_result = val.get("Ok").unwrap_or(val);

    let is_err = call_result
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let text = call_result
        .get("content")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(mcp_content_block_text)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();

    let display = if text.is_empty() {
        if is_err {
            "Error".to_string()
        } else {
            "Completed".to_string()
        }
    } else {
        text
    };

    // Only store raw if there's non-text content (images, structured_content, etc.)
    let has_non_text = call_result
        .get("content")
        .and_then(|v| v.as_array())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("type").and_then(|t| t.as_str()) != Some("text"))
        });
    let raw = if has_non_text || call_result.get("structured_content").is_some() {
        Some(val.clone())
    } else {
        None
    };

    (display, is_err, raw)
}

/// One MCP content block as display text: a text block's text, and a short
/// marker for media and resources so a result that was only an image still
/// says what it returned. Never a block's payload: image and audio data are
/// base64 the transcript has no use for.
fn mcp_content_block_text(block: &Value) -> Option<String> {
    let field = |name: &str| block.get(name).and_then(Value::as_str);
    match field("type")? {
        "text" => field("text").map(str::to_string),
        kind @ ("image" | "audio") => Some(match field("mimeType") {
            Some(mime) => format!("[{kind}: {mime}]"),
            None => format!("[{kind}]"),
        }),
        "resource_link" => field("uri").map(|uri| format!("[resource: {uri}]")),
        "resource" => block
            .pointer("/resource/uri")
            .and_then(Value::as_str)
            .map(|uri| format!("[resource: {uri}]")),
        _ => None,
    }
}

/// A completed `mcpToolCall` item's result text, whether it failed, and its
/// raw result for storage (see [`extract_mcp_result`]). A failed call carries
/// `error: {message}` and no result, and its status says `failed`.
pub fn summarize_mcp_tool_call(item: &Value) -> (String, bool, Option<Value>) {
    let failed = item.get("status").and_then(Value::as_str) == Some("failed");
    let error = item.get("error").filter(|error| !error.is_null());
    match item.get("result").filter(|result| !result.is_null()) {
        Some(result) => {
            let (text, is_error, raw) = extract_mcp_result(&Some(result.clone()));
            (text, is_error || failed || error.is_some(), raw)
        }
        None => match error {
            Some(error) => {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .or_else(|| error.as_str())
                    .unwrap_or("the MCP tool call failed");
                (format!("Error: {message}"), true, None)
            }
            None if failed => ("Error".to_string(), true, None),
            None => ("Completed".to_string(), false, None),
        },
    }
}

/// A completed `commandExecution` item's result text and whether it failed.
pub fn summarize_command_result(item: &Value) -> (String, bool) {
    let aggregated = item
        .get("aggregatedOutput")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let stdout = item
        .get("stdout")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let stderr = item
        .get("stderr")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| format!("stderr: {}", s));
    let exit_code = item.get("exitCode").and_then(|v| v.as_i64());
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");

    let result_text = aggregated
        .or(stdout)
        .or(stderr)
        .unwrap_or_else(|| match exit_code {
            Some(0) => "Command completed successfully".to_string(),
            Some(code) => format!("Exit code {}", code),
            None => status.to_string(),
        });

    let is_error = exit_code.is_some_and(|c| c != 0) || matches!(status, "failed" | "declined");
    (result_text, is_error)
}

/// A completed `fileChange` item's result text and whether it failed.
pub fn summarize_file_change_result(item: &Value) -> (String, bool) {
    if let Some(err) = item.get("error").and_then(|v| v.as_str()) {
        return (format!("Error: {}", err), true);
    }
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");
    let text = match status {
        "completed" => "File changes applied".to_string(),
        "declined" => "File changes declined".to_string(),
        "failed" => "File changes failed".to_string(),
        other => format!("File changes {}", other),
    };
    let is_error = matches!(status, "failed" | "declined");
    (text, is_error)
}
