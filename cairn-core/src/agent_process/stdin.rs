//! Stdin communication for bidirectional Claude CLI streaming.
//!
//! This module provides functions to send messages to Claude CLI via stdin
//! when using `--input-format stream-json` mode.

use base64::Engine;
use serde_json::{json, Value};
use std::io::Write;

/// Provider-neutral user message content. Image bytes are resolved from durable
/// storage before a backend serializes the message; encoded bytes never enter a
/// text part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageContent {
    pub(crate) text: String,
    pub(crate) images: Vec<MessageImage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MessageImage {
    pub(crate) mime_type: String,
    pub(crate) bytes: Vec<u8>,
}

#[cfg(test)]
pub(crate) trait StableImageResolver {
    fn resolve(&self, uri: &str) -> Result<MessageImage, String>;
}

pub(crate) async fn resolve_stable_images(
    db: &crate::db::DbState,
    authorized_project_id: &str,
    authorized_project_key: &str,
    text: impl Into<String>,
) -> Result<MessageContent, String> {
    let text = text.into();
    let mut seen = std::collections::HashSet::new();
    let mut images = Vec::new();
    for found in cairn_common::uri::scan_stored_images(&text) {
        let uri = found.uri;
        if !seen.insert(uri.clone()) {
            continue;
        }
        let cairn_common::uri::CairnResource::ProjectImage { project, reference } = found.resource
        else {
            continue;
        };
        if !project.eq_ignore_ascii_case(authorized_project_key) {
            return Err(format!(
                "durable image {uri} is outside the authorized project"
            ));
        }
        let owning_db = crate::projects::crud::owning_db(db, authorized_project_id).await?;
        let authorized_project = crate::projects::crud::get_db(&owning_db, authorized_project_id)
            .await?
            .ok_or_else(|| format!("authorized project not found: {authorized_project_id}"))?;
        if !authorized_project
            .key
            .eq_ignore_ascii_case(authorized_project_key)
        {
            return Err("durable image authority no longer matches the current project".into());
        }
        let image =
            crate::images::fetch_image_by_reference(&owning_db, authorized_project_id, &reference)
                .await
                .map_err(|error| format!("failed to resolve durable image {uri}: {error}"))?;
        images.push(MessageImage {
            mime_type: image.mime_type.to_string(),
            bytes: image.bytes,
        });
    }
    Ok(MessageContent { text, images })
}

impl MessageContent {
    /// Whether this message would serialize to nothing a provider accepts: no
    /// usable text and no images.
    pub(crate) fn is_blank(&self) -> bool {
        self.text.trim().is_empty() && self.images.is_empty()
    }

    pub(crate) fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            images: Vec::new(),
        }
    }

    pub(crate) fn with_text(&self, text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            images: self.images.clone(),
        }
    }

    /// Resolve every unique stable project-image URI in textual order. Missing
    /// or corrupt durable resources are errors; native delivery never silently
    /// degrades to text-only.
    #[cfg(test)]
    pub(crate) fn resolve(
        text: impl Into<String>,
        resolver: &dyn StableImageResolver,
    ) -> Result<Self, String> {
        let text = text.into();
        let mut seen = std::collections::HashSet::new();
        let mut images = Vec::new();
        for found in cairn_common::uri::scan_stored_images(&text) {
            if seen.insert(found.uri.clone()) {
                images.push(resolver.resolve(&found.uri)?);
            }
        }
        Ok(Self { text, images })
    }
}

/// Why a wholly empty message is refused rather than serialized.
const EMPTY_MESSAGE_REFUSAL: &str = "Refusing to send a user message with no text and no images: an empty text block is rejected by the provider, and a harness that persists one makes every later resume of the conversation fail the same way.";

/// Serialize common content into Claude stream-json's native content blocks.
///
/// Never emits an empty text block. The provider refuses one outright (`text
/// content blocks must be non-empty`), and a harness that persists it converts a
/// single bad turn into a permanently unresumable session (CAIRN-3263), so blank
/// text is dropped when images carry the message and refused when nothing else
/// does.
pub(crate) fn build_message_content(content: &MessageContent) -> Result<Value, String> {
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

/// Serialize common content into Codex app-server's verified `UserInput`
/// schema. Codex accepts durable bytes as a data URL in a native image item.
/// Empty text is elided or refused for the same reason as above.
pub(crate) fn build_codex_input(content: &MessageContent) -> Result<Value, String> {
    let has_text = !content.text.trim().is_empty();
    if !has_text && content.images.is_empty() {
        return Err(EMPTY_MESSAGE_REFUSAL.to_string());
    }

    let mut input = Vec::with_capacity(content.images.len() + 1);
    if has_text {
        input.push(json!({ "type": "text", "text": content.text }));
    }
    input.extend(content.images.iter().map(|image| {
        let data = base64::engine::general_purpose::STANDARD.encode(&image.bytes);
        json!({
            "type": "image",
            "url": format!("data:{};base64,{}", image.mime_type, data),
        })
    }));
    Ok(Value::Array(input))
}

/// Send a user message to Claude via stdin using pre-resolved native parts.
pub(crate) fn send_user_message(
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

    struct FixtureResolver;

    impl StableImageResolver for FixtureResolver {
        fn resolve(&self, _uri: &str) -> Result<MessageImage, String> {
            Ok(MessageImage {
                mime_type: "image/png".to_string(),
                bytes: b"native-image-payload".to_vec(),
            })
        }
    }

    fn assert_no_text_contains(value: &Value, needle: &str) {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(text)) = map.get("text") {
                    assert!(!text.contains(needle), "encoded image leaked into text");
                }
                map.values()
                    .for_each(|v| assert_no_text_contains(v, needle));
            }
            Value::Array(values) => values
                .iter()
                .for_each(|v| assert_no_text_contains(v, needle)),
            _ => {}
        }
    }

    #[test]
    fn stable_uri_resolves_once_and_serializes_as_native_claude_image() {
        let uri = format!("cairn://p/CAIRN/images/{}", "a".repeat(64));
        let content =
            MessageContent::resolve(format!("![one]({uri}) again {uri}"), &FixtureResolver)
                .unwrap();
        assert_eq!(content.images.len(), 1);

        let serialized = build_message_content(&content).unwrap();
        assert_eq!(serialized[1]["type"], "image");
        assert_eq!(serialized[1]["source"]["media_type"], "image/png");
        let encoded = serialized[1]["source"]["data"].as_str().unwrap();
        assert_no_text_contains(&serialized, encoded);
    }

    #[test]
    fn stable_uri_serializes_as_native_codex_image_without_base64_text() {
        let content = MessageContent {
            text: "inspect the attached image".to_string(),
            images: vec![MessageImage {
                mime_type: "image/png".to_string(),
                bytes: b"native-image-payload".to_vec(),
            }],
        };
        let serialized = build_codex_input(&content).unwrap();
        assert_eq!(serialized[1]["type"], "image");
        let data_url = serialized[1]["url"].as_str().unwrap();
        assert!(data_url.starts_with("data:image/png;base64,"));
        let encoded = data_url.split_once(',').unwrap().1;
        assert_no_text_contains(&serialized, encoded);
    }

    #[test]
    fn stable_uri_resolution_is_fail_closed() {
        struct Missing;
        impl StableImageResolver for Missing {
            fn resolve(&self, uri: &str) -> Result<MessageImage, String> {
                Err(format!("missing image: {uri}"))
            }
        }
        let uri = format!("cairn://p/CAIRN/images/{}", "b".repeat(64));
        assert!(MessageContent::resolve(uri, &Missing)
            .unwrap_err()
            .contains("missing image"));
    }

    #[test]
    fn empty_message_is_refused_by_both_serializers_and_never_reaches_stdin() {
        for blank in ["", "   ", "\n\t "] {
            let content = MessageContent::text(blank);
            assert!(build_message_content(&content).is_err());
            assert!(build_codex_input(&content).is_err());

            let mut buffer = Cursor::new(Vec::new());
            assert!(send_user_message(&mut buffer, "session-123", &content, None).is_err());
            assert!(
                buffer.into_inner().is_empty(),
                "a refused message must not be written"
            );
        }
    }

    #[test]
    fn blank_text_beside_an_image_serializes_as_the_image_alone() {
        let content = MessageContent {
            text: "  ".to_string(),
            images: vec![MessageImage {
                mime_type: "image/png".to_string(),
                bytes: b"native-image-payload".to_vec(),
            }],
        };

        let claude = build_message_content(&content).unwrap();
        assert_eq!(claude.as_array().unwrap().len(), 1);
        assert_eq!(claude[0]["type"], "image");

        let codex = build_codex_input(&content).unwrap();
        assert_eq!(codex.as_array().unwrap().len(), 1);
        assert_eq!(codex[0]["type"], "image");
    }

    #[test]
    fn test_send_user_message() {
        let mut buffer = Cursor::new(Vec::new());

        send_user_message(
            &mut buffer,
            "session-123",
            &MessageContent::text("Hello, Claude!"),
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

        send_user_message(
            &mut buffer,
            "session-456",
            &MessageContent::text("Subagent message"),
            Some("toolu_abc123"),
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
