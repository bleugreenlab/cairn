//! The `UserInput` a Codex turn starts with.

use base64::Engine;
use serde_json::{json, Value};

use crate::replay::{MessageContent, EMPTY_MESSAGE_REFUSAL};

/// Serialize common content into Codex app-server's verified `UserInput`
/// schema. Codex accepts durable bytes as a data URL in a native image item.
/// Empty text is elided beside an image and a wholly empty message refused, for
/// the same reason as the Claude builder: a persisted empty text block makes
/// every later resume fail.
pub fn build_codex_input(content: &MessageContent) -> Result<Value, String> {
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
