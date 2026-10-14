//! Resolved input and replay data; no database access.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Provider-neutral user message content. Image bytes are resolved from durable
/// storage before a backend serializes the message; encoded bytes never enter a
/// text part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageContent {
    pub text: String,
    pub images: Vec<MessageImage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageImage {
    pub mime_type: String,
    pub bytes: Vec<u8>,
}

impl MessageContent {
    /// Whether this message would serialize to nothing a provider accepts: no
    /// usable text and no images.
    pub fn is_blank(&self) -> bool {
        self.text.trim().is_empty() && self.images.is_empty()
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            images: Vec::new(),
        }
    }

    pub fn with_text(&self, text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            images: self.images.clone(),
        }
    }
}

/// Why the CLI input builders refuse a wholly empty message rather than
/// serialize it: an empty text block is rejected by the provider, and a harness
/// that persists one makes every later resume of the conversation fail the same
/// way.
pub(crate) const EMPTY_MESSAGE_REFUSAL: &str = "Refusing to send a user message with no text and no images: an empty text block is rejected by the provider, and a harness that persists one makes every later resume of the conversation fail the same way.";

/// A single tool use extracted from an assistant message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolUseInfo {
    pub id: String,
    pub name: String,
    pub input: Value,
}

/// Only the fields needed to replay a decoded transcript event on the wire.
/// Core projects its durable event into this value after loading history.
#[derive(Debug, Clone, Default)]
pub struct ReplayEvent {
    pub content: Option<String>,
    pub tool_uses: Option<Vec<ToolUseInfo>>,
    pub tool_use_id: Option<String>,
    pub tool_result: Option<String>,
    pub raw: Option<Value>,
}

/// Stands in for a tool result that was stored empty. A blank content block is
/// rejectable by every family here, and a dropped result would orphan its call,
/// so the empty result is stated rather than sent or removed.
pub const EMPTY_TOOL_RESULT: &str = "The tool returned no output.";

/// Stands in for a call whose result never landed, because the turn was
/// interrupted between dispatch and persistence.
pub const INTERRUPTED_TOOL_RESULT: &str = "Interrupted before the tool result was recorded.";

/// Answers the calls a suspended turn left unrun: every call needs a result
/// before the conversation can continue, and these will not run in this turn.
pub const SKIPPED_TOOL_RESULT: &str =
    "Skipped: turn suspended pending user response / task result.";

/// The structured reasoning an assistant event stored, if it stored any.
///
/// Writers persist this under either casing, and store `null` or `[]` when the
/// turn did no reasoning; both read as absent so a non-reasoning turn does not
/// replay an empty reasoning field.
pub fn stored_reasoning_details(event: &ReplayEvent) -> Option<serde_json::Value> {
    event
        .raw
        .as_ref()
        .and_then(|raw| {
            raw.get("reasoning_details")
                .or_else(|| raw.get("reasoningDetails"))
        })
        .filter(|value| {
            !value.is_null()
                && !matches!(value, serde_json::Value::Array(items) if items.is_empty())
        })
        .cloned()
}
