//! OpenAI-compatible chat-completions wire types: request/response DTOs, the streaming
//! chunk shapes, and the aggregator that rebuilds a response from SSE deltas.
//! Pure data plus (de)serialization; no HTTP or orchestrator state. The usage
//! payload is the neutral `http_loop::TurnUsage` (its field names/serde match the
//! OpenAI/OpenRouter wire), so a streamed response deserializes straight into it.

use crate::agent_process::stdin::MessageContent;
use crate::backends::http_loop::TurnUsage;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChatMessage {
    pub(crate) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content: Option<ChatContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tool_calls: Option<Vec<ToolCall>>,
    // Original structured reasoning, replayed verbatim and in order on the
    // assistant message that requested a tool. Anthropic providers error if the
    // thinking block is missing before a tool_use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reasoning_details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ChatContent {
    Text(String),
    Parts(Vec<ChatContentPart>),
}

impl ChatContent {
    pub(crate) fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Parts(_) => None,
        }
    }

    pub(crate) fn estimated_chars(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Parts(parts) => parts
                .iter()
                .map(|part| match part {
                    ChatContentPart::Text { text } => text.len(),
                    ChatContentPart::ImageUrl { image_url } => image_url.url.len(),
                })
                .sum(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ChatContentPart {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ImageUrl {
    pub(crate) url: String,
}

impl ChatMessage {
    pub(crate) fn system(content: String) -> Self {
        Self {
            role: "system".to_string(),
            content: Some(ChatContent::Text(content)),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
        }
    }
    pub(crate) fn user(content: String) -> Self {
        Self::user_content(&MessageContent::text(content))
    }

    /// Never emits an empty text part beside images: the provider rejects an
    /// empty text block, and a persisted one makes every later replay of the
    /// conversation fail the same way (CAIRN-3263). A message with no text and
    /// no images is refused upstream, where the turn can fail cleanly.
    pub(crate) fn user_content(content: &MessageContent) -> Self {
        let wire_content = if content.images.is_empty() {
            ChatContent::Text(content.text.clone())
        } else {
            let mut parts = Vec::with_capacity(content.images.len() + 1);
            if !content.text.trim().is_empty() {
                parts.push(ChatContentPart::Text {
                    text: content.text.clone(),
                });
            }
            parts.extend(content.images.iter().map(|image| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(&image.bytes);
                ChatContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: format!("data:{};base64,{encoded}", image.mime_type),
                    },
                }
            }));
            ChatContent::Parts(parts)
        };
        Self {
            role: "user".to_string(),
            content: Some(wire_content),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
        }
    }

    pub(crate) fn tool(tool_call_id: String, content: String) -> Self {
        Self {
            role: "tool".to_string(),
            content: Some(ChatContent::Text(content)),
            tool_call_id: Some(tool_call_id),
            tool_calls: None,
            reasoning_details: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChatResponse {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) model: Option<String>,
    pub(crate) choices: Vec<ChatChoice>,
    #[serde(default)]
    pub(crate) usage: Option<TurnUsage>,
    #[serde(default)]
    pub(crate) streamed_text: bool,
    // The generation's terminal finish_reason (e.g. "tool_calls", "stop",
    // "length"). "length" flags an output-token cutoff that may truncate the
    // last tool call. Constructed from the stream; never deserialized from wire.
    #[serde(default)]
    pub(crate) finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChatChoice {
    pub(crate) message: ChatMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ToolCall {
    pub(crate) id: String,
    #[serde(default = "default_function_type")]
    pub(crate) r#type: String,
    pub(crate) function: ToolFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ToolFunction {
    pub(crate) name: String,
    pub(crate) arguments: String,
}

pub(crate) fn default_function_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChatStreamChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    pub(crate) choices: Vec<ChatStreamChoice>,
    #[serde(default)]
    usage: Option<TurnUsage>,
    #[serde(default)]
    error: Option<StreamError>,
}

impl ChatStreamChunk {
    /// Detect an in-band error chunk: OpenRouter delivers post-stream-start
    /// errors as a top-level `error` object (HTTP stays 200) and/or a choice
    /// whose `finish_reason` is `"error"`. Returns the provider message.
    pub(crate) fn error_message(&self) -> Option<String> {
        if let Some(error) = &self.error {
            return Some(error.message.clone());
        }
        if self
            .choices
            .iter()
            .any(|choice| choice.finish_reason.as_deref() == Some("error"))
        {
            return Some("OpenRouter stream reported finish_reason=error".to_string());
        }
        None
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StreamError {
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) code: Option<Value>,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChatStreamChoice {
    #[serde(default)]
    pub(crate) delta: ChatStreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ChatStreamDelta {
    #[serde(default)]
    pub(crate) content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<StreamingToolCallDelta>>,
    #[serde(default)]
    pub(crate) reasoning: Option<String>,
    // Kept verbatim as raw JSON (not reshaped into typed structs) so order and
    // round-trip fidelity for signature/encrypted/format fields are preserved.
    #[serde(default)]
    pub(crate) reasoning_details: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct StreamingToolCallDelta {
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    function: Option<StreamingToolFunctionDelta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct StreamingToolFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct StreamingAggregate {
    pub(crate) id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) text: String,
    pub(crate) reasoning: String,
    reasoning_details: Vec<ReasoningDetailBuilder>,
    tool_calls: Vec<StreamingToolCallBuilder>,
    pub(crate) usage: Option<TurnUsage>,
    finish_reason: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct StreamingToolCallBuilder {
    id: Option<String>,
    r#type: Option<String>,
    name: String,
    arguments: String,
}

/// Accumulates one streaming `reasoning_details` block. OpenRouter streams these
/// incrementally and keyed by `index`: text/summary/signature/data arrive as
/// string deltas across chunks, while type/id/format are sent once. Appending
/// each delta as its own array element instead of merging by index splits a
/// thinking block's text from its signature, which Anthropic rejects on replay
/// with "Invalid `signature` in `thinking` block".
#[derive(Debug, Default)]
pub(crate) struct ReasoningDetailBuilder {
    /// Set-once metadata (type, id, format, index, and any unrecognized key).
    meta: serde_json::Map<String, Value>,
    text: Option<String>,
    summary: Option<String>,
    signature: Option<String>,
    data: Option<String>,
}

impl ReasoningDetailBuilder {
    fn apply(&mut self, delta: &Value) {
        let Some(obj) = delta.as_object() else {
            return;
        };
        for (key, value) in obj {
            match key.as_str() {
                "text" => append_reasoning_field(&mut self.text, value),
                "summary" => append_reasoning_field(&mut self.summary, value),
                "signature" => append_reasoning_field(&mut self.signature, value),
                "data" => append_reasoning_field(&mut self.data, value),
                _ => {
                    self.meta.insert(key.clone(), value.clone());
                }
            }
        }
    }

    fn to_value(&self) -> Value {
        let mut obj = self.meta.clone();
        if let Some(text) = &self.text {
            obj.insert("text".to_string(), Value::String(text.clone()));
        }
        if let Some(summary) = &self.summary {
            obj.insert("summary".to_string(), Value::String(summary.clone()));
        }
        if let Some(signature) = &self.signature {
            obj.insert("signature".to_string(), Value::String(signature.clone()));
        }
        if let Some(data) = &self.data {
            obj.insert("data".to_string(), Value::String(data.clone()));
        }
        Value::Object(obj)
    }
}

fn append_reasoning_field(slot: &mut Option<String>, value: &Value) {
    if let Some(text) = value.as_str() {
        slot.get_or_insert_with(String::new).push_str(text);
    }
}

impl StreamingAggregate {
    /// The terminal `finish_reason` this generation reported, if it reported
    /// one. Its absence once the connection has closed is what distinguishes a
    /// stream that finished from one that was cut off mid-generation.
    pub(crate) fn finish_reason(&self) -> Option<&str> {
        self.finish_reason.as_deref()
    }

    pub(crate) fn apply_chunk(&mut self, chunk: &ChatStreamChunk) {
        if self.id.is_none() {
            self.id = chunk.id.clone();
        }
        if self.model.is_none() {
            self.model = chunk.model.clone();
        }
        if chunk.usage.is_some() {
            self.usage = chunk.usage.clone();
        }
        for choice in &chunk.choices {
            if let Some(reason) = &choice.finish_reason {
                self.finish_reason = Some(reason.clone());
            }
            if let Some(content) = choice.delta.content.as_deref() {
                self.text.push_str(content);
            }
            if let Some(reasoning) = choice.delta.reasoning.as_deref() {
                self.reasoning.push_str(reasoning);
            }
            if let Some(details) = &choice.delta.reasoning_details {
                for detail in details {
                    // OpenRouter keys each block by `index`; merge deltas into the
                    // matching builder so text and its signature stay one block.
                    let index = detail.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                    while self.reasoning_details.len() <= index {
                        self.reasoning_details
                            .push(ReasoningDetailBuilder::default());
                    }
                    self.reasoning_details[index].apply(detail);
                }
            }
            if let Some(tool_calls) = &choice.delta.tool_calls {
                for delta in tool_calls {
                    while self.tool_calls.len() <= delta.index {
                        self.tool_calls.push(StreamingToolCallBuilder::default());
                    }
                    let builder = &mut self.tool_calls[delta.index];
                    if let Some(id) = delta.id.as_deref() {
                        builder.id = Some(id.to_string());
                    }
                    if let Some(kind) = delta.r#type.as_deref() {
                        builder.r#type = Some(kind.to_string());
                    }
                    if let Some(function) = &delta.function {
                        if let Some(name) = function.name.as_deref() {
                            builder.name.push_str(name);
                        }
                        if let Some(arguments) = function.arguments.as_deref() {
                            builder.arguments.push_str(arguments);
                        }
                    }
                }
            }
        }
    }

    fn tool_calls(&self) -> Vec<ToolCall> {
        self.tool_calls
            .iter()
            .enumerate()
            .filter_map(|(index, builder)| {
                if builder.name.is_empty() {
                    log::warn!(
                        "OpenRouter dropping streamed tool call #{index} with empty function name (id={:?}, {} arg bytes)",
                        builder.id,
                        builder.arguments.len()
                    );
                    return None;
                }
                Some(ToolCall {
                    id: builder
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("openrouter-tool-{index}")),
                    r#type: builder.r#type.clone().unwrap_or_else(default_function_type),
                    function: ToolFunction {
                        name: builder.name.clone(),
                        arguments: builder.arguments.clone(),
                    },
                })
            })
            .collect()
    }

    pub(crate) fn reasoning_detail_values(&self) -> Vec<Value> {
        self.reasoning_details
            .iter()
            .map(ReasoningDetailBuilder::to_value)
            .collect()
    }

    pub(crate) fn into_response(self, streamed_text: bool) -> ChatResponse {
        let tool_calls = self.tool_calls();
        let reasoning_details = if self.reasoning_details.is_empty() {
            None
        } else {
            Some(Value::Array(self.reasoning_detail_values()))
        };
        ChatResponse {
            id: self.id,
            model: self.model,
            choices: vec![ChatChoice {
                message: ChatMessage {
                    role: "assistant".to_string(),
                    content: if self.text.is_empty() {
                        None
                    } else {
                        Some(ChatContent::Text(self.text))
                    },
                    tool_call_id: None,
                    tool_calls: if tool_calls.is_empty() {
                        None
                    } else {
                        Some(tool_calls)
                    },
                    reasoning_details,
                },
            }],
            usage: self.usage,
            streamed_text,
            finish_reason: self.finish_reason,
        }
    }
}
