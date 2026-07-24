//! OpenRouter chat-completions wire types: request/response DTOs, the streaming
//! chunk shapes, and the aggregator that rebuilds a response from SSE deltas.
//! Pure data plus (de)serialization; no HTTP or orchestrator state. The usage
//! payload is the neutral `http_loop::TurnUsage` (its field names/serde match the
//! OpenAI/OpenRouter wire), so a streamed response deserializes straight into it.

use crate::backends::http_loop::TurnUsage;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ChatMessage {
    pub(super) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tool_calls: Option<Vec<ToolCall>>,
    // Original structured reasoning, replayed verbatim and in order on the
    // assistant message that requested a tool. Anthropic providers error if the
    // thinking block is missing before a tool_use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) reasoning_details: Option<Value>,
}

impl ChatMessage {
    pub(super) fn system(content: String) -> Self {
        Self {
            role: "system".to_string(),
            content: Some(content),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
        }
    }
    pub(super) fn user(content: String) -> Self {
        Self {
            role: "user".to_string(),
            content: Some(content),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
        }
    }
    pub(super) fn tool(tool_call_id: String, content: String) -> Self {
        Self {
            role: "tool".to_string(),
            content: Some(content),
            tool_call_id: Some(tool_call_id),
            tool_calls: None,
            reasoning_details: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ChatResponse {
    #[serde(default)]
    pub(super) id: Option<String>,
    #[serde(default)]
    pub(super) model: Option<String>,
    pub(super) choices: Vec<ChatChoice>,
    #[serde(default)]
    pub(super) usage: Option<TurnUsage>,
    #[serde(default)]
    pub(super) streamed_text: bool,
    // The generation's terminal finish_reason (e.g. "tool_calls", "stop",
    // "length"). "length" flags an output-token cutoff that may truncate the
    // last tool call. Constructed from the stream; never deserialized from wire.
    #[serde(default)]
    pub(super) finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ChatChoice {
    pub(super) message: ChatMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ToolCall {
    pub(super) id: String,
    #[serde(default = "default_function_type")]
    pub(super) r#type: String,
    pub(super) function: ToolFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ToolFunction {
    pub(super) name: String,
    pub(super) arguments: String,
}

pub(super) fn default_function_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ChatStreamChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    pub(super) choices: Vec<ChatStreamChoice>,
    #[serde(default)]
    usage: Option<TurnUsage>,
    #[serde(default)]
    error: Option<StreamError>,
}

impl ChatStreamChunk {
    /// Detect an in-band error chunk: OpenRouter delivers post-stream-start
    /// errors as a top-level `error` object (HTTP stays 200) and/or a choice
    /// whose `finish_reason` is `"error"`. Returns the provider message.
    pub(super) fn error_message(&self) -> Option<String> {
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
pub(super) struct StreamError {
    #[serde(default)]
    #[allow(dead_code)]
    pub(super) code: Option<Value>,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ChatStreamChoice {
    #[serde(default)]
    pub(super) delta: ChatStreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct ChatStreamDelta {
    #[serde(default)]
    pub(super) content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<StreamingToolCallDelta>>,
    #[serde(default)]
    pub(super) reasoning: Option<String>,
    // Kept verbatim as raw JSON (not reshaped into typed structs) so order and
    // round-trip fidelity for signature/encrypted/format fields are preserved.
    #[serde(default)]
    pub(super) reasoning_details: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct StreamingToolCallDelta {
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    function: Option<StreamingToolFunctionDelta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(super) struct StreamingToolFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct StreamingAggregate {
    pub(super) id: Option<String>,
    pub(super) model: Option<String>,
    pub(super) text: String,
    pub(super) reasoning: String,
    reasoning_details: Vec<ReasoningDetailBuilder>,
    tool_calls: Vec<StreamingToolCallBuilder>,
    pub(super) usage: Option<TurnUsage>,
    finish_reason: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct StreamingToolCallBuilder {
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
pub(super) struct ReasoningDetailBuilder {
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
    pub(super) fn apply_chunk(&mut self, chunk: &ChatStreamChunk) {
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

    pub(super) fn reasoning_detail_values(&self) -> Vec<Value> {
        self.reasoning_details
            .iter()
            .map(ReasoningDetailBuilder::to_value)
            .collect()
    }

    pub(super) fn into_response(self, streamed_text: bool) -> ChatResponse {
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
                        Some(self.text)
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
