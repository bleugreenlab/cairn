//! Anthropic Messages wire types: request/response content blocks, the named SSE
//! event shapes, and the aggregator that rebuilds a message from block deltas.
//! Pure data plus (de)serialization; no HTTP or orchestrator state.
//!
//! Shapes are taken from recorded OpenCode Go `/zen/go/v1/messages` traffic, not
//! from vendor documentation: a gateway's dialect is the contract Cairn actually
//! has to parse.

use crate::agent_process::stdin::MessageContent;
use crate::backends::http_loop::TurnUsage;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub(crate) const SYSTEM_ROLE: &str = "system";
pub(crate) const USER_ROLE: &str = "user";
pub(crate) const ASSISTANT_ROLE: &str = "assistant";

/// One message in the Messages conversation.
///
/// Anthropic carries the system prompt as a TOP-LEVEL request field rather than
/// a message, but the neutral turn loop hands the adapter a flat `Vec<Message>`.
/// So a system prompt rides here as a `system`-role message and
/// [`super::http::build_body`] lifts it out into the request's `system` field.
/// Nothing else in this module treats `system` as a legal wire role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessagesMessage {
    pub(crate) role: String,
    pub(crate) content: Vec<ContentBlock>,
}

impl MessagesMessage {
    pub(crate) fn system(text: String) -> Self {
        Self {
            role: SYSTEM_ROLE.to_string(),
            content: vec![ContentBlock::Text { text }],
        }
    }

    pub(crate) fn assistant(content: Vec<ContentBlock>) -> Self {
        Self {
            role: ASSISTANT_ROLE.to_string(),
            content,
        }
    }

    /// A user turn's text and images as content blocks.
    ///
    /// Never emits an empty text block beside images: the provider rejects one,
    /// and a persisted empty block makes every later replay of the conversation
    /// fail the same way (CAIRN-3263).
    pub(crate) fn user_content(content: &MessageContent) -> Self {
        let mut blocks = Vec::with_capacity(content.images.len() + 1);
        if !content.text.trim().is_empty() {
            blocks.push(ContentBlock::Text {
                text: content.text.clone(),
            });
        }
        blocks.extend(content.images.iter().map(|image| ContentBlock::Image {
            source: ImageSource {
                kind: "base64".to_string(),
                media_type: image.mime_type.clone(),
                data: base64::engine::general_purpose::STANDARD.encode(&image.bytes),
            },
        }));
        Self {
            role: USER_ROLE.to_string(),
            content: blocks,
        }
    }

    /// Tool results ride in a USER message on this protocol — there is no `tool`
    /// role — which is why pairing a call to its result is a message-level move
    /// here rather than a per-message one.
    pub(crate) fn tool_result(tool_use_id: String, content: String) -> Self {
        Self {
            role: USER_ROLE.to_string(),
            content: vec![ContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error: false,
            }],
        }
    }

    pub(crate) fn tool_use_ids(&self) -> Vec<String> {
        self.content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolUse { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn estimated_chars(&self) -> usize {
        self.content.iter().map(ContentBlock::estimated_chars).sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContentBlock {
    Text {
        text: String,
    },
    /// Extended thinking. The `signature` is load-bearing on replay: Anthropic
    /// rejects a thinking block whose signature is missing or altered, so it is
    /// round-tripped verbatim rather than regenerated.
    Thinking {
        thinking: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    RedactedThinking {
        data: String,
    },
    Image {
        source: ImageSource,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default, skip_serializing_if = "is_false")]
        is_error: bool,
    },
    /// A block type this build has no mapping for. Kept so an unrecognized block
    /// cannot fail the whole parse, and dropped before anything is sent back:
    /// echoing a block Cairn cannot describe would corrupt the replay.
    #[serde(other)]
    Unknown,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl ContentBlock {
    pub(crate) fn is_replayable(&self) -> bool {
        !matches!(self, ContentBlock::Unknown)
    }

    pub(crate) fn estimated_chars(&self) -> usize {
        match self {
            ContentBlock::Text { text } => text.len(),
            ContentBlock::Thinking { thinking, .. } => thinking.len(),
            ContentBlock::RedactedThinking { data } => data.len(),
            ContentBlock::Image { source } => source.data.len(),
            ContentBlock::ToolUse { name, input, .. } => name.len() + input.to_string().len(),
            ContentBlock::ToolResult { content, .. } => content.len(),
            ContentBlock::Unknown => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ImageSource {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) media_type: String,
    pub(crate) data: String,
}

// === Usage ===

/// Anthropic's usage object. Its input components are DISJOINT (`input_tokens`
/// excludes cache reads and writes), which is why it is mapped through
/// [`TurnUsage::from_anthropic`] rather than deserialized straight into the
/// neutral shape.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct MessagesUsage {
    #[serde(default)]
    pub(crate) input_tokens: Option<i32>,
    #[serde(default)]
    pub(crate) output_tokens: Option<i32>,
    #[serde(default)]
    pub(crate) cache_creation_input_tokens: Option<i32>,
    #[serde(default)]
    pub(crate) cache_read_input_tokens: Option<i32>,
}

impl MessagesUsage {
    pub(crate) fn into_turn_usage(self, cost: Option<f64>) -> TurnUsage {
        TurnUsage::from_anthropic(
            self.input_tokens,
            self.output_tokens,
            self.cache_read_input_tokens,
            self.cache_creation_input_tokens,
            cost,
        )
    }

    /// Fold a later usage report over an earlier one.
    ///
    /// `message_start` announces zeroed counts and `message_delta` carries the
    /// real ones, so later non-zero values win while anything the final report
    /// omits keeps the value already seen.
    fn merge(&mut self, other: &MessagesUsage) {
        if other.input_tokens.is_some_and(|tokens| tokens > 0) || self.input_tokens.is_none() {
            self.input_tokens = other.input_tokens.or(self.input_tokens);
        }
        if other.output_tokens.is_some_and(|tokens| tokens > 0) || self.output_tokens.is_none() {
            self.output_tokens = other.output_tokens.or(self.output_tokens);
        }
        if other.cache_read_input_tokens.is_some() {
            self.cache_read_input_tokens = other.cache_read_input_tokens;
        }
        if other.cache_creation_input_tokens.is_some() {
            self.cache_creation_input_tokens = other.cache_creation_input_tokens;
        }
    }
}

/// The gateway reports cost as a JSON string (`"cost":"0"`) where the rest of
/// the wire uses numbers, so both spellings are accepted.
pub(crate) fn parse_cost(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

// === Responses ===

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct MessagesResponse {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) content: Vec<ContentBlock>,
    #[serde(default)]
    pub(crate) stop_reason: Option<String>,
    #[serde(default)]
    pub(crate) usage: Option<MessagesUsage>,
    #[serde(default)]
    pub(crate) cost: Option<Value>,
    /// True when the assistant's text already reached the frontend as a live
    /// stream. Constructed from the stream; never present on the wire.
    #[serde(default)]
    pub(crate) streamed_text: bool,
    /// Tool arguments exactly as the model emitted them, keyed by tool-use id.
    ///
    /// [`ContentBlock::ToolUse`] holds parsed JSON because that is what the wire
    /// requires on replay, but a truncated argument string does not survive a
    /// parse. The raw text is kept beside it so the turn loop's repair path sees
    /// the truncation instead of a silently emptied payload.
    #[serde(skip)]
    pub(crate) raw_tool_input: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ApiError {
    #[serde(default)]
    pub(crate) r#type: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<String>,
}

// === Streaming events ===

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StreamEvent {
    MessageStart {
        message: StreamStartMessage,
    },
    ContentBlockStart {
        index: usize,
        content_block: ContentBlock,
    },
    ContentBlockDelta {
        index: usize,
        delta: BlockDelta,
    },
    ContentBlockStop {
        #[allow(dead_code)]
        index: usize,
    },
    MessageDelta {
        #[serde(default)]
        delta: MessageDeltaFields,
        #[serde(default)]
        usage: Option<MessagesUsage>,
    },
    MessageStop,
    /// Keep-alive. The gateway also attaches the turn's cost to the final one.
    Ping {
        #[serde(default)]
        cost: Option<Value>,
    },
    /// An error delivered in-band after the stream started (HTTP stays 200).
    Error {
        error: ApiError,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StreamStartMessage {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) usage: Option<MessagesUsage>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct MessageDeltaFields {
    #[serde(default)]
    pub(crate) stop_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BlockDelta {
    TextDelta {
        text: String,
    },
    InputJsonDelta {
        partial_json: String,
    },
    ThinkingDelta {
        thinking: String,
    },
    SignatureDelta {
        signature: String,
    },
    #[serde(other)]
    Unknown,
}

// === Streaming aggregate ===

#[derive(Debug)]
enum BlockBuilder {
    Text(String),
    Thinking {
        thinking: String,
        signature: Option<String>,
    },
    RedactedThinking(String),
    ToolUse {
        id: String,
        name: String,
        json: String,
    },
    Ignored,
}

/// Rebuilds one assistant message from its streamed blocks.
#[derive(Debug, Default)]
pub(crate) struct StreamingMessage {
    pub(crate) id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) stop_reason: Option<String>,
    pub(crate) usage: Option<MessagesUsage>,
    pub(crate) cost: Option<f64>,
    /// Whether the protocol's terminal event was actually observed.
    ///
    /// A chunked HTTP response can be closed by a proxy or upstream without
    /// producing a read error, so "the stream ended" is NOT evidence that the
    /// message finished. Without this, a truncated generation would be stored as
    /// a success and a half-assembled tool call could be dispatched with no
    /// trustworthy stop reason behind it.
    stopped: bool,
    blocks: Vec<BlockBuilder>,
}

impl StreamingMessage {
    pub(crate) fn apply(&mut self, event: &StreamEvent) {
        match event {
            StreamEvent::MessageStart { message } => {
                if self.id.is_none() {
                    self.id = message.id.clone();
                }
                if self.model.is_none() {
                    self.model = message.model.clone();
                }
                if let Some(usage) = &message.usage {
                    self.usage.get_or_insert_with(Default::default).merge(usage);
                }
            }
            StreamEvent::ContentBlockStart {
                index,
                content_block,
            } => {
                let builder = match content_block {
                    ContentBlock::Text { text } => BlockBuilder::Text(text.clone()),
                    ContentBlock::Thinking {
                        thinking,
                        signature,
                    } => BlockBuilder::Thinking {
                        thinking: thinking.clone(),
                        signature: signature.clone(),
                    },
                    ContentBlock::RedactedThinking { data } => {
                        BlockBuilder::RedactedThinking(data.clone())
                    }
                    ContentBlock::ToolUse { id, name, input } => BlockBuilder::ToolUse {
                        id: id.clone(),
                        name: name.clone(),
                        // A non-streaming start can carry the whole input; a
                        // streaming one sends `{}` and fills it via deltas.
                        json: match input {
                            Value::Object(map) if map.is_empty() => String::new(),
                            other => other.to_string(),
                        },
                    },
                    _ => BlockBuilder::Ignored,
                };
                self.set_block(*index, builder);
            }
            StreamEvent::ContentBlockDelta { index, delta } => {
                let Some(builder) = self.blocks.get_mut(*index) else {
                    return;
                };
                match (builder, delta) {
                    (BlockBuilder::Text(text), BlockDelta::TextDelta { text: delta }) => {
                        text.push_str(delta)
                    }
                    (
                        BlockBuilder::ToolUse { json, .. },
                        BlockDelta::InputJsonDelta { partial_json },
                    ) => json.push_str(partial_json),
                    (
                        BlockBuilder::Thinking { thinking, .. },
                        BlockDelta::ThinkingDelta { thinking: delta },
                    ) => thinking.push_str(delta),
                    (
                        BlockBuilder::Thinking { signature, .. },
                        BlockDelta::SignatureDelta { signature: delta },
                    ) => signature.get_or_insert_with(String::new).push_str(delta),
                    _ => {}
                }
            }
            StreamEvent::MessageDelta { delta, usage } => {
                if let Some(reason) = &delta.stop_reason {
                    self.stop_reason = Some(reason.clone());
                }
                if let Some(usage) = usage {
                    self.usage.get_or_insert_with(Default::default).merge(usage);
                }
            }
            StreamEvent::Ping { cost } => {
                if let Some(cost) = parse_cost(cost.as_ref()) {
                    self.cost = Some(cost);
                }
            }
            StreamEvent::MessageStop => self.stopped = true,
            StreamEvent::ContentBlockStop { .. }
            | StreamEvent::Error { .. }
            | StreamEvent::Unknown => {}
        }
    }

    /// Whether `message_stop` arrived. The gateway sends a cost-carrying ping
    /// after it, so a stream that ends right after `message_stop` is still
    /// complete — but one that ends before it is not.
    pub(crate) fn saw_terminal(&self) -> bool {
        self.stopped
    }

    fn set_block(&mut self, index: usize, builder: BlockBuilder) {
        while self.blocks.len() <= index {
            self.blocks.push(BlockBuilder::Ignored);
        }
        self.blocks[index] = builder;
    }

    /// Everything the assistant said as text, in block order.
    pub(crate) fn text(&self) -> String {
        self.blocks
            .iter()
            .filter_map(|block| match block {
                BlockBuilder::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Everything the assistant thought, for the live thinking stream.
    pub(crate) fn thinking(&self) -> String {
        self.blocks
            .iter()
            .filter_map(|block| match block {
                BlockBuilder::Thinking { thinking, .. } => Some(thinking.as_str()),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn into_response(self, streamed_text: bool) -> MessagesResponse {
        let mut content = Vec::with_capacity(self.blocks.len());
        let mut raw_tool_input = HashMap::new();
        for block in self.blocks {
            match block {
                // An empty text block is rejected on replay, so a block that
                // opened but never received a delta is dropped rather than sent.
                BlockBuilder::Text(text) if text.is_empty() => {}
                BlockBuilder::Text(text) => content.push(ContentBlock::Text { text }),
                BlockBuilder::Thinking {
                    thinking,
                    signature,
                } => content.push(ContentBlock::Thinking {
                    thinking,
                    signature,
                }),
                BlockBuilder::RedactedThinking(data) => {
                    content.push(ContentBlock::RedactedThinking { data })
                }
                BlockBuilder::ToolUse { id, name, json } => {
                    raw_tool_input.insert(id.clone(), json.clone());
                    content.push(ContentBlock::ToolUse {
                        id,
                        name,
                        // Replay needs a JSON object. Truncated arguments cannot
                        // produce one; the raw text above is what the repair
                        // path reads, and this keeps the replayed message legal.
                        input: serde_json::from_str(&json)
                            .unwrap_or_else(|_| Value::Object(Default::default())),
                    });
                }
                BlockBuilder::Ignored => {}
            }
        }
        MessagesResponse {
            id: self.id,
            model: self.model,
            content,
            stop_reason: self.stop_reason,
            usage: self.usage,
            cost: self.cost.map(Value::from),
            streamed_text,
            raw_tool_input,
        }
    }
}
