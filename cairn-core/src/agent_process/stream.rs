use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub(crate) struct ParseMetrics {
    pub attempts: usize,
    pub failures: usize,
    pub duration: Duration,
}

impl ParseMetrics {
    pub(crate) fn parse(&mut self, line: &str) -> Result<(ClaudeEvent, Value), String> {
        let started = Instant::now();
        self.attempts += 1;
        let result = parse_event(line);
        self.duration += started.elapsed();
        if result.is_err() {
            self.failures += 1;
        }
        result
    }
}

/// Events emitted by Claude CLI with --output-format stream-json
/// Each line of output is one of these event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClaudeEvent {
    /// System init message at start of session
    System {
        subtype: String,
        session_id: String,
        #[serde(flatten)]
        data: Value,
    },
    /// User message in conversation
    User {
        uuid: String,
        session_id: String,
        message: MessageContent,
        #[serde(default)]
        parent_tool_use_id: Option<String>,
    },
    /// Assistant (Claude) response
    Assistant {
        uuid: String,
        session_id: String,
        message: MessageContent,
        #[serde(default)]
        parent_tool_use_id: Option<String>,
    },
    /// Final result with stats
    Result {
        subtype: String,
        session_id: String,
        #[serde(default)]
        is_error: bool,
        #[serde(default)]
        duration_ms: Option<u64>,
        #[serde(default)]
        num_turns: Option<u32>,
        #[serde(default)]
        total_cost_usd: Option<f64>,
        #[serde(default)]
        result: Option<String>,
        #[serde(default)]
        usage: Option<Usage>,
        #[serde(flatten)]
        data: Value,
    },
    /// Streaming event wrapper (when using --include-partial-messages)
    #[serde(rename = "stream_event")]
    StreamEvent {
        session_id: String,
        /// Set to the Task tool_use id when the partial message belongs to a
        /// subagent; `None`/absent for the primary session. Lets the live
        /// context gauge skip subagent inferences so they don't overwrite the
        /// primary session's figure.
        #[serde(default)]
        parent_tool_use_id: Option<String>,
        #[serde(rename = "event")]
        inner: StreamEventInner,
    },
    /// Response to a control_request (interrupt, set_model, etc.)
    ControlResponse {
        request_id: String,
        response: ControlResponseInner,
    },
    /// Account rate-limit state pushed by the CLI mid-session. Reports status +
    /// reset windows (not a precise usage percent); surfaced to the live usage
    /// panel and used to classify an exhausted-limit EOF as recoverable.
    RateLimitEvent { rate_limit_info: RateLimitInfo },
    /// Any event `type` we don't model. A unit `#[serde(other)]` arm keeps the
    /// parser forward-compatible: a future event kind is ignored cleanly instead
    /// of failing to parse (which used to spam warnings, e.g. `rate_limit_event`).
    #[serde(other)]
    Unknown,
}

/// Payload of a `rate_limit_event`. Field shape is the CLI's: `status` is
/// lowercase, the rest are camelCase, so each carries an explicit rename. The
/// flattened `extra` tolerates fields the CLI adds later without a parse error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    #[serde(default)]
    pub(crate) status: String,
    #[serde(default, rename = "rateLimitType")]
    pub(crate) rate_limit_type: Option<String>,
    #[serde(default, rename = "resetsAt")]
    pub(crate) resets_at: Option<i64>,
    #[serde(default, rename = "overageResetsAt")]
    pub(crate) overage_resets_at: Option<i64>,
    #[serde(flatten, default)]
    extra: Value,
}

impl RateLimitInfo {
    /// Whether the reported status means the request was blocked by an exhausted
    /// limit, as opposed to allowed or a soft warning. Conservative on purpose:
    /// only an explicit reject counts, because a blocking-status EOF is finalized
    /// as recoverable (warm/resumable) rather than a hard crash.
    pub(crate) fn is_blocking(&self) -> bool {
        let status = self.status.to_ascii_lowercase();
        matches!(status.as_str(), "rejected" | "blocked" | "exhausted")
    }
}

/// Inner response for control requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "subtype", rename_all = "snake_case")]
pub enum ControlResponseInner {
    /// Successful control request
    Success {
        #[serde(default)]
        response: Option<Value>,
    },
    /// Failed control request
    Error {
        #[serde(default)]
        message: Option<String>,
    },
}

/// Inner streaming events from Claude CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEventInner {
    /// Start of a new message
    MessageStart {
        #[serde(default)]
        message: Option<Value>,
    },
    /// Start of a content block
    ContentBlockStart {
        index: usize,
        #[serde(default)]
        content_block: Option<Value>,
    },
    /// Delta update to a content block
    ContentBlockDelta { index: usize, delta: DeltaContent },
    /// End of a content block
    ContentBlockStop { index: usize },
    /// Message delta (stop reason, usage updates)
    MessageDelta {
        #[serde(default)]
        delta: Option<Value>,
        #[serde(default)]
        usage: Option<Value>,
    },
    /// End of message
    MessageStop,
    /// Unknown event type (for forward compatibility)
    #[serde(other)]
    Unknown,
}

/// Delta content types for streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeltaContent {
    /// Text delta
    TextDelta { text: String },
    /// Thinking delta (extended thinking)
    ThinkingDelta { thinking: String },
    /// Tool input JSON bytes streamed while Claude is constructing a tool call.
    InputJsonDelta { partial_json: String },
    /// Unknown delta type
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageContent {
    role: String,
    content: MessageContentInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContentInner {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: Value,
        #[serde(default)]
        is_error: Option<bool>,
    },
    Thinking {
        thinking: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Usage {
    #[serde(default)]
    pub(crate) input_tokens: u32,
    #[serde(default)]
    pub(crate) output_tokens: u32,
    #[serde(default)]
    pub(crate) cache_creation_input_tokens: Option<u32>,
    #[serde(default)]
    pub(crate) cache_read_input_tokens: Option<u32>,
    #[serde(default)]
    pub(crate) output_tokens_details: Option<OutputTokensDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputTokensDetails {
    #[serde(default)]
    pub(crate) thinking_tokens: Option<u32>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TokenCounts {
    pub(crate) input: Option<i32>,
    pub(crate) output: Option<i32>,
    pub(crate) cache_read: Option<i32>,
    pub(crate) cache_create: Option<i32>,
    pub(crate) thinking: Option<i32>,
}

impl TokenCounts {
    fn from_usage(usage: &Usage) -> Self {
        Self {
            input: Some(usage.input_tokens as i32),
            output: Some(usage.output_tokens as i32),
            cache_read: usage.cache_read_input_tokens.map(|tokens| tokens as i32),
            cache_create: usage
                .cache_creation_input_tokens
                .map(|tokens| tokens as i32),
            thinking: usage
                .output_tokens_details
                .as_ref()
                .and_then(|details| details.thinking_tokens)
                .map(|tokens| tokens as i32),
        }
    }

    pub(crate) fn from_optional_usage(usage: Option<&Usage>) -> Self {
        usage.map(Self::from_usage).unwrap_or_default()
    }
}

/// A single tool use extracted from an assistant message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolUseInfo {
    pub id: String,
    pub name: String,
    pub input: Value,
}

/// Simplified event for frontend consumption
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEvent {
    pub event_type: String,
    pub session_id: Option<String>,
    pub parent_tool_use_id: Option<String>, // Non-null for subagent events
    pub content: Option<String>,
    pub thinking: Option<String>, // Thinking block content (extended thinking)
    pub tool_name: Option<String>,
    pub tool_input: Option<Value>,
    pub tool_uses: Option<Vec<ToolUseInfo>>, // All tool uses in assistant message
    pub tool_use_id: Option<String>,         // For tool_result: which tool this is for
    pub tool_result: Option<String>,
    pub is_error: bool,
    /// Wall-clock duration from stream open to the finalized thinking-token
    /// count, in milliseconds. Set only on finalized assistant events that
    /// carried reasoning; `None` (and omitted from JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) thinking_ms: Option<i64>,
    /// Durable identity of the queued user message promoted into this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_message_id: Option<String>,
    /// Remainder of raw JSON after stripping extracted fields.
    /// None if only boilerplate fields remain.
    pub raw: Option<Value>,
}

impl crate::security::ObservedSafe<TranscriptEvent> {
    /// Serialize a sanitized transcript event for persistence and emission.
    ///
    /// This is the single point at which a transcript event becomes bytes, and
    /// it exists only on the sanitized wrapper, so the durable row and the value
    /// the frontend renders cannot fork into a raw and a scrubbed version.
    ///
    /// Fails closed. A serialization error yields a marker event rather than a
    /// partial or fallback rendering, either of which could carry the very bytes
    /// the crossing exists to remove.
    pub fn to_event_json(&self) -> String {
        match serde_json::to_string(&**self) {
            Ok(json) => json,
            Err(error) => {
                log::error!("transcript event serialization failed: {error}");
                let placeholder = TranscriptEvent {
                    event_type: self.event_type.clone(),
                    session_id: self.session_id.clone(),
                    parent_tool_use_id: self.parent_tool_use_id.clone(),
                    content: Some("[event omitted: serialization failed]".to_string()),
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: true,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: None,
                };
                serde_json::to_string(&placeholder).unwrap_or_else(|_| String::from("{}"))
            }
        }
    }
}

/// Strip fields we've already extracted from raw JSON to reduce storage.
/// Returns None if only minimal boilerplate remains.
fn strip_extracted_fields(mut raw: Value, event_type: &str) -> Option<Value> {
    if let Value::Object(ref mut map) = raw {
        // Remove common fields that are either extracted or not useful
        map.remove("uuid");

        // For user/assistant events, strip the message content we've extracted
        if event_type == "user" || event_type == "assistant" || event_type == "tool_result" {
            if let Some(Value::Object(ref mut msg)) = map.get_mut("message") {
                msg.remove("content");
            }
        }

        // Check what remains after stripping
        // Typical remaining: { "type": "...", "session_id": "...", "message": { "role": "..." } }
        let remaining_useful = map.iter().any(|(k, v)| {
            // Skip known boilerplate fields
            if matches!(k.as_str(), "type" | "session_id" | "parent_tool_use_id") {
                return false;
            }
            // For "message", check if it has anything useful beyond "role"
            if k == "message" {
                if let Value::Object(msg_map) = v {
                    return msg_map.iter().any(|(mk, _)| mk != "role");
                }
            }
            true
        });

        if !remaining_useful {
            return None;
        }
    }
    Some(raw)
}

impl crate::security::Sanitize for TranscriptEvent {
    /// Sanitize every field that can carry backend-produced text.
    ///
    /// Identity fields (`event_type`, `session_id`, tool-use ids) are excluded:
    /// they are Cairn-minted or protocol-fixed and cannot carry a credential,
    /// and redacting one would corrupt transcript reconstruction. `raw` is
    /// included because it is the residue of the backend's own JSON, which is
    /// exactly where an un-extracted echo would hide.
    fn sanitize_observed(&mut self, sanitizer: &mut crate::security::Sanitizer<'_>) {
        sanitizer.opt_text_in_place(&mut self.content);
        sanitizer.opt_text_in_place(&mut self.thinking);
        sanitizer.opt_text_in_place(&mut self.tool_result);
        if let Some(input) = self.tool_input.as_mut() {
            sanitizer.json(input);
        }
        if let Some(uses) = self.tool_uses.as_mut() {
            for use_info in uses.iter_mut() {
                sanitizer.json(&mut use_info.input);
            }
        }
        if let Some(raw) = self.raw.as_mut() {
            sanitizer.json(raw);
        }
    }
}

impl TranscriptEvent {
    /// Carry this event across the transcript crossing (CAIRN-3822).
    ///
    /// The only way to obtain an [`ObservedSafe<TranscriptEvent>`], and therefore
    /// the only way to reach [`ObservedSafe::to_event_json`] — which is in turn
    /// the only way a transcript event becomes bytes.
    pub fn observed(self) -> crate::security::ObservedSafe<Self> {
        crate::security::ObservedSafe::observe(self, crate::security::Crossing::Transcript)
    }

    /// Convert a backend event, sanitizing it at the transcript crossing.
    pub(crate) fn from_claude_event(
        event: &ClaudeEvent,
        raw: Value,
    ) -> crate::security::ObservedSafe<Self> {
        Self::from_claude_event_unchecked(event, raw).observed()
    }

    fn from_claude_event_unchecked(event: &ClaudeEvent, raw: Value) -> Self {
        match event {
            ClaudeEvent::System {
                subtype,
                session_id,
                ..
            } => {
                let event_type = format!("system:{}", subtype);
                TranscriptEvent {
                    event_type: event_type.clone(),
                    session_id: Some(session_id.clone()),
                    parent_tool_use_id: None,
                    content: None,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, &event_type),
                }
            }
            ClaudeEvent::User {
                session_id,
                message,
                parent_tool_use_id,
                ..
            } => {
                // Check if this is a tool result (user events can contain tool results)
                let (event_type, content, tool_use_id, tool_result, is_error) =
                    extract_user_content(&message.content);
                TranscriptEvent {
                    event_type: event_type.clone(),
                    session_id: Some(session_id.clone()),
                    parent_tool_use_id: parent_tool_use_id.clone(),
                    content,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id,
                    tool_result,
                    is_error,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, &event_type),
                }
            }
            ClaudeEvent::Assistant {
                session_id,
                message,
                parent_tool_use_id,
                ..
            } => {
                let (content, tool_uses, thinking) = extract_assistant_content(&message.content);
                // For backwards compat, also set tool_name/tool_input if single tool
                let (tool_name, tool_input) = if let Some(ref uses) = tool_uses {
                    if uses.len() == 1 {
                        (Some(uses[0].name.clone()), Some(uses[0].input.clone()))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };
                TranscriptEvent {
                    event_type: "assistant".to_string(),
                    session_id: Some(session_id.clone()),
                    parent_tool_use_id: parent_tool_use_id.clone(),
                    content,
                    thinking,
                    tool_name,
                    tool_input,
                    tool_uses,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, "assistant"),
                }
            }
            ClaudeEvent::Result {
                subtype,
                session_id,
                is_error,
                result,
                ..
            } => {
                let event_type = format!("result:{}", subtype);
                TranscriptEvent {
                    event_type: event_type.clone(),
                    session_id: Some(session_id.clone()),
                    parent_tool_use_id: None,
                    content: result.clone(),
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: *is_error,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, &event_type),
                }
            }
            // StreamEvent is handled specially in session.rs - this should not be called
            ClaudeEvent::StreamEvent {
                session_id,
                parent_tool_use_id,
                ..
            } => TranscriptEvent {
                event_type: "stream_event".to_string(),
                session_id: Some(session_id.clone()),
                parent_tool_use_id: parent_tool_use_id.clone(),
                content: None,
                thinking: None,
                tool_name: None,
                tool_input: None,
                tool_uses: None,
                tool_use_id: None,
                tool_result: None,
                is_error: false,
                thinking_ms: None,
                queued_message_id: None,
                raw: strip_extracted_fields(raw, "stream_event"),
            },
            // ControlResponse is handled specially in session.rs - this should not be called
            ClaudeEvent::ControlResponse { request_id, .. } => {
                let event_type = format!("control_response:{}", request_id);
                TranscriptEvent {
                    event_type: event_type.clone(),
                    session_id: None,
                    parent_tool_use_id: None,
                    content: None,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, &event_type),
                }
            }
            // RateLimitEvent and Unknown are intercepted in the backend reader
            // loop (logged / converted to a usage snapshot / ignored) and never
            // reach this conversion. These arms exist only for exhaustiveness.
            ClaudeEvent::RateLimitEvent { .. } | ClaudeEvent::Unknown => {
                let event_type = match event {
                    ClaudeEvent::RateLimitEvent { .. } => "rate_limit_event",
                    _ => "unknown",
                };
                TranscriptEvent {
                    event_type: event_type.to_string(),
                    session_id: None,
                    parent_tool_use_id: None,
                    content: None,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, event_type),
                }
            }
        }
    }
}

/// Extract content from user messages, handling both text and tool results
/// Returns: (event_type, content, tool_use_id, tool_result, is_error)
fn extract_user_content(
    content: &MessageContentInner,
) -> (String, Option<String>, Option<String>, Option<String>, bool) {
    match content {
        MessageContentInner::Text(s) => ("user".to_string(), Some(s.clone()), None, None, false),
        MessageContentInner::Blocks(blocks) => {
            let mut texts = Vec::new();
            let mut tool_use_id = None;
            let mut tool_result_text = None;
            let mut has_error = false;

            for block in blocks {
                match block {
                    ContentBlock::Text { text } => texts.push(text.clone()),
                    ContentBlock::ToolResult {
                        tool_use_id: id,
                        content,
                        is_error,
                    } => {
                        tool_use_id = Some(id.clone());
                        // Tool result content can be a string or array of content blocks
                        let result_text = match content {
                            Value::String(s) => s.clone(),
                            Value::Array(arr) => arr
                                .iter()
                                .filter_map(|v| v.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("\n"),
                            _ => content.to_string(),
                        };
                        tool_result_text = Some(result_text);
                        if is_error.unwrap_or(false) {
                            has_error = true;
                        }
                    }
                    _ => {}
                }
            }

            // If we have a tool result, this is a tool_result event
            if tool_result_text.is_some() {
                let content = if texts.is_empty() {
                    None
                } else {
                    Some(texts.join("\n"))
                };
                (
                    "tool_result".to_string(),
                    content,
                    tool_use_id,
                    tool_result_text,
                    has_error,
                )
            } else {
                let content = if texts.is_empty() {
                    None
                } else {
                    Some(texts.join("\n"))
                };
                ("user".to_string(), content, None, None, false)
            }
        }
    }
}

/// Why the agent runtime refused a tool call's input JSON.
///
/// With native tool use the runtime accumulates the streamed `tool_use` input
/// and parses it itself. When that parse fails the call is never dispatched —
/// Cairn's MCP server never sees it — and the runtime hands the block back with
/// `input` replaced by a `__unparsedToolInput` marker carrying the byte length
/// and a clipped prefix of the text it could not read. Classifying that prefix
/// is the only account anyone gets of why the call was lost, so the variants
/// here name causes observed in real rejections rather than a generic failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputDefect {
    /// A backslash escape JSON does not define, such as a regex `\.` or `\|`
    /// that reached the wire with only one level of escaping.
    InvalidEscape,
    /// `\u` not followed by four hex digits — Rust's `\u{2717}` form leaking
    /// into a JSON string.
    UnicodeEscape,
    /// A raw control character inside a string, which JSON requires escaped.
    /// Most often a literal newline carried in from a shell heredoc.
    ControlCharacter,
    /// A complete value followed by more text. The observed shape is a
    /// duplicated object tail, where a trailing key is emitted twice.
    TrailingGarbage,
    /// A string that never closes in a prefix known to be the whole payload.
    UnterminatedString,
    /// The retained prefix holds no defect and stops short of the full length,
    /// so the cause lies in the part the runtime did not keep.
    BeyondCapture,
    /// Well-formed JSON by our reading, so the rejection was not a syntax
    /// problem — a schema mismatch reported through the same error.
    NotSyntax,
    /// Invalid some other way, typically a string closed early by an unescaped
    /// quote, leaving the remainder of the object desynchronized.
    Structure,
}

impl InputDefect {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            InputDefect::InvalidEscape => "invalid-escape",
            InputDefect::UnicodeEscape => "unicode-escape",
            InputDefect::ControlCharacter => "control-character",
            InputDefect::TrailingGarbage => "trailing-garbage",
            InputDefect::UnterminatedString => "unterminated-string",
            InputDefect::BeyondCapture => "beyond-capture",
            InputDefect::NotSyntax => "not-syntax",
            InputDefect::Structure => "structure",
        }
    }
}

/// A tool call the runtime rejected before Cairn could run it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RejectedToolInput {
    /// Byte length of the input the runtime tried to parse.
    pub len: usize,
    /// Bytes actually retained for inspection; the runtime clips its sample.
    pub captured: usize,
    pub defect: InputDefect,
}

impl RejectedToolInput {
    /// Recognize the marker the runtime substitutes for an unparsable input.
    pub(crate) fn detect(input: &Value) -> Option<Self> {
        let marker = input.get("__unparsedToolInput")?;
        let raw = marker
            .get("raw")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let captured = raw.len();
        let len = marker
            .get("len")
            .and_then(Value::as_u64)
            .map_or(captured, |len| len as usize);
        Some(Self {
            len,
            captured,
            defect: classify_input_defect(raw, captured < len),
        })
    }
}

fn classify_input_defect(raw: &str, clipped: bool) -> InputDefect {
    match scan_json_strings(raw) {
        StringScan::Defect(defect) => return defect,
        StringScan::EndedInString if clipped => return InputDefect::BeyondCapture,
        StringScan::EndedInString => return InputDefect::UnterminatedString,
        StringScan::Balanced => {}
    }
    if clipped {
        return InputDefect::BeyondCapture;
    }
    // The payload is whole and its strings are sound, so the fault is in the
    // structure around them. Decoding one value and asking where it stopped
    // separates a duplicated tail from an object that never made sense.
    let mut values = serde_json::Deserializer::from_str(raw).into_iter::<Value>();
    match values.next() {
        Some(Ok(_)) if values.byte_offset() >= raw.trim_end().len() => InputDefect::NotSyntax,
        Some(Ok(_)) => InputDefect::TrailingGarbage,
        _ => InputDefect::Structure,
    }
}

enum StringScan {
    Balanced,
    EndedInString,
    Defect(InputDefect),
}

/// Walk the text as JSON would, reporting the first defect inside a string
/// literal. Escaping mistakes are the dominant cause of a rejected input, and
/// they are decidable from a prefix, so this runs before any whole-value parse.
fn scan_json_strings(raw: &str) -> StringScan {
    let mut chars = raw.chars();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if !in_string {
            in_string = c == '"';
            continue;
        }
        match c {
            '"' => in_string = false,
            '\\' => match chars.next() {
                None => return StringScan::EndedInString,
                Some('u') => {
                    for _ in 0..4 {
                        match chars.next() {
                            Some(hex) if hex.is_ascii_hexdigit() => {}
                            Some(_) => return StringScan::Defect(InputDefect::UnicodeEscape),
                            None => return StringScan::EndedInString,
                        }
                    }
                }
                Some('"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't') => {}
                Some(_) => return StringScan::Defect(InputDefect::InvalidEscape),
            },
            c if (c as u32) < 0x20 => return StringScan::Defect(InputDefect::ControlCharacter),
            _ => {}
        }
    }
    if in_string {
        StringScan::EndedInString
    } else {
        StringScan::Balanced
    }
}

/// Extract content, tool uses, and thinking from assistant messages
/// Returns: (content, tool_uses, thinking)
fn extract_assistant_content(
    content: &MessageContentInner,
) -> (Option<String>, Option<Vec<ToolUseInfo>>, Option<String>) {
    match content {
        MessageContentInner::Text(s) => (Some(s.clone()), None, None),
        MessageContentInner::Blocks(blocks) => {
            let mut texts = Vec::new();
            let mut tool_uses = Vec::new();
            let mut thinking_blocks = Vec::new();

            for block in blocks {
                match block {
                    ContentBlock::Text { text } => texts.push(text.clone()),
                    ContentBlock::ToolUse { id, name, input } => {
                        // A rejected input costs a full re-authoring of the
                        // payload and is otherwise visible only to whoever
                        // reads the transcript, so account for it here.
                        if let Some(rejected) = RejectedToolInput::detect(input) {
                            log::warn!(
                                "rejected-tool-input: {} never ran because the agent runtime could not \
                                 parse its input JSON (tool_use_id={}, bytes={}, inspected={}, defect={}). \
                                 The call did not reach Cairn; the agent has to re-author the payload.",
                                name,
                                id,
                                rejected.len,
                                rejected.captured,
                                rejected.defect.as_str(),
                            );
                        }
                        log::trace!(
                            "[DEBUG-TOOLUSE] Extracted tool_use from assistant content: id={} name={}",
                            id,
                            name
                        );
                        tool_uses.push(ToolUseInfo {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                        });
                    }
                    ContentBlock::Thinking { thinking } => {
                        thinking_blocks.push(thinking.clone());
                    }
                    _ => {}
                }
            }

            let content = if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            };
            let tool_uses = if tool_uses.is_empty() {
                None
            } else {
                Some(tool_uses)
            };
            let thinking = if thinking_blocks.is_empty() {
                None
            } else {
                Some(thinking_blocks.join("\n"))
            };
            (content, tool_uses, thinking)
        }
    }
}

/// Parse a line of stream-json output
pub(crate) fn parse_event(line: &str) -> Result<(ClaudeEvent, Value), String> {
    let raw: Value =
        serde_json::from_str(line).map_err(|e| format!("Failed to parse JSON: {}", e))?;
    let normalized = normalize_control_response(raw.clone());

    let event: ClaudeEvent =
        serde_json::from_value(normalized).map_err(|e| format!("Failed to parse event: {}", e))?;

    Ok((event, raw))
}

fn normalize_control_response(raw: Value) -> Value {
    let Value::Object(mut outer) = raw else {
        return raw;
    };

    if outer.get("type").and_then(Value::as_str) != Some("control_response") {
        return Value::Object(outer);
    }

    if outer.contains_key("request_id") {
        return Value::Object(outer);
    }

    let request_id = outer
        .get("response")
        .and_then(Value::as_object)
        .and_then(|response| response.get("request_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let Some(request_id) = request_id else {
        return Value::Object(outer);
    };

    outer.insert("request_id".to_string(), Value::String(request_id));
    if let Some(Value::Object(response)) = outer.get_mut("response") {
        response.remove("request_id");
    }

    Value::Object(outer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_metrics_count_attempts_failures_and_duration() {
        let mut metrics = ParseMetrics::default();
        assert!(metrics.parse(r#"{"type":"unknown"}"#).is_ok());
        assert!(metrics.parse("not json").is_err());
        assert_eq!(metrics.attempts, 2);
        assert_eq!(metrics.failures, 1);
    }

    // Every payload below is a real rejected tool input, recovered from the
    // `__unparsedToolInput` markers the runtime handed back on this machine.

    fn defect_of(raw: &str) -> InputDefect {
        classify_input_defect(raw, false)
    }

    #[test]
    fn regex_backslash_in_a_query_reads_as_an_invalid_escape() {
        // `\.` is a regex escape and not a JSON one, so the payload dies on a
        // grep pattern that reached the wire singly escaped.
        let raw =
            r#"{"paths": ["cairn://p/cairn/2802/1/coordinator/chat?grep=\.cairn/logs|jsonl"]}"#;
        assert_eq!(defect_of(raw), InputDefect::InvalidEscape);
    }

    #[test]
    fn a_rust_style_unicode_escape_is_not_a_json_one() {
        // JSON wants four bare hex digits; Rust's braced form is a defect.
        let raw = r#"{"content": "assert!(s.starts_with(\u{2717}))"}"#;
        assert_eq!(defect_of(raw), InputDefect::UnicodeEscape);
    }

    #[test]
    fn a_heredoc_newline_survives_into_the_string_unescaped() {
        let raw = "{\"commands\": [{\"command\": \"cat > m.rs <<EOF\nuse std::io;\"}]}";
        assert_eq!(defect_of(raw), InputDefect::ControlCharacter);
    }

    #[test]
    fn a_duplicated_object_tail_reads_as_trailing_garbage() {
        // The generation stutters and re-emits a trailing top-level key after
        // the object has already closed.
        let raw = r#"{"commands": [{"command": "true"}], "branch": "main"}, "branch": "main"}"#;
        assert_eq!(defect_of(raw), InputDefect::TrailingGarbage);
    }

    #[test]
    fn a_whole_payload_whose_string_never_closes_is_named_as_such() {
        let raw = r#"{"paths": ["file:src/lib.rs?grep=Command.*git|\"git\"]}"#;
        assert_eq!(defect_of(raw), InputDefect::UnterminatedString);
    }

    #[test]
    fn a_clipped_prefix_is_not_blamed_for_ending_early() {
        // The runtime retains only a prefix. Ending mid-string says nothing
        // about the payload, so the verdict points past the capture instead of
        // inventing an unterminated string.
        let raw = r#"{"paths": ["file:src-tauri/os/cairn-core/src/backends/cla"#;
        assert_eq!(classify_input_defect(raw, true), InputDefect::BeyondCapture);
    }

    #[test]
    fn a_defect_before_the_clip_still_counts() {
        // A concrete escaping fault inside the retained prefix is decidable
        // even though the rest was dropped.
        let raw = r#"{"paths": ["file:src/lib.rs?grep=a\|b"#;
        assert_eq!(classify_input_defect(raw, true), InputDefect::InvalidEscape);
    }

    #[test]
    fn syntactically_sound_json_is_reported_as_a_non_syntax_rejection() {
        let raw = r#"{"paths": ["file:src/lib.rs"]}"#;
        assert_eq!(defect_of(raw), InputDefect::NotSyntax);
    }

    #[test]
    fn an_unescaped_quote_desyncs_the_object_around_it() {
        // The string closes early, so what follows is read as structure.
        let raw = r#"{"content": "const N: &str = "node_modules";", "msg": "x"}"#;
        assert_eq!(defect_of(raw), InputDefect::Structure);
    }

    #[test]
    fn detect_reports_the_full_length_and_what_was_inspected() {
        let input = serde_json::json!({
            "__unparsedToolInput": {
                "len": 26559,
                "raw": r#"{"changes": [{"target": "file:a.rs?grep=\.x"}]}"#,
            }
        });
        let rejected = RejectedToolInput::detect(&input).expect("marker recognized");
        assert_eq!(rejected.len, 26559);
        assert_eq!(rejected.captured, 47);
        assert_eq!(rejected.defect, InputDefect::InvalidEscape);
    }

    #[test]
    fn a_rejected_call_reaches_the_transcript_carrying_its_marker() {
        // The runtime still emits the assistant message; only the input is
        // replaced. The block has to survive parsing with the marker intact,
        // because that is the whole record of a call that never ran.
        let line = r#"{"type":"assistant","uuid":"u1","session_id":"s1","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_01","name":"mcp__cairn__read","input":{"__unparsedToolInput":{"len":87,"raw":"{}"}}}]}}"#;
        let (event, raw) = parse_event(line).expect("assistant event parses");
        let transcript = TranscriptEvent::from_claude_event(&event, raw);

        let uses = transcript.tool_uses.clone().expect("tool use surfaced");
        assert_eq!(uses[0].name, "mcp__cairn__read");
        let rejected = RejectedToolInput::detect(&uses[0].input).expect("marker survives parsing");
        assert_eq!(rejected.len, 87);
        // Only two bytes were retained of eighty-seven, so no verdict is
        // available from the prefix alone.
        assert_eq!(rejected.defect, InputDefect::BeyondCapture);
    }

    #[test]
    fn an_ordinary_tool_input_carries_no_marker() {
        let input = serde_json::json!({"paths": ["file:src/lib.rs"]});
        assert!(RejectedToolInput::detect(&input).is_none());
    }

    #[test]
    fn test_parse_system_init_event() {
        let json = r#"{"type":"system","subtype":"init","session_id":"abc123","tools":[]}"#;
        let (event, _raw) = parse_event(json).unwrap();

        match event {
            ClaudeEvent::System {
                subtype,
                session_id,
                ..
            } => {
                assert_eq!(subtype, "init");
                assert_eq!(session_id, "abc123");
            }
            _ => panic!("Expected System event"),
        }
    }

    #[test]
    fn test_parse_user_text_event() {
        let json = r#"{"type":"user","uuid":"u123","session_id":"s456","message":{"role":"user","content":"Hello Claude"}}"#;
        let (event, _raw) = parse_event(json).unwrap();

        match event {
            ClaudeEvent::User {
                uuid,
                session_id,
                message,
                ..
            } => {
                assert_eq!(uuid, "u123");
                assert_eq!(session_id, "s456");
                assert_eq!(message.role, "user");
                match message.content {
                    MessageContentInner::Text(text) => assert_eq!(text, "Hello Claude"),
                    _ => panic!("Expected text content"),
                }
            }
            _ => panic!("Expected User event"),
        }
    }

    #[test]
    fn test_parse_assistant_with_tool_use() {
        let json = r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":[{"type":"text","text":"Let me help"},{"type":"tool_use","id":"tool1","name":"write_plan","input":{"title":"My Plan"}}]}}"#;
        let (event, _raw) = parse_event(json).unwrap();

        match event {
            ClaudeEvent::Assistant { message, .. } => match message.content {
                MessageContentInner::Blocks(blocks) => {
                    assert_eq!(blocks.len(), 2);
                    match &blocks[0] {
                        ContentBlock::Text { text } => assert_eq!(text, "Let me help"),
                        _ => panic!("Expected Text block"),
                    }
                    match &blocks[1] {
                        ContentBlock::ToolUse { id, name, input } => {
                            assert_eq!(id, "tool1");
                            assert_eq!(name, "write_plan");
                            assert_eq!(input["title"], "My Plan");
                        }
                        _ => panic!("Expected ToolUse block"),
                    }
                }
                _ => panic!("Expected Blocks content"),
            },
            _ => panic!("Expected Assistant event"),
        }
    }

    #[test]
    fn test_parse_result_event() {
        let json = r#"{"type":"result","subtype":"success","session_id":"s1","is_error":false,"duration_ms":1234,"num_turns":5,"total_cost_usd":0.05,"usage":{"input_tokens":100,"output_tokens":200}}"#;
        let (event, _raw) = parse_event(json).unwrap();

        match event {
            ClaudeEvent::Result {
                subtype,
                is_error,
                duration_ms,
                usage,
                ..
            } => {
                assert_eq!(subtype, "success");
                assert!(!is_error);
                assert_eq!(duration_ms, Some(1234));
                let usage = usage.unwrap();
                assert_eq!(usage.input_tokens, 100);
                assert_eq!(usage.output_tokens, 200);
            }
            _ => panic!("Expected Result event"),
        }
    }

    #[test]
    fn test_token_counts_parse_message_delta_thinking_tokens() {
        let usage: Usage = serde_json::from_str(r#"{"input_tokens":2979,"cache_creation_input_tokens":22522,"cache_read_input_tokens":0,"output_tokens":4147,"output_tokens_details":{"thinking_tokens":176}}"#).unwrap();
        let counts = TokenCounts::from_usage(&usage);

        assert_eq!(counts.input, Some(2979));
        assert_eq!(counts.cache_create, Some(22522));
        assert_eq!(counts.cache_read, Some(0));
        assert_eq!(counts.output, Some(4147));
        assert_eq!(counts.thinking, Some(176));
        let used = usage.input_tokens as i64
            + usage.cache_creation_input_tokens.unwrap_or(0) as i64
            + usage.cache_read_input_tokens.unwrap_or(0) as i64
            + usage.output_tokens as i64;
        assert_eq!(used, 29_648);
    }

    #[test]
    fn test_parse_control_response_with_nested_request_id() {
        let json = r#"{"type":"control_response","response":{"subtype":"success","request_id":"req-1234"}}"#;
        let (event, _raw) = parse_event(json).unwrap();

        match event {
            ClaudeEvent::ControlResponse {
                request_id,
                response: ControlResponseInner::Success { .. },
            } => {
                assert_eq!(request_id, "req-1234");
            }
            _ => panic!("Expected ControlResponse success event"),
        }
    }

    #[test]
    fn test_parse_invalid_json() {
        let result = parse_event("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_claude_tool_use_stream_start_and_input_delta() {
        let start_json = r#"{"type":"stream_event","session_id":"s1","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"Bash","input":{},"caller":{"type":"direct"}}}}"#;
        let (event, _) = parse_event(start_json).unwrap();
        match event {
            ClaudeEvent::StreamEvent {
                inner:
                    StreamEventInner::ContentBlockStart {
                        index,
                        content_block: Some(content_block),
                    },
                ..
            } => {
                assert_eq!(index, 1);
                assert_eq!(
                    content_block.get("type").and_then(|v| v.as_str()),
                    Some("tool_use")
                );
                assert_eq!(
                    content_block.get("id").and_then(|v| v.as_str()),
                    Some("toolu_1")
                );
                assert_eq!(
                    content_block.get("name").and_then(|v| v.as_str()),
                    Some("Bash")
                );
            }
            _ => panic!("Expected tool-use ContentBlockStart"),
        }

        let delta_json = r#"{"type":"stream_event","session_id":"s1","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"command\": \"echo tool\"}"}}}"#;
        let (event, _) = parse_event(delta_json).unwrap();
        match event {
            ClaudeEvent::StreamEvent {
                inner:
                    StreamEventInner::ContentBlockDelta {
                        index,
                        delta: DeltaContent::InputJsonDelta { partial_json },
                    },
                ..
            } => {
                assert_eq!(index, 1);
                assert_eq!(partial_json, r#"{"command": "echo tool"}"#);
            }
            _ => panic!("Expected input_json_delta"),
        }
    }

    #[test]
    fn test_transcript_event_from_assistant_with_tools() {
        let json = r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":[{"type":"text","text":"Analyzing..."},{"type":"tool_use","id":"t1","name":"read_file","input":{"path":"test.rs"}}]}}"#;
        let (event, raw) = parse_event(json).unwrap();
        let transcript = TranscriptEvent::from_claude_event(&event, raw);

        assert_eq!(transcript.event_type, "assistant");
        assert_eq!(transcript.content, Some("Analyzing...".to_string()));
        assert!(transcript.tool_uses.is_some());
        let tool_uses = transcript.tool_uses.clone().unwrap();
        assert_eq!(tool_uses.len(), 1);
        assert_eq!(tool_uses[0].name, "read_file");
        assert_eq!(transcript.tool_name, Some("read_file".to_string()));
    }

    #[test]
    fn test_transcript_event_from_tool_result() {
        let json = r#"{"type":"user","uuid":"u1","session_id":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"file contents here","is_error":false}]}}"#;
        let (event, raw) = parse_event(json).unwrap();
        let transcript = TranscriptEvent::from_claude_event(&event, raw);

        assert_eq!(transcript.event_type, "tool_result");
        assert_eq!(transcript.tool_use_id, Some("t1".to_string()));
        assert_eq!(
            transcript.tool_result,
            Some("file contents here".to_string())
        );
        assert!(!transcript.is_error);
    }

    #[test]
    fn test_transcript_event_with_thinking() {
        let json = r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Let me analyze this problem..."},{"type":"text","text":"Here's my answer"}]}}"#;
        let (event, raw) = parse_event(json).unwrap();
        let transcript = TranscriptEvent::from_claude_event(&event, raw);

        assert_eq!(transcript.content, Some("Here's my answer".to_string()));
        assert_eq!(
            transcript.thinking,
            Some("Let me analyze this problem...".to_string())
        );
    }

    #[test]
    fn test_parse_multiple_tool_uses() {
        let json = r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"read_file","input":{"path":"a.rs"}},{"type":"tool_use","id":"t2","name":"read_file","input":{"path":"b.rs"}}]}}"#;
        let (event, raw) = parse_event(json).unwrap();
        let transcript = TranscriptEvent::from_claude_event(&event, raw);

        let tool_uses = transcript.tool_uses.clone().unwrap();
        assert_eq!(tool_uses.len(), 2);
        // Multiple tools means legacy single-tool fields are None
        assert!(transcript.tool_name.is_none());
    }

    #[test]
    fn test_subagent_events_have_parent_tool_use_id() {
        let json = r#"{"type":"user","uuid":"u1","session_id":"s1","parent_tool_use_id":"toolu_abc123","message":{"role":"user","content":"Explore the codebase"}}"#;
        let (event, raw) = parse_event(json).unwrap();
        let transcript = TranscriptEvent::from_claude_event(&event, raw);

        assert_eq!(
            transcript.parent_tool_use_id,
            Some("toolu_abc123".to_string())
        );
    }

    #[test]
    fn test_parse_rate_limit_event() {
        let json = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","resetsAt":1717000000,"overageResetsAt":1717100000}}"#;
        let (event, _raw) = parse_event(json).unwrap();

        match event {
            ClaudeEvent::RateLimitEvent { rate_limit_info } => {
                assert_eq!(rate_limit_info.status, "allowed");
                assert_eq!(
                    rate_limit_info.rate_limit_type.as_deref(),
                    Some("five_hour")
                );
                assert_eq!(rate_limit_info.resets_at, Some(1717000000));
                assert_eq!(rate_limit_info.overage_resets_at, Some(1717100000));
                assert!(!rate_limit_info.is_blocking());
            }
            _ => panic!("Expected RateLimitEvent"),
        }
    }

    #[test]
    fn test_parse_rate_limit_event_blocking_status() {
        let json = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}"#;
        let (event, _raw) = parse_event(json).unwrap();
        match event {
            ClaudeEvent::RateLimitEvent { rate_limit_info } => {
                assert!(rate_limit_info.is_blocking());
                // Absent camelCase fields tolerate gracefully.
                assert_eq!(rate_limit_info.resets_at, None);
            }
            _ => panic!("Expected RateLimitEvent"),
        }
    }

    #[test]
    fn test_parse_unknown_event_type_is_ok() {
        // A future/unmodeled event type must parse cleanly to Unknown, not error.
        let json = r#"{"type":"some_future_event","foo":42}"#;
        let (event, _raw) = parse_event(json).expect("unknown type should parse, not error");
        assert!(matches!(event, ClaudeEvent::Unknown));
    }

    fn empty_transcript_event() -> super::TranscriptEvent {
        super::TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: None,
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
    }

    #[test]
    fn thinking_ms_is_omitted_from_json_when_none() {
        let json = serde_json::to_string(&empty_transcript_event()).unwrap();
        assert!(!json.contains("thinkingMs"));
    }

    #[test]
    fn thinking_ms_round_trips_when_present() {
        let mut event = empty_transcript_event();
        event.thinking_ms = Some(4200);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"thinkingMs\":4200"));
        let parsed: super::TranscriptEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.thinking_ms, Some(4200));
    }

    #[test]
    fn thinking_ms_defaults_to_none_when_absent_in_json() {
        let json = r#"{"eventType":"assistant","sessionId":null,"parentToolUseId":null,"content":null,"thinking":null,"toolName":null,"toolInput":null,"toolUses":null,"toolUseId":null,"toolResult":null,"isError":false,"raw":null}"#;
        let parsed: super::TranscriptEvent = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.thinking_ms, None);
    }
}
