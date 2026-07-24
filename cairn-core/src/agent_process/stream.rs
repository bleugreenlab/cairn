use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    /// Remainder of raw JSON after stripping extracted fields.
    /// None if only boilerplate fields remain.
    pub raw: Option<Value>,
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

impl TranscriptEvent {
    pub(crate) fn from_claude_event(event: &ClaudeEvent, raw: Value) -> Self {
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
        let tool_uses = transcript.tool_uses.unwrap();
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

        let tool_uses = transcript.tool_uses.unwrap();
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
