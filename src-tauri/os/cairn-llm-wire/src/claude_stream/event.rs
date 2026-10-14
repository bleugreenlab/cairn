//! The typed events on Claude CLI's stream-json stdout, one per line.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Events emitted by Claude CLI with --output-format stream-json
/// Each line of output is one of these event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClaudeEvent {
    /// System messages: `init` at start of session, `api_retry` while the CLI
    /// retries a failed request, and others.
    System {
        subtype: String,
        #[serde(default)]
        session_id: String,
        #[serde(flatten)]
        data: Value,
    },
    /// User message in conversation
    User {
        uuid: String,
        session_id: String,
        message: ClaudeMessage,
        #[serde(default)]
        parent_tool_use_id: Option<String>,
    },
    /// Assistant (Claude) response
    Assistant {
        uuid: String,
        session_id: String,
        message: ClaudeMessage,
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
    /// Response to a control_request (interrupt, set_model, etc.) or to an
    /// `update_environment_variables`.
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

impl ClaudeEvent {
    /// Stable discriminant name for a parsed event, for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            ClaudeEvent::System { .. } => "system",
            ClaudeEvent::User { .. } => "user",
            ClaudeEvent::Assistant { .. } => "assistant",
            ClaudeEvent::Result { .. } => "result",
            ClaudeEvent::StreamEvent { .. } => "stream_event",
            ClaudeEvent::ControlResponse { .. } => "control_response",
            ClaudeEvent::RateLimitEvent { .. } => "rate_limit_event",
            ClaudeEvent::Unknown => "unknown",
        }
    }

    /// The request a `control_response` answers.
    pub fn control_request_id(&self) -> Option<&str> {
        match self {
            ClaudeEvent::ControlResponse { request_id, .. } => Some(request_id),
            _ => None,
        }
    }

    /// The HTTP status of a `system/api_retry` frame: the CLI saw this status
    /// and is about to retry. With `CLAUDE_CODE_OAUTH_401_WAIT_MS` set, a 401
    /// here means the CLI is waiting for a different token in its environment.
    pub fn api_retry_status(&self) -> Option<u16> {
        match self {
            ClaudeEvent::System { subtype, data, .. } if subtype == "api_retry" => data
                .get("error_status")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok()),
            _ => None,
        }
    }
}

/// Payload of a `rate_limit_event`. Field shape is the CLI's: `status` is
/// lowercase, the rest are camelCase, so each carries an explicit rename. The
/// flattened `extra` tolerates fields the CLI adds later without a parse error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    #[serde(default)]
    pub status: String,
    #[serde(default, rename = "rateLimitType")]
    pub rate_limit_type: Option<String>,
    #[serde(default, rename = "resetsAt")]
    pub resets_at: Option<i64>,
    #[serde(default, rename = "overageResetsAt")]
    pub overage_resets_at: Option<i64>,
    #[serde(flatten, default)]
    extra: Value,
}

impl RateLimitInfo {
    /// Whether the reported status means the request was blocked by an exhausted
    /// limit, as opposed to allowed or a soft warning. Conservative on purpose:
    /// only an explicit reject counts, because a blocking-status EOF is finalized
    /// as recoverable (warm/resumable) rather than a hard crash.
    pub fn is_blocking(&self) -> bool {
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
        #[serde(default, alias = "error")]
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

/// The `message` object of a `user` or `assistant` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeMessage {
    pub role: String,
    pub content: MessageContentInner,
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

/// Anthropic's usage object as the CLI reports it. The input components are
/// disjoint: `input_tokens` is the uncached prompt only, with cache reads and
/// cache writes counted beside it rather than inside it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u32,
    #[serde(default)]
    pub output_tokens: u32,
    #[serde(default)]
    pub cache_creation_input_tokens: Option<u32>,
    #[serde(default)]
    pub cache_read_input_tokens: Option<u32>,
    #[serde(default)]
    pub output_tokens_details: Option<OutputTokensDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputTokensDetails {
    #[serde(default)]
    pub thinking_tokens: Option<u32>,
}

/// Parse a line of stream-json output
pub fn parse_event(line: &str) -> Result<(ClaudeEvent, Value), String> {
    let raw: Value =
        serde_json::from_str(line).map_err(|e| format!("Failed to parse JSON: {}", e))?;
    let normalized = normalize_control_response(raw.clone());

    let event: ClaudeEvent =
        serde_json::from_value(normalized).map_err(|e| format!("Failed to parse event: {}", e))?;

    Ok((event, raw))
}

/// Some CLI versions nest a control response's `request_id` inside `response`
/// rather than beside it; hoist it so both shapes parse as one event.
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

/// The id and tool name of a `content_block_start` that opens a tool call, or
/// `None` for any other block.
pub fn extract_tool_start(content_block: &Value) -> Option<(Option<String>, String)> {
    if content_block.get("type").and_then(|value| value.as_str()) != Some("tool_use") {
        return None;
    }
    let name = content_block.get("name").and_then(|value| value.as_str())?;
    let id = content_block
        .get("id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    Some((id, name.to_string()))
}

/// The prefill usage a `message_start` carries on its `message` object.
pub fn message_start_usage(message: Option<&Value>) -> Option<Usage> {
    message?
        .get("usage")
        .and_then(|usage| serde_json::from_value(usage.clone()).ok())
}

/// The absolute thinking-token estimate on a `system:thinking_tokens` event.
pub fn parse_thinking_tokens_estimate(data: &Value) -> Option<u32> {
    data.get("estimated_tokens")
        .and_then(|value| value.as_u64())
        .and_then(|tokens| u32::try_from(tokens).ok())
}
