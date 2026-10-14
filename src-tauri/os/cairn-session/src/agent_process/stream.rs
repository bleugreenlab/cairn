use cairn_llm::wire::claude_stream::{
    extract_assistant_content, extract_user_content, parse_event, ClaudeEvent, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct ParseMetrics {
    pub attempts: usize,
    pub failures: usize,
    pub duration: Duration,
}

impl ParseMetrics {
    pub fn parse(&mut self, line: &str) -> Result<(ClaudeEvent, Value), String> {
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TokenCounts {
    pub input: Option<i32>,
    pub output: Option<i32>,
    pub cache_read: Option<i32>,
    pub cache_create: Option<i32>,
    pub thinking: Option<i32>,
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

    pub fn from_optional_usage(usage: Option<&Usage>) -> Self {
        usage.map(Self::from_usage).unwrap_or_default()
    }
}

pub use cairn_llm::wire::ToolUseInfo;

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
    pub thinking_ms: Option<i64>,
    /// Durable identity of the queued user message promoted into this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_message_id: Option<String>,
    /// Remainder of raw JSON after stripping extracted fields.
    /// None if only boilerplate fields remain.
    pub raw: Option<Value>,
}

/// Serialization is available only after the transcript has crossed sanitization.
pub trait TranscriptJson {
    fn to_event_json(&self) -> String;
}

impl TranscriptJson for cairn_common::security::ObservedSafe<TranscriptEvent> {
    /// Serialize a sanitized transcript event for persistence and emission.
    ///
    /// This is the single point at which a transcript event becomes bytes, and
    /// it exists only on the sanitized wrapper, so the durable row and the value
    /// the frontend renders cannot fork into a raw and a scrubbed version.
    ///
    /// Fails closed. A serialization error yields a marker event rather than a
    /// partial or fallback rendering, either of which could carry the very bytes
    /// the crossing exists to remove.
    fn to_event_json(&self) -> String {
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

impl cairn_common::security::Sanitize for TranscriptEvent {
    /// Sanitize every field that can carry backend-produced text.
    ///
    /// Identity fields (`event_type`, `session_id`, tool-use ids) are excluded:
    /// they are Cairn-minted or protocol-fixed and cannot carry a credential,
    /// and redacting one would corrupt transcript reconstruction. `raw` is
    /// included because it is the residue of the backend's own JSON, which is
    /// exactly where an un-extracted echo would hide.
    fn sanitize_observed(&mut self, sanitizer: &mut cairn_common::security::Sanitizer<'_>) {
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
    pub fn observed(self) -> cairn_common::security::ObservedSafe<Self> {
        cairn_common::security::ObservedSafe::observe(
            self,
            cairn_common::security::Crossing::Transcript,
        )
    }

    /// Convert a backend event, sanitizing it at the transcript crossing.
    pub fn from_claude_event(
        event: &ClaudeEvent,
        raw: Value,
    ) -> cairn_common::security::ObservedSafe<Self> {
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
                let user = extract_user_content(&message.content);
                let event_type = user.kind.as_str();
                TranscriptEvent {
                    event_type: event_type.to_string(),
                    session_id: Some(session_id.clone()),
                    parent_tool_use_id: parent_tool_use_id.clone(),
                    content: user.text,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: user.tool_use_id,
                    tool_result: user.tool_result,
                    is_error: user.is_error,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: strip_extracted_fields(raw, event_type),
                }
            }
            ClaudeEvent::Assistant {
                session_id,
                message,
                parent_tool_use_id,
                ..
            } => {
                let assistant = extract_assistant_content(&message.content);
                let tool_uses = assistant.tool_uses;
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
                    content: assistant.text,
                    thinking: assistant.thinking,
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

impl From<TranscriptEvent> for cairn_llm::wire::ReplayEvent {
    fn from(event: TranscriptEvent) -> Self {
        Self {
            content: event.content,
            tool_uses: event.tool_uses,
            tool_use_id: event.tool_use_id,
            tool_result: event.tool_result,
            raw: event.raw,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_llm::wire::claude_stream::{InputDefect, RejectedToolInput};

    #[test]
    fn parse_metrics_count_attempts_failures_and_duration() {
        let mut metrics = ParseMetrics::default();
        assert!(metrics.parse(r#"{"type":"unknown"}"#).is_ok());
        assert!(metrics.parse("not json").is_err());
        assert_eq!(metrics.attempts, 2);
        assert_eq!(metrics.failures, 1);
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
