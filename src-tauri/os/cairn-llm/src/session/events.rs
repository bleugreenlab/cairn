//! [`SessionEvent`]: what a live CLI session emits, in one provider-neutral
//! vocabulary a host can forward to its own UI as is.
//!
//! A session's raw stream is provider-shaped: Claude's stream-json on stdout,
//! or a Codex thread's app-server notifications. [`claude::map`] and
//! [`codex::EventMapper`] are pure readings of those wires (the parsing itself
//! is `cairn-llm-wire`'s), and [`EventReader`] drives either one from a
//! [`SessionEvents`] until a callback stops it, a deadline passes, or the
//! stream closes. An HTTP turn has no stream of its own; its host emits the
//! same events from its [`crate::turn::HttpTurnHost`] effects, so every
//! backend presents one surface.
//!
//! Serialized internally tagged (`{"type": "text_delta", ...}`), and
//! deserialized from the same shape, so a host that persisted the stream can
//! read it back.

use std::collections::VecDeque;
use std::io::BufRead;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::codex::app_server::AppServerClient;
use crate::interface::session::SessionEvents;
use crate::wire::claude_stream::{self, ClaudeEvent, RateLimitInfo, Usage};
use crate::wire::TurnUsage;

/// One thing a session did.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    /// The CLI started the conversation (Claude's init). A Codex thread's id is
    /// already in the session's `SessionFacts`.
    Started {
        backend_session_id: Option<String>,
        model: Option<String>,
    },
    /// Streamed assistant text. `parent_tool_use_id` names the sub-agent tool
    /// call it belongs to, when not the primary conversation.
    TextDelta {
        text: String,
        parent_tool_use_id: Option<String>,
    },
    /// A complete assistant message.
    Text {
        text: String,
        parent_tool_use_id: Option<String>,
    },
    ThinkingDelta {
        text: String,
    },
    /// The model called a tool. An MCP tool is named `mcp__<server>__<tool>` on
    /// both CLIs; a Codex-native tool is named by its item type
    /// (`commandExecution`, `fileChange`, or a kind a later Codex adds).
    ToolCall {
        id: String,
        name: String,
        input: Value,
        parent_tool_use_id: Option<String>,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
    /// The app-server asks the host something (an approval, a token refresh).
    /// The host must answer it, or the turn waits.
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
    /// Claude's account rate-limit state, pushed mid-session.
    RateLimit {
        info: RateLimitInfo,
    },
    /// The turn ended. `text` is the final answer, `structured` Claude's
    /// schema-valid output, `error` what went wrong when it did not complete.
    /// `usage` is the turn's whole, in the neutral shape (`prompt_tokens` is
    /// the whole prompt, cached parts included).
    TurnComplete {
        status: TurnStatus,
        text: Option<String>,
        structured: Option<Value>,
        usage: Option<TurnUsage>,
        cost_usd: Option<f64>,
        error: Option<String>,
    },
    /// A provider error. With `will_retry` the provider is retrying and the
    /// turn goes on.
    Error {
        message: String,
        will_retry: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Completed,
    Interrupted,
    Failed,
}

/// Anthropic's disjoint usage (Codex's counters normalize into it) in the
/// neutral shape.
pub fn neutral_usage(usage: &Usage) -> TurnUsage {
    let count = |tokens: u32| i32::try_from(tokens).unwrap_or(i32::MAX);
    let mut neutral = TurnUsage::from_anthropic(
        Some(count(usage.input_tokens)),
        Some(count(usage.output_tokens)),
        usage.cache_read_input_tokens.map(count),
        usage.cache_creation_input_tokens.map(count),
        None,
    );
    neutral.reasoning_tokens = usage
        .output_tokens_details
        .as_ref()
        .and_then(|details| details.thinking_tokens)
        .map(count);
    neutral
}

/// A tool result's content as text: a string as is, an array of content
/// blocks as their text joined, anything else as JSON.
fn content_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Claude's stream-json.
pub mod claude {
    use super::*;
    use crate::wire::claude_stream::{
        ContentBlock, DeltaContent, MessageContentInner, StreamEventInner,
    };

    /// The events one stream-json event carries.
    pub fn map(event: &ClaudeEvent) -> Vec<SessionEvent> {
        match event {
            ClaudeEvent::System {
                subtype,
                session_id,
                data,
            } if subtype == "init" => vec![SessionEvent::Started {
                backend_session_id: Some(session_id.clone()),
                model: data
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }],
            ClaudeEvent::StreamEvent {
                parent_tool_use_id,
                inner: StreamEventInner::ContentBlockDelta { delta, .. },
                ..
            } => match delta {
                DeltaContent::TextDelta { text } => vec![SessionEvent::TextDelta {
                    text: text.clone(),
                    parent_tool_use_id: parent_tool_use_id.clone(),
                }],
                DeltaContent::ThinkingDelta { thinking } => vec![SessionEvent::ThinkingDelta {
                    text: thinking.clone(),
                }],
                _ => Vec::new(),
            },
            ClaudeEvent::Assistant {
                message,
                parent_tool_use_id,
                ..
            } => match &message.content {
                MessageContentInner::Text(text) => vec![SessionEvent::Text {
                    text: text.clone(),
                    parent_tool_use_id: parent_tool_use_id.clone(),
                }],
                MessageContentInner::Blocks(blocks) => blocks
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text { text } => Some(SessionEvent::Text {
                            text: text.clone(),
                            parent_tool_use_id: parent_tool_use_id.clone(),
                        }),
                        ContentBlock::ToolUse { id, name, input } => Some(SessionEvent::ToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                            parent_tool_use_id: parent_tool_use_id.clone(),
                        }),
                        _ => None,
                    })
                    .collect(),
            },
            ClaudeEvent::User { message, .. } => match &message.content {
                MessageContentInner::Blocks(blocks) => blocks
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::ToolResult {
                            tool_use_id,
                            content,
                            is_error,
                        } => Some(SessionEvent::ToolResult {
                            tool_use_id: tool_use_id.clone(),
                            content: content_text(content),
                            is_error: is_error.unwrap_or(false),
                        }),
                        _ => None,
                    })
                    .collect(),
                MessageContentInner::Text(_) => Vec::new(),
            },
            ClaudeEvent::Result {
                subtype,
                is_error,
                total_cost_usd,
                result,
                usage,
                data,
                ..
            } => {
                let text = result.clone().filter(|text| !text.trim().is_empty());
                vec![SessionEvent::TurnComplete {
                    status: if *is_error {
                        TurnStatus::Failed
                    } else {
                        TurnStatus::Completed
                    },
                    error: is_error.then(|| {
                        text.clone()
                            .unwrap_or_else(|| format!("Claude ended the turn with {subtype}"))
                    }),
                    text: if *is_error { text } else { result.clone() },
                    structured: data
                        .get("structured_output")
                        .filter(|value| !value.is_null())
                        .cloned(),
                    usage: usage.as_ref().map(neutral_usage),
                    cost_usd: *total_cost_usd,
                }]
            }
            ClaudeEvent::RateLimitEvent { rate_limit_info } => vec![SessionEvent::RateLimit {
                info: rate_limit_info.clone(),
            }],
            _ => Vec::new(),
        }
    }
}

/// A Codex thread's app-server notifications.
pub mod codex {
    use super::*;
    use crate::wire::codex_app_server::{
        CodexTurnUsageAccumulator, Notification, ProviderTurnStatus,
    };

    /// The items that are conversation, not tools. Every other item kind is
    /// a tool call, including kinds a later Codex adds.
    pub const MESSAGE_ITEMS: [&str; 3] = ["userMessage", "agentMessage", "reasoning"];

    /// Reads one thread's notifications. Stateful: it sums the turn's usage
    /// and remembers the last agent message for the turn's end.
    #[derive(Debug, Default)]
    pub struct EventMapper {
        usage: CodexTurnUsageAccumulator,
        last_message: Option<String>,
    }

    fn tool_name(item: &Value, kind: &str) -> String {
        let field = |name: &str| item.get(name).and_then(Value::as_str);
        match (kind, field("server"), field("tool")) {
            ("mcpToolCall", Some(server), Some(tool)) => format!("mcp__{server}__{tool}"),
            _ => kind.to_string(),
        }
    }

    fn tool_output(item: &Value) -> String {
        ["result", "aggregatedOutput", "output", "error"]
            .iter()
            .find_map(|field| item.get(*field).filter(|value| !value.is_null()))
            .map(content_text)
            .unwrap_or_default()
    }

    impl EventMapper {
        pub fn new() -> Self {
            Self::default()
        }

        /// The events one raw app-server message carries.
        pub fn map(&mut self, message: &Value) -> Vec<SessionEvent> {
            match Notification::parse(message) {
                Notification::ServerRequest { id, method } => vec![SessionEvent::ServerRequest {
                    id: id.clone(),
                    method: method.to_string(),
                    params: message.get("params").cloned().unwrap_or(Value::Null),
                }],
                Notification::AuthTokensRefreshRequest { id } => {
                    vec![SessionEvent::ServerRequest {
                        id: id.clone(),
                        method: "account/chatgptAuthTokens/refresh".to_string(),
                        params: message.get("params").cloned().unwrap_or(Value::Null),
                    }]
                }
                Notification::TurnStarted { .. } => {
                    self.usage.start_turn();
                    self.last_message = None;
                    Vec::new()
                }
                Notification::AgentMessageDelta { delta: Some(text) } => {
                    vec![SessionEvent::TextDelta {
                        text: text.to_string(),
                        parent_tool_use_id: None,
                    }]
                }
                Notification::ReasoningDelta { delta: Some(text) } => {
                    vec![SessionEvent::ThinkingDelta {
                        text: text.to_string(),
                    }]
                }
                Notification::ItemStarted { item } => {
                    let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
                    if MESSAGE_ITEMS.contains(&kind) {
                        return Vec::new();
                    }
                    let input = match kind {
                        "mcpToolCall" => item.get("arguments").cloned().unwrap_or(Value::Null),
                        _ => item.clone(),
                    };
                    vec![SessionEvent::ToolCall {
                        id: item
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        name: tool_name(item, kind),
                        input,
                        parent_tool_use_id: None,
                    }]
                }
                Notification::ItemCompleted { item } => {
                    let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
                    match kind {
                        "agentMessage" => match item.get("text").and_then(Value::as_str) {
                            Some(text) => {
                                self.last_message = Some(text.to_string());
                                vec![SessionEvent::Text {
                                    text: text.to_string(),
                                    parent_tool_use_id: None,
                                }]
                            }
                            None => Vec::new(),
                        },
                        _ if MESSAGE_ITEMS.contains(&kind) => Vec::new(),
                        _ => vec![SessionEvent::ToolResult {
                            tool_use_id: item
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            content: tool_output(item),
                            is_error: item.get("status").and_then(Value::as_str) == Some("failed")
                                || item.get("error").is_some_and(|error| !error.is_null()),
                        }],
                    }
                }
                Notification::TokenUsageUpdated {
                    params: Some(params),
                } => {
                    self.usage.observe(params);
                    Vec::new()
                }
                Notification::TurnCompleted { status, .. } => {
                    let (status, error) = match status {
                        ProviderTurnStatus::Completed => (TurnStatus::Completed, None),
                        other => (
                            if other == ProviderTurnStatus::Interrupted {
                                TurnStatus::Interrupted
                            } else {
                                TurnStatus::Failed
                            },
                            Some(format!("the Codex turn ended {}", other.as_str())),
                        ),
                    };
                    vec![self.turn_complete(status, error)]
                }
                Notification::TurnAborted { reason, .. } => {
                    let status = match ProviderTurnStatus::from_abort_reason(reason) {
                        ProviderTurnStatus::Interrupted => TurnStatus::Interrupted,
                        _ => TurnStatus::Failed,
                    };
                    vec![self.turn_complete(
                        status,
                        Some(format!("the Codex turn was aborted: {reason}")),
                    )]
                }
                Notification::Error {
                    message,
                    will_retry,
                    ..
                } => vec![SessionEvent::Error {
                    message: message.unwrap_or("the Codex turn failed").to_string(),
                    will_retry,
                }],
                _ => Vec::new(),
            }
        }

        fn turn_complete(&mut self, status: TurnStatus, error: Option<String>) -> SessionEvent {
            SessionEvent::TurnComplete {
                status,
                text: self.last_message.take(),
                structured: None,
                usage: self.usage.take_turn_usage().as_ref().map(neutral_usage),
                cost_usd: None,
                error,
            }
        }
    }
}

/// How many trailing stderr lines a Claude failure carries.
const STDERR_TAIL_LINES: usize = 20;

/// The last lines a process wrote to stderr, collected on their own thread so
/// a full pipe never stalls the process.
struct StderrTail(Arc<Mutex<VecDeque<String>>>);

impl StderrTail {
    fn spawn(stderr: Option<Box<dyn BufRead + Send>>) -> Self {
        let lines = Arc::new(Mutex::new(VecDeque::new()));
        if let Some(stderr) = stderr {
            let sink = lines.clone();
            std::thread::spawn(move || {
                for line in stderr.lines().map_while(Result::ok) {
                    if let Ok(mut lines) = sink.lock() {
                        if lines.len() == STDERR_TAIL_LINES {
                            lines.pop_front();
                        }
                        lines.push_back(line);
                    }
                }
            });
        }
        Self(lines)
    }

    /// `": <tail>"`, or nothing when stderr was silent.
    fn suffix(&self) -> String {
        let tail = self
            .0
            .lock()
            .map(|lines| lines.iter().cloned().collect::<Vec<_>>().join("\n"))
            .unwrap_or_default();
        let tail = tail.trim();
        if tail.is_empty() {
            String::new()
        } else {
            format!(": {tail}")
        }
    }
}

/// Why [`EventReader::read`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadEnd {
    /// The callback answered [`ControlFlow::Break`].
    Stopped,
    /// The deadline passed. The reader stopped nothing; the caller decides
    /// whether to interrupt the turn or kill the process.
    Timeout,
    /// The stream ended: the CLI exited, or the app-server closed the channel.
    Closed,
}

enum Source {
    Claude {
        events: Receiver<ClaudeEvent>,
        stderr: StderrTail,
    },
    Codex {
        client: Arc<AppServerClient>,
        notifications: Receiver<Value>,
        mapper: codex::EventMapper,
    },
}

/// Reads a live session's events as [`SessionEvent`]s.
pub struct EventReader {
    source: Source,
    /// Events mapped from a raw message that a callback stopped before
    /// taking. One raw message can carry several (Claude's text plus tool
    /// calls), so they wait here for the next [`Self::read`].
    pending: VecDeque<SessionEvent>,
}

impl EventReader {
    /// Start reading `events`. A Codex thread on a shared app-server has no
    /// channel of its own; its caller passes the one its pool routes that
    /// thread's notifications to, or this refuses.
    pub fn new(events: SessionEvents) -> Result<Self, String> {
        let source = match events {
            SessionEvents::ClaudeStream { stdout, stderr } => {
                let stderr = StderrTail::spawn(stderr);
                let (tx, rx) = unbounded();
                std::thread::spawn(move || {
                    for line in stdout.lines().map_while(Result::ok) {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match claude_stream::parse_event(&line) {
                            Ok((event, _)) => {
                                if tx.send(event).is_err() {
                                    return;
                                }
                            }
                            Err(error) => {
                                log::debug!("session events: unparsed Claude line: {error}")
                            }
                        }
                    }
                });
                Source::Claude { events: rx, stderr }
            }
            SessionEvents::CodexAppServer {
                client,
                notifications,
                ..
            } => Source::Codex {
                client,
                notifications: notifications.ok_or_else(|| {
                    "a Codex thread's events need its notification channel".to_string()
                })?,
                mapper: codex::EventMapper::new(),
            },
        };
        Ok(Self {
            source,
            pending: VecDeque::new(),
        })
    }

    /// The app-server a Codex thread's [`SessionEvent::ServerRequest`]s are
    /// answered on.
    pub fn codex_client(&self) -> Option<&Arc<AppServerClient>> {
        match &self.source {
            Source::Codex { client, .. } => Some(client),
            Source::Claude { .. } => None,
        }
    }

    /// `": <the CLI's last stderr lines>"`, or nothing (Codex, or a silent
    /// CLI).
    pub fn stderr_suffix(&self) -> String {
        match &self.source {
            Source::Claude { stderr, .. } => stderr.suffix(),
            Source::Codex { .. } => String::new(),
        }
    }

    /// Hand each event to `on_event` until it breaks, `deadline` passes, or
    /// the stream ends. Can be called again to keep reading: an event mapped
    /// but not yet handed over when the callback broke comes first.
    ///
    /// A failed Claude turn that carries no text of its own is reported with
    /// the CLI's stderr tail in its `error`, which is where the reason is.
    pub fn read(
        &mut self,
        deadline: Instant,
        mut on_event: impl FnMut(SessionEvent) -> ControlFlow<()>,
    ) -> ReadEnd {
        loop {
            while let Some(event) = self.pending.pop_front() {
                if on_event(event).is_break() {
                    return ReadEnd::Stopped;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let mapped = match &mut self.source {
                Source::Claude { events, stderr } => match events.recv_timeout(remaining) {
                    Ok(event) => {
                        let mut mapped = claude::map(&event);
                        for event in &mut mapped {
                            if let SessionEvent::TurnComplete {
                                status: TurnStatus::Failed,
                                text: None,
                                error: Some(error),
                                ..
                            } = event
                            {
                                error.push_str(&stderr.suffix());
                            }
                        }
                        mapped
                    }
                    Err(RecvTimeoutError::Timeout) => return ReadEnd::Timeout,
                    Err(RecvTimeoutError::Disconnected) => return ReadEnd::Closed,
                },
                Source::Codex {
                    notifications,
                    mapper,
                    ..
                } => match notifications.recv_timeout(remaining) {
                    Ok(message) => mapper.map(&message),
                    Err(RecvTimeoutError::Timeout) => return ReadEnd::Timeout,
                    Err(RecvTimeoutError::Disconnected) => return ReadEnd::Closed,
                },
            };
            self.pending.extend(mapped);
        }
    }
}

/// How long a stderr drain is given to catch a CLI's last words after it
/// closed its stdout.
pub(crate) const STDERR_SETTLE: Duration = Duration::from_millis(100);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const CLAUDE_FIXTURE: &str =
        include_str!("../../../cairn-llm-wire/tests/fixtures/claude_stream.jsonl");
    const CODEX_FIXTURE: &str =
        include_str!("../../../cairn-llm-wire/tests/fixtures/codex_app_server.jsonl");

    fn tags(events: &[SessionEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| {
                serde_json::to_value(event).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn a_recorded_claude_turn_reads_as_session_events() {
        let events: Vec<SessionEvent> = CLAUDE_FIXTURE
            .lines()
            .flat_map(|line| claude::map(&claude_stream::parse_event(line).unwrap().0))
            .collect();
        assert_eq!(
            tags(&events),
            [
                "started",
                "rate_limit",
                "text_delta",
                "text",
                "tool_call",
                "tool_result",
                "text_delta",
                "text",
                "turn_complete"
            ]
        );
        let SessionEvent::Started {
            backend_session_id,
            model,
        } = &events[0]
        else {
            panic!("{:?}", events[0]);
        };
        assert_eq!(backend_session_id.as_deref(), Some("session-1"));
        assert_eq!(model.as_deref(), Some("claude-opus-4"));
        let SessionEvent::ToolCall {
            id, name, input, ..
        } = &events[4]
        else {
            panic!("{:?}", events[4]);
        };
        assert_eq!(
            (id.as_str(), name.as_str()),
            ("toolu_01", "mcp__cairn__read")
        );
        assert_eq!(input["paths"][0], "file:src/lib.rs");
        let SessionEvent::ToolResult {
            content, is_error, ..
        } = &events[5]
        else {
            panic!("{:?}", events[5]);
        };
        assert_eq!((content.as_str(), *is_error), ("pub fn main() {}", false));
        let SessionEvent::TurnComplete {
            status,
            usage,
            cost_usd,
            error,
            ..
        } = &events[8]
        else {
            panic!("{:?}", events[8]);
        };
        assert_eq!(*status, TurnStatus::Completed);
        assert!(error.is_none());
        assert_eq!(*cost_usd, Some(0.01));
        let usage = usage.as_ref().unwrap();
        // The whole prompt: 16 uncached plus 30 read from cache.
        assert_eq!(usage.prompt_tokens, Some(46));
        assert_eq!(usage.completion_tokens, Some(29));
    }

    #[test]
    fn a_recorded_codex_turn_reads_as_session_events() {
        let mut mapper = codex::EventMapper::new();
        let events: Vec<SessionEvent> = CODEX_FIXTURE
            .lines()
            .flat_map(|line| mapper.map(&serde_json::from_str(line).unwrap()))
            .collect();
        assert_eq!(
            tags(&events),
            [
                "thinking_delta",
                "tool_call",
                "tool_result",
                "text_delta",
                "text",
                "error",
                "turn_complete"
            ]
        );
        let SessionEvent::ToolCall { name, id, .. } = &events[1] else {
            panic!("{:?}", events[1]);
        };
        assert_eq!((name.as_str(), id.as_str()), ("commandExecution", "cmd-1"));
        let SessionEvent::ToolResult { content, .. } = &events[2] else {
            panic!("{:?}", events[2]);
        };
        assert_eq!(content, "clean\n");
        assert!(matches!(
            &events[5],
            SessionEvent::Error {
                will_retry: true,
                ..
            }
        ));
        let SessionEvent::TurnComplete {
            status,
            text,
            usage,
            ..
        } = &events[6]
        else {
            panic!("{:?}", events[6]);
        };
        assert_eq!(*status, TurnStatus::Completed);
        assert_eq!(text.as_deref(), Some("The tree is clean."));
        let usage = usage.as_ref().unwrap();
        // The cumulative total: 160 prompt tokens, 120 of them cached.
        assert_eq!(usage.prompt_tokens, Some(160));
        assert_eq!(usage.completion_tokens, Some(20));
        assert_eq!(usage.reasoning_tokens, Some(7));
        assert_eq!(
            usage.prompt_tokens_details.as_ref().unwrap()["cached_tokens"],
            120
        );
    }

    #[test]
    fn codex_mcp_calls_are_named_as_claude_names_them() {
        let mut mapper = codex::EventMapper::new();
        let started = json!({"method": "item/started", "params": {"item": {
            "id": "call-1", "type": "mcpToolCall", "server": "echo", "tool": "echo",
            "arguments": {"text": "hi"}}}});
        let completed = json!({"method": "item/completed", "params": {"item": {
            "id": "call-1", "type": "mcpToolCall", "status": "completed",
            "result": {"content": [{"type": "text", "text": "hi"}]}}}});
        let events: Vec<SessionEvent> = [started, completed]
            .iter()
            .flat_map(|message| mapper.map(message))
            .collect();
        let SessionEvent::ToolCall { name, input, .. } = &events[0] else {
            panic!("{:?}", events[0]);
        };
        assert_eq!(name, "mcp__echo__echo");
        assert_eq!(input["text"], "hi");
        assert!(
            matches!(&events[1], SessionEvent::ToolResult { tool_use_id, is_error: false, .. } if tool_use_id == "call-1")
        );
    }

    #[test]
    fn a_server_request_carries_its_params_for_the_host_to_answer() {
        let mut mapper = codex::EventMapper::new();
        let events = mapper.map(
            &json!({"id": 7, "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "t", "command": "rm -rf /"}}),
        );
        let SessionEvent::ServerRequest { id, method, params } = &events[0] else {
            panic!("{:?}", events[0]);
        };
        assert_eq!(id, &json!(7));
        assert_eq!(method, "item/commandExecution/requestApproval");
        assert_eq!(params["command"], "rm -rf /");
    }

    /// A reader paused after the first of two tool calls in one Claude
    /// message delivers the second on the next read, not the next line.
    #[test]
    fn a_paused_read_delivers_the_rest_of_a_message_next() {
        let stream = concat!(
            r#"{"type":"assistant","uuid":"a","session_id":"s","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"a","input":{}},{"type":"tool_use","id":"t2","name":"b","input":{}}]}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","session_id":"s","is_error":false,"result":"done"}"#,
            "\n",
        );
        let mut reader = EventReader::new(SessionEvents::ClaudeStream {
            stdout: Box::new(std::io::Cursor::new(stream.as_bytes().to_vec())),
            stderr: None,
        })
        .unwrap();
        let deadline = || Instant::now() + Duration::from_secs(5);
        let mut seen = Vec::new();
        for _ in 0..3 {
            reader.read(deadline(), |event| {
                seen.push(tags(std::slice::from_ref(&event)).remove(0));
                if let SessionEvent::ToolCall { id, .. } = event {
                    seen.push(id);
                }
                ControlFlow::Break(())
            });
        }
        assert_eq!(
            seen,
            ["tool_call", "t1", "tool_call", "t2", "turn_complete"]
        );
    }

    #[test]
    fn events_serialize_tagged_for_a_ui() {
        let event = SessionEvent::TextDelta {
            text: "hi".to_string(),
            parent_tool_use_id: None,
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({"type": "text_delta", "text": "hi", "parent_tool_use_id": null})
        );
    }
}
