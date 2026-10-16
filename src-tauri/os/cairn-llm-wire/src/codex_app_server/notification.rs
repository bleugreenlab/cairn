//! The typed reading of one message on the app-server's stdout.

use serde_json::Value;

use super::items::extract_app_server_delta;

/// The server request an app-server sends when the ChatGPT tokens it was given
/// stop working; the host answers with fresh ones or the turn waits.
pub const AUTH_TOKENS_REFRESH_METHOD: &str = "account/chatgptAuthTokens/refresh";

/// Stands in for a field the notification omitted, so a reader sees the same
/// absence it would have seen through a JSON pointer.
const ABSENT: &Value = &Value::Null;

/// How the provider says a turn ended, before Cairn decides what that means for
/// its own run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderTurnStatus<'a> {
    Completed,
    Interrupted,
    /// Any other status, carried verbatim so an unrecognized failure keeps its
    /// name.
    Other(&'a str),
}

impl<'a> ProviderTurnStatus<'a> {
    /// Read `turn.status` from a `turn/completed` notification.
    pub fn from_wire(status: &'a str) -> Self {
        match status {
            "completed" => Self::Completed,
            "interrupted" => Self::Interrupted,
            other => Self::Other(other),
        }
    }

    /// Read the `reason` of a `turn/aborted` notification. An abort Codex
    /// performs on request (an interrupt, a replacement turn, the end of a
    /// review) is an interruption; any other abort is an error.
    pub fn from_abort_reason(reason: &str) -> ProviderTurnStatus<'static> {
        match reason {
            "interrupted" | "replaced" | "review_ended" => ProviderTurnStatus::Interrupted,
            _ => ProviderTurnStatus::Other("error"),
        }
    }

    pub fn as_str(self) -> &'a str {
        match self {
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
            Self::Other(status) => status,
        }
    }
}

/// One message from the app-server, classified by its `method`.
///
/// The variant is decided by the method alone; its fields are whatever the
/// message carries, borrowed, with an absent field read as absent rather than
/// rejected. The reading is total: a method this crate does not model is
/// [`Notification::Unknown`], never an error.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Notification<'a> {
    /// A request from the server that expects an answer (it carries an `id`
    /// and no `result` or `error`), such as an approval or an auth refresh. Not
    /// a notification, but it arrives on the same stream.
    ServerRequest { id: &'a Value, method: &'a str },
    /// `account/chatgptAuthTokens/refresh`: the server asks for fresh ChatGPT
    /// tokens. Named apart from [`Notification::ServerRequest`] so a reader can
    /// recognise it without linking whatever answers it; answering spends a
    /// refresh token, which is the host's decision, not the wire's.
    AuthTokensRefreshRequest { id: &'a Value },
    /// `turn/started`. The turn id is `params.turn.id` only.
    TurnStarted { turn_id: Option<&'a str> },
    /// `item/agentMessage/delta`.
    AgentMessageDelta { delta: Option<&'a str> },
    /// `item/reasoning/textDelta` or `item/reasoning/summaryTextDelta`.
    ReasoningDelta { delta: Option<&'a str> },
    /// `item/started`.
    ItemStarted { item: &'a Value },
    /// `item/completed`.
    ItemCompleted { item: &'a Value },
    /// `rawResponseItem/completed`.
    RawResponseItemCompleted {
        turn_id: Option<&'a str>,
        item: &'a Value,
    },
    /// `turn/completed`. A missing status reads as completed.
    TurnCompleted {
        turn_id: Option<&'a str>,
        status: ProviderTurnStatus<'a>,
    },
    /// `turn/aborted`. The reason is `params.reason`, then `params.message`,
    /// then `unknown`; [`ProviderTurnStatus::from_abort_reason`] reads it.
    TurnAborted {
        turn_id: Option<&'a str>,
        reason: &'a str,
    },
    /// `error`. With `will_retry` Codex is retrying the request itself and the
    /// turn goes on.
    Error {
        turn_id: Option<&'a str>,
        message: Option<&'a str>,
        will_retry: bool,
    },
    /// `thread/tokenUsage/updated`; read `params` with
    /// [`super::context_usage`] and [`super::CodexTurnUsageAccumulator`].
    TokenUsageUpdated { params: Option<&'a Value> },
    /// `account/rateLimits/updated`.
    RateLimitsUpdated { rate_limits: &'a Value },
    /// `item/commandExecution/outputDelta`; absent strings read as empty.
    CommandOutputDelta { item_id: &'a str, delta: &'a str },
    /// A method Codex sends that carries nothing a reader acts on.
    Ignored { method: &'a str },
    /// A method this crate does not model, or a message with no method.
    Unknown { method: Option<&'a str> },
}

impl<'a> Notification<'a> {
    pub fn parse(msg: &'a Value) -> Self {
        let method = msg.get("method").and_then(Value::as_str);
        if let (Some(method), Some(id)) = (method, msg.get("id")) {
            if msg.get("result").is_none() && msg.get("error").is_none() {
                return match method {
                    AUTH_TOKENS_REFRESH_METHOD => Self::AuthTokensRefreshRequest { id },
                    _ => Self::ServerRequest { id, method },
                };
            }
        }
        let Some(method) = method else {
            return Self::Unknown { method: None };
        };
        let str_at = |pointer: &str| msg.pointer(pointer).and_then(Value::as_str);
        match method {
            "turn/started" => Self::TurnStarted {
                turn_id: str_at("/params/turn/id"),
            },
            "item/agentMessage/delta" => Self::AgentMessageDelta {
                delta: extract_app_server_delta(msg),
            },
            "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
                Self::ReasoningDelta {
                    delta: extract_app_server_delta(msg),
                }
            }
            "item/started" => Self::ItemStarted {
                item: msg.pointer("/params/item").unwrap_or(ABSENT),
            },
            "item/completed" => Self::ItemCompleted {
                item: msg.pointer("/params/item").unwrap_or(ABSENT),
            },
            "rawResponseItem/completed" => Self::RawResponseItemCompleted {
                turn_id: notification_turn_id(msg),
                item: msg.pointer("/params/item").unwrap_or(ABSENT),
            },
            "turn/completed" => Self::TurnCompleted {
                turn_id: notification_turn_id(msg),
                status: ProviderTurnStatus::from_wire(
                    str_at("/params/turn/status").unwrap_or("completed"),
                ),
            },
            "turn/aborted" => Self::TurnAborted {
                turn_id: notification_turn_id(msg),
                reason: str_at("/params/reason")
                    .or_else(|| str_at("/params/message"))
                    .unwrap_or("unknown"),
            },
            "error" => Self::Error {
                turn_id: notification_turn_id(msg),
                message: str_at("/params/error/message"),
                will_retry: msg
                    .pointer("/params/willRetry")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            },
            "thread/tokenUsage/updated" => Self::TokenUsageUpdated {
                params: msg.pointer("/params"),
            },
            "account/rateLimits/updated" => Self::RateLimitsUpdated {
                rate_limits: msg.pointer("/params/rateLimits").unwrap_or(ABSENT),
            },
            "item/commandExecution/outputDelta" => Self::CommandOutputDelta {
                item_id: str_at("/params/itemId").unwrap_or(""),
                delta: str_at("/params/delta").unwrap_or(""),
            },
            "item/fileChange/outputDelta"
            | "serverRequest/resolved"
            | "turn/diff/updated"
            | "item/plan/delta"
            | "item/reasoning/summaryPartAdded"
            | "thread/status/changed" => Self::Ignored { method },
            _ => Self::Unknown {
                method: Some(method),
            },
        }
    }

    /// Whether this notification ends the provider turn: `turn/completed`,
    /// `turn/aborted`, or an `error` Codex will not retry.
    ///
    /// The notification stream closing without one of these is not a
    /// completion. The server died or the transport dropped mid-turn, and the
    /// turn's outcome is unknown.
    pub fn ends_turn(&self) -> bool {
        matches!(
            self,
            Self::TurnCompleted { .. }
                | Self::TurnAborted { .. }
                | Self::Error {
                    will_retry: false,
                    ..
                }
        )
    }
}

/// The provider turn a notification names: `params.turn.id`, then
/// `params.turnId`. Only what the message says; a reader that tracks the
/// current turn supplies its own fallback.
pub fn notification_turn_id(msg: &Value) -> Option<&str> {
    msg.pointer("/params/turn/id")
        .or_else(|| msg.pointer("/params/turnId"))
        .and_then(Value::as_str)
}

/// Fold an error message to lowercase words so provider punctuation and case
/// do not defeat an exact match.
pub fn normalize_capacity_error(message: &str) -> String {
    message
        .chars()
        .map(|character| {
            if character.is_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether an `error` message is Codex reporting that the selected model has no
/// capacity. Deliberately narrow: only this exact sentence, which means another
/// model could serve the turn, qualifies.
pub fn is_selected_model_capacity_error(message: &str) -> bool {
    normalize_capacity_error(message)
        == "selected model is at capacity please try a different model"
}
