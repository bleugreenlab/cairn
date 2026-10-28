//! When a Claude turn has ended, and what a stream that closed early proves.

use serde_json::Value;

use super::event::ClaudeEvent;

impl ClaudeEvent {
    /// Whether this event ends the turn. Only a `result` does.
    ///
    /// Claude CLI can report a terminal failure as a `Result` whose subtype is
    /// still `success`. `is_error` is authoritative. Only an actual HTTP 429 is
    /// a rate limit ([`result_is_rate_limited`]); any other failed result is
    /// neutral, because a terminal result can arrive before or after a tool call
    /// and its payload does not prove which.
    pub fn is_terminal(&self) -> bool {
        matches!(self, ClaudeEvent::Result { .. })
    }
}

/// Whether a terminal `result`'s payload reports an HTTP 429 from the provider.
pub fn result_is_rate_limited(data: &Value) -> bool {
    data.get("api_error_status").and_then(Value::as_u64) == Some(429)
}

/// Why a Claude account stopped serving a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderDepletion {
    /// A rolling usage window is exhausted and heals at its reset.
    RollingWindow { resets_at: Option<i64> },
    /// Usage credits are exhausted. Nothing Cairn can observe heals this, so
    /// the account stays out of rotation until an operator retries it.
    Credits,
    /// The account's owner has not accepted Anthropic's updated Consumer
    /// Terms, so every request fails with an HTTP 400 until they accept in
    /// claude.ai. That is not a usage window and has no reset: the account
    /// stays out of rotation until an operator acts.
    TermsAcceptance,
}

/// The provider's own sentence when a refusal says the account must accept
/// Anthropic's updated Consumer Terms, without the CLI's `API Error: 400`
/// prefix. `None` for any other text.
pub fn terms_acceptance_refusal(text: &str) -> Option<&str> {
    if !(text.contains("Consumer Terms") && text.contains("accept them in claude.ai")) {
        return None;
    }
    let text = text.trim();
    Some(
        text.strip_prefix("API Error:")
            .map(|rest| {
                rest.trim_start()
                    .trim_start_matches(|c: char| c.is_ascii_digit())
                    .trim_start()
            })
            .unwrap_or(text),
    )
}

impl ClaudeEvent {
    /// Classify only the CLI's structured synthetic API-error message. The
    /// marker checks happen after that structural gate, so the same words
    /// quoted in ordinary assistant prose are inert.
    pub fn provider_depletion(&self) -> Option<ProviderDepletion> {
        let ClaudeEvent::Assistant {
            message,
            is_api_error_message: true,
            api_error_status,
            ..
        } = self
        else {
            return None;
        };
        let text = message.text();
        if terms_acceptance_refusal(&text).is_some() {
            return Some(ProviderDepletion::TermsAcceptance);
        }
        if *api_error_status != Some(429) {
            return None;
        }
        if text.contains("cc_cli_limit_message") || text.contains("/usage-credits") {
            Some(ProviderDepletion::Credits)
        } else {
            Some(ProviderDepletion::RollingWindow { resets_at: None })
        }
    }
}

/// The depletion a failed terminal `result` ends its turn with. The CLI
/// reports credit exhaustion as a structured assistant error followed by a
/// generic error result, so a depletion classified earlier in the turn is
/// authoritative. A failed result that states the Consumer Terms refusal is
/// that refusal; a bare HTTP 429 result is a rolling-window block.
pub fn terminal_provider_depletion(
    is_error: bool,
    data: &Value,
    pending: Option<ProviderDepletion>,
) -> Option<ProviderDepletion> {
    if !is_error {
        return None;
    }
    pending
        .or_else(|| {
            data.get("result")
                .and_then(Value::as_str)
                .and_then(terms_acceptance_refusal)
                .map(|_| ProviderDepletion::TermsAcceptance)
        })
        .or_else(|| {
            result_is_rate_limited(data)
                .then_some(ProviderDepletion::RollingWindow { resets_at: None })
        })
}

/// The Claude CLI's wording when `--resume <id>` finds no conversation for the
/// process's working directory (the CLI keys its conversation store by cwd).
/// Matched here, at the boundary that emits it; no other layer sees the string.
pub fn stderr_reports_unknown_session(line: &str) -> bool {
    line.contains("No conversation found with session ID")
}

/// The Claude CLI's wording when `--session-id <id>` names a conversation the
/// CLI already created: an earlier launch under the same id got far enough to
/// create it, so a fresh start under that id is refused.
pub fn stderr_reports_session_id_in_use(line: &str) -> bool {
    line.contains("Session ID ") && line.contains(" is already in use")
}

/// The signals a reader needs at stdout EOF that follow purely from the events
/// it saw.
///
/// A `result` before EOF means the turn completed however the process then
/// exited; a blocking rate-limit status means an early exit is recoverable on
/// reset rather than a crash. Signals that depend on what the host decided (a
/// persisted quarantine, a terminal tool that already ended the turn) are not
/// evidence of the stream and are not collected here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEvidence {
    pub saw_terminal_result: bool,
    pub saw_rate_limit_event: bool,
    pub saw_blocking_rate_limit: bool,
    pub last_event_kind: &'static str,
}

impl Default for StreamEvidence {
    fn default() -> Self {
        Self {
            saw_terminal_result: false,
            saw_rate_limit_event: false,
            saw_blocking_rate_limit: false,
            last_event_kind: "none",
        }
    }
}

impl StreamEvidence {
    /// Fold one parsed event into the evidence. Call it for every event, before
    /// the reader decides whether to act on it.
    pub fn observe(&mut self, event: &ClaudeEvent) {
        self.last_event_kind = event.kind();
        if event.is_terminal() {
            self.saw_terminal_result = true;
        }
        if let ClaudeEvent::RateLimitEvent { rate_limit_info } = event {
            self.saw_rate_limit_event = true;
            if rate_limit_info.is_blocking() {
                self.saw_blocking_rate_limit = true;
            }
        }
    }

    /// Record a failed terminal `result` that [`result_is_rate_limited`] names
    /// as a 429. The account is as blocked as after a rejecting
    /// `rate_limit_event`, but whether a given result counts is the reader's
    /// call: a result that only acknowledges an interrupt the host sent is
    /// transport cleanup, so [`StreamEvidence::observe`] leaves this to the
    /// caller.
    pub fn record_terminal_rate_limit(&mut self) {
        self.saw_blocking_rate_limit = true;
    }
}
