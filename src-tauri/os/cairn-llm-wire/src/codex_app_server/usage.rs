//! Token usage from `thread/tokenUsage/updated`.
//!
//! Codex counts the way OpenAI does: `inputTokens` is the whole prompt and
//! `cachedInputTokens` the cached subset of it. Everything here normalizes into
//! [`claude_stream::Usage`](crate::claude_stream::Usage), which is Anthropic's
//! disjoint shape, so [`CodexUsageBreakdown::into_usage`] subtracts the cached
//! tokens from the input rather than passing it through.

use serde_json::Value;

use crate::claude_stream::{OutputTokensDetails, Usage};

pub fn u64_pointer_as_u32(value: &Value, pointer: &str) -> u32 {
    value.pointer(pointer).and_then(|v| v.as_u64()).unwrap_or(0) as u32
}

pub fn u64_pointer_as_i64(value: &Value, pointer: &str) -> i64 {
    value.pointer(pointer).and_then(|v| v.as_u64()).unwrap_or(0) as i64
}

/// One of Codex's token counter objects (`tokenUsage.last` or
/// `tokenUsage.total`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CodexUsageBreakdown {
    input_tokens: u32,
    cached_input_tokens: u32,
    output_tokens: u32,
    reasoning_output_tokens: u32,
}

impl CodexUsageBreakdown {
    pub fn from_value(value: &Value) -> Self {
        Self {
            input_tokens: u64_pointer_as_u32(value, "/inputTokens"),
            cached_input_tokens: u64_pointer_as_u32(value, "/cachedInputTokens"),
            output_tokens: u64_pointer_as_u32(value, "/outputTokens"),
            reasoning_output_tokens: u64_pointer_as_u32(value, "/reasoningOutputTokens"),
        }
    }

    pub fn checked_delta(self, previous: Self) -> Option<Self> {
        Some(Self {
            input_tokens: self.input_tokens.checked_sub(previous.input_tokens)?,
            cached_input_tokens: self
                .cached_input_tokens
                .checked_sub(previous.cached_input_tokens)?,
            output_tokens: self.output_tokens.checked_sub(previous.output_tokens)?,
            reasoning_output_tokens: self
                .reasoning_output_tokens
                .checked_sub(previous.reasoning_output_tokens)?,
        })
    }

    pub fn into_usage(self) -> Usage {
        Usage {
            input_tokens: self.input_tokens.saturating_sub(self.cached_input_tokens),
            output_tokens: self.output_tokens,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: Some(self.cached_input_tokens),
            output_tokens_details: Some(OutputTokensDetails {
                thinking_tokens: Some(self.reasoning_output_tokens),
            }),
        }
    }
}

/// Sums one provider turn's usage across the several model responses Codex can
/// run inside it, from the cumulative `total` where it can and the per-response
/// `last` where it must.
#[derive(Debug, Default)]
pub struct CodexTurnUsageAccumulator {
    turn_usage: Option<Usage>,
    last_response_usage: Option<Usage>,
    last_seen_total: Option<CodexUsageBreakdown>,
}

impl CodexTurnUsageAccumulator {
    pub fn start_turn(&mut self) {
        self.turn_usage = None;
        self.last_response_usage = None;
    }

    pub fn observe(&mut self, params: &Value) -> Option<Usage> {
        let last = CodexUsageBreakdown::from_value(params.pointer("/tokenUsage/last")?);
        let total = params
            .pointer("/tokenUsage/total")
            .map(CodexUsageBreakdown::from_value);
        let delta = match (total, self.last_seen_total) {
            (Some(current), Some(previous)) => {
                self.last_seen_total = Some(current);
                current.checked_delta(previous).unwrap_or(last)
            }
            (Some(current), None) => {
                self.last_seen_total = Some(current);
                last
            }
            (None, _) => {
                // Without a cumulative baseline the next total cannot be safely
                // differenced against an older snapshot without double-counting.
                self.last_seen_total = None;
                last
            }
        };
        self.last_response_usage = Some(last.into_usage());
        add_usage(&mut self.turn_usage, delta.into_usage());
        self.turn_usage.clone()
    }

    pub fn last_response_usage(&self) -> Option<Usage> {
        self.last_response_usage.clone()
    }

    pub fn take_turn_usage(&mut self) -> Option<Usage> {
        self.turn_usage.take()
    }
}

pub fn add_usage(accumulated: &mut Option<Usage>, delta: Usage) {
    let usage = accumulated.get_or_insert_with(|| Usage {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: Some(0),
        output_tokens_details: Some(OutputTokensDetails {
            thinking_tokens: Some(0),
        }),
    });
    usage.input_tokens = usage.input_tokens.saturating_add(delta.input_tokens);
    usage.output_tokens = usage.output_tokens.saturating_add(delta.output_tokens);
    usage.cache_read_input_tokens = Some(
        usage
            .cache_read_input_tokens
            .unwrap_or(0)
            .saturating_add(delta.cache_read_input_tokens.unwrap_or(0)),
    );
    let thinking = usage
        .output_tokens_details
        .get_or_insert(OutputTokensDetails {
            thinking_tokens: Some(0),
        });
    thinking.thinking_tokens = Some(
        thinking.thinking_tokens.unwrap_or(0).saturating_add(
            delta
                .output_tokens_details
                .as_ref()
                .and_then(|details| details.thinking_tokens)
                .unwrap_or(0),
        ),
    );
}

/// The context occupancy one `thread/tokenUsage/updated` reports.
#[derive(Debug, Clone, PartialEq)]
pub struct CodexContextUsage {
    /// The latest response's usage, normalized.
    pub usage: Usage,
    /// Tokens the context holds now: the latest response's `totalTokens`.
    pub used_tokens: i64,
    /// The effective `modelContextWindow`, when the server reports one.
    pub context_window: Option<i64>,
    pub reasoning_tokens: i64,
    pub last_output_tokens: i64,
}

/// Read context occupancy from a token-usage notification's `params`.
///
/// Codex sends both `total` (cumulative across turns) and `last` (the latest
/// response, which is current context occupancy). Occupancy must come from
/// `last`; reading `total` recreates the historical >100% context bug, so this
/// never looks at it.
pub fn context_usage(params: &Value) -> Option<CodexContextUsage> {
    let last = params.pointer("/tokenUsage/last")?;
    let breakdown = CodexUsageBreakdown::from_value(last);
    let context_window = params
        .pointer("/tokenUsage/modelContextWindow")
        .or_else(|| params.pointer("/modelContextWindow"))
        .and_then(|v| v.as_u64())
        .map(|v| v as i64);
    Some(CodexContextUsage {
        usage: breakdown.into_usage(),
        used_tokens: u64_pointer_as_i64(last, "/totalTokens"),
        context_window,
        reasoning_tokens: breakdown.reasoning_output_tokens as i64,
        last_output_tokens: breakdown.output_tokens as i64,
    })
}
