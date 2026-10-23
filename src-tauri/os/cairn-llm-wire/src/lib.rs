//! Cairn's model wires: the three HTTP protocol families (Anthropic Messages,
//! OpenAI-compatible chat, OpenAI Responses) with their generation assembly and
//! replay, and the two CLI stream wires (Claude stream-json and the Codex
//! app-server's JSON-RPC notifications), the two decision wires (OpenAI's
//! Decisions and System One), plus the embedding wires (OpenAI's
//! `embeddings` and Cairn's gateway `/embed`).
//!
//! A leaf: it reaches no other workspace crate, so a host that only needs to
//! speak these protocols can depend on it without the session runtime.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod anthropic_messages;
pub mod claude_stream;
pub mod codex_app_server;
pub mod decisions;
pub mod embeddings;
pub mod openai_compat;
pub mod openai_responses;
pub mod repair;
pub mod replay;
pub mod schema_order;
pub mod stream;
mod stream_end;
pub mod systemone;
pub mod tool_defs;

pub use replay::{MessageContent, MessageImage, ReplayEvent, ToolUseInfo};
pub use stream_end::require_terminal_event;
pub use tool_defs::{cairn_tool_definitions, ToolDefinition};

/// Request settings used by the HTTP protocols, independent of a running session.
#[derive(Debug, Clone, Default)]
pub struct GenerationConfig {
    pub reasoning_effort: Option<String>,
    pub max_thinking_tokens: Option<i32>,
    pub output_schema: Option<Value>,
    /// The tools the request advertises, in this order. Empty sends no tools
    /// at all (no `tools` or `tool_choice` key), a tool-free generation.
    pub tools: Vec<ToolDefinition>,
}

/// Rough chars-per-token ratio for the outgoing-trim heuristic. English and code
/// average ~3.5–4 characters per token; 4 deliberately under-counts token
/// volume, with the slack absorbed by trimming to a fraction of the window
/// ([`context_fit_budget`]) rather than to its exact edge.
///
/// Neutral on purpose: how many characters make a token is a property of
/// tokenizers, not of whether the request is shaped as chat messages, Anthropic
/// content blocks, or Responses items. All three families share this policy so
/// they cannot drift into trimming to different budgets.
pub const CHARS_PER_TOKEN: i64 = 4;

/// The most recent messages whose tool outputs are never collapsed, so the model
/// keeps full fidelity on the exchange it is actively reasoning about.
pub const PROTECT_RECENT_MESSAGES: usize = 8;

/// Trim the outgoing request down to this fraction of the model's real context
/// window. The remaining headroom covers the response (including reasoning
/// tokens) plus slack for the approximate char-per-token estimate.
const CONTEXT_FIT_FRACTION: f64 = 0.75;

/// The token budget an outgoing request is trimmed to fit under.
pub fn context_fit_budget(context_window: i64) -> i64 {
    ((context_window as f64) * CONTEXT_FIT_FRACTION) as i64
}

/// The sentence a provider wrote inside a JSON error body.
///
/// Most providers nest the message as `{"error": {"message": ...}}`, with or
/// without a sibling `type` or `code`; some put the sentence straight in
/// `{"error": "..."}` (Cairn's gateway, some llama.cpp builds), and FastAPI
/// servers (vLLM) answer `{"detail": "..."}`. Any of them reaches the
/// transcript as the explanation the provider gave ("No payment method",
/// "Model X is not supported") instead of a wall of JSON the reader has to
/// decode. Anything that is not JSON, or that carries no message, passes
/// through untouched: an unrecognized body is still evidence, and dropping it
/// would leave the failure unexplained.
pub fn upstream_error_detail(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|parsed| {
            if let Some(sentence) = parsed.get("error").and_then(Value::as_str) {
                return Some(sentence.to_string());
            }
            let error = parsed.get("error").unwrap_or(&parsed);
            error
                .get("message")
                .or_else(|| parsed.get("detail"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| body.to_string())
}

/// A model-emitted tool call, neutralized from the adapter's wire type: the
/// dispatch id, the (already tool-name-normalized) verb, and the raw JSON
/// argument string the tool dispatch / repair path parses.
pub struct TurnToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// OpenAI-style usage payload — the neutral HTTP-loop usage shape. Its field
/// names and serde attributes MUST stay byte-identical to what the stored
/// transcript's `raw.usage` has always embedded (the frontend's two-event token
/// rollup and the radial context dial read these keys), so this is deliberately
/// the OpenAI/OpenRouter usage shape rather than a reinvented one. Every field
/// is `#[serde(default)]` with NO `skip_serializing_if`, so serialization always
/// emits all eight keys (nulls included), exactly as before.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnUsage {
    #[serde(default)]
    pub prompt_tokens: Option<i32>,
    #[serde(default)]
    pub completion_tokens: Option<i32>,
    #[serde(default)]
    pub total_tokens: Option<i32>,
    #[serde(default)]
    pub reasoning_tokens: Option<i32>,
    #[serde(default)]
    pub prompt_tokens_details: Option<Value>,
    #[serde(default)]
    pub completion_tokens_details: Option<Value>,
    #[serde(default)]
    pub cost: Option<f64>,
    #[serde(default)]
    pub cost_details: Option<Value>,
}

impl TurnUsage {
    /// Map an Anthropic Messages `usage` object onto the neutral shape.
    ///
    /// Anthropic reports DISJOINT input components: `input_tokens` counts only
    /// the uncached prompt, with cache reads and cache writes alongside it. The
    /// neutral shape is OpenRouter's, where `prompt_tokens` is the WHOLE prompt
    /// and `prompt_tokens_details` names the parts of it read from cache
    /// (`cached_tokens`) and written to it (`cache_write_tokens`). So the
    /// components are summed here rather than passed through: without this, a
    /// 128-token cache read against a 40-token prompt would read as negative
    /// uncached input and under-report the prompt by the entire cached prefix.
    pub fn from_anthropic(
        input_tokens: Option<i32>,
        output_tokens: Option<i32>,
        cache_read: Option<i32>,
        cache_creation: Option<i32>,
        cost: Option<f64>,
    ) -> Self {
        let prompt_tokens = input_tokens.map(|input| {
            input
                .saturating_add(cache_read.unwrap_or(0))
                .saturating_add(cache_creation.unwrap_or(0))
        });
        let prompt_tokens_details = (cache_read.is_some() || cache_creation.is_some()).then(|| {
            let mut details = serde_json::Map::new();
            if let Some(cache_read) = cache_read {
                details.insert("cached_tokens".to_string(), Value::from(cache_read));
            }
            if let Some(cache_creation) = cache_creation {
                details.insert(
                    "cache_write_tokens".to_string(),
                    Value::from(cache_creation),
                );
            }
            Value::Object(details)
        });
        Self {
            prompt_tokens,
            completion_tokens: output_tokens,
            total_tokens: match (prompt_tokens, output_tokens) {
                (None, None) => None,
                (prompt, completion) => {
                    Some(prompt.unwrap_or(0).saturating_add(completion.unwrap_or(0)))
                }
            },
            reasoning_tokens: None,
            prompt_tokens_details,
            completion_tokens_details: None,
            cost,
            cost_details: None,
        }
    }

    /// Map an OpenAI Responses `usage` object onto the neutral shape.
    ///
    /// Responses already counts the way the neutral shape does — `input_tokens`
    /// is the whole prompt and `input_tokens_details.cached_tokens` its cached
    /// subset — so the components pass through rather than being recombined. The
    /// detail objects are carried verbatim, which is what preserves
    /// `output_tokens_details.reasoning_tokens` for the context dial.
    pub fn from_responses(
        input_tokens: Option<i32>,
        output_tokens: Option<i32>,
        total_tokens: Option<i32>,
        input_tokens_details: Option<Value>,
        output_tokens_details: Option<Value>,
        cost: Option<f64>,
    ) -> Self {
        let reasoning_tokens = output_tokens_details.as_ref().and_then(|details| {
            details
                .get("reasoning_tokens")
                .and_then(Value::as_i64)
                .map(|tokens| tokens as i32)
        });
        Self {
            prompt_tokens: input_tokens,
            completion_tokens: output_tokens,
            total_tokens: total_tokens.or_else(|| match (input_tokens, output_tokens) {
                (None, None) => None,
                (input, output) => Some(input.unwrap_or(0).saturating_add(output.unwrap_or(0))),
            }),
            reasoning_tokens,
            prompt_tokens_details: input_tokens_details,
            completion_tokens_details: output_tokens_details,
            cost,
            cost_details: None,
        }
    }
}

/// One assistant generation, neutralized from the adapter's wire response. The
/// loop owns this and never sees a wire DTO: `assistant_message` (the adapter's
/// own message type) is pushed back onto the running conversation verbatim,
/// while the neutral fields drive execution, persistence, and the context
/// snapshot.
pub struct Generation<M> {
    pub assistant_message: M,
    pub assistant_text: String,
    pub tool_calls: Vec<TurnToolCall>,
    pub reasoning_details: Option<Value>,
    pub usage: Option<TurnUsage>,
    pub finish_reason: Option<String>,
    pub generation_id: Option<String>,
    pub response_model: Option<String>,
    pub streamed_text: bool,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompletionError {
    #[error("backend does not support one-shot completions")]
    BackendUnavailable,
    #[error("completion timed out")]
    Timeout,
    #[error("completion request is invalid: {0}")]
    InvalidRequest(String),
    #[error("completion provider failed: {0}")]
    Upstream(String),
    #[error("completion response was invalid: {0}")]
    InvalidResponse(String),
    /// A streamed generation failed. The text is the transport's own message,
    /// displayed unchanged.
    #[error("{0}")]
    Stream(String),
}

impl CompletionError {
    /// The message a streamed generation reports: a stream failure's own text,
    /// or any other failure's display.
    pub fn into_stream_text(self) -> String {
        match self {
            Self::Stream(text) => text,
            other => other.to_string(),
        }
    }
}
