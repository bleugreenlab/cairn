//! OpenCode Go's endpoint and authentication facts.
//!
//! Go fronts three protocol families behind one base URL, and this module is
//! only where each one lives and how it authenticates. The request bodies and
//! stream parsers belong to their protocol modules (`openai_compat`,
//! `anthropic_messages`, `openai_responses`), which know nothing about OpenCode.

use super::{OPENCODE_BACKEND_KEY, OPENCODE_BACKEND_NAME};
use crate::backends::anthropic_messages::http::MessagesEndpoint;
use crate::backends::openai_compat::http::{
    apply_output_schema, post_chat_completion_streaming, Endpoint,
};
use crate::backends::openai_compat::wire::{ChatMessage, ChatResponse};
use crate::backends::openai_responses::http::ResponsesEndpoint;
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub(super) const CHAT_URL: &str = "https://opencode.ai/zen/go/v1/chat/completions";
pub(super) const MESSAGES_URL: &str = "https://opencode.ai/zen/go/v1/messages";
pub(super) const RESPONSES_URL: &str = "https://opencode.ai/zen/go/v1/responses";

/// The `max_tokens` every Messages request carries.
///
/// That protocol REQUIRES the field — omitting it against this gateway is an
/// upstream 500, not a default — and the value is a per-turn output cap rather
/// than a budget the subscription meters, so it is set generously enough not to
/// truncate a real coding turn.
const MESSAGES_MAX_OUTPUT_TOKENS: i64 = 32_000;

/// Build the request body for one generation.
///
/// `stream_options.include_usage` is what makes a streamed Go turn report its
/// token counts at all; without it an OpenAI-compatible stream ends with no
/// usage block and the run records nothing to meter against the subscription's
/// dollar-denominated limits.
///
/// The reasoning effort passes through as the model received it. Go's models
/// publish different effort vocabularies (`max` alone, `none`/`low`/`high`,
/// `high`/`max`), so Cairn does not invent a common ladder: an effort a model
/// does not accept comes back as the provider's own refusal, which says more
/// than a value silently coerced into something else.
pub(super) fn build_body(model: &str, messages: &[ChatMessage], config: &SessionConfig) -> Value {
    let mut body = json!({
        "model": model,
        "messages": messages,
        "tools": crate::backends::openai_compat::tool_schemas(),
        "tool_choice": "auto",
        "stream": true,
        "stream_options": { "include_usage": true },
    });
    if let Some(effort) = config.reasoning_effort.as_deref() {
        body["reasoning_effort"] = json!(effort);
    }
    apply_output_schema(&mut body, config.output_schema.as_ref());
    body
}

pub(super) fn endpoint(api_key: &str) -> Endpoint {
    Endpoint {
        provider_name: OPENCODE_BACKEND_NAME,
        backend_key: OPENCODE_BACKEND_KEY,
        chat_url: CHAT_URL.to_string(),
        headers: vec![
            ("authorization".to_string(), format!("Bearer {api_key}")),
            ("content-type".to_string(), "application/json".to_string()),
        ],
        extra_body: None,
    }
}

/// Go's Messages endpoint authenticates with Anthropic's own `x-api-key` header,
/// NOT the Bearer token its other two endpoints take.
///
/// Verified against the live gateway on 2026-08-16: the identical request
/// authenticated with `Authorization: Bearer` is refused with `401 AuthError:
/// Missing API key`, while `x-api-key` succeeds. `anthropic-version` is accepted
/// but not required, so it is not sent.
pub(super) fn messages_endpoint(api_key: &str) -> MessagesEndpoint {
    MessagesEndpoint {
        provider_name: OPENCODE_BACKEND_NAME,
        backend_key: OPENCODE_BACKEND_KEY,
        url: MESSAGES_URL.to_string(),
        headers: vec![
            ("x-api-key".to_string(), api_key.to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ],
        max_output_tokens: MESSAGES_MAX_OUTPUT_TOKENS,
    }
}

pub(super) fn responses_endpoint(api_key: &str) -> ResponsesEndpoint {
    ResponsesEndpoint {
        provider_name: OPENCODE_BACKEND_NAME,
        backend_key: OPENCODE_BACKEND_KEY,
        url: RESPONSES_URL.to_string(),
        headers: vec![
            ("authorization".to_string(), format!("Bearer {api_key}")),
            ("content-type".to_string(), "application/json".to_string()),
        ],
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn post_chat_completion(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    api_key: &str,
    model: &str,
    session_id: &str,
    messages: &[ChatMessage],
    config: &SessionConfig,
    run_id: &str,
    turn_id: Option<&str>,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    post_chat_completion_streaming(
        orch,
        run_db,
        &endpoint(api_key),
        build_body(model, messages, config),
        run_id,
        session_id,
        turn_id,
        cancel,
    )
}
