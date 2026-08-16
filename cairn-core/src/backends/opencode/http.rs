//! The OpenCode Go streaming chat-completions POST.
//!
//! Go's `chat/completions` endpoint is plain OpenAI-compatible, so the request
//! carries no provider-specific object the way OpenRouter's routing preferences
//! do — the shared transport in `openai_compat::http` does the rest.

use super::{OPENCODE_BACKEND_KEY, OPENCODE_BACKEND_NAME};
use crate::backends::openai_compat::http::{
    apply_output_schema, post_chat_completion_streaming, Endpoint,
};
use crate::backends::openai_compat::wire::{ChatMessage, ChatResponse};
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub(super) const CHAT_URL: &str = "https://opencode.ai/zen/go/v1/chat/completions";

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
