//! The OpenRouter [`WireAdapter`]: the first and today the only adapter behind
//! the neutral HTTP turn loop. It owns every OpenRouter wire concern — the
//! chat-completions DTOs and streaming reader (`wire`, `http`), transcript↔
//! `ChatMessage` mapping (`conversation`), context trimming (`context`), the
//! provider-routing object, the api key, and the backend identity — and converts
//! a streamed `ChatResponse` into the neutral [`Generation`] at `post_generation`,
//! normalizing tool names in place so the stored/replayed history references the
//! dispatched verb rather than an alias.

use super::http::{build_provider_object, post_chat_completion};
use super::{openrouter_api_key, OPENROUTER_BACKEND_KEY, OPENROUTER_BACKEND_NAME};
use crate::backends::http_loop::{
    render_tool_result, repair, Connection, Generation, TurnToolCall, WireAdapter,
};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage, ChatResponse};
use crate::backends::openai_compat::{context, conversation};
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// The OpenRouter wire adapter. Holds the provider-routing object resolved once
/// per turn at construction (it reads settings.yaml), so the same object rides
/// every tool-iteration POST without re-reading settings.
pub(super) struct OpenRouterAdapter {
    provider: Value,
}

impl OpenRouterAdapter {
    pub(super) fn new(orch: &Orchestrator) -> Self {
        Self {
            provider: build_provider_object(&orch.get_settings().openrouter_routing),
        }
    }
}

impl WireAdapter for OpenRouterAdapter {
    type Message = ChatMessage;

    fn backend_key(&self) -> &'static str {
        OPENROUTER_BACKEND_KEY
    }

    fn backend_name(&self) -> &'static str {
        OPENROUTER_BACKEND_NAME
    }

    fn default_model(&self) -> &'static str {
        "openrouter/auto"
    }

    fn connection(&self, orch: &Orchestrator) -> Result<Connection, String> {
        openrouter_api_key(orch)
            .map(|api_key| Connection { api_key: Some(api_key) })
            .ok_or_else(|| "OpenRouter API key not configured. Add an OpenRouter API key in Settings → Providers.".to_string())
    }

    fn build_conversation(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        system_prompt: &str,
    ) -> Result<Vec<ChatMessage>, String> {
        conversation::build_conversation_messages(orch, config, session_id, system_prompt)
    }

    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64> {
        // The selected model's real context window, sourced from the discovered
        // OpenRouter catalog (not a hardcoded assumption).
        orch.context_window_for_context_tokens(OPENROUTER_BACKEND_KEY, Some(model))
    }

    fn fit_conversation<'a>(
        &self,
        messages: &'a [ChatMessage],
        context_window: Option<i64>,
    ) -> Cow<'a, [ChatMessage]> {
        context::fit_conversation(messages, context_window)
    }

    #[allow(clippy::too_many_arguments)]
    fn post_generation(
        &self,
        orch: &Orchestrator,
        run_db: &Arc<LocalDb>,
        connection: &Connection,
        model: &str,
        session_id: &str,
        outgoing: &[ChatMessage],
        config: &SessionConfig,
        run_id: &str,
        turn_id: Option<&str>,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Generation<ChatMessage>, String> {
        let response = post_chat_completion(
            orch,
            run_db,
            connection
                .api_key
                .as_deref()
                .ok_or_else(|| "OpenRouter connection missing API key".to_string())?,
            model,
            session_id,
            outgoing,
            config,
            run_id,
            turn_id,
            &self.provider,
            cancel,
        )?;
        into_generation(response)
    }

    fn render_tool_result_message(
        &self,
        tool_call_id: &str,
        output: DispatchOutput,
    ) -> ChatMessage {
        ChatMessage::tool(tool_call_id.to_string(), render_tool_result(output))
    }
}

/// Convert a streamed OpenRouter `ChatResponse` into the neutral `Generation`,
/// canonicalizing tool names in place BEFORE anything is stored, pushed into the
/// conversation, or executed, so execution, the stored transcript, and any
/// replay/resume all reference the dispatched verb. Otherwise a successful call
/// replays under an invalid name like `Write_File` or `mcp__cairn__run` and
/// reinforces it in the model-facing history.
fn into_generation(response: ChatResponse) -> Result<Generation<ChatMessage>, String> {
    let ChatResponse {
        id,
        model,
        choices,
        usage,
        streamed_text,
        finish_reason,
    } = response;
    let Some(choice) = choices.into_iter().next() else {
        return Err("OpenRouter response did not include choices".to_string());
    };
    let mut assistant_message = choice.message;
    if let Some(calls) = assistant_message.tool_calls.as_mut() {
        for call in calls.iter_mut() {
            if let Some(verb) = repair::normalize_tool_name(&call.function.name) {
                if verb != call.function.name {
                    log::warn!(
                        "OpenRouter normalized tool name {:?} -> {:?}",
                        call.function.name,
                        verb
                    );
                    call.function.name = verb.to_string();
                }
            }
        }
    }
    let assistant_text = assistant_message
        .content
        .as_ref()
        .and_then(ChatContent::as_text)
        .unwrap_or_default()
        .to_string();
    let tool_calls = assistant_message
        .tool_calls
        .as_ref()
        .map(|calls| {
            calls
                .iter()
                .map(|call| TurnToolCall {
                    id: call.id.clone(),
                    name: call.function.name.clone(),
                    arguments: call.function.arguments.clone(),
                })
                .collect()
        })
        .unwrap_or_default();
    let reasoning_details = assistant_message.reasoning_details.clone();
    Ok(Generation {
        assistant_message,
        assistant_text,
        tool_calls,
        reasoning_details,
        usage,
        finish_reason,
        generation_id: id,
        response_model: model,
        streamed_text,
    })
}
