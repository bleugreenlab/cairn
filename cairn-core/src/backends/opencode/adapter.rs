//! OpenCode Go's [`WireAdapter`]s behind the neutral HTTP turn loop.
//!
//! Three protocol families, one provider. The chat-completions adapter is
//! written out here because it predates the seam; the Messages and Responses
//! adapters are the reusable protocol implementations configured with Go's
//! endpoints, so this file adds endpoint facts rather than a second parser.

use super::http::post_chat_completion;
use super::{opencode_api_key, DEFAULT_MODEL, OPENCODE_BACKEND_KEY, OPENCODE_BACKEND_NAME};
use crate::backends::anthropic_messages::AnthropicMessagesAdapter;
use crate::backends::http_loop::{render_tool_result, Connection, Generation, WireAdapter};
use crate::backends::openai_compat::generation::into_generation;
use crate::backends::openai_compat::wire::ChatMessage;
use crate::backends::openai_compat::{context, conversation};
use crate::backends::openai_responses::OpenAiResponsesAdapter;
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Resolve Go's API key into a connection, or own the missing-key error.
///
/// Shared by all three adapters: the credential is the account key regardless of
/// which endpoint family the selected model is served over.
pub(super) fn go_connection(orch: &Orchestrator) -> Result<Connection, String> {
    opencode_api_key(orch)
        .map(|api_key| Connection {
            api_key: Some(api_key),
        })
        .ok_or_else(|| {
            "OpenCode Go API key not configured. Add an OpenCode key in Settings → Providers."
                .to_string()
        })
}

/// The selected model's real context window, sourced from the discovered catalog
/// rather than assumed. Go's line-up ranges from 256K to 1M, so a single
/// hardcoded number would be wrong for most of it.
pub(super) fn go_context_window(orch: &Orchestrator, model: &str) -> Option<i64> {
    orch.context_window_for_context_tokens(OPENCODE_BACKEND_KEY, Some(model))
}

/// Go's Anthropic Messages models (the MiniMax line), served by the reusable
/// protocol adapter with Go's endpoint and its `x-api-key` authentication.
pub(super) fn messages_adapter() -> AnthropicMessagesAdapter {
    AnthropicMessagesAdapter {
        backend_key: OPENCODE_BACKEND_KEY,
        backend_name: OPENCODE_BACKEND_NAME,
        default_model: DEFAULT_MODEL,
        connect: go_connection,
        endpoint: super::http::messages_endpoint,
        context_window: go_context_window,
    }
}

/// Go's OpenAI Responses models (Grok 4.5 and GPT 5.6 Luna), served by the
/// reusable protocol adapter with Go's endpoint.
pub(super) fn responses_adapter() -> OpenAiResponsesAdapter {
    OpenAiResponsesAdapter {
        backend_key: OPENCODE_BACKEND_KEY,
        backend_name: OPENCODE_BACKEND_NAME,
        default_model: DEFAULT_MODEL,
        connect: go_connection,
        endpoint: super::http::responses_endpoint,
        context_window: go_context_window,
    }
}

pub(super) struct OpenCodeAdapter;

impl WireAdapter for OpenCodeAdapter {
    type Message = ChatMessage;

    fn backend_key(&self) -> &'static str {
        OPENCODE_BACKEND_KEY
    }

    fn backend_name(&self) -> &'static str {
        OPENCODE_BACKEND_NAME
    }

    fn default_model(&self) -> &'static str {
        DEFAULT_MODEL
    }

    fn connection(&self, orch: &Orchestrator) -> Result<Connection, String> {
        go_connection(orch)
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
        go_context_window(orch, model)
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
                .ok_or_else(|| "OpenCode Go connection missing API key".to_string())?,
            model,
            session_id,
            outgoing,
            config,
            run_id,
            turn_id,
            cancel,
        )?;
        into_generation(response, OPENCODE_BACKEND_NAME)
    }

    fn render_tool_result_message(
        &self,
        tool_call_id: &str,
        output: DispatchOutput,
    ) -> ChatMessage {
        ChatMessage::tool(tool_call_id.to_string(), render_tool_result(output))
    }
}
