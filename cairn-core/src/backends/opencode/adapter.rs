//! The OpenCode Go [`WireAdapter`] behind the neutral HTTP turn loop.

use super::http::post_chat_completion;
use super::{opencode_api_key, DEFAULT_MODEL, OPENCODE_BACKEND_KEY, OPENCODE_BACKEND_NAME};
use crate::backends::http_loop::{render_tool_result, Connection, Generation, WireAdapter};
use crate::backends::openai_compat::generation::into_generation;
use crate::backends::openai_compat::wire::ChatMessage;
use crate::backends::openai_compat::{context, conversation};
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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
        opencode_api_key(orch)
            .map(|api_key| Connection {
                api_key: Some(api_key),
            })
            .ok_or_else(|| {
                "OpenCode Go API key not configured. Add an OpenCode key in Settings → Providers."
                    .to_string()
            })
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
        // catalog rather than assumed. Go's line-up ranges from 256K to 1M, so
        // a single hardcoded number would be wrong for most of it.
        orch.context_window_for_context_tokens(OPENCODE_BACKEND_KEY, Some(model))
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
