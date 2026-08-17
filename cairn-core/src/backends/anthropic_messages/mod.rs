//! The Anthropic Messages protocol family.
//!
//! A reusable sibling of `openai_compat`, not a provider integration: it owns
//! the Messages wire format — content blocks, named SSE events, thinking blocks,
//! `tool_use`/`tool_result` pairing — and nothing about who serves it. The URL,
//! the authentication header, and the output cap arrive as
//! [`http::MessagesEndpoint`], so the same code serves OpenCode Go's gateway
//! today and a direct Anthropic API key later without a provider branch
//! anywhere inside it.
//!
//! It converges with the other families only at [`WireAdapter`], and shares with
//! them exactly two things: Cairn's neutral tool definitions and Cairn's
//! persisted transcript. No chat-completions DTO is reused here, and no
//! Anthropic block is ever handed to one.

pub(crate) mod conversation;
pub(crate) mod generation;
pub(crate) mod http;
pub(crate) mod wire;

#[cfg(test)]
mod tests;

use crate::backends::http_loop::{render_tool_result, Connection, Generation, WireAdapter};
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use http::MessagesEndpoint;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use wire::MessagesMessage;

/// A [`WireAdapter`] speaking Anthropic Messages against a configured endpoint.
///
/// The provider supplies its identity and its endpoint as plain data and
/// function pointers, which is what keeps this type reusable: there is no
/// provider enum to extend and no OpenCode-shaped hole to fill.
pub(crate) struct AnthropicMessagesAdapter {
    pub(crate) backend_key: &'static str,
    pub(crate) backend_name: &'static str,
    pub(crate) default_model: &'static str,
    pub(crate) connect: fn(&Orchestrator) -> Result<Connection, String>,
    pub(crate) endpoint: fn(&str) -> MessagesEndpoint,
    pub(crate) context_window: fn(&Orchestrator, &str) -> Option<i64>,
}

impl AnthropicMessagesAdapter {
    fn endpoint_for(&self, connection: &Connection) -> Result<MessagesEndpoint, String> {
        let api_key = connection
            .api_key
            .as_deref()
            .ok_or_else(|| format!("{} connection missing API key", self.backend_name))?;
        Ok((self.endpoint)(api_key))
    }
}

impl WireAdapter for AnthropicMessagesAdapter {
    type Message = MessagesMessage;

    fn backend_key(&self) -> &'static str {
        self.backend_key
    }

    fn backend_name(&self) -> &'static str {
        self.backend_name
    }

    fn default_model(&self) -> &'static str {
        self.default_model
    }

    fn connection(&self, orch: &Orchestrator) -> Result<Connection, String> {
        (self.connect)(orch)
    }

    fn build_conversation(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        system_prompt: &str,
    ) -> Result<Vec<MessagesMessage>, String> {
        conversation::build_conversation_messages(orch, config, session_id, system_prompt)
    }

    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64> {
        (self.context_window)(orch, model)
    }

    fn fit_conversation<'a>(
        &self,
        messages: &'a [MessagesMessage],
        context_window: Option<i64>,
    ) -> Cow<'a, [MessagesMessage]> {
        conversation::fit_conversation(messages, context_window)
    }

    #[allow(clippy::too_many_arguments)]
    fn post_generation(
        &self,
        orch: &Orchestrator,
        run_db: &Arc<LocalDb>,
        connection: &Connection,
        model: &str,
        session_id: &str,
        outgoing: &[MessagesMessage],
        config: &SessionConfig,
        run_id: &str,
        turn_id: Option<&str>,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Generation<MessagesMessage>, String> {
        let endpoint = self.endpoint_for(connection)?;
        let body = http::build_body(&endpoint, model, outgoing, config, true);
        let response = http::post_message_streaming(
            orch, run_db, &endpoint, body, run_id, session_id, turn_id, cancel,
        )?;
        generation::into_generation(response, self.backend_name)
    }

    fn render_tool_result_message(
        &self,
        tool_call_id: &str,
        output: DispatchOutput,
    ) -> MessagesMessage {
        MessagesMessage::tool_result(tool_call_id.to_string(), render_tool_result(output))
    }
}
