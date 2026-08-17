//! The OpenAI Responses protocol family.
//!
//! A reusable sibling of `openai_compat`, not a provider integration: it owns
//! the Responses wire format — flat input/output items, typed lifecycle events,
//! reasoning items, `function_call`/`function_call_output` pairing — and nothing
//! about who serves it. The URL and authentication arrive as
//! [`http::ResponsesEndpoint`], so the same code serves OpenCode Go's gateway
//! today and a direct OpenAI API key later.
//!
//! Responses is NOT chat/completions with different field names, which is why it
//! is a sibling rather than a mode of `openai_compat`: there is no message array
//! and no `choices`, a turn is several sibling items rather than one message,
//! reasoning is a first-class item with opaque content that must round-trip, and
//! the terminal event reports a status (`completed`, `failed`, `incomplete`)
//! rather than a finish reason.

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
use http::ResponsesEndpoint;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use wire::{ResponsesItem, ResponsesTurn};

/// A [`WireAdapter`] speaking OpenAI Responses against a configured endpoint.
pub(crate) struct OpenAiResponsesAdapter {
    pub(crate) backend_key: &'static str,
    pub(crate) backend_name: &'static str,
    pub(crate) default_model: &'static str,
    pub(crate) connect: fn(&Orchestrator) -> Result<Connection, String>,
    pub(crate) endpoint: fn(&str) -> ResponsesEndpoint,
    pub(crate) context_window: fn(&Orchestrator, &str) -> Option<i64>,
}

impl OpenAiResponsesAdapter {
    fn endpoint_for(&self, connection: &Connection) -> Result<ResponsesEndpoint, String> {
        let api_key = connection
            .api_key
            .as_deref()
            .ok_or_else(|| format!("{} connection missing API key", self.backend_name))?;
        Ok((self.endpoint)(api_key))
    }
}

impl WireAdapter for OpenAiResponsesAdapter {
    type Message = ResponsesTurn;

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
    ) -> Result<Vec<ResponsesTurn>, String> {
        conversation::build_conversation_messages(orch, config, session_id, system_prompt)
    }

    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64> {
        (self.context_window)(orch, model)
    }

    fn fit_conversation<'a>(
        &self,
        messages: &'a [ResponsesTurn],
        context_window: Option<i64>,
    ) -> Cow<'a, [ResponsesTurn]> {
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
        outgoing: &[ResponsesTurn],
        config: &SessionConfig,
        run_id: &str,
        turn_id: Option<&str>,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Generation<ResponsesTurn>, String> {
        let endpoint = self.endpoint_for(connection)?;
        let body = http::build_body(model, outgoing, config, true);
        let response = http::post_response_streaming(
            orch, run_db, &endpoint, body, run_id, session_id, turn_id, cancel,
        )?;
        generation::into_generation(response, self.backend_name)
    }

    fn render_tool_result_message(
        &self,
        tool_call_id: &str,
        output: DispatchOutput,
    ) -> ResponsesTurn {
        ResponsesTurn::one(ResponsesItem::FunctionCallOutput {
            call_id: tool_call_id.to_string(),
            output: render_tool_result(output),
        })
    }
}
