//! Application adapter for the OpenAI Responses protocol in `cairn-models`.
//! Core supplies credentials, model selection, and history loading.

pub(crate) mod conversation;
pub(crate) mod http;
pub(crate) use cairn_models::wire::openai_responses::wire;

use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use cairn_models::turn::ResponsesFamily;
use http::ResponsesEndpoint;
use wire::ResponsesTurn;

/// A [`WireAdapter`] speaking OpenAI Responses against a configured endpoint.
pub(crate) struct OpenAiResponsesAdapter {
    pub(crate) backend_key: &'static str,
    pub(crate) backend_name: &'static str,
    pub(crate) default_model: &'static str,
    pub(crate) connect: fn(&Orchestrator) -> Result<Connection, String>,
    pub(crate) endpoint: fn(&str) -> ResponsesEndpoint,
    pub(crate) context_window: fn(&Orchestrator, &str) -> Option<i64>,
}

impl WireAdapter for OpenAiResponsesAdapter {
    type Family = ResponsesFamily;

    fn backend_key(&self) -> &str {
        self.backend_key
    }

    fn backend_name(&self) -> &str {
        self.backend_name
    }

    fn default_model(&self) -> &'static str {
        self.default_model
    }

    fn connection(&self, orch: &Orchestrator) -> Result<Connection, String> {
        (self.connect)(orch)
    }

    fn family(&self, connection: &Connection) -> Result<ResponsesFamily, String> {
        let api_key = connection
            .api_key
            .as_deref()
            .ok_or_else(|| format!("{} connection missing API key", self.backend_name))?;
        Ok(ResponsesFamily {
            endpoint: (self.endpoint)(api_key),
            extras: serde_json::Map::new(),
        })
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
}
