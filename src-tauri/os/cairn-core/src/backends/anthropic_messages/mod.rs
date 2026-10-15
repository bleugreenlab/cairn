//! Application adapter for the Anthropic Messages protocol in `cairn-models`.
//! Core supplies credentials, model selection, and history loading.

pub(crate) mod conversation;
pub(crate) mod http;
pub(crate) use cairn_models::wire::anthropic_messages::wire;

use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use cairn_models::turn::MessagesFamily;
use http::MessagesEndpoint;
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

impl WireAdapter for AnthropicMessagesAdapter {
    type Family = MessagesFamily;

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

    fn family(&self, connection: &Connection) -> Result<MessagesFamily, String> {
        let api_key = connection
            .api_key
            .as_deref()
            .ok_or_else(|| format!("{} connection missing API key", self.backend_name))?;
        Ok(MessagesFamily {
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
    ) -> Result<Vec<MessagesMessage>, String> {
        conversation::build_conversation_messages(orch, config, session_id, system_prompt)
    }

    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64> {
        (self.context_window)(orch, model)
    }
}
