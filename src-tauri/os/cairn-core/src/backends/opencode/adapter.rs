//! OpenCode Go's [`WireAdapter`]s behind the neutral HTTP turn loop.
//!
//! Three protocol families, one provider. Each adapter here is the family's
//! reusable implementation configured with Go's endpoints, so this file adds
//! endpoint facts rather than a second parser.

use super::{opencode_api_key, DEFAULT_MODEL, OPENCODE_BACKEND_KEY, OPENCODE_BACKEND_NAME};
use crate::backends::anthropic_messages::AnthropicMessagesAdapter;
use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::openai_compat::conversation;
use crate::backends::openai_compat::http::ChatDialect;
use crate::backends::openai_compat::wire::ChatMessage;
use crate::backends::openai_responses::OpenAiResponsesAdapter;
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use cairn_models::turn::ChatFamily;
use serde_json::Map;

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

/// Go's chat family on `api_key`: the one an agent turn and a one-shot
/// completion are both built from.
pub(super) fn chat_family(api_key: &str) -> ChatFamily {
    ChatFamily {
        endpoint: super::http::endpoint(api_key),
        dialect: ChatDialect::default(),
        extras: Map::new(),
    }
}

pub(super) struct OpenCodeAdapter;

impl WireAdapter for OpenCodeAdapter {
    type Family = ChatFamily;

    fn backend_key(&self) -> &str {
        OPENCODE_BACKEND_KEY
    }

    fn backend_name(&self) -> &str {
        OPENCODE_BACKEND_NAME
    }

    fn default_model(&self) -> &'static str {
        DEFAULT_MODEL
    }

    fn connection(&self, orch: &Orchestrator) -> Result<Connection, String> {
        go_connection(orch)
    }

    /// The default dialect is what Go's chat endpoint takes.
    /// `stream_options.include_usage` is what makes a streamed Go turn report
    /// its token counts at all; without it the run records nothing to meter
    /// against the subscription's dollar-denominated limits. The effort passes
    /// through as written, because Go's models publish different effort
    /// vocabularies (`max` alone, `none`/`low`/`high`, `high`/`max`).
    fn family(&self, connection: &Connection) -> Result<ChatFamily, String> {
        let api_key = connection
            .api_key
            .as_deref()
            .ok_or_else(|| "OpenCode Go connection missing API key".to_string())?;
        Ok(chat_family(api_key))
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
}
