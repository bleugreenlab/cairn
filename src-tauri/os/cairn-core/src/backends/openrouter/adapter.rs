//! The OpenRouter [`WireAdapter`] behind the neutral HTTP turn loop: the api
//! key, the backend identity, transcript replay into chat messages
//! (`conversation`), and the chat family configured with OpenRouter's
//! endpoint, its dialect, and its provider-routing object.

use super::http::{build_provider_object, openrouter_headers, CHAT_URL};
use super::{openrouter_api_key, OPENROUTER_BACKEND_KEY, OPENROUTER_BACKEND_NAME};
use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::openai_compat::conversation;
use crate::backends::openai_compat::http::{ChatDialect, ChatReasoning, Endpoint};
use crate::backends::openai_compat::wire::ChatMessage;
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use cairn_models::turn::ChatFamily;
use serde_json::{Map, Value};

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
    type Family = ChatFamily;

    fn backend_key(&self) -> &str {
        OPENROUTER_BACKEND_KEY
    }

    fn backend_name(&self) -> &str {
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

    /// The routing object always carries `require_parameters`, which is also
    /// what a schema-constrained request needs: it routes only to providers
    /// that honor every parameter sent, the schema included.
    fn family(&self, connection: &Connection) -> Result<ChatFamily, String> {
        let api_key = connection
            .api_key
            .as_deref()
            .ok_or_else(|| "OpenRouter connection missing API key".to_string())?;
        Ok(ChatFamily {
            endpoint: Endpoint {
                provider_name: OPENROUTER_BACKEND_NAME.to_string(),
                backend_key: OPENROUTER_BACKEND_KEY.to_string(),
                chat_url: CHAT_URL.to_string(),
                headers: openrouter_headers(api_key),
            },
            dialect: ChatDialect {
                include_usage: false,
                reasoning: ChatReasoning::Object,
                ..ChatDialect::default()
            },
            extras: Map::from_iter([("provider".to_string(), self.provider.clone())]),
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
        // OpenRouter catalog (not a hardcoded assumption).
        orch.context_window_for_context_tokens(OPENROUTER_BACKEND_KEY, Some(model))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::openai_compat::completion_body;
    use crate::backends::{CompletionMessage, CompletionRequest, CompletionRole};
    use crate::models::{OpenRouterRouting, RuntimeExtras};
    use serde_json::json;

    #[test]
    fn a_completion_sends_effort_as_the_reasoning_object_beside_the_routing() {
        let family = OpenRouterAdapter {
            provider: build_provider_object(&OpenRouterRouting::default()),
        }
        .family(&Connection {
            api_key: Some("key".to_string()),
        })
        .unwrap();
        let request = CompletionRequest {
            system: None,
            messages: vec![CompletionMessage {
                role: CompletionRole::User,
                content: "hi".into(),
            }],
            model: "openai/gpt-6".into(),
            project_id: None,
            options: RuntimeExtras {
                reasoning_effort: Some("low".to_string()),
                ..Default::default()
            },
            output_schema: Some(json!({"type": "object"})),
            timeout: std::time::Duration::from_secs(5),
        };
        let body = completion_body(&request, &family).unwrap();
        assert_eq!(body["reasoning"], json!({"effort": "low"}));
        assert!(body.get("reasoningEffort").is_none(), "{body}");
        // The routing object always requires parameters, which is what keeps a
        // schema-constrained completion on providers that honour the schema.
        assert_eq!(body["provider"]["require_parameters"], json!(true));
        assert_eq!(
            body["response_format"]["json_schema"]["strict"],
            json!(true)
        );
        assert_eq!(body["stream"], json!(false));
    }
}
