//! The wire adapter for a custom provider.
//!
//! There is no custom transport here. The endpoint is handed to the shared
//! OpenAI-compatible chat-completions transport that OpenRouter, OpenCode, and
//! Ollama already use, so a custom provider inherits the same streaming
//! aggregation, fragmented-tool-call reassembly, reasoning handling, terminal-
//! event invariant, and cancellation semantics as every other provider on that
//! wire — and cannot drift from them.

use super::CustomProviderConfig;
use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::openai_compat::http::{ChatDialect, ChatReasoning, Endpoint};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage};
use crate::backends::openai_compat::{completion_body, conversation};
use crate::backends::SessionConfig;
use crate::backends::{CompletionError, CompletionOutcome, CompletionRequest, CompletionTokens};
use crate::credentials::CustomSampling;
use crate::orchestrator::Orchestrator;
use cairn_llm::interface::complete::{Complete, CompletionCall, CompletionReply, CompletionWire};
use cairn_llm::turn::ChatFamily;
use serde_json::{json, Map, Value};
use std::time::Instant;

/// The sampling keys the operator configured. An unset key is left out rather
/// than given a default, so the server's own default applies.
pub(super) fn sampling_extras(sampling: &CustomSampling) -> Map<String, Value> {
    let mut extras = Map::new();
    if let Some(value) = sampling.repetition_penalty {
        extras.insert("repetition_penalty".into(), json!(value));
    }
    if let Some(value) = sampling.temperature {
        extras.insert("temperature".into(), json!(value));
    }
    if let Some(value) = sampling.top_p {
        extras.insert("top_p".into(), json!(value));
    }
    if let Some(value) = sampling.top_k {
        extras.insert("top_k".into(), json!(value));
    }
    extras
}

pub(super) struct CustomAdapter {
    config: CustomProviderConfig,
    endpoint: Endpoint,
    backend_key: String,
}

impl CustomAdapter {
    pub(super) fn new(config: CustomProviderConfig) -> Self {
        let backend_key = config.backend_key();
        let endpoint = Endpoint {
            provider_name: config.display_name.clone(),
            backend_key: backend_key.clone(),
            chat_url: config.chat_url(),
            headers: config.headers.clone(),
        };
        Self {
            config,
            endpoint,
            backend_key,
        }
    }

    pub(super) fn complete(
        &self,
        llm: &dyn Complete,
        request: CompletionRequest,
    ) -> Result<CompletionOutcome, CompletionError> {
        // A Decisions provider has no text completion to offer.
        self.config
            .require_turns()
            .map_err(CompletionError::InvalidRequest)?;
        let requested_model = request.model.clone();
        let body = completion_body(&request)?;
        let started = Instant::now();
        let response = llm
            .complete(CompletionCall::whole(
                CompletionWire::Chat(&self.endpoint),
                body,
                request.timeout,
            ))
            .and_then(CompletionReply::into_chat)?;
        let text = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .and_then(ChatContent::as_text)
            .ok_or_else(|| CompletionError::InvalidResponse("missing text choice".to_string()))?
            .to_string();
        let usage = response.usage.as_ref();
        Ok(CompletionOutcome {
            text,
            parsed: None,
            model: response.model.unwrap_or(requested_model),
            tokens: CompletionTokens {
                input: usage.and_then(|usage| usage.prompt_tokens.map(|value| value as u64)),
                output: usage.and_then(|usage| usage.completion_tokens.map(|value| value as u64)),
            },
            // Inference on hardware the operator owns has no per-token price to
            // report. Reporting zero would claim a measurement nobody made.
            cost: None,
            latency_ms: started.elapsed().as_millis() as u64,
            account_id: None,
        })
    }
}

impl WireAdapter for CustomAdapter {
    type Family = ChatFamily;

    fn backend_key(&self) -> &str {
        &self.backend_key
    }

    fn account_id(&self) -> Option<String> {
        cairn_llm::types::catalog::custom_account_id(&self.backend_key).map(str::to_string)
    }

    fn backend_name(&self) -> &str {
        &self.config.display_name
    }

    /// A custom provider has no default model. Every model it serves is one the
    /// operator's endpoint decides on, so guessing one would put an id on the
    /// wire that nothing there answers to.
    fn default_model(&self) -> &'static str {
        ""
    }

    /// The credential rides in the endpoint's headers, resolved when this
    /// adapter was built, so there is nothing further to look up per turn.
    fn connection(&self, _: &Orchestrator) -> Result<Connection, String> {
        Ok(Connection { api_key: None })
    }

    fn family(&self, _: &Connection) -> Result<ChatFamily, String> {
        Ok(ChatFamily {
            endpoint: self.endpoint.clone(),
            dialect: ChatDialect {
                // A single tool-loop generation must stay small enough to
                // complete promptly on operator-hosted models. Without an
                // explicit cap, OpenAI-compatible servers commonly generate up
                // to the model's remaining context, turning a degenerate
                // response into tens of minutes of apparent session silence.
                max_tokens: Some(self.config.max_output_tokens),
                include_usage: true,
                reasoning: ChatReasoning::Omit,
                response_format: false,
            },
            extras: sampling_extras(&self.config.sampling),
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
        orch.context_window_for_context_tokens(&self.backend_key, Some(model))
    }
}
