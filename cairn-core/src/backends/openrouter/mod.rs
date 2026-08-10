//! Native OpenRouter HTTP backend.
//!
//! OpenRouter exposes an OpenAI-compatible chat completions API. Cairn owns the
//! turn/tool loop for this backend so models receive Cairn's direct read/write/run
//! tools instead of Codex/Claude host tools. The generic turn/tool driver lives
//! in `backends/http_loop`; this module is its OpenRouter adapter (`adapter.rs`)
//! plus its provider-specific request construction. The shared protocol lives in
//! `backends/openai_compat`.

mod adapter;
mod http;
mod models;
mod usage;

#[cfg(test)]
mod tests;

pub use usage::collect_openrouter_usage_snapshot;

use crate::agent_process::process::BackendStdin;
use crate::backends::{
    AgentBackend, CompletionError, CompletionOutcome, CompletionRequest, CompletionRole,
    CompletionShape, CompletionTokens, DiscoveredModel, OptionChoice, OptionKind,
    ProviderOptionDescriptor, ProviderOptionKey, ResolvedTools, SessionConfig,
};
use crate::identity::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use std::sync::atomic::Ordering;

pub(crate) const OPENROUTER_BACKEND_NAME: &str = "OpenRouter";
pub(crate) const OPENROUTER_BACKEND_KEY: &str = "openrouter";

#[derive(Debug, Clone, Copy)]
pub struct OpenRouterBackend;

pub(crate) fn openrouter_api_key(orch: &Orchestrator) -> Option<String> {
    orch.get_identity_store().and_then(|store| {
        store
            .accounts_for_provider(ApiProvider::OpenRouter, None)
            .into_iter()
            .find(|account| {
                account
                    .compatible_backends()
                    .contains(&OPENROUTER_BACKEND_KEY)
            })
            .and_then(|account| match &account.auth {
                ProviderAuth::ApiKey { value } => Some(value.clone()),
                _ => None,
            })
    })
}

impl AgentBackend for OpenRouterBackend {
    fn name(&self) -> &str {
        OPENROUTER_BACKEND_NAME
    }

    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
    ) -> Result<CompletionOutcome, CompletionError> {
        use crate::backends::openai_compat::http::{post_chat_completion, Endpoint};
        use serde_json::json;
        use std::time::Instant;

        if request.messages.is_empty() {
            return Err(CompletionError::InvalidRequest(
                "at least one message is required".to_string(),
            ));
        }
        let api_key = openrouter_api_key(orch).ok_or(CompletionError::BackendUnavailable)?;
        let requested_model = request.model.clone();
        let mut messages = Vec::with_capacity(request.messages.len() + 1);
        if let Some(system) = request.system {
            messages.push(json!({"role": "system", "content": system}));
        }
        messages.extend(request.messages.into_iter().map(|message| {
            let role = match message.role {
                CompletionRole::User => "user",
                CompletionRole::Assistant => "assistant",
            };
            json!({"role": role, "content": message.content})
        }));
        let mut body = json!({
            "model": requested_model,
            "messages": messages,
            "stream": false,
            "provider": {"require_parameters": true},
        });
        if let Some(extras) = request.extras.as_object() {
            body.as_object_mut()
                .expect("completion body is an object")
                .extend(extras.clone());
        } else if !request.extras.is_null() {
            return Err(CompletionError::InvalidRequest(
                "extras must be an object or null".to_string(),
            ));
        }
        http::apply_output_schema(&mut body, request.output_schema.as_ref());
        let endpoint = Endpoint {
            provider_name: OPENROUTER_BACKEND_NAME,
            backend_key: OPENROUTER_BACKEND_KEY,
            chat_url: "https://openrouter.ai/api/v1/chat/completions".to_string(),
            headers: http::openrouter_headers(&api_key).map_err(CompletionError::InvalidRequest)?,
            extra_body: None,
        };
        let started = Instant::now();
        let response = post_chat_completion(&endpoint, body, request.timeout)?;
        let text = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .and_then(|content| content.as_text())
            .ok_or_else(|| CompletionError::InvalidResponse("missing text choice".to_string()))?
            .to_string();
        let usage = response.usage.as_ref();
        Ok(CompletionOutcome {
            text,
            parsed: None,
            model: response.model.unwrap_or(request.model),
            tokens: CompletionTokens {
                input: usage.and_then(|usage| usage.prompt_tokens.map(|value| value as u64)),
                output: usage.and_then(|usage| usage.completion_tokens.map(|value| value as u64)),
            },
            cost: usage.and_then(|usage| usage.cost),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::InProcess
    }

    fn is_available(&self) -> Result<(), String> {
        // Availability depends on a configured account in the owning orchestrator;
        // `start_session` performs the authoritative check and records transcript
        // errors. Keeping this optimistic lets Settings show the provider card and
        // unauthenticated model catalog fallback.
        Ok(())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        models::discover_models_blocking(None)
    }

    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        vec![ProviderOptionDescriptor {
            key: ProviderOptionKey::ReasoningEffort,
            label: "Effort".to_string(),
            kind: OptionKind::Enum,
            choices: ["low", "medium", "high"]
                .into_iter()
                .map(|value| OptionChoice {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
            default: None,
        }]
    }

    fn resolve_tools(&self, agent_tools: &[String], _agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;
        let mut allowed = toolkits::resolve_tools(agent_tools);
        toolkits::ensure_core_verbs(&mut allowed);
        allowed.retain(|tool| tool != "apply_patch");
        ResolvedTools {
            allowed,
            disallowed: Vec::new(),
        }
    }

    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
        crate::backends::http_loop::start_session(
            config,
            orch,
            adapter::OpenRouterAdapter::new(orch),
        )
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        false
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // OpenRouter runs the whole agentic loop in-process over async HTTP; a
        // call spawns no child process. Unbounded today.
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }

    fn send_user_message(
        &self,
        _stdin: &mut dyn BackendStdin,
        _content: &crate::agent_process::stdin::MessageContent,
        _session_id: &str,
        _parent_tool_use_id: Option<&str>,
        _working_dir: Option<&str>,
    ) -> Result<(), String> {
        Err("OpenRouter HTTP turns do not keep a warm stdin; start a new run/turn".to_string())
    }

    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String> {
        // Flip the cancel flag the streaming turn polls at SSE line boundaries.
        // Dropping the response there closes the connection and stops billing.
        if let Some(s) = stdin
            .as_any_mut()
            .downcast_mut::<crate::backends::http_loop::HttpTurnStdin>()
        {
            s.cancel.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err("OpenRouter stdin unavailable".to_string())
        }
    }

    fn send_set_model(&self, _stdin: &mut dyn BackendStdin, _model: &str) -> Result<(), String> {
        Err("OpenRouter model changes apply to the next HTTP turn".to_string())
    }

    fn send_set_permission_mode(
        &self,
        _stdin: &mut dyn BackendStdin,
        _mode: &str,
    ) -> Result<(), String> {
        Err("OpenRouter permission changes apply to the next HTTP turn".to_string())
    }
}
