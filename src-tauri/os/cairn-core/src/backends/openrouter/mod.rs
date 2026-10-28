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

#[cfg(test)]
pub(crate) use models::decode_decision_models;
pub(crate) use models::discover_catalog_blocking;
pub use usage::collect_openrouter_usage_snapshot;

/// The root every OpenRouter API path hangs off.
pub(crate) const OPENROUTER_API_BASE: &str = "https://openrouter.ai/api/v1";

/// The root of OpenRouter's Decisions API, which is still an alpha path:
/// System One's `POST {base}/decisions`.
pub(crate) const OPENROUTER_DECISIONS_BASE: &str = "https://openrouter.ai/api/alpha";

/// The embeddings route on this workspace's OpenRouter key, with the headers
/// every OpenRouter request carries.
pub(crate) fn embed_route(
    orch: &Orchestrator,
) -> Result<cairn_models::interface::embed::EmbedRoute, String> {
    let api_key =
        openrouter_api_key(orch).ok_or_else(|| "needs an OpenRouter API key".to_string())?;
    Ok(cairn_models::embed::openai::route_for(
        OPENROUTER_BACKEND_KEY,
        OPENROUTER_BACKEND_NAME,
        OPENROUTER_API_BASE,
        None,
        &http::openrouter_headers(&api_key),
    ))
}

use crate::backends::{
    AgentBackend, CompletionError, CompletionFailure, CompletionOutcome, CompletionRequest,
    CompletionShape, ResolvedTools, SessionConfig,
};
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use cairn_models::types::catalog::{
    DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor, ProviderOptionKey,
};

pub(crate) const OPENROUTER_BACKEND_NAME: &str = "OpenRouter";
pub(crate) const OPENROUTER_BACKEND_KEY: &str = "openrouter";

#[derive(Debug, Clone, Copy)]
pub struct OpenRouterBackend;

pub(crate) fn openrouter_api_key(orch: &Orchestrator) -> Option<String> {
    orch.get_credential_store().and_then(|store| {
        store
            .accounts_for_provider(ApiProvider::OpenRouter, None)
            .into_iter()
            .find(|account| account.serves_backend(OPENROUTER_BACKEND_KEY))
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

    fn response_completion_availability(
        &self,
        orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Result<(), String> {
        openrouter_api_key(orch)
            .map(|_| ())
            .ok_or_else(|| "needs an OpenRouter API key".to_string())
    }

    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<CompletionOutcome, CompletionFailure> {
        use crate::backends::http_loop::{Connection, WireAdapter};
        use crate::backends::openai_compat::completion_body;
        use cairn_models::interface::complete::{CompletionCall, CompletionReply, CompletionWire};
        use std::time::Instant;

        let api_key = openrouter_api_key(orch).ok_or(CompletionError::BackendUnavailable)?;
        // The session's family, routing object included: it always carries
        // `require_parameters`, which is also what keeps a schema-constrained
        // completion off providers that would drop the schema.
        let family = adapter::OpenRouterAdapter::new(orch)
            .family(&Connection {
                api_key: Some(api_key),
            })
            .map_err(CompletionError::InvalidRequest)?;
        let body = completion_body(&request, &family)?;
        let started = Instant::now();
        let response = orch
            .models_runtime()
            .complete
            .complete(CompletionCall::whole(
                CompletionWire::Chat(&family.endpoint),
                body,
                request.timeout,
            ))
            .and_then(CompletionReply::into_chat)?;
        let text = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .and_then(|content| content.as_text())
            .ok_or_else(|| CompletionError::InvalidResponse("missing text choice".to_string()))?
            .to_string();
        Ok(CompletionOutcome {
            text,
            parsed: None,
            usage: response
                .usage
                .as_ref()
                .map(cairn_models::usage::metered)
                .unwrap_or_default(),
            model: response.model.unwrap_or(request.model),
            latency_ms: started.elapsed().as_millis() as u64,
            account_id: None,
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

    fn serves(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Vec<cairn_models::types::catalog::ServedVerb> {
        use cairn_models::types::catalog::{ServedVerb, Wire};
        ServedVerb::conversation(Wire::OpenAiChatCompletions)
    }

    /// A decision model from the `output_modalities=decisions` listing, which
    /// discovery declares as serving `decide` over System One, answers at
    /// the Decisions API on this workspace's key.
    fn decision_route(
        &self,
        orch: &Orchestrator,
        _project_id: Option<&str>,
        offer: &cairn_models::interface::SelectedOffer,
    ) -> Result<cairn_models::interface::decide::DecisionRoute, String> {
        use cairn_models::interface::decide::DecisionRoute;
        use cairn_models::types::catalog::Wire;
        use cairn_models::wire::decisions::http::DecisionsEndpoint;
        match offer.served.wire {
            Wire::SystemOne => {
                let api_key = openrouter_api_key(orch).ok_or_else(|| {
                    "OpenRouter needs an API key to answer a decision".to_string()
                })?;
                Ok(DecisionRoute::SystemOne(DecisionsEndpoint {
                    provider_name: OPENROUTER_BACKEND_NAME.to_string(),
                    backend_key: OPENROUTER_BACKEND_KEY.to_string(),
                    url: format!("{OPENROUTER_DECISIONS_BASE}/decisions"),
                    headers: http::openrouter_headers(&api_key),
                }))
            }
            wire => Err(format!("OpenRouter answers no decision over {wire}")),
        }
    }

    /// The chat listing with the image and embeddings listings merged in. The
    /// catalog refresh reads [`discover_catalog_blocking`] directly, so a
    /// failed media listing is reported beside chat models that did load.
    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        match models::discover_catalog_blocking(&[]) {
            (models, Some(error)) if models.is_empty() => Err(error),
            (models, _) => Ok(models),
        }
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
        ResolvedTools {
            allowed,
            disallowed: Vec::new(),
        }
    }

    fn start_session(
        &self,
        config: SessionConfig,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<(), String> {
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

    fn runtime_launch_capability(
        &self,
        _launch: &crate::backends::RuntimeLaunch,
    ) -> Result<crate::backends::RuntimeLaunchCapability, String> {
        Ok(crate::backends::stateless_http_runtime_capability(
            "openrouter-http",
        ))
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // OpenRouter runs the whole agentic loop in-process over async HTTP; a
        // call spawns no child process. Unbounded today.
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }
}
