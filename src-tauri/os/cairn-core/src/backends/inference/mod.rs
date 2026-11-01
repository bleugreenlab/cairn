//! Cairn's own inference deployments as a provider: every model a
//! cairn-inference deployment serves, under its served name and each alias,
//! offered whether or not it is loaded (tenet 1321).
//!
//! Nothing here starts an engine. Each request asks for its model through
//! [`crate::orchestrator::inference_route`], which loads it on a pool under
//! Cairn's control, and is sent once the deployment is healthy, straight to the
//! deployment head's API under the name the engine serves: `dsv4-flash` reaches
//! the engine as `deepseek-v4-flash-vision-exp`. A named model is never served
//! by another. An agent turn waits on its own thread, interruptibly, with what
//! it waits on in the transcript; a completion waits up to its timeout; a
//! decision does not wait, since it has no cancel signal, and is refused with
//! what its request waits on while the model loads.
//!
//! What a model serves is its recipe's word, as the operator's declaration
//! states it: the verbs it names (those this provider carries, conversation and
//! decide over openai-chat), or, when it names none, the provider-wide claim of
//! those verbs for a model that outputs text and nothing for one that does not.
//! Embeddings through this provider need a route into the embed door, which
//! does not exist yet; until it does, a recipe declared as an embedder is
//! listed here and serves nothing.

mod adapter;
#[cfg(test)]
mod tests;

use std::time::Duration;

use cairn_inference::ServedModel;
use cairn_models::interface::decide::DecisionRoute;
use cairn_models::interface::SelectedOffer;
use cairn_models::types::catalog::{DiscoveredModel, Modality, ServedVerb, Verb, Wire};
use cairn_models::types::tokenizers::tokenizer_for;

pub(crate) use cairn_models::types::catalog::INFERENCE_BACKEND_KEY;

use crate::backends::{
    AgentBackend, CompletionError, CompletionFailure, CompletionOutcome, CompletionRequest,
    CompletionShape, ResolvedTools, SessionConfig,
};
use crate::orchestrator::inference_route::{self, ready_blocking};
use crate::orchestrator::Orchestrator;
use crate::storage::run_db_blocking;

/// The name diagnostics and transcripts give this provider.
pub(crate) const INFERENCE_BACKEND_NAME: &str = "Cairn inference";

/// Conversation and decide over OpenAI chat: everything this provider
/// carries, and what it serves for a model whose recipe does not say.
fn provider_serves() -> Vec<ServedVerb> {
    let wire = Wire::OpenAiChatCompletions;
    let mut serves = ServedVerb::conversation(wire);
    serves.push(ServedVerb {
        verb: Verb::Decide,
        wire,
    });
    serves
}

/// What `served` serves here: the verbs its recipe declares that this
/// provider carries; else the provider's claim (`None`) for a model that
/// outputs text, and nothing for one that does not.
fn serves_for(served: &ServedModel) -> Option<Vec<ServedVerb>> {
    match &served.verbs {
        Some(verbs) => Some(
            provider_serves()
                .into_iter()
                .filter(|carried| verbs.contains(&carried.verb.kind()))
                .collect(),
        ),
        None if served.modalities.output.contains(&Modality::Text) => None,
        None => Some(Vec::new()),
    }
}

/// One served name as the catalog lists it.
fn discovered(served: &ServedModel) -> DiscoveredModel {
    DiscoveredModel {
        id: served.id.clone(),
        model: served.id.clone(),
        display_name: served.id.clone(),
        description: Some(match &served.alias_of {
            Some(target) => format!("{target} on deployment {}", served.deployment),
            None => format!("deployment {}", served.deployment),
        }),
        hidden: false,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: vec![],
        context_window: served
            .context_length
            .and_then(|tokens| i64::try_from(tokens).ok()),
        // A recipe's upstream id is the OpenRouter slug of what it runs.
        canonical_slug: served.upstream_id.clone(),
        serving_account_ids: vec![],
        pricing: None,
        // Whether the model makes tool calls is the first turn's to find out;
        // nothing here checked it.
        supported_parameters: vec![],
        router: false,
        modalities: Some(served.modalities.clone()),
        serves: serves_for(served),
        tokenizer: tokenizer_for(INFERENCE_BACKEND_KEY, &served.id, Some(&served.root)),
    }
}

/// Every name the deployments serve, as the catalog refresh records them. No
/// deployments is an empty catalog, not an error.
pub(crate) fn discover_catalog_blocking(
    orch: &Orchestrator,
) -> (Vec<DiscoveredModel>, Option<String>) {
    match run_db_blocking(|| inference_route::served_now(orch)) {
        Ok(served) => (served.iter().map(discovered).collect(), None),
        Err(error) => (Vec::new(), Some(error)),
    }
}

/// Who a completion's request is made on behalf of, as the queue names it.
fn completion_requester(project_id: Option<&str>) -> String {
    match project_id {
        Some(project) => format!("a completion in project {project}"),
        None => "a completion".to_string(),
    }
}

fn decision_requester(project_id: Option<&str>) -> String {
    match project_id {
        Some(project) => format!("a decision in project {project}"),
        None => "a decision".to_string(),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InferenceBackend;

impl AgentBackend for InferenceBackend {
    fn name(&self) -> &str {
        INFERENCE_BACKEND_KEY
    }

    fn is_available(&self) -> Result<(), String> {
        Ok(())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        Err("Cairn inference's models are its deployments, read by the catalog refresh".into())
    }

    fn serves(&self, _orch: &Orchestrator, _project_id: Option<&str>) -> Vec<ServedVerb> {
        provider_serves()
    }

    fn decision_route(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        offer: &SelectedOffer,
    ) -> Result<DecisionRoute, String> {
        let route = ready_blocking(
            orch,
            &offer.model,
            &decision_requester(project_id),
            Some(Duration::ZERO),
            || false,
            |_| {},
        )?;
        Ok(DecisionRoute::Logprobs(adapter::endpoint(&route)))
    }

    fn sent_model(&self, orch: &Orchestrator, model: &str) -> String {
        run_db_blocking(|| async { Ok(inference_route::served_name_of(orch, model).await) })
            .ok()
            .flatten()
            .unwrap_or_else(|| model.to_string())
    }

    fn response_completion_availability(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// A model is runnable here when a deployment serves its name, loaded or
    /// not: asking is what loads it.
    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        _project_id: Option<&str>,
        model: &str,
    ) -> Result<(), String> {
        let inventory = run_db_blocking(|| inference_route::served_inventory(orch))?;
        crate::orchestrator::pool_control::deployment_for(&inventory, model).map(|_| ())
    }

    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<CompletionOutcome, CompletionFailure> {
        let route = ready_blocking(
            orch,
            &request.model,
            &completion_requester(request.project_id.as_deref()),
            Some(request.timeout),
            || false,
            |said| {
                log::info!(
                    "inference: a completion waits for {}: {said}",
                    request.model
                )
            },
        )
        .map_err(CompletionError::Upstream)?;
        Ok(adapter::complete(
            orch.models_runtime().complete.as_ref(),
            &route,
            request,
        )?)
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::InProcess
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
        let adapter = adapter::InferenceAdapter::new(&config);
        crate::backends::http_loop::start_session(config, orch, adapter)
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
            "inference-http",
        ))
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }
}
