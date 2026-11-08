//! Cairn's own inference deployments as a provider: every model a
//! cairn-inference deployment serves, under its real name (tenet 1336),
//! offered whether or not it is loaded (tenet 1321).
//!
//! Nothing here starts an engine. Each request asks for its model through
//! [`crate::orchestrator::inference_route`], which asks placement for it
//! ([`crate::fleet::models`]), and is sent once the deployment is healthy,
//! straight to the deployment head's API under the name the engine serves:
//! `deepseek-ai/DeepSeek-V4-Flash-Vision-Exp` reaches the engine as
//! `deepseek-v4-flash-vision-exp`. A named model is never served
//! by another. An agent turn waits on its own thread, interruptibly, with what
//! it waits on in the transcript; a completion waits up to its timeout; a
//! decision does not wait, since it has no cancel signal, and is refused with
//! what its request waits on while the model loads.
//!
//! What a model serves is what its deployments' profiles serve, each judged by
//! one rule ([`cairn_inference::profile_verbs`]: the verbs the operator's
//! declaration names, else conversation and decide for a model that outputs
//! text, embed for one that outputs only embeddings), of the verbs this
//! provider carries: conversation and decide over openai-chat, embed over
//! openai-embeddings. Every call asks for its verb, so placement routes it only
//! to a deployment that serves it. An embedder's offer declares what the facet
//! checks every reply against: its family's native width and purpose handling
//! ([`cairn_models::embed::families`], looked up by the model's real name), the
//! native width only, and a local server's batch limits. Embedding deployments
//! of one model that disagree on width or weights are not offered for embed,
//! and the catalog says why. An embedding waits for its model as a completion
//! does, within the embedding timeout ([`embed_route`]).

mod adapter;
#[cfg(test)]
mod tests;

use std::sync::Mutex;
use std::time::Duration;

use cairn_inference::{ServedDeployment, ServedModel};
use cairn_models::embed::{families, local_server_limits};
use cairn_models::interface::decide::DecisionRoute;
use cairn_models::interface::embed::{EmbedRoute, EMBED_TIMEOUT};
use cairn_models::interface::{SelectedOffer, Unable};
use cairn_models::types::catalog::{DiscoveredModel, ServedVerb, Verb, VerbKind, Wire};
use cairn_models::types::embedding::EmbeddingCapabilities;
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

/// Conversation and decide over OpenAI chat: the provider-wide claim, what a
/// model whose profile does not say serves when it outputs text.
fn provider_serves() -> Vec<ServedVerb> {
    let wire = Wire::OpenAiChatCompletions;
    let mut serves = ServedVerb::conversation(wire);
    serves.push(ServedVerb {
        verb: Verb::Decide,
        wire,
    });
    serves
}

/// The width and purpose `deployment` of the model `model_id` embeds with,
/// from the model's family, found by its real name, else by the upstream id
/// the deployment's profile names.
fn family_embedding(model_id: &str, deployment: &ServedDeployment) -> EmbeddingCapabilities {
    let family_id = match &deployment.upstream_id {
        Some(upstream) if families::family(model_id).is_none() => upstream.as_str(),
        _ => model_id,
    };
    families::capabilities(family_id, None, false, local_server_limits(None))
}

/// What `served`'s embedding offer declares, from the deployments whose
/// profiles embed, or why it cannot be offered; `None` when none embeds.
///
/// Only the native width is offered: whether an engine honours `dimensions`
/// is not on the record (llama.cpp's server ignores it), and one that ignores
/// it would answer every shorter request at full width. The limits are any
/// local server's, with the smallest context an embedding deployment measured
/// as each text's token limit. Vectors are keyed by model and width (tenets
/// 1151, 1163), so deployments that would return different widths, or that
/// are not shown to load the same weights, cannot share one offer: a placement
/// choice between them would mix vectors of different weights under one id.
fn embedding_offer(served: &ServedModel) -> Option<Result<EmbeddingCapabilities, String>> {
    let embedders: Vec<&ServedDeployment> = served.serving(VerbKind::Embed).collect();
    let (first, others) = embedders.split_first()?;
    let mut capabilities = family_embedding(&served.id, first);
    for other in others {
        let theirs = family_embedding(&served.id, other);
        if (theirs.native_dims, &theirs.purpose)
            != (capabilities.native_dims, &capabilities.purpose)
        {
            return Some(Err(format!(
                "cannot embed: its deployments {} and {} embed differently ({} against {}), so their vectors cannot share one model id",
                first.deployment,
                other.deployment,
                capabilities.summary(),
                theirs.summary()
            )));
        }
        if !first.same_weights(other) {
            return Some(Err(format!(
                "cannot embed: its deployments {} and {} are not shown to load the same weights (the same pinned files and quantization), so their vectors cannot share one model id; pin the same weights on both, or declare embed on one",
                first.deployment, other.deployment
            )));
        }
    }
    let context = served
        .context_serving(VerbKind::Embed)
        .and_then(|tokens| u32::try_from(tokens).ok());
    capabilities.limits = local_server_limits(context);
    Some(Ok(capabilities))
}

/// What `served` serves here: of the verbs its deployments' profiles serve,
/// those this provider carries (conversation and decide over openai-chat,
/// embed over openai-embeddings), embed only while its embedding deployments
/// agree.
fn serves_for(served: &ServedModel) -> Vec<ServedVerb> {
    let verbs = served.verbs();
    let mut serves: Vec<ServedVerb> = provider_serves()
        .into_iter()
        .filter(|carried| verbs.contains(&carried.verb.kind()))
        .collect();
    if let Some(Ok(capabilities)) = embedding_offer(served) {
        serves.push(ServedVerb {
            verb: Verb::Embed(capabilities),
            wire: Wire::OpenAiEmbeddings,
        });
    }
    serves
}

/// Every verb a served model's deployments serve that its offer cannot, and
/// why: the catalog's refusal names the reason.
fn unable(served: &[ServedModel]) -> Vec<Unable> {
    served
        .iter()
        .filter_map(|model| match embedding_offer(model)? {
            Ok(_) => None,
            Err(why) => Some(Unable {
                model: model.id.clone(),
                verb: VerbKind::Embed,
                why,
            }),
        })
        .collect()
}

/// What the last catalog refresh found unable, held for the catalog to read
/// beside the models it listed.
static UNABLE: Mutex<Vec<Unable>> = Mutex::new(Vec::new());

/// The verbs served models could not be offered for at the last refresh.
pub(crate) fn unable_now() -> Vec<Unable> {
    UNABLE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
}

/// Where an embedding with `model` goes, and the name its engine is sent. It
/// asks for the model to embed, as every call here asks for its verb, so only
/// a deployment whose profile embeds is chosen and an unloaded one loads; it
/// waits up to the embedding timeout, and a wait that runs out leaves the
/// request queued, so the model still loads and a later call answers.
/// Blocking.
pub(crate) fn embed_route(
    orch: &Orchestrator,
    model: &str,
) -> Result<(EmbedRoute, String), String> {
    let route = ready_blocking(
        orch,
        model,
        VerbKind::Embed,
        "an embedding",
        Some(EMBED_TIMEOUT),
        || false,
        |said| log::info!("inference: an embedding waits for {model}: {said}"),
    )?;
    Ok(embedding_target(route))
}

/// The embeddings endpoint of a ready deployment, and the engine's own name
/// for the model, which the embedding is sent under.
fn embedding_target(route: inference_route::Route) -> (EmbedRoute, String) {
    (adapter::embed_route(&route), route.served_name)
}

/// One served model as the catalog lists it, by its real name.
pub(crate) fn discovered(served: &ServedModel) -> DiscoveredModel {
    DiscoveredModel {
        maker: None,
        id: served.id.clone(),
        model: served.id.clone(),
        display_name: served.id.clone(),
        description: None,
        hidden: false,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: vec![],
        context_window: served
            .context_length()
            .and_then(|tokens| i64::try_from(tokens).ok()),
        // A profile's upstream id is the OpenRouter slug of what it runs.
        canonical_slug: served.upstream_id().map(str::to_string),
        serving_account_ids: vec![],
        pricing: None,
        // Whether the model makes tool calls is the first turn's to find out;
        // nothing here checked it.
        supported_parameters: vec![],
        router: false,
        modalities: Some(served.modalities()),
        serves: Some(serves_for(served)),
        availability: None,
        availability_by_verb: Default::default(),
        tokenizer: tokenizer_for(INFERENCE_BACKEND_KEY, &served.id, Some(&served.id)),
    }
}

/// Every name the deployments serve, as the catalog refresh records them. No
/// deployments is an empty catalog, not an error.
pub(crate) fn discover_catalog_blocking(
    orch: &Orchestrator,
) -> (Vec<DiscoveredModel>, Option<String>) {
    match run_db_blocking(|| inference_route::served_now(orch)) {
        Ok(served) => {
            *UNABLE.lock().unwrap_or_else(|error| error.into_inner()) = unable(&served);
            (served.iter().map(discovered).collect(), None)
        }
        Err(error) => (Vec::new(), Some(error)),
    }
}

/// Attach placement's current answer after reading cached discovery. Reading
/// availability neither requests a model nor probes or starts an engine.
pub(crate) fn live_catalog(
    orch: &Orchestrator,
    catalog: &mut cairn_models::types::catalog::ProviderModelCatalog,
) {
    match run_db_blocking(|| crate::fleet::models::view(orch)) {
        Ok(view) => attach_availability(&mut catalog.models, &view),
        Err(error) => {
            catalog.models.clear();
            catalog.error = Some(format!("Could not read live placement: {error}"));
        }
    }
}

fn attach_availability(models: &mut Vec<DiscoveredModel>, view: &crate::fleet::models::ModelsView) {
    models.retain_mut(|model| {
        model.availability_by_verb = model
            .serves
            .iter()
            .flatten()
            .filter_map(|served| {
                availability(view, &model.model, served.verb.kind())
                    .map(|availability| (served.verb.kind(), availability))
            })
            .collect();
        if let Some(serves) = &mut model.serves {
            serves.retain(|served| model.availability_by_verb.contains_key(&served.verb.kind()));
        }
        model.availability = model
            .availability_by_verb
            .values()
            .max_by_key(|availability| {
                use cairn_models::types::catalog::ModelAvailability;
                match availability {
                    ModelAvailability::Loaded { .. } => (0, 0),
                    ModelAvailability::Loadable {
                        expected_load_ms, ..
                    }
                    | ModelAvailability::Loading {
                        expected_load_ms, ..
                    } => (1, expected_load_ms.unwrap_or(u64::MAX)),
                }
            })
            .cloned();
        model.availability.is_some()
    });
}

/// The route placement's own chooser selected for this verb, not the fastest
/// pool or a healthy deployment that serves some other verb.
fn availability(
    view: &crate::fleet::models::ModelsView,
    model: &str,
    verb: VerbKind,
) -> Option<cairn_models::types::catalog::ModelAvailability> {
    use crate::fleet::models::settings::SettingOrigin;
    use cairn_inference::control::Phase;
    use cairn_models::types::catalog::ModelAvailability;

    let deployment = view
        .routes
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(model))?
        .1
        .get(&verb)?;
    let placement = view.placement(deployment)?;
    if placement.refused.is_some() {
        return None;
    }
    let pool = view
        .pools
        .iter()
        .find(|pool| Some(&pool.key) == placement.pool.as_ref())?;
    let pool_name = pool.key.to_string();
    let expected_load_ms = match pool.settings.load_bound.origin {
        SettingOrigin::Measured(load) => Some(load.secs.saturating_mul(1_000)),
        _ => None,
    };
    if let Some(resident) = pool.residents.iter().find(|r| &r.deployment == deployment) {
        let since = match resident.phase.as_ref() {
            Some(Phase::Healthy { .. }) => {
                return Some(ModelAvailability::Loaded { pool: pool_name })
            }
            Some(Phase::Starting { requested_ms, .. }) => Some(*requested_ms),
            Some(Phase::Reserving { since_ms, .. }) => Some(*since_ms),
            None => Some(resident.loading_since.unwrap_or(resident.chosen_at)),
            _ => None,
        };
        if let Some(since) = since {
            return Some(ModelAvailability::Loading {
                pool: pool_name,
                expected_load_ms,
                elapsed_ms: u64::try_from(view.now.saturating_sub(since)).unwrap_or(0),
            });
        }
    }
    Some(ModelAvailability::Loadable {
        pool: pool_name,
        expected_load_ms,
    })
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

    /// The deployment placement chose to decide, sent the engine's own name.
    /// Cairn's deployments run on machines this workspace enrolled: there is
    /// no provider to retain anything.
    fn zero_data_retention(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
        _provider: &str,
        model: &str,
    ) -> Result<crate::backends::retention::ZdrMet, String> {
        Ok(crate::backends::retention::ZdrMet {
            basis: cairn_models::interface::retention::ZdrBasis::OperatorHardware,
            detail: format!(
                "{model} runs on a Cairn inference deployment, on hardware this workspace enrolled"
            ),
            exclude: Vec::new(),
        })
    }

    fn decision_route(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        offer: &SelectedOffer,
        _admitted: &crate::backends::Admitted,
    ) -> Result<(DecisionRoute, String), String> {
        let route = ready_blocking(
            orch,
            &offer.model,
            VerbKind::Decide,
            &decision_requester(project_id),
            Some(Duration::ZERO),
            || false,
            |_| {},
        )?;
        Ok((
            DecisionRoute::Logprobs(adapter::endpoint(&route)),
            route.served_name,
        ))
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
        inference_route::resolve_model(&inventory, model).map(|_| ())
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
            VerbKind::Complete,
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

    /// An outside client's request registers demand like any other: it asks
    /// placement for the model and waits while it loads, then goes
    /// to the engine under the engine's own name.
    fn relay_route(
        &self,
        orch: &Orchestrator,
        request: crate::backends::relay::RelayRequest<'_>,
        _admitted: crate::backends::Admitted,
    ) -> Result<cairn_models::interface::relay::RelayRoute, crate::backends::relay::RelayRefusal>
    {
        let crate::backends::relay::RelayRequest {
            model,
            door,
            requester,
            wait,
        } = request;
        let started = std::time::Instant::now();
        let cancel = wait.cancel.clone();
        let budget = wait.budget;
        let route = ready_blocking(
            orch,
            model,
            door.verb(),
            requester,
            Some(budget),
            || cancel.load(std::sync::atomic::Ordering::Relaxed),
            |said| (wait.waiting)(said),
        )
        .map_err(|error| crate::backends::relay::RelayRefusal::of_wait(error, &wait, started))?;
        Ok(adapter::relay_route(&route))
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
