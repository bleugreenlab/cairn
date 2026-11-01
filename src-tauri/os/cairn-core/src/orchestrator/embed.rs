//! The orchestrator's side of the cairn-models facet's `embed` verb.
//!
//! The facet embeds; this module decides where. It offers Cairn's own cloud
//! gateway in the catalog whenever this device holds a Cairn token, resolves
//! an explicitly named model to an offer through the facet's selector
//! ([`cairn_models::interface::Catalog::embedding_offer`]), builds that
//! provider's route from this workspace's keys and hosts, and runs the facet's
//! blocking `embed` through the runtime's one [`Complete`].
//!
//! The door always takes a model. Stored vectors are keyed by `(model, width)`,
//! so letting the catalog pick would let a catalog change switch vector spaces
//! underneath them, and would let an unset model land on the cloud gateway
//! without the consent its caller gates.
//!
//! Cairn's own consumers reach this door through [`Orchestrator::embedder`]:
//! the one selected model ([`Orchestrator::embedding_selection`], the
//! `embeddingModel` setting) behind the shared health gate
//! (`crate::embeddings::embedder`). The selection is read on every call, so a
//! settings change takes effect without respawning anything; what it means
//! for the vectors already stored is `crate::embeddings::space`'s concern.
//!
//! [`Complete`]: cairn_models::interface::complete::Complete

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use cairn_models::embed::{embed, gateway, openai};
use cairn_models::interface::complete::CompletionError;
use cairn_models::interface::embed::{
    EmbedCall, EmbedError, EmbedRequest, EmbedRoute, Embeddings, Purpose, Width, EMBED_TIMEOUT,
};
use cairn_models::interface::{
    Catalog, CatalogProvider, ModelScope, Models, NoOffer, SelectedOffer,
};
use cairn_models::types::catalog::VerbKind;
use cairn_models::types::catalog::{
    self, DiscoveredModel, Modalities, Modality, ProviderModelCatalog, ServedVerb, Verb, Wire,
};
use cairn_models::types::embedding::{
    EmbeddingCapabilities, CAIRN_GATEWAY_EMBED_MODEL, CAIRN_GATEWAY_KEY,
};
use cairn_models::types::usage::{BillingUnit, Charge};

use super::{resolve_embed_token, Orchestrator};
use crate::backends::custom::CustomProviderConfig;
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::embeddings::embedder::{EmbedSource, FacetEmbedder, SelectedEmbedOffer};
use crate::embeddings::Embed;

/// The embedding model an unset `embeddingModel` means: the Cairn gateway's
/// Cohere Embed v4.
pub fn default_embedding_selection() -> String {
    format!("{CAIRN_GATEWAY_KEY}/{CAIRN_GATEWAY_EMBED_MODEL}")
}

/// The embedding model this workspace has selected, and whether the operator
/// chose it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedSelection {
    /// As the picker saves it, `provider/model`.
    pub value: String,
    /// Set explicitly, rather than the default an unset setting means.
    /// Choosing a model is consent to embed history with it.
    pub explicit: bool,
}

/// Whether texts are what is searched or what searches, as the embed door
/// takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum EmbedPurpose {
    #[default]
    Document,
    Query,
}

/// What `run cairn://models/embed` takes. Nothing here is chosen for the
/// caller: no workspace default, no best offer.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EmbedDoorRequest {
    /// A catalog id or `provider/model`. Optional only so its absence is
    /// refused by name.
    #[serde(default)]
    pub model: Option<String>,
    pub input: Vec<String>,
    #[serde(default)]
    pub purpose: EmbedPurpose,
    /// Exactly this width; absent is the model's native one.
    #[serde(default)]
    pub width: Option<u32>,
}

/// Input tokens, when the provider reported them.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EmbedUsage {
    pub input_tokens: Option<u64>,
}

/// What the embed door returns: one vector per text in input order, and the
/// exact `(model, dims)` the facet reports for them.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EmbedOutcome {
    pub vectors: Vec<Vec<f32>>,
    pub model: String,
    pub dims: u32,
    pub provider: String,
    pub usage: EmbedUsage,
    /// USD, only when every provider request reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    pub latency_ms: u64,
}

/// How many offers a refusal names as the ones that do serve `embed`.
const ALTERNATIVES_NAMED: usize = 3;

/// The offers serving `embed` in scope, named `provider/model`: at most
/// [`ALTERNATIVES_NAMED`], then how many more. Empty when there are none.
fn embed_offers_named(catalog: &Catalog) -> String {
    let all: Vec<String> = catalog
        .offers_serving(VerbKind::Embed)
        .into_iter()
        .map(|offer| format!("{}/{}", offer.provider, offer.model))
        .collect();
    let mut named = all
        .iter()
        .take(ALTERNATIVES_NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if all.len() > ALTERNATIVES_NAMED {
        named.push_str(&format!(" and {} more", all.len() - ALTERNATIVES_NAMED));
    }
    named
}

/// Sets its flag when dropped: the embedding future it lives in was
/// abandoned (or finished, when setting it no longer matters).
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// The selected model, as this workspace reaches it.
struct WorkspaceEmbedSource {
    orch: Orchestrator,
}

impl EmbedSource for WorkspaceEmbedSource {
    fn selection(&self) -> String {
        self.orch.embedding_selection().value
    }

    /// The gateway is called with this device's Cairn token; an install
    /// without one has embedding off, which is not an outage. Any other
    /// provider is configured when the selection resolves to an offer: the
    /// catalog only lists a provider this workspace can reach.
    fn configured(&self) -> bool {
        let selection = self.selection();
        if selection.split('/').next() == Some(CAIRN_GATEWAY_KEY) {
            return self.orch.embed_token().is_some();
        }
        self.offer().is_some()
    }

    fn offer(&self) -> Option<SelectedEmbedOffer> {
        let catalog = self.orch.catalog(&ModelScope::default()).ok()?;
        let probe = EmbedRequest {
            texts: Vec::new(),
            purpose: Purpose::Document,
            width: Width::Native,
        };
        let (offer, capabilities) = catalog
            .embedding_offer(Some(&self.selection()), &probe)
            .ok()?;
        Some(SelectedEmbedOffer {
            provider: offer.provider,
            model: offer.model,
            capabilities,
        })
    }

    fn embed(&self, request: &EmbedRequest) -> Result<Embeddings, EmbedError> {
        self.orch
            .embed_blocking(&ModelScope::default(), &self.selection(), request)
    }
}

/// The resource-recall corpus as this workspace holds it: the private
/// database and every open team replica, and the workspace's skills.
#[async_trait::async_trait]
impl crate::embeddings::resources::ResourceSource for Orchestrator {
    async fn corpus_dbs(&self) -> Vec<Arc<crate::storage::LocalDb>> {
        self.db.all_dbs().await
    }

    fn skills(&self) -> Vec<crate::embeddings::resources::ResourceTarget> {
        crate::config::skills::list_skills(&self.config_dir, None)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|skill| match skill {
                crate::config::ConfigResult::Ok(skill) if !skill.is_project_scoped => Some(
                    crate::embeddings::resources::skill_embed_target(&skill.id, &skill.description),
                ),
                _ => None,
            })
            .collect()
    }
}

/// The Cairn gateway as a catalog provider: one model, Cohere Embed v4, at the
/// gateway's fixed width. Probed by nothing; present when a token resolves.
fn cairn_gateway_provider() -> CatalogProvider {
    let model = DiscoveredModel {
        id: CAIRN_GATEWAY_EMBED_MODEL.to_string(),
        model: CAIRN_GATEWAY_EMBED_MODEL.to_string(),
        display_name: "Cohere Embed v4".to_string(),
        description: Some(
            "Cohere Embed v4 on Amazon Bedrock, through Cairn's cloud gateway.".to_string(),
        ),
        hidden: false,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: Vec::new(),
        context_window: None,
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: None,
        supported_parameters: Vec::new(),
        router: false,
        modalities: Some(Modalities {
            input: vec![Modality::Text],
            output: vec![Modality::Embeddings],
        }),
        // Cohere's tokenizer has no pin; the gateway counts tokens itself.
        tokenizer: None,
        serves: Some(vec![ServedVerb {
            verb: Verb::Embed(EmbeddingCapabilities::cairn_gateway()),
            wire: Wire::CairnEmbed,
        }]),
    };
    CatalogProvider {
        descriptor: catalog::cairn_gateway_descriptor(),
        installed: true,
        models: Some(ProviderModelCatalog {
            backend: CAIRN_GATEWAY_KEY.to_string(),
            models: vec![model],
            options: Vec::new(),
            refreshed_at: None,
            error: None,
        }),
        tiers: Vec::new(),
        serves: Vec::new(),
        unable: Vec::new(),
    }
}

impl Orchestrator {
    /// The `embeddingModel` setting, or the gateway default it means unset.
    pub fn embedding_selection(&self) -> EmbedSelection {
        match self.get_settings().embedding_model {
            Some(value) if !value.trim().is_empty() => EmbedSelection {
                value: value.trim().to_string(),
                explicit: true,
            },
            _ => EmbedSelection {
                value: default_embedding_selection(),
                explicit: false,
            },
        }
    }

    /// Whether history that predates this install may be embedded: the
    /// operator chose a model, or connected an account. Every install
    /// registers an anonymous device token, so a usable token alone says
    /// nothing about consent. The turn sweep's full scope, its deferred
    /// retries, the resource sweep and the status counts all ask this.
    pub fn history_consented(&self) -> bool {
        self.embedding_selection().explicit
            || self.account_manager.get_jwt().ok().flatten().is_some()
    }

    /// Embed `issue`'s description for recall, as the agent `write` path does
    /// when it creates or patches one. `db` is the database that owns it.
    pub async fn enqueue_issue_embed(
        &self,
        db: &crate::storage::LocalDb,
        issue: &crate::models::Issue,
    ) {
        let key = db
            .query_opt(
                "SELECT key FROM projects WHERE id = ?1",
                (issue.project_id.clone(),),
                |row| crate::storage::RowExt::text(row, 0),
            )
            .await;
        match key {
            Ok(Some(key)) => self.enqueue_resource_embed(
                &cairn_common::uri::build_issue_uri(&key, issue.number),
                crate::embeddings::resources::issue_embed_text(Some(&issue.description)),
            ),
            Ok(None) => {}
            Err(error) => log::warn!(
                "embeddings: resolving the project of issue {} failed: {error}",
                issue.id
            ),
        }
    }

    /// Where embedding is: the selection, the gate, and how far each corpus
    /// is embedded in the active space. The one source the Settings page,
    /// `cairn://models/embeddings` and agents read.
    pub async fn embedding_status(&self) -> crate::embeddings::status::EmbeddingStatus {
        let selection = self.embedding_selection();
        let keeper = self.space_keeper();
        crate::embeddings::status::embedding_status(
            &keeper,
            &selection.value,
            selection.explicit,
            self.history_consented(),
            |health| self.embedding_unavailable_reason(&selection.value, health.configured),
        )
        .await
    }

    /// Why `selection` cannot be embedded with: the gateway's missing token,
    /// or why the catalog has no offer for it.
    fn embedding_unavailable_reason(&self, selection: &str, configured: bool) -> String {
        if selection.split('/').next() == Some(CAIRN_GATEWAY_KEY) && self.embed_token().is_none() {
            return "no Cairn token on this device; sign in to Cairn, or let it reach Cairn's API to register".to_string();
        }
        let probe = EmbedRequest {
            texts: Vec::new(),
            purpose: Purpose::Document,
            width: Width::Native,
        };
        match self.catalog(&ModelScope::default()) {
            Err(error) => format!("the model catalog is unavailable: {error}"),
            Ok(catalog) => match catalog.embedding_offer(Some(selection), &probe) {
                Err(error) => error.to_string(),
                Ok(_) if !configured => format!("{selection} is not configured on this device"),
                Ok(_) => format!("{selection} does not resolve to an embedding offer"),
            },
        }
    }

    /// The door Cairn's own consumers embed through: the selected model,
    /// behind the gate they all share.
    pub fn embedder(&self) -> Arc<dyn Embed> {
        Arc::new(FacetEmbedder::new(
            Arc::new(WorkspaceEmbedSource { orch: self.clone() }),
            self.embed_gate.clone(),
        ))
    }

    /// The gateway's catalog entry, when this device holds a Cairn token.
    /// Every install registers an anonymous one, so this is normally present.
    pub(crate) fn embed_gateway_provider(&self) -> Option<CatalogProvider> {
        self.embed_token().map(|_| cairn_gateway_provider())
    }

    fn embed_token(&self) -> Option<String> {
        resolve_embed_token(&self.account_manager, &self.anon_device_manager)
    }

    /// Embed `request` on `model` (a model id or catalog id, never empty), one
    /// vector per text in input order, at the width asked for or the model's
    /// native one. Blocking: it posts to the provider.
    pub fn embed_blocking(
        &self,
        scope: &ModelScope,
        model: &str,
        request: &EmbedRequest,
    ) -> Result<Embeddings, EmbedError> {
        self.embed_cancellable(scope, model, request, None)
    }

    /// [`Self::embed_blocking`], ending before its next sub-batch once
    /// `cancel` is set.
    fn embed_cancellable(
        &self,
        scope: &ModelScope,
        model: &str,
        request: &EmbedRequest,
        cancel: Option<&AtomicBool>,
    ) -> Result<Embeddings, EmbedError> {
        if model.trim().is_empty() {
            return Err(EmbedError::InvalidRequest(
                "name the embedding model: stored vectors are keyed by it, so it is never chosen implicitly"
                    .to_string(),
            ));
        }
        let catalog = self.catalog(scope).map_err(|error| {
            EmbedError::Unsupported(format!("the model catalog is unavailable: {error}"))
        })?;
        let (offer, capabilities) = catalog.embedding_offer(Some(model), request)?;
        let route = self.embed_route(scope, &offer)?;
        // An inference engine answers to its own name for the model; the
        // vectors still carry the name the caller asked by.
        let sent =
            crate::backends::backend_for_name(Some(&offer.provider)).sent_model(self, &offer.model);
        let mut embeddings = embed(
            self.models_runtime.complete.as_ref(),
            EmbedCall {
                route: &route,
                model: &sent,
                capabilities: &capabilities,
                request,
                timeout: EMBED_TIMEOUT,
                cancel,
            },
        )?;
        embeddings.model = offer.model.clone();
        Ok(embeddings)
    }

    /// [`Self::embed_blocking`], off the async runtime. Dropping the future
    /// releases the caller at once; the blocking worker then sends no further
    /// sub-batch, so it ends within the one request in flight
    /// ([`EMBED_TIMEOUT`]).
    pub async fn embed(
        &self,
        scope: ModelScope,
        model: String,
        request: EmbedRequest,
    ) -> Result<Embeddings, EmbedError> {
        let orch = self.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let _cancel_on_drop = CancelOnDrop(cancel.clone());
        tokio::task::spawn_blocking(move || {
            orch.embed_cancellable(&scope, &model, &request, Some(&cancel))
        })
        .await
        .map_err(|error| {
            EmbedError::Completion(CompletionError::Upstream(format!(
                "the embedding task failed: {error}"
            )))
        })?
    }

    /// Embed `request`'s texts with the model it names, and only that model:
    /// the door behind `run cairn://models/embed`. A model that serves no
    /// embeddings is refused naming the offers that do; a width the offer
    /// cannot return is refused by the facet's own selector, naming the widths
    /// it can. Usage is returned, not recorded, as it is for every embedding
    /// consumer.
    pub(crate) async fn embed_named(
        &self,
        project_id: Option<&str>,
        request: EmbedDoorRequest,
    ) -> Result<EmbedOutcome, String> {
        let started = Instant::now();
        let scope = ModelScope {
            project_id: project_id.map(str::to_string),
        };
        let catalog = self
            .catalog(&scope)
            .map_err(|error| format!("the model catalog is unavailable: {error}"))?;
        let Some(model) = request
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
        else {
            let lead = "Name the model to embed with (a catalog id, or provider/model): this door never chooses one, because vectors are only comparable within one model";
            let offers = embed_offers_named(&catalog);
            return Err(if offers.is_empty() {
                let nothing = NoOffer {
                    verb: VerbKind::Embed,
                    requested: None,
                    detail: None,
                };
                format!("{lead}. {nothing}")
            } else {
                format!("{lead}; offers that serve embed: {offers}.")
            });
        };
        let width = match request.width {
            None => Width::Native,
            Some(0) => {
                return Err(
                    "`width` must be at least 1; omit it for the model's native width.".to_string(),
                )
            }
            Some(width) => Width::Exactly(width),
        };
        // The serves gate first, at the native width every embedding offer
        // admits, so a model that does not embed at all is told apart from one
        // that cannot return the width asked for.
        let any_width = EmbedRequest {
            texts: Vec::new(),
            purpose: Purpose::Document,
            width: Width::Native,
        };
        if let Err(error) = catalog.embedding_offer(Some(model), &any_width) {
            return Err(match error {
                EmbedError::NoOffer(mut refusal) => {
                    let offers = embed_offers_named(&catalog);
                    if !offers.is_empty() {
                        let suffix = format!("offers that serve embed: {offers}");
                        refusal.detail = Some(match refusal.detail.take() {
                            Some(detail) => format!("{detail}; {suffix}"),
                            None => suffix,
                        });
                    }
                    refusal.to_string()
                }
                other => other.to_string(),
            });
        }
        let embeddings = self
            .embed(
                scope,
                model.to_string(),
                EmbedRequest {
                    texts: request.input,
                    purpose: match request.purpose {
                        EmbedPurpose::Document => Purpose::Document,
                        EmbedPurpose::Query => Purpose::Query,
                    },
                    width,
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(EmbedOutcome {
            usage: EmbedUsage {
                input_tokens: embeddings
                    .usage
                    .quantity(Charge::Input, BillingUnit::Tokens),
            },
            cost: embeddings.usage.cost_usd,
            vectors: embeddings.vectors,
            model: embeddings.model,
            dims: embeddings.dims,
            provider: embeddings.provider,
            latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }

    /// Where `offer` is embedded, with this workspace's credentials for it.
    pub(crate) fn embed_route(
        &self,
        scope: &ModelScope,
        offer: &SelectedOffer,
    ) -> Result<EmbedRoute, EmbedError> {
        let provider = offer.provider.as_str();
        let unavailable = |why: String| EmbedError::Unsupported(format!("{provider} {why}"));
        if provider == CAIRN_GATEWAY_KEY {
            let token = self.embed_token().ok_or_else(|| {
                unavailable(
                    "has no Cairn token on this device; sign in to Cairn, or let it reach Cairn's API to register"
                        .to_string(),
                )
            })?;
            return Ok(gateway::route_for(&self.api_config.base_url, &token));
        }
        if provider == crate::backends::openrouter::OPENROUTER_BACKEND_KEY {
            return crate::backends::openrouter::embed_route(self).map_err(unavailable);
        }
        if provider == "ollama" {
            return self
                .ollama_embed_route(scope, &offer.model)
                .map_err(unavailable);
        }
        if provider == crate::backends::inference::INFERENCE_BACKEND_KEY {
            // Asking is demand: an unloaded embedder loads, within the
            // embedding's own timeout.
            let route = super::inference_route::ready_blocking(
                self,
                &offer.model,
                "an embedding",
                Some(EMBED_TIMEOUT),
                || false,
                |_| {},
            )
            .map_err(EmbedError::Unsupported)?;
            return Ok(openai::route_for(
                crate::backends::inference::INFERENCE_BACKEND_KEY,
                crate::backends::inference::INFERENCE_BACKEND_NAME,
                &route.api_base,
                None,
                &[],
            ));
        }
        if let Some(account_id) = catalog::custom_account_id(provider) {
            let config =
                CustomProviderConfig::resolve(self, account_id, scope.project_id.as_deref())
                    .map_err(unavailable)?;
            return Ok(openai::route_for(
                &config.backend_key(),
                &config.display_name,
                &config.base_url,
                None,
                &config.headers,
            ));
        }
        Err(unavailable(
            "has no embedding route in this build".to_string(),
        ))
    }

    /// The OpenAI-compatible route on the Ollama host that serves `model`:
    /// the first of its serving hosts, else the first configured host.
    fn ollama_embed_route(&self, scope: &ModelScope, model: &str) -> Result<EmbedRoute, String> {
        let serving: Vec<String> = self
            .model_catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.get("ollama").cloned())
            .and_then(|catalog| {
                catalog
                    .models
                    .into_iter()
                    .find(|known| known.model == model)
            })
            .map(|known| known.serving_account_ids)
            .unwrap_or_default();
        let store = self
            .get_credential_store()
            .ok_or_else(|| "has no identity store".to_string())?;
        let hosts: Vec<(String, String)> = store
            .accounts_for_provider(ApiProvider::Ollama, scope.project_id.as_deref())
            .into_iter()
            .filter_map(|account| match &account.auth {
                ProviderAuth::BaseUrl { url } => Some((account.id.clone(), url.clone())),
                _ => None,
            })
            .collect();
        let url = serving
            .iter()
            .find_map(|id| hosts.iter().find(|(host, _)| host == id))
            .or_else(|| hosts.first())
            .map(|(_, url)| url.clone())
            .ok_or_else(|| "has no host configured".to_string())?;
        Ok(openai::route_for(
            "ollama",
            "Ollama",
            &format!("{}/v1", url.trim_end_matches('/')),
            None,
            &[],
        ))
    }
}

/// The scripted hub these tests embed on, shared with the run handler's tests
/// of the embed door and the read page's.
#[cfg(test)]
pub(crate) mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use cairn_models::interface::complete::{
        Complete, CompletionCall, CompletionError, CompletionReply,
    };
    use cairn_models::interface::embed::{EmbedError, EmbedRequest, EmbedRoute, Purpose, Width};
    use cairn_models::interface::{ModelScope, Models, SelectedOffer};
    use cairn_models::types::catalog::{ServedVerb, Verb, VerbKind, Wire};
    use cairn_models::types::embedding::{EmbeddingCapabilities, CAIRN_GATEWAY_KEY};
    use serde_json::{json, Value};

    use crate::credentials::{
        ApiProvider, CustomEmbeddingModel, CustomModelPolicy, CustomProtocol, CustomSampling,
        ProviderAuth,
    };
    use crate::orchestrator::models::ModelRuntime;
    use crate::orchestrator::Orchestrator;

    /// An embeddings server that answers from a script and records requests.
    #[derive(Default)]
    pub(crate) struct Scripted {
        replies: Mutex<VecDeque<Value>>,
        requests: Mutex<Vec<(&'static str, Value)>>,
    }

    impl Scripted {
        /// How many texts each request carried, in order.
        pub(crate) fn batch_sizes(&self) -> Vec<usize> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, body)| body["input"].as_array().map_or(0, Vec::len))
                .collect()
        }
    }

    /// An OpenAI-wire reply of `count` four-wide vectors, each starting with
    /// its in-batch index so order can be read back.
    pub(crate) fn hub_reply(count: usize) -> Value {
        let data: Vec<Value> = (0..count)
            .map(|index| json!({"index": index, "embedding": [index as f32, 0.5, 0.5, 0.5]}))
            .collect();
        json!({"model": "qwen3-embedding-4b", "data": data, "usage": {"prompt_tokens": count}})
    }

    impl Complete for Scripted {
        fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
            self.requests
                .lock()
                .unwrap()
                .push((call.wire.family(), call.body));
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("a request the script did not expect");
            Ok(CompletionReply::Embeddings(
                serde_json::from_value(reply).unwrap(),
            ))
        }
    }

    fn offer(provider: &str, model: &str) -> SelectedOffer {
        SelectedOffer {
            provider: provider.into(),
            model: model.into(),
            served: ServedVerb {
                verb: Verb::Embed(EmbeddingCapabilities::cairn_gateway()),
                wire: Wire::OpenAiEmbeddings,
            },
        }
    }

    fn header_names(route: &EmbedRoute) -> Vec<String> {
        let EmbedRoute::OpenAi(endpoint) = route else {
            panic!("an OpenAI route")
        };
        endpoint
            .headers
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn url(route: &EmbedRoute) -> &str {
        match route {
            EmbedRoute::OpenAi(endpoint) => &endpoint.url,
            EmbedRoute::Gateway(endpoint) => &endpoint.url,
        }
    }

    /// A workspace whose one provider is a chat hub declaring one embedding
    /// model, answering from `replies`.
    pub(crate) async fn workspace_with_hub(
        replies: Vec<Value>,
    ) -> (Orchestrator, Arc<Scripted>, String) {
        let mut orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let scripted = Arc::new(Scripted {
            replies: Mutex::new(replies.into()),
            ..Scripted::default()
        });
        orch.models_runtime = ModelRuntime {
            complete: scripted.clone(),
            ..orch.models_runtime.clone()
        };
        let auth = ProviderAuth::custom_endpoint(
            "http://hub.invalid/v1",
            CustomProtocol::OpenAiChatCompletions,
            Some("sk-hub"),
            &[],
            CustomModelPolicy::Explicit {
                ids: vec!["big".into()],
            },
            CustomSampling::default(),
            1,
        )
        .unwrap()
        .with_embedding_models(vec![CustomEmbeddingModel {
            id: "qwen/qwen3-embedding-4b".into(),
            dims: 4,
            widths: Default::default(),
        }])
        .unwrap();
        let account = orch.add_custom_provider("Hub".into(), auth, None).unwrap();
        let key = cairn_models::types::catalog::custom_key(&account.id);
        orch.refresh_provider_model_catalog_blocking(&key);
        (orch, scripted, key)
    }

    fn request(purpose: Purpose) -> EmbedRequest {
        EmbedRequest {
            texts: vec!["what is cairn".into()],
            purpose,
            width: Width::Native,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_declared_hub_model_embeds_through_the_runtime_keyed_by_the_offer() {
        let (orch, server, key) = workspace_with_hub(vec![json!({
            "model": "qwen3-embedding-4b",
            "data": [{"index": 0, "embedding": [0.1, 0.2, 0.3, 0.4]}],
            "usage": {"prompt_tokens": 7}
        })])
        .await;
        let result = orch
            .embed(
                ModelScope::default(),
                "qwen/qwen3-embedding-4b".into(),
                request(Purpose::Query),
            )
            .await
            .unwrap();
        assert_eq!(result.provider, key);
        assert_eq!(result.model, "qwen/qwen3-embedding-4b");
        assert_eq!(result.dims, 4);
        let requests = server.requests.lock().unwrap().clone();
        assert_eq!(requests[0].0, "embeddings");
        // Qwen3's query instruction, from the family table.
        assert!(requests[0].1["input"][0]
            .as_str()
            .unwrap()
            .starts_with("Instruct: "));

        // The chat model is not offered for embed, nor the embedding model
        // for chat.
        let catalog = orch.catalog(&ModelScope::default()).unwrap();
        let embedders: Vec<_> = catalog
            .offers_serving(VerbKind::Embed)
            .into_iter()
            .map(|offer| offer.model)
            .collect();
        assert_eq!(embedders, ["qwen/qwen3-embedding-4b"]);
        let hub_chat: Vec<_> = catalog
            .offers_serving(VerbKind::Complete)
            .into_iter()
            .filter(|offer| offer.provider == key)
            .map(|offer| offer.model)
            .collect();
        assert_eq!(hub_chat, ["big"]);

        let route = orch
            .embed_route(
                &ModelScope::default(),
                &offer(&key, "qwen/qwen3-embedding-4b"),
            )
            .unwrap();
        assert_eq!(url(&route), "http://hub.invalid/v1/embeddings");
        assert!(header_names(&route).contains(&"authorization".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_door_never_chooses_a_model_implicitly() {
        let (orch, server, _) = workspace_with_hub(vec![]).await;
        let error = orch
            .embed_blocking(&ModelScope::default(), "  ", &request(Purpose::Document))
            .unwrap_err();
        assert!(matches!(error, EmbedError::InvalidRequest(_)), "{error:?}");
        let error = orch
            .embed_blocking(&ModelScope::default(), "big", &request(Purpose::Document))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("No embedding-capable model serves 'big'"),
            "{error}"
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }

    fn door(model: Option<&str>, texts: usize, width: Option<u32>) -> super::EmbedDoorRequest {
        super::EmbedDoorRequest {
            model: model.map(str::to_string),
            input: (0..texts).map(|index| format!("text {index}")).collect(),
            purpose: super::EmbedPurpose::Document,
            width,
        }
    }

    const HUB_MODEL: &str = "qwen/qwen3-embedding-4b";

    /// The door embeds on exactly the model named, reports the facet's
    /// `(model, dims)`, and leaves an over-limit batch to the facet, which
    /// splits it within the offer's declared limit and keeps input order.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_door_embeds_with_the_named_model_and_the_facet_splits_the_batch() {
        let (orch, server, key) = workspace_with_hub(vec![hub_reply(32), hub_reply(8)]).await;
        let outcome = orch
            .embed_named(None, door(Some(HUB_MODEL), 40, None))
            .await
            .unwrap();
        assert_eq!(
            (
                outcome.provider.as_str(),
                outcome.model.as_str(),
                outcome.dims
            ),
            (key.as_str(), HUB_MODEL, 4)
        );
        assert_eq!(server.batch_sizes(), [32, 8], "the hub's declared limit");
        assert_eq!(outcome.vectors.len(), 40);
        assert_eq!(outcome.vectors[31][0], 31.0);
        assert_eq!(
            outcome.vectors[33][0], 1.0,
            "the second batch follows the first"
        );
        assert_eq!(outcome.usage.input_tokens, Some(40));
        assert_eq!(outcome.cost, None, "the hub reports no cost");

        // The provider-qualified form names the same offer.
        let (orch, _, key) = workspace_with_hub(vec![hub_reply(1)]).await;
        let outcome = orch
            .embed_named(None, door(Some(&format!("{key}/{HUB_MODEL}")), 1, None))
            .await
            .unwrap();
        assert_eq!(outcome.model, HUB_MODEL);
    }

    /// Every refusal comes before any request and names what would work.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_door_refuses_naming_the_fix_and_asks_nothing() {
        let (orch, server, key) = workspace_with_hub(vec![]).await;
        let serving = format!("offers that serve embed: {key}/{HUB_MODEL}");

        for unnamed in [None, Some("  ")] {
            let refusal = orch
                .embed_named(None, door(unnamed, 1, None))
                .await
                .unwrap_err();
            assert!(
                refusal.starts_with("Name the model to embed with"),
                "{refusal}"
            );
            assert!(refusal.contains(&serving), "{refusal}");
        }

        // A chat model on the same hub serves no embeddings.
        let refusal = orch
            .embed_named(None, door(Some("big"), 1, None))
            .await
            .unwrap_err();
        assert!(
            refusal.starts_with("No embedding-capable model serves 'big'"),
            "{refusal}"
        );
        assert!(refusal.contains(&serving), "{refusal}");

        let refusal = orch
            .embed_named(None, door(Some("nowhere/model"), 1, None))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("no installed provider offers"),
            "{refusal}"
        );
        assert!(refusal.contains(&serving), "{refusal}");

        // A width the offer cannot return names the one it can.
        let refusal = orch
            .embed_named(None, door(Some(HUB_MODEL), 1, Some(2)))
            .await
            .unwrap_err();
        assert!(refusal.contains("only its native 4, not 2"), "{refusal}");
        let refusal = orch
            .embed_named(None, door(Some(HUB_MODEL), 1, Some(0)))
            .await
            .unwrap_err();
        assert!(refusal.contains("`width` must be at least 1"), "{refusal}");

        let refusal = orch
            .embed_named(None, door(Some(HUB_MODEL), 0, None))
            .await
            .unwrap_err();
        assert!(refusal.contains("there are no texts to embed"), "{refusal}");
        assert!(server.batch_sizes().is_empty(), "nothing was asked");

        let bare = crate::orchestrator::models::tests::bare_orchestrator().await;
        let refusal = bare
            .embed_named(None, door(None, 1, None))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("No embedding-capable model is configured"),
            "{refusal}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn each_provider_resolves_its_own_route() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let scope = ModelScope::default();

        // No device token in a bare workspace: no gateway offer, and a
        // gateway route says why.
        let catalog = orch.catalog(&scope).unwrap();
        assert!(!catalog
            .providers
            .iter()
            .any(|provider| provider.descriptor.key == CAIRN_GATEWAY_KEY));
        let refusal = orch
            .embed_route(&scope, &offer(CAIRN_GATEWAY_KEY, "cohere-embed-v4"))
            .unwrap_err();
        assert!(refusal.to_string().contains("no Cairn token"), "{refusal}");

        orch.add_account(
            ApiProvider::OpenRouter,
            "OpenRouter".into(),
            ProviderAuth::ApiKey {
                value: "sk-or-test".into(),
            },
            None,
        )
        .unwrap();
        let route = orch
            .embed_route(
                &scope,
                &offer("openrouter", "openai/text-embedding-3-small"),
            )
            .unwrap();
        assert_eq!(url(&route), "https://openrouter.ai/api/v1/embeddings");
        let names = header_names(&route);
        assert!(names.contains(&"authorization".to_string()), "{names:?}");
        assert!(names.contains(&"http-referer".to_string()), "{names:?}");

        orch.add_account(
            ApiProvider::Ollama,
            "Studio".into(),
            ProviderAuth::base_url("http://studio.invalid:11434").unwrap(),
            None,
        )
        .unwrap();
        let route = orch
            .embed_route(&scope, &offer("ollama", "nomic-embed-text:latest"))
            .unwrap();
        assert_eq!(url(&route), "http://studio.invalid:11434/v1/embeddings");
        assert!(header_names(&route).is_empty());

        assert!(orch
            .embed_route(&scope, &offer("claude", "opus"))
            .unwrap_err()
            .to_string()
            .contains("no embedding route"));
    }

    /// Cairn's consumers reach the gateway through the shared door, and its
    /// vectors are labelled as every stored one already is. Registers an
    /// anonymous device token, as every install does.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "reaches Cairn's cloud gateway"]
    async fn the_shared_door_reaches_the_gateway_at_the_stored_width() {
        use crate::embeddings::{Corpus, VectorKey};
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        orch.anon_device_manager.ensure_registered().await;
        let embedder = orch.embedder();
        assert!(embedder.ready(), "no device token registered");
        let embedded = embedder
            .embed(
                Corpus::Turns,
                Purpose::Document,
                vec![
                    "Cairn orchestrates agents across a workspace.".to_string(),
                    "The embedding verb verifies every width it returns.".to_string(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            (embedded.model.as_str(), embedded.dims),
            ("cohere-embed-v4", 1536)
        );
        assert_eq!(embedded.vectors.len(), 2);
        assert_eq!(
            embedder.active(Corpus::Turns),
            Some(VectorKey {
                model: "cohere-embed-v4".into(),
                dims: Some(1536)
            })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn without_a_device_token_embedding_is_off_not_failing() {
        use crate::embeddings::{Corpus, EmbedFailure};
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let embedder = orch.embedder();
        assert!(!embedder.ready());
        assert_eq!(
            embedder
                .embed(Corpus::Resources, Purpose::Document, vec!["x".into()])
                .await,
            Err(EmbedFailure::Unconfigured)
        );
        let health = embedder.health();
        assert!(!health.configured);
        assert_eq!(health.unavailable_since, None);
        assert_eq!(embedder.active(Corpus::Resources), None);
    }

    /// The 10-22 outage's lesson, pinned: an install whose vectors were all
    /// made by the gateway upgrades without re-embedding anything. The space
    /// is taken from the real path (the gateway's catalog offer, resolved for
    /// the unset setting), not a stub, so a drift between the migration's
    /// literals and the gateway's constants fails here rather than purging an
    /// operator's vectors on first start.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_upgraded_gateway_install_reconciles_unchanged_and_keeps_every_vector() {
        use crate::embeddings::space::recorded_space;
        use crate::embeddings::{EmbedSpace, Reconciled, Trigger};
        use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
        use cairn_models::types::embedding::CAIRN_GATEWAY_EMBED_MODEL;

        let migration = include_str!("../../../../turso_migrations/0285_embedding_space.sql");
        assert!(migration.contains(&format!("provider = '{CAIRN_GATEWAY_KEY}'")));
        assert!(migration.contains(&format!("model = '{CAIRN_GATEWAY_EMBED_MODEL}'")));
        assert!(migration.contains(&format!("current_pos, '{CAIRN_GATEWAY_EMBED_MODEL}'")));

        cairn_common::scratch::testing::pin_private_root();
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let head: Vec<_> = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version() < "0285")
            .cloned()
            .collect();
        MigrationRunner::new(head).run(&db).await.unwrap();
        let wide = crate::embeddings::vector::to_bytes(&vec![0.5; 1536]);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('project', 'default', 'Project', 'prj', '/tmp/prj', 1, 1);
             INSERT INTO jobs (id, project_id, status, created_at, updated_at)
               VALUES ('job', 'project', 'running', 1, 1);
             INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('s', 'job', 1, 1);
             INSERT INTO turn_embeddings (turn_id, session_id, sequence, embedding, model, dims, created_at, updated_at)
               VALUES ('t-tomb', 's', 1, x'', 'cohere-embed-v4', 0, 1, 1);",
        )
        .await
        .unwrap();
        for (table, sql) in [
            (
                "turn",
                "INSERT INTO turn_embeddings (turn_id, session_id, sequence, embedding, model, dims, created_at, updated_at)
                 VALUES ('t-vec', 's', 2, ?1, 'cohere-embed-v4', 1536, 1, 1)",
            ),
            (
                "resource",
                "INSERT INTO resource_embeddings (uri, embedding, model, dims, created_at, updated_at)
                 VALUES ('cairn://p/prj/1', ?1, 'cohere-embed-v4', 1536, 1, 1)",
            ),
            ("position", "UPDATE sessions SET current_pos = ?1 WHERE id = 's'"),
        ] {
            db.execute(sql, cairn_db::turso::params![wide.clone()])
                .await
                .unwrap_or_else(|error| panic!("seeding the {table} row: {error}"));
        }
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(crate::db::DbState::new(Arc::new(db), search));
        let services = Arc::new(crate::services::testing::TestServicesBuilder::new().build());
        let orch = crate::orchestrator::OrchestratorBuilder::new(db_state, services, config_dir)
            .subscriptions(Arc::new(cairn_models::subscription::Unsubscribed))
            .build();
        orch.anon_device_manager
            .store_token_for_test("header.payload.sig")
            .await;

        let gateway = EmbedSpace {
            provider: CAIRN_GATEWAY_KEY.into(),
            model: CAIRN_GATEWAY_EMBED_MODEL.into(),
            turn_dims: Some(1536),
            resource_dims: Some(1536),
        };
        assert_eq!(orch.embedder().space(), Some(gateway.clone()));
        let local = orch.db.local.clone();
        assert_eq!(recorded_space(&local).await.unwrap(), Some(gateway));

        let counts = || async {
            local
                .query_one(
                    "SELECT (SELECT COUNT(*) FROM turn_embeddings),
                            (SELECT COUNT(*) FROM resource_embeddings),
                            (SELECT COUNT(*) FROM session_positions)",
                    (),
                    |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
                )
                .await
                .unwrap()
        };
        assert_eq!(counts().await, (2, 1, 1));
        // `Unchanged` is returned before any purge or purge log can happen, so
        // it is the whole proof here. The test binary's captured log is shared
        // by every test in the process and cannot be attributed to this one.
        let keeper = orch.space_keeper();
        for trigger in [Trigger::Startup, Trigger::Sweep] {
            assert_eq!(
                keeper.reconcile(trigger).await.unwrap(),
                Reconciled::Unchanged
            );
        }
        assert_eq!(counts().await, (2, 1, 1), "nothing re-embeds");
    }

    #[test]
    fn an_unset_selection_names_the_gateway_offer() {
        assert_eq!(
            super::default_embedding_selection(),
            "cairn/cohere-embed-v4"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_selection_is_the_setting_and_choosing_one_is_consent() {
        use crate::models::UpdateSettings;
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        assert_eq!(
            orch.embedding_selection(),
            super::EmbedSelection {
                value: "cairn/cohere-embed-v4".into(),
                explicit: false,
            }
        );
        assert!(!orch.history_consented(), "no account, no choice");

        orch.update_settings(UpdateSettings {
            embedding_model: Some(Some(" ollama/nomic-embed-text ".into())),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            orch.embedding_selection(),
            super::EmbedSelection {
                value: "ollama/nomic-embed-text".into(),
                explicit: true,
            }
        );
        assert!(orch.history_consented());

        orch.update_settings(UpdateSettings {
            embedding_model: Some(None),
            ..Default::default()
        })
        .unwrap();
        assert!(!orch.embedding_selection().explicit);
        assert!(!orch.history_consented());
    }

    #[test]
    fn the_gateway_offers_cohere_v4_at_its_fixed_width() {
        let provider = super::cairn_gateway_provider();
        assert_eq!(provider.descriptor.key, CAIRN_GATEWAY_KEY);
        let served = provider.serves_for("cohere-embed-v4");
        assert_eq!(
            served,
            vec![ServedVerb {
                verb: Verb::Embed(EmbeddingCapabilities::cairn_gateway()),
                wire: Wire::CairnEmbed,
            }]
        );
    }
}
