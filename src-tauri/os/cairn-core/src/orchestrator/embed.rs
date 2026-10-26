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
//! the one selected model ([`active_selection`]) behind the shared health gate
//! (`crate::embeddings::embedder`).
//!
//! [`Complete`]: cairn_models::interface::complete::Complete

use std::sync::Arc;

use cairn_models::embed::{embed, gateway, openai};
use cairn_models::interface::complete::CompletionError;
use cairn_models::interface::embed::{
    EmbedCall, EmbedError, EmbedRequest, EmbedRoute, Embeddings, Purpose, Width, EMBED_TIMEOUT,
};
use cairn_models::interface::{CatalogProvider, ModelScope, Models, SelectedOffer};
use cairn_models::types::catalog::{
    self, DiscoveredModel, Modalities, Modality, ProviderModelCatalog, ServedVerb, Verb, Wire,
};
use cairn_models::types::embedding::{
    EmbeddingCapabilities, CAIRN_GATEWAY_EMBED_MODEL, CAIRN_GATEWAY_KEY,
};

use super::{resolve_embed_token, Orchestrator};
use crate::backends::custom::CustomProviderConfig;
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::embeddings::embedder::{EmbedSource, FacetEmbedder, SelectedEmbedOffer};
use crate::embeddings::Embed;

/// The embedding model every Cairn consumer uses: the Cairn gateway's Cohere
/// Embed v4, the model every stored vector was made by.
pub(crate) fn active_selection() -> String {
    format!("{CAIRN_GATEWAY_KEY}/{CAIRN_GATEWAY_EMBED_MODEL}")
}

/// The selected model, as this workspace reaches it.
struct WorkspaceEmbedSource {
    orch: Orchestrator,
}

impl EmbedSource for WorkspaceEmbedSource {
    fn selection(&self) -> String {
        active_selection()
    }

    /// The gateway is called with this device's Cairn token; an install
    /// without one has embedding off, which is not an outage.
    fn configured(&self) -> bool {
        self.orch.embed_token().is_some()
    }

    fn offer(&self) -> Option<SelectedEmbedOffer> {
        let catalog = self.orch.catalog(&ModelScope::default()).ok()?;
        let probe = EmbedRequest {
            texts: Vec::new(),
            purpose: Purpose::Document,
            width: Width::Native,
        };
        let (offer, capabilities) = catalog
            .embedding_offer(Some(&active_selection()), &probe)
            .ok()?;
        Some(SelectedEmbedOffer {
            model: offer.model,
            capabilities,
        })
    }

    fn embed(&self, request: &EmbedRequest) -> Result<Embeddings, EmbedError> {
        self.orch
            .embed_blocking(&ModelScope::default(), &active_selection(), request)
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
        embed(
            self.models_runtime.complete.as_ref(),
            EmbedCall {
                route: &route,
                model: &offer.model,
                capabilities: &capabilities,
                request,
                timeout: EMBED_TIMEOUT,
            },
        )
    }

    /// [`Self::embed_blocking`], off the async runtime.
    pub async fn embed(
        &self,
        scope: ModelScope,
        model: String,
        request: EmbedRequest,
    ) -> Result<Embeddings, EmbedError> {
        let orch = self.clone();
        tokio::task::spawn_blocking(move || orch.embed_blocking(&scope, &model, &request))
            .await
            .map_err(|error| {
                EmbedError::Completion(CompletionError::Upstream(format!(
                    "the embedding task failed: {error}"
                )))
            })?
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

#[cfg(test)]
mod tests {
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
    struct Scripted {
        replies: Mutex<VecDeque<Value>>,
        requests: Mutex<Vec<(&'static str, Value)>>,
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
    async fn workspace_with_hub(replies: Vec<Value>) -> (Orchestrator, Arc<Scripted>, String) {
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

    #[test]
    fn the_selection_names_the_gateway_offer() {
        assert_eq!(super::active_selection(), "cairn/cohere-embed-v4");
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
