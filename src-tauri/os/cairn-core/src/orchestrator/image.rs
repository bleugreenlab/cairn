//! The image verb as agents and scripts call it (`run cairn://models/image`).
//!
//! The facet draws; this module decides with what and where. The model is
//! the caller's, else the workspace's `imageModel` default, else a refusal
//! naming that setting ([`super::model_defaults`]), so an unset default is
//! the off switch. It is then selected through the facet's one request-aware
//! walk ([`cairn_models::interface::Catalog::admitting_offer`]), which only
//! walks offers of that model: a model that cannot take the request is
//! refused naming the offers that could, never answered by another.
//!
//! Where an offer is drawn follows from the wire it declares: OpenRouter's
//! Image API on this workspace's key, a custom OpenAI Images provider at its
//! own address, or Codex's image tool on a pooled app-server signed in to
//! ChatGPT. Nothing is kept here: the result carries the bytes, and the
//! caller decides where they live.

use std::time::Duration;

use cairn_models::image::{admit, image, request_problems};
use cairn_models::interface::image::{
    CodexImageRoute, ImageCall, ImageCapabilities, ImageError, ImageRequest, ImageResult,
    ImageRoute, IMAGE_TIMEOUT,
};
use cairn_models::interface::{ModelScope, Models, OfferQuery, SelectedOffer};
use cairn_models::types::catalog::{self, VerbKind, Wire};

use super::model_defaults::DefaultedVerb;
use super::Orchestrator;

/// The facet call for `offer` on `route`.
fn image_call<'a>(
    route: &'a ImageRoute,
    offer: &'a SelectedOffer,
    capabilities: &'a ImageCapabilities,
    request: &'a ImageRequest,
    timeout: Duration,
) -> ImageCall<'a> {
    ImageCall {
        route,
        model: &offer.model,
        capabilities,
        request,
        timeout,
    }
}

/// Which deployment draws an offer, and over what.
enum Drawer {
    /// One of the HTTP image wires, through the runtime's image transport.
    Http(ImageRoute),
    /// Codex's image tool, on a pooled app-server.
    Codex(cairn_models::image::codex::CodexImages),
}

impl Orchestrator {
    /// Draw `request` with `model` (or the workspace's `imageModel` when it
    /// names none), narrowed to `provider`'s offers when given, within
    /// `timeout` (at most the facet's own [`IMAGE_TIMEOUT`]). Blocking: it
    /// posts to the provider, or waits on Codex's image workers.
    pub(crate) fn draw_blocking(
        &self,
        project_id: Option<&str>,
        model: Option<&str>,
        provider: Option<&str>,
        request: &ImageRequest,
        timeout: Duration,
    ) -> Result<(ImageResult, SelectedOffer), String> {
        let model = self.explicit_model_for(DefaultedVerb::Image, model)?;
        request_problems(request).map_err(|error| error.to_string())?;
        let scope = ModelScope {
            project_id: project_id.map(str::to_string),
        };
        let catalog = self
            .catalog(&scope)
            .map_err(|error| format!("the model catalog is unavailable: {error}"))?;
        let provider = provider.map(str::trim).filter(|key| !key.is_empty());
        let (offer, capabilities) = catalog
            .admitting_offer(
                OfferQuery {
                    requested: Some(&model),
                    provider,
                    ..OfferQuery::new(VerbKind::Image)
                },
                |offer| -> Result<ImageCapabilities, ImageError> {
                    let capabilities = offer.served.verb.image().cloned().ok_or_else(|| {
                        ImageError::InvalidRequest("it does not draw images".to_string())
                    })?;
                    admit(&capabilities, offer.served.wire, request)?;
                    Ok(capabilities)
                },
            )
            .map_err(|refusal| refusal.to_string())?;
        let timeout = timeout.min(IMAGE_TIMEOUT);
        let result = match self.drawer(project_id, &offer)? {
            Drawer::Http(route) => image(
                self.models_runtime.images.as_ref(),
                image_call(&route, &offer, &capabilities, request, timeout),
            ),
            Drawer::Codex(transport) => {
                let route = ImageRoute::Codex(CodexImageRoute::chatgpt(None));
                image(
                    &transport,
                    image_call(&route, &offer, &capabilities, request, timeout),
                )
            }
        };
        result
            .map(|result| (result, offer))
            .map_err(|error| error.to_string())
    }

    /// Where `offer` is drawn, with this workspace's credentials for it.
    fn drawer(&self, project_id: Option<&str>, offer: &SelectedOffer) -> Result<Drawer, String> {
        let provider = offer.provider.as_str();
        let unavailable = |why: String| format!("{provider} {why}");
        match offer.served.wire {
            Wire::OpenRouterImages => {
                let api_key = crate::backends::openrouter::openrouter_api_key(self)
                    .ok_or_else(|| unavailable("needs an OpenRouter API key to draw".into()))?;
                Ok(Drawer::Http(cairn_models::image::openrouter::endpoint(
                    &api_key,
                    &cairn_models::openrouter::Attribution::cairn(),
                )))
            }
            Wire::OpenAiImages => {
                let account_id = catalog::custom_account_id(provider).ok_or_else(|| {
                    unavailable(
                        "serves OpenAI Images, which only a custom provider reaches in this build"
                            .into(),
                    )
                })?;
                let config = crate::backends::custom::CustomProviderConfig::resolve(
                    self, account_id, project_id,
                )
                .map_err(unavailable)?;
                config.image_route().map(Drawer::Http)
            }
            Wire::CodexAppServer => {
                crate::backends::codex::codex_image_transport(self, project_id, &offer.model)
                    .map(Drawer::Codex)
                    .map_err(unavailable)
            }
            wire => Err(unavailable(format!("has no image route over {wire}"))),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use base64::Engine;
    use cairn_models::image_wire::ImageWireError;
    use cairn_models::interface::image::ImageResult;
    use cairn_models::interface::image::{
        ImageBody, ImagePost, ImageReply, ImageRequest, ImageTransport,
    };
    use cairn_models::interface::SelectedOffer;
    use serde_json::{json, Value};

    use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth};
    use crate::models::UpdateSettings;
    use crate::orchestrator::models::ModelRuntime;
    use crate::orchestrator::Orchestrator;

    pub(crate) const PNG: &[u8] = b"\x89PNG\r\n\x1a\nlighthouse";

    /// An OpenAI Images server that draws one PNG per call and records what
    /// it was sent, by the backend key the route is attributed to.
    #[derive(Default)]
    pub(crate) struct Studio {
        posts: Mutex<Vec<(String, Value)>>,
    }

    impl ImageTransport for Studio {
        fn post(&self, post: ImagePost<'_>) -> Result<ImageReply, ImageWireError> {
            let ImageBody::OpenAiGenerations(body) = post.body else {
                panic!("expected a generation, got {:?}", post.body);
            };
            self.posts
                .lock()
                .unwrap()
                .push((post.route.backend_key().to_string(), body));
            let b64 = base64::engine::general_purpose::STANDARD.encode(PNG);
            Ok(ImageReply::OpenAi(
                serde_json::from_value(json!({"data": [{"b64_json": b64}]})).unwrap(),
            ))
        }
    }

    impl Studio {
        pub(crate) fn count(&self) -> usize {
            self.posts.lock().unwrap().len()
        }
    }

    /// A workspace whose one image provider is a custom OpenAI Images server
    /// offering `gpt-image-1`, answered by [`Studio`].
    pub(crate) async fn workspace() -> (Orchestrator, Arc<Studio>, String) {
        let mut orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let studio = Arc::new(Studio::default());
        orch.models_runtime = ModelRuntime {
            images: studio.clone(),
            ..orch.models_runtime.clone()
        };
        let account = orch
            .add_custom_provider(
                "Studio".into(),
                ProviderAuth::custom_endpoint(
                    // Refused at once, with no name to resolve, so the
                    // discovery the fixture runs fails fast.
                    "http://127.0.0.1:1/v1",
                    CustomProtocol::OpenAiImages,
                    Some("sk-studio"),
                    &[],
                    CustomModelPolicy::Explicit {
                        ids: vec!["gpt-image-1".into()],
                    },
                    CustomSampling::default(),
                    1,
                )
                .unwrap(),
                None,
            )
            .unwrap();
        let key = cairn_models::types::catalog::custom_key(&account.id);
        refresh(&orch, &key).await;
        (orch, studio, key)
    }

    /// Rediscover `key`'s models off the async runtime: discovery posts with
    /// a blocking HTTP client, which may not be dropped on an async thread.
    pub(crate) async fn refresh(orch: &Orchestrator, key: &str) {
        let orch = orch.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            orch.refresh_provider_model_catalog_blocking(&key);
        })
        .await
        .unwrap();
    }

    async fn draw(
        orch: &Orchestrator,
        model: Option<&str>,
        provider: Option<&str>,
    ) -> Result<(ImageResult, SelectedOffer), String> {
        let orch = orch.clone();
        let model = model.map(str::to_string);
        let provider = provider.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            orch.draw_blocking(
                None,
                model.as_deref(),
                provider.as_deref(),
                &ImageRequest::new("a lighthouse at dusk"),
                Duration::from_secs(5),
            )
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_named_model_draws_on_its_own_provider_and_returns_the_bytes() {
        let (orch, studio, key) = workspace().await;
        let (result, offer) = draw(&orch, Some("gpt-image-1"), None).await.unwrap();
        assert_eq!(offer.provider, key);
        assert_eq!(offer.model, "gpt-image-1");
        assert_eq!(result.images.len(), 1);
        assert_eq!(result.images[0].bytes, PNG);
        assert_eq!(result.images[0].media_type, "image/png");
        assert_eq!(result.provenance.provider, key);
        let posts = studio.posts.lock().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].0, key);
        assert_eq!(posts[0].1["model"], "gpt-image-1");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_workspace_default_answers_only_a_request_that_names_no_model() {
        let (orch, studio, _) = workspace().await;
        let refused = draw(&orch, None, None).await.unwrap_err();
        assert!(refused.contains("`imageModel`"), "{refused}");
        assert_eq!(studio.count(), 0, "an unset default is the off switch");

        orch.update_settings(UpdateSettings {
            image_model: Some(Some("gpt-image-1".into())),
            ..Default::default()
        })
        .unwrap();
        let (_, offer) = draw(&orch, None, None).await.unwrap();
        assert_eq!(offer.model, "gpt-image-1");

        let refused = draw(&orch, Some("no-such-model"), None).await.unwrap_err();
        assert!(refused.contains("no-such-model"), "{refused}");
        assert_eq!(
            studio.count(),
            1,
            "a named model is never answered by another"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_that_does_not_offer_the_model_is_refused_before_any_call() {
        let (orch, studio, _) = workspace().await;
        let refused = draw(&orch, Some("gpt-image-1"), Some("openrouter"))
            .await
            .unwrap_err();
        assert!(refused.contains("gpt-image-1"), "{refused}");
        assert_eq!(studio.count(), 0);
    }
}
