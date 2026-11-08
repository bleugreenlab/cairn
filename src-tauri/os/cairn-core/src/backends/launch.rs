//! The launch doors: the only way a session or a completion reaches a backend.
//!
//! Each door asks the cairn-models facet whether the named model serves the
//! verb it is about to be launched for ([`CatalogProvider::admit`]), and hands
//! the backend proof that it asked. [`AgentBackend::start_session`] and
//! [`AgentBackend::complete`] require that proof, and only this module can mint
//! it, so a launch path that skips the gate does not compile.
//!
//! The gate builds the launching provider's record directly
//! ([`Orchestrator::catalog_provider`]) rather than looking it up among the
//! installed providers: whether a provider is installed is enforced where it
//! always was, and is not this gate's question.
//!
//! The same gate decides a call's zero-data-retention requirement
//! ([`super::retention::admit`]), and the proof carries what it admitted, so a
//! backend that routes by retention (OpenRouter) sends the routing preference
//! on every request, and a requirement nothing can meet is refused before any
//! request. Decide and embed choose their offer through the catalog rather
//! than this door; [`admit_route`] is their half of the same gate.
//!
//! [`CatalogProvider::admit`]: cairn_models::interface::CatalogProvider::admit

use cairn_models::interface::retention::Requirements;
use cairn_models::interface::{NoOffer, SelectedOffer};
use cairn_models::types::catalog::VerbKind;

use super::retention::ZdrAdmission;
use super::{AgentBackend, CompletionFailure, CompletionOutcome, CompletionRequest, SessionConfig};
use crate::orchestrator::Orchestrator;

/// Proof a launch passed the verb gate, and the zero-data-retention
/// requirement it admitted. Only this module can make one, and
/// [`AgentBackend::start_session`]/[`AgentBackend::complete`] (and every
/// decision and embedding route) require it, so no launch path can skip the
/// gate.
pub struct Admitted {
    _sealed: (),
    zero_data_retention: Option<ZdrAdmission>,
}

impl Admitted {
    /// How the call's zero-data-retention requirement was met, when one
    /// applied. A backend that can carry a retention routing preference must
    /// send it whenever this is set.
    pub(crate) fn zero_data_retention(&self) -> Option<&ZdrAdmission> {
        self.zero_data_retention.as_ref()
    }

    /// A proof for a test that drives a backend or adapter directly.
    #[cfg(test)]
    pub(crate) fn for_test(zero_data_retention: Option<ZdrAdmission>) -> Self {
        Self {
            _sealed: (),
            zero_data_retention,
        }
    }
}

/// Why a completion launched through [`complete`] did not answer.
#[derive(Debug)]
pub(crate) enum LaunchError {
    /// The model does not serve completions; nothing was sent.
    Unserved(NoOffer),
    /// The call requires zero data retention and the provider cannot keep it;
    /// nothing was sent. The text names the offers that could.
    Retention(String),
    /// The backend was asked and failed, on the account it names when it had
    /// chosen one.
    Completion(CompletionFailure),
}

/// Start `config` on `backend`, which serves `provider`, once its model is
/// admitted for sessions. A session carries no requirements of its own yet,
/// so only the workspace's OpenRouter floor can require zero retention of it.
pub(crate) fn start_session(
    orch: &Orchestrator,
    provider: &str,
    backend: &dyn AgentBackend,
    config: SessionConfig,
) -> Result<(), String> {
    let project_id = Some(config.project_id.as_str()).filter(|id| !id.is_empty());
    let model = config.model.as_ref().map(|model| model.as_str());
    let mut admitted = admit(orch, provider, model, VerbKind::Session, project_id)
        .map_err(|refusal| refusal.to_string())?;
    admitted.zero_data_retention = super::retention::admit(
        orch,
        provider,
        backend,
        model.unwrap_or_default(),
        &Requirements::default(),
        project_id,
        VerbKind::Session,
    )?;
    backend.start_session(config, orch, admitted)
}

/// Run `request` on `backend`, which serves `provider`, once its model is
/// admitted for completions and its requirements are met.
pub(crate) fn complete(
    orch: &Orchestrator,
    provider: &str,
    backend: &dyn AgentBackend,
    request: CompletionRequest,
) -> Result<CompletionOutcome, LaunchError> {
    let mut admitted = admit(
        orch,
        provider,
        Some(request.model.as_str()),
        VerbKind::Complete,
        request.project_id.as_deref(),
    )
    .map_err(LaunchError::Unserved)?;
    admitted.zero_data_retention = super::retention::admit(
        orch,
        provider,
        backend,
        &request.model,
        &request.requirements,
        request.project_id.as_deref(),
        VerbKind::Complete,
    )
    .map_err(LaunchError::Retention)?;
    backend
        .complete(request, orch, admitted)
        .map_err(LaunchError::Completion)
}

/// Admit a decision or an embedding on `offer`, which the catalog already
/// chose as serving `verb`: only its requirements are left to meet. The proof
/// is what [`AgentBackend::decision_route`] and the embedding routes take.
pub(crate) fn admit_route(
    orch: &Orchestrator,
    offer: &SelectedOffer,
    requirements: &Requirements,
    project_id: Option<&str>,
    verb: VerbKind,
) -> Result<Admitted, String> {
    let backend = super::backend_for_name(Some(&offer.provider));
    Ok(Admitted {
        _sealed: (),
        zero_data_retention: super::retention::admit(
            orch,
            &offer.provider,
            backend.as_ref(),
            &offer.model,
            requirements,
            project_id,
            verb,
        )?,
    })
}

/// The proof for a capability probe: a fixed question Cairn wrote, carrying
/// none of a caller's text, asked only to learn whether an offer can measure
/// decisions at all. It is held to the workspace floor like any call, so an
/// OpenRouter probe still routes only to zero-retention endpoints.
pub(crate) fn admit_probe(orch: &Orchestrator, offer: &SelectedOffer) -> Result<Admitted, String> {
    admit_route(
        orch,
        offer,
        &Requirements::default(),
        None,
        VerbKind::Decide,
    )
}

/// Why a relayed request got no route.
#[derive(Debug)]
pub(crate) enum RelayLaunchError {
    /// The model does not serve the door's verb; nothing was asked of the
    /// backend.
    Unserved(NoOffer),
    Refused(super::relay::RelayRefusal),
}

/// The route for an outside client's `request` on `backend`, which serves
/// `provider`, once its model is admitted for the door's verb.
pub(crate) fn relay_route(
    orch: &Orchestrator,
    provider: &str,
    backend: &dyn AgentBackend,
    request: super::relay::RelayRequest<'_>,
) -> Result<cairn_models::interface::relay::RelayRoute, RelayLaunchError> {
    let admitted = admit(
        orch,
        provider,
        Some(request.model),
        request.door.verb(),
        None,
    )
    .map_err(RelayLaunchError::Unserved)?;
    backend
        .relay_route(orch, request, admitted)
        .map_err(RelayLaunchError::Refused)
}

/// Admit a launch of `model` on `provider` for `verb`.
///
/// A launch that names no model runs the backend's own default, which is
/// conversational by construction, so it is admitted.
fn admit(
    orch: &Orchestrator,
    provider: &str,
    model: Option<&str>,
    verb: VerbKind,
    project_id: Option<&str>,
) -> Result<Admitted, NoOffer> {
    let admitted = Admitted {
        _sealed: (),
        zero_data_retention: None,
    };
    let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) else {
        return Ok(admitted);
    };
    orch.catalog_provider(provider, project_id)
        .admit(model, verb)?;
    Ok(admitted)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use cairn_models::types::catalog::{
        claude_models, DiscoveredModel, ProviderModelCatalog, ServedVerb,
    };

    use super::*;
    use crate::backends::codex::CodexBackend;
    use crate::backends::CompletionError;
    use crate::backends::{CompletionMessage, CompletionRole, ResolvedTools};
    use crate::orchestrator::launch_coverage_tests::config;
    use crate::orchestrator::models::tests::bare_orchestrator;

    const IMAGE_MODEL: &str = cairn_models::image::codex::CODEX_IMAGE_MODEL;

    fn chat_model(id: &str) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.to_string();
        model.model = id.to_string();
        model.display_name = id.to_string();
        model
    }

    /// Codex in each state its discovery can be in: listed (its image model
    /// beside a chat model), never run (the cache is cold at startup), and
    /// failed. The image model is declared in all three.
    async fn codex_orchestrators() -> Vec<(&'static str, Orchestrator)> {
        let listed = bare_orchestrator().await;
        listed.seed_model_catalog_for_test(ProviderModelCatalog {
            backend: "codex".to_string(),
            models: vec![
                cairn_models::image::codex::discovered_model(),
                chat_model("gpt-5.5"),
            ],
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        });
        let failed = bare_orchestrator().await;
        failed.seed_model_catalog_for_test(ProviderModelCatalog {
            backend: "codex".to_string(),
            models: Vec::new(),
            options: Vec::new(),
            refreshed_at: None,
            error: Some("codex is not installed".to_string()),
        });
        vec![
            ("listed", listed),
            ("cold", bare_orchestrator().await),
            ("failed", failed),
        ]
    }

    /// Counts every launch that reached it.
    struct Recording(Arc<AtomicUsize>);

    impl AgentBackend for Recording {
        fn name(&self) -> &str {
            "Recording"
        }
        fn is_available(&self) -> Result<(), String> {
            Ok(())
        }
        fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
            Ok(Vec::new())
        }
        fn serves(&self, _: &Orchestrator, _: Option<&str>) -> Vec<ServedVerb> {
            Vec::new()
        }
        fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
            ResolvedTools {
                allowed: Vec::new(),
                disallowed: Vec::new(),
            }
        }
        fn start_session(
            &self,
            _: SessionConfig,
            _: &Orchestrator,
            _: Admitted,
        ) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn complete(
            &self,
            _: CompletionRequest,
            _: &Orchestrator,
            _: Admitted,
        ) -> Result<CompletionOutcome, CompletionFailure> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(CompletionError::BackendUnavailable.into())
        }
        fn supports_resume(&self) -> bool {
            false
        }
        fn supports_warm_processes(&self) -> bool {
            false
        }
    }

    fn request(model: &str) -> CompletionRequest {
        CompletionRequest {
            system: None,
            messages: vec![CompletionMessage {
                role: CompletionRole::User,
                content: "hi".to_string(),
            }],
            model: model.to_string(),
            project_id: None,
            options: Default::default(),
            output_schema: None,
            timeout: std::time::Duration::from_secs(5),
            requirements: Default::default(),
        }
    }

    /// OpenRouter's chat endpoint as a script: answers every completion and
    /// keeps what was sent.
    #[derive(Default)]
    struct Chat(std::sync::Mutex<Vec<serde_json::Value>>);

    impl cairn_models::interface::complete::Complete for Chat {
        fn complete(
            &self,
            call: cairn_models::interface::complete::CompletionCall<'_>,
        ) -> Result<cairn_models::interface::complete::CompletionReply, CompletionError> {
            self.0.lock().unwrap().push(call.body);
            Ok(cairn_models::interface::complete::CompletionReply::Chat(
                serde_json::from_value(serde_json::json!({
                    "model": "openai/gpt-5",
                    "choices": [{"message": {"role": "assistant", "content": "ok"}}]
                }))
                .unwrap(),
            ))
        }
    }

    /// Every OpenRouter completion and session passes the gate's
    /// zero-retention decision: a required call carries `provider.zdr`, a
    /// model with no zero-retention endpoint is refused before any request,
    /// the workspace setting requires it of a call that says nothing, and a
    /// hosted provider with no checkable guarantee refuses it.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_gate_decides_zero_retention_for_every_openrouter_completion_and_session() {
        use crate::backends::retention::fixtures::{
            install_openrouter, require_zdr_of_openrouter, zdr_list,
        };
        use cairn_models::interface::retention::Requirements;
        use serde_json::json;

        let mut orch = bare_orchestrator().await;
        let chat = Arc::new(Chat::default());
        let runtime = crate::orchestrator::models::ModelRuntime {
            complete: chat.clone(),
            ..orch.models_runtime().clone()
        };
        orch.set_models_runtime_for_test(runtime);
        install_openrouter(&orch);
        orch.seed_model_catalog_for_test(ProviderModelCatalog {
            backend: "claude".to_string(),
            models: claude_models(),
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        });
        zdr_list(&mut orch, &[("openai/gpt-5", "OpenAI", "openai", false)]);
        tokio::task::spawn_blocking(move || {
            let openrouter = super::super::openrouter::OpenRouterBackend;
            let required = |model: &str| CompletionRequest {
                requirements: Requirements::zero_data_retention(),
                ..request(model)
            };
            complete(&orch, "openrouter", &openrouter, required("openai/gpt-5")).unwrap();
            assert_eq!(
                chat.0.lock().unwrap()[0]["provider"],
                json!({"require_parameters": true, "zdr": true})
            );
            match complete(&orch, "openrouter", &openrouter, required("openai/gpt-4o")) {
                Err(LaunchError::Retention(refusal)) => {
                    assert!(
                        refusal.contains("openai/gpt-4o on openrouter cannot keep it"),
                        "{refusal}"
                    );
                    assert!(refusal.contains("Nothing was sent"), "{refusal}");
                }
                other => panic!("expected the retention refusal, got {other:?}"),
            }
            let claude = super::super::claude::ClaudeBackend;
            match complete(&orch, "claude", &claude, required("opus")) {
                Err(LaunchError::Retention(refusal)) => assert!(
                    refusal.contains("offers no zero-data-retention guarantee"),
                    "{refusal}"
                ),
                other => panic!("expected the retention refusal, got {other:?}"),
            }
            assert_eq!(chat.0.lock().unwrap().len(), 1, "no refusal sent anything");

            require_zdr_of_openrouter(&orch);
            let refusal = start_session(
                &orch,
                "openrouter",
                &openrouter,
                config("run-or", "session-or", "openai/gpt-4o", false),
            )
            .unwrap_err();
            assert!(
                refusal.contains("the workspace's OpenRouter setting requires it"),
                "{refusal}"
            );
            complete(&orch, "openrouter", &openrouter, request("openai/gpt-5")).unwrap();
            assert_eq!(
                chat.0.lock().unwrap()[1]["provider"]["zdr"],
                json!(true),
                "a call that says nothing still carries the workspace's requirement"
            );
            // Claude is not OpenRouter: the setting asks nothing of it.
            assert_eq!(
                super::super::retention::admit(
                    &orch,
                    "claude",
                    &claude,
                    "opus",
                    &Requirements::default(),
                    None,
                    VerbKind::Complete,
                ),
                Ok(None)
            );
        })
        .await
        .unwrap();
    }

    fn assert_refuses_the_image_model(text: &str) {
        assert!(
            text.contains(&format!("'{IMAGE_MODEL}' on codex serves only image")),
            "{text}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_doors_refuse_a_model_that_does_not_serve_the_verb_before_the_backend_sees_it() {
        for (state, orch) in codex_orchestrators().await {
            tokio::task::spawn_blocking(move || doors_gate_on_the_verb(state, &orch))
                .await
                .unwrap();
        }
    }

    /// What the doors admit, whatever state Codex's discovery is in: a chat
    /// model discovery did not list is still a chat model.
    fn doors_gate_on_the_verb(state: &str, orch: &Orchestrator) {
        {
            let launched = Arc::new(AtomicUsize::new(0));
            let backend = Recording(launched.clone());

            let refusal = start_session(
                orch,
                "codex",
                &backend,
                config("run-image", "session-image", IMAGE_MODEL, false),
            )
            .unwrap_err();
            assert!(
                refusal.starts_with(&format!("No session-capable model serves '{IMAGE_MODEL}'"))
            );
            assert_refuses_the_image_model(&refusal);
            match complete(orch, "codex", &backend, request(IMAGE_MODEL)) {
                Err(LaunchError::Unserved(refusal)) => {
                    assert_eq!(refusal.verb, VerbKind::Complete);
                    assert_refuses_the_image_model(&refusal.to_string());
                }
                other => panic!("expected the gate's refusal, got {other:?}"),
            }
            assert_eq!(
                launched.load(Ordering::SeqCst),
                0,
                "nothing reached the backend"
            );

            start_session(
                orch,
                "codex",
                &backend,
                config("run-chat", "session-chat", "gpt-5.5", false),
            )
            .unwrap();
            let mut unnamed = config("run-default", "session-default", "unused", false);
            unnamed.model = None;
            start_session(orch, "codex", &backend, unnamed).unwrap();
            assert!(matches!(
                complete(orch, "codex", &backend, request("gpt-5.5")),
                Err(LaunchError::Completion(CompletionFailure {
                    error: CompletionError::BackendUnavailable,
                    account_id: None,
                }))
            ));
            // A model discovery has not listed serves what Codex serves for
            // any model.
            start_session(
                orch,
                "codex",
                &backend,
                config("run-new", "session-new", "gpt-6-preview", false),
            )
            .unwrap();
            assert_eq!(launched.load(Ordering::SeqCst), 4, "{state}");
        }
    }

    /// The real Codex backend would fail on the missing credential; the gate's
    /// refusal arriving instead proves it came before any app-server call.
    #[tokio::test(flavor = "multi_thread")]
    async fn codex_refuses_its_image_model_at_both_doors_on_every_session_shape() {
        for (_, orch) in codex_orchestrators().await {
            tokio::task::spawn_blocking(move || {
                for ephemeral in [false, true] {
                    let refusal = start_session(
                        &orch,
                        "codex",
                        &CodexBackend,
                        config("run-codex", "session-codex", IMAGE_MODEL, ephemeral),
                    )
                    .unwrap_err();
                    assert_refuses_the_image_model(&refusal);
                }
                match complete(&orch, "codex", &CodexBackend, request(IMAGE_MODEL)) {
                    Err(LaunchError::Unserved(refusal)) => {
                        assert_refuses_the_image_model(&refusal.to_string())
                    }
                    other => panic!("expected the gate's refusal, got {other:?}"),
                }
            })
            .await
            .unwrap();
        }
    }
}
