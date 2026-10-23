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
//! [`CatalogProvider::admit`]: cairn_models::interface::CatalogProvider::admit

use cairn_models::interface::NoOffer;
use cairn_models::types::catalog::VerbKind;

use super::{AgentBackend, CompletionError, CompletionOutcome, CompletionRequest, SessionConfig};
use crate::orchestrator::Orchestrator;

/// Proof a launch passed the verb gate. Only this module can make one, and
/// [`AgentBackend::start_session`]/[`AgentBackend::complete`] require it, so no
/// launch path can skip the gate.
pub struct Admitted {
    _sealed: (),
}

/// Why a completion launched through [`complete`] did not answer.
#[derive(Debug)]
pub(crate) enum LaunchError {
    /// The model does not serve completions; nothing was sent.
    Unserved(NoOffer),
    /// The backend was asked and failed.
    Completion(CompletionError),
}

/// Start `config` on `backend`, which serves `provider`, once its model is
/// admitted for sessions.
pub(crate) fn start_session(
    orch: &Orchestrator,
    provider: &str,
    backend: &dyn AgentBackend,
    config: SessionConfig,
) -> Result<(), String> {
    let project_id = Some(config.project_id.as_str()).filter(|id| !id.is_empty());
    let model = config.model.as_ref().map(|model| model.as_str());
    let admitted = admit(orch, provider, model, VerbKind::Session, project_id)
        .map_err(|refusal| refusal.to_string())?;
    backend.start_session(config, orch, admitted)
}

/// Run `request` on `backend`, which serves `provider`, once its model is
/// admitted for completions.
pub(crate) fn complete(
    orch: &Orchestrator,
    provider: &str,
    backend: &dyn AgentBackend,
    request: CompletionRequest,
) -> Result<CompletionOutcome, LaunchError> {
    let admitted = admit(
        orch,
        provider,
        Some(request.model.as_str()),
        VerbKind::Complete,
        request.project_id.as_deref(),
    )
    .map_err(LaunchError::Unserved)?;
    backend
        .complete(request, orch, admitted)
        .map_err(LaunchError::Completion)
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
    let Some(model) = model.map(str::trim).filter(|model| !model.is_empty()) else {
        return Ok(Admitted { _sealed: () });
    };
    orch.catalog_provider(provider, project_id)
        .admit(model, verb)?;
    Ok(Admitted { _sealed: () })
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
        ) -> Result<CompletionOutcome, CompletionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(CompletionError::BackendUnavailable)
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
        }
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
                Err(LaunchError::Completion(CompletionError::BackendUnavailable))
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
