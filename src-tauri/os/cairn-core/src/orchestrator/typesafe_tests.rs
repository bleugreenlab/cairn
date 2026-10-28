//! TypeSafe through the orchestrator: what its catalog offers with and
//! without a key, where a decision is sent, how an over-long request is
//! refused, and that nothing but `decide` launches.

use std::sync::{Arc, Mutex};

use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, CompletionWire,
};
use cairn_models::interface::{ModelScope, Models, OfferQuery};
use cairn_models::types::catalog::{ProviderModelCatalog, VerbKind, Wire};
use serde_json::{json, Value};

use super::decide::{DecideRequest, DecisionFailure};
use super::models::ModelRuntime;
use super::Orchestrator;
use crate::backends::typesafe::{assemble, CONTEXT_LIMITS, TYPESAFE_BACKEND_KEY};
use crate::backends::{backend_for_name, launch};
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::models::decision::MeasuredBy;
use crate::models::UpdateSettings;

const KEY: &str = "ts-test-key";

/// TypeSafe's documented `GET /v1/models` reply.
const LISTING: &str = crate::backends::typesafe::tests::LISTING;

/// One System One request as it left: its URL, headers, the endpoint's
/// stated context limits, and body.
type SentRequest = (String, Vec<(String, String)>, Option<String>, Value);

/// Records where each System One request went, then answers it from the
/// script: TypeSafe's documented reply, or a refusal of an over-long request.
struct TypeSafeServer {
    sent: Mutex<Vec<SentRequest>>,
    too_long: bool,
}

impl Complete for TypeSafeServer {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        let CompletionWire::SystemOne(endpoint) = call.wire else {
            panic!("expected System One, got {}", call.wire.family());
        };
        self.sent.lock().unwrap().push((
            endpoint.url.clone(),
            endpoint.headers.clone(),
            endpoint.context_limits.clone(),
            call.body,
        ));
        if self.too_long {
            // What the System One wire makes of TypeSafe's 400.
            return Err(CompletionError::ContextExceeded(format!(
                "TypeSafe refused the decision as longer than the model's context (max_tokens_exceeded). {}",
                endpoint.context_limits.as_deref().unwrap_or_default()
            )));
        }
        Ok(CompletionReply::SystemOne(
            serde_json::from_value(json!({
                "model": "jev-1.14.0",
                "answers": {
                    "is_bug": {"type": "noul", "noul": 0.93},
                    "team": {"type": "choice", "choice": "payments", "confidence": 0.71,
                             "probabilities": {"account": 0.02, "frontend": 0.17, "payments": 0.81}}
                },
                "usage": {"input_tokens": 212, "output_tokens": 9}
            }))
            .unwrap(),
        ))
    }
}

/// A workspace with TypeSafe installed, its key when `keyed`, and the catalog
/// a refresh would have kept.
async fn workspace(keyed: bool, too_long: bool) -> (Orchestrator, Arc<TypeSafeServer>) {
    let mut orch = super::models::tests::bare_orchestrator().await;
    let server = Arc::new(TypeSafeServer {
        sent: Mutex::new(Vec::new()),
        too_long,
    });
    orch.models_runtime = ModelRuntime {
        complete: server.clone(),
        ..orch.models_runtime.clone()
    };
    let mut enabled = orch.enabled_providers();
    enabled.push(TYPESAFE_BACKEND_KEY.to_string());
    orch.update_settings(UpdateSettings {
        enabled_providers: Some(enabled),
        ..Default::default()
    })
    .unwrap();
    if keyed {
        orch.add_account(
            ApiProvider::TypeSafe,
            "TypeSafe API key".into(),
            ProviderAuth::ApiKey { value: KEY.into() },
            None,
        )
        .unwrap();
    }
    let (models, error) = assemble(keyed.then(|| Ok(LISTING.to_string())));
    orch.seed_model_catalog_for_test(ProviderModelCatalog {
        backend: TYPESAFE_BACKEND_KEY.into(),
        models,
        options: Vec::new(),
        refreshed_at: Some(1_790_000_000),
        error,
    });
    (orch, server)
}

fn request(model: &str) -> DecideRequest {
    serde_json::from_value(json!({
        "model": model,
        "input": "My checkout page shows a blank screen after I click Pay.",
        "questions": [
            {"name": "is_bug", "type": "predicate", "instructions": "Is this a defect?"},
            {"name": "team", "type": "choice", "instructions": "Which team?",
             "options": [{"value": "account"}, {"value": "frontend"}, {"value": "payments"}]}
        ]
    }))
    .unwrap()
}

async fn read(orch: &Orchestrator, uri: &str) -> String {
    let request = crate::mcp::types::McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "read".to_string(),
        payload: json!({}),
        tool_use_id: None,
    };
    crate::resources::read_cairn_resource(orch, &request, uri).await
}

/// With a key, the four ids are offered for `decide` and listed with their
/// price, and `typesafe/jev-1.14.0` answers over System One at
/// `api.typesafe.ai/v1/systemone` on the workspace key, without the
/// long-context header, with typed answers and confidence.
#[tokio::test(flavor = "multi_thread")]
async fn a_typesafe_key_offers_jev_for_decide_and_answers_over_system_one() {
    let (orch, server) = workspace(true, false).await;

    let catalog = read(&orch, "cairn://models/catalog?provider=typesafe").await;
    for id in ["jev-1.14.0", "jev-1.13.0", "jev-latest", "jev-preview"] {
        assert!(catalog.contains(id), "{id}: {catalog}");
    }
    assert!(catalog.contains("in $0.042/M"), "{catalog}");
    let page = read(&orch, "cairn://models/decide").await;
    assert!(
        page.contains("`jev-1.14.0` on `typesafe` — measured by native"),
        "{page}"
    );

    let outcome = orch
        .decide_named(None, request("typesafe/jev-1.14.0"))
        .await
        .unwrap();
    assert_eq!(outcome.offer.provider, TYPESAFE_BACKEND_KEY);
    assert_eq!(outcome.offer.model, "jev-1.14.0");
    assert_eq!(outcome.offer.measured_by, MeasuredBy::Native);
    assert_eq!(outcome.offer.wire, Wire::SystemOne);
    let picks: Vec<_> = outcome
        .decision
        .answers
        .iter()
        .map(|answer| (answer.pick().unwrap(), answer.confidence().is_some()))
        .collect();
    assert_eq!(picks, vec![("yes", true), ("payments", true)]);
    let body = serde_json::to_value(&outcome).unwrap();
    assert_eq!(body["inputTokens"], 212);

    let sent = server.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1, "a native offer is never probed");
    let (url, headers, limits, sent_body) = &sent[0];
    assert_eq!(url, "https://api.typesafe.ai/v1/systemone");
    assert!(headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("authorization")
            && value == "Bearer ts-test-key"));
    assert!(
        !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("TypeSafe-Danger-Zone-Long-Context")),
        "the long-context header is never sent by default"
    );
    assert_eq!(limits.as_deref(), Some(CONTEXT_LIMITS));
    assert_eq!(sent_body["model"], "jev-1.14.0");
    assert_eq!(sent_body["questions"]["is_bug"]["type"], "noul");
}

/// A request longer than Jev's context is refused as unanswerable as asked,
/// naming both limits and the header that lifts them, and the model stays
/// offered: it still answers shorter requests.
#[tokio::test(flavor = "multi_thread")]
async fn an_over_long_request_is_unanswerable_naming_the_limits_and_the_header() {
    let (orch, _) = workspace(true, true).await;
    let failure = orch
        .decide_named(None, request("typesafe/jev-latest"))
        .await
        .unwrap_err();
    let DecisionFailure::Unanswerable(text) = failure else {
        panic!("an over-long request is not a transient failure: {failure:?}");
    };
    for part in [
        "64k tokens per request",
        "32k per question",
        "TypeSafe-Danger-Zone-Long-Context: true",
    ] {
        assert!(text.contains(part), "{part}: {text}");
    }
    let catalog = orch.catalog(&ModelScope::default()).unwrap();
    assert!(catalog
        .offer_for(OfferQuery {
            requested: Some("typesafe/jev-latest"),
            ..OfferQuery::new(VerbKind::Decide)
        })
        .is_ok());
}

/// Without a key TypeSafe offers nothing, even for an id it would accept.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_key_typesafe_offers_nothing() {
    let (orch, server) = workspace(false, false).await;
    let catalog = orch.catalog(&ModelScope::default()).unwrap();
    let typesafe = catalog
        .providers
        .iter()
        .find(|provider| provider.descriptor.key == TYPESAFE_BACKEND_KEY)
        .unwrap();
    assert!(typesafe.installed);
    assert_eq!(
        typesafe.descriptor.setup_requirement,
        "Add a TypeSafe API key."
    );
    assert!(typesafe.serves.is_empty());
    for model in ["jev-1.14.0", "typesafe/jev-1.14.0"] {
        assert!(
            catalog
                .offer_for(OfferQuery {
                    requested: Some(model),
                    ..OfferQuery::new(VerbKind::Decide)
                })
                .is_err(),
            "{model}"
        );
    }
    assert!(orch
        .decide_named(None, request("typesafe/jev-1.14.0"))
        .await
        .is_err());
    assert!(server.sent.lock().unwrap().is_empty());
}

/// TypeSafe serves no agent turn and no completion: both launch doors refuse
/// before the backend is reached, and the backend itself says why.
#[tokio::test(flavor = "multi_thread")]
async fn sessions_and_completions_are_refused() {
    let (orch, server) = workspace(true, false).await;
    let backend = backend_for_name(Some(TYPESAFE_BACKEND_KEY));
    let config = super::launch_coverage_tests::config(
        "run-typesafe",
        "session-typesafe",
        "jev-1.14.0",
        false,
    );
    let refused =
        launch::start_session(&orch, TYPESAFE_BACKEND_KEY, backend.as_ref(), config).unwrap_err();
    assert!(refused.contains("jev-1.14.0"), "{refused}");
    assert!(backend
        .response_completion_availability(&orch, None)
        .unwrap_err()
        .contains("decisions only"));
    assert!(server.sent.lock().unwrap().is_empty());
}
