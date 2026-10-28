use cairn_models::types::catalog::{DiscoveredModel, VerbKind, Wire};
use cairn_models::types::usage::{BillingUnit, Charge};

use super::{assemble, declared_models};

/// TypeSafe's documented `GET /v1/models` reply, with a second alias this
/// build does not declare.
pub(crate) const LISTING: &str = r#"{"models": [
    {"description": "General-purpose system one model.", "name": "jev-latest", "release_date": "2026-09-15"},
    {"description": "Next.", "name": "jev-next", "release_date": "2026-11-01"}
]}"#;

fn ids(models: &[DiscoveredModel]) -> Vec<&str> {
    models.iter().map(|model| model.id.as_str()).collect()
}

/// The four ids are declared, each serving `decide` over System One and
/// nothing else, at TypeSafe's list price: $0.042 per million input tokens,
/// output free, read with the 64k per-request window.
#[test]
fn the_declared_models_serve_decide_over_system_one_at_the_list_price() {
    let models = declared_models();
    assert_eq!(
        ids(&models),
        ["jev-1.14.0", "jev-1.13.0", "jev-latest", "jev-preview"]
    );
    for model in &models {
        let serves = model.serves.as_ref().unwrap();
        assert_eq!(serves.len(), 1, "{}", model.id);
        assert_eq!(serves[0].verb.kind(), VerbKind::Decide);
        assert_eq!(serves[0].wire, Wire::SystemOne);
        let pricing = model.pricing.as_ref().unwrap();
        assert_eq!(
            pricing.usd_per(Charge::Input, None, BillingUnit::Tokens),
            Some("0.000000042")
        );
        assert_eq!(
            pricing.usd_per(Charge::Output, None, BillingUnit::Tokens),
            Some("0")
        );
        assert_eq!(model.context_window, Some(64_000));
    }
}

#[test]
fn without_a_key_nothing_is_declared() {
    assert_eq!(assemble(None), (Vec::new(), None));
}

/// The listing confirms the key and adds the alias this build does not know,
/// unpriced, beside the declared ones; a declared alias it also lists is not
/// repeated.
#[test]
fn the_listing_adds_an_unknown_alias_beside_the_declared_ids() {
    let (models, error) = assemble(Some(Ok(LISTING.to_string())));
    assert_eq!(error, None);
    assert_eq!(
        ids(&models),
        [
            "jev-1.14.0",
            "jev-1.13.0",
            "jev-latest",
            "jev-preview",
            "jev-next"
        ]
    );
    let next = models.last().unwrap();
    assert_eq!(next.serves.as_ref().unwrap()[0].wire, Wire::SystemOne);
    assert_eq!(next.pricing, None, "an undeclared alias claims no price");
    assert_eq!(next.description.as_deref(), Some("Next."));
}

/// A rejected key or an unreadable listing keeps the declared models and
/// says why.
#[test]
fn a_failed_listing_keeps_the_declared_models_and_says_why() {
    let (models, error) = assemble(Some(Err(
        "TypeSafe rejected the API key (HTTP 401): bad key".to_string(),
    )));
    assert_eq!(models.len(), 4);
    assert_eq!(
        error.as_deref(),
        Some("TypeSafe rejected the API key (HTTP 401): bad key")
    );

    let (models, error) = assemble(Some(Ok("<html>".to_string())));
    assert_eq!(models.len(), 4);
    assert!(error
        .unwrap()
        .starts_with("unreadable TypeSafe model listing"));
}

/// With `TYPESAFE_API_KEY` set, reads the live listing through discovery and
/// asks `jev-1.14.0` one predicate, one choice and one score question at
/// TypeSafe's own endpoint through the facet's real `HttpComplete`, printing
/// the catalog, the typed answers and the usage the reply reports. Run with
/// `--ignored --nocapture`.
#[test]
#[ignore = "reaches api.typesafe.ai and spends from the key"]
fn live_listing_and_one_decision() {
    use crate::models::decision::{DecisionOption, DecisionQuestion, QuestionKind, ScoreLevel};
    use cairn_models::interface::decide::{DecisionCall, DecisionRoute};

    let api_key = std::env::var("TYPESAFE_API_KEY").expect("TYPESAFE_API_KEY");
    let (models, error) = assemble(Some(super::list_models(&api_key)));
    println!("listing error: {error:?}");
    for model in &models {
        println!(
            "  {}: in {:?}",
            model.id,
            model.pricing.as_ref().and_then(|p| p.usd_per(
                Charge::Input,
                None,
                BillingUnit::Tokens
            ))
        );
    }
    assert_eq!(error, None, "the key is accepted");

    let route = DecisionRoute::SystemOne(super::decision_endpoint(&api_key));
    let option = |value: &str, description: &str| DecisionOption {
        value: value.into(),
        description: Some(description.into()),
    };
    let questions = vec![
        DecisionQuestion {
            name: "is_bug".into(),
            instructions: "Is the customer reporting a software defect?".into(),
            kind: QuestionKind::Predicate,
        },
        DecisionQuestion {
            name: "team".into(),
            instructions: "Which team should own this ticket?".into(),
            kind: QuestionKind::Choice {
                options: vec![
                    option("account", "Login, permissions, or profile issues."),
                    option(
                        "frontend",
                        "Rendering, layout, or browser compatibility issues.",
                    ),
                    option(
                        "payments",
                        "Checkout, billing, or payment processing issues.",
                    ),
                ],
            },
        },
        DecisionQuestion {
            name: "urgency".into(),
            instructions: "How urgent is this ticket?".into(),
            kind: QuestionKind::Score {
                levels: [
                    "Can wait for the next release",
                    "Should be fixed this week",
                    "Blocking revenue right now",
                ]
                .into_iter()
                .map(|label| ScoreLevel {
                    label: label.into(),
                    description: None,
                })
                .collect(),
            },
        },
    ];
    let decision = cairn_models::decide::decide(
        &cairn_models::complete::HttpComplete,
        DecisionCall {
            route: &route,
            model: "jev-1.14.0",
            input: "My checkout page shows a blank screen after I click Pay. I have tried two browsers. We are an enterprise customer.",
            questions: &questions,
            timeout: std::time::Duration::from_secs(30),
        },
    )
    .unwrap();
    println!("{}", serde_json::to_string_pretty(&decision).unwrap());
    assert!(decision
        .answers
        .iter()
        .all(|answer| answer.pick().is_some() && answer.confidence().is_some()));
}
