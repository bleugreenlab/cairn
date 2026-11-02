use std::collections::{BTreeMap, BTreeSet};

use cairn_inference::served::served_models;
use cairn_inference::{EngineMeasure, Inventory};
use cairn_models::interface::CatalogProvider;
use cairn_models::types::catalog::{
    descriptor, Modalities, Modality, ProviderModelCatalog, Verb, VerbKind,
};

use super::*;

/// The Sparks and halo as adopted from bginf, with DeepSeek declared as the
/// operator declares it and the embedder declared an embedder.
fn inventory() -> Inventory {
    let mut inventory = Inventory::from_yaml(
        r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
  - {name: Qwen/Qwen3-Embedding-4B}
  - {name: Qwen/Qwen3.6-35B-A3B}
profiles:
  - id: dsv4f-tp2
    source: bginf
    model: {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
    servedName: deepseek-v4-flash-vision-exp
    upstreamId: deepseek/deepseek-v4-flash-vision-exp
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 2}
  - id: qwen3-embed-4b
    source: bginf
    model: {name: Qwen/Qwen3-Embedding-4B}
    servedName: qwen3-embedding-4b
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
  - id: qwen36-fast
    source: bginf
    model: {name: Qwen/Qwen3.6-35B-A3B}
    servedName: qwen36-fast
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
deployments:
  - {id: dsv4f-tp2, source: bginf, profile: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, source: bginf, profile: qwen3-embed-4b, machines: [halo], head: halo, apiPort: 8090, apiPath: /v1, healthPath: /health}
  - {id: qwen36-fast, source: bginf, profile: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8000, apiPath: /v1, healthPath: /v1/models}
machines:
  - {name: sp1, source: bginf, address: 10.20.20.40}
  - {name: sp2, source: bginf, address: 10.20.20.41}
  - {name: halo, source: bginf, address: 10.20.20.43}
"#,
    )
    .unwrap();
    let declarations = BTreeMap::from([
        (
            "dsv4f-tp2".to_string(),
            cairn_config::inference::InferenceProfileDeclaration {
                verbs: Some(BTreeSet::from([
                    VerbKind::Complete,
                    VerbKind::Session,
                    VerbKind::Decide,
                ])),
                input_modalities: Some(vec![Modality::Text, Modality::Image]),
                output_modalities: Some(vec![Modality::Text]),
            },
        ),
        (
            "qwen3-embed-4b".to_string(),
            cairn_config::inference::InferenceProfileDeclaration {
                verbs: Some(BTreeSet::from([VerbKind::Embed])),
                ..Default::default()
            },
        ),
    ]);
    crate::orchestrator::inference_route::declare(&mut inventory, &declarations);
    inventory
}

fn served() -> Vec<ServedModel> {
    let measures = BTreeMap::from([(
        "dsv4f-tp2".into(),
        EngineMeasure {
            max_model_len: Some(1_048_576),
            kv_cache_tokens: Some(1_447_093),
        },
    )]);
    served_models(&inventory(), &measures)
}

/// This provider as the catalog holds it after a refresh.
fn provider() -> CatalogProvider {
    CatalogProvider {
        descriptor: descriptor(INFERENCE_BACKEND_KEY).unwrap().clone(),
        installed: true,
        models: Some(ProviderModelCatalog {
            backend: INFERENCE_BACKEND_KEY.to_string(),
            models: served().iter().map(discovered).collect(),
            options: vec![],
            refreshed_at: None,
            error: None,
        }),
        tiers: vec![],
        serves: provider_serves(),
        unable: vec![],
    }
}

const DSV4: &str = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp";

#[test]
fn deepseek_is_offered_by_its_real_name_for_what_its_declaration_names() {
    let provider = provider();
    for verb in [VerbKind::Complete, VerbKind::Session, VerbKind::Decide] {
        assert!(provider.admit(DSV4, verb).is_ok(), "{verb}");
    }
    assert!(provider.admit(DSV4, VerbKind::Embed).is_err());
    let listed = provider.listed(None).catalog.unwrap().models;
    assert_eq!(
        listed.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        [DSV4, "Qwen/Qwen3-Embedding-4B", "Qwen/Qwen3.6-35B-A3B"]
    );
    let deepseek = listed.into_iter().find(|model| model.id == DSV4).unwrap();
    assert_eq!(deepseek.display_name, DSV4);
    assert_eq!(deepseek.context_window, Some(1_048_576));
    assert_eq!(
        deepseek
            .modalities
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("text+image → text")
    );
    assert_eq!(
        deepseek.canonical_slug.as_deref(),
        Some("deepseek/deepseek-v4-flash-vision-exp")
    );
}

#[test]
fn no_old_alias_served_name_or_id_names_a_model() {
    let inventory = inventory();
    let listed: Vec<_> = served().into_iter().map(|model| model.id).collect();
    for name in [
        "dsv4-flash",
        "big",
        "vision",
        "deepseek-v4-flash-vision-exp",
        "dsv4f-tp2",
    ] {
        assert!(!listed.iter().any(|id| id == name), "{name}");
        assert!(
            crate::orchestrator::inference_route::resolve_model(&inventory, name).is_err(),
            "{name}"
        );
    }
}

#[test]
fn an_embedder_is_listed_and_serves_nothing_here_until_its_route_exists() {
    let provider = provider();
    let name = "Qwen/Qwen3-Embedding-4B";
    assert!(provider.serves_for(name).is_empty());
    let refusal = provider.admit(name, VerbKind::Session).unwrap_err();
    assert!(
        refusal.to_string().contains("serves nothing Cairn runs"),
        "{refusal}"
    );
}

#[test]
fn an_undeclared_profile_takes_the_providers_claim() {
    let provider = provider();
    let fast = served()
        .into_iter()
        .find(|model| model.id == "Qwen/Qwen3.6-35B-A3B")
        .unwrap();
    assert_eq!(serves_for(&fast), None);
    for verb in [VerbKind::Complete, VerbKind::Session, VerbKind::Decide] {
        assert!(
            provider.admit("Qwen/Qwen3.6-35B-A3B", verb).is_ok(),
            "{verb}"
        );
    }
    // An unmeasured engine advertises no context.
    assert_eq!(discovered(&fast).context_window, None);
}

#[test]
fn a_profile_that_outputs_no_text_and_names_no_verbs_serves_nothing() {
    let mut model = served()
        .into_iter()
        .find(|model| model.id == "Qwen/Qwen3.6-35B-A3B")
        .unwrap();
    model.modalities = Modalities {
        input: vec![Modality::Text],
        output: vec![Modality::Audio],
    };
    assert_eq!(serves_for(&model), Some(vec![]));
}

#[test]
fn the_provider_carries_conversation_and_decide_over_openai_chat() {
    let kinds: Vec<_> = provider_serves()
        .iter()
        .map(|served| (served.verb.kind(), served.wire))
        .collect();
    assert_eq!(
        kinds,
        [
            (VerbKind::Complete, Wire::OpenAiChatCompletions),
            (VerbKind::Session, Wire::OpenAiChatCompletions),
            (VerbKind::Decide, Wire::OpenAiChatCompletions),
        ]
    );
    assert!(provider_serves()
        .iter()
        .all(|served| !matches!(served.verb, Verb::Embed(_))));
}
