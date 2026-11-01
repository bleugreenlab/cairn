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
recipes:
  - id: dsv4f-tp2
    source: bginf
    model: {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
    servedName: deepseek-v4-flash-vision-exp
    upstreamId: deepseek/deepseek-v4-flash-vision-exp
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 2}
    aliases: [big, vision, dsv4-flash]
  - id: qwen3-embed-4b
    source: bginf
    model: {name: Qwen/Qwen3-Embedding-4B-GGUF}
    servedName: qwen3-embedding-4b
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
    aliases: [embed]
  - id: qwen36-fast
    source: bginf
    model: {name: qwen36-fast}
    servedName: qwen36-fast
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
deployments:
  - {id: dsv4f-tp2, source: bginf, recipe: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, source: bginf, recipe: qwen3-embed-4b, machines: [halo], head: halo, apiPort: 8090, apiPath: /v1, healthPath: /health}
  - {id: qwen36-fast, source: bginf, recipe: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8000, apiPath: /v1, healthPath: /v1/models}
pools:
  - {id: sparks, source: bginf, machines: [{name: sp1, address: 10.20.20.40}, {name: sp2, address: 10.20.20.41}], default: [dsv4f-tp2]}
  - {id: halo, source: bginf, machines: [{name: halo, address: 10.20.20.43}], default: [qwen3-embed-4b]}
"#,
    )
    .unwrap();
    let declarations = BTreeMap::from([
        (
            "dsv4f-tp2".to_string(),
            cairn_config::inference::RecipeDeclaration {
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
            cairn_config::inference::RecipeDeclaration {
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

#[test]
fn deepseek_is_offered_by_served_name_and_alias_for_what_its_declaration_names() {
    let provider = provider();
    for name in [
        "deepseek-v4-flash-vision-exp",
        "dsv4-flash",
        "big",
        "vision",
    ] {
        for verb in [VerbKind::Complete, VerbKind::Session, VerbKind::Decide] {
            assert!(provider.admit(name, verb).is_ok(), "{name} {verb}");
        }
        assert!(provider.admit(name, VerbKind::Embed).is_err(), "{name}");
    }
    let alias = provider
        .listed(None)
        .catalog
        .unwrap()
        .models
        .into_iter()
        .find(|model| model.id == "dsv4-flash")
        .unwrap();
    assert_eq!(alias.context_window, Some(1_048_576));
    assert_eq!(
        alias
            .modalities
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("text+image → text")
    );
    assert_eq!(
        alias.canonical_slug.as_deref(),
        Some("deepseek/deepseek-v4-flash-vision-exp")
    );
    assert_eq!(
        alias.description.as_deref(),
        Some("deepseek-v4-flash-vision-exp on deployment dsv4f-tp2")
    );
}

#[test]
fn an_embedder_is_listed_and_serves_nothing_here_until_its_route_exists() {
    let provider = provider();
    for name in ["qwen3-embedding-4b", "embed"] {
        assert!(provider.serves_for(name).is_empty(), "{name}");
        let refusal = provider.admit(name, VerbKind::Session).unwrap_err();
        assert!(
            refusal.to_string().contains("serves nothing Cairn runs"),
            "{refusal}"
        );
    }
}

#[test]
fn an_undeclared_recipe_takes_the_providers_claim() {
    let provider = provider();
    let fast = served()
        .into_iter()
        .find(|model| model.id == "qwen36-fast")
        .unwrap();
    assert_eq!(serves_for(&fast), None);
    for verb in [VerbKind::Complete, VerbKind::Session, VerbKind::Decide] {
        assert!(provider.admit("qwen36-fast", verb).is_ok(), "{verb}");
    }
    // An unmeasured engine advertises no context.
    assert_eq!(discovered(&fast).context_window, None);
}

#[test]
fn a_recipe_that_outputs_no_text_and_names_no_verbs_serves_nothing() {
    let mut model = served()
        .into_iter()
        .find(|model| model.id == "qwen36-fast")
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
