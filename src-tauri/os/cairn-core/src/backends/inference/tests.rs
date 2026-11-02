use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use cairn_inference::served::served_models;
use cairn_inference::{EngineMeasure, Inventory};
use cairn_models::interface::embed::{EmbedCall, EmbedRequest, Purpose, Width};
use cairn_models::interface::CatalogProvider;
use cairn_models::types::catalog::{descriptor, Modality, ProviderModelCatalog, Verb, VerbKind};
use cairn_models::types::embedding::{EmbeddingWidths, PurposeHandling};
use serde_json::json;

use super::*;
use crate::fleet::models::RequestHandle;
use crate::orchestrator::embed::tests::Scripted;
use crate::orchestrator::inference_route::tests::waited_for;
use crate::orchestrator::inference_route::{Asked, Route};

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
    provider_of(&served())
}

/// This provider as the catalog holds it after a refresh that found `served`.
fn provider_of(served: &[ServedModel]) -> CatalogProvider {
    CatalogProvider {
        descriptor: descriptor(INFERENCE_BACKEND_KEY).unwrap().clone(),
        installed: true,
        models: Some(ProviderModelCatalog {
            backend: INFERENCE_BACKEND_KEY.to_string(),
            models: served.iter().map(discovered).collect(),
            options: vec![],
            refreshed_at: None,
            error: None,
        }),
        tiers: vec![],
        serves: provider_serves(),
        unable: unable(served),
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

const EMBEDDER: &str = "Qwen/Qwen3-Embedding-4B";

fn embed_capabilities(served: &ServedVerb) -> &EmbeddingCapabilities {
    match &served.verb {
        Verb::Embed(capabilities) => capabilities,
        other => panic!("not an embed offer: {other:?}"),
    }
}

#[test]
fn a_declared_embedder_is_admitted_for_embed_and_only_embed() {
    let provider = provider();
    let served = provider.admit(EMBEDDER, VerbKind::Embed).unwrap();
    assert_eq!(served.wire, Wire::OpenAiEmbeddings);
    // Its family's facts: Qwen3-Embedding-4B is 2560 wide and instructs its
    // queries. Native width only: the engine's honouring `dimensions` is not
    // on the record. A local server's batch limits; the engine reported no
    // context, so no per-text token limit.
    let capabilities = embed_capabilities(&served);
    assert_eq!(capabilities.native_dims, Some(2560));
    assert_eq!(capabilities.widths, EmbeddingWidths::NativeOnly);
    assert!(
        matches!(&capabilities.purpose, PurposeHandling::Prefix { document, query }
            if document.is_empty() && query.starts_with("Instruct: ")),
        "{:?}",
        capabilities.purpose
    );
    assert_eq!(capabilities.limits, local_server_limits(None));
    assert_eq!(provider.serves_for(EMBEDDER), vec![served]);
    for verb in [VerbKind::Complete, VerbKind::Session, VerbKind::Decide] {
        let refusal = provider.admit(EMBEDDER, verb).unwrap_err();
        assert!(
            refusal.to_string().contains("serves only embed"),
            "{verb}: {refusal}"
        );
    }
    // `?serves=embed` lists it under its real name, and nothing else here.
    let listed = provider
        .listed(Some(VerbKind::Embed))
        .catalog
        .unwrap()
        .models;
    assert_eq!(
        listed.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        [EMBEDDER]
    );
    assert_eq!(
        listed[0]
            .modalities
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("text → embeddings")
    );
}

fn embedder() -> ServedModel {
    served()
        .into_iter()
        .find(|model| model.id == EMBEDDER)
        .unwrap()
}

fn offered_embedding(model: &ServedModel) -> EmbeddingCapabilities {
    embedding_offer(model)
        .expect("a deployment embeds")
        .expect("its embedders agree")
}

#[test]
fn an_embedder_s_measured_context_is_its_per_text_token_limit() {
    let mut embedder = embedder();
    embedder.deployments[0].context_length = Some(8_192);
    assert_eq!(
        offered_embedding(&embedder).limits,
        local_server_limits(Some(8_192))
    );
}

#[test]
fn a_model_the_family_table_does_not_know_embeds_with_no_prefix_at_its_measured_width() {
    let mut model = served()
        .into_iter()
        .find(|model| model.id == "Qwen/Qwen3.6-35B-A3B")
        .unwrap();
    model.deployments[0].verbs = BTreeSet::from([VerbKind::Embed]);
    let serves = serves_for(&model);
    assert_eq!(
        serves
            .iter()
            .map(|served| (served.verb.kind(), served.wire))
            .collect::<Vec<_>>(),
        [(VerbKind::Embed, Wire::OpenAiEmbeddings)]
    );
    assert_eq!(embed_capabilities(&serves[0]).native_dims, None);
    assert_eq!(
        embed_capabilities(&serves[0]).purpose,
        PurposeHandling::Ignored
    );
}

#[test]
fn an_embedder_unknown_by_its_real_name_is_looked_up_by_its_upstream_id() {
    let mut embedder = embedder();
    embedder.id = "lab/house-embedder".into();
    embedder.deployments[0].upstream_id = Some("qwen/qwen3-embedding-4b".into());
    assert_eq!(offered_embedding(&embedder).native_dims, Some(2560));
}

/// The embedder deployed twice, on the halo and on bbs, each pinning one
/// Q8_0 file with the given content hash; bbs's engine measured 8k.
fn embedder_twice(halo_sha: &str, bbs_sha: &str) -> ServedModel {
    let pin = |sha: &str| {
        vec![cairn_inference::profile::WeightFile {
            repo: "Qwen/Qwen3-Embedding-4B-GGUF".into(),
            revision: "f4602530db1d980e16da9d7d3a70294cf5c190be".into(),
            path: "Qwen3-Embedding-4B-Q8_0.gguf".into(),
            sha256: sha.into(),
            size: None,
        }]
    };
    let mut model = embedder();
    model.deployments[0].quantization = Some("q8_0".into());
    model.deployments[0].weights = pin(halo_sha);
    let mut bbs = model.deployments[0].clone();
    bbs.deployment = "qwen3-embed-4b-bbs".into();
    bbs.served_name = EMBEDDER.into();
    bbs.weights = pin(bbs_sha);
    bbs.context_length = Some(8_192);
    model.deployments.push(bbs);
    model
}

#[test]
fn embedders_of_the_same_weights_share_one_offer_within_the_smallest_context() {
    let model = embedder_twice("b60ae5ce", "b60ae5ce");
    let capabilities = offered_embedding(&model);
    assert_eq!(capabilities.native_dims, Some(2560));
    assert_eq!(capabilities.limits, local_server_limits(Some(8_192)));
    assert!(unable(&[model]).is_empty());
}

/// Two embedders of one model that are not shown to load the same weights
/// would key vectors of different weights under one id, so the model is not
/// offered for embed, and the refusal says why.
#[test]
fn embedders_of_different_weights_are_refused_embed_by_name() {
    let model = embedder_twice("b60ae5ce", "0000ffff");
    assert!(serves_for(&model).is_empty());
    let provider = provider_of(&[model]);
    let refusal = provider.admit(EMBEDDER, VerbKind::Embed).unwrap_err();
    let detail = refusal.detail.unwrap_or_default();
    assert!(
        detail.contains(
            "qwen3-embed-4b and qwen3-embed-4b-bbs are not shown to load the same weights"
        ),
        "{detail}"
    );
    // A deployment that pins nothing is never shown to match.
    let mut unpinned = embedder_twice("b60ae5ce", "b60ae5ce");
    unpinned.deployments[0].weights.clear();
    assert!(matches!(embedding_offer(&unpinned), Some(Err(_))));
}

/// A model with a chat deployment and an embedding deployment serves both,
/// each verb from the deployment that serves it.
#[test]
fn a_model_with_a_chat_and_an_embedding_deployment_serves_the_union() {
    let mut model = embedder();
    let mut chat = model.deployments[0].clone();
    chat.deployment = "qwen3-embed-4b-chat".into();
    chat.verbs = BTreeSet::from([VerbKind::Complete, VerbKind::Session, VerbKind::Decide]);
    chat.context_length = Some(32_768);
    model.deployments.push(chat);
    assert_eq!(
        serves_for(&model)
            .iter()
            .map(|served| served.verb.kind())
            .collect::<Vec<_>>(),
        [
            VerbKind::Complete,
            VerbKind::Session,
            VerbKind::Decide,
            VerbKind::Embed
        ]
    );
    // The embedder measured nothing, so the chat deployment's context is
    // not the embedder's limit.
    assert_eq!(offered_embedding(&model).limits, local_server_limits(None));
    assert_eq!(discovered(&model).context_window, Some(32_768));
}

/// The embedder's deployment as its head serves it once healthy.
fn embedder_ready() -> Route {
    Route {
        deployment: "qwen3-embed-4b".into(),
        pool: "halo".into(),
        api_base: "http://10.20.20.43:8090/v1".into(),
        served_name: "qwen3-embedding-4b".into(),
        context_length: None,
    }
}

/// An embedding of the unloaded embedder asks for it, waits while it loads
/// until its deployment is healthy, and then goes to the deployment's
/// embeddings endpoint under the engine's own name, with the purpose prefix
/// and width its offer declares.
#[test]
fn an_embedding_loads_its_model_waits_for_health_and_is_sent_under_the_served_name() {
    let handle = RequestHandle {
        id: 9,
        pool: "halo".into(),
        deployment: "qwen3-embed-4b".into(),
        model: EMBEDDER.into(),
    };
    let waiting = |said: &str| Asked::Waiting {
        handle: handle.clone(),
        said: said.into(),
    };
    let heard = RefCell::new(Vec::new());
    let mut polls = 0;
    let route = waited_for(waiting("loading, up to 2m"), &heard, |_| {
        polls += 1;
        Ok(if polls < 3 {
            waiting("loading, up to 2m")
        } else {
            Asked::Ready(embedder_ready())
        })
    })
    .unwrap();
    assert_eq!(*heard.borrow(), ["loading, up to 2m"]);

    let (endpoint, sent) = embedding_target(route);
    let EmbedRoute::OpenAi(target) = &endpoint else {
        panic!("an OpenAI embeddings route")
    };
    assert_eq!(target.url, "http://10.20.20.43:8090/v1/embeddings");
    assert!(target.headers.is_empty(), "the engine takes no credential");
    assert_eq!(sent, "qwen3-embedding-4b");

    let offer = provider().admit(EMBEDDER, VerbKind::Embed).unwrap();
    let server = Scripted::answering(vec![json!({
        "model": "qwen3-embedding-4b",
        "data": [{"index": 0, "embedding": vec![0.25_f32; 2560]}],
        "usage": {"prompt_tokens": 21}
    })]);
    let embeddings = cairn_models::embed::embed(
        &server,
        EmbedCall {
            route: &endpoint,
            model: &sent,
            capabilities: embed_capabilities(&offer),
            request: &EmbedRequest {
                texts: vec!["what is cairn".into()],
                purpose: Purpose::Query,
                width: Width::Native,
            },
            timeout: EMBED_TIMEOUT,
            cancel: None,
        },
    )
    .unwrap();
    let body = &server.bodies()[0];
    assert_eq!(body["model"], "qwen3-embedding-4b");
    assert!(
        body["input"][0].as_str().unwrap().starts_with("Instruct: "),
        "{body}"
    );
    assert!(body.get("dimensions").is_none(), "native width sends none");
    assert_eq!(
        (embeddings.provider.as_str(), embeddings.dims),
        (INFERENCE_BACKEND_KEY, 2560)
    );
}

/// What a loaded embedder answers through this provider's offer and route,
/// over the production transport: its width, and a request over the offer's
/// batch limit split and joined in order. Load it first (`write
/// cairn://inference/requests {model: "Qwen/Qwen3-Embedding-4B", pool:
/// "bbs"}`), then run with `--ignored --nocapture`. `CAIRN_LIVE_EMBED_BASE` and
/// `CAIRN_LIVE_EMBED_SERVED` name another deployment's API base and served name.
#[test]
#[ignore = "reaches a loaded embedder on the lab network"]
fn live_embedder_answers_at_the_offers_width() {
    let api_base = std::env::var("CAIRN_LIVE_EMBED_BASE")
        .unwrap_or_else(|_| "http://10.10.10.10:8090/v1".into());
    let served_name = std::env::var("CAIRN_LIVE_EMBED_SERVED").unwrap_or_else(|_| EMBEDDER.into());
    let (endpoint, sent) = embedding_target(Route {
        deployment: "live".into(),
        pool: "live".into(),
        api_base,
        served_name,
        context_length: None,
    });
    let offer = provider().admit(EMBEDDER, VerbKind::Embed).unwrap();
    let capabilities = embed_capabilities(&offer);
    println!("offer: {}", capabilities.summary());
    let complete = cairn_models::complete::HttpComplete;
    let embed = |texts: Vec<String>, purpose| {
        let started = std::time::Instant::now();
        let embeddings = cairn_models::embed::embed(
            &complete,
            EmbedCall {
                route: &endpoint,
                model: &sent,
                capabilities,
                request: &EmbedRequest {
                    texts,
                    purpose,
                    width: Width::Native,
                },
                timeout: EMBED_TIMEOUT,
                cancel: None,
            },
        )
        .unwrap();
        (embeddings, started.elapsed())
    };
    let documents = vec![
        "Cairn orchestrates agents across a workspace of projects and machines.".to_string(),
        "The recipe calls for two cups of flour and a pinch of salt.".to_string(),
    ];
    let (docs, took) = embed(documents, Purpose::Document);
    println!(
        "documents: {} x {} in {took:?}, usage {:?}",
        docs.vectors.len(),
        docs.dims,
        docs.usage
    );
    let (query, _) = embed(vec!["what runs agents?".to_string()], Purpose::Query);
    let cosine = |a: &[f32], b: &[f32]| {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
        dot / (norm(a) * norm(b))
    };
    let (near, far) = (
        cosine(&query.vectors[0], &docs.vectors[0]),
        cosine(&query.vectors[0], &docs.vectors[1]),
    );
    println!("query against the agent sentence {near:.3}, against the recipe {far:.3}");
    assert_eq!((docs.dims, query.dims), (2560, 2560));
    assert!(
        near > far,
        "the instructed query finds the relevant document"
    );

    let many: Vec<String> = (0..40)
        .map(|index| format!("text number {index}"))
        .collect();
    let (split, took) = embed(many, Purpose::Document);
    println!(
        "40 texts: {} x {} in {took:?}",
        split.vectors.len(),
        split.dims
    );
    assert_eq!((split.vectors.len(), split.dims), (40, 2560));
}

/// An engine answering at another width than the offer declares has every
/// vector refused, never stored under the declared width.
#[test]
fn an_embedder_answering_at_another_width_is_refused() {
    let (endpoint, sent) = embedding_target(embedder_ready());
    let offer = provider().admit(EMBEDDER, VerbKind::Embed).unwrap();
    let server = Scripted::answering(vec![json!({
        "data": [{"index": 0, "embedding": vec![0.25_f32; 1024]}]
    })]);
    let error = cairn_models::embed::embed(
        &server,
        EmbedCall {
            route: &endpoint,
            model: &sent,
            capabilities: embed_capabilities(&offer),
            request: &EmbedRequest {
                texts: vec!["x".into()],
                purpose: Purpose::Document,
                width: Width::Native,
            },
            timeout: EMBED_TIMEOUT,
            cancel: None,
        },
    )
    .unwrap_err();
    assert_eq!(
        error,
        cairn_models::interface::embed::EmbedError::WidthMismatch {
            admitted: Some(2560),
            got: 1024
        }
    );
}

#[test]
fn an_undeclared_profile_takes_the_providers_claim() {
    let provider = provider();
    let fast = served()
        .into_iter()
        .find(|model| model.id == "Qwen/Qwen3.6-35B-A3B")
        .unwrap();
    assert_eq!(serves_for(&fast), provider_serves());
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
fn a_model_whose_profiles_serve_nothing_carried_here_serves_nothing() {
    let mut model = served()
        .into_iter()
        .find(|model| model.id == "Qwen/Qwen3.6-35B-A3B")
        .unwrap();
    model.deployments[0].verbs = BTreeSet::from([VerbKind::Speak]);
    assert!(serves_for(&model).is_empty());
}

/// The provider-wide claim is conversation and decide over chat; embed is
/// served only to a model whose profile makes it an embedder, so a model no
/// profile declares is never offered for it.
#[test]
fn the_provider_claims_conversation_and_decide_and_embeds_only_a_declared_embedder() {
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
    let provider = provider();
    for model in [DSV4, "Qwen/Qwen3.6-35B-A3B", "unlisted/model"] {
        assert!(provider.admit(model, VerbKind::Embed).is_err(), "{model}");
    }
    let embedders: Vec<_> = served()
        .iter()
        .filter(|model| {
            serves_for(model)
                .iter()
                .any(|served| matches!(served.verb, Verb::Embed(_)))
        })
        .map(|model| model.id.clone())
        .collect();
    assert_eq!(embedders, [EMBEDDER]);
}
