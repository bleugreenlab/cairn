use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use cairn_models_types::catalog::{
    claude_models, custom_descriptor, CustomProvider, DiscoveredModel, ProviderModelCatalog,
    ServedVerb, Verb, VerbKind, Wire,
};
use cairn_models_types::embedding::{
    BatchLimits, EmbeddingCapabilities, EmbeddingWidths, PurposeHandling,
};
use cairn_models_types::usage::{BillingUnit, Charge};
use serde_json::{json, Value};

use super::{embed, families};
use crate::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, Delivery,
};
use crate::interface::embed::{
    EmbedCall, EmbedError, EmbedRequest, EmbedRoute, Purpose, EMBED_FIXES,
};
use crate::interface::{Catalog, CatalogProvider};
use crate::wire::embeddings::gateway::GatewayEmbedResponse;
use crate::wire::embeddings::openai::EmbeddingsResponse;

/// A `Complete` that answers from a script and records every request.
#[derive(Default)]
struct Scripted {
    replies: Mutex<VecDeque<Result<CompletionReply, CompletionError>>>,
    requests: Mutex<Vec<(&'static str, Value)>>,
}

impl Scripted {
    fn answering(replies: Vec<Result<CompletionReply, CompletionError>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            ..Self::default()
        }
    }

    fn requests(&self) -> Vec<(&'static str, Value)> {
        self.requests.lock().unwrap().clone()
    }
}

impl Complete for Scripted {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        assert!(
            matches!(call.delivery, Delivery::Whole { .. }),
            "an embedding is never streamed"
        );
        self.requests
            .lock()
            .unwrap()
            .push((call.wire.family(), call.body));
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("an unscripted request")
    }
}

fn hub() -> EmbedRoute {
    super::openai::route_for(
        "custom:acc_hub",
        "Hub",
        "http://hub/v1/",
        Some("sk-test"),
        &[],
    )
}

fn gateway() -> EmbedRoute {
    super::gateway::route_for("https://api.example/", "device-jwt")
}

/// An OpenAI-wire reply with one `width`-wide vector per index, each filled
/// with its index so order can be read back.
fn openai_reply(indices: &[u32], width: usize, usage: Value) -> CompletionReply {
    let data: Vec<Value> = indices
        .iter()
        .map(|index| json!({"index": index, "embedding": vec![*index as f32; width]}))
        .collect();
    let mut body = json!({"object": "list", "model": "served-name", "data": data});
    if !usage.is_null() {
        body["usage"] = usage;
    }
    CompletionReply::Embeddings(serde_json::from_value::<EmbeddingsResponse>(body).unwrap())
}

fn in_order(count: u32, width: usize) -> CompletionReply {
    openai_reply(
        &(0..count).collect::<Vec<_>>(),
        width,
        json!({"prompt_tokens": count}),
    )
}

fn gateway_reply(count: usize, model: &str) -> CompletionReply {
    CompletionReply::GatewayEmbed(GatewayEmbedResponse {
        embeddings: vec![vec![0.5; 1536]; count],
        model: model.to_string(),
        dims: 1536,
        input_tokens: Some(3),
    })
}

fn request(texts: &[&str], purpose: Purpose, dims: Option<u32>) -> EmbedRequest {
    EmbedRequest {
        texts: texts.iter().map(|text| text.to_string()).collect(),
        purpose,
        dims,
    }
}

fn plain(native: Option<u32>, widths: EmbeddingWidths) -> EmbeddingCapabilities {
    EmbeddingCapabilities {
        native_dims: native,
        widths,
        purpose: PurposeHandling::Ignored,
        limits: BatchLimits::default(),
    }
}

fn run(
    complete: &Scripted,
    route: &EmbedRoute,
    model: &str,
    capabilities: &EmbeddingCapabilities,
    request: &EmbedRequest,
) -> Result<crate::interface::embed::Embeddings, EmbedError> {
    embed(
        complete,
        EmbedCall {
            route,
            model,
            capabilities,
            request,
            timeout: Duration::from_secs(5),
        },
    )
}

fn sent_texts(body: &Value, field: &str) -> Vec<String> {
    body[field]
        .as_array()
        .unwrap()
        .iter()
        .map(|text| text.as_str().unwrap().to_string())
        .collect()
}

// Purpose

#[test]
fn the_gateway_carries_purpose_in_its_field_and_leaves_texts_alone() {
    let complete = Scripted::answering(vec![Ok(gateway_reply(2, "cohere-embed-v4"))]);
    let result = run(
        &complete,
        &gateway(),
        "cohere-embed-v4",
        &EmbeddingCapabilities::cairn_gateway(),
        &request(&["a", "b"], Purpose::Query, None),
    )
    .unwrap();
    let (family, body) = &complete.requests()[0];
    assert_eq!(*family, "gateway-embed");
    assert_eq!(body["input_type"], "search_query");
    assert_eq!(sent_texts(body, "texts"), ["a", "b"]);
    assert!(body.get("dims").is_none());
    assert_eq!(
        (result.provider.as_str(), result.model.as_str(), result.dims),
        ("cairn", "cohere-embed-v4", 1536)
    );
    // The gateway's estimate is reported as input tokens; it reports no cost.
    assert_eq!(
        result.usage.quantity(Charge::Input, BillingUnit::Tokens),
        Some(3)
    );
    assert_eq!(result.usage.cost_usd, None);
}

#[test]
fn nomic_prefixes_documents_and_queries_differently() {
    let capabilities = families::capabilities(
        "nomic-embed-text:latest",
        Some(768),
        true,
        BatchLimits::default(),
    );
    for (purpose, expected) in [
        (Purpose::Document, "search_document: hello"),
        (Purpose::Query, "search_query: hello"),
    ] {
        let complete = Scripted::answering(vec![Ok(in_order(1, 768))]);
        run(
            &complete,
            &hub(),
            "nomic-embed-text:latest",
            &capabilities,
            &request(&["hello"], purpose, None),
        )
        .unwrap();
        let body = &complete.requests()[0].1;
        assert_eq!(sent_texts(body, "input"), [expected]);
        // A prefix model is not sent a purpose field.
        assert!(body.get("input_type").is_none());
    }
}

#[test]
fn qwen3_instructs_only_its_queries() {
    let capabilities = families::capabilities(
        "qwen/qwen3-embedding-4b",
        Some(2560),
        false,
        BatchLimits::default(),
    );
    let complete = Scripted::answering(vec![Ok(in_order(1, 2560)), Ok(in_order(1, 2560))]);
    for purpose in [Purpose::Document, Purpose::Query] {
        run(
            &complete,
            &hub(),
            "qwen/qwen3-embedding-4b",
            &capabilities,
            &request(&["what is cairn"], purpose, None),
        )
        .unwrap();
    }
    let requests = complete.requests();
    assert_eq!(sent_texts(&requests[0].1, "input"), ["what is cairn"]);
    assert_eq!(
        sent_texts(&requests[1].1, "input"),
        ["Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: what is cairn"]
    );
}

#[test]
fn an_ignored_purpose_sends_texts_byte_identical() {
    let texts = ["  spaced  ", "é unicode ✓", "line\nbreak"];
    let complete = Scripted::answering(vec![Ok(in_order(3, 4))]);
    run(
        &complete,
        &hub(),
        "some/unknown-embedder",
        &plain(Some(4), EmbeddingWidths::NativeOnly),
        &request(&texts, Purpose::Query, None),
    )
    .unwrap();
    assert_eq!(sent_texts(&complete.requests()[0].1, "input"), texts);
}

#[test]
fn a_native_purpose_on_the_openai_wire_rides_its_input_type_field() {
    let capabilities = EmbeddingCapabilities {
        purpose: PurposeHandling::Native,
        ..plain(Some(4), EmbeddingWidths::NativeOnly)
    };
    let complete = Scripted::answering(vec![Ok(in_order(1, 4))]);
    run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["x"], Purpose::Document, None),
    )
    .unwrap();
    assert_eq!(complete.requests()[0].1["input_type"], "search_document");
}

// Width

#[test]
fn asking_for_the_native_width_sends_no_dimensions() {
    let complete = Scripted::answering(vec![Ok(in_order(1, 8))]);
    let result = run(
        &complete,
        &hub(),
        "m",
        &plain(Some(8), EmbeddingWidths::UpTo { min: 2 }),
        &request(&["x"], Purpose::Query, Some(8)),
    )
    .unwrap();
    assert!(complete.requests()[0].1.get("dimensions").is_none());
    assert_eq!(result.dims, 8);
}

#[test]
fn a_declared_shorter_width_is_sent_and_returned() {
    for widths in [
        EmbeddingWidths::UpTo { min: 2 },
        EmbeddingWidths::Choices(vec![4]),
    ] {
        let complete = Scripted::answering(vec![Ok(in_order(1, 4))]);
        let result = run(
            &complete,
            &hub(),
            "m",
            &plain(Some(8), widths),
            &request(&["x"], Purpose::Query, Some(4)),
        )
        .unwrap();
        assert_eq!(complete.requests()[0].1["dimensions"], 4);
        assert_eq!(result.dims, 4);
    }
}

#[test]
fn a_width_outside_the_offer_is_refused_naming_what_it_offers_before_any_post() {
    let cases = [
        (
            plain(Some(2560), EmbeddingWidths::NativeOnly),
            256,
            "it returns only its native 2560, not 256",
        ),
        (
            plain(Some(1536), EmbeddingWidths::Choices(vec![256, 512])),
            300,
            "it returns 1536 or 256, 512, not 300",
        ),
        (
            plain(Some(768), EmbeddingWidths::UpTo { min: 64 }),
            32,
            "it returns any width from 64 to 768, not 32",
        ),
        (
            plain(None, EmbeddingWidths::NativeOnly),
            256,
            "its native width is not known, so it is only asked for that width, not 256",
        ),
    ];
    for (capabilities, width, text) in cases {
        let complete = Scripted::default();
        let error = run(
            &complete,
            &hub(),
            "m",
            &capabilities,
            &request(&["x"], Purpose::Query, Some(width)),
        )
        .unwrap_err();
        assert_eq!(error, EmbedError::Unsupported(text.to_string()));
        assert!(complete.requests().is_empty());
    }
}

#[test]
fn a_server_ignoring_dimensions_is_a_width_mismatch_never_truncated() {
    // The local hub answers 200 with 2560 floats whatever `dimensions` says.
    let complete = Scripted::answering(vec![Ok(in_order(1, 2560))]);
    let error = run(
        &complete,
        &hub(),
        "qwen/qwen3-embedding-4b",
        &plain(Some(2560), EmbeddingWidths::UpTo { min: 32 }),
        &request(&["x"], Purpose::Query, Some(256)),
    )
    .unwrap_err();
    assert_eq!(
        error,
        EmbedError::WidthMismatch {
            admitted: Some(256),
            got: 2560
        }
    );
    assert!(
        error.to_string().contains("declare this model native-only"),
        "{error}"
    );
}

#[test]
fn an_unknown_native_width_is_measured_and_held_across_sub_batches() {
    let limits = BatchLimits {
        max_texts: Some(1),
        ..BatchLimits::default()
    };
    let capabilities = EmbeddingCapabilities {
        limits,
        ..plain(None, EmbeddingWidths::NativeOnly)
    };
    let complete = Scripted::answering(vec![Ok(in_order(1, 6)), Ok(in_order(1, 6))]);
    let result = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["a", "b"], Purpose::Query, None),
    )
    .unwrap();
    assert_eq!(result.dims, 6);

    let complete = Scripted::answering(vec![Ok(in_order(1, 6)), Ok(in_order(1, 7))]);
    let error = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["a", "b"], Purpose::Query, None),
    )
    .unwrap_err();
    assert_eq!(
        error,
        EmbedError::WidthMismatch {
            admitted: None,
            got: 7
        }
    );
}

#[test]
fn an_empty_vector_is_refused_even_when_the_width_is_unknown() {
    // An unknown-width offer measures its width on the first reply, so an
    // empty vector would otherwise set the width to zero.
    let capabilities = EmbeddingCapabilities {
        limits: BatchLimits {
            max_texts: Some(1),
            ..BatchLimits::default()
        },
        ..plain(None, EmbeddingWidths::NativeOnly)
    };
    let complete = Scripted::answering(vec![Ok(in_order(1, 3)), Ok(in_order(1, 0))]);
    let error = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["a", "b"], Purpose::Query, None),
    )
    .unwrap_err();
    assert_eq!(
        error,
        EmbedError::Completion(CompletionError::InvalidResponse(
            "Hub returned an empty vector for text 1".into()
        ))
    );

    let complete = Scripted::answering(vec![Ok(in_order(1, 0))]);
    let error = run(
        &complete,
        &hub(),
        "m",
        &plain(None, EmbeddingWidths::NativeOnly),
        &request(&["a"], Purpose::Query, None),
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            EmbedError::Completion(CompletionError::InvalidResponse(_))
        ),
        "{error:?}"
    );
}

// Batches

fn texts(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("t{index}")).collect()
}

#[test]
fn an_oversized_request_is_split_in_order_and_joined_in_input_order() {
    let capabilities = EmbeddingCapabilities {
        limits: BatchLimits {
            max_texts: Some(32),
            ..BatchLimits::default()
        },
        ..plain(Some(2), EmbeddingWidths::NativeOnly)
    };
    let complete = Scripted::answering(vec![
        Ok(in_order(32, 2)),
        Ok(in_order(32, 2)),
        Ok(in_order(6, 2)),
    ]);
    let all = texts(70);
    let result = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &EmbedRequest {
            texts: all.clone(),
            purpose: Purpose::Document,
            dims: None,
        },
    )
    .unwrap();
    let requests = complete.requests();
    assert_eq!(requests.len(), 3);
    let sent: Vec<String> = requests
        .iter()
        .flat_map(|(_, body)| sent_texts(body, "input"))
        .collect();
    assert_eq!(sent, all);
    assert_eq!(
        requests
            .iter()
            .map(|(_, body)| body["input"].as_array().unwrap().len())
            .collect::<Vec<_>>(),
        [32, 32, 6]
    );
    assert_eq!(result.vectors.len(), 70);
    // Each sub-batch's vectors are filled with their in-batch index.
    assert_eq!(result.vectors[33][0], 1.0);
    assert_eq!(result.vectors[69][0], 5.0);
    assert_eq!(
        result.usage.quantity(Charge::Input, BillingUnit::Tokens),
        Some(70)
    );
}

#[test]
fn the_character_limit_splits_too_and_a_single_overlong_text_is_refused_by_index() {
    let capabilities = EmbeddingCapabilities {
        limits: BatchLimits {
            max_chars: Some(10),
            ..BatchLimits::default()
        },
        ..plain(Some(2), EmbeddingWidths::NativeOnly)
    };
    let complete = Scripted::answering(vec![Ok(in_order(2, 2)), Ok(in_order(1, 2))]);
    run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["aaaa", "bbbbbb", "cccc"], Purpose::Document, None),
    )
    .unwrap();
    assert_eq!(complete.requests().len(), 2);

    let complete = Scripted::default();
    let error = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["ok", "this one is far too long"], Purpose::Document, None),
    )
    .unwrap_err();
    assert!(
        matches!(&error, EmbedError::InvalidRequest(text) if text.starts_with("text 1 is 24 characters")),
        "{error:?}"
    );
    assert!(complete.requests().is_empty());
}

#[test]
fn the_limit_counts_the_prefixed_text_that_is_actually_sent() {
    let capabilities = EmbeddingCapabilities {
        purpose: PurposeHandling::Prefix {
            document: "doc: ".into(),
            query: String::new(),
        },
        limits: BatchLimits {
            max_chars: Some(6),
            ..BatchLimits::default()
        },
        ..plain(Some(2), EmbeddingWidths::NativeOnly)
    };
    let error = run(
        &Scripted::default(),
        &hub(),
        "m",
        &capabilities,
        &request(&["abc"], Purpose::Document, None),
    )
    .unwrap_err();
    assert!(matches!(error, EmbedError::InvalidRequest(_)), "{error:?}");
}

#[test]
fn a_failing_sub_batch_fails_the_whole_call() {
    let capabilities = EmbeddingCapabilities {
        limits: BatchLimits {
            max_texts: Some(1),
            ..BatchLimits::default()
        },
        ..plain(Some(2), EmbeddingWidths::NativeOnly)
    };
    let failure =
        || CompletionError::Upstream("Hub embeddings returned HTTP 429: slow down".into());
    let complete =
        Scripted::answering(vec![Ok(in_order(1, 2)), Err(failure()), Ok(in_order(1, 2))]);
    let error = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["a", "b", "c"], Purpose::Document, None),
    )
    .unwrap_err();
    assert_eq!(error, EmbedError::Completion(failure()));
    assert_eq!(
        complete.requests().len(),
        2,
        "nothing is posted after a failure"
    );
}

#[test]
fn usage_sums_and_cost_is_set_only_when_every_sub_batch_reported_it() {
    let capabilities = EmbeddingCapabilities {
        limits: BatchLimits {
            max_texts: Some(1),
            ..BatchLimits::default()
        },
        ..plain(Some(2), EmbeddingWidths::NativeOnly)
    };
    let complete = Scripted::answering(vec![
        Ok(openai_reply(
            &[0],
            2,
            json!({"prompt_tokens": 4, "cost": 0.5}),
        )),
        Ok(openai_reply(
            &[0],
            2,
            json!({"prompt_tokens": 6, "cost": 0.25}),
        )),
    ]);
    let result = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["a", "b"], Purpose::Document, None),
    )
    .unwrap();
    assert_eq!(
        result.usage.quantity(Charge::Input, BillingUnit::Tokens),
        Some(10)
    );
    assert_eq!(result.usage.cost_usd, Some(0.75));

    let complete = Scripted::answering(vec![
        Ok(openai_reply(
            &[0],
            2,
            json!({"prompt_tokens": 4, "cost": 0.5}),
        )),
        Ok(openai_reply(&[0], 2, Value::Null)),
    ]);
    let result = run(
        &complete,
        &hub(),
        "m",
        &capabilities,
        &request(&["a", "b"], Purpose::Document, None),
    )
    .unwrap();
    assert_eq!(
        result.usage.quantity(Charge::Input, BillingUnit::Tokens),
        None
    );
    assert_eq!(result.usage.cost_usd, None);
}

#[test]
fn no_texts_or_an_empty_text_is_refused_before_any_post() {
    let capabilities = plain(Some(2), EmbeddingWidths::NativeOnly);
    for (texts, expected) in [
        (vec![], "there are no texts to embed"),
        (
            vec!["a", ""],
            "text 1 is empty; every text must have content",
        ),
    ] {
        let complete = Scripted::default();
        let error = run(
            &complete,
            &hub(),
            "m",
            &capabilities,
            &request(&texts, Purpose::Query, None),
        )
        .unwrap_err();
        assert_eq!(error, EmbedError::InvalidRequest(expected.into()));
        assert!(complete.requests().is_empty());
    }
}

// Order and identity

#[test]
fn a_shuffled_reply_is_restored_to_input_order() {
    let complete = Scripted::answering(vec![Ok(openai_reply(&[2, 0, 1], 3, Value::Null))]);
    let result = run(
        &complete,
        &hub(),
        "m",
        &plain(Some(3), EmbeddingWidths::NativeOnly),
        &request(&["a", "b", "c"], Purpose::Query, None),
    )
    .unwrap();
    assert_eq!(
        result
            .vectors
            .iter()
            .map(|vector| vector[0])
            .collect::<Vec<_>>(),
        [0.0, 1.0, 2.0]
    );
}

#[test]
fn a_missing_or_duplicate_index_is_an_invalid_reply() {
    for indices in [&[0, 0][..], &[0][..], &[0, 5][..]] {
        let complete = Scripted::answering(vec![Ok(openai_reply(indices, 3, Value::Null))]);
        let error = run(
            &complete,
            &hub(),
            "m",
            &plain(Some(3), EmbeddingWidths::NativeOnly),
            &request(&["a", "b"], Purpose::Query, None),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                EmbedError::Completion(CompletionError::InvalidResponse(_))
            ),
            "{indices:?}: {error:?}"
        );
    }
}

#[test]
fn the_result_is_keyed_by_the_offer_not_the_served_name() {
    // bgllm answers `qwen3-embedding-4b` for `qwen/qwen3-embedding-4b`.
    let complete = Scripted::answering(vec![Ok(in_order(1, 4))]);
    let result = run(
        &complete,
        &hub(),
        "qwen/qwen3-embedding-4b",
        &plain(Some(4), EmbeddingWidths::NativeOnly),
        &request(&["x"], Purpose::Query, None),
    )
    .unwrap();
    assert_eq!(result.model, "qwen/qwen3-embedding-4b");
    assert_eq!(result.provider, "custom:acc_hub");
    assert_eq!(result.dims, 4);
}

#[test]
fn a_gateway_reply_naming_another_model_is_refused() {
    let complete = Scripted::answering(vec![Ok(gateway_reply(1, "cohere-embed-v5"))]);
    let error = run(
        &complete,
        &gateway(),
        "cohere-embed-v4",
        &EmbeddingCapabilities::cairn_gateway(),
        &request(&["x"], Purpose::Document, None),
    )
    .unwrap_err();
    assert_eq!(
        error,
        EmbedError::Completion(CompletionError::InvalidResponse(
            "the gateway now serves cohere-embed-v5, not cohere-embed-v4; refusing to mix vector spaces".into()
        ))
    );
}

#[test]
fn routes_place_their_urls_and_keys() {
    let EmbedRoute::OpenAi(endpoint) = hub() else {
        panic!("an OpenAI route")
    };
    assert_eq!(endpoint.url, "http://hub/v1/embeddings");
    assert_eq!(endpoint.headers[0].0, "authorization");
    assert_eq!(hub().wire(), Wire::OpenAiEmbeddings);
    let EmbedRoute::Gateway(endpoint) = gateway() else {
        panic!("a gateway route")
    };
    assert_eq!(endpoint.url, "https://api.example/embed");
    assert_eq!(endpoint.backend_key, "cairn");
    assert_eq!(gateway().wire(), Wire::CairnEmbed);
}

// Selection

fn embedding_model(id: &str, capabilities: EmbeddingCapabilities) -> DiscoveredModel {
    let mut model = claude_models().remove(0);
    model.id = id.to_string();
    model.model = id.to_string();
    model.display_name = id.to_string();
    model.serves = Some(vec![ServedVerb {
        verb: Verb::Embed(capabilities),
        wire: Wire::OpenAiEmbeddings,
    }]);
    model
}

fn provider(account: &str, models: Vec<DiscoveredModel>) -> CatalogProvider {
    let descriptor = custom_descriptor(&CustomProvider {
        account_id: account.to_string(),
        display_name: account.to_string(),
    });
    CatalogProvider {
        installed: true,
        models: Some(ProviderModelCatalog {
            backend: descriptor.key.to_string(),
            models,
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        }),
        descriptor,
        tiers: Vec::new(),
        serves: ServedVerb::conversation(Wire::OpenAiChatCompletions),
        unable: Vec::new(),
    }
}

fn two_offer_catalog() -> Catalog {
    Catalog {
        providers: vec![
            provider(
                "acc_hub",
                vec![embedding_model(
                    "qwen/qwen3-embedding-4b",
                    plain(Some(2560), EmbeddingWidths::NativeOnly),
                )],
            ),
            provider(
                "acc_ollama",
                vec![embedding_model(
                    "nomic-embed-text:latest",
                    plain(Some(768), EmbeddingWidths::UpTo { min: 64 }),
                )],
            ),
        ],
    }
}

#[test]
fn an_unset_model_takes_the_first_offer_that_admits_the_width() {
    let catalog = two_offer_catalog();
    let (offer, _) = catalog
        .embedding_offer(None, &request(&["x"], Purpose::Query, None))
        .unwrap();
    assert_eq!(offer.model, "qwen/qwen3-embedding-4b");
    let (offer, capabilities) = catalog
        .embedding_offer(None, &request(&["x"], Purpose::Query, Some(256)))
        .unwrap();
    assert_eq!(offer.model, "nomic-embed-text:latest");
    assert_eq!(capabilities.native_dims, Some(768));
    // The declared embedding model no longer inherits its provider's chat
    // claim.
    assert!(catalog
        .offers_serving(VerbKind::Complete)
        .iter()
        .all(|offer| offer.model != "qwen/qwen3-embedding-4b"));
}

#[test]
fn a_pinned_model_that_cannot_return_the_width_names_offers_that_would() {
    let error = two_offer_catalog()
        .embedding_offer(
            Some("qwen/qwen3-embedding-4b"),
            &request(&["x"], Purpose::Query, Some(256)),
        )
        .unwrap_err();
    let EmbedError::NoOffer(refusal) = &error else {
        panic!("{error:?}")
    };
    assert_eq!(
        refusal.detail.as_deref(),
        Some("qwen/qwen3-embedding-4b on custom:acc_hub: it returns only its native 2560, not 256; offers that would: custom:acc_ollama/nomic-embed-text:latest")
    );
    assert!(error
        .to_string()
        .starts_with("No embedding-capable model serves 'qwen/qwen3-embedding-4b': "));
}

#[test]
fn an_empty_catalog_refuses_naming_the_fixes() {
    let error = Catalog { providers: vec![] }
        .embedding_offer(None, &request(&["x"], Purpose::Query, None))
        .unwrap_err();
    let text = error.to_string();
    assert!(
        text.starts_with("No embedding-capable model is configured."),
        "{text}"
    );
    assert!(text.contains(EMBED_FIXES), "{text}");
}

// The family table

#[test]
fn the_family_table_matches_every_id_form_and_knows_nothing_else() {
    let width = |id: &str| families::family(id).and_then(|family| family.native_dims);
    assert_eq!(width("nomic-embed-text:latest"), Some(768));
    assert_eq!(width("nomic-embed-text"), Some(768));
    assert_eq!(width("nomic-ai/nomic-embed-text-v1.5"), Some(768));
    assert_eq!(width("nomic-embed-text:v1.5"), Some(768));
    assert_eq!(width("qwen3-embedding:4b"), Some(2560));
    assert_eq!(width("qwen/qwen3-embedding-4b"), Some(2560));
    assert_eq!(width("Qwen/Qwen3-Embedding-8B"), Some(4096));
    assert_eq!(width("qwen3-embedding:0.6b-q8_0"), Some(1024));
    assert_eq!(width("openai/text-embedding-3-small"), Some(1536));
    assert_eq!(width("text-embedding-3-large"), Some(3072));
    assert_eq!(width("openai/text-embedding-ada-002"), Some(1536));
    assert_eq!(width("google/embeddinggemma-300m"), Some(768));
    assert_eq!(width("embeddinggemma:latest"), Some(768));
    // Prefix families whose width varies by size.
    assert!(families::family("intfloat/e5-large-v2").is_some());
    assert!(families::family("intfloat/multilingual-e5-large").is_some());
    assert!(families::family("baai/bge-base-en-v1.5").is_some());
    // Unknown, or deliberately not claimed.
    for unknown in [
        "baai/bge-m3",
        "nomic-embed-text-v2-moe",
        "qwen3-embedding",
        "voyageai/voyage-4",
        "mxbai-embed-large",
    ] {
        assert_eq!(families::family(unknown), None, "{unknown}");
    }
}

#[test]
fn capabilities_take_matryoshka_only_where_the_provider_honours_it_at_the_family_width() {
    let limits = BatchLimits::default;
    let ollama = families::capabilities("nomic-embed-text:latest", Some(768), true, limits());
    assert_eq!(ollama.widths, EmbeddingWidths::UpTo { min: 64 });
    // A provider width that is not the family's: native only.
    let odd = families::capabilities("nomic-embed-text:latest", Some(512), true, limits());
    assert_eq!(odd.widths, EmbeddingWidths::NativeOnly);
    // A provider that does not honour `dimensions`: native only, at the
    // family's width.
    let openrouter = families::capabilities("qwen/qwen3-embedding-4b", None, false, limits());
    assert_eq!(openrouter.native_dims, Some(2560));
    assert_eq!(openrouter.widths, EmbeddingWidths::NativeOnly);
    let unknown = families::capabilities("voyageai/voyage-4", None, true, limits());
    assert_eq!(unknown.native_dims, None);
    assert_eq!(unknown.purpose, PurposeHandling::Ignored);
    assert_eq!(unknown.validate(), Ok(()));
    let gemma = families::capabilities("embeddinggemma", Some(768), true, limits());
    assert_eq!(gemma.widths, EmbeddingWidths::Choices(vec![512, 256, 128]));
}
