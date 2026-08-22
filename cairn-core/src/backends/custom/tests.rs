//! What a named custom provider promises, tested against the shape a real one
//! actually produces.
//!
//! The streaming fixtures are transcribed from a vLLM 0.21 deployment serving
//! `unsloth/Qwen3.8-27B-NVFP4` with the qwen3 reasoning parser, which is the
//! endpoint this feature was built against. They are replayed through the same
//! `StreamingAggregate` the live transport uses, so what they prove is a
//! property of the shared OpenAI-compatible wire rather than of a parallel
//! test-only parser.

use super::models::discover_blocking;
use super::CustomProviderConfig;
use crate::backends::catalog;
use crate::backends::openai_compat::http::Endpoint;
use crate::backends::openai_compat::wire::{ChatContent, ChatStreamChunk, StreamingAggregate};
use crate::identity::{
    ApiProvider, CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAccount, ProviderAuth,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

const TEXT_STREAM: &str = include_str!("testdata/text_stream.sse");
const TOOL_CALL_STREAM: &str = include_str!("testdata/tool_call_stream.sse");
const REASONING_ONLY_STREAM: &str = include_str!("testdata/reasoning_only_stream.sse");
/// Captured verbatim from a live vLLM 0.21 server (`gemma4-26b`, NVFP4) on an
/// operator's DGX Spark, byte for byte as it came off the wire.
///
/// The other fixtures are written to exercise a property. This one exists to
/// keep a real server's actual output in the suite, because the ways a real one
/// differs from a plausible one are the ways an adapter breaks in the field:
/// unknown sibling fields on every object (`logprobs`, `token_ids`,
/// `stop_reason`, `prompt_token_ids`, `system_fingerprint`), a tool-call
/// opening frame whose keys arrive in a different order than the spec's
/// examples, argument fragments split mid-token, and — the one that actually
/// catches code — a terminal usage frame carrying NO choices at all.
const VLLM_LIVE_TOOL_CALL: &str = include_str!("testdata/vllm_live_tool_call.sse");

/// Replay an SSE fixture through the shared aggregator exactly as the transport
/// does: strip `data:`, parse a chunk, apply it. Returns the aggregate plus
/// whether the stream carried a `[DONE]` sentinel.
fn replay(fixture: &str) -> (StreamingAggregate, bool) {
    let mut aggregate = StreamingAggregate::default();
    let mut saw_done = false;
    for line in fixture.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            saw_done = true;
            continue;
        }
        let chunk: ChatStreamChunk =
            serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
        aggregate.apply_chunk(&chunk);
    }
    (aggregate, saw_done)
}

#[test]
fn sampling_is_forwarded_only_when_the_operator_configures_it() {
    let messages = [crate::backends::openai_compat::wire::ChatMessage::user(
        "hi".to_string(),
    )];
    let absent = super::adapter::generation_body(
        "gemma-4-26b",
        &messages,
        &CustomSampling::default(),
        32_768,
    );
    for key in ["repetition_penalty", "temperature", "top_p", "top_k"] {
        assert!(
            absent.get(key).is_none(),
            "{key} must not acquire a default"
        );
    }

    let configured = super::adapter::generation_body(
        "gemma-4-26b",
        &messages,
        &CustomSampling {
            repetition_penalty: Some(1.1),
            temperature: Some(0.7),
            top_p: Some(0.9),
            top_k: Some(40),
        },
        32_768,
    );
    assert_eq!(configured["repetition_penalty"], serde_json::json!(1.1));
    assert_eq!(configured["temperature"], serde_json::json!(0.7));
    assert_eq!(configured["top_p"], serde_json::json!(0.9));
    assert_eq!(configured["top_k"], serde_json::json!(40));
}

#[test]
fn every_agent_generation_has_a_bounded_output_budget() {
    let body = super::adapter::generation_body(
        "gemma-4-26b",
        &[crate::backends::openai_compat::wire::ChatMessage::user(
            "hi".to_string(),
        )],
        &CustomSampling::default(),
        65_536,
    );
    assert_eq!(body["max_tokens"], serde_json::json!(65_536));
    assert_eq!(body["stream"], serde_json::json!(true));
}

#[test]
fn a_streamed_answer_keeps_thinking_out_of_the_visible_text() {
    // With `--reasoning-parser qwen3`, chain-of-thought streams under the bare
    // `reasoning` key rather than `reasoning_content`. Cairn's DTO reads that
    // spelling; this fixture is what keeps it read.
    let (aggregate, saw_done) = replay(TEXT_STREAM);
    assert_eq!(aggregate.text, "Hello, world.");
    assert_eq!(
        aggregate.reasoning,
        "The user asked for a greeting, so I will greet them."
    );
    assert!(saw_done, "the fixture ends with the DONE sentinel");
    assert_eq!(aggregate.finish_reason(), Some("stop"));

    let response = aggregate.into_response(true);
    assert_eq!(response.model.as_deref(), Some("qwen3.8-27b"));
    assert_eq!(
        response.choices[0]
            .message
            .content
            .as_ref()
            .and_then(ChatContent::as_text),
        Some("Hello, world.")
    );
}

#[test]
fn fragmented_tool_call_arguments_reassemble_into_dispatchable_calls() {
    // The name and id arrive in one delta and the JSON arguments stream in
    // fragments across later ones. If they do not coalesce, the turn loop sees
    // no tool calls, skips execution, and the agent silently does nothing.
    let (aggregate, _) = replay(TOOL_CALL_STREAM);
    assert_eq!(aggregate.finish_reason(), Some("tool_calls"));
    let response = aggregate.into_response(false);
    let calls = response.choices[0]
        .message
        .tool_calls
        .as_ref()
        .expect("tool calls");
    assert_eq!(calls.len(), 2, "two calls, merged by index");
    assert_eq!(calls[0].id, "call_abc");
    assert_eq!(calls[0].function.name, "read");
    assert_eq!(
        calls[0].function.arguments,
        r#"{"paths": ["file:src/lib.rs"]}"#
    );
    assert_eq!(calls[1].id, "call_def");
    assert_eq!(calls[1].function.name, "run");

    let parsed: serde_json::Value = serde_json::from_str(&calls[0].function.arguments)
        .expect("reassembled arguments are valid JSON");
    assert_eq!(parsed["paths"][0], "file:src/lib.rs");
}

#[test]
fn a_real_vllm_stream_survives_its_own_extra_fields_and_a_choiceless_usage_frame() {
    // Ground truth rather than a plausible reconstruction: replayed from bytes
    // a live vLLM 0.21 server actually sent. Three of its properties are the
    // ones a hand-written fixture is least likely to guess, and each would be a
    // field failure rather than a test failure.
    let (aggregate, saw_done) = replay(VLLM_LIVE_TOOL_CALL);

    // 1. Every frame carries fields Cairn does not model (`logprobs`,
    //    `token_ids`, `stop_reason`, `prompt_token_ids`, `system_fingerprint`).
    //    A strict deserializer would reject the whole stream, so the fact that
    //    anything parsed at all is the assertion.
    assert_eq!(aggregate.finish_reason(), Some("tool_calls"));
    assert!(saw_done, "vLLM terminates with the [DONE] sentinel");

    // 2. The usage frame carries `"choices": []`. Usage read inside a
    //    per-choice loop would silently never be recorded, and every turn on
    //    every custom provider would report zero tokens. Read first, because
    //    building the response consumes the aggregate.
    let usage = aggregate
        .usage
        .clone()
        .expect("usage from a choiceless frame");
    assert_eq!(usage.prompt_tokens, Some(23));
    assert_eq!(usage.completion_tokens, Some(20));

    // 3. The opening tool-call frame puts `id` and `type` BEFORE `index`, and
    //    the arguments arrive split at points no tokenizer boundary respects
    //    (`"src`, `/`, `lib`, `.`, `rs`). Reassembly is by index, so neither
    //    matters — but only if reassembly is genuinely index-keyed.
    let response = aggregate.into_response(false);
    let calls = response.choices[0]
        .message
        .tool_calls
        .as_ref()
        .expect("tool calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, "chatcmpl-tool-826f6bf704aade19");
    assert_eq!(calls[0].function.name, "read");
    let parsed: serde_json::Value = serde_json::from_str(&calls[0].function.arguments)
        .expect("fragments reassemble into valid JSON");
    assert_eq!(parsed["paths"][0], "src/lib.rs");
}

#[test]
fn a_turn_that_ran_out_inside_thinking_is_not_an_empty_answer() {
    // A protocol-valid stream can terminate cleanly inside reasoning with no
    // visible content at all. It consumed tokens and produced no answer, so
    // the distinguishing evidence — a `length` finish with reasoning present
    // and text empty — has to survive aggregation for the turn loop to tell it
    // apart from a model that legitimately said nothing.
    let (aggregate, _) = replay(REASONING_ONLY_STREAM);
    assert!(aggregate.text.is_empty(), "no visible content was produced");
    assert!(!aggregate.reasoning.is_empty(), "tokens went into thinking");
    assert_eq!(aggregate.finish_reason(), Some("length"));
}

fn config(name: &str, base_url: &str, models: CustomModelPolicy) -> CustomProviderConfig {
    CustomProviderConfig {
        account_id: format!("acc_{name}"),
        display_name: name.to_string(),
        base_url: base_url.to_string(),
        protocol: CustomProtocol::OpenAiChatCompletions,
        headers: vec![("content-type".into(), "application/json".into())],
        models,
        sampling: CustomSampling::default(),
        max_output_tokens: 32_768,
    }
}

fn account(id: &str, label: &str, auth: ProviderAuth) -> ProviderAccount {
    ProviderAccount {
        id: id.to_string(),
        label: label.to_string(),
        api_provider: ApiProvider::Custom,
        source: crate::identity::AccountSource::Configured,
        auth,
        project_id: None,
        sort_order: 0,
        created_at: 0,
        last_used_at: None,
        email: None,
        plan: None,
        health: None,
    }
}

fn endpoint(auth: ProviderAuth) -> ProviderAuth {
    auth
}

#[test]
fn the_chat_and_model_urls_hang_off_the_stored_api_root() {
    let config = config(
        "spark",
        "http://10.10.10.83:8000/v1",
        CustomModelPolicy::Discover,
    );
    assert_eq!(
        config.chat_url(),
        "http://10.10.10.83:8000/v1/chat/completions"
    );
    assert_eq!(config.models_url(), "http://10.10.10.83:8000/v1/models");
}

#[test]
fn an_explicit_model_list_is_served_without_touching_the_endpoint() {
    // The address is unroutable on purpose: an explicitly listed provider must
    // never probe, so it stays selectable while its server is down.
    let config = config(
        "offline",
        "http://127.0.0.1:1/v1",
        CustomModelPolicy::Explicit {
            ids: vec!["qwen3.8-27b".into(), "Meta-Llama-3.1-8B-Instruct".into()],
        },
    );
    let (models, error) = discover_blocking(&config);
    assert_eq!(error, None);
    let ids: Vec<&str> = models.iter().map(|model| model.model.as_str()).collect();
    // Byte for byte, casing included: the id is what goes back on the wire.
    assert_eq!(ids, vec!["qwen3.8-27b", "Meta-Llama-3.1-8B-Instruct"]);
    assert!(models
        .iter()
        .all(|model| model.serving_account_ids == vec!["acc_offline".to_string()]));
}

/// A one-shot HTTP server that answers the next request with `response`.
fn serve_once(response: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0; 8192];
        let _ = stream.read(&mut buffer);
        let _ = stream.write_all(response.as_bytes());
    });
    format!("http://{address}/v1")
}

fn http_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[test]
fn discovery_reads_the_openai_model_list_and_its_served_context_length() {
    let body = serde_json::json!({
        "object": "list",
        "data": [{
            "id": "qwen3.8-27b",
            "object": "model",
            "max_model_len": 131072
        }]
    })
    .to_string();
    let url = serve_once(http_response("200 OK", &body));
    let (models, error) = discover_blocking(&config("spark", &url, CustomModelPolicy::Discover));
    assert_eq!(error, None);
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].model, "qwen3.8-27b");
    assert_eq!(models[0].context_window, Some(131072));
    assert_eq!(
        models[0].wire_protocol,
        Some(crate::backends::DiscoveredWireProtocol::OpenAiChatCompletions)
    );
}

#[test]
fn a_refused_model_listing_reports_the_status_without_echoing_the_body() {
    // An endpoint can echo a rejected credential back in its error body, so
    // only the status crosses into the catalog error a panel will render.
    let url = serve_once(http_response(
        "401 Unauthorized",
        r#"{"error":{"message":"bad key sk-secret-value"}}"#,
    ));
    let (models, error) = discover_blocking(&config("spark", &url, CustomModelPolicy::Discover));
    assert!(models.is_empty());
    let error = error.expect("a refused listing is an error");
    assert!(error.contains("401"), "{error}");
    assert!(
        !error.contains("sk-secret-value"),
        "the upstream body must not reach diagnostics: {error}"
    );
}

#[test]
fn an_endpoint_that_lists_nothing_says_so_rather_than_looking_healthy() {
    let url = serve_once(http_response("200 OK", r#"{"object":"list","data":[]}"#));
    let (models, error) = discover_blocking(&config("spark", &url, CustomModelPolicy::Discover));
    assert!(models.is_empty());
    assert!(error
        .expect("empty is an error")
        .contains("list them explicitly"));
}

#[test]
fn two_custom_providers_can_serve_the_same_model_id() {
    // The same id on two endpoints is two different models. They stay apart
    // because a selection is `{backend, model}` and each provider's backend key
    // names its own account — which is what lets the ids stay unmangled.
    let (first, _) = discover_blocking(&config(
        "lab",
        "http://127.0.0.1:1/v1",
        CustomModelPolicy::Explicit {
            ids: vec!["qwen3.8-27b".into()],
        },
    ));
    let (second, _) = discover_blocking(&config(
        "desk",
        "http://127.0.0.1:2/v1",
        CustomModelPolicy::Explicit {
            ids: vec!["qwen3.8-27b".into()],
        },
    ));
    assert_eq!(first[0].model, second[0].model);
    assert_ne!(first[0].serving_account_ids, second[0].serving_account_ids);
    assert_ne!(
        catalog::custom_key(&first[0].serving_account_ids[0]),
        catalog::custom_key(&second[0].serving_account_ids[0])
    );
}

#[test]
fn an_endpoints_debug_never_renders_a_header_value() {
    // Headers carry bearer keys, so the derived Debug would put a credential
    // into any log line or panic that formatted an endpoint.
    let endpoint = Endpoint {
        provider_name: "Spark".into(),
        backend_key: "custom:acc_1".into(),
        chat_url: "http://host/v1/chat/completions".into(),
        headers: vec![
            ("authorization".into(), "Bearer sk-secret-value".into()),
            ("x-tenant".into(), "tenant-secret".into()),
        ],
        extra_body: None,
    };
    let rendered = format!("{endpoint:?}");
    assert!(!rendered.contains("sk-secret-value"), "{rendered}");
    assert!(!rendered.contains("tenant-secret"), "{rendered}");
    assert!(rendered.contains("authorization: <redacted>"), "{rendered}");
    assert!(rendered.contains("x-tenant: <redacted>"), "{rendered}");
}

#[test]
fn an_account_projection_shows_the_shape_of_a_provider_and_none_of_its_secrets() {
    let auth = endpoint(
        ProviderAuth::custom_endpoint(
            "http://10.10.10.83:8000",
            CustomProtocol::OpenAiChatCompletions,
            Some("sk-secret-value"),
            &[("x-tenant".to_string(), "tenant-secret".to_string())],
            CustomModelPolicy::Explicit {
                ids: vec!["qwen3.8-27b".into()],
            },
            CustomSampling::default(),
            32_768,
        )
        .unwrap(),
    );
    let info = crate::identity::AccountInfo::from(&account("acc_1", "Spark", auth));
    let rendered = serde_json::to_string(&info).unwrap();
    assert!(!rendered.contains("sk-secret-value"), "{rendered}");
    assert!(!rendered.contains("tenant-secret"), "{rendered}");
    // The shape is still legible: which headers are set, that a key exists, how
    // models are known, and what the endpoint resolved to.
    assert!(rendered.contains("x-tenant"), "{rendered}");
    assert!(rendered.contains("\"hasApiKey\":true"), "{rendered}");
    assert!(
        rendered.contains("\"modelDiscovery\":\"explicit\""),
        "{rendered}"
    );
    assert!(
        rendered.contains("http://10.10.10.83:8000/v1"),
        "the resolved API root is shown, not re-derived later: {rendered}"
    );
    assert!(
        rendered.contains("custom:acc_1"),
        "the account serves its own backend: {rendered}"
    );
}
