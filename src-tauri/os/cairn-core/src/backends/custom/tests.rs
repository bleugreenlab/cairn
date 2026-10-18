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
use crate::backends::openai_compat::http::Endpoint;
use crate::backends::openai_compat::wire::{ChatContent, ChatStreamChunk, StreamingAggregate};
use crate::credentials::{
    ApiProvider, CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAccount, ProviderAuth,
};
use cairn_models::types::catalog;
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

/// The body of one owned-loop generation for a provider with `sampling` and
/// `max_output_tokens`.
fn generation_body(sampling: CustomSampling, max_output_tokens: i64) -> serde_json::Value {
    use crate::backends::http_loop::{Connection, WireAdapter};
    use cairn_models::turn::ProtocolFamily;
    let mut provider = config("vLLM", "http://127.0.0.1:1/v1", CustomModelPolicy::Discover);
    provider.sampling = sampling;
    provider.max_output_tokens = max_output_tokens;
    let family = super::adapter::CustomAdapter::new(provider)
        .family(&Connection { api_key: None })
        .unwrap();
    family.body(
        "gemma-4-26b",
        &[crate::backends::openai_compat::wire::ChatMessage::user(
            "hi".to_string(),
        )],
        &cairn_models::wire::GenerationConfig {
            tools: cairn_models::wire::cairn_tool_definitions(),
            ..Default::default()
        },
    )
}

#[test]
fn sampling_is_forwarded_only_when_the_operator_configures_it() {
    let absent = generation_body(CustomSampling::default(), 32_768);
    for key in ["repetition_penalty", "temperature", "top_p", "top_k"] {
        assert!(
            absent.get(key).is_none(),
            "{key} must not acquire a default"
        );
    }

    let configured = generation_body(
        CustomSampling {
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
    let body = generation_body(CustomSampling::default(), 65_536);
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
        aggregate.reasoning(),
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
    assert!(
        !aggregate.reasoning().is_empty(),
        "tokens went into thinking"
    );
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
        embeddings: Vec::new(),
        images: None,
    }
}

#[test]
fn an_images_provider_draws_and_does_nothing_else() {
    use cairn_models::interface::decide::measured_by;
    use cairn_models::types::catalog::{VerbKind, Wire};
    use cairn_models::types::image::ImageCapabilities;

    let mut images = config(
        "sparks",
        "http://spark:8080/v1",
        CustomModelPolicy::Explicit {
            ids: vec!["qwen-image".into()],
        },
    );
    images.protocol = CustomProtocol::OpenAiImages;
    let serves = images.serves();
    assert_eq!(serves.len(), 1);
    assert_eq!(serves[0].wire, Wire::OpenAiImages);
    assert_eq!(serves[0].verb.kind(), VerbKind::Image);
    // Declared nothing: the minimal Images contract.
    assert_eq!(
        serves[0].verb.image(),
        Some(&ImageCapabilities::openai_images_minimal())
    );
    images.images = Some(ImageCapabilities::openai_gpt_image());
    assert_eq!(
        images.serves()[0].verb.image(),
        Some(&ImageCapabilities::openai_gpt_image())
    );
    assert!(images
        .require_turns()
        .unwrap_err()
        .contains("draws images only"));
    assert!(images
        .decision_route()
        .unwrap_err()
        .contains("cannot answer a decision"));
    assert_eq!(measured_by(Wire::OpenAiImages), None);
    assert_eq!(measured_by(Wire::OpenRouterImages), None);
    assert!(discover_blocking(&images).1.is_none());
}

fn account(id: &str, label: &str, auth: ProviderAuth) -> ProviderAccount {
    ProviderAccount {
        id: id.to_string(),
        label: label.to_string(),
        api_provider: ApiProvider::Custom,
        source: crate::credentials::AccountSource::Configured,
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

/// Answer one request with `response` and hand back the request body exactly
/// as it crossed the socket.
fn serve_once_capturing(response: String) -> (String, std::sync::mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut raw = Vec::new();
        let mut chunk = [0u8; 4096];
        let body = loop {
            let read = stream.read(&mut chunk).unwrap();
            raw.extend_from_slice(&chunk[..read]);
            let text = String::from_utf8_lossy(&raw).to_string();
            let Some(head_end) = text.find("\r\n\r\n") else {
                continue;
            };
            let length = text[..head_end]
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap();
            if raw.len() >= head_end + 4 + length || read == 0 {
                break text[head_end + 4..].to_string();
            }
        };
        let _ = stream.write_all(response.as_bytes());
        let _ = sender.send(body);
    });
    (format!("http://{address}/v1"), received)
}

/// The order `keys` first appear in `text`; each must be unique in it.
fn wire_order<'a>(text: &str, keys: &[&'a str]) -> Vec<&'a str> {
    let mut positioned: Vec<(usize, &str)> = keys
        .iter()
        .map(|key| (text.find(&format!("\"{key}\":")).unwrap(), *key))
        .collect();
    positioned.sort();
    positioned.into_iter().map(|(_, key)| key).collect()
}

#[test]
fn a_custom_provider_receives_a_preset_schema_required_first() {
    // The tasklist preset is what a planning call sends a vLLM endpoint. Sorted,
    // its task object put `agent` first and `id` fourth with the optional
    // `backend` and `dependencies` between them, so a model writing `id` next
    // silently dropped `dependencies` and the plan lost its DAG.
    use crate::backends::{CompletionMessage, CompletionRequest, CompletionRole};
    let reply = r#"{"choices":[{"message":{"role":"assistant","content":"{}"}}]}"#;
    let (base_url, sent) = serve_once_capturing(http_response("200 OK", reply));
    let adapter =
        super::adapter::CustomAdapter::new(config("vLLM", &base_url, CustomModelPolicy::Discover));
    let schema = crate::output_schemas::resolve_output_schema(
        None,
        &crate::models::OutputSchema::Preset("tasklist".into()),
    )
    .unwrap();
    adapter
        .complete(
            &cairn_models::complete::HttpComplete,
            CompletionRequest {
                system: None,
                messages: vec![CompletionMessage {
                    role: CompletionRole::User,
                    content: "plan it".into(),
                }],
                model: "qwen".into(),
                project_id: None,
                extras: serde_json::Value::Null,
                output_schema: Some(schema),
                timeout: std::time::Duration::from_secs(5),
            },
        )
        .unwrap();
    let sent = sent
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        wire_order(&sent, &["objective", "tasks", "requirements"]),
        ["objective", "tasks", "requirements"],
        "{sent}"
    );
    assert_eq!(
        wire_order(
            &sent,
            &[
                "id",
                "title",
                "agent",
                "prompt",
                "backend",
                "dependencies",
                "tier"
            ]
        ),
        [
            "id",
            "title",
            "agent",
            "prompt",
            "backend",
            "dependencies",
            "tier"
        ],
        "{sent}"
    );
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
    // The provider's configured protocol declares what every model serves.
    assert_eq!(models[0].serves, None);
}

#[test]
fn discovery_selects_a_pinned_tokenizer_from_the_served_root_or_id() {
    let body = serde_json::json!({
        "object": "list",
        "data": [
            {"id": "qwen3.8-27b", "object": "model", "root": "unsloth/Qwen3.8-27B-NVFP4"},
            {"id": "Qwen/Qwen3.8-27B", "object": "model"},
            {"id": "gemma-4-26b", "object": "model", "root": "/models/gemma-4-26b"},
            {"id": "Qwen/Qwen3.8-27B", "object": "model", "root": "/models/my-finetune"},
            {"id": "Qwen/Qwen3.8-27B", "object": "model", "root": "someone/Qwen3.8-27B-merged"}
        ]
    })
    .to_string();
    let url = serve_once(http_response("200 OK", &body));
    let (models, error) = discover_blocking(&config("spark", &url, CustomModelPolicy::Discover));
    assert_eq!(error, None);
    let digest = |index: usize| {
        models[index]
            .tokenizer
            .as_ref()
            .map(|tokenizer| tokenizer.sha256)
    };
    let pin = |repo: &str| {
        cairn_models::types::tokenizers::pins()
            .iter()
            .find(|pin| pin.repo == repo)
            .map(|pin| pin.digest_bytes())
    };
    // A quantized repack is its own pin, selected by the root vLLM reports.
    assert_eq!(digest(0), pin("unsloth/Qwen3.8-27B-NVFP4"));
    assert_eq!(digest(1), pin("Qwen/Qwen3.8-27B"));
    // A local path identifies nothing Cairn pinned.
    assert_eq!(digest(2), None);
    // A reported root is authoritative: a served id that names a pinned
    // repository does not override a root that names something else.
    assert_eq!(digest(3), None);
    assert_eq!(digest(4), None);
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

fn speech_config(protocol: CustomProtocol, ids: &[&str]) -> CustomProviderConfig {
    CustomProviderConfig {
        protocol,
        ..config(
            "voice",
            "http://127.0.0.1:1/v1",
            CustomModelPolicy::Explicit {
                ids: ids.iter().map(|id| id.to_string()).collect(),
            },
        )
    }
}

#[test]
fn a_speech_provider_speaks_per_model_and_serves_nothing_else() {
    use cairn_models::interface::speak::{InputStreaming, StyleControl};
    use cairn_models::types::catalog::{Verb, Wire};

    let openai = speech_config(CustomProtocol::OpenAiSpeech, &["kokoro", "gpt-4o-mini-tts"]);
    assert_eq!(openai.wire(), Wire::OpenAiSpeech);
    assert!(openai.serves().is_empty(), "nothing provider-wide");
    // The endpoint is down, so the declared models stand on their own.
    let (models, error) = discover_blocking(&openai);
    assert!(
        error
            .as_deref()
            .is_some_and(|e| e.ends_with("the declared models are still offered")),
        "{error:?}"
    );
    let speaks = |index: usize| {
        let served = models[index].serves.as_ref().expect("declared per model");
        assert_eq!(served.len(), 1);
        assert_eq!(served[0].wire, Wire::OpenAiSpeech);
        match &served[0].verb {
            Verb::Speak(capabilities) => capabilities.clone(),
            other => panic!("expected speak, got {other:?}"),
        }
    };
    assert_eq!(speaks(0).input, InputStreaming::Segmented);
    assert_eq!(speaks(0).style, StyleControl::None);
    assert_eq!(speaks(1).style, StyleControl::Instructions);

    let eleven = speech_config(CustomProtocol::ElevenLabs, &["eleven_flash_v2_5"]);
    assert_eq!(eleven.wire(), Wire::ElevenLabsStreamInput);
    let (models, _) = discover_blocking(&eleven);
    assert!(matches!(
        &models[0].serves.as_ref().unwrap()[0].verb,
        Verb::Speak(capabilities) if capabilities.input == InputStreaming::Native
    ));

    for config in [&openai, &eleven] {
        let refusal = config.require_turns().unwrap_err();
        assert!(refusal.contains("speaks speech only"), "{refusal}");
        let refusal = config.decision_route().unwrap_err();
        assert!(refusal.contains("cannot answer a decision"), "{refusal}");
    }
    // A chat provider's models still declare nothing of their own.
    let (chat, _) = discover_blocking(&config(
        "chat",
        "http://127.0.0.1:1/v1",
        CustomModelPolicy::Explicit {
            ids: vec!["qwen3".into()],
        },
    ));
    assert_eq!(chat[0].serves, None);
}

/// Kokoro-FastAPI's and OpenAI's listings mixed: chat, transcription,
/// embedding and speech ids.
const OPENAI_MIXED_MODELS: &str = r#"{"object":"list","data":[
  {"id":"gpt-4o","object":"model"},
  {"id":"tts-1","object":"model"},
  {"id":"whisper-1","object":"model"},
  {"id":"gpt-4o-mini-tts","object":"model"},
  {"id":"text-embedding-3-small","object":"model"},
  {"id":"kokoro","object":"model"}
]}"#;

/// ElevenLabs' `GET /v1/models` shape: speech models and a voice-conversion
/// one.
const ELEVENLABS_MIXED_MODELS: &str = r#"[
  {"model_id":"eleven_flash_v2_5","name":"Eleven Flash v2.5","description":"Ultra low latency","can_do_text_to_speech":true,"can_do_voice_conversion":false},
  {"model_id":"eleven_multilingual_sts_v2","name":"Eleven Multilingual STS","can_do_text_to_speech":false,"can_do_voice_conversion":true},
  {"model_id":"eleven_multilingual_v2","name":"Eleven Multilingual v2","can_do_text_to_speech":true}
]"#;

fn discovering(protocol: CustomProtocol, base: &str, declared: &[&str]) -> CustomProviderConfig {
    CustomProviderConfig {
        base_url: base.to_string(),
        models: if declared.is_empty() {
            CustomModelPolicy::Discover
        } else {
            CustomModelPolicy::Explicit {
                ids: declared.iter().map(|id| id.to_string()).collect(),
            }
        },
        ..speech_config(protocol, &[])
    }
}

fn ids(models: &[cairn_models::types::catalog::DiscoveredModel]) -> Vec<&str> {
    models.iter().map(|model| model.model.as_str()).collect()
}

#[test]
fn an_openai_speech_provider_discovers_only_its_speech_models() {
    let base = serve_once(http_response("200 OK", OPENAI_MIXED_MODELS));
    let (models, error) = discover_blocking(&discovering(CustomProtocol::OpenAiSpeech, &base, &[]));
    assert_eq!(error, None);
    assert_eq!(ids(&models), vec!["tts-1", "gpt-4o-mini-tts", "kokoro"]);
    assert!(models.iter().all(|model| model.serves.is_some()));
}

#[test]
fn an_elevenlabs_provider_discovers_only_models_that_speak() {
    let base = serve_once(http_response("200 OK", ELEVENLABS_MIXED_MODELS));
    let (models, error) = discover_blocking(&discovering(CustomProtocol::ElevenLabs, &base, &[]));
    assert_eq!(error, None);
    assert_eq!(
        ids(&models),
        vec!["eleven_flash_v2_5", "eleven_multilingual_v2"]
    );
    assert_eq!(models[0].display_name, "Eleven Flash v2.5");
    assert_eq!(models[0].description.as_deref(), Some("Ultra low latency"));
}

#[test]
fn declared_speech_models_add_to_discovery_without_duplicates() {
    let base = serve_once(http_response("200 OK", OPENAI_MIXED_MODELS));
    let (models, error) = discover_blocking(&discovering(
        CustomProtocol::OpenAiSpeech,
        &base,
        &["kokoro", "my-custom-voice-model"],
    ));
    assert_eq!(error, None);
    assert_eq!(
        ids(&models),
        vec![
            "tts-1",
            "gpt-4o-mini-tts",
            "kokoro",
            "my-custom-voice-model"
        ]
    );
    // An unrecognised declared id is still a speech model on this wire.
    assert!(models[3].serves.is_some());
}

#[test]
fn a_failed_speech_listing_keeps_the_declared_models() {
    let base = serve_once(http_response("503 Service Unavailable", "{}"));
    let (models, error) = discover_blocking(&discovering(
        CustomProtocol::OpenAiSpeech,
        &base,
        &["kokoro"],
    ));
    assert_eq!(ids(&models), vec!["kokoro"]);
    assert_eq!(
        error.as_deref(),
        Some("model listing returned HTTP 503; the declared models are still offered")
    );
    // With nothing declared, the failure is the whole answer.
    let base = serve_once(http_response("200 OK", r#"{"data":[{"id":"gpt-4o"}]}"#));
    let (models, error) = discover_blocking(&discovering(CustomProtocol::OpenAiSpeech, &base, &[]));
    assert!(models.is_empty());
    assert!(error.unwrap().contains("no speech model Cairn recognises"));
}

/// A live discovery pass against a running OpenAI-compatible TTS server (for
/// example Kokoro-FastAPI): `SPEECH_DISCOVERY_URL=http://host:8880/v1 cargo
/// test -p cairn-core --lib live_speech_discovery -- --ignored --nocapture`.
#[test]
#[ignore = "needs a running TTS server at SPEECH_DISCOVERY_URL"]
fn live_speech_discovery() {
    let base = std::env::var("SPEECH_DISCOVERY_URL").expect("SPEECH_DISCOVERY_URL");
    let (models, error) = discover_blocking(&discovering(CustomProtocol::OpenAiSpeech, &base, &[]));
    for model in &models {
        let served = model.serves.as_ref().expect("declared per model");
        println!(
            "discovered {} serves {}",
            model.model,
            serde_json::to_string(served).unwrap()
        );
    }
    assert_eq!(error, None);
    assert!(!models.is_empty());
}

#[test]
fn a_speech_provider_may_discover_or_declare() {
    for protocol in [CustomProtocol::OpenAiSpeech, CustomProtocol::ElevenLabs] {
        for models in [
            CustomModelPolicy::Discover,
            CustomModelPolicy::Explicit {
                ids: vec!["kokoro".into()],
            },
        ] {
            ProviderAuth::custom_endpoint(
                "http://localhost:8880",
                protocol,
                None,
                &[],
                models,
                CustomSampling::default(),
                32_768,
            )
            .unwrap();
        }
    }
}

#[test]
fn elevenlabs_authenticates_with_its_own_header() {
    let operator = vec![("x-team".to_string(), "t".to_string())];
    assert_eq!(
        super::request_headers(CustomProtocol::ElevenLabs, Some("xi-key"), &operator),
        vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("xi-api-key".to_string(), "xi-key".to_string()),
            ("x-team".to_string(), "t".to_string()),
        ]
    );
    for protocol in [
        CustomProtocol::OpenAiChatCompletions,
        CustomProtocol::Decisions,
        CustomProtocol::OpenAiSpeech,
    ] {
        assert_eq!(
            super::request_headers(protocol, Some("sk"), &[])[1],
            ("authorization".to_string(), "Bearer sk".to_string())
        );
    }
    assert_eq!(
        super::request_headers(CustomProtocol::OpenAiSpeech, None, &[]).len(),
        1
    );
}

#[test]
fn every_custom_protocol_round_trips_its_name() {
    for protocol in CustomProtocol::ALL {
        assert_eq!(CustomProtocol::parse(protocol.as_str()), Some(protocol));
        let json = serde_json::to_value(protocol).unwrap();
        assert_eq!(json, protocol.as_str());
        assert_eq!(
            serde_json::from_value::<CustomProtocol>(json).unwrap(),
            protocol
        );
    }
    assert_eq!(CustomProtocol::parse("openai-video"), None);
    assert!(CustomProtocol::OpenAiSpeech.is_speech() && CustomProtocol::ElevenLabs.is_speech());
    assert!(!CustomProtocol::Decisions.is_speech());
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
    let info = crate::credentials::AccountInfo::from(&account("acc_1", "Spark", auth));
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

/// Byte-exact request bodies of a custom provider's owned turn. See
/// `http_loop::scripted` for how they are recorded.
mod golden {
    use super::super::adapter::CustomAdapter;
    use super::config;
    use crate::backends::http_loop::scripted::{
        answer_schema, assert_golden, chat_text, record_turn, session_config,
    };
    use crate::credentials::{CustomModelPolicy, CustomSampling};

    #[tokio::test]
    async fn owned_turn_without_sampling() {
        let provider = config("vLLM", "http://127.0.0.1:1/v1", CustomModelPolicy::Discover);
        let bodies = record_turn(
            session_config("gemma-4-26b"),
            vec![chat_text("done")],
            |_| CustomAdapter::new(provider),
        )
        .await;
        assert_golden(
            &bodies,
            &[
                r#"{"max_tokens":32768,"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"gemma-4-26b","stream":true,"stream_options":{"include_usage":true},"tool_choice":"auto","tools":["read","write","run"]}"#,
            ],
        );
    }

    /// The schema and effort are the caller's, and the owned loop sends
    /// neither to a custom provider today.
    #[tokio::test]
    async fn owned_turn_with_all_sampling_keys() {
        let mut provider = config("vLLM", "http://127.0.0.1:1/v1", CustomModelPolicy::Discover);
        provider.sampling = CustomSampling {
            repetition_penalty: Some(1.1),
            temperature: Some(0.7),
            top_p: Some(0.9),
            top_k: Some(40),
        };
        let mut session = session_config("gemma-4-26b");
        session.reasoning_effort = Some("high".to_string());
        session.output_schema = Some(answer_schema());
        let bodies = record_turn(session, vec![chat_text(r#"{"answer":"done"}"#)], |_| {
            CustomAdapter::new(provider)
        })
        .await;
        assert_golden(
            &bodies,
            &[
                r#"{"max_tokens":32768,"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"gemma-4-26b","repetition_penalty":1.1,"stream":true,"stream_options":{"include_usage":true},"temperature":0.7,"tool_choice":"auto","tools":["read","write","run"],"top_k":40,"top_p":0.9}"#,
            ],
        );
    }
}
