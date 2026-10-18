use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use serde_json::json;

use super::gateway::{self, GatewayEmbedEndpoint};
use super::openai::{self, EmbeddingsEndpoint};
use crate::{upstream_error_detail, CompletionError};

const HUB_200: &str = include_str!("testdata/hub-200.json");
const OLLAMA_200: &str = include_str!("testdata/ollama-200.json");
const OPENROUTER_200: &str = include_str!("testdata/openrouter-200.json");
const OPENAI_200: &str = include_str!("testdata/openai-200.json");
const GATEWAY_200: &str = include_str!("testdata/gateway-200.json");

/// Serve one request with `status` and `reply`, and hand back the request
/// line, headers, and body exactly as they crossed the socket.
fn serve_once(
    status: &'static str,
    reply: &'static str,
) -> (String, std::sync::mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, received) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut raw = Vec::new();
        let mut chunk = [0u8; 8192];
        let request = loop {
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
                .unwrap_or(0);
            if raw.len() >= head_end + 4 + length || read == 0 {
                break text;
            }
        };
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
            reply.len()
        );
        let _ = sender.send(request);
    });
    (format!("http://{address}/v1/embeddings"), received)
}

fn sent_body(request: &str) -> serde_json::Value {
    serde_json::from_str(&request[request.find("\r\n\r\n").unwrap() + 4..]).unwrap()
}

fn endpoint(url: String) -> EmbeddingsEndpoint {
    EmbeddingsEndpoint {
        provider_name: "Hub".into(),
        backend_key: "custom:acc_hub".into(),
        url,
        headers: vec![("authorization".into(), "Bearer sk-test".into())],
    }
}

fn gateway_endpoint(url: String) -> GatewayEmbedEndpoint {
    GatewayEmbedEndpoint {
        provider_name: "Cairn gateway".into(),
        backend_key: "cairn".into(),
        url,
        headers: vec![("authorization".into(), "Bearer device-jwt".into())],
    }
}

fn texts(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn a_body_sends_dimensions_and_input_type_only_when_set_and_always_floats() {
    let bare = openai::build_body("m", &texts(&["a", "b"]), None, None);
    assert_eq!(
        bare,
        json!({"model": "m", "input": ["a", "b"], "encoding_format": "float"})
    );
    let full = openai::build_body("m", &texts(&["a"]), Some(256), Some("search_query"));
    assert_eq!(full["dimensions"], 256);
    assert_eq!(full["input_type"], "search_query");
    assert_eq!(full["encoding_format"], "float");

    let gateway = gateway::build_body(&texts(&["a"]), "search_document");
    assert_eq!(
        gateway,
        json!({"texts": ["a"], "input_type": "search_document"})
    );
    assert!(gateway.get("dims").is_none(), "the gateway ignores dims");
}

#[test]
fn the_post_goes_to_the_embeddings_path_with_the_bearer_key() {
    let (url, request) = serve_once("200 OK", HUB_200);
    let body = openai::build_body("qwen/qwen3-embedding-4b", &texts(&["x", "y"]), None, None);
    let reply = openai::post_embeddings(&endpoint(url), body, Duration::from_secs(5)).unwrap();
    assert_eq!(reply.data.len(), 2);
    assert_eq!(reply.model.as_deref(), Some("qwen3-embedding-4b"));
    assert_eq!(reply.usage.as_ref().unwrap().prompt_tokens, Some(7));
    assert_eq!(reply.usage.as_ref().unwrap().cost, None);
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(request.starts_with("POST /v1/embeddings "), "{request}");
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer sk-test"),
        "{request}"
    );
    assert_eq!(sent_body(&request)["model"], "qwen/qwen3-embedding-4b");
}

#[test]
fn replies_decode_out_of_order_with_or_without_usage_and_cost() {
    let openrouter = openai::read(OPENROUTER_200).unwrap();
    assert_eq!(
        openrouter
            .data
            .iter()
            .map(|item| item.index)
            .collect::<Vec<_>>(),
        vec![1, 0]
    );
    assert_eq!(openrouter.usage.unwrap().cost, Some(0.00000018));

    let ollama = openai::read(OLLAMA_200).unwrap();
    assert_eq!(ollama.model.as_deref(), Some("nomic-embed-text:latest"));
    assert_eq!(ollama.usage.unwrap().prompt_tokens, Some(10));

    let openai_reply = openai::read(OPENAI_200).unwrap();
    assert_eq!(openai_reply.data[0].embedding.len(), 3);

    let bare = openai::read(r#"{"data":[{"index":0,"embedding":[1.0]}]}"#).unwrap();
    assert_eq!(bare.usage, None);
    assert_eq!(bare.model, None);

    let base64 = openai::read(r#"{"data":[{"index":0,"embedding":"AAAA"}]}"#).unwrap_err();
    assert!(
        matches!(&base64, CompletionError::InvalidResponse(text) if text.contains("unreadable embeddings reply")),
        "{base64:?}"
    );
}

#[test]
fn the_gateway_posts_its_own_body_and_reads_dims_and_model() {
    let (url, request) = serve_once("200 OK", GATEWAY_200);
    let reply = gateway::post_gateway_embed(
        &gateway_endpoint(url.replace("/v1/embeddings", "/embed")),
        gateway::build_body(&texts(&["a", "b"]), "search_query"),
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(reply.model, "cohere-embed-v4");
    assert_eq!(reply.dims, 3);
    assert_eq!(reply.embeddings.len(), 2);
    assert_eq!(reply.input_tokens, Some(4));
    let request = request.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(request.starts_with("POST /embed "), "{request}");
    assert!(
        request
            .to_lowercase()
            .contains("authorization: bearer device-jwt"),
        "{request}"
    );
    assert_eq!(sent_body(&request)["input_type"], "search_query");
}

fn openai_error(status: &'static str, reply: &'static str) -> CompletionError {
    let (url, _) = serve_once(status, reply);
    openai::post_embeddings(&endpoint(url), json!({}), Duration::from_secs(5)).unwrap_err()
}

fn gateway_error(status: &'static str, reply: &'static str) -> CompletionError {
    let (url, _) = serve_once(status, reply);
    gateway::post_gateway_embed(&gateway_endpoint(url), json!({}), Duration::from_secs(5))
        .unwrap_err()
}

fn upstream(text: &str) -> CompletionError {
    CompletionError::Upstream(text.to_string())
}

#[test]
fn every_measured_error_body_keeps_its_status_and_the_providers_sentence() {
    // The hub's own errors: numeric `code`.
    assert_eq!(
        openai_error(
            "401 Unauthorized",
            r#"{"error":{"message":"missing or invalid bgllm key","type":"authentication_error","code":401}}"#
        ),
        upstream("Hub embeddings returned HTTP 401: missing or invalid bgllm key")
    );
    assert_eq!(
        openai_error(
            "503 Service Unavailable",
            r#"{"error":{"message":"openai/text-embedding-3-small cannot be served right now: local: no bginf profile serves this model; openrouter: client mitch is local-only","type":"model_unavailable","code":503}}"#
        ),
        upstream("Hub embeddings returned HTTP 503: openai/text-embedding-3-small cannot be served right now: local: no bginf profile serves this model; openrouter: client mitch is local-only")
    );
    assert_eq!(
        openai_error(
            "404 Not Found",
            r#"{"error":{"message":"no provider serves nope/embed-xyz","type":"model_not_found","code":404}}"#
        ),
        upstream("Hub embeddings returned HTTP 404: no provider serves nope/embed-xyz (is this an embedding model?)")
    );
    // llama.cpp behind the hub.
    assert_eq!(
        openai_error(
            "400 Bad Request",
            r#"{"error":{"code":400,"message":"\"input\" must not be empty","type":"invalid_request_error"}}"#
        ),
        upstream(r#"Hub embeddings returned HTTP 400: "input" must not be empty"#)
    );
    // vLLM (FastAPI) answering a chat model on /embeddings.
    assert_eq!(
        openai_error("404 Not Found", r#"{"detail":"Not Found"}"#),
        upstream("Hub embeddings returned HTTP 404: Not Found (is this an embedding model?)")
    );
    // Ollama: OpenAI's full shape with null code.
    assert_eq!(
        openai_error(
            "404 Not Found",
            r#"{"error":{"message":"model \"nope-embed\" not found, try pulling it first","type":"not_found_error","param":null,"code":null}}"#
        ),
        upstream(
            r#"Hub embeddings returned HTTP 404: model "nope-embed" not found, try pulling it first (is this an embedding model?)"#
        )
    );
    assert_eq!(
        openai_error(
            "501 Not Implemented",
            r#"{"error":{"message":"This server does not support embeddings. Start it with `--embeddings`","type":"api_error","param":null,"code":null}}"#
        ),
        upstream("Hub embeddings returned HTTP 501: This server does not support embeddings. Start it with `--embeddings` (is this an embedding model?)")
    );
    // OpenRouter: 402 stays distinguishable from 429.
    assert_eq!(
        openai_error(
            "402 Payment Required",
            r#"{"error":{"code":402,"message":"Insufficient credits"}}"#
        ),
        upstream("Hub embeddings returned HTTP 402: Insufficient credits")
    );
    // OpenAI: string `code`.
    assert_eq!(
        openai_error(
            "400 Bad Request",
            r#"{"error":{"message":"This model does not support specifying dimensions.","type":"invalid_request_error","param":null,"code":"invalid_value"}}"#
        ),
        upstream(
            "Hub embeddings returned HTTP 400: This model does not support specifying dimensions."
        )
    );
}

#[test]
fn the_gateways_error_bodies_keep_their_sentence() {
    assert_eq!(
        gateway_error(
            "401 Unauthorized",
            r#"{"error":"Invalid or expired token"}"#
        ),
        upstream("Cairn gateway embed returned HTTP 401: Invalid or expired token")
    );
    assert_eq!(
        gateway_error(
            "400 Bad Request",
            r#"{"error":"input_type is required and must be one of: search_document, search_query"}"#
        ),
        upstream("Cairn gateway embed returned HTTP 400: input_type is required and must be one of: search_document, search_query")
    );
    assert_eq!(
        gateway_error("500 Internal Server Error", "Internal Server Error"),
        upstream("Cairn gateway embed returned HTTP 500: Internal Server Error")
    );
}

#[test]
fn the_shared_error_reader_takes_every_family_s_shape() {
    assert_eq!(upstream_error_detail(r#"{"error":{"message":"m"}}"#), "m");
    assert_eq!(upstream_error_detail(r#"{"error":"plain"}"#), "plain");
    assert_eq!(
        upstream_error_detail(r#"{"detail":"Not Found"}"#),
        "Not Found"
    );
    assert_eq!(upstream_error_detail(r#"{"message":"top"}"#), "top");
    // FastAPI validation errors carry a list, which is kept whole.
    let list = r#"{"detail":[{"loc":["body"],"msg":"field required"}]}"#;
    assert_eq!(upstream_error_detail(list), list);
    assert_eq!(upstream_error_detail("oops"), "oops");
    assert_eq!(
        upstream_error_detail(r#"{"error":"  "}"#),
        r#"{"error":"  "}"#
    );
}

#[test]
fn a_server_that_never_answers_is_a_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_secs(3));
        drop(stream);
    });
    let error = openai::post_embeddings(
        &endpoint(format!("http://{address}/v1/embeddings")),
        json!({}),
        Duration::from_millis(200),
    )
    .unwrap_err();
    assert_eq!(error, CompletionError::Timeout);
}

#[test]
fn neither_endpoint_ever_prints_its_secret() {
    let printed = format!("{:?}", endpoint("http://x/v1/embeddings".into()));
    assert!(!printed.contains("sk-test"), "{printed}");
    assert!(printed.contains("authorization: <redacted>"), "{printed}");
    let printed = format!("{:?}", gateway_endpoint("http://x/embed".into()));
    assert!(!printed.contains("device-jwt"), "{printed}");
    assert!(printed.contains("authorization: <redacted>"), "{printed}");
}
