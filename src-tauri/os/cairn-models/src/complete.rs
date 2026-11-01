//! The facet's HTTP completion: the one caller of the protocol families' POSTs.

use crate::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, CompletionWire, Delivery,
};
use crate::interface::relay::{prepare_body, Relay, RelayBody, RelayCall, RelayError, Relayed};
use crate::relay::TapMode;
use crate::wire::{
    anthropic_messages, decisions, embeddings, openai_compat, openai_responses, systemone,
};

/// Sends each completion straight to its provider over the wire it names.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpComplete;

impl Complete for HttpComplete {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        let CompletionCall {
            wire,
            body,
            delivery,
        } = call;
        match (wire, delivery) {
            (CompletionWire::Chat(endpoint), Delivery::Stream { sink, cancel }) => {
                openai_compat::http::post_chat_completion_streaming(endpoint, body, cancel, sink)
                    .map(CompletionReply::Chat)
                    .map_err(CompletionError::Stream)
            }
            (CompletionWire::Chat(endpoint), Delivery::Whole { timeout }) => {
                openai_compat::http::post_chat_completion(endpoint, body, timeout)
                    .map(CompletionReply::Chat)
            }
            (CompletionWire::Messages(endpoint), Delivery::Stream { sink, cancel }) => {
                anthropic_messages::http::post_message_streaming(endpoint, body, cancel, sink)
                    .map(CompletionReply::Messages)
                    .map_err(CompletionError::Stream)
            }
            (CompletionWire::Messages(endpoint), Delivery::Whole { timeout }) => {
                anthropic_messages::http::post_message(endpoint, body, timeout)
                    .map(CompletionReply::Messages)
            }
            (CompletionWire::Responses(endpoint), Delivery::Stream { sink, cancel }) => {
                openai_responses::http::post_response_streaming(endpoint, body, cancel, sink)
                    .map(CompletionReply::Responses)
                    .map_err(CompletionError::Stream)
            }
            (CompletionWire::Responses(endpoint), Delivery::Whole { timeout }) => {
                openai_responses::http::post_response(endpoint, body, timeout)
                    .map(CompletionReply::Responses)
            }
            (CompletionWire::Decisions(endpoint), Delivery::Whole { timeout }) => {
                decisions::http::post_decisions(endpoint, body, timeout)
                    .map(CompletionReply::Decisions)
            }
            (CompletionWire::SystemOne(endpoint), Delivery::Whole { timeout }) => {
                systemone::http::post_systemone(endpoint, body, timeout)
                    .map(CompletionReply::SystemOne)
            }
            (
                CompletionWire::Decisions(endpoint) | CompletionWire::SystemOne(endpoint),
                Delivery::Stream { .. },
            ) => Err(CompletionError::InvalidRequest(format!(
                "{} answers a decision as one whole reply; a decision is never streamed",
                endpoint.provider_name
            ))),
            (CompletionWire::Embeddings(endpoint), Delivery::Whole { timeout }) => {
                embeddings::openai::post_embeddings(endpoint, body, timeout)
                    .map(CompletionReply::Embeddings)
            }
            (CompletionWire::GatewayEmbed(endpoint), Delivery::Whole { timeout }) => {
                embeddings::gateway::post_gateway_embed(endpoint, body, timeout)
                    .map(CompletionReply::GatewayEmbed)
            }
            (CompletionWire::Embeddings(endpoint), Delivery::Stream { .. }) => {
                Err(CompletionError::InvalidRequest(format!(
                    "{} answers embeddings as one whole reply; embeddings are never streamed",
                    endpoint.provider_name
                )))
            }
            (CompletionWire::GatewayEmbed(endpoint), Delivery::Stream { .. }) => {
                Err(CompletionError::InvalidRequest(format!(
                    "{} answers embeddings as one whole reply; embeddings are never streamed",
                    endpoint.provider_name
                )))
            }
        }
    }
}

/// How long a relayed call may run in all, a stream included.
const RELAY_ABSOLUTE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);
const RELAY_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Headers that describe one hop, not the reply, plus `content-length`, which
/// the tap may make untrue by removing a chunk.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "transfer-encoding",
    "content-length",
    "upgrade",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
];

const REQUEST_ID_HEADERS: &[&str] = &["x-request-id", "request-id", "x-generation-id"];

impl Relay for HttpComplete {
    fn relay(&self, call: RelayCall) -> Result<Relayed, RelayError> {
        let RelayCall {
            door,
            route,
            body,
            cancel,
        } = call;
        let prepared = prepare_body(door, &route.sent_model, body)?;
        let client = cairn_tls::blocking_client_builder()
            .timeout(RELAY_ABSOLUTE_TIMEOUT)
            .connect_timeout(RELAY_CONNECT_TIMEOUT)
            .build()
            .map_err(|error| {
                RelayError::Unreachable(format!(
                    "Failed to build the {} client: {error}",
                    route.provider_name
                ))
            })?;
        let mut request = client
            .post(route.url(door))
            .header("content-type", "application/json");
        for (name, value) in &route.headers {
            if !name.eq_ignore_ascii_case("content-type") {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        let response = request
            .body(prepared.body.to_string())
            .send()
            .map_err(|error| {
                RelayError::Unreachable(format!("{} unreachable: {error}", route.provider_name))
            })?;
        let status = response.status().as_u16();
        let mut headers = Vec::new();
        let mut upstream_request_id = None;
        let mut sse = false;
        for (name, value) in response.headers() {
            let name = name.as_str().to_ascii_lowercase();
            let Ok(value) = value.to_str() else { continue };
            if name == "content-type" && value.contains("text/event-stream") {
                sse = true;
            }
            if upstream_request_id.is_none() && REQUEST_ID_HEADERS.contains(&name.as_str()) {
                upstream_request_id = Some(value.to_string());
            }
            if !HOP_BY_HOP.contains(&name.as_str()) {
                headers.push((name, value.to_string()));
            }
        }
        let success = (200..300).contains(&status);
        Ok(Relayed {
            status,
            headers,
            body: RelayBody::new(
                Box::new(response),
                TapMode {
                    door,
                    sse,
                    strip_usage_chunk: prepared.strip_usage_chunk && sse && success,
                    success,
                },
                cancel,
                upstream_request_id,
            ),
            stream: prepared.stream,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::openai_compat::http::Endpoint;
    use crate::wire::stream::{StreamSink, StreamSummary};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    struct Discard;
    impl StreamSink for Discard {
        fn append(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn append_thinking(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn finalize(&mut self, _: StreamSummary) -> Result<(), String> {
            Ok(())
        }
    }

    fn refusing_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 4096];
            let _ = stream.read(&mut buffer);
            let body = r#"{"error":{"message":"model is overloaded"}}"#;
            let _ = write!(
                stream,
                "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        });
        format!("http://{address}/v1/chat/completions")
    }

    fn endpoint(url: String) -> Endpoint {
        Endpoint {
            provider_name: "Local".to_string(),
            backend_key: "local".to_string(),
            chat_url: url,
            headers: vec![("content-type".into(), "application/json".into())],
        }
    }

    /// A streamed failure reaches the caller with the transport's own text,
    /// byte for byte, so the transcript reads what it read before the seam.
    #[test]
    fn a_streaming_error_keeps_the_transport_text_verbatim() {
        let endpoint = endpoint(refusing_server());
        let cancel = Arc::new(AtomicBool::new(false));
        let expected = openai_compat::http::post_chat_completion_streaming(
            &endpoint,
            serde_json::json!({"model": "m"}),
            &cancel,
            &mut Discard,
        )
        .unwrap_err();

        let endpoint = self::endpoint(refusing_server());
        let mut sink = Discard;
        let error = HttpComplete
            .complete(CompletionCall::stream(
                CompletionWire::Chat(&endpoint),
                serde_json::json!({"model": "m"}),
                &mut sink,
                &cancel,
            ))
            .unwrap_err();
        assert_eq!(error, CompletionError::Stream(expected.clone()));
        assert_eq!(error.to_string(), expected);
        assert_eq!(error.into_stream_text(), expected);
    }

    use crate::interface::relay::{RelayDoor, RelayRoute};

    /// One fake upstream: answers `status`, `content_type`, `body`, and hands
    /// back the request it received.
    fn upstream(
        status: &str,
        content_type: &str,
        body: &'static str,
    ) -> (String, std::sync::mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let content_type = content_type.to_string();
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 8192];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).to_string();
                if let Some(split) = text.find("\r\n\r\n") {
                    let length = text[..split]
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= split + 4 + length {
                        break;
                    }
                }
                if read == 0 {
                    break;
                }
            }
            let _ = sent.send(String::from_utf8_lossy(&request).to_string());
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\nx-request-id: req-7\r\nconnection: close\r\n\r\n{body}"
            );
        });
        (format!("http://{address}/v1"), received)
    }

    fn route(base_url: String) -> RelayRoute {
        RelayRoute {
            base_url,
            headers: vec![("authorization".into(), "Bearer upstream-key".into())],
            backend_key: "openrouter".into(),
            provider_name: "OpenRouter".into(),
            sent_model: "deepseek/served".into(),
            account_id: Some("acct".into()),
        }
    }

    fn call(door: RelayDoor, base_url: String, body: serde_json::Value) -> RelayCall {
        RelayCall {
            door,
            route: route(base_url),
            body: body.as_object().unwrap().clone(),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    fn drain(relayed: &mut Relayed) -> String {
        let mut out = Vec::new();
        while let Some(chunk) = relayed.body.next_chunk() {
            out.extend(chunk.unwrap());
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn a_relayed_stream_reaches_the_door_path_with_the_model_rewritten() {
        let stream = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n";
        let (base, request) = upstream("200 OK", "text/event-stream", stream);
        let mut relayed = HttpComplete
            .relay(call(
                RelayDoor::ChatCompletions,
                base,
                serde_json::json!({"model": "big", "stream": true, "messages": []}),
            ))
            .unwrap();
        assert_eq!(relayed.status, 200);
        assert!(relayed.stream);
        assert_eq!(
            drain(&mut relayed),
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
        );
        assert_eq!(relayed.body.usage().input_tokens, Some(4));
        assert_eq!(
            relayed.body.usage().upstream_request_id.as_deref(),
            Some("req-7")
        );
        assert!(relayed
            .headers
            .iter()
            .all(|(name, _)| name != "connection" && name != "content-length"));
        let request = request.recv().unwrap();
        assert!(
            request.starts_with("POST /v1/chat/completions "),
            "{request}"
        );
        assert!(request.contains("Bearer upstream-key"));
        let body: serde_json::Value =
            serde_json::from_str(&request[request.find("\r\n\r\n").unwrap() + 4..]).unwrap();
        assert_eq!(body["model"], "deepseek/served");
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn an_upstream_failure_status_and_body_pass_through() {
        let failure = r#"{"error":{"message":"rate limited","code":429}}"#;
        let (base, _request) = upstream("429 Too Many Requests", "application/json", failure);
        let mut relayed = HttpComplete
            .relay(call(
                RelayDoor::Embeddings,
                base,
                serde_json::json!({"model": "embed", "input": "x"}),
            ))
            .unwrap();
        assert_eq!(relayed.status, 429);
        assert_eq!(drain(&mut relayed), failure);
        assert_eq!(relayed.body.usage().input_tokens, None);
    }

    #[test]
    fn an_unreachable_upstream_is_named() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        drop(listener);
        let error = HttpComplete
            .relay(call(
                RelayDoor::Responses,
                base,
                serde_json::json!({"model": "m", "input": "x"}),
            ))
            .err()
            .unwrap();
        assert!(
            matches!(error, RelayError::Unreachable(ref message) if message.starts_with("OpenRouter unreachable"))
        );
    }
}
