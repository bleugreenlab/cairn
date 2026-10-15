//! The facet's HTTP completion: the one caller of the protocol families' POSTs.

use crate::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, CompletionWire, Delivery,
};
use crate::wire::{anthropic_messages, decisions, openai_compat, openai_responses};

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
            (CompletionWire::Decisions(endpoint), Delivery::Stream { .. }) => {
                Err(CompletionError::InvalidRequest(format!(
                    "{} answers a decision as one whole reply; a decision is never streamed",
                    endpoint.provider_name
                )))
            }
        }
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
}
