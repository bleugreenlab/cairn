//! Exercise the actual HTTP readers against the caller-owned persistence seam.
use cairn_llm_wire::stream::{StreamSink, StreamSummary};
use cairn_llm_wire::{anthropic_messages, openai_compat, openai_responses};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Chat,
    Messages,
    Responses,
}
const PROTOCOLS: [Protocol; 3] = [Protocol::Chat, Protocol::Messages, Protocol::Responses];

impl Protocol {
    fn events(self) -> (Vec<Value>, Value, Value) {
        match self {
            Self::Chat => (
                vec![
                    json!({"id":"g1","model":"m1","choices":[{"delta":{"reasoning":"think"}}]}),
                    json!({"choices":[{"delta":{"content":"answer"}}]}),
                ],
                json!({"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":3}}),
                json!({"error":{"message":"provider refused"}}),
            ),
            Self::Messages => (
                vec![
                    json!({"type":"message_start","message":{"id":"g1","model":"m1"}}),
                    json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"think"}}),
                    json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
                    json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}),
                    json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":20,"output_tokens":3}}),
                ],
                json!({"type":"message_stop"}),
                json!({"type":"error","error":{"message":"provider refused"}}),
            ),
            Self::Responses => (
                vec![
                    json!({"type":"response.created","response":{"id":"g1","model":"m1"}}),
                    json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"r1","encrypted_content":"opaque"}}),
                    json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"think"}),
                    json!({"type":"response.output_text.delta","output_index":1,"delta":"answer"}),
                ],
                json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":20,"output_tokens":3}}}),
                json!({"type":"response.failed","response":{"error":{"message":"provider refused"}}}),
            ),
        }
    }

    fn post(self, events: Vec<Value>, sink: &mut RecordingSink) -> Result<bool, String> {
        let body = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>();
        let (url, server) = serve(body);
        let cancel = sink.cancel.clone();
        let result = match self {
            Self::Chat => openai_compat::http::post_chat_completion_streaming(
                &openai_compat::http::Endpoint {
                    provider_name: "test".into(),
                    backend_key: "test".into(),
                    chat_url: url,
                    headers: vec![],
                },
                json!({}),
                &cancel,
                sink,
            )
            .map(|response| response.streamed_text),
            Self::Messages => anthropic_messages::http::post_message_streaming(
                &anthropic_messages::http::MessagesEndpoint {
                    provider_name: "test".into(),
                    backend_key: "test".into(),
                    url,
                    headers: vec![],
                    max_output_tokens: 100,
                },
                json!({}),
                &cancel,
                sink,
            )
            .map(|response| response.streamed_text),
            Self::Responses => openai_responses::http::post_response_streaming(
                &openai_responses::http::ResponsesEndpoint {
                    provider_name: "test".into(),
                    backend_key: "test".into(),
                    url,
                    headers: vec![],
                },
                json!({}),
                &cancel,
                sink,
            )
            .map(|response| response.streamed_text),
        };
        server.join().unwrap();
        result
    }
}

fn serve(body: String) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(&mut socket);
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
            if let Some(length) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                content_length = length.trim().parse::<usize>().unwrap();
            }
        }
        reader.read_exact(&mut vec![0; content_length]).unwrap();
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
    });
    (url, server)
}

#[derive(Default)]
struct RecordingSink {
    calls: Vec<String>,
    summary: Option<StreamSummary>,
    cancel: Arc<AtomicBool>,
    cancel_on_delta: bool,
    reject: Option<&'static str>,
}
impl RecordingSink {
    fn delta(&mut self, kind: &str, text: &str) -> Result<(), String> {
        self.calls.push(format!("{kind}:{text}"));
        if self.cancel_on_delta {
            self.cancel.store(true, Ordering::SeqCst);
        }
        if self.reject == Some(kind) {
            return Err(format!("{kind} persistence failed"));
        }
        Ok(())
    }
}
impl StreamSink for RecordingSink {
    fn append(&mut self, text: &str) -> Result<(), String> {
        self.delta("text", text)
    }
    fn append_thinking(&mut self, text: &str) -> Result<(), String> {
        self.delta("thinking", text)
    }
    fn finalize(&mut self, summary: StreamSummary) -> Result<(), String> {
        self.calls.push("finalize".into());
        assert!(self.summary.is_none(), "a stream finalizes exactly once");
        if self.reject == Some("finalize") {
            return Err("finalize persistence failed".into());
        }
        self.summary = Some(summary);
        Ok(())
    }
}

#[test]
fn successful_stream_preserves_callback_order_and_final_metadata() {
    for protocol in PROTOCOLS {
        let (mut events, terminal, _) = protocol.events();
        events.push(terminal);
        let mut sink = RecordingSink::default();
        assert_eq!(protocol.post(events, &mut sink), Ok(true), "{protocol:?}");
        assert_eq!(sink.calls, ["thinking:think", "text:answer", "finalize"]);
        let summary = sink.summary.unwrap();
        assert_eq!(
            (summary.text.as_str(), summary.thinking.as_str()),
            ("answer", "think"),
            "{protocol:?}"
        );
        assert_eq!(summary.generation_id.as_deref(), Some("g1"));
        assert_eq!(summary.model.as_deref(), Some("m1"));
        assert_eq!(summary.usage.unwrap().prompt_tokens, Some(20));
    }
}

#[test]
fn premature_eof_finalizes_partial_output_but_cannot_return_a_generation() {
    for protocol in PROTOCOLS {
        let (events, _, _) = protocol.events();
        let mut sink = RecordingSink::default();
        let error = protocol.post(events, &mut sink).unwrap_err();
        assert!(
            error.contains("stream ended before"),
            "{protocol:?}: {error}"
        );
        assert_eq!(sink.summary.unwrap().text, "answer");
        assert_eq!(sink.calls.last().map(String::as_str), Some("finalize"));
    }
}

#[test]
fn provider_failure_finalizes_partial_output_and_preserves_the_error() {
    for protocol in PROTOCOLS {
        let (mut events, _, failure) = protocol.events();
        events.push(failure);
        let mut sink = RecordingSink::default();
        let error = protocol.post(events, &mut sink).unwrap_err();
        assert!(error.contains("provider refused"), "{protocol:?}: {error}");
        assert_eq!(sink.summary.unwrap().text, "answer");
    }
}

#[test]
fn cancellation_after_a_delta_finalizes_without_becoming_a_truncation_error() {
    for protocol in PROTOCOLS {
        let (events, _, _) = protocol.events();
        let mut sink = RecordingSink {
            cancel_on_delta: true,
            ..Default::default()
        };
        assert_eq!(protocol.post(events, &mut sink), Ok(true), "{protocol:?}");
        assert_eq!(sink.calls, ["thinking:think", "finalize"]);
        let summary = sink.summary.unwrap();
        assert_eq!(summary.thinking, "think");
        assert!(summary.text.is_empty());
    }
}

#[test]
fn a_rejected_delta_or_final_snapshot_stops_the_generation() {
    for protocol in PROTOCOLS {
        for rejected in ["thinking", "text", "finalize"] {
            let (mut events, terminal, _) = protocol.events();
            events.push(terminal);
            let mut sink = RecordingSink {
                reject: Some(rejected),
                ..Default::default()
            };
            assert_eq!(
                protocol.post(events, &mut sink),
                Err(format!("{rejected} persistence failed")),
                "{protocol:?}"
            );
            assert!(sink.summary.is_none());
            assert!(sink.calls.last().unwrap().starts_with(rejected));
        }
    }
}

#[test]
fn a_tool_only_generation_does_not_open_a_live_transcript_row() {
    for protocol in PROTOCOLS {
        let (_, terminal, _) = protocol.events();
        let tool = match protocol {
            Protocol::Chat => {
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call1","type":"function","function":{"name":"read","arguments":"{}"}}]}}]})
            }
            Protocol::Messages => {
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call1","name":"read","input":{}}})
            }
            Protocol::Responses => {
                json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call1","name":"read","arguments":"{}"}})
            }
        };
        let mut sink = RecordingSink::default();
        assert_eq!(
            protocol.post(vec![tool, terminal], &mut sink),
            Ok(false),
            "{protocol:?}"
        );
        assert!(sink.calls.is_empty());
        assert!(sink.summary.is_none());
    }
}
