//! Provider-neutral streaming chat-completions transport.

use super::wire::{ChatMessage, ChatResponse, ChatStreamChunk, StreamingAggregate};
use crate::schema_order::OrderedSchemaAt;
use crate::stream::{StreamSink, StreamSummary};
use crate::{require_terminal_event, GenerationConfig, ToolDefinition};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Map, Value};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(90);
const STREAM_ABSOLUTE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const STREAM_READ_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct Endpoint {
    /// What to call this provider in an error a user will read. A shipped
    /// provider's product name, or the name an operator gave a custom one.
    pub provider_name: String,
    /// The backend key transcript rows and usage are attributed to. Owned
    /// because a custom provider's key is minted when it is configured.
    pub backend_key: String,
    pub chat_url: String,
    pub headers: Vec<(String, String)>,
}

/// Headers carry bearer keys, so the derived `Debug` would put every provider's
/// credential into any log line or panic message that formatted an endpoint.
/// Names stay — they are what makes a header diagnosis possible — and values
/// never appear.
impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("provider_name", &self.provider_name)
            .field("backend_key", &self.backend_key)
            .field("chat_url", &self.chat_url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| format!("{name}: <redacted>"))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// How one provider's chat-completions endpoint spells the generation knobs.
///
/// Every chat body Cairn sends has the same skeleton, and providers differ only
/// in these four places, so a provider states its dialect rather than writing a
/// body. Anything else a provider needs on the wire rides in the extras handed
/// to [`build_body`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatDialect {
    /// A per-generation output cap, for servers that otherwise generate up to
    /// the model's remaining context.
    pub max_tokens: Option<i64>,
    /// Ask a streamed reply to end with a usage frame
    /// (`stream_options.include_usage`).
    pub include_usage: bool,
    /// How a configured reasoning effort is sent.
    pub reasoning: ChatReasoning,
    /// Whether the endpoint takes a `response_format` json_schema for the run's
    /// output schema.
    pub response_format: bool,
}

/// How a chat endpoint takes a reasoning effort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatReasoning {
    /// Not at all: a configured effort is not sent.
    Omit,
    /// A flat `reasoning_effort` word, OpenAI's spelling.
    Effort,
    /// A `reasoning: {effort}` object, OpenRouter's spelling.
    Object,
}

impl Default for ChatDialect {
    fn default() -> Self {
        Self {
            max_tokens: None,
            include_usage: true,
            reasoning: ChatReasoning::Effort,
            response_format: true,
        }
    }
}

/// Build a chat-completions request body.
///
/// The dialect's knobs go in first, then `extras`, then the output schema, so a
/// provider's extras can override a knob but never the schema the caller asked
/// for. The effort passes through as configured: providers publish different
/// effort vocabularies, and one a model does not accept comes back as the
/// provider's own refusal, which says more than a silently coerced value.
pub fn build_body(
    dialect: &ChatDialect,
    model: &str,
    messages: &[ChatMessage],
    config: &GenerationConfig,
    extras: &Map<String, Value>,
    stream: bool,
) -> Value {
    let mut body = Map::new();
    body.insert("model".into(), json!(model));
    body.insert("messages".into(), json!(messages));
    if !config.tools.is_empty() {
        body.insert("tools".into(), json!(tool_schemas(&config.tools)));
        body.insert("tool_choice".into(), json!("auto"));
    }
    if let Some(max_tokens) = dialect.max_tokens {
        body.insert("max_tokens".into(), json!(max_tokens));
    }
    body.insert("stream".into(), json!(stream));
    // Servers refuse `stream_options` on a request that does not stream.
    if stream && dialect.include_usage {
        body.insert("stream_options".into(), json!({ "include_usage": true }));
    }
    if let Some(effort) = config.reasoning_effort.as_deref() {
        match dialect.reasoning {
            ChatReasoning::Omit => {}
            ChatReasoning::Effort => {
                body.insert("reasoning_effort".into(), json!(effort));
            }
            ChatReasoning::Object => {
                body.insert("reasoning".into(), json!({ "effort": effort }));
            }
        }
    }
    body.extend(
        extras
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    let mut body = Value::Object(body);
    if dialect.response_format {
        apply_output_schema(&mut body, config.output_schema.as_ref());
    }
    body
}

pub fn post_chat_completion(
    endpoint: &Endpoint,
    body: Value,
    timeout: Duration,
) -> Result<ChatResponse, crate::CompletionError> {
    use crate::CompletionError;
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| CompletionError::Upstream(error.to_string()))?;
    let response = client
        .post(&endpoint.chat_url)
        .headers(
            endpoint
                .header_map()
                .map_err(CompletionError::InvalidRequest)?,
        )
        .json(&OrderedSchemaAt::new(&body, OUTPUT_SCHEMA_PATH))
        .send()
        .map_err(|error| {
            if error.is_timeout() {
                CompletionError::Timeout
            } else {
                CompletionError::Upstream(error.to_string())
            }
        })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().unwrap_or_default();
        return Err(CompletionError::Upstream(format!(
            "{} chat completion returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        )));
    }
    response
        .json()
        .map_err(|error| CompletionError::InvalidResponse(error.to_string()))
}

// Error-body extraction is shared by every HTTP protocol family, so it lives at
// the neutral boundary and is re-exported here for this module's callers.
pub use crate::upstream_error_detail;

/// Where [`apply_output_schema`] puts the schema in a chat-completions body. The
/// transport serializes this subtree in the [`crate::schema_order`] contract.
const OUTPUT_SCHEMA_PATH: &[&str] = &["response_format", "json_schema", "schema"];

/// Inject the OpenAI-native structured-output constraint into a chat-completions
/// request body. `response_format` json_schema with `strict` demands
/// conformance, and the request is serialized with the schema's properties in
/// the [`crate::schema_order`] contract (required first), because a
/// grammar-constrained provider generates keys in schema order.
///
/// Cairn's server-side validation of the stored artifact catches a result that
/// violates the schema. It cannot catch an optional field the grammar
/// foreclosed: that result is still schema-valid, which is why the order is
/// fixed here rather than left to validation. A no-op when the run carries no
/// output schema, leaving schema-less sessions bit-for-bit unchanged.
pub fn apply_output_schema(body: &mut Value, schema: Option<&Value>) {
    let Some(schema) = schema else {
        return;
    };
    body["response_format"] = serde_json::json!({
        "type": "json_schema",
        "json_schema": {
            "name": "cairn_output",
            "strict": true,
            "schema": schema,
        }
    });
}

impl Endpoint {
    fn header_map(&self) -> Result<HeaderMap, String> {
        let mut map = HeaderMap::new();
        for (name, value) in &self.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| format!("invalid {} header name: {error}", self.provider_name))?;
            let value = HeaderValue::from_str(value)
                .map_err(|error| format!("invalid {} header value: {error}", self.provider_name))?;
            map.insert(name, value);
        }
        Ok(map)
    }
}

/// What completion looks like on the chat-completions wire.
///
/// Either signal satisfies the rule because OpenAI-compatible servers are
/// reliable about different ones: requiring the sentinel alone would fail turns
/// that genuinely finished on a server that never sends it.
pub const TERMINAL_EVENTS: &str = "finish_reason or [DONE]";

enum StreamRead {
    Line(String),
    Eof,
    Cancelled,
    Stalled,
    AbsoluteDeadline,
}

fn read_stream_line(
    lines: &Receiver<std::io::Result<String>>,
    cancel: &AtomicBool,
    request_started: Instant,
    last_progress: Instant,
    stall_timeout: Duration,
    absolute_timeout: Duration,
) -> std::io::Result<StreamRead> {
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Ok(StreamRead::Cancelled);
        }
        let absolute_elapsed = request_started.elapsed();
        if absolute_elapsed >= absolute_timeout {
            return Ok(StreamRead::AbsoluteDeadline);
        }
        let stalled_for = last_progress.elapsed();
        if stalled_for >= stall_timeout {
            return Ok(StreamRead::Stalled);
        }
        let wait = STREAM_READ_POLL_INTERVAL
            .min(absolute_timeout - absolute_elapsed)
            .min(stall_timeout - stalled_for);
        match lines.recv_timeout(wait) {
            Ok(Ok(line)) => return Ok(StreamRead::Line(line)),
            Ok(Err(error)) => return Err(error),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Ok(StreamRead::Eof),
        }
    }
}

fn stream_lines(reader: impl BufRead + Send + 'static) -> Receiver<std::io::Result<String>> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in reader.lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    receiver
}

pub fn post_chat_completion_streaming(
    endpoint: &Endpoint,
    body: Value,
    cancel: &Arc<AtomicBool>,
    sink: &mut dyn StreamSink,
) -> Result<ChatResponse, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(STREAM_ABSOLUTE_TIMEOUT)
        .build()
        .map_err(|error| {
            format!(
                "Failed to build {} streaming client: {error}",
                endpoint.provider_name
            )
        })?;
    let request_started = Instant::now();
    let response = client
        .post(&endpoint.chat_url)
        .headers(endpoint.header_map()?)
        .json(&OrderedSchemaAt::new(&body, OUTPUT_SCHEMA_PATH))
        .send()
        .map_err(|error| {
            format!(
                "{} streaming request failed: {error}",
                endpoint.provider_name
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let text = response
            .text()
            .unwrap_or_else(|error| format!("<failed to read error body: {error}>"));
        return Err(format!(
            "{} chat completion returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        ));
    }

    let mut aggregate = StreamingAggregate::default();
    let mut streamed_text = false;
    let mut saw_done_sentinel = false;
    let mut stalled = false;
    let mut exceeded_absolute_deadline = false;
    let mut last_progress = Instant::now();
    let lines = stream_lines(BufReader::new(response));
    loop {
        let line = match read_stream_line(
            &lines,
            cancel,
            request_started,
            last_progress,
            STREAM_STALL_TIMEOUT,
            STREAM_ABSOLUTE_TIMEOUT,
        )
        .map_err(|error| format!("{} stream read failed: {error}", endpoint.provider_name))?
        {
            StreamRead::Line(line) => {
                last_progress = Instant::now();
                line
            }
            StreamRead::Eof | StreamRead::Cancelled => break,
            StreamRead::Stalled => {
                stalled = true;
                break;
            }
            StreamRead::AbsoluteDeadline => {
                exceeded_absolute_deadline = true;
                break;
            }
        };
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            saw_done_sentinel = true;
            break;
        }
        let chunk: ChatStreamChunk = serde_json::from_str(data).map_err(|error| {
            format!(
                "Failed to parse {} stream chunk: {error}: {data}",
                endpoint.provider_name
            )
        })?;
        if let Some(message) = chunk.error_message() {
            // OpenRouter delivers post-stream-start errors in-band (HTTP stays
            // 200). Finalize any open live stream so it is not left dangling,
            // then surface the error; the turn handler records it and crashes
            // the run.
            if streamed_text {
                sink.finalize(StreamSummary {
                    text: aggregate.text.clone(),
                    thinking: aggregate.reasoning().to_string(),
                    reasoning_details: aggregate.reasoning_detail_values(),
                    usage: aggregate.usage.clone(),
                    generation_id: aggregate.id.clone(),
                    model: aggregate.model.clone(),
                })?;
            }
            return Err(message);
        }
        aggregate.apply_chunk(&chunk);
        let reasoning_delta = chunk
            .choices
            .iter()
            .filter_map(|choice| choice.delta.plain_reasoning())
            .map(|reasoning| reasoning.text().to_string())
            .collect::<String>();
        let content_delta = chunk
            .choices
            .iter()
            .filter_map(|choice| choice.delta.content.as_deref())
            .collect::<String>();
        // Reasoning typically streams before content, so either delta opens the
        // live stream.
        if !reasoning_delta.is_empty() {
            sink.append_thinking(&reasoning_delta)?;
            streamed_text = true;
        }
        if !content_delta.is_empty() {
            sink.append(&content_delta)?;
            streamed_text = true;
        }
    }

    if streamed_text {
        sink.finalize(StreamSummary {
            text: aggregate.text.clone(),
            thinking: aggregate.reasoning().to_string(),
            reasoning_details: aggregate.reasoning_detail_values(),
            usage: aggregate.usage.clone(),
            generation_id: aggregate.id.clone(),
            model: aggregate.model.clone(),
        })?;
    }

    if stalled {
        return Err(format!(
            "{} generation stalled: no SSE progress for {}s; finalized the partial turn instead of waiting indefinitely.",
            endpoint.provider_name,
            STREAM_STALL_TIMEOUT.as_secs()
        ));
    }
    if exceeded_absolute_deadline {
        return Err(format!(
            "{} generation exceeded the {} minute streaming backstop; finalized the partial turn.",
            endpoint.provider_name,
            STREAM_ABSOLUTE_TIMEOUT.as_secs() / 60
        ));
    }

    // Checked after finalizing so a truncated stream does not also leave a live
    // assistant message dangling in the UI, and before building the response so
    // no partial generation reaches the turn loop as a whole one. The cancel flag
    // is read here as the authoritative cancellation verdict. Body reads happen
    // on a bounded worker so a silent provider cannot prevent the session from
    // advancing; the stall clock bounds silence while the client timeout remains
    // only a generous backstop for a stream that never terminates.
    require_terminal_event(
        &endpoint.provider_name,
        TERMINAL_EVENTS,
        saw_done_sentinel || aggregate.finish_reason().is_some(),
        cancel.load(Ordering::SeqCst),
    )?;

    Ok(aggregate.into_response(streamed_text))
}

#[cfg(test)]
mod tests {
    use super::{apply_output_schema, post_chat_completion, upstream_error_detail, Endpoint};
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    /// Answer one chat-completions request and hand back the request body
    /// exactly as it crossed the socket. Key order is a property of those bytes,
    /// not of any parsed `Value`, so this is the only honest place to assert it.
    fn capture_one_request_body(schema: &serde_json::Value) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture listener");
        let address = listener.local_addr().expect("listener address");
        let (sender, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut raw = Vec::new();
            let mut chunk = [0u8; 4096];
            let body = loop {
                let read = stream.read(&mut chunk).expect("read request");
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
                    .expect("request declares its length");
                if raw.len() >= head_end + 4 + length || read == 0 {
                    break text[head_end + 4..].to_string();
                }
            };
            let reply = r#"{"choices":[{"message":{"role":"assistant","content":"{}"}}]}"#;
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                )
                .as_bytes(),
            );
            let _ = sender.send(body);
        });
        let endpoint = Endpoint {
            provider_name: "vLLM".into(),
            backend_key: "custom:acc_vllm".into(),
            chat_url: format!("http://{address}/v1/chat/completions"),
            headers: vec![],
        };
        let mut body = json!({ "model": "m", "messages": [] });
        apply_output_schema(&mut body, Some(schema));
        post_chat_completion(&endpoint, body, Duration::from_secs(5)).expect("completion");
        received
            .recv_timeout(Duration::from_secs(5))
            .expect("captured request")
    }

    /// The order `keys` first appear in `text`. Each key must be unique in the
    /// body under test, which the fixtures below are written to guarantee.
    fn wire_order<'a>(text: &str, keys: &[&'a str]) -> Vec<&'a str> {
        let mut positioned: Vec<(usize, &str)> = keys
            .iter()
            .map(|key| {
                let at = text
                    .find(&format!("\"{key}\":"))
                    .unwrap_or_else(|| panic!("`{key}` is not in the request: {text}"));
                (at, *key)
            })
            .collect();
        positioned.sort();
        positioned.into_iter().map(|(_, key)| key).collect()
    }

    #[test]
    fn a_grammar_constrained_provider_receives_required_properties_before_optional_ones() {
        // Authored as text and parsed, which is how every preset and custom
        // schema reaches this boundary. Parsing sorts `properties`, which put
        // the optional `summary` between `content` and `title`: a decoder that
        // walks keys in schema order then lets a model that writes `title`
        // (as the prompt and every example teach) foreclose `summary` without
        // an error, and the truncated object still validates.
        let schema: serde_json::Value = serde_json::from_str(
            r#"{
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "content": { "type": "string" },
                    "summary": { "type": "string" }
                },
                "required": ["title", "content"]
            }"#,
        )
        .unwrap();
        let sent = capture_one_request_body(&schema);
        assert_eq!(
            wire_order(&sent, &["title", "content", "summary"]),
            ["title", "content", "summary"],
            "{sent}"
        );
    }

    #[test]
    fn nested_object_schemas_follow_the_same_order_on_the_wire() {
        let schema: serde_json::Value = serde_json::from_str(
            r#"{
                "type": "object",
                "properties": {
                    "objective": { "type": "string" },
                    "requirements": { "type": "array", "items": { "type": "string" } },
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "title": { "type": "string" },
                                "agent": { "type": "string" },
                                "prompt": { "type": "string" },
                                "dependencies": { "type": "array", "items": { "type": "string" } },
                                "tier": { "type": "string" },
                                "backend": { "type": "string" }
                            },
                            "required": ["id", "title", "agent", "prompt"]
                        }
                    }
                },
                "required": ["objective", "tasks"]
            }"#,
        )
        .unwrap();
        let sent = capture_one_request_body(&schema);
        assert_eq!(
            wire_order(&sent, &["objective", "requirements", "tasks"]),
            ["objective", "tasks", "requirements"],
            "{sent}"
        );
        // Sorted, `agent` came first and `id` fourth, so a model writing `id`
        // straight after the forced `agent` silently lost `dependencies`.
        let task_keys = [
            "id",
            "title",
            "agent",
            "prompt",
            "dependencies",
            "tier",
            "backend",
        ];
        assert_eq!(
            wire_order(&sent, &task_keys),
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
    fn a_json_error_body_reaches_the_reader_as_the_providers_own_sentence() {
        assert_eq!(
            upstream_error_detail(
                r#"{"type":"error","error":{"type":"CreditsError","message":"No payment method."}}"#
            ),
            "No payment method."
        );
        // The OpenAI shape, with no sibling `type`, resolves identically.
        assert_eq!(
            upstream_error_detail(r#"{"error":{"message":"model not found"}}"#),
            "model not found"
        );
    }

    #[test]
    fn an_unrecognized_body_is_preserved_rather_than_swallowed() {
        assert_eq!(
            upstream_error_detail("<html>502</html>"),
            "<html>502</html>"
        );
        assert_eq!(upstream_error_detail(r#"{"error":{}}"#), r#"{"error":{}}"#);
        assert_eq!(upstream_error_detail(""), "");
    }

    #[test]
    fn no_schema_leaves_the_body_unconstrained() {
        let mut body = json!({ "model": "m" });
        apply_output_schema(&mut body, None);
        assert!(body.get("response_format").is_none());
    }

    #[test]
    fn a_schema_becomes_a_strict_response_format() {
        let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}});
        let mut body = json!({ "model": "m" });
        apply_output_schema(&mut body, Some(&schema));
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["strict"], true);
        assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
    }
}

// === Reasoning echo ===

#[cfg(test)]
mod reasoning_echo_tests {
    use super::{post_chat_completion_streaming, Endpoint};
    use crate::openai_compat::generation::into_generation;
    use crate::openai_compat::wire::ChatMessage;
    use crate::stream::{StreamSink, StreamSummary};
    use serde_json::{json, Value};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    /// Ollama's shape: plain `reasoning` deltas (beside an empty `content`)
    /// ahead of a tool call, and no `reasoning_details`.
    const OLLAMA: &str = concat!(
        "data: {\"id\":\"chatcmpl-1\",\"model\":\"qwen3.8:27b\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"reasoning\":\"The user wants \"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"model\":\"qwen3.8:27b\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"reasoning\":\"the file read.\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"model\":\"qwen3.8:27b\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"id\":\"call_1\",\"index\":0,\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-1\",\"model\":\"qwen3.8:27b\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    /// DeepSeek's (and older vLLM's) spelling: `reasoning_content`.
    const REASONING_CONTENT: &str = concat!(
        "data: {\"id\":\"gen-1\",\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"reasoning_content\":\"Greet \"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-1\",\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"reasoning_content\":\"them back.\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-1\",\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello.\",\"reasoning_content\":null},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    /// OpenRouter's shape: the same thinking as plain `reasoning` and as
    /// structured `reasoning_details`.
    const OPENROUTER: &str = concat!(
        "data: {\"id\":\"gen-1\",\"model\":\"anthropic/claude\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"reasoning\":\"Think.\",\"reasoning_details\":[{\"type\":\"reasoning.text\",\"index\":0,\"text\":\"Think.\",\"signature\":\"sig\"}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-1\",\"model\":\"anthropic/claude\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Done.\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    const ANSWER: &str = concat!(
        "data: {\"id\":\"gen-2\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    #[derive(Default)]
    struct ThinkingSink(String);

    impl StreamSink for ThinkingSink {
        fn append(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn append_thinking(&mut self, text: &str) -> Result<(), String> {
            self.0.push_str(text);
            Ok(())
        }
        fn finalize(&mut self, _: StreamSummary) -> Result<(), String> {
            Ok(())
        }
    }

    /// Serve `replies` in order, one per connection, keeping each request body
    /// as it crossed the socket.
    fn serve(replies: Vec<&'static str>) -> (String, Arc<Mutex<Vec<Value>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock endpoint");
        let url = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().expect("listener address")
        );
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let seen = bodies.clone();
        std::thread::spawn(move || {
            for reply in replies {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut request = Vec::new();
                let mut buffer = [0u8; 8192];
                loop {
                    let read = stream.read(&mut buffer).expect("read request");
                    request.extend_from_slice(&buffer[..read]);
                    let text = String::from_utf8_lossy(&request).into_owned();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text[..end]
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            seen.lock().unwrap().push(
                                serde_json::from_slice(&request[end + 4..end + 4 + length])
                                    .expect("request body is JSON"),
                            );
                            break;
                        }
                    }
                    if read == 0 {
                        break;
                    }
                }
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            }
        });
        (url, bodies)
    }

    /// Stream `reply` as the model's turn, push the assistant message it
    /// produced onto the conversation, and send that conversation as the next
    /// request. Returns the assistant message exactly as the endpoint received
    /// it, and the thinking the sink was shown.
    fn echoed_assistant_turn(reply: &'static str) -> (Value, String) {
        let (chat_url, bodies) = serve(vec![reply, ANSWER]);
        let endpoint = Endpoint {
            provider_name: "Mock".into(),
            backend_key: "mock".into(),
            chat_url,
            headers: vec![],
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let mut messages = vec![ChatMessage::user("hi".to_string())];
        let mut thinking = ThinkingSink::default();
        let response = post_chat_completion_streaming(
            &endpoint,
            json!({ "model": "m", "stream": true, "messages": messages }),
            &cancel,
            &mut thinking,
        )
        .expect("first generation");
        let generation = into_generation(response, "Mock", &[]).expect("generation");
        messages.push(generation.assistant_message);
        post_chat_completion_streaming(
            &endpoint,
            json!({ "model": "m", "stream": true, "messages": messages }),
            &cancel,
            &mut ThinkingSink::default(),
        )
        .expect("second generation");
        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 2);
        let assistant = bodies[1]["messages"][1].clone();
        assert_eq!(assistant["role"], "assistant", "{assistant}");
        (assistant, thinking.0)
    }

    #[test]
    fn an_ollama_turn_gets_its_plain_reasoning_echoed_back() {
        let (assistant, thinking) = echoed_assistant_turn(OLLAMA);
        assert_eq!(thinking, "The user wants the file read.");
        assert_eq!(assistant["reasoning"], "The user wants the file read.");
        assert_eq!(assistant["tool_calls"][0]["id"], "call_1");
        assert!(assistant.get("reasoning_content").is_none(), "{assistant}");
        assert!(assistant.get("reasoning_details").is_none(), "{assistant}");
    }

    #[test]
    fn reasoning_content_is_shown_and_echoed_under_its_own_key() {
        let (assistant, thinking) = echoed_assistant_turn(REASONING_CONTENT);
        assert_eq!(thinking, "Greet them back.");
        assert_eq!(assistant["reasoning_content"], "Greet them back.");
        assert_eq!(assistant["content"], "Hello.");
        assert!(assistant.get("reasoning").is_none(), "{assistant}");
    }

    #[test]
    fn structured_reasoning_details_supersede_the_plain_string() {
        // Sending both would hand OpenRouter the same thinking twice; the
        // details are the form that carries the signature.
        let (assistant, _) = echoed_assistant_turn(OPENROUTER);
        assert_eq!(assistant["reasoning_details"][0]["signature"], "sig");
        assert!(assistant.get("reasoning").is_none(), "{assistant}");
    }
}

// === Premature end of stream ===

#[cfg(test)]
mod terminal_event_tests {
    use super::{read_stream_line, stream_lines, StreamRead, TERMINAL_EVENTS};
    use crate::openai_compat::wire::{ChatStreamChunk, StreamingAggregate};
    use crate::require_terminal_event;
    use std::io::BufReader;
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    const PROVIDER: &str = "Actual";

    fn silent_stream() -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("listener address");
        thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept test connection");
            thread::sleep(Duration::from_secs(1));
        });
        TcpStream::connect(address).expect("connect test stream")
    }

    #[test]
    fn a_silent_socket_observes_cancellation_without_waiting_for_sse_progress() {
        let cancel = Arc::new(AtomicBool::new(false));
        let cancelling = cancel.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            cancelling.store(true, Ordering::SeqCst);
        });
        let started = Instant::now();
        let lines = stream_lines(BufReader::new(silent_stream()));
        let result = read_stream_line(
            &lines,
            &cancel,
            started,
            started,
            Duration::from_secs(1),
            Duration::from_secs(2),
        )
        .expect("polling read");
        assert!(matches!(result, StreamRead::Cancelled));
        assert!(started.elapsed() < Duration::from_millis(150));
    }

    #[test]
    fn a_silent_socket_reaches_the_stall_timeout() {
        let started = Instant::now();
        let lines = stream_lines(BufReader::new(silent_stream()));
        let result = read_stream_line(
            &lines,
            &AtomicBool::new(false),
            started,
            started,
            Duration::from_millis(80),
            Duration::from_secs(1),
        )
        .expect("polling read");
        assert!(matches!(result, StreamRead::Stalled));
        assert!(started.elapsed() < Duration::from_millis(150));
    }

    /// Replay an SSE fixture through the same parse-and-aggregate steps the
    /// streaming loop uses, and report whether the protocol said it was done.
    /// Everything between the socket and the completion verdict, with no HTTP.
    fn replay(fixture: &str) -> (StreamingAggregate, bool) {
        let mut aggregate = StreamingAggregate::default();
        let mut saw_done_sentinel = false;
        for line in fixture.lines() {
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data.is_empty() {
                continue;
            }
            if data == "[DONE]" {
                saw_done_sentinel = true;
                break;
            }
            let chunk: ChatStreamChunk =
                serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
            aggregate.apply_chunk(&chunk);
        }
        (aggregate, saw_done_sentinel)
    }

    fn verdict(fixture: &str) -> Result<(), String> {
        let (aggregate, saw_done_sentinel) = replay(fixture);
        require_terminal_event(
            PROVIDER,
            TERMINAL_EVENTS,
            saw_done_sentinel || aggregate.finish_reason().is_some(),
            false,
        )
    }

    const TEXT_CUT_OFF: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Deleting \"}}]}\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"nothing\"}}]}\n",
    );

    const FINISH_REASON_NO_SENTINEL: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"}}]}\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n",
    );

    const SENTINEL_NO_FINISH_REASON: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"}}]}\n",
        "data: [DONE]\n",
    );

    /// A tool call whose arguments stopped arriving partway through. This is the
    /// case the invariant exists for: the JSON is a valid prefix, so nothing
    /// downstream can tell it was cut off.
    const TOOL_CALL_CUT_OFF: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"run\",\"arguments\":\"\"}}]}}]}\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"command\\\": \\\"rm -rf \"}}]}}]}\n",
    );

    #[test]
    fn a_connection_that_closes_mid_generation_is_not_a_finished_turn() {
        let error =
            verdict(TEXT_CUT_OFF).expect_err("a stream with no terminal event is truncated");
        assert!(error.contains(PROVIDER), "names the provider: {error}");
        assert!(
            error.contains("finish_reason") && error.contains("[DONE]"),
            "says which terminal events were missing: {error}"
        );
    }

    #[test]
    fn a_tool_call_cut_off_mid_arguments_is_refused_rather_than_dispatched() {
        // Left to the byte stream this looks like a complete turn carrying one
        // tool call, and the truncated argument JSON is a valid prefix of the
        // real one. Dispatching it would run a command the model never finished
        // asking for.
        assert!(
            verdict(TOOL_CALL_CUT_OFF).is_err(),
            "a half-streamed tool call must not reach the turn loop"
        );
    }

    #[test]
    fn a_terminal_finish_reason_completes_a_stream_that_sends_no_sentinel() {
        // Not every OpenAI-compatible server emits `[DONE]`; requiring the
        // sentinel alone would fail turns that genuinely finished.
        assert!(verdict(FINISH_REASON_NO_SENTINEL).is_ok());
    }

    #[test]
    fn the_done_sentinel_completes_a_stream_that_reports_no_finish_reason() {
        assert!(verdict(SENTINEL_NO_FINISH_REASON).is_ok());
    }

    #[test]
    fn cancellation_is_the_explicit_early_exit_and_is_never_truncation() {
        // A cancelled turn ends without a terminal event by construction. That
        // is the user's decision, not a broken stream, so it must not surface
        // as a provider error.
        let (aggregate, saw_done_sentinel) = replay(TEXT_CUT_OFF);
        assert!(require_terminal_event(
            PROVIDER,
            TERMINAL_EVENTS,
            saw_done_sentinel || aggregate.finish_reason().is_some(),
            true,
        )
        .is_ok());
    }
}

/// Tools in the chat-completions wrapper: a `function` object nested under a
/// `type` discriminator, with the argument schema named `parameters`. The
/// definitions themselves are neutral (`crate::tool_defs`), so this family,
/// Messages, and Responses cannot end up describing different tools.
pub fn tool_schemas(tools: &[ToolDefinition]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod body_tests {
    use super::{build_body, ChatDialect, ChatReasoning};
    use crate::openai_compat::wire::ChatMessage;
    use crate::{GenerationConfig, ToolDefinition};
    use serde_json::{json, Map, Value};

    fn tool() -> ToolDefinition {
        ToolDefinition {
            name: "read".to_string(),
            description: "Read.".to_string(),
            input_schema: json!({"type": "object"}),
        }
    }

    fn knobs() -> GenerationConfig {
        GenerationConfig {
            reasoning_effort: Some("high".to_string()),
            max_thinking_tokens: None,
            output_schema: Some(json!({"type": "object"})),
            tools: vec![tool()],
        }
    }

    fn body(dialect: ChatDialect, extras: Value, stream: bool) -> String {
        let Value::Object(extras) = extras else {
            panic!("extras are an object")
        };
        let messages = [ChatMessage::user("hi".to_string())];
        let body = build_body(&dialect, "m", &messages, &knobs(), &extras, stream);
        serde_json::to_string(&body).unwrap()
    }

    const TOOLS: &str = r#""tool_choice":"auto","tools":[{"function":{"description":"Read.","name":"read","parameters":{"type":"object"}},"type":"function"}]"#;
    const SCHEMA: &str = r#""response_format":{"json_schema":{"name":"cairn_output","schema":{"type":"object"},"strict":true},"type":"json_schema"}"#;

    #[test]
    fn the_default_dialect_sends_every_knob_the_openai_way() {
        assert_eq!(
            body(ChatDialect::default(), json!({}), true),
            format!(
                r#"{{"messages":[{{"content":"hi","role":"user"}}],"model":"m","reasoning_effort":"high",{SCHEMA},"stream":true,"stream_options":{{"include_usage":true}},{TOOLS}}}"#
            )
        );
    }

    #[test]
    fn an_object_dialect_nests_the_effort_and_extras_ride_beside_it() {
        let dialect = ChatDialect {
            include_usage: false,
            reasoning: ChatReasoning::Object,
            ..ChatDialect::default()
        };
        assert_eq!(
            body(
                dialect,
                json!({"provider": {"require_parameters": true}}),
                true
            ),
            format!(
                r#"{{"messages":[{{"content":"hi","role":"user"}}],"model":"m","provider":{{"require_parameters":true}},"reasoning":{{"effort":"high"}},{SCHEMA},"stream":true,{TOOLS}}}"#
            )
        );
    }

    #[test]
    fn an_omitting_dialect_sends_a_cap_and_no_effort_or_schema() {
        let dialect = ChatDialect {
            max_tokens: Some(512),
            reasoning: ChatReasoning::Omit,
            response_format: false,
            ..ChatDialect::default()
        };
        assert_eq!(
            body(dialect, json!({"temperature": 0.7}), true),
            format!(
                r#"{{"max_tokens":512,"messages":[{{"content":"hi","role":"user"}}],"model":"m","stream":true,"stream_options":{{"include_usage":true}},"temperature":0.7,{TOOLS}}}"#
            )
        );
    }

    #[test]
    fn a_whole_request_never_carries_stream_options() {
        let text = body(ChatDialect::default(), json!({}), false);
        assert!(text.contains(r#""stream":false"#));
        assert!(!text.contains("stream_options"));
    }

    #[test]
    fn a_tool_free_config_advertises_no_tools() {
        let config = GenerationConfig::default();
        let body = build_body(
            &ChatDialect::default(),
            "m",
            &[],
            &config,
            &Map::new(),
            true,
        );
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
    }

    /// Extras merge after the knobs and before the output schema.
    #[test]
    fn extras_override_a_knob_but_not_the_output_schema() {
        let text = body(
            ChatDialect::default(),
            json!({"reasoning_effort": "low", "response_format": "text"}),
            true,
        );
        assert!(text.contains(r#""reasoning_effort":"low""#));
        assert!(text.contains(SCHEMA));
    }
}
