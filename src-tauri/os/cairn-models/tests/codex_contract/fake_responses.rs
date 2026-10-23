//! A hermetic stand-in for the OpenAI Responses API, and the egress trap.
//!
//! One loopback listener plays two parts. As the contract's model provider
//! (`base_url` in the provider stanza) it answers `POST /v1/responses` with an
//! SSE stream whose shape is chosen per request from a queue of [`Mode`]s.
//! As `HTTPS_PROXY` it logs and refuses every `CONNECT` (and every absolute-URL
//! request), so any traffic Codex tries to send off the machine is recorded
//! rather than sent. A case asserts that record is empty.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

/// The text a [`Mode::Normal`] turn streams.
pub const FAKE_TEXT: &str = "Hello from fake";
const FAKE_CHUNKS: [&str; 3] = ["Hello", " from", " fake"];

/// How the fake answers one `POST /v1/responses`.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Stream [`FAKE_TEXT`] and complete.
    Normal,
    /// Stream a delta every 250 ms until the client hangs up.
    Slow,
    /// Answer with this HTTP status and a JSON error body.
    Http(u16),
    /// `response.failed` with this `error.code`.
    Failed(&'static str),
    /// Stream part of the message, then close without `response.completed`.
    Disconnect,
    /// One `function_call` to the first tool the request offers whose name
    /// contains `needle`, with these arguments.
    Tool {
        needle: &'static str,
        arguments: &'static str,
    },
}

/// One request the provider answered.
#[derive(Debug, Clone)]
pub struct Served {
    pub mode: String,
    /// Every tool name the request offered, nested namespaces included.
    pub tools: Vec<String>,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Mode>,
    served: Vec<Served>,
    egress: Vec<String>,
    counter: usize,
    reject_auth: bool,
}

pub struct FakeResponses {
    addr: String,
    state: Arc<Mutex<State>>,
}

impl FakeResponses {
    pub fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?.to_string();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let state = shared.clone();
                std::thread::spawn(move || {
                    let _ = serve(stream, &state);
                });
            }
        });
        Ok(Self { addr, state })
    }

    /// The provider's `base_url`: Codex posts to `{base_url}/responses`.
    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    /// The proxy every non-loopback request is sent through, and refused by.
    pub fn proxy_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Answer the next requests with `modes`, in order; afterwards, [`Mode::Normal`].
    pub fn queue(&self, modes: impl IntoIterator<Item = Mode>) {
        self.state.lock().unwrap().queue.extend(modes);
    }

    /// Answer every provider request (websocket upgrades included) with the
    /// ChatGPT backend's 401 for an expired token.
    pub fn reject_auth(&self) {
        self.state.lock().unwrap().reject_auth = true;
    }

    pub fn served(&self) -> Vec<Served> {
        self.state.lock().unwrap().served.clone()
    }

    /// Every attempt to reach anything but this provider.
    pub fn egress(&self) -> Vec<String> {
        self.state.lock().unwrap().egress.clone()
    }
}

fn sse(event: &str, payload: Value) -> Vec<u8> {
    let mut payload = payload;
    payload["type"] = json!(event);
    format!("event: {event}\ndata: {payload}\n\n").into_bytes()
}

fn usage() -> Value {
    json!({
        "input_tokens": 12, "input_tokens_details": {"cached_tokens": 0},
        "output_tokens": 3, "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 15
    })
}

/// Every tool in a Responses `tools` array. A tool nested in a namespace entry
/// (how Codex offers an MCP server's tools) is named `namespace/tool`.
fn tool_names(tools: &Value, namespace: Option<&str>, into: &mut Vec<String>) {
    for tool in tools.as_array().into_iter().flatten() {
        let name = tool.get("name").and_then(Value::as_str);
        match tool.get("tools") {
            Some(nested) => tool_names(nested, name, into),
            None => {
                if let Some(name) = name {
                    into.push(match namespace {
                        Some(namespace) => format!("{namespace}/{name}"),
                        None => name.to_string(),
                    });
                }
            }
        }
    }
}

fn respond(stream: &mut TcpStream, status: u16, reason: &str, body: &str) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

fn serve(mut stream: TcpStream, state: &Mutex<State>) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header == "\r\n" || header == "\n" {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }

    if method == "CONNECT" || target.starts_with("http://") || target.starts_with("https://") {
        state
            .lock()
            .unwrap()
            .egress
            .push(format!("{method} {target}"));
        return respond(&mut stream, 403, "Forbidden", "");
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    if state.lock().unwrap().reject_auth {
        state.lock().unwrap().served.push(Served {
            mode: format!("Unauthorized {method} {target}"),
            tools: Vec::new(),
        });
        return respond(
            &mut stream,
            401,
            "Unauthorized",
            r#"{"error":{"message":"Your authentication token has expired. Please try signing in again.","type":"invalid_request_error","code":"token_expired"}}"#,
        );
    }
    if method != "POST" || !target.trim_end_matches('/').ends_with("/responses") {
        return respond(
            &mut stream,
            404,
            "Not Found",
            r#"{"error":{"message":"not found"}}"#,
        );
    }

    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let mut tools = Vec::new();
    tool_names(&request["tools"], None, &mut tools);
    let (mode, n) = {
        let mut state = state.lock().unwrap();
        state.counter += 1;
        let mode = state.queue.pop_front().unwrap_or(Mode::Normal);
        state.served.push(Served {
            mode: format!("{mode:?}"),
            tools: tools.clone(),
        });
        (mode, state.counter)
    };

    if let Mode::Http(status) = mode {
        let body = json!({"error": {"type": "server_error", "code": null,
            "message": format!("fake {status} from the contract provider")}});
        return respond(&mut stream, status, "Error", &body.to_string());
    }

    stream.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: close\r\n\r\n",
    )?;
    let response_id = format!("resp_{n}");
    let message_id = format!("msg_{n}");
    stream.write_all(&sse(
        "response.created",
        json!({"response": {"id": response_id}}),
    ))?;
    stream.flush()?;

    let message_added = || {
        sse(
            "response.output_item.added",
            json!({"output_index": 0, "item": {"type": "message", "role": "assistant",
                "id": message_id, "content": []}}),
        )
    };
    let delta = |text: &str| {
        sse(
            "response.output_text.delta",
            json!({"item_id": message_id, "output_index": 0, "content_index": 0, "delta": text}),
        )
    };
    let completed = || {
        sse(
            "response.completed",
            json!({"response": {"id": response_id, "usage": usage()}}),
        )
    };

    match mode {
        Mode::Failed(code) => {
            stream.write_all(&sse(
                "response.failed",
                json!({"response": {"id": response_id, "error": {"code": code,
                    "message": format!("fake response.failed ({code}) from the contract provider")}}}),
            ))?;
        }
        Mode::Tool { needle, arguments } => {
            let name = tools
                .iter()
                .find(|name| name.contains(needle))
                .cloned()
                .unwrap_or_else(|| format!("no-offered-tool-matching-{needle}"));
            let mut item = json!({"type": "function_call", "id": format!("fc_{n}"),
                "call_id": format!("call_{n}"), "name": name, "arguments": arguments});
            if let Some((namespace, tool)) = name.split_once('/') {
                item["namespace"] = json!(namespace);
                item["name"] = json!(tool);
            }
            let mut added = item.clone();
            added["arguments"] = json!("");
            stream.write_all(&sse(
                "response.output_item.added",
                json!({"output_index": 0, "item": added}),
            ))?;
            stream.write_all(&sse(
                "response.output_item.done",
                json!({"output_index": 0, "item": item}),
            ))?;
            stream.write_all(&completed())?;
        }
        Mode::Slow => {
            stream.write_all(&message_added())?;
            for tick in 0..480 {
                stream.write_all(&delta(&format!("tick{tick} ")))?;
                stream.flush()?;
                std::thread::sleep(Duration::from_millis(250));
            }
        }
        Mode::Disconnect => {
            stream.write_all(&message_added())?;
            stream.write_all(&delta(FAKE_CHUNKS[0]))?;
        }
        Mode::Normal | Mode::Http(_) => {
            stream.write_all(&message_added())?;
            for chunk in FAKE_CHUNKS {
                stream.write_all(&delta(chunk))?;
            }
            stream.write_all(&sse(
                "response.output_item.done",
                json!({"output_index": 0, "item": {"type": "message", "role": "assistant",
                    "id": message_id, "content": [{"type": "output_text", "text": FAKE_TEXT,
                    "annotations": []}]}}),
            ))?;
            stream.write_all(&completed())?;
        }
    }
    stream.flush()
}
