//! A hermetic stand-in for the Anthropic Messages API, the egress trap, and a
//! stand-in for Cairn's HTTP MCP server.
//!
//! One loopback listener plays two parts. As `ANTHROPIC_BASE_URL` it answers
//! `POST /v1/messages` (the CLI adds `?beta=true`) with an SSE stream whose
//! shape is chosen per request from a queue of [`Mode`]s. As `HTTPS_PROXY`
//! it records and refuses every `CONNECT` (and every absolute-URL request), so
//! any traffic the CLI tries to send off the machine is recorded rather than
//! sent. A case asserts that record is empty.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

/// The text a [`Mode::Text`] turn streams, in [`TEXT_CHUNKS`].
pub const FAKE_TEXT: &str = "Hello from the fake";
const TEXT_CHUNKS: [&str; 3] = ["Hello", " from the", " fake"];
/// The thinking a [`Mode::Text`] turn streams before its text.
pub const FAKE_THINKING: &str = "Weighing the fake";

/// The usage every streamed answer reports: `message_start` carries the
/// input side, `message_delta` the output.
pub const USAGE_INPUT: u32 = 12;
pub const USAGE_CACHE_READ: u32 = 5;
pub const USAGE_CACHE_CREATION: u32 = 3;
pub const USAGE_OUTPUT: u32 = 7;

/// The schema a structured call constrains its answer to, and the input the
/// fake's `StructuredOutput` call fills it with.
pub const STRUCTURED_SCHEMA: &str =
    r#"{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}"#;
pub const STRUCTURED_INPUT: &str = r#"{"answer":"forty-two"}"#;

/// The id of the tool call a [`Mode::Tool`] answer makes.
pub const TOOL_USE_ID: &str = "toolu_contract_1";

/// How the fake answers one `POST /v1/messages`.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Stream [`FAKE_THINKING`] then [`FAKE_TEXT`], and end the turn.
    Text,
    /// Stream a text delta every 250 ms until the client hangs up.
    Slow,
    /// One call to the offered tool `name` with `args`, while the request
    /// carries no tool result yet; otherwise a [`Mode::Text`] answer.
    Tool {
        name: &'static str,
        args: &'static str,
    },
    /// One `StructuredOutput` call carrying [`STRUCTURED_INPUT`], while the
    /// request offers that tool and carries no tool result yet.
    StructuredOutput,
    /// Refuse with this HTTP status. With `rate_limit_headers`, the
    /// `anthropic-ratelimit-unified-*` set rides along, its status
    /// `unified_status`.
    Http {
        status: u16,
        rate_limit_headers: bool,
        unified_status: &'static str,
    },
    /// A [`Mode::Text`] answer carrying the unified rate-limit headers at
    /// `allowed_warning`.
    RateLimitHeaders,
}

/// One request the fake answered (egress is kept apart).
#[derive(Debug, Clone)]
pub struct Served {
    pub method: String,
    pub target: String,
    pub bearer: String,
    pub api_key: String,
    pub body: Value,
}

impl Served {
    /// An inference request: a Messages call, not a token count.
    pub fn is_inference(&self) -> bool {
        let path = self.target.split('?').next().unwrap_or_default();
        self.method == "POST" && path == "/v1/messages"
    }

    /// The names of the tools the request offered the model.
    pub fn tools(&self) -> Vec<String> {
        offered_tools(&self.body)
    }

    /// Every `tool_result` block in any message of the request. The CLI's
    /// last message is not always the one carrying it (it can end with a
    /// `role: system` message), so all are scanned.
    pub fn tool_results(&self) -> Vec<Value> {
        tool_results(&self.body)
    }
}

#[derive(Default)]
struct State {
    queue: VecDeque<Mode>,
    served: Vec<Served>,
    egress: Vec<String>,
    refused_tokens: Vec<String>,
    counter: usize,
}

pub struct FakeMessages {
    addr: String,
    state: Arc<Mutex<State>>,
}

impl FakeMessages {
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

    /// `ANTHROPIC_BASE_URL`.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The proxy every non-loopback request is sent through, and refused by.
    pub fn proxy_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Answer the next inference requests with `modes`, in order; afterwards
    /// [`Mode::Text`].
    pub fn queue(&self, modes: impl IntoIterator<Item = Mode>) {
        self.state.lock().unwrap().queue.extend(modes);
    }

    /// Answer any request carrying this bearer token with a 401.
    pub fn refuse(&self, token: &str) {
        self.state
            .lock()
            .unwrap()
            .refused_tokens
            .push(token.to_string());
    }

    pub fn served(&self) -> Vec<Served> {
        self.state.lock().unwrap().served.clone()
    }

    /// The inference requests, in order.
    pub fn inference(&self) -> Vec<Served> {
        self.served()
            .into_iter()
            .filter(Served::is_inference)
            .collect()
    }

    /// Whether any request body sent `needle` to the model.
    pub fn sent_to_the_model(&self, needle: &str) -> bool {
        self.served()
            .iter()
            .any(|served| served.body.to_string().contains(needle))
    }

    /// Every attempt to reach anything but this fake.
    pub fn egress(&self) -> Vec<String> {
        self.state.lock().unwrap().egress.clone()
    }
}

/// The names of the tools a Messages request body offers the model.
pub fn offered_tools(body: &Value) -> Vec<String> {
    body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect()
}

fn tool_results(body: &Value) -> Vec<Value> {
    body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|message| message["content"].as_array())
        .flatten()
        .filter(|block| block["type"] == "tool_result")
        .cloned()
        .collect()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

/// The `anthropic-ratelimit-unified-*` set the CLI builds its
/// `rate_limit_event` from (under OAuth only), its overall and five-hour
/// status `status`.
fn rate_limit_headers(status: &str) -> Vec<(String, String)> {
    let reset = (now() + 3600).to_string();
    let week = (now() + 3 * 86_400).to_string();
    let five_hour_utilization = if status == "rejected" { "1.0" } else { "0.91" };
    [
        ("status", status),
        ("reset", reset.as_str()),
        ("representative-claim", "five_hour"),
        ("5h-status", status),
        ("5h-reset", reset.as_str()),
        ("5h-utilization", five_hour_utilization),
        ("5h-surpassed-threshold", "0.9"),
        ("7d-status", "allowed"),
        ("7d-reset", week.as_str()),
        ("7d-utilization", "0.40"),
        ("fallback", "available"),
        ("overage-status", "rejected"),
        ("overage-disabled-reason", "org_level_disabled"),
    ]
    .into_iter()
    .map(|(name, value)| {
        (
            format!("anthropic-ratelimit-unified-{name}"),
            value.to_string(),
        )
    })
    .collect()
}

fn respond(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    headers: &[(String, String)],
    body: &str,
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nrequest-id: req_contract\r\nconnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

fn sse(event: &str, data: Value) -> Vec<u8> {
    format!("event: {event}\ndata: {data}\n\n").into_bytes()
}

fn serve(mut stream: TcpStream, state: &Mutex<State>) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let (mut content_length, mut bearer, mut api_key) = (0usize, String::new(), String::new());
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header == "\r\n" || header == "\n" {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            let value = value.trim();
            match name.trim().to_ascii_lowercase().as_str() {
                "content-length" => content_length = value.parse().unwrap_or(0),
                "authorization" => bearer = value.trim_start_matches("Bearer ").to_string(),
                "x-api-key" => api_key = value.to_string(),
                _ => {}
            }
        }
    }

    if method == "CONNECT" || target.starts_with("http://") || target.starts_with("https://") {
        state
            .lock()
            .unwrap()
            .egress
            .push(format!("{method} {target}"));
        return respond(&mut stream, 403, "Forbidden", &[], "");
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let path = target.split('?').next().unwrap_or_default().to_string();
    let inference = method == "POST" && path == "/v1/messages";

    let (mode, n, refused) = {
        let mut state = state.lock().unwrap();
        let refused = state.refused_tokens.contains(&bearer);
        let mode = if inference && !refused {
            state.counter += 1;
            Some(state.queue.pop_front().unwrap_or(Mode::Text))
        } else {
            None
        };
        state.served.push(Served {
            method: method.clone(),
            target: target.clone(),
            bearer: bearer.clone(),
            api_key,
            body: body.clone(),
        });
        (mode, state.counter, refused)
    };

    if refused {
        return respond(
            &mut stream,
            401,
            "Unauthorized",
            &[],
            r#"{"type":"error","error":{"type":"authentication_error","message":"OAuth token has expired."}}"#,
        );
    }
    if path.ends_with("/count_tokens") {
        return respond(&mut stream, 200, "OK", &[], r#"{"input_tokens":42}"#);
    }
    let Some(mode) = mode else {
        return respond(
            &mut stream,
            404,
            "Not Found",
            &[],
            r#"{"type":"error","error":{"type":"not_found_error","message":"claude-contract fake: no such route"}}"#,
        );
    };

    let mut headers = Vec::new();
    let mode = match mode {
        Mode::Http {
            status,
            rate_limit_headers: with_headers,
            unified_status,
        } => {
            if with_headers {
                headers = rate_limit_headers(unified_status);
            }
            headers.push(("retry-after".to_string(), "0".to_string()));
            let (kind, message) = match status {
                429 => ("rate_limit_error", "fake rate limit"),
                529 => ("overloaded_error", "fake overloaded"),
                401 => ("authentication_error", "fake unauthorized"),
                _ => ("api_error", "fake internal error"),
            };
            let body = json!({"type": "error", "error": {"type": kind, "message": message}});
            return respond(&mut stream, status, "Error", &headers, &body.to_string());
        }
        Mode::RateLimitHeaders => {
            headers = rate_limit_headers("allowed_warning");
            Mode::Text
        }
        other => other,
    };

    let model = body["model"]
        .as_str()
        .unwrap_or("claude-contract")
        .to_string();
    let tools = offered_tools(&body);
    let answered = !tool_results(&body).is_empty();
    let mode = match mode {
        Mode::Tool { name, .. } if answered || !tools.iter().any(|tool| tool == name) => Mode::Text,
        Mode::StructuredOutput
            if answered || !tools.iter().any(|tool| tool == "StructuredOutput") =>
        {
            Mode::Text
        }
        other => other,
    };

    let streaming = body["stream"].as_bool().unwrap_or(false);
    let message_id = format!("msg_contract_{n}");
    if !streaming {
        let answer = json!({"id": message_id, "type": "message", "role": "assistant",
            "model": model, "content": [{"type": "text", "text": FAKE_TEXT}],
            "stop_reason": "end_turn", "stop_sequence": null,
            "usage": {"input_tokens": USAGE_INPUT, "output_tokens": USAGE_OUTPUT}});
        return respond(&mut stream, 200, "OK", &headers, &answer.to_string());
    }

    let mut head = String::from(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nrequest-id: req_contract\r\nconnection: close\r\n",
    );
    for (name, value) in &headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(&sse(
        "message_start",
        json!({"type": "message_start", "message": {"id": message_id, "type": "message",
            "role": "assistant", "model": model, "content": [], "stop_reason": null,
            "stop_sequence": null, "usage": {"input_tokens": USAGE_INPUT, "output_tokens": 1,
            "cache_creation_input_tokens": USAGE_CACHE_CREATION,
            "cache_read_input_tokens": USAGE_CACHE_READ}}}),
    ))?;
    stream.flush()?;

    let block_start = |index: usize, block: Value| {
        sse(
            "content_block_start",
            json!({"type": "content_block_start", "index": index, "content_block": block}),
        )
    };
    let delta = |index: usize, delta: Value| {
        sse(
            "content_block_delta",
            json!({"type": "content_block_delta", "index": index, "delta": delta}),
        )
    };
    let block_stop = |index: usize| {
        sse(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": index}),
        )
    };
    let finish = |stop: &str| {
        let mut tail = sse(
            "message_delta",
            json!({"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": null},
                "usage": {"output_tokens": USAGE_OUTPUT}}),
        );
        tail.extend(sse("message_stop", json!({"type": "message_stop"})));
        tail
    };
    let tool_call = |name: &str, args: &str| {
        let mut call = block_start(
            0,
            json!({"type": "tool_use", "id": TOOL_USE_ID, "name": name, "input": {}}),
        );
        call.extend(delta(
            0,
            json!({"type": "input_json_delta", "partial_json": args}),
        ));
        call.extend(block_stop(0));
        call.extend(finish("tool_use"));
        call
    };

    match mode {
        Mode::Slow => {
            stream.write_all(&block_start(0, json!({"type": "text", "text": ""})))?;
            for tick in 0..480 {
                stream.write_all(&delta(
                    0,
                    json!({"type": "text_delta", "text": format!("tick{tick} ")}),
                ))?;
                stream.flush()?;
                std::thread::sleep(Duration::from_millis(250));
            }
        }
        Mode::Tool { name, args } => stream.write_all(&tool_call(name, args))?,
        Mode::StructuredOutput => {
            stream.write_all(&tool_call("StructuredOutput", STRUCTURED_INPUT))?
        }
        _ => {
            stream.write_all(&block_start(
                0,
                json!({"type": "thinking", "thinking": "", "signature": ""}),
            ))?;
            stream.write_all(&delta(
                0,
                json!({"type": "thinking_delta", "thinking": FAKE_THINKING}),
            ))?;
            stream.write_all(&delta(
                0,
                json!({"type": "signature_delta", "signature": "Y29udHJhY3Q="}),
            ))?;
            stream.write_all(&block_stop(0))?;
            stream.write_all(&block_start(1, json!({"type": "text", "text": ""})))?;
            for chunk in TEXT_CHUNKS {
                stream.write_all(&delta(1, json!({"type": "text_delta", "text": chunk})))?;
            }
            stream.write_all(&block_stop(1))?;
            stream.write_all(&finish("end_turn"))?;
        }
    }
    stream.flush()
}

/// A streamable-HTTP MCP server offering Cairn's three verbs, attached as
/// Cairn attaches its own (`McpServer::cairn_http`), answering every request
/// with plain JSON (no SSE stream).
pub struct McpStub {
    pub url: String,
    /// The JSON-RPC methods called, in order, and the bearer each carried.
    calls: Arc<Mutex<Vec<(String, String)>>>,
    /// Every JSON-RPC message posted, whole, in order.
    bodies: Arc<Mutex<Vec<Value>>>,
}

/// What every `tools/call` answers.
pub const MCP_TOOL_TEXT: &str = "contract read ok";

impl McpStub {
    pub fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}/mcp", listener.local_addr()?);
        let calls: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let (record, keep) = (calls.clone(), bodies.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (record, keep) = (record.clone(), keep.clone());
                std::thread::spawn(move || {
                    let _ = serve_mcp(stream, &record, &keep);
                });
            }
        });
        Ok(Self { url, calls, bodies })
    }

    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    pub fn bodies(&self) -> Vec<Value> {
        self.bodies.lock().unwrap().clone()
    }
}

fn serve_mcp(
    stream: TcpStream,
    record: &Mutex<Vec<(String, String)>>,
    keep: &Mutex<Vec<Value>>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(());
    }
    let method = request_line
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string();
    let (mut length, mut token) = (0usize, String::new());
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 || line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                length = value.parse().unwrap_or(0);
            } else if name.eq_ignore_ascii_case("authorization") {
                token = value.trim_start_matches("Bearer ").to_string();
            }
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    let (status, payload) = if method == "POST" {
        let request = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
        let rpc = request["method"].as_str().unwrap_or("").to_string();
        record.lock().unwrap().push((rpc.clone(), token));
        keep.lock().unwrap().push(request.clone());
        mcp_reply(&rpc, &request)
    } else {
        // No server-initiated stream: a GET for one is refused, which the
        // client takes as "none offered".
        ("405 Method Not Allowed", String::new())
    };
    let mut stream = stream;
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    )?;
    stream.flush()
}

/// The status and body answering one JSON-RPC message.
fn mcp_reply(method: &str, request: &Value) -> (&'static str, String) {
    let Some(id) = request.get("id").cloned() else {
        // A notification (`notifications/initialized`): accepted, no body.
        return ("202 Accepted", String::new());
    };
    let answer = match method {
        "initialize" => json!({
            "protocolVersion": request["params"]["protocolVersion"]
                .as_str()
                .unwrap_or("2025-06-18"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "cairn-contract-stub", "version": "0"},
        }),
        "tools/list" => {
            let tools = ["read", "write", "run"].map(|name| {
                json!({
                    "name": name,
                    "description": format!("The stub's {name} verb."),
                    "inputSchema": {"type": "object", "properties": {}},
                })
            });
            json!({ "tools": tools })
        }
        "tools/call" => json!({"content": [{"type": "text", "text": MCP_TOOL_TEXT}]}),
        _ => {
            let error = json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "method not found"}});
            return ("200 OK", error.to_string());
        }
    };
    (
        "200 OK",
        json!({"jsonrpc": "2.0", "id": id, "result": answer}).to_string(),
    )
}
