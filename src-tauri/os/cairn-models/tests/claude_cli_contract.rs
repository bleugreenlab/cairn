//! The installed Claude CLI's side of credential rotation, pinned.
//!
//! A managed Claude process runs on an access token alone, and Cairn rotates
//! that token under it by writing `update_environment_variables` to the
//! process's stream-json stdin (`ClaudeControl::update_credential`). That
//! message is in the CLI's SDK input schema but undocumented for third
//! parties, so this test holds the CLI to the two behaviours Cairn relies on:
//!
//! - a token pushed between turns is the one the next turn sends, and
//! - with `CLAUDE_CODE_OAUTH_401_WAIT_MS` set, a 401 makes the CLI report
//!   `system/api_retry {error_status: 401}` and wait, and a token pushed then
//!   rescues the same turn.
//!
//! It also holds the CLI to the tool surface Cairn launches it with: under
//! `--tools ""` no built-in tool is offered, while the tools of a configured
//! MCP server and the `--json-schema` StructuredOutput answer both survive,
//! and every MCP tool call asks for progress with `_meta.progressToken`,
//! which Cairn's heartbeat on held-open calls relies on.
//! Those launches use the argument vector of Cairn's own
//! [`ClaudeCli::spawn_config`], so the test pins Cairn's arguments rather
//! than a copy of them.
//!
//! It also holds the CLI to how Cairn hands it a credential: on an inherited
//! descriptor (`CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR` for a managed
//! account's token, `CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR` for an API key), so
//! the credential is in neither the CLI's launch environment, which every
//! same-user process can read, nor anything the CLI starts, nor any file it
//! writes, and no token endpoint is ever called.
//!
//! And it holds the CLI to the home Cairn names: every launch carries its own
//! `CLAUDE_CONFIG_DIR`, and with it the operator's `~/.claude` (their hooks,
//! `statusLine`, `CLAUDE.md`, and `~/.claude.json` MCP servers) loads into no
//! launch.
//!
//! It runs the real `claude` from PATH, in a fresh home, against a loopback
//! Messages API stub (and, for the tool surface, a loopback MCP server), with
//! every HTTPS request sent to a proxy that records and refuses it. Every launch
//! is Cairn's own: the arguments and environment of [`ClaudeCli::spawn_config`],
//! the credential and home of [`ClaudeSessionCredential::for_launch`] or
//! [`api_key_launch`], spawned by [`RealProcessSpawner`]. Only dummy tokens
//! and keys are used. Where no `claude` is
//! installed it skips; where one is, a CLI upgrade that drops the contract
//! fails here.
//!
//! With `CAIRN_RECORD_CLAUDE_INIT=<dir>` set, the tool-surface tests write the
//! CLI's raw `system`/`init` event to `<dir>/claude_init_<case>-<version>.json`,
//! the version being the CLI's own `claude_code_version`. The committed copies
//! under `tests/fixtures/` come from such a run on the CLI production agents
//! run, and are re-recorded when that CLI moves.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_models::claude::api_key::{api_key_launch, API_KEY_ENV};
use cairn_models::claude::tokens::{ClaudeOAuth, ClaudeSessionCredential, OAUTH_TOKEN_ENV};
use cairn_models::interface::session::{
    ApprovalMode, ClaudeMode, ClaudeSpec, PlacedFile, ProviderSpec, SecretEnv, SessionControl,
    SessionSpec, SessionStart,
};
use cairn_models::session::claude::{
    credential_push_generation, ClaudeCli, ClaudeControl, API_KEY_FD_ENV, OAUTH_TOKEN_FD_ENV,
};
use cairn_models::session::mcp::{McpServer, McpServers, McpTransport, MCP_TOKEN_ENV};
use cairn_models::wire::claude_stream::send_user_message;
use cairn_models::wire::MessageContent;
use cairn_platform::services::process::{ChildProcess, ProcessSpawner, RealProcessSpawner};
use serde_json::{json, Value};

const WAIT: Duration = Duration::from_secs(90);

/// A credential's variable and the variable naming its descriptor.
const OAUTH: (&str, &str) = (OAUTH_TOKEN_ENV, OAUTH_TOKEN_FD_ENV);
const API_KEY: (&str, &str) = (API_KEY_ENV, API_KEY_FD_ENV);

/// A Messages API that answers every request whose bearer token it has not
/// been told to refuse, and records which token or API key each request
/// carried, what it sent, and which tools it offered the model.
struct Stub {
    base_url: String,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    api_keys: Arc<Mutex<Vec<(String, String)>>>,
    inference_bodies: Arc<Mutex<Vec<String>>>,
    refused: Arc<Mutex<Vec<String>>>,
    offered: Arc<Mutex<Vec<Vec<String>>>>,
}

/// How the stub answers a streaming Messages request.
#[derive(Clone, Copy)]
enum Answer {
    /// Always a short text turn.
    Text,
    /// A `StructuredOutput` tool call carrying [`STRUCTURED_INPUT`] whenever
    /// the request offers that tool and has no tool result yet; a short text
    /// turn otherwise.
    StructuredOutput,
    /// A call to Cairn's `read` verb whenever the request offers it and has
    /// no tool result yet; a short text turn otherwise.
    McpRead,
}

const SSE_MCP_READ: &str = concat!(
    "event: message_start\n",
    r#"data: {"type":"message_start","message":{"id":"msg_stub_read","type":"message","role":"assistant","model":"claude-stub","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}"#,
    "\n\n",
    "event: content_block_start\n",
    r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_stub_read","name":"mcp__cairn__read","input":{}}}"#,
    "\n\n",
    "event: content_block_delta\n",
    r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}"#,
    "\n\n",
    "event: content_block_stop\n",
    r#"data: {"type":"content_block_stop","index":0}"#,
    "\n\n",
    "event: message_delta\n",
    r#"data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":5}}"#,
    "\n\n",
    "event: message_stop\n",
    r#"data: {"type":"message_stop"}"#,
    "\n\n",
);

/// The schema the StructuredOutput case constrains its answer to, and the
/// input the stub's tool call fills it with.
const STRUCTURED_SCHEMA: &str =
    r#"{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}"#;
const STRUCTURED_INPUT: &str = r#"{"answer":"forty-two"}"#;

const SSE_STRUCTURED_OUTPUT: &str = concat!(
    "event: message_start\n",
    r#"data: {"type":"message_start","message":{"id":"msg_stub_tool","type":"message","role":"assistant","model":"claude-stub","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}"#,
    "\n\n",
    "event: content_block_start\n",
    r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_stub_structured","name":"StructuredOutput","input":{}}}"#,
    "\n\n",
    "event: content_block_delta\n",
    r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"answer\":\"forty-two\"}"}}"#,
    "\n\n",
    "event: content_block_stop\n",
    r#"data: {"type":"content_block_stop","index":0}"#,
    "\n\n",
    "event: message_delta\n",
    r#"data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":5}}"#,
    "\n\n",
    "event: message_stop\n",
    r#"data: {"type":"message_stop"}"#,
    "\n\n",
);

const SSE: &str = concat!(
    "event: message_start\n",
    r#"data: {"type":"message_start","message":{"id":"msg_stub","type":"message","role":"assistant","model":"claude-stub","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}"#,
    "\n\n",
    "event: content_block_start\n",
    r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
    "\n\n",
    "event: content_block_delta\n",
    r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello from stub"}}"#,
    "\n\n",
    "event: content_block_stop\n",
    r#"data: {"type":"content_block_stop","index":0}"#,
    "\n\n",
    "event: message_delta\n",
    r#"data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":3}}"#,
    "\n\n",
    "event: message_stop\n",
    r#"data: {"type":"message_stop"}"#,
    "\n\n",
);

const MESSAGE: &str = r#"{"id":"msg_stub","type":"message","role":"assistant","model":"claude-stub","content":[{"type":"text","text":"hello from stub"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":3}}"#;

const UNAUTHORIZED: &str = r#"{"type":"error","error":{"type":"authentication_error","message":"OAuth token has expired."}}"#;

/// The names of the tools a Messages request body offers the model.
fn offered_tools(body: &Value) -> Vec<String> {
    body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect()
}

/// Whether a Messages request body already carries a tool result.
fn has_tool_result(body: &Value) -> bool {
    body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|message| message["content"].as_array())
        .flatten()
        .any(|block| block["type"] == "tool_result")
}

impl Stub {
    fn start() -> Self {
        Self::answering(Answer::Text)
    }

    fn answering(answer: Answer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let api_keys: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let inference_bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let refused: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let offered: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let shared = (
            seen.clone(),
            api_keys.clone(),
            inference_bodies.clone(),
            refused.clone(),
            offered.clone(),
        );
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (record, record_key, keep_body, refuse, offer) = shared.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
                        return;
                    }
                    let path = request_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .to_string();
                    let (mut length, mut token, mut api_key) =
                        (0usize, String::new(), String::new());
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            let value = value.trim();
                            if name.eq_ignore_ascii_case("content-length") {
                                length = value.parse().unwrap_or(0);
                            } else if name.eq_ignore_ascii_case("authorization") {
                                token = value.trim_start_matches("Bearer ").to_string();
                            } else if name.eq_ignore_ascii_case("x-api-key") {
                                api_key = value.to_string();
                            }
                        }
                    }
                    let mut body = vec![0u8; length];
                    let _ = reader.read_exact(&mut body);
                    let raw_body = String::from_utf8_lossy(&body).into_owned();
                    let body = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
                    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
                    let tools = offered_tools(&body);
                    record.lock().unwrap().push((path.clone(), token.clone()));
                    record_key.lock().unwrap().push((path.clone(), api_key));
                    if path.starts_with("/v1/messages") && !path.contains("count_tokens") {
                        offer.lock().unwrap().push(tools.clone());
                        keep_body.lock().unwrap().push(raw_body);
                    }
                    let structured = matches!(answer, Answer::StructuredOutput)
                        && tools.iter().any(|tool| tool == "StructuredOutput")
                        && !has_tool_result(&body);
                    let mcp_read = matches!(answer, Answer::McpRead)
                        && tools.iter().any(|tool| tool == "mcp__cairn__read")
                        && !has_tool_result(&body);
                    let (status, content_type, payload) = if refuse.lock().unwrap().contains(&token)
                    {
                        ("401 Unauthorized", "application/json", UNAUTHORIZED)
                    } else if path.starts_with("/v1/messages") && streaming && structured {
                        ("200 OK", "text/event-stream", SSE_STRUCTURED_OUTPUT)
                    } else if path.starts_with("/v1/messages") && streaming && mcp_read {
                        ("200 OK", "text/event-stream", SSE_MCP_READ)
                    } else if path.starts_with("/v1/messages") && streaming {
                        ("200 OK", "text/event-stream", SSE)
                    } else if path.starts_with("/v1/messages") {
                        ("200 OK", "application/json", MESSAGE)
                    } else {
                        ("200 OK", "application/json", "{}")
                    };
                    let mut stream = stream;
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                });
            }
        });
        Self {
            base_url,
            seen,
            api_keys,
            inference_bodies,
            refused,
            offered,
        }
    }

    /// The API keys inference requests carried, in order (empty for one that
    /// carried none).
    fn inference_api_keys(&self) -> Vec<String> {
        self.api_keys
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.starts_with("/v1/messages") && !path.contains("count_tokens"))
            .map(|(_, key)| key.clone())
            .collect()
    }

    /// Whether any inference request sent `needle` to the model.
    fn sent_to_the_model(&self, needle: &str) -> bool {
        self.inference_bodies
            .lock()
            .unwrap()
            .iter()
            .any(|body| body.contains(needle))
    }

    /// The tool names each inference request offered the model, in order.
    fn offered(&self) -> Vec<Vec<String>> {
        self.offered.lock().unwrap().clone()
    }

    fn refuse(&self, token: &str) {
        self.refused.lock().unwrap().push(token.to_string());
    }

    /// The tokens inference requests carried, in order.
    fn inference_tokens(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.starts_with("/v1/messages") && !path.contains("count_tokens"))
            .map(|(_, token)| token.clone())
            .collect()
    }
}

/// A proxy that accepts and immediately hangs up, so any HTTPS request the
/// CLI makes (telemetry, the real token endpoint) fails without leaving the
/// machine. It keeps the target each request asked for.
struct Proxy {
    url: String,
    targets: Arc<Mutex<Vec<String>>>,
}

fn refusing_proxy() -> Proxy {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let targets: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let record = targets.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let record = record.clone();
            std::thread::spawn(move || {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut line = String::new();
                let _ = BufReader::new(&stream).read_line(&mut line);
                if let Some(target) = line.split_whitespace().nth(1) {
                    record.lock().unwrap().push(target.to_string());
                }
            });
        }
    });
    Proxy { url, targets }
}

/// A streamable-HTTP MCP server offering Cairn's three verbs, answering
/// every request with plain JSON (no SSE stream).
struct McpStub {
    url: String,
    /// The JSON-RPC methods called, in order, and the bearer each carried.
    calls: Arc<Mutex<Vec<(String, String)>>>,
    /// Every JSON-RPC message posted, whole, in order.
    bodies: Arc<Mutex<Vec<Value>>>,
}

impl McpStub {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let calls: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let (record, keep) = (calls.clone(), bodies.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (record, keep) = (record.clone(), keep.clone());
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
                        return;
                    }
                    let method = request_line.split_whitespace().next().unwrap_or("");
                    let (mut length, mut token) = (0usize, String::new());
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
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
                    let _ = reader.read_exact(&mut body);
                    let (status, payload) = if method == "POST" {
                        let request = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
                        let rpc = request["method"].as_str().unwrap_or("").to_string();
                        record.lock().unwrap().push((rpc.clone(), token));
                        keep.lock().unwrap().push(request.clone());
                        mcp_reply(&rpc, &request)
                    } else {
                        // No server-initiated stream: a GET for one is refused,
                        // which the client takes as "none offered".
                        ("405 Method Not Allowed", String::new())
                    };
                    let mut stream = stream;
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                        payload.len()
                    );
                });
            }
        });
        Self { url, calls, bodies }
    }

    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    fn bodies(&self) -> Vec<Value> {
        self.bodies.lock().unwrap().clone()
    }
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
            "serverInfo": {"name": "cairn-stub", "version": "0"},
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
        "tools/call" => json!({"content": [{"type": "text", "text": "ok"}]}),
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

type Stdin = Arc<Mutex<Option<Box<dyn Write + Send>>>>;

struct SharedStdin(Stdin);

impl Write for SharedStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0.lock().unwrap().as_mut() {
            Some(stdin) => stdin.write(buf),
            None => Err(std::io::ErrorKind::BrokenPipe.into()),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self.0.lock().unwrap().as_mut() {
            Some(stdin) => stdin.flush(),
            None => Ok(()),
        }
    }
}

/// A fresh home for one CLI: its working directory, and the CLI home inside
/// it that the launch credential names.
struct Home {
    dir: tempfile::TempDir,
    config: PathBuf,
}

impl Home {
    fn new() -> Self {
        Self::with_config("claude-home")
    }

    /// A home whose CLI home is `relative` inside it.
    fn with_config(relative: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(relative);
        std::fs::create_dir_all(&config).unwrap();
        Self { dir, config }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }
}

struct Cli {
    child: Box<dyn ChildProcess>,
    stdin: Stdin,
    events: Receiver<Value>,
    /// Every event read so far, for a failure to show.
    history: Mutex<Vec<Value>>,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    stderr: Arc<Mutex<String>>,
    control: ClaudeControl,
    proxy: Proxy,
    home: Home,
}

impl Cli {
    fn send(&mut self, text: &str) {
        let mut stdin = SharedStdin(self.stdin.clone());
        send_user_message(&mut stdin, "", &MessageContent::text(text), None).unwrap();
    }

    /// Hand a print-mode process its prompt on stdin, then close it, as
    /// `ClaudeCli` does for a one-shot.
    fn feed_and_close(&mut self, prompt: &str) {
        let mut stdin = self.stdin.lock().unwrap().take().unwrap();
        stdin.write_all(prompt.as_bytes()).unwrap();
    }

    fn init(&self) -> Value {
        self.until("a system/init event", |event| {
            event["type"] == "system" && event["subtype"] == "init"
        })
    }

    fn push(&mut self, token: &str, generation: u64) {
        let env = SecretEnv::new().with(OAUTH_TOKEN_ENV, token);
        self.control.update_credential(&env, generation).unwrap();
    }

    /// Read events until `matches` accepts one, failing on timeout or exit.
    fn until(&self, what: &str, matches: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.events.recv_timeout(remaining) {
                Ok(event) => {
                    self.history.lock().unwrap().push(event.clone());
                    if matches(&event) {
                        return event;
                    }
                }
                Err(_) => panic!(
                    "the CLI never produced {what}.\nprocess: {}\nevents:\n{}\nrequests: {:?}\nstderr:\n{}",
                    process_report(self.child.id()),
                    self.history
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|event| {
                            let line = event.to_string();
                            line.chars().take(400).collect::<String>()
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                    self.seen.lock().unwrap(),
                    self.stderr.lock().unwrap()
                ),
            }
        }
    }

    fn turn_result(&self) -> Value {
        self.until("a result", |event| event["type"] == "result")
    }

    /// The CLI's launch environment holds none of `secrets` and does not name
    /// the credential's variable; it does name the variable of the descriptor
    /// that holds it (`credential` is the pair, [`OAUTH`] or [`API_KEY`]). The
    /// stub's address, which the launch does put there, is the control that
    /// the environment was read at all.
    fn assert_launch_environment_holds_none_of(
        &self,
        secrets: &[&str],
        stub: &Stub,
        (credential_env, descriptor_env): (&str, &str),
    ) {
        let env = LaunchEnvironment::of(self.child.id());
        assert!(
            env.holds(&stub.base_url),
            "the launch environment reader read nothing"
        );
        for secret in secrets {
            assert!(
                !env.holds(secret),
                "a credential is in the CLI's launch environment"
            );
        }
        assert!(
            !env.names().any(|name| name == credential_env),
            "the CLI's launch environment names {credential_env}"
        );
        assert!(
            env.names().any(|name| name == descriptor_env),
            "the CLI was not told which descriptor holds its credential ({descriptor_env})"
        );
    }

    /// Nothing asked for a token: no request reached an OAuth path, through
    /// the stub or the proxy, and the CLI wrote no credential into its home.
    fn assert_no_token_was_fetched(&self) {
        let paths: Vec<String> = self
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|(path, _)| path.clone())
            .collect();
        assert!(
            !paths.iter().any(|path| path.contains("oauth")),
            "the CLI called a token endpoint: {paths:?}"
        );
        let targets = self.proxy.targets.lock().unwrap().clone();
        assert!(
            !targets.iter().any(|target| {
                ["platform.claude.com", "console.anthropic.com", "claude.ai"]
                    .iter()
                    .any(|host| target.starts_with(host))
            }),
            "the CLI reached for a token endpoint: {targets:?}"
        );
        let written: Vec<String> = std::fs::read_dir(&self.home.config)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("credentials"))
            .collect();
        assert!(
            written.is_empty(),
            "the CLI wrote {written:?} into its home"
        );
    }
}

impl Drop for Cli {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && !matches!(self.child.try_wait(), Ok(Some(_))) {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// A live process's launch environment, the block `KERN_PROCARGS2` and
/// `/proc/<pid>/environ` show any process of the same user. Only names and a
/// containment check leave it, never a value: on a real machine the values
/// would be real credentials.
struct LaunchEnvironment(Vec<Vec<u8>>);

impl LaunchEnvironment {
    fn of(pid: u32) -> Self {
        Self(launch_environment_entries(pid))
    }

    fn names(&self) -> impl Iterator<Item = String> + '_ {
        self.0.iter().filter_map(|entry| {
            let name = entry.split(|byte| *byte == b'=').next()?;
            Some(String::from_utf8_lossy(name).into_owned())
        })
    }

    fn holds(&self, needle: &str) -> bool {
        let needle = needle.as_bytes();
        self.0
            .iter()
            .any(|entry| entry.windows(needle.len()).any(|window| window == needle))
    }
}

/// `KERN_PROCARGS2` is the argument count, the executable path, NUL padding,
/// the arguments, then the environment up to the first empty string.
#[cfg(target_os = "macos")]
fn launch_environment_entries(pid: u32) -> Vec<Vec<u8>> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid as libc::c_int];
    let mut buf = vec![0u8; 1 << 20];
    let mut size = buf.len();
    // SAFETY: `mib` names three levels and `buf` is writable for `size` bytes.
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buf.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(
        rc,
        0,
        "KERN_PROCARGS2 refused pid {pid}: {}",
        std::io::Error::last_os_error()
    );
    buf.truncate(size);
    let argc = i32::from_ne_bytes(buf[..4].try_into().unwrap()) as usize;
    let mut fields = buf[4..].split(|byte| *byte == 0);
    fields.next();
    let mut fields = fields.skip_while(|field| field.is_empty());
    for _ in 0..argc {
        fields.next();
    }
    fields
        .take_while(|field| !field.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

#[cfg(not(target_os = "macos"))]
fn launch_environment_entries(pid: u32) -> Vec<Vec<u8>> {
    std::fs::read(format!("/proc/{pid}/environ"))
        .unwrap()
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

/// Where a process is stuck, for a failure to show: its state, what it waits
/// in, and what its descriptors are. Nothing here is a value it holds.
#[cfg(target_os = "linux")]
fn process_report(pid: u32) -> String {
    let read = |name: &str| {
        std::fs::read_to_string(format!("/proc/{pid}/{name}")).unwrap_or_else(|e| e.to_string())
    };
    let state = read("status")
        .lines()
        .find(|line| line.starts_with("State:"))
        .unwrap_or("State: unknown")
        .to_string();
    let fds: Vec<String> = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| {
                    let target = std::fs::read_link(entry.path())
                        .map(|link| link.display().to_string())
                        .unwrap_or_default();
                    format!("{}={target}", entry.file_name().to_string_lossy())
                })
                .collect()
        })
        .unwrap_or_default();
    format!("{state}; wchan {}; fds {fds:?}", read("wchan"))
}

#[cfg(not(target_os = "linux"))]
fn process_report(pid: u32) -> String {
    format!("pid {pid}")
}

fn installed_claude() -> Option<(String, String)> {
    let output = Command::new("claude").arg("--version").output().ok()?;
    output.status.success().then(|| {
        (
            "claude".to_string(),
            String::from_utf8_lossy(&output.stdout).trim().to_string(),
        )
    })
}

/// What Cairn hands a launch on a managed account whose access token is
/// `token`.
fn launch_credential(token: &str, home: &Home) -> SecretEnv {
    let oauth = ClaudeOAuth::from_cli_credentials(
        &json!({"claudeAiOauth": {"accessToken": token, "expiresAt": 4_102_444_800_000i64}})
            .to_string(),
    )
    .unwrap();
    ClaudeSessionCredential::for_launch(&oauth, &home.config).env
}

/// Launch the CLI exactly as Cairn launches `spec`: the arguments and
/// environment `ClaudeCli::spawn_config` gives it, spawned by Cairn's own
/// spawner, with nothing inherited beyond the sealed environment and `env`.
/// The CLI home is the one the spec's credential names, never one this harness
/// overrides, so the launch runs where Cairn's would.
fn launch_as_cairn(
    claude: &str,
    stub: &Stub,
    home: Home,
    spec: &SessionSpec,
    settings: Option<String>,
    env: &[(&str, &str)],
) -> Cli {
    let proxy = refusing_proxy();
    let mut config = ClaudeCli::spawn_config(claude, spec, None, settings).unwrap();
    assert_eq!(
        config.env.get("CLAUDE_CONFIG_DIR").map(PathBuf::from),
        Some(home.config.clone()),
        "the launch runs in the home its credential names"
    );
    let sealed = [
        ("PATH", std::env::var("PATH").unwrap_or_default()),
        ("HOME", home.path().display().to_string()),
        ("ANTHROPIC_BASE_URL", stub.base_url.clone()),
        // The CLI sends everything through its proxy, plain HTTP included;
        // only the loopback stubs are exempt.
        ("HTTPS_PROXY", proxy.url.clone()),
        ("NO_PROXY", "127.0.0.1,localhost".to_string()),
        ("DISABLE_AUTOUPDATER", "1".to_string()),
        ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1".to_string()),
    ];
    for (name, value) in sealed {
        config = config.env(name, &value);
    }
    for (name, value) in env {
        config = config.env(name, value);
    }
    let inherited: Vec<String> = std::env::vars_os()
        .filter_map(|(name, _)| name.into_string().ok())
        .filter(|name| !config.env.contains_key(name))
        .collect();
    for name in inherited {
        config = config.env_remove(&name);
    }
    config.cwd = Some(home.path().display().to_string());

    let mut child = RealProcessSpawner.spawn(config).expect("spawn claude");
    let stdin: Stdin = Arc::new(Mutex::new(child.take_stdin()));
    let stdout = child.take_stdout().unwrap();
    let (tx, events) = channel();
    std::thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            if let Ok(event) = serde_json::from_str::<Value>(&line) {
                if tx.send(event).is_err() {
                    return;
                }
            }
        }
    });
    let stderr = Arc::new(Mutex::new(String::new()));
    if let Some(pipe) = child.take_stderr() {
        let keep = stderr.clone();
        std::thread::spawn(move || {
            for line in pipe.lines().map_while(Result::ok) {
                let mut kept = keep.lock().unwrap();
                kept.push_str(&line);
                kept.push('\n');
            }
        });
    }
    let control = ClaudeControl::new(Box::new(SharedStdin(stdin.clone())));
    Cli {
        child,
        stdin,
        events,
        history: Mutex::new(Vec::new()),
        seen: stub.seen.clone(),
        stderr,
        control,
        proxy,
        home,
    }
}

fn acknowledged(event: &Value, generation: u64) -> bool {
    event["type"] == "control_response"
        && [&event["response"]["request_id"], &event["request_id"]]
            .iter()
            .filter_map(|id| id.as_str())
            .any(|id| credential_push_generation(id) == Some(generation))
}

/// A session launched on its token by descriptor authenticates with it, takes
/// a pushed token between turns and inside the 401 wait, never asks for a
/// token itself, and never has any of them in its launch environment.
#[test]
fn a_pushed_token_is_used_between_turns_and_rescues_a_turn_waiting_on_a_401() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning the credential-push contract against Claude CLI {version}");
    let stub = Stub::start();
    let home = Home::new();
    let spec = spec(
        &home,
        launch_credential("at-OLD", &home),
        None,
        ClaudeMode::Interactive,
        ApprovalMode::Allow,
        None,
        Vec::new(),
    );
    let mut cli = launch_as_cairn(&claude, &stub, home, &spec, None, &[]);

    cli.send("turn one");
    let first = cli.turn_result();
    assert_eq!(first["subtype"], "success", "{first}");
    assert_eq!(
        stub.inference_tokens().last().map(String::as_str),
        Some("at-OLD")
    );
    cli.assert_launch_environment_holds_none_of(&["at-OLD"], &stub, OAUTH);

    // Between turns: the old token is now refused, and the pushed one is what
    // the next turn sends.
    stub.refuse("at-OLD");
    cli.push("at-NEW", 2);
    cli.until("an acknowledgement of generation 2", |event| {
        acknowledged(event, 2)
    });
    cli.send("turn two");
    let second = cli.turn_result();
    assert_eq!(second["subtype"], "success", "{second}");
    assert_eq!(
        stub.inference_tokens().last().map(String::as_str),
        Some("at-NEW")
    );

    // Mid-turn: the current token is refused, the CLI reports the 401 and
    // waits, and a token pushed then carries the same turn's retry.
    stub.refuse("at-NEW");
    cli.send("turn three");
    cli.until("an api_retry for a 401", |event| {
        event["type"] == "system" && event["subtype"] == "api_retry" && event["error_status"] == 401
    });
    cli.push("at-NEWER", 3);
    let third = cli.turn_result();
    assert_eq!(third["subtype"], "success", "{third}");
    assert_eq!(
        stub.inference_tokens().last().map(String::as_str),
        Some("at-NEWER")
    );

    // A push changes only the process's memory, never the block every
    // same-user process can read.
    cli.assert_launch_environment_holds_none_of(&["at-OLD", "at-NEW", "at-NEWER"], &stub, OAUTH);
    cli.assert_no_token_was_fetched();
}

/// Records, for whatever runs it, whether its environment names or holds a
/// token and what it reads from descriptor 3. Run as `probe.sh <dir> <who>`.
const PROBE: &str = r#"dir=$1; tag=$2-$$
env > "$dir/$tag.env"
if (exec 9<&3) 2>/dev/null; then
  cat <&3 > "$dir/$tag.fd3"; echo open > "$dir/$tag.state"
else
  : > "$dir/$tag.fd3"; echo closed > "$dir/$tag.state"
fi
"#;

/// Hooks and a stdio MCP server (which is how `cairn-cmd` runs) that each
/// run [`PROBE`], recording into `probes` what they inherited.
struct Probes {
    probes: PathBuf,
    settings: PathBuf,
    servers: McpServers,
}

impl Probes {
    fn in_home(home: &Home) -> Self {
        let probes = home.path().join("probes");
        std::fs::create_dir_all(&probes).unwrap();
        let probe = home.path().join("probe.sh");
        std::fs::write(&probe, PROBE).unwrap();
        let (probe, probes_dir) = (probe.display().to_string(), probes.display().to_string());
        let hook = |event: &str| json!([{"hooks": [{"type": "command", "command": format!("/bin/sh {probe} {probes_dir} hook-{event}")}]}]);
        let settings = home.path().join("settings.json");
        std::fs::write(
            &settings,
            json!({"hooks": {
                "SessionStart": hook("SessionStart"),
                "UserPromptSubmit": hook("UserPromptSubmit"),
                "Stop": hook("Stop"),
            }})
            .to_string(),
        )
        .unwrap();
        let servers = McpServers::new(vec![McpServer {
            name: "probe".to_string(),
            transport: McpTransport::Stdio {
                command: "/bin/sh".to_string(),
                args: vec![probe, probes_dir, "mcp".to_string()],
                env: Default::default(),
                inherit_env: Vec::new(),
            },
            required_tools: Vec::new(),
        }])
        .unwrap();
        Self {
            probes,
            settings,
            servers,
        }
    }

    fn settings(&self) -> Option<String> {
        Some(self.settings.display().to_string())
    }

    /// The probes whose state file names start with `prefix`. Each probe
    /// writes its state file last, so one that exists is complete.
    fn states(&self, prefix: &str) -> Vec<PathBuf> {
        std::fs::read_dir(&self.probes)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with(prefix) && name.ends_with(".state")
            })
            .collect()
    }

    /// Every hook and the MCP server ran, and none of them inherited `secret`
    /// or a variable named `credential_env`, or read `secret` from
    /// descriptor 3. With `descriptor_closed`, none of them holds descriptor 3
    /// at all.
    fn assert_none_saw(&self, secret: &str, credential_env: &str, descriptor_closed: bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.states("hook-").is_empty() || self.states("mcp-").is_empty() {
            assert!(
                Instant::now() < deadline,
                "the hooks and the MCP server never ran"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        for state in self.states("hook-").into_iter().chain(self.states("mcp-")) {
            let who = state.file_stem().unwrap().to_string_lossy().into_owned();
            let env = std::fs::read_to_string(state.with_extension("env")).unwrap();
            assert!(
                !env.contains(secret),
                "{who}: the credential is in its environment"
            );
            assert!(
                !env.lines()
                    .any(|line| line.starts_with(&format!("{credential_env}="))),
                "{who}: its environment names {credential_env}"
            );
            let fd3 = std::fs::read(state.with_extension("fd3")).unwrap();
            assert!(
                !fd3.windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "{who}: read the credential from descriptor 3"
            );
            let descriptor = std::fs::read_to_string(&state).unwrap();
            eprintln!("{who}: descriptor 3 {}", descriptor.trim());
            if descriptor_closed {
                assert_eq!(descriptor.trim(), "closed", "{who}: holds descriptor 3");
            }
        }
    }
}

/// What the CLI starts (its hooks, and a stdio MCP server, which is how
/// `cairn-cmd` runs) never sees the token: not in its environment, and not on
/// descriptor 3, which it either does not hold or finds already drained.
#[test]
fn nothing_the_cli_starts_sees_the_token() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning what the CLI's children inherit against Claude CLI {version}");
    const TOKEN: &str = "at-DESCRIPTOR";
    let stub = Stub::start();
    let home = Home::new();
    let probes = Probes::in_home(&home);
    let spec = spec(
        &home,
        launch_credential(TOKEN, &home),
        Some(probes.servers.clone()),
        ClaudeMode::Interactive,
        ApprovalMode::Allow,
        None,
        Vec::new(),
    );
    let mut cli = launch_as_cairn(&claude, &stub, home, &spec, probes.settings(), &[]);

    cli.send("turn one");
    let result = cli.turn_result();
    assert_eq!(result["subtype"], "success", "{result}");
    assert_eq!(
        stub.inference_tokens().last().map(String::as_str),
        Some(TOKEN)
    );
    cli.assert_launch_environment_holds_none_of(&[TOKEN], &stub, OAUTH);
    probes.assert_none_saw(TOKEN, OAUTH_TOKEN_ENV, false);
}

/// A dummy key in the shape of a real Anthropic API key.
const API_KEY_VALUE: &str = "sk-ant-api03-DUMMY-contract-key-000000000000000000000000-0000AA";

/// The file under `dir` that holds `needle`, if any. Symlinks are not followed.
fn file_holding(dir: &Path, needle: &[u8]) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            if let Some(found) = file_holding(&path, needle) {
                return Some(found);
            }
        } else if kind.is_file()
            && std::fs::read(&path)
                .is_ok_and(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
        {
            return Some(path);
        }
    }
    None
}

/// Neither the key nor the suffix the CLI keeps of an approved key is in any
/// file under `dir`.
fn assert_no_file_holds_the_key(dir: &Path) {
    let suffix = &API_KEY_VALUE[API_KEY_VALUE.len() - 20..];
    for needle in [API_KEY_VALUE, suffix] {
        assert_eq!(
            file_holding(dir, needle.as_bytes()),
            None,
            "a file holds the API key or its suffix"
        );
    }
}

/// An API key reaches the CLI the way a managed token does: on an inherited
/// descriptor. The CLI authenticates with exactly that key, interactively and
/// in print mode, while the key is in neither its launch environment, nor
/// anything it starts, nor any file it writes.
#[test]
fn an_api_key_launch_authenticates_by_descriptor_and_nothing_sees_the_key() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning the API-key descriptor against Claude CLI {version}");

    // Interactive, two turns: the key is read once and serves both.
    let stub = Stub::start();
    let home = Home::new();
    let probes = Probes::in_home(&home);
    let interactive = spec(
        &home,
        api_key_launch(API_KEY_VALUE, &home.config),
        Some(probes.servers.clone()),
        ClaudeMode::Interactive,
        ApprovalMode::Allow,
        None,
        Vec::new(),
    );
    let mut cli = launch_as_cairn(&claude, &stub, home, &interactive, probes.settings(), &[]);
    for turn in ["turn one", "turn two"] {
        cli.send(turn);
        let result = cli.turn_result();
        assert_eq!(result["subtype"], "success", "{result}");
    }
    let keys = stub.inference_api_keys();
    assert!(keys.len() >= 2, "{} inference requests", keys.len());
    assert!(
        keys.iter().all(|key| key == API_KEY_VALUE),
        "an inference request carried a different API key"
    );
    assert!(
        stub.inference_tokens().iter().all(String::is_empty),
        "an inference request carried a bearer token"
    );
    cli.assert_launch_environment_holds_none_of(&[API_KEY_VALUE], &stub, API_KEY);
    probes.assert_none_saw(API_KEY_VALUE, API_KEY_ENV, true);
    cli.assert_no_token_was_fetched();
    assert_no_file_holds_the_key(cli.home.path());
    drop(cli);

    // Print mode, as a one-shot completion runs.
    let stub = Stub::start();
    let home = Home::new();
    let one_shot = spec(
        &home,
        api_key_launch(API_KEY_VALUE, &home.config),
        None,
        ClaudeMode::OneShot {
            prompt: "answer".to_string(),
        },
        ApprovalMode::Deny,
        None,
        Vec::new(),
    );
    let mut cli = launch_as_cairn(&claude, &stub, home, &one_shot, None, &[]);
    cli.feed_and_close("what is the answer?");
    let result = cli.turn_result();
    assert_eq!(result["subtype"], "success", "{result}");
    assert_eq!(
        stub.inference_api_keys().last().map(String::as_str),
        Some(API_KEY_VALUE)
    );
    cli.assert_launch_environment_holds_none_of(&[API_KEY_VALUE], &stub, API_KEY);
    cli.assert_no_token_was_fetched();
    assert_no_file_holds_the_key(cli.home.path());
}

/// A canary only the operator's own `CLAUDE.md` holds.
const OPERATOR_CANARY: &str = "CANARY-operator-claude-md-7f3a";

/// An operator's `~/.claude` under `home`: hooks and a `statusLine` that
/// touch markers in `<home>/markers`, a `CLAUDE.md` holding
/// [`OPERATOR_CANARY`], and a `~/.claude.json` declaring a user MCP server.
/// Answers the markers directory.
fn operator_claude_home(home: &Path) -> PathBuf {
    let markers = home.join("markers");
    std::fs::create_dir_all(&markers).unwrap();
    let touch = |name: &str| format!("touch {}", markers.join(name).display());
    let hook = |name: &str| json!([{"hooks": [{"type": "command", "command": touch(name)}]}]);
    let dot_claude = home.join(".claude");
    std::fs::create_dir_all(&dot_claude).unwrap();
    std::fs::write(
        dot_claude.join("settings.json"),
        json!({
            "hooks": {
                "SessionStart": hook("SessionStart"),
                "UserPromptSubmit": hook("UserPromptSubmit"),
            },
            "statusLine": {"type": "command", "command": touch("statusLine")},
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        dot_claude.join("CLAUDE.md"),
        format!("Always mention {OPERATOR_CANARY}.\n"),
    )
    .unwrap();
    std::fs::write(
        home.join(".claude.json"),
        json!({"mcpServers": {"operator-probe": {
            "type": "stdio",
            "command": "/bin/sh",
            "args": ["-c", touch("mcp")],
        }}})
        .to_string(),
    )
    .unwrap();
    markers
}

fn markers_in(markers: &Path) -> Vec<String> {
    std::fs::read_dir(markers)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

/// An agent launch runs in its own home, so nothing the operator configured in
/// `~/.claude` reaches it: no hook or `statusLine` runs, their `CLAUDE.md` is
/// not sent to the model, and their MCP server does not start. The control, a
/// launch that names `~/.claude` itself, shows the fixture is live.
#[test]
fn the_operators_claude_home_never_loads_into_a_launch() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning config-dir isolation against Claude CLI {version}");
    let launch = |home: Home| {
        let stub = Stub::start();
        let launched = spec(
            &home,
            api_key_launch(API_KEY_VALUE, &home.config),
            None,
            ClaudeMode::Interactive,
            ApprovalMode::Allow,
            None,
            Vec::new(),
        );
        let mut cli = launch_as_cairn(&claude, &stub, home, &launched, None, &[]);
        cli.send("turn one");
        let init = cli.init();
        let result = cli.turn_result();
        assert_eq!(result["subtype"], "success", "{result}");
        (cli, stub, init)
    };

    // Cairn's launch: its own home beside the operator's.
    let home = Home::new();
    let markers = operator_claude_home(home.path());
    let (cli, stub, init) = launch(home);
    // Hooks run as the turn starts; give a straggler a moment to land.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        markers_in(&markers),
        Vec::<String>::new(),
        "the operator's hooks, statusLine, or MCP server ran"
    );
    assert!(
        !stub.sent_to_the_model(OPERATOR_CANARY),
        "the operator's CLAUDE.md was sent to the model"
    );
    assert!(
        !init["mcp_servers"].to_string().contains("operator-probe"),
        "the operator's MCP server was configured: {}",
        init["mcp_servers"]
    );
    drop(cli);

    // The control: the same fixture, with the launch naming `~/.claude`, the
    // home the CLI would otherwise default to.
    let home = Home::with_config(".claude");
    let markers = operator_claude_home(home.path());
    let (_cli, stub, _) = launch(home);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !markers_in(&markers)
        .iter()
        .any(|marker| marker == "UserPromptSubmit")
    {
        assert!(
            Instant::now() < deadline,
            "the control's hooks never ran: {:?}",
            markers_in(&markers)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        stub.sent_to_the_model(OPERATOR_CANARY),
        "the control never sent the operator's CLAUDE.md"
    );
}

/// A Claude session spec as Cairn's backends build one, on `secrets` (a
/// managed account's [`launch_credential`] or an [`api_key_launch`]), for the
/// given servers, mode, approval, and output schema.
fn spec(
    home: &Home,
    secrets: SecretEnv,
    mcp: Option<McpServers>,
    mode: ClaudeMode,
    approval: ApprovalMode,
    output_schema: Option<Value>,
    allowed_tools: Vec<String>,
) -> SessionSpec {
    let dir = home.path().display().to_string();
    SessionSpec {
        start: SessionStart::New {
            session_id: uuid::Uuid::new_v4().to_string(),
        },
        model: None,
        effort: None,
        approval,
        output_schema,
        cwd: dir.clone(),
        env: std::collections::BTreeMap::new(),
        mcp,
        secrets,
        account_id: None,
        provider: ProviderSpec::Claude(ClaudeSpec {
            system_prompt: PlacedFile {
                dir,
                name: "system.md".to_string(),
                contents: String::new(),
            },
            hook_settings: None,
            allowed_tools,
            disallowed_tools: Vec::new(),
            mode,
        }),
    }
}

/// The tool names a `system`/`init` event lists.
fn init_tools(init: &Value) -> Vec<String> {
    let mut tools: Vec<String> = init["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("the init event lists no tools: {init}"))
        .iter()
        .map(|tool| tool.as_str().unwrap().to_string())
        .collect();
    tools.sort();
    tools
}

/// Keep the raw init event when `CAIRN_RECORD_CLAUDE_INIT` names a directory,
/// under a name carrying the CLI version that emitted it.
fn record_init(case: &str, init: &Value) {
    let Ok(dir) = std::env::var("CAIRN_RECORD_CLAUDE_INIT") else {
        return;
    };
    let version = init["claude_code_version"]
        .as_str()
        .unwrap_or_else(|| panic!("the init event names no CLI version: {init}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = std::path::Path::new(&dir).join(format!("claude_init_{case}-{version}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(init).unwrap() + "\n").unwrap();
    eprintln!("recorded the init event to {}", path.display());
}

/// An agent session as Cairn launches one is offered its MCP server's tools
/// and no built-in tool at all: not in the init event's tool list, and not in
/// what the model is sent.
#[test]
fn the_cli_offers_only_cairn_mcp_tools_under_tools_none() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning the agent tool surface against Claude CLI {version}");
    let stub = Stub::start();
    let mcp = McpStub::start();
    let servers = McpServers::new(vec![McpServer::cairn_http(&mcp.url)]).unwrap();
    let verbs: Vec<String> = ["read", "write", "run"]
        .map(|verb| format!("mcp__cairn__{verb}"))
        .to_vec();
    let home = Home::new();
    let spec = spec(
        &home,
        launch_credential("at-SEALED", &home),
        Some(servers),
        ClaudeMode::Interactive,
        ApprovalMode::Allow,
        None,
        verbs,
    );
    let mut cli = launch_as_cairn(
        &claude,
        &stub,
        home,
        &spec,
        None,
        &[(MCP_TOKEN_ENV, "mcp-SEALED")],
    );

    cli.send("turn one");
    let init = cli.init();
    let result = cli.turn_result();
    record_init("tools_none", &init);
    eprintln!("init tools: {:?}", init["tools"]);
    eprintln!("init mcp_servers: {}", init["mcp_servers"]);
    eprintln!("requests offered: {:?}", stub.offered());
    assert_eq!(result["subtype"], "success", "{result}");

    assert_eq!(
        init_tools(&init),
        ["mcp__cairn__read", "mcp__cairn__run", "mcp__cairn__write"],
        "{init}"
    );
    let built_in: Vec<String> = stub
        .offered()
        .into_iter()
        .flatten()
        .filter(|tool| !tool.starts_with("mcp__"))
        .collect();
    assert!(
        built_in.is_empty(),
        "the model was offered built-in tools: {built_in:?}"
    );
    assert!(
        mcp.calls()
            .iter()
            .any(|(method, token)| method == "tools/list" && token == "mcp-SEALED"),
        "the CLI never listed the server's tools with its bearer: {:?}",
        mcp.calls()
    );
}

/// The CLI asks for progress on every MCP tool call: it sends
/// `_meta.progressToken`, and a progress notification on that token resets its
/// idle bound (`CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT`). Cairn's MCP server
/// heartbeats a held call on that token, so a CLI that stopped sending one
/// would abandon long calls at its idle default.
#[test]
fn the_cli_sends_a_progress_token_on_tools_call() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning the MCP progress token against Claude CLI {version}");
    let stub = Stub::answering(Answer::McpRead);
    let mcp = McpStub::start();
    let servers = McpServers::new(vec![McpServer::cairn_http(&mcp.url)]).unwrap();
    let verbs: Vec<String> = ["read", "write", "run"]
        .map(|verb| format!("mcp__cairn__{verb}"))
        .to_vec();
    let home = Home::new();
    let spec = spec(
        &home,
        launch_credential("at-SEALED", &home),
        Some(servers),
        ClaudeMode::Interactive,
        ApprovalMode::Allow,
        None,
        verbs,
    );
    let mut cli = launch_as_cairn(
        &claude,
        &stub,
        home,
        &spec,
        None,
        &[(MCP_TOKEN_ENV, "mcp-SEALED")],
    );

    cli.send("read something");
    let result = cli.turn_result();
    assert_eq!(result["subtype"], "success", "{result}");

    let call = mcp
        .bodies()
        .into_iter()
        .find(|body| body["method"] == "tools/call")
        .unwrap_or_else(|| panic!("the CLI never called a tool: {:?}", mcp.calls()));
    eprintln!(
        "tools/call as Claude CLI {version} sends it:\n{}",
        serde_json::to_string_pretty(&call).unwrap()
    );
    assert_eq!(call["params"]["name"], "read", "{call}");
    let token = &call["params"]["_meta"]["progressToken"];
    assert!(
        token.is_string() || token.is_number(),
        "the CLI sent no progress token: {call}"
    );
}

/// A schema-constrained completion runs tool-free, on its token by
/// descriptor, and its answer still arrives through the CLI's own
/// StructuredOutput tool.
#[test]
fn a_schema_constrained_call_keeps_structured_output_under_tools_none() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning StructuredOutput under --tools \"\" against Claude CLI {version}");
    let stub = Stub::answering(Answer::StructuredOutput);
    let schema: Value = serde_json::from_str(STRUCTURED_SCHEMA).unwrap();
    let home = Home::new();
    let spec = spec(
        &home,
        launch_credential("at-ONE-SHOT", &home),
        None,
        ClaudeMode::OneShot {
            prompt: "answer".to_string(),
        },
        ApprovalMode::Deny,
        Some(schema),
        Vec::new(),
    );
    let mut cli = launch_as_cairn(&claude, &stub, home, &spec, None, &[]);

    cli.feed_and_close("what is the answer?");
    let init = cli.init();
    let result = cli.turn_result();
    record_init("json_schema", &init);
    eprintln!("init tools: {:?}", init["tools"]);
    eprintln!("requests offered: {:?}", stub.offered());
    eprintln!(
        "init lists StructuredOutput: {}",
        init_tools(&init)
            .iter()
            .any(|tool| tool == "StructuredOutput")
    );

    assert_eq!(result["subtype"], "success", "{result}");
    assert_eq!(
        result["structured_output"],
        serde_json::from_str::<Value>(STRUCTURED_INPUT).unwrap(),
        "{result}"
    );
    let built_in: Vec<String> = stub
        .offered()
        .into_iter()
        .flatten()
        .filter(|tool| tool != "StructuredOutput")
        .collect();
    assert!(
        built_in.is_empty(),
        "the model was offered tools besides StructuredOutput: {built_in:?}"
    );
    assert_eq!(
        stub.inference_tokens().last().map(String::as_str),
        Some("at-ONE-SHOT")
    );
    cli.assert_launch_environment_holds_none_of(&["at-ONE-SHOT"], &stub, OAUTH);
    cli.assert_no_token_was_fetched();
}
