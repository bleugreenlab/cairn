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
//! MCP server and the `--json-schema` StructuredOutput answer both survive.
//! Those launches use the argument vector of Cairn's own
//! [`ClaudeCli::spawn_config`], so the test pins Cairn's arguments rather
//! than a copy of them.
//!
//! It runs the real `claude` from PATH, in a fresh home, against a loopback
//! Messages API stub (and, for the tool surface, a loopback MCP server), with
//! every HTTPS request sent to a proxy that refuses it. Only dummy tokens are
//! used. Where no `claude` is installed it skips; where one is, a CLI upgrade
//! that drops the contract fails here.
//!
//! With `CAIRN_RECORD_CLAUDE_INIT=<dir>` set, the tool-surface tests write the
//! CLI's raw `system`/`init` event to `<dir>/claude_init_<case>-<version>.json`,
//! the version being the CLI's own `claude_code_version`. The committed copies
//! under `tests/fixtures/` come from such a run on the CLI production agents
//! run, and are re-recorded when that CLI moves.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_models::interface::session::{
    ApprovalMode, ClaudeMode, ClaudeSpec, PlacedFile, ProviderSpec, SecretEnv, SessionControl,
    SessionSpec, SessionStart,
};
use cairn_models::session::claude::{credential_push_generation, ClaudeCli, ClaudeControl};
use cairn_models::session::mcp::{McpServer, McpServers, MCP_TOKEN_ENV};
use cairn_models::wire::claude_stream::send_user_message;
use cairn_models::wire::MessageContent;
use serde_json::{json, Value};

const WAIT: Duration = Duration::from_secs(90);

/// A Messages API that answers every request whose bearer token it has not
/// been told to refuse, and records which token each request carried and
/// which tools it offered the model.
struct Stub {
    base_url: String,
    seen: Arc<Mutex<Vec<(String, String)>>>,
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
}

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
        let refused: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let offered: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let (record, refuse, offer) = (seen.clone(), refused.clone(), offered.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (record, refuse, offer) = (record.clone(), refuse.clone(), offer.clone());
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
                    let body = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
                    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
                    let tools = offered_tools(&body);
                    record.lock().unwrap().push((path.clone(), token.clone()));
                    if path.starts_with("/v1/messages") && !path.contains("count_tokens") {
                        offer.lock().unwrap().push(tools.clone());
                    }
                    let structured = matches!(answer, Answer::StructuredOutput)
                        && tools.iter().any(|tool| tool == "StructuredOutput")
                        && !has_tool_result(&body);
                    let (status, content_type, payload) = if refuse.lock().unwrap().contains(&token)
                    {
                        ("401 Unauthorized", "application/json", UNAUTHORIZED)
                    } else if path.starts_with("/v1/messages") && streaming && structured {
                        ("200 OK", "text/event-stream", SSE_STRUCTURED_OUTPUT)
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
            refused,
            offered,
        }
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
/// machine.
fn refusing_proxy() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            drop(stream);
        }
    });
    url
}

/// A streamable-HTTP MCP server offering Cairn's three verbs, answering
/// every request with plain JSON (no SSE stream).
struct McpStub {
    url: String,
    /// The JSON-RPC methods called, in order, and the bearer each carried.
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

impl McpStub {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let calls: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let record = calls.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let record = record.clone();
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
        Self { url, calls }
    }

    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
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

struct SharedStdin(Arc<Mutex<Option<ChildStdin>>>);

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

struct Cli {
    child: Child,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    events: Receiver<Value>,
    /// Every event read so far, for a failure to show.
    history: Mutex<Vec<Value>>,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    stderr: std::path::PathBuf,
    control: ClaudeControl,
    _home: tempfile::TempDir,
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
        let env = SecretEnv::new().with("CLAUDE_CODE_OAUTH_TOKEN", token);
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
                    "the CLI never produced {what}.\nevents:\n{}\nrequests: {:?}\nstderr:\n{}",
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
                    std::fs::read_to_string(&self.stderr).unwrap_or_default()
                ),
            }
        }
    }

    fn turn_result(&self) -> Value {
        self.until("a result", |event| event["type"] == "result")
    }
}

impl Drop for Cli {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

fn launch(claude: &str, stub: &Stub, token: &str) -> Cli {
    let home = tempfile::tempdir().unwrap();
    let mut command = sealed(claude, stub, home.path(), token);
    command
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
        ])
        .env("CLAUDE_CODE_OAUTH_401_WAIT_MS", "60000");
    spawn(command, home, stub)
}

/// Launch the CLI with exactly the arguments and environment overlay Cairn's
/// own `ClaudeCli::spawn_config` gives `spec`, inside the sealed environment.
fn launch_as_cairn(claude: &str, stub: &Stub, spec: &SessionSpec, env: &[(&str, &str)]) -> Cli {
    let home = tempfile::tempdir().unwrap();
    let config = ClaudeCli::spawn_config(claude, spec, None, None).unwrap();
    let mut command = sealed(claude, stub, home.path(), "at-SEALED");
    command
        .args(&config.args)
        .envs(&config.env)
        .envs(env.iter().copied());
    spawn(command, home, stub)
}

/// The real CLI in a fresh home, able to reach only the loopback stubs.
fn sealed(claude: &str, stub: &Stub, home: &std::path::Path, token: &str) -> Command {
    let config = home.join("claude-home");
    std::fs::create_dir_all(&config).unwrap();
    let mut command = Command::new(claude);
    command
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("CLAUDE_CONFIG_DIR", &config)
        .env("CLAUDE_CODE_OAUTH_TOKEN", token)
        .env("ANTHROPIC_BASE_URL", &stub.base_url)
        // The CLI sends everything through its proxy, plain HTTP included;
        // only the loopback stubs are exempt.
        .env("HTTPS_PROXY", refusing_proxy())
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("DISABLE_AUTOUPDATER", "1")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        .current_dir(home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(std::fs::File::create(home.join("stderr.log")).unwrap());
    command
}

fn spawn(mut command: Command, home: tempfile::TempDir, stub: &Stub) -> Cli {
    let stderr = home.path().join("stderr.log");
    let mut child = command.spawn().expect("spawn claude");
    let stdin = Arc::new(Mutex::new(child.stdin.take()));
    let stdout = child.stdout.take().unwrap();
    let (tx, events) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(event) = serde_json::from_str::<Value>(&line) {
                if tx.send(event).is_err() {
                    return;
                }
            }
        }
    });
    let control = ClaudeControl::new(Box::new(SharedStdin(stdin.clone())));
    Cli {
        child,
        stdin,
        events,
        history: Mutex::new(Vec::new()),
        seen: stub.seen.clone(),
        stderr,
        control,
        _home: home,
    }
}

fn acknowledged(event: &Value, generation: u64) -> bool {
    event["type"] == "control_response"
        && [&event["response"]["request_id"], &event["request_id"]]
            .iter()
            .filter_map(|id| id.as_str())
            .any(|id| credential_push_generation(id) == Some(generation))
}

#[test]
fn a_pushed_token_is_used_between_turns_and_rescues_a_turn_waiting_on_a_401() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning the credential-push contract against Claude CLI {version}");
    let stub = Stub::start();
    let mut cli = launch(&claude, &stub, "at-OLD");

    cli.send("turn one");
    let first = cli.turn_result();
    assert_eq!(first["subtype"], "success", "{first}");
    assert_eq!(
        stub.inference_tokens().last().map(String::as_str),
        Some("at-OLD")
    );

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
}

/// A Claude session spec as Cairn's backends build one, for the given
/// servers, mode, approval, and output schema.
fn spec(
    home: &str,
    mcp: Option<McpServers>,
    mode: ClaudeMode,
    approval: ApprovalMode,
    output_schema: Option<Value>,
    allowed_tools: Vec<String>,
) -> SessionSpec {
    SessionSpec {
        start: SessionStart::New {
            session_id: uuid::Uuid::new_v4().to_string(),
        },
        model: None,
        effort: None,
        approval,
        output_schema,
        cwd: home.to_string(),
        env: std::collections::BTreeMap::new(),
        mcp,
        secrets: SecretEnv::new(),
        account_id: None,
        provider: ProviderSpec::Claude(ClaudeSpec {
            system_prompt: PlacedFile {
                dir: home.to_string(),
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
    let spec = spec(
        "/tmp",
        Some(servers),
        ClaudeMode::Interactive,
        ApprovalMode::Allow,
        None,
        verbs,
    );
    let mut cli = launch_as_cairn(&claude, &stub, &spec, &[(MCP_TOKEN_ENV, "mcp-SEALED")]);

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

/// A schema-constrained completion runs tool-free, and its answer still
/// arrives through the CLI's own StructuredOutput tool.
#[test]
fn a_schema_constrained_call_keeps_structured_output_under_tools_none() {
    let Some((claude, version)) = installed_claude() else {
        eprintln!("skipping: no `claude` on PATH");
        return;
    };
    eprintln!("pinning StructuredOutput under --tools \"\" against Claude CLI {version}");
    let stub = Stub::answering(Answer::StructuredOutput);
    let schema: Value = serde_json::from_str(STRUCTURED_SCHEMA).unwrap();
    let spec = spec(
        "/tmp",
        None,
        ClaudeMode::OneShot {
            prompt: "answer".to_string(),
        },
        ApprovalMode::Deny,
        Some(schema),
        Vec::new(),
    );
    let mut cli = launch_as_cairn(&claude, &stub, &spec, &[]);

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
}
