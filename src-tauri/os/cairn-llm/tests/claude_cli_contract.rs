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
//! It runs the real `claude` from PATH, in a fresh home, against a loopback
//! Messages API stub, with every HTTPS request sent to a proxy that refuses
//! it. Only dummy tokens are used. Where no `claude` is installed it skips;
//! where one is, a CLI upgrade that drops the contract fails here.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_llm::interface::session::{SecretEnv, SessionControl};
use cairn_llm::session::claude::{credential_push_generation, ClaudeControl};
use cairn_llm::wire::claude_stream::send_user_message;
use cairn_llm::wire::MessageContent;
use serde_json::Value;

const WAIT: Duration = Duration::from_secs(90);

/// A Messages API that answers every request whose bearer token it has not
/// been told to refuse, and records which token each request carried.
struct Stub {
    base_url: String,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    refused: Arc<Mutex<Vec<String>>>,
}

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

impl Stub {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let refused: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (record, refuse) = (seen.clone(), refused.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (record, refuse) = (record.clone(), refuse.clone());
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
                    let streaming = serde_json::from_slice::<Value>(&body)
                        .ok()
                        .and_then(|body| body.get("stream").and_then(Value::as_bool))
                        .unwrap_or(false);
                    record.lock().unwrap().push((path.clone(), token.clone()));
                    let (status, content_type, payload) = if refuse.lock().unwrap().contains(&token)
                    {
                        ("401 Unauthorized", "application/json", UNAUTHORIZED)
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
        }
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

struct SharedStdin(Arc<Mutex<std::process::ChildStdin>>);

impl Write for SharedStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

struct Cli {
    child: Child,
    stdin: Arc<Mutex<std::process::ChildStdin>>,
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
    let config = home.path().join("claude-home");
    std::fs::create_dir_all(&config).unwrap();
    let stderr = home.path().join("stderr.log");
    let mut child = Command::new(claude)
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
        ])
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("CLAUDE_CONFIG_DIR", &config)
        .env("CLAUDE_CODE_OAUTH_TOKEN", token)
        .env("CLAUDE_CODE_OAUTH_401_WAIT_MS", "60000")
        .env("ANTHROPIC_BASE_URL", &stub.base_url)
        // The CLI sends everything through its proxy, plain HTTP included;
        // only the loopback stub is exempt.
        .env("HTTPS_PROXY", refusing_proxy())
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("DISABLE_AUTOUPDATER", "1")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        .current_dir(home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .expect("spawn claude");
    let stdin = Arc::new(Mutex::new(child.stdin.take().unwrap()));
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
