use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use serde_json::{json, Value};

use cairn_platform::services::process::{ChildProcess, ProcessSpawner, SpawnConfig};

use crate::interface::session::ServerReply;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type PendingMap = HashMap<u64, Sender<Result<Value, String>>>;

pub struct AppServerClient {
    child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
    writer: Arc<Mutex<Option<Box<dyn Write + Send>>>>,
    next_id: Arc<Mutex<u64>>,
    pending: Arc<Mutex<PendingMap>>,
    notification_rx: Receiver<Value>,
    /// The MCP servers this process was started with, by name, so a thread
    /// opened tool-free can switch each of them off for itself.
    mcp_servers: Vec<String>,
}

impl AppServerClient {
    /// Spawn `codex app-server`. Each of `config_overrides` is passed as one
    /// `-c <key=value>`; this is how a process receives its MCP servers.
    pub fn spawn(
        process: &dyn ProcessSpawner,
        codex_path: &str,
        config_overrides: &[String],
        env: &HashMap<String, String>,
        cwd: &str,
    ) -> Result<Self, String> {
        let mut spawn_config = Self::spawn_config(codex_path, config_overrides, cwd);
        for (k, v) in env {
            spawn_config = spawn_config.env(k, v);
        }
        Self::launch(|config| process.spawn(config), spawn_config)
    }

    /// `codex app-server` in `cwd`, each of `config_overrides` passed as one
    /// `-c <key=value>`. The caller adds the environment.
    pub fn spawn_config(codex_path: &str, config_overrides: &[String], cwd: &str) -> SpawnConfig {
        let mut spawn_config = SpawnConfig::new(codex_path)
            .arg("app-server")
            .cwd(cwd)
            .stdin(true);
        for value in config_overrides {
            spawn_config = spawn_config.arg("-c").arg(value);
        }
        spawn_config
    }

    /// Start the app-server `config` describes with `spawn`, and connect to it.
    pub fn launch(
        spawn: impl FnOnce(SpawnConfig) -> Result<Box<dyn ChildProcess>, String>,
        config: SpawnConfig,
    ) -> Result<Self, String> {
        let mut child = spawn(config)?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| "Failed to capture Codex stdout".to_string())?;
        let stderr = child.take_stderr();
        let stdin = child
            .take_stdin()
            .ok_or_else(|| "Failed to capture Codex stdin".to_string())?;

        if let Some(stderr) = stderr {
            thread::spawn(move || {
                for line in stderr.lines().map_while(Result::ok) {
                    log::debug!("codex app-server stderr: {}", line);
                }
            });
        }

        let (notif_tx, notif_rx) = unbounded();
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();

        thread::spawn(move || {
            for line in stdout.lines() {
                match line {
                    Ok(line) if !line.trim().is_empty() => {
                        match serde_json::from_str::<Value>(&line) {
                            Ok(msg) => {
                                if let Some(id) = msg.get("id").and_then(|v| v.as_u64()) {
                                    let has_method = msg.get("method").is_some();
                                    let is_response =
                                        msg.get("result").is_some() || msg.get("error").is_some();
                                    if has_method && !is_response {
                                        let _ = notif_tx.send(msg);
                                    } else if let Some(tx) = pending_clone
                                        .lock()
                                        .ok()
                                        .and_then(|mut map| map.remove(&id))
                                    {
                                        let outcome = if msg.get("error").is_some() {
                                            Err(msg["error"].clone().to_string())
                                        } else {
                                            Ok(msg)
                                        };
                                        let _ = tx.send(outcome);
                                    } else {
                                        log::warn!(
                                            "codex app-server: response {} had no pending waiter",
                                            id
                                        );
                                    }
                                } else {
                                    let _ = notif_tx.send(msg);
                                }
                            }
                            Err(e) => {
                                log::warn!("codex app-server: invalid JSON ({}): {}", e, line);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("codex app-server read error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            child: Arc::new(Mutex::new(Some(child))),
            writer: Arc::new(Mutex::new(Some(stdin))),
            next_id: Arc::new(Mutex::new(1)),
            pending,
            notification_rx: notif_rx,
            mcp_servers: Vec::new(),
        })
    }

    /// Record the names of the MCP servers this process was started with.
    pub fn with_mcp_servers(mut self, names: Vec<String>) -> Self {
        self.mcp_servers = names;
        self
    }

    /// The MCP servers this process was started with, by name.
    pub fn mcp_servers(&self) -> &[String] {
        &self.mcp_servers
    }

    pub fn notifications(&self) -> Receiver<Value> {
        self.notification_rx.clone()
    }

    pub fn child_handle(&self) -> Arc<Mutex<Option<Box<dyn ChildProcess>>>> {
        self.child.clone()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_test_scripted(
        responses: Vec<Result<Value, String>>,
        requests: Arc<Mutex<Vec<Value>>>,
    ) -> Self {
        let mut responses = std::collections::VecDeque::from(responses);
        Self::for_test_responding(
            move |_| {
                responses
                    .pop_front()
                    .unwrap_or_else(|| Err("scripted app-server response missing".to_string()))
            },
            requests,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_test_responding(
        respond: impl FnMut(&Value) -> Result<Value, String> + Send + 'static,
        requests: Arc<Mutex<Vec<Value>>>,
    ) -> Self {
        Self::for_test_with_notifications(respond, requests).0
    }

    /// A scripted client whose notification stream the test writes to. The
    /// stream stays open while the returned sender lives.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_test_with_notifications(
        respond: impl FnMut(&Value) -> Result<Value, String> + Send + 'static,
        requests: Arc<Mutex<Vec<Value>>>,
    ) -> (Self, Sender<Value>) {
        let (notif_tx, notif_rx) = unbounded();
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));
        let writer = ScriptedAppServerWriter {
            pending: pending.clone(),
            respond: Box::new(respond),
            requests,
            buffer: Vec::new(),
        };

        let client = Self {
            child: Arc::new(Mutex::new(None)),
            writer: Arc::new(Mutex::new(Some(Box::new(writer)))),
            next_id: Arc::new(Mutex::new(1)),
            pending,
            notification_rx: notif_rx,
            mcp_servers: Vec::new(),
        };
        (client, notif_tx)
    }

    pub fn send_request(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = {
            let mut guard = self
                .next_id
                .lock()
                .map_err(|e| format!("Poisoned lock: {}", e))?;
            let current = *guard;
            *guard += 1;
            current
        };

        let (tx, rx) = bounded(1);
        self.pending
            .lock()
            .map_err(|e| format!("Poisoned lock: {}", e))?
            .insert(id, tx);

        self.send_message(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))?;

        rx.recv_timeout(REQUEST_TIMEOUT)
            .map_err(|_| format!("codex app-server request timed out: {}", method))?
            .and_then(|msg| {
                if let Some(error) = msg.get("error") {
                    Err(error.to_string())
                } else {
                    Ok(msg["result"].clone())
                }
            })
    }

    pub fn send_notification(&self, method: &str, params: Value) -> Result<(), String> {
        self.send_message(json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
    }

    pub fn respond(&self, id: &Value, result: Value) -> Result<(), String> {
        self.send_message(json!({
            "jsonrpc": "2.0",
            "id": id.clone(),
            "result": result,
        }))
    }

    pub fn respond_error(&self, id: &Value, code: i32, message: &str) -> Result<(), String> {
        self.send_message(json!({
            "jsonrpc": "2.0",
            "id": id.clone(),
            "error": {
                "code": code,
                "message": message,
            }
        }))
    }

    /// Answer the server request `id` with `reply`.
    pub fn reply(&self, id: &Value, reply: ServerReply) -> Result<(), String> {
        match reply {
            ServerReply::Result(result) => self.respond(id, result),
            ServerReply::Error { code, message } => self.respond_error(id, code, &message),
        }
    }

    /// Force the app-server transport closed.
    ///
    /// Normal user Stop uses `turn/interrupt`, which hard-aborts the active turn
    /// while preserving the app-server for a warm follow-up. Reserve shutdown for
    /// hard teardown paths such as crashes, watchdog expiry, and session eviction.
    pub fn shutdown(&self) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writer.take();
        }
        if let Ok(mut child_guard) = self.child.lock() {
            if let Some(child) = child_guard.as_mut() {
                let _ = child.kill();
            }
        }
    }

    fn send_message(&self, msg: Value) -> Result<(), String> {
        let mut guard = self
            .writer
            .lock()
            .map_err(|e| format!("Poisoned lock: {}", e))?;
        let writer = guard
            .as_mut()
            .ok_or_else(|| "Codex stdin unavailable".to_string())?;
        let line = serde_json::to_string(&msg).map_err(|e| e.to_string())?;
        writeln!(writer, "{}", line).map_err(|e| e.to_string())
    }
}

#[cfg(any(test, feature = "test-utils"))]
type ScriptedResponse = Box<dyn FnMut(&Value) -> Result<Value, String> + Send>;

#[cfg(any(test, feature = "test-utils"))]
struct ScriptedAppServerWriter {
    pending: Arc<Mutex<PendingMap>>,
    respond: ScriptedResponse,
    requests: Arc<Mutex<Vec<Value>>>,
    buffer: Vec<u8>,
}

#[cfg(any(test, feature = "test-utils"))]
impl ScriptedAppServerWriter {
    fn process_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        let request: Value = serde_json::from_slice(line).map_err(std::io::Error::other)?;
        self.requests.lock().unwrap().push(request.clone());
        let id = request
            .get("id")
            .and_then(Value::as_u64)
            .ok_or_else(|| std::io::Error::other("scripted request missing id"))?;
        let tx = self
            .pending
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or_else(|| std::io::Error::other("scripted request had no pending waiter"))?;
        let response = (self.respond)(&request);
        let _ = tx.send(response);
        Ok(())
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Write for ScriptedAppServerWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.buffer.drain(..=newline).collect();
            if line.ends_with(b"\n") {
                line.pop();
            }
            if line.ends_with(b"\r") {
                line.pop();
            }
            if !line.is_empty() {
                self.process_line(&line)?;
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_platform::services::process::{MockChildProcess, MockProcessSpawner};

    #[test]
    fn spawn_uses_requested_working_directory() {
        let mut process = MockProcessSpawner::new();
        process.expect_spawn().return_once(|config| {
            assert_eq!(config.program, "codex");
            assert_eq!(
                config.args,
                vec!["app-server", "-c", "mcp_servers.cairn={url=\"u\"}"]
            );
            assert_eq!(config.cwd.as_deref(), Some("/tmp/worktree"));
            Ok(Box::new(MockChildProcess::with_stdout(42, vec![])))
        });

        let client = AppServerClient::spawn(
            &process,
            "codex",
            &["mcp_servers.cairn={url=\"u\"}".to_string()],
            &HashMap::new(),
            "/tmp/worktree",
        )
        .expect("app-server should spawn");
        drop(client);
    }

    /// Every line written to the app-server's stdin.
    struct Written(Arc<Mutex<Vec<u8>>>);

    impl Write for Written {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_reply_is_sent_as_a_json_rpc_result_or_error() {
        let written = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient {
            child: Arc::new(Mutex::new(None)),
            writer: Arc::new(Mutex::new(Some(Box::new(Written(written.clone()))))),
            next_id: Arc::new(Mutex::new(1)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            notification_rx: unbounded().1,
            mcp_servers: Vec::new(),
        };

        client
            .reply(&json!(42), ServerReply::Result(json!({"ok": true})))
            .unwrap();
        client
            .reply(
                &json!("req-7"),
                ServerReply::Error {
                    code: -32000,
                    message: "refused".to_string(),
                },
            )
            .unwrap();

        let written = String::from_utf8(written.lock().unwrap().clone()).unwrap();
        let lines: Vec<Value> = written
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines,
            vec![
                json!({"jsonrpc": "2.0", "id": 42, "result": {"ok": true}}),
                json!({"jsonrpc": "2.0", "id": "req-7",
                    "error": {"code": -32000, "message": "refused"}}),
            ]
        );
    }
}
