use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use serde_json::{json, Value};

use crate::services::{ChildProcess, ProcessSpawner, SpawnConfig};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type PendingMap = HashMap<u64, Sender<Result<Value, String>>>;

pub struct AppServerClient {
    child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
    writer: Arc<Mutex<Option<Box<dyn Write + Send>>>>,
    next_id: Arc<Mutex<u64>>,
    pending: Arc<Mutex<PendingMap>>,
    notification_rx: Receiver<Value>,
}

impl AppServerClient {
    pub fn spawn(
        process: &dyn ProcessSpawner,
        codex_path: &str,
        env: &HashMap<String, String>,
        cwd: &str,
    ) -> Result<Self, String> {
        let mut spawn_config = SpawnConfig::new(codex_path)
            .arg("app-server")
            .cwd(cwd)
            .stdin(true);

        for (k, v) in env {
            spawn_config = spawn_config.env(k, v);
        }

        let mut child = process.spawn(spawn_config)?;
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
        })
    }

    pub fn notifications(&self) -> Receiver<Value> {
        self.notification_rx.clone()
    }

    pub fn child_handle(&self) -> Arc<Mutex<Option<Box<dyn ChildProcess>>>> {
        self.child.clone()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockChildProcess, MockProcessSpawner};

    #[test]
    fn spawn_uses_requested_working_directory() {
        let mut process = MockProcessSpawner::new();
        process.expect_spawn().return_once(|config| {
            assert_eq!(config.program, "codex");
            assert_eq!(config.args, vec!["app-server"]);
            assert_eq!(config.cwd.as_deref(), Some("/tmp/worktree"));
            Ok(Box::new(MockChildProcess::with_stdout(42, vec![])))
        });

        let client = AppServerClient::spawn(&process, "codex", &HashMap::new(), "/tmp/worktree")
            .expect("app-server should spawn");
        drop(client);
    }
}
