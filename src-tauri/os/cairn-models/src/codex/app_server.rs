use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use serde_json::{json, Value};

use cairn_platform::services::process::{ChildProcess, ProcessSpawner, SpawnConfig};

use crate::codex::stack_sample::{PendingSample, PlatformSampler, StackSampler, SAMPLE_AFTER};
use crate::interface::session::ServerReply;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type PendingMap = HashMap<u64, Sender<Result<Value, String>>>;

/// When a pending request's server is sampled, and when the request is
/// given up on.
#[derive(Debug, Clone, Copy)]
struct RequestTimeouts {
    sample_after: Duration,
    give_up: Duration,
}

const REQUEST_TIMEOUTS: RequestTimeouts = RequestTimeouts {
    sample_after: SAMPLE_AFTER,
    give_up: REQUEST_TIMEOUT,
};

pub struct AppServerClient {
    child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
    writer: Arc<Mutex<Option<Box<dyn Write + Send>>>>,
    next_id: Arc<Mutex<u64>>,
    pending: Arc<Mutex<PendingMap>>,
    notification_rx: Receiver<Value>,
    /// The MCP servers this process was started with, by name, so a thread
    /// opened tool-free can switch each of them off for itself.
    mcp_servers: Vec<String>,
    timeouts: RequestTimeouts,
    /// Samples the process's stacks when a request hangs
    /// (see [`crate::codex::stack_sample`]).
    sampler: Arc<dyn StackSampler>,
    /// `$CODEX_HOME/log`, where a sample's full report is written.
    log_dir: Option<PathBuf>,
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
        let log_dir = config
            .env
            .get("CODEX_HOME")
            .map(|home| PathBuf::from(home).join("log"));
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

        Ok(Self::new(Some(child), stdin, pending, notif_rx, log_dir))
    }

    fn new(
        child: Option<Box<dyn ChildProcess>>,
        writer: Box<dyn Write + Send>,
        pending: Arc<Mutex<PendingMap>>,
        notification_rx: Receiver<Value>,
        log_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            child: Arc::new(Mutex::new(child)),
            writer: Arc::new(Mutex::new(Some(writer))),
            next_id: Arc::new(Mutex::new(1)),
            pending,
            notification_rx,
            mcp_servers: Vec::new(),
            timeouts: REQUEST_TIMEOUTS,
            sampler: Arc::new(PlatformSampler),
            log_dir,
        }
    }

    /// Sample with `sampler` after `sample_after`, give up after `give_up`,
    /// and write full samples into `log_dir`.
    #[cfg(test)]
    fn with_stack_sampling(
        mut self,
        sampler: Arc<dyn StackSampler>,
        sample_after: Duration,
        give_up: Duration,
        log_dir: Option<PathBuf>,
    ) -> Self {
        self.sampler = sampler;
        self.timeouts = RequestTimeouts {
            sample_after,
            give_up,
        };
        self.log_dir = log_dir;
        self
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
            move |request| {
                responses.pop_front().unwrap_or_else(|| {
                    Err(format!(
                        "scripted app-server response missing for {}",
                        request["method"]
                    ))
                })
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

        let client = Self::new(None, Box::new(writer), pending, notif_rx, None);
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

        self.await_response(method, &rx)?.and_then(|msg| {
            if let Some(error) = msg.get("error") {
                Err(error.to_string())
            } else {
                Ok(msg["result"].clone())
            }
        })
    }

    /// Wait for `method`'s response. Past `sample_after` the server's stacks
    /// are sampled on another thread, and the error a request that then times
    /// out returns carries what the sample has found by then.
    fn await_response(
        &self,
        method: &str,
        rx: &Receiver<Result<Value, String>>,
    ) -> Result<Result<Value, String>, String> {
        let timed_out = || format!("codex app-server request timed out: {method}");
        let RequestTimeouts {
            sample_after,
            give_up,
        } = self.timeouts;
        match rx.recv_timeout(sample_after) {
            Ok(response) => return Ok(response),
            Err(RecvTimeoutError::Disconnected) => return Err(timed_out()),
            Err(RecvTimeoutError::Timeout) => {}
        }
        let pid = self
            .child
            .lock()
            .ok()
            .and_then(|child| child.as_ref().map(|child| child.id()));
        let sample = PendingSample::start(
            self.sampler.clone(),
            pid,
            self.log_dir.as_deref(),
            method,
            sample_after,
        );
        rx.recv_timeout(give_up.saturating_sub(sample_after))
            .map_err(|_| format!("{}\n{}", timed_out(), sample.report()))
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
        let client = AppServerClient::new(
            None,
            Box::new(Written(written.clone())),
            Arc::new(Mutex::new(HashMap::new())),
            unbounded().1,
            None,
        );

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

    /// A sampler that answers `outcome` after `delay`, recording each pid.
    struct FakeSampler {
        pids: Mutex<Vec<u32>>,
        delay: Duration,
        outcome: Result<String, String>,
    }

    impl FakeSampler {
        fn new(delay: Duration, outcome: Result<String, String>) -> Arc<Self> {
            Arc::new(Self {
                pids: Mutex::new(Vec::new()),
                delay,
                outcome,
            })
        }
    }

    impl StackSampler for FakeSampler {
        fn sample(&self, pid: u32, _limit: Duration) -> Result<String, String> {
            self.pids.lock().unwrap().push(pid);
            thread::sleep(self.delay);
            self.outcome.clone()
        }
    }

    const SAMPLE_AFTER: Duration = Duration::from_millis(50);
    const GIVE_UP: Duration = Duration::from_millis(600);

    /// A server (pid 42) that reads every request and answers none.
    fn silent_server(sampler: Arc<FakeSampler>, log_dir: Option<PathBuf>) -> AppServerClient {
        AppServerClient::launch(
            |_| Ok(Box::new(MockChildProcess::with_stdout(42, vec![]))),
            SpawnConfig::new("codex"),
        )
        .unwrap()
        .with_stack_sampling(sampler, SAMPLE_AFTER, GIVE_UP, log_dir)
    }

    fn timed_request(client: &AppServerClient) -> (String, Duration) {
        let started = std::time::Instant::now();
        let error = client
            .send_request("thread/start", json!({}))
            .expect_err("nothing answers");
        (error, started.elapsed())
    }

    #[test]
    fn a_request_pending_past_the_threshold_is_sampled_once_and_its_timeout_carries_the_excerpt() {
        let logs = tempfile::tempdir().unwrap();
        let report = format!(
            "Call graph:\n  parked in thread_start\n{}",
            "frame\n".repeat(2000)
        );
        let sampler = FakeSampler::new(Duration::ZERO, Ok(report.clone()));
        let client = silent_server(sampler.clone(), Some(logs.path().join("log")));

        let (error, elapsed) = timed_request(&client);

        assert_eq!(*sampler.pids.lock().unwrap(), [42]);
        assert!(
            error.starts_with("codex app-server request timed out: thread/start\nstack sample of pid 42 (full output: "),
            "{error}"
        );
        assert!(error.contains("parked in thread_start"), "{error}");
        assert!(
            error.len() < crate::codex::stack_sample::EXCERPT_BYTES + 512,
            "the excerpt is capped: {} bytes",
            error.len()
        );
        assert!(elapsed >= GIVE_UP, "{elapsed:?}");
        let saved: Vec<_> = std::fs::read_dir(logs.path().join("log"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(saved.len(), 1);
        assert!(saved[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("app-server-stack-42-"));
        assert_eq!(std::fs::read_to_string(&saved[0]).unwrap(), report);
    }

    #[test]
    fn a_sampler_that_fails_or_hangs_leaves_the_timeout_as_it_was() {
        let failing = FakeSampler::new(Duration::ZERO, Err("sample is not installed".to_string()));
        let (error, elapsed) = timed_request(&silent_server(failing.clone(), None));
        assert_eq!(
            error,
            "codex app-server request timed out: thread/start\nno stack sample was taken of pid 42: sample is not installed"
        );
        assert!(elapsed >= GIVE_UP, "{elapsed:?}");

        // Far longer than the request waits: a timeout that waited on the
        // sample would take all of it.
        let hanging = FakeSampler::new(Duration::from_secs(10), Ok("late".to_string()));
        let logs = tempfile::tempdir().unwrap();
        let (error, elapsed) =
            timed_request(&silent_server(hanging.clone(), Some(logs.path().into())));
        assert!(
            elapsed >= GIVE_UP && elapsed < Duration::from_secs(3),
            "the timeout does not wait on the sample: {elapsed:?}"
        );
        assert!(
            error.starts_with(
                "codex app-server request timed out: thread/start\nthe stack sample of pid 42 was still running; it will be written to "
            ),
            "{error}"
        );
        assert_eq!(*hanging.pids.lock().unwrap(), [42]);
    }

    #[test]
    fn a_request_answered_before_the_threshold_is_not_sampled() {
        let sampler = FakeSampler::new(Duration::ZERO, Ok("unused".to_string()));
        let client = AppServerClient::for_test_responding(
            |_| Ok(json!({"result": {"ok": true}})),
            Arc::new(Mutex::new(Vec::new())),
        )
        .with_stack_sampling(sampler.clone(), SAMPLE_AFTER, GIVE_UP, None);

        let result = client.send_request("thread/start", json!({})).unwrap();

        assert_eq!(result, json!({"ok": true}));
        assert!(sampler.pids.lock().unwrap().is_empty());
    }

    #[test]
    fn a_server_launched_in_a_codex_home_writes_its_samples_under_its_log_directory() {
        let client = AppServerClient::launch(
            |_| Ok(Box::new(MockChildProcess::with_stdout(42, vec![]))),
            SpawnConfig::new("codex").env("CODEX_HOME", "/homes/codex"),
        )
        .unwrap();
        assert_eq!(client.log_dir, Some(PathBuf::from("/homes/codex/log")));
    }
}
