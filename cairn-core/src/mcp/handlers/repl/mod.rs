//! Stateful REPL host.
//!
//! A REPL is a persistent interpreter subprocess (an "eval-server") that reads
//! JSON-lines requests on stdin and writes JSON-lines responses on stdout,
//! holding a live namespace across `run` calls. The live child handles live in
//! an in-memory, node-scoped registry on the orchestrator ([`ReplState`],
//! mirroring `pty_state`) with **no database table**: a REPL is explicitly
//! non-durable, so a DB row would advertise a live session whose process no
//! longer exists. The registry is the single source of truth; create, read,
//! delete, and teardown all consult it.
//!
//! Lifetime = job/worktree lifetime: the always-on orchestrator owns the child,
//! so a REPL survives intra-execution turn suspends (the whole point — state
//! persisting across `run` calls that span turns) and is killed at node/worktree
//! teardown (the hard guarantee against orphans). On host restart the registry
//! is empty, so a prior slug resolves as *unknown* (recreate it), never "died".

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use serde::Deserialize;

use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::services::ChildProcess;

/// The embedded python eval-server, materialized to the job scratch dir at REPL
/// creation and run as a script argument (stdin is reserved for the request
/// protocol, so the server cannot also arrive on stdin).
pub(crate) const PYTHON_EVAL_SERVER: &str = include_str!("eval_server.py");

/// The embedded typescript eval-server, run by `bun` with the same agent
/// PATH/env/sandbox as an inline typescript `run` item.
pub(crate) const TYPESCRIPT_EVAL_SERVER: &str = include_str!("eval_server.ts");

/// The interpreter backing a REPL session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplLang {
    Python,
    Typescript,
}

impl ReplLang {
    /// Parse the create-payload `interpreter` string. Mirrors the inline-code
    /// interpreter aliases: `python`/`py` → Python, and `typescript`/`ts`/
    /// `javascript`/`js` → Typescript (bun runs both identically, and a session
    /// created as `typescript` must accept a send tagged `javascript`). Returns
    /// `None` for an unknown interpreter so the caller can name the accepted set.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "python" | "py" => Some(Self::Python),
            "typescript" | "ts" | "javascript" | "js" => Some(Self::Typescript),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Python => "python",
            Self::Typescript => "typescript",
        }
    }
}

/// One request's response from the eval-server (the JSON-lines protocol).
#[derive(Debug, Deserialize)]
pub(crate) struct ReplResponse {
    /// `"success"` or `"error"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// `repr` of the final expression's value, omitted when the last statement is
    /// not an expression (or evaluates to `None`).
    #[serde(default)]
    pub value: Option<String>,
    /// Message + traceback, present only on `type:error`.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    /// Advisory guidance from the eval-server (typescript only; python never sets
    /// it, so serde default keeps it `None`). Currently: a top-level-await send
    /// was auto-wrapped, so its `const`/`let` declarations did not persist.
    #[serde(default)]
    pub note: Option<String>,
}

impl ReplResponse {
    pub fn succeeded(&self) -> bool {
        self.kind == "success"
    }
}

/// Outcome of one [`send`] round-trip.
pub(crate) enum ReplSendResult {
    /// The eval-server returned a framed response line.
    Response(ReplResponse),
    /// No response within the item timeout; the caller kills the REPL.
    Timeout,
    /// The child had already exited (or its pipes closed) — state is lost.
    Dead,
    /// A framed line that did not parse as the protocol.
    Protocol(String),
}

/// A live eval-server session. Held behind an `Arc` in [`ReplState`].
pub struct ReplSession {
    pub interpreter: ReplLang,
    /// The child handle, kept for liveness checks and kill.
    child: Mutex<Box<dyn ChildProcess>>,
    /// Persistent stdin — NOT closed after a write (that would EOF the server).
    stdin: Mutex<Box<dyn Write + Send>>,
    /// Framed response lines, fed by the dedicated stdout reader thread.
    responses: Mutex<mpsc::Receiver<String>>,
    pub created_at: SystemTime,
    /// Serializes request->response round-trips so two `run` items targeting the
    /// same slug (items run in parallel by default) cannot interleave on the
    /// single-threaded eval-server. Different slugs stay concurrent.
    send_lock: tokio::sync::Mutex<()>,
}

impl ReplSession {
    /// SIGKILL the eval-server process group. Best-effort.
    pub fn kill(&self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
        }
    }

    /// True while the child is still running (non-blocking).
    pub fn is_alive(&self) -> bool {
        match self.child.lock() {
            Ok(mut child) => matches!(child.try_wait(), Ok(None)),
            Err(_) => false,
        }
    }
}

/// In-memory, node-scoped registry of live REPL sessions, keyed by
/// `(job_id, slug)`. One instance lives on the orchestrator, shared by every
/// host exactly like `pty_state`.
#[derive(Default)]
pub struct ReplState {
    sessions: Mutex<HashMap<(String, String), Arc<ReplSession>>>,
}

impl ReplState {
    pub fn get(&self, job_id: &str, slug: &str) -> Option<Arc<ReplSession>> {
        self.sessions
            .lock()
            .ok()?
            .get(&(job_id.to_string(), slug.to_string()))
            .cloned()
    }

    pub fn contains(&self, job_id: &str, slug: &str) -> bool {
        self.sessions
            .lock()
            .map(|s| s.contains_key(&(job_id.to_string(), slug.to_string())))
            .unwrap_or(false)
    }

    pub fn insert(&self, job_id: String, slug: String, session: Arc<ReplSession>) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert((job_id, slug), session);
        }
    }

    pub fn remove(&self, job_id: &str, slug: &str) -> Option<Arc<ReplSession>> {
        self.sessions
            .lock()
            .ok()?
            .remove(&(job_id.to_string(), slug.to_string()))
    }

    /// Drain and return every session belonging to one of `job_ids` (teardown).
    pub fn remove_for_jobs(&self, job_ids: &[String]) -> Vec<Arc<ReplSession>> {
        let Ok(mut sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        let set: HashSet<&str> = job_ids.iter().map(String::as_str).collect();
        let keys: Vec<(String, String)> = sessions
            .keys()
            .filter(|(job_id, _)| set.contains(job_id.as_str()))
            .cloned()
            .collect();
        keys.iter().filter_map(|k| sessions.remove(k)).collect()
    }
}

/// Spawn an eval-server child and register a live [`ReplSession`].
///
/// The child gets the identical agent env/sandbox as an inline `run` item
/// (via [`build_agent_spawn_config`](crate::mcp::handlers::run::build_agent_spawn_config)):
/// same worktree cwd, `@cairn/sdk`, uv cache, callback wiring, and OS
/// confinement. Unlike an inline item it captures stdin (the request protocol)
/// and never closes it.
pub async fn spawn_session(
    orch: &Orchestrator,
    run_context: &RunContext,
    cwd: &str,
    interpreter: ReplLang,
    slug: &str,
    deps: &[String],
) -> Result<Arc<ReplSession>, String> {
    // Materialize the embedded eval-server into the per-job scratch dir (the
    // child's TMPDIR: fence-safe, outside the worktree so it can't trip the
    // worktree-restore machinery, and reclaimed at teardown).
    let scratch = crate::scratch::ensure_job_scratch_dir(&run_context.job_id, None);
    let (script_name, body) = match interpreter {
        ReplLang::Python => (format!("repl-{slug}.py"), PYTHON_EVAL_SERVER),
        ReplLang::Typescript => (format!("repl-{slug}.ts"), TYPESCRIPT_EVAL_SERVER),
    };
    let script_path = scratch.join(&script_name);
    std::fs::write(&script_path, body)
        .map_err(|e| format!("failed to materialize REPL eval-server: {e}"))?;
    let script = script_path.to_string_lossy().to_string();

    let (program, args) = match interpreter {
        ReplLang::Python => {
            if crate::env::find_binary_on_agent_path("uv").is_ok() {
                // uv reads deps from the create payload (uv run --with dep) — no
                // PEP 723 parsing needed. cwd=worktree so uv's project detection
                // and the worktree's node_modules/@cairn/sdk resolve.
                let mut args = vec!["run".to_string()];
                for dep in deps {
                    args.push("--with".to_string());
                    args.push(dep.clone());
                }
                args.push(script);
                ("uv".to_string(), args)
            } else {
                if !deps.is_empty() {
                    return Err(
                        "REPL `deps` require uv on the agent PATH, which was not found".to_string(),
                    );
                }
                ("python3".to_string(), vec![script])
            }
        }
        ReplLang::Typescript => {
            // `deps` is a uv-only affordance; typescript packages resolve from
            // the worktree node_modules, so a non-empty list is a mistake to name.
            if !deps.is_empty() {
                return Err(
                    "REPL `deps` are python-only (preloaded via uv); a typescript REPL resolves \
                     packages from the worktree node_modules, so omit `deps`."
                        .to_string(),
                );
            }
            // Same agent-PATH resolution as an inline typescript `run` item.
            if crate::env::find_binary_on_agent_path("bun").is_err() {
                return Err(
                    "A typescript REPL requires `bun` on the agent PATH, which was not found."
                        .to_string(),
                );
            }
            ("bun".to_string(), vec![script])
        }
    };

    // Same OS confinement as a run command in this worktree (captured at spawn;
    // a denial in later user code surfaces as EPERM rather than a fence prompt).
    let sandbox_policy = crate::mcp::handlers::run::build_run_sandbox_policy(
        orch,
        cwd,
        Some(run_context.run_id.as_str()),
        Some(run_context.project_id.as_str()),
        None,
        false,
    )
    .await
    .map(|(policy, _fence)| policy);

    let mut config = crate::mcp::handlers::run::build_agent_spawn_config(
        orch,
        cwd,
        Some(run_context),
        &program,
        &args,
        sandbox_policy,
    )
    .await
    .stdin(true);
    // Do NOT pipe the eval-server's stderr. The protocol lives on stdout and the
    // host never drains a stderr pipe, so a captured stderr could fill (uv's
    // dependency-resolution output before the script runs, or a native fd-2
    // write) and hang the child until the send-timeout kills its state. Leaving
    // it inherited routes such output to the host's own stderr, where it can
    // never block. User-code stderr is captured inside the eval-server (fd 2 is
    // redirected during evaluation) and returned in the response instead.
    config.capture_stderr = false;

    let mut child = orch
        .services
        .process
        .spawn(config)
        .map_err(|e| format!("failed to spawn REPL eval-server: {e}"))?;
    let stdout = child
        .take_stdout()
        .ok_or_else(|| "REPL eval-server produced no stdout".to_string())?;
    let stdin = child
        .take_stdin()
        .ok_or_else(|| "REPL eval-server produced no stdin".to_string())?;

    // A persistent child never EOFs, so a dedicated reader thread (not the
    // one-shot reader loop `execute_process` uses) forwards each framed line
    // over the channel until the pipe finally closes at process exit.
    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    Ok(Arc::new(ReplSession {
        interpreter,
        child: Mutex::new(child),
        stdin: Mutex::new(stdin),
        responses: Mutex::new(rx),
        created_at: SystemTime::now(),
        send_lock: tokio::sync::Mutex::new(()),
    }))
}

/// Send one code request to the REPL and await its single framed response.
///
/// Holds `send_lock` across the write+read so same-slug sends serialize on the
/// single-threaded eval-server. On timeout the caller kills and unregisters the
/// session (state lost).
pub(crate) async fn send(
    session: &Arc<ReplSession>,
    code: &str,
    timeout: Duration,
) -> ReplSendResult {
    let _guard = session.send_lock.lock().await;

    // A crashed/exited child is unrecoverable state loss.
    if !session.is_alive() {
        return ReplSendResult::Dead;
    }

    let request = serde_json::json!({ "code": code }).to_string();
    {
        let Ok(mut stdin) = session.stdin.lock() else {
            return ReplSendResult::Dead;
        };
        if stdin.write_all(request.as_bytes()).is_err()
            || stdin.write_all(b"\n").is_err()
            || stdin.flush().is_err()
        {
            return ReplSendResult::Dead;
        }
    }

    // Block for one response off the tokio runtime; send_lock guarantees we are
    // the only reader of the response channel.
    let sess = session.clone();
    let recv = tokio::task::spawn_blocking(move || {
        let rx = sess.responses.lock().map_err(|_| ())?;
        rx.recv_timeout(timeout).map_err(|_| ())
    })
    .await;

    match recv {
        Ok(Ok(line)) => match serde_json::from_str::<ReplResponse>(&line) {
            Ok(response) => ReplSendResult::Response(response),
            Err(e) => {
                ReplSendResult::Protocol(format!("unparseable eval-server response: {e}: {line}"))
            }
        },
        // recv_timeout Timeout, Disconnected, or a poisoned lock / join failure:
        // liveness is re-checked by the caller, which distinguishes timeout
        // (kill) from an already-dead child.
        _ => {
            if session.is_alive() {
                ReplSendResult::Timeout
            } else {
                ReplSendResult::Dead
            }
        }
    }
}

/// Render a REPL read: a one-line status banner from the live registry.
pub(crate) fn render_status(slug: &str, session: Option<&Arc<ReplSession>>) -> String {
    match session {
        None => format!(
            "[repl {slug}: not found] No REPL named '{slug}' for this node. \
             Create it: write cairn:~/repl/{slug} {{interpreter:\"python\"|\"typescript\"}}"
        ),
        Some(session) => {
            let alive = session.is_alive();
            let uptime = session
                .created_at
                .elapsed()
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let state = if alive { "running" } else { "exited" };
            format!(
                "[repl {slug}: {}, {state}, up {uptime}s]",
                session.interpreter.label()
            )
        }
    }
}

/// Resolve the top-level node job id for a `NodeRepl` URI's coordinates,
/// mirroring the terminal target lookup. Used by read and delete to reach the
/// registry key from URI coordinates.
pub(crate) async fn resolve_node_repl_job_id(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> Option<String> {
    use crate::storage::RowExt;
    let project_key = project_key.to_uppercase();
    let node_id = node_id.to_string();
    db.read(|conn| {
        let project_key = project_key.clone();
        let node_id = node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT j.id
                    FROM jobs j
                    JOIN issues i ON j.issue_id = i.id
                    JOIN projects p ON i.project_id = p.id
                    JOIN executions e ON j.execution_id = e.id
                    WHERE p.key = ?1
                      AND i.number = ?2
                      AND e.seq = ?3
                      AND j.parent_job_id IS NULL
                      AND j.uri_segment = ?4
                    LIMIT 1
                    ",
                    (project_key.as_str(), number, exec_seq, node_id.as_str()),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod lang_tests {
    use super::ReplLang;

    #[test]
    fn parse_aliases_map_to_the_two_languages() {
        for raw in ["python", "py", "PYTHON", " Py "] {
            assert_eq!(ReplLang::parse(raw), Some(ReplLang::Python), "{raw}");
        }
        // typescript, javascript, and their short aliases all resolve to one
        // Typescript session kind (bun runs them identically).
        for raw in ["typescript", "ts", "javascript", "js", "TypeScript", " JS "] {
            assert_eq!(ReplLang::parse(raw), Some(ReplLang::Typescript), "{raw}");
        }
        assert_eq!(ReplLang::parse("ruby"), None);
        assert_eq!(ReplLang::parse(""), None);
    }

    #[test]
    fn label_round_trips_the_canonical_name() {
        assert_eq!(ReplLang::Python.label(), "python");
        assert_eq!(ReplLang::Typescript.label(), "typescript");
    }
}

#[cfg(test)]
mod eval_server_tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};

    /// A live python eval-server subprocess for direct JSON-in/JSON-out tests.
    struct EvalServer {
        child: Child,
        stdin: std::process::ChildStdin,
        stdout: BufReader<std::process::ChildStdout>,
        _dir: tempfile::TempDir,
    }

    impl EvalServer {
        /// Spawn `<program> <materialized script>`, or `None` if the interpreter
        /// is not available in the test environment (so the caller can skip).
        fn start_with(program: &str, filename: &str, body: &str) -> Option<Self> {
            let dir = tempfile::tempdir().ok()?;
            let script = dir.path().join(filename);
            std::fs::write(&script, body).ok()?;
            let mut child = Command::new(program)
                .arg(&script)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;
            let stdin = child.stdin.take()?;
            let stdout = BufReader::new(child.stdout.take()?);
            Some(Self {
                child,
                stdin,
                stdout,
                _dir: dir,
            })
        }

        /// Spawn `python3 <materialized eval_server.py>`, or `None` if python3 is
        /// not available in the test environment.
        fn start() -> Option<Self> {
            Self::start_with("python3", "eval_server.py", PYTHON_EVAL_SERVER)
        }

        /// Spawn `bun <materialized eval_server.ts>`, or `None` if bun is not
        /// available in the test environment.
        fn start_ts() -> Option<Self> {
            Self::start_with("bun", "eval_server.ts", TYPESCRIPT_EVAL_SERVER)
        }

        fn eval(&mut self, code: &str) -> ReplResponse {
            self.send_raw(&serde_json::json!({ "code": code }).to_string())
        }

        /// Write one raw framed line and read one framed response — used to drive
        /// a deliberately malformed request past the `eval` JSON wrapper.
        fn send_raw(&mut self, raw: &str) -> ReplResponse {
            self.stdin.write_all(raw.as_bytes()).unwrap();
            self.stdin.write_all(b"\n").unwrap();
            self.stdin.flush().unwrap();
            let mut line = String::new();
            self.stdout.read_line(&mut line).unwrap();
            serde_json::from_str(&line)
                .unwrap_or_else(|e| panic!("unparseable response {line:?}: {e}"))
        }
    }

    impl Drop for EvalServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[test]
    fn state_persists_across_requests() {
        let Some(mut server) = EvalServer::start() else {
            eprintln!("skipping: python3 not available");
            return;
        };
        assert!(server.eval("x = 41").succeeded());
        let r = server.eval("x + 1");
        assert!(r.succeeded());
        assert_eq!(r.value.as_deref(), Some("42"));
    }

    #[test]
    fn defs_persist_and_are_callable_later() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        assert!(server.eval("def double(n):\n    return n * 2").succeeded());
        let r = server.eval("double(21)");
        assert_eq!(r.value.as_deref(), Some("42"));
    }

    #[test]
    fn trailing_expression_yields_value_statement_does_not() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let expr = server.eval("1 + 2");
        assert_eq!(expr.value.as_deref(), Some("3"));
        let stmt = server.eval("y = 5");
        assert_eq!(stmt.value, None);
    }

    #[test]
    fn print_captured_without_breaking_framing() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("print('hello')\nprint('world')");
        assert!(r.succeeded());
        assert_eq!(r.stdout, "hello\nworld\n");
        // A later request still frames correctly (stdout was restored).
        assert_eq!(server.eval("7 * 6").value.as_deref(), Some("42"));
    }

    #[test]
    fn exception_returns_error_with_traceback() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("1 / 0");
        assert!(!r.succeeded());
        assert_eq!(r.kind, "error");
        let err = r.error.unwrap_or_default();
        assert!(err.contains("ZeroDivisionError"), "got: {err}");
    }

    #[test]
    fn stderr_is_captured() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("import sys\nsys.stderr.write('oops')");
        assert!(r.succeeded());
        assert_eq!(r.stderr, "oops");
    }

    // A raw fd-1 write bypasses `sys.stdout` entirely. It must be captured (not
    // written to the protocol stream), and the NEXT response must still be the
    // correct one — i.e. the stream is not desynchronized.
    #[test]
    fn direct_fd_stdout_write_does_not_corrupt_framing() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("import os\nos.write(1, b'raw\\n')");
        assert!(r.succeeded(), "got: {r:?}");
        assert_eq!(r.stdout, "raw\n");
        // Framing intact: the following send gets its own response, not the
        // previous one.
        assert_eq!(server.eval("40 + 2").value.as_deref(), Some("42"));
    }

    // A raw fd-2 write is captured as stderr, not leaked or lost.
    #[test]
    fn direct_fd_stderr_write_is_captured() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let r = server.eval("import os\nos.write(2, b'boom')");
        assert!(r.succeeded(), "got: {r:?}");
        assert_eq!(r.stderr, "boom");
        assert_eq!(server.eval("1 + 1").value.as_deref(), Some("2"));
    }

    // A subprocess inheriting the eval-server's fds writes to the captures, not
    // the protocol stream. Covers `subprocess.run([...])` without capture, the
    // most common way exploratory REPL code emits raw fd output.
    #[test]
    fn subprocess_output_is_captured_without_breaking_framing() {
        let Some(mut server) = EvalServer::start() else {
            return;
        };
        let out = server.eval("import subprocess\nsubprocess.run(['printf', 'rawsub'])");
        assert!(out.succeeded(), "got: {out:?}");
        assert!(out.stdout.contains("rawsub"), "stdout: {:?}", out.stdout);
        let err =
            server.eval("import subprocess\nsubprocess.run(['sh', '-c', 'printf oops 1>&2'])");
        assert!(err.succeeded(), "got: {err:?}");
        assert!(err.stderr.contains("oops"), "stderr: {:?}", err.stderr);
        // Two subprocess sends later, framing is still aligned.
        assert_eq!(server.eval("7 * 6").value.as_deref(), Some("42"));
    }

    // --- typescript / bun eval-server (parity + deltas). Each skips when `bun`
    // is absent, mirroring the python3 guard. ---

    #[test]
    fn ts_state_and_defs_persist_across_requests() {
        let Some(mut server) = EvalServer::start_ts() else {
            eprintln!("skipping: bun not available");
            return;
        };
        // bare, const, function, and class declarations all carry over.
        assert!(server.eval("x = 40").succeeded());
        assert!(server.eval("const y = 2").succeeded());
        assert_eq!(server.eval("x + y").value.as_deref(), Some("42"));
        assert!(server.eval("function dbl(n){ return n * 2 }").succeeded());
        assert_eq!(server.eval("dbl(21)").value.as_deref(), Some("42"));
        assert!(server.eval("class C { get v(){ return 7 } }").succeeded());
        assert_eq!(server.eval("new C().v").value.as_deref(), Some("7"));
    }

    #[test]
    fn ts_trailing_expression_yields_value_statement_does_not() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        // Object/array/null literals survive (deadCodeElimination disabled).
        assert_eq!(server.eval("1 + 2").value.as_deref(), Some("3"));
        assert!(server.eval("({a:1})").value.is_some());
        assert_eq!(server.eval("null").value.as_deref(), Some("null"));
        assert_eq!(server.eval("let z = 5").value, None);
    }

    #[test]
    fn ts_console_and_subprocess_captured_without_breaking_framing() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("console.log('hello'); console.log('world')");
        assert!(r.succeeded(), "got: {r:?}");
        assert_eq!(r.stdout, "hello\nworld\n");
        assert_eq!(server.eval("console.error('oops')").stderr, "oops\n");
        // A subprocess inheriting the eval-server's fds lands in the capture, not
        // the protocol stream.
        let sub = server.eval("Bun.spawnSync(['printf', 'rawsub'], { stdout: 'inherit' })");
        assert!(sub.succeeded(), "got: {sub:?}");
        assert!(sub.stdout.contains("rawsub"), "stdout: {:?}", sub.stdout);
        // Framing intact several sends later.
        assert_eq!(server.eval("6 * 7").value.as_deref(), Some("42"));
    }

    #[test]
    fn ts_error_returns_stack() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("throw new Error('boom')");
        assert!(!r.succeeded());
        assert_eq!(r.kind, "error");
        assert!(r.error.unwrap_or_default().contains("boom"));
    }

    #[test]
    fn ts_typescript_annotations_are_stripped() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        assert_eq!(
            server.eval("const n: number = 9; n").value.as_deref(),
            Some("9")
        );
    }

    #[test]
    fn ts_top_level_await_bare_assignment_persists_and_notes_declarations() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        // Bare assignment in an awaiting (auto-wrapped) send persists.
        assert!(server.eval("g = await Promise.resolve(100)").succeeded());
        assert_eq!(server.eval("g").value.as_deref(), Some("100"));
        // A `const`/`let`/etc. in an awaiting send does NOT persist, and the
        // response carries a `note` telling the agent to use bare assignment.
        let decl = server.eval("const h = await Promise.resolve(1)");
        assert!(decl.succeeded(), "got: {decl:?}");
        assert!(decl.note.is_some(), "expected a note, got: {decl:?}");
    }

    #[test]
    fn ts_top_level_return_yields_value() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        // Bare top-level return (no await) is retried through the async wrap.
        assert_eq!(server.eval("return 5 + 5").value.as_deref(), Some("10"));
        // Top-level await plus return also yields the returned value.
        let r = server.eval("const os = await import('node:os'); return typeof os.tmpdir");
        assert_eq!(r.value.as_deref(), Some("\"function\""));
    }

    #[test]
    fn ts_static_import_returns_clear_error() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.eval("import x from 'node:os'");
        assert!(!r.succeeded());
        let err = r.error.unwrap_or_default();
        assert!(err.contains("require"), "got: {err}");
        // The server survives the rejected import and keeps framing.
        assert_eq!(server.eval("1 + 1").value.as_deref(), Some("2"));
    }

    #[test]
    fn ts_user_thrown_syntaxerror_does_not_double_execute() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        assert!(server.eval("count = 0").succeeded());
        // A user-thrown SyntaxError must NOT be mistaken for a top-level-return
        // parse error and retried (which would run the side effect twice).
        let r = server.eval("count++; throw new SyntaxError('nope')");
        assert!(!r.succeeded());
        assert_eq!(server.eval("count").value.as_deref(), Some("1"));
    }

    #[test]
    fn ts_malformed_request_is_framed_error() {
        let Some(mut server) = EvalServer::start_ts() else {
            return;
        };
        let r = server.send_raw("not json at all");
        assert!(!r.succeeded());
        assert!(r.error.unwrap_or_default().contains("malformed"));
        // Still alive and framing correctly.
        assert_eq!(server.eval("2 + 2").value.as_deref(), Some("4"));
    }
}
