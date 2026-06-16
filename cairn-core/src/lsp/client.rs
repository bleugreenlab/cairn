//! JSON-RPC transport for a single language-server instance.
//!
//! ## Threading model (forced by the spawner abstraction)
//!
//! [`crate::services::ProcessSpawner`] exposes only blocking `std::io` child
//! stdio, so the transport is built from blocking threads rather than async
//! tasks (which would need `tokio::process::Child` handles the spawner does not
//! provide and the mock cannot fake):
//!
//! - one **reader thread** owns `stdout`, parses `Content-Length`-framed
//!   messages, routes responses to a pending-request map by id, and handles
//!   notifications (`$/progress` end → readiness; `publishDiagnostics` → a
//!   bounded buffer). Server→client requests get an auto-`null` reply so a
//!   server that issues e.g. `window/workDoneProgress/create` never blocks.
//! - one **stderr thread** drains `stderr` into a bounded log buffer (an
//!   undrained full stderr pipe deadlocks a chatty server like rust-analyzer).
//! - the **writer** side holds `stdin` behind a `Mutex`, shared with the reader
//!   thread so it can answer server requests.
//!
//! When the reader thread hits EOF (the server died) it drains the pending map;
//! dropping each response sender disconnects the waiting receiver, so blocked
//! requests fail fast with [`LspError::Transport`] instead of waiting out their
//! timeout.
//!
//! The handshake is **tolerant**: if `initialize` fails (a server that does not
//! speak LSP, or one that died), the client is still constructed with empty
//! capabilities and `handshake_ok = false`. Ops then degrade honestly (an
//! unadvertised capability yields an "unsupported" result) rather than the pool
//! failing to cache the instance.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, RecvTimeoutError, Sender};
use serde_json::{json, Value};

use super::LspError;
use crate::services::ChildProcess;

/// Handshake timeout. Generous: a real server responds to `initialize`
/// promptly, and a dead/empty pipe unblocks immediately via reader EOF, so this
/// ceiling is only reached by a hung-but-alive server.
const INIT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default per-request timeout for localized lookups.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
/// Timeout for project-wide requests (references, hierarchy fan-out) after the
/// index is (best-effort) ready.
pub const PROJECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Graceful-shutdown request timeout before the kill backstop.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Continuous quiescence required before readiness latches — long enough to
/// clear the brief sub-second gaps between a server's progress sequences so the
/// latch marks true initial-index completion, not an early flap. Mirrors
/// `queries::MISS_SETTLE`.
const READY_SETTLE: Duration = Duration::from_secs(2);

const STDERR_CAP: usize = 256;
const DIAG_CAP: usize = 128;

/// Tracks server work-done progress to derive an indexing-readiness signal.
///
/// Servers like rust-analyzer emit several `$/progress` sequences
/// (`Fetching`, `Building CrateGraph`, `Roots Scanned`, `Indexing`, ...), each
/// a `begin`/`report*`/`end`, sometimes with gaps between sequences. A single
/// `end` does NOT mean indexing is done. Readiness is therefore **quiescence**:
/// at least one `begin` has been seen and no progress token is currently active.
/// Because gaps between sequences make quiescence flap, readiness **latches**:
/// once quiescence has held continuously for [`READY_SETTLE`] (initial indexing
/// truly settled), [`ProgressState::latched`] is set and stays set. This is
/// load-bearing for long-lived servers like rust-analyzer, whose flycheck
/// (`cargo check`) keeps emitting `$/progress` forever after indexing finishes —
/// without the latch, `active` is essentially never empty again, so every
/// project-wide op (references, hierarchy) would re-pay the full readiness
/// timeout on a fully-indexed server. Latching on sustained quiescence (rather
/// than on the first symbol answer) is deliberate: a server can answer
/// `workspace/symbol` before its project-wide reference index is complete, so
/// only true settle guarantees references/callers/subtypes are correct.
#[derive(Default)]
struct ProgressState {
    active: HashSet<String>,
    seen_begin: bool,
    /// When the server last became quiescent (active empty after a begin), or
    /// `None` while work is in flight. Drives the sustained-quiescence latch.
    idle_since: Option<Instant>,
    latched: bool,
}

impl ProgressState {
    /// Instantaneous readiness for status surfaces: latched, or quiescent right
    /// now (work started, none active).
    fn is_ready(&self) -> bool {
        self.latched || (self.seen_begin && self.active.is_empty())
    }

    /// Mutating check that latches once quiescence has held for [`READY_SETTLE`].
    fn poll_latch(&mut self) -> bool {
        if self.latched {
            return true;
        }
        if self.seen_begin && self.active.is_empty() {
            if let Some(since) = self.idle_since {
                if since.elapsed() >= READY_SETTLE {
                    self.latched = true;
                    return true;
                }
            }
        }
        false
    }
}

/// A running language-server connection.
pub struct LspClient {
    language: String,
    root: PathBuf,
    child: Arc<Mutex<Box<dyn ChildProcess>>>,
    stdin: Arc<Mutex<Box<dyn Write + Send>>>,
    next_id: AtomicI64,
    pending: Arc<Mutex<HashMap<i64, Sender<Value>>>>,
    /// Set true when the reader thread exits (server died / stream closed).
    /// A request that registers after this is rejected immediately instead of
    /// waiting out its timeout for a reply that can never come.
    closed: Arc<AtomicBool>,
    /// Raw `capabilities` object from the `initialize` result. Gated on for
    /// honest capability degradation.
    capabilities: Value,
    ready: Arc<(Mutex<ProgressState>, Condvar)>,
    opened: Mutex<HashSet<String>>,
    diagnostics: Arc<Mutex<VecDeque<Value>>>,
    stderr_log: Arc<Mutex<VecDeque<String>>>,
    reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    handshake_ok: bool,
}

impl LspClient {
    /// Spawn the reader/stderr threads, run the `initialize`/`initialized`
    /// handshake, and return a connected client. Tolerant of handshake failure
    /// (see module docs).
    pub fn start(
        mut child: Box<dyn ChildProcess>,
        language: &str,
        root: &Path,
        init_options: Option<Value>,
    ) -> Result<LspClient, LspError> {
        let stdout = child
            .take_stdout()
            .ok_or_else(|| LspError::Transport("language server has no stdout".to_string()))?;
        let stdin = child
            .take_stdin()
            .ok_or_else(|| LspError::Transport("language server has no stdin".to_string()))?;
        let stderr = child.take_stderr();

        let pending: Arc<Mutex<HashMap<i64, Sender<Value>>>> = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let ready = Arc::new((Mutex::new(ProgressState::default()), Condvar::new()));
        let diagnostics = Arc::new(Mutex::new(VecDeque::new()));
        let stdin = Arc::new(Mutex::new(stdin));
        let stderr_log = Arc::new(Mutex::new(VecDeque::new()));

        let reader = {
            let pending = pending.clone();
            let ready = ready.clone();
            let diagnostics = diagnostics.clone();
            let stdin = stdin.clone();
            let closed = closed.clone();
            std::thread::Builder::new()
                .name("lsp-reader".to_string())
                .spawn(move || reader_loop(stdout, &pending, &stdin, &ready, &diagnostics, &closed))
                .ok()
        };

        let stderr_reader = stderr.and_then(|err| {
            let stderr_log = stderr_log.clone();
            std::thread::Builder::new()
                .name("lsp-stderr".to_string())
                .spawn(move || stderr_loop(err, &stderr_log))
                .ok()
        });

        let child = Arc::new(Mutex::new(child));

        let mut client = LspClient {
            language: language.to_string(),
            root: root.to_path_buf(),
            child,
            stdin,
            next_id: AtomicI64::new(1),
            pending,
            closed,
            capabilities: Value::Null,
            ready,
            opened: Mutex::new(HashSet::new()),
            diagnostics,
            stderr_log,
            reader,
            stderr_reader,
            handshake_ok: false,
        };

        match client.initialize(init_options) {
            Ok(caps) => {
                client.capabilities = caps;
                client.handshake_ok = true;
            }
            Err(e) => {
                log::warn!("lsp initialize failed for {language}: {e}");
            }
        }

        Ok(client)
    }

    pub fn language(&self) -> &str {
        &self.language
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn handshake_ok(&self) -> bool {
        self.handshake_ok
    }

    /// Whether the underlying process is still running.
    pub fn is_alive(&self) -> bool {
        match self.child.lock() {
            Ok(mut c) => c.try_wait().map(|s| s.is_none()).unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Whether the server advertises support for `op` (truthy capability key).
    pub fn supports(&self, op: super::LspOp) -> bool {
        capability_present(&self.capabilities, op.capability_key())
    }

    /// Whether the server advertises `prepareRename` (its `renameProvider`
    /// capability is an object with `prepareProvider: true`). When true, the
    /// rename op first probes the position so a non-renameable element fails
    /// with the server's reason instead of a confusing partial edit.
    pub fn rename_prepare_supported(&self) -> bool {
        self.capabilities
            .get("renameProvider")
            .and_then(|provider| provider.get("prepareProvider"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Non-blocking peek at indexing quiescence (for status surfaces and the
    /// resolution debounce). True once work has started and no progress token is
    /// currently active. Flaps in the gaps between progress sequences.
    pub fn is_ready(&self) -> bool {
        self.ready.0.lock().unwrap().is_ready()
    }

    /// Block until the server's readiness latches — quiescence held for
    /// [`READY_SETTLE`] after the first indexing began — or `timeout` elapses.
    /// Returns whether it ended up ready. Quiescence flaps between progress
    /// sequences before the latch sets, so this re-arms a settle timer each time
    /// the server goes idle and only returns early once the latch fires.
    pub fn wait_ready(&self, timeout: Duration) -> bool {
        let (lock, cvar) = &*self.ready;
        let deadline = Instant::now() + timeout;
        let mut state = lock.lock().unwrap();
        loop {
            if state.poll_latch() {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return state.is_ready();
            }
            // Quiescent but not yet settled: wake when the settle window elapses
            // so the latch can fire without another progress event. Otherwise
            // wait for the next progress notification (or the deadline).
            let remaining = deadline.saturating_duration_since(now);
            let wait = match state.idle_since {
                Some(since) if state.seen_begin && state.active.is_empty() => READY_SETTLE
                    .saturating_sub(since.elapsed())
                    .min(remaining)
                    .max(Duration::from_millis(1)),
                _ => remaining,
            };
            let (next, _timeout) = cvar.wait_timeout(state, wait).unwrap();
            state = next;
        }
    }

    /// Most recent diagnostics frames (bounded), for status surfaces.
    pub fn diagnostics(&self) -> Vec<Value> {
        self.diagnostics.lock().unwrap().iter().cloned().collect()
    }

    /// Most recent stderr lines (bounded), for status surfaces.
    pub fn stderr_tail(&self) -> Vec<String> {
        self.stderr_log.lock().unwrap().iter().cloned().collect()
    }

    /// Lazily open a document from on-disk committed worktree state (worktree ==
    /// HEAD). Idempotent: a document is opened at most once per client.
    pub fn ensure_open(&self, path: &Path) -> Result<String, LspError> {
        let uri = path_to_uri(path);
        {
            let mut opened = self.opened.lock().unwrap();
            if opened.contains(&uri) {
                return Ok(uri);
            }
            opened.insert(uri.clone());
        }
        let text = std::fs::read_to_string(path).unwrap_or_default();
        self.notify(
            "textDocument/didOpen",
            json!({
                "textDocument": {
                    "uri": uri,
                    "languageId": self.language,
                    "version": 1,
                    "text": text,
                }
            }),
        )?;
        Ok(uri)
    }

    /// Send a request and block for its result (the inner `result` value, with
    /// `error` mapped to [`LspError::Transport`]).
    pub fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, LspError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = bounded::<Value>(1);
        self.pending.lock().unwrap().insert(id, tx);

        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if let Err(e) = self.send(&msg) {
            self.pending.lock().unwrap().remove(&id);
            return Err(LspError::Transport(e));
        }

        // The reader may have closed between our registration and now (or before
        // it). If so, no reply can arrive — fail fast rather than block.
        if self.closed.load(Ordering::SeqCst) {
            self.pending.lock().unwrap().remove(&id);
            return Err(LspError::Transport(format!("{method}: connection closed")));
        }

        match rx.recv_timeout(timeout) {
            Ok(resp) => {
                if let Some(err) = resp.get("error") {
                    if !err.is_null() {
                        return Err(LspError::Transport(format!("{method}: {err}")));
                    }
                }
                Ok(resp.get("result").cloned().unwrap_or(Value::Null))
            }
            Err(RecvTimeoutError::Timeout) => {
                self.pending.lock().unwrap().remove(&id);
                Err(LspError::Timeout(format!("{method} timed out")))
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.pending.lock().unwrap().remove(&id);
                Err(LspError::Transport(format!("{method}: connection closed")))
            }
        }
    }

    /// Send a notification (no response expected).
    pub fn notify(&self, method: &str, params: Value) -> Result<(), LspError> {
        let msg = json!({"jsonrpc": "2.0", "method": method, "params": params});
        self.send(&msg).map_err(LspError::Transport)
    }

    /// Graceful shutdown: `shutdown` request then `exit` notification, then a
    /// kill backstop. Idempotent and best-effort.
    pub fn stop(&self) {
        let _ = self.request("shutdown", Value::Null, SHUTDOWN_TIMEOUT);
        let _ = self.notify("exit", Value::Null);
        if let Ok(mut c) = self.child.lock() {
            let _ = c.kill();
        }
    }

    fn initialize(&self, init_options: Option<Value>) -> Result<Value, LspError> {
        let root_uri = path_to_uri(&self.root);
        let name = self
            .root
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("root")
            .to_string();
        let mut params = json!({
            "processId": std::process::id(),
            "rootUri": root_uri,
            "capabilities": client_capabilities(),
            "workspaceFolders": [{"uri": root_uri, "name": name}],
        });
        if let Some(opts) = init_options {
            params["initializationOptions"] = opts;
        }
        let result = self.request("initialize", params, INIT_TIMEOUT)?;
        self.notify("initialized", json!({}))?;
        Ok(result.get("capabilities").cloned().unwrap_or(Value::Null))
    }

    fn send(&self, msg: &Value) -> Result<(), String> {
        let mut w = self
            .stdin
            .lock()
            .map_err(|_| "stdin poisoned".to_string())?;
        write_message(w.as_mut(), msg).map_err(|e| e.to_string())
    }
}

impl Drop for LspClient {
    fn drop(&mut self) {
        // Final backstop: kill the process so the reader/stderr threads observe
        // EOF and exit. Detached JoinHandles drop without joining.
        let _ = self.notify("exit", Value::Null);
        if let Ok(mut c) = self.child.lock() {
            let _ = c.kill();
        }
    }
}

// ---------------------------------------------------------------------------
// Reader / stderr loops and message dispatch.
// ---------------------------------------------------------------------------

fn reader_loop(
    mut stdout: Box<dyn BufRead + Send>,
    pending: &Arc<Mutex<HashMap<i64, Sender<Value>>>>,
    stdin: &Arc<Mutex<Box<dyn Write + Send>>>,
    ready: &Arc<(Mutex<ProgressState>, Condvar)>,
    diagnostics: &Arc<Mutex<VecDeque<Value>>>,
    closed: &Arc<AtomicBool>,
) {
    loop {
        match read_message(stdout.as_mut()) {
            Ok(Some(msg)) => dispatch(msg, pending, stdin, ready, diagnostics),
            Ok(None) => break,
            Err(e) => {
                log::debug!("lsp reader stopped: {e}");
                break;
            }
        }
    }
    // Mark closed BEFORE draining so a request that registers concurrently sees
    // the flag and bails instead of waiting out its timeout.
    closed.store(true, Ordering::SeqCst);
    pending.lock().unwrap().clear();
}

fn stderr_loop(mut stderr: Box<dyn BufRead + Send>, log: &Arc<Mutex<VecDeque<String>>>) {
    let mut line = String::new();
    loop {
        line.clear();
        match stderr.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let mut buf = log.lock().unwrap();
                if buf.len() >= STDERR_CAP {
                    buf.pop_front();
                }
                buf.push_back(line.trim_end().to_string());
            }
        }
    }
}

fn dispatch(
    msg: Value,
    pending: &Arc<Mutex<HashMap<i64, Sender<Value>>>>,
    stdin: &Arc<Mutex<Box<dyn Write + Send>>>,
    ready: &Arc<(Mutex<ProgressState>, Condvar)>,
    diagnostics: &Arc<Mutex<VecDeque<Value>>>,
) {
    let has_id = msg.get("id").map(|v| !v.is_null()).unwrap_or(false);
    let method = msg
        .get("method")
        .and_then(|m| m.as_str())
        .map(str::to_string);

    match (has_id, method) {
        // Response to one of our requests: route by id.
        (true, None) => {
            if let Some(id) = msg.get("id").and_then(|v| v.as_i64()) {
                if let Some(tx) = pending.lock().unwrap().remove(&id) {
                    let _ = tx.send(msg);
                }
            }
        }
        // Server→client request: auto-reply null so the server never blocks on
        // a feature we do not drive (e.g. workDoneProgress/create).
        (true, Some(_)) => {
            let id = msg.get("id").cloned().unwrap_or(Value::Null);
            let reply = json!({"jsonrpc": "2.0", "id": id, "result": Value::Null});
            if let Ok(mut w) = stdin.lock() {
                let _ = write_message(w.as_mut(), &reply);
            }
        }
        // Notification.
        (false, Some(m)) => match m.as_str() {
            "$/progress" => {
                let token = msg
                    .pointer("/params/token")
                    .map(token_key)
                    .unwrap_or_default();
                let kind = msg
                    .pointer("/params/value/kind")
                    .and_then(|k| k.as_str())
                    .unwrap_or("");
                match kind {
                    "begin" => {
                        let (lock, cvar) = &**ready;
                        let mut state = lock.lock().unwrap();
                        state.seen_begin = true;
                        state.active.insert(token);
                        // Work resumed: reset the settle window (unless already
                        // latched, which poll_latch short-circuits).
                        state.idle_since = None;
                        drop(state);
                        // Wake any settle-timer waiter so it re-arms.
                        cvar.notify_all();
                    }
                    "end" => {
                        let (lock, cvar) = &**ready;
                        let mut state = lock.lock().unwrap();
                        state.active.remove(&token);
                        if state.seen_begin && state.active.is_empty() && state.idle_since.is_none()
                        {
                            // Became quiescent: start the settle clock.
                            state.idle_since = Some(Instant::now());
                        }
                        drop(state);
                        // Wake waiters so they park for the (remaining) settle
                        // window and latch when it elapses.
                        cvar.notify_all();
                    }
                    _ => {}
                }
            }
            "textDocument/publishDiagnostics" => {
                let mut buf = diagnostics.lock().unwrap();
                if buf.len() >= DIAG_CAP {
                    buf.pop_front();
                }
                if let Some(params) = msg.get("params") {
                    buf.push_back(params.clone());
                }
            }
            _ => {}
        },
        (false, None) => {}
    }
}

/// Stable string key for a progress `token` (a number or string in LSP).
fn token_key(token: &Value) -> String {
    match token {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Content-Length framing.
// ---------------------------------------------------------------------------

fn write_message(w: &mut dyn Write, msg: &Value) -> std::io::Result<()> {
    let body = serde_json::to_vec(msg)?;
    write!(w, "Content-Length: {}\r\n\r\n", body.len())?;
    w.write_all(&body)?;
    w.flush()
}

/// Read one framed message. `Ok(None)` signals a clean EOF / closed stream.
fn read_message(r: &mut dyn BufRead) -> std::io::Result<Option<Value>> {
    let mut content_length: Option<usize> = None;
    loop {
        let mut line = String::new();
        let n = r.read_line(&mut line)?;
        if n == 0 {
            return Ok(None);
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some(rest) = trimmed.strip_prefix("Content-Length:") {
            content_length = rest.trim().parse().ok();
        }
    }
    let len = match content_length {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    let val = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(val))
}

// ---------------------------------------------------------------------------
// Capabilities + URI helpers (shared with `queries`).
// ---------------------------------------------------------------------------

fn capability_present(caps: &Value, key: &str) -> bool {
    match caps.get(key) {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => true,
    }
}

/// The client capabilities Cairn advertises — exactly the ops the engine uses
/// plus `workDoneProgress` (for indexing readiness). Built from the canonical
/// `lsp_types` structs and serialized to JSON for the request.
fn client_capabilities() -> Value {
    use lsp_types::{
        CallHierarchyClientCapabilities, ClientCapabilities, DocumentSymbolClientCapabilities,
        GotoCapability, HoverClientCapabilities, ReferenceClientCapabilities,
        RenameClientCapabilities, ResourceOperationKind, TextDocumentClientCapabilities,
        TypeHierarchyClientCapabilities, WindowClientCapabilities, WorkspaceClientCapabilities,
        WorkspaceEditClientCapabilities, WorkspaceSymbolClientCapabilities,
    };
    let caps = ClientCapabilities {
        text_document: Some(TextDocumentClientCapabilities {
            definition: Some(GotoCapability::default()),
            references: Some(ReferenceClientCapabilities::default()),
            hover: Some(HoverClientCapabilities::default()),
            implementation: Some(GotoCapability::default()),
            call_hierarchy: Some(CallHierarchyClientCapabilities::default()),
            type_hierarchy: Some(TypeHierarchyClientCapabilities::default()),
            // Request hierarchical document symbols so each symbol carries a
            // `selectionRange` over its identifier; the flat `SymbolInformation`
            // fallback ranges cover the whole declaration (column 0), which is
            // useless as a position op anchor (`resolve_in_file`).
            document_symbol: Some(DocumentSymbolClientCapabilities {
                hierarchical_document_symbol_support: Some(true),
                ..Default::default()
            }),
            // Advertise prepareRename so a non-renameable element (keyword, macro
            // expansion) is rejected honestly up front rather than via a
            // confusing partial edit.
            rename: Some(RenameClientCapabilities {
                prepare_support: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }),
        workspace: Some(WorkspaceClientCapabilities {
            symbol: Some(WorkspaceSymbolClientCapabilities::default()),
            // Required so rust-analyzer emits the `documentChanges` form of a
            // rename `WorkspaceEdit` (carrying versions and the file-move
            // resource ops the rename applier now supports).
            workspace_edit: Some(WorkspaceEditClientCapabilities {
                document_changes: Some(true),
                resource_operations: Some(vec![
                    ResourceOperationKind::Create,
                    ResourceOperationKind::Rename,
                    ResourceOperationKind::Delete,
                ]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        window: Some(WindowClientCapabilities {
            work_done_progress: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    serde_json::to_value(caps).unwrap_or_else(|_| json!({}))
}

/// Percent-encode a filesystem path into a `file://` URI.
pub(crate) fn path_to_uri(p: &Path) -> String {
    let s = p.to_string_lossy();
    let mut out = String::from("file://");
    for b in s.bytes() {
        match b {
            b'/' | b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Parse a `file://` URI back into a filesystem path (percent-decoded).
pub(crate) fn uri_to_path(uri: &str) -> Option<PathBuf> {
    let rest = uri.strip_prefix("file://")?;
    // Strip an (almost always empty) authority component before the path.
    let path_part = match rest.find('/') {
        Some(0) => rest,
        Some(idx) => &rest[idx..],
        None => rest,
    };
    Some(PathBuf::from(percent_decode(path_part)))
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

// ---------------------------------------------------------------------------
// Test harness: a scripted in-memory language server over channel-backed stdio.
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod testkit {
    use super::*;
    use crossbeam_channel::{unbounded, Receiver};
    use std::io::{BufReader, Read};

    /// A `Read` fed by discrete byte chunks over a channel; blocks for the next
    /// chunk and reports EOF (`Ok(0)`) when the channel closes. This lets the
    /// scripted server's stdout block until the client writes the matching
    /// request, giving deterministic id correlation.
    struct ChannelReader {
        rx: Receiver<Vec<u8>>,
        buf: Vec<u8>,
        pos: usize,
    }

    impl Read for ChannelReader {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.buf.len() {
                match self.rx.recv() {
                    Ok(chunk) => {
                        self.buf = chunk;
                        self.pos = 0;
                    }
                    Err(_) => return Ok(0),
                }
            }
            let n = (self.buf.len() - self.pos).min(out.len());
            out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    struct ChannelWriter {
        tx: Sender<Vec<u8>>,
    }

    impl Write for ChannelWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let _ = self.tx.send(buf.to_vec());
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A `ChildProcess` whose stdio is wired to an in-process scripted server.
    pub(crate) struct MockLspChild {
        stdin_tx: Option<Sender<Vec<u8>>>,
        stdout_rx: Option<Receiver<Vec<u8>>>,
        killed: bool,
    }

    impl ChildProcess for MockLspChild {
        fn id(&self) -> u32 {
            4242
        }
        fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
            self.stdout_rx.take().map(|rx| {
                Box::new(BufReader::new(ChannelReader {
                    rx,
                    buf: Vec::new(),
                    pos: 0,
                })) as Box<dyn BufRead + Send>
            })
        }
        fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
            None
        }
        fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
            self.stdin_tx
                .take()
                .map(|tx| Box::new(ChannelWriter { tx }) as Box<dyn Write + Send>)
        }
        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            if self.killed {
                use std::process::Command;
                Ok(Some(Command::new("true").status()?))
            } else {
                Ok(None)
            }
        }
        fn kill(&mut self) -> std::io::Result<()> {
            self.killed = true;
            // Drop the stdout sender side by dropping our receiver handle is not
            // enough; the server thread owns the sender. Closing stdin_tx lets
            // the server thread observe EOF and exit, which drops stdout_tx.
            self.stdin_tx = None;
            Ok(())
        }
    }

    /// Build a scripted server. `responses` maps an LSP method to the canned
    /// `result` value; unmapped requests get `null`. When `emit_progress` is
    /// set, a `$/progress` end frame is sent after `initialized` to flip
    /// readiness. Every received request method is appended to `recorded`.
    pub(crate) fn mock_child(
        responses: HashMap<String, Value>,
        emit_progress: bool,
        recorded: Arc<Mutex<Vec<String>>>,
    ) -> MockLspChild {
        let (stdin_tx, stdin_rx) = unbounded::<Vec<u8>>();
        let (stdout_tx, stdout_rx) = unbounded::<Vec<u8>>();

        std::thread::spawn(move || {
            let mut reader = BufReader::new(ChannelReader {
                rx: stdin_rx,
                buf: Vec::new(),
                pos: 0,
            });
            while let Ok(Some(msg)) = read_message(&mut reader) {
                let method = msg
                    .get("method")
                    .and_then(|m| m.as_str())
                    .unwrap_or("")
                    .to_string();
                recorded.lock().unwrap().push(method.clone());
                match msg.get("id").cloned() {
                    Some(id) if !id.is_null() => {
                        let result = responses.get(&method).cloned().unwrap_or(Value::Null);
                        let resp = json!({"jsonrpc": "2.0", "id": id, "result": result});
                        if stdout_tx.send(frame(&resp)).is_err() {
                            break;
                        }
                    }
                    _ => {
                        if method == "initialized" && emit_progress {
                            // A begin/end pair drives the quiescence model to
                            // ready (seen a begin, no active token remains).
                            for kind in ["begin", "end"] {
                                let prog = json!({
                                    "jsonrpc": "2.0",
                                    "method": "$/progress",
                                    "params": {"token": "t", "value": {"kind": kind}}
                                });
                                if stdout_tx.send(frame(&prog)).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        MockLspChild {
            stdin_tx: Some(stdin_tx),
            stdout_rx: Some(stdout_rx),
            killed: false,
        }
    }

    fn frame(msg: &Value) -> Vec<u8> {
        let body = serde_json::to_vec(msg).unwrap();
        let mut out = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        out.extend_from_slice(&body);
        out
    }

    /// Default `initialize` result advertising every op this engine drives.
    pub(crate) fn full_capabilities() -> Value {
        json!({
            "capabilities": {
                "definitionProvider": true,
                "referencesProvider": true,
                "hoverProvider": true,
                "implementationProvider": true,
                "callHierarchyProvider": true,
                "typeHierarchyProvider": true,
                "workspaceSymbolProvider": true,
                "documentSymbolProvider": true
            }
        })
    }

    /// Construct a connected [`LspClient`] backed by a scripted server.
    pub(crate) fn scripted_client(
        responses: HashMap<String, Value>,
        emit_progress: bool,
    ) -> (LspClient, Arc<Mutex<Vec<String>>>) {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let child = mock_child(responses, emit_progress, recorded.clone());
        let client = LspClient::start(Box::new(child), "rust", Path::new("/tmp/lsp-root"), None)
            .expect("scripted client should start");
        (client, recorded)
    }
}

#[cfg(test)]
mod tests {
    use super::testkit::{full_capabilities, scripted_client};
    use super::*;
    use std::collections::HashMap;

    fn with_init(mut responses: HashMap<String, Value>) -> HashMap<String, Value> {
        responses.insert("initialize".to_string(), full_capabilities());
        responses
    }

    #[test]
    fn handshake_records_capabilities() {
        let (client, recorded) = scripted_client(with_init(HashMap::new()), false);
        assert!(client.handshake_ok());
        assert!(client.supports(super::super::LspOp::Definition));
        assert!(client.supports(super::super::LspOp::Callers));
        // initialize and initialized were both seen by the server. The
        // `initialized` notification is fire-and-forget, so poll briefly for the
        // scripted server thread to consume it rather than asserting instantly.
        let saw_initialized = (0..100).any(|_| {
            if recorded
                .lock()
                .unwrap()
                .contains(&"initialized".to_string())
            {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
            false
        });
        let seen = recorded.lock().unwrap();
        assert!(seen.contains(&"initialize".to_string()));
        assert!(
            saw_initialized,
            "server should observe the initialized notification"
        );
    }

    #[test]
    fn correlates_response_by_id() {
        let mut responses = with_init(HashMap::new());
        responses.insert(
            "textDocument/hover".to_string(),
            json!({"contents": {"kind": "markdown", "value": "fn foo()"}}),
        );
        let (client, _) = scripted_client(responses, false);
        let result = client
            .request(
                "textDocument/hover",
                json!({"textDocument": {"uri": "file:///x"}, "position": {"line": 0, "character": 0}}),
                DEFAULT_TIMEOUT,
            )
            .expect("hover should resolve");
        assert_eq!(
            result.pointer("/contents/value").and_then(|v| v.as_str()),
            Some("fn foo()")
        );
    }

    #[test]
    fn readiness_flips_on_progress_end() {
        let (client, _) = scripted_client(with_init(HashMap::new()), true);
        assert!(
            client.wait_ready(Duration::from_secs(2)),
            "a $/progress end frame must flip readiness"
        );
    }

    #[test]
    fn readiness_times_out_when_no_progress() {
        let (client, _) = scripted_client(with_init(HashMap::new()), false);
        assert!(
            !client.wait_ready(Duration::from_millis(150)),
            "with no progress-end frame, wait_ready must time out (still indexing)"
        );
    }

    #[test]
    fn readiness_latches_after_sustained_quiescence() {
        // begin+end makes the server quiescent; the latch fires once quiescence
        // has held for READY_SETTLE. Allow margin above that 2s window.
        let (client, _) = scripted_client(with_init(HashMap::new()), true);
        assert!(
            client.wait_ready(Duration::from_secs(5)),
            "readiness must latch after sustained quiescence"
        );
        assert!(client.is_ready(), "stays ready once latched");
    }

    #[test]
    fn supports_gates_on_advertised_capabilities() {
        // A server that advertises only definition: subtypes must read as
        // unsupported (honest capability degradation).
        let mut responses = HashMap::new();
        responses.insert(
            "initialize".to_string(),
            json!({"capabilities": {"definitionProvider": true, "typeHierarchyProvider": false}}),
        );
        let (client, _) = scripted_client(responses, false);
        assert!(client.supports(super::super::LspOp::Definition));
        assert!(!client.supports(super::super::LspOp::Subtypes));
        assert!(!client.supports(super::super::LspOp::Hover));
    }

    #[test]
    fn uri_path_roundtrip_handles_spaces() {
        let p = Path::new("/tmp/a b/c.rs");
        let uri = path_to_uri(p);
        assert_eq!(uri, "file:///tmp/a%20b/c.rs");
        assert_eq!(uri_to_path(&uri).as_deref(), Some(p));
    }
}
