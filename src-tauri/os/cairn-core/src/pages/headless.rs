//! Rendering a page headlessly: what `read cairn:~/pages/<name>` returns.
//!
//! The runner spawns bun on the page render bundle's host
//! (`packages/page-kit/headless/host.ts`, built by
//! `scripts/build-page-render.ts`), inside an isolated-child confinement
//! (`cairn_sandbox::IsolatedChild`): no network, no IPC lookups, reads only of
//! the bundle, the runtime and its scratch dir, and resource limits. The host
//! runs the page with the kit's text projection and asks this side for every
//! read over stdio; each one is answered by [`page_read`] exactly as a frame's
//! is, with the page, version and project bound here and never taken from the
//! child, and with the reading agent's grants intersected in. When the page
//! settles (or the deadline arrives) the host serializes it once and exits.
//!
//! Bounds: two renders at a time across the process and one per reader; a
//! hard wall clock of the settle deadline plus five seconds; a resident-memory
//! watchdog; at most 1000 reads per render, 8 in flight; lines of at most 8 MiB.
//! A kill takes the child's whole process group.
//!
//! Nothing the child does can block the loop that enforces those bounds: lines
//! to the child go through a writer task, and the loop only enqueues them. A
//! page that stops draining its stdin (an `Atomics.wait` costs no CPU, so the
//! CPU limit never fires) fills the queue and is stopped at its byte bound or
//! the wall clock, whichever comes first.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Semaphore};

use super::data::DEFAULT_ENTRY;
use super::log::{PageCallOutcome, PageCallVerb};
use super::read::{
    page_log_event, page_read, PageCallError, PageLogEvent, PageReadPurpose, PageReadRequest,
    PageReader,
};
use super::store::{PageAddress, PageRow};
use crate::orchestrator::Orchestrator;

/// How long a render waits for the page to settle before serializing it.
pub(crate) const DEFAULT_SETTLE: Duration = Duration::from_secs(10);
/// How far past the settle deadline the child may run before it is killed.
const HARD_GRACE: Duration = Duration::from_secs(5);
const MAX_RSS_BYTES: u64 = 512 * 1024 * 1024;
const RSS_POLL: Duration = Duration::from_millis(250);
const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 8 * 1024;
const MAX_READS: usize = 1000;
const MAX_READS_IN_FLIGHT: usize = 8;
const MAX_CONCURRENT_RENDERS: usize = 2;
/// Bytes waiting for the child to read before the render is stopped. One line
/// is always accepted onto an empty queue, so a single result as large as a
/// page read allows still goes through.
const MAX_QUEUED_BYTES: usize = 32 * 1024 * 1024;
/// Diagnostics logged as `diag` rows per render, and how many the body lists.
const MAX_LOGGED_DIAGNOSTICS: usize = 20;
const MAX_LISTED_DIAGNOSTICS: usize = 5;

/// The limits one render runs under.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RenderLimits {
    pub settle: Duration,
}

impl Default for RenderLimits {
    fn default() -> Self {
        Self {
            settle: DEFAULT_SETTLE,
        }
    }
}

impl RenderLimits {
    fn hard(&self) -> Duration {
        self.settle + HARD_GRACE
    }
}

/// Answers a rendered page's reads. Production is [`page_read`]; a test wraps
/// it to withhold a target or count calls.
pub(crate) trait PageCallAnswerer: Send + Sync {
    fn answer<'a>(
        &'a self,
        orch: &'a Orchestrator,
        request: PageReadRequest,
        viewer: &'a Value,
    ) -> BoxFuture<'a, Result<Value, PageCallError>>;
}

pub(crate) struct PageReadAnswerer;

impl PageCallAnswerer for PageReadAnswerer {
    fn answer<'a>(
        &'a self,
        orch: &'a Orchestrator,
        request: PageReadRequest,
        viewer: &'a Value,
    ) -> BoxFuture<'a, Result<Value, PageCallError>> {
        Box::pin(async move { page_read(orch, &request, viewer).await })
    }
}

/// One page version to render, and for whom.
pub(crate) struct RenderRequest<'a> {
    pub address: &'a PageAddress,
    pub project_id: &'a str,
    pub row: &'a PageRow,
    pub reader: PageReader,
}

/// How a render ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RenderOutcome {
    /// The page settled and was serialized.
    Settled,
    /// The deadline arrived first; unsettled parts read as still reading.
    Deadline { still_reading: Vec<String> },
    /// The page threw while loading or rendering.
    Failed { message: String },
    /// The child was killed at a bound.
    Stopped { message: String },
    /// The render could not run here (no confinement, no bundle, busy).
    Refused { message: String },
}

#[derive(Debug, Clone)]
pub(crate) struct RenderedPage {
    pub outcome: RenderOutcome,
    pub markdown: String,
    pub elapsed: Duration,
    /// Reads the page made (answered or refused).
    pub reads: usize,
    /// Reads refused to this reader, with why: listed in the body.
    pub refused: Vec<String>,
    /// The first diagnostics the page reported.
    pub diagnostics: Vec<String>,
}

impl RenderedPage {
    fn refused(message: String) -> Self {
        Self {
            outcome: RenderOutcome::Refused { message },
            markdown: String::new(),
            elapsed: Duration::ZERO,
            reads: 0,
            refused: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
}

fn render_permits() -> &'static Semaphore {
    static PERMITS: OnceLock<Semaphore> = OnceLock::new();
    PERMITS.get_or_init(|| Semaphore::new(MAX_CONCURRENT_RENDERS))
}

/// The readers with a render in flight: one at a time each, so a page that
/// reads many resources cannot fan one reader out into many renders.
fn readers_rendering() -> &'static Mutex<HashMap<String, usize>> {
    static READERS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    READERS.get_or_init(|| Mutex::new(HashMap::new()))
}

struct ReaderSlot(Option<String>);

impl ReaderSlot {
    fn take(reader: &PageReader) -> Result<Self, String> {
        let key = match reader {
            PageReader::Agent(agent) => agent.run_id.clone(),
            _ => return Ok(Self(None)),
        };
        let mut readers = readers_rendering()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if readers.get(&key).copied().unwrap_or(0) >= 1 {
            return Err(
                "this run is already rendering a page; read one page's rendered view at a time"
                    .to_string(),
            );
        }
        *readers.entry(key.clone()).or_insert(0) += 1;
        Ok(Self(Some(key)))
    }
}

impl Drop for ReaderSlot {
    fn drop(&mut self) {
        if let Some(key) = &self.0 {
            let mut readers = readers_rendering()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(count) = readers.get_mut(key) {
                *count -= 1;
                if *count == 0 {
                    readers.remove(key);
                }
            }
        }
    }
}

/// Render one page version as Markdown.
pub(crate) async fn render(
    orch: &Orchestrator,
    request: RenderRequest<'_>,
    limits: RenderLimits,
    answerer: &dyn PageCallAnswerer,
) -> RenderedPage {
    if let Err(why) = cairn_sandbox::IsolatedChild::availability() {
        return RenderedPage::refused(format!(
            "Rendered reads need kernel confinement, which this host lacks ({why}); read ?format=json for the page's source"
        ));
    }
    // A development build may rebuild the bundle here, which runs bun.
    let bundle = match tokio::task::spawn_blocking(crate::runtime::page_render_bundle).await {
        Ok(Ok(bundle)) => bundle,
        Ok(Err(error)) => return RenderedPage::refused(error),
        Err(error) => {
            return RenderedPage::refused(format!(
                "locating the page render bundle failed: {error}"
            ))
        }
    };
    let _slot = match ReaderSlot::take(&request.reader) {
        Ok(slot) => slot,
        Err(why) => return RenderedPage::refused(why),
    };
    let _permit =
        match tokio::time::timeout(limits.settle, render_permits().acquire()).await {
            Ok(Ok(permit)) => permit,
            _ => {
                return RenderedPage::refused(format!(
                    "the page renderer is busy ({MAX_CONCURRENT_RENDERS} renders already running); try again, or read ?format=json"
                ))
            }
        };
    let scratch = match tempfile::Builder::new()
        .prefix("cairn-page-render-")
        .tempdir()
    {
        Ok(dir) => dir,
        Err(error) => {
            return RenderedPage::refused(format!("no scratch dir for the render: {error}"))
        }
    };
    let scratch_path = scratch
        .path()
        .canonicalize()
        .unwrap_or_else(|_| scratch.path().to_path_buf());
    match run_child(orch, &request, limits, answerer, &bundle, &scratch_path).await {
        Ok(rendered) => rendered,
        Err(error) => RenderedPage::refused(error),
    }
}

fn bun_program() -> Result<PathBuf, String> {
    let bun = crate::env::resolve_sidecar("bun");
    let path = PathBuf::from(&bun);
    if !path.is_absolute() {
        return Err("bun is not installed beside Cairn or on PATH, so pages cannot render here; read ?format=json for the source".to_string());
    }
    // Confinement names the binary it allows to run, so name the real file.
    Ok(path.canonicalize().unwrap_or(path))
}

fn init_line(request: &RenderRequest<'_>, limits: RenderLimits) -> Value {
    let data = &request.row.data;
    json!({
        "page": {
            "uri": request.address.uri(),
            "name": request.address.name,
            "version": request.row.version,
            "entry": data.get("entry").and_then(Value::as_str).unwrap_or(DEFAULT_ENTRY),
            "files": data.get("files").cloned().unwrap_or_else(|| json!({})),
            "home": {
                "thread": request.address.thread_name(),
                "project": request.address.project,
            },
        },
        "settleMs": limits.settle.as_millis() as u64,
    })
}

struct Session<'a> {
    orch: &'a Orchestrator,
    request: &'a RenderRequest<'a>,
    viewer: &'a Value,
    reads: usize,
    refused: Vec<String>,
    diagnostics: Vec<String>,
    logged_diagnostics: usize,
}

impl Session<'_> {
    fn read_request(&self, message: &Value) -> Result<PageReadRequest, String> {
        let paths: Vec<String> = message
            .get("paths")
            .and_then(Value::as_array)
            .map(|paths| {
                paths
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        // A rendered read is one moment: it opens subscriptions and reads once,
        // and never refreshes.
        let purpose = match message.get("purpose").and_then(Value::as_str) {
            Some("read") => PageReadPurpose::Read,
            Some("subscribe-open") => PageReadPurpose::SubscribeOpen,
            other => {
                return Err(format!(
                    "a rendered read makes `read` and `subscribe-open` calls, not {}",
                    other.unwrap_or("an unnamed one")
                ))
            }
        };
        Ok(PageReadRequest {
            project_id: self.request.project_id.to_string(),
            page: self.request.address.uri(),
            version: self.request.row.version,
            paths,
            include_bodies: message.get("includeBodies").and_then(Value::as_bool) == Some(true),
            purpose,
            reader: self.request.reader.clone(),
        })
    }

    async fn log(&mut self, verb: PageCallVerb, outcome: PageCallOutcome, detail: String) {
        self.log_targets(verb, outcome, detail, Vec::new()).await;
    }

    async fn log_targets(
        &mut self,
        verb: PageCallVerb,
        outcome: PageCallOutcome,
        detail: String,
        targets: Vec<String>,
    ) {
        let event = PageLogEvent {
            project_id: self.request.project_id.to_string(),
            page: self.request.address.uri(),
            version: self.request.row.version,
            verb,
            outcome,
            targets,
            detail,
        };
        if let Err(error) = page_log_event(self.orch, &event, self.viewer).await {
            log::warn!("rendered page event was not logged: {error}");
        }
    }

    /// A call-log event the host reported: a page diagnostic or a refused verb.
    async fn host_event(&mut self, message: &Value) {
        let detail = message
            .get("detail")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let verb = match message.get("verb").and_then(Value::as_str) {
            Some("write") => PageCallVerb::Write,
            _ => PageCallVerb::Diag,
        };
        let outcome = match message.get("outcome").and_then(Value::as_str) {
            Some("refused") => PageCallOutcome::Refused,
            Some("ok") => PageCallOutcome::Ok,
            _ => PageCallOutcome::Error,
        };
        if self.diagnostics.len() < MAX_LISTED_DIAGNOSTICS {
            let first = detail.lines().next().unwrap_or_default();
            self.diagnostics.push(first.chars().take(400).collect());
        }
        if self.logged_diagnostics < MAX_LOGGED_DIAGNOSTICS {
            self.logged_diagnostics += 1;
            self.log(verb, outcome, detail).await;
        }
    }
}

/// The loop's side of the child's stdin: enqueue a line, never wait on the pipe.
struct ToChild {
    lines: mpsc::Sender<String>,
    queued: Arc<AtomicUsize>,
}

impl ToChild {
    fn send(&self, line: String) -> Result<(), RenderOutcome> {
        let len = line.len();
        let before = self.queued.fetch_add(len, Ordering::SeqCst);
        let stopped = || {
            RenderOutcome::Stopped {
            message: format!(
                "Page stopped taking its results: more than {} MiB waiting for it to read (stopped)",
                MAX_QUEUED_BYTES / (1024 * 1024)
            ),
        }
        };
        if before > 0 && before + len > MAX_QUEUED_BYTES {
            return Err(stopped());
        }
        match self.lines.try_send(line) {
            Err(mpsc::error::TrySendError::Full(_)) => Err(stopped()),
            // The child's stdin is gone because the child is exiting; its
            // stdout says how, so a late line is dropped, not a failure.
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => Ok(()),
        }
    }
}

/// Write queued lines to the child until the queue closes or the pipe breaks.
fn spawn_writer(
    mut stdin: tokio::process::ChildStdin,
) -> (ToChild, tokio::task::JoinHandle<std::io::Result<()>>) {
    // Every line answers one read the child asked for (plus the init line), so
    // the channel never fills before the byte bound does.
    let (lines, mut outbox) = mpsc::channel::<String>(MAX_READS + 1);
    let queued = Arc::new(AtomicUsize::new(0));
    let drained = queued.clone();
    let writer = tokio::spawn(async move {
        while let Some(line) = outbox.recv().await {
            stdin.write_all(line.as_bytes()).await?;
            drained.fetch_sub(line.len(), Ordering::SeqCst);
        }
        Ok(())
    });
    (ToChild { lines, queued }, writer)
}

fn result_line(id: &Value, outcome: Result<Value, String>) -> String {
    let message = match outcome {
        Ok(value) => json!({ "type": "result", "id": id, "ok": true, "value": value }),
        Err(error) => {
            json!({ "type": "result", "id": id, "ok": false, "error": { "message": error } })
        }
    };
    format!("{message}\n")
}

#[cfg(unix)]
fn kill_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // SAFETY: signalling a process group we created; ESRCH if it is gone.
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_group(_pid: Option<u32>) {}

fn resident_bytes(pid: Option<u32>) -> Option<u64> {
    use crate::agent_process::memory::{MemoryProbe, OsMemoryProbe};
    OsMemoryProbe.process_rss_bytes(pid?)
}

/// Split complete lines off `buffer`, refusing a line longer than the cap.
fn take_lines(buffer: &mut Vec<u8>) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = buffer.drain(..=newline).collect();
        let text = String::from_utf8_lossy(&line);
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        out.push(
            serde_json::from_str(text)
                .map_err(|error| format!("the page host wrote a line that is not JSON: {error}"))?,
        );
    }
    if buffer.len() > MAX_LINE_BYTES {
        return Err(format!(
            "the page host wrote a line longer than {MAX_LINE_BYTES} bytes"
        ));
    }
    Ok(out)
}

async fn run_child(
    orch: &Orchestrator,
    request: &RenderRequest<'_>,
    limits: RenderLimits,
    answerer: &dyn PageCallAnswerer,
    bundle: &Path,
    scratch: &Path,
) -> Result<RenderedPage, String> {
    let bun = bun_program()?;
    let host = bundle.join(crate::runtime::PAGE_RENDER_HOST);
    let isolated = cairn_sandbox::IsolatedChild::new(&bun, vec![bundle.to_path_buf()], scratch);
    let (program, args) = isolated.wrap_argv(&[
        "--smol".to_string(),
        "--no-install".to_string(),
        host.to_string_lossy().into_owned(),
    ]);
    // On macOS, watch the violation log first, so a child the profile kills at
    // startup can be told what it was refused. Elsewhere this is a no-op.
    let watcher = isolated.clone();
    let _ = tokio::task::spawn_blocking(move || watcher.watch_violations()).await;
    let spawned_at = std::time::SystemTime::now();
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .env_clear()
        .env("HOME", scratch)
        .env("TMPDIR", scratch)
        .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
        .env("NO_COLOR", "1")
        .current_dir(scratch)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        command.process_group(0);
        isolated.install_pre_exec(command.as_std_mut());
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("Rendered reads need kernel confinement, which could not be applied here ({error}); read ?format=json for the source"))?;
    let pid = child.id();
    let stdin = child.stdin.take().ok_or("the page host has no stdin")?;
    let mut stdout = child.stdout.take().ok_or("the page host has no stdout")?;
    let mut stderr = child.stderr.take().ok_or("the page host has no stderr")?;
    let stderr_task = tokio::spawn(async move {
        let mut captured = Vec::new();
        let mut chunk = [0u8; 4096];
        while let Ok(read) = stderr.read(&mut chunk).await {
            if read == 0 {
                break;
            }
            if captured.len() < MAX_STDERR_BYTES {
                captured.extend_from_slice(&chunk[..read.min(MAX_STDERR_BYTES - captured.len())]);
            }
        }
        String::from_utf8_lossy(&captured).into_owned()
    });

    let started = Instant::now();
    let viewer = request
        .reader
        .actor()
        .unwrap_or_else(|| json!({ "kind": "anonymous" }));
    let viewer = &viewer;
    let mut session = Session {
        orch,
        request,
        viewer,
        reads: 0,
        refused: Vec::new(),
        diagnostics: Vec::new(),
        logged_diagnostics: 0,
    };

    let (to_child, mut writer) = spawn_writer(stdin);
    let init = format!("{}\n", init_line(request, limits));
    let init_sent = to_child.send(init);
    let mut writer_done = false;

    let deadline = tokio::time::sleep(limits.hard());
    tokio::pin!(deadline);
    let mut watchdog = tokio::time::interval(RSS_POLL);
    let mut buffer = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut in_flight: FuturesUnordered<BoxFuture<'_, (Value, Result<Value, String>)>> =
        FuturesUnordered::new();
    let mut queued: Vec<(Value, PageReadRequest)> = Vec::new();

    let ending: Result<Value, RenderOutcome> = 'run: loop {
        if let Err(outcome) = &init_sent {
            break 'run Err(outcome.clone());
        }
        while in_flight.len() < MAX_READS_IN_FLIGHT && !queued.is_empty() {
            let (id, read) = queued.remove(0);
            in_flight.push(Box::pin(async move {
                let outcome = answerer
                    .answer(orch, read, viewer)
                    .await
                    .map_err(|error| error.to_string());
                (id, outcome)
            }));
        }
        tokio::select! {
            read = stdout.read(&mut chunk) => {
                let read = read.map_err(|error| format!("reading the page host: {error}"))?;
                if read == 0 {
                    break 'run Err(RenderOutcome::Failed { message: String::new() });
                }
                buffer.extend_from_slice(&chunk[..read]);
                let messages = match take_lines(&mut buffer) {
                    Ok(messages) => messages,
                    Err(why) => break 'run Err(RenderOutcome::Stopped { message: why }),
                };
                for message in messages {
                    match message.get("type").and_then(Value::as_str) {
                        Some("read") => {
                            let id = message.get("id").cloned().unwrap_or(Value::Null);
                            session.reads += 1;
                            let parsed = if session.reads > MAX_READS {
                                Err(format!("a rendered page makes at most {MAX_READS} reads"))
                            } else {
                                session.read_request(&message)
                            };
                            match parsed {
                                Ok(read) => queued.push((id, read)),
                                Err(why) => {
                                    if let Err(outcome) = to_child.send(result_line(&id, Err(why))) {
                                        break 'run Err(outcome);
                                    }
                                }
                            }
                        }
                        Some("log") => session.host_event(&message).await,
                        // A rendered read never writes, so an agent reading a
                        // page cannot cause one. The render ends here, whatever
                        // the page would have done with the refusal, and the
                        // child is killed below. Nothing in this module can
                        // reach `page_write`: only the invoke handler builds a
                        // page-write origin.
                        Some("write") => {
                            let targets: Vec<String> = message
                                .get("targets")
                                .and_then(Value::as_array)
                                .map(|targets| {
                                    targets
                                        .iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect()
                                })
                                .unwrap_or_default();
                            let what = if targets.is_empty() {
                                "the write".to_string()
                            } else {
                                format!("write to {}", targets.join(", "))
                            };
                            let detail = format!("a headless render cannot write ({what} refused)");
                            session
                                .log_targets(
                                    PageCallVerb::Write,
                                    PageCallOutcome::Refused,
                                    detail.clone(),
                                    targets,
                                )
                                .await;
                            break 'run Ok(json!({
                                "type": "failed",
                                "message": format!("Page failed: {detail}"),
                            }));
                        }
                        Some("rendered") | Some("failed") => break 'run Ok(message),
                        _ => {}
                    }
                }
            }
            Some((id, outcome)) = in_flight.next(), if !in_flight.is_empty() => {
                if let Err(why) = &outcome {
                    session.refused.push(why.clone());
                }
                if let Err(outcome) = to_child.send(result_line(&id, outcome)) {
                    break 'run Err(outcome);
                }
            }
            // The writer ends early only when the child's stdin broke, which a
            // host that has sent its verdict and exited does too. The end of
            // stdout is the one signal the child is gone, so this only stops
            // polling a finished task.
            _ = &mut writer, if !writer_done => {
                writer_done = true;
            }
            () = &mut deadline => {
                break 'run Err(RenderOutcome::Stopped {
                    message: format!(
                        "Page did not finish rendering within {} s (stopped)",
                        limits.hard().as_secs()
                    ),
                });
            }
            _ = watchdog.tick() => {
                if resident_bytes(pid).is_some_and(|bytes| bytes > MAX_RSS_BYTES) {
                    break 'run Err(RenderOutcome::Stopped {
                        message: format!(
                            "Page exceeded {} MiB of memory while rendering (stopped)",
                            MAX_RSS_BYTES / (1024 * 1024)
                        ),
                    });
                }
            }
        }
    };

    kill_group(pid);
    drop(to_child);
    writer.abort();
    drop(in_flight);
    let _ = child.kill().await;
    let status = child.wait().await.ok();
    let stderr = stderr_task.await.unwrap_or_default();
    let elapsed = started.elapsed();

    let outcome_and_markdown = match ending {
        Ok(message) if message.get("type").and_then(Value::as_str) == Some("rendered") => {
            let markdown = message
                .get("markdown")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let settled = message.get("settled").and_then(Value::as_bool) == Some(true);
            let mut still_reading: Vec<String> = Vec::new();
            if let Some(unsettled) = message.get("unsettled") {
                for key in ["reads", "calls"] {
                    for uri in unsettled
                        .get(key)
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if let Some(uri) = uri.as_str() {
                            still_reading.push(uri.to_string());
                        }
                    }
                }
            }
            let outcome = if settled {
                RenderOutcome::Settled
            } else {
                RenderOutcome::Deadline { still_reading }
            };
            (outcome, markdown)
        }
        Ok(message) => {
            let text = message
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Page failed")
                .to_string();
            (RenderOutcome::Failed { message: text }, String::new())
        }
        Err(RenderOutcome::Failed { .. }) => {
            // The host exited without a verdict: a refused confinement shows
            // here, before any page code ran.
            let message = if stderr.contains("sandbox_apply")
                || stderr.contains("landlock")
                || stderr.contains("seccomp")
            {
                format!("Rendered reads need kernel confinement, which could not be applied here: {}; read ?format=json for the source", stderr.trim())
            } else {
                format!(
                    "The page host exited without rendering ({}){}",
                    status.map_or_else(|| "no status".to_string(), |status| status.to_string()),
                    if stderr.trim().is_empty() {
                        String::new()
                    } else {
                        format!(": {}", stderr.trim())
                    }
                )
            };
            (RenderOutcome::Failed { message }, String::new())
        }
        Err(other) => (other, String::new()),
    };
    let (mut outcome, markdown) = outcome_and_markdown;
    if let RenderOutcome::Failed { message } | RenderOutcome::Stopped { message } = &mut outcome {
        // What the sandbox refused is the likeliest explanation of a child that
        // died without saying why.
        let probe = isolated.clone();
        let denials = tokio::task::spawn_blocking(move || probe.denials_since(spawned_at))
            .await
            .unwrap_or_default();
        if !denials.is_empty() {
            let shown: Vec<&str> = denials.iter().take(8).map(String::as_str).collect();
            message.push_str(&format!(" (the sandbox refused: {})", shown.join("; ")));
        }
    }
    if let RenderOutcome::Failed { message } | RenderOutcome::Stopped { message } = &outcome {
        session
            .log(PageCallVerb::Diag, PageCallOutcome::Error, message.clone())
            .await;
    }
    Ok(RenderedPage {
        outcome,
        markdown,
        elapsed,
        reads: session.reads.min(MAX_READS),
        refused: session.refused,
        diagnostics: session.diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_split_on_newlines_and_a_long_line_is_refused() {
        let mut buffer = b"{\"type\":\"read\",\"id\":1}\n{\"type\":\"lo".to_vec();
        let lines = take_lines(&mut buffer).unwrap();
        assert_eq!(lines, vec![json!({"type": "read", "id": 1})]);
        assert_eq!(buffer, b"{\"type\":\"lo");
        let mut long = vec![b'x'; MAX_LINE_BYTES + 1];
        assert!(take_lines(&mut long).unwrap_err().contains("longer than"));
    }
}
