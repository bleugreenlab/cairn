//! Unified logging module for all Cairn binaries.
//!
//! Provides a dual-layer tracing subscriber:
//! - JSON Lines file layer (daily rotation, configurable age retention). The directory is
//!   resolved by `paths::cairn_log_dir`, which separates dev (`~/.cairn-dev/logs`)
//!   from prod (`~/.cairn/logs`) and honors the `CAIRN_LOG_DIR` override.
//! - Pretty stderr layer (ANSI when TTY, respects RUST_LOG)
//!
//! Logging is diagnostics, never a precondition for running. A log destination
//! the process cannot write degrades the subscriber to its remaining layers
//! rather than failing [`init`] — see that function for why.
//!
//! All `log::` crate calls are bridged into tracing via `tracing-log`.
//! Call `init()` once at startup in each binary.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Identifies which binary is logging. Used in log file naming.
#[derive(Debug, Clone, Copy)]
pub enum ProcessTag {
    App,
    Cmd,
    Executor,
    Server,
    Runner,
}

fn is_cairn_jsonl(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("cairn-") && name.ends_with(".jsonl"))
}

fn maintain_log_dir(
    log_dir: &Path,
    retention_days: u64,
    now: SystemTime,
    warned_large_files: &mut HashSet<PathBuf>,
) {
    let cutoff = now
        .checked_sub(Duration::from_secs(
            retention_days.saturating_mul(24 * 60 * 60),
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !is_cairn_jsonl(&path) {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.modified().is_ok_and(|modified| modified < cutoff) {
            if let Err(error) = std::fs::remove_file(&path) {
                eprintln!(
                    "WARNING: failed to prune expired Cairn log {}: {error}",
                    path.display()
                );
            } else {
                warned_large_files.remove(&path);
            }
            continue;
        }
        if metadata.len() >= LOG_SIZE_WARNING_BYTES && warned_large_files.insert(path.clone()) {
            eprintln!(
                "WARNING: Cairn daily log {} is {} bytes (threshold {} bytes); investigate runaway logging",
                path.display(), metadata.len(), LOG_SIZE_WARNING_BYTES
            );
        }
    }
}

fn spawn_log_housekeeper(log_dir: PathBuf, retention_days: u64) -> LogHousekeeper {
    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_stop = Arc::clone(&stop);
    let thread = std::thread::spawn(move || {
        let mut warned_large_files = HashSet::new();
        loop {
            maintain_log_dir(
                &log_dir,
                retention_days,
                SystemTime::now(),
                &mut warned_large_files,
            );
            let (lock, wake) = &*thread_stop;
            let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let (stopped, _) = wake
                .wait_timeout_while(stopped, LOG_HOUSEKEEPING_INTERVAL, |stopped| !*stopped)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *stopped {
                break;
            }
        }
    });
    LogHousekeeper {
        stop,
        thread: Some(thread),
    }
}

impl ProcessTag {
    fn prefix(self) -> &'static str {
        match self {
            ProcessTag::App => "cairn-app",
            ProcessTag::Cmd => "cairn-cmd",
            ProcessTag::Executor => "cairn-executor",
            ProcessTag::Server => "cairn-server",
            ProcessTag::Runner => "cairn-runner",
        }
    }
}

/// Configuration for logging initialization.
pub struct LogConfig {
    /// Which binary is running (determines log file prefix).
    pub process: ProcessTag,
    /// Log directory. Defaults to `~/.cairn/logs/`.
    pub log_dir: Option<PathBuf>,
    /// Enable pretty stderr layer. Typically true for dev/terminal, false for GUI app.
    pub stderr: bool,
    /// File-log verbosity level. Lower priority than the `CAIRN_FILE_LOG` and
    /// `CAIRN_LOG_LEVEL` env channels; `None` falls back to `CAIRN_LOG_LEVEL`
    /// then the `Standard` default.
    pub level: Option<LogLevel>,
    /// Cap for the pretty stderr layer, independent of the file layer's `level`
    /// and still overridable by `RUST_LOG`. `None` keeps the historical `info`
    /// default. The installed runner service sets this to `Quiet` (warn-only):
    /// launchd redirects its stderr into an unrotated `runner.err.log`, so
    /// mirroring the full INFO stream there grew it without bound — the rotated
    /// JSONL file layer keeps the full stream instead.
    pub stderr_level: Option<LogLevel>,
    /// Number of days JSONL files remain in the shared log directory. `None`
    /// leaves shared-directory housekeeping to a settings-aware owner process.
    pub retention_days: Option<u64>,
}

pub const DEFAULT_LOG_RETENTION_DAYS: u64 = 7;
const LOG_SIZE_WARNING_BYTES: u64 = 1024 * 1024 * 1024;
const LOG_HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// File-log verbosity level. Each level maps to a concrete `EnvFilter` directive
/// string; the names are the stable contract shared with the `logLevel` setting
/// and the `CAIRN_LOG_LEVEL` env channel. `cairn-common` owns only the
/// name-to-directive map, never how a level was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogLevel {
    /// Errors and warnings only.
    Quiet,
    /// Errors, warnings, and operational `info` diagnostics — no crate `debug`,
    /// no profiler. The shipped default.
    #[default]
    Standard,
    /// Full crate `debug` plus profiler events — the current verbose behavior,
    /// an opt-in for local development.
    Verbose,
}

impl LogLevel {
    /// The `EnvFilter` directive string this level resolves to.
    fn directives(self) -> &'static str {
        match self {
            LogLevel::Quiet => "warn,profiler=off",
            LogLevel::Standard => "info,profiler=off",
            LogLevel::Verbose => {
                "info,cairn_lib=debug,cairn_core=debug,cairn_cmd=debug,profiler=info"
            }
        }
    }

    /// The stable level name (matching the serde representation), used for the
    /// `CAIRN_LOG_LEVEL` env channel passed to child processes.
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Quiet => "quiet",
            LogLevel::Standard => "standard",
            LogLevel::Verbose => "verbose",
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "quiet" => Ok(LogLevel::Quiet),
            "standard" => Ok(LogLevel::Standard),
            "verbose" => Ok(LogLevel::Verbose),
            _ => Err(()),
        }
    }
}

/// Holds the async writer guard. **Must be kept alive** for the duration of the
/// process — dropping it flushes and stops the background writer thread.
///
/// Also carries whether the file layer is live at all: [`init`] degrades to a
/// fileless logger when the log destination cannot be written, and a caller that
/// reports log state (the app's log viewer) needs to say so rather than claim
/// files exist.
pub struct LogGuard {
    /// `None` when the file layer degraded away and there is no writer to flush.
    _worker: Option<WorkerGuard>,
    /// Why the file layer is absent; `None` when it is live.
    file_error: Option<String>,
    _housekeeper: Option<LogHousekeeper>,
}

struct LogHousekeeper {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for LogHousekeeper {
    fn drop(&mut self) {
        let (lock, wake) = &*self.stop;
        *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_one();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl LogGuard {
    /// Whether the JSONL file layer is live.
    pub fn file_logging_enabled(&self) -> bool {
        self.file_error.is_none()
    }

    /// Why the JSONL file layer is absent, when it is.
    pub fn file_error(&self) -> Option<&str> {
        self.file_error.as_deref()
    }
}

/// Default log directory, resolved by the shared paths resolver (dev/prod
/// separated; `CAIRN_LOG_DIR` override honored).
fn default_log_dir() -> PathBuf {
    crate::paths::cairn_log_dir()
}

fn default_stderr_filter() -> EnvFilter {
    EnvFilter::new("info").add_directive("profiler=off".parse().expect("valid profiler directive"))
}

/// Resolve the file-layer filter directives, in priority order:
/// 1. `CAIRN_FILE_LOG` — a raw `EnvFilter` directive string (power-user escape hatch).
/// 2. `CAIRN_LOG_LEVEL` — a named level, the channel for spawned child processes.
/// 3. The in-process `LogConfig.level`.
/// 4. The `Standard` default.
///
/// Returned as the directive string so both the file layer's `EnvFilter` and the
/// span-duration layer's profiler gate ([`profiler_span_filter`]) derive from one
/// resolution. A `CAIRN_FILE_LOG` value that fails to parse is ignored and
/// resolution falls through to the named-level path.
fn resolve_file_directives(level: Option<LogLevel>) -> String {
    if let Ok(value) = std::env::var("CAIRN_FILE_LOG") {
        let trimmed = value.trim();
        if !trimmed.is_empty() && trimmed.parse::<EnvFilter>().is_ok() {
            return value;
        }
    }

    let resolved = std::env::var("CAIRN_LOG_LEVEL")
        .ok()
        .and_then(|v| v.parse::<LogLevel>().ok())
        .or(level)
        .unwrap_or_default();
    resolved.directives().to_string()
}

/// Resolve the pretty stderr-layer filter. `RUST_LOG` (the power-user escape
/// hatch) always wins; otherwise a caller-supplied `stderr_level` caps the layer
/// (the installed runner service passes `Quiet`), falling back to the historical
/// `info` default when unset.
fn resolve_stderr_filter(stderr_level: Option<LogLevel>) -> EnvFilter {
    if let Ok(value) = std::env::var("RUST_LOG") {
        if !value.trim().is_empty() {
            if let Ok(filter) = value.parse::<EnvFilter>() {
                return filter;
            }
        }
    }
    match stderr_level {
        Some(level) => EnvFilter::new(level.directives()),
        None => default_stderr_filter(),
    }
}

/// The tracing target that marks a span (and event) as a profiler duration
/// sample. Matches the `target: "profiler"` convention of the existing
/// `tracing::info!(target: "profiler", ...)` emit sites and is gated by the
/// `LogLevel` directives (`profiler=off` at quiet/standard, `profiler=info` at
/// verbose).
const PROFILER_TARGET: &str = "profiler";

/// Per-span state the [`SpanDurationLayer`] stores in span extensions: the open
/// instant plus any span fields, which ride out as the emitted event's `meta`.
struct SpanTiming {
    start: Instant,
    fields: serde_json::Map<String, serde_json::Value>,
}

/// Collects span fields into a JSON object for the profiler event's `meta`.
#[derive(Default)]
struct FieldVisitor(serde_json::Map<String, serde_json::Value>);

impl Visit for FieldVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_string(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_string(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_string(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}").into());
    }
}

/// A `tracing` layer that turns any `profiler`-target span into one
/// profiler-schema duration event on close. Instrumenting a unit of backend work
/// is then one line at the work's own call site:
///
/// ```ignore
/// use tracing::Instrument;
/// do_batch().instrument(tracing::info_span!(target: "profiler", "embed_batch")).await;
/// ```
///
/// The layer is filtered by [`profiler_span_filter`] so that when the profiler
/// target is off (the shipped default) its callbacks never fire — the tracing
/// callsite cache short-circuits span creation, giving effectively zero overhead.
struct SpanDurationLayer;

impl<S> Layer<S> for SpanDurationLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        // Defensive: the layer's filter already restricts callbacks to the
        // profiler target, but only time spans that actually carry it so a
        // broader reuse of this layer never mis-times unrelated spans.
        if span.metadata().target() != PROFILER_TARGET {
            return;
        }
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        span.extensions_mut().insert(SpanTiming {
            start: Instant::now(),
            fields: visitor.0,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(timing) = ext.get_mut::<SpanTiming>() {
            let mut visitor = FieldVisitor(std::mem::take(&mut timing.fields));
            values.record(&mut visitor);
            timing.fields = visitor.0;
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let timing = span.extensions_mut().remove::<SpanTiming>();
        if let Some(SpanTiming { start, fields }) = timing {
            emit_span_profile(span.name(), start.elapsed().as_secs_f64() * 1000.0, fields);
        }
    }
}

/// Emit one profiler-schema event for a closed span. The shape matches the
/// existing backend profiler emit (`src-tauri/src/commands/blocking.rs`) and the
/// consumer parser (`scripts/profiler.ts`): the event target is `profiler` and
/// its message is a JSON payload `{v, source, kind, name, durationMs, status,
/// meta}`. The event timestamp is supplied by the JSON file layer (`timestamp`),
/// which `scripts/profiler.ts` reads before any payload `ts`, so no wall-clock
/// dependency is needed here.
fn emit_span_profile(
    name: &str,
    duration_ms: f64,
    mut fields: serde_json::Map<String, serde_json::Value>,
) {
    // A `status` string field is promoted to the top-level status (letting a
    // call site record "error"); everything else stays in `meta`.
    let status = match fields.remove("status") {
        Some(serde_json::Value::String(s)) => s,
        Some(other) => {
            fields.insert("status".to_string(), other);
            "ok".to_string()
        }
        None => "ok".to_string(),
    };
    let payload = serde_json::json!({
        "v": 1,
        "source": "backend",
        "kind": "backend-span",
        "name": name,
        "durationMs": (duration_ms * 100.0).round() / 100.0,
        "status": status,
        "meta": fields,
    });
    tracing::info!(target: PROFILER_TARGET, "{}", payload);
}

/// Build the [`SpanDurationLayer`]'s filter from the resolved file-layer
/// directives so the layer tracks the exact same profiler on/off decision as the
/// JSONL file layer that must record its emitted events. When profiler is off the
/// returned `Targets` enables nothing, so the layer's callbacks never fire.
fn profiler_span_filter(directives: &str) -> Targets {
    let enabled = directives
        .parse::<Targets>()
        .map(|targets| targets.would_enable(PROFILER_TARGET, &Level::INFO))
        .unwrap_or(false);
    if enabled {
        Targets::new().with_target(PROFILER_TARGET, LevelFilter::INFO)
    } else {
        Targets::new()
    }
}

/// Build the rotating JSONL appender for `log_dir`, creating the directory if it
/// is missing.
///
/// A missing directory and an unopenable file are the same fact to a caller — no
/// writable log destination — so both collapse into one message naming the path,
/// which is the part a person needs to act on.
fn build_file_appender(
    log_dir: &Path,
    process: ProcessTag,
) -> Result<tracing_appender::rolling::RollingFileAppender, String> {
    std::fs::create_dir_all(log_dir)
        .map_err(|error| format!("create log directory {}: {error}", log_dir.display()))?;

    tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(process.prefix())
        .filename_suffix("jsonl")
        .build(log_dir)
        .map_err(|error| format!("open a log file in {}: {error}", log_dir.display()))
}

/// A [`MakeWriter`] that scrubs registered credentials out of every formatted
/// log record on its way to the sink beneath it.
///
/// # Why the sink rather than the call sites
///
/// Process output is scrubbed at the *pipe that produces it*, because one stream
/// has many readers and scrubbing per-reader is how they fork into a raw copy
/// and a redacted one. Logging inverts that shape: thousands of call sites feed
/// one writer. So the narrow waist is the writer, and putting the seam here is
/// what makes it unforgettable — a new `log::warn!` anywhere in any binary, a
/// panic hook, an error string from a dependency, and a diagnostic that has not
/// been written yet are all covered without anyone remembering to opt in.
///
/// # Why a whole record at a time
///
/// `fmt::Layer` formats an event into a thread-local buffer and issues exactly
/// one `write_all` with the complete record. A log writer therefore never sees a
/// credential split across two calls, which is why this uses the one-shot
/// [`RegistrySnapshot::scrub_bytes`] rather than a [`StreamingScrubber`]: there
/// is no boundary to carry across, and a carry would hold back log lines.
///
/// # Why it is silent
///
/// Every other crossing reports what it matched through a `DetectionReport`.
/// This one deliberately does not, because a detection is reported *by logging
/// it*, and a log emitted from inside the log writer re-enters this same writer
/// forever. The operator signal for a registered credential reaching output is
/// raised at the crossing that produced the value, which is where the useful
/// context (which process, which run) lives anyway.
///
/// # Why the JSON sink is scrubbed structurally
///
/// A record is already formatted when it reaches a writer, so a byte scan matches
/// a value only as it was *rendered*. JSON escapes `"`, `\`, and control bytes,
/// so a registered credential containing any of those reaches the file as a
/// reversible spelling like `abc\"def` that the raw needles do not match — and
/// since the broker registers operator- and provider-supplied credentials, whose
/// bytes nothing constrains, that is a live gap rather than a theoretical one.
///
/// So the JSON layer's records are parsed, scrubbed as decoded string values, and
/// re-serialized. Decoding is what makes the match escape-independent. A record
/// that does not parse falls back to the byte scan rather than passing through,
/// and a record with nothing to redact is written back byte-identical, so the
/// common path costs one parse and no rewrite.
///
/// The text layer keeps the byte scan: its records are rendered by `Display` for
/// the message and by `Debug` only for explicitly structured fields, so a
/// credential arriving through a message is matched literally.
#[derive(Clone, Copy)]
enum RecordFormat {
    /// One JSON object per record: scrub decoded string values.
    Json,
    /// Free-form rendered text: scrub the record's bytes.
    Text,
}

struct ScrubbedWriter<W> {
    inner: W,
    format: RecordFormat,
}

impl<W> ScrubbedWriter<W> {
    fn json(inner: W) -> Self {
        Self {
            inner,
            format: RecordFormat::Json,
        }
    }

    fn text(inner: W) -> Self {
        Self {
            inner,
            format: RecordFormat::Text,
        }
    }
}

/// Scrub one JSON record by its decoded values, returning `None` when nothing
/// matched so the caller can write the original bytes untouched.
fn scrub_json_record(
    buf: &[u8],
    snapshot: &crate::security::RegistrySnapshot,
    found: &mut crate::security::Detections,
) -> Option<Vec<u8>> {
    let terminator = buf.ends_with(b"\n");
    let body = if terminator {
        &buf[..buf.len() - 1]
    } else {
        buf
    };
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(body) else {
        // Not a JSON record after all — a formatter error line, or a layer whose
        // shape changed. Fall back to the byte scan rather than letting it past.
        return snapshot.scrub_bytes(buf, found);
    };
    if !scrub_json_value(&mut value, snapshot, found) {
        return None;
    }
    let mut out = serde_json::to_vec(&value).ok()?;
    if terminator {
        out.push(b'\n');
    }
    Some(out)
}

/// Scrub every string in `value` in place. Returns whether anything changed.
fn scrub_json_value(
    value: &mut serde_json::Value,
    snapshot: &crate::security::RegistrySnapshot,
    found: &mut crate::security::Detections,
) -> bool {
    match value {
        serde_json::Value::String(text) => match snapshot.scrub_bytes(text.as_bytes(), found) {
            Some(scrubbed) => {
                *text = String::from_utf8_lossy(&scrubbed).into_owned();
                true
            }
            None => false,
        },
        // Written as loops rather than `any`/`fold` deliberately. `any` reads as
        // the obvious spelling and clippy will suggest it, but it short-circuits:
        // it stops descending at the first value that matched, leaving every
        // later field of the record unscrubbed. Every element must be visited.
        serde_json::Value::Array(items) => {
            let mut changed = false;
            for item in items.iter_mut() {
                if scrub_json_value(item, snapshot, found) {
                    changed = true;
                }
            }
            changed
        }
        serde_json::Value::Object(fields) => {
            let mut changed = false;
            for (_, field) in fields.iter_mut() {
                if scrub_json_value(field, snapshot, found) {
                    changed = true;
                }
            }
            changed
        }
        _ => false,
    }
}

impl<'writer, W: fmt::MakeWriter<'writer>> fmt::MakeWriter<'writer> for ScrubbedWriter<W> {
    type Writer = ScrubbedSink<W::Writer>;

    fn make_writer(&'writer self) -> Self::Writer {
        ScrubbedSink {
            inner: self.inner.make_writer(),
            format: self.format,
        }
    }

    fn make_writer_for(&'writer self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        ScrubbedSink {
            inner: self.inner.make_writer_for(meta),
            format: self.format,
        }
    }
}

/// The per-record sink half of [`ScrubbedWriter`].
struct ScrubbedSink<W> {
    inner: W,
    format: RecordFormat,
}

impl<W: std::io::Write> std::io::Write for ScrubbedSink<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let snapshot = crate::security::registry::registry().snapshot();
        if snapshot.is_empty() {
            // Nothing registered: logging must not pay for a scan that cannot
            // match, and this is the steady state for a process that resolves no
            // credentials at all.
            return self.inner.write(buf);
        }
        let mut found = crate::security::Detections::new();
        let scrubbed = match self.format {
            RecordFormat::Json => scrub_json_record(buf, &snapshot, &mut found),
            RecordFormat::Text => snapshot.scrub_bytes(buf, &mut found),
        };
        match scrubbed {
            // Redaction changes the record's length, so the count returned to the
            // caller must describe *its* buffer rather than ours: `write_all`
            // advances through the original bytes and would otherwise re-emit a
            // tail of the unscrubbed record.
            Some(scrubbed) => {
                self.inner.write_all(&scrubbed)?;
                Ok(buf.len())
            }
            None => self.inner.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Initialize the unified logging subscriber.
///
/// Returns a `LogGuard` that must be stored (not dropped) for the lifetime of the
/// process. Dropping it flushes pending log writes.
///
/// A log destination the process cannot write is **not** a failure: the file
/// layer is dropped, the remaining layers are installed, and the reason is
/// recorded on the returned [`LogGuard`]. A Cairn binary has to run wherever it
/// is launched, and the `run` verb's OS fence proved the point — it confines
/// writes to the worktree and the sanctioned scratch dirs, so the home log
/// directory is unwritable there and every `cairn` CLI invocation from an agent
/// batch shell died at this call before it parsed its command. A read-only home,
/// a container, or any fence yet to be written would do the same.
///
/// # Errors
/// Only if a global subscriber was already installed — a double-init bug in the
/// calling binary, which is worth failing on because it means two subscribers
/// disagree about where this process logs.
pub fn init(config: LogConfig) -> Result<LogGuard, Box<dyn std::error::Error>> {
    let log_dir = config.log_dir.unwrap_or_else(default_log_dir);
    let retention_days = config.retention_days.map(|days| days.max(1));

    // File layer: JSON Lines with daily rotation and age-based housekeeping — or nothing at
    // all, when this process cannot write there.
    let (file_layer, worker, file_error, housekeeper) =
        match build_file_appender(&log_dir, config.process) {
            Ok(appender) => {
                let (non_blocking, worker) = tracing_appender::non_blocking(appender);
                let layer = fmt::layer()
                    .json()
                    .with_writer(ScrubbedWriter::json(non_blocking))
                    .with_target(true)
                    .with_level(true)
                    .with_thread_ids(false)
                    .with_thread_names(false);
                (
                    Some(layer),
                    Some(worker),
                    None,
                    retention_days.map(|days| spawn_log_housekeeper(log_dir.clone(), days)),
                )
            }
            Err(error) => (None, None, Some(error), None),
        };

    // File layer filter: resolved from CAIRN_FILE_LOG / CAIRN_LOG_LEVEL / the
    // configured level, defaulting to the light `Standard` filter (no crate
    // debug, no profiler) so normal installs stay quiet unless opted in.
    let file_directives = resolve_file_directives(config.level);
    let file_filter = EnvFilter::new(&file_directives);

    // Span-duration profiler layer: emits one profiler-schema duration event per
    // closed `profiler`-target span (see `SpanDurationLayer`). Its filter is
    // derived from the same resolved directives that gate the file layer, so the
    // profiler is on for exactly the configurations that asked for it — and inert
    // (zero overhead) otherwise. It tracks the configured level rather than
    // whether a file layer survived, so a degraded logger still profiles into
    // whatever layers remain.
    let span_layer = SpanDurationLayer.with_filter(profiler_span_filter(&file_directives));

    // Build the subscriber. `Option<Layer>` is itself a `Layer`, so a degraded
    // file layer simply contributes nothing.
    let registry = tracing_subscriber::registry()
        .with(file_layer.map(|layer| layer.with_filter(file_filter)))
        .with(span_layer);

    if config.stderr {
        // Stderr layer: pretty, ANSI when TTY, respects RUST_LOG, capped by any
        // caller-supplied `stderr_level`.
        let stderr_filter = resolve_stderr_filter(config.stderr_level);

        // Scrubbed for the same reason the file layer is, and not only for the
        // operator reading a console: the installed runner service has launchd
        // redirect its stderr into `runner.err.log`, so this layer is a durable
        // sink too.
        let stderr_layer = fmt::layer()
            .with_writer(ScrubbedWriter::text(std::io::stderr))
            .with_target(true)
            .with_ansi(atty_stderr());

        registry
            .with(stderr_layer.with_filter(stderr_filter))
            .try_init()?;
    } else {
        registry.try_init()?;
    }

    // Bridge log:: crate into tracing (ignore if already set)
    let _ = tracing_log::LogTracer::init();

    // Announce a degrade through the subscriber that was just installed, so a
    // binary with a stderr layer says why its files stopped appearing. A CLI
    // subcommand runs with `stderr: false` to keep its output pipeable, and this
    // is deliberately dropped there rather than special-cased into a print.
    if let Some(error) = &file_error {
        tracing::warn!(
            %error,
            "file logging disabled; continuing without a log file"
        );
    }

    Ok(LogGuard {
        _worker: worker,
        file_error,
        _housekeeper: housekeeper,
    })
}

/// Check if stderr is a TTY (for ANSI color support).
fn atty_stderr() -> bool {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::FileTimes;
    use std::str::FromStr;

    #[test]
    fn level_directives_are_stable() {
        assert_eq!(LogLevel::Quiet.directives(), "warn,profiler=off");
        assert_eq!(LogLevel::Standard.directives(), "info,profiler=off");
        assert_eq!(
            LogLevel::Verbose.directives(),
            "info,cairn_lib=debug,cairn_core=debug,cairn_cmd=debug,profiler=info"
        );
    }

    #[test]
    fn default_level_is_standard() {
        assert_eq!(LogLevel::default(), LogLevel::Standard);
    }

    #[test]
    fn level_name_parse_roundtrip() {
        for level in [LogLevel::Quiet, LogLevel::Standard, LogLevel::Verbose] {
            assert_eq!(LogLevel::from_str(level.as_str()), Ok(level));
        }
        assert_eq!(LogLevel::from_str("STANDARD"), Ok(LogLevel::Standard));
        assert!(LogLevel::from_str("bogus").is_err());
    }

    // Single test owns the `CAIRN_FILE_LOG` / `CAIRN_LOG_LEVEL` env vars so it
    // does not race other tests that read them in parallel.
    #[test]
    fn resolve_file_directives_precedence() {
        std::env::remove_var("CAIRN_FILE_LOG");
        std::env::remove_var("CAIRN_LOG_LEVEL");

        // 4. Default → standard (light, no profiler/debug).
        assert_eq!(
            resolve_file_directives(None),
            LogLevel::Standard.directives()
        );

        // 3. LogConfig.level.
        assert_eq!(
            resolve_file_directives(Some(LogLevel::Quiet)),
            LogLevel::Quiet.directives()
        );

        // 2. CAIRN_LOG_LEVEL beats LogConfig.level.
        std::env::set_var("CAIRN_LOG_LEVEL", "verbose");
        assert_eq!(
            resolve_file_directives(Some(LogLevel::Quiet)),
            LogLevel::Verbose.directives()
        );

        // 1. CAIRN_FILE_LOG (raw directive) beats CAIRN_LOG_LEVEL.
        std::env::set_var("CAIRN_FILE_LOG", "warn,cairn_core=trace");
        assert_eq!(
            resolve_file_directives(Some(LogLevel::Quiet)),
            "warn,cairn_core=trace"
        );

        std::env::remove_var("CAIRN_FILE_LOG");
        std::env::remove_var("CAIRN_LOG_LEVEL");
    }

    /// A path whose parent is a regular file: `create_dir_all` refuses it for
    /// every user, root included, so the "no writable log destination" case is
    /// reproducible without depending on permissions or on a fence being active.
    fn unwritable_log_dir(dir: &tempfile::TempDir) -> PathBuf {
        let blocker = dir.path().join("not-a-directory");
        std::fs::write(&blocker, b"").expect("seed the blocking file");
        blocker.join("logs")
    }

    #[test]
    fn file_appender_creates_a_missing_log_dir() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("deeply/nested/logs");

        build_file_appender(&nested, ProcessTag::Cmd).expect("a writable path must open");

        assert!(nested.is_dir(), "the log directory is created on demand");
    }

    #[test]
    fn file_appender_reports_an_unwritable_log_dir_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let target = unwritable_log_dir(&dir);

        let error = build_file_appender(&target, ProcessTag::Cmd)
            .expect_err("a log dir that cannot be created must report, not panic");

        assert!(
            error.contains(&target.display().to_string()),
            "the failure must name the path a person has to fix: {error}"
        );
    }

    #[test]
    fn housekeeping_prunes_each_expired_log_by_age_and_leaves_other_files() {
        let dir = tempfile::tempdir().unwrap();
        let old_log = dir.path().join("cairn-runner.2026-07-01.jsonl");
        let fresh_log = dir.path().join("cairn-app.2026-08-05.jsonl");
        let unrelated = dir.path().join("notes.jsonl");
        std::fs::write(&old_log, "old").unwrap();
        std::fs::write(&fresh_log, "fresh").unwrap();
        std::fs::write(&unrelated, "keep").unwrap();
        let now = SystemTime::now();
        let old_time = now - Duration::from_secs(8 * 24 * 60 * 60);
        std::fs::File::options()
            .write(true)
            .open(&old_log)
            .unwrap()
            .set_times(FileTimes::new().set_modified(old_time))
            .unwrap();

        maintain_log_dir(dir.path(), 7, now, &mut HashSet::new());

        assert!(!old_log.exists());
        assert!(fresh_log.exists());
        assert!(unrelated.exists());
    }

    #[test]
    fn housekeeping_marks_a_large_daily_file_for_one_warning() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("cairn-runner.2026-08-05.jsonl");
        std::fs::File::create(&log)
            .unwrap()
            .set_len(LOG_SIZE_WARNING_BYTES)
            .unwrap();
        let mut warned = HashSet::new();

        maintain_log_dir(dir.path(), 7, SystemTime::now(), &mut warned);
        maintain_log_dir(dir.path(), 7, SystemTime::now(), &mut warned);

        assert_eq!(warned, HashSet::from([log]));
    }

    /// The regression this pins: a `cairn` CLI invocation inside the `run`
    /// fence cannot write the home log directory, and it used to die at
    /// `logging::init` before parsing its command. Init must hand back a working
    /// (fileless) logger instead of an error every caller has to `expect`-crash
    /// on.
    ///
    /// Sole `init` caller in this crate's test binary — it installs the global
    /// subscriber, which only one test can do.
    #[test]
    fn init_degrades_to_a_fileless_logger_when_the_log_dir_is_unwritable() {
        let dir = tempfile::tempdir().unwrap();
        let target = unwritable_log_dir(&dir);

        let guard = init(LogConfig {
            process: ProcessTag::Cmd,
            log_dir: Some(target.clone()),
            // The CLI's own configuration: stderr stays clean for piping, so the
            // degraded subscriber carries no layers at all.
            stderr: false,
            level: None,
            stderr_level: None,
            retention_days: None,
        })
        .expect("an unwritable log destination must not fail logging init");

        assert!(
            !guard.file_logging_enabled(),
            "the file layer must be reported as absent"
        );
        assert!(guard
            .file_error()
            .is_some_and(|error| error.contains(&target.display().to_string())));
        // The installed subscriber accepts events rather than panicking on them.
        tracing::info!("degraded logger still accepts events");
        tracing::warn!(target: PROFILER_TARGET, "and filtered targets too");
    }

    // The profiler gate derives from the same directive resolution: on at
    // verbose, off at the shipped quiet/standard defaults.
    #[test]
    fn profiler_span_filter_tracks_profiler_directive() {
        assert!(profiler_span_filter(LogLevel::Verbose.directives())
            .would_enable(PROFILER_TARGET, &Level::INFO));
        assert!(!profiler_span_filter(LogLevel::Standard.directives())
            .would_enable(PROFILER_TARGET, &Level::INFO));
        assert!(!profiler_span_filter(LogLevel::Quiet.directives())
            .would_enable(PROFILER_TARGET, &Level::INFO));
        // A raw filter with no profiler directive leaves the layer inert.
        assert!(!profiler_span_filter("warn,cairn_core=trace")
            .would_enable(PROFILER_TARGET, &Level::INFO));
    }
}

/// The log sink is the crossing these exercise, so each drives a real formatted
/// record through [`ScrubbedWriter`] rather than calling the scrubber directly.
///
/// The shapes are chosen against a known trap: CAIRN-3822's scrubber shipped a
/// defect that survived its whole suite because every test used *one* credential,
/// in *one* form, appearing *once* — a combination that cannot fail a scan loop
/// however the loop is written. So these register two values at different
/// offsets with the shorter one earlier, and pin a single credential appearing
/// in two derived forms in one record.
#[cfg(test)]
mod scrubbed_writer_tests {
    use super::*;
    use crate::security::registry::registry;
    use crate::security::{SecretCategory, SecretId, SecretMaterial};
    use base64::Engine as _;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    /// Collects everything the writer let through, as the sink beneath it sees it.
    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl Sink {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> fmt::MakeWriter<'writer> for Sink {
        type Writer = Sink;
        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emit `message` through a JSON subscriber whose writer is the scrubbed one,
    /// and return what actually landed in the sink.
    fn emit_json_record(message: &str) -> String {
        let sink = Sink::default();
        let layer = fmt::layer()
            .json()
            .with_writer(ScrubbedWriter::json(sink.clone()));
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!("{}", message);
        });
        sink.text()
    }

    fn register(id: &str, value: &str) -> crate::security::SecretGuard<'static> {
        registry()
            .register(
                SecretId::new(id),
                SecretCategory::CallbackCredential,
                "logging-test",
                SecretMaterial::from_string(value.to_string()),
            )
            .expect("test credential must clear the registration threshold")
    }

    /// Two distinct registered values in one record, the *shorter* one earlier.
    ///
    /// This is the shape that caught the original needle-major scan: taking the
    /// first needle that matches anywhere copies everything before it verbatim,
    /// so the shorter value sitting earlier goes out in the clear while the test
    /// still sees the longer one redacted.
    #[test]
    fn two_registered_values_are_both_scrubbed_shorter_one_first() {
        let shorter = "shorter-credential-aaaaaaaaaaaa";
        let longer = "longer-credential-bbbbbbbbbbbbbbbbbbbbbbbb";
        let _short_guard = register("log-shorter", shorter);
        let _long_guard = register("log-longer", longer);

        let output = emit_json_record(&format!("child said {shorter} then {longer} and stopped"));

        assert!(
            !output.contains(shorter),
            "the shorter value sitting earlier in the record leaked: {output}"
        );
        assert!(
            !output.contains(longer),
            "the longer value leaked: {output}"
        );
        assert!(
            output.contains(crate::security::REDACTED),
            "the record should carry the redaction marker: {output}"
        );
        // The surrounding record survives: this is a scrub, not a drop.
        assert!(output.contains("child said"), "record body lost: {output}");
        assert!(output.contains("and stopped"), "record tail lost: {output}");
    }

    /// One credential, two derived forms, in one record. The base64 form is
    /// always longer than the raw form, so this alone is enough to expose a scan
    /// that resolves ties by needle order rather than by position.
    #[test]
    fn one_credential_is_scrubbed_in_both_raw_and_base64_form() {
        let raw = "credential-with-two-forms-cccccccc";
        let _guard = register("log-two-forms", raw);
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);

        let output = emit_json_record(&format!("raw={raw} encoded={encoded}"));

        assert!(!output.contains(raw), "raw form leaked: {output}");
        assert!(!output.contains(&encoded), "base64 form leaked: {output}");
    }

    /// The writer must report the caller's buffer length, not its own. Redaction
    /// shortens the record, and a short count sends `write_all` back to re-emit
    /// the tail of the *unscrubbed* buffer — which would append the credential to
    /// the sink right after redacting it.
    #[test]
    fn a_shortened_record_does_not_leave_write_all_re_emitting_the_original() {
        let value = "tail-re-emission-credential-dddddddd";
        let _guard = register("log-write-all", value);
        let sink = Sink::default();
        let mut writer = ScrubbedSink {
            inner: sink.clone(),
            format: RecordFormat::Text,
        };
        let record = format!("before {value} after\n");

        writer
            .write_all(record.as_bytes())
            .expect("the scrubbed sink accepts the record");

        let output = sink.text();
        assert!(!output.contains(value), "credential re-emitted: {output}");
        assert_eq!(
            output,
            format!("before {} after\n", crate::security::REDACTED),
            "the record should appear exactly once, scrubbed"
        );
    }

    /// A credential containing bytes JSON escapes must not reach the file in its
    /// escaped spelling.
    ///
    /// This is the case a byte scan over the formatted record cannot see: the
    /// value is written as `abc\"def\\ghi…`, which is trivially reversible by
    /// anyone reading the log and matches none of the registered forms. Nothing
    /// constrains the bytes of an operator- or provider-supplied credential, so
    /// this is reachable rather than theoretical.
    #[test]
    fn a_credential_containing_json_escapes_is_scrubbed_in_the_file_record() {
        let value = "quote\"back\\slash-credential-hhhh";
        let _guard = register("log-json-escapes", value);
        // How serde spells it inside the record, which is what a raw scan of the
        // formatted bytes would have to match and does not.
        let escaped = serde_json::to_string(value).expect("a string serializes");
        let escaped_body = escaped.trim_matches('"').to_string();

        let output = emit_json_record(&format!("child echoed {value}"));

        assert!(
            !output.contains(&escaped_body),
            "the escaped spelling reached the record: {output}"
        );
        assert!(!output.contains(value), "the raw spelling leaked: {output}");
        assert!(
            output.contains(crate::security::REDACTED),
            "the record should carry the redaction marker: {output}"
        );
        // Still a valid JSON record after the rewrite, or the log is unparseable.
        let parsed: serde_json::Value =
            serde_json::from_str(output.trim()).expect("the rewritten record is still JSON");
        assert_eq!(parsed["level"], "ERROR");
    }

    /// Two credentials in two different fields of one record must both go.
    ///
    /// The structural walk must visit every value, not stop at the first that
    /// matched. `any` and `fold(.. || ..)` both read naturally here and clippy
    /// actively suggests the short-circuiting one, so this pins the consequence:
    /// a second field would keep its credential.
    #[test]
    fn every_field_of_a_record_is_scrubbed_not_just_the_first() {
        let first = "first-field-credential-jjjjjjjj";
        let second = "second-field-credential-kkkkkkkk";
        let _first_guard = register("log-field-first", first);
        let _second_guard = register("log-field-second", second);

        let sink = Sink::default();
        let layer = fmt::layer()
            .json()
            .with_writer(ScrubbedWriter::json(sink.clone()));
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(alpha = %first, beta = %second, "two fields");
        });
        let output = sink.text();

        assert!(!output.contains(first), "first field leaked: {output}");
        assert!(!output.contains(second), "second field leaked: {output}");
    }

    /// A record with nothing to redact is written back byte-identical, so the
    /// common path never pays a rewrite and never reorders a record's fields.
    #[test]
    fn a_json_record_with_nothing_to_redact_is_written_unchanged() {
        let _guard = register("log-untouched", "unrelated-credential-iiiiiiii");

        let output = emit_json_record("an ordinary diagnostic");

        let parsed: serde_json::Value =
            serde_json::from_str(output.trim()).expect("the record is JSON");
        assert_eq!(parsed["fields"]["message"], "an ordinary diagnostic");
    }

    /// With nothing registered the writer is a pass-through: logging must not pay
    /// for a scan that cannot match, and must not alter records.
    #[test]
    fn an_empty_registry_passes_records_through_untouched() {
        let sink = Sink::default();
        let mut writer = ScrubbedSink {
            inner: sink.clone(),
            format: RecordFormat::Text,
        };
        // No guard is held here, so this asserts the behaviour of whatever the
        // registry holds rather than of an empty one; the pass-through property
        // that matters is that an unregistered string is never altered.
        writer.write_all(b"ordinary diagnostic line\n").unwrap();

        assert_eq!(sink.text(), "ordinary diagnostic line\n");
    }
}

#[cfg(test)]
mod span_duration_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Captures every dispatched event's (target, rendered message) so a test can
    /// assert what the span-duration layer emitted through the full subscriber
    /// stack — the reentrancy check the design calls for (a `tracing::info!` fired
    /// from inside another layer's `on_close`).
    #[derive(Clone, Default)]
    struct Capture {
        events: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = MessageVisitor(None);
            event.record(&mut visitor);
            if let Some(message) = visitor.0 {
                self.events
                    .lock()
                    .unwrap()
                    .push((event.metadata().target().to_string(), message));
            }
        }
    }

    struct MessageVisitor(Option<String>);
    impl Visit for MessageVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = Some(format!("{value:?}"));
            }
        }
    }

    fn profiler_events(capture: &Capture) -> Vec<serde_json::Value> {
        capture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(target, _)| target == PROFILER_TARGET)
            .map(|(_, message)| serde_json::from_str(message).expect("profiler payload is json"))
            .collect()
    }

    #[test]
    fn profiler_span_close_emits_one_backend_span_event() {
        let capture = Capture::default();
        let span_layer =
            SpanDurationLayer.with_filter(profiler_span_filter(LogLevel::Verbose.directives()));
        let subscriber = tracing_subscriber::registry()
            .with(span_layer)
            .with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: "profiler", "embed_batch", size = 7);
            let entered = span.enter();
            // A brief hold so the measured duration is a plausible positive value.
            std::thread::sleep(std::time::Duration::from_millis(2));
            drop(entered);
            drop(span);
        });

        let events = profiler_events(&capture);
        assert_eq!(events.len(), 1, "exactly one duration event per span close");
        let payload = &events[0];
        assert_eq!(payload["v"], 1);
        assert_eq!(payload["source"], "backend");
        assert_eq!(payload["kind"], "backend-span");
        assert_eq!(payload["name"], "embed_batch");
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["meta"]["size"], 7);
        let duration = payload["durationMs"]
            .as_f64()
            .expect("durationMs is a number");
        assert!(duration >= 0.0 && duration.is_finite());
    }

    #[test]
    fn status_field_is_promoted_out_of_meta() {
        let capture = Capture::default();
        let span_layer =
            SpanDurationLayer.with_filter(profiler_span_filter(LogLevel::Verbose.directives()));
        let subscriber = tracing_subscriber::registry()
            .with(span_layer)
            .with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: "profiler", "team_sync_push", status = "error");
            drop(span);
        });

        let events = profiler_events(&capture);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["status"], "error");
        assert!(events[0]["meta"].get("status").is_none());
    }

    #[test]
    fn profiler_off_emits_nothing() {
        let capture = Capture::default();
        // Standard directives → profiler=off → the layer's filter enables nothing.
        let span_layer =
            SpanDurationLayer.with_filter(profiler_span_filter(LogLevel::Standard.directives()));
        let subscriber = tracing_subscriber::registry()
            .with(span_layer)
            .with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: "profiler", "embed_batch");
            let entered = span.enter();
            drop(entered);
            drop(span);
        });

        assert!(
            capture.events.lock().unwrap().is_empty(),
            "no events when the profiler target is filtered off"
        );
    }

    #[test]
    fn non_profiler_span_is_ignored() {
        let capture = Capture::default();
        // Even with the filter broadened to every target, only profiler-target
        // spans are timed (the defensive guard in `on_new_span`).
        let span_layer =
            SpanDurationLayer.with_filter(Targets::new().with_default(LevelFilter::INFO));
        let subscriber = tracing_subscriber::registry()
            .with(span_layer)
            .with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("not_profiler");
            drop(span);
        });

        assert!(profiler_events(&capture).is_empty());
    }
}
