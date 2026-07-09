//! Unified logging module for all Cairn binaries.
//!
//! Provides a dual-layer tracing subscriber:
//! - JSON Lines file layer (daily rotation, 14-day retention). The directory is
//!   resolved by `paths::cairn_log_dir`, which separates dev (`~/.cairn-dev/logs`)
//!   from prod (`~/.cairn/logs`) and honors the `CAIRN_LOG_DIR` override.
//! - Pretty stderr layer (ANSI when TTY, respects RUST_LOG)
//!
//! All `log::` crate calls are bridged into tracing via `tracing-log`.
//! Call `init()` once at startup in each binary.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Instant;
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
    Server,
    Runner,
}

impl ProcessTag {
    fn prefix(self) -> &'static str {
        match self {
            ProcessTag::App => "cairn-app",
            ProcessTag::Cmd => "cairn-cmd",
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
}

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
    pub fn directives(self) -> &'static str {
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
pub struct LogGuard(#[allow(dead_code)] WorkerGuard);

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

/// Initialize the unified logging subscriber.
///
/// Returns a `LogGuard` that must be stored (not dropped) for the lifetime of the
/// process. Dropping it flushes pending log writes.
///
/// # Errors
/// Returns an error if the subscriber cannot be initialized (e.g. a global subscriber
/// was already set).
pub fn init(config: LogConfig) -> Result<LogGuard, Box<dyn std::error::Error>> {
    let log_dir = config.log_dir.unwrap_or_else(default_log_dir);

    // Ensure log directory exists
    std::fs::create_dir_all(&log_dir)?;

    // File layer: JSON Lines with daily rotation, 14 file max
    let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(config.process.prefix())
        .filename_suffix("jsonl")
        .max_log_files(14)
        .build(&log_dir)?;

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .json()
        .with_writer(non_blocking)
        .with_target(true)
        .with_level(true)
        .with_thread_ids(false)
        .with_thread_names(false);

    // File layer filter: resolved from CAIRN_FILE_LOG / CAIRN_LOG_LEVEL / the
    // configured level, defaulting to the light `Standard` filter (no crate
    // debug, no profiler) so normal installs stay quiet unless opted in.
    let file_directives = resolve_file_directives(config.level);
    let file_filter = EnvFilter::new(&file_directives);

    // Span-duration profiler layer: emits one profiler-schema duration event per
    // closed `profiler`-target span (see `SpanDurationLayer`). Its filter is
    // derived from the same resolved directives, so it is active exactly when the
    // file layer will record its events — and inert (zero overhead) otherwise.
    let span_layer = SpanDurationLayer.with_filter(profiler_span_filter(&file_directives));

    // Build the subscriber
    let registry = tracing_subscriber::registry()
        .with(file_layer.with_filter(file_filter))
        .with(span_layer);

    if config.stderr {
        // Stderr layer: pretty, ANSI when TTY, respects RUST_LOG, capped by any
        // caller-supplied `stderr_level`.
        let stderr_filter = resolve_stderr_filter(config.stderr_level);

        let stderr_layer = fmt::layer()
            .with_writer(std::io::stderr)
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

    Ok(LogGuard(guard))
}

/// Check if stderr is a TTY (for ANSI color support).
fn atty_stderr() -> bool {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
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
