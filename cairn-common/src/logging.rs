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

use std::path::PathBuf;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Identifies which binary is logging. Used in log file naming.
#[derive(Debug, Clone, Copy)]
pub enum ProcessTag {
    App,
    Mcp,
    Server,
}

impl ProcessTag {
    fn prefix(self) -> &'static str {
        match self {
            ProcessTag::App => "cairn-app",
            ProcessTag::Mcp => "cairn-mcp",
            ProcessTag::Server => "cairn-server",
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

fn default_file_filter_directives() -> &'static str {
    "info,cairn_lib=debug,cairn_core=debug,cairn_cli=debug,profiler=info"
}

fn default_file_filter() -> EnvFilter {
    EnvFilter::new(default_file_filter_directives())
}

fn file_filter_from_env() -> EnvFilter {
    match std::env::var("CAIRN_FILE_LOG") {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<EnvFilter>()
            .unwrap_or_else(|_| default_file_filter()),
        _ => default_file_filter(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn default_file_filter_suppresses_dependency_debug_noise() {
        assert_eq!(
            super::default_file_filter_directives(),
            "info,cairn_lib=debug,cairn_core=debug,cairn_cli=debug,profiler=info"
        );
    }
}

fn stderr_filter_from_env() -> EnvFilter {
    match std::env::var("RUST_LOG") {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<EnvFilter>()
            .unwrap_or_else(|_| default_stderr_filter()),
        _ => default_stderr_filter(),
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

    // File layer filter: keep app/profiler diagnostics while avoiding verbose dependency
    // DEBUG logs that can make the desktop app spend most of its time writing JSONL.
    let file_filter = file_filter_from_env();

    // Build the subscriber
    let registry = tracing_subscriber::registry().with(file_layer.with_filter(file_filter));

    if config.stderr {
        // Stderr layer: pretty, ANSI when TTY, respects RUST_LOG
        let stderr_filter = stderr_filter_from_env();

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
