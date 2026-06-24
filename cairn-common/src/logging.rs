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
    /// File-log verbosity level. Lower priority than the `CAIRN_FILE_LOG` and
    /// `CAIRN_LOG_LEVEL` env channels; `None` falls back to `CAIRN_LOG_LEVEL`
    /// then the `Standard` default.
    pub level: Option<LogLevel>,
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
                "info,cairn_lib=debug,cairn_core=debug,cairn_cli=debug,profiler=info"
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

/// Resolve the file-layer filter, in priority order:
/// 1. `CAIRN_FILE_LOG` — a raw `EnvFilter` directive string (power-user escape hatch).
/// 2. `CAIRN_LOG_LEVEL` — a named level, the channel for spawned child processes.
/// 3. The in-process `LogConfig.level`.
/// 4. The `Standard` default.
///
/// A `CAIRN_FILE_LOG` value that fails to parse is ignored and resolution falls
/// through to the named-level path.
fn resolve_file_filter(level: Option<LogLevel>) -> EnvFilter {
    if let Ok(value) = std::env::var("CAIRN_FILE_LOG") {
        if !value.trim().is_empty() {
            if let Ok(filter) = value.parse::<EnvFilter>() {
                return filter;
            }
        }
    }

    let resolved = std::env::var("CAIRN_LOG_LEVEL")
        .ok()
        .and_then(|v| v.parse::<LogLevel>().ok())
        .or(level)
        .unwrap_or_default();
    EnvFilter::new(resolved.directives())
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
            "info,cairn_lib=debug,cairn_core=debug,cairn_cli=debug,profiler=info"
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
    fn resolve_file_filter_precedence() {
        std::env::remove_var("CAIRN_FILE_LOG");
        std::env::remove_var("CAIRN_LOG_LEVEL");

        // 4. Default → standard (light, no profiler/debug).
        assert_eq!(
            resolve_file_filter(None).to_string(),
            EnvFilter::new(LogLevel::Standard.directives()).to_string()
        );

        // 3. LogConfig.level.
        assert_eq!(
            resolve_file_filter(Some(LogLevel::Quiet)).to_string(),
            EnvFilter::new(LogLevel::Quiet.directives()).to_string()
        );

        // 2. CAIRN_LOG_LEVEL beats LogConfig.level.
        std::env::set_var("CAIRN_LOG_LEVEL", "verbose");
        assert_eq!(
            resolve_file_filter(Some(LogLevel::Quiet)).to_string(),
            EnvFilter::new(LogLevel::Verbose.directives()).to_string()
        );

        // 1. CAIRN_FILE_LOG (raw directive) beats CAIRN_LOG_LEVEL.
        std::env::set_var("CAIRN_FILE_LOG", "warn,cairn_core=trace");
        assert_eq!(
            resolve_file_filter(Some(LogLevel::Quiet)).to_string(),
            EnvFilter::new("warn,cairn_core=trace").to_string()
        );

        std::env::remove_var("CAIRN_FILE_LOG");
        std::env::remove_var("CAIRN_LOG_LEVEL");
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

    // File layer filter: resolved from CAIRN_FILE_LOG / CAIRN_LOG_LEVEL / the
    // configured level, defaulting to the light `Standard` filter (no crate
    // debug, no profiler) so normal installs stay quiet unless opted in.
    let file_filter = resolve_file_filter(config.level);

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
