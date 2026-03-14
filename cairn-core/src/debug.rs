//! Shared debug logging utility
//!
//! Writes timestamped messages to debug.log in the app data directory.

use std::io::Write;
use std::sync::OnceLock;

static LOG_PATH: OnceLock<String> = OnceLock::new();

fn get_log_path() -> &'static str {
    LOG_PATH.get_or_init(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        format!(
            "{}/Library/Application Support/com.cairn.desktop/debug.log",
            home
        )
    })
}

/// Write a debug message to the log file with timestamp
pub fn debug_log(msg: &str) {
    let path = get_log_path();

    // Ensure directory exists
    if let Some(parent) = std::path::Path::new(path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "[{}] {}", chrono::Utc::now(), msg);
    }
}

/// Write a formatted debug message
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        $crate::debug::debug_log(&format!($($arg)*))
    };
}
