//! Logging startup reads only its own settings, before runtime configuration loads.
use cairn_common::logging::{LogLevel, DEFAULT_LOG_RETENTION_DAYS};
use serde::Deserialize;
use std::path::Path;

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoggingSettings {
    log_level: Option<LogLevel>,
    log_retention_days: Option<u64>,
}

fn read(config_dir: &Path) -> LoggingSettings {
    std::fs::read_to_string(config_dir.join("settings.yaml"))
        .ok()
        .and_then(|source| serde_yaml::from_str(&source).ok())
        .unwrap_or_default()
}

/// File-log verbosity, loaded once at process startup.
pub fn load_log_level(config_dir: &Path) -> LogLevel {
    read(config_dir).log_level.unwrap_or_default()
}

/// Retention defaults to seven days and clamps zero to one day.
pub fn load_log_retention_days(config_dir: &Path) -> u64 {
    read(config_dir)
        .log_retention_days
        .unwrap_or(DEFAULT_LOG_RETENTION_DAYS)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_malformed_logging_settings_use_defaults() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(load_log_level(home.path()), LogLevel::Standard);
        assert_eq!(
            load_log_retention_days(home.path()),
            DEFAULT_LOG_RETENTION_DAYS
        );
        for invalid in ["[invalid", "logLevel: invalid", "logRetentionDays: -1"] {
            std::fs::write(home.path().join("settings.yaml"), invalid).unwrap();
            assert_eq!(load_log_level(home.path()), LogLevel::Standard);
            assert_eq!(
                load_log_retention_days(home.path()),
                DEFAULT_LOG_RETENTION_DAYS
            );
        }
    }

    #[test]
    fn logging_is_independent_of_unrelated_runtime_settings() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(
            home.path().join("settings.yaml"),
            "logLevel: verbose\nlogRetentionDays: 3\nvoice: not-a-voice-config\n",
        )
        .unwrap();
        assert_eq!(load_log_level(home.path()), LogLevel::Verbose);
        assert_eq!(load_log_retention_days(home.path()), 3);
    }

    #[test]
    fn zero_retention_clamps_to_one() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join("settings.yaml"), "logRetentionDays: 0\n").unwrap();
        assert_eq!(load_log_retention_days(home.path()), 1);
    }
}
