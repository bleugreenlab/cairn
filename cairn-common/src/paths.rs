//! Canonical resolution of the Cairn home directory, log directory, and
//! callback port.
//!
//! This is the single source of truth for "which Cairn home am I" (`.cairn`
//! vs `.cairn-dev`) and the callback port that the app and its children share.
//! Every other crate delegates here: logging (`logging::default_log_dir`),
//! auth (`auth::load_local_mcp_token`), `cairn_core::config::get_config_dir`,
//! the Tauri log viewer, and `cairn-cli`.
//!
//! ## Mode signal: `CAIRN_ENV`, not `debug_assertions`
//!
//! `cfg!(debug_assertions)` is the wrong signal for child processes. `cairn-cli`
//! is built `--release` (so its `debug_assertions` is always false) even when it
//! serves a dev app. Mode is therefore driven by an explicit `CAIRN_ENV`
//! (`dev`/`prod`) env var, falling back to `cfg!(debug_assertions)` only when the
//! var is unset. The app reads its own mode (env unset → its build profile) and
//! propagates the resolved mode to every MCP child, so a release-built child
//! resolves the same home and port as the dev app that spawned it.

use std::path::PathBuf;

/// Maximum accepted request-body size for the MCP callback endpoint(s).
///
/// axum applies a default 2 MiB limit to `String`/`Bytes`/`Json` body
/// extractors; a large file `content` (escaped multi-line payloads especially)
/// can exceed that and be rejected with HTTP 413 before any handler runs. Both
/// the Tauri callback server and the headless `cairn-server` raise the limit to
/// this value via `axum::extract::DefaultBodyLimit`, so the cap is canonical
/// across every MCP callback router.
pub const MCP_CALLBACK_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Prod callback port (the running Cairn desktop app).
const PROD_CALLBACK_PORT: u16 = 3847;
/// Dev callback port (the `tauri dev` desktop app).
const DEV_CALLBACK_PORT: u16 = 3857;

/// True when running in dev mode.
///
/// `CAIRN_ENV=dev` → true, `CAIRN_ENV=prod` → false, unset/other →
/// `cfg!(debug_assertions)`.
pub fn is_dev() -> bool {
    match std::env::var("CAIRN_ENV") {
        Ok(value) if value.eq_ignore_ascii_case("dev") => true,
        Ok(value) if value.eq_ignore_ascii_case("prod") => false,
        _ => cfg!(debug_assertions),
    }
}

/// The resolved mode as the canonical string to propagate to child processes.
pub fn env_str() -> &'static str {
    if is_dev() {
        "dev"
    } else {
        "prod"
    }
}

/// The Cairn home directory.
///
/// If `CAIRN_HOME` is set, it is used verbatim. Otherwise resolves to
/// `~/.cairn-dev` (dev) or `~/.cairn` (prod), falling back to `/tmp` when the
/// home directory cannot be determined (matching prior logging behavior).
pub fn cairn_home() -> PathBuf {
    if let Some(path) = std::env::var_os("CAIRN_HOME").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }

    let suffix = if is_dev() { ".cairn-dev" } else { ".cairn" };
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(suffix)
}

/// The Cairn log directory.
///
/// If `CAIRN_LOG_DIR` is set, it is used verbatim (the narrow log-only
/// override). Otherwise `cairn_home().join("logs")`.
pub fn cairn_log_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("CAIRN_LOG_DIR").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }

    cairn_home().join("logs")
}

/// The MCP callback port for the current mode.
///
/// If `CAIRN_CALLBACK_PORT` is set to a valid non-zero `u16`, it wins over the
/// dev/prod default. This is the instance-key hook that lets several concurrent
/// dev builds each bind a distinct callback port (and have their spawned MCP
/// children resolve the matching `CAIRN_CALLBACK_URL`).
pub fn callback_port() -> u16 {
    if let Some(port) = std::env::var("CAIRN_CALLBACK_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
    {
        return port;
    }

    if is_dev() {
        DEV_CALLBACK_PORT
    } else {
        PROD_CALLBACK_PORT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Serialize env mutation across tests (mirrors the `env_lock` pattern in
    /// `commands/logs.rs`).
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    /// Clear every env var this module reads, so each test starts clean.
    fn clear_env() {
        std::env::remove_var("CAIRN_ENV");
        std::env::remove_var("CAIRN_HOME");
        std::env::remove_var("CAIRN_LOG_DIR");
        std::env::remove_var("CAIRN_CALLBACK_PORT");
    }

    #[test]
    fn is_dev_honors_cairn_env() {
        let _guard = env_lock();
        clear_env();

        std::env::set_var("CAIRN_ENV", "dev");
        assert!(is_dev());

        std::env::set_var("CAIRN_ENV", "DEV");
        assert!(is_dev());

        std::env::set_var("CAIRN_ENV", "prod");
        assert!(!is_dev());

        std::env::set_var("CAIRN_ENV", "PROD");
        assert!(!is_dev());

        clear_env();
    }

    #[test]
    fn is_dev_falls_back_to_build_profile_when_unset() {
        let _guard = env_lock();
        clear_env();

        // Unset and unrecognized values both fall back to the build profile.
        assert_eq!(is_dev(), cfg!(debug_assertions));
        std::env::set_var("CAIRN_ENV", "nonsense");
        assert_eq!(is_dev(), cfg!(debug_assertions));

        clear_env();
    }

    #[test]
    fn env_str_matches_mode() {
        let _guard = env_lock();
        clear_env();

        std::env::set_var("CAIRN_ENV", "dev");
        assert_eq!(env_str(), "dev");
        std::env::set_var("CAIRN_ENV", "prod");
        assert_eq!(env_str(), "prod");

        clear_env();
    }

    #[test]
    fn cairn_home_suffix_per_mode() {
        let _guard = env_lock();
        clear_env();

        std::env::set_var("CAIRN_ENV", "dev");
        assert!(
            cairn_home().ends_with(".cairn-dev"),
            "dev home should end with .cairn-dev: {:?}",
            cairn_home()
        );

        std::env::set_var("CAIRN_ENV", "prod");
        assert!(
            cairn_home().ends_with(".cairn"),
            "prod home should end with .cairn: {:?}",
            cairn_home()
        );

        clear_env();
    }

    #[test]
    fn cairn_home_override_wins() {
        let _guard = env_lock();
        clear_env();

        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", dir.path());
        // Mode is irrelevant when CAIRN_HOME is set.
        std::env::set_var("CAIRN_ENV", "dev");
        assert_eq!(cairn_home(), dir.path());

        clear_env();
    }

    #[test]
    fn cairn_log_dir_derives_from_home() {
        let _guard = env_lock();
        clear_env();

        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", dir.path());
        assert_eq!(cairn_log_dir(), dir.path().join("logs"));

        clear_env();
    }

    #[test]
    fn cairn_log_dir_override_wins() {
        let _guard = env_lock();
        clear_env();

        let home = tempfile::tempdir().unwrap();
        let logs = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", home.path());
        std::env::set_var("CAIRN_LOG_DIR", logs.path());
        // CAIRN_LOG_DIR wins over the derived home/logs path.
        assert_eq!(cairn_log_dir(), logs.path());

        clear_env();
    }

    #[test]
    fn callback_port_per_mode() {
        let _guard = env_lock();
        clear_env();

        std::env::set_var("CAIRN_ENV", "dev");
        assert_eq!(callback_port(), 3857);
        std::env::set_var("CAIRN_ENV", "prod");
        assert_eq!(callback_port(), 3847);

        clear_env();
    }

    #[test]
    fn callback_port_override_wins() {
        let _guard = env_lock();
        clear_env();

        // An explicit valid port wins over both dev and prod defaults.
        std::env::set_var("CAIRN_CALLBACK_PORT", "3861");
        std::env::set_var("CAIRN_ENV", "dev");
        assert_eq!(callback_port(), 3861);
        std::env::set_var("CAIRN_ENV", "prod");
        assert_eq!(callback_port(), 3861);

        clear_env();
    }

    #[test]
    fn callback_port_ignores_invalid_override() {
        let _guard = env_lock();
        clear_env();

        std::env::set_var("CAIRN_ENV", "dev");
        // Non-numeric, out-of-range, and zero all fall back to the mode default.
        std::env::set_var("CAIRN_CALLBACK_PORT", "not-a-port");
        assert_eq!(callback_port(), 3857);
        std::env::set_var("CAIRN_CALLBACK_PORT", "70000");
        assert_eq!(callback_port(), 3857);
        std::env::set_var("CAIRN_CALLBACK_PORT", "0");
        assert_eq!(callback_port(), 3857);

        clear_env();
    }
}
