//! Canonical resolution of the Cairn home directory, log directory, and
//! callback port.
//!
//! This is the single source of truth for "which Cairn home am I" (`.cairn`
//! vs `.cairn-dev`) and the callback port that the app and its children share.
//! Every other crate delegates here: logging (`logging::default_log_dir`),
//! auth (`auth::load_local_mcp_token`), `cairn_core::config::get_config_dir`,
//! the Tauri log viewer, and `cairn-cmd`.
//!
//! ## Mode signal: `CAIRN_ENV`, not `debug_assertions`
//!
//! `cfg!(debug_assertions)` is the wrong signal for child processes. `cairn-cmd`
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

/// Fixed production runner transport port. The desktop and runner both address
/// the local execution daemon by this convention instead of a discovery file.
pub const DEFAULT_RUNNER_PORT: u16 = 3849;

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

/// The database filename inside `cairn_home()` that the runner owns.
pub const RUNNER_DB_FILENAME: &str = "cairn.db";

/// The Tauri desktop bundle identifier — the subdirectory name under the OS
/// app-data dir where the pre-runner desktop stored its database.
const DESKTOP_BUNDLE_IDENTIFIER: &str = "com.cairn.desktop";

/// Path to the database the pre-runner desktop stored in the OS app-data dir.
///
/// Before the runner-daemon cutover, the desktop opened its database from
/// Tauri's `app_data_dir()` (`dirs::data_dir()/com.cairn.desktop/`), not
/// `cairn_home()`. The runner now owns `~/.cairn`, so on first boot it carries
/// this legacy database across (see `cairn-runner`'s legacy migration). Prod
/// used `cairn.turso.db`; dev used `cairn-dev.turso.db`.
///
/// Returns `None` when the OS app-data dir cannot be determined.
pub fn legacy_appdata_db_path() -> Option<PathBuf> {
    let name = if is_dev() {
        "cairn-dev.turso.db"
    } else {
        "cairn.turso.db"
    };
    dirs::data_dir().map(|dir| dir.join(DESKTOP_BUNDLE_IDENTIFIER).join(name))
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
    env_port("CAIRN_CALLBACK_PORT").unwrap_or_else(|| {
        if is_dev() {
            DEV_CALLBACK_PORT
        } else {
            PROD_CALLBACK_PORT
        }
    })
}

/// The runner transport port for the current process.
///
/// `bun run dev:instance` sets `CAIRN_RUNNER_PORT` to a stable per-slot value;
/// production and bare `tauri dev` use the fixed runner port by convention.
pub fn runner_port() -> u16 {
    env_port("CAIRN_RUNNER_PORT").unwrap_or(DEFAULT_RUNNER_PORT)
}

fn env_port(name: &str) -> Option<u16> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
}

// ============================================================================
// dev:instance resolution
//
// `bun run dev:instance` (scripts/dev-instance.ts) launches a branch-keyed dev
// build whose home is `~/.cairn-dev-<key>` (key = slugified branch), with its
// database at `<home>/cairn.db` and MCP callback port `3860 + slot`
// where the slot is persisted per branch in `~/.cairn-dev-instances.json`. These
// helpers mirror that launcher's path/slug/port contract so the host app can
// resolve a running dev instance and query it (see docs/dev-instances.md). They
// key off the OS home directory, not `CAIRN_HOME`: instance roots always live
// under the real home even when the host app itself runs with a `CAIRN_HOME`
// override.
// ============================================================================

/// Base runner transport port for `dev:instance` slots: slot `s` binds `BASE + s`.
/// Mirrors `RUNNER_PORT_BASE` in scripts/dev-instance-slots.ts.
pub const DEV_INSTANCE_RUNNER_PORT_BASE: u16 = DEFAULT_RUNNER_PORT;

/// Prefix marking a `dev:instance` home directory. The trailing hyphen keeps
/// instance homes (`~/.cairn-dev-<key>`) distinct from the base `tauri dev`
/// (slot 0) home `~/.cairn-dev`.
pub const DEV_INSTANCE_HOME_PREFIX: &str = ".cairn-dev-";

/// Database filename inside every `dev:instance` home.
pub const DEV_INSTANCE_DB_FILENAME: &str = RUNNER_DB_FILENAME;

/// Filename of the `dev:instance` branch->slot registry under the OS home.
const DEV_INSTANCE_REGISTRY_FILENAME: &str = ".cairn-dev-instances.json";

/// The OS user home directory (`~`), independent of any `CAIRN_HOME` override.
pub fn os_home_dir() -> Option<PathBuf> {
    dirs::home_dir()
}

/// Slugify a git branch into a `dev:instance` key, matching `slugify` in
/// scripts/dev-instance.ts: lowercase, every run of non-alphanumeric chars
/// collapses to a single `-`, and leading/trailing `-` are trimmed. Returns
/// `None` when the result is empty (mirroring the launcher's rejection).
pub fn dev_instance_slug(branch: &str) -> Option<String> {
    let mut slug = String::new();
    let mut pending_dash = false;
    for ch in branch.chars() {
        if ch.is_ascii_alphanumeric() {
            if pending_dash && !slug.is_empty() {
                slug.push('-');
            }
            pending_dash = false;
            slug.push(ch.to_ascii_lowercase());
        } else {
            pending_dash = true;
        }
    }
    if slug.is_empty() {
        None
    } else {
        Some(slug)
    }
}

/// The `dev:instance` registry path (`~/.cairn-dev-instances.json`).
pub fn dev_instance_registry_path() -> Option<PathBuf> {
    os_home_dir().map(|home| home.join(DEV_INSTANCE_REGISTRY_FILENAME))
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
        std::env::remove_var("CAIRN_RUNNER_PORT");
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
    fn runner_port_uses_env_or_default() {
        let _guard = env_lock();
        clear_env();

        assert_eq!(runner_port(), DEFAULT_RUNNER_PORT);
        std::env::set_var("CAIRN_RUNNER_PORT", "3999");
        assert_eq!(runner_port(), 3999);
        std::env::set_var("CAIRN_RUNNER_PORT", "0");
        assert_eq!(runner_port(), DEFAULT_RUNNER_PORT);

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

    #[test]
    fn dev_instance_database_uses_runner_filename() {
        assert_eq!(DEV_INSTANCE_DB_FILENAME, RUNNER_DB_FILENAME);
        assert_eq!(DEV_INSTANCE_DB_FILENAME, "cairn.db");
    }

    #[test]
    fn dev_instance_slug_matches_launcher_contract() {
        // Lowercased, non-alphanumeric runs collapse to one '-', ends trimmed.
        assert_eq!(
            dev_instance_slug("agent/CAIRN-1928-builder-0").as_deref(),
            Some("agent-cairn-1928-builder-0")
        );
        assert_eq!(dev_instance_slug("main").as_deref(), Some("main"));
        assert_eq!(
            dev_instance_slug("feature/Foo_Bar").as_deref(),
            Some("feature-foo-bar")
        );
        assert_eq!(
            dev_instance_slug("--weird//name--").as_deref(),
            Some("weird-name")
        );
        // An already-slugified key round-trips to itself (selector tolerance).
        assert_eq!(
            dev_instance_slug("agent-cairn-1928-builder-0").as_deref(),
            Some("agent-cairn-1928-builder-0")
        );
        // Empty / all-separator branches slugify to nothing.
        assert_eq!(dev_instance_slug(""), None);
        assert_eq!(dev_instance_slug("///"), None);
    }
}
