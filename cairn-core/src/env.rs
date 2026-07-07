//! Environment utilities for running CLI commands in signed/sandboxed apps.
//!
//! Signed/notarized macOS apps run with a restricted PATH that doesn't include
//! user-installed tools like claude, gh, git, npx, etc. This module provides
//! utilities to resolve the user's actual PATH and run commands with it.
//!
//! On Windows, similar issues can occur with PATH resolution in GUI apps.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

#[cfg(not(windows))]
use std::process::{Output, Stdio};
#[cfg(not(windows))]
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;

/// Cached user PATH - resolved once on first use
static USER_PATH: OnceLock<String> = OnceLock::new();

/// Get the PATH separator for the current platform
#[cfg(windows)]
const PATH_SEP: char = ';';
#[cfg(not(windows))]
const PATH_SEP: char = ':';

/// Get the user's home directory
fn get_home_dir() -> String {
    // Try platform-specific env vars first, then fall back to dirs crate
    #[cfg(windows)]
    {
        std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .unwrap_or_else(|_| {
                dirs::home_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "C:\\Users".to_string())
            })
    }
    #[cfg(not(windows))]
    {
        std::env::var("HOME").unwrap_or_else(|_| {
            dirs::home_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "/Users".to_string())
        })
    }
}

/// Get a reasonable PATH for finding CLI tools.
/// Includes common installation locations and the user's shell PATH.
/// Result is cached for subsequent calls.
pub fn get_user_path() -> &'static str {
    USER_PATH.get_or_init(|| {
        let home = get_home_dir();

        #[cfg(windows)]
        {
            // Windows common paths where CLI tools are installed
            let common_paths = [
                format!("{}\\.bun\\bin", home),
                format!("{}\\AppData\\Local\\Programs\\bun", home),
                format!("{}\\.cargo\\bin", home),
                format!("{}\\AppData\\Roaming\\npm", home),
                format!("{}\\AppData\\Local\\Yarn\\bin", home),
                format!("{}\\scoop\\shims", home),
                "C:\\Program Files\\nodejs".to_string(),
                "C:\\Program Files\\Git\\cmd".to_string(),
            ];

            // Get existing PATH and prepend common paths
            let existing_path = std::env::var("PATH").unwrap_or_default();
            let mut all_paths: Vec<&str> = common_paths.iter().map(|s| s.as_str()).collect();
            if !existing_path.is_empty() {
                all_paths.push(&existing_path);
            }

            all_paths.join(&PATH_SEP.to_string())
        }

        #[cfg(not(windows))]
        {
            // Unix common paths where CLI tools are installed
            let common_paths = format!(
                "{}/.claude/local/bin:{}/.bun/bin:{}/.local/bin:{}/.npm/bin:{}/.yarn/bin:{}/.cargo/bin:/usr/local/bin:/opt/homebrew/bin",
                home, home, home, home, home, home
            );

            // Start with the process's actual PATH (includes Docker ENV, etc.)
            let env_path = std::env::var("PATH").unwrap_or_default();

            if let Some(shell_path) = resolve_user_shell_path() {
                return format!("{}:{}:{}", common_paths, shell_path, env_path);
            }

            if env_path.is_empty() {
                format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", common_paths)
            } else {
                format!("{}:{}", common_paths, env_path)
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Agent CLI shim: `<cairn_home>/bin/cairn` -> the bundled `cairn-cmd`.
//
// Agent-spawned shells (inline `run` commands, background + PTY terminals)
// already receive the callback env (`CAIRN_CALLBACK_URL`, `CAIRN_MCP_SECRET`,
// …), so `cairn read|write|watch` works from them the moment the binary
// resolves. Rather than depend on the best-effort user-facing installer
// (`cli_install`, desktop-only and PATH-dependent), the host owns a bin dir
// keyed off the resolved Cairn home and prepends it to every agent-facing
// spawn's PATH. Keying off `cairn_home()` makes dev instances (`~/.cairn-dev*`)
// fall out naturally: each home gets its own shim tracking its own rebuild.
// ---------------------------------------------------------------------------

/// The host-owned bin directory (`<cairn_home>/bin`) that holds the `cairn`
/// shim pointing at the bundled `cairn-cmd`. Keyed off the resolved Cairn home,
/// so a dev instance's separate `~/.cairn-dev*` home gets its own shim dir and
/// tracks its own dev rebuild automatically.
pub fn cairn_bin_dir() -> PathBuf {
    cairn_common::paths::cairn_home().join("bin")
}

/// Compose an agent-shell PATH by placing `bin_dir` ahead of `user_path`.
fn prepend_cairn_bin(bin_dir: &Path, user_path: &str) -> String {
    format!("{}{}{}", bin_dir.display(), PATH_SEP, user_path)
}

/// PATH for agent-spawned shells: the host-owned cairn bin dir (holding the
/// `cairn` shim) ahead of the resolved user PATH, so an in-run `cairn read …`
/// resolves regardless of how the user's own PATH is configured. Every
/// agent-facing spawn site (inline `run` commands, background + PTY terminals)
/// sets `PATH` to this instead of bare [`get_user_path`].
pub fn agent_shell_path() -> String {
    prepend_cairn_bin(&cairn_bin_dir(), get_user_path())
}

/// Environment variables that route a dev-instance's cargo builds into the one
/// shared dev target dir (`~/.cairn-dev-target/target`).
///
/// A host orchestrator launched by `bun dev:instance` carries BOTH: it sets
/// `CAIRN_INSTANCE=1`, and `scripts/rust-cache-env.ts` derives
/// `CARGO_TARGET_DIR` from that signal. That routing is correct for the dev
/// instance's own build (its app binary must survive worktree teardown), but it
/// must never leak into a spawned worktree command: command spawns inherit the
/// orchestrator's env wholesale, so an un-stripped child routes its cargo checks
/// into the single shared dir and concurrent worktrees corrupt each other's
/// build-script `OUT_DIR` (the tree-sitter `stdlib-symbols.txt` ENOENT race —
/// CAIRN-2533). Every agent-facing spawn seam strips both keys so each worktree
/// builds into its own per-worktree `src-tauri/target`. Both are required:
/// dropping only `CARGO_TARGET_DIR` lets the child's own `rustCacheEnv`
/// re-derive the shared dir from `CAIRN_INSTANCE=1`; dropping only
/// `CAIRN_INSTANCE` leaves the directly-inherited `CARGO_TARGET_DIR` in place.
pub const DEV_INSTANCE_ROUTING_ENV: [&str; 2] = ["CAIRN_INSTANCE", "CARGO_TARGET_DIR"];

/// Maintain `<cairn_home>/bin/cairn` pointing at `cli_binary` (the resolved
/// `cairn-cmd` path), so agent-spawned shells resolve `cairn`. Called at
/// startup by whichever host owns the orchestrator (the runner and
/// `cairn-server`); the desktop thin host does not maintain it.
///
/// Best-effort and idempotent, mirroring `cli_install`'s semantics: an
/// already-correct symlink is left alone, a stale symlink (old build path) is
/// replaced, and a real file that is not our symlink is never clobbered.
/// Failures are logged, never fatal. The shim points at the sibling `cairn-cmd`
/// so it tracks app updates (prod) and rebuilds (dev) automatically.
pub fn ensure_agent_cli_shim(cli_binary: &str) {
    ensure_agent_cli_shim_in(&cairn_bin_dir(), cli_binary);
}

/// Testable core of [`ensure_agent_cli_shim`] with an explicit bin dir.
fn ensure_agent_cli_shim_in(bin_dir: &Path, cli_binary: &str) {
    let target = PathBuf::from(cli_binary);
    if !target.exists() {
        log::debug!("cairn CLI shim skipped: {cli_binary} does not exist");
        return;
    }
    if let Err(e) = std::fs::create_dir_all(bin_dir) {
        log::warn!(
            "cairn CLI shim skipped: cannot create {}: {e}",
            bin_dir.display()
        );
        return;
    }
    #[cfg(unix)]
    install_shim_unix(bin_dir, &target);
    #[cfg(windows)]
    install_shim_windows(bin_dir, &target);
    #[cfg(not(any(unix, windows)))]
    let _ = target;
}

// Unix: symlink `cairn` -> the absolute `cairn-cmd`, so it tracks the target.
#[cfg(unix)]
fn install_shim_unix(bin_dir: &Path, target: &Path) {
    let link = bin_dir.join("cairn");
    match std::fs::read_link(&link) {
        // Already points where we want.
        Ok(existing) if existing == target => return,
        // Stale symlink (old build path) — replace it.
        Ok(_) => {
            let _ = std::fs::remove_file(&link);
        }
        // A real file that isn't a symlink — don't clobber it.
        Err(_) if link.exists() => {
            log::warn!(
                "cairn CLI shim skipped: {} exists and is not our symlink",
                link.display()
            );
            return;
        }
        Err(_) => {}
    }
    match std::os::unix::fs::symlink(target, &link) {
        Ok(()) => log::info!(
            "Installed `cairn` -> {} at {}",
            target.display(),
            link.display()
        ),
        Err(e) => log::warn!("Failed to install `cairn` shim at {}: {e}", link.display()),
    }
}

// Windows: symlinks need privilege, so write a `cairn.cmd` shim that calls the
// bundled cli by absolute path (tracks updates).
#[cfg(windows)]
fn install_shim_windows(bin_dir: &Path, target: &Path) {
    let shim = bin_dir.join("cairn.cmd");
    let body = format!("@echo off\r\n\"{}\" %*\r\n", target.display());
    if std::fs::read_to_string(&shim)
        .map(|c| c == body)
        .unwrap_or(false)
    {
        return;
    }
    match std::fs::write(&shim, &body) {
        Ok(()) => log::info!("Installed `cairn` shim at {}", shim.display()),
        Err(e) => log::warn!("Failed to install `cairn` shim at {}: {e}", shim.display()),
    }
}

#[cfg(not(windows))]
fn resolve_user_shell_path() -> Option<String> {
    let user_shell = crate::services::get_default_shell();
    let mut user_shell_command = Command::new(&user_shell);
    user_shell_command.args(["-ilc", "command env"]);
    shell_path_from_command(&mut user_shell_command).or_else(|| {
        let mut fallback_command = Command::new("sh");
        fallback_command.args(["-lc", "command env"]);
        shell_path_from_command(&mut fallback_command)
    })
}

#[cfg(not(windows))]
fn shell_path_from_command(command: &mut Command) -> Option<String> {
    let output = command_output_with_timeout(command, Duration::from_secs(3)).ok()?;
    if !output.status.success() {
        return None;
    }
    parse_path_from_env_output(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(not(windows))]
fn command_output_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> std::io::Result<Output> {
    let mut child = command
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + timeout;

    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output();
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return child.wait_with_output();
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(not(windows))]
pub(crate) fn parse_path_from_env_output(output: &str) -> Option<String> {
    output
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("PATH=").filter(|path| !path.is_empty()))
        .map(ToOwned::to_owned)
}

/// Find a binary by name using the user's PATH.
/// Returns the full path to the binary if found.
pub fn find_binary(name: &str) -> Result<String, String> {
    let user_path = get_user_path();

    #[cfg(windows)]
    {
        // On Windows, use 'where' command
        let output = Command::new("cmd")
            .args(["/c", &format!("where {}", name)])
            .env("PATH", user_path)
            .output()
            .map_err(|e| format!("Failed to find {}: {}", name, e))?;

        if !output.status.success() {
            return Err(format!("{} not found in PATH", name));
        }

        // 'where' can return multiple lines, take the first one
        let paths = String::from_utf8_lossy(&output.stdout);
        let path = paths.lines().next().unwrap_or("").trim().to_string();

        if path.is_empty() {
            return Err(format!("{} not found", name));
        }

        Ok(path)
    }

    #[cfg(not(windows))]
    {
        // On Unix, use 'which' command
        let output = Command::new("sh")
            .args(["-c", &format!("which {}", name)])
            .env("PATH", user_path)
            .output()
            .map_err(|e| format!("Failed to find {}: {}", name, e))?;

        if !output.status.success() {
            return Err(format!("{} not found in PATH", name));
        }

        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            return Err(format!("{} not found", name));
        }

        Ok(path)
    }
}

/// Create a Command with the user's PATH set and console hidden on Windows.
pub fn command(program: &str) -> Command {
    let mut cmd = Command::new(program);
    cmd.env("PATH", get_user_path());

    #[cfg(windows)]
    cmd.creation_flags(CREATE_NO_WINDOW);

    cmd
}

/// Create a Command for git with the user's PATH set.
pub fn git() -> Command {
    command("git")
}

/// Create a Command for gh (GitHub CLI) with the user's PATH set.
pub fn gh() -> Command {
    command("gh")
}

#[cfg(test)]
#[cfg(not(windows))]
mod tests {
    use super::{ensure_agent_cli_shim_in, parse_path_from_env_output, prepend_cairn_bin};
    use std::path::Path;

    #[test]
    fn prepend_cairn_bin_places_bin_dir_first() {
        assert_eq!(
            prepend_cairn_bin(Path::new("/home/u/.cairn/bin"), "/usr/local/bin:/usr/bin"),
            "/home/u/.cairn/bin:/usr/local/bin:/usr/bin"
        );
    }

    #[test]
    fn agent_cli_shim_creates_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        let target = dir.path().join("cairn-cmd");
        std::fs::write(&target, b"#!/bin/sh\n").unwrap();
        let target_str = target.to_string_lossy().to_string();

        ensure_agent_cli_shim_in(&bin, &target_str);
        let link = bin.join("cairn");
        assert_eq!(std::fs::read_link(&link).unwrap(), target);

        // Second call is a no-op and leaves the correct symlink in place.
        ensure_agent_cli_shim_in(&bin, &target_str);
        assert_eq!(std::fs::read_link(&link).unwrap(), target);
    }

    #[test]
    fn agent_cli_shim_replaces_stale_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let stale = dir.path().join("old-cairn-cmd");
        std::fs::write(&stale, b"x").unwrap();
        let link = bin.join("cairn");
        std::os::unix::fs::symlink(&stale, &link).unwrap();

        let target = dir.path().join("cairn-cmd");
        std::fs::write(&target, b"y").unwrap();
        ensure_agent_cli_shim_in(&bin, &target.to_string_lossy());
        assert_eq!(std::fs::read_link(&link).unwrap(), target);
    }

    #[test]
    fn agent_cli_shim_never_clobbers_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let link = bin.join("cairn");
        std::fs::write(&link, b"user's own cairn").unwrap();

        let target = dir.path().join("cairn-cmd");
        std::fs::write(&target, b"y").unwrap();
        ensure_agent_cli_shim_in(&bin, &target.to_string_lossy());
        // Untouched: still a real file (not a symlink) with its original bytes.
        assert!(std::fs::read_link(&link).is_err());
        assert_eq!(std::fs::read(&link).unwrap(), b"user's own cairn");
    }

    #[test]
    fn agent_cli_shim_skips_missing_target() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        ensure_agent_cli_shim_in(&bin, "/nonexistent/cairn-cmd");
        assert!(!bin.join("cairn").exists());
    }

    #[test]
    fn parse_path_from_env_output_picks_path_line() {
        let output = "SHELL=/bin/zsh\nPATH=/usr/local/bin:/usr/bin\nHOME=/Users/example\n";
        assert_eq!(
            parse_path_from_env_output(output).as_deref(),
            Some("/usr/local/bin:/usr/bin")
        );
    }

    #[test]
    fn parse_path_from_env_output_ignores_non_path_lines() {
        let output = "SHELL=/bin/zsh\nCAIRN_PATH_HINT=/tmp/bin\nHOME=/Users/example\n";
        assert_eq!(parse_path_from_env_output(output), None);
    }

    #[test]
    fn parse_path_from_env_output_uses_last_path_line() {
        let output = "PATH=/minimal\nnoise from shell rc\nPATH=/shell/configured:/usr/bin\n";
        assert_eq!(
            parse_path_from_env_output(output).as_deref(),
            Some("/shell/configured:/usr/bin")
        );
    }

    #[test]
    fn parse_path_from_env_output_rejects_empty_path() {
        assert_eq!(parse_path_from_env_output("PATH=\n"), None);
    }

    #[test]
    fn dev_instance_routing_env_covers_both_routing_vars() {
        use super::DEV_INSTANCE_ROUTING_ENV;
        // Both vars are load-bearing (see the const's doc): stripping only one
        // still lets a worktree command reach the shared dev target dir.
        assert!(DEV_INSTANCE_ROUTING_ENV.contains(&"CAIRN_INSTANCE"));
        assert!(DEV_INSTANCE_ROUTING_ENV.contains(&"CARGO_TARGET_DIR"));
        assert_eq!(DEV_INSTANCE_ROUTING_ENV.len(), 2);
    }
}
