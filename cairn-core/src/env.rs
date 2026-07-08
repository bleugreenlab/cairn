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

/// Maintain the full set of agent tool shims in `<cairn_home>/bin` — `cairn`
/// (→ `cairn-cmd`), `jj`, `bun`, and `uv` — so every agent-spawned shell
/// resolves all four on PATH regardless of the host service's own (possibly
/// empty) PATH. Called at startup by whichever host owns the orchestrator (the
/// runner and `cairn-server`); the desktop thin host does not maintain them.
///
/// `cairn`, `bun`, and `uv` are plain forwarders; `jj` additionally intercepts
/// `jj workspace update-stale` (see [`ensure_jj_shim_in`]). Each install is
/// best-effort — a missing bundled binary skips only its own shim. Because
/// `<cairn_home>/bin` is prepended AHEAD of the user PATH, the bundled jj/bun/uv
/// win over any system copy, which is the intended behavior.
pub fn ensure_agent_tool_shims(
    cli_binary: &str,
    jj_binary: &str,
    bun_binary: &str,
    uv_binary: &str,
) {
    let bin_dir = cairn_bin_dir();
    ensure_forwarder_shim_in(&bin_dir, "cairn", cli_binary);
    ensure_jj_shim_in(&bin_dir, jj_binary);
    ensure_forwarder_shim_in(&bin_dir, "bun", bun_binary);
    ensure_forwarder_shim_in(&bin_dir, "uv", uv_binary);
}

/// The shared, per-home uv package cache dir (`<cairn_home>/uv-cache`), injected
/// as `UV_CACHE_DIR` into every agent-spawned process (inline `run` commands,
/// terminals, skill scripts). Placed under the Cairn home rather than uv's
/// default `~/.cache/uv` so all agents share one warm cache AND writes land in a
/// fence-permitted, Cairn-owned location — the sandbox writable set includes it
/// via [`crate::services::sandbox::default_writable_extra`].
pub fn uv_cache_dir() -> PathBuf {
    cairn_common::paths::cairn_home().join("uv-cache")
}

/// Best-effort creation of [`uv_cache_dir`] at host startup, so the sandbox
/// writable-set carve-out (which is existence-filtered) covers it and uv never
/// has to create the dir under a landlock/seatbelt rule that is not yet in
/// place. Called by the runner and `cairn-server` alongside the shim install.
pub fn ensure_uv_cache_dir() {
    let dir = uv_cache_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        log::warn!("uv cache dir skipped: cannot create {}: {e}", dir.display());
    }
}

/// Resolve a bundled sidecar binary by name for a shell-less host (the runner
/// daemon, `cairn-server`): prefer a sibling of the current executable (where
/// Tauri places `externalBin` sidecars and the runner/server binaries sit), then
/// a PATH lookup ([`find_binary`]), else the bare name. The bare-name tail lets a
/// tool that is neither bundled nor on PATH degrade to the caller's own
/// resolution instead of hard-failing.
pub fn resolve_sidecar(name: &str) -> String {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join(sidecar_file_name(name));
            if candidate.is_file() {
                return candidate.to_string_lossy().into_owned();
            }
        }
    }
    find_binary(name).unwrap_or_else(|_| name.to_string())
}

#[cfg(windows)]
fn sidecar_file_name(name: &str) -> String {
    format!("{name}.exe")
}
#[cfg(not(windows))]
fn sidecar_file_name(name: &str) -> String {
    name.to_string()
}

/// Marker embedded in every Cairn-generated script/`.cmd` shim so we only ever
/// overwrite our own shim, never a user's real file placed in `<cairn_home>/bin`.
const CAIRN_SHIM_MARKER: &str = "cairn-managed-shim";

/// Install a plain forwarder shim `<bin_dir>/<name>` → the absolute `target_bin`
/// (Unix symlink; Windows `<name>.cmd`). For tools that need no interception,
/// only PATH resolution (`cairn`, `bun`).
///
/// Best-effort and idempotent, mirroring `cli_install`'s semantics: an
/// already-correct shim is left alone, a stale one (old build path) is replaced,
/// and a real file that is not our shim is never clobbered. Failures are logged,
/// never fatal. The shim points at the sibling binary so it tracks app updates
/// (prod) and rebuilds (dev) automatically.
fn ensure_forwarder_shim_in(bin_dir: &Path, name: &str, target_bin: &str) {
    let target = PathBuf::from(target_bin);
    if !target.exists() {
        log::debug!("{name} shim skipped: {target_bin} does not exist");
        return;
    }
    if let Err(e) = std::fs::create_dir_all(bin_dir) {
        log::warn!(
            "{name} shim skipped: cannot create {}: {e}",
            bin_dir.display()
        );
        return;
    }
    #[cfg(unix)]
    install_symlink_shim_unix(bin_dir, name, &target);
    #[cfg(windows)]
    install_cmd_forwarder_windows(bin_dir, name, &target);
    #[cfg(not(any(unix, windows)))]
    let _ = target;
}

// Unix: symlink `<name>` -> the absolute bundled binary, so it tracks the target.
#[cfg(unix)]
fn install_symlink_shim_unix(bin_dir: &Path, name: &str, target: &Path) {
    let link = bin_dir.join(name);
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
                "{name} shim skipped: {} exists and is not our symlink",
                link.display()
            );
            return;
        }
        Err(_) => {}
    }
    match std::os::unix::fs::symlink(target, &link) {
        Ok(()) => log::info!(
            "Installed `{name}` -> {} at {}",
            target.display(),
            link.display()
        ),
        Err(e) => log::warn!("Failed to install `{name}` shim at {}: {e}", link.display()),
    }
}

// Windows: symlinks need privilege, so write a `<name>.cmd` shim that calls the
// bundled binary by absolute path (tracks updates).
#[cfg(windows)]
fn install_cmd_forwarder_windows(bin_dir: &Path, name: &str, target: &Path) {
    let shim = bin_dir.join(format!("{name}.cmd"));
    let body = format!(
        "@echo off\r\nrem {CAIRN_SHIM_MARKER}\r\n\"{}\" %*\r\n",
        target.display()
    );
    let _ = write_shim_file_if_ours(&shim, &body, name);
}

/// Recognize the pre-marker legacy Windows forwarder body so an upgrade replaces
/// it instead of treating it as a foreign user file. Before the marker existed,
/// `cairn.cmd` was generated as exactly `@echo off` plus a single quoted-path
/// forwarding line (`"<abs>" %*`), with no marker. A two-line `.cmd` of that
/// exact shape is one of ours; the target path is intentionally NOT matched, so
/// a shim whose bundled path has since moved (a stale upgrade, or a dev
/// instance's vanished build path) is still recognized and refreshed instead of
/// leaving agent shells resolving a dead `cairn`.
fn is_legacy_cairn_forwarder(content: &str) -> bool {
    let lines: Vec<&str> = content.lines().collect();
    lines.len() == 2
        && lines[0].trim() == "@echo off"
        && lines[1].starts_with('"')
        && lines[1].ends_with("\" %*")
}

/// (Re)write a file-backed shim (`jj` script, or a `.cmd` forwarder on Windows)
/// only when it is absent, already one of ours (carries [`CAIRN_SHIM_MARKER`]),
/// or a legacy markerless Cairn forwarder we must migrate. A real user file is
/// left untouched. Returns true when the file now holds our shim (freshly
/// written or already ours), false when a foreign file was left alone or the
/// write failed.
fn write_shim_file_if_ours(path: &Path, body: &str, name: &str) -> bool {
    match std::fs::read_to_string(path) {
        // Already correct — no-op so a shim revision does not churn mtimes.
        Ok(existing) if existing == body => return true,
        // A real user file that isn't ours (and not a legacy markerless Cairn
        // forwarder we must migrate) — don't clobber it.
        Ok(existing)
            if !existing.contains(CAIRN_SHIM_MARKER) && !is_legacy_cairn_forwarder(&existing) =>
        {
            log::warn!(
                "{name} shim skipped: {} exists and is not our shim",
                path.display()
            );
            return false;
        }
        // Ours but stale — fall through and rewrite.
        Ok(_) => {}
        Err(_) if path.exists() => {
            log::warn!(
                "{name} shim skipped: cannot read existing {}",
                path.display()
            );
            return false;
        }
        // Absent — write.
        Err(_) => {}
    }
    match std::fs::write(path, body) {
        Ok(()) => {
            log::info!("Installed `{name}` shim at {}", path.display());
            true
        }
        Err(e) => {
            log::warn!("Failed to install `{name}` shim at {}: {e}", path.display());
            false
        }
    }
}

/// The stderr notice printed when the jj shim intercepts a hand-run
/// `jj workspace update-stale`.
const JJ_UPDATE_STALE_NOTICE: &str = "Cairn reconciles jj workspace staleness automatically and serialized against concurrent store operations; the next tool call refreshes this workspace. Hand-running 'jj workspace update-stale' against the shared store can race a concurrent rebase and drop sealed commits, so Cairn intercepts it here. No action taken.";

/// The Unix `jj` shim body: intercept `jj workspace update-stale` (the one
/// command jj advertises on a stale workspace, which raced the shared store in
/// the CAIRN-2422 incident) with an explained no-op, and exec the real jj for
/// everything else — honoring `$CAIRN_JJ_BIN` first (operator/test override),
/// else the absolute bundled path baked in at install time.
#[cfg(unix)]
fn jj_shim_script_unix(jj_abs: &str) -> String {
    format!(
        r#"#!/bin/sh
# {CAIRN_SHIM_MARKER} (jj) — generated by env.rs. Cairn reconciles workspace
# staleness automatically and serialized; hand-running jj workspace update-stale
# against the shared store can race a concurrent rebase and drop sealed commits,
# so this shim intercepts exactly that one command and execs the real jj for
# everything else.
if [ "$1" = "workspace" ] && [ "$2" = "update-stale" ]; then
  echo "{JJ_UPDATE_STALE_NOTICE}" 1>&2
  exit 0
fi
exec "${{CAIRN_JJ_BIN:-{jj_abs}}}" "$@"
"#
    )
}

/// The Windows `jj.cmd` shim body: same interception + forwarding as the Unix
/// script, structured with `goto` labels (not parenthesized blocks) so the long
/// notice text needs no batch escaping.
#[cfg(any(windows, test))]
fn jj_shim_script_windows(jj_abs: &str) -> String {
    format!(
        "@echo off\r\nrem {CAIRN_SHIM_MARKER} (jj)\r\nif \"%~1\"==\"workspace\" if \"%~2\"==\"update-stale\" goto __cairn_stale\r\nif defined CAIRN_JJ_BIN goto __cairn_envbin\r\n\"{jj_abs}\" %*\r\nexit /b %ERRORLEVEL%\r\n:__cairn_envbin\r\n\"%CAIRN_JJ_BIN%\" %*\r\nexit /b %ERRORLEVEL%\r\n:__cairn_stale\r\necho {JJ_UPDATE_STALE_NOTICE} 1>&2\r\nexit /b 0\r\n"
    )
}

/// Maintain `<bin_dir>/jj` (Unix script, chmod 755) / `<bin_dir>/jj.cmd`
/// (Windows) forwarding to the resolved bundled `jj`, with the
/// `workspace update-stale` interception baked in (see [`jj_shim_script_unix`]).
/// Skips when the bundled jj does not resolve to a real path, so a
/// self-referential shim (which would infinitely re-exec itself, since
/// `<cairn_home>/bin` is first on PATH) is never written.
pub(crate) fn ensure_jj_shim_in(bin_dir: &Path, jj_binary: &str) {
    let target = PathBuf::from(jj_binary);
    if !target.exists() {
        log::debug!("jj shim skipped: {jj_binary} does not exist");
        return;
    }
    if let Err(e) = std::fs::create_dir_all(bin_dir) {
        log::warn!("jj shim skipped: cannot create {}: {e}", bin_dir.display());
        return;
    }
    #[cfg(unix)]
    install_jj_shim_unix(bin_dir, &target);
    #[cfg(windows)]
    install_jj_shim_windows(bin_dir, &target);
    #[cfg(not(any(unix, windows)))]
    let _ = target;
}

#[cfg(unix)]
fn install_jj_shim_unix(bin_dir: &Path, jj_target: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let shim = bin_dir.join("jj");
    let body = jj_shim_script_unix(&jj_target.to_string_lossy());
    if write_shim_file_if_ours(&shim, &body, "jj") {
        if let Ok(meta) = std::fs::metadata(&shim) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            if let Err(e) = std::fs::set_permissions(&shim, perms) {
                log::warn!("Failed to chmod jj shim {}: {e}", shim.display());
            }
        }
    }
}

#[cfg(windows)]
fn install_jj_shim_windows(bin_dir: &Path, jj_target: &Path) {
    let shim = bin_dir.join("jj.cmd");
    let body = jj_shim_script_windows(&jj_target.to_string_lossy());
    let _ = write_shim_file_if_ours(&shim, &body, "jj");
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
    find_binary_in(name, get_user_path())
}

/// Resolve a binary against the same PATH agent-spawned shells use
/// ([`agent_shell_path`] — the host-owned `<cairn_home>/bin` shim dir ahead of
/// the resolved user PATH). A tool installed into the cairn bin dir resolves
/// here but not through [`find_binary`], which consults only [`get_user_path`].
/// Used to probe for `uv` before routing inline python through `uv run -`.
pub fn find_binary_on_agent_path(name: &str) -> Result<String, String> {
    find_binary_in(name, &agent_shell_path())
}

/// Shared implementation behind [`find_binary`] and [`find_binary_on_agent_path`]:
/// resolve `name` against an explicit `search_path` using the platform lookup
/// tool (`where` on Windows, `which` on Unix).
fn find_binary_in(name: &str, search_path: &str) -> Result<String, String> {
    #[cfg(windows)]
    {
        // On Windows, use 'where' command
        let output = Command::new("cmd")
            .args(["/c", &format!("where {}", name)])
            .env("PATH", search_path)
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
            .env("PATH", search_path)
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
    use super::{
        ensure_forwarder_shim_in, ensure_jj_shim_in, is_legacy_cairn_forwarder,
        jj_shim_script_unix, jj_shim_script_windows, parse_path_from_env_output, prepend_cairn_bin,
        write_shim_file_if_ours, CAIRN_SHIM_MARKER,
    };
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

        ensure_forwarder_shim_in(&bin, "cairn", &target_str);
        let link = bin.join("cairn");
        assert_eq!(std::fs::read_link(&link).unwrap(), target);

        // Second call is a no-op and leaves the correct symlink in place.
        ensure_forwarder_shim_in(&bin, "cairn", &target_str);
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
        ensure_forwarder_shim_in(&bin, "cairn", &target.to_string_lossy());
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
        ensure_forwarder_shim_in(&bin, "cairn", &target.to_string_lossy());
        // Untouched: still a real file (not a symlink) with its original bytes.
        assert!(std::fs::read_link(&link).is_err());
        assert_eq!(std::fs::read(&link).unwrap(), b"user's own cairn");
    }

    #[test]
    fn agent_cli_shim_skips_missing_target() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        ensure_forwarder_shim_in(&bin, "cairn", "/nonexistent/cairn-cmd");
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

    #[test]
    fn bun_forwarder_shim_creates_replaces_and_never_clobbers() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        let target = dir.path().join("bun");
        std::fs::write(&target, b"#!/bin/sh\n").unwrap();
        let target_str = target.to_string_lossy().to_string();

        // Created as a symlink to the bundled bun, idempotently.
        ensure_forwarder_shim_in(&bin, "bun", &target_str);
        let link = bin.join("bun");
        assert_eq!(std::fs::read_link(&link).unwrap(), target);
        ensure_forwarder_shim_in(&bin, "bun", &target_str);
        assert_eq!(std::fs::read_link(&link).unwrap(), target);

        // A stale symlink (old build path) is replaced.
        std::fs::remove_file(&link).unwrap();
        let stale = dir.path().join("old-bun");
        std::fs::write(&stale, b"x").unwrap();
        std::os::unix::fs::symlink(&stale, &link).unwrap();
        ensure_forwarder_shim_in(&bin, "bun", &target_str);
        assert_eq!(std::fs::read_link(&link).unwrap(), target);

        // A real user file is never clobbered.
        std::fs::remove_file(&link).unwrap();
        std::fs::write(&link, b"user's own bun").unwrap();
        ensure_forwarder_shim_in(&bin, "bun", &target_str);
        assert!(std::fs::read_link(&link).is_err());
        assert_eq!(std::fs::read(&link).unwrap(), b"user's own bun");
    }

    #[test]
    fn bun_forwarder_shim_skips_missing_target() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        ensure_forwarder_shim_in(&bin, "bun", "/nonexistent/bun");
        assert!(!bin.join("bun").exists());
    }

    #[test]
    fn uv_forwarder_shim_creates_replaces_and_never_clobbers() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        let target = dir.path().join("uv");
        std::fs::write(&target, b"#!/bin/sh\n").unwrap();
        let target_str = target.to_string_lossy().to_string();

        // Created as a symlink to the bundled uv, idempotently.
        ensure_forwarder_shim_in(&bin, "uv", &target_str);
        let link = bin.join("uv");
        assert_eq!(std::fs::read_link(&link).unwrap(), target);
        ensure_forwarder_shim_in(&bin, "uv", &target_str);
        assert_eq!(std::fs::read_link(&link).unwrap(), target);

        // A stale symlink (old build path) is replaced.
        std::fs::remove_file(&link).unwrap();
        let stale = dir.path().join("old-uv");
        std::fs::write(&stale, b"x").unwrap();
        std::os::unix::fs::symlink(&stale, &link).unwrap();
        ensure_forwarder_shim_in(&bin, "uv", &target_str);
        assert_eq!(std::fs::read_link(&link).unwrap(), target);

        // A real user file is never clobbered.
        std::fs::remove_file(&link).unwrap();
        std::fs::write(&link, b"user's own uv").unwrap();
        ensure_forwarder_shim_in(&bin, "uv", &target_str);
        assert!(std::fs::read_link(&link).is_err());
        assert_eq!(std::fs::read(&link).unwrap(), b"user's own uv");
    }

    #[test]
    fn uv_forwarder_shim_skips_missing_target() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        ensure_forwarder_shim_in(&bin, "uv", "/nonexistent/uv");
        assert!(!bin.join("uv").exists());
    }

    #[test]
    fn jj_shim_script_unix_intercepts_and_forwards() {
        let script = jj_shim_script_unix("/opt/cairn/jj");
        assert!(script.starts_with("#!/bin/sh\n"));
        assert!(script.contains(CAIRN_SHIM_MARKER));
        // Intercepts exactly `workspace update-stale` and exits 0.
        assert!(script.contains(r#"if [ "$1" = "workspace" ] && [ "$2" = "update-stale" ]; then"#));
        assert!(script.contains("exit 0"));
        assert!(script.contains("Cairn reconciles jj workspace staleness"));
        // Otherwise forwards to $CAIRN_JJ_BIN, else the baked absolute bundled jj.
        assert!(script.contains(r#"exec "${CAIRN_JJ_BIN:-/opt/cairn/jj}" "$@""#));
    }

    #[test]
    fn jj_shim_script_windows_intercepts_and_forwards() {
        let script = jj_shim_script_windows("C:\\cairn\\jj.exe");
        assert!(script.contains(CAIRN_SHIM_MARKER));
        assert!(script.contains("if \"%~1\"==\"workspace\" if \"%~2\"==\"update-stale\" goto"));
        assert!(script.contains("Cairn reconciles jj workspace staleness"));
        assert!(script.contains("exit /b 0"));
        // Prefers CAIRN_JJ_BIN, else the baked absolute bundled jj.
        assert!(script.contains("\"%CAIRN_JJ_BIN%\" %*"));
        assert!(script.contains("\"C:\\cairn\\jj.exe\" %*"));
    }

    #[test]
    fn jj_shim_install_writes_executable_script_and_never_clobbers() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        let jj = dir.path().join("jj-bundled");
        std::fs::write(&jj, b"#!/bin/sh\n").unwrap();
        let jj_str = jj.to_string_lossy().to_string();

        ensure_jj_shim_in(&bin, &jj_str);
        let shim = bin.join("jj");
        assert!(shim.exists(), "jj shim script written");
        let mode = std::fs::metadata(&shim).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o755, "jj shim is executable");
        let body = std::fs::read_to_string(&shim).unwrap();
        assert!(body.contains(CAIRN_SHIM_MARKER));
        assert!(body.contains(&format!("${{CAIRN_JJ_BIN:-{jj_str}}}")));

        // Idempotent.
        ensure_jj_shim_in(&bin, &jj_str);
        assert_eq!(std::fs::read_to_string(&shim).unwrap(), body);

        // A foreign file (no marker) is never clobbered.
        std::fs::write(&shim, b"#!/bin/sh\necho user jj\n").unwrap();
        ensure_jj_shim_in(&bin, &jj_str);
        assert_eq!(
            std::fs::read_to_string(&shim).unwrap(),
            "#!/bin/sh\necho user jj\n",
            "a user's own jj file is left untouched"
        );

        // A missing bundled jj skips the shim entirely (no self-referential loop).
        let bin2 = dir.path().join("bin2");
        ensure_jj_shim_in(&bin2, "/nonexistent/jj");
        assert!(!bin2.join("jj").exists());
    }

    #[test]
    fn legacy_markerless_cairn_cmd_is_recognized_and_migrated() {
        // The pre-marker generator wrote exactly this shape; the absolute path
        // here has since moved (the stale-upgrade case the marker guard must not
        // mistake for a foreign file).
        let legacy = "@echo off\r\n\"C:/old/build/cairn-cmd.exe\" %*\r\n";
        assert!(is_legacy_cairn_forwarder(legacy));

        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("cairn.cmd");
        std::fs::write(&shim, legacy).unwrap();
        let new_body =
            format!("@echo off\r\nrem {CAIRN_SHIM_MARKER}\r\n\"C:/new/cairn-cmd.exe\" %*\r\n");
        assert!(
            write_shim_file_if_ours(&shim, &new_body, "cairn"),
            "a legacy markerless cairn.cmd is migrated, not treated as foreign"
        );
        assert_eq!(std::fs::read_to_string(&shim).unwrap(), new_body);
    }

    #[test]
    fn foreign_cmd_without_marker_is_not_clobbered() {
        let foreign = "@echo off\r\necho the user's own script\r\n";
        assert!(!is_legacy_cairn_forwarder(foreign));

        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("cairn.cmd");
        std::fs::write(&shim, foreign).unwrap();
        let new_body = format!("@echo off\r\nrem {CAIRN_SHIM_MARKER}\r\n\"x\" %*\r\n");
        assert!(
            !write_shim_file_if_ours(&shim, &new_body, "cairn"),
            "a foreign .cmd without the marker is left untouched"
        );
        assert_eq!(std::fs::read_to_string(&shim).unwrap(), foreign);
    }
}
