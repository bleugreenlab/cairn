//! The PATH a Cairn process gives the commands it runs.
//!
//! Every host Cairn runs on starts from a stunted PATH. A signed macOS app
//! inherits the launch services environment, a launchd daemon inherits
//! `/usr/bin:/bin:/usr/sbin:/sbin`, and a non-interactive `ssh` session gets
//! whatever sshd hands it — none of which include the user-installed toolchains
//! (`bun`, `node`, `cargo`, `gh`, `claude`) that project commands invoke by bare
//! name. This module composes the PATH a user's own login shell would give, so
//! a bare `bun i` resolves the same way no matter how the process that runs it
//! was started.
//!
//! It lives in `cairn-common` because the answer must not depend on which
//! binary asked, or on which machine asked on another machine's behalf. A PATH
//! is a fact about ONE host's filesystem: composing it here and shipping it to a
//! process elsewhere names directories that do not exist there. So each process
//! composes its own, on its own machine — the runner for what it spawns, and the
//! executor for everything it runs, cell setup commands and batch commands
//! alike. Nothing about a spawn path or a placement decision may decide what a
//! command can see.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[cfg(not(windows))]
use std::process::{Command, Output, Stdio};
#[cfg(not(windows))]
use std::time::{Duration, Instant};

/// Resolved once per process: the composition shells out to a login shell, and
/// the answer cannot change without the process restarting anyway.
static USER_PATH: OnceLock<String> = OnceLock::new();

#[cfg(windows)]
const PATH_SEP: char = ';';
#[cfg(not(windows))]
const PATH_SEP: char = ':';

/// The suffix a bare program name takes before it names a file on this
/// platform.
///
/// Windows resolves a bare `cargo` to `cargo.exe` and never to a file literally
/// named `cargo`; Unix resolves it literally. A PATH lookup is therefore not one
/// algorithm with a different separator, and treating it as one is how a
/// diagnostic reports "not found" for a toolchain that is sitting right there.
#[cfg(windows)]
const EXECUTABLE_SUFFIX: &str = ".exe";
#[cfg(not(windows))]
const EXECUTABLE_SUFFIX: &str = "";

/// The OS account this process runs as — the identity whose home directory and
/// per-user tool installs [`user_path`] composes around.
///
/// This is load-bearing for toolchain detection rather than decoration. A
/// per-user install belongs to exactly one account, so a machine can carry a
/// perfectly good toolchain that the account Cairn logs in as cannot reach.
/// Naming the account is what lets "this machine has no Rust" and "this machine
/// has Rust, installed for somebody else" be told apart at the fleet surface,
/// instead of both arriving as an unexplained empty list.
pub fn account_name() -> String {
    #[cfg(windows)]
    {
        std::env::var("USERNAME").unwrap_or_else(|_| "unknown".to_string())
    }
    #[cfg(not(windows))]
    {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "unknown".to_string())
    }
}

/// Where a spawn of `program` would find it, searching this process's own PATH
/// the way the platform's spawner does.
///
/// A diagnostic, not the spawn's own lookup: Windows also consults the
/// application directory and the system directories ahead of PATH, so `None`
/// here means "not on PATH", never "unspawnable". A caller that also ran the
/// command should believe the exit status over this answer and report both.
pub fn locate_program(program: &str) -> Option<PathBuf> {
    resolve_on_path(
        &std::env::var("PATH").unwrap_or_default(),
        PATH_SEP,
        EXECUTABLE_SUFFIX,
        program,
    )
}

/// PATH resolution as a pure function, taking the platform's separator and
/// executable suffix as arguments rather than reading them from `cfg`.
///
/// The parameters let the Windows algorithm be exercised on every host, so a
/// regression in it surfaces in the ordinary inner loop and on every platform's
/// lane rather than only where Windows is. That matters because the native
/// Windows lane in `publish-executor-protocol.yml` is gated on `main`: a
/// `#[cfg(windows)]`-only test would give its first verdict after a merge had
/// already landed. That lane does run these tests, which is what separately
/// confirms the platform's own answer for a bare program name is the one
/// modelled here.
fn resolve_on_path(
    path: &str,
    separator: char,
    executable_suffix: &str,
    program: &str,
) -> Option<PathBuf> {
    // A name carrying a path component is never searched for: the spawner uses
    // it as written, so reporting a PATH entry instead would name a directory
    // that had nothing to do with what ran.
    let given = Path::new(program);
    if given
        .parent()
        .is_some_and(|parent| !parent.as_os_str().is_empty())
    {
        return given.is_file().then(|| given.to_path_buf());
    }
    // Mirrors `CreateProcessW`: the suffix is appended only when the name
    // carries no extension of its own.
    let file_name = if executable_suffix.is_empty() || program.contains('.') {
        program.to_string()
    } else {
        format!("{program}{executable_suffix}")
    };
    path.split(separator)
        .filter(|entry| !entry.is_empty())
        .map(|entry| Path::new(entry).join(&file_name))
        .find(|candidate| candidate.is_file())
}

/// The user's home directory, as the platform names it.
pub fn user_home() -> String {
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

/// The default shell for this user, used both to probe for a login PATH and to
/// open interactive terminals.
pub fn default_shell() -> String {
    std::env::var("SHELL")
        .or_else(|_| std::env::var("COMSPEC")) // Windows shell env var
        .unwrap_or_else(|_| {
            if cfg!(windows) {
                // Prefer PowerShell if available, fall back to cmd
                if std::path::Path::new(
                    "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
                )
                .exists()
                {
                    "powershell.exe".to_string()
                } else {
                    "cmd.exe".to_string()
                }
            } else {
                "/bin/bash".to_string()
            }
        })
}

#[cfg(not(windows))]
fn compose_unix_path(home: &str, shell_path: Option<&str>, inherited_path: &str) -> String {
    let user_paths = format!(
        "{home}/.claude/local/bin:{home}/.bun/bin:{home}/.local/bin:{home}/.npm/bin:{home}/.yarn/bin:{home}/.cargo/bin:/usr/local/bin:/opt/homebrew/bin"
    );
    let mut paths = vec![user_paths];
    if let Some(shell_path) = shell_path.filter(|path| !path.is_empty()) {
        paths.push(shell_path.to_string());
    }
    if !inherited_path.is_empty() {
        paths.push(inherited_path.to_string());
    }
    paths.push("/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin".to_string());
    paths.join(":")
}

/// A PATH for finding CLI tools on THIS machine: the common install locations
/// under this user's home, the PATH their login shell resolves, the PATH this
/// process inherited, and the standard system directories. Cached after the
/// first call, which is the only one that pays for the login-shell probe.
pub fn user_path() -> &'static str {
    USER_PATH.get_or_init(|| {
        let home = user_home();

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
            compose_unix_path(
                &home,
                resolve_user_shell_path().as_deref(),
                &std::env::var("PATH").unwrap_or_default(),
            )
        }
    })
}

/// The host-owned bin directory (`<cairn_home>/bin`) that holds Cairn's own tool
/// shims (`cairn`, `jj`, `bun`, `uv`). Keyed off the resolved Cairn home, so a
/// dev instance's separate home gets its own shim dir.
pub fn cairn_bin_dir() -> PathBuf {
    crate::paths::cairn_home().join("bin")
}

/// Compose an agent-shell PATH by placing `bin_dir` ahead of `user_path`.
fn prepend_cairn_bin(bin_dir: &Path, user_path: &str) -> String {
    format!("{}{}{}", bin_dir.display(), PATH_SEP, user_path)
}

/// The PATH every command Cairn runs resolves against: the host-owned shim dir
/// ahead of [`user_path`]. Prepending the shim dir is what makes `cairn`, `jj`,
/// `bun`, and `uv` resolve regardless of how the user's own PATH is configured,
/// and lets Cairn's bundled copies win over a system install.
pub fn agent_shell_path() -> String {
    prepend_cairn_bin(&cairn_bin_dir(), user_path())
}

/// Adopt [`agent_shell_path`] as this process's own PATH, so every command it
/// spawns inherits it without each spawn site restating it. Returns the composed
/// value so the caller can log what its machine resolved.
///
/// Call this once, early, before any threads that read the environment exist.
pub fn install_process_path() -> String {
    let path = agent_shell_path();
    std::env::set_var("PATH", &path);
    path
}

#[cfg(not(windows))]
fn resolve_user_shell_path() -> Option<String> {
    let user_shell = default_shell();
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
fn parse_path_from_env_output(output: &str) -> Option<String> {
    output
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("PATH=").filter(|path| !path.is_empty()))
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepend_cairn_bin_places_bin_dir_first() {
        let composed = prepend_cairn_bin(
            Path::new("/home/u/.cairn/bin"),
            &format!("/usr/local/bin{PATH_SEP}/usr/bin"),
        );
        assert!(composed.starts_with(&format!("/home/u/.cairn/bin{PATH_SEP}")));
        assert!(composed.ends_with(&format!("/usr/local/bin{PATH_SEP}/usr/bin")));
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_keeps_standard_dirs_with_restricted_inherited_path() {
        let path = compose_unix_path("/home/dev", None, "/restricted/gui/bin");
        assert!(path.starts_with("/home/dev/.claude/local/bin:"));
        assert!(path.contains(":/restricted/gui/bin:"));
        assert!(path.ends_with("/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"));
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_includes_standard_dirs_when_inherited_path_is_empty() {
        let path = compose_unix_path("/home/dev", None, "");
        assert!(path.starts_with("/home/dev/.claude/local/bin:"));
        assert!(path.ends_with("/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"));
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_path_keeps_resolved_shell_and_inherited_entries() {
        let path = compose_unix_path(
            "/home/dev",
            Some("/shell/bin:/shell/sbin"),
            "/inherited/bin",
        );
        assert!(path.contains(":/shell/bin:/shell/sbin:/inherited/bin:"));
        assert!(path.ends_with("/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"));
    }

    /// A toolchain a user installed under their home is on the composed PATH
    /// even when the process inherited nothing useful — the whole reason a
    /// daemon or an ssh session can still run `bun i`.
    #[cfg(not(windows))]
    #[test]
    fn unix_path_carries_user_toolchain_dirs_a_daemon_never_inherits() {
        let path = compose_unix_path("/home/dev", None, "/usr/bin:/bin");
        for expected in [
            "/home/dev/.bun/bin",
            "/home/dev/.cargo/bin",
            "/home/dev/.local/bin",
            "/opt/homebrew/bin",
        ] {
            assert!(path.contains(expected), "composed PATH lacks {expected}");
        }
    }

    /// The Windows lookup a bare `cargo` actually gets: the spawner appends
    /// `.exe`, so a PATH entry holding `cargo.exe` is a hit even though nothing
    /// on that PATH is named `cargo`. Getting this wrong reports a toolchain as
    /// absent on the one platform Cairn cannot compile a test for.
    #[test]
    fn windows_resolution_appends_the_executable_suffix_to_a_bare_name() {
        let temp = tempfile::tempdir().unwrap();
        let cargo_bin = temp.path().join("cargo/bin");
        std::fs::create_dir_all(&cargo_bin).unwrap();
        std::fs::write(cargo_bin.join("cargo.exe"), b"").unwrap();

        let path = format!("C:\\windows\\system32;{}", cargo_bin.display());
        assert_eq!(
            resolve_on_path(&path, ';', ".exe", "cargo"),
            Some(cargo_bin.join("cargo.exe"))
        );
    }

    /// The Unix lookup is literal: a `cargo.exe` sitting on a Unix PATH is not
    /// the `cargo` a spawn would find, and claiming otherwise would advertise a
    /// toolchain that cannot run.
    #[test]
    fn unix_resolution_never_invents_an_executable_suffix() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("cargo.exe"), b"").unwrap();

        let path = format!("/usr/bin:{}", temp.path().display());
        assert_eq!(resolve_on_path(&path, ':', "", "cargo"), None);
    }

    /// A name that already carries an extension is used as written, matching
    /// `CreateProcessW`, which appends its suffix only to an extensionless name.
    #[test]
    fn windows_resolution_leaves_an_explicit_extension_alone() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("where.exe"), b"").unwrap();

        let path = temp.path().display().to_string();
        assert_eq!(
            resolve_on_path(&path, ';', ".exe", "where.exe"),
            Some(temp.path().join("where.exe"))
        );
    }

    /// PATH order decides, because it decides for the spawner. A diagnostic that
    /// named a later entry would point an operator at the wrong install.
    #[test]
    fn resolution_returns_the_first_path_entry_that_holds_the_program() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        std::fs::write(first.join("cargo.exe"), b"").unwrap();
        std::fs::write(second.join("cargo.exe"), b"").unwrap();

        let path = format!("{};{}", first.display(), second.display());
        assert_eq!(
            resolve_on_path(&path, ';', ".exe", "cargo"),
            Some(first.join("cargo.exe"))
        );
    }

    /// The composed Windows PATH names install locations that need not exist;
    /// an absent directory is skipped, not treated as a resolution failure.
    #[test]
    fn resolution_skips_empty_and_absent_path_entries() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("cargo.exe"), b"").unwrap();

        let path = format!(";C:\\Users\\absent\\.cargo\\bin;;{}", temp.path().display());
        assert_eq!(
            resolve_on_path(&path, ';', ".exe", "cargo"),
            Some(temp.path().join("cargo.exe"))
        );
    }

    /// A directory named like the program is not the program. `is_file` is what
    /// keeps `.../cargo/` from being reported as a resolved `cargo`.
    #[test]
    fn resolution_ignores_a_directory_sharing_the_program_name() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("cargo")).unwrap();

        let path = temp.path().display().to_string();
        assert_eq!(resolve_on_path(&path, ':', "", "cargo"), None);
    }

    /// A program named by path is used as given. Searching PATH for it would
    /// report a directory that had no part in running it.
    #[test]
    fn a_program_named_by_path_is_not_searched_for_on_path() {
        let temp = tempfile::tempdir().unwrap();
        let elsewhere = temp.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        let program = elsewhere.join("cargo");
        std::fs::write(&program, b"").unwrap();

        let program = program.display().to_string();
        assert_eq!(
            resolve_on_path("/usr/bin", ':', "", &program),
            Some(PathBuf::from(&program))
        );
        assert_eq!(
            resolve_on_path("", ':', "", &format!("{program}-absent")),
            None
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn parse_path_from_env_output_picks_path_line() {
        let output = "SHELL=/bin/zsh\nPATH=/usr/local/bin:/usr/bin\nHOME=/Users/example\n";
        assert_eq!(
            parse_path_from_env_output(output).as_deref(),
            Some("/usr/local/bin:/usr/bin")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn parse_path_from_env_output_ignores_non_path_lines() {
        let output = "SHELL=/bin/zsh\nCAIRN_PATH_HINT=/tmp/bin\nHOME=/Users/example\n";
        assert_eq!(parse_path_from_env_output(output), None);
    }

    #[cfg(not(windows))]
    #[test]
    fn parse_path_from_env_output_rejects_empty_path() {
        assert_eq!(parse_path_from_env_output("PATH=\n"), None);
    }

    #[cfg(not(windows))]
    #[test]
    fn parse_path_from_env_output_uses_last_path_line() {
        let output = "PATH=/minimal\nnoise from shell rc\nPATH=/shell/configured:/usr/bin\n";
        assert_eq!(
            parse_path_from_env_output(output).as_deref(),
            Some("/shell/configured:/usr/bin")
        );
    }
}
