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

/// Whether `execve` would run `candidate` for this process: a regular file the
/// *effective* user may execute. An execute bit for some other class of user is
/// not enough, and `execvp` passes over such a file and keeps walking PATH.
#[cfg(not(windows))]
fn executable_by_this_process(candidate: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    if !std::fs::metadata(candidate).is_ok_and(|meta| meta.is_file()) {
        return false;
    }
    let Ok(path) = std::ffi::CString::new(candidate.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `path` is a valid NUL-terminated string for the duration of the call.
    unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), libc::X_OK, libc::AT_EACCESS) == 0 }
}

/// What a PATH walk concluded.
#[cfg(not(windows))]
enum PathWalk {
    Found(PathBuf),
    NotFound,
    /// The walk reached a relative or empty entry (empty means the current
    /// directory) before any match, so the answer depends on a working
    /// directory this walk does not know.
    DependsOnWorkingDirectory,
}

/// `execvp`'s walk: entries in order, the first file this process may execute.
/// `relative_base` is the directory relative and empty entries are read
/// against; `None` stops the walk at the first such entry instead of guessing.
#[cfg(not(windows))]
fn walk_path(program: &str, search_path: &str, relative_base: Option<&Path>) -> PathWalk {
    for entry in search_path.split(PATH_SEP) {
        let entry = Path::new(if entry.is_empty() { "." } else { entry });
        let directory = if entry.is_absolute() {
            entry.to_path_buf()
        } else {
            match relative_base {
                Some(base) => base.join(entry),
                None => return PathWalk::DependsOnWorkingDirectory,
            }
        };
        let candidate = directory.join(program);
        if executable_by_this_process(&candidate) {
            return PathWalk::Found(candidate);
        }
    }
    PathWalk::NotFound
}

/// Where a lookup of `program` under `search_path`, made from this process and
/// its working directory, finds it: the answer `which` gives. A name carrying a
/// path component is taken as written. `None` means it is not found.
#[cfg(not(windows))]
pub fn resolve_spawn_program(program: &str, search_path: &str) -> Option<PathBuf> {
    if program.contains('/') {
        let given = Path::new(program);
        return executable_by_this_process(given).then(|| given.to_path_buf());
    }
    let cwd = std::env::current_dir().ok()?;
    match walk_path(program, search_path, Some(&cwd)) {
        PathWalk::Found(found) => Some(found),
        PathWalk::NotFound | PathWalk::DependsOnWorkingDirectory => None,
    }
}

/// A command that runs `program` with `search_path` as its PATH.
///
/// On Unix a bare name is resolved up front, so the spawn names an absolute
/// program and stays on `posix_spawn`; `argv[0]` keeps the name the caller
/// gave. Rust's std can only use `posix_spawn` when it knows what to execute
/// before spawning; a bare name under a PATH the *child* is given has to be
/// searched for in the child, so std forks, and on macOS `fork()` holds every
/// malloc zone lock for as long as it takes to copy the address space, which in
/// a long-lived runner is long enough to stall every thread that allocates.
///
/// The resolution must pick exactly what the child's own `execvp` would, and
/// the child's working directory is set by the caller after this returns. So
/// only absolute PATH entries are walked: if a relative or empty entry comes
/// before any match, the name is left bare and the child resolves it itself,
/// paying the fork to stay correct. A name with a path component, or one that
/// is found nowhere, is also left as given.
pub fn command_on_path(program: &str, search_path: &str) -> std::process::Command {
    #[cfg(not(windows))]
    let mut command = match (!program.contains('/')).then(|| walk_path(program, search_path, None))
    {
        Some(PathWalk::Found(resolved)) => {
            use std::os::unix::process::CommandExt;
            let mut command = std::process::Command::new(resolved);
            command.arg0(program);
            command
        }
        _ => std::process::Command::new(program),
    };
    #[cfg(windows)]
    let mut command = std::process::Command::new(program);
    command.env("PATH", search_path);
    command
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

/// The PATH an executor gives every command it runs: its own home's bin dir
/// first, then [`agent_shell_path`].
///
/// The executor's home is the one its launch named, not [`crate::paths::cairn_home`].
/// A resident executor is started by a service manager with `--config` and no
/// environment, so `cairn_home()` answers `~/.cairn` — a directory the executor
/// does not own, holding whatever `cairn` a person once installed there at
/// whatever version. The runner installs the CLI matching the executor's own
/// publication into `<executor home>/bin`, so that directory has to lead, or a
/// stale CLI that parses some subcommands and not others decides what a cell's
/// `cairn` is.
///
/// The default home's bin dir stays on the PATH behind it, because another Cairn
/// install on the same machine may hold `jj`/`bun`/`uv` shims a cell resolves;
/// it just never decides which `cairn` runs. A colocated executor's home is the
/// runner's own, so for it this is exactly [`agent_shell_path`].
pub fn executor_shell_path(executor_home: &Path) -> String {
    compose_executor_path(&executor_home.join("bin"), &cairn_bin_dir(), user_path())
}

fn compose_executor_path(executor_bin: &Path, default_bin: &Path, user_path: &str) -> String {
    if executor_bin == default_bin {
        prepend_cairn_bin(executor_bin, user_path)
    } else {
        prepend_cairn_bin(executor_bin, &prepend_cairn_bin(default_bin, user_path))
    }
}

/// Adopt [`executor_shell_path`] as this process's own PATH. The same
/// single-threaded precondition as [`install_process_path`] applies.
pub fn install_executor_process_path(executor_home: &Path) -> String {
    let path = executor_shell_path(executor_home);
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

    /// The bgsrv-ub shape: a resident executor whose home is `~/.cairn-exec`
    /// on a machine where `~/.cairn/bin/cairn` is somebody's old CLI. The
    /// runner-installed CLI's directory must come first, or the stale one runs.
    #[test]
    fn an_executor_home_bin_leads_ahead_of_the_default_cairn_bin() {
        let composed = compose_executor_path(
            Path::new("/home/dev/.cairn-exec/bin"),
            Path::new("/home/dev/.cairn/bin"),
            "/home/dev/.local/bin",
        );
        let entries: Vec<&str> = composed.split(PATH_SEP).collect();
        assert_eq!(
            entries,
            [
                "/home/dev/.cairn-exec/bin",
                "/home/dev/.cairn/bin",
                "/home/dev/.local/bin"
            ]
        );
    }

    /// A colocated executor shares the runner's home, and its PATH is the
    /// runner's agent PATH with no duplicated entry.
    #[test]
    fn a_colocated_executor_path_is_the_agent_path() {
        let bin = Path::new("/home/dev/.cairn/bin");
        assert_eq!(
            compose_executor_path(bin, bin, "/usr/bin"),
            prepend_cairn_bin(bin, "/usr/bin")
        );
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
    fn write_program(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    /// `execvp` passes over a file it cannot execute and keeps walking PATH, so
    /// the in-process resolution must as well, or it would name a different
    /// program than the forked lookup it replaces.
    #[cfg(not(windows))]
    #[test]
    fn spawn_resolution_skips_files_without_an_execute_bit() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        write_program(&first.join("tool"), 0o644);
        write_program(&second.join("tool"), 0o755);
        std::fs::create_dir_all(first.join("dir-tool")).unwrap();

        let path = format!("{}::{}", first.display(), second.display());
        assert_eq!(
            resolve_spawn_program("tool", &path),
            Some(second.join("tool"))
        );
        assert_eq!(resolve_spawn_program("dir-tool", &path), None);
        assert_eq!(resolve_spawn_program("absent", &path), None);
        let named = second.join("tool").display().to_string();
        assert_eq!(
            resolve_spawn_program(&named, ""),
            Some(PathBuf::from(&named))
        );
    }

    /// A file this user cannot execute is passed over even when some other
    /// class of user holds an execute bit on it, as `execvp` does after EACCES.
    #[cfg(not(windows))]
    #[test]
    fn resolution_requires_execute_permission_for_this_user() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        // Other-execute only, and owned by this user: an ordinary user may not
        // run it, while root may run any file carrying some execute bit.
        for (dir, mode, says) in [(&first, 0o645, "first"), (&second, 0o755, "second")] {
            let tool = dir.join("tool");
            std::fs::write(&tool, format!("#!/bin/sh\nprintf {says}\n")).unwrap();
            std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let path = format!("{}:{}", first.display(), second.display());
        let (expected_dir, expected_output) = if unsafe { libc::geteuid() } == 0 {
            (&first, "first")
        } else {
            (&second, "second")
        };

        assert_eq!(
            resolve_spawn_program("tool", &path),
            Some(expected_dir.join("tool"))
        );
        let output = command_on_path("tool", &path).output().unwrap();
        assert_eq!(String::from_utf8(output.stdout).unwrap(), expected_output);
    }

    /// Relative and empty PATH entries are read against the *child's* working
    /// directory, which the caller sets after the command is built. The
    /// resolver must not answer for them from this process's directory: the
    /// child has to find `bin/tool` and `./tool` under its own cwd ahead of
    /// an absolute fallback later in PATH.
    #[cfg(not(windows))]
    #[test]
    fn relative_path_entries_resolve_against_the_childs_directory() {
        let temp = tempfile::tempdir().unwrap();
        let child = temp.path().join("child");
        let fallback = temp.path().join("fallback");
        std::fs::create_dir_all(child.join("bin")).unwrap();
        std::fs::create_dir_all(&fallback).unwrap();
        for (path, says) in [
            (child.join("bin/tool"), "child-bin"),
            (child.join("tool"), "child-cwd"),
            (fallback.join("tool"), "fallback"),
        ] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::write(&path, format!("#!/bin/sh\nprintf {says}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let run = |search_path: String| {
            let output = command_on_path("tool", &search_path)
                .current_dir(&child)
                .output()
                .unwrap();
            String::from_utf8(output.stdout).unwrap()
        };

        assert_eq!(run(format!("bin:{}", fallback.display())), "child-bin");
        assert_eq!(run(format!(":{}", fallback.display())), "child-cwd");
        // An absolute entry ahead of any relative one decides on its own.
        assert_eq!(run(format!("{}:bin", fallback.display())), "fallback");
        assert_eq!(
            command_on_path("tool", &format!("{}:bin", fallback.display())).get_program(),
            fallback.join("tool").as_os_str()
        );
    }

    /// The resolved command runs the program it names while the child still
    /// sees the PATH it was given, and a name that resolves nowhere is left
    /// bare to fail exactly as an unresolved spawn would.
    #[cfg(not(windows))]
    #[test]
    fn command_on_path_resolves_the_program_under_the_given_path() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("probe-tool");
        std::fs::write(&script, b"#!/bin/sh\nprintf '%s' \"$PATH\"\n").unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let search_path = format!("{}:/usr/bin:/bin", temp.path().display());

        let mut command = command_on_path("probe-tool", &search_path);
        assert_eq!(command.get_program(), script.as_os_str());
        let output = command.output().unwrap();
        assert!(output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap(), search_path);

        let unresolved = command_on_path("no-such-tool-anywhere", &search_path);
        assert_eq!(unresolved.get_program(), "no-such-tool-anywhere");
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
