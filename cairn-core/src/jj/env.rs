//! jj subprocess driver (`JjEnv`), repo/file probes, per-project store
//! provisioning, and populate/auto-track fileset translation.
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use crate::mcp::git::GitAuthor;

/// Fallback identity used when no per-call author is supplied. Per-commit author
/// is injected via `--config user.{name,email}=…` on each seal.
const JJ_DEFAULT_USER_NAME: &str = "Cairn Agent";
const JJ_DEFAULT_USER_EMAIL: &str = "agent@cairn.local";
pub(crate) const JJ_DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const JJ_NETWORK_TIMEOUT: Duration = Duration::from_secs(600);
const PIPE_READER_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

fn spawn_pipe_reader<R: Read + Send + 'static>(
    mut reader: R,
) -> (
    std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    thread::JoinHandle<()>,
) {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = reader.read_to_end(&mut bytes).map(|_| bytes);
        let _ = tx.send(result);
    });
    (rx, handle)
}

fn finish_pipe_reader(
    receiver: std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    handle: thread::JoinHandle<()>,
    ctx: &str,
    stream: &str,
) -> Result<Vec<u8>, String> {
    match receiver.recv_timeout(PIPE_READER_JOIN_TIMEOUT) {
        Ok(result) => {
            handle
                .join()
                .map_err(|_| format!("{ctx}: {stream} reader panicked"))?;
            result.map_err(|error| format!("{ctx}: read {stream}: {error}"))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "{ctx}: {stream} pipe remained open more than {}s after child exit; reader detached",
            PIPE_READER_JOIN_TIMEOUT.as_secs()
        )),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            let _ = handle.join();
            Err(format!("{ctx}: {stream} reader disconnected"))
        }
    }
}

/// Run a subprocess with drained output pipes and a hard deadline. The child is
/// placed in its own process group on Unix so timeout cleanup also reaches git,
/// ssh, credential helpers, and any other descendants spawned by jj.
pub(crate) fn bounded_command_output(
    command: &mut Command,
    timeout: Duration,
    ctx: &str,
) -> Result<Output, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let mut child = command.spawn().map_err(|e| format!("{ctx}: {e}"))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (stdout_rx, stdout_reader) = spawn_pipe_reader(stdout);
    let (stderr_rx, stderr_reader) = spawn_pipe_reader(stderr);

    let deadline = Instant::now() + timeout;
    let (status, timed_out) = loop {
        match child.try_wait().map_err(|e| format!("{ctx}: {e}"))? {
            Some(status) => break (status, false),
            None if Instant::now() >= deadline => {
                #[cfg(unix)]
                unsafe {
                    libc::kill(-(child.id() as i32), libc::SIGKILL);
                }
                let _ = child.kill();
                let status = child.wait().map_err(|e| format!("{ctx}: {e}"))?;
                break (status, true);
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    };
    let stdout = finish_pipe_reader(stdout_rx, stdout_reader, ctx, "stdout")?;
    let stderr = finish_pipe_reader(stderr_rx, stderr_reader, ctx, "stderr")?;
    if timed_out {
        return Err(format!(
            "{ctx} timed out after {}s and was killed",
            timeout.as_secs_f64()
        ));
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

/// Drives a bundled, non-interactive `jj` binary.
#[derive(Clone)]
pub struct JjEnv {
    bin: String,
    config_path: PathBuf,
}

#[cfg(test)]
fn jj_subprocess_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl JjEnv {
    #[cfg(test)]
    pub(crate) fn with_binary(bin: impl Into<String>, config_dir: &Path) -> Self {
        Self {
            bin: bin.into(),
            config_path: config_dir.join("jj").join("config.toml"),
        }
    }

    /// Resolve the jj binary and the managed config path. Binary precedence:
    /// `CAIRN_JJ_BIN` (test/override) → the bundled sidecar path → PATH `jj`.
    pub fn resolve(bundled_bin: &str, config_dir: &Path) -> Self {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| Self::resolve_bundled_or_path(bundled_bin));
        Self {
            bin,
            config_path: config_dir.join("jj").join("config.toml"),
        }
    }

    fn resolve_bundled_or_path(bundled_bin: &str) -> String {
        let bundled_bin = bundled_bin.trim();
        if bundled_bin.is_empty() {
            return "jj".to_string();
        }

        match bounded_command_output(
            crate::env::command(bundled_bin).arg("--version"),
            JJ_DEFAULT_TIMEOUT,
            "bundled jj --version",
        ) {
            Ok(output) if output.status.success() => bundled_bin.to_string(),
            Ok(output) => {
                log::warn!(
                    "Bundled jj at `{bundled_bin}` failed --version with status {}; falling back to PATH jj",
                    output.status
                );
                "jj".to_string()
            }
            Err(error) => {
                log::warn!(
                    "Bundled jj at `{bundled_bin}` could not be spawned ({error}); falling back to PATH jj"
                );
                "jj".to_string()
            }
        }
    }

    /// Write the managed jj config once if absent (never clobbers user edits).
    fn ensure_config(&self) {
        if self.config_path.exists() {
            return;
        }
        if let Some(parent) = self.config_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                log::warn!("Failed to create jj config dir {:?}: {e}", parent);
                return;
            }
        }
        let body = format!(
            "ui.paginate = \"never\"\n[user]\nname = \"{JJ_DEFAULT_USER_NAME}\"\nemail = \"{JJ_DEFAULT_USER_EMAIL}\"\n"
        );
        if let Err(e) = std::fs::write(&self.config_path, body) {
            log::warn!("Failed to write jj config {:?}: {e}", self.config_path);
        }
    }

    /// A `jj` command rooted at `cwd`, wired for non-interactive use.
    fn cmd(&self, cwd: &Path) -> Command {
        self.ensure_config();
        let mut c = crate::env::command(&self.bin);
        c.current_dir(cwd)
            .env("JJ_CONFIG", &self.config_path)
            .env("EDITOR", "true")
            .env("JJ_EDITOR", "true")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "true");
        c
    }

    /// The env a bare `jj` shell command needs to behave like a managed
    /// [`JjEnv::cmd`] invocation: the Cairn-managed config path and a
    /// non-interactive editor. Exactly the env `cmd` injects, so a bare `jj` run
    /// through the run tool is byte-identical to managed jj (same managed
    /// fallback identity, same non-interactive editor) instead of writing
    /// unpushable empty-committer commits. Ensures the managed config file exists
    /// first, mirroring `cmd`, so `JJ_CONFIG` never points at a missing file.
    pub(crate) fn shell_env(&self) -> Vec<(String, String)> {
        self.ensure_config();
        vec![
            (
                "JJ_CONFIG".into(),
                self.config_path.to_string_lossy().into_owned(),
            ),
            ("EDITOR".into(), "true".into()),
            ("JJ_EDITOR".into(), "true".into()),
            ("GIT_TERMINAL_PROMPT".into(), "0".into()),
            ("GIT_ASKPASS".into(), "true".into()),
        ]
    }

    /// The resolved real jj binary path (bundled sidecar, `CAIRN_JJ_BIN`
    /// override, or PATH `jj`). Exposed so the agent-shell env can point the
    /// intercept shim's `CAIRN_JJ_BIN` at the same binary managed jj runs.
    pub fn binary_path(&self) -> &str {
        &self.bin
    }

    /// Per-call author override as repeated global `--config user.{name,email}=…`
    /// args (placed before the subcommand). jj fixes a commit's author when its
    /// working-copy commit is created, so passing this on every seal keeps a
    /// workspace's sealed commits authored consistently.
    pub(crate) fn author_args(author: Option<&GitAuthor>) -> Vec<String> {
        match author {
            Some(a) => vec![
                "--config".into(),
                format!("user.name={}", a.name),
                "--config".into(),
                format!("user.email={}", a.email),
            ],
            None => Vec::new(),
        }
    }

    /// Run a jj command, returning raw stdout bytes or a contextual error.
    fn run_bytes(&self, cwd: &Path, args: &[&str], ctx: &str) -> Result<Vec<u8>, String> {
        self.run_bytes_with_timeout(cwd, args, ctx, JJ_DEFAULT_TIMEOUT)
    }

    fn run_bytes_with_timeout(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: &str,
        timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        #[cfg(test)]
        let _guard = jj_subprocess_lock()
            .lock()
            .expect("jj subprocess test lock poisoned");

        let out = bounded_command_output(self.cmd(cwd).args(args), timeout, ctx)?;
        if !out.status.success() {
            return Err(format!(
                "{ctx} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(out.stdout)
    }

    /// Run a jj command, returning trimmed stdout or a contextual error.
    pub(crate) fn run(&self, cwd: &Path, args: &[&str], ctx: &str) -> Result<String, String> {
        let out = self.run_bytes(cwd, args, ctx)?;
        Ok(String::from_utf8_lossy(&out).trim().to_string())
    }

    pub(crate) fn run_with_timeout(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: &str,
        timeout: Duration,
    ) -> Result<String, String> {
        let out = self.run_bytes_with_timeout(cwd, args, ctx, timeout)?;
        Ok(String::from_utf8_lossy(&out).trim().to_string())
    }
}

/// Whether `dir` is a jj repo/workspace root (carries a `.jj`). The ground-truth
/// signal the commit barrier dispatches on.
pub fn is_jj_dir(dir: &Path) -> bool {
    dir.join(".jj").is_dir()
}

/// Read a file's bytes from `rev` without consulting or snapshotting the working
/// copy. `path` is a repo-relative path (or fileset expression understood by jj).
pub fn file_show(jj: &JjEnv, cwd: &Path, rev: &str, path: &str) -> Result<Vec<u8>, String> {
    jj.run_bytes(
        cwd,
        &["file", "show", "-r", rev, "--ignore-working-copy", path],
        "jj file show",
    )
}

/// List repo-relative files visible at `rev`, optionally scoped to `path`.
pub fn file_list(jj: &JjEnv, cwd: &Path, rev: &str, path: &str) -> Result<Vec<String>, String> {
    let mut args = vec!["file", "list", "-r", rev, "--ignore-working-copy"];
    if !path.is_empty() {
        args.push(path);
    }
    Ok(jj
        .run(cwd, &args, "jj file list")?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// The shared jj store directory for a project, under the Cairn home. One store
/// per project repo, named from the repo basename plus a short hash of its
/// absolute path so distinct repos that share a basename never collide.
pub fn project_store_dir(config_dir: &Path, repo_path: &Path) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let base = repo_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("project");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    repo_path.to_string_lossy().hash(&mut hasher);
    config_dir
        .join("jj-stores")
        .join(format!("{base}-{:016x}", hasher.finish()))
}

/// Create the shared per-project jj store if absent: a Cairn-managed jj repo
/// whose git backend is the project's existing `.git`. The user's checkout is
/// never touched and sealed commits land in the project's object database.
pub fn ensure_project_store(
    jj: &JjEnv,
    store_dir: &Path,
    project_repo: &Path,
) -> Result<(), String> {
    if !is_jj_dir(store_dir) {
        if let Some(parent) = store_dir.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create jj store parent dir: {e}"))?;
        }
        let cwd = store_dir.parent().unwrap_or(store_dir);
        jj.run(
            cwd,
            &[
                "git",
                "init",
                "--git-repo",
                &project_repo.to_string_lossy(),
                &store_dir.to_string_lossy(),
            ],
            "jj git init --git-repo",
        )?;
    }
    // Always sync the backing git repo into the store. `jj git init` imports on
    // creation, but an already-existing store is otherwise frozen at the refs it
    // last saw: a base ref that advanced since then would not resolve when adding
    // a new workspace (`Revision <sha> doesn't exist`), so every later job on a
    // jj-managed project would fail to provision once the project git moved.
    import_git(jj, store_dir)?;
    Ok(())
}

/// Import the backing git repo's refs and commits into the shared store, so a
/// base ref that advanced since the store was created resolves.
fn import_git(jj: &JjEnv, store_dir: &Path) -> Result<(), String> {
    jj.run(store_dir, &["git", "import"], "jj git import")
        .map(|_| ())
}

/// Fetch a remote into the shared store, advancing its remote-tracking bookmarks
/// (`<branch>@<remote>`) to the remote's current tips. Used to bring an
/// externally-advanced default branch into the store independent of the project
/// checkout's branch, so a sibling can rebase onto `<default>@origin`. Mirrors
/// `import_git`: a one-liner over the store's backing git.
pub(crate) fn fetch_remote(jj: &JjEnv, store_dir: &Path, remote: &str) -> Result<(), String> {
    jj.run_with_timeout(
        store_dir,
        &["git", "fetch", "--remote", remote],
        "jj git fetch",
        JJ_NETWORK_TIMEOUT,
    )
    .map(|_| ())
}

/// Wrap a repo-relative path as a jj fileset string literal so paths containing
/// fileset metacharacters — `(` `)` `|` `&` `~` `:`, whitespace, etc. (e.g. a
/// Next.js `(app)` route-group directory) — are matched literally instead of
/// being parsed as a fileset expression. jj positional path arguments to
/// `commit`/`squash`/`file untrack` are fileset expressions, not literal paths,
/// so an unquoted `(app)` is read as a grouping operator and the parse fails.
/// jj double-quoted strings use backslash escaping, so `\` and `"` are escaped.
pub(crate) fn quote_fileset(path: &str) -> String {
    let escaped = path.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Translate one populate glob pattern into jj `snapshot.auto-track` exclude
/// filesets. Populate matches with `literal_separator(false)` (so `*` crosses
/// `/`) against repo-relative paths; the jj exclusion must be at least as broad,
/// so each pattern is anchored with a leading `**/` to match at any depth
/// (over-exclusion is safe — it only keeps more new files untracked). A trailing
/// slash marks a directory: exclude both its subtree (`**/<dir>/**`) and the
/// entry itself (`**/<dir>`, e.g. a symlinked dir, which appears as one path).
fn populate_pattern_filesets(pattern: &str) -> Vec<String> {
    let is_dir = pattern.ends_with('/');
    let body = pattern.trim_end_matches('/');
    if body.is_empty() {
        return Vec::new();
    }
    if is_dir {
        vec![
            format!("glob:\"**/{body}/**\""),
            format!("glob:\"**/{body}\""),
        ]
    } else {
        vec![format!("glob:\"**/{body}\"")]
    }
}

/// Build the `snapshot.auto-track` revset that tracks everything EXCEPT the
/// populate-matched paths (plus any `extra_paths` exact paths fed back by the
/// security backstop after a glob-translation miss). Returns `None` when there
/// is nothing to exclude, so the caller leaves jj's `all()` default untouched.
pub(crate) fn populate_auto_track_expr(
    config: &crate::config::project_settings::PopulateConfig,
    extra_paths: &[String],
) -> Option<String> {
    let mut filesets: Vec<String> = Vec::new();
    for pattern in config.copy.iter().chain(config.symlink.iter()) {
        filesets.extend(populate_pattern_filesets(pattern));
    }
    for path in extra_paths {
        let trimmed = path.trim_matches('/');
        if !trimmed.is_empty() {
            filesets.push(quote_fileset(trimmed));
        }
    }
    if filesets.is_empty() {
        return None;
    }
    Some(format!("all() ~ ({})", filesets.join(" | ")))
}

/// Set the shared store's `snapshot.auto-track` so jj's working-copy snapshot
/// never auto-tracks explicitly-populated gitignored content. Repo-scoped
/// (`--repo`), so it applies to every workspace over the store and is idempotent
/// under concurrent job provisioning. MUST run before populate copies files in:
/// jj auto-tracks a new file on the first snapshot after it appears, and a later
/// rule cannot un-track it. `extra_paths` lets the backstop extend the exclusion
/// with exact leaked paths. No-op when there is nothing to exclude.
pub(crate) fn set_populate_auto_track(
    jj: &JjEnv,
    store_dir: &Path,
    config: &crate::config::project_settings::PopulateConfig,
    extra_paths: &[String],
) -> Result<(), String> {
    let Some(expr) = populate_auto_track_expr(config, extra_paths) else {
        return Ok(());
    };
    jj.run(
        store_dir,
        &["config", "set", "--repo", "snapshot.auto-track", &expr],
        "jj config set snapshot.auto-track",
    )
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(jj)]
    fn managed_command_times_out_and_kills_child() {
        use std::os::unix::fs::PermissionsExt;

        let home = TempDir::new().unwrap();
        let script = home.path().join("slow-jj");
        let pid_file = home.path().join("pid");
        std::fs::write(
            &script,
            format!("#!/bin/sh\necho $$ > {}\nsleep 30\n", pid_file.display()),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let jj = JjEnv {
            bin: script.to_string_lossy().into_owned(),
            config_path: home.path().join("config.toml"),
        };

        let started = Instant::now();
        let error = jj
            .run_bytes_with_timeout(home.path(), &[], "slow jj", Duration::from_millis(500))
            .unwrap_err();
        assert!(crate::jj::is_jj_timeout_error(&error), "{error}");
        assert!(started.elapsed() < Duration::from_secs(3));
        let pid: i32 = std::fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "timed-out child is still alive"
        );
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(jj)]
    fn managed_command_bounds_pipe_reader_shutdown() {
        use std::os::unix::fs::PermissionsExt;

        let home = TempDir::new().unwrap();
        let script = home.path().join("leaky-jj");
        std::fs::write(&script, "#!/bin/sh\n(sleep 30) &\nexit 0\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let jj = JjEnv {
            bin: script.to_string_lossy().into_owned(),
            config_path: home.path().join("config.toml"),
        };

        let started = Instant::now();
        let error = jj
            .run_bytes_with_timeout(home.path(), &[], "leaky jj", Duration::from_millis(500))
            .unwrap_err();
        assert!(error.contains("pipe remained open"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(jj)]
    fn managed_command_disables_git_prompts() {
        use std::os::unix::fs::PermissionsExt;

        let home = TempDir::new().unwrap();
        let script = home.path().join("env-jj");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s:%s' \"$GIT_TERMINAL_PROMPT\" \"$GIT_ASKPASS\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let jj = JjEnv {
            bin: script.to_string_lossy().into_owned(),
            config_path: home.path().join("config.toml"),
        };

        assert_eq!(jj.run(home.path(), &[], "env jj").unwrap(), "0:true");
    }

    #[test]
    #[serial_test::serial(jj)]
    fn resolve_falls_back_to_path_when_bundled_jj_is_unspawnable() {
        let original = std::env::var("CAIRN_JJ_BIN").ok();
        std::env::remove_var("CAIRN_JJ_BIN");

        let home = TempDir::new().unwrap();
        let jj = JjEnv::resolve("/definitely/not/a/spawnable/jj", home.path());

        if let Some(value) = original {
            std::env::set_var("CAIRN_JJ_BIN", value);
        }

        assert_eq!(jj.bin, "jj");
    }

    #[test]
    #[serial_test::serial(jj)]
    fn resolve_keeps_explicit_env_override() {
        let original = std::env::var("CAIRN_JJ_BIN").ok();
        std::env::set_var("CAIRN_JJ_BIN", "/explicit/jj");

        let home = TempDir::new().unwrap();
        let jj = JjEnv::resolve("/definitely/not/a/spawnable/jj", home.path());

        match original {
            Some(value) => std::env::set_var("CAIRN_JJ_BIN", value),
            None => std::env::remove_var("CAIRN_JJ_BIN"),
        }

        assert_eq!(jj.bin, "/explicit/jj");
    }
}
