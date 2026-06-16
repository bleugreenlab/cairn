//! Process spawning service for external command execution.
//!
//! Abstracts process spawning to enable testing without real subprocesses.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;

use super::sandbox::SandboxPolicy;

/// Configuration for spawning a process.
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub env: HashMap<String, String>,
    pub capture_stdout: bool,
    pub capture_stderr: bool,
    pub capture_stdin: bool,
    /// OS-level filesystem confinement to apply to this spawn. `None` = run
    /// unconfined (trusted agent, no run context, or platform without support).
    pub sandbox: Option<SandboxPolicy>,
}

impl SpawnConfig {
    pub fn new(program: &str) -> Self {
        Self {
            program: program.to_string(),
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            capture_stdout: true,
            capture_stderr: true,
            capture_stdin: false,
            sandbox: None,
        }
    }

    /// Apply an OS-level filesystem sandbox to this spawn.
    pub fn sandbox(mut self, policy: Option<SandboxPolicy>) -> Self {
        self.sandbox = policy;
        self
    }

    pub fn stdin(mut self, capture: bool) -> Self {
        self.capture_stdin = capture;
        self
    }

    pub fn arg(mut self, arg: &str) -> Self {
        self.args.push(arg.to_string());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.args
            .extend(args.into_iter().map(|s| s.as_ref().to_string()));
        self
    }

    pub fn cwd(mut self, dir: &str) -> Self {
        self.cwd = Some(dir.to_string());
        self
    }

    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.env.insert(key.to_string(), value.to_string());
        self
    }
}

/// Trait for a running child process.
///
/// This abstraction allows tests to inject fake process behavior.
pub trait ChildProcess: Send {
    /// Get the process ID.
    fn id(&self) -> u32;

    /// Take stdout for reading (can only be called once).
    fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>>;

    /// Take stderr for reading (can only be called once).
    fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>>;

    /// Take stdin for writing (can only be called once).
    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>>;

    /// Check if the process has exited without blocking.
    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>>;

    /// Kill the process.
    fn kill(&mut self) -> std::io::Result<()>;
}

/// RAII guard that SIGKILLs an inline command's process group if it is dropped
/// while still armed.
///
/// This ties process lifetime to the awaiting request future: if the future is
/// dropped mid-wait (client disconnect, MCP cancel, handler abort), `Drop`
/// reaps the whole tree — the cancellation-propagation fix. Disarm on normal
/// completion, on an explicit self-kill, or on promotion (when ownership of the
/// process moves to a terminal session whose own kill path reaps it later).
pub struct KillOnDrop {
    child: Arc<Mutex<Box<dyn ChildProcess>>>,
    armed: AtomicBool,
}

impl KillOnDrop {
    pub fn new(child: Arc<Mutex<Box<dyn ChildProcess>>>) -> Self {
        Self {
            child,
            armed: AtomicBool::new(true),
        }
    }

    /// Transfer ownership away from this guard so `Drop` does not reap.
    pub fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if self.armed.load(Ordering::SeqCst) {
            // `kill()` SIGKILLs the whole process group; ESRCH on an
            // already-dead group is harmless.
            if let Ok(mut c) = self.child.lock() {
                let _ = c.kill();
            }
        }
    }
}

/// Result of running a command to completion.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandOutput {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Trait for spawning processes.
///
/// This abstraction allows tests to mock process spawning.
#[cfg_attr(any(test, feature = "test-utils"), automock)]
pub trait ProcessSpawner: Send + Sync {
    /// Spawn a process with the given configuration.
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String>;

    /// Run a command to completion and return its output.
    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String>;
}

/// Production process spawner using std::process.
pub struct RealProcessSpawner;

/// Build a `Command` from a spawn config, applying the OS sandbox if present.
///
/// On macOS the argv is rewritten to run under `sandbox-exec`; on Linux a
/// landlock `pre_exec` hook is installed. The cwd and env are applied after so
/// they take effect inside the wrapped invocation.
fn build_command(config: &SpawnConfig) -> std::process::Command {
    let (program, args) = match &config.sandbox {
        Some(policy) => super::sandbox::wrap_argv(&config.program, &config.args, policy),
        None => (config.program.clone(), config.args.clone()),
    };

    let mut cmd = crate::env::command(&program);
    cmd.args(&args);

    if let Some(ref cwd) = config.cwd {
        cmd.current_dir(cwd);
    }

    for (key, value) in &config.env {
        cmd.env(key, value);
    }

    if config.sandbox.is_some() {
        // Mark fenced spawns so client tooling (e.g. the rustc cache wrapper)
        // connects to the Cairn-owned build-service daemon instead of
        // auto-starting its own confined one. Service-specific env (e.g.
        // SCCACHE_*) is injected into `config.env` at the spawn seam.
        cmd.env("CAIRN_SANDBOXED", "1");
    }

    if let Some(policy) = &config.sandbox {
        super::sandbox::install_pre_exec(&mut cmd, policy);
    }

    cmd
}

impl ProcessSpawner for RealProcessSpawner {
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        let mut cmd = build_command(&config);

        if config.capture_stdout {
            cmd.stdout(Stdio::piped());
        }
        if config.capture_stderr {
            cmd.stderr(Stdio::piped());
        }
        if config.capture_stdin {
            cmd.stdin(Stdio::piped());
        }

        #[cfg(unix)]
        {
            // Put spawned commands in their own process group so `kill()` can
            // cancel shell-launched descendants (e.g. `npm install`, `bun build`),
            // not just the wrapper shell process.
            cmd.process_group(0);
        }

        let child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn process: {}", e))?;

        Ok(Box::new(RealChildProcess { child }))
    }

    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String> {
        let mut cmd = build_command(&config);

        let output = cmd
            .output()
            .map_err(|e| format!("Failed to run command: {}", e))?;

        Ok(CommandOutput {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }
}

/// Production child process wrapper.
pub struct RealChildProcess {
    child: Child,
}

impl ChildProcess for RealChildProcess {
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.child
            .stdout
            .take()
            .map(|s| Box::new(BufReader::new(s)) as Box<dyn BufRead + Send>)
    }

    fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.child
            .stderr
            .take()
            .map(|s| Box::new(BufReader::new(s)) as Box<dyn BufRead + Send>)
    }

    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        self.child
            .stdin
            .take()
            .map(|s| Box::new(s) as Box<dyn Write + Send>)
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn kill(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            let pgid = nix::unistd::Pid::from_raw(self.child.id() as i32);
            match nix::sys::signal::killpg(pgid, nix::sys::signal::Signal::SIGKILL) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    log::debug!("failed to kill process group {}: {}", self.child.id(), err);
                }
            }
        }
        self.child.kill()
    }
}

/// Mock child process for testing.
///
/// Provides fake stdout content and configurable exit behavior.
#[cfg(any(test, feature = "test-utils"))]
pub struct MockChildProcess {
    id: u32,
    stdout_content: Option<Vec<String>>,
    stderr_content: Option<Vec<String>>,
    stdin_buffer: Option<std::sync::Arc<std::sync::Mutex<Vec<u8>>>>,
    exited: bool,
    exit_code: i32,
}

#[cfg(any(test, feature = "test-utils"))]
impl MockChildProcess {
    /// Create a mock process with given stdout lines.
    pub fn with_stdout(id: u32, lines: Vec<String>) -> Self {
        Self {
            id,
            stdout_content: Some(lines),
            stderr_content: Some(Vec::new()),
            stdin_buffer: Some(std::sync::Arc::new(std::sync::Mutex::new(Vec::new()))),
            exited: false,
            exit_code: 0,
        }
    }

    /// Create a mock process that immediately exits with error.
    pub fn failing(id: u32, stderr: &str, exit_code: i32) -> Self {
        Self {
            id,
            stdout_content: Some(Vec::new()),
            stderr_content: Some(vec![stderr.to_string()]),
            stdin_buffer: Some(std::sync::Arc::new(std::sync::Mutex::new(Vec::new()))),
            exited: true,
            exit_code,
        }
    }

    /// Mark the process as exited.
    pub fn set_exited(&mut self) {
        self.exited = true;
    }
}

/// Mock stdin writer that captures data to a shared buffer.
#[cfg(any(test, feature = "test-utils"))]
struct MockStdinWriter {
    buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl Write for MockStdinWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.buffer.lock().unwrap();
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl ChildProcess for MockChildProcess {
    fn id(&self) -> u32 {
        self.id
    }

    fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.stdout_content.take().map(|lines| {
            let content = lines.join("\n");
            Box::new(std::io::Cursor::new(content)) as Box<dyn BufRead + Send>
        })
    }

    fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.stderr_content.take().map(|lines| {
            let content = lines.join("\n");
            Box::new(std::io::Cursor::new(content)) as Box<dyn BufRead + Send>
        })
    }

    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        self.stdin_buffer
            .take()
            .map(|buffer| Box::new(MockStdinWriter { buffer }) as Box<dyn Write + Send>)
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if self.exited {
            // Use a trick: spawn a dummy process that exits immediately
            // to get a real ExitStatus
            use std::process::Command;
            let status = if self.exit_code == 0 {
                Command::new("true").status()?
            } else {
                Command::new("false").status()?
            };
            Ok(Some(status))
        } else {
            Ok(None)
        }
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.exited = true;
        Ok(())
    }
}

/// Test process spawner that records calls and returns inert success values.
///
/// Unlike the `mockall`-generated [`MockProcessSpawner`], this spawner has a
/// permissive default: unexpected spawns become inspectable records rather than
/// panics. Use it in higher-level harness tests where process startup is an
/// incidental side effect and the assertion should live near the orchestration
/// behavior under test.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Default)]
pub struct RecordingProcessSpawner {
    spawned: Arc<Mutex<Vec<SpawnConfig>>>,
    ran: Arc<Mutex<Vec<SpawnConfig>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl RecordingProcessSpawner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawned(&self) -> Vec<SpawnConfig> {
        self.spawned.lock().unwrap().clone()
    }

    pub fn ran(&self) -> Vec<SpawnConfig> {
        self.ran.lock().unwrap().clone()
    }

    pub fn spawn_count(&self) -> usize {
        self.spawned.lock().unwrap().len()
    }

    pub fn run_count(&self) -> usize {
        self.ran.lock().unwrap().len()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl ProcessSpawner for RecordingProcessSpawner {
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        let mut spawned = self.spawned.lock().unwrap();
        spawned.push(config);
        let id = spawned.len() as u32;
        Ok(Box::new(MockChildProcess::with_stdout(id, Vec::new())))
    }

    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String> {
        self.ran.lock().unwrap().push(config);
        Ok(CommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_config_builder() {
        let config = SpawnConfig::new("echo")
            .arg("hello")
            .args(vec!["world", "!"])
            .cwd("/tmp")
            .env("KEY", "value");

        assert_eq!(config.program, "echo");
        assert_eq!(config.args, vec!["hello", "world", "!"]);
        assert_eq!(config.cwd, Some("/tmp".to_string()));
        assert_eq!(config.env.get("KEY"), Some(&"value".to_string()));
    }

    #[test]
    fn mock_child_process_stdout() {
        let mut mock =
            MockChildProcess::with_stdout(123, vec!["line 1".to_string(), "line 2".to_string()]);

        assert_eq!(mock.id(), 123);

        let stdout = mock.take_stdout().unwrap();
        let lines: Vec<String> = stdout.lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines, vec!["line 1", "line 2"]);

        // Second call returns None
        assert!(mock.take_stdout().is_none());
    }

    #[test]
    fn mock_child_process_try_wait() {
        let mut mock = MockChildProcess::with_stdout(1, vec![]);

        // Not exited yet
        assert!(mock.try_wait().unwrap().is_none());

        // Mark as exited
        mock.set_exited();
        assert!(mock.try_wait().unwrap().is_some());
    }

    #[test]
    fn mock_child_process_kill() {
        let mut mock = MockChildProcess::with_stdout(1, vec![]);

        assert!(mock.try_wait().unwrap().is_none());
        mock.kill().unwrap();
        assert!(mock.try_wait().unwrap().is_some());
    }

    // The real spawner honors a SandboxPolicy end-to-end: an out-of-worktree
    // write is blocked by the kernel. macOS-only (sandbox-exec present).
    #[cfg(target_os = "macos")]
    #[test]
    fn real_spawner_blocks_out_of_worktree_write() {
        use super::SandboxPolicy;
        use tempfile::tempdir;

        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let escape = outside.path().join("escape.txt");
        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
        };
        let cfg = SpawnConfig::new("/bin/bash")
            .args(["-c".to_string(), format!("echo x > {}", escape.display())])
            .sandbox(Some(policy));
        let out = RealProcessSpawner.run(cfg).unwrap();
        assert!(!out.success, "out-of-worktree write must be denied");
        assert!(!escape.exists(), "escape file must not be created");
    }

    #[test]
    fn build_command_sets_cairn_sandboxed_only_when_confined() {
        use super::SandboxPolicy;
        use std::collections::HashMap;

        let envs = |cmd: &std::process::Command| -> HashMap<String, String> {
            cmd.get_envs()
                .filter_map(|(k, v)| Some((k.to_str()?.to_string(), v?.to_str()?.to_string())))
                .collect()
        };

        // Sandboxed spawn: CAIRN_SANDBOXED=1 plus the injected service env.
        let policy = SandboxPolicy {
            worktree: std::path::PathBuf::from("/work/wt"),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
        };
        let cfg = SpawnConfig::new("echo")
            .env("SCCACHE_SERVER_PORT", "4226")
            .sandbox(Some(policy));
        let confined = envs(&build_command(&cfg));
        assert_eq!(
            confined.get("CAIRN_SANDBOXED").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            confined.get("SCCACHE_SERVER_PORT").map(String::as_str),
            Some("4226")
        );

        // Unsandboxed spawn: CAIRN_SANDBOXED is absent.
        let plain = envs(&build_command(&SpawnConfig::new("echo")));
        assert!(!plain.contains_key("CAIRN_SANDBOXED"));
    }

    #[test]
    fn mock_process_spawner() {
        let mut mock_spawner = MockProcessSpawner::new();
        mock_spawner.expect_spawn().returning(|config| {
            Ok(Box::new(MockChildProcess::with_stdout(
                42,
                vec![format!("Running: {}", config.program)],
            )))
        });

        let config = SpawnConfig::new("test-program");
        let mut child = mock_spawner.spawn(config).unwrap();

        assert_eq!(child.id(), 42);

        let stdout = child.take_stdout().unwrap();
        let line = stdout.lines().next().unwrap().unwrap();
        assert!(line.contains("test-program"));
    }

    #[test]
    fn recording_process_spawner_records_without_expectations() {
        let spawner = RecordingProcessSpawner::new();

        let child = spawner
            .spawn(SpawnConfig::new("unexpected-spawn").arg("--flag"))
            .unwrap();
        let output = spawner.run(SpawnConfig::new("unexpected-run")).unwrap();

        assert_eq!(child.id(), 1);
        assert!(output.success);
        assert_eq!(spawner.spawned()[0].program, "unexpected-spawn");
        assert_eq!(spawner.spawned()[0].args, vec!["--flag"]);
        assert_eq!(spawner.ran()[0].program, "unexpected-run");
    }

    #[cfg(unix)]
    #[test]
    fn kill_on_drop_reaps_when_armed() {
        let child = RealProcessSpawner
            .spawn(SpawnConfig::new("/bin/sh").args(["-c".to_string(), "sleep 30".to_string()]))
            .unwrap();
        let shared = Arc::new(Mutex::new(child));
        {
            let _guard = KillOnDrop::new(shared.clone());
        } // dropped while armed -> SIGKILLs the group
        std::thread::sleep(std::time::Duration::from_millis(300));
        let mut c = shared.lock().unwrap();
        assert!(
            c.try_wait().unwrap().is_some(),
            "armed KillOnDrop should have reaped the child"
        );
    }

    #[cfg(unix)]
    #[test]
    fn kill_on_drop_is_noop_when_disarmed() {
        let child = RealProcessSpawner
            .spawn(SpawnConfig::new("/bin/sh").args(["-c".to_string(), "sleep 30".to_string()]))
            .unwrap();
        let shared = Arc::new(Mutex::new(child));
        {
            let guard = KillOnDrop::new(shared.clone());
            guard.disarm();
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let mut c = shared.lock().unwrap();
        assert!(
            c.try_wait().unwrap().is_none(),
            "disarmed KillOnDrop must not kill the child"
        );
        let _ = c.kill();
    }
}
