//! Process spawning service for external command execution.
//!
//! Abstracts process spawning to enable testing without real subprocesses.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ExitStatus, Stdio};

#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;

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
        }
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

impl ProcessSpawner for RealProcessSpawner {
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        let mut cmd = crate::env::command(&config.program);

        cmd.args(&config.args);

        if let Some(ref cwd) = config.cwd {
            cmd.current_dir(cwd);
        }

        for (key, value) in &config.env {
            cmd.env(key, value);
        }

        if config.capture_stdout {
            cmd.stdout(Stdio::piped());
        }
        if config.capture_stderr {
            cmd.stderr(Stdio::piped());
        }
        if config.capture_stdin {
            cmd.stdin(Stdio::piped());
        }

        let child = cmd
            .spawn()
            .map_err(|e| format!("Failed to spawn process: {}", e))?;

        Ok(Box::new(RealChildProcess { child }))
    }

    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String> {
        let mut cmd = crate::env::command(&config.program);

        cmd.args(&config.args);

        if let Some(ref cwd) = config.cwd {
            cmd.current_dir(cwd);
        }

        for (key, value) in &config.env {
            cmd.env(key, value);
        }

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
}
