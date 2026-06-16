//! PTY service abstractions and pure logic.
//!
//! Provides testable pure functions for PTY operations and buffer management,
//! plus a factory trait for creating PTY pairs in a testable way.

use super::process::ChildProcess;
use super::pty_osc::Osc133Event;
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

pub type SharedInlineChild = Arc<Mutex<Box<dyn ChildProcess>>>;

/// Per-session record of the most recent OSC 133 command lifecycle.
///
/// Folds the `C` (command start) / `D` (command end, with exit code) markers
/// into a single shared object: whether a command is currently running, the
/// exit code of the last finished command, and its wall-clock duration. The
/// markers carry no timestamps, so duration is measured on the backend as the
/// elapsed time between observing `C` and `D`. One `apply` call centralizes the
/// transition that both interactive read loops drive.
#[derive(Debug, Default)]
pub struct CommandState {
    /// True while a command runs (between `C` and `D`), false at the prompt.
    pub busy: bool,
    /// Exit code of the most recently finished command, if any has finished.
    pub last_exit_code: Option<i32>,
    /// Wall-clock duration of the most recently finished command, in ms.
    pub last_duration_ms: Option<u64>,
    /// When the in-flight command started, used to compute `last_duration_ms`.
    started_at: Option<std::time::Instant>,
}

impl CommandState {
    /// Apply an OSC 133 transition; returns `(busy, exit_code, duration_ms)` to
    /// emit on the `pty-command-state` event. `exit_code`/`duration_ms` are only
    /// `Some` on the command-end (`busy:false`) transition.
    pub fn apply(&mut self, event: Osc133Event) -> (bool, Option<i32>, Option<u64>) {
        match event {
            Osc133Event::CommandStart => {
                self.busy = true;
                self.started_at = Some(std::time::Instant::now());
                (true, None, None)
            }
            Osc133Event::CommandEnd { exit } => {
                self.busy = false;
                self.last_exit_code = Some(exit);
                let dur = self
                    .started_at
                    .take()
                    .map(|t| t.elapsed().as_millis() as u64);
                self.last_duration_ms = dur;
                (false, Some(exit), dur)
            }
        }
    }
}

// ============================================================================
// PtyState — Runtime PTY session tracking (shared by Tauri + cairn-server)
// ============================================================================

/// A running process handle backing a terminal session.
///
/// Abstracts over the two process kinds that can live in `pty_state.sessions`:
/// a `portable_pty::Child` (interactive PTY terminals) and an inline
/// `ChildProcess` promoted from a timed-out `run` command. This keeps one
/// session registry and one buffer-read path for both attachment modes.
pub trait TerminalChild: Send + Sync {
    /// SIGKILL the process (group, for inline children).
    fn kill(&mut self) -> std::io::Result<()>;
    /// Block until the process exits, reaping it.
    fn wait(&mut self) -> std::io::Result<()>;
    /// Return the exit code if the process has already exited (non-blocking).
    fn try_wait_exit(&mut self) -> Option<i32>;
    /// The OS process id, if known.
    fn process_id(&self) -> Option<u32>;
}

/// `TerminalChild` over a `portable_pty::Child` (interactive PTY terminals).
pub struct PortableTerminalChild {
    child: Box<dyn Child + Send + Sync>,
}

impl PortableTerminalChild {
    pub fn new(child: Box<dyn Child + Send + Sync>) -> Self {
        Self { child }
    }
}

impl TerminalChild for PortableTerminalChild {
    fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()
    }
    fn wait(&mut self) -> std::io::Result<()> {
        self.child.wait().map(|_| ())
    }
    fn try_wait_exit(&mut self) -> Option<i32> {
        match self.child.try_wait() {
            Ok(Some(status)) => Some(status.exit_code() as i32),
            _ => None,
        }
    }
    fn process_id(&self) -> Option<u32> {
        self.child.process_id()
    }
}

/// `TerminalChild` over an inline `ChildProcess` promoted from a `run` command.
///
/// `ChildProcess` exposes no blocking wait, so `wait` polls `try_wait`. `kill`
/// already SIGKILLs the whole process group (see `RealChildProcess::kill`).
pub struct InlineTerminalChild {
    inner: SharedInlineChild,
}

impl InlineTerminalChild {
    pub fn new(inner: SharedInlineChild) -> Self {
        Self { inner }
    }
}

impl TerminalChild for InlineTerminalChild {
    fn kill(&mut self) -> std::io::Result<()> {
        match self.inner.lock() {
            Ok(mut c) => c.kill(),
            Err(_) => Ok(()),
        }
    }
    fn wait(&mut self) -> std::io::Result<()> {
        loop {
            let exited = match self.inner.lock() {
                Ok(mut c) => matches!(c.try_wait(), Ok(Some(_))),
                Err(_) => return Ok(()),
            };
            if exited {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    fn try_wait_exit(&mut self) -> Option<i32> {
        match self.inner.lock() {
            Ok(mut c) => match c.try_wait() {
                Ok(Some(status)) => Some(status.code().unwrap_or(0)),
                _ => None,
            },
            Err(_) => None,
        }
    }
    fn process_id(&self) -> Option<u32> {
        self.inner.lock().ok().map(|c| c.id())
    }
}

/// A single terminal session.
///
/// Two attachment modes share this type: a PTY-backed interactive terminal
/// (`master`/`writer` present) and a promoted pipe-backed run command
/// (`master`/`writer` `None`). Buffer reads and exit tracking work identically
/// for both; input and resize are unavailable-but-harmless for promoted ones.
pub struct PtySession {
    /// PTY master for resize/foreground queries. `None` for promoted runs.
    pub master: Option<Box<dyn MasterPty + Send>>,
    /// PTY writer for input. `None` for promoted runs (no stdin attachment).
    pub writer: Option<Box<dyn Write + Send>>,
    /// Process handle for cleanup and exit tracking.
    pub child: Box<dyn TerminalChild>,
    /// Output buffer for late attachment (agent terminals only)
    pub output_buffer: Option<Arc<Mutex<VecDeque<u8>>>>,
    /// Whether this session was spawned by an agent (vs user)
    #[allow(dead_code)]
    pub is_agent_spawned: bool,
    /// Wall-clock time of the most recent output chunk, used for the "age of
    /// last output" signal in agent terminal status reads. `None` for user
    /// terminals, which are not polled through the resource read path.
    pub last_output_at: Option<Arc<Mutex<std::time::SystemTime>>>,
    /// Per-command record (busy + last exit code + last duration), driven by OSC
    /// 133 `C`/`D` markers parsed in the interactive read loops. `None` for
    /// agent/promoted sessions, which are non-interactive and detect completion
    /// via EOF rather than prompt markers.
    pub command_state: Option<Arc<Mutex<CommandState>>>,
}

/// Manages all active PTY sessions
pub struct PtyState {
    pub sessions: Mutex<HashMap<String, Arc<Mutex<PtySession>>>>,
    pub inline_commands: Mutex<HashMap<String, HashMap<String, SharedInlineChild>>>,
}

impl Default for PtyState {
    fn default() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            inline_commands: Mutex::new(HashMap::new()),
        }
    }
}

impl PtyState {
    pub fn register_inline_command(
        &self,
        run_id: String,
        command_id: String,
        child: SharedInlineChild,
    ) {
        if let Ok(mut commands) = self.inline_commands.lock() {
            commands
                .entry(run_id)
                .or_default()
                .insert(command_id, child);
        }
    }

    pub fn unregister_inline_command(&self, run_id: &str, command_id: &str) {
        if let Ok(mut commands) = self.inline_commands.lock() {
            if let Some(run_commands) = commands.get_mut(run_id) {
                run_commands.remove(command_id);
                if run_commands.is_empty() {
                    commands.remove(run_id);
                }
            }
        }
    }

    pub fn take_inline_commands(&self, run_id: &str) -> Vec<SharedInlineChild> {
        if let Ok(mut commands) = self.inline_commands.lock() {
            return commands
                .remove(run_id)
                .map(|run_commands| run_commands.into_values().collect())
                .unwrap_or_default();
        }

        Vec::new()
    }
}

/// Maximum output buffer size (64KB) - same as commands.rs
pub const MAX_BUFFER_SIZE: usize = 64 * 1024;

/// Ensure terminal input submits as a line: append `\n` unless already present.
pub fn ensure_submitted_line(content: &str) -> Cow<'_, str> {
    if content.ends_with('\n') {
        Cow::Borrowed(content)
    } else {
        Cow::Owned(format!("{content}\n"))
    }
}

/// Submit `command` so the shell exits with the command's status: EOF then
/// coincides with command completion and the shell's own exit code equals the
/// command's. The trailing newline separates the command from `exit $?` so a
/// multiline command completes first; nothing between them resets `$?`, and
/// `exit $?` preserves an interrupt's `128+signal` code through the shell.
///
/// Used only for agent-monitored terminals, whose shell is a one-shot host for
/// the command — not a post-command scratch shell. User scratch terminals keep
/// `ensure_submitted_line` so their shell stays interactive.
pub fn submit_command_exiting_shell(command: &str) -> String {
    format!("{}\nexit $?\n", command.trim_end_matches('\n'))
}

// ============================================================================
// PtyFactory Trait - Enables testable PTY creation
// ============================================================================

/// Result type returned by PTY operations.
pub type PtyResult<T> = Result<T, String>;

/// Abstraction for creating PTY pairs.
///
/// This trait enables dependency injection for terminal creation,
/// allowing tests to mock PTY behavior without spawning real processes.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
pub trait PtyFactory: Send + Sync {
    /// Create a new PTY pair with the given size.
    fn create_pty(&self, size: PtySize) -> PtyResult<Box<dyn PtyPair>>;
}

/// Components needed to construct a PtySession.
pub struct PtyComponents {
    pub master: Box<dyn MasterPty + Send>,
    pub writer: Box<dyn Write + Send>,
    pub child: Box<dyn Child + Send + Sync>,
    pub reader: Box<dyn Read + Send>,
}

/// Abstraction for a PTY master/slave pair.
///
/// This trait wraps portable_pty's PtyPair to enable mocking.
pub trait PtyPair: Send {
    /// Spawn a command and decompose the pair into components for PtySession.
    ///
    /// This consumes the pair and returns everything needed to create a PtySession.
    fn spawn_and_split(self: Box<Self>, cmd: CommandBuilder) -> PtyResult<PtyComponents>;
}

/// Production implementation using portable_pty.
pub struct RealPtyFactory;

impl PtyFactory for RealPtyFactory {
    fn create_pty(&self, size: PtySize) -> PtyResult<Box<dyn PtyPair>> {
        let pty_system = portable_pty::native_pty_system();
        let pair = pty_system
            .openpty(size)
            .map_err(|e| format!("Failed to create PTY: {}", e))?;
        Ok(Box::new(RealPtyPair { pair }))
    }
}

/// Wrapper around portable_pty's PtyPair.
struct RealPtyPair {
    pair: portable_pty::PtyPair,
}

impl PtyPair for RealPtyPair {
    fn spawn_and_split(self: Box<Self>, cmd: CommandBuilder) -> PtyResult<PtyComponents> {
        // Spawn the command
        let pair = self.pair;
        let child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|e| format!("Failed to spawn command: {}", e))?;

        // Get writer and reader
        let writer = pair
            .master
            .take_writer()
            .map_err(|e| format!("Failed to get writer: {}", e))?;

        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| format!("Failed to clone reader: {}", e))?;

        // Return all components
        Ok(PtyComponents {
            master: pair.master,
            writer,
            child,
            reader,
        })
    }
}

/// Manage a bounded output buffer.
///
/// This is pure logic extracted from the PTY reader for testability.
pub fn update_output_buffer(buffer: &mut VecDeque<u8>, new_data: &[u8], max_size: usize) {
    buffer.extend(new_data);
    while buffer.len() > max_size {
        buffer.pop_front();
    }
}

/// Read from a source and call handlers for data/exit events.
///
/// This is the core loop logic extracted for testability.
/// Returns when the reader returns EOF or an error.
pub fn read_pty_loop<R: Read, FData, FExit>(
    mut reader: R,
    mut on_data: FData,
    mut on_exit: FExit,
    mut buffer: Option<&mut VecDeque<u8>>,
) where
    FData: FnMut(&str),
    FExit: FnMut(Option<i32>),
{
    let mut buf = [0u8; 4096];

    loop {
        match reader.read(&mut buf) {
            Ok(0) => {
                // EOF - process exited
                on_exit(None);
                break;
            }
            Ok(n) => {
                let data = String::from_utf8_lossy(&buf[..n]).to_string();
                on_data(&data);

                // Buffer output if buffer provided
                if let Some(ref mut output_buffer) = buffer.as_deref_mut() {
                    update_output_buffer(output_buffer, &buf[..n], MAX_BUFFER_SIZE);
                }
            }
            Err(_) => {
                on_exit(None);
                break;
            }
        }
    }
}

/// Get the default shell path based on environment and platform.
pub fn get_default_shell() -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // =========================================================================
    // ensure_submitted_line tests
    // =========================================================================

    #[test]
    fn ensure_submitted_line_appends_missing_newline() {
        assert_eq!(ensure_submitted_line("rs"), "rs\n");
    }

    #[test]
    fn ensure_submitted_line_keeps_existing_newline() {
        let content = "rs\n";
        assert!(matches!(
            ensure_submitted_line(content),
            std::borrow::Cow::Borrowed(_)
        ));
        assert_eq!(ensure_submitted_line(content), content);
    }

    #[test]
    fn ensure_submitted_line_submits_empty_line() {
        assert_eq!(ensure_submitted_line(""), "\n");
    }

    #[test]
    fn ensure_submitted_line_appends_after_multiline_content() {
        assert_eq!(
            ensure_submitted_line("echo one\necho two"),
            "echo one\necho two\n"
        );
    }

    // =========================================================================
    // update_output_buffer tests
    // =========================================================================

    #[test]
    fn update_output_buffer_adds_data() {
        let mut buffer = VecDeque::new();
        update_output_buffer(&mut buffer, b"hello", 100);
        assert_eq!(buffer.len(), 5);
        assert_eq!(&buffer.iter().copied().collect::<Vec<_>>(), b"hello");
    }

    #[test]
    fn update_output_buffer_appends_data() {
        let mut buffer = VecDeque::new();
        update_output_buffer(&mut buffer, b"hello", 100);
        update_output_buffer(&mut buffer, b" world", 100);
        let result: Vec<u8> = buffer.iter().copied().collect();
        assert_eq!(result, b"hello world");
    }

    #[test]
    fn update_output_buffer_enforces_max_size() {
        let mut buffer = VecDeque::new();
        update_output_buffer(&mut buffer, b"12345", 10);
        update_output_buffer(&mut buffer, b"67890", 10);
        update_output_buffer(&mut buffer, b"ABC", 10);

        // Should have evicted oldest bytes to stay at max 10
        assert!(buffer.len() <= 10);
        // Last data should be present
        let result: Vec<u8> = buffer.iter().copied().collect();
        assert!(result.ends_with(b"ABC"));
    }

    #[test]
    fn update_output_buffer_exactly_at_limit() {
        let mut buffer = VecDeque::new();
        update_output_buffer(&mut buffer, b"12345", 5);
        assert_eq!(buffer.len(), 5);

        // Adding one more should evict oldest
        update_output_buffer(&mut buffer, b"A", 5);
        assert_eq!(buffer.len(), 5);
        let result: Vec<u8> = buffer.iter().copied().collect();
        assert_eq!(result, b"2345A");
    }

    #[test]
    fn update_output_buffer_data_larger_than_max() {
        let mut buffer = VecDeque::new();
        update_output_buffer(&mut buffer, b"1234567890", 5);

        // Buffer should be trimmed to last 5 bytes
        assert_eq!(buffer.len(), 5);
        let result: Vec<u8> = buffer.iter().copied().collect();
        assert_eq!(result, b"67890");
    }

    #[test]
    fn update_output_buffer_empty_data() {
        let mut buffer = VecDeque::new();
        update_output_buffer(&mut buffer, b"hello", 100);
        update_output_buffer(&mut buffer, b"", 100);
        assert_eq!(buffer.len(), 5);
    }

    // =========================================================================
    // submit_command_exiting_shell tests
    // =========================================================================

    #[test]
    fn submit_command_exiting_shell_appends_exit() {
        assert_eq!(
            submit_command_exiting_shell("echo hi"),
            "echo hi\nexit $?\n"
        );
    }

    #[test]
    fn submit_command_exiting_shell_empty_command() {
        // An empty command runs nothing; the shell exits 0.
        assert_eq!(submit_command_exiting_shell(""), "\nexit $?\n");
    }

    #[test]
    fn submit_command_exiting_shell_trims_trailing_newlines() {
        // A single `exit $?` is appended regardless of trailing newlines on the
        // command, so `$?` reflects the command (not a blank line).
        assert_eq!(
            submit_command_exiting_shell("make build\n"),
            "make build\nexit $?\n"
        );
        assert_eq!(
            submit_command_exiting_shell("make build\n\n"),
            "make build\nexit $?\n"
        );
    }

    #[test]
    fn submit_command_exiting_shell_multiline_command() {
        // The command's own newlines are preserved; `exit $?` follows the last
        // line so it carries that line's status.
        assert_eq!(
            submit_command_exiting_shell("a=1\necho $a"),
            "a=1\necho $a\nexit $?\n"
        );
    }

    // =========================================================================
    // TerminalChild tests
    // =========================================================================

    #[cfg(unix)]
    #[test]
    fn inline_terminal_child_normalizes_exit_code() {
        use crate::services::{ProcessSpawner, RealProcessSpawner, SpawnConfig};
        let child = RealProcessSpawner
            .spawn(SpawnConfig::new("/bin/sh").args(["-c".to_string(), "exit 3".to_string()]))
            .unwrap();
        let shared = Arc::new(Mutex::new(child));
        let mut tc = InlineTerminalChild::new(shared);
        tc.wait().unwrap();
        assert_eq!(tc.try_wait_exit(), Some(3));
    }

    #[cfg(unix)]
    #[test]
    fn portable_terminal_child_reports_exit_code() {
        let pair = RealPtyFactory
            .create_pty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new("/bin/sh");
        cmd.arg("-c");
        cmd.arg("exit 7");
        let components = pair.spawn_and_split(cmd).unwrap();
        let mut tc = PortableTerminalChild::new(components.child);
        assert!(tc.process_id().is_some());
        tc.wait().unwrap();
        assert_eq!(tc.try_wait_exit(), Some(7));
    }

    // =========================================================================
    // read_pty_loop tests
    // =========================================================================

    #[test]
    fn read_pty_loop_emits_data_events() {
        let input = Cursor::new(b"hello world");
        let mut data_received = Vec::new();
        let mut exit_called = false;

        read_pty_loop(
            input,
            |data| data_received.push(data.to_string()),
            |_| exit_called = true,
            None,
        );

        assert_eq!(data_received.len(), 1);
        assert_eq!(data_received[0], "hello world");
        assert!(exit_called); // EOF triggers exit
    }

    #[test]
    fn read_pty_loop_emits_exit_on_eof() {
        let input = Cursor::new(Vec::<u8>::new()); // Empty = immediate EOF
        let mut exit_code = Some(999i32);

        read_pty_loop(input, |_| {}, |code| exit_code = code, None);

        assert_eq!(exit_code, None); // EOF produces None exit code
    }

    #[test]
    fn read_pty_loop_buffers_output() {
        let input = Cursor::new(b"data to buffer");
        let mut buffer = VecDeque::new();

        read_pty_loop(input, |_| {}, |_| {}, Some(&mut buffer));

        let result: Vec<u8> = buffer.iter().copied().collect();
        assert_eq!(result, b"data to buffer");
    }

    #[test]
    fn read_pty_loop_handles_multiple_reads() {
        // Simulate chunked reads with a custom reader
        struct ChunkedReader {
            chunks: Vec<Vec<u8>>,
            index: usize,
        }

        impl Read for ChunkedReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.index >= self.chunks.len() {
                    return Ok(0); // EOF
                }
                let chunk = &self.chunks[self.index];
                self.index += 1;
                let len = chunk.len().min(buf.len());
                buf[..len].copy_from_slice(&chunk[..len]);
                Ok(len)
            }
        }

        let reader = ChunkedReader {
            chunks: vec![b"chunk1".to_vec(), b"chunk2".to_vec(), b"chunk3".to_vec()],
            index: 0,
        };

        let mut data_events = Vec::new();

        read_pty_loop(
            reader,
            |data| data_events.push(data.to_string()),
            |_| {},
            None,
        );

        assert_eq!(data_events.len(), 3);
        assert_eq!(data_events[0], "chunk1");
        assert_eq!(data_events[1], "chunk2");
        assert_eq!(data_events[2], "chunk3");
    }

    #[test]
    fn read_pty_loop_handles_read_error() {
        struct FailingReader;

        impl Read for FailingReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("read failed"))
            }
        }

        let mut exit_called = false;

        read_pty_loop(
            FailingReader,
            |_| panic!("Should not receive data on error"),
            |_| exit_called = true,
            None,
        );

        assert!(exit_called);
    }

    // =========================================================================
    // CommandState::apply tests
    // =========================================================================

    #[test]
    fn command_state_start_sets_busy_no_exit() {
        let mut state = CommandState::default();
        let (busy, exit, dur) = state.apply(Osc133Event::CommandStart);
        assert!(busy);
        assert_eq!(exit, None);
        assert_eq!(dur, None);
        assert!(state.busy);
        assert!(state.last_exit_code.is_none());
    }

    #[test]
    fn command_state_end_clears_busy_and_records_exit_and_duration() {
        let mut state = CommandState::default();
        state.apply(Osc133Event::CommandStart);
        let (busy, exit, dur) = state.apply(Osc133Event::CommandEnd { exit: 3 });
        assert!(!busy);
        assert_eq!(exit, Some(3));
        // A start preceded the end, so a duration is recorded (>= 0ms).
        assert!(dur.is_some());
        assert!(!state.busy);
        assert_eq!(state.last_exit_code, Some(3));
        assert_eq!(state.last_duration_ms, dur);
    }

    #[test]
    fn command_state_end_without_start_has_no_duration() {
        // A `D` with no preceding `C` (e.g. reattach mid-prompt) records the exit
        // code but cannot compute a duration.
        let mut state = CommandState::default();
        let (busy, exit, dur) = state.apply(Osc133Event::CommandEnd { exit: 0 });
        assert!(!busy);
        assert_eq!(exit, Some(0));
        assert_eq!(dur, None);
        assert_eq!(state.last_exit_code, Some(0));
        assert_eq!(state.last_duration_ms, None);
    }

    // =========================================================================
    // get_default_shell tests
    // =========================================================================

    #[test]
    fn get_default_shell_returns_string() {
        let shell = get_default_shell();
        assert!(!shell.is_empty());
        // On Unix with SHELL set, should return that
        // On Windows or without SHELL, should return a default
    }
}
