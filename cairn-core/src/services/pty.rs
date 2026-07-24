//! PTY service abstractions and pure logic.
//!
//! Provides testable pure functions for PTY operations and buffer management,
//! plus a factory trait for creating PTY pairs in a testable way.

use super::process::ChildProcess;
use super::pty_osc::Osc133Event;
use cairn_common::executor_protocol::{LifetimeLeaseFence, LifetimeProcessEvent};
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

pub struct RemoteTerminalChild;

impl TerminalChild for RemoteTerminalChild {
    fn kill(&mut self) -> std::io::Result<()> {
        Ok(())
    }
    fn wait(&mut self) -> std::io::Result<()> {
        Ok(())
    }
    fn try_wait_exit(&mut self) -> Option<i32> {
        None
    }
    fn process_id(&self) -> Option<u32> {
        None
    }
}

#[derive(Clone)]
pub struct LeaseTerminalBinding {
    pub fence: LifetimeLeaseFence,
    pub process_key: String,
    pub process_generation: u64,
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

/// A single terminal session.
///
/// Interactive terminals have a local PTY master/writer; executor-backed
/// terminals, including promoted runs, bind through `lease` and keep those local
/// handles absent.
pub struct PtySession {
    /// PTY master for resize/foreground queries. `None` for promoted runs.
    pub master: Option<Box<dyn MasterPty + Send>>,
    /// PTY writer for input. `None` for promoted runs (no stdin attachment).
    pub writer: Option<Box<dyn Write + Send>>,
    /// Process handle for cleanup and exit tracking.
    pub child: Box<dyn TerminalChild>,
    /// Executor-hosted terminal authority. When present, input, resize, and stop
    /// route through the lifetime lease instead of local process handles.
    pub lease: Option<LeaseTerminalBinding>,
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
    /// Live phrase watchers on this terminal's output stream. Populated for
    /// agent PTY terminals (the only kind whose chunked read loop scans output);
    /// `None` for promoted-run and user sessions. The agent read loop and the
    /// wake-subscribe path share this `Arc`, so a subscription created after the
    /// terminal starts is seen by the running loop without a restart.
    pub output_watchers: Option<Arc<Mutex<Vec<TerminalOutputWatcher>>>>,
}

/// Manages all active PTY sessions
pub type LifetimeProcessHandler = Arc<dyn Fn(LifetimeProcessEvent) + Send + Sync>;

pub struct PtyState {
    pub sessions: Mutex<HashMap<String, Arc<Mutex<PtySession>>>>,
    pub inline_commands: Mutex<HashMap<String, HashMap<String, SharedInlineChild>>>,
    pub lifetime_handlers: Mutex<HashMap<(String, String), LifetimeProcessHandler>>,
    pub lifetime_subscription_installed: std::sync::atomic::AtomicBool,
}

impl Default for PtyState {
    fn default() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            inline_commands: Mutex::new(HashMap::new()),
            lifetime_handlers: Mutex::new(HashMap::new()),
            lifetime_subscription_installed: std::sync::atomic::AtomicBool::new(false),
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

/// Max length (chars) of the excerpt surfaced in a phrase-match wake message.
/// Keeps the wake to a small contextual line rather than leaking a large chunk.
pub const PHRASE_EXCERPT_MAX: usize = 200;

/// A live phrase watcher on a terminal's output stream. Registered on the agent
/// PTY session when an agent subscribes `{kind:"terminal", on:"output", phrase}`,
/// matched against each output chunk by the agent read loop, and removed the
/// first time it matches (one-shot).
#[derive(Clone, Debug)]
pub struct TerminalOutputWatcher {
    /// The `wake_subscriptions` row to consume + wake when the phrase appears.
    pub subscription_id: String,
    /// The subscribing job; the wake is delivered only to it.
    pub job_id: String,
    /// Literal substring to match (case-sensitive).
    pub phrase: String,
    /// Trailing bytes carried from the previous chunk so a phrase split across a
    /// chunk boundary is still detected. Always shorter than `phrase`.
    pub carry: String,
    /// Canonical `.../terminal/<slug>` URI this watcher belongs to, so a match
    /// routes its wake without needing the originating `CairnResource`. This lets
    /// both terminal readers (agent-MCP and interactive) share one scan path.
    pub terminal_uri: String,
}

/// Outcome of scanning one output chunk for a watcher's phrase.
pub struct PhraseScan {
    /// A short excerpt (the matched line, ANSI-stripped, trimmed, capped) when
    /// the phrase appeared in this chunk; `None` otherwise.
    pub matched_excerpt: Option<String>,
    /// The carry to retain for the next chunk (empty once matched).
    pub carry: String,
}

/// Scan `chunk` (prefixed with the watcher's `carry` from the previous chunk)
/// for `phrase`. Literal, case-sensitive substring match against the raw output
/// bytes. On a hit, returns the matched line ANSI-stripped, trimmed, and capped
/// to `PHRASE_EXCERPT_MAX` chars; otherwise returns a trailing carry shorter
/// than the phrase so a match straddling the next boundary is still caught.
pub fn scan_for_phrase(carry: &str, chunk: &str, phrase: &str) -> PhraseScan {
    if phrase.is_empty() {
        return PhraseScan {
            matched_excerpt: None,
            carry: String::new(),
        };
    }
    let mut combined = String::with_capacity(carry.len() + chunk.len());
    combined.push_str(carry);
    combined.push_str(chunk);
    if let Some(pos) = combined.find(phrase) {
        return PhraseScan {
            matched_excerpt: Some(excerpt_around(&combined, pos, phrase.len())),
            carry: String::new(),
        };
    }
    let keep = phrase.len().saturating_sub(1);
    PhraseScan {
        matched_excerpt: None,
        carry: tail_within_bytes(&combined, keep),
    }
}

/// Return the suffix of `s` whose byte length is at most `max_bytes`, snapped
/// forward to the next char boundary so the slice never splits a UTF-8 char.
fn tail_within_bytes(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut start = s.len() - max_bytes;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    s[start..].to_string()
}

/// Extract the single line containing the match at `[pos, pos+phrase_len)`,
/// strip ANSI escape sequences, trim, and cap to `PHRASE_EXCERPT_MAX` chars.
fn excerpt_around(s: &str, pos: usize, phrase_len: usize) -> String {
    let line_start = s[..pos].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let after = pos + phrase_len;
    let line_end = s[after..].find('\n').map(|i| after + i).unwrap_or(s.len());
    let cleaned = strip_ansi(&s[line_start..line_end]);
    cleaned.trim().chars().take(PHRASE_EXCERPT_MAX).collect()
}

/// Remove ANSI CSI escape sequences (`ESC [ … final-byte`) so the excerpt reads
/// as plain text. Other bytes pass through unchanged.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for n in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&n) {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// Ensure terminal input submits as a line: append `\n` unless already present.
pub fn ensure_submitted_line(content: &str) -> Cow<'_, str> {
    if content.ends_with('\n') {
        Cow::Borrowed(content)
    } else {
        Cow::Owned(format!("{content}\n"))
    }
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
    // TerminalChild tests
    // =========================================================================

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

    // =========================================================================
    // scan_for_phrase tests (terminal-output phrase wake matching)
    // =========================================================================

    #[test]
    fn scan_for_phrase_matches_within_chunk() {
        let scan = scan_for_phrase("", "build finished: ready to serve\n", "ready");
        assert_eq!(
            scan.matched_excerpt.as_deref(),
            Some("build finished: ready to serve")
        );
        assert!(scan.carry.is_empty(), "a match clears the carry");
    }

    #[test]
    fn scan_for_phrase_no_match_keeps_carry_shorter_than_phrase() {
        let phrase = "ready";
        let scan = scan_for_phrase("", "server is rea", phrase);
        assert!(scan.matched_excerpt.is_none());
        // The carry retains a tail short enough that a match straddling the next
        // chunk boundary is still caught, but never as long as the phrase.
        assert!(!scan.carry.is_empty());
        assert!(scan.carry.len() < phrase.len());
    }

    #[test]
    fn scan_for_phrase_matches_across_chunk_boundary() {
        let phrase = "ready";
        let first = scan_for_phrase("", "server is rea", phrase);
        assert!(first.matched_excerpt.is_none());
        let second = scan_for_phrase(&first.carry, "dy now\n", phrase);
        let excerpt = second
            .matched_excerpt
            .expect("phrase split across two chunks must still match");
        assert!(excerpt.contains("ready"));
    }

    #[test]
    fn scan_for_phrase_is_case_sensitive() {
        let scan = scan_for_phrase("", "all READY here\n", "ready");
        assert!(
            scan.matched_excerpt.is_none(),
            "matching is a literal, case-sensitive substring"
        );
    }

    #[test]
    fn scan_for_phrase_strips_ansi_from_excerpt() {
        let chunk = "\u{1b}[32mcompile ready\u{1b}[0m\n";
        let scan = scan_for_phrase("", chunk, "ready");
        let excerpt = scan.matched_excerpt.expect("should match");
        assert_eq!(excerpt, "compile ready");
        assert!(!excerpt.contains('\u{1b}'), "escape bytes must be stripped");
    }

    #[test]
    fn scan_for_phrase_excerpt_is_single_line_and_capped() {
        let mut chunk = String::from("noise before\n");
        chunk.push_str(&"x".repeat(500));
        chunk.push_str(" ready ");
        chunk.push_str(&"y".repeat(500));
        chunk.push('\n');
        chunk.push_str("noise after\n");
        let scan = scan_for_phrase("", &chunk, "ready");
        let excerpt = scan.matched_excerpt.expect("should match");
        assert!(excerpt.chars().count() <= PHRASE_EXCERPT_MAX);
        // The excerpt is the matched line only, never neighbouring lines.
        assert!(!excerpt.contains("noise before"));
        assert!(!excerpt.contains("noise after"));
    }

    #[test]
    fn scan_for_phrase_empty_phrase_never_matches() {
        let scan = scan_for_phrase("", "anything at all\n", "");
        assert!(scan.matched_excerpt.is_none());
        assert!(scan.carry.is_empty());
    }
}
