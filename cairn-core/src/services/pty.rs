//! PTY service abstractions and pure logic.
//!
//! Provides testable pure functions for PTY operations and buffer management,
//! plus a factory trait for creating PTY pairs in a testable way.

use portable_pty::{Child, CommandBuilder, MasterPty, PtySize};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

// ============================================================================
// PtyState — Runtime PTY session tracking (shared by Tauri + cairn-server)
// ============================================================================

/// A single PTY session
pub struct PtySession {
    pub master: Box<dyn MasterPty + Send>,
    pub writer: Box<dyn Write + Send>,
    /// Child process handle for cleanup
    pub child: Box<dyn Child + Send + Sync>,
    /// Output buffer for late attachment (agent terminals only)
    pub output_buffer: Option<Arc<Mutex<VecDeque<u8>>>>,
    /// Whether this session was spawned by an agent (vs user)
    #[allow(dead_code)]
    pub is_agent_spawned: bool,
}

/// Manages all active PTY sessions
pub struct PtyState {
    pub sessions: Mutex<HashMap<String, Arc<Mutex<PtySession>>>>,
}

impl Default for PtyState {
    fn default() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }
}

/// Maximum output buffer size (64KB) - same as commands.rs
pub const MAX_BUFFER_SIZE: usize = 64 * 1024;

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
