//! Core process management for Claude CLI
//!
//! This module handles the low-level process lifecycle management,
//! including process state tracking, warm process retention, and graceful shutdown.
//!
//! ## Warm Process Retention
//!
//! Instead of killing Claude processes after each turn, processes can be kept "warm"
//! for potential follow-up. This preserves Claude's conversation cache and enables
//! faster subsequent turns without spawning new processes.

use crate::services::ChildProcess;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Type alias for stdin handle to avoid clippy::type_complexity warning.
pub type StdinHandle = Arc<Mutex<Option<Box<dyn Write + Send>>>>;

/// State of a Claude process in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// Process is actively executing a turn (doing work)
    Active,
    /// Process completed a turn and is idle, waiting for potential follow-up
    Warm,
}

/// An active Claude process with its stdin handle for bidirectional communication.
pub struct ActiveProcess {
    /// The child process handle
    pub child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
    /// The stdin handle for writing messages (only available in bidirectional mode)
    pub stdin: StdinHandle,
    /// Current state of the process
    pub state: ProcessState,
    /// Last activity timestamp (for GC relevance scoring)
    pub last_activity: Instant,
    /// The Claude session ID for this process
    pub session_id: Option<String>,
    /// The job ID associated with this process (if any)
    pub job_id: Option<String>,
    /// Cursor for channel message polling. Tracks the last message timestamp
    /// seen by this process so hooks can pull only new messages.
    pub message_cursor: Arc<Mutex<i64>>,
}

impl ActiveProcess {
    /// Create a new active process
    pub fn new(
        child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
        stdin: StdinHandle,
        session_id: Option<String>,
        job_id: Option<String>,
    ) -> Self {
        Self {
            child,
            stdin,
            state: ProcessState::Active,
            last_activity: Instant::now(),
            session_id,
            job_id,
            message_cursor: Arc::new(Mutex::new(chrono::Utc::now().timestamp())),
        }
    }

    /// Transition process to warm state (after completing a turn)
    pub fn transition_to_warm(&mut self) {
        self.state = ProcessState::Warm;
        self.last_activity = Instant::now();
    }

    /// Transition process back to active state (when starting a new turn)
    pub fn transition_to_active(&mut self) {
        self.state = ProcessState::Active;
        self.last_activity = Instant::now();
    }

    /// Update last activity timestamp
    #[allow(dead_code)]
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Check if this process is warm (idle and available for follow-up)
    pub fn is_warm(&self) -> bool {
        self.state == ProcessState::Warm
    }

    /// Check if this process is active (currently executing)
    pub fn is_active(&self) -> bool {
        self.state == ProcessState::Active
    }

    /// Get seconds since last activity
    pub fn seconds_since_activity(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }
}

/// Type alias for process map to avoid clippy::type_complexity warning.
pub type ProcessMap = HashMap<String, ActiveProcess>;

/// State for tracking active Claude processes
pub struct ClaudeProcessState {
    /// Active processes keyed by run_id
    pub processes: Mutex<ProcessMap>,
    /// Cached path to claude binary (resolved once on first use)
    pub claude_path: Mutex<Option<String>>,
}

impl ClaudeProcessState {
    /// Get the stdin Arc for a run, allowing the caller to lock and write to it.
    /// Returns None if the process doesn't exist.
    pub fn get_stdin_handle(&self, run_id: &str) -> Option<StdinHandle> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).map(|p| p.stdin.clone())
    }

    /// Get the state of a process by run_id.
    /// Returns None if the process doesn't exist.
    pub fn get_process_state(&self, run_id: &str) -> Option<ProcessState> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).map(|p| p.state)
    }

    /// Transition a process to warm state.
    /// Returns true if the process was found and transitioned.
    pub fn transition_to_warm(&self, run_id: &str) -> bool {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                process.transition_to_warm();
                log::info!("Process {} transitioned to warm state", run_id);
                return true;
            }
        }
        false
    }

    /// Transition a process back to active state.
    /// Returns true if the process was found and transitioned.
    pub fn transition_to_active(&self, run_id: &str) -> bool {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                process.transition_to_active();
                log::info!("Process {} transitioned to active state", run_id);
                return true;
            }
        }
        false
    }

    /// Get list of warm processes, sorted by last activity (oldest first).
    /// Returns (run_id, seconds_since_activity, job_id) tuples.
    pub fn warm_processes(&self) -> Vec<(String, u64, Option<String>)> {
        let processes = match self.processes.lock() {
            Ok(p) => p,
            Err(_) => return vec![],
        };

        let mut warm: Vec<_> = processes
            .iter()
            .filter(|(_, p)| p.is_warm())
            .map(|(run_id, p)| (run_id.clone(), p.seconds_since_activity(), p.job_id.clone()))
            .collect();

        // Sort by seconds since activity (oldest first - most likely to be evicted)
        warm.sort_by_key(|(_, secs, _)| std::cmp::Reverse(*secs));
        warm.reverse();

        warm
    }

    /// Count warm processes.
    pub fn warm_process_count(&self) -> usize {
        self.processes
            .lock()
            .map(|p| p.values().filter(|proc| proc.is_warm()).count())
            .unwrap_or(0)
    }

    /// Count active processes.
    pub fn active_process_count(&self) -> usize {
        self.processes
            .lock()
            .map(|p| p.values().filter(|proc| proc.is_active()).count())
            .unwrap_or(0)
    }

    /// Find a warm process by session_id (for reuse on follow-up).
    /// Returns the run_id if found.
    #[allow(dead_code)]
    pub fn find_warm_by_session(&self, session_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes
            .iter()
            .find(|(_, p)| {
                p.is_warm() && p.session_id.as_ref().is_some_and(|sid| sid == session_id)
            })
            .map(|(run_id, _)| run_id.clone())
    }

    /// Get the message cursor for a run and advance it to `now`.
    /// Returns the previous cursor value (query messages since this timestamp).
    pub fn advance_message_cursor(&self, run_id: &str) -> Option<i64> {
        let processes = self.processes.lock().ok()?;
        let process = processes.get(run_id)?;
        let mut cursor = process.message_cursor.lock().ok()?;
        let prev = *cursor;
        *cursor = chrono::Utc::now().timestamp();
        Some(prev)
    }

    /// Find any process (active or warm) by session_id.
    /// Returns the run_id if found.
    pub fn find_process_by_session(&self, session_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes
            .iter()
            .find(|(_, p)| p.session_id.as_ref().is_some_and(|sid| sid == session_id))
            .map(|(run_id, _)| run_id.clone())
    }

    /// Find a warm process by job_id (for reuse on continue_job).
    /// Returns the run_id if found.
    #[allow(dead_code)]
    pub fn find_warm_by_job(&self, job_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes
            .iter()
            .find(|(_, p)| p.is_warm() && p.job_id.as_ref().is_some_and(|jid| jid == job_id))
            .map(|(run_id, _)| run_id.clone())
    }
}

impl Default for ClaudeProcessState {
    fn default() -> Self {
        Self {
            processes: Mutex::new(HashMap::new()),
            claude_path: Mutex::new(None),
        }
    }
}

/// Gracefully stop Claude process (SIGTERM, wait, fallback to SIGKILL on Unix; direct kill on Windows)
/// Works with both real processes and mock processes.
#[cfg(unix)]
pub fn graceful_stop(child: &mut dyn ChildProcess) {
    let pid = Pid::from_raw(child.id() as i32);

    // Try SIGTERM first (only works for real processes)
    if kill(pid, Signal::SIGTERM).is_ok() {
        // Wait up to 3 seconds for graceful exit
        for _ in 0..30 {
            if let Ok(Some(_)) = child.try_wait() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // Fallback to kill() - works for both real and mock processes
    let _ = child.kill();
}

/// Gracefully stop Claude process (Windows version - uses TerminateProcess)
/// Works with both real processes and mock processes.
#[cfg(windows)]
pub fn graceful_stop(child: &mut dyn ChildProcess) {
    let _ = child.kill();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::MockChildProcess;

    #[test]
    fn test_graceful_stop_kills_process() {
        let mut mock = MockChildProcess::with_stdout(123, vec![]);
        assert!(mock.try_wait().unwrap().is_none());
        graceful_stop(&mut mock);
        assert!(mock.try_wait().unwrap().is_some());
    }

    #[test]
    fn test_graceful_stop_already_exited() {
        let mut mock = MockChildProcess::with_stdout(123, vec![]);
        mock.set_exited();
        assert!(mock.try_wait().unwrap().is_some());
        graceful_stop(&mut mock);
        assert!(mock.try_wait().unwrap().is_some());
    }

    #[test]
    fn test_claude_process_state_default() {
        let state = ClaudeProcessState::default();
        let processes = state.processes.lock().unwrap();
        assert!(processes.is_empty());
    }

    #[test]
    fn test_process_state_transitions() {
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        let mut process = ActiveProcess::new(child, stdin, Some("session-1".to_string()), None);

        assert!(process.is_active());
        assert!(!process.is_warm());

        process.transition_to_warm();
        assert!(process.is_warm());
        assert!(!process.is_active());

        process.transition_to_active();
        assert!(process.is_active());
    }

    #[test]
    fn test_warm_process_tracking() {
        let state = ClaudeProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let mut process = ActiveProcess::new(
                child,
                stdin,
                Some("session-1".to_string()),
                Some("job-1".to_string()),
            );
            process.transition_to_warm();
            processes.insert("run-1".to_string(), process);
        }

        assert_eq!(state.warm_process_count(), 1);
        assert_eq!(state.active_process_count(), 0);
        assert_eq!(
            state.find_warm_by_session("session-1"),
            Some("run-1".to_string())
        );
        assert_eq!(state.find_warm_by_job("job-1"), Some("run-1".to_string()));

        assert!(state.transition_to_active("run-1"));
        assert_eq!(state.warm_process_count(), 0);
        assert_eq!(state.active_process_count(), 1);
    }
}
