//! Core process management for Claude CLI
//!
//! This module handles the low-level process lifecycle management,
//! including process state tracking, warm process retention, and graceful shutdown.
//!
//! ## Model
//!
//! `RunHandle` represents a live process attachment. It tracks two orthogonal axes:
//! - **Lifecycle** (`RunLifecycle`): Starting → Live. Exited/Crashed are not in-memory
//!   states — once the process dies the handle is removed and DB is updated.
//! - **Occupancy** (`RunOccupancy`): Idle, ServingTurn, or AwaitingHost. Only Idle
//!   processes are GC-safe.
//!
//! `RunRegistry` replaces the raw HashMap, adding a session_id index for O(1) lookups.

use crate::services::ChildProcess;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
use std::any::Any;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Type alias for stdin handle to avoid clippy::type_complexity warning.
pub trait BackendStdin: Write + Send + Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub type StdinHandle = Arc<Mutex<Option<Box<dyn BackendStdin>>>>;

/// Wrapper for legacy stdin writers that only need plain I/O.
pub struct PlainStdin {
    inner: Box<dyn Write + Send>,
}

impl PlainStdin {
    pub fn new(inner: Box<dyn Write + Send>) -> Self {
        Self { inner }
    }
}

impl Write for PlainStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl BackendStdin for PlainStdin {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn wrap_plain_stdin(writer: Box<dyn Write + Send>) -> Box<dyn BackendStdin> {
    Box::new(PlainStdin::new(writer))
}

// ============================================================================
// Two-axis state model
// ============================================================================

/// Process lifecycle: is the OS process alive?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunLifecycle {
    /// Spawned, not yet producing events.
    Starting,
    /// Connected and responsive.
    Live,
    // Exited/Crashed are NOT in-memory states.
    // Once the process dies, the RunHandle is removed from the registry
    // and the DB record is updated.
}

/// Process occupancy: what is the process doing right now?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOccupancy {
    /// No turn in progress — warm, GC-safe.
    Idle,
    /// Actively working without a tracked turn ID (e.g. chat/direct-message resume).
    Busy,
    /// Actively executing a turn.
    ServingTurn(String),
    /// Turn yielded (ask_user/permission), process holds continuation. NOT GC-safe.
    AwaitingHost { turn_id: String },
}

// ============================================================================
// Backwards-compat aliases
// ============================================================================

/// Backwards-compatible alias. Use `RunLifecycle` for new code.
pub type ProcessState = RunLifecycle;

// ============================================================================
// RunHandle (replaces ActiveProcess)
// ============================================================================

/// A live process attachment with its stdin handle for bidirectional communication.
pub struct RunHandle {
    /// The child process handle
    pub child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
    /// The stdin handle for writing messages
    pub stdin: StdinHandle,
    /// Process lifecycle state
    pub lifecycle: RunLifecycle,
    /// Occupancy state (what the process is doing)
    pub occupancy: RunOccupancy,
    /// Last activity timestamp (for GC relevance scoring)
    pub last_activity: Instant,
    /// The backend session ID for this process
    pub session_id: Option<String>,
    /// The job ID associated with this process (if any)
    pub job_id: Option<String>,
    /// Cursor for channel message polling
    pub message_cursor: Arc<Mutex<i64>>,
    /// Backend name ("codex", etc.). None = Claude (default).
    pub backend: Option<String>,
    /// Set when a terminal tool triggers the interrupt.
    pub terminal_tool_called: Arc<AtomicBool>,
}

/// Backwards-compatible alias.
pub type ActiveProcess = RunHandle;

impl RunHandle {
    /// Create a new run handle in Starting/Idle state.
    pub fn new(
        child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
        stdin: StdinHandle,
        session_id: Option<String>,
        job_id: Option<String>,
    ) -> Self {
        Self {
            child,
            stdin,
            lifecycle: RunLifecycle::Starting,
            occupancy: RunOccupancy::Idle,
            last_activity: Instant::now(),
            session_id,
            job_id,
            message_cursor: Arc::new(Mutex::new(chrono::Utc::now().timestamp())),
            backend: None,
            terminal_tool_called: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether the process is warm (live + idle, GC candidate).
    pub fn is_warm(&self) -> bool {
        self.lifecycle == RunLifecycle::Live && self.occupancy == RunOccupancy::Idle
    }

    /// Whether the process can safely be evicted by GC.
    /// Only truly warm processes can be evicted.
    pub fn is_gc_safe(&self) -> bool {
        self.is_warm()
    }

    /// Whether the process is actively working (serving a turn or awaiting host).
    pub fn is_active(&self) -> bool {
        !matches!(self.occupancy, RunOccupancy::Idle)
    }

    /// Begin a new turn.
    pub fn begin_turn(&mut self, turn_id: &str) {
        self.occupancy = RunOccupancy::ServingTurn(turn_id.to_string());
        self.last_activity = Instant::now();
        self.terminal_tool_called.store(false, Ordering::Release);
    }

    /// Yield the current turn for host interaction (ask_user, permission).
    pub fn yield_for_host(&mut self, turn_id: &str) {
        self.occupancy = RunOccupancy::AwaitingHost {
            turn_id: turn_id.to_string(),
        };
        self.last_activity = Instant::now();
    }

    /// End the current turn, transition to idle.
    pub fn end_turn(&mut self) {
        self.occupancy = RunOccupancy::Idle;
        self.last_activity = Instant::now();
    }

    /// Get the current turn ID (if serving or awaiting).
    pub fn current_turn_id(&self) -> Option<&str> {
        match &self.occupancy {
            RunOccupancy::Busy => None,
            RunOccupancy::ServingTurn(id) => Some(id),
            RunOccupancy::AwaitingHost { turn_id } => Some(turn_id),
            RunOccupancy::Idle => None,
        }
    }

    // === Backwards-compatible transition methods ===

    /// Transition process to warm state (after completing a turn).
    /// Equivalent to `end_turn()` + setting lifecycle to Live.
    pub fn transition_to_warm(&mut self) {
        self.lifecycle = RunLifecycle::Live;
        self.occupancy = RunOccupancy::Idle;
        self.last_activity = Instant::now();
    }

    /// Transition process to active state (when starting a new turn).
    /// For backwards compat — prefer `begin_turn(turn_id)` when a real turn exists.
    pub fn transition_to_active(&mut self) {
        self.lifecycle = RunLifecycle::Live;
        if matches!(self.occupancy, RunOccupancy::Idle) {
            self.occupancy = RunOccupancy::Busy;
        }
        self.last_activity = Instant::now();
        self.terminal_tool_called.store(false, Ordering::Release);
    }

    /// Update last activity timestamp
    #[allow(dead_code)]
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Get seconds since last activity
    pub fn seconds_since_activity(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }
}

// ============================================================================
// RunRegistry (replaces raw HashMap)
// ============================================================================

/// Registry for active run handles, with session_id index.
#[derive(Default)]
pub struct RunRegistry {
    by_id: HashMap<String, RunHandle>,
    session_index: HashMap<String, String>, // session_id → run_id
}

impl RunRegistry {
    pub fn get(&self, run_id: &str) -> Option<&RunHandle> {
        self.by_id.get(run_id)
    }

    pub fn get_mut(&mut self, run_id: &str) -> Option<&mut RunHandle> {
        self.by_id.get_mut(run_id)
    }

    pub fn get_by_session(&self, session_id: &str) -> Option<&str> {
        self.session_index.get(session_id).map(|s| s.as_str())
    }

    pub fn register(&mut self, run_id: String, handle: RunHandle) {
        if let Some(ref sid) = handle.session_id {
            self.session_index.insert(sid.clone(), run_id.clone());
        }
        self.by_id.insert(run_id, handle);
    }

    pub fn remove(&mut self, run_id: &str) -> Option<RunHandle> {
        if let Some(handle) = self.by_id.remove(run_id) {
            if let Some(ref sid) = handle.session_id {
                self.session_index.remove(sid);
            }
            Some(handle)
        } else {
            None
        }
    }

    pub fn contains_key(&self, run_id: &str) -> bool {
        self.by_id.contains_key(run_id)
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &RunHandle)> {
        self.by_id.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut RunHandle)> {
        self.by_id.iter_mut()
    }

    pub fn values(&self) -> impl Iterator<Item = &RunHandle> {
        self.by_id.values()
    }
}

/// Type alias for backwards compatibility with code that uses `ProcessMap`.
pub type ProcessMap = RunRegistry;

// ============================================================================
// AgentProcessState
// ============================================================================

/// State for tracking active agent processes
pub struct AgentProcessState {
    /// Active processes keyed by run_id
    pub processes: Mutex<RunRegistry>,
    /// Cached path to default CLI binary (resolved once on first use)
    pub cli_binary_path: Mutex<Option<String>>,
    /// Last manager context sent per job_id
    pub last_manager_context: Mutex<HashMap<String, String>>,
}

impl AgentProcessState {
    /// Get the stdin Arc for a run.
    pub fn get_stdin_handle(&self, run_id: &str) -> Option<StdinHandle> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).map(|p| p.stdin.clone())
    }

    /// Get the lifecycle state of a process by run_id.
    pub fn get_process_state(&self, run_id: &str) -> Option<RunLifecycle> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).map(|p| p.lifecycle)
    }

    /// Get the occupancy of a process by run_id.
    pub fn get_occupancy(&self, run_id: &str) -> Option<RunOccupancy> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).map(|p| p.occupancy.clone())
    }

    /// Transition a process to warm state.
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

    /// Transition a process to active state (begin serving).
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

    /// Begin a turn on a process.
    pub fn begin_turn(&self, run_id: &str, turn_id: &str) -> bool {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                process.begin_turn(turn_id);
                return true;
            }
        }
        false
    }

    /// Yield a turn for host interaction.
    pub fn yield_for_host(&self, run_id: &str, turn_id: &str) -> bool {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                process.yield_for_host(turn_id);
                return true;
            }
        }
        false
    }

    /// Whether the run currently has a live process attachment waiting on the host.
    pub fn is_awaiting_host(&self, run_id: &str, turn_id: Option<&str>) -> bool {
        let Ok(processes) = self.processes.lock() else {
            return false;
        };
        let Some(process) = processes.get(run_id) else {
            return false;
        };
        match &process.occupancy {
            RunOccupancy::AwaitingHost {
                turn_id: active_turn_id,
            } => turn_id
                .map(|expected| expected == active_turn_id)
                .unwrap_or(true),
            _ => false,
        }
    }

    /// End the current turn on a process.
    pub fn end_turn(&self, run_id: &str) -> bool {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                process.end_turn();
                return true;
            }
        }
        false
    }

    /// Get list of GC-safe (idle) processes, sorted by last activity (oldest first).
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

        // Sort by seconds since activity (oldest first)
        warm.sort_by_key(|(_, secs, _)| *secs);

        warm
    }

    /// Count GC-safe (idle) processes.
    pub fn warm_process_count(&self) -> usize {
        self.processes
            .lock()
            .map(|p| p.values().filter(|proc| proc.is_warm()).count())
            .unwrap_or(0)
    }

    /// Count active processes (serving turn or awaiting host).
    pub fn active_process_count(&self) -> usize {
        self.processes
            .lock()
            .map(|p| p.values().filter(|proc| proc.is_active()).count())
            .unwrap_or(0)
    }

    /// Find a warm process by session_id (for reuse on follow-up).
    #[allow(dead_code)]
    pub fn find_warm_by_session(&self, session_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        let result = processes
            .iter()
            .find(|(_, p)| {
                p.is_warm() && p.session_id.as_ref().is_some_and(|sid| sid == session_id)
            })
            .map(|(run_id, _)| run_id.clone());
        result
    }

    /// Get the message cursor for a run and advance it to `now`.
    pub fn advance_message_cursor(&self, run_id: &str) -> Option<i64> {
        let processes = self.processes.lock().ok()?;
        let process = processes.get(run_id)?;
        let mut cursor = process.message_cursor.lock().ok()?;
        let prev = *cursor;
        *cursor = chrono::Utc::now().timestamp();
        Some(prev)
    }

    /// Mark that a terminal tool was called for this run.
    pub fn mark_terminal_tool(&self, run_id: &str) -> Option<Arc<AtomicBool>> {
        let processes = self.processes.lock().ok()?;
        let process = processes.get(run_id)?;
        process.terminal_tool_called.store(true, Ordering::Release);
        Some(process.terminal_tool_called.clone())
    }

    /// Get the current turn ID for a process by run_id.
    pub fn get_current_turn_id(&self, run_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id)?.current_turn_id().map(String::from)
    }

    /// Set the current turn ID by beginning a turn (or clearing with end_turn).
    pub fn set_current_turn_id(&self, run_id: &str, turn_id: Option<&str>) {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                if let Some(tid) = turn_id {
                    process.begin_turn(tid);
                } else {
                    process.end_turn();
                }
            }
        }
    }

    /// Find any process (active or warm) by session_id.
    pub fn find_process_by_session(&self, session_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        // Use session index for O(1) lookup
        if let Some(run_id) = processes.get_by_session(session_id) {
            return Some(run_id.to_string());
        }
        None
    }

    /// Remove a process by session_id. Returns the run_id if found.
    /// Used to evict warm processes when sessions are closed.
    pub fn remove_by_session(&self, session_id: &str) -> Option<String> {
        let mut processes = self.processes.lock().ok()?;
        if let Some(run_id) = processes.get_by_session(session_id).map(|s| s.to_string()) {
            processes.remove(&run_id);
            Some(run_id)
        } else {
            None
        }
    }

    /// Find a warm process by job_id.
    #[allow(dead_code)]
    pub fn find_warm_by_job(&self, job_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        let result = processes
            .iter()
            .find(|(_, p)| p.is_warm() && p.job_id.as_ref().is_some_and(|jid| jid == job_id))
            .map(|(run_id, _)| run_id.clone());
        result
    }
}

impl Default for AgentProcessState {
    fn default() -> Self {
        Self {
            processes: Mutex::new(RunRegistry::default()),
            cli_binary_path: Mutex::new(None),
            last_manager_context: Mutex::new(HashMap::new()),
        }
    }
}

/// Gracefully stop Claude process (SIGTERM, wait, fallback to SIGKILL on Unix; direct kill on Windows)
#[cfg(unix)]
pub fn graceful_stop(child: &mut dyn ChildProcess) {
    let pid = Pid::from_raw(child.id() as i32);

    // Try SIGTERM first
    if kill(pid, Signal::SIGTERM).is_ok() {
        for _ in 0..30 {
            if let Ok(Some(_)) = child.try_wait() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // Fallback to kill()
    let _ = child.kill();
}

/// Gracefully stop Claude process (Windows version)
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
    fn test_agent_process_state_default() {
        let state = AgentProcessState::default();
        let processes = state.processes.lock().unwrap();
        assert!(processes.is_empty());
    }

    #[test]
    fn test_run_handle_lifecycle() {
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        let mut handle = RunHandle::new(child, stdin, Some("session-1".to_string()), None);

        // Starts in Starting/Idle
        assert_eq!(handle.lifecycle, RunLifecycle::Starting);
        assert!(matches!(handle.occupancy, RunOccupancy::Idle));
        assert!(!handle.is_gc_safe());
        assert!(!handle.is_warm());
        assert!(!handle.is_active());

        // Begin a turn
        handle.lifecycle = RunLifecycle::Live;
        handle.begin_turn("turn-1");
        assert!(handle.is_active());
        assert!(!handle.is_gc_safe());
        assert_eq!(handle.current_turn_id(), Some("turn-1"));

        // Yield for host
        handle.yield_for_host("turn-1");
        assert!(handle.is_active());
        assert!(!handle.is_gc_safe()); // NOT GC-safe while awaiting
        assert_eq!(handle.current_turn_id(), Some("turn-1"));

        // End turn
        handle.end_turn();
        assert!(!handle.is_active());
        assert!(handle.is_gc_safe());
        assert!(handle.is_warm());
        assert_eq!(handle.current_turn_id(), None);
    }

    #[test]
    fn test_starting_idle_process_is_not_counted_as_warm() {
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let handle = RunHandle::new(child, stdin, Some("session-1".to_string()), None);
            processes.register("run-1".to_string(), handle);
        }

        assert_eq!(state.warm_process_count(), 0);
        assert_eq!(state.active_process_count(), 0);
        assert_eq!(state.find_warm_by_session("session-1"), None);
        assert!(state.warm_processes().is_empty());
    }

    #[test]
    fn test_warm_process_tracking() {
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let mut handle = RunHandle::new(
                child,
                stdin,
                Some("session-1".to_string()),
                Some("job-1".to_string()),
            );
            handle.transition_to_warm();
            processes.register("run-1".to_string(), handle);
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
        assert_eq!(state.get_current_turn_id("run-1"), None);
    }

    #[test]
    fn test_run_registry_session_index() {
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let handle = RunHandle::new(child, stdin, Some("session-abc".to_string()), None);
            processes.register("run-1".to_string(), handle);
        }

        assert_eq!(
            state.find_process_by_session("session-abc"),
            Some("run-1".to_string())
        );
        assert_eq!(state.find_process_by_session("nonexistent"), None);
    }

    #[test]
    fn test_terminal_tool_flag_starts_false() {
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        let handle = RunHandle::new(child, stdin, None, None);
        assert!(!handle.terminal_tool_called.load(Ordering::Acquire));
    }

    #[test]
    fn test_begin_turn_clears_terminal_tool_flag() {
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let mut handle = RunHandle::new(
                child,
                stdin,
                Some("session-1".to_string()),
                Some("job-1".to_string()),
            );
            handle.transition_to_warm();
            handle.terminal_tool_called.store(true, Ordering::Release);
            processes.register("run-1".to_string(), handle);
        }

        state.begin_turn("run-1", "turn-1");

        let processes = state.processes.lock().unwrap();
        let handle = processes.get("run-1").unwrap();
        assert!(!handle.terminal_tool_called.load(Ordering::Acquire));
        assert!(handle.is_active());
    }

    #[test]
    fn test_mark_terminal_tool_sets_flag() {
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let handle = RunHandle::new(child, stdin, None, None);
            processes.register("run-1".to_string(), handle);
        }

        let flag = state.mark_terminal_tool("run-1");
        assert!(flag.is_some());
        assert!(flag.unwrap().load(Ordering::Acquire));
    }

    #[test]
    fn test_mark_terminal_tool_missing_process() {
        let state = AgentProcessState::default();
        assert!(state.mark_terminal_tool("nonexistent").is_none());
    }

    #[test]
    fn test_awaiting_host_not_gc_safe() {
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        let mut handle = RunHandle::new(child, stdin, None, None);
        handle.lifecycle = RunLifecycle::Live;
        handle.begin_turn("turn-1");
        handle.yield_for_host("turn-1");

        assert!(!handle.is_gc_safe());
        assert!(handle.is_active());
    }

    #[test]
    fn test_registry_remove_cleans_session_index() {
        let mut registry = RunRegistry::default();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        let handle = RunHandle::new(child, stdin, Some("session-x".to_string()), None);

        registry.register("run-1".to_string(), handle);
        assert!(registry.get_by_session("session-x").is_some());

        registry.remove("run-1");
        assert!(
            registry.get_by_session("session-x").is_none(),
            "session index should be cleaned up on remove"
        );
        assert!(registry.is_empty());
    }

    #[test]
    fn test_registry_register_without_session_id() {
        let mut registry = RunRegistry::default();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        let handle = RunHandle::new(child, stdin, None, None);

        registry.register("run-1".to_string(), handle);
        assert!(registry.get("run-1").is_some());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let mut registry = RunRegistry::default();
        assert!(registry.remove("nonexistent").is_none());
    }
}
