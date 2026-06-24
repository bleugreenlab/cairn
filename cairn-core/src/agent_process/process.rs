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
use std::collections::{HashMap, HashSet};
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
// Turn-scoped flail dedup (CAIRN-1230)
// ============================================================================

/// Per-run record of the read-family calls seen in the current turn, each with
/// the content hash it last returned.
///
/// Dedup is content-aware (CAIRN-1271): a repeated identical call is only a
/// duplicate when it returns the *same content* as its prior occurrence this
/// turn. When the underlying resource changed since the last read, the repeat
/// returns fresh content and the stored hash is updated. This subsumes the old
/// terminal special-case — a cursor-advancing terminal poll naturally produces
/// changed content while there is new output and dedups only once it has gone
/// genuinely quiet.
///
/// The set self-resets whenever the live turn id differs from `turn_id` — this
/// is the single source of truth for turn scoping, so there is no `begin_turn`
/// hook to keep in sync. It dies with its [`RunHandle`], exactly like
/// `consumed_uris`, so there is no global map to sweep and nothing to leak.
#[derive(Default)]
pub struct TurnSeenCalls {
    /// The turn these fingerprints belong to. When the live turn id differs,
    /// the set is reset before recording.
    turn_id: Option<String>,
    /// Normalized call fingerprint -> the record of its occurrences this turn.
    calls: HashMap<String, CallRecord>,
}

/// One read-family fingerprint's history within a turn.
#[derive(Default)]
struct CallRecord {
    /// Number of times this exact call has been made this turn (>= 1).
    count: u32,
    /// Hash of the content the most recent occurrence returned.
    content_hash: u64,
    /// Whether the most recent occurrence returned content identical to the one
    /// before it (a genuine duplicate). Drives the broad-thrash signal.
    is_dup: bool,
}

/// Result of recording a read-family call (with the content it returned)
/// against the current turn's seen-set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupOutcome {
    /// No tracked process, or no active turn id -> do not dedup.
    PassThrough,
    /// First occurrence of this fingerprint this turn -> return fresh content.
    First,
    /// Repeat of this fingerprint this turn whose content *changed* since the
    /// last occurrence -> return the fresh content. `count` is this call's
    /// occurrence number (>= 2).
    Changed { count: u32 },
    /// Repeat of this fingerprint this turn whose content is *identical* to the
    /// last occurrence -> return the duplicate stub. `count` is this call's
    /// occurrence number (>= 2); `distinct_dupes` is how many distinct
    /// fingerprints have produced an identical-content repeat this turn (the
    /// broad-thrash signal).
    Duplicate { count: u32, distinct_dupes: usize },
}

// ============================================================================
// RunHandle (replaces ActiveProcess)
// ============================================================================

/// Why an owned-loop backend's turn was suspended at a tool boundary.
///
/// OpenRouter owns its turn/tool loop in-process and has no warm process, so a
/// foreground question or inline delegated-task append suspends the turn
/// immediately rather than inline-waiting. The handler records the reason in the
/// run's [`RunHandle::pending_suspend`] slot; the owned loop reads it back right
/// after the tool dispatch returns and finalizes the run into a resumable
/// waiting state. This is a structured side-channel keyed by `run_id`, the same
/// per-run loop-control shape as the cancellation flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuspendKind {
    /// A foreground question was asked; resume when the user answers.
    Prompt,
    /// An inline (non-background) delegated task was spawned; resume when the
    /// child task(s) complete.
    DelegatedTask,
}

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
    /// The effective model this process was started with. Used by warm reuse to
    /// detect when the job's requested model has diverged from the live process.
    pub model: Option<String>,
    /// Set when a turn-ending artifact/tool should trigger a boundary interrupt.
    pub terminal_tool_called: Arc<AtomicBool>,
    /// cairn:// resources this run has read.
    /// Lazily seeded once from prior events, then appended on each read.
    pub consumed_uris: Arc<Mutex<HashSet<String>>>,
    /// Whether `consumed_uris` has been seeded from prior events yet.
    pub consumed_seeded: Arc<AtomicBool>,
    /// The session position vector used at the last recommendation. The
    /// reserved for semantic surfacing; only changes when the live position moves;
    /// a run of reads with an unchanged position recommends only on the first.
    pub last_recommend_pos: Arc<Mutex<Option<Vec<u8>>>>,
    /// Turn-scoped fingerprints of tool calls already made this turn, for
    /// flail dedup (CAIRN-1230). Self-resets on turn change.
    pub turn_seen_calls: Arc<Mutex<TurnSeenCalls>>,
    /// Whether the backend driving this run owns its turn/tool loop in-process
    /// (OpenRouter). Owned-loop runs suspend on a foreground question or inline
    /// task instead of inline-waiting; warm-process backends (Claude/Codex)
    /// leave this false and keep their existing blocking behavior.
    pub owns_turn_loop: bool,
    /// Structured suspend request for an owned-loop run, set by a blocking
    /// handler and consumed by the owned loop after the tool dispatch returns.
    pub pending_suspend: Arc<Mutex<Option<SuspendKind>>>,
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
            model: None,
            terminal_tool_called: Arc::new(AtomicBool::new(false)),
            consumed_uris: Arc::new(Mutex::new(HashSet::new())),
            consumed_seeded: Arc::new(AtomicBool::new(false)),
            last_recommend_pos: Arc::new(Mutex::new(None)),
            turn_seen_calls: Arc::new(Mutex::new(TurnSeenCalls::default())),
            owns_turn_loop: false,
            pending_suspend: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_handle(session_id: Option<&str>, job_id: Option<&str>) -> Self {
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(None));
        Self::new(
            child,
            stdin,
            session_id.map(str::to_string),
            job_id.map(str::to_string),
        )
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
        // The terminal-tool boundary interrupt is armed by an output-artifact
        // write to end *this* turn. Clear it when the turn ends so it cannot
        // bleed into the next turn on the same warm process (e.g. the
        // post-completion memory-review turn).
        self.terminal_tool_called.store(false, Ordering::Release);
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
        // Clear the terminal-tool arm when the turn warms. The arm is scoped to
        // the turn that wrote the terminal artifact; carrying it through the
        // warm idle period would re-fire the interrupt (and suppress events) on
        // the next turn the warm process serves — notably the memory-review turn,
        // whose resume prompt is delivered before the next-turn reset would run.
        // Safe for EOF classification: a run that reached warm reads `was_warm`
        // at EOF, which dominates the terminal-tool flag in `classify_eof`.
        self.terminal_tool_called.store(false, Ordering::Release);
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

    /// Whether the process for `run_id` is currently mid-turn
    /// (ServingTurn, AwaitingHost, or Busy). Returns `false` if no live
    /// handle is registered — mirrors [`RunHandle::is_active`] at the
    /// state level so callers can ask "is this recipient mid-turn right
    /// now?" without reaching into the lock themselves.
    pub fn is_active(&self, run_id: &str) -> bool {
        let Ok(processes) = self.processes.lock() else {
            return false;
        };
        processes
            .get(run_id)
            .map(|p| p.is_active())
            .unwrap_or(false)
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

    /// Arm the boundary-interrupt flag for a run-ending artifact/tool.
    pub fn arm_terminal_tool(&self, run_id: &str) -> bool {
        if let Ok(processes) = self.processes.lock() {
            if let Some(process) = processes.get(run_id) {
                process.terminal_tool_called.store(true, Ordering::Release);
                log::info!(
                    "Armed terminal-tool boundary interrupt for run {}",
                    &run_id[..run_id.len().min(8)]
                );
                return true;
            }
        }
        false
    }

    /// Whether the terminal-tool boundary flag is armed for a run. Owned-loop
    /// backends (OpenRouter) read this after a tool boundary to end the turn once
    /// the agent has written its output artifact, mirroring the boundary
    /// interrupt the warm-process backends send at the same point.
    pub fn terminal_tool_armed(&self, run_id: &str) -> bool {
        self.processes
            .lock()
            .ok()
            .and_then(|processes| {
                processes
                    .get(run_id)
                    .map(|process| process.terminal_tool_called.load(Ordering::Acquire))
            })
            .unwrap_or(false)
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

    /// Record that a run read a cairn:// resource (adds to the consumed-set).
    pub fn note_read(&self, run_id: &str, uri: &str) {
        if let Ok(processes) = self.processes.lock() {
            if let Some(process) = processes.get(run_id) {
                if let Ok(mut consumed) = process.consumed_uris.lock() {
                    consumed.insert(uri.to_string());
                }
            }
        }
    }

    /// Snapshot the consumed-set for a run, if the run exists.
    pub fn consumed_uris(&self, run_id: &str) -> Option<HashSet<String>> {
        let processes = self.processes.lock().ok()?;
        let process = processes.get(run_id)?;
        let consumed = process.consumed_uris.lock().ok()?;
        Some(consumed.clone())
    }

    /// Bulk-add URIs to a run's consumed-set (used for one-time event seeding).
    pub fn extend_consumed(&self, run_id: &str, uris: impl IntoIterator<Item = String>) {
        if let Ok(processes) = self.processes.lock() {
            if let Some(process) = processes.get(run_id) {
                if let Ok(mut consumed) = process.consumed_uris.lock() {
                    consumed.extend(uris);
                }
            }
        }
    }

    /// Atomically claim the one-time consumed-set seed for a run. Returns `true`
    /// to exactly one caller (the first), which then seeds from prior events;
    /// subsequent calls return `false`. Returns `false` if the run is unknown.
    pub fn claim_consumed_seed(&self, run_id: &str) -> bool {
        let Ok(processes) = self.processes.lock() else {
            return false;
        };
        let Some(process) = processes.get(run_id) else {
            return false;
        };
        process
            .consumed_seeded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Whether the live position has moved since the last recommendation. On a
    /// change (or first call), records `pos` as the new baseline and returns
    /// `true`; on an unchanged position returns `false` so a run of reads at the
    /// same position recommends only once. Returns `false` if the run is unknown.
    pub fn recommend_pos_changed(&self, run_id: &str, pos: &[u8]) -> bool {
        let Ok(processes) = self.processes.lock() else {
            return false;
        };
        let Some(process) = processes.get(run_id) else {
            return false;
        };
        let Ok(mut last) = process.last_recommend_pos.lock() else {
            return false;
        };
        if last.as_deref() == Some(pos) {
            return false;
        }
        *last = Some(pos.to_vec());
        true
    }

    /// Record a read-family call's returned content against the current turn and
    /// report whether it is a content-level duplicate (content-aware flail
    /// dedup, CAIRN-1271).
    ///
    /// Returns [`DedupOutcome::PassThrough`] when there is no tracked process
    /// for `run_id` or the process has no active turn id (Idle/Busy, or an
    /// external/untracked run) — in those cases dedup is disabled and the call
    /// executes normally. On the first occurrence this turn it records the
    /// fingerprint and its `content_hash` and returns [`DedupOutcome::First`].
    /// On a repeat it compares `content_hash` against the stored hash:
    /// identical content returns [`DedupOutcome::Duplicate`] (the caller serves
    /// a stub); changed content updates the stored hash and returns
    /// [`DedupOutcome::Changed`] (the caller serves the fresh content).
    ///
    /// The seen-set self-resets when the live turn id differs from the one it
    /// last recorded, so turn-to-turn and `Busy -> ServingTurn` transitions are
    /// handled without any `begin_turn` hook. The `processes` lock serializes
    /// concurrent calls (the parallel-block case).
    pub fn check_and_record_content(
        &self,
        run_id: &str,
        fingerprint: &str,
        content_hash: u64,
    ) -> DedupOutcome {
        let Ok(processes) = self.processes.lock() else {
            return DedupOutcome::PassThrough;
        };
        let Some(process) = processes.get(run_id) else {
            return DedupOutcome::PassThrough;
        };
        let Some(turn) = process.current_turn_id().map(str::to_string) else {
            return DedupOutcome::PassThrough; // Idle/Busy -> no dedup
        };
        let Ok(mut seen) = process.turn_seen_calls.lock() else {
            return DedupOutcome::PassThrough;
        };
        if seen.turn_id.as_deref() != Some(turn.as_str()) {
            seen.turn_id = Some(turn);
            seen.calls.clear();
        }
        let (count, unchanged) = {
            let record = seen.calls.entry(fingerprint.to_string()).or_default();
            record.count += 1;
            // Genuine duplicate only on a repeat whose content matches the prior
            // occurrence's. First occurrences and changed content are not.
            let unchanged = record.count > 1 && record.content_hash == content_hash;
            record.content_hash = content_hash;
            record.is_dup = unchanged;
            (record.count, unchanged)
        };
        if count == 1 {
            DedupOutcome::First
        } else if unchanged {
            let distinct_dupes = seen.calls.values().filter(|r| r.is_dup).count();
            DedupOutcome::Duplicate {
                count,
                distinct_dupes,
            }
        } else {
            DedupOutcome::Changed { count }
        }
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

    /// Whether the backend driving `run_id` owns its turn/tool loop in-process
    /// (OpenRouter). Returns false for warm-process backends and unknown runs.
    pub fn run_owns_turn_loop(&self, run_id: &str) -> bool {
        self.processes
            .lock()
            .ok()
            .and_then(|processes| processes.get(run_id).map(|process| process.owns_turn_loop))
            .unwrap_or(false)
    }

    /// Record a structured suspend request for an owned-loop run. Runs
    /// synchronously on the owned loop's tool-dispatch thread, so the loop reads
    /// it back immediately via [`take_suspend`](Self::take_suspend) with no race.
    pub fn request_suspend(&self, run_id: &str, kind: SuspendKind) {
        if let Ok(processes) = self.processes.lock() {
            if let Some(process) = processes.get(run_id) {
                if let Ok(mut slot) = process.pending_suspend.lock() {
                    *slot = Some(kind);
                }
            }
        }
    }

    /// Consume any pending suspend request for an owned-loop run.
    pub fn take_suspend(&self, run_id: &str) -> Option<SuspendKind> {
        let processes = self.processes.lock().ok()?;
        let process = processes.get(run_id)?;
        let mut slot = process.pending_suspend.lock().ok()?;
        slot.take()
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

    /// Get the effective model recorded for a process by run_id.
    pub fn get_model(&self, run_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).and_then(|p| p.model.clone())
    }

    /// Record the effective model for a process. Returns true if the run exists.
    pub fn set_model(&self, run_id: &str, model: &str) -> bool {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(process) = processes.get_mut(run_id) {
                process.model = Some(model.to_string());
                return true;
            }
        }
        false
    }

    /// Get the backend name for a process by run_id (None for Claude/default or
    /// when the run is unknown).
    pub fn get_backend(&self, run_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).and_then(|p| p.backend.clone())
    }

    /// Remove a process from the registry (clearing the session index) and then
    /// gracefully stop its child process outside the registry lock. Returns true
    /// if a process was found and removed.
    pub fn stop_and_remove(&self, run_id: &str) -> bool {
        let handle = match self.processes.lock() {
            Ok(mut processes) => processes.remove(run_id),
            Err(_) => return false,
        };
        let Some(handle) = handle else {
            return false;
        };
        if let Ok(mut child_guard) = handle.child.lock() {
            if let Some(child) = child_guard.as_mut() {
                graceful_stop(child.as_mut());
            }
        }
        true
    }
}

impl Default for AgentProcessState {
    fn default() -> Self {
        Self {
            processes: Mutex::new(RunRegistry::default()),
            cli_binary_path: Mutex::new(None),
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
    fn run_owns_turn_loop_defaults_false_and_reflects_flag() {
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-plain".to_string(),
                RunHandle::test_handle(Some("s-plain"), Some("j")),
            );
            let mut owned = RunHandle::test_handle(Some("s-owned"), Some("j"));
            owned.owns_turn_loop = true;
            processes.register("run-owned".to_string(), owned);
        }
        // Warm-process backends leave the flag false; OpenRouter sets it true.
        assert!(!state.run_owns_turn_loop("run-plain"));
        assert!(state.run_owns_turn_loop("run-owned"));
        // An unknown run is never owned-loop.
        assert!(!state.run_owns_turn_loop("missing"));
    }

    #[test]
    fn suspend_slot_round_trips_and_is_one_shot() {
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-1".to_string(),
                RunHandle::test_handle(Some("s"), Some("j")),
            );
        }
        // Nothing requested yet.
        assert_eq!(state.take_suspend("run-1"), None);
        // request_suspend then take_suspend round-trips the kind, and take is
        // one-shot: a second take sees nothing.
        state.request_suspend("run-1", SuspendKind::Prompt);
        assert_eq!(state.take_suspend("run-1"), Some(SuspendKind::Prompt));
        assert_eq!(state.take_suspend("run-1"), None);
        // The delegated-task kind round-trips too.
        state.request_suspend("run-1", SuspendKind::DelegatedTask);
        assert_eq!(
            state.take_suspend("run-1"),
            Some(SuspendKind::DelegatedTask)
        );
    }

    #[test]
    fn suspend_request_for_unknown_run_is_noop() {
        let state = AgentProcessState::default();
        state.request_suspend("ghost", SuspendKind::Prompt);
        assert_eq!(state.take_suspend("ghost"), None);
    }

    #[test]
    fn terminal_tool_armed_reflects_arm_state() {
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-1".to_string(),
                RunHandle::test_handle(Some("s"), Some("j")),
            );
        }
        // Unarmed by default; unknown runs read false.
        assert!(!state.terminal_tool_armed("run-1"));
        assert!(!state.terminal_tool_armed("missing"));
        // After arming (an output-artifact write), the owned loop sees it.
        assert!(state.arm_terminal_tool("run-1"));
        assert!(state.terminal_tool_armed("run-1"));
    }

    #[test]
    fn test_run_handle_lifecycle() {
        let mut handle = RunHandle::test_handle(Some("session-1"), None);

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
            let handle = RunHandle::test_handle(Some("session-1"), None);
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
            let mut handle = RunHandle::test_handle(Some("session-1"), Some("job-1"));
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
            let handle = RunHandle::test_handle(Some("session-abc"), None);
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
        let handle = RunHandle::test_handle(None, None);
        assert!(!handle.terminal_tool_called.load(Ordering::Acquire));
    }

    #[test]
    fn test_begin_turn_clears_terminal_tool_flag() {
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let mut handle = RunHandle::test_handle(Some("session-1"), Some("job-1"));
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
    fn test_transition_to_warm_clears_terminal_tool_flag() {
        // The work turn arms the terminal-tool interrupt by writing its output
        // artifact; warming the process must disarm it so the next turn (e.g.
        // the memory-review turn) does not inherit the arm and get interrupted.
        let mut handle = RunHandle::test_handle(Some("session-1"), Some("job-1"));
        handle.begin_turn("turn-1");
        handle.terminal_tool_called.store(true, Ordering::Release);
        handle.transition_to_warm();
        assert!(!handle.terminal_tool_called.load(Ordering::Acquire));
    }

    #[test]
    fn test_end_turn_clears_terminal_tool_flag() {
        let mut handle = RunHandle::test_handle(Some("session-1"), Some("job-1"));
        handle.begin_turn("turn-1");
        handle.terminal_tool_called.store(true, Ordering::Release);
        handle.end_turn();
        assert!(!handle.terminal_tool_called.load(Ordering::Acquire));
    }

    #[test]
    fn test_awaiting_host_not_gc_safe() {
        let mut handle = RunHandle::test_handle(None, None);
        handle.lifecycle = RunLifecycle::Live;
        handle.begin_turn("turn-1");
        handle.yield_for_host("turn-1");

        assert!(!handle.is_gc_safe());
        assert!(handle.is_active());
    }

    #[test]
    fn test_registry_remove_cleans_session_index() {
        let mut registry = RunRegistry::default();
        let handle = RunHandle::test_handle(Some("session-x"), None);

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
        let handle = RunHandle::test_handle(None, None);

        registry.register("run-1".to_string(), handle);
        assert!(registry.get("run-1").is_some());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let mut registry = RunRegistry::default();
        assert!(registry.remove("nonexistent").is_none());
    }

    fn state_with_run(run_id: &str) -> AgentProcessState {
        let state = AgentProcessState::default();
        let mut processes = state.processes.lock().unwrap();
        let handle = RunHandle::test_handle(Some("sess"), None);
        processes.register(run_id.to_string(), handle);
        drop(processes);
        state
    }

    #[test]
    fn test_note_read_accumulates_consumed() {
        let state = state_with_run("run-1");
        assert_eq!(state.consumed_uris("run-1"), Some(HashSet::new()));
        state.note_read("run-1", "cairn://p/CAIRN/1");
        state.note_read("run-1", "cairn://p/CAIRN/1"); // idempotent
        state.note_read("run-1", "cairn://p/CAIRN/2");
        let consumed = state.consumed_uris("run-1").unwrap();
        assert_eq!(consumed.len(), 2);
        assert!(consumed.contains("cairn://p/CAIRN/1"));
        assert_eq!(state.consumed_uris("ghost"), None);
    }

    #[test]
    fn test_claim_consumed_seed_once() {
        let state = state_with_run("run-1");
        assert!(state.claim_consumed_seed("run-1"));
        assert!(!state.claim_consumed_seed("run-1"));
        assert!(!state.claim_consumed_seed("ghost"));
        state.extend_consumed("run-1", ["cairn://a".to_string(), "cairn://b".to_string()]);
        assert_eq!(state.consumed_uris("run-1").unwrap().len(), 2);
    }

    #[test]
    fn test_recommend_pos_changed_tracks_baseline() {
        let state = state_with_run("run-1");
        let a = vec![1u8, 2, 3];
        let b = vec![4u8, 5, 6];
        // First call: no baseline → changed.
        assert!(state.recommend_pos_changed("run-1", &a));
        // Same position → not changed.
        assert!(!state.recommend_pos_changed("run-1", &a));
        // New position → changed, becomes the new baseline.
        assert!(state.recommend_pos_changed("run-1", &b));
        assert!(!state.recommend_pos_changed("run-1", &b));
        assert!(!state.recommend_pos_changed("ghost", &a));
    }

    #[test]
    fn test_get_set_model() {
        let state = state_with_run("run-1");
        // No model recorded at creation.
        assert_eq!(state.get_model("run-1"), None);
        assert_eq!(state.get_model("ghost"), None);
        // Setting on an unknown run is a no-op.
        assert!(!state.set_model("ghost", "opus"));
        // Setting on a known run records and is readable back.
        assert!(state.set_model("run-1", "opus"));
        assert_eq!(state.get_model("run-1"), Some("opus".to_string()));
        // Overwriting replaces the value.
        assert!(state.set_model("run-1", "sonnet"));
        assert_eq!(state.get_model("run-1"), Some("sonnet".to_string()));
    }

    #[test]
    fn test_get_backend() {
        let state = state_with_run("run-1");
        // Default (Claude) backend is None.
        assert_eq!(state.get_backend("run-1"), None);
        assert_eq!(state.get_backend("ghost"), None);
        // A codex-tagged process reports its backend.
        {
            let mut processes = state.processes.lock().unwrap();
            processes.get_mut("run-1").unwrap().backend = Some("codex".to_string());
        }
        assert_eq!(state.get_backend("run-1"), Some("codex".to_string()));
    }

    #[test]
    fn test_stop_and_remove_cleans_session_index() {
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            let child: Arc<Mutex<Option<Box<dyn ChildProcess>>>> = Arc::new(Mutex::new(Some(
                Box::new(MockChildProcess::with_stdout(123, vec![])),
            )));
            let stdin = Arc::new(Mutex::new(None));
            let handle = RunHandle::new(child, stdin, Some("sess-1".to_string()), None);
            processes.register("run-1".to_string(), handle);
        }

        // Process and session index are present.
        assert_eq!(
            state.find_process_by_session("sess-1"),
            Some("run-1".to_string())
        );

        // Stop and remove: child is killed, registry + session index cleaned.
        assert!(state.stop_and_remove("run-1"));
        assert_eq!(state.find_process_by_session("sess-1"), None);
        {
            let processes = state.processes.lock().unwrap();
            assert!(processes.get("run-1").is_none());
            assert!(processes.is_empty());
        }

        // Removing again is a no-op.
        assert!(!state.stop_and_remove("run-1"));
    }

    #[test]
    fn test_check_and_record_content_dedups_unchanged_within_turn() {
        let state = state_with_run("run-1");
        state.begin_turn("run-1", "turn-1");
        let fp = "read\u{1}{\"path\":\"a\"}";
        // Same content hash each call (resource unchanged this turn).
        let h = 0xABCD_u64;

        // First occurrence executes.
        assert_eq!(
            state.check_and_record_content("run-1", fp, h),
            DedupOutcome::First
        );
        // Identical second call with unchanged content is a duplicate.
        assert_eq!(
            state.check_and_record_content("run-1", fp, h),
            DedupOutcome::Duplicate {
                count: 2,
                distinct_dupes: 1
            }
        );
        // A third unchanged call bumps the count.
        assert_eq!(
            state.check_and_record_content("run-1", fp, h),
            DedupOutcome::Duplicate {
                count: 3,
                distinct_dupes: 1
            }
        );
    }

    #[test]
    fn test_check_and_record_content_refetches_when_content_changed() {
        let state = state_with_run("run-1");
        state.begin_turn("run-1", "turn-1");
        let fp = "read\u{1}{\"path\":\"a\"}";

        // First read of the resource.
        assert_eq!(
            state.check_and_record_content("run-1", fp, 0x1111),
            DedupOutcome::First
        );
        // Resource changed since the last read -> Changed, not Duplicate.
        assert_eq!(
            state.check_and_record_content("run-1", fp, 0x2222),
            DedupOutcome::Changed { count: 2 }
        );
        // Re-reading the now-current content is a genuine duplicate again.
        assert_eq!(
            state.check_and_record_content("run-1", fp, 0x2222),
            DedupOutcome::Duplicate {
                count: 3,
                distinct_dupes: 1
            }
        );
        // Changing again returns fresh content and clears the dup status.
        assert_eq!(
            state.check_and_record_content("run-1", fp, 0x3333),
            DedupOutcome::Changed { count: 4 }
        );
    }

    #[test]
    fn test_check_and_record_content_resets_on_turn_change() {
        let state = state_with_run("run-1");
        state.begin_turn("run-1", "turn-1");
        assert_eq!(
            state.check_and_record_content("run-1", "fp", 7),
            DedupOutcome::First
        );
        assert!(matches!(
            state.check_and_record_content("run-1", "fp", 7),
            DedupOutcome::Duplicate { .. }
        ));

        // A new turn wipes the seen-set: the same fingerprint is First again.
        state.begin_turn("run-1", "turn-2");
        assert_eq!(
            state.check_and_record_content("run-1", "fp", 7),
            DedupOutcome::First
        );
    }

    #[test]
    fn test_check_and_record_content_passthrough_when_no_turn_or_unknown_run() {
        let state = state_with_run("run-1");
        // Idle process (no active turn) -> never deduped.
        assert_eq!(
            state.check_and_record_content("run-1", "fp", 1),
            DedupOutcome::PassThrough
        );
        // Unknown run -> never deduped.
        assert_eq!(
            state.check_and_record_content("ghost", "fp", 1),
            DedupOutcome::PassThrough
        );
    }

    #[test]
    fn test_check_and_record_content_counts_distinct_dupes() {
        let state = state_with_run("run-1");
        state.begin_turn("run-1", "turn-1");
        // Two distinct fingerprints, each a first occurrence.
        assert_eq!(
            state.check_and_record_content("run-1", "fp-a", 10),
            DedupOutcome::First
        );
        assert_eq!(
            state.check_and_record_content("run-1", "fp-b", 20),
            DedupOutcome::First
        );
        // Repeating fp-a with unchanged content -> one distinct dupe.
        assert_eq!(
            state.check_and_record_content("run-1", "fp-a", 10),
            DedupOutcome::Duplicate {
                count: 2,
                distinct_dupes: 1
            }
        );
        // Repeating fp-b with unchanged content -> two distinct dupes.
        assert_eq!(
            state.check_and_record_content("run-1", "fp-b", 20),
            DedupOutcome::Duplicate {
                count: 2,
                distinct_dupes: 2
            }
        );
    }

    #[test]
    fn test_check_and_record_content_changed_repeat_drops_distinct_dupe() {
        let state = state_with_run("run-1");
        state.begin_turn("run-1", "turn-1");
        // fp-a duplicates (unchanged), fp-b will change on its repeat.
        assert_eq!(
            state.check_and_record_content("run-1", "fp-a", 1),
            DedupOutcome::First
        );
        assert_eq!(
            state.check_and_record_content("run-1", "fp-b", 2),
            DedupOutcome::First
        );
        assert_eq!(
            state.check_and_record_content("run-1", "fp-a", 1),
            DedupOutcome::Duplicate {
                count: 2,
                distinct_dupes: 1
            }
        );
        // fp-b's content changed -> Changed, and it is not a distinct dupe.
        assert_eq!(
            state.check_and_record_content("run-1", "fp-b", 99),
            DedupOutcome::Changed { count: 2 }
        );
        // fp-a is still the only distinct dupe.
        assert_eq!(
            state.check_and_record_content("run-1", "fp-a", 1),
            DedupOutcome::Duplicate {
                count: 3,
                distinct_dupes: 1
            }
        );
    }
}
