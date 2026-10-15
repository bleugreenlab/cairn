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

use cairn_platform::services::process::ChildProcess;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub use cairn_models::interface::session::{SessionControl, SessionFacts};

/// A live session's one writer, shared by everything that talks to it. Empty
/// once the session can no longer be written to.
pub type ControlHandle = Arc<Mutex<Option<Box<dyn SessionControl>>>>;

/// What [`GoalControl::with_control`] answers when the session has no writer.
pub const NO_SESSION_CONTROL: &str = "Session control not available";

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
use cairn_models::turn::SuspendKind;

/// A process-scoped claim on a native goal's paused state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalPause {
    OperatorHold,
    SafetyBrake,
    Owned(GoalPauseOwner),
    /// The authoritative jobs row says this job is not runnable right now.
    JobState(JobPauseState),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobPauseState {
    Blocked,
    Idle,
}

impl JobPauseState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Blocked => "blocked",
            Self::Idle => "idle",
        }
    }
}

impl GoalPause {
    pub fn may_supersede(&self, current: Option<&GoalPause>) -> bool {
        match self {
            Self::OperatorHold | Self::SafetyBrake => true,
            // A newly established wait supersedes an older wait so stale results
            // cannot release the goal while the new condition remains pending.
            Self::Owned(_) => matches!(current, None | Some(Self::JobState(_) | Self::Owned(_))),
            Self::JobState(_) => matches!(current, None | Some(Self::JobState(_))),
        }
    }
}

/// Durable, never-reused identity of the condition that owns a pause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalPauseOwner {
    DurableWait { wait_id: String },
    DependencyWait { parked_turn_id: String },
    UserInput { request_id: String },
}

impl GoalPauseOwner {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::DurableWait { .. } => "durable_wait",
            Self::DependencyWait { .. } => "dependency_wait",
            Self::UserInput { .. } => "user_input",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::DurableWait { wait_id } => wait_id,
            Self::DependencyWait { parked_turn_id } => parked_turn_id,
            Self::UserInput { request_id } => request_id,
        }
    }
}

/// Every pause either names its owner or preserves the existing claim.
#[derive(Clone, Debug)]
pub enum PauseIntent {
    Stake(GoalPause),
    Reassert,
}

/// Ownership operations are available only inside the run's serialized goal control.
#[derive(Default)]
pub struct GoalPauseClaim {
    cause: Option<GoalPause>,
}

impl GoalPauseClaim {
    pub fn inspect(&self) -> Option<GoalPause> {
        self.cause.clone()
    }

    pub fn stake(&mut self, intent: PauseIntent) {
        match intent {
            PauseIntent::Stake(cause) if cause.may_supersede(self.cause.as_ref()) => {
                self.cause = Some(cause);
            }
            // A weaker classification never erases a stickier one.
            PauseIntent::Stake(_) => {}
            // Reassertion preserves; an unknown pause is not an operator's hold.
            PauseIntent::Reassert => {}
        }
    }

    /// Admission clears only unattributed and job-state pauses, never typed holds or waits.
    pub fn release_unowned(
        &mut self,
        activate: impl FnOnce() -> Result<(), String>,
    ) -> Result<bool, String> {
        if !matches!(self.cause, None | Some(GoalPause::JobState(_))) {
            return Ok(false);
        }
        activate()?;
        self.cause = None;
        Ok(true)
    }

    /// Consume only this durable condition's claim, after the provider acknowledges activation.
    pub fn release(
        &mut self,
        owner: &GoalPauseOwner,
        activate: impl FnOnce() -> Result<(), String>,
    ) -> Result<bool, String> {
        if !matches!(&self.cause, Some(GoalPause::Owned(current)) if current == owner) {
            return Ok(false);
        }
        activate()?;
        self.cause = None;
        Ok(true)
    }

    pub fn release_operator_hold(
        &mut self,
        activate: impl FnOnce() -> Result<(), String>,
    ) -> Result<bool, String> {
        if self.cause != Some(GoalPause::OperatorHold) {
            return Ok(false);
        }
        activate()?;
        self.cause = None;
        Ok(true)
    }

    pub fn rearm_safety_brake(&mut self) {
        if self.cause == Some(GoalPause::SafetyBrake) {
            self.cause = Some(GoalPause::OperatorHold);
        }
    }

    /// Restore a failed park's snapshot only if no later stake has superseded it.
    pub fn rollback(&mut self, expected: &Option<GoalPause>, previous: Option<GoalPause>) {
        if &self.cause == expected {
            self.cause = previous;
        }
    }
}

/// A process attachment's writer and ownership state, captured together under the process map.
pub struct GoalControl {
    control: ControlHandle,
    claim: Arc<Mutex<GoalPauseClaim>>,
}

impl GoalControl {
    /// Lock the session's control before ownership, keeping both held across
    /// provider I/O and claim mutation.
    pub fn with_control<T>(
        &self,
        f: impl FnOnce(&mut dyn SessionControl, &mut GoalPauseClaim) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut control = self.control.lock().map_err(|e| e.to_string())?;
        let writer = control
            .as_mut()
            .ok_or_else(|| NO_SESSION_CONTROL.to_string())?;
        let mut claim = self.claim.lock().map_err(|e| e.to_string())?;
        f(writer.as_mut(), &mut claim)
    }
}

/// Which rotation of its account's credential a live process holds.
///
/// A process launched on a managed subscription runs on an access token that
/// Cairn rotates underneath it; each rotation is pushed in over the session's
/// control. `pushed` is the newest generation sent, `acknowledged` the newest
/// the process confirmed, and a process is behind while either trails the
/// store's current generation.
#[derive(Debug)]
pub struct CredentialFeed {
    pushed: AtomicU64,
    acknowledged: AtomicU64,
}

impl CredentialFeed {
    /// A process launched on `generation`.
    pub fn launched_on(generation: u64) -> Self {
        Self {
            pushed: AtomicU64::new(generation),
            acknowledged: AtomicU64::new(generation),
        }
    }

    pub fn pushed(&self) -> u64 {
        self.pushed.load(Ordering::Acquire)
    }

    pub fn acknowledged(&self) -> u64 {
        self.acknowledged.load(Ordering::Acquire)
    }

    pub fn record_push(&self, generation: u64) {
        self.pushed.fetch_max(generation, Ordering::AcqRel);
    }

    pub fn record_ack(&self, generation: u64) {
        self.acknowledged.fetch_max(generation, Ordering::AcqRel);
        self.record_push(generation);
    }
}

/// One live process fed an account's credential, and how to reach it.
pub struct FedProcess {
    pub run_id: String,
    pub account_id: String,
    pub feed: Arc<CredentialFeed>,
    control: ControlHandle,
}

impl FedProcess {
    /// Hand the process `generation` of its credential, unless it has already
    /// been pushed that generation or a newer one. Answers whether anything was
    /// sent.
    ///
    /// The check, the send and the record all happen under the session's
    /// control lock, so concurrent reconcilers (a sweep, a publish, a 401)
    /// push each generation to a process once, and a send that failed claims
    /// nothing.
    pub fn push(
        &self,
        env: &cairn_models::interface::session::SecretEnv,
        generation: u64,
    ) -> Result<bool, String> {
        let mut control = self.control.lock().map_err(|e| e.to_string())?;
        if self.feed.pushed() >= generation {
            return Ok(false);
        }
        let control = control
            .as_mut()
            .ok_or_else(|| NO_SESSION_CONTROL.to_string())?;
        control.update_credential(env, generation)?;
        self.feed.record_push(generation);
        Ok(true)
    }
}

/// A live process attachment with the control that writes to its session.
pub struct RunHandle {
    /// The child process handle
    pub child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
    /// The session's one writer: messages, interrupts, model and permission changes.
    control: ControlHandle,
    /// What the launch knew about the session: provider, backend session id,
    /// account, host. `None` for a handle no launch registered.
    pub facts: Option<SessionFacts>,
    /// Process lifecycle state
    lifecycle: RunLifecycle,
    /// Occupancy state (what the process is doing)
    occupancy: RunOccupancy,
    /// Last activity timestamp (for GC relevance scoring)
    pub last_activity: Instant,
    /// The backend session ID for this process
    session_id: Option<String>,
    /// The job ID associated with this process (if any)
    pub job_id: Option<String>,
    /// Cursor for channel message polling
    pub message_cursor: Arc<Mutex<i64>>,
    /// Backend name ("codex", etc.). None = Claude (default).
    pub backend: Option<String>,
    /// Serialized with the control (control first); dies with this process attachment.
    goal_pause: Arc<Mutex<GoalPauseClaim>>,
    /// The effective model this process was started with. Used by warm reuse to
    /// detect when the job's requested model has diverged from the live process.
    pub model: Option<String>,
    /// Concrete provider account whose credentials were frozen into this child
    /// at launch. Warm reuse compares this with the durable session pin.
    pub account_id: Option<String>,
    /// Set for a process that runs on a rotating subscription credential and
    /// can take a renewed one live; `None` for everything else.
    pub credential_feed: Option<Arc<CredentialFeed>>,
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
    turn_seen_calls: Arc<Mutex<TurnSeenCalls>>,
    /// Whether the backend driving this run owns its turn/tool loop in-process
    /// (OpenRouter). Owned-loop runs suspend on a foreground question or inline
    /// task instead of inline-waiting; warm-process backends (Claude/Codex)
    /// leave this false and keep their existing blocking behavior.
    pub owns_turn_loop: bool,
    /// Structured suspend request for an owned-loop run, set by a blocking
    /// handler and consumed by the owned loop after the tool dispatch returns.
    pending_suspend: Arc<Mutex<Option<SuspendKind>>>,
    /// Content hashes of affordance blocks already shown in full this session
    /// (CAIRN-2592 session-scoped affordance dedup). The full block for a
    /// resource kind renders once per run; a later read whose affordance block
    /// hashes to an entry here collapses to a one-line `cairn://help?kind=<slug>`
    /// pointer instead of re-paying the whole block.
    ///
    /// Keyed by block-content hash, not `ResourceKind`, so schema-distinct
    /// artifact blocks (which derive from the artifact's schema, not the static
    /// contract) never collapse into one another.
    ///
    /// Deliberately NOT event-seeded (unlike `consumed_uris`): an empty set
    /// after a cold host restart correctly re-shows blocks into the freshly
    /// re-dispatched context. Compaction keeps the live process (and this set)
    /// up, which is exactly why the pointer must stay reachable one read away.
    seen_affordances: Arc<Mutex<HashSet<u64>>>,
    /// Generation-fenced capacity claim held for this physical process. It is
    /// retained while the process is starting, active, awaiting its host, and
    /// warm; terminal lifecycle paths consume it exactly once.
    admission_claim: Option<ProcessAdmissionClaim>,
}

/// Durable admission identity carried by a dedicated ordinary process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessAdmissionClaim {
    pub request_id: String,
    pub workspace_key: String,
    pub launch_key: String,
    pub owner: String,
    pub generation: i64,
    /// Team id when Postgres, rather than the local admission ledger, owns the lease.
    pub team_workspace_id: Option<String>,
}

/// Backwards-compatible alias.
pub type ActiveProcess = RunHandle;

impl RunHandle {
    /// Capture the lock-safe stake, release, and inspect API with its matching writer.
    pub fn goal_control(&self) -> GoalControl {
        GoalControl {
            control: self.control.clone(),
            claim: self.goal_pause.clone(),
        }
    }

    /// Create a new run handle in Starting/Idle state.
    pub fn new(
        child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
        control: ControlHandle,
        session_id: Option<String>,
        job_id: Option<String>,
    ) -> Self {
        Self {
            child,
            control,
            facts: None,
            lifecycle: RunLifecycle::Starting,
            occupancy: RunOccupancy::Idle,
            last_activity: Instant::now(),
            session_id,
            job_id,
            message_cursor: Arc::new(Mutex::new(chrono::Utc::now().timestamp())),
            backend: None,
            goal_pause: Arc::new(Mutex::new(GoalPauseClaim::default())),
            model: None,
            account_id: None,
            credential_feed: None,
            terminal_tool_called: Arc::new(AtomicBool::new(false)),
            consumed_uris: Arc::new(Mutex::new(HashSet::new())),
            consumed_seeded: Arc::new(AtomicBool::new(false)),
            last_recommend_pos: Arc::new(Mutex::new(None)),
            turn_seen_calls: Arc::new(Mutex::new(TurnSeenCalls::default())),
            owns_turn_loop: false,
            pending_suspend: Arc::new(Mutex::new(None)),
            seen_affordances: Arc::new(Mutex::new(HashSet::new())),
            admission_claim: None,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_handle(session_id: Option<&str>, job_id: Option<&str>) -> Self {
        let child = Arc::new(Mutex::new(None));
        let control = Arc::new(Mutex::new(None));
        Self::new(
            child,
            control,
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
    fn is_active(&self) -> bool {
        !matches!(self.occupancy, RunOccupancy::Idle)
    }

    /// Begin a new turn.
    pub fn begin_turn(&mut self, turn_id: &str) {
        self.occupancy = RunOccupancy::ServingTurn(turn_id.to_string());
        self.last_activity = Instant::now();
        self.terminal_tool_called.store(false, Ordering::Release);
    }

    /// Yield the current turn for host interaction (ask_user, permission).
    fn yield_for_host(&mut self, turn_id: &str) {
        self.occupancy = RunOccupancy::AwaitingHost {
            turn_id: turn_id.to_string(),
        };
        self.last_activity = Instant::now();
    }

    /// End the current turn, transition to idle.
    fn end_turn(&mut self) {
        self.occupancy = RunOccupancy::Idle;
        self.last_activity = Instant::now();
        // The terminal-tool boundary interrupt is armed by an output-artifact
        // write to end *this* turn. Clear it when the turn ends so it cannot
        // bleed into the next turn on the same warm process (e.g. the
        // post-completion memory-review turn).
        self.terminal_tool_called.store(false, Ordering::Release);
    }

    /// Get the current turn ID (if serving or awaiting).
    fn current_turn_id(&self) -> Option<&str> {
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
    fn transition_to_active(&mut self) {
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
    fn seconds_since_activity(&self) -> u64 {
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

    fn get_mut(&mut self, run_id: &str) -> Option<&mut RunHandle> {
        self.by_id.get_mut(run_id)
    }

    fn get_by_session(&self, session_id: &str) -> Option<&str> {
        self.session_index.get(session_id).map(|s| s.as_str())
    }

    /// Register a run handle, returning any *other* live handle it displaces
    /// from the session index.
    ///
    /// A session has at most one live process. This insert used to overwrite the
    /// session index unconditionally while `by_id` kept both handles, so two
    /// processes spawned against one session were silently accepted and the
    /// first was orphaned from session lookup while still running (CAIRN-3283).
    /// Per-job launch serialization is what prevents that; this is the backstop
    /// at the one moment both processes provably exist. The displaced handle is
    /// returned rather than stopped here because stopping waits on the child —
    /// [`AgentProcessState::register_process`] does it outside the registry lock.
    pub fn register(&mut self, run_id: String, handle: RunHandle) -> Option<RunHandle> {
        let displaced = handle.session_id.as_ref().and_then(|sid| {
            let prior_run_id = self.session_index.insert(sid.clone(), run_id.clone())?;
            if prior_run_id == run_id {
                return None;
            }
            log::error!(
                "Session {} registered to run {} while run {} still held a live handle for it: \
                 two agent processes on one session. Stopping the displaced process.",
                sid,
                run_id,
                prior_run_id
            );
            self.by_id.remove(&prior_run_id)
        });
        self.by_id.insert(run_id, handle);
        displaced
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

    fn values(&self) -> impl Iterator<Item = &RunHandle> {
        self.by_id.values()
    }
}

/// Type alias for backwards compatibility with code that uses `ProcessMap`.
pub type ProcessMap = RunRegistry;

// ============================================================================
// AgentProcessState
// ============================================================================

/// A warm (live + idle) process surfaced for garbage collection: its run id,
/// OS pid (present while the child handle is still attached), idle duration, and
/// owning job. The pid lets the memory-aware GC size the warm pool against real
/// RSS instead of an arbitrary count (CAIRN-2543).
#[derive(Debug, Clone)]
pub struct WarmProcess {
    pub run_id: String,
    pub pid: Option<u32>,
    pub seconds_since_activity: u64,
    pub job_id: Option<String>,
}

/// State for tracking active agent processes
pub struct AgentProcessState {
    /// Active processes keyed by run_id
    pub processes: Mutex<RunRegistry>,
    /// Cached path to default CLI binary (resolved once on first use)
    pub cli_binary_path: Mutex<Option<String>>,
    /// Run ids of workflow processes the user deliberately stopped (CAIRN-2516).
    /// A workflow process has no stdin, so a Stop hard-kills it; without this
    /// marker the supervisor would read the signal-killed exit as a crash and
    /// finalize the run `crashed` — which startup re-dispatch then resurrects.
    /// The stop path sets the marker before killing; the supervisor reads it on
    /// finalize to map the outcome to a terminal, non-crashed (cancelled) state
    /// that is not auto-re-dispatched. It lives on the state (not the per-run
    /// handle) so it survives the handle's removal during the kill. In-memory is
    /// sufficient: a host restart before the process dies is itself a crash,
    /// which is correctly re-dispatched.
    workflow_stop_requested: Mutex<std::collections::HashSet<String>>,
    /// Live workflow executor bindings keyed by run id. Workflow rows are durable,
    /// but the incarnation/epoch fence is intentionally process-local authority.
    workflow_leases:
        Mutex<std::collections::HashMap<String, cairn_common::executor_protocol::ResidencyFence>>,
    /// Claim handoff spanning promotion through backend registration. The entry
    /// moves into RunHandle at registration, so warm reuse cannot acquire or
    /// attach a second claim.
    pending_admission_claims: Mutex<HashMap<String, ProcessAdmissionClaim>>,
}

impl AgentProcessState {
    pub fn admission_claim_is_current(
        &self,
        run_id: &str,
        request_id: &str,
        generation: i64,
    ) -> bool {
        self.admission_claim(run_id)
            .is_some_and(|claim| claim.request_id == request_id && claim.generation == generation)
    }

    pub fn admission_claim(&self, run_id: &str) -> Option<ProcessAdmissionClaim> {
        if let Some(claim) = self
            .pending_admission_claims
            .lock()
            .ok()?
            .get(run_id)
            .cloned()
        {
            return Some(claim);
        }
        self.processes
            .lock()
            .ok()?
            .get(run_id)?
            .admission_claim
            .clone()
    }

    pub fn prepare_admission_claim(&self, run_id: String, claim: ProcessAdmissionClaim) {
        if let Ok(mut claims) = self.pending_admission_claims.lock() {
            claims.insert(run_id, claim);
        }
    }

    pub fn cancel_pending_admission_claim(&self, run_id: &str) -> Option<ProcessAdmissionClaim> {
        self.pending_admission_claims.lock().ok()?.remove(run_id)
    }

    /// Consume the claim from either its pre-registration handoff or live handle.
    /// Taking is the local exactly-once guard; durable owner+generation fencing is
    /// the authority against callbacks from an older process incarnation.
    pub fn take_admission_claim(&self, run_id: &str) -> Option<ProcessAdmissionClaim> {
        if let Some(claim) = self.cancel_pending_admission_claim(run_id) {
            return Some(claim);
        }
        self.processes
            .lock()
            .ok()?
            .get_mut(run_id)?
            .admission_claim
            .take()
    }
    pub fn bind_workflow_lease(
        &self,
        run_id: String,
        fence: cairn_common::executor_protocol::ResidencyFence,
    ) {
        if let Ok(mut leases) = self.workflow_leases.lock() {
            leases.insert(run_id, fence);
        }
    }

    pub fn workflow_lease(
        &self,
        run_id: &str,
    ) -> Option<cairn_common::executor_protocol::ResidencyFence> {
        self.workflow_leases.lock().ok()?.get(run_id).cloned()
    }

    pub fn clear_workflow_lease(&self, run_id: &str) {
        if let Ok(mut leases) = self.workflow_leases.lock() {
            leases.remove(run_id);
        }
    }
    /// Get the occupancy of a process by run_id.
    pub fn get_occupancy(&self, run_id: &str) -> Option<RunOccupancy> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).map(|p| p.occupancy.clone())
    }

    /// Whether this host holds a process handle for `run_id` at all — serving,
    /// awaiting the host, or parked warm — or `None` when the registry could not
    /// be inspected at all.
    ///
    /// The run-level sibling of [`Self::is_active`], which asks the narrower
    /// question of whether that handle is currently mid-turn. The two answers
    /// diverge for every warm process, so a caller reasoning about whether a run
    /// is *alive* (rather than *busy*) must ask this one: the registry is
    /// in-memory and every backend registers before its run can produce
    /// anything, which makes handle presence the authoritative answer to "is
    /// there still a process behind this run row?" (`crate::runs::reap`).
    ///
    /// The three-valued answer is the load-bearing difference from `is_active`,
    /// which collapses a poisoned lock into `false`. That collapse is survivable
    /// there — the answer only steers a scheduling decision, and a durable
    /// second opinion sits beside it. Here the answer authorizes permanently
    /// rewriting a run and its turns, and "the registry panicked mid-operation"
    /// is not evidence that a process is absent; read as one it would invent a
    /// crash for every live and warm process on this host at once. So a caller
    /// on a destructive path must act only on `Some(false)`.
    pub fn has_process(&self, run_id: &str) -> Option<bool> {
        let processes = self.processes.lock().ok()?;
        Some(processes.contains_key(run_id))
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

    /// Record that a workflow run was deliberately stopped by the user, so the
    /// supervisor's finalize maps its killed process to a terminal, non-crashed
    /// outcome instead of a crash. Set BEFORE killing the process (CAIRN-2516).
    pub fn mark_workflow_stop_requested(&self, run_id: &str) {
        if let Ok(mut set) = self.workflow_stop_requested.lock() {
            set.insert(run_id.to_string());
        }
    }

    /// Check-and-clear the deliberate-stop marker for a run. One-shot: the
    /// supervisor calls this exactly once on finalize, so a later crash-driven
    /// finalize of a re-dispatched run never sees a stale marker.
    pub fn take_workflow_stop_requested(&self, run_id: &str) -> bool {
        self.workflow_stop_requested
            .lock()
            .map(|mut set| set.remove(run_id))
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
    ///
    /// The pid is read from the still-attached child handle so the memory-aware
    /// GC can measure each warm process's RSS; a process whose child handle was
    /// already taken (e.g. mid-teardown) reports `None` and the GC imputes its
    /// size from measured siblings.
    pub fn warm_processes(&self) -> Vec<WarmProcess> {
        let processes = match self.processes.lock() {
            Ok(p) => p,
            Err(_) => return vec![],
        };

        let mut warm: Vec<WarmProcess> = processes
            .iter()
            .filter(|(_, p)| p.is_warm())
            .map(|(run_id, p)| WarmProcess {
                run_id: run_id.clone(),
                pid: p
                    .child
                    .lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().map(|child| child.id())),
                seconds_since_activity: p.seconds_since_activity(),
                job_id: p.job_id.clone(),
            })
            .collect();

        // Sort by seconds since activity (oldest first)
        warm.sort_by_key(|w| w.seconds_since_activity);

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

    /// Job ids that have a live in-memory process attachment right now,
    /// regardless of lifecycle or occupancy.
    ///
    /// EVERY registry entry counts — serving a turn, awaiting host, warm and
    /// idle, or still starting — because each holds backend session / job / run
    /// handles keyed to the job's *current* ids. That makes this the
    /// authoritative "which jobs are actually live" set for the move-to-team
    /// quiesce gate: a durable `job.status = 'running'` row can be a parked,
    /// resumable host wait with no live process (see
    /// `docs/turn-process-recovery.md`), so job rows cannot answer this — only
    /// the process registry can.
    pub fn live_job_ids(&self) -> HashSet<String> {
        self.processes
            .lock()
            .map(|processes| {
                processes
                    .values()
                    .filter_map(|handle| handle.job_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every run this host currently holds a process handle for.
    ///
    /// The registry is in-memory and starts empty at boot, so this is the
    /// authoritative answer to "which runs did THIS host process spawn" — the
    /// sibling question to [`Self::live_job_ids`], one rung down from jobs to runs.
    ///
    /// A snapshot rather than a live view: the caller releases the registry lock
    /// before acting on the ids, so a concurrent lifecycle transition can retire
    /// one out from under it. Callers must treat an id that no longer resolves as
    /// already stopped, which is what the stop paths do anyway.
    pub fn run_ids(&self) -> Vec<String> {
        self.processes
            .lock()
            .map(|processes| processes.iter().map(|(id, _)| id.clone()).collect())
            .unwrap_or_default()
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

    /// Record which affordance-block content hashes have already been shown this
    /// session and return the subset of `hashes` that were *already* present
    /// (those collapse to a pointer). Newly-seen hashes are inserted, so a later
    /// batch finds them already-present. Doing both in one locked pass gives the
    /// read batch its keep-vs-collapse decision atomically.
    ///
    /// Session-scoped, not turn-scoped: a block shown in an earlier turn stays
    /// collapsed for the rest of the run. Returns an empty set when the run is
    /// unknown (external/untracked runs keep full blocks every time).
    pub fn mark_affordances_seen(&self, run_id: &str, hashes: &[u64]) -> HashSet<u64> {
        let Ok(processes) = self.processes.lock() else {
            return HashSet::new();
        };
        let Some(process) = processes.get(run_id) else {
            return HashSet::new();
        };
        let Ok(mut seen) = process.seen_affordances.lock() else {
            return HashSet::new();
        };
        let mut already = HashSet::new();
        for &hash in hashes {
            // `insert` returns false when the value was already present.
            if !seen.insert(hash) {
                already.insert(hash);
            }
        }
        already
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

    /// Increment and return this fingerprint's per-turn occurrence count,
    /// independent of returned content. Returns 0 when there is no tracked
    /// process or active turn (no nudge basis). Used by the terminal-poll
    /// nudge, where the read result legitimately changes every call (the
    /// status banner's elapsed/idle fields advance) so content-equality dedup
    /// can never see a repeat.
    ///
    /// It shares [`TurnSeenCalls`] with [`Self::check_and_record_content`] but
    /// only touches `count`, never `content_hash` / `is_dup`. Callers key it on
    /// a distinct fingerprint namespace (`terminal_poll\u{1}...`), so it never
    /// collides with the content-aware dedup's `read_batch_target\u{1}...`
    /// records and never perturbs the broad-thrash (`is_dup`) signal.
    pub fn record_repeat(&self, run_id: &str, fingerprint: &str) -> u32 {
        let Ok(processes) = self.processes.lock() else {
            return 0;
        };
        let Some(process) = processes.get(run_id) else {
            return 0;
        };
        let Some(turn) = process.current_turn_id().map(str::to_string) else {
            return 0; // Idle/Busy -> no turn, no count
        };
        let Ok(mut seen) = process.turn_seen_calls.lock() else {
            return 0;
        };
        if seen.turn_id.as_deref() != Some(turn.as_str()) {
            seen.turn_id = Some(turn);
            seen.calls.clear();
        }
        let record = seen.calls.entry(fingerprint.to_string()).or_default();
        record.count += 1;
        record.count
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

    /// Every live process fed a rotating credential, optionally only those on
    /// `account_id`.
    pub fn fed_processes(&self, account_id: Option<&str>) -> Vec<FedProcess> {
        let Ok(processes) = self.processes.lock() else {
            return Vec::new();
        };
        processes
            .iter()
            .filter_map(|(run_id, process)| {
                let feed = process.credential_feed.clone()?;
                let account = process.facts.as_ref()?.account_id.clone()?;
                if account_id.is_some_and(|wanted| wanted != account) {
                    return None;
                }
                Some(FedProcess {
                    run_id: run_id.clone(),
                    account_id: account,
                    feed,
                    control: process.control.clone(),
                })
            })
            .collect()
    }

    /// The one process `run_id`, if it is fed a rotating credential.
    pub fn fed_process(&self, run_id: &str) -> Option<FedProcess> {
        let processes = self.processes.lock().ok()?;
        let process = processes.get(run_id)?;
        Some(FedProcess {
            run_id: run_id.to_string(),
            account_id: process.facts.as_ref()?.account_id.clone()?,
            feed: process.credential_feed.clone()?,
            control: process.control.clone(),
        })
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

    /// The provider account a live process was launched with.
    pub fn get_account_id(&self, run_id: &str) -> Option<String> {
        let processes = self.processes.lock().ok()?;
        processes.get(run_id).and_then(|p| p.account_id.clone())
    }

    /// Record the provider's session id once the provider confirms it.
    pub fn set_backend_session_id(&self, run_id: &str, backend_session_id: &str) {
        if let Ok(mut processes) = self.processes.lock() {
            if let Some(facts) = processes
                .get_mut(run_id)
                .and_then(|process| process.facts.as_mut())
            {
                facts.backend_session_id = Some(backend_session_id.to_string());
            }
        }
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

    /// Register a run handle for `run_id`, then gracefully stop whatever process
    /// it displaced from the session index. A displacement is an invariant
    /// violation (two live processes on one session) that
    /// [`RunRegistry::register`] logs; stopping happens here, outside the
    /// registry lock, because it waits on the child.
    pub fn register_process(&self, run_id: String, mut handle: RunHandle) -> Result<(), String> {
        if let Ok(mut claims) = self.pending_admission_claims.lock() {
            handle.admission_claim = claims.remove(&run_id);
        }
        let displaced = {
            let mut processes = self.processes.lock().map_err(|e| e.to_string())?;
            processes.register(run_id, handle)
        };
        if let Some(displaced) = displaced {
            if let Ok(mut child_guard) = displaced.child.lock() {
                if let Some(child) = child_guard.as_mut() {
                    graceful_stop(child.as_mut());
                }
            }
        }
        Ok(())
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
            workflow_stop_requested: Mutex::new(std::collections::HashSet::new()),
            workflow_leases: Mutex::new(std::collections::HashMap::new()),
            pending_admission_claims: Mutex::new(HashMap::new()),
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
    use cairn_platform::services::process::MockChildProcess;

    fn admission_claim(generation: i64) -> ProcessAdmissionClaim {
        ProcessAdmissionClaim {
            request_id: format!("request-{generation}"),
            workspace_key: "private-runtime".into(),
            launch_key: "ordinary:run-1".into(),
            owner: "runner-1".into(),
            generation,
            team_workspace_id: None,
        }
    }

    #[test]
    fn pending_start_claim_is_consumed_once() {
        let state = AgentProcessState::default();
        let claim = admission_claim(7);
        state.prepare_admission_claim("run-1".into(), claim.clone());

        assert_eq!(state.take_admission_claim("run-1"), Some(claim));
        assert_eq!(state.take_admission_claim("run-1"), None);
    }

    #[test]
    fn registered_claim_survives_active_awaiting_host_and_warm_until_teardown() {
        let state = AgentProcessState::default();
        let claim = admission_claim(9);
        state.prepare_admission_claim("run-1".into(), claim.clone());
        state
            .register_process(
                "run-1".into(),
                RunHandle::test_handle(Some("session-1"), Some("job-1")),
            )
            .unwrap();

        assert_eq!(state.cancel_pending_admission_claim("run-1"), None);
        state.transition_to_active("run-1");
        state.begin_turn("run-1", "turn-1");
        state.yield_for_host("run-1", "turn-1");
        state.transition_to_warm("run-1");
        assert_eq!(state.take_admission_claim("run-1"), Some(claim));
        assert_eq!(state.take_admission_claim("run-1"), None);
    }

    #[test]
    fn stale_generation_cannot_claim_replacement_process() {
        let state = AgentProcessState::default();
        state.prepare_admission_claim("run-1".into(), admission_claim(10));

        assert!(state.admission_claim_is_current("run-1", "request-10", 10));
        assert!(!state.admission_claim_is_current("run-1", "request-10", 9));
        assert!(!state.admission_claim_is_current("other-run", "request-10", 10));
    }

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
    fn live_job_ids_includes_warm_active_and_starting_handles() {
        // The move-to-team quiesce gate treats EVERY registry entry as live: a
        // warm (live + idle) process still holds a session handle keyed to the
        // job's pre-move ids, so it blocks the move exactly as an active one
        // does. Handles without a job id (external/untracked runs) contribute
        // nothing.
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();

            // Warm: live + idle.
            let mut warm = RunHandle::test_handle(Some("s-warm"), Some("job-warm"));
            warm.transition_to_warm();
            processes.register("run-warm".to_string(), warm);

            // Active: serving a turn.
            let mut active = RunHandle::test_handle(Some("s-active"), Some("job-active"));
            active.begin_turn("turn-1");
            processes.register("run-active".to_string(), active);

            // Still starting (default lifecycle) but already keyed to a job.
            processes.register(
                "run-starting".to_string(),
                RunHandle::test_handle(Some("s-start"), Some("job-starting")),
            );

            // No job id: contributes nothing.
            processes.register(
                "run-jobless".to_string(),
                RunHandle::test_handle(Some("s-jobless"), None),
            );
        }

        let live = state.live_job_ids();
        assert_eq!(live.len(), 3, "got {live:?}");
        assert!(live.contains("job-warm"));
        assert!(live.contains("job-active"));
        assert!(live.contains("job-starting"));
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
    fn record_repeat_counts_per_turn_and_resets_across_turns() {
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-1".to_string(),
                RunHandle::test_handle(Some("s"), Some("j")),
            );
        }
        let fp = "terminal_poll\u{1}cairn:~/terminal/dev";

        // No active turn yet -> no count basis; unknown run -> 0.
        assert_eq!(state.record_repeat("run-1", fp), 0);
        assert_eq!(state.record_repeat("ghost", fp), 0);

        // Within one turn the same fingerprint counts 1, 2, 3 regardless of any
        // content (there is none to compare).
        state.set_current_turn_id("run-1", Some("turn-1"));
        assert_eq!(state.record_repeat("run-1", fp), 1);
        assert_eq!(state.record_repeat("run-1", fp), 2);
        assert_eq!(state.record_repeat("run-1", fp), 3);
        // A distinct fingerprint counts independently in the same turn.
        assert_eq!(
            state.record_repeat("run-1", "terminal_poll\u{1}cairn:~/terminal/other"),
            1
        );

        // A new turn resets the per-turn seen-set.
        state.set_current_turn_id("run-1", Some("turn-2"));
        assert_eq!(state.record_repeat("run-1", fp), 1);
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
        state.note_read("run-1", "cairn://p/cairn/1");
        state.note_read("run-1", "cairn://p/cairn/1"); // idempotent
        state.note_read("run-1", "cairn://p/cairn/2");
        let consumed = state.consumed_uris("run-1").unwrap();
        assert_eq!(consumed.len(), 2);
        assert!(consumed.contains("cairn://p/cairn/1"));
        assert_eq!(state.consumed_uris("ghost"), None);
    }

    #[test]
    fn test_mark_affordances_seen_collapses_repeats_per_run() {
        let state = state_with_run("run-1");
        // Register a second run in the same state to prove per-run isolation.
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-2".to_string(),
                RunHandle::test_handle(Some("sess2"), None),
            );
        }

        // First occurrence of each hash: nothing already seen; all recorded.
        assert_eq!(
            state.mark_affordances_seen("run-1", &[10, 20]),
            HashSet::new()
        );
        // Repeat pass: the previously-recorded hashes collapse; a never-seen hash
        // stays new. Distinct hashes are tracked independently, which guards the
        // artifact case (schema-distinct blocks hash differently, never collapse).
        assert_eq!(
            state.mark_affordances_seen("run-1", &[10, 20, 30]),
            HashSet::from([10, 20])
        );
        assert_eq!(
            state.mark_affordances_seen("run-1", &[30]),
            HashSet::from([30])
        );
        // Per-run isolation: run-2 starts with an empty seen-set.
        assert_eq!(state.mark_affordances_seen("run-2", &[10]), HashSet::new());
        // Unknown run: nothing seen, nothing recorded.
        assert_eq!(state.mark_affordances_seen("ghost", &[10]), HashSet::new());
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

    /// CAIRN-3283: two live processes on one session is an invariant violation,
    /// not a state the registry may silently hold. The overwrite it used to do
    /// left the first process running while removing it from session lookup, so
    /// nothing could ever find it again to stop it.
    #[test]
    fn a_second_process_on_one_session_displaces_the_first() {
        let mut registry = RunRegistry::default();
        assert!(
            registry
                .register(
                    "run-1".to_string(),
                    RunHandle::test_handle(Some("sess-1"), Some("job-1"))
                )
                .is_none(),
            "the first registration for a session displaces nothing"
        );

        let displaced = registry.register(
            "run-2".to_string(),
            RunHandle::test_handle(Some("sess-1"), Some("job-1")),
        );

        assert!(
            displaced.is_some(),
            "the prior handle must be handed back for stopping, not left running and unreachable"
        );
        assert_eq!(
            registry.len(),
            1,
            "by_id must never hold two live handles for one session"
        );
        assert!(registry.get("run-1").is_none());
        assert_eq!(registry.get_by_session("sess-1"), Some("run-2"));
    }

    /// Re-registering the same run for its own session (a handle rebuilt in
    /// place) is not a displacement and must not evict anything.
    #[test]
    fn re_registering_the_same_run_displaces_nothing() {
        let mut registry = RunRegistry::default();
        registry.register(
            "run-1".to_string(),
            RunHandle::test_handle(Some("sess-1"), None),
        );

        let displaced = registry.register(
            "run-1".to_string(),
            RunHandle::test_handle(Some("sess-1"), None),
        );

        assert!(displaced.is_none());
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.get_by_session("sess-1"), Some("run-1"));
    }

    /// The state-level wrapper every spawn path goes through: the displaced
    /// handle leaves the registry rather than lingering in `by_id`.
    #[test]
    fn register_process_evicts_the_displaced_handle() {
        let state = AgentProcessState::default();
        state
            .register_process(
                "run-1".to_string(),
                RunHandle::test_handle(Some("sess-1"), None),
            )
            .unwrap();
        state
            .register_process(
                "run-2".to_string(),
                RunHandle::test_handle(Some("sess-1"), None),
            )
            .unwrap();

        assert_eq!(
            state.find_process_by_session("sess-1"),
            Some("run-2".to_string())
        );
        let processes = state.processes.lock().unwrap();
        assert_eq!(processes.len(), 1);
        assert!(processes.get("run-1").is_none());
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

#[cfg(test)]
mod goal_pause_claim_tests {
    use super::{GoalPause, GoalPauseClaim, GoalPauseOwner, JobPauseState, PauseIntent};

    fn causes() -> Vec<Option<GoalPause>> {
        vec![
            None,
            Some(GoalPause::OperatorHold),
            Some(GoalPause::SafetyBrake),
            Some(GoalPause::Owned(GoalPauseOwner::DurableWait {
                wait_id: "wait-a".into(),
            })),
            Some(GoalPause::Owned(GoalPauseOwner::UserInput {
                request_id: "input-b".into(),
            })),
            Some(GoalPause::JobState(JobPauseState::Blocked)),
            Some(GoalPause::JobState(JobPauseState::Idle)),
        ]
    }

    #[test]
    fn reassert_never_invents_a_claim() {
        let mut claim = GoalPauseClaim::default();
        claim.stake(PauseIntent::Reassert);
        assert_eq!(claim.inspect(), None);
    }

    #[test]
    fn reassert_preserves_every_existing_cause() {
        for cause in causes().into_iter().flatten() {
            let mut claim = GoalPauseClaim {
                cause: Some(cause.clone()),
            };
            claim.stake(PauseIntent::Reassert);
            assert_eq!(claim.inspect(), Some(cause));
        }
    }

    #[test]
    fn inspecting_a_claimless_pause_does_not_create_one() {
        let claim = GoalPauseClaim::default();
        assert_eq!(claim.inspect(), None);
        assert_eq!(claim.inspect(), None);
    }

    #[test]
    fn stake_precedence_table() {
        for current in causes() {
            for proposed in causes().into_iter().flatten() {
                let allowed = match proposed {
                    GoalPause::OperatorHold | GoalPause::SafetyBrake => true,
                    GoalPause::Owned(_) => matches!(
                        current,
                        None | Some(GoalPause::JobState(_) | GoalPause::Owned(_))
                    ),
                    GoalPause::JobState(_) => {
                        matches!(current, None | Some(GoalPause::JobState(_)))
                    }
                };
                assert_eq!(proposed.may_supersede(current.as_ref()), allowed);
                let mut claim = GoalPauseClaim {
                    cause: current.clone(),
                };
                claim.stake(PauseIntent::Stake(proposed.clone()));
                assert_eq!(
                    claim.inspect(),
                    if allowed {
                        Some(proposed)
                    } else {
                        current.clone()
                    }
                );
            }
        }
    }

    #[test]
    fn release_unowned_clears_only_unowned_causes() {
        for cause in causes() {
            let allowed = matches!(cause, None | Some(GoalPause::JobState(_)));
            let mut claim = GoalPauseClaim {
                cause: cause.clone(),
            };
            let mut called = false;
            let released = claim
                .release_unowned(|| {
                    called = true;
                    Ok(())
                })
                .unwrap();
            assert_eq!(released, allowed);
            assert_eq!(called, allowed);
            assert_eq!(claim.inspect(), if allowed { None } else { cause });
        }
    }

    #[test]
    fn failed_activation_preserves_unowned_claim() {
        for cause in [
            None,
            Some(GoalPause::JobState(JobPauseState::Blocked)),
            Some(GoalPause::JobState(JobPauseState::Idle)),
        ] {
            let mut claim = GoalPauseClaim {
                cause: cause.clone(),
            };
            assert!(claim
                .release_unowned(|| Err("provider failure".into()))
                .is_err());
            assert_eq!(claim.inspect(), cause);
        }
    }

    #[test]
    fn rearm_safety_brake_is_unchanged() {
        for cause in causes() {
            let mut claim = GoalPauseClaim {
                cause: cause.clone(),
            };
            claim.rearm_safety_brake();
            assert_eq!(
                claim.inspect(),
                if cause == Some(GoalPause::SafetyBrake) {
                    Some(GoalPause::OperatorHold)
                } else {
                    cause
                }
            );
        }
    }
}
