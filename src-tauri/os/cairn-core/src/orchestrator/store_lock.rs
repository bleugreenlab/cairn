//! The per-project jj store lock: acquisition, its metrics, and the trace that
//! makes every hold attributable.
//!
//! One lock per project store serialises every jj op-log mutation on it
//! (transaction commit, bookmark set, rebase, import, export, op restore).
//! Concurrent jj operations from forked operation logs mint divergent copies of
//! the same change, so this single-writer discipline is load-bearing, and its
//! hold time is what every other store operation waits behind.
//!
//! A hold is therefore never anonymous. Each acquisition carries a
//! [`JjStoreOperationTrace`]; every phase that begins while the guard is held is
//! entered in the trace's hold ledger with its offset and duration, together
//! with any `packed-refs.lock` patience the phase spent. When a hold runs long,
//! the guard's drop names those phases in order, the time no phase accounts
//! for, and how long this holder itself waited to get in.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::Orchestrator;

pub(super) const JJ_STORE_LOCK_WARN_AFTER: Duration = Duration::from_secs(5);
pub(super) const JJ_STORE_LOCK_SAMPLE_CAPACITY: usize = 256;

/// Bound on how many phases one hold records. Phases are code-defined and a
/// hold has a handful, so this only stops a looping caller from growing a
/// ledger without limit; phases past it count as unattributed.
const HOLD_LEDGER_CAPACITY: usize = 64;

/// Who is running, for re-entrance detection: the tokio task, or, outside any
/// task, the thread. A future driven by `block_on` (a `#[tokio::test]` body,
/// `run_db_blocking`) has no task id but never leaves its thread, so keying
/// on the thread there still catches a holder about to wait on itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Holder {
    Task(tokio::task::Id),
    Thread(std::thread::ThreadId),
}

pub(super) fn current_holder() -> Holder {
    match tokio::task::try_id() {
        Some(task) => Holder::Task(task),
        None => Holder::Thread(std::thread::current().id()),
    }
}

#[derive(Default)]
pub struct JjStoreLockMetrics {
    waiters: Vec<WaiterEntry>,
    current_operation: Option<String>,
    current_task: Option<Holder>,
    held_since: Option<Instant>,
    pub(super) waits_ms: VecDeque<u64>,
    pub(super) holds_ms: VecDeque<u64>,
    pub(super) phase_ms: HashMap<String, VecDeque<u64>>,
}

/// One phase that began while its operation held the store lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HoldPhase {
    pub(super) name: Arc<str>,
    /// Offset from the moment the lock was acquired.
    pub(super) start: Duration,
    /// `None` while the phase is still running.
    pub(super) duration: Option<Duration>,
    /// Time the phase spent waiting out `packed-refs.lock` contention.
    pub(super) packed_refs: Duration,
}

/// The phases of one hold, shared by every clone of its trace.
struct HoldLedger {
    acquired_at: Instant,
    released: bool,
    phases: Vec<HoldPhase>,
}

/// Correlates phase timings that belong to one top-level store operation, even
/// when later phases deliberately run after the store guard has been released.
#[derive(Clone)]
pub struct JjStoreOperationTrace {
    operation: Arc<str>,
    store: Arc<str>,
    pub(super) correlation_id: Arc<str>,
    metrics: Arc<Mutex<HashMap<String, JjStoreLockMetrics>>>,
    hold: Arc<Mutex<HoldLedger>>,
}

pub struct JjStorePhaseTimer {
    trace: JjStoreOperationTrace,
    phase: Arc<str>,
    started_at: Instant,
    /// This phase's entry in the hold ledger, when it began inside the hold.
    slot: Option<usize>,
    packed_refs: Duration,
    /// Set for a phase measured elsewhere; otherwise the timer's own elapsed.
    duration: Option<Duration>,
}

impl JjStoreOperationTrace {
    pub(crate) fn phase(&self, phase: impl Into<Arc<str>>) -> JjStorePhaseTimer {
        self.phase_from(phase.into(), Instant::now())
    }

    fn phase_from(&self, phase: Arc<str>, started_at: Instant) -> JjStorePhaseTimer {
        let slot = {
            let mut hold = self.hold.lock().unwrap();
            (!hold.released && hold.phases.len() < HOLD_LEDGER_CAPACITY).then(|| {
                let start = started_at.saturating_duration_since(hold.acquired_at);
                hold.phases.push(HoldPhase {
                    name: phase.clone(),
                    start,
                    duration: None,
                    packed_refs: Duration::ZERO,
                });
                hold.phases.len() - 1
            })
        };
        JjStorePhaseTimer {
            trace: self.clone(),
            phase,
            started_at,
            slot,
            packed_refs: Duration::ZERO,
            duration: None,
        }
    }

    /// Run blocking `work` as one phase, attributing to it the
    /// `packed-refs.lock` patience it spends on this thread. Call it on the
    /// thread that does the work — inside the `spawn_blocking` closure, not
    /// around it — because the patience accumulator is per thread.
    pub(crate) fn blocking_phase<T>(
        &self,
        phase: impl Into<Arc<str>>,
        work: impl FnOnce() -> T,
    ) -> T {
        let mut timer = self.phase(phase);
        // Residue from unrelated earlier work on this pooled thread is not ours.
        let _ = cairn_vcs::jj::take_packed_refs_waited();
        let output = work();
        timer.absorb_packed_refs_wait();
        output
    }

    /// Enter a phase that was measured elsewhere — on a worker this trace
    /// could not follow into — as if a timer had run from `started_at`.
    pub(crate) fn record_phase(
        &self,
        phase: impl Into<Arc<str>>,
        started_at: Instant,
        duration: Duration,
        packed_refs: Duration,
    ) {
        let mut timer = self.phase_from(phase.into(), started_at);
        timer.packed_refs = packed_refs;
        timer.duration = Some(duration);
    }

    /// Enter both legs of a logical-head publication: the store transaction
    /// and the git export that must observe it.
    pub(crate) fn record_publication(&self, timings: &cairn_vcs::jj::PublicationTimings) {
        self.record_phase(
            "land",
            timings.land_started,
            timings.land,
            timings.land_packed_refs,
        );
        self.record_phase(
            "export",
            timings.export_started,
            timings.export,
            timings.export_packed_refs,
        );
    }

    pub(crate) fn deferred(&self, phase: &str) {
        log::debug!(
            "jj_store_phase correlation_id={} operation={} store={} phase={}",
            self.correlation_id,
            self.operation,
            self.store,
            phase
        );
    }
}

impl JjStorePhaseTimer {
    /// Attribute to this phase the `packed-refs.lock` patience spent on the
    /// current thread since the accumulator was last read.
    pub(crate) fn absorb_packed_refs_wait(&mut self) {
        self.packed_refs += cairn_vcs::jj::take_packed_refs_waited();
    }
}

impl Drop for JjStorePhaseTimer {
    fn drop(&mut self) {
        let elapsed = self.duration.unwrap_or_else(|| self.started_at.elapsed());
        if let Some(slot) = self.slot {
            if let Some(entry) = self.trace.hold.lock().unwrap().phases.get_mut(slot) {
                entry.duration = Some(elapsed);
                entry.packed_refs = self.packed_refs;
            }
        }
        let duration_ms = elapsed.as_millis().try_into().unwrap_or(u64::MAX);
        let phase_key = format!("{}/{}", self.trace.operation, self.phase);
        if let Some(metrics) = self
            .trace
            .metrics
            .lock()
            .unwrap()
            .get_mut(self.trace.store.as_ref())
        {
            record_bounded(metrics.phase_ms.entry(phase_key).or_default(), duration_ms);
        }
        if elapsed >= JJ_STORE_LOCK_WARN_AFTER {
            log::info!(
                "jj_store_phase correlation_id={} operation={} store={} phase={} duration_ms={}",
                self.trace.correlation_id,
                self.trace.operation,
                self.trace.store,
                self.phase,
                duration_ms
            );
        } else {
            log::debug!(
                "jj_store_phase correlation_id={} operation={} store={} phase={} duration_ms={}",
                self.trace.correlation_id,
                self.trace.operation,
                self.trace.store,
                self.phase,
                duration_ms
            );
        }
    }
}

pub(super) fn record_bounded(samples: &mut VecDeque<u64>, value: u64) {
    if samples.len() == JJ_STORE_LOCK_SAMPLE_CAPACITY {
        samples.pop_front();
    }
    samples.push_back(value);
}

pub(super) fn summarize_bounded(
    samples: &VecDeque<u64>,
) -> cairn_common::executor_protocol::BoundedDurationSummary {
    let mut sorted: Vec<_> = samples.iter().copied().collect();
    sorted.sort_unstable();
    let percentile = |n: usize| {
        sorted
            .get((sorted.len().saturating_sub(1) * n) / 100)
            .copied()
    };
    cairn_common::executor_protocol::BoundedDurationSummary {
        sample_count: sorted.len() as u64,
        p50_ms: percentile(50),
        p95_ms: percentile(95),
        max_ms: sorted.last().copied(),
    }
}

/// The attribution half of the `jj store lock hold exceeded` line: how long
/// this holder waited, its phases in the order they began, and the hold time
/// no phase covers. A phase still running when the hold ended is reported up
/// to the release and marked open; overlapping or nested phases are counted
/// once toward the covered time.
pub(super) fn describe_hold(held: Duration, waited: Duration, phases: &[HoldPhase]) -> String {
    let mut ordered: Vec<&HoldPhase> = phases.iter().collect();
    ordered.sort_by_key(|phase| phase.start);
    let listed = ordered
        .iter()
        .map(|phase| {
            let duration = phase
                .duration
                .unwrap_or_else(|| held.saturating_sub(phase.start));
            let mut text = format!("{} {:.1}s", phase.name, duration.as_secs_f64());
            if phase.duration.is_none() {
                text.push_str(" (open)");
            }
            if !phase.packed_refs.is_zero() {
                text.push_str(&format!(
                    " (packed-refs {:.1}s)",
                    phase.packed_refs.as_secs_f64()
                ));
            }
            text
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Union of the phase intervals, clipped to the hold.
    let mut covered = Duration::ZERO;
    let mut frontier = Duration::ZERO;
    for phase in &ordered {
        let start = phase.start.min(held);
        let end = phase
            .duration
            .map_or(held, |duration| phase.start.saturating_add(duration))
            .min(held);
        let start = start.max(frontier);
        if end > start {
            covered += end - start;
            frontier = end;
        }
    }
    format!(
        "waited={:.1}s, phases=[{listed}], unattributed={:.1}s",
        waited.as_secs_f64(),
        held.saturating_sub(covered).as_secs_f64()
    )
}

/// Who an acquisition is for, which decides where it queues.
///
/// The lock is handed to the oldest Foreground waiter before any Background
/// one: a turn or a job waiting on its branch is never queued behind upkeep that
/// nobody is waiting on directly. Upkeep still cannot starve, because a
/// Background waiter queued longer than [`BACKGROUND_PROMOTED_AFTER`] competes as
/// Foreground, by age.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreLockClass {
    /// A turn or a job is blocked on this acquisition.
    Foreground,
    /// Upkeep: reconciles, imports, check-ref retirement, maintenance.
    Background,
}

impl StoreLockClass {
    fn label(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Background => "background",
        }
    }
}

/// How long a Background waiter queues before it competes as Foreground.
pub(super) const BACKGROUND_PROMOTED_AFTER: Duration = Duration::from_secs(30);

/// The order the lock serves waiters in: promoted (Foreground, or Background
/// aged past the promotion bound) before unpromoted, and oldest first within
/// each. One function decides both the handoff and the queue position a status
/// reports, so the two cannot disagree.
fn service_rank(class: StoreLockClass, since: Instant, now: Instant) -> (u8, Instant) {
    let promoted = class == StoreLockClass::Foreground
        || now.saturating_duration_since(since) >= BACKGROUND_PROMOTED_AFTER;
    (u8::from(!promoted), since)
}

fn next_in_service(
    waiters: impl Iterator<Item = (StoreLockClass, Instant)>,
    now: Instant,
) -> Option<usize> {
    waiters
        .enumerate()
        .min_by_key(|(_, (class, since))| service_rank(*class, *since, now))
        .map(|(index, _)| index)
}

/// The per-store lock: exclusive, not reentrant, and served by class.
///
/// Ownership is a ticket. Release hands the lock directly to the next waiter in
/// service order by making its ticket the owner and waking it, so no third task
/// can barge in between. A waiter that is cancelled withdraws its ticket; one
/// cancelled after it was handed the lock but before it observed that passes
/// the lock straight on, so a handoff is never lost to a dropped future.
pub(super) struct PriorityLock {
    state: Mutex<PriorityState>,
}

#[derive(Default)]
struct PriorityState {
    owner: Option<u64>,
    next_ticket: u64,
    queue: Vec<QueuedTicket>,
}

struct QueuedTicket {
    ticket: u64,
    class: StoreLockClass,
    since: Instant,
    wake: Arc<tokio::sync::Notify>,
}

/// A queued ticket whose future may still be dropped.
struct PendingTicket {
    lock: Arc<PriorityLock>,
    ticket: u64,
    armed: bool,
}

impl Drop for PendingTicket {
    fn drop(&mut self) {
        if self.armed {
            self.lock.withdraw(self.ticket);
        }
    }
}

pub(super) struct PriorityGuard {
    lock: Arc<PriorityLock>,
    ticket: u64,
}

impl Drop for PriorityGuard {
    fn drop(&mut self) {
        self.lock.release(self.ticket);
    }
}

impl PriorityLock {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PriorityState::default()),
        })
    }

    pub(super) async fn acquire(self: Arc<Self>, class: StoreLockClass) -> PriorityGuard {
        let (ticket, wake) = {
            let mut state = self.state.lock().unwrap();
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            if state.owner.is_none() {
                state.owner = Some(ticket);
                drop(state);
                return PriorityGuard { lock: self, ticket };
            }
            let wake = Arc::new(tokio::sync::Notify::new());
            state.queue.push(QueuedTicket {
                ticket,
                class,
                since: Instant::now(),
                wake: wake.clone(),
            });
            (ticket, wake)
        };
        let mut pending = PendingTicket {
            lock: self,
            ticket,
            armed: true,
        };
        loop {
            // `notify_one` stores a permit when nobody is parked yet, so a
            // handoff between this check and the await is not lost.
            if pending.lock.state.lock().unwrap().owner == Some(ticket) {
                pending.armed = false;
                return PriorityGuard {
                    lock: pending.lock.clone(),
                    ticket,
                };
            }
            wake.notified().await;
        }
    }

    fn release(&self, ticket: u64) {
        let mut state = self.state.lock().unwrap();
        if state.owner != Some(ticket) {
            return;
        }
        state.owner = None;
        let now = Instant::now();
        if let Some(index) = next_in_service(
            state
                .queue
                .iter()
                .map(|queued| (queued.class, queued.since)),
            now,
        ) {
            let next = state.queue.remove(index);
            state.owner = Some(next.ticket);
            next.wake.notify_one();
        }
    }

    fn withdraw(&self, ticket: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(index) = state
            .queue
            .iter()
            .position(|queued| queued.ticket == ticket)
        {
            state.queue.remove(index);
            return;
        }
        drop(state);
        // Handed the lock, then dropped before it saw the handoff.
        self.release(ticket);
    }
}

/// One acquisition still waiting, as the status surfaces see it.
struct WaiterEntry {
    id: u64,
    operation: String,
    class: StoreLockClass,
    since: Instant,
    since_unix_ms: i64,
}

/// A waiter in a [`StoreLockStatus`], listed in the order it will be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreLockWaiter {
    pub(crate) operation: String,
    pub(crate) class: StoreLockClass,
    pub(crate) waited: Duration,
    pub(crate) since_unix_ms: i64,
}

/// Who holds a store's lock, for how long, and who is queued behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreLockStatus {
    pub(crate) holder: Option<(String, Duration)>,
    /// In service order: the first is served next.
    pub(crate) waiters: Vec<StoreLockWaiter>,
}

impl StoreLockStatus {
    /// The holder, named with how long it has held the lock.
    pub(crate) fn describe_holder(&self) -> String {
        match &self.holder {
            Some((operation, held)) => format!("{operation} for {}s", held.as_secs()),
            None => "none (the lock is being handed over)".to_string(),
        }
    }
}

/// What a job's node page says while its branch preparation queues for the
/// store, given where in the queue it is and when it began waiting.
pub(crate) fn describe_preparation_wait(
    status: &StoreLockStatus,
    position: usize,
    since: &str,
) -> String {
    format!(
        "Preparing branch: waiting for the project's version-control store since {since} \
         (holder: {}; {position} ahead)",
        status.describe_holder()
    )
}

/// The operation name a job's branch preparation acquires the store under. One
/// definition, because the node page finds the preparation's queue entry by it.
pub(crate) fn branch_preparation_operation(job_id: &str) -> String {
    format!("branch preparation for job {job_id}")
}

static NEXT_WAITER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct JjStoreWaiterRegistration {
    store: String,
    id: u64,
    wait_started: Instant,
    metrics: Arc<Mutex<HashMap<String, JjStoreLockMetrics>>>,
    record_wait: bool,
}

impl JjStoreWaiterRegistration {
    fn register(
        metrics: &Arc<Mutex<HashMap<String, JjStoreLockMetrics>>>,
        store: &str,
        operation: &str,
        class: StoreLockClass,
    ) -> Self {
        let id = NEXT_WAITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let wait_started = Instant::now();
        metrics
            .lock()
            .unwrap()
            .entry(store.to_string())
            .or_default()
            .waiters
            .push(WaiterEntry {
                id,
                operation: operation.to_string(),
                class,
                since: wait_started,
                since_unix_ms: chrono::Utc::now().timestamp_millis(),
            });
        Self {
            store: store.to_string(),
            id,
            wait_started,
            metrics: metrics.clone(),
            record_wait: false,
        }
    }

    fn finish(mut self) -> Duration {
        self.record_wait = true;
        let waited = self.wait_started.elapsed();
        self.release(waited);
        std::mem::forget(self);
        waited
    }

    fn release(&mut self, waited: Duration) {
        if let Some(metrics) = self.metrics.lock().unwrap().get_mut(&self.store) {
            metrics.waiters.retain(|waiter| waiter.id != self.id);
            if self.record_wait {
                record_bounded(
                    &mut metrics.waits_ms,
                    waited.as_millis().try_into().unwrap_or(u64::MAX),
                );
            }
        }
    }
}

impl Drop for JjStoreWaiterRegistration {
    fn drop(&mut self) {
        self.release(self.wait_started.elapsed());
    }
}

/// How a lifecycle edge is ordered against a store's publications
/// ([`Orchestrator::acquire_jj_store_lock_for_edge`]).
#[must_use = "the edge is ordered only while the hold lives"]
pub enum EdgeHold {
    /// This call took the lock; the edge is ordered while this lives.
    Acquired(JjStoreGuard),
    /// The calling task already holds the lock, which orders the edge.
    HeldByThisTask,
}

pub struct JjStoreGuard {
    _guard: PriorityGuard,
    trace: JjStoreOperationTrace,
    acquired_at: Instant,
    waited: Duration,
}

impl JjStoreGuard {
    pub(crate) fn trace(&self) -> JjStoreOperationTrace {
        self.trace.clone()
    }

    pub(crate) fn phase(&self, phase: impl Into<Arc<str>>) -> JjStorePhaseTimer {
        self.trace.phase(phase)
    }

    /// Whether this guard is the writer lock of the store at `store_dir`, so
    /// work on that store may run under it instead of re-acquiring the
    /// non-reentrant lock.
    pub(crate) fn covers(&self, store_dir: &Path) -> bool {
        *self.trace.store == *store_dir.to_string_lossy()
    }

    /// See [`JjStoreOperationTrace::blocking_phase`].
    pub(crate) fn blocking_phase<T>(
        &self,
        phase: impl Into<Arc<str>>,
        work: impl FnOnce() -> T,
    ) -> T {
        self.trace.blocking_phase(phase, work)
    }
}

impl Drop for JjStoreGuard {
    fn drop(&mut self) {
        let held = self.acquired_at.elapsed();
        let phases = {
            let mut hold = self.trace.hold.lock().unwrap();
            hold.released = true;
            std::mem::take(&mut hold.phases)
        };
        if let Some(metrics) = self
            .trace
            .metrics
            .lock()
            .unwrap()
            .get_mut(self.trace.store.as_ref())
        {
            metrics.current_operation = None;
            metrics.current_task = None;
            metrics.held_since = None;
            record_bounded(
                &mut metrics.holds_ms,
                held.as_millis().try_into().unwrap_or(u64::MAX),
            );
        }
        if held >= JJ_STORE_LOCK_WARN_AFTER {
            log::warn!(
                "jj store lock hold exceeded {:.1}s: correlation_id={}, operation={}, store={}, {}",
                held.as_secs_f64(),
                self.trace.correlation_id,
                self.trace.operation,
                self.trace.store,
                describe_hold(held, self.waited, &phases)
            );
        }
    }
}

/// A store lock held by a separate task; see
/// [`Orchestrator::hold_jj_store_lock_on_another_task`]. Dropping it asks the
/// holder to release; [`StoreHeldElsewhere::release`] also waits until the
/// lock is free.
#[cfg(test)]
pub(crate) struct StoreHeldElsewhere {
    release: Option<tokio::sync::oneshot::Sender<()>>,
    holder: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(test)]
impl StoreHeldElsewhere {
    pub(crate) async fn release(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(holder) = self.holder.take() {
            holder
                .await
                .expect("the holder task releases the store lock");
        }
    }
}

#[cfg(test)]
impl Drop for StoreHeldElsewhere {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Orchestrator {
    /// Get or create the per-store lock. Acquire it once per logical operation
    /// at exactly one level — the lock is not reentrant, so the inner reconcile
    /// helpers must never re-acquire it while it is held.
    fn jj_store_lock(&self, store_dir: &Path) -> Arc<PriorityLock> {
        let key = store_dir.to_string_lossy().into_owned();
        let mut map = self.jj_store_locks.lock().unwrap();
        map.entry(key).or_insert_with(PriorityLock::new).clone()
    }

    /// Acquire a project store lock while recording slow waits and holds.
    pub(crate) async fn acquire_jj_store_lock(
        &self,
        store_dir: &Path,
        class: StoreLockClass,
        operation: impl Into<String>,
    ) -> JjStoreGuard {
        self.acquire_jj_store_lock_with_timeout(store_dir, class, operation, None)
            .await
            .expect("an acquisition without a timeout cannot time out")
    }

    pub(crate) async fn acquire_jj_store_lock_with_timeout(
        &self,
        store_dir: &Path,
        class: StoreLockClass,
        operation: impl Into<String>,
        timeout: Option<Duration>,
    ) -> Result<JjStoreGuard, ()> {
        let operation = operation.into();
        let store = store_dir.to_string_lossy().into_owned();
        // Checked in release builds too. The lock is not reentrant, so a task
        // that queues behind its own guard is never served: the hold lasts until
        // something outside drops the whole future, and every other writer on the
        // store waits behind it. Panicking unwinds that task and releases its
        // guard now, and names the operation that re-entered.
        let current_task = current_holder();
        let held_by_this_task = {
            let metrics = self.jj_store_lock_metrics.lock().unwrap();
            metrics.get(&store).and_then(|metrics| metrics.current_task) == Some(current_task)
        };
        if held_by_this_task {
            panic!(
                "`{operation}` attempted to re-acquire the jj store lock for {store} from the \
                 task that already holds it; the lock is not reentrant, so this would wait on \
                 itself forever"
            );
        }
        let lock = self.jj_store_lock(store_dir);
        let waiter = JjStoreWaiterRegistration::register(
            &self.jj_store_lock_metrics,
            &store,
            &operation,
            class,
        );
        let guard = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, lock.acquire(class)).await {
                Ok(guard) => guard,
                Err(_) => {
                    let waited = waiter.finish();
                    log::warn!(
                        "jj store lock wait timed out after {:.1}s: operation={operation}, class={}, store={store}",
                        waited.as_secs_f64(),
                        class.label()
                    );
                    return Err(());
                }
            },
            None => lock.acquire(class).await,
        };
        let waited = waiter.finish();
        if waited >= JJ_STORE_LOCK_WARN_AFTER {
            log::warn!(
                "jj store lock wait exceeded {:.1}s: operation={operation}, class={}, store={store}",
                waited.as_secs_f64(),
                class.label()
            );
        }
        let acquired_at = Instant::now();
        let trace = JjStoreOperationTrace {
            operation: Arc::from(operation.clone()),
            store: Arc::from(store.clone()),
            correlation_id: Arc::from(uuid::Uuid::new_v4().to_string()),
            metrics: self.jj_store_lock_metrics.clone(),
            hold: Arc::new(Mutex::new(HoldLedger {
                acquired_at,
                released: false,
                phases: Vec::new(),
            })),
        };
        if let Some(metrics) = self.jj_store_lock_metrics.lock().unwrap().get_mut(&store) {
            metrics.current_operation = Some(operation.clone());
            metrics.current_task = Some(current_task);
            metrics.held_since = Some(acquired_at);
        }
        Ok(JjStoreGuard {
            _guard: guard,
            trace,
            acquired_at,
            waited,
        })
    }

    /// Hold a store's lock from an integration test, to make an operation
    /// queue behind it at a known point.
    #[cfg(feature = "test-utils")]
    pub async fn hold_jj_store_lock_for_test(&self, store_dir: &Path) -> JjStoreGuard {
        self.acquire_jj_store_lock(
            store_dir,
            StoreLockClass::Foreground,
            "integration test holder",
        )
        .await
    }

    /// Make `store_dir` busy with another writer: a spawned task takes the lock
    /// and keeps it until the returned handle is released or dropped. A guard
    /// held by the test body itself would make the body the holder, so the
    /// code under test, awaited from that same body, would be refused as
    /// re-entering its own hold instead of waiting behind a busy store.
    #[cfg(test)]
    pub(crate) async fn hold_jj_store_lock_on_another_task(
        &self,
        store_dir: &Path,
        class: StoreLockClass,
        operation: &str,
    ) -> StoreHeldElsewhere {
        let orch = self.clone();
        let store = store_dir.to_path_buf();
        let operation = operation.to_string();
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = tokio::spawn(async move {
            let guard = orch.acquire_jj_store_lock(&store, class, operation).await;
            let _ = held_tx.send(());
            let _ = release_rx.await;
            drop(guard);
        });
        held_rx
            .await
            .expect("the holder task acquires the store lock");
        StoreHeldElsewhere {
            release: Some(release_tx),
            holder: Some(holder),
        }
    }

    /// Take `store_dir`'s lock to order a lifecycle edge against publication,
    /// for a caller that may already be inside a hold of it.
    ///
    /// Either way the edge is ordered: it waits for the lock, without a
    /// timeout, or this task already holds it, and then every publication on
    /// the store is ordered against the edge by that same hold (acquiring again
    /// would panic on the reentrancy guard). There is no third answer. A
    /// timeout cannot prove the holder is waiting on this edge, and recording
    /// the edge without the lock is exactly the unordered state it exists to
    /// prevent: a publisher that took the lock and passed its authority check
    /// would still be sealing.
    pub(crate) async fn acquire_jj_store_lock_for_edge(
        &self,
        store_dir: &Path,
        operation: impl Into<String>,
    ) -> EdgeHold {
        let store = store_dir.to_string_lossy().into_owned();
        let current_task = current_holder();
        let held_by_this_task = {
            let metrics = self.jj_store_lock_metrics.lock().unwrap();
            metrics.get(&store).and_then(|metrics| metrics.current_task) == Some(current_task)
        };
        if held_by_this_task {
            return EdgeHold::HeldByThisTask;
        }
        EdgeHold::Acquired(
            self.acquire_jj_store_lock(store_dir, StoreLockClass::Foreground, operation)
                .await,
        )
    }

    /// The operations queued on a store's lock, in service order.
    pub fn jj_store_lock_waiters_for_test(&self, store_dir: &Path) -> Vec<String> {
        self.store_lock_status(store_dir)
            .map(|status| {
                status
                    .waiters
                    .into_iter()
                    .map(|waiter| waiter.operation)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Who holds `store_dir`'s lock and who is queued behind it, in service
    /// order. `None` for a store nothing has ever acquired.
    pub(crate) fn store_lock_status(&self, store_dir: &Path) -> Option<StoreLockStatus> {
        let all = self.jj_store_lock_metrics.lock().unwrap();
        all.get(store_dir.to_string_lossy().as_ref())
            .map(|metrics| metrics.status(Instant::now()))
    }

    /// Where a job's branch preparation stands in its store's queue: the
    /// store's status and how many waiters will be served before it. `None`
    /// when the preparation is not waiting.
    pub(crate) fn branch_preparation_wait(
        &self,
        job_id: &str,
    ) -> Option<(StoreLockStatus, usize, i64)> {
        let operation = branch_preparation_operation(job_id);
        let now = Instant::now();
        let all = self.jj_store_lock_metrics.lock().unwrap();
        all.values().find_map(|metrics| {
            let status = metrics.status(now);
            let position = status
                .waiters
                .iter()
                .position(|waiter| waiter.operation == operation)?;
            let since = status.waiters[position].since_unix_ms;
            Some((status, position, since))
        })
    }
}

impl JjStoreLockMetrics {
    pub(super) fn waiter_count(&self) -> usize {
        self.waiters.len()
    }

    fn status(&self, now: Instant) -> StoreLockStatus {
        let mut waiters: Vec<&WaiterEntry> = self.waiters.iter().collect();
        waiters.sort_by_key(|waiter| service_rank(waiter.class, waiter.since, now));
        StoreLockStatus {
            holder: self.current_operation.clone().map(|operation| {
                (
                    operation,
                    self.held_since
                        .map_or(Duration::ZERO, |since| now.saturating_duration_since(since)),
                )
            }),
            waiters: waiters
                .into_iter()
                .map(|waiter| StoreLockWaiter {
                    operation: waiter.operation.clone(),
                    class: waiter.class,
                    waited: now.saturating_duration_since(waiter.since),
                    since_unix_ms: waiter.since_unix_ms,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase(name: &str, start_ms: u64, duration_ms: Option<u64>, packed_ms: u64) -> HoldPhase {
        HoldPhase {
            name: Arc::from(name),
            start: Duration::from_millis(start_ms),
            duration: duration_ms.map(Duration::from_millis),
            packed_refs: Duration::from_millis(packed_ms),
        }
    }

    #[test]
    fn the_hold_line_names_phases_in_order_with_the_unattributed_remainder() {
        let line = describe_hold(
            Duration::from_millis(10_000),
            Duration::from_millis(12_000),
            &[
                phase("integrate", 900, Some(2_100), 0),
                phase("resolve", 0, Some(900), 0),
                phase("export", 3_000, Some(4_300), 3_100),
            ],
        );
        assert_eq!(
            line,
            "waited=12.0s, phases=[resolve 0.9s, integrate 2.1s, export 4.3s (packed-refs 3.1s)], \
             unattributed=2.7s"
        );
    }

    #[test]
    fn nested_and_open_phases_are_counted_once_and_clipped_to_the_hold() {
        let line = describe_hold(
            Duration::from_millis(5_000),
            Duration::ZERO,
            &[
                phase("transaction", 0, Some(3_000), 0),
                // Nested inside `transaction`: covered time must not double.
                phase("export", 1_000, Some(1_000), 0),
                // Still running when the hold ended.
                phase("push", 4_000, None, 0),
            ],
        );
        assert_eq!(
            line,
            "waited=0.0s, phases=[transaction 3.0s, export 1.0s, push 1.0s (open)], \
             unattributed=1.0s"
        );
    }

    #[test]
    fn a_hold_with_no_phases_is_wholly_unattributed() {
        assert_eq!(
            describe_hold(Duration::from_millis(7_250), Duration::from_millis(40), &[]),
            "waited=0.0s, phases=[], unattributed=7.2s"
        );
    }

    fn trace() -> JjStoreOperationTrace {
        JjStoreOperationTrace {
            operation: Arc::from("op"),
            store: Arc::from("store"),
            correlation_id: Arc::from("id"),
            metrics: Arc::default(),
            hold: Arc::new(Mutex::new(HoldLedger {
                acquired_at: Instant::now(),
                released: false,
                phases: Vec::new(),
            })),
        }
    }

    #[test]
    fn the_ledger_records_phases_begun_inside_the_hold_only() {
        let trace = trace();
        drop(trace.phase("inside"));
        let open = trace.phase("spans release");
        trace.hold.lock().unwrap().released = true;
        drop(trace.phase("after release"));
        drop(open);

        let phases = trace.hold.lock().unwrap().phases.clone();
        let names: Vec<_> = phases.iter().map(|phase| phase.name.as_ref()).collect();
        assert_eq!(names, ["inside", "spans release"]);
        assert!(phases.iter().all(|phase| phase.duration.is_some()));
    }

    #[test]
    fn a_blocking_phase_carries_the_packed_refs_patience_spent_inside_it() {
        let trace = trace();
        let temp = tempfile::tempdir().unwrap();
        trace.blocking_phase("export", || {
            let mut attempts = 0;
            cairn_vcs::jj::with_packed_refs_patience(
                cairn_vcs::jj::PackedRefs::Checkout(temp.path()),
                "unit test",
                || {
                    attempts += 1;
                    if attempts < 2 {
                        return Err(
                            "The lock for the packed-ref file could not be obtained".to_string()
                        );
                    }
                    Ok(())
                },
            )
            .unwrap();
        });
        let phases = trace.hold.lock().unwrap().phases.clone();
        assert_eq!(phases.len(), 1);
        assert!(
            phases[0].packed_refs >= Duration::from_millis(200),
            "one lost attempt sleeps a retry delay: {:?}",
            phases[0].packed_refs
        );
        assert!(cairn_vcs::jj::take_packed_refs_waited().is_zero());
    }

    #[test]
    fn a_phase_measured_elsewhere_is_entered_at_its_own_offset() {
        let trace = trace();
        let acquired_at = trace.hold.lock().unwrap().acquired_at;
        trace.record_phase(
            "export",
            acquired_at + Duration::from_millis(300),
            Duration::from_millis(1_200),
            Duration::from_millis(400),
        );
        let phases = trace.hold.lock().unwrap().phases.clone();
        assert_eq!(
            phases,
            [HoldPhase {
                name: Arc::from("export"),
                start: Duration::from_millis(300),
                duration: Some(Duration::from_millis(1_200)),
                packed_refs: Duration::from_millis(400),
            }]
        );
    }

    fn ago(seconds: u64) -> Instant {
        Instant::now()
            .checked_sub(Duration::from_secs(seconds))
            .unwrap()
    }

    #[test]
    fn foreground_is_served_before_an_older_background_waiter() {
        let now = Instant::now();
        let queue = [
            (StoreLockClass::Background, ago(10)),
            (StoreLockClass::Foreground, ago(2)),
            (StoreLockClass::Foreground, ago(1)),
        ];
        assert_eq!(next_in_service(queue.into_iter(), now), Some(1));
    }

    #[test]
    fn a_background_waiter_queued_past_the_bound_competes_as_foreground_by_age() {
        let now = Instant::now();
        let queue = [
            (StoreLockClass::Foreground, ago(5)),
            (StoreLockClass::Background, ago(31)),
        ];
        assert_eq!(
            next_in_service(queue.into_iter(), now),
            Some(1),
            "aged upkeep is older than the foreground waiter, so it goes first"
        );
        let queue = [
            (StoreLockClass::Background, ago(40)),
            (StoreLockClass::Foreground, ago(60)),
        ];
        assert_eq!(next_in_service(queue.into_iter(), now), Some(1));
        assert_eq!(next_in_service(std::iter::empty(), now), None);
    }

    /// Poll a future once, so it enqueues without being driven to completion.
    async fn poll_once<F: std::future::Future + Unpin>(future: &mut F) -> Option<F::Output> {
        tokio::time::timeout(Duration::ZERO, future).await.ok()
    }

    #[tokio::test]
    async fn release_hands_the_lock_to_foreground_ahead_of_queued_background() {
        let lock = PriorityLock::new();
        let holder = lock.clone().acquire(StoreLockClass::Background).await;
        let mut background = Box::pin(lock.clone().acquire(StoreLockClass::Background));
        assert!(poll_once(&mut background).await.is_none());
        let mut foreground = Box::pin(lock.clone().acquire(StoreLockClass::Foreground));
        assert!(poll_once(&mut foreground).await.is_none());

        drop(holder);
        let foreground_guard = poll_once(&mut foreground)
            .await
            .expect("foreground was handed the lock");
        assert!(
            poll_once(&mut background).await.is_none(),
            "background still waits"
        );
        drop(foreground_guard);
        assert!(poll_once(&mut background).await.is_some());
    }

    #[tokio::test]
    async fn a_waiter_dropped_after_its_handoff_passes_the_lock_on() {
        let lock = PriorityLock::new();
        let holder = lock.clone().acquire(StoreLockClass::Foreground).await;
        let mut first = Box::pin(lock.clone().acquire(StoreLockClass::Foreground));
        assert!(poll_once(&mut first).await.is_none());
        let mut second = Box::pin(lock.clone().acquire(StoreLockClass::Foreground));
        assert!(poll_once(&mut second).await.is_none());

        // The handoff makes `first` the owner before it is ever polled again.
        drop(holder);
        drop(first);
        assert!(
            poll_once(&mut second).await.is_some(),
            "the dropped owner's handoff reaches the next waiter"
        );
    }

    #[tokio::test]
    async fn a_waiter_dropped_while_queued_is_never_handed_the_lock() {
        let lock = PriorityLock::new();
        let holder = lock.clone().acquire(StoreLockClass::Foreground).await;
        let mut cancelled = Box::pin(lock.clone().acquire(StoreLockClass::Foreground));
        assert!(poll_once(&mut cancelled).await.is_none());
        drop(cancelled);
        assert!(lock.state.lock().unwrap().queue.is_empty());
        drop(holder);
        assert!(lock.state.lock().unwrap().owner.is_none());
        assert!(poll_once(&mut Box::pin(
            lock.clone().acquire(StoreLockClass::Background)
        ))
        .await
        .is_some());
    }

    #[test]
    fn status_lists_waiters_in_service_order_and_names_the_holder() {
        let now = Instant::now();
        let entry = |id, operation: &str, class, seconds| WaiterEntry {
            id,
            operation: operation.to_string(),
            class,
            since: now.checked_sub(Duration::from_secs(seconds)).unwrap(),
            since_unix_ms: 0,
        };
        let metrics = JjStoreLockMetrics {
            waiters: vec![
                entry(1, "sibling reconcile", StoreLockClass::Background, 20),
                entry(
                    2,
                    "branch preparation for job j",
                    StoreLockClass::Foreground,
                    3,
                ),
                entry(
                    3,
                    "logical write transaction",
                    StoreLockClass::Foreground,
                    8,
                ),
            ],
            current_operation: Some("run commit barrier publication".to_string()),
            held_since: now.checked_sub(Duration::from_secs(12)),
            ..JjStoreLockMetrics::default()
        };
        let status = metrics.status(now);
        let order: Vec<_> = status
            .waiters
            .iter()
            .map(|waiter| waiter.operation.as_str())
            .collect();
        assert_eq!(
            order,
            [
                "logical write transaction",
                "branch preparation for job j",
                "sibling reconcile"
            ]
        );
        assert_eq!(
            describe_preparation_wait(&status, 1, "17:02:11"),
            "Preparing branch: waiting for the project's version-control store since 17:02:11 \
             (holder: run commit barrier publication for 12s; 1 ahead)"
        );
    }

    #[test]
    fn the_ledger_is_bounded() {
        let trace = trace();
        for _ in 0..(HOLD_LEDGER_CAPACITY + 10) {
            drop(trace.phase("loop"));
        }
        assert_eq!(
            trace.hold.lock().unwrap().phases.len(),
            HOLD_LEDGER_CAPACITY
        );
    }
}

// ── Branch reservations ───────────────────────────────────────────────────────────────
//
// The store lock serialises jj operations, so it is held only for local work.
// A publication that must keep one branch still across network I/O (a landing's
// push, a run's delta settling onto a prepared head) reserves that branch
// instead: every writer that moves a bookmark holds its reservation while it
// moves it, so a reserved branch cannot move while the store lock is free for
// everyone else.
//
// Lock order is reservations, then the store lock, then a short database
// transaction. A reservation is never awaited while the same task holds the
// store lock, and never re-acquired by the task that already holds it: work
// that needs a reserved branch is handed the `BranchReservation`, and checks
// `covers` before it moves the branch.

/// The registry key of a store: its canonical path, so two spellings of one
/// store reserve the same branches.
fn canonical_store_key(store: &Path) -> String {
    std::fs::canonicalize(store)
        .unwrap_or_else(|_| store.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

#[derive(Default)]
struct ReservationSlot {
    lock: Arc<tokio::sync::Mutex<()>>,
    /// Who holds it, for refusals and re-entrance detection.
    holder: Mutex<Option<(String, Holder)>>,
}

struct HeldSlot {
    slot: Arc<ReservationSlot>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for HeldSlot {
    fn drop(&mut self) {
        *self.slot.holder.lock().unwrap() = None;
    }
}

/// Reserved bookmarks of one store. Dropping it releases them.
pub(crate) struct BranchReservation {
    store: String,
    /// Sorted and distinct.
    names: Vec<String>,
    purpose: String,
    _held: Vec<HeldSlot>,
}

impl BranchReservation {
    /// Whether this reservation holds `branch` of the store at `store`, by
    /// the store's canonical path.
    pub(crate) fn covers(&self, store: &Path, branch: &str) -> bool {
        self.store == canonical_store_key(store)
            && self
                .names
                .binary_search_by(|name| name.as_str().cmp(branch))
                .is_ok()
    }
}

impl std::fmt::Debug for BranchReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BranchReservation")
            .field("store", &self.store)
            .field("names", &self.names)
            .field("purpose", &self.purpose)
            .finish()
    }
}

/// Every branch reservation slot, keyed by canonical store and bookmark.
#[derive(Default)]
pub(crate) struct BranchReservations {
    slots: Mutex<HashMap<(String, String), Arc<ReservationSlot>>>,
}

impl BranchReservations {
    /// Reserve `names` of `store` in sorted order within one cumulative
    /// `timeout`. A refusal names the holder of the branch it could not get
    /// and releases whatever it already held. Waiting is cancel-safe: a
    /// dropped caller holds nothing.
    ///
    /// Refused, without waiting and without disturbing the existing hold, when
    /// the calling task already holds one of the branches: it would wait on
    /// itself. Pass the held reservation down instead.
    pub(crate) async fn reserve(
        &self,
        store: &Path,
        names: &[&str],
        purpose: &str,
        timeout: Duration,
    ) -> Result<BranchReservation, String> {
        let key = canonical_store_key(store);
        let mut sorted: Vec<String> = names.iter().map(|name| name.to_string()).collect();
        sorted.sort();
        sorted.dedup();
        let current_task = current_holder();
        let deadline = tokio::time::Instant::now() + timeout;
        let mut held = Vec::with_capacity(sorted.len());
        for name in &sorted {
            let slot = self
                .slots
                .lock()
                .unwrap()
                .entry((key.clone(), name.clone()))
                .or_default()
                .clone();
            if let Some((holder, task)) = slot.holder.lock().unwrap().clone() {
                if task == current_task {
                    // `held` drops here, releasing any names already taken.
                    return Err(format!(
                        "`{purpose}` tried to reserve `{name}` of {key} from the task that \
                         already reserved it for `{holder}`; reservations are not reentrant \
                         (an internal error: the held reservation should have been passed down)"
                    ));
                }
            }
            match tokio::time::timeout_at(deadline, slot.lock.clone().lock_owned()).await {
                Ok(guard) => {
                    *slot.holder.lock().unwrap() = Some((purpose.to_string(), current_task));
                    held.push(HeldSlot {
                        slot,
                        _guard: guard,
                    });
                }
                Err(_) => {
                    let holder = slot
                        .holder
                        .lock()
                        .unwrap()
                        .as_ref()
                        .map(|(holder, _)| holder.clone())
                        .unwrap_or_else(|| "another operation".to_string());
                    return Err(format!(
                        "`{name}` is reserved by {holder}; it was not released within {}s",
                        timeout.as_secs()
                    ));
                }
            }
        }
        Ok(BranchReservation {
            store: key,
            names: sorted,
            purpose: purpose.to_string(),
            _held: held,
        })
    }
}

impl BranchReservations {
    /// The bookmarks of `store` reserved now, with their holders' purposes,
    /// except those `owned` covers. Reads only the reserved set; never
    /// enumerates a store's branches.
    fn foreign(&self, store: &Path, owned: Option<&BranchReservation>) -> Vec<(String, String)> {
        let key = canonical_store_key(store);
        let slots = self.slots.lock().unwrap();
        let mut foreign: Vec<(String, String)> = slots
            .iter()
            .filter(|((slot_store, _), _)| *slot_store == key)
            .filter_map(|((_, name), slot)| {
                let holder = slot.holder.lock().unwrap().clone()?;
                let mine = owned.is_some_and(|owned| owned.covers(store, name));
                (!mine).then(|| (name.clone(), holder.0))
            })
            .collect();
        foreign.sort();
        foreign
    }
}

impl Orchestrator {
    /// Admission for a mutation that may move bookmarks it does not name (an
    /// import, a store initialisation, a name repair). Under the store lock
    /// `guard`, it refuses before anything is mutated while any bookmark of
    /// that store is reserved by someone other than `owned`: such a mutation
    /// could move a branch a publication is holding across network I/O.
    /// Never waits. Callers that name the one branch they move check
    /// [`BranchReservation::covers`] instead.
    pub(crate) fn refuse_foreign_reservations(
        &self,
        guard: &JjStoreGuard,
        owned: Option<&BranchReservation>,
    ) -> Result<(), String> {
        let store = Path::new(guard.trace.store.as_ref());
        let foreign = self.branch_reservations.foreign(store, owned);
        if foreign.is_empty() {
            return Ok(());
        }
        Err(format!(
            "deferred: {} reserved for publication ({}), and this operation could move it; \
             retry once the reservation is released",
            if foreign.len() == 1 {
                "a branch is"
            } else {
                "branches are"
            },
            foreign
                .iter()
                .map(|(name, purpose)| format!("`{name}` by {purpose}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    /// The purpose of whoever reserves `branch` of `store` now, if anyone.
    #[cfg(feature = "test-utils")]
    pub(crate) fn branch_reservation_holder(&self, store: &Path, branch: &str) -> Option<String> {
        self.branch_reservations
            .foreign(store, None)
            .into_iter()
            .find(|(name, _)| name == branch)
            .map(|(_, purpose)| purpose)
    }

    /// Reserve bookmarks of a project store. See [`BranchReservations::reserve`].
    ///
    /// Refused, without waiting, when the calling task holds `store`'s writer
    /// lock: reservations come before the store lock, never under it.
    pub(crate) async fn reserve_branches(
        &self,
        store: &Path,
        names: &[&str],
        purpose: &str,
        timeout: Duration,
    ) -> Result<BranchReservation, String> {
        {
            let current = current_holder();
            let raw = store.to_string_lossy().into_owned();
            let canonical = canonical_store_key(store);
            let metrics = self.jj_store_lock_metrics.lock().unwrap();
            let holds_guard = [raw.as_str(), canonical.as_str()].iter().any(|key| {
                metrics.get(*key).and_then(|metrics| metrics.current_task) == Some(current)
            });
            if holds_guard {
                return Err(format!(
                    "`{purpose}` tried to reserve branches of {raw} while holding its store \
                     lock; reservations are taken before the store lock (an internal error)"
                ));
            }
        }
        self.branch_reservations
            .reserve(store, names, purpose, timeout)
            .await
    }
}

// ── Branch write admission ────────────────────────────────────────────────────────────
//
// An install whose acceptance is unsettled (a `pending` or `unknown` row of the
// acceptance journal, which `base_advance::preparation` owns) leaves the
// branch's head ambiguous: the row's `head_after` may be a commit that already
// existed, so only the exact head read while the row is still owed proves
// anything. While a row is owed, nothing but the preparation that settles it
// may move the branch.
//
// The journal is the only record of debt. Nothing here caches it, so after a
// restart the first writer reads (and settles) the same rows before it moves
// anything.
//
// A writer that names the branch it moves mints a [`BranchWritePermit`] under
// the store guard while holding that branch's reservation. A store-wide
// mutation (an import, an operation settlement) cannot name what it moves, and
// must not stop on another branch's debt either:
// [`Orchestrator::bracket_store_mutation`] reads only the owed branches'
// bookmarks around it and marks a row `unknown` when its branch moved. Both
// refuse foreign reservations before anything is mutated.

use crate::orchestrator::base_advance::preparation::{self, BusyReason, PreparationRefusal};
use crate::storage::{LocalDb, RowExt};

/// Witness that, under the borrowed store guard, `branch` is reserved by the
/// caller and owes no acceptance. Minted only by
/// [`JjStoreGuard::branch_write_permit`], and lives no longer than that guard:
/// a writer that releases the guard for network I/O mints a fresh permit under
/// the next one. That re-check is cheap and passes unless the branch moved
/// outside Cairn, because only a preparation opens a journal row and a
/// preparation must hold the branch's reservation.
pub(crate) struct BranchWritePermit<'g> {
    _guard: std::marker::PhantomData<&'g JjStoreGuard>,
}

/// Which run batches may hold the branch while a writer moves it.
#[derive(Clone, Copy)]
pub(crate) enum BatchAdmission<'a> {
    /// An ordinary writer: refused while any run batch on the branch may
    /// still commit to it.
    None,
    /// A run's (or a held drain's) publication of its own sealed result: only
    /// that exact job and request are exempt, and only on the project, store
    /// and branch it was sealed for, so another batch, or a child task's batch
    /// on the same branch, still refuses.
    Sealed(&'a crate::mcp::handlers::run::SealedPublication),
}

/// Why a branch writer was not admitted. In every case nothing was moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchAdmissionRefusal {
    /// The caller does not hold the branch's reservation.
    Unreserved { branch: String },
    /// A journal row is still owed after settlement against the exact head.
    Debt {
        branch: String,
        acceptance_id: String,
        status: String,
    },
    /// The journal could not be read, or the store not resolved to its
    /// journal identity: whether the branch owes an acceptance is unknown.
    JournalUnreadable { branch: String, error: String },
    /// A run batch on the branch may still commit to it.
    InFlightBatch { branch: String, batch: String },
    /// The in-flight run batches could not be read: fail closed.
    BatchUnreadable { branch: String, error: String },
}

impl std::fmt::Display for BranchAdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreserved { branch } => write!(
                f,
                "`{branch}` was not moved: this writer does not hold its reservation (an \
                 internal error: a writer reserves the branch before taking the store lock)"
            ),
            Self::Debt {
                branch,
                acceptance_id,
                status,
            } => {
                let remedy = if status == "unknown" {
                    "its head is neither the commit that install started from nor the one it \
                     installed, so it needs a repair before anything moves it"
                } else {
                    "it settles at the branch's next guarded preparation once its head is exact"
                };
                write!(
                    f,
                    "`{branch}` was not moved and nothing was published: an earlier install of \
                     it is not settled (acceptance {acceptance_id} is {status}); {remedy}"
                )
            }
            Self::JournalUnreadable { branch, error } => write!(
                f,
                "`{branch}` was not moved: its acceptance journal could not be read ({error}), \
                 so whether it owes an acceptance is unknown"
            ),
            Self::InFlightBatch { branch, batch } => write!(
                f,
                "`{branch}` was not moved and nothing was published: {batch} may still commit \
                 to this branch"
            ),
            Self::BatchUnreadable { branch, error } => write!(
                f,
                "`{branch}` was not moved: its in-flight run batches could not be read \
                 ({error})"
            ),
        }
    }
}

impl From<BranchAdmissionRefusal> for String {
    fn from(refusal: BranchAdmissionRefusal) -> Self {
        refusal.to_string()
    }
}

impl From<BranchAdmissionRefusal> for PreparationRefusal {
    fn from(refusal: BranchAdmissionRefusal) -> Self {
        match refusal {
            BranchAdmissionRefusal::Debt {
                acceptance_id,
                status,
                ..
            } => Self::Busy(BusyReason::AcceptanceDebt {
                acceptance_id,
                status,
            }),
            BranchAdmissionRefusal::InFlightBatch { batch, .. } => {
                Self::Busy(BusyReason::RunBatch(batch))
            }
            other @ (BranchAdmissionRefusal::Unreserved { .. }
            | BranchAdmissionRefusal::JournalUnreadable { .. }
            | BranchAdmissionRefusal::BatchUnreadable { .. }) => {
                Self::Repository(other.to_string())
            }
        }
    }
}

impl JjStoreGuard {
    fn store_path(&self) -> &Path {
        Path::new(self.trace.store.as_ref())
    }

    /// Admit a writer of `branch` of this guard's store: the caller must hold
    /// the branch's `reservation`; no run batch on the branch may still commit
    /// to it, except the exact sealed publication `batch` names (a capability
    /// sealed for another project, store or branch exempts nothing); and the branch
    /// must owe no acceptance once its rows are settled against its exact head
    /// (so a pending row whose install is exactly the head is accepted, and
    /// the same call admits). The batch rows are read fresh under this guard,
    /// before any settlement write. Indexed reads only; no graph work and no
    /// network. A store not created yet has no bookmark for a row to describe,
    /// so it owes nothing.
    pub(crate) async fn branch_write_permit<'g>(
        &'g self,
        db: &LocalDb,
        reservation: &BranchReservation,
        project_id: &str,
        branch: &str,
        batch: BatchAdmission<'_>,
    ) -> Result<BranchWritePermit<'g>, BranchAdmissionRefusal> {
        let store = self.store_path();
        if !reservation.covers(store, branch) {
            return Err(BranchAdmissionRefusal::Unreserved {
                branch: branch.to_string(),
            });
        }
        let exempt = match batch {
            BatchAdmission::Sealed(sealed) if sealed.is_for(project_id, store, branch) => {
                Some((sealed.job_id(), sealed.request_id()))
            }
            BatchAdmission::Sealed(_) | BatchAdmission::None => None,
        };
        match crate::orchestrator::base_advance::inflight_run_batch_except(
            db, project_id, branch, None, exempt,
        )
        .await
        {
            Ok(None) => {}
            Ok(Some(held)) => {
                return Err(BranchAdmissionRefusal::InFlightBatch {
                    branch: branch.to_string(),
                    batch: held.describe(),
                })
            }
            Err(error) => {
                return Err(BranchAdmissionRefusal::BatchUnreadable {
                    branch: branch.to_string(),
                    error,
                })
            }
        }
        let permit = BranchWritePermit {
            _guard: std::marker::PhantomData,
        };
        if !store.exists() {
            return Ok(permit);
        }
        match preparation::settle_branch_acceptances(db, self, project_id, store, branch).await {
            Ok(None) => Ok(permit),
            Ok(Some(debt)) => Err(BranchAdmissionRefusal::Debt {
                branch: branch.to_string(),
                acceptance_id: debt.id,
                status: debt.status,
            }),
            Err(error) => Err(BranchAdmissionRefusal::JournalUnreadable {
                branch: branch.to_string(),
                error,
            }),
        }
    }
}

/// The store's operation as one guard hold captured it, so that hold, and only
/// that hold, may restore it. Minted by [`JjStoreGuard::capture_operation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GuardOperation {
    operation: String,
    hold: Arc<str>,
}

impl JjStoreGuard {
    /// Capture the store's current operation under this hold, for
    /// [`Self::undo_own_operations`].
    ///
    /// Read in process from the operation heads, never through jj, which
    /// would settle a split log as a side effect: a split has no single
    /// operation to return to and is refused.
    pub(crate) fn capture_operation(
        &self,
        _jj: &crate::jj::JjEnv,
    ) -> Result<GuardOperation, String> {
        match cairn_vcs::settled_operation(self.store_path())? {
            cairn_vcs::SettledOperation::Single(operation) => Ok(GuardOperation {
                operation,
                hold: self.trace.correlation_id.clone(),
            }),
            cairn_vcs::SettledOperation::Split(heads) => Err(format!(
                "the operation log of {} is split ({}), so there is no single operation this \
                 hold could return to",
                self.trace.store,
                heads.join(", ")
            )),
        }
    }

    /// Restore the operation `captured` by this same hold, undoing only what
    /// this hold did since. The guard excludes every other Cairn writer for
    /// the whole hold, so no other branch's move or acceptance can lie between
    /// the capture and the restore, and neither a foreign reservation nor an
    /// owed acceptance stops it: refusing would leave this hold's own partial
    /// work installed. A capture from any other hold (including an earlier
    /// hold of this same store) is refused, since other writers may have run
    /// in between.
    pub(crate) fn undo_own_operations(
        &self,
        jj: &crate::jj::JjEnv,
        captured: &GuardOperation,
    ) -> Result<(), String> {
        if captured.hold != self.trace.correlation_id {
            return Err(format!(
                "refusing to restore operation {} of {}: it was captured under another store \
                 lock hold, so other writers may have run since",
                captured.operation, self.trace.store
            ));
        }
        crate::jj::restore_operation(jj, self.store_path(), &captured.operation)
    }
}

impl JjStoreGuard {
    /// Admit the creation of `branch`'s bookmark, which does not exist yet.
    /// Refused while the journal still owes an acceptance under that name: a
    /// deleted branch re-created would inherit rows whose `head_after` may be
    /// the very commit it is created at. No reservation is needed, since no
    /// writer can hold a bookmark that does not exist.
    pub(crate) async fn branch_create_permit<'g>(
        &'g self,
        db: &LocalDb,
        project_id: &str,
        branch: &str,
    ) -> Result<BranchWritePermit<'g>, BranchAdmissionRefusal> {
        let store = self.store_path();
        let permit = BranchWritePermit {
            _guard: std::marker::PhantomData,
        };
        if !store.exists() {
            return Ok(permit);
        }
        match preparation::outstanding_acceptances(db, project_id, store, branch).await {
            Ok(owed) => match owed.into_iter().next() {
                None => Ok(permit),
                Some(debt) => Err(BranchAdmissionRefusal::Debt {
                    branch: branch.to_string(),
                    acceptance_id: debt.id,
                    status: debt.status,
                }),
            },
            Err(error) => Err(BranchAdmissionRefusal::JournalUnreadable {
                branch: branch.to_string(),
                error,
            }),
        }
    }
}

/// One owed journal row and its branch's local copies, as read around a
/// store-wide mutation: the store bookmark (what settlement reads) and the Git
/// ref (what export recovery reads). `Err` when they could not be read from
/// one settled operation.
struct OwedBranch {
    acceptance_id: String,
    branch: String,
    copies: Result<(Option<String>, Option<String>), String>,
}

/// The project store's owed rows as `(id, branch)`, by its journal identity. A
/// store that does not exist yet has no bookmarks for a row to describe.
async fn owed_rows(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
) -> Result<Vec<(String, String)>, String> {
    if !store.exists() {
        return Ok(Vec::new());
    }
    let (project, key) = (
        project_id.to_string(),
        preparation::journal_store_key(store)?,
    );
    db.read(move |conn| {
        let (project, key) = (project.clone(), key.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, branch FROM branch_advance_acceptances
                      WHERE project_id = ?1 AND store_path = ?2
                        AND status IN ('pending', 'unknown')
                      ORDER BY created_at, id",
                    (project.as_str(), key.as_str()),
                )
                .await?;
            let mut owed = Vec::new();
            while let Some(row) = rows.next().await? {
                owed.push((row.text(0)?, row.text(1)?));
            }
            Ok(owed)
        })
    })
    .await
    .map_err(|error| format!("read the store's owed acceptances: {error}"))
}

/// Read the owed branches' bookmarks, each from one settled operation, on a
/// blocking worker.
async fn read_owed_bookmarks(store: &Path, owed: &[(String, String)]) -> Vec<OwedBranch> {
    if owed.is_empty() {
        return Vec::new();
    }
    let (store, rows) = (store.to_path_buf(), owed.to_vec());
    let read = tokio::task::spawn_blocking(move || {
        rows.into_iter()
            .map(|(acceptance_id, branch)| OwedBranch {
                copies: crate::jj::read_local_branch_state(&store, &branch)
                    .map(|state| (state.bookmark, state.git_ref)),
                acceptance_id,
                branch,
            })
            .collect()
    })
    .await;
    read.unwrap_or_else(|error| {
        owed.iter()
            .map(|(acceptance_id, branch)| OwedBranch {
                acceptance_id: acceptance_id.clone(),
                branch: branch.clone(),
                copies: Err(format!("the branch read worker failed: {error}")),
            })
            .collect()
    })
}

/// Fence every owed row with its branch's copies before a store-wide
/// mutation, durably and in one transaction. A branch whose copies cannot be
/// read cannot be fenced, and a fence that cannot be written protects
/// nothing: either refuses the mutation before anything changes.
async fn fence_owed(db: &LocalDb, bracket_id: &str, before: &[OwedBranch]) -> Result<(), String> {
    let mut fences = Vec::with_capacity(before.len());
    for owed in before {
        match &owed.copies {
            Ok((bookmark, git_ref)) => fences.push(preparation::AcceptanceFence {
                id: owed.acceptance_id.clone(),
                bracket_id: bracket_id.to_string(),
                bookmark: bookmark.clone(),
                git_ref: git_ref.clone(),
            }),
            Err(error) => {
                return Err(format!(
                    "deferred: `{}` owes acceptance {} and its copies cannot be read ({error}), \
                     so this store mutation could not be fenced; nothing was changed",
                    owed.branch, owed.acceptance_id
                ))
            }
        }
    }
    if fences.is_empty() {
        return Ok(());
    }
    preparation::fence_acceptances(db, &fences)
        .await
        .map_err(|error| {
            format!(
                "deferred: owed acceptances could not be fenced before this store mutation, so \
                 nothing was changed: {error}"
            )
        })
}

/// Resolve this bracket's fences: a row whose branch's bookmark or Git ref the
/// mutation moved (or that cannot be read now) becomes `unknown`, since no
/// head proves anything about it and a settlement could abandon a row whose
/// stale Git ref an import dragged back to `head_before`; every other fence is
/// cleared. If the resolution cannot be written, or this task never gets here
/// (a crash), the fence stays and the branch's next settlement resolves it
/// from the stored copies first, so the caller's own result passes through.
async fn resolve_fences(
    db: &LocalDb,
    store: &Path,
    bracket_id: &str,
    before: Vec<OwedBranch>,
    after: Vec<OwedBranch>,
) {
    if before.is_empty() {
        return;
    }
    let mut moved = Vec::new();
    for (before, after) in before.into_iter().zip(after) {
        let unchanged = matches!(
            (&before.copies, &after.copies),
            (Ok(old), Ok(new)) if old == new
        );
        if unchanged {
            continue;
        }
        log::warn!(
            "a store-wide mutation of {} moved `{}` ((bookmark, git ref) {:?} -> {:?}) while \
             acceptance {} was owed; it is now unknown and needs a repair before anything \
             moves the branch",
            store.display(),
            before.branch,
            before.copies,
            after.copies,
            before.acceptance_id
        );
        moved.push(before.acceptance_id);
    }
    if let Err(error) = preparation::resolve_acceptance_fences(db, bracket_id, &moved).await {
        log::error!(
            "bracket {bracket_id} of {}: its fences could not be resolved ({error}); each \
             fenced branch's next settlement resolves its fence first",
            store.display()
        );
    }
}

impl Orchestrator {
    /// Run a store-wide `mutation` (one that may move bookmarks it does not
    /// name: an import, an operation-head settlement, a store initialisation)
    /// under `guard`, which this call owns until it returns the guard.
    ///
    /// Refuses before anything is mutated while another operation reserves a
    /// branch of the store (see [`Self::refuse_foreign_reservations`]). An
    /// owed acceptance does not block it: each owed row is durably fenced
    /// with its branch's bookmark and Git ref first (a fence that cannot be
    /// written refuses), the copies are read again after, and a moved row
    /// becomes `unknown`. A fence left behind by a crash or a failed write is
    /// resolved by the branch's next settlement. Other branches import
    /// normally; a store with no owed rows writes nothing.
    ///
    /// The bracket runs on its own task, with the mutation on a blocking
    /// worker, so a caller that stops awaiting cannot release the guard between
    /// the mutation and the check after it.
    pub(crate) async fn bracket_store_mutation<T, F>(
        &self,
        guard: JjStoreGuard,
        owned: Option<&BranchReservation>,
        db: Arc<LocalDb>,
        project_id: &str,
        mutation: F,
    ) -> Result<(JjStoreGuard, T), String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, String> + Send + 'static,
    {
        self.refuse_foreign_reservations(&guard, owned)?;
        let project_id = project_id.to_string();
        tokio::spawn(async move {
            let store = guard.store_path().to_path_buf();
            let owed = owed_rows(&db, &project_id, &store).await?;
            let before = read_owed_bookmarks(&store, &owed).await;
            let bracket_id = format!("bracket-{}", uuid::Uuid::new_v4());
            fence_owed(&db, &bracket_id, &before).await?;
            let result = tokio::task::spawn_blocking(mutation)
                .await
                .map_err(|error| format!("the store mutation worker failed: {error}"))
                .and_then(|result| result);
            let after = read_owed_bookmarks(&store, &owed).await;
            resolve_fences(&db, &store, &bracket_id, before, after).await;
            result.map(|value| (guard, value))
        })
        .await
        .map_err(|error| format!("the bracketed store mutation did not complete: {error}"))?
    }
}

#[cfg(test)]
mod branch_reservation_tests {
    use super::*;

    const WAIT: Duration = Duration::from_millis(200);

    /// A holder outside any tokio task (a `block_on`, as `run_db_blocking`
    /// drives futures) is still caught re-reserving its own branch, at once.
    #[test]
    fn a_holder_outside_any_task_is_refused_at_once_too() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let registry = BranchReservations::default();
        let store = tempfile::tempdir().unwrap();
        runtime.block_on(async {
            assert!(
                tokio::task::try_id().is_none(),
                "the fixture runs outside a task"
            );
            let held = registry
                .reserve(store.path(), &["main"], "landing", WAIT)
                .await
                .unwrap();
            let started = Instant::now();
            let refused = registry
                .reserve(store.path(), &["main"], "nested", Duration::from_secs(30))
                .await
                .unwrap_err();
            assert!(refused.contains("not reentrant"), "{refused}");
            assert!(started.elapsed() < Duration::from_secs(5));
            assert!(held.covers(store.path(), "main"));
        });
    }

    #[tokio::test]
    async fn distinct_branches_reserve_independently() {
        let registry = BranchReservations::default();
        let store = tempfile::tempdir().unwrap();
        let landing = registry
            .reserve(store.path(), &["agent/a", "main"], "landing a", WAIT)
            .await
            .unwrap();
        let other = registry
            .reserve(store.path(), &["agent/b"], "write b", WAIT)
            .await
            .expect("an unrelated branch is never held up by a landing");
        assert!(landing.covers(store.path(), "main"));
        assert!(landing.covers(store.path(), "agent/a"));
        assert!(!landing.covers(store.path(), "agent/b"));
        assert!(other.covers(store.path(), "agent/b"));
    }

    #[tokio::test]
    async fn a_reserved_branch_refuses_naming_its_holder_and_frees_on_drop() {
        let registry = Arc::new(BranchReservations::default());
        let store = tempfile::tempdir().unwrap();
        let landing = registry
            .reserve(store.path(), &["agent/a", "main"], "landing a", WAIT)
            .await
            .unwrap();
        // Another task: re-entrance detection is per task.
        let (registry2, path) = (registry.clone(), store.path().to_path_buf());
        let refused = tokio::spawn(async move {
            registry2
                .reserve(&path, &["agent/a"], "owner write", WAIT)
                .await
                .map(|_| ())
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(refused.contains("landing a"), "{refused}");
        drop(landing);
        registry
            .reserve(store.path(), &["agent/a"], "owner write", WAIT)
            .await
            .expect("released on drop");
    }

    #[tokio::test]
    async fn a_partial_reservation_is_released_when_a_later_name_times_out() {
        let registry = Arc::new(BranchReservations::default());
        let store = tempfile::tempdir().unwrap();
        let (registry2, path) = (registry.clone(), store.path().to_path_buf());
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = tokio::spawn(async move {
            let main = registry2
                .reserve(&path, &["main"], "import main", WAIT)
                .await
                .unwrap();
            held_tx.send(()).unwrap();
            let _ = release_rx.await;
            drop(main);
        });
        held_rx.await.unwrap();
        let refused = registry
            .reserve(store.path(), &["agent/a", "main"], "landing a", WAIT)
            .await
            .unwrap_err();
        assert!(refused.contains("import main"), "{refused}");
        registry
            .reserve(store.path(), &["agent/a"], "owner write", WAIT)
            .await
            .expect("`agent/a` was released with the failed reservation");
        release_tx.send(()).unwrap();
        holder.await.unwrap();
    }

    #[tokio::test]
    async fn two_spellings_of_one_store_reserve_the_same_branch() {
        let registry = Arc::new(BranchReservations::default());
        let store = tempfile::tempdir().unwrap();
        std::fs::create_dir(store.path().join("inner")).unwrap();
        let plain = store.path().join("inner");
        let dotted = store.path().join("inner").join("..").join("inner");
        let held = registry
            .reserve(&plain, &["main"], "landing", WAIT)
            .await
            .unwrap();
        assert!(held.covers(&dotted, "main"));
        let (registry2, dotted2) = (registry.clone(), dotted.clone());
        let refused = tokio::spawn(async move {
            registry2
                .reserve(&dotted2, &["main"], "import", WAIT)
                .await
                .map(|_| ())
        })
        .await
        .unwrap();
        assert!(refused.is_err(), "the alias reserves the same slot");
    }

    #[tokio::test]
    async fn foreign_reservations_list_only_reserved_names_not_owned_ones() {
        let registry = BranchReservations::default();
        let store = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        assert!(registry.foreign(store.path(), None).is_empty());
        let landing = registry
            .reserve(store.path(), &["agent/a", "main"], "landing a", WAIT)
            .await
            .unwrap();
        let elsewhere = registry
            .reserve(other.path(), &["main"], "landing elsewhere", WAIT)
            .await
            .unwrap();
        assert_eq!(
            registry.foreign(store.path(), None),
            vec![
                ("agent/a".to_string(), "landing a".to_string()),
                ("main".to_string(), "landing a".to_string())
            ],
            "only this store's reserved names"
        );
        assert!(
            registry.foreign(store.path(), Some(&landing)).is_empty(),
            "the owner's own reservation is not foreign"
        );
        assert_eq!(registry.foreign(store.path(), Some(&elsewhere)).len(), 2);
        drop(landing);
        assert!(
            registry.foreign(store.path(), None).is_empty(),
            "a released slot is no longer reserved"
        );
    }

    #[tokio::test]
    async fn reserving_a_branch_this_task_holds_is_refused_at_once_and_keeps_the_hold() {
        let registry = Arc::new(BranchReservations::default());
        let store = tempfile::tempdir().unwrap();
        let held = registry
            .reserve(store.path(), &["main"], "landing", WAIT)
            .await
            .unwrap();
        let started = Instant::now();
        let refused = registry
            .reserve(
                store.path(),
                &["agent/a", "main"],
                "nested",
                Duration::from_secs(30),
            )
            .await
            .unwrap_err();
        assert!(refused.contains("not reentrant"), "{refused}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "refused without waiting"
        );
        assert!(
            held.covers(store.path(), "main"),
            "the existing hold is intact"
        );
        let (registry2, path) = (registry.clone(), store.path().to_path_buf());
        let still_reserved = tokio::spawn(async move {
            registry2
                .reserve(&path, &["main"], "other", WAIT)
                .await
                .is_err()
        })
        .await
        .unwrap();
        assert!(still_reserved, "`main` is still held by the landing");
        registry
            .reserve(store.path(), &["agent/a"], "later", WAIT)
            .await
            .expect("the refused attempt released `agent/a`");
    }
}
