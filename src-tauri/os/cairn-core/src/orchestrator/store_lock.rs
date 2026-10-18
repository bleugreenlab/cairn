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

#[derive(Default)]
pub struct JjStoreLockMetrics {
    waiters: Vec<WaiterEntry>,
    current_operation: Option<String>,
    current_task: Option<tokio::task::Id>,
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
        let current_task = tokio::task::try_id();
        debug_assert!(
            current_task.is_none()
                || self
                    .jj_store_lock_metrics
                    .lock()
                    .unwrap()
                    .get(&store)
                    .and_then(|metrics| metrics.current_task)
                    != current_task,
            "task attempted to re-acquire jj store lock for {store} while already holding it"
        );
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
            metrics.current_task = current_task;
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

    /// The operations queued on a store's lock, in service order.
    #[cfg(feature = "test-utils")]
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
