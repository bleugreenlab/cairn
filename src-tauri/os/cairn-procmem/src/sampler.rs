use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::membership::Membership;
use crate::{Census, CensusFn, CensusRow, ProcessIdentity};

pub type WatchId = u64;

/// Clocks captured together after one successful machine census.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TickClocks {
    pub monotonic_ms: u64,
    pub unix_ms: u64,
}

pub type Clock = Arc<dyn Fn() -> TickClocks + Send + Sync>;

pub fn monotonic_stamp() -> u64 {
    static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    u64::try_from(ORIGIN.get_or_init(Instant::now).elapsed().as_millis())
        .unwrap_or(u64::MAX - 1)
        .saturating_add(1)
}

fn clocks() -> TickClocks {
    TickClocks {
        monotonic_ms: monotonic_stamp(),
        unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    }
}

/// Readable RSS is a floor whenever incomplete is true.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeReading {
    pub resident_bytes: u64,
    /// Physical footprint summed over exactly the members `resident_bytes`
    /// sums, so the two are floors together. `None` when any of those members
    /// has no footprint reading; a reader then shows RSS, named as RSS.
    pub physical_footprint_bytes: Option<u64>,
    pub members_read: usize,
    pub members_total: usize,
    pub incomplete: bool,
    pub measured_at: TickClocks,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SamplingStatus {
    NotSampled,
    InitialLeaderUnverified,
    GoneOrReused,
    Unsupported,
    FailedSampling,
    Measured(TreeReading),
}

#[derive(Clone, Default)]
pub struct WatchOptions {
    pub storage: bool,
    /// Ask for members' physical footprint where reading it is not free
    /// (Linux's `smaps_rollup`); macOS reports it for every watch.
    pub footprint: bool,
    /// Optional monotonic deadline; expiry is serviced by the existing sampler.
    pub expires_at_ms: Option<u64>,
    #[cfg(windows)]
    pub tree_job: Option<Arc<crate::platform::TreeJob>>,
}

/// A watch's identity-fenced changes from this census. Metric data is borrowed
/// from the single batch census, not copied machine-wide for each watch.
pub struct WatchTick {
    pub id: WatchId,
    pub leader: ProcessIdentity,
    pub members: Vec<ProcessIdentity>,
    pub joined: Vec<ProcessIdentity>,
    pub retained_unreadable: Vec<ProcessIdentity>,
    pub departed: Vec<ProcessIdentity>,
    pub status: SamplingStatus,
    pub storage_requested: bool,
    /// Identity/enumeration uncertainty, independent of RSS-only instrument gaps.
    pub membership_incomplete: bool,
    pub unverified_candidates: Vec<u32>,
}

pub struct TickBatch<'a> {
    pub sequence: u64,
    pub clocks: TickClocks,
    pub watches: &'a [WatchTick],
    pub census: &'a Census,
}

impl TickBatch<'_> {
    pub fn member(&self, identity: ProcessIdentity) -> Option<&CensusRow> {
        self.census
            .row(identity.pid)
            .filter(|row| row.process.start == identity.start_time)
    }
}

/// Observer delivery and queued retirement are serialized on the sampler worker.
/// An observer may read caches, but MUST NOT register watches or wait for sampling
/// from its callback: that would wait for the delivery it is currently performing.
pub trait BatchObserver: Send + Sync {
    fn tick(&self, batch: &TickBatch<'_>);
    /// A failed census updates initial failure status only, never successful freshness.
    fn failed(&self, _clocks: TickClocks, _watches: &[(WatchId, SamplingStatus)]) {}
    fn retired(&self, _id: WatchId) {}
    fn idle(&self, _clocks: TickClocks) {}
}

thread_local! { static IN_OBSERVER: Cell<bool> = const { Cell::new(false) }; }

struct ObserverGuard;
impl ObserverGuard {
    fn enter() -> Self {
        IN_OBSERVER.with(|flag| assert!(!flag.replace(true), "recursive sampler observer"));
        Self
    }
}
impl Drop for ObserverGuard {
    fn drop(&mut self) {
        IN_OBSERVER.with(|flag| flag.set(false));
    }
}

fn assert_not_observer() {
    IN_OBSERVER.with(|flag| {
        assert!(
            !flag.get(),
            "sampler lifecycle waits are forbidden in an observer"
        )
    });
}

struct Registration {
    leader: ProcessIdentity,
    options: WatchOptions,
    membership: Membership,
    status: SamplingStatus,
    completed_censuses: u64,
}

struct Retirement {
    id: WatchId,
    idle: bool,
    clocks: TickClocks,
}

#[derive(Default)]
struct State {
    registrations: BTreeMap<WatchId, Registration>,
    // Removed from active caches immediately, retained for an in-flight census.
    retiring: BTreeMap<WatchId, Registration>,
    next_id: WatchId,
    running: bool,
    sequence: u64,
    idle_epoch: u64,
    pending_retirements: Vec<Retirement>,
    policy_pending: usize,
}

impl State {
    fn retirement(&mut self, id: WatchId, idle: bool, clocks: TickClocks) -> Retirement {
        self.policy_pending += 1;
        Retirement { id, idle, clocks }
    }
}

/// One implementation and one lazily running dedicated thread per host cadence.
///
/// Lock order is delivery -> state. The worker holds delivery across census and
/// observer processing, but never state across syscalls. Registrations take only
/// state and do not wake an active cadence. Drop takes only state and queues
/// retirement. The worker delivers any already-started census before retirement;
/// callers never wait for kernel reads or observer callbacks when dropping.
pub struct ProcessTreeSampler {
    state: Mutex<State>,
    delivery: Mutex<()>,
    woken: Condvar,
    completed: Condvar,
    source: CensusFn,
    clock: Clock,
    observer: Arc<dyn BatchObserver>,
    interval: Duration,
    automatic: bool,
    #[cfg(windows)]
    scoped_censuses: Mutex<BTreeMap<WatchId, Census>>,
    #[cfg(any(test, feature = "test-utils"))]
    fail_spawn: std::sync::atomic::AtomicBool,
}

impl ProcessTreeSampler {
    pub fn new(interval: Duration, observer: Arc<dyn BatchObserver>) -> Arc<Self> {
        Self::construct(
            interval,
            observer,
            Arc::new(crate::platform::census),
            Arc::new(clocks),
            true,
        )
    }

    fn construct(
        interval: Duration,
        observer: Arc<dyn BatchObserver>,
        source: CensusFn,
        clock: Clock,
        automatic: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            delivery: Mutex::new(()),
            woken: Condvar::new(),
            completed: Condvar::new(),
            source,
            clock,
            observer,
            interval,
            automatic,
            #[cfg(windows)]
            scoped_censuses: Mutex::new(BTreeMap::new()),
            #[cfg(any(test, feature = "test-utils"))]
            fail_spawn: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Deterministic source and clock injection, without consumer policy internals.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_source(
        interval: Duration,
        observer: Arc<dyn BatchObserver>,
        source: CensusFn,
        clock: Clock,
        automatic: bool,
    ) -> Arc<Self> {
        Self::construct(interval, observer, source, clock, automatic)
    }

    pub fn watch(
        self: &Arc<Self>,
        leader: ProcessIdentity,
        options: WatchOptions,
    ) -> io::Result<ProcessTreeWatch> {
        assert_not_observer();
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("process watch IDs exhausted"))?;
        let status = if crate::platform::MEMORY_SUPPORTED {
            SamplingStatus::NotSampled
        } else {
            SamplingStatus::Unsupported
        };
        state.registrations.insert(
            id,
            Registration {
                leader,
                options,
                membership: Membership::new(leader),
                status,
                completed_censuses: 0,
            },
        );
        if !state.running && self.automatic {
            state.running = true;
            let sampler = self.clone();
            #[cfg(any(test, feature = "test-utils"))]
            let injected_failure = self
                .fail_spawn
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            #[cfg(not(any(test, feature = "test-utils")))]
            let injected_failure = false;
            let spawned = if injected_failure {
                Err(io::Error::other(
                    "injected process sampler thread-start failure",
                ))
            } else {
                std::thread::Builder::new()
                    .name("process-tree-sampler".into())
                    .spawn(move || sampler.work())
                    .map(|_| ())
            };
            if let Err(error) = spawned {
                state.running = false;
                state.registrations.remove(&id);
                return Err(error);
            }
        }
        Ok(ProcessTreeWatch {
            sampler: self.clone(),
            id,
        })
    }

    fn work(self: Arc<Self>) {
        let mut census = Census::default();
        let mut next_walk: Option<Instant> = None;
        let mut walked_epoch = 0;
        loop {
            {
                let _delivery = self.delivery.lock().unwrap();
                self.flush_retirements();
            }
            let mut state = self.state.lock().unwrap();
            if state.registrations.is_empty() && state.pending_retirements.is_empty() {
                state.running = false;
                return;
            }
            if !state.pending_retirements.is_empty() {
                continue;
            }
            if !crate::platform::MEMORY_SUPPORTED {
                drop(self.woken.wait(state).unwrap());
                continue;
            }
            if state.idle_epoch == walked_epoch {
                if let Some(remaining) =
                    next_walk.and_then(|at| at.checked_duration_since(Instant::now()))
                {
                    let epoch = state.idle_epoch;
                    drop(
                        self.woken
                            .wait_timeout_while(state, remaining, |state| {
                                !state.registrations.is_empty()
                                    && state.pending_retirements.is_empty()
                                    && state.idle_epoch == epoch
                            })
                            .unwrap(),
                    );
                    continue;
                }
            }
            walked_epoch = state.idle_epoch;
            drop(state);
            self.sample(&mut census);
            next_walk = Instant::now().checked_add(self.interval);
        }
    }

    // Delivery is held. Lifecycle-only wakes do not move the post-walk deadline.
    fn flush_retirements(&self) {
        let events = {
            let mut state = self.state.lock().unwrap();
            let events = std::mem::take(&mut state.pending_retirements);
            for event in &events {
                state.retiring.remove(&event.id);
            }
            events
        };
        self.deliver_retirements(events);
    }

    fn deliver_retirements(&self, events: Vec<Retirement>) {
        if events.is_empty() {
            return;
        }
        {
            let _callback = ObserverGuard::enter();
            for event in &events {
                self.observer.retired(event.id);
                if event.idle {
                    self.observer.idle(event.clocks);
                }
            }
        }
        let mut state = self.state.lock().unwrap();
        state.policy_pending -= events.len();
        drop(state);
        self.completed.notify_all();
    }

    fn sample(&self, census: &mut Census) {
        assert_not_observer();
        let _delivery = self.delivery.lock().unwrap();
        self.flush_retirements();
        self.sample_active(census);
        self.flush_retirements();
    }

    // Delivery is held. Registrations captured before a syscall remain available
    // in retiring until their final policy delivery has completed.
    fn sample_active(&self, census: &mut Census) {
        self.expire((self.clock)());
        census.clear();
        let ids = {
            let state = self.state.lock().unwrap();
            for registration in state.registrations.values().filter(|r| r.options.storage) {
                // Include a new leader so its first read preserves executor's
                // existing storage baseline; newly discovered children get grace.
                census.storage_wanted.push(registration.leader.pid);
                census
                    .storage_wanted
                    .extend(registration.membership.members.keys().copied());
            }
            for registration in state.registrations.values().filter(|r| r.options.footprint) {
                census.footprint_wanted.push(registration.leader.pid);
                census
                    .footprint_wanted
                    .extend(registration.membership.members.keys().copied());
            }
            state.registrations.keys().copied().collect::<Vec<_>>()
        };
        if ids.is_empty() {
            return;
        }
        census.storage_wanted.sort_unstable();
        census.storage_wanted.dedup();
        census.footprint_wanted.sort_unstable();
        census.footprint_wanted.dedup();
        if !(self.source)(census) {
            let mut failed = Vec::new();
            let mut state = self.state.lock().unwrap();
            for id in ids {
                let State {
                    registrations,
                    retiring,
                    ..
                } = &mut *state;
                if let Some(registration) =
                    registrations.get_mut(&id).or_else(|| retiring.get_mut(&id))
                {
                    if matches!(
                        registration.status,
                        SamplingStatus::NotSampled
                            | SamplingStatus::InitialLeaderUnverified
                            | SamplingStatus::FailedSampling
                    ) {
                        registration.status = SamplingStatus::FailedSampling;
                    }
                    failed.push((id, registration.status.clone()));
                }
            }
            drop(state);
            let _callback = ObserverGuard::enter();
            self.observer.failed((self.clock)(), &failed);
            return;
        }
        #[cfg(windows)]
        let scoped = {
            let jobs: Vec<_> = {
                let state = self.state.lock().unwrap();
                ids.iter()
                    .filter_map(|id| {
                        state
                            .registrations
                            .get(id)
                            .or_else(|| state.retiring.get(id))
                            .and_then(|r| r.options.tree_job.clone())
                            .map(|job| (*id, job))
                    })
                    .collect()
            };
            let mut buffers = self.scoped_censuses.lock().unwrap();
            buffers.retain(|id, _| jobs.iter().any(|(active, _)| active == id));
            for (id, job) in jobs {
                let out = buffers.entry(id).or_default();
                out.clear();
                if !job.census(out) {
                    out.enumeration_incomplete = true;
                }
                census.rows.extend(out.rows.iter().copied());
                census.unreadable.extend(out.unreadable.iter().copied());
            }
            buffers
        };
        // A scoped job can observe a newer lifetime than the machine snapshot.
        // Keep the newest verified lifetime, never a stale PID-only attachment.
        census
            .rows
            .sort_by_key(|row| (row.process.pid, std::cmp::Reverse(row.process.start)));
        census.rows.dedup_by_key(|row| row.process.pid);
        census.unreadable.sort_unstable();
        let now = (self.clock)();
        let (sequence, ticks) = {
            let mut state = self.state.lock().unwrap();
            let sequence = state.sequence.saturating_add(1);
            let mut ticks = Vec::with_capacity(ids.len());
            for id in ids {
                let State {
                    registrations,
                    retiring,
                    ..
                } = &mut *state;
                let Some(registration) =
                    registrations.get_mut(&id).or_else(|| retiring.get_mut(&id))
                else {
                    continue;
                };
                #[cfg(windows)]
                let scoped_incomplete = scoped
                    .get(&id)
                    .is_some_and(|c| c.enumeration_incomplete || !c.unreadable.is_empty());
                #[cfg(not(windows))]
                let scoped_incomplete = false;
                #[cfg(windows)]
                let changes = registration.membership.fold_with_scope(
                    census,
                    &scoped
                        .get(&id)
                        .map(|c| {
                            c.rows
                                .iter()
                                .map(|row| row.process.identity())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                    scoped_incomplete,
                );
                #[cfg(not(windows))]
                let changes = registration.membership.fold(census);
                let members: Vec<_> = registration
                    .membership
                    .members
                    .iter()
                    .map(|(pid, start)| ProcessIdentity {
                        pid: *pid,
                        start_time: *start,
                    })
                    .collect();
                let unverified_candidates = registration.membership.unverified_candidates(census);
                let membership_incomplete = census.enumeration_incomplete
                    || scoped_incomplete
                    || !unverified_candidates.is_empty()
                    || !changes.unreadable.is_empty();
                let status = if !crate::platform::MEMORY_SUPPORTED {
                    SamplingStatus::Unsupported
                } else if !registration.membership.verified() {
                    if census.row(registration.leader.pid).is_some() {
                        SamplingStatus::GoneOrReused
                    } else if census
                        .unreadable
                        .binary_search(&registration.leader.pid)
                        .is_ok()
                        || census.enumeration_incomplete
                    {
                        SamplingStatus::InitialLeaderUnverified
                    } else {
                        SamplingStatus::GoneOrReused
                    }
                } else if members.is_empty() {
                    SamplingStatus::GoneOrReused
                } else {
                    let mut reading = TreeReading {
                        resident_bytes: 0,
                        physical_footprint_bytes: None,
                        members_read: 0,
                        members_total: members.len(),
                        incomplete: membership_incomplete,
                        measured_at: now,
                    };
                    let mut footprint = Some(0_u64);
                    for member in &members {
                        let row = census.row(member.pid);
                        match row.and_then(|row| row.resident_bytes) {
                            Some(bytes) => {
                                reading.resident_bytes =
                                    reading.resident_bytes.saturating_add(bytes);
                                reading.members_read += 1;
                                footprint = footprint
                                    .zip(row.and_then(|row| row.physical_footprint_bytes))
                                    .map(|(sum, bytes)| sum.saturating_add(bytes));
                            }
                            None => reading.incomplete = true,
                        }
                    }
                    reading.physical_footprint_bytes =
                        footprint.filter(|_| reading.members_read > 0);
                    SamplingStatus::Measured(reading)
                };
                registration.status = status.clone();
                ticks.push(WatchTick {
                    id,
                    leader: registration.leader,
                    members,
                    joined: changes.joined,
                    retained_unreadable: changes.unreadable,
                    departed: changes.departed,
                    status,
                    storage_requested: registration.options.storage,
                    membership_incomplete,
                    unverified_candidates,
                });
            }
            (sequence, ticks)
        };
        {
            let _callback = ObserverGuard::enter();
            self.observer.tick(&TickBatch {
                sequence,
                clocks: now,
                watches: &ticks,
                census,
            });
        }
        // Settle means policy completed, not merely that kernel calls returned.
        let mut state = self.state.lock().unwrap();
        state.sequence = sequence;
        for tick in &ticks {
            if let Some(registration) = state.registrations.get_mut(&tick.id) {
                registration.completed_censuses = registration.completed_censuses.saturating_add(1);
            }
        }
        drop(state);
        self.completed.notify_all();
    }

    // Called with delivery held, including on failed-census ticks. Handles may
    // outlive their expiry, but cannot resurrect registrations or notify twice.
    fn expire(&self, clocks: TickClocks) {
        let events = {
            let mut state = self.state.lock().unwrap();
            let expired: Vec<_> = state
                .registrations
                .iter()
                .filter_map(|(id, r)| {
                    r.options
                        .expires_at_ms
                        .filter(|deadline| clocks.monotonic_ms >= *deadline)
                        .map(|_| *id)
                })
                .collect();
            for id in &expired {
                state.registrations.remove(id);
            }
            let empty = !expired.is_empty() && state.registrations.is_empty();
            if empty {
                state.idle_epoch = state.idle_epoch.wrapping_add(1);
            }
            let last = expired.last().copied();
            expired
                .into_iter()
                .map(|id| state.retirement(id, empty && Some(id) == last, clocks))
                .collect()
        };
        self.deliver_retirements(events);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_next_spawn(&self) {
        self.fail_spawn
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn sample_once(&self, census: &mut Census) {
        self.sample(census);
    }

    /// Drive queued lifecycle callbacks without taking a new census.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn flush_retirements_once(&self) {
        assert_not_observer();
        let _delivery = self.delivery.lock().unwrap();
        self.flush_retirements();
    }

    /// Cached union across watches of this exact anchor. No census on reads.
    pub fn tree_members(&self, leader: ProcessIdentity) -> Vec<ProcessIdentity> {
        let state = self.state.lock().unwrap();
        let mut members = BTreeMap::new();
        for r in state.registrations.values().filter(|r| r.leader == leader) {
            members.extend(
                r.membership
                    .members
                    .iter()
                    .map(|(pid, start)| (*pid, *start)),
            );
        }
        members
            .into_iter()
            .map(|(pid, start_time)| ProcessIdentity { pid, start_time })
            .collect()
    }

    pub fn sequence(&self) -> u64 {
        self.state.lock().unwrap().sequence
    }

    pub fn settle(&self, count: u64, timeout: Duration) {
        assert_not_observer();
        let state = self.state.lock().unwrap();
        let target = state.sequence.saturating_add(count);
        drop(
            self.completed
                .wait_timeout_while(state, timeout, |state| {
                    (state.sequence < target && !state.registrations.is_empty())
                        || state.policy_pending > 0
                })
                .unwrap(),
        );
    }

    fn settle_watch(&self, id: WatchId, count: u64, timeout: Duration) {
        assert_not_observer();
        let state = self.state.lock().unwrap();
        let target = state
            .registrations
            .get(&id)
            .map(|registration| registration.completed_censuses.saturating_add(count));
        drop(
            self.completed
                .wait_timeout_while(state, timeout, |state| {
                    target.is_some_and(|target| {
                        state
                            .registrations
                            .get(&id)
                            .is_some_and(|registration| registration.completed_censuses < target)
                    }) || state.policy_pending > 0
                })
                .unwrap(),
        );
    }

    pub fn watched(&self) -> usize {
        self.state.lock().unwrap().registrations.len()
    }
}

pub struct ProcessTreeWatch {
    sampler: Arc<ProcessTreeSampler>,
    id: WatchId,
}

impl ProcessTreeWatch {
    pub fn id(&self) -> WatchId {
        self.id
    }
    pub fn latest(&self) -> SamplingStatus {
        self.sampler
            .state
            .lock()
            .unwrap()
            .registrations
            .get(&self.id)
            .map(|r| r.status.clone())
            .unwrap_or(SamplingStatus::GoneOrReused)
    }
    pub fn members(&self) -> Vec<ProcessIdentity> {
        self.sampler
            .state
            .lock()
            .unwrap()
            .registrations
            .get(&self.id)
            .map(|r| {
                r.membership
                    .members
                    .iter()
                    .map(|(pid, start)| ProcessIdentity {
                        pid: *pid,
                        start_time: *start,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn settle(&self, count: u64, timeout: Duration) {
        self.sampler.settle_watch(self.id, count, timeout);
    }
}

impl Drop for ProcessTreeWatch {
    fn drop(&mut self) {
        let mut state = self.sampler.state.lock().unwrap();
        let Some(registration) = state.registrations.remove(&self.id) else {
            return;
        };
        state.retiring.insert(self.id, registration);
        let empty = state.registrations.is_empty();
        if empty {
            state.idle_epoch = state.idle_epoch.wrapping_add(1);
        }
        let event = state.retirement(self.id, empty, (self.sampler.clock)());
        state.pending_retirements.push(event);
        drop(state);
        self.sampler.woken.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KernelProcess, StartKey};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct Observer {
        ticks: Mutex<Vec<(u64, Vec<WatchId>)>>,
        endings: Mutex<Vec<WatchId>>,
        idle_count: AtomicU64,
    }
    impl BatchObserver for Observer {
        fn tick(&self, batch: &TickBatch<'_>) {
            self.ticks
                .lock()
                .unwrap()
                .push((batch.sequence, batch.watches.iter().map(|w| w.id).collect()));
        }
        fn retired(&self, id: WatchId) {
            self.endings.lock().unwrap().push(id);
        }
        fn idle(&self, _: TickClocks) {
            self.idle_count.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn identity(pid: u32) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            start_time: StartKey(1, 0),
        }
    }
    fn row(pid: u32, parent_pid: u32, rss: Option<u64>) -> CensusRow {
        CensusRow {
            process: KernelProcess {
                pid,
                parent_pid,
                process_group_id: pid,
                session_id: None,
                start: StartKey(1, 0),
            },
            resident_bytes: rss,
            physical_footprint_bytes: None,
            cpu_ms: Some(10),
            storage_io_bytes: Some(20),
            uninterruptible: false,
            scheduling: None,
        }
    }
    type Script = Arc<Mutex<Option<(Vec<CensusRow>, Vec<u32>, bool)>>>;
    fn manual(script: &Script, observer: Arc<Observer>) -> Arc<ProcessTreeSampler> {
        let script = script.clone();
        ProcessTreeSampler::with_source(
            Duration::from_secs(1),
            observer,
            Arc::new(move |c| {
                let script = script.lock().unwrap();
                let Some((rows, unreadable, incomplete)) = script.as_ref() else {
                    return false;
                };
                c.rows.extend(rows.iter().copied());
                c.unreadable.extend(unreadable);
                c.enumeration_incomplete = *incomplete;
                true
            }),
            Arc::new(|| TickClocks {
                monotonic_ms: 42,
                unix_ms: 1000,
            }),
            false,
        )
    }
    #[test]
    fn memory_completeness_does_not_erase_cpu_or_storage_instruments() {
        let script = Arc::new(Mutex::new(Some((
            vec![row(10, 1, Some(30)), row(11, 10, None)],
            vec![],
            false,
        ))));
        let observer = Arc::new(Observer::default());
        let sampler = manual(&script, observer.clone());
        let watch = sampler
            .watch(
                identity(10),
                WatchOptions {
                    storage: true,
                    ..WatchOptions::default()
                },
            )
            .unwrap();
        assert_eq!(watch.latest(), SamplingStatus::NotSampled);
        assert!(watch.members().is_empty());
        sampler.sample_once(&mut Census::default());
        let SamplingStatus::Measured(reading) = watch.latest() else {
            panic!("not measured")
        };
        assert_eq!(
            reading,
            TreeReading {
                resident_bytes: 30,
                physical_footprint_bytes: None,
                members_read: 1,
                members_total: 2,
                incomplete: true,
                measured_at: TickClocks {
                    monotonic_ms: 42,
                    unix_ms: 1000
                }
            }
        );
        assert_eq!(watch.members().len(), 2);
        assert_eq!(observer.ticks.lock().unwrap()[0].0, sampler.sequence());
    }
    #[test]
    fn footprint_sums_over_exactly_the_members_rss_sums() {
        let with = |mut row: CensusRow, footprint: Option<u64>| {
            row.physical_footprint_bytes = footprint;
            row
        };
        let script: Script = Arc::new(Mutex::new(Some((
            vec![
                with(row(10, 1, Some(300)), Some(70)),
                with(row(11, 10, Some(200)), Some(30)),
            ],
            vec![],
            false,
        ))));
        let sampler = manual(&script, Arc::new(Observer::default()));
        let watch = sampler
            .watch(
                identity(10),
                WatchOptions {
                    footprint: true,
                    ..WatchOptions::default()
                },
            )
            .unwrap();
        let tick = || {
            let mut census = Census::default();
            sampler.sample_once(&mut census);
            let SamplingStatus::Measured(reading) = watch.latest() else {
                panic!("not measured")
            };
            (
                reading.resident_bytes,
                reading.physical_footprint_bytes,
                census.footprint_wanted,
            )
        };
        // The leader is wanted before its tree is known; members after.
        assert_eq!(tick(), (500, Some(100), vec![10]));
        assert_eq!(tick(), (500, Some(100), vec![10, 11]));
        script.lock().unwrap().as_mut().unwrap().0[1].physical_footprint_bytes = None;
        assert_eq!(
            tick(),
            (500, None, vec![10, 11]),
            "a member without footprint leaves the tree on RSS"
        );
        {
            let mut guard = script.lock().unwrap();
            let child = &mut guard.as_mut().unwrap().0[1];
            child.physical_footprint_bytes = Some(30);
            child.resident_bytes = None;
        }
        assert_eq!(
            tick(),
            (300, Some(70), vec![10, 11]),
            "footprint floors over the same members RSS floors over"
        );
    }
    #[test]
    fn failed_census_ages_previous_success_and_recovery_replaces_it() {
        let script = Arc::new(Mutex::new(None));
        let sampler = manual(&script, Arc::new(Observer::default()));
        let watch = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        sampler.sample_once(&mut Census::default());
        assert_eq!(watch.latest(), SamplingStatus::FailedSampling);
        *script.lock().unwrap() = Some((vec![row(10, 1, Some(0))], vec![], false));
        sampler.sample_once(&mut Census::default());
        let before = watch.latest();
        assert!(
            matches!(&before, SamplingStatus::Measured(r) if !r.incomplete && r.resident_bytes == 0)
        );
        *script.lock().unwrap() = None;
        sampler.sample_once(&mut Census::default());
        assert_eq!(watch.latest(), before);
        assert_eq!(sampler.sequence(), 1);
        *script.lock().unwrap() = Some((vec![row(10, 1, Some(100))], vec![], false));
        sampler.sample_once(&mut Census::default());
        assert!(matches!(watch.latest(), SamplingStatus::Measured(r) if r.resident_bytes == 100));
        assert_eq!(sampler.sequence(), 2);
    }
    #[test]
    fn retained_unreadable_identity_is_partial_not_departed() {
        let script = Arc::new(Mutex::new(Some((
            vec![row(10, 1, Some(30)), row(11, 10, Some(50))],
            vec![],
            false,
        ))));
        let sampler = manual(&script, Arc::new(Observer::default()));
        let watch = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        sampler.sample_once(&mut Census::default());
        *script.lock().unwrap() = Some((vec![row(10, 1, Some(30))], vec![11], false));
        sampler.sample_once(&mut Census::default());
        assert!(
            matches!(watch.latest(), SamplingStatus::Measured(r) if r.incomplete && r.members_total == 2 && r.members_read == 1)
        );
        assert_eq!(watch.members(), vec![identity(10), identity(11)]);
    }
    #[test]
    fn overlapping_watches_keep_independent_membership_and_retirement() {
        let script = Arc::new(Mutex::new(Some((
            vec![row(10, 1, Some(30)), row(11, 10, Some(50))],
            vec![],
            false,
        ))));
        let observer = Arc::new(Observer::default());
        let sampler = manual(&script, observer.clone());
        let first = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        let second = sampler
            .watch(identity(11), WatchOptions::default())
            .unwrap();
        sampler.sample_once(&mut Census::default());
        assert_eq!(first.members().len(), 2);
        assert_eq!(second.members().len(), 1);
        let id = first.id();
        drop(first);
        sampler.flush_retirements_once();
        assert_eq!(*observer.endings.lock().unwrap(), vec![id]);
        assert_eq!(sampler.watched(), 1);
        assert_eq!(observer.idle_count.load(Ordering::SeqCst), 0);
        drop(second);
        sampler.flush_retirements_once();
        assert_eq!(observer.idle_count.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn registration_and_cache_reads_never_census_and_ids_are_not_reused() {
        let calls = Arc::new(AtomicU64::new(0));
        let count = calls.clone();
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(1),
            Arc::new(Observer::default()),
            Arc::new(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                true
            }),
            Arc::new(TickClocks::default),
            false,
        );
        let watch = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        let id = watch.id();
        for _ in 0..100 {
            watch.latest();
            watch.members();
            sampler.tree_members(identity(10));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(watch);
        let replacement = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        assert!(replacement.id() > id);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn expiry_retires_without_sampling_and_late_handle_drop_is_inert() {
        let now = Arc::new(AtomicU64::new(10));
        let clock_now = now.clone();
        let calls = Arc::new(AtomicU64::new(0));
        let called = calls.clone();
        let observer = Arc::new(Observer::default());
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_millis(100),
            observer.clone(),
            Arc::new(move |_| {
                called.fetch_add(1, Ordering::SeqCst);
                false
            }),
            Arc::new(move || TickClocks {
                monotonic_ms: clock_now.load(Ordering::SeqCst),
                unix_ms: 1000,
            }),
            false,
        );
        let watch = sampler
            .watch(
                identity(10),
                WatchOptions {
                    expires_at_ms: Some(20),
                    ..WatchOptions::default()
                },
            )
            .unwrap();
        let id = watch.id();
        sampler.sample_once(&mut Census::default());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        now.store(20, Ordering::SeqCst);
        sampler.sample_once(&mut Census::default());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(sampler.watched(), 0);
        assert_eq!(*observer.endings.lock().unwrap(), vec![id]);
        drop(watch);
        assert_eq!(*observer.endings.lock().unwrap(), vec![id]);
        assert_eq!(observer.idle_count.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn thread_start_failure_is_an_error_and_leaves_no_active_watch() {
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(1),
            Arc::new(Observer::default()),
            Arc::new(|_| true),
            Arc::new(TickClocks::default),
            true,
        );
        sampler.fail_next_spawn();
        assert!(sampler
            .watch(identity(10), WatchOptions::default())
            .is_err());
        assert_eq!(sampler.watched(), 0);
        assert!(!sampler.state.lock().unwrap().running);
    }
    #[test]
    fn storage_interest_is_filtered_and_joiners_have_a_tick_of_grace() {
        let wanted = Arc::new(Mutex::new(Vec::new()));
        let observed = wanted.clone();
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(1),
            Arc::new(Observer::default()),
            Arc::new(move |c| {
                *observed.lock().unwrap() = c.storage_wanted.clone();
                c.rows.extend([
                    row(10, 1, Some(1)),
                    row(11, 10, Some(1)),
                    row(20, 1, Some(1)),
                ]);
                true
            }),
            Arc::new(TickClocks::default),
            false,
        );
        let _rss = sampler
            .watch(identity(20), WatchOptions::default())
            .unwrap();
        let _storage = sampler
            .watch(
                identity(10),
                WatchOptions {
                    storage: true,
                    ..WatchOptions::default()
                },
            )
            .unwrap();
        let mut census = Census::default();
        sampler.sample_once(&mut census);
        assert_eq!(*wanted.lock().unwrap(), vec![10]);
        sampler.sample_once(&mut census);
        assert_eq!(*wanted.lock().unwrap(), vec![10, 11]);
    }
    #[test]
    fn dropping_during_a_census_returns_before_final_policy_delivery() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let observer = Arc::new(Observer::default());
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(100),
            observer.clone(),
            Arc::new(move |c| {
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                c.rows.push(row(10, 1, Some(25)));
                true
            }),
            Arc::new(TickClocks::default),
            false,
        );
        let watch = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        let id = watch.id();
        let sampling = sampler.clone();
        let worker = std::thread::spawn(move || sampling.sample_once(&mut Census::default()));
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let replacement = sampler
            .watch(identity(20), WatchOptions::default())
            .unwrap();
        let (retired_tx, retired_rx) = std::sync::mpsc::channel();
        let retire = std::thread::spawn(move || {
            drop(watch);
            retired_tx.send(()).unwrap();
        });
        let returned = retired_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let active = sampler.watched();
        // Release and join before asserting so a failing implementation cannot
        // strand either the census or the thread standing in for a runtime worker.
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        retire.join().unwrap();
        assert!(returned, "dropping a watch waited for a kernel census");
        assert_eq!(active, 1, "retiring watches must leave the active cache");
        assert_eq!(*observer.ticks.lock().unwrap(), vec![(1, vec![id])]);
        assert_eq!(*observer.endings.lock().unwrap(), vec![id]);
        assert_eq!(replacement.latest(), SamplingStatus::NotSampled);
    }
    #[test]
    fn active_registration_preserves_post_walk_wait_and_worker_retires_after_last_drop() {
        let (tick_tx, tick_rx) = std::sync::mpsc::channel();
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_millis(100),
            Arc::new(Observer::default()),
            Arc::new(move |c| {
                tick_tx.send(std::thread::current().id()).unwrap();
                c.rows.push(row(10, 1, Some(1)));
                true
            }),
            Arc::new(TickClocks::default),
            true,
        );
        let first = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        let thread = tick_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = sampler
            .watch(identity(20), WatchOptions::default())
            .unwrap();
        assert!(tick_rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert_eq!(
            tick_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            thread
        );
        drop(first);
        drop(second);
        assert!(tick_rx.recv_timeout(Duration::from_millis(150)).is_err());
        assert_eq!(sampler.watched(), 0);
        let _new = sampler
            .watch(identity(10), WatchOptions::default())
            .unwrap();
        tick_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    use crate::{KernelProcess, StartKey};

    struct BlockingLifecycle {
        events: std::sync::mpsc::Sender<(u8, std::thread::ThreadId)>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        block_phase: u8,
    }
    impl BlockingLifecycle {
        fn event(&self, phase: u8) {
            self.events
                .send((phase, std::thread::current().id()))
                .unwrap();
            if phase == self.block_phase {
                self.release.lock().unwrap().recv().unwrap();
            }
        }
    }
    impl BatchObserver for BlockingLifecycle {
        fn tick(&self, _: &TickBatch<'_>) {
            self.event(0);
        }
        fn retired(&self, _: WatchId) {
            self.event(1);
        }
        fn idle(&self, _: TickClocks) {
            self.event(2);
        }
    }

    fn source(census: &mut Census) -> bool {
        census.rows.push(CensusRow {
            process: KernelProcess {
                pid: 10,
                parent_pid: 1,
                process_group_id: 10,
                session_id: Some(10),
                start: StartKey(1, 0),
            },
            resident_bytes: Some(1),
            physical_footprint_bytes: None,
            cpu_ms: Some(1),
            storage_io_bytes: None,
            uninterruptible: false,
            scheduling: None,
        });
        true
    }
    fn anchor() -> ProcessIdentity {
        ProcessIdentity {
            pid: 10,
            start_time: StartKey(1, 0),
        }
    }

    #[test]
    fn settle_waits_but_drop_returns_before_retired_and_idle_policy_including_expiry() {
        for expiry in [false, true] {
            for phase in [1, 2] {
                let (events_tx, events_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let observer = Arc::new(BlockingLifecycle {
                    events: events_tx,
                    release: Mutex::new(release_rx),
                    block_phase: phase,
                });
                let now = Arc::new(std::sync::atomic::AtomicU64::new(10));
                let clock = now.clone();
                let sampler = ProcessTreeSampler::with_source(
                    Duration::from_millis(20),
                    observer,
                    Arc::new(source),
                    Arc::new(move || TickClocks {
                        monotonic_ms: clock.load(std::sync::atomic::Ordering::SeqCst),
                        unix_ms: 1_000,
                    }),
                    true,
                );
                let watch = sampler
                    .watch(
                        anchor(),
                        WatchOptions {
                            expires_at_ms: expiry.then_some(20),
                            ..WatchOptions::default()
                        },
                    )
                    .unwrap();
                let (kind, worker) = events_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(kind, 0);
                let mut handle = Some(watch);
                let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
                let retiring = if expiry {
                    now.store(20, std::sync::atomic::Ordering::SeqCst);
                    None
                } else {
                    let watch = handle.take().unwrap();
                    Some(std::thread::spawn(move || {
                        drop(watch);
                        dropped_tx.send(()).unwrap();
                    }))
                };
                loop {
                    let (kind, thread) = events_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    assert_eq!(thread, worker);
                    if kind == phase {
                        break;
                    }
                }
                if !expiry {
                    dropped_rx
                        .recv_timeout(Duration::from_secs(1))
                        .expect("drop waited for policy");
                }
                let (settled_tx, settled_rx) = std::sync::mpsc::channel();
                let waiting = sampler.clone();
                let waiter = std::thread::spawn(move || {
                    waiting.settle(1, Duration::from_secs(5));
                    settled_tx.send(()).unwrap();
                });
                assert!(settled_rx.recv_timeout(Duration::from_millis(20)).is_err());
                // Cache reads remain available while lifecycle policy is blocked.
                assert!(sampler.tree_members(anchor()).is_empty());
                release_tx.send(()).unwrap();
                settled_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                waiter.join().unwrap();
                if let Some(retiring) = retiring {
                    retiring.join().unwrap();
                }
                if phase == 1 {
                    let (kind, thread) = events_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    assert_eq!((kind, thread), (2, worker));
                }
                drop(handle);
                assert_eq!(sampler.watched(), 0);
            }
        }
    }

    #[test]
    fn last_drop_then_registration_is_prompt_without_resurrecting_the_retired_watch() {
        let (events_tx, events_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let observer = Arc::new(BlockingLifecycle {
            events: events_tx,
            release: Mutex::new(release_rx),
            block_phase: 1,
        });
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(60),
            observer,
            Arc::new(source),
            Arc::new(clocks),
            true,
        );
        let first = sampler.watch(anchor(), WatchOptions::default()).unwrap();
        let first_id = first.id();
        let (_, worker) = events_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let retiring = std::thread::spawn(move || drop(first));
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            (1, worker)
        );
        let replacement = sampler.watch(anchor(), WatchOptions::default()).unwrap();
        assert!(replacement.id() > first_id);
        assert_eq!(replacement.latest(), SamplingStatus::NotSampled);
        release_tx.send(()).unwrap();
        retiring.join().unwrap();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            (2, worker)
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            (0, worker)
        );
        assert_eq!(sampler.watched(), 1);
        assert!(matches!(replacement.latest(), SamplingStatus::Measured(_)));
        release_tx.send(()).unwrap();
        drop(replacement);
        sampler.settle(0, Duration::from_secs(5));
    }
}

#[cfg(test)]
mod parity_tests {
    use super::*;
    use crate::{test_support::parity_census_steps, StartKey};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Instruments(AtomicU64);
    impl BatchObserver for Instruments {
        fn tick(&self, batch: &TickBatch<'_>) {
            for watch in batch.watches {
                let Some(child) = watch
                    .members
                    .iter()
                    .find(|id| id.pid == 11)
                    .and_then(|id| batch.member(*id))
                else {
                    continue;
                };
                let SamplingStatus::Measured(memory) = &watch.status else {
                    continue;
                };
                if child.resident_bytes.is_none() {
                    assert_eq!(child.cpu_ms, Some(40));
                    assert_eq!(child.storage_io_bytes, Some(400));
                    assert!(memory.incomplete);
                    assert!(!watch.membership_incomplete);
                    self.0.fetch_or(1, Ordering::SeqCst);
                } else if child.cpu_ms.is_none() {
                    assert!(!memory.incomplete);
                    self.0.fetch_or(2, Ordering::SeqCst);
                } else if child.storage_io_bytes.is_none() {
                    assert!(!memory.incomplete);
                    self.0.fetch_or(4, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn shared_consumer_corpus_pins_membership_floors_and_independent_instruments() {
        let steps = Arc::new(parity_census_steps());
        let index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source_steps = steps.clone();
        let source_index = index.clone();
        let clock_steps = steps.clone();
        let clock_index = index.clone();
        let observer = Arc::new(Instruments(AtomicU64::new(0)));
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(5),
            observer.clone(),
            Arc::new(move |census| {
                source_steps[source_index.load(Ordering::SeqCst)].census(census)
            }),
            Arc::new(move || clock_steps[clock_index.load(Ordering::SeqCst)].clocks),
            false,
        );
        let root = sampler
            .watch(
                ProcessIdentity {
                    pid: 10,
                    start_time: StartKey(1, 0),
                },
                WatchOptions {
                    storage: true,
                    ..WatchOptions::default()
                },
            )
            .unwrap();
        let child = sampler
            .watch(
                ProcessIdentity {
                    pid: 11,
                    start_time: StartKey(2, 0),
                },
                WatchOptions::default(),
            )
            .unwrap();
        let mut census = Census::default();
        for (i, step) in steps.iter().enumerate() {
            index.store(i, Ordering::SeqCst);
            let before_sequence = sampler.sequence();
            let before_status = root.latest();
            sampler.sample_once(&mut census);
            assert_eq!(
                root.members().iter().map(|id| id.pid).collect::<Vec<_>>(),
                step.root_members,
                "{}",
                step.name
            );
            match (step.root_memory, root.latest()) {
                (Some(expected), SamplingStatus::Measured(reading)) => {
                    assert_eq!(
                        (
                            reading.resident_bytes,
                            reading.members_read,
                            reading.members_total,
                            reading.incomplete
                        ),
                        expected,
                        "{}",
                        step.name
                    );
                }
                (None, SamplingStatus::GoneOrReused) => {}
                (_, actual) => panic!("{} unexpected status {actual:?}", step.name),
            }
            if !step.success {
                assert_eq!(sampler.sequence(), before_sequence);
                assert_eq!(root.latest(), before_status);
            }
            assert!(
                child.members().iter().all(|id| id.pid == 11),
                "overlapping child watch must not inherit its parent's scope"
            );
        }
        assert_eq!(observer.0.load(Ordering::SeqCst), 7);
    }
}

#[cfg(test)]
mod active_retirement_tests {
    use super::*;
    use crate::StartKey;

    struct Events(std::sync::mpsc::Sender<(&'static str, std::thread::ThreadId)>);
    impl BatchObserver for Events {
        fn tick(&self, _: &TickBatch<'_>) {
            self.0.send(("tick", std::thread::current().id())).unwrap();
        }
        fn retired(&self, _: WatchId) {
            self.0
                .send(("retired", std::thread::current().id()))
                .unwrap();
        }
        fn idle(&self, _: TickClocks) {
            self.0.send(("idle", std::thread::current().id())).unwrap();
        }
    }
    #[test]
    fn non_last_retirement_delivers_promptly_without_an_extra_census() {
        let (tx, rx) = std::sync::mpsc::channel();
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(60),
            Arc::new(Events(tx)),
            Arc::new(|_| true),
            Arc::new(clocks),
            true,
        );
        let anchor = ProcessIdentity {
            pid: 10,
            start_time: StartKey(1, 0),
        };
        let first = sampler.watch(anchor, WatchOptions::default()).unwrap();
        let (kind, worker) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(kind, "tick");
        let second = sampler.watch(anchor, WatchOptions::default()).unwrap();
        drop(first);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            ("retired", worker)
        );
        assert!(rx.recv_timeout(Duration::from_millis(30)).is_err());
        assert_eq!(sampler.sequence(), 1);
        assert_eq!(second.latest(), SamplingStatus::NotSampled);
        drop(second);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            ("retired", worker)
        );
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            ("idle", worker)
        );
        assert_eq!(sampler.sequence(), 1);
    }
}

#[cfg(test)]
mod watch_settle_tests {
    use super::*;
    use crate::{KernelProcess, StartKey};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Observer;
    impl BatchObserver for Observer {
        fn tick(&self, _: &TickBatch<'_>) {}
    }

    #[test]
    fn registering_during_a_census_waits_for_policy_that_includes_this_watch() {
        let (walking_tx, walking_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let calls = AtomicUsize::new(0);
        let sampler = ProcessTreeSampler::with_source(
            Duration::from_secs(60),
            Arc::new(Observer),
            Arc::new(move |census| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    walking_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
                for pid in [10, 20] {
                    census.rows.push(CensusRow {
                        process: KernelProcess {
                            pid,
                            parent_pid: 1,
                            process_group_id: pid,
                            session_id: None,
                            start: StartKey(1, 0),
                        },
                        resident_bytes: Some(1),
                        physical_footprint_bytes: None,
                        cpu_ms: Some(1),
                        storage_io_bytes: None,
                        uninterruptible: false,
                        scheduling: None,
                    });
                }
                true
            }),
            Arc::new(clocks),
            false,
        );
        let first = sampler
            .watch(
                ProcessIdentity {
                    pid: 10,
                    start_time: StartKey(1, 0),
                },
                WatchOptions::default(),
            )
            .unwrap();
        let walking = sampler.clone();
        let census_thread = std::thread::spawn(move || walking.sample_once(&mut Census::default()));
        walking_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let later = Arc::new(
            sampler
                .watch(
                    ProcessIdentity {
                        pid: 20,
                        start_time: StartKey(1, 0),
                    },
                    WatchOptions::default(),
                )
                .unwrap(),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiting = later.clone();
        let waiter = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            waiting.settle(1, Duration::from_secs(5));
            done_tx.send(()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_tx.send(()).unwrap();
        census_thread.join().unwrap();
        assert_eq!(later.latest(), SamplingStatus::NotSampled);
        assert!(
            done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "an unrelated in-flight tick must not complete a newly registered watch's settle"
        );
        sampler.sample_once(&mut Census::default());
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        waiter.join().unwrap();
        assert!(matches!(later.latest(), SamplingStatus::Measured(_)));
        drop(later);
        drop(first);
    }
}
