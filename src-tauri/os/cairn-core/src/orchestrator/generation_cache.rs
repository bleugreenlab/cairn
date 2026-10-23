//! Generation-fenced, single-flight materialization of an expensive projection.
//!
//! Every whole-project or whole-remote projection in the runner has the same
//! shape of problem: it costs the same to recompute no matter how little moved,
//! and it is read by mounted frontend queries that a burst of unrelated
//! `db-change` traffic can drive far faster than a person can read the result.
//! Caching the value alone is not enough, because the interesting failure is a
//! reader whose expensive computation finishes *after* an invalidation and then
//! publishes a value that was already wrong when it landed.
//!
//! The generation is the correctness key. It lives beside the single-flight
//! cell, so:
//!
//! - concurrent misses for one key at one generation share exactly one
//!   computation (`coalesced`);
//! - a repeat read at an unchanged generation performs no work at all (`hit`);
//! - a computation that spanned an invalidation is rejected and retried against
//!   the new generation (`invalidated_during_compute`) rather than published.
//!
//! A cache built this way is only as correct as its invalidation set: every
//! durable mutation that can change the projection must advance the generation,
//! and nothing else should. Reaching for a broad "invalidate on any change"
//! rule reintroduces exactly the amplification this exists to remove.

use crate::jobs::queries::{IssueStatusSnapshot, ThreadStatusSnapshot};
use crate::services::EventEmitter;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) type ThreadStatusCache = GenerationCache<Arc<ThreadStatusSnapshot>>;
pub(crate) type IssueStatusCache = GenerationCache<Arc<IssueStatusSnapshot>>;

/// How long a published thread rollup may keep being served after a TRANSCRIPT
/// EVENT lands.
///
/// Scoping removes the writes that cannot move a thread row, but not the ones
/// that matter most: an agent running IN a thread writes its transcript many
/// times a second, and that is exactly when someone is watching the row. Those
/// events are three quarters of what remains, and every one of them colds a
/// whole-project census to move a badge that saturates at 99+.
///
/// The floor applies to `events` and to nothing else, which is what makes it
/// safe. An event can only change how MUCH transcript went by; it cannot change
/// whether work is happening, whether input is wanted, or whether a thread
/// exists. Every input that can change one of those advances the generation
/// immediately.
///
/// That distinction is load-bearing, not tidiness. The sidebar query holds
/// `staleTime: Infinity` and refetches only when a `db-change` tells it to, so a
/// deferred rebuild is settled by a later READ or not at all. Deferring a
/// lifecycle transition would therefore risk stranding one: the write that ends
/// an agent's work is the last write it makes, so the read it triggers would
/// serve the pre-transition value and nothing would ever ask again. A transcript
/// event is never the last word on anything — the turn that produced it always
/// closes, immediately — so a deferred count always has a later structural
/// write to settle behind it.
pub(crate) const THREAD_STATUS_COALESCE_FLOOR: Duration = Duration::from_secs(2);

/// The tables the issue rollup reads.
///
/// Wider than the thread rollup's because an issue row says more: `executions`
/// selects which jobs count as current, `merge_requests` and `action_runs` carry
/// the PR facts, and `artifacts` carries the awaiting-confirmation glyph.
///
/// `events` is deliberately absent, and that absence is load-bearing rather than
/// an oversight: the issue rollup reads no transcript at all, so the single
/// highest-volume table in the workspace cannot touch it.
const ISSUE_STATUS_INPUT_TABLES: &[&str] = &[
    "issues",
    "executions",
    "jobs",
    "turns",
    "prompts",
    "permission_requests",
    "runs",
    "merge_requests",
    "action_runs",
    "artifacts",
];

/// The tables the thread activity rollup actually reads.
///
/// This list IS the projection's dependency set, and it has to match
/// `THREAD_STATUS_ROWS_SQL` statement for statement. The query joins threads to
/// their jobs and asks each job for its head turn's state, an unanswered prompt,
/// and a pending permission — and it resolves that last one's owning job through
/// `runs` whenever the permission row itself carries no job id
/// (`COALESCE(pr.job_id, r.job_id)`). `runs` is therefore a real input, not an
/// incidental join, and omitting it would leave a warm snapshot showing the
/// wrong Awaiting Input state until some other listed table happened to change.
///
/// Persisting the owning job authoritatively on every `permission_requests` row
/// would remove that runtime dependency and let `runs` leave this set. That is a
/// write-path and backfill change rather than a read-path one, so it is
/// deliberately not folded in here.
///
/// Everything absent is absent on purpose: merge requests, check results, issue
/// state and execution rows cannot move this answer, and routing them here would
/// rebuild a whole-project rollup to reach the value it already held.
///
/// `events` is here because the snapshot now covers the unread count as well
/// (CAIRN-4331). It was previously excluded on the grounds that an event's
/// `db-change` payload carried no project id, which would have forced the
/// unscoped path and rebuilt every project on every event. That is no longer
/// true: `notify::event_db_change_scoped` resolves and carries `projectId` and
/// `jobId`. Leaving the count outside the fence instead meant re-running a
/// whole-project statement on every sidebar poll, which cost more than the
/// rebuild it was avoiding.
///
/// Membership here only says a table CAN move the projection. Which individual
/// writes actually can is a second question, answered per frame by
/// [`ProjectionInvalidatingEmitter::touches_a_thread`].
const THREAD_STATUS_INPUT_TABLES: &[&str] = &[
    "threads",
    "jobs",
    "turns",
    "prompts",
    "permission_requests",
    "runs",
    "events",
];

/// Advances projection generations at the one boundary every durable mutation
/// already crosses: the post-commit `db-change` notification.
///
/// This is deliberately NOT "the emitted notification is the cache coherence" in
/// the sense that bit us on PR refresh, where the refresh itself emitted and so
/// invalidated its own result. Nothing in the thread rollup's path emits, so for
/// this projection the notification is exactly what it claims to be: a
/// mutation of one of its inputs, already committed, announced once.
pub(crate) struct ProjectionInvalidatingEmitter {
    inner: Arc<dyn EventEmitter>,
    thread_status: Arc<ThreadStatusCache>,
    issue_status: Arc<IssueStatusCache>,
}

impl ProjectionInvalidatingEmitter {
    pub(crate) fn new(
        inner: Arc<dyn EventEmitter>,
        thread_status: Arc<ThreadStatusCache>,
        issue_status: Arc<IssueStatusCache>,
    ) -> Self {
        Self {
            inner,
            thread_status,
            issue_status,
        }
    }

    fn note(&self, payload: &Value) {
        let Some(table) = payload.get("table").and_then(Value::as_str) else {
            return;
        };
        let thread_input = THREAD_STATUS_INPUT_TABLES.contains(&table);
        let issue_input = ISSUE_STATUS_INPUT_TABLES.contains(&table);
        if !thread_input && !issue_input {
            return;
        }
        let Some(project_id) = payload
            .get("projectId")
            .or_else(|| payload.get("project_id"))
            .and_then(Value::as_str)
        else {
            // A scope-less emit is a multi-row recovery sweep, which really can
            // have moved any project. Rebuilding all of them is the honest
            // answer; the alternative is serving a snapshot a sweep invalidated.
            if thread_input {
                self.thread_status
                    .invalidate_matching("thread-input-unscoped", |_| true);
            }
            if issue_input {
                self.issue_status
                    .invalidate_matching("issue-input-unscoped", |_| true);
            }
            return;
        };
        if thread_input && self.touches_a_thread(project_id, table, payload) {
            if table == "events" {
                // The only input allowed to wait. See
                // [`THREAD_STATUS_COALESCE_FLOOR`] for why it is the only one
                // that safely can.
                self.thread_status
                    .invalidate_coalesced(project_id, "thread-transcript");
            } else {
                self.thread_status.invalidate(project_id, "thread-input");
            }
        }
        if issue_input && self.touches_an_issue(project_id, table, payload) {
            // Every issue input is a lifecycle fact — the rollup reads no
            // transcript at all — so there is nothing here to coalesce. The
            // fence and the job scoping are the whole saving.
            self.issue_status.invalidate(project_id, "issue-input");
        }
    }

    /// Whether one committed write could have changed the thread rollup this
    /// project has already published.
    ///
    /// The rollup reads threads and the jobs hanging off them. In a real
    /// workspace those jobs are a small minority of all jobs (121 of 15,119 in
    /// the sample behind CAIRN-4331), so treating every write to an input TABLE
    /// as a write to the projection charged every agent, on every issue, for a
    /// census of rows it could not touch.
    ///
    /// The published snapshot names the jobs it read, which is what makes this
    /// answerable without a query. Anything not placeable against that set is
    /// treated as relevant: this decides whether to skip work, so uncertainty
    /// has to fall on the side of rebuilding.
    fn touches_a_thread(&self, project_id: &str, table: &str, payload: &Value) -> bool {
        match table {
            "threads" => true,
            // A job frame names its owner outright, and does so before the job
            // can produce any rows of its own, which is what lets a job that has
            // never appeared in a snapshot still cold it the moment it joins a
            // thread.
            "jobs" => payload.get("threadId").and_then(Value::as_str).is_some(),
            // Everything else is a row UNDER a job.
            _ => self
                .thread_status
                .published(project_id)
                .is_none_or(|snapshot| frame_could_touch(payload, &snapshot.thread_job_ids)),
        }
    }

    /// The same question for the issue rollup.
    ///
    /// Scoping buys less here than it does for threads — most agent work IS on
    /// issues, so most frames land inside the set — but the two cases it does
    /// exclude are worth excluding: a thread's own churn, and a job belonging to
    /// an execution that is no longer the current one.
    fn touches_an_issue(&self, project_id: &str, table: &str, payload: &Value) -> bool {
        match table {
            "issues" | "executions" => true,
            // A job that names a thread and no issue is a thread's, and the
            // issue rollup never reads it.
            "jobs" => payload.get("issueId").and_then(Value::as_str).is_some(),
            _ => self
                .issue_status
                .published(project_id)
                .is_none_or(|snapshot| frame_could_touch(payload, &snapshot.job_ids)),
        }
    }
}

/// Whether a frame describing a row under some job could touch a projection
/// that read exactly `job_ids`.
///
/// A frame that names no job cannot be placed, and unplaceable means relevant:
/// this decides whether to SKIP work, so the only safe answer to "I cannot tell"
/// is to do it.
fn frame_could_touch(payload: &Value, job_ids: &std::collections::HashSet<String>) -> bool {
    payload
        .get("jobId")
        .or_else(|| payload.get("job_id"))
        .and_then(Value::as_str)
        .is_none_or(|job_id| job_ids.contains(job_id))
}

impl EventEmitter for ProjectionInvalidatingEmitter {
    fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
        if event == "db-change" {
            self.note(&payload);
        }
        self.inner.emit(event, payload)
    }

    fn emit_empty(&self, event: &str) -> Result<(), String> {
        self.inner.emit_empty(event)
    }
}

/// What one flight produced, and the moment it finished.
///
/// `value` is `None` when the projection could not be computed, which is never
/// published and never cached, so a transient failure does not pin an absent
/// projection until the next unrelated invalidation.
///
/// `at` is recorded inside the flight, before the cell becomes visible, so any
/// reader that can see a settled cell sees exactly when it settled. It is the
/// one answer to "how old is what is published" for every freshness rule here.
struct Settled<V> {
    value: Option<V>,
    at: Instant,
}

type Cell<V> = tokio::sync::OnceCell<Settled<V>>;

/// What a read accepts as current.
#[derive(Clone, Copy, Debug)]
enum Freshness {
    /// Whatever is published at the current generation.
    Published,
    /// Anything that had not yet settled when the demand was made.
    Replace { demanded_at: Instant },
    /// What is published, if it was published no longer ago than this.
    Within(Duration),
}

/// Observable behavior of one cache, for tests and for before/after reporting.
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GenerationCacheCounters {
    /// Reads served from an already-materialized cell.
    pub hits: u64,
    /// Reads that performed the computation themselves.
    pub misses: u64,
    /// Reads that joined another reader's in-flight computation.
    pub coalesced: u64,
    /// Computations discarded because their generation was superseded.
    pub invalidated_during_compute: u64,
}

struct GenerationCacheState<V> {
    generations: HashMap<String, u64>,
    cells: HashMap<(String, u64), Arc<Cell<V>>>,
    /// Keys whose inputs have moved but whose published value is still being
    /// served, with the reason the first deferred invalidation gave. Present
    /// only for a cache built [`GenerationCache::with_coalescing`].
    deferred: HashMap<String, &'static str>,
}

impl<V> Default for GenerationCacheState<V> {
    fn default() -> Self {
        Self {
            generations: HashMap::new(),
            cells: HashMap::new(),
            deferred: HashMap::new(),
        }
    }
}

impl<V> GenerationCacheState<V> {
    fn current_generation(&self, key: &str) -> u64 {
        self.generations.get(key).copied().unwrap_or_default()
    }

    /// What the flight at `generation` settled to, or `None` while it is still
    /// in flight or was never started.
    fn settled(&self, key: &str, generation: u64) -> Option<&Settled<V>> {
        self.cells.get(&(key.to_string(), generation))?.get()
    }

    /// When the value at a key's current generation was published, or `None`
    /// when nothing is. The coalescing floor is measured from here rather than
    /// from the first deferred invalidation, so an unbroken write stream cannot
    /// push the rebuild further and further out.
    fn published_at(&self, key: &str) -> Option<Instant> {
        self.settled(key, self.current_generation(key))
            .filter(|settled| settled.value.is_some())
            .map(|settled| settled.at)
    }
}

/// Per-key generation plus a single-flight cell per (key, generation).
///
/// `label` names the projection in this cache's logs so several caches sharing
/// the primitive stay distinguishable in one log stream.
pub(crate) struct GenerationCache<V> {
    label: &'static str,
    /// Shortest interval between two rebuilds driven by
    /// [`GenerationCache::invalidate_coalesced`]. `None` means no projection
    /// using this cache has asked to trade freshness for rebuild rate, and
    /// every invalidation advances the generation at once.
    coalesce_floor: Option<Duration>,
    state: Mutex<GenerationCacheState<V>>,
    hits: AtomicU64,
    misses: AtomicU64,
    coalesced: AtomicU64,
    invalidated_during_compute: AtomicU64,
}

impl<V: Clone> GenerationCache<V> {
    pub(crate) fn new(label: &'static str) -> Self {
        Self {
            label,
            coalesce_floor: None,
            state: Mutex::new(GenerationCacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            invalidated_during_compute: AtomicU64::new(0),
        }
    }

    /// A cache that may keep serving a published value for `floor` after one of
    /// its inputs moves.
    ///
    /// Only worth reaching for when the projection is read on a cadence the
    /// WRITE rate drives, so that invalidations arrive faster than any reader
    /// could use a fresh answer. It trades a bounded, stated staleness for a
    /// rebuild rate that no longer tracks how busy the workspace is; a
    /// projection whose readers are human-paced does not need it.
    pub(crate) fn with_coalescing(label: &'static str, floor: Duration) -> Self {
        Self {
            coalesce_floor: Some(floor),
            ..Self::new(label)
        }
    }

    /// The value currently published for a key, without computing one.
    ///
    /// For callers that need to inspect what is already known in order to decide
    /// whether to invalidate it. A key with nothing published answers `None`,
    /// which such a caller must read as "cannot tell" rather than as "empty".
    pub(crate) fn published(&self, key: &str) -> Option<V> {
        let state = self.state.lock().ok()?;
        state
            .settled(key, state.current_generation(key))?
            .value
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn counters(&self) -> GenerationCacheCounters {
        GenerationCacheCounters {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            coalesced: self.coalesced.load(Ordering::Relaxed),
            invalidated_during_compute: self.invalidated_during_compute.load(Ordering::Relaxed),
        }
    }

    /// The current correctness generation for a key. Advancing it is the only
    /// thing that makes a published value stale.
    pub(crate) fn generation(&self, key: &str) -> u64 {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.generations.get(key).copied())
            .unwrap_or_default()
    }

    /// Advance a key's generation and drop everything materialized under the old
    /// one. Must be called with `state` already held so the decision to advance
    /// and the advance itself cannot be split by another caller.
    fn advance_locked(
        state: &mut GenerationCacheState<V>,
        label: &str,
        key: &str,
        reason: &'static str,
    ) -> u64 {
        let generation = state.generations.entry(key.to_string()).or_default();
        *generation = generation.wrapping_add(1);
        let generation = *generation;
        state.cells.retain(|(cached_key, _), _| cached_key != key);
        // The new generation has nothing outstanding against it. Leaving a
        // deferral behind would let the next reader skip a rebuild it owes.
        state.deferred.remove(key);
        log::debug!("{label} cache invalidated key={key} generation={generation} reason={reason}");
        generation
    }

    /// Record that an input moved, without necessarily rebuilding yet.
    ///
    /// On a cache with no floor this is exactly [`GenerationCache::invalidate`].
    /// With one, the published value keeps being served until the floor has
    /// elapsed since it was published, and the generation advances on the first
    /// read after that. A key with nothing published defers nothing: there would
    /// be no value to serve in the meantime, so waiting could only make the next
    /// reader compute the same thing later.
    ///
    /// Deferral is bounded by READS, not by a timer. If nobody reads the
    /// projection again the rebuild never happens, which is correct: the value
    /// is only observable through a read, and the read that observes it is the
    /// one that pays for it.
    pub(crate) fn invalidate_coalesced(&self, key: &str, reason: &'static str) {
        if self.coalesce_floor.is_none() {
            self.invalidate(key, reason);
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.published_at(key).is_none() {
            Self::advance_locked(&mut state, self.label, key, reason);
            return;
        }
        state.deferred.entry(key.to_string()).or_insert(reason);
    }

    /// Advance the generation if a deferred invalidation is now due. Called with
    /// `state` held, on the read path, so the decision and the advance cannot be
    /// split by a concurrent reader making the same one.
    fn settle_deferred_locked(
        &self,
        state: &mut GenerationCacheState<V>,
        key: &str,
    ) -> Option<u64> {
        let floor = self.coalesce_floor?;
        let reason = *state.deferred.get(key)?;
        let due = state
            .published_at(key)
            .is_none_or(|published| published.elapsed() >= floor);
        if !due {
            return None;
        }
        Some(Self::advance_locked(state, self.label, key, reason))
    }

    /// Advance a key's generation and drop everything materialized under the old
    /// one. Call only after the state transition that changes the projection has
    /// durably committed — an invalidation issued before the write lands can be
    /// overtaken by a reader that still sees the old row.
    pub(crate) fn invalidate(&self, key: &str, reason: &'static str) -> u64 {
        let Ok(mut state) = self.state.lock() else {
            return 0;
        };
        Self::advance_locked(&mut state, self.label, key, reason)
    }

    /// Advance the generation only when the value published at the current
    /// generation satisfies `stale`, deciding and advancing under one lock.
    ///
    /// The caller-owned freshness policy needs this for the same reason: when a
    /// window elapses, every mounted reader observes the same expired value at
    /// once. Reading the value and then invalidating as two steps would let all
    /// of them advance, periodically recreating exactly the fan-out the fence
    /// exists to remove. Returns whether this caller advanced it; after the
    /// first does, the new generation has no published value yet, so the rest
    /// find nothing to replace and join.
    pub(crate) fn invalidate_stale(
        &self,
        key: &str,
        reason: &'static str,
        stale: impl Fn(&V) -> bool,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(published) = state
            .settled(key, state.current_generation(key))
            .and_then(|settled| settled.value.as_ref())
        else {
            return false;
        };
        if !stale(published) {
            return false;
        }
        Self::advance_locked(&mut state, self.label, key, reason);
        true
    }

    /// Advance every key the predicate accepts. Used for the coarse boundaries a
    /// resync or a whole-database event genuinely does invalidate.
    pub(crate) fn invalidate_matching(
        &self,
        reason: &'static str,
        predicate: impl Fn(&str) -> bool,
    ) {
        let keys: Vec<String> = self
            .state
            .lock()
            .map(|state| {
                state
                    .generations
                    .keys()
                    .filter(|key| predicate(key))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        for key in keys {
            self.invalidate(&key, reason);
        }
    }

    /// Read the published value for a key, computing it exactly once per
    /// generation across all concurrent readers.
    ///
    /// `compute` is `Fn`, not `FnOnce`, because a computation invalidated while
    /// it was in flight is retried at the new generation rather than published.
    pub(crate) async fn get_or_compute<F, Fut>(&self, key: &str, compute: F) -> Option<V>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Option<V>>,
    {
        self.get_or_compute_inner(key, Freshness::Published, compute)
            .await
    }

    /// Read a value no older than `max_age`, computing it once per window across
    /// all concurrent readers.
    ///
    /// For a projection whose inputs move continuously while nothing announces
    /// each move: a live check wave's lanes and output tails. Every reader past
    /// the window finds the published value expired, so the first of them
    /// advances the generation under the state lock and the rest join the
    /// flight it opened; readers inside the window are hits. Invalidations still
    /// advance the generation at once, and a computation that spans one is
    /// retried rather than published, exactly as for [`Self::get_or_compute`].
    pub(crate) async fn get_or_compute_within<F, Fut>(
        &self,
        key: &str,
        max_age: Duration,
        compute: F,
    ) -> Option<V>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Option<V>>,
    {
        self.get_or_compute_inner(key, Freshness::Within(max_age), compute)
            .await
    }

    /// A value that had not yet settled when the demand was made: replace
    /// whatever had, and join or consume whatever had not.
    ///
    /// Explicit refresh is a demand, not a durable invalidation. Its answer is
    /// decided by `demanded_at`, the moment the demand was made, which the
    /// caller takes where the demand arrives rather than where it reaches this
    /// cache. That is what makes a burst cost one refresh on any host: every
    /// member was demanded before the replacement settled, so a member the
    /// scheduler carries here only after the replacement has published still
    /// consumes it instead of starting another. A demand made after the
    /// replacement settled is a new demand and gets a new computation.
    ///
    /// The generation advance and the new single-flight cell are installed
    /// under one lock, so concurrent members cannot observe the gap between
    /// them and each decide independently that the replacement is theirs.
    pub(crate) async fn refresh_or_compute<F, Fut>(
        &self,
        key: &str,
        demanded_at: Instant,
        compute: F,
    ) -> Option<V>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Option<V>>,
    {
        self.get_or_compute_inner(key, Freshness::Replace { demanded_at }, compute)
            .await
    }

    async fn get_or_compute_inner<F, Fut>(
        &self,
        key: &str,
        mut freshness: Freshness,
        compute: F,
    ) -> Option<V>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Option<V>>,
    {
        loop {
            let (generation, cell, state_kind) = {
                let mut state = self.state.lock().ok()?;
                let mut generation = *state.generations.entry(key.to_string()).or_default();
                // A deferred invalidation comes due on the first read past the
                // floor. Settling it here, under the same lock that picks the
                // cell, is what keeps a burst of readers from each deciding
                // independently that the rebuild is theirs to start.
                if let Some(settled) = self.settle_deferred_locked(&mut state, key) {
                    generation = settled;
                }
                if let Freshness::Within(max_age) = freshness {
                    let published = state.settled(key, generation).is_some();
                    let expired = state
                        .published_at(key)
                        .is_none_or(|published_at| published_at.elapsed() >= max_age);
                    // Deciding and advancing under one lock is what makes a
                    // burst of expired readers cost one computation: after the
                    // first advances, the new generation has nothing published
                    // and the rest join its flight. A retry after an
                    // invalidation finds nothing published either, so it joins
                    // rather than advancing again.
                    if published && expired {
                        generation =
                            Self::advance_locked(&mut state, self.label, key, "live-refresh");
                    }
                }
                if let Freshness::Replace { demanded_at } = freshness {
                    // Only an outcome that had already settled when the demand
                    // was made is one it asks to replace. A flight still running,
                    // or one that settled since, already answers it, however
                    // long this caller took to be scheduled here.
                    let settled_before_demand = state
                        .settled(key, generation)
                        .is_some_and(|settled| settled.at < demanded_at);
                    if settled_before_demand {
                        generation =
                            Self::advance_locked(&mut state, self.label, key, "explicit-refresh");
                    }
                    // An invalidation-during-compute retry must join the generation
                    // created by that invalidation, not advance it as another demand.
                    freshness = Freshness::Published;
                }
                let cell_key = (key.to_string(), generation);
                let state_kind = state.cells.get(&cell_key).map_or("miss", |cell| {
                    if cell.get().is_some() {
                        "hit"
                    } else {
                        "coalesced"
                    }
                });
                let cell = state
                    .cells
                    .entry(cell_key)
                    .or_insert_with(|| Arc::new(Cell::new()))
                    .clone();
                (generation, cell, state_kind)
            };
            match state_kind {
                "hit" => self.hits.fetch_add(1, Ordering::Relaxed),
                "coalesced" => self.coalesced.fetch_add(1, Ordering::Relaxed),
                _ => self.misses.fetch_add(1, Ordering::Relaxed),
            };
            log::debug!(
                "{} cache {state_kind} key={key} generation={generation}",
                self.label
            );

            let started = Instant::now();
            let value = cell
                .get_or_init(|| async {
                    let value = compute().await;
                    Settled {
                        value,
                        at: Instant::now(),
                    }
                })
                .await
                .value
                .clone();
            if state_kind == "miss" {
                log::info!(
                    "{} computed key={key} generation={generation} duration_ms={}",
                    self.label,
                    started.elapsed().as_millis()
                );
            }

            let current = self.generation(key);
            if current == generation {
                if value.is_none() {
                    // A failed computation is not a fact about the projection.
                    // Drop the cell so the next reader retries instead of
                    // inheriting this reader's transient error.
                    if let Ok(mut state) = self.state.lock() {
                        state.cells.remove(&(key.to_string(), generation));
                    }
                }
                return value;
            }
            self.invalidated_during_compute
                .fetch_add(1, Ordering::Relaxed);
            log::debug!(
                "{} cache retry key={key} generation={generation} current_generation={current} reason=invalidated-during-compute",
                self.label
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::CapturingEmitter;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Barrier;

    /// A concurrent burst of readers for one unchanged key costs exactly one
    /// computation. This is the property the whole primitive exists for: without
    /// it, sixteen mounted queries each pay for the same GitHub fetch and `jj`
    /// fan-out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_concurrent_burst_computes_once() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let computations = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(16));

        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let computations = computations.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_compute("k", || {
                        let computations = computations.clone();
                        async move {
                            computations.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            Some(7u64)
                        }
                    })
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(7));
        }

        assert_eq!(computations.load(Ordering::SeqCst), 1);
        let counters = cache.counters();
        assert_eq!(counters.misses, 1, "exactly one reader did the work");
        assert_eq!(
            counters.hits + counters.coalesced,
            15,
            "every other reader was served without computing"
        );
    }

    /// A warm read at an unchanged generation performs no work at all — the idle
    /// case the runner was burning cores on.
    #[tokio::test(flavor = "current_thread")]
    async fn a_warm_read_computes_nothing() {
        let cache: GenerationCache<u64> = GenerationCache::new("test");
        let computations = AtomicUsize::new(0);
        let compute = || async {
            computations.fetch_add(1, Ordering::SeqCst);
            Some(1u64)
        };

        assert_eq!(cache.get_or_compute("k", compute).await, Some(1));
        for _ in 0..10 {
            assert_eq!(cache.get_or_compute("k", compute).await, Some(1));
        }
        assert_eq!(computations.load(Ordering::SeqCst), 1);
        assert_eq!(cache.counters().hits, 10);
    }

    /// A computation that spanned an invalidation is rejected, not published: the
    /// reader retries at the new generation and returns the newer answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_result_invalidated_mid_flight_is_not_published() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let value = Arc::new(AtomicU64::new(1));
        let entered = Arc::new(Barrier::new(2));

        let reader = {
            let cache = cache.clone();
            let value = value.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                cache
                    .get_or_compute("k", || {
                        let value = value.clone();
                        let entered = entered.clone();
                        async move {
                            let observed = value.load(Ordering::SeqCst);
                            // Only the first pass waits for the invalidator; the
                            // retry must not deadlock on a second wait.
                            if observed == 1 {
                                entered.wait().await;
                                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                            }
                            Some(observed)
                        }
                    })
                    .await
            })
        };

        entered.wait().await;
        value.store(2, Ordering::SeqCst);
        cache.invalidate("k", "test");

        assert_eq!(
            reader.await.unwrap(),
            Some(2),
            "the stale in-flight result must not be published"
        );
        assert_eq!(cache.counters().invalidated_during_compute, 1);
    }

    /// A failed computation is not a fact. It is neither published nor cached, so
    /// a transient error does not pin an absent projection until something else
    /// happens to invalidate.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_computation_is_retried_rather_than_cached() {
        let cache: GenerationCache<u64> = GenerationCache::new("test");
        let attempts = AtomicUsize::new(0);
        let compute = || async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                None
            } else {
                Some(5u64)
            }
        };

        assert_eq!(cache.get_or_compute("k", compute).await, None);
        assert_eq!(cache.get_or_compute("k", compute).await, Some(5));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    /// A cold burst also computes once, even when the value is immediately ready
    /// and independently scheduled peers enter after the first one publishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cold_burst_of_explicit_refreshes_computes_once() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let computations = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(16));

        let demanded_at = Instant::now();
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let computations = computations.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .refresh_or_compute("k", demanded_at, || {
                        computations.fetch_add(1, Ordering::SeqCst);
                        async { Some(1u64) }
                    })
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(1));
        }

        assert_eq!(
            computations.load(Ordering::SeqCst),
            1,
            "one cold explicit burst must compute once"
        );
        let counters = cache.counters();
        assert_eq!(counters.misses, 1);
        assert_eq!(counters.hits + counters.coalesced, 15);
        assert_eq!(counters.invalidated_during_compute, 0);
    }

    /// N simultaneous demands for fresh state advance the generation once and
    /// share one computation.
    ///
    /// Advancing unconditionally per caller is the regression this guards: each
    /// advance rejects the readers already computing under the older generation,
    /// so a burst of refresh presses would recreate the expensive work N times
    /// instead of collapsing it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_burst_of_refresh_demands_advances_the_generation_once() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let computations = Arc::new(AtomicUsize::new(0));

        // Warm it, so the burst below is a demand to REPLACE a published value.
        cache
            .get_or_compute("k", || async { Some(1u64) })
            .await
            .unwrap();
        let warm_generation = cache.generation("k");

        let demanded_at = Instant::now();
        let barrier = Arc::new(Barrier::new(16));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let computations = computations.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                // Exactly what an explicit refresh does: replace a settled
                // value and reserve its replacement flight atomically, or join
                // the flight already producing a fresh one.
                cache
                    .refresh_or_compute("k", demanded_at, || {
                        let computations = computations.clone();
                        async move {
                            computations.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            Some(2u64)
                        }
                    })
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(2));
        }

        assert_eq!(
            computations.load(Ordering::SeqCst),
            1,
            "sixteen simultaneous refresh demands must produce one refresh"
        );
        assert_eq!(
            cache.generation("k"),
            warm_generation + 1,
            "one caller wins the advance; the rest join the flight it created"
        );
    }

    /// N observers polling a live projection at once share one computation per
    /// window, inside the window every read is a hit, and past it one reader
    /// recomputes for all of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_reads_compute_once_per_window() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let computations = Arc::new(AtomicUsize::new(0));
        let window = Duration::from_millis(200);
        let burst = move |cache: Arc<GenerationCache<u64>>, computations: Arc<AtomicUsize>| async move {
            let barrier = Arc::new(Barrier::new(16));
            let mut readers = Vec::new();
            for _ in 0..16 {
                let cache = cache.clone();
                let computations = computations.clone();
                let barrier = barrier.clone();
                readers.push(tokio::spawn(async move {
                    barrier.wait().await;
                    cache
                        .get_or_compute_within("k", window, || {
                            let computations = computations.clone();
                            async move {
                                let n = computations.fetch_add(1, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                Some(n as u64)
                            }
                        })
                        .await
                }));
            }
            let mut values = Vec::new();
            for reader in readers {
                values.push(reader.await.unwrap());
            }
            values
        };

        let first = burst(cache.clone(), computations.clone()).await;
        assert!(first.iter().all(|value| *value == Some(0)));
        assert_eq!(
            computations.load(Ordering::SeqCst),
            1,
            "one computation for the burst"
        );

        let inside = burst(cache.clone(), computations.clone()).await;
        assert!(
            inside.iter().all(|value| *value == Some(0)),
            "inside the window: hits"
        );
        assert_eq!(computations.load(Ordering::SeqCst), 1);

        tokio::time::sleep(window + Duration::from_millis(20)).await;
        let after = burst(cache.clone(), computations.clone()).await;
        assert!(
            after.iter().all(|value| *value == Some(1)),
            "past the window: fresh"
        );
        assert_eq!(
            computations.load(Ordering::SeqCst),
            2,
            "an expired window costs one recomputation, not one per observer"
        );
    }

    /// A live read obeys the generation fence: an invalidation during its
    /// flight is retried at the new generation rather than published, and an
    /// invalidation between reads is served at once rather than waiting out
    /// the window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_reads_honor_invalidation() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let value = Arc::new(AtomicU64::new(1));
        let entered = Arc::new(Barrier::new(2));
        let window = Duration::from_secs(60);

        let reader = {
            let cache = cache.clone();
            let value = value.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                cache
                    .get_or_compute_within("k", window, || {
                        let value = value.clone();
                        let entered = entered.clone();
                        async move {
                            let observed = value.load(Ordering::SeqCst);
                            if observed == 1 {
                                entered.wait().await;
                                tokio::time::sleep(Duration::from_millis(30)).await;
                            }
                            Some(observed)
                        }
                    })
                    .await
            })
        };
        entered.wait().await;
        value.store(2, Ordering::SeqCst);
        cache.invalidate("k", "test");
        assert_eq!(
            reader.await.unwrap(),
            Some(2),
            "the stale flight is not published"
        );
        assert_eq!(cache.counters().invalidated_during_compute, 1);

        value.store(3, Ordering::SeqCst);
        cache.invalidate("k", "settlement");
        let read = cache
            .get_or_compute_within("k", window, || {
                let value = value.clone();
                async move { Some(value.load(Ordering::SeqCst)) }
            })
            .await;
        assert_eq!(
            read,
            Some(3),
            "an invalidation does not wait out the window"
        );
    }

    /// Whether an explicit demand computes is decided by when it was made, not by
    /// when it reached the cache.
    ///
    /// The late member is the shape that failed on loaded hosts: one task of a
    /// burst parked behind the worker running the refresh, polled only after the
    /// refresh had published. It consumes that refresh however long it waited.
    /// A demand made after the refresh settled is a new operator action and
    /// starts a distinct refresh rather than being pinned to the previous one.
    #[tokio::test(flavor = "current_thread")]
    async fn a_demand_is_answered_by_whatever_settled_after_it_was_made() {
        let cache: GenerationCache<u64> = GenerationCache::new("test");
        let computations = AtomicUsize::new(0);

        cache
            .get_or_compute("k", || async { Some(1u64) })
            .await
            .unwrap();
        let refresh = || async {
            let earlier = computations.fetch_add(1, Ordering::SeqCst) as u64;
            Some(2 + earlier)
        };

        let burst = Instant::now();
        assert_eq!(cache.refresh_or_compute("k", burst, refresh).await, Some(2));
        // Longer than the scheduling delay a burst member sees on a loaded host.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            cache.refresh_or_compute("k", burst, refresh).await,
            Some(2),
            "a late member of the burst consumes the burst's refresh"
        );
        assert_eq!(computations.load(Ordering::SeqCst), 1);

        let later = Instant::now();
        assert_eq!(
            cache.refresh_or_compute("k", later, refresh).await,
            Some(3),
            "a demand made after the refresh settled starts a distinct refresh"
        );
        assert_eq!(computations.load(Ordering::SeqCst), 2);
    }

    /// The same at a freshness rollover, where every mounted reader observes the
    /// same expired value at once rather than demanding a refresh explicitly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_burst_at_freshness_rollover_recomputes_once() {
        let cache: Arc<GenerationCache<u64>> = Arc::new(GenerationCache::new("test"));
        let computations = Arc::new(AtomicUsize::new(0));

        // Publish a value the readers below will all agree has expired.
        cache
            .get_or_compute("k", || async { Some(1u64) })
            .await
            .unwrap();
        let warm_generation = cache.generation("k");

        let barrier = Arc::new(Barrier::new(16));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let computations = computations.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                cache.invalidate_stale("k", "freshness-window-elapsed", |value| *value == 1);
                cache
                    .get_or_compute("k", || {
                        let computations = computations.clone();
                        async move {
                            computations.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            Some(2u64)
                        }
                    })
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(2));
        }

        assert_eq!(
            computations.load(Ordering::SeqCst),
            1,
            "a window elapsing must cost one recomputation, not one per reader"
        );
        assert_eq!(cache.generation("k"), warm_generation + 1);
    }

    /// A value that is still fresh is not disturbed by a reader checking.
    #[tokio::test(flavor = "current_thread")]
    async fn a_fresh_value_is_not_invalidated_by_the_freshness_check() {
        let cache: GenerationCache<u64> = GenerationCache::new("test");
        cache
            .get_or_compute("k", || async { Some(1u64) })
            .await
            .unwrap();
        let generation = cache.generation("k");

        assert!(!cache.invalidate_stale("k", "test", |_| false));
        assert_eq!(cache.generation("k"), generation);
        // Nothing published yet for an untouched key: nothing to call stale.
        assert!(!cache.invalidate_stale("other", "test", |_| true));
    }

    /// Invalidating one key leaves every other key warm. A PR refresh for one job
    /// must not cost every other job its snapshot.
    #[tokio::test(flavor = "current_thread")]
    async fn invalidation_is_scoped_to_its_key() {
        let cache: GenerationCache<u64> = GenerationCache::new("test");
        let computations = AtomicUsize::new(0);
        let compute = || async {
            computations.fetch_add(1, Ordering::SeqCst);
            Some(1u64)
        };

        cache.get_or_compute("a", compute).await;
        cache.get_or_compute("b", compute).await;
        assert_eq!(computations.load(Ordering::SeqCst), 2);

        cache.invalidate("a", "test");
        cache.get_or_compute("b", compute).await;
        assert_eq!(
            computations.load(Ordering::SeqCst),
            2,
            "the untouched key stayed warm"
        );
        cache.get_or_compute("a", compute).await;
        assert_eq!(computations.load(Ordering::SeqCst), 3);
    }

    fn invalidating_emitter() -> (ProjectionInvalidatingEmitter, Arc<ThreadStatusCache>) {
        invalidating_emitter_with_floor(Duration::ZERO)
    }

    fn invalidating_emitter_with_floor(
        floor: Duration,
    ) -> (ProjectionInvalidatingEmitter, Arc<ThreadStatusCache>) {
        let (emitter, threads, _issues) = invalidating_emitter_both(floor);
        (emitter, threads)
    }

    fn invalidating_emitter_both(
        floor: Duration,
    ) -> (
        ProjectionInvalidatingEmitter,
        Arc<ThreadStatusCache>,
        Arc<IssueStatusCache>,
    ) {
        let threads = Arc::new(ThreadStatusCache::with_coalescing("test-threads", floor));
        let issues = Arc::new(IssueStatusCache::with_coalescing("test-issues", floor));
        (
            ProjectionInvalidatingEmitter::new(
                Arc::new(CapturingEmitter::new()),
                threads.clone(),
                issues.clone(),
            ),
            threads,
            issues,
        )
    }

    /// Publish an issue rollup that claims to have read exactly `job_ids`.
    async fn publish_issue_snapshot(
        cache: &IssueStatusCache,
        project: &str,
        job_ids: &[&str],
        rebuilds: &AtomicUsize,
    ) {
        let ids: std::collections::HashSet<String> =
            job_ids.iter().map(|id| id.to_string()).collect();
        cache
            .get_or_compute(project, || {
                let ids = ids.clone();
                async {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Some(Arc::new(IssueStatusSnapshot {
                        indicators: Vec::new(),
                        job_ids: ids,
                    }))
                }
            })
            .await;
    }

    /// The two sidebar projections read overlapping tables but disjoint rows, so
    /// one project's thread churn must not rebuild its issue rollup, and vice
    /// versa. Without this they would each pay for the other's traffic and the
    /// scoping would buy nothing.
    #[tokio::test]
    async fn the_two_sidebar_rollups_do_not_rebuild_each_other() {
        let (emitter, threads, issues) = invalidating_emitter_both(Duration::ZERO);
        let thread_rebuilds = AtomicUsize::new(0);
        let issue_rebuilds = AtomicUsize::new(0);
        publish_snapshot(&threads, "proj", &["thread-job"], &thread_rebuilds).await;
        publish_issue_snapshot(&issues, "proj", &["issue-job"], &issue_rebuilds).await;

        // A turn on the thread's job.
        emitter
            .emit(
                "db-change",
                serde_json::json!({
                    "table": "turns", "projectId": "proj", "jobId": "thread-job",
                }),
            )
            .unwrap();
        // A turn on the issue's job.
        emitter
            .emit(
                "db-change",
                serde_json::json!({
                    "table": "turns", "projectId": "proj", "jobId": "issue-job",
                }),
            )
            .unwrap();

        publish_snapshot(&threads, "proj", &["thread-job"], &thread_rebuilds).await;
        publish_issue_snapshot(&issues, "proj", &["issue-job"], &issue_rebuilds).await;
        assert_eq!(
            thread_rebuilds.load(Ordering::SeqCst),
            2,
            "the thread rollup rebuilt for its own job and nothing else"
        );
        assert_eq!(
            issue_rebuilds.load(Ordering::SeqCst),
            2,
            "and the issue rollup for its own"
        );
    }

    /// The issue rollup reads no transcript, so the workspace's highest-volume
    /// table cannot reach it at all.
    #[tokio::test]
    async fn transcript_events_never_rebuild_the_issue_rollup() {
        let (emitter, _threads, issues) = invalidating_emitter_both(Duration::ZERO);
        let rebuilds = AtomicUsize::new(0);
        publish_issue_snapshot(&issues, "proj", &["issue-job"], &rebuilds).await;

        for _ in 0..200 {
            emitter
                .emit("db-change", events_frame("proj", "issue-job"))
                .unwrap();
        }
        publish_issue_snapshot(&issues, "proj", &["issue-job"], &rebuilds).await;

        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
    }

    /// Publish a rollup that claims to have read exactly `thread_job_ids`, and
    /// report how many times the census actually ran.
    ///
    /// The counter is the point: every assertion below is about how much work a
    /// stream of writes causes, and a generation number only tells you that
    /// something was discarded, not that anything was recomputed.
    async fn publish_snapshot(
        cache: &ThreadStatusCache,
        project: &str,
        thread_job_ids: &[&str],
        rebuilds: &AtomicUsize,
    ) {
        let ids: std::collections::HashSet<String> =
            thread_job_ids.iter().map(|id| id.to_string()).collect();
        cache
            .get_or_compute(project, || {
                let ids = ids.clone();
                async {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Some(Arc::new(ThreadStatusSnapshot {
                        indicators: Vec::new(),
                        thread_job_ids: ids,
                    }))
                }
            })
            .await;
    }

    fn events_frame(project: &str, job: &str) -> Value {
        serde_json::json!({
            "table": "events",
            "action": "insert",
            "projectId": project,
            "jobId": job,
        })
    }

    /// The defect CAIRN-4331 fixed, asserted as work done: an agent writing a
    /// transcript on an ISSUE cannot make the thread sidebar recount anything.
    ///
    /// This is the regression that matters most, because it is invisible from
    /// the outside — every indicator stays correct while the runner burns a
    /// whole-project census per transcript event. In the sample behind the issue
    /// that was 18,565 event frames in an hour against 32 threads that four out
    /// of five of them had nothing to do with.
    #[tokio::test]
    async fn an_issue_nodes_transcript_cannot_rebuild_the_thread_rollup() {
        let (emitter, cache) = invalidating_emitter();
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;
        assert_eq!(rebuilds.load(Ordering::SeqCst), 1, "the initial build");
        let generation = cache.generation("proj");

        for _ in 0..500 {
            emitter
                .emit("db-change", events_frame("proj", "issue-node-job"))
                .unwrap();
        }
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;

        assert_eq!(
            rebuilds.load(Ordering::SeqCst),
            1,
            "500 transcript events on an issue node must cost zero thread censuses"
        );
        assert_eq!(cache.generation("proj"), generation);
    }

    /// A thread's OWN transcript does move its row, and is allowed to — but at
    /// the floor's rate, not the writer's.
    #[tokio::test]
    async fn a_threads_own_transcript_rebuilds_once_per_floor() {
        let (emitter, cache) = invalidating_emitter_with_floor(Duration::from_millis(60));
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;

        for _ in 0..200 {
            emitter
                .emit("db-change", events_frame("proj", "thread-job"))
                .unwrap();
            publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;
        }
        assert_eq!(
            rebuilds.load(Ordering::SeqCst),
            1,
            "a burst inside one floor is one rebuild, however many writes it carried"
        );

        tokio::time::sleep(Duration::from_millis(70)).await;
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;
        assert_eq!(
            rebuilds.load(Ordering::SeqCst),
            2,
            "and the first read past the floor pays for exactly one more"
        );
    }

    /// The hazard the narrow floor exists to avoid.
    ///
    /// The write that ENDS an agent's work is the last write it makes. If it
    /// could be deferred, the read it triggers would serve the pre-transition
    /// value and nothing would ever ask again, so the sidebar would keep
    /// reading Running for a thread that finished. A transcript burst may wait;
    /// the turn closing behind it may not.
    #[tokio::test]
    async fn the_write_that_ends_the_work_is_never_deferred() {
        let (emitter, cache) = invalidating_emitter_with_floor(Duration::from_secs(30));
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;

        for _ in 0..50 {
            emitter
                .emit("db-change", events_frame("proj", "thread-job"))
                .unwrap();
        }
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;
        assert_eq!(
            rebuilds.load(Ordering::SeqCst),
            1,
            "the transcript burst waits out the floor"
        );

        emitter
            .emit(
                "db-change",
                serde_json::json!({
                    "table": "turns", "projectId": "proj", "jobId": "thread-job",
                }),
            )
            .unwrap();
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;
        assert_eq!(
            rebuilds.load(Ordering::SeqCst),
            2,
            "the turn closing reaches the next read regardless of the floor"
        );
    }

    /// Coalescing bounds the rate, not the truth: whatever the writes said
    /// reaches the next reader past the floor.
    #[tokio::test]
    async fn a_deferred_invalidation_converges_after_the_floor() {
        let cache: GenerationCache<u64> =
            GenerationCache::with_coalescing("test", Duration::from_millis(50));
        let value = AtomicU64::new(1);
        let compute = || async { Some(value.load(Ordering::SeqCst)) };

        assert_eq!(cache.get_or_compute("k", compute).await, Some(1));
        value.store(2, Ordering::SeqCst);
        cache.invalidate_coalesced("k", "test");
        assert_eq!(
            cache.get_or_compute("k", compute).await,
            Some(1),
            "inside the floor the published value is still what is served"
        );

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            cache.get_or_compute("k", compute).await,
            Some(2),
            "past it, the deferred invalidation comes due"
        );
    }

    /// Deferral needs something to defer TO. With nothing published there is no
    /// value to keep serving, so waiting would only move the same work later.
    #[tokio::test]
    async fn a_key_with_nothing_published_does_not_defer() {
        let cache: GenerationCache<u64> =
            GenerationCache::with_coalescing("test", Duration::from_secs(30));
        let value = AtomicU64::new(1);
        let compute = || async { Some(value.load(Ordering::SeqCst)) };

        cache.invalidate_coalesced("k", "test");
        value.store(2, Ordering::SeqCst);
        assert_eq!(cache.get_or_compute("k", compute).await, Some(2));
    }

    /// An operator action is not something to be smoothed out. A thread
    /// appearing, being renamed, or being closed reaches the sidebar on the next
    /// read, floor or no floor.
    #[tokio::test]
    async fn a_change_to_the_thread_rows_skips_the_floor() {
        let (emitter, cache) = invalidating_emitter_with_floor(Duration::from_secs(30));
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &[], &rebuilds).await;

        emitter
            .emit(
                "db-change",
                serde_json::json!({"table": "threads", "projectId": "proj"}),
            )
            .unwrap();
        publish_snapshot(&cache, "proj", &[], &rebuilds).await;

        assert_eq!(rebuilds.load(Ordering::SeqCst), 2);
    }

    /// Marking a thread viewed clears its badge on the next read. It goes
    /// through the orchestrator's direct invalidation rather than the emitter,
    /// so it is the immediate path by construction — asserted here because the
    /// badge failing to clear is the most visible way coalescing could go wrong.
    #[tokio::test]
    async fn marking_viewed_is_not_deferred() {
        let cache: ThreadStatusCache =
            ThreadStatusCache::with_coalescing("test", Duration::from_secs(30));
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &[], &rebuilds).await;

        cache.invalidate("proj", "thread-read-position");
        publish_snapshot(&cache, "proj", &[], &rebuilds).await;

        assert_eq!(rebuilds.load(Ordering::SeqCst), 2);
    }

    /// A job that joins a thread announces itself before it can write anything,
    /// which is what lets the published job set be trusted as a filter: a job
    /// nobody has seen yet cannot be silently ignored, because its own insert
    /// carries the thread id.
    #[tokio::test]
    async fn a_job_joining_a_thread_colds_the_snapshot_that_has_not_seen_it() {
        let (emitter, cache) = invalidating_emitter();
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &["old-job"], &rebuilds).await;

        emitter
            .emit(
                "db-change",
                serde_json::json!({
                    "table": "jobs",
                    "jobId": "new-job",
                    "threadId": "th",
                    "projectId": "proj",
                }),
            )
            .unwrap();
        publish_snapshot(&cache, "proj", &["old-job", "new-job"], &rebuilds).await;
        assert_eq!(rebuilds.load(Ordering::SeqCst), 2);

        // And now that it is in the set, its transcript reaches the rollup too.
        emitter
            .emit("db-change", events_frame("proj", "new-job"))
            .unwrap();
        publish_snapshot(&cache, "proj", &["old-job", "new-job"], &rebuilds).await;
        assert_eq!(rebuilds.load(Ordering::SeqCst), 3);
    }

    /// An issue node's job insert is not a thread's business.
    #[tokio::test]
    async fn a_job_outside_any_thread_does_not_cold_the_snapshot() {
        let (emitter, cache) = invalidating_emitter();
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &[], &rebuilds).await;

        emitter
            .emit(
                "db-change",
                serde_json::json!({
                    "table": "jobs",
                    "jobId": "issue-node",
                    "issueId": "i-1",
                    "threadId": Value::Null,
                    "projectId": "proj",
                }),
            )
            .unwrap();
        publish_snapshot(&cache, "proj", &[], &rebuilds).await;

        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
    }

    /// A frame that cannot be placed against the job set rebuilds anyway.
    /// Skipping work is only safe where the projection is CERTAIN the work is
    /// unnecessary; `permission_requests` carries no job id at all.
    #[tokio::test]
    async fn an_unplaceable_frame_still_rebuilds() {
        let (emitter, cache) = invalidating_emitter();
        let rebuilds = AtomicUsize::new(0);
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;

        emitter
            .emit(
                "db-change",
                serde_json::json!({"table": "permission_requests", "projectId": "proj"}),
            )
            .unwrap();
        publish_snapshot(&cache, "proj", &["thread-job"], &rebuilds).await;

        assert_eq!(rebuilds.load(Ordering::SeqCst), 2);
    }

    /// The dependency set, asserted as behavior: a change to one of the rollup's
    /// inputs advances its project's generation, and a change to anything else
    /// — notably a merge request or a check result — does not.
    ///
    /// Every frame here is deliberately unplaceable against a job set, so this
    /// asserts only which TABLES are inputs. Which individual writes to those
    /// tables are relevant is a separate contract, covered by the job-scoping
    /// tests below.
    #[test]
    fn only_the_rollups_own_inputs_advance_its_generation() {
        // `runs` is in the set because the SQL resolves a pending permission's
        // owning job through it when the permission row carries no job id.
        // `events` is in it because the unread count it moves now lives inside
        // the same snapshot.
        for table in [
            "threads",
            "turns",
            "prompts",
            "permission_requests",
            "runs",
            "events",
        ] {
            let (emitter, cache) = invalidating_emitter();
            let before = cache.generation("proj");
            emitter
                .emit(
                    "db-change",
                    serde_json::json!({"table": table, "projectId": "proj"}),
                )
                .unwrap();
            assert!(
                cache.generation("proj") > before,
                "{table} is an input to the thread rollup"
            );
        }

        for table in [
            "merge_requests",
            "check_result_cache",
            "issues",
            "executions",
        ] {
            let (emitter, cache) = invalidating_emitter();
            let before = cache.generation("proj");
            emitter
                .emit(
                    "db-change",
                    serde_json::json!({"table": table, "projectId": "proj"}),
                )
                .unwrap();
            assert_eq!(
                cache.generation("proj"),
                before,
                "{table} cannot move the thread rollup and must not rebuild it"
            );
        }
    }

    /// A scoped input change touches only its own project.
    #[test]
    fn thread_invalidation_is_scoped_by_project() {
        let (emitter, cache) = invalidating_emitter();
        // Materialize both projects so they have a generation to compare.
        cache.invalidate("other", "seed");
        let other_before = cache.generation("other");
        emitter
            .emit(
                "db-change",
                serde_json::json!({"table": "turns", "projectId": "proj"}),
            )
            .unwrap();
        assert_eq!(cache.generation("other"), other_before);
    }

    /// A scope-less sweep really can have moved any project, so it clears them
    /// all rather than serving a snapshot the sweep invalidated.
    #[test]
    fn an_unscoped_sweep_clears_every_project() {
        let (emitter, cache) = invalidating_emitter();
        cache.invalidate("a", "seed");
        cache.invalidate("b", "seed");
        let (a, b) = (cache.generation("a"), cache.generation("b"));
        emitter
            .emit("db-change", serde_json::json!({"table": "jobs"}))
            .unwrap();
        assert!(cache.generation("a") > a);
        assert!(cache.generation("b") > b);
    }
}
