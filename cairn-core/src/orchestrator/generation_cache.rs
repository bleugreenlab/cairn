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

use crate::jobs::queries::ThreadActivityRow;
use crate::services::EventEmitter;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) type ThreadStatusCache = GenerationCache<Arc<Vec<ThreadActivityRow>>>;

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
/// `events` is absent for a stronger reason than irrelevance. A thread row also
/// carries an unread transcript count, which every durable event moves — but an
/// event's `db-change` payload carries no project id, so routing it here would
/// take the unscoped path and invalidate EVERY project's activity snapshot on
/// every event, which is precisely the continuous whole-project rebuild
/// CAIRN-4190 removed. The unread count is therefore computed fresh per read and
/// capped, rather than snapshotted; see `jobs::queries::thread_unread_counts`.
const THREAD_STATUS_INPUT_TABLES: &[&str] = &[
    "threads",
    "jobs",
    "turns",
    "prompts",
    "permission_requests",
    "runs",
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
}

impl ProjectionInvalidatingEmitter {
    pub(crate) fn new(inner: Arc<dyn EventEmitter>, thread_status: Arc<ThreadStatusCache>) -> Self {
        Self {
            inner,
            thread_status,
        }
    }

    fn note(&self, payload: &Value) {
        let Some(table) = payload.get("table").and_then(Value::as_str) else {
            return;
        };
        if !THREAD_STATUS_INPUT_TABLES.contains(&table) {
            return;
        }
        match payload
            .get("projectId")
            .or_else(|| payload.get("project_id"))
            .and_then(Value::as_str)
        {
            Some(project_id) => {
                self.thread_status.invalidate(project_id, "thread-input");
            }
            // A scope-less emit is a multi-row recovery sweep, which really can
            // have moved any project. Rebuilding all of them is the honest
            // answer; the alternative is serving a snapshot a sweep invalidated.
            None => self
                .thread_status
                .invalidate_matching("thread-input-unscoped", |_| true),
        }
    }
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

/// A computation's outcome is `Option`: `None` means "could not be computed",
/// which is never published and never cached, so a transient failure does not
/// pin an absent projection until the next unrelated invalidation.
type Cell<V> = tokio::sync::OnceCell<Option<V>>;

/// Observable behavior of one cache, for tests and for before/after reporting.
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
}

impl<V> Default for GenerationCacheState<V> {
    fn default() -> Self {
        Self {
            generations: HashMap::new(),
            cells: HashMap::new(),
        }
    }
}

/// Per-key generation plus a single-flight cell per (key, generation).
///
/// `label` names the projection in this cache's logs so several caches sharing
/// the primitive stay distinguishable in one log stream.
pub(crate) struct GenerationCache<V> {
    label: &'static str,
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
            state: Mutex::new(GenerationCacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            invalidated_during_compute: AtomicU64::new(0),
        }
    }

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
        log::debug!("{label} cache invalidated key={key} generation={generation} reason={reason}");
        generation
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

    /// Advance the generation only if a settled value is currently published for
    /// this key, returning whether this caller advanced it.
    ///
    /// This is how a *demand* for fresh state — an operator pressing Refresh —
    /// stays one refresh when N of them arrive together. `invalidate` is
    /// unconditional, so N demands would advance N times, and each advance
    /// rejects the readers already computing under the older generation: the
    /// single-flight only holds *within* a chosen generation, so choosing the
    /// generation is the part that has to be atomic.
    ///
    /// "Is something published" is the right condition, and a
    /// compare-and-advance against an observed generation is not: once the first
    /// caller advances, a caller arriving a moment later observes the *new*
    /// generation, and its compare would succeed and advance again even though a
    /// refresh for it is already in flight. A flight in progress is fetching
    /// right now, so its result already satisfies the demand — only a settled,
    /// published value is worth replacing.
    pub(crate) fn invalidate_published(&self, key: &str, reason: &'static str) -> bool {
        self.invalidate_stale(key, reason, |_| true)
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
        let generation = state.generations.get(key).copied().unwrap_or_default();
        let published = state
            .cells
            .get(&(key.to_string(), generation))
            .and_then(|cell| cell.get())
            .cloned()
            .flatten();
        let Some(published) = published else {
            return false;
        };
        if !stale(&published) {
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
        loop {
            let (generation, cell, state_kind) = {
                let mut state = self.state.lock().ok()?;
                let generation = *state.generations.entry(key.to_string()).or_default();
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
            let value = cell.get_or_init(&compute).await.clone();
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

        let barrier = Arc::new(Barrier::new(16));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let computations = computations.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                // Exactly what an explicit refresh does: replace a settled
                // value, or join the flight already producing a fresh one.
                cache.invalidate_published("k", "explicit-refresh");
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
            "sixteen simultaneous refresh demands must produce one refresh"
        );
        assert_eq!(
            cache.generation("k"),
            warm_generation + 1,
            "one caller wins the advance; the rest join the flight it created"
        );
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
        let cache = Arc::new(ThreadStatusCache::new("test"));
        (
            ProjectionInvalidatingEmitter::new(Arc::new(CapturingEmitter::new()), cache.clone()),
            cache,
        )
    }

    /// The dependency set, asserted as behavior: a change to one of the rollup's
    /// five inputs advances its project's generation, and a change to anything
    /// else — notably a merge request or a check result — does not.
    #[test]
    fn only_the_rollups_own_inputs_advance_its_generation() {
        // `runs` is in the set because the SQL resolves a pending permission's
        // owning job through it when the permission row carries no job id.
        for table in [
            "threads",
            "jobs",
            "turns",
            "prompts",
            "permission_requests",
            "runs",
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
