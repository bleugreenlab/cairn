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
//!
//! [`CadencedCache`] is the sibling for projections that cost seconds and are
//! invalidated several times a second, where rejecting an overtaken rebuild and
//! retrying it is itself the failure: it serves the last published value, owns
//! its own rebuild cadence, and announces only changed answers.

use crate::issues::crud::EncodedIssueList;
use crate::jobs::queries::{IssueStatusSnapshot, ThreadStatusSnapshot};
use crate::services::EventEmitter;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

pub(crate) type ThreadStatusCache = CadencedCache<Arc<ThreadStatusSnapshot>>;
pub(crate) type IssueStatusCache = CadencedCache<Arc<IssueStatusSnapshot>>;
pub(crate) type IssueListCache = CadencedCache<EncodedIssueList>;

/// The shortest interval between two rebuilds of one project's thread or issue
/// status rollup.
///
/// One census of a large project costs whole seconds (6.2 s for cairn's thread
/// rollup on a production clone, 0.9 s for its issue rollup), and agent writes
/// arrive several times a second, so a rollup that rebuilt per write would never
/// finish one census before the next was owed. Every input goes through this
/// floor, lifecycle transitions included: the cache schedules the trailing
/// rebuild itself and announces what it publishes, so the last write of a burst
/// reaches the sidebar within one floor rather than waiting for a reader that
/// may never come.
pub(crate) const STATUS_ROLLUP_FLOOR: Duration = Duration::from_secs(2);

/// The same bound for the issue list. Shorter, because a new or retitled issue
/// is something a person just did and is waiting to see.
pub(crate) const ISSUE_LIST_FLOOR: Duration = Duration::from_secs(1);

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
/// `tenets` is here because a thread row badges the proposals it has awaiting
/// the operator's verdict, and an answer given anywhere must clear that badge.
/// Every tenets frame carries `projectId` (`tenets::emit_changed`), so an answer
/// rebuilds only its own project's rollup.
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
    "tenets",
];

/// The tables `list_issues` reads: the issue rows themselves, their labels, and
/// the gate census each row carries (`executions`, `execution_gates`, the
/// supersession audit, and the project key a won't-start reason names).
///
/// `labels` and `projects` are workspace-level rows: renaming a label or a
/// project key moves every list that shows it, so an unscoped frame for either
/// is the honest one. Author display reads `executor_enrollments` in the local
/// database, which is equally project-agnostic and rare.
///
/// Transcript tables are absent on purpose. Nothing an agent writes while it
/// works changes an issue row until the work ends, and that end is an `issues`
/// or `executions` write.
pub(crate) const ISSUE_LIST_INPUT_TABLES: &[&str] = &[
    "issues",
    "issue_labels",
    "labels",
    "executions",
    "execution_gates",
    "execution_supersessions",
    "projects",
    "executor_enrollments",
];

/// Inputs whose rows are not owned by any one project, so a `db-change` for
/// them legitimately carries no `projectId` and invalidates every key.
#[cfg(test)]
pub(crate) const WORKSPACE_PROJECTION_INPUT_TABLES: &[&str] =
    &["labels", "projects", "executor_enrollments"];

/// Every table some sidebar projection reads, deduplicated.
///
/// The source-scan test over emit sites reads this rather than keeping its own
/// copy, so adding an input here is what extends the scoping requirement to it.
#[cfg(test)]
pub(crate) fn every_projection_input_table() -> Vec<&'static str> {
    let mut tables: Vec<&'static str> = THREAD_STATUS_INPUT_TABLES
        .iter()
        .chain(ISSUE_STATUS_INPUT_TABLES)
        .chain(ISSUE_LIST_INPUT_TABLES)
        .copied()
        .collect();
    tables.sort_unstable();
    tables.dedup();
    tables
}

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
    issue_list: Arc<IssueListCache>,
}

impl ProjectionInvalidatingEmitter {
    pub(crate) fn new(
        inner: Arc<dyn EventEmitter>,
        thread_status: Arc<ThreadStatusCache>,
        issue_status: Arc<IssueStatusCache>,
        issue_list: Arc<IssueListCache>,
    ) -> Self {
        Self {
            inner,
            thread_status,
            issue_status,
            issue_list,
        }
    }

    fn note(&self, payload: &Value) {
        let Some(table) = payload.get("table").and_then(Value::as_str) else {
            return;
        };
        if table == "team_sync" {
            self.note_team_sync(payload);
            return;
        }
        let thread_input = THREAD_STATUS_INPUT_TABLES.contains(&table);
        let issue_input = ISSUE_STATUS_INPUT_TABLES.contains(&table);
        let list_input = ISSUE_LIST_INPUT_TABLES.contains(&table);
        if !thread_input && !issue_input && !list_input {
            return;
        }
        let Some(project_id) = payload
            .get("projectId")
            .or_else(|| payload.get("project_id"))
            .and_then(Value::as_str)
        else {
            // A scope-less emit is a workspace-level row or a multi-row
            // recovery sweep, either of which really can have moved any
            // project. Rebuilding all of them is the honest answer; the
            // alternative is serving a snapshot the write invalidated.
            if thread_input {
                self.thread_status
                    .invalidate_matching("thread-input-unscoped", |_| true);
            }
            if issue_input {
                self.issue_status
                    .invalidate_matching("issue-input-unscoped", |_| true);
            }
            if list_input {
                self.issue_list
                    .invalidate_matching("issue-list-input-unscoped", |_| true);
            }
            return;
        };
        if thread_input && self.touches_a_thread(project_id, table, payload) {
            self.thread_status.invalidate(project_id, "thread-input");
        }
        if issue_input && self.touches_an_issue(project_id, table, payload) {
            self.issue_status.invalidate(project_id, "issue-input");
        }
        if list_input {
            self.issue_list.invalidate(project_id, "issue-list-input");
        }
    }

    /// A team pull applies a batch of remote rows at once and announces it with
    /// one frame naming the projects it reconciled, not one frame per table, so
    /// any of the three projections may have moved in any of those projects.
    fn note_team_sync(&self, payload: &Value) {
        let projects: Option<Vec<&str>> = payload
            .get("projectIds")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(Value::as_str).collect());
        match projects {
            Some(projects) => {
                for project_id in projects {
                    self.thread_status.invalidate(project_id, "team-sync");
                    self.issue_status.invalidate(project_id, "team-sync");
                    self.issue_list.invalidate(project_id, "team-sync");
                }
            }
            None => {
                self.thread_status
                    .invalidate_matching("team-sync", |_| true);
                self.issue_status.invalidate_matching("team-sync", |_| true);
                self.issue_list.invalidate_matching("team-sync", |_| true);
            }
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
            // A tenet hangs off its proposing thread, not a job, so there is no
            // job set to place it against.
            "threads" | "tenets" => true,
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
}

impl<V> Default for GenerationCacheState<V> {
    fn default() -> Self {
        Self {
            generations: HashMap::new(),
            cells: HashMap::new(),
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
    /// when nothing is. A live read's window is measured from here.
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

/// Rebuild one key's projection from scratch. Owned by the cache rather than
/// passed per read, because a cadenced rebuild runs with nobody waiting on it.
pub(crate) type Rebuild<V> =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<V, String>> + Send>> + Send + Sync>;

/// Tell readers that a key's answer changed and is worth asking for again.
pub(crate) type Announce = Arc<dyn Fn(&str) + Send + Sync>;

/// Box an async rebuild into a [`Rebuild`].
pub(crate) fn rebuild_with<V, F, Fut>(rebuild: F) -> Rebuild<V>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<V, String>> + Send + 'static,
{
    Arc::new(
        move |key| -> Pin<Box<dyn Future<Output = Result<V, String>> + Send>> {
            Box::pin(rebuild(key))
        },
    )
}

/// How long after its last read a key keeps being rebuilt in the background.
///
/// A collapsed sidebar project stops reading its projections, and rebuilding
/// them anyway would spend whole-project censuses on answers nobody shows.
/// Past this horizon a dirty key is left dirty; the first invalidation that
/// finds it so announces once, which is what brings a mounted-but-quiet reader
/// back. That reader is served what is published and starts the rebuild, whose
/// own announcement brings the fresh answer.
const READ_HORIZON: Duration = Duration::from_secs(60);

struct Published<V> {
    value: V,
    /// The generation the rebuild that produced this began at. The key is dirty
    /// while its current generation is ahead of it.
    generation: u64,
    at: tokio::time::Instant,
}

struct Slot<V> {
    generation: u64,
    published: Option<Published<V>>,
    /// A driver task owns this key's rebuilds. At most one per key, which is
    /// what serializes them.
    driving: bool,
    /// The next rebuild skips the floor. Set only by [`CadencedCache::invalidate_now`].
    urgent: bool,
    last_read: Option<tokio::time::Instant>,
    /// The idle announcement for the current stretch of staleness was sent.
    idle_announced: bool,
    /// Readers with nothing usable to serve, waiting for the next rebuild.
    waiters: Vec<tokio::sync::oneshot::Sender<Result<V, String>>>,
    /// Cuts short a driver waiting out the floor when something arrives that
    /// may not wait: an urgent invalidation, or a reader with nothing to serve.
    wake: Arc<tokio::sync::Notify>,
    /// How long the last rebuild took. The next one waits at least this long
    /// after a publish, so a rebuild slower than the floor still occupies a
    /// worker at most half the time.
    last_duration: Duration,
}

impl<V> Default for Slot<V> {
    fn default() -> Self {
        Self {
            generation: 0,
            published: None,
            driving: false,
            urgent: false,
            last_read: None,
            idle_announced: false,
            waiters: Vec::new(),
            wake: Arc::new(tokio::sync::Notify::new()),
            last_duration: Duration::ZERO,
        }
    }
}

impl<V> Slot<V> {
    fn dirty(&self) -> bool {
        self.published
            .as_ref()
            .is_none_or(|published| published.generation < self.generation)
    }

    fn hot(&self, now: tokio::time::Instant) -> bool {
        self.last_read
            .is_some_and(|read| now.duration_since(read) < READ_HORIZON)
    }

    fn owes_rebuild(&self, now: tokio::time::Instant) -> bool {
        self.dirty() && (self.urgent || !self.waiters.is_empty() || self.hot(now))
    }
}

/// A per-key projection the cache keeps current on its own cadence.
///
/// [`GenerationCache`] fences a value against invalidations, but leaves WHEN to
/// rebuild to its readers: every reader of a cold generation waits for a
/// rebuild, and a rebuild that an invalidation overtakes is thrown away and run
/// again. For a projection that costs seconds and is invalidated several times
/// a second, that is a reader riding superseded rebuilds indefinitely, which is
/// how a sidebar read came to take 60 s during a merge wave.
///
/// Here the backend owns cadence and freshness:
///
/// - A reader is served the last published value at once whenever one exists.
///   It waits only when nothing has ever been published.
/// - An invalidation marks the key dirty. A rebuild starts immediately if the
///   key has been quiet for a floor (the leading edge) and otherwise at
///   `published + max(floor, last rebuild's duration)` (the trailing edge), so
///   a key rebuilds at most once per floor however fast it is written, and a
///   rebuild slower than the floor still leaves its worker idle half the time.
/// - A rebuild that spans an invalidation is published anyway. Rebuilds are
///   serialized per key, so it is never older than what it replaces, and the
///   key stays dirty so the next one follows within a floor. Nobody retries.
/// - When a rebuild publishes a value different from the last one (or the
///   first one), the cache announces it. That announcement is the only thing
///   readers refetch on.
///
/// Drivers run on one long-lived runtime the cache is bound to, never on the
/// caller's. Writes arrive from agent reader threads with no runtime at all and
/// from inside short-lived single-threaded runtimes that bridge sync code to
/// the database; a driver spawned on either would never start, or would be
/// dropped with it and leave the key believing it is being driven.
///
/// Driver tasks hold a [`Weak`] to the cache, so they never keep its owner
/// alive, and the rebuild closure is expected to hold only weak references too.
pub(crate) struct CadencedCache<V> {
    label: &'static str,
    floor: Duration,
    rebuild: Rebuild<V>,
    announce: Announce,
    this: Weak<Self>,
    slots: Mutex<HashMap<String, Slot<V>>>,
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    rebuilds: AtomicU64,
}

/// Hands a key back if its driver ends without doing so itself: cancelled with
/// its runtime, or unwound by a panicking rebuild. Releases the waiting readers
/// too, whose receivers then report the rebuild abandoned instead of hanging.
struct DriverGuard<V>
where
    V: Clone + PartialEq + Send + Sync + 'static,
{
    cache: Weak<CadencedCache<V>>,
    key: String,
    armed: bool,
}

impl<V> Drop for DriverGuard<V>
where
    V: Clone + PartialEq + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(cache) = self.cache.upgrade() else {
            return;
        };
        let Ok(mut slots) = cache.slots.lock() else {
            return;
        };
        if let Some(slot) = slots.get_mut(&self.key) {
            slot.driving = false;
            slot.waiters.clear();
        }
        log::warn!(
            "{} driver for key={} ended without settling; handed the key back",
            cache.label,
            self.key
        );
    }
}

impl<V> CadencedCache<V>
where
    V: Clone + PartialEq + Send + Sync + 'static,
{
    pub(crate) fn new(
        label: &'static str,
        floor: Duration,
        rebuild: Rebuild<V>,
        announce: Announce,
    ) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            label,
            floor,
            rebuild,
            announce,
            this: this.clone(),
            slots: Mutex::new(HashMap::new()),
            runtime: Mutex::new(tokio::runtime::Handle::try_current().ok()),
            rebuilds: AtomicU64::new(0),
        })
    }

    /// Run this cache's drivers on `runtime` from now on. The orchestrator binds
    /// its long-lived runtime here when it is built; construction captures the
    /// current runtime as a default.
    pub(crate) fn bind_runtime(&self, runtime: tokio::runtime::Handle) {
        if let Ok(mut bound) = self.runtime.lock() {
            *bound = Some(runtime);
        }
    }

    /// Where to spawn a driver: the bound runtime, or failing that a current
    /// multi-threaded one. Never a current-thread runtime nobody bound, which is
    /// how this codebase bridges sync code to async for the length of one
    /// `block_on` and then drops.
    fn runtime(&self) -> Option<tokio::runtime::Handle> {
        if let Some(bound) = self.runtime.lock().ok().and_then(|bound| bound.clone()) {
            return Some(bound);
        }
        tokio::runtime::Handle::try_current()
            .ok()
            .filter(|runtime| {
                runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
            })
    }

    /// The value last published for a key, dirty or not, without computing one.
    ///
    /// For callers deciding whether a write could touch what is published. A key
    /// with nothing published answers `None`, which such a caller must read as
    /// "cannot tell" rather than as "empty".
    pub(crate) fn published(&self, key: &str) -> Option<V> {
        let slots = self.slots.lock().ok()?;
        Some(slots.get(key)?.published.as_ref()?.value.clone())
    }

    /// The current correctness generation for a key.
    #[cfg(test)]
    pub(crate) fn generation(&self, key: &str) -> u64 {
        self.slots
            .lock()
            .ok()
            .and_then(|slots| slots.get(key).map(|slot| slot.generation))
            .unwrap_or_default()
    }

    /// How many rebuilds this cache has started, across every key.
    #[cfg(test)]
    pub(crate) fn rebuilds(&self) -> u64 {
        self.rebuilds.load(Ordering::Relaxed)
    }

    /// Record that a committed write moved one of this key's inputs. Call only
    /// after the write commits, or a rebuild may read the row before it lands.
    pub(crate) fn invalidate(&self, key: &str, reason: &'static str) {
        self.advance(key, reason, false);
    }

    /// The same, rebuilding at once regardless of the floor.
    ///
    /// Reserved for a person's own acknowledgement (marking a thread viewed),
    /// whose effect they are looking at as they make it. Every other write, the
    /// important ones included, goes through the floor: importance is not what
    /// the floor protects against, rate is.
    pub(crate) fn invalidate_now(&self, key: &str, reason: &'static str) {
        self.advance(key, reason, true);
    }

    /// Invalidate every key the predicate accepts. For workspace-level rows and
    /// recovery sweeps that really can have moved any key.
    pub(crate) fn invalidate_matching(
        &self,
        reason: &'static str,
        predicate: impl Fn(&str) -> bool,
    ) {
        let keys: Vec<String> = self
            .slots
            .lock()
            .map(|slots| slots.keys().filter(|key| predicate(key)).cloned().collect())
            .unwrap_or_default();
        for key in keys {
            self.invalidate(&key, reason);
        }
    }

    /// Announce every key whose published value satisfies `predicate`, without
    /// rebuilding anything.
    ///
    /// For a fact a reader decorates onto the published value at read time, which
    /// therefore changes without the value itself changing.
    pub(crate) fn announce_where(&self, predicate: impl Fn(&V) -> bool) {
        let keys: Vec<String> = self
            .slots
            .lock()
            .map(|slots| {
                slots
                    .iter()
                    .filter(|(_, slot)| {
                        slot.published
                            .as_ref()
                            .is_some_and(|published| predicate(&published.value))
                    })
                    .map(|(key, _)| key.clone())
                    .collect()
            })
            .unwrap_or_default();
        for key in keys {
            (self.announce)(&key);
        }
    }

    fn advance(&self, key: &str, reason: &'static str, urgent: bool) {
        let announce_idle = {
            let Ok(mut slots) = self.slots.lock() else {
                return;
            };
            let slot = slots.entry(key.to_string()).or_default();
            slot.generation += 1;
            if urgent && slot.published.is_some() {
                slot.urgent = true;
                slot.wake.notify_one();
            }
            log::debug!(
                "{} cache invalidated key={key} generation={} reason={reason}",
                self.label,
                slot.generation
            );
            if slot.owes_rebuild(tokio::time::Instant::now()) {
                self.drive_locked(key, slot);
                false
            } else if slot.published.is_some() && !slot.idle_announced {
                // Nobody is reading this key, so it is not rebuilt. Saying once
                // that it went stale is what lets a reader that is still
                // mounted, but has had no reason to ask lately, come back.
                slot.idle_announced = true;
                true
            } else {
                false
            }
        };
        if announce_idle {
            (self.announce)(key);
        }
    }

    /// Start the key's driver on the bound runtime unless one is already
    /// running. With no runtime to run it on, nothing is started and the key
    /// stays dirty for the next caller that has one.
    fn drive_locked(&self, key: &str, slot: &mut Slot<V>) {
        if slot.driving {
            return;
        }
        let Some(runtime) = self.runtime() else {
            log::warn!(
                "{} has no runtime to rebuild key={key} on; it stays dirty",
                self.label
            );
            return;
        };
        slot.driving = true;
        runtime.spawn(Self::drive(self.this.clone(), key.to_string()));
    }

    /// The key's current answer.
    ///
    /// Served from what is published whenever anything is, dirty or not, and a
    /// dirty key's read starts its rebuild, whose announcement brings the fresh
    /// answer. Only a key with nothing published makes its reader wait, and
    /// every reader waiting on one key shares one rebuild.
    pub(crate) async fn get(&self, key: &str) -> Result<V, String> {
        let waiter = {
            let mut slots = self
                .slots
                .lock()
                .map_err(|_| format!("{} cache lock poisoned", self.label))?;
            let slot = slots.entry(key.to_string()).or_default();
            let now = tokio::time::Instant::now();
            slot.last_read = Some(now);
            slot.idle_announced = false;
            if let Some(published) = &slot.published {
                let value = published.value.clone();
                if slot.owes_rebuild(now) {
                    self.drive_locked(key, slot);
                }
                return Ok(value);
            }
            let (sender, receiver) = tokio::sync::oneshot::channel();
            slot.waiters.push(sender);
            slot.wake.notify_one();
            self.drive_locked(key, slot);
            receiver
        };
        waiter
            .await
            .map_err(|_| format!("{} rebuild was abandoned", self.label))?
    }

    /// One key's rebuild loop: wait out the floor, rebuild, publish, announce
    /// a change, and go again for as long as the key owes a rebuild.
    async fn drive(this: Weak<Self>, key: String) {
        let mut guard = DriverGuard {
            cache: this.clone(),
            key: key.clone(),
            armed: true,
        };
        loop {
            let Some(cache) = this.upgrade() else {
                return;
            };
            let (due, wake) = {
                let Ok(mut slots) = cache.slots.lock() else {
                    return;
                };
                let Some(slot) = slots.get_mut(&key) else {
                    return;
                };
                let now = tokio::time::Instant::now();
                if !slot.owes_rebuild(now) {
                    slot.driving = false;
                    guard.armed = false;
                    return;
                }
                let due = match &slot.published {
                    Some(published) if !slot.urgent && slot.waiters.is_empty() => {
                        published.at + cache.floor.max(slot.last_duration)
                    }
                    _ => now,
                };
                (due, slot.wake.clone())
            };
            drop(cache);
            if due > tokio::time::Instant::now() {
                tokio::select! {
                    () = tokio::time::sleep_until(due) => {}
                    // Something that may not wait arrived; decide again.
                    () = wake.notified() => continue,
                }
            }

            let Some(cache) = this.upgrade() else {
                return;
            };
            let generation = {
                let Ok(mut slots) = cache.slots.lock() else {
                    return;
                };
                let Some(slot) = slots.get_mut(&key) else {
                    return;
                };
                slot.urgent = false;
                slot.generation
            };
            cache.rebuilds.fetch_add(1, Ordering::Relaxed);
            let rebuild = (cache.rebuild)(key.clone());
            let label = cache.label;
            drop(cache);
            let started = tokio::time::Instant::now();
            let outcome = rebuild.await;
            let duration = started.elapsed();
            let duration_ms = duration.as_millis();

            let Some(cache) = this.upgrade() else {
                return;
            };
            let (waiters, changed) = {
                let Ok(mut slots) = cache.slots.lock() else {
                    return;
                };
                let Some(slot) = slots.get_mut(&key) else {
                    return;
                };
                slot.last_duration = duration;
                let changed = match &outcome {
                    // The first value published is a change too: a reader whose
                    // cold read failed has nothing else to bring it back.
                    Ok(value) => {
                        let changed = slot
                            .published
                            .as_ref()
                            .is_none_or(|published| published.value != *value);
                        slot.published = Some(Published {
                            value: value.clone(),
                            generation,
                            at: tokio::time::Instant::now(),
                        });
                        changed
                    }
                    // A failed rebuild is not a fact about the projection.
                    // Stop driving rather than retrying in a loop; the next
                    // write or read starts again.
                    Err(_) => {
                        slot.driving = false;
                        guard.armed = false;
                        false
                    }
                };
                (std::mem::take(&mut slot.waiters), changed)
            };
            match &outcome {
                Ok(_) => log::info!(
                    "{label} computed key={key} generation={generation} duration_ms={duration_ms} published changed={changed}"
                ),
                Err(error) => log::warn!(
                    "{label} rebuild failed key={key} generation={generation} duration_ms={duration_ms}: {error}"
                ),
            }
            let failed = outcome.is_err();
            for waiter in waiters {
                let _ = waiter.send(outcome.clone());
            }
            if changed {
                (cache.announce)(&key);
            }
            if failed {
                return;
            }
        }
    }

    /// Wait until the key owes nothing and no rebuild is running, so a test
    /// can assert on the settled state of a cache driven by real writes.
    #[cfg(test)]
    pub(crate) async fn settle(&self, key: &str) {
        // Generous: a paused-clock test may wait out a long floor here.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        loop {
            let busy = self.slots.lock().is_ok_and(|slots| {
                slots.get(key).is_some_and(|slot| {
                    slot.driving || slot.owes_rebuild(tokio::time::Instant::now())
                })
            });
            if !busy {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} key {key} never settled",
                self.label
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::CapturingEmitter;
    use std::collections::HashSet;
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

    // ── The cadenced cache ──

    /// A cadenced cache over one number, with its rebuild count, the number a
    /// rebuild will publish, an optional rebuild duration, and every
    /// announcement it makes.
    struct Cadenced {
        cache: Arc<CadencedCache<u64>>,
        value: Arc<AtomicU64>,
        delay: Arc<AtomicU64>,
        announced: Arc<Mutex<Vec<String>>>,
    }

    fn cadenced(floor: Duration) -> Cadenced {
        let value = Arc::new(AtomicU64::new(1));
        let delay = Arc::new(AtomicU64::new(0));
        let announced = Arc::new(Mutex::new(Vec::new()));
        let cache = CadencedCache::new(
            "test",
            floor,
            rebuild_with({
                let value = value.clone();
                let delay = delay.clone();
                move |_key| {
                    let observed = value.load(Ordering::SeqCst);
                    let delay = Duration::from_millis(delay.load(Ordering::SeqCst));
                    async move {
                        tokio::time::sleep(delay).await;
                        Ok(observed)
                    }
                }
            }),
            recording(&announced, "test"),
        );
        Cadenced {
            cache,
            value,
            delay,
            announced,
        }
    }

    fn recording(log: &Arc<Mutex<Vec<String>>>, projection: &'static str) -> Announce {
        let log = log.clone();
        Arc::new(move |key: &str| log.lock().unwrap().push(format!("{projection}:{key}")))
    }

    /// The property the whole mode exists for: a key written continuously
    /// rebuilds at the floor's rate, not the writer's.
    #[tokio::test(start_paused = true)]
    async fn a_write_stream_rebuilds_at_most_once_per_floor() {
        let floor = Duration::from_secs(2);
        let c = cadenced(floor);
        c.cache.get("k").await.unwrap();

        // 100 writes over 10 seconds, with the key being read throughout.
        for n in 0..100u64 {
            c.value.store(n, Ordering::SeqCst);
            c.cache.invalidate("k", "test");
            tokio::time::sleep(Duration::from_millis(100)).await;
            c.cache.get("k").await.unwrap();
        }
        c.cache.settle("k").await;

        let rebuilds = c.cache.rebuilds();
        assert!(
            rebuilds <= 1 + 10 / 2 + 1,
            "one cold build plus at most one per floor, got {rebuilds}"
        );
        assert_eq!(
            c.cache.published("k"),
            Some(99),
            "and the last write is what ends up published"
        );
    }

    /// Readers never wait on a rebuild that has something to replace. This is
    /// the 60-second sidebar call: callers that joined a census and then rode
    /// every superseded one after it.
    #[tokio::test(start_paused = true)]
    async fn readers_during_a_rebuild_are_served_the_last_snapshot_at_once() {
        let floor = Duration::from_secs(2);
        let c = cadenced(floor);
        c.cache.get("k").await.unwrap();
        tokio::time::sleep(floor).await;

        c.delay.store(1_000, Ordering::SeqCst);
        c.value.store(2, Ordering::SeqCst);
        c.cache.invalidate("k", "test");
        tokio::task::yield_now().await;

        let started = tokio::time::Instant::now();
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = c.cache.clone();
            readers.push(tokio::spawn(async move { cache.get("k").await }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Ok(1));
        }
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "no reader waited for the rebuild in flight"
        );

        c.cache.settle("k").await;
        assert_eq!(
            c.cache.rebuilds(),
            2,
            "the readers started no rebuild of their own"
        );
        assert_eq!(c.cache.published("k"), Some(2));
    }

    /// Cold readers have nothing to be served, so they wait, but on one shared
    /// rebuild.
    #[tokio::test(start_paused = true)]
    async fn cold_readers_share_one_rebuild() {
        let c = cadenced(Duration::from_secs(2));
        c.delay.store(500, Ordering::SeqCst);
        let mut readers = Vec::new();
        for _ in 0..16 {
            let cache = c.cache.clone();
            readers.push(tokio::spawn(async move { cache.get("k").await }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Ok(1));
        }
        assert_eq!(c.cache.rebuilds(), 1);
    }

    /// A rebuild an invalidation overtakes is published, not discarded, and the
    /// write that overtook it reaches the next rebuild within a floor. Nobody
    /// retries.
    #[tokio::test(start_paused = true)]
    async fn a_rebuild_that_spans_an_invalidation_is_published_and_followed() {
        let floor = Duration::from_secs(2);
        let c = cadenced(floor);
        c.cache.get("k").await.unwrap();
        tokio::time::sleep(floor).await;

        c.delay.store(1_000, Ordering::SeqCst);
        c.value.store(2, Ordering::SeqCst);
        c.cache.invalidate("k", "test");
        tokio::time::sleep(Duration::from_millis(500)).await;
        c.value.store(3, Ordering::SeqCst);
        c.cache.invalidate("k", "test");

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            c.cache.published("k"),
            Some(2),
            "the overtaken rebuild is published rather than retried"
        );
        let first_publish = tokio::time::Instant::now();

        c.cache.settle("k").await;
        assert_eq!(c.cache.published("k"), Some(3));
        assert_eq!(c.cache.rebuilds(), 3);
        assert!(first_publish.elapsed() <= floor + Duration::from_secs(1));
    }

    /// Only a changed answer is announced; a rebuild that arrives at the same
    /// value says nothing, so the frontend has nothing to refetch.
    #[tokio::test(start_paused = true)]
    async fn only_a_changed_value_is_announced() {
        let c = cadenced(Duration::from_secs(2));
        c.cache.get("k").await.unwrap();
        assert_eq!(
            *c.announced.lock().unwrap(),
            vec!["test:k".to_string()],
            "the first value published is a change"
        );
        c.announced.lock().unwrap().clear();

        c.cache.invalidate("k", "test");
        c.cache.settle("k").await;
        assert_eq!(c.cache.rebuilds(), 2);
        assert!(c.announced.lock().unwrap().is_empty());

        c.value.store(2, Ordering::SeqCst);
        c.cache.invalidate("k", "test");
        c.cache.settle("k").await;
        assert_eq!(*c.announced.lock().unwrap(), vec!["test:k".to_string()]);
    }

    /// A key nobody has read for the horizon is not rebuilt in the background.
    /// It says once that it went stale, which is what brings a mounted reader
    /// back; that reader is served at once and its rebuild announces the fresh
    /// answer, rather than holding the reader for a whole census.
    #[tokio::test(start_paused = true)]
    async fn an_unread_key_is_not_rebuilt_and_announces_once_that_it_went_stale() {
        let c = cadenced(Duration::from_secs(2));
        c.cache.get("k").await.unwrap();
        c.announced.lock().unwrap().clear();
        tokio::time::sleep(READ_HORIZON + Duration::from_secs(1)).await;

        c.value.store(2, Ordering::SeqCst);
        for _ in 0..3 {
            c.cache.invalidate("k", "test");
        }
        c.cache.settle("k").await;
        assert_eq!(c.cache.rebuilds(), 1, "nothing rebuilt for nobody");
        assert_eq!(*c.announced.lock().unwrap(), vec!["test:k".to_string()]);

        c.delay.store(5_000, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        assert_eq!(c.cache.get("k").await, Ok(1), "served what is published");
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "without waiting a census"
        );
        c.cache.settle("k").await;
        assert_eq!(c.cache.published("k"), Some(2));
        assert_eq!(c.cache.rebuilds(), 2);
        assert_eq!(
            *c.announced.lock().unwrap(),
            vec!["test:k".to_string(), "test:k".to_string()],
            "once for going stale, once for the fresh answer"
        );
    }

    /// A rebuild slower than the floor is not followed by another the moment it
    /// publishes: the next waits as long as the last one took, so a census that
    /// costs seconds occupies its worker at most half the time.
    #[tokio::test(start_paused = true)]
    async fn a_slow_rebuild_waits_its_own_duration_before_the_next() {
        let c = cadenced(Duration::from_secs(1));
        c.delay.store(4_000, Ordering::SeqCst);
        c.cache.get("k").await.unwrap();

        c.cache.invalidate("k", "test");
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(c.cache.rebuilds(), 1, "not yet: the last rebuild took 4 s");
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(c.cache.rebuilds(), 2);
    }

    /// Agent output is handled on plain threads with no runtime. A write from
    /// one, the last of a turn, must still be rebuilt and announced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_from_a_thread_with_no_runtime_is_rebuilt_and_announced() {
        let c = cadenced(Duration::from_millis(10));
        c.cache.get("k").await.unwrap();
        // The first publish announces just after its reader is answered.
        c.cache.settle("k").await;
        c.announced.lock().unwrap().clear();

        c.value.store(2, Ordering::SeqCst);
        let cache = c.cache.clone();
        std::thread::spawn(move || cache.invalidate("k", "reader-thread"))
            .join()
            .unwrap();
        c.cache.settle("k").await;

        assert_eq!(c.cache.published("k"), Some(2));
        assert_eq!(*c.announced.lock().unwrap(), vec!["test:k".to_string()]);
    }

    /// Sync code reaches the database through a short-lived current-thread
    /// runtime and emits from inside it. A driver spawned there would be
    /// dropped with it and leave the key believing it was being driven.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_inside_a_short_lived_runtime_does_not_wedge_the_key() {
        let c = cadenced(Duration::from_millis(10));
        c.cache.get("k").await.unwrap();
        c.cache.settle("k").await;

        c.value.store(2, Ordering::SeqCst);
        let cache = c.cache.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async { cache.invalidate("k", "bridged-write") });
        })
        .join()
        .unwrap();
        c.cache.settle("k").await;
        assert_eq!(c.cache.published("k"), Some(2));

        // And the key keeps working afterwards.
        c.value.store(3, Ordering::SeqCst);
        c.cache.invalidate("k", "later");
        c.cache.settle("k").await;
        assert_eq!(c.cache.published("k"), Some(3));
    }

    /// A rebuild that panics hands the key back: its waiting readers are told
    /// it was abandoned instead of hanging, and the next read starts afresh.
    #[tokio::test(start_paused = true)]
    async fn a_panicking_rebuild_hands_the_key_back() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let cache: Arc<CadencedCache<u64>> = CadencedCache::new(
            "test",
            Duration::from_secs(2),
            rebuild_with({
                let attempts = attempts.clone();
                move |_key| {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        assert!(attempt > 0, "the first rebuild panics");
                        Ok(5)
                    }
                }
            }),
            Arc::new(|_key: &str| {}),
        );
        let first = cache.get("k").await;
        assert!(
            first.is_err(),
            "the waiting reader is released, got {first:?}"
        );
        assert_eq!(cache.get("k").await, Ok(5));
    }

    /// The acknowledgement path skips the floor, even when a trailing rebuild is
    /// already waiting it out.
    #[tokio::test(start_paused = true)]
    async fn invalidate_now_skips_the_floor() {
        let c = cadenced(Duration::from_secs(30));
        c.cache.get("k").await.unwrap();
        c.cache.invalidate("k", "test");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(c.cache.rebuilds(), 1, "the write waits out the floor");

        c.value.store(2, Ordering::SeqCst);
        c.cache.invalidate_now("k", "thread-read-position");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(c.cache.rebuilds(), 2);
        assert_eq!(c.cache.published("k"), Some(2));
    }

    /// A failed rebuild is not a fact: waiting readers get the error, nothing is
    /// published, and the next read tries again.
    #[tokio::test(start_paused = true)]
    async fn a_failed_rebuild_reaches_its_readers_and_is_not_published() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let cache: Arc<CadencedCache<u64>> = CadencedCache::new(
            "test",
            Duration::from_secs(2),
            rebuild_with({
                let attempts = attempts.clone();
                move |_key| {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            Err("transient".to_string())
                        } else {
                            Ok(5)
                        }
                    }
                }
            }),
            Arc::new(|_key: &str| {}),
        );
        assert_eq!(cache.get("k").await, Err("transient".to_string()));
        assert_eq!(cache.published("k"), None);
        assert_eq!(cache.get("k").await, Ok(5));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    /// A driver waiting out the floor holds the cache weakly, so dropping its
    /// owner frees it rather than leaving a task to keep it alive.
    #[tokio::test(start_paused = true)]
    async fn a_waiting_rebuild_does_not_keep_the_cache_alive() {
        let c = cadenced(Duration::from_secs(30));
        c.cache.get("k").await.unwrap();
        c.cache.invalidate("k", "test");
        let weak = Arc::downgrade(&c.cache);
        drop(c);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(weak.upgrade().is_none());
    }

    // ── The emitter ──

    /// The three sidebar caches wired to the emitter exactly as the
    /// orchestrator wires them. Each status rebuild publishes whichever job set
    /// the test last put in place, so a test can say what a snapshot "read".
    struct Sidebar {
        emitter: ProjectionInvalidatingEmitter,
        threads: Arc<ThreadStatusCache>,
        issues: Arc<IssueStatusCache>,
        list: Arc<IssueListCache>,
        thread_jobs: Arc<Mutex<HashSet<String>>>,
        issue_jobs: Arc<Mutex<HashSet<String>>>,
        announced: Arc<Mutex<Vec<String>>>,
    }

    fn job_set(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    fn sidebar(floor: Duration) -> Sidebar {
        let thread_jobs = Arc::new(Mutex::new(HashSet::new()));
        let issue_jobs = Arc::new(Mutex::new(HashSet::new()));
        let announced = Arc::new(Mutex::new(Vec::new()));
        let threads = ThreadStatusCache::new(
            "test-threads",
            floor,
            rebuild_with({
                let jobs = thread_jobs.clone();
                move |_key| {
                    let thread_job_ids = jobs.lock().unwrap().clone();
                    async move {
                        Ok(Arc::new(ThreadStatusSnapshot {
                            indicators: Vec::new(),
                            thread_job_ids,
                        }))
                    }
                }
            }),
            recording(&announced, "thread_status"),
        );
        let issues = IssueStatusCache::new(
            "test-issues",
            floor,
            rebuild_with({
                let jobs = issue_jobs.clone();
                move |_key| {
                    let job_ids = jobs.lock().unwrap().clone();
                    async move {
                        Ok(Arc::new(IssueStatusSnapshot {
                            indicators: Vec::new(),
                            job_ids,
                        }))
                    }
                }
            }),
            recording(&announced, "issue_status"),
        );
        let list = IssueListCache::new(
            "test-list",
            floor,
            rebuild_with(|_key| async { Ok(EncodedIssueList::from_json("[]")) }),
            recording(&announced, "issue_list"),
        );
        let emitter = ProjectionInvalidatingEmitter::new(
            Arc::new(CapturingEmitter::new()),
            threads.clone(),
            issues.clone(),
            list.clone(),
        );
        Sidebar {
            emitter,
            threads,
            issues,
            list,
            thread_jobs,
            issue_jobs,
            announced,
        }
    }

    impl Sidebar {
        fn emit(&self, payload: Value) {
            self.emitter.emit("db-change", payload).unwrap();
        }

        async fn settle(&self, key: &str) {
            self.threads.settle(key).await;
            self.issues.settle(key).await;
            self.list.settle(key).await;
        }
    }

    fn events_frame(project: &str, job: &str) -> Value {
        serde_json::json!({
            "table": "events",
            "action": "insert",
            "projectId": project,
            "jobId": job,
        })
    }

    /// The two status rollups read overlapping tables but disjoint rows, so one
    /// project's thread churn must not rebuild its issue rollup, and vice versa.
    #[tokio::test(start_paused = true)]
    async fn the_two_sidebar_rollups_do_not_rebuild_each_other() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.thread_jobs.lock().unwrap() = job_set(&["thread-job"]);
        *s.issue_jobs.lock().unwrap() = job_set(&["issue-job"]);
        s.threads.get("proj").await.unwrap();
        s.issues.get("proj").await.unwrap();

        s.emit(serde_json::json!({"table": "turns", "projectId": "proj", "jobId": "thread-job"}));
        s.emit(serde_json::json!({"table": "turns", "projectId": "proj", "jobId": "issue-job"}));
        s.settle("proj").await;

        assert_eq!(
            s.threads.rebuilds(),
            2,
            "the thread rollup rebuilt for its own job only"
        );
        assert_eq!(s.issues.rebuilds(), 2, "and the issue rollup for its own");
    }

    /// The issue rollup reads no transcript, so the workspace's highest-volume
    /// table cannot reach it at all.
    #[tokio::test(start_paused = true)]
    async fn transcript_events_never_rebuild_the_issue_rollup() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.issue_jobs.lock().unwrap() = job_set(&["issue-job"]);
        s.issues.get("proj").await.unwrap();
        let generation = s.issues.generation("proj");

        for _ in 0..200 {
            s.emit(events_frame("proj", "issue-job"));
        }
        s.settle("proj").await;

        assert_eq!(s.issues.rebuilds(), 1);
        assert_eq!(s.issues.generation("proj"), generation);
    }

    /// The defect CAIRN-4331 fixed, asserted as work done: an agent writing a
    /// transcript on an ISSUE cannot make the thread sidebar recount anything.
    #[tokio::test(start_paused = true)]
    async fn an_issue_nodes_transcript_cannot_rebuild_the_thread_rollup() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.thread_jobs.lock().unwrap() = job_set(&["thread-job"]);
        s.threads.get("proj").await.unwrap();
        let generation = s.threads.generation("proj");

        for _ in 0..500 {
            s.emit(events_frame("proj", "issue-node-job"));
        }
        s.settle("proj").await;

        assert_eq!(
            s.threads.rebuilds(),
            1,
            "500 transcript events on an issue node must cost zero thread censuses"
        );
        assert_eq!(s.threads.generation("proj"), generation);
    }

    /// A thread's OWN transcript does move its row, at the floor's rate.
    #[tokio::test(start_paused = true)]
    async fn a_threads_own_transcript_rebuilds_at_the_floors_rate() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.thread_jobs.lock().unwrap() = job_set(&["thread-job"]);
        s.threads.get("proj").await.unwrap();

        // 200 events over 10 seconds while the sidebar keeps reading.
        for _ in 0..200 {
            s.emit(events_frame("proj", "thread-job"));
            tokio::time::sleep(Duration::from_millis(50)).await;
            s.threads.get("proj").await.unwrap();
        }
        s.settle("proj").await;

        let rebuilds = s.threads.rebuilds();
        assert!(
            (2..=7).contains(&rebuilds),
            "got {rebuilds} rebuilds for 10 s of writes"
        );
    }

    /// The hazard that kept the old floor narrow, now closed for every input:
    /// the write that ENDS an agent's work is its last, and nothing reads after
    /// it. The cache rebuilds behind it on its own and announces the change, so
    /// the sidebar learns the work ended without having to ask.
    #[tokio::test(start_paused = true)]
    async fn the_last_write_of_a_burst_reaches_the_sidebar_without_a_read() {
        let s = sidebar(Duration::from_secs(30));
        *s.thread_jobs.lock().unwrap() = job_set(&["thread-job"]);
        s.threads.get("proj").await.unwrap();
        s.announced.lock().unwrap().clear();

        for _ in 0..50 {
            s.emit(events_frame("proj", "thread-job"));
        }
        // The job's final transition changes what the rollup reads.
        *s.thread_jobs.lock().unwrap() = job_set(&["thread-job", "follow-up"]);
        s.emit(serde_json::json!({"table": "turns", "projectId": "proj", "jobId": "thread-job"}));
        s.settle("proj").await;

        assert_eq!(
            s.threads.rebuilds(),
            2,
            "one trailing rebuild for the whole burst"
        );
        assert_eq!(
            s.threads.published("proj").unwrap().thread_job_ids,
            job_set(&["thread-job", "follow-up"])
        );
        assert_eq!(
            *s.announced.lock().unwrap(),
            vec!["thread_status:proj".to_string()]
        );
    }

    /// A job that joins a thread announces itself before it can write anything,
    /// which is what lets the published job set be trusted as a filter.
    #[tokio::test(start_paused = true)]
    async fn a_job_joining_a_thread_reaches_the_rollup_that_has_not_seen_it() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.thread_jobs.lock().unwrap() = job_set(&["old-job"]);
        s.threads.get("proj").await.unwrap();

        *s.thread_jobs.lock().unwrap() = job_set(&["old-job", "new-job"]);
        let before = s.threads.generation("proj");
        s.emit(serde_json::json!({
            "table": "jobs", "jobId": "new-job", "threadId": "th", "projectId": "proj",
        }));
        assert!(s.threads.generation("proj") > before);
        s.settle("proj").await;

        // Now that it is in the set, its transcript reaches the rollup too.
        let before = s.threads.generation("proj");
        s.emit(events_frame("proj", "new-job"));
        assert!(s.threads.generation("proj") > before);
    }

    /// An issue node's job insert is not a thread's business.
    #[tokio::test(start_paused = true)]
    async fn a_job_outside_any_thread_does_not_touch_the_thread_rollup() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        s.threads.get("proj").await.unwrap();
        let before = s.threads.generation("proj");
        s.emit(serde_json::json!({
            "table": "jobs", "jobId": "issue-node", "issueId": "i-1",
            "threadId": Value::Null, "projectId": "proj",
        }));
        assert_eq!(s.threads.generation("proj"), before);
    }

    /// A frame that cannot be placed against the job set counts as relevant.
    /// Skipping work is only safe where the projection is CERTAIN it is
    /// unnecessary.
    #[tokio::test(start_paused = true)]
    async fn an_unplaceable_frame_still_invalidates() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.thread_jobs.lock().unwrap() = job_set(&["thread-job"]);
        s.threads.get("proj").await.unwrap();
        let before = s.threads.generation("proj");
        s.emit(serde_json::json!({"table": "permission_requests", "projectId": "proj"}));
        assert!(s.threads.generation("proj") > before);
    }

    /// The dependency sets, asserted as behavior: each projection advances on
    /// its own input tables and on nothing else.
    ///
    /// Every frame here is deliberately unplaceable against a job set, so this
    /// asserts only which TABLES are inputs; which writes to those tables are
    /// relevant is the job-scoping contract above.
    #[test]
    fn each_projection_advances_on_its_own_inputs_only() {
        let advances = |table: &str| {
            let s = sidebar(STATUS_ROLLUP_FLOOR);
            let before = (
                s.threads.generation("proj"),
                s.issues.generation("proj"),
                s.list.generation("proj"),
            );
            s.emit(serde_json::json!({"table": table, "projectId": "proj"}));
            (
                s.threads.generation("proj") > before.0,
                s.issues.generation("proj") > before.1,
                s.list.generation("proj") > before.2,
            )
        };
        // (thread rollup, issue rollup, issue list)
        let expected = [
            ("threads", (true, false, false)),
            ("events", (true, false, false)),
            ("tenets", (true, false, false)),
            ("turns", (true, true, false)),
            ("jobs", (false, false, false)),
            ("prompts", (true, true, false)),
            ("permission_requests", (true, true, false)),
            ("runs", (true, true, false)),
            ("issues", (false, true, true)),
            ("executions", (false, true, true)),
            ("merge_requests", (false, true, false)),
            ("action_runs", (false, true, false)),
            ("artifacts", (false, true, false)),
            ("issue_labels", (false, false, true)),
            ("execution_gates", (false, false, true)),
            ("execution_supersessions", (false, false, true)),
            ("check_result_cache", (false, false, false)),
            ("comments", (false, false, false)),
        ];
        for (table, want) in expected {
            assert_eq!(advances(table), want, "{table}: (thread, issue, list)");
        }
    }

    /// A scoped input change touches only its own project.
    #[test]
    fn invalidation_is_scoped_by_project() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        s.threads.invalidate("other", "seed");
        s.list.invalidate("other", "seed");
        let (threads, list) = (s.threads.generation("other"), s.list.generation("other"));
        s.emit(serde_json::json!({"table": "turns", "projectId": "proj"}));
        s.emit(serde_json::json!({"table": "issues", "projectId": "proj"}));
        assert_eq!(s.threads.generation("other"), threads);
        assert_eq!(s.list.generation("other"), list);
    }

    /// A scope-less frame is a workspace row or a recovery sweep, which really
    /// can have moved any project, so it reaches all of them.
    #[test]
    fn an_unscoped_frame_reaches_every_project() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        s.threads.invalidate("a", "seed");
        s.threads.invalidate("b", "seed");
        s.list.invalidate("a", "seed");
        s.list.invalidate("b", "seed");
        let threads = (s.threads.generation("a"), s.threads.generation("b"));
        let lists = (s.list.generation("a"), s.list.generation("b"));

        s.emit(serde_json::json!({"table": "turns"}));
        s.emit(serde_json::json!({"table": "labels", "action": "update"}));

        assert!(s.threads.generation("a") > threads.0);
        assert!(s.threads.generation("b") > threads.1);
        assert!(s.list.generation("a") > lists.0);
        assert!(s.list.generation("b") > lists.1);
    }

    /// A team pull lands as one frame naming the projects it reconciled, and any
    /// of the three projections may have moved in each.
    #[test]
    fn a_team_pull_reaches_every_projection_of_the_projects_it_names() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        let caches_of = |key: &str| {
            (
                s.threads.generation(key),
                s.issues.generation(key),
                s.list.generation(key),
            )
        };
        for key in ["a", "b"] {
            s.threads.invalidate(key, "seed");
            s.issues.invalidate(key, "seed");
            s.list.invalidate(key, "seed");
        }
        let (a, b) = (caches_of("a"), caches_of("b"));

        s.emit(serde_json::json!({
            "table": "team_sync", "action": "update", "teamId": "t", "projectIds": ["a"],
        }));
        let after = caches_of("a");
        assert!(after.0 > a.0 && after.1 > a.1 && after.2 > a.2);
        assert_eq!(caches_of("b"), b, "an unnamed project is untouched");

        s.emit(serde_json::json!({"table": "team_sync", "action": "update"}));
        let after = caches_of("b");
        assert!(after.0 > b.0 && after.1 > b.1 && after.2 > b.2);
    }

    /// A fact decorated at read time is announced directly, for the projects
    /// whose snapshot names the job, without a rebuild.
    #[tokio::test(start_paused = true)]
    async fn announce_where_names_only_the_matching_projects() {
        let s = sidebar(STATUS_ROLLUP_FLOOR);
        *s.issue_jobs.lock().unwrap() = job_set(&["j"]);
        s.issues.get("with-job").await.unwrap();
        *s.issue_jobs.lock().unwrap() = job_set(&["other"]);
        s.issues.get("without-job").await.unwrap();
        s.announced.lock().unwrap().clear();

        s.issues
            .announce_where(|snapshot| snapshot.job_ids.contains("j"));

        assert_eq!(
            *s.announced.lock().unwrap(),
            vec!["issue_status:with-job".to_string()]
        );
        assert_eq!(s.issues.rebuilds(), 2);
    }
}
