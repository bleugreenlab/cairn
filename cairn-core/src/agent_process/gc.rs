//! Warm process garbage collection
//!
//! Manages the lifecycle of warm agent processes to prevent resource
//! exhaustion. Warm processes are pure cache: eviction loses no work (a resume
//! re-spawns from the persisted session), so trimming aggressively under memory
//! pressure is always safe.
//!
//! ## Sizing policy (CAIRN-2543)
//!
//! The pool is sized to *actual memory headroom*, not an arbitrary count. When
//! the [`MemoryProbe`] can measure the system, the GC keeps a reserve free
//! (`max(2 GiB, 10% of RAM)`, plus room for one spawn at admission) and evicts
//! the lowest-scoring unprotected warm processes until that budget is met —
//! there is no count cap, so a machine that can hold 30 warm processes keeps
//! them. Only when memory is unmeasurable does the fixed `max_warm` count cap
//! act as a fallback, trimming the pool to the cap in one pass.
//!
//! Every collection returns the *set* of run ids to evict (not one), so a
//! parallel-spawn burst or an over-budget idle pool is trimmed in a single pass
//! rather than one eviction per admission.
//!
//! ## Relevance Scoring
//!
//! Among unprotected processes, the lowest-scoring are evicted first. Score:
//! - Job status (blocked jobs get +100, high priority to keep)
//! - Recent user view (+50 if viewed within 10 minutes)
//! - Time decay (-10 per minute since last activity)
//!
//! Protected processes (a suspended parent with active children, a pending
//! memory review, or an unresolved owning database) are never evicted,
//! regardless of pressure.

use crate::agent_process::memory::{MemoryProbe, OsMemoryProbe, SystemMemory};
use crate::agent_process::process::{AgentProcessState, WarmProcess};
use crate::db::DbState;
use crate::execution::routing::routing_db_for_id;
use crate::storage::{LocalDb, RowExt};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default fallback count cap on warm processes (used only when memory is
/// unmeasurable).
pub const DEFAULT_MAX_WARM_PROCESSES: usize = 6;

/// Duration after which a view is considered "stale" for relevance scoring
const VIEW_RELEVANCE_DURATION: Duration = Duration::from_secs(10 * 60); // 10 minutes

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Floor on the memory the GC keeps free: the larger of this and 10% of total.
const MIN_RESERVE: u64 = 2 * GIB;
/// Assumed RSS for a warm process we cannot measure and have no measured
/// siblings to borrow a median from.
const DEFAULT_UNMEASURED_RSS: u64 = 512 * MIB;
/// Assumed RSS of a process about to spawn when no warm sibling was measurable.
const DEFAULT_SPAWN_RSS: u64 = GIB;

/// Garbage collector for warm agent processes
pub struct WarmProcessGC {
    /// Fallback count cap, applied only when system memory cannot be measured.
    max_warm: usize,
    /// Last view time for each session_id
    last_viewed: Mutex<HashMap<String, Instant>>,
    /// Memory measurement source (real OS probe in production, stub in tests).
    probe: Arc<dyn MemoryProbe>,
}

impl Default for WarmProcessGC {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_WARM_PROCESSES)
    }
}

impl WarmProcessGC {
    /// Create a new GC with the specified fallback count cap, backed by the real
    /// OS memory probe.
    pub fn new(max_warm: usize) -> Self {
        Self::with_probe(max_warm, Arc::new(OsMemoryProbe))
    }

    /// Create a GC with an injected memory probe (for tests).
    pub fn with_probe(max_warm: usize, probe: Arc<dyn MemoryProbe>) -> Self {
        Self {
            max_warm,
            last_viewed: Mutex::new(HashMap::new()),
            probe,
        }
    }

    /// Record that a user viewed a session (issue/chat associated with a warm process)
    pub fn record_view(&self, session_id: &str) {
        if let Ok(mut views) = self.last_viewed.lock() {
            views.insert(session_id.to_string(), Instant::now());
            log::debug!(
                "Recorded view for session {}",
                &session_id[..session_id.len().min(8)]
            );
        }
    }

    /// Check if a session was viewed recently (within VIEW_RELEVANCE_DURATION)
    pub fn was_viewed_recently(&self, session_id: &str) -> bool {
        if let Ok(views) = self.last_viewed.lock() {
            if let Some(view_time) = views.get(session_id) {
                return view_time.elapsed() < VIEW_RELEVANCE_DURATION;
            }
        }
        false
    }

    /// Calculate relevance score for a process
    ///
    /// Higher score = more relevant = less likely to be evicted
    ///
    /// Scoring:
    /// - +100: Job is blocked (awaiting checkpoint approval)
    /// - +50: User viewed within last 10 minutes
    /// - -10: Per minute since last activity (decay)
    pub async fn score_relevance(
        &self,
        db: &LocalDb,
        session_id: Option<&str>,
        job_id: Option<&str>,
        seconds_since_activity: u64,
    ) -> i32 {
        let job_status = match job_id {
            Some(jid) => self.load_job_status(db, jid).await,
            None => None,
        };

        self.score_relevance_from_metadata(
            session_id,
            job_id,
            job_status.as_deref(),
            seconds_since_activity,
        )
    }

    fn score_relevance_from_metadata(
        &self,
        session_id: Option<&str>,
        job_id: Option<&str>,
        job_status: Option<&str>,
        seconds_since_activity: u64,
    ) -> i32 {
        let mut score: i32 = 0;

        // Check if job is blocked (+100)
        if job_status == Some("blocked") {
            if let Some(jid) = job_id {
                score += 100;
                log::debug!(
                    "Job {} is blocked, +100 relevance",
                    &jid[..jid.len().min(8)]
                );
            }
        }

        // Check if user viewed recently (+50)
        if let Some(sid) = session_id {
            if self.was_viewed_recently(sid) {
                score += 50;
                log::debug!(
                    "Session {} viewed recently, +50 relevance",
                    &sid[..sid.len().min(8)]
                );
            }
        }

        // Time decay (-10 per minute)
        let minutes_since_activity = (seconds_since_activity / 60) as i32;
        let decay = minutes_since_activity * 10;
        score -= decay;

        score
    }

    async fn load_job_status(&self, db: &LocalDb, job_id: &str) -> Option<String> {
        let job_id = job_id.to_string();
        let log_job_id = job_id.clone();
        db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status
                         FROM jobs
                         WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;

                let status = match rows.next().await? {
                    Some(row) => Some(row.text(0)?),
                    None => None,
                };

                Ok(status)
            })
        })
        .await
        .map_err(|error| {
            log::warn!(
                "GC: failed to load job status for {}: {}",
                log_job_id,
                error
            );
            error
        })
        .ok()
        .flatten()
    }

    /// Compute the set of warm run ids to evict in one collection pass.
    ///
    /// Memory-aware when the probe can measure the system: keep `reserve` bytes
    /// available (`max(2 GiB, 10% of RAM)`), plus room for one new process when
    /// `headroom_for_spawn` is set (the largest measured warm RSS, default
    /// 1 GiB); evict the lowest-scoring unprotected warm processes, crediting
    /// each one's measured RSS, until the budget is satisfied. No count cap on
    /// this path — a big idle machine keeps a big warm pool.
    ///
    /// Falls back to the `max_warm` count cap only when system memory is
    /// unmeasurable, trimming the pool to the cap in one pass. Protected
    /// processes are never in the returned set, regardless of pressure.
    ///
    /// The caller is responsible for actually killing the returned runs.
    pub async fn find_eviction_set(
        &self,
        process_state: &AgentProcessState,
        dbs: &DbState,
        headroom_for_spawn: bool,
    ) -> Vec<String> {
        let warm = process_state.warm_processes();
        if warm.is_empty() {
            return Vec::new();
        }
        let warm_count = warm.len();

        let metadata = self.load_relevance_metadata(dbs, &warm).await;

        // Score the unprotected candidates and measure their RSS.
        let mut candidates: Vec<ScoredCandidate> = Vec::new();
        for wp in &warm {
            if metadata.is_protected(&wp.run_id, wp.job_id.as_deref()) {
                log::debug!(
                    "GC: run {} protected, not an eviction target",
                    &wp.run_id[..wp.run_id.len().min(8)]
                );
                continue;
            }
            let session_id = metadata
                .session_ids
                .get(&wp.run_id)
                .and_then(|session_id| session_id.as_deref());
            let job_status = wp
                .job_id
                .as_ref()
                .and_then(|jid| metadata.job_statuses.get(jid).map(|status| status.as_str()));
            let score = self.score_relevance_from_metadata(
                session_id,
                wp.job_id.as_deref(),
                job_status,
                wp.seconds_since_activity,
            );
            let rss = wp.pid.and_then(|pid| self.probe.process_rss_bytes(pid));
            candidates.push(ScoredCandidate {
                run_id: wp.run_id.clone(),
                score,
                rss,
            });
        }

        // Impute unmeasured RSS from the median of measured siblings so an
        // un-probeable process still counts toward the budget.
        let measured: Vec<u64> = candidates.iter().filter_map(|c| c.rss).collect();
        let imputed_rss = median(&measured).unwrap_or(DEFAULT_UNMEASURED_RSS);
        let effective_rss = |c: &ScoredCandidate| c.rss.unwrap_or(imputed_rss);

        // Lowest score first — least relevant evicted first.
        candidates.sort_by(|a, b| a.score.cmp(&b.score));

        let aggregate_rss: u64 = candidates.iter().map(effective_rss).sum();
        let system = self.probe.system_memory();

        let evicted: Vec<String> = match system {
            Some(SystemMemory { total, available }) => {
                let reserve = std::cmp::max(MIN_RESERVE, total / 10);
                let spawn_room = if headroom_for_spawn {
                    measured.iter().copied().max().unwrap_or(DEFAULT_SPAWN_RSS)
                } else {
                    0
                };
                let needed = reserve.saturating_add(spawn_room);
                let mut reclaimed = 0u64;
                let mut set = Vec::new();
                for candidate in &candidates {
                    if available.saturating_add(reclaimed) >= needed {
                        break;
                    }
                    reclaimed = reclaimed.saturating_add(effective_rss(candidate));
                    set.push(candidate.run_id.clone());
                }
                set
            }
            None => {
                // Fallback: trim to the count cap in one pass. Uses total warm
                // count (protected included) so the cap reflects real pool size,
                // but only unprotected candidates are evicted.
                if warm_count <= self.max_warm {
                    Vec::new()
                } else {
                    let to_evict = warm_count - self.max_warm;
                    candidates
                        .iter()
                        .take(to_evict)
                        .map(|c| c.run_id.clone())
                        .collect()
                }
            }
        };

        log::info!(
            "GC: warm={} candidates={} agg_rss={}MiB available={}MiB measurable={} evicting {} run(s): {:?}",
            warm_count,
            candidates.len(),
            aggregate_rss / MIB,
            system.map(|s| s.available / MIB).unwrap_or(0),
            system.is_some(),
            evicted.len(),
            evicted
                .iter()
                .map(|r| r[..r.len().min(8)].to_string())
                .collect::<Vec<_>>(),
        );

        evicted
    }

    async fn load_relevance_metadata(
        &self,
        dbs: &DbState,
        warm_processes: &[WarmProcess],
    ) -> RelevanceMetadata {
        let run_ids: Vec<String> = warm_processes.iter().map(|w| w.run_id.clone()).collect();
        let job_ids: Vec<String> = warm_processes
            .iter()
            .filter_map(|w| w.job_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let mut metadata = RelevanceMetadata::default();

        for run_id in run_ids {
            let db = match routing_db_for_id(dbs, &run_id).await {
                Ok(db) => db,
                Err(error) => {
                    log::warn!(
                        "GC: failed to route run {run_id} for relevance metadata: {error}; protecting it from eviction"
                    );
                    metadata.unresolved_run_ids.insert(run_id);
                    continue;
                }
            };
            match load_session_id_for_run(&db, &run_id).await {
                Ok(session_id) => {
                    metadata.session_ids.insert(run_id, session_id);
                }
                Err(error) => {
                    log::warn!(
                        "GC: failed to load session metadata for run {run_id}: {error}; protecting it from eviction"
                    );
                    metadata.unresolved_run_ids.insert(run_id);
                }
            }
        }

        for job_id in job_ids {
            let db = match routing_db_for_id(dbs, &job_id).await {
                Ok(db) => db,
                Err(error) => {
                    log::warn!(
                        "GC: failed to route job {job_id} for relevance metadata: {error}; protecting it from eviction"
                    );
                    metadata.unresolved_job_ids.insert(job_id);
                    continue;
                }
            };
            match load_job_relevance(&db, &job_id).await {
                Ok(job) => {
                    if let Some((status, memory_review_state)) = job {
                        metadata.job_statuses.insert(job_id.clone(), status);
                        if memory_review_state.as_deref() == Some("sent") {
                            metadata
                                .jobs_with_pending_memory_review
                                .insert(job_id.clone());
                        }
                    }
                    match parent_has_active_children(&db, &job_id).await {
                        Ok(true) => {
                            metadata.parents_with_active_children.insert(job_id);
                        }
                        Ok(false) => {}
                        Err(error) => {
                            log::warn!(
                                "GC: failed to load child metadata for job {job_id}: {error}; protecting it from eviction"
                            );
                            metadata.unresolved_job_ids.insert(job_id);
                        }
                    }
                }
                Err(error) => {
                    log::warn!(
                        "GC: failed to load job metadata for job {job_id}: {error}; protecting it from eviction"
                    );
                    metadata.unresolved_job_ids.insert(job_id);
                }
            }
        }

        metadata
    }

    /// Get current statistics
    pub fn stats(&self, process_state: &AgentProcessState) -> GCStats {
        let warm_count = process_state.warm_process_count();
        let active_count = process_state.active_process_count();
        let view_count = self.last_viewed.lock().map(|v| v.len()).unwrap_or(0);

        GCStats {
            max_warm: self.max_warm,
            warm_count,
            active_count,
            tracked_views: view_count,
        }
    }

    /// Clean up stale view records (older than VIEW_RELEVANCE_DURATION).
    /// Called by the periodic warm sweep alongside `find_eviction_set`.
    pub fn cleanup_stale_views(&self) {
        if let Ok(mut views) = self.last_viewed.lock() {
            let before = views.len();
            views.retain(|_, view_time| view_time.elapsed() < VIEW_RELEVANCE_DURATION);
            let after = views.len();
            if before > after {
                log::debug!("GC: cleaned up {} stale view records", before - after);
            }
        }
    }
}

/// A warm process scored for possible eviction, with its measured RSS (if any).
struct ScoredCandidate {
    run_id: String,
    score: i32,
    rss: Option<u64>,
}

/// Median of a slice (upper-middle for even lengths), or `None` when empty.
fn median(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() / 2])
}

async fn load_session_id_for_run(
    db: &LocalDb,
    run_id: &str,
) -> Result<Option<String>, crate::storage::DbError> {
    let run_id = run_id.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT session_id
                     FROM runs
                     WHERE id = ?1",
                    (run_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
}

async fn load_job_relevance(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<(String, Option<String>)>, crate::storage::DbError> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, memory_review_state
                     FROM jobs
                     WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some((row.text(0)?, row.opt_text(1)?))),
                None => Ok(None),
            }
        })
    })
    .await
}

async fn parent_has_active_children(
    db: &LocalDb,
    job_id: &str,
) -> Result<bool, crate::storage::DbError> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut child_rows = conn
                .query(
                    "SELECT 1
                     FROM jobs
                     WHERE parent_job_id = ?1
                       AND status NOT IN ('complete', 'failed')
                     LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            Ok(child_rows.next().await?.is_some())
        })
    })
    .await
}

#[derive(Debug, Default)]
struct RelevanceMetadata {
    session_ids: HashMap<String, Option<String>>,
    job_statuses: HashMap<String, String>,
    /// Job IDs that have at least one non-terminal delegated child. These
    /// parents are suspended waiting on those children and must never be
    /// evicted (killing one forces a costly full reload and leaves the parent
    /// with no live process until resume fires).
    parents_with_active_children: HashSet<String>,
    /// Job IDs whose first-artifact memory review has been queued but not
    /// completed. These runs must stay warm so flush-on-idle can deliver the
    /// review prompt.
    jobs_with_pending_memory_review: HashSet<String>,
    /// Run IDs whose owning database could not be resolved or queried. GC treats
    /// them as protected because their relevance/protection state is unknown.
    unresolved_run_ids: HashSet<String>,
    /// Job IDs whose owning database could not be resolved or queried. GC treats
    /// them as protected because their active-child or memory-review state is unknown.
    unresolved_job_ids: HashSet<String>,
}

impl RelevanceMetadata {
    /// Whether a warm process must never be evicted, regardless of memory
    /// pressure: a suspended parent with active children, a job with a pending
    /// memory review, or a run/job whose owning database could not be resolved.
    fn is_protected(&self, run_id: &str, job_id: Option<&str>) -> bool {
        if self.unresolved_run_ids.contains(run_id) {
            return true;
        }
        if let Some(jid) = job_id {
            if self.unresolved_job_ids.contains(jid)
                || self.parents_with_active_children.contains(jid)
                || self.jobs_with_pending_memory_review.contains(jid)
            {
                return true;
            }
        }
        false
    }
}

/// Statistics about the GC state
#[derive(Debug, Clone)]
pub struct GCStats {
    pub max_warm: usize,
    pub warm_count: usize,
    pub active_count: usize,
    pub tracked_views: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::memory::{StubMemoryProbe, SystemMemory};
    use crate::agent_process::process::{AgentProcessState, RunHandle};
    use crate::services::testing::MockChildProcess;
    use crate::services::ChildProcess;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn test_db() -> (TempDir, Arc<LocalDb>) {
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(LocalDb::open(temp.path().join("gc-test.db")).await.unwrap());
        db.execute_batch(
            "
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY NOT NULL,
                status TEXT NOT NULL,
                parent_job_id TEXT,
                memory_review_state TEXT
            );

            CREATE TABLE runs (
                id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT
            );
            ",
        )
        .await
        .unwrap();

        (temp, db)
    }

    fn test_db_state(temp: &TempDir, db: Arc<LocalDb>) -> DbState {
        DbState::new(
            db,
            Arc::new(SearchIndex::open_or_create(temp.path().join("search-index.db")).unwrap()),
        )
    }

    /// A GC whose probe reports no measurable system memory, forcing the
    /// count-based fallback path.
    fn fallback_gc(max_warm: usize) -> WarmProcessGC {
        WarmProcessGC::with_probe(max_warm, Arc::new(StubMemoryProbe::new(None)))
    }

    /// A GC whose probe reports the given system memory, driving the
    /// budget-based memory path. RSS per pid can be layered on via the probe.
    fn memory_gc(total: u64, available: u64, probe: StubMemoryProbe) -> WarmProcessGC {
        let probe = StubMemoryProbe {
            system: Some(SystemMemory { total, available }),
            ..probe
        };
        WarmProcessGC::with_probe(DEFAULT_MAX_WARM_PROCESSES, Arc::new(probe))
    }

    async fn insert_job(db: &LocalDb, id: &str, status: &str) {
        db.execute("INSERT INTO jobs(id, status) VALUES (?1, ?2)", (id, status))
            .await
            .unwrap();
    }

    async fn insert_child_job(db: &LocalDb, id: &str, status: &str, parent_job_id: &str) {
        db.execute(
            "INSERT INTO jobs(id, status, parent_job_id) VALUES (?1, ?2, ?3)",
            (id, status, parent_job_id),
        )
        .await
        .unwrap();
    }

    async fn insert_run(db: &LocalDb, id: &str, session_id: Option<&str>) {
        db.execute(
            "INSERT INTO runs(id, session_id) VALUES (?1, ?2)",
            (id, session_id),
        )
        .await
        .unwrap();
    }

    /// A warm handle with no attached child (pid unmeasurable).
    fn warm_handle(
        session_id: Option<&str>,
        job_id: Option<&str>,
        seconds_since_activity: u64,
    ) -> RunHandle {
        let mut handle = RunHandle::test_handle(session_id, job_id);
        handle.transition_to_warm();
        handle.last_activity = Instant::now() - Duration::from_secs(seconds_since_activity);
        handle
    }

    /// A warm handle carrying a mock child with the given pid, so the memory
    /// probe can map it to an RSS.
    fn warm_handle_with_pid(
        session_id: Option<&str>,
        job_id: Option<&str>,
        seconds_since_activity: u64,
        pid: u32,
    ) -> RunHandle {
        let child: Arc<Mutex<Option<Box<dyn ChildProcess>>>> = Arc::new(Mutex::new(Some(
            Box::new(MockChildProcess::with_stdout(pid, vec![])),
        )));
        let stdin = Arc::new(Mutex::new(None));
        let mut handle = RunHandle::new(
            child,
            stdin,
            session_id.map(str::to_string),
            job_id.map(str::to_string),
        );
        handle.transition_to_warm();
        handle.last_activity = Instant::now() - Duration::from_secs(seconds_since_activity);
        handle
    }

    #[test]
    fn test_gc_default_capacity() {
        let gc = WarmProcessGC::default();
        assert_eq!(gc.max_warm, DEFAULT_MAX_WARM_PROCESSES);
    }

    #[test]
    fn test_record_and_check_view() {
        let gc = WarmProcessGC::new(5);

        // Initially not viewed
        assert!(!gc.was_viewed_recently("session-1"));

        // Record view
        gc.record_view("session-1");

        // Now should be viewed recently
        assert!(gc.was_viewed_recently("session-1"));

        // Other session still not viewed
        assert!(!gc.was_viewed_recently("session-2"));
    }

    #[test]
    fn test_cleanup_stale_views() {
        let gc = WarmProcessGC::new(5);

        gc.record_view("session-1");
        gc.record_view("session-2");

        // Both should be present
        assert_eq!(gc.last_viewed.lock().unwrap().len(), 2);

        // Cleanup shouldn't remove recent views
        gc.cleanup_stale_views();
        assert_eq!(gc.last_viewed.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_score_relevance_uses_job_status_and_recent_view() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-blocked", "blocked").await;

        let gc = WarmProcessGC::new(1);
        gc.record_view("session-1");

        let score = gc
            .score_relevance(&db, Some("session-1"), Some("job-blocked"), 120)
            .await;

        assert_eq!(score, 130);
    }

    // === Fallback (count-cap) path: memory unmeasurable ===

    #[tokio::test]
    async fn test_eviction_set_orders_by_relevance() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-blocked", "blocked").await;
        insert_job(&db, "job-running", "running").await;
        insert_run(&db, "run-keep", Some("session-keep")).await;
        insert_run(&db, "run-evict", Some("session-evict")).await;

        // Fallback cap of 1: with 2 warm, exactly the lowest-scoring is evicted.
        let gc = fallback_gc(1);
        gc.record_view("session-keep");
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-keep".to_string(),
                warm_handle(Some("session-keep"), Some("job-blocked"), 60),
            );
            processes.register(
                "run-evict".to_string(),
                warm_handle(Some("session-evict"), Some("job-running"), 60),
            );
        }

        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(
            gc.find_eviction_set(&state, &dbs, false).await,
            vec!["run-evict".to_string()]
        );
    }

    #[tokio::test]
    async fn test_parent_with_active_children_is_not_evicted() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-parent", "running").await;
        insert_child_job(&db, "job-child", "running", "job-parent").await;
        insert_job(&db, "job-other", "running").await;
        insert_run(&db, "run-parent", Some("session-parent")).await;
        insert_run(&db, "run-other", Some("session-other")).await;

        let gc = fallback_gc(1);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            // The parent is oldest/never-viewed (lowest score) but protected.
            processes.register(
                "run-parent".to_string(),
                warm_handle(Some("session-parent"), Some("job-parent"), 600),
            );
            processes.register(
                "run-other".to_string(),
                warm_handle(Some("session-other"), Some("job-other"), 60),
            );
        }

        // The protected parent is skipped, so the sibling is evicted instead.
        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(
            gc.find_eviction_set(&state, &dbs, false).await,
            vec!["run-other".to_string()]
        );
    }

    #[tokio::test]
    async fn test_team_parent_with_active_children_is_not_evicted() {
        let (_temp, private) = test_db().await;
        let team_temp = tempfile::tempdir().unwrap();
        let team_db = Arc::new(
            LocalDb::open(team_temp.path().join("team-gc-test.db"))
                .await
                .unwrap(),
        );
        team_db
            .execute_batch(
                "
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    parent_job_id TEXT,
                    memory_review_state TEXT
                );

                CREATE TABLE runs (
                    id TEXT PRIMARY KEY NOT NULL,
                    session_id TEXT
                );
                ",
            )
            .await
            .unwrap();
        let team = "teamgc";
        let parent = "teamgc~00000000-0000-4000-8000-000000000001";
        let child = "teamgc~00000000-0000-4000-8000-000000000002";
        let other_job = "job-other";
        let parent_run = "teamgc~00000000-0000-4000-8000-000000000003";
        insert_job(&team_db, parent, "running").await;
        insert_child_job(&team_db, child, "running", parent).await;
        insert_run(&team_db, parent_run, Some("session-parent")).await;
        insert_job(&private, other_job, "running").await;
        insert_run(&private, "run-other", Some("session-other")).await;

        let dbs = test_db_state(&_temp, private.clone());
        dbs.insert_team_db_for_test(team, team_db.clone()).await;
        let gc = fallback_gc(1);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                parent_run.to_string(),
                warm_handle(Some("session-parent"), Some(parent), 600),
            );
            processes.register(
                "run-other".to_string(),
                warm_handle(Some("session-other"), Some(other_job), 60),
            );
        }

        assert_eq!(
            gc.find_eviction_set(&state, &dbs, false).await,
            vec!["run-other".to_string()],
            "the team parent is protected by active child metadata loaded from the team replica"
        );
    }

    #[tokio::test]
    async fn test_completed_children_do_not_protect_parent() {
        let (_temp, db) = test_db().await;
        // All children terminal: the parent is a normal eviction candidate.
        insert_job(&db, "job-parent", "running").await;
        insert_child_job(&db, "job-child", "complete", "job-parent").await;
        insert_run(&db, "run-parent", Some("session-parent")).await;

        // Cap of 0: the single unprotected warm process is trimmed.
        let gc = fallback_gc(0);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-parent".to_string(),
                warm_handle(Some("session-parent"), Some("job-parent"), 600),
            );
        }

        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(
            gc.find_eviction_set(&state, &dbs, false).await,
            vec!["run-parent".to_string()]
        );
    }

    #[tokio::test]
    async fn test_sole_protected_parent_yields_no_eviction() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-parent", "running").await;
        insert_child_job(&db, "job-child", "running", "job-parent").await;
        insert_run(&db, "run-parent", Some("session-parent")).await;

        // Cap of 0 wants to evict, but the only candidate is protected.
        let gc = fallback_gc(0);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-parent".to_string(),
                warm_handle(Some("session-parent"), Some("job-parent"), 600),
            );
        }

        let dbs = test_db_state(&_temp, db.clone());
        assert!(gc.find_eviction_set(&state, &dbs, false).await.is_empty());
    }

    #[tokio::test]
    async fn test_sent_memory_review_job_is_not_evicted() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-review", "complete").await;
        db.execute(
            "UPDATE jobs SET memory_review_state = 'sent' WHERE id = 'job-review'",
            (),
        )
        .await
        .unwrap();
        insert_job(&db, "job-other", "running").await;
        insert_run(&db, "run-review", Some("session-review")).await;
        insert_run(&db, "run-other", Some("session-other")).await;

        let gc = fallback_gc(1);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-review".to_string(),
                warm_handle(Some("session-review"), Some("job-review"), 600),
            );
            processes.register(
                "run-other".to_string(),
                warm_handle(Some("session-other"), Some("job-other"), 60),
            );
        }

        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(
            gc.find_eviction_set(&state, &dbs, false).await,
            vec!["run-other".to_string()]
        );
    }

    #[tokio::test]
    async fn test_fallback_trims_to_max_warm_in_one_pass() {
        // Direct regression for the old ratchet bug: 5 warm, cap 2 => 3 evicted
        // in a single pass (not one per admission).
        let (_temp, db) = test_db().await;
        for i in 0..5 {
            insert_job(&db, &format!("job-{i}"), "running").await;
            insert_run(&db, &format!("run-{i}"), Some(&format!("session-{i}"))).await;
        }

        let gc = fallback_gc(2);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            // Increasing idle time => decreasing score; run-0 oldest (lowest).
            for i in 0..5 {
                processes.register(
                    format!("run-{i}"),
                    warm_handle(
                        Some(&format!("session-{i}")),
                        Some(&format!("job-{i}")),
                        (5 - i) as u64 * 120,
                    ),
                );
            }
        }

        let dbs = test_db_state(&_temp, db.clone());
        let mut evicted = gc.find_eviction_set(&state, &dbs, false).await;
        evicted.sort();
        // The 3 oldest (lowest-scoring) are run-0, run-1, run-2.
        assert_eq!(
            evicted,
            vec![
                "run-0".to_string(),
                "run-1".to_string(),
                "run-2".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_starting_processes_do_not_trigger_eviction() {
        let (_temp, db) = test_db().await;
        let gc = fallback_gc(0);
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            // Starting (not warm) process: excluded from the warm set entirely.
            let handle = RunHandle::test_handle(Some("session-1"), None);
            processes.register("run-1".to_string(), handle);
        }

        let dbs = test_db_state(&_temp, db.clone());
        assert!(gc.find_eviction_set(&state, &dbs, false).await.is_empty());
    }

    // === Memory-budget path ===

    #[tokio::test]
    async fn test_over_budget_trims_multiple_in_one_pass() {
        let (_temp, db) = test_db().await;
        // Four warm processes, distinct pids/RSS, increasing idle => run-a lowest.
        let specs = [
            ("run-a", 601u32, 600),
            ("run-b", 602, 300),
            ("run-c", 603, 120),
            ("run-d", 604, 60),
        ];
        for (run, _pid, _idle) in &specs {
            insert_job(&db, &format!("job-{run}"), "running").await;
            insert_run(&db, run, Some(&format!("session-{run}"))).await;
        }

        // 8 GiB total => reserve = max(2 GiB, 800 MiB) = 2 GiB. available 1 GiB,
        // no spawn headroom => need 1 GiB reclaimed. At 600 MiB each that is two.
        let probe = StubMemoryProbe::default()
            .with_rss(601, 600 * MIB)
            .with_rss(602, 600 * MIB)
            .with_rss(603, 600 * MIB)
            .with_rss(604, 600 * MIB);
        let gc = memory_gc(8 * GIB, GIB, probe);

        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            for (run, pid, idle) in specs {
                processes.register(
                    run.to_string(),
                    warm_handle_with_pid(
                        Some(&format!("session-{run}")),
                        Some(&format!("job-{run}")),
                        idle,
                        pid,
                    ),
                );
            }
        }

        let dbs = test_db_state(&_temp, db.clone());
        let mut evicted = gc.find_eviction_set(&state, &dbs, false).await;
        evicted.sort();
        assert_eq!(
            evicted,
            vec!["run-a".to_string(), "run-b".to_string()],
            "the two lowest-scoring processes are trimmed together"
        );
    }

    #[tokio::test]
    async fn test_ample_headroom_keeps_large_pool() {
        // The no-arbitrary-cap requirement: 30 warm processes and plenty of
        // free memory => zero evictions.
        let (_temp, db) = test_db().await;
        for i in 0..30 {
            insert_job(&db, &format!("job-{i}"), "running").await;
            insert_run(&db, &format!("run-{i}"), Some(&format!("session-{i}"))).await;
        }
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            for i in 0..30 {
                processes.register(
                    format!("run-{i}"),
                    warm_handle(
                        Some(&format!("session-{i}")),
                        Some(&format!("job-{i}")),
                        600,
                    ),
                );
            }
        }

        // 64 GiB total, 32 GiB free => reserve ~6.4 GiB, way under available even
        // with spawn headroom. Well above DEFAULT_MAX_WARM_PROCESSES processes.
        let gc = memory_gc(64 * GIB, 32 * GIB, StubMemoryProbe::default());
        let dbs = test_db_state(&_temp, db.clone());
        assert!(
            gc.find_eviction_set(&state, &dbs, true).await.is_empty(),
            "a big idle machine keeps a big warm pool"
        );
    }

    #[tokio::test]
    async fn test_protected_never_evicted_under_extreme_pressure() {
        let (_temp, db) = test_db().await;
        // One protected parent (active child) plus two unprotected processes.
        insert_job(&db, "job-parent", "running").await;
        insert_child_job(&db, "job-child", "running", "job-parent").await;
        insert_job(&db, "job-a", "running").await;
        insert_job(&db, "job-b", "running").await;
        insert_run(&db, "run-parent", Some("session-parent")).await;
        insert_run(&db, "run-a", Some("session-a")).await;
        insert_run(&db, "run-b", Some("session-b")).await;

        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-parent".to_string(),
                warm_handle(Some("session-parent"), Some("job-parent"), 900),
            );
            processes.register(
                "run-a".to_string(),
                warm_handle(Some("session-a"), Some("job-a"), 300),
            );
            processes.register(
                "run-b".to_string(),
                warm_handle(Some("session-b"), Some("job-b"), 60),
            );
        }

        // available 0 => extreme pressure: every unprotected candidate is
        // evicted, but the protected parent never is.
        let gc = memory_gc(8 * GIB, 0, StubMemoryProbe::default());
        let dbs = test_db_state(&_temp, db.clone());
        let mut evicted = gc.find_eviction_set(&state, &dbs, false).await;
        evicted.sort();
        assert_eq!(evicted, vec!["run-a".to_string(), "run-b".to_string()]);
        assert!(
            !evicted.contains(&"run-parent".to_string()),
            "a protected parent is never evicted, even at zero available memory"
        );
    }
}
