//! Warm process garbage collection
//!
//! Manages the lifecycle of warm Claude processes to prevent resource exhaustion.
//! Uses relevance scoring to decide which processes to evict when capacity is reached.
//!
//! ## Relevance Scoring
//!
//! Each warm process gets a relevance score based on:
//! - Job status (blocked jobs get +100, high priority to keep)
//! - Recent user view (+50 if viewed within 10 minutes)
//! - Time decay (-10 per minute since last activity)
//!
//! When a new process needs to be spawned and we're at capacity,
//! the warm process with the lowest relevance score is evicted.

use crate::agent_process::process::AgentProcessState;
use crate::db::DbState;
use crate::execution::routing::routing_db_for_id;
use crate::storage::{LocalDb, RowExt};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default maximum number of warm processes to retain
pub const DEFAULT_MAX_WARM_PROCESSES: usize = 6;

/// Duration after which a view is considered "stale" for relevance scoring
const VIEW_RELEVANCE_DURATION: Duration = Duration::from_secs(10 * 60); // 10 minutes

/// Garbage collector for warm Claude processes
pub struct WarmProcessGC {
    /// Maximum number of warm processes to retain
    max_warm: usize,
    /// Last view time for each session_id
    last_viewed: Mutex<HashMap<String, Instant>>,
}

impl Default for WarmProcessGC {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_WARM_PROCESSES)
    }
}

impl WarmProcessGC {
    /// Create a new GC with the specified capacity
    pub fn new(max_warm: usize) -> Self {
        Self {
            max_warm,
            last_viewed: Mutex::new(HashMap::new()),
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

    /// Find the warm process with the lowest relevance score for eviction.
    ///
    /// Called before spawning a new process. If we're at capacity,
    /// returns the run_id of the warm process that should be evicted.
    ///
    /// The caller is responsible for actually killing the process.
    pub async fn find_eviction_candidate(
        &self,
        process_state: &AgentProcessState,
        dbs: &DbState,
    ) -> Option<String> {
        let warm_count = process_state.warm_process_count();
        if warm_count < self.max_warm {
            log::debug!(
                "GC: {} warm processes, capacity {}, no collection needed",
                warm_count,
                self.max_warm
            );
            return None;
        }

        log::info!(
            "GC: {} warm processes at capacity {}, need to evict one",
            warm_count,
            self.max_warm
        );

        // Get all warm processes with their metadata
        let warm_processes = process_state.warm_processes();
        if warm_processes.is_empty() {
            return None;
        }

        let metadata = self.load_relevance_metadata(dbs, &warm_processes).await;

        // Score each warm process
        let mut scored: Vec<(String, i32)> = Vec::new();
        for (run_id, seconds_since_activity, job_id) in &warm_processes {
            // A parent suspended on delegated children is transitioned to a warm
            // (idle) process while it waits, and its job stays `running` (a
            // Yielded turn keeps the job Running), so relevance scoring would not
            // protect it — it just decays into the lowest-scoring candidate.
            // Killing it loses no work (resume re-spawns from the persisted
            // session), but it forces a costly full reload and, until the resume
            // fires, the parent has no live process. Never evict a parent with
            // active children: it is not an eviction target at all.
            if metadata.unresolved_run_ids.contains(run_id)
                || job_id
                    .as_ref()
                    .map(|jid| metadata.unresolved_job_ids.contains(jid))
                    .unwrap_or(false)
            {
                log::debug!(
                    "GC: run {} protected (owning database unresolved), not an eviction target",
                    &run_id[..run_id.len().min(8)]
                );
                continue;
            }

            let has_active_children = job_id
                .as_ref()
                .map(|jid| metadata.parents_with_active_children.contains(jid))
                .unwrap_or(false);
            if has_active_children {
                log::debug!(
                    "GC: run {} protected (job has active delegated children), not an eviction target",
                    &run_id[..run_id.len().min(8)]
                );
                continue;
            }
            let memory_review_pending = job_id
                .as_ref()
                .map(|jid| metadata.jobs_with_pending_memory_review.contains(jid))
                .unwrap_or(false);
            if memory_review_pending {
                log::debug!(
                    "GC: run {} protected (job has pending memory review), not an eviction target",
                    &run_id[..run_id.len().min(8)]
                );
                continue;
            }

            let session_id = metadata
                .session_ids
                .get(run_id)
                .and_then(|session_id| session_id.as_deref());
            let job_status = job_id
                .as_ref()
                .and_then(|jid| metadata.job_statuses.get(jid).map(|status| status.as_str()));

            let score = self.score_relevance_from_metadata(
                session_id,
                job_id.as_deref(),
                job_status,
                *seconds_since_activity,
            );

            log::debug!(
                "GC: run {} score={} (job={:?}, seconds_since_activity={})",
                &run_id[..run_id.len().min(8)],
                score,
                job_id.as_ref().map(|j| &j[..j.len().min(8)]),
                seconds_since_activity
            );

            scored.push((run_id.clone(), score));
        }

        // Sort by score (lowest first - most likely to evict)
        scored.sort_by_key(|(_, score)| *score);

        // Return the lowest scoring process
        if let Some((run_id, score)) = scored.first() {
            log::info!(
                "GC: eviction candidate: run {} with score {}",
                &run_id[..run_id.len().min(8)],
                score
            );
            return Some(run_id.clone());
        }

        None
    }

    async fn load_relevance_metadata(
        &self,
        dbs: &DbState,
        warm_processes: &[(String, u64, Option<String>)],
    ) -> RelevanceMetadata {
        let run_ids: Vec<String> = warm_processes
            .iter()
            .map(|(run_id, _, _)| run_id.clone())
            .collect();
        let job_ids: Vec<String> = warm_processes
            .iter()
            .filter_map(|(_, _, job_id)| job_id.clone())
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

    /// Clean up stale view records (older than VIEW_RELEVANCE_DURATION)
    #[allow(dead_code)]
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
    /// evicted (see `find_eviction_candidate`).
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
    use crate::agent_process::process::{AgentProcessState, RunHandle};
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

    #[tokio::test]
    async fn test_find_eviction_candidate_uses_relevance_metadata() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-blocked", "blocked").await;
        insert_job(&db, "job-running", "running").await;
        insert_run(&db, "run-keep", Some("session-keep")).await;
        insert_run(&db, "run-evict", Some("session-evict")).await;

        let gc = WarmProcessGC::new(2);
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
            gc.find_eviction_candidate(&state, &dbs).await.as_deref(),
            Some("run-evict")
        );
    }

    #[tokio::test]
    async fn test_parent_with_active_children_is_not_evicted() {
        let (_temp, db) = test_db().await;
        // Parent is the lowest-scoring process (oldest, never viewed) and would
        // be evicted on score alone — but it is suspended on a live child.
        insert_job(&db, "job-parent", "running").await;
        insert_child_job(&db, "job-child", "running", "job-parent").await;
        insert_job(&db, "job-other", "running").await;
        insert_run(&db, "run-parent", Some("session-parent")).await;
        insert_run(&db, "run-other", Some("session-other")).await;

        let gc = WarmProcessGC::new(2);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-parent".to_string(),
                warm_handle(Some("session-parent"), Some("job-parent"), 600),
            );
            processes.register(
                "run-other".to_string(),
                warm_handle(Some("session-other"), Some("job-other"), 60),
            );
        }

        // The protected parent is skipped, so the (higher-scoring) sibling is
        // evicted instead.
        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(
            gc.find_eviction_candidate(&state, &dbs).await.as_deref(),
            Some("run-other")
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
        let gc = WarmProcessGC::new(2);
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
            gc.find_eviction_candidate(&state, &dbs).await.as_deref(),
            Some("run-other"),
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

        let gc = WarmProcessGC::new(1);
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
            gc.find_eviction_candidate(&state, &dbs).await.as_deref(),
            Some("run-parent")
        );
    }

    #[tokio::test]
    async fn test_sole_protected_parent_yields_no_eviction() {
        let (_temp, db) = test_db().await;
        insert_job(&db, "job-parent", "running").await;
        insert_child_job(&db, "job-child", "running", "job-parent").await;
        insert_run(&db, "run-parent", Some("session-parent")).await;

        let gc = WarmProcessGC::new(1);
        let state = AgentProcessState::default();
        {
            let mut processes = state.processes.lock().unwrap();
            processes.register(
                "run-parent".to_string(),
                warm_handle(Some("session-parent"), Some("job-parent"), 600),
            );
        }

        // At capacity but the only candidate is protected: evict nothing.
        // The new process spawns anyway (the cap is soft).
        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(gc.find_eviction_candidate(&state, &dbs).await, None);
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

        let gc = WarmProcessGC::new(2);
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
            gc.find_eviction_candidate(&state, &dbs).await.as_deref(),
            Some("run-other")
        );
    }

    #[tokio::test]
    async fn test_starting_processes_do_not_trigger_eviction() {
        let (_temp, db) = test_db().await;
        let gc = WarmProcessGC::new(1);
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let handle = RunHandle::test_handle(Some("session-1"), None);
            processes.register("run-1".to_string(), handle);
        }

        let dbs = test_db_state(&_temp, db.clone());
        assert_eq!(gc.find_eviction_candidate(&state, &dbs).await, None);
    }
}
