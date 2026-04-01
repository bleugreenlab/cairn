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
use crate::schema::{jobs, runs};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use std::collections::HashMap;
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
    pub fn score_relevance(
        &self,
        session_id: Option<&str>,
        job_id: Option<&str>,
        seconds_since_activity: u64,
        conn: &mut SqliteConnection,
    ) -> i32 {
        let mut score: i32 = 0;

        // Check if job is blocked (+100)
        if let Some(jid) = job_id {
            let status: Option<String> =
                jobs::table.find(jid).select(jobs::status).first(conn).ok();

            if status.as_deref() == Some("blocked") {
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

    /// Find the warm process with the lowest relevance score for eviction.
    ///
    /// Called before spawning a new process. If we're at capacity,
    /// returns the run_id of the warm process that should be evicted.
    ///
    /// The caller is responsible for actually killing the process.
    pub fn find_eviction_candidate(
        &self,
        process_state: &AgentProcessState,
        conn: &mut SqliteConnection,
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

        // Score each warm process
        let mut scored: Vec<(String, i32)> = Vec::new();
        for (run_id, seconds_since_activity, job_id) in &warm_processes {
            // Get session_id for this run
            let session_id: Option<String> = runs::table
                .find(run_id)
                .select(runs::session_id)
                .first::<Option<String>>(conn)
                .ok()
                .flatten();

            let score = self.score_relevance(
                session_id.as_deref(),
                job_id.as_deref(),
                *seconds_since_activity,
                conn,
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
    use crate::test_utils::test_diesel_conn;
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn test_starting_processes_do_not_trigger_eviction() {
        let gc = WarmProcessGC::new(1);
        let state = AgentProcessState::default();

        {
            let mut processes = state.processes.lock().unwrap();
            let child = Arc::new(Mutex::new(None));
            let stdin = Arc::new(Mutex::new(None));
            let handle = RunHandle::new(child, stdin, Some("session-1".to_string()), None);
            processes.register("run-1".to_string(), handle);
        }

        let mut conn = test_diesel_conn();
        assert_eq!(gc.find_eviction_candidate(&state, &mut conn), None);
    }
}
