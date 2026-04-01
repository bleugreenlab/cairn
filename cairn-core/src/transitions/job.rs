//! Job state transitions with failure cascade and status propagation.
//!
//! Valid transitions:
//! - Pending → Ready (DAG advancement: upstream dependencies satisfied)
//! - Pending → Failed (upstream job failed — cascade)
//! - Ready → Running (process spawned)
//! - Ready → Failed (spawn failed)
//! - Running → Complete (run completes, no checkpoint slot)
//! - Running → Failed (run fails)
//! - Running → Blocked (checkpoint slot — run completes but approval gate attached)
//! - Blocked → Complete (checkpoint approved, DAG advances downstream)
//! - Blocked → Pending (checkpoint rejected with recovery edge — upstream re-runs)
//! - Blocked → Failed (checkpoint rejected, no recovery edge)
//!
//! Follow-up continuation (user sends message after job reaches terminal state):
//! - Complete → Running (follow-up after completion)
//! - Failed → Running (follow-up after failure)
//! - Blocked → Running (follow-up bypassing checkpoint)
//!
//! Manager recovery transitions (manager jobs survive across multiple turns):
//! - Complete → Ready (previous turn completed, re-queuing for next wake trigger)
//! - Failed → Ready (previous turn failed, re-queuing for next wake trigger)

use crate::models::{JobStatus, TriggerEvent};
use crate::schema::{executions, jobs};
use crate::services::EventEmitter;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use tokio::sync::broadcast;

use super::status::{recompute_execution_status, recompute_issue_status};
use super::TransitionError;

/// Validate and execute a job status transition.
///
/// After transitioning:
/// 1. If transitioning to Failed, cascades failure to downstream jobs
/// 2. Recomputes execution status (from job states)
/// 3. Recomputes issue status (from execution states + resolution timestamps)
///
/// Returns the previous status on success.
pub fn transition_job(
    conn: &mut SqliteConnection,
    emitter: &dyn EventEmitter,
    job_id: &str,
    to: JobStatus,
    trigger_tx: &broadcast::Sender<TriggerEvent>,
) -> Result<JobStatus, TransitionError> {
    let (current_str, execution_id, issue_id, project_id, parent_job_id): (
        String,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    ) = jobs::table
        .find(job_id)
        .select((
            jobs::status,
            jobs::execution_id,
            jobs::issue_id,
            jobs::project_id,
            jobs::parent_job_id,
        ))
        .first(conn)
        .map_err(|_| TransitionError {
            entity: "job",
            id: job_id.to_string(),
            from: "unknown".to_string(),
            to: to.to_string(),
            reason: "job not found".to_string(),
        })?;

    let from: JobStatus = current_str.parse().map_err(|_| TransitionError {
        entity: "job",
        id: job_id.to_string(),
        from: current_str.clone(),
        to: to.to_string(),
        reason: format!("unparseable current status: {}", current_str),
    })?;

    validate_job_transition(&from, &to, job_id)?;

    let now = chrono::Utc::now().timestamp() as i32;

    // Build the update based on target state
    match &to {
        JobStatus::Running => {
            if from == JobStatus::Ready {
                // First start — set started_at
                diesel::update(jobs::table.find(job_id))
                    .set((
                        jobs::status.eq(to.to_string()),
                        jobs::started_at.eq(Some(now)),
                        jobs::updated_at.eq(now),
                    ))
                    .execute(conn)
                    .map_err(|e| db_error(job_id, &from, &to, e))?;
            } else {
                // Resume from terminal state — preserve original started_at
                diesel::update(jobs::table.find(job_id))
                    .set((jobs::status.eq(to.to_string()), jobs::updated_at.eq(now)))
                    .execute(conn)
                    .map_err(|e| db_error(job_id, &from, &to, e))?;
            }
        }
        JobStatus::Complete | JobStatus::Failed => {
            diesel::update(jobs::table.find(job_id))
                .set((
                    jobs::status.eq(to.to_string()),
                    jobs::completed_at.eq(Some(now)),
                    jobs::updated_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| db_error(job_id, &from, &to, e))?;
        }
        _ => {
            diesel::update(jobs::table.find(job_id))
                .set((jobs::status.eq(to.to_string()), jobs::updated_at.eq(now)))
                .execute(conn)
                .map_err(|e| db_error(job_id, &from, &to, e))?;
        }
    }

    // Cascade failure to downstream jobs
    if to == JobStatus::Failed {
        if let Some(ref exec_id) = execution_id {
            cascade_failure(conn, emitter, exec_id, job_id, trigger_tx);
        }
    }

    // Recompute execution and issue status
    if let Some(ref exec_id) = execution_id {
        recompute_execution_status(conn, exec_id);

        // Find issue_id from execution if not on the job directly
        let effective_issue_id = issue_id.clone().or_else(|| {
            executions::table
                .find(exec_id)
                .select(executions::issue_id)
                .first::<Option<String>>(conn)
                .ok()
                .flatten()
        });

        if let Some(ref iid) = effective_issue_id {
            recompute_issue_status(conn, iid);
        }
    } else if let Some(ref iid) = issue_id {
        recompute_issue_status(conn, iid);
    }

    // Emit db-change events
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "executions", "action": "update"}),
    );
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );

    // Send trigger event for terminal top-level jobs.
    // The dispatcher enriches and dispatches to matching recipes.
    if matches!(to, JobStatus::Complete | JobStatus::Failed) && parent_job_id.is_none() {
        let _ = trigger_tx.send(TriggerEvent::JobEnded {
            job_id: job_id.to_string(),
            status: to.to_string(),
            execution_id: execution_id.clone(),
            issue_id: issue_id.clone(),
            project_id: project_id.clone(),
        });
    }

    Ok(from)
}

fn validate_job_transition(
    from: &JobStatus,
    to: &JobStatus,
    job_id: &str,
) -> Result<(), TransitionError> {
    let valid = matches!(
        (from, to),
        (JobStatus::Pending, JobStatus::Ready) // DAG advancement: dependencies satisfied
            | (JobStatus::Pending, JobStatus::Failed) // cascade from upstream
            | (JobStatus::Ready, JobStatus::Running) // process spawned
            | (JobStatus::Ready, JobStatus::Blocked) // checkpoint gate before spawn
            | (JobStatus::Ready, JobStatus::Failed) // spawn failed
            | (JobStatus::Running, JobStatus::Complete)
            | (JobStatus::Running, JobStatus::Failed)
            | (JobStatus::Running, JobStatus::Blocked)
            | (JobStatus::Blocked, JobStatus::Complete)
            | (JobStatus::Blocked, JobStatus::Pending) // rejected with recovery
            | (JobStatus::Blocked, JobStatus::Failed) // rejected, no recovery
            // Follow-up continuation: user sends message after terminal state
            | (JobStatus::Complete, JobStatus::Running)
            | (JobStatus::Failed, JobStatus::Running)
            | (JobStatus::Blocked, JobStatus::Running)
            // Manager recovery: managers survive across multiple turns; their job
            // returns to Ready after each turn so it can be re-woken.
            | (JobStatus::Complete, JobStatus::Ready)
            | (JobStatus::Failed, JobStatus::Ready)
    );

    if !valid {
        return Err(TransitionError {
            entity: "job",
            id: job_id.to_string(),
            from: from.to_string(),
            to: to.to_string(),
            reason: "transition not allowed".to_string(),
        });
    }

    Ok(())
}

/// Cascade failure to direct downstream jobs in the same execution.
///
/// Uses the recipe DAG edges from the execution snapshot to find jobs whose
/// upstream control edges include the failed job. Each downstream job is
/// transitioned individually through `transition_job`, which:
/// - Validates the transition
/// - Sets proper timestamps
/// - Emits db-change and trigger events
/// - Recursively cascades to further downstream jobs
///
/// Diamond patterns (multiple paths to the same node) are handled naturally:
/// the first cascade transitions the job to Failed, and subsequent attempts
/// are silently ignored since the job is already terminal.
fn cascade_failure(
    conn: &mut SqliteConnection,
    emitter: &dyn EventEmitter,
    execution_id: &str,
    failed_job_id: &str,
    trigger_tx: &broadcast::Sender<TriggerEvent>,
) {
    // Get the recipe_node_id for the failed job
    let failed_node_id: Option<String> = jobs::table
        .find(failed_job_id)
        .select(jobs::recipe_node_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    let Some(failed_node_id) = failed_node_id else {
        return; // Standalone job, no DAG
    };

    // Load control edges from execution snapshot
    let control_edges: Vec<(String, String)> =
        match crate::execution::dag::load_edges_from_execution(conn, execution_id) {
            Ok(edges) => edges
                .into_iter()
                .filter(|e| e.edge_type == "control")
                .map(|e| (e.source_node_id, e.target_node_id))
                .collect(),
            Err(_) => return,
        };

    // Find direct downstream nodes (not transitive — recursion handles that)
    let direct_downstream_nodes: Vec<String> = control_edges
        .iter()
        .filter(|(source, _)| *source == failed_node_id)
        .map(|(_, target)| target.clone())
        .collect();

    if direct_downstream_nodes.is_empty() {
        return;
    }

    // Find pending/ready jobs for direct downstream nodes
    let downstream_job_ids: Vec<String> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::recipe_node_id.eq_any(&direct_downstream_nodes))
        .filter(jobs::status.eq("pending").or(jobs::status.eq("ready")))
        .select(jobs::id)
        .load(conn)
        .unwrap_or_default();

    // Transition each downstream job individually.
    // transition_job will recursively cascade to further downstream jobs.
    // Errors are logged but not propagated — a job may already be failed
    // from another cascade path (diamond pattern).
    for job_id in &downstream_job_ids {
        match transition_job(conn, emitter, job_id, JobStatus::Failed, trigger_tx) {
            Ok(_) => {
                log::info!(
                    "Cascaded failure from job {} to job {} in execution {}",
                    failed_job_id,
                    job_id,
                    execution_id
                );
            }
            Err(e) => {
                log::debug!(
                    "Cascade skip for job {} (already transitioned?): {}",
                    job_id,
                    e
                );
            }
        }
    }
}

fn db_error(
    job_id: &str,
    from: &JobStatus,
    to: &JobStatus,
    e: diesel::result::Error,
) -> TransitionError {
    TransitionError {
        entity: "job",
        id: job_id.to_string(),
        from: from.to_string(),
        to: to.to_string(),
        reason: format!("DB error: {}", e),
    }
}

/// Create a no-op trigger sender for tests.
///
/// Available under `#[cfg(test)]` (unit tests) and `feature = "test-utils"` (integration tests).
#[cfg(any(test, feature = "test-utils"))]
pub fn test_trigger_tx() -> broadcast::Sender<TriggerEvent> {
    let (tx, _) = broadcast::channel(1);
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewJob;
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    fn insert_job(conn: &mut SqliteConnection, id: &str, status: &str) {
        let project_id = create_test_project(conn, "Test", "TEST");
        insert_job_with_project(conn, id, status, &project_id, None, None);
    }

    fn insert_job_with_project(
        conn: &mut SqliteConnection,
        id: &str,
        status: &str,
        project_id: &str,
        execution_id: Option<&str>,
        recipe_node_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id,
            execution_id,
            manager_id: None,
            recipe_node_id,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status,
            agent_config_id: None,
            issue_id: None,
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: None,
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .unwrap();
    }

    fn mock_emitter() -> CapturingEmitter {
        CapturingEmitter::new()
    }

    #[test]
    fn test_pending_to_ready() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "pending");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Ready,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Pending);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "ready");
    }

    #[test]
    fn test_ready_to_running() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "ready");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Ready);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "running");
    }

    #[test]
    fn test_running_to_complete() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_running_to_failed() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Failed,
            &test_trigger_tx(),
        )
        .unwrap();

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "failed");
    }

    #[test]
    fn test_running_to_blocked() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Blocked,
            &test_trigger_tx(),
        )
        .unwrap();

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "blocked");
    }

    #[test]
    fn test_blocked_to_complete() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "blocked");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();
    }

    #[test]
    fn test_blocked_to_pending() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "blocked");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Pending,
            &test_trigger_tx(),
        )
        .unwrap();
    }

    #[test]
    fn test_blocked_to_failed() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "blocked");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Failed,
            &test_trigger_tx(),
        )
        .unwrap();
    }

    #[test]
    fn test_pending_to_failed_cascade() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "pending");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Failed,
            &test_trigger_tx(),
        )
        .unwrap();
    }

    #[test]
    fn test_complete_to_running_follow_up() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "complete");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Complete);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "running");
    }

    #[test]
    fn test_failed_to_running_follow_up() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "failed");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Failed);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "running");
    }

    #[test]
    fn test_blocked_to_running_follow_up() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "blocked");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Blocked);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "running");
    }

    #[test]
    fn test_follow_up_cycle() {
        // Running → Complete → Running → Complete (full follow-up cycle)
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_follow_up_preserves_started_at() {
        // Resume from Complete should not overwrite started_at
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "ready");
        let emitter = mock_emitter();

        // First start — sets started_at
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        let original_started_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::started_at)
            .first(&mut conn)
            .unwrap();
        assert!(original_started_at.is_some());

        // Complete then resume
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();

        let resumed_started_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::started_at)
            .first(&mut conn)
            .unwrap();
        assert_eq!(
            original_started_at, resumed_started_at,
            "started_at should be preserved on resume"
        );
    }

    #[test]
    fn test_follow_up_resume_updates_updated_at() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "ready");
        let emitter = mock_emitter();

        // Ready → Running → Complete
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        let pre_resume_updated: i32 = jobs::table
            .find("job-1")
            .select(jobs::updated_at)
            .first(&mut conn)
            .unwrap();

        // Complete → Running (resume)
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();

        let post_resume_updated: i32 = jobs::table
            .find("job-1")
            .select(jobs::updated_at)
            .first(&mut conn)
            .unwrap();
        assert!(
            post_resume_updated >= pre_resume_updated,
            "updated_at should advance on resume"
        );
    }

    #[test]
    fn test_follow_up_resume_preserves_completed_at() {
        // completed_at is not cleared on resume — it reflects the last completion time.
        // It will be overwritten when the job completes again.
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();
        let completed_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::completed_at)
            .first(&mut conn)
            .unwrap();
        assert!(completed_at.is_some());

        // Resume — completed_at should still be set (not cleared)
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();
        let resumed_completed_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::completed_at)
            .first(&mut conn)
            .unwrap();
        assert_eq!(
            completed_at, resumed_completed_at,
            "completed_at should not be cleared on resume"
        );
    }

    #[test]
    fn test_invalid_complete_to_pending() {
        // Complete → Pending is not a valid transition (even with new follow-up rules)
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "complete");
        let emitter = mock_emitter();

        let result = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Pending,
            &test_trigger_tx(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_failed_to_complete() {
        // Failed → Complete is not valid (must go through Running first)
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "failed");
        let emitter = mock_emitter();

        let result = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        );
        assert!(result.is_err());
    }

    // Manager recovery transitions

    #[test]
    fn test_complete_to_ready_for_manager_recovery() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "complete");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Ready,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Complete);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "ready");
    }

    #[test]
    fn test_failed_to_ready_for_manager_recovery() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "failed");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Ready,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Failed);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "ready");
    }

    #[test]
    fn test_manager_recovery_full_cycle() {
        // Complete → Ready → Running (the full manager wake path)
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "complete");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Ready,
            &test_trigger_tx(),
        )
        .unwrap();
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "running");
    }

    #[test]
    fn test_invalid_pending_to_running() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "pending");
        let emitter = mock_emitter();

        let result = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_pending_to_complete() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "pending");
        let emitter = mock_emitter();

        let result = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_not_found() {
        let mut conn = test_diesel_conn();
        let emitter = mock_emitter();

        let result = transition_job(
            &mut conn,
            &emitter,
            "nonexistent",
            JobStatus::Running,
            &test_trigger_tx(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("job not found"));
    }

    #[test]
    fn test_ready_to_failed() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "ready");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Failed,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Ready);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "failed");
    }

    #[test]
    fn test_running_sets_started_at() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "ready");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Running,
            &test_trigger_tx(),
        )
        .unwrap();

        let started_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::started_at)
            .first(&mut conn)
            .unwrap();
        assert!(started_at.is_some(), "started_at should be set on Running");
    }

    #[test]
    fn test_complete_sets_completed_at() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        let completed_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::completed_at)
            .first(&mut conn)
            .unwrap();
        assert!(
            completed_at.is_some(),
            "completed_at should be set on Complete"
        );
    }

    #[test]
    fn test_failed_sets_completed_at() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Failed,
            &test_trigger_tx(),
        )
        .unwrap();

        let completed_at: Option<i32> = jobs::table
            .find("job-1")
            .select(jobs::completed_at)
            .first(&mut conn)
            .unwrap();
        assert!(
            completed_at.is_some(),
            "completed_at should be set on Failed"
        );
    }

    #[test]
    fn test_emits_db_change_events() {
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "running");
        let emitter = mock_emitter();

        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        let db_changes = emitter.events_named("db-change");
        let tables: Vec<String> = db_changes
            .iter()
            .filter_map(|payload| {
                payload
                    .get("table")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .collect();
        assert!(
            tables.contains(&"jobs".to_string()),
            "should emit jobs db-change"
        );
        assert!(
            tables.contains(&"executions".to_string()),
            "should emit executions db-change"
        );
        assert!(
            tables.contains(&"issues".to_string()),
            "should emit issues db-change"
        );
    }

    // === Integration: transition_job cascades to execution and issue ===

    #[test]
    fn test_job_complete_recomputes_execution_to_complete() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TINT");
        let emitter = mock_emitter();

        // Insert issue
        let now = chrono::Utc::now().timestamp() as i32;
        let _ = diesel::sql_query(format!(
            "INSERT INTO issues (id, project_id, number, title, status, priority, created_at, updated_at) \
             VALUES ('issue-1', '{}', 1, 'Test', 'active', 0, {}, {})",
            project_id, now, now
        )).execute(&mut conn);

        // Insert execution linked to issue
        let _ = diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, issue_id, status, started_at, seq) \
             VALUES ('exec-1', 'recipe-1', 'issue-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn);

        // Insert job linked to execution
        insert_job_with_project(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            Some("exec-1"),
            None,
        );

        // Transition job to complete — should cascade
        transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        // Execution should now be complete
        let exec_status: String = executions::table
            .find("exec-1")
            .select(executions::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(exec_status, "complete");

        // Issue should be recomputed (complete execution → complete issue)
        let issue_status: String = crate::schema::issues::table
            .find("issue-1")
            .select(crate::schema::issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(issue_status, "complete");
    }

    #[test]
    fn test_job_failed_recomputes_execution_to_failed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TFAI");
        let emitter = mock_emitter();

        let now = chrono::Utc::now().timestamp() as i32;
        let _ = diesel::sql_query(format!(
            "INSERT INTO issues (id, project_id, number, title, status, priority, created_at, updated_at) \
             VALUES ('issue-2', '{}', 1, 'Test', 'active', 0, {}, {})",
            project_id, now, now
        )).execute(&mut conn);

        let _ = diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, issue_id, status, started_at, seq) \
             VALUES ('exec-2', 'recipe-1', 'issue-2', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn);

        insert_job_with_project(
            &mut conn,
            "job-2",
            "running",
            &project_id,
            Some("exec-2"),
            None,
        );

        transition_job(
            &mut conn,
            &emitter,
            "job-2",
            JobStatus::Failed,
            &test_trigger_tx(),
        )
        .unwrap();

        let exec_status: String = executions::table
            .find("exec-2")
            .select(executions::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(exec_status, "failed");

        let issue_status: String = crate::schema::issues::table
            .find("issue-2")
            .select(crate::schema::issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(issue_status, "failed");
    }

    #[test]
    fn test_execution_stays_running_if_other_jobs_pending() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TSRN");
        let emitter = mock_emitter();

        let now = chrono::Utc::now().timestamp() as i32;
        let _ = diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-3', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn);

        // Two jobs in same execution: one running, one pending
        insert_job_with_project(
            &mut conn,
            "job-3a",
            "running",
            &project_id,
            Some("exec-3"),
            None,
        );
        insert_job_with_project(
            &mut conn,
            "job-3b",
            "pending",
            &project_id,
            Some("exec-3"),
            None,
        );

        // Complete one job — execution should still be running (job-3b is pending)
        transition_job(
            &mut conn,
            &emitter,
            "job-3a",
            JobStatus::Complete,
            &test_trigger_tx(),
        )
        .unwrap();

        let exec_status: String = executions::table
            .find("exec-3")
            .select(executions::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(exec_status, "running");
    }

    #[test]
    fn test_ready_to_blocked() {
        // Ready → Blocked: checkpoint gate before spawn
        let mut conn = test_diesel_conn();
        insert_job(&mut conn, "job-1", "ready");
        let emitter = mock_emitter();

        let prev = transition_job(
            &mut conn,
            &emitter,
            "job-1",
            JobStatus::Blocked,
            &test_trigger_tx(),
        )
        .unwrap();
        assert_eq!(prev, JobStatus::Ready);

        let status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "blocked");
    }
}
