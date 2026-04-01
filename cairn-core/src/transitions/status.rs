//! Deterministic status recomputation for executions and issues.
//!
//! These functions are the ONLY writers to `executions.status` and `issues.status`.
//! They are called automatically by `transition_job` after every job state change,
//! and by `resolve_issue` for merge/close events.

use crate::models::{IssueAttention, IssueProgress};
use crate::schema::{
    action_runs, executions, issues, jobs, merge_requests, permission_requests, prompts, runs,
};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::{Deserialize, Serialize};

/// Computed execution status (not stored as a Rust enum — derived from jobs).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionStatus {
    #[default]
    Running,
    Complete,
    Failed,
}

impl std::fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStatus::Running => write!(f, "running"),
            ExecutionStatus::Complete => write!(f, "complete"),
            ExecutionStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ExecutionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "running" => Ok(ExecutionStatus::Running),
            "complete" => Ok(ExecutionStatus::Complete),
            "failed" => Ok(ExecutionStatus::Failed),
            // Backwards compat
            "paused" => Ok(ExecutionStatus::Running),
            _ => Err(format!("Unknown execution status: {}", s)),
        }
    }
}

/// Resolution type for issue merge/close.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
    Merged,
    Closed,
}

/// Recompute execution status from its jobs and action runs.
///
/// Rules:
/// - If any job is Running, Pending, or Blocked → Running
/// - If all jobs are terminal (Complete or Failed):
///   - Also check action runs: if any are non-terminal → Running
///   - If at least one Failed → Failed
///   - If all Complete → Complete
///
/// Updates `executions.status` and `executions.completed_at` in the DB.
/// Returns the computed status.
pub fn recompute_execution_status(
    conn: &mut SqliteConnection,
    execution_id: &str,
) -> ExecutionStatus {
    // Count jobs by status category
    let non_terminal_jobs: i64 = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::status.ne("complete").and(jobs::status.ne("failed")))
        .count()
        .get_result(conn)
        .unwrap_or(0);

    if non_terminal_jobs > 0 {
        // Still has active work
        let _ = diesel::update(executions::table.find(execution_id))
            .set(executions::status.eq("running"))
            .execute(conn);
        return ExecutionStatus::Running;
    }

    // All jobs are terminal — check action runs too
    let non_terminal_actions: i64 = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .filter(
            action_runs::status
                .ne("complete")
                .and(action_runs::status.ne("failed")),
        )
        .count()
        .get_result(conn)
        .unwrap_or(0);

    if non_terminal_actions > 0 {
        let _ = diesel::update(executions::table.find(execution_id))
            .set(executions::status.eq("running"))
            .execute(conn);
        return ExecutionStatus::Running;
    }

    // All terminal — check if any failed
    let failed_jobs: i64 = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::status.eq("failed"))
        .count()
        .get_result(conn)
        .unwrap_or(0);

    let failed_actions: i64 = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .filter(action_runs::status.eq("failed"))
        .count()
        .get_result(conn)
        .unwrap_or(0);

    let now = chrono::Utc::now().timestamp() as i32;

    if failed_jobs > 0 || failed_actions > 0 {
        let _ = diesel::update(executions::table.find(execution_id))
            .set((
                executions::status.eq("failed"),
                executions::completed_at.eq(Some(now)),
            ))
            .execute(conn);
        ExecutionStatus::Failed
    } else {
        let _ = diesel::update(executions::table.find(execution_id))
            .set((
                executions::status.eq("complete"),
                executions::completed_at.eq(Some(now)),
            ))
            .execute(conn);
        ExecutionStatus::Complete
    }
}

fn project_issue_status(progress: &IssueProgress, attention: &IssueAttention) -> &'static str {
    match progress {
        IssueProgress::Merged => "merged",
        IssueProgress::Closed => "closed",
        _ if attention.blocks_status_projection() => "waiting",
        IssueProgress::Backlog => "backlog",
        IssueProgress::Active => "active",
        IssueProgress::Complete => "complete",
        IssueProgress::Failed => "failed",
    }
}

pub fn recompute_issue_progress(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Option<IssueProgress> {
    let resolution: Option<(Option<i32>, Option<i32>)> = issues::table
        .find(issue_id)
        .select((issues::merged_at, issues::closed_at))
        .first(conn)
        .ok();

    let (merged_at, closed_at) = resolution?;
    if merged_at.is_some() {
        return Some(IssueProgress::Merged);
    }
    if closed_at.is_some() {
        return Some(IssueProgress::Closed);
    }

    let exec_statuses: Vec<String> = executions::table
        .filter(executions::issue_id.eq(issue_id))
        .select(executions::status)
        .load(conn)
        .unwrap_or_default();

    if exec_statuses.is_empty() {
        return Some(IssueProgress::Backlog);
    }
    if exec_statuses.iter().any(|s| s == "running") {
        return Some(IssueProgress::Active);
    }
    if exec_statuses.iter().any(|s| s == "complete") {
        return Some(IssueProgress::Complete);
    }
    Some(IssueProgress::Failed)
}

pub fn recompute_issue_attention(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Option<IssueAttention> {
    issues::table
        .find(issue_id)
        .select(issues::id)
        .first::<String>(conn)
        .ok()?;

    if has_unanswered_prompts(conn, issue_id) {
        return Some(IssueAttention::NeedsInput);
    }
    if has_pending_permission_requests(conn, issue_id) {
        return Some(IssueAttention::NeedsAuthorization);
    }
    if has_blocked_jobs(conn, issue_id) {
        return Some(IssueAttention::NeedsApproval);
    }
    if has_conflicting_pr(conn, issue_id) {
        return Some(IssueAttention::NeedsConflictResolution);
    }
    if has_pr_needing_review(conn, issue_id) {
        return Some(IssueAttention::NeedsReview);
    }
    if has_pr_ready_to_merge(conn, issue_id) {
        return Some(IssueAttention::NeedsMerge);
    }
    Some(IssueAttention::None)
}

/// Recompute issue progress, attention, and projected legacy status.
pub fn recompute_issue_status(conn: &mut SqliteConnection, issue_id: &str) {
    let Some(progress) = recompute_issue_progress(conn, issue_id) else {
        return;
    };
    let Some(attention) = recompute_issue_attention(conn, issue_id) else {
        return;
    };
    let status = project_issue_status(&progress, &attention);

    let now = chrono::Utc::now().timestamp() as i32;

    // Terminal states (complete/failed) should set completed_at if not already set
    if matches!(progress, IssueProgress::Complete | IssueProgress::Failed) {
        let existing_completed_at: Option<i32> = issues::table
            .find(issue_id)
            .select(issues::completed_at)
            .first(conn)
            .ok()
            .flatten();

        if existing_completed_at.is_none() {
            let _ = diesel::update(issues::table.find(issue_id))
                .set((
                    issues::status.eq(status),
                    issues::progress.eq(progress.to_string()),
                    issues::attention.eq(attention.to_string()),
                    issues::completed_at.eq(Some(now)),
                    issues::updated_at.eq(now),
                ))
                .execute(conn);
            return;
        }
    }

    let _ = diesel::update(issues::table.find(issue_id))
        .set((
            issues::status.eq(status),
            issues::progress.eq(progress.to_string()),
            issues::attention.eq(attention.to_string()),
            issues::updated_at.eq(now),
        ))
        .execute(conn);
}

/// Check for unanswered prompts linked to this issue.
fn has_unanswered_prompts(conn: &mut SqliteConnection, issue_id: &str) -> bool {
    let count: i64 = prompts::table
        .inner_join(runs::table.on(prompts::run_id.eq(runs::id)))
        .filter(runs::issue_id.eq(issue_id))
        .filter(prompts::response.is_null())
        .count()
        .get_result(conn)
        .unwrap_or(0);
    count > 0
}

fn has_pending_permission_requests(conn: &mut SqliteConnection, issue_id: &str) -> bool {
    let count: i64 = permission_requests::table
        .inner_join(runs::table.on(permission_requests::run_id.eq(runs::id)))
        .filter(runs::issue_id.eq(issue_id))
        .filter(permission_requests::status.eq("pending"))
        .count()
        .get_result(conn)
        .unwrap_or(0);
    count > 0
}

/// Check for jobs blocked on checkpoint approval.
fn has_blocked_jobs(conn: &mut SqliteConnection, issue_id: &str) -> bool {
    let count: i64 = jobs::table
        .inner_join(executions::table.on(jobs::execution_id.eq(executions::id.nullable())))
        .filter(executions::issue_id.eq(issue_id))
        .filter(jobs::status.eq("blocked"))
        .count()
        .get_result(conn)
        .unwrap_or(0);
    count > 0
}

/// Check for open PRs linked to this issue that need conflict resolution.
fn has_conflicting_pr(conn: &mut SqliteConnection, issue_id: &str) -> bool {
    let count: i64 = merge_requests::table
        .filter(merge_requests::issue_id.eq(issue_id))
        .filter(merge_requests::status.eq("open"))
        .filter(merge_requests::github_mergeable.eq(Some("CONFLICTING")))
        .count()
        .get_result(conn)
        .unwrap_or(0);
    count > 0
}

/// Check for open PRs linked to this issue that still need review.
fn has_pr_needing_review(conn: &mut SqliteConnection, issue_id: &str) -> bool {
    let count: i64 = merge_requests::table
        .filter(merge_requests::issue_id.eq(issue_id))
        .filter(merge_requests::status.eq("open"))
        .filter(
            merge_requests::github_mergeable
                .is_null()
                .or(merge_requests::github_mergeable.ne(Some("CONFLICTING"))),
        )
        .filter(
            merge_requests::github_review
                .is_null()
                .or(merge_requests::github_review.eq(Some("REVIEW_REQUIRED"))),
        )
        .count()
        .get_result(conn)
        .unwrap_or(0);
    count > 0
}

/// Check for open PRs linked to this issue that are ready for merge.
fn has_pr_ready_to_merge(conn: &mut SqliteConnection, issue_id: &str) -> bool {
    let count: i64 = merge_requests::table
        .filter(merge_requests::issue_id.eq(issue_id))
        .filter(merge_requests::status.eq("open"))
        .filter(merge_requests::github_mergeable.eq(Some("MERGEABLE")))
        .filter(merge_requests::github_review.eq(Some("APPROVED")))
        .count()
        .get_result(conn)
        .unwrap_or(0);
    count > 0
}

/// Resolve an issue by setting a resolution timestamp.
///
/// This is used for external events (PR merged, user closes issue).
/// Sets the appropriate timestamp and recomputes status.
///
/// Accepts an optional `Clock` for testability. When `None`, uses `chrono::Utc::now()`.
/// Returns the IDs of sessions that were closed, so callers with access to
/// the process state can evict warm processes for those sessions.
pub fn resolve_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
    resolution: Resolution,
    clock: Option<&dyn crate::services::Clock>,
) -> Result<Vec<String>, String> {
    let now = clock
        .map(|c| c.now() as i32)
        .unwrap_or_else(|| chrono::Utc::now().timestamp() as i32);

    let reason = match resolution {
        Resolution::Merged => {
            diesel::update(issues::table.find(issue_id))
                .set((
                    issues::merged_at.eq(Some(now)),
                    issues::completed_at.eq(Some(now)),
                    issues::progress.eq("merged"),
                    issues::attention.eq("none"),
                    issues::status.eq("merged"),
                    issues::updated_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| format!("Failed to resolve issue: {}", e))?;
            "issue_merged"
        }
        Resolution::Closed => {
            diesel::update(issues::table.find(issue_id))
                .set((
                    issues::closed_at.eq(Some(now)),
                    issues::completed_at.eq(Some(now)),
                    issues::progress.eq("closed"),
                    issues::attention.eq("none"),
                    issues::status.eq("closed"),
                    issues::updated_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| format!("Failed to resolve issue: {}", e))?;
            "issue_closed"
        }
    };

    // Close all open sessions for this issue and return their IDs
    // so callers with process state can evict warm processes.
    let closed_session_ids =
        crate::sessions::queries::close_sessions_for_issue(conn, issue_id, reason)?;

    Ok(closed_session_ids)
}

/// Unresolve an issue (clear resolution timestamps and recompute from executions).
///
/// Accepts an optional `Clock` for testability. When `None`, uses `chrono::Utc::now()`.
pub fn unresolve_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
    clock: Option<&dyn crate::services::Clock>,
) {
    let now = clock
        .map(|c| c.now() as i32)
        .unwrap_or_else(|| chrono::Utc::now().timestamp() as i32);

    let _ = diesel::update(issues::table.find(issue_id))
        .set((
            issues::merged_at.eq(None::<i32>),
            issues::closed_at.eq(None::<i32>),
            issues::completed_at.eq(None::<i32>),
            issues::updated_at.eq(now),
        ))
        .execute(conn);

    recompute_issue_status(conn, issue_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewExecution, NewIssue, NewJob};
    use crate::test_utils::{create_test_project, test_diesel_conn};

    fn insert_issue(conn: &mut SqliteConnection, id: &str, project_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_issue = NewIssue {
            id,
            project_id,
            number: 1,
            title: "Test",
            description: None,
            status: "backlog",
            progress: "backlog",
            attention: "none",
            priority: Some(0),
            created_at: now,
            updated_at: now,
            model: None,
            manager_id: None,
        };
        diesel::insert_into(issues::table)
            .values(&new_issue)
            .execute(conn)
            .unwrap();
    }

    fn insert_execution(conn: &mut SqliteConnection, id: &str, issue_id: Option<&str>) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_exec = NewExecution {
            id,
            recipe_id: "recipe-1",
            issue_id,
            project_id: None,
            status: "running",
            started_at: now,
            completed_at: None,
            snapshot: None,
            seq: Some(1),
            initiator_sub: None,
            initiator_auth_mode: None,
            initiator_org_id: None,
            triggered_by: "manual",
        };
        diesel::insert_into(executions::table)
            .values(&new_exec)
            .execute(conn)
            .unwrap();
    }

    fn insert_job_for_exec(
        conn: &mut SqliteConnection,
        id: &str,
        execution_id: &str,
        status: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        // Ensure a project exists for the FK constraint
        let _ = diesel::sql_query(
            "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) \
             VALUES ('test-project', 'default', 'Test', 'TEST-JOB', '/tmp/test', 0, 0)"
        ).execute(conn);
        let new_job = NewJob {
            id,
            execution_id: Some(execution_id),
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status,
            agent_config_id: None,
            issue_id: None,
            project_id: "test-project",
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

    // === Execution status tests ===

    #[test]
    fn test_execution_running_when_jobs_pending() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-1", None);
        insert_job_for_exec(&mut conn, "job-1", "exec-1", "pending");
        insert_job_for_exec(&mut conn, "job-2", "exec-1", "complete");

        let status = recompute_execution_status(&mut conn, "exec-1");
        assert_eq!(status, ExecutionStatus::Running);
    }

    #[test]
    fn test_execution_running_when_jobs_running() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-1", None);
        insert_job_for_exec(&mut conn, "job-1", "exec-1", "running");

        let status = recompute_execution_status(&mut conn, "exec-1");
        assert_eq!(status, ExecutionStatus::Running);
    }

    #[test]
    fn test_execution_running_when_jobs_blocked() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-1", None);
        insert_job_for_exec(&mut conn, "job-1", "exec-1", "blocked");

        let status = recompute_execution_status(&mut conn, "exec-1");
        assert_eq!(status, ExecutionStatus::Running);
    }

    #[test]
    fn test_execution_complete_when_all_jobs_complete() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-1", None);
        insert_job_for_exec(&mut conn, "job-1", "exec-1", "complete");
        insert_job_for_exec(&mut conn, "job-2", "exec-1", "complete");

        let status = recompute_execution_status(&mut conn, "exec-1");
        assert_eq!(status, ExecutionStatus::Complete);
    }

    #[test]
    fn test_execution_failed_when_any_job_failed() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-1", None);
        insert_job_for_exec(&mut conn, "job-1", "exec-1", "complete");
        insert_job_for_exec(&mut conn, "job-2", "exec-1", "failed");

        let status = recompute_execution_status(&mut conn, "exec-1");
        assert_eq!(status, ExecutionStatus::Failed);
    }

    // === Issue status tests ===

    #[test]
    fn test_issue_backlog_when_no_executions() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "backlog");
    }

    #[test]
    fn test_issue_active_when_execution_running() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);
        insert_execution(&mut conn, "exec-1", Some("issue-1"));

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "active");
    }

    #[test]
    fn test_issue_complete_when_execution_complete() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);
        insert_execution(&mut conn, "exec-1", Some("issue-1"));
        // Mark execution as complete
        diesel::update(executions::table.find("exec-1"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_issue_failed_when_execution_failed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);
        insert_execution(&mut conn, "exec-1", Some("issue-1"));
        diesel::update(executions::table.find("exec-1"))
            .set(executions::status.eq("failed"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "failed");
    }

    #[test]
    fn test_issue_merged_takes_precedence() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);
        insert_execution(&mut conn, "exec-1", Some("issue-1"));

        // Set merged_at
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(issues::table.find("issue-1"))
            .set(issues::merged_at.eq(Some(now)))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "merged");
    }

    #[test]
    fn test_issue_closed_takes_precedence() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);
        insert_execution(&mut conn, "exec-1", Some("issue-1"));

        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(issues::table.find("issue-1"))
            .set(issues::closed_at.eq(Some(now)))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "closed");
    }

    #[test]
    fn test_resolve_issue_merged() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);

        resolve_issue(&mut conn, "issue-1", Resolution::Merged, None).unwrap();

        let (status, merged_at): (String, Option<i32>) = issues::table
            .find("issue-1")
            .select((issues::status, issues::merged_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "merged");
        assert!(merged_at.is_some());
    }

    #[test]
    fn test_resolve_issue_closed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);

        resolve_issue(&mut conn, "issue-1", Resolution::Closed, None).unwrap();

        let (status, closed_at): (String, Option<i32>) = issues::table
            .find("issue-1")
            .select((issues::status, issues::closed_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "closed");
        assert!(closed_at.is_some());
    }

    #[test]
    fn test_unresolve_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);

        resolve_issue(&mut conn, "issue-1", Resolution::Merged, None).unwrap();

        // Unresolve
        unresolve_issue(&mut conn, "issue-1", None);

        let (status, merged_at): (String, Option<i32>) = issues::table
            .find("issue-1")
            .select((issues::status, issues::merged_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "backlog"); // No executions → backlog
        assert!(merged_at.is_none());
    }

    // === Execution status with action_runs ===

    fn insert_action_run(conn: &mut SqliteConnection, id: &str, execution_id: &str, status: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        // Ensure an action_config exists for the FK constraint
        let _ = diesel::sql_query(
            "INSERT OR IGNORE INTO action_configs (id, name, description, is_builtin, created_at, updated_at) \
             VALUES ('test-config', 'test', 'test', 0, 0, 0)"
        ).execute(conn);
        // Ensure a project exists
        let _ = diesel::sql_query(
            "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) \
             VALUES ('test-project-ar', 'default', 'Test', 'TSAR', '/tmp/test', 0, 0)"
        ).execute(conn);

        use crate::diesel_models::NewActionRun;
        let new_ar = NewActionRun {
            id,
            execution_id,
            recipe_node_id: "node-1",
            action_config_id: "test-config",
            issue_id: None,
            project_id: "test-project-ar",
            status,
            inputs: None,
            output: None,
            error_message: None,
            started_at: None,
            completed_at: None,
            created_at: now,
            parent_job_id: None,
        };
        diesel::insert_into(action_runs::table)
            .values(&new_ar)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_execution_running_when_action_runs_pending() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-ar-1", None);
        // All jobs complete, but action run still running
        insert_job_for_exec(&mut conn, "job-ar-1", "exec-ar-1", "complete");
        insert_action_run(&mut conn, "ar-1", "exec-ar-1", "running");

        let status = recompute_execution_status(&mut conn, "exec-ar-1");
        assert_eq!(status, ExecutionStatus::Running);
    }

    #[test]
    fn test_execution_failed_when_action_run_failed() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-ar-2", None);
        insert_job_for_exec(&mut conn, "job-ar-2", "exec-ar-2", "complete");
        insert_action_run(&mut conn, "ar-2", "exec-ar-2", "failed");

        let status = recompute_execution_status(&mut conn, "exec-ar-2");
        assert_eq!(status, ExecutionStatus::Failed);
    }

    #[test]
    fn test_execution_complete_when_all_jobs_and_actions_complete() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-ar-3", None);
        insert_job_for_exec(&mut conn, "job-ar-3", "exec-ar-3", "complete");
        insert_action_run(&mut conn, "ar-3", "exec-ar-3", "complete");

        let status = recompute_execution_status(&mut conn, "exec-ar-3");
        assert_eq!(status, ExecutionStatus::Complete);
    }

    #[test]
    fn test_execution_complete_when_no_jobs() {
        let mut conn = test_diesel_conn();
        insert_execution(&mut conn, "exec-empty", None);
        // No jobs, no action_runs — all vacuously terminal → complete
        let status = recompute_execution_status(&mut conn, "exec-empty");
        assert_eq!(status, ExecutionStatus::Complete);
    }

    // === Backwards compatibility parsing ===

    #[test]
    fn test_execution_status_paused_parses_as_running() {
        let status: ExecutionStatus = "paused".parse().unwrap();
        assert_eq!(status, ExecutionStatus::Running);
    }

    #[test]
    fn test_issue_status_waiting_parses_as_waiting() {
        use crate::models::IssueStatus;
        let status: IssueStatus = "waiting".parse().unwrap();
        assert_eq!(status, IssueStatus::Waiting);
    }

    // === unresolve with executions ===

    #[test]
    fn test_unresolve_with_running_execution_restores_active() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TUNR");
        insert_issue(&mut conn, "issue-unr", &project_id);
        insert_execution(&mut conn, "exec-unr", Some("issue-unr"));
        // exec is still "running" (default from insert_execution)

        resolve_issue(&mut conn, "issue-unr", Resolution::Merged, None).unwrap();

        let status: String = issues::table
            .find("issue-unr")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "merged");

        unresolve_issue(&mut conn, "issue-unr", None);

        let status: String = issues::table
            .find("issue-unr")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(
            status, "active",
            "should recompute to active from running execution"
        );
    }

    #[test]
    fn test_issue_active_when_one_running_one_complete() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-1", &project_id);
        insert_execution(&mut conn, "exec-1", Some("issue-1"));
        insert_execution(&mut conn, "exec-2", Some("issue-1"));

        diesel::update(executions::table.find("exec-1"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();
        // exec-2 still "running"

        recompute_issue_status(&mut conn, "issue-1");

        let status: String = issues::table
            .find("issue-1")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "active");
    }

    // === Waiting status tests ===

    fn insert_run_for_issue(
        conn: &mut SqliteConnection,
        id: &str,
        issue_id: &str,
        job_id: &str,
        status: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        use crate::diesel_models::NewRun;
        let new_run = NewRun {
            id,
            issue_id: Some(issue_id),
            project_id: None,
            job_id: Some(job_id),
            chat_id: None,
            status: Some(status),
            session_id: None,
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .unwrap();
    }

    fn insert_unanswered_prompt(conn: &mut SqliteConnection, id: &str, run_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        use crate::diesel_models::NewPrompt;
        let new_prompt = NewPrompt {
            id,
            run_id,
            questions: "[]",
            response: None,
            created_at: now,
            answered_at: None,
            turn_id: None,
        };
        diesel::insert_into(prompts::table)
            .values(&new_prompt)
            .execute(conn)
            .unwrap();
    }

    fn insert_merge_request(
        conn: &mut SqliteConnection,
        id: &str,
        job_id: &str,
        issue_id: &str,
        status: &str,
    ) {
        diesel::sql_query(format!(
            "INSERT INTO merge_requests (id, job_id, issue_id, project_id, title, source_branch, target_branch, status, merge_method, opened_at, updated_at) \
             VALUES ('{id}', '{job_id}', '{issue_id}', (SELECT project_id FROM jobs WHERE id = '{job_id}'), 'Test PR', 'feature', 'main', '{status}', 'squash', 0, 0)"
        ))
        .execute(conn)
        .unwrap();
    }

    fn insert_merge_request_full(
        conn: &mut SqliteConnection,
        id: &str,
        job_id: &str,
        issue_id: &str,
        status: &str,
        github_review: Option<&str>,
        github_mergeable: Option<&str>,
    ) {
        let review_sql = github_review
            .map(|r| format!("'{}'", r))
            .unwrap_or_else(|| "NULL".to_string());
        let mergeable_sql = github_mergeable
            .map(|m| format!("'{}'", m))
            .unwrap_or_else(|| "NULL".to_string());
        diesel::sql_query(format!(
            "INSERT INTO merge_requests (id, job_id, issue_id, project_id, title, source_branch, target_branch, status, merge_method, github_review, github_mergeable, opened_at, updated_at) \
             VALUES ('{id}', '{job_id}', '{issue_id}', (SELECT project_id FROM jobs WHERE id = '{job_id}'), 'Test PR', 'feature', 'main', '{status}', 'squash', {review_sql}, {mergeable_sql}, 0, 0)"
        ))
        .execute(conn)
        .unwrap();
    }

    fn insert_permission_request(conn: &mut SqliteConnection, id: &str, run_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let _ = diesel::sql_query(format!(
            "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at) \
             VALUES ('{id}', '{run_id}', 'tool-1', 'bash', '{{}}', 'pending', {now})",
        ))
        .execute(conn);
    }

    fn issue_state(conn: &mut SqliteConnection, issue_id: &str) -> (String, String, String) {
        issues::table
            .find(issue_id)
            .select((issues::status, issues::progress, issues::attention))
            .first(conn)
            .unwrap()
    }

    #[test]
    fn test_issue_waiting_when_open_pr() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TWPR");
        insert_issue(&mut conn, "issue-pr", &project_id);
        insert_execution(&mut conn, "exec-pr", Some("issue-pr"));

        diesel::update(executions::table.find("exec-pr"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        insert_job_for_exec(&mut conn, "job-pr", "exec-pr", "complete");
        insert_merge_request(&mut conn, "mr-1", "job-pr", "issue-pr", "open");

        recompute_issue_status(&mut conn, "issue-pr");

        let status: String = issues::table
            .find("issue-pr")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "waiting");
    }

    #[test]
    fn test_issue_complete_when_pr_merged() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TCPM");
        insert_issue(&mut conn, "issue-pm", &project_id);
        insert_execution(&mut conn, "exec-pm", Some("issue-pm"));

        // merged_at takes precedence over open PR
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(issues::table.find("issue-pm"))
            .set(issues::merged_at.eq(Some(now)))
            .execute(&mut conn)
            .unwrap();

        insert_job_for_exec(&mut conn, "job-pm", "exec-pm", "complete");
        insert_merge_request(&mut conn, "mr-pm", "job-pm", "issue-pm", "open");

        recompute_issue_status(&mut conn, "issue-pm");

        let status: String = issues::table
            .find("issue-pm")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "merged");
    }

    #[test]
    fn test_issue_complete_when_no_pr() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TCNP");
        insert_issue(&mut conn, "issue-np", &project_id);
        insert_execution(&mut conn, "exec-np", Some("issue-np"));

        diesel::update(executions::table.find("exec-np"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-np");

        let status: String = issues::table
            .find("issue-np")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_issue_waiting_when_jobs_blocked() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TWJB");
        insert_issue(&mut conn, "issue-blk", &project_id);
        insert_execution(&mut conn, "exec-blk", Some("issue-blk"));

        insert_job_for_exec(&mut conn, "job-blk-1", "exec-blk", "blocked");
        insert_job_for_exec(&mut conn, "job-blk-2", "exec-blk", "complete");

        recompute_issue_status(&mut conn, "issue-blk");

        let status: String = issues::table
            .find("issue-blk")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "waiting");
    }

    #[test]
    fn test_issue_waiting_when_blocked_and_running_jobs() {
        // Blocked job should trigger Waiting even when other jobs are still running
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TWBR");
        insert_issue(&mut conn, "issue-br", &project_id);
        insert_execution(&mut conn, "exec-br", Some("issue-br"));

        insert_job_for_exec(&mut conn, "job-br-1", "exec-br", "running");
        insert_job_for_exec(&mut conn, "job-br-2", "exec-br", "blocked");

        recompute_issue_status(&mut conn, "issue-br");

        let status: String = issues::table
            .find("issue-br")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "waiting");
    }

    #[test]
    fn test_issue_waiting_when_unanswered_prompt() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TWUP");
        insert_issue(&mut conn, "issue-prompt", &project_id);
        insert_execution(&mut conn, "exec-prompt", Some("issue-prompt"));

        insert_job_for_exec(&mut conn, "job-prompt", "exec-prompt", "running");
        insert_run_for_issue(
            &mut conn,
            "run-prompt",
            "issue-prompt",
            "job-prompt",
            "idle",
        );
        insert_unanswered_prompt(&mut conn, "prompt-1", "run-prompt");

        recompute_issue_status(&mut conn, "issue-prompt");

        let status: String = issues::table
            .find("issue-prompt")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "waiting");
    }

    #[test]
    fn test_issue_state_split_for_unanswered_prompt() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TSIP");
        insert_issue(&mut conn, "issue-split-prompt", &project_id);
        insert_execution(&mut conn, "exec-split-prompt", Some("issue-split-prompt"));
        insert_job_for_exec(
            &mut conn,
            "job-split-prompt",
            "exec-split-prompt",
            "running",
        );
        insert_run_for_issue(
            &mut conn,
            "run-split-prompt",
            "issue-split-prompt",
            "job-split-prompt",
            "idle",
        );
        insert_unanswered_prompt(&mut conn, "prompt-split", "run-split-prompt");

        recompute_issue_status(&mut conn, "issue-split-prompt");

        let (status, progress, attention) = issue_state(&mut conn, "issue-split-prompt");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "active");
        assert_eq!(attention, "needs_input");
    }

    #[test]
    fn test_issue_state_split_for_permission_request() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TSIA");
        insert_issue(&mut conn, "issue-split-auth", &project_id);
        insert_execution(&mut conn, "exec-split-auth", Some("issue-split-auth"));
        insert_job_for_exec(&mut conn, "job-split-auth", "exec-split-auth", "running");
        insert_run_for_issue(
            &mut conn,
            "run-split-auth",
            "issue-split-auth",
            "job-split-auth",
            "idle",
        );
        insert_permission_request(&mut conn, "perm-split", "run-split-auth");

        recompute_issue_status(&mut conn, "issue-split-auth");

        let (status, progress, attention) = issue_state(&mut conn, "issue-split-auth");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "active");
        assert_eq!(attention, "needs_authorization");
    }

    #[test]
    fn test_issue_state_split_for_mergeable_open_pr() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TSIM");
        insert_issue(&mut conn, "issue-split-merge", &project_id);
        insert_execution(&mut conn, "exec-split-merge", Some("issue-split-merge"));

        diesel::update(executions::table.find("exec-split-merge"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        insert_job_for_exec(&mut conn, "job-split-merge", "exec-split-merge", "complete");
        insert_merge_request_full(
            &mut conn,
            "mr-split-merge",
            "job-split-merge",
            "issue-split-merge",
            "open",
            Some("APPROVED"),
            Some("MERGEABLE"),
        );

        recompute_issue_status(&mut conn, "issue-split-merge");

        let (status, progress, attention) = issue_state(&mut conn, "issue-split-merge");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "complete");
        assert_eq!(attention, "needs_merge");
    }

    #[test]
    fn test_issue_waiting_when_prompt_and_other_agent_running() {
        // One agent asks a question, another keeps working — should still be Waiting
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TWPR2");
        insert_issue(&mut conn, "issue-par", &project_id);
        insert_execution(&mut conn, "exec-par", Some("issue-par"));

        // Agent 1: running, has unanswered prompt
        insert_job_for_exec(&mut conn, "job-par-1", "exec-par", "running");
        insert_run_for_issue(&mut conn, "run-par-1", "issue-par", "job-par-1", "idle");
        insert_unanswered_prompt(&mut conn, "prompt-par", "run-par-1");

        // Agent 2: still actively working
        insert_job_for_exec(&mut conn, "job-par-2", "exec-par", "running");
        insert_run_for_issue(&mut conn, "run-par-2", "issue-par", "job-par-2", "running");

        recompute_issue_status(&mut conn, "issue-par");

        let status: String = issues::table
            .find("issue-par")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "waiting");
    }

    #[test]
    fn test_issue_active_when_no_pending_actions() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TANPA");
        insert_issue(&mut conn, "issue-npa", &project_id);
        insert_execution(&mut conn, "exec-npa", Some("issue-npa"));

        // Running job, running run, no prompts — normal active work
        insert_job_for_exec(&mut conn, "job-npa", "exec-npa", "running");
        insert_run_for_issue(&mut conn, "run-npa", "issue-npa", "job-npa", "running");

        recompute_issue_status(&mut conn, "issue-npa");

        let status: String = issues::table
            .find("issue-npa")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "active");
    }

    #[test]
    fn test_issue_active_when_prompt_answered() {
        // Prompt exists but has been answered — should NOT be waiting
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TAPA");
        insert_issue(&mut conn, "issue-ans", &project_id);
        insert_execution(&mut conn, "exec-ans", Some("issue-ans"));

        insert_job_for_exec(&mut conn, "job-ans", "exec-ans", "running");
        insert_run_for_issue(&mut conn, "run-ans", "issue-ans", "job-ans", "running");

        // Insert an answered prompt (response is not null)
        let now = chrono::Utc::now().timestamp() as i32;
        use crate::diesel_models::NewPrompt;
        let answered_prompt = NewPrompt {
            id: "prompt-ans",
            run_id: "run-ans",
            questions: "[]",
            response: Some("user response"),
            created_at: now,
            answered_at: Some(now),
            turn_id: None,
        };
        diesel::insert_into(prompts::table)
            .values(&answered_prompt)
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-ans");

        let status: String = issues::table
            .find("issue-ans")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "active");
    }

    #[test]
    fn test_issue_not_waiting_when_pr_merged() {
        // A merged PR (pr_status="merged") should NOT trigger Waiting.
        // This verifies has_open_pr filters on pr_status="open" specifically.
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TNWM");
        insert_issue(&mut conn, "issue-mpr", &project_id);
        insert_execution(&mut conn, "exec-mpr", Some("issue-mpr"));

        diesel::update(executions::table.find("exec-mpr"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        insert_job_for_exec(&mut conn, "job-mpr", "exec-mpr", "complete");
        insert_merge_request(&mut conn, "mr-mpr", "job-mpr", "issue-mpr", "merged");

        recompute_issue_status(&mut conn, "issue-mpr");

        let status: String = issues::table
            .find("issue-mpr")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        // PR is merged but issue.merged_at not set — should be complete, not waiting
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_issue_waiting_open_pr_via_merge_request_issue_id() {
        // merge_requests has issue_id directly — PR detection works regardless of
        // which execution or job the merge_request belongs to.
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TWAI");
        insert_issue(&mut conn, "issue-ai", &project_id);
        insert_execution(&mut conn, "exec-ai", Some("issue-ai"));

        diesel::update(executions::table.find("exec-ai"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        // Create a job linked to a different (unrelated) execution, but the
        // merge_request's issue_id points to issue-ai — it should still be detected.
        insert_execution(&mut conn, "exec-other", None);
        insert_job_for_exec(&mut conn, "job-ai", "exec-other", "complete");
        insert_merge_request(&mut conn, "mr-ai", "job-ai", "issue-ai", "open");

        recompute_issue_status(&mut conn, "issue-ai");

        let status: String = issues::table
            .find("issue-ai")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "waiting");
    }

    #[test]
    fn test_issue_closed_takes_precedence_over_pending_items() {
        // closed_at should beat pending user actions (open PR, blocked jobs, prompts)
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TCPO");
        insert_issue(&mut conn, "issue-cpo", &project_id);
        insert_execution(&mut conn, "exec-cpo", Some("issue-cpo"));

        // Set up pending items: open PR + blocked job
        insert_job_for_exec(&mut conn, "job-cpo-1", "exec-cpo", "complete");
        insert_merge_request(&mut conn, "mr-cpo", "job-cpo-1", "issue-cpo", "open");
        insert_job_for_exec(&mut conn, "job-cpo-2", "exec-cpo", "blocked");

        // But issue is closed
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(issues::table.find("issue-cpo"))
            .set(issues::closed_at.eq(Some(now)))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-cpo");

        let status: String = issues::table
            .find("issue-cpo")
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "closed");
    }

    #[test]
    fn test_resolve_issue_uses_injected_clock() {
        use crate::services::testing::MockClock;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-clk", &project_id);

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000042);

        resolve_issue(&mut conn, "issue-clk", Resolution::Merged, Some(&clock)).unwrap();

        let (merged_at, updated_at): (Option<i32>, i32) = issues::table
            .find("issue-clk")
            .select((issues::merged_at, issues::updated_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(merged_at, Some(1700000042));
        assert_eq!(updated_at, 1700000042);
    }

    #[test]
    fn test_unresolve_issue_uses_injected_clock() {
        use crate::services::testing::MockClock;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        insert_issue(&mut conn, "issue-unclk", &project_id);

        resolve_issue(&mut conn, "issue-unclk", Resolution::Merged, None).unwrap();

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000099);

        unresolve_issue(&mut conn, "issue-unclk", Some(&clock));

        // unresolve_issue clears resolution timestamps then calls recompute_issue_status,
        // which overwrites updated_at with chrono::Utc::now(). So we verify the
        // clock-controlled fields (merged_at, closed_at, completed_at) are cleared.
        let (merged_at, closed_at, completed_at): (Option<i32>, Option<i32>, Option<i32>) =
            issues::table
                .find("issue-unclk")
                .select((issues::merged_at, issues::closed_at, issues::completed_at))
                .first(&mut conn)
                .unwrap();
        assert!(merged_at.is_none());
        assert!(closed_at.is_none());
        assert!(completed_at.is_none());
    }

    // === completed_at consistency tests ===

    #[test]
    fn test_resolve_closed_sets_completed_at() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRCC");
        insert_issue(&mut conn, "issue-cc", &project_id);

        resolve_issue(&mut conn, "issue-cc", Resolution::Closed, None).unwrap();

        let (status, completed_at, closed_at): (String, Option<i32>, Option<i32>) = issues::table
            .find("issue-cc")
            .select((issues::status, issues::completed_at, issues::closed_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "closed");
        assert!(
            completed_at.is_some(),
            "Closed resolution should set completed_at"
        );
        assert!(closed_at.is_some());
        assert_eq!(
            completed_at, closed_at,
            "completed_at and closed_at should match"
        );
    }

    #[test]
    fn test_recompute_sets_completed_at_on_complete() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRCA");
        insert_issue(&mut conn, "issue-ca", &project_id);
        insert_execution(&mut conn, "exec-ca", Some("issue-ca"));

        // Mark execution complete
        diesel::update(executions::table.find("exec-ca"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-ca");

        let (status, completed_at): (String, Option<i32>) = issues::table
            .find("issue-ca")
            .select((issues::status, issues::completed_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "complete");
        assert!(
            completed_at.is_some(),
            "recompute to 'complete' should set completed_at"
        );
    }

    #[test]
    fn test_recompute_sets_completed_at_on_failed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRCF");
        insert_issue(&mut conn, "issue-cf", &project_id);
        insert_execution(&mut conn, "exec-cf", Some("issue-cf"));

        // Mark execution failed
        diesel::update(executions::table.find("exec-cf"))
            .set(executions::status.eq("failed"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-cf");

        let (status, completed_at): (String, Option<i32>) = issues::table
            .find("issue-cf")
            .select((issues::status, issues::completed_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "failed");
        assert!(
            completed_at.is_some(),
            "recompute to 'failed' should set completed_at"
        );
    }

    #[test]
    fn test_recompute_preserves_existing_completed_at() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRCP");
        insert_issue(&mut conn, "issue-cp", &project_id);
        insert_execution(&mut conn, "exec-cp", Some("issue-cp"));

        // Set completed_at manually to a known value
        diesel::update(issues::table.find("issue-cp"))
            .set(issues::completed_at.eq(Some(1000)))
            .execute(&mut conn)
            .unwrap();

        // Mark execution complete
        diesel::update(executions::table.find("exec-cp"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-cp");

        let completed_at: Option<i32> = issues::table
            .find("issue-cp")
            .select(issues::completed_at)
            .first(&mut conn)
            .unwrap();
        assert_eq!(
            completed_at,
            Some(1000),
            "should preserve existing completed_at"
        );
    }

    #[test]
    fn test_recompute_does_not_set_completed_at_for_active() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRNA");
        insert_issue(&mut conn, "issue-na", &project_id);
        insert_execution(&mut conn, "exec-na", Some("issue-na"));
        // execution is "running" by default

        recompute_issue_status(&mut conn, "issue-na");

        let (status, completed_at): (String, Option<i32>) = issues::table
            .find("issue-na")
            .select((issues::status, issues::completed_at))
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "active");
        assert!(
            completed_at.is_none(),
            "active issues should not have completed_at"
        );
    }

    // === Progress / attention split tests ===

    #[test]
    fn test_attention_precedence_prompt_beats_blocked_job() {
        // When both a prompt and a blocked job exist, NeedsInput should win
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TAPB");
        insert_issue(&mut conn, "issue-prec", &project_id);
        insert_execution(&mut conn, "exec-prec", Some("issue-prec"));
        insert_job_for_exec(&mut conn, "job-prec-1", "exec-prec", "blocked");
        insert_job_for_exec(&mut conn, "job-prec-2", "exec-prec", "running");
        insert_run_for_issue(&mut conn, "run-prec", "issue-prec", "job-prec-2", "idle");
        insert_unanswered_prompt(&mut conn, "prompt-prec", "run-prec");

        recompute_issue_status(&mut conn, "issue-prec");

        let (status, progress, attention) = issue_state(&mut conn, "issue-prec");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "active");
        assert_eq!(
            attention, "needs_input",
            "NeedsInput should take precedence over NeedsApproval"
        );
    }

    #[test]
    fn test_attention_permission_request_beats_blocked_job() {
        // Permission request has higher priority than blocked job
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPRB");
        insert_issue(&mut conn, "issue-perm-prec", &project_id);
        insert_execution(&mut conn, "exec-perm-prec", Some("issue-perm-prec"));
        insert_job_for_exec(&mut conn, "job-pp-1", "exec-perm-prec", "blocked");
        insert_job_for_exec(&mut conn, "job-pp-2", "exec-perm-prec", "running");
        insert_run_for_issue(&mut conn, "run-pp", "issue-perm-prec", "job-pp-2", "idle");
        insert_permission_request(&mut conn, "perm-prec", "run-pp");

        recompute_issue_status(&mut conn, "issue-perm-prec");

        let (status, progress, attention) = issue_state(&mut conn, "issue-perm-prec");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "active");
        assert_eq!(
            attention, "needs_authorization",
            "NeedsAuthorization should take precedence over NeedsApproval"
        );
    }

    #[test]
    fn test_conflicting_pr_produces_needs_conflict_resolution() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TCPR");
        insert_issue(&mut conn, "issue-conflict", &project_id);
        insert_execution(&mut conn, "exec-conflict", Some("issue-conflict"));

        diesel::update(executions::table.find("exec-conflict"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        insert_job_for_exec(&mut conn, "job-conflict", "exec-conflict", "complete");
        insert_merge_request_full(
            &mut conn,
            "mr-conflict",
            "job-conflict",
            "issue-conflict",
            "open",
            None,
            Some("CONFLICTING"),
        );

        recompute_issue_status(&mut conn, "issue-conflict");

        let (status, progress, attention) = issue_state(&mut conn, "issue-conflict");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "complete");
        assert_eq!(attention, "needs_conflict_resolution");
    }

    #[test]
    fn test_pr_needing_review_produces_needs_review() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TNRV");
        insert_issue(&mut conn, "issue-review", &project_id);
        insert_execution(&mut conn, "exec-review", Some("issue-review"));

        diesel::update(executions::table.find("exec-review"))
            .set(executions::status.eq("complete"))
            .execute(&mut conn)
            .unwrap();

        insert_job_for_exec(&mut conn, "job-review", "exec-review", "complete");
        insert_merge_request_full(
            &mut conn,
            "mr-review",
            "job-review",
            "issue-review",
            "open",
            Some("REVIEW_REQUIRED"),
            Some("MERGEABLE"),
        );

        recompute_issue_status(&mut conn, "issue-review");

        let (status, progress, attention) = issue_state(&mut conn, "issue-review");
        assert_eq!(status, "waiting");
        assert_eq!(progress, "complete");
        assert_eq!(attention, "needs_review");
    }

    #[test]
    fn test_failed_progress_state() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TFPS");
        insert_issue(&mut conn, "issue-fail", &project_id);
        insert_execution(&mut conn, "exec-fail", Some("issue-fail"));

        diesel::update(executions::table.find("exec-fail"))
            .set(executions::status.eq("failed"))
            .execute(&mut conn)
            .unwrap();

        recompute_issue_status(&mut conn, "issue-fail");

        let (status, progress, attention) = issue_state(&mut conn, "issue-fail");
        assert_eq!(status, "failed");
        assert_eq!(progress, "failed");
        assert_eq!(attention, "none");
    }

    #[test]
    fn test_backlog_state_columns() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TBSC");
        insert_issue(&mut conn, "issue-bl", &project_id);

        recompute_issue_status(&mut conn, "issue-bl");

        let (status, progress, attention) = issue_state(&mut conn, "issue-bl");
        assert_eq!(status, "backlog");
        assert_eq!(progress, "backlog");
        assert_eq!(attention, "none");
    }

    #[test]
    fn test_resolve_merged_sets_progress_and_attention() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRMP");
        insert_issue(&mut conn, "issue-rmp", &project_id);

        resolve_issue(&mut conn, "issue-rmp", Resolution::Merged, None).unwrap();

        let (status, progress, attention) = issue_state(&mut conn, "issue-rmp");
        assert_eq!(status, "merged");
        assert_eq!(progress, "merged");
        assert_eq!(attention, "none");
    }

    #[test]
    fn test_resolve_closed_sets_progress_and_attention() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TRCA2");
        insert_issue(&mut conn, "issue-rca", &project_id);

        resolve_issue(&mut conn, "issue-rca", Resolution::Closed, None).unwrap();

        let (status, progress, attention) = issue_state(&mut conn, "issue-rca");
        assert_eq!(status, "closed");
        assert_eq!(progress, "closed");
        assert_eq!(attention, "none");
    }
}
