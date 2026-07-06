//! Execution and issue status projection helpers.

use crate::issues::crud;
use crate::models::JobStatus;
use crate::services::EventEmitter;
use crate::services::RealClock;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;

/// Execution lifecycle status.
///
/// The canonical derivation logic is `recompute_execution_status` below; the
/// enum itself now lives in `models::execution`, re-exported here so existing
/// `transitions::ExecutionStatus` callers keep resolving.
pub use crate::models::ExecutionStatus;

/// Resolution type for issue merge/close.
#[derive(Debug, Clone, Copy)]
pub enum Resolution {
    Merged,
    Closed,
}

pub async fn recompute_execution_status(db: &LocalDb, execution_id: &str) -> ExecutionStatus {
    let execution_id = execution_id.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            super::outcome::recompute_execution_status_conn(conn, &execution_id).await?;
            let status = crate::storage::query_text_conn(
                conn,
                "SELECT status FROM executions WHERE id = ?1",
                (execution_id.as_str(),),
            )
            .await?
            .unwrap_or_else(|| "running".to_string());
            Ok(status.parse().unwrap_or_default())
        })
    })
    .await
    .unwrap_or_default()
}

pub async fn recompute_issue_status(db: &LocalDb, issue_id: &str) {
    let issue_id = issue_id.to_string();
    let _ = db
        .write(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                super::outcome::recompute_issue_status_conn(conn, &issue_id).await?;
                Ok(())
            })
        })
        .await;
}

pub async fn resolve_issue(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    issue_id: &str,
    resolution: Resolution,
) -> Result<(), String> {
    crud::resolve(db, &RealClock, issue_id, resolution)
        .await
        .map_err(|e| e.to_string())?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "sessions", "action": "update"}),
    );
    Ok(())
}

pub async fn unresolve_issue(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    issue_id: &str,
) -> Result<(), String> {
    crud::unresolve(db, &RealClock, issue_id)
        .await
        .map_err(|e| e.to_string())?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );
    Ok(())
}

/// Transition a job between non-archival readiness states and emit the scoped
/// invalidations needed by issue and execution job-list panes.
pub async fn transition_job_readiness(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    job_id: &str,
    target: JobStatus,
) -> Result<(), String> {
    if !matches!(target, JobStatus::Pending | JobStatus::Running) {
        return Err(format!(
            "Job readiness transitions only support pending/running targets, got {target}"
        ));
    }

    let job_id = job_id.to_string();
    let (execution_id, issue_id) = db
        .write(|conn| {
            let job_id = job_id.clone();
            let target = target.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, execution_id, issue_id FROM jobs WHERE id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal(format!("job not found: {job_id}")))?;
                let current_str = row.text(0)?;
                let execution_id = row.opt_text(1)?;
                let issue_id = row.opt_text(2)?;
                drop(rows);

                let current: JobStatus = current_str.parse().map_err(DbError::Row)?;
                if current == target {
                    return Ok((execution_id, issue_id));
                }
                if !valid_job_transition(&current, &target) {
                    return Err(DbError::internal(format!(
                        "Invalid job transition for {job_id}: {current} -> {target} (transition not allowed)"
                    )));
                }

                let now = chrono::Utc::now().timestamp();
                if target == JobStatus::Running && current == JobStatus::Pending {
                    conn.execute(
                        "UPDATE jobs SET status = ?1, started_at = ?2, updated_at = ?3 WHERE id = ?4",
                        params![target.to_string(), now, now, job_id.as_str()],
                    )
                    .await?;
                } else {
                    conn.execute(
                        "UPDATE jobs SET status = ?1, updated_at = ?2 WHERE id = ?3",
                        params![target.to_string(), now, job_id.as_str()],
                    )
                    .await?;
                }

                let effective_issue_id = if let Some(execution_id) = execution_id.as_deref() {
                    if target == JobStatus::Running {
                        conn.execute(
                            "UPDATE executions SET status = 'running' WHERE id = ?1",
                            params![execution_id],
                        )
                        .await?;
                    }
                    issue_id
                        .clone()
                        .or(issue_id_for_execution_conn(conn, execution_id).await?)
                } else {
                    issue_id.clone()
                };
                if let Some(issue_id) = effective_issue_id.as_deref() {
                    super::outcome::recompute_issue_status_conn(conn, issue_id).await?;
                }

                Ok((execution_id, effective_issue_id))
            })
        })
        .await
        .map_err(|e: DbError| e.to_string())?;

    let job = crate::jobs::queries::get_job(db, &job_id)
        .await
        .map_err(|e| e.to_string())?;
    let _ = emitter.emit("db-change", crate::notify::job_db_change(&job, "update"));
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "executions",
            "action": "update",
            "issueId": issue_id.as_deref(),
            "executionId": execution_id.as_deref(),
        }),
    );
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "issues",
            "action": "update",
            "issueId": issue_id.as_deref(),
        }),
    );
    Ok(())
}

async fn issue_id_for_execution_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> crate::storage::DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT issue_id FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    crate::storage::next_opt_text(&mut rows, 0).await
}

fn valid_job_transition(from: &JobStatus, to: &JobStatus) -> bool {
    matches!(
        (from, to),
        (JobStatus::Pending, JobStatus::Running)
            | (JobStatus::Running, JobStatus::Pending)
            | (JobStatus::Blocked, JobStatus::Pending)
            | (JobStatus::Blocked, JobStatus::Running)
            | (JobStatus::Complete, JobStatus::Running)
            | (JobStatus::Complete, JobStatus::Pending)
            | (JobStatus::Failed, JobStatus::Running)
            | (JobStatus::Failed, JobStatus::Pending)
            | (JobStatus::Idle, JobStatus::Running)
            | (JobStatus::Idle, JobStatus::Pending)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::JobStatus;
    use crate::services::testing::CapturingEmitter;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use cairn_db::turso::params;

    async fn migrated_db() -> LocalDb {
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("resolve.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn insert_open_issue_with_session(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/proj', 1, 1);
            INSERT INTO issues (
                id, project_id, number, title, status, progress, attention,
                created_at, updated_at
             ) VALUES ('issue-1', 'project-1', 1, 'Issue', 'active', 'active',
                'required', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe-1', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs (
                id, execution_id, recipe_node_id, issue_id, project_id, status,
                node_name, uri_segment, created_at, updated_at
             ) VALUES ('job-1', 'exec-1', 'node-1', 'issue-1', 'project-1',
                'running', 'Node', 'node', 1, 1);
            INSERT INTO sessions (id, job_id, status, created_at, updated_at)
             VALUES ('session-1', 'job-1', 'open', 1, 1);
            ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn transition_job_readiness_sets_started_at_and_emits_scoped_changes() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/proj', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 'active', 'active', 'none', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe-1', 'issue-1', 'project-1', 'pending', 1, 1);
            INSERT INTO jobs (
                id, execution_id, recipe_node_id, issue_id, project_id, status,
                node_name, uri_segment, created_at, updated_at
             ) VALUES ('parent-job', 'exec-1', 'parent-node', 'issue-1', 'project-1',
                'running', 'Parent', 'parent', 1, 1);
            INSERT INTO jobs (
                id, execution_id, recipe_node_id, issue_id, project_id, status,
                parent_job_id, parent_tool_use_id, node_name, uri_segment,
                created_at, updated_at
             ) VALUES ('job-1', 'exec-1', 'node-1', 'issue-1', 'project-1',
                'pending', 'parent-job', 'tool-1', 'Node', 'node', 1, 1);
            ",
        )
        .await
        .unwrap();
        let emitter = CapturingEmitter::new();

        transition_job_readiness(&db, &emitter, "job-1", JobStatus::Running)
            .await
            .unwrap();

        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, started_at FROM jobs WHERE id = ?1",
                        params!["job-1"],
                    )
                    .await?;
                let row = rows.next().await?.expect("job row");
                assert_eq!(row.text(0)?, "running");
                assert!(row.opt_i64(1)?.is_some());
                drop(rows);

                let mut rows = conn
                    .query(
                        "SELECT status FROM executions WHERE id = ?1",
                        params!["exec-1"],
                    )
                    .await?;
                let row = rows.next().await?.expect("execution row");
                assert_eq!(row.text(0)?, "running");
                Ok(())
            })
        })
        .await
        .unwrap();

        let db_changes = emitter.events_named("db-change");
        let job_payload = db_changes
            .iter()
            .find(|payload| payload["table"] == "jobs")
            .expect("jobs db-change");
        assert_eq!(job_payload["action"], "update");
        assert_eq!(job_payload["jobId"], "job-1");
        assert_eq!(job_payload["issueId"], "issue-1");
        assert_eq!(job_payload["executionId"], "exec-1");
        assert_eq!(job_payload["parentJobId"], "parent-job");
        assert_eq!(job_payload["parentToolUseId"], "tool-1");
        assert_eq!(job_payload["projectId"], "project-1");
        assert!(db_changes
            .iter()
            .any(|payload| payload["table"] == "executions"
                && payload["executionId"] == "exec-1"
                && payload["issueId"] == "issue-1"));
        assert!(db_changes
            .iter()
            .any(|payload| payload["table"] == "issues" && payload["issueId"] == "issue-1"));
    }

    #[tokio::test]
    async fn transition_job_readiness_rejects_terminal_targets() {
        let db = migrated_db().await;
        let emitter = CapturingEmitter::new();

        let error = transition_job_readiness(&db, &emitter, "job-1", JobStatus::Complete)
            .await
            .expect_err("terminal target should be rejected before any DB write");

        assert!(error.contains("only support pending/running"));
        assert!(emitter.events_named("db-change").is_empty());
    }

    #[tokio::test]
    async fn resolve_issue_sets_completion_attention_and_emits_changes() {
        let db = migrated_db().await;
        insert_open_issue_with_session(&db).await;
        let emitter = CapturingEmitter::new();

        resolve_issue(&db, &emitter, "issue-1", Resolution::Merged)
            .await
            .unwrap();

        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, progress, attention, completed_at, merged_at
                         FROM issues WHERE id = ?1",
                        params!["issue-1"],
                    )
                    .await?;
                let row = rows.next().await?.expect("resolved issue row");
                assert_eq!(row.text(0)?, "merged");
                assert_eq!(row.text(1)?, "merged");
                assert_eq!(row.text(2)?, "none");
                assert!(row.opt_i64(3)?.is_some());
                assert!(row.opt_i64(4)?.is_some());

                let mut rows = conn
                    .query(
                        "SELECT status, terminal_reason FROM sessions WHERE id = ?1",
                        params!["session-1"],
                    )
                    .await?;
                let row = rows.next().await?.expect("closed session row");
                assert_eq!(row.text(0)?, "closed");
                assert_eq!(row.opt_text(1)?.as_deref(), Some("issue_merged"));
                Ok(())
            })
        })
        .await
        .unwrap();

        let db_changes = emitter.events_named("db-change");
        assert!(db_changes
            .iter()
            .any(|payload| payload["table"] == "issues"));
        assert!(db_changes
            .iter()
            .any(|payload| payload["table"] == "sessions"));
    }
}
