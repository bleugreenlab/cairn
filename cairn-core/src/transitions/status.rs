//! Execution and issue status projection helpers.

use crate::issues::crud;
use crate::services::EventEmitter;
use crate::services::RealClock;
use crate::storage::LocalDb;
use serde::{Deserialize, Serialize};

/// Execution lifecycle status.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
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
        match s {
            "running" => Ok(ExecutionStatus::Running),
            "complete" => Ok(ExecutionStatus::Complete),
            "failed" => Ok(ExecutionStatus::Failed),
            "paused" => Ok(ExecutionStatus::Running),
            other => Err(format!("unknown execution status: {}", other)),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::CapturingEmitter;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use turso::params;

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
