//! Issue CRUD operations.

use crate::error::CairnError;
use crate::issues::relations;
use crate::labels::attach;
use crate::models::{CreateIssue, Issue, IssueAttention, IssueProgress, IssueStatus, UpdateIssue};
use crate::services::Clock;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use crate::transitions::Resolution;
use turso::params;
use uuid::Uuid;

const ISSUE_COLUMNS: &str = "id, project_id, number, title, description, status, progress,
    attention, priority, completed_at, dismissed_at, created_at, updated_at, model,
    merged_at, closed_at, parent_issue_id";

fn db_internal(message: impl Into<String>) -> DbError {
    DbError::internal(message.into())
}

pub async fn list_children(db: &LocalDb, parent_issue_id: &str) -> Result<Vec<Issue>, CairnError> {
    let parent_issue_id = parent_issue_id.to_string();
    db.read(|conn| {
        let parent_issue_id = parent_issue_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {ISSUE_COLUMNS}
                 FROM issues
                 WHERE parent_issue_id = ?1
                 ORDER BY number DESC"
            );
            let mut rows = conn.query(&sql, params![parent_issue_id.as_str()]).await?;
            let mut issues = Vec::new();
            while let Some(row) = rows.next().await? {
                let mut issue = issue_from_row(&row)?;
                hydrate_dependencies(conn, &mut issue).await?;
                issues.push(issue);
            }
            Ok(issues)
        })
    })
    .await
    .map_err(CairnError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delete_db_detaches_memories_before_deleting_jobs() {
        let db = crate::storage::migrated_test_db("issue-delete-memory-detach.db").await;
        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/prj', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'builder', 'builder', 'complete', 1, 1);
            INSERT INTO memories(id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)
             VALUES ('draft-1', 'Draft', 'project-1', 'draft content', 'draft', 'project', 'project-1', 'job-1', 1, 1, 1);
            ",
        )
        .await
        .unwrap();

        delete_db(&db, "issue-1").await.unwrap();

        let count = db
            .query_one(
                "SELECT COUNT(*) FROM memories WHERE id = 'draft-1'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_db_removes_full_run_subtree() {
        // Regression: a worked-on issue has runs/sessions/turns plus a PR and a
        // trigger source. Those reference the subtree without ON DELETE CASCADE,
        // so a naive delete hit "foreign key constraint failed".
        let db = crate::storage::migrated_test_db("issue-delete-full-subtree.db").await;
        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'PRJ', '/tmp/prj', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'builder', 'builder', 'complete', 1, 1);
            INSERT INTO runs(id, project_id, issue_id, job_id, status, backend, created_at, updated_at, start_mode)
             VALUES ('run-1', 'project-1', 'issue-1', 'job-1', 'live', 'codex', 1, 1, 'resume');
            INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('sess-1', 'job-1', 1, 1);
            INSERT INTO turns(id, session_id, run_id, sequence, created_at, updated_at)
             VALUES ('turn-1', 'sess-1', 'run-1', 1, 1, 1);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-1', 'job-1', 'project-1', 'issue-1', 'PR', 'src', 'dst', 'open', 1, 1);
            INSERT INTO execution_trigger_sources(id, source_job_id, triggered_execution_id, created_at)
             VALUES ('ets-1', 'job-1', 'exec-1', 1);
            INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('ev-1', 'run-1', 1, 1, 'message', '{}', 1, 'turn-1');
            INSERT INTO prompts(id, run_id, questions, created_at, turn_id)
             VALUES ('prompt-1', 'run-1', '[]', 1, 'turn-1');
            INSERT INTO permission_requests(id, run_id, tool_use_id, tool_name, tool_input, created_at, turn_id)
             VALUES ('perm-1', 'run-1', 'tu-1', 'run', '{}', 1, 'turn-1');
            INSERT INTO artifacts(id, job_id, artifact_type, data, created_at, updated_at)
             VALUES ('art-1', 'job-1', 'plan', '{}', 1, 1);
            INSERT INTO artifacts(id, job_id, artifact_type, data, parent_version_id, created_at, updated_at)
             VALUES ('art-2', 'job-1', 'plan', '{}', 'art-1', 1, 1);
            UPDATE jobs SET current_turn_id = 'turn-1', resume_session_id = 'sess-1' WHERE id = 'job-1';
            ",
        )
        .await
        .unwrap();

        delete_db(&db, "issue-1").await.unwrap();

        for (table, sql) in [
            ("issues", "SELECT COUNT(*) FROM issues WHERE id = 'issue-1'"),
            (
                "jobs",
                "SELECT COUNT(*) FROM jobs WHERE issue_id = 'issue-1'",
            ),
            (
                "runs",
                "SELECT COUNT(*) FROM runs WHERE issue_id = 'issue-1'",
            ),
            (
                "executions",
                "SELECT COUNT(*) FROM executions WHERE issue_id = 'issue-1'",
            ),
            (
                "sessions",
                "SELECT COUNT(*) FROM sessions WHERE id = 'sess-1'",
            ),
            ("turns", "SELECT COUNT(*) FROM turns WHERE id = 'turn-1'"),
            ("events", "SELECT COUNT(*) FROM events WHERE id = 'ev-1'"),
            (
                "prompts",
                "SELECT COUNT(*) FROM prompts WHERE id = 'prompt-1'",
            ),
            (
                "permission_requests",
                "SELECT COUNT(*) FROM permission_requests WHERE id = 'perm-1'",
            ),
            (
                "artifacts",
                "SELECT COUNT(*) FROM artifacts WHERE job_id = 'job-1'",
            ),
            (
                "merge_requests",
                "SELECT COUNT(*) FROM merge_requests WHERE id = 'mr-1'",
            ),
            (
                "execution_trigger_sources",
                "SELECT COUNT(*) FROM execution_trigger_sources WHERE id = 'ets-1'",
            ),
        ] {
            let count = db.query_one(sql, (), |row| row.i64(0)).await.unwrap();
            assert_eq!(count, 0, "{table} rows should be gone after delete");
        }
    }
}

fn issue_from_row(row: &turso::Row) -> DbResult<Issue> {
    Ok(Issue {
        id: row.text(0)?,
        project_id: row.text(1)?,
        number: row.i64(2)? as i32,
        title: row.text(3)?,
        description: row.opt_text(4)?.unwrap_or_default(),
        status: row.text(5)?.parse().unwrap_or(IssueStatus::Backlog),
        progress: row.text(6)?.parse().unwrap_or(IssueProgress::Backlog),
        attention: row.text(7)?.parse().unwrap_or(IssueAttention::None),
        priority: row.opt_i64(8)?.unwrap_or(0) as i32,
        completed_at: row.opt_i64(9)?,
        dismissed_at: row.opt_i64(10)?,
        created_at: row.i64(11)?,
        updated_at: row.i64(12)?,
        backend_override: row.opt_text(13)?,
        merged_at: row.opt_i64(14)?,
        closed_at: row.opt_i64(15)?,
        parent_issue_id: row.opt_text(16)?,
        unmet_dependency_count: 0,
        depends_on: Vec::new(),
        unmet_depends_on: Vec::new(),
        labels: Vec::new(),
    })
}

async fn hydrate_dependencies(conn: &turso::Connection, issue: &mut Issue) -> DbResult<()> {
    issue.depends_on = relations::list_dependency_uris(conn, &issue.id).await?;
    issue.unmet_depends_on = relations::filter_unmet_dependencies(conn, &issue.depends_on).await?;
    issue.unmet_dependency_count = issue.unmet_depends_on.len() as i64;
    issue.labels = attach::list_labels_for_issue(conn, &issue.id).await?;
    Ok(())
}

async fn load_optional_conn(conn: &turso::Connection, id: &str) -> DbResult<Option<Issue>> {
    let sql = format!("SELECT {ISSUE_COLUMNS} FROM issues WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    let mut issue = rows
        .next()
        .await?
        .map(|row| issue_from_row(&row))
        .transpose()?;
    if let Some(issue) = &mut issue {
        hydrate_dependencies(conn, issue).await?;
    }
    Ok(issue)
}

async fn load_conn(conn: &turso::Connection, id: &str) -> DbResult<Issue> {
    load_optional_conn(conn, id)
        .await?
        .ok_or_else(|| db_internal(format!("issue not found: {id}")))
}

pub async fn create(
    db: &LocalDb,
    clock: &dyn Clock,
    input: CreateIssue,
) -> Result<Issue, CairnError> {
    let CreateIssue {
        project_id,
        title,
        description,
        backend_override,
        label_ids,
    } = input;
    let id = Uuid::new_v4().to_string();
    let now = clock.now();

    db.write(|conn| {
        let id = id.clone();
        let project_id = project_id.clone();
        let title = title.clone();
        let description = description.clone();
        let backend_override = backend_override.clone();
        let label_ids = label_ids.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT next_issue_number FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                DbError::Row(format!("project not found: {}", project_id.as_str()))
            })?;
            let number = row.opt_i64(0)?.unwrap_or(1) as i32;

            conn.execute(
                "UPDATE projects SET next_issue_number = ?1, updated_at = ?2 WHERE id = ?3",
                params![number + 1, now, project_id.as_str()],
            )
            .await?;

            conn.execute(
                "INSERT INTO issues (
                    id, project_id, number, title, description, status, progress, attention,
                    priority, created_at, updated_at, model
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, 'backlog', 'backlog', 'none', 0, ?6, ?6, ?7)",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    number,
                    title.as_str(),
                    description.as_deref(),
                    now,
                    backend_override.as_deref()
                ],
            )
            .await?;

            if let Some(labels) = label_ids {
                attach::replace_issue_labels(conn, &id, &labels, now)
                    .await
                    .map_err(DbError::Row)?;
            }

            let mut issue = Issue {
                id,
                project_id,
                number,
                title,
                description: description.unwrap_or_default(),
                status: IssueStatus::Backlog,
                progress: IssueProgress::Backlog,
                attention: IssueAttention::None,
                priority: 0,
                completed_at: None,
                dismissed_at: None,
                created_at: now,
                updated_at: now,
                backend_override,
                merged_at: None,
                closed_at: None,
                parent_issue_id: None,
                unmet_dependency_count: 0,
                depends_on: Vec::new(),
                unmet_depends_on: Vec::new(),
                labels: Vec::new(),
            };
            hydrate_dependencies(conn, &mut issue).await?;
            Ok(issue)
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(message) if message.starts_with("project not found: ") => {
            CairnError::NotFound {
                entity: "project",
                id: project_id,
            }
        }
        error => CairnError::from(error),
    })
}

pub async fn get(db: &LocalDb, id: &str) -> Result<Option<Issue>, CairnError> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move { load_optional_conn(conn, &id).await })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn list(db: &LocalDb, project_id: &str) -> Result<Vec<Issue>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {ISSUE_COLUMNS}
                 FROM issues
                 WHERE project_id = ?1
                 ORDER BY number DESC"
            );
            let mut rows = conn.query(&sql, params![project_id.as_str()]).await?;
            let mut issues = Vec::new();
            while let Some(row) = rows.next().await? {
                let mut issue = issue_from_row(&row)?;
                hydrate_dependencies(conn, &mut issue).await?;
                issues.push(issue);
            }
            Ok(issues)
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn update(
    db: &LocalDb,
    clock: &dyn Clock,
    input: UpdateIssue,
) -> Result<Issue, CairnError> {
    let UpdateIssue {
        id,
        title,
        description,
        backend_override,
        depends_on,
        label_ids,
    } = input;
    let backend_present = backend_override.is_some();
    let backend_value = backend_override.flatten();
    let now = clock.now();

    db.write(|conn| {
        let id = id.clone();
        let title = title.clone();
        let description = description.clone();
        let backend_value = backend_value.clone();
        let depends_on = depends_on.clone();
        let label_ids = label_ids.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues
                 SET title = COALESCE(?1, title),
                     description = CASE WHEN ?2 IS NULL THEN description ELSE ?2 END,
                     model = CASE WHEN ?3 = 0 THEN model WHEN ?4 IS NULL THEN NULL ELSE ?4 END,
                     updated_at = ?5
                 WHERE id = ?6",
                params![
                    title.as_deref(),
                    description.as_deref(),
                    if backend_present { 1 } else { 0 },
                    backend_value.as_deref(),
                    now,
                    id.as_str()
                ],
            )
            .await?;
            if let Some(dependencies) = depends_on {
                relations::replace_dependencies(conn, &id, &dependencies, now)
                    .await
                    .map_err(DbError::Row)?;
            }
            if let Some(labels) = label_ids {
                attach::replace_issue_labels(conn, &id, &labels, now)
                    .await
                    .map_err(DbError::Row)?;
            }
            load_conn(conn, &id).await
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(message) if message.starts_with("issue not found: ") => CairnError::NotFound {
            entity: "issue",
            id,
        },
        error => CairnError::from(error),
    })
}

pub async fn dismiss(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    set_dismissed(db, clock, id, Some(clock.now())).await
}

pub async fn restore(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    set_dismissed(db, clock, id, None).await
}

async fn set_dismissed(
    db: &LocalDb,
    clock: &dyn Clock,
    id: &str,
    dismissed_at: Option<i64>,
) -> Result<(), CairnError> {
    let id = id.to_string();
    let now = clock.now();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues SET dismissed_at = ?1, updated_at = ?2 WHERE id = ?3",
                params![dismissed_at, now, id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn complete(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    resolve(db, clock, id, Resolution::Merged).await.map(|_| ())
}

pub async fn resolve(
    db: &LocalDb,
    clock: &dyn Clock,
    id: &str,
    resolution: Resolution,
) -> Result<Vec<String>, CairnError> {
    let id = id.to_string();
    let now = clock.now();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let reason = match resolution {
                Resolution::Merged => {
                    conn.execute(
                        "UPDATE issues
                         SET merged_at = ?1, completed_at = ?2, progress = 'merged',
                             attention = 'none', status = 'merged', updated_at = ?3
                         WHERE id = ?4",
                        params![now, now, now, id.as_str()],
                    )
                    .await?;
                    "issue_merged"
                }
                Resolution::Closed => {
                    conn.execute(
                        "UPDATE issues
                         SET closed_at = ?1, completed_at = ?2, progress = 'closed',
                             attention = 'none', status = 'closed', updated_at = ?3
                         WHERE id = ?4",
                        params![now, now, now, id.as_str()],
                    )
                    .await?;
                    "issue_closed"
                }
            };

            close_sessions_for_issue_conn(conn, &id, reason, now).await
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn unresolve(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let now = clock.now();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues
                 SET merged_at = NULL, closed_at = NULL, completed_at = NULL, updated_at = ?1
                 WHERE id = ?2",
                params![now, id.as_str()],
            )
            .await?;
            crate::transitions::outcome::recompute_issue_status_conn(conn, &id).await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}

async fn close_sessions_for_issue_conn(
    conn: &turso::Connection,
    issue_id: &str,
    reason: &str,
    now: i64,
) -> DbResult<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT s.id
             FROM sessions s
             INNER JOIN jobs j ON s.job_id = j.id
             WHERE j.issue_id = ?1
               AND s.status = 'open'
               AND COALESCE(j.memory_review_state, '') != 'sent'",
            params![issue_id],
        )
        .await?;

    let mut session_ids = Vec::new();
    while let Some(row) = rows.next().await? {
        session_ids.push(row.text(0)?);
    }

    for session_id in &session_ids {
        conn.execute(
            "UPDATE sessions
             SET status = 'closed', terminal_reason = ?1, closed_at = ?2, updated_at = ?3
             WHERE id = ?4",
            params![reason, now, now, session_id.as_str()],
        )
        .await?;
    }

    Ok(session_ids)
}

pub async fn delete_db(db: &LocalDb, issue_id: &str) -> Result<(), CairnError> {
    let issue_id = issue_id.to_string();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            // The issue's subtree is interlinked in ways no single delete order
            // satisfies under immediate FK enforcement, and Turso supports
            // neither `PRAGMA foreign_keys = OFF` inside a transaction nor
            // `defer_foreign_keys`. So first null the pointer columns that form
            // cycles or self-references, then delete each non-cascade referrer
            // before the row it points at, and finally the issue (whose
            // ON DELETE CASCADE clears the cascade-linked tables).
            //
            // Pointers nulled: jobs -> their current turn / resume session
            // (current_session_id is not an FK but harmless to null), turns ->
            // predecessor turn, sessions -> replaced_by/parent session, and
            // artifacts -> parent version. Artifacts cascade on job delete, so
            // their self-reference is nulled to let that cascade run freely.
            conn.execute(
                "UPDATE jobs SET current_session_id = NULL, current_turn_id = NULL,
                                 resume_session_id = NULL
                 WHERE issue_id = ?1",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE turns SET predecessor_id = NULL
                 WHERE run_id IN (SELECT id FROM runs WHERE issue_id = ?1)
                    OR job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE sessions SET replaced_by_id = NULL, parent_session_id = NULL
                 WHERE job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE artifacts SET parent_version_id = NULL
                 WHERE job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;

            // events/prompts/permission_requests reference `turns` via turn_id
            // with no cascade, so they must go before the turns they point at.
            // (They cascade on run delete, but turns are deleted first here.)
            conn.execute(
                "DELETE FROM events WHERE run_id IN (SELECT id FROM runs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM prompts WHERE run_id IN (SELECT id FROM runs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM permission_requests
                 WHERE run_id IN (SELECT id FROM runs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;

            // Now the turns themselves, then sessions, then the remaining
            // non-cascade referrers of jobs/executions/issues.
            conn.execute(
                "DELETE FROM turns
                 WHERE run_id IN (SELECT id FROM runs WHERE issue_id = ?1)
                    OR job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM sessions WHERE job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM execution_trigger_sources
                 WHERE source_job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)
                    OR triggered_execution_id IN (SELECT id FROM executions WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM merge_requests
                 WHERE issue_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;

            // memories.job_id has no cascade either. Then runs (now unreferenced),
            // jobs, and finally the issue, whose cascade clears executions and the
            // remaining cascade-linked tables.
            conn.execute(
                "DELETE FROM runs WHERE issue_id = ?1",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM memories
                 WHERE job_id IN (SELECT id FROM jobs WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM jobs WHERE issue_id = ?1",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM issues WHERE id = ?1",
                params![issue_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}
