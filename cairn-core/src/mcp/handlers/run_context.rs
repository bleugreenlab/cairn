use cairn_common::protocol::CallbackRequest;

use super::RunContext;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

pub(crate) async fn lookup_run(
    db: &LocalDb,
    request: &CallbackRequest,
) -> Result<RunContext, String> {
    if let Some(run_id) = request.run_id.as_deref() {
        lookup_run_by_id(db, run_id).await
    } else {
        lookup_run_by_cwd(db, &request.cwd).await
    }
}

pub(crate) async fn lookup_home_uri(
    db: &LocalDb,
    request: &CallbackRequest,
) -> Result<String, String> {
    if let Some(run_id) = request.run_id.as_deref() {
        lookup_home_uri_by_run_id(db, run_id).await
    } else {
        let run = lookup_run_by_cwd(db, &request.cwd).await?;
        lookup_home_uri_by_run_id(db, &run.run_id).await
    }
}

async fn lookup_home_uri_by_run_id(db: &LocalDb, run_id: &str) -> Result<String, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT projects.key,
                            issues.number,
                            executions.seq,
                            jobs.uri_segment,
                            parent_jobs.uri_segment AS parent_uri_segment
                     FROM runs
                     LEFT JOIN issues ON runs.issue_id = issues.id
                     LEFT JOIN projects ON COALESCE(runs.project_id, issues.project_id) = projects.id
                     LEFT JOIN jobs ON runs.job_id = jobs.id
                     LEFT JOIN jobs AS parent_jobs ON jobs.parent_job_id = parent_jobs.id
                     LEFT JOIN executions ON jobs.execution_id = executions.id
                     WHERE runs.id = ?1
                     LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;

            let Some(row) = rows.next().await? else {
                return Err(DbError::Row(format!("No run found with id '{}'", run_id)));
            };
            let project_key = row.opt_text(0)?;
            let issue_number = row.opt_i64(1)?.map(|value| value as i32);
            let exec_seq = row.opt_i64(2)?.map(|value| value as i32);
            let uri_segment = row.opt_text(3)?;
            let parent_uri_segment = row.opt_text(4)?;
            match (
                project_key.as_deref(),
                issue_number,
                exec_seq,
                uri_segment.as_deref(),
            ) {
                (Some(key), Some(number), Some(seq), Some(segment)) => Ok(
                    cairn_common::uri::build_job_base_uri(
                        key,
                        number,
                        seq,
                        segment,
                        parent_uri_segment.as_deref(),
                    ),
                ),
                _ => Err(DbError::Row(format!(
                    "Cannot build home URI for run {}: project_key={:?}, issue_number={:?}, exec_seq={:?}, uri_segment={:?}",
                    run_id, project_key, issue_number, exec_seq, uri_segment
                ))),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn lookup_run_by_id(db: &LocalDb, run_id: &str) -> Result<RunContext, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT r.id, r.job_id, j.execution_id, j.recipe_node_id,
                           r.issue_id, i.number, j.project_id, p.key, j.node_name,
                           e.seq
                    FROM runs r
                    JOIN jobs j ON r.job_id = j.id
                    LEFT JOIN issues i ON r.issue_id = i.id
                    JOIN projects p ON j.project_id = p.id
                    LEFT JOIN executions e ON j.execution_id = e.id
                    WHERE r.id = ?1
                    LIMIT 1
                    ",
                    (run_id.as_str(),),
                )
                .await?;

            rows.next()
                .await?
                .map(|row| run_context_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("No run found with id '{}'", run_id)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn lookup_run_by_cwd(db: &LocalDb, cwd: &str) -> Result<RunContext, String> {
    let cwd = cwd.to_string();
    db.read(|conn| {
        let cwd = cwd.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT r.id, r.job_id, j.execution_id, j.recipe_node_id,
                           r.issue_id, i.number, j.project_id, p.key, j.node_name,
                           e.seq
                    FROM runs r
                    JOIN jobs j ON r.job_id = j.id
                    LEFT JOIN issues i ON r.issue_id = i.id
                    JOIN projects p ON j.project_id = p.id
                    LEFT JOIN executions e ON j.execution_id = e.id
                    WHERE r.status IN ('starting', 'live')
                      AND (
                        j.worktree_path = ?1
                        OR (p.repo_path = ?1 AND j.issue_id IS NULL)
                      )
                    ORDER BY
                        CASE WHEN j.worktree_path = ?1 THEN 0 ELSE 1 END,
                        r.created_at DESC
                    LIMIT 1
                    ",
                    (cwd.as_str(),),
                )
                .await?;

            rows.next()
                .await?
                .map(|row| run_context_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("No active run found for path '{}'", cwd)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

fn run_context_from_row(row: &turso::Row) -> DbResult<RunContext> {
    let issue_number = row.opt_i64(5)?.map(|value| value as i32);
    let exec_seq = row.opt_i64(9)?.map(|value| value as i32);
    let job_name = row.opt_text(8)?;

    Ok(RunContext {
        run_id: row.text(0)?,
        job_id: row.opt_text(1)?.unwrap_or_default(),
        exec_seq,
        issue_id: row.opt_text(4)?,
        issue_number,
        project_id: row.text(6)?,
        project_key: row.text(7)?,
        job_name,
    })
}

/// Resolve a project's DB id by key (uppercased).
pub(crate) async fn project_id_by_key(db: &LocalDb, key: &str) -> Result<String, String> {
    let key = key.to_uppercase();
    db.query_text(
        "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
        (key.clone(),),
    )
    .await
    .map_err(|e| format!("Failed to load project: {e}"))?
    .ok_or_else(|| format!("No project found with key '{key}'"))
}

pub(crate) async fn project_path(db: &LocalDb, project_id: &str) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    db.query_text(
        "SELECT repo_path FROM projects WHERE id = ?1",
        (project_id,),
    )
    .await
    .map_err(|e| format!("Failed to load project path: {}", e))
}
