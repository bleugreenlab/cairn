use std::sync::Arc;

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

/// Resolve a run's context and the database that owns it (CAIRN-2132).
///
/// A run id is a globally-unique UUID and a project lives wholly in one
/// database, so a run appears in at most one open database. This probes the
/// private database first and short-circuits on a hit — the overwhelming
/// majority of installs have no team databases and most runs are local — and
/// only fans out across open team replicas on a private-DB miss. With no team
/// DBs open it degenerates to a single private-DB lookup: a strict no-op for
/// local-only installs. Returns the owning `Arc<LocalDb>` so every downstream
/// write for the request targets the right replica instead of defaulting to the
/// private database (which would silently misroute a shared-project run).
pub(crate) async fn lookup_run_routed(
    dbs: &crate::db::DbState,
    request: &CallbackRequest,
) -> Result<(RunContext, Arc<LocalDb>), String> {
    // A run id is self-routing: parse its team prefix to the owning database in
    // O(1) (CAIRN-2210) instead of scanning every open database. Only the
    // cwd-keyed fallback (no run id to parse) still fans out.
    if let Some(run_id) = request.run_id.as_deref() {
        let db = crate::execution::routing::routing_db_for_id(dbs, run_id)
            .await
            .map_err(|e| e.to_string())?;
        let ctx = lookup_run(&db, request).await?;
        return Ok((ctx, db));
    }
    match lookup_run(&dbs.local, request).await {
        Ok(ctx) => Ok((ctx, dbs.local.clone())),
        Err(private_err) => {
            for db in dbs.all_dbs().await {
                if Arc::ptr_eq(&db, &dbs.local) {
                    continue;
                }
                if let Ok(ctx) = lookup_run(&db, request).await {
                    return Ok((ctx, db));
                }
            }
            Err(private_err)
        }
    }
}

/// The home base URI for the request's run, searching every open database the
/// same way [`lookup_run_routed`] does (CAIRN-2132). Resolves `cairn:~/...`
/// targets for a run whose rows live in a team replica.
pub(crate) async fn lookup_home_uri_routed(
    dbs: &crate::db::DbState,
    request: &CallbackRequest,
) -> Result<String, String> {
    // Self-routing run id → O(1) prefix parse (CAIRN-2210); cwd-keyed lookup
    // (no run id) still fans out across open databases.
    if let Some(run_id) = request.run_id.as_deref() {
        let db = crate::execution::routing::routing_db_for_id(dbs, run_id)
            .await
            .map_err(|e| e.to_string())?;
        return lookup_home_uri(&db, request).await;
    }
    match lookup_home_uri(&dbs.local, request).await {
        Ok(uri) => Ok(uri),
        Err(private_err) => {
            for db in dbs.all_dbs().await {
                if Arc::ptr_eq(&db, &dbs.local) {
                    continue;
                }
                if let Ok(uri) = lookup_home_uri(&db, request).await {
                    return Ok(uri);
                }
            }
            Err(private_err)
        }
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
                           e.seq, j.worktree_path
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
                           e.seq, j.worktree_path
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

fn run_context_from_row(row: &cairn_db::turso::Row) -> DbResult<RunContext> {
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
        worktree_path: row.opt_text(10)?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    async fn local_dbs() -> std::sync::Arc<DbState> {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("p.turso.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let search = SearchIndex::open_or_create(dir.join("p.search")).unwrap();
        std::sync::Arc::new(DbState::new(
            std::sync::Arc::new(db),
            std::sync::Arc::new(search),
        ))
    }

    async fn seed_run(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path) VALUES ('j','e','i','p','Builder','running',1,1,'builder','/tmp/wt')",
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at) VALUES ('r','i','p','j','live',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    fn request_for_run(run_id: &str) -> CallbackRequest {
        CallbackRequest {
            run_id: Some(run_id.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn lookup_run_routed_resolves_a_private_run_to_the_private_db() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;
        let (ctx, db) = lookup_run_routed(&dbs, &request_for_run("r"))
            .await
            .expect("a seeded private run resolves");
        assert_eq!(ctx.project_key, "PRJ");
        assert_eq!(ctx.run_id, "r");
        assert!(
            Arc::ptr_eq(&db, &dbs.local),
            "a private run resolves to the private database (strict no-op for local installs)"
        );
    }

    #[tokio::test]
    async fn lookup_run_routed_errors_clearly_when_run_is_absent_everywhere() {
        let dbs = local_dbs().await;
        let err = match lookup_run_routed(&dbs, &request_for_run("ghost")).await {
            Ok(_) => panic!("an unknown run must error, never silently misroute"),
            Err(err) => err,
        };
        assert!(
            err.contains("ghost"),
            "the error should name the missing run id: {err}"
        );
    }
}
