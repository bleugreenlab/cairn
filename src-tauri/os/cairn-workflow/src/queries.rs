//! Execution and job query functions.
//!
//! Canonical home for read-only execution queries (snapshots, node statuses,
//! listing, detail) and job query helpers.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use cairn_db::turso::params;

use crate::db_records::{db_job_from_row, DbActionRun, DbExecution, DbJob, JOB_COLUMNS};
use crate::models::{
    ActionRun, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionSnapshot, ExecutionStatus, Job, TriggerType,
};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const EXECUTION_COLUMNS: &str = "id, recipe_id, issue_id, project_id, status, started_at,
    completed_at, snapshot, seq, initiator_sub, initiator_org_id,
    triggered_by, runner_device_id, lifecycle, released_at, release_override_json";

/// [`EXECUTION_COLUMNS`] with the snapshot read as NULL, for the queries that
/// only build [`Execution`] values, which never carry it.
///
/// A snapshot runs to hundreds of kilobytes; selecting it on a list the DTO
/// then drops was most of a nine-second issue route under load (cairn/5206).
/// Positions match [`EXECUTION_COLUMNS`] exactly, so [`db_execution_from_row`]
/// and [`EXECUTION_COLUMN_COUNT`] serve both;
/// `execution_list_columns_match_execution_columns` holds that.
const EXECUTION_LIST_COLUMNS: &str = "id, recipe_id, issue_id, project_id, status, started_at,
    completed_at, NULL AS snapshot, seq, initiator_sub, initiator_org_id,
    triggered_by, runner_device_id, lifecycle, released_at, release_override_json";

/// How many columns [`EXECUTION_COLUMNS`] selects.
///
/// A query that appends joined columns after that block reads them at
/// `EXECUTION_COLUMN_COUNT + n`, so adding a column to the block moves them
/// automatically. `execution_column_count_matches_the_column_list` holds this
/// honest — the coupling used to be a comment, and a comment does not fail.
const EXECUTION_COLUMN_COUNT: usize = 16;

const ACTION_RUN_COLUMNS: &str = "id, execution_id, recipe_node_id, action_config_id,
    issue_id, project_id, status, inputs, output, error_message, started_at, completed_at,
    created_at, parent_job_id, uri_segment";

// ── Execution queries ────────────────────────────────────────────

pub fn get_execution_snapshot(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Option<ExecutionSnapshot>, String> {
    let execution_id = execution_id.to_string();
    run_query_db(async move { get_execution_snapshot_async(&db, &execution_id).await })
}

pub fn get_execution_node_statuses(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<HashMap<String, String>, String> {
    let execution_id = execution_id.to_string();
    run_query_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move { node_statuses_conn(conn, &execution_id).await })
        })
        .await
        .map_err(|e| format!("Failed to load jobs: {e}"))
    })
}

/// Connection-level node→status projection: the newest live (non-cancelled) job
/// status per recipe node, plus each evaluated condition node.
///
/// Restart-node archives a node's prior job as `cancelled` and creates a fresh
/// one, so a node can map to several rows. Excluding cancelled archives and
/// ordering oldest→newest means the newest live attempt wins the per-node
/// collapse (a `HashMap` keeps the last value inserted for a key).
///
/// A condition node has no job, so its status comes from its latest evaluation:
/// `blocked` when it routed nowhere (the same status a blocked checkpoint
/// shows), `complete` once it routed.
pub async fn node_statuses_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<HashMap<String, String>> {
    let mut rows = conn
        .query(
            "SELECT recipe_node_id, status FROM jobs
             WHERE execution_id = ?1 AND status <> 'cancelled'
             ORDER BY created_at ASC",
            params![execution_id],
        )
        .await?;
    let mut map = HashMap::new();
    while let Some(row) = rows.next().await? {
        if let Some(node_id) = row.opt_text(0)? {
            map.insert(node_id, row.text(1)?);
        }
    }
    drop(rows);

    let mut rows = conn
        .query(
            "SELECT recipe_node_id, outcome FROM condition_evaluations
             WHERE execution_id = ?1
             ORDER BY evaluated_at ASC, rowid ASC",
            params![execution_id],
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let status = match row.text(1)?.parse::<crate::models::ConditionOutcome>() {
            Ok(crate::models::ConditionOutcome::Blocked) => "blocked",
            _ => "complete",
        };
        map.insert(row.text(0)?, status.to_string());
    }
    Ok(map)
}

/// List every attempt (job row) for a recipe node within an execution, oldest
/// to newest, **including** archived (`cancelled`) attempts. Powers the
/// attempt-lineage view: a restarted node keeps its prior attempts reachable and
/// comparable rather than dropping them from history.
pub fn get_execution_for_issue(
    db: Arc<LocalDb>,
    issue_id: &str,
) -> Result<Option<Execution>, String> {
    let issue_id = issue_id.to_string();
    run_query_db(async move {
        db.query_opt(
            format!(
                "SELECT {EXECUTION_LIST_COLUMNS}
                 FROM executions
                 WHERE issue_id = ?1
                 LIMIT 1"
            ),
            params![issue_id.as_str()],
            db_execution_from_row,
        )
        .await
        .map_err(|e| e.to_string())?
        .map(Execution::try_from)
        .transpose()
    })
}

pub fn list_executions_for_issue(
    db: Arc<LocalDb>,
    issue_id: &str,
) -> Result<Vec<Execution>, String> {
    let issue_id = issue_id.to_string();
    run_query_db(async move {
        let db_execs = db
            .read(|conn| {
                let issue_id = issue_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT {EXECUTION_LIST_COLUMNS}
                                 FROM executions
                                 WHERE issue_id = ?1
                                 ORDER BY started_at DESC"
                            ),
                            params![issue_id.as_str()],
                        )
                        .await?;
                    let mut executions = Vec::new();
                    while let Some(row) = rows.next().await? {
                        executions.push(db_execution_from_row(&row)?);
                    }
                    Ok(executions)
                })
            })
            .await
            .map_err(|e| e.to_string())?;

        db_execs
            .into_iter()
            .map(Execution::try_from)
            .collect::<Result<Vec<_>, _>>()
    })
}

fn extract_recipe_name_from_snapshot(snapshot_json: Option<&str>) -> String {
    snapshot_json
        .and_then(|s| crate::snapshot_migrate::load(s).ok())
        .map(|snap| snap.recipe.name)
        .unwrap_or_else(|| "Unknown Recipe".to_string())
}

pub fn list_all_executions(
    db: Arc<LocalDb>,
    filters: &ExecutionFilters,
    limit: i64,
    offset: i64,
) -> Result<ExecutionListResult, String> {
    let filters = filters.clone();
    run_query_db(async move { list_all_executions_async(&db, &filters, limit, offset).await })
}

pub fn get_execution_detail(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<ExecutionDetail, String> {
    let execution_id = execution_id.to_string();
    run_query_db(async move { get_execution_detail_async(&db, &execution_id).await })
}

pub fn list_triggered_executions_for_execution(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Vec<(String, crate::models::TriggeredExecution)>, String> {
    let execution_id = execution_id.to_string();
    run_query_db(async move {
        db.query_all(
            "SELECT ets.source_job_id, e.id, e.status, e.started_at,
                    e.triggered_by, e.snapshot, e.completed_at
             FROM execution_trigger_sources ets
             INNER JOIN jobs j ON j.id = ets.source_job_id
             INNER JOIN executions e ON e.id = ets.triggered_execution_id
             WHERE j.execution_id = ?1",
            params![execution_id.as_str()],
            |row| {
                let status: ExecutionStatus = row.text(2)?.parse().unwrap_or_default();
                let triggered_by: TriggerType = row.text(4)?.parse().unwrap_or_default();
                let snapshot_json = row.opt_text(5)?;
                Ok((
                    row.text(0)?,
                    crate::models::TriggeredExecution {
                        id: row.text(1)?,
                        recipe_name: extract_recipe_name_from_snapshot(snapshot_json.as_deref()),
                        status,
                        triggered_by,
                        started_at: row.i64(3)?,
                        completed_at: row.opt_i64(6)?,
                        gate_census: None,
                    },
                ))
            },
        )
        .await
        .map_err(|e| format!("Failed to query trigger sources: {e}"))
    })
}

pub async fn get_execution_snapshot_async(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<ExecutionSnapshot>, String> {
    let execution_id = execution_id.to_string();
    let snapshot_json = db
        .query_opt_text(
            "SELECT snapshot FROM executions WHERE id = ?1",
            params![execution_id.as_str()],
        )
        .await
        .map_err(|e| format!("Failed to load execution: {e}"))?;

    snapshot_json
        .map(|json| crate::snapshot_migrate::load(&json))
        .transpose()
}

async fn list_all_executions_async(
    db: &LocalDb,
    filters: &ExecutionFilters,
    limit: i64,
    offset: i64,
) -> Result<ExecutionListResult, String> {
    let status = filters.status.clone();
    let project_id = filters.project_id.clone();
    let triggered_by = filters.triggered_by.clone();

    // The `held` filter means WAITING: the projection also derives `held` for a
    // gated execution that can never start (superseded, or its issue resolved),
    // so the shared predicate narrows it in both the count and the page.
    let total = db
        .query_one(
            concat!(
                "SELECT COUNT(*)
             FROM executions e
             LEFT JOIN issues i ON i.id = e.issue_id
             LEFT JOIN projects p ON p.id = e.project_id
             WHERE (?1 IS NULL OR e.status = ?1)
               AND (?1 IS NULL OR ?1 <> 'held' OR (",
                cairn_db::awaiting_release_sql!(),
                "))
               AND (?2 IS NULL OR e.project_id = ?2 OR i.project_id = ?2)
               AND (
                 ?3 IS NULL
                 OR (?3 = 'issue' AND e.issue_id IS NOT NULL)
                 OR (?3 <> 'issue' AND e.issue_id IS NULL)
               )"
            ),
            params![
                status.as_deref(),
                project_id.as_deref(),
                triggered_by.as_deref()
            ],
            |row| row.i64(0),
        )
        .await
        .map_err(|e| e.to_string())?;

    let rows = db
        .query_all(
            format!(
                "SELECT {prefixed_execution_columns}, i.number, i.project_id, p.name
                 FROM executions e
                 LEFT JOIN issues i ON i.id = e.issue_id
                 LEFT JOIN projects p ON p.id = e.project_id
                 WHERE (?1 IS NULL OR e.status = ?1)
                   AND (?1 IS NULL OR ?1 <> 'held' OR ({awaiting_release}))
                   AND (?2 IS NULL OR e.project_id = ?2 OR i.project_id = ?2)
                   AND (
                     ?3 IS NULL
                     OR (?3 = 'issue' AND e.issue_id IS NOT NULL)
                     OR (?3 <> 'issue' AND e.issue_id IS NULL)
                   )
                 ORDER BY e.started_at DESC
                 LIMIT ?4 OFFSET ?5",
                prefixed_execution_columns = prefixed_execution_columns("e"),
                awaiting_release = cairn_db::awaiting_release_sql!()
            ),
            params![
                status.as_deref(),
                project_id.as_deref(),
                triggered_by.as_deref(),
                limit,
                offset
            ],
            |row| {
                // i.number, i.project_id and p.name follow the EXECUTION_COLUMNS
                // block, so their indices track its width rather than being
                // spelled out.
                Ok((
                    db_execution_from_row(row)?,
                    row.opt_i64(EXECUTION_COLUMN_COUNT)?
                        .map(|value| value as i32),
                    row.opt_text(EXECUTION_COLUMN_COUNT + 1)?,
                    row.opt_text(EXECUTION_COLUMN_COUNT + 2)?,
                ))
            },
        )
        .await
        .map_err(|e| e.to_string())?;

    let mut items = Vec::new();
    for (db_exec, issue_number, issue_project_id, project_name) in rows {
        let (jobs_total, jobs_complete) = execution_job_counts(db, &db_exec.id).await?;
        let status: ExecutionStatus = db_exec.status.parse().unwrap_or_default();
        let triggered_by: TriggerType = db_exec.triggered_by.parse().unwrap_or_default();
        let project_id = db_exec.project_id.clone().or(issue_project_id);
        let recipe_name = extract_recipe_name_from_snapshot(db_exec.snapshot.as_deref());

        items.push(ExecutionListItem {
            id: db_exec.id,
            recipe_name,
            issue_id: db_exec.issue_id,
            issue_number,
            project_id,
            project_name,
            triggered_by,
            status,
            started_at: db_exec.started_at as i64,
            completed_at: db_exec.completed_at.map(|t| t as i64),
            jobs_total,
            jobs_complete,
            seq: db_exec.seq,
            runner_device_id: db_exec.runner_device_id,
            lifecycle: db_exec.lifecycle.parse().unwrap_or_default(),
            gate_census: None,
        });
    }

    Ok(ExecutionListResult {
        items,
        total,
        has_more: (offset + limit) < total,
    })
}

async fn get_execution_detail_async(
    db: &LocalDb,
    execution_id: &str,
) -> Result<ExecutionDetail, String> {
    let execution_id = execution_id.to_string();
    let db_exec = db
        .query_one(
            format!(
                "SELECT {EXECUTION_COLUMNS}
                 FROM executions
                 WHERE id = ?1"
            ),
            params![execution_id.as_str()],
            db_execution_from_row,
        )
        .await
        .map_err(|e| format!("Execution not found: {e}"))?;

    let recipe_name = extract_recipe_name_from_snapshot(db_exec.snapshot.as_deref());
    let execution: Execution = db_exec.try_into()?;

    let (issue_number, issue_title) = if let Some(issue_id) = execution.issue_id.clone() {
        issue_summary(db, &issue_id).await?
    } else {
        (None, None)
    };

    let (project_id, project_name) = if let Some(project_id) = execution.project_id.clone() {
        project_summary(db, &project_id).await?
    } else if let Some(issue_id) = execution.issue_id.clone() {
        issue_project_summary(db, &issue_id).await?
    } else {
        (None, None)
    };

    let jobs = load_jobs(db, "execution_id", &execution_id)
        .await?
        .into_iter()
        .map(Job::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let action_runs = load_action_runs_for_execution(db, &execution_id).await?;
    let gates = {
        let execution_id = execution_id.clone();
        db.read(move |conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move { crate::gates::store::list_gates_conn(conn, &execution_id).await })
        })
        .await
        .map_err(|e| format!("Failed to load execution gates: {e}"))?
    };
    let issue_id = execution.issue_id.clone();

    Ok(ExecutionDetail {
        execution,
        recipe_name,
        issue_id,
        issue_number,
        issue_title,
        project_id,
        project_name,
        jobs,
        action_runs,
        gates,
    })
}

// ── Job queries ──────────────────────────────────────────────────

fn run_query_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to start query database runtime: {}", e))?
            .block_on(future)
    })
    .join()
    .map_err(|payload| {
        format!(
            "Query database task panicked: {}",
            crate::storage::panic_message(&*payload)
        )
    })?
}

async fn load_jobs(db: &LocalDb, column: &'static str, value: &str) -> Result<Vec<DbJob>, String> {
    let value = value.to_string();
    let sql = match column {
        "issue_id" => {
            format!("SELECT {JOB_COLUMNS} FROM jobs WHERE issue_id = ?1 ORDER BY created_at ASC")
        }
        "execution_id" => format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE execution_id = ?1 ORDER BY created_at ASC"
        ),
        "parent_tool_use_id" => format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE parent_tool_use_id = ?1 ORDER BY task_index ASC"
        ),
        "parent_job_id" => format!(
            "SELECT {JOB_COLUMNS}
             FROM jobs
             WHERE parent_job_id = ?1
             ORDER BY task_index ASC, created_at ASC"
        ),
        _ => return Err("unsupported job query".to_string()),
    };
    db.query_all(sql, params![value.as_str()], db_job_from_row)
        .await
        .map_err(|e| e.to_string())
}

async fn execution_job_counts(db: &LocalDb, execution_id: &str) -> Result<(i64, i64), String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*), SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END)
                     FROM jobs
                     WHERE execution_id = ?1",
                    params![execution_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.i64(0)?, row.opt_i64(1)?.unwrap_or(0))))
                .transpose()
                .map(|value| value.unwrap_or((0, 0)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn issue_summary(
    db: &LocalDb,
    issue_id: &str,
) -> Result<(Option<i32>, Option<String>), String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT number, title FROM issues WHERE id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((Some(row.i64(0)? as i32), Some(row.text(1)?))))
                .transpose()
                .map(|value| value.unwrap_or((None, None)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn project_summary(
    db: &LocalDb,
    project_id: &str,
) -> Result<(Option<String>, Option<String>), String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, name FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((Some(row.text(0)?), Some(row.text(1)?))))
                .transpose()
                .map(|value| value.unwrap_or((None, None)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn issue_project_summary(
    db: &LocalDb,
    issue_id: &str,
) -> Result<(Option<String>, Option<String>), String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.id, p.name
                     FROM issues i
                     INNER JOIN projects p ON p.id = i.project_id
                     WHERE i.id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((Some(row.text(0)?), Some(row.text(1)?))))
                .transpose()
                .map(|value| value.unwrap_or((None, None)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_action_runs_for_execution(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Vec<ActionRun>, String> {
    let execution_id = execution_id.to_string();
    let db_runs = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT {ACTION_RUN_COLUMNS}
                             FROM action_runs
                             WHERE execution_id = ?1
                             ORDER BY created_at ASC"
                        ),
                        params![execution_id.as_str()],
                    )
                    .await?;
                let mut runs = Vec::new();
                while let Some(row) = rows.next().await? {
                    runs.push(db_action_run_from_row(&row)?);
                }
                Ok(runs)
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    db_runs
        .into_iter()
        .map(ActionRun::try_from)
        .collect::<Result<Vec<_>, _>>()
}

fn prefixed_execution_columns(prefix: &str) -> String {
    EXECUTION_COLUMNS
        .split(',')
        .map(|column| format!("{prefix}.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn db_execution_from_row(row: &cairn_db::turso::Row) -> DbResult<DbExecution> {
    Ok(DbExecution {
        id: row.text(0)?,
        recipe_id: row.text(1)?,
        issue_id: row.opt_text(2)?,
        project_id: row.opt_text(3)?,
        status: row.text(4)?,
        started_at: row.i64(5)? as i32,
        completed_at: row.opt_i64(6)?.map(|value| value as i32),
        snapshot: row.opt_text(7)?,
        seq: row.opt_i64(8)?.map(|value| value as i32),
        initiator_sub: row.opt_text(9)?,
        initiator_org_id: row.opt_text(10)?,
        triggered_by: row.text(11)?,
        runner_device_id: row.opt_text(12)?,
        lifecycle: row.text(13)?,
        released_at: row.opt_i64(14)?,
        release_override_json: row.opt_text(15)?,
    })
}

fn db_action_run_from_row(row: &cairn_db::turso::Row) -> Result<DbActionRun, DbError> {
    Ok(DbActionRun {
        id: row.text(0)?,
        execution_id: row.text(1)?,
        recipe_node_id: row.text(2)?,
        action_config_id: row.text(3)?,
        issue_id: row.opt_text(4)?,
        project_id: row.text(5)?,
        status: row.text(6)?,
        inputs: row.opt_text(7)?,
        output: row.opt_text(8)?,
        error_message: row.opt_text(9)?,
        started_at: row.opt_i64(10)?.map(|value| value as i32),
        completed_at: row.opt_i64(11)?.map(|value| value as i32),
        created_at: row.i64(12)? as i32,
        parent_job_id: row.opt_text(13)?,
        uri_segment: row.opt_text(14)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Queries that append joined columns after the EXECUTION_COLUMNS block read
    /// them at offsets derived from this count. When the block grows and the
    /// count does not, those reads silently return the wrong column — which is
    /// exactly what a hardcoded offset did when `released_at` was added.
    #[test]
    fn execution_column_count_matches_the_column_list() {
        assert_eq!(
            EXECUTION_COLUMN_COUNT,
            EXECUTION_COLUMNS.split(',').count(),
            "EXECUTION_COLUMN_COUNT must equal the number of columns EXECUTION_COLUMNS selects"
        );
    }

    /// The list columns are the full columns with only the snapshot nulled, in
    /// the same positions, so one row mapper reads both.
    #[test]
    fn execution_list_columns_match_execution_columns() {
        let full: Vec<&str> = EXECUTION_COLUMNS.split(',').map(str::trim).collect();
        let list: Vec<&str> = EXECUTION_LIST_COLUMNS.split(',').map(str::trim).collect();
        assert_eq!(full.len(), list.len());
        for (full, list) in full.iter().zip(&list) {
            match *full {
                "snapshot" => assert_eq!(*list, "NULL AS snapshot"),
                _ => assert_eq!(full, list),
            }
        }
    }

    /// An issue's executions list without their snapshot and still map every
    /// other column.
    #[tokio::test]
    async fn issue_executions_list_without_reading_the_snapshot() {
        let db = migrated_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1','default','Cairn','cairn','/tmp/repo',1,1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1','proj-1',42,'T','active',1,1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, snapshot, started_at, seq, released_at)
             VALUES ('exec-1','r','issue-1','proj-1','running','{\"not\":\"read\"}',1,3,5)",
        )
        .await;
        let db = Arc::new(db);
        let listed = list_executions_for_issue(db.clone(), "issue-1").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].seq, Some(3));
        assert_eq!(listed[0].released_at, Some(5));
        let one = get_execution_for_issue(db, "issue-1").unwrap().unwrap();
        assert_eq!(one.id, "exec-1");
    }

    async fn exec(db: &LocalDb, sql: &'static str) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn migrated_db() -> LocalDb {
        let db = LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        db
    }

    /// Regression for the positional mapper in `list_all_executions_async`: the
    /// three joined columns (`i.number`, `i.project_id`, `p.name`) are read by
    /// index immediately after the `EXECUTION_COLUMNS` block, so their indices
    /// must track its width. Removing `initiator_auth_mode` shrank the block
    /// from 13 to 12 columns, shifting the joins to 12/13/14. If they drift,
    /// `issue_number` reads `i.project_id` (a TEXT) and `project_name` reads
    /// past the last selected column.
    #[tokio::test]
    async fn list_all_executions_maps_joined_issue_and_project_columns() {
        let db = migrated_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1','default','Cairn','cairn','/tmp/repo',1,1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1','proj-1',42,'T','active',1,1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1','r','issue-1','proj-1','running',1,1)",
        )
        .await;

        let result = list_all_executions_async(&db, &ExecutionFilters::default(), 10, 0)
            .await
            .unwrap();

        assert_eq!(result.items.len(), 1);
        let item = &result.items[0];
        assert_eq!(item.id, "exec-1");
        assert_eq!(
            item.issue_number,
            Some(42),
            "issue_number must map i.number, not the adjacent i.project_id"
        );
        assert_eq!(item.project_id.as_deref(), Some("proj-1"));
        assert_eq!(
            item.project_name.as_deref(),
            Some("Cairn"),
            "project_name must map p.name"
        );
    }

    /// The `held` filter selects and counts only executions still waiting to
    /// start. A superseded gated row and one on a merged issue both project
    /// `held`, but neither will ever start, so neither is in the filter.
    #[tokio::test]
    async fn the_held_filter_counts_and_pages_only_waiting_executions() {
        let db = migrated_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1','default','Cairn','cairn','/tmp/repo',1,1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at, merged_at)
             VALUES ('open','proj-1',1,'Open','backlog',1,1,NULL),
                    ('done','proj-1',2,'Done','merged',1,1,5)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
             VALUES ('waiting','r','open','proj-1','held',3,2,'pending'),
                    ('fenced','r','open','proj-1','held',2,1,'superseded'),
                    ('resolved','r','done','proj-1','held',1,1,'pending')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO execution_gates(id, execution_id, seq, predicate_json, created_at)
             VALUES ('g1','waiting',0,'{}',1), ('g2','fenced',0,'{}',1), ('g3','resolved',0,'{}',1)",
        )
        .await;

        let held = ExecutionFilters {
            status: Some("held".into()),
            ..ExecutionFilters::default()
        };
        let result = list_all_executions_async(&db, &held, 10, 0).await.unwrap();
        let ids: Vec<&str> = result.items.iter().map(|item| item.id.as_str()).collect();
        assert_eq!(ids, vec!["waiting"]);
        assert_eq!(result.total, 1, "the count agrees with the page");

        let all = list_all_executions_async(&db, &ExecutionFilters::default(), 10, 0)
            .await
            .unwrap();
        assert_eq!(all.total, 3, "won't-start rows are still listed under All");
    }
}
