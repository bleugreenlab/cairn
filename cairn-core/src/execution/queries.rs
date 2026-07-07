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
    ExecutionListResult, ExecutionSnapshot, ExecutionStatus, Job, RecipeNode, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const EXECUTION_COLUMNS: &str = "id, recipe_id, issue_id, project_id, status, started_at,
    completed_at, snapshot, seq, initiator_sub, initiator_org_id,
    triggered_by";

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
/// status per recipe node.
///
/// Restart-node archives a node's prior job as `cancelled` and creates a fresh
/// one, so a node can map to several rows. Excluding cancelled archives and
/// ordering oldest→newest means the newest live attempt wins the per-node
/// collapse (a `HashMap` keeps the last value inserted for a key).
pub(crate) async fn node_statuses_conn(
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
                "SELECT {EXECUTION_COLUMNS}
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
                                "SELECT {EXECUTION_COLUMNS}
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
        .and_then(|s| serde_json::from_str::<ExecutionSnapshot>(s).ok())
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
                    },
                ))
            },
        )
        .await
        .map_err(|e| format!("Failed to query trigger sources: {e}"))
    })
}

async fn get_execution_snapshot_async(
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
        .map(|json| crate::config::snapshot_migrate::load(&json))
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

    let total = db
        .query_one(
            "SELECT COUNT(*)
             FROM executions e
             LEFT JOIN issues i ON i.id = e.issue_id
             LEFT JOIN projects p ON p.id = e.project_id
             WHERE (?1 IS NULL OR e.status = ?1)
               AND (?2 IS NULL OR e.project_id = ?2 OR i.project_id = ?2)
               AND (
                 ?3 IS NULL
                 OR (?3 = 'issue' AND e.issue_id IS NOT NULL)
                 OR (?3 <> 'issue' AND e.issue_id IS NULL)
               )",
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
                   AND (?2 IS NULL OR e.project_id = ?2 OR i.project_id = ?2)
                   AND (
                     ?3 IS NULL
                     OR (?3 = 'issue' AND e.issue_id IS NOT NULL)
                     OR (?3 <> 'issue' AND e.issue_id IS NULL)
                   )
                 ORDER BY e.started_at DESC
                 LIMIT ?4 OFFSET ?5",
                prefixed_execution_columns = prefixed_execution_columns("e")
            ),
            params![
                status.as_deref(),
                project_id.as_deref(),
                triggered_by.as_deref(),
                limit,
                offset
            ],
            |row| {
                // The three joined columns (i.number, i.project_id, p.name) follow
                // the EXECUTION_COLUMNS block, so their indices track its width:
                // 12 execution columns (0..=11) put the joins at 12, 13, 14.
                Ok((
                    db_execution_from_row(row)?,
                    row.opt_i64(12)?.map(|value| value as i32),
                    row.opt_text(13)?,
                    row.opt_text(14)?,
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
    })
}

// ── Job queries ──────────────────────────────────────────────────

pub struct JobTabs {
    pub available_tabs: Vec<String>,
    pub initial_tab: String,
}

pub fn compute_tabs_for_jobs(
    db: Arc<LocalDb>,
    job_ids: &[String],
) -> Result<HashMap<String, JobTabs>, String> {
    let job_ids = job_ids.to_vec();
    run_query_db(async move { compute_tabs_for_jobs_async(&db, &job_ids).await })
}

pub fn list_jobs_for_issue_impl(orch: &Orchestrator, issue_id: &str) -> Result<Vec<Job>, String> {
    let db = run_query_db({
        let dbs = orch.db.clone();
        let issue_id = issue_id.to_string();
        async move {
            crate::issues::crud::owning_db_for_issue(&dbs, &issue_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    run_query_db(list_jobs_for_issue(db, issue_id.to_string()))
}

pub fn get_job_impl(orch: &Orchestrator, job_id: &str) -> Result<Job, String> {
    let db = run_query_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    run_query_db(get_job(db, job_id.to_string()))
}

pub fn list_child_jobs_impl(
    orch: &Orchestrator,
    parent_tool_use_id: &str,
) -> Result<Vec<Job>, String> {
    let db = run_query_db({
        let dbs = orch.db.clone();
        let parent_tool_use_id = parent_tool_use_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_parent_tool_use(&dbs, &parent_tool_use_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    run_query_db(list_jobs_by_column(
        db,
        "parent_tool_use_id",
        parent_tool_use_id.to_string(),
    ))
}

pub fn list_child_jobs_by_parent_impl(
    orch: &Orchestrator,
    parent_job_id: &str,
) -> Result<Vec<Job>, String> {
    let db = run_query_db({
        let dbs = orch.db.clone();
        let parent_job_id = parent_job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &parent_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    run_query_db(list_jobs_by_column(
        db,
        "parent_job_id",
        parent_job_id.to_string(),
    ))
}

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
    .map_err(|_| "Query database task panicked".to_string())?
}

async fn list_jobs_for_issue(db: Arc<LocalDb>, issue_id: String) -> Result<Vec<Job>, String> {
    let db_jobs = load_jobs(&db, "issue_id", &issue_id).await?;
    jobs_from_db(&db, db_jobs).await
}

async fn get_job(db: Arc<LocalDb>, job_id: String) -> Result<Job, String> {
    let db_jobs = load_jobs(&db, "id", &job_id).await?;
    let mut jobs = jobs_from_db(&db, db_jobs).await?;
    jobs.pop().ok_or_else(|| "Job not found".to_string())
}

async fn list_jobs_by_column(
    db: Arc<LocalDb>,
    column: &'static str,
    value: String,
) -> Result<Vec<Job>, String> {
    let db_jobs = load_jobs(&db, column, &value).await?;
    jobs_from_db(&db, db_jobs).await
}

async fn load_jobs(db: &LocalDb, column: &'static str, value: &str) -> Result<Vec<DbJob>, String> {
    let value = value.to_string();
    let sql = match column {
        "id" => format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1"),
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

async fn jobs_from_db(db: &LocalDb, db_jobs: Vec<DbJob>) -> Result<Vec<Job>, String> {
    let mut jobs = Vec::new();
    for db_job in db_jobs {
        let mut job = Job::try_from(db_job)?;
        if let Some(execution_id) = job.execution_id.as_deref() {
            job.exec_seq = execution_seq(db, execution_id).await?;
        }
        let tabs = tabs_for_job(db, &job).await?;
        job.available_tabs = tabs.available_tabs;
        job.initial_tab = tabs.initial_tab;
        jobs.push(job);
    }
    Ok(jobs)
}

async fn compute_tabs_for_jobs_async(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<HashMap<String, JobTabs>, String> {
    if job_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::new();
    for job_id in job_ids {
        let db_jobs = load_jobs(db, "id", job_id).await?;
        let Some(db_job) = db_jobs.into_iter().next() else {
            continue;
        };
        let job = Job::try_from(db_job)?;
        result.insert(job_id.clone(), tabs_for_job(db, &job).await?);
    }
    Ok(result)
}

async fn execution_seq(db: &LocalDb, execution_id: &str) -> Result<Option<i32>, String> {
    let execution_id = execution_id.to_string();
    db.query_opt(
        "SELECT seq FROM executions WHERE id = ?1",
        params![execution_id.as_str()],
        |row| row.opt_i64(0).map(|seq| seq.map(|value| value as i32)),
    )
    .await
    .map(Option::flatten)
    .map_err(|e| e.to_string())
}

async fn tabs_for_job(db: &LocalDb, job: &Job) -> Result<JobTabs, String> {
    let has_artifact = job_has_artifact(db, &job.id).await?;
    let has_downstream_action = match (job.recipe_node_id.as_deref(), job.execution_id.as_deref()) {
        (Some(node_id), Some(execution_id)) => {
            has_single_downstream_action(db, node_id, execution_id).await?
        }
        _ => false,
    };
    // An artifact that feeds a single downstream action is normally surfaced on
    // that action's node, not here. But an *unconfirmed* artifact under a `user`
    // confirm policy is pending human review on this node — it must be visible
    // here so the human can review and confirm it.
    let artifact_unconfirmed = has_artifact && latest_artifact_unconfirmed(db, &job.id).await?;
    let show_artifact = has_artifact && (!has_downstream_action || artifact_unconfirmed);
    let available_tabs = if show_artifact {
        vec!["chat".to_string(), "artifact".to_string()]
    } else {
        vec!["chat".to_string()]
    };
    let initial_tab = if job.status.to_string() == "running" {
        "chat".to_string()
    } else if available_tabs.iter().any(|tab| tab == "artifact") {
        "artifact".to_string()
    } else {
        "chat".to_string()
    };
    Ok(JobTabs {
        available_tabs,
        initial_tab,
    })
}

async fn job_has_artifact(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM artifacts WHERE job_id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// True when the job's latest artifact version is unconfirmed (`confirmed = 0`),
/// i.e. it is pending human confirmation under a `user` confirm policy.
async fn latest_artifact_unconfirmed(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT confirmed FROM artifacts WHERE job_id = ?1 ORDER BY version DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.opt_i64(0)?.unwrap_or(0) == 0,
                None => false,
            })
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn has_single_downstream_action(
    db: &LocalDb,
    node_id: &str,
    execution_id: &str,
) -> Result<bool, String> {
    let Some(snapshot) = get_execution_snapshot_async(db, execution_id).await? else {
        return Ok(false);
    };

    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();
    let target_node_ids: Vec<&str> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.edge_type.to_string() == "context" && e.source_node_id == node_id)
        .map(|e| e.target_node_id.as_str())
        .collect();

    if target_node_ids.len() != 1 {
        return Ok(false);
    }

    Ok(node_map
        .get(target_node_ids[0])
        .map(|n| n.node_type.to_string() == "action")
        .unwrap_or(false))
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
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

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
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
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
             VALUES ('proj-1','default','Cairn','CAIRN','/tmp/repo',1,1)",
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
}
