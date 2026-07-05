//! Database query helpers for jobs and execution snapshots.

use std::collections::{HashMap, HashSet};

use crate::config::slugify_resource_segment;
use crate::db_records::{db_job_from_row, DbJob, JOB_COLUMNS};
use crate::error::CairnError;
use crate::models::{ExecutionSnapshot, Job, RecipeNode};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ImplementationInfo {
    pub job_id: String,
    pub worktree_path: String,
    pub branch: String,
    pub project_id: String,
}

pub async fn get_implementation_for_pr_by_cwd(
    db: &LocalDb,
    cwd: &str,
) -> Result<ImplementationInfo, CairnError> {
    let cwd = cwd.to_string();
    db.read(|conn| {
        let cwd = cwd.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id, j.worktree_path, j.branch, p.id, j.recipe_node_id, e.snapshot
                     FROM runs r
                     JOIN jobs j ON r.job_id = j.id
                     JOIN issues i ON j.issue_id = i.id
                     JOIN projects p ON i.project_id = p.id
                     LEFT JOIN executions e ON j.execution_id = e.id
                     WHERE j.worktree_path = ?1
                       AND r.status IN ('starting', 'live')
                     ORDER BY r.created_at DESC",
                    params![cwd.as_str()],
                )
                .await?;

            while let Some(row) = rows.next().await? {
                let node_id = row.opt_text(4)?;
                let snapshot = row.opt_text(5)?;
                let node_type = match (snapshot.as_deref(), node_id.as_deref()) {
                    (Some(snapshot), Some(node_id)) => node_type_from_snapshot(snapshot, node_id),
                    _ => None,
                };
                if node_type.as_deref() != Some("agent") {
                    continue;
                }

                let worktree_path = row
                    .opt_text(1)?
                    .ok_or_else(|| DbError::internal("Implementation has no worktree path"))?;
                let branch = row
                    .opt_text(2)?
                    .ok_or_else(|| DbError::internal("Implementation has no branch name"))?;

                return Ok(ImplementationInfo {
                    job_id: row.text(0)?,
                    worktree_path,
                    branch,
                    project_id: row.text(3)?,
                });
            }

            Err(DbError::Row(format!("implementation run not found: {cwd}")))
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(_) => CairnError::NotFound {
            entity: "implementation run",
            id: cwd,
        },
        other => other.into(),
    })
}

pub async fn active_agent_job_ids_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> Result<Vec<String>, CairnError> {
    let issue_id = issue_id.to_string();
    db.query_all(
        "SELECT DISTINCT j.id
         FROM jobs j
         JOIN runs r ON r.job_id = j.id
         WHERE j.issue_id = ?1
           AND r.status IN ('starting', 'live')
         ORDER BY j.id",
        params![issue_id.as_str()],
        |row| row.text(0),
    )
    .await
    .map_err(Into::into)
}

pub async fn get_implementation_artifact(
    db: &LocalDb,
    job_id: &str,
) -> Result<(String, Option<String>), CairnError> {
    let job_id = job_id.to_string();
    let artifact_data = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data
                         FROM artifacts
                         WHERE job_id = ?1
                         ORDER BY version DESC
                         LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| row.text(0))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::internal(
                            "No implementation artifact found. Agent must call submit_output first.",
                        )
                    })
            })
        })
        .await?;

    let data: serde_json::Value = serde_json::from_str(&artifact_data)?;
    let title = data
        .get("title")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CairnError::Internal("Artifact missing 'title' field".to_string()))?
        .to_string();
    let body = data
        .get("body")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    Ok((title, body))
}

pub async fn load_execution_snapshot(
    db: &LocalDb,
    execution_id: &str,
) -> Result<ExecutionSnapshot, CairnError> {
    let execution_id = execution_id.to_string();
    let snapshot_json = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT snapshot FROM executions WHERE id = ?1",
                        params![execution_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()?
                    .flatten()
                    .ok_or_else(|| DbError::Row("Execution has no snapshot".to_string()))
            })
        })
        .await
        .map_err(|error| match error {
            DbError::Row(_) => CairnError::NotFound {
                entity: "execution",
                id: execution_id,
            },
            other => other.into(),
        })?;

    Ok(serde_json::from_str(&snapshot_json)?)
}

pub fn find_node_in_snapshot<'a>(
    snapshot: &'a ExecutionSnapshot,
    node_id: &str,
) -> Option<&'a RecipeNode> {
    snapshot.recipe.nodes.iter().find(|node| node.id == node_id)
}

pub async fn load_node_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<(RecipeNode, ExecutionSnapshot)>, CairnError> {
    let job_id = job_id.to_string();
    let row = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.recipe_node_id, e.snapshot
                         FROM jobs j
                         LEFT JOIN executions e ON j.execution_id = e.id
                         WHERE j.id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()
            })
        })
        .await?;

    let Some((Some(node_id), Some(snapshot_json))) = row else {
        return Ok(None);
    };
    let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)?;
    Ok(find_node_in_snapshot(&snapshot, &node_id)
        .cloned()
        .map(|node| (node, snapshot)))
}

pub async fn get_node_type_for_job(
    db: &LocalDb,
    execution_id: Option<&str>,
    node_id: Option<&str>,
) -> Option<String> {
    let (execution_id, node_id) = match (execution_id, node_id) {
        (Some(execution_id), Some(node_id)) => (execution_id, node_id),
        _ => return None,
    };
    let snapshot = load_execution_snapshot(db, execution_id).await.ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|node| node.node_type.to_string())
}

pub async fn get_node_name_from_execution(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
) -> Option<String> {
    let snapshot = load_execution_snapshot(db, execution_id).await.ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|node| node.name.clone())
}

pub async fn get_node_name_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    let data = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, recipe_node_id FROM jobs WHERE id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()
            })
        })
        .await
        .ok()??;

    match data {
        (Some(execution_id), Some(recipe_node_id)) => {
            get_node_name_from_execution(db, &execution_id, &recipe_node_id).await
        }
        _ => None,
    }
}

pub async fn task_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    let (stored_segment, node_name, agent_config_id) = db
        .query_opt(
            "SELECT uri_segment, node_name, agent_config_id FROM jobs WHERE id = ?1",
            params![job_id.as_str()],
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
        )
        .await
        .ok()??;

    if stored_segment
        .as_deref()
        .is_some_and(|segment| !segment.is_empty())
    {
        return stored_segment;
    }

    let from_node = node_name
        .as_deref()
        .map(slugify_resource_segment)
        .filter(|segment| !segment.is_empty());
    if from_node.is_some() {
        return from_node;
    }

    agent_config_id
        .as_deref()
        .map(slugify_resource_segment)
        .filter(|segment| !segment.is_empty())
        .or_else(|| Some("task".to_string()))
}

/// Resolve the canonical URI segment of a job's parent, if any. Returns
/// `None` for top-level node jobs (which have `parent_job_id IS NULL`).
///
/// Pair this with `node_uri_segment_for_job` and `build_job_base_uri` to
/// produce the canonical job URI from any caller that has a `job_id`. This
/// is the parent-segment input the URI builder needs to nest a sub-task as
/// `.../{seq}/{parent}/task/{segment}` instead of misreporting it as a
/// top-level node URI.
pub async fn parent_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.query_opt(
        "SELECT parent.uri_segment
         FROM jobs j
         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
         WHERE j.id = ?1",
        params![job_id.as_str()],
        |row| row.opt_text(0),
    )
    .await
    .ok()
    .flatten()
    .flatten()
}

pub async fn node_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    let (stored_segment, recipe_node_id, node_name, agent_config_id, snapshot) = db
        .query_opt(
            "SELECT j.uri_segment, j.recipe_node_id, j.node_name, j.agent_config_id, e.snapshot
             FROM jobs j
             LEFT JOIN executions e ON j.execution_id = e.id
             WHERE j.id = ?1",
            params![job_id.as_str()],
            |row| {
                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                    row.opt_text(4)?,
                ))
            },
        )
        .await
        .ok()??;

    if stored_segment
        .as_deref()
        .is_some_and(|segment| !segment.is_empty())
    {
        return stored_segment;
    }

    let resolved_name =
        node_name.or_else(|| match (snapshot.as_deref(), recipe_node_id.as_deref()) {
            (Some(snapshot), Some(node_id)) => node_name_from_snapshot(snapshot, node_id),
            _ => None,
        });

    if let Some(name) = resolved_name.as_deref() {
        let segment = slugify_resource_segment(name);
        if !segment.is_empty() {
            return Some(segment);
        }
    }

    if let Some(node_id) = recipe_node_id.as_deref().filter(|value| !value.is_empty()) {
        return Some(node_id.to_string());
    }

    agent_config_id
        .as_deref()
        .map(slugify_resource_segment)
        .filter(|segment| !segment.is_empty())
}

pub async fn resolve_task_artifact_uri(
    db: &LocalDb,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    task_job_id: &str,
) -> Option<String> {
    let task_job_id = task_job_id.to_string();
    let parent_job_id = db
        .query_opt(
            "SELECT parent_job_id FROM jobs WHERE id = ?1",
            params![task_job_id.as_str()],
            |row| row.opt_text(0),
        )
        .await
        .ok()???;

    let parent_segment = node_uri_segment_for_job(db, &parent_job_id).await?;
    let task_segment = task_uri_segment_for_job(db, &task_job_id).await?;

    Some(cairn_common::uri::build_task_artifact_uri(
        project_key,
        issue_number,
        exec_seq,
        &parent_segment,
        &task_segment,
    ))
}

#[derive(Serialize)]
pub struct JobTabs {
    pub available_tabs: Vec<String>,
    pub initial_tab: String,
}

pub async fn compute_tabs_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<HashMap<String, JobTabs>, CairnError> {
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            let mut result = HashMap::new();

            for job_id in job_ids {
                let mut job_rows = conn
                    .query(
                        "SELECT j.status, j.recipe_node_id, e.snapshot
                         FROM jobs j
                         LEFT JOIN executions e ON j.execution_id = e.id
                         WHERE j.id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let Some(row) = job_rows.next().await? else {
                    continue;
                };
                let status = row.text(0)?;
                let recipe_node_id = row.opt_text(1)?;
                let snapshot = row.opt_text(2)?;
                drop(job_rows);

                let mut artifact_rows = conn
                    .query(
                        "SELECT 1 FROM artifacts WHERE job_id = ?1 LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let has_artifact = artifact_rows.next().await?.is_some();
                drop(artifact_rows);

                let has_downstream_action = match (recipe_node_id.as_deref(), snapshot.as_deref()) {
                    (Some(node_id), Some(snapshot)) => {
                        has_single_downstream_action(snapshot, node_id).unwrap_or(false)
                    }
                    _ => false,
                };

                let available_tabs = if has_downstream_action || !has_artifact {
                    vec!["chat".to_string()]
                } else {
                    vec!["chat".to_string(), "artifact".to_string()]
                };
                let initial_tab = if status == "running" {
                    "chat".to_string()
                } else if available_tabs.iter().any(|tab| tab == "artifact") {
                    "artifact".to_string()
                } else {
                    "chat".to_string()
                };

                result.insert(
                    job_id,
                    JobTabs {
                        available_tabs,
                        initial_tab,
                    },
                );
            }

            Ok(result)
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn get_job(db: &LocalDb, job_id: &str) -> Result<Job, CairnError> {
    let job_id = job_id.to_string();
    let db_job = load_job_by_predicate(db, "id = ?1", &job_id).await?;
    let mut job = Job::try_from(db_job).map_err(CairnError::Internal)?;
    apply_tabs_and_exec_seq(db, &mut job).await?;
    Ok(job)
}

pub async fn list_jobs_for_issue(db: &LocalDb, issue_id: &str) -> Result<Vec<Job>, CairnError> {
    list_jobs_by_predicate(db, "issue_id = ?1", issue_id, "created_at ASC").await
}

pub async fn list_child_jobs(
    db: &LocalDb,
    parent_tool_use_id: &str,
) -> Result<Vec<Job>, CairnError> {
    list_jobs_by_predicate(
        db,
        "parent_tool_use_id = ?1",
        parent_tool_use_id,
        "task_index ASC",
    )
    .await
}

pub async fn list_child_jobs_by_parent(
    db: &LocalDb,
    parent_job_id: &str,
) -> Result<Vec<Job>, CairnError> {
    list_jobs_by_predicate(
        db,
        "parent_job_id = ?1",
        parent_job_id,
        "task_index ASC, created_at ASC",
    )
    .await
}

pub async fn list_jobs_for_execution(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Vec<Job>, CairnError> {
    list_jobs_by_predicate(db, "execution_id = ?1", execution_id, "created_at ASC").await
}

async fn list_jobs_by_predicate(
    db: &LocalDb,
    predicate: &str,
    value: &str,
    order_by: &str,
) -> Result<Vec<Job>, CairnError> {
    let value = value.to_string();
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE {predicate} ORDER BY {order_by}");
    let db_jobs = db
        .read(|conn| {
            let value = value.clone();
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(sql.as_str(), params![value.as_str()]).await?;
                let mut jobs = Vec::new();
                while let Some(row) = rows.next().await? {
                    jobs.push(db_job_from_row(&row)?);
                }
                Ok(jobs)
            })
        })
        .await?;

    let ids: Vec<String> = db_jobs.iter().map(|job| job.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(db, &ids).await?;
    let exec_seqs = exec_seqs_for_jobs(db, &db_jobs).await?;

    db_jobs
        .into_iter()
        .map(|db_job| {
            let id = db_job.id.clone();
            let execution_id = db_job.execution_id.clone();
            let mut job = Job::try_from(db_job).map_err(CairnError::Internal)?;
            if let Some(job_tabs) = tabs.get(&id) {
                job.available_tabs = job_tabs.available_tabs.clone();
                job.initial_tab = job_tabs.initial_tab.clone();
            }
            if let Some(execution_id) = execution_id {
                job.exec_seq = exec_seqs.get(&execution_id).copied();
            }
            Ok(job)
        })
        .collect()
}

async fn load_job_by_predicate(
    db: &LocalDb,
    predicate: &str,
    value: &str,
) -> Result<DbJob, CairnError> {
    let value = value.to_string();
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE {predicate} LIMIT 1");
    db.query_one(sql, params![value.as_str()], db_job_from_row)
        .await
        .map_err(|error| match error {
            DbError::Row(_) => CairnError::NotFound {
                entity: "job",
                id: value,
            },
            other => other.into(),
        })
}

async fn apply_tabs_and_exec_seq(db: &LocalDb, job: &mut Job) -> Result<(), CairnError> {
    let tabs = compute_tabs_for_jobs(db, std::slice::from_ref(&job.id)).await?;
    if let Some(job_tabs) = tabs.get(&job.id) {
        job.available_tabs = job_tabs.available_tabs.clone();
        job.initial_tab = job_tabs.initial_tab.clone();
    }
    if let Some(execution_id) = job.execution_id.as_deref() {
        job.exec_seq = exec_seq_for_execution(db, execution_id).await?;
    }
    Ok(())
}

async fn exec_seq_for_execution(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<i32>, CairnError> {
    let execution_id = execution_id.to_string();
    db.query_opt(
        "SELECT seq FROM executions WHERE id = ?1",
        params![execution_id.as_str()],
        |row| row.opt_i64(0).map(|seq| seq.map(|value| value as i32)),
    )
    .await
    .map(Option::flatten)
    .map_err(CairnError::from)
}

async fn exec_seqs_for_jobs(
    db: &LocalDb,
    jobs: &[DbJob],
) -> Result<HashMap<String, i32>, CairnError> {
    let execution_ids: HashSet<String> = jobs
        .iter()
        .filter_map(|job| job.execution_id.clone())
        .collect();
    let mut result = HashMap::new();
    for execution_id in execution_ids {
        if let Some(seq) = exec_seq_for_execution(db, &execution_id).await? {
            result.insert(execution_id, seq);
        }
    }
    Ok(result)
}

// ============================================================================
// Node activity indicators (status-colored node-tab icons / sidebar dots)
// ============================================================================

/// The live activity of a node's agent, independent of `JobStatus`. This is the
/// signal the node-tab bot icon (and reusable status surfaces like sidebar dots)
/// color from: a turn actively running, a turn yielded waiting on the user, or
/// nothing in flight. It is deliberately NOT derived from `job.status` — a
/// `blocked` job means a completed turn awaiting confirm (an artifact gate), not
/// a turn awaiting user input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeActivity {
    /// Node alive but no turn in flight (no head turn, or a terminal head turn).
    Idle,
    /// A turn is actively running (head turn `pending` or `running`).
    Running,
    /// A turn has yielded for the user: a pending `ask_user` prompt or a pending
    /// permission request is outstanding.
    AwaitingInput,
}

/// One job's live activity, keyed by job id so the whole strip (top-level nodes
/// plus task jobs) is computed in a single batched query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatusIndicator {
    pub job_id: String,
    pub activity: NodeActivity,
}

/// Pure mapping from the three raw facts to a `NodeActivity`. Awaiting-input is
/// checked first and does NOT require the head turn to be `pending`/`running`:
/// an `ask_user`/permission wait transitions the head turn to `yielded` (see
/// `transitions::turn::yield_turn` and the MCP handlers), so a pending
/// prompt/permission row is the authoritative signal regardless of turn state.
/// This mirrors exactly what the per-node chat surface uses
/// (`get_pending_prompt_for_job` / `get_pending_permission_for_job`).
pub fn derive_node_activity(
    head_turn_state: Option<&str>,
    has_pending_prompt: bool,
    has_pending_permission: bool,
) -> NodeActivity {
    if has_pending_prompt || has_pending_permission {
        return NodeActivity::AwaitingInput;
    }
    match head_turn_state {
        Some("pending") | Some("running") => NodeActivity::Running,
        _ => NodeActivity::Idle,
    }
}

/// Batched live-activity indicators for every job in an execution (top-level
/// nodes AND task jobs), computed in one query. Reusable for any status surface
/// that needs the running/awaiting-input/idle distinction without a per-node
/// fan-out of `get_head_turn_for_job` + `get_pending_prompt_for_job` +
/// `get_pending_permission_for_job`.
pub async fn node_status_indicators(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Vec<NodeStatusIndicator>, CairnError> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT
                        j.id,
                        (SELECT t.state
                           FROM turns t
                          WHERE t.job_id = j.id
                          ORDER BY t.sequence DESC
                          LIMIT 1) AS head_turn_state,
                        EXISTS (
                            SELECT 1 FROM prompts p
                             WHERE p.turn_id = j.current_turn_id
                               AND p.response IS NULL
                        ) AS has_pending_prompt,
                        EXISTS (
                            SELECT 1 FROM permission_requests pr
                             LEFT JOIN runs r ON pr.run_id = r.id
                             WHERE COALESCE(pr.job_id, r.job_id) = j.id
                               AND pr.status = 'pending'
                        ) AS has_pending_permission
                     FROM jobs j
                     WHERE j.execution_id = ?1",
                    params![execution_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                let head_turn_state = row.opt_text(1)?;
                let has_pending_prompt = row.i64(2)? != 0;
                let has_pending_permission = row.i64(3)? != 0;
                out.push(NodeStatusIndicator {
                    job_id,
                    activity: derive_node_activity(
                        head_turn_state.as_deref(),
                        has_pending_prompt,
                        has_pending_permission,
                    ),
                });
            }
            Ok::<Vec<NodeStatusIndicator>, DbError>(out)
        })
    })
    .await
    .map_err(CairnError::from)
}

fn node_type_from_snapshot(snapshot_json: &str, node_id: &str) -> Option<String> {
    let snapshot: ExecutionSnapshot = serde_json::from_str(snapshot_json).ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|node| node.node_type.to_string())
}

fn node_name_from_snapshot(snapshot_json: &str, node_id: &str) -> Option<String> {
    let snapshot: ExecutionSnapshot = serde_json::from_str(snapshot_json).ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|node| node.name.clone())
}

fn has_single_downstream_action(snapshot_json: &str, node_id: &str) -> Result<bool, CairnError> {
    let snapshot: ExecutionSnapshot = serde_json::from_str(snapshot_json)?;
    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let target_node_ids: Vec<&str> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|edge| edge.edge_type.to_string() == "context" && edge.source_node_id == node_id)
        .map(|edge| edge.target_node_id.as_str())
        .collect();

    if target_node_ids.len() != 1 {
        return Ok(false);
    }

    Ok(node_map
        .get(target_node_ids[0])
        .map(|node| node.node_type.to_string() == "action")
        .unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::{derive_node_activity, node_status_indicators, NodeActivity};
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use std::collections::HashMap;

    #[test]
    fn idle_when_no_turn_and_no_waits() {
        assert_eq!(derive_node_activity(None, false, false), NodeActivity::Idle);
    }

    #[test]
    fn idle_when_head_turn_terminal() {
        for state in ["complete", "failed", "yielded", "interrupted", "cancelled"] {
            assert_eq!(
                derive_node_activity(Some(state), false, false),
                NodeActivity::Idle,
                "terminal/yielded head turn with no waits is idle: {state}"
            );
        }
    }

    #[test]
    fn running_when_head_turn_live() {
        assert_eq!(
            derive_node_activity(Some("pending"), false, false),
            NodeActivity::Running
        );
        assert_eq!(
            derive_node_activity(Some("running"), false, false),
            NodeActivity::Running
        );
    }

    #[test]
    fn awaiting_input_on_pending_prompt_regardless_of_turn_state() {
        // A prompt wait yields the head turn, so awaiting-input must win even when
        // the head turn reads `yielded` (the durable-wait case) — not just while
        // it is still `running` (the inline-wait window).
        assert_eq!(
            derive_node_activity(Some("yielded"), true, false),
            NodeActivity::AwaitingInput
        );
        assert_eq!(
            derive_node_activity(Some("running"), true, false),
            NodeActivity::AwaitingInput
        );
    }

    #[test]
    fn awaiting_input_on_pending_permission() {
        assert_eq!(
            derive_node_activity(Some("yielded"), false, true),
            NodeActivity::AwaitingInput
        );
    }

    #[test]
    fn awaiting_input_outranks_running() {
        assert_eq!(
            derive_node_activity(Some("running"), false, true),
            NodeActivity::AwaitingInput
        );
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

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        std::mem::forget(temp.path().to_path_buf());
        let db = LocalDb::open(temp.path().join("node-status.db"))
            .await
            .unwrap();
        std::mem::forget(temp);
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn batched_indicators_classify_every_job_in_the_execution() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'T', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i', 'p', 1, 'T', 'active', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e', 'r', 'i', 'p', 'running', 1, 1)",
        )
        .await;

        // Head turns first (jobs.current_turn_id references turns(id)).
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-run', 's-run', 'j-run', 1, 'running', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-idle', 's-idle', 'j-idle', 1, 'complete', 'initial', 1, 1)",
        )
        .await;
        // A prompt/permission wait yields the head turn — the durable-wait case.
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
             VALUES ('t-prompt', 's-prompt', 'j-prompt', 1, 'yielded', 'user_input', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
             VALUES ('t-perm', 's-perm', 'j-perm', 1, 'yielded', 'permission', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-task', 's-task', 'task-1', 1, 'running', 'initial', 1, 1)",
        )
        .await;

        // Jobs. j-none has no turn at all; task-1 is a child job (still in the strip).
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-run', 'e', 'i', 'p', 'Run', 'running', 1, 1, 'run')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-idle', 'e', 'i', 'p', 'Idle', 'complete', 1, 1, 'idle')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, current_turn_id)
             VALUES ('j-prompt', 'e', 'i', 'p', 'Prompt', 'running', 1, 1, 'prompt', 't-prompt')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-perm', 'e', 'i', 'p', 'Perm', 'running', 1, 1, 'perm')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-none', 'e', 'i', 'p', 'None', 'pending', 1, 1, 'none')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('task-1', 'e', 'j-run', 'i', 'p', 'Task', 'running', 1, 1, 'task')",
        )
        .await;

        // Runs back the prompt (run_id NOT NULL) and the permission (COALESCE
        // falls back to r.job_id when pr.job_id is NULL).
        exec(
            &db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-prompt', 'j-prompt', 'i', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-perm', 'j-perm', 'i', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO prompts(id, run_id, turn_id, questions, response, created_at)
             VALUES ('pr-1', 'run-prompt', 't-prompt', '[]', NULL, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO permission_requests(id, run_id, job_id, turn_id, tool_use_id, tool_name, tool_input, status, created_at)
             VALUES ('perm-1', 'run-perm', NULL, 't-perm', 'tu1', 'bash', '{}', 'pending', 1)",
        )
        .await;

        let indicators = node_status_indicators(&db, "e").await.unwrap();
        let by_job: HashMap<String, NodeActivity> = indicators
            .into_iter()
            .map(|indicator| (indicator.job_id, indicator.activity))
            .collect();

        assert_eq!(by_job.len(), 6, "every job in the execution is reported");
        assert_eq!(by_job["j-run"], NodeActivity::Running);
        assert_eq!(by_job["j-idle"], NodeActivity::Idle);
        assert_eq!(by_job["j-prompt"], NodeActivity::AwaitingInput);
        assert_eq!(by_job["j-perm"], NodeActivity::AwaitingInput);
        assert_eq!(by_job["j-none"], NodeActivity::Idle);
        assert_eq!(by_job["task-1"], NodeActivity::Running);
    }

    #[tokio::test]
    async fn empty_execution_yields_no_indicators() {
        let db = test_db().await;
        let indicators = node_status_indicators(&db, "missing").await.unwrap();
        assert!(indicators.is_empty());
    }
}
