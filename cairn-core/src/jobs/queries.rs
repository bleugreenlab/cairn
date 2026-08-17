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
    pub branch: String,
    pub project_id: String,
}

pub(crate) async fn active_agent_job_ids_for_issue(
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

fn find_node_in_snapshot<'a>(
    snapshot: &'a ExecutionSnapshot,
    node_id: &str,
) -> Option<&'a RecipeNode> {
    snapshot.recipe.nodes.iter().find(|node| node.id == node_id)
}

pub async fn get_node_name_from_execution(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
) -> Option<String> {
    let snapshot = load_execution_snapshot(db, execution_id).await.ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|node| node.name.clone())
}

/// The newest run belonging to a job, which is the identity its worktree fence is
/// resolved through.
///
/// A surface a *user* opens on a job — a terminal from the `+` menu, a REPL from
/// the same menu — arrives with no run context, but it is still that job's agent
/// the process is opened for and still that agent's fence that should govern it.
/// Answering "no run identity" there means no policy is built and the process
/// spawns unconfined, so the identity is read from the job instead of from the
/// caller. Owner-agnostic: it keys on the job alone, so a thread session resolves
/// exactly as an issue node does.
pub(crate) async fn latest_run_id_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    cairn_db::turso::params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

/// Resolve the job a node coordinate addresses — the inverse of
/// [`home_uri_for_job_conn`], and owner-aware in exactly the same way.
///
/// `task_name: None` names the node (or thread session) itself; `Some` names a
/// sub-agent task beneath it. `(0, 0, name)` is the reserved thread coordinate
/// that [`cairn_common::uri::NodeAddress`] defines; every other coordinate names
/// an execution node under an issue.
///
/// This is the ONE place the two ownerships are told apart when resolving, so a
/// thread's terminals, browsers, tasks, and collections resolve through the same
/// call an issue node's do. Four near-identical issue-shaped copies of this
/// query previously lived beside the surfaces that needed them, which is why
/// terminals and browsers could not be addressed from a thread at all: their
/// resolvers had no arm for an owner without an issue.
///
/// Read-only by construction. Resolving a thread that has never run returns
/// `Ok(None)` rather than minting a session; only a write may do that.
pub(crate) async fn job_id_for_node_coordinate_conn(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> Result<Option<String>, DbError> {
    if let cairn_common::uri::NodeAddress::Thread { name } =
        cairn_common::uri::NodeAddress::new(number, exec_seq, node_id)
    {
        return match task_name {
            None => crate::threads::session_job_id_by_name_conn(conn, project_key, name).await,
            Some(task) => {
                crate::threads::task_job_id_by_name_conn(conn, project_key, name, task).await
            }
        };
    }

    let key = cairn_common::uri::canonical_project(project_key);
    let mut rows = match task_name {
        None => {
            conn.query(
                "SELECT j.id
                 FROM jobs j
                 JOIN issues i ON j.issue_id = i.id
                 JOIN projects p ON i.project_id = p.id
                 JOIN executions e ON j.execution_id = e.id
                 WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                   -- Top-level nodes have no parent; a workflow is a child job
                   -- (for the delegation tree) yet is addressable as a node by
                   -- its segment, so its own sub-resources resolve.
                   AND (j.parent_job_id IS NULL OR j.agent_config_id = 'workflow')
                   AND j.uri_segment = ?4
                 LIMIT 1",
                params![key.as_str(), number, exec_seq, node_id],
            )
            .await?
        }
        Some(task) => {
            conn.query(
                "SELECT child.id
                 FROM jobs parent
                 JOIN jobs child ON child.parent_job_id = parent.id
                 JOIN issues i ON parent.issue_id = i.id
                 JOIN projects p ON i.project_id = p.id
                 JOIN executions e ON parent.execution_id = e.id
                 WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                   AND parent.parent_job_id IS NULL AND parent.uri_segment = ?4
                   AND child.uri_segment = ?5
                 LIMIT 1",
                params![key.as_str(), number, exec_seq, node_id, task],
            )
            .await?
        }
    };
    rows.next().await?.map(|row| row.text(0)).transpose()
}

/// [`job_id_for_node_coordinate_conn`] against a routed database.
pub(crate) async fn job_id_for_node_coordinate(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> Result<Option<String>, DbError> {
    let (project_key, node_id) = (project_key.to_string(), node_id.to_string());
    let task_name = task_name.map(str::to_string);
    db.read(move |conn| {
        let (project_key, node_id) = (project_key.clone(), node_id.clone());
        let task_name = task_name.clone();
        Box::pin(async move {
            job_id_for_node_coordinate_conn(
                conn,
                &project_key,
                number,
                exec_seq,
                &node_id,
                task_name.as_deref(),
            )
            .await
        })
    })
    .await
}

/// The job's canonical home URI. Issue jobs resolve to their node URI
/// (`cairn://p/{KEY}/{number}/{seq}/{segment}`, nesting sub-agent tasks under
/// their parent); a thread's session resolves to `cairn://p/{KEY}/{thread-name}`
/// and the tasks it spawns nest beneath it the same way. This is the one
/// canonical job-id → home-URI resolution.
/// `Ok(None)` when the job can't be resolved (unknown id, or no `uri_segment`
/// assigned yet).
pub async fn home_uri_for_job(db: &LocalDb, job_id: &str) -> Result<Option<String>, DbError> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { home_uri_for_job_conn(conn, &job_id).await })
    })
    .await
}

/// Connection-level variant of [`home_uri_for_job`] for callers already inside
/// a caller-owned transaction.
pub(crate) async fn home_uri_for_job_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<Option<String>, DbError> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT p.key, i.number, COALESCE(e.seq, 1), j.uri_segment,
                        parent.uri_segment, j.agent_config_id, t.name,
                        (j.thread_id IS NOT NULL AND {session}) AS is_thread_session
                 FROM jobs j
                 LEFT JOIN issues i ON i.id = j.issue_id
                 LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                 LEFT JOIN threads t ON t.id = COALESCE(j.thread_id, parent.thread_id)
                 JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id)
                 LEFT JOIN executions e ON e.id = j.execution_id
                 WHERE j.id = ?1 LIMIT 1",
                session = crate::threads::SESSION_JOB_SHAPE
            ),
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let key = row.text(0)?;
    let segment = row.opt_text(3)?;
    // Only a thread's session job is the thread address itself; everything else
    // under it hangs beneath. A task reaches its thread through its parent, and
    // the pre-cutover jobs migration 0157 re-pointed carry the thread's id
    // directly — so "has a thread id" cannot be the test, or a task would
    // resolve to the very same home URI as the session it belongs to. This is
    // the same rule that decides session identity, spelled once.
    //
    // Resolving a task here is what gives it a home URI at all: without one the
    // run cannot start, and every task spawned from a thread died at
    // `session_start_failed` before its agent spoke.
    if let Some(name) = row.opt_text(6)? {
        let is_session = row.opt_i64(7)?.unwrap_or_default() != 0;
        return Ok(match (is_session, segment) {
            (true, _) => Some(cairn_common::uri::build_thread_uri(&key, &name)),
            (false, Some(segment)) => Some(cairn_common::uri::build_thread_task_uri(
                &key, &name, &segment,
            )),
            (false, None) => None,
        });
    }
    let number = row.i64(1)? as i32;
    let seq = row.i64(2)? as i32;
    let parent_segment = row.opt_text(4)?;
    let is_workflow = row.opt_text(5)?.as_deref() == Some("workflow");
    let parent_segment = (!is_workflow).then_some(parent_segment).flatten();
    Ok(segment.map(|seg| {
        cairn_common::uri::build_job_base_uri(&key, number, seq, &seg, parent_segment.as_deref())
    }))
}

/// Resolve the canonical URI segment of a job's parent, if any. Returns
/// `None` for top-level node jobs (which have `parent_job_id IS NULL`).
///
/// Pair this with `node_uri_segment_for_job` and `build_job_base_uri` to
/// produce the canonical job URI from any caller that has a `job_id`. This
/// is the parent-segment input the URI builder needs to nest a sub-task as
/// `.../{seq}/{parent}/task/{segment}` instead of misreporting it as a
/// top-level node URI.
pub(crate) async fn parent_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
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

pub(crate) async fn node_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
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

#[derive(Serialize)]
pub struct JobTabs {
    available_tabs: Vec<String>,
    initial_tab: String,
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
pub async fn list_jobs_for_thread(db: &LocalDb, thread_id: &str) -> Result<Vec<Job>, CairnError> {
    list_jobs_by_predicate(db, "thread_id = ?1", thread_id, "created_at DESC").await
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
    job_id: String,
    activity: NodeActivity,
}

/// Pure mapping from the three raw facts to a `NodeActivity`. Awaiting-input is
/// checked first and does NOT require the head turn to be `pending`/`running`:
/// an `ask_user`/permission wait transitions the head turn to `yielded` (see
/// `transitions::turn::yield_turn` and the MCP handlers), so a pending
/// prompt/permission row is the authoritative signal regardless of turn state.
/// This mirrors exactly what the per-node chat surface uses
/// (`get_pending_prompt_for_job` / `get_pending_permission_for_job`).
pub(crate) fn derive_node_activity(
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

/// Which jobs a batch of live-activity indicators covers.
///
/// Activity is a property of a job's head turn, not of whatever owns the job, so
/// one query answers for either owner and only the scoping predicate differs. A
/// thread's scope deliberately spans every job it owns — its session plus the
/// sub-agent tasks that session spawns — which is exactly the set its pane
/// renders tabs and badges for.
#[derive(Debug, Clone, Copy)]
pub enum NodeStatusScope<'a> {
    Execution(&'a str),
    Thread(&'a str),
}

impl NodeStatusScope<'_> {
    fn predicate(&self) -> &'static str {
        match self {
            Self::Execution(_) => "j.execution_id = ?1",
            Self::Thread(_) => "j.thread_id = ?1",
        }
    }

    fn owner_id(&self) -> &str {
        match self {
            Self::Execution(id) | Self::Thread(id) => id,
        }
    }
}

/// Batched live-activity indicators for every job in one scope (top-level
/// nodes AND task jobs), computed in one query. Reusable for any status surface
/// that needs the running/awaiting-input/idle distinction without a per-node
/// fan-out of `get_head_turn_for_job` + `get_pending_prompt_for_job` +
/// `get_pending_permission_for_job`.
pub async fn node_status_indicators(
    db: &LocalDb,
    scope: NodeStatusScope<'_>,
) -> Result<Vec<NodeStatusIndicator>, CairnError> {
    let owner_id = scope.owner_id().to_string();
    let predicate = scope.predicate();
    db.read(move |conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT
                        j.id,
                        COALESCE(
                            (SELECT ct.state
                               FROM turns ct
                              WHERE ct.id = j.current_turn_id),
                            (SELECT t.state
                               FROM turns t
                              WHERE t.job_id = j.id
                              ORDER BY t.created_at DESC, t.sequence DESC
                              LIMIT 1)
                        ) AS head_turn_state,
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
                     WHERE {predicate}"
                    ),
                    params![owner_id.as_str()],
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

/// Live status for one thread — the unit a thread row renders.
/// The published thread rollup for one project, materialized once per
/// generation.
///
/// The SQL below joins every thread's jobs and evaluates a correlated head-turn,
/// prompt and permission lookup per job, so it costs the same whether one thread
/// moved or none did. Its invalidation sources fire continuously during normal
/// work, which is how an idle app came to re-run a whole-project rollup in a
/// loop. Reading through the fence makes a repeat read of unchanged state an
/// in-memory clone, and collapses a concurrent cold burst to one SQL execution.
pub async fn published_thread_status_indicators(
    orch: &crate::orchestrator::Orchestrator,
    project_id: &str,
) -> Result<Vec<ThreadStatusIndicator>, CairnError> {
    let failure: std::sync::Mutex<Option<CairnError>> = std::sync::Mutex::new(None);
    let published = orch
        .thread_status_cache
        .get_or_compute(project_id, || async {
            let db = match crate::execution::routing::owning_db_for_project(&orch.db, project_id)
                .await
            {
                Ok(db) => db,
                Err(error) => {
                    if let Ok(mut slot) = failure.lock() {
                        *slot = Some(error);
                    }
                    return None;
                }
            };
            match thread_activity_rows(&db, project_id).await {
                Ok(rows) => Some(std::sync::Arc::new(rows)),
                Err(error) => {
                    if let Ok(mut slot) = failure.lock() {
                        *slot = Some(error);
                    }
                    None
                }
            }
        })
        .await;

    let activity = match published {
        Some(rows) => rows,
        None => {
            return Err(failure
                .lock()
                .ok()
                .and_then(|mut slot| slot.take())
                .unwrap_or_else(|| {
                    CairnError::Internal("failed to rebuild the thread status rollup".to_string())
                }))
        }
    };

    // Unread counts are computed fresh on every read rather than snapshotted
    // alongside the activity. Their dependency is `events`, which inserts orders
    // of magnitude more often than anything the activity rollup reads and whose
    // change notification carries no project scope — wiring it into the same
    // generation would invalidate every project's snapshot on every event, which
    // is exactly the continuous whole-project rebuild CAIRN-4190 removed.
    // Recomputing instead is safe because the count is capped: the work is
    // bounded by threads-in-project x UNREAD_COUNT_CAP no matter how far behind
    // the operator has fallen.
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id).await?;
    let unread = thread_unread_counts(&db, project_id).await?;

    Ok(activity
        .iter()
        .map(|row| {
            let unread = unread.get(&row.thread_id).copied().unwrap_or_default();
            ThreadStatusIndicator {
                thread_id: row.thread_id.clone(),
                activity: row.activity,
                unread_count: unread.count,
                latest_event_rowid: unread.latest_event_rowid,
            }
        })
        .collect())
}

/// Everything one thread row shows about itself beyond its name: whether work is
/// happening under it right now, and how much of its transcript this operator
/// has not seen. The two are rendered as mutually exclusive trailing states, but
/// they are computed independently and both travel on one row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStatusIndicator {
    thread_id: String,
    /// Rolled up across every job the thread owns with
    /// `AwaitingInput > Running > Idle` precedence, so a thread reads as live
    /// while either its own session or a task it delegated is working.
    activity: NodeActivity,
    /// Eligible transcript entries after this operator's acknowledged watermark,
    /// saturating at [`UNREAD_COUNT_CAP`] so the query cost never scales with how
    /// long a thread has gone unread. The renderer shows `99+` at the cap.
    unread_count: i64,
    /// The newest eligible rowid `unread_count` was computed against. A client
    /// that decides the operator has read this thread hands this back to
    /// `mark_thread_viewed`, so the marker advances to exactly the position the
    /// count described rather than to whatever is newest when the write lands.
    latest_event_rowid: i64,
}

/// One thread's live activity — the half of a thread row that is a pure function
/// of orchestration state, and therefore the half worth caching per generation.
///
/// Separate from [`ThreadStatusIndicator`] because the unread count has a
/// completely different dependency set: it moves on every durable event, which
/// no snapshot keyed on the activity inputs could notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadActivityRow {
    pub thread_id: String,
    pub activity: NodeActivity,
}

/// The one selector behind every thread activity indicator.
///
/// Named so the snapshot rebuild and the query-plan assertion below run the
/// exact same statement: a second, approximate copy of this SQL would let the
/// plan test pass while the projection took a different path.
pub(crate) const THREAD_STATUS_ROWS_SQL: &str = "SELECT
        t.id,
        COALESCE(
            (SELECT ct.state FROM turns ct WHERE ct.id = j.current_turn_id),
            (SELECT tu.state
               FROM turns tu
              WHERE tu.job_id = j.id
              ORDER BY tu.created_at DESC, tu.sequence DESC
              LIMIT 1)
        ) AS head_turn_state,
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
   FROM threads t
   LEFT JOIN jobs j ON j.thread_id = t.id
  WHERE t.project_id = ?1";

/// Batched, project-scoped live activity for every thread, one row per thread.
///
/// A thread's status column is permanently `active` and says nothing, so what a
/// thread row actually wants to show is whether work is happening under it right
/// now. That is the same head-turn question `node_status_indicators` answers,
/// rolled up per thread and computed for the whole project in one statement
/// rather than a per-row fan-out.
pub async fn thread_activity_rows(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<ThreadActivityRow>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(THREAD_STATUS_ROWS_SQL, params![project_id.as_str()])
                .await?;
            // Every thread gets a row, including one whose LEFT JOIN found no
            // job at all: a thread with no work under it is idle, not absent.
            let mut by_thread: HashMap<String, NodeActivity> = HashMap::new();
            while let Some(row) = rows.next().await? {
                let thread_id = row.text(0)?;
                let activity = derive_node_activity(
                    row.opt_text(1)?.as_deref(),
                    row.i64(2)? != 0,
                    row.i64(3)? != 0,
                );
                let entry = by_thread.entry(thread_id).or_insert(NodeActivity::Idle);
                *entry = rollup_activity([*entry, activity]);
            }
            Ok::<Vec<ThreadActivityRow>, DbError>(
                by_thread
                    .into_iter()
                    .map(|(thread_id, activity)| ThreadActivityRow {
                        thread_id,
                        activity,
                    })
                    .collect(),
            )
        })
    })
    .await
    .map_err(CairnError::from)
}

// ============================================================================
// Per-thread unread transcript counts
// ============================================================================

/// The most a thread row will ever report. The badge renders exact values through
/// 99 and `99+` at or above this, so counting past it buys nothing and costs a
/// scan proportional to how long the operator ignored the thread. Saturating here
/// is what makes the rollup's cost independent of transcript size.
pub(crate) const UNREAD_COUNT_CAP: i64 = 100;

/// What counts as one unread transcript entry, spelled once.
///
/// A row in `events` is by definition durable — the live `assistant:streaming`
/// placeholder is synthesized from `message_streams` at read time and never
/// stored — so durability needs no clause. What does need saying is that only
/// TOP-LEVEL entries count: a sub-agent's transcript is nested under the tool use
/// that spawned it (`parent_tool_use_id`), and a delegated task producing four
/// hundred events must not read as four hundred things the operator missed in the
/// parent thread. The task's outcome reaches the thread as its own top-level
/// entry when the session records it.
///
/// Bound to the alias `e`, and used for BOTH the count and the watermark that
/// clears it, so "what is unread" and "what marking viewed consumes" cannot drift.
pub(crate) const UNREAD_EVENT_SHAPE: &str = "e.parent_tool_use_id IS NULL";

/// The one selector behind every thread's unread count.
///
/// A thread's transcript is its reserved session job's, and a job's whole session
/// rotation lineage hangs off `sessions.job_id` — a cold-resume or model switch
/// mints a successor session under the SAME job — so joining through it follows
/// rotation without walking `parent_session_id` by hand. The watermark is a global
/// `events.rowid`, which is also the cursor the transcript delta walks, so the
/// sidebar's notion of "new" and the pane's are the same number.
///
/// The inner `LIMIT` is load-bearing, not decoration: without it this counts every
/// event since the operator last looked, which for an ignored thread is unbounded
/// and is re-counted on every sidebar read.
fn thread_unread_rows_sql() -> String {
    format!(
        "SELECT
        t.id,
        (SELECT COUNT(*) FROM (
            SELECT e.rowid
              FROM sessions s
              JOIN events e ON e.session_id = s.id
             WHERE s.job_id = j.id
               AND {UNREAD_EVENT_SHAPE}
               AND e.rowid > COALESCE(rp.acknowledged_event_rowid, 0)
             LIMIT {UNREAD_COUNT_CAP}
        )) AS unread_count,
        COALESCE((
            SELECT MAX(e.rowid)
              FROM sessions s
              JOIN events e ON e.session_id = s.id
             WHERE s.job_id = j.id
               AND {UNREAD_EVENT_SHAPE}
        ), COALESCE(rp.acknowledged_event_rowid, 0)) AS latest_event_rowid
   FROM threads t
   LEFT JOIN jobs j ON j.thread_id = t.id AND {session}
   LEFT JOIN thread_read_positions rp ON rp.thread_id = t.id
  WHERE t.project_id = ?1",
        session = crate::threads::SESSION_JOB_SHAPE
    )
}

/// One thread's unread facts: how many entries are waiting, and the exact rowid
/// that number was computed against.
///
/// The rowid travels with the count because marking viewed acknowledges it.
/// Without it the client could only say "clear this thread", and the backend
/// would have to re-read its own newest row at command time — which is strictly
/// later than the moment the operator was shown a count, so it would routinely
/// consume entries the client had never been told about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ThreadUnread {
    pub count: i64,
    pub latest_event_rowid: i64,
}

/// Batched, project-scoped unread transcript counts, one entry per thread.
///
/// A thread with no session job, no events, or no read position is present with
/// an explicit zero rather than missing: the caller renders "nothing unread",
/// which is a different statement from "unknown".
pub async fn thread_unread_counts(
    db: &LocalDb,
    project_id: &str,
) -> Result<HashMap<String, ThreadUnread>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let sql = thread_unread_rows_sql();
            let mut rows = conn.query(&sql, params![project_id.as_str()]).await?;
            let mut by_thread: HashMap<String, ThreadUnread> = HashMap::new();
            while let Some(row) = rows.next().await? {
                let thread_id = row.text(0)?;
                let count = row.opt_i64(1)?.unwrap_or(0);
                let latest_event_rowid = row.opt_i64(2)?.unwrap_or(0);
                // A malformed thread with two session-shaped jobs would produce
                // two rows; the larger of each is the honest answer rather than
                // whichever arrived last.
                let entry = by_thread.entry(thread_id).or_default();
                entry.count = entry.count.max(count);
                entry.latest_event_rowid = entry.latest_event_rowid.max(latest_event_rowid);
            }
            Ok::<HashMap<String, ThreadUnread>, DbError>(by_thread)
        })
    })
    .await
    .map_err(CairnError::from)
}

/// Advance one thread's read watermark to the newest entry the SERVER can see.
///
/// `through_rowid` is the client ACKNOWLEDGING a position it was shown: the
/// `latest_event_rowid` that travelled with the count it acted on. It is not a
/// claim the backend trusts — the statement clamps it to the thread's own newest
/// eligible entry, so it can never address another thread's transcript or a row
/// that does not exist, and the update takes `MAX(existing, acknowledged)`, so a
/// stale or concurrent pane cannot walk the marker backwards.
///
/// It is REQUIRED, deliberately. There is no caller that can truthfully report
/// content viewed without knowing which content it displayed, and letting the
/// backend fall back to "whatever is newest right now" would make consuming
/// unseen entries a representable, accepted state again: re-reading at write time
/// is strictly later than the moment the operator was shown a count, so an entry
/// arriving in between would be swallowed without ever having been offered. A
/// thread with no transcript reports position 0, so every caller has an honest
/// value to pass.
///
/// Returns whether the watermark actually moved, which is what tells the caller a
/// projection refresh is worth emitting.
pub async fn mark_thread_viewed(
    db: &LocalDb,
    thread_id: &str,
    through_rowid: i64,
    now: i64,
) -> Result<bool, CairnError> {
    let thread_id = thread_id.to_string();
    db.write(|conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let sql = format!(
                "WITH observed AS (
                     SELECT COALESCE((
                         SELECT MAX(e.rowid)
                           FROM jobs j
                           JOIN sessions s ON s.job_id = j.id
                           JOIN events e ON e.session_id = s.id
                          WHERE j.thread_id = ?1
                            AND {session}
                            AND {shape}
                     ), 0) AS newest
                 )
                 INSERT INTO thread_read_positions
                        (thread_id, acknowledged_event_rowid, updated_at)
                 SELECT ?1, MIN(?3, observed.newest), ?2
                   FROM observed
                  WHERE EXISTS (SELECT 1 FROM threads WHERE id = ?1)
                 ON CONFLICT(thread_id) DO UPDATE SET
                        acknowledged_event_rowid = MAX(
                            thread_read_positions.acknowledged_event_rowid,
                            excluded.acknowledged_event_rowid
                        ),
                        updated_at = excluded.updated_at
                 WHERE excluded.acknowledged_event_rowid
                       > thread_read_positions.acknowledged_event_rowid",
                session = crate::threads::SESSION_JOB_SHAPE,
                shape = UNREAD_EVENT_SHAPE,
            );
            let changed = conn
                .execute(&sql, params![thread_id.as_str(), now, through_rowid])
                .await?;
            Ok::<bool, DbError>(changed > 0)
        })
    })
    .await
    .map_err(CairnError::from)
}

// ============================================================================
// Issue-level status indicators (project sidebar status dots)
// ============================================================================

/// Which agent is live on one of an issue's jobs. The consumer renders the
/// agent's icon, so the config id plus the node name is all it needs here.
/// `activity` is this single job's classification (`derive_node_activity`); the
/// issue's rolled-up activity lives on `IssueStatusIndicator`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IssueAgentRef {
    job_id: String,
    node_name: Option<String>,
    agent_config_id: Option<String>,
    activity: NodeActivity,
    /// Timestamp of the head turn that produced this activity. Consumers use it
    /// to choose the most recently active agent when several jobs are live.
    activity_updated_at: i64,
}

/// The cached pull-request facts for an issue's current execution, mirrored
/// straight from the owning `merge_requests` row: the Cairn-owned `status`
/// (open/merged/closed) plus the last-synced GitHub columns. These are exactly
/// the fields `merge_requests::queries::PR_COLUMNS` caches; nothing here is
/// fetched live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IssuePrIndicator {
    pr_number: Option<i64>,
    pr_url: Option<String>,
    /// Cairn-owned lifecycle: `open` | `merged` | `closed`.
    status: String,
    github_state: Option<String>,
    review_decision: Option<String>,
    mergeable: Option<String>,
    checks_status: Option<String>,
    is_local: bool,
}

/// Live status rollup for one in-progress (active/waiting) issue — the unit the
/// project sidebar renders per issue row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IssueStatusIndicator {
    issue_id: String,
    /// Rolled up across the issue's current-execution jobs with
    /// `AwaitingInput > Running > Idle` precedence: awaiting-input is the
    /// actionable state, so it wins over a merely-running sibling job.
    activity: NodeActivity,
    /// The live (non-idle) jobs' agents, so the sidebar can show which agent is
    /// working. Empty when the issue is idle.
    agents: Vec<IssueAgentRef>,
    /// The issue's current pull request, if any.
    pr: Option<IssuePrIndicator>,
    /// Current-execution job ids. Not itself a rendered field: the transport
    /// command tests these against the orchestrator's in-memory turn-end-checks
    /// set to fill `checks_running`, which no SQL column records.
    pub job_ids: Vec<String>,
    /// Whether turn-end review checks are currently in flight for the issue.
    /// This is NOT persisted anywhere — it is an in-memory runtime fact on the
    /// orchestrator (`Orchestrator::turn_end_checks_in_flight`), so the pure SQL
    /// query here always leaves it `false`; the transport command decorates it
    /// from `job_ids`. Kept on this struct (rather than layered on later) so the
    /// serialized payload is whole.
    pub checks_running: bool,
    /// Whether a latest non-PR artifact version is awaiting human confirmation.
    /// This deliberately excludes `create-pr`: open PR attention is represented
    /// by `pr`, not by the generic artifact-review glyph.
    artifact_waiting: bool,
}

/// Roll several jobs' activities into one issue-level signal with
/// `AwaitingInput > Running > Idle` precedence.
fn rollup_activity(activities: impl IntoIterator<Item = NodeActivity>) -> NodeActivity {
    let mut rolled = NodeActivity::Idle;
    for activity in activities {
        match activity {
            NodeActivity::AwaitingInput => return NodeActivity::AwaitingInput,
            NodeActivity::Running => rolled = NodeActivity::Running,
            NodeActivity::Idle => {}
        }
    }
    rolled
}

/// Batched, project-scoped live status indicators for every in-progress
/// (active/waiting) issue in a project, one row per issue. A small constant
/// number of SQL statements — never a per-issue fan-out — computes, per issue:
/// the rolled-up agent activity (running/awaiting-input/idle) and the live
/// agents behind it, plus the cached PR facts.
///
/// Activity + agents are derived over the issue's CURRENT execution (highest
/// `executions.seq`) using the same three facts as `node_status_indicators`
/// (head-turn state, pending prompt, pending permission) via the shared
/// `derive_node_activity`. The head-turn state is read from `jobs.current_turn_id`
/// first because turn sequence restarts on cold-resume reseed session rotation.
/// The PR is the issue's most relevant `merge_requests`
/// row scoped to that same current execution (an open one preferred, else the
/// most recently updated), matched through BOTH supported ownership shapes:
/// a row whose `job_id` is a current-execution job, or — the legacy first-class
/// PR-node shape (migration 0019) — a row whose `job_id` is an `action_runs.id`
/// whose `parent_job_id` is a current-execution job. This mirrors the
/// parent-job/action-run fallback the other PR readers use
/// (`merge_requests::queries::get_summaries_for_action_runs`).
///
/// `checks_running` is intentionally left `false` here: whether turn-end review
/// checks are in flight is an in-memory orchestrator fact with no DB column, so
/// the transport command fills it from `Orchestrator::turn_end_checks_in_flight`
/// using the returned `job_ids`.
const ISSUE_STATUS_JOB_ROWS_SQL: &str = "SELECT
    i.id AS issue_id,
    j.id AS job_id,
    j.node_name,
    j.agent_config_id,
    COALESCE(
        (SELECT ct.state FROM turns ct WHERE ct.id = j.current_turn_id),
        (SELECT t.state
           FROM turns t
          WHERE t.job_id = j.id
          ORDER BY t.created_at DESC, t.sequence DESC
          LIMIT 1)
    ) AS head_turn_state,
    MAX(
        COALESCE((
            SELECT MAX(ms.updated_at)
              FROM message_streams ms
             WHERE ms.turn_id = j.current_turn_id
        ), 0),
        COALESCE((
            SELECT MAX(ev.created_at)
              FROM events ev
             WHERE ev.turn_id = j.current_turn_id
        ), 0),
        COALESCE((
            SELECT ct.updated_at
              FROM turns ct
             WHERE ct.id = j.current_turn_id
        ), j.updated_at)
    ) AS activity_updated_at,
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
 FROM issues i
 JOIN jobs j ON j.issue_id = i.id
 WHERE i.project_id = ?1
   AND i.status IN ('active', 'waiting')
   AND j.execution_id = (
       SELECT e.id FROM executions e
        WHERE e.issue_id = i.id
        ORDER BY e.seq DESC
        LIMIT 1
   )";

const ISSUE_STATUS_PR_ROWS_SQL: &str = "WITH current_jobs AS (
    SELECT i.id AS issue_id, j.id AS job_id
      FROM issues i
      JOIN jobs j ON j.issue_id = i.id
     WHERE i.project_id = ?1
       AND i.status IN ('active', 'waiting')
       AND j.execution_id = (
           SELECT e.id FROM executions e
            WHERE e.issue_id = i.id
            ORDER BY e.seq DESC
            LIMIT 1
       )
), owned_merge_requests AS (
    SELECT cj.issue_id, m.*
      FROM current_jobs cj
      JOIN merge_requests m ON m.job_id = cj.job_id
    UNION ALL
    SELECT cj.issue_id, m.*
      FROM current_jobs cj
      JOIN action_runs ar ON ar.parent_job_id = cj.job_id
      JOIN merge_requests m ON m.job_id = ar.id
)
SELECT issue_id, github_pr_number, github_pr_url, status, github_state,
       github_review, github_mergeable, checks_status, is_local
  FROM owned_merge_requests
 ORDER BY issue_id,
          CASE status WHEN 'open' THEN 0 ELSE 1 END,
          updated_at DESC";

pub async fn issue_status_indicators(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<IssueStatusIndicator>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            // (1) Base set: every in-progress issue in the project. Included even
            // with zero jobs, so a freshly-activated issue still gets a row.
            let mut issue_ids: Vec<String> = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT id FROM issues
                      WHERE project_id = ?1
                        AND status IN ('active', 'waiting')
                      ORDER BY id",
                    params![project_id.as_str()],
                )
                .await?;
            while let Some(row) = rows.next().await? {
                issue_ids.push(row.text(0)?);
            }

            // (2) Current-execution jobs for those issues, each with the three
            // activity facts and the agent columns, in ONE statement. Includes
            // task jobs (a running sub-agent means the issue is running), exactly
            // like `node_status_indicators`.
            struct JobRow {
                issue_id: String,
                job_id: String,
                node_name: Option<String>,
                agent_config_id: Option<String>,
                activity: NodeActivity,
                activity_updated_at: i64,
            }
            let mut job_rows: Vec<JobRow> = Vec::new();
            let mut rows = conn
                .query(ISSUE_STATUS_JOB_ROWS_SQL, params![project_id.as_str()])
                .await?;
            while let Some(row) = rows.next().await? {
                let issue_id = row.text(0)?;
                let job_id = row.text(1)?;
                let node_name = row.opt_text(2)?;
                let agent_config_id = row.opt_text(3)?;
                let head_turn_state = row.opt_text(4)?;
                let activity_updated_at = row.i64(5)?;
                let has_pending_prompt = row.i64(6)? != 0;
                let has_pending_permission = row.i64(7)? != 0;
                job_rows.push(JobRow {
                    issue_id,
                    job_id,
                    node_name,
                    agent_config_id,
                    activity: derive_node_activity(
                        head_turn_state.as_deref(),
                        has_pending_prompt,
                        has_pending_permission,
                    ),
                    activity_updated_at,
                });
            }

            // (3) The most relevant PR for each issue's CURRENT execution: open
            // preferred, else the most recently updated. Scoped to the same
            // highest-`seq` execution as the activity above (keying by `issue_id`
            // alone would leak a stale open PR from an OLDER execution onto an
            // issue whose current execution has none yet), and matched through
            // BOTH `merge_requests` ownership shapes: `job_id` is either a
            // current-execution job directly, or an `action_runs.id` whose
            // `parent_job_id` is a current-execution job (migration 0019's
            // first-class PR-node shape, which the other PR readers resolve via
            // the same action-run parent fallback).
            struct PrRow {
                issue_id: String,
                pr: IssuePrIndicator,
            }
            let mut pr_rows: Vec<PrRow> = Vec::new();
            let mut rows = conn
                .query(ISSUE_STATUS_PR_ROWS_SQL, params![project_id.as_str()])
                .await?;
            while let Some(row) = rows.next().await? {
                pr_rows.push(PrRow {
                    issue_id: row.text(0)?,
                    pr: IssuePrIndicator {
                        pr_number: row.opt_i64(1)?,
                        pr_url: row.opt_text(2)?,
                        status: row.text(3)?,
                        github_state: row.opt_text(4)?,
                        review_decision: row.opt_text(5)?,
                        mergeable: row.opt_text(6)?,
                        checks_status: row.opt_text(7)?,
                        is_local: row.opt_i64(8)?.unwrap_or(0) != 0,
                    },
                });
            }

            // Group jobs and PRs by issue in Rust, then assemble in the base
            // order. First PR row per issue wins (the ORDER BY already prefers an
            // open/most-recent one).
            let mut jobs_by_issue: HashMap<String, Vec<JobRow>> = HashMap::new();
            for job in job_rows {
                jobs_by_issue
                    .entry(job.issue_id.clone())
                    .or_default()
                    .push(job);
            }
            let mut pr_by_issue: HashMap<String, IssuePrIndicator> = HashMap::new();
            for pr_row in pr_rows {
                pr_by_issue.entry(pr_row.issue_id).or_insert(pr_row.pr);
            }

            // (4) Issues whose current execution has a latest non-PR artifact
            // version still awaiting confirmation. An older unconfirmed version
            // does not count once a newer version in the same output chain exists.
            let mut artifact_waiting_issues = std::collections::HashSet::new();
            let mut rows = conn
                .query(
                    "SELECT DISTINCT i.id
                       FROM issues i
                       JOIN jobs j ON j.issue_id = i.id
                       JOIN artifacts a ON a.job_id = j.id
                      WHERE i.project_id = ?1
                        AND i.status IN ('active', 'waiting')
                        AND j.execution_id = (
                            SELECT e.id FROM executions e
                             WHERE e.issue_id = i.id
                             ORDER BY e.seq DESC
                             LIMIT 1
                        )
                        AND a.artifact_type != 'create-pr'
                        AND a.confirmed = 0
                        AND NOT EXISTS (
                            SELECT 1 FROM artifacts newer
                             WHERE newer.job_id = a.job_id
                               AND newer.output_name IS a.output_name
                               AND newer.version > a.version
                        )",
                    params![project_id.as_str()],
                )
                .await?;
            while let Some(row) = rows.next().await? {
                artifact_waiting_issues.insert(row.text(0)?);
            }

            let mut out = Vec::with_capacity(issue_ids.len());
            for issue_id in issue_ids {
                let jobs = jobs_by_issue.remove(&issue_id).unwrap_or_default();
                let activity = rollup_activity(jobs.iter().map(|job| job.activity));
                let mut agents: Vec<_> = jobs
                    .iter()
                    .filter(|job| job.activity != NodeActivity::Idle)
                    .map(|job| IssueAgentRef {
                        job_id: job.job_id.clone(),
                        node_name: job.node_name.clone(),
                        agent_config_id: job.agent_config_id.clone(),
                        activity: job.activity,
                        activity_updated_at: job.activity_updated_at,
                    })
                    .collect();
                agents.sort_by(|a, b| {
                    b.activity_updated_at
                        .cmp(&a.activity_updated_at)
                        .then_with(|| a.job_id.cmp(&b.job_id))
                });
                let job_ids = jobs.iter().map(|job| job.job_id.clone()).collect();
                out.push(IssueStatusIndicator {
                    issue_id: issue_id.clone(),
                    activity,
                    agents,
                    pr: pr_by_issue.remove(&issue_id),
                    job_ids,
                    checks_running: false,
                    artifact_waiting: artifact_waiting_issues.contains(&issue_id),
                });
            }
            Ok::<Vec<IssueStatusIndicator>, DbError>(out)
        })
    })
    .await
    .map_err(CairnError::from)
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
    use super::{
        derive_node_activity, home_uri_for_job, issue_status_indicators, mark_thread_viewed,
        node_status_indicators, published_thread_status_indicators, thread_activity_rows,
        thread_unread_counts, thread_unread_rows_sql, NodeActivity, NodeStatusScope,
        ISSUE_STATUS_JOB_ROWS_SQL, ISSUE_STATUS_PR_ROWS_SQL, THREAD_STATUS_ROWS_SQL,
        UNREAD_COUNT_CAP,
    };
    use crate::storage::{DbError, LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use cairn_db::turso::params;
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
    async fn issue_status_job_rows_use_hot_lookup_indexes() {
        let db = test_db().await;
        let jobs_issue_index: (String, Vec<String>) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut table_rows = conn
                        .query(
                            "SELECT tbl_name FROM sqlite_master
                              WHERE type = 'index' AND name = 'idx_jobs_issue_id'",
                            (),
                        )
                        .await?;
                    let table = table_rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("idx_jobs_issue_id is missing".to_string()))?
                        .text(0)?;

                    let mut column_rows = conn
                        .query("PRAGMA index_info('idx_jobs_issue_id')", ())
                        .await?;
                    let mut columns = Vec::new();
                    while let Some(row) = column_rows.next().await? {
                        columns.push(row.text(2)?);
                    }
                    Ok((table, columns))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            jobs_issue_index,
            ("jobs".to_string(), vec!["issue_id".to_string()]),
            "the migration must retain the exact jobs(issue_id) index"
        );

        let sql = format!("EXPLAIN QUERY PLAN {ISSUE_STATUS_JOB_ROWS_SQL}");
        let plan: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(&sql, params!["p"]).await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps)
                })
            })
            .await
            .unwrap();

        assert!(
            plan.iter()
                .any(|step| step.contains("SEARCH j USING INDEX idx_jobs_")),
            "jobs must use an indexed search, got {plan:?}"
        );
        assert!(
            plan.iter()
                .any(|step| { step.contains("SEARCH ms USING INDEX idx_message_streams_turn_id") }),
            "message streams must be searched by turn_id, got {plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.contains("SCAN j")),
            "issue status must not scan jobs, got {plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.contains("SCAN ms")),
            "issue status must not scan message streams, got {plan:?}"
        );

        let sql = format!("EXPLAIN QUERY PLAN {ISSUE_STATUS_PR_ROWS_SQL}");
        let pr_plan: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(&sql, params!["p"]).await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps)
                })
            })
            .await
            .unwrap();
        assert!(
            pr_plan.iter().any(|step| step.contains("idx_mr_job")),
            "merge requests must use their owner index, got {pr_plan:?}"
        );
        assert!(
            pr_plan
                .iter()
                .any(|step| step.contains("idx_action_runs_parent_job")),
            "action-run ownership must use its parent index, got {pr_plan:?}"
        );
        assert!(
            !pr_plan.iter().any(|step| step.contains("MULTI-INDEX OR")),
            "PR ownership branches must remain independently indexable, got {pr_plan:?}"
        );
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
        // Cold-resume reseed rotates to a fresh session whose turn sequence starts
        // at 1 again. The current pointer, not max(sequence), is the durable head.
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-reseed-old', 's-reseed-old', 'j-reseed', 9, 'complete', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-reseed-new', 's-reseed-new', 'j-reseed', 1, 'running', 'follow_up', 2, 2)",
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
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, current_turn_id)
             VALUES ('j-reseed', 'e', 'i', 'p', 'Reseed', 'running', 1, 1, 'reseed', 't-reseed-new')",
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

        let indicators = node_status_indicators(&db, NodeStatusScope::Execution("e"))
            .await
            .unwrap();
        let by_job: HashMap<String, NodeActivity> = indicators
            .into_iter()
            .map(|indicator| (indicator.job_id, indicator.activity))
            .collect();

        assert_eq!(by_job.len(), 7, "every job in the execution is reported");
        assert_eq!(by_job["j-run"], NodeActivity::Running);
        assert_eq!(by_job["j-idle"], NodeActivity::Idle);
        assert_eq!(by_job["j-prompt"], NodeActivity::AwaitingInput);
        assert_eq!(by_job["j-perm"], NodeActivity::AwaitingInput);
        assert_eq!(by_job["j-none"], NodeActivity::Idle);
        assert_eq!(by_job["task-1"], NodeActivity::Running);
        assert_eq!(by_job["j-reseed"], NodeActivity::Running);
    }

    /// A thread's live activity is its own to report: its session job carries no
    /// execution, so the execution-scoped query could never see it and every
    /// thread row rendered a permanently idle indicator. The thread scope spans
    /// the session and the tasks it spawns, and stops at the thread's edge.
    #[tokio::test]
    async fn thread_scope_reports_the_session_and_its_tasks_only() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'cairn', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('th', 'p', 'roadmap', 'active', 'none', 1, 1),
                    ('th-other', 'p', 'neighbour', 'active', 'none', 1, 1)",
        )
        .await;
        // The session job: branchless, parentless, and owned by a thread rather
        // than an execution.
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-session', 'th', 'p', 'thread', 'idle', 1, 1, 'thread')",
        )
        .await;
        // A sub-agent task the session spawned carries the thread's id too.
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, parent_job_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-task', 'th', 'j-session', 'p', 'Survey', 'complete', 1, 1, 'survey')",
        )
        .await;
        // A neighbouring thread's session must not leak into this scope.
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-other', 'th-other', 'p', 'thread', 'idle', 1, 1, 'thread')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('s-thread', 'j-session', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-thread', 's-thread', 'j-session', 1, 'running', 'follow_up', 1, 1)",
        )
        .await;
        exec(
            &db,
            "UPDATE jobs SET current_turn_id = 't-thread' WHERE id = 'j-session'",
        )
        .await;

        let by_job: HashMap<String, NodeActivity> =
            node_status_indicators(&db, NodeStatusScope::Thread("th"))
                .await
                .unwrap()
                .into_iter()
                .map(|indicator| (indicator.job_id, indicator.activity))
                .collect();

        assert_eq!(
            by_job.len(),
            2,
            "the thread's session and its task, and no more"
        );
        assert_eq!(
            by_job["j-session"],
            NodeActivity::Running,
            "a running head turn is live activity even though the job status is idle"
        );
        assert_eq!(by_job["j-task"], NodeActivity::Idle);
    }

    /// Every thread in the project gets exactly one row, and a thread reads as
    /// live when EITHER its own session or a task it delegated is working — the
    /// same `AwaitingInput > Running > Idle` precedence an issue row rolls up.
    #[tokio::test]
    async fn thread_status_indicators_roll_up_per_thread_and_cover_every_thread() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'cairn', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('busy', 'p', 'busy', 'active', 'none', 1, 1),
                    ('asking', 'p', 'asking', 'active', 'none', 1, 1),
                    ('quiet', 'p', 'quiet', 'active', 'none', 1, 1),
                    ('empty', 'p', 'empty', 'active', 'none', 1, 1)",
        )
        .await;
        // `busy` is idle itself but has a running task under it.
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('busy-session', 'busy', 'p', 'thread', 'idle', 1, 1, 'thread'),
                    ('busy-task', 'busy', 'p', 'Survey', 'running', 1, 1, 'survey'),
                    ('asking-session', 'asking', 'p', 'thread', 'idle', 1, 1, 'thread'),
                    ('quiet-session', 'quiet', 'p', 'thread', 'idle', 1, 1, 'thread')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('s-busy', 'busy-task', 1, 1), ('s-asking', 'asking-session', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-busy', 's-busy', 'busy-task', 1, 'running', 'follow_up', 1, 1),
                    ('t-asking', 's-asking', 'asking-session', 1, 'yielded', 'follow_up', 1, 1)",
        )
        .await;
        exec(
            &db,
            "UPDATE jobs SET current_turn_id = 't-busy' WHERE id = 'busy-task'",
        )
        .await;
        exec(
            &db,
            "UPDATE jobs SET current_turn_id = 't-asking' WHERE id = 'asking-session'",
        )
        .await;
        // A yielded turn is idle by turn state alone; the unanswered prompt is
        // what makes the thread actionable.
        exec(
            &db,
            "INSERT INTO runs(id, job_id, created_at, updated_at)
             VALUES ('r-asking', 'asking-session', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO prompts(id, run_id, turn_id, questions, response, created_at)
             VALUES ('p-asking', 'r-asking', 't-asking', '[]', NULL, 1)",
        )
        .await;

        let by_thread: HashMap<String, NodeActivity> = thread_activity_rows(&db, "p")
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.thread_id, row.activity))
            .collect();

        assert_eq!(by_thread.len(), 4, "every thread in the project gets a row");
        assert_eq!(
            by_thread["busy"],
            NodeActivity::Running,
            "a running task makes its thread read as live"
        );
        assert_eq!(by_thread["asking"], NodeActivity::AwaitingInput);
        assert_eq!(by_thread["quiet"], NodeActivity::Idle);
        assert_eq!(
            by_thread["empty"],
            NodeActivity::Idle,
            "a thread with no jobs at all is idle, not absent"
        );
    }

    // ── Per-thread unread transcript counts ────────────────────────────────

    /// A project with one thread whose reserved session job owns one session.
    /// Everything the unread rollup reads, and nothing it does not.
    async fn unread_fixture() -> LocalDb {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'cairn', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('th', 'p', 'design', 'active', 'none', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('th-session', 'th', 'p', 'thread', 'idle', 1, 1, 'thread')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('s1', 'th-session', 1, 1)",
        )
        .await;
        // Events are foreign-keyed to a run; the unread shape never reads it, but
        // the row has to exist for the inserts below to land.
        exec(
            &db,
            "INSERT INTO runs(id, job_id, created_at, updated_at)
             VALUES ('r1', 'th-session', 1, 1)",
        )
        .await;
        db
    }

    /// One durable transcript event. `parent` non-null makes it a nested
    /// sub-agent entry, which the unread shape must skip.
    async fn insert_event(
        db: &LocalDb,
        id: &str,
        run: &str,
        session: &str,
        seq: i64,
        parent: Option<&str>,
    ) {
        let id = id.to_string();
        let run = run.to_string();
        let session = session.to_string();
        let parent = parent.map(str::to_string);
        db.write(|conn| {
            let (id, run, session, parent) =
                (id.clone(), run.clone(), session.clone(), parent.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type,
                                        data, created_at, parent_tool_use_id)
                     VALUES (?1, ?2, ?3, ?4, 1, 'assistant', '{}', 1, ?5)",
                    params![
                        id.as_str(),
                        run.as_str(),
                        session.as_str(),
                        seq,
                        parent.as_deref()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn unread_for(db: &LocalDb, thread_id: &str) -> i64 {
        thread_unread_counts(db, "p")
            .await
            .unwrap()
            .get(thread_id)
            .map(|unread| unread.count)
            .unwrap_or(-1)
    }

    async fn latest_rowid_for(db: &LocalDb, thread_id: &str) -> i64 {
        thread_unread_counts(db, "p")
            .await
            .unwrap()
            .get(thread_id)
            .map(|unread| unread.latest_event_rowid)
            .unwrap_or(-1)
    }

    /// Mark a thread viewed the way the UI does: read the position the sidebar
    /// would have been shown, then acknowledge exactly that. There is no
    /// "acknowledge whatever is newest" shortcut to reach for — the parameter is
    /// required precisely so no caller, test or otherwise, can express it.
    async fn view_at_current_position(db: &LocalDb, thread_id: &str, now: i64) -> bool {
        let shown = latest_rowid_for(db, thread_id).await;
        mark_thread_viewed(db, thread_id, shown, now).await.unwrap()
    }

    async fn acknowledged(db: &LocalDb, thread_id: &str) -> i64 {
        let thread_id = thread_id.to_string();
        db.query_opt(
            "SELECT acknowledged_event_rowid FROM thread_read_positions WHERE thread_id = ?1",
            params![thread_id.as_str()],
            |row| row.i64(0),
        )
        .await
        .unwrap()
        .unwrap_or(-1)
    }

    /// Marking viewed consumes the position the caller was SHOWN, not whatever
    /// is newest when the write executes.
    ///
    /// This is the race the read cursor exists to survive: the sidebar's count
    /// query and the pane's transcript query refetch independently, so an entry
    /// can land in between the count the operator saw and the write that acts on
    /// it. Re-reading MAX(rowid) at command time would consume that entry
    /// without it ever having been offered.
    #[tokio::test]
    async fn marking_viewed_acknowledges_only_the_position_it_was_given() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        insert_event(&db, "e2", "r1", "s1", 2, None).await;
        let shown = latest_rowid_for(&db, "th").await;
        assert_eq!(unread_for(&db, "th").await, 2);

        // An entry lands after the count was computed but before the mark runs.
        insert_event(&db, "e3", "r1", "s1", 3, None).await;

        assert!(mark_thread_viewed(&db, "th", shown, 100).await.unwrap());
        assert_eq!(acknowledged(&db, "th").await, shown);
        assert_eq!(
            unread_for(&db, "th").await,
            1,
            "the entry that arrived after the count stays unread"
        );
    }

    /// An acknowledgement is clamped to the thread's own newest entry, so an
    /// out-of-date or oversized value cannot swallow entries that do not exist
    /// yet — or that belong to another thread.
    #[tokio::test]
    async fn an_oversized_acknowledgement_is_clamped_to_this_threads_newest_entry() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        let newest = latest_rowid_for(&db, "th").await;

        assert!(mark_thread_viewed(&db, "th", 999_999, 100).await.unwrap());
        assert_eq!(acknowledged(&db, "th").await, newest);

        insert_event(&db, "e2", "r1", "s1", 2, None).await;
        assert_eq!(
            unread_for(&db, "th").await,
            1,
            "a later entry is unread despite the earlier over-claim"
        );
    }

    /// A thread with no transcript reports a floor rather than nothing, so a
    /// caller always has a position to acknowledge.
    #[tokio::test]
    async fn latest_rowid_falls_back_to_the_stored_watermark() {
        let db = unread_fixture().await;
        assert_eq!(latest_rowid_for(&db, "th").await, 0);

        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        view_at_current_position(&db, "th", 100).await;
        let acked = acknowledged(&db, "th").await;
        exec(&db, "DELETE FROM events WHERE session_id = 's1'").await;

        assert_eq!(
            latest_rowid_for(&db, "th").await,
            acked,
            "with the transcript gone the acknowledged position is still the floor"
        );
    }

    /// A thread nobody has looked at reports everything top-level in its session
    /// job's lineage, and nothing else: not a nested sub-agent entry, and not
    /// another thread's transcript.
    #[tokio::test]
    async fn unread_counts_top_level_session_entries_only() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        insert_event(&db, "e2", "r1", "s1", 2, Some("toolu_1")).await;
        insert_event(&db, "e3", "r1", "s1", 3, None).await;

        assert_eq!(
            unread_for(&db, "th").await,
            2,
            "a delegated task's nested transcript is not something the operator \
             missed in the parent thread"
        );
    }

    /// Three shapes that all mean "nothing unread" must all be PRESENT with a
    /// zero rather than missing: a caller that treats absence as unknown would
    /// otherwise never learn a thread is caught up.
    #[tokio::test]
    async fn unread_reports_explicit_zero_for_empty_threads() {
        let db = unread_fixture().await;
        exec(
            &db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('no-job', 'p', 'no-job', 'active', 'none', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('no-session', 'no-job', 'p', 'Survey', 'idle', 1, 1, 'survey')",
        )
        .await;

        let counts = thread_unread_counts(&db, "p").await.unwrap();
        assert_eq!(counts.len(), 2, "one row per thread in the project");
        assert_eq!(
            counts["th"].count, 0,
            "a session with no events is caught up"
        );
        assert_eq!(
            counts["no-job"].count, 0,
            "a thread with only a task job, no session, is caught up"
        );
    }

    /// The whole lifecycle in one pass: unread accumulates, viewing clears it to
    /// the server's own latest rowid, and entries that land afterwards are unread
    /// again.
    #[tokio::test]
    async fn marking_viewed_clears_to_the_server_watermark_and_reaccumulates() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        insert_event(&db, "e2", "r1", "s1", 2, None).await;
        assert_eq!(unread_for(&db, "th").await, 2);

        assert!(view_at_current_position(&db, "th", 100).await);
        assert_eq!(unread_for(&db, "th").await, 0);

        insert_event(&db, "e3", "r1", "s1", 3, None).await;
        assert_eq!(
            unread_for(&db, "th").await,
            1,
            "an entry written after the operator looked away is unread again"
        );
    }

    /// Marking viewed twice with nothing new in between must not report movement:
    /// the caller uses that answer to decide whether to refresh a projection, and
    /// a permanently-true answer is a refresh loop.
    #[tokio::test]
    async fn marking_viewed_is_idempotent_and_never_walks_backwards() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        assert!(view_at_current_position(&db, "th", 100).await);
        assert!(
            !view_at_current_position(&db, "th", 200).await,
            "a second view with nothing new moved no watermark"
        );

        // Force a stale marker, as a racing second pane would, and prove the
        // monotonic update refuses to regress it.
        exec(
            &db,
            "UPDATE thread_read_positions SET acknowledged_event_rowid = 999999 WHERE thread_id = 'th'",
        )
        .await;
        assert!(!view_at_current_position(&db, "th", 300).await);
        assert_eq!(
            acknowledged(&db, "th").await,
            999999,
            "an older watermark cannot overwrite a newer one"
        );
    }

    /// A rotated session is the same transcript by another name, and a rename
    /// changes nothing about what was read: the cursor is keyed by thread id, so
    /// both survive.
    #[tokio::test]
    async fn unread_survives_session_rotation_and_rename() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        assert!(view_at_current_position(&db, "th", 100).await);

        // Cold-resume rotation: a successor session under the SAME job.
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, parent_session_id, sequence, created_at, updated_at)
             VALUES ('s2', 'th-session', 's1', 2, 2, 2)",
        )
        .await;
        exec(&db, "UPDATE threads SET name = 'renamed' WHERE id = 'th'").await;
        insert_event(&db, "e2", "r1", "s2", 2, None).await;

        assert_eq!(
            unread_for(&db, "th").await,
            1,
            "the successor session's entry is unread, and the predecessor's stays read"
        );
    }

    /// Viewing one thread says nothing about any other.
    #[tokio::test]
    async fn marking_viewed_touches_only_the_named_thread() {
        let db = unread_fixture().await;
        exec(
            &db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('other', 'p', 'other', 'active', 'none', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('other-session', 'other', 'p', 'thread', 'idle', 1, 1, 'thread')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('s-other', 'other-session', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO runs(id, job_id, created_at, updated_at)
             VALUES ('r-other', 'other-session', 1, 1)",
        )
        .await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        insert_event(&db, "e2", "r-other", "s-other", 1, None).await;

        view_at_current_position(&db, "th", 100).await;

        let counts = thread_unread_counts(&db, "p").await.unwrap();
        assert_eq!(counts["th"].count, 0);
        assert_eq!(
            counts["other"].count, 1,
            "a sibling thread keeps its unread entries"
        );
    }

    /// The count saturates. This is the property that keeps the rollup's cost
    /// independent of how long a thread has been ignored, so it is asserted as
    /// behavior rather than left to the SQL to imply.
    #[tokio::test]
    async fn unread_saturates_at_the_cap() {
        let db = unread_fixture().await;
        for seq in 1..=(UNREAD_COUNT_CAP + 25) {
            insert_event(&db, &format!("e{seq}"), "r1", "s1", seq, None).await;
        }

        assert_eq!(unread_for(&db, "th").await, UNREAD_COUNT_CAP);
    }

    /// A thread deleted takes its read position with it, so a recycled id cannot
    /// inherit a stranger's watermark.
    #[tokio::test]
    async fn deleting_a_thread_cascades_its_read_position() {
        let db = unread_fixture().await;
        insert_event(&db, "e1", "r1", "s1", 1, None).await;
        view_at_current_position(&db, "th", 100).await;
        // Tear the thread's work down the way a real delete does, innermost first;
        // the read position is the one row nothing points at, so only the
        // cascade can remove it.
        exec(&db, "DELETE FROM events WHERE session_id = 's1'").await;
        exec(&db, "DELETE FROM runs WHERE job_id = 'th-session'").await;
        exec(&db, "DELETE FROM sessions WHERE job_id = 'th-session'").await;
        exec(&db, "DELETE FROM jobs WHERE thread_id = 'th'").await;
        exec(&db, "DELETE FROM threads WHERE id = 'th'").await;

        let remaining: i64 = db
            .query_opt(
                "SELECT COUNT(*) FROM thread_read_positions",
                params![],
                |row| row.i64(0),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remaining, 0);
    }

    /// The unread rollup's plan, held to the same bar as the activity rollup's:
    /// every access path is a named index. The counted range is
    /// `events(session_id, rowid > ack)`, which `idx_events_session_id` answers
    /// directly because a SQLite index is ordered by (key, rowid).
    #[tokio::test]
    async fn thread_unread_rows_use_indexed_access_paths() {
        let db = test_db().await;
        let sql = format!("EXPLAIN QUERY PLAN {}", thread_unread_rows_sql());
        let plan: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(&sql, params!["p"]).await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps)
                })
            })
            .await
            .unwrap();
        let plan_text = plan.join("\n");

        assert!(
            !plan_text.contains("AUTOMATIC"),
            "an automatic index means the schema is missing a real access path \
             this query needs, rebuilt per execution:\n{plan_text}"
        );
        assert!(
            plan_text.contains("idx_jobs_thread_id"),
            "the thread->session-job join must reach jobs through jobs(thread_id):\n{plan_text}"
        );
        assert!(
            plan_text.contains("idx_events_session_id"),
            "the counted event range must be seeked through events(session_id):\n{plan_text}"
        );
    }

    /// The plan for the thread rollup, captured before assuming anything about
    /// what it costs.
    ///
    /// The snapshot is the primary fix — an idle app stops running this at all.
    /// This asserts the shape of the one rebuild that remains: the outer scan is
    /// over `threads` filtered by project, and every join and correlated lookup
    /// under it reaches its rows through a named index rather than a scan or an
    /// automatic index Turso built on the fly.
    #[tokio::test]
    async fn thread_status_rows_use_indexed_access_paths() {
        let db = test_db().await;
        let sql = format!("EXPLAIN QUERY PLAN {THREAD_STATUS_ROWS_SQL}");
        let plan: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(&sql, params!["p"]).await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps)
                })
            })
            .await
            .unwrap();
        let plan_text = plan.join("\n");

        assert!(
            !plan_text.contains("AUTOMATIC"),
            "an automatic index means the schema is missing a real access path this \
             query needs, rebuilt per execution:\n{plan_text}"
        );
        assert!(
            plan_text.contains("jobs") && plan_text.contains("idx_jobs_thread_id"),
            "the thread->jobs join must reach jobs through jobs(thread_id):\n{plan_text}"
        );
    }

    /// An orchestrator over a caller-supplied database, so a snapshot test can
    /// seed rows and then read through the published projection.
    fn orchestrator_over(db: LocalDb) -> crate::orchestrator::Orchestrator {
        use crate::services::testing::TestServicesBuilder;
        let config_dir = tempfile::tempdir().unwrap().keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = std::sync::Arc::new(crate::db::DbState::new(
            std::sync::Arc::new(db),
            std::sync::Arc::new(crate::storage::SearchIndex::open_or_create(index_path).unwrap()),
        ));
        crate::orchestrator::Orchestrator::builder(
            db_state,
            std::sync::Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build()
    }

    /// Seed one project with one thread whose single job is idle.
    async fn seed_one_quiet_thread(db: &LocalDb) {
        exec(
            db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'cairn', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('t', 'p', 'quiet', 'active', 'none', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j', 't', 'p', 'thread', 'idle', 1, 1, 'thread')",
        )
        .await;
    }

    /// The published projection answers repeat reads of unchanged state from
    /// memory. This is the idle path: a mounted thread list re-asking costs one
    /// clone, not a whole-project SQL rollup.
    #[tokio::test(flavor = "current_thread")]
    async fn a_warm_thread_snapshot_runs_no_sql() {
        let db = test_db().await;
        seed_one_quiet_thread(&db).await;
        let orch = orchestrator_over(db);

        let first = published_thread_status_indicators(&orch, "p")
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].activity, NodeActivity::Idle);
        for _ in 0..10 {
            assert_eq!(
                published_thread_status_indicators(&orch, "p")
                    .await
                    .unwrap(),
                first
            );
        }

        let counters = orch.thread_status_cache.counters();
        assert_eq!(counters.misses, 1, "one rebuild served every read");
        assert_eq!(counters.hits, 10);
    }

    /// A concurrent cold burst produces one rebuild between them, not one each.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_thread_reads_rebuild_once() {
        let db = test_db().await;
        seed_one_quiet_thread(&db).await;
        let orch = std::sync::Arc::new(orchestrator_over(db));

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(12));
        let mut readers = Vec::new();
        for _ in 0..12 {
            let orch = orch.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                published_thread_status_indicators(&orch, "p")
                    .await
                    .unwrap()
                    .len()
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), 1);
        }

        assert_eq!(orch.thread_status_cache.counters().misses, 1);
    }

    /// The snapshot follows a real transition, and only a real one. A turn going
    /// running is visible on the next read; a merge-request change in the same
    /// project is not an input and does not rebuild anything.
    #[tokio::test(flavor = "current_thread")]
    async fn a_thread_transition_rebuilds_and_an_unrelated_change_does_not() {
        let db = test_db().await;
        seed_one_quiet_thread(&db).await;
        let orch = orchestrator_over(db);

        let idle = published_thread_status_indicators(&orch, "p")
            .await
            .unwrap();
        assert_eq!(idle[0].activity, NodeActivity::Idle);

        // An unrelated table's change must leave the snapshot warm.
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "merge_requests", "projectId": "p"}),
        );
        published_thread_status_indicators(&orch, "p")
            .await
            .unwrap();
        assert_eq!(
            orch.thread_status_cache.counters().misses,
            1,
            "a merge request is not an input to thread activity"
        );

        exec(
            &orch.db.local,
            "INSERT INTO sessions(id, job_id, created_at, updated_at) VALUES ('s', 'j', 1, 1)",
        )
        .await;
        exec(
            &orch.db.local,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('turn', 's', 'j', 1, 'running', 'follow_up', 1, 1)",
        )
        .await;
        exec(
            &orch.db.local,
            "UPDATE jobs SET current_turn_id = 'turn' WHERE id = 'j'",
        )
        .await;
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "projectId": "p"}),
        );

        let running = published_thread_status_indicators(&orch, "p")
            .await
            .unwrap();
        assert_eq!(
            running[0].activity,
            NodeActivity::Running,
            "the transition must reach the UI, not sit behind a warm snapshot"
        );
        assert_eq!(orch.thread_status_cache.counters().misses, 2);
    }

    /// A thread's session and a task it spawned both resolve a home URI, and the
    /// task takes the thread-scoped `/task/{segment}` form the thread read
    /// surface addresses.
    ///
    /// The task inherits no issue and no execution seq, so the issue-shaped
    /// builder could produce nothing for it and the run died at
    /// `session_start_failed` before its agent ever spoke. Every existing
    /// fixture here is issue-shaped, which is how that shipped.
    #[tokio::test]
    async fn a_thread_session_and_the_task_it_spawns_both_resolve_a_home_uri() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'cairn', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
             VALUES ('t', 'p', 'thread-ux', 'active', 'none', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs (id, thread_id, project_id, status, uri_segment, node_name,
                               created_at, updated_at)
             VALUES ('j-session', 't', 'p', 'running', 'thread', 'Thread', 1, 1)",
        )
        .await;
        // Delegation books its packets in a synthetic execution carrying no issue,
        // and the task job hangs off the session by parent_job_id alone.
        exec(
            &db,
            "INSERT INTO executions (id, recipe_id, project_id, status, started_at, seq)
             VALUES ('e', 'delegation', 'p', 'running', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs (id, parent_job_id, execution_id, project_id, status, uri_segment,
                               node_name, created_at, updated_at)
             VALUES ('j-task', 'j-session', 'e', 'p', 'pending', 'post-migration',
                     'post-migration smoke test', 2, 2)",
        )
        .await;

        assert_eq!(
            home_uri_for_job(&db, "j-session").await.unwrap().as_deref(),
            Some("cairn://p/cairn/thread-ux"),
            "a thread's session is the thread address itself"
        );
        assert_eq!(
            home_uri_for_job(&db, "j-task").await.unwrap().as_deref(),
            Some("cairn://p/cairn/thread-ux/task/post-migration"),
            "a task the session spawned nests beneath the thread, and is never the thread itself"
        );

        // Migration 0157 re-pointed the pre-cutover thread-issue's jobs at the
        // thread, so a task can carry the thread's id directly. It is still a
        // task, and must not resolve to the session's own home URI.
        exec(
            &db,
            "INSERT INTO jobs (id, thread_id, parent_job_id, project_id, status, uri_segment,
                               node_name, created_at, updated_at)
             VALUES ('j-migrated', 't', 'j-session', 'p', 'complete', 'survey-agent',
                     'Survey', 3, 3)",
        )
        .await;
        assert_eq!(
            home_uri_for_job(&db, "j-migrated")
                .await
                .unwrap()
                .as_deref(),
            Some("cairn://p/cairn/thread-ux/task/survey-agent"),
        );
    }

    #[tokio::test]
    async fn empty_execution_yields_no_indicators() {
        let db = test_db().await;
        let indicators = node_status_indicators(&db, NodeStatusScope::Execution("missing"))
            .await
            .unwrap();
        assert!(indicators.is_empty());
    }

    // ── Issue-level (project-scoped) status indicators ──────────────────────

    /// Seeds project `p`, an `active` issue `i`, and its running execution `e`
    /// (seq 1). Individual tests add the jobs/turns/PR they exercise.
    async fn seed_active_issue(db: &LocalDb) {
        exec(
            db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'T', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i', 'p', 1, 'T', 'active', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e', 'r', 'i', 'p', 'running', 1, 1)",
        )
        .await;
    }

    #[tokio::test]
    async fn issue_with_running_job_rolls_up_running_with_its_agent() {
        let db = test_db().await;
        seed_active_issue(&db).await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-run', 's-run', 'j-run', 1, 'running', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment)
             VALUES ('j-run', 'e', 'i', 'p', 'Builder', 'agent-1', 'running', 1, 1, 'run')",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        assert_eq!(ind.issue_id, "i");
        assert_eq!(ind.activity, NodeActivity::Running);
        assert!(
            !ind.checks_running,
            "pure SQL query leaves checks_running false"
        );
        assert!(ind.pr.is_none());
        assert_eq!(ind.job_ids, vec!["j-run".to_string()]);
        assert_eq!(ind.agents.len(), 1);
        assert_eq!(ind.agents[0].job_id, "j-run");
        assert_eq!(ind.agents[0].node_name.as_deref(), Some("Builder"));
        assert_eq!(ind.agents[0].agent_config_id.as_deref(), Some("agent-1"));
        assert_eq!(ind.agents[0].activity, NodeActivity::Running);
    }

    #[tokio::test]
    async fn issue_reseeded_session_uses_current_turn_not_highest_sequence() {
        let db = test_db().await;
        seed_active_issue(&db).await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-old', 's-old', 'j-reseed', 20, 'complete', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-new', 's-new', 'j-reseed', 1, 'running', 'follow_up', 2, 2)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment, current_turn_id)
             VALUES ('j-reseed', 'e', 'i', 'p', 'Builder', 'agent-1', 'running', 1, 1, 'reseed', 't-new')",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        assert_eq!(ind.activity, NodeActivity::Running);
        assert_eq!(ind.agents.len(), 1);
        assert_eq!(ind.agents[0].job_id, "j-reseed");
        assert_eq!(ind.agents[0].activity, NodeActivity::Running);
    }

    #[tokio::test]
    async fn issue_awaiting_input_outranks_a_running_sibling_job() {
        let db = test_db().await;
        seed_active_issue(&db).await;
        // A running sibling whose turn started first but later emitted streamed output.
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-run', 's-run', 'j-run', 1, 'running', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, current_turn_id)
             VALUES ('j-run', 'e', 'i', 'p', 'Run', 'running', 1, 1, 'run', 't-run')",
        )
        .await;
        // A later-started job whose head turn yielded on a pending prompt.
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
             VALUES ('t-prompt', 's-prompt', 'j-prompt', 1, 'yielded', 'user_input', 'initial', 2, 2)",
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
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-running', 'j-run', 'i', 1, 5)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO message_streams(id, run_id, turn_id, backend, sequence, status, created_at, updated_at)
             VALUES ('stream-running', 'run-running', 't-run', 'codex', 1, 'open', 1, 5)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-prompt', 'j-prompt', 'i', 2, 2)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO prompts(id, run_id, turn_id, questions, response, created_at)
             VALUES ('pr-1', 'run-prompt', 't-prompt', '[]', NULL, 1)",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        assert_eq!(ind.activity, NodeActivity::AwaitingInput);
        // Both live jobs surface as agents (neither is idle), newest activity first.
        assert_eq!(ind.agents.len(), 2);
        assert_eq!(ind.agents[0].job_id, "j-run");
        assert_eq!(ind.agents[0].activity_updated_at, 5);
        assert_eq!(ind.agents[1].job_id, "j-prompt");
        assert_eq!(ind.agents[1].activity_updated_at, 2);
        assert_eq!(ind.job_ids.len(), 2);
    }

    #[tokio::test]
    async fn issue_with_open_pr_reports_cached_pr_state() {
        let db = test_db().await;
        seed_active_issue(&db).await;
        // The builder job's head turn is complete → the issue itself reads idle.
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-pr', 's-pr', 'j-pr', 1, 'complete', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-pr', 'e', 'i', 'p', 'Builder', 'complete', 1, 1, 'pr')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, github_pr_number, github_pr_url, github_state, github_review, github_mergeable, checks_status, is_local, opened_at, updated_at)
             VALUES ('mr-1', 'j-pr', 'p', 'i', 'PR', 'feature', 'main', 'open', 42, 'https://x/42', 'OPEN', 'APPROVED', 'MERGEABLE', 'passing', 0, 1, 5)",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        assert_eq!(ind.activity, NodeActivity::Idle);
        let pr = ind.pr.as_ref().expect("open PR reported");
        assert_eq!(pr.status, "open");
        assert_eq!(pr.pr_number, Some(42));
        assert_eq!(pr.pr_url.as_deref(), Some("https://x/42"));
        assert_eq!(pr.github_state.as_deref(), Some("OPEN"));
        assert_eq!(pr.review_decision.as_deref(), Some("APPROVED"));
        assert_eq!(pr.mergeable.as_deref(), Some("MERGEABLE"));
        assert_eq!(pr.checks_status.as_deref(), Some("passing"));
        assert!(!pr.is_local);
    }

    #[tokio::test]
    async fn pr_is_scoped_to_the_current_execution_not_an_older_one() {
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
        // e1 (older) produced an OPEN PR; e2 (highest seq = current) has none yet.
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e1', 'r', 'i', 'p', 'complete', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e2', 'r', 'i', 'p', 'running', 1, 2)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t1', 's1', 'j1', 1, 'complete', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j1', 'e1', 'i', 'p', 'Builder', 'complete', 1, 1, 'j1')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, github_pr_number, github_state, is_local, opened_at, updated_at)
             VALUES ('mr-old', 'j1', 'p', 'i', 'Old PR', 'feature-1', 'main', 'open', 7, 'OPEN', 0, 1, 1)",
        )
        .await;
        // Current execution's job, no PR row.
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t2', 's2', 'j2', 1, 'complete', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j2', 'e2', 'i', 'p', 'Builder', 'complete', 1, 1, 'j2')",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        // Activity/agents come from the current execution (e2), and its job has no
        // PR — the older execution's open PR must NOT leak onto the issue row.
        assert_eq!(ind.job_ids, vec!["j2".to_string()]);
        assert_eq!(ind.activity, NodeActivity::Idle);
        assert!(
            ind.pr.is_none(),
            "a stale open PR from an older execution must not be reported"
        );
    }

    #[tokio::test]
    async fn action_run_owned_current_pr_is_reported() {
        // The legacy first-class PR-node shape (migration 0019): the
        // `merge_requests.job_id` holds an `action_runs.id`, not a `jobs.id`. The
        // action run's `parent_job_id` is the current-execution builder job, so
        // the PR still belongs to the current execution and must be reported.
        let db = test_db().await;
        seed_active_issue(&db).await;
        exec(
            &db,
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
             VALUES ('t-current', 's-current', 'j-current', 1, 'complete', 'initial', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j-current', 'e', 'i', 'p', 'Builder', 'complete', 1, 1, 'current')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, project_id, status, parent_job_id, created_at)
             VALUES ('ar-pr', 'e', 'pr-node', 'pr-cfg', 'p', 'complete', 'j-current', 1)",
        )
        .await;
        // PR row owned by the ACTION RUN id, not the job id.
        exec(
            &db,
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, github_pr_number, github_state, is_local, opened_at, updated_at)
             VALUES ('mr-ar', 'ar-pr', 'p', 'i', 'AR PR', 'feature', 'main', 'open', 99, 'OPEN', 0, 1, 1)",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        let pr = ind
            .pr
            .as_ref()
            .expect("action-run-owned current PR must be reported");
        assert_eq!(pr.status, "open");
        assert_eq!(pr.pr_number, Some(99));
        assert_eq!(pr.github_state.as_deref(), Some("OPEN"));
    }

    #[tokio::test]
    async fn artifact_waiting_excludes_pr_output_and_tracks_latest_non_pr_version() {
        let db = test_db().await;
        seed_active_issue(&db).await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
             VALUES ('j', 'e', 'i', 'p', 'Builder', 'blocked', 1, 1, 'builder')",
        )
        .await;
        exec(
            &db,
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES ('pr-artifact', 'j', 'create-pr', 1, '{}', 1, 'create-pr', 0, 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, checks_status, is_local, opened_at, updated_at)
             VALUES ('mr', 'j', 'p', 'i', 'PR', 'feature', 'main', 'open', 'PENDING', 0, 1, 1)",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert!(
            !indicators[0].artifact_waiting,
            "PR output is not a generic artifact wait"
        );

        exec(
            &db,
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES ('plan-v1', 'j', 'plan', 1, '{}', 1, 'plan', 0, 2, 2)",
        )
        .await;
        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert!(
            indicators[0].artifact_waiting,
            "unconfirmed non-PR artifact is waiting"
        );

        exec(
            &db,
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES ('plan-v2', 'j', 'plan', 1, '{}', 2, 'plan', 1, 3, 3)",
        )
        .await;
        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert!(
            !indicators[0].artifact_waiting,
            "confirmed latest version supersedes old wait"
        );
    }

    #[tokio::test]
    async fn active_issue_with_no_jobs_is_present_and_idle() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'T', '/tmp/r', 1, 1)",
        )
        .await;
        // Active issue, but no execution and no jobs at all.
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i', 'p', 1, 'T', 'active', 1, 1)",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        assert_eq!(indicators.len(), 1);
        let ind = &indicators[0];
        assert_eq!(ind.issue_id, "i");
        assert_eq!(ind.activity, NodeActivity::Idle);
        assert!(ind.agents.is_empty());
        assert!(ind.job_ids.is_empty());
        assert!(ind.pr.is_none());
        assert!(!ind.checks_running);
        assert!(!ind.artifact_waiting);
    }

    #[tokio::test]
    async fn only_active_and_waiting_issues_of_the_project_are_included() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'T', '/tmp/r', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p2', 'default', 'U', 'U', '/tmp/r2', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i-active', 'p', 1, 'A', 'active', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i-waiting', 'p', 2, 'W', 'waiting', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i-backlog', 'p', 3, 'B', 'backlog', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('i-other', 'p2', 1, 'O', 'active', 1, 1)",
        )
        .await;

        let indicators = issue_status_indicators(&db, "p").await.unwrap();
        let ids: HashMap<String, ()> = indicators
            .iter()
            .map(|ind| (ind.issue_id.clone(), ()))
            .collect();
        assert_eq!(ids.len(), 2, "only the project's active + waiting issues");
        assert!(ids.contains_key("i-active"));
        assert!(ids.contains_key("i-waiting"));
    }
}
