//! Database query helpers for jobs and execution snapshots.

pub use cairn_db::lineage::home_uri_for_job;
pub(crate) use cairn_db::lineage::home_uri_for_job_conn;

use std::collections::{HashMap, HashSet};

use crate::config::slugify_resource_segment;
use crate::db_records::{db_job_from_row, DbJob, JOB_COLUMNS};
use crate::error::CairnError;
use crate::models::{ExecutionSnapshot, Job, RecipeNode, RecipeSnapshot, Turn};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
use cairn_db::turso::{params, Value};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ImplementationInfo {
    pub job_id: String,
    pub branch: String,
    pub project_id: String,
}

impl NodeStatusIndicator {
    pub fn job_id(&self) -> &str {
        &self.job_id
    }
    pub fn activity(&self) -> NodeActivity {
        self.activity
    }
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

/// What a job that has not started yet is waiting on.
pub(crate) enum PendingDispatch<'a> {
    /// A task its parent spawned (`jobs.parent_tool_use_id` is set). It
    /// launches on its own once prepared; nothing has to settle first.
    Delegated,
    /// A recipe child that dispatches when its parent node settles.
    AfterParent(&'a str),
    /// A recipe node that dispatches when its recipe dependencies settle.
    AfterRecipe,
}

pub(crate) fn pending_dispatch_description(pending: PendingDispatch<'_>) -> String {
    match pending {
        PendingDispatch::Delegated => "starting: spawned by its parent and preparing to launch; it starts on its own, nothing to wait for".to_string(),
        PendingDispatch::AfterParent(parent) => {
            format!("pending, not started; dispatches when `{parent}` settles")
        }
        PendingDispatch::AfterRecipe => {
            "pending, not started; dispatches when its recipe dependencies settle".to_string()
        }
    }
}

/// A job claimed `running` before its first run exists is setting itself up;
/// without this line its read shows a bare running icon and looks hung.
pub(crate) const PREPARING_DESCRIPTION: &str =
    "preparing: claimed and setting up its branch and worktree; no run yet.";

fn address_root_predicate(alias: &str) -> String {
    format!("({alias}.parent_job_id IS NULL OR {alias}.agent_config_id = 'workflow')")
}

/// Resolve the job a node coordinate addresses — the inverse of
/// [`home_uri_for_job_conn`], and owner-aware in exactly the same way.
///
/// `task_name: None` names the node (or thread session) itself; `Some` names a
/// sub-agent task beneath it. `(0, 0, name)` is the reserved thread coordinate
/// that [`cairn_uri::NodeAddress`] defines; every other coordinate names
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
    conn: &cairn_db::storage::TrackedConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> Result<Option<String>, DbError> {
    if let cairn_uri::NodeAddress::Thread { name } =
        cairn_uri::NodeAddress::new(number, exec_seq, node_id)
    {
        return match task_name {
            None => crate::threads::session_job_id_by_name_conn(conn, project_key, name).await,
            Some(task) => {
                crate::threads::task_job_id_by_name_conn(conn, project_key, name, task).await
            }
        };
    }

    let key = cairn_uri::canonical_project(project_key);
    let node_root = address_root_predicate("j");
    let task_root = address_root_predicate("parent");
    let mut rows = match task_name {
        None => {
            conn.query(
                &format!(
                    "SELECT j.id
                 FROM jobs j
                 JOIN issues i ON j.issue_id = i.id
                 JOIN projects p ON i.project_id = p.id
                 JOIN executions e ON j.execution_id = e.id
                 WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                   -- Top-level nodes have no parent; a workflow is a child job
                   -- (for the delegation tree) yet is addressable as a node by
                   -- its segment, so its own sub-resources resolve.
                   AND {node_root}
                   AND j.uri_segment = ?4
                 LIMIT 1"
                ),
                params![key.as_str(), number, exec_seq, node_id],
            )
            .await?
        }
        Some(task) => {
            conn.query(
                &format!(
                    "SELECT child.id
                 FROM jobs parent
                 JOIN jobs child ON child.parent_job_id = parent.id
                 JOIN issues i ON parent.issue_id = i.id
                 JOIN projects p ON i.project_id = p.id
                 JOIN executions e ON parent.execution_id = e.id
                 WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                   AND {task_root} AND parent.uri_segment = ?4
                   AND child.uri_segment = ?5
                 LIMIT 1"
                ),
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

/// The tabs a job's view offers, and the one it opens on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobTabs {
    available_tabs: Vec<String>,
    initial_tab: String,
}

impl JobTabs {
    /// The tab rule. An artifact that feeds a single downstream action is
    /// surfaced on that action's node rather than here, so such a job shows
    /// chat alone, as does a job with no artifact. A running job opens on chat;
    /// a settled one opens on its artifact when it shows one.
    fn for_job(status: &str, has_artifact: bool, has_downstream_action: bool) -> Self {
        let available_tabs = if has_downstream_action || !has_artifact {
            vec!["chat".to_string()]
        } else {
            vec!["chat".to_string(), "artifact".to_string()]
        };
        let initial_tab = if status != "running" && available_tabs.len() > 1 {
            "artifact"
        } else {
            "chat"
        };
        Self {
            available_tabs,
            initial_tab: initial_tab.to_string(),
        }
    }
}

/// What a job's DTO carries beyond its own row: its tabs and its execution's
/// seq.
struct JobView {
    tabs: JobTabs,
    exec_seq: Option<i32>,
}

/// Every fact the tab rule and the exec seq need for `n` jobs, in one
/// statement.
///
/// It must never select `executions.snapshot`. A snapshot runs to hundreds of
/// kilobytes, and reading it on every job of an issue cost seconds per job
/// under load (cairn/5206); only the jobs with an artifact consult the recipe
/// graph, through [`recipe_graphs_sql`].
fn job_view_rows_sql(n: usize) -> String {
    format!(
        "SELECT j.id, j.status, j.recipe_node_id, j.execution_id, e.seq,
                CASE WHEN EXISTS (SELECT 1 FROM artifacts a WHERE a.job_id = j.id)
                     THEN 1 ELSE 0 END
           FROM jobs j
           LEFT JOIN executions e ON e.id = j.execution_id
          WHERE j.id IN ({})",
        placeholders(n)
    )
}

/// The recipe graph of `n` executions, extracted in SQL. The graph averages a
/// few kilobytes of a snapshot that averages well over a hundred, so the rest
/// of the snapshot is neither returned nor parsed.
fn recipe_graphs_sql(n: usize) -> String {
    format!(
        "SELECT id, json_extract(snapshot, '$.recipe')
           FROM executions
          WHERE id IN ({}) AND snapshot IS NOT NULL",
        placeholders(n)
    )
}

struct JobViewRow {
    id: String,
    status: String,
    recipe_node_id: Option<String>,
    execution_id: Option<String>,
    exec_seq: Option<i32>,
    has_artifact: bool,
}

/// The tabs and exec seq of each of `job_ids` that exists, in a fixed number
/// of statements however many jobs an issue has: one per [`IN_LIST_CHUNK`]
/// jobs, then one recipe-graph read per execution that one of them needs.
async fn job_views_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job_ids: &[String],
) -> Result<HashMap<String, JobView>, DbError> {
    let mut job_rows = Vec::with_capacity(job_ids.len());
    for chunk in job_ids.chunks(IN_LIST_CHUNK) {
        let mut rows = conn
            .query(&job_view_rows_sql(chunk.len()), bound_text(chunk))
            .await?;
        while let Some(row) = rows.next().await? {
            job_rows.push(JobViewRow {
                id: row.text(0)?,
                status: row.text(1)?,
                recipe_node_id: row.opt_text(2)?,
                execution_id: row.opt_text(3)?,
                exec_seq: row.opt_i64(4)?.map(|seq| seq as i32),
                has_artifact: row.i64(5)? != 0,
            });
        }
    }

    // Only a job with an artifact can have that artifact surfaced on a
    // downstream action, so only its execution's graph is read, once however
    // many of the execution's jobs need it.
    let mut graph_execution_ids: Vec<String> = job_rows
        .iter()
        .filter(|row| row.has_artifact && row.recipe_node_id.is_some())
        .filter_map(|row| row.execution_id.clone())
        .collect();
    graph_execution_ids.sort();
    graph_execution_ids.dedup();
    let mut graphs = HashMap::<String, RecipeSnapshot>::new();
    for chunk in graph_execution_ids.chunks(IN_LIST_CHUNK) {
        let mut rows = conn
            .query(&recipe_graphs_sql(chunk.len()), bound_text(chunk))
            .await?;
        while let Some(row) = rows.next().await? {
            let execution_id = row.text(0)?;
            // A graph that does not parse reads as feeding no action, so the
            // job keeps its own artifact tab.
            if let Some(graph) = row
                .opt_text(1)?
                .and_then(|json| serde_json::from_str::<RecipeSnapshot>(&json).ok())
            {
                graphs.insert(execution_id, graph);
            }
        }
    }

    Ok(job_rows
        .into_iter()
        .map(|row| {
            let graph = row.execution_id.as_ref().and_then(|id| graphs.get(id));
            let has_downstream_action = match (row.recipe_node_id.as_deref(), graph) {
                (Some(node_id), Some(graph)) if row.has_artifact => {
                    has_single_downstream_action(graph, node_id)
                }
                _ => false,
            };
            let view = JobView {
                tabs: JobTabs::for_job(&row.status, row.has_artifact, has_downstream_action),
                exec_seq: row.exec_seq,
            };
            (row.id, view)
        })
        .collect())
}

fn bound_text(values: &[String]) -> Vec<Value> {
    values
        .iter()
        .map(|value| Value::Text(value.clone()))
        .collect()
}

async fn job_views(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<HashMap<String, JobView>, CairnError> {
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move { job_views_conn(conn, &job_ids).await })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn compute_tabs_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<HashMap<String, JobTabs>, CairnError> {
    Ok(job_views(db, job_ids)
        .await?
        .into_iter()
        .map(|(id, view)| (id, view.tabs))
        .collect())
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
    let mut views = job_views(db, &ids).await?;

    db_jobs
        .into_iter()
        .map(|db_job| {
            let id = db_job.id.clone();
            let mut job = Job::try_from(db_job).map_err(CairnError::Internal)?;
            if let Some(view) = views.remove(&id) {
                apply_view(&mut job, view);
            }
            Ok(job)
        })
        .collect()
}

fn apply_view(job: &mut Job, view: JobView) {
    job.available_tabs = view.tabs.available_tabs;
    job.initial_tab = view.tabs.initial_tab;
    job.exec_seq = view.exec_seq;
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
    if let Some(view) = job_views(db, std::slice::from_ref(&job.id))
        .await?
        .remove(&job.id)
    {
        apply_view(job, view);
    }
    Ok(())
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
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatusIndicator {
    job_id: String,
    activity: NodeActivity,
    head_turn: Option<Turn>,
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

// Shared SQL facts for batched activity reads. Keep the fallback tied to job
// identity, not the newest execution or maximum sequence across sessions.
pub(crate) const JOB_HEAD_JOIN_SQL: &str = "LEFT JOIN turns ht ON ht.id = COALESCE(
    j.current_turn_id,
    (SELECT t.id FROM turns t WHERE t.job_id = j.id
     ORDER BY t.created_at DESC, t.sequence DESC LIMIT 1)
)";
pub(crate) const JOB_PENDING_PROMPT_SQL: &str = "EXISTS (
    SELECT 1 FROM prompts p WHERE p.turn_id = j.current_turn_id
    AND p.response IS NULL AND p.withdrawn_at IS NULL
)";
pub(crate) const JOB_PENDING_PERMISSION_SQL: &str = "EXISTS (
    SELECT 1 FROM permission_requests pr
    LEFT JOIN runs r ON pr.run_id = r.id
    WHERE COALESCE(pr.job_id, r.job_id) = j.id AND pr.status = 'pending'
)";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveAgentEntry {
    pub job_id: String,
    pub project_id: String,
    pub project_key: String,
    pub team_id: Option<String>,
    pub uri: String,
    pub owner_title: String,
    pub issue_id: Option<String>,
    pub issue_number: Option<i32>,
    pub thread_id: Option<String>,
    pub thread_name: Option<String>,
    pub execution_id: Option<String>,
    pub exec_seq: Option<i32>,
    pub node: String,
    pub task: Option<String>,
    pub session_id: Option<String>,
    pub activity: NodeActivity,
}

/// One compact snapshot per runner, one batched statement per open database.
/// All executions participate. No owner-list or per-job transcript reads.
pub async fn live_agent_census(
    dbs: &crate::db::DbState,
) -> Result<Vec<LiveAgentEntry>, CairnError> {
    let mut out = Vec::new();
    for (team_id, db) in dbs.navigation_dbs().await {
        match live_agent_census_db(&db, team_id.clone()).await {
            Ok(rows) => {
                dbs.record_navigation_failure("census", team_id.as_deref(), None)
                    .await;
                out.extend(rows);
            }
            Err(error) => {
                dbs.record_navigation_failure(
                    "census",
                    team_id.as_deref(),
                    Some(error.to_string()),
                )
                .await;
            }
        }
    }
    out.sort_by(|a, b| {
        a.team_id
            .cmp(&b.team_id)
            .then(a.project_id.cmp(&b.project_id))
            .then(a.uri.cmp(&b.uri))
            .then(a.job_id.cmp(&b.job_id))
    });
    Ok(out)
}

// Scan durable activity sources once, not once per historical job. Only live
// candidates pay for head resolution and ancestor traversal; ancestors may be
// terminal because their still-live children retain their canonical coordinates.
pub(crate) fn live_agent_census_sql() -> String {
    let ancestor_root = address_root_predicate("ancestor");
    let root = address_root_predicate("root");
    format!(
        "WITH RECURSIVE
        open_prompts AS MATERIALIZED (
            SELECT DISTINCT turn_id FROM prompts
             WHERE response IS NULL AND withdrawn_at IS NULL
        ),
        pending_permissions AS MATERIALIZED (
            SELECT DISTINCT COALESCE(pr.job_id, r.job_id) AS job_id
              FROM permission_requests pr LEFT JOIN runs r ON r.id = pr.run_id
             WHERE pr.status = 'pending'
        ),
        candidate_ids AS (
            SELECT job_id FROM turns WHERE state IN ('pending', 'running')
            UNION
            SELECT ht.job_id FROM open_prompts op JOIN turns ht ON ht.id = op.turn_id
            UNION
            SELECT job_id FROM pending_permissions
        ),
        activity AS MATERIALIZED (
            SELECT j.id, ht.state AS head_state,
                   CASE WHEN j.current_turn_id IN (SELECT turn_id FROM open_prompts) THEN 1 ELSE 0 END AS prompt,
                   CASE WHEN j.id IN (SELECT job_id FROM pending_permissions) THEN 1 ELSE 0 END AS permission,
                   ht.session_id AS head_session
              FROM candidate_ids c JOIN jobs j ON j.id = c.job_id
              {JOB_HEAD_JOIN_SQL}
             WHERE j.status NOT IN ('complete', 'cancelled', 'failed')
               AND COALESCE(j.agent_config_id, '') NOT IN ('service', 'action')
        ),
        homes(id, ancestor_id, path, head_state, prompt, permission, head_session) AS (
            SELECT id, id, '', head_state, prompt, permission, head_session FROM activity
             WHERE head_state IN ('pending', 'running') OR prompt OR permission
            UNION ALL
            SELECT homes.id, ancestor.parent_job_id,
                   '/task/' || ancestor.uri_segment || homes.path,
                   homes.head_state, homes.prompt, homes.permission, homes.head_session
              FROM homes JOIN jobs ancestor ON ancestor.id = homes.ancestor_id
             WHERE NOT COALESCE({ancestor_root}, 0) AND ancestor.uri_segment IS NOT NULL
        )
        SELECT j.id, p.id, p.key, i.id, i.number, t.id, t.name,
               COALESCE(t.name, i.title, j.node_name, j.uri_segment),
               root.execution_id, e.seq, root.uri_segment, homes.path,
               COALESCE(j.current_session_id, homes.head_session),
               homes.head_state, homes.prompt, homes.permission
          FROM homes JOIN jobs j ON j.id = homes.id
          JOIN jobs root ON root.id = homes.ancestor_id
          LEFT JOIN issues i ON i.id = COALESCE(j.issue_id, root.issue_id)
          LEFT JOIN threads t ON t.id = COALESCE(j.thread_id, root.thread_id)
          JOIN projects p ON p.id = COALESCE(i.project_id, t.project_id, j.project_id, root.project_id)
          LEFT JOIN executions e ON e.id = root.execution_id
         WHERE {root} AND p.id IN (SELECT value FROM json_each(?1))
         ORDER BY p.id, j.id"
    )
}

async fn live_agent_census_db(
    db: &LocalDb,
    team_id: Option<String>,
) -> Result<Vec<LiveAgentEntry>, CairnError> {
    let projects = crate::navigation::project_authority(db, team_id.as_deref()).await?;
    let projects =
        serde_json::to_string(&projects).map_err(|e| CairnError::Internal(e.to_string()))?;
    let sql = live_agent_census_sql();
    db.query_all(sql, params![projects.as_str()], move |row| {
        use cairn_uri::{build_job_base_uri, build_thread_task_uri, build_thread_uri};
        let project_key = row.text(2)?;
        let issue_number = row.opt_i64(4)?.map(|n| n as i32);
        let thread_name = row.opt_text(6)?;
        let node_segment = row.opt_text(10)?;
        let path = row.text(11)?;
        let exec_seq = row.opt_i64(9)?.map(|n| n as i32);
        let base = if let Some(name) = thread_name.as_deref() {
            if node_segment.as_deref() == Some("thread") {
                Some(build_thread_uri(&project_key, name))
            } else {
                node_segment
                    .as_deref()
                    .map(|seg| build_thread_task_uri(&project_key, name, seg))
            }
        } else {
            match (issue_number, exec_seq, node_segment.as_deref()) {
                (Some(number), Some(seq), Some(seg)) => {
                    Some(build_job_base_uri(&project_key, number, seq, seg, None))
                }
                _ => None,
            }
        };
        let Some(base) = base else {
            return Ok(None);
        };
        Ok(Some(LiveAgentEntry {
            job_id: row.text(0)?,
            project_id: row.text(1)?,
            project_key,
            team_id: team_id.clone(),
            uri: format!("{base}{path}"),
            owner_title: row.text(7)?,
            issue_id: row.opt_text(3)?,
            issue_number,
            thread_id: row.opt_text(5)?,
            node: thread_name.clone().or(node_segment).unwrap_or_default(),
            thread_name,
            execution_id: row.opt_text(8)?,
            exec_seq,
            task: path.strip_prefix("/task/").map(str::to_string),
            session_id: row.opt_text(12)?,
            activity: derive_node_activity(
                row.opt_text(13)?.as_deref(),
                row.i64(14)? != 0,
                row.i64(15)? != 0,
            ),
        }))
    })
    .await
    .map(|rows| rows.into_iter().flatten().collect())
    .map_err(Into::into)
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
                        ht.id, ht.session_id, ht.run_id, ht.job_id, ht.sequence,
                        ht.predecessor_id, ht.state, ht.yield_reason, ht.end_reason,
                        ht.start_reason, ht.created_at, ht.started_at, ht.ended_at, ht.updated_at,
                        j.id,
                        {JOB_PENDING_PROMPT_SQL} AS has_pending_prompt,
                        {JOB_PENDING_PERMISSION_SQL} AS has_pending_permission
                     FROM jobs j
                     {JOB_HEAD_JOIN_SQL}
                     WHERE {predicate}"
                    ),
                    params![owner_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let head_turn = if row.opt_text(0)?.is_some() {
                    Some(crate::turns::queries::turn_from_row(&row)?)
                } else {
                    None
                };
                let job_id = row.text(14)?;
                let has_pending_prompt = row.i64(15)? != 0;
                let has_pending_permission = row.i64(16)? != 0;
                out.push(NodeStatusIndicator {
                    job_id,
                    activity: derive_node_activity(
                        head_turn
                            .as_ref()
                            .map(|turn| turn.state.to_string())
                            .as_deref(),
                        has_pending_prompt,
                        has_pending_permission,
                    ),
                    head_turn,
                });
            }
            Ok::<Vec<NodeStatusIndicator>, DbError>(out)
        })
    })
    .await
    .map_err(CairnError::from)
}

/// The published thread rollup for one project.
///
/// The SQL behind it joins every thread's jobs and evaluates a correlated
/// head-turn, prompt and permission lookup per job, so it costs the same whether
/// one thread moved or none did. It is served from the orchestrator's cadenced
/// cache: a read returns the last published snapshot at once, and the cache
/// rebuilds at most once per [`STATUS_ROLLUP_FLOOR`] and announces a changed
/// answer. A read waits only on a cold start.
///
/// [`STATUS_ROLLUP_FLOOR`]: crate::orchestrator::generation_cache::STATUS_ROLLUP_FLOOR
pub async fn published_thread_status_indicators(
    orch: &crate::orchestrator::Orchestrator,
    project_id: &str,
) -> Result<Vec<ThreadStatusIndicator>, CairnError> {
    orch.thread_status_cache
        .get(project_id)
        .await
        .map(|snapshot| snapshot.indicators.clone())
        .map_err(CairnError::Internal)
}

/// Rebuild one project's thread rollup from its owning database: the thread
/// status cache's rebuild, and nothing else's.
pub(crate) async fn rebuild_thread_status(
    dbs: &crate::db::DbState,
    project_id: &str,
) -> Result<ThreadStatusSnapshot, String> {
    let db = crate::execution::routing::owning_db_for_project(dbs, project_id)
        .await
        .map_err(|error| error.to_string())?;
    thread_status_snapshot(&db, &dbs.local, project_id)
        .await
        .map_err(|error| error.to_string())
}

/// Everything one project's thread rows show, plus the evidence needed to decide
/// which later writes could change it.
///
/// The two halves are materialized together because they are read together and a
/// half-fenced projection is not fenced: while the unread count was recomputed on
/// every read, a warm activity snapshot saved nothing a sidebar poll could feel.
///
/// Compared whole after each rebuild, so an unchanged rollup is not announced.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ThreadStatusSnapshot {
    pub(crate) indicators: Vec<ThreadStatusIndicator>,
    /// Every job the rollup above actually read.
    ///
    /// This is the projection telling its invalidator what it depends on. A
    /// `turns`, `runs`, or `events` write names the job it belongs to, and a job
    /// outside this set is an issue node's — it cannot move any row in
    /// `indicators`, so it must not be allowed to cold this snapshot. In the
    /// sample workspace behind CAIRN-4331 that distinction is 121 jobs against
    /// 15,000, and roughly four out of five transcript writes.
    ///
    /// Replaced wholesale on every rebuild, so it cannot drift: a job that leaves
    /// a thread leaves the set at the next rebuild, and a job that joins one
    /// announces itself through its own `jobs` write before it can produce any.
    pub(crate) thread_job_ids: HashSet<String>,
}

/// Request authority stays private even when the threads being projected live
/// in a team replica. Contributions are routed at filing; count each open card
/// once per destination rather than once per rationale or member identity.
async fn thread_pending_request_counts(
    db: &LocalDb,
    private_db: &LocalDb,
    project_id: &str,
) -> Result<HashMap<String, i64>, CairnError> {
    let pending = private_db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT rr.wake_destination, COUNT(DISTINCT r.cohort_id)
             FROM request_requesters rr JOIN requests r ON r.id=rr.request_id
             JOIN request_cohorts c ON c.id=r.cohort_id
             WHERE rr.active=1 AND r.state='open' AND c.state='open'
             GROUP BY rr.wake_destination",
                        (),
                    )
                    .await?;
                let mut counts = HashMap::new();
                while let Some(row) = rows.next().await? {
                    counts.insert(row.text(0)?, row.i64(1)?);
                }
                Ok(counts)
            })
        })
        .await?;
    let project_id = project_id.to_string();
    db.read(move |conn| {
        let project_id = project_id.clone();
        let pending = pending.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT t.id, p.key, t.name FROM threads t JOIN projects p ON p.id = t.project_id
                 WHERE p.id = ?1", params![project_id.as_str()],
            ).await?;
            let mut counts = HashMap::new();
            while let Some(row) = rows.next().await? {
                let uri = cairn_uri::build_thread_uri(&row.text(1)?, &row.text(2)?);
                counts.insert(row.text(0)?, pending.get(&uri).copied().unwrap_or_default());
            }
            Ok(counts)
        })
    })
    .await
    .map_err(CairnError::Storage)
}

async fn thread_status_snapshot(
    db: &LocalDb,
    private_db: &LocalDb,
    project_id: &str,
) -> Result<ThreadStatusSnapshot, CairnError> {
    let activity = thread_activity_rows(db, project_id).await?;
    let unread = thread_unread_counts(db, project_id).await?;
    let pending_tenets = thread_pending_tenet_counts(db, project_id).await?;
    let pending_requests = thread_pending_request_counts(db, private_db, project_id).await?;
    Ok(ThreadStatusSnapshot {
        indicators: activity
            .rows
            .iter()
            .map(|row| {
                let unread = unread.get(&row.thread_id).copied().unwrap_or_default();
                ThreadStatusIndicator {
                    thread_id: row.thread_id.clone(),
                    activity: row.activity,
                    unread_count: unread.count,
                    latest_event_rowid: unread.latest_event_rowid,
                    pending_request_count: pending_requests
                        .get(&row.thread_id)
                        .copied()
                        .unwrap_or_default(),
                    pending_tenet_count: pending_tenets
                        .get(&row.thread_id)
                        .copied()
                        .unwrap_or_default(),
                }
            })
            .collect(),
        thread_job_ids: activity.job_ids,
    })
}

/// Everything one thread row shows about itself beyond its name: whether work is
/// happening under it right now, how many of its tenets await the operator's
/// verdict, and how much of its transcript this operator has not seen. They are
/// rendered as mutually exclusive trailing states, but they are computed
/// independently and all travel on one row.
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
    /// This thread's tenets still awaiting the operator's verdict (status
    /// `proposed`). Work filed behind a tenet is held on exactly these, so a
    /// nonzero count is the operator's own blocker and outranks the unread count.
    pending_tenet_count: i64,
    /// Distinct open operator-request cohorts contributed by this thread.
    pending_request_count: i64,
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
        j.id AS job_id,
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
               AND p.withdrawn_at IS NULL
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

/// One project's thread activity, and the jobs it was derived from.
pub struct ThreadActivity {
    pub rows: Vec<ThreadActivityRow>,
    /// Every job the statement visited, whatever its state. Collected here
    /// rather than re-queried because the rollup already joins exactly this set,
    /// and because a dependency list assembled by a second statement could
    /// disagree with the one the answer was actually built from.
    pub job_ids: HashSet<String>,
}

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
) -> Result<ThreadActivity, CairnError> {
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
            let mut job_ids: HashSet<String> = HashSet::new();
            while let Some(row) = rows.next().await? {
                let thread_id = row.text(0)?;
                if let Some(job_id) = row.opt_text(1)? {
                    job_ids.insert(job_id);
                }
                let activity = derive_node_activity(
                    row.opt_text(2)?.as_deref(),
                    row.i64(3)? != 0,
                    row.i64(4)? != 0,
                );
                let entry = by_thread.entry(thread_id).or_insert(NodeActivity::Idle);
                *entry = rollup_activity([*entry, activity]);
            }
            Ok::<ThreadActivity, DbError>(ThreadActivity {
                rows: by_thread
                    .into_iter()
                    .map(|(thread_id, activity)| ThreadActivityRow {
                        thread_id,
                        activity,
                    })
                    .collect(),
                job_ids,
            })
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
/// # Why both halves walk one session backwards
///
/// Both facts this returns are about the NEWEST end of a transcript, and the only
/// way to reach that end cheaply is to walk the index down from it. Turso does not
/// use a rowid range as an index seek bound, so the obvious spellings — `WHERE
/// e.rowid > watermark ... LIMIT n`, and `MAX(e.rowid)` — both read every entry
/// the session ever recorded and filter afterwards. Measured over 30 threads of
/// 1,000 entries each, that is 69.6 ms for the `MAX` alone; the descending walk
/// below answers the same question in 0.34 ms, and stays there as transcripts grow.
///
/// The ordering has to live inside a per-session correlated subquery. Hoisting it
/// above the `sessions` join makes Turso materialize a sorter over every session's
/// entries (measured: 126 ms), which is worse than what it replaced.
///
/// # Why counting the newest CAP entries is the same answer
///
/// Unread entries are by definition the newest ones — the watermark only ever
/// moves forward — so every entry above it lies within the newest
/// [`UNREAD_COUNT_CAP`] of its session unless there are more than `CAP` of them,
/// in which case the count saturates anyway. Taking the newest `CAP` and counting
/// those above the watermark is therefore identical to counting everything above
/// the watermark and capping, but it reads a bounded number of index entries
/// whether the operator is caught up or a month behind.
///
/// The per-session counts are summed rather than capped individually: the cap
/// belongs to the thread's total, and [`thread_unread_counts`] applies it once.
fn thread_unread_rows_sql() -> String {
    format!(
        "SELECT
        t.id,
        COALESCE((
            SELECT SUM(per_session.unread) FROM (
                SELECT (
                    SELECT COUNT(*) FROM (
                        SELECT e.rowid AS rowid
                          FROM events e
                         WHERE e.session_id = s.id
                           AND {UNREAD_EVENT_SHAPE}
                         ORDER BY e.rowid DESC
                         LIMIT {UNREAD_COUNT_CAP}
                    ) newest
                     WHERE newest.rowid > COALESCE(rp.acknowledged_event_rowid, 0)
                ) AS unread
                  FROM sessions s
                 WHERE s.job_id = j.id
            ) per_session
        ), 0) AS unread_count,
        COALESCE((
            SELECT MAX(per_session.newest) FROM (
                SELECT (
                    SELECT e.rowid
                      FROM events e
                     WHERE e.session_id = s.id
                       AND {UNREAD_EVENT_SHAPE}
                     ORDER BY e.rowid DESC
                     LIMIT 1
                ) AS newest
                  FROM sessions s
                 WHERE s.job_id = j.id
            ) per_session
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
                //
                // Saturation happens here, once, over the summed per-session
                // counts the statement returns. The SQL's per-session `LIMIT` is
                // what bounds the SCAN; this is what bounds the ANSWER.
                let entry = by_thread.entry(thread_id).or_default();
                entry.count = entry.count.max(count).min(UNREAD_COUNT_CAP);
                entry.latest_event_rowid = entry.latest_event_rowid.max(latest_event_rowid);
            }
            Ok::<HashMap<String, ThreadUnread>, DbError>(by_thread)
        })
    })
    .await
    .map_err(CairnError::from)
}

// ============================================================================
// Per-thread pending tenets
// ============================================================================

/// The one selector behind every thread's pending-tenet count: one grouped read
/// of the project's `proposed` tenets, seeked by project through an index, rather
/// than a lookup per thread row. A project holds tens of tenets, so filtering
/// status after the project seek costs nothing a sidebar can feel.
pub(crate) const THREAD_PENDING_TENETS_SQL: &str = "SELECT tn.proposing_thread_id, COUNT(*)
   FROM tenets tn
  WHERE tn.project_id = ?1 AND tn.status = 'proposed'
  GROUP BY tn.proposing_thread_id";

/// How many proposals each thread in a project has awaiting a verdict. A thread
/// with none is absent from the map; the snapshot reads that as zero.
pub async fn thread_pending_tenet_counts(
    db: &LocalDb,
    project_id: &str,
) -> Result<HashMap<String, i64>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(THREAD_PENDING_TENETS_SQL, params![project_id.as_str()])
                .await?;
            let mut by_thread = HashMap::new();
            while let Some(row) = rows.next().await? {
                by_thread.insert(row.text(0)?, row.i64(1)?);
            }
            Ok::<HashMap<String, i64>, DbError>(by_thread)
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
    COALESCE(
        (SELECT ct.updated_at FROM turns ct WHERE ct.id = j.current_turn_id),
        j.updated_at
    ) AS activity_updated_at,
    EXISTS (
        SELECT 1 FROM prompts p
         WHERE p.turn_id = j.current_turn_id
           AND p.response IS NULL
           AND p.withdrawn_at IS NULL
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

/// Everything one project's issue rows show, plus the jobs the rollup read.
///
/// The issue rollup is four project-wide statements that cost the same whether
/// one issue moved or none did, and unlike the thread rollup it had no fence at
/// all: in the hour profiled for CAIRN-4331 it spent 756 seconds answering 1,275
/// reads, nearly all of which were driven by `db-change` traffic rather than by
/// anyone looking.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct IssueStatusSnapshot {
    pub(crate) indicators: Vec<IssueStatusIndicator>,
    /// The current-execution jobs the rollup read. See
    /// [`ThreadStatusSnapshot::thread_job_ids`] for why the projection states
    /// its own dependency rather than having the invalidator guess at one.
    pub(crate) job_ids: HashSet<String>,
}

/// The published issue rollup for one project, served on the same terms as
/// [`published_thread_status_indicators`].
///
/// `checks_running` stays outside the snapshot deliberately. It is an in-memory
/// orchestrator fact with no DB row, so no `db-change` could invalidate it; the
/// transport command decorates every read from `job_ids`, and the orchestrator
/// announces the projection whenever that fact flips for a job it names.
pub async fn published_issue_status_indicators(
    orch: &crate::orchestrator::Orchestrator,
    project_id: &str,
) -> Result<Vec<IssueStatusIndicator>, CairnError> {
    orch.issue_status_cache
        .get(project_id)
        .await
        .map(|snapshot| snapshot.indicators.clone())
        .map_err(CairnError::Internal)
}

/// Rebuild one project's issue rollup: the issue status cache's rebuild.
pub(crate) async fn rebuild_issue_status(
    dbs: &crate::db::DbState,
    project_id: &str,
) -> Result<IssueStatusSnapshot, String> {
    let db = crate::execution::routing::owning_db_for_project(dbs, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let indicators = issue_status_indicators(&db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_ids = indicators
        .iter()
        .flat_map(|indicator| indicator.job_ids.iter().cloned())
        .collect();
    Ok(IssueStatusSnapshot {
        indicators,
        job_ids,
    })
}

fn node_name_from_snapshot(snapshot_json: &str, node_id: &str) -> Option<String> {
    let snapshot: ExecutionSnapshot = serde_json::from_str(snapshot_json).ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|node| node.name.clone())
}

/// Whether `node_id`'s only context edge feeds an action node.
fn has_single_downstream_action(recipe: &RecipeSnapshot, node_id: &str) -> bool {
    let node_map: HashMap<&str, &RecipeNode> = recipe
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let target_node_ids: Vec<&str> = recipe
        .edges
        .iter()
        .filter(|edge| edge.edge_type.to_string() == "context" && edge.source_node_id == node_id)
        .map(|edge| edge.target_node_id.as_str())
        .collect();

    if target_node_ids.len() != 1 {
        return false;
    }

    node_map
        .get(target_node_ids[0])
        .map(|node| node.node_type.to_string() == "action")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        derive_node_activity, issue_status_indicators, mark_thread_viewed, thread_activity_rows,
        thread_unread_counts, JobTabs, NodeActivity,
    };
    use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use cairn_db::turso::params;

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

    #[test]
    fn the_tab_rule() {
        let tabs = |status, has_artifact, downstream| {
            let tabs = JobTabs::for_job(status, has_artifact, downstream);
            (tabs.available_tabs.join(","), tabs.initial_tab)
        };
        let chat = || ("chat".to_string(), "chat".to_string());
        assert_eq!(tabs("complete", false, false), chat(), "no artifact");
        assert_eq!(
            tabs("complete", true, true),
            chat(),
            "an artifact feeding one action shows on that action"
        );
        assert_eq!(
            tabs("complete", true, false),
            ("chat,artifact".to_string(), "artifact".to_string())
        );
        assert_eq!(
            tabs("running", true, false),
            ("chat,artifact".to_string(), "chat".to_string()),
            "a running job opens on chat"
        );
    }

    /// The job statement must never read the snapshot column; the recipe graph
    /// is extracted in SQL for the executions that need it.
    #[test]
    fn the_job_statement_never_reads_the_snapshot() {
        assert!(!super::job_view_rows_sql(3).contains("snapshot"));
        assert!(super::recipe_graphs_sql(1).contains("json_extract(snapshot, '$.recipe')"));
    }

    // ── Per-thread unread transcript counts ────────────────────────────────

    async fn latest_rowid_for(db: &LocalDb, thread_id: &str) -> i64 {
        thread_unread_counts(db, "p")
            .await
            .unwrap()
            .get(thread_id)
            .map(|unread| unread.latest_event_rowid)
            .unwrap_or(-1)
    }

    // ── Issue-level (project-scoped) status indicators ──────────────────────

    // ========================================================================
    // Cost of the sidebar census at production scale
    // ========================================================================

    /// Seed one project shaped like a long-lived workspace: a few dozen threads
    /// with real transcripts behind them, and an order of magnitude more
    /// issue-node jobs whose writes must not be able to move a thread row.
    ///
    /// The proportions come from the sample in CAIRN-4331 (32 threads / 15,119
    /// jobs / 1.5M events, of which ~45k are top-level thread-session entries),
    /// scaled down to keep the seed under a few seconds while preserving the one
    /// property that drives cost: transcript length per thread.
    /// `caught_up` decides which side of the watermark the transcript sits on.
    /// It is the whole difference between the two cost regimes: a thread the
    /// operator has read has NOTHING above its watermark, so a count that filters
    /// on the watermark finds no match and never terminates early, while a thread
    /// nobody has opened satisfies the filter immediately. Production is mostly
    /// the first kind, so a benchmark that only seeds the second measures the
    /// easy case and reports a win that does not exist.
    async fn census_fixture_read(
        threads: usize,
        events_per_thread: usize,
        caught_up: bool,
    ) -> LocalDb {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'T', 'cairn', '/tmp/r', 1, 1)",
        )
        .await;
        db.write(move |conn| {
            Box::pin(async move {
                for thread in 0..threads {
                    let thread_id = format!("th{thread}");
                    let job_id = format!("th{thread}-session");
                    let session_id = format!("s{thread}");
                    let run_id = format!("r{thread}");
                    conn.execute(
                        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
                         VALUES (?1, 'p', ?1, 'active', 'none', 1, 1)",
                        params![thread_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO jobs(id, thread_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                         VALUES (?1, ?2, 'p', 'thread', 'idle', 1, 1, 'thread')",
                        params![job_id.as_str(), thread_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO sessions(id, job_id, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
                        params![session_id.as_str(), job_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO runs(id, job_id, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
                        params![run_id.as_str(), job_id.as_str()],
                    )
                    .await?;
                    for seq in 0..events_per_thread {
                        conn.execute(
                            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type,
                                                data, created_at, parent_tool_use_id)
                             VALUES (?1, ?2, ?3, ?4, 1, 'assistant', '{}', 1, NULL)",
                            params![
                                format!("e{thread}-{seq}"),
                                run_id.as_str(),
                                session_id.as_str(),
                                seq as i64
                            ],
                        )
                        .await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
        if caught_up {
            for thread in 0..threads {
                let thread_id = format!("th{thread}");
                let latest = latest_rowid_for(&db, &thread_id).await;
                mark_thread_viewed(&db, &thread_id, latest, 1)
                    .await
                    .unwrap();
            }
        }
        db
    }

    /// Seed in-progress issues alongside the threads, each with a current
    /// execution and a handful of jobs under it. The issue rollup's cost tracks
    /// this, not transcript length: it reads no events at all.
    async fn seed_issues(db: &LocalDb, issues: usize, jobs_per_issue: usize) {
        db.write(move |conn| {
            Box::pin(async move {
                for issue in 0..issues {
                    let issue_id = format!("i{issue}");
                    let execution_id = format!("x{issue}");
                    conn.execute(
                        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'p', ?2, 'T', 'active', 1, 1)",
                        params![issue_id.as_str(), issue as i64 + 1],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions(id, recipe_id, issue_id, project_id, seq, status, started_at)
                         VALUES (?1, 'r', ?2, 'p', 1, 'running', 1)",
                        params![execution_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    for job in 0..jobs_per_issue {
                        conn.execute(
                            "INSERT INTO jobs(id, issue_id, execution_id, project_id, node_name,
                                              status, created_at, updated_at)
                             VALUES (?1, ?2, ?3, 'p', 'builder', 'running', 1, 1)",
                            params![
                                format!("i{issue}-j{job}"),
                                issue_id.as_str(),
                                execution_id.as_str()
                            ],
                        )
                        .await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// What the sidebar census costs, printed rather than asserted.
    ///
    /// Ignored by default: it is a measurement, and a wall-clock threshold in CI
    /// would be a flake. Run it deliberately when changing either rollup's SQL or
    /// its indexes:
    ///
    /// ```text
    /// cargo test -p cairn-core --features test-utils --lib census_cost -- --ignored --nocapture
    /// ```
    ///
    /// The property it exists to expose is SHAPE, not absolute milliseconds:
    /// doubling `events_per_thread` must not double the reported cost. A rollup
    /// whose cost tracks transcript length is one that re-reads the whole
    /// transcript per sidebar poll, which is the defect CAIRN-4331 fixed.
    #[tokio::test]
    #[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
    async fn census_cost_at_production_scale() {
        for caught_up in [true, false] {
            for events_per_thread in [500usize, 1000, 2000] {
                let db = census_fixture_read(30, events_per_thread, caught_up).await;
                // One untimed pass so the comparison is between warm page caches.
                thread_activity_rows(&db, "p").await.unwrap();
                thread_unread_counts(&db, "p").await.unwrap();

                let started = std::time::Instant::now();
                for _ in 0..5 {
                    thread_activity_rows(&db, "p").await.unwrap();
                }
                let activity = started.elapsed() / 5;

                let started = std::time::Instant::now();
                for _ in 0..5 {
                    thread_unread_counts(&db, "p").await.unwrap();
                }
                let unread = started.elapsed() / 5;

                let regime = if caught_up { "read " } else { "unread" };
                println!(
                    "[{regime}] 30 threads x {events_per_thread} events: activity {activity:?}, unread {unread:?}"
                );
            }
        }

        // The issue rollup, whose cost tracks in-progress issues rather than
        // transcript length. This number is what one rebuild costs outright,
        // before the floor bounds how often it is paid.
        for issues in [25usize, 50, 100] {
            let db = census_fixture_read(4, 200, true).await;
            seed_issues(&db, issues, 3).await;
            issue_status_indicators(&db, "p").await.unwrap();

            let started = std::time::Instant::now();
            for _ in 0..5 {
                issue_status_indicators(&db, "p").await.unwrap();
            }
            println!(
                "[issues] {issues} active issues x 3 jobs: {:?}",
                started.elapsed() / 5
            );
        }
    }

    /// The same three sidebar projections priced against a real database.
    ///
    /// Point `CAIRN_MEASURE_DB` at an APFS clone of a production database
    /// (`cp -c ~/.cairn/cairn.db{,-log,-wal} $TMPDIR/...`; same volume, not kept:
    /// docs/database.md "Taking a census"), never at a live file:
    ///
    /// ```text
    /// CAIRN_MEASURE_DB=$TMPDIR/clone/cairn.db \
    ///   cargo test -p cairn-core --features test-utils --lib projection_cost_on_a_database_clone -- --ignored --nocapture
    /// ```
    ///
    /// It splits `list_issues` into the parts that run per call (the issue
    /// SELECT, labels, the gate census) and prices the serialized payload, which
    /// is what decides whether a snapshot should hold rows or encoded JSON.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "measurement against an external database; set CAIRN_MEASURE_DB"]
    async fn projection_cost_on_a_database_clone() {
        let Ok(path) = std::env::var("CAIRN_MEASURE_DB") else {
            println!("CAIRN_MEASURE_DB is not set; nothing to measure");
            return;
        };
        let db = LocalDb::open(&path).await.unwrap();
        let projects = db
            .query_all(
                "SELECT p.id, p.key, (SELECT COUNT(*) FROM issues i WHERE i.project_id = p.id)
                 FROM projects p ORDER BY 3 DESC",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?)),
            )
            .await
            .unwrap();
        let average = |started: std::time::Instant, n: u32| started.elapsed() / n;
        for (project_id, key, issue_count) in projects {
            if issue_count == 0 {
                continue;
            }
            // One untimed pass so every figure below is a warm-page-cache read.
            let issues = crate::issues::crud::list(&db, &project_id).await.unwrap();
            let ids: Vec<String> = issues.iter().map(|issue| issue.id.clone()).collect();

            let started = std::time::Instant::now();
            for _ in 0..3 {
                crate::issues::crud::list(&db, &project_id).await.unwrap();
            }
            let whole = average(started, 3);

            let started = std::time::Instant::now();
            for _ in 0..3 {
                let project = project_id.clone();
                db.query_all(
                    "SELECT * FROM issues WHERE project_id = ?1 ORDER BY number DESC",
                    params![project.as_str()],
                    |row| row.text(0),
                )
                .await
                .unwrap();
            }
            let select = average(started, 3);

            let started = std::time::Instant::now();
            for _ in 0..3 {
                let ids = ids.clone();
                db.read(|conn| {
                    let ids = ids.clone();
                    Box::pin(async move {
                        crate::labels::attach::list_labels_for_issues(conn, &ids).await
                    })
                })
                .await
                .unwrap();
            }
            let labels = average(started, 3);

            let started = std::time::Instant::now();
            for _ in 0..3 {
                let ids = ids.clone();
                db.read(|conn| {
                    let ids = ids.clone();
                    Box::pin(async move {
                        crate::execution::gates::census::gated_executions_for_issues_conn(
                            conn, &ids,
                        )
                        .await
                    })
                })
                .await
                .unwrap();
            }
            let gates = average(started, 3);

            let mut displayed = issues.clone();
            let started = std::time::Instant::now();
            crate::identity::display::resolve_author_displays(&db, &mut displayed).await;
            let authors = started.elapsed();

            let started = std::time::Instant::now();
            let encoded = serde_json::to_vec(&displayed).unwrap();
            let encode = started.elapsed();
            let description_bytes: usize = issues.iter().map(|issue| issue.description.len()).sum();

            let started = std::time::Instant::now();
            let thread = super::thread_status_snapshot(&db, &db, &project_id)
                .await
                .unwrap();
            let thread_census = started.elapsed();
            let started = std::time::Instant::now();
            let issue_rollup = issue_status_indicators(&db, &project_id).await.unwrap();
            let issue_census = started.elapsed();

            println!(
                "[{key} {project_id}] {issue_count} issues: list_issues {whole:?} \
                 (select {select:?}, labels {labels:?}, gates {gates:?}), authors {authors:?}, \
                 encode {encode:?}, payload {} bytes ({description_bytes} in descriptions); \
                 thread census {thread_census:?} ({} threads), issue census {issue_census:?} \
                 ({} rows)",
                encoded.len(),
                thread.indicators.len(),
                issue_rollup.len(),
            );
        }
    }
}
