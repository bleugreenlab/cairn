//! Scoped `db-change` payloads derived from records and their database context.
//!
//! Callers publish these values through their event emitter after persistence.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::models;
use crate::storage::{run_db_blocking, LocalDb, RowExt};

/// Build a project-scoped `issues` db-change payload.
pub fn issue_db_change_ids(action: &str, issue_id: &str, project_id: Option<&str>) -> Value {
    json!({
        "table": "issues",
        "action": action,
        "issueId": issue_id,
        "projectId": project_id,
    })
}

/// Build a fully-scoped `threads` db-change payload.
///
/// LOAD-BEARING INVARIANT: every row-specific thread change carries its current
/// name and an explicit `previousName`. Name-addressed panes can only re-point
/// after a remote rename when this payload supplies the old address; there is no
/// rename-history lookup on the client or in thread resolution.
pub fn thread_db_change_ids(
    action: &str,
    thread_id: &str,
    project_id: &str,
    name: &str,
    previous_name: Option<&str>,
) -> Value {
    json!({
        "table": "threads",
        "action": action,
        "threadId": thread_id,
        "projectId": project_id,
        "name": name,
        "previousName": previous_name,
    })
}

pub fn thread_db_change(thread: &models::Thread, action: &str) -> Value {
    thread_db_change_ids(action, &thread.id, &thread.project_id, &thread.name, None)
}

/// The `update` payload: `previousName` is set only when this write actually
/// moved the thread's name, so a status or definition edit is not mistaken for
/// a rename by the pane re-pointer.
pub fn thread_update_db_change(previous: &models::Thread, updated: &models::Thread) -> Value {
    let previous_name = (previous.name != updated.name).then_some(previous.name.as_str());
    thread_db_change_ids(
        "update",
        &updated.id,
        &updated.project_id,
        &updated.name,
        previous_name,
    )
}

pub fn issue_db_change(issue: &models::Issue, action: &str) -> Value {
    issue_db_change_ids(action, &issue.id, Some(&issue.project_id))
}

/// Build a fully-scoped `jobs` db-change payload.
///
/// LOAD-BEARING INVARIANT: any `jobs` db-change that carries `jobId` MUST also
/// carry the complete scoping set (`issueId`/`threadId`/`executionId`/
/// `parentJobId`/`parentToolUseId`/`projectId`). The frontend precisely-invalidates from these
/// ids and has no cache-scan fallback to recover a missing one, so a payload
/// with `jobId` but a missing `issueId` would silently skip the issue's
/// job-list pane. Route every single-job change through this builder (it is
/// always complete). Only the documented bare sweeps (`teardown`,
/// `reconcile_stale_runs`) may emit `{table:"jobs"}` with no ids at all, which
/// correctly degrades to a broad `["jobs"]` invalidation.
#[allow(clippy::too_many_arguments)]
pub fn job_db_change_ids(
    action: &str,
    job_id: &str,
    issue_id: Option<&str>,
    thread_id: Option<&str>,
    execution_id: Option<&str>,
    parent_job_id: Option<&str>,
    parent_tool_use_id: Option<&str>,
    project_id: &str,
) -> Value {
    json!({
        "table": "jobs",
        "action": action,
        "jobId": job_id,
        "issueId": issue_id,
        "threadId": thread_id,
        "executionId": execution_id,
        "parentJobId": parent_job_id,
        "parentToolUseId": parent_tool_use_id,
        "projectId": project_id,
    })
}

/// Build a fully-scoped `jobs` db-change payload from a `Job`.
///
/// See [`job_db_change_ids`] for the scoping invariant.
pub fn job_db_change(job: &models::Job, action: &str) -> Value {
    job_db_change_ids(
        action,
        &job.id,
        job.issue_id.as_deref(),
        job.thread_id.as_deref(),
        job.execution_id.as_deref(),
        job.parent_job_id.as_deref(),
        job.parent_tool_use_id.as_deref(),
        &job.project_id,
    )
}

/// Load the authoritative job scope after a write and build its payload.
///
/// The counterpart to [`run_db_change_for_id`] for call sites that hold only a
/// job id. A job row that has vanished degrades to the documented bare sweep
/// rather than emitting a partially-scoped payload, which the frontend would
/// mis-invalidate (see [`job_db_change_ids`]).
pub async fn job_db_change_for_id(db: &LocalDb, job_id: &str, action: &str) -> Value {
    let job_id_owned = job_id.to_string();
    let scope = db
        .read(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT issue_id, thread_id, execution_id, parent_job_id, parent_tool_use_id, project_id
                         FROM jobs WHERE id = ?1 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((
                        row.opt_text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                        row.opt_text(5)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((
            issue_id,
            thread_id,
            execution_id,
            parent_job_id,
            parent_tool_use_id,
            Some(project_id),
        ))) => job_db_change_ids(
            action,
            job_id,
            issue_id.as_deref(),
            thread_id.as_deref(),
            execution_id.as_deref(),
            parent_job_id.as_deref(),
            parent_tool_use_id.as_deref(),
            &project_id,
        ),
        Ok(_) => json!({"table": "jobs", "action": action}),
        Err(error) => {
            log::warn!("Failed to load job scope for db-change {job_id}: {error}");
            json!({"table": "jobs", "action": action})
        }
    }
}

/// Build a fully-scoped `runs` db-change payload.
pub fn run_db_change_ids(
    action: &str,
    run_id: &str,
    job_id: Option<&str>,
    issue_id: Option<&str>,
    project_id: Option<&str>,
) -> Value {
    json!({
        "table": "runs",
        "action": action,
        "runId": run_id,
        "jobId": job_id,
        "issueId": issue_id,
        "projectId": project_id,
    })
}

/// Build a fully-scoped `runs` db-change payload from a `Run`.
pub fn run_db_change(run: &models::Run, action: &str) -> Value {
    run_db_change_ids(
        action,
        &run.id,
        run.job_id.as_deref(),
        run.issue_id.as_deref(),
        run.project_id.as_deref(),
    )
}

/// Load the authoritative run scope after a write and build its payload. A
/// missing row is reserved for recovery/delete paths and falls back to runId.
pub async fn run_db_change_for_id(db: &LocalDb, run_id: &str, action: &str) -> Value {
    let run_id_owned = run_id.to_string();
    let scope = db
        .read(|conn| {
            let run_id = run_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id, issue_id, project_id FROM runs WHERE id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((job_id, issue_id, project_id))) => run_db_change_ids(
            action,
            run_id,
            job_id.as_deref(),
            issue_id.as_deref(),
            project_id.as_deref(),
        ),
        Ok(None) => run_db_change_ids(action, run_id, None, None, None),
        Err(error) => {
            log::warn!("Failed to load run scope for db-change {run_id}: {error}");
            run_db_change_ids(action, run_id, None, None, None)
        }
    }
}

fn turn_db_change_ids(
    action: &str,
    turn_id: Option<&str>,
    job_id: Option<&str>,
    run_id: Option<&str>,
    session_id: Option<&str>,
    issue_id: Option<&str>,
    project_id: Option<&str>,
) -> Value {
    json!({
        "table": "turns",
        "action": action,
        "turnId": turn_id,
        "jobId": job_id,
        "runId": run_id,
        "sessionId": session_id,
        "issueId": issue_id,
        "projectId": project_id,
    })
}

pub async fn turn_db_change_for_id(db: &LocalDb, turn_id: &str, action: &str) -> Value {
    let turn_id_owned = turn_id.to_string();
    let scope = db
        .read(|conn| {
            let turn_id = turn_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT t.job_id, t.run_id, t.session_id, j.issue_id, j.project_id
                         FROM turns t LEFT JOIN jobs j ON j.id = t.job_id
                         WHERE t.id = ?1 LIMIT 1",
                        (turn_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((
                        row.opt_text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((job_id, run_id, session_id, issue_id, project_id))) => turn_db_change_ids(
            action,
            Some(turn_id),
            job_id.as_deref(),
            run_id.as_deref(),
            session_id.as_deref(),
            issue_id.as_deref(),
            project_id.as_deref(),
        ),
        Ok(None) => turn_db_change_ids(action, Some(turn_id), None, None, None, None, None),
        Err(error) => {
            log::warn!("Failed to load turn scope for db-change {turn_id}: {error}");
            turn_db_change_ids(action, Some(turn_id), None, None, None, None, None)
        }
    }
}

pub async fn turn_db_change_for_job_id(db: &LocalDb, job_id: &str, action: &str) -> Value {
    let job_id_owned = job_id.to_string();
    let scope = db
        .read(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT issue_id, project_id, current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((issue_id, project_id, session_id))) => turn_db_change_ids(
            action,
            None,
            Some(job_id),
            None,
            session_id.as_deref(),
            issue_id.as_deref(),
            project_id.as_deref(),
        ),
        Ok(None) => turn_db_change_ids(action, None, Some(job_id), None, None, None, None),
        Err(error) => {
            log::warn!("Failed to load job scope for turns db-change {job_id}: {error}");
            turn_db_change_ids(action, None, Some(job_id), None, None, None, None)
        }
    }
}

// ── Projection-input scoping ──
//
// The sidebar projections (thread status, issue status, issue list) are
// per-project snapshots invalidated by these payloads. A payload on one of
// their input tables that carries no `projectId` cannot be placed and so
// invalidates EVERY project's snapshot; one with no `jobId` counts as relevant
// to every job the projection read. So every builder below carries the
// project, and the job wherever the row hangs off one. The `*_for_id` loaders
// resolve that scope after the write for call sites holding only a row id; a
// row that has vanished degrades to an unscoped payload, which over-invalidates
// rather than strands.

/// Where a row that hangs off one job sits: the job, its owner, and the project.
///
/// Prompts, permission requests and artifacts carry no scope of their own beyond
/// a job (or a run that names one), so they all inherit it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobScope {
    pub job_id: String,
    pub issue_id: Option<String>,
    pub thread_id: Option<String>,
    pub execution_id: Option<String>,
    pub project_id: String,
}

impl JobScope {
    pub fn of(job: &models::Job) -> Self {
        Self {
            job_id: job.id.clone(),
            issue_id: job.issue_id.clone(),
            thread_id: job.thread_id.clone(),
            execution_id: job.execution_id.clone(),
            project_id: job.project_id.clone(),
        }
    }
}

const JOB_SCOPE_BY_JOB_SQL: &str =
    "SELECT j.id, j.issue_id, j.thread_id, j.execution_id, j.project_id
     FROM jobs j WHERE j.id = ?1 LIMIT 1";

/// `COALESCE(p.job_id, r.job_id)`: older prompt rows name only their run.
const JOB_SCOPE_BY_PROMPT_SQL: &str =
    "SELECT j.id, j.issue_id, j.thread_id, j.execution_id, j.project_id
     FROM prompts p
     LEFT JOIN runs r ON r.id = p.run_id
     JOIN jobs j ON j.id = COALESCE(p.job_id, r.job_id)
     WHERE p.id = ?1 LIMIT 1";

/// The same owning-job resolution the thread rollup uses for a permission row
/// (`COALESCE(pr.job_id, r.job_id)`), so the frame names the job the rollup
/// will attribute it to.
const JOB_SCOPE_BY_PERMISSION_REQUEST_SQL: &str =
    "SELECT j.id, j.issue_id, j.thread_id, j.execution_id, j.project_id
     FROM permission_requests pr
     LEFT JOIN runs r ON r.id = pr.run_id
     JOIN jobs j ON j.id = COALESCE(pr.job_id, r.job_id)
     WHERE pr.id = ?1 LIMIT 1";

const JOB_SCOPE_BY_ARTIFACT_SQL: &str =
    "SELECT j.id, j.issue_id, j.thread_id, j.execution_id, j.project_id
     FROM artifacts a JOIN jobs j ON j.id = a.job_id
     WHERE a.id = ?1 LIMIT 1";

async fn load_job_scope_by(db: &LocalDb, sql: &'static str, id: &str) -> Option<JobScope> {
    let id_owned = id.to_string();
    let loaded = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, (id_owned.as_str(),)).await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((
                        row.opt_text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match loaded {
        Ok(Some((Some(job_id), issue_id, thread_id, execution_id, Some(project_id)))) => {
            Some(JobScope {
                job_id,
                issue_id,
                thread_id,
                execution_id,
                project_id,
            })
        }
        Ok(_) => None,
        Err(error) => {
            log::warn!("Failed to load job scope for db-change {id}: {error}");
            None
        }
    }
}

/// Load the scope a job's rows inherit, or `None` when the job is gone.
pub async fn load_job_scope(db: &LocalDb, job_id: &str) -> Option<JobScope> {
    load_job_scope_by(db, JOB_SCOPE_BY_JOB_SQL, job_id).await
}

/// The shared shape of a payload for a row under a job. `scope: None` is the
/// vanished-row fallback and names only the row.
fn job_row_db_change(
    table: &str,
    action: &str,
    id_key: &str,
    row_id: Option<&str>,
    scope: Option<&JobScope>,
) -> Value {
    let mut payload = serde_json::Map::new();
    payload.insert("table".into(), table.into());
    payload.insert("action".into(), action.into());
    payload.insert(id_key.into(), row_id.into());
    if let Some(scope) = scope {
        payload.insert("jobId".into(), scope.job_id.as_str().into());
        payload.insert("issueId".into(), scope.issue_id.as_deref().into());
        payload.insert("threadId".into(), scope.thread_id.as_deref().into());
        payload.insert("executionId".into(), scope.execution_id.as_deref().into());
        payload.insert("projectId".into(), scope.project_id.as_str().into());
    }
    Value::Object(payload)
}

/// Build a job-scoped `prompts` db-change payload. `prompt_id` is `None` when a
/// write settled several of one job's prompts at once.
pub fn prompt_db_change(action: &str, prompt_id: Option<&str>, scope: &JobScope) -> Value {
    job_row_db_change("prompts", action, "promptId", prompt_id, Some(scope))
}

pub async fn prompt_db_change_for_id(db: &LocalDb, prompt_id: &str, action: &str) -> Value {
    let scope = load_job_scope_by(db, JOB_SCOPE_BY_PROMPT_SQL, prompt_id).await;
    job_row_db_change(
        "prompts",
        action,
        "promptId",
        Some(prompt_id),
        scope.as_ref(),
    )
}

/// For a write that settled every open prompt of one job.
pub async fn prompt_db_change_for_job(db: &LocalDb, job_id: &str, action: &str) -> Value {
    match load_job_scope(db, job_id).await {
        Some(scope) => prompt_db_change(action, None, &scope),
        None => json!({"table": "prompts", "action": action, "jobId": job_id}),
    }
}

/// Build a job-scoped `permission_requests` db-change payload.
pub fn permission_request_db_change(
    action: &str,
    request_id: Option<&str>,
    scope: &JobScope,
) -> Value {
    job_row_db_change(
        "permission_requests",
        action,
        "requestId",
        request_id,
        Some(scope),
    )
}

pub async fn permission_request_db_change_for_id(
    db: &LocalDb,
    request_id: &str,
    action: &str,
) -> Value {
    let scope = load_job_scope_by(db, JOB_SCOPE_BY_PERMISSION_REQUEST_SQL, request_id).await;
    job_row_db_change(
        "permission_requests",
        action,
        "requestId",
        Some(request_id),
        scope.as_ref(),
    )
}

/// Build a job-scoped `artifacts` db-change payload.
pub fn artifact_db_change(action: &str, artifact_id: &str, scope: &JobScope) -> Value {
    job_row_db_change(
        "artifacts",
        action,
        "artifactId",
        Some(artifact_id),
        Some(scope),
    )
}

pub async fn artifact_db_change_for_id(db: &LocalDb, artifact_id: &str, action: &str) -> Value {
    let scope = load_job_scope_by(db, JOB_SCOPE_BY_ARTIFACT_SQL, artifact_id).await;
    job_row_db_change(
        "artifacts",
        action,
        "artifactId",
        Some(artifact_id),
        scope.as_ref(),
    )
}

/// One fully-scoped `jobs` payload per job of an execution, for a write that
/// moved several of them at once (job creation, a node restart, a recipe
/// edit). Per-job rather than one execution-wide frame because the frontend's
/// single-job queries are only reached through a `jobId`.
pub async fn job_db_changes_for_execution(
    db: &LocalDb,
    execution_id: &str,
    action: &str,
) -> Vec<Value> {
    job_db_changes_where(
        db,
        "SELECT id, issue_id, thread_id, execution_id, parent_job_id, parent_tool_use_id, project_id
         FROM jobs WHERE execution_id = ?1",
        execution_id,
        action,
    )
    .await
}

/// [`job_db_changes_for_execution`] across every execution of one issue.
pub async fn job_db_changes_for_issue(db: &LocalDb, issue_id: &str, action: &str) -> Vec<Value> {
    job_db_changes_where(
        db,
        "SELECT id, issue_id, thread_id, execution_id, parent_job_id, parent_tool_use_id, project_id
         FROM jobs WHERE issue_id = ?1",
        issue_id,
        action,
    )
    .await
}

/// A failed read degrades to the documented bare sweep rather than to silence,
/// so a write is never left unannounced.
async fn job_db_changes_where(
    db: &LocalDb,
    sql: &'static str,
    id: &str,
    action: &str,
) -> Vec<Value> {
    let id_owned = id.to_string();
    let loaded = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, (id_owned.as_str(),)).await?;
                let mut jobs = Vec::new();
                while let Some(row) = rows.next().await? {
                    jobs.push((
                        row.text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                        row.opt_text(5)?,
                        row.text(6)?,
                    ));
                }
                Ok(jobs)
            })
        })
        .await;
    match loaded {
        Ok(jobs) => jobs
            .iter()
            .map(
                |(
                    job_id,
                    issue_id,
                    thread_id,
                    execution_id,
                    parent_job_id,
                    parent_tool_use_id,
                    project_id,
                )| {
                    job_db_change_ids(
                        action,
                        job_id,
                        issue_id.as_deref(),
                        thread_id.as_deref(),
                        execution_id.as_deref(),
                        parent_job_id.as_deref(),
                        parent_tool_use_id.as_deref(),
                        project_id,
                    )
                },
            )
            .collect(),
        Err(error) => {
            log::warn!("Failed to load jobs for db-change ({id}): {error}");
            vec![json!({"table": "jobs", "action": action})]
        }
    }
}

/// Build a project-scoped `executions` db-change payload.
pub fn execution_db_change_ids(
    action: &str,
    execution_id: &str,
    issue_id: Option<&str>,
    project_id: &str,
) -> Value {
    json!({
        "table": "executions",
        "action": action,
        "executionId": execution_id,
        "issueId": issue_id,
        "projectId": project_id,
    })
}

/// Load an execution's scope after a write and build its payload.
/// `executions.project_id` is nullable on older rows; the issue supplies it.
pub async fn execution_db_change_for_id(db: &LocalDb, execution_id: &str, action: &str) -> Value {
    let execution_id_owned = execution_id.to_string();
    let scope = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT e.issue_id, COALESCE(e.project_id, i.project_id)
                         FROM executions e LEFT JOIN issues i ON i.id = e.issue_id
                         WHERE e.id = ?1 LIMIT 1",
                        (execution_id_owned.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((issue_id, Some(project_id)))) => {
            execution_db_change_ids(action, execution_id, issue_id.as_deref(), &project_id)
        }
        Ok(_) => json!({"table": "executions", "action": action, "executionId": execution_id}),
        Err(error) => {
            log::warn!("Failed to load execution scope for db-change {execution_id}: {error}");
            json!({"table": "executions", "action": action, "executionId": execution_id})
        }
    }
}

/// An `execution_gates` payload for a write that moved one execution's gate
/// rows without touching the execution itself: a recorded outcome or a latched
/// foreclosure. Placed exactly like the execution it belongs to, because the
/// gate census renders on that execution's issue row.
pub async fn execution_gates_db_change_for_execution(
    db: &LocalDb,
    execution_id: &str,
    action: &str,
) -> Value {
    let mut change = execution_db_change_for_id(db, execution_id, action).await;
    change["table"] = json!("execution_gates");
    change
}

/// One `executions` payload per execution of an issue, for a write that moved
/// all of them (a reopen lifting resolution-cancelled work).
pub async fn execution_db_changes_for_issue(
    db: &LocalDb,
    issue_id: &str,
    action: &str,
) -> Vec<Value> {
    let issue_id_owned = issue_id.to_string();
    let loaded = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT e.id, COALESCE(e.project_id, i.project_id)
                         FROM executions e JOIN issues i ON i.id = e.issue_id
                         WHERE e.issue_id = ?1",
                        (issue_id_owned.as_str(),),
                    )
                    .await?;
                let mut executions = Vec::new();
                while let Some(row) = rows.next().await? {
                    executions.push((row.text(0)?, row.text(1)?));
                }
                Ok(executions)
            })
        })
        .await;
    match loaded {
        Ok(executions) => executions
            .iter()
            .map(|(execution_id, project_id)| {
                execution_db_change_ids(action, execution_id, Some(issue_id), project_id)
            })
            .collect(),
        Err(error) => {
            log::warn!("Failed to load executions of issue {issue_id} for db-change: {error}");
            vec![json!({"table": "executions", "action": action, "issueId": issue_id})]
        }
    }
}

/// Build a project-scoped `action_runs` db-change payload.
///
/// An action run has no job of its own; `parentJobId` names the job that
/// produced it. It is deliberately not sent as `jobId`, which the projections
/// read as "a row under this job" and would use to skip the frame.
pub fn action_run_db_change_ids(
    action: &str,
    action_run_id: &str,
    execution_id: &str,
    issue_id: Option<&str>,
    parent_job_id: Option<&str>,
    project_id: &str,
) -> Value {
    json!({
        "table": "action_runs",
        "action": action,
        "actionRunId": action_run_id,
        "executionId": execution_id,
        "issueId": issue_id,
        "parentJobId": parent_job_id,
        "projectId": project_id,
    })
}

pub fn action_run_db_change(action_run: &models::ActionRun, action: &str) -> Value {
    action_run_db_change_ids(
        action,
        &action_run.id,
        &action_run.execution_id,
        action_run.issue_id.as_deref(),
        action_run.parent_job_id.as_deref(),
        &action_run.project_id,
    )
}

pub async fn action_run_db_change_for_id(db: &LocalDb, action_run_id: &str, action: &str) -> Value {
    let action_run_id_owned = action_run_id.to_string();
    let scope = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, issue_id, parent_job_id, project_id
                         FROM action_runs WHERE id = ?1 LIMIT 1",
                        (action_run_id_owned.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((
                        row.text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.text(3)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((execution_id, issue_id, parent_job_id, project_id))) => action_run_db_change_ids(
            action,
            action_run_id,
            &execution_id,
            issue_id.as_deref(),
            parent_job_id.as_deref(),
            &project_id,
        ),
        Ok(None) => json!({"table": "action_runs", "action": action, "actionRunId": action_run_id}),
        Err(error) => {
            log::warn!("Failed to load action run scope for db-change {action_run_id}: {error}");
            json!({"table": "action_runs", "action": action, "actionRunId": action_run_id})
        }
    }
}

/// Build a project-scoped `merge_requests` db-change payload.
///
/// `merge_request_id` and `job_id` are `None` only for a write that touched
/// every open request on a branch at once.
pub fn merge_request_db_change_ids(
    action: &str,
    merge_request_id: Option<&str>,
    job_id: Option<&str>,
    issue_id: Option<&str>,
    project_id: &str,
) -> Value {
    json!({
        "table": "merge_requests",
        "action": action,
        "mergeRequestId": merge_request_id,
        "jobId": job_id,
        "issueId": issue_id,
        "projectId": project_id,
    })
}

pub async fn merge_request_db_change_for_id(
    db: &LocalDb,
    merge_request_id: &str,
    action: &str,
) -> Value {
    let merge_request_id_owned = merge_request_id.to_string();
    let scope = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id, issue_id, project_id FROM merge_requests WHERE id = ?1 LIMIT 1",
                        (merge_request_id_owned.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.opt_text(1)?, row.text(2)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((job_id, issue_id, project_id))) => merge_request_db_change_ids(
            action,
            Some(merge_request_id),
            Some(&job_id),
            issue_id.as_deref(),
            &project_id,
        ),
        Ok(None) => json!({
            "table": "merge_requests",
            "action": action,
            "mergeRequestId": merge_request_id,
        }),
        Err(error) => {
            log::warn!(
                "Failed to load merge request scope for db-change {merge_request_id}: {error}"
            );
            json!({
                "table": "merge_requests",
                "action": action,
                "mergeRequestId": merge_request_id,
            })
        }
    }
}

/// Build a project-scoped `issue_labels` db-change payload for one issue's
/// label links.
pub fn issue_labels_db_change(action: &str, issue_id: &str, project_id: &str) -> Value {
    json!({
        "table": "issue_labels",
        "action": action,
        "issueId": issue_id,
        "projectId": project_id,
    })
}

/// Load an issue's project after a write and build its payload.
pub async fn issue_db_change_for_id(db: &LocalDb, issue_id: &str, action: &str) -> Value {
    let issue_id_owned = issue_id.to_string();
    let project_id = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT project_id FROM issues WHERE id = ?1 LIMIT 1",
                        (issue_id_owned.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => row.opt_text(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .unwrap_or_else(|error| {
            log::warn!("Failed to load issue scope for db-change {issue_id}: {error}");
            None
        });
    issue_db_change_ids(action, issue_id, project_id.as_deref())
}

/// Load a thread's project and name after a write and build its payload. A
/// thread that has vanished degrades to a payload naming only the thread.
pub async fn thread_db_change_for_id(db: &LocalDb, thread_id: &str, action: &str) -> Value {
    let thread_id_owned = thread_id.to_string();
    let scope = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT project_id, name FROM threads WHERE id = ?1 LIMIT 1",
                        (thread_id_owned.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match scope {
        Ok(Some((project_id, name))) => {
            thread_db_change_ids(action, thread_id, &project_id, &name, None)
        }
        Ok(None) => json!({"table": "threads", "action": action, "threadId": thread_id}),
        Err(error) => {
            log::warn!("Failed to load thread scope for db-change {thread_id}: {error}");
            json!({"table": "threads", "action": action, "threadId": thread_id})
        }
    }
}

/// Build a fully-scoped `events` db-change payload.
///
/// LOAD-BEARING INVARIANT: the live chat transcript's incremental delta cache
/// (`useSessionEvents(sessionId)`) is invalidated ONLY by the `events` resolver,
/// which scopes by `sessionId`. An events insert that emits the wrong
/// `session_id` — or skips the emit entirely — strands the transcript until a
/// manual reload, while the issue-overview job row survives on sibling
/// invalidations and masks the freeze (CAIRN-1916). So every live/finalize
/// events insert must emit through this one builder and always carry both
/// `run_id` and `session_id`. Route inserts through one of the shared emitting
/// seams so the scoped emit can't be forgotten:
/// `cairn_core::transcripts::stream_store::insert_event_emit` /
/// `cairn_core::transcripts::stream_store::finalize_stream_emit` for the durable
/// streaming/tool-call path, and the private `messages::transcript`
/// `insert_system_event` / `insert_system_event_sync` seams (which emit via
/// [`event_db_change_for_run`]) for boundary-delivered system/user transcript
/// inserts (CAIRN-2558). Both camel- and snake-cased keys are emitted because the
/// frontend `parseScopeIds` reads either.
pub fn event_db_change(run_id: &str, session_id: Option<&str>, action: &str) -> Value {
    event_db_change_scoped(run_id, session_id, &EventRunScope::default(), action)
}

/// Where a transcript event sits in the workspace: the issue it belongs to, the
/// job that produced it, and the project that owns both.
///
/// Resolved in ONE query rather than one per field, because these facts are only
/// useful together. A live surface that filters by project needs `project_id`
/// on the same frame that carries `run_id`, and the per-job file-activity
/// stream needs `job_id` beside it; reading them separately would let a run's
/// rows change between lookups and describe a placement that never existed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventRunScope {
    pub issue_id: Option<String>,
    /// The thread that owns this event's job, when the job belongs to a thread's
    /// descent rather than an issue's execution. Exactly one of `issue_id` and
    /// `thread_id` is set for a canonical job, so a live surface scoped to one
    /// owner is never invalidated by the other's traffic.
    pub thread_id: Option<String>,
    pub job_id: Option<String>,
    pub project_id: Option<String>,
}

/// Load a run's issue/job/project scope, degrading to an empty scope (which
/// serializes as explicit nulls) rather than failing the emit it decorates.
///
/// `runs.project_id` and `runs.job_id` are nullable for runs recorded before a
/// job was attached, so the owning job is the fallback for both — the same
/// `COALESCE` shape the analytics token CTE uses.
pub fn event_run_scope(db: Arc<LocalDb>, run_id: &str) -> EventRunScope {
    let run_id_owned = run_id.to_string();
    let loaded = run_db_blocking(move || async move {
        db.read(|conn| {
            let run_id = run_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT r.issue_id, j.thread_id, COALESCE(r.job_id, j.id),
                                COALESCE(r.project_id, j.project_id)
                         FROM runs r LEFT JOIN jobs j ON j.id = r.job_id
                         WHERE r.id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some(EventRunScope {
                        issue_id: row.opt_text(0)?,
                        thread_id: row.opt_text(1)?,
                        job_id: row.opt_text(2)?,
                        project_id: row.opt_text(3)?,
                    })),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| format!("Failed to load run scope for event db-change: {e}"))
    });
    match loaded {
        Ok(Some(scope)) => scope,
        Ok(None) => EventRunScope::default(),
        Err(error) => {
            log::warn!("{error}");
            EventRunScope::default()
        }
    }
}

pub fn event_db_change_for_run(
    db: Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    action: &str,
) -> Value {
    let scope = event_run_scope(db, run_id);
    event_db_change_scoped(run_id, session_id, &scope, action)
}

pub fn event_db_change_scoped(
    run_id: &str,
    session_id: Option<&str>,
    scope: &EventRunScope,
    action: &str,
) -> Value {
    json!({
        "table": "events",
        "action": action,
        "runId": run_id,
        "run_id": run_id,
        "sessionId": session_id,
        "session_id": session_id,
        "issueId": scope.issue_id,
        "issue_id": scope.issue_id,
        "threadId": scope.thread_id,
        "thread_id": scope.thread_id,
        "jobId": scope.job_id,
        "job_id": scope.job_id,
        "projectId": scope.project_id,
        "project_id": scope.project_id,
    })
}

/// A sidebar projection's announcement that its published answer for one
/// project changed: `projection` is `thread_status`, `issue_status` or
/// `issue_list`.
///
/// The only frame the frontend refetches those projections on. It is not a
/// table, and nothing that invalidates a projection reads it, so publishing
/// never feeds back into another rebuild.
pub fn projection_published_db_change(projection: &str, project_id: &str) -> Value {
    json!({
        "table": projection,
        "action": "published",
        "projectId": project_id,
    })
}

pub fn job_presence_db_change(scope: &EventRunScope) -> Value {
    json!({
        "table": "job_presence",
        "action": "upsert",
        "jobId": scope.job_id,
        "job_id": scope.job_id,
        "projectId": scope.project_id,
        "project_id": scope.project_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> JobScope {
        JobScope {
            job_id: "job-1".into(),
            issue_id: Some("issue-1".into()),
            thread_id: None,
            execution_id: Some("exec-1".into()),
            project_id: "project-1".into(),
        }
    }

    #[test]
    fn job_owned_rows_carry_the_job_and_its_project() {
        for (payload, table, id_key, id) in [
            (
                prompt_db_change("insert", Some("prompt-1"), &scope()),
                "prompts",
                "promptId",
                "prompt-1",
            ),
            (
                permission_request_db_change("update", Some("perm-1"), &scope()),
                "permission_requests",
                "requestId",
                "perm-1",
            ),
            (
                artifact_db_change("update", "artifact-1", &scope()),
                "artifacts",
                "artifactId",
                "artifact-1",
            ),
        ] {
            assert_eq!(payload["table"], table);
            assert_eq!(payload[id_key], id);
            assert_eq!(payload["projectId"], "project-1", "{table}");
            assert_eq!(payload["jobId"], "job-1", "{table}");
            assert_eq!(payload["issueId"], "issue-1", "{table}");
            assert_eq!(payload["executionId"], "exec-1", "{table}");
            // Present-and-null, so a thread-scoped surface can tell "not a
            // thread's" from "this build predates the key".
            assert!(payload.get("threadId").is_some_and(Value::is_null));
        }
    }

    #[test]
    fn a_vanished_rows_fallback_names_only_the_row() {
        let payload = job_row_db_change("prompts", "update", "promptId", Some("p"), None);
        assert_eq!(payload["promptId"], "p");
        assert!(payload.get("projectId").is_none());
    }

    #[test]
    fn job_scope_of_a_job_carries_its_owner_and_project() {
        let job = models::Job {
            id: "job-1".into(),
            execution_id: Some("exec-1".into()),
            recipe_node_id: None,
            parent_job_id: None,
            branch: None,
            pack_anchor: None,
            current_session_id: None,
            status: models::JobStatus::Running,
            agent_config_id: None,
            issue_id: None,
            thread_id: Some("thread-1".into()),
            project_id: "project-1".into(),
            task_description: None,
            model: None,
            created_at: 0,
            updated_at: 0,
            completed_at: None,
            started_at: None,
            available_tabs: vec![],
            initial_tab: "chat".into(),
            parent_tool_use_id: None,
            task_index: None,
            node_name: None,
            exec_seq: None,
            base_branch: None,
            uri_segment: None,
        };
        let payload = prompt_db_change("update", None, &JobScope::of(&job));
        assert_eq!(payload["threadId"], "thread-1");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["projectId"], "project-1");
        assert!(payload["promptId"].is_null());
    }

    #[test]
    fn execution_and_issue_level_builders_carry_the_project() {
        let execution = execution_db_change_ids("insert", "exec-1", Some("issue-1"), "project-1");
        assert_eq!(execution["table"], "executions");
        assert_eq!(execution["executionId"], "exec-1");
        assert_eq!(execution["issueId"], "issue-1");
        assert_eq!(execution["projectId"], "project-1");

        let labels = issue_labels_db_change("update", "issue-1", "project-1");
        assert_eq!(labels["table"], "issue_labels");
        assert_eq!(labels["issueId"], "issue-1");
        assert_eq!(labels["projectId"], "project-1");
    }

    #[test]
    fn merge_request_builder_carries_its_job_and_project() {
        let payload = merge_request_db_change_ids(
            "update",
            Some("mr-1"),
            Some("job-1"),
            Some("issue-1"),
            "project-1",
        );
        assert_eq!(payload["table"], "merge_requests");
        assert_eq!(payload["mergeRequestId"], "mr-1");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["projectId"], "project-1");

        // A branch-wide write names no row or job, but still its project.
        let branch = merge_request_db_change_ids("update", None, None, None, "project-1");
        assert_eq!(branch["projectId"], "project-1");
        assert!(branch["jobId"].is_null());
    }

    #[test]
    fn action_run_builder_names_its_parent_job_but_not_as_job_id() {
        let payload = action_run_db_change_ids(
            "update",
            "ar-1",
            "exec-1",
            Some("issue-1"),
            Some("job-1"),
            "project-1",
        );
        assert_eq!(payload["table"], "action_runs");
        assert_eq!(payload["actionRunId"], "ar-1");
        assert_eq!(payload["executionId"], "exec-1");
        assert_eq!(payload["parentJobId"], "job-1");
        assert_eq!(payload["projectId"], "project-1");
        // The projections read `jobId` as "a row under this job" and would skip
        // the frame for any job outside the set they read.
        assert!(payload.get("jobId").is_none());
    }

    #[test]
    fn turn_db_change_ids_carries_job_run_and_project_scope() {
        let payload = turn_db_change_ids(
            "update",
            Some("turn-1"),
            Some("job-1"),
            Some("run-1"),
            Some("session-1"),
            Some("issue-1"),
            Some("project-1"),
        );
        assert_eq!(payload["table"], "turns");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["turnId"], "turn-1");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["runId"], "run-1");
        assert_eq!(payload["sessionId"], "session-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["projectId"], "project-1");
    }
}
