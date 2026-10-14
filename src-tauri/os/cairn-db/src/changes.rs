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
    job_id: Option<&str>,
    run_id: Option<&str>,
    session_id: Option<&str>,
    issue_id: Option<&str>,
    project_id: Option<&str>,
) -> Value {
    json!({
        "table": "turns",
        "action": action,
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
            job_id.as_deref(),
            run_id.as_deref(),
            session_id.as_deref(),
            issue_id.as_deref(),
            project_id.as_deref(),
        ),
        Ok(None) => turn_db_change_ids(action, None, None, None, None, None),
        Err(error) => {
            log::warn!("Failed to load turn scope for db-change {turn_id}: {error}");
            turn_db_change_ids(action, None, None, None, None, None)
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
            Some(job_id),
            None,
            session_id.as_deref(),
            issue_id.as_deref(),
            project_id.as_deref(),
        ),
        Ok(None) => turn_db_change_ids(action, Some(job_id), None, None, None, None),
        Err(error) => {
            log::warn!("Failed to load job scope for turns db-change {job_id}: {error}");
            turn_db_change_ids(action, Some(job_id), None, None, None, None)
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
    #[test]
    fn turn_db_change_ids_carries_job_run_and_project_scope() {
        let payload = turn_db_change_ids(
            "update",
            Some("job-1"),
            Some("run-1"),
            Some("session-1"),
            Some("issue-1"),
            Some("project-1"),
        );
        assert_eq!(payload["table"], "turns");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["jobId"], "job-1");
        assert_eq!(payload["runId"], "run-1");
        assert_eq!(payload["sessionId"], "session-1");
        assert_eq!(payload["issueId"], "issue-1");
        assert_eq!(payload["projectId"], "project-1");
    }
}
