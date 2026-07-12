//! Terminal resource and PTY-session machinery for MCP handlers.
//!
//! Long-lived terminal resources are managed separately from synchronous `run`
//! batches, but share the same process/sandbox primitives where behavior overlaps.

use super::run::{
    apply_non_interactive_pager_env_to_pty, build_run_sandbox_policy,
    scrub_dev_instance_routing_pty, PromotedTerminal, MAX_BUFFER_SIZE,
};
use crate::services::{
    ensure_submitted_line, get_default_shell, sandbox, submit_command_exiting_shell, PtySession,
    TerminalChild,
};
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_common::uri::CairnResource;
use cairn_db::turso::params;
use portable_pty::{CommandBuilder, PtySize};
use serde::Serialize;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::SystemTime;
use uuid::Uuid;

use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;

/// PTY data event payload (mirrors Tauri-side PtyDataPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyDataPayload {
    pub session_id: String,
    pub data: String,
}

/// PTY exit event payload (mirrors Tauri-side PtyExitPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyExitPayload {
    pub session_id: String,
    pub exit_code: Option<i32>,
}

/// Event payload when agent spawns a background terminal.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentTerminalCreatedPayload {
    pub run_id: String,
    pub session_id: String,
    pub command: String,
    pub description: Option<String>,
}

#[derive(Clone)]
struct TerminalResourceTarget {
    slug: String,
    job_id: Option<String>,
    project_id: String,
    run_id: Option<String>,
    cwd: String,
}

async fn resolve_terminal_resource_target(
    db: &LocalDb,
    resource: &CairnResource,
) -> DbResult<TerminalResourceTarget> {
    let resource = resource.clone();
    db.read(|conn| {
        Box::pin(async move {
            match resource {
                CairnResource::NodeTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    slug,
                } => {
                    let job_id = find_terminal_target_job_id(conn, &project, number, exec_seq, &node_id)
                        .await?
                        .ok_or_else(|| {
                            crate::storage::DbError::Row(format!(
                                "No node job found for terminal target {project}-{number}/{exec_seq}/{node_id}"
                            ))
                        })?;
                    let mut rows = conn
                        .query(
                            "
                            SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path), r.id
                            FROM jobs j
                            JOIN projects p ON j.project_id = p.id
                            LEFT JOIN runs r ON r.job_id = j.id
                            WHERE j.id = ?1
                            ORDER BY r.created_at DESC
                            LIMIT 1
                            ",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: Some(job_id),
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: row.opt_text(2)?,
                    })
                }
                CairnResource::TaskTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    task_name,
                    slug,
                } => {
                    let job_id = find_task_terminal_target_job_id(
                        conn,
                        &project,
                        number,
                        exec_seq,
                        &node_id,
                        &task_name,
                    )
                    .await?
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!(
                            "No task job found for terminal target {project}-{number}/{exec_seq}/{node_id}/task/{task_name}"
                        ))
                    })?;
                    let mut rows = conn
                        .query(
                            "
                            SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path), r.id
                            FROM jobs j
                            JOIN projects p ON j.project_id = p.id
                            LEFT JOIN runs r ON r.job_id = j.id
                            WHERE j.id = ?1
                            ORDER BY r.created_at DESC
                            LIMIT 1
                            ",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: Some(job_id),
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: row.opt_text(2)?,
                    })
                }
                CairnResource::ProjectTerminal { project, slug } => {
                    let lookup_key = project.to_uppercase();
                    let mut rows = conn
                        .query(
                            "SELECT id, repo_path FROM projects WHERE key = ?1 LIMIT 1",
                            (lookup_key.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No project found for key {project}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: None,
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: None,
                    })
                }
                _ => Err(crate::storage::DbError::Row(
                    "Resource is not a terminal URI".to_string(),
                )),
            }
        })
    })
    .await
}

async fn ensure_terminal_slug_available(
    db: &LocalDb,
    target: &TerminalResourceTarget,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            // Only a *running* terminal blocks the slug. Exited rows linger (for
            // post-exit reads and already-exited wake subscribes) and are
            // reclaimed on insert, so they must not block re-creating the slug.
            let exists = match job_id.as_deref() {
                Some(job_id) => terminal_slug_running_for_job(conn, job_id, &slug).await?,
                None => terminal_slug_running_for_project(conn, &project_id, &slug).await?,
            };
            if exists {
                return Err(crate::storage::DbError::Row(format!(
                    "A running terminal already exists in this scope: {slug}"
                )));
            }
            Ok(())
        })
    })
    .await
}

async fn insert_terminal_resource(
    db: &LocalDb,
    target: &TerminalResourceTarget,
    session_id: &str,
    command: &str,
    description: Option<&str>,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let run_id = target.run_id.clone();
    let slug = target.slug.clone();
    let session_id = session_id.to_string();
    let command = command.to_string();
    let description = description.map(ToOwned::to_owned);

    db.write(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let run_id = run_id.clone();
        let slug = slug.clone();
        let session_id = session_id.clone();
        let command = command.clone();
        let description = description.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            let id = Uuid::new_v4().to_string();
            // Reclaim a lingering exited row for this (scope, slug). The
            // create-availability check only blocks running slugs, so an exited
            // row could still collide with the unique (scope, slug) index.
            match job_id.as_deref() {
                Some(job_id) => {
                    conn.execute(
                        "DELETE FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
                        (job_id, slug.as_str()),
                    )
                    .await?;
                }
                None => {
                    conn.execute(
                        "DELETE FROM job_terminals WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2",
                        (project_id.as_str(), slug.as_str()),
                    )
                    .await?;
                }
            }
            conn.execute(
                "
                INSERT INTO job_terminals (
                    id, job_id, project_id, run_id, session_id, command, title,
                    description, status, exit_code, created_at, exited_at, slug
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'running', NULL, ?8, NULL, ?9)
                ",
                (
                    id.as_str(),
                    job_id.as_deref(),
                    if job_id.is_some() {
                        None
                    } else {
                        Some(project_id.as_str())
                    },
                    run_id.as_deref(),
                    session_id.as_str(),
                    command.as_str(),
                    description.as_deref(),
                    now,
                    slug.as_str(),
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
}

async fn update_terminal_resource_session(
    db: &LocalDb,
    target: &TerminalResourceTarget,
    session_id: &str,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    let session_id = session_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        let session_id = session_id.clone();
        Box::pin(async move {
            if let Some(job_id) = job_id {
                conn.execute(
                    "
                    UPDATE job_terminals
                    SET session_id = ?3, status = 'running', exit_code = NULL, exited_at = NULL
                    WHERE job_id = ?1 AND slug = ?2
                    ",
                    (job_id.as_str(), slug.as_str(), session_id.as_str()),
                )
                .await?;
            } else {
                conn.execute(
                    "
                    UPDATE job_terminals
                    SET session_id = ?3, status = 'running', exit_code = NULL, exited_at = NULL
                    WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
                    ",
                    (project_id.as_str(), slug.as_str(), session_id.as_str()),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

async fn find_terminal_target_job_id(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_id: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut issue_rows = conn
        .query(
            "
            SELECT i.id
            FROM issues i
            JOIN projects p ON i.project_id = p.id
            WHERE p.key = ?1 AND i.number = ?2
            LIMIT 1
            ",
            (lookup_key.as_str(), issue_number),
        )
        .await?;

    let Some(issue_row) = issue_rows.next().await? else {
        return Ok(None);
    };
    let issue_id = issue_row.text(0)?;

    let mut exec_rows = conn
        .query(
            "
            SELECT id
            FROM executions
            WHERE issue_id = ?1 AND seq = ?2
            LIMIT 1
            ",
            (issue_id.as_str(), exec_seq),
        )
        .await?;

    let Some(exec_row) = exec_rows.next().await? else {
        return Ok(None);
    };
    let exec_id = exec_row.text(0)?;

    let mut exact_rows = conn
        .query(
            "
            SELECT id
            FROM jobs
            WHERE issue_id = ?1
              AND execution_id = ?2
              AND parent_job_id IS NULL
              AND uri_segment = ?3
            LIMIT 1
            ",
            (issue_id.as_str(), exec_id.as_str(), node_id),
        )
        .await?;

    if let Some(row) = exact_rows.next().await? {
        return Ok(Some(row.text(0)?));
    }

    Ok(None)
}

async fn find_task_terminal_target_job_id(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    parent_node_id: &str,
    task_name: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT child.id
            FROM jobs parent
            JOIN jobs child ON child.parent_job_id = parent.id
            JOIN issues i ON parent.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            JOIN executions e ON parent.execution_id = e.id
            WHERE p.key = ?1
              AND i.number = ?2
              AND e.seq = ?3
              AND parent.parent_job_id IS NULL
              AND parent.uri_segment = ?4
              AND child.uri_segment = ?5
            LIMIT 1
            ",
            (
                lookup_key.as_str(),
                issue_number,
                exec_seq,
                parent_node_id,
                task_name,
            ),
        )
        .await?;
    rows.next().await?.map(|row| row.text(0)).transpose()
}

async fn terminal_slug_exists_for_job(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
            (job_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn terminal_slug_exists_for_project(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "
            SELECT COUNT(*)
            FROM job_terminals
            WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
            ",
            (project_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

/// Whether a *running* terminal with this slug exists in the job scope. Used by
/// create-availability so a lingering exited row does not block re-use.
async fn terminal_slug_running_for_job(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals
             WHERE job_id = ?1 AND slug = ?2 AND status = 'running'",
            (job_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn terminal_slug_running_for_project(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals
             WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2 AND status = 'running'",
            (project_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

/// A job-owned terminal row, loaded for terminal-exit wake subscribe validation
/// and the already-exited immediate-fire path.
pub(crate) struct TerminalWakeRow {
    pub status: String,
    pub exit_code: Option<i32>,
    pub created_at: i64,
    pub exited_at: Option<i64>,
    pub output_tail: Option<String>,
    /// The live PTY session id, used to reach the session's in-memory output
    /// buffer and phrase-watcher registry when subscribing an output wake.
    pub session_id: Option<String>,
}

/// Look up a terminal by job scope + slug for wake subscription.
pub(crate) async fn lookup_terminal_for_wake(
    db: &LocalDb,
    job_id: &str,
    slug: &str,
) -> DbResult<Option<TerminalWakeRow>> {
    let job_id = job_id.to_string();
    let slug = slug.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, exit_code, created_at, exited_at, output_tail, session_id
                     FROM job_terminals WHERE job_id = ?1 AND slug = ?2 LIMIT 1",
                    params![job_id.as_str(), slug.as_str()],
                )
                .await?;
            terminal_wake_row_from_rows(&mut rows).await
        })
    })
    .await
}

async fn lookup_terminal_for_wake_by_session_id(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Option<TerminalWakeRow>> {
    let session_id = session_id.to_string();
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, exit_code, created_at, exited_at, output_tail, session_id
                     FROM job_terminals WHERE session_id = ?1 LIMIT 1",
                    params![session_id.as_str()],
                )
                .await?;
            terminal_wake_row_from_rows(&mut rows).await
        })
    })
    .await
}

async fn terminal_wake_row_from_rows(
    rows: &mut cairn_db::turso::Rows,
) -> DbResult<Option<TerminalWakeRow>> {
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(TerminalWakeRow {
        status: row.text(0)?,
        exit_code: row.opt_i64(1)?.map(|code| code as i32),
        created_at: row.i64(2)?,
        exited_at: row.opt_i64(3)?,
        output_tail: row.opt_text(4)?,
        session_id: row.opt_text(5)?,
    }))
}

/// List the terminal slugs owned by a job, for a precise unknown-slug error.
pub(crate) async fn list_job_terminal_slugs(db: &LocalDb, job_id: &str) -> DbResult<Vec<String>> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT slug FROM job_terminals
                     WHERE job_id = ?1 AND slug IS NOT NULL ORDER BY slug",
                    params![job_id.as_str()],
                )
                .await?;
            let mut slugs = Vec::new();
            while let Some(row) = rows.next().await? {
                slugs.push(row.text(0)?);
            }
            Ok(slugs)
        })
    })
    .await
}

pub(crate) async fn subscribe_terminal_exit_wake_once(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    uri: &str,
    row: Option<&TerminalWakeRow>,
    created_by: &str,
) -> Result<TerminalWakeSubscriptionOutcome, String> {
    // Store the canonical terminal URI as the source ref, not the bare slug:
    // slugs are unique only per job, so a slug ref would cross-match other jobs'
    // same-slug terminals. The route side keys on the same canonical URI.
    let fact_kinds = vec![crate::orchestrator::wakes::FACT_KIND_TERMINAL_EXIT.to_string()];
    crate::orchestrator::wakes::subscribe_one_shot(
        &orch.db.local,
        job_id,
        "process",
        Some(uri),
        Some(&fact_kinds),
        created_by,
    )
    .await?;

    if let Some(row) = row.filter(|row| row.status == "exited") {
        let runtime_secs = row.exited_at.map(|exited| (exited - row.created_at).max(0));
        crate::orchestrator::wakes::route_terminal_exit_async(
            orch,
            slug,
            uri,
            row.exit_code,
            runtime_secs,
            row.output_tail.as_deref(),
        )
        .await?;
        Ok(TerminalWakeSubscriptionOutcome::ExitAlreadyQueued)
    } else {
        Ok(TerminalWakeSubscriptionOutcome::ExitSubscribed)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn subscribe_terminal_output_wake_once(
    orch: &Orchestrator,
    job_id: &str,
    slug: &str,
    uri: &str,
    phrase: &str,
    row: Option<&TerminalWakeRow>,
    session_id: Option<&str>,
    created_by: &str,
) -> Result<TerminalWakeSubscriptionOutcome, String> {
    // Persist first: the subscription is a durable, terminal-scoped property,
    // not bound to whichever PTY session is live right now. A (re)starting
    // session hydrates it, so it survives the worktree-fence approval respawn.
    let sub = crate::orchestrator::wakes::subscribe_terminal_output_one_shot(
        &orch.db.local,
        job_id,
        uri,
        phrase,
        created_by,
    )
    .await?;

    if let Some(row) = row.filter(|row| row.status == "exited") {
        let runtime_secs = row.exited_at.map(|exited| (exited - row.created_at).max(0));
        crate::orchestrator::wakes::route_terminal_exit_async(
            orch,
            slug,
            uri,
            row.exit_code,
            runtime_secs,
            row.output_tail.as_deref(),
        )
        .await?;
        return Ok(TerminalWakeSubscriptionOutcome::ExitAlreadyQueued);
    }

    let live_session_id = session_id.or_else(|| row.and_then(|row| row.session_id.as_deref()));
    if let Some(session_id) = live_session_id {
        match register_terminal_output_watcher(orch, session_id, &sub.id, job_id, phrase, uri) {
            OutputWatchRegistration::AlreadyPresent { excerpt } => {
                crate::orchestrator::wakes::route_terminal_output_async(
                    orch,
                    job_id,
                    slug,
                    uri,
                    phrase,
                    excerpt.as_deref(),
                )
                .await?;
                return Ok(TerminalWakeSubscriptionOutcome::OutputAlreadyQueued);
            }
            OutputWatchRegistration::Registered => {
                return Ok(TerminalWakeSubscriptionOutcome::OutputRegistered);
            }
            OutputWatchRegistration::NotLive => {}
        }
    }

    Ok(TerminalWakeSubscriptionOutcome::OutputPersisted)
}

/// Whether a terminal with this slug exists in any state (running or exited).
/// The promote `run-N` auto-allocation scan uses this so exited run slugs stay
/// taken and run numbers remain monotonic.
async fn terminal_slug_taken_any(db: &LocalDb, target: &TerminalResourceTarget) -> DbResult<bool> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            match job_id.as_deref() {
                Some(job_id) => terminal_slug_exists_for_job(conn, job_id, &slug).await,
                None => terminal_slug_exists_for_project(conn, &project_id, &slug).await,
            }
        })
    })
    .await
}

fn block_on_background_db<T>(fut: impl std::future::Future<Output = DbResult<T>>) -> DbResult<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(crate::storage::DbError::from)?
        .block_on(fut)
}

/// Context loaded while marking a terminal exited: the row's `created_at` (for
/// runtime) and, for a nonzero exit on a job-owned terminal, the injection
/// target (the owning job's current session + the exit code).
struct TerminalExitContext {
    created_at: Option<i64>,
    injection: Option<(String, i32)>,
}

/// Mark a terminal row exited and retain its output tail (always), and load the
/// context the exit path needs. Supersedes the old delete-on-exit: the row now
/// lingers as `status='exited'` so post-exit reads and already-exited wake
/// subscribes can still see it. Mark-exited is decoupled from the nonzero-only
/// injection-target lookup so the two concerns are independent.
///
/// The UPDATE is gated on `status='running'`: it is the single idempotency guard
/// for finalize. Returns `Ok(None)` when no running row matched (already exited
/// or gone), so every caller — the reader EOF, the promoted watcher, and the
/// external kill entry point — converges here without ever finalizing twice with
/// different codes.
async fn mark_terminal_exited_and_load_context(
    db: Arc<LocalDb>,
    terminal_session_id: String,
    job_id: Option<String>,
    exit_code: Option<i32>,
    output_tail: Option<String>,
) -> DbResult<Option<TerminalExitContext>> {
    db.write(|conn| {
        let terminal_session_id = terminal_session_id.clone();
        let job_id = job_id.clone();
        let output_tail = output_tail.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let transitioned = conn
                .execute(
                    "UPDATE job_terminals
                 SET status = 'exited', exit_code = ?2, exited_at = ?3, output_tail = ?4
                 WHERE session_id = ?1 AND status = 'running'",
                    params![
                        terminal_session_id.as_str(),
                        exit_code.map(|code| code as i64),
                        now,
                        output_tail.as_deref()
                    ],
                )
                .await?;
            if transitioned == 0 {
                return Ok(None);
            }

            let mut rows = conn
                .query(
                    "SELECT created_at FROM job_terminals WHERE session_id = ?1 LIMIT 1",
                    params![terminal_session_id.as_str()],
                )
                .await?;
            let created_at = rows.next().await?.map(|row| row.i64(0)).transpose()?;
            drop(rows);

            // Nonzero-exit injection target: the owning job's current session.
            let injection = match (job_id, exit_code.filter(|code| *code != 0)) {
                (Some(job_id), Some(code)) => {
                    let mut rows = conn
                        .query(
                            "SELECT current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
                            params![job_id.as_str()],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.opt_text(0))
                        .transpose()?
                        .flatten()
                        .map(|session_id| (session_id, code))
                }
                _ => None,
            };

            Ok(Some(TerminalExitContext {
                created_at,
                injection,
            }))
        })
    })
    .await
}

/// Capture the last ~2000 chars of a terminal's buffer for the retained
/// `output_tail` and the wake message.
fn capture_output_tail(buffer: &Arc<Mutex<VecDeque<u8>>>) -> Option<String> {
    let bytes: Vec<u8> = buffer
        .lock()
        .map(|b| b.iter().copied().collect())
        .unwrap_or_default();
    if bytes.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&bytes);
    let chars: Vec<char> = text.chars().collect();
    let start = chars.len().saturating_sub(2000);
    let tail: String = chars[start..].iter().collect();
    if tail.trim().is_empty() {
        None
    } else {
        Some(tail)
    }
}

/// Outcome of registering an output-phrase watcher on a live terminal session.
pub(crate) enum OutputWatchRegistration {
    /// The phrase is already present in the terminal's current buffer; the
    /// caller should fire the wake immediately rather than register a watcher.
    AlreadyPresent { excerpt: Option<String> },
    /// A live watcher was registered; it fires when the phrase next appears.
    Registered,
    /// No live agent PTY session backs this terminal, so its output cannot be
    /// watched (e.g. a promoted-run or already-finalized session).
    NotLive,
}

/// Register a phrase watcher on a live terminal session, after first scanning
/// the session's current output buffer so an already-printed phrase fires
/// immediately instead of waiting for new output.
pub(crate) fn register_terminal_output_watcher(
    orch: &Orchestrator,
    session_id: &str,
    subscription_id: &str,
    job_id: &str,
    phrase: &str,
    terminal_uri: &str,
) -> OutputWatchRegistration {
    let session_arc = {
        let Ok(sessions) = orch.pty_state.sessions.lock() else {
            return OutputWatchRegistration::NotLive;
        };
        match sessions.get(session_id) {
            Some(arc) => arc.clone(),
            None => return OutputWatchRegistration::NotLive,
        }
    };
    let (watchers, buffer) = {
        let Ok(session) = session_arc.lock() else {
            return OutputWatchRegistration::NotLive;
        };
        let Some(watchers) = session.output_watchers.clone() else {
            return OutputWatchRegistration::NotLive;
        };
        (watchers, session.output_buffer.clone())
    };
    if let Some(buffer) = buffer {
        let bytes: Vec<u8> = buffer
            .lock()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        let scan = crate::services::scan_for_phrase("", &text, phrase);
        if scan.matched_excerpt.is_some() {
            return OutputWatchRegistration::AlreadyPresent {
                excerpt: scan.matched_excerpt,
            };
        }
    }
    // Bind to a local so the lock-guard temporary drops here, before the
    // function's `watchers`/`buffer` locals (a tail-position match would keep
    // the guard's borrow alive past their drop — E0597).
    let outcome = match watchers.lock() {
        Ok(mut guard) => {
            // Replace any existing watcher for this subscription (a re-subscribe
            // may change the phrase) so the session never carries a stale one,
            // and a subscribe that lands on a session already hydrated for this
            // same row does not double-register it.
            guard.retain(|w| w.subscription_id != subscription_id);
            guard.push(crate::services::TerminalOutputWatcher {
                subscription_id: subscription_id.to_string(),
                job_id: job_id.to_string(),
                phrase: phrase.to_string(),
                carry: String::new(),
                terminal_uri: terminal_uri.to_string(),
            });
            OutputWatchRegistration::Registered
        }
        Err(_) => OutputWatchRegistration::NotLive,
    };
    outcome
}

async fn lookup_terminal_session_id_for_target(
    db: &LocalDb,
    target: &TerminalResourceTarget,
) -> DbResult<Option<String>> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = if let Some(job_id) = job_id {
                conn.query(
                    "
                    SELECT session_id
                    FROM job_terminals
                    WHERE job_id = ?1 AND slug = ?2
                    LIMIT 1
                    ",
                    (job_id.as_str(), slug.as_str()),
                )
                .await?
            } else {
                conn.query(
                    "
                    SELECT session_id
                    FROM job_terminals
                    WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
                    LIMIT 1
                    ",
                    (project_id.as_str(), slug.as_str()),
                )
                .await?
            };
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn promote_to_terminal(
    orch: &Orchestrator,
    ctx: &super::RunContext,
    cwd: &str,
    command: &str,
    child: Arc<Mutex<Box<dyn crate::services::ChildProcess>>>,
    output_buffer: Arc<Mutex<VecDeque<u8>>>,
    last_output_at: Option<Arc<Mutex<SystemTime>>>,
    inline_command_id: &str,
) -> Result<PromotedTerminal, String> {
    let mut target = TerminalResourceTarget {
        slug: String::new(),
        job_id: Some(ctx.job_id.clone()),
        project_id: ctx.project_id.clone(),
        run_id: Some(ctx.run_id.clone()),
        cwd: cwd.to_string(),
    };

    // Auto slug: run-1, run-2, ... until one is free in this job's scope.
    let mut slug = String::new();
    for n in 1..=10_000 {
        let candidate = format!("run-{n}");
        target.slug = candidate.clone();
        // Any-state check: an exited run slug stays taken so run numbers remain
        // monotonic across promotions within a job.
        if !terminal_slug_taken_any(&orch.db.local, &target)
            .await
            .unwrap_or(true)
        {
            slug = candidate;
            break;
        }
    }
    if slug.is_empty() {
        return Err("could not allocate a terminal slug".to_string());
    }

    let session_id = ids::mint_session_id().into_string();
    insert_terminal_resource(&orch.db.local, &target, &session_id, command, None)
        .await
        .map_err(|e| format!("Database error: {e}"))?;

    // A pipe-backed session: no PTY master/writer (input/resize unavailable),
    // while exit tracking and buffer reads work the same as any terminal.
    let session = PtySession {
        master: None,
        writer: None,
        child: Box::new(crate::services::InlineTerminalChild::new(child.clone())),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
        last_output_at,
        // Agent/promoted sessions are non-interactive: no prompt markers, no busy signal.
        command_state: None,
        // Promoted runs have no chunked read loop to scan; output phrase wakes
        // are an agent-PTY-terminal feature only.
        output_watchers: None,
    };
    {
        let mut sessions = orch
            .pty_state
            .sessions
            .lock()
            .map_err(|e| format!("Failed to store session: {e}"))?;
        sessions.insert(session_id.clone(), Arc::new(Mutex::new(session)));
    }

    // Ownership of the process has moved to the terminal; stop the inline
    // interrupt bookkeeping for it.
    orch.pty_state
        .unregister_inline_command(&ctx.run_id, inline_command_id);

    // Round-trips for a top-level node; a sub-task job still gets a readable,
    // killable terminal via the UI and job teardown.
    let uri = build_promoted_terminal_uri(orch, ctx, &slug).await;

    let wake_subscribed = subscribe_promoted_terminal_exit_wake(orch, &ctx.job_id, &uri).await;

    // Promoted-process exit watcher: block on the inline child, then run the same
    // finalization a PTY terminal does on EOF. Plain OS thread, so the blocking
    // wait and `block_on_background_db` inside finalize are safe.
    {
        let orch_t = orch.clone();
        let job_id = ctx.job_id.clone();
        let command_t = command.to_string();
        let buffer_t = output_buffer.clone();
        let watch_child = child.clone();
        let sid = session_id.clone();
        let process_ref = slug.clone();
        let detail_uri = uri.clone();
        thread::spawn(move || {
            let mut tc = crate::services::InlineTerminalChild::new(watch_child);
            let _ = tc.wait();
            let exit_code = tc.try_wait_exit();
            finalize_terminal_session(
                &orch_t,
                &sid,
                exit_code,
                &buffer_t,
                &command_t,
                Some(job_id),
                &process_ref,
                &detail_uri,
            );
        });
    }

    // Surface the new terminal to the UI.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );
    let _ = orch.services.emitter.emit(
        "agent-terminal-created",
        serde_json::to_value(AgentTerminalCreatedPayload {
            run_id: ctx.run_id.clone(),
            session_id,
            command: command.to_string(),
            description: None,
        })
        .unwrap_or_default(),
    );

    Ok(PromotedTerminal {
        slug,
        uri,
        wake_subscribed,
    })
}

async fn subscribe_promoted_terminal_exit_wake(
    orch: &Orchestrator,
    job_id: &str,
    terminal_uri: &str,
) -> bool {
    match subscribe_terminal_exit_wake_once(orch, job_id, terminal_uri, terminal_uri, None, "agent")
        .await
    {
        Ok(_) => true,
        Err(error) => {
            log::warn!(
                "failed to subscribe promoted terminal exit wake for {terminal_uri}: {error}"
            );
            false
        }
    }
}

/// Build the canonical readable/killable URI for a promoted terminal.
async fn build_promoted_terminal_uri(
    orch: &Orchestrator,
    ctx: &super::RunContext,
    slug: &str,
) -> String {
    if let (Some(num), Some(seq)) = (ctx.issue_number, ctx.exec_seq) {
        if let Some(parent_segment) =
            crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
        {
            if let Some(task_segment) =
                crate::jobs::queries::task_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
            {
                return cairn_common::uri::build_task_terminal_uri(
                    &ctx.project_key,
                    num,
                    seq,
                    &parent_segment,
                    &task_segment,
                    slug,
                );
            }
        }
        if let Some(segment) =
            crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
        {
            return cairn_common::uri::build_node_terminal_uri(
                &ctx.project_key,
                num,
                seq,
                &segment,
                slug,
            );
        }
    }
    cairn_common::uri::build_project_terminal_uri(&ctx.project_key, slug)
}
/// Remove a dead terminal session from the live PTY map without killing (the
/// child has already exited). Idempotent: removing an absent session no-ops.
fn remove_pty_session(pty_state: &crate::services::PtyState, session_id: &str) {
    if let Ok(mut sessions) = pty_state.sessions.lock() {
        sessions.remove(session_id);
    }
}

/// The single terminal end-of-life sink. Attempts the conditional mark-exited
/// first; on a real `running`→`exited` transition it emits `pty-exit` +
/// `db-change`, routes the rich `terminal_exit` wake, runs nonzero-exit
/// injection, and finally drops the dead session from the live map. When no row
/// transitioned (already exited, or gone) it is a pure no-op beyond dropping the
/// session — no second `pty-exit`, wake, or injection. This makes finalize
/// idempotent across every caller (reader EOF, promoted watcher, external kill,
/// fence-deny, reader-error) at the DB boundary.
#[allow(clippy::too_many_arguments)]
fn finalize_terminal_session(
    orch: &Orchestrator,
    session_id: &str,
    exit_code: Option<i32>,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
    job_id: Option<String>,
    process_ref: &str,
    detail_uri: &str,
) {
    let output_tail = capture_output_tail(buffer);

    let context = match block_on_background_db(mark_terminal_exited_and_load_context(
        orch.db.local.clone(),
        session_id.to_string(),
        job_id,
        exit_code,
        output_tail.clone(),
    )) {
        Ok(Some(context)) => context,
        // No running row matched: another path already finalized (or the row is
        // gone). Stay idempotent — drop the dead session and return without a
        // second pty-exit/wake/injection.
        Ok(None) => {
            remove_pty_session(&orch.pty_state, session_id);
            return;
        }
        Err(error) => {
            log::warn!("failed to mark terminal exited for {process_ref}: {error}");
            remove_pty_session(&orch.pty_state, session_id);
            return;
        }
    };

    let _ = orch.services.emitter.emit(
        "pty-exit",
        serde_json::to_value(PtyExitPayload {
            session_id: session_id.to_string(),
            exit_code,
        })
        .unwrap_or_default(),
    );

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );

    let runtime_secs = context
        .created_at
        .map(|created| (chrono::Utc::now().timestamp() - created).max(0));

    if let Err(error) = crate::orchestrator::wakes::route_terminal_exit(
        orch,
        process_ref,
        detail_uri,
        exit_code,
        runtime_secs,
        output_tail.as_deref(),
    ) {
        log::warn!("failed to route terminal-exit wake for {process_ref}: {error}");
    }

    if let Some((injection_session_id, code)) = context.injection {
        send_terminal_exit_context(
            orch,
            &injection_session_id,
            session_id,
            code,
            buffer,
            command,
        );
    }

    // The process is dead and finalize has captured everything it needs from the
    // session (buffer/exit were passed in). Drop it from the live map.
    remove_pty_session(&orch.pty_state, session_id);
}

/// Exit code recorded for a terminal we intentionally killed: the SIGKILL
/// convention (128 + 9). A killed-mid-run command must never read as success.
const KILLED_EXIT_CODE: i32 = 137;

/// The fields finalize-by-session needs from a `job_terminals` row.
struct TerminalFinalizeRow {
    status: String,
    job_id: Option<String>,
    project_id: Option<String>,
    slug: Option<String>,
    command: String,
}

async fn load_terminal_finalize_row(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Option<TerminalFinalizeRow>> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT status, job_id, project_id, slug, command
         FROM job_terminals WHERE session_id = ?1 LIMIT 1",
        params![session_id.as_str()],
        |row| {
            Ok(TerminalFinalizeRow {
                status: row.text(0)?,
                job_id: row.opt_text(1)?,
                project_id: row.opt_text(2)?,
                slug: row.opt_text(3)?,
                command: row.text(4)?,
            })
        },
    )
    .await
}

/// Reconstruct a terminal's canonical detail URI from its row. Mirrors
/// `build_promoted_terminal_uri` and the subscribe path (`dispatch.rs`) so the
/// `terminal_exit` wake's match key matches what a subscriber stored, without a
/// stored-URI column: job-owned with a resolvable issue/exec/node → node URI;
/// otherwise the project URI.
async fn build_terminal_detail_uri(
    orch: &Orchestrator,
    job_id: Option<&str>,
    project_id: Option<&str>,
    slug: &str,
) -> String {
    if let Some(job_id) = job_id {
        if let Some((project_key, number, exec_seq)) =
            load_job_uri_parts(&orch.db.local, job_id).await
        {
            if let Some(parent_segment) =
                crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, job_id).await
            {
                if let Some(task_segment) =
                    crate::jobs::queries::task_uri_segment_for_job(&orch.db.local, job_id).await
                {
                    return cairn_common::uri::build_task_terminal_uri(
                        &project_key,
                        number,
                        exec_seq,
                        &parent_segment,
                        &task_segment,
                        slug,
                    );
                }
            }
            if let Some(segment) =
                crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, job_id).await
            {
                return cairn_common::uri::build_node_terminal_uri(
                    &project_key,
                    number,
                    exec_seq,
                    &segment,
                    slug,
                );
            }
            return cairn_common::uri::build_project_terminal_uri(&project_key, slug);
        }
    }
    if let Some(project_id) = project_id {
        if let Some(project_key) = load_project_key(&orch.db.local, project_id).await {
            return cairn_common::uri::build_project_terminal_uri(&project_key, slug);
        }
    }
    cairn_common::uri::build_project_terminal_uri(project_id.unwrap_or_default(), slug)
}

async fn load_job_uri_parts(db: &LocalDb, job_id: &str) -> Option<(String, i32, i32)> {
    let job_id = job_id.to_string();
    db.query_opt(
        "SELECT p.key, i.number, e.seq
         FROM jobs j
         JOIN issues i ON i.id = j.issue_id
         JOIN projects p ON p.id = i.project_id
         LEFT JOIN executions e ON e.id = j.execution_id
         WHERE j.id = ?1 LIMIT 1",
        params![job_id.as_str()],
        |row| Ok((row.text(0)?, row.i64(1)? as i32, row.opt_i64(2)?)),
    )
    .await
    .ok()
    .flatten()
    .and_then(|(key, number, seq)| seq.map(|seq| (key, number, seq as i32)))
}

async fn load_project_key(db: &LocalDb, project_id: &str) -> Option<String> {
    db.query_opt_text(
        "SELECT key FROM projects WHERE id = ?1 LIMIT 1",
        params![project_id.to_string()],
    )
    .await
    .ok()
    .flatten()
}

/// Take a live session out of the PTY map, capture its buffer, kill the child,
/// and derive an honest non-success exit code. `try_wait_exit` can report `0`
/// (or `None`) for a signalled child — portable_pty drops the signal and std's
/// `ExitStatus::code()` is `None` on signal death — so any zero/unknown is
/// normalized to the SIGKILL convention. Neither the row's `exit_code` nor the
/// wake message can then read as success for a killed terminal.
fn take_and_kill_session(
    pty_state: &crate::services::PtyState,
    session_id: &str,
) -> (Arc<Mutex<VecDeque<u8>>>, Option<i32>) {
    let session_arc = pty_state
        .sessions
        .lock()
        .ok()
        .and_then(|mut sessions| sessions.remove(session_id));
    let empty = || Arc::new(Mutex::new(VecDeque::new()));
    match session_arc {
        Some(arc) => match arc.lock() {
            Ok(mut session) => {
                let buffer = session.output_buffer.clone().unwrap_or_else(empty);
                let _ = session.child.kill();
                let _ = session.child.wait();
                let exit_code = session
                    .child
                    .try_wait_exit()
                    .filter(|code| *code != 0)
                    .or(Some(KILLED_EXIT_CODE));
                (buffer, exit_code)
            }
            Err(_) => (empty(), Some(KILLED_EXIT_CODE)),
        },
        None => (empty(), Some(KILLED_EXIT_CODE)),
    }
}

/// Converge an externally-triggered terminal end-of-life (UI tab close, resource
/// "stop", session hard kill) on the single finalize sink: kill the live child,
/// mark the row exited with an honest non-success code, route the exit wake, and
/// retain the row. No-ops when the row is missing or already exited — the reader
/// EOF that follows the kill hits the conditional UPDATE (0 rows) and no-ops too.
/// Deletion is reserved for job teardown.
///
/// The work runs on a dedicated plain thread because finalize blocks on a fresh
/// current-thread runtime, which panics on any thread that carries a tokio
/// context (a `spawn_blocking` worker or a runtime thread). Callers come from
/// both sync and async contexts, so this isolates uniformly. Async callers
/// should still wrap the call in `spawn_blocking` so the join does not stall an
/// executor worker.
pub fn finalize_terminal_by_session_id(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let orch = orch.clone();
    let session_id = session_id.to_string();
    std::thread::spawn(move || finalize_terminal_by_session_id_inner(&orch, &session_id))
        .join()
        .map_err(|_| "terminal finalize thread panicked".to_string())?
}

fn finalize_terminal_by_session_id_inner(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let loaded = block_on_background_db(async {
        let Some(row) = load_terminal_finalize_row(&orch.db.local, session_id).await? else {
            return Ok(None);
        };
        if row.status != "running" {
            return Ok(None);
        }
        let slug = row.slug.clone().unwrap_or_default();
        let detail_uri = build_terminal_detail_uri(
            orch,
            row.job_id.as_deref(),
            row.project_id.as_deref(),
            &slug,
        )
        .await;
        Ok(Some((row, detail_uri)))
    })
    .map_err(|e: crate::storage::DbError| e.to_string())?;

    let Some((row, detail_uri)) = loaded else {
        return Ok(());
    };

    let (buffer, exit_code) = take_and_kill_session(&orch.pty_state, session_id);
    let process_ref = row.slug.as_deref().unwrap_or("terminal");
    finalize_terminal_session(
        orch,
        session_id,
        exit_code,
        &buffer,
        &row.command,
        row.job_id.clone(),
        process_ref,
        &detail_uri,
    );
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) enum TerminalWakeSpec {
    Exit,
    Output(String),
}

impl TerminalWakeSpec {
    pub(crate) fn dry_run_suffix(&self) -> String {
        match self {
            Self::Exit => " (wake on exit)".to_string(),
            Self::Output(phrase) => format!(" (wake on output \"{phrase}\")"),
        }
    }
}

pub(crate) enum TerminalWakeSubscriptionOutcome {
    ExitSubscribed,
    ExitAlreadyQueued,
    OutputRegistered,
    OutputAlreadyQueued,
    OutputPersisted,
}

#[derive(Clone, Copy)]
enum TerminalSpawnMode {
    Create,
    Respawn,
}

pub(crate) async fn create_terminal_from_resource(
    orch: &Orchestrator,
    resource: &CairnResource,
    command: &str,
    description: Option<&str>,
    wake: Option<TerminalWakeSpec>,
    caller_run_id: Option<&str>,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let subscriber_job_id =
        match (&wake, target.job_id.as_deref()) {
            (Some(_), Some(job_id)) => Some(job_id.to_string()),
            (Some(_), None) => {
                let run_id = caller_run_id.ok_or_else(|| {
                    "payload.wake requires an agent caller when creating a project terminal"
                        .to_string()
                })?;
                Some(job_id_for_run(&orch.db.local, run_id).await?.ok_or_else(|| {
                format!("payload.wake requires an agent caller; no job found for run {run_id}")
            })?)
            }
            (None, _) => None,
        };
    ensure_terminal_slug_available(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = spawn_terminal_session(
        orch,
        resource.clone(),
        target,
        command.to_string(),
        description.map(ToOwned::to_owned),
        TerminalSpawnMode::Create,
    )
    .await?;

    let slug = resource_slug(resource);
    let uri = resource.to_uri();
    let Some(wake) = wake else {
        return Ok(format!("Started terminal {slug}"));
    };
    let job_id = subscriber_job_id.expect("wake subscriber resolved before spawn");
    let row = lookup_terminal_for_wake_by_session_id(&orch.db.local, &session_id)
        .await
        .map_err(|error| error.to_string());
    let wake_result = match (row.as_ref(), &wake) {
        (Ok(row), TerminalWakeSpec::Exit) => {
            subscribe_terminal_exit_wake_once(orch, &job_id, &slug, &uri, row.as_ref(), "agent")
                .await
        }
        (Ok(row), TerminalWakeSpec::Output(phrase)) => {
            subscribe_terminal_output_wake_once(
                orch,
                &job_id,
                &slug,
                &uri,
                phrase,
                row.as_ref(),
                Some(&session_id),
                "agent",
            )
            .await
        }
        (Err(error), _) => Err(error.clone()),
    };

    match (&wake, wake_result) {
        (
            TerminalWakeSpec::Exit,
            Ok(
                TerminalWakeSubscriptionOutcome::ExitSubscribed
                | TerminalWakeSubscriptionOutcome::ExitAlreadyQueued,
            ),
        ) => Ok(format!(
            "Started terminal {slug}; subscribed to exit — end your turn to resume when it finishes ({uri})"
        )),
        (TerminalWakeSpec::Output(phrase), Ok(TerminalWakeSubscriptionOutcome::ExitAlreadyQueued)) => Ok(format!(
            "Started terminal {slug}; it already exited before \"{phrase}\" appeared, so resume is queued for turn end ({uri})"
        )),
        (TerminalWakeSpec::Output(phrase), Ok(TerminalWakeSubscriptionOutcome::OutputRegistered | TerminalWakeSubscriptionOutcome::OutputPersisted)) => Ok(format!(
            "Started terminal {slug}; watching output for \"{phrase}\" (also wakes on exit) — end your turn to resume when it appears ({uri})"
        )),
        (TerminalWakeSpec::Output(phrase), Ok(TerminalWakeSubscriptionOutcome::OutputAlreadyQueued)) => Ok(format!(
            "Started terminal {slug}; output \"{phrase}\" already appeared, so resume is queued for turn end ({uri})"
        )),
        (_, Err(error)) => Ok(format!(
            "Started terminal {slug}; automatic wake could not be registered ({error}). Subscribe via cairn:~/wakes: {{subscribe:{{kind:\"terminal\",ref:\"{uri}\",on:{}}}}}",
            match wake {
                TerminalWakeSpec::Exit => "\"exit\"".to_string(),
                TerminalWakeSpec::Output(phrase) => format!("\"output\",phrase:\"{phrase}\""),
            }
        )),
        _ => Ok(format!(
            "Started terminal {slug}; automatic wake registered ({uri})"
        )),
    }
}

async fn job_id_for_run(db: &LocalDb, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            row.opt_text(0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

fn resource_slug(resource: &CairnResource) -> String {
    match resource {
        CairnResource::NodeTerminal { slug, .. }
        | CairnResource::ProjectTerminal { slug, .. }
        | CairnResource::TaskTerminal { slug, .. } => slug.clone(),
        _ => "terminal".to_string(),
    }
}

#[allow(clippy::too_many_lines)]
async fn spawn_terminal_session(
    orch: &Orchestrator,
    resource: CairnResource,
    target: TerminalResourceTarget,
    command: String,
    description: Option<String>,
    mode: TerminalSpawnMode,
) -> Result<String, String> {
    let services = &orch.services;
    let pty_state = &orch.pty_state;

    let pair = services
        .pty_factory
        .create_pty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|e| format!("Failed to create PTY: {e}"))?;

    let shell_path = get_default_shell();

    // Sandbox the interactive shell for worktree agents. On macOS the shell runs
    // under `sandbox-exec`; on Linux Landlock needs a `pre_exec` hook and the PTY
    // spawning backend does not expose one, so terminal async fence detection is
    // macOS-only until PTY spawning can apply Landlock before exec.
    let sandbox = build_run_sandbox_policy(
        orch,
        &target.cwd,
        target.run_id.as_deref(),
        Some(target.project_id.as_str()),
        Some(&command),
    )
    .await;
    let fence_mode = sandbox.as_ref().map(|(_, fence)| *fence);
    let sandbox_applied = sandbox.is_some() && cfg!(target_os = "macos");
    let sandbox_policy = sandbox.map(|(policy, _)| policy);
    let (shell_program, shell_args): (String, Vec<String>) = match &sandbox_policy {
        Some(policy) if cfg!(target_os = "macos") => sandbox::wrap_argv(&shell_path, &[], policy),
        Some(_) => {
            log::warn!(
                "PTY terminal not OS-sandboxed on this platform (cwd={}); inline run is confined",
                target.cwd
            );
            (shell_path.clone(), Vec::new())
        }
        None => (shell_path.clone(), Vec::new()),
    };

    let spawn_started = SystemTime::now();
    let mut cmd = CommandBuilder::new(&shell_program);
    for arg in &shell_args {
        cmd.arg(arg);
    }
    cmd.cwd(&target.cwd);
    for (key, value) in std::env::vars() {
        cmd.env(key, value);
    }
    // Strip a host dev-instance's shared build-target routing so this worktree
    // shell builds into its own target dir (CAIRN-2533).
    scrub_dev_instance_routing_pty(&mut cmd);
    cmd.env("PATH", crate::env::agent_shell_path());
    apply_non_interactive_pager_env_to_pty(&mut cmd);
    // Same jj-only worktree VCS env as the inline spawn path, applied to the PTY /
    // background-terminal spawn too (the background-terminal regression hides if
    // only the inline path is patched). Empty for a non-worktree cwd.
    for (k, v) in crate::mcp::vcs::worktree_shell_vcs_env(orch, std::path::Path::new(&target.cwd)) {
        cmd.env(k, v);
    }
    cmd.env("CAIRN_WORKTREE", &target.cwd);
    cmd.env(
        "CAIRN_CALLBACK_URL",
        format!("http://127.0.0.1:{}/api/mcp", orch.mcp_callback_port),
    );
    // Shared per-home uv package cache (`<cairn_home>/uv-cache`); same rationale
    // as the inline `run` path (warm shared cache in a fence-permitted location).
    cmd.env("UV_CACHE_DIR", crate::env::uv_cache_dir());
    if let Ok(secret) = orch.mcp_auth.get_secret_for_mcp() {
        cmd.env("CAIRN_MCP_SECRET", secret);
    }
    if let Some(run_id) = target.run_id.as_deref() {
        cmd.env("CAIRN_RUN_ID", run_id);
    }
    // Managed Build Services: the interactive PTY shell (e.g. the Dev terminal
    // running `bun run build:cmd`) is fenced on macOS, so inject the build-
    // service client env + CAIRN_SANDBOXED here too. The PTY path uses
    // CommandBuilder, not the process spawner, so CAIRN_SANDBOXED is set
    // explicitly rather than by `build_command`.
    if sandbox_applied {
        cmd.env("CAIRN_SANDBOXED", "1");
        for (k, v) in orch.build_service_client_env(Some(std::path::Path::new(&target.cwd))) {
            cmd.env(k, v);
        }
    }

    let components = pair
        .spawn_and_split(cmd)
        .map_err(|e| format!("Failed to spawn shell: {e}"))?;
    let mut reader = components.reader;
    let child_pid = components.child.process_id();
    let session_id = ids::mint_session_id().into_string();
    let output_buffer: Arc<Mutex<VecDeque<u8>>> = Arc::new(Mutex::new(VecDeque::new()));
    let last_output_at: Arc<Mutex<SystemTime>> = Arc::new(Mutex::new(SystemTime::now()));
    // Shared phrase-watcher registry: the read loop below scans output against it,
    // and the wake-subscribe path registers watchers into the same `Arc` while the
    // terminal runs.
    let output_watchers: Arc<Mutex<Vec<crate::services::TerminalOutputWatcher>>> =
        Arc::new(Mutex::new(Vec::new()));
    let session = PtySession {
        master: Some(components.master),
        writer: Some(components.writer),
        child: Box::new(crate::services::PortableTerminalChild::new(
            components.child,
        )),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
        last_output_at: Some(last_output_at.clone()),
        // Agent terminals are non-interactive: no prompt markers, no busy signal.
        command_state: None,
        output_watchers: Some(output_watchers.clone()),
    };
    let session_arc = Arc::new(Mutex::new(session));

    {
        let mut sessions = pty_state
            .sessions
            .lock()
            .map_err(|e| format!("Failed to store session: {e}"))?;
        sessions.insert(session_id.clone(), session_arc.clone());
    }

    // Hydrate persisted output-phrase watchers for this terminal so an output
    // wake is durable across sessions: a respawn (e.g. after a worktree-fence
    // approval restarts the terminal) and a subscribe made while no session was
    // live both re-attach their watchers here. The wake_subscriptions row is the
    // source of truth; this in-memory list is only the per-session cache the
    // read loop scans.
    {
        let detail_uri = resource.to_uri();
        match crate::orchestrator::wakes::list_terminal_output_watchers(&orch.db.local, &detail_uri)
            .await
        {
            Ok(persisted) if !persisted.is_empty() => {
                if let Ok(mut guard) = output_watchers.lock() {
                    for (subscription_id, watcher_job_id, phrase, terminal_uri) in persisted {
                        guard.push(crate::services::TerminalOutputWatcher {
                            subscription_id,
                            job_id: watcher_job_id,
                            phrase,
                            carry: String::new(),
                            terminal_uri,
                        });
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                log::warn!("failed to hydrate terminal output watchers: {error}")
            }
        }
    }

    let db_result = match mode {
        TerminalSpawnMode::Create => {
            insert_terminal_resource(
                &orch.db.local,
                &target,
                &session_id,
                &command,
                description.as_deref(),
            )
            .await
        }
        TerminalSpawnMode::Respawn => {
            update_terminal_resource_session(&orch.db.local, &target, &session_id).await
        }
    };
    if let Err(e) = db_result {
        if let Ok(mut sessions) = pty_state.sessions.lock() {
            sessions.remove(&session_id);
        }
        if let Ok(mut session) = session_arc.lock() {
            let _ = session.child.kill();
            let _ = session.child.wait();
        }
        return Err(format!("Database error: {e}"));
    }

    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );

    let sid = session_id.clone();
    let orch_t = orch.clone();
    let emitter = services.emitter.clone();
    let buffer = output_buffer.clone();
    let last_output_thread = last_output_at.clone();
    let job_id = target.job_id.clone();
    let command_for_thread = command.clone();
    let target_for_thread = target.clone();
    let resource_for_thread = resource.clone();
    let description_for_thread = description.clone();
    let watchers_for_thread = output_watchers.clone();
    let fence_handled = Arc::new(AtomicBool::new(false));
    let suppress_cleanup = Arc::new(AtomicBool::new(false));
    let fence_handled_thread = fence_handled.clone();
    let suppress_cleanup_thread = suppress_cleanup.clone();

    // Tee terminal output to a per-job scratch log so the full history survives
    // past the 64KB in-memory ring buffer, both live and after exit. Shared so
    // every writer of terminal output lands in it: the reader thread's PTY chunks
    // AND the fence-prompt thread's synthetic Cairn messages. Because the tee
    // lives inside `emit_terminal_data`, the log stays an exact mirror of the ring
    // buffer (and thus `output_tail`), so a post-exit read shows what a live read
    // displayed — fence denials and restart notices included, not just raw PTY
    // bytes. Job-scoped agent terminals only; a project terminal (job_id None) has
    // no log. Keyed by slug, so a fence respawn appends into one file separated by
    // a session marker.
    let terminal_log =
        Arc::new(Mutex::new(target.job_id.as_deref().and_then(|job_id| {
            crate::scratch::TerminalLog::open(job_id, &target.slug)
        })));

    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    if suppress_cleanup_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    let exit_code = get_exit_code_from_session(&orch_t.pty_state, &sid);
                    let process_ref = resource_slug(&resource_for_thread);
                    let detail_uri = resource_for_thread.to_uri();
                    finalize_terminal_session(
                        &orch_t,
                        &sid,
                        exit_code,
                        &buffer,
                        &command_for_thread,
                        job_id.clone(),
                        &process_ref,
                        &detail_uri,
                    );
                    break;
                }
                Ok(n) => {
                    let data = String::from_utf8_lossy(&buf[..n]).to_string();
                    emit_terminal_data(&*emitter, &sid, &data, &buffer, &terminal_log);
                    if let Ok(mut last) = last_output_thread.lock() {
                        *last = SystemTime::now();
                    }
                    crate::orchestrator::wakes::scan_and_route_terminal_output(
                        &orch_t,
                        &watchers_for_thread,
                        &data,
                    );
                    if should_handle_terminal_denial(sandbox_applied, fence_mode, &data)
                        && !fence_handled_thread.swap(true, Ordering::SeqCst)
                    {
                        let crossing =
                            terminal_denial_crossing(child_pid, spawn_started, &command_for_thread);
                        match fence_mode {
                            Some(Fence::Deny) => {
                                emit_terminal_data(
                                    &*emitter,
                                    &sid,
                                    &format!(
                                        "\r\nDenied by agent fence policy (fence: deny): {}\r\n",
                                        crossing.summary
                                    ),
                                    &buffer,
                                    &terminal_log,
                                );
                                suppress_cleanup_thread.store(true, Ordering::SeqCst);
                                // Kill the denied process, then converge on
                                // finalize so the row is marked exited (denied →
                                // exit code unknown) and retained, not deleted.
                                // The buffer is an independent Arc, so the tail
                                // survives the session removal terminate performs.
                                let process_ref = resource_slug(&resource_for_thread);
                                let detail_uri = resource_for_thread.to_uri();
                                terminate_pty_session(&orch_t, &sid);
                                finalize_terminal_session(
                                    &orch_t,
                                    &sid,
                                    None,
                                    &buffer,
                                    &command_for_thread,
                                    job_id.clone(),
                                    &process_ref,
                                    &detail_uri,
                                );
                                break;
                            }
                            Some(Fence::Ask) => {
                                emit_terminal_data(
                                    &*emitter,
                                    &sid,
                                    "\r\nTerminal is waiting for worktree-fence approval. The command will restart if allowed for the session.\r\n",
                                    &buffer,
                                    &terminal_log,
                                );
                                suppress_cleanup_thread.store(true, Ordering::SeqCst);
                                let orch_bg = orch_t.clone();
                                let sid_bg = sid.clone();
                                let buffer_bg = buffer.clone();
                                let target_bg = target_for_thread.clone();
                                let resource_bg = resource_for_thread.clone();
                                let command_bg = command_for_thread.clone();
                                let description_bg = description_for_thread.clone();
                                let log_bg = terminal_log.clone();
                                thread::spawn(move || {
                                    handle_terminal_fence_prompt(
                                        orch_bg,
                                        sid_bg,
                                        buffer_bg,
                                        target_bg,
                                        resource_bg,
                                        command_bg,
                                        description_bg,
                                        crossing,
                                        log_bg,
                                    );
                                });
                                terminate_pty_session(&orch_t, &sid);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    if suppress_cleanup_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    log::error!("Agent PTY read error: {e}");
                    // Converge on finalize (emits pty-exit, marks exited with
                    // exit code unknown, routes the wake, drops the session)
                    // rather than a bare delete.
                    let process_ref = resource_slug(&resource_for_thread);
                    let detail_uri = resource_for_thread.to_uri();
                    finalize_terminal_session(
                        &orch_t,
                        &sid,
                        None,
                        &buffer,
                        &command_for_thread,
                        job_id.clone(),
                        &process_ref,
                        &detail_uri,
                    );
                    break;
                }
            }
        }
        // Flush on every end-of-life path (EOF, error, fence deny/ask) before the
        // exit callback fires.
        if let Ok(mut guard) = terminal_log.lock() {
            if let Some(log) = guard.as_mut() {
                log.flush();
            }
        }
    });

    // Wrap so the shell exits with the command: EOF (which drives
    // `finalize_terminal_session`) then coincides with command completion and the
    // shell's exit code equals the command's. Respawn-after-fence-approval routes
    // through this same site, so it stays consistent.
    write_terminal_input_by_session(orch, &session_id, &submit_command_exiting_shell(&command))
        .await?;

    if let Some(run_id) = target.run_id.clone() {
        let _ = services.emitter.emit(
            "agent-terminal-created",
            serde_json::to_value(AgentTerminalCreatedPayload {
                run_id,
                session_id: session_id.clone(),
                command: command.to_string(),
                description,
            })
            .unwrap_or_default(),
        );
    }

    Ok(session_id)
}

fn emit_terminal_data(
    emitter: &dyn crate::services::EventEmitter,
    session_id: &str,
    data: &str,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    log: &Arc<Mutex<Option<crate::scratch::TerminalLog>>>,
) {
    let _ = emitter.emit(
        "pty-data",
        serde_json::to_value(PtyDataPayload {
            session_id: session_id.to_string(),
            data: data.to_string(),
        })
        .unwrap_or_default(),
    );
    if let Ok(mut buf_guard) = buffer.lock() {
        buf_guard.extend(data.as_bytes());
        while buf_guard.len() > MAX_BUFFER_SIZE {
            buf_guard.pop_front();
        }
    }
    // Tee the same bytes the ring buffer received to the persisted log, so a
    // post-exit read shows exactly what a live read displayed — including
    // synthetic Cairn messages (fence denials, restart notices), not just raw PTY
    // output. No-op when the terminal has no log (project terminal, or the file
    // could not be opened).
    if let Ok(mut guard) = log.lock() {
        if let Some(terminal_log) = guard.as_mut() {
            terminal_log.append(data.as_bytes());
        }
    }
}

pub(super) fn should_handle_terminal_denial(
    sandbox_applied: bool,
    fence: Option<Fence>,
    data: &str,
) -> bool {
    cfg!(target_os = "macos")
        && sandbox_applied
        && matches!(fence, Some(Fence::Ask | Fence::Deny))
        && sandbox::has_denial_signature(data)
}

pub(super) fn terminal_denial_crossing(
    child_pid: Option<u32>,
    spawned_at: SystemTime,
    command: &str,
) -> crate::mcp::handlers::fence::Crossing {
    use crate::mcp::handlers::fence::Crossing;
    #[cfg(target_os = "macos")]
    if let Some(path) = child_pid.and_then(|pid| sandbox::macos::detect_violation(pid, spawned_at))
    {
        return Crossing::shell_path(path.as_path(), &path.display().to_string());
    }
    let _ = (child_pid, spawned_at);
    Crossing::shell_command(
        format!("terminal command blocked by the worktree sandbox: {command}"),
        command,
    )
}

fn terminate_pty_session(orch: &Orchestrator, session_id: &str) {
    let session_arc = orch
        .pty_state
        .sessions
        .lock()
        .ok()
        .and_then(|mut sessions| sessions.remove(session_id));
    if let Some(session_arc) = session_arc {
        if let Ok(mut session) = session_arc.lock() {
            let _ = session.child.kill();
            let _ = session.child.wait();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_terminal_fence_prompt(
    orch: Orchestrator,
    old_session_id: String,
    buffer: Arc<Mutex<VecDeque<u8>>>,
    target: TerminalResourceTarget,
    resource: CairnResource,
    command: String,
    description: Option<String>,
    crossing: crate::mcp::handlers::fence::Crossing,
    log: Arc<Mutex<Option<crate::scratch::TerminalLog>>>,
) {
    let Some(run_id) = target.run_id.clone() else {
        return;
    };
    let request_id = match block_on_background_db(async {
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: target.cwd.clone(),
            run_id: Some(run_id.clone()),
            tool: "run".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: Some(format!("terminal-{}", old_session_id)),
        };
        let tool_input = serde_json::json!({
            "kind": crossing.kind.tag(),
            "verb": crossing.verb,
            "descriptor": crossing.descriptor.clone(),
            "summary": crossing.summary.clone(),
            "request": request,
            "origin": "terminal",
        });
        crate::mcp::handlers::permission::create_background_permission_request(
            &orch,
            &run_id,
            &format!("terminal-{}", old_session_id),
            crossing.verb,
            &tool_input,
        )
        .await
        .map_err(crate::storage::DbError::Row)
    }) {
        Ok(id) => id,
        Err(e) => {
            emit_terminal_data(
                &*orch.services.emitter,
                &old_session_id,
                &format!("\r\nFailed to create worktree-fence permission request: {e}\r\n"),
                &buffer,
                &log,
            );
            return;
        }
    };

    let mut rx = orch.permission_responses.subscribe();
    let response = match block_on_background_db(async move {
        loop {
            match rx.recv().await {
                Ok((resp_request_id, response_json)) if resp_request_id == request_id => {
                    return Ok(response_json)
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(crate::storage::DbError::Row(
                        "permission response channel closed".to_string(),
                    ))
                }
            }
        }
    }) {
        Ok(response) => response,
        Err(e) => {
            emit_terminal_data(
                &*orch.services.emitter,
                &old_session_id,
                &format!("\r\nWorktree-fence permission wait failed: {e}\r\n"),
                &buffer,
                &log,
            );
            return;
        }
    };

    let allowed = serde_json::from_str::<serde_json::Value>(&response)
        .ok()
        .and_then(|v| {
            v.get("behavior")
                .and_then(|b| b.as_str())
                .map(str::to_string)
        })
        .as_deref()
        == Some("allow");

    if !allowed {
        emit_terminal_data(
            &*orch.services.emitter,
            &old_session_id,
            &format!("\r\nDenied by worktree fence: {}\r\n", crossing.summary),
            &buffer,
            &log,
        );
        // Converge on finalize: mark the row exited (denied → exit code unknown)
        // and route the wake, retaining the row. The session was already removed
        // by the reader thread's terminate before this prompt handler ran, so
        // there is no child to kill here.
        let process_ref = resource_slug(&resource);
        let detail_uri = resource.to_uri();
        finalize_terminal_session(
            &orch,
            &old_session_id,
            None,
            &buffer,
            &command,
            target.job_id.clone(),
            &process_ref,
            &detail_uri,
        );
        return;
    }

    let session_granted = orch
        .session_allowed_crossings
        .lock()
        .ok()
        .is_some_and(|allowed| allowed.contains(&crossing.descriptor));
    if !session_granted {
        emit_terminal_data(
            &*orch.services.emitter,
            &old_session_id,
            "\r\nTerminal worktree-fence approvals must be allowed for the session; not restarting.\r\n",
            &buffer,
            &log,
        );
        // Converge on finalize: mark the row exited (denied → exit code unknown)
        // and route the wake, retaining the row. The session was already removed
        // by the reader thread's terminate before this prompt handler ran, so
        // there is no child to kill here.
        let process_ref = resource_slug(&resource);
        let detail_uri = resource.to_uri();
        finalize_terminal_session(
            &orch,
            &old_session_id,
            None,
            &buffer,
            &command,
            target.job_id.clone(),
            &process_ref,
            &detail_uri,
        );
        return;
    }

    emit_terminal_data(
        &*orch.services.emitter,
        &old_session_id,
        "\r\nWorktree-fence approval granted for this session; restarting terminal.\r\n",
        &buffer,
        &log,
    );
    let respawn = block_on_background_db(async {
        spawn_terminal_session(
            &orch,
            resource,
            target,
            command,
            description,
            TerminalSpawnMode::Respawn,
        )
        .await
        .map_err(crate::storage::DbError::Row)
    });
    if let Err(e) = respawn {
        emit_terminal_data(
            &*orch.services.emitter,
            &old_session_id,
            &format!("\r\nFailed to restart terminal after worktree-fence approval: {e}\r\n"),
            &buffer,
            &log,
        );
    }
}

async fn write_terminal_input_by_session(
    orch: &Orchestrator,
    session_id: &str,
    content: &str,
) -> Result<(), String> {
    let sessions = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| format!("Failed to access sessions: {e}"))?;
    let session = sessions
        .get(session_id)
        .ok_or_else(|| format!("Terminal session not running: {session_id}"))?;
    let mut session = session
        .lock()
        .map_err(|e| format!("Failed to lock terminal session: {e}"))?;
    let writer = session
        .writer
        .as_mut()
        .ok_or_else(|| "This terminal does not accept input".to_string())?;
    writer
        .write_all(content.as_bytes())
        .map_err(|e| format!("Failed to write terminal input: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush terminal input: {e}"))?;
    Ok(())
}

pub async fn append_terminal_input(
    orch: &Orchestrator,
    resource: &CairnResource,
    content: &str,
    submit: bool,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = lookup_terminal_session_id_for_target(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Terminal not found: {}", target.slug))?;

    let sessions = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| format!("Failed to access sessions: {e}"))?;
    let session = sessions
        .get(&session_id)
        .ok_or_else(|| format!("Terminal session not running: {}", target.slug))?;
    let mut session = session
        .lock()
        .map_err(|e| format!("Failed to lock terminal session: {e}"))?;
    let to_write = if submit {
        ensure_submitted_line(content)
    } else {
        std::borrow::Cow::Borrowed(content)
    };
    let writer = session
        .writer
        .as_mut()
        .ok_or_else(|| "This terminal does not accept input".to_string())?;
    writer
        .write_all(to_write.as_bytes())
        .map_err(|e| format!("Failed to write terminal input: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush terminal input: {e}"))?;

    Ok(format!(
        "Sent {} chars to terminal {}",
        to_write.len(),
        target.slug
    ))
}

pub async fn delete_terminal_by_resource(
    orch: &Orchestrator,
    resource: &CairnResource,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = lookup_terminal_session_id_for_target(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Terminal not found: {}", target.slug))?;

    // "Stop" converges on the single finalize sink: kill the child, mark the row
    // exited (honest non-success code) + route the wake, and retain the row.
    // Deletion is reserved for job teardown. Run on a blocking worker so the
    // finalize thread's join does not stall the async executor.
    let orch_for_finalize = orch.clone();
    let session_for_finalize = session_id.clone();
    tokio::task::spawn_blocking(move || {
        finalize_terminal_by_session_id(&orch_for_finalize, &session_for_finalize)
    })
    .await
    .map_err(|e| format!("terminal finalize task failed: {e}"))??;

    Ok(format!("Stopped terminal {}", target.slug))
}

/// Get exit code from PTY session using non-blocking try_wait.
fn get_exit_code_from_session(
    pty_state: &crate::services::PtyState,
    session_id: &str,
) -> Option<i32> {
    let sessions = pty_state.sessions.lock().ok()?;
    let session_arc = sessions.get(session_id)?;
    let mut session = session_arc.lock().ok()?;

    // Use try_wait to avoid blocking - process should have exited if we got EOF
    session.child.try_wait_exit()
}

/// Send terminal context to Claude via stdin when a background process exits with error.
fn send_terminal_exit_context(
    orch: &Orchestrator,
    session_id: &str,
    _terminal_session_id: &str,
    exit_code: i32,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
) {
    // Get recent output from buffer
    let output = {
        let buf_guard = buffer.lock().unwrap();
        let bytes: Vec<u8> = buf_guard.iter().copied().collect();
        String::from_utf8_lossy(&bytes).to_string()
    };

    // Truncate for display
    let cmd_display: String = command.chars().take(100).collect();
    let truncated_output: String = output.chars().take(2000).collect();

    let content = format!(
        "[Terminal Update] Background command '{}' exited with code {}:\n```\n{}\n```",
        cmd_display, exit_code, truncated_output
    );

    // Find the process and send via stdin
    let process_state = orch.process_state.clone();

    let run_id = match process_state.find_process_by_session(session_id) {
        Some(rid) => rid,
        None => {
            log::info!(
                "No active process for session {}, skipping terminal context",
                &session_id[..8.min(session_id.len())]
            );
            return;
        }
    };

    match crate::backends::stdin::send_user_message(
        &process_state,
        &run_id,
        &content,
        session_id,
        None,
        None,
    ) {
        Ok(()) => {
            log::info!(
                "Sent terminal context to session {}: command='{}' exit_code={}",
                &session_id[..8.min(session_id.len())],
                cmd_display,
                exit_code
            );
        }
        Err(e) => {
            log::warn!("Failed to send terminal context: {}", e);
        }
    }
}

#[cfg(test)]
mod terminal_finalize_tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Run an async block on a throwaway current-thread runtime. Each call
    /// creates and drops its own runtime, so the test thread carries no ambient
    /// runtime between calls — finalize's internal `block_on` then runs safely.
    fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',7,'I','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES('e','rec','i','p','running',1,2);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, created_at, updated_at) VALUES('j','p','i','e','builder','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, parent_job_id, uri_segment, status, created_at, updated_at) VALUES('task-j','p','i','e','j','Explore','running',1,1);
            INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('t','j','s1','sleep 100','running',1,'run-1');
            INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('task-t','task-j','task-s1','sleep 100','running',1,'run-1');
            ",
        )
        .await
        .unwrap();
    }

    async fn read_terminal(
        db: &LocalDb,
        session_id: &str,
    ) -> (String, Option<i32>, Option<String>) {
        let session_id = session_id.to_string();
        db.query_opt(
            "SELECT status, exit_code, output_tail FROM job_terminals WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_i64(1)?.map(|v| v as i32),
                    row.opt_text(2)?,
                ))
            },
        )
        .await
        .unwrap()
        .expect("terminal row present")
    }

    fn node_uri() -> String {
        cairn_common::uri::build_node_terminal_uri("P", 7, 2, "builder", "run-1")
    }

    fn task_uri() -> String {
        cairn_common::uri::build_task_terminal_uri("P", 7, 2, "builder", "Explore", "run-1")
    }

    #[test]
    fn finalize_marks_exited_retains_tail_and_is_idempotent() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_a.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let buffer: Arc<Mutex<VecDeque<u8>>> =
            Arc::new(Mutex::new(b"all done\n".iter().copied().collect()));
        let uri = node_uri();
        finalize_terminal_session(
            &orch,
            "s1",
            Some(0),
            &buffer,
            "sleep 100",
            Some("j".to_string()),
            "run-1",
            &uri,
        );

        let (status, code, tail) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status, "exited");
        assert_eq!(code, Some(0));
        assert!(tail.unwrap_or_default().contains("all done"));

        // A second finalize with a different code must not re-transition the row.
        finalize_terminal_session(
            &orch,
            "s1",
            Some(99),
            &buffer,
            "sleep 100",
            Some("j".to_string()),
            "run-1",
            &uri,
        );
        let (status2, code2, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status2, "exited");
        assert_eq!(
            code2,
            Some(0),
            "idempotent finalize must not overwrite the recorded exit code"
        );
    }

    #[test]
    fn mark_terminal_exited_is_conditional_on_running() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_b.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let first = block_on(mark_terminal_exited_and_load_context(
            orch.db.local.clone(),
            "s1".to_string(),
            Some("j".to_string()),
            Some(0),
            Some("tail".to_string()),
        ))
        .unwrap();
        assert!(first.is_some(), "first mark transitions a running row");

        let second = block_on(mark_terminal_exited_and_load_context(
            orch.db.local.clone(),
            "s1".to_string(),
            Some("j".to_string()),
            Some(5),
            Some("tail".to_string()),
        ))
        .unwrap();
        assert!(
            second.is_none(),
            "an already-exited row must not transition again"
        );
    }

    #[test]
    fn build_terminal_detail_uri_matches_node_builder() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_c.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let uri = block_on(build_terminal_detail_uri(&orch, Some("j"), None, "run-1"));
        assert_eq!(uri, node_uri());
    }

    #[test]
    fn build_terminal_detail_uri_matches_task_builder() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_task.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let uri = block_on(build_terminal_detail_uri(
            &orch,
            Some("task-j"),
            None,
            "run-1",
        ));
        assert_eq!(uri, task_uri());
    }

    #[test]
    fn resolve_task_terminal_targets_child_job_and_scopes_slug_collisions() {
        let db = block_on(crate::storage::migrated_test_db("term_resolve_task.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));
        let resource = CairnResource::TaskTerminal {
            project: "P".to_string(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "run-1".to_string(),
        };

        let target = block_on(resolve_terminal_resource_target(&orch.db.local, &resource)).unwrap();
        assert_eq!(target.job_id.as_deref(), Some("task-j"));
        assert_eq!(target.slug, "run-1");
        assert!(block_on(ensure_terminal_slug_available(&orch.db.local, &target)).is_err());

        let parent_resource = CairnResource::NodeTerminal {
            project: "P".to_string(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".to_string(),
            slug: "task-only".to_string(),
        };
        let parent_target = block_on(resolve_terminal_resource_target(
            &orch.db.local,
            &parent_resource,
        ))
        .unwrap();
        assert!(block_on(ensure_terminal_slug_available(
            &orch.db.local,
            &parent_target
        ))
        .is_ok());
    }

    #[test]
    fn finalize_by_session_records_honest_kill_code_and_retains_row() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_d.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        // No live session in the PTY map: finalize-by-session records the SIGKILL
        // convention (never success) and retains the row, not deletes it.
        finalize_terminal_by_session_id(&orch, "s1").unwrap();

        let (status, code, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status, "exited");
        assert_eq!(code, Some(KILLED_EXIT_CODE));

        // Idempotent: a second stop no-ops because the row is no longer running.
        finalize_terminal_by_session_id(&orch, "s1").unwrap();
        let (status2, code2, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status2, "exited");
        assert_eq!(code2, Some(KILLED_EXIT_CODE));
    }

    /// Regression: synthetic Cairn messages (fence denials, restart notices) are
    /// emitted through `emit_terminal_data`, which must tee them into the
    /// persisted log too — otherwise a post-exit read, which prefers the log over
    /// `output_tail`, would drop exactly the diagnostic output that explains a
    /// fence denial.
    #[test]
    fn emit_terminal_data_tees_into_the_log() {
        use crate::services::testing::CapturingEmitter;
        use std::collections::VecDeque;

        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let emitter = CapturingEmitter::new();
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let log = Arc::new(Mutex::new(crate::scratch::TerminalLog::open(
            &job_id, "dev",
        )));

        let denial = "\r\nDenied by agent fence policy (fence: deny): touch /etc/x\r\n";
        emit_terminal_data(&emitter, "sess", denial, &buffer, &log);
        if let Ok(mut guard) = log.lock() {
            if let Some(l) = guard.as_mut() {
                l.flush();
            }
        }

        // Landed in the ring buffer (and hence `output_tail`)...
        let buffered: Vec<u8> = buffer.lock().unwrap().iter().copied().collect();
        assert!(String::from_utf8_lossy(&buffered).contains("Denied by agent fence policy"));
        // ...and in the persisted log, so a post-exit read still shows it.
        let logged = std::fs::read(crate::scratch::terminal_log_path(&job_id, "dev")).unwrap();
        assert!(
            String::from_utf8_lossy(&logged).contains("Denied by agent fence policy"),
            "synthetic fence message must persist to the log"
        );

        crate::scratch::remove_job_scratch_dir(&job_id);
    }
}
