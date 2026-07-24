//! Core facade for the REPL UI surface, mirroring [`terminal_host`](crate::terminal_host).
//!
//! The runner-owned orchestrator holds every live REPL session in its in-memory
//! `repl_state` registry (no DB row — the registry is the single source of
//! truth). These functions expose that registry to the transport command layer:
//! listing live sessions for facet projection and the tab list, replaying an
//! exchange transcript to a newly-opened tab, creating/closing a session from
//! the UI, and sending user code into the shared namespace through the same
//! [`send_recorded`](crate::mcp::handlers::repl::send_recorded) funnel the agent
//! uses — so a user send and an agent send interleave in one namespace and one
//! transcript.

use std::time::Duration;

use crate::mcp::handlers::repl::{self, ReplExchange, ReplInfo, ReplLang, ReplOrigin};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;

/// Live REPL sessions for one job (facet projection + tab list).
pub fn get_job_repls(orch: &Orchestrator, job_id: String) -> Vec<ReplInfo> {
    orch.repl_state.list_for_job(&job_id)
}

/// Live REPL sessions across every job on this host (global facet source).
pub fn get_running_repls(orch: &Orchestrator) -> Vec<ReplInfo> {
    orch.repl_state.list_all()
}

/// The in-memory exchange transcript for a session (oldest first), or empty when
/// the session is unknown to this host (never created here, or lost to a restart
/// or teardown — matching the ephemeral REPL contract).
pub fn get_repl_history(orch: &Orchestrator, job_id: String, slug: String) -> Vec<ReplExchange> {
    orch.repl_state
        .get(&job_id, &slug)
        .map(|session| session.history_snapshot())
        .unwrap_or_default()
}

/// Resolve `(project_id, cwd)` for a job server-side: the worktree checkout when
/// the job runs in one, else the project's live checkout root for an ambient
/// node — the same resolution `create_job_terminal` uses. Never caller-supplied.
async fn resolve_job_cwd(db: &LocalDb, job_id: &str) -> Result<(String, String), String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path)
                     FROM jobs j JOIN projects p ON p.id = j.project_id
                     WHERE j.id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok((row.text(0)?, row.text(1)?)),
                None => Err(DbError::Row(format!("Job not found: {job_id}"))),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Create a REPL from the UI. The cwd/project are resolved server-side from the
/// job (never caller-supplied), so a UI create spawns without a live run. Emits
/// `repl-state` `created` and a system message to the node so the agent learns a
/// user REPL exists (parity with `emit_terminal_created`).
pub async fn create_job_repl(
    orch: &Orchestrator,
    job_id: String,
    slug: String,
    interpreter: ReplLang,
    deps: Vec<String>,
) -> Result<ReplInfo, String> {
    if orch.repl_state.contains(&job_id, &slug) {
        return Err(format!("REPL '{slug}' already exists for this node."));
    }
    let (project_id, cwd) = resolve_job_cwd(&orch.db.local, &job_id).await?;
    let session = repl::spawn_session(
        orch,
        &job_id,
        &project_id,
        &cwd,
        None,
        interpreter,
        &slug,
        &deps,
    )
    .await?;
    // Insert only if the slot is still vacant: an agent or another UI create that
    // landed during the spawn must not have its session orphaned by this one.
    if !orch
        .repl_state
        .insert_if_absent(job_id.clone(), slug.clone(), session.clone())
    {
        session.stop_and_release(orch).await;
        return Err(format!("REPL '{slug}' already exists for this node."));
    }
    repl::emit_repl_state(orch, &job_id, &slug, interpreter, "created");
    crate::messages::system::emit_repl_created(orch, &job_id, &slug, interpreter.label());
    orch.repl_state
        .list_for_job(&job_id)
        .into_iter()
        .find(|info| info.slug == slug)
        .ok_or_else(|| format!("REPL '{slug}' vanished immediately after creation."))
}

/// Close a REPL from the UI: remove, kill, and emit `repl-state` `deleted`.
/// Idempotent — closing an already-gone session is a no-op.
pub async fn close_job_repl(
    orch: &Orchestrator,
    job_id: String,
    slug: String,
) -> Result<(), String> {
    if let Some(session) = orch.repl_state.remove(&job_id, &slug) {
        let interpreter = session.interpreter;
        session.stop_and_release(orch).await;
        repl::emit_repl_state(orch, &job_id, &slug, interpreter, "deleted");
    }
    Ok(())
}

/// Send user code into a live REPL through the shared funnel (origin `User`).
/// Default 120s, capped at 600s. Returns the settled exchange; the funnel has
/// already recorded it, emitted the `repl-exchange` events, and performed any
/// dead/timeout kill.
pub async fn repl_send(
    orch: &Orchestrator,
    job_id: String,
    slug: String,
    code: String,
    timeout_ms: Option<u64>,
) -> Result<ReplExchange, String> {
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(120_000).min(600_000));
    repl::send_recorded(orch, &job_id, &slug, &code, timeout, ReplOrigin::User, None).await
}
