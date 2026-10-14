//! Core facade for the REPL surface, mirroring [`terminal_host`](crate::terminal_host).
//!
//! A REPL is two things at once. Its **namespace** is a live interpreter heap in
//! the runner-owned orchestrator's in-memory `repl_state` registry; its **logical
//! identity** is a durable `job_repls` row carrying the language, the dependency
//! set, the fate of the last process, and a transcript that spans every
//! generation. These functions join the two: listing REPLs (running and exited)
//! for facet projection and the tab list, replaying a transcript into a
//! newly-opened tab, opening or resuming a session, stopping and then discarding
//! one, and sending user code into the shared namespace through the same
//! [`send_recorded`](crate::mcp::handlers::repl::send_recorded) funnel the agent
//! uses — so a user send and an agent send interleave in one namespace and one
//! transcript.

use std::time::Duration;

use crate::mcp::handlers::repl::store::{self, ReplExitReason, ReplListing, ReplRowStatus};
use crate::mcp::handlers::repl::{self, ReplExchange, ReplInfo, ReplLang, ReplOrigin};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;

/// Join a durable listing with the live registry: the row supplies identity and
/// fate, the registry supplies liveness.
fn join_liveness(orch: &Orchestrator, rows: Vec<ReplListing>) -> Vec<ReplInfo> {
    let liveness = orch.repl_state.liveness();
    rows.into_iter()
        .map(|row| {
            let (alive, busy) = liveness
                .get(&(row.job_id.clone(), row.slug.clone()))
                .copied()
                .unwrap_or((false, false));
            ReplInfo {
                job_id: row.job_id,
                slug: row.slug,
                interpreter: row.interpreter.label().to_string(),
                created_at: row.created_at,
                generation: row.generation,
                status: row.status.as_str().to_string(),
                exit_reason: row.exit_reason.map(|reason| reason.as_str().to_string()),
                last_status: row.last_status,
                alive: alive && row.status == ReplRowStatus::Running,
                busy,
            }
        })
        .collect()
}

/// Every REPL belonging to one job (facet projection + tab list), running or not.
pub async fn get_job_repls(orch: &Orchestrator, job_id: String) -> Vec<ReplInfo> {
    let rows = store::list_for_job(&orch.db.local, &job_id)
        .await
        .unwrap_or_default();
    join_liveness(orch, rows)
}

/// Every REPL on this host (global facet source), running or not.
pub async fn get_running_repls(orch: &Orchestrator) -> Vec<ReplInfo> {
    let rows = store::list_all(&orch.db.local).await.unwrap_or_default();
    join_liveness(orch, rows)
}

/// The durable exchange transcript for a REPL (oldest first, spanning every
/// generation), or empty when the slug has no row at all.
pub async fn get_repl_history(
    orch: &Orchestrator,
    job_id: String,
    slug: String,
) -> Vec<ReplExchange> {
    let Ok(Some(record)) = store::load(&orch.db.local, &job_id, &slug).await else {
        return Vec::new();
    };
    store::history(&orch.db.local, &record.id)
        .await
        .unwrap_or_default()
}

/// What opening a REPL actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplOpenKind {
    /// No row existed: generation 1 of a brand-new REPL.
    Created,
    /// An exited row was brought back as the next generation.
    Resumed,
    /// A live session already served this slug; nothing changed.
    AlreadyRunning,
}

/// Result of [`open_repl`]: the listing plus what the call did, so each caller
/// can phrase its own confirmation.
#[derive(Debug, Clone)]
pub struct ReplOpen {
    pub info: ReplInfo,
    pub kind: ReplOpenKind,
}

impl ReplOpen {
    /// The agent-facing confirmation. A resume says plainly that the namespace
    /// starts empty — the transcript survives, the bindings do not, and anything
    /// vaguer would mislead.
    pub fn summary(&self) -> String {
        let ReplOpen { info, kind } = self;
        match kind {
            ReplOpenKind::Created => {
                format!("Started {} REPL {}", info.interpreter, info.slug)
            }
            ReplOpenKind::Resumed => format!(
                "Resumed {} REPL {} as generation {} — the transcript continues, but the \
                 namespace starts EMPTY (nothing is rebound). Re-send the setup you need.",
                info.interpreter, info.slug, info.generation
            ),
            ReplOpenKind::AlreadyRunning => {
                format!(
                    "REPL {} is already running ({})",
                    info.slug, info.interpreter
                )
            }
        }
    }
}

async fn info_for(orch: &Orchestrator, job_id: &str, slug: &str) -> Result<ReplInfo, String> {
    get_job_repls(orch, job_id.to_string())
        .await
        .into_iter()
        .find(|info| info.slug == slug)
        .ok_or_else(|| format!("REPL '{slug}' vanished immediately after creation."))
}

/// Open a REPL: create it, or resume an exited one as the next generation.
///
/// `interpreter` and `deps` are what the caller's payload asked for. Both are
/// inherited from an exited row when omitted, which is what makes resuming
/// ergonomic; a payload naming a *different* interpreter is rejected, because the
/// transcript's language would become incoherent.
#[allow(clippy::too_many_arguments)]
pub async fn open_repl(
    orch: &Orchestrator,
    job_id: &str,
    project_id: &str,
    cwd: &str,
    run_context: Option<&RunContext>,
    slug: &str,
    interpreter: Option<ReplLang>,
    deps: Option<Vec<String>>,
) -> Result<ReplOpen, String> {
    // Creating a generation spans a durable write (`store::begin`, inside
    // `spawn_session`) and a registry claim (`insert_if_absent`). Hold the slug's
    // lifecycle lock across BOTH so they land as one step: otherwise two
    // concurrent opens each see a vacant registry, each bump the row's
    // generation, and only then does one win the slot — leaving the winner's
    // generation disagreeing with the row, and the loser having already mutated
    // an identity it lost.
    let lifecycle = orch.repl_state.lifecycle_lock(job_id, slug);
    let _lifecycle = lifecycle.lock().await;

    if orch.repl_state.contains(job_id, slug) {
        return Ok(ReplOpen {
            info: info_for(orch, job_id, slug).await?,
            kind: ReplOpenKind::AlreadyRunning,
        });
    }
    let existing = store::load(&orch.db.local, job_id, slug)
        .await
        .map_err(|error| error.to_string())?;

    let interpreter = match (interpreter, existing.as_ref()) {
        (Some(requested), Some(record)) if record.interpreter != requested => {
            return Err(format!(
                "REPL '{slug}' is a {} session with {} recorded exchange(s); reopening it as {} \
                 would make its transcript incoherent. Discard it first (write cairn:~/repl/{slug} \
                 mode delete, once to stop and again to remove) or use a different slug.",
                record.interpreter.label(),
                record.exchange_count,
                requested.label()
            ));
        }
        (Some(requested), _) => requested,
        (None, Some(record)) => record.interpreter,
        (None, None) => {
            return Err(
                "payload.interpreter is required (python | typescript) for a new REPL".to_string(),
            )
        }
    };
    let deps = deps.unwrap_or_else(|| {
        existing
            .as_ref()
            .map(|record| record.deps.clone())
            .unwrap_or_default()
    });
    let resumed = existing.is_some();

    let session = repl::spawn_session(
        orch,
        job_id,
        project_id,
        cwd,
        run_context,
        interpreter,
        slug,
        &deps,
    )
    .await?;
    // Insert only if the slot is still vacant: a concurrent create that spawned
    // between the check above and here must not have its session orphaned.
    if !orch
        .repl_state
        .insert_if_absent(job_id.to_string(), slug.to_string(), session.clone())
    {
        // Unreachable while the lifecycle lock is held (the vacancy check above
        // and this claim are one step). Kept as a belt-and-braces guard, and it
        // must undo the generation this spawn opened rather than leave the row
        // claiming a process that is about to be killed.
        let generation = session.generation;
        session.stop_and_release(orch).await;
        let _ = store::mark_exited(
            &orch.db.local,
            &session.repl_id,
            generation,
            ReplExitReason::Closed,
        )
        .await;
        return Ok(ReplOpen {
            info: info_for(orch, job_id, slug).await?,
            kind: ReplOpenKind::AlreadyRunning,
        });
    }
    repl::emit_repl_change(orch, if resumed { "update" } else { "create" });
    Ok(ReplOpen {
        info: info_for(orch, job_id, slug).await?,
        kind: if resumed {
            ReplOpenKind::Resumed
        } else {
            ReplOpenKind::Created
        },
    })
}

/// Resolve the project and recreate the job's scratch process residence. The
/// executor residency resolves repository content from the job coordinate.
async fn resolve_job_residence(db: &LocalDb, job_id: &str) -> Result<(String, String), String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT project_id FROM jobs WHERE id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok((
                    row.text(0)?,
                    crate::scratch::ensure_job_scratch_dir(&job_id, None)
                        .to_string_lossy()
                        .into_owned(),
                )),
                None => Err(DbError::Row(format!("Job not found: {job_id}"))),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Create or resume a REPL from the UI. The cwd/project are resolved server-side
/// from the job (never caller-supplied), so a UI create spawns without a live
/// run, and a system message tells the agent a user REPL exists (parity with
/// `emit_terminal_created`).
pub async fn create_job_repl(
    orch: &Orchestrator,
    job_id: String,
    slug: String,
    interpreter: ReplLang,
    deps: Vec<String>,
) -> Result<ReplInfo, String> {
    let (project_id, cwd) = resolve_job_residence(&orch.db.local, &job_id).await?;
    let opened = open_repl(
        orch,
        &job_id,
        &project_id,
        &cwd,
        None,
        &slug,
        Some(interpreter),
        (!deps.is_empty()).then_some(deps),
    )
    .await?;
    if opened.kind != ReplOpenKind::AlreadyRunning {
        crate::messages::system::emit_repl_created(orch, &job_id, &slug, interpreter.label());
    }
    Ok(opened.info)
}

/// Stop, then discard. Stopping and discarding are different acts now that the
/// transcript outlives the process: the first `delete` kills the interpreter and
/// leaves the REPL readable as `exited`, and a second `delete` removes the row
/// and its transcript. Idempotent at both stages.
pub async fn close_job_repl(
    orch: &Orchestrator,
    job_id: String,
    slug: String,
) -> Result<String, String> {
    // Held across the stop so a resume cannot install the next generation while
    // this close is still tearing the old one down.
    let lifecycle = orch.repl_state.lifecycle_lock(&job_id, &slug);
    let _lifecycle = lifecycle.lock().await;

    if let Some(session) = orch.repl_state.remove(&job_id, &slug) {
        let repl_id = session.repl_id.clone();
        let generation = session.generation;
        session.stop_and_release(orch).await;
        if let Err(error) =
            store::mark_exited(&orch.db.local, &repl_id, generation, ReplExitReason::Closed).await
        {
            tracing::warn!(%error, %slug, "failed to mark REPL exited");
        }
        repl::emit_repl_change(orch, "update");
        return Ok(format!(
            "Stopped REPL {slug}. Its transcript stays readable; delete it again to discard it."
        ));
    }
    match store::load(&orch.db.local, &job_id, &slug)
        .await
        .map_err(|error| error.to_string())?
    {
        Some(record) => {
            store::remove(&orch.db.local, &record.id)
                .await
                .map_err(|error| error.to_string())?;
            repl::emit_repl_change(orch, "delete");
            repl::emit_exchange_change(orch);
            Ok(format!("Removed REPL {slug} and its transcript."))
        }
        None => Ok(format!(
            "No REPL named '{slug}' for this node (already removed or never created)"
        )),
    }
}

/// Startup reap. No interpreter survives a runner restart, so the registry is
/// empty by definition and any `running` row is a lie: mark every one of them
/// exited via `host_restart` and settle the sends that were in flight. This is
/// deliberately simpler than terminal recovery — a recovered REPL process would
/// have an empty namespace and is worth nothing, so there is nothing to
/// reconcile, only to record.
pub async fn reap_orphaned_repls(orch: &Orchestrator) -> Result<u64, String> {
    let reaped = store::reap_orphans(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    if reaped > 0 {
        repl::emit_repl_change(orch, "update");
        repl::emit_exchange_change(orch);
    }
    Ok(reaped)
}

/// Send user code into a live REPL through the shared funnel (origin `User`).
///
/// Default 120s, capped at 600s — an interactive bound for a person watching a
/// cell, deliberately distinct from the run-item bound an agent's `run` send
/// gets (`clamp_run_item_timeout_ms`). A person can re-send; a batch item
/// cannot. Returns the settled exchange; the funnel has already recorded it,
/// emitted its `db-change`s, and performed any dead/timeout kill.
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
