//! Application projections around session-owned run and transcript persistence.
use crate::{
    error::CairnError,
    models::{RunTodos, TodoItem},
    storage::LocalDb,
};
pub use cairn_session::runs::queries::*;
use std::sync::Arc;
pub fn get_job_todos(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<TodoItem>, CairnError> {
    let job_id = job_id.to_string();
    run_query_db(async move {
        crate::todos::get_todos_for_job(&db, &job_id)
            .await
            .map_err(CairnError::Internal)
    })
}

pub fn get_job_todos_by_run(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<RunTodos>, CairnError> {
    let todos = get_job_todos(db, job_id)?;
    if todos.is_empty() {
        return Ok(vec![]);
    }
    Ok(vec![RunTodos {
        run_id: String::new(),
        run_number: 1,
        todos,
    }])
}

/// Reconcile runs left non-terminal by a host that is no longer running, and stop
/// any of their processes that outlived it.
///
/// `boot_at` is this host's [`crate::orchestrator::Orchestrator::boot_at`]. It is
/// required, not inferred: the transport is already serving when startup recovery
/// runs, so "every non-terminal run is stale" is false, and acting on it would let
/// this sweep signal an agent this host had just spawned.
pub fn reconcile_stale_runs(
    orch: &crate::orchestrator::Orchestrator,
    emitter: &dyn crate::services::EventEmitter,
    boot_at: i64,
) {
    // Repair historical failed dependency projections before any ordinary
    // startup re-projection can announce their resolution as new work.
    let workflow = crate::workflow_host::runtime(orch);
    if let Err(error) = crate::execution::advancement::run_advancement_db(async move {
        cairn_workflow::advancement::recompute::heal_failed_dependencies(&workflow).await;
        Ok::<_, String>(())
    }) {
        log::warn!("Startup failed-dependency repair failed: {error}");
    }
    // Every `leased`/`starting` row is about to be requeued by ordinary
    // admission recovery, so all three pending states are relaunchable. A run
    // whose row is `active` had a process that died with the predecessor: it is
    // settled here, and recovery withdraws its requeued row.
    let admitted = run_query_db({
        let local = orch.db.local.clone();
        async move {
            crate::storage::pending_launch_run_ids(&local)
                .await
                .map_err(|error| CairnError::Internal(error.to_string()))
        }
    });
    let admitted = match admitted {
        Ok(admitted) => admitted,
        Err(error) => {
            // Without the durable claims a queued launch is indistinguishable
            // from a dead one; leave every run for the in-session reaper.
            log::warn!("startup reconcile could not read admission claims; skipping: {error}");
            return;
        }
    };
    let result = reconcile_stale_runs_inner(
        orch.db.local.clone(),
        emitter,
        boot_at,
        &crate::agent_process::orphan::OsProcessTable,
        &orch.claimed_launch_job_ids(),
        &admitted,
    );
    if let Ok(result) = result {
        answer_calls_cut_off_by_restart(orch, &result.settled_turns);
        // `recompute_job` sweeps a job's whole execution, and the repair
        // candidates cluster by execution (a resolved issue contributes every
        // job it left unsettled), so sweep each execution once.
        let mut swept = std::collections::HashSet::new();
        settle_stranded_thread_tasks(orch, result.stranded_thread_task_ids, &mut swept);
        for job_id in result.reproject_job_ids {
            let execution_id = execution_id_of(orch, &job_id);
            let outcome = match execution_id {
                Some(execution_id) if !swept.insert(execution_id.clone()) => continue,
                Some(execution_id) => {
                    crate::execution::advancement::recompute_execution_jobs(orch, &execution_id)
                }
                None => crate::execution::advancement::recompute_job(orch, &job_id),
            };
            if let Err(error) = outcome {
                log::warn!("Failed to re-derive job status for {job_id}: {error}");
            }
        }
    }
}

/// Answer the calls of every turn this boot's sweep found running.
///
/// A turn yielded for a held call counts too: the held call stays its wait's to
/// answer, but a sibling call of that turn has nobody else. A pending turn never
/// ran, so it made no calls. One turn's failure is logged and costs only that
/// turn its answers; the sweep has already settled it.
fn answer_calls_cut_off_by_restart(
    orch: &crate::orchestrator::Orchestrator,
    turns: &[SettledTurn],
) {
    use crate::orchestrator::lifecycle::{settle_unanswered_calls, InterruptCause};
    for turn in turns {
        let (Some(run_id), true) = (
            turn.run_id.as_deref(),
            matches!(
                turn.outcome,
                SettledTurnOutcome::Interrupted | SettledTurnOutcome::YieldedForHeldCall
            ),
        ) else {
            continue;
        };
        if let Err(error) =
            settle_unanswered_calls(orch, run_id, &turn.turn_id, InterruptCause::HostRestart)
        {
            log::warn!(
                "Could not answer the calls a restart cut off on turn {}: {error}",
                turn.turn_id
            );
        }
    }
}

/// The job's latest work turn, as a scalar subquery over a `jobs j` row. Memory
/// review is excluded because it runs after the work turn ended and is not the
/// work's own state; the order is the one the job-status projection reads.
const HEAD_WORK_TURN: &str = "(SELECT latest.id FROM turns latest
      WHERE latest.job_id = j.id AND latest.start_reason != 'memory_review'
      ORDER BY latest.created_at DESC, latest.sequence DESC
      LIMIT 1)";

/// What one boot resume did, for its summary line and its tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RestartResumeSummary {
    /// Jobs whose latest work turn this boot settled for a restart.
    pub selected: usize,
    /// Continuations admitted (started, or queued behind ordinary admission).
    pub admitted: usize,
    /// Jobs another launcher reached first, between selection and the lock.
    pub skipped: usize,
    /// Continuations the launch funnel refused. Each settles where the refusal
    /// left it and is never retried here.
    pub refused: usize,
}

/// Resume, once, every job whose turn this boot settled because the host that
/// served it is gone (cairn/5143, and tenet 822's pending-turn half).
///
/// The key is durable and needs no in-memory state: the job's latest work turn
/// ended `host_restart` (the stale-run settlement) or `orphaned_pending` (the
/// runless-pending sweep), and ended at or after this boot. An orderly shutdown
/// records `host_restart` before boot and is also eligible while its turn has no
/// recorded restart-resume attempt. Settlement re-derives each job to Pending
/// (a node) or Idle (a thread session); selection requires that status and no
/// active run.
///
/// It resumes once. A continuation that takes, or is refused after minting, leaves
/// a follow-up turn at the head, started or cancelled `launch_refused`, so the key
/// stops matching. A boot-sweep turn refused before minting predates later boots.
/// An orderly shutdown's one attempt is recorded before entering the funnel,
/// including before a pre-mint refusal, so later boots cannot retry it either.
/// A refused resume leaves the job Pending (wants-to-run) for
/// the DAG or a message to pick up, and a child's parent hears about it through
/// `lifecycle::child_unresumed`.
///
/// Each job is launched through the recovery entry of the ordinary launch
/// funnel, under the job's launch lock and after a compare-and-set that the head
/// turn, status and launch claim are still what selection saw, so a DAG claim or
/// a message that reached the job first wins and this step stands down. Jobs run
/// sequentially; ordinary admission meters the processes they start.
///
/// Only this host's private database is swept. Team replicas belong to their
/// owner's sweep.
///
/// It acts only once startup recovery is ready, because a resumed agent's first
/// tool call goes straight back through the MCP callback gateway, which refuses
/// calls until then (cairn/4758). Before readiness it selects nothing; admission
/// would hold any launch anyway, but a pass that cannot do its work should not
/// pretend to.
///
/// Every way out marks the pass over, including the two that resumed nothing,
/// because nothing retries a pass. The unresumed-child sweep acts only once that
/// mark is set, since until then it could not honestly say a resume did not take.
pub fn resume_jobs_interrupted_by_restart(
    orch: &crate::orchestrator::Orchestrator,
    boot_at: i64,
) -> RestartResumeSummary {
    let summary = restart_resume_pass(orch, boot_at);
    orch.mark_restart_resume_pass_over();
    summary
}

fn restart_resume_pass(
    orch: &crate::orchestrator::Orchestrator,
    boot_at: i64,
) -> RestartResumeSummary {
    if !orch.startup_readiness.is_ready() {
        log::warn!(
            "Not resuming restart-interrupted jobs: startup recovery is not ready, so their callbacks would be refused"
        );
        return RestartResumeSummary::default();
    }
    let candidates = match select_restart_resumes(orch, boot_at) {
        Ok(candidates) => candidates,
        Err(error) => {
            log::warn!("boot resume could not select restart-interrupted jobs: {error}");
            return RestartResumeSummary::default();
        }
    };
    let mut summary = RestartResumeSummary {
        selected: candidates.len(),
        ..RestartResumeSummary::default()
    };
    for (job_id, turn_id) in candidates {
        match resume_restart_interrupted_job(orch, &job_id, &turn_id) {
            Ok(true) => summary.admitted += 1,
            Ok(false) => summary.skipped += 1,
            Err(error) => {
                summary.refused += 1;
                log::warn!(
                    "boot resume of job {} after a host restart was refused; it stays wants-to-run: {error}",
                    &job_id[..job_id.len().min(8)]
                );
            }
        }
    }
    log::info!(
        "Boot resume of restart-interrupted jobs: selected={} admitted={} skipped={} refused={}",
        summary.selected,
        summary.admitted,
        summary.skipped,
        summary.refused
    );
    summary
}

fn select_restart_resumes(
    orch: &crate::orchestrator::Orchestrator,
    boot_at: i64,
) -> Result<Vec<(String, String)>, String> {
    use crate::storage::RowExt as _;
    let db = orch.db.local.clone();
    // Single-table shape: `turns` and `runs` are reached from `j` through
    // correlated subqueries, one indexed probe per candidate job (see the
    // repair selections in the stale-run sweep for why).
    let sql = format!(
        "SELECT j.id, {HEAD_WORK_TURN}
         FROM jobs j
         WHERE j.status IN ('pending', 'idle')
           AND NOT EXISTS (
               SELECT 1 FROM runs r
               WHERE r.job_id = j.id AND r.status IN ('starting', 'live')
           )
           AND EXISTS (
               SELECT 1 FROM turns t
               WHERE t.id = {HEAD_WORK_TURN}
                 AND t.end_reason IN ('host_restart', 'orphaned_pending')
                 AND NOT EXISTS (
                     SELECT 1 FROM events attempted
                     WHERE attempted.turn_id = t.id
                       AND attempted.event_type = 'system:restart_resume_attempt'
                 )
                 AND (t.ended_at >= ?1 OR (
                     t.end_reason = 'host_restart'
                     AND EXISTS (
                         SELECT 1 FROM runs stopped
                         WHERE stopped.id = t.run_id
                           AND stopped.exit_reason = 'runner_shutdown'
                           AND stopped.created_at < ?1
                     )
                 ))
           )
         ORDER BY j.created_at, j.id"
    );
    crate::storage::run_db_blocking(move || async move {
        db.read(|conn| {
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, (boot_at,)).await?;
                let mut candidates = Vec::new();
                while let Some(row) = rows.next().await? {
                    candidates.push((row.text(0)?, row.text(1)?));
                }
                Ok(candidates)
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

/// Orderly shutdown settles before boot, so its timestamp cannot identify the
/// boot that owes it a resume. Claim that one attempt on the interrupted turn,
/// in the same transcript ledger as the shutdown, before entering the funnel.
fn claim_shutdown_restart_resume(
    orch: &crate::orchestrator::Orchestrator,
    turn_id: &str,
) -> Result<bool, String> {
    use crate::storage::RowExt as _;
    use cairn_session::agent_process::stream::TranscriptJson as _;
    let db = orch.db.local.clone();
    let turn_id = turn_id.to_string();
    crate::storage::run_db_blocking(move || async move {
        db.write(|conn| {
            let turn_id = turn_id.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "SELECT t.run_id, t.session_id FROM turns t
                     WHERE t.id = ?1 AND t.end_reason = 'host_restart'
                       AND EXISTS (SELECT 1 FROM runs r WHERE r.id=t.run_id AND r.exit_reason='runner_shutdown')",
                    (turn_id.as_str(),),
                ).await?;
                let Some(row) = rows.next().await? else { return Ok(true); };
                let run_id = row.text(0)?;
                let session_id = row.text(1)?;
                drop(rows);
                let mut attempted = conn.query(
                    "SELECT 1 FROM events WHERE turn_id=?1 AND event_type='system:restart_resume_attempt' LIMIT 1",
                    (turn_id.as_str(),),
                ).await?;
                if attempted.next().await?.is_some() { return Ok(false); }
                drop(attempted);
                let now = chrono::Utc::now().timestamp() as i32;
                let data = crate::agent_process::stream::TranscriptEvent {
                    event_type: "system:restart_resume_attempt".to_string(),
                    session_id: Some(session_id.clone()),
                    parent_tool_use_id: None,
                    content: None,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: None,
                }
                .observed()
                .to_event_json();
                crate::transcripts::stream_store::insert_event_conn(conn,
                    &crate::transcripts::stream_store::EventInsert {
                        id: uuid::Uuid::new_v4().to_string(),
                        run_id, session_id: Some(session_id), turn_id: Some(turn_id),
                        timestamp: now, created_at: now, selected_model: None,
                        event_type: "system:restart_resume_attempt".into(),
                        data,
                        parent_tool_use_id: None, input_tokens: None, cache_read_tokens: None,
                        cache_create_tokens: None, output_tokens: None, thinking_tokens: None, cost_usd: None,
                    },
                ).await?;
                Ok(true)
            })
        }).await.map_err(|error| error.to_string())
    })
}

/// Launch one selected job's continuation. `Ok(false)` when something else
/// reached the job between selection and the lock.
fn resume_restart_interrupted_job(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    turn_id: &str,
) -> Result<bool, String> {
    let launch_lock = orch.job_launch_lock(job_id);
    let guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if orch.claimed_launch_run(job_id).is_some()
        || !restart_head_still_matches(orch, job_id, turn_id)?
    {
        return Ok(false);
    }
    if !claim_shutdown_restart_resume(orch, turn_id)? {
        return Ok(false);
    }
    // Record an orderly shutdown's one attempt even when dormancy refuses it
    // before the funnel can mint a successor. No later boot retries that refusal.
    if crate::threads::is_dormant_thread_session_sync(&orch.db.local, job_id) {
        return Err(crate::threads::dormant_thread_refusal());
    }
    let launch =
        crate::execution::jobs::continue_job_launch_locked_for_recovery(orch, job_id, None, guard)?;
    let (run, successor, state) = match launch {
        crate::execution::jobs::RecoveryContinuation::Started { run, turn_id } => {
            (run, turn_id, "started")
        }
        crate::execution::jobs::RecoveryContinuation::Queued { run, turn_id } => {
            (run, turn_id, "queued")
        }
    };
    log::info!(
        "Resumed job {} after a host restart: run {} turn {} ({state})",
        &job_id[..job_id.len().min(8)],
        &run.id[..run.id.len().min(8)],
        &successor[..successor.len().min(8)]
    );
    // The funnel writes Running before it launches. Should that write have been
    // refused, re-derive from the run it just inserted, so the job cannot sit
    // Pending beside a launch and be claimed a second time by the DAG.
    let status = crate::storage::run_db_blocking({
        let db = orch.db.local.clone();
        let job_id = job_id.to_string();
        move || async move {
            db.query_opt_text("SELECT status FROM jobs WHERE id = ?1", (job_id,))
                .await
                .map_err(|error| error.to_string())
        }
    })?;
    if status.as_deref() != Some("running") {
        crate::execution::advancement::recompute_job(orch, job_id)?;
    }
    Ok(true)
}

/// The compare-and-set the boot resume makes under the job's launch lock: the
/// job is still Pending or Idle, has no active run, and its latest work turn is
/// still the one selection saw.
fn restart_head_still_matches(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    turn_id: &str,
) -> Result<bool, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    let turn_id = turn_id.to_string();
    let sql = format!(
        "SELECT 1 FROM jobs j
         WHERE j.id = ?1
           AND j.status IN ('pending', 'idle')
           AND NOT EXISTS (
               SELECT 1 FROM runs r
               WHERE r.job_id = j.id AND r.status IN ('starting', 'live')
           )
           AND {HEAD_WORK_TURN} = ?2"
    );
    crate::storage::run_db_blocking(move || async move {
        db.query_opt_i64(&sql, (job_id, turn_id))
            .await
            .map(|row| row.is_some())
            .map_err(|error| error.to_string())
    })
}

fn execution_id_of(orch: &crate::orchestrator::Orchestrator, job_id: &str) -> Option<String> {
    crate::storage::run_db_blocking({
        let db = orch.db.local.clone();
        let job_id = job_id.to_string();
        move || async move {
            db.query_opt_text("SELECT execution_id FROM jobs WHERE id = ?1", (job_id,))
                .await
                .map_err(|error| error.to_string())
        }
    })
    .ok()
    .flatten()
}

/// Settle the thread tasks a liveness-only release stranded `idle`.
///
/// Each execution is swept once, settling all of its stranded tasks together, so
/// a blocking batch has every sibling terminal before any delivery runs and its
/// parent wakes exactly once. The settlement is quiet (see
/// `ChangeCause::Repaired`); the only thing a parent can receive is the Repair
/// resume `settle_stranded_jobs` runs on this thread, which wakes nothing whose
/// head has moved on, pushes nothing for a background task, and leads each
/// result with its delayed-delivery notice. Every delivery has finished when this
/// returns. Executions swept here are recorded in `swept` so the reprojection
/// that follows does not sweep them a second time.
fn settle_stranded_thread_tasks(
    orch: &crate::orchestrator::Orchestrator,
    stranded: Vec<String>,
    swept: &mut std::collections::HashSet<String>,
) {
    let mut by_execution: std::collections::BTreeMap<String, std::collections::HashSet<String>> =
        std::collections::BTreeMap::new();
    for job_id in stranded {
        if let Some(execution_id) = execution_id_of(orch, &job_id) {
            by_execution.entry(execution_id).or_default().insert(job_id);
        }
    }
    for (execution_id, settle) in by_execution {
        swept.insert(execution_id.clone());
        let changes =
            match crate::execution::advancement::settle_stranded_jobs(orch, &execution_id, &settle)
            {
                Ok(changes) => changes,
                Err(error) => {
                    log::warn!("Failed to settle stranded thread tasks in {execution_id}: {error}");
                    continue;
                }
            };
        let settled = changes
            .iter()
            .filter(|change| settle.contains(&change.job_id) && change.to.is_terminal())
            .count();
        if settled > 0 {
            log::info!("Settled {settled} stranded thread task(s) in execution {execution_id}");
        }
    }
}

#[cfg(test)]
#[path = "restart_resume_tests.rs"]
mod restart_resume_tests;
