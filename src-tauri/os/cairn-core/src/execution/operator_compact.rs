//! The operator's Compact action, idle or cold.
//!
//! Compact is always available. On a job with nothing live it is the ordinary
//! digest reseed ([`crate::execution::jobs::resume_job_from_digest`]), unchanged.
//! On a job that has live work — a turn that is not terminal, a run serving one,
//! a parked durable wait, a pending permission or question — it is the
//! operator's cold reset: everything live is torn down with a typed
//! `operator_compact` cause, the session is closed with one boundary in the
//! same transaction as the close, and the job continues on a fresh session
//! seeded from its digest, carrying whatever was queued for it.
//!
//! "Live" is read from durable rows, not from a label. The point of this path
//! is that a turn marked `running` may have nothing behind it, and the operator
//! still needs a way out; so the cold path never asks whether the work is
//! *really* live, it ends all of it.
//!
//! The whole cold path runs under the job's launch lock, held from the first
//! teardown step through the successor's launch, so no racing message, wait
//! resolver, or answer can resume the old session natively in between. Every
//! load-bearing step returns its error; each is idempotent, so a Compact that
//! failed part way can simply be pressed again.

use crate::agent_process::process::{GoalPause, PauseIntent};
use crate::mcp::handlers::permission::{HandBackPolicy, Withdrawal, WithdrawalCause};
use crate::models::Run;
use crate::orchestrator::lifecycle::{
    find_descendant_job_ids, kill_session_with_reason, settle_live_turn, TurnSettlement,
    OPERATOR_COMPACT_EXIT_REASON,
};
use crate::orchestrator::{Orchestrator, TurnEndCancelReason};
use crate::storage::{run_db_blocking, LocalDb, RowExt};
use cairn_db::turso::params;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

/// How long Compact waits for a launch already holding this job's launch lock.
///
/// A launch that holds the lock longer than this is itself wedged, and Compact
/// cannot preempt it: it says so rather than hanging the operator's click.
const LAUNCH_LOCK_PATIENCE: Duration = Duration::from_secs(10);

/// A non-terminal turn of the job or one of its sub-agent jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTurn {
    pub id: String,
    pub run_id: Option<String>,
}

/// A `starting`/`live` run serving a non-terminal turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRun {
    pub id: String,
    pub job_id: String,
}

/// A permission request still waiting for its answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPermission {
    pub id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub turn_id: Option<String>,
}

/// Everything live under a job and its sub-agent jobs, read from durable rows.
///
/// A warm-parked idle process is deliberately not live work: it serves no turn,
/// and the idle reseed already ends it when it rotates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveWork {
    /// The job first, then its descendants. Context, not live work.
    pub jobs: Vec<String>,
    pub turns: Vec<LiveTurn>,
    pub runs: Vec<LiveRun>,
    pub waits: Vec<String>,
    pub permissions: Vec<PendingPermission>,
    pub questions: Vec<String>,
}

/// The live work a Compact would tear down, as the confirmation reads it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveWorkCounts {
    pub turns: usize,
    pub runs: usize,
    pub waits: usize,
    pub permissions: usize,
    pub questions: usize,
}

impl LiveWork {
    /// Read the job's live work. A database error is returned, never read as
    /// "nothing live": misreading it would send a stuck job down the idle path,
    /// which refuses exactly the jobs this exists for.
    pub async fn for_job(db: &LocalDb, job_id: &str) -> Result<Self, String> {
        let root = job_id.to_string();
        let descendants = db
            .read(|conn| {
                let root = root.clone();
                Box::pin(async move { find_descendant_job_ids(conn, &root).await })
            })
            .await
            .map_err(|error| format!("failed to read the job's sub-agent jobs: {error}"))?;
        let mut work = LiveWork {
            jobs: std::iter::once(root).chain(descendants).collect(),
            ..LiveWork::default()
        };
        for job in work.jobs.clone() {
            work.read_job(db, &job).await?;
        }
        Ok(work)
    }

    async fn read_job(&mut self, db: &LocalDb, job_id: &str) -> Result<(), String> {
        let job = job_id.to_string();
        let read = |what: &'static str| move |error| format!("failed to read {what}: {error}");
        self.turns.extend(
            db.query_all(
                "SELECT id, run_id FROM turns
                  WHERE job_id = ?1 AND state IN ('pending', 'running', 'yielded')
                  ORDER BY created_at, sequence",
                params![job.clone()],
                |row| {
                    Ok(LiveTurn {
                        id: row.text(0)?,
                        run_id: row.opt_text(1)?,
                    })
                },
            )
            .await
            .map_err(read("live turns"))?,
        );
        self.runs.extend(
            db.query_all(
                "SELECT r.id, r.job_id FROM runs r
                  WHERE r.job_id = ?1 AND r.status IN ('starting', 'live')
                    AND EXISTS (SELECT 1 FROM turns t
                                 WHERE t.run_id = r.id
                                   AND t.state IN ('pending', 'running', 'yielded'))
                  ORDER BY r.created_at",
                params![job.clone()],
                |row| {
                    Ok(LiveRun {
                        id: row.text(0)?,
                        job_id: row.text(1)?,
                    })
                },
            )
            .await
            .map_err(read("live runs"))?,
        );
        self.waits.extend(
            db.query_all(
                "SELECT id FROM agent_waits
                  WHERE job_id = ?1 AND state IN ('pending', 'resolving')",
                params![job.clone()],
                |row| row.text(0),
            )
            .await
            .map_err(read("durable waits"))?,
        );
        self.permissions.extend(
            db.query_all(
                "SELECT pr.id, pr.run_id, pr.tool_use_id, pr.turn_id
                   FROM permission_requests pr
                   LEFT JOIN runs r ON r.id = pr.run_id
                  WHERE COALESCE(pr.job_id, r.job_id) = ?1 AND pr.status = 'pending'",
                params![job.clone()],
                |row| {
                    Ok(PendingPermission {
                        id: row.text(0)?,
                        run_id: row.text(1)?,
                        tool_use_id: row.text(2)?,
                        turn_id: row.opt_text(3)?,
                    })
                },
            )
            .await
            .map_err(read("pending permissions"))?,
        );
        self.questions.extend(
            db.query_all(
                "SELECT p.id FROM prompts p
                   LEFT JOIN runs r ON r.id = p.run_id
                  WHERE COALESCE(p.job_id, r.job_id) = ?1
                    AND p.response IS NULL AND p.withdrawn_at IS NULL",
                params![job],
                |row| row.text(0),
            )
            .await
            .map_err(read("pending questions"))?,
        );
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.turns.is_empty()
            && self.runs.is_empty()
            && self.waits.is_empty()
            && self.permissions.is_empty()
            && self.questions.is_empty()
    }

    pub fn counts(&self) -> LiveWorkCounts {
        LiveWorkCounts {
            turns: self.turns.len(),
            runs: self.runs.len(),
            waits: self.waits.len(),
            permissions: self.permissions.len(),
            questions: self.questions.len(),
        }
    }
}

/// Read a job's live work from a synchronous caller.
pub fn live_work_for_job(orch: &Orchestrator, job_id: &str) -> Result<LiveWork, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        LiveWork::for_job(&db, &job_id).await
    })
}

/// What a cold Compact tore down, as written to the old session's transcript.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorCompactReport {
    pub turns_cancelled: usize,
    pub runs_killed: usize,
    pub batches_cancelled: usize,
    pub checks_cancelled: usize,
    pub permissions_withdrawn: usize,
    pub questions_withdrawn: usize,
    pub waits_abandoned: usize,
    /// Live provider processes whose native goal could not be paused before
    /// teardown. Not fatal: the process is killed regardless, and the turn
    /// re-sweep after the kill settles anything that started in the gap.
    pub goal_pause_failures: usize,
}

impl OperatorCompactReport {
    fn summary(&self) -> String {
        format!(
            "The operator compacted this session while it had live work. Cancelled {} turn(s), \
             killed {} run(s), cancelled {} executor batch(es) and {} turn-end check wave(s), \
             withdrew {} permission(s) and {} question(s), and abandoned {} durable wait(s). \
             The job continues on a fresh session rebuilt from its digest.",
            self.turns_cancelled,
            self.runs_killed,
            self.batches_cancelled,
            self.checks_cancelled,
            self.permissions_withdrawn,
            self.questions_withdrawn,
            self.waits_abandoned,
        )
    }
}

/// The result of an operator Compact.
#[derive(Debug)]
pub struct OperatorCompactOutcome {
    /// The run serving the successor session (or the idle reseed's run).
    pub run: Run,
    /// What the cold path tore down; `None` for an idle Compact.
    pub report: Option<OperatorCompactReport>,
    /// A post-teardown bookkeeping step failed: the `system:operator_compact`
    /// record could not be written, or a job's status could not be re-derived.
    /// The reset itself completed; this is reported so the gap is visible,
    /// never used to abort a reset whose teardown already happened.
    pub diagnostic_error: Option<String>,
}

/// Whether the operator has consented to the cold path.
///
/// The cold path kills a running session, so it never runs on the strength of
/// a client-side guess about whether the job is live. A Compact without consent
/// that finds live work does nothing and returns the work it found, for the
/// operator to confirm against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdCompact {
    /// Compact only if the job is idle; otherwise ask.
    Unconfirmed,
    /// The operator confirmed tearing down live work.
    Confirmed,
}

/// What pressing Compact did.
#[derive(Debug)]
pub enum CompactResult {
    /// The job had live work and the cold path was not confirmed. Nothing was
    /// touched; these are the counts to confirm against.
    ConfirmationRequired(LiveWorkCounts),
    /// The job continued on a fresh session.
    Compacted(Box<OperatorCompactOutcome>),
}

/// The operator pressed Compact on `job_id`.
pub fn compact_job(
    orch: &Orchestrator,
    job_id: &str,
    identity_override: Option<crate::identity::UserIdentity>,
    consent: ColdCompact,
) -> Result<CompactResult, String> {
    let idle = |identity_override| {
        crate::execution::jobs::resume_job_from_digest(orch, job_id, identity_override).map(|run| {
            CompactResult::Compacted(Box::new(OperatorCompactOutcome {
                run,
                report: None,
                diagnostic_error: None,
            }))
        })
    };
    let live = live_work_for_job(orch, job_id)?;
    if live.is_empty() {
        return idle(identity_override);
    }
    if consent == ColdCompact::Unconfirmed {
        return Ok(CompactResult::ConfirmationRequired(live.counts()));
    }

    let db = owning_db(orch, job_id)?;
    refuse_before_teardown(orch, &db, job_id)?;

    let lock = orch.job_launch_lock(job_id);
    let guard = acquire_launch_lock(&lock, job_id)?;
    // Re-read under the lock: whatever was live may have finished while this
    // waited, and an idle job takes the idle path.
    let live = live_work_for_job(orch, job_id)?;
    if live.is_empty() {
        drop(guard);
        return idle(identity_override);
    }

    // Everything the bookkeeping after the kill needs is read now. Past the
    // kill, only the successor's own launch may fail the call: a read failing
    // there would leave the job torn down with no successor.
    let old_session_id = db_text(
        &db,
        "SELECT current_session_id FROM jobs WHERE id = ?1",
        job_id,
    )?;
    let anchor = boundary_anchor(&db, job_id)?;
    let touched_jobs = live_jobs_bottom_up(&live.jobs);

    let report = tear_down(orch, &db, job_id, live)?;

    // From here to the launch is bookkeeping: each failure is logged and
    // reported, never allowed to strand a torn-down job without its successor.
    let mut diagnostics = Vec::new();
    let recorded = match anchor.as_ref() {
        Some(anchor) => record_teardown(orch, anchor, old_session_id.as_deref(), &report).err(),
        None => None,
    };
    if let Some(error) = recorded {
        diagnostics.push(format!(
            "could not record the system:operator_compact event: {error}"
        ));
    }

    // Job status is derived, never written: re-derive every job the teardown
    // touched, sub-agents first, so none is left reading `running`.
    for job in &touched_jobs {
        if let Err(error) = crate::execution::advancement::recompute_job(orch, job) {
            diagnostics.push(format!("could not re-derive job {job}: {error}"));
        }
    }
    for diagnostic in &diagnostics {
        log::error!("operator compact of job {job_id} tore its session down, but {diagnostic}");
    }
    let diagnostic_error = (!diagnostics.is_empty()).then(|| diagnostics.join("; "));

    let run = crate::execution::jobs::continue_job_launch_locked_for_operator_compact(
        orch,
        job_id,
        identity_override,
        anchor,
        guard,
    )?;
    Ok(CompactResult::Compacted(Box::new(OperatorCompactOutcome {
        run,
        report: Some(report),
        diagnostic_error,
    })))
}

/// Write the `system:operator_compact` record of what the teardown ended.
fn record_teardown(
    orch: &Orchestrator,
    anchor: &crate::execution::jobs::OperatorBoundary,
    session_id: Option<&str>,
    report: &OperatorCompactReport,
) -> Result<(), String> {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(error) = test_hooks::record_failure(orch) {
        return Err(error);
    }
    crate::messages::transcript::insert_operator_compact_event_sync(
        orch,
        &anchor.run_id,
        session_id,
        anchor.turn_id.as_deref(),
        report.summary(),
        serde_json::to_value(report).unwrap_or_default(),
    )
}

fn owning_db(orch: &Orchestrator, job_id: &str) -> Result<Arc<LocalDb>, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())
    })
}

fn db_text(db: &Arc<LocalDb>, sql: &'static str, arg: &str) -> Result<Option<String>, String> {
    let db = db.clone();
    let arg = arg.to_string();
    run_db_blocking(move || async move {
        db.query_opt_text(sql, params![arg])
            .await
            .map_err(|error| error.to_string())
    })
}

/// Refuse a Compact the successor launch would refuse anyway, before anything
/// is torn down: tearing a session down and then failing to replace it would
/// leave the operator worse off than the stuck state they started from.
fn refuse_before_teardown(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    job_id: &str,
) -> Result<(), String> {
    if crate::threads::is_dormant_thread_session_sync(db, job_id) {
        return Err(crate::threads::dormant_thread_refusal());
    }
    let status = db_text(
        db,
        "SELECT s.status FROM jobs j JOIN sessions s ON s.id = j.current_session_id
          WHERE j.id = ?1",
        job_id,
    )?;
    if status.as_deref() != Some("open") {
        return Err("This job has no open agent session to compact.".to_string());
    }
    // The digest must compose, or there is nothing to reseed from.
    crate::execution::jobs::preview_job_digest(orch, job_id).map(|_| ())
}

fn acquire_launch_lock<'a>(
    lock: &'a Mutex<()>,
    job_id: &str,
) -> Result<MutexGuard<'a, ()>, String> {
    let deadline = Instant::now() + LAUNCH_LOCK_PATIENCE;
    loop {
        match lock.try_lock() {
            Ok(guard) => return Ok(guard),
            // What the lock guards is durable rows the teardown re-reads, so
            // an earlier panic is recovered from, as the launch funnel does.
            Err(TryLockError::Poisoned(poisoned)) => return Ok(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(TryLockError::WouldBlock) => {
                return Err(format!(
                    "A launch for job {} is holding its launch lock and has not released it \
                     after {} seconds. Compact cannot preempt a launch in progress, so nothing \
                     was torn down. If this persists, that launch is itself wedged.",
                    &job_id[..job_id.len().min(8)],
                    LAUNCH_LOCK_PATIENCE.as_secs()
                ))
            }
        }
    }
}

/// End everything live under `job_id`, in the order the contract requires:
/// nothing about the successor happens until all of this is done.
fn tear_down(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    job_id: &str,
    live: LiveWork,
) -> Result<OperatorCompactReport, String> {
    let mut report = OperatorCompactReport::default();

    for job in &live.jobs {
        if orch.cancel_turn_end_checks(job, TurnEndCancelReason::OperatorCompact) {
            report.checks_cancelled += 1;
        }
    }

    // Every process this Compact will kill: the job's own live runs, warm or
    // not, and each sub-agent run serving a turn.
    let mut doomed = job_runs(db, job_id)?;
    for run in &live.runs {
        if !doomed.iter().any(|known| known.id == run.id) {
            doomed.push(run.clone());
        }
    }

    // Fence native goal continuation first. Settling a Codex turn while its
    // process lives would leave the app-server idle under an active goal, which
    // admits a continuation turn in the gap before the kill.
    for run in &doomed {
        if let Err(error) = crate::backends::stdin::pause_goal_for_durable_wait(
            &orch.process_state,
            &run.id,
            PauseIntent::Stake(GoalPause::OperatorCompact),
        ) {
            report.goal_pause_failures += 1;
            log::error!(
                "operator compact of job {job_id} could not pause run {}'s native goal; \
                 killing it regardless: {error}",
                run.id
            );
        }
    }

    settle_asks_and_turns(orch, db, &live, &mut report)?;
    #[cfg(any(test, feature = "test-utils"))]
    test_hooks::after_settle(orch, job_id);

    // Sub-agents first, so no child outlives the parent it reports to.
    doomed.sort_by_key(|run| run.job_id == job_id);
    for run in &doomed {
        kill_session_with_reason(orch, &run.id, OPERATOR_COMPACT_EXIT_REASON)
            .map_err(|error| format!("failed to kill run {}: {error}", run.id))?;
        report.runs_killed += 1;
    }

    // After the kill, so a batch dispatched in the final moments is caught too.
    for job in &live.jobs {
        report.batches_cancelled += orch
            .fleet
            .cancel_job_requests(job, crate::fleet::CancelCause::OperatorCompact);
    }

    // Anything that went live between the first sweep and the kill — a goal
    // turn admitted in the gap, an ask raised by a call already in flight — is
    // ended the same way. Nothing non-terminal may reach the rotation.
    let after = live_work_for_job(orch, job_id)?;
    if !after.is_empty() {
        settle_asks_and_turns(orch, db, &after, &mut report)?;
        for run in &after.runs {
            kill_session_with_reason(orch, &run.id, OPERATOR_COMPACT_EXIT_REASON)
                .map_err(|error| format!("failed to kill run {}: {error}", run.id))?;
            report.runs_killed += 1;
        }
        let remaining = live_work_for_job(orch, job_id)?;
        if !remaining.is_empty() {
            return Err(format!(
                "live work survived the teardown of job {job_id}: {:?}; nothing was rotated, \
                 and pressing Compact again retries the teardown",
                remaining.counts()
            ));
        }
    }
    Ok(report)
}

/// Withdraw asks, abandon waits, then settle turns — in that order: a
/// withdrawn permission can hand an outer run-batch wait back, which the
/// abandonment that follows must then see.
fn settle_asks_and_turns(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    live: &LiveWork,
    report: &mut OperatorCompactReport,
) -> Result<(), String> {
    for permission in &live.permissions {
        let orch_for_task = orch.clone();
        let permission = permission.clone();
        let withdrawal = run_db_blocking(move || async move {
            crate::mcp::handlers::permission::withdraw_permission_request(
                &orch_for_task,
                &permission.run_id,
                &permission.id,
                &permission.tool_use_id,
                permission.turn_id.as_deref(),
                WithdrawalCause::OperatorCompact,
                HandBackPolicy::None,
            )
            .await
            .map_err(|error| format!("failed to withdraw permission {}: {error}", permission.id))
        })?;
        if matches!(withdrawal, Withdrawal::Withdrawn { .. }) {
            report.permissions_withdrawn += 1;
        }
    }

    if !live.questions.is_empty() {
        let questions = live.questions.clone();
        let db_for_task = db.clone();
        report.questions_withdrawn += run_db_blocking(move || async move {
            let now = chrono::Utc::now().timestamp();
            db_for_task
                .write(|conn| {
                    let questions = questions.clone();
                    Box::pin(async move {
                        let mut withdrawn = 0;
                        for question in &questions {
                            if crate::mcp::handlers::planning::withdraw_prompt_conn(
                                conn,
                                question,
                                WithdrawalCause::OperatorCompact,
                                now,
                            )
                            .await?
                            {
                                withdrawn += 1;
                            }
                        }
                        Ok(withdrawn)
                    })
                })
                .await
                .map_err(|error| format!("failed to withdraw pending questions: {error}"))
        })?;
        let changes = run_db_blocking({
            let db = db.clone();
            let questions = live.questions.clone();
            move || async move {
                let mut changes = Vec::with_capacity(questions.len());
                for question in &questions {
                    changes.push(
                        crate::notify::prompt_db_change_for_id(&db, question, "update").await,
                    );
                }
                Ok::<_, String>(changes)
            }
        })?;
        for change in changes {
            let _ = orch.services.emitter.emit("db-change", change);
        }
    }

    if !live.waits.is_empty() {
        for job in &live.jobs {
            let orch_for_task = orch.clone();
            let db_for_task = db.clone();
            let job = job.clone();
            report.waits_abandoned += run_db_blocking(move || async move {
                crate::mcp::handlers::durable_suspend::abandon_job_waits_for_operator_compact(
                    &orch_for_task,
                    &db_for_task,
                    &job,
                )
                .await
                .map_err(|error| format!("failed to abandon job {job}'s durable waits: {error}"))
            })?;
        }
    }

    for turn in &live.turns {
        if settle_live_turn(
            orch,
            turn.run_id.as_deref(),
            &turn.id,
            TurnSettlement::OperatorCompact,
        )
        .map_err(|error| format!("failed to cancel turn {}: {error}", turn.id))?
        {
            report.turns_cancelled += 1;
        }
    }
    Ok(())
}

/// The job's own `starting`/`live` runs, whether or not they serve a turn.
fn job_runs(db: &Arc<LocalDb>, job_id: &str) -> Result<Vec<LiveRun>, String> {
    let db = db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.query_all(
            "SELECT id, job_id FROM runs WHERE job_id = ?1 AND status IN ('starting', 'live')",
            params![job_id],
            |row| {
                Ok(LiveRun {
                    id: row.text(0)?,
                    job_id: row.text(1)?,
                })
            },
        )
        .await
        .map_err(|error| format!("failed to read the job's runs: {error}"))
    })
}

/// Where the compaction's boundary and its record go: the job's newest run,
/// at that run's newest turn. `None` for a job that has never run.
fn boundary_anchor(
    db: &Arc<LocalDb>,
    job_id: &str,
) -> Result<Option<crate::execution::jobs::OperatorBoundary>, String> {
    let Some(run_id) = db_text(
        db,
        "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
        job_id,
    )?
    else {
        return Ok(None);
    };
    let turn_id = db_text(
        db,
        "SELECT id FROM turns WHERE run_id = ?1 ORDER BY sequence DESC LIMIT 1",
        &run_id,
    )?;
    Ok(Some(crate::execution::jobs::OperatorBoundary {
        run_id,
        turn_id,
    }))
}

/// Seams for tests that must act at an exact point inside the teardown.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_hooks {
    use crate::orchestrator::Orchestrator;
    use std::sync::Mutex;

    type Hook = Box<dyn Fn(&Orchestrator, &str) + Send + Sync>;
    static AFTER_SETTLE: Mutex<Option<Hook>> = Mutex::new(None);
    type FailureHook = Box<dyn Fn(&Orchestrator) -> Option<String> + Send + Sync>;
    static RECORD_FAILURE: Mutex<Option<FailureHook>> = Mutex::new(None);

    /// Fail the teardown record with whatever `hook` returns, standing in for
    /// a database failure after the kill. Like [`set_after_settle`], a hook
    /// must answer `None` for an orchestrator that is not its own.
    pub fn set_record_failure(hook: Option<FailureHook>) {
        *RECORD_FAILURE.lock().unwrap_or_else(|p| p.into_inner()) = hook;
    }

    pub(super) fn record_failure(orch: &Orchestrator) -> Option<String> {
        RECORD_FAILURE
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|hook| hook(orch))
    }

    /// Run `hook` after the first settle and before any process is killed:
    /// the window in which a provider could still start a turn. Tests share
    /// one process, so a hook must check that the orchestrator is its own.
    pub fn set_after_settle(hook: Option<Hook>) {
        *AFTER_SETTLE.lock().unwrap_or_else(|p| p.into_inner()) = hook;
    }

    pub(super) fn after_settle(orch: &Orchestrator, job_id: &str) {
        if let Some(hook) = AFTER_SETTLE
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            hook(orch, job_id);
        }
    }
}

/// Descendants before the job itself, so a parent's derivation sees its
/// children already settled.
fn live_jobs_bottom_up(jobs: &[String]) -> Vec<String> {
    jobs.iter().rev().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seeded(file: &str, script: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(file).await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'ws', 'P', 'P', '/tmp/p', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES ('job', 'p', 'running', 's1', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('s1', 'job', 'claude', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed a job with an open session");
        db.execute_script(script)
            .await
            .expect("seed the job's state");
        db
    }

    /// A warm process parked between turns serves nothing: Compact on it is
    /// the ordinary idle reseed, not a teardown.
    #[tokio::test]
    async fn a_warm_idle_process_is_not_live_work() {
        let db = seeded(
            "live-work-warm-idle.db",
            "INSERT INTO runs(id, job_id, session_id, status, created_at, updated_at)
               VALUES ('r1', 'job', 's1', 'live', 1, 1);
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
               VALUES ('t1', 's1', 'r1', 'job', 1, 'complete', 'initial', 1, 1);",
        )
        .await;
        let work = LiveWork::for_job(&db, "job").await.expect("read live work");
        assert!(work.is_empty(), "{work:?}");
        assert_eq!(work.jobs, vec!["job".to_string()]);
    }

    /// A turn yielded on a pending question is exactly the stuck state Compact
    /// must be able to clear, so it is live work, with its run and its ask.
    #[tokio::test]
    async fn a_yielded_turn_with_a_pending_ask_is_live_work() {
        let db = seeded(
            "live-work-yielded-ask.db",
            "INSERT INTO runs(id, job_id, session_id, status, created_at, updated_at)
               VALUES ('r1', 'job', 's1', 'live', 1, 1);
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
               VALUES ('t1', 's1', 'r1', 'job', 1, 'yielded', 'initial', 1, 1);
             INSERT INTO prompts(id, run_id, job_id, turn_id, questions, created_at)
               VALUES ('q1', 'r1', 'job', 't1', '[]', 1);
             INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, turn_id)
               VALUES ('perm1', 'r1', 'job', 'tu1', 'Bash', '{}', 'pending', 1, 't1');",
        )
        .await;
        let work = LiveWork::for_job(&db, "job").await.expect("read live work");
        assert_eq!(
            work.counts(),
            LiveWorkCounts {
                turns: 1,
                runs: 1,
                waits: 0,
                permissions: 1,
                questions: 1,
            }
        );
    }

    /// A withdrawn question is no longer waiting on anyone.
    #[tokio::test]
    async fn a_withdrawn_question_is_not_live_work() {
        let db = seeded(
            "live-work-withdrawn-question.db",
            "INSERT INTO runs(id, job_id, session_id, status, created_at, updated_at)
               VALUES ('r1', 'job', 's1', 'exited', 1, 1);
             INSERT INTO prompts(id, run_id, job_id, questions, created_at, withdrawn_at, withdrawn_cause)
               VALUES ('q1', 'r1', 'job', '[]', 1, 2, 'operator_compact');",
        )
        .await;
        let work = LiveWork::for_job(&db, "job").await.expect("read live work");
        assert!(work.is_empty(), "{work:?}");
    }
}
