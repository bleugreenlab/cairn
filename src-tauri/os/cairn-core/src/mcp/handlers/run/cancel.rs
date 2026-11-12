//! Cancelling one run batch, addressed by its request id.
//!
//! For a batch whose task runs in this runner the cancel adds no second way
//! for it to end. It records who cancelled the batch, interrupts its host
//! items, and stops it exactly as its ceiling does
//! ([`super::BatchControl::stop`]): placement halts and the fleet sends the
//! batch's machine `Cancel` for its request, which ends the process tree. The
//! batch's own task then settles it through the ordinary cancelled tail, so
//! whatever is waiting on it hears the result the way it would hear any other:
//! a parked call resumes its turn once through the durable resolver, and a
//! detached batch settles its row and queues its job's notice.
//!
//! The settle gate decides whether a cancel can still matter. Before
//! settlement begins it stops the batch, and nothing from it is published.
//! Once settlement has begun the batch may already be writing its commit, so
//! the cancel touches nothing and that result stands; a replay carries the
//! commit like any other.
//!
//! A batch an earlier runner placed has no task here. If this runner's fleet
//! still binds its request, the same `Cancel` reaches its machine and that
//! machine's report settles it. If nothing binds it, its durable record is all
//! that holds the branch, and the cancel ends that record itself: a detached
//! row settles `lost` and a parked call resumes once, both saying the outcome
//! is unknown, because the batch may have run before the restart. A machine
//! whose report still lists the attempt is also told to cancel it. Its result,
//! if one arrives after all, is reported to the job and never published
//! ([`super::detached::report_late`]). A result that has already arrived and
//! is being published is not cancelled: it stands.
//!
//! Exactly one batch: only that control, that fleet request, and that batch's
//! durable records are touched, never the session, the turn, the job, or a
//! sibling batch.

use super::{adopted, detached, Interruption};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// How long a cancel waits for its batch to leave this runner and the fleet
/// before answering that it is still stopping.
pub(crate) const CANCEL_SETTLE_WAIT: std::time::Duration = std::time::Duration::from_secs(20);

/// What a cancel did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CancelReport {
    /// It was stopped before settlement began, so it publishes nothing.
    /// `settled` when its task and its fleet request both ended within the
    /// wait, which is when its durable rows release the branch.
    Stopped { settled: bool },
    /// Its commands finished on their own before the cancel reached it, and
    /// it settled with its own result.
    FinishedFirst { verdict: String },
    /// Settlement had already begun: its result, and any commit, stands.
    AlreadySettling,
    /// No task in this runner holds it, but the fleet did: its machine was
    /// told to cancel it, and that machine's report settles it.
    Retracted,
    /// No task in this runner and no fleet binding held it, so its durable
    /// record was settled as lost, which releases its branch.
    SettledUnheld,
    /// Whether its result had already arrived could not be read, so nothing
    /// was touched.
    Unread { error: String },
    /// Nothing in this runner, its fleet, or its durable records holds it.
    NotLive,
}

impl CancelReport {
    /// The cancel's answer, for the principal that asked.
    pub(crate) fn sentence(&self, request_id: &str) -> String {
        match self {
            Self::Stopped { settled: true } => format!(
                "Cancelled run batch `{request_id}`: it settled as cancelled and committed nothing, and its machine work has ended, so it no longer holds its branch. A call waiting on it resumes once with that result; a detached batch's result reaches its job as a notice."
            ),
            Self::Stopped { settled: false } => format!(
                "Cancelling run batch `{request_id}`: it was stopped before it began publishing, so it commits nothing, but its machine has not yet confirmed the cancel. It settles as cancelled when it does, and holds its branch until then."
            ),
            Self::FinishedFirst { verdict } => format!(
                "Run batch `{request_id}` finished on its own before the cancel reached it ({verdict}); its own result stands, and any commit it made is on the branch."
            ),
            Self::AlreadySettling => format!(
                "Run batch `{request_id}` was not cancelled: it had already begun publishing its result, so any commit it carries stands, and a replay carries it like any other commit. It releases its branch once it finishes publishing."
            ),
            Self::Retracted => format!(
                "Asked the machine running batch `{request_id}` to cancel it. No task in this runner holds it (an earlier runner placed it), so it settles from that machine's own report."
            ),
            Self::SettledUnheld => format!(
                "Cancelled run batch `{request_id}`: no runner task or machine held it, so it was settled as lost (outcome unknown) and no longer holds its branch. A result that arrives later is reported to the job and not published."
            ),
            Self::Unread { error } => format!(
                "Nothing was cancelled: Cairn could not read whether a result for run batch `{request_id}` had already arrived ({error}). Try again."
            ),
            Self::NotLive => format!(
                "Nothing was cancelled: no task in this runner holds run batch `{request_id}`, no machine holds its request, and nothing durable waits on it."
            ),
        }
    }

    /// Whether the cancel reached a batch at all.
    pub(crate) fn reached(&self) -> bool {
        !matches!(self, Self::NotLive | Self::Unread { .. })
    }
}

/// Cancel the one batch placed under `request_id`, on behalf of `by` (a
/// node or task home URI, or `operator`), waiting up to `settle_wait` for it
/// to leave this runner and the fleet. `db` is the database holding the
/// batch's durable records.
pub(crate) async fn cancel_batch(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    by: &str,
    settle_wait: std::time::Duration,
) -> CancelReport {
    let Some(control) = detached::live_control(request_id) else {
        // A batch an earlier runner placed is still the fleet's request: the
        // same `Cancel` ends it, and its machine's report settles its row.
        return match orch.cancel_cell_request(request_id) {
            crate::fleet::CancelOutcome::Unknown => cancel_unheld(orch, db, request_id, by).await,
            _ => CancelReport::Retracted,
        };
    };
    if control.settling() {
        return CancelReport::AlreadySettling;
    }
    control.set_cancelled_by(by);
    control.interrupt(Interruption::Cancelled(format!("cancelled by {by}")));
    control.stop(orch);
    log::info!("run batch {request_id} cancelled by {by}");
    let deadline = tokio::time::Instant::now() + settle_wait;
    // Settled once its task and fleet request have ended and its resolver has
    // written what it owes: the wait's resolution or the detached row. Those
    // durable rows are what hold the branch.
    let settled = loop {
        if !detached::batch_is_live(orch, request_id) && !detached::has_live_resolver(request_id) {
            break true;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    // The process may have finished on its own between the gate read above
    // and the stop reaching it; its verdict then says so.
    match control.settled_verdict() {
        Some(verdict) if settled && !cancelled_verdict(&verdict) => {
            CancelReport::FinishedFirst { verdict }
        }
        _ => CancelReport::Stopped { settled },
    }
}

/// End the durable records of a batch no task in this runner and no fleet
/// binding holds: its detached row, and a parked call waiting on it.
async fn cancel_unheld(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    by: &str,
) -> CancelReport {
    match adopted::has_open_adoption(db, request_id).await {
        Ok(false) => {}
        Ok(true) => return CancelReport::AlreadySettling,
        Err(error) => {
            return CancelReport::Unread {
                error: error.to_string(),
            }
        }
    }
    // A machine whose report still lists the attempt is told to end it. The
    // records below end either way; its result, if one comes, is reported.
    if let crate::fleet::RequestReport::Held { executor_id } =
        orch.fleet.request_report(request_id, None)
    {
        let told = orch.fleet.cancel_reported_attempts(request_id);
        log::info!("run batch {request_id}: told {told} attempt(s) on {executor_id} to cancel");
    }
    let wait =
        match crate::mcp::handlers::durable_suspend::pending_run_batch_wait(db, request_id).await {
            Ok(wait) => wait,
            Err(error) => return CancelReport::Unread { error },
        };
    let row = match detached::load(db, request_id).await {
        Ok(row) => row,
        Err(error) => return CancelReport::Unread { error },
    };
    // A loss already written whose call was never resumed (the runner stopped
    // between the two): finish what that cancel began.
    if let (Some(batch), Some(record)) = (&row, &wait) {
        if batch.state == detached::DetachedState::Lost {
            adopted::resolve_wait(
                orch,
                db,
                record,
                request_id,
                &detached::lost_wait_text(batch),
            )
            .await;
            return CancelReport::SettledUnheld;
        }
    }
    // A parked call with no row gets one first, so the loss below is the one
    // durable record every arbiter reads: written lost only while no adopted
    // result is open, and, once written, the reason a later result is no
    // longer the wait's to adopt but is reported against the row instead.
    if let (None, Some(record)) = (&row, &wait) {
        if let Err(error) = convert_wait(db, record).await {
            return CancelReport::Unread { error };
        }
    }
    let unsettled = match detached::load(db, request_id).await {
        Ok(Some(batch)) => matches!(
            batch.state,
            detached::DetachedState::Parked | detached::DetachedState::Running
        ),
        Ok(None) => false,
        Err(error) => return CancelReport::Unread { error },
    };
    if !unsettled {
        return CancelReport::NotLive;
    }
    match detached::cancel_unheld(orch, db, request_id, by).await {
        Ok(Some(_)) => {}
        // Something settled it first, or its result was adopted.
        Ok(None) => {
            return match detached::load(db, request_id).await {
                Ok(Some(batch)) if batch.state == detached::DetachedState::Settled => {
                    CancelReport::FinishedFirst {
                        verdict: batch.verdict.unwrap_or_else(|| "finished".into()),
                    }
                }
                _ => CancelReport::AlreadySettling,
            }
        }
        Err(error) => return CancelReport::Unread { error },
    }
    // Should the runner stop before this, the loss written above still
    // resumes the call: `adopted::judge_unheard` answers a parked call whose
    // batch is lost from that loss before it asks any machine.
    if let Some(record) = wait {
        let commits = matches!(
            record.condition,
            crate::mcp::handlers::durable_suspend::Condition::RunBatch { commits: true, .. }
        );
        adopted::resolve_wait(
            orch,
            db,
            &record,
            request_id,
            &unheld_cancel_text(request_id, by, commits),
        )
        .await;
    }
    log::info!("run batch {request_id}, held by nothing, settled as lost on {by}'s cancel");
    CancelReport::SettledUnheld
}

/// Record a parked call's batch as a detached row, in one write that first
/// checks the wait is still pending: a wait already resolving holds its real
/// answer, and that answer stands.
async fn convert_wait(
    db: &LocalDb,
    record: &crate::mcp::handlers::durable_suspend::Record,
) -> Result<(), String> {
    let record = record.clone();
    db.write(|c| {
        let record = record.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT 1 FROM agent_waits WHERE id=?1 AND state='pending'",
                    cairn_db::turso::params![record.id.as_str()],
                )
                .await?;
            let pending = rows.next().await?.is_some();
            drop(rows);
            if pending {
                detached::insert_converted(
                    c,
                    &record.job_id,
                    &record.run_id,
                    &record.condition,
                    "its owner cancelled it while no runner task or machine held it",
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("record the cancelled wait's batch: {error}"))
}

/// What a parked call waiting on a batch nothing held is told when the batch
/// is cancelled.
pub(crate) fn unheld_cancel_text(request_id: &str, by: &str, commits: bool) -> String {
    detached::lost_text_for_wait(request_id, &detached::cancel_evidence(by), commits)
}

/// Whether a settled verdict is a cancellation's (or a stop before any
/// machine claimed the batch).
fn cancelled_verdict(verdict: &str) -> bool {
    verdict.starts_with("it was cancelled") || verdict.starts_with("it was never placed")
}
