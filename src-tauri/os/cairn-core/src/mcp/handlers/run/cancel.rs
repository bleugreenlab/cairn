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
//! A cancel never revokes the batch's object-plane grant: that lives until
//! the batch's result is acknowledged or the batch is lost. Whether the
//! cancel ended the commands is decided on the machine, against its own
//! commands-ended fence. A machine whose cancel killed the commands publishes
//! nothing and answers `Cancelled`, which settles naming the cancel; one
//! already past its commands finishes its upload and answers with its result,
//! and a completed result's commit stands. Once this runner has seen the
//! batch's result upload arrive at its object server (or settlement has
//! begun), a cancel is not sent at all: it is recorded on the batch as
//! having arrived while publishing, and the batch settles at its result. The
//! observed upload only names that phase; the commit stands only on the
//! machine's completed result.
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
    /// It was told to stop before it was known to be publishing. Whether the
    /// stop ended its commands is its machine's to decide, so its result says
    /// what happened. `settled` when its task and its fleet request both ended
    /// within the wait (with a cancelled result), which is when its durable
    /// rows release the branch.
    Stopped { settled: bool },
    /// Its commands finished on their own before the cancel reached it, and
    /// it settled with its own result.
    FinishedFirst { verdict: String },
    /// Its result upload had been seen, or settlement had begun, so the cancel
    /// was recorded and not sent; the batch settles at its result.
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
                "Cancelling run batch `{request_id}`: its machine was told to stop it, and has not answered yet. If the cancel ended its commands it commits nothing; if its commands had already finished, it publishes its result and any commit stands. Its result says which, and it holds its branch until that result settles."
            ),
            Self::FinishedFirst { verdict } => format!(
                "Run batch `{request_id}` finished on its own before the cancel reached it ({verdict}); its own result stands, and any commit it made is on the branch."
            ),
            Self::AlreadySettling => format!(
                "Run batch `{request_id}` left to publish; settles at result. Its commands had finished and it was publishing its result when the cancel arrived, so the cancel was recorded on it and not sent to its machine. If that result is a completion, its commit stands. It releases its branch when the result settles."
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
    let control = detached::live_control(request_id);
    // A result already being published stands: the cancel is recorded on it
    // and not sent, and even a batch page for a suspended call is too late.
    if control
        .as_ref()
        .is_some_and(|control| control.settling() || control.commands_ended())
        || orch.fleet.request_is_publishing(request_id)
    {
        return publication_cancel(db, request_id, by).await;
    }
    // Publication already owns an adopted result, just as a live settling
    // control does.
    if control.is_none() {
        match adopted::has_open_adoption(db, request_id).await {
            Ok(true) => return publication_cancel(db, request_id, by).await,
            Ok(false) => {}
            Err(error) => {
                return CancelReport::Unread {
                    error: error.to_string(),
                }
            }
        }
    }
    // A suspended call needs a durable batch page too. Create it before
    // recording attribution or sending Cancel, so the cancel's notes reach
    // that row as well as the wait; a resolving wait is untouched.
    match crate::mcp::handlers::durable_suspend::pending_run_batch_wait(db, request_id).await {
        Ok(Some(record)) => {
            if let Err(error) = convert_wait(db, &record).await {
                return CancelReport::Unread { error };
            }
        }
        Ok(None) => {}
        Err(error) => return CancelReport::Unread { error },
    }
    let Some(control) = control else {
        // Recorded before the cancel is sent: with no task here, the durable
        // settlement is all that names the cancel when its machine's
        // `Cancelled` answer settles, so a cancel it cannot record is not sent.
        let cancel = super::DurableCancel::now(by);
        if let Err(error) = detached::record_cancel(db, request_id, &cancel).await {
            return CancelReport::Unread { error };
        }
        // Read only once the grant is genuinely retired, by an upload that
        // arrives after settlement.
        orch.object_plane.note_request_refusal_for_request(
            request_id,
            cancel.detail(true, false).trim_end_matches('.'),
        );
        // A batch an earlier runner placed is still the fleet's request: the
        // same `Cancel` ends it, and its machine's report settles its row.
        return match orch.cancel_cell_request(request_id) {
            crate::fleet::CancelOutcome::Unknown => cancel_unheld(orch, db, request_id, by).await,
            _ => CancelReport::Retracted,
        };
    };
    // Checked again: publication may have begun during the writes above.
    if control.settling()
        || control.commands_ended()
        || orch.fleet.request_is_publishing(request_id)
    {
        return publication_cancel(db, request_id, by).await;
    }
    control.set_cancelled_by(by);
    if let Some(cause) = control.cancelled_detail() {
        orch.object_plane
            .note_request_refusal_for_request(request_id, cause.trim_end_matches('.'));
    }
    // A restart before the machine answers hands that answer to a later
    // runner, which names the cancel from the batch's durable record.
    if let Some(cancel) = control.cancelled() {
        if let Err(error) = detached::record_cancel(db, request_id, &cancel).await {
            log::warn!("run batch {request_id}: {error}");
        }
    }
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

async fn publication_cancel(db: &LocalDb, request_id: &str, by: &str) -> CancelReport {
    match detached::record_publication_cancel(db, request_id, by).await {
        Ok(()) => CancelReport::AlreadySettling,
        Err(error) => CancelReport::Unread { error },
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
    if orch.fleet.request_is_publishing(request_id) {
        return publication_cancel(db, request_id, by).await;
    }
    match adopted::has_open_adoption(db, request_id).await {
        Ok(false) => {}
        Ok(true) => return publication_cancel(db, request_id, by).await,
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
        orch.fleet.settle_request_publication(request_id);
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
    orch.fleet.settle_request_publication(request_id);
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
                    &record.id,
                    &record.job_id,
                    &record.run_id,
                    &record.condition,
                    detached::INLINE_CAPTURE,
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

/// Cancel all admitted mutating work of these owners, not just detached calls.
pub(crate) async fn settle_owned_batches(
    orch: &Orchestrator,
    db: &LocalDb,
    job_ids: &[String],
    resolution: crate::transitions::Resolution,
    fallback_uri: &str,
) -> Vec<crate::issues::status::SettlementResource> {
    use crate::issues::status::{SettlementOutcome, SettlementResource};
    let mut requests = std::collections::BTreeMap::new();
    for (id, owner) in detached::live_mutating_owners() {
        if job_ids.contains(&owner.job_id) {
            requests.insert(id, owner.job_id);
        }
    }
    let mut reports = Vec::new();
    match detached::all_running(db).await {
        Ok(rows) => {
            for row in rows {
                if row.commits && job_ids.contains(&row.job_id) {
                    requests.insert(row.request_id, row.job_id);
                }
            }
        }
        Err(error) => reports.push(SettlementResource {
            uri: fallback_uri.into(),
            outcome: SettlementOutcome::CleanupPending,
            diagnostic: Some(error),
        }),
    }
    // A parked batch from an earlier runner may have no detached row yet.
    for job in job_ids {
        match db.query_all(
            "SELECT condition_json FROM agent_waits WHERE job_id=?1 AND state IN ('pending','resolving') AND kind='run_batch'",
            (job.clone(),), |row| Ok(row.get::<String>(0)?),
        ).await {
            Ok(rows) => for json in rows {
                if let Ok(crate::mcp::handlers::durable_suspend::Condition::RunBatch {
                    request_id, commits: true, ..
                }) = serde_json::from_str(&json) { requests.insert(request_id, job.clone()); }
            },
            Err(error) => reports.push(SettlementResource {
                uri: fallback_uri.into(), outcome: SettlementOutcome::CleanupPending,
                diagnostic: Some(error.to_string()),
            }),
        }
    }
    for (id, job) in requests {
        let home = crate::jobs::queries::home_uri_for_job(db, &job)
            .await
            .ok()
            .flatten();
        let cause = match resolution {
            crate::transitions::Resolution::Merged => crate::fleet::CancelCause::IssueMerged,
            crate::transitions::Resolution::Closed => crate::fleet::CancelCause::IssueClosed,
        };
        orch.fleet.cancel_request_with_cause(&id, cause);
        let by = match resolution {
            crate::transitions::Resolution::Merged => "issue resolution (merged)",
            crate::transitions::Resolution::Closed => "issue resolution (closed)",
        };
        // An unbound request is not proof that an earlier executor stopped.
        // Preserve its running row until that executor's report proves absence.
        let report = if detached::live_control(&id).is_none() {
            match orch.fleet.request_report(&id, None) {
                crate::fleet::RequestReport::Held { .. } => {
                    orch.fleet.cancel_reported_attempts(&id);
                    CancelReport::Retracted
                }
                crate::fleet::RequestReport::Unknown { .. } => CancelReport::Unread {
                    error: "waiting for its executor to report whether work has ended".into(),
                },
                crate::fleet::RequestReport::Completed(_) => CancelReport::AlreadySettling,
                crate::fleet::RequestReport::Gone => {
                    cancel_batch(orch, db, &id, by, std::time::Duration::ZERO).await
                }
            }
        } else {
            cancel_batch(orch, db, &id, by, std::time::Duration::ZERO).await
        };
        let outcome = match report {
            CancelReport::Stopped { settled: true } | CancelReport::SettledUnheld => {
                SettlementOutcome::Cancelled
            }
            CancelReport::FinishedFirst { .. } | CancelReport::NotLive => {
                SettlementOutcome::AlreadySettled
            }
            _ => SettlementOutcome::CleanupPending,
        };
        let diagnostic =
            (outcome == SettlementOutcome::CleanupPending).then(|| report.sentence(&id));
        reports.push(SettlementResource {
            uri: home
                .map(|home| format!("{home}/batches/{id}"))
                .unwrap_or_else(|| fallback_uri.into()),
            outcome,
            diagnostic,
        });
    }
    reports
}
