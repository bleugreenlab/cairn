//! Results a runner process adopts for `run` batches an earlier one started.
//!
//! An executor retains every result it sends until the runner acknowledges it,
//! and sends all of them again at the start of each new link session. A runner
//! process that restarted holds no in-memory binding for work its predecessor
//! granted, so the fleet offers such a result here instead of settling it
//! ([`crate::fleet::UnboundResult`]). Three rules govern what follows.
//!
//! - **Acknowledge only a durable outcome.** A result nothing durable claims
//!   (no pending `agent_waits` run-batch wait, no running `run_batch_detached`
//!   row) is acknowledged as soon as that is read. A claimed result is recorded
//!   as an adopted row and acknowledged only once its driver has recorded the
//!   settled outcome, because the acknowledgement also ends its machine's pin
//!   on the batch's delta, and the driver may still need that delta uploaded
//!   again. When the rows cannot be read, nothing is acknowledged. A result
//!   never acknowledged is sent again; one acknowledged early is lost.
//! - **Settle exactly once.** One driver claims each adopted row by a
//!   compare-and-set on its state and this process's incarnation, publishes the
//!   batch's commit through the ordinary [`super::settle_adopted_result`] path,
//!   records the composed result, and delivers it through the wait's own
//!   exactly-once resolution or the detached row's. A row a dead process left
//!   mid-publication is claimed again, and publication finds a landing that
//!   process made by the key it recorded in the store's operation log.
//! - **Judge by evidence, never by the restart.** A wait nothing here can hear
//!   stays parked until its machine says what became of the batch
//!   ([`judge_unheard`]). Only after [`RELEASE_AFTER`] of this process's own
//!   uptime with a machine still unaccounted for is the call released, into a
//!   detached row whose branch hold stands and which the batch's result still
//!   settles when its machine returns.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_common::executor_protocol::{CellOutcome, ReauthorizedTransfer};
use cairn_db::turso::params;

use crate::fleet::{RequestReport, UnboundResult};
use crate::mcp::handlers::durable_suspend::{self, Condition, Record};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

use super::{detached, PersistedSettlement, PlacedOn};

/// How long into this process's own uptime a wait whose machine has not
/// reported is held parked before its call is released (tenet 973: a judge
/// cannot measure silence for the time it was not running).
pub(crate) const RELEASE_AFTER: Duration = Duration::from_secs(5 * 60);

/// How long a driver waits for a machine to re-upload a delta.
const REAUTHORIZE_BOUND: Duration = Duration::from_secs(120);

/// How far before its durable row was written a batch's landing is looked
/// for. The row is written before the batch settles, so this only absorbs
/// clock skew between the store's operation log and the database.
const PUBLICATION_LOOKBACK_MS: i64 = 10 * 60 * 1000;

/// This runner process's adoption state.
pub(crate) struct AdoptionState {
    /// Distinct per orchestrator, so a row claimed by any other value was
    /// claimed by a process that has ended.
    incarnation: String,
    booted: Mutex<Instant>,
    /// Results whose rows could not be read yet, retried each sweep.
    undecided: Mutex<Vec<UnboundResult>>,
    /// Requests a driver in this process is settling now.
    driving: Mutex<HashSet<String>>,
}

impl Default for AdoptionState {
    fn default() -> Self {
        Self {
            incarnation: uuid::Uuid::new_v4().to_string(),
            booted: Mutex::new(Instant::now()),
            undecided: Mutex::default(),
            driving: Mutex::default(),
        }
    }
}

impl AdoptionState {
    /// How long this runner process has been up.
    fn uptime(&self) -> Duration {
        self.booted.lock().unwrap().elapsed()
    }

    /// Pretend this process has been up `by` longer than it has.
    #[cfg(test)]
    pub(crate) fn backdate_for_test(&self, by: Duration) {
        let mut booted = self.booted.lock().unwrap();
        *booted = booted.checked_sub(by).unwrap_or(*booted);
    }
}

/// Start taking the fleet's unbound results. Everything that arrived before
/// this was held, unacknowledged, and is decided now.
pub(crate) fn start(orch: &Orchestrator) {
    let (sink, mut results) = tokio::sync::mpsc::unbounded_channel::<UnboundResult>();
    orch.fleet.install_unbound_result_sink(sink);
    let orch = orch.clone();
    tokio::spawn(async move {
        while let Some(result) = results.recv().await {
            adopt(&orch, result).await;
        }
    });
}

/// What a durable row says about a batch whose result arrived unbound.
struct Claim {
    job_id: String,
    /// When the row was written, which bounds the publication lookup.
    created_ms: i64,
    commits: bool,
    settlement: Option<PersistedSettlement>,
}

/// The unsettled durable row that claims `request_id`, if one does: a pending
/// run-batch wait, else a running detached row. A wait already `resolving`
/// holds its answer and is not claimed: that answer is what reaches the call.
/// Read through `idx_agent_waits_pending_run_batch`, whose predicate this
/// spells exactly.
async fn claim_of(
    c: &cairn_db::storage::TrackedConnection,
    request_id: &str,
) -> DbResult<Option<Claim>> {
    let mut rows = c
        .query(
            "SELECT job_id,condition_json,created_at FROM agent_waits INDEXED BY idx_agent_waits_pending_run_batch WHERE state = 'pending' AND kind = 'run_batch' AND json_extract(condition_json,'$.request_id')=?1 LIMIT 1",
            params![request_id],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let job_id = row.text(0)?;
        let condition = serde_json::from_str::<Condition>(&row.text(1)?).ok();
        let created_ms = row.i64(2)?;
        let (commits, settlement) = match condition {
            Some(Condition::RunBatch {
                commits,
                settlement,
                ..
            }) => (commits, settlement.map(|settlement| *settlement)),
            _ => (false, None),
        };
        return Ok(Some(Claim {
            job_id,
            created_ms,
            commits,
            settlement,
        }));
    }
    drop(rows);
    let mut rows = c
        .query(
            "SELECT job_id,created_ms,commits,settlement_json FROM run_batch_detached WHERE request_id=?1 AND state='running'",
            params![request_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(Claim {
        job_id: row.text(0)?,
        created_ms: row.i64(1)?,
        commits: row.i64(2)? != 0,
        settlement: row
            .opt_text(3)?
            .and_then(|json| serde_json::from_str(&json).ok()),
    }))
}

async fn read_claim(db: &LocalDb, request_id: &str) -> Result<Option<Claim>, String> {
    let id = request_id.to_string();
    db.read(move |c| {
        let id = id.clone();
        Box::pin(async move { claim_of(c, &id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

/// What [`adopt`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Adoption {
    /// A durable row claims the batch; the result is recorded, and its driver
    /// acknowledges it once the settled outcome is recorded too.
    Adopted,
    /// Nothing durable claims it; it is acknowledged and dropped, by name.
    Unclaimed,
    /// The rows could not be read; nothing is acknowledged.
    Undecided,
}

/// Decide what an unbound result is for and record that durably. Only an
/// unclaimed result is acknowledged here; see the module's first rule.
pub(crate) async fn adopt(orch: &Orchestrator, result: UnboundResult) -> Adoption {
    let UnboundResult {
        executor_id,
        machine,
        request_id,
        attempt_id,
        ..
    } = result.clone();
    // A batch this process placed is its own path's to settle; its binding is
    // gone because that path already settled or abandoned this attempt.
    if detached::placed_here(&request_id) {
        log::info!(
            "result of request {request_id} attempt {attempt_id} from {machine} arrived after this runner settled its attempt; acknowledged without effect"
        );
        orch.fleet
            .send_result_ack(&executor_id, &request_id, &attempt_id);
        return Adoption::Unclaimed;
    }
    let outcome_json = match serde_json::to_string(&result.outcome) {
        Ok(json) => json,
        Err(error) => {
            log::error!("result of request {request_id} could not be recorded ({error}); it stays on {machine}");
            return Adoption::Undecided;
        }
    };
    let dbs = orch.db.all_dbs().await;
    if dbs.is_empty() {
        orch.adoption.undecided.lock().unwrap().push(result);
        return Adoption::Undecided;
    }
    let mut undecided = false;
    for db in dbs {
        let row = (
            request_id.clone(),
            attempt_id.clone(),
            executor_id.clone(),
            machine.clone(),
            outcome_json.clone(),
        );
        let recorded = db
            .write(|c| {
                let (request_id, attempt_id, executor_id, machine, outcome_json) = row.clone();
                Box::pin(async move {
                    let Some(claim) = claim_of(c, &request_id).await? else {
                        return Ok(None);
                    };
                    c.execute(
                        "INSERT OR IGNORE INTO run_batch_adopted_outcomes(request_id,attempt_id,job_id,executor_id,machine,outcome_json,state,received_ms) VALUES(?1,?2,?3,?4,?5,?6,'received',?7)",
                        params![
                            request_id.as_str(),
                            attempt_id.as_str(),
                            claim.job_id,
                            executor_id,
                            machine,
                            outcome_json,
                            chrono::Utc::now().timestamp_millis()
                        ],
                    )
                    .await?;
                    let mut rows = c
                        .query(
                            "SELECT state FROM run_batch_adopted_outcomes WHERE request_id=?1 AND attempt_id=?2",
                            params![request_id, attempt_id],
                        )
                        .await?;
                    let state = match rows.next().await? {
                        Some(row) => row.text(0)?,
                        None => "received".to_string(),
                    };
                    Ok(Some(state))
                })
            })
            .await;
        match recorded {
            Ok(Some(state)) => {
                log::info!(
                    "adopted the result of request {request_id} attempt {attempt_id} from {machine} (row {state}), which a batch this runner did not start still waits for"
                );
                // A resend whose earlier acknowledgement was lost. A row not
                // yet delivered is acknowledged by its driver.
                if state == "delivered" {
                    orch.fleet
                        .send_result_ack(&executor_id, &request_id, &attempt_id);
                } else {
                    let owned = orch.clone();
                    tokio::spawn(async move { drive(&owned, &request_id).await });
                }
                return Adoption::Adopted;
            }
            Ok(None) => {}
            Err(error) => {
                log::warn!("could not read whether a batch claims the result of request {request_id}: {error}");
                undecided = true;
            }
        }
    }
    if undecided {
        orch.adoption.undecided.lock().unwrap().push(result);
        return Adoption::Undecided;
    }
    log::info!(
        "result of request {request_id} attempt {attempt_id} from {machine}: no unsettled batch claims it (already settled, or never a run batch); acknowledged without effect"
    );
    orch.fleet
        .send_result_ack(&executor_id, &request_id, &attempt_id);
    Adoption::Unclaimed
}

/// One `run_batch_adopted_outcomes` row a driver works.
struct AdoptedRow {
    request_id: String,
    attempt_id: String,
    executor_id: String,
    machine: String,
    outcome_json: String,
    state: String,
    report: Option<String>,
}

const OPEN_ROWS: &str = "SELECT request_id,attempt_id,executor_id,machine,outcome_json,state,report FROM run_batch_adopted_outcomes WHERE state <> 'delivered'";

fn adopted_row(row: &cairn_db::turso::Row) -> DbResult<AdoptedRow> {
    Ok(AdoptedRow {
        request_id: row.text(0)?,
        attempt_id: row.text(1)?,
        executor_id: row.text(2)?,
        machine: row.text(3)?,
        outcome_json: row.text(4)?,
        state: row.text(5)?,
        report: row.opt_text(6)?,
    })
}

/// Whether an adopted, undelivered result exists for this request. A caller
/// that cannot read this must not judge the batch: its driver may be
/// publishing it.
pub(crate) async fn has_open_adoption(db: &LocalDb, request_id: &str) -> DbResult<bool> {
    Ok(db
        .query_opt(
            format!("{OPEN_ROWS} AND request_id=?1 LIMIT 1"),
            params![request_id.to_string()],
            |row| row.text(0),
        )
        .await?
        .is_some())
}

/// Settle every adopted result of one request, exactly once.
pub(crate) async fn drive(orch: &Orchestrator, request_id: &str) {
    if !orch
        .adoption
        .driving
        .lock()
        .unwrap()
        .insert(request_id.to_string())
    {
        return;
    }
    struct Driving<'a>(&'a AdoptionState, String);
    impl Drop for Driving<'_> {
        fn drop(&mut self) {
            self.0.driving.lock().unwrap().remove(&self.1);
        }
    }
    let _driving = Driving(&orch.adoption, request_id.to_string());
    for db in orch.db.all_dbs().await {
        let rows = db
            .query_all(
                format!("{OPEN_ROWS} AND request_id=?1 ORDER BY received_ms"),
                params![request_id.to_string()],
                adopted_row,
            )
            .await;
        let Ok(rows) = rows else { continue };
        for row in rows {
            drive_row(orch, &db, row).await;
        }
    }
}

/// Why a driver stopped short of a settlement.
enum Interrupted {
    /// Try again later from `received`: the machine's link is down, or the
    /// database refused a write.
    Retry(String),
}

async fn drive_row(orch: &Orchestrator, db: &Arc<LocalDb>, row: AdoptedRow) {
    let incarnation = orch.adoption.incarnation.clone();
    if row.state == "published" {
        let text = row.report.clone().unwrap_or_default();
        orch.fleet
            .send_result_ack(&row.executor_id, &row.request_id, &row.attempt_id);
        deliver(orch, db, &row, &detached::verdict_from_text(&text), &text).await;
        return;
    }
    let claimed = db
        .execute(
            "UPDATE run_batch_adopted_outcomes SET state='publishing',claimed_by_incarnation=?3 WHERE request_id=?1 AND attempt_id=?2 AND (state='received' OR (state='publishing' AND COALESCE(claimed_by_incarnation,'')<>?3))",
            params![row.request_id.as_str(), row.attempt_id.as_str(), incarnation.as_str()],
        )
        .await;
    match claimed {
        Ok(1) => {}
        Ok(_) => return,
        Err(error) => {
            log::warn!(
                "could not claim adopted result of request {}: {error}",
                row.request_id
            );
            return;
        }
    }
    let settled = settle(orch, db, &row).await;
    let (text, verdict) = match settled {
        Ok(settled) => settled,
        Err(Interrupted::Retry(why)) => {
            log::info!(
                "adopted result of request {} waits to be settled: {why}",
                row.request_id
            );
            let _ = db
                .execute(
                    "UPDATE run_batch_adopted_outcomes SET state='received',claimed_by_incarnation=NULL WHERE request_id=?1 AND attempt_id=?2 AND state='publishing' AND claimed_by_incarnation=?3",
                    params![row.request_id.as_str(), row.attempt_id.as_str(), incarnation.as_str()],
                )
                .await;
            return;
        }
    };
    let commit = super::run_commit_sha(&text);
    let recorded = db
        .execute(
            "UPDATE run_batch_adopted_outcomes SET state='published',report=?3,published_commit=?4,settled_ms=?5 WHERE request_id=?1 AND attempt_id=?2 AND state='publishing' AND claimed_by_incarnation=?6",
            params![
                row.request_id.as_str(),
                row.attempt_id.as_str(),
                text.as_str(),
                commit.as_deref(),
                chrono::Utc::now().timestamp_millis(),
                incarnation.as_str()
            ],
        )
        .await;
    match recorded {
        // The outcome is durable, so its machine may let the result and its
        // delta go.
        Ok(1) => orch
            .fleet
            .send_result_ack(&row.executor_id, &row.request_id, &row.attempt_id),
        // Delivered anyway, unacknowledged: the result is in hand, its machine
        // keeps it, and a re-drive finds any landing this made by its key
        // rather than landing it twice.
        Ok(_) => log::warn!(
            "adopted request {} was claimed away while it settled",
            row.request_id
        ),
        Err(error) => log::warn!(
            "could not record the settlement of adopted request {}: {error}",
            row.request_id
        ),
    }
    deliver(orch, db, &row, &verdict, &text).await;
}

/// Publish and compose one adopted result.
async fn settle(
    orch: &Orchestrator,
    db: &LocalDb,
    row: &AdoptedRow,
) -> Result<(String, String), Interrupted> {
    let mut outcome = serde_json::from_str::<CellOutcome>(&row.outcome_json).map_err(|error| {
        Interrupted::Retry(format!("its recorded outcome does not decode: {error}"))
    })?;
    let claim = read_claim(db, &row.request_id)
        .await
        .map_err(Interrupted::Retry)?;
    let Some(claim) = claim else {
        // Settled by another path while this was adopted (an operator stop, a
        // compaction): compose it for the record, deliver it to nothing.
        return Ok(super::compose_adopted_result(&outcome, &row.machine, None));
    };
    let Some(settlement) = claim.settlement.as_ref() else {
        let unpublished = claim.commits.then_some(
            "its durable record predates what Cairn needs to publish it after a restart",
        );
        return Ok(super::compose_adopted_result(
            &outcome,
            &row.machine,
            unpublished,
        ));
    };
    if settlement.commits() {
        if let Err(reason) = recover_delta(orch, settlement, row, &mut outcome).await? {
            return Ok(super::compose_adopted_result(
                &outcome,
                &row.machine,
                Some(&reason),
            ));
        }
    }
    Ok(super::settle_adopted_result(
        orch,
        settlement,
        claim.created_ms - PUBLICATION_LOOKBACK_MS,
        outcome,
        &row.machine,
    )
    .await)
}

/// Make an adopted outcome's sealed delta publishable from this process: its
/// staged upload died with the process that received it, so the machine is
/// asked to upload it again under its current link. The inner `Err` is the
/// reason it cannot be published, for the precise report.
async fn recover_delta(
    orch: &Orchestrator,
    settlement: &PersistedSettlement,
    row: &AdoptedRow,
    outcome: &mut CellOutcome,
) -> Result<Result<(), String>, Interrupted> {
    let CellOutcome::Completed {
        mutation_delta: Some(delta),
        ..
    } = outcome
    else {
        return Ok(Ok(()));
    };
    if let Some(receipt) = delta.upload_receipt.as_ref() {
        if orch.object_plane.staged_delta(receipt).is_some() {
            return Ok(Ok(()));
        }
    }
    let Some(mut request) = settlement.routed_request().cloned() else {
        return Ok(Err(
            "its durable record does not name the request it was placed under".into(),
        ));
    };
    let Some(generation) = orch.fleet.link_generation(&row.executor_id) else {
        return Err(Interrupted::Retry(format!(
            "{} is not attached to re-upload its delta",
            row.machine
        )));
    };
    request.attempt_id = row.attempt_id.clone();
    orch.object_plane
        .authorize_request(&request, &row.executor_id, generation);
    let answer = orch
        .fleet
        .reauthorize_transfers(
            &row.executor_id,
            generation,
            &row.request_id,
            &row.attempt_id,
        )
        .map_err(|error| {
            Interrupted::Retry(format!("asking {} for its delta: {error}", row.machine))
        })?;
    let transfers = match tokio::time::timeout(REAUTHORIZE_BOUND, answer).await {
        Ok(Ok(transfers)) => transfers,
        Ok(Err(_)) => {
            return Err(Interrupted::Retry(format!(
                "{}'s link ended before it answered for the delta",
                row.machine
            )))
        }
        Err(_) if orch.fleet.link_generation(&row.executor_id) != Some(generation) => {
            return Err(Interrupted::Retry(format!(
                "{} reconnected while it was asked for the delta",
                row.machine
            )))
        }
        Err(_) => {
            return Ok(Err(format!(
                "{} did not upload its delta again within {} minutes",
                row.machine,
                REAUTHORIZE_BOUND.as_secs() / 60
            )))
        }
    };
    let receipt = transfers
        .into_iter()
        .map(|transfer| match transfer {
            ReauthorizedTransfer::Delta(receipt) => receipt,
        })
        .next();
    match receipt {
        Some(Ok(receipt)) => {
            delta.upload_receipt = Some(receipt);
            Ok(Ok(()))
        }
        Some(Err(reason)) => Ok(Err(format!(
            "{} could not upload it again ({reason})",
            row.machine
        ))),
        None => Ok(Err(format!("{} holds no delta for it", row.machine))),
    }
}

/// Hand a settled result to the wait or the detached row that claims it,
/// through each one's own exactly-once path, then mark the row delivered.
async fn deliver(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    row: &AdoptedRow,
    verdict: &str,
    text: &str,
) {
    deliver_batch_result(orch, db, &row.request_id, verdict, text).await;
    if let Err(error) = db
        .execute(
            "UPDATE run_batch_adopted_outcomes SET state='delivered' WHERE request_id=?1 AND attempt_id=?2",
            params![row.request_id.as_str(), row.attempt_id.as_str()],
        )
        .await
    {
        log::warn!("could not mark adopted request {} delivered: {error}", row.request_id);
    }
}

/// The one door a batch's result takes to whatever still waits for it when
/// the process that started the batch is gone: a parked wait is resolved
/// (and diverted to a notice if its turn has moved on), and a detached row
/// is settled with its notice. Both are compare-and-set, so a second
/// delivery changes nothing.
pub(crate) async fn deliver_batch_result(
    orch: &Orchestrator,
    db: &LocalDb,
    request_id: &str,
    verdict: &str,
    text: &str,
) {
    match durable_suspend::pending_run_batch_wait(db, request_id).await {
        Ok(Some(record)) => durable_suspend::drive_resolution(orch, db, &record, text, false).await,
        Ok(None) => {}
        Err(error) => log::warn!("could not read the wait of run batch {request_id}: {error}"),
    }
    detached::settle_and_deliver(orch, db, request_id, verdict, text).await;
}

/// What a host batch's call is told after a restart: it ran inside Cairn.
pub(crate) fn host_restart_text(request_id: &str) -> String {
    format!(
        "Cairn restarted while this batch was running inside Cairn itself, and the batch ended with it (request {request_id}). Nothing of it survives; re-run it if the work still matters."
    )
}

/// What a batch on the colocated executor is told after a restart: that
/// executor stops with Cairn (tenet 316).
pub(crate) fn local_restart_text(request_id: &str, commits: bool) -> String {
    format!(
        "Cairn restarted while this batch was running on `local`, this machine's own executor, which stops with Cairn (request {request_id}). Its result did not survive.{}",
        if commits {
            " It carried a commit_msg; read `cairn:@/vcs/log?limit=5` before re-running it."
        } else {
            " Re-run it if the work still matters."
        }
    )
}

/// What a batch no reporting machine holds is told.
pub(crate) fn lost_text(request_id: &str, placed_on: Option<&PlacedOn>, commits: bool) -> String {
    let placed = match placed_on {
        Some(placed) => format!(" It was placed on {}.", placed.name),
        None => String::new(),
    };
    format!(
        "Cairn restarted while this batch was running, and every machine has reported since without holding or having finished it (request {request_id}).{placed} Its outcome is unknown.{}",
        if commits {
            " It carried a commit_msg; read `cairn:@/vcs/log?limit=5` before re-running it."
        } else {
            " Re-run it if the work still matters."
        }
    )
}

/// What a parked call is told when it is released because a machine has not
/// reconnected within [`RELEASE_AFTER`] of this process's start.
pub(crate) fn release_text(request_id: &str, machines: &[String], commits: bool) -> String {
    let away = match machines {
        [] => "a machine has not reported".to_string(),
        [one] => format!("{one} has not reconnected"),
        many => format!("{} have not reconnected", many.join(", ")),
    };
    format!(
        "Cairn restarted while this batch was running, and {away} in the {} minutes since. Nothing was cancelled: when it does, the batch's result settles `{}`{}, and reaches this job as a notice. Do not re-run it.",
        RELEASE_AFTER.as_secs() / 60,
        detached::handle_uri(request_id),
        if commits {
            " and its commit is published there"
        } else {
            ""
        }
    )
}

/// Settle a parked wait with a text of this process's own: resolve it, then
/// settle the detached row a resolution diverted it into, exactly as a live
/// resolver does.
async fn resolve_wait(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
    request_id: &str,
    text: &str,
) {
    durable_suspend::drive_resolution(orch, db, record, text, false).await;
    detached::settle_and_deliver(
        orch,
        db,
        request_id,
        &detached::verdict_from_text(text),
        text,
    )
    .await;
}

/// Release a parked call whose machine has not reported: its batch becomes a
/// detached row that keeps holding its branch, and the call is told so.
async fn release_wait(orch: &Orchestrator, db: &LocalDb, record: &Record, text: &str) {
    let (job_id, run_id, condition) = (
        record.job_id.clone(),
        record.run_id.clone(),
        record.condition.clone(),
    );
    let converted = db
        .write(|c| {
            let (job_id, run_id, condition) = (job_id.clone(), run_id.clone(), condition.clone());
            Box::pin(async move {
                detached::insert_converted(
                    c,
                    &job_id,
                    &run_id,
                    &condition,
                    "Cairn restarted while the batch ran, and its machine had not reconnected",
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
            })
        })
        .await;
    if let Err(error) = converted {
        // Released only once the batch has a row to settle into.
        log::warn!(
            "could not record wait {} as detached before releasing it: {error}",
            record.id
        );
        return;
    }
    durable_suspend::drive_resolution(orch, db, record, text, false).await;
}

/// Whether this process can hear the batch itself, in which case nothing
/// here judges it.
fn heard_here(orch: &Orchestrator, request_id: &str) -> bool {
    detached::placed_here(request_id)
        || detached::has_live_resolver(request_id)
        || detached::batch_is_live(orch, request_id)
}

/// One pass over every run-batch wait an earlier runner process left parked,
/// judged by what its machine says, and over every adopted result still
/// unsettled. Also names, for every machine's liveness report, the requests
/// those rows still claim.
pub(crate) async fn judge_unheard(orch: &Orchestrator) {
    let undecided = std::mem::take(&mut *orch.adoption.undecided.lock().unwrap());
    for result in undecided {
        adopt(orch, result).await;
    }
    let uptime = orch.adoption.uptime();
    let live = durable_suspend::live_resolvers(orch);
    let mut claimed = HashSet::new();
    for db in orch.db.all_dbs().await {
        if let Ok(rows) = db.query_all(OPEN_ROWS, (), adopted_row).await {
            for row in rows {
                claimed.insert(row.request_id.clone());
                if !heard_here(orch, &row.request_id) {
                    let owned = orch.clone();
                    tokio::spawn(async move { drive(&owned, &row.request_id).await });
                }
            }
        }
        if let Ok(rows) = detached::all_running(&db).await {
            claimed.extend(
                rows.into_iter()
                    .filter(|batch| !heard_here(orch, &batch.request_id))
                    .map(|batch| batch.request_id),
            );
        }
        let waits = match durable_suspend::pending_run_batch_waits(&db).await {
            Ok(waits) => waits,
            Err(error) => {
                log::warn!("could not read parked run-batch waits: {error}");
                continue;
            }
        };
        for record in waits {
            let Condition::RunBatch {
                request_id,
                commits,
                host,
                settlement,
                ..
            } = &record.condition
            else {
                continue;
            };
            if live.contains(&record.id) || heard_here(orch, request_id) {
                continue;
            }
            claimed.insert(request_id.clone());
            if *host {
                resolve_wait(
                    orch,
                    &db,
                    &record,
                    request_id,
                    &host_restart_text(request_id),
                )
                .await;
                continue;
            }
            match has_open_adoption(&db, request_id).await {
                Ok(false) => {}
                Ok(true) => continue,
                Err(error) => {
                    log::warn!("could not read whether run batch {request_id} has an adopted result: {error}");
                    continue;
                }
            }
            let placed_on = settlement
                .as_ref()
                .and_then(|settlement| settlement.placed_on.clone());
            if placed_on.as_ref().is_some_and(PlacedOn::colocated) {
                resolve_wait(
                    orch,
                    &db,
                    &record,
                    request_id,
                    &local_restart_text(request_id, *commits),
                )
                .await;
                continue;
            }
            match orch.fleet.request_report(request_id) {
                RequestReport::Held { .. } => {}
                RequestReport::Completed(completion) => {
                    let (_, output) = detached::completion_result(&completion, *commits);
                    resolve_wait(orch, &db, &record, request_id, &output).await;
                }
                RequestReport::Gone => {
                    // The adoption that commits before its ack and the
                    // machine's next account can cross this pass; asked again
                    // here so a result already adopted is never declared lost.
                    if !matches!(has_open_adoption(&db, request_id).await, Ok(false)) {
                        continue;
                    }
                    resolve_wait(
                        orch,
                        &db,
                        &record,
                        request_id,
                        &lost_text(request_id, placed_on.as_ref(), *commits),
                    )
                    .await;
                }
                RequestReport::Unknown if uptime >= RELEASE_AFTER => {
                    let away: Vec<String> = orch
                        .fleet
                        .unattached_enrolled_remotes()
                        .into_iter()
                        .map(|remote| remote.name)
                        .collect();
                    // The batch's own machine when it is known and among them.
                    let machines = match placed_on {
                        Some(placed) if away.contains(&placed.name) => vec![placed.name],
                        _ => away,
                    };
                    release_wait(
                        orch,
                        &db,
                        &record,
                        &release_text(request_id, &machines, *commits),
                    )
                    .await;
                }
                RequestReport::Unknown => {}
            }
        }
    }
    orch.fleet.set_adopting(claimed);
}
