//! The gate sweep: the authoritative evaluation of every held execution, and
//! `release`, the one door from held to running.
//!
//! ## The sweep is authoritative; events are hints
//!
//! Every predicate is level-resolvable, so the sweep can answer "may this
//! execution start?" from the database alone. It runs inside the existing
//! scheduler tick (`execution::scheduler`), not in a loop of its own. The
//! satisfying transitions — an issue resolving, a PR merging, an artifact
//! confirming, a check observation landing — call
//! [`notify_gate_evidence_changed`], which only shortens the wait: a dropped
//! notification costs one tick of latency, never a stranded execution.
//!
//! ## Release is exactly once, and it is the launch's second half
//!
//! [`release`] does, in ONE write transaction, the guarded `released_at`
//! update, job creation, and the status recompute. The window between the
//! first two is the trap: an execution with `released_at` set and no jobs is
//! what the projection reads as `complete`. A caller that loses the guard
//! returns having done nothing, so concurrent sweeps, a notifier batch racing
//! the tick, and a manual release cannot start one execution twice. After the
//! commit it emits and enqueues `AdvanceDag` — the same tail a fresh launch has
//! — for the SAME execution row. A release never creates an execution.
//!
//! ## A burst is released in order, and never rate-limited
//!
//! One merge can release every execution a plan filed after it. They are
//! released in ascending issue number, then execution seq, because admission
//! ranks by queue time and a nondeterministic release order would turn a plan's
//! implicit priority into a coin flip. There is deliberately NO stagger, cap,
//! or rate limit here: a count cap wearing a schedule would be a second brake
//! masking the admission defects it compensates for (ruled by the execution
//! thread). A thread that wants serialization chains its `after:` edges.
//!
//! ## Foreclosure holds and wakes
//!
//! A predicate that can never become true does not cancel or release its
//! execution: judgment belongs to whoever filed it. The gate latches
//! `foreclosed_at`, and the filing thread (resolved as a child issue's
//! coordinator) gets one message keyed `gate:{gate_id}` naming the two exits.
//!
//! The latch and the delivery are separate facts. A filer can be unaddressable
//! at the moment the gate forecloses (a thread session established but not yet
//! run), so delivery is pending until the foreclosure message exists: every
//! sweep retries a foreclosed gate whose message is missing. The message row,
//! whose id is derived from the gate, is the exactly-once record. The push that
//! wakes the filer is not: dismissing a pending notification deletes the push,
//! and must not re-arm the wake.

use std::sync::{Arc, Mutex, OnceLock};

use cairn_db::turso::params;
use cairn_workflow::gates::store::{
    latch_foreclosure_conn, list_gates_conn, list_held_executions_conn,
    list_waiting_gate_predicates_conn, record_outcome_conn, release_execution_conn, HeldExecution,
};
use tokio::sync::mpsc;

use super::authoring::gate_vocabulary;
use super::vocabulary::{GateContext, GateOutcome, GateVocabulary};
use crate::messages::queued::DeliveryUrgency;
use crate::models::ExecutionGate;
use crate::orchestrator::Orchestrator;
use crate::routes::TriggerClause;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// Coalescing window: a burst of satisfying writes (a merge resolves the issue,
/// its PR, and a check in quick succession) becomes one sweep.
const NOTIFY_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(250);

static NOTIFY: OnceLock<Mutex<Option<mpsc::UnboundedSender<()>>>> = OnceLock::new();

fn notifier() -> &'static Mutex<Option<mpsc::UnboundedSender<()>>> {
    NOTIFY.get_or_init(|| Mutex::new(None))
}

/// Say that evidence a gate might read has changed.
///
/// A hint, not a delivery: it carries nothing, routes nowhere, and a host with
/// no sweep installed drops it. Whatever it would have released, the next
/// scheduler tick releases anyway.
pub fn notify_gate_evidence_changed() {
    if let Ok(sender) = notifier().lock() {
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(());
        }
    }
}

/// Install the coalescing notifier: one channel, a 250ms debounce, one sweep
/// per batch. Installed beside the scheduler, so only a host that can run
/// executions can release one.
pub(crate) fn install_sweep_notifier(orch: Orchestrator) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    if let Ok(mut sender) = notifier().lock() {
        *sender = Some(tx);
    }
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            tokio::time::sleep(NOTIFY_DEBOUNCE).await;
            while rx.try_recv().is_ok() {}
            if let Err(error) = sweep(&orch).await {
                log::error!("gate sweep after an evidence change failed: {error}");
            }
        }
    });
}

/// Why a held execution was released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseCause {
    /// Every gate resolved true.
    Satisfied,
    /// Someone decided not to wait. Carries the acting principal, serialized,
    /// which is recorded on the row and is what renders the release as an
    /// override.
    Override { principal: serde_json::Value },
    /// The last gate was patched off, so nothing is left to wait on.
    GatesRemoved,
}

/// What one sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Released executions, in release order.
    pub released: Vec<String>,
    /// Gates this sweep latched as foreclosed.
    pub foreclosed: Vec<String>,
}

/// Evaluate every held execution in every open database, as of now.
pub async fn sweep(orch: &Orchestrator) -> Result<SweepReport, String> {
    sweep_at(orch, orch.services.clock.now()).await
}

pub(crate) async fn sweep_at(orch: &Orchestrator, now: i64) -> Result<SweepReport, String> {
    sweep_with(orch, &gate_vocabulary(), now).await
}

/// [`sweep_at`] through an explicit vocabulary, so a test can pin one fact's
/// coordinate while every other part of the sweep runs for real.
pub(crate) async fn sweep_with(
    orch: &Orchestrator,
    vocabulary: &GateVocabulary,
    now: i64,
) -> Result<SweepReport, String> {
    let mut report = SweepReport::default();
    for db in orch.db.all_dbs().await {
        let held = db
            .read(|conn| Box::pin(async move { list_held_executions_conn(conn).await }))
            .await
            .map_err(|error| format!("failed to list held executions: {error}"))?;
        for execution in held {
            // One execution whose evidence cannot be read must not hold up the
            // rest of the burst behind it.
            match evaluate_held(orch, &db, vocabulary, &execution, now).await {
                Ok(evaluation) => {
                    report.foreclosed.extend(evaluation.foreclosed);
                    if evaluation.released {
                        report.released.push(execution.execution_id);
                    }
                }
                Err(error) => log::error!(
                    "gate sweep could not evaluate held execution {}: {error}",
                    execution.execution_id
                ),
            }
        }
    }
    Ok(report)
}

/// What evaluating one held execution found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Evaluation {
    /// This evaluation released the execution.
    pub released: bool,
    /// Gates this evaluation latched as foreclosed.
    pub foreclosed: Vec<String>,
    /// The census phrase of each gate still unmet, with its evidence state.
    pub waiting: Vec<String>,
}

/// Evaluate one execution now, releasing it if every gate is already true.
///
/// Creation-time evaluation: a gate filed after something that has already
/// happened releases at once rather than waiting for an event long past. Also
/// what a gate patch calls. An execution that is not held evaluates to nothing.
pub async fn evaluate_execution(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Evaluation, String> {
    evaluate_execution_at(orch, execution_id, orch.services.clock.now()).await
}

pub(crate) async fn evaluate_execution_at(
    orch: &Orchestrator,
    execution_id: &str,
    now: i64,
) -> Result<Evaluation, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    let wanted = execution_id.to_string();
    let held = db
        .read(|conn| Box::pin(async move { list_held_executions_conn(conn).await }))
        .await
        .map_err(|error| format!("failed to list held executions: {error}"))?
        .into_iter()
        .find(|held| held.execution_id == wanted);
    match held {
        Some(held) => evaluate_held(orch, &db, &gate_vocabulary(), &held, now).await,
        None => Ok(Evaluation::default()),
    }
}

async fn evaluate_held(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    vocabulary: &GateVocabulary,
    execution: &HeldExecution,
    now: i64,
) -> Result<Evaluation, String> {
    let execution_id = execution.execution_id.clone();
    let gates = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move { list_gates_conn(conn, &execution_id).await })
        })
        .await
        .map_err(|error| format!("failed to list gates: {error}"))?;
    evaluate_gates(orch, db, vocabulary, execution, &gates, now).await
}

/// Resolve `gates` — the list as this evaluation READ it, which a concurrent
/// gate patch may already have replaced — and release when every one of them
/// is satisfied. A stale list can only propose a release; [`release_conn`]
/// decides it against the gates as stored.
async fn evaluate_gates(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    vocabulary: &GateVocabulary,
    execution: &HeldExecution,
    gates: &[ExecutionGate],
    now: i64,
) -> Result<Evaluation, String> {
    let ctx = GateContext::new(orch, now);
    let mut evaluation = Evaluation::default();
    let mut all_satisfied = true;
    for gate in gates {
        if gate.is_satisfied() {
            continue;
        }
        let clause: TriggerClause = match serde_json::from_str(&gate.predicate_json) {
            Ok(clause) => clause,
            Err(error) => {
                all_satisfied = false;
                log::error!("gate {} has an unreadable predicate: {error}", gate.id);
                continue;
            }
        };
        let outcome = match vocabulary.resolve(&ctx, &clause).await {
            Ok(outcome) => outcome,
            Err(error) => {
                all_satisfied = false;
                log::warn!("gate {} could not be resolved: {error}", gate.id);
                continue;
            }
        };
        record_outcome(db, gate, outcome, now).await?;
        if outcome == GateOutcome::Satisfied {
            continue;
        }
        all_satisfied = false;
        evaluation.waiting.push(format!(
            "{} ({})",
            vocabulary.render(&clause),
            outcome.evidence_phrase()
        ));
        let mut latched_now = false;
        let reason = match &gate.foreclosed_reason {
            Some(reason) if gate.is_foreclosed() => Some(reason.clone()),
            _ if gate.is_foreclosed() => Some("it can never be satisfied".to_string()),
            _ => match vocabulary.foreclosed(&ctx, &clause).await {
                Ok(Some(reason)) => {
                    latched_now = latch_foreclosure(db, gate, &reason, now).await?;
                    if latched_now {
                        evaluation.foreclosed.push(gate.id.clone());
                    }
                    Some(reason)
                }
                Ok(None) => None,
                Err(error) => {
                    log::warn!("gate {} foreclosure could not be read: {error}", gate.id);
                    None
                }
            },
        };
        if let Some(reason) = reason {
            let rendered = vocabulary.render(&clause);
            deliver_foreclosure(
                orch,
                db,
                execution,
                gate,
                gates,
                &rendered,
                &reason,
                latched_now,
            )
            .await?;
        }
    }

    if all_satisfied {
        evaluation.released = release_in(
            orch,
            db,
            &execution.execution_id,
            ReleaseCause::Satisfied,
            now,
        )
        .await?;
    }
    Ok(evaluation)
}

/// Record a gate's newest resolution, latching satisfaction. Skips the write
/// when nothing would change, so a quiet gate costs a read per tick, not a
/// write.
async fn record_outcome(
    db: &LocalDb,
    gate: &ExecutionGate,
    outcome: GateOutcome,
    now: i64,
) -> Result<(), String> {
    let satisfied_at = (outcome == GateOutcome::Satisfied).then_some(now);
    if satisfied_at.is_none() && gate.last_outcome.as_deref() == Some(outcome.as_str()) {
        return Ok(());
    }
    let gate_id = gate.id.clone();
    db.write(|conn| {
        let gate_id = gate_id.clone();
        Box::pin(async move {
            record_outcome_conn(conn, &gate_id, outcome.as_str(), satisfied_at).await
        })
    })
    .await
    .map_err(|error| format!("failed to record gate outcome: {error}"))
}

/// Release a held execution: the second half of a launch, applied to the row
/// that already exists. Returns `false` when it was not held — already
/// released by someone else, or never gated.
pub async fn release(
    orch: &Orchestrator,
    execution_id: &str,
    cause: ReleaseCause,
) -> Result<bool, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    release_in(orch, &db, execution_id, cause, orch.services.clock.now()).await
}

async fn release_in(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
    cause: ReleaseCause,
    now: i64,
) -> Result<bool, String> {
    let execution = execution_id.to_string();
    let released = db
        .write(|conn| {
            let execution_id = execution.clone();
            let cause = cause.clone();
            Box::pin(async move { release_conn(conn, &execution_id, now, &cause).await })
        })
        .await
        .map_err(|error| format!("failed to release execution {execution_id}: {error}"))?;
    let Some(released) = released else {
        return Ok(false);
    };
    log::info!("released held execution {execution_id} ({cause:?})");
    announce_release(orch, execution_id, released).await;
    Ok(true)
}

/// What a committed release did, for the post-commit tail.
#[derive(Debug, Clone)]
pub(crate) struct Released {
    issue_id: Option<String>,
    /// Executions a `pending` release took the attempt from, each owing the
    /// phase-2 agent stop a normal launch performs after its fence commits.
    supersessions: Vec<crate::execution::canonical::Supersession>,
}

/// The post-commit tail of a release — the same tail a fresh launch has: stop
/// the agents its fence froze, emit, then enqueue `AdvanceDag` for the
/// released row.
pub(crate) async fn announce_release(orch: &Orchestrator, execution_id: &str, released: Released) {
    let Released {
        issue_id,
        supersessions,
    } = released;
    crate::execution::recipe::stop_superseded_agents(orch, &supersessions);
    orch.notifier.emit_change("executions");
    orch.notifier.emit_change("jobs");
    if let Some(tx) = orch.effect_tx.as_ref() {
        let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
            execution_id: execution_id.to_string(),
            outbox_entry_id: None,
        });
    }
    if let Some(issue_id) = issue_id {
        orch.invalidate_sidebar_active_issues();
        orch.wake_for_issue(&issue_id).await;
    }
}

/// The transactional half of a release, inside the caller's write. `None`
/// means this caller may not release it — already released, superseded, its
/// issue resolved, or its CURRENT gates do not license `cause` — so it owns
/// nothing and mints nothing. `Some` means this caller released it and must
/// run [`announce_release`] after the commit.
///
/// A `pending` execution — a held next attempt — takes the attempt HERE: the
/// same supersede a normal launch performs at insert, run against whatever
/// owns the attempt now, then promotion into the canonical slot, both before
/// its jobs exist. If the attempt it was filed to follow has finished
/// meanwhile, that is a supersession of a finished execution and fences
/// nothing live.
///
/// Winning the guarded update is ownership, not authorization. The sweep
/// decides "every gate is satisfied" from a gate list it read before resolving
/// them, and a gate patch can land in between (adding an unmet gate, or
/// swapping a satisfied one for an unmet one). So each cause re-checks its
/// authority against the gates as stored NOW, in this transaction:
///
/// - `Satisfied` needs at least one gate and no unsatisfied one;
/// - `GatesRemoved` needs no gates at all;
/// - `Override` needs nothing: deciding not to wait is the point.
///
/// The guarded update writes the execution row, and a gate patch writes it
/// too, so a patch and a release that overlap conflict and one retries
/// against the other's committed result.
pub(crate) async fn release_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    now: i64,
    cause: &ReleaseCause,
) -> DbResult<Option<Released>> {
    let (gates, unsatisfied) =
        cairn_workflow::gates::store::gate_counts_conn(conn, execution_id).await?;
    let authorized = match cause {
        ReleaseCause::Satisfied => gates > 0 && unsatisfied == 0,
        ReleaseCause::GatesRemoved => gates == 0,
        ReleaseCause::Override { .. } => true,
    };
    if !authorized {
        return Ok(None);
    }
    let override_json = match cause {
        ReleaseCause::Override { principal } => {
            Some(serde_json::json!({ "principal": principal, "at": now }).to_string())
        }
        ReleaseCause::Satisfied | ReleaseCause::GatesRemoved => None,
    };
    if release_execution_conn(conn, execution_id, now, override_json.as_deref()).await? == 0 {
        return Ok(None);
    }
    let mut rows = conn
        .query(
            "SELECT issue_id, lifecycle FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    let (issue_id, lifecycle) = match rows.next().await? {
        Some(row) => (row.opt_text(0)?, row.text(1)?),
        None => return Ok(None),
    };
    drop(rows);

    let mut supersessions = Vec::new();
    if let (Some(issue_id), "pending") = (issue_id.as_deref(), lifecycle.as_str()) {
        let actor = match cause {
            ReleaseCause::Override { principal } => {
                crate::execution::canonical::SupersessionActor {
                    kind: "gate-override",
                    reference: Some(principal.to_string()),
                }
            }
            ReleaseCause::Satisfied | ReleaseCause::GatesRemoved => {
                crate::execution::canonical::SupersessionActor {
                    kind: "gate-release",
                    reference: Some(execution_id.to_string()),
                }
            }
        };
        // The fence is published BEFORE this row takes the slot, so there is
        // never an instant at which two rows read canonical.
        supersessions = crate::execution::canonical::supersede_canonical_conn(
            conn,
            issue_id,
            execution_id,
            &actor,
            now,
        )
        .await?;
        conn.execute(
            "UPDATE executions SET lifecycle = 'canonical' WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    }

    crate::execution::advancement::create_jobs_for_execution_conn(conn, execution_id).await?;
    crate::transitions::outcome::recompute_execution_status_conn(conn, execution_id).await?;
    if let Some(issue_id) = issue_id.as_deref() {
        crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id).await?;
    }
    Ok(Some(Released {
        issue_id,
        supersessions,
    }))
}

/// Latch a gate's foreclosure. Returns whether this call latched it; the guarded
/// latch admits exactly one caller.
async fn latch_foreclosure(
    db: &LocalDb,
    gate: &ExecutionGate,
    reason: &str,
    now: i64,
) -> Result<bool, String> {
    let gate_id = gate.id.clone();
    let reason = reason.to_string();
    db.write(|conn| {
        let gate_id = gate_id.clone();
        let reason = reason.clone();
        Box::pin(async move { latch_foreclosure_conn(conn, &gate_id, now, &reason).await })
    })
    .await
    .map_err(|error| format!("failed to latch gate foreclosure: {error}"))
}

/// The id of a gate's foreclosure message: a UUID derived from the gate, so the
/// message row itself records that the wake was delivered.
fn foreclosure_message_id(gate_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("gate-foreclosure:{gate_id}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes)
        .into_uuid()
        .to_string()
}

/// Wake the thread that filed a foreclosed gate's execution, unless that wake
/// was already delivered.
///
/// The foreclosure message is the delivery record: its absence means the wake
/// is still owed, so a filer that could not be addressed when the gate
/// foreclosed is woken on the first sweep after it can be, and never twice,
/// even if the pending notification is dismissed. The check and the insert
/// share one transaction.
#[allow(clippy::too_many_arguments)]
async fn deliver_foreclosure(
    orch: &Orchestrator,
    db: &LocalDb,
    execution: &HeldExecution,
    gate: &ExecutionGate,
    gates: &[ExecutionGate],
    rendered: &str,
    reason: &str,
    latched_now: bool,
) -> Result<(), String> {
    let execution = execution.clone();
    let gate_id = gate.id.clone();
    let remaining: Vec<serde_json::Value> = gates
        .iter()
        .filter(|other| other.id != gate.id)
        .filter_map(|other| serde_json::from_str(&other.predicate_json).ok())
        .collect();
    let rendered = rendered.to_string();
    let reason = reason.to_string();
    let delivered = db
        .write(|conn| {
            let execution = execution.clone();
            let gate_id = gate_id.clone();
            let remaining = remaining.clone();
            let rendered = rendered.clone();
            let reason = reason.clone();
            Box::pin(async move {
                let push_key = format!("gate:{gate_id}");
                let message_id = foreclosure_message_id(&gate_id);
                if crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT id FROM messages WHERE id = ?1",
                    params![message_id.as_str()],
                )
                .await?
                .is_some()
                {
                    return Ok(None);
                }
                let Some(issue_id) = execution.issue_id.as_deref() else {
                    return Ok(None);
                };
                let Some(recipient) =
                    crate::orchestrator::wakes::coordinating_job_for_child(conn, issue_id)
                        .await?
                else {
                    if latched_now {
                        log::warn!(
                            "gate {gate_id} foreclosed, but issue {issue_id} has no filing thread to wake yet; the wake stays owed"
                        );
                    }
                    return Ok(None);
                };
                let Some(recipient_run) = crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, id DESC LIMIT 1",
                    params![recipient.as_str()],
                )
                .await?
                else {
                    if latched_now {
                        log::warn!(
                            "gate {gate_id} foreclosed, but its recipient has no run to address yet; the wake stays owed"
                        );
                    }
                    return Ok(None);
                };
                let content = foreclosure_message(
                    conn, &execution, issue_id, &rendered, &reason, &remaining,
                )
                .await?;
                cairn_attention::direct::insert_system_direct_push_with_message_id_conn(
                    conn,
                    &recipient,
                    &recipient_run,
                    &content,
                    DeliveryUrgency::Steer,
                    Some(&push_key),
                    &message_id,
                )
                .await?;
                Ok(Some(recipient))
            })
        })
        .await
        .map_err(|error| format!("failed to deliver gate foreclosure: {error}"))?;

    if let Some(recipient) = delivered {
        if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            &recipient,
            DeliveryUrgency::Steer,
        ) {
            log::warn!("gate foreclosure for {recipient} is queued but the nudge failed: {error}");
        }
    }
    Ok(())
}

/// The message a foreclosure delivers: which execution, on which issue, which
/// predicate, what foreclosed it, and the two exits as writes the reader can
/// send as-is.
async fn foreclosure_message(
    conn: &cairn_db::turso::Connection,
    execution: &HeldExecution,
    issue_id: &str,
    rendered: &str,
    reason: &str,
    remaining: &[serde_json::Value],
) -> DbResult<String> {
    let issue_uri = crate::issues::relations::issue_uri_for_id(conn, issue_id).await?;
    let seq = execution
        .seq
        .map(|seq| seq.to_string())
        .unwrap_or_else(|| "?".to_string());
    let execution_uri = format!("{issue_uri}/executions/{seq}");
    let remaining = serde_json::to_string(remaining)
        .map_err(|error| DbError::internal(format!("unserializable gates: {error}")))?;
    Ok(format!(
        "Held execution {seq} on {issue_uri} can no longer start on its own: its gate \
         `after {rendered}` is foreclosed \u{2014} {reason}.\n\n\
         It stays held (not released, not cancelled) until you decide. Either:\n\
         - patch the gate off: write({{changes:[{{target:\"{execution_uri}\",mode:\"patch\",payload:{{after:{remaining}}}}}]}})\n\
         - or release it early: write({{changes:[{{target:\"{execution_uri}\",mode:\"patch\",payload:{{release:true}}}}]}})"
    ))
}

/// The soonest instant after `now` at which an unsatisfied gate could become
/// true on its own, in unix seconds. Only `clock` gates answer.
///
/// The scheduler folds this into its sleep bound so a `notBefore` releases on
/// time rather than up to a tick late. A due instant already in the past is
/// excluded: the sweep at the top of the tick handles it, and folding it in
/// would let one gate that keeps failing to resolve spin the loop.
pub async fn next_gate_due_after(orch: &Orchestrator, now: i64) -> Option<i64> {
    let vocabulary = gate_vocabulary();
    let mut soonest: Option<i64> = None;
    for db in orch.db.all_dbs().await {
        let predicates = db
            .read(|conn| {
                Box::pin(async move { list_waiting_gate_predicates_conn(conn, None).await })
            })
            .await
            .unwrap_or_else(|error| {
                log::error!("failed to read waiting gates for the scheduler bound: {error}");
                Vec::new()
            });
        for (_, predicate_json) in predicates {
            let Ok(clause) = serde_json::from_str::<TriggerClause>(&predicate_json) else {
                continue;
            };
            if let Some(due) = vocabulary.next_due_at(&clause).filter(|due| *due > now) {
                soonest = Some(soonest.map_or(due, |current| current.min(due)));
            }
        }
    }
    soonest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::effects::types::WorkflowEffect;
    use crate::execution::gates::vocabulary::{
        test_support::green_observation, CheckVerdictCoordinate, CheckVerdictFact, GateFact,
    };
    use crate::execution::recipe::LaunchIntent;
    use crate::mcp::handlers::executions::{
        patch_execution_gates, start_execution_from_collection, GatePatch,
    };
    use crate::orchestrator::OrchestratorBuilder;
    use crate::routes::FieldKind;
    use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
    use crate::storage::{RowExt, SearchIndex};
    use async_trait::async_trait;
    use serde_json::{json, Value};

    const RECIPE: &str = "memory-triage";

    struct Env {
        _temp: tempfile::TempDir,
        orch: Orchestrator,
        db: Arc<LocalDb>,
        effects: mpsc::UnboundedReceiver<WorkflowEffect>,
    }

    /// A project `prj` with issues 1..=5 (all live), a parent issue 9 driven by
    /// a live coordinator node, and a real recipe on disk, so every filing below
    /// goes through the same launch path an agent's write does.
    async fn env(name: &str) -> Env {
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        env_over(temp, db).await
    }

    /// [`env`] over a database the caller has already migrated.
    async fn env_over(temp: tempfile::TempDir, db: Arc<LocalDb>) -> Env {
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        std::fs::create_dir_all(project_dir.join(".cairn/agents")).unwrap();
        std::fs::write(
            config_dir.join("recipes/memory-triage.yaml"),
            include_str!("../../../../../packs/core/recipes/memory-triage.yaml"),
        )
        .unwrap();
        std::fs::write(
            config_dir.join("agents/integrator.md"),
            include_str!("../../../../../packs/core/agents/integrator.md"),
        )
        .unwrap();

        let mut seed = format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'default', 'Project', 'prj', '{}', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('ip', 'p', 9, 'Parent', 'active', 'active', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('ep', 'recipe', 'ip', 'p', 'running', 1, 1);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
               VALUES('coord', 'ep', 'p', 'ip', 'Coordinator', 'running', 'sess', 'coordinator', 1, 1);
             INSERT INTO runs(id, project_id, job_id, issue_id, status, created_at, updated_at)
               VALUES('run-coord', 'p', 'coord', 'ip', 'live', 1, 1);",
            project_dir.display()
        );
        for number in 1..=5 {
            seed.push_str(&format!(
                "INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_issue_id, parent_job_id, created_at, updated_at)
                   VALUES('i{number}', 'p', {number}, 'Issue {number}', 'active', 'active', 'none', 'ip', 'coord', 1, 1);"
            ));
        }
        db.execute_script(&seed).await.unwrap();

        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        // Creation-time evaluation reads the services clock, so it is pinned to
        // the same instant every explicit sweep below is taken at.
        let mut clock = crate::services::testing::MockClock::new();
        clock.expect_now().returning(|| NOW);
        clock.expect_now_u64().returning(|| NOW as u64);
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_clock(clock)
                .with_process(RecordingProcessSpawner::new().clone())
                .build(),
        );
        let (tx, effects) = mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(db.clone(), search)),
            services,
            config_dir,
        )
        .effect_tx(Some(tx))
        .build();
        Env {
            _temp: temp,
            orch,
            db,
            effects,
        }
    }

    fn merged_after(number: i32) -> Value {
        json!([{"fact": "issue_resolution", "issue": format!("cairn://p/prj/{number}"), "resolution": "merged"}])
    }

    async fn file(env: &Env, number: i32, after: Value) -> Result<String, String> {
        let clauses = super::super::authoring::parse_after(Some(&after), "after")?;
        start_execution_from_collection(
            &env.orch,
            "prj",
            number,
            Some(RECIPE),
            None,
            None,
            None,
            None,
            LaunchIntent::NewAttempt,
            clauses,
        )
        .await
    }

    async fn resolve_issue(env: &Env, number: i32, status: &str) {
        env.db
            .execute(
                &format!("UPDATE issues SET status = ?1, {status}_at = 5 WHERE number = ?2"),
                (status.to_string(), number),
            )
            .await
            .unwrap();
    }

    /// The newest execution on issue `number`: (id, status, released_at, jobs).
    async fn execution(env: &Env, number: i32) -> (String, String, Option<i64>, i64) {
        env.db
            .read(move |conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT e.id, e.status, e.released_at,
                                    (SELECT COUNT(*) FROM jobs j WHERE j.execution_id = e.id)
                               FROM executions e JOIN issues i ON i.id = e.issue_id
                              WHERE i.number = ?1 ORDER BY e.seq DESC LIMIT 1",
                            params![number],
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?, row.opt_i64(2)?, row.i64(3)?))
                })
            })
            .await
            .unwrap()
    }

    async fn count(env: &Env, sql: &str) -> i64 {
        env.db.query_opt_i64(sql, ()).await.unwrap().unwrap()
    }

    fn advanced(env: &mut Env) -> Vec<String> {
        let mut advanced = Vec::new();
        while let Ok(effect) = env.effects.try_recv() {
            if let WorkflowEffect::AdvanceDag { execution_id, .. } = effect {
                advanced.push(execution_id);
            }
        }
        advanced
    }

    /// The instant the test world stands at: the services clock and every
    /// explicit sweep agree on it.
    const NOW: i64 = 1_900_000_000;

    fn now() -> i64 {
        NOW
    }

    /// A `depends_on` edge that 0257 carries onto an execution that already ran
    /// must hold nothing. A gate on an execution that was never released would
    /// project it `held`, and the sweep would then release it by minting its
    /// jobs a second time; the migration marks it released first. Asserted
    /// end to end: the derived status is unchanged, the gate renders satisfied,
    /// and a sweep mints nothing.
    #[tokio::test]
    async fn a_dependency_edge_migrated_onto_a_finished_execution_holds_and_mints_nothing() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open(temp.path().join("retire-dependencies.db"))
                .await
                .unwrap(),
        );
        let before = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version() != "0257")
            .copied()
            .collect::<Vec<_>>();
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('q', 'default', 'Deps', 'dep', '/tmp/dep', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('dep-target', 'q', 1, 'Target', 'merged', 'complete', 'none', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('dep-done', 'q', 2, 'Finished', 'complete', 'complete', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('dep-exec', 'recipe', 'dep-done', 'q', 'complete', 100, 1);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, created_at, updated_at)
               VALUES('dep-job-1', 'dep-exec', 'q', 'dep-done', 'builder', 'complete', 1, 1);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, created_at, updated_at)
               VALUES('dep-job-2', 'dep-exec', 'q', 'dep-done', 'review', 'complete', 1, 1);
             INSERT INTO issue_dependencies(issue_id, depends_on_uri, created_at)
               VALUES('dep-done', 'cairn://p/DEP/1', 1);",
        )
        .await
        .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let mut env = env_over(temp, db).await;

        let census = env
            .db
            .write(|conn| {
                Box::pin(async move {
                    crate::transitions::outcome::recompute_execution_status_conn(conn, "dep-exec")
                        .await?;
                    crate::execution::gates::census::census_for_executions_conn(
                        conn,
                        &["dep-exec".to_string()],
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .remove("dep-exec")
            .expect("the migrated gate gives the execution a census");
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE id = 'dep-exec' AND status = 'complete'"
            )
            .await,
            1,
            "the derived status is unchanged, not held"
        );
        assert!(!census.held);
        assert_eq!(census.summary, "released: every gate met");
        assert_eq!(census.gates.len(), 1);
        assert_eq!(
            census.gates[0].state,
            crate::models::GateEvidenceState::Satisfied
        );
        assert!(
            census.gates[0].predicate.contains("cairn://p/dep/1"),
            "{:?}",
            census.gates[0]
        );

        let report = sweep_at(&env.orch, NOW).await.unwrap();
        assert!(
            !report.released.contains(&"dep-exec".to_string()),
            "{report:?}"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM jobs WHERE execution_id = 'dep-exec'"
            )
            .await,
            2,
            "a sweep mints no second set of jobs"
        );
        assert!(!advanced(&mut env).contains(&"dep-exec".to_string()));
    }

    /// Plan verification 2: held on creation, released in place on merge.
    #[tokio::test]
    async fn a_held_execution_mints_nothing_until_its_gate_and_then_advances_in_place() {
        let mut env = env("gate-sweep-release.db").await;
        let summary = file(&env, 2, merged_after(1)).await.unwrap();
        assert!(summary.contains("HELD"), "got: {summary}");
        assert!(
            summary.contains("after issue cairn://p/prj/1 merged (no verdict yet)"),
            "the filing names its gate and the evidence state: {summary}"
        );

        let (held_id, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!((status.as_str(), released_at, jobs), ("held", None, 0));
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM runtime_admission_requests").await,
            0
        );
        assert!(
            advanced(&mut env).is_empty(),
            "a held filing never advances"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM issues WHERE id = 'i2' AND status = 'backlog'"
            )
            .await,
            1,
            "an issue whose only execution is held has not started"
        );

        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert!(report.released.is_empty(), "nothing happened yet");

        resolve_issue(&env, 1, "merged").await;
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![held_id.clone()]);

        let (released_id, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!(released_id, held_id, "the SAME row advanced");
        assert_eq!(status, "running");
        assert!(released_at.is_some());
        assert!(jobs > 0, "release minted the jobs");
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE issue_id = 'i2'"
            )
            .await,
            1,
            "release never creates a second execution"
        );
        assert_eq!(advanced(&mut env), vec![held_id.clone()]);
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE id IN (SELECT id FROM executions WHERE issue_id = 'i2') AND release_override_json IS NULL"
            )
            .await,
            1,
            "a satisfied release is not an override"
        );

        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert!(report.released.is_empty());
        assert_eq!(execution(&env, 2).await.3, jobs, "no second set of jobs");
        assert!(advanced(&mut env).is_empty());
    }

    /// Plan verification 3: a held execution on an issue that already has a
    /// completed one leaves the issue's status alone.
    #[tokio::test]
    async fn a_held_execution_beside_a_completed_one_does_not_move_the_issue() {
        let env = env("gate-sweep-beside.db").await;
        env.db
            .execute_script(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, completed_at, seq)
                   VALUES('done', 'recipe', 'i5', 'p', 'complete', 1, 2, 1);
                 UPDATE issues SET status = 'complete', progress = 'complete', completed_at = 2 WHERE id = 'i5';",
            )
            .await
            .unwrap();

        file(&env, 5, merged_after(1)).await.unwrap();

        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM issues WHERE id = 'i5' AND status = 'complete'"
            )
            .await,
            1
        );
        let statuses = env
            .db
            .query_all(
                "SELECT seq, status FROM executions WHERE issue_id = 'i5' ORDER BY seq",
                (),
                |row| Ok((row.i64(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            statuses,
            vec![(1, "complete".to_string()), (2, "held".to_string())],
            "both executions stand, each with its own status"
        );
    }

    /// Plan verification 4: foreclosure holds and wakes the filing thread once.
    #[tokio::test]
    async fn foreclosure_holds_the_execution_and_wakes_its_filer_exactly_once() {
        let env = env("gate-sweep-foreclose.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        resolve_issue(&env, 1, "closed").await;

        let first = sweep_at(&env.orch, now()).await.unwrap();
        let second = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(first.foreclosed.len(), 1);
        assert!(second.foreclosed.is_empty(), "the latch admits one sweep");
        assert!(first.released.is_empty() && second.released.is_empty());

        let (_, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!(
            (status.as_str(), released_at, jobs),
            ("held", None, 0),
            "foreclosure holds: not released, not cancelled"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_gates WHERE foreclosed_at IS NOT NULL AND foreclosed_reason LIKE '%closed%'"
            )
            .await,
            1
        );
        let messages = env
            .db
            .query_all(
                "SELECT content FROM messages WHERE recipient_run_id = 'run-coord'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "exactly one message: {messages:?}");
        let message = &messages[0];
        for expected in [
            "Held execution 1 on cairn://p/prj/2",
            "after issue cairn://p/prj/1 merged",
            "is closed",
            "cairn://p/prj/2/executions/1",
            "payload:{after:[]}",
            "payload:{release:true}",
        ] {
            assert!(
                message.contains(expected),
                "missing {expected:?} in {message}"
            );
        }
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM attention_pushes WHERE recipient = 'coord' AND key LIKE 'gate:%'"
            )
            .await,
            1
        );
    }

    /// Dismissing the pending foreclosure notification through the real dismiss
    /// path does not re-arm the wake: the message row is the delivery record.
    #[tokio::test]
    async fn dismissing_a_foreclosure_notification_does_not_resend_it() {
        use crate::messages::pending::{dismiss_pending_delivery, PendingDeliverySource};
        let env = env("gate-sweep-foreclose-dismiss.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        resolve_issue(&env, 1, "closed").await;
        sweep_at(&env.orch, now()).await.unwrap();

        let pushes = "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE 'gate:%'";
        let messages = "SELECT COUNT(*) FROM messages WHERE recipient_run_id = 'run-coord'";
        assert_eq!(
            (count(&env, pushes).await, count(&env, messages).await),
            (1, 1)
        );
        let push_id = env
            .db
            .query_opt_text(
                "SELECT id FROM attention_pushes WHERE key LIKE 'gate:%'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        dismiss_pending_delivery(&env.db, "coord", &push_id, PendingDeliverySource::Push)
            .await
            .unwrap();
        assert_eq!(
            count(&env, pushes).await,
            0,
            "dismissal deletes the pending push"
        );

        sweep_at(&env.orch, now()).await.unwrap();
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(count(&env, pushes).await, 0, "no replacement push");
        assert_eq!(count(&env, messages).await, 1, "no duplicate message");
    }

    /// A self-gate whose evidence is already in is no wait at all: it files with
    /// no live producer and releases at once. The same gate with no evidence and
    /// no producer is still refused.
    #[tokio::test]
    async fn an_already_satisfied_self_gate_files_and_releases() {
        let env = env("gate-sweep-self-satisfied.db").await;
        let own_plan = |n: i32| json!([{"fact": "artifact_confirmed", "issue": format!("cairn://p/prj/{n}"), "artifact": "plan"}]);
        // A finished planner on 3 left a confirmed plan; the one on 4 left none.
        env.db
            .execute_script(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES('plan3', 'recipe', 'i3', 'p', 'complete', 1, 1),
                         ('plan4', 'recipe', 'i4', 'p', 'complete', 1, 1);
                 INSERT INTO jobs(id, execution_id, project_id, issue_id, status, created_at, updated_at)
                   VALUES('planner3', 'plan3', 'p', 'i3', 'complete', 1, 1),
                         ('planner4', 'plan4', 'p', 'i4', 'complete', 1, 1);
                 INSERT INTO artifacts(id, job_id, artifact_type, data, confirmed, created_at, updated_at)
                   VALUES('plan-a3', 'planner3', 'plan', '{}', 1, 1, 1);",
            )
            .await
            .unwrap();

        file(&env, 3, own_plan(3)).await.unwrap();
        let (_, _, released_at, jobs) = execution(&env, 3).await;
        assert!(
            released_at.is_some() && jobs > 0,
            "released at creation: {released_at:?}, {jobs} jobs"
        );

        let error = file(&env, 4, own_plan(4)).await.unwrap_err();
        assert!(
            error.contains("no other live execution on prj/4"),
            "got: {error}"
        );
    }

    fn canonical(after: Value) -> Vec<String> {
        super::super::authoring::canonical_predicates(
            &super::super::authoring::parse_after(Some(&after), "after").unwrap(),
            "after",
        )
        .unwrap()
    }

    /// The observation that waived a self-wait is latched with the gate on
    /// filing: the plan the preflight saw is gone by the time the filing
    /// commits, and nothing on the issue is live, yet the accepted gate keeps
    /// its satisfaction and releases instead of becoming a silent self-cycle.
    #[tokio::test]
    async fn the_satisfaction_that_waived_a_self_wait_is_latched_on_filing() {
        let env = env("gate-sweep-self-observed-filing.db").await;
        file(&env, 1, json!([])).await.unwrap();
        let snapshot = env
            .db
            .query_opt_text("SELECT snapshot FROM executions WHERE issue_id = 'i1'", ())
            .await
            .unwrap()
            .unwrap();
        let own_plan = canonical(
            json!([{"fact": "artifact_confirmed", "issue": "cairn://p/prj/3", "artifact": "plan"}]),
        );
        let observed = own_plan.iter().cloned().collect();

        crate::execution::recipe::file_held_with_observed_satisfaction(
            &env.orch,
            env.db.clone(),
            "i3",
            RECIPE,
            snapshot,
            own_plan,
            observed,
        )
        .unwrap();
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM execution_gates WHERE last_outcome = 'satisfied' AND satisfied_at IS NOT NULL").await,
            1,
            "the waiving observation is latched"
        );
        sweep_at(&env.orch, now()).await.unwrap();
        let (_, _, released_at, jobs) = execution(&env, 3).await;
        assert!(
            released_at.is_some() && jobs > 0,
            "released: {released_at:?}, {jobs} jobs"
        );
    }

    /// The same on patch: a newly added self-gate whose satisfaction was seen
    /// before the patch transaction keeps it, though the evidence is gone.
    #[tokio::test]
    async fn the_satisfaction_that_waived_a_self_wait_is_latched_on_patch() {
        let env = env("gate-sweep-self-observed-patch.db").await;
        file(&env, 5, merged_after(1)).await.unwrap();
        let (execution_id, ..) = execution(&env, 5).await;
        let own_plan = canonical(
            json!([{"fact": "artifact_confirmed", "issue": "cairn://p/prj/5", "artifact": "plan"}]),
        );
        let gates = canonical(json!([
            {"fact": "artifact_confirmed", "issue": "cairn://p/prj/5", "artifact": "plan"},
            {"fact": "issue_resolution", "issue": "cairn://p/prj/1", "resolution": "merged"}
        ]));

        super::super::authoring::replace_gates_observed(
            &env.orch,
            &execution_id,
            &gates,
            "execution 1 on cairn://p/prj/5",
            own_plan.iter().cloned().collect(),
        )
        .await
        .unwrap();
        sweep_at(&env.orch, now()).await.unwrap();
        let latched = format!(
            "SELECT COUNT(*) FROM execution_gates WHERE predicate_json = '{}' AND satisfied_at IS NOT NULL",
            own_plan[0]
        );
        assert_eq!(
            count(&env, &latched).await,
            1,
            "the waiving observation is latched"
        );
        let (_, status, released_at, _) = execution(&env, 5).await;
        assert_eq!(
            (status.as_str(), released_at),
            ("held", None),
            "still held on prj/1"
        );
    }

    /// A held execution whose own-artifact gate already latched may have its
    /// gates patched without a live producer: the latched gate waits on nothing.
    #[tokio::test]
    async fn patching_beside_a_latched_self_gate_needs_no_producer() {
        let env = env("gate-sweep-self-latched.db").await;
        let gates = |other: i32| {
            json!([
                {"fact": "artifact_confirmed", "issue": "cairn://p/prj/5", "artifact": "plan"},
                {"fact": "issue_resolution", "issue": format!("cairn://p/prj/{other}"), "resolution": "merged"}
            ])
        };
        file(&env, 5, json!([])).await.unwrap();
        file(&env, 5, gates(1)).await.unwrap();
        // The producer finishes with a confirmed plan, and a sweep latches it.
        env.db
            .execute_script(
                "UPDATE executions SET status = 'complete' WHERE issue_id = 'i5' AND seq = 1;
                 INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                   VALUES('planner5', 'p', 'i5', 'complete', 1, 1);
                 INSERT INTO artifacts(id, job_id, artifact_type, data, confirmed, created_at, updated_at)
                   VALUES('plan-a5', 'planner5', 'plan', '{}', 1, 1, 1);",
            )
            .await
            .unwrap();
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM execution_gates WHERE predicate_json LIKE '%artifact%' AND satisfied_at IS NOT NULL").await,
            1
        );

        let clauses = super::super::authoring::parse_after(Some(&gates(2)), "after").unwrap();
        let request = CallbackRequest {
            cwd: "/".to_string(),
            run_id: None,
            tool: "write".to_string(),
            payload: Value::Null,
            tool_use_id: None,
            thread_id: None,
        };
        patch_execution_gates(
            &env.orch,
            &request,
            "prj",
            5,
            2,
            GatePatch::Replace(clauses),
            false,
        )
        .await
        .unwrap();
    }

    /// Deleting a `pr_state` gate's target forecloses the dependent and wakes
    /// its filer, rather than leaving it held behind a PR that can never open.
    #[tokio::test]
    async fn deleting_a_pr_gate_target_forecloses_the_dependent() {
        let env = env("gate-sweep-deleted-pr-target.db").await;
        let after = json!([{"fact": "pr_state", "issue": "cairn://p/prj/1", "state": "merged"}]);
        file(&env, 2, after).await.unwrap();
        env.db
            .execute("DELETE FROM issues WHERE id = 'i1'", ())
            .await
            .unwrap();

        let first = sweep_at(&env.orch, now()).await.unwrap();
        let second = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(first.foreclosed.len(), 1);
        assert!(second.foreclosed.is_empty());
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_gates WHERE foreclosed_reason = 'cairn://p/prj/1 no longer exists'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM messages WHERE recipient_run_id = 'run-coord'"
            )
            .await,
            1
        );
        let (_, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!((status.as_str(), released_at, jobs), ("held", None, 0));
    }

    /// A filer that cannot be addressed when its gate forecloses (its job has
    /// no run yet, as a freshly established thread session does not) is still
    /// owed the wake: it arrives on the first sweep after a run exists, once.
    #[tokio::test]
    async fn a_foreclosure_wake_waits_for_an_addressable_filer_and_arrives_once() {
        let env = env("gate-sweep-foreclose-later.db").await;
        env.db
            .execute("DELETE FROM runs WHERE id = 'run-coord'", ())
            .await
            .unwrap();
        file(&env, 2, merged_after(1)).await.unwrap();
        resolve_issue(&env, 1, "closed").await;

        let latched = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            latched.foreclosed.len(),
            1,
            "the latch does not wait for a recipient"
        );
        let owed = "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE 'gate:%'";
        assert_eq!(count(&env, owed).await, 0, "no run to address yet");
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(count(&env, owed).await, 0, "still owed, not dropped");

        env.db
            .execute(
                "INSERT INTO runs(id, project_id, job_id, issue_id, status, created_at, updated_at)
                   VALUES('run-later', 'p', 'coord', 'ip', 'live', 2, 2)",
                (),
            )
            .await
            .unwrap();
        let delivered = sweep_at(&env.orch, now()).await.unwrap();
        sweep_at(&env.orch, now()).await.unwrap();
        assert!(delivered.foreclosed.is_empty(), "the latch is not re-taken");
        assert_eq!(count(&env, owed).await, 1);
        let messages = env
            .db
            .query_all(
                "SELECT content FROM messages WHERE recipient_run_id = 'run-later'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(messages.len(), 1, "exactly one late wake: {messages:?}");
        assert!(messages[0].contains("is closed"), "{}", messages[0]);
        let (_, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!((status.as_str(), released_at, jobs), ("held", None, 0));
    }

    /// Plan verification 5: a chain runs one link at a time, and a burst is
    /// released in ascending issue order.
    #[tokio::test]
    async fn a_chain_walks_one_link_at_a_time() {
        let mut env = env("gate-sweep-chain.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        file(&env, 3, merged_after(2)).await.unwrap();
        file(&env, 4, merged_after(3)).await.unwrap();
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE status = 'held'"
            )
            .await,
            3,
            "every link is visible from the start"
        );

        for (merge, starts) in [(1, 2), (2, 3), (3, 4)] {
            resolve_issue(&env, merge, "merged").await;
            let report = sweep_at(&env.orch, now()).await.unwrap();
            assert_eq!(report.released, vec![execution(&env, starts).await.0]);
            assert_eq!(
                count(
                    &env,
                    "SELECT COUNT(*) FROM executions WHERE status = 'held'"
                )
                .await,
                (4 - starts) as i64,
                "only the next link starts"
            );
        }
        assert_eq!(advanced(&mut env).len(), 3);
    }

    #[tokio::test]
    async fn a_burst_is_released_in_ascending_issue_order_without_a_cap() {
        let mut env = env("gate-sweep-burst.db").await;
        for number in [5, 2, 4, 3] {
            file(&env, number, merged_after(1)).await.unwrap();
        }
        resolve_issue(&env, 1, "merged").await;
        let report = sweep_at(&env.orch, now()).await.unwrap();
        let mut expected = Vec::new();
        for number in [2, 3, 4, 5] {
            expected.push(execution(&env, number).await.0);
        }
        assert_eq!(report.released, expected, "all at once, in issue order");
        assert_eq!(advanced(&mut env), expected);
    }

    /// Plan verification 6, clock half.
    #[tokio::test]
    async fn a_clock_gate_in_the_past_releases_at_creation() {
        let mut env = env("gate-sweep-clock-past.db").await;
        let past = chrono::DateTime::from_timestamp(now() - 3600, 0).unwrap();
        let summary = file(
            &env,
            2,
            json!([{"fact": "clock", "notBefore": past.to_rfc3339()}]),
        )
        .await
        .unwrap();
        assert!(summary.starts_with("Started execution 1"), "got: {summary}");
        let (id, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!(status, "running");
        assert!(released_at.is_some() && jobs > 0);
        assert_eq!(advanced(&mut env), vec![id]);
    }

    #[tokio::test]
    async fn a_future_clock_gate_bounds_the_scheduler_sleep_and_releases_when_due() {
        let env = env("gate-sweep-clock-future.db").await;
        let due = now() + 3600;
        let at = chrono::DateTime::from_timestamp(due, 0).unwrap();
        file(
            &env,
            2,
            json!([{"fact": "clock", "notBefore": at.to_rfc3339()}]),
        )
        .await
        .unwrap();
        assert_eq!(execution(&env, 2).await.1, "held");

        assert_eq!(next_gate_due_after(&env.orch, now()).await, Some(due));
        assert_eq!(
            next_gate_due_after(&env.orch, due).await,
            None,
            "a due instant already reached is the sweep's, not the sleep bound's"
        );

        assert!(sweep_at(&env.orch, due - 1)
            .await
            .unwrap()
            .released
            .is_empty());
        let report = sweep_at(&env.orch, due).await.unwrap();
        assert_eq!(report.released, vec![execution(&env, 2).await.0]);
        assert_eq!(next_gate_due_after(&env.orch, now()).await, None);
    }

    /// `check_verdict` with its gated-ref coordinate pinned, so the sweep is
    /// exercised without a repository while the verdict itself is still judged
    /// by the real reuse guard.
    #[derive(Debug)]
    struct PinnedCheckVerdict(CheckVerdictCoordinate);

    #[async_trait]
    impl GateFact for PinnedCheckVerdict {
        fn fact(&self) -> &'static str {
            CheckVerdictFact.fact()
        }
        fn fields(&self) -> &'static [(&'static str, FieldKind)] {
            CheckVerdictFact.fields()
        }
        fn example(&self) -> TriggerClause {
            CheckVerdictFact.example()
        }
        fn validate(&self, clause: &TriggerClause) -> Result<(), String> {
            CheckVerdictFact.validate(clause)
        }
        async fn resolve(
            &self,
            ctx: &GateContext<'_>,
            clause: &TriggerClause,
        ) -> DbResult<GateOutcome> {
            let issue = clause
                .get("issue")
                .and_then(Value::as_str)
                .unwrap_or_default();
            super::super::vocabulary::check_verdict_evidence(
                ctx.db_for_issue(issue).await,
                &self.0,
                clause.get("verdict"),
            )
            .map_err(DbError::internal)
        }
        async fn foreclosed(
            &self,
            _ctx: &GateContext<'_>,
            _clause: &TriggerClause,
        ) -> DbResult<Option<String>> {
            Ok(None)
        }
        fn render(&self, clause: &TriggerClause) -> String {
            CheckVerdictFact.render(clause)
        }
    }

    /// Plan verification 6, check half: a reusable green from a lane that
    /// persists zero per-test rows (rust-fmt's shape) releases.
    #[tokio::test]
    async fn a_reusable_green_with_no_per_test_rows_releases_a_check_gate() {
        let env = env("gate-sweep-check.db").await;
        file(
            &env,
            2,
            json!([{"fact": "check_verdict", "check": "rust-fmt", "issue": "cairn://p/prj/1", "verdict": "passed"}]),
        )
        .await
        .unwrap();
        let mut vocabulary = gate_vocabulary();
        vocabulary.register(Arc::new(PinnedCheckVerdict(CheckVerdictCoordinate {
            project_id: "p".to_string(),
            check_name: "rust-fmt".to_string(),
            input_hash: "input-gated".to_string(),
            verdict_platforms: vec!["linux".to_string()],
            verdict_environment_hash: "env-gated".to_string(),
        })));

        assert!(sweep_with(&env.orch, &vocabulary, now())
            .await
            .unwrap()
            .released
            .is_empty());
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_gates WHERE last_outcome = 'no_evidence'"
            )
            .await,
            1,
            "no verdict yet is recorded as such, not as a refutation"
        );

        crate::execution::cache::record_fresh_check_observation(
            env.db.clone(),
            green_observation("obs-fmt", "p", "input-gated", "tree-gated", "env-gated"),
        )
        .unwrap();
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM check_test_results").await,
            0,
            "the lane persisted no per-test rows"
        );
        let report = sweep_with(&env.orch, &vocabulary, now()).await.unwrap();
        assert_eq!(report.released, vec![execution(&env, 2).await.0]);
    }

    /// Sweeps racing each other, and a manual release racing them, start the
    /// execution exactly once: one set of jobs, one advancement.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_sweeps_release_exactly_once() {
        let mut env = env("gate-sweep-concurrent.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        let (id, ..) = execution(&env, 2).await;
        resolve_issue(&env, 1, "merged").await;

        let mut handles = Vec::new();
        for _ in 0..4 {
            let orch = env.orch.clone();
            handles.push(tokio::spawn(
                async move { sweep(&orch).await.unwrap().released },
            ));
        }
        let racing_release = {
            let orch = env.orch.clone();
            let id = id.clone();
            tokio::spawn(async move { release(&orch, &id, ReleaseCause::Satisfied).await.unwrap() })
        };
        let mut released = 0;
        for handle in handles {
            released += handle.await.unwrap().len();
        }
        released += usize::from(racing_release.await.unwrap());

        assert_eq!(released, 1, "exactly one caller owns the release");
        assert_eq!(
            count(
                &env,
                &format!(
                    "SELECT COUNT(*) - COUNT(DISTINCT recipe_node_id) FROM jobs WHERE execution_id = '{id}'"
                )
            )
            .await,
            0,
            "no node got a second job"
        );
        assert_eq!(advanced(&mut env), vec![id]);
    }

    fn label(number: i32) -> String {
        format!("execution 1 on cairn://p/prj/{number}")
    }

    async fn patch(env: &Env, number: i32, patch: GatePatch) -> Result<String, String> {
        let request = CallbackRequest {
            cwd: "/".to_string(),
            run_id: None,
            tool: "write".to_string(),
            payload: Value::Null,
            tool_use_id: None,
            thread_id: None,
        };
        patch_execution_gates(&env.orch, &request, "prj", number, 1, patch, false).await
    }

    use cairn_common::protocol::CallbackRequest;

    /// Plan verification 9: cycles are refused with their path.
    #[tokio::test]
    async fn a_wait_cycle_is_refused_naming_its_path() {
        let env = env("gate-sweep-cycle.db").await;
        file(&env, 2, merged_after(3)).await.unwrap();
        let error = file(&env, 3, merged_after(2)).await.unwrap_err();
        assert!(
            error.contains(
                "prj/3 after prj/2 after prj/3 \u{2014} an execution cannot wait on its own issue, directly or transitively."
            ),
            "got: {error}"
        );
        let error = file(&env, 4, merged_after(4)).await.unwrap_err();
        assert!(error.contains("prj/4 after prj/4"), "got: {error}");
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE issue_id IN ('i3', 'i4')"
            )
            .await,
            0,
            "a refused filing leaves nothing behind"
        );

        // Releasing the waiting link breaks the cycle, so the same filing is
        // then accepted.
        patch(&env, 2, GatePatch::Release).await.unwrap();
        file(&env, 3, merged_after(2)).await.unwrap();
    }

    /// An artifact or check-verdict gate is a wait edge like a resolution: a
    /// cycle through one is refused on filing and on gate patch alike.
    #[tokio::test]
    async fn a_cycle_through_artifact_and_check_gates_is_refused() {
        let env = env("gate-sweep-cycle-artifact.db").await;
        let plan_of = |n: i32| json!([{"fact": "artifact_confirmed", "issue": format!("cairn://p/prj/{n}"), "artifact": "plan"}]);
        let green_on = |n: i32| json!([{"fact": "check_verdict", "check": "lint", "issue": format!("cairn://p/prj/{n}"), "verdict": "passed"}]);

        file(&env, 2, plan_of(3)).await.unwrap();
        let error = file(&env, 3, plan_of(2)).await.unwrap_err();
        assert!(
            error.contains("prj/3 after prj/2 after prj/3"),
            "got: {error}"
        );
        let error = file(&env, 3, green_on(2)).await.unwrap_err();
        assert!(
            error.contains("prj/3 after prj/2 after prj/3"),
            "mixed facts: {error}"
        );

        // Through three issues and the patch door: 4 waits on 5's merge, 5 is
        // held on a clock, and re-gating 5 on 4's plan would close the loop.
        file(&env, 4, merged_after(5)).await.unwrap();
        file(
            &env,
            5,
            json!([{"fact": "clock", "notBefore": "2099-01-01T00:00:00Z"}]),
        )
        .await
        .unwrap();
        let clauses = super::super::authoring::parse_after(Some(&plan_of(4)), "after").unwrap();
        let error = patch(&env, 5, GatePatch::Replace(clauses))
            .await
            .unwrap_err();
        assert!(
            error.contains("prj/5 after prj/4 after prj/5"),
            "got: {error}"
        );
    }

    /// Waiting on your own issue's plan or verdict is legitimate while another
    /// live execution there can produce it, and refused, naming the way out,
    /// when none can. Waiting on your own resolution is always a cycle.
    #[tokio::test]
    async fn a_self_wait_on_an_artifact_or_verdict_needs_a_live_producer() {
        let env = env("gate-sweep-self-wait.db").await;
        let own_plan =
            json!([{"fact": "artifact_confirmed", "issue": "cairn://p/prj/3", "artifact": "plan"}]);
        let own_green = json!([{"fact": "check_verdict", "check": "lint", "issue": "cairn://p/prj/3", "verdict": "passed"}]);

        for after in [&own_plan, &own_green] {
            let error = file(&env, 3, after.clone()).await.unwrap_err();
            assert!(
                error.contains("no other live execution on prj/3"),
                "got: {error}"
            );
            assert!(
                error.contains("Start or keep the producing execution first"),
                "got: {error}"
            );
        }
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE issue_id = 'i3'"
            )
            .await,
            0,
            "a refused filing leaves nothing behind"
        );

        // A held producer produces nothing, so it does not count.
        file(&env, 3, merged_after(1)).await.unwrap();
        let error = file(&env, 3, own_plan.clone()).await.unwrap_err();
        assert!(error.contains("no other live execution"), "got: {error}");

        // A running one does.
        file(&env, 3, json!([])).await.unwrap();
        file(&env, 3, own_plan).await.unwrap();
        file(&env, 3, own_green).await.unwrap();
        let error = file(&env, 3, merged_after(3)).await.unwrap_err();
        assert!(error.contains("prj/3 after prj/3"), "got: {error}");
    }

    /// Plan verification 9: `{release:true}` records the principal, and gates on
    /// a released execution are frozen.
    #[tokio::test]
    async fn early_release_records_its_principal_and_freezes_the_gates() {
        let mut env = env("gate-sweep-override.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        let (id, ..) = execution(&env, 2).await;

        let summary = patch(&env, 2, GatePatch::Release).await.unwrap();
        assert!(
            summary.starts_with(&format!("Released {} early", label(2))),
            "got: {summary}"
        );
        let (_, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!(status, "running");
        assert!(released_at.is_some() && jobs > 0);
        assert_eq!(advanced(&mut env), vec![id.clone()]);

        let recorded: Value = serde_json::from_str(
            &env.db
                .query_opt_text(
                    "SELECT release_override_json FROM executions WHERE id = ?1",
                    (id.clone(),),
                )
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let expected_principal =
            serde_json::to_value(cairn_common::identity::PrincipalRef::Machine {
                device_id: env.orch.anon_device_manager.device_id(),
            })
            .unwrap();
        assert_eq!(recorded["principal"], expected_principal);
        assert_eq!(recorded["at"].as_i64(), released_at);

        let error = patch(&env, 2, GatePatch::Replace(Vec::new()))
            .await
            .unwrap_err();
        assert!(
            error.contains("was released at") && error.contains("one-way door"),
            "got: {error}"
        );
        let error = patch(&env, 2, GatePatch::Release).await.unwrap_err();
        assert!(error.contains("was released at"), "got: {error}");
    }

    #[tokio::test]
    async fn a_gate_patch_keeps_kept_evidence_and_emptying_it_starts_the_execution() {
        let mut env = env("gate-sweep-patch.db").await;
        let future = chrono::DateTime::from_timestamp(now() + 3600, 0).unwrap();
        let clock = json!({"fact": "clock", "notBefore": future.to_rfc3339()});
        file(&env, 2, json!([merged_after(1)[0].clone(), clock.clone()]))
            .await
            .unwrap();
        resolve_issue(&env, 1, "merged").await;
        sweep_at(&env.orch, now()).await.unwrap();
        let satisfied_before = count(
            &env,
            "SELECT COUNT(*) FROM execution_gates WHERE satisfied_at IS NOT NULL",
        )
        .await;
        assert_eq!(
            satisfied_before, 1,
            "the merge gate latched; the clock waits"
        );

        let clauses = super::super::authoring::parse_after(
            Some(&json!([clock, merged_after(1)[0].clone()])),
            "after",
        )
        .unwrap();
        let summary = patch(&env, 2, GatePatch::Replace(clauses)).await.unwrap();
        assert!(
            summary.contains("now waits after not before"),
            "got: {summary}"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM execution_gates WHERE satisfied_at IS NOT NULL AND seq = 1"
            )
            .await,
            1,
            "a kept clause keeps its latch and takes its new position"
        );

        let summary = patch(&env, 2, GatePatch::Replace(Vec::new()))
            .await
            .unwrap();
        assert!(summary.contains("started now"), "got: {summary}");
        let (id, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!(status, "running");
        assert!(released_at.is_some() && jobs > 0);
        assert_eq!(advanced(&mut env), vec![id]);
    }

    /// The held execution on issue `number` as the sweep enumerates it, and its
    /// gate list as of this moment — the snapshot a sweep resolves from.
    async fn snapshot(env: &Env, number: i32) -> (HeldExecution, Vec<ExecutionGate>) {
        let id = execution(env, number).await.0;
        let held = env
            .db
            .read(|conn| Box::pin(async move { list_held_executions_conn(conn).await }))
            .await
            .unwrap()
            .into_iter()
            .find(|held| held.execution_id == id)
            .unwrap();
        let gates = env
            .db
            .read(move |conn| {
                let id = id.clone();
                Box::pin(async move { list_gates_conn(conn, &id).await })
            })
            .await
            .unwrap();
        (held, gates)
    }

    async fn replace(env: &Env, number: i32, after: Value) -> Result<String, String> {
        let clauses = super::super::authoring::parse_after(Some(&after), "after").unwrap();
        patch(env, number, GatePatch::Replace(clauses)).await
    }

    /// Review finding: a sweep resolves the gate list it READ, and a patch can
    /// land between that read and the release. Each case below is that exact
    /// interleaving, made deterministic by handing the evaluation the stale
    /// snapshot: the sweep's view says "all satisfied", the stored gates do
    /// not, and the stored gates win.
    #[tokio::test]
    async fn a_gate_added_while_a_sweep_runs_is_not_bypassed() {
        let mut env = env("gate-sweep-race-add.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        let (held, stale) = snapshot(&env, 2).await;

        // The patch lands after the sweep read [A]: now [A, B] with B unmet.
        replace(
            &env,
            2,
            json!([merged_after(1)[0].clone(), merged_after(3)[0].clone()]),
        )
        .await
        .unwrap();
        resolve_issue(&env, 1, "merged").await;

        let evaluation =
            evaluate_gates(&env.orch, &env.db, &gate_vocabulary(), &held, &stale, now())
                .await
                .unwrap();
        assert!(
            !evaluation.released,
            "the stale view must not authorize a release"
        );
        let (_, status, released_at, jobs) = execution(&env, 2).await;
        assert_eq!((status.as_str(), released_at, jobs), ("held", None, 0));
        assert!(advanced(&mut env).is_empty());

        // An authoritative sweep sees B and keeps holding; B landing releases.
        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        resolve_issue(&env, 3, "merged").await;
        assert_eq!(sweep_at(&env.orch, now()).await.unwrap().released.len(), 1);
    }

    #[tokio::test]
    async fn a_gate_swapped_out_while_a_sweep_runs_is_not_bypassed() {
        let env = env("gate-sweep-race-swap.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        let (held, stale) = snapshot(&env, 2).await;

        // The patch replaces A with an unmet B after the sweep read [A].
        replace(&env, 2, merged_after(3)).await.unwrap();
        resolve_issue(&env, 1, "merged").await;

        let evaluation =
            evaluate_gates(&env.orch, &env.db, &gate_vocabulary(), &held, &stale, now())
                .await
                .unwrap();
        assert!(!evaluation.released);
        assert_eq!(execution(&env, 2).await.3, 0, "no jobs were minted");
    }

    /// Review finding: a gate whose target lives only in a team replica is
    /// resolved there, both when it is already true at filing and when it comes
    /// true later — never foreclosed as nonexistent.
    #[tokio::test]
    async fn a_gate_on_a_team_issue_reads_the_team_replica() {
        let mut env = env("gate-sweep-team.db").await;
        let team_db = attach_team(&env, "gate-sweep-team-replica.db").await;

        // Already true at filing: released at creation.
        let summary = file(&env, 2, team_merged(1)).await.unwrap();
        assert!(summary.starts_with("Started execution 1"), "got: {summary}");
        assert_eq!(advanced(&mut env).len(), 1);

        // True later: held, quiet rather than foreclosed, then released.
        file(&env, 3, team_merged(2)).await.unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert!(report.released.is_empty() && report.foreclosed.is_empty());
        team_db
            .execute(
                "UPDATE issues SET status = 'merged', merged_at = 6 WHERE id = 't2'",
                (),
            )
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![execution(&env, 3).await.0]);
    }

    /// A gate naming a project this host cannot see is silence, not a
    /// foreclosure: the project may be a replica not yet open.
    #[tokio::test]
    async fn a_gate_on_an_unreachable_project_waits_instead_of_foreclosing() {
        let env = env("gate-sweep-unreachable.db").await;
        file(
            &env,
            2,
            json!([{"fact": "issue_resolution", "issue": "cairn://p/ghost/1", "resolution": "merged"}]),
        )
        .await
        .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert!(report.foreclosed.is_empty() && report.released.is_empty());
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM messages WHERE recipient_run_id = 'run-coord'"
            )
            .await,
            0
        );
    }

    /// Review finding: resolving the issue that OWNS a held execution means it
    /// never starts, even once its gates come true, and it cannot be released
    /// by hand either.
    #[tokio::test]
    async fn a_held_execution_on_a_resolved_issue_never_starts() {
        let mut env = env("gate-sweep-owner-closed.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        resolve_issue(&env, 2, "closed").await;
        resolve_issue(&env, 1, "merged").await;

        assert!(sweep_at(&env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        let (id, _, released_at, jobs) = execution(&env, 2).await;
        assert_eq!((released_at, jobs), (None, 0));
        assert!(!release(&env.orch, &id, ReleaseCause::Satisfied)
            .await
            .unwrap());
        let error = patch(&env, 2, GatePatch::Release).await.unwrap_err();
        assert!(
            error.contains("already been merged or closed"),
            "got: {error}"
        );
        assert!(advanced(&mut env).is_empty());
    }

    async fn lifecycle(env: &Env, id: &str) -> String {
        env.db
            .query_opt_text(
                "SELECT lifecycle FROM executions WHERE id = ?1",
                (id.to_string(),),
            )
            .await
            .unwrap()
            .unwrap()
    }

    /// Seed issue 5 with a canonical execution `live` whose one job is running.
    async fn seed_live_attempt(env: &Env) {
        env.db
            .execute_script(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES('live', 'recipe', 'i5', 'p', 'running', 1, 1);
                 INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, uri_segment, created_at, updated_at)
                   VALUES('live-job', 'live', 'p', 'i5', 'Builder', 'running', 'builder', 1, 1);",
            )
            .await
            .unwrap();
    }

    /// Coordinator review: "re-run this after X merges" on an issue with a
    /// running attempt must not fence that attempt when filed. The held next
    /// attempt is `pending` while it waits, and takes the attempt at release.
    #[tokio::test]
    async fn a_held_next_attempt_fences_nothing_until_it_starts() {
        let env = env("gate-sweep-pending.db").await;
        seed_live_attempt(&env).await;

        let summary = file(&env, 5, merged_after(1)).await.unwrap();
        assert!(
            summary.contains("Filing it fenced nothing: execution 1 keeps running"),
            "got: {summary}"
        );
        let (held, ..) = execution(&env, 5).await;
        assert_eq!(lifecycle(&env, &held).await, "pending");
        assert_eq!(
            lifecycle(&env, "live").await,
            "canonical",
            "the live attempt still owns it"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM jobs WHERE id = 'live-job' AND status = 'running'"
            )
            .await,
            1,
            "its job was not cancelled"
        );
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM execution_supersessions").await,
            0
        );

        resolve_issue(&env, 1, "merged").await;
        assert_eq!(
            sweep_at(&env.orch, now()).await.unwrap().released,
            vec![held.clone()]
        );
        assert_eq!(lifecycle(&env, &held).await, "canonical");
        assert_eq!(lifecycle(&env, "live").await, "superseded");
        assert_eq!(
            count(
                &env,
                &format!(
                    "SELECT COUNT(*) FROM execution_supersessions
                      WHERE superseded_execution_id = 'live' AND successor_execution_id = '{held}'
                        AND actor_kind = 'gate-release'"
                )
            )
            .await,
            1,
            "release performed the supersede a launch would have"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE issue_id = 'i5' AND lifecycle = 'canonical'"
            )
            .await,
            1
        );
    }

    /// A `pending` held row — what the real filing path now produces for a
    /// held next attempt — renders through the census exactly like any other
    /// held execution, and the filing response speaks the same phrase.
    #[tokio::test]
    async fn a_pending_held_execution_renders_through_the_census() {
        let env = env("gate-sweep-pending-census.db").await;
        seed_live_attempt(&env).await;
        let summary = file(&env, 5, merged_after(1)).await.unwrap();
        let (held, ..) = execution(&env, 5).await;
        assert_eq!(lifecycle(&env, &held).await, "pending");

        let rendered = env
            .db
            .read(|conn| {
                Box::pin(async move {
                    super::super::census::gated_executions_for_issues_conn(
                        conn,
                        &["i5".to_string()],
                    )
                    .await
                })
            })
            .await
            .unwrap();
        // Pending is not superseded: the held next attempt is still waiting.
        assert!(rendered["i5"].wont_start.is_empty());
        let lines: Vec<&str> = rendered["i5"]
            .held
            .iter()
            .map(|held| held.line.as_str())
            .collect();
        let phrase = "waiting on issue cairn://p/prj/1 merged (no verdict yet)";
        assert_eq!(
            lines,
            vec![format!("exec 2 \u{2014} held, {phrase}").as_str()]
        );
        assert!(
            summary.contains("after issue cairn://p/prj/1 merged (no verdict yet)"),
            "the filing response and the census agree: {summary}"
        );
    }

    /// If the attempt a held filing was waiting to follow finishes first, the
    /// release supersedes a finished execution and cancels nothing.
    #[tokio::test]
    async fn a_release_after_the_prior_attempt_finished_cancels_nothing() {
        let env = env("gate-sweep-pending-finished.db").await;
        seed_live_attempt(&env).await;
        file(&env, 5, merged_after(1)).await.unwrap();
        env.db
            .execute_script(
                "UPDATE jobs SET status = 'complete' WHERE id = 'live-job';
                 UPDATE executions SET status = 'complete', completed_at = 3 WHERE id = 'live';",
            )
            .await
            .unwrap();

        resolve_issue(&env, 1, "merged").await;
        assert_eq!(sweep_at(&env.orch, now()).await.unwrap().released.len(), 1);
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM jobs WHERE id = 'live-job' AND status = 'complete'"
            )
            .await,
            1,
            "a finished job is left exactly as it finished"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE id = 'live' AND status = 'complete'"
            )
            .await,
            1
        );
    }

    /// A team replica holding project `tp` with issues 1 (merged) and 2 (live).
    async fn attach_team(env: &Env, name: &str) -> Arc<LocalDb> {
        let team_db = Arc::new(crate::storage::migrated_test_db(name).await);
        team_db
            .execute_script(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                   VALUES('tp', 'default', 'Team', 'tp', '/tmp/team', 1, 1);
                 INSERT INTO issues(id, project_id, number, title, status, progress, attention, merged_at, created_at, updated_at)
                   VALUES('t1', 'tp', 1, 'Landed', 'merged', 'merged', 'none', 5, 1, 1);
                 INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                   VALUES('t2', 'tp', 2, 'Pending', 'active', 'active', 'none', 1, 1);",
            )
            .await
            .unwrap();
        env.orch
            .db
            .insert_team_db_for_test("teamgate", team_db.clone())
            .await;
        env.orch
            .db
            .set_route("tp", Some("teamgate".to_string()))
            .await;
        team_db
    }

    fn team_merged(number: i32) -> Value {
        json!([{"fact": "issue_resolution", "issue": format!("cairn://p/tp/{number}"), "resolution": "merged"}])
    }

    /// Coordinator review: a cycle through a team replica would never
    /// foreclose, so the walk must cross databases. A private-DB execution on
    /// prj/2 waits on team issue tp/2; filing on tp/2 after prj/2 — checked
    /// inside the TEAM replica's transaction — must find it.
    #[tokio::test]
    async fn a_cycle_through_another_database_is_refused_with_its_path() {
        let env = env("gate-sweep-cycle-team.db").await;
        let team_db = attach_team(&env, "gate-sweep-cycle-team-replica.db").await;
        file(&env, 2, team_merged(2)).await.unwrap();

        let predicates = super::super::authoring::canonical_predicates(
            &super::super::authoring::parse_after(Some(&merged_after(2)), "after").unwrap(),
            "after",
        )
        .unwrap();
        let (dbs, own) = (env.orch.db.clone(), team_db.clone());
        let error = team_db
            .write(|conn| {
                let (dbs, own, predicates) = (dbs.clone(), own.clone(), predicates.clone());
                Box::pin(async move {
                    super::super::authoring::refuse_cycles_conn(
                        conn,
                        &dbs,
                        &own,
                        "t2",
                        None,
                        &predicates,
                        &std::collections::HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(
                "tp/2 after prj/2 after tp/2 \u{2014} an execution cannot wait on its own issue"
            ),
            "got: {error}"
        );
    }

    /// The same walk from the other side, through the real filing path: a
    /// held execution in the team replica on tp/2 waits on prj/2, so filing on
    /// prj/2 after tp/2 is refused.
    #[tokio::test]
    async fn a_filing_that_closes_a_cycle_in_a_team_replica_is_refused() {
        let env = env("gate-sweep-cycle-team-file.db").await;
        let team_db = attach_team(&env, "gate-sweep-cycle-team-file-replica.db").await;
        let predicate = super::super::authoring::canonical_predicates(
            &super::super::authoring::parse_after(Some(&merged_after(2)), "after").unwrap(),
            "after",
        )
        .unwrap()
        .remove(0);
        team_db
            .execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
                   VALUES('team-held', 'recipe', 't2', 'tp', 'held', 1, 1, 'pending')",
                (),
            )
            .await
            .unwrap();
        team_db
            .execute(
                "INSERT INTO execution_gates(id, execution_id, seq, predicate_json, created_at)
                   VALUES('team-gate', 'team-held', 0, ?1, 1)",
                (predicate,),
            )
            .await
            .unwrap();

        let error = file(&env, 2, team_merged(2)).await.unwrap_err();
        assert!(
            error.contains("prj/2 after tp/2 after prj/2"),
            "got: {error}"
        );
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM executions WHERE issue_id = 'i2'"
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn gates_cannot_be_added_to_an_execution_that_was_never_held() {
        let env = env("gate-sweep-ungated.db").await;
        file(&env, 2, json!([])).await.unwrap();
        let clauses =
            super::super::authoring::parse_after(Some(&merged_after(1)), "after").unwrap();
        let error = patch(&env, 2, GatePatch::Replace(clauses))
            .await
            .unwrap_err();
        assert!(error.contains("was never held"), "got: {error}");
    }
}
