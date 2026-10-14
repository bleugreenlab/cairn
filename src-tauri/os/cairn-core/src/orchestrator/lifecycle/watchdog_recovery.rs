//! Canonical exactly-once recovery for an expired provider-progress watchdog.

use std::sync::Arc;

use crate::execution::jobs::{
    continue_job_launch_locked_for_watchdog, ResumeContext, WatchdogContinuation,
};
use crate::orchestrator::Orchestrator;
use crate::runs::watchdog_ledger::{
    append_watchdog_lifecycle, claim_watchdog_recovery, finish_watchdog_recovery,
    owns_watchdog_recovery_claim, record_pending_recovery_successor, reserve_watchdog_successor,
    NewWatchdogLifecycle, WatchdogIdentity, WatchdogLifecycleKind, WatchdogPhase,
};
use crate::storage::{run_db_blocking, LocalDb, RowExt};

use super::{
    kill_session_with_reason, report_recovery_launch_failure, PROVIDER_SILENCE_RECOVERY_EXIT_REASON,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderWatchdogRecovery {
    LostClaim,
    PreservedUncertain {
        reason: String,
    },
    Recovered {
        reason: String,
        successor: Option<(String, String)>,
    },
    Queued {
        reason: String,
        successor: (String, String),
    },
}

fn recovery_phase(db: Arc<LocalDb>, identity: &WatchdogIdentity) -> Result<WatchdogPhase, String> {
    let identity = identity.clone();
    run_db_blocking(move || async move {
        db.query_opt(
            "SELECT phase FROM codex_watchdog_leases
             WHERE run_id = ?1 AND session_id = ?2 AND provider_turn_id = ?3
               AND generation = ?4 AND state = 'recovery_claimed'",
            (
                identity.run_id,
                identity.session_id,
                identity.provider_turn_id,
                identity.generation,
            ),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "watchdog recovery phase was unavailable".to_string())
        .and_then(|phase| match phase.as_str() {
            "provider_progress" => Ok(WatchdogPhase::ProviderProgress),
            "tool_outstanding" => Ok(WatchdogPhase::ToolOutstanding),
            "post_tool_continuation" => Ok(WatchdogPhase::PostToolContinuation),
            _ => Err(format!("unknown watchdog recovery phase {phase:?}")),
        })
    })
}

/// Whether this recovery still owns the exact job coordinate it claimed.
///
/// This used to be `require_fresh_successor_session`, which wrote
/// `needs_fresh_session = 1` under the same predicate so the successor would
/// start on a replacement session. A provider that stopped emitting for the
/// watchdog interval is uncertainty, not evidence that its thread is dead, and
/// CAIRN-4603 already established that uncertainty may not replace a native
/// conversation. The successor now resumes the same session; if the provider
/// really cannot resolve it, the backend says so on the resume attempt and the
/// digest-reseed fallback rebuilds from the transcript with a visible notice.
/// The predicate itself is kept, because a recovery that lost its target must
/// still stand down rather than continue a job it no longer owns.
pub(super) fn still_owns_recovery_target(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    target: &CurrentRecoveryTarget,
    claimed_at: i64,
) -> Result<bool, String> {
    let identity = identity.clone();
    let job_id = target.job_id.clone();
    let turn_id = target.cairn_turn_id.clone();
    run_db_blocking(move || async move {
        db.query_opt(
            "SELECT 1 FROM jobs
             WHERE id = ?1 AND current_session_id = ?2 AND current_turn_id = ?7
               AND EXISTS (
                 SELECT 1 FROM codex_watchdog_leases
                 WHERE run_id = ?3 AND session_id = ?2 AND provider_turn_id = ?4
                   AND generation = ?5 AND state = 'recovery_claimed' AND updated_at = ?6
               )",
            (
                job_id,
                identity.session_id,
                identity.run_id,
                identity.provider_turn_id,
                identity.generation,
                claimed_at,
                turn_id,
            ),
            |_| Ok(()),
        )
        .await
        .map(|row| row.is_some())
        .map_err(|error| error.to_string())
    })
}

/// Record that the watched turn had already reached its terminal state before
/// recovery arrived, and hand the lease its terminal reason.
///
/// Returns whether this recovery was in fact the settled kind. It reads the exact
/// job/turn/lease coordinate it claimed and writes nothing to the session.
///
/// It used to close that session with `provider_silence_already_terminal_reconciled`
/// and flag the job for a replacement, which is how 199 of the reference
/// install's 1,181 native-thread replacements happened: the turn had *completed
/// normally*, the watchdog merely noticed late, and the response was to discard a
/// healthy provider conversation. Killing the wedged process is recoverable
/// because a resume spawns another one against the same native thread; closing
/// the session is not, and nothing about a late notification says the thread
/// stopped being resumable (CAIRN-4618).
pub(super) fn finish_settled_recovery(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    target: &CurrentRecoveryTarget,
    claimed_at: i64,
    now: i64,
) -> Result<bool, String> {
    let identity = identity.clone();
    let job_id = target.job_id.clone();
    let turn_id = target.cairn_turn_id.clone();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let identity = identity.clone();
            let job_id = job_id.clone();
            let turn_id = turn_id.clone();
            Box::pin(async move {
                let mut settled = conn
                    .query(
                        "SELECT 1 FROM jobs j
                         JOIN turns t ON t.id = j.current_turn_id
                         JOIN sessions s ON s.id = j.current_session_id
                         JOIN codex_watchdog_leases w
                           ON w.run_id = ?1 AND w.session_id = ?2
                          AND w.provider_turn_id = ?3 AND w.generation = ?4
                         WHERE j.id = ?5 AND j.current_session_id = ?2
                           AND j.current_turn_id = ?6 AND j.status IN ('blocked', 'complete')
                           AND s.status = 'open'
                           AND t.state = 'complete'
                           AND w.state = 'recovery_claimed' AND w.updated_at = ?7",
                        (
                            identity.run_id.as_str(),
                            identity.session_id.as_str(),
                            identity.provider_turn_id.as_str(),
                            identity.generation.as_str(),
                            job_id.as_str(),
                            turn_id.as_str(),
                            claimed_at,
                        ),
                    )
                    .await?;
                if settled.next().await?.is_none() {
                    return Ok(false);
                }
                let finished = conn
                    .execute(
                        "UPDATE codex_watchdog_leases
                         SET state = 'terminalized', terminal_reason = ?1, updated_at = ?2
                         WHERE run_id = ?3 AND session_id = ?4 AND provider_turn_id = ?5
                           AND generation = ?6 AND state = 'recovery_claimed' AND updated_at = ?7",
                        (
                            ALREADY_TERMINAL_RECONCILED_REASON,
                            now,
                            identity.run_id.as_str(),
                            identity.session_id.as_str(),
                            identity.provider_turn_id.as_str(),
                            identity.generation.as_str(),
                            claimed_at,
                        ),
                    )
                    .await?;
                if finished != 1 {
                    return Err(crate::storage::DbError::internal(
                        "settled watchdog recovery lost its durable claim",
                    ));
                }
                Ok(true)
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

fn recovery_claim_is_owned(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    claimed_at: i64,
) -> Result<bool, String> {
    let identity = identity.clone();
    run_db_blocking(move || async move {
        owns_watchdog_recovery_claim(&db, &identity, claimed_at)
            .await
            .map_err(|error| error.to_string())
    })
}

pub(super) struct CurrentRecoveryTarget {
    pub(super) job_id: String,
    pub(super) cairn_turn_id: String,
    pub(super) turn_state: String,
    pub(super) job_status: String,
}

pub(crate) const ALREADY_TERMINAL_RECONCILED_REASON: &str =
    "provider_silence_already_terminal_reconciled";
const RECOVERY_SUCCESSOR_BUDGET: i64 = 3;
const RECOVERY_SUCCESSOR_WINDOW_SECS: i64 = 30 * 60;

/// The triggers that will resume a job on their own, so recovery has no need to
/// mint a successor for it.
///
/// Named as a constant because it has a second reader. The stalled-child sweep
/// must agree with recovery, exactly, about which jobs resume themselves: this
/// is the predicate that makes recovery DECLINE to start a successor
/// ([`recover_provider_watchdog`] returns `Recovered { successor: None }` on
/// it), and a job left session-less on purpose for that reason is waiting, not
/// stalled. Two copies of this question would eventually disagree, and the
/// disagreement would surface as a parent being told that recovery had
/// exhausted its attempts on a child that recovery had deliberately left alone.
pub(super) const PENDING_RESUME_TRIGGER_SQL: &str = "SELECT 1 WHERE
               EXISTS (SELECT 1 FROM queued_messages WHERE job_id = ?1 AND delivered_at IS NULL)
               OR EXISTS (SELECT 1 FROM agent_waits WHERE job_id = ?1 AND state IN ('pending', 'resolving'))
               OR EXISTS (SELECT 1 FROM jobs WHERE parent_job_id = ?1 AND status NOT IN ('complete', 'failed', 'cancelled'))
               OR EXISTS (
                 SELECT 1 FROM wake_subscriptions
                 WHERE job_id = ?1 AND state IN ('active', 'muted')
                   AND NOT (created_by = 'system' AND source_kind IN ('user', 'peer'))
               )
             LIMIT 1";

pub(crate) fn has_pending_resume_trigger(db: Arc<LocalDb>, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.query_opt(PENDING_RESUME_TRIGGER_SQL, (job_id,), |_| Ok(()))
            .await
            .map(|row| row.is_some())
            .map_err(|error| error.to_string())
    })
}

/// [`has_pending_resume_trigger`] for a caller already inside a runtime.
pub(super) async fn has_pending_resume_trigger_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<bool, String> {
    db.query_opt(
        PENDING_RESUME_TRIGGER_SQL,
        (job_id.to_string(),),
        |_| Ok(()),
    )
    .await
    .map(|row| row.is_some())
    .map_err(|error| error.to_string())
}

/// Whether the job a settled reconciliation just published still needs someone
/// to move it — asked after `recompute_job`, which may have advanced it.
///
/// Together with `target.job_status`, this replaces the `needs_fresh_session = 1`
/// read that used to gate the successor. That flag was doing two unrelated jobs
/// at once: recording that the job was blocked when recovery claimed it, and
/// requiring the successor to start on a replacement conversation. Only the first
/// was ever wanted here, and the target already carries it (CAIRN-4618).
fn job_is_still_live(db: Arc<LocalDb>, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.query_opt(
            "SELECT 1 FROM jobs WHERE id = ?1 AND status IN ('blocked', 'running')",
            (job_id,),
            |_| Ok(()),
        )
        .await
        .map(|row| row.is_some())
        .map_err(|error| error.to_string())
    })
}

fn record_successor_on_lease(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    successor_run_id: &str,
    successor_turn_id: Option<&str>,
    now: i64,
) {
    let identity = identity.clone();
    let successor_run_id = successor_run_id.to_string();
    let successor_turn_id = successor_turn_id.map(str::to_string);
    if let Err(error) = run_db_blocking(move || async move {
        crate::runs::watchdog_ledger::record_recovery_successor(
            &db,
            &identity,
            &successor_run_id,
            successor_turn_id.as_deref(),
            now,
        )
        .await
        .map_err(|error| error.to_string())
    }) {
        // The successor is already running; a lost bookkeeping write must not
        // unwind it. The cost of the miss is one redundant sweep candidate,
        // which `has_pending_resume_trigger` and the successor budget bound.
        log::warn!("failed to record watchdog recovery successor on its lease: {error}");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SuccessorReservation {
    Reserved,
    BudgetExhausted,
    Undetermined(String),
}

fn reserve_recovery_successor(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    job_id: &str,
    now: i64,
) -> SuccessorReservation {
    let identity = identity.clone();
    let job_id = job_id.to_string();
    match run_db_blocking(move || async move {
        reserve_watchdog_successor(
            &db,
            &identity,
            &job_id,
            now.saturating_sub(RECOVERY_SUCCESSOR_WINDOW_SECS),
            RECOVERY_SUCCESSOR_BUDGET,
            now,
        )
        .await
        .map_err(|error| error.to_string())
    }) {
        Ok(true) => SuccessorReservation::Reserved,
        Ok(false) => SuccessorReservation::BudgetExhausted,
        Err(error) => SuccessorReservation::Undetermined(error),
    }
}

/// Recover one exact provider turn after its durable watchdog expires.
///
/// The lease claim is the ownership boundary shared by the live worker and every
/// reconciler. All destructive work happens only after that claim and a fresh
/// check of the run, session, and Cairn head turn. A stale claimant therefore
/// cannot kill a normally completed turn or a successor that already replaced it.
pub fn recover_provider_watchdog(
    orch: &Orchestrator,
    identity: &WatchdogIdentity,
    now: i64,
) -> Result<ProviderWatchdogRecovery, String> {
    let db = owning_db_for_identity(orch, identity)?;
    let claimed_at = now;
    if !run_db_blocking({
        let db = db.clone();
        let identity = identity.clone();
        move || async move {
            claim_watchdog_recovery(&db, &identity, claimed_at)
                .await
                .map_err(|e| e.to_string())
        }
    })? {
        return Ok(ProviderWatchdogRecovery::LostClaim);
    }
    record_lifecycle(
        db.clone(),
        identity,
        WatchdogLifecycleKind::RecoveryClaimed,
        Some("provider_watchdog_expired"),
        None,
        now,
    );

    let phase = recovery_phase(db.clone(), identity)?;
    let target = match current_recovery_target(db.clone(), identity, claimed_at) {
        Ok(target) => target,
        Err(error) => {
            let reason = "watchdog_target_validation_failed";
            finish_recovery(db, identity, claimed_at, false, reason, None, now)?;
            return Err(format!(
                "failed to validate watchdog recovery target for run {}: {error}",
                identity.run_id
            ));
        }
    };
    let Some(target) = target else {
        let reason = "watchdog_target_no_longer_current";
        finish_recovery(db, identity, claimed_at, true, reason, None, now)?;
        return Ok(ProviderWatchdogRecovery::Recovered {
            reason: reason.to_string(),
            successor: None,
        });
    };

    if !recovery_claim_is_owned(db.clone(), identity, claimed_at)? {
        return Ok(ProviderWatchdogRecovery::LostClaim);
    }
    let settled_without_successor =
        target.turn_state == "complete" && target.job_status == "complete";
    if phase == WatchdogPhase::PostToolContinuation && !settled_without_successor {
        match crate::backends::stdin::interrupt_exact_codex_turn(
            &orch.process_state,
            &identity.run_id,
            &identity.provider_turn_id,
        ) {
            Ok(outcome) => {
                let reason = match outcome {
                    cairn_llm::session::codex::ExactTurnInterrupt::AlreadyTerminal { .. } => {
                        "post_tool_interrupt_already_terminal"
                    }
                    cairn_llm::session::codex::ExactTurnInterrupt::Acknowledged { .. } => {
                        "post_tool_interrupt_acknowledged"
                    }
                };
                record_lifecycle(
                    db.clone(),
                    identity,
                    WatchdogLifecycleKind::TerminalizationSucceeded,
                    Some(reason),
                    Some(serde_json::json!({
                        "provider_turn_id": identity.provider_turn_id,
                        "continuation": "provider_goal",
                        "observation": format!("{outcome:?}")
                    })),
                    now,
                );
                if !recovery_claim_is_owned(db.clone(), identity, claimed_at)? {
                    return Ok(ProviderWatchdogRecovery::LostClaim);
                }
                match crate::backends::stdin::reactivate_goal(
                    &orch.process_state,
                    &identity.run_id,
                    None,
                    crate::execution::jobs::JobRunnable::Unknown,
                ) {
                    Err(error) => {
                        record_lifecycle(
                            db.clone(),
                            identity,
                            WatchdogLifecycleKind::TerminalizationFailed,
                            Some("post_tool_goal_reactivation_failed"),
                            Some(serde_json::json!({ "error": error })),
                            now,
                        );
                    }
                    Ok(outcome) => {
                        let reason = match &outcome {
                            crate::backends::stdin::GoalReactivationResult::StillPaused {
                                ..
                            } => "post_tool_goal_continuation_held",
                            _ => "post_tool_goal_continuation_rearmed",
                        };
                        finish_recovery(db, identity, claimed_at, true, reason, None, now)?;
                        return Ok(ProviderWatchdogRecovery::Recovered {
                            reason: reason.to_string(),
                            successor: None,
                        });
                    }
                }
            }
            Err(error) if error.contains("active Codex turn mismatch") => {
                let reason = "post_tool_interrupt_target_advanced";
                record_lifecycle(
                    db.clone(),
                    identity,
                    WatchdogLifecycleKind::TerminalizationSucceeded,
                    Some(reason),
                    Some(serde_json::json!({
                        "error": error,
                        "outcome": "stale_target_noop"
                    })),
                    now,
                );
                finish_recovery(db, identity, claimed_at, true, reason, None, now)?;
                return Ok(ProviderWatchdogRecovery::Recovered {
                    reason: reason.to_string(),
                    successor: None,
                });
            }
            Err(error) => {
                let reason = if error.contains("acknowledgement timed out") {
                    "post_tool_interrupt_ack_timeout"
                } else {
                    "post_tool_interrupt_delivery_failed"
                };
                record_lifecycle(
                    db.clone(),
                    identity,
                    WatchdogLifecycleKind::TerminalizationFailed,
                    Some(reason),
                    Some(serde_json::json!({ "error": error })),
                    now,
                );
                // A missing interrupt acknowledgement is uncertainty, not proof
                // that the provider thread is dead. Preserve the live process,
                // session, and native thread so an operator continuation can
                // retry it without forfeiting native context.
                finish_recovery(db, identity, claimed_at, false, reason, None, now)?;
                return Ok(ProviderWatchdogRecovery::PreservedUncertain {
                    reason: reason.to_string(),
                });
            }
        }
    }
    if !settled_without_successor {
        match reserve_recovery_successor(db.clone(), identity, &target.job_id, now) {
            SuccessorReservation::Reserved => {}
            SuccessorReservation::BudgetExhausted => {
                report_recovery_launch_failure(orch, &identity.run_id);
                finish_recovery(
                    db,
                    identity,
                    claimed_at,
                    false,
                    "provider_silence_recovery_budget_exhausted",
                    None,
                    now,
                )?;
                return Err(format!(
                    "watchdog recovery budget exhausted for job {}",
                    target.job_id
                ));
            }
            SuccessorReservation::Undetermined(error) => {
                record_lifecycle(
                    db.clone(),
                    identity,
                    WatchdogLifecycleKind::SuccessorFailed,
                    Some("provider_silence_recovery_reservation_failed"),
                    Some(serde_json::json!({ "error": error })),
                    now,
                );
                finish_recovery(
                    db,
                    identity,
                    claimed_at,
                    false,
                    "provider_silence_recovery_reservation_failed",
                    None,
                    now,
                )?;
                return Err(format!(
                    "failed to reserve watchdog successor for job {}: {error}",
                    target.job_id
                ));
            }
        }
    }
    if let Err(error) = kill_session_with_reason(
        orch,
        &identity.run_id,
        PROVIDER_SILENCE_RECOVERY_EXIT_REASON,
    ) {
        record_lifecycle(
            db.clone(),
            identity,
            WatchdogLifecycleKind::TerminalizationFailed,
            Some("provider_silence_terminalization_failed"),
            Some(serde_json::json!({ "error": error })),
            claimed_at,
        );
        finish_recovery(
            db,
            identity,
            claimed_at,
            false,
            "provider_silence_terminalization_failed",
            None,
            now,
        )?;
        return Err(format!(
            "failed to terminalize watchdog run {} turn {}: {error}",
            identity.run_id, target.cairn_turn_id
        ));
    }

    let launch_lock = orch.job_launch_lock(&target.job_id);
    let launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if finish_settled_recovery(db.clone(), identity, &target, claimed_at, now)? {
        if let Err(error) = crate::execution::advancement::recompute_job(orch, &target.job_id) {
            log::error!(
                "failed to recompute job {} after settled watchdog recovery: {}",
                target.job_id,
                error
            );
        }
        record_lifecycle(
            db.clone(),
            identity,
            WatchdogLifecycleKind::TerminalizationSucceeded,
            Some(ALREADY_TERMINAL_RECONCILED_REASON),
            Some(serde_json::json!({
                "turn_id": target.cairn_turn_id,
                "observed_turn_state": target.turn_state,
                "observed_job_status": target.job_status,
                "outcome": "already_terminal_reconciled"
            })),
            now,
        );
        // `target.job_status` is the status read under this recovery's claim, so
        // it answers "was this job waiting when we arrived" without depending on
        // what the recompute above just did; `job_is_still_live` then confirms
        // the recompute did not already finish it.
        if target.job_status != "blocked"
            || !job_is_still_live(db.clone(), &target.job_id)?
            || has_pending_resume_trigger(db.clone(), &target.job_id)?
        {
            return Ok(ProviderWatchdogRecovery::Recovered {
                reason: ALREADY_TERMINAL_RECONCILED_REASON.to_string(),
                successor: None,
            });
        }
        let launch = continue_job_launch_locked_for_watchdog(
            orch,
            &target.job_id,
            Some(ResumeContext {
                suppress_user_event: true,
                suppress_self_suspend_note: true,
                ..ResumeContext::default()
            }),
            launch_guard,
        )?;
        let (started, run, turn_id) = match launch {
            WatchdogContinuation::Started { run, turn_id } => (true, run, turn_id),
            WatchdogContinuation::Queued { run, turn_id } => (false, run, turn_id),
        };
        let successor = (run.id, turn_id);
        record_successor_on_lease(db.clone(), identity, &successor.0, Some(&successor.1), now);
        if started {
            record_lifecycle(
                db,
                identity,
                WatchdogLifecycleKind::SuccessorStarted,
                Some(ALREADY_TERMINAL_RECONCILED_REASON),
                Some(serde_json::json!({ "predecessor_turn_id": target.cairn_turn_id })),
                now,
            );
            return Ok(ProviderWatchdogRecovery::Recovered {
                reason: ALREADY_TERMINAL_RECONCILED_REASON.to_string(),
                successor: Some(successor),
            });
        }
        notify_watchdog_successor_started(db.clone(), &successor.0, &successor.1);
        if successor_is_started(orch, &successor) {
            return Ok(ProviderWatchdogRecovery::Recovered {
                reason: ALREADY_TERMINAL_RECONCILED_REASON.to_string(),
                successor: Some(successor),
            });
        }
        return Ok(ProviderWatchdogRecovery::Queued {
            reason: ALREADY_TERMINAL_RECONCILED_REASON.to_string(),
            successor,
        });
    }

    // The successor continues on the same session. A provider that stopped
    // emitting for the watchdog interval is silence, not a death certificate for
    // its thread: `continue_job_impl` resumes it natively, and if the provider
    // genuinely cannot resolve it, it says so on the resume attempt and the
    // digest-reseed fallback rebuilds from the transcript with a visible notice
    // (CAIRN-4603, CAIRN-4618). What still has to hold is ownership — a recovery
    // whose target moved on must stand down rather than continue a job it no
    // longer owns.
    if !still_owns_recovery_target(db.clone(), identity, &target, claimed_at)? {
        if !recovery_claim_is_owned(db.clone(), identity, claimed_at)? {
            return Ok(ProviderWatchdogRecovery::LostClaim);
        }
        let reason = "watchdog_target_advanced_during_recovery";
        finish_recovery(db, identity, claimed_at, true, reason, None, now)?;
        return Ok(ProviderWatchdogRecovery::Recovered {
            reason: reason.to_string(),
            successor: None,
        });
    }

    record_lifecycle(
        db.clone(),
        identity,
        WatchdogLifecycleKind::TerminalizationSucceeded,
        Some(PROVIDER_SILENCE_RECOVERY_EXIT_REASON),
        None,
        now,
    );

    if !recovery_claim_is_owned(db.clone(), identity, claimed_at)? {
        return Ok(ProviderWatchdogRecovery::LostClaim);
    }
    let successor_launch = continue_job_launch_locked_for_watchdog(
        orch,
        &target.job_id,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            ..ResumeContext::default()
        }),
        launch_guard,
    );
    let successor_launch = match successor_launch {
        Ok(launch) => launch,
        Err(error) => {
            report_recovery_launch_failure(orch, &identity.run_id);
            record_lifecycle(
                db.clone(),
                identity,
                WatchdogLifecycleKind::SuccessorFailed,
                Some("provider_silence_successor_failed"),
                Some(serde_json::json!({ "error": error })),
                now,
            );
            finish_recovery(
                db,
                identity,
                claimed_at,
                false,
                "provider_silence_successor_failed",
                None,
                now,
            )?;
            return Err(format!(
                "watchdog terminalized run {} but failed to launch its successor: {error}",
                identity.run_id
            ));
        }
    };
    let (started, run, turn_id) = match successor_launch {
        WatchdogContinuation::Started { run, turn_id } => (true, run, turn_id),
        WatchdogContinuation::Queued { run, turn_id } => (false, run, turn_id),
    };
    let successor = (run.id, turn_id);
    if started {
        record_lifecycle(
            db.clone(),
            identity,
            WatchdogLifecycleKind::SuccessorStarted,
            Some(PROVIDER_SILENCE_RECOVERY_EXIT_REASON),
            Some(serde_json::json!({ "predecessor_turn_id": target.cairn_turn_id })),
            now,
        );
        finish_recovery(
            db,
            identity,
            claimed_at,
            true,
            PROVIDER_SILENCE_RECOVERY_EXIT_REASON,
            Some((&successor.0, &successor.1)),
            now,
        )?;
        return Ok(ProviderWatchdogRecovery::Recovered {
            reason: PROVIDER_SILENCE_RECOVERY_EXIT_REASON.to_string(),
            successor: Some(successor),
        });
    }
    let recorded = run_db_blocking({
        let db = db.clone();
        let identity = identity.clone();
        let successor = successor.clone();
        move || async move {
            record_pending_recovery_successor(
                &db,
                &identity,
                claimed_at,
                &successor.0,
                &successor.1,
            )
            .await
            .map_err(|error| error.to_string())
        }
    })?;
    if !recorded {
        return Ok(ProviderWatchdogRecovery::LostClaim);
    }
    notify_watchdog_successor_started(db.clone(), &successor.0, &successor.1);
    if successor_is_started(orch, &successor) {
        Ok(ProviderWatchdogRecovery::Recovered {
            reason: PROVIDER_SILENCE_RECOVERY_EXIT_REASON.to_string(),
            successor: Some(successor),
        })
    } else {
        Ok(ProviderWatchdogRecovery::Queued {
            reason: PROVIDER_SILENCE_RECOVERY_EXIT_REASON.to_string(),
            successor,
        })
    }
}

fn successor_is_started(orch: &Orchestrator, successor: &(String, String)) -> bool {
    orch.process_state.has_process(&successor.0) == Some(true)
        && orch
            .process_state
            .get_current_turn_id(&successor.0)
            .as_deref()
            == Some(successor.1.as_str())
}

/// Promotion is the authority for `SuccessorStarted`: durable enqueue alone is
/// not enough. A queued recovery leaves its successor coordinate on the lease,
/// and this hook completes that lease only after the process and turn exist.
pub(crate) fn notify_watchdog_successor_started(
    db: Arc<LocalDb>,
    successor_run_id: &str,
    successor_turn_id: &str,
) {
    let successor_run_id = successor_run_id.to_string();
    let successor_turn_id = successor_turn_id.to_string();
    let result = run_db_blocking(move || async move {
        let candidates = db
            .query_all(
                "SELECT run_id,session_id,provider_turn_id,generation,state,updated_at
                 FROM codex_watchdog_leases
                 WHERE successor_run_id=?1 AND successor_turn_id=?2
                   AND state IN ('recovery_claimed','terminalized')",
                (successor_run_id.clone(), successor_turn_id.clone()),
                |row| {
                    Ok((
                        WatchdogIdentity {
                            run_id: row.text(0)?,
                            session_id: row.text(1)?,
                            provider_turn_id: row.text(2)?,
                            generation: row.text(3)?,
                        },
                        row.text(4)?,
                        row.i64(5)?,
                    ))
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        for (identity, state, claimed_at) in candidates {
            let reason = if state == "recovery_claimed" {
                PROVIDER_SILENCE_RECOVERY_EXIT_REASON
            } else {
                ALREADY_TERMINAL_RECONCILED_REASON
            };
            if state == "recovery_claimed"
                && !finish_watchdog_recovery(
                    &db,
                    &identity,
                    claimed_at,
                    true,
                    reason,
                    Some((&successor_run_id, &successor_turn_id)),
                    chrono::Utc::now().timestamp(),
                )
                .await
                .map_err(|error| error.to_string())?
            {
                continue;
            }
            append_watchdog_lifecycle(
                &db,
                NewWatchdogLifecycle {
                    id: uuid::Uuid::new_v4().to_string(),
                    identity,
                    kind: WatchdogLifecycleKind::SuccessorStarted,
                    phase: None,
                    reason: Some(reason.to_string()),
                    details: Some(serde_json::json!({ "source": "ordinary_admission_promotion" })),
                    successor_run_id: Some(successor_run_id.clone()),
                    successor_turn_id: Some(successor_turn_id.clone()),
                    created_at: chrono::Utc::now().timestamp(),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok::<(), String>(())
    });
    if let Err(error) = result {
        log::error!("failed to complete queued watchdog successor: {error}");
    }
}

fn owning_db_for_identity(
    orch: &Orchestrator,
    identity: &WatchdogIdentity,
) -> Result<Arc<LocalDb>, String> {
    run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = identity.run_id.clone();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })
}

pub(super) fn current_recovery_target(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    claimed_at: i64,
) -> Result<Option<CurrentRecoveryTarget>, String> {
    let identity = identity.clone();
    run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT s.job_id, j.current_turn_id, t.state, j.status
                   FROM runs r
                   JOIN sessions s ON s.id = r.session_id
                   JOIN jobs j ON j.id = s.job_id
                   JOIN turns t ON t.id = j.current_turn_id
                   JOIN codex_watchdog_leases w
                     ON w.run_id = r.id AND w.session_id = s.id
                  WHERE r.id = ?1 AND s.id = ?2
                    AND w.provider_turn_id = ?3 AND w.generation = ?4
                    AND w.state = 'recovery_claimed' AND w.updated_at = ?5
                    AND r.status IN ('starting', 'live')
                    AND s.status = 'open'
                    AND t.run_id = r.id AND t.session_id = s.id
                  LIMIT 1",
                        (
                            identity.run_id.as_str(),
                            identity.session_id.as_str(),
                            identity.provider_turn_id.as_str(),
                            identity.generation.as_str(),
                            claimed_at,
                        ),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some(CurrentRecoveryTarget {
                        job_id: row.text(0)?,
                        cairn_turn_id: row.text(1)?,
                        turn_state: row.text(2)?,
                        job_status: row.text(3)?,
                    })),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub(crate) fn sweep_stranded_watchdog_jobs(orch: &Orchestrator) -> usize {
    const STRANDED_GRACE_SECS: i64 = 120;
    let db = orch.db.local.clone();
    let now = chrono::Utc::now().timestamp();
    let candidates = match run_db_blocking({
        let db = db.clone();
        move || async move {
            db.query_all(
                "SELECT j.id, w.run_id, w.session_id, w.provider_turn_id, w.generation
                 FROM jobs j
                 JOIN runs r ON r.job_id = j.id
                 JOIN codex_watchdog_leases w ON w.run_id = r.id
                 WHERE j.status IN ('blocked', 'running')
                   AND j.current_session_id = w.session_id
                   AND w.state = 'terminalized'
                   AND w.terminal_reason = 'provider_silence_already_terminal_reconciled'
                   -- The lease's own successor field is what says this
                   -- reconciliation has already been continued. It replaces
                   -- `needs_fresh_session`, which a successor consumed by
                   -- rotating — so the flag disarmed the sweep only as a side
                   -- effect of replacing a native conversation (CAIRN-4618).
                   AND w.successor_run_id IS NULL
                   AND EXISTS (
                     SELECT 1 FROM sessions current_session
                     WHERE current_session.id = j.current_session_id
                       AND current_session.status = 'open'
                   )
                   AND j.updated_at <= ?1
                   AND NOT EXISTS (
                     SELECT 1 FROM runs live
                     WHERE live.job_id = j.id AND live.status IN ('starting', 'live')
                   )
                   AND w.rowid = (
                     SELECT latest.rowid FROM codex_watchdog_leases latest
                     JOIN runs latest_run ON latest_run.id = latest.run_id
                     WHERE latest_run.job_id = j.id ORDER BY latest.updated_at DESC LIMIT 1
                   )",
                (now.saturating_sub(STRANDED_GRACE_SECS),),
                |row| {
                    Ok((
                        row.text(0)?,
                        WatchdogIdentity {
                            run_id: row.text(1)?,
                            session_id: row.text(2)?,
                            provider_turn_id: row.text(3)?,
                            generation: row.text(4)?,
                        },
                    ))
                },
            )
            .await
            .map_err(|error| error.to_string())
        }
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            log::error!("stranded watchdog sweep failed to load candidates: {error}");
            return 0;
        }
    };

    let mut recovered = 0;
    for (job_id, identity) in candidates {
        let launch_lock = orch.job_launch_lock(&job_id);
        let guard = match launch_lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => continue,
        };
        if has_pending_resume_trigger(db.clone(), &job_id).unwrap_or(true) {
            continue;
        }
        let reservation = reserve_recovery_successor(db.clone(), &identity, &job_id, now);
        if reservation != SuccessorReservation::Reserved {
            report_recovery_launch_failure(orch, &identity.run_id);
            let (reason, details) = match reservation {
                SuccessorReservation::BudgetExhausted => (
                    "stranded_recovery_budget_exhausted",
                    serde_json::json!({ "source": "periodic_sweep" }),
                ),
                SuccessorReservation::Undetermined(error) => (
                    "stranded_recovery_reservation_failed",
                    serde_json::json!({ "source": "periodic_sweep", "error": error }),
                ),
                SuccessorReservation::Reserved => unreachable!(),
            };
            record_lifecycle(
                db.clone(),
                &identity,
                WatchdogLifecycleKind::SuccessorFailed,
                Some(reason),
                Some(details),
                now,
            );
            continue;
        }
        match continue_job_launch_locked_for_watchdog(
            orch,
            &job_id,
            Some(ResumeContext {
                suppress_user_event: true,
                suppress_self_suspend_note: true,
                ..ResumeContext::default()
            }),
            guard,
        ) {
            Ok(launch) => {
                let (started, run, successor_turn_id) = match launch {
                    WatchdogContinuation::Started { run, turn_id } => (true, run, turn_id),
                    WatchdogContinuation::Queued { run, turn_id } => (false, run, turn_id),
                };
                record_successor_on_lease(
                    db.clone(),
                    &identity,
                    &run.id,
                    Some(&successor_turn_id),
                    now,
                );
                if started {
                    record_lifecycle(
                        db.clone(),
                        &identity,
                        WatchdogLifecycleKind::SuccessorStarted,
                        Some("stranded_watchdog_recovery"),
                        Some(serde_json::json!({
                            "source": "periodic_sweep",
                            "successor_run_id": run.id
                        })),
                        now,
                    );
                    recovered += 1;
                }
            }
            Err(error) => {
                report_recovery_launch_failure(orch, &identity.run_id);
                record_lifecycle(
                    db.clone(),
                    &identity,
                    WatchdogLifecycleKind::SuccessorFailed,
                    Some("stranded_watchdog_successor_failed"),
                    Some(serde_json::json!({ "error": error, "source": "periodic_sweep" })),
                    now,
                );
            }
        }
    }
    recovered
}

fn finish_recovery(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    claimed_at: i64,
    succeeded: bool,
    reason: &str,
    successor: Option<(&str, &str)>,
    now: i64,
) -> Result<(), String> {
    let identity = identity.clone();
    let reason = reason.to_string();
    let successor = successor.map(|(run, turn)| (run.to_string(), turn.to_string()));
    let changed = run_db_blocking(move || async move {
        finish_watchdog_recovery(
            &db,
            &identity,
            claimed_at,
            succeeded,
            &reason,
            successor
                .as_ref()
                .map(|ids| (ids.0.as_str(), ids.1.as_str())),
            now,
        )
        .await
        .map_err(|e| e.to_string())
    })?;
    if changed {
        Ok(())
    } else {
        Err("watchdog recovery outcome lost its durable claim".to_string())
    }
}

fn record_lifecycle(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    kind: WatchdogLifecycleKind,
    reason: Option<&str>,
    details: Option<serde_json::Value>,
    now: i64,
) {
    let event = NewWatchdogLifecycle {
        id: uuid::Uuid::new_v4().to_string(),
        identity: identity.clone(),
        kind,
        phase: None,
        reason: reason.map(str::to_string),
        details,
        successor_run_id: None,
        successor_turn_id: None,
        created_at: now,
    };
    if let Err(error) = run_db_blocking(move || async move {
        append_watchdog_lifecycle(&db, event)
            .await
            .map_err(|e| e.to_string())
    }) {
        log::error!("failed to append provider watchdog recovery lifecycle: {error}");
    }
}
