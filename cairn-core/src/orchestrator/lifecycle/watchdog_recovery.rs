//! Canonical exactly-once recovery for an expired provider-progress watchdog.

use std::sync::Arc;

use crate::execution::jobs::{continue_job_launch_locked_for_watchdog, ResumeContext};
use crate::orchestrator::Orchestrator;
use crate::runs::watchdog_ledger::{
    append_watchdog_lifecycle, claim_watchdog_recovery, finish_watchdog_recovery,
    owns_watchdog_recovery_claim, NewWatchdogLifecycle, WatchdogIdentity, WatchdogLifecycleKind,
};
use crate::storage::{run_db_blocking, LocalDb, RowExt};

use super::{
    kill_session_with_reason, report_recovery_launch_failure, PROVIDER_SILENCE_RECOVERY_EXIT_REASON,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderWatchdogRecovery {
    LostClaim,
    Recovered {
        reason: String,
        successor: Option<(String, String)>,
    },
}

pub(super) fn require_fresh_successor_session(
    db: Arc<LocalDb>,
    identity: &WatchdogIdentity,
    target: &CurrentRecoveryTarget,
    claimed_at: i64,
) -> Result<bool, String> {
    let identity = identity.clone();
    let job_id = target.job_id.clone();
    let turn_id = target.cairn_turn_id.clone();
    let changed = run_db_blocking(move || async move {
        db.execute(
            "UPDATE jobs SET needs_fresh_session = 1
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
        )
        .await
        .map_err(|error| error.to_string())
    })?;
    Ok(changed == 1)
}

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
                let closed = conn
                    .execute(
                        "UPDATE sessions
                         SET status = 'closed', terminal_reason = ?1, closed_at = ?2, updated_at = ?2
                         WHERE id = ?3 AND status = 'open'
                           AND EXISTS (
                             SELECT 1 FROM jobs j
                             JOIN turns t ON t.id = j.current_turn_id
                             JOIN codex_watchdog_leases w
                               ON w.run_id = ?4 AND w.session_id = ?3
                              AND w.provider_turn_id = ?5 AND w.generation = ?6
                             WHERE j.id = ?7 AND j.current_session_id = ?3
                               AND j.current_turn_id = ?8 AND j.status IN ('blocked', 'complete')
                               AND t.state = 'complete'
                               AND w.state = 'recovery_claimed' AND w.updated_at = ?9
                           )",
                        (
                            ALREADY_TERMINAL_RECONCILED_REASON,
                            now,
                            identity.session_id.as_str(),
                            identity.run_id.as_str(),
                            identity.provider_turn_id.as_str(),
                            identity.generation.as_str(),
                            job_id.as_str(),
                            turn_id.as_str(),
                            claimed_at,
                        ),
                    )
                    .await?;
                if closed != 1 {
                    return Ok(false);
                }
                let prepared = conn
                    .execute(
                        "UPDATE jobs
                         SET needs_fresh_session = CASE WHEN status = 'blocked' THEN 1 ELSE needs_fresh_session END,
                             updated_at = ?1
                         WHERE id = ?2 AND current_session_id = ?3 AND current_turn_id = ?4",
                        (
                            now,
                            job_id.as_str(),
                            identity.session_id.as_str(),
                            turn_id.as_str(),
                        ),
                    )
                    .await?;
                if prepared != 1 {
                    return Err(crate::storage::DbError::internal(
                        "settled watchdog recovery lost its job ownership",
                    ));
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
    let _launch_guard = launch_lock
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
        return Ok(ProviderWatchdogRecovery::Recovered {
            reason: ALREADY_TERMINAL_RECONCILED_REASON.to_string(),
            successor: None,
        });
    }

    // A successor must rotate away from the provider session whose ownership
    // expired. The ordinary fresh-session input is consumed exactly once by
    // `continue_job_impl`, which performs the canonical close + replacement
    // linkage rather than reviving the stale provider handle.
    if !require_fresh_successor_session(db.clone(), identity, &target, claimed_at)? {
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
    let successor_run = continue_job_launch_locked_for_watchdog(
        orch,
        &target.job_id,
        Some(ResumeContext {
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            ..ResumeContext::default()
        }),
    );
    let successor_run = match successor_run {
        Ok(run) => run,
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
    let successor_turn_id = match orch.process_state.get_current_turn_id(&successor_run.id) {
        Some(turn_id) => Some(turn_id),
        None => match current_head_turn_id(db.clone(), &target.job_id) {
            Ok(turn_id) => turn_id,
            Err(error) => {
                report_recovery_launch_failure(orch, &identity.run_id);
                record_lifecycle(
                    db.clone(),
                    identity,
                    WatchdogLifecycleKind::SuccessorFailed,
                    Some("provider_silence_successor_lookup_failed"),
                    Some(serde_json::json!({ "error": error })),
                    now,
                );
                finish_recovery(
                    db,
                    identity,
                    claimed_at,
                    false,
                    "provider_silence_successor_lookup_failed",
                    None,
                    now,
                )?;
                return Err(format!(
                    "watchdog successor run {} launched but its turn lookup failed: {error}",
                    successor_run.id
                ));
            }
        },
    };
    let Some(successor_turn_id) = successor_turn_id else {
        report_recovery_launch_failure(orch, &identity.run_id);
        record_lifecycle(
            db.clone(),
            identity,
            WatchdogLifecycleKind::SuccessorFailed,
            Some("provider_silence_successor_turn_missing"),
            None,
            now,
        );
        finish_recovery(
            db,
            identity,
            claimed_at,
            false,
            "provider_silence_successor_turn_missing",
            None,
            now,
        )?;
        return Err(format!(
            "watchdog successor run {} launched without a current turn",
            successor_run.id
        ));
    };
    let successor = (successor_run.id, successor_turn_id);
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
    Ok(ProviderWatchdogRecovery::Recovered {
        reason: PROVIDER_SILENCE_RECOVERY_EXIT_REASON.to_string(),
        successor: Some(successor),
    })
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

fn current_head_turn_id(db: Arc<LocalDb>, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.query_opt(
            "SELECT current_turn_id FROM jobs WHERE id = ?1",
            (job_id,),
            |row| row.opt_text(0),
        )
        .await
        .map_err(|e| e.to_string())
        .map(Option::flatten)
    })
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
