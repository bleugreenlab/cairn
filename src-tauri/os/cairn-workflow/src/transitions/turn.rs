//! Turn state transitions.

use crate::models::{TurnEndReason, TurnState, TurnYieldReason};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_platform::services::emitter::EventEmitter;

use super::TransitionError;

pub async fn start_turn(
    db: &LocalDb,
    turn_id: &str,
    run_id: &str,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    let from = load_turn_state(db, turn_id, "running").await?;
    if from != TurnState::Pending {
        return Err(turn_error(
            turn_id,
            &from,
            &TurnState::Running,
            "can only start a pending turn",
        ));
    }

    let turn_id = turn_id.to_string();
    let run_id = run_id.to_string();
    db.write(|conn| {
        let turn_id = turn_id.clone();
        let run_id = run_id.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "UPDATE turns
                 SET state = 'running', run_id = ?1, started_at = ?2, updated_at = ?3
                 WHERE id = ?4",
                params![run_id.as_str(), now, now, turn_id.as_str()],
            )
            .await?;
            let _ = cairn_db::stored_status::jobs::set_head_turn_at(
                conn,
                &turn_id,
                cairn_db::stored_status::jobs::HeadTurn::Running,
                now,
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| turn_db_error(&turn_id, "pending", "running", error))?;

    emit_turn_update(db, &turn_id, emitter).await;
    Ok(())
}

pub async fn apply_turn_outcome(
    db: &LocalDb,
    turn_id: &str,
    outcome: TurnState,
    end_reason: Option<TurnEndReason>,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    if !matches!(
        outcome,
        TurnState::Complete | TurnState::Failed | TurnState::Cancelled
    ) {
        return Err(TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "unknown".to_string(),
            to: outcome.to_string(),
            reason: "apply_turn_outcome only accepts Complete, Failed, or Cancelled".to_string(),
        });
    }

    let from = load_turn_state(db, turn_id, &outcome.to_string()).await?;
    if from == outcome {
        return Ok(());
    }
    if from.is_terminal()
        && !matches!(
            (&from, &outcome),
            (TurnState::Yielded, TurnState::Cancelled)
        )
    {
        return Err(turn_error(
            turn_id,
            &from,
            &outcome,
            "turn already terminal",
        ));
    }
    match (&from, &outcome) {
        (TurnState::Running, _) => {}
        (TurnState::Pending, TurnState::Failed | TurnState::Cancelled) => {}
        (TurnState::Yielded, TurnState::Cancelled) => {}
        _ => {
            return Err(turn_error(
                turn_id,
                &from,
                &outcome,
                "transition not allowed",
            ));
        }
    }

    update_terminal_turn(db, turn_id, outcome.clone(), None, end_reason).await?;
    emit_turn_update(db, turn_id, emitter).await;
    Ok(())
}

pub async fn yield_turn(
    db: &LocalDb,
    turn_id: &str,
    reason: TurnYieldReason,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    let from = load_turn_state(db, turn_id, "yielded").await?;
    if from != TurnState::Running {
        return Err(turn_error(
            turn_id,
            &from,
            &TurnState::Yielded,
            "can only yield a running turn",
        ));
    }

    update_terminal_turn(db, turn_id, TurnState::Yielded, Some(reason), None).await?;
    emit_turn_update(db, turn_id, emitter).await;
    Ok(())
}

/// Yield a running turn inside the caller's write transaction, so whatever owns
/// the wait can be recorded atomically with the yielded head. Returns whether the
/// turn was running; the caller decides whether any other state is acceptable.
pub async fn yield_running_turn_conn(
    conn: &cairn_db::storage::TrackedConnection,
    turn_id: &str,
    reason: TurnYieldReason,
    now: i64,
) -> crate::storage::DbResult<bool> {
    let changed = conn
        .execute(
            "UPDATE turns
             SET state = ?1, yield_reason = ?2, end_reason = NULL, ended_at = ?3, updated_at = ?3
             WHERE id = ?4 AND state = ?5",
            params![
                TurnState::Yielded.to_string(),
                reason.to_string(),
                now,
                turn_id,
                TurnState::Running.to_string()
            ],
        )
        .await?;
    if changed > 0 {
        let _ = cairn_db::stored_status::jobs::set_head_turn_at(
            conn,
            turn_id,
            cairn_db::stored_status::jobs::HeadTurn::Idle,
            now,
        )
        .await?;
        // A parked turn may free its execution's fleet slot.
        conn.on_commit(cairn_db::admission_hint::evidence_changed);
    }
    Ok(changed > 0)
}

pub async fn interrupt_turn(
    db: &LocalDb,
    turn_id: &str,
    end_reason: Option<TurnEndReason>,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    let from = load_turn_state(db, turn_id, "interrupted").await?;
    if from == TurnState::Interrupted {
        return Ok(());
    }
    if from.is_terminal() {
        return Err(turn_error(
            turn_id,
            &from,
            &TurnState::Interrupted,
            "turn already terminal",
        ));
    }
    if from != TurnState::Running {
        return Err(turn_error(
            turn_id,
            &from,
            &TurnState::Interrupted,
            "can only interrupt a running turn",
        ));
    }

    update_terminal_turn(db, turn_id, TurnState::Interrupted, None, end_reason).await?;
    emit_turn_update(db, turn_id, emitter).await;
    Ok(())
}

async fn load_turn_state(
    db: &LocalDb,
    turn_id: &str,
    target: &str,
) -> Result<TurnState, TransitionError> {
    let turn_id_owned = turn_id.to_string();
    let current = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
                        (turn_id_owned.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal(format!("turn not found: {turn_id_owned}")))?;
                row.text(0)
            })
        })
        .await
        .map_err(|error| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "unknown".to_string(),
            to: target.to_string(),
            reason: error.to_string(),
        })?;

    current.parse().map_err(|_| TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: current.clone(),
        to: target.to_string(),
        reason: format!("unparseable current state: {}", current),
    })
}

async fn update_terminal_turn(
    db: &LocalDb,
    turn_id: &str,
    state: TurnState,
    reason: Option<TurnYieldReason>,
    end_reason: Option<TurnEndReason>,
) -> Result<(), TransitionError> {
    let turn_id_owned = turn_id.to_string();
    db.write(|conn| {
        let turn_id = turn_id_owned.clone();
        let state = state.clone();
        let reason = reason.clone();
        let end_reason = end_reason.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "UPDATE turns
                 SET state = ?1, yield_reason = ?2, end_reason = ?3, ended_at = ?4, updated_at = ?5
                 WHERE id = ?6",
                params![
                    state.to_string(),
                    reason.map(|value| value.to_string()),
                    end_reason.map(|value| value.to_string()),
                    now,
                    now,
                    turn_id.as_str()
                ],
            )
            .await?;
            // Every state this writes is terminal: the head is no longer due.
            let _ = cairn_db::stored_status::jobs::set_head_turn_at(
                conn,
                &turn_id,
                cairn_db::stored_status::jobs::HeadTurn::Idle,
                now,
            )
            .await?;
            conn.on_commit(cairn_db::admission_hint::evidence_changed);
            Ok(())
        })
    })
    .await
    .map_err(|error| turn_db_error(turn_id, "unknown", &state.to_string(), error))
}

fn turn_error(
    turn_id: &str,
    from: &TurnState,
    to: &TurnState,
    reason: impl Into<String>,
) -> TransitionError {
    TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: from.to_string(),
        to: to.to_string(),
        reason: reason.into(),
    }
}

fn turn_db_error(turn_id: &str, from: &str, to: &str, error: DbError) -> TransitionError {
    TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: from.to_string(),
        to: to.to_string(),
        reason: format!("DB error: {error}"),
    }
}

async fn emit_turn_update(db: &LocalDb, turn_id: &str, emitter: &dyn EventEmitter) {
    let change = cairn_db::changes::turn_db_change_for_id(db, turn_id, "update").await;
    let _ = emitter.emit("db-change", change);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::admission_hint::sent_on_this_thread;
    use cairn_platform::services::emitter::CapturingEmitter;

    #[tokio::test(flavor = "current_thread")]
    async fn activity_transitions_hint_after_commit_not_on_rollback() {
        let db = crate::storage::migrated_test_db("turn-activity-hint.db").await;
        let emitter = CapturingEmitter::new();
        for (sequence, state) in [
            TurnState::Yielded,
            TurnState::Complete,
            TurnState::Failed,
            TurnState::Interrupted,
            TurnState::Cancelled,
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("hint-{sequence}");
            db.execute(
                "INSERT INTO turns(id,session_id,sequence,state,start_reason,created_at,updated_at)
                 VALUES(?1,'hint-session',?2,'running','initial',1,1)",
                params![id.as_str(), sequence as i64],
            )
            .await
            .unwrap();
            let before = sent_on_this_thread();
            match state {
                TurnState::Yielded => {
                    yield_turn(&db, &id, TurnYieldReason::UserInput, &emitter).await
                }
                TurnState::Interrupted => interrupt_turn(&db, &id, None, &emitter).await,
                _ => apply_turn_outcome(&db, &id, state.clone(), None, &emitter).await,
            }
            .unwrap();
            assert_eq!(sent_on_this_thread(), before + 1, "{state}");
        }

        db.execute(
            "INSERT INTO turns(id,session_id,sequence,state,start_reason,created_at,updated_at)
             VALUES('rollback','hint-session',9,'running','initial',1,1)",
            (),
        )
        .await
        .unwrap();
        let before = sent_on_this_thread();
        let result: crate::storage::DbResult<()> = db
            .write(|conn| {
                Box::pin(async move {
                    assert!(
                        yield_running_turn_conn(conn, "rollback", TurnYieldReason::Wait, 2).await?
                    );
                    assert_eq!(sent_on_this_thread(), before, "hint must wait for commit");
                    Err(DbError::internal("rollback"))
                })
            })
            .await;
        assert!(result.is_err());
        assert_eq!(sent_on_this_thread(), before);
        db.write(|conn| {
            Box::pin(async move {
                assert!(yield_running_turn_conn(conn, "rollback", TurnYieldReason::Wait, 2).await?);
                assert_eq!(sent_on_this_thread(), before);
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(sent_on_this_thread(), before + 1);
    }
}
