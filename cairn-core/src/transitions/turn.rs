//! Turn state transitions.
//!
//! Three lifecycle functions are the **only** paths to terminalize a turn:
//!
//! - `apply_turn_outcome` — Complete or fail a turn
//! - `yield_turn` — Yield a turn for host interaction (ask_user, permission)
//! - `interrupt_turn` — Interrupt a turn (user stop, crash)
//!
//! Valid transitions:
//! - Pending → Running (process attached, agent starts working)
//! - Running → Yielded (host interaction needed)
//! - Running → Complete (agent finishes successfully)
//! - Running → Failed (semantic failure)
//! - Running → Interrupted (user stop, process crash)
//! - Running → Cancelled (parent job cancelled)
//!
//! No re-entry. Every terminal state is final.

use crate::models::{TurnState, TurnYieldReason};
use crate::schema::turns;
use crate::services::EventEmitter;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::TransitionError;

/// Transition a turn from Pending to Running.
///
/// Called when a process attaches to the turn and starts working.
/// Also sets the run_id (late binding).
pub fn start_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    run_id: &str,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    let current_str: String = turns::table
        .find(turn_id)
        .select(turns::state)
        .first(conn)
        .map_err(|_| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "unknown".to_string(),
            to: "running".to_string(),
            reason: "turn not found".to_string(),
        })?;

    let from: TurnState = current_str.parse().map_err(|_| TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: current_str.clone(),
        to: "running".to_string(),
        reason: format!("unparseable current state: {}", current_str),
    })?;

    if from != TurnState::Pending {
        return Err(TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: "running".to_string(),
            reason: "can only start a pending turn".to_string(),
        });
    }

    let now = chrono::Utc::now().timestamp() as i32;

    diesel::update(turns::table.find(turn_id))
        .set((
            turns::state.eq("running"),
            turns::run_id.eq(Some(run_id)),
            turns::started_at.eq(Some(now)),
            turns::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "pending".to_string(),
            to: "running".to_string(),
            reason: format!("DB error: {}", e),
        })?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );

    Ok(())
}

/// Complete or fail a turn.
///
/// Called by warm transition, finalize_run, etc.
pub fn apply_turn_outcome(
    conn: &mut SqliteConnection,
    turn_id: &str,
    outcome: TurnState,
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

    let current_str: String = turns::table
        .find(turn_id)
        .select(turns::state)
        .first(conn)
        .map_err(|_| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "unknown".to_string(),
            to: outcome.to_string(),
            reason: "turn not found".to_string(),
        })?;

    let from: TurnState = current_str.parse().map_err(|_| TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: current_str.clone(),
        to: outcome.to_string(),
        reason: format!("unparseable current state: {}", current_str),
    })?;

    // Idempotent: if already in the target state, no-op
    if from == outcome {
        return Ok(());
    }

    // Already terminal for a different reason — no re-entry
    if from.is_terminal() {
        return Err(TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: outcome.to_string(),
            reason: "turn already terminal".to_string(),
        });
    }

    // Running → Complete/Failed/Cancelled (normal path)
    // Pending → Failed/Cancelled (process crashed before start_turn was called)
    match (&from, &outcome) {
        (TurnState::Running, _) => {}
        (TurnState::Pending, TurnState::Failed | TurnState::Cancelled) => {}
        _ => {
            return Err(TransitionError {
                entity: "turn",
                id: turn_id.to_string(),
                from: from.to_string(),
                to: outcome.to_string(),
                reason: format!("invalid transition: {:?} → {:?}", from, outcome),
            });
        }
    }

    let now = chrono::Utc::now().timestamp() as i32;

    diesel::update(turns::table.find(turn_id))
        .set((
            turns::state.eq(outcome.to_string()),
            turns::ended_at.eq(Some(now)),
            turns::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: outcome.to_string(),
            reason: format!("DB error: {}", e),
        })?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );

    Ok(())
}

/// Yield a turn for host interaction.
///
/// Called by ask_user and permission_prompt handlers.
pub fn yield_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    reason: TurnYieldReason,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    let current_str: String = turns::table
        .find(turn_id)
        .select(turns::state)
        .first(conn)
        .map_err(|_| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "unknown".to_string(),
            to: "yielded".to_string(),
            reason: "turn not found".to_string(),
        })?;

    let from: TurnState = current_str.parse().map_err(|_| TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: current_str.clone(),
        to: "yielded".to_string(),
        reason: format!("unparseable current state: {}", current_str),
    })?;

    if from != TurnState::Running {
        return Err(TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: "yielded".to_string(),
            reason: "can only yield a running turn".to_string(),
        });
    }

    let now = chrono::Utc::now().timestamp() as i32;

    diesel::update(turns::table.find(turn_id))
        .set((
            turns::state.eq("yielded"),
            turns::yield_reason.eq(Some(reason.to_string())),
            turns::ended_at.eq(Some(now)),
            turns::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: "yielded".to_string(),
            reason: format!("DB error: {}", e),
        })?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );

    Ok(())
}

/// Interrupt a turn.
///
/// Called by stop_session and crash handling.
pub fn interrupt_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    emitter: &dyn EventEmitter,
) -> Result<(), TransitionError> {
    let current_str: String = turns::table
        .find(turn_id)
        .select(turns::state)
        .first(conn)
        .map_err(|_| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: "unknown".to_string(),
            to: "interrupted".to_string(),
            reason: "turn not found".to_string(),
        })?;

    let from: TurnState = current_str.parse().map_err(|_| TransitionError {
        entity: "turn",
        id: turn_id.to_string(),
        from: current_str.clone(),
        to: "interrupted".to_string(),
        reason: format!("unparseable current state: {}", current_str),
    })?;

    // Idempotent: if already interrupted, no-op
    if from == TurnState::Interrupted {
        return Ok(());
    }

    // Already terminal for a different reason — no re-entry
    if from.is_terminal() {
        return Err(TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: "interrupted".to_string(),
            reason: "turn already terminal".to_string(),
        });
    }

    if from != TurnState::Running {
        return Err(TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: "interrupted".to_string(),
            reason: "can only interrupt a running turn".to_string(),
        });
    }

    let now = chrono::Utc::now().timestamp() as i32;

    diesel::update(turns::table.find(turn_id))
        .set((
            turns::state.eq("interrupted"),
            turns::ended_at.eq(Some(now)),
            turns::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| TransitionError {
            entity: "turn",
            id: turn_id.to_string(),
            from: from.to_string(),
            to: "interrupted".to_string(),
            reason: format!("DB error: {}", e),
        })?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewTurn;
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::test_diesel_conn;

    fn mock_emitter() -> CapturingEmitter {
        CapturingEmitter::new()
    }

    fn insert_turn(conn: &mut SqliteConnection, id: &str, state: &str) {
        let now = chrono::Utc::now().timestamp() as i32;

        // Insert a run if state requires run_id (FK constraint)
        if state != "pending" {
            let _ = diesel::insert_into(crate::schema::runs::table)
                .values(&crate::diesel_models::NewRun {
                    id: "run-1",
                    issue_id: None,
                    project_id: None,
                    job_id: None,
                    chat_id: None,
                    status: Some("live"),
                    session_id: None,
                    error_message: None,
                    started_at: Some(now),
                    exited_at: None,
                    created_at: now,
                    updated_at: now,
                    backend: None,
                    exit_reason: None,
                    start_mode: None,
                })
                .execute(conn);
        }

        let new_turn = NewTurn {
            id,
            session_id: "session-1",
            run_id: if state == "pending" {
                None
            } else {
                Some("run-1")
            },
            job_id: None, // No FK on job_id in turns table
            manager_id: None,
            sequence: 1,
            predecessor_id: None,
            state,
            yield_reason: None,
            start_reason: "initial",
            created_at: now,
            started_at: if state == "running" { Some(now) } else { None },
            ended_at: None,
            updated_at: now,
        };
        diesel::insert_into(turns::table)
            .values(&new_turn)
            .execute(conn)
            .unwrap();
    }

    fn insert_run(conn: &mut SqliteConnection, run_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let _ = diesel::insert_into(crate::schema::runs::table)
            .values(&crate::diesel_models::NewRun {
                id: run_id,
                issue_id: None,
                project_id: None,
                job_id: None,
                chat_id: None,
                status: Some("pending"),
                session_id: None,
                error_message: None,
                started_at: None,
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: None,
            })
            .execute(conn);
    }

    // ---- start_turn tests ----

    #[test]
    fn test_start_turn_from_pending() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "pending");
        insert_run(&mut conn, "run-1");

        start_turn(&mut conn, "turn-1", "run-1", &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "running");

        let run_id: Option<String> = turns::table
            .find("turn-1")
            .select(turns::run_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(run_id, Some("run-1".to_string()));

        let started_at: Option<i32> = turns::table
            .find("turn-1")
            .select(turns::started_at)
            .first(&mut conn)
            .unwrap();
        assert!(started_at.is_some());
    }

    #[test]
    fn test_start_turn_from_running_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        let result = start_turn(&mut conn, "turn-1", "run-1", &mock_emitter());
        assert!(result.is_err());
    }

    // ---- apply_turn_outcome tests ----

    #[test]
    fn test_complete_running_turn() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "complete");

        let ended_at: Option<i32> = turns::table
            .find("turn-1")
            .select(turns::ended_at)
            .first(&mut conn)
            .unwrap();
        assert!(ended_at.is_some());
    }

    #[test]
    fn test_fail_running_turn() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        apply_turn_outcome(&mut conn, "turn-1", TurnState::Failed, &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "failed");
    }

    #[test]
    fn test_cancel_running_turn() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        apply_turn_outcome(&mut conn, "turn-1", TurnState::Cancelled, &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "cancelled");
    }

    #[test]
    fn test_complete_pending_turn_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "pending");

        let result = apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter());
        assert!(result.is_err());
    }

    #[test]
    fn test_fail_pending_turn_succeeds() {
        // Process crashed before start_turn — Pending → Failed is valid
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "pending");

        apply_turn_outcome(&mut conn, "turn-1", TurnState::Failed, &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "failed");
    }

    #[test]
    fn test_complete_yielded_turn_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "yielded");

        let result = apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter());
        assert!(result.is_err());
    }

    #[test]
    fn test_outcome_with_invalid_state_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        let result = apply_turn_outcome(&mut conn, "turn-1", TurnState::Running, &mock_emitter());
        assert!(result.is_err());
    }

    // ---- yield_turn tests ----

    #[test]
    fn test_yield_running_turn_user_input() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        yield_turn(
            &mut conn,
            "turn-1",
            TurnYieldReason::UserInput,
            &mock_emitter(),
        )
        .unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "yielded");

        let reason: Option<String> = turns::table
            .find("turn-1")
            .select(turns::yield_reason)
            .first(&mut conn)
            .unwrap();
        assert_eq!(reason, Some("user_input".to_string()));
    }

    #[test]
    fn test_yield_running_turn_permission() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        yield_turn(
            &mut conn,
            "turn-1",
            TurnYieldReason::Permission,
            &mock_emitter(),
        )
        .unwrap();

        let reason: Option<String> = turns::table
            .find("turn-1")
            .select(turns::yield_reason)
            .first(&mut conn)
            .unwrap();
        assert_eq!(reason, Some("permission".to_string()));
    }

    #[test]
    fn test_yield_pending_turn_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "pending");

        let result = yield_turn(
            &mut conn,
            "turn-1",
            TurnYieldReason::UserInput,
            &mock_emitter(),
        );
        assert!(result.is_err());
    }

    // ---- interrupt_turn tests ----

    #[test]
    fn test_interrupt_running_turn() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        interrupt_turn(&mut conn, "turn-1", &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "interrupted");

        let ended_at: Option<i32> = turns::table
            .find("turn-1")
            .select(turns::ended_at)
            .first(&mut conn)
            .unwrap();
        assert!(ended_at.is_some());
    }

    #[test]
    fn test_interrupt_pending_turn_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "pending");

        let result = interrupt_turn(&mut conn, "turn-1", &mock_emitter());
        assert!(result.is_err());
    }

    #[test]
    fn test_interrupt_complete_turn_fails() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "complete");

        let result = interrupt_turn(&mut conn, "turn-1", &mock_emitter());
        assert!(result.is_err());
    }

    // ---- turn not found tests ----

    #[test]
    fn test_start_nonexistent_turn_fails() {
        let mut conn = test_diesel_conn();
        let result = start_turn(&mut conn, "nonexistent", "run-1", &mock_emitter());
        assert!(result.is_err());
    }

    #[test]
    fn test_outcome_nonexistent_turn_fails() {
        let mut conn = test_diesel_conn();
        let result = apply_turn_outcome(
            &mut conn,
            "nonexistent",
            TurnState::Complete,
            &mock_emitter(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_yield_nonexistent_turn_fails() {
        let mut conn = test_diesel_conn();
        let result = yield_turn(
            &mut conn,
            "nonexistent",
            TurnYieldReason::UserInput,
            &mock_emitter(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_interrupt_nonexistent_turn_fails() {
        let mut conn = test_diesel_conn();
        let result = interrupt_turn(&mut conn, "nonexistent", &mock_emitter());
        assert!(result.is_err());
    }

    // ---- db-change emission tests ----

    #[test]
    fn test_transitions_emit_db_change() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "pending");
        insert_run(&mut conn, "run-1");
        let emitter = mock_emitter();

        start_turn(&mut conn, "turn-1", "run-1", &emitter).unwrap();

        let db_changes = emitter.events_named("db-change");
        let tables: Vec<String> = db_changes
            .iter()
            .filter_map(|p| p.get("table").and_then(|v| v.as_str()).map(String::from))
            .collect();
        assert!(tables.contains(&"turns".to_string()));
    }

    // ---- idempotency tests ----

    #[test]
    fn test_apply_outcome_idempotent_complete() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        // First complete succeeds
        apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter()).unwrap();

        // Second complete is idempotent no-op
        apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "complete");
    }

    #[test]
    fn test_apply_outcome_idempotent_failed() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        apply_turn_outcome(&mut conn, "turn-1", TurnState::Failed, &mock_emitter()).unwrap();
        // Second call is no-op
        apply_turn_outcome(&mut conn, "turn-1", TurnState::Failed, &mock_emitter()).unwrap();
    }

    #[test]
    fn test_interrupt_idempotent() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        interrupt_turn(&mut conn, "turn-1", &mock_emitter()).unwrap();
        // Second interrupt is no-op
        interrupt_turn(&mut conn, "turn-1", &mock_emitter()).unwrap();

        let state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "interrupted");
    }

    #[test]
    fn test_complete_then_fail_rejected() {
        // A completed turn cannot be failed — different terminal state
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter()).unwrap();
        let result = apply_turn_outcome(&mut conn, "turn-1", TurnState::Failed, &mock_emitter());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already terminal"));
    }

    #[test]
    fn test_interrupted_then_complete_rejected() {
        let mut conn = test_diesel_conn();
        insert_turn(&mut conn, "turn-1", "running");

        interrupt_turn(&mut conn, "turn-1", &mock_emitter()).unwrap();
        let result = apply_turn_outcome(&mut conn, "turn-1", TurnState::Complete, &mock_emitter());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already terminal"));
    }

    // ---- duplicate successor prevention ----

    #[test]
    fn test_duplicate_successor_rejected() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1");

        // Create first turn, yield it
        insert_turn(&mut conn, "turn-1", "running");
        yield_turn(
            &mut conn,
            "turn-1",
            TurnYieldReason::UserInput,
            &mock_emitter(),
        )
        .unwrap();

        // Create first successor — should work
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(turns::table)
            .values(&NewTurn {
                id: "turn-2",
                session_id: "session-1",
                run_id: Some("run-1"),
                job_id: None,
                manager_id: None,
                sequence: 2,
                predecessor_id: Some("turn-1"),
                state: "pending",
                yield_reason: None,
                start_reason: "prompt_response",
                created_at: now,
                started_at: None,
                ended_at: None,
                updated_at: now,
            })
            .execute(&mut conn)
            .unwrap();

        // Try to create second successor with same predecessor — should fail
        let result = crate::turns::queries::create_successor_turn(
            &mut conn,
            "turn-3",
            "session-1",
            "job-1",
            "turn-1",
            crate::models::TurnStartReason::PromptResponse,
            &mock_emitter(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already has a successor"));
    }
}
