//! Run state transitions.
//!
//! Valid transitions (process lifecycle only):
//! - Starting → Live (process connected, producing events)
//! - Starting → Exited (spawn succeeded but process exited immediately)
//! - Starting → Crashed (spawn failed or process died before becoming live)
//! - Live → Exited (clean shutdown)
//! - Live → Crashed (abnormal exit, signal, unexpected EOF)

use crate::models::RunStatus;
use crate::schema::runs;
use crate::services::EventEmitter;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::TransitionError;

/// Validate and execute a run status transition.
///
/// After transitioning, emits a `db-change` event for the `runs` table.
/// Returns the previous status on success.
pub fn transition_run(
    conn: &mut SqliteConnection,
    run_id: &str,
    to: RunStatus,
    emitter: &dyn EventEmitter,
) -> Result<RunStatus, TransitionError> {
    let current_str: String = runs::table
        .find(run_id)
        .select(runs::status.assume_not_null())
        .first(conn)
        .map_err(|_| TransitionError {
            entity: "run",
            id: run_id.to_string(),
            from: "unknown".to_string(),
            to: to.to_string(),
            reason: "run not found".to_string(),
        })?;

    let from: RunStatus = current_str.parse().map_err(|_| TransitionError {
        entity: "run",
        id: run_id.to_string(),
        from: current_str.clone(),
        to: to.to_string(),
        reason: format!("unparseable current status: {}", current_str),
    })?;

    validate_run_transition(&from, &to, run_id)?;

    let now = chrono::Utc::now().timestamp() as i32;

    // Build the update based on target state
    match &to {
        RunStatus::Live => {
            diesel::update(runs::table.find(run_id))
                .set((
                    runs::status.eq(to.to_string()),
                    runs::started_at.eq(Some(now)),
                    runs::updated_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| TransitionError {
                    entity: "run",
                    id: run_id.to_string(),
                    from: from.to_string(),
                    to: to.to_string(),
                    reason: format!("DB error: {}", e),
                })?;
        }
        RunStatus::Exited | RunStatus::Crashed => {
            diesel::update(runs::table.find(run_id))
                .set((
                    runs::status.eq(to.to_string()),
                    runs::exited_at.eq(Some(now)),
                    runs::updated_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| TransitionError {
                    entity: "run",
                    id: run_id.to_string(),
                    from: from.to_string(),
                    to: to.to_string(),
                    reason: format!("DB error: {}", e),
                })?;
        }
        _ => {
            diesel::update(runs::table.find(run_id))
                .set((runs::status.eq(to.to_string()), runs::updated_at.eq(now)))
                .execute(conn)
                .map_err(|e| TransitionError {
                    entity: "run",
                    id: run_id.to_string(),
                    from: from.to_string(),
                    to: to.to_string(),
                    reason: format!("DB error: {}", e),
                })?;
        }
    }

    // Emit db-change event for runs table
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "update"}),
    );

    Ok(from)
}

/// Set exit_reason on a run (does NOT change status — call transition_run separately).
pub fn set_exit_reason(
    conn: &mut SqliteConnection,
    run_id: &str,
    reason: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::update(runs::table.find(run_id))
        .set((runs::exit_reason.eq(Some(reason)), runs::updated_at.eq(now)))
        .execute(conn)
        .map_err(|e| format!("Failed to set exit_reason on run {}: {}", run_id, e))?;
    Ok(())
}

fn validate_run_transition(
    from: &RunStatus,
    to: &RunStatus,
    run_id: &str,
) -> Result<(), TransitionError> {
    let valid = matches!(
        (from, to),
        (RunStatus::Starting, RunStatus::Live)
            | (RunStatus::Starting, RunStatus::Exited)
            | (RunStatus::Starting, RunStatus::Crashed)
            | (RunStatus::Live, RunStatus::Exited)
            | (RunStatus::Live, RunStatus::Crashed)
    );

    if !valid {
        return Err(TransitionError {
            entity: "run",
            id: run_id.to_string(),
            from: from.to_string(),
            to: to.to_string(),
            reason: "transition not allowed".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewRun;
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::test_diesel_conn;

    fn mock_emitter() -> CapturingEmitter {
        CapturingEmitter::new()
    }

    fn insert_run(conn: &mut SqliteConnection, id: &str, status: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id,
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some(status),
            session_id: None,
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_starting_to_live() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");

        let prev = transition_run(&mut conn, "run-1", RunStatus::Live, &mock_emitter()).unwrap();
        assert_eq!(prev, RunStatus::Starting);

        let status: String = runs::table
            .find("run-1")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "live");
    }

    #[test]
    fn test_live_to_exited() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        transition_run(&mut conn, "run-1", RunStatus::Exited, &mock_emitter()).unwrap();

        let status: String = runs::table
            .find("run-1")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "exited");

        // Should have exited_at set
        let exited_at: Option<i32> = runs::table
            .find("run-1")
            .select(runs::exited_at)
            .first(&mut conn)
            .unwrap();
        assert!(exited_at.is_some());
    }

    #[test]
    fn test_live_to_crashed() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        transition_run(&mut conn, "run-1", RunStatus::Crashed, &mock_emitter()).unwrap();

        let status: String = runs::table
            .find("run-1")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "crashed");
    }

    #[test]
    fn test_starting_to_crashed() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");

        transition_run(&mut conn, "run-1", RunStatus::Crashed, &mock_emitter()).unwrap();
    }

    #[test]
    fn test_starting_to_exited() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");

        transition_run(&mut conn, "run-1", RunStatus::Exited, &mock_emitter()).unwrap();
    }

    #[test]
    fn test_invalid_transition_exited_to_live() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "exited");

        let result = transition_run(&mut conn, "run-1", RunStatus::Live, &mock_emitter());
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_transition_crashed_to_live() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "crashed");

        let result = transition_run(&mut conn, "run-1", RunStatus::Live, &mock_emitter());
        assert!(result.is_err());
    }

    #[test]
    fn test_not_found() {
        let mut conn = test_diesel_conn();

        let result = transition_run(&mut conn, "nonexistent", RunStatus::Live, &mock_emitter());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("run not found"));
    }

    #[test]
    fn test_starting_to_live_sets_started_at() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");

        transition_run(&mut conn, "run-1", RunStatus::Live, &mock_emitter()).unwrap();

        let started_at: Option<i32> = runs::table
            .find("run-1")
            .select(runs::started_at)
            .first(&mut conn)
            .unwrap();
        assert!(
            started_at.is_some(),
            "started_at should be set on Live transition"
        );
    }

    #[test]
    fn test_live_to_crashed_sets_exited_at() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        transition_run(&mut conn, "run-1", RunStatus::Crashed, &mock_emitter()).unwrap();

        let exited_at: Option<i32> = runs::table
            .find("run-1")
            .select(runs::exited_at)
            .first(&mut conn)
            .unwrap();
        assert!(
            exited_at.is_some(),
            "exited_at should be set on Crashed transition"
        );
    }

    // Backwards compatibility: old status strings parse correctly
    #[test]
    fn test_backwards_compat_paused_parses_as_live() {
        let status: RunStatus = "paused".parse().unwrap();
        assert_eq!(status, RunStatus::Live);
    }

    #[test]
    fn test_backwards_compat_completed_parses_as_exited() {
        let status: RunStatus = "completed".parse().unwrap();
        assert_eq!(status, RunStatus::Exited);
    }

    #[test]
    fn test_emits_db_change_events() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");
        let emitter = mock_emitter();

        transition_run(&mut conn, "run-1", RunStatus::Live, &emitter).unwrap();

        let db_changes = emitter.events_named("db-change");
        let tables: Vec<String> = db_changes
            .iter()
            .filter_map(|payload| {
                payload
                    .get("table")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .collect();
        assert!(
            tables.contains(&"runs".to_string()),
            "should emit runs db-change"
        );
    }

    #[test]
    fn test_set_exit_reason() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        set_exit_reason(&mut conn, "run-1", "user_stop").unwrap();

        let reason: Option<String> = runs::table
            .find("run-1")
            .select(runs::exit_reason)
            .first(&mut conn)
            .unwrap();
        assert_eq!(reason.as_deref(), Some("user_stop"));
    }

    #[test]
    fn test_set_exit_reason_overwrite() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        set_exit_reason(&mut conn, "run-1", "timeout").unwrap();
        set_exit_reason(&mut conn, "run-1", "crash").unwrap();

        let reason: Option<String> = runs::table
            .find("run-1")
            .select(runs::exit_reason)
            .first(&mut conn)
            .unwrap();
        assert_eq!(reason.as_deref(), Some("crash"));
    }

    #[test]
    fn test_set_exit_reason_nonexistent_run() {
        let mut conn = test_diesel_conn();
        // Should not panic — the update affects 0 rows but returns Ok
        // (Diesel update on missing row is not an error, it just updates 0 rows)
        let result = set_exit_reason(&mut conn, "nonexistent", "crash");
        assert!(result.is_ok());
    }
}
