//! Session state transitions.
//!
//! Valid transitions:
//! - Open → Closed (intentional: user closes issue, ends chat)
//! - Open → Failed (positive evidence: backend rejected session token)
//!
//! No reopening — Closed and Failed are terminal.

use crate::models::SessionStatus;
use crate::services::EventEmitter;
use diesel::sqlite::SqliteConnection;

use super::TransitionError;

/// Validate and execute a session status transition.
///
/// Returns the previous status on success.
pub fn transition_session(
    conn: &mut SqliteConnection,
    session_id: &str,
    to: SessionStatus,
    reason: Option<&str>,
    emitter: &dyn EventEmitter,
) -> Result<SessionStatus, TransitionError> {
    // Get current status
    let session = crate::sessions::queries::get(conn, session_id).map_err(|_| TransitionError {
        entity: "session",
        id: session_id.to_string(),
        from: "unknown".to_string(),
        to: to.to_string(),
        reason: "session not found".to_string(),
    })?;

    let from = session.status;
    validate_session_transition(&from, &to, session_id)?;

    // Execute the status update
    crate::sessions::queries::update_status(conn, session_id, to.clone(), reason).map_err(|e| {
        TransitionError {
            entity: "session",
            id: session_id.to_string(),
            from: from.to_string(),
            to: to.to_string(),
            reason: format!("DB error: {}", e),
        }
    })?;

    // Emit db-change event
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "sessions", "action": "update"}),
    );

    Ok(from)
}

fn validate_session_transition(
    from: &SessionStatus,
    to: &SessionStatus,
    session_id: &str,
) -> Result<(), TransitionError> {
    let valid = matches!(
        (from, to),
        (SessionStatus::Open, SessionStatus::Closed) | (SessionStatus::Open, SessionStatus::Failed)
    );

    if !valid {
        return Err(TransitionError {
            entity: "session",
            id: session_id.to_string(),
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
    use crate::diesel_models::NewSession;
    use crate::schema::sessions;
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::test_diesel_conn;
    use diesel::prelude::*;

    fn mock_emitter() -> CapturingEmitter {
        CapturingEmitter::new()
    }

    fn insert_session(conn: &mut SqliteConnection, id: &str, status: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        // Need a job for the CHECK constraint — create a minimal one
        let project_id = crate::test_utils::create_test_project(conn, "Test", "TST");
        let job_id = uuid::Uuid::new_v4().to_string();
        let new_job = crate::diesel_models::NewJob {
            id: &job_id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "pending",
            agent_config_id: None,
            issue_id: None,
            project_id: &project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: None,
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(crate::schema::jobs::table)
            .values(&new_job)
            .execute(conn)
            .unwrap();

        let new_session = NewSession {
            id,
            job_id: Some(&job_id),
            chat_id: None,
            backend: "claude",
            status,
            parent_session_id: None,
            replaced_by_id: None,
            terminal_reason: None,
            sequence: 1,
            created_at: now,
            closed_at: None,
            updated_at: now,
            backend_id: None,
        };
        diesel::insert_into(sessions::table)
            .values(&new_session)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_open_to_closed() {
        let mut conn = test_diesel_conn();
        insert_session(&mut conn, "sess-1", "open");

        let prev = transition_session(
            &mut conn,
            "sess-1",
            SessionStatus::Closed,
            Some("issue_closed"),
            &mock_emitter(),
        )
        .unwrap();
        assert_eq!(prev, SessionStatus::Open);

        let session = crate::sessions::queries::get(&mut conn, "sess-1").unwrap();
        assert_eq!(session.status, SessionStatus::Closed);
        assert_eq!(session.terminal_reason.as_deref(), Some("issue_closed"));
        assert!(session.closed_at.is_some());
    }

    #[test]
    fn test_open_to_failed() {
        let mut conn = test_diesel_conn();
        insert_session(&mut conn, "sess-1", "open");

        let prev = transition_session(
            &mut conn,
            "sess-1",
            SessionStatus::Failed,
            Some("backend rejected session"),
            &mock_emitter(),
        )
        .unwrap();
        assert_eq!(prev, SessionStatus::Open);
    }

    #[test]
    fn test_closed_to_open_rejected() {
        let mut conn = test_diesel_conn();
        insert_session(&mut conn, "sess-1", "closed");

        let result = transition_session(
            &mut conn,
            "sess-1",
            SessionStatus::Open,
            None,
            &mock_emitter(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("transition not allowed"));
    }

    #[test]
    fn test_failed_to_open_rejected() {
        let mut conn = test_diesel_conn();
        insert_session(&mut conn, "sess-1", "failed");

        let result = transition_session(
            &mut conn,
            "sess-1",
            SessionStatus::Open,
            None,
            &mock_emitter(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_not_found() {
        let mut conn = test_diesel_conn();

        let result = transition_session(
            &mut conn,
            "nonexistent",
            SessionStatus::Closed,
            None,
            &mock_emitter(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("session not found"));
    }

    #[test]
    fn test_emits_db_change() {
        let mut conn = test_diesel_conn();
        insert_session(&mut conn, "sess-1", "open");
        let emitter = mock_emitter();

        transition_session(&mut conn, "sess-1", SessionStatus::Closed, None, &emitter).unwrap();

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
        assert!(tables.contains(&"sessions".to_string()));
    }
}
