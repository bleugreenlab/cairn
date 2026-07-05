//! Session state transitions.

use crate::models::SessionStatus;
use crate::services::EventEmitter;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;

use super::TransitionError;

/// Validate and execute a session status transition.
pub async fn transition_session(
    db: &LocalDb,
    session_id: &str,
    to: SessionStatus,
    reason: Option<&str>,
    emitter: &dyn EventEmitter,
) -> Result<SessionStatus, TransitionError> {
    let current = load_session_status(db, session_id)
        .await
        .map_err(|error| TransitionError {
            entity: "session",
            id: session_id.to_string(),
            from: "unknown".to_string(),
            to: to.to_string(),
            reason: error,
        })?;

    let from: SessionStatus = current.parse().map_err(|error: String| TransitionError {
        entity: "session",
        id: session_id.to_string(),
        from: current.clone(),
        to: to.to_string(),
        reason: error,
    })?;

    validate_session_transition(&from, &to, session_id)?;

    let session_id = session_id.to_string();
    let reason = reason.map(str::to_string);
    db.write(|conn| {
        let session_id = session_id.clone();
        let reason = reason.clone();
        let to = to.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let closed_at = if to == SessionStatus::Open {
                None
            } else {
                Some(now)
            };
            conn.execute(
                "UPDATE sessions
                 SET status = ?1, terminal_reason = ?2, closed_at = ?3, updated_at = ?4
                 WHERE id = ?5",
                params![
                    to.to_string(),
                    reason.as_deref(),
                    closed_at,
                    now,
                    session_id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| TransitionError {
        entity: "session",
        id: session_id.to_string(),
        from: from.to_string(),
        to: to.to_string(),
        reason: format!("DB error: {error}"),
    })?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "sessions", "action": "update"}),
    );

    Ok(from)
}

async fn load_session_status(db: &LocalDb, session_id: &str) -> Result<String, String> {
    let session_id = session_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM sessions WHERE id = ?1 LIMIT 1",
                    (session_id.as_str(),),
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::internal(format!("session not found: {session_id}")))?;
            row.text(0)
        })
    })
    .await
    .map_err(|e| e.to_string())
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
