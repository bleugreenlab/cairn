//! Run state transitions.

use crate::models::RunStatus;
use crate::services::EventEmitter;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;

use super::TransitionError;

/// Validate and execute a run status transition.
pub async fn transition_run(
    db: &LocalDb,
    run_id: &str,
    to: RunStatus,
    emitter: &dyn EventEmitter,
) -> Result<RunStatus, TransitionError> {
    let current_str = load_run_status(db, run_id)
        .await
        .map_err(|error| TransitionError {
            entity: "run",
            id: run_id.to_string(),
            from: "unknown".to_string(),
            to: to.to_string(),
            reason: error,
        })?;

    let from: RunStatus = current_str.parse().map_err(|_| TransitionError {
        entity: "run",
        id: run_id.to_string(),
        from: current_str.clone(),
        to: to.to_string(),
        reason: format!("unparseable current status: {}", current_str),
    })?;

    validate_run_transition(&from, &to, run_id)?;

    let run_id_owned = run_id.to_string();
    db.write(|conn| {
        let run_id = run_id_owned.clone();
        let to = to.clone();
        let from = from.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            match to {
                RunStatus::Live => {
                    conn.execute(
                        "UPDATE runs SET status = ?1, started_at = ?2, updated_at = ?3 WHERE id = ?4",
                        params![to.to_string(), now, now, run_id.as_str()],
                    )
                    .await?;
                }
                RunStatus::Exited | RunStatus::Crashed => {
                    conn.execute(
                        "UPDATE runs SET status = ?1, exited_at = ?2, updated_at = ?3 WHERE id = ?4",
                        params![to.to_string(), now, now, run_id.as_str()],
                    )
                    .await?;
                }
                _ => {
                    conn.execute(
                        "UPDATE runs SET status = ?1, updated_at = ?2 WHERE id = ?3",
                        params![to.to_string(), now, run_id.as_str()],
                    )
                    .await?;
                }
            }
            Ok(from)
        })
    })
    .await
    .map_err(|error| TransitionError {
        entity: "run",
        id: run_id.to_string(),
        from: current_str,
        to: to.to_string(),
        reason: format!("DB error: {error}"),
    })?;

    let job_id = crate::messages::side_channel::job_id_for_run(db, run_id).await;
    let _ = emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("update", run_id, job_id.as_deref()),
    );

    Ok(from)
}

/// Set exit_reason on a run.
pub async fn set_exit_reason(db: &LocalDb, run_id: &str, reason: &str) -> Result<(), String> {
    let run_id = run_id.to_string();
    let reason = reason.to_string();
    db.write(|conn| {
        let run_id = run_id.clone();
        let reason = reason.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            conn.execute(
                "UPDATE runs SET exit_reason = ?1, updated_at = ?2 WHERE id = ?3",
                params![reason.as_str(), now, run_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to set exit_reason on run: {e}"))
}

async fn load_run_status(db: &LocalDb, run_id: &str) -> Result<String, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM runs WHERE id = ?1 LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::internal(format!("run not found: {run_id}")))?;
            row.opt_text(0)?
                .ok_or_else(|| DbError::internal(format!("run has no status: {run_id}")))
        })
    })
    .await
    .map_err(|e| e.to_string())
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
