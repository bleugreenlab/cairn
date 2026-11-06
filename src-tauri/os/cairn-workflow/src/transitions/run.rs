//! Run state transitions.

use crate::models::RunStatus;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_platform::services::emitter::EventEmitter;

use super::TransitionError;

/// Validate and execute a run status transition. Every run-status change goes
/// through here, so a side effect of settling a run belongs in this function.
///
/// The current status is read inside the same write transaction as the UPDATE.
/// A read in a separate transaction lets a concurrent transition land between
/// validation and write, so two settlers could each see `live` and both write;
/// inside the write, a retried conflict re-reads the settled status and is
/// refused as "transition not allowed".
pub async fn transition_run(
    db: &LocalDb,
    run_id: &str,
    to: RunStatus,
    emitter: &dyn EventEmitter,
) -> Result<RunStatus, TransitionError> {
    let refusal = |from: String, reason: String| TransitionError {
        entity: "run",
        id: run_id.to_string(),
        from,
        to: to.to_string(),
        reason,
    };
    let run_id_owned = run_id.to_string();
    let outcome = db
        .write(|conn| {
            let run_id = run_id_owned.clone();
            let to = to.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status FROM runs WHERE id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                let current_str = match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                };
                let Some(current_str) = current_str else {
                    return Ok(Err(("unknown".to_string(), "run not found".to_string())));
                };
                let Ok(from) = current_str.parse::<RunStatus>() else {
                    let reason = format!("unparseable current status: {current_str}");
                    return Ok(Err((current_str, reason)));
                };
                if !run_transition_allowed(&from, &to) {
                    return Ok(Err((
                        from.to_string(),
                        "transition not allowed".to_string(),
                    )));
                }

                let now = chrono::Utc::now().timestamp();
                let mut admission_withdrawn = false;
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
                        // A run that ends while its launch is still queued (stopped,
                        // killed, or failed to start) withdraws that queue entry in
                        // the same write, so the promoter never leases a dead launch,
                        // and returns the attention the launch's turn was to carry.
                        if let Some(withdrawn) =
                            cairn_db::storage::withdraw_queued_ordinary_admission(
                                conn,
                                &run_id,
                                &format!("run {to} before admission"),
                                now,
                            )
                            .await?
                        {
                            admission_withdrawn = true;
                            cairn_attention::push::return_withdrawn_delivery_conn(
                                conn, &withdrawn,
                            )
                            .await;
                        }
                    }
                    RunStatus::Starting => {
                        conn.execute(
                            "UPDATE runs SET status = ?1, updated_at = ?2 WHERE id = ?3",
                            params![to.to_string(), now, run_id.as_str()],
                        )
                        .await?;
                    }
                }
                Ok(Ok((from, admission_withdrawn)))
            })
        })
        .await
        .map_err(|error| refusal("unknown".to_string(), format!("DB error: {error}")))?;
    let (from, admission_withdrawn) = outcome.map_err(|(from, reason)| refusal(from, reason))?;

    // The withdrawal freed a queue position; wake the promoter only after the
    // write that made it has committed.
    if admission_withdrawn {
        db.provider_admission_changed();
    }
    let change = cairn_db::changes::run_db_change_for_id(db, run_id, "update").await;
    let _ = emitter.emit("db-change", change);

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

/// The run state machine: a run starts, may go live, and ends exactly once.
fn run_transition_allowed(from: &RunStatus, to: &RunStatus) -> bool {
    matches!(
        (from, to),
        (RunStatus::Starting, RunStatus::Live)
            | (RunStatus::Starting, RunStatus::Exited)
            | (RunStatus::Starting, RunStatus::Crashed)
            | (RunStatus::Live, RunStatus::Exited)
            | (RunStatus::Live, RunStatus::Crashed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_platform::services::emitter::CapturingEmitter;
    use std::sync::Arc;

    async fn db_with_run(name: &str, status: &str) -> LocalDb {
        let db = LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join(name))
            .await
            .unwrap();
        db.execute_script(format!(
            "INSERT INTO runs (id, status, created_at, updated_at) VALUES ('run-1', '{status}', 1, 1);"
        ))
        .await
        .unwrap();
        db
    }

    async fn status(db: &LocalDb) -> String {
        db.query_opt("SELECT status FROM runs WHERE id = 'run-1'", (), |row| {
            row.text(0)
        })
        .await
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn refusals_name_the_missing_run_the_bad_status_and_the_disallowed_edge() {
        let emitter = CapturingEmitter::new();
        let db = db_with_run("refusals.db", "exited").await;

        let missing = transition_run(&db, "nope", RunStatus::Exited, &emitter)
            .await
            .unwrap_err();
        assert_eq!(
            (missing.from.as_str(), missing.reason.as_str()),
            ("unknown", "run not found")
        );

        let terminal = transition_run(&db, "run-1", RunStatus::Crashed, &emitter)
            .await
            .unwrap_err();
        assert_eq!(
            (terminal.from.as_str(), terminal.reason.as_str()),
            ("exited", "transition not allowed")
        );
        assert_eq!(status(&db).await, "exited");

        db.execute("UPDATE runs SET status = 'bogus' WHERE id = 'run-1'", ())
            .await
            .unwrap();
        let unparseable = transition_run(&db, "run-1", RunStatus::Exited, &emitter)
            .await
            .unwrap_err();
        assert_eq!(unparseable.from, "bogus");
        assert!(unparseable.reason.starts_with("unparseable current status"));
    }

    /// Two settlers race on one live run. Because the status is read inside the
    /// write, exactly one ends the run and the other is refused; a read outside
    /// the write would let both see `live` and both write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_settlers_end_a_run_exactly_once() {
        for round in 0..8 {
            let db = Arc::new(db_with_run(&format!("race-{round}.db"), "live").await);
            let emitter = Arc::new(CapturingEmitter::new());
            let settle = |to: RunStatus| {
                let db = db.clone();
                let emitter = emitter.clone();
                tokio::spawn(async move { transition_run(&db, "run-1", to, &emitter).await })
            };
            let (a, b) = tokio::join!(settle(RunStatus::Exited), settle(RunStatus::Crashed));
            let results = [a.unwrap(), b.unwrap()];
            let ended: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
            assert_eq!(ended, vec![&RunStatus::Live], "round {round}: {results:?}");
            let refused = results.iter().find_map(|r| r.as_ref().err()).unwrap();
            assert_eq!(refused.reason, "transition not allowed");
            assert!(matches!(refused.from.as_str(), "exited" | "crashed"));
        }
    }
}
