//! Shared lifecycle primitives: turn/run state transitions, id-keyed lookups,
//! and small db-change helpers. Sliced verbatim from the former single-file
//! `lifecycle.rs`; the module overview lives on the `lifecycle` facade.

use crate::models::{RunStatus, TurnEndReason, TurnState};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError};

fn emit_db_change(orch: &Orchestrator, table: &str, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": table, "action": action}),
    );
}

fn transition_error(
    entity: &'static str,
    id: &str,
    from: impl Into<String>,
    to: impl Into<String>,
    reason: impl Into<String>,
) -> String {
    crate::transitions::TransitionError {
        entity,
        id: id.to_string(),
        from: from.into(),
        to: to.into(),
        reason: reason.into(),
    }
    .to_string()
}

fn db_internal(message: impl Into<String>) -> DbError {
    DbError::internal(message.into())
}

pub(super) fn apply_turn_outcome(
    orch: &Orchestrator,
    turn_id: &str,
    outcome: TurnState,
    end_reason: Option<TurnEndReason>,
) -> Result<(), String> {
    if !matches!(
        outcome,
        TurnState::Complete | TurnState::Failed | TurnState::Cancelled
    ) {
        return Err(transition_error(
            "turn",
            turn_id,
            "unknown",
            outcome.to_string(),
            "apply_turn_outcome only accepts Complete, Failed, or Cancelled",
        ));
    }

    let turn_id = turn_id.to_string();
    let outcome_str = outcome.to_string();
    let end_reason = end_reason.map(|reason| reason.to_string());
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_turn(&dbs, &turn_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let turn_id = turn_id.clone();
                let outcome_str = outcome_str.clone();
                let end_reason = end_reason.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT state
                         FROM turns
                         WHERE id = ?1",
                            (turn_id.as_str(),),
                        )
                        .await?;
                    let current_str = crate::storage::next_text(&mut rows, 0).await?;
                    let Some(current_str) = current_str else {
                        return Err(db_internal(transition_error(
                            "turn",
                            &turn_id,
                            "unknown",
                            outcome_str,
                            "turn not found",
                        )));
                    };

                    let from: TurnState = current_str.parse().map_err(|_| {
                        db_internal(transition_error(
                            "turn",
                            &turn_id,
                            current_str.clone(),
                            outcome_str.clone(),
                            format!("unparseable current state: {}", current_str),
                        ))
                    })?;

                    let outcome: TurnState = outcome_str.parse().map_err(db_internal)?;
                    if from == outcome {
                        return Ok(());
                    }
                    if from.is_terminal() {
                        return Err(db_internal(transition_error(
                            "turn",
                            &turn_id,
                            from.to_string(),
                            outcome.to_string(),
                            "turn already terminal",
                        )));
                    }

                    match (&from, &outcome) {
                        (TurnState::Running, _) => {}
                        (TurnState::Pending, TurnState::Failed | TurnState::Cancelled) => {}
                        _ => {
                            return Err(db_internal(transition_error(
                                "turn",
                                &turn_id,
                                from.to_string(),
                                outcome.to_string(),
                                format!("invalid transition: {:?} -> {:?}", from, outcome),
                            )));
                        }
                    }

                    let now = chrono::Utc::now().timestamp();
                    conn.execute(
                        "UPDATE turns
                     SET state = ?1,
                         end_reason = ?2,
                         ended_at = ?3,
                         updated_at = ?3
                     WHERE id = ?4",
                        (
                            outcome.to_string().as_str(),
                            end_reason.as_deref(),
                            now,
                            turn_id.as_str(),
                        ),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    emit_db_change(orch, "turns", "update");
    Ok(())
}

pub(super) fn interrupt_turn(
    orch: &Orchestrator,
    turn_id: &str,
    end_reason: Option<TurnEndReason>,
) -> Result<(), String> {
    let turn_id = turn_id.to_string();
    let end_reason = end_reason.map(|reason| reason.to_string());
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_turn(&dbs, &turn_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let turn_id = turn_id.clone();
                let end_reason = end_reason.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT state
                         FROM turns
                         WHERE id = ?1",
                            (turn_id.as_str(),),
                        )
                        .await?;
                    let current_str = crate::storage::next_text(&mut rows, 0).await?;
                    let Some(current_str) = current_str else {
                        return Err(db_internal(transition_error(
                            "turn",
                            &turn_id,
                            "unknown",
                            "interrupted",
                            "turn not found",
                        )));
                    };

                    let from: TurnState = current_str.parse().map_err(|_| {
                        db_internal(transition_error(
                            "turn",
                            &turn_id,
                            current_str.clone(),
                            "interrupted",
                            format!("unparseable current state: {}", current_str),
                        ))
                    })?;

                    if from == TurnState::Interrupted {
                        return Ok(());
                    }
                    if from.is_terminal() {
                        return Err(db_internal(transition_error(
                            "turn",
                            &turn_id,
                            from.to_string(),
                            "interrupted",
                            "turn already terminal",
                        )));
                    }
                    if from != TurnState::Running {
                        return Err(db_internal(transition_error(
                            "turn",
                            &turn_id,
                            from.to_string(),
                            "interrupted",
                            "can only interrupt a running turn",
                        )));
                    }

                    let now = chrono::Utc::now().timestamp();
                    conn.execute(
                        "UPDATE turns
                     SET state = 'interrupted',
                         end_reason = ?1,
                         ended_at = ?2,
                         updated_at = ?2
                     WHERE id = ?3",
                        (end_reason.as_deref(), now, turn_id.as_str()),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    emit_db_change(orch, "turns", "update");
    Ok(())
}

pub(super) fn transition_run(
    orch: &Orchestrator,
    run_id: &str,
    to: RunStatus,
) -> Result<RunStatus, String> {
    let run_id = run_id.to_string();
    let emit_run_id = run_id.clone();
    let to_str = to.to_string();
    let from = run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let run_id = run_id.clone();
                let to_str = to_str.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status
                         FROM runs
                         WHERE id = ?1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let current_str = crate::storage::next_opt_text(&mut rows, 0)
                        .await?
                        .ok_or_else(|| {
                            db_internal(transition_error(
                                "run",
                                &run_id,
                                "unknown",
                                to_str.clone(),
                                "run not found",
                            ))
                        })?;

                    let from: RunStatus = current_str.parse().map_err(|_| {
                        db_internal(transition_error(
                            "run",
                            &run_id,
                            current_str.clone(),
                            to_str.clone(),
                            format!("unparseable current status: {}", current_str),
                        ))
                    })?;
                    let to: RunStatus = to_str.parse().map_err(db_internal)?;
                    let valid = matches!(
                        (&from, &to),
                        (RunStatus::Starting, RunStatus::Live)
                            | (RunStatus::Starting, RunStatus::Exited)
                            | (RunStatus::Starting, RunStatus::Crashed)
                            | (RunStatus::Live, RunStatus::Exited)
                            | (RunStatus::Live, RunStatus::Crashed)
                    );
                    if !valid {
                        return Err(db_internal(transition_error(
                            "run",
                            &run_id,
                            from.to_string(),
                            to.to_string(),
                            "transition not allowed",
                        )));
                    }

                    let now = chrono::Utc::now().timestamp();
                    match to {
                        RunStatus::Live => {
                            conn.execute(
                                "UPDATE runs
                             SET status = ?1,
                                 started_at = ?2,
                                 updated_at = ?2
                             WHERE id = ?3",
                                (to.to_string().as_str(), now, run_id.as_str()),
                            )
                            .await?;
                        }
                        RunStatus::Exited | RunStatus::Crashed => {
                            conn.execute(
                                "UPDATE runs
                             SET status = ?1,
                                 exited_at = ?2,
                                 updated_at = ?2
                             WHERE id = ?3",
                                (to.to_string().as_str(), now, run_id.as_str()),
                            )
                            .await?;
                        }
                        RunStatus::Starting => {
                            conn.execute(
                                "UPDATE runs
                             SET status = ?1,
                                 updated_at = ?2
                             WHERE id = ?3",
                                (to.to_string().as_str(), now, run_id.as_str()),
                            )
                            .await?;
                        }
                    }
                    Ok(from)
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    let change = run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = emit_run_id.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(crate::notify::run_db_change_for_id(&db, &run_id, "update").await)
        }
    })?;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(from)
}

pub(crate) fn set_exit_reason(
    orch: &Orchestrator,
    run_id: &str,
    reason: &str,
) -> Result<(), String> {
    let run_id = run_id.to_string();
    let reason = reason.to_string();
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let run_id = run_id.clone();
                let reason = reason.clone();
                Box::pin(async move {
                    let now = chrono::Utc::now().timestamp();
                    conn.execute(
                        "UPDATE runs
                     SET exit_reason = ?1,
                         updated_at = ?2
                     WHERE id = ?3",
                        (reason.as_str(), now, run_id.as_str()),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })
}

pub(super) fn finalize_todos(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let job_id = job_id.to_string();
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())?;
            crate::todos::finalize_todos(&db, &job_id).await
        }
    })
}

#[derive(Clone, Copy)]
pub(super) enum TextColumn {
    Required,
    Optional,
}

pub(super) fn blocking_text_lookup(
    orch: &Orchestrator,
    key: &str,
    query: &'static str,
    column: TextColumn,
) -> Option<String> {
    // Shared id-keyed read helper: the key is a self-routing run/turn/job id, so
    // parse its team prefix to the owning database in O(1) (CAIRN-2210) instead
    // of scanning every open database. A team id whose replica is not open routes
    // to an Err, which collapses to None below — exactly as the old scan returned
    // None when no open database carried the row.
    let dbs = orch.db.clone();
    let key = key.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::routing_db_for_id(&dbs, &key)
            .await
            .map_err(|e| e.to_string())?;
        db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn.query(query, (key.as_str(),)).await?;
                match column {
                    TextColumn::Required => crate::storage::next_text(&mut rows, 0).await,
                    TextColumn::Optional => crate::storage::next_opt_text(&mut rows, 0).await,
                }
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
}

pub(super) fn current_turn_id_for_run(orch: &Orchestrator, run_id: &str) -> Option<String> {
    blocking_text_lookup(
        orch,
        run_id,
        "SELECT jobs.current_turn_id
         FROM runs
         JOIN jobs ON runs.job_id = jobs.id
         WHERE runs.id = ?1
           AND runs.job_id IS NOT NULL",
        TextColumn::Optional,
    )
}

pub(super) fn turn_state(orch: &Orchestrator, turn_id: &str) -> Option<String> {
    blocking_text_lookup(
        orch,
        turn_id,
        "SELECT state
         FROM turns
         WHERE id = ?1",
        TextColumn::Required,
    )
}

pub(super) fn active_turn_id_for_run(orch: &Orchestrator, run_id: &str) -> Option<String> {
    orch.process_state
        .get_current_turn_id(run_id)
        .or_else(|| current_turn_id_for_run(orch, run_id))
}

pub(super) fn run_status(orch: &Orchestrator, run_id: &str) -> Option<String> {
    blocking_text_lookup(
        orch,
        run_id,
        "SELECT status
         FROM runs
         WHERE id = ?1",
        TextColumn::Optional,
    )
}

pub(super) fn job_id_for_run(orch: &Orchestrator, run_id: &str) -> Option<String> {
    blocking_text_lookup(
        orch,
        run_id,
        "SELECT job_id
         FROM runs
         WHERE id = ?1
           AND job_id IS NOT NULL",
        TextColumn::Optional,
    )
}
