//! Session lifecycle management
//!
//! This module handles cleanup, finalization, and stopping of backend sessions,
//! including cascading stops to child runs.
//!
//! ## Warm Process Retention
//!
//! When a turn completes successfully, processes can be transitioned to "warm" state
//! instead of being terminated. Warm processes:
//! - Keep their stdin open for follow-up messages
//! - Retain their MCP authentication token
//! - Preserve Claude's conversation cache
//! - Can be reused for continuation without spawning a new process

use crate::mcp::handlers::{emit_attention, AttentionEvent};
use crate::models::{Run, RunStatus, TurnState};
use crate::storage::{run_db_blocking, DbError, DbResult, RowExt};

use super::Orchestrator;

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

fn apply_turn_outcome(
    orch: &Orchestrator,
    turn_id: &str,
    outcome: TurnState,
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

    let db = orch.db.local.clone();
    let turn_id = turn_id.to_string();
    let outcome_str = outcome.to_string();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let turn_id = turn_id.clone();
            let outcome_str = outcome_str.clone();
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
                         ended_at = ?2,
                         updated_at = ?2
                     WHERE id = ?3",
                    (outcome.to_string().as_str(), now, turn_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    emit_db_change(orch, "turns", "update");
    Ok(())
}

fn interrupt_turn(orch: &Orchestrator, turn_id: &str) -> Result<(), String> {
    let db = orch.db.local.clone();
    let turn_id = turn_id.to_string();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let turn_id = turn_id.clone();
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
                         ended_at = ?1,
                         updated_at = ?1
                     WHERE id = ?2",
                    (now, turn_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    emit_db_change(orch, "turns", "update");
    Ok(())
}

fn transition_run(orch: &Orchestrator, run_id: &str, to: RunStatus) -> Result<RunStatus, String> {
    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    let emit_run_id = run_id.clone();
    let to_str = to.to_string();
    let from = run_db_blocking(move || async move {
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
    })?;
    let job_id = job_id_for_run(orch, &emit_run_id);
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("update", &emit_run_id, job_id.as_deref()),
    );
    Ok(from)
}

fn set_exit_reason(orch: &Orchestrator, run_id: &str, reason: &str) -> Result<(), String> {
    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    let reason = reason.to_string();
    run_db_blocking(move || async move {
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
    })
}

fn finalize_todos(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { crate::todos::finalize_todos(&db, &job_id).await })
}

#[derive(Clone, Copy)]
enum TextColumn {
    Required,
    Optional,
}

fn blocking_text_lookup(
    orch: &Orchestrator,
    key: &str,
    query: &'static str,
    column: TextColumn,
) -> Option<String> {
    let db = orch.db.local.clone();
    let key = key.to_string();
    run_db_blocking(move || async move {
        db.read(|conn| {
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

fn current_turn_id_for_run(orch: &Orchestrator, run_id: &str) -> Option<String> {
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

fn turn_state(orch: &Orchestrator, turn_id: &str) -> Option<String> {
    blocking_text_lookup(
        orch,
        turn_id,
        "SELECT state
         FROM turns
         WHERE id = ?1",
        TextColumn::Required,
    )
}

fn active_turn_id_for_run(orch: &Orchestrator, run_id: &str) -> Option<String> {
    orch.process_state
        .get_current_turn_id(run_id)
        .or_else(|| current_turn_id_for_run(orch, run_id))
}

pub fn stop_active_turn_for_run(orch: &Orchestrator, run_id: &str) {
    let Some(turn_id) = active_turn_id_for_run(orch, run_id) else {
        return;
    };

    let Some(state) = turn_state(orch, &turn_id) else {
        log::warn!("Run {} current turn {} was not found", run_id, turn_id);
        return;
    };

    let result = match state.as_str() {
        "running" => interrupt_turn(orch, &turn_id),
        "pending" => apply_turn_outcome(orch, &turn_id, TurnState::Cancelled),
        _ => Ok(()),
    };

    if let Err(error) = result {
        log::warn!(
            "Failed to stop turn {} for run {} from state {}: {}",
            turn_id,
            run_id,
            state,
            error
        );
    }
}

fn run_from_row(row: &turso::Row) -> DbResult<Run> {
    Ok(Run {
        id: row.text(0)?,
        issue_id: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        job_id: row.opt_text(3)?,
        status: row
            .opt_text(4)?
            .and_then(|status| status.parse().ok())
            .unwrap_or(RunStatus::Starting),
        session_id: row.opt_text(5)?,
        error_message: row.opt_text(6)?,
        started_at: row.opt_i64(7)?,
        exited_at: row.opt_i64(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        chat_id: row.opt_text(11)?,
        backend: row.opt_text(12)?,
        exit_reason: row.opt_text(13)?,
        start_mode: row.opt_text(14)?.and_then(|mode| mode.parse().ok()),
    })
}

fn run_for_sync(orch: &Orchestrator, run_id: &str) -> Option<Run> {
    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, issue_id, project_id, job_id, status, session_id,
                                error_message, started_at, exited_at, created_at, updated_at,
                                chat_id, backend, exit_reason, start_mode
                         FROM runs
                         WHERE id = ?1",
                        (run_id.as_str(),),
                    )
                    .await?;
                rows.next().await?.map(|row| run_from_row(&row)).transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
}

fn run_status(orch: &Orchestrator, run_id: &str) -> Option<String> {
    blocking_text_lookup(
        orch,
        run_id,
        "SELECT status
         FROM runs
         WHERE id = ?1",
        TextColumn::Optional,
    )
}

/// Synchronously read `(issue_id, IssueAttentionContext)` from a job_id.
/// Returns None when the job has no issue (project-level jobs).
fn issue_for_attention_by_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<(
    String,
    crate::orchestrator::attention::IssueAttentionContext,
)> {
    let db = orch.db.local.clone();
    let issue_id = blocking_text_lookup(
        orch,
        job_id,
        "SELECT issue_id FROM jobs WHERE id = ?1",
        TextColumn::Optional,
    )?;
    run_db_blocking(move || async move {
        let ctx = crate::orchestrator::attention::read_issue_for_attention(&db, &issue_id).await?;
        Ok(Some((issue_id, ctx)))
    })
    .ok()
    .flatten()
}

/// Emit the typed attention event for a turn that just terminalized.
/// - Issue reached a terminal status → `Resolved`
/// - Issue still needs the driver (attention != None) → `AgentIdleWithWork`
/// - Issue has an open PR work product while attention is None → `AgentIdleWithWork`
///   pointing at the producing builder's `/pr` (the freshly-opened-PR case the
///   attention projection deliberately leaves silent)
/// - Otherwise: no emit (the turn ended cleanly with no work left).
///
/// Returns `true` when an actionable fact was emitted, `false` when the turn
/// ended with nothing for the driver to act on. The warm-transition caller uses
/// this to gate the desktop "completed" toast so it fires only on a real
/// idle-with-work, not on every intermediate turn-end (CAIRN-1625).
///
/// Fact construction is shared with the boundary wake (`wake_for_issue`) via
/// [`attention::idle_fact_for_issue`] so the two paths cannot diverge. The
/// terminalized `job_id` biases the open-PR lookup toward this builder's `/pr`.
fn emit_for_turn_end(orch: &Orchestrator, job_id: &str) -> bool {
    let Some((issue_id, ctx)) = issue_for_attention_by_job(orch, job_id) else {
        return false;
    };
    let issue_uri = ctx.issue_uri();
    // CAIRN-1647: the child's turn just ended — enrich any unresponded
    // message items for its issue with the response state so the requesting
    // parent's next briefing shows the child already acted on the message.
    crate::orchestrator::attention_delivery::enrich_message_items_on_turn_end(
        orch, &issue_id, &issue_uri,
    );
    // Resolve the fact (and any detail URI) synchronously via the shared helper.
    let db = orch.db.local.clone();
    let issue_id_for_fact = issue_id.clone();
    let ctx_for_fact = ctx.clone();
    let job_id_owned = job_id.to_string();
    let idle = run_db_blocking(move || async move {
        Ok::<_, String>(
            crate::orchestrator::attention::idle_fact_for_issue(
                &db,
                &issue_id_for_fact,
                &ctx_for_fact,
                Some(&job_id_owned),
            )
            .await,
        )
    })
    .ok()
    .flatten();
    let Some(idle) = idle else {
        return false;
    };
    orch.emit_attention_event(crate::orchestrator::AttentionEvent {
        issue_id,
        issue_uri,
        fact: idle.fact,
        attention: ctx.attention,
        status: ctx.status,
        updated_at: idle.updated_at,
    });
    true
}

fn job_id_for_run(orch: &Orchestrator, run_id: &str) -> Option<String> {
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

fn running_terminals_for_job(orch: &Orchestrator, job_id: &str) -> Vec<(String, String)> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, session_id
                         FROM job_terminals
                         WHERE job_id = ?1
                           AND status = 'running'",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut terminals = Vec::new();
                while let Some(row) = rows.next().await? {
                    terminals.push((row.text(0)?, row.text(1)?));
                }
                Ok(terminals)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

/// Transition a run to warm state after successful turn completion.
///
/// The Run stays `Live` in the DB — no durable status change.
/// Completes the current Turn and transitions process occupancy to Idle.
///
/// Returns true if the process was successfully transitioned to warm.
pub fn transition_to_warm_state(orch: &Orchestrator, run_id: &str) -> bool {
    // Complete the current turn before transitioning occupancy
    let completed_turn_id = orch.process_state.get_current_turn_id(run_id);
    if let Some(turn_id) = completed_turn_id.as_deref() {
        let _ = apply_turn_outcome(orch, turn_id, TurnState::Complete);
    }

    if orch.process_state.transition_to_warm(run_id) {
        // Emit turn db-change so frontend sees the turn completion
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );

        log::info!(
            "Run {} transitioned to warm state (process retained for potential follow-up)",
            &run_id[..run_id.len().min(8)]
        );

        // The turn just completed (recorded above). Job status is a derived
        // projection, so recompute it now that the turn is terminal — this is
        // what derives Blocked (open `user` confirm gate), Complete, and the DAG
        // advance. Previously the `return` tool's interrupt routed completion
        // through `finalize_run`; with the interrupt gone, the clean warm
        // transition is the turn-complete signal and must drive the recompute.
        if let Some(job_id) = job_id_for_run(orch, run_id) {
            if let Err(e) = crate::execution::advancement::recompute_job(orch, &job_id) {
                log::error!(
                    "Failed to recompute job {} after warm transition: {}",
                    job_id,
                    e
                );
            }
            finish_memory_review_if_due(orch, &job_id, run_id);
            // Turn-end: the agent went idle. Emit a fact-driven event so any
            // in-flight `watch` learns the issue is actionable (or resolved)
            // without depending on the recompute sweep poke that this work
            // is replacing.
            let needs_attention = emit_for_turn_end(orch, &job_id);
            // Raise the desktop "completed" toast only when that idle left
            // something for the driver/user to act on — a plan awaiting
            // confirmation, a PR awaiting merge, a pending question, or a
            // terminal status. A bare turn-end with no work left (e.g. a planner
            // that just spawned child tasks and is now waiting on them) is not a
            // completion worth pinging about (CAIRN-1625).
            if needs_attention {
                emit_agent_terminal_attention_once(orch, run_id, "completed");
            }
            // Flush any directs/side-channel notices queued mid-turn for this
            // run. If this turn was the run's last, no further prompt boundary
            // fires, so without this they would sit unclaimed (CAIRN-1297).
            crate::messages::delivery::flush_pending_directs_on_idle(orch, run_id);
        }

        // CAIRN-1576 routes terminal-tool completion (e.g. a child's `return`)
        // through this warm transition instead of `finalize_run`, so the full
        // completion contract must live here too. Mirror `finalize_run`'s tail:
        //
        // 1. Wake a suspended delegated parent. A child that completes via the
        //    terminal-tool warm path is recomputed to `complete` but its process
        //    is retained warm — it never reaches stdout EOF, so `finalize_run`
        //    never runs for it. Without this call the resume trigger is dropped
        //    entirely and a suspended batch parent hangs forever. self-gates on
        //    the packet/sibling terminal state, so it is a cheap no-op for
        //    non-delegated jobs and every other warm-transition caller.
        try_resume_delegated_parent(orch, run_id);

        // 2. Signal the internal completion broadcast consumed by
        //    `spawn_task_packets`' inline 45s wait so a child that finishes
        //    fast is detected and the batch returns inline. This is the tokio
        //    broadcast, NOT the frontend `run-completed` emit — the run is warm,
        //    not exited, so the frontend keeps receiving `run-turn-completed`.
        //    Harmless for top-level jobs: their run ids are never in an inline
        //    wait's pending set.
        let _ = orch.run_completions.send(run_id.to_string());

        true
    } else {
        log::warn!(
            "Failed to transition run {} to warm state (process not found)",
            &run_id[..run_id.len().min(8)]
        );
        false
    }
}

/// Park a run for durable wait without tearing down its process.
///
/// Durable waits are not crashes. We interrupt the current turn, clean up any
/// foreground inline commands, and leave the process warm so it can resume when
/// the awaited dependency resolves.
pub fn suspend_run_for_durable_wait(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
) -> Result<(), String> {
    let _ = exit_reason;
    stop_session_internal(orch, run_id)?;
    Ok(())
}

/// Wake a delegated parent whose turn was suspended waiting on this job's run.
///
/// Called on every run finalization (normal exit, crash, or re-finalization of
/// an already-settled run). The resume logic self-gates: it only proceeds when
/// the finalized job maps to a delegated packet whose siblings are all terminal,
/// so calling it for non-delegated jobs or partially-complete batches is a
/// cheap no-op. This must run even on the already-finalized fast path, because
/// a child that submits via the `return` tool settles its run before the
/// process exit re-enters here — skipping it leaves suspended batch parents
/// stopped forever.
fn finish_memory_review_if_due(orch: &Orchestrator, job_id: &str, run_id: &str) {
    let state = match crate::memories::commands::memory_review_idle_state_for_job(orch, job_id) {
        Ok(state) => state,
        Err(error) => {
            log::warn!("Failed to read memory review state for job {job_id}: {error}");
            return;
        }
    };

    match state.state.as_deref() {
        // Fire the end-step when the job has finished its real work (an
        // artifact exists) and either captured drafts to review (any run,
        // tasks included) or is a top-level node job worth a reflection nudge.
        None if state.has_artifact && (state.draft_count > 0 || !state.is_task) => {
            match crate::memories::commands::send_memory_review_on_idle(orch, job_id, run_id) {
                Ok(true) => log::info!(
                    "Sent memory {} prompt for job {} ({} draft memor{})",
                    if state.draft_count > 0 {
                        "review"
                    } else {
                        "reflection"
                    },
                    &job_id[..job_id.len().min(8)],
                    state.draft_count,
                    if state.draft_count == 1 { "y" } else { "ies" }
                ),
                Ok(false) => {}
                Err(error) => log::warn!(
                    "Failed to send memory review prompt for job {}: {error}",
                    &job_id[..job_id.len().min(8)]
                ),
            }
            return;
        }
        // Complete the review only once its MemoryReview turn has actually
        // ended. The review prompt resumes the agent into a turn tagged
        // `memory_review`; completing the review must key off that turn reaching
        // a terminal state, not the next warm transition after the prompt was
        // sent. The old `Some("sent") => {}` fall-through completed on the very
        // next (often back-to-back) warm transition — confirming surviving
        // drafts before the reflection turn had run, and orphaning drafts the
        // reflection turn was still writing (CAIRN-1576).
        Some("sent") if memory_review_turn_ended(orch, job_id) => {}
        _ => return,
    }

    match crate::memories::commands::complete_sent_memory_review(orch, job_id) {
        Ok(completion) => {
            log::info!(
                "Completed memory review for job {}; confirmed {} surviving draft memor{}",
                &job_id[..job_id.len().min(8)],
                completion.confirmed_count,
                if completion.confirmed_count == 1 {
                    "y"
                } else {
                    "ies"
                }
            );
            let triage_orch = orch.clone();
            let confirmed_scopes = completion.confirmed_scopes.clone();
            if let Err(error) = run_db_blocking(move || async move {
                crate::memories::triage::maybe_spawn_triage(triage_orch, confirmed_scopes).await
            }) {
                log::warn!("memory triage check after review failed: {error}");
            }
            if let Err(error) = crate::execution::advancement::recompute_job(orch, job_id) {
                log::warn!(
                    "Failed to recompute job {} after memory review completion: {error}",
                    &job_id[..job_id.len().min(8)]
                );
            }
            close_terminal_sessions_after_memory_review(orch, job_id);
        }
        Err(error) => log::warn!(
            "Failed to complete memory review for job {}: {error}",
            &job_id[..job_id.len().min(8)]
        ),
    }
}

/// Whether the job's MemoryReview turn has ended. The review prompt resumes the
/// agent into a turn tagged `memory_review`; review completion keys off that
/// turn reaching a terminal, non-yielded state (a yielded review turn is paused
/// on a host wait, not done). Returns false when the latest turn is the work
/// turn or a still-running review turn, so a warm transition that fires before
/// the reflection turn ends does not complete the review early.
pub(crate) fn memory_review_turn_ended(orch: &Orchestrator, job_id: &str) -> bool {
    blocking_text_lookup(
        orch,
        job_id,
        "SELECT CASE
                  WHEN start_reason = 'memory_review'
                   AND state IN ('complete', 'failed', 'interrupted', 'cancelled')
                  THEN '1' ELSE '0' END
         FROM turns WHERE job_id = ?1
         ORDER BY created_at DESC, sequence DESC LIMIT 1",
        TextColumn::Optional,
    )
    .as_deref()
        == Some("1")
}

fn close_terminal_sessions_after_memory_review(orch: &Orchestrator, job_id: &str) {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    let _ = run_db_blocking(move || async move {
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.status
                         FROM jobs j
                         JOIN issues i ON i.id = j.issue_id
                         WHERE j.id = ?1
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(());
                };
                let status = row.text(0)?;
                if !matches!(status.as_str(), "closed" | "merged") {
                    return Ok(());
                }
                let now = chrono::Utc::now().timestamp();
                conn.execute(
                    "UPDATE sessions
                     SET status = 'closed', terminal_reason = 'issue_closed', closed_at = ?1, updated_at = ?1
                     WHERE job_id = ?2 AND status = 'open'",
                    (now, job_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    });
}

fn try_resume_delegated_parent(orch: &Orchestrator, run_id: &str) {
    let Some(job_id) = job_id_for_run(orch, run_id) else {
        return;
    };
    if let Err(e) =
        crate::execution::delegation::resume_suspended_parent_after_task_completion(orch, &job_id)
    {
        log::warn!(
            "Failed to resume suspended delegated parent after job {} finalized: {}",
            job_id,
            e
        );
    }
}

pub fn finalize_run(orch: &Orchestrator, run_id: &str, status: RunStatus) {
    // Clean up system prompt temp file
    super::session::cleanup_prompt_file(run_id);

    // Finalize the current turn based on run outcome.
    // Primary: in-memory process state. Fallback: job's current_turn_id in DB
    // (covers crashes where the process was never registered or already deregistered).
    let turn_id = orch
        .process_state
        .get_current_turn_id(run_id)
        .or_else(|| current_turn_id_for_run(orch, run_id));
    let had_active_turn = turn_id.is_some();
    if let Some(ref turn_id) = turn_id {
        if let Some(turn_state) = turn_state(orch, turn_id) {
            let result = if status == RunStatus::Exited {
                // Clean exit: a Running turn completed; a turn that never reached
                // Running (Pending) produced nothing, so fail it rather than
                // leaving it live (which would keep the job derived as Running).
                if turn_state.as_str() == "pending" {
                    apply_turn_outcome(orch, turn_id, TurnState::Failed)
                } else {
                    apply_turn_outcome(orch, turn_id, TurnState::Complete)
                }
            } else if turn_state.as_str() == "running" {
                interrupt_turn(orch, turn_id)
            } else {
                apply_turn_outcome(orch, turn_id, TurnState::Failed)
            };

            if let Err(e) = result {
                log::warn!(
                    "Failed to finalize turn {} for run {}: {}",
                    turn_id,
                    run_id,
                    e
                );
            }
        }
    }

    // Transition run via transition_run (validates state machine, emits db-change).
    // Also get the job_id for subsequent job lifecycle.
    let current_status = run_status(orch, run_id);
    if matches!(current_status.as_deref(), Some("exited") | Some("crashed")) {
        log::info!(
            "Run {} already finalized as {:?}, skipping re-finalization as {:?}",
            &run_id[..run_id.len().min(8)],
            current_status,
            status
        );
        // Still emit run-completed so task handlers waiting on this event are unblocked.
        let _ = orch
            .services
            .emitter
            .emit("run-completed", serde_json::json!(run_id));
        let _ = orch.run_completions.send(run_id.to_string());
        // A delegated child whose run was already settled (typically via the
        // return tool, which finalizes before the process exits) must still wake
        // a suspended parent batch. Without this, the parent never resumes.
        try_resume_delegated_parent(orch, run_id);
        // The run settled via the `return` tool before this process-exit
        // re-entry; still flush any direct queued against it so a parent that
        // finalized that way isn't left unaware of a stuck child (CAIRN-1297).
        crate::messages::delivery::flush_pending_directs_on_idle(orch, run_id);
        return;
    }

    if let Err(e) = transition_run(orch, run_id, status.clone()) {
        log::error!("Failed to transition run {}: {}", run_id, e);
    }

    if let Some(run) = run_for_sync(orch, run_id) {
        orch.sync(crate::sync::SyncMessage::Run((&run).into()));
    }

    let job_id = job_id_for_run(orch, run_id);

    if had_active_turn {
        // Finalize todos: mark any in_progress as completed
        if let Some(ref job_id) = job_id {
            let _ = finalize_todos(orch, job_id);
        }

        // Job status is a derived projection. The turn outcome was already
        // recorded above (Complete on clean exit, Failed/Interrupted on crash);
        // recompute derives the job's status from it — Complete, Blocked (open
        // approval checkpoint), or Failed — and cascades + advances the DAG.
        // This is purely mechanical now; finalize_run no longer decides outcomes.
        if let Some(job_id) = job_id.clone() {
            if let Err(e) = crate::execution::advancement::recompute_job(orch, &job_id) {
                log::error!(
                    "Failed to recompute job {} after run finalize: {}",
                    job_id,
                    e
                );
            }
            finish_memory_review_if_due(orch, &job_id, run_id);
            // Turn-end on any terminal run outcome (clean exit or crash): the
            // agent is idle now. The recompute above may have flipped status
            // to terminal (→ Resolved) or left attention pointing at the next
            // human action (→ AgentIdleWithWork). Either way, the long-poll
            // hears about it through this fact rather than the recompute-sweep
            // poke this work removes.
            emit_for_turn_end(orch, &job_id);
            // Run-terminal idle: flush any directs/side-channel notices still
            // pending for this run so a queued child-attention update is not
            // stranded when the run never takes another turn (CAIRN-1297).
            crate::messages::delivery::flush_pending_directs_on_idle(orch, run_id);
        }
    } else if let Some(ref job_id) = job_id {
        log::info!(
            "Run {} exited without an active turn; skipping job lifecycle reduction for {}",
            &run_id[..run_id.len().min(8)],
            &job_id[..job_id.len().min(8)]
        );
    }

    // Wake a suspended delegated parent on any terminal outcome (exit or crash):
    // a crashed child still resolves its packet to Failed, and the parent should
    // resume with that failure rather than hang. resume_... self-gates on the
    // packet/sibling terminal state.
    try_resume_delegated_parent(orch, run_id);

    // Emit run completed event (frontend)
    let _ = orch
        .services
        .emitter
        .emit("run-completed", serde_json::json!(run_id));

    // Signal run_completions broadcast (unblocks handle_task waiters)
    let _ = orch.run_completions.send(run_id.to_string());

    // Log session context for crash observability
    if status == RunStatus::Crashed {
        log_session_crash_context(orch, run_id);
    }

    // Completion attention fires when the agent goes idle/warm. Finalization is
    // only a legacy toast source for genuine crash paths that terminalize an
    // in-flight turn without reaching the idle boundary first.
    if had_active_turn && status == RunStatus::Crashed {
        emit_agent_terminal_attention_once(orch, run_id, "failed");
    }
}

/// Log session context when a run crashes. If this was a resume attempt, the session
/// may be invalid — log a warning so operators can investigate.
fn log_session_crash_context(orch: &Orchestrator, run_id: &str) {
    let db = orch.db.local.clone();
    let log_run_id = run_id.to_string();
    let run_id = run_id.to_string();
    let run_info = run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT session_id, start_mode
                         FROM runs
                         WHERE id = ?1",
                        (run_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten();

    if let Some((Some(session_id), start_mode)) = run_info {
        if start_mode.as_deref() == Some("resume") {
            log::warn!(
                "Resume run {} crashed for session {} — \
                 session may be invalid. If this repeats, session rotation may be needed.",
                &log_run_id[..log_run_id.len().min(8)],
                &session_id[..session_id.len().min(8)]
            );
        }
    }
}

/// Emit legacy `agent-attention` terminal toast once per run, but only for top-level jobs.
fn emit_agent_terminal_attention_once(
    orch: &Orchestrator,
    run_id: &str,
    attention_type: &'static str,
) {
    let inserted = {
        let mut seen = orch.agent_completion_attention_dedupe.lock().unwrap();
        seen.insert(run_id.to_string())
    };
    if !inserted {
        log::debug!(
            "Suppressing duplicate legacy agent-attention terminal event for run {} ({})",
            run_id,
            attention_type
        );
        return;
    }

    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    let row = run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT projects.key, issues.number, issues.title, jobs.node_name, executions.seq
                         FROM runs
                         JOIN jobs ON runs.job_id = jobs.id
                         JOIN projects ON jobs.project_id = projects.id
                         LEFT JOIN issues ON jobs.issue_id = issues.id
                         LEFT JOIN executions ON jobs.execution_id = executions.id
                         WHERE runs.id = ?1
                           AND jobs.parent_job_id IS NULL
                           AND runs.job_id IS NOT NULL",
                        (run_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok((
                            row.text(0)?,
                            row.opt_i64(1)?.map(|n| n as i32),
                            row.opt_text(2)?,
                            row.opt_text(3)?,
                            row.opt_i64(4)?.map(|n| n as i32),
                        ))
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten();

    let Some((project_key, issue_number, issue_title, node_name, exec_seq)) = row else {
        return;
    };

    emit_attention(
        &*orch.services.emitter,
        &AttentionEvent {
            attention_type,
            project_key: &project_key,
            issue_number,
            issue_title: issue_title.as_deref(),
            node_name: node_name.as_deref(),
            exec_seq,
            tool_name: None,
        },
    );
}

fn child_run_ids_for_run(orch: &Orchestrator, run_id: &str) -> Vec<String> {
    let Some(job_id) = job_id_for_run(orch, run_id) else {
        return Vec::new();
    };
    let db = orch.db.local.clone();
    run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(async move {
                let descendant_job_ids = find_descendant_job_ids(conn, &job_id).await?;
                if descendant_job_ids.is_empty() {
                    return Ok(Vec::new());
                }
                get_running_runs_for_jobs(conn, &descendant_job_ids).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

async fn find_descendant_job_ids(conn: &turso::Connection, job_id: &str) -> DbResult<Vec<String>> {
    let mut all_descendants = Vec::new();
    let mut current_parents = vec![job_id.to_string()];

    while !current_parents.is_empty() {
        let mut children = Vec::new();
        for parent_id in &current_parents {
            let mut rows = conn
                .query(
                    "SELECT id
                     FROM jobs
                     WHERE parent_job_id = ?1",
                    (parent_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                children.push(row.text(0)?);
            }
        }

        if children.is_empty() {
            break;
        }

        all_descendants.extend(children.clone());
        current_parents = children;
    }

    Ok(all_descendants)
}

async fn get_running_runs_for_jobs(
    conn: &turso::Connection,
    job_ids: &[String],
) -> DbResult<Vec<String>> {
    let mut run_ids = Vec::new();
    for job_id in job_ids {
        let mut rows = conn
            .query(
                "SELECT id
                 FROM runs
                 WHERE job_id = ?1
                   AND status IN ('starting', 'live')",
                (job_id.as_str(),),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            run_ids.push(row.text(0)?);
        }
    }
    Ok(run_ids)
}

/// Resolve the live (`starting`/`live`) run id for a job, if one exists.
///
/// Used by the resource-layer node `stop` action to find the run whose turn to
/// interrupt. Returns `None` when the node has no active run (already complete,
/// failed, or never started), letting the caller report a clear no-op rather
/// than guessing a run id.
pub fn live_run_id_for_job(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.read(|conn| {
            Box::pin(
                async move { get_running_runs_for_jobs(conn, std::slice::from_ref(&job_id)).await },
            )
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .and_then(|ids| ids.into_iter().next())
}

/// Stop a running backend session, cascading to child runs
pub fn stop_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // First, collect child runs to stop
    let child_run_ids = child_run_ids_for_run(orch, run_id);

    // Stop child runs first
    for child_run_id in &child_run_ids {
        log::info!(
            "Stopping child run {} (parent run {} stopped)",
            child_run_id,
            run_id
        );
        let _ = stop_session_internal(orch, child_run_id);
    }

    // Stop the requested run
    stop_session_internal(orch, run_id)
}

/// Internal stop without cascading (used by cascading stop)
///
/// Sends an interrupt control request via stdin and transitions the process
/// to warm state. The process is NOT killed - it stays available for follow-up
/// messages.
fn stop_session_internal(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // Interrupt/cancel the current turn. Fall back to the DB current_turn_id so
    // stop repairs stale UI-visible state even when the process map lost the run.
    stop_active_turn_for_run(orch, run_id);

    // Send interrupt via backend-aware stdin handler
    if let Err(e) = crate::backends::stdin::send_interrupt(&orch.process_state, run_id) {
        log::warn!("Failed to send interrupt to run {}: {}", run_id, e);
    }

    // Only kill foreground bash processes — background terminals survive the interrupt
    cleanup_inline_commands(orch, run_id);

    // Transition to warm state instead of killing
    if orch.process_state.transition_to_warm(run_id) {
        log::info!(
            "Run {} interrupted and transitioned to warm state",
            &run_id[..run_id.len().min(8)]
        );
    } else {
        log::warn!(
            "Run {} not found in process map after interrupt",
            &run_id[..run_id.len().min(8)]
        );
        if matches!(
            run_status(orch, run_id).as_deref(),
            Some("starting" | "live" | "running" | "idle")
        ) {
            let _ = set_exit_reason(orch, run_id, "user_stop");
            if let Err(error) = transition_run(orch, run_id, RunStatus::Exited) {
                log::warn!("Failed to finalize stopped stale run {}: {}", run_id, error);
            }
            let _ = orch
                .services
                .emitter
                .emit("run-completed", serde_json::json!(run_id));
            let _ = orch.run_completions.send(run_id.to_string());
        }
    }

    Ok(())
}

/// Kill only foreground (inline) bash processes for a run.
///
/// Background terminals are intentionally left alive — they should survive
/// an interrupt so the agent can resume and still interact with them.
fn cleanup_inline_commands(orch: &Orchestrator, run_id: &str) {
    for child in orch.pty_state.take_inline_commands(run_id) {
        if let Ok(mut child) = child.lock() {
            let _ = child.kill();
            let _ = child.try_wait();
        }
    }
}

/// Finalize background terminals associated with a run's job on hard kill (not
/// interrupt).
///
/// This runs on user-stop / GC eviction: the run stops but the issue/job
/// persists and may resume, so terminals are marked `exited` (retained) rather
/// than deleted — deletion is reserved for true job teardown
/// (`execution/teardown.rs`). Each terminal converges on the single finalize
/// sink, which kills the child, records an honest non-success exit code, routes
/// the exit wake, and drops the live session.
fn cleanup_job_terminals(orch: &Orchestrator, run_id: &str) {
    let job_id = job_id_for_run(orch, run_id);
    let Some(job_id) = job_id else {
        return;
    };

    let running_terminals = running_terminals_for_job(orch, &job_id);

    for (_terminal_id, session_id) in running_terminals {
        if let Err(error) =
            crate::mcp::handlers::bash::finalize_terminal_by_session_id(orch, &session_id)
        {
            log::warn!("failed to finalize terminal {session_id} on session kill: {error}");
        }
    }
}

/// Forcefully kill a backend session and finalize.
///
/// Use this when truly terminating a session (e.g., closing an issue,
/// GC eviction, or cleanup). Unlike `stop_session`, this actually kills
/// the process and cannot be resumed.
pub fn kill_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    kill_session_with_reason(orch, run_id, "user_stop")
}

/// Kill a session with a specific exit reason.
pub fn kill_session_with_reason(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
) -> Result<(), String> {
    // Only send interrupt to non-idle processes
    let is_idle = orch
        .process_state
        .get_occupancy(run_id)
        .map(|o| matches!(o, crate::agent_process::process::RunOccupancy::Idle))
        .unwrap_or(false);

    if !is_idle {
        let _ = crate::backends::stdin::send_interrupt(&orch.process_state, run_id);

        // Brief wait for graceful handling
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Clean up all tool processes — both foreground and background
    cleanup_inline_commands(orch, run_id);
    cleanup_job_terminals(orch, run_id);

    // Kill the process
    let mut processes = orch
        .process_state
        .processes
        .lock()
        .map_err(|e| e.to_string())?;

    if let Some(handle) = processes.remove(run_id) {
        if let Ok(mut child_guard) = handle.child.lock() {
            if let Some(mut child) = child_guard.take() {
                crate::agent_process::process::graceful_stop(&mut *child);
                log::info!("Killed process for run {}", &run_id[..run_id.len().min(8)]);
            }
        }
    }

    // Drop the lock before calling finalize_run (which also locks)
    drop(processes);

    // Set exit reason and finalize as Exited (clean kill) or Crashed
    let final_status = if exit_reason == "crash" {
        RunStatus::Crashed
    } else {
        RunStatus::Exited
    };

    let _ = set_exit_reason(orch, run_id, exit_reason);

    finalize_run(orch, run_id, final_status);

    Ok(())
}

#[cfg(test)]
mod memory_review_tests {
    use super::finish_memory_review_if_due;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use std::sync::Arc;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("memory-review.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn test_orchestrator(db: LocalDb) -> crate::orchestrator::Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        std::fs::write(
            config_dir.join("recipes/memory-triage.yaml"),
            include_str!("../../../../recipes/memory-triage.yaml"),
        )
        .unwrap();
        std::fs::write(
            config_dir.join("agents/integrator.md"),
            include_str!("../../../../agents/integrator.md"),
        )
        .unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    async fn seed_job(db: &LocalDb, review_state: Option<&str>) {
        seed_job_row(db, review_state, false).await;
        insert_draft_memory(db, "m-review", 1).await;
    }

    /// Insert the project/issue/execution scaffold and the `j-review` job
    /// without any draft memory. When `is_task` is set, a parent job is
    /// inserted first and `j-review.parent_job_id` points at it, so the job
    /// reads back as a sub-agent task.
    async fn seed_job_row(db: &LocalDb, review_state: Option<&str>, is_task: bool) {
        db.write(|conn| {
            let review_state = review_state.map(str::to_string);
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-review','recipe','i-review','p-review','running',1,1)", ()).await?;
                let parent_job_id = if is_task {
                    conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-parent','e-review','coordinator','i-review','p-review','complete','coordinator','coordinator',1,1)", ()).await?;
                    Some("j-parent")
                } else {
                    None
                };
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder',?1,'i-review','p-review','complete','builder','builder',?2,1,1)",
                    (parent_job_id, review_state.as_deref()),
                ).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn insert_draft_memory(db: &LocalDb, id: &str, node_seq: i64) {
        crate::memories::db::create_memory(
            db,
            id,
            Some(id),
            "remember durable behavior",
            Some("p-review"),
            "project",
            "p-review",
            Some("j-review"),
            Some(node_seq),
            None,
        )
        .await
        .unwrap();
    }

    async fn insert_artifact(db: &LocalDb) {
        db.execute(
            "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)",
            (),
        )
        .await
        .unwrap();
    }

    async fn insert_run_session_with_turns(db: &LocalDb, turn_count: i64) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO sessions (id, job_id, status, created_at, updated_at) VALUES ('s-review','j-review','open',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id, created_at, updated_at) VALUES ('run-review','i-review','p-review','j-review','completed','s-review',1,1)",
                    (),
                )
                .await?;
                for sequence in 1..=turn_count {
                    conn.execute(
                        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1,'s-review','run-review','j-review',?2,'complete','initial',?2,?2)",
                        (format!("t-work-{sequence}"), sequence),
                    )
                    .await?;
                }
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// Insert a MemoryReview turn for `j-review` in the given state. Review
    /// completion now keys off this turn reaching a terminal state, so the
    /// `sent` path needs one present to fire.
    async fn insert_review_turn(db: &LocalDb, state: &str) {
        db.write(|conn| {
            let state = state.to_string();
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO sessions (id, job_id, status, created_at, updated_at) VALUES ('s-review','j-review','open',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('t-review','s-review','j-review',1,?1,'memory_review',2,2)",
                    (state,),
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn memory_status(orch: &crate::orchestrator::Orchestrator) -> String {
        orch.db
            .local
            .query_one(
                "SELECT status FROM memories WHERE id = 'm-review'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    async fn review_state(orch: &crate::orchestrator::Orchestrator) -> Option<String> {
        orch.db
            .local
            .query_one(
                "SELECT memory_review_state FROM jobs WHERE id = 'j-review'",
                (),
                |row| row.opt_text(0),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn null_with_drafts_sends_review_without_confirming() {
        let db = test_db().await;
        seed_job(&db, None).await;
        insert_artifact(&db).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
        assert_eq!(memory_status(&orch).await, "draft");
        let messages = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'direct' AND recipient_run_id = 'run-review' AND content LIKE '%Draft memories:%'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(messages, 1);
    }

    #[tokio::test]
    async fn null_with_drafts_but_no_artifact_does_not_send_review() {
        let db = test_db().await;
        seed_job(&db, None).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await, None);
        assert_eq!(memory_status(&orch).await, "draft");
        let messages = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'direct' AND recipient_run_id = 'run-review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(messages, 0);
    }

    #[tokio::test]
    async fn sent_idle_confirms_surviving_drafts_and_spawns_triage() {
        let db = test_db().await;
        seed_job(&db, Some("sent")).await;
        for node_seq in 2..=5 {
            insert_draft_memory(&db, &format!("m-review-{node_seq}"), node_seq).await;
        }
        // The review turn has ended (Complete): completion is allowed to fire.
        insert_review_turn(&db, "complete").await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await.as_deref(), Some("done"));
        assert_eq!(
            orch.db
                .local
                .query_one(
                    "SELECT COUNT(*) FROM memories WHERE job_id = 'j-review' AND status = 'draft'",
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            orch.db
                .local
                .query_one(
                    "SELECT COUNT(*) FROM memories WHERE job_id = 'j-review' AND status = 'claimed' AND scope = 'project' AND scope_value = 'p-review'",
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap(),
            5
        );
        assert_eq!(
            orch.db
                .local
                .query_one(
                    "SELECT COUNT(*)
                     FROM issues i
                     JOIN executions e ON e.issue_id = i.id
                     WHERE i.project_id = 'p-review'
                       AND i.title LIKE 'Memory triage: project=p-review%'
                       AND e.recipe_id = 'memory-triage'",
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap(),
            1
        );
    }

    async fn direct_message_count(orch: &crate::orchestrator::Orchestrator, like: &str) -> i64 {
        let like = like.to_string();
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'direct' AND recipient_run_id = 'run-review' AND content LIKE ?1",
                (like,),
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn null_no_drafts_node_job_short_session_does_not_send_reflection() {
        let db = test_db().await;
        seed_job_row(&db, None, false).await;
        insert_artifact(&db).await;
        insert_run_session_with_turns(&db, 10).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await, None);
        assert_eq!(direct_message_count(&orch, "%").await, 0);
    }

    #[tokio::test]
    async fn null_no_drafts_node_job_long_session_sends_reflection() {
        let db = test_db().await;
        seed_job_row(&db, None, false).await;
        insert_artifact(&db).await;
        insert_run_session_with_turns(&db, 11).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
        // A reflection prompt went out, and it is not the draft-review variant.
        assert_eq!(direct_message_count(&orch, "%reflect%").await, 1);
        assert_eq!(direct_message_count(&orch, "%Draft memories:%").await, 0);
    }

    #[tokio::test]
    async fn null_no_drafts_task_does_not_send_reflection() {
        let db = test_db().await;
        seed_job_row(&db, None, true).await;
        insert_artifact(&db).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await, None);
        assert_eq!(direct_message_count(&orch, "%").await, 0);
    }

    #[tokio::test]
    async fn null_with_drafts_task_sends_review() {
        let db = test_db().await;
        seed_job_row(&db, None, true).await;
        insert_draft_memory(&db, "m-review", 1).await;
        insert_artifact(&db).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        // Review fires for tasks too when they captured drafts.
        assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
        assert_eq!(direct_message_count(&orch, "%Draft memories:%").await, 1);
    }

    #[tokio::test]
    async fn sent_with_running_review_turn_does_not_complete() {
        // The core CAIRN-1576 fix: a warm transition that lands while the
        // reflection turn is still running must not confirm surviving drafts or
        // mark the review done. Completion waits for the review turn to end.
        let db = test_db().await;
        seed_job(&db, Some("sent")).await;
        insert_review_turn(&db, "running").await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
        assert_eq!(memory_status(&orch).await, "draft");
    }

    #[tokio::test]
    async fn done_state_is_noop() {
        let db = test_db().await;
        seed_job(&db, Some("done")).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await.as_deref(), Some("done"));
        assert_eq!(memory_status(&orch).await, "draft");
    }
}

/// CAIRN-1582: a run that completes via the terminal-tool warm transition
/// (instead of `finalize_run`) must still carry the full completion contract —
/// wake a suspended delegated parent and signal the internal completion
/// broadcast. CAIRN-1576 routed terminal-tool completion through
/// `transition_to_warm_state`, and that path had dropped both, leaving batch
/// parents hung forever.
#[cfg(test)]
mod warm_completion_tests {
    use super::{finalize_run, transition_to_warm_state};
    use crate::agent_process::process::{wrap_plain_stdin, RunHandle};
    use crate::db::DbState;
    use crate::models::RunStatus;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::services::EventEmitter;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use serde_json::Value;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct SharedCaptureEmitter {
        events: Arc<Mutex<Vec<(String, Value)>>>,
    }

    impl SharedCaptureEmitter {
        fn events_named(&self, name: &str) -> Vec<Value> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|(event, _)| event == name)
                .map(|(_, payload)| payload.clone())
                .collect()
        }
    }

    impl EventEmitter for SharedCaptureEmitter {
        fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push((event.to_string(), payload));
            Ok(())
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.emit(event, Value::Null)
        }
    }

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("warm-completion.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        test_orchestrator_with_emitter(db, None).0
    }

    fn test_orchestrator_with_emitter(
        db: LocalDb,
        emitter: Option<SharedCaptureEmitter>,
    ) -> (Orchestrator, SharedCaptureEmitter) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let emitter = emitter.unwrap_or_default();
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_emitter(emitter.clone())
                .build(),
        );
        (
            OrchestratorBuilder::new(db_state, services, config_dir).build(),
            emitter,
        )
    }

    /// Register a warm-able process so `transition_to_warm` succeeds. The stdin
    /// is an in-memory writer; the child slot is empty (no real process).
    fn register_warm_process(orch: &Orchestrator, run_id: &str, job_id: Option<&str>) {
        let mut processes = orch.process_state.processes.lock().unwrap();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(Some(wrap_plain_stdin(Box::new(
            Vec::<u8>::new(),
        )))));
        let handle = RunHandle::new(
            child,
            stdin,
            Some(format!("sess-{run_id}")),
            job_id.map(str::to_string),
        );
        processes.register(run_id.to_string(), handle);
    }

    async fn turn_state(orch: &Orchestrator, id: &str) -> String {
        let id = id.to_string();
        orch.db
            .local
            .read(move |conn| {
                let id = id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT state FROM turns WHERE id = ?1", (id.as_str(),))
                        .await?;
                    rows.next().await?.unwrap().text(0)
                })
            })
            .await
            .unwrap()
    }

    async fn seed_top_level_run(
        db: &LocalDb,
        run_id: &str,
        job_id: &str,
        turn_id: &str,
        attention: &str,
    ) {
        let run_id = run_id.to_string();
        let job_id = job_id.to_string();
        let turn_id = turn_id.to_string();
        let attention = attention.to_string();
        db.write(move |conn| {
            let run_id = run_id.clone();
            let job_id = job_id.clone();
            let turn_id = turn_id.clone();
            let attention = attention.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-attn','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-attn','w-attn','Project','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-attn','p-attn',42,'Toast issue','active',?1,1,1)", (attention.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, issue_id, project_id, status, node_name, created_at, updated_at) VALUES (?1,'i-attn','p-attn','running','builder',1,1)", (job_id.as_str(),)).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES (?1,?2,'live',1,1)", (run_id.as_str(), job_id.as_str())).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1,'sess-attn',?2,1,'running','initial',1,1)", (turn_id.as_str(), job_id.as_str())).await?;
                conn.execute("UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2", (turn_id.as_str(), job_id.as_str())).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    }

    fn agent_attention_events(emitter: &SharedCaptureEmitter, attention_type: &str) -> Vec<Value> {
        emitter
            .events_named("agent-attention")
            .into_iter()
            .filter(|payload| payload.get("type").and_then(Value::as_str) == Some(attention_type))
            .collect()
    }

    #[tokio::test]
    async fn warm_transition_emits_completion_attention_for_top_level_run() {
        let db = test_db().await;
        // Issue still needs the driver (plan/PR awaiting confirmation looks like
        // this): the turn-end is a real idle-with-work, so the toast fires.
        seed_top_level_run(&db, "run-attn", "job-attn", "turn-attn", "needs_approval").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "run-attn", Some("job-attn"));
        orch.process_state
            .set_current_turn_id("run-attn", Some("turn-attn"));

        assert!(transition_to_warm_state(&orch, "run-attn"));

        let completed = agent_attention_events(&emitter, "completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].get("projectKey"),
            Some(&Value::String("PRJ".into()))
        );
        assert_eq!(
            completed[0].get("issueNumber").and_then(Value::as_i64),
            Some(42)
        );
        assert_eq!(
            completed[0].get("issueTitle").and_then(Value::as_str),
            Some("Toast issue")
        );
        assert_eq!(
            completed[0].get("nodeName").and_then(Value::as_str),
            Some("builder")
        );
    }

    #[tokio::test]
    async fn warm_transition_without_actionable_work_skips_completed_attention() {
        let db = test_db().await;
        // attention=none, status=active, no PR: the turn ended cleanly with
        // nothing for the driver to act on — a planner that just spawned child
        // tasks and is now waiting on them looks exactly like this. The desktop
        // "completed" toast must stay silent (CAIRN-1625).
        seed_top_level_run(&db, "run-quiet", "job-quiet", "turn-quiet", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "run-quiet", Some("job-quiet"));
        orch.process_state
            .set_current_turn_id("run-quiet", Some("turn-quiet"));

        assert!(transition_to_warm_state(&orch, "run-quiet"));

        assert!(agent_attention_events(&emitter, "completed").is_empty());
    }

    #[tokio::test]
    async fn later_finalize_does_not_duplicate_completed_attention() {
        let db = test_db().await;
        seed_top_level_run(
            &db,
            "run-dedupe",
            "job-dedupe",
            "turn-dedupe",
            "needs_approval",
        )
        .await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "run-dedupe", Some("job-dedupe"));
        orch.process_state
            .set_current_turn_id("run-dedupe", Some("turn-dedupe"));

        assert!(transition_to_warm_state(&orch, "run-dedupe"));
        finalize_run(&orch, "run-dedupe", RunStatus::Exited);

        assert_eq!(agent_attention_events(&emitter, "completed").len(), 1);
        assert_eq!(emitter.events_named("run-completed").len(), 1);
    }

    #[tokio::test]
    async fn clean_finalize_without_prior_warm_does_not_emit_completed_attention() {
        let db = test_db().await;
        seed_top_level_run(&db, "run-finalize", "job-finalize", "turn-finalize", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);

        finalize_run(&orch, "run-finalize", RunStatus::Exited);

        assert!(agent_attention_events(&emitter, "completed").is_empty());
    }

    #[tokio::test]
    async fn crash_finalize_emits_failed_attention_once() {
        let db = test_db().await;
        seed_top_level_run(&db, "run-crash", "job-crash", "turn-crash", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);

        finalize_run(&orch, "run-crash", RunStatus::Crashed);
        finalize_run(&orch, "run-crash", RunStatus::Crashed);

        assert_eq!(agent_attention_events(&emitter, "failed").len(), 1);
    }

    #[tokio::test]
    async fn warm_transition_broadcasts_run_completion() {
        let db = test_db().await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-bcast", None);

        // `spawn_task_packets`' inline 45s wait subscribes here to detect a
        // fast-finishing child. A warmed child must broadcast on it just like
        // `finalize_run` does, or a sub-45s batch never returns inline.
        let mut rx = orch.run_completions.subscribe();
        assert!(transition_to_warm_state(&orch, "run-bcast"));

        assert_eq!(rx.try_recv().ok(), Some("run-bcast".to_string()));
    }

    /// Seed a parent suspended on a delegated wait (anchor turn + pending
    /// `dependency_unblock` successor it points at) and a completed delegated
    /// child whose execution snapshot carries the matching packet. The parent
    /// has no session, so once the resume gate claims the successor the
    /// subsequent `continue_job_impl` fast-fails cleanly — the claimed successor
    /// is the observable proof the resume fired.
    async fn seed_suspended_parent_with_completed_child(db: &LocalDb, snapshot: String) {
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-warm','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-warm','p-warm',1,'T','active','none',1,1)", ()).await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-warm','recipe','i-warm','p-warm','running',1,1,?1)",
                    (snapshot.as_str(),),
                ).await?;
                // Anchor turn the parent suspended on, plus its pending
                // dependency_unblock successor (what the resume gate claims).
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                // Suspended parent: current_turn_id points at the pending
                // successor; no current_session_id so the post-claim
                // continue_job_impl fast-fails after the successor is claimed.
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','waiting','succ',1,1)", ()).await?;
                // Completed delegated child whose run finishes via the warm path.
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-child','e-warm','j-parent','i-warm','p-warm','complete',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    }

    #[tokio::test]
    async fn warm_completion_resumes_suspended_delegated_parent() {
        let snapshot = serde_json::json!({
            "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [], "edges": []},
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId": "i-warm", "projectId": "p-warm", "triggerType": "manual"},
            "delegatedPackets": [{
                "id": "pkt-1",
                "parentJobId": "j-parent",
                "parentTurnId": "anchor",
                "parentToolUseId": "tool-1",
                "origin": "task_tool",
                "title": "Explore",
                "problemStatement": "x",
                "agentConfigId": "Explore",
                "ownership": {"cwd": "/tmp"},
                "outputContract": {"schemaType": "return"},
                "resultArtifactJobId": "j-child",
                "status": "completed",
                "taskIndex": 0,
                "createdAt": 0
            }],
            "createdAt": 0
        })
        .to_string();

        let db = test_db().await;
        seed_suspended_parent_with_completed_child(&db, snapshot).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-child", Some("j-child"));

        // Pre-completion: the parent's resume successor is still pending.
        assert_eq!(turn_state(&orch, "succ").await, "pending");

        // The child completes through the warm path (CAIRN-1576), not finalize_run.
        assert!(transition_to_warm_state(&orch, "run-child"));

        // The resume gate fired: the pending successor was claimed (flipped
        // terminal), the linkage finalize_run's try_resume_delegated_parent
        // produces. Before the fix this stayed 'pending' forever and the parent
        // batch hung.
        assert_eq!(turn_state(&orch, "succ").await, "complete");
    }
}
