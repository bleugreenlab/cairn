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

use crate::agent_process::stream::TranscriptEvent;
use crate::mcp::handlers::{emit_attention, AttentionEvent};
use crate::models::{RunStatus, TurnState};
use crate::storage::{run_db_blocking, DbError, DbResult, RowExt};
use crate::transcripts::stream_store::{self, EventInsert};
use cairn_common::ids;
use std::collections::HashSet;

use super::attention_push::{Boundary, Wake};
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

    let turn_id = turn_id.to_string();
    let outcome_str = outcome.to_string();
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_turn(&dbs, &turn_id)
                .await
                .map_err(|e| e.to_string())?;
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
        }
    })?;
    emit_db_change(orch, "turns", "update");
    Ok(())
}

fn interrupt_turn(orch: &Orchestrator, turn_id: &str) -> Result<(), String> {
    let turn_id = turn_id.to_string();
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_turn(&dbs, &turn_id)
                .await
                .map_err(|e| e.to_string())?;
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
        }
    })?;
    emit_db_change(orch, "turns", "update");
    Ok(())
}

fn transition_run(orch: &Orchestrator, run_id: &str, to: RunStatus) -> Result<RunStatus, String> {
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
    let job_id = job_id_for_run(orch, &emit_run_id);
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("update", &emit_run_id, job_id.as_deref()),
    );
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

fn finalize_todos(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
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

#[derive(Debug, Clone)]
struct PendingRunToolResult {
    tool_use_id: String,
    tool_name: String,
    session_id: Option<String>,
    parent_tool_use_id: Option<String>,
}

fn normalized_tool_name(name: &str) -> &str {
    name.rsplit("__").next().unwrap_or(name)
}

fn is_run_tool_call(name: &str, input: &serde_json::Value) -> bool {
    let normalized = normalized_tool_name(name).to_ascii_lowercase();
    normalized == "run"
        || normalized == "bash"
        || name == "Bash"
        || input.get("command").is_some_and(|value| value.is_string())
}

fn pending_run_tool_results(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
) -> Result<Vec<PendingRunToolResult>, String> {
    let run_id = run_id.to_string();
    let turn_id = turn_id.to_string();
    run_db_blocking({
        let dbs = orch.db.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            db.read(|conn| {
                let run_id = run_id.clone();
                let turn_id = turn_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT data
                         FROM events
                         WHERE run_id = ?1
                           AND turn_id = ?2
                           AND event_type IN ('assistant', 'tool_result')
                         ORDER BY sequence ASC, rowid ASC",
                            (run_id.as_str(), turn_id.as_str()),
                        )
                        .await?;
                    let mut candidates = Vec::new();
                    let mut completed = HashSet::new();
                    while let Some(row) = rows.next().await? {
                        let data = row.text(0)?;
                        let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                            continue;
                        };
                        match event.event_type.as_str() {
                            "assistant" => {
                                for tool in event.tool_uses.unwrap_or_default() {
                                    if is_run_tool_call(&tool.name, &tool.input) {
                                        candidates.push(PendingRunToolResult {
                                            tool_use_id: tool.id,
                                            tool_name: tool.name,
                                            session_id: event.session_id.clone(),
                                            parent_tool_use_id: event.parent_tool_use_id.clone(),
                                        });
                                    }
                                }
                            }
                            "tool_result" => {
                                if let Some(tool_use_id) = event.tool_use_id {
                                    completed.insert(tool_use_id);
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(candidates
                        .into_iter()
                        .filter(|candidate| !completed.contains(&candidate.tool_use_id))
                        .collect())
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })
}

fn run_issue_id(orch: &Orchestrator, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    let db = orch.db.local.clone();
    run_db_blocking(move || async move {
        db.query_opt_text(
            "SELECT issue_id FROM runs WHERE id = ?1 LIMIT 1",
            turso::params![run_id.as_str()],
        )
        .await
        .map_err(|e| format!("Failed to load issue id for run db-change: {e}"))
    })
}

fn fail_pending_run_tool_results(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
) -> Result<usize, String> {
    let pending = pending_run_tool_results(orch, run_id, turn_id)?;
    if pending.is_empty() {
        return Ok(0);
    }

    let owning = run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let mut sequence = stream_store::get_next_sequence(owning.clone(), run_id)?;
    let now = chrono::Utc::now().timestamp() as i32;
    let mut inserted = 0;
    for pending_result in pending {
        let event_id = ids::mint_child(run_id);
        let event = TranscriptEvent {
            event_type: "tool_result".to_string(),
            session_id: pending_result.session_id.clone(),
            parent_tool_use_id: pending_result.parent_tool_use_id.clone(),
            content: None,
            thinking: None,
            tool_name: Some(pending_result.tool_name.clone()),
            tool_input: None,
            tool_uses: None,
            tool_use_id: Some(pending_result.tool_use_id.clone()),
            tool_result: Some("Run interrupted by user stop.".to_string()),
            is_error: true,
            thinking_ms: None,
            raw: Some(serde_json::json!({ "synthetic": true, "reason": "user_stop" })),
        };
        let data = serde_json::to_string(&event).unwrap_or_default();
        let event_insert = EventInsert {
            id: event_id.clone(),
            run_id: run_id.to_string(),
            session_id: pending_result.session_id.clone(),
            sequence,
            timestamp: now,
            event_type: "tool_result".to_string(),
            data: data.clone(),
            parent_tool_use_id: pending_result.parent_tool_use_id.clone(),
            created_at: now,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            thinking_tokens: None,
            turn_id: Some(turn_id.to_string()),
            cost_usd: None,
        };
        if stream_store::insert_event(owning.clone(), event_insert)? {
            inserted += 1;
            let issue_id = run_issue_id(orch, run_id)?;
            let _ = orch.services.emitter.emit(
                "db-change",
                crate::notify::event_db_change_scoped(
                    run_id,
                    pending_result.session_id.as_deref(),
                    issue_id.as_deref(),
                    "insert",
                ),
            );
        }
        sequence += 1;
    }
    Ok(inserted)
}

pub fn stop_active_turn_for_run(orch: &Orchestrator, run_id: &str) {
    let Some(turn_id) = active_turn_id_for_run(orch, run_id) else {
        return;
    };

    let Some(state) = turn_state(orch, &turn_id) else {
        log::warn!("Run {} current turn {} was not found", run_id, turn_id);
        return;
    };

    if state == "running" {
        if let Err(error) = fail_pending_run_tool_results(orch, run_id, &turn_id) {
            log::warn!(
                "Failed to fail pending run tool results for run {} turn {}: {}",
                run_id,
                turn_id,
                error
            );
        }
    }

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
    let issue_id = blocking_text_lookup(
        orch,
        job_id,
        "SELECT issue_id FROM jobs WHERE id = ?1",
        TextColumn::Optional,
    )?;
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
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
    // CAIRN-1882: the single creator of a review push. At this work-turn idle
    // edge, push a review to the issue's watchers when there is reviewable
    // output. Independent of the idle fact computed below (which still drives the
    // desktop "completed" toast and `cairn watch`): only the review-to-watcher
    // wake moved to the push queue.
    create_review_push_on_turn_end(orch, job_id, &issue_id, &ctx);
    // Resolve the fact (and any detail URI) synchronously via the shared helper.
    let dbs = orch.db.clone();
    let issue_id_for_fact = issue_id.clone();
    let ctx_for_fact = ctx.clone();
    let job_id_owned = job_id.to_string();
    let idle = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id_owned)
            .await
            .map_err(|e| e.to_string())?;
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

/// Fire the turn-end (`when:idle`/`when:review`) project checks for a job that
/// just idled, detached onto a background task so the minutes-long suite never
/// blocks the turn from ending. Skipped for a trailing memory-review turn (not a
/// work turn) and when a run is already in flight for the job (single-flight).
/// Runs UNSANDBOXED in the background; on any check failure it resumes the idle
/// builder with the failure inlined. Invoked at BOTH turn-end callers
/// (`finalize_run` and `transition_to_warm_state`) so the two stay mirrored.
fn spawn_turn_end_checks(orch: &Orchestrator, job_id: &str) {
    if latest_turn_is_memory_review(orch, job_id) {
        return;
    }
    // Claim the single-flight slot; a concurrent run for this job means skip.
    if !orch.try_begin_turn_end_checks(job_id) {
        return;
    }
    let orch_clone = orch.clone();
    let job_id_owned = job_id.to_string();
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::spawn(async move {
            crate::execution::checks_turn_end::run_turn_end_checks(orch_clone, job_id_owned).await;
        });
    } else {
        // No runtime to detach onto (not expected at turn-end): release the slot
        // so a later turn-end can retry rather than being permanently blocked.
        orch.end_turn_end_checks(job_id);
    }
}

/// Create the review push at this work-turn idle edge (CAIRN-1882).
///
/// The node-idle edge of the review push (the second edge — when a PR opens
/// after the node already idled — is [`create_review_push_for_pr_open`]): when a
/// producing node's turn ends and it is idle, push a `review:{issue}` to each
/// watcher of the node's issue if
///
/// 1. the just-ended turn was a *work* turn (`start_reason != 'memory_review'`),
///    and
/// 2. the issue has reviewable output — either an open, unmerged `merge_requests`
///    row, or a create-pr/unconfirmed-plan artifact for the producing job.
///
/// `content_ref` is the producing node's `/pr` URI when a PR exists, else its
/// artifact URI — the resolvable thing the watcher reviews. Supersede-by-key
/// collapses repeats; the delivery layer (CAIRN-1881) drains, lazy-resolves, and
/// stamps the push. The producing node is never a recipient of its own review.
fn create_review_push_on_turn_end(
    orch: &Orchestrator,
    job_id: &str,
    issue_id: &str,
    ctx: &crate::orchestrator::attention::IssueAttentionContext,
) {
    // Gate: only a WORK-turn idle creates a review push. A trailing memory-review
    // turn end (start_reason = 'memory_review') must not.
    if latest_turn_is_memory_review(orch, job_id) {
        return;
    }
    let dbs = orch.db.clone();
    let issue_id_owned = issue_id.to_string();
    let job_id_owned = job_id.to_string();
    let ctx_owned = ctx.clone();
    let result = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id_owned)
            .await
            .map_err(|e| e.to_string())?;
        create_review_push_rows(&db, &job_id_owned, &issue_id_owned, &ctx_owned).await
    });
    match result {
        Ok(recipients) => {
            orch.notifier.emit_change("attention_pushes");
            wake_review_recipients(orch, &recipients);
        }
        Err(e) => log::warn!(
            "review push creation for job {} failed: {}",
            &job_id[..job_id.len().min(8)],
            e
        ),
    }
}

/// The PR-open edge of the review push (CAIRN-1891) — the second of the two edges
/// at which a review fires. The model: a review fires when (producing node
/// quiescent) AND (reviewable work exists), at the moment the *later* of the two
/// becomes true. [`create_review_push_on_turn_end`] is the edge where idle is the
/// later event; this is the edge where the PR opening is.
///
/// A builder that writes a `create-pr` artifact and idles cannot fire the review
/// at its idle edge: the PR is not open yet (branch push + webhook lag), and the
/// create-pr artifact auto-confirms on write (the PR lifecycle is the gate,
/// CAIRN-1219), so neither reviewable arm matches at that instant. Slice B
/// (CAIRN-1882) demoted the PR webhook to state-only, so without this edge the
/// review wake is lost entirely. The webhook is the moment that observes the PR
/// opening, so it fires the review here — gated on the producing node being
/// quiescent (not running a *work* turn — a trailing memory-review turn still
/// counts as quiescent — and not self-suspended on its own work) so a mid-work
/// `synchronize` does not fire, and fingerprint-deduped by the shared row creator
/// so a mergeability-only settle (unchanged diffstat) does not re-wake.
///
/// `issue_id` and `source_branch` come from the merge_request the webhook just
/// updated. The producing builder is resolved by `source_branch`, NOT by
/// `merge_requests.job_id`: the PR is opened by a separate pr-action node, which
/// is blocked while the PR is open and so always reads as non-quiescent — gating
/// on it would never fire (the live CAIRN-1891 bug). Builder and pr-node share the
/// branch, so the builder is the work-producing job on `source_branch`. Shares
/// [`create_review_push_rows`] and [`wake_review_recipients`] with the node-idle
/// edge — one implementation of the fingerprint/dedup/push/wake logic, two
/// trigger edges.
pub async fn create_review_push_for_pr_open(
    orch: &Orchestrator,
    issue_id: &str,
    source_branch: &str,
) {
    let owning = match crate::issues::crud::owning_db_for_issue(&orch.db, issue_id).await {
        Ok(db) => db,
        Err(e) => {
            log::warn!("PR-open review push: issue db resolve failed: {e}");
            return;
        }
    };
    let db = &owning;
    // Resolve the producing BUILDER by the shared branch — not the pr-action node
    // that owns the merge_request. Without a builder there is nothing to gate on,
    // so skip rather than fire blind.
    let builder_job_id = match find_producing_builder_job(db, issue_id, source_branch).await {
        Ok(Some(job_id)) => job_id,
        Ok(None) => {
            log::warn!(
                "PR-open review push: no producing builder on branch {} for issue {}",
                source_branch,
                &issue_id[..issue_id.len().min(8)]
            );
            return;
        }
        Err(e) => {
            log::warn!("PR-open review push: builder lookup failed: {e}");
            return;
        }
    };
    // Quiescence gate on the BUILDER. A builder running a *work* turn is still
    // committing work (the diffstat is in flux), so the review is premature. But a
    // running *memory-review* turn is quiescent-for-review: the work turn already
    // ended and the PR is the reviewable output, and the node-idle edge gates out
    // memory-review turn ends — so on the common build path (create-pr → trailing
    // memory-review turn → `opened` webhook lands while it runs) this edge is the
    // ONLY one that can wake the watcher. Treating a running memory-review turn as
    // busy would lose that wake permanently. A builder self-suspended on its own
    // sub-work is not done either. Fail closed (a lookup error reads as
    // non-quiescent) so a transient read error never fires a spurious wake.
    let active = crate::messages::delivery::head_turn_active(db, &builder_job_id)
        .await
        .unwrap_or(true);
    let memory_review = latest_turn_is_memory_review(orch, &builder_job_id);
    let self_suspended = crate::messages::delivery::head_turn_self_suspended(db, &builder_job_id)
        .await
        .unwrap_or(true);
    log::debug!(
        "PR-open review: issue={} branch={} builder={} active={} memory_review={} self_suspended={}",
        &issue_id[..issue_id.len().min(8)],
        source_branch,
        &builder_job_id[..builder_job_id.len().min(8)],
        active,
        memory_review,
        self_suspended,
    );
    if (active && !memory_review) || self_suspended {
        return;
    }
    let ctx = match crate::orchestrator::attention::read_issue_for_attention(db, issue_id).await {
        Ok(ctx) => ctx,
        Err(e) => {
            log::warn!(
                "PR-open review push: issue context for {} failed: {}",
                &issue_id[..issue_id.len().min(8)],
                e
            );
            return;
        }
    };
    match create_review_push_rows(db, &builder_job_id, issue_id, &ctx).await {
        Ok(recipients) => {
            log::debug!(
                "PR-open review: pushed to {} watcher(s) for issue {}",
                recipients.len(),
                &issue_id[..issue_id.len().min(8)]
            );
            orch.notifier.emit_change("attention_pushes");
            wake_review_recipients(orch, &recipients);
        }
        Err(e) => log::warn!(
            "PR-open review push creation for job {} failed: {}",
            &builder_job_id[..builder_job_id.len().min(8)],
            e
        ),
    }
}

/// The work-producing builder job for a PR, resolved by the shared branch rather
/// than `merge_requests.job_id` (CAIRN-1891). The PR is opened by a separate
/// pr-action node (`merge_requests.job_id`), which is blocked-while-open with a
/// pending turn and so never reads as quiescent; gating on it silently loses the
/// wake. The builder and the pr-node share the branch, so among jobs on
/// `source_branch` the builder is selected as the node that **produced the
/// reviewable artifact** (a create-pr or plan). The pr-action node consumes that
/// artifact and opens the PR but writes none itself, so this deterministically
/// picks the builder even when the pr-node also carries the branch, a worktree,
/// and a (blocked) turn. `None` when no job on the branch exists.
async fn find_producing_builder_job(
    db: &crate::storage::LocalDb,
    issue_id: &str,
    source_branch: &str,
) -> Result<Option<String>, String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        let source_branch = source_branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id FROM jobs j
                     WHERE j.issue_id = ?1
                       AND j.branch = ?2
                     ORDER BY
                       -- The builder is the node that produced the reviewable
                       -- artifact (create-pr/plan); the pr-action node writes
                       -- none, so this excludes it even when it shares the branch
                       -- and carries a blocked turn.
                       CASE WHEN EXISTS (
                         SELECT 1 FROM artifacts a
                         WHERE a.job_id = j.id
                           AND a.artifact_type IN ('create-pr', 'plan')
                       ) THEN 0 ELSE 1 END,
                       -- Then a node with a worktree that ran agent work turns,
                       -- as a secondary guard against pure action nodes.
                       CASE WHEN j.worktree_path IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN EXISTS (SELECT 1 FROM turns t WHERE t.job_id = j.id)
                            THEN 0 ELSE 1 END,
                       j.created_at DESC
                     LIMIT 1",
                    (issue_id.as_str(), source_branch.as_str()),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Create the review push rows for an issue's watchers — the single shared
/// implementation behind both review edges (node-idle and PR-open). Resolves the
/// reviewable `content_ref` + change fingerprint, then pushes a `review:{issue}`
/// to each watcher except the producing node, skipping any watcher whose latest
/// review push already carries this fingerprint (CAIRN-1889 change-trigger;
/// supersede-by-key collapses an undelivered older-fingerprint row). Returns the
/// recipients that received a fresh push so the caller can wake them.
///
/// DB-only and async: the node-idle edge runs it inside `run_db_blocking`; the
/// PR-open webhook edge awaits it directly.
async fn create_review_push_rows(
    db: &crate::storage::LocalDb,
    producing_job_id: &str,
    issue_id: &str,
    ctx: &crate::orchestrator::attention::IssueAttentionContext,
) -> Result<Vec<String>, String> {
    let issue_uri = ctx.issue_uri();
    // Reviewable predicate (disjunction), resolved straight into the content_ref
    // the watcher follows plus a change fingerprint:
    //   arm 1 — an open unmerged PR -> the producing node's `/pr` URI;
    //   arm 2 — a create-pr artifact or unconfirmed plan artifact -> its artifact URI.
    // The second arm is load-bearing at the node-idle edge: at the create-pr idle
    // the PR may not be open yet, but the artifact write is already observable.
    // Neither arm -> no push.
    let Some((content_ref, fingerprint)) = reviewable_ref_and_fingerprint(
        db,
        &ctx.project_key,
        ctx.number,
        issue_id,
        producing_job_id,
    )
    .await?
    else {
        log::debug!(
            "review push: no reviewable ref (job={} issue={})",
            &producing_job_id[..producing_job_id.len().min(8)],
            issue_uri
        );
        return Ok(Vec::new());
    };
    let key = format!("review:{issue_uri}");
    let watchers =
        crate::orchestrator::attention_delivery::subscriber_jobs_for_issue(db, &issue_uri).await?;
    log::debug!(
        "review push: job={} content_ref={} fp={} watchers={}",
        &producing_job_id[..producing_job_id.len().min(8)],
        content_ref,
        fingerprint,
        watchers.len()
    );
    let mut pushed = Vec::new();
    for recipient in watchers {
        // Never push to the producing node itself.
        if recipient == producing_job_id {
            continue;
        }
        // CAIRN-1889 change-trigger: skip when the latest review push to this
        // recipient (delivered OR undelivered) already carries this fingerprint —
        // the reviewable state is unchanged, so re-firing would spuriously
        // re-wake. A changed fingerprint creates a new push; supersede-by-key
        // still collapses an undelivered older-fingerprint row to the newest.
        if let Some(Some(prev)) =
            super::attention_push::latest_push_fingerprint(db, &recipient, &key)
                .await
                .map_err(|e| e.to_string())?
        {
            if prev == fingerprint {
                log::debug!(
                    "review push: recipient {} deduped (fingerprint unchanged)",
                    &recipient[..recipient.len().min(8)]
                );
                continue;
            }
        }
        let (_, effective) = super::attention_push::push_with_fingerprint(
            db,
            &recipient,
            &content_ref,
            Wake::Wake,
            Boundary::Event,
            &key,
            Some(&fingerprint),
        )
        .await
        .map_err(|e| e.to_string())?;
        // A watcher that muted this issue gets a `Passive` review row that rides
        // along on its next run; it is not handed back to `wake_review_recipients`
        // so an idle muted watcher is never woken (CAIRN-1900).
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// Wake each watcher that received a fresh review push so an already-idle one
/// wakes now instead of only seeing the review ride along with an unrelated later
/// wake (CAIRN-1889). `nudge_job_for_urgency` is the shared resume-ladder
/// primitive: an idle recipient resumes; a busy one (a mid-turn agent OR a
/// self-suspended one, both of which read as active) is left alone for the
/// event-boundary push drain or its own-work resume to deliver the push. Steer
/// wakes idle and never stops a busy turn.
fn wake_review_recipients(orch: &Orchestrator, recipients: &[String]) {
    for recipient in recipients {
        if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            recipient,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            log::warn!(
                "review push wake for {} failed: {}",
                &recipient[..recipient.len().min(8)],
                e
            );
        }
    }
}

/// Whether the job's most recent turn was a memory-review turn. The work-turn
/// idle gate for the review push: a trailing `memory_review` turn end is not a
/// work turn and must not create a review push (CAIRN-1882).
fn latest_turn_is_memory_review(orch: &Orchestrator, job_id: &str) -> bool {
    blocking_text_lookup(
        orch,
        job_id,
        "SELECT CASE WHEN start_reason = 'memory_review' THEN '1' ELSE '0' END
         FROM turns WHERE job_id = ?1
         ORDER BY created_at DESC, sequence DESC LIMIT 1",
        TextColumn::Optional,
    )
    .as_deref()
        == Some("1")
}

/// The reviewable content_ref + change fingerprint at a work-turn idle, or `None`
/// when nothing is reviewable (CAIRN-1889). The fingerprint is the change key the
/// creator compares against the latest review push to decide whether to re-fire.
///
/// - Arm 1 (open unmerged PR), branch-precise: scoped to the producing builder's
///   own branch (`jobs.branch == merge_requests.source_branch`) so an unrelated
///   open PR on another branch for the same issue can't drive this node's review.
///   The producing node's `/pr` URI and a head-SHA-or-diffstat fingerprint (see
///   [`open_pr_review_arm`]).
/// - Arm 2 (create-pr artifact or unconfirmed plan artifact): the artifact URI
///   and an `artifact:{version}:{updated_at}` fingerprint.
async fn reviewable_ref_and_fingerprint(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    // Arm 1 is scoped to this builder's own branch. Both edges pass the builder
    // job, whose `jobs.branch` is the PR's `source_branch`, so the open-PR lookup
    // is unambiguous even when an issue carries more than one branch/PR.
    if let Some(source_branch) = job_branch(db, job_id).await? {
        if let Some(arm1) =
            open_pr_review_arm(db, project_key, number, issue_id, &source_branch, job_id).await?
        {
            return Ok(Some(arm1));
        }
    }
    review_artifact_ref(db, project_key, number, job_id).await
}

/// The producing builder's branch (`jobs.branch`), or `None` when unset (a
/// plan-only node has no worktree branch, in which case arm 1 never applies).
async fn job_branch(db: &crate::storage::LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT branch FROM jobs WHERE id = ?1", (job_id.as_str(),))
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(row.opt_text(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Arm 1: an open PR on `source_branch` for the issue, resolved to a content_ref
/// the watcher reviews plus a change fingerprint.
///
/// Reviewability is driven by the `merge_requests` row ALONE — "an open PR on
/// this branch." The content URI is built from the `builder_job_id` we already
/// resolved (cleanly joinable to its execution), NOT from `mr.job_id`: that is
/// the pr-action node, whose execution may not join, and an inner join through it
/// dropped the whole row so a real open PR read as unreviewable (the live
/// CAIRN-1891 failure). The builder node is the right review target anyway. If
/// the builder's node coordinates don't resolve, the content_ref falls back to
/// the issue URI, which still resolves for the drain/render path.
///
/// The fingerprint prefers the PR head commit SHA (`pr:{mr}:sha:{sha}`), which a
/// real new commit always changes and a mergeability-only settle never does; it
/// falls back to the diffstat (`pr:{mr}:{additions}:{deletions}`) when no head
/// SHA has been recorded yet. `None` when no open PR exists on that branch.
async fn open_pr_review_arm(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    source_branch: &str,
    builder_job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    let builder_job_id = builder_job_id.to_string();
    #[allow(clippy::type_complexity)]
    let resolved: Option<(
        String,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
    )> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            let source_branch = source_branch.clone();
            let builder_job_id = builder_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT mr.id, mr.additions, mr.deletions, mr.head_sha,
                                e.seq, j.uri_segment, parent.uri_segment
                         FROM merge_requests mr
                         LEFT JOIN jobs j ON j.id = ?3
                         LEFT JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                         WHERE mr.issue_id = ?1 AND mr.source_branch = ?2
                           AND mr.status = 'open'
                         ORDER BY mr.updated_at DESC
                         LIMIT 1",
                        (
                            issue_id.as_str(),
                            source_branch.as_str(),
                            builder_job_id.as_str(),
                        ),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.text(0)?,
                        row.opt_i64(1)?,
                        row.opt_i64(2)?,
                        row.opt_text(3)?,
                        row.opt_i64(4)?,
                        row.opt_text(5)?,
                        row.opt_text(6)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(resolved.map(
        |(mr_id, additions, deletions, head_sha, seq, uri_segment, parent_segment)| {
            let content_ref = match (seq, uri_segment) {
                (Some(seq), Some(node)) => match parent_segment {
                    Some(parent) => cairn_common::uri::build_task_artifact_uri_named(
                        project_key,
                        number,
                        seq as i32,
                        &parent,
                        &node,
                        None,
                    ),
                    None => cairn_common::uri::build_node_artifact_uri_named(
                        project_key,
                        number,
                        seq as i32,
                        &node,
                        None,
                    ),
                },
                _ => format!("cairn://p/{project_key}/{number}"),
            };
            let fingerprint = match head_sha {
                Some(sha) => format!("pr:{mr_id}:sha:{sha}"),
                None => {
                    let fmt =
                        |n: Option<i64>| n.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
                    format!("pr:{mr_id}:{}:{}", fmt(additions), fmt(deletions))
                }
            };
            (content_ref, fingerprint)
        },
    ))
}

/// The producing job's create-pr or unconfirmed plan artifact, resolved to its
/// node-artifact URI plus an `artifact:{version}:{updated_at}` change fingerprint
/// (CAIRN-1889), or `None` when the job has no such artifact. Arm 2 of the
/// review-push reviewable predicate. `create-pr` remains reviewable even when it
/// is already confirmed because the PR lifecycle auto-confirms that artifact
/// before every deployment shape has observed the PR-open edge.
async fn review_artifact_ref(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let job_id = job_id.to_string();
    #[allow(clippy::type_complexity)]
    let resolved: Option<(i64, String, Option<String>, Option<String>, i64, i64)> = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT e.seq, j.uri_segment, parent.uri_segment, a.output_name, a.version, a.updated_at
                         FROM artifacts a
                         JOIN jobs j ON a.job_id = j.id
                         JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                         WHERE a.job_id = ?1
                           AND (a.artifact_type = 'create-pr'
                                OR (a.artifact_type = 'plan' AND a.confirmed = 0))
                         ORDER BY a.version DESC
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.i64(0)?,
                        row.text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.i64(4)?,
                        row.i64(5)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    // A sub-agent task job nests its artifact under the parent node
    // (`.../{parent}/task/{task}/...`); a top-level node uses the flat node URI.
    // The flat form does not resolve for a sub-task (issue #143).
    Ok(resolved.map(
        |(seq, node, parent_segment, output_name, version, updated_at)| {
            let uri = match parent_segment {
                Some(parent) => cairn_common::uri::build_task_artifact_uri_named(
                    project_key,
                    number,
                    seq as i32,
                    &parent,
                    &node,
                    output_name.as_deref(),
                ),
                None => cairn_common::uri::build_node_artifact_uri_named(
                    project_key,
                    number,
                    seq as i32,
                    &node,
                    output_name.as_deref(),
                ),
            };
            (uri, format!("artifact:{version}:{updated_at}"))
        },
    ))
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
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
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
            // Turn-end project checks (when:idle/when:review), detached so the
            // suite never blocks the turn from ending.
            spawn_turn_end_checks(orch, &job_id);
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
    // Self-suspend: do NOT reap the run's inline commands and do NOT hard-kill
    // on interrupt failure. A durable wait is a warm park, not user Stop; the run
    // must remain resumable when the awaited dependency resolves.
    stop_session_internal(orch, run_id, false, InterruptFailurePolicy::WarmAnyway)?;
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
        // Fire the end-step when the job has finished its real work (the
        // declared output artifact exists) and either captured drafts to review
        // (any run, tasks included) or is a top-level node job worth a
        // reflection nudge.
        None if state.has_output_artifact && (state.draft_count > 0 || !state.is_task) => {
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
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    let _ = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
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

/// True when the run belongs to a delegated task (its job has a `parent_job_id`).
///
/// Mirrors `is_task_spawned_run` (`backends/run_state.rs`), which is
/// `pub(in crate::backends)` and so not reachable from `orchestrator`. A
/// delegated task is the only kind of run with a suspended parent blocked on
/// the resume gate, so it is the only kind whose genuine turn failure must be
/// finalized terminally rather than left resumable.
fn run_is_delegated_task(orch: &Orchestrator, run_id: &str) -> bool {
    blocking_text_lookup(
        orch,
        run_id,
        "SELECT jobs.parent_job_id
         FROM runs
         JOIN jobs ON runs.job_id = jobs.id
         WHERE runs.id = ?1
           AND jobs.parent_job_id IS NOT NULL",
        TextColumn::Optional,
    )
    .is_some()
}

/// Finalize a run that hit an unrecoverable backend turn failure.
///
/// For a delegated TASK run (the job has a `parent_job_id`), the turn is marked
/// terminally `Failed` so the job derives `Failed`, the delegated packet
/// resolves `Failed`, and the suspended parent resumes with the error instead
/// of hanging in `running` forever. For a top-level JOB run, the failure is a
/// resumable interruption (the existing `finalize_run(Crashed)` path,
/// unchanged): a job can be resumed by the user or by re-advancement, and only
/// a task has a blocked parent that needs a terminal answer.
///
/// Backends call this on a *genuinely fatal* turn failure (an `Err` from the
/// owned loop, an unrecoverable Codex error). Recoverable crashes (rate limits,
/// process death) keep calling `finalize_run(Crashed)` directly and stay
/// resumable regardless of task-vs-job.
pub fn fail_run(orch: &Orchestrator, run_id: &str, reason: &str) {
    if !run_is_delegated_task(orch, run_id) {
        // Top-level job: keep the resumable-interrupt behavior unchanged.
        finalize_run(orch, run_id, RunStatus::Crashed);
        return;
    }

    // Mark the live turn terminally Failed before finalize. `apply_turn_outcome`
    // accepts Running or Pending; once the turn is `failed` (terminal),
    // `finalize_run`'s `turn_state == "running"` branch is false and its `else`
    // re-applies Failed as a no-op (`from == outcome`).
    let turn_id = orch
        .process_state
        .get_current_turn_id(run_id)
        .or_else(|| current_turn_id_for_run(orch, run_id));
    if let Some(turn_id) = turn_id {
        if let Err(e) = apply_turn_outcome(orch, &turn_id, TurnState::Failed) {
            log::warn!(
                "fail_run: failed to mark turn {} as Failed for run {}: {}",
                turn_id,
                run_id,
                e
            );
        }
    }
    let _ = set_exit_reason(orch, run_id, reason);
    finalize_run(orch, run_id, RunStatus::Crashed);
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
            // Turn-end project checks (when:idle/when:review), detached so the
            // suite never blocks the turn from ending. Mirrors the warm-transition
            // turn-end caller above.
            spawn_turn_end_checks(orch, &job_id);
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
    let dbs = orch.db.clone();
    let log_run_id = run_id.to_string();
    let run_id = run_id.to_string();
    let run_info = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
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

    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    let row = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
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
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
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
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
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
        let _ = stop_session_internal(orch, child_run_id, true, InterruptFailurePolicy::HardKill);
    }

    // Stop the requested run
    stop_session_internal(orch, run_id, true, InterruptFailurePolicy::HardKill)
}

/// Marker recorded as the response of an `ask_user` prompt cancelled by a
/// job-level stop, so the pending prompt no longer counts toward the issue's
/// NeedsInput attention. The agent never reads it — the turn is terminalized and
/// the user starts a fresh turn with a follow-up (CAIRN-1907, Option A).
const STOP_CANCELLED_PROMPT_RESPONSE: &str = "[cancelled by stop]";

/// Job-level stop for a suspended/waiting job that has no live run attached.
///
/// Run-scoped [`stop_session`] needs a `run_id` to interrupt and park warm, but a
/// job that suspended on a foreground question or an inline delegated task can
/// finalize its run (the OpenRouter owned loop keeps no warm process) and rest in
/// a non-terminal state with NO run to attach to. Pressing Stop there used to fail
/// with "no active run". This idles the job from its id directly:
///
/// 1. A live run IS attached -> defer entirely to the run-scoped path, leaving the
///    existing warm-park behavior unchanged.
/// 2. Otherwise fully idle the job (Option A): cascade-stop every descendant child
///    run, cancel any open prompt, and terminalize the live
///    (`pending`/`running`/`yielded`) turns — which drops a pending delegated
///    successor and ends the yielded work turn — then recompute so the projection
///    reflects a steerable, no-longer-waiting state. The user can immediately send
///    a follow-up that starts a fresh turn.
pub fn stop_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    // A live run is attached: the run-scoped path is the unchanged behavior.
    if let Some(run_id) = live_run_id_for_job(orch, job_id) {
        return stop_session(orch, &run_id);
    }

    // No run to attach to. Cascade-stop descendant child runs first, mirroring
    // the child cascade `stop_session` performs from a run id.
    for child_run_id in descendant_running_run_ids_for_job(orch, job_id) {
        log::info!(
            "Stopping child run {} (job-level stop of {})",
            child_run_id,
            job_id
        );
        let _ = stop_session_internal(orch, &child_run_id, true, InterruptFailurePolicy::HardKill);
    }

    // Cancel open input and terminalize the job's live turns (drops a pending
    // delegated successor), then recompute the projection.
    cancel_open_input_and_live_turns_for_job(orch, job_id)?;
    if let Err(error) = crate::execution::advancement::recompute_job(orch, job_id) {
        log::warn!("Failed to recompute job {job_id} after job-level stop: {error}");
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "update"}),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );

    Ok(())
}

/// Running (`starting`/`live`) run ids for every descendant job of `job_id`. The
/// job-level analogue of [`child_run_ids_for_run`], resolved from a job id rather
/// than a run id (a suspended job may have no run of its own).
fn descendant_running_run_ids_for_job(orch: &Orchestrator, job_id: &str) -> Vec<String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
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

/// Cancel any open `ask_user` prompt on the job and terminalize its live
/// (`pending`/`running`/`yielded`) turns in one write. Cancelling the open prompt
/// clears the issue's NeedsInput attention; terminalizing the live turns drops a
/// pending delegated successor and ends the yielded work turn so the job's latest
/// turn is no longer live and it recomputes to a steerable state.
fn cancel_open_input_and_live_turns_for_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<(), String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                conn.execute(
                    "UPDATE prompts
                     SET response = ?1, answered_at = ?2
                     WHERE response IS NULL
                       AND turn_id IN (SELECT id FROM turns WHERE job_id = ?3)",
                    (STOP_CANCELLED_PROMPT_RESPONSE, now, job_id.as_str()),
                )
                .await?;
                conn.execute(
                    "UPDATE turns
                     SET state = 'cancelled', ended_at = ?1, updated_at = ?1
                     WHERE job_id = ?2 AND state IN ('pending', 'running', 'yielded')",
                    (now, job_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InterruptFailurePolicy {
    /// External Stop must not leave a still-running backend parked as warm when
    /// interrupt delivery genuinely failed.
    HardKill,
    /// Durable waits are self-suspends; even if the best-effort interrupt fails,
    /// the process must remain warm for the eventual resume.
    WarmAnyway,
}

/// Internal stop without cascading (used by cascading stop)
///
/// Sends an interrupt control request via stdin and transitions the process
/// to warm state. The process is NOT killed - it stays available for follow-up
/// messages.
///
/// `reap_inline` decides the fate of the run's foreground (inline) shell
/// commands. An external stop (user Stop, cascade) passes `true` to kill them.
/// A *self*-suspend for a durable wait passes `false`: the run paused itself on
/// a dependency it raised mid-batch, and in a parallel `run()` the suspending
/// item's still-executing siblings must keep running so `handle_run` can collect
/// their outcomes before returning the suspend marker (the whole batch re-runs
/// on resume). Reaping them here would kill siblings mid-flight (CAIRN-2123).
fn stop_session_internal(
    orch: &Orchestrator,
    run_id: &str,
    reap_inline: bool,
    interrupt_failure_policy: InterruptFailurePolicy,
) -> Result<(), String> {
    // Interrupt/cancel the current turn. Fall back to the DB current_turn_id so
    // stop repairs stale UI-visible state even when the process map lost the run.
    stop_active_turn_for_run(orch, run_id);

    // Send interrupt via backend-aware stdin handler. A successful Codex
    // `turn/interrupt` response is deferred until the turn is actually aborted.
    if let Err(e) = crate::backends::stdin::send_interrupt(&orch.process_state, run_id) {
        if e.starts_with("Process not found:") {
            log::warn!(
                "Run {} missing from process map during stop; reconciling stale DB state",
                run_id
            );
        } else if interrupt_failure_policy == InterruptFailurePolicy::HardKill {
            log::warn!(
                "Failed to send interrupt to run {}; falling back to hard termination: {}",
                run_id,
                e
            );
            return kill_session_with_reason(orch, run_id, "user_stop");
        } else {
            log::warn!(
                "Failed to send interrupt to run {}; preserving warm durable-wait state: {}",
                run_id,
                e
            );
        }
    }

    // Only kill foreground bash processes — background terminals survive the
    // interrupt regardless. A self-suspend leaves the inline siblings alone.
    if reap_inline {
        cleanup_inline_commands(orch, run_id);
    }

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
            crate::mcp::handlers::terminal::finalize_terminal_by_session_id(orch, &session_id)
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
    use super::{finish_memory_review_if_due, review_artifact_ref};
    use crate::agent_process::process::{wrap_plain_stdin, RunHandle};
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use std::sync::{Arc, Mutex};

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let db = LocalDb::open(root.join("memory-review.db")).await.unwrap();
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
                    "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at) VALUES ('s-review','j-review','open','backend-review',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET current_session_id = 's-review' WHERE id = 'j-review'",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id, created_at, updated_at) VALUES ('run-review','i-review','p-review','j-review','live','s-review',1,1)",
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

    /// Seed `count` recorded events on `run-review`. The no-drafts reflection
    /// nudge is gated on event activity (`events > REFLECTION_ACTIVITY_THRESHOLD`),
    /// not turn count, so a node job needs enough events to clear the threshold.
    async fn insert_events_for_review_run(db: &LocalDb, count: i64) {
        db.write(move |conn| {
            Box::pin(async move {
                for seq in 0..count {
                    conn.execute(
                        "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
                         VALUES (?1, 'run-review', ?2, 1, 'assistant', '{}', 1)",
                        (format!("ev-{seq}"), seq),
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

    /// A sub-agent task's review artifact ref must nest under the parent node
    /// (`.../{parent}/task/{task}/...`) so the wake's detail URI resolves; the
    /// old code emitted a flat top-level node URI naming the task's own segment,
    /// which does not resolve to the sub-task job (issue #143).
    #[tokio::test]
    async fn review_artifact_ref_nests_subtask_artifact_under_parent_task() {
        use cairn_common::uri::{parse_uri, CairnResource};

        // Top-level node job: the flat node artifact URI (unchanged behavior).
        let db = test_db().await;
        seed_job_row(&db, None, false).await;
        insert_artifact(&db).await;
        let (node_uri, _fp) = review_artifact_ref(&db, "PRJ", 2, "j-review")
            .await
            .unwrap()
            .expect("artifact ref for node job");
        assert_eq!(node_uri, "cairn://p/PRJ/2/1/builder/create-pr");
        assert!(
            matches!(
                parse_uri(&node_uri),
                Some(CairnResource::NodeArtifact { ref node_id, .. }) if node_id == "builder"
            ),
            "top-level node artifact URI should parse to NodeArtifact: {node_uri}"
        );

        // Sub-agent task job: the artifact nests under the parent node and
        // parses to a TaskArtifact that resolves through the read path.
        let task_db = test_db().await;
        seed_job_row(&task_db, None, true).await;
        insert_artifact(&task_db).await;
        let (task_uri, _fp) = review_artifact_ref(&task_db, "PRJ", 2, "j-review")
            .await
            .unwrap()
            .expect("artifact ref for task job");
        assert_eq!(
            task_uri,
            "cairn://p/PRJ/2/1/coordinator/task/builder/create-pr"
        );
        assert!(
            task_uri.contains("/task/"),
            "sub-task URI must carry /task/"
        );
        match parse_uri(&task_uri) {
            Some(CairnResource::TaskArtifact {
                node_id,
                task_name,
                name,
                ..
            }) => {
                assert_eq!(node_id, "coordinator");
                assert_eq!(task_name, "builder");
                assert_eq!(name.as_deref(), Some("create-pr"));
            }
            other => panic!("expected TaskArtifact, got {other:?}"),
        }

        // The old flat form named the task's own segment as a top-level node;
        // that URI cannot resolve to the sub-task job.
        let broken = cairn_common::uri::build_node_artifact_uri_named(
            "PRJ",
            2,
            1,
            "builder",
            Some("create-pr"),
        );
        assert_eq!(broken, "cairn://p/PRJ/2/1/builder/create-pr");
        assert_ne!(broken, task_uri);
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
        assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 1);
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
                       AND i.title LIKE 'Memory triage: project=P%'
                       AND e.recipe_id = 'memory-triage'",
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap(),
            1
        );
    }

    async fn direct_push_count(orch: &crate::orchestrator::Orchestrator, like: &str) -> i64 {
        let like = like.to_string();
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*)
                 FROM attention_pushes p
                 JOIN messages m ON p.key = 'direct:' || m.id
                 WHERE p.recipient = 'j-review'
                   AND p.delivered_event_id IS NULL
                   AND m.channel_type = 'direct'
                   AND m.recipient_run_id = 'run-review'
                   AND m.content LIKE ?1",
                (like,),
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    async fn memory_review_turn_count(orch: &crate::orchestrator::Orchestrator) -> i64 {
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM turns WHERE job_id = 'j-review' AND start_reason = 'memory_review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    async fn latest_memory_review_turn_state(orch: &crate::orchestrator::Orchestrator) -> String {
        orch.db
            .local
            .query_one(
                "SELECT state FROM turns WHERE job_id = 'j-review' AND start_reason = 'memory_review' ORDER BY sequence DESC LIMIT 1",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    fn register_warm_process(orch: &crate::orchestrator::Orchestrator) {
        let mut processes = orch.process_state.processes.lock().unwrap();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(Some(wrap_plain_stdin(Box::new(
            Vec::<u8>::new(),
        )))));
        let mut handle = RunHandle::new(
            child,
            stdin,
            Some("s-review".to_string()),
            Some("j-review".to_string()),
        );
        handle.transition_to_warm();
        processes.register("run-review".to_string(), handle);
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
    async fn null_no_drafts_node_job_substantial_activity_sends_reflection() {
        let db = test_db().await;
        seed_job_row(&db, None, false).await;
        insert_artifact(&db).await;
        insert_run_session_with_turns(&db, 11).await;
        // The no-drafts reflection nudge is gated on event activity (events > 50),
        // not turn count, so seed enough events to clear the threshold.
        insert_events_for_review_run(&db, 60).await;
        let orch = test_orchestrator(db);

        finish_memory_review_if_due(&orch, "j-review", "run-review");

        assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
        // A reflection prompt went out, and it is not the draft-review variant.
        assert_eq!(direct_push_count(&orch, "%reflect%").await, 1);
        assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 0);
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
        assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 1);
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
    async fn idle_flush_creates_memory_review_turn_from_pending_push() {
        let db = test_db().await;
        seed_job(&db, None).await;
        insert_artifact(&db).await;
        insert_run_session_with_turns(&db, 1).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch);

        finish_memory_review_if_due(&orch, "j-review", "run-review");
        assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 1);
        assert!(
            crate::orchestrator::attention_push::has_pending_waking_live(
                &orch.db.local,
                "j-review"
            )
            .await
            .unwrap()
        );
        crate::messages::delivery::flush_pending_directs_on_idle(&orch, "run-review");

        assert_eq!(memory_review_turn_count(&orch).await, 1);
        assert!(matches!(
            latest_memory_review_turn_state(&orch).await.as_str(),
            "pending" | "running"
        ));
    }

    #[tokio::test]
    async fn stranded_sent_reconcile_resumes_once() {
        let db = test_db().await;
        seed_job(&db, Some("sent")).await;
        insert_artifact(&db).await;
        insert_run_session_with_turns(&db, 1).await;
        db.execute(
            "INSERT INTO messages (id, channel_type, sender_name, recipient_run_id, content, created_at)
             VALUES ('msg-review', 'direct', 'system', 'run-review', 'Before you finish, review the memories you captured during this job. Draft memories:', 2)",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        register_warm_process(&orch);
        let first =
            crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()).unwrap();
        let second =
            crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()).unwrap();

        assert_eq!(first, 1);
        assert_eq!(second, 0);
        assert_eq!(memory_review_turn_count(&orch).await, 1);
    }

    #[tokio::test]
    async fn stranded_sent_reconcile_preserves_queued_user_followup() {
        let db = test_db().await;
        seed_job(&db, Some("sent")).await;
        insert_artifact(&db).await;
        insert_run_session_with_turns(&db, 1).await;
        db.execute(
            "INSERT INTO messages (id, channel_type, sender_name, recipient_run_id, content, created_at)
             VALUES ('msg-review', 'direct', 'system', 'run-review', 'Before you finish, review the memories you captured during this job. Draft memories:', 2)",
            (),
        )
        .await
        .unwrap();
        crate::messages::queued::enqueue_async(
            &db,
            "j-review",
            "user has a follow-up",
            crate::messages::queued::Delivery::Queue,
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        register_warm_process(&orch);

        let resumed =
            crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()).unwrap();

        assert_eq!(resumed, 0);
        assert_eq!(memory_review_turn_count(&orch).await, 0);
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
    use super::{
        finalize_run, stop_job, stop_session_internal, suspend_run_for_durable_wait,
        transition_to_warm_state, InterruptFailurePolicy,
    };
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

    fn register_process_without_stdin(orch: &Orchestrator, run_id: &str, job_id: Option<&str>) {
        let mut processes = orch.process_state.processes.lock().unwrap();
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(None)),
            Some(format!("sess-{run_id}")),
            job_id.map(str::to_string),
        );
        handle.begin_turn("turn-without-stdin");
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
    async fn stop_hard_terminates_instead_of_warming_when_interrupt_delivery_fails() {
        let db = test_db().await;
        let orch = test_orchestrator(db);
        register_process_without_stdin(&orch, "run-no-stdin", Some("job-no-stdin"));

        stop_session_internal(
            &orch,
            "run-no-stdin",
            true,
            InterruptFailurePolicy::HardKill,
        )
        .expect("hard-stop fallback should complete");

        assert!(
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .get("run-no-stdin")
                .is_none(),
            "failed interrupt delivery must not leave the process parked warm"
        );
    }

    #[tokio::test]
    async fn durable_wait_preserves_warm_process_when_interrupt_delivery_fails() {
        let db = test_db().await;
        let orch = test_orchestrator(db);
        register_process_without_stdin(&orch, "run-suspend", Some("job-suspend"));

        suspend_run_for_durable_wait(&orch, "run-suspend", "delegated_wait_suspended")
            .expect("durable wait suspend should tolerate interrupt failure");

        let processes = orch.process_state.processes.lock().unwrap();
        let handle = processes
            .get("run-suspend")
            .expect("durable wait must keep the process registered for resume");
        assert!(
            handle.is_warm(),
            "durable wait must park the process warm instead of hard-killing it"
        );
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

    /// Seed a parent suspended on a delegated wait (anchor turn + pending
    /// `dependency_unblock` successor) and a delegated child that is still
    /// *running* — a live run, a `running` job, and a `running` current turn.
    /// This is the durable-suspend shape: the parent finalized its own run and
    /// is blocked on the resume gate, waiting on this child. A genuine turn
    /// failure of the child must fail it terminally and fire the gate.
    async fn seed_suspended_parent_with_running_child(db: &LocalDb, snapshot: String) {
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
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                // Parent job stays `running` while suspended on the delegated
                // wait (it is the issue that is `waiting`, not the job). A valid
                // JobStatus matters: the execution sweep loads every job in the
                // execution and parses its status.
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','running','succ',1,1)", ()).await?;
                // Delegated child still running: live run + running current turn.
                // It carries a recipe_node_id for its synthetic node so the
                // execution sweep recomputes its status (production shape).
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-child','e-warm','child-node','j-parent','i-warm','p-warm','running',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('child-turn','s-child','j-child',1,'running','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id = 'child-turn' WHERE id = 'j-child'", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    }

    fn running_child_snapshot() -> String {
        // The delegated child materializes as a synthetic Agent node in the
        // execution snapshot, which is what makes the execution sweep recompute
        // its status from its turn outcome. A bare node with no edges is
        // dag-ready and has no downstream artifact contract.
        let child_node = serde_json::json!({
            "id": "child-node",
            "nodeType": "agent",
            "name": "Child",
            "position": {"x": 0.0, "y": 0.0},
            "parentId": null,
            "triggerConfig": null,
            "agentConfig": null,
            "actionConfig": null,
            "checkpointConfig": null,
            "artifactConfig": null,
            "conditionConfig": null,
            "contextConfig": null
        });
        serde_json::json!({
            "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [child_node], "edges": []},
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
                "status": "running",
                "taskIndex": 0,
                "createdAt": 0
            }],
            "createdAt": 0
        })
        .to_string()
    }

    /// The durable-suspend hang, fixed: a genuine turn failure of a *running*
    /// delegated child finalizes it terminally `Failed` (not a resumable
    /// interrupt), so its packet resolves `Failed` and the suspended parent's
    /// resume gate fires (the pending successor is claimed). Before the fix the
    /// child's turn was interrupted, the packet stayed `Materialized`, and the
    /// parent hung in `running` forever.
    #[tokio::test]
    async fn fail_run_on_delegated_task_resumes_suspended_parent() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));

        assert_eq!(turn_state(&orch, "succ").await, "pending");

        super::fail_run(&orch, "run-child", "turn_failed");

        // The child's turn is terminally Failed (not interrupted).
        assert_eq!(turn_state(&orch, "child-turn").await, "failed");
        // The child job derives Failed from the failed turn.
        assert_eq!(
            scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
                .await
                .as_deref(),
            Some("failed"),
        );
        // The resume gate fired: the parent's pending successor was claimed.
        assert_eq!(turn_state(&orch, "succ").await, "complete");
    }

    /// A genuine turn failure of a *top-level* job (no `parent_job_id`) stays a
    /// resumable interruption — `fail_run` falls back to the unchanged
    /// `finalize_run(Crashed)` path: the running turn is interrupted, not
    /// failed, and the job is not driven terminal. Only a delegated task has a
    /// blocked parent that needs a terminal answer.
    #[tokio::test]
    async fn fail_run_on_top_level_job_stays_resumable() {
        let db = test_db().await;
        seed_top_level_run(&db, "run-top", "job-top", "turn-top", "none").await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-top", Some("job-top"));
        orch.process_state
            .set_current_turn_id("run-top", Some("turn-top"));

        super::fail_run(&orch, "run-top", "turn_failed");

        // The running turn is interrupted (resumable), not failed.
        assert_eq!(turn_state(&orch, "turn-top").await, "interrupted");
        // The job is not terminally failed.
        assert_ne!(
            scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "job-top")
                .await
                .as_deref(),
            Some("failed"),
        );
    }

    async fn scalar_text(orch: &Orchestrator, sql: &'static str, id: &str) -> Option<String> {
        let id = id.to_string();
        orch.db
            .local
            .read(move |conn| {
                let id = id.clone();
                Box::pin(async move {
                    let mut rows = conn.query(sql, (id.as_str(),)).await?;
                    match rows.next().await? {
                        Some(row) => row.opt_text(0),
                        None => Ok(None),
                    }
                })
            })
            .await
            .unwrap()
    }

    /// Seed a runless suspended parent: a `running` job with no live run of its
    /// own, a yielded work turn plus a pending `dependency_unblock` successor, an
    /// open `ask_user` prompt, and a delegated child with a live run. This is the
    /// shape a job rests in after suspending on a foreground question or inline
    /// task when its run finalized (the OpenRouter owned loop keeps no warm
    /// process). Standalone (no execution) so the recompute takes the simple path.
    async fn seed_runless_suspended_job(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-sj','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-sj','w-sj','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-sj','p-sj',7,'T','waiting','needs_input',1,1)", ()).await?;
                // Yielded work turn + the pending successor it points at. Inserted
                // before the job because `jobs.current_turn_id` references it.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('sj-anchor','s-sj','j-sj-parent',1,'yielded','awaiting_input','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('sj-succ','s-sj','j-sj-parent',2,'sj-anchor','pending','dependency_unblock',1,1)", ()).await?;
                // Parent job: running, no live run (its prior run exited).
                conn.execute("INSERT INTO jobs (id, issue_id, project_id, status, node_name, current_turn_id, created_at, updated_at) VALUES ('j-sj-parent','i-sj','p-sj','running','builder','sj-succ',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('r-sj-parent','j-sj-parent','i-sj','p-sj','exited',1,1)", ()).await?;
                // Open ask_user prompt on the work turn.
                conn.execute("INSERT INTO prompts (id, run_id, turn_id, questions, response, created_at) VALUES ('sj-prompt','r-sj-parent','sj-anchor','[]',NULL,1)", ()).await?;
                // Delegated child still running.
                conn.execute("INSERT INTO jobs (id, parent_job_id, issue_id, project_id, status, node_name, created_at, updated_at) VALUES ('j-sj-child','j-sj-parent','i-sj','p-sj','running','task',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('r-sj-child','j-sj-child','i-sj','p-sj','live',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    }

    #[tokio::test]
    async fn stop_job_idles_runless_suspended_job_and_stops_children() {
        let db = test_db().await;
        seed_runless_suspended_job(&db).await;
        let orch = test_orchestrator(db);

        stop_job(&orch, "j-sj-parent").unwrap();

        // The open prompt is cancelled (no longer counts toward NeedsInput).
        assert!(
            scalar_text(
                &orch,
                "SELECT response FROM prompts WHERE id = ?1",
                "sj-prompt"
            )
            .await
            .is_some(),
            "open prompt should be answered/cancelled"
        );
        // The yielded work turn and the pending successor are both terminalized,
        // so the job's latest turn is no longer live (drops the pending successor).
        assert_eq!(turn_state(&orch, "sj-anchor").await, "cancelled");
        assert_eq!(turn_state(&orch, "sj-succ").await, "cancelled");
        // The delegated child's live run is stopped.
        assert_ne!(
            scalar_text(&orch, "SELECT status FROM runs WHERE id = ?1", "r-sj-child")
                .await
                .as_deref(),
            Some("live"),
            "child run should no longer be live"
        );
    }
}

/// CAIRN-1882: the work-turn idle edge is the single creator of a review push.
/// These cover the canonical scenarios from `docs/attention-redesign.md`: a
/// reviewable work-turn idle pushes exactly one `review:{issue}` to each watcher
/// (never to the producing node), a memory-review turn end does not, an idle with
/// no reviewable output does not, and successive idles supersede to one
/// undelivered row.
#[cfg(test)]
mod review_push_tests {
    use super::{
        create_review_push_for_pr_open, create_review_push_on_turn_end, issue_for_attention_by_job,
    };
    use crate::db::DbState;
    use crate::orchestrator::attention_push::{
        list_pending, stamp_delivered, Boundary, Push, Wake,
    };
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    const ISSUE_URI: &str = "cairn://p/PRJ/7";
    const REVIEW_KEY: &str = "review:cairn://p/PRJ/7";

    async fn test_db() -> LocalDb {
        crate::storage::migrated_test_db("review-push.db").await
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    /// Producing builder node `j-prod` (issue `i-rev` / `cairn://p/PRJ/7`, exec
    /// seq 1) whose just-ended turn carries `start_reason`, a watcher job
    /// `j-watch`, and an active issue subscription for BOTH so the producing
    /// node's self-exclusion is exercised.
    async fn seed(db: &LocalDb, start_reason: &str) {
        db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p-rev','w','Project','PRJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i-rev','p-rev',7,'Rev','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e-rev','r','i-rev','p-rev','running',1,1);
            INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment, node_name, branch, worktree_path, created_at, updated_at)
              VALUES('j-prod','e-rev','p-rev','i-rev','complete','builder','builder','b','/tmp/wt',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, node_name, created_at, updated_at)
              VALUES('j-watch','p-rev','i-rev','running','watcher',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('r-prod','p-rev','j-prod','i-rev',1,1);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
              VALUES('t-prod','s-prod','j-prod',1,'complete','{start_reason}',1,1);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-watch','j-watch','issue','{ISSUE_URI}','active','agent',1,1,0);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-prod','j-prod','issue','{ISSUE_URI}','active','agent',1,1,0);
            "
        ))
        .await
        .unwrap();
    }

    async fn insert_open_pr(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-rev','j-prod','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
    }

    async fn insert_artifact(db: &LocalDb, artifact_type: &str, confirmed: i64) {
        db.execute_script(&format!(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-rev','j-prod','{artifact_type}',1,'{{}}',1,'{artifact_type}',{confirmed},1,1);"
        ))
        .await
        .unwrap();
    }

    async fn pending(orch: &Orchestrator, recipient: &str) -> Vec<Push> {
        list_pending(&orch.db.local, recipient).await.unwrap()
    }

    fn run_review_push(orch: &Orchestrator) {
        let (issue_id, ctx) =
            issue_for_attention_by_job(orch, "j-prod").expect("issue context for producing job");
        create_review_push_on_turn_end(orch, "j-prod", &issue_id, &ctx);
    }

    async fn run_pr_open(orch: &Orchestrator) {
        // The webhook passes (issue_id, source_branch); the builder is resolved
        // from the branch inside the creator. `insert_open_pr` uses branch 'b',
        // matching the seeded builder j-prod's branch.
        create_review_push_for_pr_open(orch, "i-rev", "b").await;
    }

    #[tokio::test]
    async fn work_idle_with_unconfirmed_create_pr_artifact_pushes_review() {
        // The create-pr idle: the PR is not open yet, but the unconfirmed
        // artifact is observable -> the second predicate arm fires.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_artifact(&db, "create-pr", 0).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);

        let watcher = pending(&orch, "j-watch").await;
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].key, REVIEW_KEY);
        assert!(watcher[0].content_ref.contains("/builder/"));
        // The producing node is never a recipient of its own review.
        assert!(pending(&orch, "j-prod").await.is_empty());
    }

    #[tokio::test]
    async fn work_idle_with_open_pr_pushes_review() {
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);

        let watcher = pending(&orch, "j-watch").await;
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].key, REVIEW_KEY);
        assert!(watcher[0]
            .content_ref
            .starts_with("cairn://p/PRJ/7/1/builder"));
        assert!(pending(&orch, "j-prod").await.is_empty());
    }

    #[tokio::test]
    async fn memory_review_turn_end_creates_no_review_push() {
        // The gate: a trailing memory-review turn is not a work turn, so even an
        // open PR yields no review push.
        let db = test_db().await;
        seed(&db, "memory_review").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);

        assert!(pending(&orch, "j-watch").await.is_empty());
    }

    #[tokio::test]
    async fn work_idle_with_confirmed_create_pr_artifact_pushes_review() {
        // CAIRN-1999 shape: the create-pr artifact was already confirmed by the
        // artifact lifecycle, but the parent still needs a review push for the
        // child output even if no PR-open edge creates one.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_artifact(&db, "create-pr", 1).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);

        let watcher = pending(&orch, "j-watch").await;
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].key, REVIEW_KEY);
        assert!(watcher[0].content_ref.contains("/builder/"));
        assert!(pending(&orch, "j-prod").await.is_empty());
    }

    #[tokio::test]
    async fn work_idle_without_reviewable_output_no_push() {
        // A work turn with neither an open PR nor a create-pr/unconfirmed-plan
        // artifact -> nothing reviewable.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_artifact(&db, "plan", 1).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);

        assert!(pending(&orch, "j-watch").await.is_empty());
    }

    #[tokio::test]
    async fn successive_work_idles_collapse_to_one_undelivered() {
        // Two work-turn idles with the SAME reviewable state and no delivery in
        // between yield one undelivered review row: the first creates it, the
        // second is skipped by the change-trigger (CAIRN-1889) because the
        // undelivered push already carries the same fingerprint.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);
        run_review_push(&orch);

        assert_eq!(pending(&orch, "j-watch").await.len(), 1);
    }

    #[tokio::test]
    async fn unchanged_fingerprint_skips_review_even_after_delivery() {
        // One review fires (fp=A); after it is delivered, a second work-turn idle
        // with the SAME reviewable state must NOT re-create a review push.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);
        let first = pending(&orch, "j-watch").await;
        assert_eq!(first.len(), 1);

        // Deliver the first push: it leaves the supersede partial index but stays
        // in the table for the fingerprint lookup.
        stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
            .await
            .unwrap();
        assert!(pending(&orch, "j-watch").await.is_empty());

        // Same diffstat -> skipped, no re-wake.
        run_review_push(&orch);
        assert!(
            pending(&orch, "j-watch").await.is_empty(),
            "an unchanged reviewable state must not re-create a review push"
        );
    }

    #[tokio::test]
    async fn changed_diffstat_creates_new_review_after_delivery() {
        // New commits change the diffstat -> a fresh review push, even after the
        // first was delivered.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);
        let first = pending(&orch, "j-watch").await;
        assert_eq!(first.len(), 1);
        stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
            .await
            .unwrap();

        orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET additions=10, deletions=2 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
        run_review_push(&orch);
        let second = pending(&orch, "j-watch").await;
        assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
        assert_ne!(second[0].id, first[0].id);
    }

    #[tokio::test]
    async fn mergeability_only_change_does_not_refire_review() {
        // A mergeability settle touches non-diffstat columns only -> same
        // fingerprint -> no new review push.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_review_push(&orch);
        let first = pending(&orch, "j-watch").await;
        assert_eq!(first.len(), 1);
        stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
            .await
            .unwrap();

        orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
        run_review_push(&orch);
        assert!(
            pending(&orch, "j-watch").await.is_empty(),
            "a mergeability-only settle must not re-create a review push"
        );
    }

    // --- CAIRN-1891: the PR-open edge of the review push ---------------------

    #[tokio::test]
    async fn pr_open_with_quiescent_producer_pushes_one_review() {
        // The producing builder's head turn is complete (quiescent) and the PR is
        // now open -> exactly one review to the watcher, never to the producing
        // node itself. This is the wake the create-pr idle edge cannot fire.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;

        let watcher = pending(&orch, "j-watch").await;
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].key, REVIEW_KEY);
        assert!(watcher[0]
            .content_ref
            .starts_with("cairn://p/PRJ/7/1/builder"));
        assert!(pending(&orch, "j-prod").await.is_empty());
    }

    #[tokio::test]
    async fn pr_open_with_running_producer_does_not_push() {
        // The quiescence gate: a producing node still mid-turn (a `synchronize`
        // landing during active work) does NOT fire a review.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
            .await
            .unwrap();
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;

        assert!(pending(&orch, "j-watch").await.is_empty());
    }

    #[tokio::test]
    async fn pr_open_self_suspended_producer_does_not_push() {
        // A producing node self-suspended on its own work (yielded waiting on a
        // dependency/sub-agent) is not quiescent either -> no review.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        db.execute_script(
            "UPDATE turns SET state='yielded', yield_reason='dependency_wait' WHERE id='t-prod';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;

        assert!(pending(&orch, "j-watch").await.is_empty());
    }

    #[tokio::test]
    async fn pr_open_resolves_builder_by_branch_not_mr_job() {
        // The live CAIRN-1891 job-identity bug: the merge_request is owned by a
        // separate pr-action node (blocked while the PR is open -> a running turn,
        // never quiescent), while the builder that did the work is a DIFFERENT job
        // on the same branch. Gating on `mr.job_id` would always bail; the gate
        // must resolve and check the builder via `source_branch`.
        let db = test_db().await;
        seed(&db, "initial").await; // builder j-prod: branch 'b', turn complete (quiescent)
                                    // The pr-action node owns the merge_request and — reproducing the live
                                    // shape — has NO joinable execution (execution_id NULL), so an arm-1 query
                                    // that joined through mr.job_id would drop the row and read the open PR as
                                    // unreviewable. The builder (j-prod) is the joinable node.
        db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES('j-prnode','p-rev','i-rev','blocked','pr','pr',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-rev','j-prnode','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;

        let watcher = pending(&orch, "j-watch").await;
        assert_eq!(
            watcher.len(),
            1,
            "must resolve the builder by source_branch and fire, not gate on the blocked pr-node"
        );
        assert_eq!(watcher[0].key, REVIEW_KEY);
    }

    #[tokio::test]
    async fn pr_open_changed_head_sha_refires_even_with_same_diffstat() {
        // Head SHA is the precise change key: two different commits can share a
        // diffstat, so a real new commit must re-review even when +/- is unchanged.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        db.execute_script("UPDATE merge_requests SET head_sha='sha-aaa' WHERE id='mr-rev';")
            .await
            .unwrap();
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;
        let first = pending(&orch, "j-watch").await;
        assert_eq!(first.len(), 1);
        stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
            .await
            .unwrap();

        // New commit, SAME diffstat, different head SHA -> must re-fire.
        orch.db
            .local
            .execute_script("UPDATE merge_requests SET head_sha='sha-bbb' WHERE id='mr-rev';")
            .await
            .unwrap();
        run_pr_open(&orch).await;
        let second = pending(&orch, "j-watch").await;
        assert_eq!(
            second.len(),
            1,
            "a changed head SHA must re-create the review even with an unchanged diffstat"
        );
        assert_ne!(second[0].id, first[0].id);
    }

    #[tokio::test]
    async fn pr_open_during_memory_review_turn_fires_review() {
        // The common build path: create-pr (work turn ends) -> a trailing
        // memory-review turn runs -> the `opened` webhook lands while it runs. A
        // running memory-review turn is quiescent-for-review (the PR is the
        // reviewable output, and the node-idle edge gates out memory-review turn
        // ends), so the PR-open edge MUST fire here — otherwise the wake is lost
        // permanently when no further commit produces a later webhook.
        let db = test_db().await;
        seed(&db, "memory_review").await;
        insert_open_pr(&db).await;
        db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
            .await
            .unwrap();
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;

        let watcher = pending(&orch, "j-watch").await;
        assert_eq!(
            watcher.len(),
            1,
            "a running memory-review turn must not block the PR-open review"
        );
        assert_eq!(watcher[0].key, REVIEW_KEY);

        // The node-idle edge for that same memory-review turn end is gated out, so
        // it adds nothing — still exactly one undelivered review (no double).
        run_review_push(&orch);
        assert_eq!(pending(&orch, "j-watch").await.len(), 1);
    }

    #[tokio::test]
    async fn pr_open_same_diffstat_is_deduped() {
        // A mergeability-only settle re-delivers the open PR with an unchanged
        // diffstat -> the fingerprint matches the delivered push, so no re-wake.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;
        let first = pending(&orch, "j-watch").await;
        assert_eq!(first.len(), 1);
        stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
            .await
            .unwrap();
        assert!(pending(&orch, "j-watch").await.is_empty());

        orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
        run_pr_open(&orch).await;
        assert!(
            pending(&orch, "j-watch").await.is_empty(),
            "a mergeability-only settle must not re-create a review push"
        );
    }

    #[tokio::test]
    async fn pr_open_changed_diffstat_creates_new_review() {
        // New commits change the diffstat between webhook deliveries -> a fresh
        // review push, even after the first was delivered.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;
        let first = pending(&orch, "j-watch").await;
        assert_eq!(first.len(), 1);
        stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
            .await
            .unwrap();

        orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET additions=20, deletions=4 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
        run_pr_open(&orch).await;
        let second = pending(&orch, "j-watch").await;
        assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
        assert_ne!(second[0].id, first[0].id);
    }

    #[tokio::test]
    async fn pr_open_and_node_idle_share_one_creator() {
        // Both edges run the same row creator: the PR-open edge creates the
        // review, and a subsequent node-idle edge against the unchanged diffstat
        // is deduped by the same fingerprint logic to the one undelivered row.
        let db = test_db().await;
        seed(&db, "initial").await;
        insert_open_pr(&db).await;
        let orch = test_orchestrator(db);

        run_pr_open(&orch).await;
        let after_pr_open = pending(&orch, "j-watch").await;
        assert_eq!(after_pr_open.len(), 1);
        let row_id = after_pr_open[0].id.clone();

        run_review_push(&orch);
        let after_idle = pending(&orch, "j-watch").await;
        assert_eq!(after_idle.len(), 1);
        assert_eq!(
            after_idle[0].id, row_id,
            "both edges share one push row keyed review:{{issue}}"
        );
    }

    #[tokio::test]
    async fn render_push_resolved_inlines_referent_content() {
        // CAIRN-1891 Deliverable 2: a drained push renders its referent content
        // inline, not just the URI. The header carries the wake level + the
        // content_ref URI; a resolved body is appended beneath it.
        let db = test_db().await;
        seed(&db, "initial").await;
        let orch = test_orchestrator(db);

        let push = Push {
            id: "p-render".into(),
            recipient: "j-watch".into(),
            content_ref: ISSUE_URI.into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: REVIEW_KEY.into(),
            created_at: 1,
            delivered_event_id: None,
        };
        let rendered =
            crate::orchestrator::attention_delivery::render_push_resolved(&orch, &push).await;

        let header = format!("Attention update (wake): {ISSUE_URI}");
        assert!(
            rendered.starts_with(&header),
            "header must carry the wake level + content_ref URI: {rendered}"
        );
        assert!(
            rendered.len() > header.len(),
            "expected resolved referent content inlined beneath the URI header: {rendered}"
        );
    }
}
