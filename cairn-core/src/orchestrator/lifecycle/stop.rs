//! Stopping and killing sessions: turn interruption, cascade stop, hard kill,
//! and durable-wait suspension. Sliced verbatim from the former `lifecycle.rs`.

use crate::agent_process::stream::TranscriptEvent;
use crate::models::{RunStatus, TurnState};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbResult, RowExt};
use crate::transcripts::stream_store::{self, EventInsert};
use cairn_common::ids;
use std::collections::HashSet;

use super::common::*;
use super::finalize::finalize_run;

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
            cairn_db::turso::params![run_id.as_str()],
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

async fn find_descendant_job_ids(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Vec<String>> {
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
    conn: &cairn_db::turso::Connection,
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

/// Deliberately stop a running workflow node (CAIRN-2516).
///
/// A workflow node's process is a stdin-less `bun <script>`, so the ordinary
/// interrupt cannot reach it; a Stop must hard-kill it AND cascade to its
/// in-flight child calls. This marks the workflow's live run as
/// deliberately-stopped, then reuses [`stop_session`] verbatim: the child cascade
/// kills orphaned agent sessions, and the workflow run's own interrupt-send fails
/// (no stdin) and falls through to [`kill_session_with_reason`]. The workflow
/// supervisor, seeing the marker on its finalize, maps the killed process to a
/// terminal, non-crashed (Stopped) outcome and KEEPS the re-dispatch record so a
/// later Restart works — while the terminal status keeps the startup sweep from
/// resurrecting it (the trap this issue turns on). A no-op when the node has no
/// live run.
pub fn stop_workflow(orch: &Orchestrator, workflow_job_id: &str) -> Result<(), String> {
    let Some(run_id) = live_run_id_for_job(orch, workflow_job_id) else {
        log::info!("stop_workflow: no live run for job {workflow_job_id}; nothing to stop");
        return Ok(());
    };
    // Set the marker BEFORE the kill so the supervisor's finalize (which races
    // the kill path's own finalize) observes it and maps to the cancelled,
    // non-re-dispatched outcome regardless of which finalizer wins the status
    // race.
    orch.process_state.mark_workflow_stop_requested(&run_id);
    stop_session(orch, &run_id)
}

/// Stop an in-flight workflow child call (CAIRN-2516).
///
/// Hard-terminates the call's agent session via [`kill_session_with_reason`]
/// (`"user_stop"`) — NOT the warm-park [`stop_session`], which would leave the
/// run non-terminal and hang the workflow's awaiting `agent()`. The killed run
/// reaches `exited` with no artifact, which `terminal_call_body` maps to the call
/// failure sentinel so `agent()` resolves `null` (deep-research's salvage paths
/// handle it). `finalize_run` then journals the call as Failure(null) at its
/// `(workflow_run_id, ordinal)` and deletes the link — a stopped call is journaled
/// exactly like any failed call. Rejects a run that is not a workflow child call
/// or is already terminal.
pub fn stop_call(orch: &Orchestrator, call_run_id: &str) -> Result<(), String> {
    match run_status(orch, call_run_id).as_deref() {
        None => return Err(format!("Call run {call_run_id} not found")),
        Some(status) if is_terminal_run_status(status) => {
            log::info!("stop_call: run {call_run_id} already terminal ({status}); nothing to stop");
            return Ok(());
        }
        Some(_) => {}
    }
    if !is_workflow_child_run(orch, call_run_id) {
        return Err(format!(
            "Run {call_run_id} is not a workflow child call; refusing stop_call"
        ));
    }
    kill_session_with_reason(orch, call_run_id, "user_stop")
}

/// The stored run statuses that count as terminal (its process has exited).
/// Mirrors `RunStatus::is_terminal` plus the legacy stored spellings.
fn is_terminal_run_status(status: &str) -> bool {
    matches!(
        status,
        "exited" | "crashed" | "complete" | "completed" | "failed"
    )
}

/// Whether a run is a child call of a workflow node — its parent job carries the
/// synthetic `agent_config_id = "workflow"`. Gates [`stop_call`] so it acts only
/// on genuine workflow calls, validated server-side rather than trusting the UI.
fn is_workflow_child_run(orch: &Orchestrator, run_id: &str) -> bool {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent.agent_config_id FROM runs r \
                         JOIN jobs j ON r.job_id = j.id \
                         JOIN jobs parent ON j.parent_job_id = parent.id \
                         WHERE r.id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or(None)
    .as_deref()
        == Some("workflow")
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
pub(crate) enum InterruptFailurePolicy {
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
pub(crate) fn stop_session_internal(
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
