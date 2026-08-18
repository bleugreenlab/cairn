//! Stopping and killing sessions: turn interruption, cascade stop, hard kill,
//! and durable-wait suspension. Sliced verbatim from the former `lifecycle.rs`.

use crate::agent_process::stream::TranscriptEvent;
use crate::models::{RunStatus, TurnEndReason, TurnState};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbResult, RowExt};
use crate::transcripts::stream_store::{self, EventInsert};
use cairn_common::ids;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tokio::sync::broadcast;

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

pub(crate) const USER_STOP_TOOL_RESULT: &str = "Run interrupted by user stop.";

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
            tool_result: Some(USER_STOP_TOOL_RESULT.to_string()),
            is_error: true,
            thinking_ms: None,
            queued_message_id: None,
            raw: Some(serde_json::json!({ "synthetic": true, "reason": "user_stop" })),
        };
        let data = event.observed().to_event_json();
        let event_insert = EventInsert {
            id: event_id.clone(),
            run_id: run_id.to_string(),
            session_id: pending_result.session_id.clone(),
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
            let scope = crate::notify::event_run_scope(orch.db.local.clone(), run_id);
            let _ = orch.services.emitter.emit(
                "db-change",
                crate::notify::event_db_change_scoped(
                    run_id,
                    pending_result.session_id.as_deref(),
                    &scope,
                    "insert",
                ),
            );
        }
    }
    Ok(inserted)
}

pub fn stop_active_turn_for_run(orch: &Orchestrator, run_id: &str, cancel_owned_waits: bool) {
    // A durable self-suspend parks warm on its OWN pending `agent_waits` row and
    // must leave it intact so the eventual resolver can resume the run; only an
    // external stop cancels the wait. Unconditionally cancelling here — including on
    // a self-suspend — was the latent defect behind never-resumed durable waits
    // (CAIRN-2970).
    if cancel_owned_waits {
        let cancel_result = run_db_blocking({
            let dbs = orch.db.clone();
            let run_id = run_id.to_string();
            move || async move {
                let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                    .await
                    .map_err(|e| e.to_string())?;
                db.write(|conn| {
                    let run_id = run_id.clone();
                    Box::pin(async move {
                        conn.execute(
                            "UPDATE agent_waits SET state='cancelled', resolved_at=?2 WHERE run_id=?1 AND state IN ('pending','resolving')",
                            cairn_db::turso::params![run_id, chrono::Utc::now().timestamp_millis()],
                        ).await?;
                        Ok(())
                    })
                }).await.map_err(|e| e.to_string())
            }
        });
        if let Err(error) = cancel_result {
            log::warn!("Failed to cancel owned wait for run {run_id}: {error}");
        }
    }
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
        "running" => interrupt_turn(orch, &turn_id, Some(TurnEndReason::UserStop)),
        "pending" | "yielded" => apply_turn_outcome(
            orch,
            &turn_id,
            TurnState::Cancelled,
            Some(TurnEndReason::UserStop),
        ),
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
///
/// Call this directly only when no agent-visible tool result is racing the
/// interrupt (startup reconciliation, tests). Every suspension that answers a
/// pending tool call goes through [`suspend_run_for_durable_wait_after_handoff`]
/// instead -- see [`SUSPEND_HANDOFF_GRACE`] for why.
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

/// How long a Cairn-initiated suspension lets its own tool result reach the
/// agent before the interrupt that ends the turn.
///
/// The interrupt cancels whatever tool call is in flight. When it lands before
/// the suspension's own result has travelled back through the MCP transport, the
/// agent CLI fills in a result for the cancelled call in its stock rejection
/// wording -- "The user doesn't want to proceed with this tool use. The tool use
/// was rejected" -- and writes it into its own session transcript. That text,
/// not ours, is then what the agent reads for the rest of the session: a routine
/// pause wearing the user's face. Agents have acted on it, concluding the
/// operator vetoed them and saying so to the operator (CAIRN-3162).
///
/// The window only has to cover an in-process MCP response travelling back
/// through `cairn-cmd` into the CLI's transcript -- single-digit milliseconds
/// normally. It is sized well above that because losing the race is permanent
/// (the false text stays in context for the whole session) while overshooting
/// costs nothing: the turn is ending regardless, and the run is about to wait on
/// something far slower. A 75ms window measured a ~10% loss rate in practice.
pub const SUSPEND_HANDOFF_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// One run's pending durable-wait park, shared by every call the run's current
/// turn is suspending at once.
struct ParkSlot {
    /// Bumped by each suspension. Only the timer holding the current generation
    /// parks; an earlier one stands down when a sibling supersedes it.
    generation: u64,
    /// Carries the single park's outcome to every suspension waiting on it,
    /// including those whose own timer stood down.
    done: broadcast::Sender<Result<(), String>>,
}

/// The debounce state behind [`suspend_run_for_durable_wait_after_handoff_then`],
/// keyed by run.
///
/// It lives on the orchestrator so every clone shares it and it is scoped to one
/// host, exactly like the processes it parks. A park is in-memory control flow
/// over a live process; nothing here needs to survive a restart, because a
/// suspension whose park never happened is left for startup reconciliation.
#[derive(Default)]
pub struct ParkSlots(Mutex<HashMap<String, ParkSlot>>);

/// Park a run for a durable wait once its pending tool call has had the handoff
/// grace to receive the suspension's own result.
///
/// This is the canonical way a Cairn-initiated suspension ends a turn. See
/// [`SUSPEND_HANDOFF_GRACE`] for what parking synchronously costs.
pub fn suspend_run_for_durable_wait_after_handoff(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
) {
    suspend_run_for_durable_wait_after_handoff_then(orch, run_id, exit_reason, |_| async {})
}

/// [`suspend_run_for_durable_wait_after_handoff`] with a continuation.
///
/// `after_park` receives the park's outcome and runs once the predecessor turn
/// has actually been interrupted. Anything that can resume the run belongs
/// there: the park must be the last thing that happens to the predecessor, or an
/// already-satisfied condition resumes a run the park then re-suspends
/// (CAIRN-2970).
///
/// The park itself is coalesced per run rather than per suspension — see
/// [`arm_coalesced_park`] — so a turn suspending several of its calls at once is
/// interrupted once, after the last of them has had its grace.
pub fn suspend_run_for_durable_wait_after_handoff_then<F, Fut>(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    after_park: F,
) where
    F: FnOnce(Result<(), String>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut parked = arm_coalesced_park(orch, run_id, exit_reason);
    tokio::spawn(async move {
        let outcome = match parked.recv().await {
            Ok(outcome) => outcome,
            // The park reported nothing at all. Report it as a failed park: a
            // caller must never resume a run it cannot know was suspended, and
            // the pending row is left for startup reconciliation.
            Err(error) => Err(format!("durable-wait park reported no outcome: {error}")),
        };
        after_park(outcome).await;
    });
}

/// Arm the run's single deferred park, coalescing this suspension with any
/// sibling call of the same turn.
///
/// Each suspension bumps the run's generation and subscribes to the slot's
/// broadcast, then spawns a timer that parks only if its generation is still
/// current when the grace elapses. A later sibling therefore supersedes an
/// earlier one's timer, and the run is parked once — [`SUSPEND_HANDOFF_GRACE`]
/// after the LAST suspension marker rather than the first.
///
/// That is what the coalescing is for. The interrupt cancels whatever call is in
/// flight, so a park fired on the first sibling's schedule can land before a
/// later sibling's marker has travelled back through the MCP transport — which
/// is precisely the CAIRN-3162 misattribution the grace exists to prevent, where
/// the CLI writes its own "the user doesn't want to proceed" text into the
/// model's context in place of ours.
///
/// Whichever timer does park broadcasts its outcome on the shared sender, so
/// every sibling learns it, including those whose own timer stood down. A
/// suspension arriving after the slot is retired opens a fresh one and parks an
/// already-parked run, which is a benign no-op.
fn arm_coalesced_park(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
) -> broadcast::Receiver<Result<(), String>> {
    let (generation, done, parked) = {
        let mut slots = orch.park_slots.0.lock().unwrap();
        let slot = slots.entry(run_id.to_string()).or_insert_with(|| ParkSlot {
            generation: 0,
            done: broadcast::channel(1).0,
        });
        slot.generation += 1;
        (slot.generation, slot.done.clone(), slot.done.subscribe())
    };
    let orch = orch.clone();
    let run_id = run_id.to_string();
    let exit_reason = exit_reason.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(SUSPEND_HANDOFF_GRACE).await;
        // A sibling suspension armed after this one owns the park now.
        if !park_generation_is_current(&orch, &run_id, generation) {
            return;
        }
        // `suspend_run_for_durable_wait` blocks on DB work, so it never runs on
        // the async worker itself.
        let joined = {
            let orch = orch.clone();
            let run_id = run_id.clone();
            tokio::task::spawn_blocking(move || {
                suspend_run_for_durable_wait(&orch, &run_id, &exit_reason)
            })
            .await
        };
        let parked = match joined {
            Ok(result) => result,
            Err(join_error) => Err(format!("park worker panicked: {join_error}")),
        };
        if let Err(error) = &parked {
            log::warn!("deferred durable-wait park failed for run {run_id}: {error}");
        }
        // Retire the slot BEFORE broadcasting, so a suspension arriving
        // afterwards opens its own rather than waiting on a signal already sent.
        retire_park_slot(&orch, &run_id, generation);
        let _ = done.send(parked);
    });
    parked
}

fn park_generation_is_current(orch: &Orchestrator, run_id: &str, generation: u64) -> bool {
    orch.park_slots
        .0
        .lock()
        .unwrap()
        .get(run_id)
        .is_some_and(|slot| slot.generation == generation)
}

fn retire_park_slot(orch: &Orchestrator, run_id: &str, generation: u64) {
    let mut slots = orch.park_slots.0.lock().unwrap();
    if slots
        .get(run_id)
        .is_some_and(|slot| slot.generation == generation)
    {
        slots.remove(run_id);
    }
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
pub async fn stop_workflow(orch: &Orchestrator, workflow_job_id: &str) -> Result<(), String> {
    let Some(run_id) = live_run_id_for_job(orch, workflow_job_id) else {
        log::info!("stop_workflow: no live run for job {workflow_job_id}; nothing to stop");
        return Ok(());
    };
    // Set the marker BEFORE the kill so the supervisor's finalize (which races
    // the kill path's own finalize) observes it and maps to the cancelled,
    // non-re-dispatched outcome regardless of which finalizer wins the status
    // race.
    orch.process_state.mark_workflow_stop_requested(&run_id);
    // Preserve the existing child-call cascade; the workflow itself no longer
    // has a runner-local child handle, so its expected final error is ignored.
    let _ = stop_session(orch, &run_id);
    let fence = orch
        .process_state
        .workflow_lease(&run_id)
        .ok_or_else(|| format!("workflow {run_id} has no live executor binding"))?;
    crate::fleet::residency::stop(orch, &fence, &run_id).await
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
    let change = run_db_blocking({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        move || async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(crate::notify::turn_db_change_for_job_id(&db, &job_id, "update").await)
        }
    })?;
    let _ = orch.services.emitter.emit("db-change", change);

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
    // A WarmAnyway stop is a durable self-suspend, which must PRESERVE its own
    // pending wait row; every other (HardKill) stop is an external cancel.
    let cancel_owned_waits = interrupt_failure_policy != InterruptFailurePolicy::WarmAnyway;
    stop_active_turn_for_run(orch, run_id, cancel_owned_waits);

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
        // A stop must also reach the executor. `cleanup_inline_commands` reaps
        // only host-side children, so a routed batch — including one this run
        // suspended on — would otherwise keep running with nobody to read its
        // result.
        if let Some(job_id) = job_id_for_run(orch, run_id) {
            let cancelled = orch.fleet.cancel_job_requests(&job_id);
            if cancelled > 0 {
                log::info!("stop cancelled {cancelled} in-flight batch(es) for job {job_id}");
            }
        }
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
            // This reconcile early-return transitions to Exited WITHOUT reaching
            // finalize_run, so a queued call killed here (status `starting`, no
            // process handle) would otherwise be stranded in its admission queue.
            // Dequeue it explicitly; `release` is idempotent so the finalize hook
            // covering the normal path stays harmless.
            crate::execution::jobs::on_call_run_finalized(orch, run_id);
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

/// Exit reason for a provider turn deliberately interrupted immediately before
/// an automatic same-session recovery. It remains a crashed/interrupted turn,
/// but terminal failure attention must not fire between the two turns.
pub(crate) const PROVIDER_SILENCE_RECOVERY_EXIT_REASON: &str = "provider_silence_recovery";
pub(crate) const WATCHDOG_ARM_FAILED_EXIT_REASON: &str = "watchdog_arm_failed";

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
    // A panic while another thread holds the registry must not strand durable
    // run state. The child handle may still be safely removed from a poisoned
    // mutex; more importantly, exit-reason persistence and finalization below
    // must remain unconditional.
    let mut processes = orch
        .process_state
        .processes
        .lock()
        .unwrap_or_else(|poisoned| {
            log::error!(
                "Process registry lock was poisoned while killing run {}; recovering to finalize it",
                &run_id[..run_id.len().min(8)]
            );
            poisoned.into_inner()
        });

    if let Some(handle) = processes.remove(run_id) {
        if let Ok(mut child_guard) = handle.child.lock() {
            if let Some(mut child) = child_guard.take() {
                crate::agent_process::process::graceful_stop(&mut *child);
                log::info!("Killed process for run {}", &run_id[..run_id.len().min(8)]);
            }
        }
    }

    // `into_inner` repairs access for this caller but does not clear the mutex's
    // poison bit. Clear it while the repaired registry is still locked: releasing
    // first would let a successor observe stale poison, or let a new panic's
    // poison be cleared without this caller inspecting that state.
    orch.process_state.processes.clear_poison();

    // Drop the lock before calling finalize_run (which also locks)
    drop(processes);

    // Set exit reason and finalize as Exited (clean kill) or Crashed
    let final_status = if matches!(
        exit_reason,
        "crash" | PROVIDER_SILENCE_RECOVERY_EXIT_REASON | WATCHDOG_ARM_FAILED_EXIT_REASON
    ) {
        RunStatus::Crashed
    } else {
        RunStatus::Exited
    };

    set_exit_reason(orch, run_id, exit_reason)?;

    if exit_reason == PROVIDER_SILENCE_RECOVERY_EXIT_REASON {
        super::finalize_run_for_recovery(orch, run_id, final_status)?;
    } else {
        finalize_run(orch, run_id, final_status);
    }

    Ok(())
}

/// The exit reason recorded for an agent stopped because its host is shutting
/// down. Distinguishes "we stopped it" from the blanket `crash` that startup
/// reconciliation would otherwise invent for the same row.
pub const RUNNER_SHUTDOWN_EXIT_REASON: &str = "runner_shutdown";

/// What a host's agent teardown accomplished: enough for the caller's one log
/// line, and for a test to assert on.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HostShutdownStops {
    /// Agents stopped and finalized before the budget elapsed.
    pub stopped: usize,
    /// Agents whose stop reported an error, left to startup reconciliation.
    pub failed: usize,
    /// Agents still stopping when the budget elapsed. Their blocking work cannot
    /// be aborted, so it continues until the host process exits.
    pub timed_out: usize,
}

/// Stop every agent process this host spawned, before the host process exits.
///
/// Without this a runner abandons its agents (CAIRN-3287). A run handle's child
/// is an `Arc<Mutex<Option<Box<dyn ChildProcess>>>>` whose drop does not signal
/// the OS process, so an exiting runner leaves each agent reparented to
/// launchd/systemd and still running — while the reader thread that was
/// persisting its transcript dies with the runner. The agent then works on
/// against a wall: `dispatch_tool`'s ownership fence refuses its calls, but only
/// this stops the process from making them.
///
/// Each agent goes through [`kill_session_with_reason`] rather than a raw
/// `graceful_stop`, so its run and turn get the same bookkeeping any other stop
/// produces and the row records [`RUNNER_SHUTDOWN_EXIT_REASON`] instead of the
/// `crash` a later startup sweep would invent. A clean restart should therefore
/// leave startup reconciliation with nothing to reconcile.
///
/// Latency is bounded because a successor host is waiting to bind the port:
/// `graceful_stop` alone spends up to ~3s per process on its SIGTERM poll, so the
/// stops run concurrently under one overall `budget` and the caller proceeds
/// regardless of what is still outstanding. Each stop is synchronous and blocks
/// on database work, so it runs on the blocking pool rather than an async worker.
pub async fn stop_agents_for_host_shutdown(
    orch: &Orchestrator,
    budget: std::time::Duration,
) -> HostShutdownStops {
    let mut outcome = HostShutdownStops::default();
    let run_ids = orch.process_state.run_ids();
    if run_ids.is_empty() {
        return outcome;
    }

    let mut stops = tokio::task::JoinSet::new();
    for run_id in run_ids {
        let orch = orch.clone();
        stops.spawn_blocking(move || {
            kill_session_with_reason(&orch, &run_id, RUNNER_SHUTDOWN_EXIT_REASON)
                .map_err(|error| (run_id, error))
        });
    }

    let deadline = tokio::time::sleep(budget);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            joined = stops.join_next() => match joined {
                None => break,
                Some(Ok(Ok(()))) => outcome.stopped += 1,
                Some(Ok(Err((run_id, error)))) => {
                    outcome.failed += 1;
                    log::warn!("host shutdown could not stop agent for run {run_id}: {error}");
                }
                Some(Err(join_error)) => {
                    outcome.failed += 1;
                    log::warn!("host shutdown agent stop panicked: {join_error}");
                }
            },
            _ = &mut deadline => {
                outcome.timed_out = stops.len();
                break;
            }
        }
    }
    outcome
}
