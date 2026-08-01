//! Warm transition, run finalization, task-failure finalize, and memory-review
//! completion. Sliced verbatim from the former `lifecycle.rs`.

use crate::mcp::handlers::{emit_attention, AttentionEvent};
use crate::models::{RunStatus, TurnEndReason, TurnState};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, RowExt};

use super::common::*;
use super::review_push::{detach_onto_runtime, emit_for_turn_end, spawn_turn_end_checks};

/// Transition a run to warm state after successful turn completion.
///
/// The Run stays `Live` in the DB — no durable status change.
/// Completes the current Turn and transitions process occupancy to Idle.
///
/// Returns true if the process was successfully transitioned to warm.
pub fn transition_to_warm_state(
    orch: &Orchestrator,
    run_id: &str,
    end_reason: Option<TurnEndReason>,
) -> bool {
    // Complete the current turn before transitioning occupancy
    let completed_turn_id = orch.process_state.get_current_turn_id(run_id);
    if let Some(turn_id) = completed_turn_id.as_deref() {
        let _ = apply_turn_outcome(orch, turn_id, TurnState::Complete, end_reason);
    }

    if orch.process_state.transition_to_warm(run_id) {
        // Emit the completed turn with its authoritative job/project scope.
        if let Some(turn_id) = completed_turn_id.as_deref() {
            let change = run_db_blocking({
                let dbs = orch.db.clone();
                let run_id = run_id.to_string();
                let turn_id = turn_id.to_string();
                move || async move {
                    let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await)
                }
            });
            if let Ok(change) = change {
                let _ = orch.services.emitter.emit("db-change", change);
            }
        }

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
            // Turn-end project checks (when:review), detached so the suite never
            // blocks the turn from ending. Detached review-cadence checks are
            // child feedback and do not gate the parent review wake; semantic
            // liveness remains owned by `issue_settled` in the recompute hook.
            spawn_turn_end_checks(orch, &job_id);
            if let Err(e) = crate::execution::advancement::recompute_job(orch, &job_id) {
                log::error!(
                    "Failed to recompute job {} after warm transition: {}",
                    job_id,
                    e
                );
            }
            reduce_nodeless_delegated_child(orch, &job_id);
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

        // A one-shot ephemeral call child is never resumed; once its work is
        // done, reap it instead of leaving it in the warm pool (CAIRN-2543).
        maybe_kill_completed_call_child(orch, run_id);

        true
    } else {
        log::warn!(
            "Failed to transition run {} to warm state (process not found)",
            &run_id[..run_id.len().min(8)]
        );
        false
    }
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
pub(crate) fn finish_memory_review_if_due(orch: &Orchestrator, job_id: &str, run_id: &str) {
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

/// Journal a completed workflow-parented ephemeral call's result under its
/// `(workflow_run_id, ordinal)` key, then drop the pending link. A clean exit
/// with an artifact journals the validated result (a later replay returns it
/// with no spawn); any other outcome journals a failure (replays as `null`). A
/// call with no journal link (an ordinary call, or a workflow call that hit the
/// journal) is a no-op. All errors are logged and swallowed.
fn maybe_journal_call_result(orch: &Orchestrator, run_id: &str, job_id: Option<&str>) {
    let Some(job_id) = job_id else { return };
    let outcome = run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        let job_id = job_id.to_string();
        move || async move {
            let private = dbs.local.clone();
            let Some(link) = crate::workflow_journal::load_call_link(&private, &run_id).await?
            else {
                return Ok(false);
            };
            // The return artifact IS the call's completion contract: if it was
            // written, journal Success with that payload regardless of the run's
            // terminal STATUS. A CLI stream that crashed AFTER the artifact landed
            // (CAIRN-2677 bug 1) must replay as the result, not `null` — believing
            // the exit type over the artifact is exactly what lost a finished
            // call's result. Only a terminal run that produced NO artifact journals
            // a Failure (replays as `null`), mirroring the `?wait` terminal mapping.
            // The call's artifact lives in the run's owning database (a team run in
            // its replica); route to it rather than assuming private.
            let owning = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .unwrap_or_else(|_| dbs.local.clone());
            // Scoped to the call's CONTRACTED return artifact (not any artifact
            // the run wrote), so an unrelated named artifact can never journal a
            // false Success (CAIRN-2677).
            let (result_json, jstatus) =
                match crate::artifacts::queries::contracted_return_artifact_data(&owning, &job_id)
                    .await
                {
                    Some(data) => (Some(data), crate::workflow_journal::JournalStatus::Success),
                    None => (None, crate::workflow_journal::JournalStatus::Failure),
                };
            crate::workflow_journal::store_entry(
                &private,
                &link.workflow_run_id,
                link.ordinal,
                &link.prompt_hash,
                result_json.as_deref(),
                jstatus,
            )
            .await?;
            crate::workflow_journal::delete_call_link(&private, &run_id).await?;
            Ok(true)
        }
    });
    if let Err(e) = outcome {
        log::warn!("Failed to journal workflow call result for run {run_id}: {e}");
    }
}

/// Reap a completed one-shot `CallTool` child instead of leaving it warm.
///
/// A call child is created for an ephemeral agent call (a `write` to a node's
/// calls collection); it produces a single result artifact and is never
/// resumed, so keeping its process warm only holds memory (CAIRN-2543). Runs
/// after the warm-transition completion tail, guarded so a call still awaiting
/// its first-artifact memory review stays warm — the GC's own protection covers
/// that, and this check re-fires and reaps it at the review turn's own warm
/// transition. The kill is detached so it never blocks the turn from ending.
fn maybe_kill_completed_call_child(orch: &Orchestrator, run_id: &str) {
    let Some(job_id) = job_id_for_run(orch, run_id) else {
        return;
    };
    if !crate::execution::delegation::is_call_child(orch, &job_id) {
        return;
    }
    let status = blocking_text_lookup(
        orch,
        &job_id,
        "SELECT status FROM jobs WHERE id = ?1",
        TextColumn::Optional,
    );
    if !matches!(
        status.as_deref(),
        Some("complete") | Some("failed") | Some("cancelled")
    ) {
        return;
    }
    // A pending memory review must complete before the process is reaped; the
    // review turn's warm transition re-fires this check and kills it then.
    let review_state = blocking_text_lookup(
        orch,
        &job_id,
        "SELECT memory_review_state FROM jobs WHERE id = ?1",
        TextColumn::Optional,
    );
    if review_state.as_deref() == Some("sent") {
        return;
    }
    log::info!(
        "Reaping completed call child run {} (call_complete): a one-shot call is never resumed",
        &run_id[..run_id.len().min(8)]
    );
    let orch = orch.clone();
    let run_id = run_id.to_string();
    detach_onto_runtime(
        async move {
            if let Err(e) = crate::orchestrator::lifecycle::kill_session_with_reason(
                &orch,
                &run_id,
                "call_complete",
            ) {
                log::warn!("Failed to reap completed call child {}: {}", run_id, e);
            }
        },
        || {},
    );
}

/// Terminalize a node-less delegated child (an ephemeral call or a workflow)
/// whose status the execution sweep can't derive, so the `try_resume_delegated_parent`
/// that follows resolves its packet and wakes the suspended caller in this same
/// finalize pass.
///
/// `recompute_job` reduces only recipe-node-backed jobs; a pre-materialized
/// node-less child (`recipe_node_id IS NULL`) is invisible to the sweep, so
/// without this its job stays `running` after its run finalizes and a durably
/// suspended caller hangs forever (CAIRN-2559). Fail-closed: a failure here
/// would silently strand the caller, so it is logged at `error`, not `warn`.
fn reduce_nodeless_delegated_child(orch: &Orchestrator, job_id: &str) {
    match crate::execution::advancement::reduce_delegated_child_job(orch, job_id) {
        Ok(Some(status)) => log::info!(
            "Terminalized node-less delegated child job {} as {} (sweep-unreachable)",
            &job_id[..job_id.len().min(8)],
            status
        ),
        Ok(None) => {}
        Err(e) => log::error!(
            "Failed to terminalize node-less delegated child job {job_id}: {e} — \
             a suspended caller may be stranded"
        ),
    }
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
/// True when a genuinely fatal turn failure must be finalized TERMINALLY (turn
/// Failed → job Failed) rather than left as a resumable interruption.
///
/// Two kinds qualify. A delegated TASK (its job has a `parent_job_id`) has a
/// suspended parent blocked on the resume gate that needs a terminal answer. A
/// WORKFLOW node — delegated OR standalone (CAIRN-2651) — must map an
/// exit-0-without-artifact / non-zero exit to `Failed`, holding the documented
/// completion mapping identically; a standalone workflow has NULL
/// `parent_job_id` yet is a terminal top-level run, not a resumable agent job.
/// Every other top-level agent job stays resumable (the existing
/// `finalize_run(Crashed)` interruption).
fn run_fails_terminally(orch: &Orchestrator, run_id: &str) -> bool {
    blocking_text_lookup(
        orch,
        run_id,
        "SELECT jobs.id
         FROM runs
         JOIN jobs ON runs.job_id = jobs.id
         WHERE runs.id = ?1
           AND (jobs.parent_job_id IS NOT NULL OR jobs.agent_config_id = 'workflow')",
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
    if !run_fails_terminally(orch, run_id) {
        // Top-level agent job: keep the resumable-interrupt behavior unchanged.
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
        if let Err(e) = apply_turn_outcome(orch, &turn_id, TurnState::Failed, None) {
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
    crate::orchestrator::session::cleanup_prompt_file(run_id);

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
                    apply_turn_outcome(orch, turn_id, TurnState::Failed, None)
                } else {
                    apply_turn_outcome(orch, turn_id, TurnState::Complete, None)
                }
            } else if turn_state.as_str() == "running" {
                interrupt_turn(orch, turn_id, Some(TurnEndReason::Crash))
            } else {
                apply_turn_outcome(orch, turn_id, TurnState::Failed, None)
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

    // Release this run's call-admission slot (if it held one) and start the next
    // queued call. No-op for uncapped/non-call runs; idempotent. Tied to the
    // finalize choke so a crashed/killed call cannot leak its slot.
    crate::execution::jobs::on_call_run_finalized(orch, run_id);

    let job_id = job_id_for_run(orch, run_id);

    // Journal a workflow-parented call's result on completion (CAIRN-2498), so a
    // host-restart replay of the workflow short-circuits this ordinal instead of
    // re-running the call. This is the genuine first finalize (the re-entry above
    // already returned), and the link is deleted after storing so it is never
    // double-recorded. Best-effort: a journal failure never affects the run.
    maybe_journal_call_result(orch, run_id, job_id.as_deref());

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
            // Turn-end project checks (when:idle/when:review), detached so the
            // suite never blocks the turn from ending. Claimed BEFORE
            // `recompute_job` so the recompute review-readiness hook sees this
            // job's checks in-flight and defers rather than racing the launch and
            // pushing a premature parent review (CAIRN-2483). Mirrors the
            // warm-transition turn-end caller above.
            spawn_turn_end_checks(orch, &job_id);
            if let Err(e) = crate::execution::advancement::recompute_job(orch, &job_id) {
                log::error!(
                    "Failed to recompute job {} after run finalize: {}",
                    job_id,
                    e
                );
            }
            reduce_nodeless_delegated_child(orch, &job_id);
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
        // A node-less ephemeral CALL establishes no DB turn, so `had_active_turn`
        // is false and the turn-driven reduction above never runs for it — yet its
        // job row must still terminalize from its run outcome + return artifact, or
        // it spins `running` forever and the monitoring panel shows a stuck call
        // even after the work is done (CAIRN-2677 bug 2). `reduce_delegated_child_job`
        // keys on the artifact (the call's completion contract) for a turn-less
        // child, so this is correct on a clean exit and on a crashed stream that
        // still landed the artifact. `try_resume_delegated_parent` below then wakes
        // any suspended caller against the now-terminal job.
        reduce_nodeless_delegated_child(orch, job_id);
        log::info!(
            "Run {} exited without an active turn; reduced node-less child {} from run + artifact",
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

    // Crash observability, and the decision to self-heal a native resume the
    // backend reported unresolvable. Computed here — before the terminal
    // attention toast — so a planned fallback suppresses the failure alarm.
    let reseed_fallback = if status == RunStatus::Crashed {
        handle_session_crash(orch, run_id, turn_id.as_deref())
    } else {
        None
    };

    // Completion attention fires when the agent goes idle/warm. Finalization is
    // only a legacy toast source for genuine crash paths that terminalize an
    // in-flight turn without reaching the idle boundary first. A crash we are
    // about to recover from is a self-healing event, not a failure to report.
    if had_active_turn && status == RunStatus::Crashed && reseed_fallback.is_none() {
        emit_agent_terminal_attention_once(orch, run_id, "failed");
    }

    // Last statement: the fallback opens a NEW turn on this job, so the job's
    // derived state (recompute, DAG advance, turn-end emits above) must have
    // settled before it runs.
    if let Some(plan) = reseed_fallback {
        spawn_digest_reseed_fallback(orch, plan);
    }
}

/// A crashed run whose native resume handle the backend could not resolve, and
/// the job that should be reseeded from its transcript digest to recover.
pub(crate) struct DigestReseedFallback {
    pub(crate) job_id: String,
    pub(crate) session_id: String,
}

/// Which crashed run earns the digest-reseed fallback.
///
/// A run that already started fresh cannot have failed to resolve a resume
/// handle, so the start-mode check is belt-and-braces on top of the typed
/// reason. Pure so the matrix is unit-testable without a database.
fn should_fall_back_to_digest_reseed(
    status: &RunStatus,
    start_mode: Option<&str>,
    exit_reason: Option<&str>,
) -> bool {
    *status == RunStatus::Crashed
        && start_mode == Some("resume")
        && exit_reason.and_then(crate::backends::BackendFailure::from_exit_reason)
            == Some(crate::backends::BackendFailure::SessionUnresolvable)
}

/// Claim the one-shot fallback slot for a session identity.
///
/// Insert-and-check: only the caller that wins the insert schedules. Returns
/// false when this session was already handed to the fallback.
fn claim_reseed_fallback(
    attempted: &std::sync::Mutex<std::collections::HashSet<String>>,
    session_id: &str,
) -> bool {
    let mut attempted = attempted.lock().unwrap();
    attempted.insert(session_id.to_string())
}

/// React to a crashed run's session context.
///
/// Always logs a crashed resume for operator visibility. When the backend
/// recorded [`BackendFailure::SessionUnresolvable`], claims the one-shot
/// per-session fallback slot, notes the reconstruction in the transcript, and
/// returns the plan for the caller to schedule.
///
/// [`BackendFailure::SessionUnresolvable`]: crate::backends::BackendFailure::SessionUnresolvable
pub(crate) fn handle_session_crash(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: Option<&str>,
) -> Option<DigestReseedFallback> {
    let dbs = orch.db.clone();
    let log_run_id = run_id.to_string();
    let query_run_id = run_id.to_string();
    let run_info = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &query_run_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT session_id, start_mode, exit_reason, job_id
                         FROM runs
                         WHERE id = ?1",
                        (query_run_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok((
                            row.opt_text(0)?,
                            row.opt_text(1)?,
                            row.opt_text(2)?,
                            row.opt_text(3)?,
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

    let (Some(session_id), start_mode, exit_reason, job_id) = run_info? else {
        return None;
    };

    if !should_fall_back_to_digest_reseed(
        &RunStatus::Crashed,
        start_mode.as_deref(),
        exit_reason.as_deref(),
    ) {
        // Still the right observability for a resume that died for some other
        // reason — that one has no automatic recovery.
        if start_mode.as_deref() == Some("resume") {
            log::warn!(
                "Resume run {} crashed for session {} — \
                 session may be invalid. If this repeats, session rotation may be needed.",
                &log_run_id[..log_run_id.len().min(8)],
                &session_id[..session_id.len().min(8)]
            );
        }
        return None;
    }

    let Some(job_id) = job_id else {
        log::warn!(
            "Run {} reported an unresolvable session but has no job to reseed",
            &log_run_id[..log_run_id.len().min(8)]
        );
        return None;
    };

    if !claim_reseed_fallback(&orch.session_reseed_fallback_attempted, &session_id) {
        log::warn!(
            "Session {} already had a digest-reseed fallback attempt; not retrying for run {}",
            &session_id[..session_id.len().min(8)],
            &log_run_id[..log_run_id.len().min(8)]
        );
        return None;
    }

    log::warn!(
        "Resume run {} crashed because session {} was unresolvable by the backend — \
         reseeding the job from its transcript digest.",
        &log_run_id[..log_run_id.len().min(8)],
        &session_id[..session_id.len().min(8)]
    );

    // Without this the user watches a digest-seeded turn appear from nowhere.
    if let Err(error) = crate::messages::transcript::insert_system_message_sync(
        orch,
        run_id,
        Some(&session_id),
        turn_id,
        "This session's conversation could not be resumed, so it is being reconstructed from the node's transcript digest.",
        serde_json::json!({ "kind": "session_reseed_fallback" }),
    ) {
        log::warn!(
            "Failed to record the session reseed notice for run {}: {}",
            &log_run_id[..log_run_id.len().min(8)],
            error
        );
    }

    Some(DigestReseedFallback { job_id, session_id })
}

/// Drive the claimed fallback on its own thread.
///
/// A thread is required: `finalize_run` runs on the backend's reader thread,
/// and the continuation does blocking database work and spawns a process.
/// [`resume_job_from_digest`] is the same entry point the manual **resume from
/// digest** control uses, so the automatic recovery and the manual one stay one
/// implementation — including its `head_turn_active_sync` guard, which declines
/// if a user continuation raced in first.
///
/// [`resume_job_from_digest`]: crate::execution::jobs::resume_job_from_digest
fn spawn_digest_reseed_fallback(orch: &Orchestrator, plan: DigestReseedFallback) {
    let orch = orch.clone();
    let spawn_job_id = plan.job_id.clone();
    if let Err(error) = std::thread::Builder::new()
        .name("session-reseed-fallback".to_string())
        .spawn(move || {
            if let Err(error) =
                crate::execution::jobs::resume_job_from_digest(&orch, &plan.job_id, None)
            {
                // Nothing was rotated (an empty digest fails before any
                // mutation), so the turn stays crashed exactly as it does
                // today. The claim is not released: retrying will not fix it.
                log::warn!(
                    "Digest-reseed fallback for job {} (session {}) did not launch: {}",
                    plan.job_id,
                    &plan.session_id[..plan.session_id.len().min(8)],
                    error
                );
            }
        })
    {
        log::warn!(
            "Failed to schedule the digest-reseed fallback for job {}: {}",
            spawn_job_id,
            error
        );
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

#[cfg(test)]
mod session_reseed_fallback_tests {
    use super::{claim_reseed_fallback, should_fall_back_to_digest_reseed};
    use crate::models::RunStatus;
    use std::collections::HashSet;
    use std::sync::Mutex;

    #[test]
    fn only_a_crashed_unresolvable_resume_falls_back() {
        assert!(should_fall_back_to_digest_reseed(
            &RunStatus::Crashed,
            Some("resume"),
            Some("session_unresolvable")
        ));
    }

    #[test]
    fn a_fresh_start_never_falls_back() {
        // A fresh run passes no --resume flag, so it cannot have failed to
        // resolve one. This is what structurally bounds the loop.
        assert!(!should_fall_back_to_digest_reseed(
            &RunStatus::Crashed,
            Some("fresh"),
            Some("session_unresolvable")
        ));
        assert!(!should_fall_back_to_digest_reseed(
            &RunStatus::Crashed,
            Some("fork"),
            Some("session_unresolvable")
        ));
        assert!(!should_fall_back_to_digest_reseed(
            &RunStatus::Crashed,
            None,
            Some("session_unresolvable")
        ));
    }

    #[test]
    fn a_clean_exit_never_falls_back() {
        assert!(!should_fall_back_to_digest_reseed(
            &RunStatus::Exited,
            Some("resume"),
            Some("session_unresolvable")
        ));
    }

    #[test]
    fn other_crash_reasons_never_fall_back() {
        // The pre-existing crashed-resume warning still covers these; they have
        // no automatic recovery.
        for reason in [None, Some("capacity_retry"), Some("turn_failed")] {
            assert!(
                !should_fall_back_to_digest_reseed(&RunStatus::Crashed, Some("resume"), reason),
                "exit reason {reason:?} must not trigger the fallback"
            );
        }
    }

    #[test]
    fn a_session_is_claimed_exactly_once() {
        let attempted = Mutex::new(HashSet::new());
        assert!(claim_reseed_fallback(&attempted, "session-a"));
        assert!(!claim_reseed_fallback(&attempted, "session-a"));
        // A distinct session identity — including the one a reseed rotates to —
        // gets its own attempt.
        assert!(claim_reseed_fallback(&attempted, "session-b"));
    }
}

#[cfg(test)]
mod ordering_tests {
    //! CAIRN-2483: the turn-end-check single-flight slot must be claimed
    //! (`spawn_turn_end_checks`) BEFORE `recompute_job` in both turn-end callers,
    //! so the recompute review-readiness hook observes this job's checks as
    //! in-flight and defers instead of racing the launch and pushing a premature
    //! parent review. Guarded structurally because the ordering is load-bearing
    //! and a silent reorder would reintroduce the race.
    const SOURCE: &str = include_str!("finalize.rs");

    fn assert_spawn_before_recompute(func_signature: &str) {
        let start = SOURCE
            .find(func_signature)
            .unwrap_or_else(|| panic!("caller {func_signature} present in source"));
        let body = &SOURCE[start..];
        let spawn = body
            .find("spawn_turn_end_checks(orch")
            .expect("spawn_turn_end_checks call present");
        let recompute = body
            .find("recompute_job(orch")
            .expect("recompute_job call present");
        assert!(
            spawn < recompute,
            "{func_signature}: spawn_turn_end_checks must precede recompute_job (CAIRN-2483)"
        );
    }

    #[test]
    fn turn_end_callers_claim_checks_slot_before_recompute() {
        assert_spawn_before_recompute("pub fn transition_to_warm_state");
        assert_spawn_before_recompute("pub fn finalize_run");
    }

    /// CAIRN-3104: the digest-reseed fallback opens a NEW turn on the job, so it
    /// must be scheduled only after `recompute_job` and the turn-end emits have
    /// settled the job's derived state. Guarded structurally for the same reason
    /// as the check above — a silent reorder would race the new turn against the
    /// recompute of the crashed one.
    #[test]
    fn reseed_fallback_is_spawned_after_recompute() {
        let start = SOURCE
            .find("pub fn finalize_run")
            .expect("finalize_run present in source");
        let body = &SOURCE[start..];
        let recompute = body
            .find("recompute_job(orch")
            .expect("recompute_job call present");
        let fallback = body
            .find("spawn_digest_reseed_fallback(orch")
            .expect("spawn_digest_reseed_fallback call present");
        assert!(
            recompute < fallback,
            "finalize_run: the reseed fallback must be spawned after recompute_job (CAIRN-3104)"
        );
    }
}
