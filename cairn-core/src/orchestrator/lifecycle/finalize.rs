//! Warm transition, run finalization, task-failure finalize, and memory-review
//! completion. Sliced verbatim from the former `lifecycle.rs`.

use crate::mcp::handlers::{emit_attention, AttentionEvent};
use crate::models::{RunStatus, TurnState};
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
            // Turn-end project checks (when:review), detached so the suite never
            // blocks the turn from ending. This MUST precede `recompute_job`: the
            // recompute review-readiness hook (fired when this job flips
            // terminal/idle) detaches an `evaluate_review_readiness` that reads
            // the turn-end-check single-flight markers. Claiming this job's slot
            // synchronously first guarantees that hook sees checks in-flight and
            // defers, instead of racing the launch and pushing a premature parent
            // review before this job's own review-cadence suite has even started
            // (CAIRN-2483). If the suite is skipped (memory-review turn, no
            // worktree, no applicable checks) no slot is held and the hook fires
            // correctly — there is genuinely nothing to wait for.
            spawn_turn_end_checks(orch, &job_id);
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
            maybe_reclaim_ephemeral_task_worktree(orch, &job_id);
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
fn maybe_journal_call_result(
    orch: &Orchestrator,
    run_id: &str,
    job_id: Option<&str>,
    status: RunStatus,
) {
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
            // Success iff a clean exit produced a result artifact; anything else
            // (a crash, a terminal run with no artifact) is a journaled failure
            // that replays as `null`, mirroring the `?wait` terminal mapping.
            let (result_json, jstatus) = if status == RunStatus::Exited {
                // The call's artifact lives in the run's owning database (a team
                // run in its replica); route to it rather than assuming private.
                let owning = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                    .await
                    .unwrap_or_else(|_| dbs.local.clone());
                match latest_artifact_data(&owning, &job_id).await? {
                    Some(data) => (Some(data), crate::workflow_journal::JournalStatus::Success),
                    None => (None, crate::workflow_journal::JournalStatus::Failure),
                }
            } else {
                (None, crate::workflow_journal::JournalStatus::Failure)
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

/// Raw `data` of the most recent artifact written for a job, or `None`.
async fn latest_artifact_data(
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM artifacts WHERE job_id = ?1 \
                     ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
        })
    })
    .await
    .map_err(|e| e.to_string())
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

/// Reclaim a task's ephemeral worktree the moment its owning job terminalizes.
///
/// A task (or Inherit-mode call/workflow) delegated by an ambient (no-worktree)
/// parent runs in its own throwaway worktree marked `owns_ephemeral_worktree`; it
/// has no PR machinery, so nothing else tears it down. When the job reaches a
/// terminal status, discard that one worktree — detached so it never blocks the
/// turn from ending. A task suspended waiting on its own sub-tasks is not
/// terminal, so this cannot fire while inheritors still share the worktree; by
/// the time the task terminalizes they already have. Both turn-end sites (warm
/// transition and run finalize) call this after recompute so a clean completion
/// and a crash are covered.
///
/// Exception for a restartable workflow: a workflow keeps a `workflow_run`
/// re-dispatch record across every *restartable* terminal state (deliberate
/// stop / script failure / crash) and drops it only on clean completion, and
/// `restart_workflow` respawns into the worktree's persisted `working_dir`. So
/// while that record still exists the worktree must survive — the reclaim is
/// bound to the record's lifetime, not to bare terminalization. Clean completion
/// clears the record *before* `finalize_run`, so the reclaim still fires then;
/// the worktree GC's terminal-`owns_ephemeral_worktree` backstop catches any
/// stray a never-restarted, record-dropped workflow leaves behind.
fn maybe_reclaim_ephemeral_task_worktree(orch: &Orchestrator, job_id: &str) {
    let Some((status, owns)) = load_job_status_and_ephemeral(orch, job_id) else {
        return;
    };
    if !owns || !matches!(status.as_str(), "complete" | "failed" | "cancelled") {
        return;
    }
    // A surviving workflow_run record marks a restartable workflow whose worktree
    // Restart still needs; defer reclaim until the record is dropped.
    let record_exists = crate::storage::run_db_blocking({
        let db = orch.db.local.clone();
        let job_id = job_id.to_string();
        move || async move {
            crate::execution::jobs::workflow_run_record_exists(&db, &job_id).await
        }
    })
    .unwrap_or(false);
    if record_exists {
        return;
    }
    let orch = orch.clone();
    let job_id = job_id.to_string();
    detach_onto_runtime(
        async move {
            if let Err(e) = crate::execution::teardown::teardown_worktrees(
                &orch,
                crate::execution::teardown::TeardownScope::Job(job_id.clone()),
                crate::execution::teardown::TeardownReason::Discarded,
            )
            .await
            {
                log::warn!("ephemeral task worktree reclaim failed for {job_id}: {e}");
            }
        },
        || {},
    );
}

fn load_job_status_and_ephemeral(orch: &Orchestrator, job_id: &str) -> Option<(String, bool)> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, owns_ephemeral_worktree FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.i64(1)? != 0)),
                    None => None,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
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
    maybe_journal_call_result(orch, run_id, job_id.as_deref(), status.clone());

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
            finish_memory_review_if_due(orch, &job_id, run_id);
            // Turn-end on any terminal run outcome (clean exit or crash): the
            // agent is idle now. The recompute above may have flipped status
            // to terminal (→ Resolved) or left attention pointing at the next
            // human action (→ AgentIdleWithWork). Either way, the long-poll
            // hears about it through this fact rather than the recompute-sweep
            // poke this work removes.
            emit_for_turn_end(orch, &job_id);
            maybe_reclaim_ephemeral_task_worktree(orch, &job_id);
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
}
