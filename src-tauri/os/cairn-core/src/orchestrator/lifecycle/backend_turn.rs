//! Host policy for backend turn completion. Provider facts arrive as a typed
//! report; run finalization, task resumption, and safety pauses stay here.

use crate::agent_process::completion::{
    SessionLifetime, SessionTurnCompletion, SessionTurnOutcome,
};
use crate::backends::codex::{
    emit_codex_run_turn_completed, store_usage_result_event, terminal_tool_called_for_run,
};
use crate::backends::is_task_spawned_run;
use crate::models::RunStatus;
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::services::EventEmitter;
use crate::storage::LocalDb;
use std::sync::Arc;

pub(crate) use cairn_session::agent_process::completion::TurnFailureDisposition;

/// Decisions already made by host recovery and repeat-output admission.
/// These are not provider outcomes: a completed response can require a pause.
pub(crate) use cairn_session::agent_process::completion::CodexTurnContext;

/// Apply a report after the backend has flushed its final transcript chunks.
/// Usage must be recorded before transitions clear the current Cairn turn ID.
pub(crate) fn apply_codex_turn_completion(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    completion: SessionTurnCompletion,
    context: CodexTurnContext,
) {
    if context.repeat_safety_pause {
        super::transition_to_warm_safety_pause(orch, run_id);
        emit_codex_run_turn_completed(emitter, run_id);
        return;
    }

    match completion.outcome {
        SessionTurnOutcome::Completed => {
            store_usage_result_event(
                orch,
                run_db,
                emitter,
                run_id,
                session_id,
                "result:success",
                false,
                completion.usage,
                true,
            );

            if completion.session_lifetime == SessionLifetime::OneShot {
                // The pooled thread is abandoned after this turn. Finalize
                // explicitly — driving `on_call_run_finalized` (slot release,
                // workflow journal, parent resume) — rather than warming a
                // process that no longer maps 1:1 to this run.
                super::transition_to_warm_state(orch, run_id, None);
                super::finalize_run(orch, run_id, RunStatus::Exited);
                return;
            }

            let is_task = is_task_spawned_run("Codex", run_db, run_id);
            if is_task {
                super::finalize_run(orch, run_id, RunStatus::Exited);
                orch.process_state.transition_to_warm(run_id);
            } else {
                super::transition_to_warm_state(orch, run_id, None);
                emit_codex_run_turn_completed(emitter, run_id);
            }
        }
        SessionTurnOutcome::Interrupted => {
            store_usage_result_event(
                orch,
                run_db,
                emitter,
                run_id,
                session_id,
                "result:interrupted",
                false,
                completion.usage,
                false,
            );
            handle_codex_interrupted_turn(orch, run_db, emitter, run_id, session_id);
        }
        SessionTurnOutcome::Failed { status } => {
            store_usage_result_event(
                orch,
                run_db,
                emitter,
                run_id,
                session_id,
                "result:error",
                true,
                completion.usage,
                false,
            );
            if context.failure_disposition == TurnFailureDisposition::AlreadyHandled {
                return;
            }
            if context.failure_disposition == TurnFailureDisposition::Unhandled {
                insert_error_event(
                    orch,
                    run_id,
                    session_id,
                    &format!("Codex turn failed: {}", status),
                );
            }
            // A genuine turn failure: finalize task-aware so a delegated child
            // fails terminally (its parent resumes) rather than stranding it.
            super::fail_run(orch, run_id, "turn_failed");
        }
    }
}

fn should_finalize_task_run_on_interrupted_turn(
    terminal_tool_called: bool,
    is_task_spawned: bool,
) -> bool {
    terminal_tool_called && is_task_spawned
}

fn handle_codex_interrupted_turn(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
) {
    if let Some(session_id) = session_id {
        if crate::execution::jobs::codex_repeat_breaker_armed(run_db.clone(), run_id, session_id)
            .unwrap_or(false)
        {
            super::transition_to_warm_safety_pause(orch, run_id);
            emit_codex_run_turn_completed(emitter, run_id);
            return;
        }
    }
    let terminal_tool_called = terminal_tool_called_for_run(orch, run_id);
    if should_finalize_task_run_on_interrupted_turn(
        terminal_tool_called,
        is_task_spawned_run("Codex", run_db, run_id),
    ) {
        log::info!(
            "codex interrupted after terminal tool for task-spawned run {}; finalizing as completed",
            &run_id[..run_id.len().min(8)]
        );
        super::transition_to_warm_state(
            orch,
            run_id,
            Some(crate::models::TurnEndReason::ArtifactHandoff),
        );
        super::finalize_run(orch, run_id, RunStatus::Exited);
        orch.process_state.transition_to_warm(run_id);
    } else {
        super::transition_to_warm_state(
            orch,
            run_id,
            terminal_tool_called.then_some(crate::models::TurnEndReason::ArtifactHandoff),
        );
        emit_codex_run_turn_completed(emitter, run_id);
    }
}

mod claude;
mod owned;

pub(crate) use claude::{
    apply_claude_artifact_handoff, apply_claude_turn_completion, was_host_interrupted,
    ClaudeTurnContext,
};
pub(crate) use owned::apply_owned_turn_outcome;
