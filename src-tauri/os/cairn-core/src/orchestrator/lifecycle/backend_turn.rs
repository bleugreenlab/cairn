//! Host policy for backend turn completion. Provider facts arrive as a typed
//! report; run finalization and task resumption stay here.

use crate::agent_process::completion::{
    SessionLifetime, SessionTurnCompletion, SessionTurnOutcome,
};
use crate::backends::codex::{
    emit_codex_run_turn_completed, store_usage_result_event, terminal_tool_called_for_run,
};
use crate::models::RunStatus;
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::services::EventEmitter;
use crate::storage::LocalDb;
use std::sync::Arc;

pub(crate) use cairn_session::agent_process::completion::TurnFailureDisposition;

/// Decisions already made by host recovery. These are not provider outcomes.
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

            // A delegated task's persistent session keeps its run live across
            // turns, exactly like a top-level run: the re-prompt, follow-up and
            // wait-resolved turns it takes next all start on this run.
            super::transition_to_warm_state(orch, run_id, None);
            emit_codex_run_turn_completed(emitter, run_id);
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
            handle_codex_interrupted_turn(orch, emitter, run_id);
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
            // A failed provider turn on a persistent session ends the turn, not
            // the process: the run stays live and the native conversation is
            // kept. A delegated child or workflow still fails terminally, so the
            // parent waiting on it hears the failure instead of a stranded child.
            if completion.session_lifetime == SessionLifetime::Persistent
                && !super::finalize::run_fails_terminally(orch, run_id)
                && super::settle_provider_turn_failure(orch, run_id)
            {
                emit_codex_run_turn_completed(emitter, run_id);
                if let Err(error) =
                    crate::messages::delivery::flush_pending_directs_on_idle(orch, run_id)
                {
                    log::warn!("provider-failure queued delivery flush failed: {error}");
                }
                return;
            }
            // A genuine turn failure: finalize task-aware so a delegated child
            // fails terminally (its parent resumes) rather than stranding it.
            super::fail_run(orch, run_id, "turn_failed");
        }
    }
}

/// An interrupted Codex turn ends warm. One interrupted after a terminal tool
/// (a task's `return`) handed its work off, so it completes as such; the run
/// stays live either way.
fn handle_codex_interrupted_turn(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
) {
    let terminal_tool_called = terminal_tool_called_for_run(orch, run_id);
    super::transition_to_warm_state(
        orch,
        run_id,
        terminal_tool_called.then_some(crate::models::TurnEndReason::ArtifactHandoff),
    );
    emit_codex_run_turn_completed(emitter, run_id);
}

mod claude;
mod owned;

pub(crate) use claude::{
    apply_claude_artifact_handoff, apply_claude_turn_completion, was_host_interrupted,
    ClaudeTurnContext,
};
pub(crate) use owned::apply_owned_turn_outcome;
