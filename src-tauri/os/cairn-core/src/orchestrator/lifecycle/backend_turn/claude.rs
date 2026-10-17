//! Claude Result policy after the reader has persisted the provider event.

use crate::agent_process::completion::ClaudeTurnOutcome;
use crate::backends::is_task_spawned_run;
use crate::models::{RunStatus, TurnEndReason};
use crate::orchestrator::lifecycle::{finalize_run, transition_to_warm_state};
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::services::EventEmitter;
use crate::storage::LocalDb;
use std::sync::Arc;

pub(crate) use cairn_session::agent_process::completion::ClaudeTurnContext;

pub(crate) fn apply_claude_turn_completion(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    outcome: ClaudeTurnOutcome,
    context: ClaudeTurnContext,
) {
    // A committed terminal tool is authoritative even when its cleanup Result
    // reports an error. This precedes the delayed-ack path, matching the reader.
    if !context.terminal_tool_completed {
        match outcome {
            ClaudeTurnOutcome::Failed { message } => {
                if context.terminal_interrupt_ack_pending {
                    // A successor (for example memory review) may already be
                    // running on this process. The acknowledgement owns no turn.
                    log::info!("Swallowed terminal-tool interrupt ack for run {} (work turn already warmed)", &run_id[..run_id.len().min(8)]);
                    return;
                }
                // A provider depletion never reaches here: the reader hands it
                // to the usage-limit hold instead of completing the turn.
                if was_host_interrupted(orch, run_id) {
                    warm_and_emit(orch, emitter, run_id, None);
                } else {
                    insert_error_event(orch, run_id, session_id, &message);
                    // Claude's existing failure contract leaves the turn
                    // resumable; Codex/HTTP task-aware failure is different.
                    finalize_run(orch, run_id, RunStatus::Crashed);
                }
                return;
            }
            ClaudeTurnOutcome::Completed => {}
        }
    }

    if is_task_spawned_run("Claude", run_db, run_id) {
        finalize_run(orch, run_id, RunStatus::Exited);
        orch.process_state.transition_to_warm(run_id);
    } else {
        warm_and_emit(orch, emitter, run_id, None);
    }
}

/// The work boundary precedes the delayed CLI interrupt acknowledgement.
pub(crate) fn apply_claude_artifact_handoff(
    orch: &Orchestrator,
    emitter: &Arc<dyn EventEmitter>,
    run_id: &str,
) {
    warm_and_emit(orch, emitter, run_id, Some(TurnEndReason::ArtifactHandoff));
}

fn warm_and_emit(
    orch: &Orchestrator,
    emitter: &Arc<dyn EventEmitter>,
    run_id: &str,
    reason: Option<TurnEndReason>,
) {
    transition_to_warm_state(orch, run_id, reason);
    let _ = emitter.emit(
        "run-turn-completed",
        serde_json::json!({"run_id":run_id,"is_warm":true}),
    );
}

/// The host parks a process warm after sending an interrupt. The subsequent
/// error Result or EOF is transport cleanup rather than a new failure.
///
/// Warm, not merely `Idle`: a launch whose first turn has not begun yet is
/// `Idle` as well, and a CLI that dies in that window (a refused start) died on
/// its own.
pub(crate) fn was_host_interrupted(orch: &Orchestrator, run_id: &str) -> bool {
    orch.process_state.is_warm(run_id)
}
