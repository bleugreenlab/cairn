//! Lifecycle policy for an owned HTTP turn loop that has stopped producing work.

use crate::agent_process::completion::OwnedTurnOutcome;
use crate::models::RunStatus;
use crate::orchestrator::lifecycle::{fail_run, finalize_run, set_exit_reason};
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use cairn_models::turn::SuspendKind;

pub(crate) fn apply_owned_turn_outcome(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    backend_name: &str,
    outcome: OwnedTurnOutcome,
) {
    match outcome {
        OwnedTurnOutcome::Completed => finalize_run(orch, run_id, RunStatus::Exited),
        OwnedTurnOutcome::Cancelled => {
            let _ = set_exit_reason(orch, run_id, "user_stop");
            finalize_run(orch, run_id, RunStatus::Exited);
        }
        OwnedTurnOutcome::Suspended(kind) => {
            let reason = match kind {
                SuspendKind::Prompt => "prompt_wait_suspended",
                SuspendKind::DelegatedTask => "delegated_wait_suspended",
            };
            let _ = set_exit_reason(orch, run_id, reason);
            finalize_run(orch, run_id, RunStatus::Exited);
        }
        OwnedTurnOutcome::Failed { message } => {
            let detail = format!("{backend_name} turn failed: {message}");
            log::error!("{detail}");
            insert_error_event(orch, run_id, Some(session_id), &detail);
            fail_run(orch, run_id, "turn_failed");
        }
        OwnedTurnOutcome::Panicked { message } => {
            let detail = format!("{backend_name} turn panicked: {message}");
            log::error!("{detail}");
            insert_error_event(orch, run_id, Some(session_id), &detail);
            fail_run(orch, run_id, "turn_panicked");
        }
    }
    // Finalization broadcasts and resumes parents before releasing the owned
    // loop's handle. It has no warm stdin to retain on any of these paths.
    crate::orchestrator::lifecycle::release_and_stop_process(orch, run_id);
}
