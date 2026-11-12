//! Host policy for backend turn completion. Provider facts arrive as a typed
//! report; run finalization and task resumption stay here.

use crate::agent_process::completion::{
    ProviderInterruption, SessionLifetime, SessionTurnCompletion, SessionTurnOutcome,
};
use crate::backends::codex::{
    emit_codex_run_turn_completed, store_usage_result_event, terminal_tool_called_for_run,
};
use crate::models::RunStatus;
use crate::models::TurnEndReason;
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::services::EventEmitter;
use crate::storage::LocalDb;
use cairn_models::interface::session::InterruptCause;
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
                None,
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
        SessionTurnOutcome::Interrupted(interruption) => {
            // Stored even without usage: it is the ledger's record of who
            // stopped the turn.
            store_usage_result_event(
                orch,
                run_db,
                emitter,
                run_id,
                session_id,
                "result:interrupted",
                false,
                completion.usage,
                true,
                Some(interruption_record(&interruption)),
            );
            handle_codex_interrupted_turn(orch, emitter, run_id, interruption.cause.as_ref());
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
                None,
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

/// The `result:interrupted` event's record of who stopped the turn, so the
/// ledger answers it without the watchdog's lease table.
fn interruption_record(interruption: &ProviderInterruption) -> serde_json::Value {
    serde_json::json!({
        "providerTurnId": interruption.provider_turn_id,
        "providerReason": interruption.provider_reason,
        "interruptCause": interruption.cause.as_ref().map(InterruptCause::as_str),
        "exitReason": match &interruption.cause {
            Some(InterruptCause::Teardown { exit_reason }) => Some(exit_reason.as_str()),
            _ => None,
        },
    })
}

/// What an interrupted Codex turn's Cairn turn becomes, by who asked for it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum InterruptSettlement {
    /// The turn handed its work off through a terminal tool: it completes.
    Handoff,
    /// It was cut off before finishing: interrupted, with this reason.
    Interrupted(TurnEndReason),
    /// Whoever asked has already settled the Cairn turn (a stop settles it
    /// `user_stop`, a park yields it), or the provider turn was never Cairn's
    /// (a deferred goal turn): nothing is written over that.
    AlreadySettled,
    /// The process is being torn down. The teardown finalizes the run right
    /// after, and its finalization settles the turn by the teardown's own
    /// exit reason, so this abort writes nothing and warms nothing: the
    /// result cannot depend on whether the abort beat the process exit.
    Teardown,
}

/// Decide [`InterruptSettlement`]. A provider abort with no recorded cause
/// was the provider's own and cuts the turn off. It must never read as
/// complete; that is how a watchdog's own kill was once mistaken for a
/// natural completion and no successor started (cairn/4634).
pub(crate) fn interrupt_settlement(
    terminal_tool_called: bool,
    cause: Option<&InterruptCause>,
) -> InterruptSettlement {
    if terminal_tool_called {
        return InterruptSettlement::Handoff;
    }
    match cause {
        None => InterruptSettlement::Interrupted(TurnEndReason::ProviderInterrupt),
        Some(InterruptCause::WatchdogPostTool) => {
            InterruptSettlement::Interrupted(TurnEndReason::WatchdogRecovery)
        }
        Some(InterruptCause::ArtifactHandoff) => InterruptSettlement::Handoff,
        Some(
            InterruptCause::OperatorStop
            | InterruptCause::DurablePark
            | InterruptCause::GoalDeferral,
        ) => InterruptSettlement::AlreadySettled,
        // A pooled thread is released only as its run is torn down.
        Some(InterruptCause::Teardown { .. } | InterruptCause::ThreadRelease) => {
            InterruptSettlement::Teardown
        }
        // A one-shot call is read by its own reader and owns no Cairn turn.
        Some(InterruptCause::OneShotStop) => InterruptSettlement::AlreadySettled,
    }
}

/// An interrupted Codex turn settles as [`interrupt_settlement`] decides, and
/// the process then ends warm: the run stays live and the native conversation
/// is kept.
fn handle_codex_interrupted_turn(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    cause: Option<&InterruptCause>,
) {
    let settlement = interrupt_settlement(terminal_tool_called_for_run(orch, run_id), cause);
    let warm_reason = match settlement {
        InterruptSettlement::Teardown => {
            log::info!(
                "Codex turn on run {} aborted by its teardown; the teardown settles it",
                &run_id[..run_id.len().min(8)]
            );
            return;
        }
        InterruptSettlement::Handoff => Some(TurnEndReason::ArtifactHandoff),
        InterruptSettlement::AlreadySettled => None,
        InterruptSettlement::Interrupted(reason) => {
            // `interrupt_turn` is the sole writer of `interrupted`; after it
            // the warm transition's Complete is refused by the terminal row.
            if let Some(turn_id) = orch.process_state.get_current_turn_id(run_id) {
                if let Err(error) =
                    super::common::interrupt_turn(orch, &turn_id, Some(reason.clone()))
                {
                    log::info!(
                        "Interrupted Codex turn {turn_id} was already settled; leaving it ({reason}): {error}"
                    );
                }
            }
            None
        }
    };
    super::transition_to_warm_state(orch, run_id, warm_reason);
    emit_codex_run_turn_completed(emitter, run_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every cause, and no cause, settles an interrupted Codex turn the one
    /// way it should. None of them is a crash or a failure.
    #[test]
    fn every_interrupt_cause_settles_its_turn_truthfully() {
        let teardown = |exit_reason: &str| InterruptCause::Teardown {
            exit_reason: exit_reason.to_string(),
        };
        let cases = [
            (
                None,
                InterruptSettlement::Interrupted(TurnEndReason::ProviderInterrupt),
            ),
            (
                Some(InterruptCause::WatchdogPostTool),
                InterruptSettlement::Interrupted(TurnEndReason::WatchdogRecovery),
            ),
            (
                Some(InterruptCause::OperatorStop),
                InterruptSettlement::AlreadySettled,
            ),
            (
                Some(InterruptCause::DurablePark),
                InterruptSettlement::AlreadySettled,
            ),
            (
                Some(InterruptCause::GoalDeferral),
                InterruptSettlement::AlreadySettled,
            ),
            (
                Some(InterruptCause::ArtifactHandoff),
                InterruptSettlement::Handoff,
            ),
            (
                Some(teardown("provider_silence_recovery")),
                InterruptSettlement::Teardown,
            ),
            (Some(teardown("user_stop")), InterruptSettlement::Teardown),
            (Some(teardown("anything")), InterruptSettlement::Teardown),
            (
                Some(InterruptCause::ThreadRelease),
                InterruptSettlement::Teardown,
            ),
            (
                Some(InterruptCause::OneShotStop),
                InterruptSettlement::AlreadySettled,
            ),
        ];
        for (cause, expected) in cases {
            assert_eq!(
                interrupt_settlement(false, cause.as_ref()),
                expected,
                "{cause:?}"
            );
            assert_eq!(
                interrupt_settlement(true, cause.as_ref()),
                InterruptSettlement::Handoff,
                "a terminal tool's handoff outranks the cause: {cause:?}"
            );
            if let InterruptSettlement::Interrupted(reason) = expected {
                assert!(
                    !matches!(
                        reason,
                        TurnEndReason::Crash | TurnEndReason::ProviderFailure
                    ),
                    "no interrupt is a failure: {cause:?}"
                );
            }
        }
    }
}

mod claude;
mod owned;

pub(crate) use claude::{
    apply_claude_artifact_handoff, apply_claude_turn_completion, was_host_interrupted,
    ClaudeTurnContext,
};
pub(crate) use owned::apply_owned_turn_outcome;
