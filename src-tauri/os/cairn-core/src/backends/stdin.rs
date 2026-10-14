//! Writing to a live session.
//!
//! Every write goes through the [`SessionControl`] its launch registered on
//! the run's handle: the control already knows its provider, so nothing here
//! dispatches by backend name.

use crate::agent_process::process::{
    AgentProcessState, GoalPauseClaim, PauseIntent, SessionControl,
};
use cairn_llm::interface::session::SessionInput;

/// Lock a run's control and goal claim, then call `f` with them.
fn with_session_control<T, F>(
    process_state: &AgentProcessState,
    run_id: &str,
    f: F,
) -> Result<T, String>
where
    F: FnOnce(&mut dyn SessionControl, &mut GoalPauseClaim) -> Result<T, String>,
{
    // Snapshot the handle. Never hold the process map during I/O.
    let control = {
        let processes = process_state.processes.lock().map_err(|e| e.to_string())?;
        processes
            .get(run_id)
            .ok_or_else(|| format!("Process not found: {run_id}"))?
            .goal_control()
    };
    control.with_control(f)
}

pub(crate) use super::codex::{GoalPauseCause, GoalReactivationResult};
use crate::execution::jobs::{GoalReactivation, GoalReconciliation, JobRunnable};

/// Pause a native Codex goal before a durable suspension is published.
/// Non-Codex backends have no provider-owned continuation to fence.
pub(crate) fn pause_goal_for_durable_wait(
    process_state: &AgentProcessState,
    run_id: &str,
    intent: PauseIntent,
) -> Result<Option<super::codex::DurableGoalPause>, String> {
    match with_session_control(process_state, run_id, |stdin, claim| {
        super::codex::pause_app_server_goal_for_durable_wait(stdin, claim, intent)
    }) {
        // No provider process means there is no provider-owned continuation to
        // fence. In-process tool loops and runs whose process entry has already
        // gone still need Cairn's durable wait just as they did before Codex
        // gained native goals.
        Err(error)
            if error.starts_with("Process not found:")
                || error == crate::agent_process::process::NO_SESSION_CONTROL =>
        {
            Ok(None)
        }
        result => result,
    }
}

pub(crate) fn restore_goal_after_failed_durable_wait(
    process_state: &AgentProcessState,
    run_id: &str,
    pause: Option<super::codex::DurableGoalPause>,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |stdin, claim| {
        super::codex::restore_app_server_goal_after_failed_durable_wait(stdin, claim, pause)
    })
}

pub(crate) fn defer_codex_goal_turn(
    process_state: &AgentProcessState,
    run_id: &str,
    provider_turn_id: &str,
    intent: PauseIntent,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |stdin, claim| {
        if super::codex::pause_goal_and_interrupt_provider_turn(
            stdin,
            claim,
            provider_turn_id,
            intent,
        )? {
            Ok(())
        } else {
            Err("Codex app-server stdin unavailable".to_string())
        }
    })
}

pub(crate) fn set_goal_objective(
    process_state: &AgentProcessState,
    run_id: &str,
    objective: &str,
) -> Result<bool, String> {
    with_session_control(process_state, run_id, |stdin, _claim| {
        super::codex::set_app_server_goal_objective(stdin, objective)
    })
}

pub(crate) fn interrupt_exact_codex_turn(
    process_state: &AgentProcessState,
    run_id: &str,
    provider_turn_id: &str,
) -> Result<cairn_llm::session::codex::ExactTurnInterrupt, String> {
    with_session_control(process_state, run_id, |stdin, _claim| {
        super::codex::interrupt_exact_app_server_turn(stdin, provider_turn_id)
    })
}

/// Send a user message to a running agent process.
pub(crate) fn send_user_message(
    process_state: &AgentProcessState,
    run_id: &str,
    content: &crate::agent_process::stdin::MessageContent,
    session_id: &str,
    parent_tool_use_id: Option<&str>,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |control, _claim| {
        control.send(&SessionInput {
            content,
            session_id,
            parent_tool_use_id,
        })
    })
}

/// Inspect the live status and consume only the matching authorized claim.
pub(crate) fn reactivate_goal(
    process_state: &AgentProcessState,
    run_id: &str,
    trigger: Option<&GoalReactivation>,
    runnable: JobRunnable,
) -> Result<GoalReactivationResult, String> {
    with_session_control(process_state, run_id, |stdin, claim| {
        super::codex::reactivate_app_server_goal(stdin, claim, trigger, runnable)
    })
}

/// Only the explicit operator rearm path may remove a tripped safety brake.
pub(crate) fn rearm_goal_safety_brake(
    process_state: &AgentProcessState,
    run_id: &str,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |_stdin, claim| {
        claim.rearm_safety_brake();
        Ok(())
    })
}

pub(crate) fn reconcile_goal(
    process_state: &AgentProcessState,
    run_id: &str,
    reconciliation: GoalReconciliation,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |stdin, claim| {
        super::codex::reconcile_app_server_goal(stdin, claim, reconciliation)
    })
}

/// Send an interrupt to a running agent process.
pub(crate) fn send_interrupt(
    process_state: &AgentProcessState,
    run_id: &str,
    intent: PauseIntent,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |control, claim| {
        if super::codex::pause_goal_and_interrupt_app_server_turn(control, claim, intent)? {
            return Ok(());
        }
        control.interrupt()
    })
}

/// A process that is being destroyed needs no surviving goal pause claim.
pub(crate) fn send_interrupt_before_kill(
    process_state: &AgentProcessState,
    run_id: &str,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |control, _claim| control.interrupt())
}

/// Send a model change to a running agent process.
pub(crate) fn send_set_model(
    process_state: &AgentProcessState,
    run_id: &str,
    model: &str,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |control, _claim| {
        control.set_model(model)
    })
}

/// Send a permission mode change to a running agent process.
pub(crate) fn send_set_permission_mode(
    process_state: &AgentProcessState,
    run_id: &str,
    mode: &str,
) -> Result<(), String> {
    with_session_control(process_state, run_id, |control, _claim| {
        control.set_permission_mode(mode)
    })
}
