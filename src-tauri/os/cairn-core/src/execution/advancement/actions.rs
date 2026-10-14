use super::*;

/// Advance execution DAG and handle action/condition/checkpoint nodes.
///
/// Returns agent jobs that are ready to start. All operations flow through
/// the typed effects system: `reduce_dag` produces effects, `execute_effects`
/// processes them through the core effect loop. Actions, checkpoints,
/// conditions, and branch-coordinate preparation are all handled inline by the loop.
///
/// This is the canonical entry point for DAG advancement. All callers —
/// event listeners, direct async calls, scheduler — go through this function,
/// which means all paths use `reduce_dag` as the single source of truth.
pub async fn advance_execution_with_actions(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    cairn_workflow::advancement::advance_execution_with_actions(
        &crate::workflow_host::runtime(orch),
        execution_id,
    )
    .await
}

pub fn block_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    cairn_workflow::advancement::block_job(&crate::workflow_host::runtime(orch), job_id)
}

pub fn mark_job_failed(orch: &Orchestrator, job_id: &str, error: &str) -> Result<(), String> {
    cairn_workflow::advancement::mark_job_failed(
        &crate::workflow_host::runtime(orch),
        job_id,
        error,
    )
}

/// Manual Re-run entry for a Blocked command checkpoint. Validates the job is a
/// Blocked, sessionless (command) checkpoint, clears its run history for a fresh
/// attempt cycle, re-pends it (bypassing the SHA gate — explicit user intent),
/// and advances the DAG so it re-runs. Returns any newly ready agent jobs.
pub async fn rerun_checkpoint_job(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    cairn_workflow::advancement::rerun_checkpoint_job(&crate::workflow_host::runtime(orch), job_id)
        .await
}

/// Re-evaluate a condition that blocked and routed nowhere. Refuses a condition
/// that already routed or has never been evaluated.
pub async fn reevaluate_condition(
    orch: &Orchestrator,
    execution_id: &str,
    node_id: &str,
) -> Result<cairn_workflow::conditions::ConditionVerdict, String> {
    cairn_workflow::advancement::reevaluate_condition(
        &crate::workflow_host::runtime(orch),
        execution_id,
        node_id,
    )
    .await
}

/// Whether `node_id` is a blocked condition that `reevaluate_condition` would
/// accept; returns its name. Evaluates nothing.
pub async fn reevaluable_condition(
    orch: &Orchestrator,
    execution_id: &str,
    node_id: &str,
) -> Result<String, String> {
    cairn_workflow::advancement::reevaluable_condition(
        &crate::workflow_host::runtime(orch),
        execution_id,
        node_id,
    )
    .await
}
