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
///
/// Every DAG reduction (this one and any cascading `AdvanceDag`) runs under the
/// execution's snapshot lock, the one packet persistence and settlement take,
/// through `WorkflowHost::execution_snapshot_guard`. So expanding delegated
/// packets never interleaves with them: no reader sees a packet marked
/// materialized whose job is still being created, and the reduction's snapshot
/// rewrite cannot drop a packet persisted while it ran.
///
/// A failure after agent jobs were claimed hands those jobs to a runner here
/// (see [`hand_over_claimed_jobs`]); callers see only the error, exactly as
/// before.
pub async fn advance_execution_with_actions(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    let result = cairn_workflow::advancement::advance_execution_with_actions(
        &crate::workflow_host::runtime(orch),
        execution_id,
    )
    .await;
    settle_advance(orch, result).await
}

/// Turn a workflow-layer result into the host's, first settling any agent jobs
/// the failure left claimed and unhanded. Every host entry into advancement or
/// the effect loop goes through this.
pub(crate) async fn settle_advance<T>(
    orch: &Orchestrator,
    result: Result<T, cairn_workflow::advancement::AdvanceError>,
) -> Result<T, String> {
    match result {
        Ok(jobs) => Ok(jobs),
        Err(error) => {
            if !error.claimed_agent_jobs.is_empty() {
                hand_over_claimed_jobs(orch, error.claimed_agent_jobs, &error.message).await;
            }
            Err(error.message)
        }
    }
}

/// Start the agent jobs a failed advancement had already claimed.
///
/// A caller given an advancement error dispatches nothing, and no later
/// advancement scans a `running` job, so without this a claimed job would sit
/// `running` with no run until boot repair — with any parent parked on it.
/// They are handed to the runner exactly as a successful advancement's caller
/// would; if even that fails, each is failed through `mark_job_start_failed`,
/// which also answers whoever waits on it.
async fn hand_over_claimed_jobs(orch: &Orchestrator, jobs: Vec<Job>, cause: &str) {
    let handed = match orch.executor.get() {
        Some(executor) => executor
            .execute(
                orch,
                crate::effects::types::WorkflowEffect::StartAgentJobs(jobs.clone()),
            )
            .await
            .map(|_| ()),
        None => Err("no executor is configured".to_string()),
    };
    let Err(handover) = handed else {
        return;
    };
    for job in jobs {
        let db = crate::execution::routing::owning_db_for_job(&orch.db, &job.id)
            .await
            .unwrap_or_else(|_| orch.db.local.clone());
        crate::execution::jobs::mark_job_start_failed(
            orch,
            &db,
            &job.id,
            &format!(
                "advancement failed after claiming this job ({cause}), and handing it to a runner failed: {handover}"
            ),
            true,
        )
        .await;
    }
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
    let result = cairn_workflow::advancement::rerun_checkpoint_job(
        &crate::workflow_host::runtime(orch),
        job_id,
    )
    .await;
    settle_advance(orch, result).await
}

/// Re-evaluate a condition that blocked and routed nowhere. Refuses a condition
/// that already routed or has never been evaluated.
pub async fn reevaluate_condition(
    orch: &Orchestrator,
    execution_id: &str,
    node_id: &str,
) -> Result<cairn_workflow::conditions::ConditionVerdict, String> {
    let result = cairn_workflow::advancement::reevaluate_condition(
        &crate::workflow_host::runtime(orch),
        execution_id,
        node_id,
    )
    .await;
    settle_advance(orch, result).await
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

pub fn rearm_failed_pr_actions(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<cairn_workflow::pr_publish_attempts::RearmedPrAction>, String> {
    cairn_workflow::advancement::rearm_failed_pr_actions(
        &crate::workflow_host::runtime(orch),
        execution_id,
    )
}
pub async fn validate_pr_publish_retry(
    orch: &Orchestrator,
    action_run_id: &str,
) -> Result<(), String> {
    cairn_workflow::advancement::validate_pr_publish_retry(
        &crate::workflow_host::runtime(orch),
        action_run_id,
    )
    .await
}
pub async fn retry_pr_publish(orch: &Orchestrator, action_run_id: &str) -> Result<(), String> {
    cairn_workflow::advancement::retry_pr_publish(
        &crate::workflow_host::runtime(orch),
        action_run_id,
    )
    .await
}

/// Recover a persisted PR dispatch after host restart.
pub async fn replay_pr_publish(
    orch: &Orchestrator,
    payload: &str,
) -> Result<Option<crate::effects::types::WorkflowEffect>, String> {
    cairn_workflow::pr_publish_attempts::replay_dispatch(
        &crate::workflow_host::runtime(orch),
        payload,
        orch.boot_at.saturating_mul(1000),
    )
    .await
}
