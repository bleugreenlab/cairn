use crate::orchestrator::Orchestrator;
use cairn_workflow::checkpoint_runs::CheckpointRunRow;

/// The most recent recorded run for a checkpoint job, if any.
pub fn latest_checkpoint_run(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<CheckpointRunRow>, String> {
    cairn_workflow::checkpoint_runs::latest_checkpoint_run(
        &crate::workflow_host::runtime(orch),
        job_id,
    )
}
