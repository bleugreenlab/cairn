use super::types::WorkflowEffect;
use crate::orchestrator::Orchestrator;

pub fn approve_job_pure(orch: &Orchestrator, job_id: &str) -> Result<Vec<WorkflowEffect>, String> {
    cairn_workflow::effects::checkpoint::approve_job_pure(
        &crate::workflow_host::runtime(orch),
        job_id,
    )
}
