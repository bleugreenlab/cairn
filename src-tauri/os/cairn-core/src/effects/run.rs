use super::types::WorkflowEffect;
use crate::orchestrator::Orchestrator;
pub use cairn_workflow::effects::run::EffectLoopReport;

pub async fn execute_effects(
    orch: &Orchestrator,
    effects: Vec<WorkflowEffect>,
) -> Result<EffectLoopReport, String> {
    cairn_workflow::effects::run::execute_effects(&crate::workflow_host::runtime(orch), effects)
        .await
}
