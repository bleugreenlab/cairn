use super::types::WorkflowEffect;
use crate::orchestrator::Orchestrator;
pub use cairn_workflow::effects::run::EffectLoopReport;

/// Run the effect loop. Agent jobs a failed loop had claimed but not handed
/// over are started (or failed) before the error is returned, as for
/// top-level advancement.
pub async fn execute_effects(
    orch: &Orchestrator,
    effects: Vec<WorkflowEffect>,
) -> Result<EffectLoopReport, String> {
    let result = cairn_workflow::effects::run::execute_effects(
        &crate::workflow_host::runtime(orch),
        effects,
    )
    .await;
    crate::execution::advancement::settle_advance(orch, result).await
}
