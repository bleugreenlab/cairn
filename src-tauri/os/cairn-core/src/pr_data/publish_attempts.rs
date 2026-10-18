//! Application facade for the workflow-owned PR publication journal.
use crate::orchestrator::Orchestrator;
pub use cairn_workflow::pr_publish_attempts::{
    is_pr_publish_retry_eligible, PublishAttemptRow, PR_PUBLISH_MAX_ATTEMPTS,
};

pub async fn list_publish_attempts(
    orch: &Orchestrator,
    id: &str,
) -> Result<Vec<PublishAttemptRow>, String> {
    let db = crate::execution::routing::owning_db_for_action_run(&orch.db, id).await?;
    cairn_workflow::pr_publish_attempts::list_publish_attempts(&db, id).await
}
pub async fn latest_publish_attempt(
    orch: &Orchestrator,
    id: &str,
) -> Result<Option<PublishAttemptRow>, String> {
    Ok(list_publish_attempts(orch, id).await?.into_iter().next())
}
pub async fn publish_attempt_count(orch: &Orchestrator, id: &str) -> Result<i64, String> {
    Ok(latest_publish_attempt(orch, id)
        .await?
        .map_or(0, |a| a.attempt))
}
pub async fn settle_publish_attempt(
    orch: &Orchestrator,
    id: &str,
    outcome: &str,
    detail: &str,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_action_run(&orch.db, id).await?;
    cairn_workflow::pr_publish_attempts::settle_publish_attempt(&db, id, outcome, detail).await
}
