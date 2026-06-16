//! Checkpoint approval/rejection for the effect loop (programmatic checkpoints).
//!
//! These record the resolution fact and recompute the job's status; the
//! recompute emits its own follow-on effects (lifecycle message, DAG advance,
//! manager wake), so these return no effects of their own.

use crate::orchestrator::Orchestrator;

use super::types::WorkflowEffect;

/// Confirm the checkpoint's resolution; the projection derives Complete and
/// advances the DAG. A passing programmatic check confirms via this path.
pub fn approve_job_pure(orch: &Orchestrator, job_id: &str) -> Result<Vec<WorkflowEffect>, String> {
    let db = orch.db.local.clone();
    let job_id_owned = job_id.to_string();
    crate::execution::advancement::run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                crate::execution::checkpoints::confirm_latest_artifact_conn(conn, &job_id).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    crate::execution::advancement::recompute_job(orch, job_id)?;
    Ok(Vec::new())
}
