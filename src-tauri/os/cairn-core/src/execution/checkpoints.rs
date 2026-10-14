use crate::models::Job;
use crate::orchestrator::Orchestrator;
pub use cairn_workflow::checkpoints::*;

/// Approve a blocked checkpoint job.
/// Marks the job as complete, then advances the DAG.
/// Returns the list of newly ready agent jobs after advancement.
pub async fn approve_job_inner(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await?;
    validate_confirmable(db.as_ref(), job_id).await?;

    // Record the resolution fact (artifact.confirmed), then let the projection
    // derive Complete and advance the DAG. Agent jobs from the resulting
    // AdvanceDag are started by the executor directly.
    let job_id_owned = job_id.to_string();
    let artifact = db
        .write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move { confirm_latest_artifact_conn(conn, &job_id).await })
        })
        .await
        .map_err(|e| e.to_string())?;
    orch.notifier.artifact(&artifact);
    crate::execution::advancement::recompute_job(orch, job_id)?;
    Ok(vec![])
}

/// Confirm a job's resolution (artifact.confirmed) and recompute its status.
/// Public orchestrator-level entry for hosts that approve/complete a job.
pub fn confirm_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = crate::execution::advancement::run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id_owned = job_id.to_string();
    let artifact = crate::execution::advancement::run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move { confirm_latest_artifact_conn(conn, &job_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    orch.notifier.artifact(&artifact);
    crate::execution::advancement::recompute_job(orch, job_id)
}
