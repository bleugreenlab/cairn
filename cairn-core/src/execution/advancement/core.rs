use super::*;

/// Advance an execution's DAG: claim every pending job whose control-deps are now
/// satisfied by transitioning it `pending → running` (the single claim point —
/// scanning only `pending` guarantees each job is claimed exactly once). Emits
/// db-change events and returns the claimed jobs for the host to spawn / for
/// `reduce_dag` to dispatch by node type.
pub fn advance_execution_impl(orch: &Orchestrator, execution_id: &str) -> Result<Vec<Job>, String> {
    let execution_id = execution_id.to_string();
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let execution_id = execution_id.clone();
        async move {
            crate::execution::routing::owning_db_for_execution(&dbs, &execution_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let newly_ready = run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let sql = format!(
                    "SELECT {JOB_COLUMNS}
                     FROM jobs
                     WHERE execution_id = ?1 AND status = 'pending'
                     ORDER BY created_at ASC"
                );
                let mut rows = conn.query(&sql, (execution_id.as_str(),)).await?;
                let mut pending_jobs = Vec::new();
                while let Some(row) = rows.next().await? {
                    pending_jobs.push(db_job_from_row(&row)?);
                }

                let mut newly_ready = Vec::new();
                for job in pending_jobs {
                    if is_job_ready_conn(conn, &job).await? {
                        transition_job_to_running_conn(conn, &job).await?;
                        let updated_job = load_job_by_id_conn(conn, &job.id)
                            .await?
                            .ok_or_else(|| db_internal(format!("Job not found: {}", job.id)))?;
                        newly_ready.push(Job::try_from(updated_job).map_err(db_internal)?);
                    }
                }

                Ok(newly_ready)
            })
        })
        .await
        .map_err(|e| format!("Failed to advance execution: {e}"))
    })?;

    if !newly_ready.is_empty() {
        // One scoped jobs event per newly-ready job; the frontend's invalidation
        // batch dedupes them into the minimal scoped set. Executions/issues stay
        // broad (this sweep moves an execution and possibly its issue wholesale).
        for job in &newly_ready {
            let _ = orch
                .services
                .emitter
                .emit("db-change", crate::notify::job_db_change(job, "update"));
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "executions", "action": "update"}),
        );
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "issues", "action": "update"}),
        );
    }

    Ok(newly_ready)
}
