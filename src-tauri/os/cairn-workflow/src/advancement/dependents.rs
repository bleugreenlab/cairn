use super::*;

pub async fn release_dependent_executions(
    runtime: &WorkflowRuntime,
    resolved_issue_id: &str,
) -> Result<(), String> {
    let resolved_issue_id = resolved_issue_id.to_string();
    let db = runtime.database(WorkflowOwner::Issue(&resolved_issue_id))?;
    let execution_ids = run_advancement_db(async move {
        db.read(|conn| {
            let resolved_issue_id = resolved_issue_id.clone();
            Box::pin(async move {
                let resolved_uri = cairn_db::relations::issue_uri_for_id(
                    conn,
                    &resolved_issue_id,
                )
                .await?;
                let dependent_ids = cairn_db::relations::list_dependent_issue_ids(
                    conn,
                    &resolved_uri,
                )
                .await?;
                let mut execution_ids = Vec::new();
                for dependent_id in dependent_ids {
                    if !cairn_db::relations::dependencies_ready(conn, &dependent_id).await? {
                        continue;
                    }
                    let mut rows = conn
                        .query(
                            "SELECT id FROM executions WHERE issue_id = ?1 AND status = 'running' ORDER BY started_at ASC",
                            params![dependent_id.as_str()],
                        )
                        .await?;
                    while let Some(row) = rows.next().await? {
                        execution_ids.push(row.text(0)?);
                    }
                }
                Ok(execution_ids)
            })
        })
        .await
        .map_err(|e| format!("Failed to list dependent executions: {e}"))
    })?;

    for execution_id in execution_ids {
        recompute_execution_jobs(runtime, &execution_id)?;
        if let Err(error) = runtime.enqueue_advance(&execution_id).await {
            log::warn!("Failed to enqueue dependent execution advance for {execution_id}: {error}");
        }
    }

    let _ = runtime
        .emitter
        .emit("db-change", serde_json::json!({"table": "issues"}));
    Ok(())
}
