use super::*;

pub async fn release_dependent_executions(
    orch: &Orchestrator,
    resolved_issue_id: &str,
) -> Result<(), String> {
    let resolved_issue_id = resolved_issue_id.to_string();
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let resolved_issue_id = resolved_issue_id.clone();
        async move {
            crate::issues::crud::owning_db_for_issue(&dbs, &resolved_issue_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let execution_ids = run_advancement_db(async move {
        db.read(|conn| {
            let resolved_issue_id = resolved_issue_id.clone();
            Box::pin(async move {
                let resolved_uri = crate::issues::relations::issue_uri_for_id(
                    conn,
                    &resolved_issue_id,
                )
                .await?;
                let dependent_ids = crate::issues::relations::list_dependent_issue_ids(
                    conn,
                    &resolved_uri,
                )
                .await?;
                let mut execution_ids = Vec::new();
                for dependent_id in dependent_ids {
                    if !crate::issues::relations::dependencies_ready(conn, &dependent_id).await? {
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
        recompute_execution_jobs(orch, &execution_id)?;
        match crate::effects::outbox::insert_pending_with_payload_async(
            &orch.db.local,
            "advance_dag",
            &execution_id,
            "{}",
        )
        .await
        {
            Ok(entry_id) => {
                if let Some(ref tx) = orch.effect_tx {
                    let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
                        execution_id: execution_id.clone(),
                        outbox_entry_id: Some(entry_id),
                    });
                } else {
                    log::debug!(
                        "No effect_tx configured — relying on outbox replay for dependent execution {}",
                        &execution_id[..execution_id.len().min(8)]
                    );
                }
            }
            Err(error) => log::warn!(
                "Failed to enqueue dependent execution advance for {}: {}",
                execution_id,
                error
            ),
        }
    }

    orch.notifier.emit_change("issues");
    Ok(())
}
