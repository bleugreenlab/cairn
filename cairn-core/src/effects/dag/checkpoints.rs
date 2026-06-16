use super::*;

pub(super) fn handle_checkpoint_node(
    orch: &Orchestrator,
    db_job: &DbJob,
    node: &DbRecipeNode,
    execution_id: &str,
    effects: &mut Vec<WorkflowEffect>,
) -> Result<(), String> {
    // Standalone checkpoint nodes are command gates: run the command. Exit 0
    // passes (the effect loop confirms the artifact -> Complete); a non-zero exit
    // blocks the job (resumable). There is no human-approval checkpoint variant.
    let checkpoint_config: Option<crate::models::CheckpointNodeConfig> = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok());

    let command = checkpoint_config
        .as_ref()
        .and_then(|c| c.command.clone())
        .unwrap_or_else(|| "exit 0".to_string());

    let worktree_path = find_checkpoint_worktree(orch, db_job, node)?;
    let cached_result = check_checkpoint_cache(orch, db_job, &command, &worktree_path);
    let cached_pass = matches!(&cached_result, Some((0, _, true)));

    effects.push(WorkflowEffect::RunCheckpointCommand {
        job_id: db_job.id.clone(),
        node_name: node.name.clone(),
        command,
        worktree_path: PathBuf::from(&worktree_path),
        cached_pass,
        ctx: EffectContext {
            job_id: Some(db_job.id.clone()),
            run_id: None,
            execution_id: Some(execution_id.to_string()),
            source: EffectSource::DagAdvancement,
        },
    });

    Ok(())
}

fn find_checkpoint_worktree(
    orch: &Orchestrator,
    job: &DbJob,
    node: &DbRecipeNode,
) -> Result<String, String> {
    let execution_id = job.execution_id.clone().ok_or("Job has no execution_id")?;
    let parent_id = node.parent_id.clone();
    let db = orch.db.local.clone();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let parent_id = parent_id.clone();
            Box::pin(async move {
                if let Some(parent_id) = parent_id.as_deref() {
                    let mut rows = conn
                        .query(
                            "SELECT worktree_path
                             FROM jobs
                             WHERE execution_id = ?1
                               AND recipe_node_id = ?2
                               AND worktree_path IS NOT NULL
                               AND status <> 'cancelled'
                             ORDER BY created_at DESC
                             LIMIT 1",
                            params![execution_id.as_str(), parent_id],
                        )
                        .await?;
                    if let Some(row) = rows.next().await? {
                        return row.text(0);
                    }
                }

                let mut rows = conn
                    .query(
                        "SELECT worktree_path
                         FROM jobs
                         WHERE execution_id = ?1
                           AND worktree_path IS NOT NULL
                           AND status <> 'cancelled'
                         ORDER BY created_at DESC
                         LIMIT 1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| row.text(0))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::internal("No worktree found for programmatic checkpoint")
                    })
            })
        })
        .await
        .map_err(|e| format!("Failed to find checkpoint worktree: {e}"))
    })
}

fn check_checkpoint_cache(
    orch: &Orchestrator,
    checkpoint_job: &DbJob,
    command: &str,
    worktree_path: &str,
) -> Option<(i32, String, bool)> {
    let parent_job_id = checkpoint_job.parent_job_id.clone()?;
    let normalized = normalize_command(command);
    let db = orch.db.local.clone();
    let cached = run_advancement_db(async move {
        db.read(|conn| {
            let parent_job_id = parent_job_id.clone();
            let normalized = normalized.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT exit_code, commit_sha, is_dirty
                         FROM checkpoint_command_cache
                         WHERE job_id = ?1
                           AND normalized_command = ?2
                         ORDER BY ran_at DESC
                         LIMIT 1",
                        params![parent_job_id.as_str(), normalized.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.i64(0)? as i32, row.text(1)?, row.i64(2)? as i32)))
                    .transpose()
            })
        })
        .await
        .map_err(|e| format!("Failed to load checkpoint cache: {e}"))
    })
    .ok()??;

    let current_sha = get_current_head_sha(worktree_path).ok()?;
    let currently_dirty = is_worktree_dirty(worktree_path).unwrap_or(true);
    let is_valid = cached.1 == current_sha && cached.2 == 0 && !currently_dirty;

    Some((cached.0, cached.1, is_valid))
}
