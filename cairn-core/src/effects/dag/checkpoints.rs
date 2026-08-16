use super::*;

pub(super) fn handle_checkpoint_node(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    db_job: &DbJob,
    node: &DbRecipeNode,
    execution_id: &str,
    effects: &mut Vec<WorkflowEffect>,
) -> Result<(), String> {
    let checkpoint_config: Option<crate::models::CheckpointNodeConfig> = node
        .config
        .as_ref()
        .and_then(|config| serde_json::from_str(config).ok());
    let command = checkpoint_config
        .as_ref()
        .and_then(|config| config.command.clone())
        .unwrap_or_else(|| "exit 0".to_string());

    let coordinate = resolve_checkpoint_coordinate(orch, db, db_job, node)?;
    let cached_pass = matches!(
        check_checkpoint_cache(db, db_job, &command, &coordinate),
        Some((0, _, true))
    );

    effects.push(WorkflowEffect::RunCheckpointCommand {
        job_id: db_job.id.clone(),
        node_name: node.name.clone(),
        command,
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

/// Resolve the branch coordinate a checkpoint verifies. A checkpoint attached to
/// a parent node follows that parent's newest live job; otherwise it verifies its
/// own job branch. Neither case allocates or inspects a checkout.
fn resolve_checkpoint_coordinate(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    job: &DbJob,
    node: &DbRecipeNode,
) -> Result<String, String> {
    let execution_id = job.execution_id.clone().ok_or("Job has no execution_id")?;
    let checkpoint_job_id = job.id.clone();
    let parent_id = node.parent_id.clone();
    let db = db.clone();
    let (branch, repo_path) = run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let checkpoint_job_id = checkpoint_job_id.clone();
            let parent_id = parent_id.clone();
            Box::pin(async move {
                if let Some(parent_id) = parent_id.as_deref() {
                    let mut rows = conn
                        .query(
                            "SELECT j.branch, p.repo_path
                             FROM jobs j
                             JOIN projects p ON p.id = j.project_id
                             WHERE j.execution_id = ?1
                               AND j.recipe_node_id = ?2
                               AND j.branch IS NOT NULL
                               AND j.status <> 'cancelled'
                             ORDER BY j.created_at DESC
                             LIMIT 1",
                            params![execution_id.as_str(), parent_id],
                        )
                        .await?;
                    if let Some(row) = rows.next().await? {
                        return Ok((row.text(0)?, row.text(1)?));
                    }
                }

                let mut rows = conn
                    .query(
                        "SELECT j.branch, p.repo_path
                         FROM jobs j
                         JOIN projects p ON p.id = j.project_id
                         WHERE j.id = ?1 AND j.branch IS NOT NULL
                         LIMIT 1",
                        (checkpoint_job_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?)))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::internal("Checkpoint has no resolvable branch coordinate")
                    })
            })
        })
        .await
        .map_err(|error| format!("Failed to resolve checkpoint coordinate: {error}"))
    })?;

    let repository = PathBuf::from(repo_path);
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let project_repo = repository.clone();
    let coordinate_repository = run_advancement_db(async move {
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::JjEnv::resolve(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("checkpoint coordinate repository task failed: {error}"))?
    })?;
    run_advancement_db(async move {
        cairn_vcs::resolve_coordinate(&coordinate_repository, &branch)
            .await
            .map_err(|error| format!("Checkpoint branch '{branch}' is unresolvable: {error}"))
    })
}

fn check_checkpoint_cache(
    db: &Arc<LocalDb>,
    checkpoint_job: &DbJob,
    command: &str,
    current_coordinate: &str,
) -> Option<(i32, String, bool)> {
    let parent_job_id = checkpoint_job.parent_job_id.clone()?;
    let normalized = normalize_command(command);
    let db = db.clone();
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
        .map_err(|error| format!("Failed to load checkpoint cache: {error}"))
    })
    .ok()??;

    let is_valid = cached.1 == current_coordinate && cached.2 == 0;
    Some((cached.0, cached.1, is_valid))
}
