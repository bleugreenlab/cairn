use super::*;

// ============================================================================
// create_child_task
// ============================================================================

/// Create a user-initiated child task under a running job.
///
/// The child inherits the parent's worktree. A new backend session is started
/// immediately (not via DAG advancement).
pub fn create_child_task(
    orch: &Orchestrator,
    input: CreateChildTaskInput,
) -> Result<CreateChildTaskResult, String> {
    // ---- Load parent job ------------------------------------------------
    let parent_job = run_db(load_job(
        orch.db.local.clone(),
        input.parent_job_id.clone(),
        "Parent job not found",
    ))?;
    if parent_job.worktree_path.is_none() {
        return Err("Parent job has no worktree - cannot spawn child task".to_string());
    }
    let project_id = parent_job.project_id.clone();
    let issue_id = parent_job.issue_id.clone();
    let execution_id = parent_job.execution_id.clone();

    let worktree_path = parent_job
        .worktree_path
        .as_ref()
        .ok_or("Parent job has no worktree")?
        .clone();

    let project_path = run_db(load_project_path(orch.db.local.clone(), project_id.clone()))?;

    // ---- Load agent config from files -----------------------------------
    let config_dir = config::get_config_dir()?;
    let mut agent_config: AgentConfig = {
        let file_agent = match config_agents::get_agent(
            &config_dir,
            &input.subagent_type,
            project_path.as_deref(),
        ) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
                // Fall back to searching by name
                let agents = config_agents::list_agents(&config_dir, project_path.as_deref())
                    .unwrap_or_default();
                let mut found = None;
                for result in agents {
                    if let ConfigResult::Ok(agent) = result {
                        if agent.name == input.subagent_type {
                            found = Some(agent);
                            break;
                        }
                    }
                }
                found.ok_or_else(|| format!("Agent config not found: {}", input.subagent_type))?
            }
            Err(e) => return Err(format!("Failed to load agent config: {}", e)),
        };

        AgentConfig {
            id: file_agent.id,
            name: file_agent.name,
            description: file_agent.description,
            prompt: file_agent.prompt,
            tools: file_agent.tools,
            tier: file_agent.tier,
            workspace_id: if file_agent.is_project_scoped {
                None
            } else {
                Some("workspace".to_string())
            },
            project_id: if file_agent.is_project_scoped {
                Some(project_id.clone())
            } else {
                None
            },
            created_at: 0,
            updated_at: 0,
            disallowed_tools: file_agent.disallowed_tools,
            skills: file_agent.skills,
            fence: file_agent.fence,
            backend_preference: file_agent.backend_preference,
            selection: None,
            extras: None,
        }
    };

    // ---- Create job + run -----------------------------------------------
    let job_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let session_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let presets = load_effective_presets(&config_dir, project_path.as_deref());
    let inherited_backend = parent_job
        .model
        .as_ref()
        .and_then(|model| crate::backends::backend_for_model(model.as_str()));
    let authored_tier = input
        .tier
        .as_deref()
        .or(agent_config.tier.as_ref().map(Model::as_str));
    let authored_backend = input
        .backend_preference
        .as_deref()
        .or(agent_config.backend_preference.as_deref())
        .or(inherited_backend);
    // Resolve-early: carry the concrete atomic selection + extras into the child
    // AgentConfig so the child session never re-resolves a tier.
    let (selection, extras) = resolve_runtime_selection(authored_tier, authored_backend, &presets)?;
    let selected_model = Some(selection.model.clone());
    agent_config.selection = Some(selection);
    agent_config.extras = Some(extras);

    // The child reuses the parent's worktree; its base_commit is that worktree's
    // current HEAD.
    let base_commit = worktree_head_commit(orch, Path::new(&worktree_path));

    run_db(insert_child_job_session_run(
        orch.db.local.clone(),
        ChildInsert {
            job_id: job_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_job_id: input.parent_job_id.clone(),
            worktree_path: worktree_path.clone(),
            agent_config_id: agent_config.id.clone(),
            project_id: project_id.clone(),
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            description: input.description.clone(),
            model: selected_model.as_ref().map(|m| m.to_string()),
            base_commit,
            now,
        },
    ))?;

    // Sync new job and run
    orch.sync(SyncMessage::Job(crate::sync::SyncJob {
        id: job_id.clone(),
        issue_id: issue_id.clone(),
        project_id: Some(project_id.clone()),
        execution_id: execution_id.clone(),
        node_name: None,
        task_description: Some(input.description.clone()),
        status: Some("running".to_string()),
        model: selected_model.as_ref().map(|m| m.to_string()),
        branch: None,
        created_at: Some(now as i64),
        updated_at: Some(now as i64),
        started_at: Some(now as i64),
        completed_at: None,
    }));
    orch.sync(SyncMessage::Run(crate::sync::SyncRun {
        id: run_id.clone(),
        job_id: Some(job_id.clone()),
        issue_id: issue_id.clone(),
        status: Some("starting".to_string()),
        exit_reason: None,
        error_message: None,
        started_at: Some(now as i64),
        exited_at: None,
        created_at: Some(now as i64),
    }));

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::job_db_change_ids(
            "insert",
            &job_id,
            issue_id.as_deref(),
            execution_id.as_deref(),
            Some(&input.parent_job_id),
            None,
            &project_id,
        ),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("insert", &run_id, Some(&job_id)),
    );

    // ---- Store user event -----------------------------------------------
    store_user_event(orch, &run_id, &session_id, &input.prompt, now, -1)?;

    // ---- Output schema --------------------------------------------------
    let output_schema = OutputSchemaInfo {
        schema: OutputSchema::Preset("return".to_string()),
        artifact_name: Some("result".to_string()),
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: None,
        description: Some("Submit the task result".to_string()),
    };

    // ---- Start backend session -------------------------------------------
    crate::orchestrator::session::start_agent_session(
        orch,
        &run_id,
        &input.prompt,
        &worktree_path,
        crate::backends::SessionStart::New {
            session_id: session_id.clone(),
        },
        selected_model,
        None,
        Some(&agent_config),
        Some(&output_schema),
        false,
        execution_id.as_deref(),
        None, // Child task: inherits parent's execution identity
    )?;

    Ok(CreateChildTaskResult { job_id, run_id })
}
