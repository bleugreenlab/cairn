use super::*;

// ============================================================================
// create_child_task
// ============================================================================

/// Create a user-initiated child task under a running job.
///
/// The child inherits the parent's durable branch coordinate. A new backend session is started
/// immediately (not via DAG advancement).
pub fn create_child_task(
    orch: &Orchestrator,
    input: CreateChildTaskInput,
) -> Result<CreateChildTaskResult, String> {
    // Resolve the parent job's owning database (a team run lives in its replica).
    let db = run_db({
        let dbs = orch.db.clone();
        let parent_job_id = input.parent_job_id.clone();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &parent_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    // ---- Load parent job ------------------------------------------------
    let parent_job = run_db(load_job(
        db.clone(),
        input.parent_job_id.clone(),
        "Parent job not found",
    ))?;
    let project_id = parent_job.project_id.clone();
    let issue_id = parent_job.issue_id.clone();
    let execution_id = parent_job.execution_id.clone();

    let project_path = run_db(load_project_path(orch.db.clone(), project_id.clone()))?;

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
            icon: file_agent.icon,
            backend_preference: file_agent.backend_preference,
            selection: None,
            extras: None,
        }
    };

    // ---- Create job + run -----------------------------------------------
    let job_id = ids::mint_child(&project_id);
    let run_id = ids::mint_child(&job_id);
    let session_id = ids::mint_session_id().into_string();
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

    let branch = parent_job.branch.clone().ok_or_else(|| {
        format!(
            "Cannot create child task: parent job {} has no branch",
            input.parent_job_id
        )
    })?;
    // The parent's recorded base is carried forward as bookkeeping, never as a
    // coordinate: a child task resolves its own placement live at run time
    // (`resolve_current_for_read`), and this row's only consumer is the
    // `pack_anchor` lineage, which already prefers the parent's own anchor.
    // Its absence therefore has nothing to do with whether this task can run,
    // and refusing the spawn over it was substrate state failing a verb.
    let base_commit = parent_job.base_commit.clone();

    run_db(insert_child_job_session_run(
        db.clone(),
        ChildInsert {
            job_id: job_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_job_id: Some(input.parent_job_id.clone()),
            branch: Some(branch),
            agent_config_id: agent_config.id.clone(),
            project_id: project_id.clone(),
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            description: input.description.clone(),
            model: selected_model.as_ref().map(|m| m.to_string()),
            base_commit,
            // A cross-node child task keeps the fixed `return` contract via the
            // artifact-write handler's `task_name -> return` fallback; it does not
            // persist a per-run output_contract (CAIRN-2481).
            output_contract: None,
            label: None,
            phase: None,
            parent_tool_use_id: None,
            task_index: None,
            now,
        },
    ))?;

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
        crate::notify::run_db_change_ids(
            "insert",
            &run_id,
            Some(&job_id),
            issue_id.as_deref(),
            Some(&project_id),
        ),
    );

    // ---- Store the launch prompt ----------------------------------------
    // A sub-task's prompt is written by the agent that spawned it, which is the
    // clearest case of all for keeping it out of the `user` role (CAIRN-3408).
    store_launch_event(orch, &run_id, &session_id, &input.prompt, now)?;

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
        crate::backends::SessionStart::New {
            session_id: session_id.clone(),
        },
        selected_model,
        None,
        Some(&agent_config),
        Some(&output_schema),
        // A delegated task is a multi-turn subagent that writes its artifact via
        // the write verb (with confirm gates); it stays prompt-driven, not
        // natively constrained (CAIRN-2505).
        false,
        false,
        execution_id.as_deref(),
        None, // Child task: inherits parent's execution identity
    )?;

    Ok(CreateChildTaskResult { job_id, run_id })
}
