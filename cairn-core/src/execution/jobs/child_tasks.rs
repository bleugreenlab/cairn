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

    // Resolve the child task's working directory. A worktree-backed parent shares
    // its worktree with the child; an ambient (Branch: main / no-worktree) parent
    // has none to inherit, so the task gets its own ephemeral worktree off the
    // parent's base branch — isolation from the user's live checkout — reclaimed
    // when the task job terminalizes (owns_ephemeral_worktree).
    let (worktree_path, ephemeral_branch, owns_ephemeral_worktree) =
        match parent_job.worktree_path.clone() {
            Some(path) => (path, None, false),
            None => {
                let repo_path = project_path
                    .as_ref()
                    .ok_or("Project has no repo path for ephemeral task worktree")?
                    .to_string_lossy()
                    .to_string();
                let base_ref = parent_job
                    .base_branch
                    .clone()
                    .unwrap_or_else(|| "HEAD".to_string());
                let (path, branch) = super::worktrees::ensure_ephemeral_task_worktree(
                    orch,
                    &repo_path,
                    &job_id,
                    issue_id.clone(),
                    &base_ref,
                )?;
                // Record the branch on the child job (below) so teardown can
                // `jj workspace forget` and delete it — the DAG path gets this from
                // prepare_job's worktree write, but this synchronous path must set
                // it explicitly or the ephemeral bookmark leaks in the jj store.
                (path, Some(branch), true)
            }
        };

    // The child's base_commit is its worktree's current HEAD.
    let base_commit = worktree_head_commit(orch, Path::new(&worktree_path));

    run_db(insert_child_job_session_run(
        db.clone(),
        ChildInsert {
            job_id: job_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_job_id: Some(input.parent_job_id.clone()),
            worktree_path: Some(worktree_path.clone()),
            branch: ephemeral_branch,
            agent_config_id: agent_config.id.clone(),
            project_id: project_id.clone(),
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            description: input.description.clone(),
            model: selected_model.as_ref().map(|m| m.to_string()),
            base_commit,
            owns_ephemeral_worktree,
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
    if let Err(e) = crate::orchestrator::session::start_agent_session(
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
        // A delegated task is a multi-turn subagent that writes its artifact via
        // the write verb (with confirm gates); it stays prompt-driven, not
        // natively constrained (CAIRN-2505).
        false,
        false,
        execution_id.as_deref(),
        None, // Child task: inherits parent's execution identity
    ) {
        // The ephemeral worktree was minted before the session started. On a
        // startup failure the job never terminalizes, so neither the finalize
        // reclaim nor the terminal-status GC would fire — discard it now
        // (best-effort) so a failed spawn cannot strand a worktree + branch.
        if owns_ephemeral_worktree {
            let cleanup_orch = orch.clone();
            let cleanup_job_id = job_id.clone();
            if let Err(teardown_err) = run_db(async move {
                crate::execution::teardown::teardown_worktrees(
                    &cleanup_orch,
                    crate::execution::teardown::TeardownScope::Job(cleanup_job_id),
                    crate::execution::teardown::TeardownReason::Discarded,
                )
                .await
            }) {
                log::warn!(
                    "failed to reclaim ephemeral task worktree after session start failure: {teardown_err}"
                );
            }
        }
        return Err(e);
    }

    Ok(CreateChildTaskResult { job_id, run_id })
}
