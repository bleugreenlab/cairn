use super::*;
use cairn_config::library::LibraryRoots;

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
    let thread_id = parent_job.thread_id.clone();
    let execution_id = parent_job.execution_id.clone();

    let project_path = run_db(load_project_path(orch.db.clone(), project_id.clone()))?;

    // ---- Load agent config from files -----------------------------------
    // The same root the child's snapshot entry is resolved from below.
    let config_dir = orch.config_dir.clone();
    let mut agent_config: AgentConfig = {
        let file_agent = match config_agents::get_agent(
            &LibraryRoots::at(&config_dir, project_path.as_deref()),
            &input.subagent_type,
        ) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
                // Fall back to searching by name
                let agents = config_agents::list_agents(&LibraryRoots::at(
                    &config_dir,
                    project_path.as_deref(),
                ))
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
        file_agent.agent_config(&project_id)
    };

    // ---- Create job + run -----------------------------------------------
    let job_id = ids::mint_child(&project_id);
    let run_id = ids::mint_child(&job_id);
    let session_id = ids::mint_session_id().into_string();
    let now = chrono::Utc::now().timestamp() as i32;

    // A child task starts now, so it selects now, and the record is stored
    // with the job below.
    let record = orch.select_for_agent(
        &project_id,
        input.models.as_deref(),
        &agent_config.models,
        crate::execution::model_selection::SelectionSubject {
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            node_id: None,
            job_id: Some(job_id.clone()),
        },
    )?;
    let selected_model = Some(record.chosen.model.clone());
    agent_config.selection = Some(record.chosen.clone());
    agent_config.extras = Some(record.extras.clone());

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

    let execution_id = run_db_borrowed(insert_child_with_agent_snapshot(
        orch,
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
        project_path.as_deref(),
    ))?;

    orch.store_job_selection(&job_id, &record)?;

    let turn_id = ids::mint_child(&job_id);
    super::turns::create_initial_turn(orch, &turn_id, &session_id, &job_id)?;

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::job_db_change_ids(
            "insert",
            &job_id,
            issue_id.as_deref(),
            thread_id.as_deref(),
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
    store_launch_event_with_turn(
        orch,
        &run_id,
        &session_id,
        &input.prompt,
        now,
        Some(&turn_id),
    )?;

    // ---- Output schema --------------------------------------------------
    let output_schema = OutputSchemaInfo {
        schema: OutputSchema::Preset("return".to_string()),
        artifact_name: Some("result".to_string()),
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: None,
        description: Some("Submit the task result".to_string()),
    };

    // ---- Start backend session -------------------------------------------
    super::start_prepared_job_session(
        orch,
        PreparedJob {
            job_id: job_id.clone(),
            project_id,
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            session_start: crate::backends::SessionStart::New {
                session_id: session_id.clone(),
            },
            prompt: input.prompt,
            job_model: selected_model,
            agent_config: Some(agent_config),
            artifact_schema_info: Some(output_schema),
            execution_id,
            turn_id,
            start_reason: TurnStartReason::Initial,
            origin: crate::storage::LaunchOrigin::Background,
            launch_claim: None,
        },
        None,
    )?;

    Ok(CreateChildTaskResult { job_id, run_id })
}
