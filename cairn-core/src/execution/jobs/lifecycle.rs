use super::*;

// ============================================================================
// on_job_complete_impl
// ============================================================================

/// Called when a job finishes. Advances the execution DAG if applicable.
///
/// Only advances for jobs that are part of a recipe DAG (have both `execution_id`
/// and `recipe_node_id`). Manager jobs have `execution_id` for config storage
/// but no `recipe_node_id`, so they skip DAG advancement and worktree cleanup.
pub async fn on_job_complete_impl(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    // Tear down any external MCP gateway connections this job opened
    // (cairn://mcp/... family). Connections are pooled per job id, so closing
    // here releases the per-session server processes (e.g. Playwright browsers).
    if let Some(gateway) = orch.mcp_gateway() {
        gateway.close_session(job_id).await;
    }

    let job_id = job_id.to_string();
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
        .await
        .map_err(|e| e.to_string())?;
    let (execution_id, recipe_node_id): (Option<String>, Option<String>) = run_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, recipe_node_id FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("job not found".to_string()))?;
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .map_err(|e| db_error("Job not found", e))
    })?;

    match execution_id {
        Some(exec_id) if recipe_node_id.is_some() => {
            crate::execution::advancement::advance_execution_with_actions(orch, &exec_id).await
        }
        _ => Ok(vec![]), // Standalone job or manager job — no DAG to advance
    }
}

// ============================================================================
// prepare_job
// ============================================================================

/// Prepare a job for execution: set up worktree, create run record, store initial
/// user event. Returns a [`PreparedJob`] with everything the host layer needs to
/// call `start_agent_session`.
///
/// The job status must already be set to `"running"` by the caller before this is
/// invoked (Tauri does this synchronously so the UI sees the change immediately).
pub fn prepare_job(orch: &Orchestrator, job_id: &str) -> Result<PreparedJob, String> {
    // Resolve the job's owning database ONCE (fail-closed) and thread it through
    // every run/session/turn/event write below: a team job's rows live wholly in
    // its synced replica, so prepare must read and write there, not the private DB.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    // ---- Load job -------------------------------------------------------
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found",
    ))?;

    // ---- Load project info ----------------------------------------------
    let (repo_path, project_key) = run_db(load_project_repo_and_key(
        orch.db.clone(),
        job.project_id.clone(),
    ))?;

    // ---- Execution seq (for job-activated event) ------------------------
    let exec_seq = match &job.execution_id {
        Some(exec_id) => run_db(load_execution_seq(owning_db.clone(), exec_id.clone()))?,
        None => None,
    };

    // ---- Display ID (issue number or sequential run counter) ------------
    let display_id = run_db(load_display_id(
        owning_db.clone(),
        job.project_id.clone(),
        job.issue_id.clone(),
    ))?;

    // ---- Determine node behavior ----------------------------------------
    let (needs_worktree, inherits_worktree, step_name): (bool, bool, String) =
        if let Some(node_id) = &job.recipe_node_id {
            let execution_id = job
                .execution_id
                .as_ref()
                .ok_or("Job has recipe node but no execution_id")?;

            let all_nodes = run_db(load_nodes_from_execution(
                owning_db.clone(),
                execution_id.clone(),
            ))?;
            let node_map: HashMap<&str, &DbRecipeNode> =
                all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

            let node = node_map
                .get(node_id.as_str())
                .ok_or_else(|| format!("Recipe node not found: {}", node_id))?;

            if node.node_type == "action" {
                return Err("Action nodes execute inline during DAG advancement".to_string());
            }
            if node.node_type == "checkpoint" {
                return Err("Checkpoint nodes wait for approval, not session start".to_string());
            }

            let behavior = resolve_node_behavior(node);
            (
                behavior.needs_worktree,
                behavior.inherits_worktree,
                node.name.clone(),
            )
        } else {
            // Standalone job (e.g., manager).
            // If the job's pre-set branch matches the project default branch,
            // run on the project root without a worktree.
            let needs_wt = if let Some(ref branch) = job.branch {
                let default_branch = run_db(load_project_default_branch(
                    owning_db.clone(),
                    job.project_id.clone(),
                ))?
                .unwrap_or_else(|| "main".to_string());
                branch != &default_branch
            } else {
                true
            };
            (needs_wt, false, "standalone".to_string())
        };

    // ---- Worktree setup -------------------------------------------------
    // Create / inherit owner for the worktree lifecycle. The unit is the
    // `worktree_path`/`branch` pair: inherited children (below) copy the
    // parent's pair, so N jobs reference ONE path. Teardown
    // (`execution::teardown`) keys on the same pair; it happens once, at
    // issue/PR-terminal time, removing the path unconditionally.
    if job.worktree_path.is_some() {
        // Already has a worktree — just emit job-activated
        let _ = orch.services.emitter.emit(
            "job-activated",
            serde_json::json!({
                "jobId": job_id,
                "issueId": job.issue_id,
                "nodeName": job.node_name,
                "execSeq": exec_seq,
            }),
        );
    } else if inherits_worktree {
        // Copy worktree from parent job
        let parent_job_id = job
            .parent_job_id
            .as_ref()
            .ok_or("Job with inherit mode has no parent_job_id")?;

        let (parent_worktree, parent_branch): (String, Option<String>) = {
            let parent = run_db(load_job(
                owning_db.clone(),
                parent_job_id.to_string(),
                "Parent job not found",
            ))?;
            (
                parent
                    .worktree_path
                    .ok_or("Parent job has no worktree - cannot inherit")?,
                parent.branch,
            )
        };

        // The child shares the parent's worktree, so its base_commit is that
        // worktree's HEAD at the moment of inheritance.
        let base_commit = worktree_head_commit(orch, Path::new(&parent_worktree));
        let now = chrono::Utc::now().timestamp() as i32;
        run_db(update_job_worktree(
            owning_db.clone(),
            job_id.to_string(),
            Some(parent_worktree.clone()),
            parent_branch.clone(),
            base_commit,
            now,
        ))?;

        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &job.id,
                job.issue_id.as_deref(),
                job.execution_id.as_deref(),
                job.parent_job_id.as_deref(),
                job.parent_tool_use_id.as_deref(),
                &job.project_id,
            ),
        );
        let _ = orch.services.emitter.emit(
            "job-activated",
            serde_json::json!({
                "jobId": job_id,
                "issueId": job.issue_id,
                "nodeName": job.node_name,
                "execSeq": exec_seq,
            }),
        );
    } else if needs_worktree {
        // Count existing branched jobs to compute unique sequence number
        let seq = run_db(count_existing_branched_jobs(
            owning_db.clone(),
            job.issue_id.clone(),
            job.execution_id.clone(),
        ))?;

        let safe_step_name: String = step_name
            .to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .to_string();

        let worktrees_dir = dirs::home_dir()
            .ok_or("Could not find home directory")?
            .join(".cairn")
            .join("worktrees");

        let (branch, wt_dir) = if let Some(ref existing) = job.branch {
            // Job already has a branch (e.g., feature-branch manager) — use it
            let dir = existing.replace('/', "-");
            (existing.clone(), dir)
        } else {
            let mut candidate_seq = seq;
            loop {
                let name = format!(
                    "{}-{}-{}-{}",
                    project_key, display_id, safe_step_name, candidate_seq
                );
                if !worktrees_dir.join(&name).exists() {
                    break (format!("agent/{}", name), name);
                }
                log::warn!(
                    "Auto-generated worktree path already exists ({}), trying next sequence",
                    worktrees_dir.join(&name).display()
                );
                candidate_seq += 1;
                if candidate_seq > seq + 100 {
                    return Err(format!(
                        "Could not find an available worktree path for job {} after 100 attempts",
                        job_id
                    ));
                }
            }
        };

        let wt_path = worktrees_dir.join(&wt_dir);

        let base_ref = job.base_branch.as_deref().unwrap_or("HEAD");
        let cancel = Arc::new(AtomicBool::new(false));
        let child_slot = Arc::new(Mutex::new(None));
        let sink = setup_progress::make_sink(orch, job_id, job.issue_id.clone());
        setup_progress::emit(
            &sink,
            job_id,
            job.issue_id.clone(),
            "started",
            None,
            None,
            None,
        );
        {
            let mut registry = orch.setup_registry.lock().unwrap();
            registry.insert(
                job_id.to_string(),
                crate::orchestrator::SetupHandle {
                    cancel: cancel.clone(),
                    child: child_slot.clone(),
                },
            );
        }

        let setup_result = prepare_worktree_for_job(
            orch,
            &repo_path,
            &wt_path,
            &branch,
            base_ref,
            job_id,
            job.issue_id.clone(),
            &sink,
            &cancel,
            &child_slot,
        );
        orch.setup_registry.lock().unwrap().remove(job_id);
        match setup_result {
            Ok(()) => setup_progress::emit(
                &sink,
                job_id,
                job.issue_id.clone(),
                "complete",
                None,
                None,
                None,
            ),
            Err(crate::git::worktree::SetupError::Cancelled) => {
                setup_progress::emit(
                    &sink,
                    job_id,
                    job.issue_id.clone(),
                    "failed",
                    None,
                    None,
                    Some("Setup cancelled".to_string()),
                );
                return Err(SETUP_CANCELLED_ERROR.to_string());
            }
            Err(e) => {
                setup_progress::emit(
                    &sink,
                    job_id,
                    job.issue_id.clone(),
                    "failed",
                    None,
                    None,
                    Some(e.to_string()),
                );
                return Err(e.to_string());
            }
        }

        let wt_path_str = wt_path.to_string_lossy().to_string();
        // The freshly created worktree's HEAD is the tip of its base branch.
        let base_commit = worktree_head_commit(orch, &wt_path);
        let now = chrono::Utc::now().timestamp() as i32;
        run_db(update_job_worktree(
            owning_db.clone(),
            job_id.to_string(),
            Some(wt_path_str.clone()),
            Some(branch.clone()),
            base_commit,
            now,
        ))?;

        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &job.id,
                job.issue_id.as_deref(),
                job.execution_id.as_deref(),
                job.parent_job_id.as_deref(),
                job.parent_tool_use_id.as_deref(),
                &job.project_id,
            ),
        );
        let _ = orch.services.emitter.emit(
            "job-activated",
            serde_json::json!({
                "jobId": job_id,
                "issueId": job.issue_id,
                "nodeName": job.node_name,
                "execSeq": exec_seq,
            }),
        );
    } else {
        // No worktree needed — just emit job-activated
        let _ = orch.services.emitter.emit(
            "job-activated",
            serde_json::json!({
                "jobId": job_id,
                "issueId": job.issue_id,
                "nodeName": job.node_name,
                "execSeq": exec_seq,
            }),
        );
    }

    // ---- Reload job (picks up worktree_path/branch updates) -------------
    let job = run_db(load_job(
        owning_db.clone(),
        job_id.to_string(),
        "Job not found after worktree setup",
    ))?;

    // ---- Agent config ---------------------------------------------------
    let project_path = Some(PathBuf::from(repo_path.clone()));

    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;

    // ---- Create session + run record --------------------------------------
    let run_id = ids::mint_child(job_id);
    let now = chrono::Utc::now().timestamp() as i32;
    let status_str = RunStatus::Starting.to_string();

    // Ensure a Session record exists for this job and derive the first-start mode.
    let had_current_session = job.current_session_id.is_some();
    let (session_id, session_start, run_start_mode) = run_db(prepare_session(
        owning_db.clone(),
        job_id.to_string(),
        job.clone(),
        now,
    ))?;
    if !had_current_session {
        // `prepare_session` backfills jobs.current_session_id for older/sessionless
        // jobs. The returned `start_job` value and earlier worktree invalidations
        // can both predate that write, so emit an explicit jobs change to refresh
        // chat chrome such as ContextUsage as soon as the session is known.
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &job.id,
                job.issue_id.as_deref(),
                job.execution_id.as_deref(),
                job.parent_job_id.as_deref(),
                job.parent_tool_use_id.as_deref(),
                &job.project_id,
            ),
        );
    }

    let existing_active_count = run_db(insert_run(
        owning_db.clone(),
        RunInsert {
            run_id: run_id.clone(),
            issue_id: job.issue_id.clone(),
            project_id: Some(job.project_id.clone()),
            job_id: Some(job_id.to_string()),
            status: status_str.clone(),
            session_id: Some(session_id.clone()),
            started_at: None,
            created_at: now,
            updated_at: now,
            start_mode: Some(run_start_mode.clone()),
            warn_existing_active: true,
        },
    ))?;

    if existing_active_count > 0 {
        log::warn!(
            "[prepare_job] Job {} already has {} active runs",
            job_id,
            existing_active_count
        );
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "insert", "runId": run_id.as_str(), "jobId": job_id}),
    );

    // ---- Create initial turn ------------------------------------------------
    let turn_id = ids::mint_child(job_id);
    create_initial_turn(orch, &turn_id, &session_id, job_id)?;

    // ---- Resolve inputs + build prompt ----------------------------------
    let (resolved_inputs, artifact_schema_info) =
        run_db(resolve_inputs_and_schema(owning_db.clone(), job.clone()))?;

    let prompt = format_resolved_inputs(&resolved_inputs);

    let worktree_path: String = job
        .worktree_path
        .clone()
        .or_else(|| {
            project_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
        })
        .ok_or("Job has no worktree path and project path is unavailable")?;

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store initial user event ---------------------------------------
    store_user_event_with_turn(orch, &run_id, &session_id, &prompt, now, -1, Some(&turn_id))?;

    // ---- Emit system message for job start ------------------------------
    crate::messages::system::emit_job_event(
        orch,
        job_id,
        Some(&run_id),
        crate::messages::system::JobEvent::Started,
    );

    Ok(PreparedJob {
        run_id,
        session_id,
        session_start,
        prompt,
        worktree_path,
        job_model,
        agent_config,
        artifact_schema_info,
        execution_id: job.execution_id,
        turn_id,
    })
}

// ============================================================================
// continue_job_impl
// ============================================================================

/// Context for resuming a job after a durable suspend on the slow (>45s) path.
///
/// Its presence tells [`continue_job_impl`] not to store the resume message as a
/// visible user event: the result is already rendered in place as the
/// originating `write` call's synthetic tool_result. The message is still
/// forwarded to the agent so the model sees it in context.
#[derive(Debug, Clone, Default)]
pub struct ResumeContext {
    /// When true, skip storing the resume message as a `user` transcript event.
    pub suppress_user_event: bool,
}

/// Continue an existing job with an optional follow-up message.
///
/// Reuses a warm process if one exists for the job's session, otherwise starts
/// a new Claude process with `--resume`.
pub fn continue_job_impl(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
    prompt_resume: Option<ResumeContext>,
) -> Result<Run, String> {
    // Resolve the job's owning database ONCE (fail-closed): a team job resumes
    // against its synced replica, never the private DB.
    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    // ---- Load job -------------------------------------------------------
    let (job, project_id, issue_id, project_path) = run_db(load_job_context(
        owning_db.clone(),
        orch.db.clone(),
        job_id.to_string(),
    ))?;

    let current_session_id = job.current_session_id.as_ref().ok_or_else(|| {
        if job.status == "blocked" {
            "This job has no agent session to resume. A command checkpoint is resolved by confirming it (an override that continues the workflow).".to_string()
        } else {
            "Job has no current session to resume".to_string()
        }
    })?;

    // ---- Transition job to Running if in terminal state -------------------
    // Any resume (including the post-completion memory review) makes the agent
    // active, so the job is Running. The confirm affordance is decoupled from
    // run state (it keys off the unconfirmed artifact), so a Running review does
    // not hide it (CAIRN-1576).
    if job.status != "running" {
        if let Err(e) = transition_job_to_running(orch, job_id) {
            log::warn!("Failed to transition job {} to running: {}", job_id, e);
        }
    }

    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;

    let worktree_path: String = job
        .worktree_path
        .clone()
        .or_else(|| {
            project_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
        })
        .ok_or("Job has no worktree path and project path is unavailable")?;

    let now = chrono::Utc::now().timestamp() as i32;

    // Desired backend for this turn, mirroring cold-start backend selection
    // (agent `backend_preference`, else inferred from the model). Used to detect
    // a model change that crosses providers so the session can rotate.
    // Resolve-early: prefer the snapshot's stored selection backend so a resumed
    // session uses what it was launched with, never a re-resolution against
    // changed settings. backend_preference / model-derivation are fallbacks.
    let desired_backend = agent_config
        .as_ref()
        .and_then(|ac| ac.selection.as_ref().map(|s| s.backend.clone()))
        .or_else(|| {
            agent_config
                .as_ref()
                .and_then(|ac| ac.backend_preference.clone())
        })
        .or_else(|| {
            job.model
                .as_deref()
                .and_then(crate::backends::backend_for_model)
                .map(str::to_string)
        });

    // ---- Session identity check -----------------------------------------
    // Load Session and derive explicit continue semantics. When the requested
    // model now implies a different backend than the session was started on, the
    // prior backend's resume handle is invalid on the new backend, so rotate to a
    // fresh session on the new backend rather than resuming with a wrong handle.
    // This runs before the warm/cold split, so it covers cold continues too.
    let (session_id, session_start, run_start_mode) = match run_db(load_session_optional(
        owning_db.clone(),
        current_session_id.clone(),
    ))? {
        Some(session) => match session.status {
            SessionStatus::Open => {
                // A prompt edit since this session spawned marks the job for a
                // fresh session (the system prompt is fixed at spawn). Read-and-
                // clear it here, between turns, so a live turn's output is never
                // orphaned. A backend rotation below also produces a fresh
                // session, so consuming the flag here is correct either way.
                let needs_fresh_session = run_db(take_needs_fresh_session(
                    owning_db.clone(),
                    job_id.to_string(),
                ))?;
                match desired_backend
                    .as_deref()
                    .filter(|want| *want != session.backend)
                {
                    Some(want) => {
                        // Evict any live process bound to the old session first.
                        if let Some(old_run) =
                            orch.process_state.find_process_by_session(&session.id)
                        {
                            orch.process_state.stop_and_remove(&old_run);
                        }
                        log::info!(
                            "Backend change {} -> {} for job {}; rotating to a fresh session",
                            session.backend,
                            want,
                            job_id
                        );
                        let new_session = run_db({
                            let db = owning_db.clone();
                            let session = session.clone();
                            let job_id = job_id.to_string();
                            let want = want.to_string();
                            async move {
                                crate::sessions::queries::rotate_job_session_to_backend(
                                    db.as_ref(),
                                    &session,
                                    &job_id,
                                    &want,
                                )
                                .await
                            }
                        })?;
                        let session_start = crate::backends::SessionStart::New {
                            session_id: new_session.id.clone(),
                        };
                        let run_start_mode = run_start_mode(&session_start).to_string();
                        (new_session.id, session_start, run_start_mode)
                    }
                    None if needs_fresh_session => {
                        // Evict any live process bound to the old session, then
                        // rotate to a fresh same-backend session so this turn
                        // rebuilds the edited system prompt.
                        if let Some(old_run) =
                            orch.process_state.find_process_by_session(&session.id)
                        {
                            orch.process_state.stop_and_remove(&old_run);
                        }
                        log::info!(
                            "Prompt change for job {}; rotating to a fresh session",
                            job_id
                        );
                        let new_session = run_db({
                            let db = owning_db.clone();
                            let session = session.clone();
                            let job_id = job_id.to_string();
                            async move {
                                crate::sessions::queries::rotate_job_session(
                                    db.as_ref(),
                                    &session,
                                    &job_id,
                                )
                                .await
                            }
                        })?;
                        let session_start = crate::backends::SessionStart::New {
                            session_id: new_session.id.clone(),
                        };
                        let run_start_mode = run_start_mode(&session_start).to_string();
                        (new_session.id, session_start, run_start_mode)
                    }
                    None => {
                        let session_start = resolve_continue_session_start(&session)?;
                        let run_start_mode = run_start_mode(&session_start).to_string();
                        (session.id.clone(), session_start, run_start_mode)
                    }
                }
            }
            SessionStatus::Failed | SessionStatus::Closed => {
                return Err(format!(
                    "Session {} is {:?} and cannot be continued",
                    &session.id[..session.id.len().min(8)],
                    session.status
                ));
            }
        },
        None => {
            // No Session record found — legacy data (e.g. old Codex thread_id).
            // Use session_id directly; it may itself be a resume handle.
            log::info!(
                "No Session record for {}, using as-is (legacy)",
                &current_session_id[..current_session_id.len().min(8)]
            );
            (
                current_session_id.clone(),
                crate::backends::SessionStart::Resume {
                    session_id: current_session_id.clone(),
                    backend_id: current_session_id.clone(),
                },
                "resume".to_string(),
            )
        }
    };

    // ---- Find or create run ---------------------------------------------
    // Reconcile a reusable process against the job's requested model *before*
    // deciding to reuse it. `jobs.model` is the source of truth: a model change
    // restarts the process (cold resume with the new model) so the persisted
    // model wins. Cross-backend changes were already handled by rotation above,
    // so any process found here is on the correct backend.
    let existing_run_id = match orch.process_state.find_process_by_session(&session_id) {
        Some(found_run_id) => {
            match ensure_reused_process_model(
                &orch.process_state,
                &found_run_id,
                job.model.as_deref(),
            )? {
                ReuseDecision::Reuse => Some(found_run_id),
                ReuseDecision::Restart => {
                    log::info!(
                        "Model changed for session {}; evicting warm process {} and restarting",
                        &session_id[..session_id.len().min(8)],
                        &found_run_id[..found_run_id.len().min(8)]
                    );
                    orch.process_state.stop_and_remove(&found_run_id);
                    None
                }
            }
        }
        None => None,
    };
    if existing_run_id.is_none() {
        let _ = reconcile_stale_active_turn_for_continue(orch, job_id, &session_id)?;
    }
    let (run_id, is_process_reuse) = if let Some(existing_id) = existing_run_id {
        log::info!(
            "Found existing process for session {}, reusing run {}",
            &session_id[..session_id.len().min(8)],
            &existing_id[..existing_id.len().min(8)]
        );
        (existing_id, true)
    } else {
        let new_run_id = ids::mint_child(job_id);
        let status_str = RunStatus::Starting.to_string();
        run_db(insert_run(
            owning_db.clone(),
            RunInsert {
                run_id: new_run_id.clone(),
                issue_id: issue_id.clone(),
                project_id: Some(project_id.clone()),
                job_id: Some(job_id.to_string()),
                status: status_str.clone(),
                session_id: Some(session_id.clone()),
                started_at: None,
                created_at: now,
                updated_at: now,
                start_mode: Some(run_start_mode.clone()),
                warn_existing_active: false,
            },
        ))?;
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "runs", "action": "insert", "runId": new_run_id.as_str(), "jobId": job_id}),
        );
        (new_run_id, false)
    };

    // ---- Create successor turn for follow-up ----------------------------
    // A resume carrying user-authored content (an explicit message or a
    // prompt/permission answer) is a work turn, never the post-completion
    // memory-review reflection. The pending-queued-message case (a user steer
    // that arrives without an explicit message) is detected inside
    // `create_followup_turn` against the rows the claim below sweeps up.
    let user_initiated = message.is_some() || prompt_resume.is_some();
    let turn_id = create_followup_turn(orch, &session_id, job_id, user_initiated)?;

    // ---- Artifact schema ------------------------------------------------
    let artifact_schema_info = run_db(find_job_downstream_artifact_schema(
        owning_db.clone(),
        job.clone(),
    ))?;

    // CAIRN-1309: claim every queued user follow-up for this job (both `queue`
    // and any `steer` row that never reached a tool boundary). They are
    // delivered on this resume — covering the turn-end flush and the resume that
    // follows answering a question/permission prompt.
    let queued_messages = match crate::messages::queued::claim_all_for_job(&owning_db, job_id) {
        Ok(msgs) => msgs,
        Err(error) => {
            log::warn!(
                "failed to claim queued messages for resume prompt on job {}: {}",
                &job_id[..job_id.len().min(8)],
                error
            );
            Vec::new()
        }
    };
    let has_queued = !queued_messages.is_empty();

    // When there is no explicit resume message but the user queued follow-ups,
    // the queued content *is* the user's prompt — don't lead with the generic
    // "Continue where you left off." placeholder (and don't store it as a "You"
    // event; each queued message is stored as its own event below).
    // CAIRN-1881: drain this job's pending attention pushes. Both rousing
    // (`wake`/`interrupt`) and `passive` ride-along pushes deliver on a resume
    // that is already happening; each is lazy-resolved so a push whose referent
    // already resolved is skipped. They are stamped delivered atomically with
    // their carrying event, persisted below once the prompt is assembled.
    let drained_pushes = {
        let db = owning_db.clone();
        let recipient = job_id.to_string();
        run_db(async move {
            crate::orchestrator::attention_push::list_pending_live(&db, &recipient)
                .await
                .map_err(|e| e.to_string())
        })
        .unwrap_or_default()
    };
    let has_pushes = !drained_pushes.is_empty();
    // CAIRN-1891: resolve each push's content_ref to its rendered resource content
    // so the resumed agent acts without a round-trip read. Uses the same in-process
    // backs `cairn read`; run on a scoped DB runtime so it can borrow orch.
    let push_prompt = if has_pushes {
        let pushes = drained_pushes.clone();
        crate::storage::run_db_blocking(move || async move {
            Ok::<_, String>(
                crate::orchestrator::attention_delivery::render_pushes_resolved(orch, &pushes)
                    .await,
            )
        })
        .ok()
        .flatten()
    } else {
        None
    };
    // CAIRN-1891: the persisted carrying event is the wake-card payload
    // (`{active, catchup}`) PLUS the `resolved` content the agent received, so the
    // transcript renders a card and its detail modal shows the full content (not
    // just the resource ref). The agent gets the same resolved content inline in
    // the prompt below.
    let push_summary = if has_pushes {
        Some(
            crate::orchestrator::attention_push::push_event_content_json(
                &drained_pushes,
                push_prompt.as_deref().unwrap_or_default(),
            ),
        )
    } else {
        None
    };
    let user_message = match message {
        Some(m) => m.to_string(),
        None if has_queued || has_pushes => String::new(),
        None => "Continue where you left off.".to_string(),
    };
    let base_prompt = resolve_skill_slash_command(orch, &user_message, project_path.as_deref());
    let side_channel_notices =
        match crate::messages::side_channel::claim_pending_side_channel_for_job(&owning_db, job_id)
        {
            Ok(notices) => notices,
            Err(error) => {
                log::warn!(
                    "failed to claim side-channel notices for resume prompt on job {}: {}",
                    &job_id[..job_id.len().min(8)],
                    error
                );
                Vec::new()
            }
        };
    if !side_channel_notices.is_empty() {
        crate::messages::transcript::insert_side_channel_events_sync(
            orch,
            &run_id,
            Some(&session_id),
            Some(&turn_id),
            &side_channel_notices,
        )?;
    }
    let queued_block = if has_queued {
        Some(
            queued_messages
                .iter()
                .map(|m| m.content.clone())
                .collect::<Vec<_>>()
                .join("\n\n"),
        )
    } else {
        None
    };
    let side_channel_block = if side_channel_notices.is_empty() {
        None
    } else {
        Some(crate::messages::transcript::render_side_channel_prompt_block(&side_channel_notices))
    };
    // A durable self-suspend resume injects the awaited result as the suspended
    // tool call's synthetic `tool_result` (this is exactly when
    // `suppress_user_event` is set). From the agent's side that call looks
    // interrupted mid-execution and then returns fine, which is disconcerting;
    // lead the forwarded prompt with a short note that names the pause as
    // deliberate. The note rides only in the live prompt, never a stored event,
    // so it frames this turn without cluttering the transcript.
    let suppress_user_event = prompt_resume
        .as_ref()
        .map(|ctx| ctx.suppress_user_event)
        .unwrap_or(false);
    let resume_note = suppress_user_event.then_some(RESUME_AFTER_SELF_SUSPEND_NOTE);
    let prompt = assemble_resume_prompt(
        resume_note,
        queued_block,
        &base_prompt,
        side_channel_block,
        push_prompt.as_deref(),
    );

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store user event -----------------------------------------------
    // Store the user's message so the UI displays what was actually sent.
    //
    // Slow-path prompt resumes skip this: the answer is already rendered as the
    // originating Question call's synthetic tool_result, so a separate "You"
    // block would duplicate it. The message is still forwarded to the agent
    // below for the model's context.
    //
    // `suppress_user_event` was resolved above (it also gates the resume note);
    // reuse it here.
    // Skip the default "You" event when the user supplied no explicit message and
    // the content is carried entirely by queued follow-ups (stored individually
    // below) — storing the empty placeholder would render a blank You block.
    // CAIRN-1881: persist the carrying event for the drained pushes and stamp each
    // delivered by it, atomically (same transaction as the event INSERT). The
    // pushes already ride in the resume prompt above, so they are delivered
    // regardless of `suppress_user_event`; recovery redelivers only pushes whose
    // carrying event never durably landed.
    if let Some(text) = &push_summary {
        let push_ids: Vec<String> = drained_pushes.iter().map(|p| p.id.clone()).collect();
        crate::execution::jobs::snapshots::store_attention_push_event(
            orch,
            &run_id,
            &session_id,
            text,
            &push_ids,
            now,
            Some(&turn_id),
        )?;
    }
    // Queued follow-ups — including passive "quiet" notes that rode along without
    // waking the agent — were authored before the immediate resume message, so
    // they render first, matching the prompt order assembled above. Each shows as
    // its own "You" block and drops out of the pending strip.
    if has_queued {
        for queued in &queued_messages {
            let display_message = queued.content.clone();
            store_user_event_with_turn(
                orch,
                &run_id,
                &session_id,
                &display_message,
                now,
                -1,
                Some(&turn_id),
            )?;
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "queued_messages", "action": "update"}),
        );
    }
    let store_default_user_event =
        !(suppress_user_event || (message.is_none() && (has_queued || has_pushes)));
    if store_default_user_event {
        let display_message = user_message.clone();
        store_user_event_with_turn(
            orch,
            &run_id,
            &session_id,
            &display_message,
            now,
            -1,
            Some(&turn_id),
        )?;
    }

    // ---- Warm process or new session ------------------------------------
    if is_process_reuse {
        // Establish the turn FULLY before waking the agent (CAIRN-2123). A warm
        // agent commonly fires a tool call the instant it resumes; if the wake
        // (`send_user_message`) preceded turn establishment, that call could
        // land in the `Busy`-without-turn window and capture a NULL turn for any
        // worktree-fence crossing it raised — durably parking the run with no
        // way to resume it. Ordering occupancy (`transition_to_active` then
        // `ServingTurn` via `begin_turn`) and the persisted DB turn
        // (`start_turn` writes `jobs.current_turn_id`) ahead of the wake closes
        // that window: a tool call produced on resume always observes
        // `ServingTurn(turn)`, never `Busy`.
        orch.process_state.transition_to_active(&run_id);
        let _ = start_turn(orch, &turn_id, &run_id);
        orch.process_state
            .set_current_turn_id(&run_id, Some(&turn_id));

        // Run stays Live — no durable status change for warm reuse.
        crate::backends::stdin::send_user_message(
            &orch.process_state,
            &run_id,
            &prompt,
            &session_id,
            None,
            Some(&worktree_path),
        )?;
    } else {
        crate::orchestrator::session::start_agent_session(
            orch,
            &run_id,
            &prompt,
            &worktree_path,
            session_start,
            job_model,
            None,
            agent_config.as_ref(),
            artifact_schema_info.as_ref(),
            false,
            job.execution_id.as_deref(),
            identity_override,
        )?;

        // The new session's process handle now exists, so establish the turn on
        // it. `start_agent_session` spawns the CLI and returns before the MCP
        // handshake completes, so the first tool call cannot arrive until after
        // this point — the turn is always established before the session emits a
        // crossing. (`set_current_turn_id` needs the handle to exist, which is
        // why this follows the spawn rather than preceding it.)
        let _ = start_turn(orch, &turn_id, &run_id);
        orch.process_state
            .set_current_turn_id(&run_id, Some(&turn_id));
    }

    // ---- Return run -----------------------------------------------------
    let run = run_db(load_run(
        owning_db.clone(),
        run_id.clone(),
        "Run not found after creation",
    ))?;
    Ok(run)
}

/// Short note that leads the forwarded prompt on a durable self-suspend resume.
///
/// On the slow (>45s) path the awaited result is delivered as the suspended tool
/// call's synthetic `tool_result`, so to the resumed agent that call looks
/// interrupted mid-execution and then returns — a deliberate pause that reads
/// like a glitch. Naming it as an intentional suspend up front removes that
/// tension (CAIRN-2173).
const RESUME_AFTER_SELF_SUSPEND_NOTE: &str = "Resuming after an intentional self-suspend. Your run paused itself to wait on delegated work (a sub-agent task or a user question) and resumed now that the work finished — the earlier tool call was suspended on purpose, not interrupted or errored, and its result follows.";

/// Assemble a resume prompt so notes that accumulated before this wake lead and
/// the immediate resume message follows them.
///
/// A `resume_note` (the self-suspend frame) leads everything when present.
/// Queued user follow-ups — including `passive` "quiet" notes that rode along
/// without waking the agent — were authored before the message that triggers
/// this resume, so they precede `base_prompt`. A quiet note "A" sent before a
/// waking message "B" is therefore delivered as "A\n\nB", matching the order the
/// user sent them rather than reversing it. Side-channel notices and resolved
/// attention pushes keep their established position after the immediate
/// message. Falls back to the generic continue placeholder when every part is
/// empty.
fn assemble_resume_prompt(
    resume_note: Option<&str>,
    queued_block: Option<String>,
    base_prompt: &str,
    side_channel_block: Option<String>,
    push_prompt: Option<&str>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(note) = resume_note {
        if !note.is_empty() {
            parts.push(note.to_string());
        }
    }
    if let Some(q) = queued_block {
        if !q.is_empty() {
            parts.push(q);
        }
    }
    if !base_prompt.is_empty() {
        parts.push(base_prompt.to_string());
    }
    if let Some(s) = side_channel_block {
        if !s.is_empty() {
            parts.push(s);
        }
    }
    if let Some(p) = push_prompt {
        if !p.is_empty() {
            parts.push(p.to_string());
        }
    }
    if parts.is_empty() {
        "Continue where you left off.".to_string()
    } else {
        parts.join("\n\n")
    }
}

/// Outcome of reconciling a reusable process against the job's requested model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReuseDecision {
    /// The live process already matches (or can serve) the requested model — reuse it.
    Reuse,
    /// The live process cannot serve the requested model and must be restarted.
    Restart,
}

/// Reconcile a reusable warm/active process against the job's requested model.
///
/// `jobs.model` and the model recorded on the live process (`RunHandle.model`,
/// set at startup) are the source of truth. By the time this runs, any
/// cross-backend change has already been handled by session rotation in
/// `continue_job_impl`, so a process found for the current session is guaranteed
/// to be on the right backend — only the model can differ here.
///
/// A model change resolves to `Restart` rather than an in-place `set_model`. A
/// live switch is not trusted: for Claude the accept/reject arrives later as an
/// async control response, so reusing the process immediately would risk running
/// the next turn on a stale model and recording a model the process never
/// confirmed. Restart (cold resume of the same session with the new model) is
/// deterministic. If a restart would be required but the process is not idle,
/// returns an error rather than killing an active or host-blocked turn.
fn ensure_reused_process_model(
    process_state: &crate::agent_process::process::AgentProcessState,
    run_id: &str,
    desired_model: Option<&str>,
) -> Result<ReuseDecision, String> {
    // No requested model → nothing to reconcile; reuse as-is.
    let Some(desired) = desired_model else {
        return Ok(ReuseDecision::Reuse);
    };

    // Already serving the requested model → reuse.
    if process_state.get_model(run_id).as_deref() == Some(desired) {
        return Ok(ReuseDecision::Reuse);
    }

    // Model diverged → restart deterministically, but refuse to tear down a
    // process that is mid-turn or host-blocked.
    match process_state.get_occupancy(run_id) {
        Some(crate::agent_process::process::RunOccupancy::Idle) | None => {
            log::info!(
                "Model changed to {} for run {}; restarting to apply it",
                desired,
                &run_id[..run_id.len().min(8)]
            );
            Ok(ReuseDecision::Restart)
        }
        Some(other) => Err(format!(
            "Cannot change model to {} for run {}: process is busy ({:?})",
            desired,
            &run_id[..run_id.len().min(8)],
            other
        )),
    }
}

pub(super) struct ActiveTurnForContinue {
    turn_id: String,
    state: TurnState,
    run_id: Option<String>,
}

pub(super) fn load_active_turn_for_continue(
    db: Arc<LocalDb>,
    job_id: String,
) -> Result<Option<ActiveTurnForContinue>, String> {
    run_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, state, run_id
                         FROM turns
                         WHERE job_id = ?1
                           AND state IN ('pending', 'running')
                         ORDER BY sequence DESC
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        let state = row.text(1)?.parse().map_err(db_internal)?;
                        Ok(ActiveTurnForContinue {
                            turn_id: row.text(0)?,
                            state,
                            run_id: row.opt_text(2)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub(super) fn mark_stale_active_turn_for_continue(
    db: Arc<LocalDb>,
    active: ActiveTurnForContinue,
) -> Result<(), String> {
    run_db(async move {
        db.write(|conn| {
            let turn_id = active.turn_id.clone();
            let state = active.state.clone();
            let run_id = active.run_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                let target_state = match &state {
                    TurnState::Running => TurnState::Interrupted,
                    TurnState::Pending => TurnState::Cancelled,
                    _ => return Ok(()),
                };

                conn.execute(
                    "UPDATE turns
                     SET state = ?1,
                         ended_at = ?2,
                         updated_at = ?2
                     WHERE id = ?3
                       AND state = ?4",
                    params![
                        target_state.to_string(),
                        now,
                        turn_id.as_str(),
                        state.to_string()
                    ],
                )
                .await?;

                if let Some(run_id) = run_id.as_deref() {
                    conn.execute(
                        "UPDATE runs
                         SET status = 'crashed',
                             exit_reason = 'stale_continue_recovery',
                             exited_at = ?1,
                             updated_at = ?1
                         WHERE id = ?2
                           AND status IN ('starting', 'live', 'running', 'idle')",
                        params![now, run_id],
                    )
                    .await?;
                }

                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub(super) fn reconcile_stale_active_turn_for_continue(
    orch: &Orchestrator,
    job_id: &str,
    session_id: &str,
) -> Result<bool, String> {
    if orch
        .process_state
        .find_process_by_session(session_id)
        .is_some()
    {
        return Ok(false);
    }

    let owning_db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let Some(active) = load_active_turn_for_continue(owning_db.clone(), job_id.to_string())? else {
        return Ok(false);
    };

    if active
        .run_id
        .as_deref()
        .and_then(|run_id| orch.process_state.get_current_turn_id(run_id))
        .is_some()
    {
        return Ok(false);
    }

    let turn_id = active.turn_id.clone();
    let run_id = active.run_id.clone();
    let state = active.state.clone();
    mark_stale_active_turn_for_continue(owning_db.clone(), active)?;
    log::warn!(
        "Recovered stale {} turn {} for job {} before continue",
        state,
        turn_id,
        job_id
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );
    if run_id.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "runs", "action": "update"}),
        );
    }
    Ok(true)
}

#[cfg(any(test, feature = "test-utils"))]
pub fn reconcile_stale_active_turn_for_continue_for_test(
    orch: &Orchestrator,
    job_id: &str,
    session_id: &str,
) -> Result<bool, String> {
    reconcile_stale_active_turn_for_continue(orch, job_id, session_id)
}

#[cfg(test)]
mod tests {
    use super::{assemble_resume_prompt, ensure_reused_process_model, ReuseDecision};
    use crate::agent_process::process::{wrap_plain_stdin, AgentProcessState, RunHandle};
    use std::sync::{Arc, Mutex};

    #[test]
    fn passive_note_precedes_immediate_resume_message() {
        // A quiet note "A" queued before a waking message "B" must deliver as
        // "A then B", matching send order — not reversed.
        let prompt = assemble_resume_prompt(None, Some("A".to_string()), "B", None, None);
        assert_eq!(prompt, "A\n\nB");
    }

    #[test]
    fn multiple_queued_notes_keep_order_before_message() {
        let prompt = assemble_resume_prompt(None, Some("A1\n\nA2".to_string()), "B", None, None);
        assert_eq!(prompt, "A1\n\nA2\n\nB");
    }

    #[test]
    fn resume_prompt_without_queued_is_just_the_message() {
        let prompt = assemble_resume_prompt(None, None, "B", None, None);
        assert_eq!(prompt, "B");
    }

    #[test]
    fn queued_only_resume_has_no_placeholder() {
        let prompt = assemble_resume_prompt(None, Some("A".to_string()), "", None, None);
        assert_eq!(prompt, "A");
    }

    #[test]
    fn empty_resume_falls_back_to_placeholder() {
        let prompt = assemble_resume_prompt(None, None, "", None, None);
        assert_eq!(prompt, "Continue where you left off.");
    }

    #[test]
    fn side_channel_and_push_follow_the_immediate_message() {
        // Queued notes lead; the immediate message, then side-channel notices and
        // resolved attention pushes, follow — the established position for those
        // blocks.
        let prompt = assemble_resume_prompt(
            None,
            Some("A".to_string()),
            "B",
            Some("side".to_string()),
            Some("push"),
        );
        assert_eq!(prompt, "A\n\nB\n\nside\n\npush");
    }

    #[test]
    fn self_suspend_note_leads_the_resume_prompt() {
        // On a durable self-suspend resume the note frames the whole turn, so it
        // leads even queued notes — the agent reads "this pause was deliberate"
        // before the awaited result that follows.
        let prompt = assemble_resume_prompt(
            Some("NOTE"),
            Some("A".to_string()),
            "B",
            Some("side".to_string()),
            Some("push"),
        );
        assert_eq!(prompt, "NOTE\n\nA\n\nB\n\nside\n\npush");
    }

    #[test]
    fn absent_self_suspend_note_is_omitted() {
        // A normal (non-suspended) resume passes no note and is unchanged.
        let prompt = assemble_resume_prompt(None, None, "B", None, None);
        assert_eq!(prompt, "B");
    }

    /// Register a warm process with a recorded model and backend. The stdin is an
    /// in-memory writer so a live `send_set_model` (Claude) succeeds in tests.
    fn register_run(
        state: &AgentProcessState,
        run_id: &str,
        model: Option<&str>,
        backend: Option<&str>,
    ) {
        let mut processes = state.processes.lock().unwrap();
        let child = Arc::new(Mutex::new(None));
        let stdin = Arc::new(Mutex::new(Some(wrap_plain_stdin(Box::new(
            Vec::<u8>::new(),
        )))));
        let mut handle = RunHandle::new(child, stdin, Some(format!("sess-{run_id}")), None);
        handle.transition_to_warm();
        handle.model = model.map(|m| m.to_string());
        handle.backend = backend.map(|b| b.to_string());
        processes.register(run_id.to_string(), handle);
    }

    #[test]
    fn matching_model_reuses_without_set_model() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("opus")).unwrap();
        assert_eq!(decision, ReuseDecision::Reuse);
        assert_eq!(state.get_model("run-1"), Some("opus".to_string()));
    }

    #[test]
    fn no_desired_model_reuses() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None);
        let decision = ensure_reused_process_model(&state, "run-1", None).unwrap();
        assert_eq!(decision, ReuseDecision::Reuse);
    }

    #[test]
    fn changed_model_requests_restart_when_idle() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("sonnet"), None);
        let decision = ensure_reused_process_model(&state, "run-1", Some("opus")).unwrap();
        assert_eq!(decision, ReuseDecision::Restart);
        // No unconfirmed in-place switch: tracked model is left unchanged; the
        // fresh process records the new model at startup.
        assert_eq!(state.get_model("run-1"), Some("sonnet".to_string()));
    }

    #[test]
    fn changed_model_requests_restart_for_any_backend() {
        // The helper no longer cares about backend (rotation handles cross-backend
        // upstream); a model change on a same-backend process always restarts.
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("gpt-5.4-mini"), Some("codex"));
        let decision = ensure_reused_process_model(&state, "run-1", Some("gpt-5.4")).unwrap();
        assert_eq!(decision, ReuseDecision::Restart);
        assert_eq!(state.get_model("run-1"), Some("gpt-5.4-mini".to_string()));
    }

    #[test]
    fn changed_model_errors_when_busy() {
        let state = AgentProcessState::default();
        register_run(&state, "run-1", Some("opus"), None);
        // Mid-turn: a restart would kill active work → error instead.
        state.begin_turn("run-1", "turn-1");
        let err = ensure_reused_process_model(&state, "run-1", Some("sonnet")).unwrap_err();
        assert!(err.contains("busy"), "unexpected error: {err}");
    }
}
