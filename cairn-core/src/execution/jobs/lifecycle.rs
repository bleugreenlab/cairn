use super::*;

// ============================================================================
// on_job_complete_impl
// ============================================================================

/// Called when a job finishes. Advances the execution DAG if applicable.
///
/// Only advances for jobs that are part of a recipe DAG (have both `execution_id`
/// and `recipe_node_id`). Rows missing either field are not runnable DAG jobs and
/// are ignored here.
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
        _ => Ok(vec![]), // Not a runnable DAG job, so there is no DAG to advance.
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
    log::info!("[prepare_job] resolving owner for job {job_id}");
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
    let node_id = job
        .recipe_node_id
        .as_ref()
        .ok_or("Job has no recipe_node_id; standalone jobs are no longer runnable")?;
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
    let (needs_worktree, inherits_worktree, step_name): (bool, bool, String) = (
        behavior.needs_worktree,
        behavior.inherits_worktree,
        node.name.clone(),
    );

    log::info!(
        "[prepare_job] job {job_id}: behavior needs_worktree={needs_worktree} inherits_worktree={inherits_worktree} step={step_name}"
    );

    // ---- Worktree setup -------------------------------------------------
    // Create / inherit owner for the worktree lifecycle. The unit is the
    // `worktree_path`/`branch` pair: inherited children (below) copy the
    // parent's pair, so N jobs reference ONE path. Teardown
    // (`execution::teardown`) keys on the same pair; it happens once, at
    // issue/PR-terminal time, removing the path unconditionally.
    let assigned_context = if job.worktree_path.is_some() {
        run_db(workspace_identity::resolve_managed_workspace_context(
            owning_db.clone(),
            job_id.to_string(),
        ))?
    } else {
        None
    };
    let assigned_workspace_ready = assigned_context.as_ref().is_some_and(|context| {
        let path = context.identity.worktree_path.as_path();
        crate::jj::is_jj_dir(path)
            && crate::jj::read_workspace_identity(path).as_ref() == Some(&context.identity)
    });
    if assigned_workspace_ready {
        // The durable assignment and physical marker agree — reuse it.
        let _ = orch.services.emitter.emit(
            "job-activated",
            serde_json::json!({
                "jobId": job_id,
                "issueId": job.issue_id,
                "nodeName": job.node_name,
                "execSeq": exec_seq,
            }),
        );
    } else if inherits_worktree && job.worktree_path.is_none() {
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

        let worktrees_dir = crate::managed_worktrees::base_dir()
            .ok_or("Could not find managed worktrees directory")?;

        let initial_name = if let Some(ref existing) = job.branch {
            job.worktree_path
                .as_deref()
                .and_then(|path| Path::new(path).file_name())
                .and_then(|name| name.to_str())
                .map(str::to_string)
                .unwrap_or_else(|| existing.replace('/', "-"))
        } else {
            format!("{}-{}-{}-{}", project_key, display_id, safe_step_name, seq)
        };
        let initial_branch = job
            .branch
            .clone()
            .unwrap_or_else(|| format!("agent/{initial_name}"));

        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
        let lock_orch = orch.clone();
        let lock_store = store.clone();
        let lock_operation = format!("workspace provisioning for {job_id}");
        let _store_guard = run_db(async move {
            Ok(lock_orch
                .acquire_jj_store_lock(&lock_store, lock_operation)
                .await)
        })?;
        crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path))?;

        let short_job: String = job_id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .take(8)
            .collect();
        let mut branch = initial_branch.clone();
        let mut wt_dir = initial_name.clone();
        let mut allow_retry_cleanup = false;
        for attempt in 0..=100 {
            let wt_path = worktrees_dir.join(&wt_dir);
            let workspace_name = crate::jj::workspace_name_for_branch(&branch);
            let db_owner = run_db(workspace_identity::coordinate_owner(
                owning_db.clone(),
                job.project_id.clone(),
                branch.clone(),
                wt_path.to_string_lossy().to_string(),
            ))?;
            let exact_retry = db_owner.as_deref() == Some(job_id)
                && job.branch.as_deref() == Some(branch.as_str())
                && job.worktree_path.as_deref() == Some(wt_path.to_string_lossy().as_ref());
            let occupied = wt_path.exists()
                || crate::jj::workspace_registered(&jj, &store, &workspace_name)
                || crate::jj::bookmark_commit(&jj, &store, &branch).is_some()
                || db_owner.is_some();
            if exact_retry || !occupied {
                allow_retry_cleanup = exact_retry;
                break;
            }
            if attempt == 100 {
                return Err(format!(
                    "Could not allocate an unoccupied managed workspace for job {job_id}"
                ));
            }
            let suffix = if attempt == 0 {
                format!("-j{short_job}")
            } else {
                format!("-j{short_job}-{attempt}")
            };
            branch = format!("{}{}", initial_branch, suffix);
            wt_dir = format!("{}{}", initial_name, suffix);
        }

        let wt_path = worktrees_dir.join(&wt_dir);
        let base_ref = job.base_branch.as_deref().unwrap_or("HEAD");
        let base_rev = crate::jj::resolve_base_rev(&jj, &store, base_ref, |r| {
            orch.services
                .git
                .rev_parse(Path::new(&repo_path), vec![r.to_string()])
                .ok()
                .filter(|sha| !sha.is_empty())
        });
        let identity = crate::jj::WorkspaceIdentity::new(
            job_id,
            job_id,
            job.project_id.clone(),
            PathBuf::from(&repo_path),
            wt_path.clone(),
            branch.clone(),
            crate::jj::workspace_name_for_branch(&branch),
            base_rev,
        );
        let now = chrono::Utc::now().timestamp() as i32;
        run_db(workspace_identity::assign_workspace_if_unset_or_same(
            owning_db.clone(),
            job_id.to_string(),
            job.worktree_path.clone(),
            job.branch.clone(),
            wt_path.to_string_lossy().to_string(),
            branch.clone(),
            now,
        ))?;
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

        log::info!(
            "[prepare_job] job {job_id}: creating worktree {} from {base_ref} on branch {branch}",
            wt_path.display()
        );
        let setup_result = prepare_worktree_for_job(
            orch,
            &repo_path,
            &wt_path,
            &branch,
            base_ref,
            &identity,
            allow_retry_cleanup,
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

        log::info!("[prepare_job] job {job_id}: worktree setup complete");
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
    log::info!("[prepare_job] job {job_id}: loaded agent config");

    // ---- Create session + run record --------------------------------------
    let run_id = ids::mint_child(job_id);
    let now = chrono::Utc::now().timestamp() as i32;
    let status_str = RunStatus::Starting.to_string();

    // Ensure a Session record exists for this job and derive the first-start mode.
    let had_current_session = job.current_session_id.is_some();
    log::info!("[prepare_job] job {job_id}: preparing session");
    let (session_id, session_start, run_start_mode) = run_db(prepare_session(
        owning_db.clone(),
        job_id.to_string(),
        job.clone(),
        now,
    ))?;
    log::info!("[prepare_job] job {job_id}: session {session_id} prepared");
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

    log::info!("[prepare_job] job {job_id}: inserting run {run_id}");
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

    log::info!("[prepare_job] job {job_id}: run {run_id} inserted");
    if existing_active_count > 0 {
        log::warn!(
            "[prepare_job] Job {} already has {} active runs",
            job_id,
            existing_active_count
        );
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids(
            "insert",
            &run_id,
            Some(job_id),
            job.issue_id.as_deref(),
            Some(&job.project_id),
        ),
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
// continue_job_or_enqueue
// ============================================================================

/// User-facing continue: enqueue instead of resuming when a turn is already
/// active, so a stale composer send never 500s and never drops the message.
///
/// The chat composer decides send-vs-queue from a cached head-turn read; when a
/// turn goes active just after that read was taken, a plain Enter still routes to
/// the `continue_job` command (this path) rather than the queue. Left unguarded,
/// that reaches [`continue_job_impl`] with a `running`/`pending` head turn, trips
/// the active-turn guard, and returns an error the composer surfaces as a 500 —
/// dropping the typed text (CAIRN-2657).
///
/// Mirroring the direct-message guard ([`crate::messages::delivery::head_turn_active`]),
/// a message-bearing send against an active turn lands in the queue
/// ([`crate::messages::queued::Delivery::Queue`], delivered at turn end) instead.
/// The existing turn-end / flush-on-idle machinery already claims and delivers
/// `queue` rows, so no new delivery code is needed. Guarding here — the
/// authoritative layer — protects every caller of the command, not just the
/// composer. Internal Rust callers (prompt/permission answers, wakes, delegation
/// resume, advancement) keep calling [`continue_job_impl`] directly: they run at
/// genuine turn boundaries where no turn is active and must not be silently
/// converted into queued messages.
pub fn continue_job_or_enqueue(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<Run, String> {
    // Only a message-bearing send can be queued; a bare continue has nothing to
    // enqueue, so it falls straight through to the resume (and its guard).
    if let Some(text) = message.filter(|m| !m.trim().is_empty()) {
        // Resolve the owning DB (team job -> its synced replica), mirroring
        // continue_job_impl's own routing so team turns/queued rows stay correct.
        let owning_db = run_db({
            let dbs = orch.db.clone();
            let job_id = job_id.to_string();
            async move {
                crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?;
        if crate::messages::delivery::head_turn_active_sync(&owning_db, job_id) {
            crate::messages::queued::enqueue(
                &owning_db,
                job_id,
                text,
                crate::messages::queued::Delivery::Queue,
            )?;
            // Make the pending-queue chip appear immediately: QueryProvider
            // invalidates `queuedMessages` on this db-change, the same mechanism
            // the composer's own enqueue path relies on.
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "queued_messages", "action": "update"}),
            );
            // Return the job's latest run to honor the Run return contract; the
            // frontend ignores the value. `list_runs_for_job` orders newest-first,
            // matching the frontend's `runs[0]` "latest" convention. Fall through to
            // the resume only in the pathological case where an active turn has no
            // run row at all.
            if let Some(run) = crate::runs::queries::list_runs_for_job(owning_db.clone(), job_id)
                .map_err(|e| e.to_string())?
                .into_iter()
                .next()
            {
                return Ok(run);
            }
        }
    }
    continue_job_impl(
        orch,
        job_id,
        message,
        identity_override,
        Some(ResumeContext {
            supersede_pending_retry: true,
            ..Default::default()
        }),
    )
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
    /// Consume this already-claimed pending retry turn instead of creating a
    /// follow-up. Used only by best-effort automatic backend retries.
    pub preclaimed_retry_turn_id: Option<String>,
    /// User-facing continuation may take over an unstarted automatic retry.
    /// Reclassifying that pending head as a follow-up resets the retry budget
    /// and makes the sleeping timer's retry-head check fail.
    pub supersede_pending_retry: bool,
}

pub(crate) fn continue_automatic_retry(
    orch: &Orchestrator,
    job_id: &str,
    retry_turn_id: &str,
) -> Result<Run, String> {
    continue_job_impl(
        orch,
        job_id,
        None,
        None,
        Some(ResumeContext {
            suppress_user_event: true,
            preclaimed_retry_turn_id: Some(retry_turn_id.to_string()),
            supersede_pending_retry: false,
        }),
    )
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

    let preclaimed_retry_turn_id = prompt_resume
        .as_ref()
        .and_then(|context| context.preclaimed_retry_turn_id.clone());
    if let Some(retry_turn_id) = preclaimed_retry_turn_id.as_deref() {
        if job.parent_job_id.is_some()
            || job.recipe_node_id.is_some()
            || job.agent_config_id.as_deref() == Some("workflow")
            || !pending_retry_head_matches(owning_db.clone(), job_id, retry_turn_id)?
        {
            return Err("automatic retry was superseded or is ineligible".to_string());
        }
    }

    // CAIRN-2629: same device-ownership guard as the start/claim path — refuse to
    // resume an execution owned by another machine (its runner owns the lifecycle).
    // Fail-open on a read hiccup (deferred_owner returns None), and treat a NULL /
    // this-machine owner as "may proceed".
    if let Some(exec_id) = job.execution_id.clone() {
        let this_device = orch.anon_device_manager.device_id();
        let deferred = run_db({
            let owning_db = owning_db.clone();
            async move {
                Ok::<_, String>(
                    crate::execution::ownership::deferred_owner(&owning_db, &exec_id, &this_device)
                        .await,
                )
            }
        })?;
        if let Some(owner) = deferred {
            return Err(format!(
                "This execution is owned by device {owner}, not this machine; resume it there."
            ));
        }
    }

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
    // Cold-resume reseed decision (CAIRN-2534) is produced in the plain-resume
    // arm below and threaded into prompt assembly + seed-event storage.
    let mut reseed_outcome: Option<ReseedOutcome> = None;
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
                match decide_continue_action(
                    &session.backend,
                    session.backend_id.as_deref(),
                    desired_backend.as_deref(),
                    needs_fresh_session,
                ) {
                    ContinueSessionAction::RotateToBackend(want) => {
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
                    ContinueSessionAction::RotateFresh => {
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
                    ContinueSessionAction::Resume => {
                        // Native backend resume of the open session (e.g. a Codex
                        // session carrying its stored thread id). Reseed is
                        // bypassed so a reply never restarts at the system prompt
                        // (CAIRN-2598).
                        let session_start = resolve_continue_session_start(&session)?;
                        let run_start_mode = run_start_mode(&session_start).to_string();
                        (session.id.clone(), session_start, run_start_mode)
                    }
                    ContinueSessionAction::MaybeReseed => {
                        // Reseed-eligible open session. If it has gone stale (last
                        // event older than the staleness threshold), a native
                        // backend resume would reload a prompt cache the provider
                        // has likely evicted; reseed instead by rotating to a
                        // fresh session primed with the node's `/chat` digest
                        // (CAIRN-2534). Any failure in the attempt falls open to
                        // native resume with session state untouched.
                        let now_secs = orch.services.clock.now();
                        match attempt_session_reseed(orch, &owning_db, &job, &session, now_secs) {
                            Some(outcome) => {
                                let session_start = crate::backends::SessionStart::New {
                                    session_id: outcome.new_session_id.clone(),
                                };
                                let run_start_mode = run_start_mode(&session_start).to_string();
                                let new_id = outcome.new_session_id.clone();
                                reseed_outcome = Some(outcome);
                                (new_id, session_start, run_start_mode)
                            }
                            None => {
                                let session_start = resolve_continue_session_start(&session)?;
                                let run_start_mode = run_start_mode(&session_start).to_string();
                                (session.id.clone(), session_start, run_start_mode)
                            }
                        }
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
    // A delayed retry is best-effort. Recheck its durable claim immediately
    // before allocating or waking a run so a superseding action wins cleanly.
    if let Some(retry_turn_id) = preclaimed_retry_turn_id.as_deref() {
        if !pending_retry_head_matches(owning_db.clone(), job_id, retry_turn_id)? {
            return Err("automatic retry was superseded before run launch".to_string());
        }
    }
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
            crate::notify::run_db_change_ids(
                "insert",
                &new_run_id,
                Some(job_id),
                issue_id.as_deref(),
                Some(&project_id),
            ),
        );
        (new_run_id, false)
    };

    // ---- Create successor turn for follow-up ----------------------------
    // A resume carrying user-authored content (an explicit message or a
    // prompt/permission answer) is a work turn, never the post-completion
    // memory-review reflection. The pending-queued-message case (a user steer
    // that arrives without an explicit message) is detected inside
    // `create_followup_turn` against the rows the claim below sweeps up.
    let user_initiated = message.is_some()
        || prompt_resume
            .as_ref()
            .is_some_and(|context| context.preclaimed_retry_turn_id.is_none());
    let supersede_pending_retry = prompt_resume
        .as_ref()
        .is_some_and(|context| context.supersede_pending_retry);
    let turn_id = if let Some(retry_turn_id) = preclaimed_retry_turn_id {
        if !pending_retry_head_matches(owning_db.clone(), job_id, &retry_turn_id)? {
            return Err("automatic retry was superseded before launch".to_string());
        }
        retry_turn_id
    } else {
        create_followup_turn(
            orch,
            &session_id,
            job_id,
            user_initiated,
            supersede_pending_retry,
        )?
    };
    let retry_turn_prestarted = prompt_resume
        .as_ref()
        .is_some_and(|context| context.preclaimed_retry_turn_id.is_some());
    if retry_turn_prestarted && !claim_retry_turn_start(orch, &turn_id, &run_id)? {
        return Err("automatic retry was superseded before turn reservation".to_string());
    }

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
    let artifact_handoff_note = artifact_handoff_resume_note(owning_db.clone(), job_id)?;
    let resume_note = match (
        suppress_user_event.then_some(RESUME_AFTER_SELF_SUSPEND_NOTE),
        artifact_handoff_note.as_deref(),
    ) {
        (Some(self_suspend), Some(handoff)) => Some(format!("{self_suspend}\n\n{handoff}")),
        (Some(self_suspend), None) => Some(self_suspend.to_string()),
        (None, Some(handoff)) => Some(handoff.to_string()),
        (None, None) => None,
    };
    let prompt = assemble_resume_prompt(
        resume_note.as_deref(),
        queued_block,
        &base_prompt,
        side_channel_block,
        push_prompt.as_deref(),
    );
    // Reseed: the seed (header + prior-session digest) leads the reconstructed
    // context; the normal trigger tail follows. The seed is also stored as a
    // collapsible `user:seed` event below, while the trigger keeps its own
    // verbatim user event (CAIRN-2534).
    let prompt = apply_reseed_seed(prompt, reseed_outcome.as_ref());
    // This is the single outer turn-boundary stamp. Apply it after every producer,
    // including cold-resume reseeding, so all resumed input opens with the clock.
    let previous_turn_end = previous_turn_end_for_resume(owning_db.clone(), &turn_id)?;
    let prompt = prepend_resume_stamp(
        prompt,
        &crate::clock::HostClock::local(),
        chrono::Utc::now(),
        previous_turn_end,
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
    // Reseed: store the seed (header + digest) as a collapsible `user:seed`
    // event ahead of every other event in this turn, so the transcript renders a
    // divider followed by the verbatim trigger message rather than a giant seed
    // bubble, and future digests collapse the seed to one line (CAIRN-2534).
    if let Some(outcome) = &reseed_outcome {
        store_seed_event_with_turn(
            orch,
            &run_id,
            &session_id,
            &outcome.seed_content,
            now,
            -1,
            Some(&turn_id),
        )?;
    }
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
        if !retry_turn_prestarted {
            start_turn(orch, &turn_id, &run_id)?;
        }
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
            // Recipe node jobs write their artifact via the write verb through
            // the normal confirm/review flow; they are never natively
            // constrained (CAIRN-2505).
            false,
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
        if !retry_turn_prestarted {
            start_turn(orch, &turn_id, &run_id)?;
        }
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

fn artifact_handoff_resume_note(
    owning_db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    run_db(async move {
        owning_db
            .query_opt(
                "SELECT a.output_name, a.artifact_type, a.version, a.confirmed
                   FROM jobs j
                   JOIN turns t ON t.id = j.current_turn_id
                   JOIN artifacts a ON a.job_id = j.id
                  WHERE j.id = ?1 AND t.end_reason = 'artifact_handoff'
                  ORDER BY a.created_at DESC, a.rowid DESC
                  LIMIT 1",
                (job_id,),
                |row| {
                    Ok((
                        row.opt_text(0)?,
                        row.text(1)?,
                        row.i64(2)? as i32,
                        row.i64(3)? != 0,
                    ))
                },
            )
            .await
            .map_err(|error| format!("Failed to resolve artifact handoff resume note: {error}"))
            .map(|artifact| {
                artifact.map(|(output_name, artifact_type, version, confirmed)| {
                    let name = output_name.unwrap_or(artifact_type);
                    let state = if confirmed {
                        "applied successfully"
                    } else {
                        "applied successfully and is awaiting user confirmation"
                    };
                    format!(
                        "Resuming after an artifact handoff. Your previous turn ended because you wrote your terminal output artifact (cairn:~/{name}, version {version}) — the write was {state} and the session paused for review. Any interruption notice in the prior transcript was Cairn ending the turn at that boundary, not a user abort. The message that resumes you follows."
                    )
                })
            })
    })
}

/// Short note that leads the forwarded prompt on a durable self-suspend resume.
///
/// On the slow (>45s) path the awaited result is delivered as the suspended tool
/// call's synthetic `tool_result`, so to the resumed agent that call looks
/// interrupted mid-execution and then returns — a deliberate pause that reads
/// like a glitch. Naming it as an intentional suspend up front removes that
/// tension (CAIRN-2173).
const RESUME_AFTER_SELF_SUSPEND_NOTE: &str = "Resuming after an intentional self-suspend. Your run paused itself to wait on delegated work (a sub-agent task or a user question) and resumed now that the work finished — the earlier tool call was suspended on purpose, not interrupted or errored, and its result follows.";

fn prepend_resume_stamp(
    prompt: String,
    clock: &crate::clock::HostClock,
    now: chrono::DateTime<chrono::Utc>,
    previous_turn_end: Option<i64>,
) -> String {
    format!(
        "{}\n\n{}",
        clock.resume_prefix(now, previous_turn_end),
        prompt
    )
}

fn previous_turn_end_for_resume(db: Arc<LocalDb>, turn_id: &str) -> Result<Option<i64>, String> {
    let turn_id = turn_id.to_string();
    run_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT predecessor.ended_at
                           FROM turns current
                           LEFT JOIN turns predecessor ON predecessor.id = current.predecessor_id
                          WHERE current.id = ?1
                          LIMIT 1",
                        (turn_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => row.opt_i64(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

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

// ============================================================================
// Cold-resume reseed (CAIRN-2534)
// ============================================================================

/// Cold-resume reseed staleness threshold: when an open session's last event is
/// older than this at resume, the native backend resume is replaced by a fresh
/// session seeded with the node's `/chat` digest. One hour comfortably outlives
/// a provider's prompt-cache TTL, so a resume inside the window still hits a warm
/// cache and reseeding would buy nothing.
const SESSION_STALENESS_THRESHOLD_SECS: i64 = 60 * 60;

/// Header that leads the reseed seed prompt, framing the digest that follows.
const RESEED_SEED_HEADER: &str = "The prior events in this session are summarized below because the underlying agent session was reconstructed after a period of inactivity (tool-result bodies were elided). Re-read any files whose content is still relevant, and re-read your working documents (todos/plan/board) for the current plan state before acting.";

/// Resolve the staleness threshold, honoring the `CAIRN_SESSION_STALENESS_SECS`
/// dev/test override (parsed, non-empty) and falling back to the constant —
/// mirroring the `CAIRN_JJ_BIN` env-override convention.
fn staleness_threshold_secs() -> i64 {
    std::env::var("CAIRN_SESSION_STALENESS_SECS")
        .ok()
        .and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<i64>().ok()
            }
        })
        .unwrap_or(SESSION_STALENESS_THRESHOLD_SECS)
}

/// Pure staleness predicate: the session is stale once its last event is more
/// than `threshold` seconds behind `now`. A resume exactly at the boundary is
/// not stale (native resume), so the comparison is strict.
fn is_session_stale(now: i64, last_event_at: i64, threshold: i64) -> bool {
    now - last_event_at > threshold
}

/// The `sessions.backend` value persisted for a Codex session. A Codex session
/// with a stored `backend_id` (its native thread id) resumes through
/// `thread/resume`, which has its own stale-thread fallback (a fresh thread with
/// transcript preload) that only fires after Codex reports the thread missing.
const CODEX_SESSION_BACKEND: &str = "codex";

/// Whether an open session resumes natively and so must NOT be preempted by the
/// cold-resume reseed (CAIRN-2534). Codex sessions carrying a native thread id
/// (`backend_id`) resume via `thread/resume`; rotating them to a fresh
/// `SessionStart::New` would wipe the Codex thread and restart the reply at the
/// system prompt (CAIRN-2598). Claude and handle-less sessions stay
/// reseed-eligible.
fn session_prefers_native_resume(backend: &str, backend_id: Option<&str>) -> bool {
    backend.eq_ignore_ascii_case(CODEX_SESSION_BACKEND) && backend_id.is_some()
}

/// The continuation action for an open session on a user reply. This is the pure
/// decision; the caller performs the DB rotation / reseed / native-resume work.
#[derive(Debug, PartialEq, Eq)]
enum ContinueSessionAction {
    /// The requested model implies a different backend than the session was
    /// started on — rotate to a fresh session on that backend (the old backend's
    /// resume handle is invalid on the new one).
    RotateToBackend(String),
    /// A prompt edit since spawn requires a fresh same-backend session so this
    /// turn rebuilds the edited system prompt.
    RotateFresh,
    /// Attempt the stale cold-resume reseed, falling back to native resume.
    MaybeReseed,
    /// Native backend resume of the open session.
    Resume,
}

/// Decide how an open session continues on a user reply. Ordering matters: a
/// cross-backend model change rotates first, then a prompt edit forces a fresh
/// same-backend session, then a session that resumes natively (Codex with a
/// stored thread id) resumes directly, and only the remaining sessions are
/// eligible for the cold-resume reseed.
fn decide_continue_action(
    session_backend: &str,
    session_backend_id: Option<&str>,
    desired_backend: Option<&str>,
    needs_fresh_session: bool,
) -> ContinueSessionAction {
    if let Some(want) = desired_backend.filter(|want| *want != session_backend) {
        return ContinueSessionAction::RotateToBackend(want.to_string());
    }
    if needs_fresh_session {
        return ContinueSessionAction::RotateFresh;
    }
    if session_prefers_native_resume(session_backend, session_backend_id) {
        return ContinueSessionAction::Resume;
    }
    ContinueSessionAction::MaybeReseed
}

/// The seed content delivered and stored on a reseed: the header framing the
/// digest, then the digest itself (`header + digest`). The trigger is appended
/// separately at delivery and stored as its own verbatim `user` event.
fn build_reseed_seed_content(digest: &str) -> String {
    format!("{RESEED_SEED_HEADER}\n\n{digest}")
}

/// Prepend the reseed seed to the trigger prompt when this resume reseeded;
/// identity otherwise. Keeps the seed-before-trigger ordering in one place.
fn apply_reseed_seed(prompt: String, reseed: Option<&ReseedOutcome>) -> String {
    match reseed {
        Some(outcome) => format!("{}\n\n{}", outcome.seed_content, prompt),
        None => prompt,
    }
}

/// A successful cold-resume reseed: the fresh session that was rotated in, and
/// the seed (header + prior-session digest) to deliver and store.
struct ReseedOutcome {
    new_session_id: String,
    seed_content: String,
}

/// Attempt a cold-resume reseed for an open, stale session (CAIRN-2534).
///
/// Returns `Some` only when the session is stale AND a fresh session was
/// successfully rotated in; the caller then spawns that fresh session and stores
/// the seed. Returns `None` — fail open — when the session is still fresh or any
/// step fails. Rotation is the last, only-persisted mutation, so a `None` from a
/// mid-attempt failure leaves session state exactly as a native resume would.
///
/// The digest is rendered from the prior history *before* any rotation or new
/// event, so it can never contain itself.
fn attempt_session_reseed(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    session: &Session,
    now: i64,
) -> Option<ReseedOutcome> {
    let last_event_at = run_db(load_last_event_time_for_session(
        owning_db.clone(),
        session.id.clone(),
    ))
    .ok()
    .flatten()?;
    if !is_session_stale(now, last_event_at, staleness_threshold_secs()) {
        return None;
    }

    // Render the digest of the prior history before rotating or storing any
    // event, through the one canonical digest path.
    let digest = {
        let db = owning_db.clone();
        let job = job.clone();
        run_db(
            async move { Ok::<_, String>(crate::resources::render_reseed_digest(&db, &job).await) },
        )
        .ok()?
    };
    if digest.trim().is_empty() || digest == "No runs found for this node." {
        return None;
    }
    let seed_content = build_reseed_seed_content(&digest);

    // Evict any live process bound to the old session; the fresh session id then
    // cold-spawns naturally (its warm-process lookup returns None).
    if let Some(old_run) = orch.process_state.find_process_by_session(&session.id) {
        orch.process_state.stop_and_remove(&old_run);
    }

    // Rotate LAST — the only persisted mutation. A failure here still leaves the
    // native-resume path fully intact.
    let new_session =
        run_db({
            let db = owning_db.clone();
            let session = session.clone();
            let job_id = job.id.clone();
            async move {
                crate::sessions::queries::rotate_job_session(db.as_ref(), &session, &job_id).await
            }
        })
        .ok()?;

    log::info!(
        "Cold-resume reseed for job {}: rotated session {} -> {} ({}s idle > {}s)",
        &job.id[..job.id.len().min(8)],
        &session.id[..session.id.len().min(8)],
        &new_session.id[..new_session.id.len().min(8)],
        now - last_event_at,
        staleness_threshold_secs(),
    );
    Some(ReseedOutcome {
        new_session_id: new_session.id,
        seed_content,
    })
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
    let turn_change = run_db({
        let db = owning_db.clone();
        let turn_id = turn_id.clone();
        async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
    })?;
    let _ = orch.services.emitter.emit("db-change", turn_change);
    if let Some(run_id) = run_id.as_deref() {
        let run_change = run_db({
            let db = owning_db.clone();
            let run_id = run_id.to_string();
            async move { Ok(crate::notify::run_db_change_for_id(&db, &run_id, "update").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", run_change);
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
    use super::{
        apply_reseed_seed, assemble_resume_prompt, build_reseed_seed_content,
        decide_continue_action, ensure_reused_process_model, is_session_stale,
        prepend_resume_stamp, previous_turn_end_for_resume, session_prefers_native_resume,
        staleness_threshold_secs, ContinueSessionAction, ReseedOutcome, ReuseDecision,
        RESEED_SEED_HEADER, SESSION_STALENESS_THRESHOLD_SECS,
    };
    use crate::agent_process::process::{wrap_plain_stdin, AgentProcessState, RunHandle};
    use crate::storage::migrated_test_db;
    use std::sync::{Arc, Mutex};

    #[tokio::test(flavor = "current_thread")]
    async fn resume_elapsed_uses_stored_predecessor_turn_end() {
        let db = Arc::new(migrated_test_db("resume-clock-predecessor").await);
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES('j','p','running','s',1,1);
             INSERT INTO sessions(id, job_id, backend, status, created_at, updated_at)
               VALUES('s','j','codex','open',1,1);
             INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
               VALUES('previous','s','j',1,'completed','initial',1,1000,1000);
             INSERT INTO turns(id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at)
               VALUES('current','s','j',2,'previous','running','follow_up',1100,1100);",
        )
        .await
        .unwrap();

        assert_eq!(
            previous_turn_end_for_resume(db, "current").unwrap(),
            Some(1000)
        );
    }

    #[test]
    fn resumed_input_stamp_is_outermost_even_for_reseeded_context() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let now = chrono::DateTime::from_timestamp(1_752_381_600, 0).unwrap();
        let reconstructed = apply_reseed_seed(
            "TRIGGER".to_string(),
            Some(&ReseedOutcome {
                new_session_id: "session-new".to_string(),
                seed_content: "SEED".to_string(),
            }),
        );
        let prompt =
            prepend_resume_stamp(reconstructed, &clock, now, Some(now.timestamp() - 192 * 60));
        assert_eq!(
            prompt,
            "[Sat 21:40 — resumed after 3h 12m]\n\nSEED\n\nTRIGGER"
        );
    }

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

    #[test]
    fn fresh_session_is_not_stale() {
        // Under the threshold → native resume (not stale).
        assert!(!is_session_stale(1_000, 1_000, 3_600));
        assert!(!is_session_stale(4_599, 1_000, 3_600));
    }

    #[test]
    fn stale_session_is_detected() {
        // Over the threshold → reseed (stale).
        assert!(is_session_stale(4_601, 1_000, 3_600));
    }

    #[test]
    fn staleness_boundary_is_not_stale() {
        // Exactly at the threshold is NOT stale — the comparison is strict, so a
        // resume right at the edge takes the native path.
        assert!(!is_session_stale(4_600, 1_000, 3_600));
    }

    #[test]
    #[serial_test::serial(reseed_staleness_env)]
    fn staleness_threshold_honors_env_override_then_falls_back() {
        let original = std::env::var("CAIRN_SESSION_STALENESS_SECS").ok();

        std::env::set_var("CAIRN_SESSION_STALENESS_SECS", "30");
        assert_eq!(staleness_threshold_secs(), 30);

        // Blank / unparseable values fall back to the constant.
        std::env::set_var("CAIRN_SESSION_STALENESS_SECS", "  ");
        assert_eq!(staleness_threshold_secs(), SESSION_STALENESS_THRESHOLD_SECS);
        std::env::set_var("CAIRN_SESSION_STALENESS_SECS", "not-a-number");
        assert_eq!(staleness_threshold_secs(), SESSION_STALENESS_THRESHOLD_SECS);

        std::env::remove_var("CAIRN_SESSION_STALENESS_SECS");
        assert_eq!(staleness_threshold_secs(), SESSION_STALENESS_THRESHOLD_SECS);

        match original {
            Some(value) => std::env::set_var("CAIRN_SESSION_STALENESS_SECS", value),
            None => std::env::remove_var("CAIRN_SESSION_STALENESS_SECS"),
        }
    }

    #[test]
    fn reseed_seed_orders_header_digest_then_trigger() {
        // The seed content is HEADER + digest; applying it to the trigger yields
        // HEADER + digest + trigger, in that order (CAIRN-2534).
        let seed_content = build_reseed_seed_content("DIGEST_BODY");
        assert!(seed_content.starts_with(RESEED_SEED_HEADER));
        assert!(seed_content.contains("DIGEST_BODY"));

        let outcome = ReseedOutcome {
            new_session_id: "sess-new".to_string(),
            seed_content: seed_content.clone(),
        };
        let delivered = apply_reseed_seed("TRIGGER".to_string(), Some(&outcome));
        assert_eq!(delivered, format!("{seed_content}\n\nTRIGGER"));
        let header_at = delivered.find(RESEED_SEED_HEADER).unwrap();
        let digest_at = delivered.find("DIGEST_BODY").unwrap();
        let trigger_at = delivered.find("TRIGGER").unwrap();
        assert!(header_at < digest_at && digest_at < trigger_at);
    }

    #[test]
    fn apply_reseed_seed_is_identity_without_reseed() {
        // A non-reseed resume delivers the trigger unchanged.
        assert_eq!(apply_reseed_seed("TRIGGER".to_string(), None), "TRIGGER");
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
    fn codex_reply_with_thread_id_resumes_not_reseeds() {
        // The CAIRN-2598 regression: an open Codex child-task session whose
        // backend matches the requested one and whose native thread id is stored
        // must resume, never rotate to a fresh session (which restarts at the
        // system prompt and wipes history).
        assert_eq!(
            decide_continue_action("codex", Some("thread-existing"), Some("codex"), false),
            ContinueSessionAction::Resume
        );
        // With no desired backend supplied (nothing to compare), a Codex session
        // with a thread id still resumes rather than reseeding.
        assert_eq!(
            decide_continue_action("codex", Some("thread-existing"), None, false),
            ContinueSessionAction::Resume
        );
    }

    #[test]
    fn matching_backend_without_native_handle_is_reseed_eligible() {
        // A Claude session (no native-resume policy) still flows through the
        // cold-resume reseed path.
        assert_eq!(
            decide_continue_action("claude", Some("sess-abc"), Some("claude"), false),
            ContinueSessionAction::MaybeReseed
        );
        // A Codex session with no stored thread id can't resume natively, so it
        // is reseed-eligible too.
        assert_eq!(
            decide_continue_action("codex", None, Some("codex"), false),
            ContinueSessionAction::MaybeReseed
        );
    }

    #[test]
    fn cross_backend_model_change_rotates() {
        // This is what the hardcoded-"claude" child session bug produced every
        // reply: desired "codex" vs stored "claude" rotated to a fresh session.
        assert_eq!(
            decide_continue_action("claude", Some("sess"), Some("codex"), false),
            ContinueSessionAction::RotateToBackend("codex".to_string())
        );
    }

    #[test]
    fn prompt_edit_forces_fresh_same_backend_session() {
        // needs_fresh_session wins over native resume: the edited system prompt
        // requires a fresh session even for a Codex thread.
        assert_eq!(
            decide_continue_action("codex", Some("thread-x"), Some("codex"), true),
            ContinueSessionAction::RotateFresh
        );
    }

    #[test]
    fn native_resume_predicate_is_codex_with_handle_only() {
        assert!(session_prefers_native_resume("codex", Some("thread-x")));
        assert!(session_prefers_native_resume("Codex", Some("thread-x")));
        assert!(!session_prefers_native_resume("codex", None));
        assert!(!session_prefers_native_resume("claude", Some("sess")));
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
