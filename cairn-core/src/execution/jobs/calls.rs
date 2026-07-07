use super::*;
use crate::models::{ConfirmPolicy, OutputSchemaInfo};

// ============================================================================
// create_call_run
// ============================================================================

/// Create an ephemeral agent-call run: a node-less job+session+run started
/// directly (never via DAG advancement), carrying a per-run output contract and
/// running either in the caller's inherited worktree or a fresh scratch dir.
///
/// This is the call-primitive sibling of [`create_child_task`] (CAIRN-2481): the
/// same real-run shape (job, session, run, event stream, backend session), but
/// with a per-call worktree mode, an enforced output schema, and durable
/// workflow tags. The spawn pipeline persists a pre-materialized `CallTool`
/// packet around it so the delegated-task wait/suspend/resume machinery is
/// reused unchanged.
///
/// Splitting `prepare` (rows + transcript seed) from [`start_call_run`] (the
/// backend session) lets the caller persist the call packet BETWEEN them, so a
/// packet always exists before the run can finalize — closing the fast-finish
/// resume race.
pub(crate) fn prepare_call_run(
    orch: &Orchestrator,
    input: CreateCallRunInput,
) -> Result<PreparedCallRun, String> {
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

    let parent_job = run_db(load_job(
        db.clone(),
        input.parent_job_id.clone(),
        "Parent job not found",
    ))?;
    let project_id = parent_job.project_id.clone();
    let issue_id = parent_job.issue_id.clone();
    // The call job hangs off the parent's task-execution (created/ensured by the
    // spawn pipeline) so its snapshot carries the call packet.
    let execution_id = input
        .execution_id
        .clone()
        .or_else(|| parent_job.execution_id.clone());

    let project_path = run_db(load_project_path(orch.db.clone(), project_id.clone()))?;

    // ---- Load agent config from files (mirrors create_child_task) --------
    let config_dir = config::get_config_dir()?;
    let mut agent_config: AgentConfig = {
        let file_agent = match config_agents::get_agent(
            &config_dir,
            &input.subagent_type,
            project_path.as_deref(),
        ) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
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

    // ---- Mint ids + resolve model selection -----------------------------
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
    let (selection, extras) = resolve_runtime_selection(authored_tier, authored_backend, &presets)?;
    let selected_model = Some(selection.model.clone());
    agent_config.selection = Some(selection);
    agent_config.extras = Some(extras);

    // ---- Resolve the worktree mode --------------------------------------
    // `inherit` shares the caller's worktree (mutating calls are first-class);
    // `none` runs in a fresh scratch dir under $TMPDIR (sandbox-writable) with no
    // project-tree binding, so the call cannot read or mutate the caller's tree.
    let (worktree_path, working_dir, base_commit) = match input.worktree {
        CallWorktree::Inherit => {
            let path = parent_job
                .worktree_path
                .clone()
                .ok_or("Parent job has no worktree - cannot inherit for a call")?;
            let base = worktree_head_commit(orch, Path::new(&path));
            (Some(path.clone()), path, base)
        }
        CallWorktree::None => {
            let scratch = std::env::temp_dir().join(format!("cairn-call-{run_id}"));
            std::fs::create_dir_all(&scratch)
                .map_err(|e| format!("Failed to create call scratch dir: {e}"))?;
            (None, scratch.to_string_lossy().into_owned(), None)
        }
    };

    // ---- Persist job + session + run ------------------------------------
    let output_contract_json = serde_json::to_string(&input.output_contract)
        .map_err(|e| format!("Failed to serialize call output contract: {e}"))?;

    run_db(insert_child_job_session_run(
        db.clone(),
        ChildInsert {
            job_id: job_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_job_id: input.parent_job_id.clone(),
            worktree_path: worktree_path.clone(),
            // A call inherits the parent's branch (inherit mode) or runs in a
            // scratch dir with no jj branch (none mode); it never owns an
            // ephemeral task worktree.
            branch: None,
            agent_config_id: agent_config.id.clone(),
            project_id: project_id.clone(),
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            description: input.description.clone(),
            model: selected_model.as_ref().map(|m| m.to_string()),
            base_commit,
            owns_ephemeral_worktree: false,
            output_contract: Some(output_contract_json),
            label: input.label.clone(),
            phase: input.phase.clone(),
            parent_tool_use_id: input.parent_tool_use_id.clone(),
            task_index: input.task_index,
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

    // Link this call's run to its workflow journal key (private, runner-
    // transient) so the completion path journals the result under
    // (workflow_run_id, ordinal). Best-effort: journaling never blocks a call.
    if let Some(link) = input.workflow_journal_link.as_ref() {
        if let Err(e) = run_db({
            let db = orch.db.local.clone();
            let run_id = run_id.clone();
            let link = link.clone();
            async move { crate::workflow_journal::store_call_link(&db, &run_id, &link).await }
        }) {
            log::warn!("Failed to record workflow_call link for run {run_id}: {e}");
        }
    }

    // ---- Seed the transcript --------------------------------------------
    store_user_event(orch, &run_id, &session_id, &input.prompt, now, -1)?;

    // The prompt's output-artifact instruction is built from the SAME contract
    // that resolve_artifact_contract validates against, so they cannot drift.
    let output_schema = OutputSchemaInfo {
        schema: input.output_contract.schema_type.clone(),
        artifact_name: Some(input.output_contract.artifact_name()),
        confirm_policy: ConfirmPolicy::default(),
        tool_name: input.output_contract.tool_name.clone(),
        description: input.output_contract.description.clone(),
    };

    Ok(PreparedCallRun {
        job_id,
        run_id,
        session_id,
        agent_config,
        selected_model,
        working_dir,
        prompt: input.prompt,
        output_schema,
        execution_id,
        worktree_path,
    })
}

/// Start the backend session for a prepared call run. Split from
/// [`prepare_call_run`] so the call packet is persisted first.
pub(crate) fn start_call_run(
    orch: &Orchestrator,
    prepared: &PreparedCallRun,
) -> Result<(), String> {
    crate::orchestrator::session::start_agent_session(
        orch,
        &prepared.run_id,
        &prepared.prompt,
        &prepared.working_dir,
        crate::backends::SessionStart::New {
            session_id: prepared.session_id.clone(),
        },
        prepared.selected_model.clone(),
        None,
        Some(&prepared.agent_config),
        Some(&prepared.output_schema),
        // A call is a node-less, one-shot prompt->JSON worker: constrain its
        // output to the contract natively so cheap tiers return schema-valid
        // results reliably, captured server-side (CAIRN-2505).
        true,
        false,
        prepared.execution_id.as_deref(),
        None,
    )?;

    // Mark the call's process ACTIVE (CAIRN-2526). A call establishes no DB turn
    // (it is node-less), so unlike an ordinary run it never reaches the
    // `set_current_turn_id -> begin_turn` that flips a normal run's occupancy off
    // `Idle`. Left `Idle`, the Claude reader's `was_host_interrupted` check
    // (which is exactly `occupancy == Idle`) treats every `tool_result` user
    // event as a post-interrupt transport artifact and drops it — so a completed
    // call's transcript kept its `tool_use` rows but stored zero results, and the
    // drill-in tool detail rendered forever "pending". The call IS actively
    // serving its prompt, so `Busy` is the honest occupancy; the handle exists by
    // now (start_agent_session registered it before returning, the same ordering
    // the normal path relies on for `set_current_turn_id`), and no tool_result
    // can arrive before the MCP handshake completes.
    orch.process_state.transition_to_active(&prepared.run_id);
    Ok(())
}

// ============================================================================
// restart_call (CAIRN-2516)
// ============================================================================

/// Restart an undelivered workflow child call.
///
/// Only legal for an in-flight/undelivered call — one whose `workflow_call`
/// journal link still exists. A delivered call (link deleted at finalize) is
/// rejected server-side: the script already consumed its result and re-running
/// would diverge from the journal. The harness awaits the call by its JOB (the
/// latest run's status), so a restart is a fresh RUN under the SAME job, ordered
/// to avoid resolving the parked `?wait` with a premature `null` OR finalizing
/// the job before the new run takes over:
///
/// 1. create a new session+run under the same job (`starting`, newest
///    `created_at` -> the latest run -> `terminal_call_body` sees non-terminal ->
///    the parked await keeps waiting) and seed its prompt;
/// 2. move the journal link from the old run to the new one (delete old, store
///    new), so the OLD run's finalize journals nothing and the NEW run journals
///    on completion;
/// 3. start the new backend session — this creates the new run's turn and makes
///    it live, so a subsequent recompute tracks it;
/// 4. hard-kill the old run. Its link is gone (no journal write) and it is no
///    longer the latest run, so the await ignores it; the job stays live on the
///    new run's running turn rather than briefly finalizing.
pub fn restart_call(orch: &Orchestrator, call_job_id: &str) -> Result<(), String> {
    let owning = run_db({
        let dbs = orch.db.clone();
        let call_job_id = call_job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &call_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    let job = run_db(load_job(
        owning.clone(),
        call_job_id.to_string(),
        "Call job not found",
    ))?;

    if !job_parent_is_workflow(orch, call_job_id)? {
        return Err(format!(
            "Job {call_job_id} is not a workflow child call; refusing restart_call"
        ));
    }

    // Resolve the latest run + its workflow tags; reject if there is none.
    let Some((old_run_id, label, phase)) =
        run_db(latest_run_row(owning.clone(), call_job_id.to_string()))?
    else {
        return Err(format!("Call job {call_job_id} has no run to restart"));
    };

    // Boundary check: only an undelivered call (link still present) is
    // restartable. Validated here, not just hidden in the UI.
    let private = orch.db.local.clone();
    let Some(link) = run_db({
        let private = private.clone();
        let old_run_id = old_run_id.clone();
        async move { crate::workflow_journal::load_call_link(&private, &old_run_id).await }
    })?
    else {
        return Err(format!(
            "Call {call_job_id} is already delivered (no pending journal link); refusing restart"
        ));
    };

    // Reconstruct the spawn inputs from the persisted job + old run.
    let prompt = run_db(seed_user_prompt(owning.clone(), old_run_id.clone()))?
        .ok_or_else(|| format!("Call {call_job_id} has no seed prompt to restart"))?;
    let contract_json = run_db(job_output_contract(owning.clone(), call_job_id.to_string()))?
        .ok_or_else(|| format!("Call job {call_job_id} has no output contract"))?;
    let output_contract: crate::models::DelegatedOutputContract =
        serde_json::from_str(&contract_json)
            .map_err(|e| format!("Failed to parse call output contract: {e}"))?;

    let project_path = run_db(load_project_path(orch.db.clone(), job.project_id.clone()))?;
    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?
        .ok_or_else(|| format!("Could not reconstruct agent config for call {call_job_id}"))?;
    let selected_model = job.model.as_deref().map(Model::new);

    let run_id = ids::mint_child(call_job_id);
    let session_id = ids::mint_session_id().into_string();

    // Worktree: inherit the persisted path, else a fresh scratch dir (a `none`
    // call was created that way).
    let working_dir = match job.worktree_path.clone() {
        Some(path) => path,
        None => {
            let scratch = std::env::temp_dir().join(format!("cairn-call-{run_id}"));
            std::fs::create_dir_all(&scratch)
                .map_err(|e| format!("Failed to create call scratch dir: {e}"))?;
            scratch.to_string_lossy().into_owned()
        }
    };

    let now = chrono::Utc::now().timestamp() as i32;

    // 1. New session+run under the SAME job (starting -> newest -> latest run),
    //    then seed its transcript. No turn yet: the call start path creates it.
    run_db(insert_restart_run_rows(
        owning.clone(),
        call_job_id.to_string(),
        run_id.clone(),
        session_id.clone(),
        job.issue_id.clone(),
        job.project_id.clone(),
        label.clone(),
        phase.clone(),
        now,
    ))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("insert", &run_id, Some(call_job_id)),
    );
    store_user_event(orch, &run_id, &session_id, &prompt, now, -1)?;

    // 2. Move the journal link to the new run BEFORE killing the old run, so the
    //    old run's finalize finds no link and journals nothing.
    run_db({
        let private = private.clone();
        let old_run_id = old_run_id.clone();
        let run_id = run_id.clone();
        let link = link.clone();
        async move {
            crate::workflow_journal::delete_call_link(&private, &old_run_id).await?;
            crate::workflow_journal::store_call_link(&private, &run_id, &link).await
        }
    })?;

    // 3. Start the new backend session (creates the new turn, makes it live).
    let output_schema = crate::models::OutputSchemaInfo {
        schema: output_contract.schema_type.clone(),
        artifact_name: Some(output_contract.artifact_name()),
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: output_contract.tool_name.clone(),
        description: output_contract.description.clone(),
    };
    let prepared = PreparedCallRun {
        job_id: call_job_id.to_string(),
        run_id: run_id.clone(),
        session_id,
        agent_config,
        selected_model,
        working_dir,
        prompt,
        output_schema,
        execution_id: job.execution_id.clone(),
        worktree_path: job.worktree_path.clone(),
    };
    start_call_run(orch, &prepared)?;

    // 4. Hard-kill the old run now that the new one is live. Its link is gone so
    //    it journals nothing, and it is no longer the latest run so the await
    //    ignores it; the job stays live on the new run's turn.
    if let Err(e) =
        crate::orchestrator::lifecycle::kill_session_with_reason(orch, &old_run_id, "user_stop")
    {
        log::warn!("restart_call: failed to kill prior run {old_run_id}: {e}");
    }

    Ok(())
}

/// Whether a job's parent node carries the synthetic `agent_config_id =
/// "workflow"`. Gates [`restart_call`] to genuine workflow child calls.
fn job_parent_is_workflow(orch: &Orchestrator, call_job_id: &str) -> Result<bool, String> {
    let dbs = orch.db.clone();
    let call_job_id = call_job_id.to_string();
    let aid = run_db(async move {
        let owning = crate::execution::routing::owning_db_for_job(&dbs, &call_job_id)
            .await
            .map_err(|e| e.to_string())?;
        owning
            .read(|conn| {
                let call_job_id = call_job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT parent.agent_config_id FROM jobs j \
                             JOIN jobs parent ON j.parent_job_id = parent.id \
                             WHERE j.id = ?1 LIMIT 1",
                            (call_job_id.as_str(),),
                        )
                        .await?;
                    Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
                })
            })
            .await
            .map_err(|e| e.to_string())
    })?;
    Ok(aid.as_deref() == Some("workflow"))
}

/// `(run_id, label, phase)` of the latest run for a job (by `created_at`), or
/// `None` when it has no run.
async fn latest_run_row(
    db: Arc<LocalDb>,
    job_id: String,
) -> Result<Option<(String, Option<String>, Option<String>)>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    // `rowid DESC` tiebreaks the second-resolution `created_at`
                    // so the latest run is deterministic when two runs share a
                    // timestamp (a fast restart), matching the wait path.
                    "SELECT id, label, phase FROM runs \
                     WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?))),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The seed user prompt of a call run (its earliest `user` transcript event).
async fn seed_user_prompt(db: Arc<LocalDb>, run_id: String) -> Result<Option<String>, String> {
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM events \
                     WHERE run_id = ?1 AND event_type = 'user' \
                     ORDER BY sequence ASC, rowid ASC LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let data = row.text(0)?;
            let content = serde_json::from_str::<serde_json::Value>(&data)
                .ok()
                .and_then(|value| {
                    value
                        .get("content")
                        .and_then(|c| c.as_str())
                        .map(str::to_string)
                });
            Ok(content)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The stored `output_contract` JSON for a job, or `None`.
async fn job_output_contract(db: Arc<LocalDb>, job_id: String) -> Result<Option<String>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT output_contract FROM jobs WHERE id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.opt_text(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Insert a fresh session + `starting` run under an EXISTING call job and point
/// the job's `current_session_id` at the new session. The restart analogue of
/// the run/session tail of `insert_child_job_session_run`, minus the job insert.
#[allow(clippy::too_many_arguments)]
async fn insert_restart_run_rows(
    db: Arc<LocalDb>,
    job_id: String,
    run_id: String,
    session_id: String,
    issue_id: Option<String>,
    project_id: String,
    label: Option<String>,
    phase: Option<String>,
    now: i32,
) -> Result<(), String> {
    db.write(move |conn| {
        let job_id = job_id.clone();
        let run_id = run_id.clone();
        let session_id = session_id.clone();
        let issue_id = issue_id.clone();
        let project_id = project_id.clone();
        let label = label.clone();
        let phase = phase.clone();
        Box::pin(async move {
            insert_session_conn(conn, &session_id, Some(job_id.as_str()), None, "claude", now)
                .await?;
            conn.execute(
                "INSERT INTO runs(
                    id, issue_id, project_id, job_id, chat_id, status, session_id,
                    error_message, started_at, exited_at, created_at, updated_at,
                    exit_reason, start_mode, label, phase
                )
                VALUES (?1, ?2, ?3, ?4, NULL, 'starting', ?5, NULL, ?6, NULL, ?6, ?6, NULL, 'fresh', ?7, ?8)",
                params![
                    run_id.as_str(),
                    issue_id.as_deref(),
                    Some(project_id.as_str()),
                    Some(job_id.as_str()),
                    Some(session_id.as_str()),
                    now,
                    label.as_deref(),
                    phase.as_deref(),
                ],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = ?1, updated_at = ?2 WHERE id = ?3",
                params![session_id.as_str(), now, job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod restart_call_tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open(tempfile::tempdir().unwrap().keep().join("restart-call.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    /// Seed a workflow node job `j-wf` and a child call job `j-call` with a live
    /// run `call-run`, but NO `workflow_call` link — i.e. an already-delivered
    /// call. `parent_agent` sets the parent node's `agent_config_id`.
    async fn seed_call(db: &LocalDb, parent_agent: &str) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)".to_string(),
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)".to_string(),
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',3,'T','active',1,1)".to_string(),
            format!("INSERT INTO jobs (id, project_id, issue_id, status, agent_config_id, uri_segment, node_name, created_at, updated_at) VALUES ('j-wf','p','i','running','{parent_agent}','wf','Flow',1,1)"),
            "INSERT INTO jobs (id, project_id, issue_id, parent_job_id, status, agent_config_id, output_contract, uri_segment, node_name, created_at, updated_at) VALUES ('j-call','p','i','j-wf','running','Explore','{\"schemaType\":\"return\"}','call','Explore',1,1)".to_string(),
            "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('sess','j-call',1,1)".to_string(),
            "INSERT INTO runs (id, job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('call-run','j-call','i','p','live',1,1)".to_string(),
        ] {
            db.execute(&sql, ()).await.unwrap();
        }
    }

    /// A call whose `workflow_call` link is gone is delivered; restarting it
    /// would diverge from the journal, so it is rejected at the boundary.
    #[tokio::test]
    async fn delivered_call_restart_is_rejected() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "workflow").await;
        let err = restart_call(&orch, "j-call").expect_err("delivered call must be rejected");
        assert!(
            err.contains("delivered"),
            "expected a delivered-call rejection, got: {err}"
        );
    }

    /// A non-workflow child call is refused before any link lookup.
    #[tokio::test]
    async fn non_workflow_call_restart_is_rejected() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "Build").await;
        let err = restart_call(&orch, "j-call").expect_err("non-workflow call must be rejected");
        assert!(
            err.contains("not a workflow child call"),
            "expected a not-a-workflow-call rejection, got: {err}"
        );
    }

    /// Regression for CAIRN-2526: a call must not be left at the default `Idle`
    /// occupancy while it runs. The Claude reader's `was_host_interrupted` check
    /// is exactly `occupancy == Idle`, so an `Idle` call has every `tool_result`
    /// user event dropped as a post-interrupt transport artifact — which is why
    /// completed calls stored their `tool_use` rows but zero results and the
    /// monitor drill-in tool detail rendered forever "pending". A call establishes
    /// no DB turn, so unlike an ordinary run it never reaches the
    /// `set_current_turn_id -> begin_turn` that flips occupancy off `Idle`;
    /// `start_call_run` therefore marks the process active after spawning. This
    /// pins that contract at the occupancy layer the reader keys on.
    #[tokio::test]
    async fn call_process_marked_active_keeps_reader_from_dropping_tool_results() {
        use crate::agent_process::process::{RunHandle, RunOccupancy};
        let (orch, _db) = orch_with_db().await;
        let run_id = "call-run";
        {
            let mut processes = orch.process_state.processes.lock().unwrap();
            processes.register(
                run_id.to_string(),
                RunHandle::test_handle(Some("sess"), Some("j-call")),
            );
        }
        // A freshly registered process is Idle: the reader would read that as a
        // host interrupt and drop this call's tool_result events.
        assert!(matches!(
            orch.process_state.get_occupancy(run_id),
            Some(RunOccupancy::Idle)
        ));
        // The active-marking start_call_run performs after start_agent_session.
        assert!(orch.process_state.transition_to_active(run_id));
        // No longer Idle -> was_host_interrupted() is false -> tool_results persist.
        assert!(!matches!(
            orch.process_state.get_occupancy(run_id),
            Some(RunOccupancy::Idle)
        ));
    }
}
