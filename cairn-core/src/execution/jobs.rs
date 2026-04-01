//! Job lifecycle functions — start, continue, complete, create child task.
//!
//! All business logic lives here. Host layers (Tauri, cairn-server) provide thin
//! wrappers that handle framework-specific concerns (async spawning, process start).
//!
//! ## Key functions
//!
//! - [`prepare_job`] — DB work + worktree setup, returns [`PreparedJob`] for session spawn.
//! - [`continue_job_impl`] — sends follow-up message to a running/warm job.
//! - [`on_job_complete_impl`] — DAG advancement after a job finishes.
//! - [`create_child_task`] — user-initiated sub-agent under a running job.

use crate::agent_process::stream::TranscriptEvent;
use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_runtime_selection, PresetsConfig,
};
use crate::config::project_settings::load_project_settings;
use crate::config::{self, agents as config_agents, ConfigResult};
use crate::diesel_models::{DbJob, DbRecipeNode, NewEvent, NewJob, NewRun, UpdateJobChangeset};
use crate::execution::advancement::{format_resolved_inputs, resolve_job_inputs};
use crate::execution::dag::{find_downstream_artifact_schema, load_nodes_from_execution};
use crate::execution::step_behavior::resolve_node_behavior;
use crate::models::{
    AgentConfig, AgentSnapshot, ExecutionSnapshot, Job, Model, OutputSchema, OutputSchemaInfo, Run,
    RunStatus,
};
use crate::orchestrator::Orchestrator;
use crate::runs::queries::db_run_to_run;
use crate::schema::{events, executions, issues, jobs, projects, runs};
use crate::sync::SyncMessage;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

// ============================================================================
// Public types
// ============================================================================

/// Input for creating a user-initiated child task.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChildTaskInput {
    pub parent_job_id: String,
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    #[serde(alias = "model")]
    pub tier: Option<String>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
}

/// Result of creating a child task.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChildTaskResult {
    pub job_id: String,
    pub run_id: String,
}

/// Everything needed by the host layer to spawn a Claude process for a job.
///
/// Returned by [`prepare_job`] after all DB work, worktree setup, run creation,
/// and initial user-event storage are complete.
pub struct PreparedJob {
    pub run_id: String,
    pub session_id: String,
    pub session_start: crate::backends::SessionStart,
    pub prompt: String,
    pub worktree_path: String,
    pub job_model: Option<Model>,
    pub agent_config: Option<AgentConfig>,
    pub artifact_schema_info: Option<OutputSchemaInfo>,
    pub execution_id: Option<String>,
    pub turn_id: String,
}

// ============================================================================
// on_job_complete_impl
// ============================================================================

/// Called when a job finishes. Advances the execution DAG if applicable.
///
/// Only advances for jobs that are part of a recipe DAG (have both `execution_id`
/// and `recipe_node_id`). Manager jobs have `execution_id` for config storage
/// but no `recipe_node_id`, so they skip DAG advancement and worktree cleanup.
pub async fn on_job_complete_impl(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    let (execution_id, recipe_node_id): (Option<String>, Option<String>) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        jobs::table
            .find(job_id)
            .select((jobs::execution_id, jobs::recipe_node_id))
            .first(&mut *conn)
            .map_err(|e| format!("Job not found: {}", e))?
    };

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
    // ---- Load job -------------------------------------------------------
    let job: DbJob = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        jobs::table
            .find(job_id)
            .first(&mut *conn)
            .map_err(|e| format!("Job not found: {}", e))?
    };

    // ---- Load project info ----------------------------------------------
    let (repo_path, project_key): (String, String) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        projects::table
            .find(&job.project_id)
            .select((projects::repo_path, projects::key))
            .first(&mut *conn)
            .map_err(|e| format!("Project not found: {}", e))?
    };

    // ---- Execution seq (for job-activated event) ------------------------
    let exec_seq: Option<i32> = if let Some(exec_id) = &job.execution_id {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        executions::table
            .filter(executions::id.eq(exec_id))
            .select(executions::seq)
            .first(&mut *conn)
            .ok()
            .flatten()
    } else {
        None
    };

    // ---- Display ID (issue number or sequential run counter) ------------
    let display_id: String = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        if let Some(iid) = &job.issue_id {
            let num: i32 = issues::table
                .find(iid)
                .select(issues::number)
                .first(&mut *conn)
                .unwrap_or(0);
            num.to_string()
        } else {
            let run_count: i64 = executions::table
                .filter(executions::project_id.eq(&job.project_id))
                .filter(executions::issue_id.is_null())
                .count()
                .get_result(&mut *conn)
                .unwrap_or(0);
            format!("run{}", run_count)
        }
    };

    // ---- Determine node behavior ----------------------------------------
    let (needs_worktree, inherits_worktree, step_name): (bool, bool, String) =
        if let Some(node_id) = &job.recipe_node_id {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;

            let execution_id = job
                .execution_id
                .as_ref()
                .ok_or("Job has recipe node but no execution_id")?;

            let all_nodes = load_nodes_from_execution(&mut conn, execution_id)?;
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
            if node.node_type == "executor" {
                return Err(
                    "Executor nodes expand during DAG advancement, not session start".to_string(),
                );
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
                let mut conn = orch
                    .db
                    .conn
                    .lock()
                    .map_err(|e| format!("Failed to lock database: {}", e))?;
                let default_branch: String = projects::table
                    .find(&job.project_id)
                    .select(projects::default_branch)
                    .first::<Option<String>>(&mut *conn)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| "main".to_string());
                branch != &default_branch
            } else {
                true
            };
            (needs_wt, false, "standalone".to_string())
        };

    // ---- Worktree setup -------------------------------------------------
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
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            let parent: DbJob = jobs::table
                .find(parent_job_id)
                .first(&mut *conn)
                .map_err(|e| format!("Parent job not found: {}", e))?;
            (
                parent
                    .worktree_path
                    .ok_or("Parent job has no worktree - cannot inherit")?,
                parent.branch,
            )
        };

        let now = chrono::Utc::now().timestamp() as i32;
        {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            let changeset = UpdateJobChangeset {
                worktree_path: Some(Some(&parent_worktree)),
                branch: Some(parent_branch.as_deref()),
                updated_at: Some(now),
                ..Default::default()
            };
            diesel::update(jobs::table.find(job_id))
                .set(&changeset)
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to update job: {}", e))?;
        }

        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "jobs", "action": "update"}),
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
        let seq: i64 = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            if let Some(iid) = &job.issue_id {
                jobs::table
                    .filter(jobs::issue_id.eq(iid))
                    .filter(jobs::branch.is_not_null())
                    .count()
                    .get_result(&mut *conn)
                    .unwrap_or(0)
            } else if let Some(exec_id) = &job.execution_id {
                jobs::table
                    .filter(jobs::execution_id.eq(exec_id))
                    .filter(jobs::branch.is_not_null())
                    .count()
                    .get_result(&mut *conn)
                    .unwrap_or(0)
            } else {
                0
            }
        };

        let safe_step_name: String = step_name
            .to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .to_string();

        let (branch, wt_dir) = if let Some(ref existing) = job.branch {
            // Job already has a branch (e.g., feature-branch manager) — use it
            let dir = existing.replace('/', "-");
            (existing.clone(), dir)
        } else {
            let name = format!("{}-{}-{}-{}", project_key, display_id, safe_step_name, seq);
            (format!("agent/{}", name), name)
        };

        let wt_path = dirs::home_dir()
            .ok_or("Could not find home directory")?
            .join(".cairn")
            .join("worktrees")
            .join(&wt_dir);

        let base_ref = job.base_branch.as_deref().unwrap_or("HEAD");
        prepare_worktree_for_job(orch, &repo_path, &wt_path, &branch, base_ref)?;

        let wt_path_str = wt_path.to_string_lossy().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            let changeset = UpdateJobChangeset {
                worktree_path: Some(Some(&wt_path_str)),
                branch: Some(Some(&branch)),
                updated_at: Some(now),
                ..Default::default()
            };
            diesel::update(jobs::table.find(job_id))
                .set(&changeset)
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to update job: {}", e))?;
        }

        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "jobs", "action": "update"}),
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
    let job: DbJob = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        jobs::table
            .find(job_id)
            .first(&mut *conn)
            .map_err(|e| format!("Job not found after worktree setup: {}", e))?
    };

    // ---- Agent config ---------------------------------------------------
    let project_path: Option<PathBuf> = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        projects::table
            .find(&job.project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(PathBuf::from)
    };

    let agent_config = load_agent_config(orch, &job, project_path.as_deref())?;

    // ---- Create session + run record --------------------------------------
    let run_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    let status_str = RunStatus::Starting.to_string();

    // Ensure a Session record exists for this job and derive the correct startup mode.
    let (session_id, session_start, run_start_mode) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let session_id = if let Some(sid) = job.current_session_id.as_deref() {
            sid.to_string()
        } else {
            let backend = resolve_backend_name(orch, &job);
            let session = crate::sessions::queries::create_for_job(&mut conn, job_id, &backend)?;
            diesel::update(jobs::table.find(job_id))
                .set((
                    jobs::current_session_id.eq(Some(&session.id)),
                    jobs::updated_at.eq(now),
                ))
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to update job session: {}", e))?;
            session.id
        };

        let session = crate::sessions::queries::get(&mut conn, &session_id)?;
        let job_backend = resolve_backend_name(orch, &job);
        let (session_start, run_start_mode) = if let Some(backend_id) = session.backend_id.clone() {
            (
                crate::backends::SessionStart::Resume {
                    session_id: session.id.clone(),
                    backend_id,
                },
                "resume",
            )
        } else if let Some(parent_session_id) = session.parent_session_id.as_deref() {
            let parent_session = crate::sessions::queries::get(&mut conn, parent_session_id)?;
            if parent_session.backend == job_backend {
                if let Some(source_backend_id) = parent_session.backend_id {
                    (
                        crate::backends::SessionStart::Fork {
                            session_id: session.id.clone(),
                            source_backend_id,
                        },
                        "fork",
                    )
                } else {
                    (
                        crate::backends::SessionStart::Resume {
                            session_id: session.id.clone(),
                            backend_id: session.id.clone(),
                        },
                        "resume",
                    )
                }
            } else {
                (
                    crate::backends::SessionStart::New {
                        session_id: session.id.clone(),
                    },
                    "fresh",
                )
            }
        } else {
            (
                crate::backends::SessionStart::New {
                    session_id: session.id.clone(),
                },
                "fresh",
            )
        };

        (session_id, session_start, run_start_mode)
    };

    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let existing_active: Vec<String> = runs::table
            .filter(runs::job_id.eq(job_id))
            .filter(runs::status.eq_any(&["starting", "live"]))
            .select(runs::id)
            .load(&mut *conn)
            .unwrap_or_default();

        if !existing_active.is_empty() {
            log::warn!(
                "[prepare_job] Job {} already has {} active runs",
                job_id,
                existing_active.len()
            );
        }

        let new_run = NewRun {
            id: &run_id,
            issue_id: job.issue_id.as_deref(),
            project_id: Some(&job.project_id),
            job_id: Some(job_id),
            chat_id: None,
            status: Some(&status_str),
            session_id: Some(&session_id),
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: Some(run_start_mode),
        };

        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to create run: {}", e))?;
    }

    orch.sync(SyncMessage::Run(crate::sync::SyncRun {
        id: run_id.clone(),
        job_id: Some(job_id.to_string()),
        issue_id: job.issue_id.clone(),
        status: Some(status_str.clone()),
        backend: None,
        exit_reason: None,
        error_message: None,
        started_at: Some(now as i64),
        exited_at: None,
        created_at: Some(now as i64),
    }));
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "insert"}),
    );

    // ---- Create initial turn ------------------------------------------------
    let turn_id = uuid::Uuid::new_v4().to_string();
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        crate::turns::queries::create_initial_turn(
            &mut conn,
            &turn_id,
            &session_id,
            job_id,
            &*orch.services.emitter,
        )?;
    }

    // ---- Resolve inputs + build prompt ----------------------------------
    let (resolved_inputs, artifact_schema_info) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        let inputs = resolve_job_inputs(&mut conn, &job)?;
        let schema_info =
            if let (Some(node_id), Some(execution_id)) = (&job.recipe_node_id, &job.execution_id) {
                find_downstream_artifact_schema(&mut conn, node_id, execution_id)?
            } else {
                None
            };
        (inputs, schema_info)
    };

    let prompt = format_resolved_inputs(&resolved_inputs);

    // If this is a manager job, prepend manager context to the prompt
    let (prompt, artifact_schema_info, is_manager) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        if let Some(manager) =
            crate::managers::identity::lookup_manager_actor_by_job(&mut conn, job_id)?
        {
            let wt = job
                .worktree_path
                .as_ref()
                .map(PathBuf::from)
                .or_else(|| project_path.clone());
            let context = crate::managers::context::build_manager_context(
                &mut conn,
                &manager,
                project_path.as_deref(),
                wt.as_deref(),
            )?;
            // Seed context cache so warm continues can detect changes
            if let Ok(mut cache) = orch.process_state.last_manager_context.lock() {
                cache.insert(manager.id.clone(), context.clone());
            }
            let prompt = if prompt.is_empty() {
                context
            } else {
                format!("{}\n\n---\n\n{}", context, prompt)
            };
            (prompt, artifact_schema_info, true)
        } else {
            (prompt, artifact_schema_info, false)
        }
    };

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
    // Manager jobs skip this — wake_manager() stores the event with the
    // actual wake message instead of the full prompt (which contains context).
    if !is_manager {
        store_user_event_with_turn(orch, &run_id, &session_id, &prompt, now, -1, Some(&turn_id))?;
    }

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

/// Continue an existing job with an optional follow-up message.
///
/// Reuses a warm process if one exists for the job's session, otherwise starts
/// a new Claude process with `--resume`.
pub fn continue_job_impl(
    orch: &Orchestrator,
    job_id: &str,
    message: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<Run, String> {
    // ---- Load job -------------------------------------------------------
    let (job, project_id, issue_id, project_path): (
        DbJob,
        String,
        Option<String>,
        Option<PathBuf>,
    ) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let job: DbJob = jobs::table
            .find(job_id)
            .first(&mut *conn)
            .map_err(|e| format!("Job not found: {}", e))?;

        let issue_id = job.issue_id.clone();
        let project_id = job.project_id.clone();
        let project_path = projects::table
            .find(&project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(PathBuf::from);

        (job, project_id, issue_id, project_path)
    };

    // ---- Transition job to Running if in terminal state -------------------
    {
        let current_status: String = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            jobs::table
                .find(job_id)
                .select(jobs::status)
                .first(&mut *conn)
                .map_err(|e| format!("Job not found: {}", e))?
        };
        if current_status != "running" {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            if let Err(e) = crate::transitions::transition_job(
                &mut conn,
                &*orch.services.emitter,
                job_id,
                crate::models::JobStatus::Running,
                &orch.trigger_events,
            ) {
                log::warn!("Failed to transition job {} to running: {}", job_id, e);
            }
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

    let current_session_id = job
        .current_session_id
        .as_ref()
        .ok_or("Job has no current session to resume")?;

    let now = chrono::Utc::now().timestamp() as i32;

    // ---- Session identity check -----------------------------------------
    // Load Session, check health, rotate if necessary.
    let (session_id, session_start, run_start_mode) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        match crate::sessions::queries::get(&mut conn, current_session_id) {
            Ok(session) => {
                use crate::models::SessionStatus;
                match session.status {
                    SessionStatus::Open => {
                        let session_id = session.id.clone();
                        if session.backend_id.is_none() {
                            if let Some(parent_session_id) = session.parent_session_id.as_deref() {
                                if let Ok(parent_session) =
                                    crate::sessions::queries::get(&mut conn, parent_session_id)
                                {
                                    let job_backend = resolve_backend_name(orch, &job);
                                    if parent_session.backend == job_backend {
                                        if let Some(source_backend_id) = parent_session.backend_id {
                                            (
                                                session_id.clone(),
                                                crate::backends::SessionStart::Fork {
                                                    session_id,
                                                    source_backend_id,
                                                },
                                                "fork",
                                            )
                                        } else {
                                            (
                                                session_id.clone(),
                                                crate::backends::SessionStart::Resume {
                                                    session_id: session_id.clone(),
                                                    backend_id: session_id.clone(),
                                                },
                                                "resume",
                                            )
                                        }
                                    } else {
                                        (
                                            session_id.clone(),
                                            crate::backends::SessionStart::New {
                                                session_id: session_id.clone(),
                                            },
                                            "fresh",
                                        )
                                    }
                                } else {
                                    (
                                        session_id.clone(),
                                        crate::backends::SessionStart::Resume {
                                            session_id: session_id.clone(),
                                            backend_id: session_id.clone(),
                                        },
                                        "resume",
                                    )
                                }
                            } else {
                                (
                                    session_id.clone(),
                                    crate::backends::SessionStart::Resume {
                                        session_id: session_id.clone(),
                                        backend_id: session_id.clone(),
                                    },
                                    "resume",
                                )
                            }
                        } else {
                            let backend_id = session.backend_id.unwrap();
                            (
                                session_id.clone(),
                                crate::backends::SessionStart::Resume {
                                    session_id,
                                    backend_id,
                                },
                                "resume",
                            )
                        }
                    }
                    SessionStatus::Failed | SessionStatus::Closed => {
                        // Session is bad or intentionally ended — rotate
                        let new_session = crate::sessions::queries::rotate_job_session(
                            &mut conn, &session, job_id,
                        )?;
                        log::info!(
                            "Session {} was {:?}, rotated to {} (seq {})",
                            &session.id[..session.id.len().min(8)],
                            session.status,
                            &new_session.id[..new_session.id.len().min(8)],
                            new_session.sequence
                        );
                        (
                            new_session.id.clone(),
                            crate::backends::SessionStart::New {
                                session_id: new_session.id,
                            },
                            "fresh",
                        )
                    }
                }
            }
            Err(_) => {
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
                    "resume",
                )
            }
        }
    };

    // ---- Find or create run ---------------------------------------------
    let existing_run_id = orch.process_state.find_process_by_session(&session_id);
    let (run_id, is_process_reuse) = if let Some(existing_id) = existing_run_id {
        log::info!(
            "Found existing process for session {}, reusing run {}",
            &session_id[..session_id.len().min(8)],
            &existing_id[..existing_id.len().min(8)]
        );
        (existing_id, true)
    } else {
        let new_run_id = Uuid::new_v4().to_string();
        let status_str = RunStatus::Starting.to_string();
        {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            let new_run = NewRun {
                id: &new_run_id,
                issue_id: issue_id.as_deref(),
                project_id: Some(&project_id),
                job_id: Some(job_id),
                chat_id: None,
                status: Some(&status_str),
                session_id: Some(&session_id),
                error_message: None,
                started_at: None,
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some(run_start_mode),
            };
            diesel::insert_into(runs::table)
                .values(&new_run)
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to create run: {}", e))?;
        }
        orch.sync(SyncMessage::Run(crate::sync::SyncRun {
            id: new_run_id.clone(),
            job_id: Some(job_id.to_string()),
            issue_id: issue_id.clone(),
            status: Some(status_str),
            backend: None,
            exit_reason: None,
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at: Some(now as i64),
        }));
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "runs", "action": "insert"}),
        );
        (new_run_id, false)
    };

    // ---- Create successor turn for follow-up ----------------------------
    let turn_id = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        // Find the head turn for this job to use as predecessor
        let head_turn = crate::turns::queries::get_head_turn(&mut conn, job_id)?;

        let new_turn_id = uuid::Uuid::new_v4().to_string();
        let start_reason = crate::models::TurnStartReason::FollowUp;

        if let Some(head) = head_turn {
            if head.state == crate::models::TurnState::Pending {
                head.id
            } else {
                match crate::turns::queries::create_successor_turn(
                    &mut conn,
                    &new_turn_id,
                    &session_id,
                    job_id,
                    &head.id,
                    start_reason,
                    &*orch.services.emitter,
                ) {
                    Ok(_) => new_turn_id,
                    Err(e) => {
                        log::warn!("Failed to create successor turn: {}", e);
                        // Fallback: create initial turn if successor failed
                        let fallback_id = uuid::Uuid::new_v4().to_string();
                        crate::turns::queries::create_initial_turn(
                            &mut conn,
                            &fallback_id,
                            &session_id,
                            job_id,
                            &*orch.services.emitter,
                        )?;
                        fallback_id
                    }
                }
            }
        } else {
            // No prior turn exists — create initial
            crate::turns::queries::create_initial_turn(
                &mut conn,
                &new_turn_id,
                &session_id,
                job_id,
                &*orch.services.emitter,
            )?;
            new_turn_id
        }
    };

    // ---- Artifact schema ------------------------------------------------
    let artifact_schema_info: Option<OutputSchemaInfo> = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        if let (Some(node_id), Some(execution_id)) = (&job.recipe_node_id, &job.execution_id) {
            find_downstream_artifact_schema(&mut conn, node_id, execution_id)?
        } else {
            None
        }
    };

    let user_message = message
        .unwrap_or("Continue where you left off.")
        .to_string();
    let base_prompt = resolve_skill_slash_command(orch, &user_message, project_path.as_deref());

    // If this is a manager job, prepend manager context
    let (prompt, artifact_schema_info) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        if let Some(manager) =
            crate::managers::identity::lookup_manager_actor_by_job(&mut conn, job_id)?
        {
            let wt = job
                .worktree_path
                .as_ref()
                .map(PathBuf::from)
                .or_else(|| project_path.clone());
            let context = crate::managers::context::build_manager_context(
                &mut conn,
                &manager,
                project_path.as_deref(),
                wt.as_deref(),
            )?;
            // Seed context cache so warm continues can detect changes
            if let Ok(mut cache) = orch.process_state.last_manager_context.lock() {
                cache.insert(manager.id.clone(), context.clone());
            }
            let prompt = format!("{}\n\n---\n\n{}", context, base_prompt);
            (prompt, artifact_schema_info)
        } else {
            (base_prompt.clone(), artifact_schema_info)
        }
    };

    let job_model = job.model.as_ref().map(Model::new);

    // ---- Store user event -----------------------------------------------
    // Store with base_prompt (user's message) not prompt (which includes
    // manager context) so the UI displays the actual user message.
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let max_seq: Option<i32> = events::table
            .filter(events::session_id.eq(&session_id))
            .select(diesel::dsl::max(events::sequence))
            .first(&mut *conn)
            .unwrap_or(None);
        let next_seq = max_seq.unwrap_or(-1) + 1;

        let event_id = Uuid::new_v4().to_string();
        let transcript_event = TranscriptEvent {
            event_type: "user".to_string(),
            session_id: Some(session_id.clone()),
            parent_tool_use_id: None,
            content: Some(user_message.clone()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            usage: None,
            raw: None,
        };
        let event_data = serde_json::to_string(&transcript_event).unwrap_or_default();

        let new_event = NewEvent {
            id: &event_id,
            run_id: &run_id,
            session_id: Some(&session_id),
            sequence: next_seq,
            timestamp: now,
            event_type: "user",
            data: &event_data,
            parent_tool_use_id: None,
            created_at: now,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            turn_id: Some(&turn_id),
        };
        let _ = diesel::insert_into(events::table)
            .values(&new_event)
            .execute(&mut *conn);

        // Sync event to cloud
        orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
            id: event_id.clone(),
            run_id: run_id.to_string(),
            session_id: Some(session_id.to_string()),
            sequence: Some(next_seq),
            event_type: "user".to_string(),
            data: Some(event_data.clone()),
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            created_at: Some(now as i64),
            turn_id: Some(turn_id.clone()),
        }));

        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "events", "action": "insert"}),
        );
    }

    // ---- Warm process or new session ------------------------------------
    if is_process_reuse {
        crate::backends::stdin::send_user_message(
            &orch.process_state,
            &run_id,
            &prompt,
            &session_id,
            None,
            Some(&worktree_path),
        )?;

        // Run stays Live — no durable status change for warm reuse.
        // Process occupancy changes to ServingTurn via begin_turn.
        orch.process_state.transition_to_active(&run_id);
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
    }

    // ---- Start the successor turn and attach to process -------------------
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        let _ =
            crate::transitions::start_turn(&mut conn, &turn_id, &run_id, &*orch.services.emitter);
    }
    orch.process_state
        .set_current_turn_id(&run_id, Some(&turn_id));

    // ---- Return run -----------------------------------------------------
    let db_run = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        runs::table
            .find(&run_id)
            .first(&mut *conn)
            .map_err(|e| format!("Run not found after creation: {}", e))?
    };
    Ok(db_run_to_run(db_run))
}

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
    let (parent_job, project_id, issue_id, execution_id): (
        DbJob,
        String,
        Option<String>,
        Option<String>,
    ) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        let parent: DbJob = jobs::table
            .find(&input.parent_job_id)
            .first(&mut *conn)
            .map_err(|e| format!("Parent job not found: {}", e))?;

        if parent.worktree_path.is_none() {
            return Err("Parent job has no worktree - cannot spawn child task".to_string());
        }

        let proj = parent.project_id.clone();
        let iss = parent.issue_id.clone();
        let exec = parent.execution_id.clone();
        (parent, proj, iss, exec)
    };

    let worktree_path = parent_job
        .worktree_path
        .as_ref()
        .ok_or("Parent job has no worktree")?
        .clone();

    let project_path: Option<PathBuf> = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        projects::table
            .find(&project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(PathBuf::from)
    };

    // ---- Load agent config from files -----------------------------------
    let config_dir = config::get_config_dir()?;
    let agent_config: AgentConfig = {
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
            approval_policy: file_agent.approval_policy,
            filesystem_scope: file_agent.filesystem_scope,
            backend_preference: file_agent.backend_preference,
        }
    };

    // ---- Create job + run -----------------------------------------------
    let job_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let session_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let presets = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        load_execution_presets(
            &mut conn,
            execution_id.as_deref(),
            &config_dir,
            project_path.as_deref(),
        )?
    };
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
    let (selected_model, _selected_backend, _selected_extras) =
        resolve_runtime_selection(authored_tier, authored_backend, &presets)?;
    let selected_model = Some(selected_model);

    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let model_str = selected_model.as_ref().map(|m| m.to_string());
        let new_job = NewJob {
            id: &job_id,
            execution_id: execution_id.as_deref(),
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: Some(&input.parent_job_id),
            worktree_path: Some(&worktree_path),
            branch: None,
            base_commit: None,
            current_session_id: Some(&session_id),
            resume_session_id: None,
            status: "running",
            agent_config_id: Some(&agent_config.id),
            issue_id: issue_id.as_deref(),
            project_id: &project_id,
            task_description: Some(&input.description),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: model_str.as_deref(),
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };

        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to create child job: {}", e))?;

        // Create Session record (after Job exists to satisfy FK)
        crate::sessions::queries::create_with_id(
            &mut conn,
            &session_id,
            Some(&job_id),
            None,
            "claude",
        )?;

        let run_status = RunStatus::Starting.to_string();
        let new_run = NewRun {
            id: &run_id,
            issue_id: issue_id.as_deref(),
            project_id: Some(&project_id),
            job_id: Some(&job_id),
            chat_id: None,
            status: Some(&run_status),
            session_id: Some(&session_id),
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: Some("fresh"),
        };

        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to create run: {}", e))?;
    }

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
        backend: None,
        exit_reason: None,
        error_message: None,
        started_at: Some(now as i64),
        exited_at: None,
        created_at: Some(now as i64),
    }));

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "insert"}),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "insert"}),
    );

    // ---- Store user event -----------------------------------------------
    store_user_event(orch, &run_id, &session_id, &input.prompt, now, -1)?;

    // ---- Output schema --------------------------------------------------
    let output_schema = OutputSchemaInfo {
        schema: OutputSchema::Preset("return".to_string()),
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

// ============================================================================
// Private helpers
// ============================================================================

/// Create a worktree for a job using the orchestrator's service traits.
pub(crate) fn prepare_worktree_for_job(
    orch: &Orchestrator,
    repo_path: &str,
    worktree_path: &Path,
    branch: &str,
    base_ref: &str,
) -> Result<(), String> {
    let settings = load_project_settings(Path::new(repo_path));
    let should_seed_worktree_ignored = settings.should_seed_worktree_ignored();
    let setup_commands = settings.setup_commands.unwrap_or_default();

    let git = &*orch.services.git;
    let fs = &*orch.services.fs;
    let process = &*orch.services.process;
    let repo = Path::new(repo_path);

    crate::git::worktree::create_worktree_with_services(
        git,
        fs,
        repo,
        worktree_path,
        branch,
        base_ref,
    )?;

    if should_seed_worktree_ignored {
        // Seed gitignored content: symlink dirs, copy files (best-effort)
        match crate::git::worktree::seed_worktree_ignored(git, fs, repo, worktree_path) {
            Ok(result) => log::info!(
                "Seeded worktree: {} entries linked/copied, {} skipped, {} failed",
                result.seeded,
                result.skipped,
                result.failed
            ),
            Err(e) => log::warn!("Worktree seeding failed (continuing): {}", e),
        }
    } else {
        log::info!("Skipping worktree ignored-content seeding (worktree.seedIgnored=false)");
    }

    if !setup_commands.is_empty() {
        if let Err(e) = crate::git::worktree::run_setup_commands_with_process(
            process,
            worktree_path,
            &setup_commands,
        ) {
            log::error!("Setup commands failed, cleaning up worktree: {}", e);
            let _ = crate::git::worktree::remove_worktree_with_services(
                git,
                fs,
                repo,
                worktree_path,
                true,
            );
            return Err(e);
        }
    }

    Ok(())
}

/// Load agent config for a job — tries execution snapshot first, falls back to config files.
/// Resolve the backend name for a job based on its model string.
fn resolve_backend_name(_orch: &Orchestrator, job: &DbJob) -> String {
    job.model
        .as_deref()
        .and_then(crate::backends::backend_for_model)
        .unwrap_or("claude")
        .to_string()
}

fn load_agent_config(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: Option<&Path>,
) -> Result<Option<AgentConfig>, String> {
    let Some(aid) = &job.agent_config_id else {
        return Ok(None);
    };

    // Try snapshot first (ensures reproducibility for execution-based jobs)
    let mut snapshot_tier_override: Option<Model> = None;
    let mut snapshot_backend_preference: Option<String> = None;
    let mut snapshot_presets: Option<PresetsConfig> = None;
    if let Some(exec_id) = &job.execution_id {
        log::info!(
            "load_agent_config: job {} has execution_id={}, trying snapshot for agent '{}'",
            job.id,
            exec_id,
            aid
        );
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        snapshot_presets = load_snapshot_presets(&mut conn, exec_id)?;
        if let Some(agent) = load_agent_from_snapshot(&mut conn, exec_id, aid)? {
            if !agent.prompt.is_empty() {
                log::info!(
                    "load_agent_config: loaded agent '{}' from snapshot (prompt len={}, tools={:?})",
                    aid, agent.prompt.len(), agent.tools
                );
                return Ok(Some(agent));
            }
            // Snapshot agent has empty prompt — likely a placeholder from executor
            // expansion. Fall through to config files, but remember overrides.
            snapshot_tier_override = agent.tier.clone();
            snapshot_backend_preference = agent.backend_preference.clone();
            log::info!(
                "load_agent_config: snapshot agent '{}' has empty prompt, falling through to config files",
                aid
            );
        } else {
            log::info!(
                "load_agent_config: agent '{}' not found in snapshot for execution {}",
                aid,
                exec_id
            );
        }
    } else {
        log::info!(
            "load_agent_config: job {} has no execution_id, using file-based config for '{}'",
            job.id,
            aid
        );
    }

    // Fall back to config files
    let project_id = &job.project_id;
    let agent = config::get_config_dir().ok().and_then(|cd| {
        let presets = snapshot_presets
            .clone()
            .unwrap_or_else(|| load_effective_presets(&cd, project_path));
        config_agents::get_agent(&cd, aid, project_path)
            .ok()
            .flatten()
            .and_then(|mut fa| {
                if snapshot_backend_preference.is_some() {
                    fa.backend_preference = snapshot_backend_preference.clone();
                }
                let snapshot = resolve_agent_snapshot(
                    &fa,
                    snapshot_tier_override.as_ref().map(|m| m.as_str()),
                    &presets,
                )
                .ok()?;

                Some(AgentConfig {
                    id: snapshot.id,
                    name: snapshot.name,
                    description: snapshot.description,
                    prompt: snapshot.prompt,
                    tools: snapshot.tools,
                    tier: snapshot.tier,
                    workspace_id: if fa.is_project_scoped {
                        None
                    } else {
                        Some("workspace".to_string())
                    },
                    project_id: if fa.is_project_scoped {
                        Some(project_id.clone())
                    } else {
                        None
                    },
                    created_at: 0,
                    updated_at: 0,
                    disallowed_tools: snapshot.disallowed_tools,
                    skills: snapshot.skills,
                    approval_policy: snapshot.approval_policy,
                    filesystem_scope: snapshot.filesystem_scope,
                    backend_preference: snapshot.backend_preference,
                })
            })
    });

    Ok(agent)
}

/// Load an agent config from the execution snapshot (avoids file-level config drift).
fn load_agent_from_snapshot(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    agent_config_id: &str,
) -> Result<Option<AgentConfig>, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load execution: {}", e))?
        .flatten();

    let Some(json) = snapshot_json else {
        return Ok(None);
    };

    let snapshot: ExecutionSnapshot =
        serde_json::from_str(&json).map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    snapshot
        .agents
        .get(agent_config_id)
        .map(|agent: &AgentSnapshot| {
            Ok(AgentConfig {
                id: agent.id.clone(),
                name: agent.name.clone(),
                description: agent.description.clone(),
                prompt: agent.prompt.clone(),
                tools: agent.tools.clone(),
                tier: agent.tier.clone().or(agent.model.clone()),
                workspace_id: None,
                project_id: None,
                created_at: snapshot.created_at as i32,
                updated_at: snapshot.created_at as i32,
                disallowed_tools: agent.disallowed_tools.clone(),
                skills: agent.skills.clone(),
                approval_policy: agent.approval_policy,
                filesystem_scope: agent.filesystem_scope,
                backend_preference: agent.backend_preference.clone(),
            })
        })
        .transpose()
}

fn load_snapshot_presets(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Option<PresetsConfig>, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load execution: {}", e))?
        .flatten();

    let Some(json) = snapshot_json else {
        return Ok(None);
    };

    let snapshot: ExecutionSnapshot =
        serde_json::from_str(&json).map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    Ok(snapshot.presets.as_ref().map(PresetsConfig::from))
}

fn load_execution_presets(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: Option<&str>,
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<PresetsConfig, String> {
    if let Some(exec_id) = execution_id {
        if let Some(presets) = load_snapshot_presets(conn, exec_id)? {
            return Ok(presets);
        }
    }

    Ok(load_effective_presets(config_dir, project_path))
}

/// Scan `message` for `/skill-id` tokens, prepend matched skill content,
/// and return the full original message unchanged after the skill blocks.
pub fn resolve_skill_slash_command(
    orch: &Orchestrator,
    message: &str,
    project_path: Option<&std::path::Path>,
) -> String {
    let mut skill_blocks: Vec<String> = Vec::new();

    for word in message.split_whitespace() {
        if !word.starts_with('/') {
            continue;
        }
        let id = &word[1..];
        if id.is_empty() || id == "compact" {
            continue;
        }
        if let Ok(Some(skill)) =
            crate::config::skills::get_skill(&orch.config_dir, id, project_path)
        {
            skill_blocks.push(format!(
                "<skill name=\"{}\">\n{}\n</skill>",
                skill.name, skill.prompt
            ));
        }
    }

    if skill_blocks.is_empty() {
        return message.to_string();
    }

    format!("{}\n\n{}", skill_blocks.join("\n\n"), message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::NewJob;
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::{create_test_project, test_diesel_conn};
    use std::sync::{Arc, Mutex};

    fn test_orchestrator(conn: diesel::sqlite::SqliteConnection) -> Orchestrator {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));
        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(crate::agent_process::process::AgentProcessState::default()),
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    fn create_standalone_job(
        conn: &mut diesel::sqlite::SqliteConnection,
        project_id: &str,
        branch: Option<&str>,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        let new_job = NewJob {
            id: &id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "running",
            agent_config_id: None,
            issue_id: None,
            project_id,
            task_description: Some("Test standalone job"),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };

        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .expect("Failed to create test job");

        id
    }

    #[test]
    fn test_prepare_job_default_branch_manager_skips_worktree() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let job_id = create_standalone_job(&mut conn, &project_id, Some("main"));

        let orch = test_orchestrator(conn);
        let result = prepare_job(&orch, &job_id).unwrap();

        // Should use project repo_path directly, not a worktree
        assert_eq!(result.worktree_path, "/tmp/test-repo");

        // Job in DB should NOT have worktree_path set
        let mut conn = orch.db.conn.lock().unwrap();
        let db_job: DbJob = jobs::table.find(&job_id).first(&mut *conn).unwrap();
        assert!(
            db_job.worktree_path.is_none(),
            "Expected no worktree_path for default-branch manager"
        );
        // Branch should remain as-is
        assert_eq!(db_job.branch.as_deref(), Some("main"));
    }

    #[test]
    fn test_prepare_job_no_branch_standalone_needs_worktree() {
        // A standalone job with no pre-set branch should try to create a worktree.
        // Mock services will panic, confirming the worktree creation path is entered.
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let job_id = create_standalone_job(&mut conn, &project_id, None);

        let orch = test_orchestrator(conn);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| prepare_job(&orch, &job_id)));

        assert!(
            result.is_err(),
            "Expected panic from mock filesystem when attempting worktree creation"
        );
    }

    #[test]
    fn test_prepare_job_feature_branch_standalone_needs_worktree() {
        // A standalone job with a branch that differs from the default should
        // try to create a worktree.
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let job_id = create_standalone_job(&mut conn, &project_id, Some("mgr/feature"));

        let orch = test_orchestrator(conn);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| prepare_job(&orch, &job_id)));

        assert!(
            result.is_err(),
            "Expected panic from mock filesystem when attempting worktree creation"
        );
    }
}

/// Store a user event in the transcript.
pub(crate) fn store_user_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    sequence: i32,
) -> Result<(), String> {
    // Look up current turn_id from process state (may be None if called before process spawn)
    let current_turn = orch.process_state.get_current_turn_id(run_id);
    store_user_event_with_turn(
        orch,
        run_id,
        session_id,
        content,
        now,
        sequence,
        current_turn.as_deref(),
    )
}

/// Store a user event in the transcript with an explicit turn_id.
pub(crate) fn store_user_event_with_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    sequence: i32,
    turn_id: Option<&str>,
) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let event_id = Uuid::new_v4().to_string();
    let transcript_event = TranscriptEvent {
        event_type: "user".to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: Some(content.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        usage: None,
        raw: None,
    };
    let event_data = serde_json::to_string(&transcript_event).unwrap_or_default();

    let new_event = NewEvent {
        id: &event_id,
        run_id,
        session_id: Some(session_id),
        sequence,
        timestamp: now,
        event_type: "user",
        data: &event_data,
        parent_tool_use_id: None,
        created_at: now,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        turn_id,
    };

    diesel::insert_into(events::table)
        .values(&new_event)
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to store user event: {}", e))?;

    // Sync event to cloud
    orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
        id: event_id.clone(),
        run_id: run_id.to_string(),
        session_id: Some(session_id.to_string()),
        sequence: Some(sequence),
        event_type: "user".to_string(),
        data: Some(event_data.clone()),
        input_tokens: None,
        output_tokens: None,
        cache_read_tokens: None,
        created_at: Some(now as i64),
        turn_id: turn_id.map(|s| s.to_string()),
    }));

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
    );

    Ok(())
}
