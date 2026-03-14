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

use crate::claude::stdin::send_user_message_with_images;
use crate::claude::stream::TranscriptEvent;
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
use crate::schema::{events, executions, issues, jobs, projects, prompts, runs};
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
    pub model: Option<String>,
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
    pub prompt: String,
    pub worktree_path: String,
    pub job_model: Option<Model>,
    pub agent_config: Option<AgentConfig>,
    pub artifact_schema_info: Option<OutputSchemaInfo>,
    pub execution_id: Option<String>,
}

// ============================================================================
// on_job_complete_impl
// ============================================================================

/// Called when a job finishes. Advances the execution DAG if applicable.
pub async fn on_job_complete_impl(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    let execution_id: Option<String> = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        jobs::table
            .find(job_id)
            .select(jobs::execution_id)
            .first(&mut *conn)
            .map_err(|e| format!("Job not found: {}", e))?
    };

    match execution_id {
        Some(exec_id) => {
            crate::execution::advancement::advance_execution_with_actions(orch, &exec_id).await
        }
        None => Ok(vec![]), // Standalone job — no DAG to advance
    }
}

// ============================================================================
// prepare_job
// ============================================================================

/// Prepare a job for execution: set up worktree, create run record, store initial
/// user event. Returns a [`PreparedJob`] with everything the host layer needs to
/// call `start_claude_session`.
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
            (true, false, "standalone".to_string())
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

        let branch = format!(
            "agent/{}-{}-{}-{}",
            project_key, display_id, safe_step_name, seq
        );

        let wt_path = dirs::home_dir()
            .ok_or("Could not find home directory")?
            .join(".cairn")
            .join("worktrees")
            .join(format!(
                "{}-{}-{}-{}",
                project_key, display_id, safe_step_name, seq
            ));

        prepare_worktree_for_job(orch, &repo_path, &wt_path, &branch, "HEAD")?;

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

    // ---- Create run record ----------------------------------------------
    let run_id = Uuid::new_v4().to_string();
    let session_id = job
        .claude_session_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let now = chrono::Utc::now().timestamp() as i32;
    let status_str = RunStatus::Running.to_string();

    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let existing_active: Vec<String> = runs::table
            .filter(runs::job_id.eq(job_id))
            .filter(runs::status.eq("running").or(runs::status.eq("pending")))
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
            claude_session_id: Some(&session_id),
            error_message: None,
            started_at: Some(now),
            completed_at: None,
            created_at: now,
            updated_at: now,
            todos: None,
        };

        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to create run: {}", e))?;
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "insert"}),
    );

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

    let worktree_path: String = job
        .worktree_path
        .clone()
        .or_else(|| {
            project_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
        })
        .ok_or("Job has no worktree path and project path is unavailable")?;

    let job_model = job
        .model
        .as_ref()
        .map(|m| m.parse::<Model>())
        .transpose()
        .map_err(|e: String| format!("Invalid model in job: {}", e))?;

    // ---- Store initial user event ---------------------------------------
    store_user_event(orch, &run_id, &session_id, &prompt, now, -1)?;

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
        prompt,
        worktree_path,
        job_model,
        agent_config,
        artifact_schema_info,
        execution_id: job.execution_id,
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

    log::info!(
        "[DEBUG-RESUME] continue_job_impl: job_id={}, claude_session_id={:?}",
        job.id,
        job.claude_session_id
    );

    let session_id = job
        .claude_session_id
        .as_ref()
        .ok_or("Job has no Claude session to resume")?;

    let now = chrono::Utc::now().timestamp() as i32;

    // ---- Find or create run ---------------------------------------------
    let existing_run_id = orch.process_state.find_process_by_session(session_id);
    let (run_id, is_process_reuse) = if let Some(existing_id) = existing_run_id {
        log::info!(
            "Found existing process for session {}, reusing run {}",
            &session_id[..session_id.len().min(8)],
            &existing_id[..existing_id.len().min(8)]
        );
        (existing_id, true)
    } else {
        let new_run_id = Uuid::new_v4().to_string();
        let status_str = RunStatus::Pending.to_string();
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
                claude_session_id: Some(session_id),
                error_message: None,
                started_at: None,
                completed_at: None,
                created_at: now,
                updated_at: now,
                todos: None,
            };
            diesel::insert_into(runs::table)
                .values(&new_run)
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to create run: {}", e))?;
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "runs", "action": "insert"}),
        );
        (new_run_id, false)
    };

    // ---- Handle paused runs + issue status ------------------------------
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let paused_run_ids: Vec<String> = runs::table
            .filter(runs::job_id.eq(job_id))
            .filter(runs::status.eq("paused"))
            .select(runs::id)
            .load(&mut *conn)
            .unwrap_or_default();

        for rid in &paused_run_ids {
            let _ = diesel::update(prompts::table.filter(prompts::run_id.eq(rid)))
                .filter(prompts::response.is_null())
                .set((
                    prompts::response.eq(message),
                    prompts::answered_at.eq(Some(now)),
                ))
                .execute(&mut *conn);
        }

        let _ = diesel::update(runs::table)
            .filter(runs::job_id.eq(job_id))
            .filter(runs::status.eq("paused"))
            .set((
                runs::status.eq("completed"),
                runs::completed_at.eq(Some(now)),
                runs::updated_at.eq(now),
            ))
            .execute(&mut *conn);

        if let Some(ref iid) = issue_id {
            let _ = diesel::update(issues::table.find(iid))
                .set((
                    issues::status.eq("active"),
                    issues::wait_state.eq(None::<String>),
                    issues::updated_at.eq(now),
                ))
                .execute(&mut *conn);
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "issues", "action": "update"}),
            );
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "update"}),
    );

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

    let prompt = message
        .unwrap_or("Continue where you left off.")
        .to_string();

    let job_model = job
        .model
        .as_ref()
        .map(|m| m.parse::<Model>())
        .transpose()
        .map_err(|e: String| format!("Invalid model in job: {}", e))?;

    // ---- Store user event -----------------------------------------------
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let max_seq: Option<i32> = events::table
            .filter(events::session_id.eq(session_id))
            .select(diesel::dsl::max(events::sequence))
            .first(&mut *conn)
            .unwrap_or(None);
        let next_seq = max_seq.unwrap_or(-1) + 1;

        let event_id = Uuid::new_v4().to_string();
        let transcript_event = TranscriptEvent {
            event_type: "user".to_string(),
            session_id: Some(session_id.clone()),
            parent_tool_use_id: None,
            content: Some(prompt.clone()),
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
            session_id: Some(session_id),
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
        };
        let _ = diesel::insert_into(events::table)
            .values(&new_event)
            .execute(&mut *conn);
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "events", "action": "insert"}),
        );
    }

    // ---- Warm process or new session ------------------------------------
    if is_process_reuse {
        let stdin_handle = orch
            .process_state
            .get_stdin_handle(&run_id)
            .ok_or("Warm process stdin not available")?;

        {
            let mut stdin_guard = stdin_handle
                .lock()
                .map_err(|e| format!("Failed to lock stdin: {}", e))?;
            if let Some(ref mut stdin) = *stdin_guard {
                // Pass None for image cache — embedding happens via session args in fresh starts
                send_user_message_with_images(
                    stdin,
                    session_id,
                    &prompt,
                    None,
                    Some(&worktree_path),
                    None,
                )?;
            } else {
                return Err("Warm process stdin is None".to_string());
            }
        }

        orch.process_state.transition_to_active(&run_id);

        {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            diesel::update(runs::table.find(&run_id))
                .set((runs::status.eq("running"), runs::updated_at.eq(now)))
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to update run status: {}", e))?;
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "runs", "action": "update"}),
        );
    } else {
        crate::orchestrator::session::start_claude_session(
            orch,
            &run_id,
            &prompt,
            &worktree_path,
            None,
            Some(session_id),
            job_model,
            None,
            agent_config.as_ref(),
            artifact_schema_info.as_ref(),
            false,
            job.execution_id.as_deref(),
        )?;
    }

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
/// The child inherits the parent's worktree. A new Claude session is started
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
    let agent_config: AgentConfig = {
        let config_dir = config::get_config_dir()?;

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
            model: file_agent.model,
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
            permission_mode: file_agent.permission_mode,
        }
    };

    // ---- Create job + run -----------------------------------------------
    let job_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let session_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let selected_model: Option<Model> = if let Some(ref model_str) = input.model {
        Some(
            model_str
                .parse::<Model>()
                .map_err(|e: String| format!("Invalid model: {}", e))?,
        )
    } else {
        agent_config.model.clone()
    };

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
            recipe_node_id: None,
            parent_job_id: Some(&input.parent_job_id),
            worktree_path: Some(&worktree_path),
            branch: None,
            base_commit: None,
            claude_session_id: Some(&session_id),
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
        };

        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to create child job: {}", e))?;

        let run_status = RunStatus::Running.to_string();
        let new_run = NewRun {
            id: &run_id,
            issue_id: issue_id.as_deref(),
            project_id: Some(&project_id),
            job_id: Some(&job_id),
            chat_id: None,
            status: Some(&run_status),
            claude_session_id: Some(&session_id),
            error_message: None,
            started_at: Some(now),
            completed_at: None,
            created_at: now,
            updated_at: now,
            todos: None,
        };

        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to create run: {}", e))?;
    }

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

    // ---- Start Claude session -------------------------------------------
    crate::orchestrator::session::start_claude_session(
        orch,
        &run_id,
        &input.prompt,
        &worktree_path,
        Some(&session_id),
        None,
        selected_model,
        None,
        Some(&agent_config),
        Some(&output_schema),
        false,
        None,
    )?;

    Ok(CreateChildTaskResult { job_id, run_id })
}

// ============================================================================
// Private helpers
// ============================================================================

/// Create a worktree for a job using the orchestrator's service traits.
fn prepare_worktree_for_job(
    orch: &Orchestrator,
    repo_path: &str,
    worktree_path: &Path,
    branch: &str,
    base_ref: &str,
) -> Result<(), String> {
    let settings = load_project_settings(Path::new(repo_path));
    let copy_files = settings.copy_files.unwrap_or_default();
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

    if !copy_files.is_empty() {
        if let Err(e) = crate::git::worktree::copy_files_to_worktree_with_services(
            fs,
            repo,
            worktree_path,
            &copy_files,
        ) {
            log::error!("Copy files failed, cleaning up worktree: {}", e);
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
fn load_agent_config(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: Option<&Path>,
) -> Result<Option<AgentConfig>, String> {
    let Some(aid) = &job.agent_config_id else {
        return Ok(None);
    };

    // Try snapshot first (ensures reproducibility for execution-based jobs)
    let mut snapshot_model_override: Option<Model> = None;
    if let Some(exec_id) = &job.execution_id {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        if let Some(agent) = load_agent_from_snapshot(&mut conn, exec_id, aid)? {
            if !agent.prompt.is_empty() {
                return Ok(Some(agent));
            }
            // Snapshot agent has empty prompt — likely a placeholder from executor
            // expansion. Fall through to config files, but remember overrides.
            snapshot_model_override = agent.model;
            log::debug!(
                "Snapshot agent '{}' has empty prompt, falling through to config files",
                aid
            );
        }
    }

    // Fall back to config files
    let project_id = &job.project_id;
    let agent = config::get_config_dir()
        .ok()
        .and_then(|cd| {
            config_agents::get_agent(&cd, aid, project_path)
                .ok()
                .flatten()
        })
        .map(|fa| {
            // Apply snapshot model override if the TaskList specified one
            let model = snapshot_model_override.or(fa.model);

            AgentConfig {
                id: fa.id,
                name: fa.name,
                description: fa.description,
                prompt: fa.prompt,
                tools: fa.tools,
                model,
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
                disallowed_tools: fa.disallowed_tools,
                skills: fa.skills,
                permission_mode: fa.permission_mode,
            }
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
                model: agent.model.clone(),
                workspace_id: None,
                project_id: None,
                created_at: snapshot.created_at as i32,
                updated_at: snapshot.created_at as i32,
                disallowed_tools: agent.disallowed_tools.clone(),
                skills: agent.skills.clone(),
                permission_mode: agent.permission_mode.clone(),
            })
        })
        .transpose()
}

/// Store a user event in the transcript.
fn store_user_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    now: i32,
    sequence: i32,
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
    };

    diesel::insert_into(events::table)
        .values(&new_event)
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to store user event: {}", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
    );

    Ok(())
}
