//! Checkpoint approval flow for recipe execution.
//!
//! Human-in-the-loop checkpoints where agents wait for user approval
//! before continuing execution. Supports approval, rejection with feedback.
//!
//! These are internal helpers called from advancement logic.
//! The Tauri layer wraps these as `#[tauri::command]` functions.

use diesel::prelude::*;
use uuid::Uuid;

use crate::diesel_models::{DbArtifact, DbJob, DbRecipeEdge, NewRun, UpdateJobChangeset};
use crate::models::{AgentConfig, AnnotationInput, Job, Model};
use crate::orchestrator::Orchestrator;
use crate::schema::{artifacts, executions, issues, jobs, projects, runs};

/// Format annotations as markdown for agent prompts.
/// Used by both rejection messages and resolved inputs.
pub fn format_annotations(annotations: &[serde_json::Value]) -> String {
    if annotations.is_empty() {
        return String::new();
    }

    let mut message = String::from("\n## User Feedback:\n");
    for (i, annot) in annotations.iter().enumerate() {
        let annotation_type = annot
            .get("annotationType")
            .and_then(|v| v.as_str())
            .unwrap_or("note");
        let type_label = match annotation_type {
            "correction" => "Correction",
            "question" => "Question",
            "context" => "Context",
            _ => "Note",
        };
        let source_text = annot
            .get("sourceText")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let note = annot.get("note").and_then(|v| v.as_str()).unwrap_or("");

        message.push_str(&format!(
            "\n{}. **{}** on \"{}\":\n   {}\n",
            i + 1,
            type_label,
            source_text,
            note
        ));
    }
    message
}

/// Approve a blocked checkpoint job.
/// Marks the job as complete, then advances the DAG.
/// Returns the list of newly ready agent jobs after advancement.
pub async fn approve_job_inner(
    orch: &Orchestrator,
    job_id: &str,
    annotations: Option<Vec<AnnotationInput>>,
) -> Result<Vec<Job>, String> {
    let execution_id: Option<String> = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let current_status: String = jobs::table
            .find(job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .map_err(|e| format!("Job not found: {}", e))?;

        if current_status != "blocked" {
            return Err(format!(
                "Job is not blocked (current status: {})",
                current_status
            ));
        }

        // Add annotations to latest artifact if provided
        if let Some(annots) = &annotations {
            if !annots.is_empty() {
                let latest_artifact: Option<DbArtifact> = artifacts::table
                    .filter(artifacts::job_id.eq(job_id))
                    .order(artifacts::version.desc())
                    .first(&mut *conn)
                    .optional()
                    .map_err(|e| format!("Failed to query artifact: {}", e))?;

                if let Some(artifact) = latest_artifact {
                    let mut data: serde_json::Value = serde_json::from_str(&artifact.data)
                        .map_err(|e| format!("Invalid artifact data JSON: {}", e))?;

                    let annotations_with_ids: Vec<serde_json::Value> = annots
                        .iter()
                        .map(|a| {
                            serde_json::json!({
                                "id": Uuid::new_v4().to_string(),
                                "sourceText": a.source_text,
                                "note": a.note,
                                "annotationType": a.annotation_type,
                            })
                        })
                        .collect();

                    if let Some(obj) = data.as_object_mut() {
                        let existing = obj
                            .entry("annotations")
                            .or_insert_with(|| serde_json::json!([]));
                        if let Some(arr) = existing.as_array_mut() {
                            arr.extend(annotations_with_ids);
                        }
                    }

                    let now = chrono::Utc::now().timestamp() as i32;
                    let data_str = serde_json::to_string(&data)
                        .map_err(|e| format!("Failed to serialize data: {}", e))?;

                    diesel::update(artifacts::table.find(&artifact.id))
                        .set((artifacts::data.eq(&data_str), artifacts::updated_at.eq(now)))
                        .execute(&mut *conn)
                        .map_err(|e| format!("Failed to update artifact: {}", e))?;
                }
            }
        }

        let now = chrono::Utc::now().timestamp() as i32;

        let changeset = UpdateJobChangeset {
            status: Some("complete"),
            updated_at: Some(now),
            completed_at: Some(Some(now)),
            ..Default::default()
        };

        diesel::update(jobs::table.find(job_id))
            .set(&changeset)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to update job: {}", e))?;

        let (exec_id, issue_id): (Option<String>, Option<String>) = jobs::table
            .find(job_id)
            .select((jobs::execution_id, jobs::issue_id))
            .first(&mut *conn)
            .ok()
            .unwrap_or((None, None));

        // Clear wait_state on issue (waiting(checkpoint) → active)
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

        exec_id
    };

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    match execution_id {
        Some(exec_id) => {
            Box::pin(crate::execution::advancement::advance_execution_with_actions(orch, &exec_id))
                .await
        }
        None => Ok(vec![]),
    }
}

/// Build rejection message including artifact content and annotation details.
fn build_rejection_message(
    rejection_message: &Option<String>,
    annotations: &Option<Vec<AnnotationInput>>,
    artifact_data: &Option<serde_json::Value>,
) -> String {
    let mut message = String::new();

    if let Some(data) = artifact_data {
        message.push_str("## Your Previous Output:\n\n");
        if let Some(obj) = data.as_object() {
            for (key, value) in obj {
                if key == "annotations" {
                    continue;
                }
                if let Some(s) = value.as_str() {
                    message.push_str(&format!("**{}**:\n{}\n\n", key, s));
                } else {
                    let formatted = serde_json::to_string_pretty(value).unwrap_or_default();
                    message.push_str(&format!("**{}**:\n```json\n{}\n```\n\n", key, formatted));
                }
            }
        } else if let Some(s) = data.as_str() {
            message.push_str(s);
            message.push_str("\n\n");
        } else {
            let formatted = serde_json::to_string_pretty(data).unwrap_or_default();
            message.push_str(&format!("```json\n{}\n```\n\n", formatted));
        }
    }

    if let Some(custom_msg) = rejection_message {
        if !message.is_empty() {
            message.push_str("---\n\n");
        }
        message.push_str(custom_msg);
        message.push('\n');
    }

    let mut all_annotations: Vec<serde_json::Value> = Vec::new();
    if let Some(data) = artifact_data {
        if let Some(stored_annots) = data.get("annotations").and_then(|a| a.as_array()) {
            all_annotations.extend(stored_annots.iter().cloned());
        }
    }
    if let Some(annots) = annotations {
        for a in annots {
            let annot_json = serde_json::json!({
                "sourceText": a.source_text,
                "note": a.note,
                "annotationType": a.annotation_type,
            });
            if !all_annotations.iter().any(|existing| {
                existing.get("sourceText") == annot_json.get("sourceText")
                    && existing.get("note") == annot_json.get("note")
            }) {
                all_annotations.push(annot_json);
            }
        }
    }

    if !all_annotations.is_empty() {
        message.push_str(&format_annotations(&all_annotations));
    }

    if message.is_empty() {
        message = "Your previous output was rejected. Please revise.".to_string();
    }

    message
}

/// Reject a blocked checkpoint job.
/// For checkpoint nodes with rejection edges: resets checkpoint and resumes upstream agent.
/// For other jobs: marks as failed.
pub async fn reject_job_inner(
    orch: &Orchestrator,
    job_id: &str,
    rejection_message: Option<String>,
    annotations: Option<Vec<AnnotationInput>>,
) -> Result<Job, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let job: DbJob = jobs::table
        .find(job_id)
        .first(&mut *conn)
        .map_err(|e| format!("Job not found: {}", e))?;

    if job.status != "blocked" {
        return Err(format!(
            "Job is not blocked (current status: {})",
            job.status
        ));
    }

    let latest_artifact: Option<DbArtifact> = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .order(artifacts::version.desc())
        .first(&mut *conn)
        .optional()
        .map_err(|e| format!("Failed to query artifact: {}", e))?;

    let artifact_data: Option<serde_json::Value> = latest_artifact
        .as_ref()
        .and_then(|a| serde_json::from_str(&a.data).ok());

    // Add annotations to artifact if provided
    if let Some(annots) = &annotations {
        if !annots.is_empty() {
            if let Some(artifact) = &latest_artifact {
                let mut data: serde_json::Value = serde_json::from_str(&artifact.data)
                    .map_err(|e| format!("Invalid artifact data JSON: {}", e))?;

                let annotations_with_ids: Vec<serde_json::Value> = annots
                    .iter()
                    .map(|a| {
                        serde_json::json!({
                            "id": Uuid::new_v4().to_string(),
                            "sourceText": a.source_text,
                            "note": a.note,
                            "annotationType": a.annotation_type,
                        })
                    })
                    .collect();

                if let Some(obj) = data.as_object_mut() {
                    let existing = obj
                        .entry("annotations")
                        .or_insert_with(|| serde_json::json!([]));
                    if let Some(arr) = existing.as_array_mut() {
                        arr.extend(annotations_with_ids);
                    }
                }

                let now = chrono::Utc::now().timestamp() as i32;
                let data_str = serde_json::to_string(&data)
                    .map_err(|e| format!("Failed to serialize data: {}", e))?;

                diesel::update(artifacts::table.find(&artifact.id))
                    .set((artifacts::data.eq(&data_str), artifacts::updated_at.eq(now)))
                    .execute(&mut *conn)
                    .map_err(|e| format!("Failed to update artifact: {}", e))?;
            }
        }
    }

    // Check if this is a node-based checkpoint with a rejection edge
    if let (Some(node_id), Some(execution_id)) = (&job.recipe_node_id, &job.execution_id) {
        use crate::execution::dag::load_edges_from_execution;

        let all_edges = load_edges_from_execution(&mut conn, execution_id)?;

        let reject_edge: Option<&DbRecipeEdge> = all_edges.iter().find(|e| {
            e.source_node_id == *node_id
                && e.edge_type == "control"
                && e.source_handle.contains("reject")
        });

        if let Some(edge) = reject_edge {
            let upstream_job: Result<DbJob, _> = jobs::table
                .filter(jobs::execution_id.eq(execution_id))
                .filter(jobs::recipe_node_id.eq(&edge.target_node_id))
                .first(&mut *conn);

            if let Ok(upstream) = upstream_job {
                let now = chrono::Utc::now().timestamp() as i32;
                diesel::update(jobs::table.find(job_id))
                    .set((jobs::status.eq("pending"), jobs::updated_at.eq(now)))
                    .execute(&mut *conn)
                    .map_err(|e| format!("Failed to reset checkpoint: {}", e))?;

                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "jobs", "action": "update"}),
                );

                // Drop lock before calling continue (which re-acquires it)
                drop(conn);

                let message =
                    build_rejection_message(&rejection_message, &annotations, &artifact_data);

                continue_job_for_rejection(orch, &upstream.id, message)?;

                let mut conn = orch
                    .db
                    .conn
                    .lock()
                    .map_err(|e| format!("Failed to lock database: {}", e))?;

                let updated_job: DbJob = jobs::table
                    .find(job_id)
                    .first(&mut *conn)
                    .map_err(|e| format!("Failed to reload job: {}", e))?;

                return Job::try_from(updated_job);
            }
        }
    }

    // No rejection edge — mark as failed
    let now = chrono::Utc::now().timestamp() as i32;
    let changeset = UpdateJobChangeset {
        status: Some("failed"),
        updated_at: Some(now),
        completed_at: Some(Some(now)),
        ..Default::default()
    };

    diesel::update(jobs::table.find(job_id))
        .set(&changeset)
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to update job: {}", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    let db_job: DbJob = jobs::table
        .find(job_id)
        .first(&mut *conn)
        .map_err(|e| format!("Failed to reload job: {}", e))?;

    Job::try_from(db_job)
}

/// Continue a job by creating a new run and resuming its Claude session.
/// Used for the rejection flow: resumes the upstream agent with a feedback message.
fn continue_job_for_rejection(
    orch: &Orchestrator,
    job_id: &str,
    message: String,
) -> Result<(), String> {
    let (job, project_id, issue_id, project_path) = {
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

        let project_path: Option<std::path::PathBuf> = projects::table
            .find(&project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(std::path::PathBuf::from);

        (job, project_id, issue_id, project_path)
    };

    // Load agent config from execution snapshot
    let agent_config: Option<AgentConfig> = load_agent_config_for_job(orch, &job, &project_path)?;

    let worktree_path: String = job
        .worktree_path
        .clone()
        .or_else(|| {
            project_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
        })
        .ok_or("Job has no worktree path and project path is unavailable")?;

    let session_id = job
        .claude_session_id
        .as_ref()
        .ok_or("Job has no Claude session to resume")?;

    let now = chrono::Utc::now().timestamp() as i32;
    let run_id = Uuid::new_v4().to_string();

    // Create a new run
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let new_run = NewRun {
            id: &run_id,
            issue_id: issue_id.as_deref(),
            project_id: Some(&project_id),
            job_id: Some(job_id),
            chat_id: None,
            status: Some("pending"),
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

    // Look up artifact schema from the execution snapshot
    let artifact_schema_info = if let (Some(node_id), Some(execution_id)) =
        (&job.recipe_node_id, &job.execution_id)
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        crate::execution::dag::find_downstream_artifact_schema(&mut conn, node_id, execution_id)?
    } else {
        None
    };

    let job_model = job
        .model
        .as_ref()
        .map(|m| m.parse::<Model>())
        .transpose()
        .map_err(|e: String| format!("Invalid model in job: {}", e))?;

    crate::orchestrator::session::start_claude_session(
        orch,
        &run_id,
        &message,
        &worktree_path,
        None,             // Not a new session
        Some(session_id), // Resume existing session
        job_model,
        None, // No separate initial_user_message (message is the prompt)
        agent_config.as_ref(),
        artifact_schema_info.as_ref(),
        false,
        job.execution_id.as_deref(),
    )?;

    Ok(())
}

/// Load agent config for a job, trying the execution snapshot first.
fn load_agent_config_for_job(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: &Option<std::path::PathBuf>,
) -> Result<Option<AgentConfig>, String> {
    let Some(ref agent_config_id) = job.agent_config_id else {
        return Ok(None);
    };

    // Try snapshot first if job is part of an execution
    if let Some(ref execution_id) = job.execution_id {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        use crate::models::ExecutionSnapshot;

        let snapshot_json: Option<String> = executions::table
            .find(execution_id)
            .select(executions::snapshot)
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("Failed to load execution: {}", e))?
            .flatten();

        if let Some(json) = snapshot_json {
            if let Ok(snapshot) = serde_json::from_str::<ExecutionSnapshot>(&json) {
                if let Some(agent) = snapshot.agents.get(agent_config_id) {
                    return Ok(Some(AgentConfig {
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
                    }));
                }
            }
        }
    }

    // Fall back to file-based config
    let config_dir = crate::config::get_config_dir()?;
    let project_id = &job.project_id;
    let file_agent =
        crate::config::agents::get_agent(&config_dir, agent_config_id, project_path.as_deref())
            .ok()
            .flatten();

    Ok(file_agent.map(|a| AgentConfig {
        id: a.id,
        name: a.name,
        description: a.description,
        prompt: a.prompt,
        tools: a.tools,
        model: a.model,
        workspace_id: if a.is_project_scoped {
            None
        } else {
            Some("workspace".to_string())
        },
        project_id: if a.is_project_scoped {
            Some(project_id.clone())
        } else {
            None
        },
        created_at: 0,
        updated_at: 0,
        disallowed_tools: a.disallowed_tools,
        skills: a.skills,
        permission_mode: a.permission_mode,
    }))
}
