//! Implementation-related MCP handlers.
//!
//! Handles: add_comment, return

use crate::diesel_models::{NewComment, UpdateRunChangeset};
use crate::jobs::queries::{
    find_node_in_snapshot, get_node_name_for_job, get_task_parent_info, load_execution_snapshot,
};
use crate::mcp::types::{AddCommentPayload, McpCallbackRequest};
use crate::models::RunStatus;
use crate::orchestrator::Orchestrator;
use crate::schema::{artifacts, comments, jobs, runs};
use diesel::prelude::*;

use super::{lookup_run, RunContext};

// ============================================================================
// Handlers
// ============================================================================

/// Handle add_comment tool call - adds a comment to the issue associated with this run
pub async fn handle_add_comment(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: AddCommentPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!(
        "add_comment for cwd={}, {} chars",
        request.cwd,
        payload.content.len()
    );

    let services = &orch.services;
    let db_state = &orch.db;

    let mut conn = match db_state.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Get the issue_id from the run
    let ctx = match lookup_run(diesel_conn, request) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };
    let issue_id = match ctx.issue_id {
        Some(id) => id,
        None => {
            return "Cannot add comment: project-level jobs don't have an associated issue"
                .to_string()
        }
    };

    // Insert the comment
    let now = chrono::Utc::now().timestamp() as i32;
    let comment_id = uuid::Uuid::new_v4().to_string();

    let new_comment = NewComment {
        id: &comment_id,
        issue_id: &issue_id,
        content: &payload.content,
        source: "agent",
        created_at: now,
    };

    if let Err(e) = diesel::insert_into(comments::table)
        .values(&new_comment)
        .execute(diesel_conn)
    {
        return format!("Failed to insert comment: {}", e);
    }

    // Emit db-change event so frontend can refresh comments
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "comments", "action": "insert"}),
    );

    "Comment added to issue.".to_string()
}

/// Handle return tool call - submit final output and complete the job
pub async fn handle_return(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    use crate::diesel_models::{NewArtifact, UpdateJobChangeset};

    // The payload is the raw artifact output (already validated by cairn-mcp against schema)
    let payload = &request.payload;

    log::info!(
        "return for cwd={}, payload keys: {:?}",
        request.cwd,
        payload.as_object().map(|o| o.keys().collect::<Vec<_>>())
    );

    let services = &orch.services;

    // Collect data needed for DAG advancement and checkpoint execution outside the lock
    let (
        run_id,
        job_id,
        artifact_type,
        artifact_uri,
        execution_id,
        checkpoint_info,
        worktree_path,
        is_blocked,
    ) = {
        let db_state = &orch.db;

        let mut conn = match db_state.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Failed to lock database: {}", e),
        };

        let diesel_conn = &mut *conn;

        // Get the run context
        let ctx = match lookup_run(diesel_conn, request) {
            Ok(ctx) => ctx,
            Err(e) => return e,
        };

        // Get artifact type and output name from three sources (in priority order):
        // 1. Source node's own outputSchema.schemaType
        // 2. Downstream ActionNode's inputSchema.schemaType
        // 3. Downstream ArtifactNode's artifactType
        // This matches the priority logic in find_downstream_artifact_schema() in job.rs
        let (artifact_type, output_name) =
            resolve_artifact_type_and_output(diesel_conn, &ctx.job_id, &ctx.job_type);

        // Serialize the payload
        let data_json = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => return format!("Failed to serialize artifact data: {}", e),
        };

        // Check for existing artifact on this job (for versioning)
        let existing: Option<(String, i32)> = artifacts::table
            .filter(artifacts::job_id.eq(&ctx.job_id))
            .order(artifacts::version.desc())
            .select((artifacts::id, artifacts::version))
            .first(diesel_conn)
            .ok();

        let (parent_version_id, version) = match existing {
            Some((parent_id, parent_version)) => (Some(parent_id), parent_version + 1),
            None => (None, 1),
        };

        // Create the artifact
        let now = chrono::Utc::now().timestamp() as i32;
        let artifact_id = uuid::Uuid::new_v4().to_string();

        let new_artifact = NewArtifact {
            id: &artifact_id,
            job_id: Some(&ctx.job_id),
            artifact_type: &artifact_type,
            schema_version: 1,
            data: &data_json,
            version,
            parent_version_id: parent_version_id.as_deref(),
            output_name: output_name.as_deref(),
            created_at: now,
            updated_at: now,
        };

        if let Err(e) = diesel::insert_into(artifacts::table)
            .values(&new_artifact)
            .execute(diesel_conn)
        {
            return format!("Failed to store artifact: {}", e);
        }

        log::info!(
            "Artifact stored: id={}, job_id={}, type={}, version={}",
            artifact_id,
            ctx.job_id,
            artifact_type,
            version
        );

        // Get checkpoint info to determine how to handle completion
        let checkpoint_info = get_checkpoint_info(diesel_conn, &ctx.job_id);

        // Determine initial status based on checkpoint type:
        // - approval/prompt: block immediately
        // - programmatic: mark complete (will verify after lock release)
        // - none: complete
        let (initial_status, is_blocked) = match &checkpoint_info {
            Some(info)
                if info.checkpoint_type == "approval" || info.checkpoint_type == "prompt" =>
            {
                ("blocked", true)
            }
            _ => ("complete", false),
        };

        // Mark job with initial status
        let job_update = UpdateJobChangeset {
            status: Some(initial_status),
            updated_at: Some(now),
            completed_at: if is_blocked { None } else { Some(Some(now)) },
            ..Default::default()
        };

        if let Err(e) = diesel::update(jobs::table.find(&ctx.job_id))
            .set(&job_update)
            .execute(diesel_conn)
        {
            log::error!("Failed to update job status: {}", e);
        }

        // Get execution_id and worktree for post-lock operations
        let (execution_id, worktree_path): (Option<String>, Option<String>) = jobs::table
            .find(&ctx.job_id)
            .select((jobs::execution_id, jobs::worktree_path))
            .first(diesel_conn)
            .ok()
            .unwrap_or((None, None));

        if is_blocked {
            log::info!(
                "Job {} blocked at checkpoint - awaiting approval",
                ctx.job_id
            );

            // Update issue status to waiting(checkpoint) if this job has an issue
            if let Some(ref issue_id) = ctx.issue_id {
                use crate::schema::issues;
                let _ = diesel::update(issues::table.find(issue_id))
                    .set((
                        issues::status.eq("waiting"),
                        issues::wait_state.eq(Some("checkpoint")),
                        issues::updated_at.eq(now),
                    ))
                    .execute(diesel_conn);
            }
        }

        // Emit events for frontend
        let _ = services.emitter.emit(
            "artifact-submitted",
            serde_json::json!({
                "artifact_id": artifact_id,
                "run_id": ctx.run_id,
                "job_id": ctx.job_id,
                "artifact_type": artifact_type,
                "version": version,
            }),
        );

        let _ = services
            .emitter
            .emit("db-change", serde_json::json!({ "table": "artifacts" }));
        let _ = services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "jobs", "action": "update"}),
        );
        if is_blocked {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "issues", "action": "update"}),
            );
        }

        // Build artifact URI while we have the context and db lock
        let artifact_uri = build_artifact_uri(diesel_conn, &ctx, &ctx.job_id);

        (
            ctx.run_id,
            ctx.job_id,
            artifact_type,
            artifact_uri,
            execution_id,
            checkpoint_info,
            worktree_path,
            is_blocked,
        )
    };

    // Mark run completed (process stays alive for warm retention)
    mark_run_completed(orch, &run_id);

    // Handle programmatic checkpoint if present (outside db lock)
    let mut should_advance_dag = !is_blocked;

    if let Some(ref info) = checkpoint_info {
        if info.checkpoint_type == "programmatic" {
            let command = info.command.clone().unwrap_or_else(|| "exit 0".to_string());

            if let Some(ref worktree) = worktree_path {
                log::info!(
                    "Executing programmatic checkpoint for job {}: {}",
                    job_id,
                    command
                );

                match crate::execution::conditions::execute_programmatic_checkpoint(
                    worktree, &command,
                )
                .await
                {
                    Ok(true) => {
                        log::info!("Programmatic checkpoint passed for job {}", job_id);
                        // Job already marked complete - continue with DAG advancement
                    }
                    Ok(false) | Err(_) => {
                        log::warn!("Programmatic checkpoint failed for job {}", job_id);
                        // Mark job as failed
                        if let Ok(mut conn) = orch.db.conn.lock() {
                            let now = chrono::Utc::now().timestamp() as i32;
                            let _ = diesel::update(jobs::table.find(&job_id))
                                .set((
                                    jobs::status.eq("failed"),
                                    jobs::updated_at.eq(now),
                                    jobs::completed_at.eq(Some(now)),
                                ))
                                .execute(&mut *conn);

                            let _ = services.emitter.emit(
                                "db-change",
                                serde_json::json!({"table": "jobs", "action": "update"}),
                            );
                        }
                        should_advance_dag = false;
                    }
                }
            } else {
                log::warn!(
                    "No worktree found for programmatic checkpoint on job {}",
                    job_id
                );
            }
        }
    }

    // Advance DAG with action execution (async, outside db lock)
    if should_advance_dag {
        if let Some(exec_id) = execution_id {
            match crate::execution::advancement::advance_execution_with_actions(orch, &exec_id)
                .await
            {
                Ok(ready_jobs) => {
                    if !ready_jobs.is_empty() {
                        log::info!(
                            "DAG advancement: {} agent jobs now ready in execution {}",
                            ready_jobs.len(),
                            exec_id
                        );
                    }
                }
                Err(e) => {
                    log::error!("Failed to advance execution DAG: {}", e);
                }
            }
        }
    }

    // Schedule interrupt after a short delay to ensure MCP response is delivered first.
    // This stops Claude from generating more after receiving the tool result.
    let process_state = orch.process_state.clone();
    let run_id_clone = run_id.clone();
    tokio::spawn(async move {
        // Small delay to let the MCP response get through
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        if let Some(stdin_handle) = process_state.get_stdin_handle(&run_id_clone) {
            if let Ok(mut stdin_guard) = stdin_handle.lock() {
                if let Some(ref mut stdin) = *stdin_guard {
                    let request_id = uuid::Uuid::new_v4().to_string();
                    if crate::claude::stdin::send_interrupt_request(stdin.as_mut(), &request_id)
                        .is_ok()
                    {
                        log::info!(
                            "Sent interrupt after turn-ender tool for run {}",
                            &run_id_clone[..run_id_clone.len().min(8)]
                        );
                    }
                }
            }
        }
    });

    // Build artifact reference - prefer URI, fall back to job UUID
    let artifact_ref = artifact_uri.unwrap_or_else(|| format!("job: {}", job_id));

    format!(
        "Output submitted successfully ({}, type: {})",
        artifact_ref, artifact_type
    )
}

/// Helper to mark run as completed (process stays alive for warm retention)
///
/// Note: We intentionally do NOT kill the Claude process here. The process
/// will transition to warm state when it emits its Result event, allowing
/// it to be reused for follow-up messages. The session.rs reader thread
/// handles process lifecycle via transition_to_warm_state().
fn mark_run_completed(orch: &Orchestrator, run_id: &str) {
    let services = &orch.services;
    // Mark run as completed
    if let Ok(mut conn) = orch.db.conn.lock() {
        let now = chrono::Utc::now().timestamp() as i32;
        let status_str = RunStatus::Completed.to_string();
        let run_update = UpdateRunChangeset {
            status: Some(&status_str),
            completed_at: Some(Some(now)),
            updated_at: Some(now),
            ..Default::default()
        };
        let _ = diesel::update(runs::table.find(run_id))
            .set(&run_update)
            .execute(&mut *conn);
    }

    // Emit db-change for runs table
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "update"}),
    );

    log::info!(
        "Run {} marked completed, process retained for warm state",
        &run_id[..run_id.len().min(8)]
    );
}

/// Checkpoint information for a job's node.
#[derive(Debug, Clone)]
struct CheckpointInfo {
    checkpoint_type: String, // "approval", "prompt", or "programmatic"
    command: Option<String>, // For programmatic: the command to run
}

/// Get checkpoint information for a job's node.
/// Checks for embedded checkpoint in agent/action nodes or standalone checkpoint nodes.
fn get_checkpoint_info(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> Option<CheckpointInfo> {
    // Get job's execution_id and recipe_node_id
    let job_data: Option<(Option<String>, Option<String>)> = jobs::table
        .find(job_id)
        .select((jobs::execution_id, jobs::recipe_node_id))
        .first(conn)
        .ok();

    let (execution_id, node_id) = match job_data {
        Some((Some(e), Some(n))) => (e, n),
        _ => return None,
    };

    // Load node from execution snapshot
    let snapshot = load_execution_snapshot(conn, &execution_id).ok()?;
    let node = find_node_in_snapshot(&snapshot, &node_id)?;

    // Check for embedded checkpoint in agent nodes
    if let Some(ref agent_cfg) = node.agent_config {
        if let Some(ref checkpoint) = agent_cfg.checkpoint {
            return Some(CheckpointInfo {
                checkpoint_type: checkpoint.checkpoint_type.to_string(),
                command: checkpoint.command.clone(),
            });
        }
    }

    // Check for embedded checkpoint in action nodes
    if let Some(ref action_cfg) = node.action_config {
        if let Some(ref checkpoint) = action_cfg.checkpoint {
            return Some(CheckpointInfo {
                checkpoint_type: checkpoint.checkpoint_type.to_string(),
                command: checkpoint.command.clone(),
            });
        }
    }

    // Check for standalone checkpoint node
    if node.node_type.to_string() == "checkpoint" {
        if let Some(ref checkpoint_cfg) = node.checkpoint_config {
            return Some(CheckpointInfo {
                checkpoint_type: checkpoint_cfg.checkpoint_type.to_string(),
                command: checkpoint_cfg.command.clone(),
            });
        }
    }

    None
}

/// Build artifact URI for the tool response.
/// Returns format like `cairn://CAIRN/123/Planner/artifact` or `cairn://CAIRN/123/Builder/task/Explore/artifact`.
fn build_artifact_uri(
    conn: &mut diesel::sqlite::SqliteConnection,
    ctx: &RunContext,
    job_id: &str,
) -> Option<String> {
    // Only issue-based jobs get URIs
    let issue_number = ctx.issue_number?;

    // Check if this is a task-spawned job (has parent_job_id)
    if let Some((parent_job_id, agent_config_id, task_index)) = get_task_parent_info(conn, job_id) {
        // Task-spawned job: cairn://PROJECT/123/ParentNode/task/TaskName/artifact
        let parent_node = get_node_name_for_job(conn, &parent_job_id)?;
        let task_name = agent_config_id.unwrap_or_else(|| "Task".to_string());
        // Handle task_index for disambiguation if > 0 (0-indexed, so first task has no suffix)
        let task_suffix = task_index
            .filter(|&i| i > 0)
            .map(|i| format!("-{}", i + 1))
            .unwrap_or_default();
        Some(format!(
            "cairn://{}/{}/{}/task/{}{}/artifact",
            ctx.project_key, issue_number, parent_node, task_name, task_suffix
        ))
    } else {
        // Regular job: cairn://PROJECT/123/NodeName/artifact
        let node_name = get_node_name_for_job(conn, job_id)?;
        Some(format!(
            "cairn://{}/{}/{}/artifact",
            ctx.project_key, issue_number, node_name
        ))
    }
}

/// Resolve artifact type and output name from three sources (in priority order):
/// 1. Source node's own outputSchema.schemaType
/// 2. Downstream ActionNode's inputSchema.schemaType
/// 3. Downstream ArtifactNode's artifactType
fn resolve_artifact_type_and_output(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
    default_type: &str,
) -> (String, Option<String>) {
    // Get job's recipe_node_id and execution_id
    let job_data: Option<(Option<String>, Option<String>)> = jobs::table
        .find(job_id)
        .select((jobs::recipe_node_id, jobs::execution_id))
        .first(conn)
        .ok();

    let (node_id, execution_id) = match job_data {
        Some((Some(n), Some(e))) => (n, e),
        _ => return (default_type.to_string(), None),
    };

    // Load execution snapshot
    let snapshot = match load_execution_snapshot(conn, &execution_id) {
        Ok(s) => s,
        Err(_) => return (default_type.to_string(), None),
    };

    // Find source node in snapshot
    let source_node = match find_node_in_snapshot(&snapshot, &node_id) {
        Some(n) => n,
        None => return (default_type.to_string(), None),
    };

    // PRIORITY 1: Check source node's own outputSchema
    if source_node.node_type.to_string() == "agent" {
        if let Some(ref agent_cfg) = source_node.agent_config {
            if let Some(ref output_schema) = agent_cfg.output_schema {
                let schema_type = &output_schema.schema_type;
                if !schema_type.is_empty() && schema_type != "custom" {
                    return (schema_type.clone(), None);
                }
            }
        }
    }

    // PRIORITY 2 & 3: Check downstream nodes via context edges
    let context_edges: Vec<_> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.source_node_id == node_id && e.edge_type.to_string() == "context")
        .collect();

    for edge in context_edges {
        let target_node = match find_node_in_snapshot(&snapshot, &edge.target_node_id) {
            Some(n) => n,
            None => continue,
        };

        // PRIORITY 2: Action node with inputSchema
        if target_node.node_type.to_string() == "action" {
            if let Some(ref action_cfg) = target_node.action_config {
                if let Some(ref input_schema) = action_cfg.input_schema {
                    let schema_type = &input_schema.schema_type;
                    if !schema_type.is_empty() && schema_type != "custom" {
                        return (schema_type.clone(), Some(edge.source_handle.clone()));
                    }
                }
            }
        }

        // PRIORITY 3: Artifact node
        if target_node.node_type.to_string() == "artifact" {
            if let Some(ref artifact_cfg) = target_node.artifact_config {
                let art_type = if artifact_cfg.artifact_type.is_empty() {
                    default_type.to_string()
                } else {
                    artifact_cfg.artifact_type.clone()
                };
                return (art_type, Some(edge.source_handle.clone()));
            }
        }
    }

    (default_type.to_string(), None)
}
