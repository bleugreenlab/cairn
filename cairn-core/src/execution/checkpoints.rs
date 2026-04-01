//! Checkpoint approval flow for recipe execution.
//!
//! Human-in-the-loop checkpoints where agents wait for user approval
//! before continuing execution. Supports approval, rejection with feedback.
//!
//! These are internal helpers called from advancement logic.
//! The Tauri layer wraps these as `#[tauri::command]` functions.

use diesel::prelude::*;
use uuid::Uuid;

use crate::diesel_models::{DbArtifact, DbJob, DbRecipeEdge};
use crate::models::{AnnotationInput, Job};
use crate::orchestrator::Orchestrator;
use crate::schema::{artifacts, jobs};

// ============================================================================
// Checkpoint type detection — shared by Tauri and cairn-server
// ============================================================================

/// Checkpoint type information for a job's node.
///
/// Used by `update_job_status` in both hosts to determine what happens
/// when an agent completes: nothing, block for approval, or run a command.
#[derive(Debug)]
pub enum JobCheckpointType {
    None,
    Approval,
    Programmatic { command: String },
}

/// Determine the checkpoint type for a job by loading its node from the
/// execution snapshot and parsing the checkpoint config.
///
/// Both Tauri `commands/job/mod.rs` and cairn-server `routes/invoke.rs`
/// had character-for-character identical copies of this logic.
pub fn get_job_checkpoint_type(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> Result<JobCheckpointType, String> {
    use crate::execution::dag::load_nodes_from_execution;
    use std::collections::HashMap;

    use crate::diesel_models::DbRecipeNode;
    let job: DbJob = jobs::table
        .find(job_id)
        .first(conn)
        .map_err(|e| format!("Job not found: {}", e))?;

    let node_id = match &job.recipe_node_id {
        Some(id) => id,
        None => return Ok(JobCheckpointType::None),
    };

    let execution_id = match &job.execution_id {
        Some(id) => id,
        None => return Ok(JobCheckpointType::None),
    };

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let node = node_map
        .get(node_id.as_str())
        .ok_or_else(|| format!("Node not found: {}", node_id))?;

    if node.node_type != "agent" && node.node_type != "action" {
        return Ok(JobCheckpointType::None);
    }

    let config: Option<crate::models::NodeConfig> = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok());

    if let Some(checkpoint) = config.and_then(|c| c.checkpoint) {
        match checkpoint.checkpoint_type {
            crate::models::CheckpointType::Approval => Ok(JobCheckpointType::Approval),
            crate::models::CheckpointType::Programmatic => {
                let command = checkpoint.command.unwrap_or_else(|| "exit 0".to_string());
                Ok(JobCheckpointType::Programmatic { command })
            }
        }
    } else {
        Ok(JobCheckpointType::None)
    }
}

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
    // Validate and store annotations under the db lock
    {
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
    }
    // Lock dropped — apply_step_outcome acquires its own lock

    use crate::transitions::outcome::{
        apply_step_outcome, emit_outcome_effects, OutcomeContext, OutcomeSource, StepOutcome,
    };

    let ctx = OutcomeContext {
        run_id: None,
        source: OutcomeSource::CheckpointApproved,
    };
    let (_result, effects) =
        apply_step_outcome(orch, job_id, &ctx, StepOutcome::Complete).map_err(|e| e.to_string())?;
    // Effects are dispatched through the effect_tx → drainer → executor path.
    // Agent jobs from AdvanceDag are started by the executor directly.
    emit_outcome_effects(orch, effects);
    Ok(vec![])
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
                let is_self_loop_recovery = upstream.id == job.id;

                if !is_self_loop_recovery {
                    // Standalone checkpoint jobs reset to Pending so DAG advancement can
                    // re-arm them after the recovery node completes.
                    crate::transitions::transition_job(
                        &mut conn,
                        &*orch.services.emitter,
                        job_id,
                        crate::models::JobStatus::Pending,
                        &orch.trigger_events,
                    )
                    .map_err(|e| format!("Failed to reset checkpoint: {}", e))?;
                }

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

    // No rejection edge — mark as failed via unified reducer
    // (includes system message + manager wake that were previously missing)
    drop(conn);

    use crate::transitions::outcome::{
        apply_step_outcome, emit_outcome_effects, OutcomeContext, OutcomeSource, StepOutcome,
    };

    let ctx = OutcomeContext {
        run_id: None,
        source: OutcomeSource::CheckpointRejected,
    };
    let (_result, effects) =
        apply_step_outcome(orch, job_id, &ctx, StepOutcome::Failed).map_err(|e| e.to_string())?;
    // Execute effects inline since reject_job_inner is async
    emit_outcome_effects(orch, effects);

    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let db_job: DbJob = jobs::table
        .find(job_id)
        .first(&mut *conn)
        .map_err(|e| format!("Failed to reload job: {}", e))?;

    Job::try_from(db_job)
}

/// Continue a job by creating a new run and resuming its backend session.
/// Used for the rejection flow: resumes the upstream agent with a feedback message.
fn continue_job_for_rejection(
    orch: &Orchestrator,
    job_id: &str,
    message: String,
) -> Result<(), String> {
    crate::execution::jobs::continue_job_impl(orch, job_id, Some(&message), None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{NewArtifact, NewJob};
    use crate::orchestrator::Orchestrator;
    use crate::schema::{jobs, runs};
    use crate::services::testing::{MockProcessSpawner, TestServicesBuilder};
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn test_orchestrator(
        conn: diesel::sqlite::SqliteConnection,
        process: MockProcessSpawner,
    ) -> Orchestrator {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().with_process(process).build());
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

    // =================================================================
    // get_job_checkpoint_type tests
    // =================================================================

    fn insert_snapshot_with_nodes(
        conn: &mut diesel::sqlite::SqliteConnection,
        execution_id: &str,
        issue_id: &str,
        nodes_json: serde_json::Value,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "recipe-1",
                "name": "test-recipe",
                "trigger": "issue",
                "context": "issue",
                "nodes": nodes_json,
                "edges": []
            },
            "agents": {},
            "skills": {},
            "tools": {},
            "triggerContext": {
                "projectId": "test-project",
                "triggerType": "manual"
            },
            "createdAt": 0
        });

        diesel::sql_query(
            "INSERT INTO executions (id, recipe_id, issue_id, status, started_at, seq, snapshot) \
             VALUES (?, 'recipe-1', ?, 'running', ?, 1, ?)",
        )
        .bind::<diesel::sql_types::Text, _>(execution_id)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(issue_id))
        .bind::<diesel::sql_types::Integer, _>(now)
        .bind::<diesel::sql_types::Text, _>(&snapshot.to_string())
        .execute(conn)
        .unwrap();
    }

    fn insert_job_for_checkpoint_test(
        conn: &mut diesel::sqlite::SqliteConnection,
        job_id: &str,
        project_id: &str,
        issue_id: &str,
        execution_id: Option<&str>,
        recipe_node_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let job = NewJob {
            id: job_id,
            execution_id,
            manager_id: None,
            recipe_node_id,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "running",
            agent_config_id: None,
            issue_id: Some(issue_id),
            project_id,
            task_description: None,
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
            .values(&job)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn checkpoint_type_none_when_no_recipe_node_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = Uuid::new_v4().to_string();

        insert_job_for_checkpoint_test(&mut conn, &job_id, &project_id, &issue_id, None, None);

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        assert!(matches!(result, JobCheckpointType::None));
    }

    #[test]
    fn checkpoint_type_none_when_no_execution_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = Uuid::new_v4().to_string();

        // Has recipe_node_id but no execution_id
        insert_job_for_checkpoint_test(
            &mut conn,
            &job_id,
            &project_id,
            &issue_id,
            None,
            Some("some-node"),
        );

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        assert!(matches!(result, JobCheckpointType::None));
    }

    #[test]
    fn checkpoint_type_none_for_non_agent_node() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let execution_id = Uuid::new_v4().to_string();
        let job_id = Uuid::new_v4().to_string();

        // Checkpoint node type — should return None
        insert_snapshot_with_nodes(
            &mut conn,
            &execution_id,
            &issue_id,
            serde_json::json!([
                {
                    "id": "node-1",
                    "name": "My Checkpoint",
                    "nodeType": "checkpoint",
                    "position": { "x": 0.0, "y": 0.0 }
                }
            ]),
        );

        insert_job_for_checkpoint_test(
            &mut conn,
            &job_id,
            &project_id,
            &issue_id,
            Some(&execution_id),
            Some("node-1"),
        );

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        assert!(matches!(result, JobCheckpointType::None));
    }

    #[test]
    fn checkpoint_type_none_when_agent_has_no_checkpoint_config() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let execution_id = Uuid::new_v4().to_string();
        let job_id = Uuid::new_v4().to_string();

        insert_snapshot_with_nodes(
            &mut conn,
            &execution_id,
            &issue_id,
            serde_json::json!([
                {
                    "id": "agent-node",
                    "name": "Builder",
                    "nodeType": "agent",
                    "position": { "x": 0.0, "y": 0.0 },
                    "agentConfig": { "agentConfigId": "builder" }
                }
            ]),
        );

        insert_job_for_checkpoint_test(
            &mut conn,
            &job_id,
            &project_id,
            &issue_id,
            Some(&execution_id),
            Some("agent-node"),
        );

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        assert!(matches!(result, JobCheckpointType::None));
    }

    #[test]
    fn checkpoint_type_approval_for_agent_with_approval_checkpoint() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let execution_id = Uuid::new_v4().to_string();
        let job_id = Uuid::new_v4().to_string();

        // checkpoint is nested inside agentConfig — that's where recipe_node_to_db
        // serializes it into the DbRecipeNode.config JSON
        insert_snapshot_with_nodes(
            &mut conn,
            &execution_id,
            &issue_id,
            serde_json::json!([
                {
                    "id": "agent-node",
                    "name": "Planner",
                    "nodeType": "agent",
                    "position": { "x": 0.0, "y": 0.0 },
                    "agentConfig": {
                        "agentConfigId": "planner",
                        "checkpoint": {
                            "checkpointType": "approval",
                            "prompt": "Review the plan"
                        }
                    }
                }
            ]),
        );

        insert_job_for_checkpoint_test(
            &mut conn,
            &job_id,
            &project_id,
            &issue_id,
            Some(&execution_id),
            Some("agent-node"),
        );

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        assert!(matches!(result, JobCheckpointType::Approval));
    }

    #[test]
    fn checkpoint_type_programmatic_with_command() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let execution_id = Uuid::new_v4().to_string();
        let job_id = Uuid::new_v4().to_string();

        insert_snapshot_with_nodes(
            &mut conn,
            &execution_id,
            &issue_id,
            serde_json::json!([
                {
                    "id": "agent-node",
                    "name": "Builder",
                    "nodeType": "agent",
                    "position": { "x": 0.0, "y": 0.0 },
                    "agentConfig": {
                        "agentConfigId": "builder",
                        "checkpoint": {
                            "checkpointType": "programmatic",
                            "command": "cargo test"
                        }
                    }
                }
            ]),
        );

        insert_job_for_checkpoint_test(
            &mut conn,
            &job_id,
            &project_id,
            &issue_id,
            Some(&execution_id),
            Some("agent-node"),
        );

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        match result {
            JobCheckpointType::Programmatic { command } => {
                assert_eq!(command, "cargo test");
            }
            other => panic!(
                "Expected Programmatic, got {:?}",
                checkpoint_type_name(&other)
            ),
        }
    }

    #[test]
    fn checkpoint_type_programmatic_defaults_command_to_exit_0() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CKT");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let execution_id = Uuid::new_v4().to_string();
        let job_id = Uuid::new_v4().to_string();

        insert_snapshot_with_nodes(
            &mut conn,
            &execution_id,
            &issue_id,
            serde_json::json!([
                {
                    "id": "action-node",
                    "name": "Deploy",
                    "nodeType": "action",
                    "position": { "x": 0.0, "y": 0.0 },
                    "actionConfig": {
                        "action": "deploy",
                        "checkpoint": {
                            "checkpointType": "programmatic"
                        }
                    }
                }
            ]),
        );

        insert_job_for_checkpoint_test(
            &mut conn,
            &job_id,
            &project_id,
            &issue_id,
            Some(&execution_id),
            Some("action-node"),
        );

        let result = get_job_checkpoint_type(&mut conn, &job_id).unwrap();
        match result {
            JobCheckpointType::Programmatic { command } => {
                assert_eq!(command, "exit 0");
            }
            other => panic!(
                "Expected Programmatic, got {:?}",
                checkpoint_type_name(&other)
            ),
        }
    }

    #[test]
    fn checkpoint_type_errors_on_missing_job() {
        let mut conn = test_diesel_conn();
        let result = get_job_checkpoint_type(&mut conn, "nonexistent-job");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Job not found"));
    }

    fn checkpoint_type_name(ct: &JobCheckpointType) -> &'static str {
        match ct {
            JobCheckpointType::None => "None",
            JobCheckpointType::Approval => "Approval",
            JobCheckpointType::Programmatic { .. } => "Programmatic",
        }
    }

    // =================================================================
    // reject_job_inner tests (pre-existing)
    // =================================================================

    fn insert_execution_snapshot(
        conn: &mut diesel::sqlite::SqliteConnection,
        execution_id: &str,
        issue_id: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "recipe-1",
                "name": "test-recipe",
                "trigger": "issue",
                "context": "issue",
                "nodes": [
                    {
                        "id": "planner-node",
                        "name": "planner-node",
                        "nodeType": "agent",
                        "position": { "x": 0.0, "y": 0.0 },
                        "agentConfig": { "agentConfigId": "planner-agent" }
                    },
                    {
                        "id": "checkpoint-node",
                        "name": "checkpoint-node",
                        "nodeType": "checkpoint",
                        "position": { "x": 0.0, "y": 0.0 }
                    }
                ],
                "edges": [
                    {
                        "id": "reject-edge",
                        "edgeType": "control",
                        "sourceNodeId": "checkpoint-node",
                        "sourceHandle": "reject",
                        "targetNodeId": "planner-node",
                        "targetHandle": "in"
                    }
                ]
            },
            "agents": {
                "planner-agent": {
                    "id": "planner-agent",
                    "name": "Planner",
                    "description": "Test planner",
                    "prompt": "You are a planner.",
                    "tools": [],
                    "model": "sonnet",
                    "disallowedTools": [],
                    "skills": [],
                    "permissionMode": null
                }
            },
            "skills": {},
            "tools": {},
            "triggerContext": {
                "projectId": "test-project",
                "triggerType": "manual"
            },
            "createdAt": 0
        });

        diesel::sql_query(
            "INSERT INTO executions (id, recipe_id, issue_id, status, started_at, seq, snapshot) \
             VALUES (?, 'recipe-1', ?, 'running', ?, 1, ?)",
        )
        .bind::<diesel::sql_types::Text, _>(execution_id)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(issue_id))
        .bind::<diesel::sql_types::Integer, _>(now)
        .bind::<diesel::sql_types::Text, _>(&snapshot.to_string())
        .execute(conn)
        .unwrap();
    }

    #[tokio::test]
    async fn reject_recovery_resumes_upstream_job_as_running_before_spawn() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CHK");
        let issue_id = create_test_issue(&mut conn, &project_id, "Checkpoint");
        let execution_id = Uuid::new_v4().to_string();
        insert_execution_snapshot(&mut conn, &execution_id, &issue_id);

        let now = chrono::Utc::now().timestamp() as i32;
        let upstream_job_id = Uuid::new_v4().to_string();
        let checkpoint_job_id = Uuid::new_v4().to_string();

        let upstream_job = NewJob {
            id: &upstream_job_id,
            execution_id: Some(&execution_id),
            manager_id: None,
            recipe_node_id: Some("planner-node"),
            parent_job_id: None,
            worktree_path: Some("/tmp/test-repo"),
            branch: None,
            base_commit: None,
            current_session_id: Some("session-upstream"),
            resume_session_id: None,
            status: "complete",
            agent_config_id: Some("planner-agent"),
            issue_id: Some(&issue_id),
            project_id: &project_id,
            task_description: Some("Planner"),
            created_at: now,
            updated_at: now,
            completed_at: Some(now),
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: Some("sonnet"),
            node_name: Some("Planner"),
            base_branch: None,
            current_turn_id: None,
        };

        let checkpoint_job = NewJob {
            id: &checkpoint_job_id,
            execution_id: Some(&execution_id),
            manager_id: None,
            recipe_node_id: Some("checkpoint-node"),
            parent_job_id: None,
            worktree_path: Some("/tmp/test-repo"),
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "blocked",
            agent_config_id: None,
            issue_id: Some(&issue_id),
            project_id: &project_id,
            task_description: Some("Checkpoint"),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: Some("Checkpoint"),
            base_branch: None,
            current_turn_id: None,
        };

        diesel::insert_into(jobs::table)
            .values(&upstream_job)
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(jobs::table)
            .values(&checkpoint_job)
            .execute(&mut conn)
            .unwrap();

        let artifact_id = Uuid::new_v4().to_string();
        let artifact = NewArtifact {
            id: &artifact_id,
            job_id: Some(&checkpoint_job_id),
            artifact_type: "plan",
            schema_version: 1,
            data: r#"{"content":"original plan"}"#,
            version: 1,
            parent_version_id: None,
            output_name: Some("Plan"),
            created_at: now,
            updated_at: now,
        };
        diesel::insert_into(crate::schema::artifacts::table)
            .values(&artifact)
            .execute(&mut conn)
            .unwrap();

        let mut mock_process = MockProcessSpawner::new();
        mock_process
            .expect_spawn()
            .returning(|_| Err("mock: process spawn blocked".to_string()));

        let orch = test_orchestrator(conn, mock_process);
        let result = reject_job_inner(&orch, &checkpoint_job_id, None, None).await;
        assert!(result.is_err());

        let mut conn = orch.db.conn.lock().unwrap();
        let upstream_status: String = jobs::table
            .find(&upstream_job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        let checkpoint_status: String = jobs::table
            .find(&checkpoint_job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        let run_count: i64 = runs::table
            .filter(runs::job_id.eq(&upstream_job_id))
            .count()
            .get_result(&mut *conn)
            .unwrap();

        assert_eq!(checkpoint_status, "pending");
        assert_eq!(upstream_status, "running");
        assert_eq!(run_count, 1);
    }

    #[tokio::test]
    async fn reject_self_loop_recovery_keeps_embedded_checkpoint_job_running() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CHK");
        let issue_id = create_test_issue(&mut conn, &project_id, "Embedded checkpoint");
        let execution_id = Uuid::new_v4().to_string();

        let now = chrono::Utc::now().timestamp() as i32;
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "recipe-1",
                "name": "test-recipe",
                "trigger": "issue",
                "context": "issue",
                "nodes": [
                    {
                        "id": "planner-node",
                        "name": "planner-node",
                        "nodeType": "agent",
                        "position": { "x": 0.0, "y": 0.0 },
                        "agentConfig": { "agentConfigId": "planner-agent" }
                    }
                ],
                "edges": [
                    {
                        "id": "reject-edge",
                        "edgeType": "control",
                        "sourceNodeId": "planner-node",
                        "sourceHandle": "reject",
                        "targetNodeId": "planner-node",
                        "targetHandle": "continue"
                    }
                ]
            },
            "agents": {
                "planner-agent": {
                    "id": "planner-agent",
                    "name": "Planner",
                    "description": "Test planner",
                    "prompt": "You are a planner.",
                    "tools": [],
                    "model": "sonnet",
                    "disallowedTools": [],
                    "skills": [],
                    "permissionMode": null
                }
            },
            "skills": {},
            "tools": {},
            "triggerContext": {
                "projectId": "test-project",
                "triggerType": "manual"
            },
            "createdAt": 0
        });

        diesel::sql_query(
            "INSERT INTO executions (id, recipe_id, issue_id, status, started_at, seq, snapshot) \
             VALUES (?, 'recipe-1', ?, 'running', ?, 1, ?)",
        )
        .bind::<diesel::sql_types::Text, _>(&execution_id)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(&issue_id))
        .bind::<diesel::sql_types::Integer, _>(now)
        .bind::<diesel::sql_types::Text, _>(&snapshot.to_string())
        .execute(&mut conn)
        .unwrap();

        let planner_job_id = Uuid::new_v4().to_string();
        let planner_job = NewJob {
            id: &planner_job_id,
            execution_id: Some(&execution_id),
            manager_id: None,
            recipe_node_id: Some("planner-node"),
            parent_job_id: None,
            worktree_path: Some("/tmp/test-repo"),
            branch: None,
            base_commit: None,
            current_session_id: Some("session-planner"),
            resume_session_id: None,
            status: "blocked",
            agent_config_id: Some("planner-agent"),
            issue_id: Some(&issue_id),
            project_id: &project_id,
            task_description: Some("Planner"),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: Some("sonnet"),
            node_name: Some("Planner"),
            base_branch: None,
            current_turn_id: None,
        };

        diesel::insert_into(jobs::table)
            .values(&planner_job)
            .execute(&mut conn)
            .unwrap();

        let artifact_id = Uuid::new_v4().to_string();
        let artifact = NewArtifact {
            id: &artifact_id,
            job_id: Some(&planner_job_id),
            artifact_type: "plan",
            schema_version: 1,
            data: r#"{"content":"original plan"}"#,
            version: 1,
            parent_version_id: None,
            output_name: Some("Plan"),
            created_at: now,
            updated_at: now,
        };
        diesel::insert_into(crate::schema::artifacts::table)
            .values(&artifact)
            .execute(&mut conn)
            .unwrap();

        let mut mock_process = MockProcessSpawner::new();
        mock_process
            .expect_spawn()
            .returning(|_| Err("mock: process spawn blocked".to_string()));

        let orch = test_orchestrator(conn, mock_process);
        let result = reject_job_inner(&orch, &planner_job_id, None, None).await;
        assert!(result.is_err());

        let mut conn = orch.db.conn.lock().unwrap();
        let planner_status: String = jobs::table
            .find(&planner_job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        let run_count: i64 = runs::table
            .filter(runs::job_id.eq(&planner_job_id))
            .count()
            .get_result(&mut *conn)
            .unwrap();

        assert_eq!(planner_status, "running");
        assert_eq!(run_count, 1);
    }
}
