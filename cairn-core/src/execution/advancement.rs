//! DAG advancement logic for recipe execution.
//!
//! Core execution engine that advances the recipe DAG, resolves inputs from artifacts,
//! determines which jobs are ready to run, and executes action/condition nodes inline.
//!
//! ## Architecture
//!
//! Action node execution is handled via the event system: cairn-core creates the
//! `action_run` record and emits a `"dag-execute-action"` event. The host layer
//! (Tauri, cairn-server) listens for this event, performs the actual execution, and
//! then re-triggers advancement via `"dag-advance"`.
//!
//! This mirrors the existing `"dag-advance"` event pattern already in the Tauri layer.

use crate::diesel_models::{
    DbActionRun, DbArtifact, DbJob, DbRecipeEdge, DbRecipeNode, NewActionRun,
    UpdateActionRunChangeset, UpdateExecutionChangeset, UpdateJobChangeset,
};
use crate::models::Job;
use crate::orchestrator::Orchestrator;
use crate::schema::{action_runs, artifacts, events, executions, issues, jobs, projects, runs};
use crate::services::EventEmitter;
use diesel::prelude::*;
use uuid::Uuid;

use super::checkpoints::{approve_job_inner, format_annotations, reject_job_inner};
use super::conditions::{
    check_checkpoint_cache, count_pending_condition_nodes, evaluate_condition_node,
    find_checkpoint_worktree, find_ready_condition_nodes, gather_condition_context,
    is_condition_edge_satisfied, store_condition_evaluation,
};

/// Resolved input from an upstream job's artifact.
#[derive(Debug, Clone)]
pub struct ResolvedInput {
    pub artifact_type: String,
    pub data: serde_json::Value,
}

/// Resolve all inputs for a job from upstream artifacts via context edges.
pub fn resolve_job_inputs(
    conn: &mut diesel::sqlite::SqliteConnection,
    job: &DbJob,
) -> Result<Vec<ResolvedInput>, String> {
    match &job.recipe_node_id {
        Some(node_id) => resolve_node_job_inputs(conn, job, node_id),
        None => Ok(vec![]),
    }
}

fn resolve_node_job_inputs(
    conn: &mut diesel::sqlite::SqliteConnection,
    job: &DbJob,
    node_id: &str,
) -> Result<Vec<ResolvedInput>, String> {
    use super::dag::{load_edges_from_execution, load_nodes_from_execution};
    use std::collections::HashMap;

    let execution_id = match &job.execution_id {
        Some(id) => id,
        None => return Ok(vec![]),
    };

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let all_edges = load_edges_from_execution(conn, execution_id)?;
    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let context_edges: Vec<&DbRecipeEdge> = all_edges
        .iter()
        .filter(|e| e.target_node_id == node_id && e.edge_type == "context")
        .collect();

    let mut inputs = Vec::new();

    for edge in context_edges {
        let source_node = match node_map.get(edge.source_node_id.as_str()) {
            Some(n) => *n,
            None => continue,
        };

        if source_node.node_type == "trigger" {
            if let Ok(input) = build_trigger_context(conn, job) {
                inputs.push(input);
            }
            continue;
        }

        if source_node.node_type == "context" {
            if let Some(config_str) = &source_node.config {
                if let Ok(config) = serde_json::from_str::<serde_json::Value>(config_str) {
                    if let Some(content) = config.get("content").and_then(|v| v.as_str()) {
                        inputs.push(ResolvedInput {
                            artifact_type: "context".to_string(),
                            data: serde_json::json!({
                                "content": content,
                                "title": source_node.name
                            }),
                        });
                    }
                }
            }
            continue;
        }

        let source_job: Result<DbJob, _> = jobs::table
            .filter(jobs::execution_id.eq(execution_id))
            .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
            .first(conn);

        let source_job = match source_job {
            Ok(j) => j,
            Err(_) => {
                log::warn!(
                    "Source job for node '{}' not found, skipping input",
                    edge.source_node_id
                );
                continue;
            }
        };

        if let Ok(artifact) = get_artifact_for_edge(conn, &source_job.id, &edge.source_handle) {
            let data: serde_json::Value = serde_json::from_str(&artifact.data).unwrap_or_default();
            inputs.push(ResolvedInput {
                artifact_type: artifact.artifact_type,
                data,
            });
        } else if source_job.status == "complete" {
            // Fallback: use the last assistant message as context
            let last_msg = runs::table
                .filter(runs::job_id.eq(&source_job.id))
                .inner_join(events::table.on(events::run_id.eq(runs::id)))
                .filter(events::event_type.eq("assistant"))
                .order(events::sequence.desc())
                .select(events::data)
                .first::<String>(conn)
                .ok()
                .and_then(|data_json| {
                    serde_json::from_str::<crate::claude::stream::TranscriptEvent>(&data_json)
                        .ok()
                        .and_then(|t| t.content)
                });

            if let Some(content) = last_msg {
                inputs.push(ResolvedInput {
                    artifact_type: "task_output".to_string(),
                    data: serde_json::json!({
                        "source_node": source_node.name,
                        "content": content,
                    }),
                });
            }
        }
    }

    Ok(inputs)
}

fn build_trigger_context(
    conn: &mut diesel::sqlite::SqliteConnection,
    job: &DbJob,
) -> Result<ResolvedInput, String> {
    let issue_id = job.issue_id.as_ref().ok_or("Job has no issue_id")?;

    let (title, description): (String, Option<String>) = issues::table
        .find(issue_id)
        .select((issues::title, issues::description))
        .first(conn)
        .map_err(|e| format!("Failed to get issue: {}", e))?;

    Ok(ResolvedInput {
        artifact_type: "trigger_context".to_string(),
        data: serde_json::json!({
            "issue": {
                "id": issue_id,
                "title": title,
                "description": description,
            }
        }),
    })
}

fn get_artifact_for_edge(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
    output_name: &str,
) -> Result<DbArtifact, String> {
    let exact: Result<DbArtifact, _> = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .filter(artifacts::output_name.eq(output_name))
        .order(artifacts::version.desc())
        .first(conn);

    if let Ok(artifact) = exact {
        return Ok(artifact);
    }

    artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .order(artifacts::version.desc())
        .first(conn)
        .map_err(|e| format!("No artifact found for job {}: {}", job_id, e))
}

fn filter_annotations_from_data(data: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = data.as_object() {
        let filtered: serde_json::Map<String, serde_json::Value> = obj
            .iter()
            .filter(|(k, _)| *k != "annotations")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        serde_json::Value::Object(filtered)
    } else {
        data.clone()
    }
}

/// Format resolved inputs as markdown for injection into agent prompt.
pub fn format_resolved_inputs(inputs: &[ResolvedInput]) -> String {
    if inputs.is_empty() {
        return String::new();
    }

    let mut sections = Vec::new();

    for input in inputs {
        let annotations: Vec<serde_json::Value> = input
            .data
            .get("annotations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut section = match input.artifact_type.as_str() {
            "trigger_context" => {
                if let Some(issue) = input.data.get("issue") {
                    let title = issue
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Untitled");
                    let description = issue
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if description.is_empty() {
                        format!("# {}", title)
                    } else {
                        format!("# {}\n\n{}", title, description)
                    }
                } else {
                    serde_json::to_string_pretty(&input.data).unwrap_or_default()
                }
            }
            "context" => {
                if let Some(content) = input.data.get("content").and_then(|v| v.as_str()) {
                    if let Some(title) = input.data.get("title").and_then(|v| v.as_str()) {
                        format!("## {}\n\n{}", title, content)
                    } else {
                        format!("## Context\n\n{}", content)
                    }
                } else {
                    input
                        .data
                        .get("content")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                }
            }
            "plan" => {
                if let Some(content) = input.data.get("content").and_then(|v| v.as_str()) {
                    if let Some(title) = input.data.get("title").and_then(|v| v.as_str()) {
                        format!("**{}**\n\n{}", title, content)
                    } else {
                        content.to_string()
                    }
                } else {
                    let data_without_annotations = filter_annotations_from_data(&input.data);
                    format!(
                        "```json\n{}\n```",
                        serde_json::to_string_pretty(&data_without_annotations).unwrap_or_default()
                    )
                }
            }
            "tasklist" => {
                let mut parts = Vec::new();
                if let Some(objective) = input.data.get("objective").and_then(|v| v.as_str()) {
                    parts.push(format!("**Objective:** {}", objective));
                }
                if let Some(reqs) = input.data.get("requirements").and_then(|v| v.as_array()) {
                    let items: Vec<String> = reqs
                        .iter()
                        .filter_map(|r| r.as_str())
                        .map(|r| format!("- {}", r))
                        .collect();
                    if !items.is_empty() {
                        parts.push(format!("**Requirements:**\n{}", items.join("\n")));
                    }
                }
                if let Some(tasks) = input.data.get("tasks").and_then(|v| v.as_array()) {
                    let items: Vec<String> = tasks
                        .iter()
                        .filter_map(|t| {
                            let id = t.get("id").and_then(|v| v.as_str())?;
                            let title = t.get("title").and_then(|v| v.as_str())?;
                            let deps = t
                                .get("dependencies")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|d| d.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                })
                                .unwrap_or_default();
                            if deps.is_empty() {
                                Some(format!("- **{}** ({})", title, id))
                            } else {
                                Some(format!("- **{}** ({}) — depends on: {}", title, id, deps))
                            }
                        })
                        .collect();
                    parts.push(format!("**Tasks:**\n{}", items.join("\n")));
                }
                parts.join("\n\n")
            }
            _ => {
                let data_without_annotations = filter_annotations_from_data(&input.data);
                format!(
                    "**{}**\n\n```json\n{}\n```",
                    input.artifact_type,
                    serde_json::to_string_pretty(&data_without_annotations).unwrap_or_default()
                )
            }
        };

        if !annotations.is_empty() {
            section.push_str(&format_annotations(&annotations));
        }

        sections.push(section);
    }

    sections.join("\n\n")
}

fn is_job_ready(conn: &mut diesel::sqlite::SqliteConnection, job: &DbJob) -> Result<bool, String> {
    match &job.recipe_node_id {
        Some(node_id) => is_node_job_ready(conn, job, node_id),
        None => Ok(true),
    }
}

fn is_node_job_ready(
    conn: &mut diesel::sqlite::SqliteConnection,
    job: &DbJob,
    node_id: &str,
) -> Result<bool, String> {
    use super::dag::{load_edges_from_execution, load_nodes_from_execution};
    use std::collections::HashMap;

    let execution_id = match &job.execution_id {
        Some(id) => id,
        None => return Ok(true),
    };

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let all_edges = load_edges_from_execution(conn, execution_id)?;
    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let incoming_edges: Vec<&DbRecipeEdge> = all_edges
        .iter()
        .filter(|e| e.target_node_id == node_id && e.edge_type == "control")
        .collect();

    if incoming_edges.is_empty() {
        let trigger_edge_count = all_edges
            .iter()
            .filter(|e| e.target_node_id == node_id && e.edge_type == "control")
            .filter(|e| {
                node_map
                    .get(e.source_node_id.as_str())
                    .map(|n| n.node_type == "trigger")
                    .unwrap_or(false)
            })
            .count();
        return Ok(trigger_edge_count > 0);
    }

    for edge in incoming_edges {
        if edge.target_handle == "continue" {
            continue;
        }

        let source_node_type = node_map
            .get(edge.source_node_id.as_str())
            .map(|n| n.node_type.as_str());

        match source_node_type {
            Some("trigger") => continue,
            Some("action") => {
                let action_run_complete: bool = action_runs::table
                    .filter(action_runs::execution_id.eq(execution_id))
                    .filter(action_runs::recipe_node_id.eq(&edge.source_node_id))
                    .filter(action_runs::status.eq("complete"))
                    .first::<DbActionRun>(conn)
                    .is_ok();
                if !action_run_complete {
                    return Ok(false);
                }
            }
            Some("condition") => {
                if !is_condition_edge_satisfied(conn, execution_id, edge)? {
                    return Ok(false);
                }
            }
            _ => {
                let source_job: Result<DbJob, _> = jobs::table
                    .filter(jobs::execution_id.eq(execution_id))
                    .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
                    .first(conn);
                match source_job {
                    Ok(j) if j.status == "complete" => continue,
                    _ => return Ok(false),
                }
            }
        }
    }

    Ok(true)
}

fn count_pending_action_nodes(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<i64, String> {
    use super::dag::load_nodes_from_execution;

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let total_action_nodes = all_nodes.iter().filter(|n| n.node_type == "action").count() as i64;

    let executed_action_runs: i64 = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .count()
        .get_result(conn)
        .unwrap_or(0);

    Ok(total_action_nodes - executed_action_runs)
}

fn find_ready_action_nodes(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Vec<DbRecipeNode>, String> {
    use super::dag::{load_edges_from_execution, load_nodes_from_execution};
    use std::collections::HashMap;

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let all_edges = load_edges_from_execution(conn, execution_id)?;
    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let action_nodes: Vec<&DbRecipeNode> = all_nodes
        .iter()
        .filter(|n| n.node_type == "action")
        .collect();

    let executed_node_ids: Vec<String> = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .select(action_runs::recipe_node_id)
        .load(conn)
        .unwrap_or_default();

    let mut ready_nodes = Vec::new();
    for node in action_nodes {
        if executed_node_ids.contains(&node.id) {
            continue;
        }
        if super::dag::is_action_node_ready_with_snapshot(
            conn,
            execution_id,
            &node.id,
            &all_edges,
            &node_map,
        )? {
            ready_nodes.push(node.clone());
        }
    }

    Ok(ready_nodes)
}

/// Check if an action/condition node's dependencies are satisfied.
pub fn is_action_node_ready(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    node_id: &str,
    recipe_id: &str,
) -> Result<bool, String> {
    super::dag::is_action_node_ready(conn, execution_id, node_id, recipe_id)
}

/// Advance an execution's DAG: find pending jobs that are now ready.
/// Emits db-change events via `emitter`. Returns newly-ready agent jobs.
pub fn advance_execution_impl(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    emitter: &dyn EventEmitter,
) -> Result<Vec<Job>, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let pending_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::status.eq("pending"))
        .load(conn)
        .map_err(|e| format!("Failed to load pending jobs: {}", e))?;

    let mut newly_ready = Vec::new();

    for job in pending_jobs {
        if is_job_ready(conn, &job)? {
            let changeset = UpdateJobChangeset {
                status: Some("ready"),
                updated_at: Some(now),
                ..Default::default()
            };

            diesel::update(jobs::table.find(&job.id))
                .set(&changeset)
                .execute(conn)
                .map_err(|e| format!("Failed to update job to ready: {}", e))?;

            let updated_job: DbJob = jobs::table
                .find(&job.id)
                .first(conn)
                .map_err(|e| format!("Failed to reload job: {}", e))?;

            newly_ready.push(Job::try_from(updated_job)?);
        }
    }

    // Transition issue from backlog → active when jobs become ready
    if !newly_ready.is_empty() {
        let issue_id: Option<String> = executions::table
            .find(execution_id)
            .select(executions::issue_id)
            .first(conn)
            .ok()
            .flatten();

        if let Some(ref iid) = issue_id {
            let current_status: Option<String> = issues::table
                .find(iid)
                .select(issues::status)
                .first(conn)
                .ok();

            if current_status.as_deref() == Some("backlog") {
                let _ = diesel::update(issues::table.find(iid))
                    .set((issues::status.eq("active"), issues::updated_at.eq(now)))
                    .execute(conn);

                log::info!("Issue {} transitioned from backlog to active", iid);
                let _ = emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "issues", "action": "update"}),
                );
            }
        }
    }

    // Check if all jobs and action_runs are complete → mark execution complete
    let incomplete_jobs: i64 = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::status.ne("complete"))
        .filter(jobs::status.ne("failed"))
        .count()
        .get_result(conn)
        .unwrap_or(1);

    let incomplete_action_runs: i64 = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .filter(action_runs::status.ne("complete"))
        .filter(action_runs::status.ne("failed"))
        .count()
        .get_result(conn)
        .unwrap_or(0);

    let pending_action_nodes = count_pending_action_nodes(conn, execution_id)?;
    let pending_condition_nodes = count_pending_condition_nodes(conn, execution_id)?;

    if incomplete_jobs == 0
        && incomplete_action_runs == 0
        && pending_action_nodes == 0
        && pending_condition_nodes == 0
    {
        let exec_changeset = UpdateExecutionChangeset {
            status: Some("complete".to_string()),
            completed_at: Some(Some(now)),
            snapshot: None,
        };

        diesel::update(executions::table.find(execution_id))
            .set(&exec_changeset)
            .execute(conn)
            .map_err(|e| format!("Failed to update execution: {}", e))?;

        log::info!(
            "Execution {} completed - all jobs, actions, and conditions done",
            execution_id
        );
    }

    Ok(newly_ready)
}

/// Advance execution DAG and handle action/condition/checkpoint nodes inline.
///
/// Returns agent jobs that are ready to start. Checkpoint jobs are handled
/// internally (blocked for approval or auto-approved for programmatic checkpoints).
/// Action nodes are dispatched via the event system for host-layer execution.
pub async fn advance_execution_with_actions(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    // Advance the DAG to find ready jobs
    let ready_jobs = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        advance_execution_impl(&mut conn, execution_id, &*orch.services.emitter)?
    };

    // Categorize jobs by their node type
    let mut agent_jobs = Vec::new();
    let mut node_jobs: Vec<(DbJob, DbRecipeNode)> = Vec::new();

    {
        use super::dag::load_nodes_from_execution;
        use std::collections::HashMap;

        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let mut node_map: HashMap<String, DbRecipeNode> = HashMap::new();
        if let Some(first_job) = ready_jobs.first() {
            if let Some(exec_id) = &first_job.execution_id {
                if let Ok(nodes) = load_nodes_from_execution(&mut conn, exec_id) {
                    for node in nodes {
                        node_map.insert(node.id.clone(), node);
                    }
                }
            }
        }

        for job in ready_jobs {
            if let Some(node_id) = &job.recipe_node_id {
                if let Some(node) = node_map.get(node_id) {
                    let db_job: DbJob = jobs::table
                        .find(&job.id)
                        .first(&mut *conn)
                        .map_err(|e| format!("Failed to load job: {}", e))?;
                    node_jobs.push((db_job, node.clone()));
                    continue;
                }
            }
            agent_jobs.push(job);
        }
    }

    // Handle node-based jobs
    for (db_job, node) in node_jobs {
        match node.node_type.as_str() {
            "checkpoint" => {
                let checkpoint_config: Option<crate::models::CheckpointNodeConfig> = node
                    .config
                    .as_ref()
                    .and_then(|c| serde_json::from_str(c).ok());

                let is_programmatic = checkpoint_config
                    .as_ref()
                    .map(|c| {
                        matches!(
                            c.checkpoint_type,
                            crate::models::CheckpointType::Programmatic
                        )
                    })
                    .unwrap_or(false);

                if is_programmatic {
                    let command = checkpoint_config
                        .as_ref()
                        .and_then(|c| c.command.clone())
                        .unwrap_or_else(|| "exit 0".to_string());

                    let worktree_path = {
                        let mut conn = orch
                            .db
                            .conn
                            .lock()
                            .map_err(|e| format!("Failed to lock database: {}", e))?;
                        find_checkpoint_worktree(&mut conn, &db_job, &node)?
                    };

                    let cached_result = {
                        let mut conn = orch
                            .db
                            .conn
                            .lock()
                            .map_err(|e| format!("Failed to lock database: {}", e))?;
                        check_checkpoint_cache(&mut conn, &db_job, &command, &worktree_path)
                    };

                    let use_cached = matches!(&cached_result, Some((0, _, true)));

                    if use_cached {
                        if let Some((_, sha, _)) = cached_result {
                            log::info!(
                                "Using cached checkpoint result: passed at commit {}",
                                &sha[..7.min(sha.len())]
                            );
                        }
                        log::info!(
                            "Programmatic checkpoint '{}' using cached pass, auto-approving",
                            node.name
                        );
                        let ready = Box::pin(approve_job_inner(orch, &db_job.id, None)).await?;
                        agent_jobs.extend(ready);
                    } else {
                        match super::conditions::execute_programmatic_checkpoint(
                            &worktree_path,
                            &command,
                        )
                        .await
                        {
                            Ok(true) => {
                                log::info!(
                                    "Programmatic checkpoint '{}' passed, auto-approving",
                                    node.name
                                );
                                let ready =
                                    Box::pin(approve_job_inner(orch, &db_job.id, None)).await?;
                                agent_jobs.extend(ready);
                            }
                            Ok(false) => {
                                log::info!(
                                    "Programmatic checkpoint '{}' failed, auto-rejecting",
                                    node.name
                                );
                                let _ = Box::pin(reject_job_inner(
                                    orch,
                                    &db_job.id,
                                    Some(format!("Programmatic check failed: {}", command)),
                                    None,
                                ))
                                .await?;
                            }
                            Err(e) => {
                                log::error!("Programmatic checkpoint '{}' error: {}", node.name, e);
                                mark_job_failed(orch, &db_job.id, &e)?;
                            }
                        }
                    }
                } else {
                    // Approval checkpoint: block for user action
                    let mut conn = orch
                        .db
                        .conn
                        .lock()
                        .map_err(|e| format!("Failed to lock database: {}", e))?;

                    let now = chrono::Utc::now().timestamp() as i32;
                    let changeset = UpdateJobChangeset {
                        status: Some("blocked"),
                        updated_at: Some(now),
                        ..Default::default()
                    };

                    diesel::update(jobs::table.find(&db_job.id))
                        .set(&changeset)
                        .execute(&mut *conn)
                        .map_err(|e| format!("Failed to update job: {}", e))?;

                    let _ = orch.services.emitter.emit(
                        "db-change",
                        serde_json::json!({"table": "jobs", "action": "update"}),
                    );
                }
            }

            "action" => {
                log::warn!(
                    "Found job for action node - this shouldn't happen with new architecture"
                );
            }

            "executor" => {
                // Expand TaskList artifact into task nodes, mark executor job complete
                log::info!("Expanding executor node '{}'", node.name);

                // Resolve project path for agent config loading
                let project_path: Option<std::path::PathBuf> = {
                    let mut conn = orch
                        .db
                        .conn
                        .lock()
                        .map_err(|e| format!("Failed to lock database: {}", e))?;
                    projects::table
                        .find(&db_job.project_id)
                        .select(projects::repo_path)
                        .first::<String>(&mut *conn)
                        .ok()
                        .map(std::path::PathBuf::from)
                };

                let expand_result = super::executor::expand_executor_node(
                    &mut *orch
                        .db
                        .conn
                        .lock()
                        .map_err(|e| format!("Failed to lock database: {}", e))?,
                    execution_id,
                    &node,
                    &db_job.id,
                    &orch.config_dir,
                    project_path.as_deref(),
                    &*orch.services.emitter,
                );

                match expand_result {
                    Ok(_) => {
                        // Cascade: pick up newly ready task jobs
                        let cascade_jobs =
                            Box::pin(advance_execution_with_actions(orch, execution_id)).await?;
                        agent_jobs.extend(cascade_jobs);
                    }
                    Err(e) => {
                        log::error!("Failed to expand executor node '{}': {}", node.name, e);
                        mark_job_failed(orch, &db_job.id, &e)?;
                    }
                }
            }

            "agent" => {
                agent_jobs.push(Job::try_from(db_job)?);
            }

            _ => {
                log::warn!("Unknown node type '{}', skipping", node.node_type);
            }
        }
    }

    // Dispatch ready action nodes via the event system.
    //
    // cairn-core creates the action_run record and emits "dag-execute-action".
    // The host layer (Tauri, cairn-server) listens for this event, executes the action,
    // marks it complete, then re-triggers advancement via "dag-advance".
    let ready_action_nodes = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        find_ready_action_nodes(&mut conn, execution_id)?
    };

    for node in ready_action_nodes {
        let action_run_id = create_action_run(orch, execution_id, &node)?;

        // Emit event for host layer to execute and advance afterward
        let _ = orch.services.emitter.emit(
            "dag-execute-action",
            serde_json::json!({
                "action_run_id": action_run_id,
                "execution_id": execution_id,
                "node_id": node.id,
            }),
        );
    }

    // Evaluate ready condition nodes
    let ready_condition_nodes = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        find_ready_condition_nodes(&mut conn, execution_id)?
    };

    for node in ready_condition_nodes {
        let recipe_id = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            executions::table
                .find(execution_id)
                .select(executions::recipe_id)
                .first::<String>(&mut *conn)
                .map_err(|e| format!("Failed to get execution: {}", e))?
        };

        let context = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            gather_condition_context(&mut conn, execution_id, &node.id, &recipe_id)?
        };

        match evaluate_condition_node(&node, &context) {
            Ok((port, error_msg)) => {
                log::info!(
                    "Condition node '{}' evaluated to port '{}'",
                    node.name,
                    port
                );

                {
                    let mut conn = orch
                        .db
                        .conn
                        .lock()
                        .map_err(|e| format!("Failed to lock database: {}", e))?;
                    store_condition_evaluation(
                        &mut conn,
                        execution_id,
                        &node.id,
                        &port,
                        error_msg.as_deref(),
                        &*orch.services.emitter,
                    )?;
                }

                let cascade_jobs =
                    Box::pin(advance_execution_with_actions(orch, execution_id)).await?;
                agent_jobs.extend(cascade_jobs);
            }
            Err(e) => {
                log::error!("Condition node '{}' failed: {}", node.name, e);
            }
        }
    }

    // Clean up worktrees for non-issue executions that have completed
    cleanup_non_issue_worktrees_if_complete(orch, execution_id)?;

    Ok(agent_jobs)
}

fn cleanup_non_issue_worktrees_if_complete(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let (status, issue_id, project_id): (String, Option<String>, Option<String>) =
        executions::table
            .find(execution_id)
            .select((
                executions::status,
                executions::issue_id,
                executions::project_id,
            ))
            .first(&mut *conn)
            .map_err(|e| format!("Execution not found: {}", e))?;

    if status != "complete" || issue_id.is_some() {
        return Ok(());
    }

    let project_id = match project_id {
        Some(pid) => pid,
        None => return Ok(()),
    };

    let repo_path: String = projects::table
        .find(&project_id)
        .select(projects::repo_path)
        .first(&mut *conn)
        .map_err(|e| format!("Project not found: {}", e))?;

    let worktree_paths: Vec<Option<String>> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::worktree_path.is_not_null())
        .select(jobs::worktree_path)
        .load(&mut *conn)
        .unwrap_or_default();

    drop(conn);

    for wt_path_str in worktree_paths.into_iter().flatten() {
        let wt_path = std::path::PathBuf::from(&wt_path_str);
        if wt_path.exists() {
            match crate::git::worktree::remove_worktree(&repo_path, &wt_path, true) {
                Ok(_) => {
                    log::info!(
                        "Cleaned up worktree for non-issue execution {}: {}",
                        execution_id,
                        wt_path.display()
                    );
                }
                Err(e) => {
                    log::warn!(
                        "Failed to cleanup worktree {} for execution {}: {}",
                        wt_path.display(),
                        execution_id,
                        e
                    );
                }
            }
        }
    }

    Ok(())
}

fn mark_job_failed(orch: &Orchestrator, job_id: &str, error: &str) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

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

    log::error!("Job {} failed: {}", job_id, error);

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    Ok(())
}

fn create_action_run(
    orch: &Orchestrator,
    execution_id: &str,
    node: &DbRecipeNode,
) -> Result<String, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let (issue_id, project_id): (Option<String>, Option<String>) = executions::table
        .find(execution_id)
        .select((executions::issue_id, executions::project_id))
        .first(&mut *conn)
        .map_err(|e| format!("Failed to get execution: {}", e))?;

    let project_id = match project_id {
        Some(pid) => pid,
        None => {
            if let Some(ref iid) = issue_id {
                issues::table
                    .find(iid)
                    .select(issues::project_id)
                    .first(&mut *conn)
                    .map_err(|e| format!("Failed to get issue project: {}", e))?
            } else {
                return Err("No project_id found for action_run".to_string());
            }
        }
    };

    let parent_job_id = find_parent_job_id(&mut conn, execution_id, &node.id)?;

    let node_config: crate::models::ActionNodeConfig = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok())
        .ok_or("Action node has no valid config")?;
    let action_config_id = node_config
        .action_config_id
        .unwrap_or_else(|| format!("builtin:{}", node_config.action));

    let now = chrono::Utc::now().timestamp() as i32;
    let action_run_id = Uuid::new_v4().to_string();

    let new_action_run = NewActionRun {
        id: &action_run_id,
        execution_id,
        recipe_node_id: &node.id,
        action_config_id: &action_config_id,
        issue_id: issue_id.as_deref(),
        project_id: &project_id,
        status: "pending",
        inputs: None,
        output: None,
        error_message: None,
        started_at: Some(now),
        completed_at: None,
        created_at: now,
        parent_job_id: parent_job_id.as_deref(),
    };

    diesel::insert_into(action_runs::table)
        .values(&new_action_run)
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to insert action_run: {}", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "insert"}),
    );

    Ok(action_run_id)
}

fn find_parent_job_id(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    recipe_node_id: &str,
) -> Result<Option<String>, String> {
    use super::dag::load_edges_from_execution;

    let edges = load_edges_from_execution(conn, execution_id)?;

    let control_edge = edges
        .iter()
        .find(|e| e.edge_type == "control" && e.target_node_id == recipe_node_id);

    if let Some(edge) = control_edge {
        let parent_job: Option<DbJob> = jobs::table
            .filter(jobs::execution_id.eq(execution_id))
            .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
            .first(conn)
            .ok();
        return Ok(parent_job.map(|j| j.id));
    }

    Ok(None)
}

pub fn mark_action_run_failed(
    orch: &Orchestrator,
    action_run_id: &str,
    error: &str,
) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let now = chrono::Utc::now().timestamp() as i32;
    let changeset = UpdateActionRunChangeset {
        status: Some("failed"),
        error_message: Some(Some(error)),
        completed_at: Some(Some(now)),
        ..Default::default()
    };

    diesel::update(action_runs::table.find(action_run_id))
        .set(&changeset)
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to update action_run: {}", e))?;

    log::error!("Action run {} failed: {}", action_run_id, error);

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "update"}),
    );

    Ok(())
}
