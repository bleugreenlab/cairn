//! DAG advancement logic for recipe execution.
//!
//! Core execution engine that advances the recipe DAG, resolves inputs from artifacts,
//! and determines which jobs are ready to run.
//!
//! ## Architecture
//!
//! `advance_execution_with_actions` is the canonical async entry point. It calls
//! `reduce_dag` (synchronous DB advancement → typed effects) then processes effects
//! through `execute_effects` with the `LegacyExecutor`. All callers — event listeners,
//! direct async calls, scheduler — go through this function, ensuring `reduce_dag` is
//! the single source of truth for DAG advancement.

use crate::diesel_models::{
    DbActionRun, DbArtifact, DbJob, DbRecipeEdge, DbRecipeNode, NewActionRun,
    UpdateActionRunChangeset,
};
use crate::models::{Job, JobStatus};
use crate::orchestrator::Orchestrator;
use crate::schema::{action_runs, artifacts, events, executions, issues, jobs, projects, runs};
use crate::services::EventEmitter;
use diesel::prelude::*;
use uuid::Uuid;

use super::checkpoints::format_annotations;
use super::conditions::is_condition_edge_satisfied;

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
                    serde_json::from_str::<crate::agent_process::stream::TranscriptEvent>(
                        &data_json,
                    )
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
    let mut data = serde_json::json!({});

    // Include event payload if this is an event-triggered execution
    let mut event_payload = None;
    if let Some(ref execution_id) = job.execution_id {
        if let Ok(snapshot) = crate::jobs::queries::load_execution_snapshot(conn, execution_id) {
            if let Some(ref payload) = snapshot.trigger_context.event_payload {
                data["event"] = payload.clone();
                event_payload = Some(payload.clone());
            }
        }
    }

    // For accumulated events, look up all source issues from the events array.
    // For single events, use the job's issue_id as before.
    let is_accumulated = event_payload
        .as_ref()
        .and_then(|p| p.get("accumulated"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if is_accumulated {
        if let Some(events) = event_payload
            .as_ref()
            .and_then(|p| p.get("events"))
            .and_then(|v| v.as_array())
        {
            let mut issues = Vec::new();
            let mut seen_issues = std::collections::HashSet::new();
            for event in events {
                let issue_number = event.get("issueNumber").and_then(|v| v.as_i64());
                let project_key = event
                    .get("projectKey")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(num) = issue_number {
                    let key = format!("{}:{}", project_key, num);
                    if seen_issues.insert(key) {
                        // Look up issue by project key + number
                        if let Ok((id, title, description)) = issues::table
                            .inner_join(projects::table.on(projects::id.eq(issues::project_id)))
                            .filter(projects::key.eq(project_key))
                            .filter(issues::number.eq(num as i32))
                            .select((issues::id, issues::title, issues::description))
                            .first::<(String, String, Option<String>)>(conn)
                        {
                            issues.push(serde_json::json!({
                                "id": id,
                                "key": format!("{}-{}", project_key, num),
                                "title": title,
                                "description": description,
                            }));
                        }
                    }
                }
            }
            if !issues.is_empty() {
                data["issues"] = serde_json::json!(issues);
            }
        }
    } else if let Some(issue_id) = &job.issue_id {
        if let Ok((title, description)) = issues::table
            .find(issue_id)
            .select((issues::title, issues::description))
            .first::<(String, Option<String>)>(conn)
        {
            data["issue"] = serde_json::json!({
                "id": issue_id,
                "title": title,
                "description": description,
            });
        }
    }

    Ok(ResolvedInput {
        artifact_type: "trigger_context".to_string(),
        data,
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
                let has_event = input.data.get("event").is_some();
                let mut parts = Vec::new();

                // Accumulated triggers: multiple source issues
                if let Some(issues_arr) = input.data.get("issues").and_then(|v| v.as_array()) {
                    let mut section = "## Source Issues\n\nThe following issues' jobs contributed to this batch:\n".to_string();
                    for issue in issues_arr {
                        let key = issue.get("key").and_then(|v| v.as_str()).unwrap_or("???");
                        let title = issue
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Untitled");
                        let description = issue
                            .get("description")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        section.push_str(&format!("\n### {} — {}", key, title));
                        if !description.is_empty() {
                            section.push_str(&format!("\n\n{}", description));
                        }
                    }
                    parts.push(section);
                } else if let Some(issue) = input.data.get("issue") {
                    // Single event trigger: one source issue
                    let title = issue
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Untitled");
                    let description = issue
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if has_event {
                        let mut section = format!(
                            "## Source Issue\n\nThe following issue's job has ended. This is the issue that was worked on:\n\n**{}**",
                            title
                        );
                        if !description.is_empty() {
                            section.push_str(&format!("\n\n{}", description));
                        }
                        parts.push(section);
                    } else if description.is_empty() {
                        parts.push(format!("# {}", title));
                    } else {
                        parts.push(format!("# {}\n\n{}", title, description));
                    }
                }

                if let Some(event) = input.data.get("event") {
                    parts.push(format!(
                        "## Trigger Event\n\n```json\n{}\n```",
                        serde_json::to_string_pretty(event).unwrap_or_default()
                    ));
                }

                if parts.is_empty() {
                    serde_json::to_string_pretty(&input.data).unwrap_or_default()
                } else {
                    parts.join("\n\n")
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

pub fn find_ready_action_nodes(
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
    trigger_tx: &tokio::sync::broadcast::Sender<crate::models::TriggerEvent>,
) -> Result<Vec<Job>, String> {
    let pending_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::status.eq("pending"))
        .load(conn)
        .map_err(|e| format!("Failed to load pending jobs: {}", e))?;

    let mut newly_ready = Vec::new();

    for job in pending_jobs {
        if is_job_ready(conn, &job)? {
            // Use transition_job for validated Pending→Ready transition
            crate::transitions::transition_job(
                conn,
                emitter,
                &job.id,
                JobStatus::Ready,
                trigger_tx,
            )
            .map_err(|e| format!("Failed to transition job to ready: {}", e))?;

            let updated_job: DbJob = jobs::table
                .find(&job.id)
                .first(conn)
                .map_err(|e| format!("Failed to reload job: {}", e))?;

            newly_ready.push(Job::try_from(updated_job)?);
        }
    }

    Ok(newly_ready)
}

/// Advance execution DAG and handle action/condition/checkpoint nodes.
///
/// Returns agent jobs that are ready to start. All operations flow through
/// the typed effects system: `reduce_dag` produces effects, `execute_effects`
/// processes them through the core effect loop. Actions, checkpoints,
/// conditions, and worktree creation are all handled inline by the loop.
///
/// This is the canonical entry point for DAG advancement. All callers —
/// event listeners, direct async calls, scheduler — go through this function,
/// which means all paths use `reduce_dag` as the single source of truth.
pub async fn advance_execution_with_actions(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    use crate::effects::dag::reduce_dag;
    use crate::effects::run::execute_effects;
    use crate::effects::types::WorkflowEffect;

    // Step 1: Synchronous DAG reduction — produces typed effects
    let (mut agent_jobs, effects) = reduce_dag(orch, execution_id)?;

    // Step 2: Separate StartAgentJobs from other effects (we accumulate them)
    let mut remaining_effects = Vec::new();
    for effect in effects {
        match effect {
            WorkflowEffect::StartAgentJobs(jobs) => {
                agent_jobs.extend(jobs);
            }
            other => remaining_effects.push(other),
        }
    }

    // Step 3: Execute remaining effects through the core effect loop.
    // Cascading StartAgentJobs are handled by the executor directly.
    if !remaining_effects.is_empty() {
        let report = execute_effects(orch, remaining_effects).await?;

        log::debug!(
            "Effect loop completed: {} effects executed for execution {}",
            report.effects_executed,
            &execution_id[..execution_id.len().min(8)]
        );
    }

    Ok(agent_jobs)
}

pub fn cleanup_non_issue_worktrees_if_complete(
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

/// Create a dedicated worktree for the executor job.
///
/// Tasks and the collector inherit this worktree via `WorktreeMode::Inherit`.
/// The branch is named `agent/{PROJECT}-{ISSUE}-executor-{SEQ}`.
pub fn create_executor_worktree(orch: &Orchestrator, db_job: &DbJob) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    // Load project info
    let (repo_path, project_key): (String, String) = projects::table
        .find(&db_job.project_id)
        .select((projects::repo_path, projects::key))
        .first(&mut *conn)
        .map_err(|e| format!("Project not found: {}", e))?;

    // Display ID (issue number)
    let display_id: String = if let Some(ref iid) = db_job.issue_id {
        let num: i32 = issues::table
            .find(iid)
            .select(issues::number)
            .first(&mut *conn)
            .unwrap_or(0);
        num.to_string()
    } else {
        "0".to_string()
    };

    // Compute unique sequence number
    let seq: i64 = if let Some(ref iid) = db_job.issue_id {
        jobs::table
            .filter(jobs::issue_id.eq(iid))
            .filter(jobs::branch.is_not_null())
            .count()
            .get_result(&mut *conn)
            .unwrap_or(0)
    } else if let Some(ref exec_id) = db_job.execution_id {
        jobs::table
            .filter(jobs::execution_id.eq(exec_id))
            .filter(jobs::branch.is_not_null())
            .count()
            .get_result(&mut *conn)
            .unwrap_or(0)
    } else {
        0
    };

    let branch = format!("agent/{}-{}-executor-{}", project_key, display_id, seq);
    let wt_path = dirs::home_dir()
        .ok_or("Could not find home directory")?
        .join(".cairn")
        .join("worktrees")
        .join(format!("{}-{}-executor-{}", project_key, display_id, seq));

    drop(conn);

    super::jobs::prepare_worktree_for_job(orch, &repo_path, &wt_path, &branch, "HEAD")?;

    let wt_path_str = wt_path.to_string_lossy().to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        diesel::update(jobs::table.find(&db_job.id))
            .set((
                jobs::worktree_path.eq(Some(&wt_path_str)),
                jobs::branch.eq(Some(&branch)),
                jobs::updated_at.eq(now),
            ))
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to update executor job worktree: {}", e))?;
    }

    log::info!(
        "Executor worktree created: {} (branch: {})",
        wt_path_str,
        branch
    );

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    Ok(())
}

pub fn mark_job_failed(orch: &Orchestrator, job_id: &str, error: &str) -> Result<(), String> {
    let run_id: Option<String> = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        crate::transitions::transition_job(
            &mut conn,
            &*orch.services.emitter,
            job_id,
            crate::models::JobStatus::Failed,
            &orch.trigger_events,
        )
        .map_err(|e| e.to_string())?;

        // Find the latest run for this job to attach the error event
        runs::table
            .filter(runs::job_id.eq(job_id))
            .order(runs::created_at.desc())
            .select(runs::id)
            .first::<String>(&mut *conn)
            .ok()
    };

    log::error!("Job {} failed: {}", job_id, error);

    if let Some(run_id) = run_id {
        crate::orchestrator::session::insert_error_event(orch, &run_id, None, error);
    }

    Ok(())
}

pub fn create_action_run(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{DbEvent, NewJob, NewRun};
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

    fn create_job(conn: &mut diesel::sqlite::SqliteConnection, project_id: &str) -> String {
        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id: &id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "running",
            agent_config_id: None,
            issue_id: None,
            project_id,
            task_description: Some("Test job"),
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

    fn create_run(conn: &mut diesel::sqlite::SqliteConnection, job_id: &str) -> String {
        let id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id: &id,
            issue_id: None,
            project_id: None,
            job_id: Some(job_id),
            status: Some("live"),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
            chat_id: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .expect("Failed to create test run");
        id
    }

    #[test]
    fn mark_job_failed_inserts_error_event_when_run_exists() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let orch = test_orchestrator(conn);

        let (job_id, run_id) = {
            let mut conn = orch.db.conn.lock().unwrap();
            let job_id = create_job(&mut conn, &project_id);
            let run_id = create_run(&mut conn, &job_id);
            (job_id, run_id)
        };

        let result = mark_job_failed(&orch, &job_id, "Worktree creation failed");
        assert!(result.is_ok());

        // Verify the job was marked failed
        let mut conn = orch.db.conn.lock().unwrap();
        let status: String = jobs::table
            .find(&job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        assert_eq!(status, "failed");

        // Verify an error event was inserted for the run
        let error_events: Vec<DbEvent> = events::table
            .filter(events::run_id.eq(&run_id))
            .filter(events::event_type.eq("system:error"))
            .load(&mut *conn)
            .unwrap();

        assert_eq!(error_events.len(), 1);
        let data: serde_json::Value = serde_json::from_str(&error_events[0].data).unwrap();
        assert_eq!(data["content"], "Worktree creation failed");
        assert_eq!(data["isError"], true);
    }

    #[test]
    fn mark_job_failed_succeeds_without_run() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let orch = test_orchestrator(conn);

        let job_id = {
            let mut conn = orch.db.conn.lock().unwrap();
            create_job(&mut conn, &project_id)
        };

        // No run created — mark_job_failed should still succeed
        let result = mark_job_failed(&orch, &job_id, "No run available");
        assert!(result.is_ok());

        // Job should be failed
        let mut conn = orch.db.conn.lock().unwrap();
        let status: String = jobs::table
            .find(&job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        assert_eq!(status, "failed");

        // No error events should exist (no run to attach them to)
        let event_count: i64 = events::table.count().get_result(&mut *conn).unwrap();
        assert_eq!(event_count, 0);
    }

    #[test]
    fn mark_job_failed_uses_latest_run() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let orch = test_orchestrator(conn);

        let (job_id, _old_run_id, new_run_id) = {
            let mut conn = orch.db.conn.lock().unwrap();
            let job_id = create_job(&mut conn, &project_id);

            // Create an older run
            let old_id = Uuid::new_v4().to_string();
            let old_time = chrono::Utc::now().timestamp() as i32 - 100;
            diesel::insert_into(runs::table)
                .values(&NewRun {
                    id: &old_id,
                    issue_id: None,
                    project_id: None,
                    job_id: Some(&job_id),
                    status: Some("completed"),
                    session_id: None,
                    error_message: None,
                    started_at: Some(old_time),
                    exited_at: Some(old_time + 50),
                    created_at: old_time,
                    updated_at: old_time,
                    backend: None,
                    exit_reason: None,
                    start_mode: None,
                    chat_id: None,
                })
                .execute(&mut *conn)
                .unwrap();

            // Create a newer run
            let new_id = Uuid::new_v4().to_string();
            let new_time = chrono::Utc::now().timestamp() as i32;
            diesel::insert_into(runs::table)
                .values(&NewRun {
                    id: &new_id,
                    issue_id: None,
                    project_id: None,
                    job_id: Some(&job_id),
                    status: Some("live"),
                    session_id: None,
                    error_message: None,
                    started_at: Some(new_time),
                    exited_at: None,
                    created_at: new_time,
                    updated_at: new_time,
                    backend: None,
                    exit_reason: None,
                    start_mode: None,
                    chat_id: None,
                })
                .execute(&mut *conn)
                .unwrap();

            (job_id, old_id, new_id)
        };

        mark_job_failed(&orch, &job_id, "DAG failure").unwrap();

        // Error event should be attached to the newer run
        let mut conn = orch.db.conn.lock().unwrap();
        let events: Vec<DbEvent> = events::table
            .filter(events::event_type.eq("system:error"))
            .load(&mut *conn)
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].run_id, new_run_id);
    }

    // ---- format_resolved_inputs: event trigger rendering ----

    #[test]
    fn format_trigger_context_with_issue_only() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Fix the bug",
                    "description": "It crashes on startup",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("# Fix the bug"));
        assert!(output.contains("It crashes on startup"));
        assert!(!output.contains("Trigger Event"));
    }

    #[test]
    fn format_trigger_context_with_event_only() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "event": {
                    "jobId": "j1",
                    "status": "complete",
                    "projectId": "p1",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("## Trigger Event"));
        assert!(output.contains("\"status\": \"complete\""));
        // No issue heading (h1), only the event section (h2)
        assert!(!output.starts_with("# "));
        assert!(!output.contains("\n# "));
    }

    #[test]
    fn format_trigger_context_with_issue_and_event() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Deploy monitoring",
                    "description": null,
                },
                "event": {
                    "skillId": "deploy",
                    "skillName": "Deploy",
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        // Event-triggered: issue framed as source context, not open problem
        assert!(output.contains("## Source Issue"));
        assert!(output.contains("**Deploy monitoring**"));
        assert!(output.contains("## Trigger Event"));
        assert!(output.contains("\"skillId\": \"deploy\""));
    }

    #[test]
    fn format_trigger_context_empty_data_falls_through() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({}),
        }];
        let output = format_resolved_inputs(&inputs);
        // Falls through to JSON pretty-print of empty object
        assert!(output.contains("{}"));
    }

    #[test]
    fn format_trigger_context_issue_without_description() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issue": {
                    "id": "iss-1",
                    "title": "Title only",
                    "description": null,
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("# Title only"));
        // Should not have a blank line after title with no description
        assert!(!output.contains("# Title only\n\n\n"));
    }

    #[test]
    fn format_trigger_context_accumulated_multiple_issues() {
        let inputs = vec![ResolvedInput {
            artifact_type: "trigger_context".to_string(),
            data: serde_json::json!({
                "issues": [
                    {
                        "id": "iss-1",
                        "key": "PROJ-42",
                        "title": "Fix login",
                        "description": "Login page crashes",
                    },
                    {
                        "id": "iss-2",
                        "key": "PROJ-55",
                        "title": "Update deps",
                        "description": null,
                    }
                ],
                "event": {
                    "accumulated": true,
                    "groupKey": "quickbuild",
                    "threshold": 2,
                    "eventCount": 2,
                    "events": [],
                }
            }),
        }];
        let output = format_resolved_inputs(&inputs);
        assert!(output.contains("## Source Issues"));
        assert!(output.contains("### PROJ-42 — Fix login"));
        assert!(output.contains("Login page crashes"));
        assert!(output.contains("### PROJ-55 — Update deps"));
        assert!(output.contains("## Trigger Event"));
    }
}
