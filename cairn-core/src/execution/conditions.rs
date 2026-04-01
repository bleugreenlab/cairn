//! Condition node evaluation for recipe execution.
//!
//! Handles conditional branching logic in recipes by evaluating conditions
//! against upstream artifact context.

use diesel::prelude::*;
use uuid::Uuid;

use crate::diesel_models::{
    DbArtifact, DbCheckpointCommandCache, DbConditionEvaluation, DbIssue, DbJob, DbRecipeEdge,
    DbRecipeNode, NewConditionEvaluation,
};
use crate::execution::cache::{get_current_head_sha, is_worktree_dirty, normalize_command};
use crate::execution::dag::is_action_node_ready;
use crate::schema::{
    action_runs, artifacts, checkpoint_command_cache, condition_evaluations, executions, issues,
    jobs,
};
use crate::services::EventEmitter;

/// Find condition nodes whose dependencies are satisfied and haven't been evaluated yet.
pub(crate) fn find_ready_condition_nodes(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Vec<DbRecipeNode>, String> {
    use crate::execution::dag::load_nodes_from_execution;

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;

    let condition_nodes: Vec<&DbRecipeNode> = all_nodes
        .iter()
        .filter(|n| n.node_type == "condition")
        .collect();

    let evaluated_node_ids: Vec<String> = condition_evaluations::table
        .filter(condition_evaluations::execution_id.eq(execution_id))
        .select(condition_evaluations::recipe_node_id)
        .load(conn)
        .unwrap_or_default();

    // Unused API compat placeholder
    let recipe_id = all_nodes
        .first()
        .map(|n| n.recipe_id.clone())
        .unwrap_or_default();

    let mut ready_nodes = Vec::new();
    for node in condition_nodes {
        if evaluated_node_ids.contains(&node.id) {
            continue;
        }
        if is_action_node_ready(conn, execution_id, &node.id, &recipe_id)? {
            ready_nodes.push(node.clone());
        }
    }

    Ok(ready_nodes)
}

/// Gather context from upstream artifacts for condition evaluation.
pub(crate) fn gather_condition_context(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    node_id: &str,
    _recipe_id: &str,
) -> Result<serde_json::Value, String> {
    use crate::execution::dag::{load_edges_from_execution, load_nodes_from_execution};
    use std::collections::HashMap;

    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let all_edges = load_edges_from_execution(conn, execution_id)?;

    let node_map: HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let context_edges: Vec<&DbRecipeEdge> = all_edges
        .iter()
        .filter(|e| e.target_node_id == node_id && e.edge_type == "context")
        .collect();

    let mut context = serde_json::Map::new();

    for edge in context_edges {
        let source_node = node_map.get(edge.source_node_id.as_str()).copied();

        if let Some(node) = source_node {
            if node.node_type == "trigger" {
                let issue_id: Option<String> = executions::table
                    .find(execution_id)
                    .select(executions::issue_id)
                    .first(conn)
                    .ok()
                    .flatten();

                if let Some(issue_id) = issue_id {
                    if let Ok(issue) = issues::table.find(&issue_id).first::<DbIssue>(conn) {
                        context.insert(
                            "issue".to_string(),
                            serde_json::json!({
                                "id": issue.id,
                                "title": issue.title,
                                "description": issue.description.unwrap_or_default(),
                            }),
                        );
                    }
                }
                continue;
            }

            if node.node_type == "context" {
                if let Some(config_str) = &node.config {
                    if let Ok(config) = serde_json::from_str::<serde_json::Value>(config_str) {
                        if let Some(content) = config.get("content").and_then(|v| v.as_str()) {
                            context.insert(
                                "context".to_string(),
                                serde_json::json!({ "content": content }),
                            );
                        }
                    }
                }
                continue;
            }
        }

        // Find jobs for the source node and get their artifacts
        let job_artifacts: Vec<DbArtifact> = jobs::table
            .inner_join(artifacts::table.on(artifacts::job_id.eq(jobs::id.nullable())))
            .filter(jobs::execution_id.eq(execution_id))
            .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
            .select(artifacts::all_columns)
            .load(conn)
            .unwrap_or_default();

        for artifact in job_artifacts {
            let key = artifact
                .output_name
                .unwrap_or_else(|| artifact.artifact_type.clone());
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&artifact.data) {
                context.insert(key, data);
            }
        }

        let action_outputs: Vec<Option<String>> = action_runs::table
            .filter(action_runs::execution_id.eq(execution_id))
            .filter(action_runs::recipe_node_id.eq(&edge.source_node_id))
            .filter(action_runs::status.eq("complete"))
            .select(action_runs::output)
            .load(conn)
            .unwrap_or_default();

        for output in action_outputs.into_iter().flatten() {
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&output) {
                let key = edge.source_handle.trim_end_matches("-out").to_string();
                context.insert(key, data);
            }
        }
    }

    Ok(serde_json::Value::Object(context))
}

/// Evaluate a condition node and return the result port.
pub(crate) fn evaluate_condition_node(
    completion: &dyn crate::services::CompletionService,
    node: &DbRecipeNode,
    context: &serde_json::Value,
) -> Result<(String, Option<String>), String> {
    let config: crate::models::ConditionNodeConfig = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok())
        .ok_or("Condition node has no valid config")?;

    let result = match config.condition_type {
        crate::models::ConditionType::Programmatic => {
            let expression = config
                .expression
                .as_ref()
                .ok_or("Programmatic condition requires expression")?;
            crate::condition::evaluate_programmatic_condition(expression, context, &config.ports)
        }
        crate::models::ConditionType::Ai => {
            let question = config
                .question
                .as_ref()
                .ok_or("AI condition requires question")?;
            let context_str = crate::condition::serialize_context_for_ai(context);
            crate::condition::evaluate_ai_condition(
                completion,
                question,
                &config.ports,
                &context_str,
                config.model.as_deref(),
            )
        }
    };

    match result {
        Ok(port) => Ok((port, None)),
        Err(e) => match config.on_error {
            crate::models::ConditionErrorBehavior::UseDefault => {
                if let Some(default_port) = config.default_port {
                    log::warn!(
                        "Condition '{}' failed, using default port '{}': {}",
                        node.name,
                        default_port,
                        e
                    );
                    Ok((default_port, Some(e)))
                } else {
                    Err(format!(
                        "Condition failed with no default port configured: {}",
                        e
                    ))
                }
            }
            crate::models::ConditionErrorBehavior::Block => {
                Err(format!("Condition evaluation blocked: {}", e))
            }
        },
    }
}

/// Store a condition evaluation result and emit a db-change event.
pub(crate) fn store_condition_evaluation(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    node_id: &str,
    result_port: &str,
    error_message: Option<&str>,
    emitter: &dyn EventEmitter,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let eval_id = Uuid::new_v4().to_string();

    let new_eval = NewConditionEvaluation {
        id: &eval_id,
        execution_id,
        recipe_node_id: node_id,
        result_port,
        raw_result: None,
        error_message,
        evaluated_at: now,
    };

    diesel::insert_into(condition_evaluations::table)
        .values(&new_eval)
        .execute(conn)
        .map_err(|e| format!("Failed to store condition evaluation: {}", e))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "condition_evaluations", "action": "insert"}),
    );

    Ok(())
}

/// Check if an edge from a condition node is satisfied (matches the evaluated port).
pub(crate) fn is_condition_edge_satisfied(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    edge: &DbRecipeEdge,
) -> Result<bool, String> {
    let evaluation: Option<DbConditionEvaluation> = condition_evaluations::table
        .filter(condition_evaluations::execution_id.eq(execution_id))
        .filter(condition_evaluations::recipe_node_id.eq(&edge.source_node_id))
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to check condition evaluation: {}", e))?;

    match evaluation {
        Some(eval) => Ok(edge.source_handle == eval.result_port),
        None => Ok(false),
    }
}

/// Check if there's a valid cached checkpoint result for a job.
/// Returns Some((exit_code, commit_sha, is_valid)) if cached, None otherwise.
pub(crate) fn check_checkpoint_cache(
    conn: &mut diesel::sqlite::SqliteConnection,
    checkpoint_job: &DbJob,
    command: &str,
    worktree_path: &str,
) -> Option<(i32, String, bool)> {
    let parent_job_id = checkpoint_job.parent_job_id.as_ref()?;
    let normalized = normalize_command(command);

    let cached: DbCheckpointCommandCache = checkpoint_command_cache::table
        .filter(checkpoint_command_cache::job_id.eq(parent_job_id))
        .filter(checkpoint_command_cache::normalized_command.eq(&normalized))
        .order(checkpoint_command_cache::ran_at.desc())
        .first(conn)
        .ok()?;

    let current_sha = get_current_head_sha(worktree_path).ok()?;
    let currently_dirty = is_worktree_dirty(worktree_path).unwrap_or(true);
    let is_valid = cached.commit_sha == current_sha && cached.is_dirty == 0 && !currently_dirty;

    Some((cached.exit_code, cached.commit_sha.clone(), is_valid))
}

/// Find the worktree path for a checkpoint node.
/// For slot checkpoints (with parent_id), finds the parent agent's worktree.
/// Falls back to looking for any worktree in the same execution.
pub(crate) fn find_checkpoint_worktree(
    conn: &mut diesel::sqlite::SqliteConnection,
    job: &DbJob,
    node: &DbRecipeNode,
) -> Result<String, String> {
    let execution_id = job.execution_id.as_ref().ok_or("Job has no execution_id")?;

    if let Some(parent_id) = &node.parent_id {
        let parent_job: Option<DbJob> = jobs::table
            .filter(jobs::execution_id.eq(execution_id))
            .filter(jobs::recipe_node_id.eq(parent_id))
            .first(conn)
            .ok();

        if let Some(pj) = parent_job {
            if let Some(wt) = pj.worktree_path {
                return Ok(wt);
            }
        }
    }

    let worktree_job: Option<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .filter(jobs::worktree_path.is_not_null())
        .first(conn)
        .ok();

    worktree_job
        .and_then(|j| j.worktree_path)
        .ok_or_else(|| "No worktree found for programmatic checkpoint".to_string())
}

/// Execute a programmatic checkpoint command.
/// Returns Ok(true) if command exits with 0, Ok(false) if non-zero.
pub async fn execute_programmatic_checkpoint(
    worktree_path: &str,
    command: &str,
) -> Result<bool, String> {
    use std::process::Command;

    log::info!(
        "Running programmatic checkpoint: '{}' in {}",
        command,
        worktree_path
    );

    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .current_dir(worktree_path)
        .env("PATH", crate::env::get_user_path())
        .output()
        .map_err(|e| format!("Failed to execute checkpoint command: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stdout.is_empty() {
        log::info!("Checkpoint stdout: {}", stdout);
    }
    if !stderr.is_empty() {
        log::warn!("Checkpoint stderr: {}", stderr);
    }

    Ok(output.status.success())
}
