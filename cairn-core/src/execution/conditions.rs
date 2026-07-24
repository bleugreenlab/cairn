//! Condition node evaluation for recipe execution.
//!
//! Handles conditional branching logic in recipes by evaluating conditions
//! against upstream artifact context.

use cairn_common::ids;

use crate::db_records::{DbRecipeEdge, DbRecipeNode};
use crate::services::EventEmitter;
use crate::storage::{DbError, LocalDb, RowExt};

/// Load a condition node from an execution snapshot.
pub(crate) async fn load_condition_node(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
) -> Result<DbRecipeNode, String> {
    let snapshot = load_execution_snapshot(db, execution_id).await?;
    let recipe_id = snapshot.recipe.id.clone();
    snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| crate::execution::dag::recipe_node_to_db(node, &recipe_id))
        .find(|node| node.id == node_id)
        .ok_or_else(|| format!("Condition node not found in execution snapshot: {node_id}"))
}

/// Gather context from upstream artifacts for condition evaluation.
pub(crate) async fn gather_condition_context(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
) -> Result<serde_json::Value, String> {
    use std::collections::HashMap;

    let snapshot = load_execution_snapshot(db, execution_id).await?;
    let recipe_id = snapshot.recipe.id.clone();
    let all_nodes: Vec<DbRecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| crate::execution::dag::recipe_node_to_db(node, &recipe_id))
        .collect();
    let all_edges: Vec<DbRecipeEdge> = snapshot
        .recipe
        .edges
        .iter()
        .map(|edge| crate::execution::dag::recipe_edge_to_db(edge, &recipe_id))
        .collect();

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
                if let Some(issue) = load_execution_issue(db, execution_id).await? {
                    context.insert(
                        "issue".to_string(),
                        serde_json::json!({
                            "id": issue.id,
                            "title": issue.title,
                            "description": issue.description.unwrap_or_default(),
                        }),
                    );
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

        for artifact in load_source_artifacts(db, execution_id, &edge.source_node_id).await? {
            let key = artifact
                .output_name
                .unwrap_or_else(|| artifact.artifact_type.clone());
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&artifact.data) {
                context.insert(key, data);
            }
        }

        for output in load_action_outputs(db, execution_id, &edge.source_node_id).await? {
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
pub(crate) async fn store_condition_evaluation(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
    result_port: &str,
    error_message: Option<&str>,
    emitter: &dyn EventEmitter,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let eval_id = ids::mint_child(execution_id);
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    let result_port = result_port.to_string();
    let error_message = error_message.map(ToOwned::to_owned);

    db.write(|conn| {
        let eval_id = eval_id.clone();
        let execution_id = execution_id.clone();
        let node_id = node_id.clone();
        let result_port = result_port.clone();
        let error_message = error_message.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO condition_evaluations(
                    id, execution_id, recipe_node_id, result_port,
                    raw_result, error_message, evaluated_at
                 )
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6)",
                (
                    eval_id.as_str(),
                    execution_id.as_str(),
                    node_id.as_str(),
                    result_port.as_str(),
                    error_message.as_deref(),
                    now,
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to store condition evaluation: {e}"))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "condition_evaluations", "action": "insert"}),
    );

    Ok(())
}

async fn load_execution_snapshot(
    db: &LocalDb,
    execution_id: &str,
) -> Result<crate::models::ExecutionSnapshot, String> {
    let execution_id = execution_id.to_string();
    let snapshot_json = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT snapshot FROM executions WHERE id = ?1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                crate::storage::next_opt_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|e| format!("Failed to load execution: {e}"))?
        .ok_or_else(|| "Execution has no snapshot".to_string())?;

    serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {e}"))
}

struct ConditionIssueContext {
    id: String,
    title: String,
    description: Option<String>,
}

async fn load_execution_issue(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<ConditionIssueContext>, String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.id, i.title, i.description
                     FROM executions e
                     INNER JOIN issues i ON i.id = e.issue_id
                     WHERE e.id = ?1",
                    (execution_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(ConditionIssueContext {
                        id: row.text(0)?,
                        title: row.text(1)?,
                        description: row.opt_text(2)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(condition_db_error)
}

struct ConditionArtifactContext {
    artifact_type: String,
    data: String,
    output_name: Option<String>,
}

async fn load_source_artifacts(
    db: &LocalDb,
    execution_id: &str,
    source_node_id: &str,
) -> Result<Vec<ConditionArtifactContext>, String> {
    let execution_id = execution_id.to_string();
    let source_node_id = source_node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let source_node_id = source_node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT a.artifact_type, a.data, a.output_name
                     FROM jobs j
                     INNER JOIN artifacts a ON a.job_id = j.id
                     WHERE j.execution_id = ?1
                       AND j.recipe_node_id = ?2",
                    (execution_id.as_str(), source_node_id.as_str()),
                )
                .await?;
            let mut artifacts = Vec::new();
            while let Some(row) = rows.next().await? {
                artifacts.push(ConditionArtifactContext {
                    artifact_type: row.text(0)?,
                    data: row.text(1)?,
                    output_name: row.opt_text(2)?,
                });
            }
            Ok(artifacts)
        })
    })
    .await
    .map_err(condition_db_error)
}

async fn load_action_outputs(
    db: &LocalDb,
    execution_id: &str,
    source_node_id: &str,
) -> Result<Vec<String>, String> {
    let execution_id = execution_id.to_string();
    let source_node_id = source_node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let source_node_id = source_node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT output
                     FROM action_runs
                     WHERE execution_id = ?1
                       AND recipe_node_id = ?2
                       AND status = 'complete'
                       AND output IS NOT NULL",
                    (execution_id.as_str(), source_node_id.as_str()),
                )
                .await?;
            let mut outputs = Vec::new();
            while let Some(row) = rows.next().await? {
                outputs.push(row.text(0)?);
            }
            Ok(outputs)
        })
    })
    .await
    .map_err(condition_db_error)
}

fn condition_db_error(error: DbError) -> String {
    error.to_string()
}

/// Outcome of running a programmatic checkpoint command. Carries the captured
/// output so a failing checkpoint can record its run history and wake the
/// upstream agent with the actual failure (not just a pass/fail bit).
#[derive(Debug, Clone)]
pub struct CheckpointRunOutput {
    pub(crate) passed: bool,
    pub(crate) exit_code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// Execute a programmatic checkpoint command, capturing its output.
pub(crate) async fn execute_programmatic_checkpoint(
    worktree_path: &str,
    command: &str,
) -> Result<CheckpointRunOutput, String> {
    use tokio::process::Command;

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
        .await
        .map_err(|e| format!("Failed to execute checkpoint command: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !stdout.is_empty() {
        log::info!("Checkpoint stdout: {}", stdout);
    }
    if !stderr.is_empty() {
        log::warn!("Checkpoint stderr: {}", stderr);
    }

    Ok(CheckpointRunOutput {
        passed: output.status.success(),
        exit_code: output.status.code().unwrap_or(-1),
        stdout,
        stderr,
    })
}
