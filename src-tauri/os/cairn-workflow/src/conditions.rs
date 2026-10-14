//! Condition node evaluation for recipe execution.
//!
//! Handles conditional branching logic in recipes by evaluating conditions
//! against upstream artifact context.

use cairn_common::ids;

use crate::db_records::{DbRecipeEdge, DbRecipeNode};
use crate::models::ConditionOutcome;
use crate::runtime::{AiConditionRequest, WorkflowHost};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_platform::services::emitter::EventEmitter;

/// Load a condition node from an execution snapshot.
pub async fn load_condition_node(
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
        .map(|node| crate::dag::recipe_node_to_db(node, &recipe_id))
        .find(|node| node.id == node_id)
        .ok_or_else(|| format!("Condition node not found in execution snapshot: {node_id}"))
}

/// Gather context from upstream artifacts for condition evaluation.
pub async fn gather_condition_context(
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
        .map(|node| crate::dag::recipe_node_to_db(node, &recipe_id))
        .collect();
    let all_edges: Vec<DbRecipeEdge> = snapshot
        .recipe
        .edges
        .iter()
        .map(|edge| crate::dag::recipe_edge_to_db(edge, &recipe_id))
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

/// What evaluating a condition decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionVerdict {
    /// Routed to `port`. `error` is set when evaluation failed and `useDefault`
    /// took the configured default port instead.
    Routed { port: String, error: Option<String> },
    /// Routed nowhere; `reason` is shown verbatim wherever the row renders.
    Blocked { reason: String },
}

impl ConditionVerdict {
    pub fn outcome(&self) -> ConditionOutcome {
        match self {
            ConditionVerdict::Routed { .. } => ConditionOutcome::Routed,
            ConditionVerdict::Blocked { .. } => ConditionOutcome::Blocked,
        }
    }
}

/// Evaluate a condition node to a verdict.
///
/// AI conditions are answered by the host; the answer is matched to a port
/// here. Every failure that does not end on a port is a `Blocked` verdict, so
/// the caller always has something to record.
pub async fn evaluate_condition_node(
    host: &dyn WorkflowHost,
    execution_id: &str,
    node: &DbRecipeNode,
    context: &serde_json::Value,
) -> ConditionVerdict {
    let Some(config) = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str::<crate::models::ConditionNodeConfig>(c).ok())
    else {
        return ConditionVerdict::Blocked {
            reason: "Condition node has no valid config".to_string(),
        };
    };

    let result = match config.condition_type {
        crate::models::ConditionType::Programmatic => {
            let Some(expression) = config.expression.as_ref() else {
                return ConditionVerdict::Blocked {
                    reason: "Programmatic condition requires expression".to_string(),
                };
            };
            crate::condition::evaluate_programmatic_condition(expression, context, &config.ports)
        }
        crate::models::ConditionType::Ai => {
            let Some(question) = config.question.as_ref() else {
                return ConditionVerdict::Blocked {
                    reason: "AI condition requires question".to_string(),
                };
            };
            let request = AiConditionRequest {
                execution_id: execution_id.to_string(),
                node_id: node.id.clone(),
                node_name: node.name.clone(),
                question: question.clone(),
                ports: config.ports.clone(),
                context: crate::condition::serialize_context_for_ai(context),
                model: config.model.clone(),
            };
            host.complete_condition(request).await.and_then(|answer| {
                crate::condition::match_ai_condition_answer(&answer, &config.ports)
            })
        }
    };

    match result {
        Ok(port) => ConditionVerdict::Routed { port, error: None },
        Err(e) => match config.on_error {
            crate::models::ConditionErrorBehavior::UseDefault => match config.default_port {
                Some(default_port) => {
                    log::warn!(
                        "Condition '{}' failed, using default port '{}': {}",
                        node.name,
                        default_port,
                        e
                    );
                    ConditionVerdict::Routed {
                        port: default_port,
                        error: Some(e),
                    }
                }
                None => ConditionVerdict::Blocked {
                    reason: format!(
                        "onError is useDefault but no defaultPort is configured; evaluation failed: {e}"
                    ),
                },
            },
            crate::models::ConditionErrorBehavior::Block => ConditionVerdict::Blocked { reason: e },
        },
    }
}

/// Store a condition evaluation row, re-project the owning issue's status, and
/// emit db-change events.
///
/// A blocked verdict stores `result_port = ''` because the column is NOT NULL;
/// readers map that to no port (see the `condition_evaluation_outcome`
/// migration). The issue recompute runs in
/// the same write because a blocked condition puts the issue in `waiting` and a
/// routed re-evaluation takes it back out, and no job transition will.
///
/// The insert is guarded, and the guard is what makes a decision final: it is
/// refused when the node already has a routed row, or when the execution is no
/// longer live (superseded, or its issue resolved). So at most one routed row
/// exists per node however evaluations interleave, an evaluation that finishes
/// after its execution was fenced records nothing, and nothing advances on it.
pub async fn store_condition_evaluation(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
    verdict: &ConditionVerdict,
    emitter: &dyn EventEmitter,
) -> Result<StoredEvaluation, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let eval_id = ids::mint_child(execution_id);
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    let outcome = verdict.outcome().as_str();
    let (result_port, error_message) = match verdict {
        ConditionVerdict::Routed { port, error } => (port.clone(), error.clone()),
        ConditionVerdict::Blocked { reason } => (String::new(), Some(reason.clone())),
    };

    let stored = db
        .write(|conn| {
            let eval_id = eval_id.clone();
            let execution_id = execution_id.clone();
            let node_id = node_id.clone();
            let result_port = result_port.clone();
            let error_message = error_message.clone();
            Box::pin(async move {
                let inserted = conn
                    .execute(
                        concat!(
                            "INSERT INTO condition_evaluations(
                            id, execution_id, recipe_node_id, outcome, result_port,
                            raw_result, error_message, evaluated_at
                         )
                         SELECT ?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7
                         FROM executions e
                         WHERE e.id = ?2
                           AND NOT EXISTS (
                               SELECT 1 FROM condition_evaluations decided
                               WHERE decided.execution_id = ?2
                                 AND decided.recipe_node_id = ?3
                                 AND decided.outcome = 'routed')
                           AND ",
                            cairn_db::live_execution_sql!()
                        ),
                        (
                            eval_id.as_str(),
                            execution_id.as_str(),
                            node_id.as_str(),
                            outcome,
                            result_port.as_str(),
                            error_message.as_deref(),
                            now,
                        ),
                    )
                    .await?;
                let issue_id = crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT issue_id FROM executions WHERE id = ?1",
                    (execution_id.as_str(),),
                )
                .await?;
                if inserted == 0 {
                    return Ok(StoredEvaluation {
                        accepted: false,
                        issue_id,
                    });
                }
                // A still-blocked condition keeps a live execution running, and
                // the execution may already have been marked complete before the
                // condition was evaluated.
                crate::transitions::outcome::recompute_execution_status_conn(conn, &execution_id)
                    .await?;
                if let Some(issue_id) = issue_id.as_deref() {
                    crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id)
                        .await?;
                }
                Ok(StoredEvaluation {
                    accepted: true,
                    issue_id,
                })
            })
        })
        .await
        .map_err(|e| format!("Failed to store condition evaluation: {e}"))?;

    if stored.accepted {
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "condition_evaluations", "action": "insert"}),
        );
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "executions", "action": "update", "executionId": execution_id}),
        );
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "issues", "action": "update", "issueId": stored.issue_id}),
        );
    }

    Ok(stored)
}

/// What [`store_condition_evaluation`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvaluation {
    /// False when the guard refused the row: the node had already routed, or the
    /// execution was no longer live. Nothing was written and nothing may advance.
    pub accepted: bool,
    pub issue_id: Option<String>,
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
    pub passed: bool,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

pub struct ExecutedCheckpoint {
    pub coordinate: String,
    pub output: CheckpointRunOutput,
}
