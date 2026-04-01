//! DAG reduction for the effect loop.
//!
//! `reduce_dag` advances the execution DAG and produces typed `WorkflowEffect`s
//! for each operation that requires host resources (process spawning, worktree
//! creation, shell commands, LLM calls).
//!
//! DB reads/writes happen inside `reduce_dag` (advancing pending→ready,
//! expanding executor nodes, creating action_run records). Everything that
//! crosses the host boundary is returned as an effect.

use std::path::PathBuf;

use crate::diesel_models::{DbJob, DbRecipeNode};
use crate::execution::advancement::advance_execution_impl;
use crate::execution::conditions::{
    check_checkpoint_cache, find_checkpoint_worktree, find_ready_condition_nodes,
    gather_condition_context,
};
use crate::models::{CheckpointType, Job, JobStatus};
use crate::orchestrator::Orchestrator;
use crate::schema::{executions, jobs, projects};
use diesel::prelude::*;

use super::types::{ConditionSpec, EffectContext, EffectSource, WorkflowEffect};

/// Advance the DAG for an execution and produce effects.
///
/// Returns `(agent_jobs, effects)`:
/// - `agent_jobs`: Agent-type jobs ready to start (host spawns processes)
/// - `effects`: Follow-on effects for non-agent node types
///
/// This is the synchronous replacement for `advance_execution_with_actions`.
/// All DB mutations happen here; host-crossing operations are returned as effects.
pub fn reduce_dag(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<(Vec<Job>, Vec<WorkflowEffect>), String> {
    let mut effects = Vec::new();

    // Step 1: Materialize pending delegated packets, then advance the DAG.
    let ready_jobs = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;
        crate::execution::delegation::expand_delegated_packets(
            &mut conn,
            execution_id,
            &orch.config_dir,
            &*orch.services.emitter,
        )?;
        advance_execution_impl(
            &mut conn,
            execution_id,
            &*orch.services.emitter,
            &orch.trigger_events,
        )?
    };

    // Step 2: Categorize jobs by their node type
    let mut agent_jobs = Vec::new();
    let mut node_jobs: Vec<(DbJob, DbRecipeNode)> = Vec::new();

    {
        use crate::execution::dag::load_nodes_from_execution;
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

    // Step 3: Handle node-based jobs
    for (db_job, node) in node_jobs {
        match node.node_type.as_str() {
            "checkpoint" => {
                handle_checkpoint_node(orch, &db_job, &node, execution_id, &mut effects)?;
            }
            "executor" => {
                handle_executor_node(orch, &db_job, &node, execution_id, &mut effects)?;
            }
            "agent" => {
                agent_jobs.push(Job::try_from(db_job)?);
            }
            "action" => {
                log::warn!(
                    "Found job for action node - this shouldn't happen with new architecture"
                );
            }
            _ => {
                log::warn!("Unknown node type '{}', skipping", node.node_type);
            }
        }
    }

    // Step 4: Dispatch ready action nodes
    {
        let ready_action_nodes = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            crate::execution::advancement::find_ready_action_nodes(&mut conn, execution_id)?
        };

        for node in ready_action_nodes {
            let action_run_id =
                crate::execution::advancement::create_action_run(orch, execution_id, &node)?;

            effects.push(WorkflowEffect::ExecuteAction {
                action_run_id,
                execution_id: execution_id.to_string(),
                node_id: node.id.clone(),
                ctx: EffectContext {
                    job_id: None,
                    run_id: None,
                    execution_id: Some(execution_id.to_string()),
                    source: EffectSource::DagAdvancement,
                },
            });
        }
    }

    // Step 5: Dispatch ready condition nodes
    {
        let ready_condition_nodes = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            find_ready_condition_nodes(&mut conn, execution_id)?
        };

        for node in ready_condition_nodes {
            // Gather condition context (needed for the host executor to evaluate)
            {
                let mut conn = orch
                    .db
                    .conn
                    .lock()
                    .map_err(|e| format!("Failed to lock database: {}", e))?;

                let recipe_id: String = executions::table
                    .find(execution_id)
                    .select(executions::recipe_id)
                    .first(&mut *conn)
                    .map_err(|e| format!("Failed to get execution: {}", e))?;

                // Validate context can be gathered (fail early if not)
                let _context =
                    gather_condition_context(&mut conn, execution_id, &node.id, &recipe_id)?;
            }

            // Extract condition spec from the node config
            let condition_spec = extract_condition_spec(&node);

            effects.push(WorkflowEffect::EvaluateCondition {
                execution_id: execution_id.to_string(),
                node_id: node.id.clone(),
                node_name: node.name.clone(),
                condition: condition_spec,
                ctx: EffectContext {
                    job_id: None,
                    run_id: None,
                    execution_id: Some(execution_id.to_string()),
                    source: EffectSource::DagAdvancement,
                },
            });
        }
    }

    // Step 6: Clean up worktrees for non-issue executions that have completed
    effects.push(WorkflowEffect::CleanupWorktrees {
        execution_id: execution_id.to_string(),
    });

    Ok((agent_jobs, effects))
}

/// Handle a checkpoint node: determine if it's programmatic or approval,
/// and produce the appropriate effect.
fn handle_checkpoint_node(
    orch: &Orchestrator,
    db_job: &DbJob,
    node: &DbRecipeNode,
    execution_id: &str,
    effects: &mut Vec<WorkflowEffect>,
) -> Result<(), String> {
    let checkpoint_config: Option<crate::models::CheckpointNodeConfig> = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok());

    let is_programmatic = checkpoint_config
        .as_ref()
        .map(|c| matches!(c.checkpoint_type, CheckpointType::Programmatic))
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
            find_checkpoint_worktree(&mut conn, db_job, node)?
        };

        let cached_result = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            check_checkpoint_cache(&mut conn, db_job, &command, &worktree_path)
        };

        let cached_pass = matches!(&cached_result, Some((0, _, true)));

        effects.push(WorkflowEffect::RunCheckpointCommand {
            job_id: db_job.id.clone(),
            node_name: node.name.clone(),
            command,
            worktree_path: PathBuf::from(&worktree_path),
            cached_pass,
            ctx: EffectContext {
                job_id: Some(db_job.id.clone()),
                run_id: None,
                execution_id: Some(execution_id.to_string()),
                source: EffectSource::DagAdvancement,
            },
        });
    } else {
        // Approval checkpoint: block for user action via transition_job
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        if let Err(e) = crate::transitions::transition_job(
            &mut conn,
            &*orch.services.emitter,
            &db_job.id,
            JobStatus::Blocked,
            &orch.trigger_events,
        ) {
            log::error!("Failed to block checkpoint job {}: {}", db_job.id, e);
        }
    }

    Ok(())
}

/// Handle an executor node: expand the task list, then produce a worktree
/// creation effect.
fn handle_executor_node(
    orch: &Orchestrator,
    db_job: &DbJob,
    node: &DbRecipeNode,
    execution_id: &str,
    effects: &mut Vec<WorkflowEffect>,
) -> Result<(), String> {
    log::info!("Expanding executor node '{}'", node.name);

    // Resolve project path for agent config loading
    let project_path: Option<PathBuf> = {
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
            .map(PathBuf::from)
    };

    let expand_result = crate::execution::executor::expand_executor_node(
        &mut *orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?,
        execution_id,
        node,
        &db_job.id,
        &orch.config_dir,
        project_path.as_deref(),
        &*orch.services.emitter,
    );

    match expand_result {
        Ok(_) => {
            // Produce effect for worktree creation (host-specific)
            effects.push(WorkflowEffect::CreateExecutorWorktree {
                job_id: db_job.id.clone(),
                execution_id: execution_id.to_string(),
                project_id: db_job.project_id.clone(),
                ctx: EffectContext {
                    job_id: Some(db_job.id.clone()),
                    run_id: None,
                    execution_id: Some(execution_id.to_string()),
                    source: EffectSource::DagAdvancement,
                },
            });
        }
        Err(e) => {
            log::error!("Failed to expand executor node '{}': {}", node.name, e);
            effects.push(WorkflowEffect::MarkJobFailed {
                job_id: db_job.id.clone(),
                error: e,
            });
        }
    }

    Ok(())
}

/// Extract a lightweight condition spec from a DbRecipeNode.
fn extract_condition_spec(node: &DbRecipeNode) -> ConditionSpec {
    let config: Option<serde_json::Value> = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok());

    let condition_type = config
        .as_ref()
        .and_then(|c| c.get("conditionType").and_then(|v| v.as_str()))
        .unwrap_or("programmatic")
        .to_string();

    let expression = config
        .as_ref()
        .and_then(|c| c.get("expression").and_then(|v| v.as_str()))
        .map(|s| s.to_string());

    let question = config
        .as_ref()
        .and_then(|c| c.get("question").and_then(|v| v.as_str()))
        .map(|s| s.to_string());

    let ports = config
        .as_ref()
        .and_then(|c| c.get("ports").and_then(|v| v.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let error_handling = config
        .as_ref()
        .and_then(|c| c.get("errorHandling").and_then(|v| v.as_str()))
        .unwrap_or("use_default")
        .to_string();

    ConditionSpec {
        condition_type,
        expression,
        question,
        ports,
        error_handling,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(config: Option<&str>) -> DbRecipeNode {
        DbRecipeNode {
            id: "cond-1".into(),
            recipe_id: "r-1".into(),
            node_type: "condition".into(),
            name: "Test Condition".into(),
            position_x: 0.0,
            position_y: 0.0,
            config: config.map(|s| s.to_string()),
            created_at: 0,
            updated_at: 0,
            parent_id: None,
        }
    }

    #[test]
    fn extract_condition_spec_full_config() {
        let node = make_node(Some(
            r#"{
            "conditionType": "llm",
            "expression": "x > 0",
            "question": "Is the value positive?",
            "ports": ["yes", "no"],
            "errorHandling": "fail_closed"
        }"#,
        ));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "llm");
        assert_eq!(spec.expression.as_deref(), Some("x > 0"));
        assert_eq!(spec.question.as_deref(), Some("Is the value positive?"));
        assert_eq!(spec.ports, vec!["yes", "no"]);
        assert_eq!(spec.error_handling, "fail_closed");
    }

    #[test]
    fn extract_condition_spec_defaults_on_missing_fields() {
        let node = make_node(Some("{}"));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "programmatic");
        assert!(spec.expression.is_none());
        assert!(spec.question.is_none());
        assert!(spec.ports.is_empty());
        assert_eq!(spec.error_handling, "use_default");
    }

    #[test]
    fn extract_condition_spec_defaults_on_no_config() {
        let node = make_node(None);
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "programmatic");
        assert!(spec.expression.is_none());
        assert!(spec.question.is_none());
        assert!(spec.ports.is_empty());
        assert_eq!(spec.error_handling, "use_default");
    }

    #[test]
    fn extract_condition_spec_invalid_json_uses_defaults() {
        let node = make_node(Some("not valid json"));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "programmatic");
        assert_eq!(spec.error_handling, "use_default");
    }

    #[test]
    fn extract_condition_spec_ignores_non_string_ports() {
        let node = make_node(Some(r#"{"ports": ["yes", 42, "no", null]}"#));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.ports, vec!["yes", "no"]);
    }
}
