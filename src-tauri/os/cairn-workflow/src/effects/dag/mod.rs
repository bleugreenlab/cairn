//! DAG reduction for the effect loop.
//!
//! `reduce_dag` advances the execution DAG and produces typed `WorkflowEffect`s
//! for each operation that requires host resources (process spawning, worktree
//! creation, shell commands, LLM calls).
//!
//! DB reads/writes happen inside `reduce_dag` (claiming pending→running,
//! creating action_run records). Everything that
//! crosses the host boundary is returned as an effect.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::advancement::{
    advance_execution_impl, create_jobs_for_new_nodes, find_ready_condition_nodes,
    load_execution_snapshot, load_job, load_nodes_from_execution, load_project_repo_path,
    run_advancement_db, update_execution_snapshot,
};
use crate::checkpoint_cache::normalize_command;
use crate::db_records::{DbJob, DbRecipeNode};
use crate::models::{
    AgentGitConfig, AgentNodeConfig, BranchMode, ContextNodeConfig, DelegatedOutputContract,
    DelegatedStatus, DelegatedWorkPacket, ExecutionSnapshot, Job, Model, NodePosition, RecipeEdge,
    RecipeEdgeType, RecipeNode, RecipeNodeType, SchemaConfig,
};
use crate::runtime::{WorkflowOwner, WorkflowRuntime};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_config::agents as config_agents;
use cairn_config::derive_unique_task_slug;
use cairn_config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_runtime_selection,
    LaunchSelectionOverride, PresetsConfig,
};
use cairn_db::turso::params;
use uuid::Uuid;

use super::types::{EffectContext, EffectSource, WorkflowEffect};

mod checkpoints;
pub mod delegation;
mod graph;

use checkpoints::*;
use delegation::*;
pub use graph::*;

/// Advance the DAG for an execution and produce effects.
///
/// Returns `(agent_jobs, effects)`:
/// - `agent_jobs`: Agent-type jobs ready to start (host spawns processes)
/// - `effects`: Follow-on effects for non-agent node types
///
/// This is the synchronous replacement for `advance_execution_with_actions`.
/// All DB mutations happen here; host-crossing operations are returned as effects.
///
/// This is the only reduction entry point. It holds the host's execution
/// snapshot guard (`WorkflowHost::execution_snapshot_guard`) for the reduction
/// and nothing else, so top-level advancement and a cascading `AdvanceDag` in
/// the effect loop are serialized with packet writers alike, and no effect — a
/// checkpoint command can run for minutes — ever runs under it.
pub async fn reduce_dag(
    runtime: &WorkflowRuntime,
    execution_id: &str,
) -> Result<(Vec<Job>, Vec<WorkflowEffect>), crate::advancement::AdvanceError> {
    let _guard = runtime.host.execution_snapshot_guard(execution_id).await;
    reduce_dag_under_guard(runtime, execution_id)
}

fn reduce_dag_under_guard(
    runtime: &WorkflowRuntime,
    execution_id: &str,
) -> Result<(Vec<Job>, Vec<WorkflowEffect>), crate::advancement::AdvanceError> {
    // Resolve the execution's OWNING database once (CAIRN-2197). A team
    // execution's rows live in its synced replica, not the private DB; every
    // snapshot/job/node read below threads this handle so the DAG actually
    // advances for team executions instead of failing with "Execution has no
    // snapshot". Fail-closed: a team replica that is not open errors rather than
    // silently resolving the private DB. For a local execution the private DB
    // owns the row, so this is a strict no-op.
    let owning_db = runtime.database(WorkflowOwner::Execution(execution_id))?;

    // Step 0: Re-arm any Blocked command checkpoints whose upstream agent has
    // committed a fix since the last run. This re-pends them so the pending-scan
    // below re-runs the command against the new worktree HEAD. Gated by the
    // SHA-change progress check, upstream-not-running, and the attempt cap, so
    // it is a cheap no-op on every other advance.
    crate::advancement::rearm_blocked_checkpoints(runtime, execution_id)?;

    // Step 0b: Existing action rows are never readiness candidates. A revised
    // failed PR claims its same row and explicitly dispatches the next attempt.
    let rearmed_pr_dispatches: Vec<WorkflowEffect> =
        crate::advancement::rearm_failed_pr_actions(runtime, execution_id)?
            .iter()
            .map(|action| action.effect())
            .collect();

    // Step 1: Materialize pending delegated packets, then advance the DAG.
    expand_delegated_packets(runtime, &owning_db, execution_id)?;
    let ready_jobs = advance_execution_impl(runtime, execution_id)?;

    // Every ready job is now claimed, and no later advancement will see it
    // again. A failure from here on must carry the claimed agent jobs back to
    // the caller instead of dropping them, so they are named before anything
    // else can fail.
    let mut node_map: HashMap<String, DbRecipeNode> = HashMap::new();
    if let Some(first_job) = ready_jobs.first() {
        if let Some(exec_id) = &first_job.execution_id {
            if let Ok(nodes) = load_nodes_from_execution(owning_db.clone(), exec_id) {
                for node in nodes {
                    node_map.insert(node.id.clone(), node);
                }
            }
        }
    }
    let claimed_agent_jobs: Vec<Job> = ready_jobs
        .iter()
        .filter(|job| {
            job.recipe_node_id
                .as_ref()
                .and_then(|node_id| node_map.get(node_id))
                .is_none_or(|node| node.node_type == "agent")
        })
        .cloned()
        .collect();
    reduce_claimed_jobs(
        runtime,
        &owning_db,
        execution_id,
        ready_jobs,
        &node_map,
        rearmed_pr_dispatches,
    )
    .map_err(|message| crate::advancement::AdvanceError {
        message,
        claimed_agent_jobs,
    })
}

/// The part of [`reduce_dag`] that runs after its claim: dispatch the claimed
/// jobs by node type and produce the follow-on effects, after any `effects`
/// produced before the claim.
fn reduce_claimed_jobs(
    runtime: &WorkflowRuntime,
    owning_db: &Arc<LocalDb>,
    execution_id: &str,
    ready_jobs: Vec<Job>,
    node_map: &HashMap<String, DbRecipeNode>,
    mut effects: Vec<WorkflowEffect>,
) -> Result<(Vec<Job>, Vec<WorkflowEffect>), String> {
    // Step 2: Categorize jobs by their node type
    let mut agent_jobs = Vec::new();
    let mut node_jobs: Vec<(DbJob, DbRecipeNode)> = Vec::new();

    {
        for job in ready_jobs {
            if let Some(node_id) = &job.recipe_node_id {
                if let Some(node) = node_map.get(node_id) {
                    let db_job = load_job(owning_db.clone(), &job.id)?
                        .ok_or_else(|| format!("Failed to load job: {}", job.id))?;
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
                handle_checkpoint_node(
                    runtime,
                    owning_db,
                    &db_job,
                    &node,
                    execution_id,
                    &mut effects,
                )?;
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
        let ready_action_nodes =
            crate::advancement::find_ready_action_nodes(owning_db.clone(), execution_id)?;

        for node in ready_action_nodes {
            let action_run_id =
                crate::advancement::create_action_run(runtime, execution_id, &node)?;

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
        let ready_condition_nodes = find_ready_condition_nodes(owning_db.clone(), execution_id)?;

        for node in ready_condition_nodes {
            run_advancement_db({
                let db = owning_db.clone();
                let execution_id = execution_id.to_string();
                let node_id = node.id.clone();
                async move {
                    crate::conditions::gather_condition_context(
                        db.as_ref(),
                        &execution_id,
                        &node_id,
                    )
                    .await
                    .map(|_| ())
                }
            })?;

            effects.push(WorkflowEffect::EvaluateCondition {
                execution_id: execution_id.to_string(),
                node_id: node.id.clone(),
                node_name: node.name.clone(),
                ctx: EffectContext {
                    job_id: None,
                    run_id: None,
                    execution_id: Some(execution_id.to_string()),
                    source: EffectSource::DagAdvancement,
                },
            });
        }
    }

    Ok((agent_jobs, effects))
}
