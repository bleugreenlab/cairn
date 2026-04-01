//! Recipe execution tracking and management.
//!
//! Core DB and config operations for recipe executions — listing, creating,
//! and managing the lifecycle of recipe execution records.
//!
//! Functions taking `&mut SqliteConnection` are pure DB operations.
//! Functions taking `&Orchestrator` also need config/settings access.

use diesel::prelude::*;
use std::path::Path;
use uuid::Uuid;

use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot_with_seed_backend, PresetsConfig,
};
use crate::config::{agents as config_agents, get_project_path, recipes as config_recipes};
use crate::config::{file_recipe_to_recipe, get_recipe_from_files};
use crate::diesel_models::NewExecution;
use crate::execution::Initiator;
use crate::models::{
    Execution, ExecutionStatus, RecipeNodeType, RecipeTrigger, SnapshotOverrides, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::schema::executions;
use crate::snapshot::build_execution_snapshot;

/// Find a default recipe from files for a given trigger type.
///
/// If `project_path` is Some and `project_only` is true, only checks project-scoped recipes.
/// If `project_path` is None, only checks workspace-scoped recipes.
fn find_default_recipe_from_files(
    config_dir: &Path,
    project_path: Option<&Path>,
    trigger: RecipeTrigger,
    project_only: bool,
) -> Result<Option<crate::models::Recipe>, String> {
    let recipes = config_recipes::list_recipes(config_dir, project_path)?;

    for result in recipes {
        if let crate::config::ConfigResult::Ok(file_recipe) = result {
            if file_recipe.recipe.is_default && file_recipe.recipe.trigger == trigger {
                if project_only && !file_recipe.is_project_scoped {
                    continue;
                }
                if project_path.is_none() && file_recipe.is_project_scoped {
                    continue;
                }

                let (ws_id, proj_id) = if file_recipe.is_project_scoped {
                    (None, None)
                } else {
                    (Some("default".to_string()), None)
                };

                return Ok(Some(file_recipe_to_recipe(file_recipe, ws_id, proj_id)));
            }
        }
    }

    Ok(None)
}

// Re-export execution queries with _impl aliases for backward compatibility.
pub use super::queries::get_execution_for_issue as get_execution_for_issue_impl;
pub use super::queries::list_executions_for_issue as list_executions_for_issue_impl;

/// Start executing a recipe for an issue.
///
/// Creates an execution record with a snapshot of the recipe and agents captured
/// at the current point in time. Caller is responsible for creating jobs
/// (`create_jobs_for_execution`) and advancing the DAG.
///
/// Optional `overrides` can customize the recipe graph and agents before execution starts.
pub fn start_recipe_execution_impl(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    overrides: Option<SnapshotOverrides>,
    backend: Option<&str>,
    initiator: Option<Initiator>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    // Get project path for file-based recipe loading
    let project_path = get_project_path(&mut conn, project_id)?;

    // Get the recipe (use provided or find default)
    let recipe = match recipe_id {
        Some(id) => get_recipe_from_files(&orch.config_dir, Some(&project_path), id)?,
        None => {
            // Try project default first, then workspace default
            find_default_recipe_from_files(
                &orch.config_dir,
                Some(&project_path),
                RecipeTrigger::Manual,
                true,
            )?
            .or(find_default_recipe_from_files(
                &orch.config_dir,
                None,
                RecipeTrigger::Manual,
                false,
            )?)
            .ok_or("No default recipe found")?
        }
    };

    // Build execution snapshot
    let mut snapshot = build_execution_snapshot(
        &mut conn,
        &orch.config_dir,
        &recipe.id,
        Some(issue_id),
        project_id,
        TriggerType::Manual,
        None,
        backend,
    )?;
    let frozen_presets = snapshot
        .presets
        .as_ref()
        .map(PresetsConfig::from)
        .unwrap_or_else(|| load_effective_presets(&orch.config_dir, Some(&project_path)));

    // Apply overrides if provided
    if let Some(overrides) = overrides {
        if let Some(recipe_override) = overrides.recipe {
            snapshot.recipe = recipe_override;
        }
        if let Some(agents_override) = overrides.agents {
            for (agent_id, agent_snapshot) in agents_override {
                let normalized = resolve_agent_snapshot_with_seed_backend(
                    &crate::config::agents::FileAgent {
                        id: agent_snapshot.id.clone(),
                        name: agent_snapshot.name.clone(),
                        description: agent_snapshot.description.clone(),
                        prompt: agent_snapshot.prompt.clone(),
                        tools: agent_snapshot.tools.clone(),
                        tier: agent_snapshot.tier.clone().or(agent_snapshot.model.clone()),
                        approval_policy: agent_snapshot.approval_policy,
                        filesystem_scope: agent_snapshot.filesystem_scope,
                        disallowed_tools: agent_snapshot.disallowed_tools.clone(),
                        skills: agent_snapshot.skills.clone(),
                        hooks: None,
                        backend_preference: agent_snapshot.backend_preference.clone(),
                        is_project_scoped: true,
                        file_path: std::path::PathBuf::new(),
                    },
                    None,
                    backend,
                    &frozen_presets,
                )?;
                snapshot.agents.insert(agent_id, normalized);
            }
        }

        // Load any new agents referenced by the overridden recipe that aren't in the snapshot
        let new_agent_ids: Vec<String> = snapshot
            .recipe
            .nodes
            .iter()
            .filter(|n| n.node_type == RecipeNodeType::Agent)
            .filter_map(|n| {
                n.agent_config
                    .as_ref()
                    .and_then(|cfg| cfg.agent_config_id.clone())
            })
            .filter(|id| !snapshot.agents.contains_key(id))
            .collect();

        if !new_agent_ids.is_empty() {
            // Load each new agent
            for agent_id in new_agent_ids {
                if let Ok(Some(file_agent)) =
                    config_agents::get_agent(&orch.config_dir, &agent_id, Some(&project_path))
                {
                    let agent_snapshot = resolve_agent_snapshot_with_seed_backend(
                        &file_agent,
                        None,
                        backend,
                        &frozen_presets,
                    )?;
                    snapshot.agents.insert(agent_id.clone(), agent_snapshot);
                }
            }
        }
    }

    let snapshot_json = snapshot.to_json()?;

    // Calculate next seq for this issue
    let next_seq: i32 = executions::table
        .filter(executions::issue_id.eq(issue_id))
        .select(diesel::dsl::max(executions::seq))
        .first::<Option<i32>>(&mut *conn)
        .ok()
        .flatten()
        .map(|max| max + 1)
        .unwrap_or(1);

    // Create execution
    let exec_id = Uuid::new_v4().to_string();
    let new_exec = NewExecution {
        id: &exec_id,
        recipe_id: &recipe.id,
        issue_id: Some(issue_id),
        project_id: None,
        status: "running",
        started_at: now,
        completed_at: None,
        snapshot: Some(&snapshot_json),
        seq: Some(next_seq),
        initiator_sub: initiator.as_ref().map(|i| i.sub.as_str()),
        initiator_auth_mode: initiator.as_ref().map(|i| i.auth_mode.as_str()),
        initiator_org_id: initiator.as_ref().map(|i| i.org_id.as_str()),
        triggered_by: "manual",
    };

    diesel::insert_into(executions::table)
        .values(&new_exec)
        .execute(&mut *conn)
        .map_err(|e| e.to_string())?;

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id,
        issue_id: Some(issue_id.to_string()),
        project_id: None,
        status: ExecutionStatus::Running,
        started_at: now as i64,
        completed_at: None,
        seq: Some(next_seq),
        initiator: initiator.clone(),
        triggered_by: TriggerType::Manual,
    })
}

/// Start executing a recipe manually (no issue, just project).
///
/// Creates an execution record for a manual trigger. Caller is responsible for
/// creating jobs and advancing the DAG.
pub fn start_manual_execution_impl(
    orch: &Orchestrator,
    recipe_id: &str,
    project_id: &str,
    initiator: Option<Initiator>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    // Get project path for file-based recipe loading
    let project_path = get_project_path(&mut conn, project_id)?;

    // Verify recipe exists (load from files)
    let recipe = get_recipe_from_files(&orch.config_dir, Some(&project_path), recipe_id)?;

    // Build execution snapshot
    let snapshot = build_execution_snapshot(
        &mut conn,
        &orch.config_dir,
        &recipe.id,
        None,
        project_id,
        TriggerType::Manual,
        None,
        None,
    )?;
    let snapshot_json = snapshot.to_json()?;

    // Create execution
    let exec_id = Uuid::new_v4().to_string();
    let new_exec = NewExecution {
        id: &exec_id,
        recipe_id: &recipe.id,
        issue_id: None,               // No issue for manual execution
        project_id: Some(project_id), // Store project directly
        status: "running",
        started_at: now,
        completed_at: None,
        snapshot: Some(&snapshot_json),
        seq: None, // No seq for non-issue executions
        initiator_sub: initiator.as_ref().map(|i| i.sub.as_str()),
        initiator_auth_mode: initiator.as_ref().map(|i| i.auth_mode.as_str()),
        initiator_org_id: initiator.as_ref().map(|i| i.org_id.as_str()),
        triggered_by: "manual",
    };

    diesel::insert_into(executions::table)
        .values(&new_exec)
        .execute(&mut *conn)
        .map_err(|e| e.to_string())?;

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id,
        issue_id: None,
        project_id: Some(project_id.to_string()),
        status: ExecutionStatus::Running,
        started_at: now as i64,
        completed_at: None,
        seq: None,
        initiator: initiator.clone(),
        triggered_by: TriggerType::Manual,
    })
}

/// Start a recipe execution triggered by an event (JobEnded or SkillCalled).
///
/// Similar to `start_manual_execution_impl` but:
/// - Sets `trigger_type` to the event's type (JobEnded/SkillCalled)
/// - Stores `event_payload` in TriggerContext
/// - Uses the triggering execution's initiator for credential resolution
/// - Supports optional issue_id (Issue-scoped vs Project-scoped triggers)
pub fn start_event_triggered_execution(
    orch: &Orchestrator,
    recipe: &crate::models::Recipe,
    trigger_type: TriggerType,
    event_payload: serde_json::Value,
    issue_id: Option<&str>,
    project_id: &str,
    initiator: Option<Initiator>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let trigger_type_str = trigger_type.to_string();

    // Build execution snapshot with event payload
    let snapshot = build_execution_snapshot(
        &mut conn,
        &orch.config_dir,
        &recipe.id,
        issue_id,
        project_id,
        trigger_type,
        Some(event_payload),
        None,
    )?;
    let snapshot_json = snapshot.to_json()?;

    // Calculate next seq if issue-scoped
    let next_seq: Option<i32> = issue_id.and_then(|iid| {
        executions::table
            .filter(executions::issue_id.eq(iid))
            .select(diesel::dsl::max(executions::seq))
            .first::<Option<i32>>(&mut *conn)
            .ok()
            .flatten()
            .map(|max| max + 1)
            .or(Some(1))
    });

    // Create execution
    let exec_id = uuid::Uuid::new_v4().to_string();
    let new_exec = crate::diesel_models::NewExecution {
        id: &exec_id,
        recipe_id: &recipe.id,
        issue_id,
        project_id: if issue_id.is_none() {
            Some(project_id)
        } else {
            None
        },
        status: "running",
        started_at: now,
        completed_at: None,
        snapshot: Some(&snapshot_json),
        seq: next_seq,
        initiator_sub: initiator.as_ref().map(|i| i.sub.as_str()),
        initiator_auth_mode: initiator.as_ref().map(|i| i.auth_mode.as_str()),
        initiator_org_id: initiator.as_ref().map(|i| i.org_id.as_str()),
        triggered_by: &trigger_type_str,
    };

    diesel::insert_into(executions::table)
        .values(&new_exec)
        .execute(&mut *conn)
        .map_err(|e| e.to_string())?;

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id.clone(),
        issue_id: issue_id.map(|s| s.to_string()),
        project_id: if issue_id.is_none() {
            Some(project_id.to_string())
        } else {
            None
        },
        status: ExecutionStatus::Running,
        started_at: now as i64,
        completed_at: None,
        seq: next_seq,
        initiator: initiator.clone(),
        triggered_by: trigger_type_str.parse().unwrap_or_default(),
    })
}

pub use super::queries::get_execution_detail as get_execution_detail_impl;
pub use super::queries::list_all_executions as list_all_executions_impl;
