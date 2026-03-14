//! Recipe execution tracking and management.
//!
//! Core DB and config operations for recipe executions — listing, creating,
//! and managing the lifecycle of recipe execution records.
//!
//! Functions taking `&mut SqliteConnection` are pure DB operations.
//! Functions taking `&Orchestrator` also need config/settings access.

use diesel::prelude::*;
use diesel::SqliteConnection;
use std::path::Path;
use uuid::Uuid;

use crate::config::{agents as config_agents, get_project_path, recipes as config_recipes};
use crate::config::{file_recipe_to_recipe, get_recipe_from_files};
use crate::diesel_models::{DbExecution, NewExecution};
use crate::models::{
    ActionRun, AgentSnapshot, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionSnapshot, ExecutionStatus, Job, Model, RecipeNodeType,
    RecipeTrigger, SnapshotOverrides, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::schema::{action_runs, executions, issues, jobs, projects};
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

/// Get the execution for an issue (if any).
pub fn get_execution_for_issue_impl(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Result<Option<Execution>, String> {
    let db_exec: Option<DbExecution> = executions::table
        .filter(executions::issue_id.eq(issue_id))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    db_exec.map(Execution::try_from).transpose()
}

/// List all executions for an issue, ordered by started_at descending (newest first).
pub fn list_executions_for_issue_impl(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Result<Vec<Execution>, String> {
    let db_execs: Vec<DbExecution> = executions::table
        .filter(executions::issue_id.eq(issue_id))
        .order(executions::started_at.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    db_execs
        .into_iter()
        .map(Execution::try_from)
        .collect::<Result<Vec<_>, _>>()
}

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
                RecipeTrigger::Issue,
                true,
            )?
            .or(find_default_recipe_from_files(
                &orch.config_dir,
                None,
                RecipeTrigger::Issue,
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
        TriggerType::Issue,
    )?;

    // Apply overrides if provided
    if let Some(overrides) = overrides {
        if let Some(recipe_override) = overrides.recipe {
            snapshot.recipe = recipe_override;
        }
        if let Some(agents_override) = overrides.agents {
            for (agent_id, agent_snapshot) in agents_override {
                snapshot.agents.insert(agent_id, agent_snapshot);
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
            // Get issue model if available
            let issue_model: Option<Model> = issues::table
                .find(issue_id)
                .select(issues::model)
                .first::<Option<String>>(&mut *conn)
                .ok()
                .flatten()
                .and_then(|s| s.parse().ok());

            // Get workspace default model
            let workspace_default_model = orch.get_settings().default_model;

            // Load each new agent
            for agent_id in new_agent_ids {
                if let Ok(Some(file_agent)) =
                    config_agents::get_agent(&orch.config_dir, &agent_id, Some(&project_path))
                {
                    let resolved_model = issue_model
                        .clone()
                        .or(file_agent.model.clone())
                        .unwrap_or_else(|| workspace_default_model.clone());

                    snapshot.agents.insert(
                        agent_id.clone(),
                        AgentSnapshot {
                            id: file_agent.id,
                            name: file_agent.name,
                            description: file_agent.description,
                            prompt: file_agent.prompt,
                            tools: file_agent.tools,
                            model: Some(resolved_model),
                            disallowed_tools: file_agent.disallowed_tools,
                            skills: file_agent.skills,
                            permission_mode: file_agent.permission_mode,
                        },
                    );
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
    })
}

/// Extract recipe name from execution snapshot JSON.
fn extract_recipe_name_from_snapshot(snapshot_json: Option<&str>) -> String {
    snapshot_json
        .and_then(|s| serde_json::from_str::<ExecutionSnapshot>(s).ok())
        .map(|snap| snap.recipe.name)
        .unwrap_or_else(|| "Unknown Recipe".to_string())
}

/// List all executions with filters and pagination (for the Executions view).
pub fn list_all_executions_impl(
    conn: &mut SqliteConnection,
    filters: &ExecutionFilters,
    limit: i64,
    offset: i64,
) -> Result<ExecutionListResult, String> {
    let mut query = executions::table
        .left_join(issues::table.on(issues::id.nullable().eq(executions::issue_id)))
        .left_join(projects::table.on(projects::id.nullable().eq(executions::project_id)))
        .into_boxed();

    // Apply filters
    if let Some(status) = &filters.status {
        query = query.filter(executions::status.eq(status));
    }

    if let Some(project_id) = &filters.project_id {
        query = query.filter(
            executions::project_id
                .eq(project_id)
                .or(issues::project_id.eq(project_id)),
        );
    }

    if let Some(triggered_by) = &filters.triggered_by {
        match triggered_by.as_str() {
            "issue" => {
                query = query.filter(executions::issue_id.is_not_null());
            }
            _ => {
                query = query.filter(executions::issue_id.is_null());
            }
        }
    }

    // Rebuild filters for count query
    let mut count_query = executions::table
        .left_join(issues::table.on(issues::id.nullable().eq(executions::issue_id)))
        .left_join(projects::table.on(projects::id.nullable().eq(executions::project_id)))
        .into_boxed();

    if let Some(status) = &filters.status {
        count_query = count_query.filter(executions::status.eq(status));
    }
    if let Some(project_id) = &filters.project_id {
        count_query = count_query.filter(
            executions::project_id
                .eq(project_id)
                .or(issues::project_id.eq(project_id)),
        );
    }
    if let Some(triggered_by) = &filters.triggered_by {
        match triggered_by.as_str() {
            "issue" => {
                count_query = count_query.filter(executions::issue_id.is_not_null());
            }
            _ => {
                count_query = count_query.filter(executions::issue_id.is_null());
            }
        }
    }

    let total: i64 = count_query
        .count()
        .get_result(conn)
        .map_err(|e| e.to_string())?;

    #[allow(clippy::type_complexity)]
    let results: Vec<(
        DbExecution,
        Option<i32>,    // issue number
        Option<String>, // issue project_id
        Option<String>, // project name from direct link
    )> = query
        .select((
            executions::all_columns,
            issues::number.nullable(),
            issues::project_id.nullable(),
            projects::name.nullable(),
        ))
        .order(executions::started_at.desc())
        .limit(limit)
        .offset(offset)
        .load(conn)
        .map_err(|e| e.to_string())?;

    let exec_ids: Vec<String> = results.iter().map(|(e, _, _, _)| e.id.clone()).collect();

    let job_counts: Vec<(String, i64, i64)> = if !exec_ids.is_empty() {
        jobs::table
            .filter(jobs::execution_id.eq_any(&exec_ids))
            .group_by(jobs::execution_id)
            .select((
                jobs::execution_id.assume_not_null(),
                diesel::dsl::count(jobs::id),
                diesel::dsl::count(diesel::dsl::sql::<
                    diesel::sql_types::Nullable<diesel::sql_types::Text>,
                >(
                    "CASE WHEN jobs.status = 'complete' THEN 1 END"
                )),
            ))
            .load(conn)
            .map_err(|e| e.to_string())?
    } else {
        vec![]
    };

    let job_counts_map: std::collections::HashMap<String, (i64, i64)> = job_counts
        .into_iter()
        .map(|(exec_id, total, complete)| (exec_id, (total, complete)))
        .collect();

    let items: Vec<ExecutionListItem> = results
        .into_iter()
        .map(|(db_exec, issue_number, issue_project_id, project_name)| {
            let (jobs_total, jobs_complete) =
                job_counts_map.get(&db_exec.id).copied().unwrap_or((0, 0));

            let status: ExecutionStatus = db_exec.status.parse().unwrap_or_default();
            let triggered_by = if db_exec.issue_id.is_some() {
                TriggerType::Issue
            } else {
                TriggerType::Manual
            };

            let project_id = db_exec.project_id.clone().or(issue_project_id);
            let recipe_name = extract_recipe_name_from_snapshot(db_exec.snapshot.as_deref());

            ExecutionListItem {
                id: db_exec.id,
                recipe_name,
                issue_id: db_exec.issue_id,
                issue_number,
                project_id,
                project_name,
                triggered_by,
                status,
                started_at: db_exec.started_at as i64,
                completed_at: db_exec.completed_at.map(|t| t as i64),
                jobs_total,
                jobs_complete,
                seq: db_exec.seq,
            }
        })
        .collect();

    let has_more = (offset + limit) < total;

    Ok(ExecutionListResult {
        items,
        total,
        has_more,
    })
}

/// Get full execution detail with related data.
pub fn get_execution_detail_impl(
    conn: &mut SqliteConnection,
    execution_id: &str,
) -> Result<ExecutionDetail, String> {
    let db_exec: DbExecution = executions::table
        .filter(executions::id.eq(execution_id))
        .first(conn)
        .map_err(|e| format!("Execution not found: {}", e))?;

    let recipe_name = extract_recipe_name_from_snapshot(db_exec.snapshot.as_deref());
    let execution: Execution = db_exec.try_into()?;

    let (issue_number, issue_title): (Option<i32>, Option<String>) =
        if let Some(issue_id) = &execution.issue_id {
            issues::table
                .filter(issues::id.eq(issue_id))
                .select((issues::number, issues::title))
                .first::<(i32, String)>(conn)
                .optional()
                .map_err(|e| e.to_string())?
                .map(|(n, t)| (Some(n), Some(t)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

    let (project_id, project_name): (Option<String>, Option<String>) =
        if let Some(pid) = &execution.project_id {
            projects::table
                .filter(projects::id.eq(pid))
                .select((projects::id, projects::name))
                .first::<(String, String)>(conn)
                .optional()
                .map_err(|e| e.to_string())?
                .map(|(id, name)| (Some(id), Some(name)))
                .unwrap_or((None, None))
        } else if let Some(issue_id) = &execution.issue_id {
            issues::table
                .inner_join(projects::table.on(projects::id.eq(issues::project_id)))
                .filter(issues::id.eq(issue_id))
                .select((projects::id, projects::name))
                .first::<(String, String)>(conn)
                .optional()
                .map_err(|e| e.to_string())?
                .map(|(id, name)| (Some(id), Some(name)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

    let db_jobs: Vec<crate::diesel_models::DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .order(jobs::created_at.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    let exec_jobs: Vec<Job> = db_jobs
        .into_iter()
        .map(Job::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let db_action_runs: Vec<crate::diesel_models::DbActionRun> = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .order(action_runs::created_at.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    let exec_action_runs: Vec<ActionRun> = db_action_runs
        .into_iter()
        .map(ActionRun::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let exec_issue_id = execution.issue_id.clone();

    Ok(ExecutionDetail {
        execution,
        recipe_name,
        issue_id: exec_issue_id,
        issue_number,
        issue_title,
        project_id,
        project_name,
        jobs: exec_jobs,
        action_runs: exec_action_runs,
    })
}
