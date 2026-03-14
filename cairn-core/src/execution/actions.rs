//! Action node execution for DAG advancement (framework-agnostic).
//!
//! Action nodes are recipe components that execute actions — either built-in
//! operations like creating PRs, or custom shell commands. They execute inline
//! during DAG advancement when their dependencies are satisfied.
//!
//! This module is the cairn-core equivalent of the Tauri-only
//! `commands/action_execution.rs`. All functions take `&Orchestrator` instead
//! of `AppHandle`.

use crate::diesel_models::{
    DbActionConfig, DbActionRun, DbArtifact, DbJob, DbRecipeNode, NewPrData, UpdateIssueChangeset,
};
use crate::models::{
    interpolate_template, ActionConfig, ActionNodeConfig, ExecutionSnapshot, RecipeEdge, RecipeNode,
};
use crate::orchestrator::Orchestrator;
use crate::schema::{action_configs, action_runs, artifacts, executions, issues, jobs, pr_data};
use diesel::prelude::*;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::process::Command;

/// Load recipe nodes and edges from execution snapshot.
fn load_recipe_data_from_snapshot(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<(Vec<RecipeNode>, Vec<RecipeEdge>), String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to get execution: {}", e))?;

    let snapshot_json = snapshot_json.ok_or_else(|| "Execution has no snapshot".to_string())?;

    let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;

    Ok((snapshot.recipe.nodes, snapshot.recipe.edges))
}

/// Execute an action node inline during DAG advancement.
///
/// Takes an action_run_id to track execution state.
pub async fn execute_action_node(
    orch: &Orchestrator,
    action_run_id: &str,
    node: &DbRecipeNode,
) -> Result<Option<serde_json::Value>, String> {
    log::info!(
        "Executing action node '{}' for action_run {}",
        node.name,
        action_run_id
    );

    // Load the action_run to get context
    let action_run = get_action_run(orch, action_run_id)?;

    // Parse node config
    let node_config: ActionNodeConfig = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok())
        .ok_or("Action node has no valid config")?;

    // Resolve input values from context edges
    let inputs = resolve_action_inputs(orch, &action_run)?;

    // Look up the ActionConfig
    let action_config = get_action_config_for_node(orch, &node_config)?;

    // Execute the action
    let result = if action_config.is_builtin {
        execute_builtin_action(orch, &action_run, &action_config, inputs).await?
    } else {
        execute_shell_action(orch, &action_run, &action_config, inputs).await?
    };

    // Handle issue status updates for certain actions
    if action_config.id == "builtin:create_pr" {
        if let Some(issue_id) = &action_run.issue_id {
            update_issue_status_with_wait_state(orch, issue_id, "waiting", Some("pr_review"))?;
        }
    } else if action_config.id == "builtin:close_issue" {
        if let Some(issue_id) = &action_run.issue_id {
            update_issue_status_with_wait_state(orch, issue_id, "closed", None)?;
        }
    }

    log::info!(
        "Action node '{}' completed for action_run {}",
        node.name,
        action_run_id
    );

    Ok(result)
}

/// Load an action_run from the database.
fn get_action_run(orch: &Orchestrator, action_run_id: &str) -> Result<DbActionRun, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    action_runs::table
        .find(action_run_id)
        .first(&mut *conn)
        .map_err(|e| format!("Failed to load action_run: {}", e))
}

/// Get the ActionConfig for a node.
fn get_action_config_for_node(
    orch: &Orchestrator,
    node_config: &ActionNodeConfig,
) -> Result<ActionConfig, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    // Try action_config_id first
    if let Some(ref config_id) = node_config.action_config_id {
        let db_config: DbActionConfig = action_configs::table
            .find(config_id)
            .first(&mut *conn)
            .map_err(|e| format!("Action config not found: {}", e))?;

        return ActionConfig::try_from(db_config);
    }

    // Fall back to legacy action string
    let builtin_id = match node_config.action.as_str() {
        "create_pr" => "builtin:create_pr",
        "merge_pr" => "builtin:merge_pr",
        "close_pr" => "builtin:close_pr",
        "close_issue" => "builtin:close_issue",
        other => return Err(format!("Unknown legacy action: {}", other)),
    };

    let db_config: DbActionConfig = action_configs::table
        .find(builtin_id)
        .first(&mut *conn)
        .map_err(|e| format!("Built-in action config not found: {}", e))?;

    ActionConfig::try_from(db_config)
}

/// Execute a built-in action.
async fn execute_builtin_action(
    orch: &Orchestrator,
    action_run: &DbActionRun,
    config: &ActionConfig,
    inputs: HashMap<String, serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    match config.id.as_str() {
        "builtin:create_pr" => handle_create_pr(orch, action_run, inputs).await,
        "builtin:merge_pr" => handle_merge_pr(orch, action_run).await,
        "builtin:close_pr" => handle_close_pr(orch, action_run).await,
        "builtin:close_issue" => handle_close_issue(orch, action_run).await,
        _ => Err(format!("Unknown builtin action: {}", config.id)),
    }
}

/// Execute a custom shell action.
async fn execute_shell_action(
    orch: &Orchestrator,
    action_run: &DbActionRun,
    config: &ActionConfig,
    inputs: HashMap<String, serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let template = config
        .command_template
        .as_ref()
        .ok_or("Custom action has no command template")?;

    let command = interpolate_template(template, &inputs);
    log::info!("Executing shell action: {}", command);

    let cwd = get_action_working_dir(orch, action_run)?;

    let output = Command::new("sh")
        .arg("-c")
        .arg(&command)
        .current_dir(&cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("Failed to execute command: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        log::error!("Action command failed: {}", stderr);
        return Err(format!(
            "Command failed with exit code {}: {}",
            output.status.code().unwrap_or(-1),
            stderr
        ));
    }

    Ok(Some(serde_json::json!({
        "stdout": stdout.trim(),
        "stderr": stderr.trim(),
        "exit_code": output.status.code().unwrap_or(0)
    })))
}

/// Get the working directory for an action.
fn get_action_working_dir(orch: &Orchestrator, action_run: &DbActionRun) -> Result<String, String> {
    find_implementation_context(orch, action_run).map(|(wt, _)| wt)
}

/// Resolve input values for an action from context edges.
fn resolve_action_inputs(
    orch: &Orchestrator,
    action_run: &DbActionRun,
) -> Result<HashMap<String, serde_json::Value>, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let mut inputs = HashMap::new();

    // Load recipe data from execution snapshot
    let (nodes, edges) = load_recipe_data_from_snapshot(&mut conn, &action_run.execution_id)?;

    // Build node lookup map
    let node_map: HashMap<&str, &RecipeNode> = nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    // Find incoming context edges for this action node
    let context_edges: Vec<&RecipeEdge> = edges
        .iter()
        .filter(|e| {
            e.edge_type.to_string() == "context" && e.target_node_id == action_run.recipe_node_id
        })
        .collect();

    for edge in context_edges {
        // Get source node type from snapshot
        let source_node = node_map.get(edge.source_node_id.as_str());

        if let Some(source_node) = source_node {
            if source_node.node_type.to_string() == "trigger" {
                // Build context from issue
                if let Some(issue_id) = &action_run.issue_id {
                    let (title, description): (String, Option<String>) = issues::table
                        .find(issue_id)
                        .select((issues::title, issues::description))
                        .first(&mut *conn)
                        .unwrap_or(("Unknown".to_string(), None));

                    inputs.insert(
                        edge.target_handle.clone(),
                        serde_json::json!({
                            "issue": {
                                "id": issue_id,
                                "title": title,
                                "description": description,
                            }
                        }),
                    );
                }
                continue;
            }

            if source_node.node_type.to_string() == "context" {
                // Context nodes provide static content via context_config
                if let Some(ref context_cfg) = source_node.context_config {
                    inputs.insert(
                        edge.target_handle.clone(),
                        serde_json::json!({ "content": context_cfg.content }),
                    );
                }
                continue;
            }

            if source_node.node_type.to_string() == "action" {
                // Get output from action_run
                let source_action_run: Option<DbActionRun> = action_runs::table
                    .filter(action_runs::execution_id.eq(&action_run.execution_id))
                    .filter(action_runs::recipe_node_id.eq(&edge.source_node_id))
                    .first(&mut *conn)
                    .ok();

                if let Some(source_run) = source_action_run {
                    if let Some(output) = source_run.output {
                        if let Ok(data) = serde_json::from_str(&output) {
                            inputs.insert(edge.target_handle.clone(), data);
                        }
                    }
                }
                continue;
            }
        }

        // Find source job for agent nodes
        let source_job: Option<DbJob> = jobs::table
            .filter(jobs::execution_id.eq(&action_run.execution_id))
            .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
            .first(&mut *conn)
            .ok();

        if let Some(source_job) = source_job {
            let artifact: Option<DbArtifact> = artifacts::table
                .filter(artifacts::job_id.eq(&source_job.id))
                .filter(artifacts::output_name.eq(&edge.source_handle))
                .order(artifacts::version.desc())
                .first(&mut *conn)
                .ok()
                .or_else(|| {
                    artifacts::table
                        .filter(artifacts::job_id.eq(&source_job.id))
                        .order(artifacts::version.desc())
                        .first(&mut *conn)
                        .ok()
                });

            if let Some(artifact) = artifact {
                if let Ok(data) = serde_json::from_str::<serde_json::Value>(&artifact.data) {
                    if let Some(field_value) = data.get(&edge.source_handle) {
                        inputs.insert(edge.target_handle.clone(), field_value.clone());
                    } else {
                        inputs.insert(edge.target_handle.clone(), data);
                    }
                }
            }
        }
    }

    Ok(inputs)
}

// ============================================================================
// Built-in action handlers
// ============================================================================

/// Handle the create_pr action using `gh` CLI.
async fn handle_create_pr(
    orch: &Orchestrator,
    action_run: &DbActionRun,
    inputs: HashMap<String, serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let (worktree_path, branch_name) = find_implementation_context(orch, action_run)?;
    let (title, body) = extract_pr_details(&inputs, orch, action_run)?;

    log::info!("Creating PR for branch {}", branch_name);

    // Push to origin first
    crate::mcp::git::push_to_origin(std::path::Path::new(&worktree_path));

    // Try to create PR with gh
    let mut args = vec!["pr", "create", "--title", &title, "--body"];
    let body_str = body.as_deref().unwrap_or("");
    args.push(body_str);

    let output = crate::env::gh()
        .args(&args)
        .current_dir(&worktree_path)
        .output()
        .map_err(|e| format!("Failed to run gh: {}", e))?;

    let (pr_url, pr_number) = if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Check if PR already exists for this branch
        if stderr.contains("already exists") || stderr.contains("pull request for") {
            log::info!(
                "PR already exists for branch {}, updating instead",
                branch_name
            );

            // Get existing PR details
            let view_output = crate::env::gh()
                .args(["pr", "view", "--json", "number,url"])
                .current_dir(&worktree_path)
                .output()
                .map_err(|e| format!("Failed to run gh pr view: {}", e))?;

            if !view_output.status.success() {
                let view_stderr = String::from_utf8_lossy(&view_output.stderr);
                return Err(format!(
                    "Failed to get existing PR details: {}",
                    view_stderr
                ));
            }

            let view_json: serde_json::Value = serde_json::from_slice(&view_output.stdout)
                .map_err(|e| format!("Failed to parse PR details: {}", e))?;

            let pr_number: i32 = view_json
                .get("number")
                .and_then(|n| n.as_i64())
                .ok_or("Missing PR number in response")?
                .try_into()
                .map_err(|_| "Invalid PR number")?;

            let pr_url = view_json
                .get("url")
                .and_then(|u| u.as_str())
                .ok_or("Missing PR URL in response")?
                .to_string();

            // Update the existing PR
            log::info!("Updating PR #{} with new title and body", pr_number);
            let edit_output = crate::env::gh()
                .args([
                    "pr",
                    "edit",
                    &pr_number.to_string(),
                    "--title",
                    &title,
                    "--body",
                    body_str,
                ])
                .current_dir(&worktree_path)
                .output()
                .map_err(|e| format!("Failed to run gh pr edit: {}", e))?;

            if !edit_output.status.success() {
                let edit_stderr = String::from_utf8_lossy(&edit_output.stderr);
                return Err(format!("Failed to update PR: {}", edit_stderr));
            }

            log::info!("Updated PR #{} at {}", pr_number, pr_url);
            (pr_url, pr_number)
        } else {
            return Err(format!("gh pr create failed: {}", stderr));
        }
    } else {
        let pr_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let pr_number: i32 = pr_url
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("Could not parse PR number from URL: {}", pr_url))?;

        log::info!("Created PR #{} at {}", pr_number, pr_url);
        (pr_url, pr_number)
    };

    // Store or update pr_data linked to this action_run
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        // Check if pr_data already exists for this PR number
        let existing_pr: Option<crate::diesel_models::DbPrData> = pr_data::table
            .filter(pr_data::pr_number.eq(pr_number))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("Failed to check existing pr_data: {}", e))?;

        if let Some(existing) = existing_pr {
            log::info!("Updating existing pr_data for PR #{}", pr_number);
            diesel::update(pr_data::table.filter(pr_data::id.eq(&existing.id)))
                .set((
                    pr_data::action_run_id.eq(Some(&action_run.id)),
                    pr_data::pr_url.eq(&pr_url),
                ))
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to update pr_data: {}", e))?;
        } else {
            log::info!("Creating new pr_data for PR #{}", pr_number);
            let pr_data_id = uuid::Uuid::new_v4().to_string();
            let new_pr_data = NewPrData {
                id: &pr_data_id,
                action_run_id: Some(&action_run.id),
                pr_number,
                pr_url: &pr_url,
                pr_status: "open",
                title: None,
                body: None,
                state: None,
                is_draft: None,
                review_decision: None,
                mergeable: None,
                additions: None,
                deletions: None,
                checks_status: None,
                checks_json: None,
                fetched_at: None,
                opened_at: Some(now),
                merged_at: None,
                closed_at: None,
                updated_at: now,
            };

            diesel::insert_into(pr_data::table)
                .values(&new_pr_data)
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to create pr_data: {}", e))?;
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "pr_data", "action": "update"}),
    );

    Ok(Some(serde_json::json!({ "pr_url": pr_url })))
}

/// Handle the merge_pr action using `gh` CLI.
async fn handle_merge_pr(
    orch: &Orchestrator,
    action_run: &DbActionRun,
) -> Result<Option<serde_json::Value>, String> {
    let (worktree_path, _) = find_implementation_context(orch, action_run)?;

    // Get merge method from settings
    let merge_method = orch.get_settings().merge_type.to_string();

    // Use gh pr merge
    let output = crate::env::gh()
        .args(["pr", "merge", "--auto", &format!("--{}", merge_method)])
        .current_dir(&worktree_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr merge: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("gh pr merge failed: {}", stderr));
    }

    // Update pr_data status
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let pr_action_run_id = find_pr_action_run_id(orch, action_run)?;
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        use crate::diesel_models::UpdatePrDataChangeset;
        let pr_update = UpdatePrDataChangeset {
            pr_status: Some("merged"),
            merged_at: Some(Some(now)),
            ..Default::default()
        };

        diesel::update(pr_data::table.filter(pr_data::action_run_id.eq(Some(&pr_action_run_id))))
            .set(&pr_update)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to update pr_data: {}", e))?;

        // Update issue status to merged
        if let Some(ref issue_id) = action_run.issue_id {
            let issue_update = UpdateIssueChangeset {
                status: Some("merged"),
                updated_at: Some(now),
                ..Default::default()
            };

            diesel::update(issues::table.find(issue_id))
                .set(&issue_update)
                .execute(&mut *conn)
                .map_err(|e| format!("Failed to update issue: {}", e))?;
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "pr_data", "action": "update"}),
    );
    if action_run.issue_id.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "issues", "action": "update"}),
        );
    }

    Ok(Some(serde_json::json!({ "merged": true })))
}

/// Handle the close_pr action using `gh` CLI.
async fn handle_close_pr(
    orch: &Orchestrator,
    action_run: &DbActionRun,
) -> Result<Option<serde_json::Value>, String> {
    let (worktree_path, _) = find_implementation_context(orch, action_run)?;

    let output = crate::env::gh()
        .args(["pr", "close"])
        .current_dir(&worktree_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr close: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("gh pr close failed: {}", stderr));
    }

    // Update pr_data status
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let pr_action_run_id = find_pr_action_run_id(orch, action_run)?;
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        use crate::diesel_models::UpdatePrDataChangeset;
        let pr_update = UpdatePrDataChangeset {
            pr_status: Some("closed"),
            closed_at: Some(Some(now)),
            ..Default::default()
        };

        diesel::update(pr_data::table.filter(pr_data::action_run_id.eq(Some(&pr_action_run_id))))
            .set(&pr_update)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to update pr_data: {}", e))?;
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "pr_data", "action": "update"}),
    );

    Ok(Some(serde_json::json!({ "closed": true })))
}

/// Handle the close_issue action.
async fn handle_close_issue(
    orch: &Orchestrator,
    action_run: &DbActionRun,
) -> Result<Option<serde_json::Value>, String> {
    let issue_id = action_run
        .issue_id
        .as_ref()
        .ok_or("close_issue requires an issue context")?;

    update_issue_status_with_wait_state(orch, issue_id, "closed", None)?;
    Ok(Some(serde_json::json!({ "closed": true })))
}

/// Find the action_run ID that created the PR for this execution.
fn find_pr_action_run_id(orch: &Orchestrator, action_run: &DbActionRun) -> Result<String, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let pr: Option<crate::diesel_models::DbPrData> = pr_data::table
        .filter(pr_data::action_run_id.is_not_null())
        .inner_join(
            action_runs::table.on(action_runs::id.eq(pr_data::action_run_id.assume_not_null())),
        )
        .filter(action_runs::execution_id.eq(&action_run.execution_id))
        .select(crate::diesel_models::DbPrData::as_select())
        .first(&mut *conn)
        .optional()
        .map_err(|e| format!("Failed to query PR: {}", e))?;

    if let Some(pr) = pr {
        return pr
            .action_run_id
            .ok_or("PR has no action_run_id".to_string());
    }

    Err("No PR found for this execution".to_string())
}

// ============================================================================
// Helper functions
// ============================================================================

/// Extract PR title and body from inputs or issue.
fn extract_pr_details(
    inputs: &HashMap<String, serde_json::Value>,
    orch: &Orchestrator,
    action_run: &DbActionRun,
) -> Result<(String, Option<String>), String> {
    // Try inputs first
    if let Some(title) = inputs.get("title").and_then(|v| v.as_str()) {
        let body = inputs
            .get("body")
            .and_then(|v| v.as_str())
            .map(String::from);
        return Ok((title.to_string(), body));
    }

    // Try pr_details object
    if let Some(pr_details) = inputs.get("pr_details") {
        let title = pr_details.get("title").and_then(|v| v.as_str());
        let body = pr_details
            .get("body")
            .and_then(|v| v.as_str())
            .map(String::from);
        if let Some(title) = title {
            return Ok((title.to_string(), body));
        }
    }

    // Search within all input values for title/body
    for value in inputs.values() {
        if let Some(obj) = value.as_object() {
            if let Some(title) = obj.get("title").and_then(|v| v.as_str()) {
                let body = obj.get("body").and_then(|v| v.as_str()).map(String::from);
                log::info!("Found PR title/body in nested input object");
                return Ok((title.to_string(), body));
            }
        }
    }

    // Fall back to issue title/description
    if let Some(issue_id) = &action_run.issue_id {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        let (title, description): (String, Option<String>) = issues::table
            .find(issue_id)
            .select((issues::title, issues::description))
            .first(&mut *conn)
            .map_err(|e| format!("Failed to get issue: {}", e))?;

        log::info!("Using issue title/description as PR fallback");
        return Ok((title, description));
    }

    Err("Could not determine PR title - no inputs or issue found".to_string())
}

/// Find the implementation job's worktree and branch via context edges.
fn find_implementation_context(
    orch: &Orchestrator,
    action_run: &DbActionRun,
) -> Result<(String, String), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    // Load recipe data from execution snapshot
    let (_nodes, edges) = load_recipe_data_from_snapshot(&mut conn, &action_run.execution_id)?;

    // Find incoming context edges for this action node
    let context_edges: Vec<&RecipeEdge> = edges
        .iter()
        .filter(|e| {
            e.edge_type.to_string() == "context" && e.target_node_id == action_run.recipe_node_id
        })
        .collect();

    // Look for source jobs with branches
    for edge in context_edges {
        let source_job: Option<DbJob> = jobs::table
            .filter(jobs::execution_id.eq(&action_run.execution_id))
            .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
            .filter(jobs::branch.is_not_null())
            .filter(jobs::worktree_path.is_not_null())
            .first(&mut *conn)
            .ok();

        if let Some(impl_job) = source_job {
            if let (Some(wt), Some(branch)) = (impl_job.worktree_path, impl_job.branch) {
                log::info!("Found implementation context: branch={}", branch);
                return Ok((wt, branch));
            }
        }
    }

    // Fallback: any complete job with branch in this execution
    let impl_job: Option<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(&action_run.execution_id))
        .filter(jobs::worktree_path.is_not_null())
        .filter(jobs::branch.is_not_null())
        .filter(jobs::status.eq("complete"))
        .order(jobs::completed_at.desc())
        .first(&mut *conn)
        .optional()
        .map_err(|e| format!("Failed to query job: {}", e))?;

    if let Some(impl_job) = impl_job {
        if let (Some(wt), Some(branch)) = (impl_job.worktree_path, impl_job.branch) {
            return Ok((wt, branch));
        }
    }

    Err("No implementation job found with worktree and branch".to_string())
}

/// Update issue status with optional wait_state.
fn update_issue_status_with_wait_state(
    orch: &Orchestrator,
    issue_id: &str,
    status: &str,
    wait_state: Option<&str>,
) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let now = chrono::Utc::now().timestamp() as i32;

    let changeset = UpdateIssueChangeset {
        status: Some(status),
        wait_state: Some(wait_state),
        updated_at: Some(now),
        ..Default::default()
    };

    diesel::update(issues::table.find(issue_id))
        .set(&changeset)
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to update issue: {}", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );

    Ok(())
}
