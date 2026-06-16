//! Action node execution for DAG advancement (framework-agnostic).
//!
//! Action nodes are recipe components that execute actions — either built-in
//! operations like creating PRs, or custom shell commands. They execute inline
//! during DAG advancement when their dependencies are satisfied.
//!
//! This module is the cairn-core equivalent of the Tauri-only
//! `commands/action_execution.rs`. All functions take `&Orchestrator` instead
//! of `AppHandle`.

use crate::action_runs::queries::action_run_from_row;
use crate::models::{
    interpolate_template, ActionConfig, ActionNodeConfig, ActionRun, ActionRunStatus, Artifact,
    ExecutionSnapshot, RecipeEdge, RecipeNode,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use std::collections::HashMap;
use std::process::Stdio;
use tokio::process::Command;
use turso::params;

fn parse_json_option(
    value: Option<String>,
    label: &str,
) -> Result<Option<serde_json::Value>, DbError> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| DbError::Row(format!("invalid {label} JSON: {error}")))
}

// ============================================================================
// Shared helpers — called by both Tauri and cairn-server hosts
// ============================================================================

/// Load the typed recipe node for an action_run from the execution snapshot.
///
/// Both hosts need this when handling `WorkflowEffect::ExecuteAction`.
pub async fn load_action_node(
    orch: &Orchestrator,
    action_run_id: &str,
    execution_id: &str,
) -> Result<RecipeNode, String> {
    let node_id = action_run_node_id(&orch.db.local, action_run_id).await?;
    let snapshot = load_execution_snapshot(&orch.db.local, execution_id).await?;

    snapshot
        .recipe
        .nodes
        .into_iter()
        .find(|n| n.id == node_id)
        .ok_or_else(|| format!("Node {} not found in snapshot", node_id))
}

/// Find the worktree path for the execution that owns an action_run.
///
/// Both hosts use identical logic: look up execution_id from the action_run,
/// then find the first job in that execution with a worktree_path.
pub async fn find_worktree_for_action_run(
    orch: &Orchestrator,
    action_run_id: &str,
) -> Result<String, String> {
    let execution_id = action_run_execution_id(&orch.db.local, action_run_id).await?;
    let worktree = first_worktree_for_execution(&orch.db.local, &execution_id).await?;

    worktree.ok_or_else(|| "No worktree found for action checkpoint".to_string())
}

/// Complete an action_run after execution: mark it complete in the database.
///
/// This is the shared post-execution logic that was duplicated in both
/// Tauri's `advancement.rs` and cairn-server's `event_loop.rs`.
pub async fn complete_action_run(
    orch: &Orchestrator,
    action_run_id: &str,
    node: &RecipeNode,
    result: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let output_json = result.as_ref().map(|v| v.to_string());

    // A `pr` node holds the DAG open: the action ran (PR is open, `open` port
    // fired) but it is not Complete — it is `Blocked` until the PR merges or
    // closes. A blocked action_run drives the issue's `NeedsApproval` attention
    // exactly as a blocked job does (see `issue_progress_attention`), without
    // being a job. Resolution flips it to Complete. See CAIRN-1220.
    if node.node_type == crate::models::RecipeNodeType::Pr {
        update_action_run_status(
            &orch.db.local,
            action_run_id,
            ActionRunStatus::Blocked,
            output_json.as_deref(),
            now,
        )
        .await?;
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "action_runs", "action": "update"}),
        );
        // Recompute execution + issue status so the open PR surfaces as
        // `NeedsApproval` immediately (the blocked action_run is non-terminal, so
        // the execution stays running and the issue moves to `waiting`). The
        // sweep recomputes attention even with no job-status changes, but only
        // emits/wakes when a job flips — so emit the issue change and wake any
        // in-flight `cairn watch` explicitly here.
        let pr_action_run = get_action_run(&orch.db.local, action_run_id).await?;
        crate::execution::advancement::recompute_execution_jobs(orch, &pr_action_run.execution_id)?;
        for table in ["issues", "executions"] {
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": table, "action": "update"}),
            );
        }
        if let Some(issue_id) = pr_action_run.issue_id.as_deref() {
            orch.wake_for_issue(issue_id).await;
        }
        return Ok(result);
    }

    // Mark action_run as complete
    update_action_run_complete(&orch.db.local, action_run_id, output_json.as_deref(), now).await?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "update"}),
    );

    Ok(result)
}

/// Execute an action node inline during DAG advancement.
///
/// Takes an action_run_id to track execution state.
pub async fn execute_action_node(
    orch: &Orchestrator,
    action_run_id: &str,
    node: &RecipeNode,
) -> Result<Option<serde_json::Value>, String> {
    log::info!(
        "Executing action node '{}' for action_run {}",
        node.name,
        action_run_id
    );

    // Load the action_run to get context
    let action_run = get_action_run_for_orchestrator(orch, action_run_id).await?;

    // Resolve input values from context edges
    let inputs = resolve_action_inputs(orch, &action_run).await?;

    // A `pr` node is a first-class action whose vehicle is `builtin:pr` — it has
    // no `action_configs` row, so it is handled here directly rather than via the
    // ActionConfig lookup. It opens the PR and holds the DAG open (the action_run
    // is set to `Blocked` by `complete_action_run`). See CAIRN-1220.
    if node.node_type == crate::models::RecipeNodeType::Pr {
        let pr_url = handle_pr_node(orch, &action_run, inputs).await?;
        return Ok(Some(serde_json::json!({ "pr_url": pr_url })));
    }

    let node_config: ActionNodeConfig = node
        .action_config
        .clone()
        .ok_or("Action node has no action config")?;

    // Look up the ActionConfig
    let action_config = get_action_config_for_node(orch, &node_config).await?;

    // Execute the action
    let result = if action_config.is_builtin {
        execute_builtin_action(orch, &action_run, &action_config, inputs).await?
    } else {
        execute_shell_action(orch, &action_run, &action_config, inputs).await?
    };

    log::info!(
        "Action node '{}' completed for action_run {}",
        node.name,
        action_run_id
    );

    Ok(result)
}

/// Load an action_run from the database.
async fn get_action_run_for_orchestrator(
    orch: &Orchestrator,
    action_run_id: &str,
) -> Result<ActionRun, String> {
    get_action_run(&orch.db.local, action_run_id).await
}

/// Get the ActionConfig for a node.
async fn get_action_config_for_node(
    orch: &Orchestrator,
    node_config: &ActionNodeConfig,
) -> Result<ActionConfig, String> {
    // Try action_config_id first
    let config_id =
        node_config
            .action_config_id
            .clone()
            .unwrap_or_else(|| match node_config.action.as_str() {
                "create_pr" => "builtin:create_pr".to_string(),
                "merge_pr" => "builtin:merge_pr".to_string(),
                "close_pr" => "builtin:close_pr".to_string(),
                "close_issue" => "builtin:close_issue".to_string(),
                other => format!("unknown:{other}"),
            });

    if config_id.starts_with("unknown:") {
        return Err(format!(
            "Unknown legacy action: {}",
            config_id.trim_start_matches("unknown:")
        ));
    }

    get_action_config(&orch.db.local, &config_id).await
}

/// Execute a built-in action.
async fn execute_builtin_action(
    orch: &Orchestrator,
    action_run: &ActionRun,
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

/// Handle a first-class PR node: open the PR with the same mechanics the legacy
/// `create_pr` action used, link the `merge_requests` row to the PR action_run's
/// own id, and fire the fixed `open` port. The action_run is left `Running`; the
/// shared `complete_action_run` flips it to `Blocked` (holding the DAG open while
/// the PR awaits merge/close). See CAIRN-1220.
pub async fn handle_pr_node(
    orch: &Orchestrator,
    action_run: &ActionRun,
    inputs: HashMap<String, serde_json::Value>,
) -> Result<String, String> {
    let (worktree_path, branch_name, base_branch) =
        find_implementation_context(orch, action_run).await?;
    let (title, body) = extract_pr_details(&inputs, orch, action_run).await?;

    let has_remote = crate::mcp::git::has_remote(std::path::Path::new(&worktree_path));
    let github = if has_remote {
        let pr_url = open_or_update_github_pr(
            &worktree_path,
            &branch_name,
            base_branch.as_deref(),
            &title,
            body.as_deref(),
        )
        .await?;
        let pr_number: i32 = pr_url
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("Could not parse PR number from URL: {pr_url}"))?;
        Some((pr_url, pr_number))
    } else {
        None
    };

    let now = chrono::Utc::now().timestamp() as i32;
    upsert_merge_request_for_pr(
        &orch.db.local,
        &action_run.id,
        &action_run.project_id,
        action_run.issue_id.as_deref(),
        &title,
        body.as_deref(),
        &branch_name,
        base_branch.as_deref(),
        github.as_ref().map(|(url, number)| (url.as_str(), *number)),
        now,
    )
    .await?;
    crate::pr_data::ports::fire_pr_node_port(
        &orch.db.local,
        &action_run.execution_id,
        &action_run.recipe_node_id,
        "open",
    )
    .await?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );
    let _ = crate::pr_data::actions::refresh_pr_for_job(orch, &action_run.id).await;

    Ok(github
        .map(|(url, _)| url)
        .unwrap_or_else(|| format!("local://{}", branch_name)))
}

/// Execute a custom shell action.
async fn execute_shell_action(
    orch: &Orchestrator,
    action_run: &ActionRun,
    config: &ActionConfig,
    inputs: HashMap<String, serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let template = config
        .command_template
        .as_ref()
        .ok_or("Custom action has no command template")?;

    let command = interpolate_template(template, &inputs);
    log::info!("Executing shell action: {}", command);

    let cwd = get_action_working_dir(orch, action_run).await?;

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
async fn get_action_working_dir(
    orch: &Orchestrator,
    action_run: &ActionRun,
) -> Result<String, String> {
    find_implementation_context(orch, action_run)
        .await
        .map(|(wt, _, _)| wt)
}

/// Resolve input values for an action from context edges.
async fn resolve_action_inputs(
    orch: &Orchestrator,
    action_run: &ActionRun,
) -> Result<HashMap<String, serde_json::Value>, String> {
    let mut inputs = HashMap::new();

    // Load recipe data from execution snapshot
    let snapshot = load_execution_snapshot(&orch.db.local, &action_run.execution_id).await?;
    let nodes = snapshot.recipe.nodes;
    let edges = snapshot.recipe.edges;

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
                    let (title, description) = issue_title_description(&orch.db.local, issue_id)
                        .await
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
                if let Some(output) = source_action_output(
                    &orch.db.local,
                    &action_run.execution_id,
                    &edge.source_node_id,
                )
                .await?
                {
                    if let Ok(data) = serde_json::from_str(&output) {
                        inputs.insert(edge.target_handle.clone(), data);
                    }
                }
                continue;
            }
        }

        // Find source job for agent nodes
        let source_job = source_job_for_node(
            &orch.db.local,
            &action_run.execution_id,
            &edge.source_node_id,
        )
        .await?;

        if let Some(source_job) = source_job {
            if let Some(artifact) =
                latest_artifact_for_job(&orch.db.local, &source_job.id, &edge.source_handle).await?
            {
                if let Some(field_value) = artifact.data.get(&edge.source_handle) {
                    inputs.insert(edge.target_handle.clone(), field_value.clone());
                } else {
                    inputs.insert(edge.target_handle.clone(), artifact.data);
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
    action_run: &ActionRun,
    inputs: HashMap<String, serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let (worktree_path, branch_name, base_branch) =
        find_implementation_context(orch, action_run).await?;
    let (title, body) = extract_pr_details(&inputs, orch, action_run).await?;

    let has_remote = crate::mcp::git::has_remote(std::path::Path::new(&worktree_path));
    let github = if has_remote {
        let pr_url = open_or_update_github_pr(
            &worktree_path,
            &branch_name,
            base_branch.as_deref(),
            &title,
            body.as_deref(),
        )
        .await?;
        let pr_number: i32 = pr_url
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("Could not parse PR number from URL: {}", pr_url))?;
        Some((pr_url, pr_number))
    } else {
        None
    };

    // Store or update merge_request linked to the parent job
    let now = chrono::Utc::now().timestamp() as i32;
    let parent_job_id = action_run
        .parent_job_id
        .as_deref()
        .ok_or("Action run has no parent_job_id")?;
    upsert_merge_request_for_pr(
        &orch.db.local,
        parent_job_id,
        &action_run.project_id,
        action_run.issue_id.as_deref(),
        &title,
        body.as_deref(),
        &branch_name,
        base_branch.as_deref(),
        github.as_ref().map(|(url, number)| (url.as_str(), *number)),
        now,
    )
    .await?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );

    // Refresh the GitHub PR cache so mergeability / review / check state is
    // known promptly. Without this the cache stays unknown until a webhook or
    // the next refresh-on-read, leaving the issue's attention projection at
    // `None` and the desktop badge blank. Non-fatal: the PR is already open, so
    // a transient GitHub/API failure must not fail the build output — log and
    // continue.
    match crate::pr_data::actions::refresh_pr_for_job(orch, parent_job_id).await {
        Ok(_) => {
            // Live GitHub data is cached now — recompute issue status/attention
            // so live PR detail is available without waiting for a webhook.
            // recompute_job routes through the execution sweep, which recomputes
            // the issue projection.
            if let Err(e) = crate::execution::advancement::recompute_job(orch, parent_job_id) {
                log::warn!(
                    "Failed to recompute job {} after PR cache refresh: {}",
                    parent_job_id,
                    e
                );
            }
        }
        Err(e) => {
            log::warn!(
                "Failed to refresh PR cache after creating PR for job {} (PR is open; continuing): {}",
                parent_job_id,
                e
            );
        }
    }

    // Wake any in-flight `cairn watch` now that the PR is open (or updated). The
    // builder's turn already ended before this action ran, so its turn-end emit
    // fired while no PR existed yet — this is the single idle-with-work wake for
    // the freshly-opened-PR case. The attention projection may still be `None`
    // (unknown GitHub state), so `wake_for_issue` falls back to the open-PR work
    // product when resolving the event's detail URI.
    if let Some(issue_id) = action_run.issue_id.as_deref() {
        orch.wake_for_issue(issue_id).await;
    }

    Ok(Some(serde_json::json!({ "pr_url": github
        .map(|(url, _)| url)
        .unwrap_or_else(|| format!("local://{}", branch_name)) })))
}

async fn open_or_update_github_pr(
    worktree_path: &str,
    branch_name: &str,
    base_branch: Option<&str>,
    title: &str,
    body: Option<&str>,
) -> Result<String, String> {
    log::info!("Creating PR for branch {}", branch_name);
    crate::mcp::git::push_to_origin(std::path::Path::new(worktree_path));

    if let Some(base) = base_branch {
        ensure_base_branch_on_origin(worktree_path, base)?;
    }

    let body_str = body.unwrap_or("");
    let mut args = vec!["pr", "create", "--title", title, "--body", body_str];
    if let Some(base) = base_branch {
        args.push("--base");
        args.push(base);
    }

    let output = crate::env::gh()
        .args(&args)
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("Failed to run gh: {}", e))?;

    if output.status.success() {
        let pr_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
        log::info!("Created PR at {}", pr_url);
        return Ok(pr_url);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if !(stderr.contains("already exists") || stderr.contains("pull request for")) {
        return Err(format!("gh pr create failed: {}", stderr));
    }

    log::info!(
        "PR already exists for branch {}, updating instead",
        branch_name
    );
    let view_output = crate::env::gh()
        .args(["pr", "view", "--json", "number,url"])
        .current_dir(worktree_path)
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
    let pr_number = view_json
        .get("number")
        .and_then(|n| n.as_i64())
        .ok_or("Missing PR number in response")?
        .to_string();
    let pr_url = view_json
        .get("url")
        .and_then(|u| u.as_str())
        .ok_or("Missing PR URL in response")?
        .to_string();
    let edit_output = crate::env::gh()
        .args([
            "pr", "edit", &pr_number, "--title", title, "--body", body_str,
        ])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr edit: {}", e))?;
    if !edit_output.status.success() {
        let edit_stderr = String::from_utf8_lossy(&edit_output.stderr);
        return Err(format!("Failed to update PR: {}", edit_stderr));
    }
    log::info!("Updated PR #{} at {}", pr_number, pr_url);
    Ok(pr_url)
}

fn ensure_base_branch_on_origin(worktree_path: &str, base_branch: &str) -> Result<(), String> {
    if base_branch == "HEAD" || base_branch.starts_with("origin/") {
        return Ok(());
    }

    let default_branch = crate::env::git()
        .args(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
        .current_dir(worktree_path)
        .output()
        .ok()
        .and_then(|output| {
            output.status.success().then(|| {
                String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .strip_prefix("origin/")
                    .unwrap_or("")
                    .to_string()
            })
        })
        .filter(|branch| !branch.is_empty())
        .unwrap_or_else(|| "main".to_string());
    if base_branch == default_branch {
        return Ok(());
    }

    let remote_ref = format!("refs/remotes/origin/{base_branch}");
    let exists = crate::env::git()
        .args(["show-ref", "--verify", "--quiet", &remote_ref])
        .current_dir(worktree_path)
        .status()
        .map_err(|e| format!("Failed to check remote base branch: {e}"))?
        .success();
    if exists {
        return Ok(());
    }

    let output = crate::env::git()
        .args(["push", "origin", base_branch])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("Failed to push base branch {base_branch}: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "Failed to push base branch {base_branch}: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// Refuse an automated terminal resolution while the resolved issue still has
/// active work — e.g. a reviewer still running on a child issue a coordinator is
/// about to merge. The user-driven UI path (`issues::status::update_status`)
/// overrides by stopping that work; this guards only the recipe merge/close
/// actions so an agent does not resolve an issue out from under a live reviewer.
async fn ensure_resolution_not_blocked(
    orch: &Orchestrator,
    issue_id: &str,
    verb: &str,
) -> Result<(), String> {
    let blockers = crate::issues::status::terminal_resolution_blockers(orch, issue_id).await?;
    if blockers.is_empty() {
        return Ok(());
    }
    Err(format!(
        "Refusing to mark issue {verb} while it still has {}; finish or stop the running work first.",
        blockers.join(", ")
    ))
}

/// Handle the merge_pr action using `gh` CLI.
async fn handle_merge_pr(
    orch: &Orchestrator,
    action_run: &ActionRun,
) -> Result<Option<serde_json::Value>, String> {
    // Block a premature merge before touching GitHub: if the issue still has
    // active reviewers/jobs, fail the action rather than resolve it out from
    // under them.
    if let Some(ref issue_id) = action_run.issue_id {
        ensure_resolution_not_blocked(orch, issue_id, "merged").await?;
    }

    let (worktree_path, _, _) = find_implementation_context(orch, action_run).await?;

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

    // Update merge_request status
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let mr_job_id = find_mr_job_id_for_action(orch, action_run).await?;
        update_merge_request_status(&orch.db.local, &mr_job_id, "merged", now).await?;

        // Resolve issue as merged
        if let Some(ref issue_id) = action_run.issue_id {
            crate::issues::crud::resolve(
                &orch.db.local,
                &*orch.services.clock,
                issue_id,
                crate::transitions::Resolution::Merged,
            )
            .await
            .map_err(|e| format!("Failed to resolve issue as merged: {}", e))?;
            crate::execution::advancement::release_dependent_executions(orch, issue_id).await?;
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
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
    action_run: &ActionRun,
) -> Result<Option<serde_json::Value>, String> {
    let (worktree_path, _, _) = find_implementation_context(orch, action_run).await?;

    let output = crate::env::gh()
        .args(["pr", "close"])
        .current_dir(&worktree_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr close: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("gh pr close failed: {}", stderr));
    }

    // Update merge_request status
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let mr_job_id = find_mr_job_id_for_action(orch, action_run).await?;
        update_merge_request_status(&orch.db.local, &mr_job_id, "closed", now).await?;
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );

    Ok(Some(serde_json::json!({ "closed": true })))
}

/// Handle the close_issue action.
async fn handle_close_issue(
    orch: &Orchestrator,
    action_run: &ActionRun,
) -> Result<Option<serde_json::Value>, String> {
    let issue_id = action_run
        .issue_id
        .as_ref()
        .ok_or("close_issue requires an issue context")?;

    ensure_resolution_not_blocked(orch, issue_id, "closed").await?;

    crate::issues::crud::resolve(
        &orch.db.local,
        &*orch.services.clock,
        issue_id,
        crate::transitions::Resolution::Closed,
    )
    .await
    .map_err(|e| format!("Failed to resolve issue as closed: {}", e))?;
    crate::execution::advancement::release_dependent_executions(orch, issue_id).await?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );

    Ok(Some(serde_json::json!({ "closed": true })))
}

/// Find the job_id that has a merge request for this execution.
async fn find_mr_job_id_for_action(
    orch: &Orchestrator,
    action_run: &ActionRun,
) -> Result<String, String> {
    find_mr_job_id_for_execution(&orch.db.local, &action_run.execution_id).await
}

// ============================================================================
// Helper functions
// ============================================================================

/// Extract PR title and body from inputs or issue.
async fn extract_pr_details(
    inputs: &HashMap<String, serde_json::Value>,
    orch: &Orchestrator,
    action_run: &ActionRun,
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
        let (title, description) = issue_title_description(&orch.db.local, issue_id).await?;

        log::info!("Using issue title/description as PR fallback");
        return Ok((title, description));
    }

    Err("Could not determine PR title - no inputs or issue found".to_string())
}

/// Find the implementation job's worktree and branch via context edges.
async fn find_implementation_context(
    orch: &Orchestrator,
    action_run: &ActionRun,
) -> Result<(String, String, Option<String>), String> {
    // Load recipe data from execution snapshot
    let snapshot = load_execution_snapshot(&orch.db.local, &action_run.execution_id).await?;
    let edges = snapshot.recipe.edges;

    // Find incoming context edges for this action node
    let context_edges: Vec<&RecipeEdge> = edges
        .iter()
        .filter(|e| {
            e.edge_type.to_string() == "context" && e.target_node_id == action_run.recipe_node_id
        })
        .collect();

    // Look for source jobs with branches
    for edge in context_edges {
        if let Some((wt, branch, base_branch)) = implementation_job_for_node(
            &orch.db.local,
            &action_run.execution_id,
            &edge.source_node_id,
        )
        .await?
        {
            log::info!("Found implementation context: branch={}", branch);
            return Ok((wt, branch, base_branch));
        }
    }

    // Fallback: any complete job with branch in this execution
    if let Some(context) =
        latest_complete_implementation_job(&orch.db.local, &action_run.execution_id).await?
    {
        return Ok(context);
    }

    Err("No implementation job found with worktree and branch".to_string())
}

async fn load_execution_snapshot(
    db: &LocalDb,
    execution_id: &str,
) -> Result<ExecutionSnapshot, String> {
    let execution_id = execution_id.to_string();
    let snapshot_json: String = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT snapshot FROM executions WHERE id = ?1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("execution not found".to_string()))?;
                row.opt_text(0)?
                    .ok_or_else(|| DbError::Row("execution has no snapshot".to_string()))
            })
        })
        .await
        .map_err(|e| format!("Failed to get execution: {}", e))?;

    serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))
}

async fn action_run_node_id(db: &LocalDb, action_run_id: &str) -> Result<String, String> {
    let action_run_id = action_run_id.to_string();
    db.query_text(
        "SELECT recipe_node_id FROM action_runs WHERE id = ?1",
        params![action_run_id.as_str()],
    )
    .await
    .and_then(|value| value.ok_or_else(|| DbError::Row("action_run not found".to_string())))
    .map_err(|e| format!("action_run not found: {}", e))
}

async fn action_run_execution_id(db: &LocalDb, action_run_id: &str) -> Result<String, String> {
    let action_run_id = action_run_id.to_string();
    db.query_text(
        "SELECT execution_id FROM action_runs WHERE id = ?1",
        params![action_run_id.as_str()],
    )
    .await
    .and_then(|value| value.ok_or_else(|| DbError::Row("action_run not found".to_string())))
    .map_err(|e| format!("Action run not found: {}", e))
}

async fn first_worktree_for_execution(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<String>, String> {
    let execution_id = execution_id.to_string();
    db.query_text(
        "SELECT worktree_path
         FROM jobs
         WHERE execution_id = ?1
           AND worktree_path IS NOT NULL
         LIMIT 1",
        params![execution_id.as_str()],
    )
    .await
    .map_err(|e| format!("Failed to query worktree: {}", e))
}

async fn get_action_run(db: &LocalDb, action_run_id: &str) -> Result<ActionRun, String> {
    let action_run_id = action_run_id.to_string();
    db.read(|conn| {
        let action_run_id = action_run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, execution_id, recipe_node_id, action_config_id, issue_id,
                            project_id, status, inputs, output, error_message, started_at,
                            completed_at, created_at, parent_job_id, uri_segment
                     FROM action_runs
                     WHERE id = ?1",
                    (action_run_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| action_run_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row("action_run not found".to_string()))
        })
    })
    .await
    .map_err(|e| format!("Failed to load action_run: {}", e))
}

async fn get_action_config(db: &LocalDb, config_id: &str) -> Result<ActionConfig, String> {
    let config_id = config_id.to_string();
    db.read(|conn| {
        let config_id = config_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, name, description, command_template, input_schema,
                            output_schema, is_builtin, workspace_id, project_id,
                            created_at, updated_at, tool_name, tool_description
                     FROM action_configs
                     WHERE id = ?1",
                    (config_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| action_config_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row("action_config not found".to_string()))
        })
    })
    .await
    .map_err(|e| format!("Action config not found: {}", e))
}

/// Set an action_run's status, persisting its output and stamping `completed_at`
/// only for terminal states (`Complete`/`Failed`). `Blocked` is non-terminal — it
/// records output but leaves `completed_at` NULL, since the run is still open.
async fn update_action_run_status(
    db: &LocalDb,
    action_run_id: &str,
    status: ActionRunStatus,
    output_json: Option<&str>,
    now: i32,
) -> Result<(), String> {
    let action_run_id = action_run_id.to_string();
    let status_str = status.to_string();
    let terminal = matches!(status, ActionRunStatus::Complete | ActionRunStatus::Failed);
    let output_json = output_json.map(ToOwned::to_owned);
    db.write(|conn| {
        let action_run_id = action_run_id.clone();
        let status_str = status_str.clone();
        let output_json = output_json.clone();
        Box::pin(async move {
            if terminal {
                conn.execute(
                    "UPDATE action_runs
                     SET status = ?1, output = ?2, completed_at = ?3
                     WHERE id = ?4",
                    params![
                        status_str.as_str(),
                        output_json.as_deref(),
                        now,
                        action_run_id.as_str()
                    ],
                )
                .await?;
            } else {
                conn.execute(
                    "UPDATE action_runs
                     SET status = ?1, output = ?2
                     WHERE id = ?3",
                    params![
                        status_str.as_str(),
                        output_json.as_deref(),
                        action_run_id.as_str()
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to update action_run status: {e}"))
}

async fn update_action_run_complete(
    db: &LocalDb,
    action_run_id: &str,
    output_json: Option<&str>,
    now: i32,
) -> Result<(), String> {
    let action_run_id = action_run_id.to_string();
    let output_json = output_json.map(ToOwned::to_owned);
    db.execute(
        "UPDATE action_runs
         SET status = 'complete',
             output = ?1,
             completed_at = ?2
         WHERE id = ?3",
        params![output_json.as_deref(), now, action_run_id.as_str()],
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("Failed to update action_run: {}", e))
}

async fn issue_title_description(
    db: &LocalDb,
    issue_id: &str,
) -> Result<(String, Option<String>), String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT title, description FROM issues WHERE id = ?1",
                    (issue_id.as_str(),),
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("issue not found".to_string()))?;
            Ok((row.text(0)?, row.opt_text(1)?))
        })
    })
    .await
    .map_err(|e| format!("Failed to get issue: {}", e))
}

async fn source_action_output(
    db: &LocalDb,
    execution_id: &str,
    recipe_node_id: &str,
) -> Result<Option<String>, String> {
    let execution_id = execution_id.to_string();
    let recipe_node_id = recipe_node_id.to_string();
    db.query_opt_text(
        "SELECT output
         FROM action_runs
         WHERE execution_id = ?1
           AND recipe_node_id = ?2
         LIMIT 1",
        params![execution_id.as_str(), recipe_node_id.as_str()],
    )
    .await
    .map_err(|e| format!("Failed to query source action output: {}", e))
}

struct SourceJob {
    id: String,
}

async fn source_job_for_node(
    db: &LocalDb,
    execution_id: &str,
    recipe_node_id: &str,
) -> Result<Option<SourceJob>, String> {
    let execution_id = execution_id.to_string();
    let recipe_node_id = recipe_node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let recipe_node_id = recipe_node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id
                     FROM jobs
                     WHERE execution_id = ?1
                       AND recipe_node_id = ?2
                       AND status <> 'cancelled'
                     ORDER BY created_at DESC
                     LIMIT 1",
                    (execution_id.as_str(), recipe_node_id.as_str()),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok(SourceJob { id: row.text(0)? }))
                .transpose()
        })
    })
    .await
    .map_err(|e| format!("Failed to query source job: {}", e))
}

async fn latest_artifact_for_job(
    db: &LocalDb,
    job_id: &str,
    output_name: &str,
) -> Result<Option<Artifact>, String> {
    let job_id = job_id.to_string();
    let output_name = output_name.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let output_name = output_name.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, artifact_type, schema_version, data, version,
                            parent_version_id, output_name, created_at, updated_at, seen_at, confirmed
                     FROM artifacts
                     WHERE job_id = ?1
                       AND output_name = ?2
                     ORDER BY version DESC
                     LIMIT 1",
                    (job_id.as_str(), output_name.as_str()),
                )
                .await?;
            if let Some(row) = rows.next().await? {
                return Ok(Some(artifact_from_row(&row)?));
            }

            let mut rows = conn
                .query(
                    "SELECT id, job_id, artifact_type, schema_version, data, version,
                            parent_version_id, output_name, created_at, updated_at, seen_at, confirmed
                     FROM artifacts
                     WHERE job_id = ?1
                     ORDER BY version DESC
                     LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| artifact_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(|e| format!("Failed to query artifact: {}", e))
}

async fn implementation_job_for_node(
    db: &LocalDb,
    execution_id: &str,
    recipe_node_id: &str,
) -> Result<Option<(String, String, Option<String>)>, String> {
    let execution_id = execution_id.to_string();
    let recipe_node_id = recipe_node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let recipe_node_id = recipe_node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT worktree_path, branch, base_branch
                     FROM jobs
                     WHERE execution_id = ?1
                       AND recipe_node_id = ?2
                       AND worktree_path IS NOT NULL
                       AND branch IS NOT NULL
                       AND status <> 'cancelled'
                     ORDER BY created_at DESC
                     LIMIT 1",
                    (execution_id.as_str(), recipe_node_id.as_str()),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)))
                .transpose()
        })
    })
    .await
    .map_err(|e| format!("Failed to query implementation job: {}", e))
}

async fn latest_complete_implementation_job(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<(String, String, Option<String>)>, String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT worktree_path, branch, base_branch
                     FROM jobs
                     WHERE execution_id = ?1
                       AND worktree_path IS NOT NULL
                       AND branch IS NOT NULL
                       AND status = 'complete'
                     ORDER BY completed_at DESC
                     LIMIT 1",
                    (execution_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)))
                .transpose()
        })
    })
    .await
    .map_err(|e| format!("Failed to query job: {}", e))
}

#[allow(clippy::too_many_arguments)]
async fn upsert_merge_request_for_pr(
    db: &LocalDb,
    parent_job_id: &str,
    project_id: &str,
    issue_id: Option<&str>,
    title: &str,
    body: Option<&str>,
    source_branch: &str,
    base_branch: Option<&str>,
    github: Option<(&str, i32)>,
    now: i32,
) -> Result<(), String> {
    let parent_job_id = parent_job_id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.map(ToOwned::to_owned);
    let title = title.to_string();
    let body = body.map(ToOwned::to_owned);
    let source_branch = source_branch.to_string();
    let base_branch = base_branch.map(ToOwned::to_owned);
    let github = github.map(|(url, number)| (url.to_string(), number));

    db.write(|conn| {
        let parent_job_id = parent_job_id.clone();
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let title = title.clone();
        let body = body.clone();
        let source_branch = source_branch.clone();
        let base_branch = base_branch.clone();
        let github = github.clone();
        Box::pin(async move {
            let mut existing = conn
                .query(
                    "SELECT id, target_branch FROM merge_requests WHERE job_id = ?1 LIMIT 1",
                    (parent_job_id.as_str(),),
                )
                .await?;
            if let Some(row) = existing.next().await? {
                let existing_id = row.text(0)?;
                let existing_target = row.text(1).unwrap_or_else(|_| "main".to_string());
                let target_branch = base_branch.as_deref().unwrap_or(&existing_target);
                log::info!("Updating existing merge_request for job {}", parent_job_id);
                let (github_pr_url, github_pr_number, github_state): (
                    Option<String>,
                    Option<i32>,
                    Option<String>,
                ) = github
                    .as_ref()
                    .map(|(url, number)| {
                        (Some(url.clone()), Some(*number), Some("OPEN".to_string()))
                    })
                    .unwrap_or((None, None, None));
                conn.execute(
                    "UPDATE merge_requests
                     SET title = ?1,
                         body = ?2,
                         source_branch = ?3,
                         target_branch = ?4,
                         status = 'open',
                         github_pr_number = ?5,
                         github_pr_url = ?6,
                         github_state = ?7,
                         updated_at = ?8
                     WHERE id = ?9",
                    params![
                        title.as_str(),
                        body.as_deref(),
                        source_branch.as_str(),
                        target_branch,
                        github_pr_number,
                        github_pr_url.as_deref(),
                        github_state.as_deref(),
                        now,
                        existing_id.as_str()
                    ],
                )
                .await?;
                return Ok(());
            }

            log::info!("Creating new merge_request for job {}", parent_job_id);
            let mut default_branch_rows = conn
                .query(
                    "SELECT default_branch FROM projects WHERE id = ?1",
                    (project_id.as_str(),),
                )
                .await?;
            let default_branch = default_branch_rows
                .next()
                .await?
                .and_then(|row| row.opt_text(0).ok().flatten())
                .unwrap_or_else(|| "main".to_string());
            let target_branch = base_branch.as_deref().unwrap_or(&default_branch);
            let mr_id = uuid::Uuid::new_v4().to_string();
            let (github_pr_url, github_pr_number, github_state): (
                Option<String>,
                Option<i32>,
                Option<String>,
            ) = github
                .as_ref()
                .map(|(url, number)| (Some(url.clone()), Some(*number), Some("OPEN".to_string())))
                .unwrap_or((None, None, None));

            conn.execute(
                "INSERT INTO merge_requests(
                    id, job_id, project_id, issue_id, title, body,
                    source_branch, target_branch, status, merge_method, opened_at,
                    updated_at, github_pr_number, github_pr_url, github_state
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'open',
                         'squash', ?9, ?10, ?11, ?12, ?13)",
                params![
                    mr_id.as_str(),
                    parent_job_id.as_str(),
                    project_id.as_str(),
                    issue_id.as_deref(),
                    title.as_str(),
                    body.as_deref(),
                    source_branch.as_str(),
                    target_branch,
                    now,
                    now,
                    github_pr_number,
                    github_pr_url.as_deref(),
                    github_state.as_deref(),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to upsert merge_request: {}", e))
}

async fn find_mr_job_id_for_execution(db: &LocalDb, execution_id: &str) -> Result<String, String> {
    let execution_id = execution_id.to_string();
    db.query_text(
        "SELECT mr.job_id
         FROM merge_requests mr
         INNER JOIN jobs j ON mr.job_id = j.id
         WHERE j.execution_id = ?1
         LIMIT 1",
        params![execution_id.as_str()],
    )
    .await
    .and_then(|value| {
        value.ok_or_else(|| DbError::Row("No merge request found for this execution".into()))
    })
    .map_err(|e| format!("Failed to query merge request: {}", e))
}

async fn update_merge_request_status(
    db: &LocalDb,
    mr_job_id: &str,
    status: &str,
    now: i32,
) -> Result<(), String> {
    let mr_job_id = mr_job_id.to_string();
    let status = status.to_string();
    let _source_branch = if status == "merged" {
        db.query_text(
            "SELECT source_branch FROM merge_requests WHERE job_id = ?1 LIMIT 1",
            params![mr_job_id.as_str()],
        )
        .await
        .map_err(|e| format!("Failed to query merge_request source branch: {e}"))?
    } else {
        None
    };
    db.write(|conn| {
        let mr_job_id = mr_job_id.clone();
        let status = status.clone();
        Box::pin(async move {
            match status.as_str() {
                "merged" => {
                    conn.execute(
                        "UPDATE merge_requests
                         SET status = 'merged',
                             merged_at = ?1,
                             updated_at = ?2
                         WHERE job_id = ?3",
                        params![now, now, mr_job_id.as_str()],
                    )
                    .await?;
                }
                "closed" => {
                    conn.execute(
                        "UPDATE merge_requests
                         SET status = 'closed',
                             closed_at = ?1,
                             updated_at = ?2
                         WHERE job_id = ?3",
                        params![now, now, mr_job_id.as_str()],
                    )
                    .await?;
                }
                other => return Err(DbError::internal(format!("unknown MR status {other}"))),
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to update merge_request: {}", e))?;

    Ok(())
}

fn action_config_from_row(row: &turso::Row) -> Result<ActionConfig, DbError> {
    let input_schema = parse_json_option(row.opt_text(4)?, "input_schema")?;
    let output_schema = parse_json_option(row.opt_text(5)?, "output_schema")?;

    Ok(ActionConfig {
        id: row.text(0)?,
        name: row.text(1)?,
        description: row.text(2)?,
        command_template: row.opt_text(3)?,
        input_schema,
        output_schema,
        is_builtin: row.i64(6)? != 0,
        workspace_id: row.opt_text(7)?,
        project_id: row.opt_text(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        tool_name: row.opt_text(11)?,
        tool_description: row.opt_text(12)?,
    })
}

fn artifact_from_row(row: &turso::Row) -> Result<Artifact, DbError> {
    let data = serde_json::from_str(&row.text(4)?)
        .map_err(|error| DbError::Row(format!("invalid artifact data JSON: {error}")))?;

    Ok(Artifact {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        artifact_type: row.text(2)?,
        schema_version: row.i64(3)? as i32,
        data,
        version: row.i64(5)? as i32,
        parent_version_id: row.opt_text(6)?,
        output_name: row.opt_text(7)?,
        created_at: row.i64(8)?,
        updated_at: row.i64(9)?,
        seen_at: row.opt_i64(10)?,
        confirmed: row.i64(11)? != 0,
    })
}
