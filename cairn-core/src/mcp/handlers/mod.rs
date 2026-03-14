//! MCP request handlers organized by domain.
//!
//! Framework-agnostic handler logic. Both Tauri and cairn-server dispatch to these.

use crate::jobs::queries::{find_node_in_snapshot, load_execution_snapshot};
use crate::models::CheckpointType;
use crate::schema::{issues as issues_table, jobs, projects, runs};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

pub mod agents;
pub mod bash;
pub mod bug_report;
pub mod custom_tool;
pub mod execute;
pub mod external;
pub mod files;
pub mod implementation;
pub mod issue_resources;
pub mod issues;
pub mod memories;
pub mod messages;
pub mod permission;
pub mod planning;
pub mod resources;
pub mod search;
pub mod skills;
pub mod slug;

// Re-export slug utilities
pub use slug::{
    build_terminal_uri, ensure_unique_slug, generate_slug_from_title, generate_terminal_slug,
    slugify, slugify_command,
};

/// Information about a run looked up by worktree path
pub struct RunContext {
    pub run_id: String,
    pub job_id: String,
    pub execution_id: Option<String>, // Groups related jobs in a recipe execution
    pub issue_id: Option<String>,     // Null for project-level runs
    pub issue_number: Option<i32>, // Issue number for building issue keys (e.g., 123 for CAIRN-123)
    pub project_id: String,
    pub project_key: String,
    pub job_type: String,         // From recipe_step node_type or inferred
    pub job_name: Option<String>, // Human-readable job name from execution snapshot (e.g., "builder-1")
}

impl RunContext {
    /// Get the issue key (e.g., "CAIRN-123") or None for project-level runs
    #[allow(dead_code)]
    pub fn issue_key(&self) -> Option<String> {
        self.issue_number
            .map(|num| format!("{}-{}", self.project_key, num))
    }
}

/// Minimal project context for external tools (no active run required)
pub struct ProjectContext {
    pub project_id: String,
    pub project_key: String,
}

/// Look up the active run by working directory (worktree_path or repo_path).
/// For issue jobs: matches worktree_path
/// For project jobs: matches repo_path directly
/// Returns run context or an error message.
pub fn lookup_run_by_cwd(conn: &mut SqliteConnection, cwd: &str) -> Result<RunContext, String> {
    // First try: worktree-based lookup (issue jobs)
    let worktree_result: Result<RunContext, _> = runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .inner_join(issues_table::table.on(runs::issue_id.eq(issues_table::id.nullable())))
        .inner_join(projects::table.on(issues_table::project_id.eq(projects::id)))
        .filter(jobs::worktree_path.eq(cwd))
        .filter(runs::status.eq("running"))
        .order(runs::created_at.desc())
        .select((
            runs::id,
            runs::job_id,
            jobs::execution_id,
            jobs::recipe_node_id,
            runs::issue_id,
            issues_table::number,
            issues_table::project_id,
            projects::key,
        ))
        .first::<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            i32,
            String,
            String,
        )>(conn)
        .map(
            |(
                run_id,
                job_id,
                execution_id,
                recipe_node_id,
                issue_id,
                issue_number,
                project_id,
                project_key,
            )| {
                // Get node_type and node_name from execution snapshot
                let (node_type, node_name) = execution_id
                    .as_ref()
                    .and_then(|exec_id| load_execution_snapshot(conn, exec_id).ok())
                    .and_then(|snapshot| {
                        recipe_node_id
                            .as_ref()
                            .and_then(|nid| find_node_in_snapshot(&snapshot, nid))
                            .map(|n| (Some(n.node_type.to_string()), Some(n.name.clone())))
                    })
                    .unwrap_or((None, None));

                RunContext {
                    run_id,
                    job_id: job_id.unwrap_or_default(),
                    execution_id,
                    issue_id,
                    issue_number: Some(issue_number),
                    project_id,
                    project_key,
                    // Map node_type: "agent" is the new implementation type
                    job_type: match node_type.as_deref() {
                        Some("agent") => "implementation".to_string(),
                        Some(t) => t.to_string(),
                        None => "implementation".to_string(),
                    },
                    job_name: node_name,
                }
            },
        );

    if worktree_result.is_ok() {
        return worktree_result.map_err(|e| e.to_string());
    }

    // Fallback: repo_path lookup (project-level jobs use repo_path directly)
    // Project jobs are identified by project_id set and issue_id null
    runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .inner_join(projects::table.on(jobs::project_id.eq(projects::id)))
        .filter(projects::repo_path.eq(cwd))
        .filter(runs::status.eq("running"))
        .filter(jobs::issue_id.is_null())
        .order(runs::created_at.desc())
        .select((
            runs::id,
            runs::job_id,
            jobs::execution_id,
            runs::issue_id,
            jobs::project_id,
            projects::key,
        ))
        .first::<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            String,
        )>(conn)
        .map(
            |(run_id, job_id, execution_id, issue_id, project_id, project_key)| {
                RunContext {
                    run_id,
                    job_id: job_id.unwrap_or_default(),
                    execution_id,
                    issue_id,
                    issue_number: None, // Project-level jobs have no issue
                    project_id,
                    project_key,
                    job_type: "project".to_string(), // Project-level jobs
                    job_name: None,                  // Project-level jobs have no node name
                }
            },
        )
        .map_err(|e| format!("No active run found for path '{}': {}", cwd, e))
}

/// Look up run by ID directly (preferred for accuracy).
/// This avoids the ambiguity of cwd-based lookup when multiple runs share a worktree.
pub fn lookup_run_by_id(conn: &mut SqliteConnection, run_id: &str) -> Result<RunContext, String> {
    runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .left_join(issues_table::table.on(runs::issue_id.eq(issues_table::id.nullable())))
        .inner_join(projects::table.on(jobs::project_id.eq(projects::id)))
        .filter(runs::id.eq(run_id))
        .select((
            runs::id,
            runs::job_id,
            jobs::execution_id,
            jobs::recipe_node_id,
            runs::issue_id,
            issues_table::number.nullable(),
            jobs::project_id,
            projects::key,
        ))
        .first::<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i32>,
            String,
            String,
        )>(conn)
        .map(
            |(
                run_id,
                job_id,
                execution_id,
                recipe_node_id,
                issue_id,
                issue_number,
                project_id,
                project_key,
            )| {
                // Get node_type and node_name from execution snapshot
                let (node_type, node_name) = execution_id
                    .as_ref()
                    .and_then(|exec_id| load_execution_snapshot(conn, exec_id).ok())
                    .and_then(|snapshot| {
                        recipe_node_id
                            .as_ref()
                            .and_then(|nid| find_node_in_snapshot(&snapshot, nid))
                            .map(|n| (Some(n.node_type.to_string()), Some(n.name.clone())))
                    })
                    .unwrap_or((None, None));

                RunContext {
                    run_id,
                    job_id: job_id.unwrap_or_default(),
                    execution_id,
                    issue_id,
                    issue_number,
                    project_id,
                    project_key,
                    job_type: match node_type.as_deref() {
                        Some("agent") => "implementation".to_string(),
                        Some(t) => t.to_string(),
                        None => "implementation".to_string(),
                    },
                    job_name: node_name,
                }
            },
        )
        .map_err(|e| format!("No run found with id '{}': {}", run_id, e))
}

/// Look up run using run_id if provided, otherwise fall back to cwd.
/// This is the preferred method for handlers - use run_id for accuracy when available.
/// If run_id is provided but doesn't exist (stale), falls back to cwd lookup.
pub fn lookup_run(
    conn: &mut SqliteConnection,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<RunContext, String> {
    if let Some(ref run_id) = request.run_id {
        // Try run_id first, fall back to cwd if run doesn't exist (stale run_id)
        match lookup_run_by_id(conn, run_id) {
            Ok(ctx) => Ok(ctx),
            Err(_) => lookup_run_by_cwd(conn, &request.cwd),
        }
    } else {
        lookup_run_by_cwd(conn, &request.cwd)
    }
}

/// Look up project by repo_path (for external tools that don't require an active run).
/// Returns project context or an error message.
pub fn lookup_project_by_cwd(
    conn: &mut SqliteConnection,
    cwd: &str,
) -> Result<ProjectContext, String> {
    projects::table
        .filter(projects::repo_path.eq(cwd))
        .select((projects::id, projects::key, projects::repo_path))
        .first::<(String, String, String)>(conn)
        .map(|(project_id, project_key, _repo_path)| ProjectContext {
            project_id,
            project_key,
        })
        .map_err(|_| format!("No Cairn project found for path '{}'. Make sure this directory is registered as a Cairn project.", cwd))
}

/// Look up project context, preferring run context but falling back to project-only.
/// Use this for tools that only need project_id (list_issues, docs, skills, etc.)
pub fn lookup_project_context(
    conn: &mut SqliteConnection,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<ProjectContext, String> {
    // Try run lookup first (provides richer context when available)
    if let Ok(run_ctx) = lookup_run(conn, request) {
        return Ok(ProjectContext {
            project_id: run_ctx.project_id,
            project_key: run_ctx.project_key,
        });
    }
    // Fall back to project-only lookup
    lookup_project_by_cwd(conn, &request.cwd)
}

/// Parse issue identifier into optional project key and issue number.
/// Returns (project_key, issue_number) - project_key is None if not specified.
///
/// Supported formats:
/// - "37" -> (None, 37)
/// - "#37" -> (None, 37)
/// - "CAIRN-37" -> (Some("CAIRN"), 37)
pub fn parse_issue_identifier(input: &str) -> Option<(Option<String>, i32)> {
    let trimmed = input.trim();

    // Try direct parse first (e.g., "37")
    if let Ok(n) = trimmed.parse::<i32>() {
        return Some((None, n));
    }

    // Handle "#37" format
    if let Some(stripped) = trimmed.strip_prefix('#') {
        if let Ok(n) = stripped.parse::<i32>() {
            return Some((None, n));
        }
    }

    // Handle "PREFIX-37" format (case-insensitive)
    if let Some(pos) = trimmed.rfind('-') {
        let prefix = &trimmed[..pos];
        if let Ok(n) = trimmed[pos + 1..].parse::<i32>() {
            return Some((Some(prefix.to_uppercase()), n));
        }
    }

    None
}

/// Look up project by key (for cross-project queries).
/// Returns project context or an error message.
pub fn lookup_project_by_key(
    conn: &mut SqliteConnection,
    key: &str,
) -> Result<ProjectContext, String> {
    projects::table
        .filter(projects::key.eq(key.to_uppercase()))
        .select((projects::id, projects::key, projects::repo_path))
        .first::<(String, String, String)>(conn)
        .map(|(project_id, project_key, _repo_path)| ProjectContext {
            project_id,
            project_key,
        })
        .map_err(|_| format!("No project found with key '{}'", key))
}

// ============================================================================
// Checkpoint Cache Helper Functions
// ============================================================================

/// Normalize command for matching: trim, collapse whitespace.
/// This ensures "bun run ci" matches "bun  run   ci".
pub fn normalize_command(cmd: &str) -> String {
    cmd.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Get current HEAD SHA for a worktree.
pub fn get_current_head_sha(worktree_path: &str) -> Result<String, String> {
    let output = crate::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("git rev-parse failed: {}", e))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err("Failed to get HEAD".to_string())
    }
}

/// Check if worktree has uncommitted changes.
pub fn is_worktree_dirty(worktree_path: &str) -> Result<bool, String> {
    let output = crate::env::git()
        .args(["status", "--porcelain"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("git status failed: {}", e))?;

    Ok(!output.stdout.is_empty())
}

/// Get checkpoint command for a job (from its parent agent's recipe node config).
/// This looks for checkpoint nodes connected to the agent node to determine
/// if the job has an associated programmatic checkpoint command.
pub fn get_job_checkpoint_command(conn: &mut SqliteConnection, job_id: &str) -> Option<String> {
    use crate::diesel_models::DbJob;
    use crate::jobs::queries::load_node_for_job;
    use crate::schema::jobs as jobs_table;

    // Get the job and its recipe_node_id
    let job: DbJob = jobs_table::table.find(job_id).first(conn).ok()?;
    let agent_node_id = job.recipe_node_id.as_ref()?;

    // Load the agent node from execution snapshot
    let (agent_node, snapshot) = load_node_for_job(conn, job_id).ok()??;

    // First, check if the agent node itself has a checkpoint in its agent_config
    if let Some(ref agent_cfg) = agent_node.agent_config {
        if let Some(ref checkpoint) = agent_cfg.checkpoint {
            if matches!(checkpoint.checkpoint_type, CheckpointType::Programmatic) {
                return checkpoint.command.clone();
            }
        }
    }

    // Second, look for slot-based checkpoint node attached to this agent
    // Slot checkpoints have parent_id pointing to the agent node
    let checkpoint_node = snapshot.recipe.nodes.iter().find(|n| {
        n.parent_id.as_deref() == Some(agent_node_id) && n.node_type.to_string() == "checkpoint"
    });

    if let Some(node) = checkpoint_node {
        if let Some(ref checkpoint_cfg) = node.checkpoint_config {
            if matches!(checkpoint_cfg.checkpoint_type, CheckpointType::Programmatic) {
                return checkpoint_cfg.command.clone();
            }
        }
    }

    None
}
