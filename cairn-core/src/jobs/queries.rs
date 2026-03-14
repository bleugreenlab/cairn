//! Database query helpers for jobs and execution snapshots.
//!
//! These functions take a `&mut SqliteConnection` directly to enable
//! testing without Tauri state. Callers are responsible for obtaining
//! the connection.

use std::collections::{HashMap, HashSet};

use crate::diesel_models::DbJob;
use crate::models::{ExecutionSnapshot, Job, RecipeNode};
use crate::schema::{artifacts, executions, issues, jobs, projects, runs};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::Serialize;

/// Implementation details needed for PR operations.
#[allow(dead_code)]
pub struct ImplementationInfo {
    pub job_id: String,
    pub worktree_path: String,
    pub branch: String,
    pub project_id: String,
}

/// Get implementation details for PR creation by worktree path (cwd).
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
pub fn get_implementation_for_pr_by_cwd(
    conn: &mut SqliteConnection,
    cwd: &str,
) -> Result<ImplementationInfo, String> {
    // Query for running jobs at this worktree, ordered by most recent
    let results: Vec<(
        String,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
    )> = runs::table
        .inner_join(jobs::table.on(runs::job_id.assume_not_null().eq(jobs::id)))
        .inner_join(issues::table.on(jobs::issue_id.assume_not_null().eq(issues::id)))
        .inner_join(projects::table.on(issues::project_id.eq(projects::id)))
        .filter(jobs::worktree_path.eq(cwd))
        .filter(runs::status.eq("running"))
        .order(runs::created_at.desc())
        .select((
            jobs::id,
            jobs::worktree_path,
            jobs::branch,
            projects::id,
            jobs::execution_id,
            jobs::recipe_node_id,
        ))
        .load(conn)
        .map_err(|e| format!("Failed to query jobs: {}", e))?;

    // Find the first job that is an "agent" node
    for (job_id, wt_path, branch, project_id, execution_id, node_id) in results {
        // Check node type from execution snapshot
        let node_type = get_node_type_for_job(conn, execution_id.as_deref(), node_id.as_deref());
        if node_type.as_deref() == Some("agent") {
            let worktree_path =
                wt_path.ok_or_else(|| "Implementation has no worktree path".to_string())?;
            let branch = branch.ok_or_else(|| "Implementation has no branch name".to_string())?;

            return Ok(ImplementationInfo {
                job_id,
                worktree_path,
                branch,
                project_id,
            });
        }
    }

    Err(format!(
        "No active implementation run for worktree '{}'",
        cwd
    ))
}

/// Get implementation artifact data (PR title/body) for a job.
///
/// Implementation artifacts contain PR details submitted by the agent via submit_output.
/// Returns (title, body) where body may be None.
#[allow(dead_code)]
pub fn get_implementation_artifact(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<(String, Option<String>), String> {
    // Get latest artifact for job
    let artifact_data: String = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .order(artifacts::version.desc())
        .select(artifacts::data)
        .first(conn)
        .map_err(|_| "No implementation artifact found. Agent must call submit_output first.")?;

    // Parse JSON and extract title/body
    let data: serde_json::Value = serde_json::from_str(&artifact_data)
        .map_err(|e| format!("Invalid artifact data: {}", e))?;

    let title = data
        .get("title")
        .and_then(|v| v.as_str())
        .ok_or("Artifact missing 'title' field")?
        .to_string();

    let body = data
        .get("body")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok((title, body))
}

/// Load an execution snapshot from the database.
pub fn load_execution_snapshot(
    conn: &mut SqliteConnection,
    execution_id: &str,
) -> Result<ExecutionSnapshot, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Execution not found: {}", e))?;

    let snapshot_str = snapshot_json.ok_or("Execution has no snapshot")?;

    serde_json::from_str(&snapshot_str)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))
}

/// Find a recipe node in an execution snapshot by node ID.
pub fn find_node_in_snapshot<'a>(
    snapshot: &'a ExecutionSnapshot,
    node_id: &str,
) -> Option<&'a RecipeNode> {
    snapshot.recipe.nodes.iter().find(|n| n.id == node_id)
}

/// Load a recipe node from the execution snapshot for a job.
/// Returns (node, snapshot) if found.
pub fn load_node_for_job(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Option<(RecipeNode, ExecutionSnapshot)>, String> {
    // Get job's execution_id and recipe_node_id
    let job_data: Option<(Option<String>, Option<String>)> = jobs::table
        .find(job_id)
        .select((jobs::execution_id, jobs::recipe_node_id))
        .first(conn)
        .ok();

    let Some((Some(execution_id), Some(node_id))) = job_data else {
        return Ok(None);
    };

    let snapshot = load_execution_snapshot(conn, &execution_id)?;
    let node = find_node_in_snapshot(&snapshot, &node_id).cloned();

    Ok(node.map(|n| (n, snapshot)))
}

/// Get the node type for a job from its execution snapshot.
pub fn get_node_type_for_job(
    conn: &mut SqliteConnection,
    execution_id: Option<&str>,
    node_id: Option<&str>,
) -> Option<String> {
    let (execution_id, node_id) = match (execution_id, node_id) {
        (Some(e), Some(n)) => (e, n),
        _ => return None,
    };

    let snapshot = load_execution_snapshot(conn, execution_id).ok()?;
    let node = find_node_in_snapshot(&snapshot, node_id)?;
    Some(node.node_type.to_string())
}

/// Get node name from execution snapshot (node_id is the UUID).
/// Returns the human-readable name (e.g., "Planner", "Builder") or None.
pub fn get_node_name_from_execution(
    conn: &mut SqliteConnection,
    execution_id: &str,
    node_id: &str,
) -> Option<String> {
    let snapshot = load_execution_snapshot(conn, execution_id).ok()?;
    find_node_in_snapshot(&snapshot, node_id).map(|n| n.name.clone())
}

/// Get parent job info for a task-spawned job.
/// Returns (parent_job_id, agent_config_id, task_index) if this is a task job.
pub fn get_task_parent_info(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Option<(String, Option<String>, Option<i32>)> {
    let result: Option<(Option<String>, Option<String>, Option<i32>)> = jobs::table
        .find(job_id)
        .select((jobs::parent_job_id, jobs::agent_config_id, jobs::task_index))
        .first(conn)
        .ok();

    result.and_then(|(parent_id, agent_config_id, task_index)| {
        parent_id.map(|p| (p, agent_config_id, task_index))
    })
}

/// Get node name for a job by looking up its execution and recipe_node_id.
pub fn get_node_name_for_job(conn: &mut SqliteConnection, job_id: &str) -> Option<String> {
    let job_data: Option<(Option<String>, Option<String>)> = jobs::table
        .find(job_id)
        .select((jobs::execution_id, jobs::recipe_node_id))
        .first(conn)
        .ok();

    match job_data {
        Some((Some(execution_id), Some(recipe_node_id))) => {
            get_node_name_from_execution(conn, &execution_id, &recipe_node_id)
        }
        _ => None,
    }
}

/// Tab computation result for a job
#[derive(Serialize)]
pub struct JobTabs {
    /// Available tabs: ["chat"] or ["chat", "artifact"]
    pub available_tabs: Vec<String>,
    /// Initial tab to show: "chat" or "artifact"
    pub initial_tab: String,
}

/// Compute tabs for a batch of jobs.
/// Returns a HashMap of job_id -> JobTabs
///
/// Logic:
/// - available_tabs: `["chat"]` if has_downstream_action OR no artifact,
///   `["chat", "artifact"]` otherwise
/// - initial_tab: `"chat"` if running, `"artifact"` if available_tabs
///   includes artifact, `"chat"` otherwise
pub fn compute_tabs_for_jobs(
    conn: &mut SqliteConnection,
    job_ids: &[String],
) -> Result<HashMap<String, JobTabs>, String> {
    if job_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Get job statuses and recipe info
    let jobs_info: Vec<(String, String, Option<String>, Option<String>)> = jobs::table
        .filter(jobs::id.eq_any(job_ids))
        .select((
            jobs::id,
            jobs::status,
            jobs::recipe_node_id,
            jobs::execution_id,
        ))
        .load(conn)
        .map_err(|e| format!("Failed to query jobs: {}", e))?;

    // Get jobs that have artifacts
    let jobs_with_artifacts: HashSet<String> = artifacts::table
        .filter(artifacts::job_id.eq_any(job_ids))
        .select(artifacts::job_id)
        .distinct()
        .load::<Option<String>>(conn)
        .map_err(|e| format!("Failed to query artifacts: {}", e))?
        .into_iter()
        .flatten()
        .collect();

    // Compute tabs for each job
    let mut result = HashMap::new();
    for (job_id, status, recipe_node_id, execution_id) in jobs_info {
        let has_artifact = jobs_with_artifacts.contains(&job_id);

        // Check if output feeds into downstream action (hide artifact tab)
        let has_downstream_action = match (recipe_node_id, execution_id) {
            (Some(node_id), Some(exec_id)) => {
                has_single_downstream_action(conn, &node_id, &exec_id)?
            }
            _ => false,
        };

        // Determine available_tabs
        let available_tabs = if has_downstream_action || !has_artifact {
            vec!["chat".to_string()]
        } else {
            vec!["chat".to_string(), "artifact".to_string()]
        };

        // Determine initial_tab
        let initial_tab = if status == "running" {
            "chat".to_string()
        } else if available_tabs.contains(&"artifact".to_string()) {
            "artifact".to_string()
        } else {
            "chat".to_string()
        };

        result.insert(
            job_id,
            JobTabs {
                available_tabs,
                initial_tab,
            },
        );
    }

    Ok(result)
}

/// Check if a job's output feeds into exactly one downstream ActionNode via context edge.
fn has_single_downstream_action(
    conn: &mut SqliteConnection,
    node_id: &str,
    execution_id: &str,
) -> Result<bool, String> {
    // Load execution snapshot
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .ok()
        .flatten();

    let snapshot_json = match snapshot_json {
        Some(s) => s,
        None => return Ok(false), // No snapshot means we can't determine
    };

    let snapshot: ExecutionSnapshot = match serde_json::from_str(&snapshot_json) {
        Ok(s) => s,
        Err(_) => return Ok(false),
    };

    // Build node lookup map
    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();

    // Find outgoing context edges from this node
    let target_node_ids: Vec<&str> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.edge_type.to_string() == "context" && e.source_node_id == node_id)
        .map(|e| e.target_node_id.as_str())
        .collect();

    // Must be exactly one downstream context edge
    if target_node_ids.len() != 1 {
        return Ok(false);
    }

    // Check if that single target is an action node
    let is_action = node_map
        .get(target_node_ids[0])
        .map(|n| n.node_type.to_string() == "action")
        .unwrap_or(false);

    Ok(is_action)
}

/// Get a single job by ID with computed tabs and exec_seq.
pub fn get_job(conn: &mut SqliteConnection, job_id: &str) -> Result<Job, String> {
    let db_job: DbJob = jobs::table
        .find(job_id)
        .first(conn)
        .map_err(|e| format!("Job not found: {}", e))?;

    let execution_id = db_job.execution_id.clone();
    let mut job = Job::try_from(db_job)?;
    let tabs = compute_tabs_for_jobs(conn, std::slice::from_ref(&job_id.to_string()))?;
    if let Some(job_tabs) = tabs.get(job_id) {
        job.available_tabs = job_tabs.available_tabs.clone();
        job.initial_tab = job_tabs.initial_tab.clone();
    }
    // Populate exec_seq from the execution
    if let Some(exec_id) = execution_id {
        job.exec_seq = executions::table
            .filter(executions::id.eq(&exec_id))
            .select(executions::seq)
            .first(conn)
            .ok()
            .flatten();
    }
    Ok(job)
}

/// Get all jobs for an issue with computed tabs and exec_seq.
pub fn list_jobs_for_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Result<Vec<Job>, String> {
    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .order(jobs::created_at.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(conn, &job_ids)?;

    // Collect unique execution IDs and fetch their seq values
    let execution_ids: Vec<String> = db_jobs
        .iter()
        .filter_map(|j| j.execution_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let exec_seqs: HashMap<String, i32> = if !execution_ids.is_empty() {
        executions::table
            .filter(executions::id.eq_any(&execution_ids))
            .select((executions::id, executions::seq))
            .load::<(String, Option<i32>)>(conn)
            .map_err(|e| format!("Failed to load execution seqs: {}", e))?
            .into_iter()
            .filter_map(|(id, seq)| seq.map(|s| (id, s)))
            .collect()
    } else {
        HashMap::new()
    };

    db_jobs
        .into_iter()
        .map(|db_job| {
            let job_id = db_job.id.clone();
            let execution_id = db_job.execution_id.clone();
            let mut job = Job::try_from(db_job)?;
            if let Some(job_tabs) = tabs.get(&job_id) {
                job.available_tabs = job_tabs.available_tabs.clone();
                job.initial_tab = job_tabs.initial_tab.clone();
            }
            // Populate exec_seq from the execution
            if let Some(exec_id) = execution_id {
                job.exec_seq = exec_seqs.get(&exec_id).copied();
            }
            Ok(job)
        })
        .collect()
}

/// List child jobs for a batch_tasks call by parent_tool_use_id.
pub fn list_child_jobs(
    conn: &mut SqliteConnection,
    parent_tool_use_id: &str,
) -> Result<Vec<Job>, String> {
    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::parent_tool_use_id.eq(parent_tool_use_id))
        .order(jobs::task_index.asc())
        .load(conn)
        .map_err(|e| format!("Failed to list child jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(conn, &job_ids)?;

    db_jobs
        .into_iter()
        .map(|db_job| {
            let job_id = db_job.id.clone();
            let mut job = Job::try_from(db_job)?;
            if let Some(job_tabs) = tabs.get(&job_id) {
                job.available_tabs = job_tabs.available_tabs.clone();
                job.initial_tab = job_tabs.initial_tab.clone();
            }
            Ok(job)
        })
        .collect()
}

/// List child jobs by parent_job_id (for querying subagent children).
pub fn list_child_jobs_by_parent(
    conn: &mut SqliteConnection,
    parent_job_id: &str,
) -> Result<Vec<Job>, String> {
    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::parent_job_id.eq(parent_job_id))
        .order(jobs::task_index.asc())
        .then_order_by(jobs::created_at.asc())
        .load(conn)
        .map_err(|e| format!("Failed to list child jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(conn, &job_ids)?;

    db_jobs
        .into_iter()
        .map(|db_job| {
            let job_id = db_job.id.clone();
            let mut job = Job::try_from(db_job)?;
            if let Some(job_tabs) = tabs.get(&job_id) {
                job.available_tabs = job_tabs.available_tabs.clone();
                job.initial_tab = job_tabs.initial_tab.clone();
            }
            Ok(job)
        })
        .collect()
}

/// Get jobs for an execution with computed tabs and exec_seq.
pub fn list_jobs_for_execution(
    conn: &mut SqliteConnection,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    // Get the execution's seq value
    let exec_seq: Option<i32> = executions::table
        .filter(executions::id.eq(execution_id))
        .select(executions::seq)
        .first(conn)
        .ok()
        .flatten();

    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .order(jobs::created_at.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(conn, &job_ids)?;

    db_jobs
        .into_iter()
        .map(|db_job| {
            let job_id = db_job.id.clone();
            let mut job = Job::try_from(db_job)?;
            if let Some(job_tabs) = tabs.get(&job_id) {
                job.available_tabs = job_tabs.available_tabs.clone();
                job.initial_tab = job_tabs.initial_tab.clone();
            }
            job.exec_seq = exec_seq;
            Ok(job)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewArtifact;
    use crate::test_utils::{create_test_issue, create_test_job, create_test_project};
    use diesel::RunQueryDsl;

    #[test]
    fn test_get_implementation_artifact_returns_pr_details() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let job_id = create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "implementation",
            "complete",
            None,
        );

        // Create artifact with PR details
        let now = chrono::Utc::now().timestamp() as i32;
        let artifact_id = uuid::Uuid::new_v4().to_string();
        let data = serde_json::json!({
            "title": "Add feature X",
            "body": "This PR adds feature X\n\n- Item 1\n- Item 2",
            "summary": "Added feature X"
        });
        let data_str = data.to_string();

        diesel::insert_into(artifacts::table)
            .values(NewArtifact {
                id: &artifact_id,
                job_id: Some(&job_id),
                artifact_type: "implementation",
                schema_version: 1,
                data: &data_str,
                version: 1,
                parent_version_id: None,
                output_name: None,
                created_at: now,
                updated_at: now,
            })
            .execute(&mut conn)
            .unwrap();

        // Test the helper
        let (title, body) = get_implementation_artifact(&mut conn, &job_id).unwrap();
        assert_eq!(title, "Add feature X");
        assert_eq!(
            body,
            Some("This PR adds feature X\n\n- Item 1\n- Item 2".to_string())
        );
    }

    #[test]
    fn test_get_implementation_artifact_no_artifact() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let job_id = create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "implementation",
            "running",
            None,
        );

        let result = get_implementation_artifact(&mut conn, &job_id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("No implementation artifact found"));
    }
}
