//! Job query functions — list jobs, compute tabs, fetch child jobs.

use std::collections::{HashMap, HashSet};

use diesel::prelude::*;

use crate::diesel_models::DbJob;
use crate::models::{ExecutionSnapshot, Job, RecipeNode};
use crate::orchestrator::Orchestrator;
use crate::schema::{artifacts, executions, jobs};

/// Tab computation result for a job.
pub struct JobTabs {
    /// Available tabs: `["chat"]` or `["chat", "artifact"]`
    pub available_tabs: Vec<String>,
    /// Initial tab to show: `"chat"` or `"artifact"`
    pub initial_tab: String,
}

/// Compute tabs for a batch of jobs.
/// Returns a `HashMap` of `job_id -> JobTabs`.
///
/// Logic:
/// - `available_tabs`: `["chat"]` if has_downstream_action OR no artifact,
///   `["chat", "artifact"]` otherwise.
/// - `initial_tab`: `"chat"` if running, `"artifact"` if available_tabs
///   includes artifact, `"chat"` otherwise.
pub fn compute_tabs_for_jobs(
    conn: &mut diesel::sqlite::SqliteConnection,
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
    conn: &mut diesel::sqlite::SqliteConnection,
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
        None => return Ok(false),
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

/// Get all jobs for an issue.
pub fn list_jobs_for_issue_impl(orch: &Orchestrator, issue_id: &str) -> Result<Vec<Job>, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .order(jobs::created_at.asc())
        .load(&mut *conn)
        .map_err(|e| format!("Failed to load jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(&mut conn, &job_ids)?;

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
            .load::<(String, Option<i32>)>(&mut *conn)
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
            if let Some(exec_id) = execution_id {
                job.exec_seq = exec_seqs.get(&exec_id).copied();
            }
            Ok(job)
        })
        .collect()
}

/// Get a single job by ID.
pub fn get_job_impl(orch: &Orchestrator, job_id: &str) -> Result<Job, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let db_job: DbJob = jobs::table
        .find(job_id)
        .first(&mut *conn)
        .map_err(|e| format!("Job not found: {}", e))?;

    let execution_id = db_job.execution_id.clone();
    let mut job = Job::try_from(db_job)?;
    let tabs = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id.to_string()))?;
    if let Some(job_tabs) = tabs.get(job_id) {
        job.available_tabs = job_tabs.available_tabs.clone();
        job.initial_tab = job_tabs.initial_tab.clone();
    }
    if let Some(exec_id) = execution_id {
        job.exec_seq = executions::table
            .filter(executions::id.eq(&exec_id))
            .select(executions::seq)
            .first(&mut *conn)
            .ok()
            .flatten();
    }
    Ok(job)
}

/// List child jobs for a batch_tasks call by `parent_tool_use_id`.
pub fn list_child_jobs_impl(
    orch: &Orchestrator,
    parent_tool_use_id: &str,
) -> Result<Vec<Job>, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::parent_tool_use_id.eq(parent_tool_use_id))
        .order(jobs::task_index.asc())
        .load(&mut *conn)
        .map_err(|e| format!("Failed to list child jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(&mut conn, &job_ids)?;

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

/// List child jobs by `parent_job_id` (for querying subagent children).
pub fn list_child_jobs_by_parent_impl(
    orch: &Orchestrator,
    parent_job_id: &str,
) -> Result<Vec<Job>, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::parent_job_id.eq(parent_job_id))
        .order(jobs::task_index.asc())
        .then_order_by(jobs::created_at.asc())
        .load(&mut *conn)
        .map_err(|e| format!("Failed to list child jobs: {}", e))?;

    let job_ids: Vec<String> = db_jobs.iter().map(|j| j.id.clone()).collect();
    let tabs = compute_tabs_for_jobs(&mut conn, &job_ids)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewExecution, NewJob};
    use crate::models::{
        NodePosition, RecipeContext, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType,
        RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };
    use crate::schema::executions;
    use crate::test_utils::{create_test_project, test_diesel_conn};
    use uuid::Uuid;

    fn create_test_execution_with_snapshot(
        conn: &mut SqliteConnection,
        project_id: &str,
        nodes: Vec<RecipeNode>,
        edges: Vec<RecipeEdge>,
    ) -> String {
        let recipe_id = Uuid::new_v4().to_string();
        let execution_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        let recipe_snapshot = RecipeSnapshot {
            id: recipe_id.clone(),
            name: "Test Recipe".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            context: RecipeContext::Project,
            nodes,
            edges,
        };

        let snapshot = ExecutionSnapshot::new(
            recipe_snapshot,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            TriggerContext {
                issue_id: None,
                project_id: project_id.to_string(),
                trigger_type: TriggerType::Manual,
                issue_skills: vec![],
            },
        );

        let snapshot_json = serde_json::to_string(&snapshot).unwrap();

        diesel::insert_into(executions::table)
            .values(&NewExecution {
                id: &execution_id,
                recipe_id: &recipe_id,
                issue_id: None,
                project_id: Some(project_id),
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: None,
            })
            .execute(conn)
            .unwrap();

        execution_id
    }

    fn make_node(id: &str, node_type: RecipeNodeType, name: &str) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type,
            name: name.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
            parent_id: None,
        }
    }

    fn make_edge(source_id: &str, target_id: &str, edge_type: RecipeEdgeType) -> RecipeEdge {
        RecipeEdge {
            id: Uuid::new_v4().to_string(),
            source_node_id: source_id.to_string(),
            target_node_id: target_id.to_string(),
            source_handle: "context".to_string(),
            target_handle: "context".to_string(),
            edge_type,
        }
    }

    fn create_test_job(
        conn: &mut SqliteConnection,
        project_id: &str,
        execution_id: Option<&str>,
        recipe_node_id: Option<&str>,
    ) -> String {
        let job_id = Uuid::new_v4().to_string();
        let session_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(jobs::table)
            .values(&NewJob {
                id: &job_id,
                execution_id,
                recipe_node_id,
                parent_job_id: None,
                worktree_path: None,
                branch: None,
                base_commit: None,
                claude_session_id: Some(&session_id),
                status: "pending",
                agent_config_id: None,
                issue_id: None,
                project_id,
                task_description: None,
                created_at: now,
                updated_at: now,
                completed_at: None,
                model: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: None,
                node_name: None,
            })
            .execute(conn)
            .unwrap();

        job_id
    }

    #[test]
    fn test_tabs_single_downstream_action() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let agent_node_id = "agent-1";
        let action_node_id = "action-1";

        let nodes = vec![
            make_node(agent_node_id, RecipeNodeType::Agent, "Build"),
            make_node(action_node_id, RecipeNodeType::Action, "create_pr"),
        ];
        let edges = vec![make_edge(
            agent_node_id,
            action_node_id,
            RecipeEdgeType::Context,
        )];

        let execution_id =
            create_test_execution_with_snapshot(&mut conn, &project_id, nodes, edges);
        let job_id = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_id),
        );

        let result = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id)).unwrap();
        let tabs = result.get(&job_id).unwrap();

        assert_eq!(tabs.available_tabs, vec!["chat"]);
        assert_eq!(tabs.initial_tab, "chat");
    }

    #[test]
    fn test_tabs_standalone_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let job_id = create_test_job(&mut conn, &project_id, None, None);

        let result = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id)).unwrap();
        let tabs = result.get(&job_id).unwrap();

        assert_eq!(tabs.available_tabs, vec!["chat"]);
        assert_eq!(tabs.initial_tab, "chat");
    }

    #[test]
    fn test_tabs_no_downstream() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let agent_node_id = "agent-1";
        let nodes = vec![make_node(agent_node_id, RecipeNodeType::Agent, "Plan")];
        let edges = vec![];

        let execution_id =
            create_test_execution_with_snapshot(&mut conn, &project_id, nodes, edges);
        let job_id = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_id),
        );

        let result = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id)).unwrap();
        let tabs = result.get(&job_id).unwrap();

        assert_eq!(tabs.available_tabs, vec!["chat"]);
        assert_eq!(tabs.initial_tab, "chat");
    }

    #[test]
    fn test_tabs_multiple_downstream() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let agent_node_id = "agent-1";
        let action_node_id = "action-1";
        let checkpoint_node_id = "checkpoint-1";

        let nodes = vec![
            make_node(agent_node_id, RecipeNodeType::Agent, "Build"),
            make_node(action_node_id, RecipeNodeType::Action, "create_pr"),
            make_node(checkpoint_node_id, RecipeNodeType::Checkpoint, "Review"),
        ];
        let edges = vec![
            make_edge(agent_node_id, action_node_id, RecipeEdgeType::Context),
            make_edge(agent_node_id, checkpoint_node_id, RecipeEdgeType::Context),
        ];

        let execution_id =
            create_test_execution_with_snapshot(&mut conn, &project_id, nodes, edges);
        let job_id = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_id),
        );

        let result = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id)).unwrap();
        let tabs = result.get(&job_id).unwrap();

        assert_eq!(tabs.available_tabs, vec!["chat"]);
        assert_eq!(tabs.initial_tab, "chat");
    }

    #[test]
    fn test_tabs_single_downstream_agent() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let agent_node_1 = "agent-1";
        let agent_node_2 = "agent-2";

        let nodes = vec![
            make_node(agent_node_1, RecipeNodeType::Agent, "Plan"),
            make_node(agent_node_2, RecipeNodeType::Agent, "Build"),
        ];
        let edges = vec![make_edge(
            agent_node_1,
            agent_node_2,
            RecipeEdgeType::Context,
        )];

        let execution_id =
            create_test_execution_with_snapshot(&mut conn, &project_id, nodes, edges);
        let job_id = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_1),
        );

        let result = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id)).unwrap();
        let tabs = result.get(&job_id).unwrap();

        assert_eq!(tabs.available_tabs, vec!["chat"]);
        assert_eq!(tabs.initial_tab, "chat");
    }

    #[test]
    fn test_tabs_single_downstream_checkpoint() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let agent_node_id = "agent-1";
        let checkpoint_node_id = "checkpoint-1";

        let nodes = vec![
            make_node(agent_node_id, RecipeNodeType::Agent, "Build"),
            make_node(checkpoint_node_id, RecipeNodeType::Checkpoint, "Review"),
        ];
        let edges = vec![make_edge(
            agent_node_id,
            checkpoint_node_id,
            RecipeEdgeType::Context,
        )];

        let execution_id =
            create_test_execution_with_snapshot(&mut conn, &project_id, nodes, edges);
        let job_id = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_id),
        );

        let result = compute_tabs_for_jobs(&mut conn, std::slice::from_ref(&job_id)).unwrap();
        let tabs = result.get(&job_id).unwrap();

        assert_eq!(tabs.available_tabs, vec!["chat"]);
        assert_eq!(tabs.initial_tab, "chat");
    }

    #[test]
    fn test_tabs_batch_jobs() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let agent_node_1 = "agent-1";
        let agent_node_2 = "agent-2";
        let action_node = "action-1";

        let nodes = vec![
            make_node(agent_node_1, RecipeNodeType::Agent, "Build"),
            make_node(agent_node_2, RecipeNodeType::Agent, "Plan"),
            make_node(action_node, RecipeNodeType::Action, "create_pr"),
        ];
        let edges = vec![make_edge(
            agent_node_1,
            action_node,
            RecipeEdgeType::Context,
        )];

        let execution_id =
            create_test_execution_with_snapshot(&mut conn, &project_id, nodes, edges);

        let job_id_1 = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_1),
        );
        let job_id_2 = create_test_job(
            &mut conn,
            &project_id,
            Some(&execution_id),
            Some(agent_node_2),
        );
        let job_id_3 = create_test_job(&mut conn, &project_id, None, None);

        let result = compute_tabs_for_jobs(
            &mut conn,
            &[job_id_1.clone(), job_id_2.clone(), job_id_3.clone()],
        )
        .unwrap();

        let tabs_1 = result.get(&job_id_1).unwrap();
        assert_eq!(tabs_1.available_tabs, vec!["chat"]);

        let tabs_2 = result.get(&job_id_2).unwrap();
        assert_eq!(tabs_2.available_tabs, vec!["chat"]);

        let tabs_3 = result.get(&job_id_3).unwrap();
        assert_eq!(tabs_3.available_tabs, vec!["chat"]);
    }
}
