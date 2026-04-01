//! Execution and job query functions.
//!
//! Canonical home for all read-only execution queries (snapshots, node statuses,
//! listing, detail) **and** job queries (tabs, list, get, children).

use std::collections::{HashMap, HashSet};

use diesel::prelude::*;

use crate::diesel_models::{DbActionRun, DbExecution, DbJob};
use crate::models::{
    ActionRun, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionSnapshot, ExecutionStatus, Job, RecipeNode, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::schema::{action_runs, artifacts, executions, issues, jobs, projects};

// ── Execution queries ────────────────────────────────────────────

/// Get the full execution snapshot for an execution.
pub fn get_execution_snapshot(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Option<ExecutionSnapshot>, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load execution: {}", e))?
        .flatten();

    match snapshot_json {
        Some(json) => {
            let snapshot: ExecutionSnapshot = serde_json::from_str(&json)
                .map_err(|e| format!("Failed to parse snapshot: {}", e))?;
            Ok(Some(snapshot))
        }
        None => Ok(None),
    }
}

/// Get job statuses for all nodes in an execution.
///
/// Returns a map of recipe_node_id -> job status for the editor to display
/// execution state and restrict editing of running/complete nodes.
pub fn get_execution_node_statuses(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<HashMap<String, String>, String> {
    let job_data: Vec<(Option<String>, String)> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .select((jobs::recipe_node_id, jobs::status))
        .load(conn)
        .map_err(|e| format!("Failed to load jobs: {}", e))?;

    let mut statuses = HashMap::new();
    for (node_id, status) in job_data {
        if let Some(id) = node_id {
            statuses.insert(id, status);
        }
    }

    Ok(statuses)
}

/// Get the execution for an issue (if any).
pub fn get_execution_for_issue(
    conn: &mut diesel::sqlite::SqliteConnection,
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
pub fn list_executions_for_issue(
    conn: &mut diesel::sqlite::SqliteConnection,
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

/// Extract recipe name from execution snapshot JSON.
fn extract_recipe_name_from_snapshot(snapshot_json: Option<&str>) -> String {
    snapshot_json
        .and_then(|s| serde_json::from_str::<ExecutionSnapshot>(s).ok())
        .map(|snap| snap.recipe.name)
        .unwrap_or_else(|| "Unknown Recipe".to_string())
}

/// List all executions with filters and pagination (for the Executions view).
pub fn list_all_executions(
    conn: &mut diesel::sqlite::SqliteConnection,
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

    let job_counts_map: HashMap<String, (i64, i64)> = job_counts
        .into_iter()
        .map(|(exec_id, total, complete)| (exec_id, (total, complete)))
        .collect();

    let items: Vec<ExecutionListItem> = results
        .into_iter()
        .map(|(db_exec, issue_number, issue_project_id, project_name)| {
            let (jobs_total, jobs_complete) =
                job_counts_map.get(&db_exec.id).copied().unwrap_or((0, 0));

            let status: ExecutionStatus = db_exec.status.parse().unwrap_or_default();
            let triggered_by: TriggerType = db_exec.triggered_by.parse().unwrap_or_default();

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
pub fn get_execution_detail(
    conn: &mut diesel::sqlite::SqliteConnection,
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

    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .order(jobs::created_at.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    let exec_jobs: Vec<Job> = db_jobs
        .into_iter()
        .map(Job::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let db_action_runs: Vec<DbActionRun> = action_runs::table
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

// ── Trigger source queries ──────────────────────────────────────

/// Find all review executions triggered by jobs in a given execution.
///
/// Returns pairs of `(source_job_id, TriggeredExecution)` so the frontend
/// can map indicators to specific job rows in the execution panel.
pub fn list_triggered_executions_for_execution(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Vec<(String, crate::models::TriggeredExecution)>, String> {
    use crate::schema::execution_trigger_sources;

    // Find all trigger source rows where the source job belongs to this execution
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        String,
        String,
        String,
        i32,
        String,
        Option<String>,
        Option<i32>,
    )> = execution_trigger_sources::table
        .inner_join(jobs::table.on(jobs::id.eq(execution_trigger_sources::source_job_id)))
        .inner_join(
            executions::table
                .on(executions::id.eq(execution_trigger_sources::triggered_execution_id)),
        )
        .filter(jobs::execution_id.eq(execution_id))
        .select((
            execution_trigger_sources::source_job_id,
            executions::id,
            executions::status,
            executions::started_at,
            executions::triggered_by,
            executions::snapshot,
            executions::completed_at,
        ))
        .load(conn)
        .map_err(|e| format!("Failed to query trigger sources: {}", e))?;

    let mut results = Vec::new();
    for (
        source_job_id,
        exec_id,
        status_str,
        started_at,
        triggered_by_str,
        snapshot_json,
        completed_at,
    ) in rows
    {
        let status: ExecutionStatus = status_str.parse().unwrap_or_default();
        let triggered_by: TriggerType = triggered_by_str.parse().unwrap_or_default();
        let recipe_name = extract_recipe_name_from_snapshot(snapshot_json.as_deref());

        results.push((
            source_job_id,
            crate::models::TriggeredExecution {
                id: exec_id,
                recipe_name,
                status,
                triggered_by,
                started_at: started_at as i64,
                completed_at: completed_at.map(|t| t as i64),
            },
        ));
    }

    Ok(results)
}

// ── Job queries ──────────────────────────────────────────────────

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
        NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot,
        RecipeTrigger, TriggerContext, TriggerType,
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

                event_payload: None,
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
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
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
                manager_id: None,
                recipe_node_id,
                parent_job_id: None,
                worktree_path: None,
                branch: None,
                base_commit: None,
                current_session_id: Some(&session_id),
                resume_session_id: None,
                status: "pending",
                agent_config_id: None,
                issue_id: None,
                project_id,
                task_description: None,
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: None,
                model: None,
                node_name: None,
                base_branch: None,
                current_turn_id: None,
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
