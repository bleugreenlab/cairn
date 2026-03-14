use crate::diesel_models::{DbActionRun, DbExecution, DbJob};
use crate::models::{
    ActionRun, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionSnapshot, ExecutionStatus, Job, TriggerType,
};
use crate::schema::{action_runs, executions, issues, jobs, projects};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use std::collections::HashMap;

/// Get the full execution snapshot for an execution
pub fn get_execution_snapshot(
    conn: &mut SqliteConnection,
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
    conn: &mut SqliteConnection,
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

/// Get the execution for an issue (if any)
pub fn get_execution_for_issue(
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

/// List all executions for an issue, ordered by started_at descending (newest first)
pub fn list_executions_for_issue(
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

/// Extract recipe name from execution snapshot JSON
fn extract_recipe_name_from_snapshot(snapshot_json: Option<&str>) -> String {
    snapshot_json
        .and_then(|s| serde_json::from_str::<ExecutionSnapshot>(s).ok())
        .map(|snap| snap.recipe.name)
        .unwrap_or_else(|| "Unknown Recipe".to_string())
}

/// List all executions with filters and pagination (for the Executions view)
pub fn list_all_executions(
    conn: &mut SqliteConnection,
    filters: &ExecutionFilters,
    limit: i64,
    offset: i64,
) -> Result<ExecutionListResult, String> {
    // Build base query
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
            "manual" => {
                query = query.filter(executions::issue_id.is_null());
            }
            _ => {
                query = query.filter(executions::issue_id.is_null());
            }
        }
    }

    // Get total count with same filters
    let count_query = executions::table
        .left_join(issues::table.on(issues::id.nullable().eq(executions::issue_id)))
        .left_join(projects::table.on(projects::id.nullable().eq(executions::project_id)))
        .into_boxed();

    let mut count_query = count_query;
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

    // Fetch paginated results
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

    // Get job counts for each execution
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

    // Build list items
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

/// Get full execution detail with related data
pub fn get_execution_detail(
    conn: &mut SqliteConnection,
    execution_id: &str,
) -> Result<ExecutionDetail, String> {
    let db_exec: DbExecution = executions::table
        .filter(executions::id.eq(execution_id))
        .first(conn)
        .map_err(|e| format!("Execution not found: {}", e))?;

    let recipe_name = extract_recipe_name_from_snapshot(db_exec.snapshot.as_deref());

    let execution: Execution = db_exec.try_into()?;

    // Get issue details if present
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

    // Get project details
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

    // Get jobs for this execution
    let db_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .order(jobs::created_at.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    let exec_jobs: Vec<Job> = db_jobs
        .into_iter()
        .map(Job::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    // Get action runs for this execution
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
