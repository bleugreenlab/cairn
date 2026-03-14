use crate::diesel_models::DbActionRun;
use crate::diesel_models::DbConditionEvaluation;
use crate::models::{ActionRun, ConditionEvaluation};
use crate::schema::{action_runs, condition_evaluations, executions};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

pub fn list_action_runs_for_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Result<Vec<ActionRun>, String> {
    let db_runs: Vec<DbActionRun> = action_runs::table
        .filter(action_runs::issue_id.eq(issue_id))
        .order(action_runs::created_at.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load action runs: {}", e))?;
    db_runs.into_iter().map(ActionRun::try_from).collect()
}

pub fn list_action_runs_for_execution(
    conn: &mut SqliteConnection,
    execution_id: &str,
) -> Result<Vec<ActionRun>, String> {
    let db_runs: Vec<DbActionRun> = action_runs::table
        .filter(action_runs::execution_id.eq(execution_id))
        .order(action_runs::created_at.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load action runs: {}", e))?;
    db_runs.into_iter().map(ActionRun::try_from).collect()
}

pub fn get_action_run_by_id(
    conn: &mut SqliteConnection,
    action_run_id: &str,
) -> Result<ActionRun, String> {
    let db_run: DbActionRun = action_runs::table
        .find(action_run_id)
        .first(conn)
        .map_err(|e| format!("Action run not found: {}", e))?;
    ActionRun::try_from(db_run)
}

pub fn list_condition_evaluations_for_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Result<Vec<ConditionEvaluation>, String> {
    let execution_ids: Vec<String> = executions::table
        .filter(executions::issue_id.eq(issue_id))
        .select(executions::id)
        .load(conn)
        .map_err(|e| format!("Failed to load executions: {}", e))?;
    let db_evals: Vec<DbConditionEvaluation> = condition_evaluations::table
        .filter(condition_evaluations::execution_id.eq_any(&execution_ids))
        .order(condition_evaluations::evaluated_at.asc())
        .load(conn)
        .map_err(|e| format!("Failed to load condition evaluations: {}", e))?;
    Ok(db_evals
        .into_iter()
        .map(ConditionEvaluation::from)
        .collect())
}
