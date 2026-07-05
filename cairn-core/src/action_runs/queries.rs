use crate::models::{ActionRun, ActionRunStatus, ConditionEvaluation};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_db::turso::params;

const ACTION_RUN_COLUMNS: &str = "
    id, execution_id, recipe_node_id, action_config_id, issue_id, project_id,
    status, inputs, output, error_message, started_at, completed_at, created_at,
    parent_job_id, uri_segment
";

pub(crate) fn action_run_from_row(row: &cairn_db::turso::Row) -> DbResult<ActionRun> {
    let status = row
        .text(6)?
        .parse::<ActionRunStatus>()
        .map_err(DbError::Row)?;

    Ok(ActionRun {
        id: row.text(0)?,
        execution_id: row.text(1)?,
        recipe_node_id: row.text(2)?,
        action_config_id: row.text(3)?,
        issue_id: row.opt_text(4)?,
        project_id: row.text(5)?,
        status,
        inputs: row.opt_text(7)?,
        output: row.opt_text(8)?,
        error_message: row.opt_text(9)?,
        started_at: row.opt_i64(10)?,
        completed_at: row.opt_i64(11)?,
        created_at: row.i64(12)?,
        parent_job_id: row.opt_text(13)?,
        uri_segment: row.opt_text(14)?,
    })
}

fn condition_evaluation_from_row(row: &cairn_db::turso::Row) -> DbResult<ConditionEvaluation> {
    Ok(ConditionEvaluation {
        id: row.text(0)?,
        execution_id: row.text(1)?,
        recipe_node_id: row.text(2)?,
        result_port: row.text(3)?,
        raw_result: row.opt_text(4)?,
        error_message: row.opt_text(5)?,
        evaluated_at: row.i64(6)?,
    })
}

pub async fn list_action_runs_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> Result<Vec<ActionRun>, String> {
    let issue_id = issue_id.to_string();
    db.query_all(
        format!(
            "SELECT {ACTION_RUN_COLUMNS}
             FROM action_runs
             WHERE issue_id = ?1
             ORDER BY created_at ASC"
        ),
        params![issue_id.as_str()],
        action_run_from_row,
    )
    .await
    .map_err(|error| format!("Failed to load action runs: {error}"))
}

pub async fn list_action_runs_for_execution(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Vec<ActionRun>, String> {
    let execution_id = execution_id.to_string();
    db.query_all(
        format!(
            "SELECT {ACTION_RUN_COLUMNS}
             FROM action_runs
             WHERE execution_id = ?1
             ORDER BY created_at ASC"
        ),
        params![execution_id.as_str()],
        action_run_from_row,
    )
    .await
    .map_err(|error| format!("Failed to load action runs: {error}"))
}

pub async fn get_action_run_by_id(db: &LocalDb, action_run_id: &str) -> Result<ActionRun, String> {
    let action_run_id = action_run_id.to_string();
    db.query_opt(
        format!(
            "SELECT {ACTION_RUN_COLUMNS}
             FROM action_runs
             WHERE id = ?1"
        ),
        params![action_run_id.as_str()],
        action_run_from_row,
    )
    .await
    .and_then(|run| {
        run.ok_or_else(|| DbError::Row(format!("Action run not found: {action_run_id}")))
    })
    .map_err(|error| error.to_string())
}

pub async fn list_condition_evaluations_for_issue(
    db: &LocalDb,
    issue_id: &str,
) -> Result<Vec<ConditionEvaluation>, String> {
    let issue_id = issue_id.to_string();
    db.query_all(
        "SELECT ce.id, ce.execution_id, ce.recipe_node_id, ce.result_port,
                ce.raw_result, ce.error_message, ce.evaluated_at
         FROM condition_evaluations ce
         INNER JOIN executions e ON e.id = ce.execution_id
         WHERE e.issue_id = ?1
         ORDER BY ce.evaluated_at ASC",
        params![issue_id.as_str()],
        condition_evaluation_from_row,
    )
    .await
    .map_err(|error| format!("Failed to load condition evaluations: {error}"))
}
