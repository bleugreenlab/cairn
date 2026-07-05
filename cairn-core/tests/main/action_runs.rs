use crate::common;

use cairn_core::action_runs::queries;
use cairn_core::internal::storage::LocalDb;
use cairn_core::models::ActionRunStatus;
use cairn_db::turso::params;

async fn insert_issue(db: &LocalDb, project_id: &str, issue_id: &str, number: i64) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'Issue', 'active', 1, 1)",
        params![issue_id.as_str(), project_id.as_str(), number],
    )
    .await
    .unwrap();
}

async fn insert_execution(db: &LocalDb, id: &str, project_id: &str, issue_id: Option<&str>) {
    let id = id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.map(str::to_string);
    db.execute(
        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
         VALUES (?1, 'recipe-1', ?2, ?3, 'running', 1, 1)",
        params![id.as_str(), issue_id.as_deref(), project_id.as_str()],
    )
    .await
    .unwrap();
}

async fn insert_action_run(
    db: &LocalDb,
    id: &str,
    execution_id: &str,
    project_id: &str,
    issue_id: Option<&str>,
    status: &str,
    created_at: i64,
) {
    let id = id.to_string();
    let execution_id = execution_id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.map(str::to_string);
    let status = status.to_string();
    db.write(|conn| {
        let id = id.clone();
        let execution_id = execution_id.clone();
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let status = status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO action_runs(
                    id, execution_id, recipe_node_id, action_config_id, issue_id, project_id,
                    status, inputs, output, error_message, started_at, completed_at, created_at,
                    parent_job_id
                 )
                 VALUES (?1, ?2, 'node-1', 'action-1', ?3, ?4, ?5, '{\"x\":1}', '{\"ok\":true}',
                         NULL, 10, 20, ?6, NULL)",
                params![
                    id.as_str(),
                    execution_id.as_str(),
                    issue_id.as_deref(),
                    project_id.as_str(),
                    status.as_str(),
                    created_at
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_condition_evaluation(
    db: &LocalDb,
    id: &str,
    execution_id: &str,
    result_port: &str,
    evaluated_at: i64,
) {
    let id = id.to_string();
    let execution_id = execution_id.to_string();
    let result_port = result_port.to_string();
    db.write(|conn| {
        let id = id.clone();
        let execution_id = execution_id.clone();
        let result_port = result_port.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO condition_evaluations(
                    id, execution_id, recipe_node_id, result_port, raw_result, error_message, evaluated_at
                 )
                 VALUES (?1, ?2, 'condition-1', ?3, '{\"ok\":true}', NULL, ?4)",
                params![
                    id.as_str(),
                    execution_id.as_str(),
                    result_port.as_str(),
                    evaluated_at
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn action_runs_list_by_issue_and_execution_in_created_order() {
    let (_temp, db) = common::migrated_db().await;
    let project_id = common::create_project(&db, "AR").await;
    insert_issue(&db, &project_id, "issue-1", 1).await;
    insert_issue(&db, &project_id, "issue-2", 2).await;
    insert_execution(&db, "exec-1", &project_id, Some("issue-1")).await;
    insert_execution(&db, "exec-2", &project_id, Some("issue-2")).await;
    insert_action_run(
        &db,
        "run-2",
        "exec-1",
        &project_id,
        Some("issue-1"),
        "complete",
        20,
    )
    .await;
    insert_action_run(
        &db,
        "run-1",
        "exec-1",
        &project_id,
        Some("issue-1"),
        "running",
        10,
    )
    .await;
    insert_action_run(
        &db,
        "run-3",
        "exec-2",
        &project_id,
        Some("issue-2"),
        "failed",
        30,
    )
    .await;

    let issue_runs = queries::list_action_runs_for_issue(&db, "issue-1")
        .await
        .unwrap();
    assert_eq!(
        issue_runs
            .iter()
            .map(|run| run.id.as_str())
            .collect::<Vec<_>>(),
        vec!["run-1", "run-2"]
    );
    assert_eq!(issue_runs[0].status, ActionRunStatus::Running);
    assert_eq!(issue_runs[1].status, ActionRunStatus::Complete);

    let execution_runs = queries::list_action_runs_for_execution(&db, "exec-1")
        .await
        .unwrap();
    assert_eq!(execution_runs.len(), 2);
    assert!(execution_runs
        .iter()
        .all(|run| run.execution_id == "exec-1"));
}

#[tokio::test]
async fn get_action_run_maps_optional_fields_and_reports_missing_id() {
    let (_temp, db) = common::migrated_db().await;
    let project_id = common::create_project(&db, "ARG").await;
    insert_issue(&db, &project_id, "issue-1", 1).await;
    insert_execution(&db, "exec-1", &project_id, Some("issue-1")).await;
    insert_action_run(
        &db,
        "run-1",
        "exec-1",
        &project_id,
        Some("issue-1"),
        "complete",
        10,
    )
    .await;

    let run = queries::get_action_run_by_id(&db, "run-1").await.unwrap();
    assert_eq!(run.id, "run-1");
    assert_eq!(run.inputs.as_deref(), Some("{\"x\":1}"));
    assert_eq!(run.output.as_deref(), Some("{\"ok\":true}"));
    assert_eq!(run.started_at, Some(10));
    assert_eq!(run.completed_at, Some(20));

    let missing = queries::get_action_run_by_id(&db, "missing")
        .await
        .unwrap_err();
    assert!(missing.contains("Action run not found"));
}

#[tokio::test]
async fn condition_evaluations_list_by_issue_execution_join() {
    let (_temp, db) = common::migrated_db().await;
    let project_id = common::create_project(&db, "CE").await;
    insert_issue(&db, &project_id, "issue-1", 1).await;
    insert_issue(&db, &project_id, "issue-2", 2).await;
    insert_execution(&db, "exec-1", &project_id, Some("issue-1")).await;
    insert_execution(&db, "exec-2", &project_id, Some("issue-2")).await;
    insert_condition_evaluation(&db, "eval-2", "exec-1", "false", 20).await;
    insert_condition_evaluation(&db, "eval-1", "exec-1", "true", 10).await;
    insert_condition_evaluation(&db, "eval-3", "exec-2", "true", 30).await;

    let evaluations = queries::list_condition_evaluations_for_issue(&db, "issue-1")
        .await
        .unwrap();
    assert_eq!(
        evaluations
            .iter()
            .map(|evaluation| evaluation.id.as_str())
            .collect::<Vec<_>>(),
        vec!["eval-1", "eval-2"]
    );
    assert_eq!(evaluations[0].result_port, "true");
}
