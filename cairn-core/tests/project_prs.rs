//! Integration tests for cairn_core::merge_requests::queries.

mod common;

use cairn_core::internal::storage::{DbResult, LocalDb};
use cairn_core::merge_requests::queries;
use turso::params;

async fn create_execution(db: &LocalDb, project_id: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO executions (
            id, recipe_id, project_id, status, started_at
        )
        VALUES (?1, 'recipe-1', ?2, 'running', ?3)
        ",
        params![id.as_str(), project_id.as_str(), now],
    )
    .await
    .unwrap();

    id
}

async fn create_job(
    db: &LocalDb,
    project_id: &str,
    issue_id: Option<&str>,
    execution_id: Option<&str>,
    branch: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.map(str::to_string);
    let execution_id = execution_id.map(str::to_string);
    let branch = branch.map(str::to_string);
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO jobs (
            id, project_id, issue_id, execution_id, status,
            branch, created_at, updated_at
        )
        VALUES (?1, ?2, ?3, ?4, 'complete', ?5, ?6, ?7)
        ",
        params![
            id.as_str(),
            project_id.as_str(),
            issue_id.as_deref(),
            execution_id.as_deref(),
            branch.as_deref(),
            now,
            now
        ],
    )
    .await
    .unwrap();

    id
}

#[allow(clippy::too_many_arguments)]
async fn create_merge_request(
    db: &LocalDb,
    job_id: &str,
    project_id: &str,
    issue_id: Option<&str>,
    title: &str,
    status: &str,
    github_pr_number: Option<i64>,
    github_pr_url: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let job_id = job_id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.map(str::to_string);
    let title = title.to_string();
    let status = status.to_string();
    let github_pr_url = github_pr_url.map(str::to_string);
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO merge_requests (
            id, job_id, project_id, issue_id, title,
            source_branch, target_branch, status, merge_method,
            opened_at, updated_at, github_pr_number, github_pr_url
        )
        VALUES (
            ?1, ?2, ?3, ?4, ?5,
            'feature', 'main', ?6, 'squash',
            ?7, ?8, ?9, ?10
        )
        ",
        params![
            id.as_str(),
            job_id.as_str(),
            project_id.as_str(),
            issue_id.as_deref(),
            title.as_str(),
            status.as_str(),
            now,
            now,
            github_pr_number,
            github_pr_url.as_deref()
        ],
    )
    .await
    .unwrap();

    id
}

async fn create_action_run(
    db: &LocalDb,
    project_id: &str,
    execution_id: &str,
    job_id: &str,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let execution_id = execution_id.to_string();
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO action_runs (
            id, execution_id, recipe_node_id, action_config_id, project_id,
            status, created_at, parent_job_id
        )
        VALUES (?1, ?2, 'node-create-pr', 'builtin:create_pr', ?3, 'complete', ?4, ?5)
        ",
        params![
            id.as_str(),
            execution_id.as_str(),
            project_id.as_str(),
            now,
            job_id.as_str()
        ],
    )
    .await
    .unwrap();

    id
}

async fn insert_webhook_event(db: &LocalDb, pr_number: i64, processed_at: i64) -> DbResult<String> {
    let id = uuid::Uuid::new_v4().to_string();
    db.execute(
        "
        INSERT INTO webhook_events (
            id, event_type, action, repo_full_name, pr_number,
            payload_summary, processed_at
        )
        VALUES (?1, 'pull_request', 'opened', 'owner/repo', ?2, 'opened PR', ?3)
        ",
        params![id.as_str(), pr_number, processed_at],
    )
    .await
    .map(|_| id)
}

#[tokio::test]
async fn summaries_for_action_runs_preserve_action_run_id() {
    let (_temp, db) = common::migrated_db().await;
    let project = common::create_project(&db, "PRS").await;
    let execution = create_execution(&db, &project).await;
    let job = create_job(&db, &project, None, Some(&execution), Some("feature")).await;
    let action_run = create_action_run(&db, &project, &execution, &job).await;
    let merge_request = create_merge_request(
        &db,
        &job,
        &project,
        None,
        "Action PR",
        "open",
        Some(12),
        Some("https://github.com/test/test/pull/12"),
    )
    .await;

    let summaries = queries::get_summaries_for_action_runs(&db, std::slice::from_ref(&action_run))
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, merge_request);
    assert_eq!(summaries[0].action_run_id, Some(action_run));
    assert_eq!(summaries[0].pr_number, 12);
}

#[tokio::test]
async fn get_by_action_run_id_accepts_action_run_or_job_id() {
    let (_temp, db) = common::migrated_db().await;
    let project = common::create_project(&db, "PRD").await;
    let execution = create_execution(&db, &project).await;
    let job = create_job(&db, &project, None, Some(&execution), Some("feature")).await;
    let action_run = create_action_run(&db, &project, &execution, &job).await;
    let merge_request = create_merge_request(
        &db,
        &job,
        &project,
        None,
        "Details PR",
        "open",
        Some(33),
        Some("https://github.com/test/test/pull/33"),
    )
    .await;

    let via_action = queries::get_by_action_run_id(&db, &action_run)
        .await
        .unwrap()
        .unwrap();
    let via_job = queries::get_by_action_run_id(&db, &job)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(via_action.id, merge_request);
    assert_eq!(via_job.id, via_action.id);
    assert_eq!(via_action.pr_number, 33);
}

#[tokio::test]
async fn webhook_events_can_load_by_number_or_job() {
    let (_temp, db) = common::migrated_db().await;
    let project = common::create_project(&db, "PRW").await;
    let execution = create_execution(&db, &project).await;
    let job = create_job(&db, &project, None, Some(&execution), Some("feature")).await;
    create_merge_request(
        &db,
        &job,
        &project,
        None,
        "Webhook PR",
        "open",
        Some(51),
        Some("https://github.com/test/test/pull/51"),
    )
    .await;
    let older = insert_webhook_event(&db, 51, 10).await.unwrap();
    let newer = insert_webhook_event(&db, 51, 20).await.unwrap();

    let by_number = queries::get_webhook_events(&db, 51).await.unwrap();
    assert_eq!(by_number.len(), 2);
    assert_eq!(by_number[0].id, newer);
    assert_eq!(by_number[1].id, older);

    let by_job = queries::get_webhook_events_for_job(&db, &job)
        .await
        .unwrap();
    assert_eq!(by_job.len(), 2);
    assert_eq!(by_job[0].processed_at, 20);
}
