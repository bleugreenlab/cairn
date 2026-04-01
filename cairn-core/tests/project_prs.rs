//! Tests for the get_project_merge_requests query.

mod common;

use diesel::prelude::*;

/// Create a test issue. Returns the issue ID.
fn create_issue(conn: &mut SqliteConnection, project_id: &str, title: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    // Use a unique number per call
    static COUNTER: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);
    let number = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    diesel::sql_query(
        "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
         VALUES (?, ?, ?, ?, 'active', 'idle', 'none', ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::Text, _>(project_id)
    .bind::<diesel::sql_types::Integer, _>(number)
    .bind::<diesel::sql_types::Text, _>(title)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .expect("Failed to create issue");

    id
}

/// Create a job for testing. Returns the job_id.
fn create_test_job(
    conn: &mut SqliteConnection,
    issue_id: Option<&str>,
    project_id: &str,
    branch: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    diesel::sql_query(
        "INSERT INTO jobs (id, project_id, issue_id, status, branch, created_at, updated_at)
         VALUES (?, ?, ?, 'complete', ?, ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::Text, _>(project_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(issue_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(branch)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .expect("Failed to create job");

    id
}

/// Insert a merge_request. Returns the merge_request ID.
fn create_merge_request(
    conn: &mut SqliteConnection,
    job_id: &str,
    project_id: &str,
    issue_id: Option<&str>,
    manager_id: Option<&str>,
    title: &str,
    status: &str,
    github_pr_number: Option<i32>,
    github_pr_url: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    diesel::sql_query(
        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, manager_id, title, source_branch, target_branch, status, merge_method, opened_at, updated_at, github_pr_number, github_pr_url)
         VALUES (?, ?, ?, ?, ?, ?, 'feature', 'main', ?, 'squash', ?, ?, ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::Text, _>(job_id)
    .bind::<diesel::sql_types::Text, _>(project_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(issue_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(manager_id)
    .bind::<diesel::sql_types::Text, _>(title)
    .bind::<diesel::sql_types::Text, _>(status)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(github_pr_number)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(github_pr_url)
    .execute(conn)
    .expect("Failed to create merge_request");

    id
}

#[test]
fn get_project_merge_requests_returns_results_for_project() {
    let mut conn = common::test_conn();
    let project_a = common::create_test_project(&mut conn, "Project A", "PA");
    let project_b = common::create_test_project(&mut conn, "Project B", "PB");

    let issue = create_issue(&mut conn, &project_a, "Test Issue");

    let job_a = create_test_job(&mut conn, Some(&issue), &project_a, Some("feature-a"));
    let _job_b = create_test_job(&mut conn, None, &project_b, Some("feature-b"));

    create_merge_request(
        &mut conn,
        &job_a,
        &project_a,
        Some(&issue),
        None,
        "Test PR",
        "open",
        Some(1),
        Some("https://github.com/test/test/pull/1"),
    );

    let result =
        cairn_core::merge_requests::queries::get_project_merge_requests(&mut conn, &project_a)
            .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].title, Some("Test PR".to_string()));
    assert_eq!(result[0].pr_number, 1);

    // Project B should have no merge requests
    let result_b =
        cairn_core::merge_requests::queries::get_project_merge_requests(&mut conn, &project_b)
            .unwrap();
    assert!(result_b.is_empty());
}

#[test]
fn get_project_merge_requests_includes_issue_info() {
    let mut conn = common::test_conn();
    let project = common::create_test_project(&mut conn, "Test Project", "TP");
    let issue = create_issue(&mut conn, &project, "My Issue");

    let job = create_test_job(&mut conn, Some(&issue), &project, Some("feature"));

    create_merge_request(
        &mut conn,
        &job,
        &project,
        Some(&issue),
        None,
        "Fix bug",
        "open",
        Some(42),
        Some("https://github.com/test/test/pull/42"),
    );

    let result =
        cairn_core::merge_requests::queries::get_project_merge_requests(&mut conn, &project)
            .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].issue_id, Some(issue));
    assert_eq!(result[0].issue_title, Some("My Issue".to_string()));
}

#[test]
fn get_project_merge_requests_without_github() {
    let mut conn = common::test_conn();
    let project = common::create_test_project(&mut conn, "Test Project", "TP");
    let issue = create_issue(&mut conn, &project, "Local Issue");

    let job = create_test_job(&mut conn, Some(&issue), &project, Some("feature"));

    // Create a merge request without GitHub info
    create_merge_request(
        &mut conn,
        &job,
        &project,
        Some(&issue),
        None,
        "Local PR",
        "open",
        None,
        None,
    );

    let result =
        cairn_core::merge_requests::queries::get_project_merge_requests(&mut conn, &project)
            .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].title, Some("Local PR".to_string()));
    assert_eq!(result[0].pr_number, 0);
    assert!(result[0].pr_url.is_empty());
}
