//! Test utilities for cairn-core tests.
//!
//! Provides helpers for creating in-memory databases and mock data.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::MigrationHarness;

use crate::db::MIGRATIONS;
use crate::diesel_models::{NewIssue, NewJob, NewProject};
use crate::schema::{issues, jobs, projects};

/// Create an in-memory SQLite database with migrations applied.
pub fn test_diesel_conn() -> SqliteConnection {
    let mut conn =
        SqliteConnection::establish(":memory:").expect("Failed to open in-memory database");
    diesel::sql_query("PRAGMA foreign_keys = ON")
        .execute(&mut conn)
        .expect("Failed to enable foreign keys");
    conn.run_pending_migrations(MIGRATIONS)
        .expect("Failed to run Diesel migrations");
    conn
}

/// Create a test project. Returns the project ID.
pub fn create_test_project(conn: &mut SqliteConnection, name: &str, key: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let new_project = NewProject {
        id: &id,
        workspace_id: "default",
        name,
        key,
        repo_path: "/tmp/test-repo",
        context: Some(""),
        docs_enabled: Some(1),
        default_branch: Some("main"),
        next_issue_number: Some(1),
        created_at: now,
        updated_at: now,
        remote_url: None,
        server_id: None,
    };

    diesel::insert_into(projects::table)
        .values(&new_project)
        .execute(conn)
        .expect("Failed to create test project");

    id
}

/// Create a test issue. Returns the issue ID.
pub fn create_test_issue(conn: &mut SqliteConnection, project_id: &str, title: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let number: i32 = projects::table
        .find(project_id)
        .select(projects::next_issue_number)
        .first::<Option<i32>>(conn)
        .expect("Failed to get next issue number")
        .unwrap_or(1);

    let new_issue = NewIssue {
        id: &id,
        project_id,
        number,
        title,
        description: Some(""),
        status: "backlog",
        progress: "backlog",
        attention: "none",
        priority: Some(0),
        created_at: now,
        updated_at: now,
        model: None,
        manager_id: None,
    };

    diesel::insert_into(issues::table)
        .values(&new_issue)
        .execute(conn)
        .expect("Failed to create test issue");

    diesel::update(projects::table.find(project_id))
        .set(projects::next_issue_number.eq(Some(number + 1)))
        .execute(conn)
        .expect("Failed to increment issue number");

    id
}

/// Create a test job. Returns the job ID.
pub fn create_test_job(
    conn: &mut SqliteConnection,
    issue_id: &str,
    project_id: &str,
    _node_type: &str,
    status: &str,
    parent_job_id: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let new_job = NewJob {
        id: &id,
        execution_id: None,
        manager_id: None,
        recipe_node_id: None,
        parent_job_id,
        worktree_path: None,
        branch: None,
        base_commit: None,
        current_session_id: None,
        resume_session_id: None,
        status,
        agent_config_id: None,
        issue_id: Some(issue_id),
        project_id,
        task_description: None,
        created_at: now,
        updated_at: now,
        completed_at: None,
        parent_tool_use_id: None,
        task_index: None,
        started_at: if status == "running" { Some(now) } else { None },
        model: None,
        node_name: None,
        base_branch: None,
        current_turn_id: None,
    };

    diesel::insert_into(jobs::table)
        .values(&new_job)
        .execute(conn)
        .expect("Failed to create test job");

    id
}
