//! Shared test helpers for cairn-core integration tests.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::MigrationHarness;

use cairn_core::db::MIGRATIONS;

/// Create an in-memory SQLite database with all migrations applied.
pub fn test_conn() -> SqliteConnection {
    let mut conn =
        SqliteConnection::establish(":memory:").expect("Failed to open in-memory database");
    conn.run_pending_migrations(MIGRATIONS)
        .expect("Failed to run migrations");
    diesel::sql_query("PRAGMA foreign_keys = ON")
        .execute(&mut conn)
        .expect("Failed to enable FK constraints");
    conn
}

/// Create a test project. Returns the project ID.
#[allow(dead_code)]
pub fn create_test_project(conn: &mut SqliteConnection, name: &str, key: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    diesel::sql_query(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, docs_enabled, default_branch, next_issue_number, created_at, updated_at)
         VALUES (?, 'default', ?, ?, '/tmp/test-repo', 1, 'main', 1, ?, ?)"
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::Text, _>(name)
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .expect("Failed to create test project");

    id
}
