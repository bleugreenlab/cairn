//! Tests for artifact seen_at tracking.

mod common;

use common::{create_test_project, test_conn};
use diesel::prelude::*;

use diesel::sql_types::{Integer, Text};

/// Helper: insert a minimal job so the artifact FK can reference it.
fn create_test_job(conn: &mut SqliteConnection, job_id: &str, project_id: &str) {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::sql_query(
        "INSERT INTO jobs (id, project_id, status, created_at, updated_at)
         VALUES (?, ?, 'completed', ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(job_id)
    .bind::<diesel::sql_types::Text, _>(project_id)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .expect("insert job");
}

/// Helper: insert an artifact for a given job.
/// Temporarily disables FK checks because the migration-created artifacts table
/// has a stale self-referential FK `REFERENCES artifacts_new(id)` from a
/// rename-and-recreate migration. With FK enforcement ON, SQLite validates
/// FK table existence at INSERT time even for NULL FK values.
fn insert_artifact(conn: &mut SqliteConnection, id: &str, job_id: &str) {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::sql_query("PRAGMA foreign_keys = OFF")
        .execute(conn)
        .unwrap();
    diesel::sql_query(
        "INSERT INTO artifacts (id, job_id, artifact_type, schema_version, data, version, created_at, updated_at)
         VALUES (?, ?, 'plan', 1, '{}', 1, ?, ?)",
    )
    .bind::<Text, _>(id)
    .bind::<Text, _>(job_id)
    .bind::<Integer, _>(now)
    .bind::<Integer, _>(now)
    .execute(conn)
    .expect("insert artifact");
    diesel::sql_query("PRAGMA foreign_keys = ON")
        .execute(conn)
        .unwrap();
}

#[test]
fn new_artifact_has_null_seen_at() {
    let mut conn = test_conn();
    let project_id = create_test_project(&mut conn, "Test", "TST");
    create_test_job(&mut conn, "job-1", &project_id);
    insert_artifact(&mut conn, "art-1", "job-1");

    let artifact = cairn_core::artifacts::queries::get(&mut conn, "art-1").unwrap();
    assert!(
        artifact.seen_at.is_none(),
        "new artifact should have seen_at = NULL"
    );
}

#[test]
fn mark_seen_sets_timestamp() {
    let mut conn = test_conn();
    let project_id = create_test_project(&mut conn, "Test", "TST");
    create_test_job(&mut conn, "job-1", &project_id);
    insert_artifact(&mut conn, "art-1", "job-1");

    // Precondition: seen_at is NULL
    let before = cairn_core::artifacts::queries::get(&mut conn, "art-1").unwrap();
    assert!(before.seen_at.is_none());

    // Act
    cairn_core::artifacts::queries::mark_seen(&mut conn, "art-1").unwrap();

    // Assert: seen_at is now set to a reasonable timestamp
    let after = cairn_core::artifacts::queries::get(&mut conn, "art-1").unwrap();
    assert!(
        after.seen_at.is_some(),
        "seen_at should be set after mark_seen"
    );
    let ts = after.seen_at.unwrap();
    let now = chrono::Utc::now().timestamp();
    assert!(
        (now - ts).abs() < 10,
        "seen_at timestamp should be close to current time, got delta: {}",
        now - ts
    );
}

#[test]
fn mark_seen_is_idempotent() {
    let mut conn = test_conn();
    let project_id = create_test_project(&mut conn, "Test", "TST");
    create_test_job(&mut conn, "job-1", &project_id);
    insert_artifact(&mut conn, "art-1", "job-1");

    cairn_core::artifacts::queries::mark_seen(&mut conn, "art-1").unwrap();
    let first = cairn_core::artifacts::queries::get(&mut conn, "art-1").unwrap();

    // Calling again should succeed without error
    cairn_core::artifacts::queries::mark_seen(&mut conn, "art-1").unwrap();
    let second = cairn_core::artifacts::queries::get(&mut conn, "art-1").unwrap();

    assert!(first.seen_at.is_some());
    assert!(second.seen_at.is_some());
}

#[test]
fn mark_seen_nonexistent_artifact_succeeds_silently() {
    let mut conn = test_conn();
    // Diesel update on non-matching row returns Ok (0 rows affected), not an error
    let result = cairn_core::artifacts::queries::mark_seen(&mut conn, "does-not-exist");
    assert!(
        result.is_ok(),
        "mark_seen on missing artifact should not error"
    );
}

#[test]
fn get_latest_returns_seen_at() {
    let mut conn = test_conn();
    let project_id = create_test_project(&mut conn, "Test", "TST");
    create_test_job(&mut conn, "job-1", &project_id);
    insert_artifact(&mut conn, "art-1", "job-1");

    // Before marking
    let before = cairn_core::artifacts::queries::get_latest(&mut conn, "job-1")
        .unwrap()
        .unwrap();
    assert!(before.seen_at.is_none());

    // After marking
    cairn_core::artifacts::queries::mark_seen(&mut conn, "art-1").unwrap();
    let after = cairn_core::artifacts::queries::get_latest(&mut conn, "job-1")
        .unwrap()
        .unwrap();
    assert!(after.seen_at.is_some());
}
