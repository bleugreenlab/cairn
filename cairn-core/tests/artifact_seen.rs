//! Tests for artifact query behavior.

mod common;

use cairn_core::artifacts::queries;
use cairn_core::internal::storage::{LocalDb, RowExt};
use turso::params;

async fn insert_artifact(db: &LocalDb, id: &str, job_id: &str, version: i64) {
    let id = id.to_string();
    let job_id = job_id.to_string();
    let data = format!(r#"{{"title":"artifact {version}"}}"#);
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let job_id = job_id.clone();
        let data = data.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO artifacts (
                    id, job_id, artifact_type, schema_version, data, version, created_at, updated_at
                )
                VALUES (?1, ?2, 'plan', 1, ?3, ?4, ?5, ?6)
                ",
                params![
                    id.as_str(),
                    job_id.as_str(),
                    data.as_str(),
                    version,
                    now,
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn job_project_id(db: &LocalDb, job_id: &str) -> String {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT project_id FROM jobs WHERE id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("job row");
            row.text(0)
        })
    })
    .await
    .unwrap()
}

async fn create_issue_for_job(db: &LocalDb, issue_id: &str, job_id: &str) {
    let issue_id = issue_id.to_string();
    let job_id = job_id.to_string();
    let project_id = job_project_id(db, &job_id).await;
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let issue_id = issue_id.clone();
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO issues (
                    id, project_id, number, title, status, created_at, updated_at
                )
                VALUES (?1, ?2, 1, 'Artifact issue', 'backlog', ?3, ?4)
                ",
                params![issue_id.as_str(), project_id.as_str(), now, now],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET issue_id = ?1 WHERE id = ?2",
                params![issue_id.as_str(), job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn artifact_fixture() -> (tempfile::TempDir, LocalDb, String) {
    let (temp, db) = common::migrated_db().await;
    let job_id = common::create_job(&db, None, None).await;
    insert_artifact(&db, "art-1", &job_id, 1).await;
    (temp, db, job_id)
}

#[tokio::test]
async fn new_artifact_has_null_seen_at() {
    let (_temp, db, _job_id) = artifact_fixture().await;

    let artifact = queries::get(&db, "art-1").await.unwrap();
    assert!(artifact.seen_at.is_none());
}

#[tokio::test]
async fn mark_seen_sets_timestamp() {
    let (_temp, db, _job_id) = artifact_fixture().await;

    let before = queries::get(&db, "art-1").await.unwrap();
    assert!(before.seen_at.is_none());

    queries::mark_seen(&db, "art-1").await.unwrap();

    let after = queries::get(&db, "art-1").await.unwrap();
    let ts = after.seen_at.expect("seen_at should be set");
    let now = chrono::Utc::now().timestamp();
    assert!((now - ts).abs() < 10);
}

#[tokio::test]
async fn mark_seen_at_sets_exact_timestamp() {
    let (_temp, db, _job_id) = artifact_fixture().await;

    queries::mark_seen_at(&db, "art-1", 12345).await.unwrap();

    let artifact = queries::get(&db, "art-1").await.unwrap();
    assert_eq!(artifact.seen_at, Some(12345));
}

#[tokio::test]
async fn mark_seen_is_idempotent() {
    let (_temp, db, _job_id) = artifact_fixture().await;

    queries::mark_seen_at(&db, "art-1", 10).await.unwrap();
    queries::mark_seen_at(&db, "art-1", 11).await.unwrap();

    let artifact = queries::get(&db, "art-1").await.unwrap();
    assert_eq!(artifact.seen_at, Some(11));
}

#[tokio::test]
async fn mark_seen_nonexistent_artifact_succeeds_silently() {
    let (_temp, db) = common::migrated_db().await;
    queries::mark_seen(&db, "does-not-exist").await.unwrap();
}

#[tokio::test]
async fn get_latest_returns_highest_version_with_seen_at() {
    let (_temp, db, job_id) = artifact_fixture().await;
    insert_artifact(&db, "art-2", &job_id, 2).await;
    queries::mark_seen_at(&db, "art-2", 12345).await.unwrap();

    let latest = queries::get_latest(&db, &job_id).await.unwrap().unwrap();
    assert_eq!(latest.id, "art-2");
    assert_eq!(latest.version, 2);
    assert_eq!(latest.seen_at, Some(12345));
}

#[tokio::test]
async fn list_orders_versions_newest_first() {
    let (_temp, db, job_id) = artifact_fixture().await;
    insert_artifact(&db, "art-3", &job_id, 3).await;
    insert_artifact(&db, "art-2", &job_id, 2).await;

    let artifacts = queries::list(&db, &job_id).await.unwrap();
    let ids: Vec<_> = artifacts
        .iter()
        .map(|artifact| artifact.id.as_str())
        .collect();
    assert_eq!(ids, vec!["art-3", "art-2", "art-1"]);
}

#[tokio::test]
async fn list_for_issue_returns_latest_artifact_per_issue_job() {
    let (_temp, db, job_id) = artifact_fixture().await;
    create_issue_for_job(&db, "issue-1", &job_id).await;
    insert_artifact(&db, "art-2", &job_id, 2).await;

    let unrelated_job_id = common::create_job(&db, None, None).await;
    insert_artifact(&db, "other-art", &unrelated_job_id, 9).await;

    let artifacts = queries::list_for_issue(&db, "issue-1").await.unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].id, "art-2");
}
