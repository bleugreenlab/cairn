//! Integration tests for checkpoint outcome effects.

mod common;

use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::effects::checkpoint::approve_job_pure;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use tempfile::{tempdir, TempDir};
use turso::params;

struct TestContext {
    orch: Orchestrator,
    db: Arc<LocalDb>,
    _db_temp: TempDir,
    _temp: TempDir,
}

async fn test_context() -> TestContext {
    let temp = tempdir().unwrap();
    let (db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();

    TestContext {
        orch,
        db,
        _db_temp: db_temp,
        _temp: temp,
    }
}

async fn insert_job(db: &LocalDb, id: &str, status: &str) {
    let project_id = common::create_project(db, &format!("CP{}", &id[id.len() - 1..])).await;
    let now = chrono::Utc::now().timestamp();
    let id = id.to_string();
    let status = status.to_string();

    db.write(|conn| {
        let id = id.clone();
        let status = status.clone();
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO jobs (
                    id, project_id, status, created_at, updated_at, started_at, completed_at,
                    node_name
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'Checkpoint')
                ",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    status.as_str(),
                    now,
                    now,
                    Some(now),
                    if status == "complete" {
                        Some(now)
                    } else {
                        None
                    },
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn job_status(db: &LocalDb, id: &str) -> String {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM jobs WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| cairn_core::internal::storage::DbError::Row("missing job".into()))?;
            row.text(0)
        })
    })
    .await
    .unwrap()
}

// Approve records the resolution fact (artifact.confirmed) and recomputes the
// job's derived status. Follow-on effects are emitted inline rather than
// returned, so these assert on the derived status. (Lifecycle/wake emission on
// the execution path is covered by the cascade tests in transitions.rs.)

#[tokio::test]
async fn approve_job_derives_complete() {
    let ctx = test_context().await;
    insert_job(&ctx.db, "job-1", "blocked").await;

    let effects = approve_job_pure(&ctx.orch, "job-1").unwrap();

    assert_eq!(job_status(&ctx.db, "job-1").await, "complete");
    assert!(effects.is_empty());
}

#[tokio::test]
async fn continue_sessionless_checkpoint_errors_and_stays_blocked() {
    // A command checkpoint job is blockable but has no agent session. Continue
    // must reject cleanly without flipping it to a sessionless Running — it
    // stays Blocked, resolvable by Confirm.
    let ctx = test_context().await;
    insert_job(&ctx.db, "job-4", "blocked").await;

    let result = cairn_core::internal::execution::jobs::continue_job_impl(
        &ctx.orch,
        "job-4",
        Some("please revise"),
        None,
        None,
    );

    assert!(result.is_err(), "continuing a sessionless job should error");
    assert_eq!(job_status(&ctx.db, "job-4").await, "blocked");
}

#[tokio::test]
async fn approve_already_complete_stays_complete() {
    let ctx = test_context().await;
    insert_job(&ctx.db, "job-3", "complete").await;

    let effects = approve_job_pure(&ctx.orch, "job-3").unwrap();

    assert_eq!(job_status(&ctx.db, "job-3").await, "complete");
    assert!(effects.is_empty());
}
