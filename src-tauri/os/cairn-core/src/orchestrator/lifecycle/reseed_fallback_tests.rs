//! Database-backed coverage for [`handle_session_crash`] — the half of the
//! crashed-resume fallback that decides and records. The scheduling half
//! ([`spawn_digest_reseed_fallback`]) is deliberately not called here: this seam
//! is where the decision is made, so it can be asserted without launching a
//! process.

use super::handle_session_crash;
use crate::db::DbState;
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{DbError, LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
use std::sync::Arc;

async fn test_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let db = LocalDb::open(temp.keep().join("reseed-fallback.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}

fn test_orchestrator(db: LocalDb) -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    OrchestratorBuilder::new(db_state, services, config_dir).build()
}

/// Seed the shape `finalize_run` observes at a crashed resume: a crashed run
/// carrying its session identity, start mode, and whatever exit reason the
/// backend recorded before finalizing.
async fn seed_crashed_run(
    db: &LocalDb,
    run_id: &str,
    session_id: &str,
    start_mode: &str,
    exit_reason: Option<&str>,
) {
    let run_id = run_id.to_string();
    let session_id = session_id.to_string();
    let start_mode = start_mode.to_string();
    let exit_reason = exit_reason.map(str::to_string);
    db.write(move |conn| {
        let run_id = run_id.clone();
        let session_id = session_id.clone();
        let start_mode = start_mode.clone();
        let exit_reason = exit_reason.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO workspaces (id, name, created_at, updated_at)
                 VALUES ('w-reseed','W',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('p-reseed','w-reseed','Project','prj','/tmp/prj',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT OR IGNORE INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('i-reseed','p-reseed',3104,'Reseed','active',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT OR IGNORE INTO jobs (id, issue_id, project_id, status, node_name, created_at, updated_at)
                 VALUES ('job-reseed','i-reseed','p-reseed','running','builder',1,1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, job_id, session_id, status, start_mode, exit_reason, created_at, updated_at)
                 VALUES (?1,'job-reseed',?2,'crashed',?3,?4,1,1)",
                (
                    run_id.as_str(),
                    session_id.as_str(),
                    start_mode.as_str(),
                    exit_reason.as_deref(),
                ),
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
}

/// Every `system:message` this run recorded that is the reseed notice.
async fn reseed_notices(orch: &Orchestrator, run_id: &str) -> Vec<String> {
    let run_id = run_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data FROM events
                         WHERE run_id = ?1 AND event_type = 'system:message'
                         ORDER BY sequence",
                        (run_id.as_str(),),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    use crate::storage::RowExt;
                    let data = row.text(0)?;
                    if data.contains("session_reseed_fallback") {
                        found.push(data);
                    }
                }
                Ok(found)
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn unresolvable_resume_plans_a_reseed_and_records_the_notice() {
    let db = test_db().await;
    seed_crashed_run(
        &db,
        "run-unresolvable",
        "sess-unresolvable",
        "resume",
        Some("session_unresolvable"),
    )
    .await;
    let orch = test_orchestrator(db);

    let plan = handle_session_crash(&orch, "run-unresolvable", None)
        .expect("a crashed unresolvable resume must plan a digest reseed");

    assert_eq!(plan.job_id, "job-reseed");
    assert_eq!(plan.session_id, "sess-unresolvable");
    assert_eq!(
        reseed_notices(&orch, "run-unresolvable").await.len(),
        1,
        "the user must see why a digest-seeded turn is about to appear"
    );
}

#[tokio::test]
async fn a_duplicate_finalize_does_not_plan_a_second_reseed() {
    let db = test_db().await;
    seed_crashed_run(
        &db,
        "run-dupe",
        "sess-dupe",
        "resume",
        Some("session_unresolvable"),
    )
    .await;
    let orch = test_orchestrator(db);

    assert!(handle_session_crash(&orch, "run-dupe", None).is_some());
    assert!(
        handle_session_crash(&orch, "run-dupe", None).is_none(),
        "the per-session claim must hold across a duplicate finalize"
    );
    assert_eq!(
        reseed_notices(&orch, "run-dupe").await.len(),
        1,
        "a declined fallback must not add a second notice"
    );
}

#[tokio::test]
async fn an_ordinary_crashed_resume_is_only_logged() {
    let db = test_db().await;
    seed_crashed_run(&db, "run-ordinary", "sess-ordinary", "resume", None).await;
    let orch = test_orchestrator(db);

    assert!(
        handle_session_crash(&orch, "run-ordinary", None).is_none(),
        "a resume that crashed for some other reason has no automatic recovery"
    );
    assert!(reseed_notices(&orch, "run-ordinary").await.is_empty());
}

#[tokio::test]
async fn a_fresh_run_never_plans_a_reseed() {
    let db = test_db().await;
    seed_crashed_run(
        &db,
        "run-fresh",
        "sess-fresh",
        "fresh",
        Some("session_unresolvable"),
    )
    .await;
    let orch = test_orchestrator(db);

    assert!(
        handle_session_crash(&orch, "run-fresh", None).is_none(),
        "a fresh run passes no --resume flag, so it can never be the one to recover"
    );
    assert!(reseed_notices(&orch, "run-fresh").await.is_empty());
}
