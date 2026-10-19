//! Database-backed coverage for the crashed-resume fallback: [`handle_session_crash`]
//! decides and claims, and [`run_digest_reseed_fallback`] — the body the
//! scheduling thread runs — rebuilds the session from its digest. The user is
//! told why at the rebuild itself, as the `digest_reseed` boundary's
//! `session_unresolvable` reason, so that is where the notice is asserted.

use super::{handle_session_crash, run_digest_reseed_fallback};
use crate::db::DbState;
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use std::sync::Arc;

async fn test_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    LocalDb::open_migrated_for_test(temp.keep().join("reseed-fallback.db"))
        .await
        .unwrap()
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

/// Seed the shape `finalize_run` observes at a crashed resume: a job on an open
/// session with a settled transcript to rebuild from, and a crashed run carrying
/// that session, its start mode, and whatever exit reason the backend recorded.
async fn seed_crashed_run(
    db: &LocalDb,
    run_id: &str,
    session_id: &str,
    start_mode: &str,
    exit_reason: Option<&str>,
) {
    let exit_reason = exit_reason.map_or("NULL".to_string(), |reason| format!("'{reason}'"));
    db.execute_script(&format!(
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-reseed','W',1,1);
         INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-reseed','w-reseed','Project','prj','/tmp/prj',1,1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('i-reseed','p-reseed',3104,'Reseed','active',1,1);
         INSERT INTO jobs (id, issue_id, project_id, status, node_name, uri_segment,
                           current_session_id, model, created_at, updated_at)
           VALUES ('job-reseed','i-reseed','p-reseed','running','builder','builder',
                   '{session_id}','opus',1,1);
         INSERT INTO sessions (id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
           VALUES ('{session_id}','job-reseed','claude','native-{session_id}','open',1,1,1);
         INSERT INTO runs (id, job_id, session_id, status, started_at, start_mode, exit_reason,
                           created_at, updated_at)
           VALUES ('{run_id}','job-reseed','{session_id}','crashed',2,'{start_mode}',{exit_reason},1,3);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                            created_at, ended_at, updated_at)
           VALUES ('turn-{run_id}','{session_id}','{run_id}','job-reseed',1,'interrupted','initial',1,3,3);
         INSERT INTO events (id, run_id, session_id, turn_id, sequence, timestamp, event_type,
                             data, parent_tool_use_id, created_at)
           VALUES ('e-{run_id}-1','{run_id}','{session_id}','turn-{run_id}',1,2,'user',
                   '{{\"content\":\"start the work\"}}',NULL,2),
                  ('e-{run_id}-2','{run_id}','{session_id}','turn-{run_id}',2,3,'assistant',
                   '{{\"content\":\"working on it\"}}',NULL,3);"
    ))
    .await
    .unwrap();
}

/// Every session boundary on this job that tells the user its conversation was
/// rebuilt because the native session could not be resumed.
async fn reseed_notices(orch: &Orchestrator) -> Vec<serde_json::Value> {
    orch.db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT e.data FROM events e JOIN runs r ON r.id = e.run_id
                         WHERE r.job_id = 'job-reseed'
                           AND e.event_type = 'system:session_boundary'
                         ORDER BY e.created_at, e.sequence",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    use crate::storage::RowExt;
                    let data: serde_json::Value = serde_json::from_str(&row.text(0)?).unwrap();
                    if data["raw"]["reason"] == "session_unresolvable" {
                        found.push(data["raw"].clone());
                    }
                }
                Ok(found)
            })
        })
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    let plan = handle_session_crash(&orch, "run-unresolvable")
        .expect("a crashed unresolvable resume must plan a digest reseed");
    assert_eq!(plan.job_id, "job-reseed");
    assert_eq!(plan.session_id, "sess-unresolvable");

    run_digest_reseed_fallback(&orch, &plan);

    let notices = reseed_notices(&orch).await;
    assert_eq!(
        notices.len(),
        1,
        "the user must see why a digest-seeded turn is about to appear"
    );
    assert_eq!(notices[0]["kind"], "digest_reseed");
    assert!(
        notices[0]["seedBytes"].as_u64().unwrap_or(0) > 0,
        "the rebuilt session is seeded from the job's own transcript"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    let plan = handle_session_crash(&orch, "run-dupe").expect("the first finalize plans");
    run_digest_reseed_fallback(&orch, &plan);
    assert!(
        handle_session_crash(&orch, "run-dupe").is_none(),
        "the per-session claim must hold across a duplicate finalize"
    );
    assert_eq!(
        reseed_notices(&orch).await.len(),
        1,
        "a declined fallback must not add a second notice"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ordinary_crashed_resume_is_only_logged() {
    let db = test_db().await;
    seed_crashed_run(&db, "run-ordinary", "sess-ordinary", "resume", None).await;
    let orch = test_orchestrator(db);

    assert!(
        handle_session_crash(&orch, "run-ordinary").is_none(),
        "a resume that crashed for some other reason has no automatic recovery"
    );
    assert!(reseed_notices(&orch).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        handle_session_crash(&orch, "run-fresh").is_none(),
        "a fresh run passes no --resume flag, so it can never be the one to recover"
    );
    assert!(reseed_notices(&orch).await.is_empty());
}
