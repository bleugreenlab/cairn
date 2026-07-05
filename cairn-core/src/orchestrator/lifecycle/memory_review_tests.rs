use super::{finish_memory_review_if_due, review_artifact_ref};
use crate::agent_process::process::{wrap_plain_stdin, RunHandle};
use crate::db::DbState;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
use std::sync::{Arc, Mutex};

async fn test_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let db = LocalDb::open(root.join("memory-review.db")).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}

fn test_orchestrator(db: LocalDb) -> crate::orchestrator::Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    std::fs::write(
        config_dir.join("recipes/memory-triage.yaml"),
        include_str!("../../../../../recipes/memory-triage.yaml"),
    )
    .unwrap();
    std::fs::write(
        config_dir.join("agents/integrator.md"),
        include_str!("../../../../../agents/integrator.md"),
    )
    .unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    OrchestratorBuilder::new(db_state, services, config_dir).build()
}

async fn seed_job(db: &LocalDb, review_state: Option<&str>) {
    seed_job_row(db, review_state, false).await;
    insert_draft_memory(db, "m-review", 1).await;
}

/// Insert the project/issue/execution scaffold and the `j-review` job
/// without any draft memory. When `is_task` is set, a parent job is
/// inserted first and `j-review.parent_job_id` points at it, so the job
/// reads back as a sub-agent task.
async fn seed_job_row(db: &LocalDb, review_state: Option<&str>, is_task: bool) {
    db.write(|conn| {
            let review_state = review_state.map(str::to_string);
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-review','recipe','i-review','p-review','running',1,1)", ()).await?;
                let parent_job_id = if is_task {
                    conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-parent','e-review','coordinator','i-review','p-review','complete','coordinator','coordinator',1,1)", ()).await?;
                    Some("j-parent")
                } else {
                    None
                };
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder',?1,'i-review','p-review','complete','builder','builder',?2,1,1)",
                    (parent_job_id, review_state.as_deref()),
                ).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
}

async fn insert_draft_memory(db: &LocalDb, id: &str, node_seq: i64) {
    crate::memories::db::create_memory(
        db,
        id,
        Some(id),
        "remember durable behavior",
        Some("p-review"),
        "project",
        "p-review",
        Some("j-review"),
        Some(node_seq),
        None,
    )
    .await
    .unwrap();
}

async fn insert_artifact(db: &LocalDb) {
    db.execute(
            "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)",
            (),
        )
        .await
        .unwrap();
}

async fn insert_run_session_with_turns(db: &LocalDb, turn_count: i64) {
    db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at) VALUES ('s-review','j-review','open','backend-review',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET current_session_id = 's-review' WHERE id = 'j-review'",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id, created_at, updated_at) VALUES ('run-review','i-review','p-review','j-review','live','s-review',1,1)",
                    (),
                )
                .await?;
                for sequence in 1..=turn_count {
                    conn.execute(
                        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1,'s-review','run-review','j-review',?2,'complete','initial',?2,?2)",
                        (format!("t-work-{sequence}"), sequence),
                    )
                    .await?;
                }
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
}

/// Seed `count` recorded events on `run-review`. The no-drafts reflection
/// nudge is gated on event activity (`events > REFLECTION_ACTIVITY_THRESHOLD`),
/// not turn count, so a node job needs enough events to clear the threshold.
async fn insert_events_for_review_run(db: &LocalDb, count: i64) {
    db.write(move |conn| {
            Box::pin(async move {
                for seq in 0..count {
                    conn.execute(
                        "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
                         VALUES (?1, 'run-review', ?2, 1, 'assistant', '{}', 1)",
                        (format!("ev-{seq}"), seq),
                    )
                    .await?;
                }
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
}

/// Insert a MemoryReview turn for `j-review` in the given state. Review
/// completion now keys off this turn reaching a terminal state, so the
/// `sent` path needs one present to fire.
async fn insert_review_turn(db: &LocalDb, state: &str) {
    db.write(|conn| {
            let state = state.to_string();
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO sessions (id, job_id, status, created_at, updated_at) VALUES ('s-review','j-review','open',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('t-review','s-review','j-review',1,?1,'memory_review',2,2)",
                    (state,),
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
}

async fn memory_status(orch: &crate::orchestrator::Orchestrator) -> String {
    orch.db
        .local
        .query_one(
            "SELECT status FROM memories WHERE id = 'm-review'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
}

async fn review_state(orch: &crate::orchestrator::Orchestrator) -> Option<String> {
    orch.db
        .local
        .query_one(
            "SELECT memory_review_state FROM jobs WHERE id = 'j-review'",
            (),
            |row| row.opt_text(0),
        )
        .await
        .unwrap()
}

/// A sub-agent task's review artifact ref must nest under the parent node
/// (`.../{parent}/task/{task}/...`) so the wake's detail URI resolves; the
/// old code emitted a flat top-level node URI naming the task's own segment,
/// which does not resolve to the sub-task job (issue #143).
#[tokio::test]
async fn review_artifact_ref_nests_subtask_artifact_under_parent_task() {
    use cairn_common::uri::{parse_uri, CairnResource};

    // Top-level node job: the flat node artifact URI (unchanged behavior).
    let db = test_db().await;
    seed_job_row(&db, None, false).await;
    insert_artifact(&db).await;
    let (node_uri, _fp) = review_artifact_ref(&db, "PRJ", 2, "j-review")
        .await
        .unwrap()
        .expect("artifact ref for node job");
    assert_eq!(node_uri, "cairn://p/PRJ/2/1/builder/create-pr");
    assert!(
        matches!(
            parse_uri(&node_uri),
            Some(CairnResource::NodeArtifact { ref node_id, .. }) if node_id == "builder"
        ),
        "top-level node artifact URI should parse to NodeArtifact: {node_uri}"
    );

    // Sub-agent task job: the artifact nests under the parent node and
    // parses to a TaskArtifact that resolves through the read path.
    let task_db = test_db().await;
    seed_job_row(&task_db, None, true).await;
    insert_artifact(&task_db).await;
    let (task_uri, _fp) = review_artifact_ref(&task_db, "PRJ", 2, "j-review")
        .await
        .unwrap()
        .expect("artifact ref for task job");
    assert_eq!(
        task_uri,
        "cairn://p/PRJ/2/1/coordinator/task/builder/create-pr"
    );
    assert!(
        task_uri.contains("/task/"),
        "sub-task URI must carry /task/"
    );
    match parse_uri(&task_uri) {
        Some(CairnResource::TaskArtifact {
            node_id,
            task_name,
            name,
            ..
        }) => {
            assert_eq!(node_id, "coordinator");
            assert_eq!(task_name, "builder");
            assert_eq!(name.as_deref(), Some("create-pr"));
        }
        other => panic!("expected TaskArtifact, got {other:?}"),
    }

    // The old flat form named the task's own segment as a top-level node;
    // that URI cannot resolve to the sub-task job.
    let broken =
        cairn_common::uri::build_node_artifact_uri_named("PRJ", 2, 1, "builder", Some("create-pr"));
    assert_eq!(broken, "cairn://p/PRJ/2/1/builder/create-pr");
    assert_ne!(broken, task_uri);
}

#[tokio::test]
async fn null_with_drafts_sends_review_without_confirming() {
    let db = test_db().await;
    seed_job(&db, None).await;
    insert_artifact(&db).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
    assert_eq!(memory_status(&orch).await, "draft");
    assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 1);
}

#[tokio::test]
async fn null_with_drafts_but_no_artifact_does_not_send_review() {
    let db = test_db().await;
    seed_job(&db, None).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await, None);
    assert_eq!(memory_status(&orch).await, "draft");
    let messages = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'direct' AND recipient_run_id = 'run-review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
    assert_eq!(messages, 0);
}

#[tokio::test]
async fn sent_idle_confirms_surviving_drafts_and_spawns_triage() {
    let db = test_db().await;
    seed_job(&db, Some("sent")).await;
    for node_seq in 2..=5 {
        insert_draft_memory(&db, &format!("m-review-{node_seq}"), node_seq).await;
    }
    // The review turn has ended (Complete): completion is allowed to fire.
    insert_review_turn(&db, "complete").await;
    // The draft-confirmation eligibility gate in `confirm_draft_memories_for_job`
    // only advances a job's drafts out of `draft` once the work that produced
    // them has landed — i.e. the owning issue is merged. Mark `i-review` merged
    // so the review-completion hook actually confirms its survivors (matching the
    // production path where a review completes on an already-merged issue).
    db.execute("UPDATE issues SET merged_at = 1 WHERE id = 'i-review'", ())
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await.as_deref(), Some("done"));
    assert_eq!(
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM memories WHERE job_id = 'j-review' AND status = 'draft'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap(),
        0
    );
    assert_eq!(
            orch.db
                .local
                .query_one(
                    "SELECT COUNT(*) FROM memories WHERE job_id = 'j-review' AND status = 'claimed' AND scope = 'project' AND scope_value = 'p-review'",
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap(),
            5
        );
    assert_eq!(
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*)
                     FROM issues i
                     JOIN executions e ON e.issue_id = i.id
                     WHERE i.project_id = 'p-review'
                       AND i.title LIKE 'Memory triage: project=P%'
                       AND e.recipe_id = 'memory-triage'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap(),
        1
    );
}

async fn direct_push_count(orch: &crate::orchestrator::Orchestrator, like: &str) -> i64 {
    let like = like.to_string();
    orch.db
        .local
        .query_one(
            "SELECT COUNT(*)
                 FROM attention_pushes p
                 JOIN messages m ON p.key = 'direct:' || m.id
                 WHERE p.recipient = 'j-review'
                   AND p.delivered_event_id IS NULL
                   AND m.channel_type = 'direct'
                   AND m.recipient_run_id = 'run-review'
                   AND m.content LIKE ?1",
            (like,),
            |row| row.i64(0),
        )
        .await
        .unwrap()
}

async fn memory_review_turn_count(orch: &crate::orchestrator::Orchestrator) -> i64 {
    orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM turns WHERE job_id = 'j-review' AND start_reason = 'memory_review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
}

async fn latest_memory_review_turn_state(orch: &crate::orchestrator::Orchestrator) -> String {
    orch.db
            .local
            .query_one(
                "SELECT state FROM turns WHERE job_id = 'j-review' AND start_reason = 'memory_review' ORDER BY sequence DESC LIMIT 1",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
}

fn register_warm_process(orch: &crate::orchestrator::Orchestrator) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let child = Arc::new(Mutex::new(None));
    let stdin = Arc::new(Mutex::new(Some(wrap_plain_stdin(Box::new(
        Vec::<u8>::new(),
    )))));
    let mut handle = RunHandle::new(
        child,
        stdin,
        Some("s-review".to_string()),
        Some("j-review".to_string()),
    );
    handle.transition_to_warm();
    processes.register("run-review".to_string(), handle);
}

async fn direct_message_count(orch: &crate::orchestrator::Orchestrator, like: &str) -> i64 {
    let like = like.to_string();
    orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'direct' AND recipient_run_id = 'run-review' AND content LIKE ?1",
                (like,),
                |row| row.i64(0),
            )
            .await
            .unwrap()
}

#[tokio::test]
async fn null_no_drafts_node_job_short_session_does_not_send_reflection() {
    let db = test_db().await;
    seed_job_row(&db, None, false).await;
    insert_artifact(&db).await;
    insert_run_session_with_turns(&db, 10).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await, None);
    assert_eq!(direct_message_count(&orch, "%").await, 0);
}

#[tokio::test]
async fn null_no_drafts_node_job_substantial_activity_sends_reflection() {
    let db = test_db().await;
    seed_job_row(&db, None, false).await;
    insert_artifact(&db).await;
    insert_run_session_with_turns(&db, 11).await;
    // The no-drafts reflection nudge is gated on event activity (events > 50),
    // not turn count, so seed enough events to clear the threshold.
    insert_events_for_review_run(&db, 60).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
    // A reflection prompt went out, and it is not the draft-review variant.
    assert_eq!(direct_push_count(&orch, "%reflect%").await, 1);
    assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 0);
}

#[tokio::test]
async fn null_no_drafts_task_does_not_send_reflection() {
    let db = test_db().await;
    seed_job_row(&db, None, true).await;
    insert_artifact(&db).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await, None);
    assert_eq!(direct_message_count(&orch, "%").await, 0);
}

#[tokio::test]
async fn null_with_drafts_task_sends_review() {
    let db = test_db().await;
    seed_job_row(&db, None, true).await;
    insert_draft_memory(&db, "m-review", 1).await;
    insert_artifact(&db).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    // Review fires for tasks too when they captured drafts.
    assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
    assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 1);
}

#[tokio::test]
async fn sent_with_running_review_turn_does_not_complete() {
    // The core CAIRN-1576 fix: a warm transition that lands while the
    // reflection turn is still running must not confirm surviving drafts or
    // mark the review done. Completion waits for the review turn to end.
    let db = test_db().await;
    seed_job(&db, Some("sent")).await;
    insert_review_turn(&db, "running").await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await.as_deref(), Some("sent"));
    assert_eq!(memory_status(&orch).await, "draft");
}

#[tokio::test]
async fn idle_flush_creates_memory_review_turn_from_pending_push() {
    let db = test_db().await;
    seed_job(&db, None).await;
    insert_artifact(&db).await;
    insert_run_session_with_turns(&db, 1).await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch);

    finish_memory_review_if_due(&orch, "j-review", "run-review");
    assert_eq!(direct_push_count(&orch, "%Draft memories:%").await, 1);
    assert!(
        crate::orchestrator::attention_push::has_pending_waking_live(&orch.db.local, "j-review")
            .await
            .unwrap()
    );
    crate::messages::delivery::flush_pending_directs_on_idle(&orch, "run-review");

    assert_eq!(memory_review_turn_count(&orch).await, 1);
    assert!(matches!(
        latest_memory_review_turn_state(&orch).await.as_str(),
        "pending" | "running"
    ));
}

#[tokio::test]
async fn stranded_sent_reconcile_resumes_once() {
    let db = test_db().await;
    seed_job(&db, Some("sent")).await;
    insert_artifact(&db).await;
    insert_run_session_with_turns(&db, 1).await;
    db.execute(
            "INSERT INTO messages (id, channel_type, sender_name, recipient_run_id, content, created_at)
             VALUES ('msg-review', 'direct', 'system', 'run-review', 'Before you finish, review the memories you captured during this job. Draft memories:', 2)",
            (),
        )
        .await
        .unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch);
    let first = crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()).unwrap();
    let second =
        crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()).unwrap();

    assert_eq!(first, 1);
    assert_eq!(second, 0);
    assert_eq!(memory_review_turn_count(&orch).await, 1);
}

#[tokio::test]
async fn stranded_sent_reconcile_preserves_queued_user_followup() {
    let db = test_db().await;
    seed_job(&db, Some("sent")).await;
    insert_artifact(&db).await;
    insert_run_session_with_turns(&db, 1).await;
    db.execute(
            "INSERT INTO messages (id, channel_type, sender_name, recipient_run_id, content, created_at)
             VALUES ('msg-review', 'direct', 'system', 'run-review', 'Before you finish, review the memories you captured during this job. Draft memories:', 2)",
            (),
        )
        .await
        .unwrap();
    crate::messages::queued::enqueue_async(
        &db,
        "j-review",
        "user has a follow-up",
        crate::messages::queued::Delivery::Queue,
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch);

    let resumed =
        crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()).unwrap();

    assert_eq!(resumed, 0);
    assert_eq!(memory_review_turn_count(&orch).await, 0);
}

#[tokio::test]
async fn done_state_is_noop() {
    let db = test_db().await;
    seed_job(&db, Some("done")).await;
    let orch = test_orchestrator(db);

    finish_memory_review_if_due(&orch, "j-review", "run-review");

    assert_eq!(review_state(&orch).await.as_deref(), Some("done"));
    assert_eq!(memory_status(&orch).await, "draft");
}
