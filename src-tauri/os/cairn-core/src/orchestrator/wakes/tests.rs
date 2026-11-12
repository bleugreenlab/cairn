use super::child::{issue_recipients, issue_watchers};
use super::matching::matching_subscription;
use super::routing::child_attention_message;
use super::types::FACT_KIND_MESSAGE;

use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::attention_push::Wake;
use crate::orchestrator::Orchestrator;
use cairn_db::turso::params;

use super::*;
use std::sync::Arc;

use crate::db::DbState;
use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
use crate::storage::{LocalDb, RowExt, SearchIndex};
use tempfile::tempdir;

async fn migrated_db() -> LocalDb {
    crate::storage::migrated_test_db("wakes.db").await
}

/// Write a thread's standing definition through the canonical update path and
/// name the session job it establishes — the derived trigger index these tests
/// inspect is rebuilt inside that one transaction.
async fn write_thread_definition(
    db: &LocalDb,
    thread_id: &str,
    definition: &str,
) -> Result<String, String> {
    update_thread(
        db,
        crate::models::UpdateThread {
            id: thread_id.to_string(),
            name: None,
            jurisdiction: None,
            definition: Some(Some(definition.to_string())),
            status: None,
            model: None,
        },
    )
    .await?;
    crate::threads::ensure_thread_session(db, thread_id).await
}

async fn update_thread(db: &LocalDb, input: crate::models::UpdateThread) -> Result<(), String> {
    crate::threads::crud::update(db, input)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[tokio::test(flavor = "current_thread")]
async fn a_thread_child_establishes_and_reaches_its_thread_session() {
    let db = migrated_db().await;
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('p','w','P','P','/tmp',1,1);
         INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES('thread','p','general','active','none',1,1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_thread_id, created_at, updated_at)
           VALUES('child','p',2,'Child','active','active','none','thread',2,2);",
    )
    .await
    .unwrap();
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM jobs WHERE thread_id='thread'", ())
            .await
            .unwrap(),
        Some(0)
    );
    let recipient = coordinating_job_for_child_issue(&db, CHILD_URI)
        .await
        .unwrap()
        .expect("a dormant thread acquires a session before child attention is routed");
    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec![recipient.clone()]
    );
    assert_eq!(
        coordinated_child_issue_uris_for_job(&db, &recipient)
            .await
            .unwrap(),
        vec![CHILD_URI.to_string()],
        "the /wakes inverse projection lists thread-parented children"
    );

    crate::orchestrator::attention_delivery::push_to_issue_watchers(
        &db,
        CHILD_URI,
        None,
        "cairn://p/p/2/1/builder/questions",
        Wake::Wake,
        crate::orchestrator::attention_push::Boundary::Event,
        "question:q1",
    )
    .await
    .unwrap();
    let pushes = crate::orchestrator::attention_push::list_pending(&db, &recipient)
        .await
        .unwrap();
    assert_eq!(pushes.len(), 1);
    assert_eq!(pushes[0].wake, Wake::Wake);
}

/// A thread child's recorded filer (`issues.parent_job_id`) never steers its
/// attention. Here the filer is a live, resumable root job on another issue,
/// exactly what would win were the spawner layer consulted on the thread axis;
/// the thread's own session is still the coordinator, watcher and inverse
/// projection owner. Re-parenting the child under an issue moves it to that
/// issue's coordinator, and a thread-session filer, sitting on no issue, is
/// passed over there too.
#[tokio::test(flavor = "current_thread")]
async fn a_thread_childs_recorded_filer_never_steers_its_attention() {
    let db = migrated_db().await;
    seed_job(&db).await;
    db.execute_script(
        "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES('thread','p','general','active','none',1,1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_thread_id, created_at, updated_at)
           VALUES('child','p',2,'Child','active','active','none','thread',2,2);",
    )
    .await
    .unwrap();
    let session = crate::threads::ensure_thread_session(&db, "thread")
        .await
        .unwrap();

    for filer in ["j", session.as_str()] {
        stamp_spawning_node(&db, filer).await;
        assert_eq!(
            coordinating_job_for_child_issue(&db, CHILD_URI)
                .await
                .unwrap()
                .as_deref(),
            Some(session.as_str()),
            "filer {filer}: the thread session coordinates its child"
        );
        assert_eq!(
            issue_watchers(&db, CHILD_URI).await.unwrap(),
            vec![session.clone()],
            "filer {filer}"
        );
        assert_eq!(
            coordinated_child_issue_uris_for_job(&db, &session)
                .await
                .unwrap(),
            vec![CHILD_URI.to_string()],
            "filer {filer}"
        );
        assert!(coordinated_child_issue_uris_for_job(&db, "j")
            .await
            .unwrap()
            .is_empty());
    }

    db.execute(
        "UPDATE issues SET parent_issue_id = 'i', parent_thread_id = NULL WHERE id = 'child'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        coordinating_job_for_child_issue(&db, CHILD_URI)
            .await
            .unwrap()
            .as_deref(),
        Some("j"),
        "re-parented, the child follows the issue's coordinator, not its thread filer"
    );
}

/// Closing a thread quiets the RECIPIENT, never the child's fact.
///
/// The distinction is the whole design: the child issue keeps producing
/// attention and every other watcher keeps receiving it; what disappears is the
/// derived coordinator on the thread axis, and it comes back on reopen without
/// anything being rebuilt.
#[tokio::test(flavor = "current_thread")]
async fn a_closed_thread_stops_coordinating_its_children_without_silencing_them() {
    let db = migrated_db().await;
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('p','w','P','P','/tmp',1,1);
         INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES('thread','p','general','closed','none',1,1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_thread_id, created_at, updated_at)
           VALUES('child','p',2,'Child','active','active','none','thread',2,2);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
           VALUES('watcher-issue','p',3,'Watcher','active','active','none',3,3);
         INSERT INTO jobs(id, issue_id, project_id, status, node_name, uri_segment, current_session_id, created_at, updated_at)
           VALUES('j-watcher','watcher-issue','p','running','builder','builder','s-watcher',3,3);",
    )
    .await
    .unwrap();

    assert_eq!(
        coordinating_job_for_child_issue(&db, CHILD_URI)
            .await
            .unwrap(),
        None,
        "a closed thread is no derived coordinator"
    );
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM jobs WHERE thread_id='thread'", ())
            .await
            .unwrap(),
        Some(0),
        "and resolving that answer establishes no session behind the closure"
    );

    // An explicit watcher on the same child is untouched.
    super::store::subscribe(&db, "j-watcher", "issue", Some(CHILD_URI), None, "agent")
        .await
        .unwrap();
    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j-watcher".to_string()],
        "the child's own watchers keep receiving what it produces"
    );
    crate::orchestrator::attention_delivery::push_to_issue_watchers(
        &db,
        CHILD_URI,
        None,
        "cairn://p/p/2/1/builder/questions",
        Wake::Wake,
        crate::orchestrator::attention_push::Boundary::Event,
        "question:q1",
    )
    .await
    .unwrap();
    assert_eq!(
        crate::orchestrator::attention_push::list_pending(&db, "j-watcher")
            .await
            .unwrap()
            .len(),
        1
    );

    update_thread(
        &db,
        crate::models::UpdateThread {
            id: "thread".into(),
            name: None,
            jurisdiction: None,
            definition: None,
            status: Some(crate::models::ThreadStatus::Active),
            model: None,
        },
    )
    .await
    .unwrap();
    let recipient = coordinating_job_for_child_issue(&db, CHILD_URI)
        .await
        .unwrap()
        .expect("reopening restores the thread as its children's coordinator");
    assert!(
        issue_watchers(&db, CHILD_URI)
            .await
            .unwrap()
            .contains(&recipient),
        "and it takes its place beside the explicit watcher"
    );
}

/// A wake addressed at a closed thread's session is DROPPED, not delivered and
/// not consumed. A one-shot subscription the thread was holding when it closed
/// still fires on reopen — dormancy suspends delivery rather than spending it.
#[tokio::test(flavor = "current_thread")]
async fn a_targeted_wake_to_a_closed_thread_is_dropped_and_spends_nothing() {
    let db = migrated_db().await;
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES('p','w','P','P','/tmp',1,1);
         INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES('thread','p','general','active','none',1,1);",
    )
    .await
    .unwrap();
    let job_id = crate::threads::ensure_thread_session(&db, "thread")
        .await
        .unwrap();
    seed_run(&db, &job_id).await;
    super::store::subscribe_one_shot(
        &db,
        &job_id,
        "process",
        Some("cairn://p/p/general/terminal/build"),
        None,
        "agent",
    )
    .await
    .unwrap();

    // Delivering the reopened wake resumes the thread's session, which cold-starts
    // its agent process; this test asserts routing, not process mechanics.
    let orch = test_orchestrator_with_services(
        db,
        TestServicesBuilder::new()
            .with_process(RecordingProcessSpawner::new().clone())
            .build(),
    );
    let exit_wake = || WakeEvent {
        source: WakeSource::Process {
            reference: "cairn://p/p/general/terminal/build".into(),
        },
        fact_kind: "exit".into(),
        detail_uri: None,
        delivery: WakeDelivery::Broadcast {
            message: "[Process update] build exited.".into(),
        },
        urgency: DeliveryUrgency::Queue,
        muted: MutedDelivery::Drop,
        fact_key: None,
    };

    set_thread_status(&orch.db.local, "closed").await;
    assert_eq!(
        route_wake(&orch, exit_wake()).await.unwrap(),
        WakeRouteAction::Dropped,
        "a closed thread's session is not a deliverable recipient"
    );
    assert_eq!(
        list_subscriptions_for_job(&orch.db.local, &job_id)
            .await
            .unwrap()
            .iter()
            .filter(|subscription| subscription.one_shot)
            .count(),
        1,
        "and the one-shot it was holding is not spent by the drop"
    );

    set_thread_status(&orch.db.local, "active").await;
    assert_eq!(
        route_wake(&orch, exit_wake()).await.unwrap(),
        WakeRouteAction::Delivered,
        "reopening restores wake delivery"
    );
}

async fn set_thread_status(db: &LocalDb, status: &str) {
    db.execute(
        "UPDATE threads SET status = ?1 WHERE id = 'thread'",
        params![status],
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn thread_definition_triggers_rebuild_and_route_to_a_new_session() {
    let db = migrated_db().await;
    db.execute_script(
        "
        INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('p','w','P','P','/tmp',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('child','p',2,'Child','active','active','none',1,1);
        INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
          VALUES('thread','p','triage','active','none',1,1);
        ",
    )
    .await
    .unwrap();
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM jobs WHERE thread_id='thread'", ())
            .await
            .unwrap(),
        Some(0),
        "the definition write starts from a genuinely session-less thread"
    );

    let definition = serde_json::json!({
        "agent": "thread",
        "triggers": [{
            "fact": "attention",
            "detailUri": CHILD_URI,
            "status": ["merged", "closed", "failed"]
        }]
    })
    .to_string();
    let job_id = write_thread_definition(&db, "thread", &definition)
        .await
        .unwrap();
    seed_run(&db, &job_id).await;

    let derived = list_subscriptions_for_job(&db, &job_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|subscription| subscription.id.starts_with(DERIVED_THREAD_ID_PREFIX))
        .collect::<Vec<_>>();
    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].source_ref.as_deref(), Some(CHILD_URI));
    assert_eq!(
        derived[0].fact_kinds.as_deref(),
        Some(&["resolved".into()][..])
    );

    // Delivering a wake to a session-less thread cold-starts its agent process
    // (the thread message-delivery path), so the orchestrator needs a spawner
    // that accepts the launch; this test asserts routing, not process mechanics.
    let orch = test_orchestrator_with_services(
        db,
        TestServicesBuilder::new()
            .with_process(RecordingProcessSpawner::new().clone())
            .build(),
    );
    let action = route_wake(
        &orch,
        WakeEvent {
            source: WakeSource::Issue {
                reference: CHILD_URI.into(),
            },
            fact_kind: "resolved".into(),
            detail_uri: Some(CHILD_URI.into()),
            delivery: WakeDelivery::Broadcast {
                message: "[Child update] resolved".into(),
            },
            urgency: DeliveryUrgency::Queue,
            muted: MutedDelivery::Drop,
            fact_key: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(action, WakeRouteAction::Delivered);

    orch.db
        .local
        .exclusive(|conn| {
            Box::pin(async move {
                conn.execute(
                    "CREATE TRIGGER fail_derived_thread_subscription
             BEFORE INSERT ON wake_subscriptions
             WHEN NEW.id LIKE 'derived:thread:%'
             BEGIN
               SELECT RAISE(FAIL, 'forced derived rebuild failure');
             END",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let failing_definition = serde_json::json!({
        "agent": "thread",
        "triggers": [{
            "fact": "attention",
            "detailUri": "cairn://p/p/3",
            "status": ["merged", "closed", "failed"]
        }]
    })
    .to_string();
    assert!(
        write_thread_definition(&orch.db.local, "thread", &failing_definition)
            .await
            .unwrap_err()
            .contains("forced derived rebuild failure")
    );
    assert_eq!(
        orch.db
            .local
            .query_one(
                "SELECT definition FROM threads WHERE id='thread'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap(),
        definition,
        "a failed rebuild rolls the definition write back"
    );
    let surviving = list_subscriptions_for_job(&orch.db.local, &job_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|subscription| subscription.id.starts_with(DERIVED_THREAD_ID_PREFIX))
        .collect::<Vec<_>>();
    assert_eq!(surviving.len(), 1);
    assert_eq!(surviving[0].source_ref.as_deref(), Some(CHILD_URI));
    orch.db
        .local
        .exclusive(|conn| {
            Box::pin(async move {
                conn.execute("DROP TRIGGER fail_derived_thread_subscription", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    orch.db
        .local
        .execute(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
             VALUES('duplicate','p','taken','active','none',1,1)",
            (),
        )
        .await
        .unwrap();
    assert!(update_thread(
        &orch.db.local,
        crate::models::UpdateThread {
            id: "thread".into(),
            name: Some("taken".into()),
            jurisdiction: None,
            definition: Some(Some(failing_definition.clone())),
            status: None,
            model: None,
        },
    )
    .await
    .unwrap_err()
    .contains("UNIQUE"));
    assert_eq!(
        orch.db
            .local
            .query_one(
                "SELECT definition FROM threads WHERE id='thread'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap(),
        definition,
        "a duplicate-name metadata failure rolls back the definition too"
    );
    assert_eq!(
        list_subscriptions_for_job(&orch.db.local, &job_id)
            .await
            .unwrap()
            .into_iter()
            .find(|subscription| subscription.id.starts_with(DERIVED_THREAD_ID_PREFIX))
            .and_then(|subscription| subscription.source_ref),
        Some(CHILD_URI.to_string()),
        "a duplicate-name metadata failure preserves the old derived index"
    );

    let rewritten = serde_json::json!({
        "agent": "thread",
        "triggers": []
    })
    .to_string();
    assert_eq!(
        write_thread_definition(&orch.db.local, "thread", &rewritten)
            .await
            .unwrap(),
        job_id,
        "definition rebuild reuses the live session"
    );
    assert!(
        list_subscriptions_for_job(&orch.db.local, &job_id)
            .await
            .unwrap()
            .into_iter()
            .all(|subscription| !subscription.id.starts_with(DERIVED_THREAD_ID_PREFIX)),
        "whole-index rebuild deletes stale derived rows"
    );
}

fn test_orchestrator(db: LocalDb) -> Orchestrator {
    test_orchestrator_with_services(db, TestServicesBuilder::new().build())
}

fn test_orchestrator_with_services(
    db: LocalDb,
    services: crate::services::Services,
) -> Orchestrator {
    let temp = tempdir().unwrap();
    let config_dir = temp.keep();
    let index_path = config_dir.join("search-index.db");
    let db_state = Arc::new(DbState::new(
        Arc::new(db),
        Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
    ));
    Orchestrator::builder(db_state, Arc::new(services), config_dir).build()
}

/// A run for `job_id`, so a delivery to it persists a direct message rather
/// than cold-starting an agent: these tests assert routing, not launches.
async fn seed_run(db: &LocalDb, job_id: &str) {
    db.execute(
        "INSERT INTO runs(id, project_id, job_id, created_at, updated_at) VALUES(?1, 'p', ?2, 1, 1)",
        params![format!("{job_id}-run"), job_id.to_string()],
    )
    .await
    .unwrap();
}

async fn seed_job(db: &LocalDb) {
    db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j','p','i','complete','s',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at) VALUES('j-run','p','j','i',1,1);
            ",
        )
        .await
        .unwrap();
}

async fn seed_second_job(db: &LocalDb) {
    db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j2','p','i','complete','s2',2,2)",
            (),
        )
        .await
        .unwrap();
}

/// The child-attention fixture's child issue: `cairn://p/p/2`, parented to the
/// fixture issue `i` that `seed_job` puts jobs on.
const CHILD_URI: &str = "cairn://p/p/2";

async fn seed_child_issue(db: &LocalDb) {
    db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_issue_id, created_at, updated_at)
             VALUES('child','p',2,'Child','active','active','none','i',2,2)",
            (),
        )
        .await
        .unwrap();
}

/// A `plan>coordinator` execution on the fixture's parent issue `i`, plus the
/// child issue it owns.
///
/// Both agent nodes are recipe-root, both already hold a session, and both carry
/// the SAME `created_at` — that is not a convenience, it is how
/// `create_jobs_for_execution` mints a graph: one pass, one timestamp for every
/// node. Neither has started yet.
async fn seed_plan_coordinator_graph(db: &LocalDb) {
    db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e1','plan-coordinator','i','p','running',5,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, node_name, status, current_session_id, created_at, updated_at)
              VALUES('planner','p','i','e1','planner','pending','s-planner',5,5);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, node_name, status, current_session_id, created_at, updated_at)
              VALUES('coordinator','p','i','e1','coordinator','pending','s-coord',5,5);
            ",
        )
        .await
        .unwrap();
    seed_child_issue(db).await;
}

/// Record `job_id` as the node that filed the fixture's child issue, the way
/// `create_issue_row` stamps it.
async fn stamp_spawning_node(db: &LocalDb, job_id: &str) {
    let job_id = job_id.to_string();
    db.write(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues SET parent_job_id = ?1 WHERE id = 'child'",
                params![job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

struct QueueableNodeFixture {
    job_id: &'static str,
    run_id: &'static str,
    issue_uri: &'static str,
    terminal_uri: &'static str,
}

/// Seed a deliverable node whose wake delivery path queues but does not
/// resume/spawn.
///
/// The fixture includes a complete execution/session/run graph and a running
/// head turn. That makes the node deliverable (`latest_run_for_job` resolves
/// a recipient run) while `nudge_job_for_urgency` sees an active turn and
/// leaves Queue/Steer wakes pending for the next prompt boundary. Insert the
/// turn before updating `jobs.current_turn_id`; FK enforcement rejects the
/// opposite order.
async fn seed_queueable_node(db: &LocalDb) -> QueueableNodeFixture {
    db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES('e','recipe','i','p','running',1,1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, branch, status, current_session_id, created_at, updated_at) VALUES('j','e','builder','i','p','builder','builder','agent/builder','running','s',1,1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at) VALUES('s','j','claude','open',1,1,1);
            INSERT INTO runs(id, project_id, issue_id, job_id, session_id, status, created_at, updated_at) VALUES('r','p','i','j','s','live',1,1);
            INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, started_at, updated_at) VALUES('t','s','r','j',1,'running','initial',1,1,1);
            UPDATE jobs SET current_turn_id = 't' WHERE id = 'j';
            ",
        )
        .await
        .unwrap();

    QueueableNodeFixture {
        job_id: "j",
        run_id: "r",
        issue_uri: "cairn://p/p/1",
        terminal_uri: "cairn://p/p/1/1/builder/terminal/run-1",
    }
}

#[tokio::test(flavor = "current_thread")]
async fn queueable_node_delivers_wake_without_resuming_or_spawning() {
    let db = migrated_db().await;
    let fixture = seed_queueable_node(&db).await;
    subscribe_one_shot(
        &db,
        fixture.job_id,
        "process",
        Some(fixture.terminal_uri),
        Some(&["terminal_exit".to_string()]),
        "agent",
    )
    .await
    .unwrap();
    let recorder = RecordingProcessSpawner::new();
    let orch = test_orchestrator_with_services(
        db,
        TestServicesBuilder::new()
            .with_process(recorder.clone())
            .build(),
    );

    let action = route_terminal_exit_async(
        &orch,
        "run-1",
        fixture.terminal_uri,
        Some(0),
        Some(12),
        Some("ok"),
        None,
    )
    .await
    .unwrap();

    assert_eq!(action, WakeRouteAction::Delivered);
    assert_eq!(
        recorder.spawn_count(),
        0,
        "queue wake must not resume/spawn"
    );
    assert_eq!(recorder.run_count(), 0, "queue wake must not run a process");
    let rows = orch
            .db
            .local
            .query_all(
                "SELECT sender_name, content FROM messages WHERE channel_type='direct' AND id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1)",
                params![fixture.run_id],
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "system");
    assert!(rows[0].1.contains(fixture.terminal_uri));
    assert!(
        list_subscriptions_for_job(&orch.db.local, fixture.job_id)
            .await
            .unwrap()
            .is_empty(),
        "one-shot wake is consumed after delivery"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn queueable_node_fixture_can_drive_issue_wakes_without_tribal_setup() {
    let db = migrated_db().await;
    let fixture = seed_queueable_node(&db).await;
    subscribe(
        &db,
        fixture.job_id,
        "issue",
        Some(fixture.issue_uri),
        Some(&["review".to_string()]),
        "agent",
    )
    .await
    .unwrap();
    let recorder = RecordingProcessSpawner::new();
    let orch = test_orchestrator_with_services(
        db,
        TestServicesBuilder::new()
            .with_process(recorder.clone())
            .build(),
    );

    let action = route_wake(
        &orch,
        WakeEvent {
            source: WakeSource::Issue {
                reference: fixture.issue_uri.to_string(),
            },
            fact_kind: "review".to_string(),
            detail_uri: Some(format!("{}/review", fixture.issue_uri)),
            delivery: WakeDelivery::Broadcast {
                message: "child needs review".to_string(),
            },
            urgency: DeliveryUrgency::Queue,
            muted: MutedDelivery::Drop,
            fact_key: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(action, WakeRouteAction::Delivered);
    assert_eq!(recorder.spawn_count(), 0);
    let rows = orch
        .db
        .local
        .query_all(
            "SELECT COUNT(*) FROM messages WHERE channel_type='direct' AND id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1)",
            params![fixture.run_id],
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![1]);
}

#[test]
fn terminal_exit_message_carries_slug_code_runtime_uri_and_tail() {
    let msg = format_terminal_exit_message(
        "run-1",
        "cairn://p/p/1/1/builder/terminal/run-1",
        Some(2),
        Some(125),
        Some("error: boom"),
        None,
    );
    assert!(msg.contains("run-1"), "{msg}");
    assert!(msg.contains("exit code 2"), "{msg}");
    assert!(msg.contains("2m05s"), "{msg}");
    assert!(
        msg.contains("cairn://p/p/1/1/builder/terminal/run-1"),
        "{msg}"
    );
    assert!(msg.contains("error: boom"), "{msg}");
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_one_shot_sets_flag_and_persists() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let sub = subscribe_one_shot(
        &db,
        "j",
        "process",
        Some("run-1"),
        Some(&["terminal_exit".to_string()]),
        "agent",
    )
    .await
    .unwrap();
    assert!(sub.one_shot);
    let listed = list_subscriptions_for_job(&db, "j").await.unwrap();
    assert!(listed
        .iter()
        .any(|s| s.one_shot && s.source_ref.as_deref() == Some("run-1")));
}

/// The cairn/4820 specimen: one settlement routed by all three callers (the
/// edge, the subscribe-time fire and the verdictless confirm) is one message
/// per watcher, a persistent subscriber and a one-shot one alike. A new
/// actionable verdict on the same lanes is a second message; routing the
/// unchanged settlement again after it is none.
#[tokio::test(flavor = "current_thread")]
async fn one_checks_settlement_is_one_message_per_watcher_however_often_routed() {
    use crate::execution::checks_settlement::{classify, ChecksSnapshot};
    use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
    use crate::messages::delivery::HeadTurn;

    let db = migrated_db().await;
    seed_job(&db).await;
    seed_second_job(&db).await;
    seed_run(&db, "j2").await;
    let orch = test_orchestrator(db);
    let uri = "cairn://p/p/1/1/builder/checks";
    let kinds = ["checks_settled".to_string()];
    subscribe(
        &orch.db.local,
        "j",
        "condition",
        Some(uri),
        Some(&kinds),
        "agent",
    )
    .await
    .unwrap();
    subscribe_one_shot(
        &orch.db.local,
        "j2",
        "condition",
        Some(uri),
        Some(&kinds),
        "agent",
    )
    .await
    .unwrap();
    let snapshot = |observation: &str| {
        let statuses = vec![NodeCheckStatus {
            observation_id: Some(observation.to_string()),
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-tests".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: None,
            ran_at: None,
            passed: Some(38),
            failed: Some(2),
            skipped: None,
            deferred: None,
            parser: None,
            complete: None,
            selection: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
            declared_unverified: None,
            progress: None,
            base_attribution: None,
            evidence: None,
            plan_audit_note: None,
        }];
        ChecksSnapshot {
            settlement: classify(&statuses, HeadTurn::Idle, false),
            statuses,
            terminal_reason: None,
            retired: None,
        }
    };
    let told = |run: &'static str| {
        let db = orch.db.local.clone();
        async move {
            db.query_one(
                "SELECT COUNT(*) FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1) AND content LIKE '[Checks%'",
                (run.to_string(),),
                |row| row.i64(0),
            )
            .await
            .unwrap()
        }
    };

    let first = snapshot("obs-1");
    for _caller in ["edge", "subscribe-time fire", "verdictless confirm"] {
        route_checks_settled(&orch, "builder", uri, &first)
            .await
            .unwrap();
    }
    assert_eq!(told("j-run").await, 1, "the persistent subscriber");
    assert_eq!(told("j2-run").await, 1, "the one-shot subscriber");

    route_checks_settled(&orch, "builder", uri, &snapshot("obs-2"))
        .await
        .unwrap();
    assert_eq!(told("j-run").await, 2, "a new verdict is a new settlement");
    route_checks_settled(&orch, "builder", uri, &snapshot("obs-2"))
        .await
        .unwrap();
    assert_eq!(told("j-run").await, 2, "and routing it again is not");
}

/// A settled-checks subscription is keyed on the node's canonical `/checks`
/// URI, so it wakes for that node and no other. The whole point of the
/// subscription is to let an orchestrator end its turn while a child's suite
/// runs; a cross-node match would resume it on a stranger's verdict.
#[tokio::test(flavor = "current_thread")]
async fn checks_settled_wake_fires_once_for_its_own_node_then_is_consumed() {
    use crate::execution::checks_settlement::{classify, ChecksSnapshot, Settlement};
    use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
    use crate::messages::delivery::HeadTurn;

    let db = migrated_db().await;
    seed_job(&db).await;
    let orch = test_orchestrator(db);
    let uri = "cairn://p/p/1/1/builder/checks";
    subscribe_one_shot(
        &orch.db.local,
        "j",
        "condition",
        Some(uri),
        Some(&["checks_settled".to_string()]),
        "agent",
    )
    .await
    .unwrap();

    let statuses = vec![NodeCheckStatus {
        observation_id: None,
        job_id: "job".to_string(),
        request_id: None,
        name: "rust-tests".to_string(),
        state: NodeCheckState::Failed,
        policy: "advisory".to_string(),
        when: "review".to_string(),
        cached: None,
        duration_ms: None,
        ran_at: None,
        passed: Some(38),
        failed: Some(2),
        skipped: None,
        deferred: None,
        parser: None,
        complete: None,
        selection: None,
        suite_failures: None,
        failure_names: Vec::new(),
        output_tail: None,
        failure_kind: None,
        suppressed_after: None,
        executor: None,
        verified_on: None,
        notice: None,
        declared_unverified: None,
        progress: None,
        base_attribution: None,
        evidence: None,
        plan_audit_note: None,
    }];
    let snapshot = ChecksSnapshot {
        settlement: classify(&statuses, HeadTurn::Idle, false),
        statuses,
        terminal_reason: None,
        retired: None,
    };
    assert!(matches!(snapshot.settlement, Settlement::Settled { .. }));

    let other = route_checks_settled(
        &orch,
        "builder",
        "cairn://p/p/9/1/builder/checks",
        &snapshot,
    )
    .await
    .unwrap();
    assert_eq!(
        other,
        WakeRouteAction::Dropped,
        "another node's lanes settling must not wake this subscriber"
    );

    let action = route_checks_settled(&orch, "builder", uri, &snapshot)
        .await
        .unwrap();
    assert_eq!(action, WakeRouteAction::Delivered);

    let subs = list_subscriptions_for_job(&orch.db.local, "j")
        .await
        .unwrap();
    assert!(
        !subs
            .iter()
            .any(|s| s.source_kind == "condition" && s.source_ref.as_deref() == Some(uri)),
        "settlement is a moment, so its one-shot subscription must be consumed"
    );
}

/// The subscribe-time read is the one settlement question asked with neither
/// the routing edges' ordering nor the polling wait's dwell, so it is the one
/// place the completion-to-arming window can be observed raw. A subscription
/// landing inside another node's window sees every lane about to run as
/// verdictless; firing on that would consume the one-shot row and wake the
/// orchestrator before the checks ran, with nothing left for the correctly
/// ordered edge to repair.
#[test]
fn a_verdictless_subscribe_time_reading_is_confirmed_rather_than_fired_on() {
    use super::checks::immediate_fire;
    use crate::execution::checks_settlement::{classify, Settlement};
    use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
    use crate::messages::delivery::HeadTurn;

    let lane = |name: &str, state: NodeCheckState| NodeCheckStatus {
        observation_id: None,
        job_id: "job".to_string(),
        request_id: None,
        name: name.to_string(),
        state,
        policy: "advisory".to_string(),
        when: "review".to_string(),
        cached: None,
        duration_ms: None,
        ran_at: None,
        passed: None,
        failed: None,
        skipped: None,
        deferred: None,
        parser: None,
        complete: None,
        selection: None,
        suite_failures: None,
        failure_names: Vec::new(),
        output_tail: None,
        failure_kind: None,
        suppressed_after: None,
        executor: None,
        verified_on: None,
        notice: None,
        declared_unverified: None,
        progress: None,
        base_attribution: None,
        evidence: None,
        plan_audit_note: None,
    };

    // What a node caught between its turn completing and its wave arming looks
    // like: idle, no wave in flight, every lane still without a verdict. It
    // classifies as settled, and must NOT be fired on immediately.
    let mid_arming = [lane("rust-tests", NodeCheckState::Pending)];
    let settlement = classify(&mid_arming, HeadTurn::Idle, false);
    assert!(matches!(settlement, Settlement::Settled { .. }));
    assert!(
        !immediate_fire(&settlement),
        "a verdictless reading is indistinguishable from the arming window and must be confirmed first"
    );

    // Real verdicts are not ambiguous: nothing about that reading could be the
    // arming window, so the subscriber is woken at once.
    let settled = [
        lane("rust-tests", NodeCheckState::Passed),
        lane("frontend-tests", NodeCheckState::NotApplicable),
    ];
    assert!(immediate_fire(&classify(&settled, HeadTurn::Idle, false)));

    // A node still moving was never a candidate either way.
    let moving = [lane("rust-tests", NodeCheckState::Running)];
    assert!(!immediate_fire(&classify(&moving, HeadTurn::Idle, true)));
}

/// The resume is what the orchestrator acts on, so it has to carry the verdict
/// and the lanes rather than only a URI to go read.
#[test]
fn checks_settled_message_carries_verdict_tally_lanes_and_gaps() {
    use crate::execution::checks_settlement::{classify, ChecksSnapshot};
    use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
    use crate::messages::delivery::HeadTurn;

    let lane = |name: &str, state: NodeCheckState| NodeCheckStatus {
        observation_id: None,
        job_id: "job".to_string(),
        request_id: None,
        name: name.to_string(),
        state,
        policy: "advisory".to_string(),
        when: "review".to_string(),
        cached: None,
        duration_ms: None,
        ran_at: None,
        passed: None,
        failed: None,
        skipped: None,
        deferred: None,
        parser: None,
        complete: None,
        selection: None,
        suite_failures: None,
        failure_names: Vec::new(),
        output_tail: None,
        failure_kind: None,
        suppressed_after: None,
        executor: None,
        verified_on: None,
        notice: None,
        declared_unverified: None,
        progress: None,
        base_attribution: None,
        evidence: None,
        plan_audit_note: None,
    };
    let statuses = vec![
        lane("rust-lint", NodeCheckState::Pending),
        lane("rust-tests", NodeCheckState::Passed),
    ];
    let snapshot = ChecksSnapshot {
        settlement: classify(&statuses, HeadTurn::Idle, false),
        statuses,
        terminal_reason: Some("issue merged before submission".to_string()),
        retired: None,
    };
    let uri = "cairn://p/p/1/1/builder/checks";
    let message = format_checks_settled_message("builder", uri, &snapshot);

    assert!(message.contains("[Checks settled]"));
    assert!(message.contains("builder"));
    assert!(message.contains("incomplete"));
    assert!(message.contains(uri));
    assert!(message.contains("- rust-tests [passed]"));
    assert!(
        message.contains("No verdict was produced for: rust-lint"),
        "a lane nothing will run must be named, not silently absent: {message}"
    );
    assert!(message.contains("Terminal reason: issue merged before submission"));
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_exit_wake_fires_once_then_is_consumed() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let orch = test_orchestrator(db);
    // The subscription is keyed on the canonical URI, matching what the route
    // side emits.
    let uri = "cairn://p/p/1/1/builder/terminal/run-1";
    subscribe_one_shot(
        &orch.db.local,
        "j",
        "process",
        Some(uri),
        Some(&["terminal_exit".to_string()]),
        "agent",
    )
    .await
    .unwrap();

    // A same-slug terminal in a different scope must NOT match.
    let other = route_terminal_exit_async(
        &orch,
        "run-1",
        "cairn://p/p/9/1/builder/terminal/run-1",
        Some(0),
        Some(3),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        other,
        WakeRouteAction::Dropped,
        "a same-slug terminal in another scope must not wake this subscriber"
    );
    assert!(
        list_subscriptions_for_job(&orch.db.local, "j")
            .await
            .unwrap()
            .iter()
            .any(|s| s.source_kind == "process"),
        "a non-matching exit must leave the one-shot subscription intact"
    );

    let action =
        route_terminal_exit_async(&orch, "run-1", uri, Some(0), Some(12), Some("ok"), None)
            .await
            .unwrap();
    assert_eq!(action, WakeRouteAction::Delivered);

    // The one-shot subscription is consumed on first matching fire.
    let subs = list_subscriptions_for_job(&orch.db.local, "j")
        .await
        .unwrap();
    assert!(
        !subs
            .iter()
            .any(|s| s.source_kind == "process" && s.source_ref.as_deref() == Some(uri)),
        "one-shot subscription should be gone after firing"
    );

    // A second exit event for the same terminal finds nothing.
    let again = route_terminal_exit_async(&orch, "run-1", uri, Some(0), None, None, None)
        .await
        .unwrap();
    assert_eq!(again, WakeRouteAction::Dropped);
}

/// A terminal Cairn settled after its process was lost has no exit code, and
/// its exit wake leads with why it ended instead of an "exit code unknown" that
/// reads like a finished command.
#[test]
fn terminal_exit_message_leads_with_a_recorded_reason() {
    let msg = format_terminal_exit_message(
        "tests",
        "cairn://p/p/1/1/builder/terminal/tests",
        None,
        Some(3240),
        Some("Compiling cairn-core"),
        Some("process lost: executor bgsrv-ub no longer reports this terminal's process"),
    );
    assert!(
        msg.starts_with("[Terminal exit] `tests` ended — process lost: executor bgsrv-ub"),
        "{msg}"
    );
    assert!(!msg.contains("exit code unknown"), "{msg}");
    assert!(msg.contains("54m00s") || msg.contains("0h54m"), "{msg}");
    assert!(msg.contains("Compiling cairn-core"), "{msg}");
}

#[test]
fn terminal_output_message_carries_slug_phrase_uri_and_excerpt() {
    let msg = format_terminal_output_message(
        "dev",
        "cairn://p/p/1/1/builder/terminal/dev",
        "ready",
        Some("VITE ready in 412 ms"),
    );
    assert!(msg.contains("dev"), "{msg}");
    assert!(msg.contains("ready"), "{msg}");
    assert!(
        msg.contains("cairn://p/p/1/1/builder/terminal/dev"),
        "{msg}"
    );
    assert!(msg.contains("VITE ready in 412 ms"), "{msg}");
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_output_wake_delivers_targeted_and_consumes_one_shot() {
    let db = migrated_db().await;
    let fixture = seed_queueable_node(&db).await;
    subscribe_terminal_output_one_shot(&db, fixture.job_id, fixture.terminal_uri, "ready", "agent")
        .await
        .unwrap();
    let recorder = RecordingProcessSpawner::new();
    let orch = test_orchestrator_with_services(
        db,
        TestServicesBuilder::new()
            .with_process(recorder.clone())
            .build(),
    );

    let action = route_terminal_output_async(
        &orch,
        fixture.job_id,
        "dev",
        fixture.terminal_uri,
        "ready",
        Some("server ready on :3860"),
    )
    .await
    .unwrap();
    assert_eq!(action, WakeRouteAction::Delivered);
    assert_eq!(
        recorder.spawn_count(),
        0,
        "queue wake must not resume/spawn"
    );

    let rows = orch
        .db
        .local
        .query_all(
            "SELECT content FROM messages WHERE channel_type='direct' AND id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1)",
            params![fixture.run_id],
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("ready"), "{}", rows[0]);
    assert!(rows[0].contains(fixture.terminal_uri), "{}", rows[0]);
    assert!(rows[0].contains("server ready on :3860"), "{}", rows[0]);

    assert!(
        list_subscriptions_for_job(&orch.db.local, fixture.job_id)
            .await
            .unwrap()
            .is_empty(),
        "one-shot output wake is consumed after delivery"
    );

    // A second output match for the same terminal finds nothing left to wake.
    let again = route_terminal_output_async(
        &orch,
        fixture.job_id,
        "dev",
        fixture.terminal_uri,
        "ready",
        None,
    )
    .await
    .unwrap();
    assert_eq!(again, WakeRouteAction::Dropped);
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_output_subscription_also_wakes_on_exit() {
    let db = migrated_db().await;
    let fixture = seed_queueable_node(&db).await;
    subscribe_terminal_output_one_shot(&db, fixture.job_id, fixture.terminal_uri, "ready", "agent")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    // The terminal dies before ever printing the phrase; the dual-fact
    // subscription still wakes the waiting agent on exit rather than
    // stranding it forever.
    let action = route_terminal_exit_async(
        &orch,
        "dev",
        fixture.terminal_uri,
        Some(1),
        Some(3),
        Some("error: build failed"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(action, WakeRouteAction::Delivered);
    assert!(
        list_subscriptions_for_job(&orch.db.local, fixture.job_id)
            .await
            .unwrap()
            .is_empty(),
        "the output subscription is consumed by the exit wake"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_output_watchers_persist_for_session_hydration() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let uri = "cairn://p/p/1/1/builder/terminal/dev";
    subscribe_terminal_output_one_shot(&db, "j", uri, "ready", "agent")
        .await
        .unwrap();

    // A (re)starting session hydrates its in-memory watchers from this list,
    // so the subscription is not bound to the session live at subscribe time.
    let watchers = list_terminal_output_watchers(&db, uri).await.unwrap();
    assert_eq!(watchers.len(), 1);
    assert_eq!(watchers[0].1, "j", "carries the subscribing job");
    assert_eq!(watchers[0].2, "ready", "carries the phrase to scan for");
    assert_eq!(watchers[0].3, uri, "carries the canonical terminal URI");

    // The same rows resolve by (job_id, slug), which is how the interactive
    // reader — which lacks the canonical URI — hydrates its registry.
    let by_slug = list_terminal_output_watchers_for_job_terminal(&db, "j", "dev")
        .await
        .unwrap();
    assert_eq!(by_slug.len(), 1);
    assert_eq!(by_slug[0].3, uri);
    // A different slug on the same job does not match.
    let other_slug = list_terminal_output_watchers_for_job_terminal(&db, "j", "build")
        .await
        .unwrap();
    assert!(other_slug.is_empty());

    // A different terminal URI shares nothing.
    let other = list_terminal_output_watchers(&db, "cairn://p/p/1/1/builder/terminal/other")
        .await
        .unwrap();
    assert!(other.is_empty());
}

#[test]
fn child_attention_message_with_detail_reads_detail_once() {
    let issue_uri = "cairn://p/p/2";
    let detail_uri = "cairn://p/p/2/1/builder/permissions/perm-2";
    let message = child_attention_message(
        issue_uri,
        "needs_approval",
        "agent_idle_with_work",
        Some(detail_uri),
    );

    assert_eq!(
            message,
            "[Child update] needs_approval/agent_idle_with_work. Read cairn://p/p/2/1/builder/permissions/perm-2."
        );
    assert_eq!(message.matches(issue_uri).count(), 1);
    assert_eq!(message.matches(detail_uri).count(), 1);
}

#[test]
fn child_attention_message_without_detail_reads_issue_once() {
    let issue_uri = "cairn://p/p/2";
    let message = child_attention_message(issue_uri, "needs_input", "question", None);

    assert_eq!(
        message,
        "[Child update] needs_input/question. Read cairn://p/p/2."
    );
    assert_eq!(message.matches(issue_uri).count(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn scoped_fact_kinds_match_granularly() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let kinds = vec![
        "pr_state_change".to_string(),
        "agent_idle_with_work".to_string(),
    ];
    subscribe(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        Some(&kinds),
        "agent",
    )
    .await
    .unwrap();
    mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        Some(&kinds),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    assert!(
        matching_subscription(&db, "j", "issue", Some("cairn://p/p/2"), "pr_state_change")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        matching_subscription(&db, "j", "issue", Some("cairn://p/p/2"), "question")
            .await
            .unwrap()
            .is_none()
    );
}

/// A narrow mute on a derived child watch narrows only what it names. The mute
/// row is the coordinator's only row for the child, but it must not shrink the
/// watch itself: unnamed facts still wake.
#[tokio::test(flavor = "current_thread")]
async fn narrow_mute_downgrades_only_the_facts_it_names() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;
    let kinds = vec![
        "pr_state_change".to_string(),
        "agent_idle_with_work".to_string(),
    ];
    mute(
        &db,
        "j",
        "issue",
        Some(CHILD_URI),
        Some(&kinds),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j".to_string()],
        "a muted coordinator still watches its child"
    );
    assert_eq!(
        mute_downgrade(
            &db,
            "j",
            "issue",
            Some(CHILD_URI),
            "pr_state_change",
            Wake::Wake
        )
        .await
        .unwrap(),
        Wake::Passive
    );
    assert_eq!(
        mute_downgrade(&db, "j", "issue", Some(CHILD_URI), "question", Wake::Wake)
            .await
            .unwrap(),
        Wake::Wake,
        "a fact the mute does not name still wakes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn narrow_active_overrides_broad_muted_scope() {
    let db = migrated_db().await;
    seed_job(&db).await;
    subscribe(&db, "j", "issue", Some("cairn://p/p/2"), None, "agent")
        .await
        .unwrap();
    mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    let kinds = vec!["question".to_string()];
    subscribe(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        Some(&kinds),
        "agent",
    )
    .await
    .unwrap();

    let question = matching_subscription(&db, "j", "issue", Some("cairn://p/p/2"), "question")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(question.state, WakeSubscriptionState::Active);
    assert_eq!(question.fact_kinds.as_ref().unwrap(), &kinds);

    let pr = matching_subscription(&db, "j", "issue", Some("cairn://p/p/2"), "pr_state_change")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pr.state, WakeSubscriptionState::Muted);
    assert!(pr.fact_kinds.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn muted_non_interrupt_wake_drops_but_interrupt_pierces_mute() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let sub = mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    // CAIRN-1900: mute is downgrade-at-creation for pushes; the suppressed_wakes
    // digest store is gone. A non-interrupt wake to a muted source on these live
    // non-push callers is dropped and writes no suppressed_wakes row.
    let queue_action = route_wake(
        &orch,
        WakeEvent {
            source: WakeSource::Issue {
                reference: "cairn://p/p/2".to_string(),
            },
            fact_kind: "pr_state_change".to_string(),
            detail_uri: Some("cairn://p/p/2/1/pr".to_string()),
            delivery: WakeDelivery::Broadcast {
                message: "routine PR update".to_string(),
            },
            urgency: DeliveryUrgency::Queue,
            muted: MutedDelivery::Drop,
            fact_key: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(queue_action, WakeRouteAction::Dropped);
    assert!(
        peek_pending_suppressed_for_job(&orch.db.local, "j")
            .await
            .unwrap()
            .is_empty(),
        "a muted non-interrupt wake writes no suppressed_wakes digest row"
    );

    // Interrupt still pierces the mute and delivers.
    let interrupt_action = route_wake(
        &orch,
        WakeEvent {
            source: WakeSource::Issue {
                reference: "cairn://p/p/2".to_string(),
            },
            fact_kind: "question".to_string(),
            detail_uri: Some("cairn://p/p/2/1/questions/q".to_string()),
            delivery: WakeDelivery::Targeted {
                subscriber_job_id: sub.job_id.clone(),
                sender_name: None,
                message: "needs answer".to_string(),
            },
            urgency: DeliveryUrgency::Interrupt,
            muted: MutedDelivery::Drop,
            fact_key: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(interrupt_action, WakeRouteAction::Delivered);
}

#[tokio::test(flavor = "current_thread")]
async fn passive_message_like_wake_respects_subscription_state() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_default_job_subscriptions(&db, "j").await.unwrap();
    let orch = test_orchestrator(db);

    let delivered = route_wake(
        &orch,
        WakeEvent {
            source: WakeSource::User,
            fact_kind: FACT_KIND_MESSAGE.to_string(),
            detail_uri: Some("cairn://p/p/1/1/builder".to_string()),
            delivery: WakeDelivery::MessageDigest {
                subscriber_job_id: "j".to_string(),
                content: "passive note".to_string(),
            },
            urgency: DeliveryUrgency::Passive,
            muted: MutedDelivery::Drop,
            fact_key: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(delivered, WakeRouteAction::Delivered);
    assert!(
        peek_pending_suppressed_for_job(&orch.db.local, "j")
            .await
            .unwrap()
            .is_empty(),
        "active passive messages remain claimable through their original row, not wake digest"
    );

    unsubscribe_matching(&orch.db.local, "j", "user", None, "agent")
        .await
        .unwrap();
    let dropped = route_wake(
        &orch,
        WakeEvent {
            source: WakeSource::User,
            fact_kind: FACT_KIND_MESSAGE.to_string(),
            detail_uri: Some("cairn://p/p/1/1/builder".to_string()),
            delivery: WakeDelivery::MessageDigest {
                subscriber_job_id: "j".to_string(),
                content: "dropped note".to_string(),
            },
            urgency: DeliveryUrgency::Interrupt,
            muted: MutedDelivery::Drop,
            fact_key: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(dropped, WakeRouteAction::Dropped);
}

// ---- Child attention: derived from the parent edge (CAIRN-3293) -------------
//
// The recipient of a child's attention is the node currently driving the parent
// issue, resolved at wake time. These cover the three ways a coordinator comes
// to own a child (it filed the child, someone else filed it pre-parented, it
// started an execution on an already-parented parent) plus coordinator
// succession, since a snapshot taken at filing time got the first case right and
// silently failed the rest.

#[tokio::test(flavor = "current_thread")]
async fn a_parented_child_reaches_its_coordinator_with_no_subscription_row() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;

    assert_eq!(
        coordinating_job_for_child_issue(&db, CHILD_URI)
            .await
            .unwrap(),
        Some("j".to_string())
    );
    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j".to_string()]
    );
    assert!(
        list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .is_empty(),
        "the watch is derived, so nothing is persisted for it"
    );
}

/// Coordinator succession. A second execution's coordinator on the parent issue
/// owns the parent's children from the moment it exists; the retired one stops
/// receiving them. Both directions of the CAIRN-3293 defect are this assertion.
#[tokio::test(flavor = "current_thread")]
async fn a_new_coordinator_execution_takes_over_the_parents_children() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;
    seed_second_job(&db).await;

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j2".to_string()],
        "the newest coordinator receives the child's gates, and only it"
    );
    assert!(
        coordinated_child_issue_uris_for_job(&db, "j")
            .await
            .unwrap()
            .is_empty(),
        "a superseded coordinator coordinates nothing"
    );
    assert_eq!(
        coordinated_child_issue_uris_for_job(&db, "j2")
            .await
            .unwrap(),
        vec![CHILD_URI.to_string()]
    );
}

/// A child filed by a job working some *other* issue, then parented here. The
/// recorded spawner only names the recipient while it sits on the child's own
/// parent issue, so this one is rejected and the parent's driver takes it. This
/// is one of CAIRN-3293's two failure modes, answered by validation rather than
/// by discarding the record.
#[tokio::test(flavor = "current_thread")]
async fn a_child_filed_by_another_issues_job_reaches_the_parents_coordinator() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;
    db.execute_script(
            "
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('other','p',3,'Other','active','active','none',3,3);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('filer','p','other','running','sf',9,9);
            ",
        )
        .await
        .unwrap();
    stamp_spawning_node(&db, "filer").await;

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j".to_string()],
        "a spawner on another issue never enters the child's watcher set"
    );
}

/// CAIRN-3712, the reported bug. Every node of a `plan>coordinator` graph is
/// minted in one pass with one shared `created_at`, so recency alone cannot tell
/// the upstream planner from the coordinator that actually spawned the children —
/// and the tie resolved to the planner, which then received every child's
/// catch-up, question, permission, and review. The child records which node filed
/// it, so it reaches that node.
#[tokio::test(flavor = "current_thread")]
async fn a_spawned_child_reaches_the_node_that_filed_it_not_its_upstream() {
    let db = migrated_db().await;
    seed_plan_coordinator_graph(&db).await;
    stamp_spawning_node(&db, "coordinator").await;

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["coordinator".to_string()]
    );
    assert_eq!(
        coordinated_child_issue_uris_for_job(&db, "coordinator")
            .await
            .unwrap(),
        vec![CHILD_URI.to_string()],
        "and the /wakes projection agrees with the routing"
    );
    assert!(
        coordinated_child_issue_uris_for_job(&db, "planner")
            .await
            .unwrap()
            .is_empty(),
        "the upstream planner coordinates nothing it did not file"
    );
}

/// CAIRN-3293's case under the new design. The recorded spawner is validated on
/// every wake, so a coordinator whose execution has been superseded stops
/// receiving: the parent's children move to the execution that replaced it.
#[tokio::test(flavor = "current_thread")]
async fn a_superseded_spawner_hands_its_children_to_the_new_execution() {
    let db = migrated_db().await;
    seed_plan_coordinator_graph(&db).await;
    stamp_spawning_node(&db, "coordinator").await;
    db.execute_script(
            "
            UPDATE executions SET lifecycle='superseded' WHERE id='e1';
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
              VALUES('e2','plan-coordinator','i','p','running',20,2,'canonical');
            INSERT INTO jobs(id, project_id, issue_id, execution_id, node_name, status, current_session_id, started_at, created_at, updated_at)
              VALUES('coordinator-2','p','i','e2','coordinator','running','s-coord-2',21,20,20);
            ",
        )
        .await
        .unwrap();

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["coordinator-2".to_string()],
        "a retired coordinator does not keep the children it filed"
    );
    assert!(coordinated_child_issue_uris_for_job(&db, "coordinator")
        .await
        .unwrap()
        .is_empty());
}

/// A spawner that can no longer receive anything is no recipient: resolution
/// falls through to whoever is driving the parent now.
#[tokio::test(flavor = "current_thread")]
async fn a_dead_spawner_falls_through_to_the_parents_driver() {
    let db = migrated_db().await;
    seed_plan_coordinator_graph(&db).await;
    stamp_spawning_node(&db, "coordinator").await;
    db.execute_script(
        "
            UPDATE jobs SET started_at = 6, status = 'complete' WHERE id = 'planner';
            UPDATE jobs SET status = 'failed' WHERE id = 'coordinator';
            ",
    )
    .await
    .unwrap();

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["planner".to_string()]
    );
}

/// With no spawner recorded (a human-created child, a legacy row, an adopted
/// issue) the recipient is derived — and the same one-pass `created_at` tie is
/// broken by which node has actually started.
#[tokio::test(flavor = "current_thread")]
async fn the_derived_driver_is_the_node_that_started_last() {
    let db = migrated_db().await;
    seed_plan_coordinator_graph(&db).await;

    // The planner ran and finished; the coordinator started after it and is now
    // holding the issue open.
    db.execute_script(
        "
            UPDATE jobs SET started_at = 6, status = 'complete' WHERE id = 'planner';
            UPDATE jobs SET started_at = 7, status = 'idle' WHERE id = 'coordinator';
            ",
    )
    .await
    .unwrap();
    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["coordinator".to_string()],
        "a started coordinator outranks the finished planner it came from"
    );

    // Rewind: the coordinator has not started yet, so the planner is still the
    // node driving the parent.
    db.execute(
        "UPDATE jobs SET started_at = NULL, status = 'pending' WHERE id = 'coordinator'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["planner".to_string()],
        "a pending coordinator never outranks the node still running"
    );
}

/// Acquisition path 3: the child is already parented before any execution runs on
/// the parent, and the coordinator arrives later. Execution start mints nothing,
/// which is exactly why the recipient must be derived rather than minted.
#[tokio::test(flavor = "current_thread")]
async fn a_coordinator_started_after_the_child_still_receives_it() {
    let db = migrated_db().await;
    db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            ",
        )
        .await
        .unwrap();
    seed_child_issue(&db).await;

    assert!(issue_watchers(&db, CHILD_URI).await.unwrap().is_empty());

    db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES('late','p','i','running','sl',5,5)",
            (),
        )
        .await
        .unwrap();

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["late".to_string()]
    );
}

/// A coordinator's delegated sub-task is not a coordinator, even when it is the
/// newest job on the parent issue and holds a live session. This mis-pick is what
/// originally motivated snapshotting the spawning job (CAIRN-1302); the
/// recipe-root filter is what makes the live derivation safe from it.
#[tokio::test(flavor = "current_thread")]
async fn a_delegated_sub_task_never_coordinates_a_child() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;
    db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, parent_job_id, status, current_session_id, created_at, updated_at)
             VALUES('subtask','p','i','j','complete','ss',9,9)",
            (),
        )
        .await
        .unwrap();

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j".to_string()]
    );
}

/// A failed job, or one that never ran, cannot be the coordinator — attention
/// falls back to the newest job that can actually receive it.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_or_never_run_parent_job_does_not_coordinate() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;
    db.execute_script(
            "
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('failed','p','i','failed','sx',8,8);
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
              VALUES('unstarted','p','i','pending',9,9);
            ",
        )
        .await
        .unwrap();

    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["j".to_string()]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_unparented_issue_has_no_derived_watcher() {
    let db = migrated_db().await;
    seed_job(&db).await;
    db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('child','p',2,'Child','active','active','none',2,2)",
            (),
        )
        .await
        .unwrap();

    assert_eq!(
        coordinating_job_for_child_issue(&db, CHILD_URI)
            .await
            .unwrap(),
        None
    );
    assert!(issue_watchers(&db, CHILD_URI).await.unwrap().is_empty());
}

/// Opting out of a derived watch has no row to flip, so the refusal is recorded
/// as one. Without that, the derivation would re-add the coordinator on the very
/// next wake.
#[tokio::test(flavor = "current_thread")]
async fn an_explicit_unsubscribe_opts_a_coordinator_out_of_its_derived_watch() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;

    assert_eq!(
        unsubscribe_matching(&db, "j", "issue", Some(CHILD_URI), "agent")
            .await
            .unwrap(),
        1
    );
    assert!(
        issue_watchers(&db, CHILD_URI).await.unwrap().is_empty(),
        "a recorded refusal survives the derivation"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn manual_watchers_and_the_derived_coordinator_both_watch() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;
    db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES('manual','p','i','complete','sm',3,3)",
            (),
        )
        .await
        .unwrap();
    subscribe(&db, "manual", "issue", Some(CHILD_URI), None, "agent")
        .await
        .unwrap();

    // `manual` is the newest recipe-root job on the parent, so it is also the
    // derived coordinator; its explicit row is what governs it, and no duplicate
    // recipient appears.
    assert_eq!(
        issue_watchers(&db, CHILD_URI).await.unwrap(),
        vec!["manual".to_string()]
    );
}

/// The broadcast router (external message replies on a child issue) resolves the
/// derived coordinator too, scoped to the default child fact kinds.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_child_facts_match_the_derived_coordinator() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_child_issue(&db).await;

    let matched = issue_recipients(&db, CHILD_URI, "message").await.unwrap();
    assert_eq!(matched.len(), 1);
    assert_eq!(
        matched[0].recipient,
        cairn_attention::wakes::StableRecipient::Job("j".into())
    );
    assert_eq!(
        matched[0].reasons,
        vec![cairn_attention::wakes::Reached {
            reason: cairn_attention::wakes::Reason::Parent {
                issue: CHILD_URI.into()
            },
            level: Wake::Wake,
        }]
    );

    // The derived watch carries exactly the fact vocabulary the seeded default
    // carried, so what the broadcast router reaches is unchanged. Notably
    // `external_message_reply` — the one fact still routed this way — is outside
    // that vocabulary and has always required an explicit broad issue
    // subscription, which the derivation deliberately does not widen.
    for outside in ["terminal_exit", "external_message_reply"] {
        assert!(
            issue_recipients(&db, CHILD_URI, outside)
                .await
                .unwrap()
                .is_empty(),
            "{outside} is outside the child fact vocabulary"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn review_fact_aliases_match_legacy_child_subscription_kinds() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let legacy = vec![
        "agent_idle_with_work".to_string(),
        "pr_state_change".to_string(),
    ];
    subscribe(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        Some(&legacy),
        "agent",
    )
    .await
    .unwrap();

    assert!(
        matching_subscription(&db, "j", "issue", Some("cairn://p/p/2"), "review")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn source_taxonomy_is_validated() {
    let db = migrated_db().await;
    seed_job(&db).await;
    assert!(subscribe(&db, "j", "issue", None, None, "agent")
        .await
        .is_err());
    assert!(subscribe(&db, "j", "user", Some("nope"), None, "agent")
        .await
        .is_err());
    assert!(subscribe(&db, "j", "time", None, None, "agent")
        .await
        .is_err());
    let sub = subscribe(&db, "j", "user", None, None, "agent")
        .await
        .unwrap();
    assert_eq!(sub.source_kind, "user");
    assert!(sub.source_ref.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn mute_creates_a_scoped_subscription() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let sub = mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/99"),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    assert_eq!(sub.state, WakeSubscriptionState::Muted);
    assert_eq!(sub.source_kind, "issue");
    assert_eq!(sub.source_ref.as_deref(), Some("cairn://p/p/99"));
    assert_eq!(list_subscriptions_for_job(&db, "j").await.unwrap().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn default_seed_does_not_reactivate_unsubscribed_scope() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_default_job_subscriptions(&db, "j").await.unwrap();
    unsubscribe_matching(&db, "j", "user", None, "agent")
        .await
        .unwrap();
    seed_default_job_subscriptions(&db, "j").await.unwrap();

    let user = matching_subscription(&db, "j", "user", None, "message")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user.state, WakeSubscriptionState::Unsubscribed);
}

#[tokio::test(flavor = "current_thread")]
async fn default_job_subscriptions_cover_user_and_any_peer() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_default_job_subscriptions(&db, "j").await.unwrap();
    let user = matching_subscription(&db, "j", "user", None, "message")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user.state, WakeSubscriptionState::Active);
    let peer = matching_subscription(&db, "j", "peer", Some("cairn://p/p/1/1/planner"), "message")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.state, WakeSubscriptionState::Active);
    assert!(
        peer.source_ref.is_none(),
        "broad peer subscription should match any peer ref"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn specific_peer_subscription_overrides_broad_default() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_default_job_subscriptions(&db, "j").await.unwrap();
    subscribe(
        &db,
        "j",
        "peer",
        Some("cairn://p/p/1/1/planner"),
        None,
        "system",
    )
    .await
    .unwrap();
    mute(
        &db,
        "j",
        "peer",
        Some("cairn://p/p/1/1/planner"),
        None,
        None,
        None,
        "system",
    )
    .await
    .unwrap();

    let specific =
        matching_subscription(&db, "j", "peer", Some("cairn://p/p/1/1/planner"), "message")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(specific.state, WakeSubscriptionState::Muted);
    assert_eq!(
        specific.source_ref.as_deref(),
        Some("cairn://p/p/1/1/planner")
    );

    let other = matching_subscription(
        &db,
        "j",
        "peer",
        Some("cairn://p/p/1/1/reviewer"),
        "message",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(other.state, WakeSubscriptionState::Active);
    assert!(other.source_ref.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn source_matching_returns_best_subscription_for_every_subscriber() {
    let db = migrated_db().await;
    seed_job(&db).await;
    seed_second_job(&db).await;
    subscribe(&db, "j", "issue", Some("cairn://p/p/2"), None, "agent")
        .await
        .unwrap();
    subscribe(&db, "j2", "issue", Some("cairn://p/p/2"), None, "agent")
        .await
        .unwrap();
    let routine = vec!["pr_state_change".to_string()];
    mute(
        &db,
        "j2",
        "issue",
        Some("cairn://p/p/2"),
        Some(&routine),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();

    // One candidate per subscriber, each reached through its own best row: a
    // mute keeps its subscriber, passively.
    let matches = issue_recipients(&db, "cairn://p/p/2", "pr_state_change")
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
    let reached = |job: &str| {
        matches
            .iter()
            .find(|candidate| {
                candidate.recipient == cairn_attention::wakes::StableRecipient::Job(job.into())
            })
            .unwrap()
    };
    assert_eq!(reached("j").level, Wake::Wake);
    assert!(!reached("j").only_muted());
    assert_eq!(reached("j2").level, Wake::Passive);
    assert!(reached("j2").only_muted());
}

#[tokio::test(flavor = "current_thread")]
async fn digest_render_names_lifted_scope_and_live_wake() {
    let notice = SuppressedWake {
        id: "n".to_string(),
        subscription_id: Some("s".to_string()),
        job_id: "j".to_string(),
        source_kind: "issue".to_string(),
        source_ref: Some("cairn://p/p/2".to_string()),
        fact_kind: Some("pr_state_change".to_string()),
        occurrences: 3,
        latest_detail_uri: Some("latest".to_string()),
        content: None,
        created_at: 1,
        updated_at: 1,
        delivered_at: None,
    };
    let rendered = SuppressedWake::render_digest_with_context(&[notice], Some(&WakeSource::User));
    assert!(rendered.contains("lifting wake snooze on issue cairn://p/p/2"));
    assert!(rendered.contains("woken by: user"));
    assert!(rendered.contains("pr_state_change ×3"));
}

#[tokio::test(flavor = "current_thread")]
async fn mute_downgrade_lowers_wake_for_muted_source_only() {
    let db = migrated_db().await;
    seed_job(&db).await;
    // Unmuted source: a requested Wake stays Wake.
    assert_eq!(
        mute_downgrade(
            &db,
            "j",
            "issue",
            Some("cairn://p/p/2"),
            "review",
            Wake::Wake
        )
        .await
        .unwrap(),
        Wake::Wake
    );
    mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/p/2"),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    // Muted source: a requested Wake is downgraded to Passive (ride-along).
    assert_eq!(
        mute_downgrade(
            &db,
            "j",
            "issue",
            Some("cairn://p/p/2"),
            "review",
            Wake::Wake
        )
        .await
        .unwrap(),
        Wake::Passive
    );
    // Interrupt is never downgraded, even when muted.
    assert_eq!(
        mute_downgrade(
            &db,
            "j",
            "issue",
            Some("cairn://p/p/2"),
            "review",
            Wake::Interrupt
        )
        .await
        .unwrap(),
        Wake::Interrupt
    );
    // Passive is already the lowest level and short-circuits unchanged.
    assert_eq!(
        mute_downgrade(
            &db,
            "j",
            "issue",
            Some("cairn://p/p/2"),
            "review",
            Wake::Passive
        )
        .await
        .unwrap(),
        Wake::Passive
    );
}

/// The rebuild wake, judged against synthetic boots. Ancestry is answered from
/// a table here; `cairn_vcs::history::contains` has its own store tests.
mod rebuild {
    use super::*;
    use crate::orchestrator::build_change::BuildChange;
    use futures_util::future::BoxFuture;
    use std::time::Duration;

    const TARGET: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CONTAINING: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OLDER: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const WAKE_LINE: &str = "From camp base: Cairn was rebuilt: 0.13.8 (content-v1:test) \u{2192} 0.13.9 (content-v2:test)";
    const BASE_CAMP: &str = "cairn://p/cairn/camps/base";

    /// `CONTAINING` contains `TARGET`; nothing else contains anything.
    struct Graph;

    impl Containment for Graph {
        fn contains<'a>(
            &'a self,
            _project: &'a str,
            target_commit: &'a str,
            boot_commit: &'a str,
        ) -> BoxFuture<'a, Result<bool, String>> {
            let answer = target_commit == boot_commit
                || (target_commit == TARGET && boot_commit == CONTAINING);
            Box::pin(async move { Ok(answer) })
        }
    }

    fn boot(source_commit: Option<&str>) -> RunnerBoot {
        RunnerBoot {
            booted_at: 1_800_000_000,
            version: "0.13.9".to_string(),
            build_id: "content-v2:test".to_string(),
            source_commit: source_commit.map(str::to_string),
            build_change: None,
        }
    }

    /// A containing boot that changed builds, so it owes base camps a notice.
    fn rebuilt_boot() -> RunnerBoot {
        RunnerBoot {
            build_change: Some(BuildChange {
                from_version: "0.13.8".to_string(),
                from_build_id: "content-v1:test".to_string(),
                to_version: "0.13.9".to_string(),
                to_build_id: "content-v2:test".to_string(),
                booted_at: 1_800_000_000,
            }),
            ..boot(Some(CONTAINING))
        }
    }

    fn commit_clause() -> RebuildClause {
        RebuildClause::new(RebuildTarget::parse(TARGET).unwrap(), "p")
    }

    async fn pass_on(orch: &Orchestrator, boot: &RunnerBoot) -> RebuildPass {
        evaluate_rebuild_wakes(orch, boot, &Graph, Duration::ZERO)
            .await
            .unwrap()
    }

    async fn pass(orch: &Orchestrator, commit: Option<&str>) -> RebuildPass {
        pass_on(orch, &boot(commit)).await
    }

    async fn armed(orch: &Orchestrator, job_id: &str) -> usize {
        list_subscriptions_for_job(&orch.db.local, job_id)
            .await
            .unwrap()
            .iter()
            .filter(|subscription| RebuildClause::from_subscription(subscription).is_some())
            .count()
    }

    /// The direct messages a run holds once the fact log's drain has run.
    async fn delivered(orch: &Orchestrator, run_id: &str) -> Vec<String> {
        crate::routes::drain_fact_log(orch).await;
        orch.db
            .local
            .query_all(
                "SELECT content FROM messages WHERE channel_type='direct' AND id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1)",
                params![run_id],
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    fn queueable_orchestrator(db: LocalDb) -> Orchestrator {
        test_orchestrator_with_services(
            db,
            TestServicesBuilder::new()
                .with_process(RecordingProcessSpawner::new())
                .build(),
        )
    }

    #[test]
    fn a_target_is_a_full_commit_or_an_issue_and_nothing_else() {
        assert_eq!(
            RebuildTarget::parse(&TARGET.to_ascii_uppercase()).unwrap(),
            RebuildTarget::Commit(TARGET.to_string()),
            "a commit id is stored lowercase"
        );
        assert_eq!(
            RebuildTarget::parse("cairn://p/cairn/6158").unwrap(),
            RebuildTarget::Issue {
                project: "cairn".to_string(),
                number: 6158
            }
        );
        for refused in ["abc123", "main", "cairn://p/cairn/6158/1/builder", ""] {
            assert!(
                RebuildTarget::parse(refused).is_err(),
                "{refused:?} is not a target"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_stored_clause_reads_back_and_renders_with_its_target() {
        let db = migrated_db().await;
        let fixture = seed_queueable_node(&db).await;
        let orch = queueable_orchestrator(db);
        subscribe_rebuild_once(
            &orch,
            fixture.job_id,
            &commit_clause(),
            "agent",
            None,
            &Graph,
        )
        .await
        .unwrap();
        let subscriptions = list_subscriptions_for_job(&orch.db.local, fixture.job_id)
            .await
            .unwrap();
        let row = subscriptions
            .iter()
            .find(|subscription| subscription.source_kind == SOURCE_KIND_FACT)
            .expect("a rebuild wake is a fact subscription");
        assert!(row.one_shot);
        assert_eq!(RebuildClause::from_subscription(row), Some(commit_clause()));
        let line = describe_rebuild_subscription(row).unwrap();
        assert!(
            line.contains(&format!("rebuild contains `{TARGET}`")),
            "{line}"
        );
        assert!(line.contains("waiting"), "{line}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fires_once_on_the_first_boot_that_contains_the_target_and_never_on_unknown() {
        let db = migrated_db().await;
        let fixture = seed_queueable_node(&db).await;
        let orch = queueable_orchestrator(db);
        let outcome = subscribe_rebuild_once(
            &orch,
            fixture.job_id,
            &commit_clause(),
            "agent",
            Some(&boot(Some(OLDER))),
            &Graph,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            RebuildSubscribeOutcome::Waiting(RebuildWait::NotContained {
                boot_commit: OLDER.to_string()
            })
        );

        assert_eq!(
            pass(&orch, None).await,
            RebuildPass {
                waiting: 1,
                ..RebuildPass::default()
            },
            "a boot whose commit is unknown satisfies nothing"
        );
        assert_eq!(
            pass(&orch, Some(OLDER)).await,
            RebuildPass {
                waiting: 1,
                ..RebuildPass::default()
            },
            "a boot that does not contain the target leaves it waiting"
        );
        assert!(delivered(&orch, fixture.run_id).await.is_empty());
        assert_eq!(armed(&orch, fixture.job_id).await, 1);

        assert_eq!(
            pass(&orch, Some(CONTAINING)).await,
            RebuildPass {
                fired: 1,
                ..RebuildPass::default()
            }
        );
        assert_eq!(
            armed(&orch, fixture.job_id).await,
            0,
            "the firing spends it as its receipt is written"
        );
        let messages = delivered(&orch, fixture.run_id).await;
        assert_eq!(messages.len(), 1);
        assert!(
            !messages[0].starts_with("From camp"),
            "a boot with no camp fact is not framed as one: {}",
            messages[0]
        );
        for fact in [
            "booted with a build that contains",
            TARGET,
            CONTAINING,
            "0.13.9",
            "content-v2:test",
        ] {
            assert!(
                messages[0].contains(fact),
                "{fact} missing: {}",
                messages[0]
            );
        }
        assert_eq!(armed(&orch, fixture.job_id).await, 0, "firing spends it");

        assert_eq!(pass(&orch, Some(CONTAINING)).await, RebuildPass::default());
        assert_eq!(
            delivered(&orch, fixture.run_id).await.len(),
            1,
            "a later containing boot does not fire it again"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fires_at_once_when_the_current_boot_already_contains_the_target() {
        let db = migrated_db().await;
        let fixture = seed_queueable_node(&db).await;
        let orch = queueable_orchestrator(db);
        let outcome = subscribe_rebuild_once(
            &orch,
            fixture.job_id,
            &commit_clause(),
            "agent",
            Some(&boot(Some(CONTAINING))),
            &Graph,
        )
        .await
        .unwrap();
        assert_eq!(outcome, RebuildSubscribeOutcome::FiredNow);
        let messages = delivered(&orch, fixture.run_id).await;
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0].contains("current boot already contains")
                && messages[0].contains("fired at once"),
            "the wake says why it fired without a rebuild: {}",
            messages[0]
        );
        assert_eq!(armed(&orch, fixture.job_id).await, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_issue_target_waits_for_its_merge_then_resolves_through_the_merge_commit() {
        let db = migrated_db().await;
        let fixture = seed_queueable_node(&db).await;
        db.execute_script(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('i2','p',2,'Target','active','active','none',1,1);",
        )
        .await
        .unwrap();
        let orch = queueable_orchestrator(db);

        let missing = RebuildClause::new(RebuildTarget::parse("cairn://p/p/99").unwrap(), "p");
        assert!(
            subscribe_rebuild_once(&orch, fixture.job_id, &missing, "agent", None, &Graph)
                .await
                .is_err(),
            "an issue that does not exist is refused, not waited on forever"
        );

        let clause = RebuildClause::new(RebuildTarget::parse("cairn://p/p/2").unwrap(), "other");
        assert_eq!(clause.project, "p", "an issue's own project answers");
        let outcome = subscribe_rebuild_once(
            &orch,
            fixture.job_id,
            &clause,
            "agent",
            Some(&boot(Some(CONTAINING))),
            &Graph,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            RebuildSubscribeOutcome::Waiting(RebuildWait::IssueNotMerged),
            "a not-yet-merged issue is accepted and waits"
        );

        orch.db
            .local
            .execute(
                "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                            target_branch, status, opened_at, updated_at,
                                            merged_commit, merged_at)
                 VALUES ('mr', 'j', 'p', 'i2', 'Target', 'agent/b', 'main', 'merged', 1, 5, ?1, 5)",
                params![TARGET],
            )
            .await
            .unwrap();
        assert_eq!(
            pass(&orch, Some(OLDER)).await.waiting,
            1,
            "merged, but this boot does not contain the merge commit"
        );
        assert_eq!(pass(&orch, Some(CONTAINING)).await.fired, 1);
        let messages = delivered(&orch, fixture.run_id).await;
        assert!(
            messages[0].contains(&format!("cairn://p/p/2 (merged as `{TARGET}`)")),
            "{}",
            messages[0]
        );
    }

    /// (e) A closed thread cannot take the wake now. The firing is still
    /// written and the subscription spent, so no later boot fires it again;
    /// the receipt waits, listed on the thread's `/wakes`, and is delivered
    /// once the thread reopens, and never again.
    #[tokio::test(flavor = "current_thread")]
    async fn a_closed_thread_s_firing_waits_queued_and_is_delivered_once_on_reopen() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('thread','p','general','active','none',1,1);",
        )
        .await
        .unwrap();
        let job_id = crate::threads::ensure_thread_session(&db, "thread")
            .await
            .unwrap();
        super::seed_run(&db, &job_id).await;
        let run_id = format!("{job_id}-run");
        let orch = queueable_orchestrator(db);
        subscribe_rebuild_once(&orch, &job_id, &commit_clause(), "agent", None, &Graph)
            .await
            .unwrap();

        set_thread_status(&orch.db.local, "closed").await;
        assert_eq!(pass(&orch, Some(CONTAINING)).await.fired, 1);
        assert_eq!(armed(&orch, &job_id).await, 0, "never re-armed");
        let now = || chrono::Utc::now().timestamp_millis();
        assert_eq!(
            crate::routes::flush_due_wakes(&orch, now()).await.unwrap(),
            1
        );
        assert!(delivered_now(&orch, &run_id).await.is_empty());
        assert_eq!(
            crate::routes::flush_due_wakes(&orch, now()).await.unwrap(),
            0,
            "held, it is parked until its horizon, not polled"
        );
        let parked = orch
            .db
            .local
            .query_opt_i64(
                "SELECT deliver_after FROM fact_log_wakes WHERE delivered_at IS NULL",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            parked >= now() + crate::routes::HELD_HORIZON_MS - 60_000,
            "{parked}"
        );
        let thread = cairn_attention::wakes::StableRecipient::Thread("thread".into());
        let held = cairn_attention::wakes::overdue_receipts(
            &orch.db.local,
            &thread,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();
        assert_eq!(held.len(), 1, "the held firing is visible on /wakes");
        assert!(held[0].fact_key.starts_with(REBUILD_WAKE_KEY_PREFIX));
        assert_eq!(
            held[0].last_refusal.as_deref(),
            Some("the thread is closed")
        );

        reopen(&orch, "thread").await;
        assert_eq!(pass(&orch, Some(CONTAINING)).await, RebuildPass::default());
        assert_eq!(
            crate::routes::flush_due_wakes(&orch, now()).await.unwrap(),
            1,
            "reopening re-arms the held wake"
        );
        let messages = delivered_now(&orch, &run_id).await;
        assert_eq!(messages.len(), 1, "delivered once it can be");
        assert!(messages[0].contains("booted with a build that contains"));
        crate::storage::recover_fact_log(&orch.db.local, i64::MAX / 2, 0)
            .await
            .unwrap();
        assert_eq!(delivered(&orch, &run_id).await.len(), 1, "and never again");
    }

    /// The direct messages a run holds now, without draining.
    async fn delivered_now(orch: &Orchestrator, run_id: &str) -> Vec<String> {
        orch.db
            .local
            .query_all(
                "SELECT content FROM messages WHERE channel_type='direct' AND id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1)",
                params![run_id],
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    async fn reopen(orch: &Orchestrator, thread_id: &str) {
        crate::threads::crud::update_with_lifecycle(
            orch,
            &orch.db.local,
            crate::models::UpdateThread {
                id: thread_id.to_string(),
                name: None,
                jurisdiction: None,
                definition: None,
                status: Some(crate::models::ThreadStatus::Active),
                model: None,
            },
        )
        .await
        .unwrap();
    }

    /// A held firing whose thread never reopens is dropped at the horizon,
    /// with its reason kept on the thread's `/wakes`, and never delivered.
    #[tokio::test(flavor = "current_thread")]
    async fn a_held_firing_is_dropped_at_its_horizon_and_stays_listed() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','P','P','/tmp',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('thread','p','general','active','none',1,1);",
        )
        .await
        .unwrap();
        let job_id = crate::threads::ensure_thread_session(&db, "thread")
            .await
            .unwrap();
        super::seed_run(&db, &job_id).await;
        let run_id = format!("{job_id}-run");
        let orch = queueable_orchestrator(db);
        subscribe_rebuild_once(&orch, &job_id, &commit_clause(), "agent", None, &Graph)
            .await
            .unwrap();
        set_thread_status(&orch.db.local, "closed").await;
        assert_eq!(pass(&orch, Some(CONTAINING)).await.fired, 1);
        let now = || chrono::Utc::now().timestamp_millis();
        crate::routes::flush_due_wakes(&orch, now()).await.unwrap();

        // The horizon passes with the thread still closed.
        orch.db
            .local
            .execute_batch(&format!(
                "UPDATE delivery_receipts SET created_at = created_at - {};
                 UPDATE fact_log_wakes SET deliver_after = 0 WHERE delivered_at IS NULL;",
                crate::routes::HELD_HORIZON_MS + 1_000
            ))
            .await
            .unwrap();
        assert_eq!(
            crate::routes::flush_due_wakes(&orch, now()).await.unwrap(),
            1
        );
        let (state, why) = orch
            .db
            .local
            .query_opt(
                "SELECT state, last_refusal FROM delivery_receipts",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state, "dropped");
        assert_eq!(
            why.as_deref(),
            Some("held 7 days and never deliverable (the thread is closed); dropped")
        );
        let listed = cairn_attention::wakes::dropped_receipts(
            &orch.db.local,
            &cairn_attention::wakes::StableRecipient::Thread("thread".into()),
            REBUILD_WAKE_KEY_PREFIX,
            now() - crate::routes::HELD_HORIZON_MS,
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 1, "the drop stays visible on /wakes");

        reopen(&orch, "thread").await;
        assert!(
            delivered(&orch, &run_id).await.is_empty(),
            "never delivered"
        );
    }

    /// Base camp, whose members are thread `n0` and the routes fixture's
    /// thread `t`. The project took no turns, so it is outside the rebuild
    /// announcement's activity window.
    async fn camp_fixture(temp: &std::path::Path) -> (Orchestrator, Arc<LocalDb>) {
        let (orch, db) = crate::routes::fixture::orchestrator(temp, &[]).await;
        db.execute_batch(
            "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('n0','p','n0','active',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('n0-job','n0','p','running','thread',1,1);
             INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('n0-run','n0-job','running',1,1);",
        )
        .await
        .unwrap();
        db.ensure_base_camp("p", 1).await.unwrap();
        (orch, db)
    }

    fn camp_clause() -> RebuildClause {
        RebuildClause::new(RebuildTarget::parse(TARGET).unwrap(), "cairn")
    }

    async fn told(db: &LocalDb, run: &str) -> Vec<(String, String)> {
        db.query_all(
            "SELECT sender_name, content FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = ?1) ORDER BY rowid",
            params![run],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
    }

    /// How each wake owed to `n0` was delivered: (level, action).
    async fn n0_wakes(db: &LocalDb) -> Vec<(String, Option<String>)> {
        db.query_all(
            "SELECT level, delivered_action FROM fact_log_wakes WHERE recipient_key = 'thread:n0' ORDER BY id",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
    }

    async fn count(db: &LocalDb, sql: &str) -> i64 {
        db.query_opt_i64(sql, ()).await.unwrap().unwrap_or_default()
    }

    /// (d) Nothing more after delivery: a later boot finds nothing pending,
    /// a repeated evaluator pass fires nothing, a restart's recovery delivers
    /// nothing, and a firing re-attempted for the same subscription is
    /// refused by its receipt.
    async fn nothing_more_for_n0(orch: &Orchestrator, db: &LocalDb, subscription_id: &str) {
        assert_eq!(pass_on(orch, &rebuilt_boot()).await, RebuildPass::default());
        crate::storage::recover_fact_log(db, i64::MAX / 2, 0)
            .await
            .unwrap();
        crate::routes::drain_fact_log(orch).await;
        db.execute(
            "INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, fact_kinds_json, state, created_by, created_at, updated_at, one_shot)
             VALUES(?1, 'n0-job', ?2, ?3, ?4, 'active', 'agent', 1, 1, 1)",
            params![
                subscription_id,
                SOURCE_KIND_FACT,
                camp_clause().clause_ref().unwrap(),
                fact_kinds_json(Some(&[RUNNER_BOOTED.to_string()]))
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            pass_on(orch, &rebuilt_boot()).await,
            RebuildPass::default(),
            "the unique firing key refuses a second receipt"
        );
        crate::routes::drain_fact_log(orch).await;
        assert_eq!(told(db, "n0-run").await.len(), 1, "nothing more");
    }

    async fn subscribe_n0(orch: &Orchestrator, boot: Option<&RunnerBoot>) -> String {
        let outcome = subscribe_rebuild_once(orch, "n0-job", &camp_clause(), "agent", boot, &Graph)
            .await
            .unwrap();
        if boot.is_none() {
            assert_eq!(outcome, RebuildSubscribeOutcome::NoBootRecorded);
        }
        let db = orch.db.local.clone();
        db.query_opt_text(
            "SELECT fact_key FROM delivery_receipts WHERE fact_key LIKE 'rebuild-wake:%'
             UNION ALL SELECT ?1 || id FROM wake_subscriptions WHERE job_id = 'n0-job' AND one_shot = 1",
            params![REBUILD_WAKE_KEY_PREFIX],
        )
        .await
        .unwrap()
        .and_then(|key| key.strip_prefix(REBUILD_WAKE_KEY_PREFIX).map(str::to_owned))
        .expect("the subscription or its firing")
    }

    /// (a) A boot firing for a base camp member is one message at wake, framed
    /// by camp, when the camp fact was indexed first.
    #[tokio::test]
    async fn a_boot_firing_beside_an_indexed_camp_fact_is_one_camp_message_at_wake() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = camp_fixture(temp.path()).await;
        let subscription = subscribe_n0(&orch, None).await;
        let change = rebuilt_boot().build_change.unwrap();
        crate::orchestrator::build_change::announce_project(&orch, &change, "p", "cairn")
            .await
            .unwrap()
            .expect("the notice is new");
        crate::routes::index_pass(&orch, &crate::routes::EventLogPolicy::default())
            .await
            .unwrap();

        assert_eq!(pass_on(&orch, &rebuilt_boot()).await.fired, 1);
        crate::routes::drain_fact_log(&orch).await;
        let messages = told(&db, "n0-run").await;
        assert_eq!(messages.len(), 1, "{messages:?}");
        assert_eq!(messages[0].0, BASE_CAMP);
        assert!(messages[0].1.starts_with(WAKE_LINE), "{}", messages[0].1);
        assert_eq!(
            n0_wakes(&db).await,
            [("wake".to_string(), Some("delivered".to_string()))]
        );
        nothing_more_for_n0(&orch, &db, &subscription).await;
    }

    /// (a) The other write order, delivered before the camp fact is even
    /// indexed, and (f) a project outside the activity window still gets its
    /// fact: the firing mints it.
    #[tokio::test]
    async fn a_boot_firing_delivered_before_its_camp_fact_is_indexed_is_still_one_message() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = camp_fixture(temp.path()).await;
        let subscription = subscribe_n0(&orch, None).await;

        assert_eq!(pass_on(&orch, &rebuilt_boot()).await.fired, 1);
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'camp_notice'"
            )
            .await,
            1,
            "the subscriber's project is told though it took no turns"
        );
        crate::routes::flush_due_wakes(&orch, i64::MAX)
            .await
            .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        let messages = told(&db, "n0-run").await;
        assert_eq!(messages.len(), 1, "{messages:?}");
        assert!(messages[0].1.starts_with(WAKE_LINE), "{}", messages[0].1);
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM delivery_receipts WHERE recipient_id = 'n0' AND state = 'delivered'"
            )
            .await,
            2,
            "the camp fact's receipt was carried by the firing's message"
        );
        nothing_more_for_n0(&orch, &db, &subscription).await;
    }

    /// (b) Subscribing to a target the current boot already contains, after
    /// the thread read the boot's camp fact passively: one more message, at
    /// wake, with the camp framing.
    #[tokio::test]
    async fn a_subscribe_time_firing_after_a_passive_camp_line_is_one_camp_message_at_wake() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = camp_fixture(temp.path()).await;
        let change = rebuilt_boot().build_change.unwrap();
        crate::orchestrator::build_change::announce_project(&orch, &change, "p", "cairn")
            .await
            .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        let passive = told(&db, "n0-run").await;
        assert_eq!(passive.len(), 1);
        assert!(
            passive[0]
                .1
                .starts_with("From camp base: Cairn was rebuilt"),
            "{}",
            passive[0].1
        );

        let subscription = subscribe_n0(&orch, Some(&rebuilt_boot())).await;
        crate::routes::drain_fact_log(&orch).await;
        let messages = told(&db, "n0-run").await;
        assert_eq!(messages.len(), 2, "{messages:?}");
        assert!(messages[1].1.starts_with(WAKE_LINE), "{}", messages[1].1);
        assert_eq!(
            n0_wakes(&db).await,
            [
                ("passive".to_string(), Some("suppressed".to_string())),
                ("wake".to_string(), Some("delivered".to_string()))
            ]
        );
        let after = told(&db, "n0-run").await.len();
        assert_eq!(
            pass_on(&orch, &rebuilt_boot()).await,
            RebuildPass::default()
        );
        crate::storage::recover_fact_log(&db, i64::MAX / 2, 0)
            .await
            .unwrap();
        crate::routes::drain_fact_log(&orch).await;
        assert_eq!(told(&db, "n0-run").await.len(), after);
        assert!(!subscription.is_empty());
    }

    /// (c) Subscribing to an already-contained target on a boot with no camp
    /// fact: one message at wake, in the firing's own words.
    #[tokio::test]
    async fn a_subscribe_time_firing_with_no_camp_fact_is_one_message_in_its_own_words() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = camp_fixture(temp.path()).await;
        subscribe_n0(&orch, Some(&boot(Some(CONTAINING)))).await;
        crate::routes::drain_fact_log(&orch).await;
        let messages = told(&db, "n0-run").await;
        assert_eq!(messages.len(), 1, "{messages:?}");
        assert_eq!(messages[0].0, "system");
        assert!(
            messages[0]
                .1
                .starts_with("[Rebuild] The runner's current boot already contains"),
            "{}",
            messages[0].1
        );
        // The firing's own wake holds the no-row sentinel, and nothing reads
        // it as a row: no event line, no events range.
        let sentinel = db
            .query_opt(
                "SELECT source, first_row_id, last_row_id FROM fact_log_wakes WHERE recipient_key = 'thread:n0'",
                (),
                |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            sentinel,
            (REBUILD_WAKE_SOURCE.to_string(), NO_FACT_ROW, NO_FACT_ROW)
        );
        assert!(
            !messages[0].1.contains("[Event]") && !messages[0].1.contains("events"),
            "{}",
            messages[0].1
        );
        assert_eq!(
            n0_wakes(&db).await,
            [("wake".to_string(), Some("delivered".to_string()))]
        );
        assert_eq!(
            pass_on(&orch, &boot(Some(CONTAINING))).await,
            RebuildPass::default()
        );
        crate::routes::drain_fact_log(&orch).await;
        assert_eq!(told(&db, "n0-run").await.len(), 1);
    }

    /// Boot-time firings are paced by the drain: each one's wake comes due a
    /// spacing after the previous one's.
    #[tokio::test]
    async fn boot_firings_come_due_a_spacing_apart() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = camp_fixture(temp.path()).await;
        subscribe_n0(&orch, None).await;
        subscribe_rebuild_once(&orch, "watcher", &camp_clause(), "agent", None, &Graph)
            .await
            .unwrap();
        let began = chrono::Utc::now().timestamp_millis();
        let pass = evaluate_rebuild_wakes(
            &orch,
            &boot(Some(CONTAINING)),
            &Graph,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert_eq!(pass.fired, 2);
        let due: Vec<i64> = db
            .query_all(
                "SELECT deliver_after FROM fact_log_wakes ORDER BY deliver_after",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(due.len(), 2);
        assert!(due[0] < began + 3_000, "the first is due at once: {due:?}");
        assert!(
            due[1] >= began + 3_000,
            "the second is due a spacing after the pass began: {due:?}"
        );
    }

    /// The single-fact pin, extended to the rebuild notice and its firings:
    /// however many threads wait on a rebuild, the boot is one base-camp fact
    /// row, and each waiting thread gets exactly one message, at wake.
    #[tokio::test]
    async fn a_rebuild_is_one_camp_fact_beside_its_firings_at_any_size() {
        for threads in [1usize, 3, 25] {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
            let mut seed = String::new();
            for n in 0..threads {
                seed.push_str(&format!(
                    "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('n{n}','p','n{n}','active',1,1);
                     INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('n{n}-job','n{n}','p','running','thread',1,1);
                     INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('n{n}-run','n{n}-job','running',1,1);"
                ));
            }
            db.execute_batch(&seed).await.unwrap();
            db.ensure_base_camp("p", 1).await.unwrap();
            for n in 0..threads {
                subscribe_rebuild_once(
                    &orch,
                    &format!("n{n}-job"),
                    &camp_clause(),
                    "agent",
                    None,
                    &Graph,
                )
                .await
                .unwrap();
            }
            assert_eq!(pass_on(&orch, &rebuilt_boot()).await.fired, threads);
            crate::routes::drain_fact_log(&orch).await;
            assert_eq!(
                count(
                    &db,
                    "SELECT COUNT(*) FROM fact_log WHERE source = 'camp_notice'"
                )
                .await,
                1,
                "{threads} threads"
            );
            for n in 0..threads {
                let messages = told(&db, &format!("n{n}-run")).await;
                assert_eq!(messages.len(), 1, "n{n} of {threads}: {messages:?}");
                assert!(messages[0].1.starts_with(WAKE_LINE), "{}", messages[0].1);
            }
        }
    }
}
