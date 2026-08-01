use super::matching::{matching_subscription, matching_subscriptions_for_source};
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

async fn seed_job(db: &LocalDb) {
    db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j','p','i','complete','s',1,1);
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

/// The child-attention fixture's child issue: `cairn://p/P/2`, parented to the
/// fixture issue `i` that `seed_job` puts jobs on.
const CHILD_URI: &str = "cairn://p/P/2";

async fn seed_child_issue(db: &LocalDb) {
    db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_issue_id, created_at, updated_at)
             VALUES('child','p',2,'Child','active','active','none','i',2,2)",
            (),
        )
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
        issue_uri: "cairn://p/P/1",
        terminal_uri: "cairn://p/P/1/1/builder/terminal/run-1",
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
                "SELECT sender_name, content FROM messages WHERE channel_type='direct' AND recipient_run_id = ?1",
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
            "SELECT COUNT(*) FROM messages WHERE channel_type='direct' AND recipient_run_id = ?1",
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
        "cairn://p/P/1/1/builder/terminal/run-1",
        Some(2),
        Some(125),
        Some("error: boom"),
    );
    assert!(msg.contains("run-1"), "{msg}");
    assert!(msg.contains("exit code 2"), "{msg}");
    assert!(msg.contains("2m05s"), "{msg}");
    assert!(
        msg.contains("cairn://p/P/1/1/builder/terminal/run-1"),
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

#[tokio::test(flavor = "current_thread")]
async fn terminal_exit_wake_fires_once_then_is_consumed() {
    let db = migrated_db().await;
    seed_job(&db).await;
    let orch = test_orchestrator(db);
    // The subscription is keyed on the canonical URI, matching what the route
    // side emits.
    let uri = "cairn://p/P/1/1/builder/terminal/run-1";
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
        "cairn://p/P/9/1/builder/terminal/run-1",
        Some(0),
        Some(3),
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

    let action = route_terminal_exit_async(&orch, "run-1", uri, Some(0), Some(12), Some("ok"))
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
    let again = route_terminal_exit_async(&orch, "run-1", uri, Some(0), None, None)
        .await
        .unwrap();
    assert_eq!(again, WakeRouteAction::Dropped);
}

#[test]
fn terminal_output_message_carries_slug_phrase_uri_and_excerpt() {
    let msg = format_terminal_output_message(
        "dev",
        "cairn://p/P/1/1/builder/terminal/dev",
        "ready",
        Some("VITE ready in 412 ms"),
    );
    assert!(msg.contains("dev"), "{msg}");
    assert!(msg.contains("ready"), "{msg}");
    assert!(
        msg.contains("cairn://p/P/1/1/builder/terminal/dev"),
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
            "SELECT content FROM messages WHERE channel_type='direct' AND recipient_run_id = ?1",
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
    let uri = "cairn://p/P/1/1/builder/terminal/dev";
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
    let other = list_terminal_output_watchers(&db, "cairn://p/P/1/1/builder/terminal/other")
        .await
        .unwrap();
    assert!(other.is_empty());
}

#[test]
fn child_attention_message_with_detail_reads_detail_once() {
    let issue_uri = "cairn://p/P/2";
    let detail_uri = "cairn://p/P/2/1/builder/permissions/perm-2";
    let message = child_attention_message(
        issue_uri,
        "needs_approval",
        "agent_idle_with_work",
        Some(detail_uri),
    );

    assert_eq!(
            message,
            "[Child update] needs_approval/agent_idle_with_work. Read cairn://p/P/2/1/builder/permissions/perm-2."
        );
    assert_eq!(message.matches(issue_uri).count(), 1);
    assert_eq!(message.matches(detail_uri).count(), 1);
}

#[test]
fn child_attention_message_without_detail_reads_issue_once() {
    let issue_uri = "cairn://p/P/2";
    let message = child_attention_message(issue_uri, "needs_input", "question", None);

    assert_eq!(
        message,
        "[Child update] needs_input/question. Read cairn://p/P/2."
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
        Some("cairn://p/P/2"),
        Some(&kinds),
        "agent",
    )
    .await
    .unwrap();
    mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/P/2"),
        Some(&kinds),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    assert!(
        matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "pr_state_change")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "question")
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
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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
    subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
        .await
        .unwrap();
    mute(
        &db,
        "j",
        "issue",
        Some("cairn://p/P/2"),
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
        Some("cairn://p/P/2"),
        Some(&kinds),
        "agent",
    )
    .await
    .unwrap();

    let question = matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "question")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(question.state, WakeSubscriptionState::Active);
    assert_eq!(question.fact_kinds.as_ref().unwrap(), &kinds);

    let pr = matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "pr_state_change")
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
        Some("cairn://p/P/2"),
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
                reference: "cairn://p/P/2".to_string(),
            },
            fact_kind: "pr_state_change".to_string(),
            detail_uri: Some("cairn://p/P/2/1/pr".to_string()),
            delivery: WakeDelivery::Broadcast {
                message: "routine PR update".to_string(),
            },
            urgency: DeliveryUrgency::Queue,
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
                reference: "cairn://p/P/2".to_string(),
            },
            fact_kind: "question".to_string(),
            detail_uri: Some("cairn://p/P/2/1/questions/q".to_string()),
            delivery: WakeDelivery::Targeted {
                subscriber_job_id: sub.job_id.clone(),
                message: "needs answer".to_string(),
            },
            urgency: DeliveryUrgency::Interrupt,
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
            detail_uri: Some("cairn://p/P/1/1/builder".to_string()),
            delivery: WakeDelivery::MessageDigest {
                subscriber_job_id: "j".to_string(),
                content: "passive note".to_string(),
            },
            urgency: DeliveryUrgency::Passive,
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
            detail_uri: Some("cairn://p/P/1/1/builder".to_string()),
            delivery: WakeDelivery::MessageDigest {
                subscriber_job_id: "j".to_string(),
                content: "dropped note".to_string(),
            },
            urgency: DeliveryUrgency::Interrupt,
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
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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

/// Acquisition path 2: a child filed by some *other* job, already parented. The
/// filer is irrelevant — only the parent edge decides.
#[tokio::test(flavor = "current_thread")]
async fn a_child_filed_by_another_job_reaches_the_parents_coordinator() {
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

    assert_eq!(
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
        vec!["j".to_string()],
        "the filing job never enters the child's watcher set"
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

    assert!(watcher_jobs_for_issue(&db, CHILD_URI)
        .await
        .unwrap()
        .is_empty());

    db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES('late','p','i','running','sl',5,5)",
            (),
        )
        .await
        .unwrap();

    assert_eq!(
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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
    assert!(watcher_jobs_for_issue(&db, CHILD_URI)
        .await
        .unwrap()
        .is_empty());
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
        watcher_jobs_for_issue(&db, CHILD_URI)
            .await
            .unwrap()
            .is_empty(),
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
        watcher_jobs_for_issue(&db, CHILD_URI).await.unwrap(),
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

    let matched = matching_subscriptions_for_source(&db, "issue", Some(CHILD_URI), "message")
        .await
        .unwrap();
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].job_id, "j");
    assert_eq!(matched[0].state, WakeSubscriptionState::Active);

    // The derived watch carries exactly the fact vocabulary the seeded default
    // carried, so what the broadcast router reaches is unchanged. Notably
    // `external_message_reply` — the one fact still routed this way — is outside
    // that vocabulary and has always required an explicit broad issue
    // subscription, which the derivation deliberately does not widen.
    for outside in ["terminal_exit", "external_message_reply"] {
        assert!(
            matching_subscriptions_for_source(&db, "issue", Some(CHILD_URI), outside)
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
        Some("cairn://p/P/2"),
        Some(&legacy),
        "agent",
    )
    .await
    .unwrap();

    assert!(
        matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "review")
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
        Some("cairn://p/P/99"),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    assert_eq!(sub.state, WakeSubscriptionState::Muted);
    assert_eq!(sub.source_kind, "issue");
    assert_eq!(sub.source_ref.as_deref(), Some("cairn://p/P/99"));
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
    let peer = matching_subscription(&db, "j", "peer", Some("cairn://p/P/1/1/planner"), "message")
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
        Some("cairn://p/P/1/1/planner"),
        None,
        "system",
    )
    .await
    .unwrap();
    mute(
        &db,
        "j",
        "peer",
        Some("cairn://p/P/1/1/planner"),
        None,
        None,
        None,
        "system",
    )
    .await
    .unwrap();

    let specific =
        matching_subscription(&db, "j", "peer", Some("cairn://p/P/1/1/planner"), "message")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(specific.state, WakeSubscriptionState::Muted);
    assert_eq!(
        specific.source_ref.as_deref(),
        Some("cairn://p/P/1/1/planner")
    );

    let other = matching_subscription(
        &db,
        "j",
        "peer",
        Some("cairn://p/P/1/1/reviewer"),
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
    subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
        .await
        .unwrap();
    subscribe(&db, "j2", "issue", Some("cairn://p/P/2"), None, "agent")
        .await
        .unwrap();
    let routine = vec!["pr_state_change".to_string()];
    mute(
        &db,
        "j2",
        "issue",
        Some("cairn://p/P/2"),
        Some(&routine),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();

    let matches =
        matching_subscriptions_for_source(&db, "issue", Some("cairn://p/P/2"), "pr_state_change")
            .await
            .unwrap();
    assert_eq!(matches.len(), 2);
    let j = matches.iter().find(|sub| sub.job_id == "j").unwrap();
    assert_eq!(j.state, WakeSubscriptionState::Active);
    let j2 = matches.iter().find(|sub| sub.job_id == "j2").unwrap();
    assert_eq!(j2.state, WakeSubscriptionState::Muted);
    assert_eq!(j2.fact_kinds.as_ref().unwrap(), &routine);
}

#[tokio::test(flavor = "current_thread")]
async fn digest_render_names_lifted_scope_and_live_wake() {
    let notice = SuppressedWake {
        id: "n".to_string(),
        subscription_id: Some("s".to_string()),
        job_id: "j".to_string(),
        source_kind: "issue".to_string(),
        source_ref: Some("cairn://p/P/2".to_string()),
        fact_kind: Some("pr_state_change".to_string()),
        occurrences: 3,
        latest_detail_uri: Some("latest".to_string()),
        content: None,
        created_at: 1,
        updated_at: 1,
        delivered_at: None,
    };
    let rendered = SuppressedWake::render_digest_with_context(&[notice], Some(&WakeSource::User));
    assert!(rendered.contains("lifting wake snooze on issue cairn://p/P/2"));
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
            Some("cairn://p/P/2"),
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
        Some("cairn://p/P/2"),
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
            Some("cairn://p/P/2"),
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
            Some("cairn://p/P/2"),
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
            Some("cairn://p/P/2"),
            "review",
            Wake::Passive
        )
        .await
        .unwrap(),
        Wake::Passive
    );
}
