//! A child whose restart-cancelled turn was never resumed must reach the node
//! driving its parent exactly once per episode, and the shapes that look similar
//! from a distance (a resume that took, a job about to resume itself, a node that
//! is over) must not.
//!
//! These seed the row shape directly, which is what lets them cover the failures
//! that leave no trace but that shape. The path through the real reconcile and
//! resume is pinned in `runs::restart_resume_tests`.

use std::collections::HashSet;

use super::child_stall::stalled_children;
use super::child_unresumed::*;
use crate::storage::{LocalDb, RowExt};

const BOOT_AT: i64 = 2_000_000;
const BEFORE_BOOT: i64 = BOOT_AT - 3_600;
const AFTER_BOOT: i64 = BOOT_AT + 60;
const NOW: i64 = BOOT_AT + 7_200;

const CHILD_ISSUE: &str = "cairn://p/proj/2";
const CHILD_NODE: &str = "cairn://p/proj/2/1/builder";
const CHILD_KEY: &str = "child-unresumed:cairn://p/proj/2/1/builder";

async fn migrated_db() -> LocalDb {
    crate::storage::migrated_test_db("child-unresumed.db").await
}

/// A parent issue with one live coordinator, and a child issue with one node
/// holding an open session and no turns yet, plus the `review` task the standard
/// recipe creates up front, `pending` with no turns. Both are the specimen's: the
/// open session and the waiting review must neither enable nor suppress this fact.
async fn seed(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','proj','/tmp/p',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
           VALUES('parent','p',1,'Parent','active','active','none',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at,parent_issue_id)
           VALUES('child','p',2,'Child','active','active','none',2,2,'parent');

         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('parent-exec','r','parent','p','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('parent-job','p','parent','parent-exec','running','coordinator',
                  'parent-session',3,3,3);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('parent-session','parent-job','open',1,3,3);

         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('child-exec','r','child','p','running',4,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('child-job','p','child','child-exec','pending','builder',
                  'child-session',5,5,5);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('child-session','child-job','open',1,5,5);

         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,parent_job_id,
                          current_session_id,created_at,updated_at)
           VALUES('review-job','p','child','child-exec','pending','review','child-job',
                  'review-session',5,5);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('review-session','review-job','open',1,5,5);",
    )
    .await
    .unwrap();
}

/// One turn of `job`. `seq` is both its position and, offset from just before the
/// boot, when it was created, so later seqs read as later turns.
struct Turn<'a> {
    id: &'a str,
    job: &'a str,
    session: &'a str,
    seq: i64,
    state: &'a str,
    start_reason: &'a str,
    end_reason: Option<&'a str>,
    started: bool,
    ended_at: Option<i64>,
    run: Option<&'a str>,
}

async fn add_turn(db: &LocalDb, turn: Turn<'_>) {
    let created_at = BEFORE_BOOT + turn.seq;
    db.execute(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,end_reason,
                           created_at,started_at,ended_at,updated_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?9)",
        (
            turn.id.to_string(),
            turn.session.to_string(),
            turn.run.map(str::to_string),
            turn.job.to_string(),
            turn.seq,
            turn.state.to_string(),
            turn.start_reason.to_string(),
            turn.end_reason.map(str::to_string),
            created_at,
            turn.started.then_some(created_at),
            turn.ended_at,
        ),
    )
    .await
    .unwrap();
}

/// The specimen's turn: queued, runless, cancelled by the restart before it
/// started.
async fn orphan(db: &LocalDb, id: &str, seq: i64, ended_at: i64) {
    add_turn(
        db,
        Turn {
            id,
            job: "child-job",
            session: "child-session",
            seq,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("orphaned_pending"),
            started: false,
            ended_at: Some(ended_at),
            run: None,
        },
    )
    .await;
}

/// A launch the funnel refused after minting its turn.
async fn refused(db: &LocalDb, id: &str, seq: i64) {
    add_turn(
        db,
        Turn {
            id,
            job: "child-job",
            session: "child-session",
            seq,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("launch_refused"),
            started: false,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;
}

/// A later turn in `state`, which is the child having moved on.
async fn later_turn(db: &LocalDb, id: &str, seq: i64, state: &str, started: bool) {
    add_turn(
        db,
        Turn {
            id,
            job: "child-job",
            session: "child-session",
            seq,
            state,
            start_reason: "follow_up",
            end_reason: None,
            started,
            ended_at: (state == "complete").then_some(AFTER_BOOT),
            run: None,
        },
    )
    .await;
}

async fn sweep(db: &LocalDb) -> Vec<String> {
    let sweep = sweep_unresumed_in(db, &HashSet::new())
        .await
        .unwrap();
    sweep
        .told
        .into_iter()
        .flat_map(|(_, recipients)| recipients)
        .collect()
}

async fn standing(db: &LocalDb) -> Vec<UnresumedChild> {
    unresumed_children(db, &HashSet::new())
        .await
        .unwrap()
}

/// `(recipient, key, content_ref, fingerprint, wake)` of every row, delivered or
/// not.
async fn pushes(db: &LocalDb) -> Vec<(String, String, String, String, String)> {
    db.query_all(
        "SELECT recipient, key, content_ref, fingerprint, wake FROM attention_pushes
          ORDER BY recipient, key, created_at",
        (),
        |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                row.opt_text(3)?.unwrap_or_default(),
                row.text(4)?,
            ))
        },
    )
    .await
    .unwrap()
}

async fn queue_message(db: &LocalDb, job: &str) {
    db.execute(
        "INSERT INTO queued_messages(id,job_id,content,delivery,created_at)
         VALUES('msg-' || ?1,?1,'continue','boundary',1)",
        (job.to_string(),),
    )
    .await
    .unwrap();
}

async fn add_wait(db: &LocalDb, job: &str, session: &str, turn: &str) {
    db.execute(
        "INSERT INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
         VALUES('wait-run-' || ?1,'p',?1,?2,'complete',1,1)",
        (job.to_string(), session.to_string()),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,
                                 condition_json,created_at)
         VALUES('wait-' || ?1,?1,'wait-run-' || ?1,?2,?3,'tool-' || ?1,'{}',1)",
        (job.to_string(), session.to_string(), turn.to_string()),
    )
    .await
    .unwrap();
}

async fn mark_delivered(db: &LocalDb) {
    db.execute(
        "UPDATE attention_pushes SET delivered_event_id = 'delivered'",
        (),
    )
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// The fact itself
// ---------------------------------------------------------------------------

/// A refusal before minting leaves only a log line. The head turn is still the
/// orphan, which is exactly what the predicate reads, so this is told like any
/// other, and says plainly that no resume turn exists.
#[tokio::test]
async fn a_refusal_before_minting_is_told_from_the_head_turn_alone() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;

    assert_eq!(sweep(&db).await, vec!["parent-job".to_string()]);
    assert_eq!(
        pushes(&db).await,
        vec![(
            "parent-job".to_string(),
            CHILD_KEY.to_string(),
            CHILD_NODE.to_string(),
            "turn-e".to_string(),
            "wake".to_string(),
        )]
    );

    let rendered = standing(&db).await[0].render(NOW);
    assert!(rendered.contains(CHILD_NODE), "{rendered}");
    assert!(rendered.contains(CHILD_ISSUE), "{rendered}");
    assert!(rendered.contains("turn-e"), "{rendered}");
    assert!(rendered.contains("orphaned_pending"), "{rendered}");
    assert!(rendered.contains("before it started"), "{rendered}");
    assert!(
        rendered.contains("no resume turn was ever minted"),
        "{rendered}"
    );
    assert!(rendered.contains("resumable"), "{rendered}");
    assert!(
        rendered.contains("nothing will resume it automatically"),
        "{rendered}"
    );
}

/// A refused launch does not hide the restart that preceded it. The refusals are
/// counted as evidence and change nothing about whose episode this is, so a later
/// refusal cannot re-wake a parent that has already heard.
#[tokio::test]
async fn refused_launches_are_evidence_and_never_a_new_episode() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    refused(&db, "turn-r1", 3).await;

    assert_eq!(sweep(&db).await, vec!["parent-job".to_string()]);
    let rendered = standing(&db).await[0].render(NOW);
    assert!(rendered.contains("1 refused launch since"), "{rendered}");

    refused(&db, "turn-r2", 4).await;
    assert!(
        sweep(&db).await.is_empty(),
        "the same episode wakes nobody again"
    );
    let child = standing(&db).await.remove(0);
    assert_eq!(child.fingerprint(), "turn-e");
    let rendered = child.render(NOW);
    assert!(rendered.contains("2 refused launches since"), "{rendered}");
    assert_eq!(pushes(&db).await.len(), 1);
}

/// A refused launch from before the episode is no evidence about its resume.
#[tokio::test]
async fn a_refusal_older_than_the_episode_is_not_counted() {
    let db = migrated_db().await;
    seed(&db).await;
    refused(&db, "turn-old", 1).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;

    let child = standing(&db).await.remove(0);
    assert_eq!(child.refusals.count, 0);
}

/// The same end reason covers a turn that was running, and that one's parent needs
/// to know the outcome of its last call is in doubt.
#[tokio::test]
async fn a_started_host_restart_turn_says_it_was_interrupted_mid_flight() {
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(
        "INSERT INTO runs(id,project_id,job_id,session_id,status,exit_reason,created_at,updated_at)
           VALUES('run-1','p','child-job','child-session','crashed','crash',1,1);",
    )
    .await
    .unwrap();
    add_turn(
        &db,
        Turn {
            id: "turn-e",
            job: "child-job",
            session: "child-session",
            seq: 2,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("host_restart"),
            started: true,
            ended_at: Some(AFTER_BOOT),
            run: Some("run-1"),
        },
    )
    .await;
    refused(&db, "turn-r1", 3).await;

    assert_eq!(sweep(&db).await, vec!["parent-job".to_string()]);
    let rendered = standing(&db).await[0].render(NOW);
    assert!(rendered.contains("host_restart"), "{rendered}");
    assert!(rendered.contains("mid-flight"), "{rendered}");
    assert!(rendered.contains("unknown"), "{rendered}");
    assert!(
        rendered.contains("Its run `run-1` reads `crashed` (`crash`)"),
        "{rendered}"
    );
}

/// A `host_restart` turn bound to a run that never began it is the queued shape,
/// and saying a tool call is in doubt would be false.
#[tokio::test]
async fn a_never_started_host_restart_turn_is_described_as_queued() {
    let db = migrated_db().await;
    seed(&db).await;
    add_turn(
        &db,
        Turn {
            id: "turn-e",
            job: "child-job",
            session: "child-session",
            seq: 2,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("host_restart"),
            started: false,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;

    let rendered = standing(&db).await[0].render(NOW);
    assert!(rendered.contains("before it started"), "{rendered}");
    assert!(!rendered.contains("mid-flight"), "{rendered}");
}

/// The specimen's run crashed while its runless successor waited. The turn cannot
/// name that run as its own, but the parent should still see it.
#[tokio::test]
async fn a_runless_turn_points_at_the_nodes_last_run() {
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(
        "INSERT INTO runs(id,project_id,job_id,session_id,status,exit_reason,created_at,updated_at)
           VALUES('run-old','p','child-job','child-session','complete',NULL,1,1),
                 ('run-new','p','child-job','child-session','crashed','crash',2,2);",
    )
    .await
    .unwrap();
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;

    let rendered = standing(&db).await[0].render(NOW);
    assert!(
        rendered
            .contains("never had a run; the node's last run `run-new` reads `crashed` (`crash`)"),
        "{rendered}"
    );
}

/// A memory review runs after a turn ends and is not the work's own state, so it
/// does not move the head.
#[tokio::test]
async fn a_memory_review_does_not_hide_the_episode() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    add_turn(
        &db,
        Turn {
            id: "turn-m",
            job: "child-job",
            session: "child-session",
            seq: 3,
            state: "complete",
            start_reason: "memory_review",
            end_reason: None,
            started: true,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;

    assert_eq!(standing(&db).await[0].fingerprint(), "turn-e");
}

/// Two nodes on one child issue are two episodes. Sharing a key would make each
/// tick's push overwrite the other's fingerprint, and the parent would be woken
/// every thirty seconds for as long as both stood.
#[tokio::test]
async fn two_nodes_on_one_issue_are_told_once_each_and_never_flap() {
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(
        "INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('child-job-2','p','child','child-exec','pending','review',
                  'child-session-2',6,6,6);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('child-session-2','child-job-2','open',1,6,6);",
    )
    .await
    .unwrap();
    orphan(&db, "turn-a", 2, AFTER_BOOT).await;
    add_turn(
        &db,
        Turn {
            id: "turn-b",
            job: "child-job-2",
            session: "child-session-2",
            seq: 2,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("orphaned_pending"),
            started: false,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;

    assert_eq!(sweep(&db).await.len(), 2);
    assert_eq!(pushes(&db).await.len(), 2);
    for _ in 0..3 {
        assert!(sweep(&db).await.is_empty(), "the sweep must not flap");
    }
    assert_eq!(pushes(&db).await.len(), 2);
}

// ---------------------------------------------------------------------------
// Retirement
// ---------------------------------------------------------------------------

/// The resume took: a later turn holds the head. Nothing is told, and that holds
/// for each state a turn can be in on its way there.
#[tokio::test]
async fn a_later_turn_means_nothing_is_owed() {
    for (state, started) in [("pending", false), ("running", true), ("complete", true)] {
        let db = migrated_db().await;
        seed(&db).await;
        orphan(&db, "turn-e", 2, AFTER_BOOT).await;
        later_turn(&db, "turn-next", 3, state, started).await;

        assert!(sweep(&db).await.is_empty(), "{state}");
        assert!(pushes(&db).await.is_empty(), "{state}");
    }
}

/// A successful child turn retires an undelivered fact.
#[tokio::test]
async fn a_later_turn_withdraws_an_undelivered_fact() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    assert_eq!(sweep(&db).await.len(), 1);
    assert_eq!(pushes(&db).await.len(), 1);

    later_turn(&db, "turn-next", 3, "running", true).await;

    let swept = sweep_unresumed_in(&db, &HashSet::new())
        .await
        .unwrap();
    assert_eq!(swept.withdrawn, 1);
    assert!(swept.told.is_empty());
    assert!(pushes(&db).await.is_empty());
}

/// So does terminal resolution of the job or of its issue.
#[tokio::test]
async fn terminal_resolution_withdraws_an_undelivered_fact() {
    for resolve in [
        "UPDATE jobs SET status = 'complete' WHERE id = 'child-job'",
        "UPDATE jobs SET status = 'cancelled' WHERE id = 'child-job'",
        "UPDATE jobs SET status = 'failed' WHERE id = 'child-job'",
        "UPDATE issues SET status = 'merged' WHERE id = 'child'",
        "UPDATE issues SET status = 'closed' WHERE id = 'child'",
        "UPDATE issues SET status = 'failed' WHERE id = 'child'",
    ] {
        let db = migrated_db().await;
        seed(&db).await;
        orphan(&db, "turn-e", 2, AFTER_BOOT).await;
        assert_eq!(sweep(&db).await.len(), 1, "{resolve}");

        db.execute(resolve, ()).await.unwrap();

        assert!(sweep(&db).await.is_empty(), "{resolve}");
        assert!(pushes(&db).await.is_empty(), "{resolve}");
    }
}

/// A delivered row is the record of what the parent read, so it is never deleted,
/// and it is what keeps an unchanged episode from waking anyone again. A new orphan
/// after a real recovery is a new turn and a new wake.
#[tokio::test]
async fn a_delivered_fact_is_kept_and_only_a_new_episode_wakes_again() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    assert_eq!(sweep(&db).await.len(), 1);
    mark_delivered(&db).await;

    assert!(sweep(&db).await.is_empty(), "an unchanged episode is quiet");
    assert_eq!(pushes(&db).await.len(), 1);

    later_turn(&db, "turn-next", 3, "complete", true).await;
    let swept = sweep_unresumed_in(&db, &HashSet::new())
        .await
        .unwrap();
    assert_eq!(swept.withdrawn, 0, "the delivered row stays");
    assert_eq!(pushes(&db).await.len(), 1);

    orphan(&db, "turn-e2", 4, AFTER_BOOT).await;
    assert_eq!(sweep(&db).await, vec!["parent-job".to_string()]);
    let rows = pushes(&db).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row.3 == "turn-e2"), "{rows:?}");
}

// ---------------------------------------------------------------------------
// Shapes that must not fire
// ---------------------------------------------------------------------------

/// The specimen's own shape, and the reason this fact has its own question about
/// motion. A review task waiting for the node to finish cannot resume it.
#[tokio::test]
async fn a_review_task_waiting_on_the_node_does_not_hold_the_fact_back() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    assert_eq!(
        db.query_opt_text(
            "SELECT status FROM jobs WHERE id = 'review-job'",
            (),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("pending")
    );

    assert_eq!(standing(&db).await.len(), 1);
}

/// A queued message or a pending wait, such as an unanswered question, will start
/// the node by itself, and the fact returns if that goes away and it has not.
#[tokio::test]
async fn a_message_or_a_wait_on_the_node_holds_the_fact_back() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;

    queue_message(&db, "child-job").await;
    assert!(standing(&db).await.is_empty(), "a queued message");
    db.execute_script("UPDATE queued_messages SET delivered_at = 1;")
        .await
        .unwrap();
    assert_eq!(standing(&db).await.len(), 1);

    add_wait(&db, "child-job", "child-session", "turn-e").await;
    assert!(standing(&db).await.is_empty(), "a pending wait");
    db.execute_script("UPDATE agent_waits SET state = 'resolved';")
        .await
        .unwrap();
    assert_eq!(standing(&db).await.len(), 1);
}

/// A subscription may never fire and a muted one rouses nothing, so neither is
/// grounds for open-ended silence.
#[tokio::test]
async fn a_wake_subscription_does_not_hold_the_fact_back() {
    for state in ["active", "muted"] {
        let db = migrated_db().await;
        seed(&db).await;
        orphan(&db, "turn-e", 2, AFTER_BOOT).await;
        db.execute(
            "INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,state,created_by,
                                            created_at,updated_at)
             VALUES('sub','child-job','issue','cairn://p/proj/9',?1,'agent',1,1)",
            (state.to_string(),),
        )
        .await
        .unwrap();

        assert_eq!(standing(&db).await.len(), 1, "{state}");
    }
}

/// A sub-task in motion is real work the node is waiting on, whichever way it is
/// moving, and the fact would be false.
#[tokio::test]
async fn a_sub_task_in_motion_holds_the_fact_back() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    assert_eq!(standing(&db).await.len(), 1, "control: not moving yet");

    let claimed: HashSet<String> = ["review-job".to_string()].into();
    assert!(
        unresumed_children(&db, &claimed).await.unwrap().is_empty(),
        "an in-flight launch"
    );

    db.execute_script(
        "INSERT INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
           VALUES('run-t','p','review-job','review-session','live',1,1);",
    )
    .await
    .unwrap();
    assert!(standing(&db).await.is_empty(), "a live run");
    db.execute_script("UPDATE runs SET status = 'complete' WHERE id = 'run-t';")
        .await
        .unwrap();
    assert_eq!(standing(&db).await.len(), 1);

    queue_message(&db, "review-job").await;
    assert!(standing(&db).await.is_empty(), "a queued message");
    db.execute_script("UPDATE queued_messages SET delivered_at = 1;")
        .await
        .unwrap();

    add_turn(
        &db,
        Turn {
            id: "turn-t",
            job: "review-job",
            session: "review-session",
            seq: 3,
            state: "pending",
            start_reason: "initial",
            end_reason: None,
            started: false,
            ended_at: None,
            run: None,
        },
    )
    .await;
    assert!(standing(&db).await.is_empty(), "a turn that has not ended");
}

/// A task blocked on its own sub-task holds no run, so the walk has to reach the
/// one that does.
#[tokio::test]
async fn a_nested_task_in_motion_holds_the_fact_back() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    db.execute_script(
        "INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,parent_job_id,
                          current_session_id,created_at,updated_at)
           VALUES('deep-job','p','child','child-exec','running','deep','review-job',
                  'deep-session',6,6);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('deep-session','deep-job','open',1,6,6);
         INSERT INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
           VALUES('run-d','p','deep-job','deep-session','live',1,1);",
    )
    .await
    .unwrap();

    assert!(standing(&db).await.is_empty());
}

/// A shared cause can refuse both the node's resume and its task's. The task is
/// excluded from having a fact of its own, so if it held the node's fact back
/// nobody would hear about either.
#[tokio::test]
async fn a_sub_task_the_restart_stranded_does_not_hold_the_fact_back() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    add_turn(
        &db,
        Turn {
            id: "turn-t",
            job: "review-job",
            session: "review-session",
            seq: 2,
            state: "cancelled",
            start_reason: "initial",
            end_reason: Some("orphaned_pending"),
            started: false,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;

    assert_eq!(standing(&db).await.len(), 1);
}

#[tokio::test]
async fn a_live_run_or_an_in_flight_launch_holds_the_fact_back() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;

    let claimed: HashSet<String> = ["child-job".to_string()].into();
    assert!(unresumed_children(&db, &claimed)
        .await
        .unwrap()
        .is_empty());

    for status in ["starting", "live"] {
        db.execute(
            "INSERT OR REPLACE INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
             VALUES('run-1','p','child-job','child-session',?1,1,1)",
            (status.to_string(),),
        )
        .await
        .unwrap();
        assert!(standing(&db).await.is_empty(), "{status}");
    }
}

#[tokio::test]
async fn a_top_level_issue_has_no_parent_to_tell() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    db.execute(
        "UPDATE issues SET parent_issue_id = NULL WHERE id = 'child'",
        (),
    )
    .await
    .unwrap();

    assert!(standing(&db).await.is_empty());
}

/// A sub-agent task is its parent node's problem, and routing it to the
/// grandparent issue's coordinator would address someone with no standing.
#[tokio::test]
async fn a_sub_agent_task_is_not_a_child_node() {
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(
        "INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,parent_job_id,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('task-job','p','child','child-exec','pending','task','child-job',
                  'task-session',6,6,6);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('task-session','task-job','open',1,6,6);",
    )
    .await
    .unwrap();
    add_turn(
        &db,
        Turn {
            id: "turn-t",
            job: "task-job",
            session: "task-session",
            seq: 2,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("orphaned_pending"),
            started: false,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;

    assert!(standing(&db).await.is_empty());
}

/// A re-run of the child issue replaced this work, and its old job can sit Pending
/// indefinitely. Telling the parent it needs attention would be false.
#[tokio::test]
async fn a_superseded_execution_is_not_reported() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    db.execute(
        "UPDATE executions SET lifecycle = 'superseded' WHERE id = 'child-exec'",
        (),
    )
    .await
    .unwrap();

    assert!(standing(&db).await.is_empty());
}

/// Any other reason a turn ended is not this fact's.
#[tokio::test]
async fn a_turn_ended_some_other_way_is_not_an_episode() {
    let db = migrated_db().await;
    seed(&db).await;
    add_turn(
        &db,
        Turn {
            id: "turn-e",
            job: "child-job",
            session: "child-session",
            seq: 2,
            state: "cancelled",
            start_reason: "follow_up",
            end_reason: Some("user_stop"),
            started: true,
            ended_at: Some(AFTER_BOOT),
            run: None,
        },
    )
    .await;

    assert!(standing(&db).await.is_empty());
}

// ---------------------------------------------------------------------------
// Recipients
// ---------------------------------------------------------------------------

/// A parent that muted the child still gets the fact, but as a passive ride-along
/// that rouses nobody.
#[tokio::test]
async fn a_parent_that_muted_the_child_gets_a_passive_ride_along() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    db.execute(
        "INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,state,created_by,
                                        created_at,updated_at)
         VALUES('mute','parent-job','issue',?1,'muted','agent',1,1)",
        (CHILD_ISSUE.to_string(),),
    )
    .await
    .unwrap();

    assert!(sweep(&db).await.is_empty(), "no recipient is roused");
    let rows = pushes(&db).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].4, "passive");
}

/// The CAIRN-3293 invariant, on this fact: the recipient is the node currently
/// driving the parent issue, derived live.
#[tokio::test]
async fn the_fact_follows_the_parent_issues_current_coordinator() {
    let db = migrated_db().await;
    seed(&db).await;
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    db.execute_script(
        "UPDATE executions SET lifecycle='superseded' WHERE id='parent-exec';
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,lifecycle)
           VALUES('parent-exec-2','r','parent','p','running',100,2,'canonical');
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('parent-job-2','p','parent','parent-exec-2','running','coordinator',
                  'parent-session-2',100,100,100);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('parent-session-2','parent-job-2','open',1,100,100);",
    )
    .await
    .unwrap();

    assert_eq!(sweep(&db).await, vec!["parent-job-2".to_string()]);
}

// ---------------------------------------------------------------------------
// Separation from provider-silence stalls
// ---------------------------------------------------------------------------

/// Neither fact is the other's. A recovery-death stall with no restart-cancelled
/// head is not a candidate here, the specimen is not a stall, and the two keep
/// their own rows when a node happens to be both.
#[tokio::test]
async fn this_fact_and_the_stalled_child_fact_do_not_cross() {
    const REASON: &str = "provider_silence_already_terminal_reconciled";
    let recovery_death = format!(
        "INSERT INTO sessions(id,job_id,status,sequence,terminal_reason,created_at,updated_at,closed_at)
           VALUES('dead','child-job','closed',2,'{REASON}',6,6,{BEFORE_BOOT});
         UPDATE jobs SET current_session_id = 'dead' WHERE id = 'child-job';"
    );

    // The stalled-child fact reads a pending review task as a resume trigger, which
    // would make every assertion about it here vacuous (cairn/6124).
    let without_review = "DELETE FROM sessions WHERE id = 'review-session';
                          DELETE FROM jobs WHERE id = 'review-job';";

    // A stall alone: a stalled-child candidate, and not one here.
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(without_review).await.unwrap();
    db.execute_script(&recovery_death).await.unwrap();
    assert_eq!(stalled_children(&db, NOW).await.unwrap().len(), 1);
    assert!(standing(&db).await.is_empty());

    // The specimen alone: the reverse, open session and all.
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(without_review).await.unwrap();
    orphan(&db, "turn-e", 2, AFTER_BOOT).await;
    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
    assert_eq!(standing(&db).await.len(), 1);

    // Both at once: two facts, two rows, neither overwriting the other.
    db.execute_script(&recovery_death).await.unwrap();
    let stalled = stalled_children(&db, NOW).await.unwrap();
    assert_eq!(stalled.len(), 1);
    super::child_stall::tell_watchers(&db, &stalled[0])
        .await
        .unwrap();
    assert_eq!(sweep(&db).await.len(), 1);
    let rows = pushes(&db).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().any(|row| row.1.starts_with("child-stalled:")));
    assert!(rows.iter().any(|row| row.1 == CHILD_KEY));
}
