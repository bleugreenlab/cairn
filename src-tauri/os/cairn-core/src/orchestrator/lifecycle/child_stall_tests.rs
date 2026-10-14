//! CAIRN-4609: a child that still claims to be working while holding no session
//! that could do it must reach the node currently driving its parent, promptly,
//! once — and the four healthy shapes that look similar from a distance must not.
//!
//! The fixtures are CAIRN-4602's own rows. Its builder's five sessions, their
//! terminal reasons, and the instants they closed at are transcribed literally
//! from the incident, so the states under test are states that actually happened
//! rather than states that seemed plausible afterwards. That matters most for
//! the cases where the answer is "do not fire": the incident's own final state
//! is one of them.

use super::child_stall::*;
use crate::storage::{LocalDb, RowExt};

/// The session terminal reason watchdog recovery writes. Spelled out here rather
/// than imported so a rename of the constant has to face this test.
const REASON: &str = "provider_silence_already_terminal_reconciled";

// CAIRN-4602's builder, transcribed. Sessions 1-3 died to provider silence;
// sessions 4 and 5 were minted and left open; the job failed as 5 was created.
const DEATH_1: i64 = 1_787_617_320;
const DEATH_2: i64 = 1_787_678_730;
const DEATH_3: i64 = 1_787_691_963;
const SESSION_4_BORN: i64 = 1_787_696_874;
const SESSION_5_BORN: i64 = 1_787_697_655;
/// A few minutes after the last thing that happened in the incident.
const NOW: i64 = 1_787_700_000;

const CHILD_ISSUE: &str = "cairn://p/proj/2";
const CHILD_NODE: &str = "cairn://p/proj/2/1/builder";

async fn migrated_db() -> LocalDb {
    crate::storage::migrated_test_db("child-stall.db").await
}

/// A parent issue with one live coordinator, and a child issue with one node
/// that has no sessions yet.
///
/// Every fixture below differs from this only in the child node's job status and
/// its session history — which is the point, since telling those apart is the
/// whole of what the predicate does.
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
                          created_at,updated_at,started_at)
           VALUES('child-job','p','child','child-exec','running','builder',5,5,5);",
    )
    .await
    .unwrap();
}

/// Add one session to the child node.
async fn add_session(
    db: &LocalDb,
    id: &str,
    sequence: i64,
    status: &str,
    reason: Option<&str>,
    created_at: i64,
    closed_at: Option<i64>,
) {
    db.execute(
        "INSERT INTO sessions(id,job_id,status,sequence,terminal_reason,
                              created_at,updated_at,closed_at)
         VALUES(?1,'child-job',?2,?3,?4,?5,?5,?6)",
        (
            id.to_string(),
            status.to_string(),
            sequence,
            reason.map(str::to_string),
            created_at,
            closed_at,
        ),
    )
    .await
    .unwrap();
}

/// Record that `id` was rotated out in favour of `successor`.
///
/// A separate step from [`add_session`] because `sessions.replaced_by_id` is a
/// foreign key back into `sessions`: a chain can only ever be linked backwards,
/// once the successor exists. Which is also why a real rotation leaves a window
/// where the predecessor is open and unreplaced.
async fn replaced_by(db: &LocalDb, id: &str, successor: &str) {
    db.execute(
        "UPDATE sessions SET replaced_by_id = ?1 WHERE id = ?2",
        (successor.to_string(), id.to_string()),
    )
    .await
    .unwrap();
}

async fn set_child_job(db: &LocalDb, status: &str, current_session: Option<&str>) {
    db.execute(
        "UPDATE jobs SET status = ?1, current_session_id = ?2 WHERE id = 'child-job'",
        (status.to_string(), current_session.map(str::to_string)),
    )
    .await
    .unwrap();
}

/// The incident's first shape: three recovery deaths, the last of them still the
/// node's current session, and a job that goes on reading as running.
async fn seed_shape_a(db: &LocalDb) {
    seed(db).await;
    add_session(db, "s1", 1, "closed", Some(REASON), 1, Some(DEATH_1)).await;
    add_session(
        db,
        "s2",
        2,
        "closed",
        Some(REASON),
        DEATH_1 + 1,
        Some(DEATH_2),
    )
    .await;
    add_session(
        db,
        "s3",
        3,
        "closed",
        Some(REASON),
        DEATH_2 + 1,
        Some(DEATH_3),
    )
    .await;
    replaced_by(db, "s1", "s2").await;
    replaced_by(db, "s2", "s3").await;
    set_child_job(db, "running", Some("s3")).await;
}

/// The incident's final shape: the same three deaths, then two sessions that
/// were minted, left `open` with no terminal reason, and outlived the job.
async fn seed_shape_b(db: &LocalDb) {
    seed_shape_a(db).await;
    add_session(db, "s4", 4, "open", None, SESSION_4_BORN, None).await;
    add_session(db, "s5", 5, "open", None, SESSION_5_BORN, None).await;
    replaced_by(db, "s3", "s4").await;
    replaced_by(db, "s4", "s5").await;
    set_child_job(db, "failed", Some("s5")).await;
}

/// Observe the stall and tell its watchers, as one sweep tick would. Returns the
/// recipients this tick actually roused, so an empty vector is the dedupe
/// holding.
async fn notify(db: &LocalDb) -> Vec<String> {
    let stalled = stalled_children(db, NOW).await.unwrap();
    tell_watchers(db, &stalled[0]).await.unwrap()
}

async fn undelivered_pushes(db: &LocalDb) -> Vec<(String, String, String, String)> {
    db.query_all(
        "SELECT recipient, key, content_ref, fingerprint FROM attention_pushes
          WHERE delivered_event_id IS NULL ORDER BY recipient",
        (),
        |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                row.opt_text(3)?.unwrap_or_default(),
            ))
        },
    )
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------
// The viability predicate, on its own
// ---------------------------------------------------------------------------

fn session(status: &str, replaced_by: Option<&str>) -> CurrentSession {
    CurrentSession {
        id: "s".into(),
        status: status.into(),
        replaced_by_id: replaced_by.map(str::to_string),
        created_at: 1,
    }
}

/// The one arrangement that is viable, stated positively so the negatives below
/// have something to be negative about.
#[test]
fn an_open_unreplaced_session_is_viable() {
    assert_eq!(classify(Some(&session("open", None))), None);
}

/// The correction that makes this predicate different from the obvious one: an
/// `open` row is evidence that a row was written, not authority that anything
/// can resume through it. CAIRN-4602 ended with two such rows and nothing alive.
#[test]
fn an_open_session_row_is_not_authority_that_the_node_can_work() {
    assert_eq!(
        classify(Some(&session("open", Some("successor")))),
        Some(StallCause::SupersededSessionRetained {
            session_id: "s".into()
        }),
        "a session something else has already replaced cannot carry a turn"
    );
}

#[test]
fn a_terminal_or_absent_session_leaves_the_node_with_nothing() {
    assert_eq!(
        classify(Some(&session("closed", None))),
        Some(StallCause::NoSurvivingSession)
    );
    assert_eq!(
        classify(Some(&session("failed", None))),
        Some(StallCause::NoSurvivingSession)
    );
    assert_eq!(classify(None), Some(StallCause::NoSurvivingSession));
}

// ---------------------------------------------------------------------------
// Acceptance 1 and 5 — the seventeen-hour shape reaches the parent, saying what
// happened
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_child_whose_recovery_terminalized_its_session_reaches_the_parent_driver() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;

    let stalled = stalled_children(&db, NOW).await.unwrap();
    assert_eq!(stalled.len(), 1, "{stalled:#?}");
    assert_eq!(stalled[0].job_id, "child-job");
    assert_eq!(stalled[0].node_uri, CHILD_NODE);
    assert_eq!(stalled[0].cause, StallCause::NoSurvivingSession);
    assert_eq!(stalled[0].deaths.len(), 3);

    let recipients = tell_watchers(&db, &stalled[0]).await.unwrap();
    assert_eq!(
        recipients,
        vec!["parent-job".to_string()],
        "the notice goes to the node currently driving the parent issue"
    );
    assert_eq!(
        undelivered_pushes(&db).await,
        vec![(
            "parent-job".to_string(),
            format!("child-stalled:{CHILD_ISSUE}"),
            CHILD_NODE.to_string(),
            "no-surviving-session:3:1787691963".to_string(),
        )],
        "one rousing push, keyed by the child issue, pointing at the child node"
    );
}

/// Acceptance 5. A URI alone would send the parent to read a node without
/// telling it why, so the fact carries its own evidence.
#[tokio::test]
async fn the_rendered_fact_names_the_node_the_cause_and_the_repeated_deaths() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;
    let stalled = stalled_children(&db, NOW).await.unwrap();

    let rendered = stalled[0].render(NOW);

    assert!(rendered.contains(CHILD_NODE), "{rendered}");
    assert!(rendered.contains(CHILD_ISSUE), "{rendered}");
    assert!(rendered.contains("has no viable session"), "{rendered}");
    assert!(rendered.contains(REASON), "the lifecycle cause: {rendered}");
    assert!(
        rendered.contains("Session 3 ended 2h 13m ago"),
        "the latest session transition, by sequence and age: {rendered}"
    );
    assert!(
        rendered.contains("3 of its sessions have ended that way over 20h 44m"),
        "the repeated-death count and the window it spans: {rendered}"
    );
    assert!(
        rendered.contains("does not spend another"),
        "and it says plainly that no session is being minted on its behalf: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// The side-channel correction — the hard-failure shape, and telling the two apart
// ---------------------------------------------------------------------------

/// CAIRN-4602's final state, and the boundary of this fact.
///
/// The job hard-failed while two of its sessions still read `open` with NULL
/// terminal reasons. That is a genuine bookkeeping lie (CAIRN-4613), and the
/// viability predicate is built so it cannot be mistaken for health — but the
/// node is over, and a node that is over is reported by its own resolution. The
/// alternative is worse than silence: the newest thing that happened to this job
/// is its own failure, and firing here would name a recovery death from hours
/// earlier as the cause of a death it did not cause.
///
/// The same reasoning is what stops a child that recovered months ago from being
/// relabelled the day it finally fails.
#[tokio::test]
async fn a_child_that_hard_failed_is_left_to_its_own_resolution() {
    let db = migrated_db().await;
    seed_shape_b(&db).await;

    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
}

/// The other half of the same boundary, without the hard failure: a node that
/// lost a session, got a healthy one, and is working again is not stalled — and
/// once that healthy session ends for some unrelated reason, the old recovery
/// death must not be resurrected as its explanation.
#[tokio::test]
async fn a_recovered_child_is_not_relabelled_by_its_old_recovery_death() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;
    add_session(&db, "s4", 4, "open", None, SESSION_4_BORN, None).await;
    replaced_by(&db, "s3", "s4").await;
    set_child_job(&db, "running", Some("s4")).await;

    assert!(
        stalled_children(&db, NOW).await.unwrap().is_empty(),
        "recovery worked; the deaths behind it are history"
    );

    // Much later, that healthy session ends for a reason of its own.
    db.execute(
        "UPDATE sessions SET status='closed', terminal_reason='node_removed', closed_at=?1
          WHERE id='s4'",
        (NOW - 3600,),
    )
    .await
    .unwrap();

    assert!(
        stalled_children(&db, NOW).await.unwrap().is_empty(),
        "the node's latest word about itself is `node_removed`, not a provider-silence death"
    );
}

/// Recovery does not only fail — it also DECLINES, on purpose, when the job has
/// something that will resume it (`watchdog_recovery::has_pending_resume_trigger`,
/// which this asks rather than re-implements). Such a node is session-less by
/// design, and telling its parent that recovery had exhausted its attempts would
/// be false twice over.
#[tokio::test]
async fn a_child_waiting_on_its_own_pending_work_is_not_stalled() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;

    assert_eq!(
        stalled_children(&db, NOW).await.unwrap().len(),
        1,
        "the same fixture without the wait IS a stall"
    );

    db.execute_script(
        "INSERT INTO runs(id,issue_id,project_id,job_id,status,session_id,created_at,updated_at)
           VALUES('child-run','child','p','child-job','live','s3',6,6);
         INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
           VALUES('child-turn','s3','child-run','child-job',1,'complete','initial',6,6);
         INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,
                                 condition_json,state,created_at)
           VALUES('wait','child-job','child-run','s3','child-turn','tu','{}','pending',6);",
    )
    .await
    .unwrap();

    assert!(
        stalled_children(&db, NOW).await.unwrap().is_empty(),
        "an intentional wait is not a stall, however long it lasts"
    );
}

/// The way an open row lies that this fact DOES fire on: the node's pointer is
/// left on a session something else has already replaced, and the replacement
/// died to provider silence during that pointer's life.
#[tokio::test]
async fn a_current_session_that_was_already_replaced_is_not_viable() {
    let db = migrated_db().await;
    seed(&db).await;
    add_session(&db, "s1", 1, "open", None, 1, None).await;
    add_session(&db, "s2", 2, "closed", Some(REASON), 2, Some(DEATH_1)).await;
    replaced_by(&db, "s1", "s2").await;
    set_child_job(&db, "running", Some("s1")).await;

    let stalled = stalled_children(&db, NOW).await.unwrap();
    assert_eq!(
        stalled[0].cause,
        StallCause::SupersededSessionRetained {
            session_id: "s1".into()
        }
    );
    assert!(
        stalled[0]
            .render(NOW)
            .contains("still reads `open`, but another session has already replaced it"),
        "and it renders as its own cause, not as the all-sessions-terminal one"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 2 — more deaths update the same condition, and never unbounded
// ---------------------------------------------------------------------------

/// The sweep re-observes a stall on every tick for as long as it lasts. What the
/// parent must see is one notice per change, not one per tick.
#[tokio::test]
async fn repeated_deaths_update_one_notice_instead_of_accumulating_wakes() {
    let db = migrated_db().await;
    seed(&db).await;
    add_session(&db, "s1", 1, "closed", Some(REASON), 1, Some(DEATH_1)).await;
    set_child_job(&db, "running", Some("s1")).await;

    assert_eq!(notify(&db).await, vec!["parent-job".to_string()]);
    assert!(
        notify(&db).await.is_empty(),
        "an unchanged stall re-observed is not a second wake"
    );
    assert!(notify(&db).await.is_empty());

    // The second short-lived successor dies.
    add_session(
        &db,
        "s2",
        2,
        "closed",
        Some(REASON),
        DEATH_1 + 1,
        Some(DEATH_2),
    )
    .await;
    replaced_by(&db, "s1", "s2").await;
    set_child_job(&db, "running", Some("s2")).await;
    assert_eq!(
        notify(&db).await,
        vec!["parent-job".to_string()],
        "a new death is new information and updates the condition"
    );

    // And the third.
    add_session(
        &db,
        "s3",
        3,
        "closed",
        Some(REASON),
        DEATH_2 + 1,
        Some(DEATH_3),
    )
    .await;
    replaced_by(&db, "s2", "s3").await;
    set_child_job(&db, "running", Some("s3")).await;
    assert_eq!(notify(&db).await, vec!["parent-job".to_string()]);

    let pushes = undelivered_pushes(&db).await;
    assert_eq!(
        pushes.len(),
        1,
        "three deaths and five sweeps leave ONE pending notice, not five: {pushes:#?}"
    );
    assert_eq!(pushes[0].3, "no-surviving-session:3:1787691963");
}

/// Observation, never authorization. The parent is told; nothing here reaches
/// for the child's own lifecycle.
#[tokio::test]
async fn telling_the_parent_does_not_touch_the_child() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;
    let before = db
        .query_one(
            "SELECT (SELECT COUNT(*) FROM sessions) || '/' || (SELECT COUNT(*) FROM runs)
             || '/' || (SELECT status || COALESCE(current_session_id,'') FROM jobs WHERE id='child-job')",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();

    let stalled = stalled_children(&db, NOW).await.unwrap();
    tell_watchers(&db, &stalled[0]).await.unwrap();

    let after = db
        .query_one(
            "SELECT (SELECT COUNT(*) FROM sessions) || '/' || (SELECT COUNT(*) FROM runs)
             || '/' || (SELECT status || COALESCE(current_session_id,'') FROM jobs WHERE id='child-job')",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "no session minted, no run started, job untouched"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 3 — the healthy shapes that look similar from a distance
// ---------------------------------------------------------------------------

/// The four quiet-but-fine shapes. Each has been silent for as long as the
/// incident's worst gap; none of them has lost a session, which is the whole
/// difference and the reason the predicate is built on lifecycle evidence rather
/// than on elapsed silence.
#[tokio::test]
async fn healthy_quiet_nodes_do_not_trigger() {
    for (label, job_status, session) in [
        ("a warm idle node", "idle", Some("live")),
        ("a node waiting on the user", "blocked", Some("live")),
        ("a node queued for an executor", "pending", None),
        ("a node in the middle of a turn", "running", Some("live")),
    ] {
        let db = migrated_db().await;
        seed(&db).await;
        if session.is_some() {
            add_session(&db, "live", 1, "open", None, 1, None).await;
        }
        set_child_job(&db, job_status, session).await;

        assert!(
            stalled_children(&db, NOW).await.unwrap().is_empty(),
            "{label} is not a stall"
        );
    }
}

/// Recovery is allowed to work. A death inside the grace window is an ordinary
/// provider-silence recovery in progress, and reporting those would make the
/// fact mean nothing.
#[tokio::test]
async fn a_death_inside_the_recovery_grace_window_is_left_alone() {
    let db = migrated_db().await;
    seed(&db).await;
    add_session(&db, "s1", 1, "closed", Some(REASON), 1, Some(NOW - 60)).await;
    set_child_job(&db, "running", Some("s1")).await;

    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
    assert_eq!(
        stalled_children(&db, NOW + 600).await.unwrap().len(),
        1,
        "and it is reported once the grace has passed without a successor"
    );
}

/// A session closed for a reason that is not a recovery death is not evidence of
/// one. Closing an issue closes its sessions, and that must not read as a stall.
#[tokio::test]
async fn an_ordinarily_closed_session_is_not_a_recovery_death() {
    let db = migrated_db().await;
    seed(&db).await;
    add_session(
        &db,
        "s1",
        1,
        "closed",
        Some("issue_closed"),
        1,
        Some(DEATH_1),
    )
    .await;
    set_child_job(&db, "running", Some("s1")).await;

    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
}

/// A resolved child needs no rescuing, whatever its sessions look like.
#[tokio::test]
async fn a_child_whose_issue_has_resolved_is_not_reported() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;
    db.execute("UPDATE issues SET status = 'merged' WHERE id = 'child'", ())
        .await
        .unwrap();

    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
}

/// An issue with no parent has no parent driver to tell. Its stall is the
/// operator's to see on the issue itself.
#[tokio::test]
async fn a_root_issue_has_no_parent_to_wake() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;
    db.execute(
        "UPDATE issues SET parent_issue_id = NULL WHERE id = 'child'",
        (),
    )
    .await
    .unwrap();

    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
}

/// A sub-agent task that loses its session is its parent NODE's problem, felt as
/// a task that never returns. Routing it to the grandparent issue's coordinator
/// would address it to someone with no standing to act on it.
#[tokio::test]
async fn a_sub_agent_task_is_not_routed_to_the_grandparent() {
    let db = migrated_db().await;
    seed(&db).await;
    db.execute_script(
        "INSERT INTO jobs(id,project_id,issue_id,execution_id,parent_job_id,status,uri_segment,
                          current_session_id,created_at,updated_at)
           VALUES('task-job','p','child','child-exec','child-job','running','review','t1',6,6);
         INSERT INTO sessions(id,job_id,status,sequence,terminal_reason,created_at,updated_at,closed_at)
           VALUES('t1','task-job','closed','1','provider_silence_already_terminal_reconciled',6,6,1787617320);",
    )
    .await
    .unwrap();

    assert!(stalled_children(&db, NOW).await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Acceptance 4 — parent succession
// ---------------------------------------------------------------------------

/// The CAIRN-3293 invariant, on this fact: the recipient is the node currently
/// driving the parent issue, derived live, never a snapshot of whoever drove it
/// when the child was filed.
#[tokio::test]
async fn the_notice_follows_the_parent_issues_current_coordinator() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;
    db.execute_script(
        "UPDATE executions SET lifecycle='superseded' WHERE id='parent-exec';
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq, lifecycle)
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

    let stalled = stalled_children(&db, NOW).await.unwrap();
    assert_eq!(
        tell_watchers(&db, &stalled[0]).await.unwrap(),
        vec!["parent-job-2".to_string()],
        "the superseded coordinator does not hear it; the current one does"
    );
}

/// A node cannot act on its own death, and a sibling on the same issue is no
/// better placed. The child's own jobs are excluded from its watchers.
#[tokio::test]
async fn the_childs_own_nodes_are_never_told_about_the_child() {
    let db = migrated_db().await;
    seed_shape_a(&db).await;

    // Detected first: a subscription of the child's own is itself a pending
    // resume trigger, so adding it before detection would exclude the child for
    // an unrelated reason and prove nothing about the recipient set.
    let stalled = stalled_children(&db, NOW).await.unwrap();
    db.execute_script(
        "INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,state,created_by,
                                        created_at,updated_at)
           VALUES('sub','child-job','issue','cairn://p/proj/2','active','agent',1,1);",
    )
    .await
    .unwrap();

    let recipients = tell_watchers(&db, &stalled[0]).await.unwrap();

    assert_eq!(recipients, vec!["parent-job".to_string()]);
    assert!(
        undelivered_pushes(&db)
            .await
            .iter()
            .all(|(recipient, ..)| recipient != "child-job"),
        "not even an explicit subscription puts the stall in the stalled node's own queue"
    );
}
