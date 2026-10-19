//! CAIRN-2657: the user-facing `continue_job` entry point must queue a
//! message-bearing send when the job's head turn is already active, rather than
//! trip the active-turn guard and return an error the composer surfaces as a 500
//! (which also drops the typed text).
//!
//! Every message-bearing send is now acknowledged at its durable
//! `queued_messages` row and delivered by a background nudge, whatever the head
//! turn's state. These pin the acknowledgement — a `Queue` row and no new turn —
//! and, separately, what a delivery attempt that cannot launch leaves behind,
//! without needing a real worktree/process.

use crate::common;
use cairn_core::internal::execution::jobs::{
    continue_job_or_enqueue, continue_job_or_enqueue_with_before_resume_for_test,
};
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::messages::delivery::nudge_job_for_urgency;
use cairn_core::messages::queued::{self, Delivery};
use cairn_db::turso::params;

/// Seed a project/issue/execution/job/session/run whose head turn sits in
/// `turn_state` (`running`/`pending` exercise the queue path). Mirrors the
/// minimal shape `insert_dm_recipient` builds in `queued_direct_messages.rs`.
async fn seed_job(db: &LocalDb, turn_state: &str) {
    let turn_state = turn_state.to_string();
    db.write(|conn| {
        let turn_state = turn_state.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('proj-1', 'default', 'Test Project', 'proj', '/tmp/test-repo', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                 VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 'session-1', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'open', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', 'proj-1', 'job-1', NULL, 'live', 'session-1', 1, 1, 'resume')",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, ?1, 1, 1)",
                params![turn_state.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn settling_boundary_queues_instead_of_leaving_a_delivered_orphan() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "complete").await;
    let db = orch.db.local.clone();

    continue_job_or_enqueue_with_before_resume_for_test(
        &orch,
        "job-1",
        Some("boundary message"),
        "boundary-request",
        move || {
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(
                        db.execute("UPDATE turns SET state = 'running' WHERE id = 'turn-1'", ()),
                    )
                    .unwrap();
            })
            .join()
            .unwrap();
        },
    )
    .expect("a turn going live before the nudge leaves the send queued");
    continue_job_or_enqueue_with_before_resume_for_test(
        &orch,
        "job-1",
        Some("boundary message"),
        "boundary-request",
        || {},
    )
    .expect("retrying the raced operation must return the original queue row");

    assert_eq!(
        queued_rows(&orch.db.local, "job-1").await,
        vec![("boundary message".to_string(), "queue".to_string())],
        "the same operation id must produce exactly one pending row"
    );
    assert_eq!(
        recorded_rows(&orch.db.local, "job-1").await,
        vec![("boundary message".to_string(), false)],
        "the raced message remains claimable rather than stamped delivered"
    );
}

/// COUNT(*) FROM turns for the job. The queue path must not create a successor
/// turn — only the pre-existing one (sequence 1) should remain.
async fn turn_count(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM turns WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

/// (content, delivery) of every queued_messages row for the job, oldest first.
async fn queued_rows(db: &LocalDb, job_id: &str) -> Vec<(String, String)> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT content, delivery FROM queued_messages
                     WHERE job_id = ?1 ORDER BY created_at ASC, rowid ASC",
                    params![job_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

/// (content, delivered) of every queued_messages row for the job, oldest first.
async fn recorded_rows(db: &LocalDb, job_id: &str) -> Vec<(String, bool)> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT content, delivered_at FROM queued_messages
                     WHERE job_id = ?1 ORDER BY created_at ASC, rowid ASC",
                    params![job_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.opt_i64(1)?.is_some()));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

/// (key, wake) of every catch-up push addressed to `recipient`.
async fn catchup_pushes(db: &LocalDb, recipient: &str) -> Vec<(String, String)> {
    let recipient = recipient.to_string();
    db.read(|conn| {
        let recipient = recipient.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT key, wake FROM attention_pushes
                     WHERE recipient = ?1 AND key LIKE 'catchup:%' ORDER BY created_at ASC",
                    params![recipient.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

/// Parent the seeded issue under a new issue driven by a coordinator job, so the
/// child has a derived watcher.
async fn seed_coordinating_parent(db: &LocalDb) {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                 VALUES ('parent-issue', 'proj-1', 41, 'parent issue', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE issues SET parent_issue_id = 'parent-issue' WHERE id = 'issue-1'",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, project_id, issue_id, status, current_session_id, created_at, updated_at)
                 VALUES ('coordinator', 'proj-1', 'parent-issue', 'running', 'session-c', 1, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// CAIRN-3342: the desktop composer is where operator→child messages actually
/// enter the system, and until this it produced nothing for the jobs watching
/// the child — so the person with the most authority to change a child's scope
/// was the one participant a coordinator never heard from. A queued send now
/// leaves the coordinator a passive catch-up row, and only a passive one: an
/// operator conversation must not cost a coordinator turn per message.
///
/// This pins the composer seam — that the push is created at all, which is the
/// half that was dead. What that push carries, including the busy-child ordering
/// where the text is still queued and not yet in the transcript, is proven by the
/// digest tests in `orchestrator::attention_delivery`.
#[tokio::test(flavor = "current_thread")]
async fn a_queued_operator_message_leaves_the_watching_coordinator_a_passive_catchup() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "running").await;
    seed_coordinating_parent(&orch.db.local).await;

    continue_job_or_enqueue(
        &orch,
        "job-1",
        Some("also ship the trusted producer"),
        None,
        None,
    )
    .expect("queued send must return Ok");

    assert_eq!(
        catchup_pushes(&orch.db.local, "coordinator").await,
        vec![("catchup:job-1".to_string(), "passive".to_string())],
        "the coordinating job gets one passive catch-up for the operator's message"
    );
    assert!(
        catchup_pushes(&orch.db.local, "job-1").await.is_empty(),
        "the addressed node never receives catch-up for its own chat"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn continue_job_or_enqueue_queues_when_head_turn_running() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "running").await;
    let before_turns = turn_count(&orch.db.local, "job-1").await;

    // A racy send against a running head turn queues instead of 500ing.
    continue_job_or_enqueue(&orch, "job-1", Some("ping"), None, None)
        .expect("queued send must return Ok, not the active-turn error");

    assert_eq!(
        queued_rows(&orch.db.local, "job-1").await,
        vec![("ping".to_string(), "queue".to_string())],
        "the message must be enqueued as a Queue-delivery row"
    );
    assert_eq!(
        turn_count(&orch.db.local, "job-1").await,
        before_turns,
        "the queue path must not create a successor turn"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn continue_job_or_enqueue_queues_when_head_turn_pending() {
    // Pending counts as mid-turn for the active-turn predicate — the turn-creation
    // guard refuses pending and running alike, so the queue path treats them the same.
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "pending").await;

    continue_job_or_enqueue(&orch, "job-1", Some("ping"), None, None)
        .expect("queued send must return Ok, not the active-turn error");

    assert_eq!(
        queued_rows(&orch.db.local, "job-1").await,
        vec![("ping".to_string(), "queue".to_string())],
        "a pending head turn must queue the same as a running one"
    );
}

/// A send to an idle job is acknowledged the same way a send to a busy one is:
/// by the durable row, not by a launch. The launch is the nudge's, after the
/// caller already has its answer.
#[tokio::test(flavor = "current_thread")]
async fn an_idle_send_is_acknowledged_at_the_durable_row() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "complete").await;

    let run = continue_job_or_enqueue(&orch, "job-1", Some("^"), None, None)
        .expect("an idle send is accepted once its row is durable");

    assert_eq!(
        run.map(|run| run.id).as_deref(),
        Some("run-1"),
        "the acknowledgement reports the job's latest run"
    );
    assert_eq!(
        queued_rows(&orch.db.local, "job-1").await,
        vec![("^".to_string(), "queue".to_string())],
        "the idle send is the same Queue row a busy one is"
    );
}

/// Delivery is the resume claiming the row, so a resume that claims it and then
/// cannot launch is the one place an acknowledged send could be lost: a row
/// left stamped delivered is a message nobody received and nothing redelivers.
/// This fixture's execution has no host agent snapshot, so the launch is refused
/// after it has claimed the row and published its successor turn — the window
/// under test. The refusal is asserted by name: if the fixture ever becomes
/// launchable, this should fail as that, not as a confusing delivery assertion.
///
/// Driven through the nudge synchronously, on a row enqueued without one, so the
/// assertions observe the settled outcome rather than racing a background thread.
#[tokio::test(flavor = "current_thread")]
async fn a_launch_that_fails_returns_the_claimed_send_to_pending() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "complete").await;
    queued::enqueue(&orch.db.local, "job-1", "^", Delivery::Queue).unwrap();

    let error = nudge_job_for_urgency(&orch, "job-1", Delivery::Queue)
        .expect_err("an execution with no agent snapshot cannot launch");
    assert!(
        error.contains("Execution has no snapshot"),
        "the launch must fail at the snapshot step, after claiming: {error}"
    );

    assert_eq!(
        recorded_rows(&orch.db.local, "job-1").await,
        vec![("^".to_string(), false)],
        "a claim whose launch failed must not leave the message reading as delivered"
    );
    assert_eq!(
        turn_states(&orch.db.local, "job-1").await,
        vec![
            ("complete".to_string(), None),
            ("cancelled".to_string(), Some("launch_refused".to_string())),
        ],
        "the successor a refused launch published is settled, not left as a live head"
    );
}

/// (state, end_reason) of every turn for the job, oldest first.
async fn turn_states(db: &LocalDb, job_id: &str) -> Vec<(String, Option<String>)> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT state, end_reason FROM turns WHERE job_id = ?1 ORDER BY sequence ASC",
                    params![job_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.opt_text(1)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn continue_job_or_enqueue_empty_message_does_not_queue() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "running").await;

    // Whitespace-only and absent messages have nothing to enqueue, so they fall
    // straight through to continue_job_impl (which errors here for lack of a real
    // worktree/process). Neither must leave a queued row behind.
    let _ = continue_job_or_enqueue(&orch, "job-1", Some("   "), None, None);
    let _ = continue_job_or_enqueue(&orch, "job-1", None, None, None);

    assert!(
        queued_rows(&orch.db.local, "job-1").await.is_empty(),
        "an empty send must not be enqueued"
    );
}
