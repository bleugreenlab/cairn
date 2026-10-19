//! A closed thread takes no new turns — including from the desktop, which never
//! touches the message-delivery or wake paths that the rest of dormancy gates.
//!
//! The composer sends by writing the operator queue for the thread's session job
//! (`enqueue_user_message`, and the `continue_job` → `continue_job_or_enqueue`
//! compatibility ingress), which persists before any turn is admitted; the
//! pane's resume-from-digest control and the answer-a-question path resume the
//! job directly. None of them ask for a session to be established, so a gate that
//! lived only at session establishment would leave the app's own send path
//! enforced by nothing but a React prop. These pin the refusal at both funnels:
//! the queue's one writer and turn admission.

use crate::common;
use cairn_core::internal::execution::jobs::{continue_job_or_enqueue, resume_job_from_digest};
use cairn_core::internal::orchestrator::attention_push::{
    has_pending_waking_live, list_pending, push, Boundary, Wake,
};
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::messages::queued::{enqueue_idempotent, DeliveryUrgency};
use cairn_db::turso::params;

async fn seed_thread_session(db: &LocalDb, status: &str) {
    db.execute_script(&format!(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('proj-1','default','Test Project','PROJ','/tmp/test-repo',1,1);
         INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('thread-1','proj-1','general','{status}','none',1,1);
         INSERT INTO jobs (id, thread_id, project_id, node_name, uri_segment, status,
                           current_session_id, created_at, updated_at)
           VALUES ('job-thread','thread-1','proj-1','thread','thread','idle','session-1',1,1);
         INSERT INTO sessions (id, job_id, status, created_at, updated_at)
           VALUES ('session-1','job-thread','active',1,1);
         INSERT INTO jobs (id, thread_id, parent_job_id, project_id, node_name, uri_segment, status,
                           current_session_id, created_at, updated_at)
           VALUES ('job-task','thread-1','job-thread','proj-1','Survey','survey','idle','session-2',9,9);
         INSERT INTO sessions (id, job_id, status, created_at, updated_at)
           VALUES ('session-2','job-task','active',9,9);"
    ))
    .await
    .unwrap();
}

async fn set_status(db: &LocalDb, status: &str) {
    db.execute(
        "UPDATE threads SET status = ?1 WHERE id = 'thread-1'",
        params![status],
    )
    .await
    .unwrap();
}

fn is_dormancy_refusal(error: &str) -> bool {
    error.contains("closed") && error.contains("Reopen")
}

/// Operator messages accepted for a job, delivered or not. A message-bearing
/// send is acknowledged at this durable row and its turn starts behind a
/// background nudge, which may already have claimed it — so this reads the row,
/// not its pending state.
async fn operator_messages(db: &LocalDb, job_id: &str) -> Vec<String> {
    db.query_all(
        "SELECT content FROM queued_messages WHERE job_id = ?1 ORDER BY created_at, rowid",
        params![job_id],
        |row| row.text(0),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn the_desktop_send_path_is_refused_on_a_closed_thread() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_thread_session(&orch.db.local, "closed").await;

    let refused = continue_job_or_enqueue(
        &orch,
        "job-thread",
        Some("still there?"),
        None,
        Some("req-1"),
    )
    .expect_err("a closed thread starts no turn");
    assert!(
        is_dormancy_refusal(&refused),
        "the refusal names dormancy and how to undo it: {refused}"
    );

    // Nothing was persisted on the way to the refusal.
    assert_eq!(
        orch.db
            .local
            .query_one("SELECT COUNT(*) FROM runs", (), |row| row.i64(0))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        orch.db
            .local
            .query_one("SELECT COUNT(*) FROM queued_messages", (), |row| row.i64(0))
            .await
            .unwrap(),
        0,
        "a refused send is not silently converted into a durable queue row"
    );
}

/// The composer's own send is the canonical enqueue, which persists before any
/// turn is admitted — so it takes the refusal at the queue, not at turn start.
#[tokio::test(flavor = "current_thread")]
async fn the_composer_enqueue_is_refused_on_a_closed_thread() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_thread_session(&orch.db.local, "closed").await;

    let refused = enqueue_idempotent(
        &orch.db.local,
        "job-thread",
        "still there?",
        DeliveryUrgency::Queue,
        "req-1",
    )
    .expect_err("a closed thread takes no new operator message");
    assert!(
        is_dormancy_refusal(&refused),
        "the composer sees the same refusal the continue path does: {refused}"
    );
    assert!(operator_messages(&orch.db.local, "job-thread").await.is_empty());
}

/// Closing does not cancel a turn in flight, and a follow-up to that turn is not
/// new attention: it is kept, and claimed by the thread's next turn on reopen.
#[tokio::test(flavor = "current_thread")]
async fn a_follow_up_to_a_turn_still_running_on_a_closed_thread_is_kept() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_thread_session(&orch.db.local, "closed").await;
    orch.db
        .local
        .execute_script(
            "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
               VALUES ('run-1','proj-1','job-thread',NULL,'live','session-1',1,1,'resume');
             INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
               VALUES ('turn-1','session-1','run-1','job-thread',1,'running',1,1);",
        )
        .await
        .unwrap();

    continue_job_or_enqueue(&orch, "job-thread", Some("one more thing"), None, Some("req-1"))
        .expect("a follow-up to the running turn is queued, not refused");
    assert_eq!(
        operator_messages(&orch.db.local, "job-thread").await,
        vec!["one more thing".to_string()]
    );
}

/// The pane keeps every control it had while closed, resume-from-digest included
/// — so that control has to be refused by the backend rather than by whether the
/// UI happened to hide it.
#[tokio::test(flavor = "current_thread")]
async fn resume_from_digest_is_refused_on_a_closed_thread() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_thread_session(&orch.db.local, "closed").await;

    let refused = resume_job_from_digest(&orch, "job-thread", None)
        .expect_err("a closed thread starts no turn");
    assert!(
        is_dormancy_refusal(&refused),
        "resume-from-digest takes the same refusal the composer does: {refused}"
    );
}

/// The gate keys on the session job's shape, so work already running under the
/// thread is untouched — the same rule the wake and push axes take.
#[tokio::test(flavor = "current_thread")]
async fn a_task_under_a_closed_thread_still_starts_turns() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_thread_session(&orch.db.local, "closed").await;

    continue_job_or_enqueue(&orch, "job-task", Some("carry on"), None, Some("req-1"))
        .expect("a task the thread spawned is an ordinary job and starts its turn");
    assert_eq!(
        operator_messages(&orch.db.local, "job-task").await,
        vec!["carry on".to_string()],
        "the send is accepted for the task, where the nudge starts its turn"
    );
}

/// Dormancy and retirement both stop a push being delivered, and CAIRN-4182
/// makes it possible to confuse them. Only one is permanent.
///
/// A closed thread is a recipient that is not listening, not a referent that has
/// resolved. The queued row must stay pending — not retired, not delivered, not
/// deleted — and reopening must make the SAME row deliverable again. Mapping
/// dormancy onto retirement would silently discard everything queued while a
/// thread was closed, and the loss would be invisible: a push that never arrives
/// produces no signal at all (CAIRN-2410).
#[tokio::test(flavor = "current_thread")]
async fn a_push_queued_before_closure_survives_dormancy_and_delivers_after_reopen() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = &orch.db.local;
    seed_thread_session(db, "active").await;

    push(
        db,
        "job-thread",
        "cairn://p/PROJ/1/1/builder",
        Wake::Wake,
        Boundary::Event,
        "direct:cairn://p/PROJ/1/1/builder",
    )
    .await
    .unwrap();
    let queued = list_pending(db, "job-thread").await.unwrap();
    assert_eq!(queued.len(), 1);
    let id = queued[0].id.clone();
    assert!(has_pending_waking_live(db, "job-thread").await.unwrap());

    set_status(db, "closed").await;

    // Suspended: no wake, no delivery — and, critically, no retirement.
    assert!(
        !has_pending_waking_live(db, "job-thread").await.unwrap(),
        "a dormant thread is not roused by its queued pushes"
    );
    assert_eq!(
        count(db, "retired_at IS NOT NULL").await,
        0,
        "closing a thread suspends delivery; it does not resolve the referent"
    );
    assert_eq!(count(db, "delivered_event_id IS NOT NULL").await, 0);
    assert_eq!(
        list_pending(db, "job-thread").await.unwrap().len(),
        1,
        "the row is still pending, waiting for the thread to come back"
    );

    set_status(db, "active").await;

    assert!(
        has_pending_waking_live(db, "job-thread").await.unwrap(),
        "reopening restores deliverability"
    );
    let after = list_pending(db, "job-thread").await.unwrap();
    assert_eq!(after.len(), 1, "exactly one delivery, not a duplicate");
    assert_eq!(
        after[0].id, id,
        "the SAME row becomes deliverable again — nothing was reconstructed"
    );
}

async fn count(db: &LocalDb, predicate: &str) -> i64 {
    db.query_one(
        &format!("SELECT COUNT(*) FROM attention_pushes WHERE {predicate}"),
        (),
        |row| row.i64(0),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn reopening_admits_turns_again() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_thread_session(&orch.db.local, "closed").await;
    assert!(is_dormancy_refusal(
        &continue_job_or_enqueue(&orch, "job-thread", Some("hello"), None, Some("req-1"))
            .unwrap_err()
    ));

    set_status(&orch.db.local, "active").await;
    continue_job_or_enqueue(&orch, "job-thread", Some("hello"), None, Some("req-2"))
        .expect("reopening lets the same send start its turn");
    assert_eq!(
        operator_messages(&orch.db.local, "job-thread").await,
        vec!["hello".to_string()],
        "only the send after reopening was accepted"
    );
}
