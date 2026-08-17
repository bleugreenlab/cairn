//! A closed thread takes no new turns — including from the desktop, which never
//! touches the message-delivery or wake paths that the rest of dormancy gates.
//!
//! The composer sends by resuming the thread's session job directly
//! (`continue_job` → `continue_job_or_enqueue` → `continue_job_impl`), and the
//! pane's resume-from-digest control and the answer-a-question path do the same.
//! None of them ask for a session to be established, so a gate that lived only at
//! session establishment would leave the app's own send path enforced by nothing
//! but a React prop. These pin the refusal at the funnel all three share.

use crate::common;
use cairn_core::internal::execution::jobs::{continue_job_or_enqueue, resume_job_from_digest};
use cairn_core::internal::orchestrator::attention_push::{
    has_pending_waking_live, list_pending, push, Boundary, Wake,
};
use cairn_core::internal::storage::{LocalDb, RowExt};
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

    let run = continue_job_or_enqueue(&orch, "job-task", Some("carry on"), None, Some("req-1"))
        .expect("a task the thread spawned is an ordinary job and starts its turn");
    assert_eq!(run.job_id.as_deref(), Some("job-task"));
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
    let run = continue_job_or_enqueue(&orch, "job-thread", Some("hello"), None, Some("req-2"))
        .expect("reopening lets the same send start its turn");
    assert_eq!(run.job_id.as_deref(), Some("job-thread"));
}
