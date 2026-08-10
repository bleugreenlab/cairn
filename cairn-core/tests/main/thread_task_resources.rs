//! A thread-spawned task writing to its own home.
//!
//! A task under a thread is addressed at `cairn://p/{project}/{thread}/task/
//! {segment}`, which every one of its `cairn:~/` writes resolves to. Those
//! writes reached the resource contract (which advertises them) and then fell
//! off the end of dispatch, so a thread-spawned task could not write the
//! artifact it was spawned to produce: its job stayed `blocked` on the missing
//! output, its delegated packet never went terminal, and the thread suspended on
//! it waited forever (CAIRN-3755).
//!
//! The existing parity test covers one URI per resource KIND, and every thread
//! descendant collapses into the single `Thread` kind — which is how a whole
//! writable subtree shipped with no dispatch behind it. These tests walk the
//! paths themselves.

use crate::common;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;
use serde_json::json;

const PROJECT: &str = "CAIRN";
const THREAD: &str = "thread-ux";
const TASK: &str = "probe";

/// A thread with a live session and one task hanging off it — the shape
/// delegation leaves behind, with the task reached through its parent rather
/// than by a denormalized thread id.
async fn seed_thread_with_task(db: &LocalDb) {
    db.execute(
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
         VALUES ('th', 'project-CAIRN', ?1, 'active', 'none', 1, 1)",
        params![THREAD],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs (id, thread_id, project_id, status, uri_segment, node_name,
                           created_at, updated_at)
         VALUES ('j-session', 'th', 'project-CAIRN', 'running', 'thread', 'Thread', 1, 1)",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs (id, parent_job_id, project_id, status, uri_segment, node_name,
                           created_at, updated_at)
         VALUES ('j-task', 'j-session', 'project-CAIRN', 'running', ?1, 'Explore', 2, 2)",
        params![TASK],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at)
         VALUES ('r-task', 'j-task', 'project-CAIRN', 'live', 2, 2)",
        (),
    )
    .await
    .unwrap();
}

async fn artifact_owner(db: &LocalDb, output_name: &str) -> Option<String> {
    let output_name = output_name.to_string();
    db.read(move |conn| {
        let output_name = output_name.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM artifacts WHERE output_name = ?1 LIMIT 1",
                    params![output_name.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| cairn_core::internal::storage::RowExt::opt_text(&row, 0))
                .transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

/// The completion contract of a thread-spawned task: the `return` artifact it
/// was spawned to produce lands on the TASK's job. Without it the job's derived
/// status is `blocked` on a missing required output, which is exactly where the
/// live acceptance run stalled.
#[tokio::test]
async fn a_thread_task_writes_the_artifact_it_was_spawned_to_produce() {
    let (_temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    seed_thread_with_task(&db).await;

    // The address under test is the one the task is actually handed: its home
    // URI is what every `cairn:~/` write it makes resolves to, so writing
    // anywhere else would prove nothing about the live failure.
    let home = cairn_core::internal::jobs::queries::home_uri_for_job(&db, "j-task")
        .await
        .unwrap();
    assert_eq!(
        home.as_deref(),
        Some(format!("cairn://p/{PROJECT}/{THREAD}/task/{TASK}").as_str())
    );

    let result = common::change_resource(
        &orch,
        json!([{
            "target": format!("{}/return", home.unwrap()),
            "mode": "create",
            "payload": { "content": "the answer" }
        }]),
    )
    .await;

    assert!(
        !result.contains("no dispatch arm handles it"),
        "a task's artifact write fell through dispatch: {result}"
    );
    assert_eq!(
        artifact_owner(&db, "return").await.as_deref(),
        Some("j-task"),
        "the artifact belongs to the task that wrote it, not to the thread's session: {result}"
    );
}

/// The task's other job-owned writes land on the task too. A thread's session
/// and its tasks are different jobs; addressing one must never mutate the other.
#[tokio::test]
async fn a_thread_task_owns_its_todos_and_its_session_keeps_its_own() {
    let (_temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    seed_thread_with_task(&db).await;

    let result = common::change_resource(
        &orch,
        json!([{
            "target": format!("cairn://p/{PROJECT}/{THREAD}/task/{TASK}/todos"),
            "mode": "append",
            "payload": { "todos": [{ "content": "read the parser", "status": "pending" }] }
        }]),
    )
    .await;
    assert!(
        !result.contains("no dispatch arm handles it"),
        "a task's todos write fell through dispatch: {result}"
    );

    let owner = common::scalar_text_by_id(
        &db,
        "SELECT job_id FROM todos WHERE content = ?1 LIMIT 1",
        "read the parser",
    )
    .await;
    assert_eq!(
        owner.as_deref(),
        Some("j-task"),
        "a task's todos belong to the task, not to the session that spawned it: {result}"
    );
}

/// Every writable descendant of a thread task reaches a dispatch arm.
///
/// The catch-all this guards against is not a refusal an agent can act on: the
/// contract advertises the mutation and then reports an internal error, which is
/// what sent the live task looping through four different addresses before it
/// gave up and answered into the thread's chat instead.
#[tokio::test]
async fn every_writable_thread_task_descendant_reaches_dispatch() {
    let (_temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    seed_thread_with_task(&db).await;

    let base = format!("cairn://p/{PROJECT}/{THREAD}/task/{TASK}");
    let cases = [
        (format!("{base}/return"), "create", json!({"content": "x"})),
        (
            format!("{base}/artifact"),
            "create",
            json!({"content": "x"}),
        ),
        (
            format!("{base}/todos"),
            "append",
            json!({"todos": [{"content": "x", "status": "pending"}]}),
        ),
        (
            format!("{base}/messages"),
            "append",
            json!({"content": "x"}),
        ),
    ];

    for (target, mode, payload) in cases {
        let result = common::change_resource(
            &orch,
            json!([{ "target": target, "mode": mode, "payload": payload }]),
        )
        .await;
        assert!(
            !result.contains("no dispatch arm handles it"),
            "{target} mode={mode} fell through dispatch: {result}"
        );
        assert!(
            !result.contains("Unsupported resource mutation"),
            "{target} mode={mode} was gate-rejected: {result}"
        );
    }

    // A read-only descendant is refused by the contract, which is an answer the
    // caller can act on — unlike the internal catch-all, which advertises the
    // mutation and then reports a defect. Both canonical phrases are asserted:
    // the gate's rejection and the reason, which together are what tell a caller
    // the address was right and the mode was not.
    for target in [
        base.clone(),
        format!("{base}/chat"),
        format!("{base}/checks"),
    ] {
        let read_only = common::change_resource(
            &orch,
            json!([{ "target": target, "mode": "append", "payload": {"content": "x"} }]),
        )
        .await;
        assert!(
            read_only.contains("Unsupported resource mutation")
                && read_only.contains("This resource is read-only"),
            "{target} is read-only and should say so canonically: {read_only}"
        );
        assert!(
            !read_only.contains("no dispatch arm handles it"),
            "{target} must be gate-refused, never fall through dispatch: {read_only}"
        );
    }

    // The fixture is load-bearing: a task that does not exist would make every
    // assertion above pass for the wrong reason.
    assert!(
        common::scalar_text_by_id(&db, "SELECT id FROM jobs WHERE id = ?1", "j-task")
            .await
            .is_some()
    );
}

/// A task that is not there is reported as missing, by name, under its thread —
/// not as a lookup for issue zero.
#[tokio::test]
async fn an_unknown_thread_task_is_refused_by_its_own_address() {
    let (_temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    seed_thread_with_task(&db).await;

    let result = common::change_resource(
        &orch,
        json!([{
            "target": format!("cairn://p/{PROJECT}/{THREAD}/task/ghost/return"),
            "mode": "create",
            "payload": { "content": "x" }
        }]),
    )
    .await;

    assert!(
        result.contains("ghost") && result.contains(THREAD),
        "a missing task should be named under its thread: {result}"
    );
}
