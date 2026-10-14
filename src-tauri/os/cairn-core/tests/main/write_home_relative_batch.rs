//! `cairn:~` across one write batch.
//!
//! A new thread's first two canonical actions are renaming itself off its
//! `thread-<n>` placeholder and writing its arc. Sent as one batch, the rename
//! moves the very home the arc item names — and the write handler resolves every
//! home-relative target to canonical before ANY item applies, because
//! blocking-append classification reads the raw target ahead of dispatch. The
//! arc item therefore used to be dispatched at the pre-rename address and
//! rejected with `Thread 'thread-7' not found in hom`: a name the caller never
//! typed, which by then genuinely did not exist.
//!
//! These tests pin the batch as a whole — what resolves, what is refused, and in
//! whose vocabulary — through `handle_write` against a real database.

use crate::common;
use cairn_core::internal::storage::{LocalDb, RowExt};
use serde_json::json;

const PROJECT: &str = "hom";
/// The placeholder name a thread is minted with, and renames away from.
const PLACEHOLDER: &str = "thread-7";
const RUN: &str = "r-1";

async fn fixture() -> (
    tempfile::TempDir,
    std::sync::Arc<LocalDb>,
    cairn_core::internal::orchestrator::Orchestrator,
) {
    let (temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    common::seed_authenticated_thread_run(&db, &format!("project-{PROJECT}"), PLACEHOLDER, RUN)
        .await;
    (temp, db, orch)
}

/// A normal authenticated CLI write must not inherit the minted branch
/// continuation's poll stack. This is the one-item project-thread creation that
/// previously overflowed a Tokio runtime worker before resource dispatch.
#[tokio::test]
async fn authenticated_resource_only_thread_creation_completes() {
    let (_temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    common::seed_authenticated_thread_run(&db, &format!("project-{PROJECT}"), PLACEHOLDER, RUN)
        .await;

    let result = common::change_resource_as_run(
        &orch,
        json!([{
            "target": format!("cairn://p/{PROJECT}/threads"),
            "mode": "append",
            "payload": { "name": "created-from-write" }
        }]),
        Some(RUN),
    )
    .await;

    assert!(
        result.contains("created-from-write"),
        "the resource write must reach thread creation instead of overflowing: {result}"
    );
}

/// The thread's current name, which is what the run's home URI is built from.
async fn thread_name(db: &LocalDb) -> String {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query("SELECT name FROM threads LIMIT 1", ()).await?;
            rows.next().await?.expect("the thread row survives").text(0)
        })
    })
    .await
    .unwrap()
}

/// The job that owns the stored arc, or `None` when no arc was ever written.
async fn arc_owner(db: &LocalDb) -> Option<String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM artifacts WHERE output_name = 'arc' LIMIT 1",
                    (),
                )
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

fn rename_to(name: &str) -> serde_json::Value {
    json!({ "target": "cairn:~", "mode": "patch", "payload": { "name": name } })
}

fn create_arc(intent: &str) -> serde_json::Value {
    json!({
        "target": "cairn:~/arc",
        "mode": "create",
        "payload": { "current_intent": intent }
    })
}

/// The motivating batch: rename this thread, then write its arc, in one call.
/// Both items land, and the arc lands on the thread under its NEW name.
#[tokio::test]
async fn a_thread_renames_itself_and_writes_its_arc_in_one_batch() {
    let (_temp, db, orch) = fixture().await;

    let result = common::change_resource_as_run(
        &orch,
        json!([
            rename_to("execution"),
            create_arc("Placing work on machines.")
        ]),
        Some(RUN),
    )
    .await;

    assert!(
        result.contains(&format!("Patched thread cairn://p/{PROJECT}/execution")),
        "the rename must land: {result}"
    );
    assert!(
        result.contains("version 1"),
        "the arc must land in the same batch: {result}"
    );
    assert!(
        !result.contains("not found"),
        "nothing in this batch addresses a thread that does not exist: {result}"
    );
    assert!(
        result.contains(&format!("cairn://p/{PROJECT}/execution/arc")),
        "the arc item resolves against the renamed thread: {result}"
    );

    assert_eq!(thread_name(&db).await, "execution");
    assert_eq!(
        arc_owner(&db).await.as_deref(),
        Some(format!("job-{RUN}").as_str()),
        "the arc lands on the renamed thread's own session job"
    );
}

/// When the rename does NOT apply, the item that resolved through it is refused
/// naming that dependency — both items, and the name the batch was moving to.
/// Reporting a missing thread here would describe a lookup rather than a cause.
#[tokio::test]
async fn an_item_that_resolves_through_a_failed_rename_names_the_rename() {
    let (_temp, db, orch) = fixture().await;

    // A name the thread-name contract refuses, so item 0 fails on its own terms
    // and item 1's resolution is left hanging off a rename that never happened.
    let result = common::change_resource_as_run(
        &orch,
        json!([rename_to("Execution_Thread"), create_arc("Placing work.")]),
        Some(RUN),
    )
    .await;

    assert!(
        result.contains("item 1's `cairn:~/arc` resolves through item 0's rename"),
        "the dependent item must name the rename it resolved through: {result}"
    );
    assert!(
        result.contains("Execution_Thread"),
        "the refusal must name what the batch was renaming to: {result}"
    );
    assert!(
        !result.contains("not found"),
        "a missing thread is a symptom here, not the cause: {result}"
    );

    assert_eq!(
        thread_name(&db).await,
        PLACEHOLDER,
        "a refused rename leaves the thread where it was"
    );
    assert_eq!(
        arc_owner(&db).await,
        None,
        "the dependent item wrote nothing"
    );
}

/// A home that cannot be resolved at all fails the items that named it, in the
/// shorthand they were written in. Left to fall through as a raw `cairn:~`
/// target, the same item failed downstream in whatever vocabulary the dispatcher
/// it happened to reach used.
#[tokio::test]
async fn an_unresolvable_home_fails_the_item_naming_the_shorthand() {
    let (_temp, db, orch) = fixture().await;

    // No run id: there is no authenticated run whose home `cairn:~` could name.
    let result =
        common::change_resource_as_run(&orch, json!([create_arc("Placing work.")]), None).await;

    assert!(
        result.contains("`cairn:~/arc` names this run's home, which could not be resolved"),
        "the refusal must name the shorthand and say what failed: {result}"
    );
    assert!(
        result.contains("run ID"),
        "the refusal must carry the underlying resolution error: {result}"
    );
    assert_eq!(arc_owner(&db).await, None, "nothing was written");
}

/// Whatever a home-derived item fails on, the rejection names the bridge the
/// caller never saw: the shorthand as typed, and the canonical URI it became.
/// This is the backstop behind the reported `Thread 'executor' not found in
/// cairn` — a message that describes a lookup against an address the caller
/// never wrote.
#[tokio::test]
async fn a_home_derived_failure_names_the_shorthand_and_what_it_resolved_to() {
    let (_temp, _db, orch) = fixture().await;

    common::change_resource_as_run(&orch, json!([create_arc("Placing work.")]), Some(RUN)).await;

    let result = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/arc",
            "mode": "patch",
            "payload": { "census": "a field the arc does not have" }
        }]),
        Some(RUN),
    )
    .await;

    assert!(
        result.contains("has no field"),
        "the underlying rejection survives: {result}"
    );
    assert!(
        result.contains(&format!(
            "(`cairn:~/arc` resolved to `cairn://p/{PROJECT}/{PLACEHOLDER}/arc`)"
        )),
        "the rejection must name the shorthand and its canonical target: {result}"
    );
}

/// The pre-pass exists so blocking-append classification sees a real resource:
/// it runs on the raw target, before the dispatch that would otherwise resolve
/// it. A `cairn:~/tasks` append must still be recognized as a blocking append
/// rather than dispatched as an ordinary resource write.
#[tokio::test]
async fn a_home_relative_tasks_append_is_still_classified_as_a_blocking_append() {
    let (_temp, db, orch) = fixture().await;

    let result = common::change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn:~/tasks",
            "mode": "append",
            "payload": {
                "subagentType": "Explore",
                "description": "map the fleet",
                "prompt": "Trace how executors are placed."
            }
        }]),
        Some(RUN),
    )
    .await;

    // Classification is explicit in the host envelope, not inferred from prose.
    let report: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(report["applied"], json!([]));
    assert!(
        report["blocking"]["text"]
            .as_str()
            .unwrap()
            .contains("Agent config not found: Explore"),
        "the blocking group must be the path that ran: {result}"
    );
    assert_eq!(
        arc_owner(&db).await,
        None,
        "a tasks append writes no artifact"
    );
}

#[tokio::test]
async fn blocking_envelope_keeps_successes_and_failures_in_the_same_report() {
    let (_temp, _db, orch) = fixture().await;
    let result = common::change_resource_as_run(
        &orch,
        json!([
            {
                "target": "cairn://p/missing/messages",
                "mode": "append",
                "payload": { "content": "must fail" }
            },
            {
                "target": format!("cairn://p/{PROJECT}/messages"),
                "mode": "append",
                "payload": { "content": "must persist" }
            },
            {
                "target": "cairn:~/tasks",
                "mode": "append",
                "payload": {
                    "subagentType": "Explore",
                    "description": "missing fixture agent",
                    "prompt": "Do not spawn"
                }
            }
        ]),
        Some(RUN),
    )
    .await;
    let report: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(report["applied"].as_array().unwrap().len(), 1, "{result}");
    assert_eq!(report["applied"][0]["index"], 1);
    assert_eq!(report["failures"].as_array().unwrap().len(), 1, "{result}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert_eq!(report["partial_success"], true);
    assert!(report["blocking"]["text"]
        .as_str()
        .unwrap()
        .contains("Agent config not found: Explore"));
}
