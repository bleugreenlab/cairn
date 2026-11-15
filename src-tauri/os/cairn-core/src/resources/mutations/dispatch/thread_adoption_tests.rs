//! A same-project thread filing an execution on an issue that hangs from
//! nothing adopts the issue before admission; every other filer, and every
//! issue that already has a parent, is left exactly as it was.
//!
//! Each filing goes through the real `IssueExecutions` append dispatcher with
//! a run whose thread is derived from its own job, never from the payload.

use serde_json::{json, Value};

use crate::execution::gates::capacity::tests::{
    add_issue, add_started, add_thread, caps, detail, is_held,
};
use crate::execution::gates::capacity::THREAD_FOCUS_FACT;
use crate::execution::gates::reorder::tests::{request, rows, thread_run, write};
use crate::execution::gates::sweep::tests::{count, env, execution, Env};
use crate::mcp::types::{ChangeItem, ChangeMode};

const ALPHA: &str = "run-alpha";

/// Fleet room to spare, thread alpha (`ta`) and beta (`tb`), and alpha's
/// session live.
async fn adoption_env(name: &str) -> Env {
    let env = env(name).await;
    caps(&env, 50, 50);
    add_thread(&env, "ta", "alpha").await;
    add_thread(&env, "tb", "beta").await;
    thread_run(&env, ALPHA, Some("ta"), None).await;
    env
}

async fn file_as(
    env: &Env,
    run: Option<&str>,
    number: i32,
    payload: Value,
) -> Result<String, String> {
    write(
        env,
        run,
        &format!("cairn://p/prj/{number}/executions"),
        ChangeMode::Append,
        payload,
    )
    .await
}

async fn preview_as(env: &Env, run: Option<&str>, number: i32, payload: Value) -> String {
    let item = ChangeItem {
        target: format!("cairn://p/prj/{number}/executions"),
        mode: ChangeMode::Append,
        payload: Some(payload),
    };
    crate::resources::mutations::dispatch_resource_change(&env.orch, &request(run), 0, &item, true)
        .await
        .map(|change| change.summary)
        .unwrap_or_else(|failure| panic!("preview refused: {}", failure.error))
}

/// `(parent_issue_id, parent_thread_id, parent_job_id)` of issue `number`.
async fn parents(env: &Env, number: i32) -> (Option<String>, Option<String>, Option<String>) {
    env.db
        .query_one(
            "SELECT parent_issue_id, parent_thread_id, parent_job_id FROM issues WHERE number = ?1",
            (number,),
            |row| {
                use crate::storage::RowExt;
                Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?))
            },
        )
        .await
        .unwrap()
}

async fn executions_of(env: &Env, number: i32) -> i64 {
    count(
        env,
        &format!(
            "SELECT COUNT(*) FROM executions e JOIN issues i ON i.id = e.issue_id \
             WHERE i.number = {number}"
        ),
    )
    .await
}

fn triage() -> Value {
    json!({"recipe": "memory-triage"})
}

/// The thread that files the work owns it: the issue hangs from alpha before
/// admission is decided, so alpha's own focus cap holds it, and the reply says
/// so. Filing again adopts nothing twice.
#[tokio::test]
async fn a_thread_filing_adopts_an_unparented_issue_before_admission() {
    let env = env("adopt-before-admission.db").await;
    // Alpha's focus cap is one open child, and it already has one running.
    caps(&env, 50, 1);
    add_thread(&env, "ta", "alpha").await;
    add_started(&env, 150, Some("ta"), "running").await;
    thread_run(&env, ALPHA, Some("ta"), None).await;
    add_issue(&env, 210, None).await;

    // The preview asks the queue as it will stand once alpha has adopted the
    // issue: alpha's cap is full, so it would be held there. Asked of the
    // still-unparented row, the queue would wrongly say it starts.
    let preview = preview_as(&env, Some(ALPHA), 210, triage()).await;
    let (_, held_line) = preview
        .split_once("the queue would hold it now: ")
        .unwrap_or_else(|| panic!("the preview names the hold: {preview}"));
    assert!(held_line.contains("alpha"), "on alpha's cap: {preview}");
    assert!(
        preview.contains("It would count against alpha's focus cap"),
        "{preview}"
    );
    assert_eq!(
        parents(&env, 210).await,
        (None, None, None),
        "preview writes no edge"
    );
    assert_eq!(executions_of(&env, 210).await, 0, "preview files nothing");

    let filed = file_as(&env, Some(ALPHA), 210, triage()).await.unwrap();
    assert!(
        filed.contains("Adopted prj/210 under thread alpha: it had no parent"),
        "{filed}"
    );
    assert_eq!(
        parents(&env, 210).await,
        (None, Some("ta".to_string()), None),
        "a thread edge, and no issue ancestry"
    );
    assert!(is_held(&env, 210).await, "alpha's full focus cap holds it");
    assert!(
        filed.contains(
            "Filed execution 1 for prj/210 \"Issue 210\" (recipe 'memory-triage'): Queued · \
             alpha 1/1 open."
        ),
        "the reply says it is held on alpha's cap: {filed}"
    );
    let (execution_id, ..) = execution(&env, 210).await;
    // The reply's line carries the count; the hold's detail names the child.
    let held_detail = detail(&env, &execution_id).await;
    assert!(
        held_detail.contains("alpha") && held_detail.contains("150"),
        "the hold names alpha's open child prj/150: {held_detail}"
    );
    assert!(
        rows(&env)
            .await
            .contains_key(&(execution_id, THREAD_FOCUS_FACT.to_string())),
        "it is held on alpha's own focus cap, which only a parented child counts against"
    );

    let again = file_as(&env, Some(ALPHA), 210, triage()).await.unwrap();
    assert!(!again.contains("Adopted"), "{again}");
    assert_eq!(parents(&env, 210).await.1.as_deref(), Some("ta"));
}

/// A loose issue that already has live work becomes one of the thread's open
/// children the moment it is adopted, so a rerun on it opens no new child and
/// the full thread cap does not hold it. The preview says what the filing
/// then does.
#[tokio::test]
async fn an_adopting_rerun_on_live_work_is_previewed_as_the_filing_decides() {
    let env = env("adopt-rerun-open.db").await;
    caps(&env, 50, 1);
    add_thread(&env, "ta", "alpha").await;
    add_started(&env, 150, Some("ta"), "running").await;
    thread_run(&env, ALPHA, Some("ta"), None).await;
    // Unparented, with an execution already running.
    add_started(&env, 220, None, "running").await;

    let preview = preview_as(&env, Some(ALPHA), 220, triage()).await;
    assert!(
        preview.contains("Would adopt prj/220 under thread alpha first"),
        "{preview}"
    );
    assert!(
        !preview.contains("would hold it now"),
        "already open once adopted, so alpha's full cap does not hold it: {preview}"
    );
    assert_eq!(parents(&env, 220).await, (None, None, None));
    assert_eq!(executions_of(&env, 220).await, 1);

    let filed = file_as(&env, Some(ALPHA), 220, triage()).await.unwrap();
    assert!(
        filed.contains("Adopted prj/220 under thread alpha"),
        "{filed}"
    );
    assert_eq!(executions_of(&env, 220).await, 2);
    assert!(
        !is_held(&env, 220).await,
        "it starts, as previewed: {filed}"
    );
}

/// Nothing but a same-project thread's filing on an unparented issue adopts:
/// the operator and an issue's own worker infer no parent, an existing parent
/// of either kind stands, and a thread of another project that happens to
/// share the name never qualifies.
#[tokio::test]
async fn only_a_same_project_thread_adopts_and_only_what_hangs_from_nothing() {
    let env = adoption_env("adopt-controls.db").await;
    for number in [210, 211, 213] {
        add_issue(&env, number, None).await;
    }
    add_issue(&env, 212, Some("tb")).await;
    // An ordinary issue worker: the coordinator's run on issue 9.
    // A same-named thread in another project, with its own live session.
    env.db
        .execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('q', 'default', 'Other', 'other', '/tmp/other', 1, 1);
             INSERT INTO threads(id, project_id, name, created_at, updated_at)
               VALUES('tq', 'q', 'alpha', 1, 1);
             INSERT INTO jobs(id, project_id, node_name, status, thread_id, uri_segment,
                              created_at, updated_at)
               VALUES('job-foreign', 'q', 'session', 'running', 'tq', 'thread', 1, 1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
               VALUES('run-foreign', 'q', 'job-foreign', 'live', 1, 1);",
        )
        .await
        .unwrap();

    file_as(&env, None, 210, triage()).await.unwrap();
    assert_eq!(parents(&env, 210).await, (None, None, None), "operator");

    file_as(&env, Some("run-coord"), 211, triage())
        .await
        .unwrap();
    assert_eq!(parents(&env, 211).await, (None, None, None), "issue worker");

    file_as(&env, Some("run-foreign"), 213, triage())
        .await
        .unwrap();
    assert_eq!(
        parents(&env, 213).await,
        (None, None, None),
        "a foreign project's same-named thread"
    );

    let other_thread = file_as(&env, Some(ALPHA), 212, triage()).await.unwrap();
    assert!(!other_thread.contains("Adopted"), "{other_thread}");
    assert_eq!(parents(&env, 212).await.1.as_deref(), Some("tb"));

    // Issue 2 hangs from issue 9, filed by its coordinator.
    let issue_child = file_as(&env, Some(ALPHA), 2, triage()).await.unwrap();
    assert!(!issue_child.contains("Adopted"), "{issue_child}");
    assert_eq!(
        parents(&env, 2).await,
        (Some("ip".to_string()), None, Some("coord".to_string()))
    );
}

/// Adoption sets the semantic edge and nothing else: a recorded creator
/// (`parent_job_id`) is provenance and survives it.
#[tokio::test]
async fn adoption_keeps_the_creator_provenance() {
    let env = adoption_env("adopt-provenance.db").await;
    add_issue(&env, 210, None).await;
    env.db
        .execute(
            "UPDATE issues SET parent_job_id = 'coord' WHERE number = 210",
            (),
        )
        .await
        .unwrap();
    file_as(&env, Some(ALPHA), 210, triage()).await.unwrap();
    assert_eq!(
        parents(&env, 210).await,
        (None, Some("ta".to_string()), Some("coord".to_string()))
    );
}

/// A preview says it would adopt and writes nothing; a refusal before the
/// launch (an unknown recipe, a malformed argument, a branch target the
/// recipe does not declare) adopts nothing either.
#[tokio::test]
async fn a_preview_or_a_refused_filing_adopts_nothing() {
    let env = adoption_env("adopt-preview.db").await;
    add_issue(&env, 210, None).await;

    let preview = preview_as(&env, Some(ALPHA), 210, triage()).await;
    assert!(
        preview.contains(
            "Would adopt prj/210 under thread alpha first: it has no parent. It would count \
             against alpha's focus cap."
        ),
        "room to spare, so nothing would hold it: {preview}"
    );

    let unknown = file_as(&env, Some(ALPHA), 210, json!({"recipe": "no-such-recipe"}))
        .await
        .unwrap_err();
    assert!(!unknown.contains("adopted"), "{unknown}");
    let malformed = file_as(
        &env,
        Some(ALPHA),
        210,
        json!({"recipe": "memory-triage", "lifecycle": "sideways"}),
    )
    .await
    .unwrap_err();
    assert!(malformed.contains("payload.lifecycle"), "{malformed}");
    // memory-triage declares no branch targets.
    let undeclared = file_as(
        &env,
        Some(ALPHA),
        210,
        json!({"recipe": "memory-triage", "branch": "base"}),
    )
    .await
    .unwrap_err();
    assert!(!undeclared.contains("adopted"), "{undeclared}");

    assert_eq!(parents(&env, 210).await, (None, None, None));
    assert_eq!(executions_of(&env, 210).await, 0);
}

/// A start refused after the adoption committed keeps the adoption (undoing
/// it unconditionally could erase a parent set since) and says so; the retry
/// adopts nothing twice.
#[tokio::test]
async fn a_start_refused_after_adoption_keeps_the_parent_and_says_so() {
    let env = adoption_env("adopt-then-refused.db").await;
    add_issue(&env, 210, None).await;
    // A held filing adopts like any other. Waiting on its own merge is a wait
    // cycle, refused only inside the launch's insert transaction, after the
    // adoption committed.
    let refused = file_as(
        &env,
        Some(ALPHA),
        210,
        json!({"recipe": "memory-triage",
               "after": [{"fact": "issue_resolution", "issue": "cairn://p/prj/210", "resolution": "merged"}]}),
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains(
            "prj/210 was adopted under thread alpha before the start failed and keeps that parent"
        ),
        "{refused}"
    );
    assert_eq!(parents(&env, 210).await.1.as_deref(), Some("ta"));
    assert_eq!(executions_of(&env, 210).await, 0);

    let retried = file_as(&env, Some(ALPHA), 210, triage()).await.unwrap();
    assert!(!retried.contains("Adopted"), "{retried}");
    assert_eq!(executions_of(&env, 210).await, 1);
}

/// `place:"head"` on an unparented issue is decided against the thread about
/// to adopt it: that thread may place its own work, and an operator's head
/// filing, which adopts nothing, is still refused for want of a thread.
#[tokio::test]
async fn a_head_filing_is_placed_in_the_adopting_threads_queue() {
    let env = adoption_env("adopt-head.db").await;
    add_issue(&env, 210, None).await;
    add_issue(&env, 211, None).await;
    let head = json!({"recipe": "memory-triage", "place": "head"});

    let operator = file_as(&env, None, 211, head.clone()).await.unwrap_err();
    assert!(operator.contains("has no parent thread"), "{operator}");
    assert_eq!(parents(&env, 211).await, (None, None, None));

    let with_after = file_as(
        &env,
        Some(ALPHA),
        211,
        json!({"recipe": "memory-triage", "place": "head",
               "after": [{"fact": "issue_resolution", "issue": "cairn://p/prj/1", "resolution": "merged"}]}),
    )
    .await
    .unwrap_err();
    assert!(
        with_after.contains("place:\"head\" orders cap-held work"),
        "{with_after}"
    );
    assert_eq!(
        parents(&env, 211).await,
        (None, None, None),
        "refused before adopting"
    );

    let filed = file_as(&env, Some(ALPHA), 210, head).await.unwrap();
    assert!(
        filed.contains("It started now; nothing to place."),
        "{filed}"
    );
    assert!(
        filed.contains("Adopted prj/210 under thread alpha"),
        "{filed}"
    );
    assert_eq!(parents(&env, 210).await.1.as_deref(), Some("ta"));
}
