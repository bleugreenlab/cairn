//! A same-project thread filing an execution on an issue that hangs from
//! nothing adopts the issue before admission; every other filer, and every
//! issue that already has a parent, is left exactly as it was.
//!
//! Each filing goes through the real `IssueExecutions` append dispatcher with
//! a run whose thread is derived from its own job, never from the payload.

use serde_json::{json, Value};

use crate::execution::gates::sweep::tests::{count, env, Env};
use crate::mcp::types::{ChangeItem, ChangeMode};

const ALPHA: &str = "run-alpha";

/// Fleet room to spare, thread alpha (`ta`) and beta (`tb`), and alpha's
/// session live.
async fn adoption_env(name: &str) -> Env {
    let env = env(name).await;
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
        preview.contains("Would adopt prj/210 under thread alpha first: it has no parent."),
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

pub(crate) async fn add_thread(env: &Env, id: &str, name: &str) {
    env.db
        .execute(
            "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES(?1,'p',?2,1,1)",
            (id, name),
        )
        .await
        .unwrap();
}

pub(crate) async fn add_issue(env: &Env, number: i32, thread: Option<&str>) {
    env.db.execute("INSERT INTO issues(id,project_id,number,title,status,progress,attention,parent_thread_id,created_at,updated_at) VALUES(?1,'p',?2,?3,'backlog','backlog','none',?4,1,1)", cairn_db::turso::params![format!("si{number}"),number,format!("Issue {number}"),thread]).await.unwrap();
}

pub(crate) async fn thread_run(env: &Env, run: &str, thread: Option<&str>, parent: Option<&str>) {
    let job = format!("job-{run}");
    env.db.execute("INSERT INTO jobs(id,project_id,node_name,status,thread_id,parent_job_id,uri_segment,created_at,updated_at) VALUES(?1,'p','session','running',?2,?3,?4,1,1)",cairn_db::turso::params![job.as_str(),thread,parent,if parent.is_some(){"task-1"}else{"thread"}]).await.unwrap();
    env.db.execute("INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at) VALUES(?1,'p',?2,'live',1,1)",(run,job.as_str())).await.unwrap();
}

pub(crate) fn request(run: Option<&str>) -> crate::mcp::types::McpCallbackRequest {
    crate::mcp::types::McpCallbackRequest {
        answer_bound_ms: None,
        cwd: "/".into(),
        run_id: run.map(str::to_string),
        tool: "write".into(),
        payload: Value::Null,
        tool_use_id: None,
        thread_id: None,
        origin: None,
    }
}

async fn write(
    env: &Env,
    run: Option<&str>,
    target: &str,
    mode: ChangeMode,
    payload: Value,
) -> Result<String, String> {
    let item = ChangeItem {
        target: target.into(),
        mode,
        payload: Some(payload),
    };
    crate::resources::mutations::dispatch_resource_change(&env.orch, &request(run), 0, &item, false)
        .await
        .map(|change| change.summary)
        .map_err(|failure| failure.error)
}
