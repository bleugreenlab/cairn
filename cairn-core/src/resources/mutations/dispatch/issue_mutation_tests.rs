use super::*;
use crate::db::DbState;
use crate::issues::comments;
use crate::issues::crud as issue_crud;
use crate::models::{CommentSource, CreateComment, CreateIssue, CreateProject, IssueStatus};
use crate::orchestrator::OrchestratorBuilder;
use crate::projects::crud as project_crud;
use crate::services::testing::TestServicesBuilder;
use crate::services::RealClock;
use crate::storage::{LocalDb, MigrationRunner, PostScope, SearchIndex, TURSO_MIGRATIONS};
use std::sync::Arc;

async fn seeded_orch() -> Orchestrator {
    let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&local)
        .await
        .unwrap();
    let search =
        Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
    let db = Arc::new(DbState::new(Arc::new(local), search));
    OrchestratorBuilder::new(
        db,
        Arc::new(TestServicesBuilder::new().build()),
        tempfile::tempdir().unwrap().keep(),
    )
    .build()
}

async fn preview(orch: &Orchestrator, item: &ChangeItem) -> ResourceMutationResult<String> {
    dispatch_resource_change(orch, &request(), 0, item, true)
        .await
        .map(|change| change.summary)
}

#[tokio::test]
async fn route_preview_performs_apply_validation_without_writing() {
    let orch = seeded_orch().await;
    seed_issue(&orch).await;
    let route_path = orch.config_dir.join("routes/valid.yaml");

    let invalid = preview(
        &orch,
        &change_item(
            "cairn://routes",
            ChangeMode::Create,
            Some(serde_json::json!({"definition": {
                "name": "",
                "description": "invalid",
                "when": [{"fact": "attention"}],
                "to": {"kind": "message", "target": "cairn://p/cairn"}
            }})),
        ),
    )
    .await
    .unwrap_err();
    assert!(invalid.error.contains("route name cannot be empty"));

    let missing_target = preview(
        &orch,
        &change_item(
            "cairn://routes",
            ChangeMode::Create,
            Some(serde_json::json!({"definition": {
                "name": "Missing target",
                "description": "invalid",
                "when": [{"fact": "attention"}],
                "to": {"kind": "message", "target": "cairn://p/MISSING"}
            }})),
        ),
    )
    .await
    .unwrap_err();
    assert!(missing_target.error.contains("Project not found: missing"));

    let summary = preview(
        &orch,
        &change_item(
            "cairn://routes",
            ChangeMode::Create,
            Some(serde_json::json!({"definition": {
                "name": "Valid",
                "description": "valid",
                "when": [{"fact": "attention"}],
                "to": {"kind": "message", "target": "cairn://p/cairn"}
            }})),
        ),
    )
    .await
    .unwrap();
    assert_eq!(summary, "Would create route 'valid'");
    assert!(
        !route_path.exists(),
        "preview must not write the route file"
    );
}

#[tokio::test]
async fn route_preview_resolves_response_bindings_and_issue_recipes() {
    let orch = seeded_orch().await;
    std::fs::create_dir_all(orch.config_dir.join("responses")).unwrap();
    std::fs::write(
        orch.config_dir.join("responses/summarize.md"),
        "---\nname: Summarize\ndescription: test\nvariables:\n  - name: input\n    required: true\n---\n{{input}}",
    )
    .unwrap();

    let bad_field = preview(
        &orch,
        &change_item(
            "cairn://routes",
            ChangeMode::Create,
            Some(serde_json::json!({"definition": {
                "name": "Bad field",
                "description": "invalid",
                "when": [{"fact": "attention"}, {"fact": "thread_stream"}],
                "transforms": [{"response": "summarize", "args": {"input": {"field": "attention"}}}],
                "to": {"kind": "channel", "register": "notify"}
            }})),
        ),
    )
    .await
    .unwrap_err();
    assert!(bad_field
        .error
        .contains("not available from every fact source"));

    let missing_variable = preview(
        &orch,
        &change_item(
            "cairn://routes",
            ChangeMode::Create,
            Some(serde_json::json!({"definition": {
                "name": "Missing variable",
                "description": "invalid",
                "when": [{"fact": "attention"}],
                "transforms": [{"response": "summarize"}],
                "to": {"kind": "channel", "register": "notify"}
            }})),
        ),
    )
    .await
    .unwrap_err();
    assert!(missing_variable
        .error
        .contains("Missing required variable 'input'"));

    let missing_recipe = preview(
        &orch,
        &change_item(
            "cairn://routes",
            ChangeMode::Create,
            Some(serde_json::json!({"definition": {
                "name": "Missing recipe",
                "description": "invalid",
                "when": [{"fact": "attention"}],
                "to": {"kind": "issue", "labels": ["bug"], "recipe": "missing"}
            }})),
        ),
    )
    .await
    .unwrap_err();
    assert!(missing_recipe.error.contains("Unknown recipe 'missing'"));
}

#[tokio::test]
async fn merged_issue_refuses_channel_messages() {
    let orch = seeded_orch().await;
    let (_, number) = seed_issue(&orch).await;
    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"status": "merged"})),
        ),
    )
    .await
    .unwrap();

    let error = apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{number}/messages"),
            ChangeMode::Append,
            Some(serde_json::json!({"content": "too late"})),
        ),
    )
    .await
    .unwrap_err();
    assert!(error.error.contains("is terminal (merged)"), "{error:?}");
}

#[tokio::test]
async fn agent_on_merged_issue_refuses_direct_messages() {
    let orch = seeded_orch().await;
    let (_, number) = seed_issue(&orch).await;
    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"status": "merged"})),
        ),
    )
    .await
    .unwrap();

    let error = apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{number}/1/builder/messages"),
            ChangeMode::Append,
            Some(serde_json::json!({"content": "too late"})),
        ),
    )
    .await
    .unwrap_err();
    assert!(error.error.contains("is terminal (merged)"), "{error:?}");
}

#[tokio::test]
async fn checks_alias_mute_and_unmute_use_the_canonical_condition_source() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    orch.db
        .local
        .execute_script(&format!(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               SELECT 'exec-checks','recipe',id,project_id,'running',1,1 FROM issues WHERE id='{issue_id}';
             INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment, node_name, branch, created_at, updated_at)
               SELECT 'job-checks','exec-checks',project_id,id,'running','builder','builder','agent/test',1,1
               FROM issues WHERE id='{issue_id}';"
        ))
        .await
        .unwrap();
    let target = format!("cairn://p/cairn/{number}/1/builder/wakes");
    apply(
        &orch,
        &change_item(
            &target,
            ChangeMode::Append,
            Some(serde_json::json!({"mute":{"kind":"checks"}})),
        ),
    )
    .await
    .unwrap();

    let subscriptions =
        crate::orchestrator::wakes::list_subscriptions_for_job(&orch.db.local, "job-checks")
            .await
            .unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].source_kind, "condition");
    assert_eq!(
        subscriptions[0].source_ref.as_deref(),
        Some(format!("cairn://p/cairn/{number}/1/builder/checks").as_str())
    );

    apply(
        &orch,
        &change_item(
            &target,
            ChangeMode::Patch,
            Some(serde_json::json!({"unmute":{"kind":"checks"}})),
        ),
    )
    .await
    .unwrap();
    let subscriptions =
        crate::orchestrator::wakes::list_subscriptions_for_job(&orch.db.local, "job-checks")
            .await
            .unwrap();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(
        subscriptions[0].state,
        crate::orchestrator::wakes::WakeSubscriptionState::Active
    );
    let checks_uri = format!("cairn://p/cairn/{number}/1/builder/checks");
    let (_, effective_wake) = crate::orchestrator::attention_push::push_with_fingerprint(
        &orch.db.local,
        "job-checks",
        &checks_uri,
        crate::orchestrator::attention_push::Wake::Wake,
        crate::orchestrator::attention_push::Boundary::Event,
        &format!("turn-checks:{checks_uri}"),
        Some("state"),
    )
    .await
    .unwrap();
    assert_eq!(
        effective_wake,
        crate::orchestrator::attention_push::Wake::Wake
    );
}

/// Create a `CAIRN` project plus one issue; returns the issue id and number.
async fn seed_issue(orch: &Orchestrator) -> (String, i32) {
    let clock = RealClock;
    let repo_path = tempfile::tempdir()
        .unwrap()
        .keep()
        .to_string_lossy()
        .to_string();
    let project = project_crud::create_db(
        &orch.db.local,
        &clock,
        &CreateProject {
            id: None,
            name: "Cairn".to_string(),
            key: "cairn".to_string(),
            repo_path,
            team_id: None,
        },
    )
    .await
    .unwrap();
    let issue = issue_crud::create(
        &orch.db.local,
        &clock,
        CreateIssue {
            project_id: project.id.clone(),
            title: "Test issue".to_string(),
            description: Some("body".to_string()),
            backend_override: None,
            label_ids: None,
        },
        issue_crud::installation_machine_authorship("test-installation", 1).unwrap(),
    )
    .await
    .unwrap();
    (issue.id, issue.number)
}

fn request() -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    }
}

async fn seed_comment(
    orch: &Orchestrator,
    issue_id: &str,
    content: &str,
) -> crate::models::Comment {
    comments::create(
        &orch.db.local,
        &RealClock,
        CreateComment {
            issue_id: issue_id.to_string(),
            content: content.to_string(),
            source: CommentSource::User,
        },
    )
    .await
    .unwrap()
}

/// The label refs an issue write carries are a vocabulary the write may extend:
/// naming a label nobody has created yet creates it rather than failing the
/// whole change (CAIRN-3100).
#[tokio::test]
async fn creating_an_issue_with_an_unknown_label_creates_the_label() {
    let orch = seeded_orch().await;
    let (_, _, run_id) = seed_running_node(&orch).await;

    let item = change_item(
        "cairn://p/cairn/issues",
        ChangeMode::Append,
        Some(serde_json::json!({
            "title": "Labelled issue",
            "labels": ["execution-fabric"],
        })),
    );
    apply_as_run(&orch, &item, &run_id).await.unwrap();

    let vocabulary = crate::labels::crud::list_labels(&orch.db.local)
        .await
        .unwrap();
    assert_eq!(
        vocabulary.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        vec!["execution-fabric"]
    );

    let rendered = crate::resources::issue::read_issue(&orch.db.local, "cairn", 2).await;
    assert!(rendered.contains("execution-fabric"), "in: {rendered}");
}

#[tokio::test]
async fn patching_an_issue_with_an_unknown_label_creates_the_label() {
    let orch = seeded_orch().await;
    let (_issue_id, number) = seed_issue(&orch).await;

    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"labels": ["Execution Fabric", "urgent"]})),
    );
    apply(&orch, &item).await.unwrap();

    let vocabulary = crate::labels::crud::list_labels(&orch.db.local)
        .await
        .unwrap();
    assert_eq!(
        vocabulary.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        vec!["execution-fabric", "urgent"]
    );

    // Re-attaching by the slug of a label created from prose reuses that label
    // instead of minting a second row for the same words.
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"labels": ["execution-fabric"]})),
    );
    apply(&orch, &item).await.unwrap();
    assert_eq!(
        crate::labels::crud::list_labels(&orch.db.local)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn comments_get_sequential_per_issue_seqs() {
    let orch = seeded_orch().await;
    let (issue_id, _number) = seed_issue(&orch).await;
    let c1 = seed_comment(&orch, &issue_id, "first").await;
    let c2 = seed_comment(&orch, &issue_id, "second").await;
    let c3 = seed_comment(&orch, &issue_id, "third").await;
    assert_eq!((c1.seq, c2.seq, c3.seq), (1, 2, 3));
}

#[tokio::test]
async fn read_collection_lists_comments_with_seq_source_and_content() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let c1 = seed_comment(&orch, &issue_id, "first comment").await;
    let _c2 = seed_comment(&orch, &issue_id, "second comment").await;
    let rendered =
        crate::resources::issue::read_issue_comments(&orch.db.local, "cairn", number).await;
    assert!(rendered.contains("### comment 1"), "in: {rendered}");
    assert!(rendered.contains("### comment 2"), "in: {rendered}");
    assert!(rendered.contains("first comment"));
    assert!(rendered.contains("second comment"));
    assert!(rendered.contains("[user]"));
    assert!(rendered.contains("2 comment(s)"));
    // The raw UUID must NOT be surfaced as the comment identifier.
    assert!(!rendered.contains(&c1.id), "uuid leaked into: {rendered}");
    // Each comment surfaces its addressable member URI so edit/delete are
    // discoverable from the collection view.
    assert!(
        rendered.contains(&format!("cairn://p/cairn/{number}/comments/1")),
        "missing member URI in: {rendered}"
    );
    assert!(rendered.contains("edit/delete:"), "in: {rendered}");
}

#[test]
fn comments_collection_affordance_advertises_edit_and_delete() {
    let block = crate::resources::common::affordance_for_kind(
        cairn_common::contract::ResourceKind::IssueComments,
    );
    assert!(block.contains("edit comment"), "block: {block}");
    assert!(block.contains("delete comment"), "block: {block}");
}

#[tokio::test]
async fn edit_comment_by_seq_updates_only_that_comment() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let c1 = seed_comment(&orch, &issue_id, "first").await;
    let c2 = seed_comment(&orch, &issue_id, "second").await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/comments/{}", c1.seq),
        ChangeMode::Patch,
        Some(serde_json::json!({"content": "edited"})),
    );
    apply(&orch, &item).await.unwrap();
    let listed = comments::list(&orch.db.local, &issue_id).await.unwrap();
    assert_eq!(
        listed.iter().find(|c| c.id == c1.id).unwrap().content,
        "edited"
    );
    assert_eq!(
        listed.iter().find(|c| c.id == c2.id).unwrap().content,
        "second"
    );
}

#[tokio::test]
async fn delete_comment_by_seq_removes_only_that_comment() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let c1 = seed_comment(&orch, &issue_id, "first").await;
    let c2 = seed_comment(&orch, &issue_id, "second").await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/comments/{}", c1.seq),
        ChangeMode::Delete,
        None,
    );
    apply(&orch, &item).await.unwrap();
    let listed = comments::list(&orch.db.local, &issue_id).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, c2.id);
}

#[tokio::test]
async fn edit_missing_comment_seq_is_clean_not_found() {
    let orch = seeded_orch().await;
    let (_issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/comments/999"),
        ChangeMode::Patch,
        Some(serde_json::json!({"content": "edited"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("not found"), "got: {}", err.error);
}

#[tokio::test]
async fn delete_missing_comment_seq_is_clean_not_found() {
    let orch = seeded_orch().await;
    let (_issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/comments/999"),
        ChangeMode::Delete,
        None,
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("not found"), "got: {}", err.error);
}

#[tokio::test]
async fn issue_uri_append_still_creates_a_comment() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Append,
        Some(serde_json::json!({"content": "a fresh comment"})),
    );
    apply(&orch, &item).await.unwrap();
    let listed = comments::list(&orch.db.local, &issue_id).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].content, "a fresh comment");
}

fn change_item(target: &str, mode: ChangeMode, payload: Option<serde_json::Value>) -> ChangeItem {
    ChangeItem {
        target: target.to_string(),
        mode,
        payload,
    }
}

async fn apply(orch: &Orchestrator, item: &ChangeItem) -> ResourceMutationResult<String> {
    dispatch_resource_change(orch, &request(), 0, item, false)
        .await
        .map(|change| change.summary)
}

/// Apply a change as if it came from `run_id`, so re-parenting records the
/// caller's root job in `parent_job_id`.
async fn apply_as_run(
    orch: &Orchestrator,
    item: &ChangeItem,
    run_id: &str,
) -> ResourceMutationResult<String> {
    let req = McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    };
    dispatch_resource_change(orch, &req, 0, item, false)
        .await
        .map(|change| change.summary)
}

/// Create an extra issue in `project_id`; returns (issue id, number).
async fn add_issue(orch: &Orchestrator, project_id: &str, title: &str) -> (String, i32) {
    let issue = issue_crud::create(
        &orch.db.local,
        &RealClock,
        CreateIssue {
            project_id: project_id.to_string(),
            title: title.to_string(),
            description: None,
            backend_override: None,
            label_ids: None,
        },
        issue_crud::installation_machine_authorship("test-installation", 1).unwrap(),
    )
    .await
    .unwrap();
    (issue.id, issue.number)
}

async fn project_id_of(orch: &Orchestrator, issue_id: &str) -> String {
    issue_crud::get(&orch.db.local, issue_id)
        .await
        .unwrap()
        .unwrap()
        .project_id
}

async fn parent_issue_id_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
    issue_crud::get(&orch.db.local, issue_id)
        .await
        .unwrap()
        .unwrap()
        .parent_issue_id
}

async fn parent_thread_id_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
    issue_crud::get(&orch.db.local, issue_id)
        .await
        .unwrap()
        .unwrap()
        .parent_thread_id
}

/// Seed a thread in `project_id`; returns its id. `migrated_from` gives it the
/// issue number the thread cutover vacated, which stays a valid address for it.
async fn add_thread(
    orch: &Orchestrator,
    project_id: &str,
    name: &str,
    migrated_from: Option<i32>,
) -> String {
    let id = format!("t-{name}");
    let migrated = match migrated_from {
        Some(number) => number.to_string(),
        None => "NULL".to_string(),
    };
    exec_sql(
        orch,
        format!(
            "INSERT INTO threads (id, project_id, name, status, attention, \
             migrated_from_number, created_at, updated_at) \
             VALUES ('{id}', '{project_id}', '{name}', 'active', 'none', {migrated}, 1, 1)"
        ),
    )
    .await;
    id
}

/// The node recorded as having filed this issue, which is null for every issue
/// nobody filed under an issue parent.
async fn parent_job_id_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
    orch.db
        .local
        .query_all(
            "SELECT parent_job_id FROM issues WHERE id = ?1",
            (issue_id.to_string(),),
            |row| crate::storage::RowExt::opt_text(row, 0),
        )
        .await
        .unwrap()
        .pop()
        .flatten()
}

/// Seed a thread in `project_id` together with its session job and a live run
/// on that session — the shape an agent actually holds a thread in. Returns
/// (thread id, session job id, run id).
async fn seed_thread_session_run(
    orch: &Orchestrator,
    project_id: &str,
    name: &str,
) -> (String, String, String) {
    let thread_id = add_thread(orch, project_id, name, None).await;
    let session = crate::threads::ensure_thread_session(&orch.db.local, &thread_id)
        .await
        .unwrap();
    let run_id = format!("run-{name}");
    exec_sql(
        orch,
        format!(
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) \
             VALUES ('{run_id}', '{project_id}', '{session}', 'live', 1, 1)"
        ),
    )
    .await;
    (thread_id, session, run_id)
}

/// Append an issue to `CAIRN`'s collection as `run_id` would, carrying `parent`
/// only when one is given. Returns the created issue's id and number.
async fn create_issue_as_run(
    orch: &Orchestrator,
    run_id: &str,
    title: &str,
    parent: Option<&str>,
) -> (String, i32) {
    let mut payload = serde_json::json!({"title": title});
    if let Some(parent) = parent {
        payload["parent"] = serde_json::json!(parent);
    }
    apply_as_run(
        orch,
        &change_item("cairn://p/cairn/issues", ChangeMode::Append, Some(payload)),
        run_id,
    )
    .await
    .unwrap();
    let mut found = orch
        .db
        .local
        .query_all(
            "SELECT id, number FROM issues WHERE title = ?1",
            (title.to_string(),),
            |row| {
                Ok((
                    crate::storage::RowExt::text(row, 0)?,
                    crate::storage::RowExt::opt_i64(row, 1)?.unwrap_or_default() as i32,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1, "expected exactly one issue titled {title}");
    found.pop().unwrap()
}

/// The branch a child would inherit from its current parent, straight from the
/// resolver every execution launch consults.
async fn parent_branch_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
    let issue_id = issue_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                crate::issues::relations::resolve_parent_branch(conn, &issue_id).await
            })
        })
        .await
        .unwrap()
}

/// Run a SQL statement against the local db in a test.
async fn exec_sql(orch: &Orchestrator, sql: String) {
    orch.db
        .local
        .write(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                conn.execute(&sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// Read a single run's status string.
async fn run_status_for(orch: &Orchestrator, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT status FROM runs WHERE id = ?1", (run_id.as_str(),))
                    .await?;
                crate::storage::next_opt_text(&mut rows, 0).await
            })
        })
        .await
        .unwrap()
}

/// Seed a `CAIRN` project + issue + execution (seq 1) + agent job
/// (uri_segment `builder`) + a `live` run. Returns (issue number, job id,
/// run id).
async fn seed_running_node(orch: &Orchestrator) -> (i32, String, String) {
    let (issue_id, number) = seed_issue(orch).await;
    // The project created by seed_issue keys on CAIRN; recover its id from
    // the issue so the execution/job/run FKs all resolve.
    let issue_id_for_lookup = issue_id.clone();
    let project_id = orch
        .db
        .local
        .read(move |conn| {
            let issue_id = issue_id_for_lookup.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT project_id FROM issues WHERE id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                crate::storage::next_opt_text(&mut rows, 0).await
            })
        })
        .await
        .unwrap()
        .unwrap();

    exec_sql(
        orch,
        format!(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) \
                 VALUES ('exec-stop', 'recipe', '{issue_id}', '{project_id}', 'running', 1, 1)"
        ),
    )
    .await;
    exec_sql(
            orch,
            format!(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch) \
                 VALUES ('job-stop', 'exec-stop', '{issue_id}', '{project_id}', 'Builder', 'running', 1, 1, 'builder', 'agent/builder')"
            ),
        )
        .await;
    exec_sql(
        orch,
        format!(
            "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at) \
                 VALUES ('run-stop', '{issue_id}', '{project_id}', 'job-stop', 'live', 1, 1)"
        ),
    )
    .await;

    (number, "job-stop".to_string(), "run-stop".to_string())
}

#[tokio::test]
async fn node_patch_stop_interrupts_live_run() {
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/1/builder"),
        ChangeMode::Patch,
        Some(serde_json::json!({"action": "stop"})),
    );
    let summary = apply(&orch, &item).await.unwrap();
    assert!(
        summary.contains("Stopped") && summary.contains(&run_id),
        "got: {summary}"
    );
    // With no live backend process registered, stop's warm-park fallback
    // finalizes the stale live run off the active set, so it is no longer
    // 'live' — evidence the stop path actually ran against the resolved run.
    let status = run_status_for(&orch, &run_id).await;
    assert_ne!(
        status.as_deref(),
        Some("live"),
        "run should no longer be live"
    );
}

#[tokio::test]
async fn node_patch_stop_without_live_run_idles_nonterminal_job() {
    // A non-terminal job with no live run (suspended/waiting — e.g. an
    // OpenRouter agent that finalized its run on a foreground question) is
    // idled at the job level rather than rejected (CAIRN-1907).
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    // No live run: mark the only run exited first. The job stays 'running'.
    exec_sql(
        &orch,
        format!("UPDATE runs SET status = 'exited' WHERE id = '{run_id}'"),
    )
    .await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/1/builder"),
        ChangeMode::Patch,
        Some(serde_json::json!({"action": "stop"})),
    );
    let summary = apply(&orch, &item).await.unwrap();
    assert!(
        summary.contains("Stopped") && summary.contains("idled"),
        "got: {summary}"
    );
}

#[tokio::test]
async fn node_patch_stop_terminal_job_reports_no_active_run() {
    // A genuinely terminal job with no live run has nothing to stop.
    let orch = seeded_orch().await;
    let (number, job_id, run_id) = seed_running_node(&orch).await;
    exec_sql(
        &orch,
        format!("UPDATE runs SET status = 'exited' WHERE id = '{run_id}'"),
    )
    .await;
    exec_sql(
        &orch,
        format!("UPDATE jobs SET status = 'complete' WHERE id = '{job_id}'"),
    )
    .await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/1/builder"),
        ChangeMode::Patch,
        Some(serde_json::json!({"action": "stop"})),
    );
    let summary = apply(&orch, &item).await.unwrap();
    assert!(summary.contains("no active run"), "got: {summary}");
}

#[tokio::test]
async fn node_patch_merge_without_pr_still_errors() {
    // Reordering stop before the PR gate must not regress the PR-action
    // path: merge/close/refresh on a node with no merge_requests row still
    // returns the 'no PR yet' error.
    let orch = seeded_orch().await;
    let (number, _job_id, _run_id) = seed_running_node(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/1/builder"),
        ChangeMode::Patch,
        Some(serde_json::json!({"action": "merge"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("no PR yet"), "got: {}", err.error);
}

#[tokio::test]
async fn node_patch_stop_dry_run_describes_without_stopping() {
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}/1/builder"),
        ChangeMode::Patch,
        Some(serde_json::json!({"action": "stop"})),
    );
    let change = dispatch_resource_change(&orch, &request(), 0, &item, true)
        .await
        .unwrap();
    assert!(
        change.summary.contains("Would stop"),
        "got: {}",
        change.summary
    );
    // A dry run leaves the run untouched.
    assert_eq!(
        run_status_for(&orch, &run_id).await.as_deref(),
        Some("live")
    );
}

#[tokio::test]
async fn patch_status_closed_resolves_issue() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"status": "closed"})),
    );
    apply(&orch, &item).await.unwrap();
    let issue = issue_crud::get(&orch.db.local, &issue_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(issue.status, IssueStatus::Closed);
    assert!(issue.closed_at.is_some());
}

#[tokio::test]
async fn patch_status_merged_resolves_issue() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"status": "merged"})),
    );
    apply(&orch, &item).await.unwrap();
    let issue = issue_crud::get(&orch.db.local, &issue_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(issue.status, IssueStatus::Merged);
    assert!(issue.merged_at.is_some());
}

/// Read an issue by its project-scoped number.
async fn issue_by_number(orch: &Orchestrator, number: i32) -> crate::models::Issue {
    let id = orch
        .db
        .local
        .read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT id FROM issues WHERE number = ?1", (number,))
                    .await?;
                crate::storage::next_opt_text(&mut rows, 0).await
            })
        })
        .await
        .unwrap()
        .unwrap();
    issue_crud::get(&orch.db.local, &id).await.unwrap().unwrap()
}

/// Live work turns a close into a confirmation: the first attempt changes
/// nothing and comes back naming both the work and the key that confirms it.
#[tokio::test]
async fn patch_status_closed_with_live_work_asks_to_confirm() {
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"status": "closed"})),
    );

    let err = apply(&orch, &item).await.unwrap_err();

    assert!(
        err.error.contains("confirm: true"),
        "names the confirming key: {}",
        err.error
    );
    assert!(
        err.error.contains("Builder (running now)"),
        "names the live work and its state: {}",
        err.error
    );
    assert_eq!(
        issue_by_number(&orch, number).await.status,
        IssueStatus::Backlog
    );
    assert_eq!(
        run_status_for(&orch, &run_id).await.as_deref(),
        Some("live")
    );
}

/// A refusal has to be true of the whole write. A combined patch that renames
/// the issue and closes it must leave the title alone when the close is refused,
/// or a caller retrying "the same write" repeats the rename.
#[tokio::test]
async fn a_refused_resolution_leaves_the_rest_of_the_patch_unapplied() {
    let orch = seeded_orch().await;
    let (number, _job_id, _run_id) = seed_running_node(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "Renamed", "status": "closed"})),
    );

    let err = apply(&orch, &item).await.unwrap_err();

    assert!(err.error.contains("confirm: true"), "{}", err.error);
    let issue = issue_by_number(&orch, number).await;
    assert_eq!(
        issue.title, "Test issue",
        "a refused write applies none of its fields"
    );
    assert_eq!(issue.status, IssueStatus::Backlog);
}

/// The confirmed close resolves the issue and stops the work it named.
#[tokio::test]
async fn patch_status_closed_with_confirm_resolves_and_stops_live_work() {
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"status": "closed", "confirm": true})),
    );

    apply(&orch, &item).await.unwrap();

    let issue = issue_by_number(&orch, number).await;
    assert_eq!(issue.status, IssueStatus::Closed);
    assert_ne!(
        run_status_for(&orch, &run_id).await.as_deref(),
        Some("live"),
        "the confirmed close stops the run it named"
    );
}

/// `confirm` answers a resolution; on its own it is a typo worth naming.
#[tokio::test]
async fn patch_confirm_without_a_resolution_is_rejected() {
    let orch = seeded_orch().await;
    let (_issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "Renamed", "confirm": true})),
    );

    let err = apply(&orch, &item).await.unwrap_err();

    assert!(
        err.error.contains("payload.status"),
        "points at the key confirm belongs with: {}",
        err.error
    );
}

#[tokio::test]
async fn patch_invalid_status_is_rejected() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    // `backlog` is derived, not settable, and must be rejected alongside any
    // other unknown value. Each is paired with a title so the refusal is shown
    // to leave the rest of the write unapplied too.
    for bad in ["backlog", "active", "frobnicate"] {
        let item = change_item(
            &format!("cairn://p/cairn/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"title": "Renamed", "status": bad})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert_eq!(
            issue_by_number(&orch, number).await.title,
            "Test issue",
            "a refused status must not apply the rest of the patch"
        );
        assert!(
            err.error.contains("merged") && err.error.contains("closed"),
            "expected allowed-set message, got: {}",
            err.error
        );
    }
    // The issue was never resolved by a rejected patch.
    let issue = issue_crud::get(&orch.db.local, &issue_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(issue.status, IssueStatus::Backlog);
}

#[tokio::test]
async fn patch_title_leaves_status_untouched() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"title": "Renamed"})),
    );
    apply(&orch, &item).await.unwrap();
    let issue = issue_crud::get(&orch.db.local, &issue_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(issue.title, "Renamed");
    assert_eq!(issue.status, IssueStatus::Backlog);
}

#[tokio::test]
async fn patch_parent_adopts_issue() {
    let orch = seeded_orch().await;
    let (child_id, child_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &child_id).await;
    let (parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/cairn/{parent_num}")})),
    );
    apply(&orch, &item).await.unwrap();
    assert_eq!(
        parent_issue_id_of(&orch, &child_id).await.as_deref(),
        Some(parent_id.as_str())
    );
}

#[tokio::test]
async fn patch_parent_hands_child_attention_to_the_parents_coordinator() {
    let orch = seeded_orch().await;
    let (number, job_id, run_id) = seed_running_node(&orch).await;
    let running_issue =
        crate::issues::relations::issue_id_for_project_number(&orch.db.local, "cairn", number)
            .await
            .unwrap()
            .unwrap();
    let project_id = project_id_of(&orch, &running_issue).await;
    let (parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
    let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/cairn/{parent_num}")})),
    );
    apply_as_run(&orch, &item, &run_id).await.unwrap();
    assert_eq!(
        parent_issue_id_of(&orch, &child_id).await.as_deref(),
        Some(parent_id.as_str())
    );

    // The adopting run's job coordinates the *running* issue, not the newly
    // adopted parent, so it gains nothing. The child's attention is addressed by
    // the parent edge alone, and this parent has no execution on it yet.
    assert!(
        crate::orchestrator::wakes::watcher_jobs_for_issue(
            &orch.db.local,
            &format!("cairn://p/cairn/{child_num}")
        )
        .await
        .unwrap()
        .is_empty(),
        "adoption alone does not name a coordinator; the parent issue must have one"
    );

    // Re-point the caller's job at the parent issue and give it a session — the
    // shape of a coordinator that has actually run on the parent. The child's
    // attention follows with no mint.
    exec_sql(
        &orch,
        format!(
            "UPDATE jobs SET issue_id = '{parent_id}', current_session_id = 'sess' \
             WHERE id = '{job_id}'"
        ),
    )
    .await;
    assert_eq!(
        crate::orchestrator::wakes::watcher_jobs_for_issue(
            &orch.db.local,
            &format!("cairn://p/cairn/{child_num}")
        )
        .await
        .unwrap(),
        vec![job_id]
    );
}

#[tokio::test]
async fn patch_parent_null_orphans_issue() {
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    let running_issue =
        crate::issues::relations::issue_id_for_project_number(&orch.db.local, "cairn", number)
            .await
            .unwrap()
            .unwrap();
    let project_id = project_id_of(&orch, &running_issue).await;
    let (_parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
    let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;
    let adopt = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/cairn/{parent_num}")})),
    );
    apply_as_run(&orch, &adopt, &run_id).await.unwrap();
    assert!(parent_issue_id_of(&orch, &child_id).await.is_some());

    let orphan = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": serde_json::Value::Null})),
    );
    apply(&orch, &orphan).await.unwrap();
    assert!(parent_issue_id_of(&orch, &child_id).await.is_none());
}

#[tokio::test]
async fn patch_parent_self_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/cairn/{child_num}")})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("its own parent"), "got: {}", err.error);
}

#[tokio::test]
async fn patch_parent_unknown_uri_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": "cairn://p/cairn/9999"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(
        err.error.contains("parent issue or thread not found"),
        "got: {}",
        err.error
    );
}

/// The operation this whole seam exists for: a thread claiming work that was
/// filed without it. The patch takes the canonical thread URI, the edge lands in
/// `parent_thread_id` alone, and the thread's census shows the issue as its own.
#[tokio::test]
async fn patch_parent_adopts_issue_under_a_thread() {
    let orch = seeded_orch().await;
    let (child_id, child_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &child_id).await;
    let thread_id = add_thread(&orch, &project_id, "thread-ux", None).await;

    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn://p/cairn/thread-ux"})),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        parent_thread_id_of(&orch, &child_id).await.as_deref(),
        Some(thread_id.as_str())
    );
    assert_eq!(parent_issue_id_of(&orch, &child_id).await, None);

    let overview = crate::resources::read::produce_cairn_resource(
        &orch,
        &request(),
        "cairn://p/cairn/thread-ux",
    )
    .await;
    assert!(
        overview
            .content
            .contains(&format!("cairn://p/cairn/{child_num}")),
        "the thread's census should list the adopted issue: {}",
        overview.content
    );

    // ...and the issue says whose it is, at the address an operator confirming
    // the adoption actually reads.
    let child = crate::resources::read::produce_cairn_resource(
        &orch,
        &request(),
        &format!("cairn://p/cairn/{child_num}"),
    )
    .await;
    assert!(
        child
            .content
            .contains("Parent: `cairn://p/cairn/thread-ux`"),
        "the adopted issue should name its thread parent: {}",
        child.content
    );
}

/// Patch resolves a thread by every address create does, including the number
/// the thread cutover vacated — both paths ask the same resolver.
#[tokio::test]
async fn patch_parent_accepts_a_migrated_thread_number() {
    let orch = seeded_orch().await;
    let (child_id, child_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &child_id).await;
    let thread_id = add_thread(&orch, &project_id, "general", Some(3404)).await;

    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn://p/cairn/3404"})),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        parent_thread_id_of(&orch, &child_id).await.as_deref(),
        Some(thread_id.as_str())
    );
}

/// An adopted child's attention reaches the thread's live session, and it gets
/// there by the parent edge alone — adoption mints no subscription row.
#[tokio::test]
async fn an_adopted_child_wakes_the_thread_without_minting_a_subscription() {
    let orch = seeded_orch().await;
    let (child_id, child_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &child_id).await;
    let thread_id = add_thread(&orch, &project_id, "thread-ux", None).await;
    let session = crate::threads::ensure_thread_session(&orch.db.local, &thread_id)
        .await
        .unwrap();

    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn://p/cairn/thread-ux"})),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        crate::orchestrator::wakes::watcher_jobs_for_issue(
            &orch.db.local,
            &format!("cairn://p/cairn/{child_num}")
        )
        .await
        .unwrap(),
        vec![session.clone()]
    );
    let subscriptions: Vec<String> = orch
        .db
        .local
        .query_all(
            "SELECT job_id FROM wake_subscriptions WHERE source_kind = 'issue' AND source_ref = ?1",
            (format!("cairn://p/cairn/{child_num}"),),
            |row| crate::storage::RowExt::text(row, 0),
        )
        .await
        .unwrap();
    assert!(
        subscriptions.is_empty(),
        "adoption routes by the edge, not by a minted row: {subscriptions:?}"
    );
    // The child issue is also what the thread's session sees itself watching.
    assert_eq!(
        crate::orchestrator::wakes::coordinated_child_issue_uris_for_job(&orch.db.local, &session)
            .await
            .unwrap(),
        vec![format!("cairn://p/cairn/{child_num}")]
    );
}

/// An issue an agent files while holding a thread belongs to that thread. The
/// payload names no parent; the edge lands anyway, and it lands as a thread
/// edge — routing attention back to the session that decided on the work,
/// carrying neither an issue parent nor a spawning job.
#[tokio::test]
async fn an_issue_filed_from_a_thread_defaults_to_that_thread() {
    let orch = seeded_orch().await;
    let (seed_id, _) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &seed_id).await;
    let (thread_id, session, run_id) =
        seed_thread_session_run(&orch, &project_id, "thread-ux").await;

    let (child_id, child_num) =
        create_issue_as_run(&orch, &run_id, "Filed by the thread", None).await;

    assert_eq!(
        parent_thread_id_of(&orch, &child_id).await.as_deref(),
        Some(thread_id.as_str())
    );
    assert_eq!(parent_issue_id_of(&orch, &child_id).await, None);
    assert_eq!(
        parent_job_id_of(&orch, &child_id).await,
        None,
        "a thread edge routes attention and confers no branch ancestry, so nothing\
         records a spawning node"
    );

    let overview = crate::resources::read::produce_cairn_resource(
        &orch,
        &request(),
        "cairn://p/cairn/thread-ux",
    )
    .await;
    assert!(
        overview
            .content
            .contains(&format!("cairn://p/cairn/{child_num}")),
        "the thread's census should list the issue it filed: {}",
        overview.content
    );

    // Attention reaches the thread's live session by the parent edge alone, the
    // same way an adopted child's does — creation mints no subscription either.
    let child_uri = format!("cairn://p/cairn/{child_num}");
    assert_eq!(
        crate::orchestrator::wakes::watcher_jobs_for_issue(&orch.db.local, &child_uri)
            .await
            .unwrap(),
        vec![session.clone()]
    );
    let subscriptions: Vec<String> = orch
        .db
        .local
        .query_all(
            "SELECT job_id FROM wake_subscriptions WHERE source_kind = 'issue' AND source_ref = ?1",
            (child_uri.clone(),),
            |row| crate::storage::RowExt::text(row, 0),
        )
        .await
        .unwrap();
    assert!(
        subscriptions.is_empty(),
        "the edge routes the child; nothing is minted: {subscriptions:?}"
    );
}

/// A sub-agent a thread delegated to is acting for the thread, so what it files
/// is the thread's too. The task job here carries no thread id of its own, so
/// the answer can only come from the owning-node walk.
#[tokio::test]
async fn a_subagent_of_a_thread_files_for_that_thread() {
    let orch = seeded_orch().await;
    let (seed_id, _) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &seed_id).await;
    let (thread_id, session, _run_id) =
        seed_thread_session_run(&orch, &project_id, "thread-ux").await;
    exec_sql(
        &orch,
        format!(
            "INSERT INTO jobs(id, parent_job_id, project_id, status, node_name, uri_segment, \
             created_at, updated_at) \
             VALUES ('thread-task', '{session}', '{project_id}', 'running', 'Explore', 'explore', 2, 2)"
        ),
    )
    .await;
    exec_sql(
        &orch,
        format!(
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) \
             VALUES ('run-thread-task', '{project_id}', 'thread-task', 'live', 2, 2)"
        ),
    )
    .await;

    let (child_id, _) =
        create_issue_as_run(&orch, "run-thread-task", "Filed by a sub-agent", None).await;

    assert_eq!(
        parent_thread_id_of(&orch, &child_id).await.as_deref(),
        Some(thread_id.as_str())
    );
}

/// The inference is a default, not a rule: a thread that names a parent gets
/// the parent it named, whether that is another thread or an issue.
#[tokio::test]
async fn an_explicit_parent_beats_the_creating_thread() {
    let orch = seeded_orch().await;
    let (seed_id, _) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &seed_id).await;
    let (_thread_id, session, run_id) =
        seed_thread_session_run(&orch, &project_id, "thread-ux").await;
    let other_thread = add_thread(&orch, &project_id, "thread-ops", None).await;
    let (parent_issue_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;

    let (to_other_thread, _) = create_issue_as_run(
        &orch,
        &run_id,
        "Handed to another thread",
        Some("cairn://p/cairn/thread-ops"),
    )
    .await;
    assert_eq!(
        parent_thread_id_of(&orch, &to_other_thread)
            .await
            .as_deref(),
        Some(other_thread.as_str())
    );

    let (to_issue, _) = create_issue_as_run(
        &orch,
        &run_id,
        "Filed under an issue",
        Some(&format!("cairn://p/cairn/{parent_num}")),
    )
    .await;
    assert_eq!(
        parent_issue_id_of(&orch, &to_issue).await.as_deref(),
        Some(parent_issue_id.as_str())
    );
    assert_eq!(
        parent_thread_id_of(&orch, &to_issue).await,
        None,
        "the two columns stay one edge"
    );
    assert_eq!(
        parent_job_id_of(&orch, &to_issue).await.as_deref(),
        Some(session.as_str()),
        "an issue parent still records the node that filed the child"
    );
}

/// Outside a thread nothing changes: agents on an issue execution create an
/// unparented issue when the payload names no parent, regardless of which
/// authenticated builder performs the write.
#[tokio::test]
async fn an_issue_filed_outside_a_thread_stays_unparented() {
    let orch = seeded_orch().await;
    let (_number, _job_id, run_id) = seed_running_node(&orch).await;

    let (from_execution, _) = create_issue_as_run(&orch, &run_id, "Filed by a builder", None).await;
    assert_eq!(parent_thread_id_of(&orch, &from_execution).await, None);
    assert_eq!(parent_issue_id_of(&orch, &from_execution).await, None);

    apply_as_run(
        &orch,
        &change_item(
            "cairn://p/cairn/issues",
            ChangeMode::Append,
            Some(serde_json::json!({"title": "Filed by another builder"})),
        ),
        &run_id,
    )
    .await
    .unwrap();
    let ambient = orch
        .db
        .local
        .query_all(
            "SELECT parent_issue_id, parent_thread_id FROM issues WHERE title = 'Filed by another builder'",
            (),
            |row| {
                Ok((
                    crate::storage::RowExt::opt_text(row, 0)?,
                    crate::storage::RowExt::opt_text(row, 1)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(ambient, vec![(None, None)]);
}

/// The inferred parent is scoped to the project that owns the issue. A thread
/// filing into another project gets no edge rather than one that would route
/// the work's attention out of the project holding it — the same rule an
/// explicitly named cross-project parent is refused by.
#[tokio::test]
async fn a_thread_filing_into_another_project_infers_no_parent() {
    let orch = seeded_orch().await;
    let (seed_id, _) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &seed_id).await;
    let (_thread_id, _session, run_id) =
        seed_thread_session_run(&orch, &project_id, "thread-ux").await;
    project_crud::create_db(
        &orch.db.local,
        &RealClock,
        &CreateProject {
            id: None,
            name: "Other".to_string(),
            key: "OTHER".to_string(),
            repo_path: tempfile::tempdir()
                .unwrap()
                .keep()
                .to_string_lossy()
                .to_string(),
            team_id: None,
        },
    )
    .await
    .unwrap();

    apply_as_run(
        &orch,
        &change_item(
            "cairn://p/OTHER/issues",
            ChangeMode::Append,
            Some(serde_json::json!({"title": "Filed across projects"})),
        ),
        &run_id,
    )
    .await
    .unwrap();

    let edges = orch
        .db
        .local
        .query_all(
            "SELECT parent_issue_id, parent_thread_id FROM issues WHERE title = 'Filed across projects'",
            (),
            |row| {
                Ok((
                    crate::storage::RowExt::opt_text(row, 0)?,
                    crate::storage::RowExt::opt_text(row, 1)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(edges, vec![(None, None)]);
}

/// Branch ancestry is the half of parenthood a thread does not carry. Moving a
/// child from an issue parent to a thread drops the inherited branch, which is
/// what puts the child's own pull request back on the project base.
#[tokio::test]
async fn adopting_under_a_thread_drops_the_inherited_branch() {
    let orch = seeded_orch().await;
    let (parent_num, _job_id, _run_id) = seed_running_node(&orch).await;
    let parent_issue =
        crate::issues::relations::issue_id_for_project_number(&orch.db.local, "cairn", parent_num)
            .await
            .unwrap()
            .unwrap();
    let project_id = project_id_of(&orch, &parent_issue).await;
    add_thread(&orch, &project_id, "thread-ux", None).await;
    let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;

    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/cairn/{parent_num}")})),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        parent_branch_of(&orch, &child_id).await.as_deref(),
        Some("agent/builder"),
        "an issue parent confers its integration branch"
    );

    apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn://p/cairn/thread-ux"})),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        parent_branch_of(&orch, &child_id).await,
        None,
        "a thread parent leaves the child on the project base"
    );
}

/// The two columns are one edge however often it is re-pointed: every step
/// leaves at most one populated, and `null` clears both.
///
/// The order walks every transition, including both direct hand-offs between
/// the two kinds. A sequence that only ever passes through `null` would leave
/// each arm's clear of the *other* column unexercised, and that clear failing is
/// silent where it hurts most: two columns set at once routes attention to the
/// new issue parent while the census still lists the old thread and
/// `resolve_parent_branch` still refuses the inherited branch.
#[tokio::test]
async fn parent_round_trips_between_thread_issue_and_none() {
    let orch = seeded_orch().await;
    let (child_id, child_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &child_id).await;
    let thread_id = add_thread(&orch, &project_id, "thread-ux", None).await;
    let (parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
    let child_uri = format!("cairn://p/cairn/{child_num}");
    let thread = serde_json::json!("cairn://p/cairn/thread-ux");
    let issue = serde_json::json!(format!("cairn://p/cairn/{parent_num}"));
    let none = serde_json::Value::Null;

    let steps: [(serde_json::Value, Option<&str>, Option<&str>); 6] = [
        // none -> thread
        (thread.clone(), None, Some(thread_id.as_str())),
        // thread -> issue, the hand-off that needs the issue arm's thread clear
        (issue.clone(), Some(parent_id.as_str()), None),
        // issue -> thread, the hand-off that needs the thread arm's issue clear
        (thread.clone(), None, Some(thread_id.as_str())),
        // thread -> none
        (none.clone(), None, None),
        // none -> issue
        (issue, Some(parent_id.as_str()), None),
        // issue -> none
        (none, None, None),
    ];
    for (parent, expected_issue, expected_thread) in steps {
        apply(
            &orch,
            &change_item(
                &child_uri,
                ChangeMode::Patch,
                Some(serde_json::json!({"parent": parent})),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            (
                parent_issue_id_of(&orch, &child_id).await.as_deref(),
                parent_thread_id_of(&orch, &child_id).await.as_deref()
            ),
            (expected_issue, expected_thread),
            "after adopting under {parent:?}"
        );
    }
}

#[tokio::test]
async fn patch_parent_unknown_thread_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": "cairn://p/cairn/thread-nobody"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(
        err.error.contains("parent issue or thread not found"),
        "got: {}",
        err.error
    );
}

#[tokio::test]
async fn patch_parent_cross_project_thread_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let repo_path = tempfile::tempdir()
        .unwrap()
        .keep()
        .to_string_lossy()
        .to_string();
    let other = project_crud::create_db(
        &orch.db.local,
        &RealClock,
        &CreateProject {
            id: None,
            name: "Agg".to_string(),
            key: "AGG".to_string(),
            repo_path,
            team_id: None,
        },
    )
    .await
    .unwrap();
    add_thread(&orch, &other.id, "thread-elsewhere", None).await;

    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": "cairn://p/AGG/thread-elsewhere"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("same project"), "got: {}", err.error);
}

/// A thread's sub-resource is not the thread, and `cairn:~/` is whoever is
/// writing rather than whoever should parent. Both are refused by shape, with
/// the accepted forms named.
#[tokio::test]
async fn patch_parent_rejects_descendant_and_home_relative_uris() {
    let orch = seeded_orch().await;
    let (child_id, child_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &child_id).await;
    add_thread(&orch, &project_id, "thread-ux", None).await;

    let descendant = apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn://p/cairn/thread-ux/chat"})),
        ),
    )
    .await
    .unwrap_err();
    assert!(
        descendant.error.contains("canonical thread URI"),
        "got: {}",
        descendant.error
    );

    let home_relative = apply(
        &orch,
        &change_item(
            &format!("cairn://p/cairn/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn:~/"})),
        ),
    )
    .await
    .unwrap_err();
    assert!(
        home_relative
            .error
            .contains("cairn:~/ resolves to the node"),
        "got: {}",
        home_relative.error
    );
    assert_eq!(parent_thread_id_of(&orch, &child_id).await, None);
}

#[tokio::test]
async fn patch_parent_cross_project_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let repo_path = tempfile::tempdir()
        .unwrap()
        .keep()
        .to_string_lossy()
        .to_string();
    let other = project_crud::create_db(
        &orch.db.local,
        &RealClock,
        &CreateProject {
            id: None,
            name: "Agg".to_string(),
            key: "AGG".to_string(),
            repo_path,
            team_id: None,
        },
    )
    .await
    .unwrap();
    let (_agg_id, agg_num) = add_issue(&orch, &other.id, "AggParent").await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/AGG/{agg_num}")})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("same project"), "got: {}", err.error);
}

#[tokio::test]
async fn patch_parent_cycle_rejected() {
    let orch = seeded_orch().await;
    let (a_id, a_num) = seed_issue(&orch).await;
    let project_id = project_id_of(&orch, &a_id).await;
    let (_b_id, b_num) = add_issue(&orch, &project_id, "B").await;
    // A adopts B as its parent.
    let adopt = change_item(
        &format!("cairn://p/cairn/{a_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/cairn/{b_num}")})),
    );
    apply(&orch, &adopt).await.unwrap();
    // Adopting B under A would close the loop A -> B -> A.
    let cycle = change_item(
        &format!("cairn://p/cairn/{b_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/cairn/{a_num}")})),
    );
    let err = apply(&orch, &cycle).await.unwrap_err();
    assert!(err.error.contains("cycle"), "got: {}", err.error);
}

#[tokio::test]
async fn patch_parent_malformed_uri_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": "not-a-uri"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("issue URI"), "got: {}", err.error);
}

#[tokio::test]
async fn delete_removes_issue() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Delete,
        None,
    );
    apply(&orch, &item).await.unwrap();
    assert!(issue_crud::get(&orch.db.local, &issue_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn delete_rejects_payload() {
    let orch = seeded_orch().await;
    let (_, number) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/cairn/{number}"),
        ChangeMode::Delete,
        Some(serde_json::json!({"force": true})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    // The contract gate owns this now, and says more than the hand-rolled check
    // it replaced: which key was not understood, and that the mutation takes
    // none at all.
    assert!(
        err.error.contains("Unknown payload key"),
        "got: {}",
        err.error
    );
    assert!(err.error.contains("`force`"), "got: {}", err.error);
    assert!(
        err.error.contains("takes no payload keys"),
        "got: {}",
        err.error
    );
}

#[tokio::test]
async fn delete_unknown_issue_errors() {
    let orch = seeded_orch().await;
    seed_issue(&orch).await;
    let item = change_item("cairn://p/cairn/9999", ChangeMode::Delete, None);
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("not found"), "got: {}", err.error);
}

/// End-to-end through `dispatch_resource_change`: a patch on
/// `.../executions/{seq}` routes to the agent-edit arm (not the parity-bug
/// catch-all) and persists the edited agent snapshot. The test caller has no
/// resolvable run, so the self-edit guard allows it.
#[tokio::test]
async fn patch_execution_agent_snapshot_updates_stored_snapshot() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    let snapshot_json = serde_json::json!({
            "recipe": {"id":"r","name":"R","description":null,"trigger":"manual","nodes":[],"edges":[]},
            "agents": {"builder": {"id":"builder","name":"Builder","description":"","prompt":"old","tools":[],"selection":{"backend":"claude","model":"sonnet"},"disallowedTools":null,"skills":null,"fence":"ask"}},
            "skills": {},
            "triggerContext": {"issueId": issue_id, "projectId":"p","triggerType":"manual"},
            "createdAt": 1
        })
        .to_string();
    let issue_id_for_insert = issue_id.clone();
    orch.db
            .local
            .write(|conn| {
                let issue_id = issue_id_for_insert.clone();
                let snapshot_json = snapshot_json.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                         VALUES ('exec-x','r',?1,(SELECT project_id FROM issues WHERE id=?1),'running',1,1,?2)",
                        (issue_id.as_str(), snapshot_json.as_str()),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

    let item = change_item(
        &format!("cairn://p/cairn/{number}/executions/1"),
        ChangeMode::Patch,
        Some(serde_json::json!({
            "agent": "builder",
            "snapshot": {"id":"builder","name":"Builder","description":"","prompt":"new","tools":[],"selection":{"backend":"claude","model":"sonnet"},"disallowedTools":null,"skills":null,"fence":"ask"}
        })),
    );
    let summary = apply(&orch, &item).await.unwrap();
    assert!(summary.contains("Edited agent 'builder'"), "got: {summary}");

    let json = orch
        .db
        .local
        .query_opt_text("SELECT snapshot FROM executions WHERE id='exec-x'", ())
        .await
        .unwrap()
        .unwrap();
    let snap = crate::config::snapshot_migrate::load(&json).unwrap();
    assert_eq!(snap.agents["builder"].prompt, "new");
}

fn dummy_value(ty: cairn_common::contract::KeyType) -> serde_json::Value {
    use cairn_common::contract::KeyType;
    match ty {
        KeyType::Str => serde_json::json!("sample"),
        KeyType::Bool => serde_json::json!(true),
        KeyType::Int => serde_json::json!(1),
        KeyType::Float => serde_json::json!(1.0),
        KeyType::Array => serde_json::json!([]),
        KeyType::Object => serde_json::json!({}),
        KeyType::Any => serde_json::Value::Null,
    }
}

/// Payload satisfying a mutation's required keys in their canonical spelling,
/// so the gate passes and dispatch reaches the real arm instead of the
/// gate's missing-key rejection.
fn required_payload(spec: &cairn_common::contract::MutationSpec) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for key in spec.required {
        map.insert(key.key.to_string(), dummy_value(key.ty));
    }
    serde_json::Value::Object(map)
}

/// A parseable sample URI for every resource kind that carries a mutation, so
/// the parity test can build a representative `CairnResource` for each
/// advertised `(kind, mode)`. Only `Mcp` is mode-sensitive — its dispatch
/// arms split on whether the URI names a server (create targets the bare
/// registry; patch/delete name one server). A kind that gains a mutation
/// without a sample here trips the explicit panic, telling the next builder
/// to add one.
fn sample_resource(kind: cairn_common::contract::ResourceKind, mode: ChangeMode) -> CairnResource {
    use cairn_common::contract::ResourceKind as K;
    let uri = match kind {
        K::Mcp => {
            if matches!(mode, ChangeMode::Create) {
                "cairn://mcp"
            } else {
                "cairn://mcp/playwright"
            }
        }
        K::Project => "cairn://p/cairn",
        K::Settings => "cairn://settings",
        K::Projects => "cairn://projects",
        K::ProjectSettings => "cairn://p/cairn/settings",
        K::ProjectIssues => "cairn://p/cairn/issues",
        K::Posts => "cairn://posts",
        K::Post => "cairn://posts/1",
        K::ProjectThreads => "cairn://p/cairn/threads",
        K::Thread => "cairn://p/cairn/design-review",
        K::ProjectMessages => "cairn://p/cairn/messages",
        K::ProjectTerminal => "cairn://p/cairn/terminal/dev",
        K::ProjectBrowser => "cairn://p/cairn/browser/main",
        K::NodeBrowser => "cairn://p/cairn/1/1/builder/browser/main",
        K::TaskBrowser => "cairn://p/cairn/1/1/builder/task/sub/browser/main",
        K::Issue => "cairn://p/cairn/1",
        K::IssueExecutions => "cairn://p/cairn/1/executions",
        K::IssueExecution => "cairn://p/cairn/1/executions/2",
        K::IssueMessages => "cairn://p/cairn/1/messages",
        K::IssueComment => "cairn://p/cairn/1/comments/1",
        K::Node => "cairn://p/cairn/1/1/builder",
        K::NodeMessages => "cairn://p/cairn/1/1/builder/messages",
        K::NodeProgress => "cairn://p/cairn/1/1/builder/progress",
        K::NodeRebase => "cairn://p/cairn/1/1/builder/rebase",
        K::NodeArtifact => "cairn://p/cairn/1/1/builder/plan",
        K::NodeTerminal => "cairn://p/cairn/1/1/builder/terminal/dev",
        K::NodeRepl => "cairn://p/cairn/1/1/builder/repl/analysis",
        K::TaskTerminal => "cairn://p/cairn/1/1/builder/task/sub/terminal/dev",
        K::TaskMessages => "cairn://p/cairn/1/1/builder/task/sub/messages",
        K::TaskArtifact => "cairn://p/cairn/1/1/builder/task/sub/result",
        K::JobTodos => "cairn://p/cairn/1/1/builder/todos",
        K::HomeFeed => "cairn://p/cairn/1/1/builder/feed",
        K::NodeWakes => "cairn://p/cairn/1/1/builder/wakes",
        K::NodeTasks => "cairn://p/cairn/1/1/builder/tasks",
        K::NodeQuestions => "cairn://p/cairn/1/1/builder/questions",
        K::NodeQuestion => "cairn://p/cairn/1/1/builder/questions/q-1",
        K::NodePermission => "cairn://p/cairn/1/1/builder/permissions/perm-1",
        K::TaskPermission => "cairn://p/cairn/1/1/builder/task/sub/permissions/perm-1",
        K::TaskPermissions => "cairn://p/cairn/1/1/builder/task/sub/permissions",
        K::Bug => "cairn://bug",
        K::Skills => "cairn://skills",
        K::Skill => "cairn://skills/testing",
        K::ProjectSkills => "cairn://p/cairn/skills",
        K::ProjectSkill => "cairn://p/cairn/skills/testing",
        K::ProjectReferences => "cairn://p/cairn/references",
        K::ProjectReference => "cairn://p/cairn/references/openpnp",
        K::Packs => "cairn://packs",
        K::Pack => "cairn://packs/matlab",
        K::Labels => "cairn://labels",
        K::Label => "cairn://labels/bug",
        K::NodeMemories => "cairn://p/cairn/1/1/builder/memories",
        K::NodeMemory => "cairn://p/cairn/1/1/builder/memories/1",
        K::Recipes => "cairn://recipes",
        K::Recipe => "cairn://recipes/build",
        K::ProjectRecipes => "cairn://p/cairn/recipes",
        K::ProjectRecipe => "cairn://p/cairn/recipes/build",
        K::Agents => "cairn://agents",
        K::Agent => "cairn://agents/build",
        K::ProjectAgents => "cairn://p/cairn/agents",
        K::ProjectAgent => "cairn://p/cairn/agents/build",
        K::Actions => "cairn://actions",
        K::Action => "cairn://actions/example",
        K::ProjectActions => "cairn://p/cairn/actions",
        K::ProjectAction => "cairn://p/cairn/actions/example",
        K::NodeCalls => "cairn://p/cairn/1/1/builder/calls",
        K::Executors => "cairn://executors",
        K::Executor => "cairn://executors/bglab-ub",
        K::Routes => "cairn://routes",
        K::Route => "cairn://routes/notify-on-attention",
        K::ProjectRoutes => "cairn://p/cairn/routes",
        K::ProjectRoute => "cairn://p/cairn/routes/notify-on-attention",
        K::Responses => "cairn://responses",
        K::Response => "cairn://responses/summarize",
        K::ProjectResponses => "cairn://p/cairn/responses",
        K::ProjectResponse => "cairn://p/cairn/responses/summarize",
        K::Grant => "cairn://grants/grant-1",
        other => {
            panic!("sample_resource: {other:?} carries a mutation but has no sample URI; add one")
        }
    };
    let resource = cairn_common::uri::parse_uri(uri)
        .unwrap_or_else(|| panic!("sample_resource URI failed to parse: {uri}"));
    assert_eq!(
        resource.kind(),
        kind,
        "sample_resource URI {uri} parsed to a different kind",
    );
    resource
}

/// Parity backstop for the claim in `cairn-common/src/contract.rs`: every
/// `(kind, mode)` the contract table advertises must be handled by a real
/// dispatch arm, never falling through to the catch-all. Runtime parity (a
/// dry-run dispatch per advertised mutation) rather than a duplicated static
/// arm table, which would be a second source of truth that can itself drift.
/// The mutation need not succeed: any error other than the catch-all sentinel
/// (not-found, deep validation) proves an arm exists, and dry_run suppresses
/// side effects.
#[tokio::test]
async fn contract_mutations_all_have_dispatch_arms() {
    const SENTINEL: &str = "no dispatch arm handles it";
    let orch = seeded_orch().await;
    for contract in cairn_common::contract::RESOURCE_CONTRACTS {
        for spec in contract.mutations {
            let resource = sample_resource(contract.kind, spec.mode);
            let item = change_item(&resource.to_uri(), spec.mode, Some(required_payload(spec)));
            if let Err(failure) = dispatch_resource_change(&orch, &request(), 0, &item, true).await
            {
                assert!(
                    !failure.error.contains(SENTINEL),
                    "contract advertises {:?} mode={} but no dispatch arm handles it: {}",
                    contract.kind,
                    mode_name(spec.mode),
                    failure.error
                );
            }
        }
    }
}

/// The same parity claim, aimed at a THREAD's writable subtree.
///
/// `contract_mutations_all_have_dispatch_arms` samples one URI per kind, and
/// every node-family sample it carries is an issue coordinate — which is how a
/// whole writable subtree behind a thread shipped with the contract advertising
/// mutations and nothing behind them. Here the thread address itself is the
/// target: each path is normalized to its node-family kind, and every mutation
/// that kind advertises is dispatched at the thread URI. A path that reaches the
/// contract but falls off the end of dispatch fails.
#[tokio::test]
async fn thread_subtree_mutations_all_have_dispatch_arms() {
    const SENTINEL: &str = "no dispatch arm handles it";
    let orch = seeded_orch().await;
    orch.db
        .local
        .execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-thr', 'default', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, created_at, updated_at)
             VALUES ('t-dr', 'p-thr', 'design-review', 2, 2);",
        )
        .await
        .unwrap();

    let mut covered = 0;
    for path in [
        "arc",
        "todos",
        "tasks",
        "wakes",
        "questions",
        "memories",
        "messages",
        "permissions",
        "progress",
        "terminal/smoke",
        "repl/analysis",
        "browser",
        "artifact",
        "task/probe",
        "task/probe/todos",
        "task/probe/messages",
        "task/probe/artifact",
        "task/probe/return",
        "task/probe/permissions",
        "task/probe/terminal/build",
        "task/probe/browser",
    ] {
        let uri = format!("cairn://p/cairn/design-review/{path}");
        let delegated = crate::resources::threads::delegate_thread_descendant(
            cairn_common::uri::parse_uri(&uri).expect("a thread path parses"),
        )
        .unwrap_or_else(|error| panic!("{path} must delegate: {error}"));
        let Some(contract) = cairn_common::contract::contract_for(delegated.kind()) else {
            continue;
        };
        for spec in contract.mutations {
            let item = change_item(&uri, spec.mode, Some(required_payload(spec)));
            if let Err(failure) = dispatch_resource_change(&orch, &request(), 0, &item, true).await
            {
                assert!(
                    !failure.error.contains(SENTINEL),
                    "a thread's {path} advertises {:?} mode={} with no dispatch arm behind it: {}",
                    delegated.kind(),
                    mode_name(spec.mode),
                    failure.error
                );
            }
            covered += 1;
        }
    }
    assert!(
        covered >= 15,
        "only {covered} thread-subtree mutations were exercised; this test is not \
         covering what it claims"
    );
}

/// A dispatch arm has to RESOLVE, not merely exist.
///
/// `thread_subtree_mutations_all_have_dispatch_arms` dry-runs, and several arms
/// (repl create, terminal create) short-circuit inside `if dry_run` before any
/// job resolution happens — so it proves an arm was reached and cannot prove the
/// arm can find its owner. That gap let `repl/<slug>` from a thread pass the
/// parity test while failing at runtime on an issue-shaped resolver, which is
/// the exact bug class this family is about: the contract advertises a mutation
/// and nothing usable is behind it.
///
/// This wet-runs the paths whose resolution is owner-sensitive and asserts that
/// whatever comes back, it is never a failure to resolve the owning job.
#[tokio::test]
async fn thread_subtree_mutations_resolve_their_owning_job() {
    let orch = seeded_orch().await;
    orch.db
        .local
        .execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-thr', 'default', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO threads (id, project_id, name, created_at, updated_at)
             VALUES ('t-dr', 'p-thr', 'design-review', 2, 2);",
        )
        .await
        .unwrap();
    // The session must exist for a task to hang off it, and writes to a thread's
    // own resources mint it anyway.
    let session = crate::threads::ensure_thread_session(&orch.db.local, "t-dr")
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-probe', ?1, 'p-thr', 'running', 'probe', 'probe', 3, 3)",
            (session.as_str(),),
        )
        .await
        .unwrap();

    for (path, mode, payload) in [
        (
            "repl/analysis",
            ChangeMode::Create,
            Some(serde_json::json!({"interpreter": "python"})),
        ),
        (
            "todos",
            ChangeMode::Replace,
            Some(serde_json::json!({"todos": []})),
        ),
        (
            "wakes",
            ChangeMode::Append,
            Some(serde_json::json!({"subscribe": {"kind": "checks"}})),
        ),
        (
            "task/probe/todos",
            ChangeMode::Replace,
            Some(serde_json::json!({"todos": []})),
        ),
    ] {
        let uri = format!("cairn://p/cairn/design-review/{path}");
        let item = change_item(&uri, mode, payload);
        if let Err(failure) = dispatch_resource_change(&orch, &request(), 0, &item, false).await {
            // The signatures of an owner that could not be resolved: the reserved
            // coordinate leaking into an agent-facing message, or a lookup that
            // reported the job itself missing.
            assert!(
                !failure.error.contains("/0/0/"),
                "a thread's {path} leaked the reserved coordinate: {}",
                failure.error
            );
            assert!(
                !failure.error.contains("No node found")
                    && !failure.error.contains("no dispatch arm handles it"),
                "a thread's {path} could not resolve its owning job: {}",
                failure.error
            );
        }
    }
}

/// Alias analogue of the parity test. Every alias a mutation advertises must
/// be honored end-to-end (gate + dispatch arm), not merely matched by the
/// gate's `satisfied_by`: dispatch each owning mutation with the aliased key
/// in its ALIAS spelling (other required keys canonical) and assert it is
/// never rejected for a missing required key — exactly what a gate or handler
/// that ignores the alias would produce.
///
/// This bites on *required* aliased keys, where alias-honoring is gate-
/// observable. An *optional* aliased key deserialized into a struct can still
/// be silently dropped without erroring; full per-mutation coverage of that
/// is out of scope, so the one advertised serde-alias case is pinned
/// separately by `agent_frontmatter_honors_model_alias_for_tier`.
#[tokio::test]
async fn advertised_aliases_are_honored_by_dispatch() {
    const MISSING: &str = "Missing required payload key";
    let orch = seeded_orch().await;
    for contract in cairn_common::contract::RESOURCE_CONTRACTS {
        for spec in contract.mutations {
            let aliased = spec
                .required
                .iter()
                .chain(spec.optional.iter())
                .filter(|k| !k.aliases.is_empty());
            for key in aliased {
                let mut map = serde_json::Map::new();
                for req in spec.required {
                    map.insert(req.key.to_string(), dummy_value(req.ty));
                }
                // Re-spell the targeted key with its first alias.
                map.remove(key.key);
                let alias = key.aliases[0];
                map.insert(alias.to_string(), dummy_value(key.ty));
                let resource = sample_resource(contract.kind, spec.mode);
                let item = change_item(
                    &resource.to_uri(),
                    spec.mode,
                    Some(serde_json::Value::Object(map)),
                );
                if let Err(failure) =
                    dispatch_resource_change(&orch, &request(), 0, &item, true).await
                {
                    assert!(
                        !failure.error.contains(MISSING),
                        "{:?} mode={} does not honor alias '{}' for key '{}': {}",
                        contract.kind,
                        mode_name(spec.mode),
                        alias,
                        key.key,
                        failure.error
                    );
                }
            }
        }
    }
}

/// `AGENT_TIER` advertises `model` as an alias for `tier`. Unlike a gate-
/// checked required key, this optional field deserializes into a struct, so a
/// missing serde alias would silently drop it rather than erroring. Pin it:
/// agent frontmatter carrying `model` must populate `tier`.
#[test]
fn agent_frontmatter_honors_model_alias_for_tier() {
    let front: crate::agents::AgentFrontmatter = serde_json::from_value(serde_json::json!({
        "name": "Demo",
        "description": "demo agent",
        "tools": [],
        "model": "md",
    }))
    .expect("frontmatter with model alias should deserialize");
    assert_eq!(front.tier.as_deref(), Some("md"));
}

/// Launch overrides are refused at the write, not after the jobs exist. The
/// preview path never reaches the start, so a refusal that shows up here is one
/// that lands before an execution row could be created — which is the whole
/// reason the grammar is validated at the door rather than compiled later.
#[tokio::test]
async fn executions_append_refuses_a_malformed_override_before_starting() {
    let orch = seeded_orch().await;
    let (_, number) = seed_issue(&orch).await;
    let target = format!("cairn://p/cairn/{number}/executions");

    let append = |overrides: serde_json::Value| {
        change_item(
            &target,
            ChangeMode::Append,
            Some(serde_json::json!({"recipe": "build", "overrides": overrides})),
        )
    };

    let typo = preview(&orch, &append(serde_json::json!({"witout": ["review"]})))
        .await
        .unwrap_err();
    assert!(
        typo.error.contains("is not a launch override key"),
        "{}",
        typo.error
    );
    assert!(typo.error.contains("payload.overrides"), "{}", typo.error);

    let fence = preview(
        &orch,
        &append(serde_json::json!({"agents": {"build": {"fence": "allow"}}})),
    )
    .await
    .unwrap_err();
    assert!(
        fence.error.contains("not settable at launch"),
        "{}",
        fence.error
    );

    // A well-formed delta gets past the door; the recipe itself is resolved by
    // the start, which a preview deliberately does not perform.
    let accepted = preview(&orch, &append(serde_json::json!({"without": ["review"]})))
        .await
        .unwrap();
    assert!(
        accepted.starts_with("Would start an execution"),
        "{accepted}"
    );
}

/// The create-and-start door takes the same grammar, and says so in its own
/// coordinates: a caller reading `payload.overrides` on an issue-create would go
/// looking for a top-level key that door does not have.
#[tokio::test]
async fn issue_create_reports_override_failures_under_its_own_key() {
    let orch = seeded_orch().await;
    seed_issue(&orch).await;

    let failure = preview(
        &orch,
        &change_item(
            "cairn://p/cairn/issues",
            ChangeMode::Append,
            Some(serde_json::json!({
                "title": "tiny fix",
                "execution": {"recipe": "build", "overrides": {"witout": ["review"]}}
            })),
        ),
    )
    .await
    .unwrap_err();

    assert!(
        failure.error.contains("payload.execution.overrides"),
        "{}",
        failure.error
    );
    assert!(
        !failure.error.contains("payload.overrides."),
        "the collection-append coordinates must not leak into the create door: {}",
        failure.error
    );
}

#[tokio::test]
async fn posts_require_a_live_authenticated_identity_and_reject_forged_provenance() {
    let orch = seeded_orch().await;
    let item = change_item(
        "cairn://posts",
        ChangeMode::Append,
        Some(serde_json::json!({"content": "identity required"})),
    );
    let error = apply(&orch, &item).await.unwrap_err();
    assert!(
        error.error.contains("run") || error.error.contains("authenticated"),
        "got: {}",
        error.error
    );
    assert!(orch
        .db
        .local
        .list_posts(PostScope::Corpus, None, 10)
        .await
        .unwrap()
        .is_empty());

    let forged = change_item(
        "cairn://posts",
        ChangeMode::Append,
        Some(serde_json::json!({
            "content": "forged",
            "author": {"kind": "machine", "deviceId": "attacker"}
        })),
    );
    let error = apply(&orch, &forged).await.unwrap_err();
    assert!(error.error.contains("provenance is server-captured"));
    assert!(orch
        .db
        .local
        .list_posts(PostScope::Corpus, None, 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn posts_capture_live_agent_authorship_and_only_accept_own_project_scope() {
    let orch = seeded_orch().await;
    let (number, _, run_id) = seed_running_node(&orch).await;

    let workspace = change_item(
        "cairn://posts",
        ChangeMode::Append,
        Some(serde_json::json!({"title": "Workspace", "content": "shared"})),
    );
    apply_as_run(&orch, &workspace, &run_id).await.unwrap();

    let project = change_item(
        "cairn://posts",
        ChangeMode::Append,
        Some(serde_json::json!({
            "title": "Project",
            "content": "scoped",
            "scope": "CAIRN"
        })),
    );
    apply_as_run(&orch, &project, &run_id).await.unwrap();

    let posts = orch
        .db
        .local
        .list_posts(PostScope::Corpus, None, 10)
        .await
        .unwrap();
    assert_eq!(posts.len(), 2);
    let captured = posts
        .iter()
        .find(|post| post.title.as_deref() == Some("Project"))
        .unwrap();
    assert!(captured.project_id.is_some());
    assert_eq!(
        captured.author,
        cairn_common::identity::PrincipalRef::Agent {
            node: format!("cairn://p/cairn/{number}/1/builder"),
            run_id: Some(run_id.clone()),
        }
    );
    assert_eq!(captured.appearance.principal(), &captured.author);
    let evidence = captured.appearance.evidence();
    assert_eq!(
        evidence.transport,
        cairn_common::identity::AppearanceTransport::ResourcePatch
    );
    assert_eq!(
        evidence.verification.status(),
        cairn_common::identity::VerificationStatus::Verified
    );
    assert_eq!(evidence.verification.session(), Some(run_id.as_str()));

    let project_posts = orch
        .db
        .local
        .list_posts(PostScope::Project("cairn"), None, 10)
        .await
        .unwrap();
    assert_eq!(project_posts.len(), 1);
    assert_eq!(project_posts[0].title.as_deref(), Some("Project"));

    let unrelated = change_item(
        "cairn://posts",
        ChangeMode::Append,
        Some(serde_json::json!({
            "content": "must not land",
            "scope": "OTHER"
        })),
    );
    let error = apply_as_run(&orch, &unrelated, &run_id).await.unwrap_err();
    assert!(error.error.contains("own project key"));
    assert_eq!(
        orch.db
            .local
            .list_posts(PostScope::Corpus, None, 10)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn post_reads_are_complete_ordered_and_search_before_limit_without_mutating_query() {
    let orch = seeded_orch().await;
    let (_, _, run_id) = seed_running_node(&orch).await;
    let matching = change_item(
        "cairn://posts",
        ChangeMode::Append,
        Some(serde_json::json!({"title": "Needle", "content": "old match"})),
    );
    apply_as_run(&orch, &matching, &run_id).await.unwrap();
    let post = orch
        .db
        .local
        .list_posts(PostScope::Corpus, Some("needle"), 1)
        .await
        .unwrap()
        .remove(0);

    for title in ["Newer one", "Newer two"] {
        let item = change_item(
            "cairn://posts",
            ChangeMode::Append,
            Some(serde_json::json!({"title": title, "content": "not a match"})),
        );
        apply_as_run(&orch, &item, &run_id).await.unwrap();
    }
    let params = vec![
        cairn_common::query::QueryParam {
            key: "search".into(),
            value: "needle".into(),
        },
        cairn_common::query::QueryParam {
            key: "limit".into(),
            value: "1".into(),
        },
    ];
    let original = params.clone();
    let rendered =
        crate::resources::posts::read_posts(&orch.db.local, PostScope::Corpus, &params).await;
    assert!(rendered.contains("Needle") && rendered.contains("old match"));
    assert!(!rendered.contains("Newer one"));
    assert_eq!(
        params, original,
        "reading must not consume or rewrite query state"
    );

    for content in ["first comment", "second comment"] {
        let comment = change_item(
            &format!("cairn://posts/{}", post.id),
            ChangeMode::Append,
            Some(serde_json::json!({"content": content})),
        );
        apply_as_run(&orch, &comment, &run_id).await.unwrap();
    }

    let json_params = vec![cairn_common::query::QueryParam {
        key: "format".into(),
        value: "json".into(),
    }];
    let json: serde_json::Value = serde_json::from_str(
        &crate::resources::posts::read_post(&orch.db.local, post.id, &json_params).await,
    )
    .unwrap();
    assert_eq!(json["post"]["content"], "old match");
    assert!(json["post"]["author"].is_object());
    assert!(json["post"]["appearance"].is_object());
    assert_eq!(json["comments"].as_array().unwrap().len(), 2);
    assert!(json["comments"][0]["author"].is_object());
    assert!(json["comments"][0]["appearance"].is_object());

    let markdown = crate::resources::posts::read_post(&orch.db.local, post.id, &[]).await;
    let first = markdown.find("first comment").unwrap();
    let second = markdown.find("second comment").unwrap();
    assert!(first < second, "comments must render in creation order");
    assert!(markdown.contains("# Needle") && markdown.contains("## Comments"));
}

/// A second project alongside the `cairn` one `seed_issue` creates, so a scoped
/// post has somewhere to live that is not the caller's own. Returns its id.
async fn seed_other_project(orch: &Orchestrator) -> String {
    project_crud::create_db(
        &orch.db.local,
        &RealClock,
        &CreateProject {
            id: None,
            name: "Other".to_string(),
            key: "other".to_string(),
            repo_path: tempfile::tempdir()
                .unwrap()
                .keep()
                .to_string_lossy()
                .to_string(),
            team_id: None,
        },
    )
    .await
    .unwrap()
    .id
}

/// A post at a chosen scope, written straight to storage. Authorship is not what
/// the corpus-window tests are about, so it is minted here rather than routed
/// through a second live run.
async fn seed_post(
    orch: &Orchestrator,
    project_id: Option<&str>,
    title: &str,
    content: &str,
) -> i64 {
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
        VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
    };
    let author = PrincipalRef::Human {
        issuer: "https://identity.example".to_string(),
        subject: "author".to_string(),
        organization: None,
    };
    let verification = VerificationRecord::new(
        VerificationMethod::JwtOperator,
        VerificationStatus::Verified,
        Some("https://identity.example".to_string()),
        Some("author".to_string()),
        None,
        None,
        VerificationStrength::new("strong").unwrap(),
        900,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::AuthenticatedOperator,
        Address::Invoke { origin: None },
        verification,
        900,
        None,
    )
    .unwrap();
    let appearance = AppearanceSnapshot::new(author.clone(), evidence, vec![], None).unwrap();
    orch.db
        .local
        .create_post(crate::models::CreatePost {
            project_id: project_id.map(str::to_string),
            title: Some(title.to_string()),
            content: content.to_string(),
            author,
            appearance,
        })
        .await
        .unwrap()
        .id
}

/// Read a resource as a given run identity would — `None` for a request that
/// carries none at all, which is what an operator's own read looks like.
async fn read_as(orch: &Orchestrator, run_id: Option<&str>, uri: &str) -> String {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: "read".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    };
    crate::resources::read_cairn_resource(orch, &request, uri).await
}

/// Scope is a relevance filter on the UNADDRESSED corpus: an agent asking "what
/// is there" gets the workspace-wide posts plus its own project's, and never
/// another project's scoped post — the same window the feed and the desktop
/// timeline render, applied to the one surface that answers with everything.
///
/// `?search=` narrows inside that window rather than around it: a term that
/// matches only another project's post finds nothing.
#[tokio::test]
async fn the_unaddressed_corpus_renders_only_the_caller_s_own_window() {
    let orch = seeded_orch().await;
    let (_, _, run_id) = seed_running_node(&orch).await;
    let other = seed_other_project(&orch).await;
    let own = project_id_for_key(&orch, "cairn").await;

    seed_post(&orch, None, "Everyone", "a workspace observation").await;
    seed_post(&orch, Some(&own), "Ours", "a cairn observation").await;
    seed_post(&orch, Some(&other), "Theirs", "an unrelated observation").await;

    let corpus = read_as(&orch, Some(&run_id), "cairn://posts").await;
    assert!(corpus.contains("Everyone"), "{corpus}");
    assert!(corpus.contains("Ours"), "{corpus}");
    assert!(
        !corpus.contains("Theirs") && !corpus.contains("an unrelated observation"),
        "another project's scoped post must not appear in the unaddressed corpus: {corpus}"
    );

    let searched = read_as(&orch, Some(&run_id), "cairn://posts?search=unrelated").await;
    assert!(
        !searched.contains("Theirs") && !searched.contains("an unrelated observation"),
        "search must narrow inside the window, not around it: {searched}"
    );
    assert!(searched.contains("No posts found"), "{searched}");

    // Narrowing the corpus leaves its ordering alone: what survives the window
    // still renders newest first.
    assert!(
        corpus.find("Ours").unwrap() < corpus.find("Everyone").unwrap(),
        "the window must not disturb newest-first ordering: {corpus}"
    );
}

/// The addressed surfaces stay workspace-open, and that is the INTENT rather
/// than an oversight: naming another project's posts collection, naming a post
/// by id, or commenting on one is a deliberate act, and it reaches across
/// projects exactly as reading another project's issues does. A future pass that
/// "fixes" this by adding an ACL has to delete this test to do it.
#[tokio::test]
async fn deliberately_addressed_posts_stay_readable_and_commentable_across_projects() {
    let orch = seeded_orch().await;
    let (_, _, run_id) = seed_running_node(&orch).await;
    let other = seed_other_project(&orch).await;
    let theirs = seed_post(&orch, Some(&other), "Theirs", "an unrelated observation").await;

    let collection = read_as(&orch, Some(&run_id), "cairn://p/other/posts").await;
    assert!(
        collection.contains("Theirs") && collection.contains("an unrelated observation"),
        "another project's posts collection is addressed, so it renders: {collection}"
    );

    let post = read_as(&orch, Some(&run_id), &format!("cairn://posts/{theirs}")).await;
    assert!(
        post.contains("an unrelated observation"),
        "a post named by id renders whoever asks: {post}"
    );

    let comment = change_item(
        &format!("cairn://posts/{theirs}"),
        ChangeMode::Append,
        Some(serde_json::json!({"content": "replying from another project"})),
    );
    apply_as_run(&orch, &comment, &run_id)
        .await
        .expect("commenting across projects is allowed");
    assert_eq!(
        orch.db
            .local
            .list_post_comments(theirs)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// Fail-closed, in the one direction that matters. A request claiming a run that
/// cannot be resolved has an unknown window, and an unknown window is not the
/// whole workspace: the read errors and carries nothing it could not place.
///
/// The other direction is deliberate and asserted beside it: a request carrying
/// no run identity at all is an operator's own, and an operator stands in no
/// project, so the whole corpus is theirs.
#[tokio::test]
async fn an_unresolvable_caller_errors_while_an_operator_reads_the_whole_corpus() {
    let orch = seeded_orch().await;
    seed_running_node(&orch).await;
    let other = seed_other_project(&orch).await;
    seed_post(&orch, Some(&other), "Theirs", "an unrelated observation").await;

    let refused = read_as(&orch, Some("ghost"), "cairn://posts").await;
    assert!(
        refused.contains("cannot resolve") && refused.contains("ghost"),
        "the refusal must say the window is unknown and name the run: {refused}"
    );
    assert!(
        !refused.contains("an unrelated observation"),
        "an unresolvable identity must not degrade to the unfiltered corpus: {refused}"
    );

    let operator = read_as(&orch, None, "cairn://posts").await;
    assert!(
        operator.contains("an unrelated observation"),
        "an operator holds no project jurisdiction, so nothing is withheld: {operator}"
    );
}

/// The project key a seeded project was created under, as a row id.
async fn project_id_for_key(orch: &Orchestrator, key: &str) -> String {
    crate::mcp::handlers::run_context::project_id_by_key(&orch.db.local, key)
        .await
        .unwrap()
}
