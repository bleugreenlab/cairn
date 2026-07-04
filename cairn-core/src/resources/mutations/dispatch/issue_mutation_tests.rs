use super::*;
use crate::db::DbState;
use crate::issues::comments;
use crate::issues::crud as issue_crud;
use crate::models::{CommentSource, CreateComment, CreateIssue, CreateProject, IssueStatus};
use crate::orchestrator::OrchestratorBuilder;
use crate::projects::crud as project_crud;
use crate::services::testing::TestServicesBuilder;
use crate::services::RealClock;
use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
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
            key: "CAIRN".to_string(),
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
    )
    .await
    .unwrap();
    (issue.id, issue.number)
}

fn request() -> McpCallbackRequest {
    McpCallbackRequest {
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
        crate::resources::issue::read_issue_comments(&orch.db.local, "CAIRN", number).await;
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
        rendered.contains(&format!("cairn://p/CAIRN/{number}/comments/1")),
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
        &format!("cairn://p/CAIRN/{number}/comments/{}", c1.seq),
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
        &format!("cairn://p/CAIRN/{number}/comments/{}", c1.seq),
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
        &format!("cairn://p/CAIRN/{number}/comments/999"),
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
        &format!("cairn://p/CAIRN/{number}/comments/999"),
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
        &format!("cairn://p/CAIRN/{number}"),
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

async fn parent_job_id_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
    let issue_id = issue_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent_job_id FROM issues WHERE id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                crate::storage::next_opt_text(&mut rows, 0).await
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
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path) \
                 VALUES ('job-stop', 'exec-stop', '{issue_id}', '{project_id}', 'Builder', 'running', 1, 1, 'builder', '/tmp/repo-builder')"
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
        &format!("cairn://p/CAIRN/{number}/1/builder"),
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
        &format!("cairn://p/CAIRN/{number}/1/builder"),
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
        &format!("cairn://p/CAIRN/{number}/1/builder"),
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
        &format!("cairn://p/CAIRN/{number}/1/builder"),
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
        &format!("cairn://p/CAIRN/{number}/1/builder"),
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
        &format!("cairn://p/CAIRN/{number}"),
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
        &format!("cairn://p/CAIRN/{number}"),
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

#[tokio::test]
async fn patch_invalid_status_is_rejected() {
    let orch = seeded_orch().await;
    let (issue_id, number) = seed_issue(&orch).await;
    // `backlog` is derived, not settable, and must be rejected alongside any
    // other unknown value.
    for bad in ["backlog", "active", "frobnicate"] {
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"status": bad})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
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
        &format!("cairn://p/CAIRN/{number}"),
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
        &format!("cairn://p/CAIRN/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{parent_num}")})),
    );
    apply(&orch, &item).await.unwrap();
    assert_eq!(
        parent_issue_id_of(&orch, &child_id).await.as_deref(),
        Some(parent_id.as_str())
    );
}

#[tokio::test]
async fn patch_parent_records_parent_job_id_from_caller() {
    let orch = seeded_orch().await;
    let (number, job_id, run_id) = seed_running_node(&orch).await;
    let running_issue =
        crate::issues::relations::issue_id_for_project_number(&orch.db.local, "CAIRN", number)
            .await
            .unwrap()
            .unwrap();
    let project_id = project_id_of(&orch, &running_issue).await;
    let (parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
    let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;
    let item = change_item(
        &format!("cairn://p/CAIRN/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{parent_num}")})),
    );
    apply_as_run(&orch, &item, &run_id).await.unwrap();
    assert_eq!(
        parent_issue_id_of(&orch, &child_id).await.as_deref(),
        Some(parent_id.as_str())
    );
    // run-stop's job is a recipe-root, so its own job is the recorded spawner.
    assert_eq!(
        parent_job_id_of(&orch, &child_id).await.as_deref(),
        Some(job_id.as_str())
    );
}

#[tokio::test]
async fn patch_parent_null_orphans_issue() {
    let orch = seeded_orch().await;
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    let running_issue =
        crate::issues::relations::issue_id_for_project_number(&orch.db.local, "CAIRN", number)
            .await
            .unwrap()
            .unwrap();
    let project_id = project_id_of(&orch, &running_issue).await;
    let (_parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
    let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;
    // Adopt as a job-bound run so both parent fields are populated.
    let adopt = change_item(
        &format!("cairn://p/CAIRN/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{parent_num}")})),
    );
    apply_as_run(&orch, &adopt, &run_id).await.unwrap();
    assert!(parent_issue_id_of(&orch, &child_id).await.is_some());
    assert!(parent_job_id_of(&orch, &child_id).await.is_some());
    // Orphan clears both the parent and its now-meaningless spawner.
    let orphan = change_item(
        &format!("cairn://p/CAIRN/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": serde_json::Value::Null})),
    );
    apply(&orch, &orphan).await.unwrap();
    assert!(parent_issue_id_of(&orch, &child_id).await.is_none());
    assert!(parent_job_id_of(&orch, &child_id).await.is_none());
}

#[tokio::test]
async fn patch_parent_self_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/CAIRN/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{child_num}")})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(err.error.contains("its own parent"), "got: {}", err.error);
}

#[tokio::test]
async fn patch_parent_unknown_uri_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/CAIRN/{child_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": "cairn://p/CAIRN/9999"})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(
        err.error.contains("parent issue not found"),
        "got: {}",
        err.error
    );
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
        &format!("cairn://p/CAIRN/{child_num}"),
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
        &format!("cairn://p/CAIRN/{a_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{b_num}")})),
    );
    apply(&orch, &adopt).await.unwrap();
    // Adopting B under A would close the loop A -> B -> A.
    let cycle = change_item(
        &format!("cairn://p/CAIRN/{b_num}"),
        ChangeMode::Patch,
        Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{a_num}")})),
    );
    let err = apply(&orch, &cycle).await.unwrap_err();
    assert!(err.error.contains("cycle"), "got: {}", err.error);
}

#[tokio::test]
async fn patch_parent_malformed_uri_rejected() {
    let orch = seeded_orch().await;
    let (_child_id, child_num) = seed_issue(&orch).await;
    let item = change_item(
        &format!("cairn://p/CAIRN/{child_num}"),
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
        &format!("cairn://p/CAIRN/{number}"),
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
        &format!("cairn://p/CAIRN/{number}"),
        ChangeMode::Delete,
        Some(serde_json::json!({"force": true})),
    );
    let err = apply(&orch, &item).await.unwrap_err();
    assert!(
        err.error.contains("does not accept payload"),
        "got: {}",
        err.error
    );
}

#[tokio::test]
async fn delete_unknown_issue_errors() {
    let orch = seeded_orch().await;
    seed_issue(&orch).await;
    let item = change_item("cairn://p/CAIRN/9999", ChangeMode::Delete, None);
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
        &format!("cairn://p/CAIRN/{number}/executions/1"),
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
    let snap = crate::models::ExecutionSnapshot::from_json(&json).unwrap();
    assert_eq!(snap.agents["builder"].prompt, "new");
}

fn dummy_value(ty: cairn_common::contract::KeyType) -> serde_json::Value {
    use cairn_common::contract::KeyType;
    match ty {
        KeyType::Str => serde_json::json!("sample"),
        KeyType::Bool => serde_json::json!(true),
        KeyType::Int => serde_json::json!(1),
        KeyType::Array => serde_json::json!([]),
        KeyType::Object => serde_json::json!({}),
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
        K::Project => "cairn://p/CAIRN",
        K::Settings => "cairn://settings",
        K::Projects => "cairn://projects",
        K::ProjectSettings => "cairn://p/CAIRN/settings",
        K::ProjectIssues => "cairn://p/CAIRN/issues",
        K::ProjectMessages => "cairn://p/CAIRN/messages",
        K::ProjectTerminal => "cairn://p/CAIRN/terminal/dev",
        K::ProjectBrowser => "cairn://p/CAIRN/browser/main",
        K::NodeBrowser => "cairn://p/CAIRN/1/1/builder/browser/main",
        K::TaskBrowser => "cairn://p/CAIRN/1/1/builder/task/sub/browser/main",
        K::Issue => "cairn://p/CAIRN/1",
        K::IssueExecutions => "cairn://p/CAIRN/1/executions",
        K::IssueExecution => "cairn://p/CAIRN/1/executions/2",
        K::IssueMessages => "cairn://p/CAIRN/1/messages",
        K::IssueComment => "cairn://p/CAIRN/1/comments/1",
        K::Node => "cairn://p/CAIRN/1/1/builder",
        K::NodeMessages => "cairn://p/CAIRN/1/1/builder/messages",
        K::NodeArtifact => "cairn://p/CAIRN/1/1/builder/plan",
        K::NodeTerminal => "cairn://p/CAIRN/1/1/builder/terminal/dev",
        K::TaskTerminal => "cairn://p/CAIRN/1/1/builder/task/sub/terminal/dev",
        K::TaskMessages => "cairn://p/CAIRN/1/1/builder/task/sub/messages",
        K::TaskArtifact => "cairn://p/CAIRN/1/1/builder/task/sub/result",
        K::JobTodos => "cairn://p/CAIRN/1/1/builder/todos",
        K::NodeWakes => "cairn://p/CAIRN/1/1/builder/wakes",
        K::NodeTasks => "cairn://p/CAIRN/1/1/builder/tasks",
        K::NodeQuestions => "cairn://p/CAIRN/1/1/builder/questions",
        K::NodeQuestion => "cairn://p/CAIRN/1/1/builder/questions/q-1",
        K::NodePermission => "cairn://p/CAIRN/1/1/builder/permissions/perm-1",
        K::TaskPermission => "cairn://p/CAIRN/1/1/builder/task/sub/permissions/perm-1",
        K::TaskPermissions => "cairn://p/CAIRN/1/1/builder/task/sub/permissions",
        K::Bug => "cairn://bug",
        K::Skills => "cairn://skills",
        K::Skill => "cairn://skills/testing",
        K::ProjectSkills => "cairn://p/CAIRN/skills",
        K::ProjectSkill => "cairn://p/CAIRN/skills/testing",
        K::ProjectReferences => "cairn://p/CAIRN/references",
        K::ProjectReference => "cairn://p/CAIRN/references/openpnp",
        K::Labels => "cairn://labels",
        K::Label => "cairn://labels/bug",
        K::NodeMemories => "cairn://p/CAIRN/1/1/builder/memories",
        K::NodeMemory => "cairn://p/CAIRN/1/1/builder/memories/1",
        K::Recipes => "cairn://recipes",
        K::Recipe => "cairn://recipes/build",
        K::ProjectRecipes => "cairn://p/CAIRN/recipes",
        K::ProjectRecipe => "cairn://p/CAIRN/recipes/build",
        K::Agents => "cairn://agents",
        K::Agent => "cairn://agents/build",
        K::ProjectAgents => "cairn://p/CAIRN/agents",
        K::ProjectAgent => "cairn://p/CAIRN/agents/build",
        K::Actions => "cairn://actions",
        K::Action => "cairn://actions/example",
        K::ProjectActions => "cairn://p/CAIRN/actions",
        K::ProjectAction => "cairn://p/CAIRN/actions/example",
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
