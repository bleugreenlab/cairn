//! Fail-closed agent status resolution (CAIRN-2287). An agent marking an issue
//! `merged` while its PR is still open is refused and pointed at the real merge
//! lever; with no open PR the record-only resolution is allowed; the user path is
//! a deliberate override and resolves regardless.

use crate::common;

use std::sync::Arc;

use cairn_core::internal::storage::LocalDb;
use cairn_core::issues::status::{update_status, ResolutionActor};
use turso::params;

async fn insert_issue(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.execute(
        "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
         VALUES (?1, ?2, 1, 'Issue', 'active', 'idle', 'none', 1, 1)",
        params![issue_id.as_str(), project_id.as_str()],
    )
    .await
    .unwrap();
}

async fn insert_open_mr(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.execute(
        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, merge_method, opened_at, updated_at)
         VALUES ('mr-1', 'job-x', ?1, ?2, 'PR', 'feature', 'main', 'open', 'squash', 1, 1)",
        params![project_id.as_str(), issue_id.as_str()],
    )
    .await
    .unwrap();
}

async fn issue_status(db: &LocalDb, issue_id: &str) -> String {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM issues WHERE id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.unwrap();
            cairn_core::internal::storage::RowExt::text(&row, 0)
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn agent_status_merged_with_open_pr_is_refused() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RES").await;
    insert_issue(&db, &project_id, "issue-1").await;
    insert_open_mr(&db, &project_id, "issue-1").await;
    let orch = common::orchestrator(&temp, db.clone());

    let err = update_status(&orch, "issue-1", "merged", ResolutionActor::Agent)
        .await
        .expect_err("an open PR must refuse a status:merged resolution");
    assert!(
        err.contains("OPEN pull request"),
        "names the open PR: {err}"
    );
    assert!(
        err.contains("action:\"merge\"") && err.contains("create-pr"),
        "points at the create-pr merge lever: {err}"
    );
    // The issue is left unresolved.
    assert_eq!(issue_status(&db, "issue-1").await, "active");
}

#[tokio::test]
async fn agent_status_merged_without_pr_resolves() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RES").await;
    insert_issue(&db, &project_id, "issue-1").await;
    let orch = common::orchestrator(&temp, db.clone());

    // No merge_requests row — a record-only resolution is legitimate.
    update_status(&orch, "issue-1", "merged", ResolutionActor::Agent)
        .await
        .expect("merged with no PR is allowed");
    assert_eq!(issue_status(&db, "issue-1").await, "merged");
}

#[tokio::test]
async fn user_status_merged_with_open_pr_overrides() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RES").await;
    insert_issue(&db, &project_id, "issue-1").await;
    insert_open_mr(&db, &project_id, "issue-1").await;
    let orch = common::orchestrator(&temp, db.clone());

    // A person acting through the UI is a deliberate override: it resolves even
    // with an open PR (the menu confirmed first).
    update_status(&orch, "issue-1", "merged", ResolutionActor::User)
        .await
        .expect("the user override resolves regardless of an open PR");
    assert_eq!(issue_status(&db, "issue-1").await, "merged");
}
