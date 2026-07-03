//! Regression tests for the issue attention/status recompute over PRs.
//!
//! A non-terminal merge request is user-actionable even when its PR action run is
//! no longer the durable blocked row. Mergeability/review/check metadata does not
//! change the attention category; only terminal PR status releases it.

use crate::common;

use cairn_core::internal::storage::{DbError, LocalDb, RowExt};
use cairn_core::transitions::outcome::recompute_issue_status_conn;
use turso::params;

/// Seed `(project, issue, running execution, complete builder job, open PR)`.
/// The PR's GitHub state fields are caller-supplied so each test can model a
/// distinct mergeability / review / checks combination. `None` columns model
/// the unknown state right after a PR is opened.
async fn seed_issue_with_pr(
    db: &LocalDb,
    pr_status: &str,
    github_mergeable: Option<&str>,
    github_review: Option<&str>,
    checks_status: Option<&str>,
) -> String {
    let project_id = common::create_project(db, "OUT").await;
    let issue_id = uuid::Uuid::new_v4().to_string();
    let execution_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4().to_string();
    let mr_id = uuid::Uuid::new_v4().to_string();

    let p = project_id.clone();
    let i = issue_id.clone();
    let e = execution_id.clone();
    let j = job_id.clone();
    let m = mr_id.clone();
    let mergeable = github_mergeable.map(str::to_string);
    let review = github_review.map(str::to_string);
    let checks = checks_status.map(str::to_string);
    let pr_status = pr_status.to_string();
    db.write(move |conn| {
        let p = p.clone();
        let i = i.clone();
        let e = e.clone();
        let j = j.clone();
        let m = m.clone();
        let mergeable = mergeable.clone();
        let review = review.clone();
        let checks = checks.clone();
        let pr_status = pr_status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES (?1, ?2, 1, 'Issue', 'active', 'active', 'none', 1, 1)",
                params![i.as_str(), p.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES (?1, 'default', ?2, ?3, 'running', 1, 1)",
                params![e.as_str(), i.as_str(), p.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'complete', 'builder', 'builder', 1, 1)",
                params![j.as_str(), e.as_str(), i.as_str(), p.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO merge_requests (
                    id, job_id, project_id, issue_id, title, source_branch, target_branch,
                    status, merge_method, opened_at, updated_at, github_pr_number, github_pr_url,
                    github_mergeable, github_review, checks_status
                 ) VALUES (?1, ?2, ?3, ?4, 'PR', 'feature', 'main', ?5, 'squash', 1, 1, 7,
                           'https://github.com/octo/widget/pull/7', ?6, ?7, ?8)",
                params![
                    m.as_str(),
                    j.as_str(),
                    p.as_str(),
                    i.as_str(),
                    pr_status.as_str(),
                    mergeable.as_deref(),
                    review.as_deref(),
                    checks.as_deref()
                ],
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
    issue_id
}

async fn recompute_and_read(db: &LocalDb, issue_id: &str) -> (String, String) {
    let id = issue_id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        Box::pin(async move { recompute_issue_status_conn(conn, &id).await })
    })
    .await
    .unwrap();

    let id = issue_id.to_string();
    db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT attention, status FROM issues WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let row = rows.next().await?.unwrap();
            Ok::<_, DbError>((row.text(0)?, row.text(1)?))
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn open_pr_with_unknown_github_state_needs_approval() {
    // The crux of CAIRN-2294: a just-opened PR whose mergeability / review /
    // checks are still unknown must keep the issue waiting so parent/coordinator
    // wakes continue until the PR itself reaches a terminal state.
    let (_temp, db) = common::migrated_db().await;
    let issue_id = seed_issue_with_pr(&db, "open", None, None, None).await;
    let (attention, status) = recompute_and_read(&db, &issue_id).await;
    assert_eq!(attention, "needs_approval");
    assert_eq!(status, "waiting");
}

#[tokio::test]
async fn mergeable_open_pr_with_passing_checks_still_needs_approval() {
    let (_temp, db) = common::migrated_db().await;
    let issue_id = seed_issue_with_pr(&db, "open", Some("MERGEABLE"), None, Some("SUCCESS")).await;
    let (attention, status) = recompute_and_read(&db, &issue_id).await;
    assert_eq!(attention, "needs_approval");
    assert_eq!(status, "waiting");
}

#[tokio::test]
async fn conflicting_open_pr_still_needs_approval() {
    let (_temp, db) = common::migrated_db().await;
    let issue_id = seed_issue_with_pr(&db, "open", Some("CONFLICTING"), None, None).await;
    let (attention, status) = recompute_and_read(&db, &issue_id).await;
    assert_eq!(attention, "needs_approval");
    assert_eq!(status, "waiting");
}

#[tokio::test]
async fn changes_requested_open_pr_still_needs_approval() {
    let (_temp, db) = common::migrated_db().await;
    let issue_id = seed_issue_with_pr(
        &db,
        "open",
        Some("MERGEABLE"),
        Some("CHANGES_REQUESTED"),
        Some("SUCCESS"),
    )
    .await;
    let (attention, status) = recompute_and_read(&db, &issue_id).await;
    assert_eq!(attention, "needs_approval");
    assert_eq!(status, "waiting");
}

#[tokio::test]
async fn terminal_pr_status_releases_attention() {
    for pr_status in ["merged", "closed"] {
        let (_temp, db) = common::migrated_db().await;
        let issue_id =
            seed_issue_with_pr(&db, pr_status, Some("MERGEABLE"), None, Some("SUCCESS")).await;
        let (attention, status) = recompute_and_read(&db, &issue_id).await;
        assert_eq!(attention, "none", "{pr_status} should release attention");
        assert_eq!(status, "active", "{pr_status} should not force waiting");
    }
}
