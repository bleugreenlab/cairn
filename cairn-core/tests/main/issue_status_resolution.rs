//! Terminal issue resolution at the mutation seam.
//!
//! Two guards live here and they are different in kind. An agent marking an
//! issue `merged` while its PR is still open is refused outright and pointed at
//! the real merge lever (CAIRN-2287) — a confirmation cannot make that right.
//! An issue that still has live work is a *confirmation* (CAIRN-3212): the first
//! attempt is refused with the work enumerated and the confirming key named, and
//! the confirmed attempt stops that work cleanly before resolving. Both callers
//! — the agent write path and the UI — confirm the same way.

use crate::common;

use cairn_core::internal::storage::LocalDb;
use cairn_core::issues::status::{
    live_work_for_issue, update_status, Confirmation, ResolutionActor, ResolutionRefusal,
};
use cairn_db::turso::params;

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

/// A bare job row: enough to be counted as (or excluded from) live work.
async fn insert_job(
    db: &LocalDb,
    project_id: &str,
    job_id: &str,
    node_name: &str,
    status: &str,
    created_at: i64,
) {
    let project_id = project_id.to_string();
    let job_id = job_id.to_string();
    let node_name = node_name.to_string();
    let status = status.to_string();
    db.execute(
        "INSERT INTO jobs (id, project_id, issue_id, node_name, status, created_at, updated_at)
         VALUES (?1, ?2, 'issue-1', ?3, ?4, ?5, ?5)",
        params![
            job_id.as_str(),
            project_id.as_str(),
            node_name.as_str(),
            status.as_str(),
            created_at
        ],
    )
    .await
    .unwrap();
}

/// A started job with the session, run, and turn a live agent holds — the shape
/// the canonical stop path acts on.
async fn insert_running_job(db: &LocalDb, project_id: &str) {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs (id, project_id, issue_id, node_name, status, current_session_id, created_at, updated_at)
                 VALUES ('job-builder', ?1, 'issue-1', 'builder', 'running', 'session-1', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-builder', 'open', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'job-builder', NULL, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-builder', 1, 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-builder'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn issue_status(db: &LocalDb, issue_id: &str) -> String {
    common::scalar_text_by_id(db, "SELECT status FROM issues WHERE id = ?1", issue_id)
        .await
        .expect("issue row")
}

async fn job_status(db: &LocalDb, job_id: &str) -> String {
    common::scalar_text_by_id(db, "SELECT status FROM jobs WHERE id = ?1", job_id)
        .await
        .expect("job row")
}

#[tokio::test]
async fn agent_status_merged_with_open_pr_is_refused() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    insert_open_mr(&orch.db.local, &project_id, "issue-1").await;

    let refusal = update_status(
        &orch,
        "issue-1",
        "merged",
        ResolutionActor::Agent,
        Confirmation::Absent,
    )
    .await
    .expect_err("an open PR must refuse a status:merged resolution");

    assert!(
        matches!(refusal, ResolutionRefusal::Rejected(_)),
        "an open PR is not something a confirmation clears: {refusal:?}"
    );
    let text = refusal.to_string();
    assert!(
        text.contains("OPEN pull request"),
        "names the open PR: {text}"
    );
    assert!(
        text.contains("action:\"merge\"") && text.contains("create-pr"),
        "points at the create-pr merge lever: {text}"
    );
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "active");
}

#[tokio::test]
async fn agent_status_merged_without_pr_resolves() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;

    // No merge_requests row — a record-only resolution is legitimate.
    update_status(
        &orch,
        "issue-1",
        "merged",
        ResolutionActor::Agent,
        Confirmation::Absent,
    )
    .await
    .expect("merged with no PR is allowed");
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "merged");
}

#[tokio::test]
async fn user_status_merged_with_open_pr_overrides() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    insert_open_mr(&orch.db.local, &project_id, "issue-1").await;

    // A person has no create-pr merge lever to be redirected to, so the open-PR
    // guard does not apply to the UI path.
    update_status(
        &orch,
        "issue-1",
        "merged",
        ResolutionActor::User,
        Confirmation::Absent,
    )
    .await
    .expect("the user path resolves regardless of an open PR");
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "merged");
}

#[tokio::test]
async fn unconfirmed_close_enumerates_live_work_and_names_the_key() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    insert_running_job(&orch.db.local, &project_id).await;
    insert_job(
        &orch.db.local,
        &project_id,
        "job-reviewer",
        "reviewer",
        "pending",
        2,
    )
    .await;

    let refusal = update_status(
        &orch,
        "issue-1",
        "closed",
        ResolutionActor::Agent,
        Confirmation::Absent,
    )
    .await
    .expect_err("live work must ask for a confirmation");

    match &refusal {
        ResolutionRefusal::NeedsConfirmation { status, live_work } => {
            assert_eq!(status, "closed");
            let named: Vec<&str> = live_work.iter().map(|job| job.name.as_str()).collect();
            assert_eq!(named, vec!["builder", "reviewer"]);
        }
        other => panic!("expected a confirmation refusal, got {other:?}"),
    }

    let text = refusal.to_string();
    assert!(
        text.contains("builder (running now)") && text.contains("reviewer (queued, never started)"),
        "names each piece of live work and its state: {text}"
    );
    assert!(
        text.contains("confirm: true"),
        "names the key that confirms the close: {text}"
    );
    assert_eq!(
        issue_status(&orch.db.local, "issue-1").await,
        "active",
        "a refused close changes nothing"
    );
    assert_eq!(job_status(&orch.db.local, "job-reviewer").await, "pending");
}

#[tokio::test]
async fn user_close_with_live_work_needs_the_same_confirmation() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    insert_running_job(&orch.db.local, &project_id).await;

    // The UI is not exempt by species of caller: it confirms through the same key.
    let refusal = update_status(
        &orch,
        "issue-1",
        "closed",
        ResolutionActor::User,
        Confirmation::Absent,
    )
    .await
    .expect_err("the UI path takes the same confirmation");
    assert!(matches!(
        refusal,
        ResolutionRefusal::NeedsConfirmation { .. }
    ));
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "active");

    update_status(
        &orch,
        "issue-1",
        "closed",
        ResolutionActor::User,
        Confirmation::Given,
    )
    .await
    .expect("the confirmed close proceeds");
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "closed");
}

#[tokio::test]
async fn confirmed_close_stops_running_work_and_cancels_queued_work() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    insert_running_job(&orch.db.local, &project_id).await;
    insert_job(
        &orch.db.local,
        &project_id,
        "job-reviewer",
        "reviewer",
        "pending",
        2,
    )
    .await;
    insert_job(
        &orch.db.local,
        &project_id,
        "job-checkpoint",
        "approval",
        "blocked",
        3,
    )
    .await;

    update_status(
        &orch,
        "issue-1",
        "closed",
        ResolutionActor::Agent,
        Confirmation::Given,
    )
    .await
    .expect("a confirmed close proceeds");

    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "closed");

    // Work that never started is cancelled, transcript preserved.
    assert_eq!(
        job_status(&orch.db.local, "job-reviewer").await,
        "cancelled"
    );
    assert_eq!(
        job_status(&orch.db.local, "job-checkpoint").await,
        "cancelled"
    );

    // Running work goes through the canonical node stop: the turn is
    // interrupted and the run exits as a user stop, not an unresumable kill.
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("interrupted".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("exited".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("user_stop".to_string())
    );

    // No process is left executing against the closed issue, and nothing that
    // never started survives.
    assert_eq!(
        common::query_i64(
            &orch.db.local,
            "SELECT COUNT(*) FROM runs WHERE status IN ('starting', 'live')"
        )
        .await
        .unwrap(),
        0,
        "no run keeps executing against a closed issue"
    );
    let remaining = live_work_for_issue(&orch, "issue-1").await.unwrap();
    assert!(
        remaining.iter().all(|job| job.is_started()),
        "no queued work survives a confirmed close: {remaining:?}"
    );
    // The parked builder stays claimed rather than being force-failed: that is
    // what makes it resumable if the issue is reopened.
    assert_eq!(job_status(&orch.db.local, "job-builder").await, "running");
}

#[tokio::test]
async fn close_with_no_live_work_needs_no_confirmation() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    insert_job(
        &orch.db.local,
        &project_id,
        "job-done",
        "builder",
        "complete",
        1,
    )
    .await;

    update_status(
        &orch,
        "issue-1",
        "closed",
        ResolutionActor::Agent,
        Confirmation::Absent,
    )
    .await
    .expect("an issue with nothing live closes on the first attempt");
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "closed");
}

#[tokio::test]
async fn archived_jobs_are_not_live_work() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "res").await;
    insert_issue(&orch.db.local, &project_id, "issue-1").await;
    // A node removed from the snapshot is archived, not live: it would never run
    // again, and counting it taxed every later close with a confirmation.
    insert_job(
        &orch.db.local,
        &project_id,
        "job-archived",
        "dropped node",
        "cancelled",
        1,
    )
    .await;

    assert!(live_work_for_issue(&orch, "issue-1")
        .await
        .unwrap()
        .is_empty());
    update_status(
        &orch,
        "issue-1",
        "closed",
        ResolutionActor::Agent,
        Confirmation::Absent,
    )
    .await
    .expect("an archived job is not live work");
    assert_eq!(issue_status(&orch.db.local, "issue-1").await, "closed");
}
