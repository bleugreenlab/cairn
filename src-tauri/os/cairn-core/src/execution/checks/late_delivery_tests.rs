//! Late write-check delivery through the real system-direct path: what one
//! burst of commits, or one wave handed off twice, leaves in the recipient's
//! messages.

use super::write_wave::deliver_late_write_check_summary;
use crate::db::DbState;
use crate::orchestrator::Orchestrator;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use cairn_db::storage::RowExt;
use std::sync::Arc;
use tempfile::TempDir;

const JOB: &str = "job-late";
const RUN: &str = "run-late";

/// What a runner stop wrote into every lane of every pending wave at once.
const RUNNER_STOP: &str = "Checks: \u{26A0} cargo-test (cancelled: Cairn's runner is stopping) \u{00B7} \u{26A0} cargo-fmt (cancelled: Cairn's runner is stopping)";

async fn orchestrator_with_recipient(temp: &TempDir) -> Orchestrator {
    let local = LocalDb::open_migrated_for_test(temp.path().join("late.db"))
        .await
        .unwrap();
    for sql in [
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES ('proj-1', 'default', 'Test Project', 'proj', '/tmp/test-repo', 1, 1)",
        "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
         VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
         VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
         VALUES ('job-late', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 1, 1)",
        "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
         VALUES ('run-late', 'proj-1', 'job-late', 'live', 1, 1)",
    ] {
        local.execute(sql, ()).await.unwrap();
    }
    let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        temp.path().to_path_buf(),
    )
    .build()
}

fn commit(n: usize) -> String {
    format!("{n:x}{}", "c".repeat(39))
}

async fn late_messages(orch: &Orchestrator) -> Vec<String> {
    orch.db
        .local
        .query_all(
            "SELECT m.content FROM messages m
             JOIN attention_pushes p ON p.key = 'direct:' || m.id
             WHERE p.recipient = ?1 AND m.content LIKE '[Write checks]%'
             ORDER BY m.rowid",
            (JOB,),
            |row| row.text(0),
        )
        .await
        .unwrap()
}

/// The specimen: eight commits in a burst, each answered pending, and all
/// eight waves settling in the same second with the same text. The agent gets
/// one summary, the newest commit's, naming the seven it stands in for.
#[tokio::test(flavor = "multi_thread")]
async fn a_burst_of_eight_pending_commits_leaves_one_summary_for_the_newest() {
    let temp = TempDir::new().unwrap();
    let orch = orchestrator_with_recipient(&temp).await;
    let tickets: Vec<_> = (0..8)
        .map(|n| {
            let ticket = Arc::new(
                orch.late_write_checks
                    .open(JOB, &format!("toolu_burst_{n}")),
            );
            ticket.planned(&commit(n));
            ticket
        })
        .collect();
    for ticket in tickets {
        deliver_late_write_check_summary(orch.clone(), RUN.into(), ticket, RUNNER_STOP.into())
            .await;
    }

    let messages = late_messages(&orch).await;
    assert_eq!(messages.len(), 1, "{messages:#?}");
    let message = &messages[0];
    assert!(
        message.contains(&format!("commit {} (wave toolu_burst_7)", &commit(7)[..12])),
        "the summary names the newest commit and its wave: {message}"
    );
    assert!(
        message.contains("supersedes 7 earlier pending waves"),
        "{message}"
    );
    for n in 0..7 {
        assert!(message.contains(&commit(n)[..12]), "{message}");
    }
    assert!(message.contains("Cairn's runner is stopping"), "{message}");
}

/// One wave handed to late delivery twice writes one message.
#[tokio::test(flavor = "multi_thread")]
async fn a_wave_delivered_twice_writes_one_message() {
    let temp = TempDir::new().unwrap();
    let orch = orchestrator_with_recipient(&temp).await;
    let ticket = Arc::new(orch.late_write_checks.open(JOB, "toolu_once"));
    ticket.planned(&commit(1));
    for _ in 0..2 {
        deliver_late_write_check_summary(
            orch.clone(),
            RUN.into(),
            ticket.clone(),
            "Checks: \u{2713} rust-fmt (0.4s)".into(),
        )
        .await;
    }
    assert_eq!(late_messages(&orch).await.len(), 1);
}

/// Two waves that each settle as their job's newest, with the same verdict
/// text, read as two different commits.
#[tokio::test(flavor = "multi_thread")]
async fn summaries_for_two_commits_can_be_told_apart() {
    let temp = TempDir::new().unwrap();
    let orch = orchestrator_with_recipient(&temp).await;
    for (n, wave) in [(1, "toolu_first"), (2, "toolu_second")] {
        let ticket = Arc::new(orch.late_write_checks.open(JOB, wave));
        ticket.planned(&commit(n));
        deliver_late_write_check_summary(orch.clone(), RUN.into(), ticket, RUNNER_STOP.into())
            .await;
    }

    let messages = late_messages(&orch).await;
    assert_eq!(messages.len(), 2, "{messages:#?}");
    assert_ne!(messages[0], messages[1]);
    assert!(
        messages[0].contains(&format!("commit {} (wave toolu_first)", &commit(1)[..12])),
        "{}",
        messages[0]
    );
    assert!(
        messages[1].contains(&format!("commit {} (wave toolu_second)", &commit(2)[..12])),
        "{}",
        messages[1]
    );
}

/// Commit A fails rust-fmt; commit B's wave is then cancelled by a runner stop
/// before any verdict. The one message is B's, and it names A's failure, which
/// B's tree most likely still carries.
#[tokio::test(flavor = "multi_thread")]
async fn a_superseded_failure_survives_a_newer_wave_cancelled_without_a_verdict() {
    use crate::execution::late_write_checks::LaneVerdict;
    let temp = TempDir::new().unwrap();
    let orch = orchestrator_with_recipient(&temp).await;
    let a = Arc::new(orch.late_write_checks.open(JOB, "toolu_a"));
    a.planned(&commit(1));
    let b = Arc::new(orch.late_write_checks.open(JOB, "toolu_b"));
    b.planned(&commit(2));
    a.recorded([("rust-fmt", LaneVerdict::Failed)]);
    deliver_late_write_check_summary(
        orch.clone(),
        RUN.into(),
        a,
        "Checks: \u{2717} rust-fmt (exit 1)".into(),
    )
    .await;
    deliver_late_write_check_summary(orch.clone(), RUN.into(), b, RUNNER_STOP.into()).await;

    let messages = late_messages(&orch).await;
    assert_eq!(messages.len(), 1, "{messages:#?}");
    let message = &messages[0];
    assert!(
        message.contains(&format!("commit {} (wave toolu_b)", &commit(2)[..12])),
        "{message}"
    );
    assert!(
        message.contains(&format!("{}: rust-fmt failed", &commit(1)[..12])),
        "the fold names A's failure: {message}"
    );
    assert_eq!(
        urgency_of(&orch).await,
        vec!["queue".to_string()],
        "a folded failure wakes the agent like any failure"
    );
}

/// The control: B records its own verdict on the lane A failed, so B's
/// verdict is the truth and the message carries no failure from A.
#[tokio::test(flavor = "multi_thread")]
async fn a_newer_pass_on_the_same_lane_retires_the_superseded_failure() {
    use crate::execution::late_write_checks::LaneVerdict;
    let temp = TempDir::new().unwrap();
    let orch = orchestrator_with_recipient(&temp).await;
    let a = Arc::new(orch.late_write_checks.open(JOB, "toolu_a"));
    a.planned(&commit(1));
    let b = Arc::new(orch.late_write_checks.open(JOB, "toolu_b"));
    b.planned(&commit(2));
    a.recorded([("rust-fmt", LaneVerdict::Failed)]);
    deliver_late_write_check_summary(
        orch.clone(),
        RUN.into(),
        a,
        "Checks: \u{2717} rust-fmt (exit 1)".into(),
    )
    .await;
    b.recorded([("rust-fmt", LaneVerdict::Passed)]);
    deliver_late_write_check_summary(
        orch.clone(),
        RUN.into(),
        b,
        "Checks: \u{2713} rust-fmt (0.4s)".into(),
    )
    .await;

    let messages = late_messages(&orch).await;
    assert_eq!(messages.len(), 1, "{messages:#?}");
    let message = &messages[0];
    assert!(
        message.contains("supersedes 1 earlier pending wave"),
        "{message}"
    );
    assert!(!message.contains("rust-fmt failed"), "{message}");
    assert!(!message.contains("No newer wave"), "{message}");
}

async fn urgency_of(orch: &Orchestrator) -> Vec<String> {
    orch.db
        .local
        .query_all(
            "SELECT m.urgency FROM messages m
             JOIN attention_pushes p ON p.key = 'direct:' || m.id
             WHERE p.recipient = ?1 AND m.content LIKE '[Write checks]%'
             ORDER BY m.rowid",
            (JOB,),
            |row| row.text(0),
        )
        .await
        .unwrap()
}

/// A failure in a wave nothing superseded still queues; the message is
/// written either way, so this pins that the wave is not dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_wave_with_no_newer_planned_wave_reports_its_own_failure() {
    let temp = TempDir::new().unwrap();
    let orch = orchestrator_with_recipient(&temp).await;
    let failing = Arc::new(orch.late_write_checks.open(JOB, "toolu_src"));
    failing.planned(&commit(1));
    // A docs-only commit after it: its wave opens and plans nothing.
    let _docs = orch.late_write_checks.open(JOB, "toolu_docs");
    deliver_late_write_check_summary(
        orch.clone(),
        RUN.into(),
        failing,
        "Checks: \u{2717} cargo-test (1 of 40 failed, exit 101)".into(),
    )
    .await;
    let messages = late_messages(&orch).await;
    assert_eq!(messages.len(), 1, "{messages:#?}");
    assert!(messages[0].contains("1 of 40 failed"), "{}", messages[0]);
}
