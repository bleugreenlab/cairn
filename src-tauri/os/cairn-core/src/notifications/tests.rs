//! Each kind, seeded from its source rows, opens; its cause resolved the way
//! the owning surface resolves it, the entry reads resolved and leaves the
//! open view without the operator touching the center.
//!
//! Resolutions are applied as the row writes their command paths commit
//! (`answer_prompt_id` sets `response` and `answered_at`, a permission answer
//! sets `status` and `responded_at`, and so on): the derivation reads rows, so
//! the rows are the contract. Dismissals go through the real store.

use super::*;
use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
use std::sync::{Arc, Mutex};

/// "Now" for every derivation here, placed so the window opens at 100: the
/// fixture's small timestamps are recent enough to show as resolved.
const NOW: i64 = RECENT_WINDOW_SECS + 100;

async fn test_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("notifications.db"))
        .await
        .unwrap();
    std::mem::forget(temp);
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}

async fn exec(db: &LocalDb, sql: &str) {
    db.execute(sql.to_string(), ()).await.unwrap();
}

/// One project (`cairn`), a waiting issue #7 on execution 1, and its builder
/// node with a live run and a yielded current turn.
async fn fixture() -> LocalDb {
    let db = test_db().await;
    for sql in [
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES ('p', 'default', 'Cairn', 'cairn', '/tmp/r', 1, 1)",
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
         VALUES ('th', 'p', 'design', 'active', 'none', 1, 1)",
        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
         VALUES ('i7', 'p', 7, 'Seven', 'waiting', 1, 1)",
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
         VALUES ('x1', 'recipe', 'i7', 'p', 'running', 1, 1)",
        "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment, status,
                           created_at, updated_at)
         VALUES ('j1', 'x1', 'i7', 'p', 'builder', 'builder', 'running', 1, 1)",
        "INSERT INTO runs (id, job_id, project_id, issue_id, status, created_at, updated_at)
         VALUES ('r1', 'j1', 'p', 'i7', 'live', 1, 1)",
        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
         VALUES ('t1', 's1', 'r1', 'j1', 1, 'yielded', 100, 100)",
        "UPDATE jobs SET current_turn_id = 't1' WHERE id = 'j1'",
    ] {
        exec(&db, sql).await;
    }
    db
}

async fn entries_with(
    db: &LocalDb,
    holds: &HoldRegistry,
    dismissals: &HashMap<String, i64>,
) -> Vec<OperatorNotification> {
    derive_snapshot(db, holds, "p", NOW, dismissals)
        .await
        .unwrap()
        .entries
}

async fn entries(db: &LocalDb) -> Vec<OperatorNotification> {
    entries_with(db, &HoldRegistry::default(), &HashMap::new()).await
}

fn find<'a>(entries: &'a [OperatorNotification], key: &str) -> &'a OperatorNotification {
    entries
        .iter()
        .find(|entry| entry.key == key)
        .unwrap_or_else(|| panic!("no entry {key} in {entries:#?}"))
}

fn open_keys(entries: &[OperatorNotification]) -> Vec<String> {
    select_view(entries, NotificationView::Open, NOW - RECENT_WINDOW_SECS)
        .into_iter()
        .map(|entry| entry.key)
        .collect()
}

fn assert_resolved(entries: &[OperatorNotification], key: &str, at: i64) {
    let entry = find(entries, key);
    assert_eq!(entry.state, NotificationState::Resolved, "{key}");
    assert_eq!(entry.state_at, Some(at), "{key}");
    assert!(entry.action.is_none(), "a resolved entry offers no action");
    assert!(!open_keys(entries).contains(&key.to_string()));
    let all = select_view(entries, NotificationView::All, NOW - RECENT_WINDOW_SECS);
    assert!(
        all.iter().any(|entry| entry.key == key),
        "all still shows {key}"
    );
}

#[tokio::test]
async fn a_question_opens_and_resolves_when_answered() {
    let db = fixture().await;
    exec(
        &db,
        r#"INSERT INTO prompts (id, run_id, job_id, turn_id, questions, created_at)
           VALUES ('q1', 'r1', 'j1', 't1', '[{"question":"Which path?"}]', 500)"#,
    )
    .await;

    let listed = entries(&db).await;
    let question = find(&listed, "question:q1");
    assert!(question.is_open());
    assert_eq!(question.kind, NotificationKind::Question);
    assert_eq!(question.at, 500);
    assert_eq!(question.summary, "#7 builder asks: Which path?");
    assert_eq!(question.source_uri, "cairn://p/cairn/7/1/builder");
    assert_eq!(
        question.action,
        Some(NotificationAction::Question {
            prompt_id: "q1".into(),
            run_id: "r1".into(),
            questions: r#"[{"question":"Which path?"}]"#.into(),
        })
    );

    exec(
        &db,
        "UPDATE prompts SET response = 'left', answered_at = 600 WHERE id = 'q1'",
    )
    .await;
    assert_resolved(&entries(&db).await, "question:q1", 600);
}

/// A run that crashed leaves its unanswered prompt behind on a turn that is no
/// longer the job's current one. It is not something the operator can answer.
#[tokio::test]
async fn a_question_left_on_a_superseded_turn_is_not_open() {
    let db = fixture().await;
    exec(
        &db,
        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
         VALUES ('t0', 's1', 'r1', 'j1', 0, 'interrupted', 50, 50)",
    )
    .await;
    exec(
        &db,
        "INSERT INTO prompts (id, run_id, job_id, turn_id, questions, created_at)
         VALUES ('q0', 'r1', 'j1', 't0', '[]', 60)",
    )
    .await;
    assert!(entries(&db).await.is_empty());
}

#[tokio::test]
async fn a_permission_opens_and_resolves_when_answered() {
    let db = fixture().await;
    exec(
        &db,
        r#"INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input,
                                            status, created_at)
           VALUES ('perm1', 'r1', 'j1', 'tu1', 'Bash', '{"command":"ls"}', 'pending', 510)"#,
    )
    .await;

    let listed = entries(&db).await;
    let permission = find(&listed, "permission:perm1");
    assert!(permission.is_open());
    assert_eq!(permission.summary, "#7 builder needs permission for Bash");
    let Some(NotificationAction::Permission { request }) = &permission.action else {
        panic!("permission action expected");
    };
    assert_eq!(request.tool_input, serde_json::json!({"command": "ls"}));
    assert_eq!(request.run_id, "r1");

    exec(
        &db,
        "UPDATE permission_requests SET status = 'allowed', responded_at = 520 WHERE id = 'perm1'",
    )
    .await;
    assert_resolved(&entries(&db).await, "permission:perm1", 520);
}

#[tokio::test]
async fn a_proposed_tenet_opens_and_resolves_on_its_verdict() {
    let db = fixture().await;
    exec(
        &db,
        r#"INSERT INTO tenets (id, project_id, number, proposing_thread_id, statement, status, origin,
                               proposer_principal_json, created_at, updated_at)
           VALUES ('tn', 'p', 12, 'th', 'Entries resolve from their cause.', 'proposed',
                   'standing', '{}', 400, 400)"#,
    )
    .await;

    let listed = entries(&db).await;
    let tenet = find(&listed, "tenet:p:12");
    assert!(tenet.is_open());
    assert_eq!(tenet.source_uri, "cairn://p/cairn/tenets/12");
    assert_eq!(
        tenet.action,
        Some(NotificationAction::Tenet {
            number: 12,
            tenet_uri: "cairn://p/cairn/tenets/12".into(),
        })
    );

    exec(
        &db,
        "UPDATE tenets SET status = 'approved', verdict = 'yes', answered_at = 450, updated_at = 450
          WHERE id = 'tn'",
    )
    .await;
    assert_resolved(&entries(&db).await, "tenet:p:12", 450);
}

async fn open_pr(db: &LocalDb) {
    exec(
        db,
        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch,
                                     target_branch, status, opened_at, updated_at, checks_status)
         VALUES ('mr1', 'j1', 'p', 'i7', 'Add the bell', 'agent/x', 'main', 'open', 700, 700,
                 'SUCCESS')",
    )
    .await;
}

#[tokio::test]
async fn an_open_pr_opens_and_resolves_when_merged() {
    let db = fixture().await;
    open_pr(&db).await;

    let listed = entries(&db).await;
    let pr = find(&listed, "pr:mr1");
    assert!(pr.is_open());
    assert_eq!(pr.at, 700);
    assert_eq!(
        pr.summary,
        "#7 PR ready to merge: Add the bell (checks passing)"
    );
    assert_eq!(
        pr.action,
        Some(NotificationAction::Merge {
            action_run_id: "j1".into(),
            merge_method: "squash".into(),
        })
    );

    exec(
        &db,
        "UPDATE merge_requests SET status = 'merged', merged_at = 800 WHERE id = 'mr1'",
    )
    .await;
    assert_resolved(&entries(&db).await, "pr:mr1", 800);
}

#[tokio::test]
async fn an_open_pr_resolves_when_closed_or_when_its_issue_resolves() {
    let db = fixture().await;
    open_pr(&db).await;
    exec(
        &db,
        "UPDATE merge_requests SET status = 'closed', closed_at = 810 WHERE id = 'mr1'",
    )
    .await;
    assert_resolved(&entries(&db).await, "pr:mr1", 810);

    // A superseded execution's PR row stays `open` after the issue merges.
    exec(
        &db,
        "UPDATE merge_requests SET status = 'open', closed_at = NULL WHERE id = 'mr1'",
    )
    .await;
    exec(
        &db,
        "UPDATE issues SET status = 'merged', updated_at = 820 WHERE id = 'i7'",
    )
    .await;
    assert_resolved(&entries(&db).await, "pr:mr1", 820);
}

#[tokio::test]
async fn a_crashed_agent_opens_and_its_next_turn_resolves_it() {
    let db = fixture().await;
    exec(
        &db,
        "UPDATE turns SET state = 'interrupted', end_reason = 'crash', ended_at = 300
          WHERE id = 't1'",
    )
    .await;
    // Recent, so the resolved form is still inside the window afterwards.
    exec(
        &db,
        &format!("UPDATE turns SET created_at = {} WHERE id = 't1'", NOW - 60),
    )
    .await;

    exec(
        &db,
        &format!(
            "UPDATE turns SET ended_at = {} WHERE id = 't1'",
            NOW - AGENT_FAILURE_SETTLE_SECS + 5
        ),
    )
    .await;
    assert!(
        entries(&db).await.is_empty(),
        "a crash too fresh to call may still be recovering"
    );

    exec(
        &db,
        &format!(
            "UPDATE turns SET ended_at = {} WHERE id = 't1'",
            NOW - AGENT_FAILURE_SETTLE_SECS - 5
        ),
    )
    .await;
    let listed = entries(&db).await;
    let failed = find(&listed, "agent_failed:t1");
    assert!(failed.is_open());
    assert_eq!(failed.summary, "#7 builder crashed");

    exec(
        &db,
        &format!(
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at,
                                updated_at)
             VALUES ('t2', 's1', 'r1', 'j1', 2, 'running', {at}, {at})",
            at = NOW - 30
        ),
    )
    .await;
    exec(
        &db,
        "UPDATE jobs SET current_turn_id = 't2' WHERE id = 'j1'",
    )
    .await;
    assert_resolved(&entries(&db).await, "agent_failed:t1", NOW - 30);
}

/// A failure that never went through finalization (a launch that failed
/// before any run existed) is held back like any other, and the snapshot says
/// when it settles so the projection rebuilds then without a write.
#[tokio::test]
async fn a_fresh_failure_reports_when_it_settles() {
    let db = fixture().await;
    exec(
        &db,
        &format!(
            "UPDATE turns SET state = 'failed', run_id = NULL, ended_at = {} WHERE id = 't1'",
            NOW - 10
        ),
    )
    .await;
    let snapshot = derive_snapshot(&db, &HoldRegistry::default(), "p", NOW, &HashMap::new())
        .await
        .unwrap();
    assert!(snapshot.entries.is_empty());
    assert_eq!(
        snapshot.next_change_at,
        Some(NOW - 10 + AGENT_FAILURE_SETTLE_SECS)
    );

    let settled = derive_snapshot(
        &db,
        &HoldRegistry::default(),
        "p",
        NOW - 10 + AGENT_FAILURE_SETTLE_SECS + 1,
        &HashMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        open_keys(&settled.entries),
        vec!["agent_failed:t1".to_string()]
    );
    assert_eq!(settled.next_change_at, None);
}

/// A warm run hosts many turns. Dismissing one failure must not silence the
/// next one in the same run.
#[tokio::test]
async fn a_dismissed_failure_does_not_hide_a_later_failure_in_the_same_run() {
    let db = fixture().await;
    exec(
        &db,
        &format!(
            "UPDATE turns SET state = 'failed', ended_at = {at}, created_at = {at} WHERE id = 't1'",
            at = NOW - 600
        ),
    )
    .await;
    let mut dismissals = HashMap::new();
    dismissals.insert("agent_failed:t1".to_string(), NOW - 500);
    let holds = HoldRegistry::default();
    let listed = entries_with(&db, &holds, &dismissals).await;
    assert_eq!(
        find(&listed, "agent_failed:t1").state,
        NotificationState::Dismissed
    );

    exec(
        &db,
        &format!(
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at,
                                updated_at, ended_at)
             VALUES ('t2', 's1', 'r1', 'j1', 2, 'failed', {at}, {at}, {at})",
            at = NOW - 300
        ),
    )
    .await;
    exec(
        &db,
        "UPDATE jobs SET current_turn_id = 't2' WHERE id = 'j1'",
    )
    .await;
    let listed = entries_with(&db, &holds, &dismissals).await;
    assert_eq!(open_keys(&listed), vec!["agent_failed:t2".to_string()]);
}

/// An unanswered question on a job that already ended, or on a cancelled
/// turn, cannot resume anything and is not open.
#[tokio::test]
async fn a_question_on_a_finished_job_or_cancelled_turn_is_not_open() {
    let db = fixture().await;
    exec(
        &db,
        "INSERT INTO prompts (id, run_id, job_id, turn_id, questions, created_at)
         VALUES ('q1', 'r1', 'j1', 't1', '[]', 500)",
    )
    .await;
    assert_eq!(
        open_keys(&entries(&db).await),
        vec!["question:q1".to_string()]
    );

    exec(&db, "UPDATE jobs SET status = 'failed' WHERE id = 'j1'").await;
    assert!(open_keys(&entries(&db).await).is_empty(), "failed job");

    exec(&db, "UPDATE jobs SET status = 'running' WHERE id = 'j1'").await;
    exec(&db, "UPDATE turns SET state = 'cancelled' WHERE id = 't1'").await;
    assert!(open_keys(&entries(&db).await).is_empty(), "cancelled turn");
}

#[tokio::test]
async fn a_blocked_review_opens_and_leaves_when_confirmed() {
    let db = fixture().await;
    exec(&db, "UPDATE jobs SET status = 'blocked' WHERE id = 'j1'").await;
    exec(
        &db,
        "INSERT INTO artifacts (id, job_id, artifact_type, data, output_name, created_at,
                                updated_at, confirmed)
         VALUES ('a1', 'j1', 'plan', '{}', 'plan', 650, 650, 0)",
    )
    .await;

    let listed = entries(&db).await;
    let review = find(&listed, "review:a1");
    assert!(review.is_open());
    assert_eq!(review.summary, "#7 builder: plan awaits your review");
    assert_eq!(review.source_uri, "cairn://p/cairn/7/1/builder/plan");

    exec(&db, "UPDATE artifacts SET confirmed = 1 WHERE id = 'a1'").await;
    exec(&db, "UPDATE jobs SET status = 'running' WHERE id = 'j1'").await;
    assert!(open_keys(&entries(&db).await).is_empty());
}

/// A planner stored `running` while its plan waits (as cairn/4527's was) is
/// still awaiting review. Past a week undeferred it reads stale, and the list
/// says when that happens so the cache rebuilds then.
#[tokio::test]
async fn a_running_job_with_a_pending_plan_awaits_review_and_turns_stale() {
    let db = fixture().await;
    let written = NOW - 3 * 86_400;
    exec(
        &db,
        &format!(
            "INSERT INTO artifacts (id, job_id, artifact_type, data, output_name, created_at,
                                    updated_at, confirmed)
             VALUES ('a1', 'j1', 'plan', '{{}}', 'plan', {written}, {written}, 0)"
        ),
    )
    .await;

    let snapshot = derive_snapshot(&db, &HoldRegistry::default(), "p", NOW, &HashMap::new())
        .await
        .unwrap();
    let review = find(&snapshot.entries, "review:a1");
    assert!(review.is_open());
    assert_eq!(
        review.at, written,
        "the entry's time is the age of the wait"
    );
    assert!(!review.stale);
    assert_eq!(review.deferred_until, None);
    let stale_from = written + crate::execution::approvals::STALE_AFTER_SECS + 1;
    assert_eq!(snapshot.next_change_at, Some(stale_from));

    let later = derive_snapshot(
        &db,
        &HoldRegistry::default(),
        "p",
        stale_from,
        &HashMap::new(),
    )
    .await
    .unwrap();
    let review = find(&later.entries, "review:a1");
    assert!(review.stale);
    let wire = serde_json::to_value(review).unwrap();
    assert_eq!(wire["stale"], serde_json::json!(true));
    assert!(
        wire.get("deferredUntil").is_none(),
        "absent while undeferred"
    );
}

/// A deferred review stays listed, marked with its cause and never stale.
/// Once the deferral fires it is a new occurrence: an earlier dismissal does
/// not hide it.
#[tokio::test]
async fn a_deferred_review_is_marked_and_resurfaces_as_a_new_occurrence() {
    let db = fixture().await;
    exec(
        &db,
        "INSERT INTO artifacts (id, job_id, artifact_type, data, output_name, created_at,
                                updated_at, confirmed)
         VALUES ('a1', 'j1', 'plan', '{}', 'plan', 1, 1, 0)",
    )
    .await;
    exec(
        &db,
        r#"INSERT INTO ask_conditions (id, ask_kind, ask_id, job_id, issue_id, effect, until_json, created_at)
           VALUES ('c1', 'artifact', 'a1', 'j1', 'i7', 'resurface',
                   '[{"fact":"issue_resolution","issue":"cairn://p/cairn/4526","resolution":"merged"}]', 2)"#,
    )
    .await;
    let listed = entries(&db).await;
    let review = find(&listed, "review:a1");
    assert!(review.is_open(), "deferred, not hidden");
    assert_eq!(
        review.deferred_until.as_deref(),
        Some("issue cairn://p/cairn/4526 merged")
    );
    assert!(!review.stale, "a deferral is its own explanation");

    let dismissals = HashMap::from([("review:a1".to_string(), 3)]);
    let listed = entries_with(&db, &HoldRegistry::default(), &dismissals).await;
    assert!(open_keys(&listed).is_empty(), "dismissed while deferred");

    exec(
        &db,
        "UPDATE ask_conditions SET fired_at = 4, fired_kind = 'satisfied', delivered_at = 4 WHERE id = 'c1'",
    )
    .await;
    let listed = entries_with(&db, &HoldRegistry::default(), &dismissals).await;
    assert_eq!(open_keys(&listed), vec!["review:a1:c1".to_string()]);
    assert_eq!(find(&listed, "review:a1:c1").deferred_until, None);
}

/// The center lists only what waits on the operator. A thread's unread replies
/// are the sidebar's badge, and a red check on a PR's node is its agent's to
/// fix; neither is an entry.
#[tokio::test]
async fn unread_thread_replies_and_a_red_check_are_not_entries() {
    let db = fixture().await;
    open_pr(&db).await;
    for sql in [
        "INSERT INTO check_result_cache (project_id, tree_hash, input_hash, check_name, exit_code,
                                         passed, output_tail, duration_ms, ran_at, job_id)
         VALUES ('p', 'tree', 'a', 'rust-tests', 1, 0, '', 1, 910000, 'j1')",
        "INSERT INTO jobs (id, thread_id, project_id, node_name, status, created_at, updated_at,
                           uri_segment)
         VALUES ('th-session', 'th', 'p', 'thread', 'idle', 1, 1, 'thread')",
        "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('ts', 'th-session', 1, 1)",
        "INSERT INTO runs (id, job_id, created_at, updated_at) VALUES ('tr', 'th-session', 1, 1)",
        "INSERT INTO events (id, run_id, session_id, sequence, timestamp, event_type, data,
                             created_at)
         VALUES ('ev1', 'tr', 'ts', 1, 1000, 'assistant', '{}', 1000)",
    ] {
        exec(&db, sql).await;
    }
    assert_eq!(
        crate::jobs::queries::thread_unread_counts(&db, "p")
            .await
            .unwrap()["th"]
            .count,
        1,
        "the thread has an unread reply"
    );

    let listed = entries(&db).await;
    assert_eq!(
        listed
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        vec!["pr:mr1"]
    );
}

#[tokio::test]
async fn a_dismissal_survives_rebuilds_and_source_updates_but_not_a_new_occurrence() {
    let db = fixture().await;
    open_pr(&db).await;
    assert!(record_dismissal(&db, "p", "pr:mr1", 750).await.unwrap());
    assert!(
        !record_dismissal(&db, "p", "pr:mr1", 760).await.unwrap(),
        "a second close is a no-op"
    );
    let dismissals = load_dismissals(&db, "p").await.unwrap();

    let holds = HoldRegistry::default();
    let listed = entries_with(&db, &holds, &dismissals).await;
    let pr = find(&listed, "pr:mr1");
    assert_eq!(pr.state, NotificationState::Dismissed);
    assert_eq!(pr.state_at, Some(750));
    assert!(open_keys(&listed).is_empty());

    // The PR moves on (a refresh bumps it); the operator's close holds.
    exec(
        &db,
        "UPDATE merge_requests SET updated_at = 900, checks_status = 'FAILURE' WHERE id = 'mr1'",
    )
    .await;
    let listed = entries_with(&db, &holds, &dismissals).await;
    assert_eq!(find(&listed, "pr:mr1").state, NotificationState::Dismissed);

    // A new PR is a new occurrence, and open.
    exec(
        &db,
        "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment, status,
                           created_at, updated_at)
         VALUES ('j2', 'x1', 'i7', 'p', 'builder', 'builder-2', 'running', 1, 1)",
    )
    .await;
    exec(
        &db,
        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch,
                                     target_branch, status, opened_at, updated_at)
         VALUES ('mr2', 'j2', 'p', 'i7', 'Again', 'agent/y', 'main', 'open', 950, 950)",
    )
    .await;
    let listed = entries_with(&db, &holds, &dismissals).await;
    assert_eq!(open_keys(&listed), vec!["pr:mr2".to_string()]);
}

#[derive(Default)]
struct FixtureHolds {
    released_at: Mutex<Option<i64>>,
    overridden: Mutex<Vec<String>>,
}

impl HoldSource for FixtureHolds {
    fn name(&self) -> &'static str {
        "queue"
    }

    fn holds(&self, _project_id: &str) -> Vec<Hold> {
        vec![Hold {
            id: "h1".into(),
            project_id: Some("p".into()),
            reason: "Queued: position 3 of 7".into(),
            expected_release: Some(ExpectedRelease::At(NOW + 600)),
            override_label: "Start now".into(),
            source_uri: None,
            opened_at: NOW - 120,
            released_at: *self.released_at.lock().unwrap(),
        }]
    }

    fn override_hold(&self, hold_id: &str) -> Result<(), String> {
        self.overridden.lock().unwrap().push(hold_id.to_string());
        *self.released_at.lock().unwrap() = Some(NOW - 5);
        Ok(())
    }
}

#[tokio::test]
async fn a_hold_renders_its_reason_release_and_override_and_resolves_when_released() {
    let db = fixture().await;
    let source = Arc::new(FixtureHolds::default());
    let holds = HoldRegistry::default();
    holds.register(source.clone());

    let listed = entries_with(&db, &holds, &HashMap::new()).await;
    let hold = find(&listed, "hold:queue:h1");
    assert!(hold.is_open());
    assert_eq!(hold.kind, NotificationKind::Hold);
    assert_eq!(hold.summary, "Queued: position 3 of 7");
    assert_eq!(hold.source_uri, "cairn://p/cairn");
    assert_eq!(
        serde_json::to_value(&hold.action).unwrap(),
        serde_json::json!({
            "kind": "hold",
            "source": "queue",
            "holdId": "h1",
            "reason": "Queued: position 3 of 7",
            "expectedRelease": {"at": NOW + 600},
            "overrideLabel": "Start now",
        })
    );

    holds.override_hold("queue", "h1").unwrap();
    assert_eq!(*source.overridden.lock().unwrap(), vec!["h1".to_string()]);
    assert!(holds.override_hold("elsewhere", "h1").is_err());
    assert_resolved(
        &entries_with(&db, &holds, &HashMap::new()).await,
        "hold:queue:h1",
        NOW - 5,
    );
}

/// The frontend reads these names; a rename here is a contract break there.
#[test]
fn the_wire_shape_is_camel_case_with_snake_case_kinds() {
    let entry = OperatorNotification {
        key: "pr:mr1".into(),
        project_id: "p".into(),
        project_key: "cairn".into(),
        kind: NotificationKind::PrReady,
        at: 1,
        summary: "s".into(),
        source_uri: "cairn://p/cairn".into(),
        state: NotificationState::Open,
        state_at: None,
        action: Some(NotificationAction::Merge {
            action_run_id: "j1".into(),
            merge_method: "squash".into(),
        }),
        deferred_until: None,
        stale: false,
    };
    assert_eq!(
        serde_json::to_value(&entry).unwrap(),
        serde_json::json!({
            "key": "pr:mr1",
            "projectId": "p",
            "projectKey": "cairn",
            "kind": "pr_ready",
            "at": 1,
            "summary": "s",
            "sourceUri": "cairn://p/cairn",
            "state": "open",
            "stateAt": null,
            "action": {"kind": "merge", "actionRunId": "j1", "mergeMethod": "squash"},
        })
    );
}
