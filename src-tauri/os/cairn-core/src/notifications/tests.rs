//! Each kind, seeded from its source rows, opens; its cause resolved the way
//! the owning surface resolves it, the entry reads resolved and leaves the
//! open view without the operator touching the center.
//!
//! Resolutions are applied as the row writes their command paths commit
//! (`answer_prompt_id` sets `response` and `answered_at`, a permission answer
//! sets `status` and `responded_at`, and so on): the derivation reads rows, so
//! the rows are the contract. Dismissals go through the real store.

use super::*;

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
        headline: None,
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

/// A PR-ready entry carries the line its coordinating thread wrote about the
/// issue, the newest one when there are several; an entry no line points at
/// carries none.
#[tokio::test]
async fn a_pr_entry_carries_the_newest_headline_about_its_issue() {
    use crate::threads::db_store;
    let dir = tempfile::tempdir().unwrap();
    let db = crate::storage::migrated_test_db("notif-headline").await;
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-nh', 'default', 'Headlines', 'nh', '/tmp/nh', 1, 1);
         INSERT INTO threads (id, project_id, name, created_at, updated_at)
           VALUES ('nh-thread', 'p-nh', 'coord', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES
           ('nh-i7', 'p-nh', 7, 'Seven', 'active', 1, 1),
           ('nh-i8', 'p-nh', 8, 'Eight', 'active', 1, 1);
         INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at) VALUES
           ('nh-mr7', 'nh-gone-7', 'p-nh', 'nh-i7', 'Seven', 'agent/7', 'main', 'open', 10, 10),
           ('nh-mr8', 'nh-gone-8', 'p-nh', 'nh-i8', 'Eight', 'agent/8', 'main', 'open', 20, 20);",
    )
    .await
    .unwrap();
    db_store::use_test_path("nh-thread", dir.path().join("nh-thread.db"));
    let store = db_store::open("nh-thread").await.unwrap();
    for (ts, line, link) in [
        (
            "2026-11-10T01:00:00Z",
            "Older word on #7.",
            "cairn://p/nh/7",
        ),
        (
            "2026-11-10T02:00:00Z",
            "Merging #7 retires the old home.",
            "cairn://p/nh/7",
        ),
        ("2026-11-10T03:00:00Z", "Unrelated.", "cairn://p/nh/99"),
    ] {
        store
            .execute(
                "INSERT INTO headlines(ts, line, ref, until) VALUES (?1, ?2, ?3, '2099-01-01T00:00:00Z')",
                cairn_db::turso::params![ts, line, link],
            )
            .await
            .unwrap();
    }

    let snapshot = derive_snapshot(
        &db,
        &db,
        &HoldRegistry::default(),
        "p-nh",
        1_000,
        &HashMap::new(),
    )
    .await
    .unwrap();
    let by_key: HashMap<_, _> = snapshot
        .entries
        .iter()
        .map(|entry| (entry.key.as_str(), entry))
        .collect();
    let seven = by_key["pr:nh-mr7"];
    assert_eq!(seven.kind, NotificationKind::PrReady);
    assert_eq!(
        seven.headline,
        Some(crate::threads::headlines::HeadlineAbout {
            line: "Merging #7 retires the old home.".into(),
            uri: "cairn://p/nh/coord/db/headlines/2".into(),
        })
    );
    assert_eq!(by_key["pr:nh-mr8"].headline, None);
    let wire = serde_json::to_value(seven).unwrap();
    assert_eq!(
        wire["headline"],
        serde_json::json!({"line": "Merging #7 retires the old home.", "uri": "cairn://p/nh/coord/db/headlines/2"})
    );
}

#[test]
fn the_missing_output_kind_is_snake_case_on_the_wire() {
    assert_eq!(
        serde_json::to_value(NotificationKind::MissingOutput).unwrap(),
        serde_json::json!("missing_output")
    );
}

/// cairn/5960: a planner whose turn ended without its plan is an open,
/// link-only entry at the node, dated by the turn's end, and it leaves the
/// list as soon as the plan is written (which makes it an approval instead).
#[tokio::test]
async fn a_job_blocked_on_its_missing_output_is_an_open_link_only_entry() {
    let db = crate::storage::migrated_test_db("notif-missing-output").await;
    crate::execution::missing_output::tests::seed_blocked_planner(&db, "i4", 4, "j-plan", 500)
        .await;
    let derive = || async {
        derive_snapshot(
            &db,
            &db,
            &HoldRegistry::default(),
            "p",
            500 + 30 * 86_400,
            &HashMap::new(),
        )
        .await
        .unwrap()
    };

    let snapshot = derive().await;
    let missing: Vec<_> = snapshot
        .entries
        .iter()
        .filter(|entry| entry.kind == NotificationKind::MissingOutput)
        .collect();
    assert_eq!(missing.len(), 1, "{:#?}", snapshot.entries);
    let entry = missing[0];
    assert_eq!(entry.key, "missing-output:j-plan:500");
    assert_eq!(entry.at, 500);
    assert_eq!(entry.summary, "#4 Planner: blocked, no plan");
    assert_eq!(entry.source_uri, "cairn://p/prj/4/1/planner");
    assert_eq!(entry.state, NotificationState::Open);
    assert_eq!(entry.action, None);
    assert!(!entry.stale);
    assert_eq!(
        snapshot.next_change_at, None,
        "an age is rendered by the reader; nothing reschedules the list"
    );

    db.execute(
        "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
         VALUES ('a1', 'j-plan', 'plan', '{}', 1, 'plan', 0, 600, 600)",
        (),
    )
    .await
    .unwrap();
    let snapshot = derive().await;
    let kinds: Vec<_> = snapshot.entries.iter().map(|entry| entry.kind).collect();
    assert_eq!(kinds, vec![NotificationKind::AwaitingReview]);
}

/// cairn/5519: a thread's non-blocking question stays open after its turn has
/// moved on, links its own URI and carries its context; it resolves on an
/// answer, a handled or moot closure (by the operator or its condition), and
/// a withdrawal. A blocking question off the current turn is not listed.
#[tokio::test]
async fn a_threads_non_blocking_question_stays_open_across_turns_and_resolves_every_way() {
    let db = crate::storage::migrated_test_db("notif-thread-question").await;
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-tq', 'default', 'Q', 'tq', '/tmp/tq', 1, 1);
         INSERT INTO threads (id, project_id, name, created_at, updated_at)
           VALUES ('tq-thread', 'p-tq', 'platform', 1, 1);
         INSERT INTO executions (id, recipe_id, project_id, status, started_at, seq)
           VALUES ('tq-e', 'thread', 'p-tq', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, thread_id, project_id, status, uri_segment, node_name, created_at, updated_at)
           VALUES ('tq-job', 'tq-e', 'tq-thread', 'p-tq', 'running', 'thread', 'thread', 1, 1);
         INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('tq-s', 'tq-job', 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('tq-run', 'tq-job', 'tq-s', 'live', 1, 1);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
           VALUES ('tq-t1', 'tq-s', 'tq-run', 'tq-job', 1, 'completed', 'initial', 1, 1),
                  ('tq-t2', 'tq-s', 'tq-run', 'tq-job', 2, 'running', 'user_message', 2, 2);
         UPDATE jobs SET current_turn_id = 'tq-t2' WHERE id = 'tq-job';
         INSERT INTO prompts (id, run_id, job_id, questions, created_at, turn_id, uri_segment, background, context)
           VALUES ('tq-p1', 'tq-run', 'tq-job', '[{\"question\":\"Merge #4382?\"}]', 10, 'tq-t1', 'q-1', 1, 'About cairn://p/tq/4382');
         INSERT INTO prompts (id, run_id, job_id, questions, created_at, turn_id, uri_segment, background)
           VALUES ('tq-p2', 'tq-run', 'tq-job', '[{\"question\":\"Blocking, on an old turn?\"}]', 11, 'tq-t1', 'q-2', 0);",
    )
    .await
    .unwrap();
    let derive = || async {
        derive_snapshot(
            &db,
            &db,
            &HoldRegistry::default(),
            "p-tq",
            1_000,
            &HashMap::new(),
        )
        .await
        .unwrap()
    };
    let question = |snapshot: &OperatorNotificationsSnapshot| {
        snapshot
            .entries
            .iter()
            .find(|entry| entry.key == "question:tq-p1")
            .cloned()
    };

    let snapshot = derive().await;
    assert!(
        !snapshot
            .entries
            .iter()
            .any(|entry| entry.key == "question:tq-p2"),
        "a blocking question holds only its current turn: {:#?}",
        snapshot.entries
    );
    let entry = question(&snapshot).expect("the non-blocking question is listed");
    assert_eq!(entry.state, NotificationState::Open);
    assert_eq!(entry.summary, "thread platform asks: Merge #4382?");
    assert_eq!(entry.source_uri, "cairn://p/tq/platform");
    assert_eq!(
        entry.action,
        Some(NotificationAction::Question {
            prompt_id: "tq-p1".into(),
            run_id: "tq-run".into(),
            questions: "[{\"question\":\"Merge #4382?\"}]".into(),
            uri: Some("cairn://p/tq/platform/questions/q-1".into()),
            context: Some("About cairn://p/tq/4382".into()),
        })
    );

    for close in [
        "UPDATE prompts SET response = '[{\"index\":0,\"answer\":\"yes\"}]', answered_at = 20 WHERE id = 'tq-p1'",
        "UPDATE prompts SET response = '{\"form\":\"handled\"}', answered_at = 20 WHERE id = 'tq-p1'",
        "UPDATE prompts SET response = '{\"form\":\"moot\",\"condition\":{\"id\":\"c\",\"reason\":\"merged\"}}', answered_at = 20 WHERE id = 'tq-p1'",
        "UPDATE prompts SET withdrawn_at = 20, withdrawn_cause = 'retracted' WHERE id = 'tq-p1'",
    ] {
        db.execute(close, ()).await.unwrap();
        let entry = question(&derive().await).expect("a resolved question is still listed recently");
        assert_eq!(entry.state, NotificationState::Resolved, "{close}");
        assert_eq!(entry.state_at, Some(20), "{close}");
        db.execute(
            "UPDATE prompts SET response = NULL, answered_at = NULL, withdrawn_at = NULL, withdrawn_cause = NULL WHERE id = 'tq-p1'",
            (),
        )
        .await
        .unwrap();
    }
}
