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

// ── Which writes can move the center ──
//
// The generation cache skips a `turns` or `jobs` frame unless it names a job or
// turn the last rebuild read, or commits a state that opens an entry by
// itself. These tests hold that rule to the derivation: the frames come from
// the real builders after real writes, and each class either leaves the
// derived answer unchanged or is a frame the cache lets through.

use crate::orchestrator::generation_cache::dirties_notification_center;

const NOW: i64 = 10_000;

/// One project with an agent in each situation the center reads: one asking a
/// blocking question, one working, a planner that ended with its plan
/// unconfirmed, and one whose turn failed five seconds ago (held back).
async fn lifecycle_fixture(name: &str) -> LocalDb {
    let db = crate::storage::migrated_test_db(name).await;
    let mut script = String::from(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-lc', 'default', 'Lifecycle', 'lc', '/tmp/lc', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('lc-i1', 'p-lc', 1, 'One', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, project_id, issue_id, status, started_at, seq)
           VALUES ('lc-e', 'build', 'p-lc', 'lc-i1', 'running', 1, 1);",
    );
    for (job, status, turn, state, created, ended) in [
        ("asker", "running", "a1", "yielded", 100, None),
        ("worker", "running", "w1", "complete", 100, Some(200)),
        ("planner", "complete", "p1", "complete", 100, Some(200)),
        ("flaky", "running", "f1", "failed", NOW - 50, Some(NOW - 5)),
    ] {
        let ended = ended.map_or("NULL".to_string(), |at: i64| at.to_string());
        script.push_str(&format!(
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('lc-{job}', 'lc-e', 'lc-i1', 'p-lc', '{status}', '{job}', '{job}', 1, 1);
             INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('lc-{job}-s', 'lc-{job}', 1, 1);
             INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
               VALUES ('lc-{job}-r', 'lc-{job}', 'lc-{job}-s', 'live', 1, 1);
             INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at, ended_at)
               VALUES ('lc-{turn}', 'lc-{job}-s', 'lc-{job}-r', 'lc-{job}', 1, '{state}', 'initial', {created}, {created}, {ended});
             UPDATE jobs SET current_turn_id = 'lc-{turn}' WHERE id = 'lc-{job}';"
        ));
    }
    script.push_str(
        "INSERT INTO prompts (id, run_id, job_id, questions, created_at, turn_id, uri_segment, background)
           VALUES ('lc-q', 'lc-asker-r', 'lc-asker', '[{\"question\":\"Ship it?\"}]', 150, 'lc-a1', 'q-1', 0);
         INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
           VALUES ('lc-plan', 'lc-planner', 'plan', '{}', 1, 'plan', 0, 150, 150);",
    );
    db.execute_script(&script).await.unwrap();
    db
}

async fn derive_lifecycle(db: &LocalDb) -> OperatorNotificationsSnapshot {
    derive_snapshot(
        db,
        db,
        &HoldRegistry::default(),
        "p-lc",
        NOW,
        &HashMap::new(),
    )
    .await
    .unwrap()
}

/// A write, and the frame its real call site would announce for it.
enum Write {
    Turn(&'static str, &'static str, &'static str),
    Job(&'static str, &'static str),
    Run(&'static str, &'static str),
}

impl Write {
    async fn apply(&self, db: &LocalDb) -> serde_json::Value {
        match self {
            Self::Turn(sql, turn, action) => {
                db.execute(*sql, ()).await.unwrap();
                crate::notify::turn_db_change_for_id(db, turn, action).await
            }
            Self::Job(sql, job) => {
                db.execute(*sql, ()).await.unwrap();
                crate::notify::job_db_change_for_id(db, job, "update").await
            }
            Self::Run(sql, run) => {
                db.execute(*sql, ()).await.unwrap();
                crate::notify::run_db_change_for_id(db, run, "update").await
            }
        }
    }
}

/// The in-test measurement the frame filter was built for: a whole agent turn
/// on a job with no entry, through to its run ending, moves nothing and
/// dirties the center zero times. The same turn ending in failure moves the
/// answer and dirties it.
#[tokio::test]
async fn a_working_agents_turn_dirties_the_center_zero_times_and_a_failure_does() {
    let db = lifecycle_fixture("notif-lifecycle-tally").await;
    let published = derive_lifecycle(&db).await;
    assert!(published
        .entries
        .iter()
        .any(|entry| entry.key == "question:lc-q"));
    assert_eq!(
        published.next_change_at,
        Some(NOW + 25),
        "flaky is held back"
    );
    for job in ["lc-asker", "lc-planner", "lc-flaky"] {
        assert!(published.watched_job_ids.contains(job), "{job} is read");
    }
    assert!(
        !published.watched_job_ids.contains("lc-worker"),
        "a job with nothing to show is not"
    );
    for turn in ["lc-a1", "lc-f1"] {
        assert!(published.watched_turn_ids.contains(turn), "{turn} is read");
    }

    let mut turn = vec![
        Write::Turn(
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
               VALUES ('lc-w2', 'lc-worker-s', 'lc-worker-r', 'lc-worker', 2, 'pending', 'user_message', 9000, 9000)",
            "lc-w2",
            "insert",
        ),
        Write::Job("UPDATE jobs SET current_turn_id = 'lc-w2' WHERE id = 'lc-worker'", "lc-worker"),
        Write::Turn("UPDATE turns SET state = 'running' WHERE id = 'lc-w2'", "lc-w2", "update"),
    ];
    for _ in 0..5 {
        turn.push(Write::Job(
            "UPDATE jobs SET updated_at = updated_at + 1 WHERE id = 'lc-worker'",
            "lc-worker",
        ));
    }
    turn.extend([
        Write::Turn(
            "UPDATE turns SET state = 'complete', ended_at = 9500 WHERE id = 'lc-w2'",
            "lc-w2",
            "update",
        ),
        Write::Job(
            "UPDATE jobs SET status = 'idle' WHERE id = 'lc-worker'",
            "lc-worker",
        ),
        Write::Run(
            "UPDATE runs SET status = 'exited' WHERE id = 'lc-worker-r'",
            "lc-worker-r",
        ),
    ]);
    let mut dirtied = 0;
    for write in &turn {
        let frame = write.apply(&db).await;
        if dirties_notification_center(Some(&published), &frame) {
            dirtied += 1;
            eprintln!("dirtied by {frame}");
        }
    }
    assert_eq!(dirtied, 0, "{} writes", turn.len());
    assert_eq!(
        derive_lifecycle(&db).await,
        published,
        "and the answer did not move"
    );

    let failing = [
        Write::Turn(
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
               VALUES ('lc-w3', 'lc-worker-s', 'lc-worker-r', 'lc-worker', 3, 'running', 'user_message', 9600, 9600)",
            "lc-w3",
            "insert",
        ),
        Write::Job("UPDATE jobs SET current_turn_id = 'lc-w3', status = 'running' WHERE id = 'lc-worker'", "lc-worker"),
        Write::Turn(
            "UPDATE turns SET state = 'failed', ended_at = 9700 WHERE id = 'lc-w3'",
            "lc-w3",
            "update",
        ),
    ];
    let mut dirtied = Vec::new();
    for write in &failing {
        let frame = write.apply(&db).await;
        dirtied.push(dirties_notification_center(Some(&published), &frame));
    }
    assert_eq!(dirtied, [false, false, true], "only the failure itself");
    let failed = derive_lifecycle(&db).await;
    assert!(
        failed
            .entries
            .iter()
            .any(|entry| entry.key == "agent_failed:lc-w3"),
        "{:#?}",
        failed.entries
    );
}

/// Each write that can open, close or resolve an entry on a job the center
/// would otherwise skip is a frame it lets through, and the answer moves.
#[tokio::test]
async fn writes_that_move_an_entry_are_frames_the_center_lets_through() {
    let cases = [
        (
            "a planner resumed with its plan unconfirmed reopens its review",
            Write::Job("UPDATE jobs SET status = 'running' WHERE id = 'lc-planner'", "lc-planner"),
            true,
        ),
        (
            "a successor inside the settle window resolves the held failure",
            Write::Turn(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
                   VALUES ('lc-f2', 'lc-flaky-s', 'lc-flaky-r', 'lc-flaky', 2, 'pending', 'user_message', 9999, 9999)",
                "lc-f2",
                "insert",
            ),
            true,
        ),
        (
            "the asker's turn being cancelled closes its question",
            Write::Turn("UPDATE turns SET state = 'cancelled' WHERE id = 'lc-a1'", "lc-a1", "update"),
            true,
        ),
        (
            "the asker's job ending closes its question",
            Write::Job("UPDATE jobs SET status = 'complete' WHERE id = 'lc-asker'", "lc-asker"),
            true,
        ),
        (
            "a turn crashing at its provider",
            Write::Turn(
                "UPDATE turns SET state = 'interrupted', end_reason = 'provider_failure', ended_at = 9000 WHERE id = 'lc-w1'",
                "lc-w1",
                "update",
            ),
            true,
        ),
        (
            // The fixture has no recipe for its derivation to find, so this
            // one only checks the frame.
            "a job reaching blocked may be missing its output",
            Write::Job("UPDATE jobs SET status = 'blocked' WHERE id = 'lc-worker'", "lc-worker"),
            false,
        ),
    ];
    for (n, (why, write, moves)) in cases.iter().enumerate() {
        let db = lifecycle_fixture(&format!("notif-moves-{n}")).await;
        let published = derive_lifecycle(&db).await;
        let frame = write.apply(&db).await;
        assert!(
            dirties_notification_center(Some(&published), &frame),
            "{why}: {frame}"
        );
        if *moves {
            assert_ne!(derive_lifecycle(&db).await, published, "{why}");
        }
    }
}
