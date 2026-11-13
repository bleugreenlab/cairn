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
