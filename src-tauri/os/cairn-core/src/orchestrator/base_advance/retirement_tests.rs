//! Retirement of the old runtime's queue against a real store: what it
//! records, what it never touches, and how the next exact demand recovers.

use super::*;
use crate::orchestrator::base_advance::preparation::tests::{
    git, harness_or_skip, Harness, BRANCH,
};
use crate::orchestrator::base_advance::preparation::PreparationPurpose;

fn item(status: &str, outcome: Option<&str>, observed: Option<&str>) -> LegacyItem {
    LegacyItem {
        bookmark: "agent/x".to_string(),
        status: status.to_string(),
        outcome_kind: outcome.map(str::to_string),
        observed_tip: observed.map(str::to_string),
        open_session: false,
        replay_requested: false,
    }
}

#[test]
fn settlement_follows_the_accepted_install_and_the_literal_head() {
    let installed = item("graph_moved", Some("rebased_clean"), Some("h1"));
    let pending = settle(&installed, Some("h1")).unwrap();
    assert_eq!(
        (
            pending.outcome,
            pending.observed_tip.as_str(),
            pending.status
        ),
        (OUTCOME_EXPORT_PENDING, "h1", Some("completed"))
    );
    let moved_on = settle(&installed, Some("h2")).unwrap();
    assert_eq!(
        (moved_on.outcome, moved_on.observed_tip.as_str()),
        (OUTCOME_EXPORT_SUPERSEDED, "h1"),
        "the later operation owns the export; the record keeps the install it describes"
    );
    assert!(moved_on.note.contains("literal head h2"));
    assert!(
        settle(&item("graph_moved", Some("silent"), Some("h1")), Some("h1")).is_some(),
        "a silent install moved the branch too"
    );

    let unfinished = item("pending", Some("deferred"), Some("h0"));
    assert_eq!(
        settle(&unfinished, Some("h0")),
        None,
        "nothing was installed"
    );
    let maybe_installed = settle(&unfinished, Some("h1")).unwrap();
    assert_eq!(
        (
            maybe_installed.outcome,
            maybe_installed.observed_tip.as_str(),
            maybe_installed.status
        ),
        (OUTCOME_EXPORT_PENDING, "h1", None)
    );
    assert!(
        maybe_installed.note.contains("observed h0"),
        "the old coordinate is kept"
    );

    for terminal in [
        item("graph_moved", Some("conflicted"), Some("h0")),
        item("completed", Some("rebased_clean"), Some("h0")),
        item("suppressed", Some("quarantined"), Some("h0")),
    ] {
        assert!(
            !terminal.inspected(),
            "{terminal:?} installed nothing outstanding"
        );
    }
    let mut open = installed.clone();
    open.open_session = true;
    assert!(!open.inspected(), "an open session is left verbatim");
    let mut served = installed.clone();
    served.replay_requested = true;
    assert!(served.inspected(), "the install itself served the request");
    let mut requested = unfinished.clone();
    requested.replay_requested = true;
    assert!(
        !requested.inspected(),
        "an unserved request is the replay path's"
    );
    assert_eq!(
        settle(&installed, None),
        None,
        "a vanished bookmark exports nothing"
    );
}

const DORMANT: &str = "agent/dormant";
const MOVED: &str = "agent/moved";
const STILL: &str = "agent/still";

const ROW: &str = "SELECT json_array(status, outcome_kind, observed_tip, failure_kind,
        last_diagnostic, notification_sent, attempt_count, updated_at, diagnostic_version,
        resolution_state, marker_state, replay_requested_at, replay_requested_tip,
        replay_requested_destination, resolution_basis_json)
   FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2";
const SESSION_COLUMNS: &str = "SELECT json_array(diagnostic_version, resolution_state,
        marker_state, base_commit, ours_commit, theirs_commit, replay_requested_at,
        replay_requested_tip, replay_requested_destination, notification_sent, failure_kind)
   FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2";

async fn columns(h: &Harness, sql: &str, intent: &str, bookmark: &str) -> String {
    h.db.query_text(sql.to_string(), (intent.to_string(), bookmark.to_string()))
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{intent}/{bookmark} exists"))
}

async fn intent(h: &Harness, id: &str) -> (String, String) {
    h.db.query_one(
        "SELECT status, COALESCE(last_diagnostic, '') FROM jj_reconcile_intents WHERE id = ?1",
        (id.to_string(),),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .unwrap()
}

async fn item_outcome(h: &Harness, intent: &str, bookmark: &str) -> (String, String, String) {
    h.db.query_one(
        "SELECT status, COALESCE(outcome_kind, ''), COALESCE(observed_tip, '')
           FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2",
        (intent.to_string(), bookmark.to_string()),
        |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
    )
    .await
    .unwrap()
}

async fn count(h: &Harness, sql: &str) -> String {
    h.db.query_text(sql.to_string(), ()).await.unwrap().unwrap()
}

fn sorted(ids: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut ids: Vec<String> = ids.into_iter().collect();
    ids.sort();
    ids
}

/// The previous runtime's queue, as an upgrade finds it: a pending intent
/// written in the same second the new process started, whose clean install of
/// the branch was accepted but never delivered (its row also carries a
/// resolved session and the request the install served); a running intent
/// stamped from a skewed clock whose lease the stopped worker still holds (an
/// unfinished deferral whose head moved, one whose head did not, a superseded
/// install, an open conflict session and an owner's unserved replay request);
/// a pending intent created after the capture; and history.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn retiring_the_old_queue_moves_no_branch_wakes_nobody_and_leaves_exports_to_exact_demand() {
    let h = harness_or_skip!("retire_legacy");
    let old_main = h.tip("main");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    // The legacy worker's own rewrite of the branch onto the new base,
    // accepted in the store; its delivery never happened. The rewrite is
    // synchronous jj work that drives its own runtime, so it runs on a
    // blocking worker as production runs it.
    let (jj, store, onto) = (h.jj.clone(), h.store.clone(), base.clone());
    let report = tokio::task::spawn_blocking(move || {
        crate::jj::reconcile_siblings_without_publication(&jj, &store, &onto, &[BRANCH.to_string()])
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.rebased_clean, vec![BRANCH.to_string()]);
    let installed = h.tip(BRANCH);
    for name in [DORMANT, MOVED, STILL] {
        h.set_bookmark(name, &old_main);
    }

    let store = h.store.to_string_lossy().into_owned();
    let now = chrono::Utc::now().timestamp();
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-dormant', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', '{DORMANT}', 'main', 1, 1);
         INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit, status, created_at, updated_at)
         VALUES ('legacy-pending', 'proj-1', '{store}', 'main', '{base}', 'pending', {now}, {now}),
                ('legacy-leased', 'proj-1', '{store}', 'main', 'old-destination', 'running', 2, {skewed}),
                ('history', 'proj-1', '{store}', 'main', 'history-destination', 'completed', 3, 3);
         UPDATE jj_reconcile_intents SET lease_owner = 'stopped-worker', lease_expires_at = {lease}
           WHERE id = 'legacy-leased';
         INSERT INTO jj_reconcile_items
           (intent_id, bookmark, observed_tip, status, outcome_kind, diagnostic_version,
            resolution_state, replay_requested_at, replay_requested_tip, updated_at)
         VALUES ('legacy-pending', '{BRANCH}', '{installed}', 'graph_moved', 'rebased_clean', 1,
                 'resolved', 7, 'resolved-tip', 1);
         INSERT INTO jj_reconcile_items (intent_id, bookmark, observed_tip, status, outcome_kind, updated_at)
         VALUES ('legacy-leased', '{DORMANT}', 'an-older-observation', 'pending', 'deferred', 2),
                ('legacy-leased', '{STILL}', '{old_main}', 'pending', 'deferred', 2),
                ('legacy-leased', '{MOVED}', 'a-superseded-install', 'graph_moved', 'rebased_clean', 2),
                ('history', '{DORMANT}', 'history-tip', 'pending', 'deferred', 3);
         INSERT INTO jj_reconcile_items
           (intent_id, bookmark, observed_tip, status, outcome_kind, diagnostic_version, resolution_state, marker_state, updated_at)
         VALUES ('legacy-leased', 'agent/session', 'conflicted-tip', 'graph_moved', 'conflicted', 1, 'open', 'pending', 2);
         INSERT INTO jj_reconcile_items
           (intent_id, bookmark, observed_tip, status, outcome_kind, replay_requested_at, replay_requested_tip, replay_requested_destination, updated_at)
         VALUES ('legacy-leased', 'agent/requested', 'requested-tip', 'pending', 'deferred', 5, 'requested-tip', 'old-destination', 2);",
        skewed = now + 86_400,
        lease = now + 600,
    ))
    .await
    .unwrap();
    let captured = capture_legacy_intents(&h.db).await.unwrap();
    assert_eq!(
        sorted(captured.iter().cloned()),
        vec!["legacy-leased", "legacy-pending"]
    );
    // Written after the capture: not the old runtime's.
    h.db.execute(
        "INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit, status, created_at, updated_at)
         VALUES ('after-capture', 'proj-1', ?1, 'main', 'after-capture-destination', 'pending', 1, 1)",
        (store.clone(),),
    )
    .await
    .unwrap();

    let verbatim = [
        ("legacy-leased", "agent/session"),
        ("legacy-leased", "agent/requested"),
        ("legacy-leased", STILL),
        ("history", DORMANT),
    ];
    let mut before = Vec::new();
    for (intent_id, bookmark) in verbatim {
        before.push(columns(&h, ROW, intent_id, bookmark).await);
    }
    let branch_session = columns(&h, SESSION_COLUMNS, "legacy-pending", BRANCH).await;
    let rows_before = count(&h, "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_items").await;
    let tips = |h: &Harness| [BRANCH, DORMANT, MOVED, STILL, "main"].map(|name| h.tip(name));
    let tips_before = tips(&h);

    let (retired, settled) = retire_legacy_reconcile_intents(&h.orch, &h.db, &captured).await;
    assert_eq!(
        retired,
        RetirementReport {
            retired: 2,
            export_recovery: 2,
            deferred: 0
        }
    );
    assert_eq!(sorted(settled), vec!["legacy-leased", "legacy-pending"]);
    for id in ["legacy-pending", "legacy-leased"] {
        let (status, diagnostic) = intent(&h, id).await;
        assert_eq!(status, "superseded", "{id}");
        assert!(diagnostic.starts_with(RETIRED), "{id}: {diagnostic}");
    }
    assert_eq!(intent(&h, "after-capture").await.0, "pending");
    assert_eq!(intent(&h, "history").await.0, "completed");
    assert_eq!(
        h.db.query_text(
            "SELECT COALESCE(lease_owner, '') FROM jj_reconcile_intents WHERE id = 'legacy-leased'",
            ()
        )
        .await
        .unwrap()
        .as_deref(),
        Some(""),
        "the stopped worker's lease is released, not honoured"
    );

    assert_eq!(
        item_outcome(&h, "legacy-pending", BRANCH).await,
        (
            "completed".into(),
            OUTCOME_EXPORT_PENDING.into(),
            installed.clone()
        ),
        "the accepted install is kept and its export left to the branch's next demand"
    );
    assert_eq!(
        columns(&h, SESSION_COLUMNS, "legacy-pending", BRANCH).await,
        branch_session,
        "its resolved session and served request are history, kept as they were"
    );
    assert_eq!(
        item_outcome(&h, "legacy-leased", DORMANT).await,
        (
            "pending".into(),
            OUTCOME_EXPORT_PENDING.into(),
            old_main.clone()
        ),
        "the literal head is recorded"
    );
    let dormant_diagnostic = h
        .db
        .query_text(
            "SELECT last_diagnostic FROM jj_reconcile_items WHERE intent_id = 'legacy-leased' AND bookmark = ?1",
            (DORMANT.to_string(),),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        dormant_diagnostic.contains("observed an-older-observation"),
        "the old observed coordinate is kept: {dormant_diagnostic}"
    );
    assert_eq!(
        item_outcome(&h, "legacy-leased", MOVED).await,
        (
            "completed".into(),
            OUTCOME_EXPORT_SUPERSEDED.into(),
            "a-superseded-install".into()
        ),
    );
    for ((intent_id, bookmark), was) in verbatim.into_iter().zip(before) {
        assert_eq!(
            columns(&h, ROW, intent_id, bookmark).await,
            was,
            "{intent_id}/{bookmark} is left verbatim"
        );
    }

    assert_eq!(tips(&h), tips_before, "retirement moves no bookmark");
    assert_eq!(
        count(&h, "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_items").await,
        rows_before,
        "no intent is expanded into branch work"
    );
    assert_eq!(
        count(
            &h,
            "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_intents"
        )
        .await,
        "4"
    );
    assert!(h.messages().await.is_empty(), "no note for retired work");
    assert!(h.wakes().await.is_empty(), "nobody is woken");

    assert_eq!(
        retire_legacy_reconcile_intents(&h.orch, &h.db, &captured)
            .await
            .0,
        RetirementReport::default(),
        "the next upkeep pass finds nothing left to retire"
    );

    // The branch's next exact demand recovers its export, and only its own.
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(
        !prepared.advanced,
        "the accepted install already carries the base"
    );
    assert_eq!(prepared.head_after, installed, "nothing is replayed");
    assert_eq!(
        item_outcome(&h, "legacy-pending", BRANCH).await.1,
        "exported",
        "the pending export was recovered"
    );
    assert_eq!(git(&h.repo, &["rev-parse", BRANCH]), installed);
    assert_eq!(
        item_outcome(&h, "legacy-leased", DORMANT).await.1,
        OUTCOME_EXPORT_PENDING,
        "a sibling's recovery waits for its own demand"
    );
    assert_eq!(h.tip(DORMANT), old_main);
    assert_eq!(h.tip(MOVED), old_main);
    assert!(h.wakes().await.is_empty(), "recovery wakes nobody");
}

/// The upkeep loop as a process runs it: the first pass captures and retires
/// what the stopped runtime left pending or leased (including a partially
/// published install), later passes in the same process never touch a row the
/// process itself wrote, and a restart's first pass captures what is left. No
/// pass restores a legacy worker: nothing moves and nobody is woken.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn the_upkeep_loop_retires_what_it_finds_at_start_and_restores_no_worker() {
    let h = harness_or_skip!("retire_upkeep");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let store = h.store.to_string_lossy().into_owned();
    let intent = |id: &str, status: &str| {
        format!(
            "INSERT INTO jj_reconcile_intents
               (id, project_id, store_path, target_branch, destination_commit, status, lease_owner, lease_expires_at, created_at, updated_at)
             VALUES ('{id}', 'proj-1', '{store}', 'main', '{id}-destination', '{status}', 'stopped-worker', 4102444800, 1, 1);"
        )
    };
    h.db.execute_script(format!(
        "{}\n{}\n
         INSERT INTO jj_reconcile_items (intent_id, bookmark, observed_tip, status, outcome_kind, updated_at)
         VALUES ('leased', '{BRANCH}', '{own}', 'graph_moved', 'rebased_clean', 1);",
        intent("pending", "pending"),
        intent("leased", "running"),
    ))
    .await
    .unwrap();

    let queue = LegacyQueue::default();
    upkeep_pass(&h.orch, &queue).await;
    for id in ["pending", "leased"] {
        assert_eq!(intent_status(&h, id).await, "superseded", "{id}");
    }
    assert_eq!(
        item_outcome(&h, "leased", BRANCH).await,
        (
            "completed".into(),
            OUTCOME_EXPORT_PENDING.into(),
            own.clone()
        ),
        "the partially published install is left to its branch's next demand"
    );

    // A row written by this process after the capture is not legacy.
    h.db.execute_script(intent("later", "pending"))
        .await
        .unwrap();
    upkeep_pass(&h.orch, &queue).await;
    assert_eq!(intent_status(&h, "later").await, "pending");

    // After a restart, whatever the previous process left is captured.
    upkeep_pass(&h.orch, &LegacyQueue::default()).await;
    assert_eq!(intent_status(&h, "later").await, "superseded");

    assert_eq!(h.tip(BRANCH), own, "no pass moved a branch");
    assert!(h.messages().await.is_empty());
    assert!(h.wakes().await.is_empty());
}

async fn intent_status(h: &Harness, id: &str) -> String {
    intent(h, id).await.0
}

/// A store with concurrent operation heads cannot be read literally. The
/// retirement defers rather than settling them, and retires once something
/// else has.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn split_operation_heads_defer_the_retirement_without_being_settled() {
    let h = harness_or_skip!("retire_split_ops");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let parent = git(&h.repo, &["rev-parse", &format!("{own}^")]);
    h.db.execute(
        "INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit, status, created_at, updated_at)
         VALUES ('legacy', 'proj-1', ?1, 'main', 'old-destination', 'pending', 1, 1)",
        (h.store.to_string_lossy().into_owned(),),
    )
    .await
    .unwrap();
    h.db.execute(
        "INSERT INTO jj_reconcile_items (intent_id, bookmark, observed_tip, status, outcome_kind, updated_at)
         VALUES ('legacy', ?1, 'an-older-observation', 'pending', 'deferred', 1)",
        (BRANCH.to_string(),),
    )
    .await
    .unwrap();
    let captured = capture_legacy_intents(&h.db).await.unwrap();

    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    for (name, target) in [
        ("agent/left", own.as_str()),
        ("agent/right", parent.as_str()),
    ] {
        h.jj.run(
            &h.store,
            &[
                "--at-op",
                &op,
                "bookmark",
                "set",
                name,
                "-r",
                target,
                "--ignore-working-copy",
            ],
            "concurrent operation",
        )
        .unwrap();
    }
    // `resolve_coordinate` reports a split as a repository error naming the
    // head count, as cairn-vcs's own literal-read test asserts.
    let split = |read: &Result<String, cairn_vcs::CoordinateResolutionError>| matches!(read, Err(error) if error.to_string().contains("2 operation heads"));
    let read = cairn_vcs::resolve_coordinate(&h.store, BRANCH).await;
    assert!(
        split(&read),
        "fixture must hold split operation heads: {read:?}"
    );

    let (report, settled) = retire_legacy_reconcile_intents(&h.orch, &h.db, &captured).await;
    assert_eq!(
        report,
        RetirementReport {
            retired: 0,
            export_recovery: 0,
            deferred: 1
        }
    );
    assert!(settled.is_empty());
    assert_eq!(intent(&h, "legacy").await.0, "pending");
    assert_eq!(
        item_outcome(&h, "legacy", BRANCH).await.1,
        "deferred",
        "nothing is recorded from an unreadable store"
    );
    let read = cairn_vcs::resolve_coordinate(&h.store, BRANCH).await;
    assert!(
        split(&read),
        "the retirement left the operation heads as it found them"
    );

    // Any ordinary jj load settles the heads; the next pass then retires.
    h.jj.run(
        &h.store,
        &["log", "-r", "root()", "--ignore-working-copy", "--no-graph"],
        "settle the concurrent operations",
    )
    .unwrap();
    let (report, _) = retire_legacy_reconcile_intents(&h.orch, &h.db, &captured).await;
    assert_eq!(
        report,
        RetirementReport {
            retired: 1,
            export_recovery: 1,
            deferred: 0
        }
    );
    assert_eq!(
        item_outcome(&h, "legacy", BRANCH).await,
        ("pending".into(), OUTCOME_EXPORT_PENDING.into(), own),
    );
    assert!(h.wakes().await.is_empty());
}

/// Main's eager design recorded each descendant of a conflicted parent as a
/// pending `held_descendant` intent with an owner session. Retirement
/// supersedes it like any other, but names every branch it held, the parent
/// and the session, and leaves the items themselves verbatim.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_retired_held_descendant_intent_names_each_branch_its_parent_and_session() {
    let h = harness_or_skip!("retire_held_descendant");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let store = h.store.to_string_lossy().into_owned();
    h.db.execute_script(format!(
        "INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit, trigger_sources_json, status, created_at, updated_at)
         VALUES ('held', 'proj-1', '{store}', 'agent/parent', 'parent-destination', '[\"held_descendant\"]', 'pending', 1, 1);
         INSERT INTO jj_reconcile_items (intent_id, bookmark, observed_tip, status, outcome_kind, updated_at)
         VALUES ('held', '{BRANCH}', '{own}', 'completed', 'held_parent', 1);
         INSERT INTO jj_reconcile_items
           (intent_id, bookmark, observed_tip, status, outcome_kind, diagnostic_version, resolution_state, updated_at)
         VALUES ('held', 'agent/held-session', 'held-tip', 'completed', 'conflicted', 1, 'open', 1);"
    ))
    .await
    .unwrap();
    let captured = capture_legacy_intents(&h.db).await.unwrap();
    let rows = [BRANCH, "agent/held-session"];
    let mut before = Vec::new();
    for bookmark in rows {
        before.push(columns(&h, ROW, "held", bookmark).await);
    }

    let (report, settled) = retire_legacy_reconcile_intents(&h.orch, &h.db, &captured).await;
    assert_eq!(
        report,
        RetirementReport {
            retired: 1,
            export_recovery: 0,
            deferred: 0
        }
    );
    assert_eq!(settled, vec!["held".to_string()]);
    let (status, diagnostic) = intent(&h, "held").await;
    assert_eq!(status, "superseded");
    for named in [
        format!(
            "held descendant `{BRANCH}` of parent `agent/parent` at parent-destination \
             (session held, no open session)"
        ),
        "held descendant `agent/held-session` of parent `agent/parent` at parent-destination \
         (session held, open)"
            .to_string(),
    ] {
        assert!(diagnostic.contains(&named), "{named} in {diagnostic}");
    }
    for (bookmark, was) in rows.into_iter().zip(before) {
        assert_eq!(
            columns(&h, ROW, "held", bookmark).await,
            was,
            "{bookmark} is left verbatim"
        );
    }
    assert_eq!(h.tip(BRANCH), own, "retirement moves no branch");
    assert!(h.wakes().await.is_empty(), "nobody is woken");
}
