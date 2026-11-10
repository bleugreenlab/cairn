//! Real-store explicit replay tests, on the preparation harness: a jj store, a
//! migrated database, and an orchestrator pointed at both.

use super::*;
use crate::orchestrator::base_advance::preparation::tests::{
    git, harness_or_skip, Harness, BRANCH, JOB,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

type Hook = Box<dyn FnMut(u32) + Send>;

fn hooks() -> &'static Mutex<HashMap<PathBuf, Hook>> {
    static HOOKS: OnceLock<Mutex<HashMap<PathBuf, Hook>>> = OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

/// Called after the carry is planned, before the preparer captures.
pub(super) fn after_carry_plan(store: &Path, attempt: u32) {
    let mut hook = hooks().lock().unwrap().remove(store);
    if let Some(hook) = hook.as_mut() {
        hook(attempt);
    }
    if let Some(hook) = hook {
        hooks().lock().unwrap().insert(store.to_path_buf(), hook);
    }
}

async fn unserved(h: &Harness) -> Vec<String> {
    conflict_session::list_pending_replay_requests(&h.db)
        .await
        .unwrap()
        .into_iter()
        .map(|listed| listed.bookmark)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_expected_head_refuses_a_capture_at_any_other_head_under_the_guard() {
    let h = harness_or_skip!("explicit_expected_head");
    let accepted = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    h.advance_main("incoming.txt", "theirs\n");
    let moved = h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.explicit_replay = true;
    request.expected_head = Some(accepted.clone());
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Moved { what } if what.contains(&accepted) && what.contains(&moved)),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), moved, "the newer commit is never replayed");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn concurrent_servers_attempt_one_request_once() {
    let h = harness_or_skip!("explicit_marker_single_flight");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let base = h.advance_main("shared.txt", "theirs\n");
    conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &base,
        BRANCH,
        Some(&own),
        false,
        None,
    )
    .await
    .unwrap();

    tokio::join!(
        serve_pending_replay_requests(&h.orch, None),
        serve_pending_replay_requests(&h.orch, Some((&h.store, BRANCH))),
    );

    // The conflict is recorded by exactly one attempt.
    let attempts =
        h.db.query_text(
            "SELECT CAST(MAX(attempt_count) AS TEXT) FROM jj_reconcile_items
              WHERE bookmark = ?1 AND outcome_kind = 'conflicted'",
            (BRANCH,),
        )
        .await
        .unwrap();
    assert_eq!(attempts.as_deref(), Some("1"), "one attempt, not two");
    assert_eq!(
        h.wakes()
            .await
            .iter()
            .filter(|wake| *wake == "wake")
            .count(),
        1
    );
    assert!(unserved(&h).await.is_empty(), "the conflict is its verdict");
    assert_eq!(h.tip(BRANCH), own);
}

/// A duplicate claim is refused at once, never touches the held one, and the
/// holder's release admits the next. Bounded on its own thread: the defect
/// this pins was a self-deadlock.
#[test]
fn a_held_serving_claim_refuses_a_second_without_releasing_the_first() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let first = ServingClaim::take("/claim-store", "agent/claim").unwrap();
        let refused = ServingClaim::take("/claim-store", "agent/claim").is_none();
        let kept = ServingClaim::take("/claim-store", "agent/claim").is_none();
        drop(first);
        let next = ServingClaim::take("/claim-store", "agent/claim").is_some();
        tx.send((refused, kept, next)).unwrap();
    });
    let (refused, kept, next) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("claiming never deadlocks");
    assert!(refused, "a duplicate claim is refused");
    assert!(kept, "and refusing it does not release the held claim");
    assert!(next, "the holder's release admits the next server");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_explicit_resolved_replay_lands_past_the_bound_session_shortcut_on_its_branch_only() {
    let h = harness_or_skip!("explicit_lands");
    let sibling_ws = h.workspaces.path().join("dormant");
    crate::jj::add_workspace(&h.jj, &h.store, &sibling_ws, SIBLING, "main", None).unwrap();
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-dormant', 'exec-1', 'dormant', 'issue-1', 'proj-1', 'running', '{SIBLING}', NULL, 2, 2);"
    ))
    .await
    .unwrap();
    let sibling = h.commit(&sibling_ws, SIBLING, "dormant.txt", "dormant\n");
    let (d1, _) = resolved_conflict(&h).await;

    // An ordinary commit resolving the recorded conflict binds to the session
    // and is not carried onto the base.
    let bound = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!bound.advanced && bound.session.is_some());

    // The owner's explicit request is never answered by that shortcut.
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap();
    assert!(answer.starts_with("Replayed"), "{answer}");
    assert!(answer.contains("not a green build"), "{answer}");
    let tip = h.tip(BRANCH);
    assert!(h.is_ancestor(&d1, &tip), "the replay carries D1");
    assert_eq!(show(&h, &tip, "shared.txt"), "original\nA\nX");
    assert_eq!(
        git(&h.repo, &["rev-parse", BRANCH]),
        tip,
        "the Git ref is exported"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());

    assert_eq!(h.tip(SIBLING), sibling, "a dormant sibling is untouched");
    assert!(!h.is_ancestor(&d1, &sibling));
    assert_eq!(sibling_items(&h).await, 0, "nor ever enumerated");
    assert_eq!(
        h.wakes()
            .await
            .iter()
            // A Steer delivery is stored at the `wake` level; the clean
            // replay's note is `passive`.
            .filter(|wake| *wake == "wake")
            .count(),
        1,
        "only the recorded conflict woke the owner"
    );
}

const SIBLING: &str = "agent/dormant";

use crate::orchestrator::conflict_session::load_active_session;

/// The branch appends X where D1 appends A; the owner's commit records the
/// session, and the owner commits the resolution A-then-X on the old fork.
async fn resolved_conflict(h: &Harness) -> (String, String) {
    h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nX\n");
    let d1 = h.advance_main("shared.txt", "original\nA\n");
    match h.prepare(PreparationPurpose::OwnerCommit).await {
        Err(PreparationRefusal::Conflict(_)) => {}
        // Merge mode: the commit publishes under the session it recorded,
        // with the catch-up merge left pending.
        Ok(prepared) if prepared.session.is_some() && !prepared.advanced => {}
        Ok(_) => panic!("the owner's commit records the conflict, not an advance"),
        Err(refusal) => panic!("the owner's commit records the conflict: {refusal}"),
    }
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_some());
    let resolved = h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nA\nX\n");
    (d1, resolved)
}

fn show(h: &Harness, revision: &str, path: &str) -> String {
    git(&h.repo, &["show", &format!("{revision}:{path}")])
}

async fn sibling_items(h: &Harness) -> i64 {
    h.db.query_text(
        "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_items WHERE bookmark = ?1",
        (SIBLING,),
    )
    .await
    .unwrap()
    .unwrap()
    .parse()
    .unwrap()
}
