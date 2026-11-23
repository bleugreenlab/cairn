//! Real-store explicit replay tests, on the preparation harness: a jj store, a
//! migrated database, and an orchestrator pointed at both.

use super::*;
use crate::orchestrator::base_advance::load_reconcile_quarantine;
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

fn deferred_increments() -> &'static Mutex<std::collections::HashSet<PathBuf>> {
    static DEFERRED: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();
    DEFERRED.get_or_init(Default::default)
}

/// Make every following round in `store` find its parent's budget spent.
pub(super) fn defer_increments(store: &Path) {
    deferred_increments()
        .lock()
        .unwrap()
        .insert(store.to_path_buf());
}

pub(super) fn increment_deferred(store: &Path) -> bool {
    deferred_increments().lock().unwrap().contains(store)
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
    assert_reply_receipt(&h, &answer, "rebased_clean").await;
    assert!(answer.contains("Replayed"), "{answer}");
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

use crate::orchestrator::base_advance::preparation::tests::{
    expire_install, on_compute, starve_compute,
};
use crate::orchestrator::base_advance::preparation::ConflictRecord;
use crate::orchestrator::base_advance::replay_cost::{
    self as cost, branch_blockers, branch_samples, BudgetClass, CostOutcome,
};

async fn newest_sample(h: &Harness) -> cost::ReplayCostEvidence {
    branch_samples(&h.db, "proj-1", &h.store, BRANCH, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("a sample was recorded")
        .unwrap()
}

async fn sample_count(h: &Harness) -> usize {
    branch_samples(&h.db, "proj-1", &h.store, BRANCH, 100)
        .await
        .unwrap()
        .len()
}

/// cairn/6456: an explicit replay whose detached computation exhausted its
/// budget leaves a typed sample and a blocker; the identical request is then
/// refused before computing (no new sample), names what would change it and
/// the operator-approved exit, and survives a newer request. A new commit is a
/// different attempt, and its install clears the record.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_explicit_compute_timeout_refuses_an_identical_repeat_before_computing() {
    let h = harness_or_skip!("explicit_identical_timeout");
    use crate::orchestrator::base_advance::preparation::tests::{
        ReplayAdmissionObservation, ReplayAllowances,
    };
    crate::orchestrator::base_advance::preparation::tests::trace_replay(&h.store);
    // Equal wall-clock deadlines can leave materially different compute allowances.
    // This fixture exercises an identical allowance, not elapsed host time.
    let allowances =
        ReplayAllowances::new(&h.store, Duration::from_secs(58), Duration::from_secs(58));
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);

    let first = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert_reply_receipt(&h, &first, "timed out").await;
    assert!(first.contains("exceeded"), "{first}");
    assert!(
        first.contains("already exhausted its compute budget")
            && first.contains("refused before computing anything")
            && first.contains("merge catch-up mode")
            && !first.contains("requested again at once"),
        "the timeout answer says a repeat is blocked, not that it may run: {first}"
    );
    let sample = newest_sample(&h).await;
    assert_eq!(sample.outcome, CostOutcome::ComputeExhausted);
    assert_eq!(sample.budget_class, BudgetClass::Explicit);
    assert_eq!(
        (sample.head.as_str(), sample.base.as_str()),
        (head.as_str(), base.as_str())
    );
    assert_eq!(sample.engine_version, crate::jj::REPLAY_ENGINE_VERSION);
    assert!(sample.compute_allowance_ms.unwrap() >= 30_000, "{sample:?}");
    assert!(sample.phases.proof_ms.is_some() && sample.phases.capture_ms.is_some());
    assert!(
        sample
            .failure_phase
            .as_deref()
            .unwrap()
            .contains("detached computation"),
        "{sample:?}"
    );
    let blockers = branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap();
    assert_eq!(blockers.len(), 1);
    assert_eq!(blockers[0].key.head, head);
    assert_eq!(blockers[0].key.base, base);
    assert_eq!(blockers[0].key.mode, cost::ReplayMode::Replay);
    let samples = sample_count(&h).await;

    // The composed page agrees with admission: its next action is no longer
    // the replay it would refuse.
    h.db.execute_script(format!(
        "UPDATE jobs SET uri_segment = 'node', base_branch = 'main' WHERE id = '{JOB}';"
    ))
    .await
    .unwrap();
    let page = crate::resources::read_node_rebase(&h.orch, "proj", 1, 1, "node", &[]).await;
    assert!(
        page.contains("**An identical explicit replay is refused.**"),
        "{page}"
    );
    let next = page
        .split("## Next action")
        .nth(1)
        .unwrap_or_else(|| panic!("the page has a next action: {page}"));
    assert!(
        next.trim_start()
            .starts_with("Do not request the same replay again")
            && !next.contains("action:\"replay\""),
        "{page}"
    );

    // The identical request: a newer request record, same coordinates.
    let second = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None).await;
    let trace = crate::orchestrator::base_advance::preparation::tests::replay_trace(&h.store);
    eprintln!("replay admission: {trace:#?}");
    let second = second.unwrap_err();
    assert_eq!(
        trace
            .iter()
            .filter(|event| matches!(event, ReplayAdmissionObservation::Compute { .. }))
            .count(),
        1,
        "the refused request must not enter detached computation: {trace:#?}"
    );
    assert_reply_receipt(&h, &second, REPEAT_REFUSED).await;
    for expected in [
        "was not attempted",
        "refused before computing anything",
        "a new commit on",
        "advancing",
        "nobody has to clear this record",
        "catch-up-mode",
    ] {
        assert!(second.contains(expected), "{expected}: {second}");
    }
    assert_eq!(sample_count(&h).await, samples, "nothing was computed");
    assert_eq!(h.tip(BRANCH), head);
    assert!(
        unserved(&h).await.is_empty(),
        "the refused request is closed"
    );
    assert_eq!(
        branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
            .await
            .unwrap()
            .len(),
        1,
        "the blocker survives a newer request"
    );

    // A new commit is a different attempt; it installs and clears the record.
    drop(allowances);
    starve_compute(&h.store, false);
    h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    let third = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert_reply_receipt(&h, &third, "rebased_clean").await;
    assert!(third.contains("Replayed"), "{third}");
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
    assert!(branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());
    let landed = newest_sample(&h).await;
    assert_eq!(landed.outcome, CostOutcome::Installed);
    assert!(landed.phases.install_ms.is_some() && landed.phases.export_ms.is_some());
    assert_eq!(landed.observed.phase, Some(crate::jj::ComputePhase::Done));
}

/// The execution thread's binding: a timeout under the 10 s background budget
/// never blocks an explicit attempt at the same coordinates.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_background_timeout_never_blocks_an_explicit_replay() {
    let h = harness_or_skip!("background_timeout_explicit");
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    let before = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let budget =
        cairn_vcs::jj::testing::ReplayBudget::expire_at(&h.store, "advance: before classification");
    let refusal = h.prepare(PreparationPurpose::Run).await.unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::TimedOut { .. }),
        "{refusal}"
    );
    assert!(
        budget.reached(),
        "expiry must occur after the hidden replay"
    );
    assert_eq!(h.tip(BRANCH), head);
    assert_eq!(git(&h.repo, &["rev-parse", BRANCH]), head);
    assert_eq!(crate::jj::operation_id(&h.jj, &h.store).unwrap(), before);
    assert!(
        conflict_session::load_latest_session(&h.db, BRANCH)
            .await
            .unwrap()
            .is_none(),
        "a timeout must not record a conflict session"
    );
    assert!(
        h.messages().await.is_empty(),
        "a timeout must not send the BLOCKING conflict notice"
    );
    let quarantine = load_reconcile_quarantine(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .expect("a background timeout records its cooldown");
    assert_eq!(quarantine.failure_kind, crate::jj::RECONCILE_TIMEOUT);
    let sample = newest_sample(&h).await;
    assert_eq!(sample.budget_class, BudgetClass::Background);
    assert_eq!(sample.outcome, CostOutcome::ComputeExhausted);
    assert_eq!(
        sample.observed.phase,
        Some(crate::jj::ComputePhase::Validate)
    );
    assert!(
        branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
            .await
            .unwrap()
            .is_empty(),
        "a background exhaustion records no blocker"
    );
    drop(budget);
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert_reply_receipt(&h, &answer, "rebased_clean").await;
    assert!(answer.contains("Replayed"), "{answer}");
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
}

async fn quarantine(h: &Harness) -> Option<String> {
    crate::orchestrator::base_advance::load_reconcile_quarantine(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .map(|quarantine| format!("{}={}", quarantine.failure_kind, quarantine.fingerprint))
}

fn timed_out_at(refusal: &PreparationRefusal, at: &str) -> bool {
    matches!(refusal, PreparationRefusal::TimedOut { head, .. } if head == at)
}

/// cairn/7237: an automatic timeout's cooldown holds automatic preparation of
/// the unchanged head (cairn/6070), but never the owner's commit. That commit
/// publishes on the branch's own head with the advance pending and named, and
/// the new head it makes releases the cooldown: automatic preparation retries.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_commit_passes_an_automatic_timeout_cooldown_with_the_advance_pending() {
    let h = harness_or_skip!("owner_commit_passes_cooldown");
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);

    let first = h.prepare(PreparationPurpose::Run).await.unwrap_err();
    assert!(timed_out_at(&first, &head), "{first}");
    assert_eq!(
        quarantine(&h).await,
        Some(format!("{}={head}:{base}", crate::jj::RECONCILE_TIMEOUT))
    );
    let second = h.prepare(PreparationPurpose::Run).await.unwrap_err();
    assert!(
        matches!(second, PreparationRefusal::Cooldown { .. }),
        "the unchanged head cools down: {second}"
    );
    assert!(second.to_string().contains("cairn:~/rebase"), "{second}");

    let owner = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!owner.advanced && owner.session.is_none());
    assert_eq!(
        (owner.head_before.as_str(), owner.head_after.as_str()),
        (head.as_str(), head.as_str())
    );
    assert_eq!(
        owner.base_commit, base,
        "revalidation still guards this base"
    );
    let pending = owner
        .advance_pending
        .as_deref()
        .expect("the advance is owed");
    for expected in [
        "base advance",
        "`main`",
        base.as_str(),
        "cooling down",
        "cairn:~/rebase",
    ] {
        assert!(pending.contains(expected), "{expected}: {pending}");
    }
    assert_eq!(h.tip(BRANCH), head, "preparing the commit moved nothing");

    // The commit lands on the branch's own head, and that new head is no
    // longer the one the cooldown holds.
    let next = h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    assert!(h.is_ancestor(&head, &next) && !h.is_ancestor(&base, &next));
    let retried = h.prepare(PreparationPurpose::Run).await.unwrap_err();
    assert!(
        timed_out_at(&retried, &next),
        "retried, not held: {retried}"
    );
}

/// The owner's commit that meets a fresh timeout, not only the cooldown after
/// one, publishes with the advance pending.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_commit_meeting_a_fresh_timeout_publishes_with_the_advance_pending() {
    let h = harness_or_skip!("owner_commit_fresh_timeout");
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);
    let owner = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    starve_compute(&h.store, false);
    assert!(!owner.advanced && owner.head_after == head);
    let pending = owner.advance_pending.as_deref().unwrap();
    assert!(
        pending.contains("exceeded") && pending.contains(&base),
        "{pending}"
    );
    assert_eq!(
        quarantine(&h).await,
        Some(format!("{}={head}:{base}", crate::jj::RECONCILE_TIMEOUT)),
        "automatic preparation of this head still cools down"
    );
    assert_eq!(
        newest_sample(&h).await.outcome,
        CostOutcome::ComputeExhausted
    );
}

/// An owner's commit whose computed advance expires before it installs
/// publishes pending, and its cost sample records a timeout outside
/// computation, not a conflict: nothing conflicted.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_commit_whose_install_expires_records_a_timeout_not_a_conflict() {
    let h = harness_or_skip!("owner_commit_install_expiry");
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    expire_install(&h.store, true);
    let owner = h.prepare(PreparationPurpose::OwnerCommit).await;
    expire_install(&h.store, false);
    let owner = owner.unwrap();
    assert!(!owner.advanced && owner.head_after == head);
    assert!(
        owner.advance_pending.as_deref().unwrap().contains(&base),
        "{:?}",
        owner.advance_pending
    );
    assert_eq!(
        h.tip(BRANCH),
        head,
        "the expired candidate installed nothing"
    );
    assert_eq!(newest_sample(&h).await.outcome, CostOutcome::TimedOut);
}

/// The specimen: an explicit replay records a session at H while an automatic
/// timeout row stands at H (an ordering race). The owner's resolution write
/// prepares at H under that session, and the cooldown never refuses it.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_commit_under_its_open_session_is_never_held_by_a_timeout_cooldown() {
    let h = harness_or_skip!("owner_commit_session_cooldown");
    let head = h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nX\n");
    let base = h.advance_main("shared.txt", "original\nA\n");
    let _ = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None).await;
    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("the explicit replay records the conflict");
    crate::orchestrator::base_advance::record_reconcile_timeout(
        &h.db,
        "proj-1",
        &h.store,
        BRANCH,
        &head,
        &base,
        "reconcile timeout: detached preparation exceeded its 10s budget",
    )
    .await
    .unwrap();

    let owner = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!owner.advanced && owner.head_after == head);
    assert_eq!(
        owner.session.map(|bound| bound.intent_id),
        Some(session.intent_id),
        "published under the open session"
    );
    assert!(owner.advance_pending.is_none(), "nothing was deferred");
}

/// An explicit replay that records a session retires the automatic timeout
/// cooldown at that head, and the session takes over what the cooldown did:
/// automatic preparation of the head is answered from it, without computing.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_explicit_replay_session_replaces_the_timeout_cooldown_at_its_head() {
    let h = harness_or_skip!("explicit_session_replaces_cooldown");
    let head = h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nX\n");
    h.advance_main("shared.txt", "original\nA\n");
    starve_compute(&h.store, true);
    let timed_out = h.prepare(PreparationPurpose::Run).await.unwrap_err();
    assert!(timed_out_at(&timed_out, &head), "{timed_out}");
    assert!(quarantine(&h).await.is_some());

    starve_compute(&h.store, false);
    let _ = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None).await;
    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("the explicit replay records the conflict");
    assert_eq!(quarantine(&h).await, None, "the cooldown is retired");

    // Starved again: anything that computed would time out.
    starve_compute(&h.store, true);
    for purpose in [PreparationPurpose::Run, PreparationPurpose::Check] {
        let answered = h.prepare(purpose).await.unwrap_err();
        let PreparationRefusal::Conflict(conflict) = &answered else {
            panic!("the session answers, not a cooldown or a timeout: {answered}");
        };
        assert_eq!(
            conflict.record,
            ConflictRecord::Session {
                intent_id: session.intent_id.clone(),
                reopened: false
            }
        );
        assert_eq!(conflict.paths, vec!["shared.txt".to_string()]);
    }
    starve_compute(&h.store, false);
    assert_eq!(quarantine(&h).await, None, "and nothing re-cooled");
    assert_eq!(h.tip(BRANCH), head);
}

/// cairn/7237 changes cairn/6752's merge-mode rule for the timeout class: an
/// owner's commit that meets a cooldown publishes with the catch-up pending
/// even with no session open.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_merge_mode_owner_commit_passes_a_cooldown_without_a_session() {
    let h = harness_or_skip!("merge_owner_commit_cooldown");
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let main = h.tip("main");
    crate::orchestrator::catch_up::set(
        &h.db,
        crate::orchestrator::catch_up::SetCatchUpMode {
            project_id: "proj-1",
            store: &h.store,
            bookmark: BRANCH,
            job_id: JOB,
            actor: "operator (test)",
            review_paths: &[],
            reviewed_through: &main,
        },
    )
    .await
    .unwrap();
    let base = h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);
    let first = h.prepare(PreparationPurpose::Run).await.unwrap_err();
    assert!(timed_out_at(&first, &head), "{first}");
    let owner = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    starve_compute(&h.store, false);
    assert!(!owner.advanced && owner.session.is_none() && owner.head_after == head);
    let pending = owner.advance_pending.as_deref().unwrap();
    assert!(
        pending.contains("catch-up merge") && pending.contains(&base),
        "{pending}"
    );
}

/// Time an explicit attempt lost before computing (here, its computation was
/// left 5 s of a 60 s request, as substantial lock contention would leave it)
/// is recorded with the blocker and never refuses a later attempt that has
/// that time back: the identical request with the full allowance runs.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn time_lost_to_waits_never_refuses_a_later_attempt_with_that_time_back() {
    let h = harness_or_skip!("explicit_starved_then_full");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.explicit_replay = true;
    request.compute_budget = Duration::from_secs(5);
    request.explicit_cost = Some(cost::ExplicitCost {
        started: std::time::Instant::now(),
        budget: Duration::from_secs(60),
        take_committed_tip: false,
        drop_incoming: false,
    });
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    starve_compute(&h.store, false);
    let PreparationRefusal::TimedOut {
        blocker: Some(blocker),
        ..
    } = refusal
    else {
        panic!("the explicit compute exhaustion records its blocker: {refusal}")
    };
    assert!(blocker.allowance_ms <= 5_000, "{blocker:?}");
    let sample = newest_sample(&h).await;
    assert!(sample.compute_allowance_ms.unwrap() <= 5_000, "{sample:?}");
    assert_eq!(sample.budget_ms, 60_000);

    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert_reply_receipt(&h, &answer, "rebased_clean").await;
    assert!(
        answer.contains("Replayed"),
        "the same request with its time back runs: {answer}"
    );
    assert!(branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());
}

/// Detached computation holds no store lock: while one branch's explicit
/// replay computes, an unrelated branch's preparation takes the lock and
/// installs.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unrelated_branch_installs_while_a_detached_computation_runs() {
    let h = harness_or_skip!("unrelated_during_compute");
    let sibling_ws = h.workspaces.path().join("dormant");
    crate::jj::add_workspace(&h.jj, &h.store, &sibling_ws, SIBLING, "main", None).unwrap();
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-dormant', 'exec-1', 'dormant', 'issue-1', 'proj-1', 'running', '{SIBLING}', NULL, 2, 2);"
    ))
    .await
    .unwrap();
    h.commit(&sibling_ws, SIBLING, "dormant.txt", "dormant\n");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");

    let orch = h.orch.clone();
    let (sender, received) = std::sync::mpsc::channel();
    let mut sender = Some(sender);
    on_compute(
        &h.store,
        Box::new(move |_| {
            let Some(sender) = sender.take() else {
                return;
            };
            let sibling = tokio::runtime::Handle::current().block_on(prepare_branch(
                &orch,
                PreparationRequest::new(
                    "proj-1",
                    SIBLING,
                    Some("job-dormant".to_string()),
                    PreparationPurpose::OwnerCommit,
                ),
            ));
            sender
                .send(
                    sibling
                        .map(|prepared| prepared.advanced)
                        .map_err(|r| r.to_string()),
                )
                .unwrap();
        }),
    );
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert_reply_receipt(&h, &answer, "rebased_clean").await;
    assert!(answer.contains("Replayed"), "{answer}");
    assert_eq!(
        received
            .recv_timeout(Duration::from_secs(5))
            .expect("the sibling ran during the computation"),
        Ok(true),
        "the sibling installed while the owner's computation held no lock"
    );
    assert!(h.is_ancestor(&base, &h.tip(SIBLING)));
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
}

/// Opt-in measurement of COMPLETE explicit requests (cairn/6456): a branch of
/// `n` commits that each change the hot file AND a file of their own, a base
/// that changed the hot file, a recorded conflict session and the owner's
/// committed resolution, then `take-committed-tip` replays through
/// [`replay`] under a real 10 s and a real 60 s budget, each on a fresh
/// fixture. Set `CAIRN_REPLAY_BENCH=1`; prints one `BENCH` line per request
/// with the wall time and the sample the request recorded.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn benchmark_explicit_replay_requests() {
    if std::env::var_os("CAIRN_REPLAY_BENCH").is_none() {
        return;
    }
    let sizes: Vec<usize> = std::env::var("CAIRN_REPLAY_BENCH_SIZES")
        .ok()
        .map(|sizes| sizes.split(',').filter_map(|n| n.parse().ok()).collect())
        .unwrap_or_else(|| vec![16, 50]);
    for n in sizes {
        for budget in [10u64, 60] {
            let h = harness_or_skip!("bench_explicit_request");
            let built = std::time::Instant::now();
            for i in 0..n {
                std::fs::write(
                    h.branch_ws.join(format!("own{i}.txt")),
                    format!("own {i}\n"),
                )
                .unwrap();
                h.commit(
                    &h.branch_ws,
                    BRANCH,
                    "shared.txt",
                    &format!("original\nX{i}\n"),
                );
            }
            h.advance_main("shared.txt", "original\nA\n");
            match h.prepare(PreparationPurpose::OwnerCommit).await {
                Err(PreparationRefusal::Conflict(_)) => {}
                other => panic!("the owner's commit records the conflict: {:?}", other.err()),
            }
            h.commit(
                &h.branch_ws,
                BRANCH,
                "shared.txt",
                &format!("original\nA\nX{}\n", n - 1),
            );
            let build_ms = built.elapsed().as_millis();
            let started = std::time::Instant::now();
            let answer = replay(
                &h.orch,
                &h.db,
                &ExplicitReplay {
                    job_id: JOB,
                    branch: BRANCH,
                    expected_fingerprint: None,
                    take_committed_tip: true,
                    drop_incoming_reason: None,
                    budget: Duration::from_secs(budget),
                    step_minimum: EXPLICIT_STEP_MINIMUM,
                    recovered: None,
                    increment_from: None,
                },
            )
            .await;
            let wall_ms = started.elapsed().as_millis();
            let outcome = match &answer {
                Ok(text) if text.contains("Replayed") => "landed".to_string(),
                Ok(text) | Err(text) => text.lines().next().unwrap_or_default().to_string(),
            };
            let sample = branch_samples(&h.db, "proj-1", &h.store, BRANCH, 1)
                .await
                .unwrap()
                .into_iter()
                .next()
                .and_then(Result::ok)
                .map(|sample| serde_json::to_string(&sample).unwrap())
                .unwrap_or_else(|| "none".to_string());
            let (host, load) = cost::host_context();
            println!(
                "BENCH request commits={n} budget_s={budget} build_ms={build_ms} \
                 wall_ms={wall_ms} host={} load1={} outcome={outcome:?} sample={sample}",
                host.unwrap_or_default(),
                load.map_or_else(|| "unavailable".to_string(), |load| format!("{load:.2}"))
            );
        }
    }
}

async fn assert_reply_receipt(h: &Harness, answer: &str, outcome: &str) {
    let expected =
        h.db.query_text(
            "SELECT replay_request_id || '|' || intent_id || '|' ||
                COALESCE(CAST(replay_served_at AS TEXT), 'not served')
         FROM jj_reconcile_items WHERE bookmark = ?1 AND replay_request_id IS NOT NULL
         ORDER BY replay_requested_at DESC, rowid DESC LIMIT 1",
            params![BRANCH],
        )
        .await
        .unwrap()
        .unwrap();
    let fields: Vec<_> = expected.split('|').collect();
    assert_ne!(
        fields[2], "not served",
        "the tested outcome must be durable"
    );
    assert_eq!(
        answer.lines().next().unwrap(),
        format!(
            "replay_request_id: `{}`; intent id: `{}`; outcome: {outcome}; served time: {} (Unix seconds).",
            fields[0], fields[1], fields[2]
        ),
        "{answer}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_conflicted_replay_reply_names_its_durable_request() {
    let h = harness_or_skip!("conflicted_receipt");
    h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nours\n");
    h.advance_main("shared.txt", "original\ntheirs\n");
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert_reply_receipt(&h, &answer, "conflicted").await;
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_superseded_replay_receipt_keeps_the_original_identity() {
    let h = harness_or_skip!("superseded_receipt");
    let head = h.tip(BRANCH);
    let base = h.tip("main");
    let first = conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &base,
        BRANCH,
        Some(&head),
        false,
        None,
    )
    .await
    .unwrap();
    let second = conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &base,
        BRANCH,
        Some(&head),
        false,
        None,
    )
    .await
    .unwrap();
    let answer = replay_receipt(&h.db, &first).await;
    assert!(
        answer.contains(&first.id) && answer.contains(&first.intent_id),
        "{answer}"
    );
    assert!(!answer.contains(&second.id), "{answer}");
    assert!(answer.contains("outcome: superseded"), "{answer}");
    let pending = replay_receipt(&h.db, &second).await;
    assert!(pending.contains(&second.id), "{pending}");
    assert!(
        pending.contains("outcome: pending; served time: not served"),
        "{pending}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_replay_refused_before_acceptance_says_no_request_was_minted() {
    let h = harness_or_skip!("unminted_receipt");
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap_err();
    assert!(
        answer.starts_with("No replay request was minted;"),
        "{answer}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_recovered_request_superseded_by_a_tip_move_returns_its_receipt() {
    let h = harness_or_skip!("moved_receipt");
    let head = h.tip(BRANCH);
    let base = h.tip("main");
    let request = conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &base,
        BRANCH,
        Some(&head),
        false,
        None,
    )
    .await
    .unwrap();
    h.commit(&h.branch_ws, BRANCH, "own.txt", "new tip\n");
    let answer = replay(
        &h.orch,
        &h.db,
        &ExplicitReplay {
            job_id: JOB,
            branch: BRANCH,
            expected_fingerprint: None,
            take_committed_tip: false,
            drop_incoming_reason: None,
            budget: EXPLICIT_REPLAY_BUDGET,
            step_minimum: EXPLICIT_STEP_MINIMUM,
            recovered: Some(request.clone()),
            increment_from: None,
        },
    )
    .await
    .unwrap();
    assert_reply_receipt(&h, &answer, "superseded").await;
    assert!(answer.lines().next().unwrap().contains(&request.id));
}

/// Opt-in wide-conflict measurement, excluding fixture construction and rebase.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn benchmark_pre_rebase_wide_conflicts() {
    if std::env::var_os("CAIRN_PRE_REBASE_BENCH").is_none() {
        return;
    }
    for n in [325usize, 748] {
        let h = harness_or_skip!("bench_pre_rebase_wide");
        let built = Instant::now();
        for i in 0..n {
            std::fs::write(h.branch_ws.join(format!("wide{i}.txt")), "own\n").unwrap();
            std::fs::write(h.main_ws.join(format!("wide{i}.txt")), "incoming\n").unwrap();
        }
        h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\n");
        let base = h.advance_main("shared.txt", "original\n");
        match h.prepare(PreparationPurpose::OwnerCommit).await {
            Err(PreparationRefusal::Conflict(_)) => {}
            other => panic!("expected conflict: {:?}", other.err()),
        }
        for i in 0..n {
            std::fs::write(h.branch_ws.join(format!("wide{i}.txt")), "incoming\nown\n").unwrap();
        }
        let tip = h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\n");
        let mut session = load_active_session(&h.db, BRANCH).await.unwrap().unwrap();
        assert_eq!(session.conflicting().count(), n);
        let fixture_s = built.elapsed().as_secs_f64();
        let started = Instant::now();
        let assessed_store = h.store.clone();
        let assessed_session = session.clone();
        let assessment = conflict_session::bounded_store_assessment(
            &h.jj,
            Duration::from_secs(60),
            AssessmentScope {
                phase: "take-committed-tip assessment",
                branch: BRANCH,
                candidate_paths: n,
            },
            move |jj| {
                Ok(conflict_session::assess_session_tip_exhaustively(
                    jj,
                    &assessed_store,
                    &assessed_session,
                    None,
                    conflict_session::ReplayScope::Carry,
                ))
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(assessment, RestoreProof::Assessed(_)),
            "{assessment:?}"
        );
        let assessment_s = started.elapsed().as_secs_f64();
        session.replay_requested_tip = Some(tip.clone());
        let carry_start = Instant::now();
        let planned_store = h.store.clone();
        let left = Duration::from_secs(60).saturating_sub(started.elapsed());
        let plan = conflict_session::bounded_store_assessment(
            &h.jj,
            left,
            AssessmentScope {
                phase: "resolution carry plan",
                branch: BRANCH,
                candidate_paths: n,
            },
            move |jj| plan_resolution_carry(jj, &planned_store, &session, &base, &tip),
        )
        .await
        .unwrap();
        assert!(plan.refused.is_empty());
        let carry_s = carry_start.elapsed().as_secs_f64();
        let total_s = started.elapsed().as_secs_f64();
        let (host, load) = cost::host_context();
        println!("BENCH pre_rebase paths={n} fixture_s={fixture_s:.6} assessment_s={assessment_s:.6} carry_s={carry_s:.6} total_s={total_s:.6} margin_s={:.6} host={host:?} load1={load:?}", 60.0-total_s);
        let request_started = Instant::now();
        let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
            .await
            .unwrap();
        let request_s = request_started.elapsed().as_secs_f64();
        let sample = newest_sample(&h).await;
        println!(
            "BENCH full_wide paths={n} request_s={request_s:.6} answer={answer:?} sample={}",
            serde_json::to_string(&sample).unwrap()
        );
        assert_reply_receipt(&h, &answer, "rebased_clean").await;
        assert!(answer.contains("Replayed"), "{answer}");
    }
}

#[test]
fn before_step_exhaustion_attributes_work_on_identical_requests_but_not_lock_waits() {
    for _ in 0..2 {
        let mut budget = Budget::start(Duration::from_secs(60), Duration::from_secs(1));
        budget.deadline = Instant::now();
        budget.record(
            "the take-committed-tip assessment",
            Duration::from_millis(59_900),
            Some(325),
            true,
        );
        let refusal = budget
            .admit("agent/budget", "the rebase, export and ancestry heal")
            .unwrap_err();
        assert!(refusal.contains("stopped before the rebase"), "{refusal}");
        assert!(
            refusal.contains("assessment of 325 paths used 59.900s"),
            "{refusal}"
        );
        assert!(refusal.contains("60.0s explicit budget"), "{refusal}");
        assert!(
            refusal.contains("Retrying with the same branch and base will time out again."),
            "{refusal}"
        );
        assert!(!refusal.contains("requested again at once"), "{refusal}");
    }
    let mut budget = Budget::start(Duration::from_secs(60), Duration::from_secs(1));
    budget.deadline = Instant::now();
    budget.record(
        "the base resolution (including lock waiting)",
        Duration::from_secs(59),
        None,
        false,
    );
    budget.record(
        "the take-committed-tip assessment",
        Duration::from_millis(900),
        Some(325),
        true,
    );
    let refusal = budget.admit("agent/budget", "the rebase").unwrap_err();
    assert!(refusal.contains("lock waiting) used 59.000s"), "{refusal}");
    assert!(
        refusal.contains("assessment of 325 paths used 0.900s"),
        "{refusal}"
    );
    assert!(!refusal.contains("will time out again"), "{refusal}");
    for completed_work in [None, Some(Duration::from_millis(100))] {
        let mut queued = Budget::start(Duration::from_secs(60), Duration::from_secs(1));
        queued.deadline = Instant::now();
        queued.record_assessment(
            "the assessment",
            325,
            &conflict_session::StoreAssessment {
                outcome: Ok(()),
                elapsed: Duration::from_millis(59_900),
                completed_work,
            },
            true,
        );
        let refusal = queued.admit("agent/budget", "the rebase").unwrap_err();
        assert!(
            refusal.contains("assessment of 325 paths used 59.900s"),
            "{refusal}"
        );
        assert!(
            !refusal.contains("will time out again"),
            "queue/grace is not deterministic work: {refusal}"
        );
    }
    *budget.coordinate.lock().unwrap() = Some(("old-head".into(), "old-base".into()));
    budget.start_attempt();
    assert!(budget.coordinate.lock().unwrap().is_none());
    budget.record(
        "the branch preparation",
        Duration::from_secs(59),
        None,
        false,
    );
    let refusal = budget
        .admit("agent/budget", "the next assessment")
        .unwrap_err();
    assert!(
        refusal.contains("branch preparation used 59.000s"),
        "{refusal}"
    );
    assert!(
        !refusal.contains("will time out again"),
        "discarded coordinates: {refusal}"
    );
}

fn exhausted_assessments() -> &'static Mutex<std::collections::HashSet<PathBuf>> {
    static STORES: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();
    STORES.get_or_init(Default::default)
}

pub(super) fn after_budgeted_assessment(store: &Path, budget: &Budget) {
    if exhausted_assessments().lock().unwrap().contains(store) {
        // Charge the assessment without making the test wait a minute.
        let mut steps = budget.steps.lock().unwrap();
        let step = steps.last_mut().unwrap();
        step.elapsed = Duration::from_millis(59_900);
        step.deterministic = step.elapsed;
        *budget.elapsed_offset.lock().unwrap() = Duration::from_secs(60);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn exhausted_assessment_names_work_on_two_identical_replay_requests() {
    let h = harness_or_skip!("assessment_budget_attribution");
    h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let base = h.advance_main("shared.txt", "incoming\n");
    assert!(matches!(
        h.prepare(PreparationPurpose::OwnerCommit).await,
        Err(PreparationRefusal::Conflict(_))
    ));
    let head = h.commit(&h.branch_ws, BRANCH, "shared.txt", "incoming\nmine\n");
    exhausted_assessments()
        .lock()
        .unwrap()
        .insert(h.store.clone());
    for _ in 0..2 {
        let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
            .await
            .unwrap();
        assert!(
            answer.contains("stopped before the resolution carry plan"),
            "{answer}"
        );
        assert!(
            answer.contains("assessment of 1 paths used 59.900s"),
            "{answer}"
        );
        assert!(
            answer.contains("Retrying with the same branch and base will time out again."),
            "{answer}"
        );
        assert_eq!(h.tip(BRANCH), head);
        assert_eq!(h.tip("main"), base);
        assert!(unserved(&h).await.is_empty());
        let sample = newest_sample(&h).await;
        assert!(
            answer.starts_with("No replay request was minted;"),
            "{answer}"
        );
        let (_, diagnostic) = answer.split_once("\n\n").unwrap();
        assert_eq!(sample.diagnostic.as_deref(), Some(diagnostic));
        assert_eq!(sample.head, head);
        assert_eq!(sample.base, base);
    }
    exhausted_assessments().lock().unwrap().remove(&h.store);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn replay_allowance_at_capture_controls_whether_a_repeat_is_identical() {
    use crate::orchestrator::base_advance::preparation::tests::{
        replay_trace, trace_replay, ReplayAdmissionObservation, ReplayAllowances,
    };
    let h = harness_or_skip!("explicit_allowance_boundary");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    h.advance_main("incoming.txt", "theirs\n");
    trace_replay(&h.store);
    starve_compute(&h.store, true);

    // Charge two seconds between admission and compute, without a sleep.
    {
        let _allowances =
            ReplayAllowances::new(&h.store, Duration::from_secs(58), Duration::from_secs(56));
        let first = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
            .await
            .unwrap();
        assert_reply_receipt(&h, &first, "timed out").await;
    }
    let blocked = branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap();
    assert_eq!(blocked[0].allowance_ms, 56_000);
    let samples = sample_count(&h).await;
    for ms in [55_000, 56_000, 57_000] {
        let _allowances = ReplayAllowances::new(
            &h.store,
            Duration::from_millis(ms),
            Duration::from_millis(ms),
        );
        let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None).await;
        assert!(
            answer.is_err(),
            "allowance {ms}: {answer:?}\n{:#?}",
            replay_trace(&h.store)
        );
        assert_eq!(sample_count(&h).await, samples);
    }
    assert_eq!(
        replay_trace(&h.store)
            .iter()
            .filter(|event| matches!(event, ReplayAdmissionObservation::Compute { .. }))
            .count(),
        1
    );

    // The strict boundary is exceeded by one millisecond: this must compute
    // even at unchanged coordinates, and still reports a normal timeout.
    let _allowances = ReplayAllowances::new(
        &h.store,
        Duration::from_millis(57_001),
        Duration::from_millis(57_001),
    );
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None).await;
    let trace = replay_trace(&h.store);
    eprintln!("controlled replay admission: {trace:#?}");
    assert!(answer.is_ok(), "{answer:?}\n{trace:#?}");
    assert_eq!(
        trace
            .iter()
            .filter(|event| matches!(event, ReplayAdmissionObservation::Compute { .. }))
            .count(),
        2,
        "{trace:#?}"
    );
    assert_eq!(sample_count(&h).await, samples + 1);
    starve_compute(&h.store, false);
}
