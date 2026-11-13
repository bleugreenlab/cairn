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

use crate::orchestrator::base_advance::preparation::tests::{on_compute, starve_compute};
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
    let head = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);

    let first = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
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
    let second = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
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
    starve_compute(&h.store, false);
    h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    let third = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(third.starts_with("Replayed"), "{third}");
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
    h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    starve_compute(&h.store, true);
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::TimedOut { .. }),
        "{refusal}"
    );
    let sample = newest_sample(&h).await;
    assert_eq!(sample.budget_class, BudgetClass::Background);
    assert_eq!(sample.outcome, CostOutcome::ComputeExhausted);
    assert!(
        branch_blockers(&h.db, "proj-1", &h.store, BRANCH)
            .await
            .unwrap()
            .is_empty(),
        "a background exhaustion records no blocker"
    );
    starve_compute(&h.store, false);
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(answer.starts_with("Replayed"), "{answer}");
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
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
    assert!(
        answer.starts_with("Replayed"),
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
    assert!(answer.starts_with("Replayed"), "{answer}");
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
                Ok(text) if text.starts_with("Replayed") => "landed".to_string(),
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
