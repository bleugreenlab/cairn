//! Storage and rendering of replay cost evidence and timeout blockers.

use super::*;
use crate::jj::{CarriedContent, ResolutionCarry};

const HEAD: &str = "1111111111111111111111111111111111111111";
const BASE: &str = "2222222222222222222222222222222222222222";
const STORE: &str = "/store";

async fn db(name: &str) -> LocalDb {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
           VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);"
            .to_string(),
    )
    .await
    .unwrap();
    db
}

fn key() -> TimeoutKey {
    TimeoutKey {
        project_id: "proj-1".into(),
        store_path: STORE.into(),
        branch: "agent/x".into(),
        head: HEAD.into(),
        base_branch: "main".into(),
        base: BASE.into(),
        mode: ReplayMode::Replay,
        resolution: "tip=true;drop=false;carry=0011223344556677".into(),
        engine_version: REPLAY_ENGINE_VERSION,
    }
}

fn observed() -> ReplayCostObservation {
    ReplayCostObservation {
        phase: Some(ComputePhase::Heal),
        branch_commits: Some(247),
        flagged_in_history: Some(14),
        flagged_after_rebase: Some(14),
        restored_commits: 9,
        affected_paths: Some(1),
        rebase_ms: Some(6_400),
        heal_ms: Some(51_000),
    }
}

fn blocker(key: TimeoutKey) -> TimeoutBlocker {
    TimeoutBlocker {
        key,
        phase: Some(ComputePhase::Heal),
        compute_ms: 57_500,
        allowance_ms: 57_800,
        budget_ms: 60_000,
        observed: observed(),
        build: Some("content-v2:abc".into()),
        host: Some("bbl.local".into()),
        recorded_at: 1_000,
    }
}

fn sample(head: &str, base: &str, outcome: CostOutcome) -> ReplayCostEvidence {
    ReplayCostEvidence {
        version: EVIDENCE_VERSION,
        engine_version: REPLAY_ENGINE_VERSION,
        build: Some("content-v2:abc".into()),
        recorded_at: 1_000,
        host: Some("bbl.local".into()),
        load1: Some(11.5),
        branch: "agent/x".into(),
        head: head.into(),
        base_branch: "main".into(),
        base: base.into(),
        mode: ReplayMode::Replay,
        budget_class: BudgetClass::Explicit,
        resolution: "none".into(),
        observed: observed(),
        phases: PhaseTimings {
            proof_ms: Some(2_200),
            capture_ms: Some(300),
            compute_ms: Some(57_500),
            install_ms: None,
            export_ms: None,
            total_ms: 60_000,
        },
        budget_ms: 60_000,
        compute_allowance_ms: Some(57_500),
        outcome,
        failure_phase: Some("detached computation (ancestry heal)".into()),
    }
}

fn at(head: Option<&'static str>, base: &'static str) -> (Option<&'static str>, String) {
    (head, base.to_string())
}

fn render(
    samples: Result<Vec<StoredSample>, String>,
    engine: Result<Vec<StoredSample>, String>,
    blockers: Result<Vec<TimeoutBlocker>, String>,
    live: (Option<&'static str>, String),
) -> String {
    render_in_mode(samples, engine, blockers, live, ReplayMode::Replay).0
}

/// The cost section and the next action that replaces the replay command.
fn render_in_mode(
    samples: Result<Vec<StoredSample>, String>,
    engine: Result<Vec<StoredSample>, String>,
    blockers: Result<Vec<TimeoutBlocker>, String>,
    live: (Option<&'static str>, String),
    mode: ReplayMode,
) -> (String, Option<String>) {
    let base = live.1.clone();
    let base_tip = move |_: &str| Some(base.clone());
    let live = LiveCoordinates {
        head: live.0,
        base_tip: &base_tip,
        mode: Some(mode),
    };
    (
        render_cost_section(&samples, &engine, &blockers, &live, 1_600),
        blocked_next_action(&blockers, &live),
    )
}

#[test]
fn the_resolution_fingerprint_changes_with_what_is_carried_and_not_with_wording() {
    let carry = |paths: Vec<(&str, CarriedContent)>| ResolutionCarry {
        expected_pre_tip: HEAD.into(),
        paths: paths
            .into_iter()
            .map(|(path, content)| (path.to_string(), content))
            .collect(),
    };
    assert_eq!(resolution_fingerprint(false, false, None), "none");
    let tip = carry(vec![("a.rs", CarriedContent::FromTip)]);
    let text = carry(vec![("a.rs", CarriedContent::Text("x".into()))]);
    let other_text = carry(vec![("a.rs", CarriedContent::Text("y".into()))]);
    let both = carry(vec![
        ("a.rs", CarriedContent::FromTip),
        ("b.rs", CarriedContent::FromTip),
    ]);
    let both_reordered = carry(vec![
        ("b.rs", CarriedContent::FromTip),
        ("a.rs", CarriedContent::FromTip),
    ]);
    let all = [
        resolution_fingerprint(true, false, Some(&tip)),
        resolution_fingerprint(true, true, Some(&tip)),
        resolution_fingerprint(true, false, Some(&text)),
        resolution_fingerprint(true, false, Some(&other_text)),
        resolution_fingerprint(true, false, Some(&both)),
        resolution_fingerprint(false, false, None),
        resolution_fingerprint(true, false, None),
    ];
    for (i, a) in all.iter().enumerate() {
        for b in &all[i + 1..] {
            assert_ne!(a, b, "each resolution is its own attempt");
        }
    }
    assert_eq!(
        resolution_fingerprint(true, false, Some(&both)),
        resolution_fingerprint(true, false, Some(&both_reordered)),
        "path order is not a different resolution"
    );
}

fn full(allowance_ms: u64) -> Attempt<'static> {
    Attempt {
        head: Some(HEAD),
        base: Some(BASE),
        mode: Some(ReplayMode::Replay),
        resolution: Some("tip=true;drop=false;carry=0011223344556677"),
        allowance: Some(Duration::from_millis(allowance_ms)),
    }
}

/// One decision over the whole key: engine, head, base, mode, resolution, and
/// whether the attempt could compute materially longer than the exhausted one.
#[test]
fn eligibility_weighs_every_key_dimension_and_the_available_allowance() {
    let blocked = blocker(key());
    assert_eq!(blocked.eligibility(&full(57_800)), Eligibility::Refused);
    assert_eq!(
        blocked.eligibility(&full(58_700)),
        Eligibility::Refused,
        "scheduling jitter is the same attempt"
    );
    assert!(
        matches!(blocked.eligibility(&full(59_000)), Eligibility::Different(why) if why.contains("can run 59.0s")),
        "time the exhausted attempt lost to waits is a different attempt once it is back"
    );
    let old = blocker(TimeoutKey {
        engine_version: REPLAY_ENGINE_VERSION + 1,
        ..key()
    });
    assert!(
        matches!(old.eligibility(&full(57_800)), Eligibility::Different(why) if why.contains("engine"))
    );
    for (what, attempt) in [
        (
            "merge mode",
            Attempt {
                mode: Some(ReplayMode::Merge),
                ..full(57_800)
            },
        ),
        (
            "another resolution",
            Attempt {
                resolution: Some("none"),
                ..full(57_800)
            },
        ),
        (
            "a new head",
            Attempt {
                head: Some("4444444444444444444444444444444444444444"),
                ..full(57_800)
            },
        ),
        (
            "an advanced base",
            Attempt {
                base: Some("5555555555555555555555555555555555555555"),
                ..full(57_800)
            },
        ),
    ] {
        assert!(
            matches!(blocked.eligibility(&attempt), Eligibility::Different(_)),
            "{what} is a different attempt"
        );
    }
    assert!(matches!(
        blocked.eligibility(&Attempt {
            head: None,
            ..full(57_800)
        }),
        Eligibility::Unknown(_)
    ));
}

#[tokio::test]
async fn a_blocker_matches_only_its_exact_key_and_never_outlives_its_coordinates() {
    let db = db("replay-cost-blocker-key.db").await;
    record_blocker(&db, &blocker(key())).await.unwrap();
    let same = Duration::from_millis(57_800);
    assert_eq!(
        matching_blocker(&db, &key(), same).await.unwrap(),
        Some(blocker(key()))
    );
    assert_eq!(
        matching_blocker(&db, &key(), Duration::from_secs(60))
            .await
            .unwrap(),
        None,
        "an attempt that can compute materially longer is a different one"
    );
    for (what, changed) in [
        (
            "merge mode",
            TimeoutKey {
                mode: ReplayMode::Merge,
                ..key()
            },
        ),
        (
            "another resolution",
            TimeoutKey {
                resolution: "none".into(),
                ..key()
            },
        ),
        (
            "another branch",
            TimeoutKey {
                branch: "agent/y".into(),
                ..key()
            },
        ),
    ] {
        assert_eq!(
            matching_blocker(&db, &changed, same).await.unwrap(),
            None,
            "{what} is a different attempt"
        );
    }
    // Those lookups were at the blocker's own coordinates (or another
    // branch's), so it still stands.
    assert_eq!(
        branch_blockers(&db, "proj-1", Path::new(STORE), "agent/x")
            .await
            .unwrap()
            .len(),
        1
    );
    // The base advanced: a new attempt, and the old row can never match again.
    let moved = TimeoutKey {
        base: "3333333333333333333333333333333333333333".into(),
        ..key()
    };
    assert_eq!(matching_blocker(&db, &moved, same).await.unwrap(), None);
    assert!(
        branch_blockers(&db, "proj-1", Path::new(STORE), "agent/x")
            .await
            .unwrap()
            .is_empty(),
        "a blocker never outlives its coordinates"
    );

    record_blocker(&db, &blocker(key())).await.unwrap();
    clear_blockers(&db, "proj-1", Path::new(STORE), "agent/x")
        .await
        .unwrap();
    assert_eq!(matching_blocker(&db, &key(), same).await.unwrap(), None);
}

#[tokio::test]
async fn samples_are_bounded_per_project_and_unknown_versions_are_unreadable_not_guessed() {
    let db = db("replay-cost-samples.db").await;
    record_blocker(&db, &blocker(key())).await.unwrap();
    for n in 0..(SAMPLES_KEPT_PER_PROJECT + 3) {
        let mut evidence = sample(HEAD, BASE, CostOutcome::Installed);
        evidence.recorded_at = n;
        record_sample(&db, "proj-1", Path::new(STORE), &evidence)
            .await
            .unwrap();
    }
    let kept = db
        .query_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM replay_cost_samples WHERE project_id = 'proj-1'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(kept, Some(SAMPLES_KEPT_PER_PROJECT.to_string()));
    assert_eq!(
        matching_blocker(&db, &key(), Duration::from_millis(57_800))
            .await
            .unwrap(),
        Some(blocker(key())),
        "sample retention never evicts a blocker"
    );
    let newest = branch_samples(&db, "proj-1", Path::new(STORE), "agent/x", 1)
        .await
        .unwrap();
    assert_eq!(
        newest[0].as_ref().unwrap().recorded_at,
        SAMPLES_KEPT_PER_PROJECT + 2
    );

    db.execute_script(
        "INSERT INTO replay_cost_samples
           (project_id, store_path, branch, engine_version, budget_class, outcome, recorded_at,
            evidence_json)
         VALUES ('proj-1', '/store', 'agent/x', 1, 'explicit', 'installed', 99999,
                 '{\"version\":0,\"heal\":3}');"
            .to_string(),
    )
    .await
    .unwrap();
    let legacy = branch_samples(&db, "proj-1", Path::new(STORE), "agent/x", 1)
        .await
        .unwrap();
    let reason = legacy[0].as_ref().unwrap_err();
    assert!(reason.contains("evidence version 0"), "{reason}");
    let page = render(
        Ok(legacy),
        Ok(Vec::new()),
        Ok(Vec::new()),
        at(Some(HEAD), BASE),
    );
    assert!(
        page.contains("cannot be read") && page.contains("its cost is unknown"),
        "{page}"
    );
}

#[test]
fn a_page_with_no_measurement_still_states_both_budgets_and_allows_a_first_attempt() {
    let page = render(
        Ok(Vec::new()),
        Ok(Vec::new()),
        Ok(Vec::new()),
        at(Some(HEAD), BASE),
    );
    for expected in [
        "## Replay cost",
        "explicit replay request has 60.0s in total",
        "automatic preparation (a commit, a check, a landing) has 10.0s",
        "Neither is widened",
        "No replay of this branch has been measured yet",
        "never refuses a first attempt",
        "healed a conflict-flagged commit yet",
    ] {
        assert!(page.contains(expected), "{expected}: {page}");
    }
    assert!(!page.contains("refused.**"), "{page}");
}

#[test]
fn a_measurement_reports_observed_counts_phases_and_that_the_heal_is_only_part_of_the_cost() {
    let page = render(
        Ok(vec![Ok(sample(HEAD, BASE, CostOutcome::ComputeExhausted))]),
        Ok(vec![
            Ok(sample(HEAD, BASE, CostOutcome::ComputeExhausted)),
            Ok({
                let mut other = sample(HEAD, BASE, CostOutcome::Installed);
                other.observed.heal_ms = Some(1_400);
                other
            }),
        ]),
        Ok(Vec::new()),
        at(Some(HEAD), BASE),
    );
    let header = format!(
        "explicit budget of 60.0s, replay mode, engine v{REPLAY_ENGINE_VERSION}, build `content-v2:abc`, host bbl.local, load 11.50"
    );
    for expected in [
        header.as_str(),
        "current: the branch and base are where it was measured",
        "247 branch commit(s); 14 already conflict-flagged in its history; 14 conflict-flagged after the rebase",
        "a newer base may introduce more or fewer, unknown until it runs",
        "9 restored; 1 distinct affected path(s)",
        "proof and carry plan 2.2s, capture 0.3s, compute 57.5s (of which rebase 6.4s and ancestry heal 51.0s), install not reached, export not reached; total 60.0s of 60.0s",
        "averaged 3643 ms per conflict-flagged commit in this one sample",
        "part of the request, not its total",
        "not a prediction",
        "Outcome: compute budget exhausted in detached computation (ancestry heal)",
        "the heal took 100 to 3643 ms per conflict-flagged commit",
        "not an estimate for this branch",
    ] {
        assert!(page.contains(expected), "{expected}: {page}");
    }
}

#[test]
fn stale_and_other_engine_measurements_are_labelled_as_such() {
    let moved_head = render(
        Ok(vec![Ok(sample(HEAD, BASE, CostOutcome::Installed))]),
        Ok(Vec::new()),
        Ok(Vec::new()),
        at(Some("4444444444444444444444444444444444444444"), BASE),
    );
    assert!(
        moved_head.contains("stale: the branch has a new head since"),
        "{moved_head}"
    );
    let moved_base = render(
        Ok(vec![Ok(sample(HEAD, BASE, CostOutcome::Installed))]),
        Ok(Vec::new()),
        Ok(Vec::new()),
        at(Some(HEAD), "5555555555555555555555555555555555555555"),
    );
    assert!(
        moved_base.contains("stale: the base has advanced since"),
        "{moved_base}"
    );
    let unknown = render(
        Ok(vec![Ok(sample(HEAD, BASE, CostOutcome::Installed))]),
        Ok(Vec::new()),
        Ok(Vec::new()),
        at(None, BASE),
    );
    assert!(unknown.contains("could not be read"), "{unknown}");

    let mut old_engine = sample(HEAD, BASE, CostOutcome::Installed);
    old_engine.engine_version = REPLAY_ENGINE_VERSION + 7;
    let page = render(
        Ok(vec![Ok(old_engine)]),
        Ok(Vec::new()),
        Ok(Vec::new()),
        at(Some(HEAD), BASE),
    );
    assert!(
        page.contains("does not describe a replay now"),
        "a measurement of another engine is not this engine's cost: {page}"
    );
}

#[test]
fn a_current_blocker_says_an_identical_replay_is_refused_what_changes_it_and_the_exit() {
    let page = render(
        Ok(Vec::new()),
        Ok(Vec::new()),
        Ok(vec![blocker(key())]),
        at(Some(HEAD), BASE),
    );
    let key_line = format!(
        "`agent/x` at `111111111111` onto `main` at `222222222222` (replay mode, resolution `tip=true;drop=false;carry=0011223344556677`, engine v{REPLAY_ENGINE_VERSION})"
    );
    for expected in [
        "**An identical explicit replay is refused.**",
        key_line.as_str(),
        "it ran 57.5s of the 57.8s it had, in ancestry heal",
        "14 conflict-flagged commit(s) to heal of 247 on the branch and 9 restored",
        "a new commit on `agent/x`, `main` advancing, a different resolution, a new replay engine version, or the operator's approval of merge catch-up mode",
        "nobody has to clear this record",
        "merge catch-up mode (cairn/6080)",
        "payload:{action:\"catch-up-mode\",mode:\"merge\"}",
        "asks the operator to approve",
        "never automatic",
        "can still conflict",
        "does not bypass an incomplete rollback",
    ] {
        assert!(page.contains(expected), "{expected}: {page}");
    }
    assert!(!page.contains("Request it again"), "{page}");

    let (_, next) = render_in_mode(
        Ok(Vec::new()),
        Ok(Vec::new()),
        Ok(vec![blocker(key())]),
        at(Some(HEAD), BASE),
        ReplayMode::Replay,
    );
    let next = next.expect("a current blocker replaces the replay next action");
    assert!(
        next.starts_with("## Next action")
            && next.contains("Do not request the same replay again")
            && !next.contains("action:\"replay\""),
        "{next}"
    );

    // Any key dimension the page can see that differs means it refuses
    // nothing, and the replay next action stays.
    let mut old_engine = blocker(key());
    old_engine.key.engine_version = REPLAY_ENGINE_VERSION + 1;
    let other_engine = format!("replay engine v{}", REPLAY_ENGINE_VERSION + 1);
    for (what, blockers, head, mode, says) in [
        (
            "a new head",
            vec![blocker(key())],
            "4444444444444444444444444444444444444444",
            ReplayMode::Replay,
            "new head",
        ),
        (
            "another engine",
            vec![old_engine],
            HEAD,
            ReplayMode::Replay,
            other_engine.as_str(),
        ),
        (
            "approved merge mode",
            vec![blocker(key())],
            HEAD,
            ReplayMode::Merge,
            "catch-up mode has changed",
        ),
    ] {
        let head: &'static str = head;
        let (page, next) = render_in_mode(
            Ok(Vec::new()),
            Ok(Vec::new()),
            Ok(blockers),
            at(Some(head), BASE),
            mode,
        );
        assert!(
            page.contains("refuses nothing now")
                && page.contains(says)
                && !page.contains("is refused.**"),
            "{what}: {page}"
        );
        assert_eq!(next, None, "{what} keeps the replay next action");
    }
}

#[test]
fn the_timeout_answer_and_the_refusal_share_the_blocker_guidance() {
    let blocked = blocker(key());
    let answer = timeout_guidance(Some(&blocked));
    assert!(answer.contains(&blocked_guidance(&blocked)), "{answer}");
    assert!(answer.contains(merge_catch_up_exit()), "{answer}");
    assert!(answer.contains("cairn://p/cairn/execution"), "{answer}");
    assert!(!answer.contains("requested again at once"), "{answer}");

    assert!(
        blocked_guidance(&blocked).contains("can run longer than 58.8s is a different attempt"),
        "time lost to waits is named as a way out"
    );
    let full = TimeoutBlocker {
        allowance_ms: 59_500,
        ..blocked.clone()
    };
    assert!(
        !blocked_guidance(&full).contains("can run longer than"),
        "no way out is promised that the budget cannot give"
    );
    let none = timeout_guidance(None);
    assert!(none.contains("No repeat was blocked"), "{none}");
}
