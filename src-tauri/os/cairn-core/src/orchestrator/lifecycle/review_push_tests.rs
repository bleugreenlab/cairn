use super::review_push::{
    bounded_rearm_candidates, rearm_candidate_jobs_sql, rearm_one_bounded_failed_review,
    resolve_producing_job_for_issue, resolve_producing_job_sql, review_artifact_ref_sql,
    spawn_turn_end_checks, BOUNDED_REARM_CANDIDATES_SQL, DORMANT_INFRA_RETRY_COOLDOWN_SECONDS,
};
use super::{
    create_review_push_for_pr_open, evaluate_review_readiness, record_bounded_rearm_lookup_failure,
};
use crate::db::DbState;
use crate::orchestrator::attention_push::{
    latest_push_fingerprint, list_pending, stamp_delivered, Boundary, Push, Wake,
};
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, RowExt, SearchIndex};
use cairn_db::turso::Value;
use std::sync::Arc;

const ISSUE_URI: &str = "cairn://p/prj/7";
const PLANBUILD_YAML: &str = include_str!("../../../../../packs/core/recipes/planbuild.yaml");
const REVIEW_KEY: &str = "review:cairn://p/prj/7";

async fn test_db() -> LocalDb {
    crate::storage::migrated_test_db("review-push.db").await
}

#[tokio::test]
async fn later_requester_that_wins_single_flight_becomes_the_durable_current_owner() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let first = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist first requester");
    let winner = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist concurrent requester");

    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch, "j-prod", &winner, "claimed", None,
    )
    .expect("the later requester wins single-flight");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch,
        "j-prod",
        &first,
        "superseded",
        Some("another turn-end check attempt won the slot"),
    )
    .expect("terminalize the losing requester");

    let current =
        crate::execution::checks_turn_end::current_turn_end_attempt(&orch.db.local, "j-prod")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(current.id, winner);
    assert_eq!(current.state, "claimed");
}

#[tokio::test]
async fn cancelled_wave_rearms_after_a_dormant_nodes_base_advances() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    let stale = orch
        .try_begin_turn_end_checks("j-prod", "a-stale")
        .expect("claim the stale wave's single-flight slot");
    crate::execution::checks::cancel_stale_review_on_branch_advance(
        &orch,
        "j-prod",
        crate::execution::checks::BranchAdvance::BaseReplay {
            patch_unchanged: false,
        },
    )
    .await;
    assert!(
        stale.is_cancelled(),
        "a base advance that altered the patch cancels the stale wave"
    );

    // The stale wave returning early releases its slot, and the release arms
    // the successor.
    drop(stale);

    assert!(
        wave_scheduled(&orch, "j-prod"),
        "a dormant PR owner receives a successor without another agent turn"
    );
}

fn waited_on_lanes(
    lanes: &[(&str, crate::execution::checks_status::NodeCheckState, &str)],
) -> crate::execution::checks_settlement::ChecksSnapshot {
    let statuses: Vec<_> = lanes
        .iter()
        .map(
            |(name, state, when)| crate::execution::checks_status::NodeCheckStatus {
                job_id: "j-prod".to_string(),
                request_id: None,
                name: name.to_string(),
                state: *state,
                policy: "advisory".to_string(),
                when: when.to_string(),
                cached: None,
                duration_ms: None,
                ran_at: None,
                passed: None,
                failed: None,
                skipped: None,
                deferred: None,
                parser: None,
                complete: None,
                selection: None,
                suite_failures: None,
                failure_names: Vec::new(),
                output_tail: None,
                failure_kind: None,
                suppressed_after: None,
                executor: None,
                verified_on: None,
                notice: None,
                declared_unverified: None,
                progress: None,
                base_attribution: None,
            },
        )
        .collect();
    let settlement = crate::execution::checks_settlement::classify(
        &statuses,
        crate::messages::delivery::HeadTurn::Idle,
        false,
    );
    crate::execution::checks_settlement::ChecksSnapshot {
        statuses,
        settlement,
        terminal_reason: None,
        retired: None,
    }
}

/// The cairn/4872 shape, closed lazily. A base advance rebased a dormant PR
/// owner's branch and armed nothing, leaving review lanes with no verdict for
/// the new head. The moment someone waits on those lanes, a wave is armed.
#[tokio::test]
async fn waiting_on_a_dormant_pr_owners_verdictless_review_lanes_arms_its_wave() {
    use crate::execution::checks_status::NodeCheckState;
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    let snapshot = waited_on_lanes(&[
        ("rust-tests", NodeCheckState::Passed, "review"),
        ("rust-lint", NodeCheckState::Pending, "review"),
    ]);
    assert!(crate::execution::checks_settlement::arm_wave_for_waiter(
        &orch, "j-prod", &snapshot
    ));
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the waiter's arrival arms the dormant owner's wave"
    );
}

/// A verdictless write lane is not something a review wave can answer, so
/// waiting on it arms nothing (cairn/4876 owns why it is verdictless).
#[tokio::test]
async fn waiting_on_a_verdictless_write_lane_arms_nothing() {
    use crate::execution::checks_status::NodeCheckState;
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    let snapshot = waited_on_lanes(&[
        ("rust-tests", NodeCheckState::Passed, "review"),
        ("rust-fmt", NodeCheckState::Pending, "write"),
    ]);
    assert!(!crate::execution::checks_settlement::arm_wave_for_waiter(
        &orch, "j-prod", &snapshot
    ));
    assert!(!wave_scheduled(&orch, "j-prod"));
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn bounded_rearm_resolves_agent_branch_from_managed_store() {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    // Backed by the project repository, as in production: the check contract
    // the due rule reads is read from the project's object database.
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/cairn-3604-builder-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    std::fs::write(workspace.join("agent-only.rs"), "wave path\n").unwrap();
    std::fs::create_dir_all(workspace.join(".cairn")).unwrap();
    std::fs::write(
        workspace.join(".cairn/config.yaml"),
        "checks:\n  review:\n    command: echo ok\n    when: review\n",
    )
    .unwrap();
    crate::jj::seal(&jj, &workspace, "agent branch", None).unwrap();
    let commit = crate::jj::head_commit(&jj, &workspace).unwrap();
    let tree_hash = crate::jj::logical_tree_hash(&jj, &store, &commit).unwrap();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repository.path().to_string_lossy().as_ref(),),
        )
        .await
        .unwrap();

    insert_artifact(&orch.db.local, "create-pr", 1).await;
    insert_failed_check(&orch.db.local, "infrastructure", 1).await;
    attach_planbuild_topology(&orch.db.local, "j-prod", NodeRole::Builder).await;
    orch.db
        .local
        .execute("UPDATE jobs SET status='running' WHERE id='j-prod'", ())
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1 WHERE job_id='j-prod'",
            (tree_hash.as_str(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();
    // The lane last failed under placement facts the fleet no longer has.
    insert_infrastructure_observation(
        &orch.db.local,
        "obs-stale",
        "review",
        "input",
        &tree_hash,
        1,
        Some("placement-facts-of-a-fleet-since-changed"),
    )
    .await;

    let candidate = super::review_push::BoundedRearmCandidate {
        job_id: "j-prod".to_string(),
        tree_hash,
        input_hash: "input".to_string(),
        ran_at: 1,
        check_name: "review".to_string(),
        failure_kind: "infrastructure".to_string(),
        environment_fingerprint: String::new(),
    };
    assert!(
        super::review_push::bounded_candidate_is_due(&orch, &candidate)
            .await
            .unwrap(),
        "the wave path must derive the managed store and resolve its agent-only branch"
    );
    assert!(
        rearm_one_bounded_failed_review(&orch).await,
        "the maintenance cadence must nominate the dormant running PR owner"
    );
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the maintenance cadence must autonomously dispatch a fresh wave"
    );
}

/// cairn/4982. A dormant PR owner whose only open lane was recorded as not
/// placeable has no turn, base advance, or waiter to arm a wave, and a machine
/// attaching schedules nothing by itself. The maintenance cadence is the
/// recovery edge: it passes the owner over while no known machine could take
/// the lane, and dispatches a wave once one enrolls.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_dormant_not_placeable_lane_is_rearmed_once_a_machine_that_can_take_it_enrolls() {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    // The store is backed by the project repository, as in production: the
    // check contract is read from the project's object database at the sealed
    // commit, so a store over some other repository would never find it.
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/cairn-4982-builder-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    // A platform the runner's own executor cannot provide.
    let platform = if std::env::consts::OS == "windows" {
        "linux"
    } else {
        "windows"
    };
    std::fs::create_dir_all(workspace.join(".cairn")).unwrap();
    std::fs::write(
        workspace.join(".cairn/config.yaml"),
        format!(
            "checks:\n  executor-windows-check:\n    command: cargo check --locked\n    executor:\n      requiredToolchains:\n      - rust\n    verdictPlatforms:\n    - {platform}\n    when: review\n"
        ),
    )
    .unwrap();
    crate::jj::seal(&jj, &workspace, "declare a lane no machine runs", None).unwrap();
    let commit = crate::jj::head_commit(&jj, &workspace).unwrap();
    let tree_hash = crate::jj::logical_tree_hash(&jj, &store, &commit).unwrap();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repository.path().to_string_lossy().as_ref(),),
        )
        .await
        .unwrap();
    insert_artifact(&orch.db.local, "create-pr", 1).await;
    insert_failed_check(&orch.db.local, "not_placeable", 0).await;
    attach_planbuild_topology(&orch.db.local, "j-prod", NodeRole::Builder).await;
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1, check_name='executor-windows-check' \
             WHERE job_id='j-prod'",
            (tree_hash.as_str(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();

    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap()[0].failure_kind,
        "not_placeable",
        "a not-placeable lane is a candidate for the maintenance cadence"
    );
    let candidate = &bounded_rearm_candidates(&orch.db).await.unwrap()[0];
    assert!(
        crate::execution::checks::load_checks_contract_at_commit(
            repository.path(),
            &crate::jj::head_commit(&jj, &workspace).unwrap(),
        )
        .await
        .is_some_and(|loaded| loaded.contract.checks.contains_key(&candidate.check_name)),
        "the lane's definition is readable where the cadence reads it, so the refusals \
         below are the fleet's answer and not a missing contract"
    );
    for _ in 0..3 {
        assert!(
            !rearm_one_bounded_failed_review(&orch).await,
            "no wave while no known machine could take the lane"
        );
    }
    assert!(!wave_scheduled(&orch, "j-prod"));

    orch.fleet
        .declare_enrolled_remote("bgsrv-win", "bgsrv-win", platform, "x86_64");
    assert!(
        rearm_one_bounded_failed_review(&orch).await,
        "the first tick after a machine that can take it enrolls nominates the owner"
    );
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "and dispatches the wave that runs it"
    );
}

/// The scope set a review wave for `j-prod` at `commit` reads `check` against.
async fn review_wave_scopes(
    orch: &Orchestrator,
    repository: &str,
    commit: &str,
    check: &crate::config::project_settings::CheckCommand,
) -> crate::execution::cache::InfraScopes {
    crate::execution::checks_turn_end::review_infra_scopes(
        orch,
        "p-rev",
        "j-prod",
        commit,
        repository,
        &[crate::execution::checks_turn_end::review_check_placement_question(Some(check))],
    )
    .await
    .pop()
    .unwrap()
}

/// cairn/4983. An enrolled-but-detached machine is a wait, not a refusal, so
/// the lane goes to placement, fails as infrastructure, and is charged in the
/// FLEET domain. No timer repairs that: the dormant re-arm used to zero the
/// fleet counter every cooldown, and nominate the job every tick off the
/// streak-zero re-stamp a suppressed wave leaves behind, so the lane ran three
/// more times every five minutes for as long as the machine stayed away.
///
/// cairn/5671. A fleet counter counts heads, so every wave of this one head is
/// one count; what keeps the dormant cadence from re-asking it every tick is
/// that its newest death was against the very fleet the wave would meet.
///
/// The wave is simulated with the real counter: a cleared claim is one
/// execution and stores a fleet-domain infrastructure failure, a suppressed
/// claim re-stamps the head row exactly as `hold_back_suppressed` does.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_fleet_bounded_lane_is_never_released_on_a_timer_and_runs_once_its_machine_attaches() {
    use crate::execution::cache::{CheckExecutionClaim, CheckResultCacheWrite};
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/cairn-4983-builder-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    let platform = if std::env::consts::OS == "windows" {
        "linux"
    } else {
        "windows"
    };
    std::fs::create_dir_all(workspace.join(".cairn")).unwrap();
    std::fs::write(
        workspace.join(".cairn/config.yaml"),
        format!(
            "checks:\n  executor-windows-check:\n    command: cargo check --locked\n    executor:\n      requiredToolchains:\n      - rust\n    verdictPlatforms:\n    - {platform}\n    when: review\n"
        ),
    )
    .unwrap();
    crate::jj::seal(
        &jj,
        &workspace,
        "declare a lane on a detached machine",
        None,
    )
    .unwrap();
    let commit = crate::jj::head_commit(&jj, &workspace).unwrap();
    let tree_hash = crate::jj::logical_tree_hash(&jj, &store, &commit).unwrap();
    let repo_path = repository.path().to_string_lossy().to_string();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repo_path.as_str(),),
        )
        .await
        .unwrap();
    insert_artifact(&orch.db.local, "create-pr", 1).await;
    attach_planbuild_topology(&orch.db.local, "j-prod", NodeRole::Builder).await;
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();
    // Enrolled, never attached: placeable in principle, so this is a wait.
    orch.fleet
        .declare_enrolled_remote("bgsrv-win", "bgsrv-win", platform, "x86_64");

    let check_name = "executor-windows-check";
    let input_hash = "ih-windows";
    let check =
        crate::execution::checks::load_checks_contract_at_commit(repository.path(), &commit)
            .await
            .and_then(|loaded| loaded.contract.checks.get(check_name).cloned())
            .expect("the lane is declared at the sealed commit");
    assert!(
        orch.fleet
            .unplaceable_reason(
                check.executor.as_ref(),
                &crate::execution::check_identity::verdict_platforms(&check),
            )
            .is_none(),
        "an enrolled machine makes the lane a wait, not a refusal"
    );
    let detached = review_wave_scopes(&orch, &repo_path, &commit, &check).await;
    let fleet_scope = detached
        .fleet
        .clone()
        .expect("a review wave sees the fleet");

    let db = orch.db.local.clone();
    let mut executions = 0;
    let mut wave = |scopes: &crate::execution::cache::InfraScopes| {
        match crate::execution::cache::claim_check_execution(
            db.clone(),
            "p-rev",
            check_name,
            input_hash,
            scopes,
        )
        .unwrap()
        {
            CheckExecutionClaim::Clear { .. } => {
                executions += 1;
                crate::execution::cache::store_check_result(
                    db.clone(),
                    CheckResultCacheWrite {
                        project_id: "p-rev".to_string(),
                        tree_hash: tree_hash.clone(),
                        input_hash: input_hash.to_string(),
                        check_name: check_name.to_string(),
                        exit_code: -1,
                        passed: false,
                        output_tail: "NoMatchingExecutor".to_string(),
                        duration_ms: 0,
                        target_results_json: None,
                        job_id: Some("j-prod".to_string()),
                        cached: Some(false),
                        failure_kind: Some("infrastructure".to_string()),
                        executor_id: None,
                        executor_device_id: None,
                        executor_connection_generation: None,
                        executor_cell_id: None,
                        executor_lease_epoch: None,
                        executor_started_at_unix_ms: None,
                        executor_finished_at_unix_ms: None,
                        toolchain_fingerprint: None,
                        defined_by_commit_sha: Some(commit.clone()),
                        environment_fingerprint: scopes
                            .fleet
                            .clone()
                            .expect("a review wave sees the fleet"),
                        verdict_platform: None,
                        verdict_arch: None,
                        verdict_environment_hash: None,
                        transient_within_grace: false,
                    },
                )
                .unwrap();
            }
            CheckExecutionClaim::Suppressed(bounded) => {
                crate::execution::checks::record_suppressed_check(
                    &db, &tree_hash, "j-prod", &commit, check_name, &bounded,
                );
            }
        }
    };

    // The turn that pushed the commit, plus the waves its own agent triggers.
    for _ in 0..6 {
        wave(&detached);
    }
    // The observation the last of those waves recorded, under the placement
    // facts it met.
    insert_infrastructure_observation(
        &orch.db.local,
        "obs-detached",
        check_name,
        input_hash,
        &tree_hash,
        chrono::Utc::now().timestamp_millis(),
        Some(&detached.placement_facts()),
    )
    .await;
    // Every row is now well past the cooldown.
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET ran_at = ran_at - ?1",
            ((DORMANT_INFRA_RETRY_COOLDOWN_SECONDS + 60) * 1000,),
        )
        .await
        .unwrap();

    for tick in 0..20 {
        let nominated = super::review_push::bounded_rearm_candidate(&orch)
            .await
            .unwrap();
        assert_eq!(
            nominated, None,
            "tick {tick}: neither the cooled fleet row nor the re-stamp the suppressed wave \
             left behind makes a fleet-bounded lane due"
        );
        if let Some(candidate) = nominated {
            assert_eq!(candidate.job_id, "j-prod");
            wave(&review_wave_scopes(&orch, &repo_path, &commit, &check).await);
        }
    }
    let fleet_streak: i64 = orch
        .db
        .local
        .read(move |conn| {
            let fleet_scope = fleet_scope.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT infra_failure_streak FROM check_result_cache
                          WHERE environment_fingerprint = ?1",
                        (fleet_scope.as_str(),),
                    )
                    .await?;
                rows.next().await?.expect("the fleet row").i64(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        fleet_streak, 1,
        "six waves of one head against one fleet are one arm cycle"
    );
    assert!(!rearm_one_bounded_failed_review(&orch).await);
    assert!(!wave_scheduled(&orch, "j-prod"));
    assert_eq!(
        executions, 6,
        "the agent's own waves ran; twenty dormant ticks added none while the fleet stayed as it is"
    );

    // The machine attaches. Its arrival moves the fleet's capability identity,
    // which is this domain's repair.
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    std::mem::forget(receiver);
    orch.fleet
        .attach_advertised_executor(
            cairn_common::executor_protocol::ExecutorAdvertisement {
                identity: cairn_common::executor_protocol::ExecutorIdentity {
                    device_id: "bgsrv-win-device".into(),
                    executor_id: "bgsrv-win".into(),
                    display_name: "bgsrv-win".into(),
                },
                capabilities: cairn_common::executor_protocol::ExecutorCapabilities {
                    os: platform.into(),
                    arch: "x86_64".into(),
                    logical_cores: 8,
                    concurrency_capacity: None,
                    toolchains: vec!["rust".into()],
                    projects_served: Vec::new(),
                    disk_budget_bytes: None,
                    memory_budget_bytes: None,
                    sandbox: None,
                    toolchain_detection: None,
                    build_prerequisites: None,
                    containment: None,
                    page_call_programs: None,
                    github_credential: None,
                    machine_services: Vec::new(),
                },
                current_load: 0,
                warm_roots: Vec::new(),
                observed_at_unix_ms: chrono::Utc::now().timestamp_millis() as u64,
                liveness_observed_at_unix_ms: None,
                facts_stale_refresh_blocked: None,
                last_refresh: None,
                node_scratches: Vec::new(),
            },
            sender,
            false,
            None,
            None,
        )
        .unwrap();
    let attached = review_wave_scopes(&orch, &repo_path, &commit, &check).await;
    assert_ne!(
        attached.fleet, detached.fleet,
        "an attached machine that can take the lane moves the capability identity"
    );
    assert!(
        matches!(
            crate::execution::cache::claim_check_execution(
                orch.db.local.clone(),
                "p-rev",
                check_name,
                input_hash,
                &attached,
            )
            .unwrap(),
            CheckExecutionClaim::Clear { .. }
        ),
        "the next wave executes the lane"
    );
    assert!(
        rearm_one_bounded_failed_review(&orch).await,
        "the first tick after the machine attaches nominates the dormant owner"
    );
    assert!(wave_scheduled(&orch, "j-prod"));
}

#[tokio::test]
async fn bounded_rearm_lookup_failure_becomes_visible_infrastructure_evidence() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_failed_check(&db, "capacity", 1).await;
    let orch = test_orchestrator(db);

    record_bounded_rearm_lookup_failure(&orch, "j-prod", "object database unavailable")
        .await
        .unwrap();

    let row = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT failure_kind, output_tail, infra_failure_streak
                           FROM check_result_cache WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                let row = rows.next().await?.expect("check result");
                Ok::<_, crate::storage::DbError>((row.text(0)?, row.text(1)?, row.i64(2)?))
            })
        })
        .await
        .unwrap();
    assert_eq!(row.0, "infrastructure");
    assert!(row.1.contains("object database unavailable"), "{}", row.1);
    assert_eq!(row.2, 2);
}

/// `check_result_cache` holds two row families, and the wave-preparation repaint
/// must only ever touch one of them. The other is the hot projection of an
/// immutable observation: painting `infrastructure` over a green would leave the
/// row contradicting the very observation it names, and would strip that green of
/// reuse, because the reusable lookup requires a null `failure_kind`.
#[tokio::test]
async fn bounded_rearm_lookup_failure_leaves_a_recorded_verdict_alone() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_failed_check(&db, "capacity", 1).await;
    // A genuine green for a DIFFERENT check on the same job: a real immutable
    // observation, plus the hot row that projects it. This is the exact shape the
    // repaint must not touch — the row names the observation, so contradicting it
    // would make the cache disagree with the evidence it points at.
    db.execute_script(
        "INSERT INTO check_result_observations
           (id, project_id, commit_sha, tree_hash, check_name, input_hash,
            environment_fingerprint, exit_code, verdict, complete, reusable,
            parser_version, result_schema_version, ran_at, duration_ms, job_id,
            cadence, output_tail)
         VALUES('obs-1','p-rev','commit','tree','typecheck','input-tc','env-a',0,'passed',1,1,
                1,1,1,5,'j-prod','review','ok');
         INSERT INTO check_result_cache
           (project_id, tree_hash, input_hash, check_name, environment_fingerprint,
            result_schema_version, source_observation_id, exit_code, passed,
            output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
         VALUES('p-rev','tree','input-tc','typecheck','env-a',1,'obs-1',0,1,'ok',5,1,'j-prod',NULL,0);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    record_bounded_rearm_lookup_failure(&orch, "j-prod", "object database unavailable")
        .await
        .unwrap();

    let rows = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT check_name, passed, COALESCE(failure_kind,''), infra_failure_streak
                           FROM check_result_cache WHERE job_id='j-prod'
                          ORDER BY check_name",
                        (),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.i64(1)?, row.text(2)?, row.i64(3)?));
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            ("review".to_string(), 0, "infrastructure".to_string(), 2),
            ("typecheck".to_string(), 1, String::new(), 0),
        ],
        "the infra row takes the failure and the streak; the recorded verdict is untouched"
    );
}

#[tokio::test]
async fn bounded_rearm_selects_retryable_review_work() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    insert_failed_check(&db, "infrastructure", 0).await;

    let orch = test_orchestrator(db);
    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap()[0].job_id,
        "j-prod",
        "transient infrastructure failures are eligible below the retry bound"
    );

    orch.db
        .local
        .execute("UPDATE jobs SET status='running' WHERE id='j-prod'", ())
        .await
        .unwrap();
    orch.db
        .local
        .execute(
            // `ran_at` is Unix MILLISECONDS; ten minutes back is past the cooldown.
            "UPDATE check_result_cache SET infra_failure_streak=?1, ran_at=(unixepoch()-600)*1000",
            (crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,),
        )
        .await
        .unwrap();
    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap()[0].job_id,
        "j-prod",
        "a dormant open-PR owner is retried after the bounded cooldown even while its job remains running"
    );

    orch.db
        .local
        .execute("UPDATE turns SET state='running' WHERE id='t-prod'", ())
        .await
        .unwrap();
    assert!(
        bounded_rearm_candidates(&orch.db).await.unwrap().is_empty(),
        "a live agent turn must never be mistaken for dormant review work"
    );
    orch.db
        .local
        .execute("UPDATE turns SET state='complete' WHERE id='t-prod'", ())
        .await
        .unwrap();
    assert!(
        !rearm_one_bounded_failed_review(&orch).await,
        "a cooled candidate is asked, and nothing about its lane has changed"
    );
    let streak: i64 = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT infra_failure_streak FROM check_result_cache WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                rows.next().await?.expect("check result").i64(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        streak,
        crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
        "cooling is not a fact: no counter is released without one (cairn/5357)"
    );

    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET infra_failure_streak=?1, ran_at=unixepoch()*1000",
            (crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,),
        )
        .await
        .unwrap();
    assert!(
        bounded_rearm_candidates(&orch.db).await.unwrap().is_empty(),
        "the attempt bound remains a hot-loop circuit breaker during the cooldown"
    );

    orch.db
        .local
        .execute_script(
            "UPDATE turns SET start_reason='memory_review' WHERE id='t-prod';
             INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment,
                              node_name, branch, created_at, updated_at)
               VALUES('j-later','e-rev','p-rev','i-rev','running','later','builder','later',2,2);
             INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
               VALUES('t-later','s-later','j-later',1,'complete','follow_up',2,2);
             INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name,
                confirmed, created_at, updated_at)
               VALUES('a-later','j-later','create-pr',1,'{}',1,'create-pr',1,2,2);
             INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
               VALUES('p-rev','tree','later-input','review',-1,0,'failed',0,2,
                      'j-later','infrastructure',1);",
        )
        .await
        .unwrap();
    let candidates = bounded_rearm_candidates(&orch.db).await.unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.job_id.as_str())
            .collect::<Vec<_>>(),
        vec!["j-later"],
        "a completed memory-review head must not starve later dormant PR recovery"
    );
}

/// The bounded re-arm sweep re-reads its candidate set every 30 seconds, so what
/// that read costs has to follow the number of CANDIDATES. It used to follow the
/// check HISTORY: the statement drove off `check_result_cache`, and on a
/// production runner with ~34k cached results it spent 17 seconds of every
/// 30-second tick -- one full core, permanently -- to return single digits
/// (CAIRN-4196).
///
/// The plan is what pins this, because the regression is invisible to every
/// cheaper seam: the old shape was also one statement, in one read transaction,
/// returning exactly these rows. Only its plan said that finding a handful of
/// dormant reviews meant walking every check ever recorded.
#[tokio::test]
async fn bounded_rearm_candidates_never_walk_the_check_history() {
    let db = test_db().await;
    let plan = query_plan(
        &db,
        BOUNDED_REARM_CANDIDATES_SQL,
        vec![Value::Integer(DORMANT_INFRA_RETRY_COOLDOWN_SECONDS)],
    )
    .await;

    assert!(
        !plan.contains("SCAN check_result_cache"),
        "the sweep reaches the cache one live job at a time; a plan that scans \
         it is the history walk coming back:\n{plan}"
    );
    assert!(
        plan.contains("SEARCH c USING INDEX idx_check_result_cache_job"),
        "the driving set is `live_job`, and each of its jobs seeks its own cache \
         rows through idx_check_result_cache_job (migration 0093):\n{plan}"
    );
    let cache_reads = plan
        .lines()
        .filter(|step| step.contains("check_result_cache"))
        .count();
    assert_eq!(
        cache_reads, 1,
        "the cache is read exactly ONCE and ranked; a second access is the \
         newest-row anti-join coming back, which pays for a job's whole history \
         once per row of it:\n{plan}"
    );
    assert!(
        plan.contains("SEARCH j USING INDEX idx_jobs_issue_id"),
        "`live_job` is driven by the unresolved ISSUES and reaches their jobs by \
         index; driving it from `jobs` walks every job ever run:\n{plan}"
    );
}

/// Narrowing to live work does NOT exclude a long-lived unresolved issue's own
/// history, and `check_result_cache` keeps a row per input hash per check
/// forever, so one busy job accumulates without bound. That is the case where an
/// anti-join would have stayed quadratic after the live-work drive: 400 rows
/// here would have meant re-reading them once per row.
///
/// What this test holds is the RESULT at that scale -- only the current row of
/// each check survives, none of the 396 behind them. The cost shape is held by
/// the single-read assertion in the plan test above; a runtime bound would only
/// have been a flake.
#[tokio::test]
async fn superseded_history_on_a_live_job_yields_only_its_newest_rows() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;

    let mut script = String::new();
    for generation in 1..=100 {
        for check in 0..4 {
            let ran_at = generation * 1000 + check;
            script.push_str(&format!(
                "INSERT INTO check_result_cache
                   (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                    output_tail, duration_ms, ran_at, job_id, failure_kind,
                    infra_failure_streak)
                 VALUES('p-rev','tree','h{ran_at}','c{check}',-1,0,'failed',0,{ran_at},
                        'j-prod','infrastructure',0);"
            ));
        }
    }
    db.execute_script(&script).await.unwrap();
    let orch = test_orchestrator(db);

    let candidates = bounded_rearm_candidates(&orch.db).await.unwrap();
    let ran_at: Vec<i64> = candidates.iter().map(|row| row.ran_at).collect();
    assert_eq!(
        ran_at,
        vec![100_000, 100_001, 100_002, 100_003],
        "one candidate per check name -- its newest row -- and nothing from the \
         superseded generations behind them"
    );
}

/// Each pair removes exactly one reason the seeded job is a candidate, then puts
/// it back. The issue-status arm is the one CAIRN-4196 MOVED -- it became the
/// driving CTE rather than an outer predicate -- so what it excludes is the part
/// of that change a plan assertion cannot speak to.
const REARM_ARM_MUTATIONS: [(&str, &str, &str); 3] = [
    (
        "UPDATE issues SET status='merged' WHERE id='i-rev'",
        "UPDATE issues SET status='active' WHERE id='i-rev'",
        "a resolved issue is not live work, and live work is the driving set",
    ),
    (
        "UPDATE check_result_cache SET failure_kind='timed_out'",
        "UPDATE check_result_cache SET failure_kind='infrastructure'",
        "only capacity and infrastructure failures are substrate faults",
    ),
    (
        "UPDATE artifacts SET artifact_type='plan'",
        "UPDATE artifacts SET artifact_type='create-pr'",
        "without a create-pr artifact or an open PR there is no review to re-arm",
    ),
];

#[tokio::test]
async fn bounded_rearm_candidate_arms_survive_the_live_job_drive() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    insert_failed_check(&db, "infrastructure", 0).await;
    let orch = test_orchestrator(db);

    let baseline = bounded_rearm_candidates(&orch.db).await.unwrap();
    assert_eq!(
        baseline.first().map(|candidate| candidate.job_id.as_str()),
        Some("j-prod"),
        "the seeded job satisfies every arm of the candidate statement"
    );

    for (suppress, restore, why) in REARM_ARM_MUTATIONS {
        orch.db.local.execute(suppress, ()).await.unwrap();
        let suppressed = bounded_rearm_candidates(&orch.db).await.unwrap();
        assert!(suppressed.is_empty(), "{why}");
        orch.db.local.execute(restore, ()).await.unwrap();
    }

    let restored = bounded_rearm_candidates(&orch.db).await.unwrap();
    assert_eq!(
        restored.first().map(|candidate| candidate.job_id.as_str()),
        Some("j-prod"),
        "every arm was restored, so the job is a candidate again"
    );
}

async fn query_plan(db: &LocalDb, sql: &str, binds: Vec<Value>) -> String {
    let sql = format!("EXPLAIN QUERY PLAN {sql}");
    db.read(move |conn| {
        let sql = sql.clone();
        let binds = binds.clone();
        Box::pin(async move {
            let mut rows = conn.query(&sql, binds).await?;
            let mut plan = String::new();
            while let Some(row) = rows.next().await? {
                plan.push_str(&row.text(3)?);
                plan.push('\n');
            }
            Ok::<_, crate::storage::DbError>(plan)
        })
    })
    .await
    .unwrap()
}

/// Every reviewable-output statement must cost the JOB's artifacts, never the
/// workspace's.
///
/// The plan is the only seam where that difference is visible: the regression
/// returned the right job from one statement, while the planner, keeping no
/// statistics, drove each job's probe from `idx_artifacts_type` and fetched
/// every create-pr and plan row ever written, overflow pages included. That was
/// 90 s p50 for one evaluation of a 119-job issue on the 11-02 production clone
/// (cairn/5643), the same trap cairn/4207 pinned in the attention-push resolver.
#[tokio::test]
async fn review_readiness_statements_seek_by_job() {
    let db = test_db().await;
    let statements = [
        (
            "resolve_producing_job_for_issue",
            resolve_producing_job_sql(),
            vec![Value::Text("i-rev".to_string())],
            "SEARCH a USING COVERING INDEX idx_artifacts_job_review",
        ),
        (
            "rearm_candidate_jobs",
            rearm_candidate_jobs_sql(),
            Vec::new(),
            "SEARCH a USING COVERING INDEX idx_artifacts_job_review",
        ),
        // `updated_at` is selected and not covered, so the chain heads that
        // pass the predicate fetch their row; the walk itself stays on the index.
        (
            "review_artifact_ref",
            review_artifact_ref_sql(),
            vec![Value::Text("j-prod".to_string())],
            "SEARCH a USING INDEX idx_artifacts_job_review",
        ),
    ];
    for (name, sql, binds, outer) in statements {
        let plan = query_plan(&db, &sql, binds).await;
        assert!(
            plan.contains(outer),
            "{name}: a job's artifacts are reached by job, through the index \
             that covers the reviewable predicate (migration 0333):\n{plan}"
        );
        assert!(
            plan.contains("SEARCH newer USING COVERING INDEX idx_artifacts_output_job_version"),
            "{name}: the chain-head test seeks 0306's index and reads no row \
             bodies:\n{plan}"
        );
        assert!(
            !plan.contains("idx_artifacts_type"),
            "{name}: an artifact index keyed by TYPE is keyed by the workspace's \
             history: every plan or create-pr ever written matches it, and \
             intersecting that set per job is the 90-second evaluation coming \
             back:\n{plan}"
        );
    }

    let rearm = query_plan(&db, &rearm_candidate_jobs_sql(), Vec::new()).await;
    assert!(
        rearm.contains("SEARCH j USING INDEX idx_jobs_issue_id") && !rearm.contains("SCAN jobs"),
        "the boot re-arm is driven by the unresolved ISSUES and reaches their \
         jobs by index; driving it from `jobs` walks every job ever run:\n{rearm}"
    );
}

/// The cost the plan pin above protects, held at a scale where losing it
/// shows: ~10k create-pr and plan artifacts on unrelated jobs, each carrying
/// 8 KB of data so every row overflows, and an issue of 50 jobs where only one
/// owns a reviewable head (the other 49 own confirmed plans, which are not
/// reviewable). Resolving the producing job must cost that issue's artifacts.
///
/// Measured on a debug build (Linux arm64, shared executor): 7.4 ms with the
/// covering index, 1.17 s with the same statement over a bare `artifacts a`,
/// which the planner drives from `idx_artifacts_type`. The bound sits between
/// them, far from both.
#[tokio::test]
async fn resolving_the_producing_job_costs_the_issue_not_the_workspace() {
    const NOISE_ARTIFACTS: usize = 10_000;
    const NOISE_JOBS: usize = 200;
    const ISSUE_JOBS: usize = 50;
    const BOUND_MS: u64 = 250;

    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES('i-noise','p-rev',8,'Noise','active','active','none',1,1)",
                (),
            )
            .await?;
            for n in 0..NOISE_JOBS {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, node_name, created_at, updated_at)
                     VALUES(?1,'p-rev','i-noise','complete','noise',1,1)",
                    (format!("j-noise-{n}"),),
                )
                .await?;
            }
            // `j-watch` is the seed's second job on the issue.
            for n in 2..ISSUE_JOBS {
                let job = format!("j-issue-{n}");
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, node_name, created_at, updated_at)
                     VALUES(?1,'p-rev','i-rev','complete','other',?2,?2)",
                    (job.as_str(), n as i64 + 10),
                )
                .await?;
                conn.execute(
                    "INSERT INTO artifacts
                       (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
                     VALUES(?1,?2,'plan',1,'{}',1,'plan',1,1,1)",
                    (format!("a-issue-{n}"), job.as_str()),
                )
                .await?;
            }
            let body = format!("{{\"pad\":\"{}\"}}", "x".repeat(8 * 1024));
            for n in 0..NOISE_ARTIFACTS {
                let kind = if n % 4 == 0 { "plan" } else { "create-pr" };
                conn.execute(
                    "INSERT INTO artifacts
                       (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
                     VALUES(?1,?2,?3,1,?4,?5,?3,0,1,1)",
                    (
                        format!("a-noise-{n}"),
                        format!("j-noise-{}", n % NOISE_JOBS),
                        kind,
                        body.as_str(),
                        (n / NOISE_JOBS) as i64 + 1,
                    ),
                )
                .await?;
            }
            Ok::<_, crate::storage::DbError>(())
        })
    })
    .await
    .unwrap();

    let started = std::time::Instant::now();
    let producing = resolve_producing_job_for_issue(&db, "i-rev").await.unwrap();
    let elapsed = started.elapsed();
    eprintln!(
        "resolve_producing_job_for_issue over {NOISE_ARTIFACTS} noise artifacts: {elapsed:?}"
    );

    assert_eq!(
        producing.as_deref(),
        Some("j-prod"),
        "j-prod owns the issue's only reviewable head; the others' plans are confirmed"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(BOUND_MS),
        "one evaluation took {elapsed:?}: it is reading the workspace's create-pr \
         and plan artifacts, not the issue's"
    );
}

#[tokio::test]
async fn stale_bounded_input_is_not_a_rearm_candidate_after_a_newer_result() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    insert_failed_check(&db, "capacity", 1).await;
    db.execute_script(
        "UPDATE check_result_cache SET ran_at=1;
         INSERT INTO check_result_cache
           (project_id, tree_hash, input_hash, check_name, exit_code, passed,
            output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
         VALUES('p-rev','tree','new-input','review',0,1,'passed',1,1,'j-prod',NULL,0);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    assert_eq!(
        bounded_rearm_candidates(&orch.db).await.unwrap(),
        Vec::new(),
        "rowid breaks equal-second ties, so an older capacity input on the same tree cannot remain latest"
    );
}

#[tokio::test]
async fn bounded_rearm_enumerates_open_team_databases() {
    let local = test_db().await;
    let team = test_db().await;
    seed(&team, "initial").await;
    insert_artifact(&team, "create-pr", 1).await;
    insert_failed_check(&team, "capacity", 1).await;
    let orch = test_orchestrator(local);
    orch.db
        .register_team_db_for_test("team-a".to_string(), Arc::new(team))
        .await;

    let candidates = bounded_rearm_candidates(&orch.db).await.unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].job_id, "j-prod");
}

/// One review-cadence infrastructure failure of `check` at `input`, as the
/// runner records it: signed, and carrying the placement facts it met.
async fn insert_infrastructure_observation(
    db: &LocalDb,
    id: &str,
    check: &str,
    input: &str,
    tree: &str,
    ran_at: i64,
    placement_facts: Option<&str>,
) {
    db.execute(
        "INSERT INTO check_result_observations
           (id, project_id, commit_sha, tree_hash, check_name, input_hash,
            environment_fingerprint, exit_code, verdict, failure_kind, complete, reusable,
            parser_version, result_schema_version, ran_at, duration_ms, job_id, cadence,
            output_tail, runner_build_id, failure_signature, placement_facts)
         VALUES(?1,'p-rev','commit',?2,?3,?4,'',-1,'failed','infrastructure',0,0,
                1,?5,?6,0,'j-prod','review',
                'Cairn could not start this check.
 Failing step:
 conflicting review check executor selector',
                ?7,'sig-identical',?8)",
        (
            id,
            tree,
            check,
            input,
            crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            ran_at,
            cairn_checkenv::implementation_identity(),
            placement_facts,
        ),
    )
    .await
    .unwrap();
}

/// A dormant PR owner `j-prod` whose branch declares one review check, in a
/// managed store backed by the project repository.
struct DormantLane {
    _dirs: Vec<tempfile::TempDir>,
    repo_path: String,
    commit: String,
    tree_hash: String,
    check: crate::config::project_settings::CheckCommand,
}

async fn dormant_lane(
    orch: &Orchestrator,
    bin: &str,
    check_name: &str,
    config: &str,
) -> DormantLane {
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = format!("agent/cairn-5357-{check_name}-0");
    crate::jj::add_workspace(&jj, &store, &workspace, &branch, "main", None).unwrap();
    std::fs::create_dir_all(workspace.join(".cairn")).unwrap();
    std::fs::write(workspace.join(".cairn/config.yaml"), config).unwrap();
    crate::jj::seal(&jj, &workspace, "declare the lane", None).unwrap();
    let commit = crate::jj::head_commit(&jj, &workspace).unwrap();
    let tree_hash = crate::jj::logical_tree_hash(&jj, &store, &commit).unwrap();
    let repo_path = repository.path().to_string_lossy().to_string();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repo_path.as_str(),),
        )
        .await
        .unwrap();
    insert_artifact(&orch.db.local, "create-pr", 1).await;
    attach_planbuild_topology(&orch.db.local, "j-prod", NodeRole::Builder).await;
    orch.db
        .local
        .execute(
            "UPDATE jobs SET branch=?1 WHERE id='j-prod'",
            (branch.as_str(),),
        )
        .await
        .unwrap();
    let check =
        crate::execution::checks::load_checks_contract_at_commit(repository.path(), &commit)
            .await
            .and_then(|loaded| loaded.contract.checks.get(check_name).cloned())
            .expect("the lane is declared at the sealed commit");
    DormantLane {
        _dirs: vec![repository, workspaces],
        repo_path,
        commit,
        tree_hash,
        check,
    }
}

/// Attach `executor_id` on `platform`, advertising the rust toolchain.
fn attach_machine(orch: &Orchestrator, executor_id: &str, platform: &str) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    std::mem::forget(receiver);
    orch.fleet
        .attach_advertised_executor(
            cairn_common::executor_protocol::ExecutorAdvertisement {
                identity: cairn_common::executor_protocol::ExecutorIdentity {
                    device_id: format!("{executor_id}-device"),
                    executor_id: executor_id.into(),
                    display_name: executor_id.into(),
                },
                capabilities: cairn_common::executor_protocol::ExecutorCapabilities {
                    os: platform.into(),
                    arch: "x86_64".into(),
                    logical_cores: 8,
                    concurrency_capacity: None,
                    toolchains: vec!["rust".into()],
                    projects_served: Vec::new(),
                    disk_budget_bytes: None,
                    memory_budget_bytes: None,
                    sandbox: None,
                    toolchain_detection: None,
                    build_prerequisites: None,
                    containment: None,
                    page_call_programs: None,
                    github_credential: None,
                    machine_services: Vec::new(),
                },
                current_load: 0,
                warm_roots: Vec::new(),
                observed_at_unix_ms: chrono::Utc::now().timestamp_millis() as u64,
                liveness_observed_at_unix_ms: None,
                facts_stale_refresh_blocked: None,
                last_refresh: None,
                node_scratches: Vec::new(),
            },
            sender,
            false,
            None,
            None,
        )
        .unwrap();
}

/// A platform the runner's own executor cannot provide.
fn foreign_platform() -> &'static str {
    if std::env::consts::OS == "windows" {
        "linux"
    } else {
        "windows"
    }
}

/// Every dormant tick, `ticks` times over, with the lane's rows moved one
/// cooldown further into the past before each: the time that used to re-arm
/// the wave.
async fn dormant_ticks(orch: &Orchestrator, ticks: usize) -> usize {
    let mut waves = 0;
    for _ in 0..ticks {
        orch.db
            .local
            .execute(
                "UPDATE check_result_cache SET ran_at = ran_at - ?1 WHERE job_id='j-prod'",
                ((DORMANT_INFRA_RETRY_COOLDOWN_SECONDS + 1) * 1000,),
            )
            .await
            .unwrap();
        if rearm_one_bounded_failed_review(orch).await {
            waves += 1;
        }
    }
    waves
}

/// cairn/5357, the arb shape: every lane of a dormant job fails as
/// infrastructure before any machine takes it (a `Dispatch` refusal charged to
/// the head, no environment, no executor). Nothing about the lane changes
/// while the job sleeps, so sixty cooldowns re-dispatch nothing. They used to
/// re-dispatch the whole wave every one of them.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_dormant_lane_that_failed_before_any_machine_took_it_is_never_rearmed_on_a_timer() {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let lane = dormant_lane(
        &orch,
        &bin,
        "cargo-fmt",
        "checks:\n  cargo-fmt:\n    command: cargo fmt --check\n    when: review\n",
    )
    .await;
    insert_failed_check(&orch.db.local, "infrastructure", 1).await;
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1, check_name='cargo-fmt', \
             environment_fingerprint=?2 WHERE job_id='j-prod'",
            (
                lane.tree_hash.as_str(),
                crate::execution::cache::infra_suppression_scope("j-prod", &lane.commit),
            ),
        )
        .await
        .unwrap();
    let facts = review_wave_scopes(&orch, &lane.repo_path, &lane.commit, &lane.check)
        .await
        .placement_facts();
    insert_infrastructure_observation(
        &orch.db.local,
        "obs-arb",
        "cargo-fmt",
        "input",
        &lane.tree_hash,
        1,
        Some(&facts),
    )
    .await;

    assert_eq!(
        dormant_ticks(&orch, 60).await,
        0,
        "sixty cooldowns are not a fact about the lane"
    );
    assert!(!wave_scheduled(&orch, "j-prod"));
}

/// cairn/5357, the 5226 shape: a dormant job whose one red lane fails as
/// infrastructure on every machine the fleet has. Its waves are owed only to a
/// changed fact, so sixty cooldowns re-run nothing; a machine attaching moves
/// the lane's placement facts and earns exactly one wave, and once that wave
/// records its failure under the new facts the job is quiet again.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_dormant_infrastructure_red_rearms_once_per_changed_placement_fact() {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let platform = foreign_platform();
    let lane = dormant_lane(
        &orch,
        &bin,
        "frontend-tests",
        &format!(
            "checks:\n  frontend-tests:\n    command: bun run test:frontend\n    executor:\n      requiredToolchains:\n      - rust\n    verdictPlatforms:\n    - {platform}\n    when: review\n"
        ),
    )
    .await;
    orch.fleet
        .declare_enrolled_remote("fleet-a", "fleet-a", platform, "x86_64");
    insert_failed_check(&orch.db.local, "infrastructure", 1).await;
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1, check_name='frontend-tests', \
             environment_fingerprint=?2 WHERE job_id='j-prod'",
            (
                lane.tree_hash.as_str(),
                crate::execution::cache::infra_suppression_scope("j-prod", &lane.commit),
            ),
        )
        .await
        .unwrap();
    let before = review_wave_scopes(&orch, &lane.repo_path, &lane.commit, &lane.check).await;
    insert_infrastructure_observation(
        &orch.db.local,
        "obs-before",
        "frontend-tests",
        "input",
        &lane.tree_hash,
        1,
        Some(&before.placement_facts()),
    )
    .await;

    assert_eq!(dormant_ticks(&orch, 60).await, 0, "no fact moved");
    assert!(!wave_scheduled(&orch, "j-prod"));

    attach_machine(&orch, "fleet-a", platform);
    let after = review_wave_scopes(&orch, &lane.repo_path, &lane.commit, &lane.check).await;
    assert_ne!(after.placement_facts(), before.placement_facts());
    assert!(
        rearm_one_bounded_failed_review(&orch).await,
        "the machine attaching is the fact that earns the lane a wave"
    );
    assert!(wave_scheduled(&orch, "j-prod"));
    orch.release_turn_end_slot_for_test("j-prod");

    // That wave failed the same way on the machine it met, and says so.
    insert_infrastructure_observation(
        &orch.db.local,
        "obs-after",
        "frontend-tests",
        "input",
        &lane.tree_hash,
        2,
        Some(&after.placement_facts()),
    )
    .await;
    assert_eq!(
        dormant_ticks(&orch, 60).await,
        0,
        "one wave per changed fact, then quiet"
    );
}

/// The breaker's own hold must not read as a fact change. Live waves trip the
/// lane under facts A; a machine leaves (facts B), the lane earns its one wave
/// and fails again under B; the machine returns (A). The newest recorded
/// failure still says B, because a held lane records nothing, so a due rule
/// that compared only against it would re-arm every cooldown for as long as
/// the fleet stayed at A. It asks the breaker under A instead.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_held_lane_stays_quiet_when_its_facts_change_and_change_back() {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let platform = foreign_platform();
    let lane = dormant_lane(
        &orch,
        &bin,
        "frontend-tests",
        &format!(
            "checks:\n  frontend-tests:\n    command: bun run test:frontend\n    executor:\n      requiredToolchains:\n      - rust\n    verdictPlatforms:\n    - {platform}\n    when: review\n"
        ),
    )
    .await;
    orch.fleet
        .declare_enrolled_remote("fleet-a", "fleet-a", platform, "x86_64");
    insert_failed_check(&orch.db.local, "infrastructure", 1).await;
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1, check_name='frontend-tests', \
             environment_fingerprint=?2 WHERE job_id='j-prod'",
            (
                lane.tree_hash.as_str(),
                crate::execution::cache::infra_suppression_scope("j-prod", &lane.commit),
            ),
        )
        .await
        .unwrap();
    let facts_a = review_wave_scopes(&orch, &lane.repo_path, &lane.commit, &lane.check)
        .await
        .placement_facts();
    for (id, ran_at) in [("obs-a1", 1), ("obs-a2", 2), ("obs-a3", 3)] {
        insert_infrastructure_observation(
            &orch.db.local,
            id,
            "frontend-tests",
            "input",
            &lane.tree_hash,
            ran_at,
            Some(&facts_a),
        )
        .await;
    }
    insert_infrastructure_observation(
        &orch.db.local,
        "obs-b",
        "frontend-tests",
        "input",
        &lane.tree_hash,
        4,
        Some("facts-while-a-machine-was-away"),
    )
    .await;

    assert_eq!(
        dormant_ticks(&orch, 60).await,
        0,
        "the breaker holds the lane under the facts the wave would meet now"
    );
    assert!(!wave_scheduled(&orch, "j-prod"));
}

/// A capacity-refused dormant lane waits for room, never for a timer.
#[test]
fn a_capacity_refused_lane_is_due_only_once_room_appears() {
    use super::review_push::capacity_relief_is_due;
    use crate::fleet::occupancy::{MachineOccupancy, OccupancyForecast};
    let forecast = |relief_ms| {
        MachineOccupancy::Predicted(OccupancyForecast {
            relief_ms,
            blocking: "a sibling's rust-tests".to_string(),
            occupant_count: 1,
        })
    };
    assert!(!capacity_relief_is_due(&MachineOccupancy::Unforecastable));
    assert!(!capacity_relief_is_due(&forecast(30_000)));
    assert!(capacity_relief_is_due(&forecast(0)));
    assert!(capacity_relief_is_due(&MachineOccupancy::Idle));
}

/// One review-cadence capacity failure of `check` at `input`, as the runner
/// records it: signed, carrying the placement facts it met and the substrate
/// shape it concluded, beside whatever diagnostic it stored.
#[allow(clippy::too_many_arguments)]
async fn insert_capacity_observation(
    db: &LocalDb,
    id: &str,
    check: &str,
    tree: &str,
    ran_at: i64,
    placement_facts: &str,
    substrate_shape: &str,
    output_tail: &str,
) {
    db.execute(
        "INSERT INTO check_result_observations
           (id, project_id, commit_sha, tree_hash, check_name, input_hash,
            environment_fingerprint, exit_code, verdict, failure_kind, complete, reusable,
            parser_version, result_schema_version, ran_at, duration_ms, job_id, cadence,
            output_tail, runner_build_id, failure_signature, placement_facts, substrate_shape)
         VALUES(?1,'p-rev','commit',?2,?3,'input','',-1,'failed','capacity',0,0,
                1,?4,?5,0,'j-prod','review',?6,?7,'sig-capacity',?8,?9)",
        (
            id,
            tree,
            check,
            crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            ran_at,
            output_tail,
            cairn_checkenv::implementation_identity(),
            placement_facts,
            substrate_shape,
        ),
    )
    .await
    .unwrap();
}

/// cairn/5921. Occupancy counts only Cairn's own cells, so it cannot see an
/// inference model that preempted a lane; a preempted dormant lane must wait
/// on a verdict or moved placement facts, never on the room rule. Which rule a
/// lane gets is read from the shape the runner stored, so rewording the
/// preemption sentence moves nothing: a preemption under a reworded lead
/// still waits, and a plain capacity refusal whose tail carries today's
/// preemption sentence still takes the room rule.
///
/// Main's database holds no preempted observation to copy (none had been
/// recorded when this was written), so the stored tail is composed by the
/// runner's own path for the preemption it reports
/// (`SubstrateFailure::agent_message` over `Preemption::sentence`).
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_preempted_dormant_lane_is_ruled_by_its_stored_shape_not_its_wording() {
    use crate::execution::checks::{SubstrateFailure, SubstrateFailureShape};
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let platform = foreign_platform();
    let lane = dormant_lane(
        &orch,
        &bin,
        "frontend-tests",
        &format!(
            "checks:\n  frontend-tests:\n    command: bun run test:frontend\n    executor:\n      requiredToolchains:\n      - rust\n    verdictPlatforms:\n    - {platform}\n    when: review\n"
        ),
    )
    .await;
    attach_machine(&orch, "fleet-a", platform);
    insert_failed_check(&orch.db.local, "capacity", 1).await;
    orch.db
        .local
        .execute(
            "UPDATE check_result_cache SET tree_hash=?1, check_name='frontend-tests', \
             environment_fingerprint=?2 WHERE job_id='j-prod'",
            (
                lane.tree_hash.as_str(),
                crate::execution::cache::infra_suppression_scope("j-prod", &lane.commit),
            ),
        )
        .await
        .unwrap();
    let facts = review_wave_scopes(&orch, &lane.repo_path, &lane.commit, &lane.check)
        .await
        .placement_facts();
    let recorded = SubstrateFailure::new(
        SubstrateFailureShape::CommandPreempted,
        "Preempted to load DeepSeek-V4-Flash (inference deployment dsv4f-tp2) on bginf-sp1, which needed 100.0 GiB there with 60.0 GiB free while this work held 35.0 GiB, measured.",
    )
    .agent_message();
    let (_, failing_step) = recorded
        .split_once("\nFailing step:\n")
        .expect("a substrate message names its failing step");
    let reworded = format!(
        "An inference model needed this machine's memory, so Cairn ended the check early.\nFailing step:\n{failing_step}"
    );
    assert!(!reworded.contains("to make room for an inference model"));

    // A plain capacity refusal whose stored tail happens to carry today's
    // preemption sentence: the room rule answers it, and the machine is idle.
    insert_capacity_observation(
        &orch.db.local,
        "obs-no-room",
        "frontend-tests",
        &lane.tree_hash,
        1,
        &facts,
        SubstrateFailureShape::Capacity.stored(),
        &recorded,
    )
    .await;
    assert_eq!(
        dormant_ticks(&orch, 1).await,
        1,
        "a capacity refusal re-arms once room appears, whatever its tail says"
    );
    orch.release_turn_end_slot_for_test("j-prod");

    // That wave was preempted, and its lead sentence has since been reworded.
    insert_capacity_observation(
        &orch.db.local,
        "obs-preempted",
        "frontend-tests",
        &lane.tree_hash,
        2,
        &facts,
        SubstrateFailureShape::CommandPreempted.stored(),
        &reworded,
    )
    .await;
    assert_eq!(
        dormant_ticks(&orch, 60).await,
        0,
        "an idle machine is not room for a lane a model preempted"
    );
    assert!(!wave_scheduled(&orch, "j-prod"));

    attach_machine(&orch, "fleet-b", platform);
    assert_ne!(
        review_wave_scopes(&orch, &lane.repo_path, &lane.commit, &lane.check)
            .await
            .placement_facts(),
        facts
    );
    assert!(
        rearm_one_bounded_failed_review(&orch).await,
        "moved placement facts are what earn a preempted lane its wave"
    );
}

/// cairn/5357 comment 1. A restart re-spawns a wave only where one is owed: a
/// wave the restart interrupted. A wave still moving in this process (hung or
/// slow, cairn/4690's case) is not re-spawned, and no counter is reset.
#[tokio::test]
async fn startup_respawns_only_the_waves_a_restart_interrupted_and_resets_nothing() {
    async fn streak(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT infra_failure_streak FROM check_result_cache \
                             WHERE job_id='j-prod'",
                            (),
                        )
                        .await?;
                    rows.next().await?.expect("check result").i64(0)
                })
            })
            .await
            .unwrap()
    }
    for (interrupted, expected) in [(false, 0), (true, 1)] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
        insert_artifact(&db, "create-pr", 1).await;
        insert_failed_check(
            &db,
            "infrastructure",
            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
        )
        .await;
        let orch = test_orchestrator(db);
        let attempt =
            crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod").unwrap();
        crate::execution::checks_turn_end::transition_turn_end_attempt(
            &orch,
            "j-prod",
            &attempt,
            "runtime_started",
            None,
        )
        .unwrap();
        if interrupted {
            crate::execution::checks_turn_end::reconcile_turn_end_attempts_on_startup(&orch).await;
        }

        assert_eq!(
            super::rearm_review_checks_on_startup(&orch).await,
            expected,
            "interrupted={interrupted}: a wave is owed only to an attempt the restart cut off"
        );
        assert_eq!(wave_scheduled(&orch, "j-prod"), interrupted);
        assert_eq!(
            streak(&orch).await,
            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
            "a restart is not a fact about the lane, so its counter stands"
        );
    }
}

async fn insert_failed_check(db: &LocalDb, kind: &str, streak: i64) {
    db.execute_script(format!(
        "INSERT INTO check_result_cache
           (project_id, tree_hash, input_hash, check_name, exit_code, passed,
            output_tail, duration_ms, ran_at, job_id, failure_kind, infra_failure_streak)
         VALUES('p-rev','tree','input','review',-1,0,'failed',0,1,'j-prod','{kind}',{streak});"
    ))
    .await
    .unwrap();
}

fn test_orchestrator(db: LocalDb) -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    OrchestratorBuilder::new(db_state, services, config_dir).build()
}

/// Producing builder node `j-prod` (issue `i-rev` / `cairn://p/prj/7`, exec
/// seq 1) whose just-ended turn carries `start_reason`, a watcher job
/// `j-watch`, and an active issue subscription for BOTH so the producing
/// node's self-exclusion is exercised.
async fn seed(db: &LocalDb, start_reason: &str) {
    db.execute_script(format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p-rev','w','Project','prj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i-rev','p-rev',7,'Rev','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e-rev','r','i-rev','p-rev','running',1,1);
            INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment, node_name, branch, created_at, updated_at)
              VALUES('j-prod','e-rev','p-rev','i-rev','complete','builder','builder','b',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, node_name, created_at, updated_at)
              VALUES('j-watch','p-rev','i-rev','running','watcher',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('r-prod','p-rev','j-prod','i-rev',1,1);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
              VALUES('t-prod','s-prod','j-prod',1,'complete','{start_reason}',1,1);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-watch','j-watch','issue','{ISSUE_URI}','active','agent',1,1,0);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-prod','j-prod','issue','{ISSUE_URI}','active','agent',1,1,0);
            "
        ))
        .await
        .unwrap();
}

enum NodeRole {
    Builder,
    Planner,
    Review,
}

/// Give execution `e-rev` the bundled PlanBuild snapshot and bind `job_id` to
/// one of its nodes. Only the turn-end-cadence tests need it: a live DAG makes
/// the issue non-quiescent (its other nodes have no jobs in this fixture), which
/// is the correct answer for a real execution but not what the review-push
/// tests around it are modelling.
async fn attach_planbuild_topology(db: &LocalDb, job_id: &str, role: NodeRole) {
    let recipe = crate::models::RecipeFile::from_yaml(PLANBUILD_YAML)
        .expect("bundled planbuild recipe parses")
        .into_recipe(Some("default".to_string()), None);
    // `into_recipe` reassigns node ids, so nodes are keyed by agent config.
    let node_id = |agent: &str| {
        recipe
            .nodes
            .iter()
            .find(|node| {
                node.agent_config
                    .as_ref()
                    .and_then(|c| c.agent_config_id.as_deref())
                    == Some(agent)
            })
            .unwrap_or_else(|| panic!("planbuild has an agent node for '{agent}'"))
            .id
            .clone()
    };
    let node_id = match role {
        NodeRole::Builder => node_id("build"),
        NodeRole::Planner => node_id("planner"),
        NodeRole::Review => node_id("pr-review"),
    };
    let snapshot = crate::models::ExecutionSnapshot::new(
        crate::models::RecipeSnapshot {
            id: recipe.id.clone(),
            name: recipe.name.clone(),
            description: recipe.description.clone(),
            trigger: recipe.trigger.clone(),
            nodes: recipe.nodes.clone(),
            edges: recipe.edges.clone(),
        },
        std::collections::HashMap::new(),
        std::collections::HashMap::new(),
        crate::models::TriggerContext {
            issue_id: Some("i-rev".to_string()),
            project_id: "p-rev".to_string(),
            trigger_type: crate::models::TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    );
    let snapshot_json = snapshot.to_json().expect("snapshot serializes");
    let job_id = job_id.to_string();
    db.write(move |conn| {
        let snapshot_json = snapshot_json.clone();
        let node_id = node_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = 'e-rev'",
                (snapshot_json.as_str(),),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET recipe_node_id = ?1 WHERE id = ?2",
                (node_id.as_str(), job_id.as_str()),
            )
            .await?;
            Ok::<_, crate::storage::DbError>(())
        })
    })
    .await
    .unwrap();
}

async fn insert_open_pr(db: &LocalDb) {
    db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-rev','j-prod','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
}

async fn insert_artifact(db: &LocalDb, artifact_type: &str, confirmed: i64) {
    db.execute_script(format!(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-rev','j-prod','{artifact_type}',1,'{{}}',1,'{artifact_type}',{confirmed},1,1);"
        ))
        .await
        .unwrap();
}

/// A `plan` chain on `j-prod` with versions `1..=latest`, every version
/// unconfirmed except those in `confirmed`. Version `v` carries
/// `updated_at = v * 10` so a fingerprint names the version it came from.
async fn insert_plan_chain(db: &LocalDb, latest: i64, confirmed: &[i64]) {
    let mut script = String::new();
    for version in 1..=latest {
        let flag = i64::from(confirmed.contains(&version));
        let stamp = version * 10;
        script.push_str(&format!(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-plan-{version}','j-prod','plan',1,'{{}}',{version},'plan',{flag},{stamp},{stamp});\n"
        ));
    }
    db.execute_script(&script).await.unwrap();
}

async fn pending(orch: &Orchestrator, recipient: &str) -> Vec<Push> {
    list_pending(&orch.db.local, recipient).await.unwrap()
}

async fn run_review_push(orch: &Orchestrator) {
    // Both trigger edges now run through the single readiness evaluator; this
    // helper models any semantic or recovery re-evaluation edge.
    evaluate_review_readiness(orch, "i-rev").await;
}

async fn run_pr_open(orch: &Orchestrator) {
    // The PR-open edge now defers to the same readiness evaluator; the source
    // branch is retained only for the caller's signature/logging.
    create_review_push_for_pr_open(orch, "i-rev", "b").await;
}

#[tokio::test]
async fn work_idle_with_unconfirmed_create_pr_artifact_pushes_review() {
    // The create-pr idle: the PR is not open yet, but the unconfirmed
    // artifact is observable -> the second predicate arm fires.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 0).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0].content_ref.contains("/builder/"));
    // The producing node is never a recipient of its own review.
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn work_idle_with_open_pr_pushes_review() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0]
        .content_ref
        .starts_with("cairn://p/prj/7/1/builder"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn settled_after_memory_review_completes_fires_review() {
    // The common build path: the builder's WORK turn produced the PR, then a
    // trailing memory-review turn ran and completed. Once that reflection turn
    // terminalizes the issue is settled, so the review fires normally — there is
    // deliberately no separate memory-review gate that would block it forever
    // (a builder's latest turn is permanently its memory-review turn; CAIRN-2483).
    let db = test_db().await;
    seed(&db, "memory_review").await; // t-prod: memory_review turn, state complete
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn work_idle_with_confirmed_create_pr_artifact_pushes_review() {
    // CAIRN-1999 shape: the create-pr artifact was already confirmed by the
    // artifact lifecycle, but the parent still needs a review push for the
    // child output even if no PR-open edge creates one.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0].content_ref.contains("/builder/"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn work_idle_without_reviewable_output_no_push() {
    // A work turn with neither an open PR nor a create-pr/unconfirmed-plan
    // artifact -> nothing reviewable.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "plan", 1).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

/// A plan that went through several versions and then had its latest confirmed
/// is resolved output: the superseded versions keep `confirmed = 0` by
/// construction, but they are history, not something awaiting review. Every
/// later trigger edge must leave the watcher alone.
#[tokio::test]
async fn a_confirmed_plans_superseded_versions_are_not_reviewable() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_plan_chain(&db, 5, &[5]).await;
    let orch = test_orchestrator(db);

    assert_eq!(
        super::review_push::review_artifact_ref(&orch.db.local, "prj", 7, "j-prod")
            .await
            .unwrap(),
        None,
        "a superseded unconfirmed version must not stand in for the confirmed latest"
    );

    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a later trigger edge mints no review push for a confirmed plan"
    );

    super::rearm_review_checks_on_startup(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "the startup re-arm does not treat a confirmed plan as reviewable output"
    );
}

/// The same chain with its latest version still awaiting confirmation is
/// reviewable, and it is that latest version — not any older one — that the
/// watcher is pointed at and fingerprinted by.
#[tokio::test]
async fn an_unconfirmed_latest_plan_version_is_the_reviewable_ref() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_plan_chain(&db, 5, &[]).await;
    let orch = test_orchestrator(db);

    let (content_ref, fingerprint) =
        super::review_push::review_artifact_ref(&orch.db.local, "prj", 7, "j-prod")
            .await
            .unwrap()
            .expect("an unconfirmed latest plan is reviewable");
    assert_eq!(content_ref, "cairn://p/prj/7/1/builder/plan");
    assert_eq!(fingerprint, "artifact:5:50");

    run_review_push(&orch).await;
    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].content_ref, content_ref);
    assert_eq!(
        latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
            .await
            .unwrap(),
        Some(Some("artifact:5:50".to_string()))
    );
}

/// The cairn/4841 sequence end to end: the watcher is woken for the pending
/// plan, takes delivery, confirms it, and a later edge (the builder's idle) must
/// not re-wake it with "ready for review" for the plan it already confirmed.
#[tokio::test]
async fn confirming_a_reviewed_plan_does_not_rewake_on_the_next_edge() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_plan_chain(&db, 5, &[]).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script(
            "UPDATE artifacts SET confirmed = 1, updated_at = 60 WHERE id = 'a-plan-5';",
        )
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "confirmation resolves the review; it must not read as a changed reviewable state"
    );
}

#[tokio::test]
async fn successive_work_idles_collapse_to_one_undelivered() {
    // Two work-turn idles with the SAME reviewable state and no delivery in
    // between yield one undelivered review row: the first creates it, the
    // second is skipped by the change-trigger (CAIRN-1889) because the
    // undelivered push already carries the same fingerprint.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    run_review_push(&orch).await;

    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn unchanged_fingerprint_skips_review_even_after_delivery() {
    // One review fires (fp=A); after it is delivered, a second work-turn idle
    // with the SAME reviewable state must NOT re-create a review push.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);

    // Deliver the first push: it leaves the supersede partial index but stays
    // in the table for the fingerprint lookup.
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();
    assert!(pending(&orch, "j-watch").await.is_empty());

    // Same diffstat -> skipped, no re-wake.
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "an unchanged reviewable state must not re-create a review push"
    );
}

#[tokio::test]
async fn changed_diffstat_creates_new_review_after_delivery() {
    // New commits change the diffstat -> a fresh review push, even after the
    // first was delivered.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script("UPDATE merge_requests SET additions=10, deletions=2 WHERE id='mr-rev';")
        .await
        .unwrap();
    run_review_push(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn mergeability_only_change_does_not_refire_review() {
    // A mergeability settle touches non-diffstat columns only -> same
    // fingerprint -> no new review push.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a mergeability-only settle must not re-create a review push"
    );
}

// --- CAIRN-1891: the PR-open edge of the review push ---------------------

#[tokio::test]
async fn pr_open_with_quiescent_producer_pushes_one_review() {
    // The producing builder's head turn is complete (quiescent) and the PR is
    // now open -> exactly one review to the watcher, never to the producing
    // node itself. This is the wake the create-pr idle edge cannot fire.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0]
        .content_ref
        .starts_with("cairn://p/prj/7/1/builder"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn pr_open_with_running_producer_does_not_push() {
    // The quiescence gate: a producing node still mid-turn (a `synchronize`
    // landing during active work) does NOT fire a review.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn pr_open_self_suspended_producer_does_not_push() {
    // A producing node self-suspended on its own work (yielded waiting on a
    // dependency/sub-agent) is not quiescent either -> no review.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script(
        "UPDATE turns SET state='yielded', yield_reason='dependency_wait' WHERE id='t-prod';",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn pr_open_resolves_builder_by_branch_not_mr_job() {
    // The live CAIRN-1891 job-identity bug: the merge_request is owned by a
    // separate pr-action node (blocked while the PR is open -> a running turn,
    // never quiescent), while the builder that did the work is a DIFFERENT job
    // on the same branch. Gating on `mr.job_id` would always bail; the gate
    // must resolve and check the builder via `source_branch`.
    let db = test_db().await;
    seed(&db, "initial").await; // builder j-prod: branch 'b', turn complete (quiescent)
                                // The pr-action node owns the merge_request and — reproducing the live
                                // shape — has NO joinable execution (execution_id NULL), so an arm-1 query
                                // that joined through mr.job_id would drop the row and read the open PR as
                                // unreviewable. The builder (j-prod) is the joinable node.
    db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES('j-prnode','p-rev','i-rev','blocked','pr','pr',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-rev','j-prnode','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "must resolve the builder by source_branch and fire, not gate on the blocked pr-node"
    );
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn pr_open_changed_head_sha_refires_even_with_same_diffstat() {
    // Head SHA is the precise change key: two different commits can share a
    // diffstat, so a real new commit must re-review even when +/- is unchanged.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE merge_requests SET head_sha='sha-aaa' WHERE id='mr-rev';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    // New commit, SAME diffstat, different head SHA -> must re-fire.
    orch.db
        .local
        .execute_script("UPDATE merge_requests SET head_sha='sha-bbb' WHERE id='mr-rev';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(
        second.len(),
        1,
        "a changed head SHA must re-create the review even with an unchanged diffstat"
    );
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn running_memory_review_turn_defers_then_fires_on_settle() {
    // A running memory-review turn keeps the issue unsettled (issue_settled's
    // liveness check includes memory-review turns), so no review fires
    // mid-reflection. Once that reflection turn completes the issue settles and
    // the review fires — exactly once (CAIRN-2483).
    let db = test_db().await;
    seed(&db, "memory_review").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a running memory-review turn must defer the review"
    );

    orch.db
        .local
        .execute_script("UPDATE turns SET state='complete' WHERE id='t-prod';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "the review fires once the reflection turn settles"
    );
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn pr_open_same_diffstat_is_deduped() {
    // A mergeability-only settle re-delivers the open PR with an unchanged
    // diffstat -> the fingerprint matches the delivered push, so no re-wake.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();
    assert!(pending(&orch, "j-watch").await.is_empty());

    orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
    run_pr_open(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a mergeability-only settle must not re-create a review push"
    );
}

#[tokio::test]
async fn pr_open_changed_diffstat_creates_new_review() {
    // New commits change the diffstat between webhook deliveries -> a fresh
    // review push, even after the first was delivered.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script("UPDATE merge_requests SET additions=20, deletions=4 WHERE id='mr-rev';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn pr_open_and_node_idle_share_one_creator() {
    // Both edges run the same row creator: the PR-open edge creates the
    // review, and a subsequent node-idle edge against the unchanged diffstat
    // is deduped by the same fingerprint logic to the one undelivered row.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let after_pr_open = pending(&orch, "j-watch").await;
    assert_eq!(after_pr_open.len(), 1);
    let row_id = after_pr_open[0].id.clone();

    run_review_push(&orch).await;
    let after_idle = pending(&orch, "j-watch").await;
    assert_eq!(after_idle.len(), 1);
    assert_eq!(
        after_idle[0].id, row_id,
        "both edges share one push row keyed review:{{issue}}"
    );
}

#[tokio::test]
async fn pr_open_after_idle_artifact_push_supersedes_to_pr_fingerprint() {
    // The CAIRN-2410 incident, reconstructed. A builder's `create-pr` artifact
    // auto-confirms on write (CAIRN-1219); the coordinator's node-idle edge
    // pushes a review with an `artifact:` fingerprint while no merge_requests
    // row exists yet. The PR opens ~42ms later. Before the fix the PR-open edge
    // never ran on the first-class PR-node path, so this second edge never
    // re-fired and the wake was lost. Now the idle-edge artifact push, then the
    // PR opening, then the PR-open edge supersedes the SAME review:{issue} row to
    // a `pr:` fingerprint on a rousing push that wakes the idle watcher.
    let db = test_db().await;
    seed(&db, "initial").await;
    // The reviewable artifact is a CONFIRMED create-pr (the auto-confirm the
    // incident hinges on), and there is no merge_requests row yet.
    insert_artifact(&db, "create-pr", 1).await;
    let orch = test_orchestrator(db);

    // Idle edge fires first: one review push fingerprinted on the artifact.
    run_review_push(&orch).await;
    let after_idle = pending(&orch, "j-watch").await;
    assert_eq!(after_idle.len(), 1);
    let idle_row = after_idle[0].id.clone();
    let idle_fp = latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
        .await
        .unwrap()
        .flatten()
        .unwrap();
    assert!(
        idle_fp.starts_with("artifact:"),
        "idle edge fingerprints on the artifact, got {idle_fp}"
    );

    // The PR opens: the merge_requests row lands (the 42ms-late seed).
    insert_open_pr(&orch.db.local).await;

    // The PR-open edge re-evaluates the same review key. The reviewable ref is
    // now the open PR, so the fingerprint changes to `pr:` and the row is
    // superseded in place — still exactly one undelivered review.
    run_pr_open(&orch).await;
    let after_open = pending(&orch, "j-watch").await;
    assert_eq!(
        after_open.len(),
        1,
        "supersede-by-key collapses the idle and PR-open pushes to one undelivered row"
    );
    assert_eq!(
        after_open[0].id, idle_row,
        "the PR-open push supersedes the idle push in place (same review:{{issue}} key)"
    );
    assert_eq!(
        after_open[0].wake,
        Wake::Wake,
        "the superseding review is rousing, so an idle watcher is woken"
    );
    let open_fp = latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
        .await
        .unwrap()
        .flatten()
        .unwrap();
    assert!(
        open_fp.starts_with("pr:"),
        "the PR-open edge fingerprints on the open PR, got {open_fp}"
    );
}

// --- CAIRN-2483: the issue-quiescence gate -------------------------------

#[tokio::test]
async fn review_check_outcome_does_not_gate_the_review() {
    // Check outcomes are child feedback. A settled child with reviewable output
    // wakes its parent without a second, check-specific readiness state.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    evaluate_review_readiness(&orch, "i-rev").await;
    assert_eq!(
        pending(&orch, "j-watch").await.len(),
        1,
        "review readiness depends on semantic child state, not check outcome"
    );
}

#[tokio::test]
async fn in_flight_turn_end_checks_do_not_defer_the_review() {
    // Detached advisory checks may outlive the child work. They must not suppress
    // the coordinator's only durable wake once the issue itself is quiescent.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    let slot = orch
        .try_begin_turn_end_checks("j-prod", "a-running")
        .expect("the slot is free");
    run_review_push(&orch).await;
    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "an in-flight turn-end suite must not hold the review"
    );
    assert_eq!(watcher[0].wake, Wake::Wake);
    assert!(pending(&orch, "j-prod").await.is_empty());

    drop(slot);
    run_review_push(&orch).await;
    assert_eq!(
        pending(&orch, "j-watch").await.len(),
        1,
        "check completion re-evaluates idempotently without duplicating the wake"
    );
}

#[tokio::test]
async fn transient_action_run_defers_then_blocked_fires() {
    // The pr action opening the PR (a pending/running action_run) keeps the issue
    // unsettled until it terminalizes or blocks (the open-PR human gate).
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    orch.db
        .local
        .execute_script(
            "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at)
             VALUES('ar-pr','e-rev','pr','builtin:pr','i-rev','p-rev','running',1);",
        )
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a transient pr action_run must hold the review"
    );

    // The action blocks (PR open, human gate) -> settled -> the review fires.
    orch.db
        .local
        .execute_script("UPDATE action_runs SET status='blocked' WHERE id='ar-pr';")
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn render_push_resolved_inlines_referent_content() {
    // CAIRN-1891 Deliverable 2: a drained push renders its referent content
    // inline, not just the URI. The header carries the wake level + the
    // content_ref URI; a resolved body is appended beneath it.
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let push = Push {
        id: "p-render".into(),
        recipient: "j-watch".into(),
        content_ref: ISSUE_URI.into(),
        wake: Wake::Wake,
        boundary: Boundary::Event,
        key: REVIEW_KEY.into(),
        created_at: 1,
        delivered_event_id: None,
    };
    let rendered =
        crate::orchestrator::attention_delivery::render_push_resolved(&orch, &push).await;

    let header = format!("Attention update (wake): {ISSUE_URI}");
    assert!(
        rendered.starts_with(&header),
        "header must carry the wake level + content_ref URI: {rendered}"
    );
    assert!(
        rendered.len() > header.len(),
        "expected resolved referent content inlined beneath the URI header: {rendered}"
    );
}

#[tokio::test]
async fn turn_end_cancel_resolves_immediately_when_already_cancelled() {
    use crate::orchestrator::TurnEndCancel;
    let cancel = TurnEndCancel::default();
    assert!(!cancel.is_cancelled());
    cancel.cancel(crate::orchestrator::TurnEndCancelReason::IssueResolved);
    assert!(cancel.is_cancelled());
    let reason = tokio::time::timeout(std::time::Duration::from_secs(1), cancel.cancelled())
        .await
        .expect("an already-cancelled token resolves without blocking");
    assert_eq!(
        reason,
        crate::orchestrator::TurnEndCancelReason::IssueResolved
    );
}

#[tokio::test]
async fn turn_end_cancel_wakes_a_parked_waiter() {
    use crate::orchestrator::TurnEndCancel;
    let cancel = TurnEndCancel::default();
    let waiter = {
        let cancel = cancel.clone();
        tokio::spawn(async move { cancel.cancelled().await })
    };
    // Let the waiter park on `notified()` before signalling.
    tokio::task::yield_now().await;
    cancel.cancel(crate::orchestrator::TurnEndCancelReason::BranchAdvanced);
    let reason = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("cancel wakes the parked waiter")
        .expect("the waiter task joins cleanly");
    assert_eq!(
        reason,
        crate::orchestrator::TurnEndCancelReason::BranchAdvanced
    );
}

#[tokio::test]
async fn cancel_turn_end_checks_signals_the_in_flight_token() {
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let slot = orch
        .try_begin_turn_end_checks("j-prod", "a-first")
        .expect("first claim wins the single-flight slot");
    assert!(
        orch.try_begin_turn_end_checks("j-prod", "a-second")
            .is_err(),
        "a second claim while one is in flight is refused"
    );
    assert!(!slot.is_cancelled());

    orch.cancel_turn_end_checks(
        "j-prod",
        crate::orchestrator::TurnEndCancelReason::IssueResolved,
    );
    assert!(
        slot.is_cancelled(),
        "the cancel lever signals the in-flight suite's token"
    );
    assert_eq!(
        slot.cancel().cancelled().await,
        crate::orchestrator::TurnEndCancelReason::IssueResolved,
        "the cause travels with the cancellation signal"
    );
    orch.cancel_turn_end_checks(
        "j-prod",
        crate::orchestrator::TurnEndCancelReason::BranchAdvanced,
    );
    assert_eq!(
        slot.cancel().cancelled().await,
        crate::orchestrator::TurnEndCancelReason::IssueResolved,
        "a later producer cannot overwrite the cause that cancelled the wave"
    );

    drop(slot);
    // After release the slot is free again and a stale cancel is a no-op.
    orch.cancel_turn_end_checks(
        "j-prod",
        crate::orchestrator::TurnEndCancelReason::BranchAdvanced,
    );
    assert!(orch.try_begin_turn_end_checks("j-prod", "a-third").is_ok());
}

#[tokio::test]
async fn branch_advance_cancels_the_in_flight_review_suite() {
    // A commit sealing mid-turn advances the branch; the branch-advance hook
    // cancels the job's in-flight when:review suite so its heavy compiles stop
    // starving the builder's own when:write checks. Idempotent afterward.
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let slot = orch
        .try_begin_turn_end_checks("j-prod", "a-advanced")
        .expect("claim the job's single-flight slot");
    assert!(!slot.is_cancelled());

    crate::execution::checks::cancel_stale_review_on_branch_advance(
        &orch,
        "j-prod",
        crate::execution::checks::BranchAdvance::Commit,
    )
    .await;
    assert!(
        slot.is_cancelled(),
        "a sealed commit cancels the in-flight review suite for the job"
    );

    // No job row backs `j-prod` here, so the successor the release arms
    // withdraws itself without claiming the slot.
    drop(slot);
    // No suite in flight ⇒ the branch-advance cancel is a harmless no-op, and the
    // single-flight slot remains claimable.
    crate::execution::checks::cancel_stale_review_on_branch_advance(
        &orch,
        "j-prod",
        crate::execution::checks::BranchAdvance::Commit,
    )
    .await;
    assert!(orch.try_begin_turn_end_checks("j-prod", "a-after").is_ok());
}

/// A base advance that replays the branch without changing its own patch leaves
/// the running wave alone, however many land while it runs: its verdict is still
/// the verdict about the change the branch claims, and withdrawing it on every
/// merge is how cairn/4210 lost four waves in fifteen minutes and recorded
/// nothing. Nothing is re-armed when it ends, because nothing was superseded. A
/// commit, or a replay that altered the patch, still withdraws it.
#[tokio::test]
async fn a_base_replay_that_keeps_the_patch_keeps_the_wave_and_a_commit_withdraws_it() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);
    let kept = crate::execution::checks::BranchAdvance::BaseReplay {
        patch_unchanged: true,
    };

    let wave = orch
        .try_begin_turn_end_checks("j-prod", "a-kept")
        .expect("claim the wave's single-flight slot");
    for _ in 0..4 {
        crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod", kept)
            .await;
    }
    assert!(
        !wave.is_cancelled(),
        "four patch-preserving base advances withdraw nothing"
    );
    assert!(orch.turn_end_checks_in_flight("j-prod"));
    drop(wave);
    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "a wave that finished on its own head arms no successor"
    );

    for advance in [
        crate::execution::checks::BranchAdvance::Commit,
        crate::execution::checks::BranchAdvance::BaseReplay {
            patch_unchanged: false,
        },
    ] {
        // A fresh node per advance: the successor each withdrawal arms holds
        // the slot for the rest of its test.
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
        let orch = test_orchestrator(db);
        let wave = orch
            .try_begin_turn_end_checks("j-prod", "a-withdrawn")
            .expect("the slot is free");
        crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod", advance)
            .await;
        assert!(
            wave.is_cancelled(),
            "{advance:?} changes the patch and withdraws the wave"
        );
        drop(wave);
        assert!(
            wave_scheduled(&orch, "j-prod"),
            "{advance:?} arms one successor for the tree that replaced the stale one"
        );
    }
}

/// Two jobs on one branch and one job on another, so a branch-advance can be
/// asked to distinguish them — plus a second project carrying a job on a branch
/// of the SAME name, which only project scoping can tell apart.
async fn seed_branch_sharers(db: &LocalDb) {
    db.execute_script(
        "
        INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('p-rev','w','Project','prj','/tmp/repo',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('i-rev','p-rev',7,'Rev','active','active','none',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-node','p-rev','i-rev','running','builder','agent/shared',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-task','p-rev','i-rev','running','task','agent/shared',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-other','p-rev','i-rev','running','sibling','agent/other',1,1);

        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('p-far','w','Far','FAR','/tmp/far',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('i-far','p-far',7,'Far','active','active','none',1,1);
        INSERT INTO jobs(id, project_id, issue_id, status, node_name, branch, created_at, updated_at)
          VALUES('j-far','p-far','i-far','running','builder','agent/shared',1,1);
        ",
    )
    .await
    .unwrap();
}

/// A task commits into the branch it inherited from the node that owns it. The
/// node's in-flight review wave is now keyed to a tree that no longer exists, so
/// it must be cancelled even though the commit arrived under a different job id
/// — the gap that let a full exclusive lane keep burning against a superseded
/// tree and then throw its verdicts away.
#[tokio::test]
async fn a_commit_supersedes_every_review_suite_on_the_same_branch() {
    let db = test_db().await;
    seed_branch_sharers(&db).await;
    let orch = test_orchestrator(db);

    let node = orch.try_begin_turn_end_checks("j-node", "a-node").unwrap();
    let other = orch
        .try_begin_turn_end_checks("j-other", "a-other")
        .unwrap();

    crate::execution::checks::cancel_stale_review_on_branch_advance(
        &orch,
        "j-task",
        crate::execution::checks::BranchAdvance::Commit,
    )
    .await;

    assert!(
        node.is_cancelled(),
        "a task's commit supersedes the wave of the node whose branch it shares"
    );
    assert!(
        !other.is_cancelled(),
        "a job on a different branch keeps its wave: its inputs did not change"
    );
}

/// A branch name identifies a tree only inside one repository. Names like `main`
/// recur across projects and generated names are unique only per project, so
/// matching a branch by name alone would let a commit in one project destroy
/// live review waves in an unrelated one whose tree it never touched.
#[tokio::test]
async fn a_commit_spares_a_same_named_branch_in_another_project() {
    let db = test_db().await;
    seed_branch_sharers(&db).await;
    let orch = test_orchestrator(db);

    let near = orch.try_begin_turn_end_checks("j-node", "a-near").unwrap();
    let far = orch.try_begin_turn_end_checks("j-far", "a-far").unwrap();

    crate::execution::checks::cancel_stale_review_on_branch_advance(
        &orch,
        "j-task",
        crate::execution::checks::BranchAdvance::Commit,
    )
    .await;

    assert!(
        near.is_cancelled(),
        "the branch's own project still supersedes as before"
    );
    assert!(
        !far.is_cancelled(),
        "another project's identically named branch is a different tree"
    );
}

/// The committing job's own suite is cancelled even when its branch is unknown,
/// so an unreadable or branchless job row degrades to the previous behavior
/// rather than to no cancellation at all.
#[tokio::test]
async fn a_branchless_job_still_cancels_its_own_suite() {
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let own = orch
        .try_begin_turn_end_checks("j-unknown", "a-own")
        .unwrap();
    crate::execution::checks::cancel_stale_review_on_branch_advance(
        &orch,
        "j-unknown",
        crate::execution::checks::BranchAdvance::Commit,
    )
    .await;
    assert!(own.is_cancelled());
}

// --- CAIRN-3154: only a turn that really ended earns a review wave -------

/// Replace `j-prod`'s single turn with one in `state`, yielded for `reason`
/// when given, so the turn-end hook reads one deterministic head turn.
async fn set_prod_head_turn(db: &LocalDb, state: &str, reason: Option<&str>) {
    let reason = reason
        .map(|r| format!("'{r}'"))
        .unwrap_or_else(|| "NULL".to_string());
    db.execute_script(format!(
        "DELETE FROM turns WHERE job_id='j-prod';
         INSERT INTO turns(id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
           VALUES('t-prod','s-prod','j-prod',1,'{state}',{reason},'initial',1,1);"
    ))
    .await
    .unwrap();
}

/// Did the turn-end hook schedule a review wave for `job_id`? The hook claims
/// the job's single-flight slot before detaching the suite, so the slot is the
/// observable.
///
/// Deterministic: the suite is spawned onto the orchestrator's runtime, which in
/// a test is the test's own current-thread runtime, so the detached suite — the
/// only party that releases the slot — cannot poll until the test next awaits.
fn wave_scheduled(orch: &Orchestrator, job_id: &str) -> bool {
    orch.turn_end_checks_in_flight(job_id)
}

/// The reported defect: the heavy `when:review` suites fired at the turn end of
/// EVERY job, so a builder that delegated work multiplied minutes of exclusive
/// machine time by its sub-agent turn count, all against intermediate trees
/// nobody reviews. A delegated task is materialized into the DAG with no node
/// of its own to ship from, so its turn end now schedules nothing (CAIRN-3334).
#[tokio::test]
async fn a_delegated_sub_agents_turn_end_schedules_no_review_wave() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    db.execute_script(
        "INSERT INTO jobs(id, execution_id, parent_job_id, project_id, issue_id, status,
                          uri_segment, node_name, branch, task_index, created_at, updated_at)
           VALUES('j-task','e-rev','j-prod','p-rev','i-rev','complete','explore','Explore','b',0,1,1);
         INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
           VALUES('t-task','s-task','j-task',1,'complete','initial',1,1);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    spawn_turn_end_checks(&orch, "j-task");
    assert!(
        !wave_scheduled(&orch, "j-task"),
        "a sub-agent task ships no PR, so its turn end runs no review cadence"
    );

    // The builder it belongs to is unaffected: its branch is the one that ships.
    spawn_turn_end_checks(&orch, "j-prod");
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the builder whose branch ships the PR still runs its review cadence"
    );
}

/// Scoping is by recipe topology, not by agent name: within the SAME recipe the
/// planner (whose output terminates in a plan artifact) and the review node
/// (which has no context-out at all) run nothing, while the builder does.
#[tokio::test]
async fn only_the_node_whose_branch_ships_runs_the_review_cadence() {
    for (role, expected) in [
        (NodeRole::Builder, true),
        (NodeRole::Planner, false),
        (NodeRole::Review, false),
    ] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", role).await;
        let orch = test_orchestrator(db);

        spawn_turn_end_checks(&orch, "j-prod");
        assert_eq!(
            wave_scheduled(&orch, "j-prod"),
            expected,
            "a node's review cadence follows its recipe topology"
        );
    }
}

#[tokio::test]
async fn a_turn_ended_by_self_suspension_schedules_no_review_wave() {
    // The agent kicked off its own tests and self-suspended waiting on them.
    // That yield reaches the turn-end hook looking like an idle, but the job is
    // mid-work: launching the heavy review lanes here would put them in
    // contention with the very tests being waited on.
    let db = test_db().await;
    seed(&db, "initial").await;
    // A builder that would otherwise qualify, so the mid-work guard is what is
    // under test here rather than the topology gate ahead of it.
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    for reason in ["wait", "dependency_wait", "user_input", "permission"] {
        set_prod_head_turn(&orch.db.local, "yielded", Some(reason)).await;

        spawn_turn_end_checks(&orch, "j-prod");

        assert!(
            !wave_scheduled(&orch, "j-prod"),
            "a turn yielded on the agent's own {reason} is not a turn end"
        );
    }
}

#[tokio::test]
async fn the_review_wave_lands_once_after_the_resume() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    // Two suspended turn-ends in a row schedule nothing, and leave nothing owed:
    // there is no make-up wave per skipped turn.
    for _ in 0..2 {
        set_prod_head_turn(&orch.db.local, "yielded", Some("wait")).await;
        spawn_turn_end_checks(&orch, "j-prod");
        assert!(!wave_scheduled(&orch, "j-prod"));
    }

    // The wait resolves and the synthetic continuation turn starts. The
    // suspension's late interrupt ack lands on this hook here; it must not
    // schedule a wave against the pre-resume tree, nor take the single-flight
    // slot away from the continuation's own turn-end.
    set_prod_head_turn(&orch.db.local, "running", None).await;
    spawn_turn_end_checks(&orch, "j-prod");
    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "the resume boundary must not double-fire ahead of the real turn-end"
    );

    // The continuation reaches a real turn end: one wave, against the tree that
    // exists now.
    set_prod_head_turn(&orch.db.local, "complete", None).await;
    spawn_turn_end_checks(&orch, "j-prod");
    assert!(
        wave_scheduled(&orch, "j-prod"),
        "the first real turn-end after the resume runs the suite"
    );
}

#[tokio::test]
async fn a_plain_turn_end_still_schedules_its_review_wave() {
    // The unchanged path: `seed` leaves `t-prod` complete, which is what an
    // ordinary turn ending through the warm transition looks like.
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    spawn_turn_end_checks(&orch, "j-prod");

    assert!(
        wave_scheduled(&orch, "j-prod"),
        "a completed head turn is a real turn end and still fires the cadence"
    );
}

/// Startup must re-derive the review wake for a job the check cadence skips
/// (CAIRN-3347). A planner parked at its plan gate is settled, owns reviewable
/// output, and ships no PR — so a re-arm that only re-spawns the cadence restores
/// nothing for it, and its coordinator stays asleep across the restart.
#[tokio::test]
async fn startup_rearm_recovers_the_wake_for_a_gated_job_that_ships_no_pr() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Planner).await;
    db.execute_script(
        "UPDATE jobs SET status='blocked', branch=NULL WHERE id='j-prod';
         INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                               output_name, created_at, updated_at)
           VALUES('a-plan','j-prod','plan',0,'{}',1,'plan',1,1);",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    super::rearm_review_checks_on_startup(&orch).await;

    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "a planner's branch ships no PR, so the re-arm schedules no check wave for it"
    );
    let pending = list_pending(&orch.db.local, "j-watch").await.unwrap();
    let review: Vec<&Push> = pending.iter().filter(|p| p.key == REVIEW_KEY).collect();
    assert_eq!(
        review.len(),
        1,
        "the watcher's wake is re-derived directly, not through the cadence"
    );
    assert_eq!(review[0].wake, Wake::Wake);
    assert!(
        review[0].content_ref.ends_with("/plan"),
        "the watcher is pointed at the plan awaiting confirmation, got {}",
        review[0].content_ref
    );
}

/// Every settled stored status is a re-arm candidate, including `cancelled`
/// (CAIRN-3347). The recompute hook filters derived *transitions* and so can
/// never see an archived job — cancellation is an explicit sticky override the
/// sweep skips — which makes this query the only place archived-but-reviewable
/// work can reach its watcher.
#[tokio::test]
async fn every_settled_status_is_a_rearm_candidate() {
    for status in ["idle", "complete", "failed", "blocked", "cancelled"] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Planner).await;
        db.execute_script(format!(
            "UPDATE jobs SET status='{status}', branch=NULL WHERE id='j-prod';
             INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                                   output_name, created_at, updated_at)
               VALUES('a-plan','j-prod','plan',0,'{{}}',1,'plan',1,1);"
        ))
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        super::rearm_review_checks_on_startup(&orch).await;

        let pending = list_pending(&orch.db.local, "j-watch").await.unwrap();
        assert_eq!(
            pending.iter().filter(|p| p.key == REVIEW_KEY).count(),
            1,
            "a settled '{status}' job owning reviewable output must reach its watcher"
        );
    }
}

/// The re-arm now runs as a detached task after the transport is already serving
/// (CAIRN-3382), which is safe for exactly one reason: it is idempotent across
/// boots. This pins both halves of that claim — a task-spawned re-arm reaches the
/// same outcome the blocking one did, and running it a second time (the next boot
/// after one that was interrupted or never finished) neither duplicates the
/// watcher's wake nor changes what it points at.
#[tokio::test]
async fn a_detached_rearm_converges_and_stays_convergent_across_boots() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Planner).await;
    db.execute_script(
        "UPDATE jobs SET status='blocked', branch=NULL WHERE id='j-prod';
         INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                               output_name, created_at, updated_at)
           VALUES('a-plan','j-prod','plan',0,'{}',1,'plan',1,1);",
    )
    .await
    .unwrap();
    let orch = std::sync::Arc::new(test_orchestrator(db));

    async fn review_wakes(orch: &Orchestrator) -> Vec<String> {
        list_pending(&orch.db.local, "j-watch")
            .await
            .unwrap()
            .into_iter()
            .filter(|push| push.key == REVIEW_KEY)
            .map(|push| push.content_ref)
            .collect()
    }

    // Boot one, spawned rather than awaited inline — what the runtime now does.
    let first_waves = {
        let orch = orch.clone();
        tokio::spawn(async move { super::rearm_review_checks_on_startup(&orch).await })
    }
    .await
    .expect("the background re-arm task runs to completion");
    assert_eq!(
        first_waves, 0,
        "a settled job with no interrupted wave and no dormant lane due owes no wave"
    );
    let after_first = review_wakes(&orch).await;
    assert_eq!(
        after_first.len(),
        1,
        "a detached re-arm must reach the watcher exactly as a blocking one did"
    );

    // Boot two, over state boot one already re-armed.
    let second_waves = super::rearm_review_checks_on_startup(&orch).await;
    assert_eq!(
        second_waves, first_waves,
        "the candidate set is a function of stored state, not of how many boots ran"
    );
    assert_eq!(
        review_wakes(&orch).await,
        after_first,
        "re-running the re-arm must not duplicate or move the watcher's wake"
    );
}

/// The launchability guard, at the moment it is cheapest to honour: a wave for
/// a resolved issue or a cancelled job is never armed at all, so it claims no
/// single-flight slot and spends no minutes planning against a tree nobody will
/// review (CAIRN-3345).
#[tokio::test]
async fn a_wave_is_never_armed_for_a_resolved_issue_or_a_cancelled_job() {
    for (script, why) in [
        (
            "UPDATE issues SET status='merged' WHERE id='i-rev';",
            "a merged issue has no tree left to review",
        ),
        (
            "UPDATE issues SET status='closed' WHERE id='i-rev';",
            "a closed issue has no tree left to review",
        ),
        (
            "UPDATE jobs SET status='cancelled' WHERE id='j-prod';",
            "a cancelled job's work was withdrawn",
        ),
    ] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
        db.execute_script(script).await.unwrap();
        let orch = test_orchestrator(db);

        spawn_turn_end_checks(&orch, "j-prod");
        assert!(!wave_scheduled(&orch, "j-prod"), "{why}");
    }
}

/// The negative half, so the guard cannot pass by refusing everything: an active
/// issue with a live job still arms its wave.
#[tokio::test]
async fn a_live_issue_still_arms_its_wave() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);

    spawn_turn_end_checks(&orch, "j-prod");
    assert!(wave_scheduled(&orch, "j-prod"));
}

#[tokio::test]
async fn resolving_an_issue_cancels_its_jobs_turn_end_checks() {
    // The issue-scoped lever the merge/close path pulls: every job of the issue
    // with an in-flight suite is signalled to quit (CAIRN-2648).
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let slot = orch
        .try_begin_turn_end_checks("j-prod", "a-resolving")
        .expect("claim the builder job's slot");
    assert!(!slot.is_cancelled());

    crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(
        &orch,
        &orch.db.local,
        "i-rev",
    )
    .await;

    assert!(
        slot.is_cancelled(),
        "resolving issue i-rev quits its builder job's in-flight suite"
    );
    drop(slot);
    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "a wave cancelled because its issue resolved owes no successor"
    );
}

#[tokio::test]
async fn repeated_turn_ends_preserve_attempt_history_and_current_ordering() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let owner = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist owner attempt");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch, "j-prod", &owner, "claimed", None,
    )
    .expect("claim owner");
    let successor = crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod")
        .expect("persist successor attempt");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch,
        "j-prod",
        &successor,
        "superseded",
        Some("owner remains active"),
    )
    .expect("terminalize successor");

    let rows = orch.db.local.read(|conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT id,state FROM turn_end_check_attempts WHERE job_id='j-prod' ORDER BY created_at,id",
            (),
        ).await?;
        let mut found=Vec::new();
        while let Some(row)=rows.next().await? { found.push((row.text(0)?,row.text(1)?)); }
        Ok::<_,crate::storage::DbError>(found)
    })).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(owner.clone(), "claimed".to_string())));
    assert!(rows.contains(&(successor.clone(), "superseded".to_string())));
    let current =
        crate::execution::checks_turn_end::current_turn_end_attempt(&orch.db.local, "j-prod")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(current.id, owner);
    assert_eq!(current.state, "claimed");
}

#[tokio::test]
async fn startup_reconciliation_terminalizes_every_moving_attempt_with_named_reason() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    for state in ["requested", "claimed", "runtime_started", "submitted"] {
        let id =
            crate::execution::checks_turn_end::request_turn_end_attempt(&orch, "j-prod").unwrap();
        crate::execution::checks_turn_end::transition_turn_end_attempt(
            &orch, "j-prod", &id, state, None,
        )
        .unwrap();
    }

    crate::execution::checks_turn_end::reconcile_turn_end_attempts_on_startup(&orch).await;

    let rows = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT state,reason FROM turn_end_check_attempts WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    found.push((row.text(0)?, row.text(1)?));
                }
                Ok::<_, crate::storage::DbError>(found)
            })
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    assert!(rows
        .iter()
        .all(|(state, reason)| state == "failed" && reason.contains("host restarted")));
}

// --- cairn/4690: the turn-end slot cannot leak, and a refused turn end is owed ---

/// Every attempt row for `job_id` as `(id, state, reason)`, oldest first.
async fn attempts(db: &LocalDb, job_id: &str) -> Vec<(String, String, Option<String>)> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, state, reason FROM turn_end_check_attempts
                      WHERE job_id = ?1 ORDER BY created_at, rowid",
                    (job_id.as_str(),),
                )
                .await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                found.push((row.text(0)?, row.text(1)?, row.opt_text(2)?));
            }
            Ok::<_, crate::storage::DbError>(found)
        })
    })
    .await
    .unwrap()
}

/// A second builder on `e-rev` whose last turn completed, so a turn end arms
/// its review wave. Fault-injection tests use one of these: the fault seam is
/// keyed by job id, and no other test uses these ids.
async fn insert_shipping_builder(db: &LocalDb, job_id: &str) {
    db.execute_script(format!(
        "INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment, node_name, branch, created_at, updated_at)
           VALUES('{job_id}','e-rev','p-rev','i-rev','complete','{job_id}','builder','b',1,1);
         INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
           VALUES('t-{job_id}','s-{job_id}','{job_id}',1,'complete','initial',1,1);"
    ))
    .await
    .unwrap();
    attach_planbuild_topology(db, job_id, NodeRole::Builder).await;
}

/// A claimed attempt and the slot it holds, as the turn-end hook leaves them.
fn claimed_slot(orch: &Orchestrator, job_id: &str) -> (String, crate::orchestrator::TurnEndSlot) {
    let id = crate::execution::checks_turn_end::request_turn_end_attempt(orch, job_id).unwrap();
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        orch, job_id, &id, "claimed", None,
    )
    .unwrap();
    let slot = orch
        .try_begin_turn_end_checks(job_id, &id)
        .expect("the slot is free");
    (id, slot)
}

/// The immutable facts a wave that sealed `head` publishes.
fn plan_at(head: &str) -> Arc<crate::execution::checks_turn_end::WavePlan> {
    Arc::new(crate::execution::checks_turn_end::WavePlan {
        head: head.to_string(),
        base: String::new(),
        tree_hash: String::new(),
        checks: Default::default(),
        applicable_names: Default::default(),
        input_hashes: Default::default(),
        expected_toolchain: String::new(),
        unsupported: Default::default(),
    })
}

/// A failed `claimed` write used to return while holding the slot, so every
/// later turn end for the job was `superseded` until the host restarted.
#[tokio::test]
async fn a_claim_write_failure_leaves_the_slot_free_and_the_attempt_failed() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_shipping_builder(&db, "j-claim-fault").await;
    let orch = test_orchestrator(db);

    crate::execution::checks_turn_end::transition_faults::fail("j-claim-fault", "claimed", 1);
    spawn_turn_end_checks(&orch, "j-claim-fault");

    assert!(
        !orch.turn_end_checks_in_flight("j-claim-fault"),
        "a claim that could not be recorded frees the slot"
    );
    let rows = attempts(&orch.db.local, "j-claim-fault").await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1, "failed", "{rows:?}");
    assert_eq!(
        rows[0].2.as_deref(),
        Some(crate::orchestrator::TURN_END_ABANDONED_REASON)
    );

    spawn_turn_end_checks(&orch, "j-claim-fault");
    assert!(
        wave_scheduled(&orch, "j-claim-fault"),
        "the next turn end claims the slot"
    );
}

/// A panic in the detached wave never reached the release, which left the slot
/// held by nothing.
#[tokio::test]
async fn a_panicking_wave_leaves_the_slot_free_and_the_attempt_failed() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let (id, slot) = claimed_slot(&orch, "j-prod");

    let wave = tokio::spawn(async move {
        let _slot = slot;
        panic!("the wave panicked mid-flight");
    })
    .await;

    assert!(wave.is_err(), "the wave panicked");
    assert!(!orch.turn_end_checks_in_flight("j-prod"));
    let rows = attempts(&orch.db.local, "j-prod").await;
    let row = rows.iter().find(|row| row.0 == id).unwrap();
    assert_eq!(row.1, "failed");
    assert_eq!(
        row.2.as_deref(),
        Some(crate::orchestrator::TURN_END_ABANDONED_REASON)
    );
}

/// A guard dropped after someone else terminalized its attempt keeps their
/// reason: the spawn-failure path records why the wave never ran, and the
/// guard inside the dropped future must not overwrite it.
#[tokio::test]
async fn an_abandoned_guard_never_overwrites_a_terminal_state() {
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);
    let (id, slot) = claimed_slot(&orch, "j-prod");
    crate::execution::checks_turn_end::transition_turn_end_attempt(
        &orch,
        "j-prod",
        &id,
        "failed",
        Some("detached runtime construction failed"),
    )
    .unwrap();

    drop(slot);

    let rows = attempts(&orch.db.local, "j-prod").await;
    assert_eq!(
        rows[0].2.as_deref(),
        Some("detached runtime construction failed")
    );
    assert!(!orch.turn_end_checks_in_flight("j-prod"));
}

/// A terminal write that keeps failing used to retain the slot. Releasing it
/// behind a non-terminal row is safe; holding it made the job permanently
/// superseded.
#[tokio::test]
async fn a_terminal_write_failure_still_releases_the_slot() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_shipping_builder(&db, "j-terminal-fault").await;
    let orch = test_orchestrator(db);
    let (id, slot) = claimed_slot(&orch, "j-terminal-fault");

    crate::execution::checks_turn_end::transition_faults::fail("j-terminal-fault", "completed", 3);
    slot.finish("completed", "check wave completed").await;

    assert!(!orch.turn_end_checks_in_flight("j-terminal-fault"));
    let rows = attempts(&orch.db.local, "j-terminal-fault").await;
    let row = rows.iter().find(|row| row.0 == id).unwrap();
    assert_eq!(row.1, "claimed", "the write never landed: {rows:?}");

    spawn_turn_end_checks(&orch, "j-terminal-fault");
    assert!(
        wave_scheduled(&orch, "j-terminal-fault"),
        "the next turn end claims the slot and becomes the current attempt"
    );
}

/// A release is the holder's own: a late one from an older wave never frees the
/// slot a newer wave holds.
#[tokio::test]
async fn a_late_release_never_frees_a_newer_waves_slot() {
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let old = orch.try_begin_turn_end_checks("j-prod", "a-old").unwrap();
    let old_wave = old.cancel().clone();
    drop(old);
    let newer = orch.try_begin_turn_end_checks("j-prod", "a-new").unwrap();

    assert!(!orch.end_turn_end_checks("j-prod", &old_wave));
    assert!(
        orch.turn_end_checks_in_flight("j-prod"),
        "the newer wave still holds its slot"
    );
    drop(newer);
    assert!(!orch.turn_end_checks_in_flight("j-prod"));
}

/// `j-prod` on a real agent branch in a managed store, so its logical head
/// resolves. Returns that head and the directories backing it, or `None` when
/// no jj binary is available.
async fn resolvable_head(orch: &Orchestrator) -> Option<(String, Vec<tempfile::TempDir>)> {
    let bin = crate::jj::tests::jj_bin()?;
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/cairn-4690-builder-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    std::fs::write(workspace.join("owed.rs"), "owed\n").unwrap();
    crate::jj::seal(&jj, &workspace, "agent branch", None).unwrap();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repository.path().to_string_lossy().as_ref(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();
    let head = crate::execution::cache::resolve_job_logical_head(orch, "j-prod")
        .await
        .expect("the agent branch resolves");
    Some((head, vec![repository, workspaces]))
}

/// Two turn ends arriving while a wave runs on an earlier head give exactly one
/// successor, for the head the agent actually ended on.
#[tokio::test]
#[serial_test::serial(jj)]
async fn turn_ends_during_a_wave_on_an_earlier_head_owe_exactly_one_successor() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);
    let Some((head, _dirs)) = resolvable_head(&orch).await else {
        return;
    };
    let (wave, slot) = claimed_slot(&orch, "j-prod");
    let earlier = "0".repeat(head.len());
    slot.cancel().set_runtime_status(&wave, plan_at(&earlier));

    spawn_turn_end_checks(&orch, "j-prod");
    spawn_turn_end_checks(&orch, "j-prod");
    let refused = attempts(&orch.db.local, "j-prod").await;
    assert_eq!(refused.len(), 3, "{refused:?}");
    assert!(
        refused[1..].iter().all(|row| row.1 == "superseded"
            && row.2.as_deref() == Some(super::review_push::OWED_SUCCESSOR_REASON)),
        "each refused turn end names the debt: {refused:?}"
    );

    slot.finish("completed", "check wave completed").await;

    let rows = attempts(&orch.db.local, "j-prod").await;
    assert_eq!(
        rows.len(),
        4,
        "one successor, not one per turn end: {rows:?}"
    );
    assert_ne!(rows[3].1, "superseded", "{rows:?}");
}

/// A turn end that left the head where the running wave sealed it was answered
/// by that wave, and owes nothing.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_turn_end_on_the_head_the_wave_checked_owes_no_successor() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);
    let Some((head, _dirs)) = resolvable_head(&orch).await else {
        return;
    };
    let (wave, slot) = claimed_slot(&orch, "j-prod");
    slot.cancel().set_runtime_status(&wave, plan_at(&head));

    spawn_turn_end_checks(&orch, "j-prod");
    slot.finish("completed", "check wave completed").await;

    let rows = attempts(&orch.db.local, "j-prod").await;
    assert_eq!(rows.len(), 2, "no successor: {rows:?}");
    assert!(!orch.turn_end_checks_in_flight("j-prod"));
}

/// The owed successor goes through every ordinary gate: a live turn arms its
/// own wave when it ends, and a resolved issue arms nothing.
#[tokio::test]
async fn an_owed_successor_is_suppressed_by_a_live_turn_or_a_resolved_issue() {
    for (gate, expected) in [("live turn", "skipped"), ("resolved issue", "withdrawn")] {
        let db = test_db().await;
        seed(&db, "initial").await;
        attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
        let orch = test_orchestrator(db);
        let (wave, slot) = claimed_slot(&orch, "j-prod");
        slot.cancel().set_runtime_status(&wave, plan_at("earlier"));
        spawn_turn_end_checks(&orch, "j-prod");

        match gate {
            "live turn" => set_prod_head_turn(&orch.db.local, "running", None).await,
            _ => orch
                .db
                .local
                .execute("UPDATE issues SET status='merged' WHERE id='i-rev'", ())
                .await
                .map(|_| ())
                .unwrap(),
        }
        slot.finish("completed", "check wave completed").await;

        let rows = attempts(&orch.db.local, "j-prod").await;
        assert_eq!(rows.len(), 3, "{gate}: {rows:?}");
        assert_eq!(rows[2].1, expected, "{gate}: {rows:?}");
        assert!(!orch.turn_end_checks_in_flight("j-prod"), "{gate}");
    }
}

/// The edges that are not turn ends never mark a running wave owed: its own
/// release arms whatever its job is owed.
#[tokio::test]
async fn a_recovery_edge_leaves_a_running_wave_alone() {
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);
    let (_wave, slot) = claimed_slot(&orch, "j-prod");

    assert!(!super::review_push::spawn_review_wave_unless_in_flight(
        &orch, "j-prod"
    ));

    assert_eq!(
        attempts(&orch.db.local, "j-prod").await.len(),
        1,
        "a recovery edge records nothing against a held slot"
    );
    slot.finish("completed", "check wave completed").await;
    assert!(
        !wave_scheduled(&orch, "j-prod"),
        "nothing was owed, so nothing is armed"
    );
}

#[derive(Clone, Default)]
struct ChangeSink {
    events: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
}

impl crate::services::EventEmitter for ChangeSink {
    fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push((event.to_string(), payload));
        Ok(())
    }

    fn emit_empty(&self, event: &str) -> Result<(), String> {
        self.emit(event, serde_json::Value::Null)
    }
}

/// A live checks view refetches on a `check_result_cache` change and does not
/// poll while no lane reads as running, so every move of a review wave's board
/// must announce one. Without it, a panel open at submission kept every member
/// `queued` for the whole wave.
#[tokio::test]
async fn every_move_of_a_review_waves_board_announces_a_check_change() {
    let db = test_db().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let sink = ChangeSink::default();
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_emitter(sink.clone())
            .build(),
    );
    let orch = OrchestratorBuilder::new(
        Arc::new(DbState::new(Arc::new(db), search_index)),
        services,
        config_dir,
    )
    .build();
    let plan = crate::execution::selection::CheckPlan {
        name: "rust-tests".into(),
        applies: true,
        command: "true".into(),
        scope: crate::execution::selection::CheckScope::Full,
        resource_class: crate::config::project_settings::CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        verdict_platforms: Vec::new(),
        verdict_runtime: Vec::new(),
        verdict_executor_ids: None,
        config_error: None,
    };
    let board = crate::execution::checks_turn_end::review_wave_board(
        &orch,
        "j-prod",
        &[(plan, "input".to_string())],
    );
    let check_changes = || {
        sink.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, payload)| {
                event == "db-change" && payload["table"] == "check_result_cache"
            })
            .count()
    };

    board.transition(0, "running", None);
    assert_eq!(check_changes(), 1, "a member starting is announced");
    board.transition(0, "finished", Some("finished in 2s (exit 0)".into()));
    assert_eq!(check_changes(), 2, "a member finishing is announced");
}

/// cairn/4690. The regression fixture the issue names, composed end to end on
/// the runner: a real review wave (`run_turn_end_checks`, from the sealed head
/// of a real agent branch) whose middle member never returns. The machine is a
/// fake executor on the fleet link. It runs the members in order, reports each
/// start and finish, and stops the silent one as `CommandStop::Silent`.
///
/// While the batch is open, the wave's board shows the member the executor is
/// on and keeps the stopped one `finished`, not red. When the batch returns,
/// the silent member is recorded as the named stall non-verdict, both siblings
/// are recorded passed, the attempt reaches `completed`, and the slot is free.
/// The real-process half (the silent tree actually stopped and reaped) is the
/// executor's own and is pinned in `cairn-exec`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(jj)]
async fn a_silent_member_ends_its_wave_as_a_named_stall_beside_recorded_siblings() {
    use cairn_common::executor_protocol::{CommandStop, ExecutorMessage, RunnerCallback};
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/cairn-4690-silent-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    std::fs::create_dir_all(workspace.join(".cairn")).unwrap();
    std::fs::write(
        workspace.join(".cairn/config.yaml"),
        "checks:\n  fast-a:\n    command: \"true\"\n    when: review\n  silent:\n    command: \"true\"\n    when: review\n  fast-b:\n    command: \"true\"\n    when: review\n",
    )
    .unwrap();
    std::fs::write(workspace.join("change.rs"), "the change\n").unwrap();
    crate::jj::seal(&jj, &workspace, "three review lanes", None).unwrap();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repository.path().to_string_lossy().as_ref(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();

    let (sender, mut executor) = tokio::sync::mpsc::unbounded_channel();
    let generation = orch.fleet.attach_executor(sender);
    let (attempt, slot) = claimed_slot(&orch, "j-prod");
    let wave = tokio::spawn(crate::execution::checks_turn_end::run_turn_end_checks(
        orch.clone(),
        "j-prod".to_string(),
        slot,
    ));

    // The machine: claim what is offered, then take the granted batch.
    let (request, batch) = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            match executor.recv().await.expect("the link stays open") {
                ExecutorMessage::Offer { entry_id, .. } => {
                    orch.fleet.handle_executor_message(
                        crate::fleet::COLOCATED_EXECUTOR_ID,
                        generation,
                        ExecutorMessage::Claim { entry_id },
                    );
                }
                ExecutorMessage::Granted {
                    request,
                    batch: Some(batch),
                    ..
                } => return (request, batch),
                _ => {}
            }
        }
    })
    .await
    .expect("the wave reaches the machine");
    assert!(batch.sequential, "the wave's members run in order");
    assert_eq!(batch.items.len(), 3);
    let context = batch
        .runner_context_id
        .clone()
        .expect("the wave names a context for its members' reports");
    let board = orch
        .turn_end_check_runtime_status("j-prod")
        .and_then(|status| status.board)
        .expect("a submitted wave publishes its board");
    let index_of = |stream_id: &str| -> usize {
        stream_id
            .rsplit_once(":check-")
            .and_then(|(_, index)| index.parse().ok())
            .unwrap()
    };
    let name_of = |stream_id: &str| board.snapshot()[index_of(stream_id)].name.clone();
    let state_of = |name: &str| {
        board
            .snapshot()
            .into_iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.state)
            .unwrap()
    };

    let mut outcomes = Vec::new();
    for item in &batch.items {
        let name = name_of(&item.stream_id);
        orch.fleet
            .handle_runner_callback(
                &orch,
                RunnerCallback::ProcessItemStarted {
                    runner_context_id: context.clone(),
                    stream_id: item.stream_id.clone(),
                },
            )
            .await;
        assert_eq!(state_of(&name), "running", "{name} starts");
        let silent = name == "silent";
        let exit = if silent { 143 } else { 0 };
        orch.fleet
            .handle_runner_callback(
                &orch,
                RunnerCallback::ProcessItemCompleted {
                    runner_context_id: context.clone(),
                    stream_id: item.stream_id.clone(),
                    succeeded: !silent,
                    exit_code: Some(exit),
                    timed_out: false,
                    duration_ms: 1_000,
                },
            )
            .await;
        assert_eq!(
            state_of(&name),
            "finished",
            "{name}'s exit is not its verdict"
        );
        let mut outcome = serde_json::json!({
            "header": name, "body": format!("{name} output"), "succeeded": !silent,
            "suspended": false, "exitCode": exit, "startedAtUnixMs": 1,
            "finishedAtUnixMs": 2, "durationMs": 1,
        });
        if silent {
            outcome["stop"] = serde_json::to_value(CommandStop::Silent {
                quiet_ms: 1_800_000,
                last_output_unix_ms: None,
                cpu_ms_in_window: Some(0),
                storage_io_bytes_in_window: Some(0),
                tree: Vec::new(),
            })
            .unwrap();
        }
        outcomes.push(outcome);
    }

    let metadata: cairn_common::executor_protocol::CellExecutionMeta =
        serde_json::from_value(serde_json::json!({
            "executorId": crate::fleet::COLOCATED_EXECUTOR_ID,
            "executorDeviceId": "local-device",
            "executorConnectionGeneration": generation,
            "slotId": "cell", "cellEpoch": 1,
            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
        }))
        .unwrap();
    orch.fleet.handle_executor_message(
        crate::fleet::COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            outcome: cairn_common::executor_protocol::CellOutcome::Completed {
                request_id: request.request_id.clone(),
                attempt_id: request.attempt_id.clone(),
                exit_code: Some(1),
                output: serde_json::to_string(&outcomes).unwrap(),
                timed_out: false,
                metadata,
                mutation_delta: None,
                sandbox_denials: Vec::new(),
                tracked_modifications: None,
                node_scratch: None,
            },
        },
    );
    tokio::time::timeout(std::time::Duration::from_secs(120), wave)
        .await
        .expect("the wave settles once its batch returns")
        .expect("the wave does not panic");

    assert!(
        !orch.turn_end_checks_in_flight("j-prod"),
        "the slot is released"
    );
    let rows = attempts(&orch.db.local, "j-prod").await;
    let row = rows.iter().find(|row| row.0 == attempt).unwrap();
    assert_eq!(row.1, "completed", "{rows:?}");

    let recorded = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT check_name, passed, failure_kind, output_tail
                           FROM check_result_cache WHERE job_id='j-prod'",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    found.push((
                        row.text(0)?,
                        row.i64(1)? != 0,
                        row.opt_text(2)?,
                        row.text(3)?,
                    ));
                }
                Ok::<_, crate::storage::DbError>(found)
            })
        })
        .await
        .unwrap();
    let lane = |name: &str| {
        recorded
            .iter()
            .find(|row| row.0 == name)
            .unwrap_or_else(|| panic!("{name} is recorded: {recorded:?}"))
    };
    for sibling in ["fast-a", "fast-b"] {
        let row = lane(sibling);
        assert!(row.1, "{sibling} is recorded passed: {row:?}");
        assert_eq!(row.2, None, "{row:?}");
    }
    let stalled = lane("silent");
    assert!(!stalled.1, "{stalled:?}");
    // A stall is a substrate death, stored under the kind every non-capacity
    // substrate shape shares; the tail is what names it a stall.
    assert_eq!(
        stalled.2.as_deref(),
        Some("infrastructure"),
        "a stall is a classified non-verdict, never a bare red: {stalled:?}"
    );
    assert!(
        stalled.3.contains("stopped making progress") || stalled.3.contains("1800s"),
        "the stall is named: {stalled:?}"
    );
}

/// Owner acceptance for cairn/6083 (cairn/6091): cancellation must retain a
/// complete group's immutable evidence even while another placement is held.
///
/// The second grant is an ordering barrier ONLY while placement groups are
/// submitted serially: the first full Result has returned into the coordinator
/// before it submits the second group. ProcessItemCompleted is not that barrier.
/// When 6083 makes submission concurrent, replace this trigger with the owner's
/// explicit durable-completion acknowledgment (observation AND log), not a sleep
/// or a status callback, before treating a green run as acceptance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(jj)]
#[ignore = "owner acceptance: cairn/6083 must retain completed groups across cancellation; concurrent submission needs a durable-completion acknowledgment"]
async fn cancellation_retains_completed_placement_evidence_under_the_original_plan() {
    use cairn_common::executor_protocol::{CellOutcome, CommandStop, ExecutorMessage};

    let bin = crate::jj::tests::jj_bin()
        .expect("owner acceptance requires jj; a missing fixture capability is not a pass");
    let db = test_db().await;
    seed(&db, "initial").await;
    attach_planbuild_topology(&db, "j-prod", NodeRole::Builder).await;
    let orch = test_orchestrator(db);
    let repository = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repository.path());
    let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
    let jj = crate::jj::environment(&bin, &orch.config_dir);
    crate::jj::ensure_project_store(&jj, &store, repository.path()).unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let workspace = workspaces.path().join("builder");
    let branch = "agent/cairn-6091-cancellation-0";
    crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
    assert!(crate::jj::is_jj_dir(&workspace));
    assert!(!workspace.join(".git").exists());
    let platform = crate::fleet::ExecutorPlatform::this_process();
    let other_os = if platform.os == "linux" {
        "macos"
    } else {
        "linux"
    };
    std::fs::create_dir_all(workspace.join(".cairn")).unwrap();
    std::fs::write(
        workspace.join(".cairn/config.yaml"),
        format!(
            "checks:\n  fast-a:\n    command: \"true\"\n    when: review\n    verdictPlatforms: [{}]\n  held-b:\n    command: \"true\"\n    when: review\n    verdictPlatforms: [{}, {}]\n  stopped-a:\n    command: \"true\"\n    when: review\n    verdictPlatforms: [{}]\n",
            platform.os, platform.os, other_os, platform.os
        ),
    )
    .unwrap();
    std::fs::write(workspace.join("change.rs"), "the original change\n").unwrap();
    crate::jj::seal(&jj, &workspace, "review cancellation evidence", None).unwrap();
    orch.db
        .local
        .execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p-rev'",
            (repository.path().to_string_lossy().as_ref(),),
        )
        .await
        .unwrap();
    orch.db
        .local
        .execute("UPDATE jobs SET branch=?1 WHERE id='j-prod'", (branch,))
        .await
        .unwrap();

    let (sender, mut executor) = tokio::sync::mpsc::unbounded_channel();
    let generation = orch.fleet.attach_executor(sender);
    let (attempt, slot) = claimed_slot(&orch, "j-prod");
    let wave = tokio::spawn(crate::execution::checks_turn_end::run_turn_end_checks(
        orch.clone(),
        "j-prod".to_string(),
        slot,
    ));
    let mut original_plan = None;
    let mut original_plan_json = None;
    for group in 0..2 {
        let (request, batch) = tokio::time::timeout(std::time::Duration::from_secs(120), async {
            loop {
                match executor
                    .recv()
                    .await
                    .expect("the fake fleet link stays open")
                {
                    ExecutorMessage::Offer { entry_id, .. } => {
                        orch.fleet.handle_executor_message(
                            crate::fleet::COLOCATED_EXECUTOR_ID,
                            generation,
                            ExecutorMessage::Claim { entry_id },
                        );
                    }
                    ExecutorMessage::Granted {
                        request,
                        batch: Some(batch),
                        ..
                    } => {
                        return (request, batch);
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("the next placement group reaches the fake machine");
        let board = orch
            .turn_end_check_runtime_status("j-prod")
            .and_then(|status| status.board)
            .expect("the wave publishes a board");
        let names: Vec<_> = batch
            .items
            .iter()
            .map(|item| {
                let index: usize = item
                    .stream_id
                    .rsplit_once(":check-")
                    .and_then(|(_, index)| index.parse().ok())
                    .unwrap();
                board.snapshot()[index].name.clone()
            })
            .collect();
        if group == 1 {
            assert_eq!(names, ["held-b"], "distinct placement stays pending");
            // Intentionally send neither status completion nor a full Result.
            // On the serial implementation this grant proves the first group's
            // authoritative Result has been consumed, not merely sent.
            orch.cancel_turn_end_checks(
                "j-prod",
                crate::orchestrator::TurnEndCancelReason::BranchAdvanced,
            );
            break;
        }
        assert_eq!(names, ["fast-a", "stopped-a"]);
        let attempt_id = attempt.clone();
        let plan_json = orch
            .db
            .local
            .read(move |conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT plan_json FROM turn_end_check_attempts WHERE id=?1",
                            (attempt_id.as_str(),),
                        )
                        .await?;
                    rows.next()
                        .await?
                        .expect("attempt has a sealed plan")
                        .text(0)
                })
            })
            .await
            .unwrap();
        let plan: crate::execution::checks_turn_end::WavePlan =
            serde_json::from_str(&plan_json).unwrap();
        assert_eq!(plan.applicable_names.len(), 3);
        let outcomes: Vec<_> = names
            .iter()
            .map(|name| {
                let stopped = name == "stopped-a";
                let mut result = serde_json::json!({
                    "header": name, "body": format!("{name} original output"),
                    "succeeded": true, "suspended": false,
                    "exitCode": 0,
                    "startedAtUnixMs": 1, "finishedAtUnixMs": 2, "durationMs": 1,
                });
                if stopped {
                    result["stop"] = serde_json::to_value(CommandStop::Silent {
                        quiet_ms: 1_800_000,
                        last_output_unix_ms: None,
                        cpu_ms_in_window: Some(0),
                        storage_io_bytes_in_window: Some(0),
                        tree: Vec::new(),
                    })
                    .unwrap();
                }
                result
            })
            .collect();
        let metadata = serde_json::from_value(serde_json::json!({
            "executorId": crate::fleet::COLOCATED_EXECUTOR_ID,
            "executorDeviceId": "local-device",
            "executorConnectionGeneration": generation,
            "slotId": "cell", "cellEpoch": 1,
            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
            "verdictPlatform": platform.os, "verdictArch": platform.arch,
            "toolchainFingerprint": plan.expected_toolchain,
            "verdictEnvironmentHash": cairn_checkenv::verdict_environment_hash(
                Vec::<String>::new(), |name| std::env::var(name).ok()
            ),
        }))
        .unwrap();
        orch.fleet.handle_executor_message(
            crate::fleet::COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Result {
                request_id: request.request_id.clone(),
                attempt_id: request.attempt_id.clone(),
                outcome: CellOutcome::Completed {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    exit_code: Some(1),
                    output: serde_json::to_string(&outcomes).unwrap(),
                    timed_out: false,
                    metadata,
                    mutation_delta: None,
                    sandbox_denials: Vec::new(),
                    tracked_modifications: None,
                    node_scratch: None,
                },
            },
        );
        original_plan = Some(plan);
        original_plan_json = Some(plan_json);
    }
    tokio::time::timeout(std::time::Duration::from_secs(120), wave)
        .await
        .expect("cancellation joins without a held-group result")
        .expect("the wave does not panic");
    assert!(
        !orch.turn_end_checks_in_flight("j-prod"),
        "the slot is released"
    );

    // Read through a fresh handle, not the hot cache or the wave's status board.
    let reopened = LocalDb::open(orch.db.local.path()).await.unwrap();
    let rows = attempts(&reopened, "j-prod").await;
    assert_eq!(
        rows.iter().find(|row| row.0 == attempt).unwrap().1,
        "cancelled"
    );
    let plan = original_plan.unwrap();
    let attempt_id = attempt.clone();
    let saved_json = reopened
        .read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT plan_json FROM turn_end_check_attempts WHERE id=?1",
                        (attempt_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .expect("cancelled attempt retains its plan")
                    .text(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        Some(saved_json),
        original_plan_json,
        "cancellation must not replan identity"
    );
    let recorded = reopened
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, check_name, commit_sha, tree_hash, input_hash, verdict,
                    complete, reusable, failure_kind, output_tail, defined_by_commit_sha
               FROM check_result_observations WHERE job_id='j-prod' AND cadence='review'
               ORDER BY check_name",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    found.push(serde_json::json!({
                        "id": row.text(0)?, "name": row.text(1)?, "head": row.text(2)?,
                        "tree": row.text(3)?, "input": row.text(4)?, "verdict": row.text(5)?,
                        "complete": row.i64(6)?, "reusable": row.i64(7)?,
                        "failure": row.opt_text(8)?, "output": row.text(9)?,
                        "definedBy": row.text(10)?,
                    }));
                }
                Ok::<_, crate::storage::DbError>(found)
            })
        })
        .await
        .unwrap();
    eprintln!("owner acceptance after cancellation: immutable observations={recorded:?}");
    assert!(
        !recorded
            .iter()
            .any(|row| row["name"] == "held-b" && row["verdict"] == "passed"),
        "an incomplete group must never manufacture a pass"
    );
    for name in ["fast-a", "stopped-a"] {
        assert_eq!(
            recorded.iter().filter(|row| row["name"] == name).count(),
            1,
            "{name} retains exactly one completed observation: {recorded:?}"
        );
        let row = recorded
            .iter()
            .find(|row| row["name"] == name)
            .expect("completed member recorded");
        assert_eq!(row["head"], plan.head);
        assert_eq!(row["definedBy"], plan.head);
        assert_eq!(row["tree"], plan.tree_hash);
        assert_eq!(row["input"], plan.input_hashes[name]);
        let log = orch
            .config_dir
            .join("turn-checks/j-prod")
            .join(format!("{name}.log"));
        assert!(
            std::fs::read_to_string(&log)
                .unwrap_or_else(|error| panic!("{}: {error}", log.display()))
                .contains(&format!("{name} original output")),
            "the original member log survives the cancelled wave"
        );
    }
    let passed = recorded.iter().find(|row| row["name"] == "fast-a").unwrap();
    assert_eq!(passed["verdict"], "passed");
    assert_eq!(passed["complete"], 1);
    assert_eq!(passed["reusable"], 1);
    assert_eq!(passed["failure"], serde_json::Value::Null);
    assert!(passed["output"]
        .as_str()
        .unwrap()
        .contains("fast-a original output"));
    let stopped = recorded
        .iter()
        .find(|row| row["name"] == "stopped-a")
        .unwrap();
    assert_ne!(
        stopped["verdict"], "passed",
        "a stopped member is not a successful verdict"
    );
    assert_eq!(stopped["reusable"], 0);
    assert_eq!(stopped["failure"], "infrastructure");

    let reused = reopened
        .read(move |conn| {
            Box::pin(async move {
                let hit = crate::execution::checks::reusable_verdict_for_check_conn(
                    conn,
                    "p-rev",
                    "fast-a",
                    &plan.checks["fast-a"],
                    &plan.input_hashes["fast-a"],
                    &plan.expected_toolchain,
                )
                .await;
                for name in ["stopped-a", "held-b"] {
                    assert!(
                        crate::execution::checks::reusable_verdict_for_check_conn(
                            conn,
                            "p-rev",
                            name,
                            &plan.checks[name],
                            &plan.input_hashes[name],
                            &plan.expected_toolchain,
                        )
                        .await
                        .is_none(),
                        "{name} cannot supply reusable pass evidence"
                    );
                }
                Ok::<_, crate::storage::DbError>(hit.map(|hit| hit.source_observation_id))
            })
        })
        .await
        .unwrap();
    assert_eq!(
        reused.as_deref(),
        passed["id"].as_str(),
        "same-input reuse cites the original immutable observation"
    );
}
