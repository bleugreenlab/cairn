/// Every substrate shape names the repair that would clear it, in a table
/// rather than behind a wildcard.
///
/// A new shape has to be classified deliberately, because both defaults are
/// wrong in an expensive way: inheriting the head domain lets a structural
/// gap re-arm on every commit, which is the defect this mechanism exists to
/// end, and inheriting the fleet domain strips a transient stumble of its
/// retries everywhere at once.
#[test]
fn every_substrate_shape_names_its_repair_domain() {
    use super::SubstrateFailureShape as Shape;
    use crate::execution::cache::InfraRepairDomain as Domain;
    let table = [
        (Shape::NoMachine, Domain::Fleet),
        (Shape::Draining, Domain::Fleet),
        (Shape::Dispatch, Domain::Head),
        (Shape::Capacity, Domain::Head),
        (Shape::MachineUnreachable, Domain::Head),
        (Shape::NoReachableMachine, Domain::Head),
        (Shape::Preparation, Domain::Head),
        (Shape::EnvironmentRetired, Domain::Head),
        (Shape::Result, Domain::Head),
        (Shape::OutcomeUnknown, Domain::Head),
        (Shape::Storage, Domain::Head),
        (Shape::RunnerFault, Domain::Head),
        (Shape::CommandStalled, Domain::HeadOnPlatform),
        (Shape::CommandOomKilled, Domain::Environment),
        (Shape::BuildVolumeAbsent, Domain::Fleet),
        (Shape::BuildVolumeLost, Domain::Environment),
        (Shape::MeasurementBlind, Domain::Fleet),
    ];
    assert_eq!(
        table.len(),
        17,
        "a new substrate shape must be classified in this table, not defaulted"
    );
    for (shape, expected) in table {
        assert_eq!(
            super::infra_repair_domain(Some(shape), Some(super::CheckFailureKind::Infrastructure)),
            expected,
            "{shape:?}"
        );
    }

    // No substrate failure means the command RAN and its own output named
    // the cause — an evidence-matched host defect, which belongs to the
    // machine that produced it.
    assert_eq!(
        super::infra_repair_domain(None, Some(super::CheckFailureKind::Infrastructure)),
        Domain::Environment
    );
    assert_eq!(
        super::infra_repair_domain(None, Some(super::CheckFailureKind::Capacity)),
        Domain::Head
    );
    assert_eq!(super::infra_repair_domain(None, None), Domain::Head);
}

#[test]
fn no_matching_executor_classifies_immutable_refusals() {
    use super::{infra_repair_domain, SubstrateFailureShape as Shape};
    use crate::execution::cache::InfraRepairDomain as Domain;
    use cairn_common::executor_protocol::{
        CellUnavailableReason, DeclineReason, MachineMeasurement, MachineRole, PlacementRefusal,
        PlacementRejectionReason as Reason, PlacementWorkClass,
    };

    let structural = vec![
        Reason::Drained,
        Reason::SelectorMismatch {
            requested: "os windows".into(),
        },
        Reason::AbsenceExpired {
            absent_since_unix_ms: 1,
        },
        Reason::RoleExcluded {
            role: MachineRole::InferenceOnly,
            work_class: PlacementWorkClass::ReviewChecks,
            publication_leg: false,
        },
        Reason::BuildPrerequisitesMissing {
            project_id: "project".into(),
            script: "build-prerequisites".into(),
            detail: "MISSING helper".into(),
        },
    ];
    let classify = |reasons: Vec<Reason>| {
        let rejected = reasons
            .into_iter()
            .enumerate()
            .map(|(i, reason)| PlacementRefusal {
                executor_id: format!("machine-{i}"),
                executor_name: format!("machine-{i}"),
                reason,
            })
            .collect();
        super::check_result_from_cell_outcome(
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::NoMatchingExecutor { rejected },
                diagnostic: "all machines refused".into(),
            },
            &crate::fleet::CellAccount::default(),
            None,
        )
        .unwrap_err()
    };
    let CheckExecutionFailure::Substrate(failure) = classify(structural.clone()) else {
        panic!("expected substrate refusal");
    };
    assert_eq!(failure.shape(), Shape::NoMachine);
    assert_eq!(
        infra_repair_domain(Some(failure.shape()), None),
        Domain::Fleet
    );

    for reason in [
        Reason::TelemetryStale {
            measurement: MachineMeasurement::Cpu,
            age_ms: 108_276,
            stale_after_ms: 90_000,
        },
        Reason::TelemetryGap {
            measurement: MachineMeasurement::Cpu,
            gap: cairn_common::executor_protocol::MeasurementGap::NotSampled,
        },
        Reason::UpdatePending {
            returns_by_unix_ms: Some(123_000),
        },
        Reason::ProvisioningTurn {
            project_id: "project".into(),
            ahead_request_id: "earlier".into(),
            ahead_waiting_since_unix_ms: 1,
        },
        Reason::Declined {
            reason: DeclineReason::Pressure,
            detail: Some("CPU pressure".into()),
            declined_at_unix_ms: 1,
        },
        Reason::Reconnecting {
            returns_by_unix_ms: 123_000,
        },
    ] {
        let description = reason.describe();
        let mut reasons = structural.clone();
        reasons.push(reason);
        let CheckExecutionFailure::Substrate(failure) = classify(reasons) else {
            panic!("expected substrate refusal");
        };
        assert_eq!(failure.shape(), Shape::Capacity, "{description}");
        assert_eq!(
            infra_repair_domain(Some(failure.shape()), None),
            Domain::Head
        );
        let message = failure.agent_message();
        assert!(message.contains(&description), "{message}");
        assert!(message.contains("retried"), "{message}");
        assert!(!message.contains(Shape::NoMachine.lead()), "{message}");
        assert_eq!(
            capacity_retry_decision(&PlannedCheckBatchOutcome::failed(vec![0], failure)),
            CapacityRetry::Surface,
            "a terminal placement refusal must not restart its bounded wait"
        );
    }
}

/// A fleet suppression says what lifts it, and the two populations get
/// two sentences: one keyed on blind machines lifts on their telemetry,
/// one keyed on the capability set alone only on a fleet change. Both say
/// they counted commits, not attempts.
#[test]
fn a_fleet_suppression_names_what_lifts_it() {
    use crate::execution::cache::{
        infra_fleet_scope, InfraRepairDomain, SuppressedCheck, SuppressedScope,
    };
    let mut entry = cache_entry("workspace-compile", "tree-a", false);
    entry.infra_failure_streak = 3;
    let suppressed = |fleet_blind: Vec<String>| SuppressedCheck {
        domain: InfraRepairDomain::Fleet,
        scopes: vec![SuppressedScope {
            scope: infra_fleet_scope("digest"),
            executor_id: None,
            platform: None,
            entry: entry.clone(),
        }],
        fleet_blind,
    };

    let blind = super::suppressed_check_message(&suppressed(vec!["fedora".to_string()]));
    assert!(blind.contains("died on 3 separate commits"), "{blind}");
    assert!(
        blind.contains("retries of one commit count once"),
        "{blind}"
    );
    assert!(
        blind.contains("could not read the current load of fedora")
            && blind.contains("as soon as their telemetry is fresh"),
        "{blind}"
    );

    let structural = super::suppressed_check_message(&suppressed(Vec::new()));
    assert!(
        structural.contains("when the set of machines matching its requirements changes")
            && !structural.contains("telemetry"),
        "{structural}"
    );
}

/// The refusal a thread session gets instead of an internal row error
/// (CAIRN-4237). It has to state the posture and hand back a command that
/// actually parses: `--branch` is a flag, and a bare revision after the
/// suite name would be read as a second suite.
#[test]
fn the_thread_refusal_states_the_posture_and_a_command_that_parses() {
    let refusal = super::thread_session_coordinate_refusal("rust-lint");

    assert!(
        refusal.contains("thread") && refusal.contains("owns no branch of its own"),
        "the refusal states the posture, not a lookup failure: {refusal}"
    );
    assert!(
        refusal.contains("cairn check run rust-lint --branch <revision>"),
        "the refusal names the coordinate form the CLI accepts: {refusal}"
    );
}

/// Both production check-request constructors obtain their admission horizon
/// from this boundary. A finite value recreates the wide-wave starvation:
/// the last suite expires while its siblings visibly hold the machine.
#[test]
fn production_check_requests_park_without_an_elapsed_capacity_deadline() {
    let request = || super::CellRequest {
        request_id: "request".into(),
        attempt_id: "attempt".into(),
        project_id: "project".into(),
        repository: super::RepositoryLocator::ScratchOnly {
            owner_id: "project".into(),
        },
        base_commit: "commit".into(),
        command: "check".into(),
        command_class: super::CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::ReviewChecks,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: super::CellPriority::ReviewCheck,
        wait_horizon_unix_ms: 1,
        waiting_since_unix_ms: 0,
        timeout_ms: 1,
        mutation_policy: super::MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    };
    let emitted = crate::execution::wire::durable_check_request(|wait_horizon_unix_ms| {
        let mut request = request();
        request.wait_horizon_unix_ms = wait_horizon_unix_ms;
        request
    });
    assert_eq!(
        emitted.wait_horizon_unix_ms,
        u64::MAX,
        "review checks remain wanted until admission, cancellation, or requester loss"
    );
}

/// An operator reading the log finds the cause in the first line, ahead of
/// an environment dump that can run to kilobytes. This ordering is the whole
/// point: the same failure, with its diagnostic sorted into the dump, went
/// undiagnosed while every review check on the host failed.
#[test]
fn infrastructure_failure_log_leads_with_the_cause() {
    use super::infrastructure_failure_log_line;

    let line = infrastructure_failure_log_line(
        "rust-lint",
        Some(
            "Preparation: cell preparation command failed: bun i\nstderr:\n/bin/sh: bun: command not found\n\nExit code: 127",
        ),
        serde_json::json!({
            "buildService": { "configFingerprint": "x".repeat(4_096) },
            "processTree": ["a", "b"],
        }),
    );

    let first = line.lines().next().unwrap();
    assert!(first.contains("rust-lint"), "first line: {first}");
    assert!(
        first.contains("cell preparation command failed: bun i"),
        "first line: {first}"
    );
    assert!(
        line.find("buildService") > line.find("bun: command not found"),
        "the environment dump must follow the cause, not bury it"
    );
}

/// A missing diagnostic is a finding, not an absence to be silently skipped:
/// it says the failure reached the log with no substrate evidence attached.
#[test]
fn infrastructure_failure_log_names_a_missing_diagnostic() {
    use super::{infrastructure_failure_log_line, NO_SUBSTRATE_DIAGNOSTIC};

    let line = infrastructure_failure_log_line("lint", None, serde_json::json!({}));
    assert!(line
        .lines()
        .next()
        .unwrap()
        .contains(NO_SUBSTRATE_DIAGNOSTIC));
}

/// Eligibility is a property of what the work does, not of what it asked
/// for. Both a constrained and an unconstrained pure-verdict group are free
/// to move; a mutating group never is, whatever it stated.
#[test]
fn only_pure_verdict_check_batches_are_free_to_move() {
    use super::batch_placement_mobility;
    use crate::fleet::MutationPolicy;
    use cairn_common::executor_protocol::PlacementMobility;

    assert_eq!(
        batch_placement_mobility(&MutationPolicy::PureVerdict),
        PlacementMobility::SpillEligible
    );
    assert_eq!(
        batch_placement_mobility(&MutationPolicy::AllowDelta),
        PlacementMobility::PinnedOrColocated,
        "a batch whose delta has to come back cannot be placed away from the tree it mutates"
    );
}

/// The selector partition runs before dispatch, so a group that named two
/// contradictory machines is refused as the configuration error it is rather
/// than becoming a placement problem.
#[test]
fn only_a_pure_verdict_batch_partitions_by_placement() {
    assert!(partitions_by_placement(&MutationPolicy::PureVerdict));
    assert!(!partitions_by_placement(&MutationPolicy::AllowDelta));
}

fn completed_placement_group(index: usize) -> PlannedCheckBatchOutcome {
    PlannedCheckBatchOutcome {
        results: HashMap::from([(
            index,
            exec_ok(Some(0), format!("group {index}")).map_err(CheckExecutionFailure::from),
        )]),
        request: None,
        delta: None,
        store_dir: None,
    }
}

#[tokio::test]
async fn a_held_placement_group_does_not_delay_its_siblings_completion() {
    let (release, held) = tokio::sync::oneshot::channel();
    let (completed, mut received) = tokio::sync::mpsc::unbounded_channel();
    let groups = [(0, Some(held)), (1, None)]
        .into_iter()
        .map(|(index, held)| {
            let completed = completed.clone();
            async move {
                if let Some(held) = held {
                    held.await.unwrap();
                }
                let outcome = completed_placement_group(index);
                completed.send(index).unwrap();
                outcome
            }
        });
    let wave = collect_placement_group_results(groups);
    tokio::pin!(wave);
    tokio::select! {
        biased;
        _ = &mut wave => panic!("the first group must still be held"),
        completed = received.recv() => assert_eq!(completed, Some(1)),
    }
    release.send(()).unwrap();
    let results = wave.await;
    assert!(results[&0].is_ok());
    assert!(results[&1].is_ok());
}

#[tokio::test]
async fn a_placement_group_error_preserves_other_results_and_its_own_reason() {
    let results = collect_placement_group_results([
        std::future::ready(placement_group_outcome(
            &[0, 2],
            Err("group-specific configuration fault".into()),
        )),
        std::future::ready(completed_placement_group(1)),
    ])
    .await;
    assert!(results[&1].is_ok());
    for index in [0, 2] {
        let Err(CheckExecutionFailure::Substrate(failure)) = &results[&index] else {
            panic!("a submission fault is not a command verdict");
        };
        assert_eq!(failure.shape(), SubstrateFailureShape::Dispatch);
        assert_eq!(failure.diagnostic(), "group-specific configuration fault");
    }
}

#[tokio::test]
async fn placement_capacity_retry_never_resubmits_or_rerecords_a_completed_sibling() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let recorded = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let (completed, mut received) = tokio::sync::mpsc::unbounded_channel();
    let complete: CheckGroupCompletion = {
        let recorded = recorded.clone();
        Arc::new(move |results| {
            for (&index, result) in results {
                assert!(result.is_ok(), "a capacity refusal is not a final result");
                recorded[index].fetch_add(1, Ordering::SeqCst);
                completed.send(index).unwrap();
            }
            Box::pin(std::future::ready(()))
        })
    };
    let groups = (0..2).map(|index| {
        let attempts = attempts.clone();
        let complete = complete.clone();
        async move {
            submit_placement_group_until_final(&[index], Some(&complete), || {
                let attempt = attempts[index].fetch_add(1, Ordering::SeqCst);
                let outcome = if index == 0 && attempt == 0 {
                    PlannedCheckBatchOutcome::failed(
                        vec![index],
                        SubstrateFailure::new(
                            SubstrateFailureShape::Capacity,
                            "provisioning a cell",
                        ),
                    )
                } else {
                    completed_placement_group(index)
                };
                std::future::ready((
                    GroupWait {
                        description: "the group's own capacity hold".into(),
                        relief_ms: None,
                        unforecastable: false,
                    },
                    Ok(outcome),
                ))
            })
            .await
        }
    });
    let wave = collect_placement_group_results(groups);
    tokio::pin!(wave);
    tokio::select! {
        biased;
        _ = &mut wave => panic!("group zero must still be waiting to retry"),
        completed = received.recv() => assert_eq!(completed, Some(1)),
    }
    assert_eq!(attempts[0].load(Ordering::SeqCst), 1);
    let results = wave.await;
    assert!(results.values().all(Result::is_ok));
    assert_eq!(attempts[0].load(Ordering::SeqCst), 2);
    assert_eq!(attempts[1].load(Ordering::SeqCst), 1);
    assert_eq!(recorded[0].load(Ordering::SeqCst), 1);
    assert_eq!(recorded[1].load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_a_placement_wave_drops_every_pending_group() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Interest(Arc<AtomicUsize>);
    impl Drop for Interest {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let active = Arc::new(AtomicUsize::new(0));
    let groups = (0..3).map(|_| {
        let active = active.clone();
        async move {
            active.fetch_add(1, Ordering::SeqCst);
            let _interest = Interest(active);
            std::future::pending::<PlannedCheckBatchOutcome>().await
        }
    });
    let mut wave = Box::pin(collect_placement_group_results(groups));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(wave.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(active.load(Ordering::SeqCst), 3);
    drop(wave);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[test]
fn platform_scoped_checks_are_not_members_of_an_other_platform_batch() {
    let checks = HashMap::from([
        (
            "native".to_string(),
            serde_yaml::from_str::<CheckCommand>(
                "command: native\nverdictPlatforms: [macos, linux]\n",
            )
            .unwrap(),
        ),
        (
            "windows".to_string(),
            serde_yaml::from_str::<CheckCommand>("command: windows\nverdictPlatforms: [windows]\n")
                .unwrap(),
        ),
    ]);
    let plans = |platform| {
        let mut plans = plan_checks(
            &checks,
            &inputs(&checks),
            &[change("src/lib.rs")],
            Path::new("."),
        );
        retain_gating_platform_plans(&mut plans, &checks, platform);
        plans.into_iter().map(|plan| plan.name).collect::<Vec<_>>()
    };

    assert_eq!(plans("macos"), vec!["native"]);
    assert_eq!(plans("windows"), vec!["windows"]);
}

#[test]
fn contradictory_selectors_within_a_group_refuse_before_dispatch() {
    use super::{merge_batch_executor, PlannedCheckBatchItem};
    use cairn_common::executor_protocol::ExecutorSelector;

    let named = |name: &str| ExecutorSelector {
        name: Some(name.into()),
        ..ExecutorSelector::default()
    };
    let item = |name: &str| PlannedCheckBatchItem {
        executor: Some(named(name)),
        ..batch_item("true", super::CheckResourceClass::Shared)
    };
    let refusal = merge_batch_executor(&[item("bglab-ub"), item("bglab-mac")]).unwrap_err();
    assert!(refusal.contains("bglab-ub"), "{refusal}");
    assert!(refusal.contains("bglab-mac"), "{refusal}");
}

use super::*;

#[test]
fn cached_transition_marks_the_emitted_status_entry() {
    let snapshots = Arc::new(std::sync::Mutex::new(Vec::new()));
    let emitted = snapshots.clone();
    let plans = vec![(plan("api", "true"), "input".to_string())];
    let board = CheckStatusBoard::new(
        &plans,
        Arc::new(move |checks, _| emitted.lock().unwrap().push(checks)),
    );

    board.transition_cached(0, "passed", Some("cached".into()));

    let snapshots = snapshots.lock().unwrap();
    let entry = &snapshots.last().unwrap()[0];
    assert_eq!(entry.state, "passed");
    assert!(entry.cached);
}

/// Every check's selector, resolved with NO tree in hand. Glob and
/// no-input checks resolve exactly as they do in production; a `scope:`
/// check would degrade to the conservative whole-tree selector, which the
/// tests below do not exercise (see `execution::inputs` for those).
fn inputs(checks: &HashMap<String, CheckCommand>) -> ResolvedInputs {
    ResolvedInputs::resolve(checks, &HashMap::new(), &TreeSnapshot::empty())
}

fn inputs_for(name: &str, check: &CheckCommand) -> ResolvedInputs {
    inputs(&HashMap::from([(name.to_string(), check.clone())]))
}

fn selector(check: &CheckCommand) -> InputSelector {
    crate::execution::inputs::resolve_one(check, &HashMap::new(), &TreeSnapshot::empty())
}

#[test]
fn slot_check_env_enforces_line_tables_only_without_mutating_agent_env() {
    let agent_env = vec![
        ("PATH".to_string(), "/tools".to_string()),
        (SLOT_CHECK_DEV_DEBUG_ENV.0.to_string(), "full".to_string()),
    ];
    let slot_env = slot_check_env(agent_env.clone());

    assert_eq!(
        slot_env
            .iter()
            .find(|(key, _)| key == SLOT_CHECK_DEV_DEBUG_ENV.0)
            .map(|(_, value)| value.as_str()),
        Some(SLOT_CHECK_DEV_DEBUG_ENV.1)
    );
    assert_eq!(
        agent_env
            .iter()
            .find(|(key, _)| key == SLOT_CHECK_DEV_DEBUG_ENV.0)
            .map(|(_, value)| value.as_str()),
        Some("full")
    );
}

#[test]
fn manual_contract_snapshot_refuses_command_changed_at_execution_seam() {
    let original = plan("race", "command-a");
    let snapshot_command = original.command.clone();
    let snapshot_identity = check_resource_identity(
        "race",
        &serde_yaml::from_str::<CheckCommand>("command: command-a\n").unwrap(),
    )
    .key;

    // This models Settings replacing the live contract after planning. The
    // operation carries only owned snapshot values across the seam.
    let changed = plan("race", "command-b");
    let changed_identity = check_resource_identity(
        "race",
        &serde_yaml::from_str::<CheckCommand>("command: command-b\n").unwrap(),
    )
    .key;

    assert_ne!(snapshot_identity, changed_identity);
    assert!(require_snapshot_command(&snapshot_command, "command-a").is_ok());
    assert!(require_snapshot_command(&snapshot_command, &changed.command).is_err());
}

#[test]
fn manual_producer_uses_the_same_observation_shape_with_manual_provenance() {
    let plan = plan("rust-tests", "bun run test:rust");
    let coordinate =
        crate::execution::cache::SealedCheckCoordinate::new("commit", "commit", "tree", "input");
    let write = fresh_observation_write(
        "project",
        &coordinate,
        &plan,
        "job",
        "manual-check:run-123:rust-tests",
        0,
        None,
        12,
        "ok",
        ObservedOn::Coordinator,
        None,
        CheckReuseDecision {
            reusable: true,
            reason: None,
        },
        Vec::new(),
    );
    assert_eq!(write.cadence, "manual");
    assert_eq!(write.run_id.as_deref(), Some("run-123"));
    assert_eq!(write.commit_sha, "commit");
    assert_eq!(write.check_name, "rust-tests");
}

#[test]
fn dirty_manual_cache_context_rejects_store() {
    let context = ManualCheckCacheContext {
        project_id: "project".into(),
        project_key: "PROJECT".into(),
        job_id: "job".into(),
        commit_sha: "commit".into(),
        tree_hash: "tree".into(),
        input_hash: "input".into(),
        cacheable: false,
        entry: None,
    };
    assert!(context.require_cacheable().is_err());
    assert!(ManualCheckCacheContext {
        cacheable: true,
        ..context
    }
    .require_cacheable()
    .is_ok());
}
use crate::config::project_settings::{CheckPolicy, CheckResourceClass};
use crate::db::DbState;
use crate::execution::selection::CheckScope;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

async fn test_orchestrator(config_dir: &Path) -> Orchestrator {
    let local = LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.to_path_buf(),
    )
    .build()
}

#[tokio::test]
async fn write_check_guard_cleans_up_on_drop_and_counts_overlap() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let first = WriteChecksInFlightGuard::new(&orch, "job-a");
    let second = WriteChecksInFlightGuard::new(&orch, "job-a");
    assert!(orch.write_checks_in_flight("job-a"));

    drop(first);
    assert!(
        orch.write_checks_in_flight("job-a"),
        "one dropped future must not clear an overlapping write batch",
    );
    drop(second);
    assert!(
        !orch.write_checks_in_flight("job-a"),
        "dropping the final future must clear sidebar state",
    );
}

fn ignore_late(_summary: String) -> std::future::Ready<()> {
    std::future::ready(())
}

fn wait_bounds(start_ms: u64, total_ms: u64) -> InlineWaitBounds {
    InlineWaitBounds {
        start: std::time::Duration::from_millis(start_ms),
        total: std::time::Duration::from_millis(total_ms),
    }
}

/// A progress channel nothing ever publishes to. Hold the sender: a
/// dropped one is the wave finishing.
fn quiet_progress() -> (
    tokio::sync::watch::Sender<WaveProgress>,
    tokio::sync::watch::Receiver<WaveProgress>,
) {
    tokio::sync::watch::channel(WaveProgress::default())
}

fn queued_progress(reason: &str) -> WaveProgress {
    WaveProgress {
        stage: WaveStage::Queued,
        reason: Some(reason.into()),
        ..WaveProgress::default()
    }
}

fn running_progress(executor: &str) -> WaveProgress {
    WaveProgress {
        stage: WaveStage::Running,
        executor: Some(executor.into()),
        since_unix_ms: Some(unix_time_ms_for_checks()),
        reason: None,
        admitted: true,
    }
}

const HELD_BY_THE_BRAKE: &str = "declined by local (host pressure: cpu 97%)";

/// The case the start bound exists for: a lane no machine claims (the
/// local brake holding non-interactive work). The call answers at the
/// start bound, not the total, says what holds the lane, and the verdict
/// still reaches the agent late, exactly once.
#[tokio::test]
async fn a_wave_never_admitted_returns_pending_at_the_start_bound() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (progress, seen) = tokio::sync::watch::channel(queued_progress(HELD_BY_THE_BRAKE));
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (delivered_tx, mut delivered) = tokio::sync::mpsc::unbounded_channel::<String>();
    let verdict = "Checks: \u{2713} rust-fmt (0.4s)";

    let started = std::time::Instant::now();
    let summary = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        await_write_check_wave(
            guard,
            wait_bounds(50, 3_000),
            seen,
            async move {
                let _ = released.await;
                Some(verdict.to_string())
            },
            move |late: String| async move {
                let _ = delivered_tx.send(late);
            },
        ),
    )
    .await
    .expect("the committing call must answer, not wait for the checks")
    .expect("a pending wave still reports a checks line");
    let waited = started.elapsed();

    assert!(waited >= std::time::Duration::from_millis(50), "{waited:?}");
    assert!(
        waited < std::time::Duration::from_millis(3_000),
        "an unadmitted wave must not wait the total bound: {waited:?}"
    );
    assert!(
        summary.starts_with("Checks: \u{22EF} pending (not admitted after "),
        "{summary}"
    );
    assert!(summary.contains(HELD_BY_THE_BRAKE), "{summary}");
    assert!(summary.contains("the commit above landed"), "{summary}");
    assert!(summary.contains("cairn:~/checks"), "{summary}");
    assert!(summary.contains("[Write checks]"), "{summary}");
    assert!(
        orch.write_checks_in_flight("job-a"),
        "the detached wave still runs, so /checks must keep reporting it in flight",
    );

    drop(progress);
    release.send(()).unwrap();
    let late = tokio::time::timeout(std::time::Duration::from_secs(5), delivered.recv())
        .await
        .expect("the verdict of a wave answered pending must be delivered late")
        .expect("one delivery");
    assert_eq!(late, verdict);
    assert!(delivered.recv().await.is_none(), "delivered exactly once");
}

/// A lane a machine claimed is a verdict on its way, so the wait runs to
/// the total bound and names the machine.
#[tokio::test]
async fn an_admitted_wave_waits_to_the_total_bound() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (progress, seen) = quiet_progress();
    // Paused after the orchestrator is built, so the claim and both bounds
    // share one clock however loaded the host running the test is.
    tokio::time::pause();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        progress.send_replace(running_progress("bgsrv-mac"));
        std::future::pending::<()>().await;
    });

    let started = tokio::time::Instant::now();
    let summary = await_write_check_wave(
        guard,
        wait_bounds(80, 400),
        seen,
        std::future::pending::<Option<String>>(),
        ignore_late,
    )
    .await
    .unwrap();

    assert!(
        started.elapsed() >= std::time::Duration::from_millis(400),
        "{:?}",
        started.elapsed()
    );
    assert!(
        summary.starts_with("Checks: \u{22EF} pending (running on bgsrv-mac since "),
        "{summary}"
    );
    assert!(summary.contains("the commit above landed"), "{summary}");
}

/// Admission past the start bound's reach still extends the wait: a lane
/// claimed early whose verdict lands after the start bound is returned
/// inline, with nothing delivered late.
#[tokio::test]
async fn admission_extends_the_wait_and_the_verdict_returns_inline() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (progress, seen) = quiet_progress();
    let (delivered_tx, mut delivered) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::time::pause();

    let summary = await_write_check_wave(
        guard,
        wait_bounds(80, 3_000),
        seen,
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            progress.send_replace(running_progress("bgsrv-mac"));
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            Some("Checks: \u{2713} rust-fmt (0.3s)".to_string())
        },
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    )
    .await;

    assert_eq!(summary.as_deref(), Some("Checks: \u{2713} rust-fmt (0.3s)"));
    assert!(
        delivered.recv().await.is_none(),
        "an outcome returned inline is not also delivered late"
    );
}

/// Admission is latched. A wave that ran and went back to preparing (the
/// batch that re-verifies its fix) keeps the full wait, and expiring then
/// reads as neither "not admitted" nor "running".
#[tokio::test]
async fn admission_is_latched() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (progress, seen) = quiet_progress();
    tokio::time::pause();
    tokio::spawn(async move {
        progress.send_replace(running_progress("bgsrv-mac"));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        // The board's next snapshot: preparing the fix re-verification.
        progress.send_modify(|current| {
            *current = wave_progress(
                current,
                &[status_entry("pending")],
                &check_phase_status(
                    Some("provisioning"),
                    None,
                    Some("resolving build slot".into()),
                ),
            );
        });
        std::future::pending::<()>().await;
    });

    let started = tokio::time::Instant::now();
    let summary = await_write_check_wave(
        guard,
        wait_bounds(60, 300),
        seen,
        std::future::pending::<Option<String>>(),
        ignore_late,
    )
    .await
    .unwrap();

    assert!(
        started.elapsed() >= std::time::Duration::from_millis(300),
        "{:?}",
        started.elapsed()
    );
    assert!(
        summary.starts_with(
            "Checks: \u{22EF} pending (admitted but not running after 0s: resolving build slot)"
        ),
        "{summary}"
    );
    assert_eq!(
        classify_expired_wait(true, WaveStage::Preparing),
        InlineWaitOutcome::PendingAtBound
    );
    assert_eq!(
        classify_expired_wait(true, WaveStage::Running),
        InlineWaitOutcome::PendingRunning
    );
    assert_eq!(
        classify_expired_wait(false, WaveStage::Queued),
        InlineWaitOutcome::PendingNotStarted
    );
}

/// A wave answered entirely from the cache never reports progress and
/// settles at once; the call returns at once with its verdict.
#[tokio::test]
async fn a_cache_served_wave_answers_immediately() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (_progress, seen) = quiet_progress();

    let started = std::time::Instant::now();
    let summary = await_write_check_wave(
        guard,
        wait_bounds(5_000, 10_000),
        seen,
        async { Some("Checks: \u{2713} rust-fmt (cached)".to_string()) },
        ignore_late,
    )
    .await;

    assert_eq!(
        summary.as_deref(),
        Some("Checks: \u{2713} rust-fmt (cached)")
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}

/// The wave drops its progress sender as it finishes, a moment before its
/// outcome is offered. That must not end the wait: the outcome still comes
/// back inline.
#[tokio::test]
async fn progress_sender_dropping_does_not_end_the_wait_early() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (progress, seen) = quiet_progress();
    drop(progress);

    let summary = await_write_check_wave(
        guard,
        wait_bounds(1_000, 2_000),
        seen,
        async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Some("Checks: \u{2713} rust-fmt (0.1s)".to_string())
        },
        ignore_late,
    )
    .await;

    assert_eq!(summary.as_deref(), Some("Checks: \u{2713} rust-fmt (0.1s)"));
}

/// The receipt handshake at the new, short bound: a verdict offered in the
/// very instant the start bound expires is either returned inline or
/// delivered late, never both and never neither.
#[tokio::test]
async fn a_verdict_offered_as_the_start_bound_expires_reaches_the_agent_once() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (_progress, seen) = tokio::sync::watch::channel(queued_progress(HELD_BY_THE_BRAKE));
    let (delivered_tx, mut delivered) = tokio::sync::mpsc::unbounded_channel::<String>();
    let verdict = "Checks: \u{2713} rust-fmt (0.1s)";
    // Paused after the orchestrator is built, so its own I/O ran on real
    // time; from here the wave's timer and the start bound share one
    // instant.
    tokio::time::pause();
    let start = std::time::Duration::from_millis(50);

    let summary = await_write_check_wave(
        guard,
        wait_bounds(50, 5_000),
        seen,
        async move {
            tokio::time::sleep(start).await;
            Some(verdict.to_string())
        },
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    )
    .await
    .unwrap();

    let late: Vec<String> = {
        let mut late = Vec::new();
        while let Some(summary) = delivered.recv().await {
            late.push(summary);
        }
        late
    };
    if summary == verdict {
        assert!(
            late.is_empty(),
            "returned inline and delivered late: {late:?}"
        );
    } else {
        assert!(summary.contains("not admitted"), "{summary}");
        assert_eq!(late, vec![verdict.to_string()]);
    }
}

fn status_entry(state: &str) -> CheckStatusEntry {
    CheckStatusEntry {
        index: 0,
        name: "rust-fmt".into(),
        state: state.into(),
        annotation: None,
        cached: false,
        since_unix_ms: None,
    }
}

#[test]
fn wave_progress_reads_the_stage_from_the_board() {
    let start = WaveProgress::default();
    let queued = wave_progress(
        &start,
        &[status_entry("pending")],
        &CheckPhaseStatus {
            since_unix_ms: Some(PARKED_AT),
            held_by: Some(HELD_BY_THE_BRAKE.into()),
            ..check_phase_status(
                Some("queued"),
                Some("queued"),
                Some(format!(
                    "a long forecast; parked since 09:48:00Z; {HELD_BY_THE_BRAKE}"
                )),
            )
        },
    );
    assert_eq!(
        queued,
        WaveProgress {
            stage: WaveStage::Queued,
            executor: None,
            since_unix_ms: Some(PARKED_AT),
            reason: Some(HELD_BY_THE_BRAKE.into()),
            admitted: false,
        },
        "a queued wave names what holds it, not the whole diagnostic"
    );
    let undeclined = wave_progress(
        &start,
        &[status_entry("pending")],
        &check_phase_status(Some("queued"), Some("queued"), Some("the forecast".into())),
    );
    assert_eq!(undeclined.reason.as_deref(), Some("the forecast"));

    for phase in ["provisioning", "dispatching"] {
        let preparing = wave_progress(
            &start,
            &[status_entry("pending")],
            &check_phase_status(Some(phase), None, Some("resolving build slot".into())),
        );
        assert_eq!(preparing.stage, WaveStage::Preparing, "{phase}");
        assert_eq!(preparing.reason.as_deref(), Some("resolving build slot"));
    }

    let claimed = wave_progress(
        &queued,
        &[status_entry("pending")],
        &CheckPhaseStatus {
            executor: Some("bgsrv-mac".into()),
            since_unix_ms: Some(PARKED_AT + 9_000),
            ..check_phase_status(Some("running"), Some("running checks"), None)
        },
    );
    assert_eq!(claimed, running_at("bgsrv-mac", PARKED_AT + 9_000));

    let started = wave_progress(
        &claimed,
        &[status_entry("running")],
        &check_phase_status(Some("running"), Some("running checks"), None),
    );
    assert_eq!(
        started, claimed,
        "a lane's own start keeps the machine the claim named"
    );
    let started_unclaimed = wave_progress(
        &queued,
        &[status_entry("running")],
        &CheckPhaseStatus::default(),
    );
    assert_eq!(started_unclaimed.stage, WaveStage::Running);
    assert_eq!(started_unclaimed.executor, None);
    assert!(started_unclaimed.admitted);

    let reverifying = wave_progress(
        &claimed,
        &[status_entry("pending")],
        &check_phase_status(
            Some("provisioning"),
            None,
            Some("resolving build slot".into()),
        ),
    );
    assert_eq!(reverifying.stage, WaveStage::Preparing);
    assert!(
        reverifying.admitted,
        "admission is latched: a wave that ran stays admitted while it prepares its fix re-verification"
    );
    let requeued = wave_progress(
        &reverifying,
        &[status_entry("pending")],
        &check_phase_status(Some("queued"), Some("queued"), None),
    );
    assert!(requeued.admitted);

    let settled = wave_progress(
        &claimed,
        &[status_entry("passed")],
        &CheckPhaseStatus::default(),
    );
    assert_eq!(settled, claimed, "a settled board keeps the last stage");
}

fn running_at(executor: &str, since_unix_ms: u64) -> WaveProgress {
    WaveProgress {
        stage: WaveStage::Running,
        executor: Some(executor.into()),
        since_unix_ms: Some(since_unix_ms),
        reason: None,
        admitted: true,
    }
}

/// The real path from the fleet's dispatch facts to the inline wait: the
/// narrator renders them, the write wave's board shows them (`set_phase`
/// and `show_phase` both publish), and the board's sink, built as
/// `run_write_checks_after_seal_inner` builds it, feeds the progress the
/// wait reads. Were any link missing, a lane really running would be cut
/// at the start bound as "not admitted".
#[tokio::test(start_paused = true)]
async fn the_write_wave_board_feeds_its_dispatch_to_the_inline_wait() {
    let (progress, mut seen) = tokio::sync::watch::channel(WaveProgress::default());
    let emitted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let emitted_by_sink = emitted.clone();
    let plans = vec![(plan("rust-fmt", "true"), "input".to_string())];
    let board = CheckStatusBoard::new(
        &plans,
        wave_status_notify(
            move |_, phase: CheckPhaseStatus| {
                emitted_by_sink.lock().unwrap().push(phase);
            },
            progress,
        ),
    );

    board.set_phase(
        Some("provisioning"),
        Some("preparing build slot"),
        Some("resolving build slot".into()),
    );
    assert!(seen.has_changed().unwrap());
    assert_eq!(seen.borrow_and_update().stage, WaveStage::Preparing);

    let held = crate::fleet::CheckDispatch {
        phase: crate::fleet::CheckDispatchPhase::Placing {
            since_unix_ms: PARKED_AT,
            turn: None,
            placement: None,
            declines: vec![crate::fleet::CheckDispatchDecline {
                executor_name: "local".into(),
                reason: cairn_common::executor_protocol::DeclineReason::Pressure,
                detail: Some("cpu 97%".into()),
                at_unix_ms: PARKED_AT + 1_000,
                superseded_at_unix_ms: None,
                retry_after_unix_ms: PARKED_AT + 2_000,
            }],
        },
        joined: None,
    };
    let reads = std::sync::atomic::AtomicUsize::new(0);
    let queued_seen = std::sync::Mutex::new(None);
    narrate_dispatch(
        tokio::time::sleep(std::time::Duration::from_secs(5)),
        &GroupWait::from_occupancy(predicted(292_000, 1)),
        || {
            Some(
                match reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    0 => held.clone(),
                    _ => running_on("bgsrv-mac", PARKED_AT + 9_000, None),
                },
            )
        },
        || -> GroupWait { unreachable!("the forecast stays fresh for 30s") },
        |status| {
            board.show_phase(status);
            queued_seen
                .lock()
                .unwrap()
                .get_or_insert_with(|| seen.borrow().clone());
        },
    )
    .await;

    assert_eq!(
        queued_seen.into_inner().unwrap(),
        Some(WaveProgress {
            stage: WaveStage::Queued,
            executor: None,
            since_unix_ms: Some(PARKED_AT),
            reason: Some(HELD_BY_THE_BRAKE.into()),
            admitted: false,
        }),
        "a held lane's decline reaches the wait as its reason"
    );
    assert_eq!(
        *seen.borrow_and_update(),
        running_at("bgsrv-mac", PARKED_AT + 9_000),
        "a claim reaches the wait as running, with the machine"
    );

    board.transition(0, "running", None);
    assert_eq!(*seen.borrow(), running_at("bgsrv-mac", PARKED_AT + 9_000));
    let shown = emitted.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        (shown.executor.as_deref(), shown.since_unix_ms),
        (Some("bgsrv-mac"), Some(PARKED_AT + 9_000)),
        "a lane's start (and each output chunk) keeps the machine on the status row"
    );
    board.transition(0, "passed", Some("0.4s".into()));
    assert_eq!(seen.borrow().stage, WaveStage::Running);
    assert!(
        emitted
            .lock()
            .unwrap()
            .iter()
            .any(|phase| phase.phase.as_deref() == Some("queued")),
        "the UI sink still receives every snapshot"
    );
}

#[test]
fn group_status_board_keeps_concurrent_runners_and_blockers_local() {
    use crate::execution::checks_status::{grouped_lane_progress, LaneProgress};
    let plans = [
        "linux-run",
        "mac-run",
        "linux-next",
        "mac-next",
        "unscheduled",
    ]
    .into_iter()
    .map(|name| (plan(name, "true"), "input".into()))
    .collect::<Vec<_>>();
    let board = CheckStatusBoard::for_review_wave(&plans, || {});
    let linux = board.for_group(&[0, 2]);
    let mac = board.for_group(&[1, 3]);
    linux.show_phase(CheckPhaseStatus {
        phase: Some("running".into()),
        executor: Some("linux-box".into()),
        since_unix_ms: Some(100),
        ..Default::default()
    });
    mac.show_phase(CheckPhaseStatus {
        phase: Some("running".into()),
        executor: Some("mac-box".into()),
        since_unix_ms: Some(200),
        ..Default::default()
    });
    linux.transition(0, "running", None);
    mac.transition(1, "running", None);
    assert_eq!(
        board
            .snapshot()
            .iter()
            .filter(|entry| entry.state == "running")
            .count(),
        2
    );
    for (name, behind, machine) in [
        ("linux-next", "linux-run", "linux-box"),
        ("mac-next", "mac-run", "mac-box"),
    ] {
        assert_eq!(
            grouped_lane_progress(&board, name, None),
            Some(LaneProgress::Queued {
                behind: Some(behind.into())
            })
        );
        assert_eq!(board.lane_phase(name).executor.as_deref(), Some(machine));
    }
    assert_eq!(
        grouped_lane_progress(&board, "unscheduled", None),
        Some(LaneProgress::Queued { behind: None })
    );
    assert_eq!(board.executor(), None);
    let since = board.snapshot()[0].since_unix_ms;
    linux.transition(0, "running", None);
    assert_eq!(board.snapshot()[0].since_unix_ms, since);
    assert_eq!(board.lane_phase("linux-run").since_unix_ms, Some(100));

    linux.transition(
        0,
        "finished",
        Some("finished in 2s (exit 0); awaiting recording".into()),
    );
    assert_eq!(board.snapshot()[0].state, "finished");
    assert!(grouped_lane_progress(&board, "linux-run", None)
        .unwrap()
        .describe(300)
        .contains("awaiting recording"));
    // Recording one group neither finishes nor changes a sibling's claim.
    board.transition(0, "passed", None);
    assert_eq!(board.snapshot()[0].state, "passed");
    assert_eq!(
        board.lane_phase("linux-run").executor.as_deref(),
        Some("linux-box")
    );
    board.transition(2, "unverified", None);
    assert_eq!(board.lane_phase("linux-run"), CheckPhaseStatus::default());
    assert_eq!(board.snapshot()[1].state, "running");
    assert_eq!(
        board.lane_phase("mac-run").executor.as_deref(),
        Some("mac-box")
    );
}

#[test]
fn group_status_board_projects_a_held_groups_own_dispatch_phase() {
    use crate::execution::checks_status::{grouped_lane_progress, LaneProgress};
    let plans = ["busy", "held"]
        .into_iter()
        .map(|name| (plan(name, "true"), "input".into()))
        .collect::<Vec<_>>();
    let board = CheckStatusBoard::for_review_wave(&plans, || {});
    let busy = board.for_group(&[0]);
    let held = board.for_group(&[1]);
    busy.show_phase(CheckPhaseStatus {
        phase: Some("running".into()),
        executor: Some("busy-box".into()),
        ..Default::default()
    });
    busy.transition(0, "running", None);
    held.show_phase(CheckPhaseStatus {
        phase: Some("queued".into()),
        label: Some("waiting for macOS capacity".into()),
        diagnostic: Some("mac-box declined: memory pressure".into()),
        since_unix_ms: Some(7),
        ..Default::default()
    });
    let progress = grouped_lane_progress(&board, "held", None).unwrap();
    assert_eq!(
        progress,
        LaneProgress::Dispatching {
            dispatch_phase: "queued".into(),
            label: Some("waiting for macOS capacity".into()),
            diagnostic: Some("mac-box declined: memory pressure".into()),
            since_unix_ms: Some(7),
        }
    );
    assert_eq!(board.lane_phase("held").executor, None);
    assert_eq!(
        board.lane_phase("busy").executor.as_deref(),
        Some("busy-box")
    );
    let json = serde_json::to_value(progress).unwrap();
    assert_eq!(json["phase"], "dispatching");
    assert_eq!(json["dispatchPhase"], "queued");
    assert_eq!(json["sinceUnixMs"], 7);
}

/// A `tree: impact` lane runs on the runner and never meets the fleet's
/// queue, so the narrator never reports it claimed. It reports its own
/// start instead; without that its wave would read as never admitted and
/// be cut at the start bound while the lane ran.
#[tokio::test]
async fn a_runner_lane_marks_its_wave_admitted_as_it_starts() {
    let (progress, seen) = tokio::sync::watch::channel(WaveProgress::default());
    let plans = vec![(plan("migrations", "true"), "input".to_string())];
    let board = CheckStatusBoard::new(&plans, wave_status_notify(|_, _| {}, progress));
    let temp = TempDir::new().unwrap();
    let request = PlannedCheckBatchRequest {
        project_id: "project".into(),
        // No repository: the lane fails to materialize, after its start.
        repository: temp.path().join("absent").to_string_lossy().into_owned(),
        store_dir: temp.path().join("store"),
        sealed_commit: "commit".into(),
        requesting_job_id: "job".into(),
        owner: cairn_common::executor_protocol::CellOwnerRef {
            project_id: "project".into(),
            project_key: None,
            issue_number: None,
            job_id: Some("job".into()),
            execution_seq: None,
            node_kind: None,
        },
        affinity_key: None,
        priority: CellPriority::WriteCheck,
        env: Vec::new(),
        items: Vec::new(),
        run_context: None,
        mutation_policy: MutationPolicy::PureVerdict,
        status_board: Some(board),
        on_group_complete: None,
        purpose: None,
        cell_groups: Default::default(),
        publication: None,
    };

    let results = crate::execution::impact_slice::run_lanes(
        &request,
        vec![(
            batch_item("true", CheckResourceClass::Shared),
            vec!["migrations/**".into()],
        )],
    )
    .await;

    assert!(results.contains_key(&0));
    let progress = seen.borrow().clone();
    assert!(progress.admitted, "{progress:?}");
    assert_eq!(progress.stage, WaveStage::Running);
}

#[test]
fn the_pending_line_says_why_the_checks_are_pending() {
    let pending = |outcome, waited_ms, progress| PendingWave {
        outcome,
        waited: std::time::Duration::from_millis(waited_ms),
        progress,
    };
    let not_admitted = pending_wave_reason(
        &pending(
            InlineWaitOutcome::PendingNotStarted,
            15_004,
            queued_progress(HELD_BY_THE_BRAKE),
        ),
        PARKED_AT,
    );
    assert_eq!(
        not_admitted,
        format!("not admitted after 15s: {HELD_BY_THE_BRAKE}")
    );
    assert_eq!(
        pending_wave_reason(
            &pending(
                InlineWaitOutcome::PendingNotStarted,
                15_000,
                WaveProgress {
                    stage: WaveStage::Preparing,
                    ..WaveProgress::default()
                },
            ),
            PARKED_AT,
        ),
        "not admitted after 15s: still preparing the check's inputs"
    );
    assert_eq!(
        pending_wave_reason(
            &pending(
                InlineWaitOutcome::PendingNotStarted,
                15_000,
                WaveProgress::default(),
            ),
            PARKED_AT,
        ),
        "not admitted after 15s: still planning"
    );
    assert_eq!(
        pending_wave_reason(
            &pending(
                InlineWaitOutcome::PendingRunning,
                120_000,
                running_at("bgsrv-mac", PARKED_AT),
            ),
            PARKED_AT + 100_000,
        ),
        "running on bgsrv-mac since 09:48:00Z, 1m40s in"
    );
    assert_eq!(
        pending_wave_reason(
            &pending(
                InlineWaitOutcome::PendingAtBound,
                120_000,
                WaveProgress {
                    stage: WaveStage::Queued,
                    ..WaveProgress::default()
                },
            ),
            PARKED_AT,
        ),
        "admitted but not running after 2m: queued, and no machine has claimed it"
    );

    let line = pending_write_checks_summary(&not_admitted);
    assert!(
        line.starts_with(&format!(
            "Checks: \u{22EF} pending (not admitted after 15s: {HELD_BY_THE_BRAKE}) \u{2014} \
                 the commit above landed;"
        )),
        "{line}"
    );
    assert!(line.contains("cairn:~/checks"), "{line}");
    assert!(line.contains("[Write checks]"), "{line}");
}

/// After the call answered pending, the wave's final word may exist nowhere
/// else: a fold conflict, say, with the patch the agent needs to replay. It
/// must reach the late-delivery path rather than be dropped with the handle.
#[tokio::test]
async fn a_wave_that_fails_after_answering_pending_delivers_its_outcome() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (delivered_tx, delivered) = tokio::sync::oneshot::channel::<String>();
    let fold_failure =
        "Checks: \u{2717} write-check fold (conflict in src/a.rs)\n```diff\n-x\n+y\n```";

    let (_progress, seen) = quiet_progress();
    let summary = await_write_check_wave(
        guard,
        wait_bounds(20, 20),
        seen,
        async move {
            let _ = released.await;
            Some(fold_failure.to_string())
        },
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    )
    .await
    .unwrap();
    assert!(summary.contains("pending"), "{summary}");

    release.send(()).unwrap();
    let late = tokio::time::timeout(std::time::Duration::from_secs(5), delivered)
        .await
        .expect("the late outcome must be delivered")
        .unwrap();
    assert_eq!(late, fold_failure);
    let message = late_write_check_message(&late);
    assert!(message.contains("conflict in src/a.rs"), "{message}");
    assert!(message.contains("cairn:~/checks"), "{message}");
}

/// The handoff race: the outcome is buffered in the caller's receiver, and
/// the caller is cancelled before it ever polls it. The buffered value dies
/// with the receiver, so the supervisor must not count the successful send
/// as delivery; without a receipt it delivers late, exactly once.
#[tokio::test]
async fn an_outcome_buffered_for_a_caller_cancelled_before_reading_it_is_delivered_late() {
    let (offer, inline) = tokio::sync::oneshot::channel::<OfferedOutcome>();
    let (delivered_tx, mut delivered) = tokio::sync::mpsc::unbounded_channel::<String>();
    let fold_failure = "Checks: \u{2717} write-check fold (conflict in src/a.rs)";
    let supervisor = tokio::spawn(hand_off_wave_outcome(
        offer,
        Some(fold_failure.to_string()),
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    ));
    // Current-thread runtime: one yield runs the supervisor up to its wait
    // for the receipt, so the outcome is now buffered and unread.
    tokio::task::yield_now().await;
    assert!(!supervisor.is_finished(), "the supervisor awaits a receipt");
    drop(inline);

    supervisor.await.unwrap();
    assert_eq!(delivered.recv().await.as_deref(), Some(fold_failure));
    assert!(delivered.recv().await.is_none(), "delivered exactly once");
}

/// The ordinary path: the caller claims the outcome, and nothing is
/// delivered late.
#[tokio::test]
async fn a_claimed_outcome_is_not_also_delivered_late() {
    let (offer, inline) = tokio::sync::oneshot::channel::<OfferedOutcome>();
    let (delivered_tx, mut delivered) = tokio::sync::mpsc::unbounded_channel::<String>();
    let supervisor = tokio::spawn(hand_off_wave_outcome(
        offer,
        Some("Checks: \u{2713} fmt".to_string()),
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    ));
    let claimed = claim_offered_outcome(inline.await.unwrap());
    assert_eq!(claimed.as_deref(), Some("Checks: \u{2713} fmt"));
    supervisor.await.unwrap();
    assert!(delivered.recv().await.is_none());
}

/// A wave that answered inside the bound has nothing late to deliver.
#[tokio::test]
async fn a_wave_that_settles_in_time_delivers_nothing_late() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (delivered_tx, mut delivered) = tokio::sync::oneshot::channel::<String>();
    let (_progress, seen) = quiet_progress();
    await_write_check_wave(
        guard,
        wait_bounds(5_000, 5_000),
        seen,
        async { Some("Checks: \u{2713} fmt".to_string()) },
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    )
    .await;
    tokio::task::yield_now().await;
    assert!(delivered.try_recv().is_err());
}

/// A wave that settles inside the bound returns its verdict line unchanged,
/// and its in-flight state clears with it.
#[tokio::test]
async fn a_write_check_wave_that_settles_in_time_returns_its_verdicts() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");

    let (_progress, seen) = quiet_progress();
    let summary = await_write_check_wave(
        guard,
        wait_bounds(5_000, 5_000),
        seen,
        async { Some("Checks: \u{2713} rust-fmt (0.4s)".to_string()) },
        ignore_late,
    )
    .await;

    assert_eq!(summary.as_deref(), Some("Checks: \u{2713} rust-fmt (0.4s)"));
    assert!(!orch.write_checks_in_flight("job-a"));
}

/// Dropping the caller before the bound (a transport timeout, an interrupt)
/// must not cancel checks for a commit that already landed, and must not
/// lose what the wave finally says: the outcome reaches late delivery
/// exactly once, and only then does the job's in-flight state clear.
#[tokio::test]
async fn dropping_the_caller_leaves_the_write_check_wave_running() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let guard = WriteChecksInFlightGuard::new(&orch, "job-a");
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (delivered_tx, mut delivered) = tokio::sync::mpsc::unbounded_channel::<String>();
    let fold_failure =
        "Checks: \u{2717} write-check fold (non-convergent: verification mutated again)";

    let (_progress, seen) = quiet_progress();
    let caller = tokio::spawn(await_write_check_wave(
        guard,
        wait_bounds(60_000, 60_000),
        seen,
        async move {
            let _ = released.await;
            Some(fold_failure.to_string())
        },
        move |late: String| async move {
            let _ = delivered_tx.send(late);
        },
    ));
    tokio::task::yield_now().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(orch.write_checks_in_flight("job-a"));

    release.send(()).unwrap();
    let late = tokio::time::timeout(std::time::Duration::from_secs(5), delivered.recv())
        .await
        .expect("the outcome of a wave whose caller is gone must still be delivered")
        .expect("one delivery");
    assert_eq!(late, fold_failure);
    assert!(
        delivered.recv().await.is_none(),
        "the outcome is delivered exactly once"
    );
    for _ in 0..100 {
        if !orch.write_checks_in_flight("job-a") {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the finished wave must release the job's in-flight state");
}

#[test]
fn the_inline_write_check_wait_sits_inside_the_write_callback_contract() {
    let write = cairn_common::protocol::CallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".into(),
        run_id: None,
        tool: "write".into(),
        payload: serde_json::json!({ "changes": [{ "target": "file:a", "mode": "create" }] }),
        tool_use_id: None,
        origin: None,
    };
    let cairn_common::callback_contract::HostAnswerBound::Bounded(host) =
        cairn_common::callback_contract::host_answer_bound(&write)
    else {
        panic!("a plain write has a bounded host answer");
    };
    assert!(host >= WRITE_CHECK_INLINE_WAIT);
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .expect("git should run in cache-key fixture");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn git_tree_entries(repo: &Path, revision: &str) -> Vec<(String, String)> {
    git(repo, &["ls-tree", "-r", revision])
        .lines()
        .map(|line| {
            let (metadata, path) = line.split_once('\t').expect("ls-tree row has a path");
            let blob = metadata
                .split_whitespace()
                .nth(2)
                .expect("ls-tree row has an object id");
            (path.to_string(), blob.to_string())
        })
        .collect()
}

fn check_ref_exists(repo: &Path, reference: &str) -> bool {
    Command::new("git")
        .args(["show-ref", "--verify", "--quiet", reference])
        .current_dir(repo)
        .status()
        .unwrap()
        .success()
}

fn committed_git_repo(temp: &TempDir) -> (std::path::PathBuf, String) {
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-b", "main", "-q"]);
    std::fs::write(repo.join("checked.txt"), "sealed").unwrap();
    git(&repo, &["add", "checked.txt"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Cairn Test",
            "-c",
            "user.email=test@cairn.local",
            "commit",
            "-q",
            "-m",
            "sealed",
        ],
    );
    let commit = git(&repo, &["rev-parse", "HEAD"]);
    (repo, commit)
}

#[tokio::test]
async fn dirty_checkout_cannot_change_a_sealed_check_coordinate() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, _) = committed_git_repo(&temp);
    std::fs::create_dir_all(repo.join(".cairn")).unwrap();
    std::fs::write(
        repo.join(".cairn/config.yaml"),
        "checks:\n  sealed:\n    command: grep sealed checked.txt\n    impact:\n      - checked.txt\n",
    )
    .unwrap();
    git(&repo, &["add", ".cairn/config.yaml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Cairn Test",
            "-c",
            "user.email=test@cairn.local",
            "commit",
            "-q",
            "-m",
            "sealed contract",
        ],
    );
    let sealed_commit = git(&repo, &["rev-parse", "HEAD"]);
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);

    async fn identity(
        jj: &JjEnv,
        repo: &Path,
        commit: &str,
    ) -> (String, String, String, CheckPlan) {
        let CommitChecksContract {
            contract:
                ChecksContract {
                    checks,
                    extra_inputs,
                    ..
                },
            defined_by_commit,
        } = load_checks_contract_at_commit_detailed(repo, commit)
            .await
            .unwrap_or_else(|error| panic!("{}", error.manual_diagnostic(commit)));
        let tree_hash = crate::jj::logical_tree_hash(jj, repo, commit).unwrap();
        let entries = crate::jj::tree_entries(jj, repo, commit).ok();
        let blobs = TreeBlobs {
            jj,
            repository: repo,
        };
        let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
        let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
        let (plan, input_hash) = manual_check_plan(
            &checks,
            &inputs,
            entries.as_deref(),
            &tree_hash,
            "sealed",
            repo,
            ChangeBasis::Unknown,
        )
        .expect("sealed check is planned from the sealed contract");
        (defined_by_commit, tree_hash, input_hash, plan)
    }

    let clean = identity(&jj, &repo, &sealed_commit).await;
    std::fs::write(repo.join("checked.txt"), "loose poison").unwrap();
    std::fs::write(
        repo.join(".cairn/config.yaml"),
        "checks:\n  sealed:\n    command: false\n    impact:\n      - checked.txt\n",
    )
    .unwrap();
    let dirty = identity(&jj, &repo, &sealed_commit).await;
    assert_eq!(
        (&clean.0, &clean.1, &clean.2),
        (&dirty.0, &dirty.1, &dirty.2),
        "loose files cannot alter sealed ownership"
    );
    assert_eq!(
        dirty.3.command, clean.3.command,
        "loose config cannot replace the sealed command"
    );

    let db = cache_db().await;
    let plans = vec![(clean.3.clone(), clean.2.clone())];
    let coordinates = vec![crate::execution::cache::SealedCheckCoordinate::new(
        &sealed_commit,
        &clean.0,
        &clean.1,
        &clean.2,
    )];
    let outcomes = run_planned_checks_at_commit_with_coordinates(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: &sealed_commit,
            defined_by: &clean.0,
        },
        &clean.1,
        "job-a",
        &plans,
        &coordinates,
        &head_infra_scopes(plans.len(), "job-a", &sealed_commit),
        "manual-check:sealed",
        CheckExecMode::Shared,
        None,
        |_index, command, _stream_id| async move {
            assert_eq!(command, "grep sealed checked.txt");
            exec_ok(Some(0), "sealed content matched")
        },
        |_| {},
    )
    .await;
    assert!(outcomes[0].passed);
    outcomes[0]
        .recorded
        .as_ref()
        .expect("sealed verdict recorded");
    let stored = get_check_result(db.clone(), "project-a", "sealed", &clean.2)
        .unwrap()
        .expect("sealed coordinate is reloadable");
    assert_eq!(stored.tree_hash, clean.1);
    assert_eq!(stored.input_hash, clean.2);
    assert_eq!(
        stored.defined_by_commit_sha.as_deref(),
        Some(sealed_commit.as_str())
    );

    git(&repo, &["add", "checked.txt", ".cairn/config.yaml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Cairn Test",
            "-c",
            "user.email=test@cairn.local",
            "commit",
            "-q",
            "-m",
            "seal poison",
        ],
    );
    let changed_commit = git(&repo, &["rev-parse", "HEAD"]);
    let changed = identity(&jj, &repo, &changed_commit).await;
    assert_ne!(
        clean.1, changed.1,
        "a newly sealed tree has a new tree hash"
    );
    assert_ne!(
        clean.2, changed.2,
        "a newly sealed contract/input has a new input hash"
    );
    assert!(
        get_check_result(db, "project-a", "sealed", &changed.2)
            .unwrap()
            .is_none(),
        "loose-derived identity was never persisted"
    );
}

/// A selector check's runner, standing in for `test-rust-nextest.ts related`:
/// handed the literal `{changedFiles}` it selects nothing (the token names no
/// file), and handed its whole-suite flag it runs and passes a test.
const SELECTOR_FIXTURE_SCRIPT: &str = r#"if [ "$1" = "--all" ]; then
  echo 'running the whole suite'
  echo 'CAIRN_CHECK_RESULT_JSON={"schemaVersion":1,"parser":"nextest","complete":true,"selection":"full","undeclaredSkips":0,"tests":[{"id":"fixture sel::passes","status":"passed","attempts":1,"retried":false,"flaky":false}]}'
else
  echo "no tests selected for $1"
  echo 'CAIRN_CHECK_RESULT_JSON={"schemaVersion":1,"parser":"nextest","complete":true,"selection":"empty","undeclaredSkips":0,"tests":[]}'
fi
"#;

/// A manual run of a selector check executes real tests. Planned against an
/// empty change set, the configured command used to reach the shell with its
/// placeholder unexpanded, so the runner selected nothing and every manual
/// verdict was recorded as an empty, non-reusable selection.
#[tokio::test]
async fn manual_selector_check_executes_its_whole_suite_with_nonzero_tests() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, _) = committed_git_repo(&temp);
    std::fs::write(repo.join("sel.sh"), SELECTOR_FIXTURE_SCRIPT).unwrap();
    std::fs::create_dir_all(repo.join(".cairn")).unwrap();
    std::fs::write(
        repo.join(".cairn/config.yaml"),
        "checks:\n  selected:\n    command: sh sel.sh {changedFiles}\n    fullCommand: sh sel.sh --all\n    impact:\n      - sel.sh\n",
    )
    .unwrap();
    git(&repo, &["add", "sel.sh", ".cairn/config.yaml"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Cairn Test",
            "-c",
            "user.email=test@cairn.local",
            "commit",
            "-q",
            "-m",
            "selector check",
        ],
    );
    let commit = git(&repo, &["rev-parse", "HEAD"]);
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let CommitChecksContract {
        contract:
            ChecksContract {
                checks,
                extra_inputs,
                ..
            },
        defined_by_commit,
    } = load_checks_contract_at_commit(&repo, &commit)
        .await
        .expect("the commit declares the selector check");
    let tree_hash = crate::jj::logical_tree_hash(&jj, &repo, &commit).unwrap();
    let entries = crate::jj::tree_entries(&jj, &repo, &commit).ok();
    let blobs = TreeBlobs {
        jj: &jj,
        repository: &repo,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let (plan, key) = manual_check_plan(
        &checks,
        &inputs,
        entries.as_deref(),
        &tree_hash,
        "selected",
        &repo,
        ChangeBasis::Unknown,
    )
    .expect("a selector check is runnable manually");

    let db = cache_db().await;
    let coordinates = vec![crate::execution::cache::SealedCheckCoordinate::new(
        &commit,
        &defined_by_commit,
        &tree_hash,
        &key,
    )];
    let plans = vec![(plan, key)];
    let workdir = repo.clone();
    let outcomes = run_planned_checks_at_commit_with_coordinates(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: &commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        "job-a",
        &plans,
        &coordinates,
        &head_infra_scopes(plans.len(), "job-a", &commit),
        "manual-check:selected",
        CheckExecMode::Shared,
        None,
        move |_index, command, _stream_id| {
            let workdir = workdir.clone();
            async move {
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(&command)
                    .current_dir(&workdir)
                    .output()
                    .expect("fixture command spawns");
                exec_ok(
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout).into_owned(),
                )
            }
        },
        |_| {},
    )
    .await;

    let outcome = &outcomes[0];
    let parsed = outcome
        .parsed
        .as_ref()
        .expect("the fixture runner emits a structured result");
    assert_ne!(
        parsed.selection, "empty",
        "a manual run must not execute the raw placeholder: {}",
        outcome.output_tail
    );
    assert!(parsed.tests_run() > 0, "{}", outcome.output_tail);
    assert!(outcome.passed, "{}", outcome.output_tail);
    let recorded = outcome
        .recorded
        .as_ref()
        .expect("the manual verdict is recorded");
    assert!(
        recorded.reusable,
        "a whole-suite pass is reusable evidence: {:?}",
        outcome.not_recorded
    );
}

fn rust_tests_shaped_checks() -> HashMap<String, CheckCommand> {
    let mut configured = check(
        "bun run test:rust {changedFiles}",
        Some(&["src-tauri/**"]),
        CheckWhen::Review,
    );
    configured.full_command = Some("bun run test:rust:full".to_string());
    HashMap::from([("rust-tests".to_string(), configured)])
}

/// A manual run evaluates a sealed tree the way main-health does: it runs the
/// whole suite and keys the verdict by the command it executed, so a manual
/// `--branch main` run and a main-health run of the same tree answer each
/// other, while a turn-end partial verdict never answers for either.
#[test]
fn manual_rust_tests_plan_runs_the_whole_suite() {
    let checks = rust_tests_shaped_checks();
    let resolved = inputs(&checks);
    let entries = [(
        "src-tauri/os/cairn-core/src/lib.rs".to_string(),
        "blob".to_string(),
    )];
    let temp = TempDir::new().unwrap();
    let (plan, key) = manual_check_plan(
        &checks,
        &resolved,
        Some(&entries),
        "tree",
        "rust-tests",
        temp.path(),
        ChangeBasis::Unknown,
    )
    .expect("rust-tests is runnable manually");
    assert_eq!(plan.command, "bun run test:rust:full");
    assert_eq!(plan.scope, CheckScope::Full);
    assert!(plan.config_error.is_none());

    let (_, main_health_key) = key_review_tree_plans(
        vec![plan.clone()],
        &checks,
        &resolved,
        &entries,
        "tree",
        &check_platform_identity(),
        check_toolchain_identity(),
        ChangeBasis::Unknown,
    )
    .pop()
    .unwrap();
    assert_eq!(key, main_health_key, "one sealed-tree evaluation, one key");
    assert_ne!(
        key,
        check_input_hash(
            &checks["rust-tests"],
            resolved.for_check("rust-tests"),
            Some(&entries),
            "tree",
            CheckChange::unknown("test"),
        ),
        "execution breadth is evidence identity"
    );
}

/// A command with two selectors has no whole-suite form, so a manual run
/// refuses it as unrunnable instead of executing a leftover literal.
#[test]
fn manual_run_refuses_a_command_with_two_selectors() {
    let temp = TempDir::new().unwrap();
    let configured = check(
        "tool {targets} {changedFiles}",
        Some(&["src-tauri/**"]),
        CheckWhen::Review,
    );
    let checks = HashMap::from([("mixed".to_string(), configured)]);
    let error = manual_check_plan(
        &checks,
        &inputs(&checks),
        None,
        "tree",
        "mixed",
        temp.path(),
        ChangeBasis::Unknown,
    )
    .expect_err("two selectors have no whole-suite form");
    assert!(error.contains("is unrunnable"), "{error}");
}

/// A selector check that cannot narrow executes its `fullCommand`, so that
/// command keys the verdict: editing it alone must not let a verdict of the
/// old command answer for the new one.
#[test]
fn full_command_is_part_of_a_checks_identity() {
    let key = |full: Option<&str>| {
        let mut configured = check(
            "bun run test:rust {changedFiles}",
            Some(&["src-tauri/**"]),
            CheckWhen::Review,
        );
        configured.full_command = full.map(str::to_string);
        check_result_key(
            &configured,
            &selector(&configured),
            None,
            "tree",
            TEST_PLATFORM,
            TEST_TOOLCHAIN,
            CheckChange::unknown("test"),
        )
    };
    assert_ne!(key(Some("tool full-a")), key(Some("tool full-b")));
    assert_ne!(key(None), key(Some("tool full-a")));
    assert_eq!(key(Some("tool full-a")), key(Some("tool full-a")));
}

/// For a placeholder-free check the executed command IS the configured one,
/// so the manual key stays byte-identical to the lane key and no stored
/// verdict is orphaned.
#[test]
fn manual_key_is_unchanged_for_placeholder_free_checks() {
    let entries = [(
        "src-tauri/os/cairn-core/src/lib.rs".to_string(),
        "blob".to_string(),
    )];
    let temp = TempDir::new().unwrap();
    for impact in [Some(&["src-tauri/**/*.rs"][..]), None] {
        let configured = check("cargo test", impact, CheckWhen::Review);
        let checks = HashMap::from([("rust".to_string(), configured.clone())]);
        let resolved = inputs(&checks);
        let (plan, key) = manual_check_plan(
            &checks,
            &resolved,
            Some(&entries),
            "tree",
            "rust",
            temp.path(),
            ChangeBasis::Unknown,
        )
        .expect("a placeholder-free check is runnable manually");
        assert_eq!(plan.command, "cargo test");
        assert_eq!(
            key,
            check_input_hash(
                &configured,
                resolved.for_check("rust"),
                Some(&entries),
                "tree",
                CheckChange::unknown("test")
            ),
            "impact {impact:?}"
        );
    }
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !done() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

#[tokio::test]
async fn post_publication_verification_failure_removes_temporary_check_ref() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let request_id = "verification-failure";
    let reference = format!("refs/cairn/checks/{request_id}");

    let error = publish_check_commit_ref_with_verifier(
        &orch,
        &repo,
        &store,
        &commit,
        request_id,
        |_repo, _reference| Err("injected post-publication verification failure".into()),
    )
    .await
    .unwrap_err();

    assert!(error.contains("injected post-publication verification failure"));
    wait_until("the unverified ref is retired", || {
        !check_ref_exists(&repo, &reference)
    })
    .await;
}

/// Publishing a sealed check ref is invisible to jj and takes no
/// `packed-refs.lock`, so it does not queue behind whoever holds the store.
#[tokio::test]
async fn publishing_a_check_ref_does_not_wait_for_the_store_lock() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let _held = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Foreground,
            "long publication",
        )
        .await;
    let published = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        publish_check_commit_ref(&orch, &repo, &store, &commit, "unlocked"),
    )
    .await
    .expect("publication must not wait on the store lock")
    .expect("the sealed check reference publishes");
    assert!(check_ref_exists(&repo, "refs/cairn/checks/unlocked"));
    assert!(check_ref_is_held("refs/cairn/checks/unlocked"));
    std::mem::forget(published);
}

/// `git update-ref -d` always rewrites `packed-refs`, so retiring a sealed
/// check reference contends for the repository's one `packed-refs.lock` —
/// and git does not wait for that lock at all, it fails immediately. Losing
/// that race is what discarded the verdicts of two review lanes that had
/// both already finished, so the deletion has to be the patient one.
#[tokio::test]
async fn sealed_check_ref_delete_waits_for_a_held_then_released_packed_refs_lock() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let request_id = "held-packed-refs";
    let reference = format!("refs/cairn/checks/{request_id}");

    let mut published = publish_check_commit_ref(&orch, &repo, &store, &commit, request_id)
        .await
        .expect("the sealed check reference publishes");
    assert!(check_ref_exists(&repo, &reference));

    let lock = repo.join(".git").join("packed-refs.lock");
    std::fs::write(&lock, "held by an ordinary git writer\n").unwrap();
    let release = lock.clone();
    let holder = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(1500));
        std::fs::remove_file(release).unwrap();
    });

    let started = std::time::Instant::now();
    published.retire();
    wait_until("the patient deletion retires the reference", || {
        !check_ref_exists(&repo, &reference)
    })
    .await;
    holder.join().unwrap();
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the fixture must actually have made the deletion wait"
    );
    assert!(!check_ref_is_held(&reference));
}

/// Releases that arrive together are retired by one ref transaction, and a
/// released ref stays held — out of the ref-store sweep's reach — until it
/// is actually gone.
#[tokio::test]
async fn released_check_refs_are_retired_together_and_stay_held_until_deleted() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let mut published = Vec::new();
    for id in ["batch-a", "batch-b", "batch-c"] {
        published.push(
            publish_check_commit_ref(&orch, &repo, &store, &commit, id)
                .await
                .unwrap(),
        );
    }
    for reference in &mut published {
        reference.retire();
    }
    for id in ["batch-a", "batch-b", "batch-c"] {
        assert!(
            check_ref_is_held(&format!("refs/cairn/checks/{id}")),
            "a released ref is held until it is deleted"
        );
    }
    wait_until("all three refs are retired", || {
        ["batch-a", "batch-b", "batch-c"]
            .iter()
            .all(|id| !check_ref_exists(&repo, &format!("refs/cairn/checks/{id}")))
    })
    .await;
    assert_eq!(
        CHECK_REF_RETIRE_TRANSACTIONS
            .lock()
            .unwrap()
            .get(&store)
            .copied(),
        Some(1),
        "three releases inside the debounce cost one deletion"
    );
    for id in ["batch-a", "batch-b", "batch-c"] {
        assert!(!check_ref_is_held(&format!("refs/cairn/checks/{id}")));
    }
}

/// One stale entry fails a batched transaction; the rest are still retired.
#[tokio::test]
async fn a_stale_entry_does_not_keep_the_rest_of_its_batch() {
    let temp = tempfile::tempdir().unwrap();
    let (repo, commit) = committed_git_repo(&temp);
    git(&repo, &["update-ref", "refs/cairn/checks/good", &commit]);
    hold_check_ref("refs/cairn/checks/good");
    let failed = delete_check_refs(
        &repo,
        vec![
            CheckRefRetirement {
                repository: repo.clone(),
                reference: "refs/cairn/checks/good".into(),
                commit: commit.clone(),
                attempts: 0,
            },
            CheckRefRetirement {
                repository: repo.clone(),
                reference: "refs/cairn/checks/moved".into(),
                commit: "1".repeat(40),
                attempts: 0,
            },
        ],
    )
    .await;
    assert!(!check_ref_exists(&repo, "refs/cairn/checks/good"));
    assert!(!check_ref_is_held("refs/cairn/checks/good"));
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0.reference, "refs/cairn/checks/moved");
}

/// A batch that cannot retire its check ref releases it rather than
/// holding it forever, so the ref-store sweep can retire it instead.
#[tokio::test]
async fn a_check_ref_whose_cleanup_gives_up_is_left_for_the_sweep() {
    let temp = TempDir::new().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let reference = "refs/cairn/checks/cleanup-gave-up".to_string();
    hold_check_ref(&reference);
    assert!(check_ref_is_held(&reference));

    // Not a repository at all, so every cleanup attempt fails.
    drop(TemporaryCheckRef {
        orch: orch.clone(),
        repository: temp.path().join("not-a-repository"),
        store_dir: temp.path().join("store"),
        reference: reference.clone(),
        commit: "0".repeat(40),
        armed: true,
    });
    wait_until("the exhausted cleanup hands the ref to the sweep", || {
        !check_ref_is_held(&reference)
    })
    .await;
}

#[tokio::test]
async fn dropped_temporary_check_ref_retries_cleanup_under_store_lock() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let reference = "refs/cairn/checks/cancelled".to_string();
    git(&repo, &["update-ref", &reference, &commit]);
    let held = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            "hold cancellation cleanup test",
        )
        .await;

    drop(TemporaryCheckRef {
        orch: orch.clone(),
        repository: repo.clone(),
        store_dir: store,
        reference: reference.clone(),
        commit,
        armed: true,
    });
    // Past the debounce: the sweeper is now queued on the store lock.
    tokio::time::sleep(CHECK_REF_RETIRE_DEBOUNCE * 3).await;
    assert!(
        check_ref_exists(&repo, &reference),
        "deletion must not bypass the held store lock"
    );
    drop(held);

    wait_until(
        "the ref is removed after the store lock is acquired",
        || !check_ref_exists(&repo, &reference),
    )
    .await;
}

fn retirement_of(repo: &Path, reference: &str, commit: &str) -> CheckRefRetirement {
    CheckRefRetirement {
        repository: repo.to_path_buf(),
        reference: reference.to_string(),
        commit: commit.to_string(),
        attempts: 0,
    }
}

fn pending_check_ref_retirements(store: &Path) -> usize {
    retiring_check_refs()
        .get(store)
        .map_or(0, |pending| pending.refs.len())
}

/// A ref is often released by a destructor while the task holding it is
/// dropped, and that task may be on a runtime that is itself going away,
/// as a turn-end wave started off any runtime used to be. Its retirement
/// must not go down with that runtime.
#[tokio::test]
async fn a_check_ref_released_on_a_runtime_that_then_ends_is_still_retired() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let reference = "refs/cairn/checks/runtime-ended".to_string();
    let wave = std::thread::spawn({
        let (orch, repo, store) = (orch.clone(), repo.clone(), store.clone());
        move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let published =
                    publish_check_commit_ref(&orch, &repo, &store, &commit, "runtime-ended")
                        .await
                        .expect("the sealed check reference publishes");
                drop(published);
            });
            // The runtime drops here, with anything that was spawned onto it.
        }
    });
    tokio::task::spawn_blocking(move || wave.join().unwrap())
        .await
        .unwrap();
    wait_until(
        "the released ref outlives its runtime and is retired",
        || !check_ref_exists(&repo, &reference) && !check_ref_is_held(&reference),
    )
    .await;
}

/// A sweep that ends with its batch taken but not deleted gives the batch
/// back and lets the next release start a new sweep, rather than leaving
/// the store marked as swept, and its refs held, for the rest of the
/// process.
#[tokio::test]
async fn a_sweep_dropped_mid_batch_hands_its_batch_to_the_next_sweep() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let (first, second) = (
        "refs/cairn/checks/mid-batch-first",
        "refs/cairn/checks/mid-batch-second",
    );
    for reference in [first, second] {
        git(&repo, &["update-ref", reference, &commit]);
        hold_check_ref(reference);
    }
    // Holding the store makes the sweep take its batch and then wait.
    let held = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            "hold the sweep mid-batch",
        )
        .await;
    let sweep = queue_check_ref_retirement(&orch, &store, retirement_of(&repo, first, &commit))
        .expect("a store with no live sweep starts one");
    let sweep = tokio::spawn(sweep);
    wait_until("the sweep has taken its batch", || {
        pending_check_ref_retirements(&store) == 0
    })
    .await;
    sweep.abort();
    assert!(sweep.await.unwrap_err().is_cancelled());
    drop(held);
    assert_eq!(
        pending_check_ref_retirements(&store),
        1,
        "the unfinished batch is queued again"
    );
    assert!(
        check_ref_is_held(first),
        "and its ref is still owed a deletion"
    );

    retire_check_ref(&orch, &store, retirement_of(&repo, second, &commit));
    wait_until("the next sweep retires both batches", || {
        [first, second]
            .iter()
            .all(|reference| !check_ref_exists(&repo, reference) && !check_ref_is_held(reference))
    })
    .await;
}

/// A sweep dropped before it ever ran, as a task spawned onto a runtime
/// already shutting down is, leaves no store marked as swept.
#[tokio::test]
async fn a_sweep_dropped_before_it_ran_leaves_its_store_unswept() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (repo, commit) = committed_git_repo(&temp);
    let store = temp.path().join("store");
    let (first, second) = (
        "refs/cairn/checks/unpolled-first",
        "refs/cairn/checks/unpolled-second",
    );
    for reference in [first, second] {
        git(&repo, &["update-ref", reference, &commit]);
        hold_check_ref(reference);
    }
    let sweep = queue_check_ref_retirement(&orch, &store, retirement_of(&repo, first, &commit))
        .expect("a store with no live sweep starts one");
    drop(sweep);

    retire_check_ref(&orch, &store, retirement_of(&repo, second, &commit));
    wait_until("a new sweep retires both refs", || {
        [first, second]
            .iter()
            .all(|reference| !check_ref_exists(&repo, reference) && !check_ref_is_held(reference))
    })
    .await;
}

#[test]
fn result_key_is_impact_filtered_and_includes_definition_platform_and_toolchain() {
    let mut definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
    let base = vec![
        ("src/lib.rs".to_string(), "blob-a".to_string()),
        ("docs/readme.md".to_string(), "docs-a".to_string()),
    ];
    let key = check_result_key(
        &definition,
        &selector(&definition),
        Some(&base),
        "whole-tree-a",
        "macos-aarch64",
        "rustc=1;bun=1",
        CheckChange::unknown("test"),
    );

    let mut changed_definition = definition.clone();
    changed_definition.command = "cargo test --all".to_string();
    assert_ne!(
        key,
        check_result_key(
            &changed_definition,
            &selector(&changed_definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    changed_definition = definition.clone();
    changed_definition.policy = CheckPolicy::Gate;
    assert_ne!(
        key,
        check_result_key(
            &changed_definition,
            &selector(&changed_definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    changed_definition = definition.clone();
    changed_definition.resource_class = CheckResourceClass::Exclusive;
    assert_ne!(
        key,
        check_result_key(
            &changed_definition,
            &selector(&changed_definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    changed_definition = definition.clone();
    changed_definition.timeout = Some(42);
    assert_ne!(
        key,
        check_result_key(
            &changed_definition,
            &selector(&changed_definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    changed_definition = definition.clone();
    changed_definition.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
        name: Some("bglab-mac".to_string()),
        os: None,
        required_toolchains: vec!["rust".to_string()],
        requires_macos_nested_sandbox_write: false,
        containment: None,
        page_call_programs: Vec::new(),
    });
    assert_ne!(
        key,
        check_result_key(
            &changed_definition,
            &selector(&changed_definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    assert_ne!(
        key,
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&base),
            "whole-tree-a",
            "linux-x86_64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    assert_ne!(
        key,
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=2;bun=1",
            CheckChange::unknown("test")
        )
    );

    let inside_changed = vec![
        ("src/lib.rs".to_string(), "blob-b".to_string()),
        ("docs/readme.md".to_string(), "docs-a".to_string()),
    ];
    assert_ne!(
        key,
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&inside_changed),
            "whole-tree-b",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
    let outside_changed = vec![
        ("src/lib.rs".to_string(), "blob-a".to_string()),
        ("docs/readme.md".to_string(), "docs-b".to_string()),
    ];
    assert_eq!(
        key,
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&outside_changed),
            "whole-tree-b",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );

    definition.impact = Some(vec!["docs/**".to_string()]);
    assert_ne!(
        key,
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
            CheckChange::unknown("test")
        )
    );
}

// --- keying by the derived closure --------------------------------------

/// A pinned workspace graph plus the source files a case needs. These tests
/// assert closure keying, so their graph must not change with the checkout
/// while the parallel suite is running.
fn workspace_tree(sources: &[(&str, &str)]) -> crate::execution::inputs::fixtures::TreeFixture {
    use crate::execution::inputs::fixtures::TreeFixture;

    let mut fixture = TreeFixture::new()
        .file(
            "src-tauri/Cargo.toml",
            r#"[workspace]
members = ["os/cairn-cmd", "os/cairn-common", "os/cairn-core", "os/cairn-db", "os/cairn-vcs"]
"#,
        )
        .file(
            "src-tauri/os/cairn-common/Cargo.toml",
            "[package]\nname = \"cairn-common\"\n",
        )
        .file(
            "src-tauri/os/cairn-vcs/Cargo.toml",
            "[package]\nname = \"cairn-vcs\"\n",
        )
        .file(
            "src-tauri/os/cairn-db/Cargo.toml",
            r#"[package]
name = "cairn-db"
[dependencies]
cairn-common = { path = "../cairn-common" }
"#,
        )
        .file(
            "src-tauri/os/cairn-core/Cargo.toml",
            r#"[package]
name = "cairn-core"
[dependencies]
cairn-common = { path = "../cairn-common" }
cairn-db = { path = "../cairn-db" }
cairn-vcs = { path = "../cairn-vcs" }
"#,
        )
        .file(
            "src-tauri/os/cairn-cmd/Cargo.toml",
            r#"[package]
name = "cairn-cmd"
[dependencies]
cairn-common = { path = "../cairn-common" }
"#,
        );
    for (path, version) in sources {
        fixture = fixture.source(path, version);
    }
    fixture
}

fn scoped(command: &str, tokens: &[&str]) -> CheckCommand {
    let mut definition = check(command, None, CheckWhen::Review);
    definition.scope = Some(crate::config::project_settings::CheckScopeSelector::Many(
        tokens.iter().map(|token| token.to_string()).collect(),
    ));
    definition
}

/// This check's key over this tree, with its selector resolved from the same
/// tree — exactly what the runners do.
fn scoped_key(
    definition: &CheckCommand,
    extra_inputs: &HashMap<String, Vec<String>>,
    fixture: &crate::execution::inputs::fixtures::TreeFixture,
) -> String {
    let entries = fixture.entries();
    let snapshot = TreeSnapshot::new(Some(&entries), fixture);
    let selector = crate::execution::inputs::resolve_one(definition, extra_inputs, &snapshot);
    check_result_key(
        definition,
        &selector,
        Some(&entries),
        "whole-tree",
        TEST_PLATFORM,
        TEST_TOOLCHAIN,
        CheckChange::unknown("test"),
    )
}

#[test]
fn a_change_outside_a_scoped_check_s_closure_reuses_its_verdict() {
    let rust = scoped("bun run test:rust", &["rust:cairn-core"]);
    let no_extra = HashMap::new();
    let before = workspace_tree(&[
        ("src/App.tsx", "v1"),
        ("src-tauri/os/cairn-core/src/lib.rs", "v1"),
    ]);
    let frontend_only = workspace_tree(&[
        ("src/App.tsx", "v2"),
        ("src-tauri/os/cairn-core/src/lib.rs", "v1"),
    ]);
    assert_eq!(
        scoped_key(&rust, &no_extra, &before),
        scoped_key(&rust, &no_extra, &frontend_only),
        "a frontend-only commit changes no Rust input, so the verdict is reused"
    );

    // And a change INSIDE the closure moves the key, including one in a
    // transitive dependency the check never names.
    for path in [
        "src-tauri/os/cairn-core/src/lib.rs",
        "src-tauri/os/cairn-vcs/src/lib.rs",
        "src-tauri/Cargo.lock",
    ] {
        let changed = workspace_tree(&[
            ("src/App.tsx", "v1"),
            ("src-tauri/os/cairn-core/src/lib.rs", "v1"),
            (path, "v2"),
        ]);
        assert_ne!(
            scoped_key(&rust, &no_extra, &before),
            scoped_key(&rust, &no_extra, &changed),
            "{path} is an input of rust:cairn-core and must produce a new key"
        );
    }
}

#[test]
fn retargeting_a_scope_or_an_extra_input_produces_a_new_key() {
    let tree = workspace_tree(&[("src-tauri/os/cairn-core/src/lib.rs", "v1")]);
    let no_extra = HashMap::new();
    let core = scoped("bun run test:rust", &["rust:cairn-core"]);
    let cmd = scoped("bun run test:rust", &["rust:cairn-cmd"]);
    assert_ne!(
        scoped_key(&core, &no_extra, &tree),
        scoped_key(&cmd, &no_extra, &tree),
        "the selector definition is part of the key"
    );

    let with_extra = HashMap::from([(
        "rust:cairn-db".to_string(),
        vec!["src-tauri/turso_migrations/**".to_string()],
    )]);
    assert_ne!(
        scoped_key(&core, &no_extra, &tree),
        scoped_key(&core, &with_extra, &tree),
        "declaring an extra input its closure reaches re-keys the check"
    );
}

#[test]
fn a_migration_is_an_input_of_every_check_whose_closure_reaches_cairn_db() {
    let extra_inputs = HashMap::from([(
        "rust:cairn-db".to_string(),
        vec!["src-tauri/turso_migrations/**".to_string()],
    )]);
    let core = scoped("bun run test:rust", &["rust:cairn-core"]);
    let cmd = scoped("bun run test:rust", &["rust:cairn-cmd"]);
    let before = workspace_tree(&[("src-tauri/turso_migrations/0084_x.sql", "v1")]);
    let after = workspace_tree(&[("src-tauri/turso_migrations/0084_x.sql", "v2")]);
    assert_ne!(
        scoped_key(&core, &extra_inputs, &before),
        scoped_key(&core, &extra_inputs, &after)
    );
    assert_eq!(
        scoped_key(&cmd, &extra_inputs, &before),
        scoped_key(&cmd, &extra_inputs, &after),
        "cairn-cmd never compiles the migrations in"
    );
}

#[test]
fn a_config_error_runs_nothing_and_caches_nothing() {
    let mut slice = check(
        "bun run check:migrations",
        Some(&["src-tauri/turso_migrations/**"]),
        CheckWhen::Write,
    );
    slice.tree = crate::config::project_settings::CheckTree::Impact;
    slice.scope = Some(crate::config::project_settings::CheckScopeSelector::One(
        "rust:cairn-db".to_string(),
    ));
    let map = HashMap::from([("migrations".to_string(), slice)]);
    let plans = plan_checks(
        &map,
        &inputs(&map),
        &[change("src-tauri/turso_migrations/0001_x.sql")],
        Path::new("/repo"),
    );
    let plan = plans.first().expect("one plan");
    let error = plan
        .config_error
        .as_deref()
        .expect("an impact slice cannot carry a scope closure");
    assert!(error.contains("tree: impact"));
    assert!(error.contains("scope"));
}

/// cairn/5268's pin: codex-pin as the REAL config declares it. Its key
/// moves when anything the suite compiles moves (a file deep in the
/// `rust:cairn-models` closure) or the script it runs, and stays put for
/// cairn-orch, which only the `ensure-sidecar` prerequisite compiles and
/// whose failure is a non-reusable `prerequisite` red instead.
#[test]
fn codex_pin_keys_on_what_its_suite_compiles_and_runs() {
    use crate::execution::inputs::fixtures::real_workspace;

    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository project config must parse");
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .expect("repository must declare checks");
    let codex_pin = contract
        .checks
        .get("codex-pin")
        .expect("codex-pin is configured")
        .clone();
    let extra_inputs = contract.extra_inputs;

    let key_with = |path: &str, version: &str| {
        let fixture = real_workspace()
            .source("scripts/tool-pin.ts", "v1")
            .source("scripts/lib/managed-tools.ts", "v1")
            .source("src-tauri/os/cairn-common/src/lib.rs", "v1")
            .source("src-tauri/cairn-orch/src/executor_supervisor.rs", "v1")
            .source(path, version);
        scoped_key(&codex_pin, &extra_inputs, &fixture)
    };
    let base = key_with("docs/checks.md", "v1");
    assert_eq!(
        base,
        key_with("docs/checks.md", "v2"),
        "a doc edit is no input"
    );
    assert_eq!(
        base,
        key_with("src-tauri/cairn-orch/src/executor_supervisor.rs", "v2"),
        "cairn-orch is the sidecar prerequisite's input, not the suite's"
    );
    for input in [
        "src-tauri/os/cairn-common/src/lib.rs",
        "src-tauri/os/cairn-models/src/session/codex/pin.rs",
        "src-tauri/os/cairn-models/codex-pin.json",
        "scripts/tool-pin.ts",
        "scripts/lib/managed-tools.ts",
        "scripts/lib/codex-app-server-smoke.ts",
        "scripts/lib/pinned-download.ts",
        "package.json",
    ] {
        assert_ne!(base, key_with(input, "v2"), "{input} must re-key codex-pin");
    }
}

#[test]
fn resource_identity_is_stable_across_tree_states() {
    let definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
    assert_ne!(
        check_result_key(
            &definition,
            &selector(&definition),
            None,
            "tree-one",
            "platform",
            "toolchain",
            CheckChange::unknown("test")
        ),
        check_result_key(
            &definition,
            &selector(&definition),
            None,
            "tree-two",
            "platform",
            "toolchain",
            CheckChange::unknown("test")
        ),
    );
    assert_eq!(
        check_resource_identity("rust-full", &definition),
        check_resource_identity("rust-full", &definition),
    );
    assert_ne!(
        check_resource_identity("rust-full", &definition),
        check_resource_identity("rust-fmt", &definition),
    );
}

#[tokio::test]
async fn cached_partial_observation_is_a_miss_for_main_health_full_run() {
    let db = cache_db().await;
    let mut configured = check(
        "bun run test:rust {changedFiles}",
        Some(&["src-tauri/**"]),
        CheckWhen::Review,
    );
    configured.full_command = Some("bun run test:rust:full".to_string());
    let selector = selector(&configured);
    let entries = [(
        "src-tauri/os/cairn-core/src/lib.rs".to_string(),
        "blob".to_string(),
    )];
    let partial_command = "bun run test:rust src-tauri/os/cairn-core/src/lib.rs";
    let full_command = "bun run test:rust:full";
    let partial_key = check_result_key_for_executed_command(
        &configured,
        partial_command,
        &selector,
        Some(&entries),
        "tree",
        TEST_PLATFORM,
        TEST_TOOLCHAIN,
        CheckChange::unknown("test"),
    );
    let full_plan = plan("rust-tests", full_command);
    let checks = HashMap::from([("rust-tests".to_string(), configured.clone())]);
    let resolved = inputs(&checks);
    let (_, full_key) = key_review_tree_plans(
        vec![full_plan.clone()],
        &checks,
        &resolved,
        &entries,
        "tree",
        TEST_PLATFORM,
        TEST_TOOLCHAIN,
        ChangeBasis::Unknown,
    )
    .pop()
    .unwrap();
    assert_ne!(
        partial_key, full_key,
        "execution breadth is evidence identity"
    );

    let environment = plan_environment_fingerprint(&full_plan);
    record_fresh_check_observation(
        db.clone(),
        reusable_observation_for("rust-tests", &partial_key, environment),
    )
    .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let outcomes = run_planned_checks(
        db.clone(),
        "project-a",
        "tree",
        "main:project-a",
        &[(full_plan, full_key)],
        "main-health",
        CheckExecMode::Shared,
        None,
        move |_index, command, _stream_id| {
            let counted = counted.clone();
            async move {
                assert_eq!(command, "bun run test:rust:full");
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "full suite passed")
            }
        },
        |_| {},
    )
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "partial evidence is a cache miss"
    );
    assert!(outcomes[0].passed);
    assert!(!outcomes[0].cached);
}

#[test]
fn main_health_full_tree_planning_uses_explicit_full_command() {
    let mut configured = check(
        "bun run test:rust {changedFiles}",
        Some(&["src-tauri/**"]),
        CheckWhen::Review,
    );
    configured.full_command = Some("bun run test:rust:full".to_string());
    let checks = HashMap::from([("rust-tests".to_string(), configured)]);
    let planned = CheckPlan {
        name: "rust-tests".to_string(),
        applies: true,
        command: "bun run test:rust src-tauri/os/cairn-core/src/lib.rs".to_string(),
        scope: CheckScope::Partial,
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        verdict_platforms: vec![crate::execution::check_identity::gating_platform().to_string()],
        verdict_runtime: Vec::new(),
        verdict_executor_ids: None,
        config_error: None,
    };

    let plans = resolve_full_tree_plans(vec![planned], &checks);
    assert_eq!(plans[0].command, "bun run test:rust:full");
    assert_eq!(plans[0].scope, CheckScope::Full);
    assert!(plans[0].config_error.is_none());
}

#[test]
fn cargo_control_inputs_select_a_rust_check() {
    // A Rust check that only watched `**/*.rs` would skip a rebuild that a
    // lockfile bump, a feature-flag edit, a linker setting, or a build script
    // makes necessary. The impact set below is the one a Rust check must
    // declare to cover every input cargo's output depends on; this pins that
    // each of those inputs actually selects the check.
    //
    // The globs are the fixture, not a reading of the repository's own
    // `.cairn/config.yaml`: a check set is project configuration a user may
    // edit or clear at will, so asserting against the live file tests the
    // configuration rather than the selection logic and breaks on an
    // unrelated settings edit.
    let impact = [
        "src-tauri/**/*.rs",
        "src-tauri/Cargo.toml",
        "src-tauri/**/Cargo.toml",
        "src-tauri/Cargo.lock",
        "src-tauri/.cargo/config.toml",
        "src-tauri/**/build.rs",
    ];
    let definition = check("bun run check:rust", Some(&impact), CheckWhen::Review);
    let temp = TempDir::new().unwrap();

    for path in [
        "src-tauri/os/cairn-core/src/execution/checks.rs",
        "src-tauri/.cargo/config.toml",
        "src-tauri/Cargo.toml",
        "src-tauri/os/cairn-core/Cargo.toml",
        "src-tauri/Cargo.lock",
        "src-tauri/os/cairn-core/build.rs",
    ] {
        let map = HashMap::from([("rust".to_string(), definition.clone())]);
        let planned = plan_checks(&map, &inputs(&map), &[change(path)], temp.path());
        assert!(
            planned
                .iter()
                .any(|plan| plan.name == "rust" && plan.applies),
            "a Rust check must apply when {path} changes; impact={impact:?}"
        );
    }

    // The impact set stays a filter: a change outside it selects nothing.
    let map = HashMap::from([("rust".to_string(), definition)]);
    let planned = plan_checks(
        &map,
        &inputs(&map),
        &[change("web/src/pages/docs/recipes.mdx")],
        temp.path(),
    );
    assert!(
        planned.iter().all(|plan| !plan.applies),
        "a documentation-only change selects no Rust check"
    );
}

#[test]
fn repository_cell_dependency_installation_is_frozen() {
    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository project config must parse");
    assert_eq!(
        settings.setup_commands,
        Some(vec!["bun install --frozen-lockfile".into()]),
        "cell preparation must reject a stale bun.lock instead of rewriting it"
    );

    let package: serde_json::Value =
        serde_json::from_str(include_str!("../../../../../../package.json"))
            .expect("repository package.json must parse");
    assert_eq!(
        package["scripts"]["postinstall"], "cd api && bun install --frozen-lockfile",
        "the nested API install must preserve the frozen preparation boundary"
    );
}

#[test]
fn repository_checks_declare_only_the_toolchains_their_lanes_require() {
    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository project config must parse");
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .expect("repository must declare checks");

    for (name, check) in contract.checks {
        let declared = &check
            .executor
            .as_ref()
            .unwrap_or_else(|| panic!("{name} must declare required toolchains"))
            .required_toolchains;
        assert!(
            !check.command.split_whitespace().any(|word| word == "bunx")
                || check.command.contains("bunx --bun "),
            "{name} must force Bun when invoking package executables through bunx"
        );
        let expected: &[&str] = match name.as_str() {
            "infra-stacks" => &["bun", "node", "npm"],
            "rust-fmt"
            | "lockfile"
            | "licenses"
            | "rust-lint"
            | "rust-tests"
            | "external-host"
            | "codex-pin"
            | "codex-contract"
            | "claude-pin"
            | "claude-contract"
            | "uri-wasm"
            | "rust-write-pins"
            | "tls-sole-loader"
            | "home-coverage"
            | "plan-audit"
            | "workspace-compile"
            | "source-scans"
            | "third-party-notices" => &["bun", "rust"],
            "executor-windows-check" | "executor-macos-check" | "core-macos-check" => &["rust"],
            _ if check
                .command
                .split_whitespace()
                .any(|word| word == "bun" || word == "bunx") =>
            {
                &["bun"]
            }
            _ => panic!(
                "{name} has an unclassified check command: {}",
                check.command
            ),
        };
        assert_eq!(declared, expected, "{name} overdeclares its toolchains");
    }
}

#[test]
fn repository_native_compile_checks_cover_macos_and_windows_all_targets() {
    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository project config must parse");
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .expect("repository must declare checks");

    for (name, target, platform, selection) in [
        (
            "executor-macos-check",
            None,
            "macos",
            "-p cairn-exec -p cairn-common",
        ),
        (
            "executor-windows-check",
            Some("x86_64-pc-windows-msvc"),
            "windows",
            "-p cairn-exec -p cairn-common",
        ),
        // Without the feature a cairn-core test build is a compile_error!.
        (
            "core-macos-check",
            None,
            "macos",
            "-p cairn-core --features cairn-core/test-utils",
        ),
    ] {
        let check = contract
            .checks
            .get(name)
            .unwrap_or_else(|| panic!("{name} exists"));
        assert_eq!(check.when, CheckWhen::Main, "{name} runs on main only");
        assert_eq!(
            check
                .main_cadence
                .as_ref()
                .map(|cadence| cadence.platforms.as_slice()),
            Some(&[platform.to_string()][..]),
            "{name} has one main lane, on its native platform"
        );
        assert_eq!(
            check.verdict_platforms.as_deref(),
            Some(&[platform.to_string()][..]),
            "{name} must run on its native platform"
        );
        match target {
            Some(target) => assert!(
                check.command.contains(&format!("--target {target}")),
                "{name} checks the intended target"
            ),
            None => assert!(
                !check
                    .command
                    .split_whitespace()
                    .any(|word| word == "--target"),
                "{name} must use the executor's native architecture"
            ),
        }
        assert!(
            check.command.contains("--all-targets"),
            "{name} must compile test targets as well as the library"
        );
        assert!(
            check.command.contains(selection),
            "{name} compiles the intended crates: {selection}"
        );
    }
}

#[test]
fn repository_gates_the_actual_sandbox_sensitive_rust_lane() {
    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository project config must parse");
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .expect("repository must declare checks");

    let rust_tests = contract
        .checks
        .get("rust-tests")
        .expect("rust-tests exists");
    assert!(
        rust_tests
            .executor
            .as_ref()
            .is_some_and(|selector| selector.requires_macos_nested_sandbox_write),
        "the lane that executes sandbox lifecycle tests must require the live capability"
    );
    assert!(
        !contract
            .checks
            .get("rust-fmt")
            .and_then(|check| check.executor.as_ref())
            .is_some_and(|selector| selector.requires_macos_nested_sandbox_write),
        "ordinary formatting must remain placeable on an incapable macOS host"
    );
}

#[test]
fn repository_rust_tests_key_on_the_whole_workspace_and_cover_harness_inputs() {
    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository check config must parse");
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .expect("repository must declare checks");
    let checks: HashMap<String, CheckCommand> = contract
        .checks
        .into_iter()
        .filter(|(name, _)| matches!(name.as_str(), "rust-lint" | "rust-tests"))
        .collect();
    let resolved = inputs(&checks);
    let applies = |path: &str| {
        applicable_turn_end_checks(&checks, &resolved, &[change(path)], Path::new("/repo"))
            .into_iter()
            .map(|plan| plan.name)
            .collect::<Vec<_>>()
    };

    // The app crate's tests `include_str!` its capability files, so a Tauri
    // asset is a test input; clippy's lint key stays on sources.
    assert_eq!(
        applies("src-tauri/capabilities/default.json"),
        vec!["rust-tests"]
    );
    assert_eq!(
        applies("src-tauri/os/cairn-core/src/execution/checks.rs"),
        vec!["rust-lint", "rust-tests"]
    );
    assert_eq!(
        applies("scripts/lib/skip-ledger.ts"),
        vec!["rust-lint", "rust-tests"]
    );
    assert_eq!(applies("src-tauri/skip-manifest.toml"), vec!["rust-tests"]);
}

#[test]
fn repository_skip_census_lane_follows_what_it_audits() {
    // The census reads every `.rs` under src-tauri and the skip manifest. A
    // lane keyed on anything narrower reuses its verdict across the change
    // that breaks or fixes it, which is how frontend-tests carried it.
    let config = include_str!("../../../../../../.cairn/config.yaml");
    let (settings, _) = crate::config::project_settings::parse_project_settings(config)
        .expect("repository check config must parse");
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .expect("repository must declare checks");
    let census = contract
        .checks
        .get("skip-census")
        .expect("skip-census exists")
        .clone();
    assert_eq!(
        census.when,
        CheckWhen::Write,
        "the breaking commit sees its red"
    );
    assert!(matches!(census.policy, CheckPolicy::Advisory));
    assert!(
        census.main_cadence.is_some(),
        "main's tip carries a census row"
    );
    assert!(
        crate::execution::check_parsers::is_test_runner_command(&census.command),
        "a per-test lane, so its row names the census test"
    );

    let checks = HashMap::from([("skip-census".to_string(), census)]);
    let resolved = inputs(&checks);
    let applies = |path: &str| {
        plan_checks(&checks, &resolved, &[change(path)], Path::new("/repo"))
            .iter()
            .any(|plan| plan.applies)
    };
    for path in [
        "src-tauri/skip-manifest.toml",
        "src-tauri/os/cairn-core/src/pages/tests.rs",
        "src-tauri/os/cairn-common/src/skip_ledger.rs",
        "scripts/lib/skip-ledger.test.ts",
        "scripts/lib/skip-ledger.ts",
    ] {
        assert!(applies(path), "skip-census must run when {path} changes");
    }
    assert!(!applies("src/App.tsx"));
    assert!(!applies("docs/checks.md"));
}

#[test]
fn result_key_uses_tree_content_not_commit_history() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    git(repo, &["init", "-b", "main", "--quiet"]);
    git(repo, &["config", "user.name", "Cairn Test"]);
    git(repo, &["config", "user.email", "cairn@example.test"]);
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").unwrap();
    std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "--quiet", "-m", "first message"]);

    let first_commit = git(repo, &["rev-parse", "HEAD"]);
    let tree = git(repo, &["rev-parse", "HEAD^{tree}"]);
    let second_commit = {
        let mut child = Command::new("git")
            .args(["commit-tree", tree.as_str(), "-p", first_commit.as_str()])
            .current_dir(repo)
            .env("GIT_AUTHOR_NAME", "Cairn Test")
            .env("GIT_AUTHOR_EMAIL", "cairn@example.test")
            .env("GIT_COMMITTER_NAME", "Cairn Test")
            .env("GIT_COMMITTER_EMAIL", "cairn@example.test")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"different message and parent\n")
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    };
    assert_ne!(first_commit, second_commit);

    let definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
    let first_entries = git_tree_entries(repo, &first_commit);
    let second_entries = git_tree_entries(repo, &second_commit);
    assert_eq!(
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&first_entries),
            &tree,
            "test-platform",
            "test-toolchain",
            CheckChange::unknown("test"),
        ),
        check_result_key(
            &definition,
            &selector(&definition),
            Some(&second_entries),
            &tree,
            "test-platform",
            "test-toolchain",
            CheckChange::unknown("test"),
        ),
        "identical impact-filtered trees must hit despite different commit history"
    );
}

fn change(path: &str) -> GraphFileChange {
    GraphFileChange {
        path: path.to_string(),
        previous_path: None,
        status: "modified".to_string(),
        additions: 1,
        deletions: 0,
    }
}

fn check(command: &str, impact: Option<&[&str]>, when: CheckWhen) -> CheckCommand {
    CheckCommand {
        command: command.to_string(),
        full_command: None,
        impact: impact.map(|globs| globs.iter().map(|s| s.to_string()).collect()),
        scope: None,
        policy: CheckPolicy::Advisory,
        when,
        resource_class: CheckResourceClass::Shared,
        timeout: None,
        executor: None,
        verdict_environment: Vec::new(),
        verdict_platforms: None,
        runtime: None,
        fixes: false,
        base_relative: false,
        main_cadence: None,
        tree: Default::default(),
    }
}

fn cache_entry(check_name: &str, tree_hash: &str, passed: bool) -> CheckResultCacheEntry {
    CheckResultCacheEntry {
        source_observation_id: None,
        project_id: "project-a".to_string(),
        tree_hash: tree_hash.to_string(),
        input_hash: format!("input-{tree_hash}"),
        check_name: check_name.to_string(),
        exit_code: if passed { 0 } else { 1 },
        passed,
        output_tail: String::new(),
        duration_ms: 1,
        ran_at: 1,
        target_results_json: None,
        job_id: None,
        cached: None,
        failure_kind: None,
        infra_failure_streak: 0,
        executor_id: None,
        executor_device_id: None,
        executor_connection_generation: None,
        executor_cell_id: None,
        executor_lease_epoch: None,
        executor_started_at_unix_ms: None,
        executor_finished_at_unix_ms: None,
        toolchain_fingerprint: None,
        defined_by_commit_sha: Some(format!("commit-{tree_hash}")),
    }
}

/// The repo's `checks` shape: two `when:write` checks (frontend, typecheck)
/// scoped to the frontend trees, one `when:review` check (rust) scoped to
/// src-tauri.
fn repo_checks() -> HashMap<String, CheckCommand> {
    let mut checks = HashMap::new();
    checks.insert(
        "frontend".to_string(),
        check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**", "packages/ui/**"]),
            CheckWhen::Write,
        ),
    );
    checks.insert(
        "typecheck".to_string(),
        check(
            "bunx tsc --noEmit",
            Some(&["src/**", "packages/ui/**"]),
            CheckWhen::Write,
        ),
    );
    checks.insert(
        "rust".to_string(),
        check(
            "bun run test:rust",
            Some(&["src-tauri/**"]),
            CheckWhen::Review,
        ),
    );
    checks
}

// --- the write-cadence gate -------------------------------------------

#[test]
fn gate_selects_write_checks_for_a_src_change() {
    let plans = applicable_write_checks(
        &repo_checks(),
        &inputs(&repo_checks()),
        &[change("src/App.tsx")],
        Path::new("/repo"),
    );
    let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["frontend", "typecheck"]);
}

#[test]
fn gate_is_empty_for_a_doc_only_change() {
    let plans = applicable_write_checks(
        &repo_checks(),
        &inputs(&repo_checks()),
        &[change("docs/x.md")],
        Path::new("/repo"),
    );
    assert!(plans.is_empty(), "a doc-only commit triggers no checks");
}

#[test]
fn gate_excludes_review_checks_for_a_rust_change() {
    let plans = applicable_write_checks(
        &repo_checks(),
        &inputs(&repo_checks()),
        &[change("src-tauri/os/cairn-core/src/lib.rs")],
        Path::new("/repo"),
    );
    // rust matches the change but is when:review; frontend/typecheck do not
    // match the src-tauri impact ⇒ nothing applies at the write cadence.
    assert!(
        !plans.iter().any(|p| p.name == "rust"),
        "a when:review check never runs on write"
    );
    assert!(plans.is_empty());
}

// --- contract source: the evaluated commit declares its own checks ------

/// Write a minimal `.cairn/config.yaml` declaring one `when:write` check
/// whose command greps a marker file of the same name.
fn write_checks_config(dir: &Path, check_name: &str) {
    let path = crate::config::project_settings::get_project_config_path(dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(
            "checks:\n  {check_name}:\n    command: grep marker {check_name}.txt\n    impact:\n      - src/**\n    when: write\n"
        ),
    )
    .unwrap();
}

/// An empty `checks:` mapping is a config that declares no checks at all.
fn write_checkless_config(dir: &Path) {
    let path = crate::config::project_settings::get_project_config_path(dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "setupCommands: []\n").unwrap();
}

fn commit_at(repo: &Path, message: &str) -> String {
    git(repo, &["add", "-A"]);
    git(
        repo,
        &[
            "-c",
            "user.name=Cairn Test",
            "-c",
            "user.email=test@cairn.local",
            "commit",
            "-q",
            "-m",
            message,
        ],
    );
    git(repo, &["rev-parse", "HEAD"])
}

/// A repository whose default branch carries check A and whose sibling
/// branch carries check B — the exact shape of the leak: two live commits,
/// each declaring a check the other's tree knows nothing about.
fn sibling_definitions(temp: &TempDir) -> (std::path::PathBuf, String, String) {
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("src").join("lib.rs"), "fn base() {}").unwrap();
    let base = commit_at(&repo, "base");

    write_checks_config(&repo, "check-a");
    std::fs::write(repo.join("check-a.txt"), "marker").unwrap();
    let commit_a = commit_at(&repo, "define check-a");

    git(&repo, &["checkout", "-q", "-b", "sibling", &base]);
    write_checks_config(&repo, "check-b");
    std::fs::write(repo.join("check-b.txt"), "marker").unwrap();
    let commit_b = commit_at(&repo, "define check-b");
    // Leave the checkout on the default branch, as a project checkout sits
    // while an agent branch advances elsewhere.
    git(&repo, &["checkout", "-q", "main"]);
    (repo, commit_a, commit_b)
}

/// Each sealed commit declares its own checks, and only its own: the
/// definition and the content it evaluates come from one tree.
#[test]
fn a_commit_declares_its_own_checks_and_only_its_own() {
    let temp = tempfile::tempdir().unwrap();
    let (repo, commit_a, commit_b) = sibling_definitions(&temp);

    for (commit, own, foreign) in [
        (&commit_a, "check-a", "check-b"),
        (&commit_b, "check-b", "check-a"),
    ] {
        let loaded = checks_contract_at_commit(&repo, commit)
            .unwrap_or_else(|| panic!("commit {commit} declares {own}"));
        assert_eq!(
            loaded.defined_by_commit, *commit,
            "the contract records the commit that declared it"
        );
        let names: Vec<&str> = loaded.contract.checks.keys().map(String::as_str).collect();
        assert_eq!(names, vec![own], "only {own} is declared at {commit}");
        assert_eq!(
            loaded.contract.checks[own].command,
            format!("grep marker {own}.txt"),
            "the command is the one this commit's own tree declares"
        );

        // Selection agrees: planning this contract can only ever select the
        // commit's own check, so a sibling's definition is never run or
        // recorded against it.
        let changed = vec![change("src/lib.rs")];
        let inputs = inputs(&loaded.contract.checks);
        let planned = applicable_write_checks(&loaded.contract.checks, &inputs, &changed, &repo);
        let planned_names: Vec<&str> = planned.iter().map(|plan| plan.name.as_str()).collect();
        assert_eq!(planned_names, vec![own]);
        assert!(
            !planned.iter().any(|plan| plan.command.contains(foreign)),
            "{foreign} must never be planned for {commit}"
        );
    }
}

/// The canonical checkout sits on the default branch while a sibling job
/// seals its own commit. The sibling's cadence sees only its own check; the
/// project-level projection still reads the canonical configuration.
#[test]
fn a_sibling_commit_neither_reads_nor_feeds_the_project_projection() {
    let temp = tempfile::tempdir().unwrap();
    let (repo, _commit_a, commit_b) = sibling_definitions(&temp);

    let sibling = checks_contract_at_commit(&repo, &commit_b).unwrap();
    assert!(
        !sibling.contract.checks.contains_key("check-a"),
        "the default branch's check must not reach a sibling commit"
    );

    let project = crate::config::project_settings::load_checks_contract(&repo)
        .expect("the project checkout declares checks");
    assert!(
        project.checks.contains_key("check-a"),
        "the project-level projection reads the canonical checkout"
    );
    assert!(
        !project.checks.contains_key("check-b"),
        "a branch's definition never becomes the project's"
    );
}

/// A check begins at the commit that adds it and stops at the commit that
/// removes it — never earlier, never later.
#[test]
fn adding_and_removing_a_check_takes_effect_only_from_its_own_commit() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    write_checkless_config(&repo);
    let before = commit_at(&repo, "no checks yet");
    write_checks_config(&repo, "check-a");
    let added = commit_at(&repo, "add check-a");
    write_checkless_config(&repo);
    let removed = commit_at(&repo, "remove check-a");

    assert!(
        checks_contract_at_commit(&repo, &before).is_none(),
        "a later addition cannot reach back to an earlier commit"
    );
    assert!(checks_contract_at_commit(&repo, &added)
        .unwrap()
        .contract
        .checks
        .contains_key("check-a"));
    assert!(
        checks_contract_at_commit(&repo, &removed).is_none(),
        "the removal takes effect at the commit that carries it"
    );
}

/// A commit with no config, an unparseable one, or one that declares no
/// checks selects nothing — the same as an absent contract always has. It
/// never falls back to another tree.
#[test]
fn an_absent_or_invalid_config_selects_nothing_rather_than_falling_back() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("README.md"), "no config here").unwrap();
    let bare = commit_at(&repo, "no config");
    let config_path = crate::config::project_settings::get_project_config_path(&repo);
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(&config_path, "checks: [this is not a mapping\n").unwrap();
    let invalid = commit_at(&repo, "invalid config");
    // A later commit DOES declare a check; neither earlier commit may borrow it.
    write_checks_config(&repo, "check-a");
    let valid = commit_at(&repo, "valid config");

    assert!(checks_contract_at_commit(&repo, &bare).is_none());
    assert!(checks_contract_at_commit(&repo, &invalid).is_none());
    assert!(checks_contract_at_commit(&repo, &valid).is_some());
    assert!(
        checks_contract_at_commit(&repo, "0000000000000000000000000000000000000000").is_none(),
        "an unresolvable commit selects nothing"
    );
}

#[test]
fn detailed_commit_config_outcomes_preserve_their_diagnostics() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("README.md"), "no config here").unwrap();
    let missing = commit_at(&repo, "missing config");

    write_checkless_config(&repo);
    let checkless = commit_at(&repo, "checkless config");

    let config_path = crate::config::project_settings::get_project_config_path(&repo);
    std::fs::write(&config_path, [0xff, 0xfe]).unwrap();
    let invalid_utf8 = commit_at(&repo, "invalid utf8");

    std::fs::write(&config_path, "checks: [unterminated\n").unwrap();
    let malformed = commit_at(&repo, "malformed yaml");

    std::fs::write(
        &config_path,
        "checks:\n  strict:\n    command: true\n    executor:\n      unknownFutureField: true\n",
    )
    .unwrap();
    let unreadable_selector = commit_at(&repo, "unreadable selector");

    assert!(matches!(
        checks_contract_at_commit_detailed(&repo, &missing),
        Err(CommitChecksContractError::Missing)
    ));
    assert!(matches!(
        checks_contract_at_commit_detailed(&repo, &checkless),
        Err(CommitChecksContractError::NoChecks)
    ));
    assert!(matches!(
        checks_contract_at_commit_detailed(&repo, &invalid_utf8),
        Err(CommitChecksContractError::InvalidUtf8(_))
    ));

    let error = checks_contract_at_commit_detailed(&repo, &malformed).unwrap_err();
    assert!(matches!(&error, CommitChecksContractError::Parse(_)));
    let diagnostic = error.manual_diagnostic(&malformed);
    assert!(diagnostic.contains(".cairn/config.yaml"));
    assert!(diagnostic.contains(&malformed));
    assert!(diagnostic.contains("failed to parse"));

    // A selector this build cannot read costs only its own check: the
    // commit's contract loads, with that check set aside and named.
    let sealed = checks_contract_at_commit_detailed(&repo, &unreadable_selector)
        .expect("an unreadable selector does not fail the commit's contract");
    assert!(sealed.contract.checks.is_empty());
    assert_eq!(sealed.contract.unsupported.len(), 1);
    let strict = &sealed.contract.unsupported["strict"];
    assert_eq!(strict.field, "executor");
    assert_eq!(strict.value, r#"{"unknownFutureField":true}"#);
    assert!(strict.known.is_empty());
    assert!(
        strict.notice.contains("needs a newer Cairn"),
        "{}",
        strict.notice
    );

    let unreadable = "0000000000000000000000000000000000000000";
    let error = checks_contract_at_commit_detailed(&repo, unreadable).unwrap_err();
    assert!(matches!(&error, CommitChecksContractError::Read(_)));
    assert!(error
        .manual_diagnostic(unreadable)
        .contains("failed to read"));

    for commit in [&missing, &checkless] {
        let error = checks_contract_at_commit_detailed(&repo, commit).unwrap_err();
        assert_eq!(
            error.manual_diagnostic(commit),
            format!("commit {commit} declares no configured checks")
        );
    }

    assert!(checks_contract_at_commit(&repo, &invalid_utf8).is_none());
    assert!(checks_contract_at_commit(&repo, &malformed).is_none());
    assert!(checks_contract_at_commit(&repo, &unreadable_selector).is_some());
}

// --- passing-baseline delta selection ---------------------------------

#[test]
fn tree_entry_delta_reports_added_changed_removed_under_impact_globs() {
    let baseline = vec![
        ("src/a.ts".to_string(), "a1".to_string()),
        ("src/b.ts".to_string(), "b1".to_string()),
        ("src/removed.ts".to_string(), "r1".to_string()),
        ("docs/ignored.md".to_string(), "d1".to_string()),
    ];
    let current = vec![
        ("src/a.ts".to_string(), "a2".to_string()),
        ("src/b.ts".to_string(), "b1".to_string()),
        ("src/added.ts".to_string(), "n1".to_string()),
        ("docs/ignored.md".to_string(), "d2".to_string()),
    ];
    let impact = vec!["src/**".to_string()];

    let delta =
        diff_tree_entries_for_selector(&baseline, &current, &InputSelector::from_globs(&impact));
    let observed: Vec<(&str, &str)> = delta
        .iter()
        .map(|change| (change.path.as_str(), change.status.as_str()))
        .collect();
    assert_eq!(
        observed,
        vec![
            ("src/a.ts", "modified"),
            ("src/added.ts", "added"),
            ("src/removed.ts", "deleted"),
        ]
    );
}

const TEST_PLATFORM: &str = "test-platform";
const TEST_TOOLCHAIN: &str = "test-toolchain";

/// A cache row whose `input_hash` is the REAL key for `check` over
/// `baseline_entries` — i.e. a green row genuinely written under that contract.
/// Narrowing now requires this, so a row with a synthetic hash reads as
/// contract-mismatched and correctly declines to narrow.
fn cache_entry_under(
    check_name: &str,
    tree_hash: &str,
    passed: bool,
    check: &CheckCommand,
    baseline_entries: &[(String, String)],
) -> CheckResultCacheEntry {
    let mut entry = cache_entry(check_name, tree_hash, passed);
    entry.input_hash = check_result_key(
        check,
        &selector(check),
        Some(baseline_entries),
        tree_hash,
        TEST_PLATFORM,
        TEST_TOOLCHAIN,
        CheckChange::unknown("test"),
    );
    entry
}

fn delta_for(
    row: Option<&CheckResultCacheEntry>,
    baseline: Option<&[(String, String)]>,
    current: Option<&[(String, String)]>,
    check: &CheckCommand,
    cumulative: &[GraphFileChange],
) -> Vec<GraphFileChange> {
    baseline_delta_changed_files(
        row,
        baseline,
        current,
        check,
        &selector(check),
        &selector(check),
        TEST_PLATFORM,
        TEST_TOOLCHAIN,
        cumulative,
    )
}

#[test]
fn baseline_decision_uses_delta_only_from_passing_baseline() {
    let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
    let current = vec![
        ("src/a.ts".to_string(), "a1".to_string()),
        ("src/b.ts".to_string(), "b1".to_string()),
    ];
    let frontend = check(
        "bunx vitest related {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let cumulative = vec![change("src/a.ts"), change("src/b.ts")];
    let passing = cache_entry_under("frontend", "tree-a", true, &frontend, &baseline);
    let failing = cache_entry_under("frontend", "tree-a", false, &frontend, &baseline);

    let narrowed = delta_for(
        Some(&passing),
        Some(&baseline),
        Some(&current),
        &frontend,
        &cumulative,
    );
    assert_eq!(
        narrowed.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
        vec!["src/b.ts"]
    );

    let from_fail = delta_for(
        Some(&failing),
        Some(&baseline),
        Some(&current),
        &frontend,
        &cumulative,
    );
    assert_eq!(from_fail, cumulative);

    let from_missing = delta_for(
        None,
        Some(&baseline),
        Some(&current),
        &frontend,
        &cumulative,
    );
    assert_eq!(from_missing, cumulative);
}

#[test]
fn baseline_decision_falls_back_to_cumulative_on_empty_or_uncertain_delta() {
    let entries = vec![("src/a.ts".to_string(), "a1".to_string())];
    let frontend = check(
        "bunx vitest related {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let cumulative = vec![change("src/a.ts")];
    let passing = cache_entry_under("frontend", "tree-a", true, &frontend, &entries);

    let empty_delta = delta_for(
        Some(&passing),
        Some(&entries),
        Some(&entries),
        &frontend,
        &cumulative,
    );
    assert_eq!(empty_delta, cumulative);

    let unreadable_current =
        delta_for(Some(&passing), Some(&entries), None, &frontend, &cumulative);
    assert_eq!(unreadable_current, cumulative);
}

/// The checks contract is re-read from the LIVE project config on every commit,
/// so a green verdict can outlive the definition that produced it. Widening
/// `impact` is the dangerous direction: the old run never examined the newly
/// covered tree, yet diffing that same tree under the new globs reports the file
/// as unchanged and drops it from the selector. Nothing else catches this — the
/// input-hash cache correctly misses, and the row-ranking query cannot know which
/// contract a row was written under.
#[test]
fn widened_impact_discards_a_baseline_that_never_covered_the_new_glob() {
    // The green run examined only `src/**`, so the UI file in its tree went
    // unchecked even though it sat right there.
    let baseline = vec![
        ("packages/ui/x.ts".to_string(), "x1".to_string()),
        ("src/a.ts".to_string(), "a1".to_string()),
    ];
    let narrow = check(
        "bunx vitest related {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let green = cache_entry_under("frontend", "tree-a", true, &narrow, &baseline);

    // The user then widens the live contract to cover packages/ui too.
    let widened = check(
        "bunx vitest related {changedFiles}",
        Some(&["src/**", "packages/ui/**"]),
        CheckWhen::Write,
    );
    let current = vec![
        ("packages/ui/x.ts".to_string(), "x1".to_string()),
        ("src/a.ts".to_string(), "a1".to_string()),
        ("src/b.ts".to_string(), "b1".to_string()),
    ];
    let cumulative = vec![
        change("packages/ui/x.ts"),
        change("src/a.ts"),
        change("src/b.ts"),
    ];

    let selected = delta_for(
        Some(&green),
        Some(&baseline),
        Some(&current),
        &widened,
        &cumulative,
    );

    assert_eq!(
        selected, cumulative,
        "a baseline written under narrower impact must not anchor narrowing"
    );
    assert!(
        selected.iter().any(|c| c.path == "packages/ui/x.ts"),
        "the newly covered file is unchanged since the baseline tree, so only \
             discarding that baseline keeps it in the selector"
    );
}

/// The same-contract case still narrows: the gate must not be so blunt that it
/// disables the optimization it guards.
#[test]
fn unchanged_contract_still_narrows_to_the_tree_delta() {
    let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
    let current = vec![
        ("src/a.ts".to_string(), "a1".to_string()),
        ("src/b.ts".to_string(), "b1".to_string()),
    ];
    let frontend = check(
        "bunx vitest related {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let green = cache_entry_under("frontend", "tree-a", true, &frontend, &baseline);
    let cumulative = vec![change("src/a.ts"), change("src/b.ts")];

    let selected = delta_for(
        Some(&green),
        Some(&baseline),
        Some(&current),
        &frontend,
        &cumulative,
    );
    assert_eq!(
        selected.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
        vec!["src/b.ts"]
    );
}

/// Editing the command is the other contract change that invalidates a baseline:
/// the green verdict was produced by a different runner invocation.
#[test]
fn changed_command_discards_the_baseline() {
    let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
    let current = vec![
        ("src/a.ts".to_string(), "a1".to_string()),
        ("src/b.ts".to_string(), "b1".to_string()),
    ];
    let before = check(
        "bunx vitest related {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let after = check(
        "bunx vitest related --reporter=json {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let green = cache_entry_under("frontend", "tree-a", true, &before, &baseline);
    let cumulative = vec![change("src/a.ts"), change("src/b.ts")];

    let selected = delta_for(
        Some(&green),
        Some(&baseline),
        Some(&current),
        &after,
        &cumulative,
    );
    assert_eq!(selected, cumulative);
}

#[test]
fn passing_baseline_delta_replans_changed_files_selector_to_new_file_only() {
    // Commit A touched f1 and passed, so the cached baseline tree contains f1.
    let baseline = vec![("src/f1.ts".to_string(), "f1-a".to_string())];
    // Commit B touches f2. The cumulative branch diff still contains f1 and f2,
    // but a passing baseline makes the safe selector just the tree delta: f2.
    let current = vec![
        ("src/f1.ts".to_string(), "f1-a".to_string()),
        ("src/f2.ts".to_string(), "f2-b".to_string()),
    ];
    let check = check(
        "bunx vitest related --reporter=json {changedFiles}",
        Some(&["src/**"]),
        CheckWhen::Write,
    );
    let cumulative = vec![change("src/f1.ts"), change("src/f2.ts")];
    let passing = cache_entry_under("frontend", "tree-a", true, &check, &baseline);
    let selected = delta_for(
        Some(&passing),
        Some(&baseline),
        Some(&current),
        &check,
        &cumulative,
    );

    let plan = replan_one_check(
        "frontend",
        &check,
        &inputs_for("frontend", &check),
        &selected,
        Path::new("/repo"),
    )
    .unwrap();
    assert_eq!(
        plan.command,
        "bunx vitest related --reporter=json src/f2.ts"
    );
    assert_eq!(plan.scope, CheckScope::Partial);
}

// --- the turn-end-cadence gate ----------------------------------------

/// A checks map with one check per cadence, all scoped to `src/**`.
fn cadence_checks() -> HashMap<String, CheckCommand> {
    let mut checks = HashMap::new();
    checks.insert(
        "w".to_string(),
        check("run-w", Some(&["src/**"]), CheckWhen::Write),
    );
    checks.insert(
        "r".to_string(),
        check("run-r", Some(&["src/**"]), CheckWhen::Review),
    );
    checks.insert(
        "m".to_string(),
        check("run-m", Some(&["src/**"]), CheckWhen::Main),
    );
    checks
}

#[test]
fn no_branch_wave_runs_a_main_only_check() {
    let checks = cadence_checks();
    let changed = [change("src/App.tsx")];
    let write: Vec<String> =
        applicable_write_checks(&checks, &inputs(&checks), &changed, Path::new("/repo"))
            .into_iter()
            .map(|plan| plan.name)
            .collect();
    assert_eq!(write, vec!["w"]);
}

#[test]
fn turn_end_gate_runs_review_not_write() {
    let plans = applicable_turn_end_checks(
        &cadence_checks(),
        &inputs(&cadence_checks()),
        &[change("src/App.tsx")],
        Path::new("/repo"),
    );
    let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["r"],
        "review runs at every turn-end; write never runs here"
    );
}

#[test]
fn turn_end_selection_keeps_advisory_and_gate_review_checks() {
    let mut checks = cadence_checks();
    let mut gate = check("run-gate", Some(&["src/**"]), CheckWhen::Review);
    gate.policy = CheckPolicy::Gate;
    checks.insert("g".to_string(), gate);

    let plans = applicable_turn_end_checks(
        &checks,
        &inputs(&checks),
        &[change("src/App.tsx")],
        Path::new("/repo"),
    );
    let names: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
    assert_eq!(names, vec!["g", "r"]);
}

#[test]
fn combined_tree_selection_excludes_default_advisory_review_checks() {
    let default_advisory: CheckCommand =
        serde_yaml::from_str("command: run-advisory\nimpact:\n  - src/**\nwhen: review\n").unwrap();
    assert_eq!(default_advisory.policy, CheckPolicy::Advisory);
    let checks = HashMap::from([("advisory".to_string(), default_advisory)]);

    let plans = applicable_combined_tree_gate_checks(
        &checks,
        &inputs(&checks),
        &[change("src/App.tsx")],
        Path::new("/repo"),
    );
    assert!(plans.is_empty());
}

#[test]
fn combined_tree_selection_keeps_only_applicable_review_gates() {
    let mut applicable = check("run-gate", Some(&["src/**"]), CheckWhen::Review);
    applicable.policy = CheckPolicy::Gate;
    let mut impact_mismatch = check("run-docs", Some(&["docs/**"]), CheckWhen::Review);
    impact_mismatch.policy = CheckPolicy::Gate;
    let mut write = check("run-write", Some(&["src/**"]), CheckWhen::Write);
    write.policy = CheckPolicy::Gate;
    let checks = HashMap::from([
        ("applicable".to_string(), applicable),
        ("impact-mismatch".to_string(), impact_mismatch),
        ("write".to_string(), write),
    ]);

    let plans = applicable_combined_tree_gate_checks(
        &checks,
        &inputs(&checks),
        &[change("src/App.tsx")],
        Path::new("/repo"),
    );
    let names: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
    assert_eq!(names, vec!["applicable"]);
}

#[test]
fn turn_end_gate_runs_an_idle_aliased_check() {
    // `when: idle` in a project config deserializes to CheckWhen::Review, so
    // an un-migrated check still runs at turn-end (the alias path).
    let aliased: CheckWhen = serde_yaml::from_str("idle").unwrap();
    assert_eq!(aliased, CheckWhen::Review);
    let mut checks = HashMap::new();
    checks.insert(
        "legacy".to_string(),
        check("run", Some(&["src/**"]), aliased),
    );
    let plans = applicable_turn_end_checks(
        &checks,
        &inputs(&checks),
        &[change("src/App.tsx")],
        Path::new("/repo"),
    );
    let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["legacy"]);
}

#[test]
fn turn_end_gate_excludes_a_non_matching_impact() {
    // A doc-only change matches no impact glob, so nothing applies.
    let plans = applicable_turn_end_checks(
        &cadence_checks(),
        &inputs(&cadence_checks()),
        &[change("docs/x.md")],
        Path::new("/repo"),
    );
    assert!(plans.is_empty());
}

// --- summary formatting -----------------------------------------------

/// A bare outcome with no structured detail and no output tail, so the
/// summary renders only the header status line.
fn outcome(name: &str, passed: bool, exit_code: Option<i32>) -> CheckOutcome {
    CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: name.to_string(),
        passed,
        exit_code,
        failure_kind: None,
        parsed: None,
        output_tail: String::new(),
        cached: false,
        recorded: None,
        duration_ms: 0,
        suppressed_after: None,
        not_recorded: None,
    }
}

#[test]
fn an_off_pin_green_reads_unverified_inline_and_is_never_reused() {
    let output = "Ran 716 tests across 56 files.\nCAIRN_CHECK_RESULT_JSON={\"schemaVersion\":1,\"parser\":\"bun-test\",\"unverified\":\"runtime bun 1.3.13 ≠ pinned 1.4.4\"}\n";
    let mut off_pin = outcome("api", true, Some(0));
    off_pin.parsed =
        crate::execution::check_parsers::parse_check_output("bun run test:api", output);
    assert_eq!(
        summary_annotation(&off_pin).as_deref(),
        Some("unverified — runtime bun 1.3.13 ≠ pinned 1.4.4")
    );
    let summary = format_check_summary(std::slice::from_ref(&off_pin), &HashMap::new());
    assert!(summary.starts_with("⚠ api (unverified"), "{summary}");
    assert!(!check_reuse_decision("bun run test:api", None, off_pin.parsed.as_ref()).reusable);

    // On the pinned runtime the lane declares nothing: the exit code stands.
    let pinned = outcome("api", true, Some(0));
    let summary = format_check_summary(std::slice::from_ref(&pinned), &HashMap::new());
    assert!(summary.starts_with("✓ api"), "{summary}");
    assert!(check_reuse_decision("bun run test:api", None, None).reusable);
}

/// A test-runner parse with explicit pass/fail counts, so the summary's
/// count-bearing annotations are exercised without a real runner.
fn runner_parse(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
    ParsedCheckResult {
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: parser.to_string(),
        passed,
        failed,
        skipped: 0,
        suite_failures: 0,
        phases: None,
        deferred: 0,
        report: None,
        unverified: None,
        failures: (0..failed)
            .map(|i| crate::execution::check_parsers::CheckFailure {
                name: format!("t{i}"),
                message: None,
            })
            .collect(),
        lint_findings: None,
        plan_audit: None,
        accounting: None,
        ingestion: None,
    }
}

/// Rulings 695/691: a label that merges populations destroys attribution. A
/// fast-tier green names how many it ran and deferred, and a red one still
/// says what it deferred beside its failures.
#[test]
fn a_fast_tier_verdict_never_renders_as_a_bare_green() {
    let fast = |passed, failed| {
        let mut parse = runner_parse("nextest", passed, failed);
        parse.selection = "fast".to_string();
        parse.complete = true;
        parse.deferred = 3_596;
        parse
    };
    let mut green = outcome("rust-tests", true, Some(0));
    green.parsed = Some(fast(8_501, 0));
    let annotation = summary_annotation(&green).unwrap();
    assert!(
        annotation.starts_with("fast tier: 8501 ran, 3596 deferred"),
        "{annotation}"
    );
    assert!(!annotation.contains("8501 tests"), "{annotation}");

    let mut red = outcome("rust-tests", false, Some(100));
    red.parsed = Some(fast(8_499, 2));
    let annotation = summary_annotation(&red).unwrap();
    assert!(
        annotation.starts_with("2 of 8501 failed, exit 100"),
        "{annotation}"
    );
    assert!(
        annotation.contains("fast tier: 8501 ran, 3596 deferred"),
        "{annotation}"
    );
}

#[test]
fn summary_renders_pass_and_fail() {
    // No structured detail / output ⇒ header line only.
    let s = format_check_summary(
        &[
            outcome("frontend", true, Some(0)),
            outcome("typecheck", false, Some(1)),
        ],
        &HashMap::new(),
    );
    assert_eq!(s, "\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)");
}

/// A write-cadence test lane's red is split by what main at the base
/// observed, as the turn-checks wake splits it: the tally carries the
/// headline, and each bucket is named beneath it.
#[test]
fn summary_splits_a_red_test_lane_by_main_at_the_base() {
    use crate::execution::checks_base::{
        partition,
        tests::{failures, main_row},
    };
    use crate::execution::checks_main::TestRate;
    let parsed: crate::execution::check_parsers::ParsedCheckResult =
        serde_json::from_value(serde_json::json!({
            "schemaVersion": 1, "complete": true, "selection": "full",
            "parser": "nextest", "passed": 897, "failed": 3, "skipped": 0,
            "failures": [{"name": "core mine"}, {"name": "core main"}, {"name": "core flaky"}],
        }))
        .unwrap();
    let red = CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: "rust-tests".to_string(),
        passed: false,
        exit_code: Some(100),
        failure_kind: None,
        parsed: Some(parsed),
        output_tail: String::new(),
        cached: false,
        recorded: None,
        duration_ms: 0,
        suppressed_after: None,
        not_recorded: None,
    };
    let main = main_row(
        "base0000",
        &[
            ("core mine", "passed"),
            ("core main", "failed"),
            ("core flaky", "passed"),
        ],
    );
    let attribution = partition(
        "base0000",
        "linux",
        &failures(&["core mine", "core main", "core flaky"]),
        Some(&main),
        Some(0),
    )
    .with_main_rates(Ok(HashMap::from([(
        "core flaky".to_string(),
        TestRate { red: 3, ran: 10 },
    )])));
    let attributions = HashMap::from([("rust-tests".to_string(), attribution)]);

    let s = format_check_summary(&[red], &attributions);

    assert!(
        s.contains(
            "\u{2717} rust-tests \u{2014} 3 of 900 failed; 1 passed at base and failed here, \
                 1 red at base too, 1 intermittent on main\n"
        ),
        "{s}"
    );
    assert!(
        s.contains("\n    1 also red at base `base0000` on linux")
            && s.contains("e884003b): core main"),
        "{s}"
    );
    assert!(s.contains("not proof of cause: core mine"), "{s}");
    assert!(
        s.contains(
            "(intermittent on main): core flaky (red on 3 of the last 10 main rows that ran it)"
        ),
        "{s}"
    );
    assert!(!s.contains("this branch's"), "{s}");
    assert!(
        !s.contains("3 failed: "),
        "the unattributed list is replaced: {s}"
    );
}

#[test]
fn summary_renders_spawn_failure_without_exit_code() {
    let s = format_check_summary(&[outcome("frontend", false, None)], &HashMap::new());
    assert_eq!(s, "\u{2717} frontend (failed to run)");
}

#[test]
fn summary_appends_failing_test_names_and_excerpt() {
    let parsed = crate::execution::check_parsers::parse_check_output(
        "bunx tsc --noEmit",
        "a.ts(1,7): error TS2322: Type 'string' is not assignable to type 'number'.",
    );
    let results = vec![CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: "typecheck".to_string(),
        passed: false,
        exit_code: Some(1),
        failure_kind: None,
        parsed,
        output_tail: "raw output tail".to_string(),
        cached: false,
        recorded: None,
        duration_ms: 0,
        suppressed_after: None,
        not_recorded: None,
    }];
    let s = format_check_summary(&results, &HashMap::new());
    // Header status line first.
    assert!(s.starts_with("\u{2717} typecheck (exit 1)"));
    // Then a detail block naming the failing test and quoting the error.
    assert!(s.contains("\u{2717} typecheck \u{2014} 1 failed: a.ts(1,7)"));
    assert!(s.contains("TS2322: Type 'string' is not assignable"));
}

/// Build a passing/failing outcome carrying a runner parse, for the
/// count-bearing annotation tests.
fn parsed_outcome(
    name: &str,
    passed: bool,
    exit_code: Option<i32>,
    parsed: ParsedCheckResult,
    cached: bool,
) -> CheckOutcome {
    CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: name.to_string(),
        passed,
        exit_code,
        failure_kind: None,
        parsed: Some(parsed),
        output_tail: String::new(),
        cached,
        recorded: None,
        duration_ms: 0,
        suppressed_after: None,
        not_recorded: None,
    }
}

#[test]
fn summary_shows_test_count_on_a_passing_runner_check() {
    let o = parsed_outcome(
        "frontend",
        true,
        Some(0),
        runner_parse("vitest", 12, 0),
        false,
    );
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2713} frontend (12 tests)"
    );
}

#[test]
fn summary_flags_a_zero_selection_pass_honestly() {
    // A `related` selector that matched nothing exits 0 but validated nothing:
    // the annotation must say so rather than render a bare green.
    let o = parsed_outcome(
        "frontend",
        true,
        Some(0),
        runner_parse("vitest", 0, 0),
        false,
    );
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2713} frontend (no tests matched the change)"
    );
}

#[test]
fn summary_names_skipped_tests_inside_a_passing_suite() {
    // A skip is not a pass. A green whose suite skipped part of itself has to
    // read differently from one that ran everything (CAIRN-3164).
    let mut parse = runner_parse("nextest", 5009, 0);
    parse.skipped = 44;
    let o = parsed_outcome("rust-full", true, Some(0), parse, false);
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2713} rust-full (5009 tests, 44 skipped)"
    );
}

#[test]
fn summary_distinguishes_a_wholly_self_skipped_suite_from_a_zero_selection() {
    // The CAIRN-3112 shape: every test in the suite skipped, so the runner
    // reported zero executed. "no tests matched the change" would be a lie
    // about WHY nothing ran.
    let mut parse = runner_parse("nextest", 0, 0);
    parse.skipped = 12;
    let o = parsed_outcome("rust-full", true, Some(0), parse, true);
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2713} rust-full (no tests ran, 12 skipped, cached)"
    );
}

#[test]
fn summary_names_a_suite_collection_failure_instead_of_a_zero_tally() {
    // 881 tests passed and none failed, yet the check is red because one file
    // never got as far as running a test. Folding that into the assertion
    // tally renders "0 of 881 failed": a red check pointing at nothing.
    let mut parse = runner_parse("vitest", 881, 0);
    parse.suite_failures = 1;
    parse.failures = vec![crate::execution::check_parsers::CheckFailure {
        name: "src/components/FileTabView.test.tsx".to_string(),
        message: Some(
            "Cannot find module '../../packages/ui/src/lib/readableMarkdown'".to_string(),
        ),
    }];
    let o = parsed_outcome("frontend-partial", false, Some(1), parse, false);
    let s = format_check_summary(&[o], &HashMap::new());
    assert!(
        s.starts_with("\u{2717} frontend-partial (1 suite failed to load, exit 1)"),
        "got: {s}"
    );
    assert!(
        s.contains("src/components/FileTabView.test.tsx"),
        "got: {s}"
    );
    assert!(s.contains("Cannot find module"), "got: {s}");
}

#[test]
fn summary_names_undeclared_skips_instead_of_a_zero_failure_tally() {
    // Every test that ran passed, but jj was missing, so the jj-gated tests
    // skipped without a declaration and the ledger failed the run. The red
    // has to point at the skips, not read "0 of 4800 failed".
    let mut parse = runner_parse("nextest", 4800, 0);
    parse.skipped = 236;
    parse.undeclared_skips = 236;
    let o = parsed_outcome("rust-tests", false, Some(1), parse, false);
    let s = format_check_summary(&[o], &HashMap::new());
    assert!(
        s.starts_with("\u{2717} rust-tests (236 undeclared skips, exit 1)"),
        "got: {s}"
    );
}

fn audit_block(
    state: &str,
    selection: &str,
    gating: usize,
) -> crate::execution::check_parsers::ParsedPlanAudit {
    let rows: Vec<_> = (0..gating)
        .map(|index| {
            serde_json::json!({
                "id": format!("a.rs|wide-scan|events|{index}"), "status": "failed",
                "file": "a.rs", "rule": "wide-scan", "table": "events",
                "statement": index.to_string(), "lines": [1], "columns": [],
            })
        })
        .collect();
    serde_json::from_value(serde_json::json!({
        "state": state, "selection": selection, "explained": 3, "rows": rows,
    }))
    .unwrap()
}

/// A runner that exits 1 for its audit on a tree whose tests all passed has
/// named its red, so it is no "runner failed outside any test". Any other
/// exit stays the runner's own, however many findings sit beside it.
#[test]
fn a_gating_audit_on_a_green_run_is_an_ordinary_named_failure() {
    use crate::execution::check_parsers::PLAN_AUDIT_EXIT_CODE;
    let mut parse = runner_parse("nextest", 4800, 0);
    parse.plan_audit = Some(audit_block("evaluated", "full", 2));
    assert!(classify_check_failure(
        "bun run test:rust:full",
        Some(PLAN_AUDIT_EXIT_CODE),
        false,
        false,
        Some(&parse),
        "",
    )
    .is_none());
    assert!(parse.exit_is_plan_audits(Some(PLAN_AUDIT_EXIT_CODE)));
    assert!(!parse.exit_is_plan_audits(Some(101)));

    let o = parsed_outcome("rust-tests", false, Some(1), parse, false);
    let s = format_check_summary(&[o], &HashMap::new());
    assert!(
        s.starts_with("\u{2717} rust-tests (plan audit: 2 gating, all 4800 passed, exit 1)"),
        "got: {s}"
    );
}

/// A write wave's attributed detail line counts the audit too, not only the
/// tests: a tally read without the failure names leaves an all-passed lane
/// with nothing to say before its charge clause.
#[test]
fn an_attributed_summary_leads_with_a_gating_audit() {
    use crate::execution::check_parsers::CheckFailure;
    use crate::execution::checks_base::partition;

    const FINDING: &str = "plan-audit a.rs|wide-scan|events|0";
    let summary = |passed, tests: &[&str]| {
        let mut parse = runner_parse("nextest", passed, tests.len());
        parse.plan_audit = Some(audit_block("evaluated", "full", 1));
        parse.failures = tests
            .iter()
            .chain([&FINDING])
            .map(|name| CheckFailure {
                name: name.to_string(),
                message: None,
            })
            .collect();
        let names: Vec<crate::execution::checks_base::BranchFailure> = parse
            .failures
            .iter()
            .map(|f| crate::execution::checks_base::BranchFailure {
                name: f.name.clone(),
                signature: None,
            })
            .collect();
        let attributions = HashMap::from([(
            "rust-tests".to_string(),
            partition("4a52d52e0000", "linux", &names, None, None),
        )]);
        let o = parsed_outcome("rust-tests", false, Some(1), parse, false);
        format_check_summary(&[o], &attributions)
    };

    let clean = summary(4800, &[]);
    assert!(
        clean.contains("\u{2014} plan audit: 1 gating, all 4800 passed; "),
        "{clean}"
    );
    assert!(!clean.contains("\u{2014} ;"), "{clean}");

    let beside_tests = summary(38, &["t0", "t1"]);
    assert!(
        beside_tests.contains("\u{2014} plan audit: 1 gating, 2 of 40 failed; "),
        "{beside_tests}"
    );
}

/// A whole suite always explains statements, so its unjudged audit is worth
/// a fresh run. A narrower selection can honestly reach none.
#[test]
fn only_a_whole_suites_unjudged_audit_refuses_reuse() {
    let result = |state: &str, selection: &str| {
        let mut parse = runner_parse("nextest", 10, 0);
        parse.complete = true;
        parse.selection = selection.to_string();
        parse.plan_audit = Some(audit_block(state, selection, 0));
        parse
    };
    let reusable = |parse: &ParsedCheckResult| {
        check_reuse_decision("bun run test:rust:full", None, Some(parse)).reusable
    };
    assert!(!reusable(&result("not-evaluated", "full")));
    assert!(reusable(&result("not-evaluated", "filtered")));
    assert!(reusable(&result("evaluated", "full")));
}

#[test]
fn summary_counts_failing_tests_and_uncollected_suites_separately() {
    let mut parse = runner_parse("vitest", 38, 2);
    parse.suite_failures = 3;
    let o = parsed_outcome("frontend-partial", false, Some(1), parse, false);
    let s = format_check_summary(&[o], &HashMap::new());
    assert!(
        s.starts_with(
            "\u{2717} frontend-partial (2 of 40 failed, 3 suites failed to load, exit 1)"
        ),
        "got: {s}"
    );
}

#[test]
fn summary_shows_pass_of_total_on_a_failing_runner_check() {
    let o = parsed_outcome(
        "rust",
        false,
        Some(101),
        runner_parse("nextest", 38, 2),
        false,
    );
    let s = format_check_summary(&[o], &HashMap::new());
    assert!(
        s.starts_with("\u{2717} rust (2 of 40 failed, exit 101)"),
        "got: {s}"
    );
}

#[test]
fn summary_shows_duration_on_a_passing_unparsed_check() {
    // typecheck / api have no test-runner parse; a fresh pass shows duration.
    let mut o = outcome("typecheck", true, Some(0));
    o.duration_ms = 4100;
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2713} typecheck (4.1s)"
    );
}

#[test]
fn summary_annotates_a_cache_hit() {
    // A reused verdict is distinguishable from a fresh run. Duration is
    // suppressed for a cache hit (it belonged to the original run).
    let mut o = outcome("typecheck", true, Some(0));
    o.cached = true;
    o.duration_ms = 4100;
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2713} typecheck (cached)"
    );

    // A cached test-runner pass keeps its count AND flags the reuse.
    let cached_runner = parsed_outcome(
        "frontend",
        true,
        Some(0),
        runner_parse("vitest", 7, 0),
        true,
    );
    assert_eq!(
        format_check_summary(&[cached_runner], &HashMap::new()),
        "\u{2713} frontend (7 tests, cached)"
    );
}

#[test]
fn fixed_summary_attributes_a_combined_delta_once_at_batch_level() {
    let summary = "Checks: ✓ rust-fmt (2.1s) · ✓ migrations (cached)";
    let rendered = format_fixed_batch_summary(
        summary,
        "1234567890abcdef",
        &["src/lib.rs".into(), "src/main.rs".into()],
    );
    assert_eq!(
        rendered,
        "Checks: ✓ write-check fixes (fixed, 1234567890ab; 2 files) · ✓ rust-fmt (2.1s) · ✓ migrations (cached)"
    );
    assert!(!rendered.contains("migrations (fixed"));
}

// --- one wave per commit: fixers first, then re-key, never re-run --------

/// A declared fixer — the formatter shape this contract exists for.
fn fixing_check(command: &str, impact: Option<&[&str]>) -> CheckCommand {
    CheckCommand {
        fixes: true,
        main_cadence: None,
        ..check(command, impact, CheckWhen::Write)
    }
}

/// The repro's wave: this repository's write cadence, where the formatter
/// sorts LAST by name and every other check reads the Rust sources it
/// rewrites.
fn write_wave() -> (HashMap<String, CheckCommand>, Vec<(CheckPlan, String)>) {
    let checks = HashMap::from([
        (
            "api".to_string(),
            check("bun run test:api", Some(&["api/**"]), CheckWhen::Write),
        ),
        (
            "lockfile".to_string(),
            check(
                "bun run check:lockfile",
                Some(&["src-tauri/**/Cargo.toml"]),
                CheckWhen::Write,
            ),
        ),
        (
            "migrations".to_string(),
            check(
                "bun run check:migrations",
                Some(&["src-tauri/**/*.rs"]),
                CheckWhen::Write,
            ),
        ),
        (
            "rust-fmt".to_string(),
            fixing_check("bun run fmt", Some(&["src-tauri/**/*.rs"])),
        ),
    ]);
    // Plans are ordered by name, which is exactly where the formatter lands
    // last and the cascade came from.
    let keyed: Vec<(CheckPlan, String)> = ["api", "lockfile", "migrations", "rust-fmt"]
        .into_iter()
        .map(|name| {
            (
                plan(name, &checks[name].command),
                format!("key-before-{name}"),
            )
        })
        .collect();
    (checks, keyed)
}

#[test]
fn a_declared_fixer_is_submitted_before_the_checks_that_read_its_output() {
    let (checks, keyed) = write_wave();
    let order = fixer_first_submission_order(&keyed, &checks, &[0, 1, 2, 3]);
    let names: Vec<&str> = order.iter().map(|i| keyed[*i].0.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["rust-fmt", "api", "lockfile", "migrations"],
        "the fixer runs first; every other check keeps plan order"
    );
    // Indices themselves are untouched, so the status board, the per-check
    // output streams, and the rendered summary all stay in plan order.
    let mut covered = order.clone();
    covered.sort_unstable();
    assert_eq!(covered, vec![0, 1, 2, 3]);
}

/// #435: a lane declaring only a platform the write cell is not on leaves
/// the cell as a verdict-only lane instead of waiting on a machine that can
/// never answer it. A lane that admits the cell's platform stays, and a
/// declared fixer the cell cannot run is refused, because its fix can fold
/// only from the write cell.
#[test]
fn a_write_wave_splits_by_the_write_cell_platform() {
    let (mut checks, keyed) = write_wave();
    let here = crate::execution::check_identity::gating_platform();
    let elsewhere = if here == "linux" { "macos" } else { "linux" };
    checks.get_mut("api").unwrap().verdict_platforms = Some(vec![elsewhere.to_string()]);
    checks.get_mut("lockfile").unwrap().verdict_platforms =
        Some(vec![here.to_string(), elsewhere.to_string()]);

    let split = split_write_wave(&keyed, &checks, &[0, 1, 2, 3], here, "bgsrv-mac");
    assert_eq!(
        split.cell,
        vec![1, 2, 3],
        "undeclared and admitting lanes stay"
    );
    assert_eq!(
        split.spilled,
        vec![0],
        "the lane naming only another platform leaves"
    );
    assert!(split.refused.is_empty());

    checks.get_mut("rust-fmt").unwrap().verdict_platforms = Some(vec![elsewhere.to_string()]);
    let split = split_write_wave(&keyed, &checks, &[0, 1, 2, 3], here, "bgsrv-mac");
    assert_eq!(split.cell, vec![1, 2]);
    assert_eq!(split.spilled, vec![0]);
    let [(index, refusal)] = split.refused.as_slice() else {
        panic!("the excluded fixer is refused: {:?}", split.refused);
    };
    assert_eq!(*index, 3);
    assert_eq!(
        refusal,
        &format!(
            "check `rust-fmt` fixes files but its verdictPlatforms [{elsewhere}] exclude the write cell on bgsrv-mac ({here}); a fix folds only from the write cell. Widen verdictPlatforms or drop `fixes` in .cairn/config.yaml"
        )
    );

    // Only cache misses are split; a hit never meets placement.
    let split = split_write_wave(&keyed, &checks, &[1], here, "bgsrv-mac");
    assert_eq!(
        split,
        WriteWaveSplit {
            cell: vec![1],
            ..WriteWaveSplit::default()
        }
    );
}

#[test]
fn submission_order_is_plan_order_when_nothing_declares_a_fix() {
    let (mut checks, keyed) = write_wave();
    checks.get_mut("rust-fmt").unwrap().fixes = false;
    assert_eq!(
        fixer_first_submission_order(&keyed, &checks, &[0, 1, 2, 3]),
        vec![0, 1, 2, 3],
        "a wave with no declared fixer must submit exactly as it did before"
    );
}

#[test]
fn a_declared_fixers_fix_re_verifies_nothing_and_never_re_runs_the_fixer() {
    let (checks, keyed) = write_wave();
    let fixed = vec![
        "src-tauri/os/a.rs".to_string(),
        "src-tauri/os/b.rs".to_string(),
    ];
    let resolved = [selector(&checks["rust-fmt"])];
    let fixers: Vec<&InputSelector> = resolved.iter().collect();
    assert!(fix_is_attributed_to_declared_fixers(&fixed, &fixers));
    // The fix changed every Rust-impacted check's inputs, so every key moves.
    // Each verdict still describes the tree that landed: the fixer ran first
    // inside the shared slot, so the rest of the wave already validated its
    // output — and the fixer's own verdict is a verdict on the tree it just
    // produced. Nothing here executes twice.
    for (plan, key_before) in &keyed {
        let key_after = format!("key-after-{}", plan.name);
        assert!(
            verdict_survives_fix(true, true, true, false, key_before, &key_after, false),
            "{} must not run again against a fix it already saw",
            plan.name
        );
    }
}

#[test]
fn a_verdict_the_fix_invalidated_is_re_verified_rather_than_re_keyed() {
    // Answered from the cache BEFORE the fix: it never saw the fixed tree, so
    // its verdict cannot be keyed onto it. Re-verifying is the only honest
    // answer; re-keying it would be exactly the false green this forbids.
    assert!(!verdict_survives_fix(
        true, false, true, false, "before", "after", false
    ));
    // Unless the fix left its inputs alone — the same argument the result
    // cache itself rests on.
    assert!(verdict_survives_fix(
        true, false, true, false, "same", "same", false
    ));
    // An unattributed fix invalidates even a check that ran: something else
    // rewrote the tree mid-wave and nothing proves this check saw it.
    assert!(!verdict_survives_fix(
        true, true, false, false, "before", "after", false
    ));
    // A fixer another fixer ran after cannot carry: it never saw the output
    // that landed, however well the fold is attributed.
    assert!(!verdict_survives_fix(
        true, true, true, true, "before", "after", false
    ));
    // Unless the later fixer left its inputs alone.
    assert!(verdict_survives_fix(
        true, true, true, true, "same", "same", false
    ));
}

/// Wave A answered pending, the agent committed B, and A's formatter fix
/// then merged onto B. A's suite passed on A's tree; nothing ran B's code.
/// That pass must not become B+fix's green: it carries only if B+fix left
/// the suite's inputs exactly as A had them.
#[test]
fn a_fix_merged_onto_a_newer_head_carries_no_executed_verdict() {
    assert!(!verdict_survives_fix(
        false,
        true,
        true,
        false,
        "a-tree",
        "b-plus-fix",
        false
    ));
    assert!(verdict_survives_fix(
        false, true, true, false, "same", "same", false
    ));
    // The same pass carries when the fix landed on the tree it ran against.
    assert!(verdict_survives_fix(
        true,
        true,
        true,
        false,
        "a-tree",
        "a-plus-fix",
        false
    ));
}

#[test]
fn a_fix_outside_every_declared_fixers_impact_is_not_attributed() {
    let fmt = fixing_check("bun run fmt", Some(&["src-tauri/**/*.rs"]));
    assert!(fix_is_attributed_to_declared_fixers(
        &["src-tauri/os/a.rs".to_string()],
        &[&selector(&fmt)]
    ));
    // A lockfile the formatter cannot have written: some undeclared check
    // mutated the tree, so the wave falls back to re-verification.
    assert!(!fix_is_attributed_to_declared_fixers(
        &["src-tauri/Cargo.lock".to_string()],
        &[&selector(&fmt)]
    ));
    // A wave with no declared fixer can attribute nothing, so an un-migrated
    // config keeps re-verifying rather than recording verdicts about a tree
    // no check ran against.
    assert!(!fix_is_attributed_to_declared_fixers(
        &["src-tauri/os/a.rs".to_string()],
        &[]
    ));
    // A fixer with no impact globs owns the whole tree.
    let everything = fixing_check("format-everything", None);
    assert!(fix_is_attributed_to_declared_fixers(
        &["anywhere/at/all".to_string()],
        &[&selector(&everything)]
    ));
}

#[test]
fn an_earlier_fixer_never_carries_a_later_fixers_rewrite() {
    // The wave the single-fixer tests cannot reach: Prettier, then
    // `eslint --fix`. Both are declared fixers over the same files, so
    // ESLint can rewrite a file Prettier already passed. Prettier ran first
    // and never saw that rewrite.
    let checks = HashMap::from([
        (
            "prettier".to_string(),
            fixing_check("bunx prettier --write .", Some(&["src/**/*.ts"])),
        ),
        (
            "eslint".to_string(),
            fixing_check("bunx eslint --fix src/", Some(&["src/**/*.ts"])),
        ),
        (
            "typecheck".to_string(),
            check(
                "bunx tsc --noEmit",
                Some(&["src/**/*.ts"]),
                CheckWhen::Write,
            ),
        ),
    ]);
    let keyed = vec![
        (plan("prettier", "bunx prettier --write ."), "p".to_string()),
        (plan("eslint", "bunx eslint --fix src/"), "e".to_string()),
        (plan("typecheck", "bunx tsc --noEmit"), "t".to_string()),
    ];
    let order = fixer_first_submission_order(&keyed, &checks, &[0, 1, 2]);
    assert_eq!(
        order,
        vec![0, 1, 2],
        "both fixers precede the verdict check"
    );

    let superseded = fixers_superseded_by_a_later_fixer(&keyed, &checks, &order);
    assert!(
        superseded.contains(&0),
        "prettier ran before eslint, so eslint's rewrite is behind its back"
    );
    assert!(
        !superseded.contains(&1),
        "eslint is the last fixer: it observed prettier's output and its own"
    );
    assert!(
        !superseded.contains(&2),
        "a non-fixer runs after the whole fixer prefix"
    );

    // The fold is attributed — every path is inside a declared fixer's impact
    // — and prettier did execute. Attribution alone must NOT carry it.
    let fixed = vec!["src/app.ts".to_string()];
    let resolved = [selector(&checks["prettier"]), selector(&checks["eslint"])];
    let fixers: Vec<&InputSelector> = resolved.iter().collect();
    assert!(fix_is_attributed_to_declared_fixers(&fixed, &fixers));
    assert!(
        !verdict_survives_fix(
            true,
            true,
            true,
            superseded.contains(&0),
            "p",
            "p-after",
            false
        ),
        "a green prettier verdict must not be keyed to a tree eslint rewrote"
    );
    // The other two are proven by ordering and carry as before.
    assert!(verdict_survives_fix(
        true,
        true,
        true,
        superseded.contains(&1),
        "e",
        "e-after",
        false
    ));
    assert!(verdict_survives_fix(
        true,
        true,
        true,
        superseded.contains(&2),
        "t",
        "t-after",
        false
    ));
}

#[test]
fn a_lone_fixer_is_never_superseded() {
    // The repository's own wave, and the reason supersession is scoped to
    // fixer-after-fixer: with one declared fixer there is nothing behind it,
    // so rust-fmt still carries its verdict and never re-runs.
    let (checks, keyed) = write_wave();
    let order = fixer_first_submission_order(&keyed, &checks, &[0, 1, 2, 3]);
    assert!(
        fixers_superseded_by_a_later_fixer(&keyed, &checks, &order).is_empty(),
        "a single-fixer wave keeps the one-execution-per-commit contract intact"
    );
}

#[test]
fn a_fixer_that_keeps_rewriting_the_tree_terminates() {
    // Termination is structural now that nothing recurses. A wave publishes
    // at most ONE fix commit, and the fixer's verdict survives that fix, so
    // the wave never asks it for a second one. A further mutation can only
    // come from the bounded verification batch, whose delta is reported as
    // non-convergent and never folded (`FixedWave::non_convergent`).
    let checks = HashMap::from([(
        "fmt".to_string(),
        fixing_check("rewrite-forever", Some(&["**/*.rs"])),
    )]);
    let keyed = vec![(plan("fmt", "rewrite-forever"), "key-before".to_string())];
    assert_eq!(fixer_first_submission_order(&keyed, &checks, &[0]), vec![0]);
    assert!(fix_is_attributed_to_declared_fixers(
        &["a.rs".to_string()],
        &[&selector(&checks["fmt"])]
    ));
    assert!(
        verdict_survives_fix(true, true, true, false, "key-before", "key-after", false),
        "re-checking a fixer against its own output is the loop this removes"
    );
}

// --- timeout budgets + failure classification -------------------------

#[test]
fn sandbox_denial_evidence_is_legible_without_overwriting_command_output() {
    let mut output = "1,412 tests passed".to_string();
    append_sandbox_denial_evidence(
        &mut output,
        &[cairn_common::executor_protocol::SandboxDenialEvidence {
            denial: cairn_common::executor_protocol::SandboxDenial::Path("/tmp/tool-cache".into()),
            operation: Some("file-write-create".into()),
            command: "bunx vitest run".into(),
            stream_id: "turn-checks:1".into(),
        }],
    );

    assert!(output.starts_with("1,412 tests passed"));
    assert!(output.contains("operation=file-write-create"));
    assert!(output.contains("path=/tmp/tool-cache"));
    assert!(output.contains("command=bunx vitest run"));
    assert!(output.contains("stream=turn-checks:1"));
}

/// A lane's own output may print a lowercase errno denial, which the
/// executor records as command-scoped evidence. The red it carries is the
/// test's red: neither the text nor the appended evidence is infrastructure.
#[test]
fn lowercase_denial_in_lane_output_stays_an_ordinary_red() {
    let mut output = "--- FAIL: TestRuntimeLock\n    lock_test.go:12: open /run/user/1000/libpod/tmp/alive.lck: permission denied\nFAIL".to_string();
    append_sandbox_denial_evidence(
        &mut output,
        &[cairn_common::executor_protocol::SandboxDenialEvidence {
            denial: cairn_common::executor_protocol::SandboxDenial::Path(
                "/run/user/1000/libpod/tmp/alive.lck".into(),
            ),
            operation: None,
            command: "go test ./...".into(),
            stream_id: "merge-gate:1".into(),
        }],
    );
    assert_eq!(
        classify_check_failure("go test ./...", Some(1), false, false, None, &output),
        None
    );
}

/// cairn/4650 specimen 4: an OOM killer's SIGTERM reached `rust-tests` as
/// exit 143 and rendered as a red about the change. A termination signal
/// is a killed run on every lane; a crash signal is still the process's own
/// failure.
#[test]
fn a_termination_signal_exit_is_a_killed_run_and_a_crash_is_not() {
    for (exit, signal) in [(143, "SIGTERM"), (137, "SIGKILL")] {
        let classified = classify_check_failure(
            "bun run test:rust:tiered",
            Some(exit),
            false,
            false,
            None,
            "   Compiling cairn-core v0.13.1\n",
        )
        .expect("a signal death is classified");
        assert_eq!(classified.kind, CheckFailureKind::Killed);
        assert!(classified.reason.contains(signal), "{}", classified.reason);
    }
    assert_eq!(
        classify_check_failure(
            "bun run test:rust:tiered",
            Some(139),
            false,
            false,
            None,
            ""
        ),
        None,
        "a segfault is the process's own outcome"
    );
}

/// cairn/5735: a compile the host ended (a terminating signal under rustc,
/// or a storage error) exits 101 like a compile error, and is stored as
/// `infrastructure` naming the ending, so it is charged to the machine as a
/// death and never reused. A compile error about the code stays the
/// tree's, and a crash signal stays the compiler's own outcome.
#[test]
fn a_compile_the_host_ended_is_stored_as_infrastructure_naming_the_ending() {
    use super::host_ended_compile_specimens::*;
    let command = "bun run test:rust:full";
    // What the Rust test script reports for a build that never ran a test.
    let result = r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"full","undeclaredSkips":0,"tests":[]}"#;
    for (tail, ending, evidence) in [
        (
            KILLED_RMCP_TAIL,
            "a compiler process was killed by SIGTERM while compiling `rmcp`",
            "(signal: 15, SIGTERM",
        ),
        (
            KILLED_CORE_TEST_TAIL,
            "a compiler process was killed by SIGKILL while compiling `cairn-core`",
            "(signal: 9, SIGKILL",
        ),
        (
            DISK_FULL_TAIL,
            "the host's storage failed (No space left on device) while compiling `cairn-core`",
            "No space left on device",
        ),
    ] {
        let output = format!("{tail}\n{RESULT_ARTIFACT_JSON_MARKER}{result}");
        let parsed = parse_check_output(command, &output);
        for parsed in [None, parsed.as_ref()] {
            let classification =
                classify_check_failure(command, Some(101), false, false, parsed, &output)
                    .expect("a host-ended compile is classified");
            assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
            assert!(
                classification
                    .reason
                    .starts_with(&format!("Cairn: {ending};")),
                "{}",
                classification.reason
            );
            let line = output
                .lines()
                .nth(classification.evidence_line.expect("evidence is cited"))
                .unwrap();
            assert!(line.contains(evidence), "{line}");
            assert!(!check_reuse_decision(command, Some(classification.kind), parsed).reusable);
            assert_eq!(
                infra_repair_domain(None, Some(classification.kind)),
                InfraRepairDomain::Environment,
                "the death is charged to the machine, not the head"
            );
        }
    }

    let classify = |output: &str| {
        classify_check_failure(command, Some(101), false, false, None, output)
            .map(|classification| classification.kind)
    };
    assert_eq!(
        classify(COMPILE_ERROR_TAIL),
        None,
        "a compile error is the tree's red"
    );
    assert_eq!(
        classify(&format!("{COMPILE_ERROR_TAIL}\n{KILLED_RMCP_TAIL}")),
        None,
        "a compile error beside a killed compile is still the tree's"
    );
    assert_eq!(
        classify(&KILLED_RMCP_TAIL.replace(
            "signal: 15, SIGTERM: termination signal",
            "signal: 11, SIGSEGV: invalid memory reference"
        )),
        None,
        "a crash signal is the compiler's own outcome"
    );
    assert_eq!(
        classify(
            "error: test failed, to rerun pass `-p cairn-core --lib`\n\nCaused by:\n  process didn't exit successfully: `/target/debug/deps/cairn_core-0a1b` (signal: 9, SIGKILL: kill)"
        ),
        None,
        "a killed test binary is not a killed compile"
    );
}

/// The store and the main cadence answer "did the host end this compile"
/// with one predicate: the classifier stores `infrastructure` exactly when
/// [`host_ended_compile`] holds over the output, and the excerpt it stores
/// still yields the same ending to the cadence, which reads the stored
/// tail with that predicate.
#[test]
fn the_store_and_the_cadence_share_one_host_ended_compile_predicate() {
    use super::host_ended_compile_specimens::*;
    let command = "bun run test:rust:full";
    let io = DISK_FULL_TAIL.replace("No space left on device", "Input/output error (os error 5)");
    let windows = DISK_FULL_TAIL.replace(
        "No space left on device",
        "There is not enough space on the disk. (os error 112)",
    );
    let build_script = "error: failed to run custom build command for `ring v0.17.8`\n\nCaused by:\n  process didn't exit successfully: `/target/debug/build/ring-1/build-script-build` (signal: 15, SIGTERM: termination signal)";
    let segv = KILLED_RMCP_TAIL.replace(
        "signal: 15, SIGTERM: termination signal",
        "signal: 11, SIGSEGV: invalid memory reference",
    );
    let beside_error = format!("{COMPILE_ERROR_TAIL}\n{DISK_FULL_TAIL}");
    for output in [
        KILLED_RMCP_TAIL,
        KILLED_CORE_TEST_TAIL,
        DISK_FULL_TAIL,
        &io,
        &windows,
        build_script,
        MIRROR_WRITE_DENIED_TAIL,
        COMPILE_ERROR_TAIL,
        &segv,
        &beside_error,
        "",
    ] {
        let ended = host_ended_compile(output);
        let classification = classify_check_failure(command, Some(101), false, false, None, output);
        assert_eq!(
            classification.as_ref().map(|c| c.kind),
            ended.as_ref().map(|_| CheckFailureKind::Infrastructure),
            "{output}"
        );
        let Some(ended) = ended else { continue };
        let stored = classified_output_excerpt(output, classification.as_ref(), None);
        let reread = host_ended_compile(&stored).expect("the stored row still reads as host-ended");
        assert_eq!(
            (reread.by, reread.krate),
            (ended.by, ended.krate),
            "{stored}"
        );
    }
}

/// cairn/6279: the external-host mirror builds under the executor's temp
/// root, outside any build slot, and the macOS sandbox refused rustc's
/// dep-info writes there. The retained output replays as a host-ended
/// compile: infrastructure citing the first refused write, charged to the
/// machine, never reused, never a red, rendered as no tally, and signed the
/// same however cargo ordered the crates, so the lane breaker holds a
/// standing refusal instead of rerunning it as if time would repair it.
#[test]
fn a_compiler_output_write_the_os_refused_outside_a_build_slot_is_not_a_verdict() {
    use super::host_ended_compile_specimens::*;
    let command = "bun run check:external-host";
    let classify =
        |output: &str| classify_check_failure(command, Some(101), false, false, None, output);
    let reason_head = "Cairn: the operating system refused the compiler's write of its build \
                           output (Operation not permitted)";

    let mut classification = classify(MIRROR_WRITE_DENIED_TAIL);
    attach_executor_provenance(&mut classification, Some("local"));
    let classification = classification.expect("a refused output write is classified");
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert!(
        !classification.cache_corruption,
        "no uncached retry: the cache is not implicated"
    );
    assert_eq!(classification.implied_requirement, None);
    let reason = &classification.reason;
    assert!(reason.starts_with(reason_head), "{reason}");
    assert!(reason.contains("recurs on this machine"), "{reason}");
    assert!(reason.contains("not one that clears with time"), "{reason}");
    assert!(reason.contains("not a verdict on the tree"), "{reason}");
    assert!(reason.ends_with(" on executor `local`"), "{reason}");
    assert_eq!(
        MIRROR_WRITE_DENIED_TAIL
            .lines()
            .nth(classification.evidence_line.expect("evidence is cited")),
        Some(MIRROR_WRITE_DENIED_EVIDENCE),
        "the evidence is the first refused write"
    );
    assert!(classification.kind.is_infrastructure());
    assert!(
        !classification.kind.is_verdict_about_the_code(),
        "never a red charged to the branch"
    );
    assert!(!check_reuse_decision(command, Some(classification.kind), None).reusable);
    assert!(
        check_reuse_decision(command, None, None).reusable,
        "unclassified, this red was served as a reusable verdict"
    );
    assert_eq!(
        infra_repair_domain(None, Some(classification.kind)),
        InfraRepairDomain::Environment,
        "charged to the machine, not the head"
    );

    // Rendered as the death it is: still failed, no test tally, and the
    // stored excerpt keeps the refused write.
    let mut rendered = outcome("external-host", false, Some(101));
    rendered.failure_kind = Some(classification.kind);
    rendered.output_tail =
        classified_output_excerpt(MIRROR_WRITE_DENIED_TAIL, Some(&classification), None);
    assert!(!rendered.passed);
    let annotation = summary_annotation(&rendered).expect("a classified death is annotated");
    assert!(
        annotation.starts_with("infrastructure/toolchain failure"),
        "{annotation}"
    );
    assert!(
        !annotation.contains("passed") && !annotation.contains("tests"),
        "{annotation}"
    );
    assert!(
        rendered.output_tail.contains(MIRROR_WRITE_DENIED_EVIDENCE),
        "{}",
        rendered.output_tail
    );

    // One standing refusal signs one way, whichever crate cargo reached
    // first and whichever checkout's temp key the mirror lives under.
    let signature = |output: &str| {
        let classification = classify(output).expect("classified");
        let (cause, evidence) = failure_cause_and_evidence(None, Some(&classification), output);
        crate::execution::lane_breaker::failure_signature(
            classification.kind.as_str(),
            &cause,
            101,
            &evidence,
        )
    };
    let reordered = MIRROR_WRITE_DENIED_TAIL
        .replacen(MIRROR_WRITE_DENIED_EVIDENCE, "", 1)
        .replace("could not compile `cfg-if`", "could not compile `memchr`")
        .replace("cbf981ee2f28", "0a1b2c3d4e5f");
    assert_eq!(signature(MIRROR_WRITE_DENIED_TAIL), signature(&reordered));

    // Positive controls: the same refusal inside a build slot, and rustc's
    // other output writes.
    let slot = MIRROR_WRITE_DENIED_TAIL.replace(
        "/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target",
        "/Users/dev/.cairn-exec/build-slots/cairn/slot-5/src-tauri/target",
    );
    for output in [
        slot.as_str(),
        "error: could not write output to /tmp/m/target/debug/deps/a-1.a.o: Permission denied (os error 13)\nerror: could not compile `a` (lib) due to 1 previous error",
        "error: failed to write /tmp/m/target/debug/deps/liba-1.rmeta: Operation not permitted (os error 1)\nerror: could not compile `a` (lib) due to 1 previous error",
    ] {
        let classification = classify(output).unwrap_or_else(|| panic!("{output}"));
        assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
        assert!(
            classification
                .reason
                .starts_with("Cairn: the operating system refused the compiler's write"),
            "{}",
            classification.reason
        );
    }

    // Not evidence: a bare EPERM line on a target path outside a build
    // slot, a refused write outside any compile, test names and result
    // payloads that quote the diagnostic, an expected permission-denial
    // test's output beside its failing row, and an exit that says nothing.
    let quoted = "error: error writing dependencies to `/t/target/debug/deps/a.d`: Operation not permitted (os error 1)";
    let without_denials = MIRROR_WRITE_DENIED_TAIL
        .lines()
        .filter(|line| !line.contains("error writing dependencies"))
        .collect::<Vec<_>>()
        .join("\n");
    let sessions_assertion = "running 1 test\n\
            test sessions::resume::tests::resumes_after_compaction ... FAILED\n\n\
            failures:\n\n\
            ---- sessions::resume::tests::resumes_after_compaction stdout ----\n\n\
            thread 'sessions::resume::tests::resumes_after_compaction' panicked at os/cairn-core/src/sessions/resume.rs:88:9:\n\
            assertion `left == right` failed\n\
            \x20 left: 2\n\
            \x20right: 3\n\n\
            test result: FAILED. 0 passed; 1 failed; 0 ignored\n\n\
            error: test failed, to rerun pass `-p cairn-core --lib`";
    for (output, why) in [
        (
            "error: could not compile `cfg-if` (lib)\nOperation not permitted (os error 1) at /Volumes/Builds/cairn-executor/tmp/x/cairn-external-host-y/target/debug/deps/.tmp1".to_string(),
            "a bare EPERM line is not rustc's output-write diagnostic",
        ),
        (
            "error: failed to write /tmp/cairn-external-host-x/target/report.json: Operation not permitted (os error 1)\nerror: script \"check:x\" exited with code 101".to_string(),
            "a refused write outside a compile names no compile the host ended",
        ),
        (
            format!(" ✓ scripts/lib/classifier.test.ts > classifier > {quoted} 0ms\nerror: could not compile `a` (lib)\nerror: script \"test\" exited with code 101"),
            "a test name quoting the diagnostic is data",
        ),
        (
            format!("{RESULT_ARTIFACT_JSON_MARKER}{{\"schemaVersion\":1,\"parser\":\"nextest\",\"tests\":[{{\"name\":\"{quoted}\"}}]}}\nerror: could not compile `a` (lib)"),
            "a structured result quoting the diagnostic is data",
        ),
        (
            format!("test sandbox::tests::refuses_a_read_only_target ... FAILED\n{quoted}\nerror: could not compile `a` (lib)\nerror: test failed, to rerun pass `-p cairn-exec --lib`"),
            "an expected denial beside a failing test is that test's red",
        ),
        (
            COMPILE_ERROR_TAIL.to_string(),
            "a compile error is the tree's red",
        ),
        (
            format!("{COMPILE_ERROR_TAIL}\n{MIRROR_WRITE_DENIED_TAIL}"),
            "a compile error beside a refused write is still the tree's",
        ),
        (
            sessions_assertion.to_string(),
            "a sessions assertion is the tree's red",
        ),
        (
            format!("{MIRROR_WRITE_DENIED_TAIL}\n{sessions_assertion}"),
            "a named failing test beside a refused write is still the tree's",
        ),
        (
            "check:external-host: build the mirror's libraries and binaries failed\nerror: script \"check:external-host\" exited with code 101".to_string(),
            "an exit 101 that names no cause stays ordinary",
        ),
        (
            without_denials,
            "cargo's could-not-compile summary alone is not host evidence",
        ),
    ] {
        assert_eq!(classify(&output), None, "{why}: {output}");
    }

    // A build that recovered from a cache failure passed; no warning it
    // printed on the way can turn the pass into anything else.
    let recovered = "warning: sccache failed fatally; compiling directly without cache\n\
                         sccache: encountered fatal error: failed to create output file\n\
                         \x20   Finished `dev` profile [unoptimized + debuginfo] target(s) in 41.20s";
    for output in [recovered, MIRROR_WRITE_DENIED_TAIL] {
        assert_eq!(
            classify_check_failure(command, Some(0), false, false, None, output),
            None
        );
    }
}

/// cairn/5873: cargo fetching a private git dependency on a machine with no
/// credential exits 101 and words it as a missing revision. It is stored as
/// `infrastructure` naming the machine and the missing credential, never
/// reused, charged to the machine, and kept off the wake path.
#[test]
fn a_dependency_fetch_without_a_credential_is_stored_as_infrastructure_naming_the_machine() {
    use super::git_credential_fetch_specimens::*;
    let command = "cargo test --workspace";
    let mut specimens: Vec<(String, &str)> = vec![
        (
            TM_230_TAIL.to_string(),
            "fatal: could not read Username for 'https://github.com'",
        ),
        (
            LIBGIT2_TAIL.to_string(),
            "failed to authenticate when downloading repository",
        ),
    ];
    for auth in AUTH_LINES {
        specimens.push((
            wrapped(auth),
            auth.lines()
                .find(|l| l.contains("fatal:") || l.contains("publickey"))
                .unwrap(),
        ));
    }
    for (output, evidence) in &specimens {
        let mut classification =
            classify_check_failure(command, Some(101), false, false, None, output);
        attach_executor_provenance(&mut classification, Some("bginf-sp1"));
        let classification = classification.unwrap_or_else(|| panic!("{output}"));
        assert_eq!(
            classification.kind,
            CheckFailureKind::Infrastructure,
            "{output}"
        );
        let reason = &classification.reason;
        assert!(reason.contains("credential"), "{reason}");
        assert!(
            reason.contains("`https://github.com/bleugreen/tm-cards`"),
            "the reason names the repository: {reason}"
        );
        assert!(reason.contains("no usable GitHub credential"), "{reason}");
        assert!(reason.contains("not a verdict on the tree"), "{reason}");
        assert!(reason.ends_with(" on executor `bginf-sp1`"), "{reason}");
        let line = output
            .lines()
            .nth(classification.evidence_line.expect("evidence is cited"))
            .unwrap();
        assert!(line.contains(evidence), "{line}");
        assert!(!check_reuse_decision(command, Some(classification.kind), None).reusable);
        assert!(
            !classification.kind.is_verdict_about_the_code(),
            "an infrastructure row never wakes the builder as a red"
        );
        assert_eq!(
            classification.implied_requirement,
            Some(ImpliedRequirement {
                capability: ProjectCapability::GithubCredential,
                repository: Some("https://github.com/bleugreen/tm-cards".to_string()),
            }),
            "a github.com refusal teaches placement the project needs a credential"
        );
        assert_eq!(
            infra_repair_domain(None, Some(classification.kind)),
            InfraRepairDomain::Environment,
            "the failure is charged to the machine, not the head"
        );
    }
    let tm = classify_check_failure(command, Some(101), false, false, None, TM_230_TAIL).unwrap();
    assert_eq!(
        TM_230_TAIL.lines().nth(tm.evidence_line.unwrap()),
        Some("fatal: could not read Username for 'https://github.com': No such device or address"),
        "the evidence is the first of git's lines"
    );
    assert!(
        tm.reason
            .contains("cargo reports this as 'revision … not found'"),
        "{}",
        tm.reason
    );

    let gitlab = classify_check_failure(command, Some(101), false, false, None, GITLAB_TAIL)
        .expect("another host's refusal is still the machine's");
    assert_eq!(gitlab.kind, CheckFailureKind::Infrastructure);
    assert!(
        gitlab
            .reason
            .contains("no usable `gitlab.example.com` credential"),
        "{}",
        gitlab.reason
    );
    assert_eq!(
        gitlab.implied_requirement, None,
        "no capability measures another host's credential"
    );
}

/// Output that merely mentions git's words, a test that failed for its own
/// reasons, a compile error, and a genuinely missing revision all keep the
/// classification they had.
#[test]
fn credential_lookalikes_keep_their_classification() {
    use super::git_credential_fetch_specimens::*;
    use super::host_ended_compile_specimens::COMPILE_ERROR_TAIL;
    let command = "cargo test --workspace";
    let classify = |output: &str| {
        classify_check_failure(command, Some(101), false, false, None, output)
            .map(|classification| classification.kind)
    };
    assert_eq!(
        classify(
            "test git::fatal_could_not_read_username_for_https_github_com ... ok\n\
                 test git::reads_auth ... FAILED\n\
                 fatal: could not read Username for 'https://github.com': terminal prompts disabled"
        ),
        None,
        "a named failing test that prints git's line is the test's red"
    );
    assert_eq!(
        classify(
            "test fatal: could not read Username for 'https://github.com' ... ok\nerror: test failed"
        ),
        None,
        "a passing row that names git's line is blanked before the scan"
    );
    // Compiler diagnostics echo source lines, and this suite's own
    // fixtures are such lines: a build broken beside them is the tree's.
    let rustc = format!(
        "error[E0308]: mismatched types\n    --> os/cairn-core/src/execution/checks.rs:6287:9\n     |\n6287 | {}\n     |         ^^^^^ expected `&str`, found `()`\nerror: could not compile `cairn-core` (lib test) due to 1 previous error",
        TM_230_TAIL.lines().nth(1).unwrap()
    );
    assert_eq!(
        classify(&rustc),
        None,
        "a rustc diagnostic quoting a fixture"
    );
    let clippy = "error: this could be simplified\n   --> src/git_credential.rs:12:5\n   |\n12 |     \"git@github.com: Permission denied (publickey).\\nfatal: Could not read from remote repository.\",\n13 |     \"  failed to authenticate when downloading repository\",\nerror: could not compile `cairn-common` (lib) due to 1 previous error";
    assert_eq!(
        classify_check_failure("bun run check:rust", Some(101), false, false, None, clippy)
            .map(|classification| classification.kind),
        None,
        "a clippy diagnostic quoting a fixture"
    );
    assert_eq!(
        classify(COMPILE_ERROR_TAIL),
        None,
        "a compile error is the tree's red"
    );
    assert_eq!(
        classify(&wrapped("")),
        None,
        "a revision missing with no auth line is a bad pin, the tree's red"
    );
}

/// The lane breaker groups infrastructure reds by their failure signature,
/// so the same red on two machines must sign the same though its stored
/// reason names each machine.
#[test]
fn one_infrastructure_red_on_two_machines_signs_the_same() {
    use super::git_credential_fetch_specimens::TM_230_TAIL;
    use super::host_ended_compile_specimens::KILLED_RMCP_TAIL;
    for output in [TM_230_TAIL, KILLED_RMCP_TAIL] {
        let signature = |executor: &str| {
            let mut classification =
                classify_check_failure("cargo test", Some(101), false, false, None, output);
            attach_executor_provenance(&mut classification, Some(executor));
            let classification = classification.unwrap();
            assert!(classification.reason.ends_with(&format!("`{executor}`")));
            let (cause, evidence) = failure_cause_and_evidence(None, Some(&classification), output);
            crate::execution::lane_breaker::failure_signature(
                classification.kind.as_str(),
                &cause,
                101,
                &evidence,
            )
        };
        assert_eq!(signature("bginf-sp1"), signature("bginf-sp2"), "{output}");
    }
    assert_eq!(
        without_executor_provenance("Cairn: x on executor `bginf-sp1`"),
        "Cairn: x"
    );
    assert_eq!(
        without_executor_provenance("Cairn: ran on executor nodes"),
        "Cairn: ran on executor nodes"
    );
}

/// The classifier stores `infrastructure` for a credential failure exactly
/// when the shared matcher the executor's GitHub probe reads `git ls-remote`
/// with holds over the output, so the two cannot disagree about what a
/// credential failure looks like.
#[test]
fn the_classifier_and_the_github_probe_share_one_credential_matcher() {
    use super::git_credential_fetch_specimens::*;
    let command = "cargo build";
    let mut outputs = vec![
        TM_230_TAIL.to_string(),
        LIBGIT2_TAIL.to_string(),
        GITLAB_TAIL.to_string(),
        wrapped(""),
        "dev@bginf-sp1: Permission denied (publickey).".to_string(),
        String::new(),
    ];
    outputs.extend(AUTH_LINES.iter().map(|auth| wrapped(auth)));
    for output in &outputs {
        let matched = cairn_common::git_credential::credential_failure(output);
        let classification = classify_check_failure(command, Some(101), false, false, None, output);
        assert_eq!(
            classification.as_ref().map(|c| c.kind),
            matched.as_ref().map(|_| CheckFailureKind::Infrastructure),
            "{output}"
        );
        if let (Some(matched), Some(classification)) = (matched, classification) {
            assert_eq!(classification.evidence_line, Some(matched.line_index));
        }
    }
}

/// The manual reply classifies a failed run the way the wake body and the
/// merge gate do: a killed run and an empty selection are not reds, a
/// substrate failure keeps its machinery wording, and an ordinary red stays
/// a red.
#[test]
fn a_manual_run_without_a_verdict_never_reports_a_red() {
    let kind = |failure_kind: Option<CheckFailureKind>| {
        manual_check_no_verdict(&CheckOutcome {
            failure_kind,
            output_tail: "cause".to_string(),
            ..outcome("rust-tests", false, Some(1))
        })
        .map(|no_verdict| no_verdict.kind)
    };
    assert_eq!(
        kind(Some(CheckFailureKind::Killed)).as_deref(),
        Some("killed")
    );
    assert_eq!(
        kind(Some(CheckFailureKind::Selection)).as_deref(),
        Some("empty")
    );
    assert_eq!(
        kind(Some(CheckFailureKind::Capacity)).as_deref(),
        Some("infrastructure")
    );
    assert_eq!(kind(None), None, "an ordinary red is a verdict");
}

#[test]
fn tracked_modification_evidence_is_legible_without_overwriting_command_output() {
    let mut output = "cargo check failed with code 101".to_string();
    append_tracked_modification_evidence(
        &mut output,
        Some(
            &cairn_common::executor_protocol::TrackedModificationEvidence {
                paths: vec!["Cargo.lock".into(), "src/generated.rs".into()],
                files_changed: 2,
                lines_added: 4,
                lines_deleted: 1,
                index_restore_failure: None,
            },
        ),
    );

    assert!(output.starts_with("cargo check failed with code 101"));
    assert!(output.contains("check modified tracked paths: Cargo.lock, src/generated.rs"));
    assert!(output.contains("2 files, +4 -1"));
    assert!(output.ends_with("changes were discarded"));
}

#[test]
fn an_index_restore_failure_is_named_beside_the_discard() {
    let mut output = String::new();
    append_tracked_modification_evidence(
        &mut output,
        Some(
            &cairn_common::executor_protocol::TrackedModificationEvidence {
                paths: vec!["src/lib.rs".into()],
                files_changed: 1,
                lines_added: 1,
                lines_deleted: 0,
                index_restore_failure: Some(
                    "index restore failed: index.lock exists; 1 path(s) may still be staged".into(),
                ),
            },
        ),
    );

    assert!(output.contains("changes were discarded\nnote: index restore failed"));
    assert!(output.ends_with("1 path(s) may still be staged"));
}

#[test]
fn timeout_budget_formats_minutes_and_seconds() {
    assert_eq!(format_timeout_budget(600_000), "10m");
    assert_eq!(format_timeout_budget(1_800_000), "30m");
    assert_eq!(format_timeout_budget(45_000), "45s");
    assert_eq!(format_timeout_budget(0), "0s");
}

#[test]
fn resolve_timeout_prefers_schema_then_default_then_cap() {
    let default_ms = DEFAULT_REVIEW_CHECK_TIMEOUT_MS;
    // No check / no schema timeout ⇒ the cadence default.
    assert_eq!(resolve_check_timeout_ms(None, default_ms), default_ms);
    let mut c = check("run", None, CheckWhen::Review);
    assert_eq!(resolve_check_timeout_ms(Some(&c), default_ms), default_ms);
    // A schema timeout (SECONDS) wins, converted to ms.
    c.timeout = Some(120);
    assert_eq!(resolve_check_timeout_ms(Some(&c), default_ms), 120_000);
    // An absurd value is clamped to the hard 60-minute ceiling.
    c.timeout = Some(10_000);
    assert_eq!(
        resolve_check_timeout_ms(Some(&c), default_ms),
        MAX_CHECK_TIMEOUT_MS
    );
}

#[test]
fn defaults_give_the_heavy_review_cadence_more_headroom() {
    // The whole point: review's default must sit well above the 10-min wall
    // the write cadence keeps, or a healthy-but-slow suite is guillotined
    // again (dozens of `rust-full` rows were killed at ~600s). Bind to locals
    // so the guards aren't flagged as constant-value assertions.
    let (write, review) = (
        DEFAULT_WRITE_CHECK_TIMEOUT_MS,
        DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
    );
    assert_eq!(write, 600_000);
    assert!(
        review >= 1_800_000,
        "review default must cover a cold, uncached full Rust build"
    );
    assert!(
        review > write,
        "review default must exceed the tighter write default"
    );
}

#[tokio::test]
async fn merge_gate_classifies_all_substrate_failures_as_infrastructure() {
    use cairn_common::executor_protocol::{CellUnavailableReason::*, ObjectInfrastructureStage};
    let unavailable = vec![
        Deadline {
            host_pressure: None,
            substrate: None,
        },
        Provisioning,
        Checkout,
        Spawn,
        Preparation,
        ExecutorUnavailable,
        NoMatchingExecutor {
            rejected: Vec::new(),
        },
        ObjectInfrastructure(ObjectInfrastructureStage::FetchInterrupted),
        ObjectInfrastructure(ObjectInfrastructureStage::IntegrityFailure),
        ObjectInfrastructure(ObjectInfrastructureStage::IncompleteClosure),
        ObjectInfrastructure(ObjectInfrastructureStage::InstallFailure),
        ObjectInfrastructure(ObjectInfrastructureStage::UploadFailure),
        ObjectInfrastructure(ObjectInfrastructureStage::ExpiredReceipt),
        ObjectInfrastructure(ObjectInfrastructureStage::StaleReceipt),
        ObjectInfrastructure(ObjectInfrastructureStage::MaterializationRefused),
    ];
    let mut failures = unavailable
        .into_iter()
        .map(|reason| CellOutcome::Unavailable {
            reason,
            diagnostic: "unavailable".to_string(),
        })
        .collect::<Vec<_>>();
    failures.push(CellOutcome::FailedAfterExecution {
        request_id: "request".to_string(),
        attempt_id: "attempt".to_string(),
        diagnostic: "publication failed".to_string(),
    });
    // A cancellation is not here: it is not a substrate failure, and the
    // gate reports it as one (see
    // `merge_gate_reports_a_cancelled_lane_as_a_cancellation`).
    failures.push(CellOutcome::StorageFailure {
        request_id: "request".to_string(),
        attempt_id: "attempt".to_string(),
        stage: cairn_common::executor_protocol::StorageFailureStage::Recovery,
        kind: cairn_common::executor_protocol::StorageFailureKind::CleanupFailed,
        diagnostic: "cleanup failed".to_string(),
        slot_retired: false,
    });
    failures.push(CellOutcome::Completed {
        request_id: "request".to_string(),
        attempt_id: "attempt".to_string(),
        exit_code: Some(0),
        output: String::new(),
        timed_out: false,
        metadata: cairn_common::executor_protocol::CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: "executor".to_string(),
            executor_device_id: "device".to_string(),
            executor_connection_generation: 1,
            cell_id: "slot".to_string(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        },
        mutation_delta: Some(Box::new(cairn_common::executor_protocol::MutationDelta {
            base_commit: "base".to_string(),
            delta_commit: "delta".to_string(),
            upload_receipt: None,
        })),
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    });

    for (index, outcome) in failures.into_iter().enumerate() {
        let failure =
            check_result_from_cell_outcome(outcome, &crate::fleet::CellAccount::default(), None)
                .expect_err("substrate outcome must not become a command verdict");
        let db = cache_db().await;
        let results = run_planned_checks(
            db,
            "project-a",
            &format!("tree-{index}"),
            "job-a",
            &[(plan("rust", "cargo test"), format!("input-{index}"))],
            "tool",
            CheckExecMode::Shared,
            None,
            move |_, _, _| {
                let failure = failure.clone();
                async move { Err::<CheckExecResult, _>(failure) }
            },
            |_| {},
        )
        .await;
        assert!(matches!(
            review_tree_gate_result(results),
            ReviewTreeGateResult::InfrastructureFailure(_)
        ));
    }
}

/// A check its executor stopped for stalling is a named non-verdict on both
/// paths a result arrives by, whatever exit code the killed process left,
/// and its diagnostic is the request's own evidence: how long it was quiet
/// and when it last wrote, not what the machine is doing now.
#[tokio::test]
async fn a_stalled_command_is_a_named_non_verdict_on_both_paths() {
    let stop = cairn_common::executor_protocol::CommandStop::Silent {
        quiet_ms: 900_000,
        last_output_unix_ms: Some(1_792_000_000_000),
        cpu_ms_in_window: Some(12),
        storage_io_bytes_in_window: Some(0),
        tree: Vec::new(),
    };
    let metadata: cairn_common::executor_protocol::CellExecutionMeta =
        serde_json::from_value(serde_json::json!({
            "slotId": "slot", "cellEpoch": 1,
            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
        }))
        .unwrap();
    let stalled = |failure: &CheckExecutionFailure| match failure {
        CheckExecutionFailure::Substrate(substrate) => {
            assert_eq!(substrate.shape, SubstrateFailureShape::CommandStalled);
            assert!(
                substrate.diagnostic.contains("for 900s"),
                "{}",
                substrate.diagnostic
            );
            assert!(
                substrate.diagnostic.contains("last output at"),
                "{}",
                substrate.diagnostic
            );
            assert!(
                substrate.diagnostic.contains("the tail"),
                "{}",
                substrate.diagnostic
            );
        }
        other => panic!("a stalled command became {other:?}"),
    };

    // One check of a coalesced batch, narrowed to its own outcome.
    let mut item_metadata = metadata.clone();
    item_metadata.stop = Some(stop.clone());
    let failure = check_result_from_cell_outcome(
        CellOutcome::Completed {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            exit_code: Some(137),
            output: "the tail".into(),
            timed_out: false,
            metadata: item_metadata,
            mutation_delta: None,
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
            node_scratch: None,
        },
        &crate::fleet::CellAccount::default(),
        None,
    )
    .expect_err("a stopped command is not a verdict");
    stalled(&failure);

    // The write cadence's typed batch outcome.
    let item = serde_json::json!({
        "header": "rust-tests", "body": "the tail", "succeeded": false,
        "suspended": false, "exitCode": 137, "startedAtUnixMs": 1,
        "finishedAtUnixMs": 2, "durationMs": 1, "stop": serde_json::to_value(&stop).unwrap(),
    });
    let (results, _) = split_write_check_batch_outcome(
        CellOutcome::Completed {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            exit_code: Some(1),
            output: serde_json::to_string(&vec![item]).unwrap(),
            timed_out: false,
            metadata,
            mutation_delta: None,
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
            node_scratch: None,
        },
        &crate::fleet::CellAccount::default(),
        1,
    );
    stalled(
        results[0]
            .as_ref()
            .expect_err("a stopped command is not a verdict"),
    );

    // Never a red: the merge gate reads it as infrastructure, not as a
    // result about the tree.
    let db = cache_db().await;
    let results = run_planned_checks(
        db,
        "project-a",
        "tree-stalled",
        "job-a",
        &[(plan("rust", "cargo test"), "input-stalled".to_string())],
        "tool",
        CheckExecMode::Shared,
        None,
        move |_, _, _| {
            let failure = failure.clone();
            async move { Err::<CheckExecResult, _>(failure) }
        },
        |_| {},
    )
    .await;
    assert!(matches!(
        review_tree_gate_result(results),
        ReviewTreeGateResult::InfrastructureFailure(_)
    ));
}

/// A stall is charged to the platform it happened on, end to end: three
/// macOS silent stops, decoded from real cell outcomes and recorded by the
/// evaluator, leave a `[linux, macos]` request free to run on Linux, and
/// the stalled result still names its machine.
#[tokio::test]
async fn three_macos_stalls_leave_a_linux_request_runnable() {
    use crate::execution::cache::{claim_check_execution, get_suppressed_check_result};
    let db = cache_db().await;
    let stop = cairn_common::executor_protocol::CommandStop::Silent {
        quiet_ms: 1_800_000,
        last_output_unix_ms: None,
        cpu_ms_in_window: Some(3),
        storage_io_bytes_in_window: None,
        tree: Vec::new(),
    };
    let metadata: cairn_common::executor_protocol::CellExecutionMeta =
        serde_json::from_value(serde_json::json!({
            "executorId": "mac-1", "slotId": "slot", "cellEpoch": 1,
            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
            "verdictPlatform": "macos",
            "stop": serde_json::to_value(&stop).unwrap(),
        }))
        .unwrap();
    let failure = check_result_from_cell_outcome(
        CellOutcome::Completed {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            exit_code: Some(137),
            output: "resources::browsers::tests SLOW >60s".into(),
            timed_out: false,
            metadata,
            mutation_delta: None,
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
            node_scratch: None,
        },
        &crate::fleet::CellAccount::default(),
        None,
    )
    .expect_err("a stopped command is not a verdict");
    let CheckExecutionFailure::Substrate(substrate) = &failure else {
        panic!("a stall is a substrate failure: {failure:?}");
    };
    assert_eq!(
        substrate
            .provenance()
            .and_then(|meta| meta.verdict_platform.as_deref()),
        Some("macos"),
        "the stall keeps the platform it ran on"
    );

    let lane = plan("rust-tests", "bun run test:rust");
    let plans = vec![(lane.clone(), "ih-rust".to_string())];
    let commit = CheckRunCommit {
        evaluated: "commit-a",
        defined_by: "commit-a",
    };
    let platforms = vec!["linux".to_string(), "macos".to_string()];
    let scopes = InfraScopes {
        platform_heads: InfraScopes::platform_heads_for("job-a", "commit-a", &platforms),
        ..InfraScopes::head_only("job-a", "commit-a")
    };
    for _ in 0..crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
        // Submission reserves the attempt; the evaluator records its result.
        assert!(matches!(
            claim_check_execution(db.clone(), "project-a", &lane.name, "ih-rust", &scopes).unwrap(),
            crate::execution::cache::CheckExecutionClaim::Clear { .. }
        ));
        let coordinates = sealed_coordinates(commit, "tree-a", &plans);
        let failure = failure.clone();
        let outcome = run_planned_checks_with_board(
            db.clone(),
            "project-a",
            commit,
            "tree-a",
            "job-a",
            &plans,
            &coordinates,
            std::slice::from_ref(&scopes),
            "turn-checks:job-a",
            CheckExecMode::Shared,
            None,
            None,
            &[],
            false,
            move |_index, _command, _stream_id| {
                let failure = failure.clone();
                async move { Err::<CheckExecResult, _>(failure) }
            },
            |_| {},
        )
        .await
        .remove(0);
        assert_eq!(
            outcome
                .recorded
                .as_ref()
                .and_then(|recorded| recorded.executor_id.as_deref()),
            Some("mac-1"),
            "a stalled result names its machine"
        );
    }

    let claim =
        claim_check_execution(db.clone(), "project-a", &lane.name, "ih-rust", &scopes).unwrap();
    let crate::execution::cache::CheckExecutionClaim::Clear {
        avoid_platforms, ..
    } = claim
    else {
        panic!("macOS stalls must not suppress the Linux half of the request: {claim:?}");
    };
    assert_eq!(avoid_platforms, vec!["macos".to_string()]);
    assert!(
        get_suppressed_check_result(db, "project-a", &lane.name, "ih-rust", &scopes)
            .unwrap()
            .is_none()
    );
}

/// Every reason a cell can refuse to start, mapped to the condition class an
/// agent is told — one at a time, from an exhaustive match, so a new reason
/// upstream cannot reach an agent as a generic "could not start".
///
/// The classes that matter are the ones that call for different responses:
/// wait (capacity), look at the fleet (no machine, draining, unreachable),
/// look at the environment (preparation), and look at Cairn (dispatch,
/// storage). Collapsing these into one label is what forced every reader to
/// the operator log (CAIRN-3345).
#[test]
fn every_no_start_reason_keeps_its_own_condition_class() {
    use cairn_common::executor_protocol::{
        AdmissionRejectionReason, CellUnavailableReason, ExecutorSubstrateState,
        HostPressureCondition, ObjectInfrastructureStage,
    };
    let sample = CellUnavailableReason::Provisioning;
    match &sample {
        CellUnavailableReason::Deadline { .. }
        | CellUnavailableReason::Provisioning
        | CellUnavailableReason::Checkout
        | CellUnavailableReason::Spawn
        | CellUnavailableReason::RunnerContext
        | CellUnavailableReason::Preparation
        | CellUnavailableReason::PreparationCommandNotFound { .. }
        | CellUnavailableReason::SlotUnhealthy
        | CellUnavailableReason::ExecutorUnavailable
        | CellUnavailableReason::NoMatchingExecutor { .. }
        | CellUnavailableReason::AdmissionRejected { .. }
        | CellUnavailableReason::ObjectInfrastructure(_)
        | CellUnavailableReason::HomeCannotHold { .. }
        | CellUnavailableReason::RunnerStopping { .. }
        | CellUnavailableReason::ClaimedAttemptSilent { .. }
        | CellUnavailableReason::BuildVolumeAbsent { .. }
        | CellUnavailableReason::BuildVolumeLost { .. } => {}
    }
    let cases = [
        (
            deadline(Some(ExecutorSubstrateState::CapacityBusy), None),
            SubstrateFailureShape::Capacity,
        ),
        (
            deadline(Some(ExecutorSubstrateState::SlotAdoption), None),
            SubstrateFailureShape::Capacity,
        ),
        (
            deadline(Some(ExecutorSubstrateState::ConnectedStalled), None),
            SubstrateFailureShape::MachineUnreachable,
        ),
        (
            deadline(Some(ExecutorSubstrateState::Draining), None),
            SubstrateFailureShape::Draining,
        ),
        (
            deadline(None, None),
            SubstrateFailureShape::MachineUnreachable,
        ),
        (
            deadline(
                None,
                Some(vec![HostPressureCondition::MemoryAvailable {
                    available_bytes: 1,
                    floor_bytes: 2,
                    owed_bytes: 0,
                    owing: Vec::new(),
                    total_bytes: None,
                    width_floor: None,
                    alone_beside: None,
                }]),
            ),
            SubstrateFailureShape::Capacity,
        ),
        (
            deadline(
                None,
                Some(vec![HostPressureCondition::DiskFree {
                    inodes: None,
                    free_bytes: 1,
                    floor_bytes: 2,
                }]),
            ),
            SubstrateFailureShape::Storage,
        ),
        (
            deadline(None, Some(Vec::new())),
            SubstrateFailureShape::MachineUnreachable,
        ),
        (
            CellUnavailableReason::AdmissionRejected {
                reason: AdmissionRejectionReason::QueueFull,
            },
            SubstrateFailureShape::Capacity,
        ),
        (
            CellUnavailableReason::AdmissionRejected {
                reason: AdmissionRejectionReason::Draining,
            },
            SubstrateFailureShape::Draining,
        ),
        (
            CellUnavailableReason::AdmissionRejected {
                reason: AdmissionRejectionReason::StorageCleanupFailed,
            },
            SubstrateFailureShape::Storage,
        ),
        (
            CellUnavailableReason::AdmissionRejected {
                reason: AdmissionRejectionReason::RequestTooLarge,
            },
            SubstrateFailureShape::NoMachine,
        ),
        (
            CellUnavailableReason::ExecutorUnavailable,
            SubstrateFailureShape::MachineUnreachable,
        ),
        (
            CellUnavailableReason::NoMatchingExecutor {
                rejected: Vec::new(),
            },
            SubstrateFailureShape::NoMachine,
        ),
        (
            CellUnavailableReason::BuildVolumeAbsent {
                path: "/Volumes/Builds/cairn".into(),
            },
            SubstrateFailureShape::BuildVolumeAbsent,
        ),
        (
            CellUnavailableReason::BuildVolumeLost {
                path: "/Volumes/Builds/cairn".into(),
            },
            SubstrateFailureShape::BuildVolumeLost,
        ),
        (
            CellUnavailableReason::Provisioning,
            SubstrateFailureShape::Preparation,
        ),
        (
            CellUnavailableReason::Checkout,
            SubstrateFailureShape::Preparation,
        ),
        (
            CellUnavailableReason::Preparation,
            SubstrateFailureShape::Preparation,
        ),
        // The pair that must not collapse: an environment that could not be
        // prepared is re-presented unchanged, and one that was retired for
        // being unfit is tried again somewhere else.
        (
            CellUnavailableReason::SlotUnhealthy,
            SubstrateFailureShape::EnvironmentRetired,
        ),
        (
            CellUnavailableReason::Spawn,
            SubstrateFailureShape::Dispatch,
        ),
        (
            CellUnavailableReason::RunnerContext,
            SubstrateFailureShape::Dispatch,
        ),
        (
            CellUnavailableReason::ObjectInfrastructure(
                ObjectInfrastructureStage::FetchInterrupted,
            ),
            SubstrateFailureShape::Dispatch,
        ),
    ];
    for (reason, expected) in cases {
        assert_eq!(
            no_start(&reason),
            NoStart::Substrate(expected),
            "{reason:?}"
        );
    }
    // The runner's own shutdown is neither a lost machine nor a missing
    // one, which is what it used to be reported as, and not a substrate
    // failure at all: it is Cairn ending the check.
    assert_eq!(
        no_start(&CellUnavailableReason::RunnerStopping {
            stopped_at_unix_ms: 1,
        }),
        NoStart::Cancelled(crate::fleet::CancelCause::RunnerStopping)
    );
}

fn deadline(
    substrate: Option<cairn_common::executor_protocol::ExecutorSubstrateState>,
    pressure: Option<Vec<cairn_common::executor_protocol::HostPressureCondition>>,
) -> cairn_common::executor_protocol::CellUnavailableReason {
    cairn_common::executor_protocol::CellUnavailableReason::Deadline {
        host_pressure: pressure
            .map(|conditions| cairn_common::executor_protocol::HostPressureEvidence { conditions }),
        substrate: substrate.map(|state| {
            cairn_common::executor_protocol::ExecutorSubstrateEvidence::without_queue(state, 0, 0)
        }),
    }
}

/// An elapsed deadline is a moment, not a condition: what the wait was ON is
/// in the executor's evidence, and the fleet already decides from that same
/// evidence whether the wait was worth having. So retry eligibility here must
/// equal the fleet's own capacity verdict, specimen for specimen.
///
/// Without this, a machine that stopped answering or was draining would be
/// waited through two more horizons and then reported as "could not obtain
/// capacity" — the condition-class collapse this mapping exists to remove,
/// reintroduced at the one reason where evidence decides.
#[test]
fn a_deadlines_retry_eligibility_matches_the_fleets_own_capacity_verdict() {
    use crate::fleet::placement::{classify_unavailable, LinkRestoration};
    use cairn_common::executor_protocol::{ExecutorSubstrateState, HostPressureCondition};
    let specimens = [
        deadline(Some(ExecutorSubstrateState::CapacityBusy), None),
        deadline(Some(ExecutorSubstrateState::SupervisorRespawning), None),
        deadline(Some(ExecutorSubstrateState::SlotAdoption), None),
        deadline(Some(ExecutorSubstrateState::ConnectedStalled), None),
        deadline(Some(ExecutorSubstrateState::PermanentRefusal), None),
        deadline(Some(ExecutorSubstrateState::Draining), None),
        deadline(None, None),
        deadline(None, Some(Vec::new())),
        deadline(
            None,
            Some(vec![HostPressureCondition::MemoryAvailable {
                available_bytes: 1,
                floor_bytes: 2,
                owed_bytes: 0,
                owing: Vec::new(),
                total_bytes: None,
                width_floor: None,
                alone_beside: None,
            }]),
        ),
        deadline(
            None,
            Some(vec![HostPressureCondition::DiskFree {
                inodes: None,
                free_bytes: 1,
                floor_bytes: 2,
            }]),
        ),
        deadline(
            Some(ExecutorSubstrateState::ConnectedStalled),
            Some(vec![HostPressureCondition::DiskFree {
                inodes: None,
                free_bytes: 1,
                floor_bytes: 2,
            }]),
        ),
    ];
    assert_eq!(
        no_start(&deadline(
            Some(ExecutorSubstrateState::PermanentRefusal),
            None,
        )),
        NoStart::Substrate(SubstrateFailureShape::Dispatch),
        "an explicit refusal must not claim Cairn lost contact with the machine"
    );
    for reason in specimens {
        assert_eq!(
            no_start(&reason).is_transient(),
            classify_unavailable(&reason, LinkRestoration::NotRestoring).is_capacity(),
            "check composition and fleet placement must read {reason:?} the same way"
        );
    }
}

/// Capacity and retirement of the selected environment are the two
/// conditions a fresh placement can relieve. Everything else is
/// re-presented unchanged and would only spend the machine twice.
/// A check preempted for an inference load is the named non-verdict: not
/// red, re-armable like capacity, charged to no machine or fleet, presented
/// again at once, and its record names the model and the measured memory.
#[test]
fn a_preempted_check_is_a_named_non_verdict_presented_again() {
    use crate::fleet::{inference::Preemption, CancelCause, CellAccount};
    let preemption = Preemption {
        deployment: "dsv4f-tp2".into(),
        model: "DeepSeek-V4-Flash".into(),
        executor: "bginf-sp1".into(),
        need_bytes: 100 << 30,
        free_bytes: 60 << 30,
        resident_bytes: 35 << 30,
    };
    let account = CellAccount {
        cancel_cause: Some(CancelCause::Preempted(std::sync::Arc::new(preemption))),
        ..CellAccount::default()
    };
    let cancelled = CellOutcome::Cancelled {
        request_id: "r".into(),
        attempt_id: "a".into(),
    };
    let Err(CheckExecutionFailure::Substrate(failure)) =
        check_result_from_cell_outcome(cancelled, &account, None)
    else {
        panic!("a preemption is a substrate non-verdict, not a cancellation");
    };
    assert_eq!(failure.shape(), SubstrateFailureShape::CommandPreempted);
    let message = failure.agent_message();
    assert!(
        message.contains("this is not a verdict on the code"),
        "{message}"
    );
    assert!(
        message.contains("Preempted to load DeepSeek-V4-Flash (inference deployment dsv4f-tp2) on bginf-sp1, which needed 100.0 GiB there with 60.0 GiB free while this work held 35.0 GiB, measured."),
        "{message}"
    );
    let kind = substrate_failure_kind(&failure);
    assert_eq!(kind, CheckFailureKind::Capacity);
    assert!(!kind.is_verdict_about_the_code(), "never red");
    assert_eq!(
        infra_repair_domain(Some(failure.shape()), Some(kind)),
        InfraRepairDomain::Head
    );

    let stopped = stopped_command_failure(
        &cairn_common::executor_protocol::CommandStop::Cancelled,
        "",
        serde_json::from_value(serde_json::json!({
            "executorId": "e-1", "slotId": "slot", "cellEpoch": 1,
            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
        }))
        .unwrap(),
        &account,
    );
    let CheckExecutionFailure::Substrate(stopped) = stopped else {
        panic!("a stopped preempted command is the same non-verdict");
    };
    assert_eq!(stopped.shape(), SubstrateFailureShape::CommandPreempted);
    assert!(stopped.provenance().is_some(), "it names where it ran");

    let outcome = PlannedCheckBatchOutcome::failed(vec![0, 1], failure);
    assert!(matches!(
        capacity_retry_decision(&outcome),
        CapacityRetry::Again { .. }
    ));
}

/// No observation carried `command_oom_killed` while bgsrv-ub's kernel
/// killed three rustc processes, because the stop never arrived. When it
/// does, the observation stores the shape, never prose, and the result
/// names the victim, its size and the machine.
#[test]
fn an_out_of_memory_stop_is_stored_as_its_shape_and_names_the_victim() {
    let stopped = stopped_command_failure(
        &cairn_common::executor_protocol::CommandStop::OomKilled {
            pid: 2_597_426,
            rss_bytes: Some(10_583_000_000),
            machine_total_bytes: Some(13_000_000_000),
            process_name: Some("rustc".into()),
            bound_bytes: None,
        },
        "error: could not compile `cairn-core`",
        serde_json::from_value(serde_json::json!({
            "executorId": "bgsrv-ub", "slotId": "slot", "cellEpoch": 1,
            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
        }))
        .unwrap(),
        &crate::fleet::CellAccount::default(),
    );
    let CheckExecutionFailure::Substrate(failure) = stopped else {
        panic!("an out-of-memory kill is a substrate failure");
    };
    assert_eq!(failure.shape(), SubstrateFailureShape::CommandOomKilled);
    assert_eq!(failure.shape().stored(), "command_oom_killed");
    let message = failure.agent_message();
    assert!(
        message.contains("Killed by the kernel out-of-memory killer: rustc (pid 2597426) at 9.9 GiB on a machine with 12.1 GiB"),
        "{message}"
    );
    assert!(!substrate_failure_kind(&failure).is_verdict_about_the_code());
}

#[test]
fn only_a_capacity_refusal_is_worth_asking_again() {
    assert!(SubstrateFailureShape::Capacity.is_transient());
    for shape in [
        SubstrateFailureShape::Dispatch,
        SubstrateFailureShape::MachineUnreachable,
        SubstrateFailureShape::Draining,
        SubstrateFailureShape::NoMachine,
        SubstrateFailureShape::Preparation,
        SubstrateFailureShape::Result,
        SubstrateFailureShape::Storage,
        SubstrateFailureShape::RunnerFault,
        SubstrateFailureShape::CommandStalled,
    ] {
        assert!(!shape.is_transient(), "{shape:?} is not relieved by time");
    }
}

fn capacity_outcome(indices: [usize; 2]) -> PlannedCheckBatchOutcome {
    PlannedCheckBatchOutcome::failed(
        indices.to_vec(),
        SubstrateFailure::new(SubstrateFailureShape::Capacity, "no room"),
    )
}

#[test]
fn capacity_is_represented_but_structural_failure_surfaces() {
    assert!(matches!(
        capacity_retry_decision(&capacity_outcome([0, 1])),
        CapacityRetry::Again { .. }
    ));
    let structural = PlannedCheckBatchOutcome::failed(
        vec![0],
        SubstrateFailure::new(SubstrateFailureShape::NoMachine, "no machine"),
    );
    assert_eq!(capacity_retry_decision(&structural), CapacityRetry::Surface);
}

#[test]
fn all_retired_candidates_are_represented_for_fresh_placement() {
    let all_dirty = PlannedCheckBatchOutcome::failed(
        vec![0, 1],
        SubstrateFailure::new(
            SubstrateFailureShape::EnvironmentRetired,
            "every selected slot was quarantined",
        ),
    );
    assert!(matches!(
        capacity_retry_decision(&all_dirty),
        CapacityRetry::Again { .. }
    ));
}

#[test]
fn cancellation_wins_over_a_retired_candidate_retry() {
    let mut outcome = PlannedCheckBatchOutcome::failed(
        vec![0],
        SubstrateFailure::new(
            SubstrateFailureShape::EnvironmentRetired,
            "selected slot was quarantined",
        ),
    );
    outcome.results.insert(
        1,
        Err(CheckExecutionFailure::Cancelled(
            crate::fleet::CancelCause::JobStopped,
        )),
    );
    assert_eq!(capacity_retry_decision(&outcome), CapacityRetry::Surface);
}

#[test]
fn a_completion_racing_slot_retirement_is_never_represented() {
    let mut outcome = PlannedCheckBatchOutcome::failed(
        vec![0],
        SubstrateFailure::new(
            SubstrateFailureShape::EnvironmentRetired,
            "selected slot was quarantined",
        ),
    );
    outcome.results.insert(
        1,
        Ok(CheckExecResult {
            exit_code: Some(0),
            output: "ran once".into(),
            timed_out: false,
            duration_ms: Some(1),
            provenance: None,
            publication: None,
        }),
    );
    assert_eq!(
        capacity_retry_decision(&outcome),
        CapacityRetry::Surface,
        "re-presenting after any completion could execute the command twice"
    );
}

fn predicted(relief_ms: u64, occupant_count: usize) -> crate::fleet::occupancy::MachineOccupancy {
    crate::fleet::occupancy::MachineOccupancy::Predicted(
        crate::fleet::occupancy::OccupancyForecast {
            relief_ms,
            blocking: "cairn-3414's rust-tests".into(),
            occupant_count,
        },
    )
}

#[test]
fn measured_and_unmeasured_holders_both_park() {
    let known = GroupWait::from_occupancy(predicted(240_000, 3));
    assert_eq!(
        known.description,
        "parked behind cairn-3414's rust-tests, predicted to finish in 4m, with 2 other cells running there; holding this check's queue entry until capacity is released"
    );
    let unmeasured =
        GroupWait::from_occupancy(crate::fleet::occupancy::MachineOccupancy::Unforecastable);
    assert!(unmeasured.description.contains("no measured duration"));
    assert!(unmeasured
        .description
        .contains("holding this check's queue entry"));
}

/// 2026-10-19T09:48:00Z.
const PARKED_AT: u64 = 1_792_403_280_000;

fn placing(since_unix_ms: u64) -> crate::fleet::CheckDispatch {
    crate::fleet::CheckDispatch {
        phase: crate::fleet::CheckDispatchPhase::Placing {
            since_unix_ms,
            declines: Vec::new(),
            placement: None,
            turn: None,
        },
        joined: None,
    }
}

fn running_on(
    executor_name: &str,
    since_unix_ms: u64,
    joined: Option<&str>,
) -> crate::fleet::CheckDispatch {
    crate::fleet::CheckDispatch {
        phase: crate::fleet::CheckDispatchPhase::Running {
            executor_name: executor_name.into(),
            since_unix_ms,
        },
        joined: joined.map(str::to_string),
    }
}

/// The live case: forecast 4m52s, still parked ten minutes later. Past
/// twice its own prediction the forecast is withdrawn for the rest of the
/// wait, even when a fresher reading offers a new finite number.
#[test]
fn a_parked_forecast_is_quoted_until_twice_its_own_prediction() {
    let mut narrator =
        DispatchNarrator::new(GroupWait::from_occupancy(predicted(292_000, 1)), PARKED_AT);
    let early = narrator
        .next(&placing(PARKED_AT), PARKED_AT + 60_000)
        .unwrap();
    assert_eq!(early.phase.as_deref(), Some("queued"));
    assert_eq!(
        early.diagnostic.as_deref(),
        Some("parked behind cairn-3414's rust-tests, predicted to finish in 4m52s; holding this check's queue entry until capacity is released; parked since 09:48:00Z")
    );
    assert_eq!(early.since_unix_ms, Some(PARKED_AT));

    let late = narrator
        .next(&placing(PARKED_AT), PARKED_AT + 600_000)
        .unwrap();
    assert_eq!(
        late.diagnostic.as_deref(),
        Some(
            "parked since 09:48:00Z; wait unpredictable (forecast 4m52s at 09:48:00Z, waited 10m)"
        )
    );

    narrator.absorb_wait(
        GroupWait::from_occupancy(predicted(60_000, 1)),
        PARKED_AT + 630_000,
    );
    let after = narrator.render(&placing(PARKED_AT), PARKED_AT + 660_000);
    assert_eq!(
        after.diagnostic.as_deref(),
        Some(
            "parked since 09:48:00Z; wait unpredictable (forecast 4m52s at 09:48:00Z, waited 11m)"
        )
    );
}

/// A claimed execution that is queued again (its machine was lost) waits
/// afresh: the first wait's withdrawn forecast does not follow it.
#[test]
fn a_requeued_check_starts_a_new_wait_with_its_own_forecast() {
    let mut narrator =
        DispatchNarrator::new(GroupWait::from_occupancy(predicted(292_000, 1)), PARKED_AT);
    narrator.note_phase(&placing(PARKED_AT).phase);
    narrator.render(&placing(PARKED_AT), PARKED_AT + 600_000);
    assert!(
        narrator.unpredictable,
        "the first wait outlived its forecast"
    );

    let claimed = running_on("fedora", PARKED_AT + 610_000, None);
    narrator.note_phase(&claimed.phase);
    let requeued_at = PARKED_AT + 3_600_000;
    narrator.note_phase(&placing(requeued_at).phase);
    assert!(
        narrator.wait_is_stale(requeued_at),
        "the new wait re-reads occupancy"
    );
    narrator.absorb_wait(GroupWait::from_occupancy(predicted(60_000, 1)), requeued_at);
    let status = narrator.render(&placing(requeued_at), requeued_at + 10_000);
    assert_eq!(
        status.diagnostic.as_deref(),
        Some("parked behind cairn-3414's rust-tests, predicted to finish in 1m; holding this check's queue entry until capacity is released; parked since 10:48:00Z")
    );
}

#[test]
fn an_unforecastable_machine_is_an_unpredictable_wait_for_good() {
    let mut narrator = DispatchNarrator::new(
        GroupWait::from_occupancy(crate::fleet::occupancy::MachineOccupancy::Unforecastable),
        PARKED_AT,
    );
    narrator.absorb_wait(
        GroupWait::from_occupancy(predicted(60_000, 1)),
        PARKED_AT + 30_000,
    );
    let status = narrator.render(&placing(PARKED_AT), PARKED_AT + 31_000);
    assert_eq!(
        status.diagnostic.as_deref(),
        Some("parked since 09:48:00Z; wait unpredictable: what holds the eligible machines has no measured duration")
    );
}

#[test]
fn a_claimed_check_says_where_it_runs_and_whose_run_it_joined() {
    let mut narrator =
        DispatchNarrator::new(GroupWait::from_occupancy(predicted(292_000, 1)), PARKED_AT);
    let own = narrator
        .next(
            &running_on("fedora", PARKED_AT + 9_000, None),
            PARKED_AT + 10_000,
        )
        .unwrap();
    assert_eq!(own.phase.as_deref(), Some("running"));
    assert_eq!(
        own.diagnostic.as_deref(),
        Some("running on fedora since 09:48:09Z")
    );
    assert_eq!(own.executor.as_deref(), Some("fedora"));
    assert_eq!(own.since_unix_ms, Some(PARKED_AT + 9_000));
    assert_eq!(
        narrator.next(
            &running_on("fedora", PARKED_AT + 9_000, None),
            PARKED_AT + 3_600_000
        ),
        None,
        "an unchanged phase emits nothing, however long it runs"
    );

    let joined = narrator.render(
        &running_on("fedora", PARKED_AT + 9_000, Some("cairn-5096")),
        PARKED_AT + 20_000,
    );
    assert_eq!(
        joined.diagnostic.as_deref(),
        Some("joined cairn-5096's run on fedora since 09:48:09Z")
    );
}

#[test]
fn a_parked_check_names_the_machines_that_declined_it() {
    let mut narrator = DispatchNarrator::new(
        GroupWait::from_occupancy(crate::fleet::occupancy::MachineOccupancy::Idle),
        PARKED_AT,
    );
    let dispatch = crate::fleet::CheckDispatch {
        phase: crate::fleet::CheckDispatchPhase::Placing {
            since_unix_ms: PARKED_AT,
            turn: None,
            placement: None,
            declines: vec![crate::fleet::CheckDispatchDecline {
                executor_name: "colocated".into(),
                reason: cairn_common::executor_protocol::DeclineReason::Disk,
                detail: Some("12 GiB free, floor 20 GiB".into()),
                at_unix_ms: PARKED_AT + 1_000,
                superseded_at_unix_ms: None,
                retry_after_unix_ms: PARKED_AT + 2_000,
            }],
        },
        joined: None,
    };
    let diagnostic = narrator
        .render(&dispatch, PARKED_AT + 2_000)
        .diagnostic
        .unwrap();
    assert!(
        diagnostic.ends_with(
            "; parked since 09:48:00Z; declined by colocated (low disk: 12 GiB free, floor 20 GiB)"
        ),
        "{diagnostic}"
    );
}

/// Every machine of the latest pass is named with what actually happened
/// to it, past any count: machines the runner excluded before an offer
/// are not offered, never declined; a superseded decline says so; and a
/// decline the pass did not consider is still told.
#[test]
fn a_parked_check_names_every_machine_of_its_latest_placement_pass() {
    use crate::fleet::{
        CheckCandidateStatus, CheckDispatchDecline, CheckPlacementCandidate, CheckPlacementPass,
    };
    use cairn_common::executor_protocol::{DeclineReason, PlacementRejectionReason};
    let mut narrator = DispatchNarrator::new(
        GroupWait::from_occupancy(crate::fleet::occupancy::MachineOccupancy::Idle),
        PARKED_AT,
    );
    let decline = |name: &str, superseded_at_unix_ms| CheckDispatchDecline {
        executor_name: name.into(),
        reason: DeclineReason::Pressure,
        detail: Some("memory short".into()),
        at_unix_ms: PARKED_AT + 1_000,
        superseded_at_unix_ms,
        retry_after_unix_ms: PARKED_AT + 5_000,
    };
    let ceiling = |estimate_bytes| {
        CheckCandidateStatus::NotOffered(PlacementRejectionReason::ExceedsMemoryCeiling {
            estimate_bytes,
            ceiling_bytes: 72_215_848_550,
            killed_here: None,
        })
    };
    let candidate = |name: &str, status| CheckPlacementCandidate {
        unreadable_memory: Vec::new(),
        executor_name: name.into(),
        status,
    };
    let pass = CheckPlacementPass {
        request_id: "d9e55c03".into(),
        attempt_id: "3e18a12d".into(),
        decided_at_unix_ms: PARKED_AT + 3_000,
        candidates: vec![
            candidate("bginf-sp1", ceiling(73_152_409_600)),
            candidate("bginf-sp2", ceiling(72_489_070_720)),
            candidate(
                "fedora",
                CheckCandidateStatus::Declined(decline("fedora", None)),
            ),
            candidate(
                "bbs",
                CheckCandidateStatus::Declined(decline("bbs", Some(PARKED_AT + 4_000))),
            ),
            candidate("mac", CheckCandidateStatus::Outranked { by: "win".into() }),
            candidate("win", CheckCandidateStatus::Offered),
            candidate(
                "inf-1",
                CheckCandidateStatus::NotOffered(PlacementRejectionReason::Drained),
            ),
            candidate(
                "inf-2",
                CheckCandidateStatus::NotOffered(PlacementRejectionReason::Drained),
            ),
        ],
    };
    let dispatch = crate::fleet::CheckDispatch {
        phase: crate::fleet::CheckDispatchPhase::Placing {
            since_unix_ms: PARKED_AT,
            turn: None,
            declines: vec![
                decline("fedora", None),
                decline("bbs", Some(PARKED_AT + 4_000)),
                decline("gone", None),
            ],
            placement: Some(pass),
        },
        joined: None,
    };
    let status = narrator.render(&dispatch, PARKED_AT + 6_000);
    let held_by = status.held_by.clone().unwrap();
    assert_eq!(
        held_by,
        format!(
            "latest placement pass 09:48:03Z (request d9e55c03, attempt 3e18a12d): \
             offered to win, unanswered; \
             declined by fedora (host pressure: memory short), \
             bbs (host pressure: memory short; its posted state changed at 09:48:04Z, offerable again from 09:48:05Z); \
             eligible, not chosen: mac (outranked by win); \
             not offered to bginf-sp1 ({}); \
             not offered to bginf-sp2 ({}); \
             not offered to inf-1, inf-2 (drained, refusing new work); \
             earlier declined by gone (host pressure: memory short)",
            PlacementRejectionReason::ExceedsMemoryCeiling {
                estimate_bytes: 73_152_409_600,
                ceiling_bytes: 72_215_848_550,
                killed_here: None,
            }
            .describe(),
            PlacementRejectionReason::ExceedsMemoryCeiling {
                estimate_bytes: 72_489_070_720,
                ceiling_bytes: 72_215_848_550,
                killed_here: None,
            }
            .describe(),
        )
    );
    assert!(!held_by.contains("more"), "{held_by}");
    assert!(
        status
            .diagnostic
            .as_deref()
            .unwrap()
            .ends_with(&format!("; parked since 09:48:00Z; {held_by}")),
        "{status:?}"
    );
}

#[test]
fn a_check_wait_names_unreadable_memory_evidence() {
    let pass = crate::fleet::CheckPlacementPass {
        request_id: "request".into(),
        attempt_id: "attempt".into(),
        decided_at_unix_ms: 1,
        candidates: vec![crate::fleet::CheckPlacementCandidate {
            executor_name: "spark".into(),
            status: crate::fleet::CheckCandidateStatus::NotOffered(
                cairn_common::executor_protocol::PlacementRejectionReason::Drained,
            ),
            unreadable_memory: vec![cairn_common::executor_protocol::MemoryEvidenceFailure {
                query: "suite_evidence".into(),
                error_class: "schema".into(),
                error: "no such column: job_census_samples".into(),
            }],
        }],
    };
    let line = describe_placement_pass(&pass, &[]);
    assert!(line.contains("not offered to spark (drained"), "{line}");
    assert!(
        line.contains("spark: memory evidence unreadable: suite_evidence (schema:"),
        "{line}"
    );
}

/// The board follows the execution while the submission is outstanding,
/// and says each phase once.
#[tokio::test(start_paused = true)]
async fn an_outstanding_batch_moves_from_parked_to_running_on_its_board() {
    let reads = std::sync::atomic::AtomicUsize::new(0);
    let shown = std::sync::Mutex::new(Vec::new());
    let delivered = narrate_dispatch(
        async {
            tokio::time::sleep(std::time::Duration::from_secs(7)).await;
            "delivered"
        },
        &GroupWait::from_occupancy(predicted(292_000, 1)),
        || {
            Some(
                match reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    0 | 1 => placing(PARKED_AT),
                    _ => running_on("fedora", PARKED_AT + 9_000, None),
                },
            )
        },
        || -> GroupWait { unreachable!("the forecast stays fresh for 30s") },
        |status| shown.lock().unwrap().push(status),
    )
    .await;
    assert_eq!(delivered, "delivered");
    assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    let phases: Vec<_> = shown
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|status| (status.phase.unwrap(), status.executor))
        .collect();
    assert_eq!(
        phases,
        vec![
            ("queued".to_string(), None),
            ("running".to_string(), Some("fedora".to_string())),
        ]
    );
}

#[test]
fn a_manual_requester_keeps_the_running_diagnostic_and_its_machine() {
    let update = manual_progress_update(
        &[],
        CheckPhaseStatus {
            executor: Some("fedora".into()),
            since_unix_ms: Some(PARKED_AT + 9_000),
            ..check_phase_status(
                Some("running"),
                Some("running checks"),
                Some("running on fedora since 09:48:09Z".into()),
            )
        },
    );
    assert_eq!(
        update,
        ManualCheckProgressUpdate {
            state: "running".into(),
            diagnostic: "running on fedora since 09:48:09Z".into(),
            executor: Some("fedora".into()),
            since_unix_ms: Some(PARKED_AT + 9_000),
        }
    );
    let parked = manual_progress_update(
        &[],
        check_phase_status(Some("queued"), Some("queued"), None),
    );
    assert_eq!(parked.state, "parked");
    assert_eq!(parked.diagnostic, "parked waiting for check capacity");
}

/// The named wait reaches the agent. A capacity row that says only "there
/// was no room" is the row CAIRN-3429 was filed about; every other shape is
/// left alone because none of them was about waiting.
#[test]
fn only_a_capacity_refusal_is_told_what_it_waited_on() {
    let mut outcome = capacity_outcome([0, 1]);
    outcome.results.insert(
        2,
        Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::NoMachine,
            "no executor advertises matlab",
        )),
    );
    attribute_capacity_wait(
        &mut outcome,
        &GroupWait::from_occupancy(predicted(240_000, 3)),
    );

    for index in [0, 1] {
        let Some(Err(CheckExecutionFailure::Substrate(failure))) = outcome.results.get(&index)
        else {
            panic!("index {index} is a substrate failure");
        };
        let message = failure.agent_message();
        assert!(
            message.contains("cairn-3414's rust-tests"),
            "a capacity row names what held the machine: {message}"
        );
        assert!(
            message.contains(SUBSTRATE_FAILURE_CONSEQUENCE),
            "the consequence still closes the message: {message}"
        );
    }

    let Some(Err(CheckExecutionFailure::Substrate(structural))) = outcome.results.get(&2) else {
        panic!("index 2 is a substrate failure");
    };
    let message = structural.agent_message();
    assert!(message.starts_with(&format!(
        "{} {SUBSTRATE_FAILURE_CONSEQUENCE}",
        SubstrateFailureShape::NoMachine.lead()
    )));
    assert!(message.contains("no executor advertises matlab"));
    assert!(
        !message.contains("cairn-3414's rust-tests"),
        "a refusal that was never about waiting gains no wait story: {message}"
    );
}

/// Two selector groups are two different waits.
///
/// A batch whose items name different executors is presented as separate
/// requests against separate machines, so each group is blocked by its own
/// occupant and is attributed before the results merge. One basis stamped
/// across the whole batch would tell half its rows the name of work they
/// were never queued behind.
#[test]
fn each_selector_group_is_attributed_to_its_own_blocker() {
    let mut linux_group = capacity_outcome([0, 1]);
    attribute_capacity_wait(
        &mut linux_group,
        &GroupWait::from_occupancy(crate::fleet::occupancy::MachineOccupancy::Predicted(
            crate::fleet::occupancy::OccupancyForecast {
                relief_ms: 300_000,
                blocking: "cairn-3414's rust-tests".into(),
                occupant_count: 1,
            },
        )),
    );

    let mut local_group = PlannedCheckBatchOutcome::failed(
        vec![2],
        SubstrateFailure::new(SubstrateFailureShape::Capacity, "no room"),
    );
    attribute_capacity_wait(
        &mut local_group,
        &GroupWait::from_occupancy(crate::fleet::occupancy::MachineOccupancy::Predicted(
            crate::fleet::occupancy::OccupancyForecast {
                relief_ms: 4_000,
                blocking: "cairn-3421's frontend-tests".into(),
                occupant_count: 1,
            },
        )),
    );

    let mut combined = linux_group;
    combined.results.extend(local_group.results);

    let waited_on = |index: usize| match combined.results.get(&index) {
        Some(Err(CheckExecutionFailure::Substrate(failure))) => {
            failure.waited_on().unwrap().to_string()
        }
        _ => panic!("index {index} is a capacity failure"),
    };
    assert!(waited_on(0).contains("cairn-3414's rust-tests"));
    assert!(waited_on(1).contains("cairn-3414's rust-tests"));
    assert!(
        waited_on(2).contains("cairn-3421's frontend-tests"),
        "the second group kept its own blocker: {}",
        waited_on(2)
    );

    // And the operator's one log line reports both, without repeating the
    // group that covered two checks.
    let logged = describe_capacity_waits(&combined);
    assert_eq!(
        logged.matches("parked behind").count(),
        2,
        "two distinct waits, deduplicated within each group: {logged}"
    );
}

/// What must NOT retry. A structural refusal has the same answer next time,
/// a cancellation was deliberate, and a partially-run batch already spent
/// the machine on the part that ran.
#[test]
fn a_refusal_that_time_cannot_relieve_surfaces_at_once() {
    for shape in [
        SubstrateFailureShape::NoMachine,
        SubstrateFailureShape::Draining,
        SubstrateFailureShape::Preparation,
        SubstrateFailureShape::Storage,
        SubstrateFailureShape::Result,
        SubstrateFailureShape::RunnerFault,
        SubstrateFailureShape::CommandStalled,
        SubstrateFailureShape::Dispatch,
        SubstrateFailureShape::EnvironmentRetired,
    ] {
        let outcome =
            PlannedCheckBatchOutcome::failed(vec![0], SubstrateFailure::new(shape, "diagnostic"));
        let decision = capacity_retry_decision(&outcome);
        if shape == SubstrateFailureShape::EnvironmentRetired {
            assert!(matches!(decision, CapacityRetry::Again { .. }));
        } else {
            assert_eq!(
                decision,
                CapacityRetry::Surface,
                "{shape:?} must not be asked again"
            );
        }
    }

    let mut partial = capacity_outcome([0, 1]);
    partial.results.insert(
        1,
        Ok(CheckExecResult {
            exit_code: Some(0),
            output: "ran".into(),
            timed_out: false,
            duration_ms: Some(1),
            provenance: None,
            publication: None,
        }),
    );
    assert_eq!(
        capacity_retry_decision(&partial),
        CapacityRetry::Surface,
        "a batch that partly ran is not the same ask any more"
    );

    assert_eq!(
        capacity_retry_decision(&PlannedCheckBatchOutcome {
            results: HashMap::new(),
            request: None,
            delta: None,
            store_dir: None,
        }),
        CapacityRetry::Surface,
        "an empty outcome states no condition to retry"
    );
}

/// The composition rule at the seam: every executor outcome names its
/// failing step inline while retaining the complete operator diagnostic.
#[test]
fn every_substrate_outcome_composes_an_agent_message_with_the_failing_step() {
    use cairn_common::executor_protocol::CellUnavailableReason::*;
    // The specimen from the transcript that opened CAIRN-3219.
    let scratch_path = "remove cell scratch /Users/mitch/.cairn/build-slots/cairn/.authority/slot-366/scratch: Directory not empty";
    let outcomes = vec![
        CellOutcome::StorageFailure {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            stage: cairn_common::executor_protocol::StorageFailureStage::Recovery,
            kind: cairn_common::executor_protocol::StorageFailureKind::CleanupFailed,
            diagnostic: scratch_path.to_string(),
            slot_retired: true,
        },
        CellOutcome::Unavailable {
            reason: Deadline {
                host_pressure: None,
                substrate: Some(cairn_common::executor_protocol::ExecutorSubstrateEvidence {
                    state: cairn_common::executor_protocol::ExecutorSubstrateState::CapacityBusy,
                    since_unix_ms: 0,
                    last_progress_unix_ms: 0,
                    diagnostic: None,
                    queue_depth: Some(4),
                    queue_position: Some(3),
                    active_cell_count: Some(2),
                    oldest_running_started_at_unix_ms: None,
                }),
            },
            diagnostic: "acquisition deadline elapsed".to_string(),
        },
        CellOutcome::Unavailable {
            reason: Spawn,
            diagnostic: "/Users/mitch/.cairn/build-slots/cairn/slot-366 spawn refused".to_string(),
        },
        CellOutcome::FailedAfterExecution {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            diagnostic: "executor lost the slot before publication".to_string(),
        },
        CellOutcome::Unavailable {
            reason: NoMatchingExecutor {
                rejected: Vec::new(),
            },
            diagnostic: "no executor advertises toolchain matlab".to_string(),
        },
        CellOutcome::Unavailable {
            reason: AdmissionRejected {
                reason: cairn_common::executor_protocol::AdmissionRejectionReason::Draining,
            },
            diagnostic: "executor slot pool is draining".to_string(),
        },
        CellOutcome::Unavailable {
            reason: Preparation,
            diagnostic: "could not load canonical project execution policy".to_string(),
        },
        // The specimen from the transcript that opened CAIRN-3430, now on
        // the reason that says the environment was given up rather than the
        // one that says preparation failed.
        CellOutcome::Unavailable {
            reason: SlotUnhealthy,
            diagnostic: scratch_path.to_string(),
        },
        CellOutcome::Unavailable {
            reason: ExecutorUnavailable,
            diagnostic: "connection closed while the request was queued".to_string(),
        },
    ];

    let mut leads = BTreeSet::new();
    for outcome in outcomes {
        let failure =
            check_result_from_cell_outcome(outcome, &crate::fleet::CellAccount::default(), None)
                .expect_err("substrate outcome must not become a command verdict");
        let CheckExecutionFailure::Substrate(failure) = failure else {
            panic!("a substrate outcome must compose a substrate failure");
        };
        let message = failure.agent_message();
        assert!(
            message.contains("not a result about your change")
                && message.contains("Failing step:")
                && message.contains(failure.diagnostic()),
            "agent message must say whose failure this is and name the failing step: {message}"
        );
        assert!(
            !failure.diagnostic().is_empty(),
            "the operator half must keep the diagnostic"
        );
        leads.insert(
            message
                .split_once(". ")
                .expect("lead sentence")
                .0
                .to_string(),
        );
    }
    // Nine outcome classes, nine distinct openings: composition informs, it
    // does not flatten every infrastructure failure into one sentence. The
    // count is the assertion — a new class that reuses an existing lead is
    // exactly the collapse this guards against. (A cancellation is not a
    // substrate outcome and has no lead here.)
    assert_eq!(leads.len(), 9, "each outcome class needs its own lead");
}

/// The inline diagnostic of a substrate failure, with the authored
/// explanation in front of it removed.
fn inline_diagnostic(failure: &SubstrateFailure) -> String {
    let message = failure.agent_message();
    let (explanation, inline) = message
        .split_once("\nFailing step:\n")
        .expect("message keeps the authored explanation before its diagnostic");
    assert!(
        explanation.starts_with(failure.shape().lead())
            && explanation.ends_with(SUBSTRATE_FAILURE_CONSEQUENCE),
        "the authored explanation still leads the failing step: {explanation}"
    );
    inline.to_string()
}

/// Every shape, so the bound is checked against all of them rather than
/// against whichever one a specimen happens to use. The match below makes
/// the list exhaustive by construction: a new shape stops compiling here
/// until it is added.
const ALL_SUBSTRATE_SHAPES: [SubstrateFailureShape; 18] = [
    SubstrateFailureShape::Dispatch,
    SubstrateFailureShape::Capacity,
    SubstrateFailureShape::MachineUnreachable,
    SubstrateFailureShape::NoReachableMachine,
    SubstrateFailureShape::Draining,
    SubstrateFailureShape::NoMachine,
    SubstrateFailureShape::Preparation,
    SubstrateFailureShape::EnvironmentRetired,
    SubstrateFailureShape::Result,
    SubstrateFailureShape::OutcomeUnknown,
    SubstrateFailureShape::Storage,
    SubstrateFailureShape::RunnerFault,
    SubstrateFailureShape::CommandStalled,
    SubstrateFailureShape::CommandPreempted,
    SubstrateFailureShape::CommandOomKilled,
    SubstrateFailureShape::BuildVolumeAbsent,
    SubstrateFailureShape::BuildVolumeLost,
    SubstrateFailureShape::MeasurementBlind,
];

/// A failing observation stores its shape as a token, and rows already on
/// disk must keep their meaning: every shape round-trips, no two share a
/// token, and the token the dormant re-arm reads is pinned literally, so a
/// rename of the variant cannot move it.
#[test]
fn every_shape_round_trips_through_its_stored_token() {
    let mut seen = std::collections::HashSet::new();
    for shape in ALL_SUBSTRATE_SHAPES {
        assert!(seen.insert(shape.stored()), "{shape:?} shares a token");
        assert_eq!(
            SubstrateFailureShape::from_stored(shape.stored()),
            Some(shape)
        );
    }
    assert_eq!(
        SubstrateFailureShape::CommandPreempted.stored(),
        "command_preempted"
    );
    assert_eq!(
        SubstrateFailureShape::from_stored("CommandPreempted"),
        None,
        "the token is not the variant's Debug name"
    );
}

/// The bound is on the COMPOSED message, so it has to hold for every lead
/// and for an attribution of any length — not for the one shape a specimen
/// happens to exercise. The ten leads differ by tens of characters, and a
/// capacity refusal splices in placement text with no structural limit, so
/// a per-diagnostic bound leaves the stored record free to overrun.
#[test]
fn every_shape_composes_inside_the_message_bound() {
    let log = "x".repeat(20_000);

    for shape in ALL_SUBSTRATE_SHAPES {
        match shape {
            SubstrateFailureShape::Dispatch
            | SubstrateFailureShape::Capacity
            | SubstrateFailureShape::MachineUnreachable
            | SubstrateFailureShape::NoReachableMachine
            | SubstrateFailureShape::Draining
            | SubstrateFailureShape::NoMachine
            | SubstrateFailureShape::Preparation
            | SubstrateFailureShape::EnvironmentRetired
            | SubstrateFailureShape::Result
            | SubstrateFailureShape::OutcomeUnknown
            | SubstrateFailureShape::Storage
            | SubstrateFailureShape::RunnerFault
            | SubstrateFailureShape::CommandStalled
            | SubstrateFailureShape::CommandPreempted
            | SubstrateFailureShape::CommandOomKilled
            | SubstrateFailureShape::BuildVolumeAbsent
            | SubstrateFailureShape::BuildVolumeLost
            | SubstrateFailureShape::MeasurementBlind => {}
        }

        let message = SubstrateFailure::new(shape, &log).agent_message();
        assert!(
            message.chars().count() <= SUBSTRATE_AGENT_MESSAGE_CHARS,
            "{shape:?} composes to {} chars, past the bound",
            message.chars().count()
        );
        assert!(
            message.contains(" characters omitted "),
            "{shape:?} must still name what it dropped"
        );
    }

    // Attribution is composed from placement records and has no length of
    // its own, so it is the path that can push the total past the bound —
    // and the path that could silently spend the evidence's share.
    let wait = "CAIRN-3414's rust-tests ".repeat(200);
    let mut attributed = SubstrateFailure::new(SubstrateFailureShape::Capacity, &log);
    attributed.name_the_wait(&wait);
    let message = attributed.agent_message();

    assert!(
        message.chars().count() <= SUBSTRATE_AGENT_MESSAGE_CHARS,
        "an attributed capacity refusal composes to {} chars, past the bound",
        message.chars().count()
    );
    assert!(
        message.contains("CAIRN-3414's rust-tests"),
        "the attribution still identifies the wait: {message}"
    );
    assert!(
        message.contains(" characters omitted ") && message.contains('x'),
        "attribution never crowds the evidence out of the message: {message}"
    );
    assert_eq!(
        attributed.waited_on(),
        Some(wait.as_str()),
        "the operator's copy of the attribution is not abridged"
    );
}

/// `bounded_diagnostic` accepts any budget and promises never to truncate
/// silently, so the promise has to hold at the sizes too small to state an
/// omission count — where a bare tail would read as a complete diagnostic.
#[test]
fn a_budget_too_small_to_name_its_omission_still_marks_it() {
    let diagnostic = "abcdefghij".repeat(6);
    let marker = omission_marker(diagnostic.chars().count()).chars().count();

    assert_eq!(
        bounded_diagnostic(&diagnostic, 0),
        "",
        "a budget of nothing can say nothing"
    );

    for budget in 1..=marker {
        let bounded = bounded_diagnostic(&diagnostic, budget);
        assert!(
            bounded.chars().count() <= budget,
            "budget {budget} produced {} chars",
            bounded.chars().count()
        );
        assert!(
            bounded.starts_with('…'),
            "budget {budget} truncated silently: {bounded}"
        );
        assert!(
            diagnostic.ends_with(bounded.trim_start_matches('…')),
            "budget {budget} kept the end of the diagnostic: {bounded}"
        );
    }

    assert_eq!(bounded_diagnostic(&diagnostic, 1), "…");
    assert_eq!(bounded_diagnostic(&diagnostic, 3), "…ij");
}

#[test]
fn agent_diagnostic_is_unicode_safe_and_bounded_without_mutating_operator_detail() {
    let diagnostic = "é".repeat(SUBSTRATE_AGENT_MESSAGE_CHARS + 1);
    let failure = SubstrateFailure::new(SubstrateFailureShape::Preparation, &diagnostic);
    let inline = inline_diagnostic(&failure);

    assert!(
        failure.agent_message().chars().count() <= SUBSTRATE_AGENT_MESSAGE_CHARS,
        "the composed message stays inside its bound: {}",
        failure.agent_message().chars().count()
    );
    let (head, rest) = inline
        .split_once("\n… ")
        .expect("an elided diagnostic names its gap");
    let (omitted, end) = rest
        .split_once(" …\n")
        .expect("the omission marker closes before the surviving tail");

    assert_eq!(head.chars().count(), INLINE_DIAGNOSTIC_HEAD_CHARS);
    assert!(head.chars().all(|character| character == 'é'));
    assert!(
        !end.is_empty() && end.chars().all(|character| character == 'é'),
        "the remainder of the budget is spent on the end of the diagnostic"
    );
    assert_eq!(
        omitted,
        format!(
            "{} characters omitted",
            diagnostic.chars().count() - head.chars().count() - end.chars().count()
        )
    );
    assert_eq!(failure.diagnostic(), diagnostic);
}

/// A diagnostic that fits is rendered exactly as it was composed: the
/// two-ended bound is what happens at overflow, not a reformatting every
/// failure pays for.
#[test]
fn a_diagnostic_that_fits_is_rendered_whole_and_unmarked() {
    let diagnostic = "remote cleanup could not be verified: Directory not empty";
    let failure = SubstrateFailure::new(SubstrateFailureShape::Preparation, diagnostic);

    assert_eq!(inline_diagnostic(&failure), diagnostic);
    assert_eq!(
        bounded_diagnostic(diagnostic, SUBSTRATE_AGENT_MESSAGE_CHARS),
        diagnostic,
        "nothing is omitted from a diagnostic that fits"
    );
}

/// Two misdiagnoses four days apart read a SIGKILLed remote process as an
/// inexplicable clean transport exit, because the stored diagnostic kept
/// the beginning of the captured executor log and cut the end — which is
/// where processes die. The lines that decide the question, including the
/// machine-parseable `CAIRN_REMOTE_PROCESS_EXIT`, are the last ones, so the
/// budget's remainder is spent there (cairn/4380).
#[test]
fn an_overflowing_executor_log_keeps_the_lines_that_say_how_the_process_died() {
    let mut log = String::from(
        "the link to this machine was lost: the SSH session carrying the link exited after 1507s (exit status: 0)\n",
    );
    for stamp in 0..400 {
        log.push_str(&format!(
            "17:23:{:02} executor progress stamp {stamp}: cell running, {} MB available\n",
            stamp % 60,
            506 - stamp,
        ));
    }
    log.push_str("setsid: child 1442406 did not exit normally: Success\n");
    log.push_str("CAIRN_REMOTE_PROCESS_EXIT=9\n");

    let failure = SubstrateFailure::new(SubstrateFailureShape::MachineUnreachable, &log);
    let message = failure.agent_message();
    let inline = inline_diagnostic(&failure);

    assert!(
        message.chars().count() <= SUBSTRATE_AGENT_MESSAGE_CHARS,
        "the whole rendered failure stays inside the storage bound it always had: {}",
        message.chars().count()
    );
    assert!(
        inline.starts_with("the link to this machine was lost: the SSH session carrying the link exited after 1507s (exit status: 0)"),
        "the head still identifies which failure this is: {inline}"
    );
    assert!(
        inline.contains("CAIRN_REMOTE_PROCESS_EXIT=9")
            && inline.contains("setsid: child 1442406 did not exit normally"),
        "the decisive final lines survive the bound: {inline}"
    );
    assert!(
        inline.contains(" characters omitted "),
        "the gap between head and tail is named, never silent: {inline}"
    );

    // The old bound spent the whole budget on the beginning, so the two
    // facts above could not have reached a reader at all. Taking the whole
    // message budget as a head is strictly more generous than the bound
    // that produced the misdiagnoses, so a specimen that defeats this one
    // defeats that one too.
    let head_only: String = log.chars().take(SUBSTRATE_AGENT_MESSAGE_CHARS).collect();
    assert!(
        !head_only.contains("CAIRN_REMOTE_PROCESS_EXIT=9")
            && !head_only.contains("setsid: child 1442406"),
        "the specimen must be one a prefix-only bound would have destroyed"
    );

    assert_eq!(
        failure.diagnostic(),
        log,
        "the operator's copy is not abridged"
    );
}

/// Composition happens at the executor seam, so it must survive the whole
/// path to the text an agent actually reads: the check outcome appended to
/// its tool result, and the cache row the `/checks` surface and the wake
/// detail read back later.
#[tokio::test]
async fn the_agent_facing_verdict_and_cache_row_carry_the_failing_step() {
    let scratch_path = "remove cell scratch /Users/mitch/.cairn/build-slots/cairn/.authority/slot-366/scratch: Directory not empty";
    let failure = check_result_from_cell_outcome(
        CellOutcome::StorageFailure {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            stage: cairn_common::executor_protocol::StorageFailureStage::Recovery,
            kind: cairn_common::executor_protocol::StorageFailureKind::CleanupFailed,
            diagnostic: scratch_path.to_string(),
            slot_retired: true,
        },
        &crate::fleet::CellAccount::default(),
        None,
    )
    .expect_err("a storage failure is never a verdict");

    let db = cache_db().await;
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-specimen",
        "job-test",
        &[(plan("rust", "cargo test"), "input-specimen".to_string())],
        "tool",
        CheckExecMode::Shared,
        None,
        move |_, _, _| {
            let failure = failure.clone();
            async move { Err::<CheckExecResult, _>(failure) }
        },
        |_| {},
    )
    .await;

    assert_eq!(
        results[0].failure_kind,
        Some(CheckFailureKind::Infrastructure),
        "an infrastructure failure keeps its classification"
    );
    let row = crate::execution::cache::list_check_results(db, "project-a", "tree-specimen")
        .unwrap()
        .into_iter()
        .find(|row| row.check_name == "rust")
        .expect("the failure is recorded for the checks surface");

    for text in [results[0].output_tail.as_str(), row.output_tail.as_str()] {
        assert!(
            text.starts_with("Cairn's own storage for this check failed."),
            "the verdict text is the composed message: {text}"
        );
        assert!(text.contains("Failing step:"));
        assert!(text.contains(scratch_path));
    }
}

/// cairn/6341. A macOS row records the nested-Seatbelt state its executor
/// attested for the run, so a row from a lane under the credential floor
/// says it ran without the capability. A Linux row, and evidence that
/// names no measured context, record nothing.
#[test]
fn a_macos_row_records_the_lanes_attested_nested_seatbelt_state() {
    use cairn_common::executor_protocol::{
        NestedSandboxProbeContext, SandboxCapabilityEvidence, SandboxLauncherClass,
    };
    let floored = SandboxCapabilityEvidence {
        cairn_sandboxed_marker: false,
        nested_sandbox_write_available: false,
        nested_sandbox_write_detail: "nested Seatbelt in-bounds temp write refused (exit Some(71))"
            .into(),
        nested_sandbox_write_context: Some(NestedSandboxProbeContext {
            launcher: SandboxLauncherClass::Unconfined,
            credential_floor_sha256: Some("ab".repeat(32)),
            outer_profile_sha256: Some("cd".repeat(32)),
            fingerprint: Some("ef".repeat(32)),
            measured_at_unix_ms: 1,
        }),
        violation_recovery: None,
    };
    let meta = |platform: &str, sandbox: Option<SandboxCapabilityEvidence>| {
        cairn_common::executor_protocol::CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: "local".to_string(),
            executor_device_id: "device".to_string(),
            executor_connection_generation: 1,
            cell_id: "slot".to_string(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: "env".to_string(),
            verdict_platform: Some(platform.to_string()),
            verdict_arch: Some("aarch64".to_string()),
            verdict_environment_hash: None,
            sandbox,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        }
    };
    assert_eq!(
        nested_sandbox_lane(Some(&meta("macos", Some(floored.clone())))).as_deref(),
        Some(
            "unavailable (under the operator credential floor abababababab; launcher unconfined): nested Seatbelt in-bounds temp write refused (exit Some(71))"
        )
    );
    assert_eq!(
        nested_sandbox_lane(Some(&meta("linux", Some(floored.clone())))),
        None
    );
    let mut contextless = floored;
    contextless.nested_sandbox_write_context = None;
    assert_eq!(
        nested_sandbox_lane(Some(&meta("macos", Some(contextless)))),
        None
    );
    assert_eq!(nested_sandbox_lane(Some(&meta("macos", None))), None);
    assert_eq!(nested_sandbox_lane(None), None);
}

#[tokio::test]
async fn remote_executor_without_environment_identity_is_not_reusable() {
    let db = cache_db().await;
    let provenance = cairn_common::executor_protocol::CellExecutionMeta {
        warmth: None,
        load_context: None,
        executor_id: "executor-a".to_string(),
        executor_device_id: "device-a".to_string(),
        executor_connection_generation: 3,
        cell_id: "slot-a".to_string(),
        cell_epoch: 4,
        started_at_unix_ms: 100,
        finished_at_unix_ms: 200,
        duration_ms: None,
        peak_rss_bytes: None,
        peak_physical_footprint_bytes: None,
        peak_process: None,
        cpu: None,
        allocation: None,
        disk_delta_bytes: None,
        measurement_quality: None,
        environment_fingerprint: String::new(),
        verdict_platform: None,
        verdict_arch: None,
        verdict_environment_hash: None,
        sandbox: None,
        toolchain_fingerprint: None,
        containment: None,
        stop: None,
        cell_origin: None,
        test_phase: None,
        build_job_processes: None,
    };
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-provenance",
        "job-a",
        &[(plan("rust", "cargo test"), "input-provenance".to_string())],
        "tool",
        CheckExecMode::Shared,
        None,
        move |_, _, _| {
            let provenance = provenance.clone();
            async move {
                Ok::<_, String>(CheckExecResult {
                    exit_code: Some(0),
                    output: "ok".to_string(),
                    timed_out: false,
                    duration_ms: Some(12_345),
                    provenance: Some(provenance),
                    publication: None,
                })
            }
        },
        |_| {},
    )
    .await;
    assert!(results[0].passed);
    assert_eq!(results[0].duration_ms, 12_345);
    assert!(
        get_reusable_check_result(
            db,
            ReusableCheckLookup {
                project_id: "project-a",
                check_name: "rust",
                input_hash: "input-provenance",
                verdict_platforms: &[
                    crate::execution::check_identity::gating_platform().to_string()
                ],
                executor_ids: None,
                implementation_identity: cairn_checkenv::implementation_identity(),
                verdict_environment_hash: &cairn_checkenv::verdict_environment_hash(
                    Vec::<String>::new(),
                    |_| None,
                ),
                result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION
                    as i64,
            }
        )
        .unwrap()
        .is_none(),
        "a remote verdict with no executor-computed environment identity must miss"
    );
}

#[test]
fn merge_gate_treats_command_timeout_as_infrastructure_failure() {
    let result = review_tree_gate_result(vec![CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: "rust-full".to_string(),
        passed: false,
        exit_code: None,
        failure_kind: Some(CheckFailureKind::TimedOut),
        parsed: None,
        output_tail: "timed out after 30m".to_string(),
        cached: false,
        recorded: None,
        duration_ms: 1_800_000,
        suppressed_after: None,
        not_recorded: None,
    }]);
    assert!(matches!(
        result,
        ReviewTreeGateResult::InfrastructureFailure(ref detail)
            if detail.contains("rust-full") && detail.contains("timed out")
    ));
}

fn cancelled_outcome(name: &str) -> CheckOutcome {
    record_cancelled_check("job", name, crate::fleet::CancelCause::JobStopped)
}

/// A cancelled gating lane judged nothing: the merge is told to retry,
/// never that the tree is red or that the infrastructure failed.
#[test]
fn merge_gate_reports_a_cancelled_lane_as_a_cancellation() {
    let result = review_tree_gate_result(vec![cancelled_outcome("rust-tests")]);
    let ReviewTreeGateResult::Cancelled(detail) = result else {
        panic!("a cancelled lane must gate as a cancellation, got {result:?}");
    };
    assert_eq!(detail, "check 'rust-tests' cancelled: job stopped");
}

/// A cancellation must not hide a real red that came after it.
#[test]
fn merge_gate_still_reports_a_red_behind_a_cancelled_lane() {
    let mut red = outcome("lint", false, Some(1));
    red.output_tail = "error: unused".to_string();
    let result = review_tree_gate_result(vec![cancelled_outcome("rust-tests"), red]);
    assert!(matches!(
        result,
        ReviewTreeGateResult::CheckFailed { ref name, .. } if name == "lint"
    ));
}

#[test]
fn a_cancellation_is_neither_infrastructure_nor_a_verdict() {
    let kind = CheckFailureKind::Cancelled;
    assert!(!kind.is_infrastructure());
    assert!(!kind.is_verdict_about_the_code());
    assert_eq!(CheckFailureKind::from_stored(kind.as_str()), Some(kind));
    assert!(CheckFailureKind::Killed.is_verdict_about_the_code());
    assert!(!CheckFailureKind::Infrastructure.is_verdict_about_the_code());
    let cancelled = cancelled_outcome("rust-tests");
    assert!(!cancelled.is_genuine_failure());
    let summary = format_check_summary(&[cancelled], &HashMap::new());
    assert_eq!(summary, "\u{26a0} rust-tests (cancelled: job stopped)");
}

#[test]
fn adoption_recording_plan_never_serializes_process_credentials() {
    let mut selected = plan("smoke", "echo selected");
    selected.scope = CheckScope::Partial;
    let recording = CheckAdoptionRecordingPlan::new(
        &selected,
        &crate::execution::cache::SealedCheckCoordinate::new(
            "commit",
            "definition",
            "tree",
            "input",
        ),
        "job",
        "manual-check:producer",
        &[("MCP_CALLBACK_BEARER".into(), "secret-bearer-value".into())],
    );
    let json = serde_json::to_string(&recording).unwrap();
    assert!(!json.contains("secret-bearer-value"));
    assert!(!json.contains("MCP_CALLBACK_BEARER"));
    let restored: CheckAdoptionRecordingPlan = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.plan.scope, CheckScope::Partial);
    assert_eq!(restored.plan.command, "echo selected");
    assert!(restored.effective_env.is_empty());
}

#[tokio::test]
async fn a_remote_lane_left_running_records_no_failure_or_cancellation() {
    let attempt = crate::fleet::LeftRunningAttempt {
        machine: "fedora".into(),
        request_id: "request".into(),
        attempt_id: "attempt".into(),
        executor_id: "executor".into(),
        stopped_at_ms: 1_794_185_000_000,
    };
    let db = cache_db().await;
    let outcomes = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-left-running",
        "job-test",
        &[(plan("rust", "cargo test"), "input-left-running".to_string())],
        "tool",
        CheckExecMode::Shared,
        None,
        move |_, _, _| {
            let attempt = attempt.clone();
            async move { Err::<CheckExecResult, _>(CheckExecutionFailure::LeftRunning(attempt)) }
        },
        |_| {},
    )
    .await;
    let outcome = &outcomes[0];
    assert!(outcome.left_running.is_some());
    assert!(outcome.failure_kind.is_none());
    assert!(outcome.recorded.is_none());
    assert!(!outcome.is_genuine_failure());
    assert!(holds_no_verdict(outcome));
    assert!(outcome
        .output_tail
        .contains("while this lane ran on fedora (request request); awaiting adoption"));
    assert_eq!(
        manual_check_no_verdict(outcome).unwrap().kind,
        NO_VERDICT_LEFT_RUNNING
    );
    assert!(
        crate::execution::cache::list_check_results(db, "project-a", "tree-left-running")
            .unwrap()
            .is_empty()
    );
}

/// Every way the executor or the fleet reports a cancellation becomes the
/// one typed cancellation, carrying the cause the runner recorded.
#[test]
fn every_cancellation_shape_maps_to_a_cancellation_with_its_cause() {
    use crate::fleet::{CancelCause, CellAccount, CoalescedWaitError};
    let stopped = CellAccount {
        cancel_cause: Some(CancelCause::JobStopped),
        ..CellAccount::default()
    };
    let cancelled = || CellOutcome::Cancelled {
        request_id: "r".into(),
        attempt_id: "a".into(),
    };
    assert_eq!(
        check_result_from_cell_outcome(cancelled(), &stopped, None).unwrap_err(),
        CheckExecutionFailure::Cancelled(CancelCause::JobStopped)
    );
    assert_eq!(
        check_result_from_cell_outcome(cancelled(), &CellAccount::default(), None).unwrap_err(),
        CheckExecutionFailure::Cancelled(CancelCause::Unrecorded)
    );
    assert_eq!(
        stopped_command_failure(
            &cairn_common::executor_protocol::CommandStop::Cancelled,
            "",
            serde_json::from_value(serde_json::json!({
                "executorId": "e-1", "slotId": "slot", "cellEpoch": 1,
                "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
            }))
            .unwrap(),
            &stopped
        ),
        CheckExecutionFailure::Cancelled(CancelCause::JobStopped)
    );
    assert_eq!(
        check_result_from_cell_outcome(
            CellOutcome::Unavailable {
                reason: cairn_common::executor_protocol::CellUnavailableReason::RunnerStopping {
                    stopped_at_unix_ms: 1,
                },
                diagnostic: "stopping".into(),
            },
            &CellAccount::default(),
            None,
        )
        .unwrap_err(),
        CheckExecutionFailure::Cancelled(CancelCause::RunnerStopping)
    );
    assert_eq!(
        check_result_from_delivery(Err(CoalescedWaitError::Cancelled(CancelCause::Superseded)))
            .unwrap_err(),
        CheckExecutionFailure::Cancelled(CancelCause::Superseded)
    );
}

fn executor_unavailable_outcome() -> CellOutcome {
    CellOutcome::Unavailable {
        reason: cairn_common::executor_protocol::CellUnavailableReason::ExecutorUnavailable,
        diagnostic: "executor e-1 (generation 4): connection closed before returning a result"
            .into(),
    }
}

/// A lost link is laid at the machine that held the work, by name, and
/// only when one did. Before any claim the honest sentence is that the
/// fleet had nowhere to send it.
#[test]
fn lost_contact_is_said_only_of_a_machine_that_held_the_check() {
    let placed = crate::fleet::CellAccount {
        placement: Some(crate::fleet::CellPlacement {
            executor_id: "e-1".into(),
            generation: 4,
            name: "bglab-ub".into(),
            platform: None,
        }),
        ..crate::fleet::CellAccount::default()
    };
    let CheckExecutionFailure::Substrate(failure) =
        check_result_from_cell_outcome(executor_unavailable_outcome(), &placed, None).unwrap_err()
    else {
        panic!("a lost link is a substrate failure");
    };
    assert_eq!(failure.shape(), SubstrateFailureShape::MachineUnreachable);
    assert_eq!(failure.placement().unwrap().executor_id, "e-1");
    assert!(failure
        .agent_message()
        .starts_with("Cairn lost contact with bglab-ub, the machine running this check."));

    let CheckExecutionFailure::Substrate(unplaced) = check_result_from_cell_outcome(
        executor_unavailable_outcome(),
        &crate::fleet::CellAccount::default(),
        None,
    )
    .unwrap_err() else {
        panic!("an unplaced refusal is a substrate failure");
    };
    assert_eq!(unplaced.shape(), SubstrateFailureShape::NoReachableMachine);
    assert!(unplaced.placement().is_none());
    assert!(!unplaced.agent_message().contains("lost contact"));
}

/// The rows a lost link writes name the machine that claimed the check,
/// which is what makes the failure attributable after the fact. They carry
/// that machine's advertised platform, never the coordinator's: a Linux
/// lane requested from a Mac and lost is a Linux row (cairn/5175).
#[tokio::test]
async fn a_lost_link_records_the_machine_that_held_the_check() {
    let db = cache_db().await;
    let plans = vec![(plan("cargo-test", "cargo test"), "ih-link".to_string())];
    // A platform no coordinator runs, so the coordinator's cannot pass for it.
    let placed = crate::fleet::CellAccount {
        placement: Some(crate::fleet::CellPlacement {
            executor_id: "e-1".into(),
            generation: 4,
            name: "bglab-ub".into(),
            platform: Some(crate::fleet::ExecutorPlatform {
                os: "plan9".into(),
                arch: "mips64".into(),
            }),
        }),
        ..crate::fleet::CellAccount::default()
    };
    let result = std::sync::Mutex::new(Some(check_result_from_cell_outcome(
        executor_unavailable_outcome(),
        &placed,
        None,
    )));
    let outcomes = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-link",
        "job-a",
        &plans,
        "write-checks:run-a",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| {
            let result = result.lock().unwrap().take().unwrap();
            async move { result }
        },
        |_| {},
    )
    .await;
    assert!(outcomes[0]
        .output_tail
        .contains("lost contact with bglab-ub"));
    let recorded = db
        .query_opt_text(
            "SELECT executor_id || ':' || executor_connection_generation \
                 FROM check_result_observations LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(recorded.as_deref(), Some("e-1:4"));
    let machine = db
        .query_opt_text(
            "SELECT verdict_platform || '/' || verdict_arch || ' env=' \
                        || environment_fingerprint || ' hash=' \
                        || COALESCE(verdict_environment_hash, '-') || ' tools=' \
                        || COALESCE(toolchain_fingerprint, '-') \
                 FROM check_result_observations LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        machine.as_deref(),
        Some("plan9/mips64 env= hash=- tools=-"),
        "the claim's platform labels the row, and nothing of the coordinator's \
             environment or toolchain is recorded for a machine it is not"
    );
    let legacy = db
        .query_opt_text(
            "SELECT verdict_platform || '/' || verdict_arch FROM check_result_cache LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(legacy.as_deref(), Some("plan9/mips64"));
}

/// A lane that failed before any machine claimed it ran nowhere, so its
/// row names no platform rather than the requester's.
#[tokio::test]
async fn an_unclaimed_substrate_failure_records_no_platform() {
    let db = cache_db().await;
    let plans = vec![(plan("cargo-test", "cargo test"), "ih-unclaimed".to_string())];
    let result = std::sync::Mutex::new(Some(check_result_from_cell_outcome(
        executor_unavailable_outcome(),
        &crate::fleet::CellAccount::default(),
        None,
    )));
    run_planned_checks(
        db.clone(),
        "project-a",
        "tree-unclaimed",
        "job-a",
        &plans,
        "write-checks:run-a",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| {
            let result = result.lock().unwrap().take().unwrap();
            async move { result }
        },
        |_| {},
    )
    .await;
    let recorded = db
        .query_opt_text(
            "SELECT COALESCE(executor_id, '-') || ' ' || COALESCE(verdict_platform, '-') \
                        || '/' || COALESCE(verdict_arch, '-') \
                 FROM check_result_observations LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(recorded.as_deref(), Some("- -/-"));
}

/// A result the runner lost is a bug in Cairn, and says so; it never
/// claims a machine was lost.
#[test]
fn a_lost_coalesced_result_is_a_runner_fault_not_a_lost_machine() {
    use crate::fleet::CoalescedWaitError;
    for error in [
        CoalescedWaitError::LeaderVanished,
        CoalescedWaitError::Orphaned,
    ] {
        let CheckExecutionFailure::Substrate(failure) =
            check_result_from_delivery(Err(error.clone())).unwrap_err()
        else {
            panic!("{error:?} must be a substrate failure");
        };
        assert_eq!(failure.shape(), SubstrateFailureShape::RunnerFault);
        let message = failure.agent_message();
        assert!(message.contains("bug in Cairn"), "{message}");
        assert!(!message.contains("lost contact"), "{message}");
    }
}

#[test]
fn infrastructure_predicate_covers_operator_owned_failures() {
    assert!(CheckFailureKind::TimedOut.is_infrastructure());
    assert!(CheckFailureKind::SpawnError.is_infrastructure());
    assert!(CheckFailureKind::Infrastructure.is_infrastructure());
    assert!(CheckFailureKind::Capacity.is_infrastructure());
    assert!(CheckFailureKind::RunnerError.is_infrastructure());
    assert!(!CheckFailureKind::Killed.is_infrastructure());
}

#[test]
fn failure_kind_describe_names_each_death() {
    assert_eq!(
        CheckFailureKind::TimedOut.describe(600_000),
        "timed out after 10m"
    );
    assert_eq!(CheckFailureKind::SpawnError.describe(6), "failed to spawn");
    assert_eq!(CheckFailureKind::Killed.describe(67_000), "killed (signal)");
}

#[test]
fn failure_kind_round_trips_through_its_string() {
    for kind in [
        CheckFailureKind::TimedOut,
        CheckFailureKind::SpawnError,
        CheckFailureKind::Killed,
        CheckFailureKind::Infrastructure,
        CheckFailureKind::Capacity,
        CheckFailureKind::RunnerError,
        CheckFailureKind::NotPlaceable,
        CheckFailureKind::ConfigUnreadable,
    ] {
        assert_eq!(CheckFailureKind::from_stored(kind.as_str()), Some(kind));
    }
    assert_eq!(CheckFailureKind::from_stored("nonsense"), None);
}

#[test]
fn only_capacity_shaped_substrate_failures_gain_the_rearmable_kind() {
    let capacity = SubstrateFailure::new(SubstrateFailureShape::Capacity, "busy");
    let dispatch = SubstrateFailure::new(SubstrateFailureShape::Dispatch, "broken");
    assert_eq!(
        substrate_failure_kind(&capacity),
        CheckFailureKind::Capacity
    );
    assert_eq!(
        substrate_failure_kind(&dispatch),
        CheckFailureKind::Infrastructure
    );
}

/// String-level guard on the acceptance requirement: a timed-out check must
/// render "timed out after …", never a bare exit code, so the wording cannot
/// silently regress to a generic failure that sends an agent debugging tests
/// that never failed.
#[test]
fn summary_renders_a_timeout_as_a_timeout_not_an_exit_code() {
    let mut o = outcome("rust-full", false, None);
    o.failure_kind = Some(CheckFailureKind::TimedOut);
    o.duration_ms = 1_800_000;
    let s = format_check_summary(&[o], &HashMap::new());
    assert!(s.contains("timed out after 30m"), "got: {s}");
    assert!(!s.contains("failed to run"), "got: {s}");
    assert!(!s.contains("exit"), "got: {s}");
}

#[test]
fn summary_renders_a_spawn_error_legibly() {
    let mut o = outcome("rust-lint", false, None);
    o.failure_kind = Some(CheckFailureKind::SpawnError);
    assert_eq!(
        format_check_summary(&[o], &HashMap::new()),
        "\u{2717} rust-lint (failed to spawn)"
    );
}

// --- cache hit / miss at the runner seam ------------------------------

async fn cache_db() -> Arc<LocalDb> {
    let db = crate::storage::migrated_test_db("when-write-runner-test.db").await;
    db.execute_script(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);",
    )
    .await
    .unwrap();
    Arc::new(db)
}

fn plan(name: &str, command: &str) -> CheckPlan {
    let configured: CheckCommand =
        serde_yaml::from_str(&format!("command: {command:?}\n")).unwrap();
    CheckPlan {
        name: name.to_string(),
        applies: true,
        command: command.to_string(),
        scope: CheckScope::Full,
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
            &configured,
        ),
        verdict_executor_ids: None,
        config_error: None,
        verdict_platforms: vec![crate::execution::check_identity::gating_platform().to_string()],
        verdict_runtime: crate::execution::check_identity::expected_verdict_runtime(
            &configured,
            test_expected_toolchain(),
        ),
    }
}

/// The toolchain a test plan expects: the one a check this test process
/// runs itself records, so a test's own fresh verdict answers its own
/// later lookup the way it would in production.
fn test_expected_toolchain() -> &'static str {
    check_toolchain_identity()
}

/// The input hash every plan, `/checks` read, and gate coordinate keys by
/// does not move with the runner's host Node, and is the key main made
/// from a `rustc=…;bun=…` identity.
#[test]
fn a_check_result_key_ignores_the_hosts_node() {
    let check: CheckCommand = serde_yaml::from_str("command: bun run test:frontend\n").unwrap();
    let selector = InputSelector::from_globs(&["**".to_string()]);
    let key = |toolchain: &str| {
        check_result_key(
            &check,
            &selector,
            None,
            "tree",
            "macos-aarch64",
            toolchain,
            CheckChange::unknown("test"),
        )
    };
    let before_node = "rustc=rustc 1.97.1 (8bab26f4f 2026-07-14);bun=1.4.4";
    assert_eq!(
        key(&format!("{before_node};node=26.12.1")),
        key(before_node)
    );
    assert_eq!(
        key(&format!("{before_node};node=26.12.1")),
        key(&format!("{before_node};node=27.0.0"))
    );
    assert_ne!(
        key(before_node),
        key("rustc=rustc 1.97.1 (8bab26f4f 2026-07-14);bun=1.4.5"),
        "a keyed instrument still moves the key"
    );
}

/// A declared-variable hash as a test plan's lookup keys it for a check
/// that declares no runtime: extended by every declarable instrument at
/// [`test_expected_toolchain`]'s version.
fn test_verdict_environment(declared: &str) -> String {
    cairn_checkenv::verdict_environment_with_runtime(
        declared,
        &cairn_checkenv::runtime_versions(
            crate::config::project_settings::RuntimeInstrument::ALL
                .iter()
                .map(|instrument| instrument.as_str()),
            Some(test_expected_toolchain()),
        ),
    )
}

fn batch_item(command: &str, resource_class: CheckResourceClass) -> PlannedCheckBatchItem {
    PlannedCheckBatchItem {
        recording_plan: None,
        index: 0,
        name: "check".into(),
        input_hash: "hash".into(),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: "resource".into(),
            command: "resource".into(),
        },
        command: command.into(),
        stream_id: "stream".into(),
        env: Vec::new(),
        verdict_environment_names: Vec::new(),
        timeout_ms: 1,
        executor: None,
        verdict_platforms: Vec::new(),
        resource_class,
        infra_scopes: InfraScopes::head_only("job", "commit"),
        avoid_executor_ids: Vec::new(),
    }
}

/// A `shared` check declares one unit of concurrency, and a tool that
/// parallelizes internally is still a shared check.
///
/// This is the reversal CAIRN-3345 turns on. Cargo, vitest, and bundlers
/// used to be read out of the command text as whole-machine work, so two
/// ordinary review batches reserved 32 units on a 16-unit host, admission
/// reported `17 of 16` reserved at ~31% CPU, and five-second write-cadence
/// checks queued until their deadlines and surfaced as verdictless red
/// infrastructure failures. Opportunistic parallelism is not a requirement.
#[test]
fn check_resource_classes_never_charge_cpu_admission() {
    assert_eq!(
        declared_check_reservation(CheckResourceClass::Shared).concurrency_units,
        0
    );
    assert_eq!(
        declared_check_reservation(CheckResourceClass::Shared).source,
        ResourceReservationSource::Declared
    );
    for command in [
        "cargo test --workspace",
        "bun run check:rust",
        "bunx vitest run",
        "bun run build",
    ] {
        let items = [batch_item(command, CheckResourceClass::Shared)];
        assert_eq!(
            declared_batch_reservation(&items).concurrency_units,
            0,
            "{command} carries no CPU admission charge"
        );
    }
}

/// The legacy `exclusive` value remains parseable but cannot gate CPU.
#[test]
fn an_exclusive_check_is_cpu_admission_inert() {
    assert_eq!(
        declared_check_reservation(CheckResourceClass::Exclusive).concurrency_units,
        0
    );
}

/// Memory and disk stay unstated: they are learned per command identity from
/// observed runs, and a number written down here would replace a better
/// estimate with a worse one.
#[test]
fn a_declaration_states_concurrency_only() {
    let reservation = declared_check_reservation(CheckResourceClass::Exclusive);
    assert_eq!(reservation.memory_bytes, 0);
    assert_eq!(reservation.disk_growth_bytes, 0);
}

/// Items in a batch share one cell, so the batch is as heavy as its heaviest
/// DECLARED class — one exclusive lane cannot hide behind lighter company,
/// and a batch of heavyweight-looking shared commands stays co-runnable.
#[test]
fn a_batch_declares_its_heaviest_declared_class() {
    let light = batch_item("bun run check:migrations", CheckResourceClass::Shared);
    let parallel = batch_item("cargo clippy --workspace", CheckResourceClass::Shared);
    let exclusive = batch_item("./run-the-suite", CheckResourceClass::Exclusive);
    assert_eq!(
        declared_batch_reservation(std::slice::from_ref(&light)).concurrency_units,
        0
    );
    assert_eq!(
        declared_batch_reservation(&[light.clone(), parallel]).concurrency_units,
        0,
        "a shared batch carries no CPU admission charge"
    );
    assert_eq!(
        declared_batch_reservation(&[light, exclusive]).concurrency_units,
        0
    );
}

/// The cell's class must come from the commands it will run. Classifying the
/// batch's display string — a join of check NAMES — matched no command
/// pattern, so every batch reported `other`.
#[test]
fn a_batch_class_reads_its_commands_not_its_display_name() {
    let items = [
        batch_item("bun run check:migrations", CheckResourceClass::Shared),
        batch_item("cargo test --workspace", CheckResourceClass::Shared),
    ];
    assert_eq!(batch_command_class(&items), CellCommandClass::CargoTest);
    assert_eq!(
        CellCommandClass::classify("rust-lint · rust-full"),
        CellCommandClass::Other,
        "the display join this replaced classifies as nothing"
    );
}

/// A fake successful (or non-zero-exit) check run for the `run_planned_checks`
/// harness: a completed process that did not time out. Timeout / spawn / signal
/// cases build [`CheckExecResult`] / `Err` explicitly.
fn exec_ok(exit_code: Option<i32>, output: impl Into<String>) -> Result<CheckExecResult, String> {
    Ok(CheckExecResult {
        exit_code,
        output: output.into(),
        timed_out: false,
        duration_ms: None,
        provenance: None,
        publication: None,
    })
}

fn reusable_observation_for(
    check_name: &str,
    input_hash: &str,
    environment_fingerprint: String,
) -> FreshCheckObservationWrite {
    FreshCheckObservationWrite {
        manual_handle: None,
        publication_id: None,
        sealed_coordinate: crate::execution::cache::SealedCheckCoordinate::new(
            "commit-source",
            "commit-source",
            "tree-source",
            input_hash,
        ),
        id: format!("obs-{check_name}"),
        public_handle: format!("handle-{check_name}"),
        infra_reset_scopes: Vec::new(),
        nested_sandbox_lane: None,
        failure_signature: None,
        placement_facts: None,
        substrate_shape: None,
        selection: None,
        deferred_count: 0,
        plan_audit_state: None,
        plan_audit_note: None,
        audit_findings: Vec::new(),
        phases: None,
        execution_warmth: None,
        cell_origin: None,
        build_jobs: None,
        project_id: "project-a".to_string(),
        commit_sha: "commit-source".to_string(),
        defined_by_commit_sha: "commit-source".to_string(),
        tree_hash: "tree-source".to_string(),
        check_name: check_name.to_string(),
        input_hash: input_hash.to_string(),
        environment_fingerprint,
        exit_code: 0,
        verdict: "passed".to_string(),
        failure_kind: None,
        complete: true,
        reusable: true,
        non_reusable_reason: None,
        parser_version: crate::execution::check_identity::CHECK_PARSER_VERSION as i64,
        result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        ran_at: 100,
        duration_ms: 1,
        job_id: Some("job-a".to_string()),
        run_id: None,
        cadence: "write".to_string(),
        executor_id: None,
        executor_device_id: None,
        executor_connection_generation: None,
        executor_cell_id: None,
        executor_lease_epoch: None,
        executor_started_at_unix_ms: None,
        executor_finished_at_unix_ms: None,
        runner_build_id: Some(cairn_checkenv::implementation_identity().to_string()),
        toolchain_fingerprint: Some(test_expected_toolchain().to_string()),
        output_tail: "cached".to_string(),
        target_results_json: None,
        tests: Vec::new(),
        verdict_platform: Some(crate::execution::check_identity::gating_platform().to_string()),
        verdict_arch: Some(std::env::consts::ARCH.to_string()),
        verdict_environment_hash: Some(test_verdict_environment(
            &cairn_checkenv::verdict_environment_hash(Vec::<String>::new(), |_| None),
        )),
    }
}

fn reusable_observation(environment_fingerprint: String) -> FreshCheckObservationWrite {
    reusable_observation_for("frontend", "ih-frontend", environment_fingerprint)
}

/// A check run reported by a REMOTE executor — what an enrolled machine
/// returns when a spill-eligible suite lands on it. An executor that
/// reports its environment reports its platform with it, as `cairn-exec`
/// computes both in one place; an empty fingerprint models a legacy
/// executor that reports neither.
fn exec_on_executor(
    exit_code: Option<i32>,
    output: impl Into<String>,
    executor_id: &str,
    environment_fingerprint: impl Into<String>,
) -> Result<CheckExecResult, String> {
    let environment_fingerprint = environment_fingerprint.into();
    let reports = !environment_fingerprint.is_empty();
    Ok(CheckExecResult {
        exit_code,
        output: output.into(),
        timed_out: false,
        duration_ms: Some(1),
        provenance: Some(cairn_common::executor_protocol::CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: executor_id.to_string(),
            executor_device_id: "device-remote".to_string(),
            executor_connection_generation: 1,
            cell_id: "slot-1".to_string(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: Some(1),
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint,
            verdict_platform: reports
                .then(|| crate::execution::check_identity::gating_platform().to_string()),
            verdict_arch: reports.then(|| std::env::consts::ARCH.to_string()),
            verdict_environment_hash: reports.then(|| {
                cairn_checkenv::verdict_environment_hash(Vec::<String>::new(), |name| {
                    std::env::var(name).ok()
                })
            }),
            sandbox: None,
            toolchain_fingerprint: reports.then(|| test_expected_toolchain().to_string()),
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        }),
        publication: None,
    })
}

/// The specimen behind CAIRN-3413: a manual check that spilled to another
/// machine.
///
/// Its observation is recorded under an EMPTY environment fingerprint,
/// because Cairn cannot identify a remote machine's verdict environment and
/// must not publish its row under a coordinator-derived key. Both halves are
/// pinned here: the coordinator's key still resolves nothing (reuse stays
/// exactly as closed as CAIRN-3328 left it), and the caller still gets its
/// verdict and the id of the row that was written.
#[tokio::test]
async fn a_remotely_executed_check_returns_the_observation_it_recorded() {
    let db = cache_db().await;
    let plans = vec![(
        plan("rust-tests", "bunx tsc --noEmit"),
        "ih-remote".to_string(),
    )];
    let results = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:rust-tests",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| async {
            exec_on_executor(Some(0), "remote green", "bglab-ub", "")
        },
        |_| {},
    )
    .await;

    let recorded = results[0]
        .recorded
        .clone()
        .expect("a remote run records the observation it wrote");
    assert!(
        recorded.environment_fingerprint.is_empty(),
        "a remote machine's verdict environment is unidentified"
    );
    assert!(
        !recorded.reusable,
        "a remote verdict must never suppress a later execution"
    );

    let schema = crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;
    assert!(
        crate::execution::cache::get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-target",
            "rust-tests",
            &plan_environment_fingerprint(&plans[0].0),
            schema,
            None,
            None,
            0,
            0,
        )
        .unwrap()
        .is_none(),
        "the requesting side's key must not resolve a remote row — which is why \
             the reply cannot be derived from a second lookup"
    );
    assert_eq!(
        crate::execution::cache::get_check_result_observation(
            db,
            "project-a",
            "commit-target",
            "rust-tests",
            "",
            schema,
            None,
            None,
            0,
            0,
        )
        .unwrap()
        .expect("the row exists under the identity it was recorded with")
        .observation_id,
        recorded.id,
        "the run recorded exactly the observation it reported"
    );

    let reply = manual_configured_check_result(
        "rust-tests",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-remote".to_string(),
        Some("bunx tsc --noEmit"),
        results.into_iter().next().unwrap(),
    );
    assert!(reply.passed, "a remote green reaches the caller as a green");
    assert_eq!(reply.observation_id.as_deref(), Some(recorded.id.as_str()));
    assert_eq!(reply.disposition, "fresh");
    assert!(!reply.reusable);
    assert!(reply.environment_fingerprint.is_empty());
    assert_eq!(
        reply.not_recorded.as_deref(),
        Some("the selected legacy executor did not report a complete verdict environment identity")
    );
    assert!(reply.no_verdict.is_none());
}

#[tokio::test]
async fn matching_remote_environment_is_reused_without_running_again() {
    let db = cache_db().await;
    let local_environment = current_check_environment_fingerprint();
    let executor_environment = cairn_checkenv::fingerprint(
        std::env::consts::OS,
        std::env::consts::ARCH,
        [cairn_checkenv::keyed_toolchain_identity().to_string()],
        Some(cairn_checkenv::implementation_identity()),
        Vec::<String>::new(),
        |_| None,
    );
    assert_eq!(
        executor_environment, local_environment,
        "runner and executor composition must agree without sharing executable byte identity"
    );
    let plans = vec![(
        plan("frontend", "bunx tsc --noEmit"),
        "ih-matching-remote".to_string(),
    )];
    let first = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:frontend",
        CheckExecMode::Shared,
        None,
        {
            let environment = executor_environment.clone();
            move |_, _, _| {
                let environment = environment.clone();
                async move { exec_on_executor(Some(0), "remote green", "bglab-ub", environment) }
            }
        },
        |_| {},
    )
    .await;
    let source = first[0]
        .recorded
        .as_ref()
        .expect("remote green must record its observation");
    assert_eq!(source.environment_fingerprint, executor_environment);
    assert!(source.reusable);

    let second = run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-next",
            defined_by: "commit-next",
        },
        "tree-next",
        "job-b",
        &plans,
        "manual-check:run-b:frontend",
        CheckExecMode::Shared,
        None,
        |_, _, _| async {
            panic!("a matching remote observation must answer from cache");
            #[allow(unreachable_code)]
            exec_on_executor(Some(1), "should not run", "local", "")
        },
        |_| {},
    )
    .await;
    assert!(second[0].cached);
    assert!(second[0].passed);
    assert_eq!(
        second[0]
            .recorded
            .as_ref()
            .map(|recorded| recorded.id.as_str()),
        Some(source.id.as_str()),
        "the cache hit must preserve the source observation identity"
    );
}

/// A local run is unchanged: the recorded identity carries this machine's
/// fingerprint, the coordinator's own key resolves it, and it stays reusable.
#[tokio::test]
async fn a_local_verdict_reports_a_reusable_observation_under_this_machine() {
    let db = cache_db().await;
    let plans = vec![(
        plan("frontend", "bunx tsc --noEmit"),
        "ih-local".to_string(),
    )];
    let results = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:frontend",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| async { exec_ok(Some(0), "ok") },
        |_| {},
    )
    .await;

    let recorded = results[0].recorded.clone().expect("a local run records");
    assert_eq!(
        recorded.environment_fingerprint,
        plan_environment_fingerprint(&plans[0].0)
    );
    assert!(recorded.reusable);
    assert_eq!(
        crate::execution::cache::get_check_result_observation(
            db,
            "project-a",
            "commit-target",
            "frontend",
            &recorded.environment_fingerprint,
            crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            None,
            None,
            0,
            0,
        )
        .unwrap()
        .expect("a local row is addressable by this machine's key")
        .observation_id,
        recorded.id
    );

    let reply = manual_configured_check_result(
        "frontend",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-local".to_string(),
        Some("bunx tsc --noEmit"),
        results.into_iter().next().unwrap(),
    );
    assert!(reply.passed && reply.reusable);
    assert_eq!(reply.disposition, "fresh");
    assert!(reply.no_verdict.is_none());
}

// --- rust-lint: the stored record carries the findings (cairn/4888) -----

fn compact_json(json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(json)
        .unwrap()
        .to_string()
}

fn rust_lint_json(complete: bool) -> String {
    format!(
        r#"{{"schemaVersion":1,"parser":"rust-lint","complete":{complete},"selection":"unknown","tests":[],"undeclaredSkips":0,"failures":[],"report":"✓ rustfmt: no formatting drift\n✓ clippy: no findings in files your diff changed"}}"#
    )
}

/// The executor's body for the cairn/4820 red: the script's report on
/// stdout, minutes of compile chatter on stderr after it, the exit code, and
/// (when the script wrote one) the result-file line the executor appends.
fn rust_lint_specimen_body(result_json: Option<&str>) -> String {
    let stdout = "\
ℹ️  scoped lint: 3 crate(s) — cairn-core, cairn-exec, cairn-app

❌ rustfmt: formatting drift — run `bun run fmt` to fix.
Diff in /slots/slot-10/src-tauri/os/cairn-core/src/execution/checks_turn_end.rs:2805:
❌ clippy: 1 finding(s) in files your diff changed (lint every file you touch)
warning: this call to `clone` can be replaced with `std::slice::from_ref`
    --> os/cairn-core/src/execution/checks_turn_end.rs:2811:20
  ℹ 15 finding(s) in files your diff did not change (not gating)";
    let mut stderr = String::new();
    let mut crate_index = 0;
    while stderr.len() < 20_000 {
        stderr.push_str(&format!("    Checking crate-{crate_index} v0.1.0\n"));
        crate_index += 1;
    }
    stderr.push_str(
        "    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3m 19s\n\
             warning: the following packages contain code that will be rejected by a future \
             version of Rust: proc-macro-error2 v2.0.1\n\
             note: to see what the problems were, use the option `--future-incompat-report`\n\
             error: script \"check:rust\" exited with code 1",
    );
    let mut body = format!("{stdout}\n\nstderr:\n{stderr}\n\nExit code: 1");
    if let Some(json) = result_json {
        body.push('\n');
        body.push_str(RESULT_ARTIFACT_JSON_MARKER);
        body.push_str(json);
    }
    body
}

async fn run_rust_lint(
    db: Arc<LocalDb>,
    input_hash: &str,
    exit_code: i32,
    body: String,
) -> CheckOutcome {
    let plans = vec![(
        plan("rust-lint", "bun run check:rust"),
        input_hash.to_string(),
    )];
    run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:rust-lint",
        CheckExecMode::Shared,
        None,
        move |_, _, _| {
            let body = body.clone();
            async move { exec_ok(Some(exit_code), body) }
        },
        |_| {},
    )
    .await
    .into_iter()
    .next()
    .unwrap()
}

fn observation_of(
    db: Arc<LocalDb>,
    outcome: &CheckOutcome,
) -> crate::execution::cache::CheckResultObservationProjection {
    let recorded = outcome
        .recorded
        .as_ref()
        .expect("the run records its observation");
    crate::execution::cache::get_check_result_observation(
        db,
        "project-a",
        "commit-target",
        "rust-lint",
        &recorded.environment_fingerprint,
        crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        None,
        None,
        0,
        0,
    )
    .unwrap()
    .expect("the observation is addressable")
}

/// Required #2 of cairn/4888. Twenty thousand characters of stderr chatter
/// sit after the script's stdout report, so no tail window holds the
/// findings; the stored record must carry them anyway.
#[tokio::test]
async fn a_rust_lint_red_stores_its_findings_ahead_of_the_compile_chatter() {
    let db = cache_db().await;
    let fixture = compact_json(include_str!("../fixtures/rust_lint_result.json"));
    let outcome = run_rust_lint(
        db.clone(),
        "ih-lint-red",
        1,
        rust_lint_specimen_body(Some(&fixture)),
    )
    .await;
    assert!(!outcome.passed);
    assert_eq!(outcome.failure_kind, None, "an ordinary lint red");

    let observation = observation_of(db.clone(), &outcome);
    for text in [
        outcome.output_tail.as_str(),
        observation.output_tail.as_str(),
    ] {
        assert!(
            text.contains(
                "❌ clippy: 1 finding(s) in files your diff changed (lint every file you touch)"
            ),
            "{text}"
        );
        assert!(text.contains("checks_turn_end.rs:2811"), "{text}");
        assert!(
            text.contains("15 finding(s) in files your diff did not change"),
            "{text}"
        );
        assert!(
            text.find("❌ clippy").unwrap() < text.find("Final output:").unwrap(),
            "the verdict leads the chatter: {text}"
        );
        assert!(text.contains("error: script \"check:rust\" exited with code 1"));
        assert!(
            text.ends_with("Exit code: 1"),
            "the tail still ends the record"
        );
        assert!(text.chars().count() <= OUTPUT_TAIL_CHARS);
        assert!(!text.contains(RESULT_ARTIFACT_JSON_MARKER), "{text}");
    }
    assert!(observation.complete);

    let row = crate::execution::cache::list_check_results(db, "project-a", "tree-target")
        .unwrap()
        .into_iter()
        .find(|row| row.check_name == "rust-lint")
        .expect("the verdict is recorded for the checks surface");
    let stored: ParsedCheckResult =
        serde_json::from_str(row.target_results_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        stored.failures[0].name,
        "src-tauri/os/cairn-core/src/execution/checks_turn_end.rs:2811 clippy::cloned_ref_to_slice_refs"
    );
    let summary = format_check_summary(std::slice::from_ref(&outcome), &HashMap::new());
    assert!(
        summary.contains(
            "✗ rust-lint — 2 failed: src-tauri/os/cairn-core/src/execution/checks_turn_end.rs:2811 clippy::cloned_ref_to_slice_refs"
        ),
        "{summary}"
    );
    assert!(
        summary.contains(
            "```\n❌ clippy: 1 finding(s) in files your diff changed (lint every file you touch)"
        ),
        "the excerpt is the lint's own report: {summary}"
    );
    assert!(
        summary.contains("15 finding(s) in files your diff did not change"),
        "{summary}"
    );
    assert!(
        !summary.contains("Checking crate-"),
        "the chatter after the report stays out of the inline summary: {summary}"
    );
}

/// The same chatter with no result file, the record cairn/4888 reported:
/// a plain tail loses the stdout findings, and the stream-bounded tail
/// keeps them without needing a report.
#[tokio::test]
async fn without_a_result_file_the_stored_record_still_holds_the_stdout_findings() {
    let db = cache_db().await;
    let body = rust_lint_specimen_body(None);
    assert!(!tail(&body, OUTPUT_TAIL_CHARS).contains("❌ clippy"));
    let outcome = run_rust_lint(db, "ih-lint-plain", 1, body.clone()).await;
    assert!(outcome.parsed.is_none());
    assert!(
        outcome
            .output_tail
            .contains("❌ clippy: 1 finding(s) in files your diff changed"),
        "{}",
        outcome.output_tail
    );
    assert!(outcome.output_tail.ends_with("Exit code: 1"));
    assert!(outcome.output_tail.chars().count() <= OUTPUT_TAIL_CHARS);
}

// --- the stored record keeps a failure on either stream (cairn/6321) -----

/// libtest's report for cairn/6064's red, which libtest writes to stdout.
const LIBTEST_FAILURE_BLOCK: &str = "\
failures:

---- claude_session_survives_a_resume stdout ----
thread 'claude_session_survives_a_resume' panicked at tests/sessions.rs:120:5:
assertion `left == right` failed: the resumed session keeps its id

failures:
    claude_session_survives_a_resume
    claude_session_reports_its_model

test result: FAILED. 9 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out";

/// More than the window of cargo's progress lines.
fn cargo_chatter() -> String {
    let mut chatter = String::new();
    let mut index = 0;
    while chatter.len() <= 2 * OUTPUT_TAIL_CHARS {
        chatter.push_str(&format!("   Compiling crate-{index} v0.1.0\n"));
        index += 1;
    }
    chatter.push_str("     Running tests/sessions.rs (target/debug/deps/sessions-0123)\n");
    chatter
}

async fn run_external_host(db: Arc<LocalDb>, input_hash: &str, body: String) -> CheckOutcome {
    let plans = vec![(
        plan("external-host", "bun run check:external-host"),
        input_hash.to_string(),
    )];
    run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:external-host",
        CheckExecMode::Shared,
        None,
        move |_, _, _| {
            let body = body.clone();
            async move { exec_ok(Some(1), body) }
        },
        |_| {},
    )
    .await
    .into_iter()
    .next()
    .unwrap()
}

fn stored_tail(db: Arc<LocalDb>, outcome: &CheckOutcome) -> String {
    let recorded = outcome
        .recorded
        .as_ref()
        .expect("the run records its observation");
    crate::execution::cache::get_check_result_observation(
        db,
        "project-a",
        "commit-target",
        "external-host",
        &recorded.environment_fingerprint,
        crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        None,
        None,
        0,
        0,
    )
    .unwrap()
    .expect("the observation is addressable")
    .output_tail
}

/// The external-host shape: libtest's failure block on stdout early, then
/// more than the window of cargo chatter on stderr, then a nonzero exit.
/// Every stored red ended at `error: test failed` and named no test.
#[tokio::test]
async fn a_stdout_failure_block_survives_the_stderr_chatter_after_it() {
    let db = cache_db().await;
    let body = format!(
        "{LIBTEST_FAILURE_BLOCK}\n\nstderr:\n{}\
             error: test failed, to rerun pass `--test sessions`\n\
             check:external-host: tests failed\n\
             error: script \"check:external-host\" exited with code 1\n\nExit code: 1",
        cargo_chatter()
    );
    assert!(
        !crate::execution::check_parsers::anchored_tail(&body, OUTPUT_TAIL_CHARS)
            .contains("panicked"),
        "the single-window tail loses the failure block, which is the defect"
    );
    let outcome = run_external_host(db.clone(), "ih-host-stdout", body).await;
    assert!(!outcome.passed);
    let stored = stored_tail(db, &outcome);
    for text in [outcome.output_tail.as_str(), stored.as_str()] {
        assert!(text.contains(LIBTEST_FAILURE_BLOCK), "{text}");
        assert!(text.contains("`--test sessions`"), "{text}");
        assert!(text.ends_with("Exit code: 1"), "{text}");
        assert!(text.chars().count() <= OUTPUT_TAIL_CHARS);
    }
}

/// The reverse: a diagnostic on stderr after more than the window of
/// progress on stdout.
#[tokio::test]
async fn a_stderr_failure_survives_the_stdout_chatter_before_it() {
    let db = cache_db().await;
    let failure = "error: the mirror's manifest names an unpublished crate: cairn-exec\n\
                       check:external-host: assemble the public mirror failed";
    let body = format!("{}\n\nstderr:\n{failure}\n\nExit code: 1", cargo_chatter());
    let outcome = run_external_host(db.clone(), "ih-host-stderr", body).await;
    assert!(!outcome.passed);
    let stored = stored_tail(db, &outcome);
    for text in [outcome.output_tail.as_str(), stored.as_str()] {
        assert!(text.contains(failure), "{text}");
        assert!(text.contains("Running tests/sessions.rs"), "{text}");
        assert!(text.chars().count() <= OUTPUT_TAIL_CHARS);
    }
}

#[tokio::test]
async fn an_exit_code_only_manual_pass_cites_a_readable_public_observation() {
    let db = cache_db().await;
    let outcome = run_rust_lint(db.clone(), "ih-script-pass", 0, "validator passed".into()).await;
    assert!(outcome.parsed.is_none());
    let reply = manual_configured_check_result(
        "rust-lint",
        "project-a",
        "commit-target".into(),
        "tree-target".into(),
        "ih-script-pass".into(),
        Some("bun run check:rust"),
        outcome,
    );
    let reference = reply.observation_ref.as_deref().expect("a public citation");
    let handle = reference.rsplit('/').next().unwrap();
    assert_eq!(handle.len(), 24, "{reference}");
    assert!(handle.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let stored =
        crate::execution::cache::get_check_result_observation_by_handle(db, "project-a", handle)
            .unwrap()
            .expect("the printed citation reads back from the ledger");
    assert_eq!(stored.verdict, "passed");
    assert_eq!(stored.exit_code, 0);
    assert!(reply.passed && reply.verified, "{reply:?}");
    assert_eq!(reply.unverified_reason, None);
}

#[tokio::test]
async fn a_vite_build_without_test_counts_is_a_verified_manual_pass() {
    let db = cache_db().await;
    let plans = vec![(
        plan("frontend-build", "bunx --bun vite build"),
        "ih-vite".into(),
    )];
    let outcome = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit { evaluated: "commit-target", defined_by: "commit-target" },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:frontend-build",
        CheckExecMode::Shared,
        None,
        |_, _, _| async { exec_ok(Some(0), "vite v7.0.0 building for production...\n✓ 2048 modules transformed.\n✓ built in 23.27s") },
        |_| {},
    )
    .await
    .into_iter()
    .next()
    .unwrap();
    assert!(outcome.parsed.is_none());
    let reply = manual_configured_check_result(
        "frontend-build",
        "project-a",
        "commit-target".into(),
        "tree-target".into(),
        "ih-vite".into(),
        Some("bunx --bun vite build"),
        outcome,
    );
    assert!(
        reply.passed && reply.verified && reply.reusable,
        "{reply:?}"
    );
    let reference = reply
        .observation_ref
        .as_deref()
        .expect("the build has a public citation");
    let handle = reference.rsplit('/').next().unwrap();
    let stored =
        crate::execution::cache::get_check_result_observation_by_handle(db, "project-a", handle)
            .unwrap()
            .expect("the citation reads back");
    assert_eq!(stored.check_name, "frontend-build");
    assert_eq!(stored.verdict, "passed");
    assert_eq!(stored.exit_code, 0);
    assert!(stored.complete && stored.reusable);
}

/// One exit-zero manual run of `command` whose output no parser reads.
async fn run_parse_less_green(
    db: Arc<LocalDb>,
    name: &str,
    command: &str,
    input_hash: &str,
) -> CheckOutcome {
    let plans = vec![(plan(name, command), input_hash.to_string())];
    run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        &format!("manual-check:run-a:{name}"),
        CheckExecMode::Shared,
        None,
        |_, _, _| async { exec_ok(Some(0), "the runner exited before its summary") },
        |_| {},
    )
    .await
    .into_iter()
    .next()
    .unwrap()
}

/// A test runner that exits zero without a summary any parser reads has not
/// shown that a test ran. Its green stays unverified and unreusable, while
/// the observation it recorded stays readable at its public citation.
#[tokio::test]
async fn a_parse_less_test_runner_exit_zero_stays_an_unverified_manual_pass() {
    let db = cache_db().await;
    for (name, command) in [
        ("rust-tests", "bun run test:rust"),
        ("frontend-tests", "bun run test:frontend"),
        ("bun-tests", "bun test"),
    ] {
        let input_hash = format!("ih-parse-less-{name}");
        let outcome = run_parse_less_green(db.clone(), name, command, &input_hash).await;
        assert!(outcome.passed && outcome.parsed.is_none(), "{command}");
        let reply = manual_configured_check_result(
            name,
            "project-a",
            "commit-target".into(),
            "tree-target".into(),
            input_hash.clone(),
            Some(command),
            outcome.clone(),
        );
        // A request recorded before its command was kept recovers with
        // none, and must not be promoted to verified by the omission.
        let legacy = manual_configured_check_result(
            name,
            "project-a",
            "commit-target".into(),
            "tree-target".into(),
            input_hash,
            None,
            outcome,
        );
        assert_eq!(
            (legacy.verified, &legacy.unverified_reason),
            (reply.verified, &reply.unverified_reason),
            "{command}"
        );
        assert!(reply.passed && !reply.verified, "{command}: {reply:?}");
        assert_eq!(
            reply.unverified_reason.as_deref(),
            Some("the command produced no structured coverage result"),
            "{command}"
        );
        assert!(!reply.reusable, "{command}: {reply:?}");
        let reference = reply.observation_ref.as_deref().expect("a public citation");
        let stored = crate::execution::cache::get_check_result_observation_by_handle(
            db.clone(),
            "project-a",
            reference.rsplit('/').next().unwrap(),
        )
        .unwrap()
        .expect("the citation reads back");
        assert_eq!(stored.verdict, "passed", "{command}");
        assert_eq!(stored.exit_code, 0, "{command}");
        assert!(!stored.reusable, "{command}");
    }
    // The same parse-less green from a command that is not a test runner
    // is a complete answer, as the merge gates read it: a script whose
    // first word is `test` is a shell test, not a test runner.
    for (name, command) in [
        ("frontend-build", "bunx --bun vite build"),
        ("workspace-compile", "cargo check --workspace --locked"),
        ("rust-lint", "bun run check:rust"),
        ("smoke", "test -f README.md"),
    ] {
        let input_hash = format!("ih-parse-less-{name}");
        let outcome = run_parse_less_green(db.clone(), name, command, &input_hash).await;
        assert!(outcome.parsed.is_none(), "{command}");
        let reply = manual_configured_check_result(
            name,
            "project-a",
            "commit-target".into(),
            "tree-target".into(),
            input_hash,
            Some(command),
            outcome,
        );
        assert!(
            reply.passed && reply.verified && reply.reusable,
            "{command}: {reply:?}"
        );
        assert_eq!(reply.unverified_reason, None, "{command}");
    }
}

#[tokio::test]
async fn a_complete_rust_lint_pass_is_a_verified_manual_verdict() {
    let db = cache_db().await;
    let body = format!(
        "✓ clippy: no findings in files your diff changed\n{RESULT_ARTIFACT_JSON_MARKER}{}",
        rust_lint_json(true)
    );
    let outcome = run_rust_lint(db.clone(), "ih-lint-pass", 0, body).await;
    let observation = observation_of(db, &outcome);
    assert!(observation.complete && observation.reusable);
    let reply = manual_configured_check_result(
        "rust-lint",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-lint-pass".to_string(),
        Some("bun run check:rust"),
        outcome,
    );
    assert!(
        reply.passed && reply.verified,
        "{:?}",
        reply.unverified_reason
    );
    assert_eq!(reply.unverified_reason, None);
}

#[tokio::test]
async fn an_incomplete_rust_lint_exit_zero_is_neither_verified_nor_reusable() {
    let db = cache_db().await;
    let body = format!(
        "ℹ️  nothing to lint.\n{RESULT_ARTIFACT_JSON_MARKER}{}",
        rust_lint_json(false)
    );
    let outcome = run_rust_lint(db.clone(), "ih-lint-empty", 0, body).await;
    assert!(outcome.passed, "the exit code is still the verdict");
    // The live board publishes this annotation, and the inline summary marks
    // the lane as unverified rather than green.
    assert_eq!(
        summary_annotation(&outcome).as_deref(),
        Some("unverified — the lint did not run to completion")
    );
    let summary = format_check_summary(std::slice::from_ref(&outcome), &HashMap::new());
    assert!(
        summary.starts_with("⚠ rust-lint (unverified — the lint did not run to completion"),
        "{summary}"
    );
    let observation = observation_of(db, &outcome);
    assert!(
        !observation.complete,
        "a run that measured nothing is not complete"
    );
    assert!(!observation.reusable);
    let reply = manual_configured_check_result(
        "rust-lint",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-lint-empty".to_string(),
        Some("bun run check:rust"),
        outcome,
    );
    assert!(reply.passed && !reply.verified);
    assert_eq!(
        reply.unverified_reason.as_deref(),
        Some("the lint did not run to completion")
    );
}

/// A lint result names sites, but it must not pre-empt classification: a
/// compile failure caused by sccache corruption still has to reach the
/// uncached retry.
#[test]
fn a_rust_lint_red_with_named_sites_still_classifies_cache_corruption() {
    let result = r#"{"schemaVersion":1,"parser":"rust-lint","complete":true,"failures":[{"name":"src-tauri/src/lib.rs:3 E0786","message":"found invalid metadata files"}]}"#;
    let output = format!(
        "error[E0786]: found invalid metadata files for crate `serde`\n\
             = note: failed to mmap file: memory map must have a non-zero length\n\
             Exit code: 1\n{RESULT_ARTIFACT_JSON_MARKER}{result}"
    );
    let parsed = parse_check_output("bun run check:rust", &output).unwrap();
    assert_eq!(parsed.failed, 1);
    let classification = classify_check_failure(
        "bun run check:rust",
        Some(1),
        false,
        false,
        Some(&parsed),
        &output,
    )
    .expect("cache corruption is still classified");
    assert!(classification.cache_corruption);
}

/// Cairn's own machinery failing is not a result about the tree. It renders
/// as the named substrate cause, never as a red and never as a complaint
/// about recording.
#[tokio::test]
async fn an_infrastructure_failure_reports_its_substrate_cause() {
    let db = cache_db().await;
    let plans = vec![(
        plan("rust-tests", "bunx tsc --noEmit"),
        "ih-infra".to_string(),
    )];
    let results = run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:rust-tests",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| async {
            Err::<CheckExecResult, CheckExecutionFailure>(CheckExecutionFailure::substrate(
                SubstrateFailureShape::MachineUnreachable,
                "cell link dropped mid-execution",
            ))
        },
        |_| {},
    )
    .await;

    let outcome = results.into_iter().next().unwrap();
    assert_eq!(outcome.failure_kind, Some(CheckFailureKind::Infrastructure));
    let reply = manual_configured_check_result(
        "rust-tests",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-infra".to_string(),
        Some("bunx tsc --noEmit"),
        outcome,
    );
    let no_verdict = reply
        .no_verdict
        .expect("an infrastructure failure produced no verdict");
    assert_eq!(no_verdict.kind, "infrastructure");
    assert_eq!(no_verdict.after_failures, None);
    assert!(
        no_verdict.cause.contains("lost contact with the machine"),
        "the reader gets the named cause: {}",
        no_verdict.cause
    );
    assert!(
        no_verdict.cause.contains("cell link dropped"),
        "the reader gets the concrete failing step: {}",
        no_verdict.cause
    );
}

/// The cairn/4966 specimen: a write check queued behind a busy machine when
/// the runner restarted. It is Cairn ending the check because the runner
/// stopped — not a lost machine, not a missing one, and not a substrate
/// failure — so it is reported as a cancellation with its cause and
/// nothing is recorded: no observation, no cache row, no streak.
#[tokio::test]
async fn a_runner_stop_is_a_cancellation_that_records_nothing() {
    let db = cache_db().await;
    let observed_db = db.clone();
    let plans = vec![(plan("cargo-test", "cargo test"), "ih-stop".to_string())];
    let mut results = HashMap::from([(
        0,
        check_result_from_cell_outcome(
            CellOutcome::Unavailable {
                reason: cairn_common::executor_protocol::CellUnavailableReason::RunnerStopping {
                    stopped_at_unix_ms: 1,
                },
                diagnostic:
                    "this runner began shutting down while this request was waiting to be placed"
                        .into(),
            },
            &crate::fleet::CellAccount::default(),
            None,
        ),
    )]);
    stamp_substrate_elapsed(&mut results, std::time::Duration::from_secs(190));
    let batched = Arc::new(std::sync::Mutex::new(results));
    let outcomes = run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "write-checks:run-a",
        CheckExecMode::Shared,
        None,
        move |index, _command, _stream_id| {
            let batched = batched.clone();
            async move { batched.lock().unwrap().remove(&index).unwrap() }
        },
        |_| {},
    )
    .await;

    let outcome = outcomes.into_iter().next().unwrap();
    assert_eq!(outcome.failure_kind, Some(CheckFailureKind::Cancelled));
    assert_eq!(
        outcome.not_recorded.as_deref(),
        Some("cancelled: Cairn's runner is stopping")
    );
    assert!(
        outcome.output_tail.contains("Cairn cancelled this check"),
        "{}",
        outcome.output_tail
    );
    assert!(!outcome.output_tail.contains("lost contact"));
    assert!(!outcome.output_tail.contains("No machine available"));
    assert!(!outcome.is_genuine_failure());
    assert_eq!(cancelled_rows(&observed_db).await, (0, 0));
}

/// Rows a cancellation must never write: observations, and cache rows.
async fn cancelled_rows(db: &Arc<LocalDb>) -> (i64, i64) {
    let observations = db
        .query_opt_text(
            "SELECT CAST(count(*) AS TEXT) FROM check_result_observations",
            (),
        )
        .await
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    let cache_rows = db
        .query_opt_text("SELECT CAST(count(*) AS TEXT) FROM check_result_cache", ())
        .await
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    (observations, cache_rows)
}

/// A check Cairn declined to run says so, and an ordinary red stays an
/// ordinary red rather than being laundered into a no-verdict.
#[test]
fn a_suppressed_check_reports_that_cairn_declined_to_run_it() {
    let suppressed = CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: "rust-tests".to_string(),
        passed: false,
        exit_code: None,
        failure_kind: Some(CheckFailureKind::Infrastructure),
        parsed: None,
        output_tail: "the last failure was a spawn error".to_string(),
        cached: false,
        duration_ms: 0,
        suppressed_after: Some(3),
        recorded: None,
        not_recorded: None,
    };
    let reply = manual_configured_check_result(
        "rust-tests",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-suppressed".to_string(),
        Some("bun run test:rust"),
        suppressed,
    );
    let no_verdict = reply.no_verdict.expect("a suppressed check has no verdict");
    assert_eq!(no_verdict.kind, "suppressed");
    assert_eq!(no_verdict.after_failures, Some(3));
    assert!(
        reply.observation_id.is_none(),
        "nothing ran, so nothing was observed"
    );

    let red = CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: "rust-tests".to_string(),
        passed: false,
        exit_code: Some(1),
        failure_kind: None,
        parsed: None,
        output_tail: "2 tests failed".to_string(),
        cached: false,
        duration_ms: 10,
        suppressed_after: None,
        recorded: Some(crate::execution::cache::RecordedCheckObservation {
            id: "obs-red".to_string(),
            public_handle: "111111111111111111111111".into(),
            ran_at: 1_754_246_320_000,
            environment_fingerprint: "env-local".to_string(),
            executor_id: None,
            reusable: false,
        }),
        not_recorded: None,
    };
    let reply = manual_configured_check_result(
        "rust-tests",
        "cairn",
        "commit-target".to_string(),
        "tree-target".to_string(),
        "ih-red".to_string(),
        Some("bun run test:rust"),
        red,
    );
    assert!(
        reply.no_verdict.is_none(),
        "a failing check IS a verdict about the tree"
    );
    assert_eq!(reply.observation_id.as_deref(), Some("obs-red"));
}

#[test]
fn repeated_observations_at_one_coordinate_get_distinct_public_permalinks() {
    let make = |id: uuid::Uuid, public_handle: &str, ran_at| CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: "rust-tests".into(),
        passed: true,
        exit_code: Some(0),
        failure_kind: None,
        parsed: None,
        output_tail: String::new(),
        cached: false,
        duration_ms: 1,
        suppressed_after: None,
        recorded: Some(crate::execution::cache::RecordedCheckObservation {
            id: id.to_string(),
            public_handle: public_handle.into(),
            ran_at,
            environment_fingerprint: "env".into(),
            executor_id: None,
            reusable: true,
        }),
        not_recorded: None,
    };
    let first_id = uuid::Uuid::new_v4();
    let second_id = uuid::Uuid::new_v4();
    let first = manual_configured_check_result(
        "rust-tests",
        "cairn",
        "same-commit".into(),
        "tree".into(),
        "input".into(),
        Some("bun run test:rust"),
        make(first_id, "111111111111111111111111", 1_754_246_320_000),
    );
    let second = manual_configured_check_result(
        "rust-tests",
        "cairn",
        "same-commit".into(),
        "tree".into(),
        "input".into(),
        Some("bun run test:rust"),
        make(second_id, "222222222222222222222222", 1_754_246_321_000),
    );
    assert_ne!(first.observation_ref, second.observation_ref);
    assert!(
        first
            .ran_at
            .as_deref()
            .is_some_and(|stamp| stamp.starts_with("2025-")),
        "the observation identity must interpret ran_at as Unix milliseconds"
    );
    let serialized = serde_json::to_string(&first).unwrap();
    assert!(!serialized.contains(&first_id.to_string()));
    assert!(serialized.contains("check-observations"));
}

/// Two machines' toolchains, a Bun release apart and on one pinned rustc:
/// bginf-sp1 and bginf-sp2 as cairn://posts/505 measured them.
const BUN_1_3_14: &str =
    "rustc=rustc 1.97.1 (8bab26f4f 2026-07-14)\nhost: x86_64-unknown-linux-gnu;bun=1.3.14";
const BUN_1_4_4: &str =
    "rustc=rustc 1.97.1 (8bab26f4f 2026-07-14)\nhost: x86_64-unknown-linux-gnu;bun=1.4.4";
const BUN_DECLARED: &str = "command: bun run test:api\nruntime: [bun]\n";
const RUSTC_DECLARED: &str = "command: cargo check --locked\nruntime: [rustc]\n";

/// `yaml`'s check planned in a checkout whose toolchain is `expected`.
fn runtime_plan(yaml: &str, expected: &str) -> CheckPlan {
    let configured: CheckCommand = serde_yaml::from_str(yaml).unwrap();
    let mut planned = plan("lane", &configured.command);
    planned.verdict_runtime =
        crate::execution::check_identity::expected_verdict_runtime(&configured, expected);
    planned
}

/// A green verdict for `planned`, recorded the way the production writer
/// records one whose run measured `toolchain`, on its own machine.
fn green_measured_under(
    planned: &CheckPlan,
    id: &str,
    toolchain: &str,
    ran_at: i64,
) -> FreshCheckObservationWrite {
    let mut observation =
        reusable_observation_for(&planned.name, "input-runtime", format!("env-{id}"));
    observation.id = id.to_string();
    observation.public_handle = format!("handle-{id}");
    observation.ran_at = ran_at;
    observation.toolchain_fingerprint = Some(toolchain.to_string());
    observation.verdict_environment_hash = observed_verdict_environment_hash(
        planned,
        Some(cairn_checkenv::verdict_environment_hash(
            planned.verdict_environment_names.clone(),
            |name| std::env::var(name).ok(),
        )),
        Some(toolchain),
    );
    observation
}

/// One `yaml` check, green under Bun 1.3.14 and, later, green under Bun
/// 1.4.4, both on rustc 1.97.1. Each machine plans the check as its own
/// checkout resolves it, and its run measures that same toolchain.
async fn greens_a_bun_release_apart(yaml: &str) -> Arc<LocalDb> {
    let db = cache_db().await;
    let older = green_measured_under(&runtime_plan(yaml, BUN_1_3_14), "obs-1314", BUN_1_3_14, 100);
    let newer = green_measured_under(&runtime_plan(yaml, BUN_1_4_4), "obs-144", BUN_1_4_4, 200);
    record_fresh_check_observation(db.clone(), older).unwrap();
    record_fresh_check_observation(db.clone(), newer).unwrap();
    db
}

fn answered_by(db: &Arc<LocalDb>, yaml: &str, expected: &str) -> Option<String> {
    reusable_verdict_for_plan(
        db.clone(),
        "project-a",
        &runtime_plan(yaml, expected),
        "input-runtime",
        &[],
    )
    .map(|reused| reused.source_observation_id)
}

/// The specimen behind cairn/5790: a Bun check's verdict is a fact about
/// the Bun it ran under. A lookup expecting Bun 1.3.14 is answered by the
/// 1.3.14 row even though the 1.4.4 row is newer, one expecting 1.4.4 by the
/// 1.4.4 row, and one expecting a Bun neither ran under by nothing.
#[tokio::test]
async fn a_bun_declared_check_is_answered_only_under_its_own_bun() {
    let db = greens_a_bun_release_apart(BUN_DECLARED).await;
    assert_eq!(
        answered_by(&db, BUN_DECLARED, BUN_1_3_14).as_deref(),
        Some("obs-1314")
    );
    assert_eq!(
        answered_by(&db, BUN_DECLARED, BUN_1_4_4).as_deref(),
        Some("obs-144")
    );
    assert_eq!(
        answered_by(
            &db,
            BUN_DECLARED,
            &BUN_1_4_4.replace("bun=1.4.4", "bun=1.3.13")
        ),
        None,
        "no Bun 1.3.13 run exists to answer for one"
    );
}

/// The other half of the ruling: only declared instruments separate
/// verdicts. A rustc check's two rows, identical but for the Bun beside
/// them, are interchangeable, so either expectation is answered by the
/// newest.
#[tokio::test]
async fn a_rustc_declared_check_is_not_separated_by_bun() {
    let db = greens_a_bun_release_apart(RUSTC_DECLARED).await;
    assert_eq!(
        answered_by(&db, RUSTC_DECLARED, BUN_1_3_14).as_deref(),
        Some("obs-144")
    );
    assert_eq!(
        answered_by(&db, RUSTC_DECLARED, BUN_1_4_4).as_deref(),
        Some("obs-144")
    );
    assert_eq!(
        answered_by(
            &db,
            RUSTC_DECLARED,
            &BUN_1_4_4.replace("rustc 1.97.1", "rustc 1.98.0")
        ),
        None,
        "its own instrument still separates it"
    );
}

/// A check that declares nothing is keyed on every probed toolchain, so a
/// Bun change separates it too.
#[tokio::test]
async fn an_undeclared_check_is_separated_by_every_probed_toolchain() {
    const UNDECLARED: &str = "command: bun run check:dead\n";
    let db = greens_a_bun_release_apart(UNDECLARED).await;
    assert_eq!(
        answered_by(&db, UNDECLARED, BUN_1_3_14).as_deref(),
        Some("obs-1314")
    );
}

/// The `/checks` projection borrows from another tree by the same key: a
/// head expecting Bun 1.3.14 shows the 1.3.14 green, never the newer 1.4.4
/// one, and a reuse hit records its commit alias against the row it
/// reused.
#[tokio::test]
async fn the_head_listing_and_commit_alias_borrow_only_same_runtime_verdicts() {
    let db = greens_a_bun_release_apart(BUN_DECLARED).await;
    let configured: CheckCommand = serde_yaml::from_str(BUN_DECLARED).unwrap();
    let checks = HashMap::from([("lane".to_string(), configured)]);
    let input_hashes = HashMap::from([("lane".to_string(), "input-runtime".to_string())]);
    let shown = |expected: &str| {
        head_verdicts(
            db.clone(),
            "project-a",
            &checks,
            crate::execution::cache::HeadVerdictKeys {
                tree_hash: "tree-other",
                input_hashes: &input_hashes,
                job_id: "job-other",
                commit_sha: "commit-other",
                expected_toolchain: expected,
            },
        )
        .unwrap()
        .into_iter()
        .map(|lane| lane.row.toolchain_fingerprint)
        .collect::<Vec<_>>()
    };
    assert_eq!(shown(BUN_1_3_14), vec![Some(BUN_1_3_14.to_string())]);
    assert_eq!(shown(BUN_1_4_4), vec![Some(BUN_1_4_4.to_string())]);
    assert!(shown(&BUN_1_4_4.replace("bun=1.4.4", "bun=1.3.13")).is_empty());

    let plans = vec![(
        runtime_plan(BUN_DECLARED, BUN_1_3_14),
        "input-runtime".to_string(),
    )];
    let results = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-target",
        &plans,
        "turn-checks:job-target",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| async move { exec_ok(Some(0), "must not run") },
        |_| {},
    )
    .await;
    assert!(
        results[0].cached,
        "the 1.3.14 green answers without running"
    );
    let aliased = db
        .query_one(
            "SELECT source_observation_id FROM check_result_commit_aliases
                  WHERE project_id='project-a' AND commit_sha='commit-target' AND check_name='lane'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(aliased, "obs-1314");
}

#[tokio::test]
#[serial_test::serial]
async fn configured_verdict_environment_requires_an_exact_cache_hit() {
    let db = cache_db().await;
    let configured: CheckCommand = serde_yaml::from_str(
        "command: bun run test:rust\nverdictEnvironment:\n  - CAIRN_CUSTOM_VERDICT_MODE\n",
    )
    .unwrap();
    let mut rust = plan("rust", &configured.command);
    rust.verdict_environment_names =
        crate::execution::check_identity::verdict_environment_names(&configured);

    let old_optional = std::env::var_os("CAIRN_SYNC_TESTS_OPTIONAL");
    let old_custom = std::env::var_os("CAIRN_CUSTOM_VERDICT_MODE");
    std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", "enabled");
    std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", "strict");
    let recorded = plan_environment_fingerprint(&rust);
    let mut observation = reusable_observation_for("rust", "input-env", recorded.clone());
    observation.verdict_environment_hash = Some(effective_verdict_environment_hash(&rust, &[]));
    record_fresh_check_observation(db.clone(), observation).unwrap();
    assert!(!needs_execution(
        db.clone(),
        "project-a",
        &rust,
        "input-env",
        &[],
    ));

    std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", "disabled");
    assert!(needs_execution(
        db.clone(),
        "project-a",
        &rust,
        "input-env",
        &[]
    ));
    std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", "enabled");
    std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", "relaxed");
    assert!(needs_execution(
        db.clone(),
        "project-a",
        &rust,
        "input-env",
        &[]
    ));
    std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", "strict");
    assert_eq!(plan_environment_fingerprint(&rust), recorded);
    assert!(!needs_execution(db, "project-a", &rust, "input-env", &[]));

    match old_optional {
        Some(value) => std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", value),
        None => std::env::remove_var("CAIRN_SYNC_TESTS_OPTIONAL"),
    }
    match old_custom {
        Some(value) => std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", value),
        None => std::env::remove_var("CAIRN_CUSTOM_VERDICT_MODE"),
    }
}

#[tokio::test]
async fn manual_observation_is_reused_by_review_without_spawning() {
    let db = cache_db().await;
    let environment = current_check_environment_fingerprint();
    record_fresh_check_observation(db.clone(), reusable_observation(environment.clone())).unwrap();

    let plans = vec![(
        plan("frontend", "bunx vitest run"),
        "ih-frontend".to_string(),
    )];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let results = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "manual-check:run-a:frontend",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "ran")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 0, "a hit must spawn nothing");
    assert!(results[0].cached);
    assert_eq!(results[0].output_tail, "cached");
    let reused = results[0]
        .recorded
        .clone()
        .expect("a hit reports the observation it reused");
    assert_eq!(reused.id, "obs-frontend");
    assert_eq!(reused.environment_fingerprint, environment);
    assert!(reused.reusable);
    let alias = crate::execution::cache::get_check_result_observation(
        db.clone(),
        "project-a",
        "commit-target",
        "frontend",
        &environment,
        crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        None,
        None,
        100,
        0,
    )
    .unwrap()
    .expect("cache hit must create a target-commit alias");
    assert_eq!(alias.disposition, "cached");
    assert_eq!(alias.observation_id, "obs-frontend");
    assert_eq!(alias.source_commit_sha, "commit-source");
    assert_eq!(alias.evaluated_tree_hash, "tree-target");

    let review_calls = Arc::new(AtomicUsize::new(0));
    let counted = review_calls.clone();
    let review = run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-review",
            defined_by: "commit-review",
        },
        "tree-review",
        "job-a",
        &plans,
        "turn-checks:job-a",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "must not run")
            }
        },
        |_| {},
    )
    .await;
    assert_eq!(review_calls.load(Ordering::SeqCst), 0);
    assert!(review[0].cached);
}

#[tokio::test]
async fn turn_end_reuses_a_genuine_red_without_dispatching() {
    let db = cache_db().await;
    let mut failed = reusable_observation(current_check_environment_fingerprint());
    failed.exit_code = 1;
    failed.verdict = "failed".to_string();
    failed.output_tail = "assertion failed".to_string();
    record_fresh_check_observation(db.clone(), failed).unwrap();
    let plans = vec![(plan("frontend", "bunx vitest run"), "ih-frontend".into())];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let results = run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "turn-checks:job-a",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "must not run")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(results[0].cached);
    assert!(!results[0].passed);
    assert_eq!(results[0].output_tail, "assertion failed");
    assert_eq!(results[0].recorded.as_ref().unwrap().id, "obs-frontend");
}

/// End to end through the engine's real reuse arm: a lane answered from a
/// verdict produced at another tree records only a commit alias, and the
/// head that reused it must still find that verdict by its input hash. This
/// is the path that left cairn/4843 and cairn/4860 reading `no verdict` over
/// a store that held one for every lane.
#[tokio::test]
async fn an_alias_only_reuse_is_visible_to_the_head_that_reused_it() {
    let db = cache_db().await;
    let mut failed = reusable_observation(current_check_environment_fingerprint());
    failed.exit_code = 1;
    failed.verdict = "failed".to_string();
    record_fresh_check_observation(db.clone(), failed).unwrap();
    let plans = vec![(plan("frontend", "bunx vitest run"), "ih-frontend".into())];
    let results = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "turn-checks:job-a",
        CheckExecMode::Shared,
        None,
        |_index, _command, _stream_id| async move { exec_ok(Some(0), "must not run") },
        |_| {},
    )
    .await;
    assert!(results[0].cached, "the lane is answered by reuse");

    let rows = frontend_head_verdicts(db, "tree-target", "job-a", "commit-target");
    assert_eq!(
        rows.len(),
        1,
        "the reused verdict must reach the head's lane"
    );
    assert!(!rows[0].passed, "a reused red renders red");
    assert_ne!(
        rows[0].tree_hash, "tree-target",
        "reuse wrote no cache row at the head's tree; the verdict is the source's"
    );
}

/// The `/checks` lanes for one head configured with the single `frontend`
/// check of `reusable_observation`, at input hash `ih-frontend`.
fn frontend_head_verdicts(
    db: Arc<LocalDb>,
    tree_hash: &str,
    job_id: &str,
    commit_sha: &str,
) -> Vec<CheckResultCacheEntry> {
    let checks = HashMap::from([(
        "frontend".to_string(),
        check("bunx vitest run", None, CheckWhen::Review),
    )]);
    let input_hashes = HashMap::from([("frontend".to_string(), "ih-frontend".to_string())]);
    head_verdicts(
        db,
        "project-a",
        &checks,
        crate::execution::cache::HeadVerdictKeys {
            tree_hash,
            input_hashes: &input_hashes,
            job_id,
            commit_sha,
            expected_toolchain: test_expected_toolchain(),
        },
    )
    .unwrap()
    .into_iter()
    .map(|lane| lane.row)
    .collect()
}

/// Two heads on different trees that share a check's inputs both see the
/// reusable verdict, whichever of them wrote it.
#[tokio::test]
async fn heads_sharing_inputs_both_see_a_reusable_verdict() {
    let db = cache_db().await;
    record_fresh_check_observation(
        db.clone(),
        reusable_observation(current_check_environment_fingerprint()),
    )
    .unwrap();
    for (tree, job, commit) in [
        ("tree-source", "job-a", "commit-source"),
        ("tree-other", "job-b", "commit-other"),
    ] {
        let rows = frontend_head_verdicts(db.clone(), tree, job, commit);
        assert_eq!(rows.len(), 1, "{tree} must see the shared verdict");
        assert!(rows[0].passed);
    }
}

/// A green the engine would NOT reuse — a pass that needed a retry is
/// recorded `reusable = false` — is no answer for another head: the next
/// wave would run that lane. Its lane must stay verdictless there so a
/// settle-wait keeps waiting and the lazy arm still asks for the wave. The
/// head that produced it keeps it as its own evidence.
#[tokio::test]
async fn a_non_reusable_green_answers_only_the_head_that_produced_it() {
    let db = cache_db().await;
    let mut retried = reusable_observation(current_check_environment_fingerprint());
    retried.reusable = false;
    retried.non_reusable_reason = Some("passed only after a retry".to_string());
    record_fresh_check_observation(db.clone(), retried).unwrap();

    assert!(
        frontend_head_verdicts(db.clone(), "tree-other", "job-b", "commit-other").is_empty(),
        "reuse would execute this lane, so it holds no verdict on another head"
    );
    let own = frontend_head_verdicts(db, "tree-source", "job-a", "commit-source");
    assert_eq!(
        own.len(),
        1,
        "the producing head keeps its own diagnostic row"
    );
}

/// A verdict from a platform outside the check's declared trust set is not
/// one the engine would serve, so it does not answer another head either.
#[tokio::test]
async fn a_verdict_outside_the_platform_trust_set_answers_no_other_head() {
    let db = cache_db().await;
    let mut foreign = reusable_observation(current_check_environment_fingerprint());
    foreign.verdict_platform = Some("plan9".to_string());
    record_fresh_check_observation(db.clone(), foreign).unwrap();

    assert!(
        frontend_head_verdicts(db, "tree-other", "job-b", "commit-other").is_empty(),
        "a plan9 verdict is untrusted for a check gating on this host's platform"
    );
}

/// A verdict recorded under a different verdict environment is likewise
/// one reuse rejects.
#[tokio::test]
async fn a_verdict_from_another_verdict_environment_answers_no_other_head() {
    let db = cache_db().await;
    let mut elsewhere = reusable_observation(current_check_environment_fingerprint());
    elsewhere.verdict_environment_hash = Some("another-environment".to_string());
    record_fresh_check_observation(db.clone(), elsewhere).unwrap();

    assert!(frontend_head_verdicts(db, "tree-other", "job-b", "commit-other").is_empty());
}

/// The cadences and the `/checks` projection key a lane through one
/// function, so the hash a wave reuses under is the hash a lane is read by.
/// A check whose inputs a commit did not touch keeps its key across the two
/// trees, which is what lets the reader find its verdict after a rebase.
#[test]
fn every_configured_check_is_keyed_by_the_shared_input_hash() {
    let checks = HashMap::from([
        (
            "rust".to_string(),
            check("cargo test", Some(&["src/**"]), CheckWhen::Review),
        ),
        (
            "docs".to_string(),
            check("bun run docs", Some(&["docs/**"]), CheckWhen::Write),
        ),
    ]);
    let inputs = ResolvedInputs::resolve(&checks, &HashMap::new(), &TreeSnapshot::empty());
    let before = vec![
        ("src/lib.rs".to_string(), "blob-a".to_string()),
        ("docs/readme.md".to_string(), "docs-a".to_string()),
    ];
    let after = vec![
        ("src/lib.rs".to_string(), "blob-a".to_string()),
        ("docs/readme.md".to_string(), "docs-b".to_string()),
    ];
    let keyed = check_input_hashes(
        &checks,
        &inputs,
        Some(&before),
        "tree-a",
        ChangeBasis::Unknown,
    );
    for (name, definition) in &checks {
        assert_eq!(
            keyed[name],
            check_result_key(
                definition,
                inputs.for_check(name),
                Some(&before),
                "tree-a",
                &check_platform_identity(),
                check_toolchain_identity(),
                CheckChange::unknown("test"),
            ),
            "{name}: the shared key is the engine's key on this host"
        );
    }
    let moved = check_input_hashes(
        &checks,
        &inputs,
        Some(&after),
        "tree-b",
        ChangeBasis::Unknown,
    );
    assert_eq!(
        keyed["rust"], moved["rust"],
        "untouched inputs keep their key"
    );
    assert_ne!(
        keyed["docs"], moved["docs"],
        "touched inputs move their key"
    );
}

#[tokio::test]
async fn retry_bypasses_a_reusable_verdict_and_dispatches() {
    let db = cache_db().await;
    let environment = current_check_environment_fingerprint();
    let mut original = reusable_observation(environment.clone());
    original.commit_sha = "commit-target".to_string();
    original.defined_by_commit_sha = "commit-target".to_string();
    original.tree_hash = "tree-target".to_string();
    original.sealed_coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        "commit-target",
        "commit-target",
        "tree-target",
        "ih-frontend",
    );
    record_fresh_check_observation(db.clone(), original).unwrap();
    let check = plan("frontend", "bunx tsc --noEmit");
    let plans = vec![(check.clone(), "ih-frontend".into())];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let coordinates = sealed_coordinates(
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        &plans,
    );
    let results = run_planned_checks_with_board(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        &coordinates,
        &head_infra_scopes(plans.len(), "job-a", "commit-target"),
        "manual-check:run-a:frontend",
        CheckExecMode::Shared,
        None,
        None,
        &[],
        true,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "fresh execution")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!results[0].cached);
    assert_eq!(results[0].output_tail, "fresh execution");
    let retry_observation = results[0]
        .recorded
        .as_ref()
        .expect("retry records a fresh observation");
    assert_ne!(retry_observation.id, "obs-frontend");
    assert!(retry_observation.reusable);

    let reused = get_reusable_check_result(
        db,
        ReusableCheckLookup {
            project_id: "project-a",
            check_name: "frontend",
            input_hash: "ih-frontend",
            verdict_platforms: &check.verdict_platforms,
            executor_ids: None,
            implementation_identity: cairn_checkenv::implementation_identity(),
            verdict_environment_hash: &effective_verdict_environment_hash(&check, &[]),
            result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION
                as i64,
        },
    )
    .unwrap()
    .expect("the retry observation supersedes future lookup");
    assert_eq!(reused.source_observation_id, retry_observation.id);
    assert_eq!(reused.entry.output_tail, "fresh execution");
}

async fn count_rows(db: &LocalDb, sql: &'static str) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows.next().await?.expect("a count returns one row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

/// cairn/4982. A review lane whose platform no machine the fleet knows of
/// runs (the specimen: `executor-windows-check` with no Windows machine
/// enrolled) was offered to placement every wave, refused, and recorded as
/// a fresh infrastructure failure: 36 observations on two commits in three
/// hours. Through the real submission gate and the real engine, many waves
/// on one commit now attempt nothing and leave one row for that commit, no
/// observation, and no streak; a new commit gets its own one row; and once a
/// machine on the platform enrolls, the next wave runs the lane.
#[tokio::test]
async fn a_lane_no_known_machine_can_take_is_recorded_once_per_commit_until_one_enrolls() {
    let db = cache_db().await;
    let fleet = crate::fleet::Fleet::default();
    // A platform the runner's own executor cannot provide, so the only way
    // to satisfy it is a machine enrolling.
    let platform = if std::env::consts::OS == "windows" {
        "linux"
    } else {
        "windows"
    };
    let mut lane = plan("executor-windows-check", "cargo check --locked");
    lane.verdict_platforms = vec![platform.to_string()];
    let plans = vec![(lane.clone(), "ih-windows".to_string())];
    let mut item = batch_item(&lane.command, CheckResourceClass::Shared);
    item.name = lane.name.clone();
    item.executor = Some(ExecutorSelector {
        required_toolchains: vec!["rust".into()],
        ..ExecutorSelector::default()
    });
    item.verdict_platforms = lane.verdict_platforms.clone();
    let executions = Arc::new(AtomicUsize::new(0));

    let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> = Arc::default();
    let wave = |commit: &'static str| {
        let db = db.clone();
        let fleet = &fleet;
        let item = item.clone();
        let plans = plans.clone();
        let executions = executions.clone();
        let snapshots = snapshots.clone();
        async move {
            let commit = CheckRunCommit {
                evaluated: commit,
                defined_by: commit,
            };
            let coordinates = sealed_coordinates(commit, "tree-target", &plans);
            run_planned_checks_with_board(
                db,
                "project-a",
                commit,
                "tree-target",
                "job-a",
                &plans,
                &coordinates,
                &head_infra_scopes(plans.len(), "job-a", commit.evaluated),
                "turn-checks:job-a",
                CheckExecMode::Shared,
                None,
                None,
                &[],
                false,
                move |_index, _command, _stream_id| {
                    let mut items = vec![item.clone()];
                    let refusal = refuse_unplaceable_items(fleet, &mut items).remove(&0);
                    let executions = executions.clone();
                    async move {
                        match refusal {
                            Some(refused) => refused,
                            None => {
                                executions.fetch_add(1, Ordering::SeqCst);
                                exec_ok(Some(0), "checked").map_err(Into::into)
                            }
                        }
                    }
                },
                move |checks| snapshots.lock().unwrap().push(checks),
            )
            .await
            .remove(0)
        }
    };
    const ROWS: &str =
        "SELECT COUNT(*) FROM check_result_cache WHERE check_name = 'executor-windows-check'";
    const OBSERVATIONS: &str = "SELECT COUNT(*) FROM check_result_observations \
             WHERE check_name = 'executor-windows-check'";
    const STREAKS: &str = "SELECT COALESCE(SUM(infra_failure_streak), 0) FROM check_result_cache \
             WHERE check_name = 'executor-windows-check'";

    for _ in 0..12 {
        let outcome = wave("commit-a").await;
        assert_eq!(outcome.failure_kind, Some(CheckFailureKind::NotPlaceable));
        assert!(!outcome.is_genuine_failure(), "not placeable is no red");
        assert!(
            outcome.output_tail.contains("toolchains rust on"),
            "{}",
            outcome.output_tail
        );
    }
    assert_eq!(
        executions.load(Ordering::SeqCst),
        0,
        "nothing was attempted"
    );
    // The live board and the inline summary agree it is no red.
    let seen = snapshots.lock().unwrap().clone();
    assert!(
        seen.iter().flatten().all(|entry| entry.state != "failed"),
        "never a failed board entry: {seen:?}"
    );
    assert_eq!(
        seen.last().unwrap()[0].state,
        "unverified",
        "a terminal non-verdict"
    );
    let summary = format_check_summary(&[wave("commit-a").await], &HashMap::new());
    assert!(
        summary.starts_with("\u{26a0} executor-windows-check (not placeable"),
        "{summary}"
    );
    assert!(
        !summary.contains('\u{2717}'),
        "no failure marker: {summary}"
    );
    assert_eq!(count_rows(&db, ROWS).await, 1, "one row for the commit");
    assert_eq!(
        count_rows(&db, OBSERVATIONS).await,
        0,
        "nothing was evaluated"
    );
    assert_eq!(count_rows(&db, STREAKS).await, 0, "no failure was counted");

    for _ in 0..3 {
        wave("commit-b").await;
    }
    assert_eq!(count_rows(&db, ROWS).await, 2, "one row per commit");
    assert_eq!(executions.load(Ordering::SeqCst), 0);

    fleet.declare_enrolled_remote("bgsrv-win", "bgsrv-win", platform, "x86_64");
    let outcome = wave("commit-b").await;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "the next wave runs it"
    );
    assert!(outcome.passed);
    assert_eq!(outcome.failure_kind, None);
}

/// cairn/4982, the case that starts from history. A lane that reached the
/// fleet-domain bound while some machine could still take it (enrolled but
/// detached), and that no known machine can take now, must not keep being
/// re-stamped as a suppression by the turn-end gate: that would report a
/// repair that can no longer happen. It goes on to submission, which refuses
/// it as not placeable, and the bounded row is left exactly as it was.
#[tokio::test]
async fn a_bounded_fleet_suppression_yields_to_not_placeable_without_losing_its_evidence() {
    use crate::execution::cache::{
        claim_check_execution, get_suppressed_check_result, infra_fleet_scope,
        infra_suppression_scope, InfraRepairDomain,
    };
    let db = cache_db().await;
    let lane = plan("executor-windows-check", "cargo check --locked");
    let plans = vec![(lane.clone(), "ih-windows".to_string())];
    let commit = CheckRunCommit {
        evaluated: "commit-a",
        defined_by: "commit-a",
    };
    let scopes = InfraScopes {
        head: infra_suppression_scope("job-a", "commit-a"),
        platform_heads: Vec::new(),
        fleet: Some(infra_fleet_scope("empty-eligible-set")),
        environments: Vec::new(),
        fleet_blind: Vec::new(),
        lane_breaker: false,
    };
    let run = |tree: &'static str, failure: CheckExecutionFailure| {
        let db = db.clone();
        let plans = plans.clone();
        let scopes = scopes.clone();
        async move {
            let coordinates = sealed_coordinates(commit, tree, &plans);
            run_planned_checks_with_board(
                db,
                "project-a",
                commit,
                tree,
                "job-a",
                &plans,
                &coordinates,
                std::slice::from_ref(&scopes),
                "turn-checks:job-a",
                CheckExecMode::Shared,
                None,
                None,
                &[],
                false,
                move |_index, _command, _stream_id| {
                    let failure = failure.clone();
                    async move { Err::<CheckExecResult, _>(failure) }
                },
                |_| {},
            )
            .await
            .remove(0)
        }
    };

    // An enrolled-but-detached history: placement found no live match,
    // which the fleet domain bounds, and three heads dying against it
    // spend it. A reservation alone never charges a fleet counter.
    for tree in ["tree-earlier", "tree-earliest", "tree-target"] {
        claim_check_execution(db.clone(), "project-a", &lane.name, "ih-windows", &scopes).unwrap();
        run(
            tree,
            CheckExecutionFailure::substrate(
                SubstrateFailureShape::NoMachine,
                "no live enrolled executor satisfies toolchains rust on windows",
            ),
        )
        .await;
    }
    let bounded =
        get_suppressed_check_result(db.clone(), "project-a", &lane.name, "ih-windows", &scopes)
            .unwrap()
            .expect("the fleet domain is at its bound");
    assert_eq!(bounded.domain, InfraRepairDomain::Fleet);
    assert_eq!(bounded.streak(), 3);

    let input_hashes = HashMap::from([(lane.name.clone(), "ih-windows".to_string())]);
    let wave_scopes = HashMap::from([(lane.name.clone(), scopes.clone())]);
    let hold = |unplaceable: std::collections::HashSet<String>| {
        crate::execution::checks_turn_end::hold_back_suppressed(
            &db,
            "project-a",
            "tree-target",
            "job-a",
            "commit-a",
            vec![lane.clone()],
            &input_hashes,
            &wave_scopes,
            &unplaceable,
        )
    };
    let (to_run, suppressed) = hold(Default::default());
    assert!(
        to_run.is_empty(),
        "while a machine is enrolled, the bound holds"
    );
    assert_eq!(suppressed, vec![lane.name.clone()]);

    let (to_run, suppressed) = hold([lane.name.clone()].into());
    assert!(suppressed.is_empty(), "no known machine: the bound yields");
    assert_eq!(to_run.len(), 1);

    let outcome = run(
        "tree-target",
        CheckExecutionFailure::NotPlaceable(
        "No machine enrolled in this fleet, attached or not, satisfies toolchains rust on windows."
            .to_string(),
    ))
    .await;
    assert_eq!(outcome.failure_kind, Some(CheckFailureKind::NotPlaceable));
    assert_eq!(outcome.suppressed_after, None);
    let still =
        get_suppressed_check_result(db.clone(), "project-a", &lane.name, "ih-windows", &scopes)
            .unwrap()
            .expect("the bounded row is untouched");
    assert_eq!(still.domain, InfraRepairDomain::Fleet);
    assert_eq!(still.streak(), 3);
    assert!(
        still
            .evidence()
            .output_tail
            .contains("no live enrolled executor"),
        "the old diagnostic survives: {}",
        still.evidence().output_tail
    );
}

async fn run_cache_boundary_case(
    attempts: Vec<(Option<i32>, &'static str)>,
) -> (Vec<CheckOutcome>, Vec<String>) {
    let db = cache_db().await;
    let plans = vec![(plan("rust", "cargo test"), "ih-rust".into())];
    let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = commands.clone();
    let attempts = Arc::new(std::sync::Mutex::new(attempts.into_iter()));
    let coordinates = sealed_coordinates(
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        &plans,
    );
    let results = run_planned_checks_with_board(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        &coordinates,
        &head_infra_scopes(plans.len(), "job-a", "commit-target"),
        "turn-checks:job-a",
        CheckExecMode::Isolated,
        None,
        None,
        &[],
        true,
        move |_index, command, _stream_id| {
            captured.lock().unwrap().push(command);
            let attempt = attempts.lock().unwrap().next().expect("unexpected retry");
            async move { exec_ok(attempt.0, attempt.1) }
        },
        |_| {},
    )
    .await;
    let commands = commands.lock().unwrap().clone();
    (results, commands)
}

#[tokio::test]
async fn cache_corruption_retries_once_uncached_and_canonicalizes_pass() {
    let (results, commands) = run_cache_boundary_case(vec![
        (Some(1), "sccache: memory map must have a non-zero length"),
        (Some(0), "clean pass"),
    ])
    .await;
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0], "cargo test");
    assert!(commands[1].starts_with(UNCACHED_RETRY_PREFIX));
    assert!(results[0].passed);
    assert_eq!(results[0].output_tail, "clean pass");
}

#[tokio::test]
async fn cache_corruption_retry_preserves_a_genuine_red() {
    let (results, commands) = run_cache_boundary_case(vec![
        (Some(1), "sccache: memory map must have a non-zero length"),
        (Some(1), "error[E0308]: mismatched types"),
    ])
    .await;
    assert_eq!(commands.len(), 2);
    assert!(!results[0].passed);
    assert_eq!(results[0].failure_kind, None);
}

#[tokio::test]
async fn repeated_cache_corruption_stops_after_one_retry() {
    let (results, commands) = run_cache_boundary_case(vec![
        (Some(1), "sccache: memory map must have a non-zero length"),
        (Some(1), "sccache: memory map must have a non-zero length"),
    ])
    .await;
    assert_eq!(commands.len(), 2);
    assert_eq!(
        results[0].failure_kind,
        Some(CheckFailureKind::Infrastructure)
    );
}

#[tokio::test]
async fn ordinary_failure_does_not_cross_cache_retry_boundary() {
    let (results, commands) =
        run_cache_boundary_case(vec![(Some(1), "error[E0308]: mismatched types")]).await;
    assert_eq!(commands.len(), 1);
    assert!(!results[0].passed);
    assert_eq!(results[0].failure_kind, None);
}

#[tokio::test]
async fn fleet_backed_execution_reuses_declared_trust_hit_without_launching() {
    let db = cache_db().await;
    let environment = current_check_environment_fingerprint();
    let effective_env = vec![("ONLY_IN_BATCH".to_string(), "batch-value".to_string())];
    let mut plan = plan("frontend", "bunx vitest run");
    plan.verdict_environment_names = vec!["ONLY_IN_BATCH".to_string()];
    let mut observation = reusable_observation(environment);
    observation.verdict_environment_hash =
        Some(effective_verdict_environment_hash(&plan, &effective_env));
    record_fresh_check_observation(db.clone(), observation).unwrap();

    let config = TempDir::new().unwrap();
    let orch = test_orchestrator(config.path()).await;
    let plans = vec![(plan, "ih-frontend".to_string())];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let results = run_planned_checks_at_commit_with_env(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        Some(&orch),
        &effective_env,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "remote-selected execution")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a declared-trust hit must resolve before fleet placement"
    );
    assert!(results[0].cached);
}

#[tokio::test]
async fn provenance_fingerprint_mismatch_does_not_narrow_declared_trust() {
    let db = cache_db().await;
    record_fresh_check_observation(
        db.clone(),
        reusable_observation("definitely-not-the-current-environment".to_string()),
    )
    .unwrap();

    let plans = vec![(
        plan("frontend", "bunx vitest run"),
        "ih-frontend".to_string(),
    )];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let results = run_planned_checks_at_commit(
        db,
        "project-a",
        CheckRunCommit {
            evaluated: "commit-target",
            defined_by: "commit-target",
        },
        "tree-target",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "fresh execution")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "opaque provenance must not narrow the declared platform trust set"
    );
    assert!(results[0].cached);
}

/// One evaluation performed the way a cadence actually performs it: RESERVE
/// at submission, launch only what the reservation admits, and only then
/// settle the outcomes through the engine.
///
/// The ordering is the whole point. Both real cadences build a batch, run it
/// on a build cell, and hand the finished results to `run_planned_checks`
/// afterwards — so a test that drives the engine with a closure that
/// executes inline models an ordering that does not exist, and cannot
/// observe an over-execution at all. `launches` counts commands actually
/// started, which is the number the bound is about.
async fn evaluate_infra_suite(
    db: &Arc<LocalDb>,
    plans: &[(CheckPlan, String)],
    tree: &str,
    launches: &Arc<AtomicUsize>,
) -> Vec<CheckOutcome> {
    let items: Vec<PlannedCheckBatchItem> = plans
        .iter()
        .enumerate()
        .map(|(index, (plan, input_hash))| PlannedCheckBatchItem {
            recording_plan: None,
            index,
            name: plan.name.clone(),
            input_hash: input_hash.clone(),
            resource_identity: CheckResourceIdentityInput::Checkpoint {
                name: String::new(),
                command: String::new(),
            },
            command: plan.command.clone(),
            stream_id: String::new(),
            env: Vec::new(),
            timeout_ms: 1_000,
            executor: None,
            verdict_platforms: Vec::new(),
            resource_class: CheckResourceClass::Shared,
            verdict_environment_names: Vec::new(),
            infra_scopes: InfraScopes::head_only("job-test", "commit-test"),
            avoid_executor_ids: Vec::new(),
        })
        .collect();
    let (admitted, mut results) = reserve_batch_items(db.clone(), "project-a", items);
    for item in &admitted {
        launches.fetch_add(1, Ordering::SeqCst);
        results.insert(
            item.index,
            Ok(CheckExecResult {
                exit_code: Some(254),
                // The sccache adoption shape: a real exit code with positive
                // infrastructure evidence in the output.
                output: "error: could not compile `serde`: process didn't exit successfully: \
                             `rustc` (exit status: 254)"
                    .to_string(),
                timed_out: false,
                duration_ms: None,
                provenance: None,
                publication: None,
            }),
        );
    }

    let results = Arc::new(std::sync::Mutex::new(results));
    run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-test",
            defined_by: "commit-test",
        },
        tree,
        "job-test",
        plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |index, _command, _stream_id| {
            let results = results.clone();
            async move {
                results.lock().unwrap().remove(&index).unwrap_or_else(|| {
                    Err(CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        format!("missing batch outcome for plan index {index}"),
                    ))
                })
            }
        },
        |_| {},
    )
    .await
}

/// The whole point of the kill switch, driven through the real submission
/// ordering. Three consecutive infrastructure failures at one input hash, and
/// the fourth evaluation LAUNCHES NOTHING.
///
/// Note the shape: the first three evaluations must each still launch. A
/// guard that stopped executing on the first infrastructure failure would
/// also pass a test that only asserted the fourth, and would strip every
/// genuinely transient failure of its retries.
#[tokio::test]
async fn repeated_infrastructure_failure_stops_being_executed_at_the_bound() {
    let db = cache_db().await;
    let plans = vec![(plan("rust", "cargo test"), "ih-rust".to_string())];
    let launches = Arc::new(AtomicUsize::new(0));

    let mut last = Vec::new();
    for evaluation in 1..=(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND + 1) {
        last = evaluate_infra_suite(&db, &plans, "commit-test", &launches).await;
        let expected_launches =
            evaluation.min(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND) as usize;
        assert_eq!(
            launches.load(Ordering::SeqCst),
            expected_launches,
            "evaluation {evaluation} must {} launch the command",
            if evaluation > crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
                "NOT"
            } else {
                "still"
            }
        );
    }

    // The suppressed evaluation reports itself as what it is: not run.
    assert_eq!(last.len(), 1);
    assert!(!last[0].passed, "a suppression is not a pass");
    assert_eq!(
        last[0].suppressed_after,
        Some(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
    );
    assert!(!last[0].is_genuine_failure(), "and it is not a red either");
    assert!(last[0].output_tail.contains(
        "Stopped after 3 infrastructure failures on this commit; re-runs automatically on the next commit."
    ));
    assert!(last[0].output_tail.contains("Last diagnostic:"));

    // The row follows the current tree, so the checklist still shows the
    // check rather than dropping it silently.
    let listed =
        crate::execution::cache::list_check_results(db.clone(), "project-a", "commit-test")
            .unwrap();
    assert!(
        listed.iter().any(|entry| {
            entry.infra_failure_streak == crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND
        }),
        "the re-stamped result must retain the bounded infrastructure streak"
    );
}

/// Two cadences submitting the same triple at the same moment. The
/// reservation lives at submission, so only one batch ever carries the item
/// and only one command is ever launched.
///
/// This is the ordering a post-execution check cannot fix: by the time the
/// engine sees either result, both cells would already have run.
#[tokio::test]
async fn concurrent_submissions_admit_the_item_to_only_one_batch() {
    let db = cache_db().await;
    let plans = vec![(plan("rust", "cargo test"), "ih-rust".to_string())];
    let launches = Arc::new(AtomicUsize::new(0));

    // Spend every retry but the last.
    for _ in 1..crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
        evaluate_infra_suite(&db, &plans, "commit-test", &launches).await;
    }
    assert_eq!(
        launches.load(Ordering::SeqCst),
        (crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND - 1) as usize
    );

    let item = PlannedCheckBatchItem {
        recording_plan: None,
        index: 0,
        name: "rust".to_string(),
        input_hash: "ih-rust".to_string(),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: String::new(),
            command: String::new(),
        },
        command: "cargo test".to_string(),
        stream_id: String::new(),
        env: Vec::new(),
        verdict_environment_names: Vec::new(),
        timeout_ms: 1_000,
        executor: None,
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        infra_scopes: InfraScopes::head_only("job-test", "commit-test"),
        avoid_executor_ids: Vec::new(),
    };
    let racers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            let item = item.clone();
            tokio::task::spawn_blocking(move || {
                reserve_batch_items(db, "project-a", vec![item]).0.len()
            })
        })
        .collect();
    let mut carried = 0;
    for racer in racers {
        carried += racer.await.unwrap();
    }

    assert_eq!(
        carried, 1,
        "exactly one of two simultaneous submissions may carry the item to a cell"
    );
}

/// A genuine verdict at the same input hash restores execution immediately —
/// suppression can never be the reason a real result goes unmeasured.
#[tokio::test]
async fn a_genuine_verdict_restores_execution() {
    let db = cache_db().await;
    // Drive the triple to the bound the way execution does — reserve, then
    // record. Storing alone would only ever OPEN the streak at 1, because a
    // retry is counted when it is admitted, not when it completes.
    for _ in 0..crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
        let _ = crate::execution::cache::claim_check_execution(
            db.clone(),
            "project-a",
            "rust",
            "ih-rust",
            &InfraScopes::head_only("job-a", "commit-a"),
        );
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "tree-a".to_string(),
                input_hash: "ih-rust".to_string(),
                check_name: "rust".to_string(),
                exit_code: 1,
                passed: false,
                output_tail: "sccache: server startup failed".to_string(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
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
                defined_by_commit_sha: Some("commit-a".to_string()),
                environment_fingerprint: infra_suppression_scope("job-a", "commit-a"),
                verdict_platform: None,
                verdict_arch: None,
                verdict_environment_hash: None,
                transient_within_grace: false,
            },
        )
        .unwrap();
    }

    // An ordinary red lands at the same hash (e.g. a sibling cadence got a
    // real answer out of the same command).
    store_check_result(
        db.clone(),
        CheckResultCacheWrite {
            project_id: "project-a".to_string(),
            tree_hash: "tree-a".to_string(),
            input_hash: "ih-rust".to_string(),
            check_name: "rust".to_string(),
            exit_code: 101,
            passed: false,
            output_tail: "assertion failed".to_string(),
            duration_ms: 1,
            target_results_json: None,
            job_id: Some("job-a".to_string()),
            cached: Some(false),
            failure_kind: None,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some("commit-a".to_string()),
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            transient_within_grace: false,
        },
    )
    .unwrap();

    let plans = vec![(plan("rust", "cargo test"), "ih-rust".to_string())];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-b",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "ok")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1, "the check runs again");
    assert!(results[0].passed);
    assert_eq!(results[0].suppressed_after, None);
}

/// A suite abandoned mid-flight — the shape a superseded review wave takes
/// when its cancellation lever fires — must leave no verdict behind. A
/// half-finished check is not a result, and persisting one would let a tree
/// nobody validated read as checked.
#[tokio::test]
async fn abandoning_a_running_miss_stores_nothing() {
    let db = cache_db().await;
    let started = Arc::new(AtomicUsize::new(0));
    let task = {
        let db = db.clone();
        let started = started.clone();
        tokio::spawn(async move {
            let plans = vec![(plan("queued", "run-queued"), "ih-queued".to_string())];
            run_planned_checks(
                db,
                "project-a",
                "tree-cancelled",
                "job-a",
                &plans,
                "tool",
                CheckExecMode::Isolated,
                None,
                move |_index, _command, _stream_id| {
                    let started = started.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        // Outlives the abort below, so the check is still
                        // running when its suite is dropped.
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        exec_ok(Some(0), "never completes")
                    }
                },
                |_| {},
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        started.load(Ordering::SeqCst),
        1,
        "the check must be in flight when the suite is abandoned"
    );
    task.abort();
    let _ = task.await;
    assert!(
        crate::execution::cache::list_check_results(db, "project-a", "tree-cancelled")
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn cache_miss_runs_then_stores() {
    let db = cache_db().await;
    // A command with no recognized runner, so this stays a test about cache
    // write-through rather than about failure classification (which
    // `miss_classifies_and_persists_timeout_spawn_and_signal` covers).
    let plans = vec![(plan("frontend", "bun run check:web"), "ih-b".to_string())];
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-b",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(1), "check failed")
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a miss runs the check once"
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "frontend");
    assert!(!results[0].passed);
    assert_eq!(results[0].exit_code, Some(1));

    assert!(
        get_check_result(db.clone(), "project-a", "frontend", "ih-b")
            .unwrap()
            .is_none(),
        "a failed verdict is stored for visibility but is not reusable"
    );
    let stored = crate::execution::cache::list_check_results(db, "project-a", "tree-b").unwrap();
    assert_eq!(stored.len(), 1, "a miss stores exactly one visible result");
    let stored = &stored[0];
    assert_eq!(stored.input_hash, "ih-b");
    assert_eq!(stored.check_name, "frontend");
    assert_eq!(stored.exit_code, 1);
    assert!(!stored.passed);
    assert_eq!(stored.failure_kind, None);
    assert_eq!(stored.output_tail, "check failed");
    assert_eq!(stored.job_id.as_deref(), Some("job-a"));
    assert_eq!(stored.cached, Some(false));
    assert_eq!(stored.executor_id, None);
    assert_eq!(stored.executor_device_id, None);
    assert_eq!(stored.executor_connection_generation, None);
    assert_eq!(stored.executor_cell_id, None);
    assert_eq!(stored.executor_lease_epoch, None);
    assert_eq!(stored.executor_started_at_unix_ms, None);
    assert_eq!(stored.executor_finished_at_unix_ms, None);
    assert_eq!(
        stored.toolchain_fingerprint.as_deref(),
        Some(cairn_checkenv::toolchain_identity())
    );
}

#[tokio::test]
async fn cache_miss_persists_structured_results() {
    let db = cache_db().await;
    let plans = vec![(
        plan("rust", "bun run test:rust:nextest"),
        "ih-structured".to_string(),
    )];
    let nextest_output = "     Summary [   0.1s] 3 tests run: 1 passed, 2 failed, 0 skipped\n\
            \x20       FAIL [   0.0s] (1/3) mycrate mod::test_a\n\
            \x20       FAIL [   0.0s] (2/3) mycrate mod::test_b"
        .to_string();
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-structured",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let out = nextest_output.clone();
            async move { exec_ok(Some(100), out) }
        },
        |_| {},
    )
    .await;

    // The outcome carries the parsed per-test detail.
    let parsed = results[0].parsed.as_ref().expect("nextest output parses");
    assert_eq!(parsed.parser, "nextest");
    assert_eq!(parsed.failed, 2);
    assert_eq!(parsed.failures.len(), 2);

    // The failed verdict is not reusable, but its structured evidence remains
    // visible on the tree-scoped result row for diagnostics and baseline work.
    assert!(
        get_check_result(db.clone(), "project-a", "rust", "ih-structured")
            .unwrap()
            .is_none(),
        "a failed verdict is stored for visibility but is not reusable"
    );
    let stored =
        crate::execution::cache::list_check_results(db, "project-a", "tree-structured").unwrap();
    assert_eq!(stored.len(), 1, "a miss stores exactly one visible result");
    let stored = &stored[0];
    assert_eq!(stored.input_hash, "ih-structured");
    assert_eq!(stored.check_name, "rust");
    assert_eq!(stored.exit_code, 100);
    assert!(!stored.passed);
    assert_eq!(stored.failure_kind, None);
    assert_eq!(stored.job_id.as_deref(), Some("job-a"));
    assert_eq!(stored.cached, Some(false));
    assert_eq!(stored.executor_id, None);
    assert_eq!(stored.executor_device_id, None);
    assert_eq!(stored.executor_connection_generation, None);
    assert_eq!(stored.executor_cell_id, None);
    assert_eq!(stored.executor_lease_epoch, None);
    assert_eq!(stored.executor_started_at_unix_ms, None);
    assert_eq!(stored.executor_finished_at_unix_ms, None);
    assert_eq!(
        stored.toolchain_fingerprint.as_deref(),
        Some(cairn_checkenv::toolchain_identity())
    );
    let json = stored
        .target_results_json
        .as_deref()
        .expect("structured results persisted");
    assert!(json.contains("\"parser\":\"nextest\""));
    assert!(json.contains("mycrate mod::test_a"));
    assert!(json.contains("mycrate mod::test_b"));
}

/// The repro this whole change fixes. A src-tauri commit runs the rust check
/// and records an immutable reusable observation keyed by the src-tauri input hash. A following
/// doc-only commit moves the WHOLE-tree hash but leaves that input hash
/// unchanged, so the verdict is a cache HIT — rust does not re-run — and the
/// commit receives a cached alias rather than mutating its source evidence.
#[tokio::test]
#[serial_test::serial]
async fn doc_only_commit_reuses_impact_scoped_verdict() {
    let db = cache_db().await;
    let calls = Arc::new(AtomicUsize::new(0));

    // Commit 1 touches src-tauri and produced a complete immutable reusable
    // observation for input hash IH1 at whole-tree tree-1.
    let plans = vec![(plan("rust", "bun run test:rust"), "IH1".to_string())];
    let environment = plan_environment_fingerprint(&plans[0].0);
    let mut source = reusable_observation_for("rust", "IH1", environment.clone());
    source.commit_sha = "commit-1".to_string();
    source.tree_hash = "tree-1".to_string();
    source.sealed_coordinate = crate::execution::cache::SealedCheckCoordinate::new(
        "commit-1",
        "commit-source",
        "tree-1",
        "IH1",
    );
    source.verdict_environment_hash = Some(effective_verdict_environment_hash(&plans[0].0, &[]));
    record_fresh_check_observation(db.clone(), source).unwrap();

    // Commit 2 is doc-only: the whole tree changes to tree-2, but the rust
    // input hash is UNCHANGED (still IH1), so the verdict is a cache hit and
    // the check does not re-run.
    let counted = calls.clone();
    let r2 = run_planned_checks_at_commit(
        db.clone(),
        "project-a",
        CheckRunCommit {
            evaluated: "commit-2",
            defined_by: "commit-2",
        },
        "tree-2",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                exec_ok(Some(0), "ran")
            }
        },
        |_| {},
    )
    .await;
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].name, "rust");
    assert!(r2[0].passed);
    assert_eq!(r2[0].exit_code, Some(0));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a doc-only commit must not re-run the rust check"
    );

    let alias = crate::execution::cache::get_check_result_observation(
        db,
        "project-a",
        "commit-2",
        "rust",
        &environment,
        crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        None,
        None,
        1,
        0,
    )
    .unwrap()
    .expect("the doc commit must receive a cached alias");
    assert_eq!(alias.disposition, "cached");
    assert_eq!(alias.evaluated_tree_hash, "tree-2");
}

/// A fold onto a moved head re-keys a lane to the head's tree while the
/// head's own wave is verifying that same key (cairn/5932, where six lanes
/// ran twice at 8da684fc). The fold waits for that wave, runs nothing, and
/// records the head wave's verdict at the fix commit by reuse, so the fix
/// commit's row names the head's observation. A fix that changed no paths
/// publishes the head itself as the fix commit; one with paths publishes a
/// child of it. Both read the same way.
#[tokio::test]
#[serial_test::serial]
async fn a_fold_onto_a_moved_head_reuses_the_heads_own_verdict_instead_of_running_it() {
    for (fix_commit, fix_tree) in [("commit-h2", "tree-h2"), ("commit-fix", "tree-fix")] {
        let db = cache_db().await;
        let registry = Arc::new(crate::execution::lane_claims::LaneClaims::default());
        let keyed = vec![(
            plan("migrations", "bun run check:migrations"),
            "IH2".to_string(),
        )];
        let environment = plan_environment_fingerprint(&keyed[0].0);

        // H2's own wave planned first and is verifying migrations at IH2.
        let mut head_wave = registry.open_wave("job-a");
        head_wave.claim([lane_key("project-a", &keyed[0])], "commit-h2");
        // H1's wave, folding, still holds the key it planned under at H1.
        let mut fold = registry.open_wave("job-a");
        fold.claim(
            [LaneKey {
                project_id: "project-a".into(),
                check_name: "migrations".into(),
                input_hash: "IH1".into(),
            }],
            "commit-h1",
        );

        let mut observation = reusable_observation_for("migrations", "IH2", environment.clone());
        observation.id = "obs-h2".into();
        observation.commit_sha = "commit-h2".into();
        observation.defined_by_commit_sha = "commit-h2".into();
        observation.tree_hash = "tree-h2".into();
        observation.sealed_coordinate = crate::execution::cache::SealedCheckCoordinate::new(
            "commit-h2",
            "commit-h2",
            "tree-h2",
            "IH2",
        );
        observation.verdict_environment_hash =
            Some(effective_verdict_environment_hash(&keyed[0].0, &[]));
        let recorder = db.clone();
        let head_records = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            record_fresh_check_observation(recorder, observation).unwrap();
            drop(head_wave);
        });

        let (left, answered) = lanes_left_to_run(
            db.clone(),
            &mut fold,
            "project-a",
            &keyed,
            &[0],
            &[],
            std::time::Duration::from_secs(30),
        )
        .await;
        head_records.await.unwrap();
        assert!(
            left.is_empty(),
            "the head's wave verified migrations at this key; the fold must not run it again"
        );
        assert_eq!(
            answered,
            vec![ClaimedElsewhere {
                check_name: "migrations".into(),
                commit: "commit-h2".into(),
            }]
        );
        let line = format_answered_elsewhere(&answered).expect("the summary names the reuse");
        assert!(
            line.contains("migrations from the wave at commit-h2"),
            "{line}"
        );

        // Recording the fold runs nothing: the head's verdict answers it.
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let outcomes = run_planned_checks_at_commit(
            db.clone(),
            "project-a",
            CheckRunCommit {
                evaluated: fix_commit,
                defined_by: fix_commit,
            },
            fix_tree,
            "job-a",
            &keyed,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 0, "fix commit {fix_commit}");
        assert!(
            outcomes[0].passed && outcomes[0].cached,
            "the lane is answered by reuse: {}",
            outcomes[0].output_tail
        );

        let row = crate::execution::cache::get_check_result_observation(
            db,
            "project-a",
            fix_commit,
            "migrations",
            &environment,
            crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            None,
            None,
            1,
            0,
        )
        .unwrap()
        .expect("the fix commit is not blank");
        assert_eq!(
            row.source_commit_sha, "commit-h2",
            "the fix commit's row names the head wave's observation"
        );
    }
}

/// The claim is a hint, never a verdict: a claimant that ends without
/// recording one (its lane died, or its own fold re-keyed it away) leaves
/// the lane a miss, and the fold runs it.
#[tokio::test]
#[serial_test::serial]
async fn a_fold_runs_the_lane_another_wave_left_without_a_verdict() {
    let db = cache_db().await;
    let registry = Arc::new(crate::execution::lane_claims::LaneClaims::default());
    let keyed = vec![(
        plan("migrations", "bun run check:migrations"),
        "IH2".to_string(),
    )];
    let mut head_wave = registry.open_wave("job-a");
    head_wave.claim([lane_key("project-a", &keyed[0])], "commit-h2");
    let mut fold = registry.open_wave("job-a");
    fold.claim([], "commit-h1");
    let head_ends = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        drop(head_wave);
    });
    let (left, answered) = lanes_left_to_run(
        db,
        &mut fold,
        "project-a",
        &keyed,
        &[0],
        &[],
        std::time::Duration::from_secs(30),
    )
    .await;
    head_ends.await.unwrap();
    assert_eq!(left, vec![0]);
    assert!(answered.is_empty());
}

/// The three abnormal deaths a check can suffer are each classified,
/// persisted durably, and rendered as themselves — the core of this change.
#[tokio::test]
async fn miss_classifies_and_persists_timeout_spawn_and_signal() {
    let db = cache_db().await;
    let plans = vec![
        (plan("slow", "run-slow"), "ih-slow".to_string()),
        (plan("nogo", "run-nogo"), "ih-nogo".to_string()),
        (plan("crash", "run-crash"), "ih-crash".to_string()),
    ];
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-cls",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |index, _command, _stream_id| async move {
            match index {
                // Killed at its budget, with a nextest SLOW line naming the
                // test in flight at the kill.
                0 => Ok(CheckExecResult {
                    exit_code: None,
                    output: "     SLOW [>  60.000s] mycrate mod::hangs\nstill going".to_string(),
                    timed_out: true,
                    duration_ms: None,
                    provenance: None,
                    publication: None,
                }),
                // The process could not be spawned.
                1 => Err("Failed to spawn command: No such file or directory".to_string()),
                // Died by signal mid-run (no exit code, not a timeout).
                _ => Ok(CheckExecResult {
                    exit_code: None,
                    output: "segfault".to_string(),
                    timed_out: false,
                    duration_ms: None,
                    provenance: None,
                    publication: None,
                }),
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(results[0].failure_kind, Some(CheckFailureKind::TimedOut));
    assert_eq!(results[1].failure_kind, Some(CheckFailureKind::SpawnError));
    assert_eq!(results[2].failure_kind, Some(CheckFailureKind::Killed));
    assert!(results.iter().all(|o| !o.passed));

    // The classification is persisted, so every downstream surface can render
    // the real death rather than re-deriving it from exit -1.
    let stored =
        crate::execution::cache::list_check_results(db.clone(), "project-a", "tree-cls").unwrap();
    let kind_of = |name: &str| {
        stored
            .iter()
            .find(|row| row.check_name == name)
            .and_then(|row| row.failure_kind.as_deref())
    };
    assert_eq!(kind_of("slow"), Some("timed_out"));
    assert_eq!(kind_of("nogo"), Some("spawn_error"));
    assert_eq!(kind_of("crash"), Some("killed"));
    assert!(get_check_result(db.clone(), "project-a", "slow", "ih-slow")
        .unwrap()
        .is_none());

    // The timeout summary names the timeout AND the still-running test; the
    // spawn error names itself.
    let summary = format_check_summary(&results, &HashMap::new());
    assert!(summary.contains("timed out after"), "got: {summary}");
    assert!(summary.contains("mycrate mod::hangs"), "got: {summary}");
    assert!(summary.contains("failed to spawn"), "got: {summary}");
}

// --- live status snapshots + sequential ordering ----------------------

fn find<'a>(snap: &'a [CheckStatusEntry], name: &str) -> &'a CheckStatusEntry {
    snap.iter()
        .find(|e| e.name == name)
        .unwrap_or_else(|| panic!("no `{name}` entry in snapshot"))
}

/// The notify callback receives a full checklist snapshot on every
/// transition: the planned set is all-pending, a cache hit jumps straight to
/// its final state (annotated `cached`, never `running`), and a miss passes
/// through `running` before its annotated final state.
#[tokio::test]
async fn notify_emits_planned_running_and_final_snapshots() {
    let db = cache_db().await;
    // frontend is already cached (passing); typecheck is a fresh miss.
    record_fresh_check_observation(
        db.clone(),
        reusable_observation(current_check_environment_fingerprint()),
    )
    .unwrap();

    let plans = vec![
        (plan("frontend", "run-frontend"), "ih-frontend".to_string()),
        (
            plan("typecheck", "run-typecheck"),
            "ih-typecheck".to_string(),
        ),
    ];
    let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = snapshots.clone();
    run_planned_checks(
        db.clone(),
        "project-a",
        "tree-a",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        // typecheck misses and fails with a bare exit code.
        move |_index, _command, _stream_id| async move { exec_ok(Some(1), "boom") },
        move |checks| captured.lock().unwrap().push(checks),
    )
    .await;

    let snaps = snapshots.lock().unwrap();
    assert!(
        snaps.len() >= 4,
        "planned + hit + running + final, got {}",
        snaps.len()
    );

    // First snapshot is the planned set: everything pending, unannotated.
    let planned = &snaps[0];
    assert!(planned
        .iter()
        .all(|e| e.state == "pending" && e.annotation.is_none()));

    // frontend was a cache hit: it reaches `passed` annotated `cached` and is
    // NEVER seen in a `running` state (no run phase for a hit).
    assert!(
        snaps.iter().all(|s| find(s, "frontend").state != "running"),
        "a cache hit must never pass through `running`"
    );
    let frontend_final = find(snaps.last().unwrap(), "frontend");
    assert_eq!(frontend_final.state, "passed");
    assert_eq!(frontend_final.annotation.as_deref(), Some("cached"));

    // typecheck (a miss) passes through `running` (unannotated) then `failed`
    // with the same annotation the final summary uses.
    assert!(
        snaps.iter().any(|s| {
            let e = find(s, "typecheck");
            e.state == "running" && e.annotation.is_none()
        }),
        "a miss must surface a `running` snapshot"
    );
    let typecheck_final = find(snaps.last().unwrap(), "typecheck");
    assert_eq!(typecheck_final.state, "failed");
    assert_eq!(typecheck_final.annotation.as_deref(), Some("exit 1"));
}

/// Outcomes — and the summary built from them — come back in plan order.
#[tokio::test]
async fn checks_return_and_summarize_in_plan_order() {
    let db = cache_db().await;
    let plans = vec![
        (plan("a", "cmd-a"), "ih-a".to_string()),
        (plan("b", "cmd-b"), "ih-b".to_string()),
        (plan("c", "cmd-c"), "ih-c".to_string()),
    ];
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-p",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| async move { exec_ok(Some(0), String::new()) },
        |_| {},
    )
    .await;

    let names: Vec<&str> = results.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "c"], "outcomes must be in plan order");
    // The summary follows the same plan order (each name carries a duration
    // annotation, so match on relative position rather than exact text).
    let summary = format_check_summary(&results, &HashMap::new());
    let pos = |name: &str| summary.find(name).expect("name present in summary");
    assert!(
        pos("a") < pos("b") && pos("b") < pos("c"),
        "the summary must reflect plan order: {summary}"
    );
}

/// `Shared` is the canonical write-check mode and must never overlap two check
/// commands in the one checkout: a mutating check (a formatter / `--fix` lint)
/// has to settle before the next check observes the shared sealed worktree.
/// Even when each executor yields at an await, the concurrent-invocation
/// high-water mark stays 1.
#[tokio::test]
async fn shared_mode_checks_stay_sequential() {
    let db = cache_db().await;
    let plans = vec![
        (plan("x", "cmd-x"), "ih-x".to_string()),
        (plan("y", "cmd-y"), "ih-y".to_string()),
    ];
    let active = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));
    let a = active.clone();
    let hw = high_water.clone();
    run_planned_checks(
        db.clone(),
        "project-a",
        "tree-c",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |_index, _command, _stream_id| {
            let a = a.clone();
            let hw = hw.clone();
            async move {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                hw.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                a.fetch_sub(1, Ordering::SeqCst);
                exec_ok(Some(0), String::new())
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(
        high_water.load(Ordering::SeqCst),
        1,
        "shared-mode checks must not overlap; exactly one may run at a time"
    );
}

#[tokio::test]
async fn shared_mode_exposes_earlier_mutations_across_cache_hit_and_failure() {
    let db = cache_db().await;
    let plans = vec![
        (plan("formatter", "format"), "ih-format".to_string()),
        (plan("cached", "cached"), "ih-cached".to_string()),
        (plan("reader", "read"), "ih-read".to_string()),
    ];
    record_fresh_check_observation(
        db.clone(),
        reusable_observation_for(
            "cached",
            "ih-cached",
            current_check_environment_fingerprint(),
        ),
    )
    .unwrap();

    let checkout = Arc::new(tempfile::tempdir().unwrap());
    let marker = checkout.path().join("formatted.txt");
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let marker_for_run = marker.clone();
    let observed_for_run = observed.clone();
    let results = run_planned_checks(
        db,
        "project-a",
        "tree-shared",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Shared,
        None,
        move |index, _command, _stream_id| {
            let marker = marker_for_run.clone();
            let observed = observed_for_run.clone();
            async move {
                observed.lock().unwrap().push(index);
                match index {
                    0 => {
                        std::fs::write(&marker, "settled formatter output").unwrap();
                        exec_ok(Some(1), "formatter reported a failure")
                    }
                    2 => {
                        let contents = std::fs::read_to_string(&marker)
                            .expect("later check sees the earlier in-place mutation");
                        assert_eq!(contents, "settled formatter output");
                        exec_ok(Some(0), contents)
                    }
                    _ => panic!("the cached plan must not execute"),
                }
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(*observed.lock().unwrap(), vec![0, 2]);
    assert!(
        !results[0].passed,
        "a failed mutating check is still recorded"
    );
    assert!(results[1].cached, "the middle plan remains a cache hit");
    assert!(results[2].passed);
    assert_eq!(
        std::fs::read_to_string(marker).unwrap(),
        "settled formatter output",
        "the mutation remains in the real checkout for the canonical fold"
    );
}

/// In `Isolated` mode the misses run CONCURRENTLY. Each executor signals
/// entry into a 2-party barrier and then awaits the other; both must be
/// in-flight at once for the barrier to release. Sequential execution would
/// park the first executor forever and trip the timeout, so a pass proves
/// genuine overlap.
#[tokio::test]
async fn isolated_checks_run_concurrently() {
    let db = cache_db().await;
    let plans = vec![
        (plan("x", "cmd-x"), "ih-x".to_string()),
        (plan("y", "cmd-y"), "ih-y".to_string()),
    ];
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let b = barrier.clone();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-iso",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            None,
            move |_index, _command, _stream_id| {
                let b = b.clone();
                async move {
                    b.wait().await;
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
        ),
    )
    .await;

    let results = outcome.expect(
        "isolated checks must run concurrently; the rendezvous timed out (ran sequentially?)",
    );
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|o| o.passed));
}

/// Concurrent misses may complete out of plan order (here the later-in-plan
/// checks finish FIRST), but the runner reassembles outcomes into plan order.
#[tokio::test]
async fn isolated_results_reassemble_into_plan_order() {
    let db = cache_db().await;
    let plans = vec![
        (plan("a", "cmd-a"), "ih-a".to_string()),
        (plan("b", "cmd-b"), "ih-b".to_string()),
        (plan("c", "cmd-c"), "ih-c".to_string()),
    ];
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-rev",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Isolated,
        None,
        // index 0 sleeps longest, so completion order reverses plan order.
        move |index, _command, _stream_id| async move {
            let delay = (3 - index as u64) * 20;
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            exec_ok(Some(0), String::new())
        },
        |_| {},
    )
    .await;

    let names: Vec<&str> = results.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["a", "b", "c"],
        "results must reassemble into plan order despite reversed completion"
    );
}

/// Concurrent transitions still emit FULL snapshots, and the last snapshot has
/// every entry in a final (passed/failed) state.
#[tokio::test]
async fn isolated_concurrent_transitions_end_all_final() {
    let db = cache_db().await;
    let plans = vec![
        (plan("x", "cmd-x"), "ih-x".to_string()),
        (plan("y", "cmd-y"), "ih-y".to_string()),
        (plan("z", "cmd-z"), "ih-z".to_string()),
    ];
    let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = snapshots.clone();
    run_planned_checks(
        db.clone(),
        "project-a",
        "tree-snap",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Isolated,
        None,
        // z fails, the others pass — mixed final states under concurrency.
        move |index, _command, _stream_id| async move {
            let code = if index == 2 { 1 } else { 0 };
            exec_ok(Some(code), String::new())
        },
        move |checks| captured.lock().unwrap().push(checks),
    )
    .await;

    let snaps = snapshots.lock().unwrap();
    assert!(
        snaps.iter().all(|s| s.len() == 3),
        "every snapshot carries the full checklist"
    );
    let last = snaps.last().expect("at least the planned snapshot emitted");
    assert!(
        last.iter()
            .all(|e| e.state == "passed" || e.state == "failed"),
        "the final snapshot must have every entry final: {last:?}"
    );
    assert_eq!(find(last, "z").state, "failed");
}

/// `Isolated` mode runs its misses concurrently. Capping that concurrency is
/// the executor's job, reached through the reservation each submission
/// declares — there is deliberately no second cap here, so all three misses
/// overlap.
#[tokio::test]
async fn isolated_misses_run_concurrently() {
    let db = cache_db().await;
    let plans = vec![
        (plan("a", "cmd-a"), "ih-a".to_string()),
        (plan("b", "cmd-b"), "ih-b".to_string()),
        (plan("c", "cmd-c"), "ih-c".to_string()),
    ];
    let active = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));
    let a = active.clone();
    let hw = high_water.clone();
    let results = run_planned_checks(
        db.clone(),
        "project-a",
        "tree-bounded",
        "job-a",
        &plans,
        "tool",
        CheckExecMode::Isolated,
        None,
        move |_index, _command, _stream_id| {
            let a = a.clone();
            let hw = hw.clone();
            async move {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                hw.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                a.fetch_sub(1, Ordering::SeqCst);
                exec_ok(Some(0), String::new())
            }
        },
        |_| {},
    )
    .await;

    assert_eq!(results.len(), 3);
    assert_eq!(
        high_water.load(Ordering::SeqCst),
        3,
        "isolated misses must not be serialized by a runner-local gate"
    );
}

fn parsed(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
    ParsedCheckResult {
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: parser.to_string(),
        passed,
        failed,
        skipped: 0,
        suite_failures: 0,
        phases: None,
        deferred: 0,
        report: None,
        failures: Vec::new(),
        unverified: None,
        lint_findings: None,
        plan_audit: None,
        accounting: None,
        ingestion: None,
    }
}

/// A daemon whose grant does not cover this runner still answers, and is
/// unfit only for the roots it was not granted: builds there compile
/// directly. Refusing check dispatch over it would turn a cache miss into
/// a machine-wide outage on every runner upgrade, since no daemon running
/// before grant records existed has one.
#[tokio::test]
#[serial_test::serial(build_service_port)]
async fn a_stale_grant_never_refuses_check_dispatch() {
    let (snapshot, _harness) =
        crate::orchestrator::build_services::tests::stale_grant_reconciled_without_destroy().await;
    assert!(
        snapshot.runtime.grant_stale.is_some(),
        "the fixture must be the stale-grant state: {snapshot:?}"
    );
    assert_eq!(active_build_service_failure(&snapshot), None);
}

#[test]
fn active_build_service_failure_preflights_without_relabeling_spawn_outcomes() {
    let config_fingerprint = "active-config".to_string();
    let snapshot = crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot {
        name: "sccache".to_string(),
        configured: true,
        enabled: true,
        supervised_child: false,
        config_fingerprint: Some(config_fingerprint.clone()),
        state_dir: None,
        error_log_tail: Some("historical log entry".to_string()),
        runtime: crate::orchestrator::build_services::BuildServiceRuntimeDiagnostic {
            current_failure: Some(
                "sccache: error: Address already in use (os error 48)".to_string(),
            ),
            failure_config: Some(config_fingerprint),
            ..Default::default()
        },
    };
    assert_eq!(
        active_build_service_failure(&snapshot).as_deref(),
        Some("sccache port conflict: sccache: error: Address already in use (os error 48)")
    );

    let mut disabled = snapshot.clone();
    disabled.enabled = false;
    assert_eq!(active_build_service_failure(&disabled), None);

    let mut replaced = snapshot.clone();
    replaced.config_fingerprint = Some("replacement-config".to_string());
    assert_eq!(active_build_service_failure(&replaced), None);

    let outcome = CellOutcome::Unavailable {
        reason: cairn_common::executor_protocol::CellUnavailableReason::Spawn,
        diagnostic: "sandbox denial cannot be adjudicated without runner context".to_string(),
    };
    let failure =
        check_result_from_cell_outcome(outcome, &crate::fleet::CellAccount::default(), None)
            .unwrap_err();
    assert_eq!(
        failure,
        CheckExecutionFailure::substrate(
            SubstrateFailureShape::Dispatch,
            "Spawn: sandbox denial cannot be adjudicated without runner context"
        )
    );
}

#[test]
fn failure_classifier_requires_positive_evidence_and_preserves_precedence() {
    // A command with no recognized runner, so the Vitest arms stay clear of
    // the infrastructure-evidence precedence under test.
    let opaque = "bun run check:web";
    let warning = "sccache: warning: The server looks like it shut down unexpectedly";
    assert_eq!(
        classify_check_failure(opaque, Some(0), false, false, None, warning),
        None
    );
    assert_eq!(
        classify_check_failure(opaque, Some(254), false, false, None, "script exited 254"),
        None
    );

    let abnormal = "error: process didn't exit successfully: sccache rustc --crate-name bytes (exit status: 254)";
    assert_eq!(
        classify_check_failure(opaque, Some(254), false, false, None, abnormal)
            .map(|classification| classification.kind),
        Some(CheckFailureKind::Infrastructure)
    );
    let corruption = "sccache: error: memory map must have a non-zero length";
    let classification = classify_check_failure(opaque, Some(1), false, false, None, corruption)
        .expect("reviewed cache corruption must be classified");
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert!(classification.cache_corruption);
    assert!(classification.reason.contains("cache-read corruption"));
    for signature in [
        "Failed to send data to or receive data from server",
        "failed client/server communication",
        "failed to fill whole buffer",
        "server looks like it shut down unexpectedly",
    ] {
        assert_eq!(
            classify_check_failure(opaque, Some(1), false, false, None, signature)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure),
            "signature: {signature}"
        );
    }
    for near_miss in ["graph node: not found", "registry cargo: not found"] {
        assert_eq!(
            classify_check_failure(opaque, Some(1), false, false, None, near_miss),
            None,
            "near miss: {near_miss}"
        );
    }
    for signature in [
        "sccache: encountered fatal error: failed to create output file",
        "error: couldn't create a temp dir: Operation not permitted (os error 1) at path /Users/mitch/.cairn/build-slots/cairn/slot-542/src-tauri/target/debug/deps/.tmp123456",
        "Operation not permitted (os error 1) at /Users/dev/.cairn-exec/build-slots/cairn/slot-5/src-tauri/target/debug/deps/.tmp123456",
    ] {
        assert_eq!(
            classify_check_failure(opaque, Some(1), false, false, None, signature)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure),
            "signature: {signature}"
        );
    }
    assert_eq!(
        classify_check_failure(
            opaque,
            Some(75),
            false,
            false,
            None,
            "CAIRN_INFRASTRUCTURE_EXIT: clippy exited 1 without a compiler diagnostic"
        )
        .map(|classification| classification.kind),
        Some(CheckFailureKind::Infrastructure)
    );
    for (marker, exit, expected) in [
        (
            "CAIRN_ENVIRONMENT_EXIT: clippy exited 101 on bglab-sp1\nCause (verbatim):\ncannot find -lglib-2.0",
            1,
            CheckFailureKind::Environment,
        ),
        (
            "CAIRN_UNCLASSIFIED_EXIT: clippy exited 1 on runner-a\nCause (verbatim):\nprocess exited without explanation",
            1,
            CheckFailureKind::Unclassified,
        ),
        (
            "CAIRN_SELECTION_EXIT: cargo nextest exited 4 on runner-a\nCause (verbatim):\nno tests matched",
            1,
            CheckFailureKind::Selection,
        ),
        (
            "CAIRN_PREREQUISITE_EXIT: sidecar build exited 101 on fedora\nCause (verbatim):\nerror[E0603]: function `unix_time_ms` is private",
            101,
            CheckFailureKind::Prerequisite,
        ),
    ] {
        assert_eq!(
            classify_check_failure(opaque, Some(exit), false, false, None, marker)
                .map(|classification| classification.kind),
            Some(expected)
        );
    }
    let marker_in_cause = "CAIRN_UNCLASSIFIED_EXIT: clippy exited 1 on runner-a\nCause (verbatim):\nsource printed CAIRN_ENVIRONMENT_EXIT: but supplied no environment headline";
    let mut classification =
        classify_check_failure(opaque, Some(1), false, false, None, marker_in_cause);
    assert_eq!(
        classification
            .as_ref()
            .map(|classification| classification.kind),
        Some(CheckFailureKind::Unclassified),
        "verbatim stderr must not override the producer-owned headline"
    );
    attach_executor_provenance(&mut classification, Some("bglab-sp1"));
    assert!(
        classification
            .expect("marker classifies")
            .reason
            .contains("bglab-sp1"),
        "the consumer must attach canonical executor provenance"
    );

    let missing =
        "couldn't read target/debug/build/tree/out/generated.txt: No such file or directory";
    assert_eq!(
        classify_check_failure(opaque, Some(1), false, false, None, missing)
            .map(|classification| classification.kind),
        Some(CheckFailureKind::Infrastructure)
    );
    for signature in [
        "/usr/bin/env: 'node': No such file or directory",
        "sh: bun: command not found",
        "bash: npm: command not found",
        "bash: line 1: cargo: command not found",
        "zsh: command not found: node",
        "'node' is not recognized as an internal or external command",
        "The term 'bun' is not recognized as the name of a cmdlet, function, script file, or operable program.",
    ] {
        assert_eq!(
            classify_check_failure(opaque, Some(1), false, false, None, signature)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure),
            "signature: {signature}"
        );
    }
    assert_eq!(
        classify_check_failure(
            VITEST_COMMAND,
            Some(1),
            false,
            false,
            Some(&parsed("vitest", 2, 1)),
            warning
        ),
        None,
        "real assertion failures outrank incidental infrastructure text"
    );
    assert_eq!(
        classify_check_failure(
            VITEST_COMMAND,
            Some(1),
            false,
            false,
            Some(&parsed("vitest", 2, 1)),
            corruption
        ),
        None,
        "a named test failure outranks cache-corruption text in scrollback"
    );
}

const VITEST_COMMAND: &str = "bunx vitest related --reporter=default --reporter=json src/a.ts";

/// Passing rows of `scripts/lib/rust-process-outcome.test.ts` as a sharded
/// frontend run prints them: each test is named after a transport signature
/// the classifier matches.
const ROWS_NAMED_AFTER_SIGNATURES: &str = concat!(
    " \x1b[32m✓\x1b[39m \x1b[30m\x1b[46m scripts \x1b[49m\x1b[39m scripts/lib/rust-process-outcome.test.ts\x1b[2m > \x1b[22mRust process outcomes\x1b[2m > \x1b[22mdoes not let Cargo's compile summary hide cache infrastructure failure: Failed to send data to or receive data from server: Connection reset by peer (os error 54)\x1b[32m 0\x1b[2mms\x1b[22m\x1b[39m\n",
    " ✓ scripts/lib/rust-process-outcome.test.ts > Rust process outcomes > does not let Cargo's compile summary hide cache infrastructure failure: failed client/server communication 0ms\n",
    " ✓ scripts/lib/rust-process-outcome.test.ts > Rust process outcomes > does not let Cargo's compile summary hide cache infrastructure failure: failed to fill whole buffer 0ms\n",
);

const FRONTEND_COMMAND: &str = "bun run test:frontend:scoped";

#[test]
fn a_passing_test_named_after_an_infrastructure_signature_is_not_evidence() {
    // Vitest failed the run over an error that escaped a test file; no
    // assertion failed. That is a runner error, never the build host's.
    let output = format!(
        "{ROWS_NAMED_AFTER_SIGNATURES}\n Unhandled Errors \n\
             Vitest caught 1 unhandled error during the test run.\n\
             ReferenceError: window is not defined\n\
             error: script \"test:frontend:scoped\" exited with code 1\n\nExit code: 1"
    );
    let classification = classify_check_failure(
        FRONTEND_COMMAND,
        Some(1),
        false,
        false,
        Some(&parsed("vitest", 4210, 0)),
        &output,
    )
    .expect("an abnormal Vitest exit");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);
    assert!(
        classification.reason.contains("unhandled") || classification.reason.contains("escaped"),
        "{}",
        classification.reason
    );
    // A command no runner arm claims stays an ordinary failure.
    assert_eq!(
        classify_check_failure(
            "bun run check:web",
            Some(1),
            false,
            false,
            None,
            ROWS_NAMED_AFTER_SIGNATURES
        ),
        None
    );
}

/// cairn/5888 (tenet 707): a marker the run quotes is not the marker a
/// script wrote. `rust-process-outcome.test.ts` names its cases after the
/// markers and its fixtures print them, so a run quoting one is classified
/// as whatever it actually was; only the step's exit with the line it
/// wrote is a prerequisite failure.
#[test]
fn a_quoted_producer_marker_never_classifies_the_run() {
    let fixture = "CAIRN_PREREQUISITE_EXIT: sidecar build exited 101 on fedora\n\
             Cause (verbatim):\n\
             error[E0603]: function `unix_time_ms` is private";
    let passing_row = " ✓ scripts/lib/rust-process-outcome.test.ts > prerequisiteFailureReport > \
             CAIRN_PREREQUISITE_EXIT: sidecar build exited 1 on fedora 0ms\n\
             Cause (verbatim):";

    // A Vitest run that failed over an escaped error, quoting the marker in
    // a passing row and in a fixture it echoed: a runner error.
    let output = format!(
        "{passing_row}\n{fixture}\n Unhandled Errors \n\
             ReferenceError: window is not defined\n\
             error: script \"test:frontend:scoped\" exited with code 1"
    );
    let classification = classify_check_failure(
        FRONTEND_COMMAND,
        Some(1),
        false,
        false,
        Some(&parsed("vitest", 4210, 0)),
        &output,
    )
    .expect("an abnormal Vitest exit");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);

    // An ordinary red that echoed the fixture: the run did not exit with
    // the code the quoted line states, so it is the suite's own red.
    let command = "bun run codex:pin:check";
    let output =
        format!("{passing_row}\n{fixture}\nerror: script \"codex:pin:check\" exited with code 1");
    assert_eq!(
        classify_check_failure(command, Some(1), false, false, None, &output),
        None
    );
    assert!(check_reuse_decision(command, None, None).reusable);

    // The marker inside a log line is a quotation even under the exit it
    // states.
    let quoted = "docs/checks.md: the step prints CAIRN_PREREQUISITE_EXIT: sidecar build exited 101 on fedora\n\
             Cause (verbatim):\n\
             error: script \"codex:pin:check\" exited with code 101";
    assert_eq!(
        classify_check_failure(command, Some(101), false, false, None, quoted),
        None
    );

    // The step's own exit with the line it wrote is the prerequisite
    // failure, and never the suite's reusable verdict.
    let real =
        format!("{passing_row}\n{fixture}\nerror: script \"codex:pin:check\" exited with code 101");
    let classification =
        classify_check_failure(command, Some(101), false, false, None, &real).unwrap();
    assert_eq!(classification.kind, CheckFailureKind::Prerequisite);
    assert_eq!(
        real.lines().nth(classification.evidence_line.unwrap()),
        Some("CAIRN_PREREQUISITE_EXIT: sidecar build exited 101 on fedora"),
        "the evidence is the line the step wrote, not the passing row"
    );
    assert!(!check_reuse_decision(command, Some(classification.kind), None).reusable);

    // The infrastructure and handoff markers are read the same way: the
    // exit they own and the line opening with them.
    for (marker, exit) in [
        ("CAIRN_INFRASTRUCTURE_EXIT", 75),
        ("CAIRN_HANDOFF_FAILED_EXIT", 74),
    ] {
        let quoted = format!("note: a script exits {exit} after printing {marker}: its reason");
        assert_eq!(
            classify_check_failure(command, Some(exit), false, false, None, &quoted),
            None,
            "{marker} quoted mid-line"
        );
        let written = format!("{marker}: its reason");
        assert_eq!(
            classify_check_failure(command, Some(exit), false, false, None, &written)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure),
            "{marker} as its script writes it"
        );
    }
}

#[test]
fn a_named_failing_test_is_an_ordinary_failure_without_a_structured_result() {
    // The cairn/4209 specimen's shape: genuine assertion failures, named in
    // the run's closing report, beside passing rows that read as transport
    // failure, and no parsed result to say so.
    let output = format!(
        "{ROWS_NAMED_AFTER_SIGNATURES}\n\
             \x20  FAIL  scripts/field-guide.test.ts > field guide > keeps generated outputs current and deterministic\n\
             \x20  FAIL  scripts/field-guide.test.ts > field guide > builds publish-clean pages while preserving source metadata\n\n\
             error: script \"test:frontend:scoped\" exited with code 1\n\nExit code: 1"
    );
    assert_eq!(
        classify_check_failure(FRONTEND_COMMAND, Some(1), false, false, None, &output),
        None
    );

    // Every runner's failing row outranks infrastructure text, as a parsed
    // failure does.
    for row in [
        "(fail) api > rejects a stale token [0.20ms]",
        "        FAIL [   0.088s] (3/4) cairn-core execution::tests::breaks",
        "test execution::tests::breaks ... FAILED",
    ] {
        let output = format!("Failed to send data to or receive data from server\n{row}\n");
        assert_eq!(
            classify_check_failure("bun run check:web", Some(1), false, false, None, &output),
            None,
            "{row}"
        );
    }

    // A script's own verdict glyph names no test, so the infrastructure
    // evidence beside it still stands.
    let output =
        "✗ clippy: 2 findings\nsccache: encountered fatal error: failed to create output file\n";
    let classification =
        classify_check_failure("bun run check:web", Some(1), false, false, None, output)
            .expect("positive infrastructure evidence");
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert_eq!(classification.evidence_line, Some(1));
}

/// A parse shaped like Vitest's when a test FILE failed to collect: no
/// assertion failed, so `failed` stays zero while the file itself is a named
/// failure site.
fn suite_failed(passed: usize, suites: usize) -> ParsedCheckResult {
    let mut result = parsed("vitest", passed, 0);
    result.suite_failures = suites;
    result.failures = (0..suites)
        .map(|i| crate::execution::check_parsers::CheckFailure {
            name: format!("src/s{i}.test.tsx"),
            message: Some("Cannot find module './missing'".to_string()),
        })
        .collect();
    result
}

#[test]
fn runner_error_is_vitest_only_and_reports_progress() {
    let vitest = parsed("vitest", 12, 0);
    let classification = classify_check_failure(
        VITEST_COMMAND,
        Some(1),
        false,
        false,
        Some(&vitest),
        "report complete",
    )
    .expect("abnormal Vitest exit");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);
    assert!(classification.reason.contains("12 tests passed"));
    assert_eq!(
        classify_check_failure(
            "bun run test:rust",
            Some(101),
            false,
            false,
            Some(&parsed("nextest", 0, 0)),
            "compile error"
        ),
        None
    );
}

#[test]
fn a_suite_that_failed_to_collect_is_an_ordinary_named_failure() {
    // The file failed; no assertion did. Calling this a runner error would
    // bury the only thing that explains the red check (the file name and the
    // resolver error under it) behind "no assertion failures".
    assert_eq!(
        classify_check_failure(
            VITEST_COMMAND,
            Some(1),
            false,
            false,
            Some(&suite_failed(881, 1)),
            "FAIL src/s0.test.tsx"
        ),
        None
    );
}

#[test]
fn an_error_escaping_a_test_file_is_named_as_such() {
    // Vitest's own JSON calls this run a success (every count clean,
    // `success` true) while the process exits 1. The only evidence is in the
    // output, so the classification has to point the excerpt at it.
    let output = concat!(
        "881 passed\n",
        " Unhandled Errors \n",
        "Vitest caught 2 unhandled errors during the test run.\n",
        "ReferenceError: window is not defined\n",
        "  at resolveUpdatePriority react-dom-client.development.js:1308:7\n"
    );
    let classification = classify_check_failure(
        VITEST_COMMAND,
        Some(1),
        false,
        false,
        Some(&parsed("vitest", 881, 0)),
        output,
    )
    .expect("unhandled errors fail the run");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);
    assert!(classification.reason.contains("escaped a test file"));
    let excerpt = classified_output_excerpt(output, Some(&classification), None);
    assert!(
        excerpt.contains("window is not defined"),
        "the excerpt must carry the escaping error: {excerpt}"
    );
}

#[test]
fn a_shard_whose_report_never_arrived_is_a_runner_error_not_a_zero_failure_red() {
    // What scripts/test-frontend.ts hands back when a shard's vitest process
    // died before writing its JSON report (and so before printing its FAIL
    // block): an incomplete result naming no failure, and a tail that says
    // which shard reported nothing. Captured from a constructed run.
    let result = r#"{"schemaVersion":1,"parser":"vitest","complete":false,"selection":"full","tests":[{"id":"scripts/b.test.ts > leaks a rejection","status":"passed","attempts":1,"retried":false,"flaky":false}],"undeclaredSkips":0}"#;
    let output = format!(
        "{RESULT_ARTIFACT_JSON_MARKER}{result}\nstderr:\n\n\u{274c} frontend tests failed\n\n\
             \x20  no failing test was named \u{2014} a shard failed to collect, matched\n\
             \x20  no tests, or errored outside a test body. Rerun and read its output.\n\n\
             \x20  shard scripts 1/1 wrote no readable JSON report; its other tests have no result rows.\n\n\
             \x20  rerun: bun run test:frontend scripts/b.test.ts scripts/e.test.ts\n\n\
             error: script \"test:frontend:scoped\" exited with code 1\n\nExit code: 1"
    );
    let parsed = parse_check_output(FRONTEND_COMMAND, &output).expect("the runner's result");
    assert!(!parsed.complete);
    assert_eq!((parsed.failed, parsed.suite_failures), (0, 0));
    let classification = classify_check_failure(
        FRONTEND_COMMAND,
        Some(1),
        false,
        false,
        Some(&parsed),
        &output,
    )
    .expect("a red with nothing named is not an ordinary failure");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);
    assert!(classification.kind.is_infrastructure());
}

/// Identities of `scripts/lib/rust-process-outcome.test.ts` as a structured
/// result carries them: each names a transport signature the classifier
/// matches in free text.
const SIGNATURE_IDS: [&str; 3] = [
    "scripts/lib/rust-process-outcome.test.ts > Rust process outcomes > does not let Cargo's compile summary hide cache infrastructure failure: Failed to send data to or receive data from server: Connection reset by peer (os error 54)",
    "scripts/lib/rust-process-outcome.test.ts > Rust process outcomes > does not let Cargo's compile summary hide cache infrastructure failure: failed client/server communication",
    "scripts/lib/rust-process-outcome.test.ts > Rust process outcomes > does not let Cargo's compile summary hide cache infrastructure failure: failed to fill whole buffer",
];

/// The duplicate identity cairn/6196 found in `src/lib/diffViewer.test.ts`.
const COLLIDING_ID: &str =
    "src/lib/diffViewer.test.ts > diffViewer > preserves modified and zero-count hunk coordinates";

/// The executor's structured-result line for `rows` of `parser`.
fn result_envelope(parser: &str, rows: &[(&str, &str)], complete: bool) -> String {
    let tests: Vec<serde_json::Value> = rows
        .iter()
        .map(|(id, status)| {
            serde_json::json!({
                "id": id, "status": status, "attempts": 1, "retried": false, "flaky": false
            })
        })
        .collect();
    let result = serde_json::json!({
        "schemaVersion": 1, "parser": parser, "complete": complete,
        "selection": "full", "tests": tests, "undeclaredSkips": 0
    });
    format!("{RESULT_ARTIFACT_JSON_MARKER}{result}")
}

fn passing_signature_rows() -> Vec<(&'static str, &'static str)> {
    SIGNATURE_IDS.iter().map(|id| (*id, "passed")).collect()
}

#[test]
fn a_structured_result_is_never_free_text_infrastructure_evidence() {
    // Observation 820cbc1a's shape: every row passed, two rows share an
    // identity so the result is incomplete, the run exited 1, and the
    // passing identities quote all three transport signatures.
    let mut rows = passing_signature_rows();
    rows.extend([(COLLIDING_ID, "passed"), (COLLIDING_ID, "passed")]);
    let envelope = result_envelope("vitest", &rows, true);
    let tail = "error: script \"test:frontend:scoped\" exited with code 1\n\nExit code: 1";
    let output = format!(" JSON report written to /tmp/cairn-vitest-a/0.json\n{envelope}\n{tail}");
    let parsed = parse_check_output(FRONTEND_COMMAND, &output).expect("the runner's result");
    assert!(
        !parsed.complete,
        "a duplicate identity keeps the result incomplete"
    );
    assert_eq!(
        (parsed.passed, parsed.failed, parsed.suite_failures),
        (4, 0, 0)
    );
    let classification = classify_check_failure(
        FRONTEND_COMMAND,
        Some(1),
        false,
        false,
        Some(&parsed),
        &output,
    )
    .expect("a nonzero exit naming no failure is not an ordinary failure");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);
    assert_ne!(classification.evidence_line, Some(1));
    assert!(
        classification
            .reason
            .contains("repeat an earlier test identity")
            && classification.reason.contains(COLLIDING_ID),
        "the reason names why the result is incomplete: {}",
        classification.reason
    );

    // An envelope the canonical parser cannot read is still the envelope,
    // not a diagnostic the process printed.
    let truncated = {
        let end = envelope.find("failed to fill whole buffer").unwrap()
            + "failed to fill whole buffer".len();
        envelope[..end].to_string()
    };
    let unknown_schema = envelope.replace("\"schemaVersion\":1", "\"schemaVersion\":9");
    assert_ne!(unknown_schema, envelope);
    for envelope in [truncated, unknown_schema] {
        let output = format!("vitest run\n{envelope}\n{tail}");
        let parsed = parse_check_output(FRONTEND_COMMAND, &output);
        assert!(parsed.is_none(), "{envelope}");
        let classification =
            classify_check_failure(FRONTEND_COMMAND, Some(1), false, false, None, &output);
        assert!(
            classification.as_ref().is_none_or(|classification| {
                classification.kind != CheckFailureKind::Infrastructure
                    && classification.evidence_line != Some(1)
            }),
            "{classification:?}"
        );
    }

    // Nor does a quoted signature sign a host-ended compile beside a
    // cargo crate failure.
    let command = "cargo test --workspace";
    let envelope = result_envelope(
        "nextest",
        &[(
            "cairn-core execution::tests::a_rustc_ended_by (signal: 9, SIGKILL: kill)",
            "passed",
        )],
        false,
    );
    let output = format!("error: could not compile `cairn-core` (lib test)\n{envelope}");
    let parsed = parse_check_output(command, &output);
    assert_eq!(
        classify_check_failure(command, Some(101), false, false, parsed.as_ref(), &output),
        None
    );
}

#[test]
fn evidence_beside_a_structured_result_still_classifies() {
    let tail = "error: script \"test:frontend:scoped\" exited with code 1";

    // An infrastructure diagnostic the process printed on its own line.
    let envelope = result_envelope("vitest", &passing_signature_rows(), false);
    let output = format!(
        "{envelope}\nsccache: caused by: Failed to send data to or receive data from server\n{tail}"
    );
    let parsed = parse_check_output(FRONTEND_COMMAND, &output).expect("the runner's result");
    let classification = classify_check_failure(
        FRONTEND_COMMAND,
        Some(1),
        false,
        false,
        Some(&parsed),
        &output,
    )
    .expect("a printed transport failure");
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert_eq!(classification.evidence_line, Some(1));

    // A script's explicit marker keeps its exit-plus-start-of-line contract.
    let output = format!("{envelope}\nCAIRN_INFRASTRUCTURE_EXIT: runner lost its cell\n{tail}");
    let classification = classify_check_failure(
        FRONTEND_COMMAND,
        Some(75),
        false,
        false,
        Some(&parsed),
        &output,
    )
    .expect("the script's own marker");
    assert_eq!(classification.kind, CheckFailureKind::Infrastructure);
    assert_eq!(classification.evidence_line, Some(1));

    // A named assertion failure stays an ordinary, visible failure even
    // when its identity quotes a signature.
    let mut rows = passing_signature_rows();
    rows[2].1 = "failed";
    let envelope = result_envelope("vitest", &rows, true);
    let output = format!("{envelope}\n{tail}");
    let parsed = parse_check_output(FRONTEND_COMMAND, &output).expect("the runner's result");
    assert_eq!(parsed.failed, 1);
    assert_eq!(parsed.failures[0].name, SIGNATURE_IDS[2]);
    assert_eq!(
        classify_check_failure(
            FRONTEND_COMMAND,
            Some(1),
            false,
            false,
            Some(&parsed),
            &output
        ),
        None
    );
}

#[test]
fn vitest_exiting_without_a_report_is_named_a_runner_error() {
    // No JSON object at all: Vitest died loading its config or its deps, so
    // nothing was ever collected. Left unclassified this renders as a bare
    // `exit 1` beside a tail of module-resolution noise.
    let output = concat!(
        "failed to load config from /slot-176/vitest.config.ts\n",
        "Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'vitest'\n",
        "Exit code: 1\n"
    );
    let classification =
        classify_check_failure(VITEST_COMMAND, Some(1), false, false, None, output)
            .expect("a Vitest run that produced no report");
    assert_eq!(classification.kind, CheckFailureKind::RunnerError);
    assert!(classification.reason.contains("without producing a report"));
}

#[test]
fn review_batch_selectors_union_toolchains_and_reject_scalar_conflicts() {
    let item = |index, executor| PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: format!("check-{index}"),
        input_hash: format!("hash-{index}"),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: format!("resource-{index}"),
            command: format!("resource-{index}"),
        },
        command: "true".into(),
        stream_id: format!("stream-{index}"),
        env: Vec::new(),
        timeout_ms: 1,
        executor: Some(executor),
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        infra_scopes: InfraScopes::head_only("job", "commit"),
        avoid_executor_ids: Vec::new(),
    };
    let merged = merge_batch_executor(&[
        item(
            0,
            ExecutorSelector {
                os: Some("linux".into()),
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            },
        ),
        item(
            1,
            ExecutorSelector {
                os: Some("linux".into()),
                required_toolchains: vec!["bun".into(), "rust".into()],
                ..ExecutorSelector::default()
            },
        ),
    ])
    .unwrap()
    .unwrap();
    assert_eq!(merged.os.as_deref(), Some("linux"));
    assert_eq!(merged.required_toolchains, vec!["bun", "rust"]);

    let conflict = merge_batch_executor(&[
        item(
            0,
            ExecutorSelector {
                name: Some("bglab-ub".into()),
                ..ExecutorSelector::default()
            },
        ),
        item(
            1,
            ExecutorSelector {
                name: Some("bglab-mac".into()),
                ..ExecutorSelector::default()
            },
        ),
    ])
    .unwrap_err();
    assert!(conflict.contains("conflicting review check executor selector name"));

    // A named machine and a bare platform are different questions, and one
    // batch cannot honor both.
    let mixed = merge_batch_executor(&[
        item(
            0,
            ExecutorSelector {
                name: Some("bglab-ub".into()),
                ..ExecutorSelector::default()
            },
        ),
        item(
            1,
            ExecutorSelector {
                os: Some("linux".into()),
                ..ExecutorSelector::default()
            },
        ),
    ])
    .unwrap_err();
    assert!(mixed.contains("conflicting executor selectors"), "{mixed}");
}

/// The cross-pattern. Two checks in one wave, each suppressed on a
/// DIFFERENT machine: there is no single placement that satisfies both, so
/// the wave splits and each half goes where it is clear. Merging them into
/// an avoid-union would strand both.
#[test]
fn disjoint_environment_suppressions_split_a_batch() {
    let item = |index: usize, avoid: &str| PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: format!("check-{index}"),
        input_hash: format!("hash-{index}"),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: format!("resource-{index}"),
            command: format!("resource-{index}"),
        },
        command: "true".into(),
        stream_id: format!("stream-{index}"),
        env: Vec::new(),
        timeout_ms: 1,
        executor: None,
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        infra_scopes: InfraScopes {
            head: "head".into(),
            platform_heads: Vec::new(),
            fleet: None,
            environments: vec![
                ("machine-a".into(), "env-a".into()),
                ("machine-b".into(), "env-b".into()),
            ],
            fleet_blind: Vec::new(),
            lane_breaker: false,
        },
        avoid_executor_ids: vec![avoid.to_string()],
    };

    let groups =
        partition_check_items_by_placement(vec![item(0, "machine-a"), item(1, "machine-b")]);
    assert_eq!(
        groups.len(),
        2,
        "checks that disagree about where they may NOT go cannot share one cell"
    );
    assert_eq!(groups[0].0.avoid, vec!["machine-a".to_string()]);
    assert_eq!(groups[1].0.avoid, vec!["machine-b".to_string()]);

    // And each half still has somewhere to go.
    for (_, items) in &groups {
        assert!(merge_batch_avoid(items).unwrap().len() < 2);
    }
}

/// A batch that cannot split and has no machine clear for all of it refuses
/// rather than placing an item on a bounded environment. Suppression that a
/// later placement can override is advisory, and this one may not be.
#[test]
fn a_batch_with_no_common_machine_refuses_rather_than_placing() {
    let item = |index: usize, avoid: &str| PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: format!("check-{index}"),
        input_hash: format!("hash-{index}"),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: format!("resource-{index}"),
            command: format!("resource-{index}"),
        },
        command: "true".into(),
        stream_id: format!("stream-{index}"),
        env: Vec::new(),
        timeout_ms: 1,
        executor: None,
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        infra_scopes: InfraScopes {
            head: "head".into(),
            platform_heads: Vec::new(),
            fleet: None,
            environments: vec![
                ("machine-a".into(), "env-a".into()),
                ("machine-b".into(), "env-b".into()),
            ],
            fleet_blind: Vec::new(),
            lane_breaker: false,
        },
        avoid_executor_ids: vec![avoid.to_string()],
    };
    let conflict = merge_batch_avoid(&[item(0, "machine-a"), item(1, "machine-b")])
        .expect_err("no machine is clear for both checks");
    assert!(
        conflict.contains("check-0") && conflict.contains("machine-a"),
        "the refusal names both conflicting suppressions: {conflict}"
    );
    assert!(
        conflict.contains("check-1") && conflict.contains("machine-b"),
        "the refusal names both conflicting suppressions: {conflict}"
    );

    // A machine clear for both is a placement, not a conflict.
    let mut wide = item(0, "machine-a");
    wide.infra_scopes
        .environments
        .push(("machine-c".into(), "env-c".into()));
    let mut other = item(1, "machine-b");
    other
        .infra_scopes
        .environments
        .push(("machine-c".into(), "env-c".into()));
    assert_eq!(
        merge_batch_avoid(&[wide, other]).unwrap(),
        vec!["machine-a".to_string(), "machine-b".to_string()]
    );
}

/// The normal case costs nothing. With no avoid sets anywhere, grouping is
/// byte-identical to what it was before avoidance existed.
#[test]
fn an_empty_avoid_set_partitions_exactly_as_before() {
    let item = |index: usize| PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: format!("check-{index}"),
        input_hash: format!("hash-{index}"),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: format!("resource-{index}"),
            command: format!("resource-{index}"),
        },
        command: "true".into(),
        stream_id: format!("stream-{index}"),
        env: Vec::new(),
        timeout_ms: 1,
        executor: None,
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        infra_scopes: InfraScopes::head_only("job", "commit"),
        avoid_executor_ids: Vec::new(),
    };
    let groups = partition_check_items_by_placement(vec![item(0), item(1), item(2)]);
    assert_eq!(groups.len(), 1, "one placement, one cell, no fan-out");
    assert!(merge_batch_avoid(&groups[0].1).unwrap().is_empty());
}

fn grouped_item(
    index: usize,
    name: &str,
    executor: Option<ExecutorSelector>,
    verdict_platforms: &[&str],
) -> PlannedCheckBatchItem {
    PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: name.into(),
        input_hash: format!("hash-{index}"),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: name.into(),
            command: name.into(),
        },
        command: "true".into(),
        stream_id: format!("stream-{index}"),
        env: Vec::new(),
        timeout_ms: 1,
        executor,
        verdict_platforms: verdict_platforms.iter().map(|p| p.to_string()).collect(),
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        infra_scopes: InfraScopes::head_only("job", "commit"),
        avoid_executor_ids: Vec::new(),
    }
}

fn rust_toolchains(nested_sandbox: bool) -> Option<ExecutorSelector> {
    Some(ExecutorSelector {
        required_toolchains: vec!["bun".into(), "rust".into()],
        requires_macos_nested_sandbox_write: nested_sandbox,
        ..Default::default()
    })
}

fn rust_cell_group() -> BTreeMap<String, Vec<String>> {
    BTreeMap::from([(
        "rust".to_string(),
        vec![
            "rust-lint".to_string(),
            "rust-tests".to_string(),
            "source-scans".to_string(),
        ],
    )])
}

/// The review wave's Rust lanes share one cell, in the group's declared
/// order whatever order the wave planned them in, on the intersection of
/// their platforms. rust-tests' nested-sandbox requirement means something
/// only on macOS, so the Linux group does not carry it (execution's
/// condition E1 on cairn/6771). The TS lane keeps its own cell.
#[test]
fn a_cell_group_is_one_batch_in_declared_order_on_the_shared_platform() {
    let (groups, fallbacks) = partition_check_items_with_cell_groups(
        vec![
            grouped_item(0, "source-scans", rust_toolchains(false), &["linux"]),
            grouped_item(1, "typecheck", None, &[]),
            grouped_item(2, "rust-tests", rust_toolchains(true), &["linux", "macos"]),
            grouped_item(3, "rust-lint", rust_toolchains(false), &["linux", "macos"]),
        ],
        &rust_cell_group(),
        &|_, _| None,
    );
    assert!(fallbacks.is_empty(), "{fallbacks:?}");
    assert_eq!(groups.len(), 2, "one Rust cell and one TS cell");
    let (placement, members) = &groups[0];
    assert_eq!(placement.cell_group.as_deref(), Some("rust"));
    assert_eq!(
        members
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["rust-lint", "rust-tests", "source-scans"],
        "rust-lint runs first, so a compile error is the fastest red"
    );
    assert_eq!(placement.verdict_platforms, vec!["linux".to_string()]);
    let selector = placement.executor.as_ref().unwrap();
    assert!(!selector.requires_macos_nested_sandbox_write);
    assert_eq!(selector.required_toolchains, vec!["bun", "rust"]);
    // The submission computes the same placement from the same items.
    let (executor, platforms) = merge_batch_selector(members).unwrap();
    assert_eq!(executor.as_ref(), Some(selector));
    assert_eq!(platforms, placement.verdict_platforms);
    assert_eq!(groups[1].0.cell_group, None);
    assert_eq!(groups[1].1[0].name, "typecheck");

    // A lone rust-tests keeps its requirement: it may land on macOS.
    let (alone, _) = merge_batch_selector(&[grouped_item(
        0,
        "rust-tests",
        rust_toolchains(true),
        &["linux", "macos"],
    )])
    .unwrap();
    assert!(alone.unwrap().requires_macos_nested_sandbox_write);
}

#[test]
fn a_member_the_group_cannot_place_is_placed_alone_and_reported() {
    let pinned = Some(ExecutorSelector {
        name: Some("bgsrv-mac".into()),
        ..Default::default()
    });
    let named = Some(ExecutorSelector {
        name: Some("bginf-sp1".into()),
        ..Default::default()
    });
    let (groups, fallbacks) = partition_check_items_with_cell_groups(
        vec![
            grouped_item(0, "rust-lint", named.clone(), &["linux"]),
            grouped_item(1, "rust-tests", named, &["linux"]),
            grouped_item(2, "source-scans", pinned, &["linux"]),
        ],
        &rust_cell_group(),
        &|_, _| None,
    );
    assert_eq!(fallbacks.len(), 1);
    assert_eq!(
        (fallbacks[0].name.as_str(), fallbacks[0].group.as_str()),
        ("source-scans", "rust")
    );
    assert!(
        fallbacks[0].reason.contains("conflicting"),
        "{:?}",
        fallbacks[0]
    );
    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups[0].1.len(),
        2,
        "the members that agree still share a cell"
    );
    assert_eq!(groups[1].1[0].name, "source-scans");

    // A group reduced to one member is no group at all.
    let (groups, _) = partition_check_items_with_cell_groups(
        vec![grouped_item(
            0,
            "rust-lint",
            rust_toolchains(false),
            &["linux"],
        )],
        &rust_cell_group(),
        &|_, _| None,
    );
    assert_eq!(groups[0].0.cell_group, None);
}

/// Two merges that each succeed can still contradict each other: a member
/// asking for macOS and one counting only Linux merge into a selector that
/// no trusted machine satisfies. That member is placed alone, where it
/// runs, rather than taking the group down with it.
#[test]
fn a_member_whose_selector_contradicts_the_groups_platforms_is_placed_alone() {
    let on_mac = Some(ExecutorSelector {
        os: Some("macos".into()),
        required_toolchains: vec!["rust".into()],
        ..Default::default()
    });
    let (groups, fallbacks) = partition_check_items_with_cell_groups(
        vec![
            grouped_item(0, "rust-lint", rust_toolchains(false), &["linux"]),
            grouped_item(1, "rust-tests", on_mac, &["linux", "macos"]),
            grouped_item(2, "source-scans", rust_toolchains(false), &["linux"]),
        ],
        &rust_cell_group(),
        &|_, _| None,
    );
    assert_eq!(fallbacks.len(), 1, "{fallbacks:?}");
    assert_eq!(fallbacks[0].name, "rust-tests");
    assert!(
        fallbacks[0]
            .reason
            .contains("ask for macos but counts verdicts only on linux"),
        "{:?}",
        fallbacks[0]
    );
    assert_eq!(
        groups[0]
            .1
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["rust-lint", "source-scans"]
    );
    assert_eq!(groups[0].0.verdict_platforms, vec!["linux".to_string()]);
    assert_eq!(groups[1].1[0].name, "rust-tests");
}

/// The reviewer's specimen: Linux L and Mac M. rust-lint counts both and is
/// suppressed on L, so alone it runs on M; source-scans counts Linux only,
/// so alone it runs on L. Together they could run only on L, which is
/// avoided, though the union of their machines still shows M clear. The
/// later member is placed alone and both lanes still run.
#[test]
fn a_member_whose_shared_machines_are_all_suppressed_is_placed_alone() {
    let scoped =
        |index: usize, name: &str, platforms: &[&str], machines: &[&str], avoid: &[&str]| {
            let mut item = grouped_item(index, name, rust_toolchains(false), platforms);
            item.infra_scopes.environments = machines
                .iter()
                .map(|machine| (machine.to_string(), format!("env-{machine}")))
                .collect();
            item.avoid_executor_ids = avoid.iter().map(|id| id.to_string()).collect();
            item
        };
    let (groups, fallbacks) = partition_check_items_with_cell_groups(
        vec![
            scoped(0, "rust-lint", &["linux", "macos"], &["L", "M"], &["L"]),
            scoped(1, "source-scans", &["linux"], &["L"], &[]),
        ],
        &rust_cell_group(),
        &|_, _| None,
    );
    assert_eq!(fallbacks.len(), 1, "{fallbacks:?}");
    assert_eq!(fallbacks[0].name, "source-scans");
    assert!(
        fallbacks[0]
            .reason
            .contains("suppressed for one of them: L"),
        "{:?}",
        fallbacks[0]
    );
    assert_eq!(groups.len(), 2, "each lane keeps a machine it can run on");
    assert!(groups
        .iter()
        .all(|(placement, _)| placement.cell_group.is_none()));
    assert_eq!(
        merge_batch_avoid(&groups[0].1).unwrap(),
        vec!["L".to_string()]
    );

    // With M clear for both, they share it and stay grouped.
    let (groups, fallbacks) = partition_check_items_with_cell_groups(
        vec![
            scoped(0, "rust-lint", &["linux", "macos"], &["L", "M"], &["L"]),
            scoped(1, "source-scans", &["linux", "macos"], &["L", "M"], &[]),
        ],
        &rust_cell_group(),
        &|_, _| None,
    );
    assert!(fallbacks.is_empty(), "{fallbacks:?}");
    assert_eq!(groups[0].0.cell_group.as_deref(), Some("rust"));
    assert_eq!(groups[0].0.avoid, vec!["L".to_string()]);
}

/// A union no single machine satisfies (here, toolchains) is the fleet's
/// to judge, and the group asks it before seating each member.
#[test]
fn a_member_the_fleet_cannot_place_with_the_group_is_placed_alone() {
    let wants_wasm = Some(ExecutorSelector {
        required_toolchains: vec!["rust".into(), "wasm-pack".into()],
        ..Default::default()
    });
    let fleet = |selector: Option<&ExecutorSelector>, _: &[String]| {
        selector
            .is_some_and(|selector| {
                selector
                    .required_toolchains
                    .iter()
                    .any(|t| t == "wasm-pack")
                    && selector.required_toolchains.iter().any(|t| t == "bun")
            })
            .then(|| "no machine advertises bun with wasm-pack".to_string())
    };
    let (groups, fallbacks) = partition_check_items_with_cell_groups(
        vec![
            grouped_item(0, "rust-lint", rust_toolchains(false), &["linux"]),
            grouped_item(1, "rust-tests", wants_wasm, &["linux"]),
            grouped_item(2, "source-scans", rust_toolchains(false), &["linux"]),
        ],
        &rust_cell_group(),
        &fleet,
    );
    assert_eq!(fallbacks.len(), 1, "{fallbacks:?}");
    assert!(fallbacks[0].reason.contains("bun with wasm-pack"));
    assert_eq!(groups[0].1.len(), 2);
    assert_eq!(groups[1].1[0].name, "rust-tests");
}

/// With no cell groups, or none of their members in the wave, the wave
/// partitions exactly as it did before groups existed.
#[test]
fn an_ungrouped_wave_partitions_exactly_as_before() {
    let wave = || {
        vec![
            grouped_item(0, "typecheck", None, &[]),
            grouped_item(1, "rust-tests", rust_toolchains(true), &["linux", "macos"]),
            grouped_item(2, "lint", None, &[]),
        ]
    };
    let names = |groups: &[(CheckBatchPlacement, Vec<PlannedCheckBatchItem>)]| {
        groups
            .iter()
            .map(|(placement, items)| {
                (
                    placement.cell_group.clone(),
                    items.iter().map(|item| item.index).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };
    let before = partition_check_items_by_placement(wave());
    for groups in [BTreeMap::new(), rust_cell_group()] {
        let (after, fallbacks) =
            partition_check_items_with_cell_groups(wave(), &groups, &|_, _| None);
        assert!(fallbacks.is_empty());
        assert_eq!(names(&after), names(&before));
    }
}

/// The gate is read by the lanes' own processes, so it has to arrive in
/// each spawned process's environment: the wire's per-item `env`, which is
/// all a pure-verdict batch spawns with.
#[test]
fn a_cell_groups_batch_carries_its_compile_gate_and_no_other_batch_does() {
    let items = vec![
        grouped_item(0, "rust-lint", None, &[]),
        grouped_item(1, "rust-tests", None, &[]),
    ];
    let mut placement = CheckBatchPlacement {
        executor: None,
        verdict_platforms: Vec::new(),
        avoid: Vec::new(),
        cell_group: None,
    };
    let mut batch = PlannedCheckBatchRequest {
        project_id: "p".into(),
        repository: "/repo".into(),
        store_dir: "/store".into(),
        sealed_commit: "abc123".into(),
        requesting_job_id: "job".into(),
        owner: cairn_common::executor_protocol::CellOwnerRef::default(),
        affinity_key: None,
        priority: CellPriority::ReviewCheck,
        env: Vec::new(),
        items,
        cell_groups: rust_cell_group(),
        run_context: None,
        mutation_policy: MutationPolicy::PureVerdict,
        status_board: None,
        on_group_complete: None,
        purpose: None,
        publication: None,
    };
    let spawned_env = |batch: &PlannedCheckBatchRequest| {
        crate::execution::wire::build_check_batch_wire(
            batch,
            "request".into(),
            "attempt".into(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .items
        .into_iter()
        .map(|item| item.process.env)
        .collect::<Vec<_>>()
    };
    gate_cell_group(&mut batch, &placement);
    assert!(spawned_env(&batch).iter().all(Vec::is_empty));
    placement.cell_group = Some("rust".into());
    gate_cell_group(&mut batch, &placement);
    let token = (CELL_GROUP_GATE_ENV.to_string(), "abc123:rust".to_string());
    assert_eq!(
        spawned_env(&batch),
        vec![vec![token.clone()], vec![token]],
        "every lane's process sees the token"
    );
}

#[test]
fn pure_verdict_batches_keep_untargeted_and_targeted_checks_separate() {
    let item = |index, executor| PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: format!("check-{index}"),
        input_hash: format!("hash-{index}"),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: format!("resource-{index}"),
            command: format!("resource-{index}"),
        },
        command: "true".into(),
        stream_id: format!("stream-{index}"),
        env: Vec::new(),
        timeout_ms: 1,
        executor,
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        verdict_environment_names: Vec::new(),
        infra_scopes: InfraScopes::head_only("job", "commit"),
        avoid_executor_ids: Vec::new(),
    };
    let groups = partition_check_items_by_placement(vec![
        item(0, None),
        item(
            1,
            Some(ExecutorSelector {
                os: Some("linux".into()),
                ..Default::default()
            }),
        ),
        item(2, None),
    ]);
    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups[0]
            .1
            .iter()
            .map(|item| item.index)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(
        groups[1]
            .1
            .iter()
            .map(|item| item.index)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn classified_excerpt_keeps_early_evidence_and_final_tail_bounded() {
    let output = format!(
        "failed to fill whole buffer\n{}\nFINAL-MARKER",
        "noise\n".repeat(2000)
    );
    let classification =
        classify_check_failure("bun run check:web", Some(1), false, false, None, &output);
    let excerpt = classified_output_excerpt(&output, classification.as_ref(), None);
    assert!(excerpt.contains("failed to fill whole buffer"));
    assert!(excerpt.contains("FINAL-MARKER"));
    assert!(excerpt.chars().count() <= OUTPUT_TAIL_CHARS);
}

/// A claimed attempt's silence never becomes a claim about whether the
/// check ran, whatever the machine said last. Each case is a route the
/// runner's between-poll account cannot see through: an attempt that ran
/// and was then held between items before vanishing, one whose execution
/// fell entirely between two reads, a completion record for a refusal that
/// spawned nothing, and a machine that went away mid-run.
#[test]
fn a_claimed_attempts_silence_keeps_whether_the_check_ran_unknown() {
    use cairn_common::executor_protocol::{
        CellUnavailableReason, ClaimedAttemptAccount as Account,
    };
    let cases = [
        (Some(Account::Held), Account::NotListed),
        (None, Account::NotListed),
        (Some(Account::Queued), Account::CompletedWithoutResult),
        (None, Account::CompletedWithoutResult),
        (Some(Account::Executing), Account::NotListed),
        (Some(Account::Executing), Account::LinkDown),
        (Some(Account::Held), Account::LinkSilent),
    ];
    for (last_progress, final_account) in cases {
        let outcome = CellOutcome::Unavailable {
            reason: CellUnavailableReason::ClaimedAttemptSilent {
                executor_name: "bgsrv-ub".into(),
                quiet_ms: 600_000,
                last_progress,
                final_account,
            },
            diagnostic: "bgsrv-ub gave no account of request r attempt a for 10m00s".into(),
        };
        let CheckExecutionFailure::Substrate(failure) =
            check_result_from_cell_outcome(outcome, &crate::fleet::CellAccount::default(), None)
                .unwrap_err()
        else {
            panic!("silence is a substrate failure");
        };
        assert_eq!(
            failure.shape(),
            SubstrateFailureShape::OutcomeUnknown,
            "{last_progress:?} -> {final_account:?}"
        );
        let agent = failure.agent_message();
        assert!(agent.contains("whether it ran is unknown"), "{agent}");
        assert!(agent.contains("bgsrv-ub gave no account"), "{agent}");
        for claim in ["could not start", "This check ran", "capacity"] {
            assert!(!agent.contains(claim), "{claim} in {agent}");
        }
    }
}

#[test]
fn build_slot_deadline_formats_truthful_capacity_and_stall_evidence() {
    let now = unix_time_ms_for_checks();
    let outcome = |state, last_progress_unix_ms| CellOutcome::Unavailable {
        reason: cairn_common::executor_protocol::CellUnavailableReason::Deadline {
            host_pressure: None,
            substrate: Some(cairn_common::executor_protocol::ExecutorSubstrateEvidence {
                state,
                since_unix_ms: last_progress_unix_ms,
                last_progress_unix_ms,
                diagnostic: None,
                queue_depth: Some(4),
                queue_position: Some(3),
                active_cell_count: Some(2),
                oldest_running_started_at_unix_ms: Some(now.saturating_sub(500)),
            }),
        },
        diagnostic: "acquisition deadline elapsed".into(),
    };

    let fresh = check_result_from_cell_outcome(
        outcome(
            cairn_common::executor_protocol::ExecutorSubstrateState::CapacityBusy,
            now,
        ),
        &crate::fleet::CellAccount::default(),
        None,
    )
    .unwrap_err();
    let CheckExecutionFailure::Substrate(fresh) = fresh else {
        panic!("deadline must be a substrate failure");
    };
    let fresh = fresh.diagnostic();
    assert!(fresh.contains("substrate=CapacityBusy"));
    assert!(!fresh.contains("ConnectedStalled"));

    let stale = check_result_from_cell_outcome(
        outcome(
            cairn_common::executor_protocol::ExecutorSubstrateState::ConnectedStalled,
            now.saturating_sub(cairn_common::executor_protocol::EXECUTOR_PROGRESS_FRESHNESS_MS + 1),
        ),
        &crate::fleet::CellAccount::default(),
        None,
    )
    .unwrap_err();
    let CheckExecutionFailure::Substrate(stale) = stale else {
        panic!("deadline must be a substrate failure");
    };
    // The concise agent explanation carries the same bounded diagnostic as
    // the operator record so the refusal is actionable without log access.
    let agent = stale.agent_message();
    let stale = stale.diagnostic();
    assert!(stale.contains("substrate=ConnectedStalled"));
    assert!(stale.contains("lastProgressAge="));
    assert!(stale.contains("queueDepth=4"));
    assert!(stale.contains("queuePosition=3"));
    assert!(stale.contains("activeSlots=2"));
    for fact in ["substrate=", "queueDepth", "activeSlots", "build-slot"] {
        assert!(
            agent.contains(fact),
            "agent message must carry {fact}: {agent}"
        );
    }
}

fn reusable_test_result() -> ParsedCheckResult {
    ParsedCheckResult {
        schema_version: 1,
        complete: true,
        selection: "full".to_string(),
        tests: vec![crate::execution::check_parsers::CheckTestResult {
            id: "suite::passes".to_string(),
            status: "passed".to_string(),
            duration_ms: Some(1),
            attempts: 1,
            retried: false,
            flaky: false,
            failure_message: None,
            skip_reason: None,
            skip_declaration: None,
            skip_declaration_source: None,
            started_offset_ms: None,
        }],
        undeclared_skips: 0,
        parser: "nextest".to_string(),
        passed: 1,
        failed: 0,
        suite_failures: 0,
        phases: None,
        deferred: 0,
        report: None,
        skipped: 0,
        failures: Vec::new(),
        unverified: None,
        lint_findings: None,
        plan_audit: None,
        accounting: None,
        ingestion: None,
    }
}

#[test]
fn strict_reuse_policy_accepts_only_complete_stable_test_green() {
    let result = reusable_test_result();
    assert!(check_reuse_decision("bun run test:rust", None, Some(&result)).reusable);

    let mut incomplete = result.clone();
    incomplete.complete = false;
    assert!(!check_reuse_decision("bun run test:rust", None, Some(&incomplete)).reusable);

    let mut empty = result.clone();
    empty.selection = "empty".to_string();
    empty.passed = 0;
    assert!(!check_reuse_decision("bun run test:rust", None, Some(&empty)).reusable);

    let mut retried = result.clone();
    retried.tests[0].attempts = 2;
    retried.tests[0].retried = true;
    assert!(!check_reuse_decision("bun run test:rust", None, Some(&retried)).reusable);

    let mut flaky = result.clone();
    flaky.tests[0].flaky = true;
    assert!(!check_reuse_decision("bun run test:rust", None, Some(&flaky)).reusable);

    let mut undeclared = result;
    undeclared.undeclared_skips = 1;
    assert!(!check_reuse_decision("bun run test:rust", None, Some(&undeclared)).reusable);
}

#[test]
fn complete_genuine_verdicts_reuse_but_infrastructure_never_does() {
    assert!(check_reuse_decision("bunx tsc --noEmit", None, None).reusable);
    assert!(
        !check_reuse_decision(
            "bunx tsc --noEmit",
            Some(CheckFailureKind::Infrastructure),
            None,
        )
        .reusable
    );
    assert!(!check_reuse_decision("bun run test:rust", None, None).reusable);
}
