use super::*;

const GIB: u64 = 1 << 30;

/// A rustc demand, with `(count, machine total)` kills while it ran alone.
fn rustc(peak_bytes: u64, kills_here: Option<(u32, u64)>) -> resource_profiles::ProcessDemand {
    resource_profiles::ProcessDemand {
        peak_bytes,
        name: Some("rustc".into()),
        suite: Some("rust-tests".into()),
        measured_on: Some("bgsrv-ub2-device:bgsrv-ub2".into()),
        kills_here: kills_here.map(
            |(alone, machine_total_bytes)| resource_profiles::SuiteKills {
                alone,
                machine_total_bytes,
                memory_brake: Some(true),
                crowded: None,
                event: None,
            },
        ),
        basis: Default::default(),
    }
}

/// sp1 on 11-08, as cairn/6496 found it: 121.7 GiB, 9.3 GiB standing.
const SP1_TOTAL: u64 = 130_663_231_488;
const SP1_STANDING: u64 = 10_029_236_224;
const SP1: &str = "bginf-sp1-device:bginf-sp1";

/// The rustc sp2 measured at 26.0 GiB, with sp1's kills.
fn sp1_rustc(kills: resource_profiles::SuiteKills) -> resource_profiles::ProcessDemand {
    resource_profiles::ProcessDemand {
        measured_on: Some("bginf-sp2-device:bginf-sp2".into()),
        kills_here: Some(kills),
        ..rustc(26_017_456_128, None)
    }
}

fn sp1_crowded() -> cairn_common::executor_protocol::CrowdedKills {
    cairn_common::executor_protocol::CrowdedKills {
        count: 3,
        most_co_resident: Some(18),
        memory_brake: false,
        last_killed_at_unix_ms: 1_794_100_665_217,
    }
}

/// cairn/6496's specimen: three kills of a 32.5 GiB rustc on a 120.6 GiB
/// capacity while eighteen other lanes shared the machine with no memory
/// brake. They say admission over-committed sp1, not that one rustc
/// cannot fit it, so sp1 is offered the suite.
#[test]
fn crowded_kills_do_not_end_a_machines_fit() {
    let crowded = sp1_rustc(resource_profiles::SuiteKills {
        crowded: Some(sp1_crowded()),
        ..Default::default()
    });
    assert_eq!(
        refuse(&crowded, SP1_TOTAL, Some(SP1_STANDING), None, SP1),
        None
    );
}

/// A size misfit names the crowded kills beside it as admission's, not as the
/// machine's answer.
#[test]
fn a_size_misfit_names_crowded_kills_as_admissions() {
    let demand = resource_profiles::ProcessDemand {
        kills_here: Some(resource_profiles::SuiteKills {
            crowded: Some(sp1_crowded()),
            ..Default::default()
        }),
        ..rustc(200 * GIB, None)
    };
    let Some(PlacementRejectionReason::ProcessTooLarge(misfit)) =
        refuse(&demand, SP1_TOTAL, Some(SP1_STANDING), None, SP1)
    else {
        panic!("a 200 GiB rustc does not fit sp1");
    };
    let text = misfit.describe();
    assert!(
        text.contains("the kernel killed it here 3 times while up to 18 other commands shared the machine with no memory brake (last 11-08 UTC), which says admission over-committed the machine then, not that one process cannot fit it"),
        "{text}"
    );
}

/// The fit of a single named demand on a platform whose kernel kills: a
/// refusal, or nothing.
fn refuse(
    demand: &resource_profiles::ProcessDemand,
    total_bytes: u64,
    standing_bytes: Option<u64>,
    memory_budget_bytes: Option<u64>,
    here: &str,
) -> Option<PlacementRejectionReason> {
    let evidence = resource_profiles::ProcessEvidence {
        demand: Some(demand.clone()),
        unattributed: None,
        unmeasured_compile: false,
        test_phase: None,
        unreadable_memory: Vec::new(),
        slot_tree: None,
    };
    match judge_process_evidence(
        &evidence,
        total_bytes,
        standing_bytes,
        memory_budget_bytes,
        None,
        here,
        true,
    )? {
        ProcessFit::Refused(reason) => Some(reason),
        ProcessFit::Waived(waiver) => {
            panic!("a named demand on Linux is never waived: {waiver:?}")
        }
        ProcessFit::Deferred(misfit) => {
            panic!("a measured demand is never deferred: {misfit:?}")
        }
    }
}

/// The issue's test: a learned 10 GiB process is not offered to a 12 GiB
/// machine with 2.7 GiB standing, is offered to a 125 GiB one, and the
/// record names the process, the suite, where it was measured, and that
/// the number is an estimate here.
#[test]
fn a_ten_gib_process_fits_a_large_machine_and_not_a_small_one() {
    let demand = rustc(10 * GIB, None);
    let reason = refuse(
        &demand,
        13 * GIB,
        Some(2 * GIB + 7 * GIB / 10),
        Some(11 * GIB),
        "bgsrv-ub-device:bgsrv-ub",
    )
    .expect("refused on the small machine");
    let text = reason.describe();
    assert!(
        text.starts_with("needs one process of ~12.5 GiB (rustc, suite rust-tests, 10.0 GiB measured on bgsrv-ub2 x 1.25 headroom; estimated here); this machine can give one process 10.3 GiB (13.0 GiB total, 2.7 GiB standing)"),
        "{text}"
    );
    assert_eq!(
        refuse(
            &demand,
            125 * GIB,
            Some(6 * GIB),
            Some(110 * GIB),
            "fedora:fedora"
        ),
        None
    );
}

/// An executor that posts no standing use is judged against its
/// reservation cap, which is no more permissive than before.
#[test]
fn without_standing_use_the_cap_is_the_capacity() {
    let Some(PlacementRejectionReason::ProcessTooLarge(misfit)) = refuse(
        &rustc(7 * GIB, None),
        13 * GIB,
        None,
        Some(11 * GIB),
        "bgsrv-ub-device:bgsrv-ub",
    ) else {
        panic!("8.75 GiB does not fit an 8.25 GiB cap");
    };
    assert_eq!(
        misfit.capacity_bytes,
        resource_profiles::memory_reservation_cap(11 * GIB)
    );
    assert!(misfit.describe().contains("its standing use is unmeasured"));
}

/// Repeated kills whose cause is unit-bound or unknown do not by themselves
/// end a machine's fit (cairn/6496's two-kill rule is suspended until a kill's
/// cause can be established), and they never waive the per-process bound.
#[test]
fn repeated_unknown_kills_preserve_the_process_size_guard() {
    let total = 13 * GIB;
    let here = "bgsrv-ub-device:bgsrv-ub";
    for kills in [1, 2, 3] {
        assert_eq!(
            refuse(
                &rustc(2 * GIB, Some((kills, total))),
                total,
                Some(GIB),
                None,
                here
            ),
            None
        );
        assert!(refuse(
            &rustc(20 * GIB, Some((kills, total))),
            total,
            Some(GIB),
            None,
            here
        )
        .is_some());
    }
}

/// cairn/6310's specimen: bgsrv-mac's only evidence for the whole suite
/// is the 15.6 GiB its sampler recorded with no process named, against a
/// 15.3 GiB cap. It is offered the suite, and the record keeps the
/// refusal it would have made, with its arithmetic, and why it did not.
/// The same unnamed peak on Linux is waived the same way: the rule is
/// about the measurement, not the platform.
#[test]
fn an_unnamed_peak_is_waived_on_any_platform_with_its_arithmetic_shown() {
    let here = "bgsrv-mac-device:bgsrv-mac";
    let evidence = resource_profiles::ProcessEvidence {
        demand: None,
        unattributed: Some(resource_profiles::ProcessDemand {
            peak_bytes: 16_751_935_488,
            name: None,
            suite: Some("rust-tests".into()),
            measured_on: Some(here.into()),
            kills_here: None,
            basis: Default::default(),
        }),
        unmeasured_compile: false,
        test_phase: None,
        unreadable_memory: Vec::new(),
        slot_tree: None,
    };
    let budget = Some(24 * GIB / 100 * 85);
    for kernel_kills in [false, true] {
        let Some(ProcessFit::Waived(waiver)) =
            judge_process_evidence(&evidence, 24 * GIB, None, budget, None, here, kernel_kills)
        else {
            panic!("an unnamed peak never refuses (kernel kills: {kernel_kills})");
        };
        assert_eq!(
            waiver.reason,
            cairn_common::executor_protocol::ProcessFitWaiverReason::UnattributedPeak
        );
        let text = waiver.describe();
        assert!(
            text.contains("needs one process of ~19.5 GiB (suite rust-tests, 15.6 GiB measured here x 1.25 headroom); this machine can give one process 15.3 GiB (24.0 GiB total; its standing use is unmeasured"),
            "{text}"
        );
        assert!(
            text.contains("no process was named for that peak"),
            "{text}"
        );
    }
}

/// A named process too large for the machine is refused where the kernel
/// kills and waived where it cannot: macOS slows it, never kills it.
#[test]
fn a_named_misfit_is_waived_only_where_nothing_kills() {
    let evidence = resource_profiles::ProcessEvidence {
        demand: Some(rustc(10 * GIB, None)),
        unattributed: None,
        unmeasured_compile: false,
        test_phase: None,
        unreadable_memory: Vec::new(),
        slot_tree: None,
    };
    let judge = |kernel_kills| {
        judge_process_evidence(
            &evidence,
            13 * GIB,
            Some(2 * GIB),
            None,
            None,
            "m-device:m",
            kernel_kills,
        )
    };
    assert!(matches!(
        judge(true),
        Some(ProcessFit::Refused(
            PlacementRejectionReason::ProcessTooLarge(_)
        ))
    ));
    let Some(ProcessFit::Waived(waiver)) = judge(false) else {
        panic!("no kernel memory killer, no refusal");
    };
    assert_eq!(
        waiver.reason,
        cairn_common::executor_protocol::ProcessFitWaiverReason::NoMemoryKiller
    );
    assert!(!kernel_kills_for_memory("macos"));
    assert!(kernel_kills_for_memory("linux"));
}

/// A named demand that fits keeps an unnamed, larger peak from being
/// judged as one process, and an unnamed peak that fits says nothing.
#[test]
fn a_fitting_unnamed_peak_leaves_no_record() {
    let evidence = resource_profiles::ProcessEvidence {
        demand: Some(rustc(GIB, None)),
        unattributed: Some(resource_profiles::ProcessDemand {
            name: None,
            ..rustc(2 * GIB, None)
        }),
        unmeasured_compile: false,
        test_phase: None,
        unreadable_memory: Vec::new(),
        slot_tree: None,
    };
    assert_eq!(
        judge_process_evidence(
            &evidence,
            13 * GIB,
            Some(GIB),
            None,
            None,
            "m-device:m",
            true
        ),
        None
    );
}

/// bgsrv-ub as it reads on its page: 12.1 GiB total, 2.5 GiB standing,
/// so 9.6 GiB for one process.
const UB_TOTAL: u64 = 12_992_276_070;
const UB_STANDING: u64 = 2_684_354_560;
const UB: &str = "bgsrv-ub-device:bgsrv-ub";
/// The rustc the kernel killed there twice on 11-06 (9.2 GiB).
const KILLED_RUSTC: u64 = 9_910_000_000;

fn linux(executor_class: &str, arch: &str) -> resource_profiles::ProfileContext {
    resource_profiles::ProfileContext {
        executor_class: executor_class.into(),
        os: "linux".into(),
        arch: arch.into(),
        toolchain_fingerprint: "bun\u{1f}rust".into(),
    }
}

fn workspace_compile(key: &str) -> CommandResourceIdentity {
    CommandResourceIdentity {
        version: 1,
        key: key.into(),
        suite: Some("workspace-compile".into()),
    }
}

fn rustc_completion(peak: u64, at: u64) -> CellExecutionMeta {
    CellExecutionMeta {
        stop: None,
        peak_process: Some(cairn_common::executor_protocol::PeakProcess {
            bytes: peak,
            name: Some("rustc".into()),
        }),
        ..resource_profiles::oom_killed_for_test(peak, None, 0, at)
    }
}

async fn evidence_on_ub(
    db: Arc<cairn_db::storage::LocalDb>,
    identity: &CommandResourceIdentity,
) -> resource_profiles::ProcessEvidence {
    let class = cairn_common::executor_protocol::CellCommandClass::CargoCheck;
    let suite = resource_profiles::suite_key("cairn", Some(identity), class);
    resource_profiles::resolve_process_demand(
        db,
        Some(identity),
        suite.as_deref(),
        class,
        &linux(UB, "x86_64"),
    )
    .await
}

/// cairn/6312 pin (a): the identity has no x86_64 peak (fedora's rows
/// predate the peak-reporting sampler, bbs had not yet written one) but
/// bginf-sp1 measured a 12 GiB rustc on aarch64. bgsrv-ub refuses it on
/// that borrowed number, and the sentence names where it came from.
#[tokio::test]
async fn an_aarch64_peak_refuses_a_cold_x86_identity_on_the_small_machine() {
    let db = Arc::new(crate::storage::migrated_test_db("process-fit-borrowed.db").await);
    let id = workspace_compile("workspace-compile:6a649d48");
    let unnamed = CellExecutionMeta {
        peak_process: None,
        ..rustc_completion(4 * GIB, 1)
    };
    resource_profiles::observe_completed(
        db.clone(),
        Some(&id),
        &linux("fedora:fedora", "x86_64"),
        &unnamed,
        resource_profiles::CompletionKind::Succeeded,
        resource_profiles::CompletedSource::default(),
    )
    .await;
    resource_profiles::observe_completed(
        db.clone(),
        Some(&id),
        &linux("bginf-sp1-device:bginf-sp1", "aarch64"),
        &rustc_completion(12 * GIB, 2),
        resource_profiles::CompletionKind::Succeeded,
        resource_profiles::CompletedSource::default(),
    )
    .await;
    let evidence = evidence_on_ub(db, &id).await;
    let Some(ProcessFit::Refused(PlacementRejectionReason::ProcessTooLarge(misfit))) =
        judge_process_evidence(&evidence, UB_TOTAL, Some(UB_STANDING), None, None, UB, true)
    else {
        panic!("a borrowed measurement refuses like one taken here: {evidence:?}");
    };
    let text = misfit.describe();
    assert!(
        text.contains("needs one process of ~15.0 GiB (rustc, 12.0 GiB measured on bginf-sp1 (aarch64) x 1.25 headroom; estimated here); this machine can give one process 9.6 GiB"),
        "{text}"
    );
    assert_eq!(
        judge_process_evidence(
            &evidence,
            62 * GIB,
            Some(4 * GIB),
            None,
            None,
            "bbs:bbs",
            true
        ),
        None,
        "a 62 GiB machine holds it"
    );
}

/// cairn/6263 on bgsrv-ub2 (15.4 GiB, 2 GiB standing, executor bound at
/// 11.25 GiB by cairn/6994): a rustc measured at 10 GiB needs ~12.5 GiB,
/// which the machine's total less its standing use would give it, but the
/// kernel kills a cell's process at the bound. Refused by the bound, by
/// name; an unbounded machine of the same size still takes it.
#[test]
fn the_executors_memory_bound_is_what_one_process_can_have() {
    const UB2_TOTAL: u64 = 154 * GIB / 10;
    const UB2_BOUND: u64 = 45 * GIB / 4;
    let evidence = resource_profiles::ProcessEvidence {
        demand: Some(resource_profiles::ProcessDemand {
            peak_bytes: 10 * GIB,
            name: Some("rustc".into()),
            suite: Some("rust-tests".into()),
            measured_on: Some("bginf-sp1-device:bginf-sp1".into()),
            kills_here: None,
            basis: cairn_common::executor_protocol::ProcessDemandBasis::ThisPlatform,
        }),
        ..resource_profiles::ProcessEvidence::default()
    };
    let judge = |bound| {
        judge_process_evidence(
            &evidence,
            UB2_TOTAL,
            Some(2 * GIB),
            None,
            bound,
            "bgsrv-ub2-device:bgsrv-ub2",
            true,
        )
    };
    assert_eq!(judge(None), None, "13.4 GiB of room holds 12.5 GiB");
    let Some(ProcessFit::Refused(PlacementRejectionReason::ProcessTooLarge(misfit))) =
        judge(Some(UB2_BOUND))
    else {
        panic!("the bound refuses it");
    };
    assert_eq!(misfit.capacity_bytes, UB2_BOUND);
    assert_eq!(misfit.bound_bytes, Some(UB2_BOUND));
    let text = misfit.describe();
    assert!(
        text.contains("needs one process of ~12.5 GiB (rustc, suite rust-tests, 10.0 GiB measured on bginf-sp1"),
        "{text}"
    );
    assert!(
        text.contains("(its executor's memory bound, on a 15.4 GiB machine)"),
        "{text}"
    );
    // A bound above what the machine can give names nothing.
    let Some(ProcessFit::Refused(PlacementRejectionReason::ProcessTooLarge(roomier))) =
        judge_process_evidence(
            &evidence,
            12 * GIB,
            Some(2 * GIB),
            None,
            Some(UB2_BOUND),
            "m-device:m",
            true,
        )
    else {
        panic!("10 GiB of room refuses 12.5 GiB");
    };
    assert_eq!(roomier.bound_bytes, None);
}

/// cairn/6312 pin (b): nothing of the identity was measured anywhere,
/// and the project's largest rustc is the 9.2 GiB its rust-tests suite
/// lost. bgsrv-ub is deferred on that prior, not refused, and a 62 GiB
/// machine fits it.
#[tokio::test]
async fn the_class_prior_defers_the_small_machine() {
    let db = Arc::new(crate::storage::migrated_test_db("process-fit-prior.db").await);
    resource_profiles::observe_suite(
        db.clone(),
        Some("cairn/rust-tests"),
        CellCommandClass::CargoTest,
        &linux("bgsrv-ub2-device:bgsrv-ub2", "x86_64"),
        &rustc_completion(KILLED_RUSTC, 1),
        resource_profiles::CompletionKind::Failed,
        resource_profiles::MachineAdmission::default(),
        resource_profiles::CompletedSource::default(),
    )
    .await;
    let evidence = evidence_on_ub(db, &workspace_compile("workspace-compile:cold")).await;
    let Some(ProcessFit::Deferred(misfit)) =
        judge_process_evidence(&evidence, UB_TOTAL, Some(UB_STANDING), None, None, UB, true)
    else {
        panic!("a class prior defers: {evidence:?}");
    };
    let text = PlacementRejectionReason::ProcessDeferred(misfit).describe();
    assert!(
        text.contains("(rustc, unmeasured; standing in: suite rust-tests's peak 9.2 GiB from bgsrv-ub2 x 1.25 headroom)"),
        "{text}"
    );
    assert!(text.starts_with("deferred: "), "{text}");
    assert_eq!(
        judge_process_evidence(
            &evidence,
            62 * GIB,
            Some(4 * GIB),
            None,
            None,
            "bbs:bbs",
            true
        ),
        None
    );
    assert!(
        matches!(
            judge_process_evidence(&evidence, 10 * GIB, None, None, None, UB, false),
            Some(ProcessFit::Waived(_))
        ),
        "a platform with no memory killer prices a prior as it prices a measurement"
    );
}

/// cairn/7027's specimen: bgsrv-win, 27.8 GiB with 23.3 GiB standing, can
/// give one process 4.5 GiB. An agent's ad-hoc `cargo test -p cairn-codec`,
/// measured nowhere, stands on `class:cargoTest`'s 29.7 GiB rustc from bbs on
/// Linux: a number of other work, so bgsrv-win is deferred rather than
/// refused, and the sentence says the number is borrowed. A configured
/// suite's own 29.7 GiB, with the same numbers, still refuses.
#[tokio::test]
async fn a_class_suites_peak_defers_an_unmeasured_command() {
    let db = Arc::new(crate::storage::migrated_test_db("process-fit-class-suite.db").await);
    let class = CellCommandClass::CargoTest;
    let bbs = linux("bbs-device:bbs", "x86_64");
    let windows = resource_profiles::ProfileContext {
        os: "windows".into(),
        ..linux("bgsrv-win-device:bgsrv-win", "x86_64")
    };
    let peak = 29_700 * GIB / 1000;
    let total = 27_800 * GIB / 1000;
    let standing = 23_300 * GIB / 1000;
    let codec = resource_profiles::ad_hoc_identity(
        "cairn",
        "cargo test --manifest-path src-tauri/Cargo.toml -p cairn-codec --lib -- one",
        class,
    )
    .unwrap();
    let configured = CommandResourceIdentity {
        version: 1,
        key: "rust-tests:cold".into(),
        suite: Some("rust-tests".into()),
    };
    let mut evidence = Vec::new();
    for identity in [&codec, &configured] {
        let suite = resource_profiles::suite_key("cairn", Some(identity), class);
        resource_profiles::observe_suite(
            db.clone(),
            suite.as_deref(),
            class,
            &bbs,
            &rustc_completion(peak, 1),
            resource_profiles::CompletionKind::Failed,
            resource_profiles::MachineAdmission::default(),
            resource_profiles::CompletedSource::default(),
        )
        .await;
        evidence.push(
            resource_profiles::resolve_process_demand(
                db.clone(),
                Some(identity),
                suite.as_deref(),
                class,
                &windows,
            )
            .await,
        );
    }
    let here = "bgsrv-win-device:bgsrv-win";
    let Some(ProcessFit::Deferred(misfit)) =
        judge_process_evidence(&evidence[0], total, Some(standing), None, None, here, true)
    else {
        panic!("a class suite's peak defers: {:?}", evidence[0]);
    };
    let text = PlacementRejectionReason::ProcessDeferred(misfit).describe();
    assert!(
        text.contains("(rustc, unmeasured; standing in: suite class:cargoTest's peak 29.7 GiB from bbs (linux x86_64) x 1.25 headroom)"),
        "{text}"
    );
    assert!(
        matches!(
            judge_process_evidence(&evidence[1], total, Some(standing), None, None, here, true),
            Some(ProcessFit::Refused(
                PlacementRejectionReason::ProcessTooLarge(_)
            ))
        ),
        "a configured suite's peak is a measurement of the suite: {:?}",
        evidence[1]
    );
}

/// cairn/6312 pin (d), on 11-06's rows: workspace-compile's rustc
/// measured 9.2 GiB on bgsrv-ub and was killed there twice at 12.1 GiB.
/// The next request, under any identity of the suite, is refused there,
/// on its size measured here.
#[tokio::test]
async fn a_suite_killed_here_at_its_measured_size_is_refused_on_the_next_request() {
    let db = Arc::new(crate::storage::migrated_test_db("process-fit-killed.db").await);
    for at in [1, 2] {
        let mut kill = resource_profiles::oom_killed_for_test(KILLED_RUSTC, Some(UB_TOTAL), 0, at);
        if let Some(cairn_common::executor_protocol::CommandStop::OomKilled {
            process_name, ..
        }) = &mut kill.stop
        {
            *process_name = Some("rustc".into());
        }
        resource_profiles::observe_suite(
            db.clone(),
            Some("cairn/workspace-compile"),
            CellCommandClass::CargoTest,
            &linux(UB, "x86_64"),
            &kill,
            resource_profiles::CompletionKind::Failed,
            resource_profiles::MachineAdmission::default(),
            resource_profiles::CompletedSource::default(),
        )
        .await;
    }
    let evidence = evidence_on_ub(db, &workspace_compile("workspace-compile:next")).await;
    let demand = evidence.demand.as_ref().unwrap();
    assert_eq!(
        demand
            .kills_here
            .as_ref()
            .map(|kills| (kills.alone, kills.machine_total_bytes)),
        Some((2, UB_TOTAL))
    );
    let Some(ProcessFit::Refused(reason)) =
        judge_process_evidence(&evidence, UB_TOTAL, Some(UB_STANDING), None, None, UB, true)
    else {
        panic!("refused where it was killed");
    };
    let text = reason.describe();
    assert!(
        text.contains("(rustc, suite workspace-compile, 9.2 GiB measured here x 1.25 headroom)"),
        "{text}"
    );
}

#[test]
fn a_number_measured_here_is_not_called_an_estimate() {
    let demand = resource_profiles::ProcessDemand {
        measured_on: Some("bgsrv-ub-device:bgsrv-ub".into()),
        ..rustc(10 * GIB, None)
    };
    let reason = refuse(
        &demand,
        13 * GIB,
        Some(GIB),
        None,
        "bgsrv-ub-device:bgsrv-ub",
    )
    .unwrap();
    let text = reason.describe();
    assert!(
        text.contains("measured here") && !text.contains("estimated"),
        "{text}"
    );
}
