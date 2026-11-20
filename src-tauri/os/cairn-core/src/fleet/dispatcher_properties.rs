//! The room-driven dispatcher's promises (cairn/7303 step 4), checked on a
//! real [`Fleet`]: fake machines attached to it, their room announced through
//! the runner's own `record_room`, the frames sent to them drained, waiting
//! requests run as `execute_entry` runs them, and the counts read from
//! [`counters::FleetCounters`] and the profile store's statement counter.
//!
//! The pure core's own tests in `dispatcher.rs` hold the arithmetic; these
//! hold the wiring around it: that a sample, a snapshot, a beat or an
//! unchanged room reaches no placement code, that a profile write re-figures
//! only the entries it names and only once per burst, and that the runner's
//! eligibility rules still decide where work lands now that ranking is gone.
//!
//! Real time with a settle step, not paused tokio time: a waiting request's
//! needs are read from the profile store off the runtime, and a paused clock
//! jumps to the next timer, a waiting request's horizon, while they are.

use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Instant as StdInstant;

use cairn_common::executor_protocol::{
    Allowance, AnnouncedRoom, CompileSlots, CpuPressure, DiskRoom, GitObjectFormat,
    GithubCredentialEvidence, GithubCredentialState, HostPressureCondition, MachineMemory,
    MachineRole, MachineVolume, MemoryRoom, NestedSandboxProbeContext, ProjectCapability,
    ProjectPlacementRequirements, ProvisioningRoomHold, RoomClosure, RoomScope,
    SandboxCapabilityEvidence, SandboxLauncherClass,
};
use cairn_common::wire_vocabulary::{CellAdmissionKind, Measurement};

use super::tests::{fleet_entry_at, measured_at, spillable_request};
use super::*;

const GIB: u64 = 1 << 30;

/// An eight-core machine's announced room: `free_cores` of its cores not
/// given out, and `available_gib` of memory.
fn room(free_cores: u64, available_gib: u64) -> AnnouncedRoom {
    AnnouncedRoom {
        slots: CompileSlots::of(8),
        cores: Allowance {
            limit: 8,
            committed: 8 - free_cores,
        },
        memory: Some(MemoryRoom {
            available_bytes: available_gib * GIB,
            owed_bytes: 0,
            total_bytes: 1024 * GIB,
        }),
        memory_budget: None,
        disk: Some(DiskRoom {
            free_bytes: 500 * GIB,
            watermark_bytes: 10 * GIB,
            reserved_bytes: 0,
            inodes: None,
        }),
        disk_budget: None,
        reclaimable_memory_bytes: 0,
        alone: None,
        provisioning: Vec::new(),
        holds: Vec::new(),
        closures: Vec::new(),
    }
}

/// Attach a machine to `fleet` the way a dialled link is attached, with
/// `edit` applied to it first, and hand back its link and its frames.
fn attach(
    fleet: &Fleet,
    id: &str,
    os: &str,
    edit: impl FnOnce(&mut ExecutorConnectionState),
) -> (u64, mpsc::UnboundedReceiver<ExecutorMessage>) {
    let now = unix_time_ms();
    let (id, mut entry) = fleet_entry_at(id, os, 0, &[], now);
    measured_at(&mut entry, 0.2, 60 * GIB, 500 * GIB, now);
    let (sender, receiver) = mpsc::unbounded_channel();
    entry.sender = sender;
    entry.snapshots_received = 1;
    edit(&mut entry);
    let generation = entry.generation;
    fleet.lock_connections().insert(id, entry);
    (generation, receiver)
}

/// `id` announces `room` under a sequence newer than its last.
fn announce(fleet: &Fleet, id: &str, room: AnnouncedRoom) {
    let (generation, seq) = {
        let connections = fleet.lock_connections();
        let entry = &connections[id];
        (
            entry.generation,
            entry.room.as_ref().map_or(0, |held| held.seq) + 1,
        )
    };
    fleet.record_room(id, generation, seq, room);
}

/// The machine's side: every offer it is sent is counted and claimed while
/// `claim` holds, refused otherwise.
fn answer_offers(
    fleet: &Fleet,
    id: &str,
    generation: u64,
    mut frames: mpsc::UnboundedReceiver<ExecutorMessage>,
    claim: Arc<AtomicBool>,
) -> (Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let offers = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn({
        let fleet = fleet.clone();
        let id = id.to_string();
        let offers = offers.clone();
        async move {
            while let Some(frame) = frames.recv().await {
                let ExecutorMessage::Offer { entry_id, .. } = frame else {
                    continue;
                };
                offers.fetch_add(1, Ordering::SeqCst);
                let answer = if claim.load(Ordering::SeqCst) {
                    ExecutorMessage::Claim { entry_id }
                } else {
                    ExecutorMessage::Decline {
                        entry_id,
                        reason: DeclineReason::Pressure,
                        detail: Some("no room after all".into()),
                        disk: None,
                    }
                };
                fleet.handle_executor_message(&id, generation, answer);
            }
        }
    });
    (offers, task)
}

/// A machine attached, announcing `room_now`, and refusing every offer it is
/// sent, with `edit` applied to it first. Its link stays open for the test.
fn machine(
    fleet: &Fleet,
    id: &str,
    os: &str,
    edit: impl FnOnce(&mut ExecutorConnectionState),
    room_now: AnnouncedRoom,
) -> Arc<AtomicUsize> {
    let (generation, frames) = attach(fleet, id, os, edit);
    announce(fleet, id, room_now);
    answer_offers(fleet, id, generation, frames, Arc::default()).0
}

fn refusing(fleet: &Fleet, id: &str, os: &str, room_now: AnnouncedRoom) -> Arc<AtomicUsize> {
    machine(fleet, id, os, |_| {}, room_now)
}

fn new_entry(request: &CellRequest) -> queue::NewEntry {
    queue::NewEntry {
        request: request.clone(),
        admission_kind: CellAdmissionKind::Command,
        interactive: false,
        execution_residency: None,
        standing: false,
    }
}

/// One request's wait, as `execute_entry` runs it: wait for an assignment,
/// offer it there, and wait again when the offer comes back.
async fn wait_for_a_machine(
    fleet: Fleet,
    request: CellRequest,
    plan: Option<Arc<ReservationPlan>>,
) {
    let Ok((entry_id, _waiter)) = fleet.queue.enqueue(new_entry(&request), unix_time_ms()) else {
        return;
    };
    let _joined = super::assignments::JoinedDispatcher {
        pool: fleet.clone(),
        entry_id: entry_id.clone(),
    };
    let policy = ActivePlacementPolicy::default_profile();
    let mut deliveries = None;
    loop {
        let Ok(placement) = fleet
            .await_assignment(&entry_id, &request, plan.as_ref(), &policy, &mut deliveries)
            .await
        else {
            return;
        };
        let mut decision = placement.decision;
        let mut attempt = request.clone();
        if let Some(resolved) = placement.reservation {
            attempt.resource_reservation = resolved.reservation;
            attempt.learned_estimate = resolved.learned_estimate;
        }
        match fleet
            .offer_once(
                &entry_id,
                &attempt,
                &placement.selected,
                &mut decision,
                placement.headroom,
            )
            .await
        {
            OfferAnswer::Requeued => continue,
            _ => return,
        }
    }
}

/// Where the dispatcher sends `request`, or what it is refused with.
// The outcome owed, as `await_assignment` returns it.
#[allow(clippy::result_large_err)]
async fn lands(fleet: &Fleet, request: &CellRequest) -> Result<String, CellOutcome> {
    let (entry_id, _waiter) = fleet
        .queue
        .enqueue(new_entry(request), unix_time_ms())
        .expect("the request enqueues");
    let _joined = super::assignments::JoinedDispatcher {
        pool: fleet.clone(),
        entry_id: entry_id.clone(),
    };
    let policy = ActivePlacementPolicy::default_profile();
    tokio::time::timeout(
        Duration::from_secs(30),
        fleet.await_assignment(&entry_id, request, None, &policy, &mut None),
    )
    .await
    .expect("the dispatcher answers within the request's horizon")
    .map(|placement| placement.selected.executor_id)
}

fn work(fleet: &Fleet) -> counters::PlacementCounts {
    fleet.placement_counters().totals()
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = StdInstant::now() + Duration::from_secs(60);
    while !done() {
        assert!(StdInstant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until the counters have stood still for a few hundred
/// milliseconds: every offer answered, every refusal recorded, every
/// re-figure done.
async fn settled(fleet: &Fleet) {
    let deadline = StdInstant::now() + Duration::from_secs(60);
    let mut last = work(fleet);
    let mut still = 0;
    while still < 3 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = work(fleet);
        if now == last {
            still += 1;
        } else {
            still = 0;
            last = now;
        }
        assert!(StdInstant::now() < deadline, "the dispatcher never settled");
    }
}

/// Work free to move, needing four cores and 4 GiB, waiting an hour.
fn movable(id: &str) -> CellRequest {
    let mut request = spillable_request();
    request.request_id = id.into();
    request.repository = RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "repo".into(),
        object_format: GitObjectFormat::Sha1,
    };
    request.wait_horizon_unix_ms = unix_time_ms() + 60 * 60_000;
    request.resource_reservation.concurrency_units = 4;
    request.resource_reservation.memory_bytes = 4 * GIB;
    request
}

/// A compile whose needs are read from the profile store under `identity`.
fn compile(id: &str, identity: &str) -> CellRequest {
    let mut request = movable(id);
    request.command_class = CellCommandClass::CargoTest;
    request.command_resource_identity = Some(CommandResourceIdentity {
        version: 1,
        key: identity.into(),
        suite: None,
    });
    request
}

fn sample(n: usize) -> MachineSample {
    let now = unix_time_ms();
    MachineSample {
        cpu: Measurement::measured(
            now,
            CpuPressure {
                utilization: 0.2 + (n % 7) as f64 * 0.01,
                user: 0.2,
                system: 0.0,
                logical_cores: 8,
                clusters: None,
            },
        ),
        memory: Measurement::measured(
            now,
            MachineMemory {
                total_bytes: 64 * GIB,
                available_bytes: 60 * GIB - (n % 5) as u64 * GIB / 8,
            },
        ),
        volume: Measurement::measured(
            now,
            MachineVolume {
                total_inodes: None,
                free_inodes: None,
                total_bytes: 1024 * GIB,
                free_bytes: 500 * GIB,
            },
        ),
        ..Default::default()
    }
}

/// Property 1: with 200 entries parked on ten machines, a thousand samples
/// and a hundred each of snapshots, beats and re-announced unchanged rooms
/// cost no placement pass, no fit evaluation, no needs worked out and no
/// statement, and offer nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_entries_cost_nothing_while_their_machines_post() {
    const PARKED: usize = 200;
    const MACHINES: usize = 10;
    let db = Arc::new(crate::storage::migrated_test_db("dispatcher-property-1.db").await);
    let fleet = Fleet::default();
    let full = room(0, 500);
    let machines: Vec<(String, u64)> = (0..MACHINES)
        .map(|n| {
            let id = format!("posting-{n}");
            let (generation, frames) = attach(&fleet, &id, "linux", |_| {});
            announce(&fleet, &id, full.clone());
            answer_offers(&fleet, &id, generation, frames, Arc::default());
            (id, generation)
        })
        .collect();
    let waits: Vec<_> = (0..PARKED)
        .map(|n| {
            let request = compile(&format!("parked-{n}"), &format!("test:rust-{n}"));
            let plan = Arc::new(ReservationPlan::new(db.clone(), &request, None));
            tokio::spawn(wait_for_a_machine(fleet.clone(), request, Some(plan)))
        })
        .collect();
    until("every entry to join", || {
        work(&fleet).placement_passes >= PARKED as u64
    })
    .await;
    settled(&fleet).await;
    assert_eq!(work(&fleet).offers, 0, "no room fits a compile slot");

    let (before, statements) = (work(&fleet), db.read_statement_count());
    for n in 0..1_000 {
        let (id, generation) = &machines[n % MACHINES];
        fleet.record_machine_sample(id, *generation, sample(n));
        if n % 10 == 0 {
            let (snapshot, beat) = {
                let connections = fleet.lock_connections();
                let entry = &connections[id];
                let mut advertisement = (*entry.advertisement).clone();
                advertisement.observed_at_unix_ms = unix_time_ms();
                (
                    ExecutorMessage::SnapshotUpdated {
                        snapshot: (*entry.snapshot).clone(),
                        health: (*entry.health).clone(),
                    },
                    ExecutorMessage::Heartbeat {
                        advertisement,
                        health: (*entry.health).clone(),
                        last_update: None,
                        machine: None,
                    },
                )
            };
            fleet.handle_executor_message(id, *generation, snapshot);
            fleet.handle_executor_message(id, *generation, beat);
            announce(&fleet, id, full.clone());
        }
    }
    settled(&fleet).await;
    let posted = work(&fleet).since(before);
    assert_eq!(posted.announcements, 100);
    assert_eq!(
        (
            posted.placement_passes,
            posted.fit_evaluations,
            posted.needs_worked_out,
            posted.offers,
        ),
        (0, 0, 0, 0),
        "passes, fit evaluations, needs worked out and offers while machines post"
    );
    assert_eq!(
        db.read_statement_count() - statements,
        0,
        "a parked entry reads nothing"
    );
    let ended = waits.iter().filter(|wait| wait.is_finished()).count();
    for wait in waits {
        wait.abort();
    }
    assert_eq!(ended, 0, "every entry is still waiting");
}

/// Property 3, extended, and the coalesced re-figure: fifty profile writes
/// naming identity X in one burst work out each waiting X entry's needs once,
/// not once per write, and touch no Y entry; one real completion of X does
/// the same, handing the dispatcher at most one new figure per X entry.
#[tokio::test]
async fn a_burst_of_profile_writes_refigures_each_named_entry_once_and_no_other() {
    const EACH: usize = 3;
    let db = Arc::new(crate::storage::migrated_test_db("dispatcher-refigure.db").await);
    let fleet = Fleet::default();
    refusing(&fleet, "m", "linux", room(0, 500));
    let x = CommandResourceIdentity {
        version: 1,
        key: "test:x".into(),
        suite: None,
    };
    let mut waits = Vec::new();
    for (identity, n) in ["test:x", "test:y"]
        .into_iter()
        .flat_map(|identity| (0..EACH).map(move |n| (identity, n)))
    {
        let request = compile(&format!("{identity}-{n}"), identity);
        let plan = Arc::new(ReservationPlan::new(db.clone(), &request, None));
        waits.push(tokio::spawn(wait_for_a_machine(
            fleet.clone(),
            request,
            Some(plan),
        )));
    }
    until("every entry to join", || {
        work(&fleet).placement_passes >= 2 * EACH as u64
    })
    .await;
    settled(&fleet).await;

    // The burst lands before the placement task can run: no await between.
    let before = work(&fleet);
    let scope = profile_reads::identity_scope(&x);
    for _ in 0..50 {
        drop(profile_reads::Writing::teaching(&db, vec![scope.clone()]));
    }
    settled(&fleet).await;
    let burst = work(&fleet).since(before);
    assert_eq!(
        burst.needs_worked_out, EACH as u64,
        "one re-figure per X entry on its one machine, none for Y"
    );
    assert_eq!(
        (burst.placement_passes, burst.offers),
        (0, 0),
        "needs that came out the same are not handed to the dispatcher"
    );

    let context = {
        let connections = fleet.lock_connections();
        let entry = &connections["m"];
        ReservationPlan::context_for(
            &entry.identity.device_id,
            &entry.identity.executor_id,
            &entry.advertisement.capabilities,
        )
    };
    let before = work(&fleet);
    resource_profiles::record_completion_for_test(db.clone(), &x, &context, 6 * GIB).await;
    settled(&fleet).await;
    let finished = work(&fleet).since(before);
    assert_eq!(
        finished.needs_worked_out, EACH as u64,
        "X's entries, not Y's"
    );
    assert!(
        finished.placement_passes <= EACH as u64,
        "at most one set_candidates per X entry, each matching that entry only: {finished:?}"
    );
    for wait in waits {
        wait.abort();
    }
}

/// Property 4: a room from A evaluates only the entries that may run on A,
/// none of the fifty pinned to B; a refusal evaluates only the refused
/// entry, against B, and walks A once for the share A's first refusal under
/// these figures gives back, evaluating nothing there (its one entry is the
/// one it refused); a claim evaluates nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_evaluates_only_the_entries_it_can_affect() {
    const PINNED: usize = 50;
    let fleet = Fleet::default();
    let (a_generation, a_frames) = attach(&fleet, "a", "linux", |_| {});
    announce(&fleet, "a", room(0, 400));
    let claim = Arc::new(AtomicBool::new(false));
    let (a_offers, _a) = answer_offers(&fleet, "a", a_generation, a_frames, claim.clone());
    let b_offers = refusing(&fleet, "b", "linux", room(0, 400));
    let mut waits = vec![tokio::spawn(wait_for_a_machine(
        fleet.clone(),
        movable("either"),
        None,
    ))];
    for n in 0..PINNED {
        let mut request = movable(&format!("pinned-{n}"));
        request.pinned_executor_id = Some("b".into());
        waits.push(tokio::spawn(wait_for_a_machine(
            fleet.clone(),
            request,
            None,
        )));
    }
    until("every entry to join", || {
        work(&fleet).placement_passes > PINNED as u64
    })
    .await;
    settled(&fleet).await;

    let before = work(&fleet);
    announce(&fleet, "a", room(0, 300));
    settled(&fleet).await;
    let walked = work(&fleet).since(before);
    assert_eq!(
        (walked.placement_passes, walked.fit_evaluations),
        (1, 1),
        "one walk of A, evaluating its one eligible entry"
    );

    // Room for it on A: offered (the walk evaluates it, then checks it again
    // as it assigns), refused, and matched once more against B alone. The
    // share the refusal gives back walks A again, which evaluates nothing.
    let before = work(&fleet);
    announce(&fleet, "a", room(4, 300));
    settled(&fleet).await;
    let refused = work(&fleet).since(before);
    assert_eq!((refused.offers, refused.refusals), (1, 1));
    assert_eq!(
        (refused.placement_passes, refused.fit_evaluations),
        (3, 3),
        "A's walk (2 evaluations), the refused entry against B (1), and A's walk for the share given back (0)"
    );

    // A newer room on A: offered again and claimed. The claim adds nothing
    // to the walk that offered it.
    claim.store(true, Ordering::SeqCst);
    let before = work(&fleet);
    announce(&fleet, "a", room(4, 250));
    settled(&fleet).await;
    let claimed = work(&fleet).since(before);
    assert_eq!(claimed.offers, 1);
    assert_eq!(
        (claimed.placement_passes, claimed.fit_evaluations),
        (1, 2),
        "the walk that offered it; the claim evaluates nothing"
    );
    assert_eq!(a_offers.load(Ordering::SeqCst), 2);

    // B's room evaluates B's entries.
    let before = work(&fleet);
    announce(&fleet, "b", room(0, 300));
    settled(&fleet).await;
    assert_eq!(
        work(&fleet).since(before).fit_evaluations,
        PINNED as u64,
        "the fifty pinned to B, and not the claimed entry"
    );
    assert_eq!(b_offers.load(Ordering::SeqCst), 0);
    for wait in waits {
        wait.abort();
    }
}

/// The per-room bound (cairn/7339 comment 2, restated by comment 11): an
/// announcement offers what its room fits, and one more for the share a
/// machine's first refusal under it gives back, never one offer per waiting
/// entry. K entries that fit nothing get no offer from a newer room; K
/// entries each fitting a room of one, on a machine refusing everything,
/// get at most two.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_announcement_offers_what_its_room_fits_whatever_is_waiting() {
    for k in [1usize, 200] {
        let fleet = Fleet::default();
        let offers = refusing(&fleet, "m", "linux", room(0, 500));
        let waits: Vec<_> = (0..k)
            .map(|n| {
                tokio::spawn(wait_for_a_machine(
                    fleet.clone(),
                    movable(&format!("bound-{k}-{n}")),
                    None,
                ))
            })
            .collect();
        until("every entry to join", || {
            work(&fleet).placement_passes >= k as u64
        })
        .await;
        settled(&fleet).await;
        assert_eq!(offers.load(Ordering::SeqCst), 0, "k = {k}");

        let before = work(&fleet);
        announce(&fleet, "m", room(0, 400));
        settled(&fleet).await;
        let full = work(&fleet).since(before);
        assert_eq!(
            (full.placement_passes, full.offers),
            (1, 0),
            "k = {k}: one walk, and a full room offers nothing"
        );

        announce(&fleet, "m", room(4, 400));
        settled(&fleet).await;
        // Four free cores fit one four-core entry; its refusal gives that
        // share back once, and the second refusal keeps it.
        let fits = 1;
        assert_eq!(
            offers.load(Ordering::SeqCst),
            k.min(fits + 1),
            "k = {k}: what the room fits, and one more"
        );
        assert_eq!(work(&fleet).offers, k.min(fits + 1) as u64, "k = {k}");
        for wait in waits {
            wait.abort();
        }
    }
}

/// Roles: of a publish-only, a services-only, an inference-only and a
/// builds-only machine, an agent's batch lands on the builds-only one even
/// with the least room; without it, the batch is refused, never offered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_lands_only_on_a_role_that_takes_batches() {
    let roles = [
        ("publisher", MachineRole::PublishOnly),
        ("services", MachineRole::ServicesOnly),
        ("inference", MachineRole::InferenceOnly),
        ("builds", MachineRole::BuildsOnly),
    ];
    for with_builds in [true, false] {
        let fleet = Fleet::default();
        let mut offers = Vec::new();
        for (id, role) in roles {
            if role == MachineRole::BuildsOnly && !with_builds {
                continue;
            }
            let (generation, frames) = attach(&fleet, id, "linux", |entry| {
                entry.machine_role = role;
            });
            let roomy = if role == MachineRole::BuildsOnly {
                20
            } else {
                400
            };
            announce(&fleet, id, room(8, roomy));
            offers.push(answer_offers(&fleet, id, generation, frames, Arc::default()).0);
        }
        let landed = lands(&fleet, &movable("agent-batch")).await;
        if with_builds {
            assert_eq!(landed.ok().as_deref(), Some("builds"));
        } else {
            assert!(
                matches!(landed, Err(CellOutcome::Unavailable { .. })),
                "{landed:?}"
            );
        }
        settled(&fleet).await;
        let offered: usize = offers.iter().map(|o| o.load(Ordering::SeqCst)).sum();
        assert_eq!(offered, 0, "assignment alone offers nothing to anyone");
    }
}

/// A project that fetches a private GitHub repository goes to the machine
/// that measured a credential, not the roomier one that measured none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_github_credential_floor_holds_against_more_room() {
    let fleet = Fleet::default();
    let credential = |state| {
        move |entry: &mut ExecutorConnectionState| {
            Arc::make_mut(&mut entry.advertisement)
                .capabilities
                .github_credential = Some(GithubCredentialEvidence {
                state,
                measured_at_unix_ms: unix_time_ms(),
                probed_repositories: Vec::new(),
            });
        }
    };
    machine(
        &fleet,
        "no-credential",
        "linux",
        credential(GithubCredentialState::Absent {
            reason: "no credential helper answers for https://github.com".into(),
        }),
        room(8, 400),
    );
    machine(
        &fleet,
        "credential",
        "linux",
        credential(GithubCredentialState::Available {
            account: Some("bleugreen".into()),
            verified_against: None,
            note: None,
        }),
        room(8, 20),
    );

    let landed = lands(&fleet, &movable("public")).await;
    assert_eq!(
        landed.ok().as_deref(),
        Some("no-credential"),
        "the counterfactual: the most room left wins"
    );
    let mut private = movable("private");
    *private.project_requirements = ProjectPlacementRequirements {
        capabilities: vec![ProjectCapability::GithubCredential],
        ..ProjectPlacementRequirements::default()
    };
    let landed = lands(&fleet, &private).await;
    assert_eq!(landed.ok().as_deref(), Some("credential"));
}

/// Work goes only where its project's setup can run: the roomier machine
/// that advertises none of the setup's toolchains is passed over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_project_setup_gate_holds_against_more_room() {
    let fleet = Fleet::default();
    let toolchains = |names: &'static [&'static str]| {
        move |entry: &mut ExecutorConnectionState| {
            Arc::make_mut(&mut entry.advertisement)
                .capabilities
                .toolchains = names.iter().map(|name| name.to_string()).collect();
        }
    };
    machine(&fleet, "bare", "linux", toolchains(&[]), room(8, 400));
    machine(&fleet, "ready", "linux", toolchains(&["bun"]), room(8, 20));

    assert_eq!(
        lands(&fleet, &movable("no-setup")).await.ok().as_deref(),
        Some("bare"),
        "the counterfactual: the most room left wins"
    );
    let mut request = movable("bun-setup");
    *request.project_requirements = ProjectPlacementRequirements {
        setup_toolchains: vec!["bun".into()],
        ..ProjectPlacementRequirements::default()
    };
    assert_eq!(lands(&fleet, &request).await.ok().as_deref(), Some("ready"));
}

/// A pinned request goes to its pin, whatever room another machine has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_request_ignores_a_roomier_machine() {
    let fleet = Fleet::default();
    machine(&fleet, "pinned-home", "linux", |_| {}, room(4, 20));
    machine(&fleet, "roomy", "linux", |_| {}, room(8, 400));
    let mut request = movable("pinned");
    request.pinned_executor_id = Some("pinned-home".into());
    assert_eq!(
        lands(&fleet, &request).await.ok().as_deref(),
        Some("pinned-home")
    );
    assert_eq!(
        lands(&fleet, &movable("unpinned")).await.ok().as_deref(),
        Some("roomy"),
        "the counterfactual"
    );
}

/// Nested-Seatbelt evidence: a Mac whose check lanes can nest, one refused
/// nesting by the operator credential floor alone, and one refused outright.
fn nesting(capable: bool, floored: bool) -> SandboxCapabilityEvidence {
    let context = NestedSandboxProbeContext {
        launcher: SandboxLauncherClass::Unconfined,
        credential_floor_sha256: floored.then(|| "f1007f1007f1007".into()),
        outer_profile_sha256: floored.then(|| "outer".into()),
        fingerprint: Some("f".into()),
        measured_at_unix_ms: 1,
    };
    SandboxCapabilityEvidence {
        cairn_sandboxed_marker: false,
        nested_sandbox_write_available: capable,
        nested_sandbox_write_detail: if capable {
            "live probe succeeded".into()
        } else {
            "outer profile inherited: a confined process cannot apply a second profile".into()
        },
        nested_sandbox_write_context: Some(context),
        violation_recovery: None,
    }
}

fn with_sandbox(evidence: SandboxCapabilityEvidence) -> impl FnOnce(&mut ExecutorConnectionState) {
    move |entry| {
        Arc::make_mut(&mut entry.advertisement).capabilities.sandbox = Some(evidence);
    }
}

fn nested_sandbox_lane(id: &str) -> CellRequest {
    let mut request = movable(id);
    request.executor = Some(ExecutorSelector {
        os: Some("macos".into()),
        requires_macos_nested_sandbox_write: true,
        ..ExecutorSelector::default()
    });
    request.wait_horizon_unix_ms = unix_time_ms() + 3_000;
    request
}

/// A macOS lane needing a nested Seatbelt write, with the only Mac unable to
/// apply one, is refused naming that Mac and the capability it lacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_macos_refusal_names_the_excluded_mac_and_the_capability_it_lacks() {
    let fleet = Fleet::default();
    machine(
        &fleet,
        "mac-without",
        "macos",
        with_sandbox(nesting(false, false)),
        room(8, 400),
    );
    machine(&fleet, "linux-box", "linux", |_| {}, room(8, 400));
    let Err(CellOutcome::Unavailable { diagnostic, .. }) =
        lands(&fleet, &nested_sandbox_lane("lane")).await
    else {
        panic!("no Mac can nest, so the lane is refused");
    };
    assert!(diagnostic.contains("mac-without"), "{diagnostic}");
    assert!(diagnostic.contains("nested Seatbelt write"), "{diagnostic}");
}

/// A Mac refused nesting only by the credential floor takes a lane needing
/// it only when no Mac that can nest could: with a capable Mac attached and
/// room on it, the lane goes there, however much more room the floored Mac
/// has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lane_needing_nesting_prefers_a_capable_mac_to_a_floored_one() {
    let fleet = Fleet::default();
    machine(
        &fleet,
        "mac-floored",
        "macos",
        with_sandbox(nesting(false, true)),
        room(8, 400),
    );
    machine(
        &fleet,
        "mac-capable",
        "macos",
        with_sandbox(nesting(true, false)),
        room(8, 20),
    );
    assert_eq!(
        lands(&fleet, &nested_sandbox_lane("lane"))
            .await
            .ok()
            .as_deref(),
        Some("mac-capable")
    );
}

/// Enqueue `request` and join it to the dispatcher directly, as
/// `await_assignment` does on first use, keeping its channel.
// The outcome owed, as `join_dispatcher` returns it.
#[allow(clippy::result_large_err)]
async fn join(
    fleet: &Fleet,
    request: &CellRequest,
    plan: Option<Arc<ReservationPlan>>,
) -> (
    super::assignments::JoinedDispatcher,
    Result<mpsc::UnboundedReceiver<super::assignments::Delivery>, CellOutcome>,
) {
    let (entry_id, waiter) = fleet
        .queue
        .enqueue(new_entry(request), unix_time_ms())
        .expect("the request enqueues");
    // The entry is the queue's for as long as the test holds the guard.
    std::mem::forget(waiter);
    let joined = super::assignments::JoinedDispatcher {
        pool: fleet.clone(),
        entry_id: entry_id.clone(),
    };
    let policy = ActivePlacementPolicy::default_profile();
    let deliveries = fleet
        .join_dispatcher(&entry_id, request, plan, &policy)
        .await;
    (joined, deliveries)
}

/// The next thing `deliveries` is told, within ten seconds.
async fn next_delivery(
    deliveries: &mut mpsc::UnboundedReceiver<super::assignments::Delivery>,
) -> super::assignments::Delivery {
    tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await
        .expect("the dispatcher tells the entry something within ten seconds")
        .expect("the entry's channel stays open")
}

/// A machine's link made after the runner is up, as the listener makes it:
/// attached, then announcing a full room. Its frames are kept open.
fn attach_late(fleet: &Fleet, id: &str) {
    let (generation, frames) = attach(fleet, id, "linux", |_| {});
    fleet.dispatcher_attached(id, generation);
    announce(fleet, id, room(0, 500));
    answer_offers(fleet, id, generation, frames, Arc::default());
}

/// Waits up to five seconds for the dispatcher to hold `entry_id` a
/// candidate on exactly `expected`.
async fn candidacy(fleet: &Fleet, entry_id: &str, expected: &[&str]) {
    let deadline = StdInstant::now() + Duration::from_secs(5);
    loop {
        let now = fleet.dispatcher_candidates_for_test(entry_id);
        if now == expected {
            return;
        }
        assert!(
            StdInstant::now() < deadline,
            "{entry_id} is a candidate on {now:?}, not {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Work only one machine may take, which `admit` turned away there (its link
/// was stale), is offered on that machine's next room frame once `admit`
/// would take it, though the frame repeats the figures before it: the frame
/// clears the runner's note that `admit` turned work away there, and nothing
/// after it would ask again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_room_frame_repeating_its_figures_offers_work_admit_turned_away() {
    let fleet = Fleet::default();
    let (generation, frames) = attach(&fleet, "x", "linux", |_| {});
    fleet.dispatcher_attached("x", generation);
    announce(&fleet, "x", room(0, 500));
    let mut request = movable("pinned-to-x");
    request.pinned_executor_id = Some("x".into());
    let (joined, deliveries) = join(&fleet, &request, None).await;
    let mut deliveries = match deliveries {
        Ok(deliveries) => deliveries,
        Err(outcome) => panic!("refused: {outcome:?}"),
    };
    candidacy(&fleet, &joined.entry_id, &["x"]).await;
    settled(&fleet).await;

    let clock = fleet.link_clock("x", generation).expect("x is attached");
    clock.stage(EXECUTOR_PROGRESS_FRESHNESS_MS + 1_000, 0);
    announce(&fleet, "x", room(8, 500));
    settled(&fleet).await;
    assert!(
        deliveries.try_recv().is_err(),
        "the stale link is turned away"
    );

    clock.stage(0, 0);
    announce(&fleet, "x", room(8, 500));
    match next_delivery(&mut deliveries).await {
        super::assignments::Delivery::Assigned(assignment) => {
            assert_eq!(assignment.executor_id, "x")
        }
        other => panic!("{other:?}"),
    }
    drop(frames);
}

/// "Can never fit" is judged against what a machine can ever hold. Work that
/// joins while the only machine is pressure-braked, or provisioning the
/// work's project, waits for that to lift and is not refused; it is assigned
/// there once the machine announces the condition gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn work_joining_while_its_machine_is_braked_or_provisioning_waits_for_it() {
    for condition in ["braked", "provisioning"] {
        let fleet = Fleet::default();
        let request = movable(&format!("transient-{condition}"));
        let mut held = room(8, 500);
        if condition == "braked" {
            held.closures.push(RoomClosure {
                scope: RoomScope::NonInteractive,
                reason: DeclineReason::Pressure,
                detail: None,
                disk: None,
                conditions: vec![HostPressureCondition::CpuUtilization {
                    utilization: 0.97,
                    entry_utilization: 0.9,
                    clear_utilization: 0.8,
                    state_since_unix_ms: unix_time_ms(),
                    measured_at_unix_ms: unix_time_ms(),
                }],
            });
        } else {
            held.provisioning.push(ProvisioningRoomHold {
                project_id: request.project_id.clone(),
                request_id: "another".into(),
                since_unix_ms: unix_time_ms(),
            });
        }
        refusing(&fleet, "m", "linux", held);
        let (_joined, deliveries) = join(&fleet, &request, None).await;
        let mut deliveries = match deliveries {
            Ok(deliveries) => deliveries,
            Err(outcome) => panic!("{condition}: refused at join: {outcome:?}"),
        };
        announce(&fleet, "m", room(8, 500));
        match next_delivery(&mut deliveries).await {
            super::assignments::Delivery::Assigned(assignment) => {
                assert_eq!(assignment.executor_id, "m", "{condition}")
            }
            other => panic!("{condition}: {other:?}"),
        }
    }
}

/// A machine that attaches after a joining entry's needs were worked out,
/// but before the entry is in the dispatcher, becomes its candidate: the
/// join looks again at the machines attached since its snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_machine_attached_while_an_entry_joins_becomes_its_candidate() {
    let fleet = Fleet::default();
    refusing(&fleet, "a", "linux", room(0, 500));
    fleet.after_figure_snapshot_for_test(Box::new(|fleet| {
        Box::pin(async move { attach_late(&fleet, "b") })
    }));
    let request = movable("joins-during-attach");
    let (joined, deliveries) = join(&fleet, &request, None).await;
    assert!(deliveries.is_ok(), "parks");
    candidacy(&fleet, &joined.entry_id, &["a", "b"]).await;
}

/// A re-figure across every machine, from a snapshot taken before a machine
/// attached, does not take that machine's candidacy away when the attach's
/// own re-figure committed first. The re-figure is the profile store's:
/// a write naming the entry's identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_figure_from_an_older_snapshot_keeps_a_machine_attached_since() {
    let db = Arc::new(crate::storage::migrated_test_db("dispatcher-refigure-race.db").await);
    let fleet = Fleet::default();
    refusing(&fleet, "a", "linux", room(0, 500));
    let request = compile("learns-during-attach", "test:race");
    let plan = Arc::new(ReservationPlan::new(db.clone(), &request, None));
    let (joined, deliveries) = join(&fleet, &request, Some(plan)).await;
    assert!(deliveries.is_ok(), "parks");
    let entry_id = joined.entry_id.clone();
    candidacy(&fleet, &entry_id, &["a"]).await;

    let attached = Arc::new(AtomicBool::new(false));
    fleet.after_figure_snapshot_for_test(Box::new({
        let attached = attached.clone();
        let entry_id = entry_id.clone();
        move |fleet| {
            Box::pin(async move {
                attach_late(&fleet, "b");
                // The attach's own re-figure commits before this one does.
                candidacy(&fleet, &entry_id, &["a", "b"]).await;
                attached.store(true, Ordering::SeqCst);
            })
        }
    }));
    let scope = profile_reads::identity_scope(&CommandResourceIdentity {
        version: 1,
        key: "test:race".into(),
        suite: None,
    });
    drop(profile_reads::Writing::teaching(&db, vec![scope]));
    until("the attach inside the re-figure", || {
        attached.load(Ordering::SeqCst)
    })
    .await;
    settled(&fleet).await;
    assert_eq!(
        fleet.dispatcher_candidates_for_test(&entry_id),
        ["a", "b"],
        "the older snapshot did not take b's candidacy away"
    );
}

/// The deadline task survives a panic: it is restarted, and the deadline it
/// panicked at, and a later one, still fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_task_is_restarted_after_a_panic_and_deadlines_still_fire() {
    let fleet = Fleet::default();
    refusing(&fleet, "m", "linux", room(0, 500));
    fleet.panic_at_next_deadline_for_test();
    for n in 0..2 {
        let mut request = movable(&format!("short-{n}"));
        request.wait_horizon_unix_ms = unix_time_ms() + 300;
        let (_joined, deliveries) = join(&fleet, &request, None).await;
        let mut deliveries = deliveries.expect("parks");
        assert!(
            matches!(
                next_delivery(&mut deliveries).await,
                super::assignments::Delivery::Expired(_)
            ),
            "entry {n} expires at its horizon"
        );
    }
}

/// The runner's restart, as the dispatcher sees it: a fresh fleet that
/// knows its enrolled machine but has no link yet, and a request its owner
/// re-drives at boot (a check request's `rearm_at_boot`, a main-cadence
/// lane's `arm_at_boot`). The request waits rather than being refused, and
/// is assigned once the machine attaches and announces room.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_re_driven_at_boot_before_any_machine_attaches_lands_when_one_does() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("m", "m", "linux", "x86_64");
    let request = movable("re-driven-at-boot");
    let (joined, deliveries) = join(&fleet, &request, None).await;
    let mut deliveries = match deliveries {
        Ok(deliveries) => deliveries,
        Err(outcome) => panic!("refused at boot: {outcome:?}"),
    };
    let (generation, frames) = attach(&fleet, "m", "linux", |_| {});
    fleet.dispatcher_attached("m", generation);
    candidacy(&fleet, &joined.entry_id, &["m"]).await;
    announce(&fleet, "m", room(8, 500));
    match next_delivery(&mut deliveries).await {
        super::assignments::Delivery::Assigned(assignment) => {
            assert_eq!(assignment.executor_id, "m")
        }
        other => panic!("{other:?}"),
    }
    drop(frames);
}
