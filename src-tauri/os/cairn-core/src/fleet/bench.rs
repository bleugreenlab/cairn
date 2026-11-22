//! What waiting for a machine costs the runner, measured.
//!
//! Parks N requests on ten machines that announce room and decline every
//! offer, through the same dispatcher, offer path and frame handler the runner
//! uses, and measures a window of real time: process CPU, placement passes
//! (the dispatcher's matches), fit evaluations, offers, refusals and database
//! statements. A refusal stands under the room the machine announced, and no
//! machine announces newer room here, so once every machine has refused every
//! request nothing is placed again. Two modes:
//!
//! - silent: every link is kept fresh (its progress stamp moved, as a beat
//!   would) and nothing new is posted;
//! - posting: every machine posts what a live executor does, through the
//!   runner's own handlers: a sample twice a second, a snapshot every five
//!   seconds and a beat every thirty.
//!
//! Each window ends by asserting that no machine reads as a stale link and
//! that every parked request is still waiting, so a row measures a live fleet
//! declining, never one placement has stopped considering.
//!
//! Each row also reports what parking cost: the needs worked out and the
//! statements read in the five seconds before the window, while every request
//! is first placed. That is also what a runner restart costs, because a
//! restart closes the queue and every request placed again is placed afresh.
//!
//! Ignored by default because each row takes its whole window. Run it by name:
//!
//! ```text
//! bun run test:rust -p cairn-core --lib -- fleet::bench --ignored --nocapture
//! ```
//!
//! `CAIRN_PARK_BENCH_SECS` sets the window (120 by default),
//! `CAIRN_PARK_BENCH_N` the parked counts (`0,50,200` by default) and
//! `CAIRN_PARK_BENCH_IDENTITIES` whether the parked requests run one command
//! (`shared`, the default and the step 1 baseline's shape) or one each
//! (`distinct`, which is what a live queue holds and the shape a per-request
//! read shows up under rather than hiding behind one shared memo).

use std::time::Instant;

use cairn_common::executor_protocol::{
    CellCommandClass, CpuPressure, DeclineReason, GitObjectFormat, MachineMemory, MachineVolume,
};
use cairn_common::wire_vocabulary::{CellAdmissionKind, Measurement};

use super::tests::{fleet_entry_at, measured_at, spillable_request};
use super::*;

const MACHINES: usize = 10;
const GIB: u64 = 1024 * 1024 * 1024;

struct Measured {
    parked: usize,
    posting: bool,
    window_s: f64,
    cpu_s: f64,
    counts: counters::PlacementCounts,
    statements: usize,
    /// What placing every request the first time cost, before the window.
    parking: counters::PlacementCounts,
    parking_statements: usize,
}

/// Which command each parked request runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Identities {
    Shared,
    Distinct,
}

fn process_cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the whole struct it is handed, which is
    // zero-initialised besides.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// Ten fresh, roomy machines, and the frames this runner sends each.
fn fleet_of_machines(
    now: u64,
) -> (
    Fleet,
    Vec<(String, mpsc::UnboundedReceiver<ExecutorMessage>)>,
) {
    let fleet = Fleet::default();
    let mut frames = Vec::new();
    let mut connections = HashMap::new();
    for n in 0..MACHINES {
        let (id, mut entry) = fleet_entry_at(&format!("bench-{n}"), "linux", 0, &[], now);
        Arc::make_mut(&mut entry.advertisement)
            .capabilities
            .memory_budget_bytes = Some(100 * GIB);
        measured_at(&mut entry, 0.2, 60 * GIB, 500 * GIB, now);
        let (sender, receiver) = mpsc::unbounded_channel();
        entry.sender = sender;
        entry.snapshots_received = 1;
        frames.push((id.clone(), receiver));
        connections.insert(id, entry);
    }
    let generations: Vec<(String, u64)> = connections
        .iter()
        .map(|(id, entry)| (id.clone(), entry.generation))
        .collect();
    *fleet.lock_connections() = connections;
    // A real executor announces its room as it attaches.
    for (id, generation) in generations {
        fleet.announce_room_for_test(&id, generation);
    }
    (fleet, frames)
}

fn parked_request(n: usize, now: u64, identities: Identities) -> CellRequest {
    let mut request = spillable_request();
    request.request_id = format!("parked-{n}");
    request.wait_horizon_unix_ms = now + 60 * 60 * 1_000;
    request.command_class = CellCommandClass::CargoTest;
    request.command_resource_identity = Some(CommandResourceIdentity {
        version: 1,
        key: match identities {
            Identities::Shared => "test:rust".into(),
            Identities::Distinct => format!("test:rust-{n}"),
        },
        suite: None,
    });
    request.repository = RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "repo".into(),
        object_format: GitObjectFormat::Sha1,
    };
    request
}

/// One request's wait, as `execute_entry` runs it: wait for the dispatcher to
/// assign it a machine, offer it there, and wait again on a decline.
async fn wait_for_a_machine(
    fleet: Fleet,
    db: Arc<cairn_db::storage::LocalDb>,
    request: CellRequest,
) {
    let plan = Arc::new(ReservationPlan::new(db, &request, None));
    let policy = ActivePlacementPolicy::default_profile();
    let Ok((entry_id, _outcome)) = fleet.queue.enqueue(
        queue::NewEntry {
            request: request.clone(),
            admission_kind: CellAdmissionKind::Command,
            interactive: false,
            execution_residency: None,
            standing: false,
        },
        unix_time_ms(),
    ) else {
        return;
    };
    let _joined = super::assignments::JoinedDispatcher {
        pool: fleet.clone(),
        entry_id: entry_id.clone(),
    };
    let mut deliveries = None;
    loop {
        let Ok(placement) = fleet
            .await_assignment(&entry_id, &request, Some(&plan), &policy, &mut deliveries)
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

/// A machine that refuses every offer it is sent, through the runner's own
/// frame handler.
async fn decline_every_offer(
    fleet: Fleet,
    executor_id: String,
    mut frames: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    while let Some(frame) = frames.recv().await {
        if let ExecutorMessage::Offer { entry_id, .. } = frame {
            fleet.handle_executor_message(
                &executor_id,
                1,
                ExecutorMessage::Decline {
                    entry_id,
                    reason: DeclineReason::Pressure,
                    detail: Some("the bench declines every offer".into()),
                    disk: None,
                },
            );
        }
    }
}

/// Every machine posting a fresh sample twice a second, round-robin.
async fn post_samples(fleet: Fleet, executor_ids: Vec<String>) {
    let mut tick = tokio::time::interval(Duration::from_millis(500 / executor_ids.len() as u64));
    let mut n = 0usize;
    loop {
        n = n.wrapping_add(1);
        tick.tick().await;
        let now = unix_time_ms();
        let sample = cairn_common::executor_protocol::MachineSample {
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
        };
        fleet.record_machine_sample(&executor_ids[n % executor_ids.len()], 1, sample);
    }
}

/// Every machine sending one `frame` per `period`, round-robin, built from
/// what it has posted so far.
async fn post_each(
    fleet: Fleet,
    executor_ids: Vec<String>,
    period: Duration,
    frame: impl Fn(&ExecutorConnectionState, u64) -> ExecutorMessage,
) {
    let mut tick = tokio::time::interval(period / executor_ids.len() as u32);
    let mut n = 0usize;
    loop {
        n = n.wrapping_add(1);
        tick.tick().await;
        let executor_id = &executor_ids[n % executor_ids.len()];
        let Some(message) = fleet
            .lock_connections()
            .get(executor_id)
            .map(|entry| frame(entry, unix_time_ms()))
        else {
            continue;
        };
        fleet.handle_executor_message(executor_id, 1, message);
    }
}

fn snapshot_frame(entry: &ExecutorConnectionState, _now: u64) -> ExecutorMessage {
    ExecutorMessage::SnapshotUpdated {
        snapshot: (*entry.snapshot).clone(),
        health: (*entry.health).clone(),
    }
}

fn beat_frame(entry: &ExecutorConnectionState, now: u64) -> ExecutorMessage {
    let mut advertisement = (*entry.advertisement).clone();
    advertisement.observed_at_unix_ms = now;
    ExecutorMessage::Heartbeat {
        advertisement,
        health: (*entry.health).clone(),
        last_update: None,
        machine: None,
    }
}

/// Every link's progress stamp moved every five seconds, and nothing else.
async fn keep_links_fresh(fleet: Fleet) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        let now = unix_time_ms();
        for entry in fleet.lock_connections().values_mut() {
            entry.last_progress_unix_ms = now;
        }
    }
}

async fn measure(
    parked: usize,
    posting: bool,
    window: Duration,
    identities: Identities,
) -> Measured {
    let now = unix_time_ms();
    let db = Arc::new(
        crate::storage::migrated_test_db(&format!("park-bench-{parked}-{posting}.db")).await,
    );
    let before_parking = fleet_counts_and_statements(None, &db);
    let (fleet, frames) = fleet_of_machines(now);
    let executor_ids: Vec<String> = frames.iter().map(|(id, _)| id.clone()).collect();
    let mut tasks = Vec::new();
    if posting {
        tasks.push(cairn_common::task::spawn(post_each(
            fleet.clone(),
            executor_ids.clone(),
            Duration::from_secs(30),
            beat_frame,
        )));
    } else {
        tasks.push(cairn_common::task::spawn(keep_links_fresh(fleet.clone())));
    }
    for (executor_id, frames) in frames {
        tasks.push(cairn_common::task::spawn(decline_every_offer(
            fleet.clone(),
            executor_id,
            frames,
        )));
    }
    let waits: Vec<_> = (0..parked)
        .map(|n| {
            cairn_common::task::spawn(wait_for_a_machine(
                fleet.clone(),
                db.clone(),
                parked_request(n, now, identities),
            ))
        })
        .collect();
    // Every request is offered and declined at the start; the window measures
    // the wait after that, which is what grows with N.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (parking, parking_statements) = {
        let (counts, statements) = fleet_counts_and_statements(Some(&fleet), &db);
        let (earlier, earlier_statements) = before_parking;
        (counts.since(earlier), statements - earlier_statements)
    };
    if posting {
        tasks.push(cairn_common::task::spawn(post_samples(
            fleet.clone(),
            executor_ids.clone(),
        )));
        tasks.push(cairn_common::task::spawn(post_each(
            fleet.clone(),
            executor_ids,
            Duration::from_secs(5),
            snapshot_frame,
        )));
    }
    let counts = fleet.placement_counters().totals();
    let statements = db.read_statement_count();
    let cpu = process_cpu_seconds();
    let started = Instant::now();
    tokio::time::sleep(window).await;
    let measured = Measured {
        parked,
        posting,
        window_s: started.elapsed().as_secs_f64(),
        cpu_s: process_cpu_seconds() - cpu,
        counts: fleet.placement_counters().totals().since(counts),
        statements: db.read_statement_count() - statements,
        parking,
        parking_statements,
    };
    let now = unix_time_ms();
    let stale: Vec<String> = fleet
        .lock_connections()
        .values()
        .filter(|entry| super::residency_routes::link_silence_ms(entry, now).is_some())
        .map(|entry| entry.identity.executor_id.clone())
        .collect();
    let ended = waits.iter().filter(|wait| wait.is_finished()).count();
    for task in tasks.into_iter().chain(waits) {
        task.abort();
    }
    assert!(
        stale.is_empty(),
        "every machine stays live through the window; stale: {stale:?}"
    );
    assert_eq!(
        ended, 0,
        "every parked request is still waiting at the end of the window"
    );
    measured
}

/// The placement counts so far and the statements read so far. A fleet not
/// yet built has counted nothing.
fn fleet_counts_and_statements(
    fleet: Option<&Fleet>,
    db: &cairn_db::storage::LocalDb,
) -> (counters::PlacementCounts, usize) {
    (
        fleet.map_or_else(Default::default, |fleet| {
            fleet.placement_counters().totals()
        }),
        db.read_statement_count(),
    )
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// One table per mode, so each stays inside the test runner's per-test bound
/// at the default window.
async fn print_table(posting: bool) {
    let window = Duration::from_secs(env_or("CAIRN_PARK_BENCH_SECS", 120));
    let sizes: Vec<usize> = std::env::var("CAIRN_PARK_BENCH_N")
        .unwrap_or_else(|_| "0,50,200".into())
        .split(',')
        .filter_map(|n| n.trim().parse().ok())
        .collect();
    let identities = match std::env::var("CAIRN_PARK_BENCH_IDENTITIES").as_deref() {
        Ok("distinct") => Identities::Distinct,
        _ => Identities::Shared,
    };
    println!("| Parked | Machines | Window s | CPU cores | Passes/s | Fit evaluations/s | Offers | Refusals | Statements/s | Needs worked out in window | Parking: needs worked out | Parking: statements |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for &parked in &sizes {
        let row = measure(parked, posting, window, identities).await;
        println!(
            "| {} | {} | {:.0} | {:.2} | {:.1} | {:.1} | {} | {} | {:.1} | {} | {} | {} |",
            row.parked,
            if row.posting { "posting" } else { "silent" },
            row.window_s,
            row.cpu_s / row.window_s,
            row.counts.placement_passes as f64 / row.window_s,
            row.counts.fit_evaluations as f64 / row.window_s,
            row.counts.offers,
            row.counts.refusals,
            row.statements as f64 / row.window_s,
            row.counts.needs_worked_out,
            row.parking.needs_worked_out,
            row.parking_statements,
        );
    }
}

/// Polls `done` until it holds, failing the test after thirty seconds.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// What the dispatcher has done so far, as one comparable value.
fn work_done(fleet: &Fleet) -> (u64, u64, u64, u64, u64) {
    let totals = fleet.placement_counters().totals();
    (
        totals.placement_passes,
        totals.fit_evaluations,
        totals.offers,
        totals.refusals,
        totals.needs_worked_out,
    )
}

/// Waits until the dispatcher's counters have stood still for half a second:
/// every offer answered, every refusal recorded, nothing left to work out.
async fn settled(fleet: &Fleet) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = work_done(fleet);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = work_done(fleet);
        if now == last {
            return;
        }
        assert!(Instant::now() < deadline, "the dispatcher never settled");
        last = now;
    }
}

/// A parked request costs nothing while nothing it waits on moves: no
/// placement work, no needs worked out, no statement read, with every machine
/// posting samples. One command finishing (one profile write, so one
/// publication of moved needs) works out that command's needs again, at most
/// once per machine, and no other request's. Each request runs its own
/// command, so a read per request cannot hide behind one shared memo.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_requests_read_nothing_and_a_finish_redoes_only_its_own_needs() {
    const PARKED: usize = 20;
    let now = unix_time_ms();
    let db = Arc::new(crate::storage::migrated_test_db("parked-passes-read-nothing.db").await);
    let (fleet, frames) = fleet_of_machines(now);
    let executor_ids: Vec<String> = frames.iter().map(|(id, _)| id.clone()).collect();
    let mut tasks = vec![cairn_common::task::spawn(keep_links_fresh(fleet.clone()))];
    for (executor_id, frames) in frames {
        tasks.push(cairn_common::task::spawn(decline_every_offer(
            fleet.clone(),
            executor_id,
            frames,
        )));
    }
    let requests: Vec<CellRequest> = (0..PARKED)
        .map(|n| parked_request(n, now, Identities::Distinct))
        .collect();
    let waits: Vec<_> = requests
        .iter()
        .map(|request| {
            cairn_common::task::spawn(wait_for_a_machine(
                fleet.clone(),
                db.clone(),
                request.clone(),
            ))
        })
        .collect();
    tasks.push(cairn_common::task::spawn(post_samples(
        fleet.clone(),
        executor_ids.clone(),
    )));
    let totals = || fleet.placement_counters().totals();

    // Parked: every request has been offered and refused, and the dispatcher
    // has nothing left to do.
    until("every request to be offered", || {
        totals().offers >= PARKED as u64
    })
    .await;
    settled(&fleet).await;
    assert!(
        totals().needs_worked_out >= PARKED as u64,
        "every request's needs were worked out when it joined"
    );

    // Waiting: samples go on, and a parked request costs nothing at all.
    let (before, statements_before) = (totals(), db.read_statement_count());
    tokio::time::sleep(Duration::from_secs(5)).await;
    let waited = totals().since(before);
    assert_eq!(
        (
            waited.placement_passes,
            waited.fit_evaluations,
            waited.needs_worked_out
        ),
        (0, 0, 0),
        "a parked request is not placed again while nothing it waits on moves"
    );
    assert_eq!(
        db.read_statement_count() - statements_before,
        0,
        "a parked request reads nothing"
    );

    // One command finishes on one machine: only its request's needs are
    // worked out again, once on each machine it may run on.
    let finished = fleet
        .lock_connections()
        .get(&executor_ids[0])
        .map(|entry| {
            ReservationPlan::context_for(
                &entry.identity.device_id,
                &entry.identity.executor_id,
                &entry.advertisement.capabilities,
            )
        })
        .unwrap();
    let before = totals();
    resource_profiles::record_completion_for_test(
        db.clone(),
        requests[0].command_resource_identity.as_ref().unwrap(),
        &finished,
        6 * GIB,
    )
    .await;
    until(
        "the finished command's needs to be worked out again",
        || totals().needs_worked_out > before.needs_worked_out,
    )
    .await;
    settled(&fleet).await;
    let redone = totals().since(before);
    assert!(
        (1..=MACHINES as u64).contains(&redone.needs_worked_out),
        "one finish redoes one request's needs, at most once per machine; redone {}",
        redone.needs_worked_out
    );
    assert_eq!(
        redone.offers, 0,
        "every machine refused it under the room it still announces, so nothing is offered"
    );

    let ended = waits.iter().filter(|wait| wait.is_finished()).count();
    for task in tasks.into_iter().chain(waits) {
        task.abort();
    }
    assert_eq!(ended, 0, "every parked request is still waiting");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement taking its whole window per row; run it by name with --ignored --nocapture"]
async fn what_parked_requests_cost_while_machines_are_silent() {
    print_table(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement taking its whole window per row; run it by name with --ignored --nocapture"]
async fn what_parked_requests_cost_while_machines_post_samples() {
    print_table(true).await;
}
