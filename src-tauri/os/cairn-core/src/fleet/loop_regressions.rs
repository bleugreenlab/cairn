//! The two placement loops cairn/7303 was written against, replayed through
//! the runner's own wire handlers: a fake machine announces its room, refuses
//! every offer it is sent, and posts samples while the request waits.
//!
//! - 10-16: a `Pressure` refusal that names no posted condition, with the
//!   machine posting a sample every 200 ms. The per-request loop treated every
//!   post as news and asked again.
//! - 11-15: an aged request entitled to hold room, on a machine with no room
//!   free, refused with 'holding headroom for' another entry while samples
//!   flow. The per-request loop re-offered an entitled request at once.
//!
//! Each allows at most two offers over the window: the refusal stands under
//! the room the machine announced, and nothing here announces newer room.
//!
//! Real time, not paused tokio time: the loop these replace paced its
//! re-offers by wall-clock backoffs, which paused time would freeze, hiding
//! the loop rather than bounding it. Written only against the fleet's wire
//! surface so the same file runs against the code before the dispatcher.

use std::sync::atomic::AtomicUsize;

use cairn_common::executor_protocol::{
    Allowance, AnnouncedRoom, CompileSlots, CpuPressure, DiskRoom, MachineMemory, MachineVolume,
    MemoryRoom,
};
use cairn_common::wire_vocabulary::Measurement;

use super::tests::targeted_request;
use super::*;
use crate::db::DbState;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};

const GIB: u64 = 1 << 30;
/// How long each loop is watched.
const WINDOW: Duration = Duration::from_secs(10);
const SAMPLE_EVERY: Duration = Duration::from_millis(200);
/// The most offers the window may hold: the first, and one spare.
const AT_MOST: usize = 2;

async fn orchestrator(dir: &std::path::Path) -> Arc<Orchestrator> {
    let local = LocalDb::open_migrated_for_test(dir.join("build-slots.db"))
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(dir.join("search")).unwrap());
    Arc::new(
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            dir.to_path_buf(),
        )
        .build(),
    )
}

/// An eight-core machine's room: all of it free, or every core given out
/// with the memory and disk to run more once that work ends.
fn room(full: bool) -> AnnouncedRoom {
    AnnouncedRoom {
        slots: CompileSlots::of(8),
        compiling: u32::from(full),
        test_cores: 0,
        standing: Default::default(),
        cores: Allowance {
            limit: 8,
            committed: if full { 8 } else { 0 },
        },
        memory: Some(MemoryRoom {
            available_bytes: 48 * GIB,
            owed_bytes: 0,
            total_bytes: 64 * GIB,
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

/// The `n`th sample a busy machine posts: its readings move a little each
/// time, as a live machine's do.
fn sample(n: u64) -> MachineSample {
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
                available_bytes: 48 * GIB - (n % 5) * GIB / 8,
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

/// The request's offers and the runner's reservation count over the window,
/// with the machine announcing `room` once, refusing every offer with
/// `reason` and `detail`, and posting a sample every 200 ms.
async fn offers_over_the_window(
    room: AnnouncedRoom,
    reason: DeclineReason,
    detail: &'static str,
    request: CellRequest,
) -> (usize, u64) {
    let temp = tempfile::tempdir().unwrap();
    let orch = orchestrator(temp.path()).await;
    let fleet = orch.fleet.clone();
    let (sender, mut frames) = mpsc::unbounded_channel();
    let generation = fleet.attach_executor(sender);
    fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Room { seq: 1, room },
    );
    let offers = Arc::new(AtomicUsize::new(0));
    let machine = tokio::spawn({
        let fleet = fleet.clone();
        let offers = offers.clone();
        async move {
            while let Some(frame) = frames.recv().await {
                if let ExecutorMessage::Offer { entry_id, .. } = frame {
                    offers.fetch_add(1, Ordering::SeqCst);
                    fleet.handle_executor_message(
                        COLOCATED_EXECUTOR_ID,
                        generation,
                        ExecutorMessage::Decline {
                            entry_id,
                            reason,
                            detail: Some(detail.into()),
                            disk: None,
                        },
                    );
                }
            }
        }
    });
    let samples = tokio::spawn({
        let fleet = fleet.clone();
        async move {
            let mut tick = tokio::time::interval(SAMPLE_EVERY);
            let mut n = 0;
            loop {
                tick.tick().await;
                fleet.record_machine_sample(COLOCATED_EXECUTOR_ID, generation, sample(n));
                n += 1;
            }
        }
    });
    let waiting = tokio::spawn({
        let orch = orch.clone();
        async move { orch.fleet.submit(&orch, request).await }
    });
    tokio::time::sleep(WINDOW).await;
    let offered = offers.load(Ordering::SeqCst);
    let reservations = fleet.placement_counters().totals().reservations;
    let ended = waiting.is_finished();
    for task in [machine, samples] {
        task.abort();
    }
    waiting.abort();
    assert!(
        !ended,
        "the request is still waiting at the end of the window"
    );
    (offered, reservations)
}

/// A request with nothing to prepare, waiting well past the window.
fn waiting_request(id: &str) -> CellRequest {
    let mut request = targeted_request("linux");
    request.request_id = id.into();
    request.executor = None;
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    request.resource_reservation.concurrency_units = 4;
    request.resource_reservation.memory_bytes = 4 * GIB;
    request.wait_horizon_unix_ms = unix_time_ms() + 10 * 60_000;
    request
}

/// 10-16: a refusal for pressure the machine never posted a condition for,
/// with samples arriving five times a second, is asked again at most once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pressure_refusal_naming_no_condition_is_not_re_asked_on_every_sample() {
    let (offers, _) = offers_over_the_window(
        room(false),
        DeclineReason::Pressure,
        "frobnicator says no",
        waiting_request("loop-10-16-pressure"),
    )
    .await;
    assert!(
        (1..=AT_MOST).contains(&offers),
        "{offers} offers in {WINDOW:?} of samples every {SAMPLE_EVERY:?}"
    );
}

/// The 10-16 shape for a refusal nothing the machine posts can be tied to:
/// a reason the runner does not classify is paced like any other, by room,
/// and every sample is not a reason to ask again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unclassified_refusal_is_not_re_asked_on_every_sample() {
    let (offers, _) = offers_over_the_window(
        room(false),
        DeclineReason::Unknown,
        "frobnicator says no",
        waiting_request("loop-10-16-unknown"),
    )
    .await;
    assert!(
        (1..=AT_MOST).contains(&offers),
        "{offers} offers in {WINDOW:?} of samples every {SAMPLE_EVERY:?}"
    );
}

/// 11-15: an aged request entitled to hold room, refused by a machine holding
/// headroom for another entry, is asked again at most once while samples
/// flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aged_request_refused_for_another_entrys_headroom_is_not_re_asked_on_every_sample() {
    let mut request = waiting_request("loop-11-15");
    // Waiting longer than the headroom wait: entitled to hold room.
    request.waiting_since_unix_ms = unix_time_ms() - 20 * 60_000;
    let (offers, reservations) = offers_over_the_window(
        room(true),
        DeclineReason::Capacity,
        "holding headroom for entry X (waiting 22m)",
        request,
    )
    .await;
    assert!(
        (1..=AT_MOST).contains(&offers),
        "{offers} offers in {WINDOW:?} of samples every {SAMPLE_EVERY:?}"
    );
    assert!(
        reservations >= 1,
        "the aged request asked the full machine to hold room"
    );
}
