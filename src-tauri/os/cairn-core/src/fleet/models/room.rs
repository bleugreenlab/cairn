//! Room: what the model residents on a machine take from the memory batch
//! work uses, and how a loading model drains the work already on its gang.
//!
//! A resident's reservation on a machine is its footprint there, and the part
//! it holds back from batches is the part it has not taken yet: `footprint -
//! allocated now`. A loading model therefore keeps nearly all of its footprint
//! off-limits, and a healthy one keeps almost none, so batches run beside it as
//! measured room allows. A model held in discrete VRAM takes nothing batches
//! use and never refuses one.
//!
//! A reserving start goes on FIT, not on empty: once every gang machine's
//! measured free memory covers the model's need, work still running there is
//! no reason to wait, and short of it only the memory it needs is drained
//! ([`drain`]). Every start has a need to drain for: measured, stated, or
//! predicted from what its profile runs
//! ([`crate::orchestrator::inference_control::need_of`]); one with nothing to
//! predict from is refused before it reaches the drain.

use std::collections::BTreeMap;

use cairn_common::executor_protocol::{
    model_gpus, GpuMemory, HostHealth, MachineModelResident, MeasurementReading,
    PlacementRejectionReason,
};
use cairn_inference::control::gib;

/// How long a reserving start lets the restartable batch work already on its
/// machines run before it preempts the checks whose measured memory is in its
/// way. Well inside the act side's fit bound
/// ([`cairn_inference::control::FIT_WAIT_SECS`]), so preempted work has time to
/// end and its memory to be measured free before the start gives up.
pub const DRAIN_BOUND_SECS: u64 = 300;

/// The memory a machine's GPUs hold: the GPU-attributed share on a
/// unified-memory machine, VRAM used on discrete cards. `None` when no GPU
/// memory was measured. Only the GPUs a model runs on count
/// ([`model_gpus`]), so an integrated GPU's system memory is never added to a
/// card's VRAM.
pub fn gpu_allocated_bytes(host: &HostHealth) -> Option<u64> {
    let MeasurementReading::Measured { value: devices } = &host.gpus.reading else {
        return None;
    };
    let mut allocated: Option<u64> = devices.is_empty().then_some(0);
    for device in model_gpus(devices) {
        match &device.memory {
            MeasurementReading::Measured {
                value: GpuMemory::Discrete { used_bytes, .. },
            } => *allocated.get_or_insert(0) += used_bytes,
            MeasurementReading::Measured {
                value:
                    GpuMemory::Unified {
                        gpu_allocated_bytes: MeasurementReading::Measured { value: bytes },
                        ..
                    },
            } => *allocated.get_or_insert(0) += bytes,
            _ => {}
        }
    }
    allocated
}

/// What the unified-memory residents on one machine take from batch work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentRoom {
    /// The part of the loading models' footprints not yet allocated: memory
    /// they are about to take, which the machine's measured free memory still
    /// counts as free. A healthy model's memory is already in use and already
    /// out of the measured free memory, so it holds back nothing more.
    pub unmet_bytes: u64,
    /// The memory evicting every one of them that may give way for room would
    /// return. A pinned resident never does, so its memory is no relief: work
    /// short of room beside it is an ordinary memory misfit.
    pub held_bytes: u64,
}

/// The room `residents` take on a machine whose GPUs hold `gpu_allocated`.
pub fn resident_room(
    residents: &[MachineModelResident],
    gpu_allocated: Option<u64>,
) -> ResidentRoom {
    let unified: Vec<&MachineModelResident> = residents.iter().filter(|r| r.unified).collect();
    if unified.is_empty() {
        return ResidentRoom::default();
    }
    let (healthy, loading): (Vec<&MachineModelResident>, Vec<&MachineModelResident>) =
        unified.iter().partition(|r| r.phase == "healthy");
    let footprint =
        |of: &[&MachineModelResident]| -> u64 { of.iter().filter_map(|r| r.held_bytes).sum() };
    let healthy_footprint = footprint(&healthy);
    let loading_footprint = footprint(&loading);
    // What the loading models have taken so far: the GPU allocation beyond the
    // healthy models' footprints. Unmeasured, it is read as nothing taken.
    let loading_taken = gpu_allocated
        .unwrap_or(0)
        .saturating_sub(healthy_footprint)
        .min(loading_footprint);
    let unmet_bytes = loading_footprint - loading_taken;
    // What eviction returns, counting only the unpinned: the healthy models'
    // footprints (capped by what the GPUs hold beyond the pinned ones, or that
    // whole reading when an unpinned footprint was never measured and every
    // pinned one was) and what the loading ones have taken beyond the pinned
    // loaders' footprints. Each unknown is read as less relief, never more.
    let (pinned_healthy, free_healthy): (Vec<_>, Vec<_>) =
        healthy.iter().copied().partition(|r| r.pinned);
    let healthy_held = if free_healthy.is_empty() {
        0
    } else {
        let free_footprint = footprint(&free_healthy);
        let free_unmeasured = free_healthy.iter().any(|r| r.held_bytes.is_none());
        let pinned_unmeasured = pinned_healthy.iter().any(|r| r.held_bytes.is_none());
        match gpu_allocated {
            Some(allocated) => {
                let beyond_pins = allocated.saturating_sub(footprint(&pinned_healthy));
                if free_unmeasured && !pinned_unmeasured {
                    beyond_pins
                } else {
                    beyond_pins.min(free_footprint)
                }
            }
            None => free_footprint,
        }
    };
    let pinned_loading: Vec<_> = loading.iter().copied().filter(|r| r.pinned).collect();
    let loading_held = if pinned_loading.len() == loading.len() {
        0
    } else {
        loading_taken.saturating_sub(footprint(&pinned_loading))
    };
    ResidentRoom {
        unmet_bytes,
        held_bytes: healthy_held.saturating_add(loading_held),
    }
}

/// Why a machine with `available_bytes` measured free cannot take work
/// needing `demand_bytes` because of the model residents on it, or `None`
/// when they are not the reason: it fits beside them, or evicting them would
/// not make it fit (an ordinary memory misfit, which evicts nothing).
pub fn resident_misfit(
    available_bytes: u64,
    demand_bytes: u64,
    residents: &[MachineModelResident],
    gpu_allocated: Option<u64>,
) -> Option<PlacementRejectionReason> {
    let room = resident_room(residents, gpu_allocated);
    if room == ResidentRoom::default() {
        return None;
    }
    let effective = available_bytes.saturating_sub(room.unmet_bytes);
    let relieved = available_bytes.saturating_add(room.held_bytes);
    if demand_bytes <= effective || demand_bytes > relieved {
        return None;
    }
    // Name the resident in the way: one still taking room first, then the
    // largest that would give way. With nothing loading, only eviction could
    // make room, so a pinned resident is never the one named.
    let named = residents
        .iter()
        .filter(|r| r.unified && (room.unmet_bytes > 0 || !r.pinned))
        .max_by_key(|r| (r.phase != "healthy", r.held_bytes.unwrap_or(0)))?;
    Some(model_resident_rejection(named, false))
}

/// Why a machine cannot take work whose memory has never been measured
/// because of its model residents, or `None`. Nothing shows such work fits
/// beside the room a loading model has reserved, so it is refused that room
/// (fails closed, tenet 302), naming the loading model. So is a loading model
/// whose footprint here is unknown (its need is a share of memory this machine
/// has not reported). A healthy model holds back nothing, so the work runs
/// beside it as before. The refusal waits for the model and is never grounds
/// to evict it.
pub fn unmeasured_misfit(
    residents: &[MachineModelResident],
    gpu_allocated: Option<u64>,
) -> Option<PlacementRejectionReason> {
    let loading = |r: &&MachineModelResident| r.unified && r.phase != "healthy";
    let unknown = residents
        .iter()
        .filter(loading)
        .any(|r| r.held_bytes.is_none());
    if !unknown && resident_room(residents, gpu_allocated).unmet_bytes == 0 {
        return None;
    }
    let named = residents
        .iter()
        .filter(loading)
        .max_by_key(|r| r.held_bytes.unwrap_or(u64::MAX))?;
    Some(model_resident_rejection(named, true))
}

fn model_resident_rejection(
    named: &MachineModelResident,
    demand_unmeasured: bool,
) -> PlacementRejectionReason {
    PlacementRejectionReason::ModelResident {
        model: named.model.clone(),
        phase: named.phase.clone(),
        held_bytes: named.held_bytes.unwrap_or(0),
        pinned: named.pinned,
        leased: named.leased,
        evictable_at_unix_ms: named.evictable_at_unix_ms,
        demand_unmeasured,
    }
}

/// What one machine has left for a deployment and what the deployment needs
/// there, both in bytes of the same kind of memory (system RAM on a
/// unified-memory machine, VRAM on a discrete card). `None` free memory is
/// unmeasured; a `None` need is a share of memory the machine has not
/// reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Room {
    pub free_bytes: Option<u64>,
    pub need_bytes: Option<u64>,
}

/// One machine short of room: its name, what it has free and what the
/// deployment needs there, either unknown until the machine reports it.
pub type Shortfall = (String, Option<u64>, Option<u64>);

/// Whether a gang fits, from each machine's [`Room`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fit {
    Fits,
    /// These machines lack room, or have not reported enough to tell.
    Short(Vec<Shortfall>),
}

impl Fit {
    pub fn describe(&self) -> String {
        match self {
            Self::Fits => "every machine has the memory it needs".into(),
            Self::Short(short) => short
                .iter()
                .map(|(machine, free, need)| match (free, need) {
                    (Some(free), Some(need)) => {
                        format!("{machine} has {} free and needs {}", gib(*free), gib(*need))
                    }
                    (None, Some(need)) => format!(
                        "{machine}'s free memory is unmeasured; it needs {}",
                        gib(*need)
                    ),
                    (_, None) => format!(
                        "{machine} has not reported its memory, so its share of it is unknown"
                    ),
                })
                .collect::<Vec<_>>()
                .join("; "),
        }
    }
}

/// Whether every machine has the room the deployment needs on it. A machine
/// with no measured free memory, or whose need is a share of memory it has not
/// reported, does not fit: an unmeasured reading is never read as room.
pub fn fit(rooms: &BTreeMap<String, Room>) -> Fit {
    let short: Vec<_> = rooms
        .iter()
        .filter_map(|(machine, room)| match (room.free_bytes, room.need_bytes) {
            (Some(free), Some(need)) if free >= need => None,
            (free, need) => Some((machine.clone(), free, need)),
        })
        .collect();
    if short.is_empty() {
        Fit::Fits
    } else {
        Fit::Short(short)
    }
}

/// One batch running on a gang machine, as the fleet measures it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningBatch {
    pub request_id: String,
    pub attempt_id: String,
    /// Whether placement may stop it to make room: a check lane, which is run
    /// again elsewhere. An agent turn never is (tenet 1323), nor is work that
    /// states no check's priority.
    pub restartable: bool,
    /// The memory its whole process tree holds, as last measured. `None` is
    /// unmeasured, which is never read as a conflict.
    pub resident_bytes: Option<u64>,
    /// Already stopped for this start, so its memory is on its way back.
    pub preempted: bool,
}

/// One gang machine as the drain sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GangMachine {
    pub room: Room,
    /// Whether the memory the deployment needs here is the memory batch work
    /// uses: system RAM, as on a unified-memory machine. False for a discrete
    /// card, whose VRAM no batch touches, so stopping a batch frees none of
    /// what the deployment is short of.
    pub shared_with_batches: bool,
    pub batches: Vec<RunningBatch>,
}

/// A batch to stop so the deployment fits, with the readings behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreemptionChoice {
    pub machine: String,
    pub request_id: String,
    pub attempt_id: String,
    /// What the batch held when it was chosen.
    pub resident_bytes: u64,
    /// What the deployment needs on the machine, and what was free there.
    pub need_bytes: u64,
    pub free_bytes: u64,
}

/// What a reserving start does next about the batch work on its machines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drain {
    /// Send the start: every gang machine has the room.
    Start,
    /// Keep waiting, with what it waits on.
    Wait(String),
    /// Stop these batches, then keep waiting for their memory.
    Preempt {
        batches: Vec<PreemptionChoice>,
        waiting: String,
    },
}

/// Drain a reserving start's machines. Placement already keeps new batches
/// off the room the model has not taken ([`resident_misfit`]), so this decides
/// only about the work already there.
///
/// The start goes on FIT, not on empty, and never waits for its machines to
/// run no batch work: the only conflict is memory. Short of room, restartable
/// work is left to finish for [`DRAIN_BOUND_SECS`]; past that, only checks
/// whose measured memory is in the way are preempted, the fewest that cover
/// each short machine's shortfall, largest first. Agent turns and unmeasured
/// batches are never preempted. All or nothing across the gang: when any short
/// machine cannot be covered that way, nothing is preempted anywhere, because
/// the start could not be sent anyway. `predicted` is how the need was
/// predicted, when nothing measured or stated it, and the wait says so.
pub fn drain(
    gang: &BTreeMap<String, GangMachine>,
    predicted: Option<&str>,
    reserving_since_ms: i64,
    now_ms: i64,
) -> Drain {
    let rooms: BTreeMap<String, Room> = gang
        .iter()
        .map(|(machine, state)| (machine.clone(), state.room))
        .collect();
    let short = match fit(&rooms) {
        Fit::Fits => return Drain::Start,
        Fit::Short(short) => short,
    };
    let memory = match predicted {
        Some(how) => format!(
            "memory: {} (a predicted need: {how})",
            Fit::Short(short.clone()).describe()
        ),
        None => format!("memory: {}", Fit::Short(short.clone()).describe()),
    };
    let elapsed_ms = now_ms.saturating_sub(reserving_since_ms).max(0) as u64;
    let bound_ms = DRAIN_BOUND_SECS * 1_000;
    if elapsed_ms < bound_ms {
        let left_secs = (bound_ms - elapsed_ms).div_ceil(1_000);
        return Drain::Wait(format!(
            "{memory}; the work on its machines may finish for {} more before checks whose measured memory is in the way are preempted",
            duration(left_secs)
        ));
    }

    let mut chosen = Vec::new();
    let mut blocked = Vec::new();
    let mut returning = 0usize;
    for (machine, free, need) in &short {
        let Some(state) = gang.get(machine) else {
            continue;
        };
        let Some(need) = *need else {
            blocked.push(format!("{machine} has not reported its memory"));
            continue;
        };
        let Some(free) = *free else {
            blocked.push(format!("{machine}'s free memory is unmeasured"));
            continue;
        };
        if !state.shared_with_batches {
            blocked.push(format!(
                "{machine}'s batch work uses none of the memory it is short of"
            ));
            continue;
        }
        let shortfall = need.saturating_sub(free);
        let stopped: Vec<&RunningBatch> = state
            .batches
            .iter()
            .filter(|batch| batch.preempted)
            .collect();
        let coming_back: u64 = stopped
            .iter()
            .filter_map(|batch| batch.resident_bytes)
            .sum();
        if coming_back >= shortfall {
            returning += stopped.len();
            continue;
        }
        let remaining = shortfall - coming_back;
        let mut candidates: Vec<(&RunningBatch, u64)> = state
            .batches
            .iter()
            .filter(|batch| batch.restartable && !batch.preempted)
            .filter_map(|batch| Some((batch, batch.resident_bytes.filter(|bytes| *bytes > 0)?)))
            .collect();
        candidates.sort_by(|(a, a_bytes), (b, b_bytes)| {
            b_bytes
                .cmp(a_bytes)
                .then_with(|| a.request_id.cmp(&b.request_id))
        });
        let mut covered = 0u64;
        let mut here = Vec::new();
        for (batch, bytes) in candidates {
            if covered >= remaining {
                break;
            }
            covered += bytes;
            here.push(PreemptionChoice {
                machine: machine.clone(),
                request_id: batch.request_id.clone(),
                attempt_id: batch.attempt_id.clone(),
                resident_bytes: bytes,
                need_bytes: need,
                free_bytes: free,
            });
        }
        if covered < remaining {
            let turns = state
                .batches
                .iter()
                .filter(|batch| !batch.restartable)
                .count();
            let unmeasured = state
                .batches
                .iter()
                .filter(|batch| batch.restartable && batch.resident_bytes.is_none())
                .count();
            blocked.push(format!(
                "{machine} is {} short and the checks measured there hold {}; the rest is held by work that is never preempted ({} and {} unmeasured) or by memory outside batch work",
                gib(remaining),
                gib(covered),
                plural(turns, "agent turn"),
                batches(unmeasured)
            ));
            continue;
        }
        chosen.extend(here);
    }
    if !blocked.is_empty() {
        return Drain::Wait(format!(
            "{memory}; nothing is preempted, because {}",
            blocked.join("; ")
        ));
    }
    if chosen.is_empty() {
        return Drain::Wait(format!(
            "{memory}; waiting for the memory of the {} already preempted",
            batches(returning)
        ));
    }
    let total: u64 = chosen.iter().map(|batch| batch.resident_bytes).sum();
    Drain::Preempt {
        waiting: format!(
            "{memory}; preempted {} measured at {} to make room",
            plural(chosen.len(), "check"),
            gib(total)
        ),
        batches: chosen,
    }
}

fn counted(count: usize, one: &str, many: &str) -> String {
    format!("{count} {}", if count == 1 { one } else { many })
}

fn plural(count: usize, noun: &str) -> String {
    counted(count, noun, &format!("{noun}s"))
}

fn batches(count: usize) -> String {
    counted(count, "batch", "batches")
}

fn duration(secs: u64) -> String {
    if secs >= 60 {
        format!("{} min", secs.div_ceil(60))
    } else {
        format!("{secs} s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_000_000;
    const GIB: u64 = 1 << 30;

    /// bbs: a 4090's VRAM in use, and an integrated Radeon's mapped system
    /// memory beside it. The footprint a model on the card is measured by is
    /// the card's alone; on an APU-only machine the GTT share is the figure.
    #[test]
    fn an_integrated_gpu_beside_a_card_adds_nothing_to_its_vram() {
        use cairn_common::executor_protocol::{
            GpuDevice, GpuVendor, Measurement, UnifiedAllocationSource, UnifiedPool,
        };
        let device = |vendor, memory| GpuDevice {
            index: 0,
            vendor,
            model: String::new(),
            uuid: None,
            cores: None,
            memory: MeasurementReading::Measured { value: memory },
            utilization_millis: MeasurementReading::Measured { value: 0 },
        };
        let card = device(
            GpuVendor::Nvidia,
            GpuMemory::Discrete {
                used_bytes: 18 * GIB,
                total_bytes: 24 * GIB,
            },
        );
        let integrated = device(
            GpuVendor::Amd,
            GpuMemory::Unified {
                gpu_allocated_bytes: MeasurementReading::Measured { value: GIB },
                source: UnifiedAllocationSource::AmdgpuGtt,
                pool: MeasurementReading::Measured {
                    value: UnifiedPool::HostMemory,
                },
                apertures: None,
            },
        );
        let host = |devices| HostHealth {
            gpus: Measurement::measured(T0 as u64, devices),
            ..HostHealth::default()
        };
        assert_eq!(
            gpu_allocated_bytes(&host(vec![card, integrated.clone()])),
            Some(18 * GIB)
        );
        assert_eq!(gpu_allocated_bytes(&host(vec![integrated])), Some(GIB));
    }

    fn resident(phase: &str, held_gib: Option<u64>, unified: bool) -> MachineModelResident {
        MachineModelResident {
            model: "deepseek-ai/DeepSeek-V4-Flash".into(),
            deployment: "dsv4f-tp2".into(),
            phase: phase.into(),
            held_bytes: held_gib.map(|gib| gib * GIB),
            unified,
            pinned: false,
            leased: false,
            evictable_at_unix_ms: Some(0),
        }
    }

    #[test]
    fn a_loading_unified_resident_holds_back_its_unmet_footprint() {
        let loading = [resident("reserving", Some(100), true)];
        assert_eq!(
            resident_room(&loading, Some(0)),
            ResidentRoom {
                unmet_bytes: 100 * GIB,
                held_bytes: 0
            }
        );
        // 110 GiB free, 100 GiB of it about to go to the model: a 20 GiB
        // batch does not fit beside it, though it fits the raw reading.
        let Some(PlacementRejectionReason::ModelResident {
            model,
            phase,
            held_bytes,
            ..
        }) = resident_misfit(110 * GIB, 20 * GIB, &loading, Some(0))
        else {
            panic!("the loading model is the reason");
        };
        assert_eq!(model, "deepseek-ai/DeepSeek-V4-Flash");
        assert_eq!(phase, "reserving");
        assert_eq!(held_bytes, 100 * GIB);
        assert_eq!(
            resident_misfit(110 * GIB, 8 * GIB, &loading, Some(0)),
            None,
            "what the model leaves is still room"
        );
        // Half loaded: half its footprint is already taken from the reading.
        assert_eq!(
            resident_room(&loading, Some(60 * GIB)).unmet_bytes,
            40 * GIB
        );
    }

    #[test]
    fn a_healthy_resident_with_room_admits_a_batch_beside_it() {
        let healthy = [resident("healthy", Some(100), true)];
        assert_eq!(
            resident_misfit(20 * GIB, 16 * GIB, &healthy, Some(100 * GIB)),
            None
        );
        assert_eq!(
            resident_room(&healthy, None).unmet_bytes,
            0,
            "its memory is already out of the measured free memory, read or not"
        );
        assert_eq!(
            resident_misfit(20 * GIB, 16 * GIB, &healthy, None),
            None,
            "an unread GPU never counts a healthy model twice"
        );
        // Short of room, evicting it would return its memory: it is the
        // reason, so placement may evict it.
        assert!(matches!(
            resident_misfit(20 * GIB, 64 * GIB, &healthy, Some(100 * GIB)),
            Some(PlacementRejectionReason::ModelResident { .. })
        ));
    }

    #[test]
    fn a_misfit_evicting_would_not_relieve_is_not_the_resident_s() {
        let healthy = [resident("healthy", Some(100), true)];
        assert_eq!(
            resident_misfit(20 * GIB, 200 * GIB, &healthy, Some(100 * GIB)),
            None,
            "120 GiB would not hold it either: an ordinary memory misfit"
        );
    }

    #[test]
    fn a_pinned_resident_is_no_relief_so_a_batch_short_beside_it_is_an_ordinary_misfit() {
        let pinned = MachineModelResident {
            pinned: true,
            evictable_at_unix_ms: None,
            ..resident("healthy", Some(101), true)
        };
        // sp1 with DeepSeek pinned and healthy and 20 GiB free while its
        // checks run: a 24 GiB check waits for those checks, not the model.
        assert_eq!(
            resident_room(std::slice::from_ref(&pinned), Some(101 * GIB)),
            ResidentRoom::default()
        );
        assert_eq!(
            resident_misfit(
                20 * GIB,
                24 * GIB,
                std::slice::from_ref(&pinned),
                Some(101 * GIB)
            ),
            None
        );
        // Beside an unpinned model, only the unpinned one is relief, and it
        // is the one named.
        let idle = MachineModelResident {
            model: "Qwen/Qwen3-Embedding-8B".into(),
            deployment: "qwen-embed".into(),
            ..resident("healthy", Some(16), true)
        };
        let both = [pinned.clone(), idle];
        assert_eq!(resident_room(&both, Some(117 * GIB)).held_bytes, 16 * GIB);
        let Some(PlacementRejectionReason::ModelResident { model, pinned, .. }) =
            resident_misfit(20 * GIB, 30 * GIB, &both, Some(117 * GIB))
        else {
            panic!("evicting the idle model makes room");
        };
        assert_eq!(model, "Qwen/Qwen3-Embedding-8B");
        assert!(!pinned);
        assert_eq!(
            resident_misfit(20 * GIB, 40 * GIB, &both, Some(117 * GIB)),
            None,
            "36 GiB would not hold it: the pinned model's memory is no relief"
        );
    }

    #[test]
    fn beside_a_healthy_model_only_what_the_loading_one_has_not_taken_is_held() {
        let both = [
            resident("healthy", Some(40), true),
            resident("starting", Some(50), true),
        ];
        // 40 GiB is the healthy model's; 20 more is the loading one's so far.
        assert_eq!(
            resident_room(&both, Some(60 * GIB)),
            ResidentRoom {
                unmet_bytes: 30 * GIB,
                held_bytes: 60 * GIB
            }
        );
    }

    #[test]
    fn unmeasured_work_is_refused_a_loading_model_s_room_and_runs_beside_a_healthy_one() {
        let loading = [resident("reserving", Some(101), true)];
        let Some(PlacementRejectionReason::ModelResident {
            model,
            demand_unmeasured,
            ..
        }) = unmeasured_misfit(&loading, Some(0))
        else {
            panic!("nothing shows the work fits beside the reserved room");
        };
        assert_eq!(model, "deepseek-ai/DeepSeek-V4-Flash");
        assert!(demand_unmeasured, "so the refusal never evicts");
        assert_eq!(
            unmeasured_misfit(&loading, Some(101 * GIB)),
            None,
            "fully loaded, it has nothing left to reserve"
        );
        let healthy = [resident("healthy", Some(101), true)];
        assert_eq!(unmeasured_misfit(&healthy, Some(101 * GIB)), None);
        let vram = [resident("reserving", Some(20), false)];
        assert_eq!(unmeasured_misfit(&vram, Some(0)), None);
    }

    #[test]
    fn a_discrete_vram_resident_never_refuses_a_batch() {
        let vram = [resident("reserving", Some(20), false)];
        assert_eq!(resident_room(&vram, Some(0)), ResidentRoom::default());
        assert_eq!(resident_misfit(8 * GIB, 6 * GIB, &vram, Some(0)), None);
    }

    /// A loading model's footprint is unknown only on a machine that has not
    /// reported the memory its share is of. That holds back no arithmetic
    /// "everything": measured work is weighed on the room the machine reports,
    /// and unmeasured work, which nothing shows fits, is still refused.
    #[test]
    fn a_loading_model_with_no_footprint_holds_back_no_arithmetic_everything() {
        let unknown = [resident("reserving", None, true)];
        assert_eq!(resident_room(&unknown, Some(0)), ResidentRoom::default());
        let Some(PlacementRejectionReason::ModelResident {
            demand_unmeasured, ..
        }) = unmeasured_misfit(&unknown, Some(0))
        else {
            panic!("unmeasured work is refused the loading model's room");
        };
        assert!(demand_unmeasured);
        let healthy = [resident("healthy", None, true)];
        assert_eq!(
            resident_misfit(20 * GIB, 8 * GIB, &healthy, Some(90 * GIB)),
            None
        );
        assert_eq!(unmeasured_misfit(&healthy, Some(90 * GIB)), None);
    }

    fn batch(id: &str, restartable: bool, resident_gib: Option<u64>) -> RunningBatch {
        RunningBatch {
            request_id: id.into(),
            attempt_id: format!("{id}-a"),
            restartable,
            resident_bytes: resident_gib.map(|gib| gib * GIB),
            preempted: false,
        }
    }

    fn machine(free_gib: u64, need_gib: Option<u64>, batches: Vec<RunningBatch>) -> GangMachine {
        GangMachine {
            room: Room {
                free_bytes: Some(free_gib * GIB),
                need_bytes: need_gib.map(|gib| gib * GIB),
            },
            shared_with_batches: true,
            batches,
        }
    }

    fn gang(machines: Vec<(&str, GangMachine)>) -> BTreeMap<String, GangMachine> {
        machines
            .into_iter()
            .map(|(name, state)| (name.to_string(), state))
            .collect()
    }

    const PAST_BOUND: i64 = T0 + (DRAIN_BOUND_SECS as i64) * 1_000;

    #[test]
    fn fit_compares_each_machine_and_never_reads_unmeasured_as_room() {
        let room = |free: Option<u64>, need: Option<u64>| Room {
            free_bytes: free,
            need_bytes: need,
        };
        let mut rooms = BTreeMap::new();
        rooms.insert("sp1".to_string(), room(Some(110 * GIB), Some(100 * GIB)));
        rooms.insert("sp2".to_string(), room(Some(110 * GIB), Some(100 * GIB)));
        assert_eq!(fit(&rooms), Fit::Fits);
        rooms.insert("sp2".to_string(), room(Some(20 * GIB), Some(100 * GIB)));
        assert_eq!(
            fit(&rooms).describe(),
            "sp2 has 20.0 GiB free and needs 100.0 GiB"
        );
        rooms.insert("sp2".to_string(), room(None, Some(100 * GIB)));
        assert!(matches!(fit(&rooms), Fit::Short(_)));
        let unreported: BTreeMap<_, _> = [("sp1".to_string(), room(Some(1), None))].into();
        assert_eq!(
            fit(&unreported).describe(),
            "sp1 has not reported its memory, so its share of it is unknown"
        );
    }

    #[test]
    fn a_gang_that_fits_starts_beside_the_work_already_there() {
        let gang = gang(vec![
            (
                "sp1",
                machine(110, Some(100), vec![batch("check", true, Some(8))]),
            ),
            (
                "sp2",
                machine(105, Some(100), vec![batch("turn", false, Some(4))]),
            ),
        ]);
        assert_eq!(drain(&gang, None, T0, T0), Drain::Start, "fit, not empty");
    }

    /// The operator's rule (cairn/5881): a load drains only the memory it
    /// needs. A profile stating no memory and running vLLM at
    /// `--gpu-memory-utilization 0.85` needs 0.85 of each 128 GiB Spark,
    /// predicted, and past the bound only the checks covering that are
    /// preempted while other work runs on beside it.
    #[test]
    fn a_predicted_need_drains_only_the_checks_covering_it_never_waiting_for_empty() {
        let need = (128 * GIB) as f64 * 0.85;
        let need_gib = (need / GIB as f64).ceil() as u64;
        assert_eq!(need_gib, 109);
        let how =
            "vLLM pre-allocates 0.85 of each machine's memory (`--gpu-memory-utilization 0.85`)";
        let gang = gang(vec![
            (
                "sp1",
                machine(
                    80,
                    Some(need_gib),
                    vec![
                        batch("check-large", true, Some(40)),
                        batch("check-small", true, Some(6)),
                        batch("turn", false, Some(2)),
                    ],
                ),
            ),
            (
                "sp2",
                machine(
                    115,
                    Some(need_gib),
                    vec![batch("check-beside", true, Some(8))],
                ),
            ),
        ]);
        let Drain::Wait(why) = drain(&gang, Some(how), T0, T0 + 1_000) else {
            panic!("inside the bound the work may finish");
        };
        assert!(
            why.starts_with("memory: sp1 has 80.0 GiB free and needs 109.0 GiB (a predicted need: vLLM pre-allocates 0.85"),
            "{why}"
        );
        let Drain::Preempt { batches, waiting } = drain(&gang, Some(how), T0, PAST_BOUND) else {
            panic!("a coverable shortfall preempts, never waiting for empty");
        };
        assert_eq!(
            batches
                .iter()
                .map(|b| b.request_id.as_str())
                .collect::<Vec<_>>(),
            ["check-large"],
            "29 GiB short: the largest check covers it; the small check, the turn and sp2's check run on"
        );
        assert!(waiting.contains("a predicted need"), "{waiting}");

        // Room enough on both: the start goes beside every batch still running.
        let roomy = gang_with_free(&gang, 112);
        assert_eq!(drain(&roomy, Some(how), T0, T0), Drain::Start);
    }

    fn gang_with_free(
        gang: &BTreeMap<String, GangMachine>,
        free_gib: u64,
    ) -> BTreeMap<String, GangMachine> {
        gang.iter()
            .map(|(name, state)| {
                let mut state = state.clone();
                state.room.free_bytes = Some(free_gib * GIB);
                (name.clone(), state)
            })
            .collect()
    }

    /// A share of memory a machine has not reported waits on that reading,
    /// preempting nothing, however many batches run or how long it has waited.
    #[test]
    fn a_share_of_unreported_memory_waits_for_the_reading_and_preempts_nothing() {
        let gang = gang(vec![(
            "sp1",
            machine(10, None, vec![batch("check", true, Some(90))]),
        )]);
        let Drain::Wait(why) = drain(&gang, Some("vLLM"), T0, PAST_BOUND * 10) else {
            panic!("nothing to cover, so nothing is preempted");
        };
        assert!(why.contains("sp1 has not reported its memory"), "{why}");
        assert!(!why.contains("to finish"), "never a wait for empty: {why}");
    }

    #[test]
    fn restartable_work_may_finish_until_the_bound() {
        let gang = gang(vec![(
            "sp1",
            machine(60, Some(100), vec![batch("check", true, Some(50))]),
        )]);
        let Drain::Wait(why) = drain(&gang, None, T0, T0 + 60_000) else {
            panic!("inside the bound nothing is preempted");
        };
        assert!(
            why.starts_with("memory: sp1 has 60.0 GiB free and needs 100.0 GiB"),
            "{why}"
        );
        assert!(why.contains("may finish for 4 min more"), "{why}");
        assert!(matches!(
            drain(&gang, None, T0, PAST_BOUND),
            Drain::Preempt { .. }
        ));
    }

    #[test]
    fn past_the_bound_only_the_fewest_measured_checks_in_the_way_are_preempted() {
        let gang = gang(vec![
            (
                "sp1",
                machine(
                    60,
                    Some(100),
                    vec![
                        batch("small", true, Some(10)),
                        batch("large", true, Some(35)),
                        batch("medium", true, Some(20)),
                        batch("turn", false, Some(60)),
                        batch("unmeasured", true, None),
                    ],
                ),
            ),
            (
                "sp2",
                machine(105, Some(100), vec![batch("fits", true, Some(30))]),
            ),
        ]);
        let Drain::Preempt { batches, waiting } = drain(&gang, None, T0, PAST_BOUND) else {
            panic!("a coverable shortfall preempts");
        };
        let stopped: Vec<&str> = batches.iter().map(|b| b.request_id.as_str()).collect();
        assert_eq!(
            stopped,
            ["large", "medium"],
            "40 GiB short: the two largest measured checks cover it; the turn, the unmeasured batch and the machine that fits are untouched"
        );
        assert_eq!(batches[0].machine, "sp1");
        assert_eq!(batches[0].need_bytes, 100 * GIB);
        assert_eq!(batches[0].free_bytes, 60 * GIB);
        assert_eq!(batches[0].resident_bytes, 35 * GIB);
        assert!(
            waiting.contains("preempted 2 checks measured at 55.0 GiB"),
            "{waiting}"
        );
    }

    #[test]
    fn nothing_is_preempted_when_any_short_machine_cannot_be_covered() {
        let gang = gang(vec![
            (
                "sp1",
                machine(60, Some(100), vec![batch("check", true, Some(50))]),
            ),
            (
                "sp2",
                machine(
                    60,
                    Some(100),
                    vec![
                        batch("turn", false, Some(40)),
                        batch("check2", true, Some(5)),
                    ],
                ),
            ),
        ]);
        let Drain::Wait(why) = drain(&gang, None, T0, PAST_BOUND) else {
            panic!("preempting on sp1 alone would not let the start go");
        };
        assert!(
            why.contains("sp2 is 40.0 GiB short and the checks measured there hold 5.0 GiB; the rest is held by work that is never preempted (1 agent turn and 0 batches unmeasured)"),
            "{why}"
        );
    }

    #[test]
    fn memory_batches_do_not_use_or_cannot_measure_is_no_reason_to_preempt() {
        let mut discrete = machine(4, Some(20), vec![batch("check", true, Some(30))]);
        discrete.shared_with_batches = false;
        let Drain::Wait(why) = drain(&gang(vec![("bbs", discrete)]), None, T0, PAST_BOUND) else {
            panic!("a VRAM shortfall is not a batch's");
        };
        assert!(
            why.contains("uses none of the memory it is short of"),
            "{why}"
        );

        let mut blind = machine(0, Some(20), vec![batch("check", true, Some(30))]);
        blind.room.free_bytes = None;
        let Drain::Wait(why) = drain(&gang(vec![("sp1", blind)]), None, T0, PAST_BOUND) else {
            panic!("an unmeasured free reading preempts nothing");
        };
        assert!(why.contains("sp1's free memory is unmeasured"), "{why}");
    }

    #[test]
    fn memory_already_coming_back_is_waited_for_not_preempted_again() {
        let mut stopped = batch("stopped", true, Some(45));
        stopped.preempted = true;
        let gang = gang(vec![(
            "sp1",
            machine(60, Some(100), vec![stopped, batch("other", true, Some(30))]),
        )]);
        let Drain::Wait(why) = drain(&gang, None, T0, PAST_BOUND) else {
            panic!("the stopped check's memory covers the shortfall");
        };
        assert!(why.contains("the 1 batch already preempted"), "{why}");
    }
}
