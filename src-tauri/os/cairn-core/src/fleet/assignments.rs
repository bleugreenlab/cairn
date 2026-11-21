//! The runner's side of the room-driven dispatcher ([`super::dispatcher`]).
//!
//! The dispatcher decides which waiting entry goes to which machine and holds
//! no lock, reads no clock and does no I/O. Everything around that decision
//! that touches the runner lives here:
//!
//! - working out what an entry needs on each machine it may run on, once, off
//!   every lock, before it joins ([`Fleet::join_dispatcher`]), and again only
//!   when a machine's placement facts move or the profile store learns
//!   something that names the entry's scopes;
//! - the last look at a pair before it is assigned (`admit`): the runner's
//!   own eligibility re-read at that instant, and the room model residents
//!   hold;
//! - carrying out what a decision says: an assignment to the entry's own
//!   task, a headroom release to a machine, an expiry;
//! - the one task that wakes at the earliest deadline, which also works needs
//!   out again when the profile store learns something.
//!
//! Each waiting entry's task waits on its own channel ([`Delivery`]) and on
//! its own queue entry's wake for the machine's answer. Nothing here wakes on
//! a sample, a heartbeat or a snapshot from a machine whose placement facts
//! did not change.
//!
//! Lock order: `connections`, then `placing`, then the queue. Nothing here
//! takes the queue's lock while holding `placing`.

use super::dispatcher::{Assignment, Candidate, Dispatcher, Effects, HardKey, Waiting};
use super::selection::{
    candidate_warmth, entry_process_capacity, executor_bound_rejection, hard_process_refusal,
    model_room_rejection, placement_readings, policy_evidence, process_demand_record, process_fit,
    refused_placement, unresolved_rationale, PlacementFailure, ProcessFit,
};
use super::*;
use cairn_common::executor_protocol::{
    AnnouncedRoom, CompileSlots, HeadroomAuthorization, HostPressureCondition, Need,
    ObservationReuse, PlacementReason, PlacementSelection, PlacementSyncCost, RoomShort,
};
use std::hash::{Hash, Hasher};

/// What a waiting entry's own task is told.
#[derive(Debug)]
pub(super) enum Delivery {
    /// Offer the entry to this machine.
    Assigned(Assignment),
    /// The entry's horizon arrived while it waited: what each machine was
    /// doing with it then, worked out before the dispatcher let it go, for
    /// the refusal's record.
    Expired(Vec<PlacementRejection>),
}

/// One waiting entry's side of the dispatcher: what `admit` and the offer
/// read, and where its assignments go.
struct Pending {
    request: Arc<CellRequest>,
    plan: Option<Arc<ReservationPlan>>,
    interactive: bool,
    execution_residency: Option<ResidencyFence>,
    /// The profile resolved on each candidate machine: what an offer there
    /// charges.
    resolved: HashMap<String, resource_profiles::ResolvedResourceProfile>,
    /// What the needs were worked out from; a profile write that moves it
    /// works them out again.
    stamp: Option<profile_reads::NeedsStamp>,
    reserve_from_unix_ms: u64,
    /// Machines whose size can never hold one process of this work (a
    /// memory-kill exclusion among them), as last figured. They are named in
    /// the why-queued account and in the decision records.
    excluded: Vec<PlacementRejection>,
    deliveries: mpsc::UnboundedSender<Delivery>,
}

/// The dispatcher and the runner state its decisions are carried out with.
#[derive(Default)]
pub(super) struct Placing {
    core: Dispatcher,
    pending: HashMap<String, Pending>,
    /// Each attached machine's placement facts as last figured, so a post
    /// that changes none of them figures nothing.
    facts: HashMap<String, u64>,
    /// Machines `admit` turned a fitting entry away from (a stale link, a
    /// pipe floor, a model resident's room). Nothing the dispatcher holds
    /// changes when those clear, so the machine's next post walks it once.
    admit_refused: HashSet<String>,
    /// When each attached machine's current link was made, for the account
    /// of a machine that has announced no room since.
    attached_at: HashMap<String, u64>,
    /// The rows of entries the deadline task is about to expire, worked out
    /// while they were still waiting, handed over with the expiry.
    expiring: HashMap<String, Vec<PlacementRejection>>,
    /// Test-only: run once inside `figure`, after it has read the connection
    /// table and before it works anything out, so a test can attach a
    /// machine in exactly that window.
    #[cfg(test)]
    after_snapshot: Option<SnapshotHook>,
    /// Test-only: the deadline task panics the next time a deadline fires.
    #[cfg(test)]
    panic_at_deadline: bool,
}

#[cfg(test)]
pub(super) type SnapshotHook = Box<
    dyn FnOnce(Fleet) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send,
>;

/// One machine's row of the why-queued account, with what a decision
/// record needs beside it.
struct MachineRow {
    executor_id: String,
    drain_blockers: Vec<cairn_common::executor_protocol::DrainBlocker>,
    /// A memory or disk shortfall of its room, in the wire vocabulary's
    /// words, which `NoRoomFits` alone cannot carry into a record.
    room_short: Option<PlacementRejectionReason>,
    candidate: CheckPlacementCandidate,
}

impl MachineRow {
    /// This row as a decision's rejection, where the wire vocabulary has a
    /// reason that says it.
    fn recorded(&self, now: u64) -> Option<PlacementRejection> {
        self.candidate
            .status
            .rejection_reason(now)
            .or_else(|| self.room_short.clone())
            .map(|reason| self.rejection(reason))
    }

    fn rejection(&self, reason: PlacementRejectionReason) -> PlacementRejection {
        PlacementRejection {
            unreadable_memory: self.candidate.unreadable_memory.clone(),
            drain_blockers: self.drain_blockers.clone(),
            prediction: None,
            executor_name: self.candidate.executor_name.clone(),
            executor_id: self.executor_id.clone(),
            reason,
        }
    }
}

/// A host-pressure condition in a few words.
fn condition_words(condition: &HostPressureCondition) -> String {
    match condition {
        HostPressureCondition::MemoryAvailable { .. } => "memory available".into(),
        HostPressureCondition::DiskFree { .. } => "disk free".into(),
        HostPressureCondition::ResidentOccupancy { process_count, .. } => {
            format!("{process_count} resident process(es)")
        }
        HostPressureCondition::CpuUtilization { utilization, .. } => {
            format!("cpu at {:.0}%", utilization * 100.0)
        }
        HostPressureCondition::CpuLoweredWait { wait, .. } => wait.describe(),
        HostPressureCondition::MemoryStall { .. } => "memory stall".into(),
        HostPressureCondition::DiskWriteStall { device, .. } => {
            format!("write stall on {device}")
        }
        other => format!("{other:?}"),
    }
}

/// What `room` is short of for `need`, from the shared `fits`' shortfall:
/// the X of "no announced room fits: needs X".
fn describe_room_short(short: &RoomShort, need: &Need, room: &AnnouncedRoom) -> String {
    use cairn_common::executor_protocol::describe_gib;
    match short {
        RoomShort::Capacity => {
            let mut parts = vec![format!(
                "{} unit(s), {} of {} free",
                need.cores,
                room.cores.free(),
                room.cores.limit
            )];
            if let Some(budget) = room.memory_budget.filter(|budget| {
                budget.committed.saturating_add(need.budget_memory_bytes) > budget.limit
            }) {
                parts.push(format!(
                    "{} of memory budget, {} free",
                    describe_gib(need.budget_memory_bytes),
                    describe_gib(budget.free())
                ));
            }
            if let Some(budget) = room
                .disk_budget
                .filter(|budget| budget.committed.saturating_add(need.disk_bytes) > budget.limit)
            {
                parts.push(format!(
                    "{} of disk budget, {} free",
                    describe_gib(need.disk_bytes),
                    describe_gib(budget.free())
                ));
            }
            parts.join("; ")
        }
        RoomShort::Memory(HostPressureCondition::MemoryAvailable {
            available_bytes,
            floor_bytes,
            owed_bytes,
            ..
        }) => format!(
            "{} of memory, {} available less {} owed",
            describe_gib(*floor_bytes),
            describe_gib(*available_bytes),
            describe_gib(*owed_bytes)
        ),
        RoomShort::Disk(HostPressureCondition::DiskFree {
            inodes: Some(inodes),
            ..
        }) => format!("free inodes: {}", inodes.describe()),
        RoomShort::Disk(HostPressureCondition::DiskFree {
            free_bytes,
            floor_bytes,
            ..
        }) => format!(
            "{} of build volume above its floor, {} free, floor {}",
            describe_gib(need.disk_bytes),
            describe_gib(*free_bytes),
            describe_gib(*floor_bytes)
        ),
        RoomShort::Memory(condition) | RoomShort::Disk(condition) => condition_words(condition),
        RoomShort::Pressure(conditions) => format!(
            "its host pressure to clear ({})",
            conditions
                .iter()
                .map(condition_words)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RoomShort::Closed(closure) => format!(
            "it to reopen: {}",
            closure
                .detail
                .clone()
                .unwrap_or_else(|| format!("{:?}", closure.reason))
        ),
        RoomShort::Provisioning(hold) => format!(
            "its provisioning of {} (request {}) to end",
            hold.project_id, hold.request_id
        ),
    }
}

/// A room shortfall as a placement rejection, where one says it: memory
/// available under the need's floor, or a build volume under its floor.
fn room_short_reason(short: &RoomShort, need: &Need) -> Option<PlacementRejectionReason> {
    match short {
        RoomShort::Memory(HostPressureCondition::MemoryAvailable {
            available_bytes,
            floor_bytes,
            owed_bytes,
            ..
        }) => Some(PlacementRejectionReason::InsufficientMemory {
            required_bytes: *floor_bytes,
            available_bytes: available_bytes.saturating_sub(*owed_bytes),
        }),
        RoomShort::Disk(HostPressureCondition::DiskFree {
            free_bytes,
            floor_bytes,
            inodes: None,
        }) => Some(PlacementRejectionReason::InsufficientVolume {
            required_bytes: need.disk_bytes.saturating_add(*floor_bytes),
            free_bytes: *free_bytes,
        }),
        _ => None,
    }
}

/// A headroom hold as a machine's announced room carries it.
fn describe_hold(hold: &cairn_common::executor_protocol::HeadroomRoomHold) -> String {
    use cairn_common::executor_protocol::describe_gib;
    format!(
        "{} unit(s), {} memory, {} disk{}",
        hold.cores,
        describe_gib(hold.memory_bytes),
        describe_gib(hold.disk_bytes),
        if hold.protected { ", protected" } else { "" }
    )
}

/// A drained machine, named by what holds it from new admission.
fn drain_sentence(
    name: &str,
    blockers: &[cairn_common::executor_protocol::DrainBlocker],
) -> String {
    if blockers.is_empty() {
        return format!("{name} drained, refusing new work");
    }
    let held: Vec<String> = blockers
        .iter()
        .map(|blocker| {
            format!(
                "{} generation {}: {}{}",
                blocker.owner,
                blocker.generation,
                blocker.reason,
                blocker
                    .estimate
                    .as_ref()
                    .map(|estimate| format!("; estimate {estimate}"))
                    .unwrap_or_default()
            )
        })
        .collect();
    format!("{name} held from new admission: {}", held.join(", "))
}

/// Every machine's row of `entry_id`'s account: the machines it may run on
/// in executor-id order, then the ones it is kept off. `declines` and
/// `offered_to` are the queue's, read before the placing lock was taken.
/// `None` when the entry is not waiting in the dispatcher.
fn account_rows(
    connections: &HashMap<String, ExecutorConnectionState>,
    unattached: &[EnrolledRemote],
    placing: &Placing,
    entry_id: &str,
    declines: &HashMap<String, queue::EntryDecline>,
    offered_to: Option<&str>,
    now: u64,
) -> Option<Vec<MachineRow>> {
    let pending = placing.pending.get(entry_id)?;
    let core = &placing.core;
    let candidates = core.candidates(entry_id)?;
    let scope = PlacementScope::of(&pending.request);
    let name_of = |executor_id: &str| {
        connections
            .get(executor_id)
            .map_or_else(|| executor_id.to_string(), executor_public_name)
    };
    let superseded = |executor_id: &str, name: &str, standing: bool| {
        let decline = declines.get(executor_id).filter(|_| !standing)?;
        Some(SupersededDecline {
            decline: CheckDispatchDecline::from_entry(name, decline),
            current_seq: core.room(executor_id).map(|(seq, _, _)| seq),
        })
    };
    let mut rows = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut ids: Vec<&String> = candidates.keys().collect();
    ids.sort();
    for executor_id in ids {
        seen.insert(executor_id.clone());
        let name = name_of(executor_id);
        let machine = connections
            .get(executor_id.as_str())
            .filter(|machine| core.generation(executor_id) == Some(machine.generation));
        let room = core.room(executor_id);
        // The refusal that stands: given under the figures the machine still
        // announces. One under older figures, or from a replaced link, is
        // superseded and told as such.
        let standing = core
            .declined_at(entry_id, executor_id)
            .filter(|_| core.refusal_stands(entry_id, executor_id));
        let mut drain_blockers = Vec::new();
        let mut room_short = None;
        // A machine away for now (a candidate only while emptied for an
        // update) is told as away from its own connection, whether or not
        // the dispatcher holds its link yet: it is never offered anything.
        let away = connections.get(executor_id.as_str()).and_then(|machine| {
            candidate_rejection(machine, scope, now)
                .filter(away_for_now)
                .map(|reason| (machine, reason))
        });
        let status = match (machine, away) {
            (_, Some((away, reason))) => {
                drain_blockers.clone_from(&away.drain_blockers);
                CheckCandidateStatus::Away {
                    sentence: drain_sentence(&name, &away.drain_blockers),
                    reason,
                }
            }
            (None, None) => {
                CheckCandidateStatus::NotOffered(PlacementRejectionReason::ConnectionClosed)
            }
            (Some(_), None) if offered_to == Some(executor_id.as_str()) => {
                CheckCandidateStatus::Offered
            }
            (Some(machine), None) => match candidate_rejection(machine, scope, now) {
                Some(PlacementRejectionReason::ConnectionStale { .. }) => {
                    CheckCandidateStatus::LinkStale {
                        since_unix_ms: machine.last_progress_unix_ms,
                    }
                }
                Some(reason) => CheckCandidateStatus::NotOffered(reason),
                None => match room {
                    None => CheckCandidateStatus::RoomNotAnnounced {
                        attached_at_unix_ms: placing.attached_at.get(executor_id).copied(),
                    },
                    Some((seq, announced, left)) => {
                        if let Some(declined) = standing {
                            // The queue's record of the refusal, if the link
                            // the machine is on now gave it.
                            match declines.get(executor_id.as_str()).filter(|decline| {
                                core.generation(executor_id) == Some(decline.generation)
                            }) {
                                Some(decline) => CheckCandidateStatus::Declined(
                                    CheckDispatchDecline::from_entry(&name, decline),
                                ),
                                None => {
                                    CheckCandidateStatus::OfferUndelivered { room_seq: declined }
                                }
                            }
                        } else if core.reserved_on(entry_id) == Some(executor_id.as_str()) {
                            CheckCandidateStatus::HoldingRoom {
                                room_seq: seq,
                                hold: announced
                                    .holds
                                    .iter()
                                    .find(|hold| hold.entry_id == entry_id)
                                    .map(describe_hold),
                            }
                        } else {
                            match core.shortfall(entry_id, executor_id, now) {
                                Some(Err(_))
                                    if core.kept_after_second_refusal(
                                        entry_id,
                                        executor_id,
                                        now,
                                    ) =>
                                {
                                    CheckCandidateStatus::RefusedTwice { room_seq: seq }
                                }
                                Some(Err(short)) => {
                                    let need = &candidates[executor_id].need;
                                    room_short = room_short_reason(&short, need);
                                    let capacity = announced.capacity();
                                    let needs = match cairn_common::executor_protocol::fits(&capacity, need) {
                                        Err(capacity_short) => format!(
                                            "{}; this work can never fit here while standing residents remain",
                                            describe_room_short(&capacity_short, need, &capacity)
                                        ),
                                        Ok(()) => describe_room_short(&short, need, left),
                                    };
                                    CheckCandidateStatus::NoRoomFits {
                                        needs,
                                        room_seq: seq,
                                        since_unix_ms: core.room_since(executor_id).unwrap_or(0),
                                    }
                                }
                                _ => {
                                    let held_off = core.held_off_by(entry_id, executor_id, now);
                                    let demand = pending.resolved.get(executor_id).map_or(
                                        &pending.request.resource_reservation,
                                        |resolved| &resolved.reservation,
                                    );
                                    let pinned_here = pending.request.pinned_executor_id.as_deref()
                                        == Some(executor_id.as_str());
                                    match (
                                        held_off,
                                        model_room_rejection(machine, Some(demand), pinned_here),
                                    ) {
                                        (Some(better), _) => CheckCandidateStatus::WaitsFor {
                                            executor_name: name_of(better),
                                            room_seq: seq,
                                        },
                                        (None, Some(reason)) => {
                                            CheckCandidateStatus::NotOffered(reason)
                                        }
                                        (None, None) => {
                                            CheckCandidateStatus::Fits { room_seq: seq }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
        };
        rows.push(MachineRow {
            executor_id: executor_id.clone(),
            drain_blockers,
            room_short,
            candidate: CheckPlacementCandidate {
                unreadable_memory: pending
                    .resolved
                    .get(executor_id)
                    .map(|resolved| resolved.rationale.unreadable_memory.clone())
                    .unwrap_or_default(),
                superseded: superseded(executor_id, &name, standing.is_some()),
                executor_name: name,
                status,
            },
        });
    }
    let surveyed = match survey_candidates(connections, unattached, &pending.request, now) {
        Ok(survey) => survey.rejected,
        Err(failure) => failure.rejected,
    };
    for rejection in pending.excluded.iter().chain(surveyed.iter()) {
        // A pinned request waits on its machine alone; every other machine
        // would say only that it is not that one.
        if matches!(
            rejection.reason,
            PlacementRejectionReason::PinMismatch { .. }
        ) || !seen.insert(rejection.executor_id.clone())
        {
            continue;
        }
        let machine = connections.get(&rejection.executor_id);
        let mut drain_blockers = rejection.drain_blockers.clone();
        if drain_blockers.is_empty() {
            if let Some(machine) = machine {
                drain_blockers.clone_from(&machine.drain_blockers);
            }
        }
        let status = match &rejection.reason {
            reason @ (PlacementRejectionReason::Drained
            | PlacementRejectionReason::UpdatePending { .. }) => CheckCandidateStatus::Away {
                sentence: drain_sentence(&rejection.executor_name, &drain_blockers),
                reason: reason.clone(),
            },
            PlacementRejectionReason::ConnectionStale { silence_ms, .. } => {
                CheckCandidateStatus::LinkStale {
                    since_unix_ms: machine.map_or(now.saturating_sub(*silence_ms), |machine| {
                        machine.last_progress_unix_ms
                    }),
                }
            }
            reason => CheckCandidateStatus::NotOffered(reason.clone()),
        };
        rows.push(MachineRow {
            executor_id: rejection.executor_id.clone(),
            drain_blockers,
            room_short: None,
            candidate: CheckPlacementCandidate {
                unreadable_memory: rejection.unreadable_memory.clone(),
                superseded: superseded(&rejection.executor_id, &rejection.executor_name, false),
                executor_name: rejection.executor_name.clone(),
                status,
            },
        });
    }
    Some(rows)
}

/// What one entry needs on the machines it may run on.
#[derive(Default)]
struct Figured {
    candidates: HashMap<String, Candidate>,
    resolved: HashMap<String, resource_profiles::ResolvedResourceProfile>,
    /// Machines whose size can never hold one process of this work.
    excluded: Vec<PlacementRejection>,
    /// Machines away for now (drained, or installing an update) whose size
    /// would hold the work. Not candidates: the drain is one of the
    /// machine's placement facts, so its lifting works the entry out again
    /// there. They make the work worth waiting for rather than refusing.
    away: Vec<String>,
    /// Every machine the connection snapshot held (within `only`), with the
    /// epoch it was figured at, eligible or not. What is figured for a
    /// machine is committed only while it still stands at that epoch.
    seen: HashMap<String, Epoch>,
}

/// Where a machine stood when an entry was figured on it: its link's
/// generation and its placement facts as last recorded. Either moving
/// re-figures every waiting entry there, so figures from an older epoch are
/// never committed over a newer one's.
type Epoch = (u64, Option<u64>, Option<CompileSlots>);

fn epoch(
    connections: &HashMap<String, ExecutorConnectionState>,
    facts: &HashMap<String, u64>,
    executor_id: &str,
) -> Option<Epoch> {
    connections.get(executor_id).map(|machine| {
        (
            machine.generation,
            facts.get(executor_id).copied(),
            machine.room.as_ref().map(|held| held.room.slots),
        )
    })
}

/// A rejection that clears with time alone. The machine stays a candidate,
/// and `admit` reads the link again at the moment of assignment.
fn clears_with_time(reason: &PlacementRejectionReason) -> bool {
    matches!(
        reason,
        PlacementRejectionReason::ConnectionStale { .. }
            | PlacementRejectionReason::RunnerNotListening { .. }
            | PlacementRejectionReason::PipeDegraded { .. }
    )
}

/// A rejection only the machine's drain makes: an operator or the update
/// rollout lifts it, and the machine is then what it was before.
fn away_for_now(reason: &PlacementRejectionReason) -> bool {
    matches!(
        reason,
        PlacementRejectionReason::Drained | PlacementRejectionReason::UpdatePending { .. }
    )
}

/// Where `machine` stands on the dispatcher's memory-safety order for work
/// resolved there as `resolved`: deferred on a class prior it cannot give
/// one process, its single-process capacity for a Rust compile nothing has
/// measured, and clamped below a measured demand. Work resolved against no
/// profile is equal everywhere.
fn hard_key(
    machine: &ExecutorConnectionState,
    resolved: Option<&resource_profiles::ResolvedResourceProfile>,
) -> HardKey {
    let Some(resolved) = resolved else {
        return HardKey::default();
    };
    let evidence = &resolved.process_evidence;
    HardKey {
        deferred: matches!(
            process_fit(evidence, machine),
            Some(ProcessFit::Deferred(_))
        ),
        unmeasured_capacity: evidence
            .unmeasured_compile
            .then(|| entry_process_capacity(machine)),
        clamped: resolved.memory_fit.ceiling().is_some(),
    }
}

/// Whether the dispatcher should prefer this machine among those whose room
/// fits: never whether it may take the work. Work that carries affinities
/// (a thread home's runner platform and previous machine, where its node
/// scratch is already current) prefers a machine they price at nothing;
/// other work prefers a machine whose warm roots hold its base commit.
fn preferred_here(request: &CellRequest, machine: &ExecutorConnectionState) -> bool {
    let executor_id = machine.identity.executor_id.as_str();
    let affinity_ms = request.home_affinity.as_ref().map_or(0, |affinity| {
        affinity.price_ms(executor_id, &machine.advertisement.capabilities.os)
    });
    let scratch_ms = request
        .scratch_sync
        .as_ref()
        .map_or(0, |estimate| estimate.ms_for(executor_id));
    if request.home_affinity.is_some() || request.scratch_sync.is_some() {
        return affinity_ms.saturating_add(scratch_ms) == 0;
    }
    machine
        .warm_roots
        .holds(&request.repository.identity(), &request.base_commit)
}

/// The repository an offer to this machine names: the request's own on the
/// colocated machine, managed objects elsewhere, a scratch-only acquisition
/// as itself.
fn offered_repository(request: &CellRequest, colocated: bool) -> RepositoryLocator {
    if colocated || matches!(request.repository, RepositoryLocator::ScratchOnly { .. }) {
        return request.repository.clone();
    }
    let identity = request.repository.identity();
    RepositoryLocator::ManagedObjects {
        project_id: identity.project_id,
        repository_id: identity.repository_id,
        object_format: identity.object_format,
    }
}

/// Everything about a machine that decides which entries may run there and
/// what they need there, and nothing that moves on every post: what it
/// advertises it can do, what it is held to, its size, the models resident
/// there, the projects it cannot prepare, and whether its build volume is
/// mounted. Freshness is not here; `admit` reads it.
fn placement_facts(entry: &ExecutorConnectionState) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!(
        "{:?}|{:?}|{}|{:?}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{}",
        entry.advertisement.capabilities,
        entry.machine_role,
        entry.drained,
        entry.drain_blockers,
        entry.update_hold,
        entry.model_residents,
        {
            let mut refused: Vec<&String> = entry.preparation_refusals.keys().collect();
            refused.sort();
            refused
        },
        entry
            .health
            .build_volume
            .as_ref()
            .map(|volume| volume.is_absent()),
        entry
            .health
            .machine
            .memory
            .value()
            .map(|memory| memory.total_bytes),
        entry.health.memory_record.standing_bytes,
        entry.sender.is_closed(),
    )
    .hash(&mut hasher);
    hasher.finish()
}

/// The last look at one pair before it is assigned: the runner's own
/// eligibility read again now, and the room model residents hold. A machine
/// refused only for a resident's room, with measured demand, is noted for
/// the model pass, which may evict an idle resident there.
fn admit_pair(
    connections: &HashMap<String, ExecutorConnectionState>,
    pending: &HashMap<String, Pending>,
    entry_id: &str,
    executor_id: &str,
    now_unix_ms: u64,
    room_refusals: &mut Vec<String>,
    refused_on: &mut Vec<String>,
) -> bool {
    let (Some(pending), Some(machine)) = (pending.get(entry_id), connections.get(executor_id))
    else {
        return false;
    };
    if candidate_rejection(machine, PlacementScope::of(&pending.request), now_unix_ms).is_some() {
        refused_on.push(executor_id.to_string());
        return false;
    }
    let pinned_here = pending.request.pinned_executor_id.as_deref() == Some(executor_id);
    let demand = pending
        .resolved
        .get(executor_id)
        .map_or(&pending.request.resource_reservation, |resolved| {
            &resolved.reservation
        });
    match model_room_rejection(machine, Some(demand), pinned_here) {
        None => true,
        Some(reason) => {
            if matches!(
                reason,
                PlacementRejectionReason::ModelResident {
                    demand_unmeasured: false,
                    ..
                }
            ) {
                room_refusals.push(executor_public_name(machine));
            }
            refused_on.push(executor_id.to_string());
            false
        }
    }
}

/// Carry out a decision: each assignment to its entry's task, each expiry,
/// each released hold to its machine. The entries whose task has gone, for
/// the caller to withdraw from the dispatcher.
fn carry_out(
    connections: &HashMap<String, ExecutorConnectionState>,
    placing: &mut Placing,
    effects: Effects,
) -> Vec<String> {
    let mut gone = Vec::new();
    for assignment in effects.assigned {
        let entry_id = assignment.entry_id.clone();
        let delivered = placing.pending.get(&entry_id).is_some_and(|pending| {
            pending
                .deliveries
                .send(Delivery::Assigned(assignment))
                .is_ok()
        });
        if !delivered {
            placing.pending.remove(&entry_id);
            gone.push(entry_id);
        }
    }
    for entry_id in effects.expired {
        let rejected = placing.expiring.remove(&entry_id).unwrap_or_default();
        if let Some(pending) = placing.pending.remove(&entry_id) {
            let _ = pending.deliveries.send(Delivery::Expired(rejected));
        }
    }
    for (entry_id, executor_id, generation) in effects.released {
        release_headroom(connections, &entry_id, &executor_id, Some(generation));
    }
    gone
}

/// Run one dispatcher mutator with `admit` reading `connections`.
fn decide(
    connections: &HashMap<String, ExecutorConnectionState>,
    placing: &mut Placing,
    now: u64,
    room_refusals: &mut Vec<String>,
    refused_on: &mut Vec<String>,
    act: impl FnOnce(&mut Dispatcher, Admit<'_>, u64) -> Effects,
) -> Effects {
    let Placing { core, pending, .. } = placing;
    let mut admit = |entry_id: &str, executor_id: &str| {
        admit_pair(
            connections,
            pending,
            entry_id,
            executor_id,
            now,
            room_refusals,
            refused_on,
        )
    };
    act(core, &mut admit, now)
}

fn release_headroom(
    connections: &HashMap<String, ExecutorConnectionState>,
    entry_id: &str,
    executor_id: &str,
    generation: Option<u64>,
) {
    if let Some(machine) = connections
        .get(executor_id)
        .filter(|machine| generation.is_none_or(|generation| machine.generation == generation))
    {
        let _ = machine.sender.send(ExecutorMessage::ReleaseHeadroom {
            entry_id: entry_id.to_string(),
        });
    }
}

/// Whether `request` is a lane that needs a nested Seatbelt write, whose
/// eligible Macs depend on one another ([`nested_sandbox_preference`]).
fn needs_nesting(request: &CellRequest) -> bool {
    request
        .executor
        .as_ref()
        .is_some_and(|selector| selector.requires_macos_nested_sandbox_write)
}

/// The admit closure every mutator is handed.
pub(super) type Admit<'a> = &'a mut dyn FnMut(&str, &str) -> bool;

/// Leaves the dispatcher when the task waiting for a machine ends, on every
/// path out of it.
pub(super) struct JoinedDispatcher {
    pub(super) pool: Fleet,
    pub(super) entry_id: String,
}

impl Drop for JoinedDispatcher {
    fn drop(&mut self) {
        self.pool.leave_dispatcher(&self.entry_id);
    }
}

impl Fleet {
    fn lock_placing(&self) -> std::sync::MutexGuard<'_, Placing> {
        self.placing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Run one dispatcher mutator under the placement lock, with `admit`
    /// reading `connections`, and carry out what it decided. The caller holds
    /// the connection lock, which comes first.
    fn dispatch_locked(
        &self,
        connections: &HashMap<String, ExecutorConnectionState>,
        placing: &mut Placing,
        act: impl FnOnce(&mut Dispatcher, Admit<'_>, u64) -> Effects,
    ) {
        let now = unix_time_ms();
        let mut room_refusals = Vec::new();
        let mut refused_on = Vec::new();
        let mut effects = decide(
            connections,
            placing,
            now,
            &mut room_refusals,
            &mut refused_on,
            act,
        );
        loop {
            // An entry assigned after its task had gone was never offered:
            // it leaves, giving back what its assignment took. It has no
            // task to be assigned to again, so this ends.
            let gone = carry_out(connections, placing, effects);
            if gone.is_empty() {
                break;
            }
            effects = decide(
                connections,
                placing,
                now,
                &mut room_refusals,
                &mut refused_on,
                |core, mut admit, now| {
                    let mut effects = Effects::default();
                    for entry_id in &gone {
                        effects.absorb(core.withdraw(entry_id, now, &mut admit));
                    }
                    effects
                },
            );
        }
        placing.admit_refused.extend(refused_on);
        let work = placing.core.take_work();
        self.counters
            .placement_work(work.passes, work.fit_evaluations);
        #[cfg(test)]
        {
            let stranded = placing.core.stranded(now);
            assert!(
                stranded.is_empty(),
                "waiting entries fit an announced room they were never offered: {stranded:?}"
            );
        }
        if !room_refusals.is_empty() {
            self.note_room_refusal(room_refusals);
        }
    }

    /// [`Self::dispatch_locked`], taking both locks in order.
    fn dispatch(&self, act: impl FnOnce(&mut Dispatcher, Admit<'_>, u64) -> Effects) {
        let connections = self.lock_connections();
        let mut placing = self.lock_placing();
        self.dispatch_locked(&connections, &mut placing, act);
    }

    /// Start this fleet's placement tasks, once, on the runtime the fleet was
    /// built on when it has one: the deadline task, and the one that works
    /// needs out again when the profile store learns something. Each is
    /// supervised, so a panic in one is logged and the task started again.
    fn ensure_placing_task(&self) {
        if self
            .placing_task
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        self.supervise("placement deadline", Self::run_placing_deadlines);
        self.supervise("learned needs re-figure", Self::run_learned_refigures);
    }

    /// Run `task` until it returns, starting it again after each panic. A
    /// panic is logged at ERROR with what it said; the restart waits a
    /// second, so a task that panics at once every time is loud, not hot.
    fn supervise<Task, Run>(&self, name: &'static str, task: Task)
    where
        Task: Fn(Fleet) -> Run + Send + 'static,
        Run: std::future::Future<Output = ()> + Send + 'static,
    {
        let fleet = self.clone();
        let supervisor = async move {
            loop {
                match tokio::spawn(task(fleet.clone())).await {
                    Ok(()) => return,
                    Err(error) if error.is_panic() => {
                        let panic = error.into_panic();
                        let said = panic
                            .downcast_ref::<&str>()
                            .map(|said| (*said).to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "no message".into());
                        log::error!("the {name} task panicked ({said}); starting it again");
                    }
                    // Cancelled: the runtime is shutting down.
                    Err(_) => return,
                }
                tokio::select! {
                    _ = fleet.runner_stop.stopped() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        };
        match &self.host_runtime {
            Some(runtime) => {
                runtime.spawn(supervisor);
            }
            None => {
                tokio::spawn(supervisor);
            }
        }
    }

    /// Work needs out again when the profile store learns something, once
    /// per burst of writes. A start, first or after a panic, looks once at
    /// once, so writes made while no task watched are not missed.
    async fn run_learned_refigures(self) {
        let mut learned = profile_reads::needs_moved();
        learned.borrow_and_update();
        self.refigure_learned().await;
        loop {
            tokio::select! {
                biased;
                _ = self.runner_stop.stopped() => return,
                changed = learned.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    self.refigure_learned().await;
                }
            }
        }
    }

    /// The one task that sleeps to the earliest deadline: an entry becoming
    /// entitled to hold room, or its horizon.
    async fn run_placing_deadlines(self) {
        loop {
            let next = self.lock_placing().core.next_deadline();
            let deadline = async {
                match next {
                    Some(at) => {
                        tokio::time::sleep(Duration::from_millis(at.saturating_sub(unix_time_ms())))
                            .await
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                _ = self.runner_stop.stopped() => return,
                () = self.placing_deadline.notified() => {}
                () = deadline => {
                    #[cfg(test)]
                    if std::mem::take(&mut self.lock_placing().panic_at_deadline) {
                        panic!("injected into the placement deadline task");
                    }
                    // What each expiring entry stood at, read before the
                    // dispatcher lets it go: once per expiry.
                    let expiring = self.lock_placing().core.expiring(unix_time_ms());
                    for entry_id in expiring {
                        let rejected = self.waiting_rejections(&entry_id);
                        self.lock_placing().expiring.insert(entry_id, rejected);
                    }
                    self.dispatch(|core, mut admit, now| core.deadline_reached(now, &mut admit));
                    // An entry that did not expire after all is read afresh
                    // when it does.
                    self.lock_placing().expiring.clear();
                }
            }
        }
    }

    /// What this entry needs on each machine it may run on, worked out off
    /// every lock. `only` limits it to one machine.
    async fn figure(
        &self,
        request: &CellRequest,
        plan: Option<&ReservationPlan>,
        interactive: bool,
        execution_residency: Option<&ResidencyFence>,
        only: Option<&str>,
    ) -> Figured {
        let now = unix_time_ms();
        // Each machine that may run the work, whether it is away for now,
        // and whether that absence ends on its own (an update, not a drain).
        type Snapshot = Vec<(ExecutorConnectionState, bool, bool)>;
        let (machines, seen): (Snapshot, HashMap<String, Epoch>) = {
            let connections = self.lock_connections();
            let seen: HashMap<String, Epoch> = {
                let placing = self.lock_placing();
                connections
                    .keys()
                    .filter(|id| only.is_none_or(|only| only == id.as_str()))
                    .filter_map(|id| {
                        epoch(&connections, &placing.facts, id).map(|at| (id.clone(), at))
                    })
                    .collect()
            };
            let scope = PlacementScope::of(request);
            // A Mac refused nesting by the credential floor alone takes a lane
            // that needs it only while no Mac that can nest could.
            let ordered: Vec<&ExecutorConnectionState> = connections.values().collect();
            let nests_elsewhere = !nested_sandbox_preference(&ordered, scope, now).is_empty();
            let machines: Snapshot = connections
                .values()
                .filter(|entry| only.is_none_or(|id| id == entry.identity.executor_id))
                .filter(|entry| {
                    !nests_elsewhere
                        || scope.selector.is_none_or(|selector| {
                            nested_sandbox_fit(&entry.advertisement.capabilities, selector)
                                != NestedSandboxFit::Waivable
                        })
                })
                .filter_map(|entry| match candidate_rejection(entry, scope, now) {
                    None => Some((entry.clone(), false, false)),
                    Some(reason) if clears_with_time(&reason) => {
                        Some((entry.clone(), false, false))
                    }
                    Some(reason) if away_for_now(&reason) => {
                        Some((entry.clone(), true, waits_on_machine(&reason)))
                    }
                    Some(_) => None,
                })
                .collect();
            (machines, seen)
        };
        #[cfg(test)]
        {
            let hook = self.lock_placing().after_snapshot.take();
            if let Some(hook) = hook {
                hook(self.clone()).await;
            }
        }
        let mut figured = Figured {
            seen,
            ..Figured::default()
        };
        for (machine, away, returns) in &machines {
            let executor_id = machine.identity.executor_id.clone();
            let resolved = match plan {
                Some(plan) => {
                    let duration_context = resource_profiles::DurationContext {
                        class: request.command_class,
                        warmth: candidate_warmth(machine, request, now).for_lookup(),
                        now_unix_ms: now,
                    };
                    let mut resolved = off_frame(|| {
                        plan.resolve_for(
                            request,
                            &machine.identity.device_id,
                            &executor_id,
                            &machine.advertisement.capabilities,
                            machine
                                .room
                                .as_ref()
                                .map_or(0, |held| held.room.standing.cores),
                            duration_context,
                        )
                    })
                    .await;
                    self.record_memory_lookup_failures(&mut resolved, &executor_id);
                    Some(resolved)
                }
                None => None,
            };
            let reservation = resolved
                .as_ref()
                .map_or(&request.resource_reservation, |resolved| {
                    &resolved.reservation
                });
            if let Some(reason) = hard_process_refusal(machine, resolved.as_ref())
                .or_else(|| executor_bound_rejection(machine, reservation))
            {
                figured.excluded.push(PlacementRejection {
                    unreadable_memory: Vec::new(),
                    drain_blockers: Vec::new(),
                    prediction: None,
                    executor_name: executor_public_name(machine),
                    executor_id,
                    reason,
                });
                continue;
            }
            // A machine emptied for an update is a candidate never offered
            // the work, so it can keep deferred or clamped work off a machine
            // behind it until it returns. An operator's drain is no wait.
            if *away && !*returns {
                figured.away.push(executor_id);
                continue;
            }
            // Whether the executor will judge this work as provisioning there,
            // by its own predicate, against the cells the machine posted.
            let resident_here = execution_residency.is_some_and(|fence| {
                fence.is_active_among(&request.project_id, &machine.snapshot.cells)
            });
            let provisions = cairn_common::executor_protocol::admission_provisions(
                &offered_repository(request, machine.colocated),
                resident_here,
            );
            let need = Need::of(
                reservation,
                request.command_class,
                interactive,
                pipe_floor_work(request),
                cairn_common::executor_protocol::needs_build_volume(&offered_repository(
                    request,
                    machine.colocated,
                )),
                provisions.then_some(request.project_id.as_str()),
                CompileSlots::net(
                    machine
                        .advertisement
                        .capabilities
                        .admission_concurrency_budget(),
                    machine
                        .room
                        .as_ref()
                        .map_or(0, |held| held.room.standing.cores),
                ),
            );
            let warm = preferred_here(request, machine);
            let key = hard_key(machine, resolved.as_ref());
            figured.candidates.insert(
                executor_id.clone(),
                Candidate {
                    need,
                    warm,
                    key,
                    away: *away,
                },
            );
            if let Some(resolved) = resolved {
                figured.resolved.insert(executor_id, resolved);
            }
        }
        if let Some(plan) = plan {
            self.counters.needs_worked_out(plan.take_needs_worked_out());
        }
        figured
    }

    /// The refusal owed at once to work that can never be placed, instead of
    /// a wait out to its horizon: targeted work no usable machine satisfies
    /// and nothing to wait for, work tied to a home that can never take it,
    /// untargeted work with nothing attached that could take it, and work
    /// every machine is too small for. A machine away for now that would
    /// hold the work is worth waiting for, so it holds off the last two.
    fn refusal_at_join(
        &self,
        request: &CellRequest,
        policy: &ActivePlacementPolicy,
        figured: &Figured,
        now: u64,
    ) -> Option<(CellOutcome, Option<PlacementDecision>)> {
        // Taken before the connection lock: it takes that lock itself.
        let unattached = self.unattached_enrolled_remotes();
        // The survey alone reads the table; everything after it takes locks
        // of its own, the connection lock among them.
        let surveyed = {
            let connections = self.lock_connections();
            survey_candidates(&connections, &unattached, request, now).map(|survey| {
                survey
                    .rejected
                    .iter()
                    .any(|rejection| waits_on_machine(&rejection.reason))
            })
        };
        let something_to_wait_for = match surveyed {
            Ok(waits) => waits,
            Err(failure) => {
                let refused = refused_placement(request, now, policy, None, failure);
                return Some((
                    CellOutcome::Unavailable {
                        reason: refused.reason,
                        diagnostic: refused.diagnostic,
                    },
                    Some(refused.decision),
                ));
            }
        };
        if !figured.candidates.is_empty() || !figured.away.is_empty() {
            return None;
        }
        if something_to_wait_for {
            return None;
        }
        if !figured.excluded.is_empty() {
            let rejected = figured.excluded.clone();
            let diagnostic = format!(
                "no attached machine can hold one process of this work: {}",
                rejected
                    .iter()
                    .map(|rejection| format!(
                        "{}: {}",
                        rejection.executor_name,
                        rejection.reason.describe()
                    ))
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            let refused = refused_placement(
                request,
                now,
                policy,
                None,
                PlacementFailure {
                    diagnostic,
                    rejected,
                },
            );
            return Some((
                CellOutcome::Unavailable {
                    reason: refused.reason,
                    diagnostic: refused.diagnostic,
                },
                Some(refused.decision),
            ));
        }
        if PlacementScope::of(request).targeted() {
            return None;
        }
        let colocated = self.colocated_substrate();
        let generation_held = colocated
            .as_ref()
            .filter(|evidence| evidence.state == ExecutorSubstrateState::GenerationHeld)
            .and_then(|evidence| evidence.diagnostic.clone());
        let transient = colocated
            .filter(|evidence| {
                evidence.state != ExecutorSubstrateState::GenerationHeld
                    && now.saturating_sub(evidence.last_progress_unix_ms)
                        <= EXECUTOR_PROGRESS_FRESHNESS_MS
            })
            .filter(|_| !self.colocated_pipe_floor_refuses(pipe_floor_work(request), now));
        if transient.is_some() {
            return None;
        }
        Some((
            executor_unavailable(generation_held.unwrap_or_else(|| NO_COLOCATED_EXECUTOR.into())),
            None,
        ))
    }

    /// Enter an entry the queue holds into the dispatcher, matched once
    /// against the rooms already announced, and return the channel its
    /// assignments arrive on. Refuses at once what can never be placed.
    // The refusal is the outcome the requester is owed, carried as every
    // placement path carries it; it is returned once per join, never stored.
    #[allow(clippy::result_large_err)]
    pub(super) async fn join_dispatcher(
        &self,
        entry_id: &str,
        request: &CellRequest,
        plan: Option<Arc<ReservationPlan>>,
        policy: &ActivePlacementPolicy,
    ) -> Result<mpsc::UnboundedReceiver<Delivery>, CellOutcome> {
        self.ensure_placing_task();
        let now = unix_time_ms();
        let cancelled = || CellOutcome::Cancelled {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
        };
        let (interactive, execution_residency, standing, queued_at, sequence) = self
            .queue
            .with_entry(entry_id, |entry| {
                (
                    entry.interactive,
                    entry.execution_residency.clone(),
                    entry.standing(),
                    entry.queued_at_unix_ms(),
                    entry.sequence(),
                )
            })
            .ok_or_else(cancelled)?;
        let horizon = request.wait_horizon_unix_ms;
        // Standing work is entitled to hold room at once (tenet 1444), and a
        // machine honours an authorization only from its stated instant.
        let reserve_from = if interactive {
            horizon
        } else if standing {
            now
        } else {
            self.queue
                .headroom_plan(entry_id, now)
                .map_or(horizon, |plan| plan.reserve_from_unix_ms)
        };
        loop {
            let figured = self
                .figure(
                    request,
                    plan.as_deref(),
                    interactive,
                    execution_residency.as_ref(),
                    None,
                )
                .await;
            let stamp = plan.as_ref().map(|plan| plan.needs_stamp());
            let now = unix_time_ms();
            let refusal = self.refusal_at_join(request, policy, &figured, now);
            let connections = self.lock_connections();
            let mut placing = self.lock_placing();
            // Refusals, resolved reservations and candidate needs all describe
            // this snapshot. Commit none of them after a resident or link moved.
            let fresh = connections.len() == figured.seen.len()
                && connections.keys().all(|id| {
                    epoch(&connections, &placing.facts, id) == figured.seen.get(id).copied()
                });
            if !fresh {
                drop(placing);
                drop(connections);
                if !matches!(
                    self.queue.state_of(entry_id),
                    Some(queue::EntryState::Queued)
                ) {
                    return Err(cancelled());
                }
                if unix_time_ms() >= horizon {
                    return Err(self.refuse_at_horizon(
                        request,
                        policy,
                        Some(entry_id),
                        Vec::new(),
                    ));
                }
                continue;
            }
            if let Some((outcome, decision)) = refusal {
                drop(placing);
                drop(connections);
                if let Some(mut decision) = decision {
                    self.stamp_update_bounds(&mut decision.rejected);
                    self.record_placement_decision(decision);
                }
                return Err(outcome);
            }
            if !PlacementScope::of(request).targeted() && !figured.candidates.is_empty() {
                // Every candidate has announced its room, and none could hold
                // this work even with nothing running and every transient
                // condition (a brake, a drain, a provisioning hold) lifted.
                let never = figured.candidates.iter().all(|(executor_id, candidate)| {
                    placing.core.room(executor_id).is_some_and(|(_, room, _)| {
                        cairn_common::executor_protocol::fits(&room.capacity(), &candidate.need)
                            .is_err()
                    })
                });
                if never {
                    let need = figured
                        .candidates
                        .values()
                        .next()
                        .map(|candidate| format!("{:?}", candidate.need))
                        .unwrap_or_default();
                    return Err(executor_unavailable(format!(
                    "this work can never fit any announced machine while standing residents remain, even after running commands end: needs {need}"
                )));
                }
            }
            let (deliveries, receiver) = mpsc::unbounded_channel();
            placing.pending.insert(
                entry_id.to_string(),
                Pending {
                    request: Arc::new(request.clone()),
                    plan,
                    interactive,
                    execution_residency,
                    resolved: figured.resolved,
                    stamp,
                    reserve_from_unix_ms: reserve_from,
                    excluded: figured.excluded,
                    deliveries,
                },
            );
            let waiting = Waiting {
                entry_id: entry_id.to_string(),
                standing,
                priority: request.priority,
                queued_at_unix_ms: queued_at,
                sequence,
                horizon_unix_ms: horizon,
                reserve_from_unix_ms: reserve_from,
                candidates: figured.candidates,
                may_spill: request.placement_mobility.may_spill()
                    && request.pinned_executor_id.is_none()
                    && request
                        .executor
                        .as_ref()
                        .is_none_or(|selector| selector.name.is_none()),
            };
            self.dispatch_locked(&connections, &mut placing, |core, mut admit, now| {
                core.enqueue(waiting, now, &mut admit)
            });
            drop(placing);
            drop(connections);
            self.placing_deadline.notify_one();
            return Ok(receiver);
        }
    }

    /// Wait for the dispatcher to assign this entry a machine, and answer as
    /// the old per-request loop did: the placement to offer, or the outcome
    /// the requester is owed. `deliveries` is this entry's channel, joined on
    /// first use and again when the entry has left the dispatcher (a claim
    /// that came back).
    // As `join_dispatcher`: the outcome owed, returned once, never stored.
    #[allow(clippy::result_large_err)]
    pub(super) async fn await_assignment(
        &self,
        entry_id: &str,
        request: &CellRequest,
        plan: Option<&Arc<ReservationPlan>>,
        policy: &ActivePlacementPolicy,
        deliveries: &mut Option<mpsc::UnboundedReceiver<Delivery>>,
    ) -> Result<Placement, CellOutcome> {
        loop {
            let joined = deliveries.is_some() && self.lock_placing().core.contains(entry_id);
            if !joined {
                *deliveries = Some(
                    self.join_dispatcher(entry_id, request, plan.cloned(), policy)
                        .await?,
                );
            }
            let receiver = deliveries.as_mut().expect("joined above");
            let remaining = request.wait_horizon_unix_ms.saturating_sub(unix_time_ms());
            let delivery =
                match tokio::time::timeout(Duration::from_millis(remaining), receiver.recv()).await
                {
                    Ok(delivery) => delivery,
                    Err(_) => Some(Delivery::Expired(self.waiting_rejections(entry_id))),
                };
            match delivery {
                Some(Delivery::Assigned(assignment)) => {
                    match self.placement_for(entry_id, request, policy, &assignment) {
                        Some(placement) => return Ok(placement),
                        // The link it was assigned on is gone or closed:
                        // nothing was sent, and the entry waits again.
                        None => self.dispatcher_declined(entry_id, &assignment.executor_id),
                    }
                }
                Some(Delivery::Expired(rejected)) => {
                    self.leave_dispatcher(entry_id);
                    return Err(self.refuse_at_horizon(request, policy, Some(entry_id), rejected));
                }
                None => {
                    *deliveries = None;
                    if !matches!(
                        self.queue.state_of(entry_id),
                        Some(queue::EntryState::Queued)
                    ) {
                        return Err(CellOutcome::Cancelled {
                            request_id: request.request_id.clone(),
                            attempt_id: request.attempt_id.clone(),
                        });
                    }
                }
            }
        }
    }

    /// The placement an assignment stands for: the machine's current link,
    /// the profile resolved for it, and the decision record. `None` when the
    /// link it was made on is no longer current.
    fn placement_for(
        &self,
        entry_id: &str,
        request: &CellRequest,
        policy: &ActivePlacementPolicy,
        assignment: &Assignment,
    ) -> Option<Placement> {
        // Taken before the locks below: each takes a lock that comes first
        // (the connection table) or after the placing lock (the queue).
        let unattached = self.unattached_enrolled_remotes();
        let declines = self
            .queue
            .with_entry(entry_id, |entry| entry.declined.clone())
            .unwrap_or_default();
        let now = unix_time_ms();
        let connections = self.lock_connections();
        let machine = connections.get(&assignment.executor_id).filter(|machine| {
            machine.generation == assignment.generation && !machine.sender.is_closed()
        })?;
        let placing = self.lock_placing();
        let pending = placing.pending.get(entry_id)?;
        let chosen_name = executor_public_name(machine);
        // Every machine passed over, as the account tells it now: a machine
        // whose room also fit was passed over for the one preferred.
        let mut rejected: Vec<PlacementRejection> = account_rows(
            &connections,
            &unattached,
            &placing,
            entry_id,
            &declines,
            None,
            now,
        )
        .unwrap_or_default()
        .into_iter()
        .filter(|row| row.executor_id != assignment.executor_id)
        .filter_map(|row| match row.candidate.status {
            CheckCandidateStatus::Fits { .. } => {
                Some(row.rejection(PlacementRejectionReason::OutrankedBy {
                    executor_name: chosen_name.clone(),
                }))
            }
            _ => row.recorded(now),
        })
        .collect();
        let resolved = pending.resolved.get(&assignment.executor_id).cloned();
        let headroom = assignment.authorized.then_some(HeadroomAuthorization {
            reserve_from_unix_ms: pending.reserve_from_unix_ms,
            forecast_ms: None,
        });
        let candidates = placing.core.candidates(entry_id).map_or(0, HashMap::len);
        let need = placing
            .core
            .candidates(entry_id)
            .and_then(|candidates| candidates.get(&assignment.executor_id))
            .map(|candidate| format!("{:?}", candidate.need))
            .unwrap_or_default();
        let room = placing
            .core
            .room(&assignment.executor_id)
            .map(|(seq, room, _)| format!("#{seq} {room:?}"))
            .unwrap_or_default();
        drop(placing);
        let executor_name = executor_public_name(machine);
        let scope = PlacementScope::of(request);
        let reason = if request.pinned_executor_id.is_some() {
            PlacementReason::Pinned
        } else if candidates == 1 {
            PlacementReason::OnlyCandidate
        } else if !scope.targeted() && !request.placement_mobility.may_spill() {
            PlacementReason::ColocatedHome
        } else {
            PlacementReason::FirstWithRoom
        };
        log::debug!(
            "placement: entry {entry_id} assigned to {executor_name}{}: first machine with room; room {room}, need {need}",
            if assignment.reserve { " to hold room" } else { "" }
        );
        let selection = PlacementSelection {
            executor_name,
            executor_id: machine.identity.executor_id.clone(),
            colocated: machine.colocated,
            reason,
            tie_break: None,
            readings: placement_readings(&machine.health.machine),
            reservation: resolved.as_ref().map_or_else(
                || request.resource_reservation.clone(),
                |resolved| resolved.reservation.clone(),
            ),
            reservation_rationale: resolved.as_ref().map_or_else(
                || unresolved_rationale(&request.resource_reservation),
                |resolved| resolved.rationale.clone(),
            ),
            sync_cost: PlacementSyncCost::Unknown,
            object_transfer: None,
            observation_reuse: if machine.colocated {
                ObservationReuse::Colocated
            } else {
                ObservationReuse::UntrustedRemoteEnvironment
            },
            prediction: None,
            memory_ceiling: resolved
                .as_ref()
                .and_then(|resolved| resolved.memory_fit.ceiling()),
            process_fit_waived: None,
            nested_sandbox_waived: None,
            restart_exposure: None,
            process_demand: resolved
                .as_ref()
                .and_then(|resolved| process_demand_record(&resolved.process_evidence, machine)),
            cache_distance: None,
        };
        let selected = selected_executor(machine);
        drop(connections);
        self.stamp_update_bounds(&mut rejected);
        let decision = placement_decision(
            request,
            now,
            Some(policy_evidence(request, policy, false)),
            None,
            PlacementOutcome::Selected(Box::new(selection)),
            rejected,
        );
        Some(Placement {
            selected,
            reservation: resolved,
            decision,
            above_ceiling: None,
            headroom,
        })
    }

    /// Why `entry_id` is still queued, machine by machine
    /// ([`CheckPlacementAccount`]), worked out now. `declines` and
    /// `offered_to` are the queue's, which the caller reads first: the
    /// queue's lock comes after the placing lock. `None` when the entry is
    /// not waiting in the dispatcher.
    pub(super) fn placement_account(
        &self,
        entry_id: &str,
        declines: &HashMap<String, queue::EntryDecline>,
        offered_to: Option<&str>,
        now_unix_ms: u64,
    ) -> Option<CheckPlacementAccount> {
        let unattached = self.unattached_enrolled_remotes();
        let (request_id, attempt_id, rows) = {
            let connections = self.lock_connections();
            let placing = self.lock_placing();
            let request = placing.pending.get(entry_id)?.request.clone();
            let rows = account_rows(
                &connections,
                &unattached,
                &placing,
                entry_id,
                declines,
                offered_to,
                now_unix_ms,
            )?;
            (request.request_id.clone(), request.attempt_id.clone(), rows)
        };
        // The rollout's lock, once the others are released. The update is
        // told at the minute: it says how long the drain still waits, and a
        // line quoting it changes at most once a minute.
        let minute = now_unix_ms - now_unix_ms % 60_000;
        let machines = rows
            .into_iter()
            .map(|mut row| {
                if let CheckCandidateStatus::Away {
                    reason: PlacementRejectionReason::UpdatePending { .. },
                    sentence,
                } = &mut row.candidate.status
                {
                    *sentence = self.update_wait_sentence(
                        &row.executor_id,
                        &row.candidate.executor_name,
                        minute,
                    );
                }
                row.candidate
            })
            .collect();
        Some(CheckPlacementAccount {
            request_id,
            attempt_id,
            machines,
        })
    }

    /// The account's rows as a decision's rejections, for the refusal owed
    /// when an entry's horizon arrives: recorded once, at that event.
    fn waiting_rejections(&self, entry_id: &str) -> Vec<PlacementRejection> {
        let unattached = self.unattached_enrolled_remotes();
        let (declines, offered_to) = self
            .queue
            .with_entry(entry_id, |entry| {
                (
                    entry.declined.clone(),
                    entry.state.executor_id().map(str::to_string),
                )
            })
            .unwrap_or_default();
        let now = unix_time_ms();
        let mut rejected: Vec<PlacementRejection> = {
            let connections = self.lock_connections();
            let placing = self.lock_placing();
            account_rows(
                &connections,
                &unattached,
                &placing,
                entry_id,
                &declines,
                offered_to.as_deref(),
                now,
            )
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| row.recorded(now))
            .collect()
        };
        self.stamp_update_bounds(&mut rejected);
        rejected
    }

    /// What work waiting on `executor_id`'s self-update is told: the machine,
    /// that it is being updated, and how much of that is left.
    pub(super) fn update_wait_sentence(
        &self,
        executor_id: &str,
        name: &str,
        now_unix_ms: u64,
    ) -> String {
        use cairn_common::executor_protocol::describe_clock;
        let wait = self.self_update.lock().unwrap().update_wait(executor_id);
        let progress = match wait {
            Some(rollout::UpdateWait::Draining {
                holds,
                residents,
                returns_by_unix_ms,
            }) => {
                let state = match (holds.first(), residents.as_slice()) {
                    (Some(hold), residents) => format!(
                        "{} in flight, {}{}, {}{}",
                        holds.len(),
                        if holds.len() > 1 { "the longest " } else { "" },
                        hold.label,
                        crate::resources::executors::hold_remaining(hold, now_unix_ms),
                        match residents {
                            [] => String::new(),
                            residents =>
                                format!("; the restart ends {}", rollout::listed(residents)),
                        }
                    ),
                    (None, []) => "empty, about to install it".to_string(),
                    (None, [one]) => format!("only {one} remains, which the restart ends"),
                    (None, residents) => format!(
                        "only {} remain, which the restart ends",
                        rollout::listed(residents)
                    ),
                };
                let back = match returns_by_unix_ms {
                    Some(at) => format!(", expected back by {}", describe_clock(at)),
                    None => ", with no safe restart time yet while work remains or the machine updating ahead has no bound".to_string(),
                };
                format!(" ({state}){back}")
            }
            Some(rollout::UpdateWait::Directed {
                at_unix_ms,
                returns_by_unix_ms,
            }) => format!(
                " (installing it, directed {} ago), expected back by {}",
                residency_routes::describe_quiet_ms(now_unix_ms.saturating_sub(at_unix_ms)),
                describe_clock(returns_by_unix_ms)
            ),
            None => String::new(),
        };
        format!(
            "{name} being emptied for an executor update{progress}, which ends on its own when it returns on the runner's build (cairn://executors/{name})"
        )
    }

    /// The entry stopped waiting: settled, retired, cancelled, expired or
    /// claimed. Room held for it is released, and what an unclaimed offer of
    /// it took from a machine's room is given back.
    pub(super) fn leave_dispatcher(&self, entry_id: &str) {
        let connections = self.lock_connections();
        let mut placing = self.lock_placing();
        placing.pending.remove(entry_id);
        // A claim the queue granted before the dispatcher heard of it keeps
        // what its offer took. The queue's lock comes after the placing lock.
        let claimed_by = match self.queue.state_of(entry_id) {
            Some(queue::EntryState::Claimed { executor_id, .. }) => Some(executor_id),
            _ => None,
        };
        self.dispatch_locked(
            &connections,
            &mut placing,
            |core, mut admit, now| match &claimed_by {
                Some(executor_id) => core.claimed(entry_id, executor_id),
                None => core.withdraw(entry_id, now, &mut admit),
            },
        );
    }

    /// A machine refused this entry, its link closed with the offer unsent,
    /// or it never answered. The refusal stands under the figures the
    /// machine last announced, and the entry is matched against the other
    /// machines. What the offer took is given back on the machine's first
    /// refusal since its last claim under those figures and kept after that
    /// ([`super::dispatcher::Dispatcher::decline`]), so a closed link, whose
    /// room goes with it when it detaches, is assigned at most one more
    /// entry before then. A requeue here would assign the same closed link
    /// again every time, since admission does not read the link's liveness.
    pub(super) fn dispatcher_declined(&self, entry_id: &str, executor_id: &str) {
        let connections = self.lock_connections();
        let mut placing = self.lock_placing();
        let seq = placing.core.room(executor_id).map_or(0, |(seq, _, _)| seq);
        self.dispatch_locked(&connections, &mut placing, |core, mut admit, now| {
            core.decline(entry_id, executor_id, seq, now, &mut admit)
        });
    }

    /// An offer came back that the machine never heard: the entry waits again
    /// as it stands, and the room the offer took is given back.
    pub(super) fn dispatcher_requeued(&self, entry_id: &str) {
        self.dispatch(|core, mut admit, now| core.requeue(entry_id, now, &mut admit));
    }

    /// A machine claimed the entry: it leaves the dispatcher, and room held
    /// for it elsewhere is released.
    pub(super) fn dispatcher_claimed(&self, entry_id: &str, executor_id: &str) {
        let connections = self.lock_connections();
        let mut placing = self.lock_placing();
        placing.pending.remove(entry_id);
        self.dispatch_locked(&connections, &mut placing, |core, _, _| {
            core.claimed(entry_id, executor_id)
        });
    }

    /// A machine announced its room. The caller holds the connection lock.
    /// When `admit` last turned work away there, the frame takes the place of
    /// the post that would have walked the machine again
    /// ([`Self::machine_facts_posted`]), so a frame repeating its figures,
    /// which matches nothing of itself, walks it.
    pub(super) fn dispatcher_room(
        &self,
        connections: &HashMap<String, ExecutorConnectionState>,
        executor_id: &str,
        generation: u64,
        seq: u64,
        room: cairn_common::executor_protocol::AnnouncedRoom,
    ) {
        let mut placing = self.lock_placing();
        if placing.core.generation(executor_id) != Some(generation) {
            placing
                .attached_at
                .insert(executor_id.to_string(), unix_time_ms());
        }
        if placing.core.generation(executor_id) == Some(generation)
            && placing
                .core
                .room(executor_id)
                .is_some_and(|(held_seq, _, _)| held_seq >= seq)
        {
            return;
        }
        let slots_moved = placing
            .core
            .room(executor_id)
            .is_none_or(|(_, held, _)| held.slots != room.slots);
        // Learned tree figures and the charged reservation are priced at a
        // slot width. Do not offer a figure from the previous width while
        // its off-lock refresh is in flight.
        let repricing: Vec<_> = if slots_moved {
            placing
                .pending
                .iter()
                .filter_map(|(id, pending)| {
                    pending.plan.as_ref()?;
                    let mut candidates = placing.core.candidates(id)?.clone();
                    candidates.get_mut(executor_id)?.away = true;
                    Some((id.clone(), candidates))
                })
                .collect()
        } else {
            Vec::new()
        };
        let needs_refigure = !repricing.is_empty();
        let turned_away = placing.admit_refused.remove(executor_id);
        self.dispatch_locked(connections, &mut placing, |core, mut admit, now| {
            let mut effects = core.attach_link(executor_id, generation, now, &mut admit);
            for (id, candidates) in repricing {
                effects.absorb(core.set_candidates(&id, candidates, now, &mut admit));
            }
            let repeats = core
                .room(executor_id)
                .is_some_and(|(_, held, _)| *held == room);
            effects.absorb(core.set_room(executor_id, generation, seq, room, now, &mut admit));
            if turned_away && repeats {
                effects.absorb(core.rewalk(executor_id, now, &mut admit));
            }
            effects
        });
        drop(placing);
        if needs_refigure {
            self.spawn_refigure(Some(executor_id.to_string()));
        }
    }

    /// A machine's link was made. It has no room until it announces; what
    /// each waiting entry needs there is worked out now.
    pub(super) fn dispatcher_attached(&self, executor_id: &str, generation: u64) {
        {
            let connections = self.lock_connections();
            let facts = connections
                .get(executor_id)
                .filter(|entry| entry.generation == generation)
                .map(placement_facts);
            let mut placing = self.lock_placing();
            if placing.core.generation(executor_id) != Some(generation) {
                placing
                    .attached_at
                    .insert(executor_id.to_string(), unix_time_ms());
            }
            self.dispatch_locked(&connections, &mut placing, |core, mut admit, now| {
                core.attach_link(executor_id, generation, now, &mut admit)
            });
            match facts {
                Some(facts) => placing.facts.insert(executor_id.to_string(), facts),
                None => placing.facts.remove(executor_id),
            };
            if placing.pending.is_empty() {
                return;
            }
        }
        self.spawn_refigure(Some(executor_id.to_string()));
    }

    /// A machine's link ended: entries holding room there look elsewhere,
    /// and its offers, back in the queue, wait again.
    pub(super) fn dispatcher_detached(
        &self,
        executor_id: &str,
        generation: u64,
        released_offers: &[String],
    ) {
        let connections = self.lock_connections();
        let mut placing = self.lock_placing();
        if placing.core.generation(executor_id) == Some(generation) {
            placing.facts.remove(executor_id);
            placing.admit_refused.remove(executor_id);
            placing.attached_at.remove(executor_id);
        }
        self.dispatch_locked(&connections, &mut placing, |core, mut admit, now| {
            let mut effects = core.detach(executor_id, generation, now, &mut admit);
            for entry_id in released_offers {
                effects.absorb(core.requeue(entry_id, now, &mut admit));
            }
            effects
        });
        // A Mac that could nest leaving may let a floored one take a lane.
        let nesting = placing
            .pending
            .values()
            .any(|pending| needs_nesting(&pending.request));
        drop(placing);
        drop(connections);
        if nesting {
            self.spawn_refigure(Some(executor_id.to_string()));
        }
    }

    /// Something about this machine was posted. When its placement facts
    /// moved, what each waiting entry may do there is worked out again; when
    /// `admit` last turned work away from it, its room is walked once more.
    /// Otherwise nothing runs.
    pub(super) fn machine_facts_posted(&self, executor_id: &str) {
        let moved = {
            let connections = self.lock_connections();
            let Some(facts) = connections.get(executor_id).map(placement_facts) else {
                return;
            };
            let mut placing = self.lock_placing();
            if placing.pending.is_empty() {
                placing.facts.insert(executor_id.to_string(), facts);
                return;
            }
            if placing.admit_refused.remove(executor_id) {
                self.dispatch_locked(&connections, &mut placing, |core, mut admit, now| {
                    core.rewalk(executor_id, now, &mut admit)
                });
            }
            placing.facts.insert(executor_id.to_string(), facts) != Some(facts)
        };
        if moved {
            self.spawn_refigure(Some(executor_id.to_string()));
        }
    }

    /// [`Self::machine_facts_posted`] for every attached machine, after a
    /// stamp that may have touched several (drains, roles, model residents).
    pub(super) fn machine_facts_posted_all(&self) {
        let machines: Vec<String> = self.lock_connections().keys().cloned().collect();
        for executor_id in machines {
            self.machine_facts_posted(&executor_id);
        }
    }

    fn spawn_refigure(&self, only: Option<String>) {
        let fleet = self.clone();
        let refigure = async move { fleet.refigure(only.as_deref(), false).await };
        match &self.host_runtime {
            Some(runtime) => {
                runtime.spawn(refigure);
            }
            None => {
                tokio::spawn(refigure);
            }
        }
    }

    /// The profile store learned something: work out again the needs of the
    /// entries whose scopes it named, once each.
    async fn refigure_learned(&self) {
        self.refigure(None, true).await;
    }

    /// Work out again what waiting entries may do on `only` (or on every
    /// machine), and hand the dispatcher what changed. With `learned`, only
    /// entries whose needs stamp moved are worked out.
    async fn refigure(&self, only: Option<&str>, learned: bool) {
        type Work = (
            String,
            Arc<CellRequest>,
            Option<Arc<ReservationPlan>>,
            bool,
            Option<ResidencyFence>,
        );
        let entries: Vec<Work> = self
            .lock_placing()
            .pending
            .iter()
            .filter(|(_, pending)| {
                !learned
                    || pending
                        .plan
                        .as_ref()
                        .is_some_and(|plan| Some(plan.needs_stamp()) != pending.stamp)
            })
            .map(|(entry_id, pending)| {
                (
                    entry_id.clone(),
                    pending.request.clone(),
                    pending.plan.clone(),
                    pending.interactive,
                    pending.execution_residency.clone(),
                )
            })
            .collect();
        for (entry_id, request, plan, interactive, residency) in entries {
            // Whether a floored Mac may take a lane needing nesting turns on
            // every other Mac, so such a lane is figured over all of them.
            let only = only.filter(|_| !needs_nesting(&request));
            let stamp = plan.as_ref().map(|plan| plan.needs_stamp());
            let figured = self
                .figure(
                    &request,
                    plan.as_deref(),
                    interactive,
                    residency.as_ref(),
                    only,
                )
                .await;
            let connections = self.lock_connections();
            let mut placing = self.lock_placing();
            let Some(current) = placing.core.candidates(&entry_id).cloned() else {
                continue;
            };
            // Merged machine by machine: what was figured replaces what the
            // entry holds only on a machine still at the epoch the snapshot
            // saw. A machine attached since keeps the candidacy its own
            // re-figure gave it, and one that moved is left to the re-figure
            // its move started.
            let fresh: HashSet<&String> = figured
                .seen
                .iter()
                .filter(|(id, at)| epoch(&connections, &placing.facts, id) == Some(**at))
                .map(|(id, _)| id)
                .collect();
            let Some(pending) = placing.pending.get_mut(&entry_id) else {
                continue;
            };
            if learned {
                pending.stamp = stamp;
            }
            pending.resolved.retain(|id, _| !fresh.contains(id));
            pending.resolved.extend(
                figured
                    .resolved
                    .into_iter()
                    .filter(|(id, _)| fresh.contains(id)),
            );
            pending
                .excluded
                .retain(|rejection| !fresh.contains(&rejection.executor_id));
            pending.excluded.extend(
                figured
                    .excluded
                    .into_iter()
                    .filter(|rejection| fresh.contains(&rejection.executor_id)),
            );
            let mut next = current.clone();
            next.retain(|id, _| !fresh.contains(id));
            next.extend(
                figured
                    .candidates
                    .into_iter()
                    .filter(|(id, _)| fresh.contains(id)),
            );
            if next == current {
                continue;
            }
            self.dispatch_locked(&connections, &mut placing, |core, mut admit, now| {
                core.set_candidates(&entry_id, next, now, &mut admit)
            });
        }
    }
}

#[cfg(test)]
impl Fleet {
    /// Run `hook` once, the next time an entry is figured, between the
    /// connection snapshot and the work worked out from it.
    pub(super) fn after_figure_snapshot_for_test(&self, hook: SnapshotHook) {
        self.lock_placing().after_snapshot = Some(hook);
    }

    /// Make the deadline task panic the next time a deadline fires.
    pub(super) fn panic_at_next_deadline_for_test(&self) {
        self.lock_placing().panic_at_deadline = true;
    }

    /// The machines the dispatcher holds `entry_id` a candidate on, sorted.
    pub(super) fn dispatcher_candidates_for_test(&self, entry_id: &str) -> Vec<String> {
        let mut machines: Vec<String> = self
            .lock_placing()
            .core
            .candidates(entry_id)
            .map(|candidates| candidates.keys().cloned().collect())
            .unwrap_or_default();
        machines.sort();
        machines
    }
}
