//! What a machine's posted state says about the reason it gave for a decline.
//!
//! A decline stops an entry being offered to the machine that gave it, and
//! something has to say when asking again is worth it. "Whenever the machine
//! posts anything" was that rule, and it made a hot loop: a snapshot carries
//! readings that move on nearly every frame (free disk, CPU utilization), so a
//! machine that declined for pressure was re-asked, and declined again, as fast
//! as it could post. On 10-16 one request was declined 1,160 times in a second.
//!
//! The rule now is that a decline holds until the posted condition behind *its
//! reason* changes. [`posted_condition`] reads that condition from the state
//! the machine posted; the queue records it beside the decline and supersedes
//! the decline only when a later post reads differently. When nothing posted
//! explains a decline (an older executor, a request-specific refusal, a reason
//! with no posted counterpart) the answer is `None` and any new posted state
//! supersedes it, as before. Either way the queue's per-machine re-offer
//! backoff is the ceiling, so a condition that flaps cannot loop either.
//!
//! The same posted state answers the forward question too. [`posted_hold`]
//! asks, before an offer, whether the machine's own admission would decline
//! this request on what it has posted, walking the executor's check order.
//! Ranking reads it so that a machine which holds, posts, and would decline
//! again does not re-enter the ranking looking free and outrank an idle one.
//! Both questions go through the per-reason readers below, so the condition a
//! hold reports is exactly the one a decline for that reason would record.

use std::borrow::Cow;

use cairn_common::executor_protocol::{
    admission_provisions, AdmissionHoldEvidence, CellPriority, CellRequest, CpuAdmissionState,
    DeclineReason, DiskHealthStatus, ExecutorSubstrateReport, FleetSnapshot, ProvisioningHold,
    RepositoryLocator, ResidencyFence, StoragePressureOutcome, StorageSweepStatus,
    ADMISSION_MEMORY_FLOOR_BYTES, CPU_ADMISSION_BLIND_AFTER_MS,
};

/// How the executor will judge one piece of work: the facts the offer carries
/// beside the request, which change what its admission checks.
///
/// Built from the queue entry, which is also what the offer is built from, so
/// the view the runner reasons with is the view the executor will judge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AdmissionView {
    /// The offer's `interactive`: the executor skips host pressure for it and
    /// checks its disk against the bare watermark.
    pub(crate) interactive: bool,
    /// The queue entry being placed. A provisioning hold this entry took is
    /// its own, not something it waits on.
    pub(crate) entry_id: Option<String>,
    /// The residency the work runs inside, which decides whether it provisions.
    pub(crate) execution_residency: Option<ResidencyFence>,
}

impl AdmissionView {
    /// The view of a request with no queue entry behind it: interactive when
    /// its priority says so, inside no residency.
    pub(crate) fn of_request(request: &CellRequest) -> Self {
        Self {
            interactive: request.priority == CellPriority::AgentInteractive,
            entry_id: None,
            execution_residency: None,
        }
    }

    /// Whether the executor will judge this work as provisioning on `machine`,
    /// by the executor's own predicate, against the cells the machine posted.
    fn provisions(&self, request: &CellRequest, machine: PostedState<'_>) -> bool {
        let repository = offered_repository(request, machine.colocated);
        let resident_here = self.execution_residency.as_ref().is_some_and(|fence| {
            fence.is_active_among(&request.project_id, &machine.snapshot.cells)
        });
        admission_provisions(&repository, resident_here)
    }
}

/// The repository an offer to this machine names. A remote machine is offered
/// managed objects and the colocated one the request's own locator, which is
/// how both offer paths rewrite it. (A remote acquisition keeps a scratch-only
/// locator, which provisions exactly as managed objects do outside a
/// residency, and an acquisition never runs inside one.)
fn offered_repository(request: &CellRequest, colocated: bool) -> Cow<'_, RepositoryLocator> {
    if colocated {
        return Cow::Borrowed(&request.repository);
    }
    let identity = request.repository.identity();
    Cow::Owned(RepositoryLocator::ManagedObjects {
        project_id: identity.project_id,
        repository_id: identity.repository_id,
        object_format: identity.object_format,
    })
}

/// One machine's posted state, and whether it is the colocated machine.
#[derive(Clone, Copy)]
pub(crate) struct PostedState<'a> {
    pub(crate) snapshot: &'a FleetSnapshot,
    pub(crate) health: &'a ExecutorSubstrateReport,
    pub(crate) colocated: bool,
}

/// A condition a machine has posted that its admission would decline this
/// request on, in the executor's check order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostedHold {
    /// The reason the executor would decline with.
    pub(crate) reason: DeclineReason,
    /// What [`posted_condition`] reads for that reason, so a decline that
    /// follows records the same condition this hold reported.
    pub(crate) condition: PostedCondition,
    pub(crate) evidence: AdmissionHoldEvidence,
    /// Whether anything the machine is doing now can end the hold. Pressure
    /// and a provisioning hold end when the work causing them does; a disk
    /// hold ends by itself only while a sweep is in flight, which is measured
    /// rather than declared; a latched cleanup needs an operator.
    pub(crate) relieves_itself: bool,
}

/// Whether this machine's admission would decline this request on what it
/// has posted, and on which condition.
///
/// Walks the executor's `evaluate_admission` order and returns the first
/// condition that would decline: the cleanup latch, the project's
/// provisioning hold (only for work that provisions, and only one another
/// request holds), disk headroom, then host pressure (skipped for interactive
/// work, as the executor skips it). Capacity is not a hold: units that do not
/// fit are priced by the queue forecast.
///
/// The CPU verdict counts only while it is fresh, the same bound ranking uses
/// to call a machine blind; a stale verdict is blindness, which ranking
/// handles on its own.
pub(crate) fn posted_hold(
    view: &AdmissionView,
    request: &CellRequest,
    machine: PostedState<'_>,
    now_unix_ms: u64,
) -> Option<PostedHold> {
    let health = machine.health;
    if health.disk.cleanup_blocked {
        if let Some(reading) = read_disk(view, request, machine) {
            return Some(reading.into_hold(DeclineReason::Disk, false));
        }
    }
    if view.provisions(request, machine) {
        if let Some(reading) = read_provisioning(view, request, machine) {
            return Some(reading.into_hold(DeclineReason::ProjectProvisioning, true));
        }
    }
    if let Some(reading) = read_disk(view, request, machine) {
        let sweeping = health.disk.sweep_status == StorageSweepStatus::InFlight;
        return Some(reading.into_hold(DeclineReason::Disk, sweeping));
    }
    if !view.interactive {
        let cpu_fresh = health
            .host
            .cpu_admission
            .measured_at_unix_ms
            .is_some_and(|measured_at| {
                now_unix_ms.saturating_sub(measured_at) <= CPU_ADMISSION_BLIND_AFTER_MS
            });
        if let Some(mut reading) = read_pressure(request, health) {
            let AdmissionHoldEvidence::Pressure {
                cpu_pressured,
                available_memory_bytes,
                memory_floor_bytes,
            } = &mut reading.evidence
            else {
                unreachable!("the pressure reader reads pressure");
            };
            *cpu_pressured &= cpu_fresh;
            let memory_short =
                available_memory_bytes.is_some_and(|available| available < *memory_floor_bytes);
            if *cpu_pressured || memory_short {
                return Some(reading.into_hold(DeclineReason::Pressure, true));
            }
        }
    }
    None
}

/// A reason's posted condition with the figures a person reads it by.
struct Reading {
    condition: PostedCondition,
    evidence: AdmissionHoldEvidence,
}

impl Reading {
    fn into_hold(self, reason: DeclineReason, relieves_itself: bool) -> PostedHold {
        PostedHold {
            reason,
            condition: self.condition,
            evidence: self.evidence,
            relieves_itself,
        }
    }
}

/// The free space the executor's admission requires before it takes this
/// work: cairn-exec's `admission_disk_pressure_condition` over
/// `disk_charge_for`. Interactive work is checked against the bare watermark
/// plus the growth it is charged, which is none unless it provisions;
/// everything else against the watermark plus the growth admitted work holds
/// plus its own.
fn disk_floor_bytes(
    view: &AdmissionView,
    provisions: bool,
    request: &CellRequest,
    health: &ExecutorSubstrateReport,
) -> u64 {
    let requested = request.resource_reservation.disk_growth_bytes;
    let watermark = health.disk.watermark_bytes;
    if view.interactive {
        watermark.saturating_add(if provisions { requested } else { 0 })
    } else {
        watermark
            .saturating_add(health.admission.active_reservation.disk_growth_bytes)
            .saturating_add(requested)
    }
}

fn read_disk(
    view: &AdmissionView,
    request: &CellRequest,
    machine: PostedState<'_>,
) -> Option<Reading> {
    let health = machine.health;
    let disk = &health.disk;
    // The executor also charges residency footprints it does not post, so this
    // can read a refusal as unexplained; for supersession that falls back to
    // any post superseding it, and for ranking to no hold, which are the safe
    // directions.
    let floor = disk_floor_bytes(view, view.provisions(request, machine), request, health);
    // The executor judges disk on the volume cells grow on: the build root's
    // while it is here, the home's otherwise.
    let free = health
        .machine
        .build_volume
        .as_ref()
        .and_then(|volume| volume.value())
        .or_else(|| health.machine.volume.value())
        .map(|volume| volume.free_bytes);
    let headroom = free.map(|free| i128::from(free) - i128::from(floor));
    let short = disk.watermark_bytes > 0 && headroom.is_some_and(|headroom| headroom < 0);
    if !(disk.cleanup_blocked || short) {
        return None;
    }
    let sweep_in_flight = disk.sweep_status == StorageSweepStatus::InFlight;
    Some(Reading {
        condition: PostedCondition::Disk {
            status: disk.status,
            sweep_in_flight,
            cleanup_blocked: disk.cleanup_blocked,
            last_pass: disk.pressure.as_ref().map(|pass| pass.outcome),
            headroom_bytes: headroom.unwrap_or(i128::MIN),
        },
        evidence: if disk.cleanup_blocked {
            AdmissionHoldEvidence::CleanupLatched {
                error: disk.cleanup_last_error.clone(),
            }
        } else {
            AdmissionHoldEvidence::Disk {
                free_bytes: free.unwrap_or(0),
                floor_bytes: floor,
                sweep_in_flight,
            }
        },
    })
}

/// The hold on this request's project that some other request took. A hold
/// this entry or this request took is its own, and waits on nothing.
fn foreign_hold<'a>(
    view: &AdmissionView,
    request: &CellRequest,
    snapshot: &'a FleetSnapshot,
) -> Option<&'a ProvisioningHold> {
    snapshot.provisioning_holds.iter().find(|hold| {
        hold.project_id == request.project_id
            && hold.owner_request_id != request.request_id
            && !(view.entry_id.is_some() && hold.entry_id == view.entry_id)
    })
}

fn read_provisioning(
    view: &AdmissionView,
    request: &CellRequest,
    machine: PostedState<'_>,
) -> Option<Reading> {
    foreign_hold(view, request, machine.snapshot).map(|hold| Reading {
        condition: PostedCondition::ProjectProvisioning {
            owner_request_id: hold.owner_request_id.clone(),
            entry_id: hold.entry_id.clone(),
            since_unix_ms: hold.since_unix_ms,
        },
        evidence: AdmissionHoldEvidence::ProjectProvisioning {
            project_id: hold.project_id.clone(),
            holder_request_id: hold.owner_request_id.clone(),
            holder_entry_id: hold.entry_id.clone(),
            since_unix_ms: hold.since_unix_ms,
        },
    })
}

fn read_pressure(request: &CellRequest, health: &ExecutorSubstrateReport) -> Option<Reading> {
    let cpu_pressured = health.host.cpu_admission.state == CpuAdmissionState::Pressured;
    let floor = ADMISSION_MEMORY_FLOOR_BYTES.max(request.resource_reservation.memory_bytes);
    let available = health
        .machine
        .memory
        .value()
        .map(|memory| memory.available_bytes);
    let memory_short = available.is_some_and(|available| available < floor);
    (cpu_pressured || memory_short).then_some(Reading {
        condition: PostedCondition::Pressure {
            cpu_pressured,
            memory_short,
        },
        evidence: AdmissionHoldEvidence::Pressure {
            cpu_pressured,
            available_memory_bytes: available,
            memory_floor_bytes: floor,
        },
    })
}

/// The part of a machine's posted state that a decline reason depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostedCondition {
    /// The machine's sustained CPU verdict, and whether its posted free memory
    /// is below the floor this request would have to leave.
    Pressure {
        cpu_pressured: bool,
        memory_short: bool,
    },
    /// The machine's disk verdict, the state of the work that could relieve
    /// it, and its posted headroom: free space less the floor admission would
    /// charge this request (see `disk_floor_bytes`). Admission refuses when
    /// headroom is negative, so headroom growing by any amount (a reservation
    /// released, a sweep that freed bytes) is relief, while headroom shrinking
    /// or holding is not.
    Disk {
        status: DiskHealthStatus,
        sweep_in_flight: bool,
        cleanup_blocked: bool,
        last_pass: Option<StoragePressureOutcome>,
        headroom_bytes: i128,
    },
    /// What the machine has committed against what it will commit.
    Capacity {
        committed_units: u32,
        committed_memory_bytes: u64,
        capacity_units: Option<u32>,
        capacity_memory_bytes: Option<u64>,
    },
    /// The request holding this project's provisioning on the machine. The
    /// same owner implies the same entry and start, so equality is in effect
    /// keyed on the owner.
    ProjectProvisioning {
        owner_request_id: String,
        entry_id: Option<String>,
        since_unix_ms: u64,
    },
    /// The machine posts its build volume absent. The decline stands exactly
    /// as long as that post does, and the first post saying the volume is
    /// present supersedes it, with nothing for anyone to mark repaired.
    BuildVolumeAbsent { root: String },
}

impl PostedCondition {
    /// Whether `current`, the machine's latest posted condition for the same
    /// reason, still bears out the decline this condition was recorded with.
    ///
    /// Equality for every reason but disk, where only headroom that has not
    /// grown keeps the refusal standing: free space moving down or sideways
    /// by a few megabytes a frame is not relief, and any growth may be. A
    /// current condition that no longer explains the decline (`None`) never
    /// holds it.
    pub(crate) fn still_holds(&self, current: Option<&PostedCondition>) -> bool {
        let Some(current) = current else {
            return false;
        };
        match (self, current) {
            (
                Self::Disk {
                    status,
                    sweep_in_flight,
                    cleanup_blocked,
                    last_pass,
                    headroom_bytes: recorded,
                },
                Self::Disk {
                    status: now_status,
                    sweep_in_flight: now_sweeping,
                    cleanup_blocked: now_blocked,
                    last_pass: now_pass,
                    headroom_bytes: now,
                },
            ) => {
                status == now_status
                    && sweep_in_flight == now_sweeping
                    && cleanup_blocked == now_blocked
                    && last_pass == now_pass
                    && now <= recorded
            }
            (recorded, current) => recorded == current,
        }
    }
}

/// The posted condition behind `reason` for `request`, or `None` when the
/// machine's posted state does not show it.
///
/// `None` is not "clear". It means the decline cannot be tied to anything the
/// machine posts, so the caller falls back to superseding it on any new post.
/// That is the safe direction: a decline tied to a condition that never
/// changes would park the request until its horizon.
pub(crate) fn posted_condition(
    reason: DeclineReason,
    view: &AdmissionView,
    request: &CellRequest,
    machine: PostedState<'_>,
) -> Option<PostedCondition> {
    let reading = match reason {
        DeclineReason::Pressure => read_pressure(request, machine.health),
        DeclineReason::Disk => read_disk(view, request, machine),
        DeclineReason::Capacity => {
            let health = machine.health;
            let allocation = &health.allocation;
            // An executor that posts no commitment cannot be explaining a
            // capacity refusal with it.
            return (allocation.committed_concurrency_units > 0
                || allocation.committed_memory_bytes > 0)
                .then_some(PostedCondition::Capacity {
                    committed_units: allocation.committed_concurrency_units,
                    committed_memory_bytes: allocation.committed_memory_bytes,
                    capacity_units: health.admission.concurrency_capacity,
                    capacity_memory_bytes: health.admission.memory_capacity_bytes,
                });
        }
        DeclineReason::ProjectProvisioning => read_provisioning(view, request, machine),
        DeclineReason::BuildVolumeAbsent => {
            return machine
                .health
                .build_volume
                .as_ref()
                .filter(|volume| volume.is_absent())
                .map(|volume| PostedCondition::BuildVolumeAbsent {
                    root: volume.root.clone(),
                });
        }
        DeclineReason::Constraint | DeclineReason::Unknown => None,
    };
    reading.map(|reading| reading.condition)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        CpuAdmissionHealth, MachineMemory, MachineVolume, Measurement, ProvisioningHold,
    };

    const GIB: u64 = 1024 * 1024 * 1024;
    const NOW: u64 = 10_000_000;

    fn request() -> CellRequest {
        crate::fleet::queue::tests::request(
            "r",
            cairn_common::executor_protocol::PlacementMobility::SpillEligible,
        )
    }

    fn health() -> ExecutorSubstrateReport {
        ExecutorSubstrateReport::default()
    }

    fn colocated<'a>(
        snapshot: &'a FleetSnapshot,
        health: &'a ExecutorSubstrateReport,
    ) -> PostedState<'a> {
        PostedState {
            snapshot,
            health,
            colocated: true,
        }
    }

    /// The condition a decline for `reason` would record, for a request with
    /// no entry behind it.
    fn condition(
        reason: DeclineReason,
        request: &CellRequest,
        snapshot: &FleetSnapshot,
        health: &ExecutorSubstrateReport,
    ) -> Option<PostedCondition> {
        posted_condition(
            reason,
            &AdmissionView::of_request(request),
            request,
            colocated(snapshot, health),
        )
    }

    fn hold(
        view: &AdmissionView,
        request: &CellRequest,
        snapshot: &FleetSnapshot,
        health: &ExecutorSubstrateReport,
    ) -> Option<PostedHold> {
        posted_hold(view, request, colocated(snapshot, health), NOW)
    }

    fn with_cpu(
        mut health: ExecutorSubstrateReport,
        state: CpuAdmissionState,
    ) -> ExecutorSubstrateReport {
        health.host.cpu_admission = CpuAdmissionHealth {
            state,
            ..CpuAdmissionHealth::default()
        };
        health
    }

    /// A machine with `free` bytes against a 10 GiB watermark while its
    /// admitted work holds `reserved` bytes of growth.
    fn disk_at(free: u64, reserved: u64) -> ExecutorSubstrateReport {
        let mut health = health();
        health.disk.watermark_bytes = 10 * GIB;
        health.admission.active_reservation.disk_growth_bytes = reserved;
        health.machine.volume = Measurement::measured(
            1,
            MachineVolume {
                total_bytes: 1024 * GIB,
                free_bytes: free,
            },
        );
        health
    }

    fn growing(bytes: u64) -> CellRequest {
        let mut request = request();
        request.resource_reservation.disk_growth_bytes = bytes;
        request
    }

    fn provisioning(owner: &str, entry_id: Option<&str>) -> FleetSnapshot {
        FleetSnapshot {
            provisioning_holds: vec![ProvisioningHold {
                project_id: "p".into(),
                owner_request_id: owner.into(),
                entry_id: entry_id.map(str::to_string),
                since_unix_ms: NOW - 61_000,
            }],
            ..FleetSnapshot::default()
        }
    }

    /// A build-volume decline holds exactly as long as the machine posts the
    /// volume absent, and the first post saying present supersedes it, with
    /// nobody marking anything repaired.
    #[test]
    fn a_build_volume_decline_clears_when_the_volume_is_posted_present() {
        use cairn_common::executor_protocol::{BuildVolumeReport, BuildVolumeState};
        let request = request();
        let snapshot = FleetSnapshot::default();
        let report = |state| BuildVolumeReport {
            root: "/Volumes/Builds/cairn".into(),
            state,
            since_unix_ms: NOW,
            suspended_cells: Vec::new(),
            readopted_cells: 0,
        };
        let mut absent = health();
        absent.build_volume = Some(report(BuildVolumeState::Absent {
            reason: "not mounted".into(),
        }));
        let recorded = condition(
            DeclineReason::BuildVolumeAbsent,
            &request,
            &snapshot,
            &absent,
        )
        .expect("an absent volume explains the decline");
        assert!(recorded.still_holds(
            condition(
                DeclineReason::BuildVolumeAbsent,
                &request,
                &snapshot,
                &absent
            )
            .as_ref()
        ));
        let mut present = health();
        present.build_volume = Some(report(BuildVolumeState::Present { mount_point: None }));
        assert!(!recorded.still_holds(
            condition(
                DeclineReason::BuildVolumeAbsent,
                &request,
                &snapshot,
                &present
            )
            .as_ref()
        ));
    }

    fn existing_checkout(mut request: CellRequest) -> CellRequest {
        request.repository = RepositoryLocator::ExistingCheckout {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: "/repo".into(),
        };
        request
    }

    /// The 10-16 loop: a machine pressured on CPU posts a new utilization
    /// figure every sample. Those posts read the same condition, so the
    /// decline stands; the verdict clearing reads differently.
    #[test]
    fn a_cpu_pressure_decline_reads_the_verdict_not_the_utilization() {
        let snapshot = FleetSnapshot::default();
        let mut pressured = with_cpu(health(), CpuAdmissionState::Pressured);
        let recorded =
            condition(DeclineReason::Pressure, &request(), &snapshot, &pressured).unwrap();
        pressured.host.cpu_admission.utilization = Some(0.97);
        pressured.host.cpu_admission.measured_at_unix_ms = Some(123);
        assert_eq!(
            condition(DeclineReason::Pressure, &request(), &snapshot, &pressured),
            Some(recorded.clone())
        );
        let cleared = with_cpu(health(), CpuAdmissionState::Accepting);
        assert_ne!(
            condition(DeclineReason::Pressure, &request(), &snapshot, &cleared),
            Some(recorded)
        );
    }

    /// A pressure decline the posted state does not explain (memory the
    /// runner reads as fine, a CPU verdict that is not pressured) is tied to
    /// nothing, so any post may supersede it.
    #[test]
    fn an_unexplained_pressure_decline_has_no_condition() {
        let snapshot = FleetSnapshot::default();
        let mut fine = with_cpu(health(), CpuAdmissionState::Accepting);
        fine.machine.memory = Measurement::measured(
            1,
            MachineMemory {
                total_bytes: 64 * GIB,
                available_bytes: 32 * GIB,
            },
        );
        assert_eq!(
            condition(DeclineReason::Pressure, &request(), &snapshot, &fine),
            None
        );
        fine.machine.memory = Measurement::measured(
            2,
            MachineMemory {
                total_bytes: 64 * GIB,
                available_bytes: GIB,
            },
        );
        assert_eq!(
            condition(DeclineReason::Pressure, &request(), &snapshot, &fine),
            Some(PostedCondition::Pressure {
                cpu_pressured: false,
                memory_short: true
            })
        );
    }

    /// The reviewer's specimen: 20 GiB free, a 10 GiB watermark, 8 GiB held
    /// by other work, and a request that grows 4 GiB is refused. Releasing
    /// the other work's reservation changes nothing physical about the volume
    /// and is exactly the relief that makes asking again worth it.
    #[test]
    fn a_disk_decline_is_relieved_by_a_released_reservation() {
        let snapshot = FleetSnapshot::default();
        let request = growing(4 * GIB);
        let recorded = condition(
            DeclineReason::Disk,
            &request,
            &snapshot,
            &disk_at(20 * GIB, 8 * GIB),
        )
        .expect("2 GiB of headroom explains refusing 4 GiB of growth");
        let released = condition(
            DeclineReason::Disk,
            &request,
            &snapshot,
            &disk_at(20 * GIB, 0),
        );
        assert!(!recorded.still_holds(released.as_ref()));
    }

    /// Headroom that shrinks or holds is not relief, however the free figure
    /// jitters; headroom that grows across the request's own floor within a
    /// gibibyte is.
    #[test]
    fn a_disk_decline_holds_until_headroom_grows() {
        let snapshot = FleetSnapshot::default();
        let request = growing(GIB);
        let at = |free: u64| condition(DeclineReason::Disk, &request, &snapshot, &disk_at(free, 0));
        let recorded = at(10 * GIB + GIB / 2).unwrap();
        assert!(recorded.still_holds(at(10 * GIB + GIB / 2).as_ref()));
        assert!(recorded.still_holds(at(10 * GIB + GIB / 4).as_ref()));
        assert!(
            !recorded.still_holds(at(10 * GIB + GIB / 2 + 4096).as_ref()),
            "any growth may be relief"
        );
        assert_eq!(
            at(10 * GIB + GIB + 1),
            None,
            "past the floor nothing explains a refusal"
        );
        let mut sweeping = disk_at(10 * GIB + GIB / 2, 0);
        sweeping.disk.sweep_status = StorageSweepStatus::InFlight;
        assert!(!recorded
            .still_holds(condition(DeclineReason::Disk, &request, &snapshot, &sweeping).as_ref()));
    }

    /// A peer that posts no watermark cannot explain a disk refusal with one.
    #[test]
    fn a_disk_decline_without_a_posted_watermark_is_unexplained() {
        let mut health = disk_at(GIB, 0);
        health.disk.watermark_bytes = 0;
        assert_eq!(
            condition(
                DeclineReason::Disk,
                &growing(GIB),
                &FleetSnapshot::default(),
                &health
            ),
            None
        );
    }

    #[test]
    fn a_provisioning_decline_reads_the_hold_on_its_own_project() {
        let mut snapshot = FleetSnapshot::default();
        assert_eq!(
            condition(
                DeclineReason::ProjectProvisioning,
                &request(),
                &snapshot,
                &health()
            ),
            None
        );
        snapshot.provisioning_holds.push(ProvisioningHold {
            project_id: "other".into(),
            owner_request_id: "x".into(),
            ..ProvisioningHold::default()
        });
        snapshot.provisioning_holds.push(ProvisioningHold {
            project_id: "p".into(),
            owner_request_id: "holder".into(),
            entry_id: Some("holder-entry".into()),
            since_unix_ms: 7,
        });
        assert_eq!(
            condition(
                DeclineReason::ProjectProvisioning,
                &request(),
                &snapshot,
                &health()
            ),
            Some(PostedCondition::ProjectProvisioning {
                owner_request_id: "holder".into(),
                entry_id: Some("holder-entry".into()),
                since_unix_ms: 7,
            })
        );
    }

    #[test]
    fn a_capacity_decline_needs_a_posted_commitment_to_stand_on() {
        let snapshot = FleetSnapshot::default();
        assert_eq!(
            condition(DeclineReason::Capacity, &request(), &snapshot, &health()),
            None
        );
        let mut committed = health();
        committed.allocation.committed_concurrency_units = 4;
        assert!(matches!(
            condition(DeclineReason::Capacity, &request(), &snapshot, &committed),
            Some(PostedCondition::Capacity {
                committed_units: 4,
                ..
            })
        ));
        assert_eq!(
            condition(DeclineReason::Unknown, &request(), &snapshot, &committed),
            None
        );
    }

    /// A machine latched, provisioning the project for someone else, under
    /// its disk floor, and CPU-pressured at once is held by the latch, which
    /// the executor checks first; peeling each condition away reveals the
    /// next in the executor's order.
    #[test]
    fn a_hold_is_the_first_condition_in_the_executors_order() {
        let request = growing(GIB);
        let view = AdmissionView::of_request(&request);
        let snapshot = provisioning("holder", Some("holder-entry"));
        let mut machine = disk_at(10 * GIB + GIB / 2, 0);
        machine.host.cpu_admission = CpuAdmissionHealth {
            state: CpuAdmissionState::Pressured,
            measured_at_unix_ms: Some(NOW),
            ..CpuAdmissionHealth::default()
        };
        machine.disk.cleanup_blocked = true;
        machine.disk.cleanup_last_error = Some("permission denied".into());

        let latched = hold(&view, &request, &snapshot, &machine).unwrap();
        assert_eq!(latched.reason, DeclineReason::Disk);
        assert_eq!(
            latched.evidence,
            AdmissionHoldEvidence::CleanupLatched {
                error: Some("permission denied".into())
            }
        );
        assert!(!latched.relieves_itself, "a latch needs an operator");

        machine.disk.cleanup_blocked = false;
        let provisioning = hold(&view, &request, &snapshot, &machine).unwrap();
        assert_eq!(provisioning.reason, DeclineReason::ProjectProvisioning);
        assert!(provisioning.relieves_itself);
        assert_eq!(
            provisioning.evidence.describe_at(NOW),
            "held: project p provisioning for holder-entry for 61s"
        );

        let disk = hold(&view, &request, &FleetSnapshot::default(), &machine).unwrap();
        assert_eq!(disk.reason, DeclineReason::Disk);
        assert_eq!(
            disk.evidence,
            AdmissionHoldEvidence::Disk {
                free_bytes: 10 * GIB + GIB / 2,
                floor_bytes: 11 * GIB,
                sweep_in_flight: false,
            }
        );
        assert!(
            !disk.relieves_itself,
            "a disk with no sweep in flight is not going to free itself"
        );
        machine.disk.sweep_status = StorageSweepStatus::InFlight;
        assert!(
            hold(&view, &request, &FleetSnapshot::default(), &machine)
                .unwrap()
                .relieves_itself,
            "a sweep in flight may free the bytes"
        );

        machine.machine.volume = Measurement::measured(
            2,
            MachineVolume {
                total_bytes: 1024 * GIB,
                free_bytes: 20 * GIB,
            },
        );
        let pressure = hold(&view, &request, &FleetSnapshot::default(), &machine).unwrap();
        assert_eq!(pressure.reason, DeclineReason::Pressure);
        assert!(pressure.relieves_itself);
    }

    /// The executor skips host pressure for interactive work, and a CPU
    /// verdict past the blind bound is blindness rather than pressure.
    #[test]
    fn interactive_work_and_a_stale_verdict_are_not_held_on_cpu() {
        let request = request();
        let snapshot = FleetSnapshot::default();
        let mut machine = with_cpu(health(), CpuAdmissionState::Pressured);
        machine.host.cpu_admission.measured_at_unix_ms = Some(NOW);
        let batch = AdmissionView::of_request(&request);
        assert_eq!(
            hold(&batch, &request, &snapshot, &machine).map(|hold| hold.reason),
            Some(DeclineReason::Pressure)
        );
        let interactive = AdmissionView {
            interactive: true,
            ..batch.clone()
        };
        assert_eq!(hold(&interactive, &request, &snapshot, &machine), None);
        machine.host.cpu_admission.measured_at_unix_ms =
            Some(NOW - CPU_ADMISSION_BLIND_AFTER_MS - 1);
        assert_eq!(hold(&batch, &request, &snapshot, &machine), None);
    }

    /// Interactive work is checked against the bare watermark plus the growth
    /// it is charged, never the growth other admitted work holds; on a
    /// checkout it already has it is charged none. Both supersession and
    /// ranking read the same floor.
    #[test]
    fn interactive_disk_is_the_bare_watermark_plus_its_own_growth() {
        let request = growing(GIB);
        let snapshot = FleetSnapshot::default();
        let batch = AdmissionView::of_request(&request);
        let interactive = AdmissionView {
            interactive: true,
            ..batch.clone()
        };
        let busy = disk_at(11 * GIB + GIB / 2, 8 * GIB);
        assert!(hold(&batch, &request, &snapshot, &busy).is_some());
        assert_eq!(hold(&interactive, &request, &snapshot, &busy), None);
        assert_eq!(
            posted_condition(
                DeclineReason::Disk,
                &interactive,
                &request,
                colocated(&snapshot, &busy)
            ),
            None,
            "supersession reads the interactive floor too"
        );

        let tight = disk_at(10 * GIB + GIB / 2, 0);
        assert!(hold(&interactive, &request, &snapshot, &tight).is_some());
        let checkout = existing_checkout(growing(GIB));
        assert_eq!(
            hold(&interactive, &checkout, &snapshot, &tight),
            None,
            "a checkout it already has is charged no growth"
        );
    }

    /// A hold this request or this entry took is its own. Work that does not
    /// provision never waits on a hold at all.
    #[test]
    fn only_another_requests_hold_holds_work_that_provisions() {
        let request = request();
        let view = AdmissionView {
            entry_id: Some("mine".into()),
            ..AdmissionView::of_request(&request)
        };
        let machine = health();
        assert!(hold(&view, &request, &provisioning("holder", None), &machine).is_some());
        assert_eq!(
            hold(&view, &request, &provisioning("r", None), &machine),
            None,
            "its own request"
        );
        assert_eq!(
            hold(
                &view,
                &request,
                &provisioning("other", Some("mine")),
                &machine
            ),
            None,
            "its own entry's acquisition"
        );
        let checkout = existing_checkout(request.clone());
        assert_eq!(
            hold(&view, &checkout, &provisioning("holder", None), &machine),
            None,
            "an existing checkout provisions nothing"
        );
        assert!(
            posted_hold(
                &view,
                &checkout,
                PostedState {
                    snapshot: &provisioning("holder", None),
                    health: &machine,
                    colocated: false,
                },
                NOW
            )
            .is_some(),
            "a remote machine is offered managed objects, which provision"
        );
    }

    /// The two consumers cannot drift: whatever a hold reports is what a
    /// decline for its reason would record.
    #[test]
    fn a_hold_reports_the_condition_a_decline_would_record() {
        let request = growing(GIB);
        let view = AdmissionView::of_request(&request);
        let mut cpu = with_cpu(health(), CpuAdmissionState::Pressured);
        cpu.host.cpu_admission.measured_at_unix_ms = Some(NOW);
        let mut latched = disk_at(30 * GIB, 0);
        latched.disk.cleanup_blocked = true;
        let cases = [
            (FleetSnapshot::default(), latched),
            (provisioning("holder", Some("e")), health()),
            (FleetSnapshot::default(), disk_at(10 * GIB, 4 * GIB)),
            (FleetSnapshot::default(), cpu),
        ];
        for (snapshot, machine) in &cases {
            let hold = hold(&view, &request, snapshot, machine).expect("each case holds");
            assert_eq!(
                posted_condition(hold.reason, &view, &request, colocated(snapshot, machine)),
                Some(hold.condition),
                "{:?}",
                hold.evidence
            );
        }
    }
}
