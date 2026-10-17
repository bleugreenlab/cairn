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

use cairn_common::executor_protocol::{
    CellPriority, CellRequest, CpuAdmissionState, DeclineReason, DiskHealthStatus,
    ExecutorSubstrateReport, FleetSnapshot, RepositoryLocator, StoragePressureOutcome,
    ADMISSION_MEMORY_FLOOR_BYTES,
};

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
    /// it, and its posted headroom: free space less the admission watermark
    /// and the disk growth its admitted work has reserved. Admission refuses
    /// when headroom is short of what the request would grow, so headroom
    /// growing by any amount (a reservation released, a sweep that freed
    /// bytes) is relief, while headroom shrinking or holding is not.
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
    /// The request holding this project's provisioning on the machine.
    ProjectProvisioning { owner_request_id: String },
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
    request: &CellRequest,
    snapshot: &FleetSnapshot,
    health: &ExecutorSubstrateReport,
) -> Option<PostedCondition> {
    match reason {
        DeclineReason::Pressure => {
            let cpu_pressured = health.host.cpu_admission.state == CpuAdmissionState::Pressured;
            let floor = ADMISSION_MEMORY_FLOOR_BYTES.max(request.resource_reservation.memory_bytes);
            let memory_short = health
                .machine
                .memory
                .value()
                .is_some_and(|memory| memory.available_bytes < floor);
            (cpu_pressured || memory_short).then_some(PostedCondition::Pressure {
                cpu_pressured,
                memory_short,
            })
        }
        DeclineReason::Disk => {
            let disk = &health.disk;
            // The executor's own predicate, from what it posts: free space
            // against its watermark plus the growth admitted work holds plus
            // what this request would grow. Interactive work on a checkout it
            // already has is charged no growth. The executor also charges
            // residency footprints it does not post, so this can read a
            // refusal as unexplained; that falls back to any post superseding
            // it, which is the safe direction.
            let requested = if request.priority == CellPriority::AgentInteractive
                && matches!(
                    request.repository,
                    RepositoryLocator::ExistingCheckout { .. }
                ) {
                0
            } else {
                request.resource_reservation.disk_growth_bytes
            };
            let headroom = health.machine.volume.value().map(|volume| {
                i128::from(volume.free_bytes)
                    - i128::from(disk.watermark_bytes)
                    - i128::from(health.admission.active_reservation.disk_growth_bytes)
            });
            let short = disk.watermark_bytes > 0
                && headroom.is_some_and(|headroom| headroom < i128::from(requested));
            (disk.cleanup_blocked || short).then(|| PostedCondition::Disk {
                status: disk.status,
                sweep_in_flight: disk.sweep_status
                    == cairn_common::executor_protocol::StorageSweepStatus::InFlight,
                cleanup_blocked: disk.cleanup_blocked,
                last_pass: disk.pressure.as_ref().map(|pass| pass.outcome),
                headroom_bytes: headroom.unwrap_or(i128::MIN),
            })
        }
        DeclineReason::Capacity => {
            let allocation = &health.allocation;
            // An executor that posts no commitment cannot be explaining a
            // capacity refusal with it.
            (allocation.committed_concurrency_units > 0 || allocation.committed_memory_bytes > 0)
                .then_some(PostedCondition::Capacity {
                    committed_units: allocation.committed_concurrency_units,
                    committed_memory_bytes: allocation.committed_memory_bytes,
                    capacity_units: health.admission.concurrency_capacity,
                    capacity_memory_bytes: health.admission.memory_capacity_bytes,
                })
        }
        DeclineReason::ProjectProvisioning => snapshot
            .provisioning_holds
            .iter()
            .find(|hold| hold.project_id == request.project_id)
            .map(|hold| PostedCondition::ProjectProvisioning {
                owner_request_id: hold.owner_request_id.clone(),
            }),
        DeclineReason::Constraint | DeclineReason::Unknown => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        CpuAdmissionHealth, MachineMemory, MachineVolume, Measurement, ProvisioningHold,
    };

    fn request() -> CellRequest {
        crate::fleet::queue::tests::request(
            "r",
            cairn_common::executor_protocol::PlacementMobility::SpillEligible,
        )
    }

    fn health() -> ExecutorSubstrateReport {
        ExecutorSubstrateReport::default()
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

    /// The 10-16 loop: a machine pressured on CPU posts a new utilization
    /// figure every sample. Those posts read the same condition, so the
    /// decline stands; the verdict clearing reads differently.
    #[test]
    fn a_cpu_pressure_decline_reads_the_verdict_not_the_utilization() {
        let snapshot = FleetSnapshot::default();
        let mut pressured = with_cpu(health(), CpuAdmissionState::Pressured);
        let recorded =
            posted_condition(DeclineReason::Pressure, &request(), &snapshot, &pressured).unwrap();
        pressured.host.cpu_admission.utilization = Some(0.97);
        pressured.host.cpu_admission.measured_at_unix_ms = Some(123);
        assert_eq!(
            posted_condition(DeclineReason::Pressure, &request(), &snapshot, &pressured),
            Some(recorded.clone())
        );
        let cleared = with_cpu(health(), CpuAdmissionState::Accepting);
        assert_ne!(
            posted_condition(DeclineReason::Pressure, &request(), &snapshot, &cleared),
            Some(recorded)
        );
    }

    /// A pressure decline the posted state does not explain (memory the
    /// runner reads as fine, a CPU verdict that is not pressured) is tied to
    /// nothing, so any post may supersede it.
    #[test]
    fn an_unexplained_pressure_decline_has_no_condition() {
        let mut fine = with_cpu(health(), CpuAdmissionState::Accepting);
        fine.machine.memory = Measurement::measured(
            1,
            MachineMemory {
                total_bytes: 64 * GIB,
                available_bytes: 32 * GIB,
            },
        );
        assert_eq!(
            posted_condition(
                DeclineReason::Pressure,
                &request(),
                &FleetSnapshot::default(),
                &fine
            ),
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
            posted_condition(
                DeclineReason::Pressure,
                &request(),
                &FleetSnapshot::default(),
                &fine
            ),
            Some(PostedCondition::Pressure {
                cpu_pressured: false,
                memory_short: true
            })
        );
    }

    const GIB: u64 = 1024 * 1024 * 1024;

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

    /// The reviewer's specimen: 20 GiB free, a 10 GiB watermark, 8 GiB held
    /// by other work, and a request that grows 4 GiB is refused. Releasing
    /// the other work's reservation changes nothing physical about the volume
    /// and is exactly the relief that makes asking again worth it.
    #[test]
    fn a_disk_decline_is_relieved_by_a_released_reservation() {
        let snapshot = FleetSnapshot::default();
        let request = growing(4 * GIB);
        let recorded = posted_condition(
            DeclineReason::Disk,
            &request,
            &snapshot,
            &disk_at(20 * GIB, 8 * GIB),
        )
        .expect("2 GiB of headroom explains refusing 4 GiB of growth");
        let released = posted_condition(
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
        let at = |free: u64| {
            posted_condition(DeclineReason::Disk, &request, &snapshot, &disk_at(free, 0))
        };
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
        sweeping.disk.sweep_status = cairn_common::executor_protocol::StorageSweepStatus::InFlight;
        assert!(!recorded.still_holds(
            posted_condition(DeclineReason::Disk, &request, &snapshot, &sweeping).as_ref()
        ));
    }

    /// A peer that posts no watermark cannot explain a disk refusal with one.
    #[test]
    fn a_disk_decline_without_a_posted_watermark_is_unexplained() {
        let mut health = disk_at(GIB, 0);
        health.disk.watermark_bytes = 0;
        assert_eq!(
            posted_condition(
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
            posted_condition(
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
            ..ProvisioningHold::default()
        });
        assert_eq!(
            posted_condition(
                DeclineReason::ProjectProvisioning,
                &request(),
                &snapshot,
                &health()
            ),
            Some(PostedCondition::ProjectProvisioning {
                owner_request_id: "holder".into()
            })
        );
    }

    #[test]
    fn a_capacity_decline_needs_a_posted_commitment_to_stand_on() {
        let snapshot = FleetSnapshot::default();
        assert_eq!(
            posted_condition(DeclineReason::Capacity, &request(), &snapshot, &health()),
            None
        );
        let mut committed = health();
        committed.allocation.committed_concurrency_units = 4;
        assert!(matches!(
            posted_condition(DeclineReason::Capacity, &request(), &snapshot, &committed),
            Some(PostedCondition::Capacity {
                committed_units: 4,
                ..
            })
        ));
        assert_eq!(
            posted_condition(DeclineReason::Unknown, &request(), &snapshot, &committed),
            None
        );
    }
}
