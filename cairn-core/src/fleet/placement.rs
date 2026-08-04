//! Whether a placement failure is something waiting can fix.
//!
//! Placement fails for two unrelated kinds of reason, and a caller that could
//! wait needs to tell them apart before it decides what to do. A machine with no
//! room right now is a *slower* run: the queue is long, the concurrency units are
//! spoken for, the residency operations are contended. A machine that cannot run
//! this batch at all is a *broken* run: no executor advertises the platform it
//! needs, the executor is draining, the request is larger than admission will
//! ever accept.
//!
//! The distinction is load-bearing because the two demand opposite answers.
//! Refusing a capacity failure hands an agent a fact more tokens cannot act on,
//! and it responds rationally by retrying — feeding the very congestion that
//! refused it. Waiting on a structural failure strands the agent in a queue it
//! will never leave. So every failure kind is enumerated here, exhaustively, and
//! anything this module cannot classify is a refusal: a wrong refusal costs one
//! reissued call, a wrong wait costs the whole turn.

use cairn_common::executor_protocol::{
    AdmissionRejectionReason, CellOutcome, CellUnavailableReason, ExecutorSubstrateState,
    HostPressureCondition, HostPressureEvidence, ResidencyFailureKind,
};

/// What a caller that is able to wait should do with a placement failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlacementVerdict {
    /// The machine had no room at this moment. Nothing about the batch is wrong,
    /// so presenting it again later is the correct response and the only thing
    /// that changes is when it runs.
    Capacity,
    /// Something about this batch, this executor, or this machine means the work
    /// cannot be placed however long anyone waits. The caller must say so.
    Structural,
}

impl PlacementVerdict {
    pub(crate) fn is_capacity(self) -> bool {
        matches!(self, Self::Capacity)
    }
}

/// Whether the runner is in the middle of restoring the link a failure came from.
///
/// A lost link is the one failure whose meaning is not in the failure itself. The
/// same `ExecutorUnavailable` means "there is no machine" when nothing is coming,
/// and "the machine is restarting, ask again in a moment" when the supervisor is
/// already rebuilding it — opposite answers from an identical reason. So the fact
/// is supplied by the runner, which is the only party that knows it, and the
/// verdict stays here where every other verdict is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkRestoration {
    /// The supervisor is spawning, respawning, or attaching, and says so freshly
    /// and without a failure of its own. Positive evidence that the environment
    /// is coming back.
    Restoring,
    /// Nothing says a link is on its way back. This is the default and the safe
    /// reading: a stale declaration, a recovery that is itself failing, and a
    /// machine that was never there all land here.
    NotRestoring,
}

/// A placement failure with the verdict already taken, so the caller decides
/// what to do without re-reading a diagnostic sentence.
#[derive(Debug, Clone)]
pub(crate) struct PlacementRefusal {
    pub(crate) verdict: PlacementVerdict,
    /// The agent-facing sentence. Typed machinery an agent cannot act on stays
    /// in the log; this is what reaches the surface that asked.
    pub(crate) diagnostic: String,
}

impl std::fmt::Display for PlacementRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.diagnostic)
    }
}

impl PlacementRefusal {
    pub(crate) fn structural(diagnostic: impl Into<String>) -> Self {
        Self {
            verdict: PlacementVerdict::Structural,
            diagnostic: diagnostic.into(),
        }
    }
}

/// Whether an executor reporting this substrate state is working rather than
/// broken — busy, provisioning, adopting a cell, attaching its protocol.
///
/// This is the single answer to "does this state mean the machine will get to my
/// request?", and it has two callers by design. A coalesced subscriber pauses its
/// acquisition deadline while it holds, so a waiter is never charged for time the
/// executor spent working. [`classify_unavailable`] reads the same set to decide
/// whether an elapsed deadline was congestion or silence. They must not drift:
/// pausing a deadline for a state and then refusing the request that state
/// produced is the same contradiction seen twice.
///
/// `ExecutionRunning` is deliberately absent: it is progress on a request that
/// has already been admitted, which the caller handles elsewhere and which never
/// reaches placement classification. `Draining` and `ConnectedStalled` are absent
/// because neither is work — one is an executor refusing new work, the other is
/// an executor that stopped reporting at all.
pub(crate) fn substrate_is_working(state: ExecutorSubstrateState) -> bool {
    match state {
        ExecutorSubstrateState::SupervisorSpawning
        | ExecutorSubstrateState::SupervisorRespawning
        | ExecutorSubstrateState::ProtocolAttaching
        | ExecutorSubstrateState::InitialStorageSweep
        | ExecutorSubstrateState::StorageAccounting
        | ExecutorSubstrateState::DispatchPreparing
        | ExecutorSubstrateState::SlotAdoption
        | ExecutorSubstrateState::CapacityBusy => true,
        ExecutorSubstrateState::ExecutionRunning
        | ExecutorSubstrateState::ConnectedStalled
        | ExecutorSubstrateState::Draining => false,
    }
}

/// Whether a host-pressure hold is the kind that ends by itself.
///
/// Memory frees when the work using it finishes, and resident occupancy falls
/// when the processes holding it exit — those are the machine being busy. A disk
/// below its floor is not: nothing running is going to give those bytes back, so
/// it needs a person, and queueing on it would park an agent on an operator
/// task. This is the same split the executor makes when it decides whether a
/// pressure hold pauses a queued request's deadline, and it must stay the same
/// split: pausing a deadline for a condition and then refusing the request that
/// condition produced is one contradiction seen twice.
///
/// Evidence naming no condition at all decides nothing, so it is not a wait:
/// this module refuses whatever it cannot classify, and "pressure held me, and I
/// cannot say which pressure" is exactly that.
///
/// Shared with check composition
/// ([`crate::execution::checks`]), which must reach the same split when it
/// decides whether an elapsed deadline is worth asking about again. Two copies
/// of this rule would be the same contradiction seen a third time.
pub(crate) fn pressure_relieves_itself(evidence: &HostPressureEvidence) -> bool {
    !evidence.conditions.is_empty()
        && evidence.conditions.iter().all(|condition| match condition {
            HostPressureCondition::MemoryAvailable { .. }
            | HostPressureCondition::ResidentOccupancy { .. }
            | HostPressureCondition::CpuUtilization { .. } => true,
            HostPressureCondition::DiskFree { .. } => false,
        })
}

/// Classify why a cell could not be placed.
///
/// The match is exhaustive on purpose: a new [`CellUnavailableReason`] must not
/// silently inherit either answer, because inheriting `Capacity` would park an
/// agent forever on a condition nobody is going to clear.
pub(crate) fn classify_unavailable(
    reason: &CellUnavailableReason,
    link: LinkRestoration,
) -> PlacementVerdict {
    match reason {
        // A deadline elapsed while the request sat in a queue. Whether that is
        // congestion or silence is exactly what the executor's own substrate
        // evidence says, so read it rather than guessing from the shape of the
        // failure: `CapacityBusy` with a queue position is a machine doing its
        // job, `ConnectedStalled` (or no evidence at all) is one that stopped
        // answering, and waiting on the latter is waiting on nothing.
        CellUnavailableReason::Deadline {
            host_pressure,
            substrate,
        } => {
            let busy = substrate
                .as_ref()
                .is_some_and(|evidence| substrate_is_working(evidence.state));
            let pressured = host_pressure.as_ref().is_some_and(pressure_relieves_itself);
            if busy || pressured {
                PlacementVerdict::Capacity
            } else {
                PlacementVerdict::Structural
            }
        }
        CellUnavailableReason::AdmissionRejected { reason } => match reason {
            // The queue is full, which is the most literal statement of "no room
            // right now" the executor can make.
            AdmissionRejectionReason::QueueFull => PlacementVerdict::Capacity,
            // A request too large for admission is the same size next hour.
            AdmissionRejectionReason::RequestTooLarge => PlacementVerdict::Structural,
            // Storage cleanup failing is an operator-visible fault, not load.
            AdmissionRejectionReason::StorageCleanupFailed => PlacementVerdict::Structural,
            // A draining executor is not busy — it is refusing new work on
            // purpose, usually on its way out. Queueing behind that is queueing
            // behind a shutdown.
            AdmissionRejectionReason::Draining => PlacementVerdict::Structural,
        },
        // A slot that could not be made fit for the batch, and was retired for
        // it. Nothing about the batch is wrong and nothing of it ran, so this is
        // capacity in the most literal sense the word has here: the machine had
        // no usable room at this moment. What makes the retry meaningful rather
        // than a loop is the retirement — the slot that refused the work is out
        // of the pool, so re-presenting it takes a fresh slot, and a batch free
        // to move can take a different machine entirely. A machine that cannot
        // produce a healthy slot at all fails the next attempt in provisioning,
        // which is structural and refuses there.
        CellUnavailableReason::SlotUnhealthy => PlacementVerdict::Capacity,
        // Everything below is a fault in provisioning, in the request, or in the
        // fleet's ability to serve it. None of them are relieved by time.
        CellUnavailableReason::Provisioning
        | CellUnavailableReason::Checkout
        | CellUnavailableReason::Spawn
        | CellUnavailableReason::Preparation
        | CellUnavailableReason::ObjectInfrastructure(_) => PlacementVerdict::Structural,
        // No executor is connected. Usually that is a machine that is not there,
        // which is actionable and must be reported — but it is also what a link
        // bounce looks like from here, and a bounce is a few seconds of an
        // environment being rebuilt. Refusing a batch for that is the refusal
        // CAIRN-3258 set out to delete, and long waits make it reachable rather
        // than theoretical: a batch that waits an hour for capacity crosses far
        // more supervisor restarts than one that waited twenty seconds.
        //
        // Only positive evidence tips it. The runner supplies whether it is
        // actively restoring this link, and everything else — no evidence, stale
        // evidence, a recovery that is itself failing — keeps the refusal.
        CellUnavailableReason::ExecutorUnavailable => match link {
            LinkRestoration::Restoring => PlacementVerdict::Capacity,
            LinkRestoration::NotRestoring => PlacementVerdict::Structural,
        },
        // No executor matches the request's selector: a machine name, platform,
        // or toolchain nothing in the fleet provides. Time does not add one, and
        // no link coming back changes what an executor will advertise.
        CellUnavailableReason::NoMatchingExecutor => PlacementVerdict::Structural,
    }
}

/// Classify a cell outcome that is not a completion.
///
/// Only `Unavailable` can ever be capacity-shaped: every other terminal outcome
/// means the batch reached an executor, so re-presenting it would re-run work
/// that already ran.
pub(crate) fn classify_cell_outcome(
    outcome: &CellOutcome,
    link: LinkRestoration,
) -> PlacementVerdict {
    match outcome {
        CellOutcome::Unavailable { reason, .. } => classify_unavailable(reason, link),
        CellOutcome::Completed { .. }
        | CellOutcome::FailedAfterExecution { .. }
        | CellOutcome::StorageFailure { .. }
        | CellOutcome::Cancelled { .. } => PlacementVerdict::Structural,
    }
}

/// Classify a failed residency operation.
///
/// Acquiring an execution environment IS a cell placement, so a residency
/// failure is capacity-shaped only when both halves of it say so: the kind must
/// be the one that means "this never got in", and the placement evidence it
/// carries must say the machine was working. Either half alone is a refusal —
/// a kind that names a declaration fault is not made waitable by whatever the
/// queue happened to look like, and admission with no evidence behind it never
/// reached a queue at all.
pub(crate) fn classify_residency_failure(
    kind: &ResidencyFailureKind,
    cell_outcome: Option<&CellOutcome>,
    link: LinkRestoration,
) -> PlacementVerdict {
    let admission = match kind {
        ResidencyFailureKind::Admission => true,
        ResidencyFailureKind::InvalidDeclaration
        | ResidencyFailureKind::ConflictingDeclaration
        | ResidencyFailureKind::NotFound
        | ResidencyFailureKind::Unavailable
        | ResidencyFailureKind::StaleEpoch
        | ResidencyFailureKind::InvalidState
        | ResidencyFailureKind::Process
        | ResidencyFailureKind::Cleanup
        | ResidencyFailureKind::Persistence => false,
    };
    match (admission, cell_outcome) {
        (true, Some(outcome)) => classify_cell_outcome(outcome, link),
        _ => PlacementVerdict::Structural,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        ExecutorSubstrateEvidence, ObjectInfrastructureStage, StorageFailureKind,
        StorageFailureStage,
    };

    fn evidence(state: ExecutorSubstrateState) -> ExecutorSubstrateEvidence {
        ExecutorSubstrateEvidence::without_queue(state, 0, 0)
    }

    fn deadline(substrate: Option<ExecutorSubstrateState>) -> CellUnavailableReason {
        CellUnavailableReason::Deadline {
            host_pressure: None,
            substrate: substrate.map(evidence),
        }
    }

    /// Most verdicts do not depend on a link being rebuilt, so they are taken
    /// against the reading that refuses.
    fn settled(reason: &CellUnavailableReason) -> PlacementVerdict {
        classify_unavailable(reason, LinkRestoration::NotRestoring)
    }

    /// The whole point of the classifier, stated as the two cases it exists to
    /// separate: a queue that is long is a wait, and an executor that stopped
    /// answering is not.
    #[test]
    fn an_elapsed_deadline_is_a_wait_only_while_the_machine_is_working() {
        assert_eq!(
            settled(&deadline(Some(ExecutorSubstrateState::CapacityBusy))),
            PlacementVerdict::Capacity
        );
        assert_eq!(
            settled(&deadline(Some(ExecutorSubstrateState::SlotAdoption))),
            PlacementVerdict::Capacity
        );
        assert_eq!(
            settled(&deadline(Some(ExecutorSubstrateState::ConnectedStalled))),
            PlacementVerdict::Structural
        );
        assert_eq!(
            settled(&deadline(Some(ExecutorSubstrateState::Draining))),
            PlacementVerdict::Structural
        );
        // No evidence is not evidence of congestion.
        assert_eq!(settled(&deadline(None)), PlacementVerdict::Structural);
    }

    /// Every reason an executor can give, decided one at a time. The list is
    /// built from an exhaustive match so a new variant cannot be added without
    /// landing here, which is the only thing standing between a novel failure
    /// and an agent parked on it forever.
    #[test]
    fn every_unavailable_reason_has_a_deliberate_verdict() {
        let sample = deadline(None);
        match &sample {
            CellUnavailableReason::Deadline { .. }
            | CellUnavailableReason::Provisioning
            | CellUnavailableReason::Checkout
            | CellUnavailableReason::Spawn
            | CellUnavailableReason::Preparation
            | CellUnavailableReason::SlotUnhealthy
            | CellUnavailableReason::ExecutorUnavailable
            | CellUnavailableReason::NoMatchingExecutor
            | CellUnavailableReason::AdmissionRejected { .. }
            | CellUnavailableReason::ObjectInfrastructure(_) => {}
        }
        let waits = [
            deadline(Some(ExecutorSubstrateState::CapacityBusy)),
            // A retired slot is the one non-deadline wait: the thing that
            // refused the work is gone, so the next attempt meets a different
            // one. Sitting next to `Preparation` in the refusals below is the
            // whole point — the two describe the same moment in a batch's life
            // and call for opposite answers.
            CellUnavailableReason::SlotUnhealthy,
        ];
        let refusals = [
            deadline(Some(ExecutorSubstrateState::ConnectedStalled)),
            CellUnavailableReason::Provisioning,
            CellUnavailableReason::Checkout,
            CellUnavailableReason::Spawn,
            CellUnavailableReason::Preparation,
            CellUnavailableReason::ExecutorUnavailable,
            CellUnavailableReason::NoMatchingExecutor,
            CellUnavailableReason::ObjectInfrastructure(
                ObjectInfrastructureStage::FetchInterrupted,
            ),
        ];
        for reason in waits {
            assert!(
                settled(&reason).is_capacity(),
                "expected a wait: {reason:?}"
            );
        }
        for reason in refusals {
            assert!(
                !settled(&reason).is_capacity(),
                "expected a refusal: {reason:?}"
            );
        }
    }

    /// A lost link is the one failure whose verdict depends on something outside
    /// it, so both readings are asserted from the same reason.
    ///
    /// A bounce is a few seconds of an environment being rebuilt, and a batch
    /// that waits an hour for capacity crosses far more of them than one that
    /// waited twenty seconds — which is why the refusal that was theoretical
    /// before a real wait horizon is reachable after it. The default stays
    /// refusal: only the runner positively saying "I am rebuilding this" turns it
    /// into a wait, because an agent parked on a machine that is never coming
    /// back is the worse of the two mistakes.
    #[test]
    fn a_lost_link_is_a_wait_only_while_the_runner_is_restoring_it() {
        assert_eq!(
            classify_unavailable(
                &CellUnavailableReason::ExecutorUnavailable,
                LinkRestoration::Restoring
            ),
            PlacementVerdict::Capacity
        );
        assert_eq!(
            classify_unavailable(
                &CellUnavailableReason::ExecutorUnavailable,
                LinkRestoration::NotRestoring
            ),
            PlacementVerdict::Structural
        );
        // A fleet that advertises nothing matching the request is not waiting for
        // a link: no restart changes what an executor advertises.
        assert_eq!(
            classify_unavailable(
                &CellUnavailableReason::NoMatchingExecutor,
                LinkRestoration::Restoring
            ),
            PlacementVerdict::Structural
        );
        // And a restoring link does not launder a verdict taken for another
        // reason entirely.
        assert_eq!(
            classify_unavailable(
                &CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::RequestTooLarge
                },
                LinkRestoration::Restoring
            ),
            PlacementVerdict::Structural
        );
    }

    /// A machine short of memory is busy; a machine short of disk needs a
    /// person. Waiting out the second would park an agent on an operator task.
    #[test]
    fn host_pressure_is_a_wait_only_when_it_ends_by_itself() {
        let held = |conditions| CellUnavailableReason::Deadline {
            host_pressure: Some(HostPressureEvidence { conditions }),
            substrate: None,
        };
        assert!(settled(&held(vec![HostPressureCondition::MemoryAvailable {
            available_bytes: 1,
            floor_bytes: 2
        }]))
        .is_capacity());
        assert!(!settled(&held(vec![HostPressureCondition::DiskFree {
            free_bytes: 1,
            floor_bytes: 2
        }]))
        .is_capacity());
        // A hold that names no condition explains nothing, and this module
        // refuses what it cannot classify rather than waiting on it.
        assert!(!settled(&held(Vec::new())).is_capacity());
        // One unrelievable condition is enough: the hold outlasts the busy ones.
        assert!(!settled(&held(vec![
            HostPressureCondition::MemoryAvailable {
                available_bytes: 1,
                floor_bytes: 2
            },
            HostPressureCondition::DiskFree {
                free_bytes: 1,
                floor_bytes: 2
            },
        ]))
        .is_capacity());
    }

    /// Admission rejections split: a full queue drains, and the other three do
    /// not become acceptable by being asked again.
    #[test]
    fn only_a_full_queue_is_worth_waiting_out() {
        let rejected = |reason| CellUnavailableReason::AdmissionRejected { reason };
        match AdmissionRejectionReason::QueueFull {
            AdmissionRejectionReason::QueueFull
            | AdmissionRejectionReason::RequestTooLarge
            | AdmissionRejectionReason::StorageCleanupFailed
            | AdmissionRejectionReason::Draining => {}
        }
        assert!(settled(&rejected(AdmissionRejectionReason::QueueFull)).is_capacity());
        for reason in [
            AdmissionRejectionReason::RequestTooLarge,
            AdmissionRejectionReason::StorageCleanupFailed,
            AdmissionRejectionReason::Draining,
        ] {
            assert!(
                !settled(&rejected(reason.clone())).is_capacity(),
                "expected a refusal: {reason:?}"
            );
        }
    }

    /// An outcome that reached an executor is never re-presented: re-running a
    /// batch that already ran is worse than any refusal. A completion is covered
    /// by the exhaustive match in [`classify_cell_outcome`] itself, which is
    /// what forces a new variant to be decided rather than defaulted.
    #[test]
    fn an_outcome_that_reached_an_executor_is_never_a_wait() {
        let executed = [
            CellOutcome::FailedAfterExecution {
                request_id: "r".into(),
                attempt_id: "a".into(),
                diagnostic: String::new(),
            },
            CellOutcome::StorageFailure {
                request_id: "r".into(),
                attempt_id: "a".into(),
                stage: StorageFailureStage::DeltaUpload,
                kind: StorageFailureKind::NoSpace,
                diagnostic: String::new(),
                slot_retired: false,
            },
            CellOutcome::Cancelled {
                request_id: "r".into(),
                attempt_id: "a".into(),
            },
        ];
        for outcome in executed {
            assert!(!classify_cell_outcome(&outcome, LinkRestoration::NotRestoring).is_capacity());
        }
    }

    /// A residency failure is classified by the placement evidence it carries,
    /// so the environment path and the batch path answer the same question the
    /// same way. A kind with no evidence behind it is a refusal.
    #[test]
    fn a_residency_failure_follows_the_placement_evidence_it_carries() {
        let busy = CellOutcome::Unavailable {
            reason: deadline(Some(ExecutorSubstrateState::CapacityBusy)),
            diagnostic: "residency acquisition deadline elapsed".into(),
        };
        assert!(classify_residency_failure(
            &ResidencyFailureKind::Admission,
            Some(&busy),
            LinkRestoration::NotRestoring,
        )
        .is_capacity());
        assert!(!classify_residency_failure(
            &ResidencyFailureKind::Admission,
            None,
            LinkRestoration::NotRestoring
        )
        .is_capacity());
        // A declaration fault is not made waitable by a busy queue behind it.
        assert!(!classify_residency_failure(
            &ResidencyFailureKind::InvalidDeclaration,
            Some(&busy),
            LinkRestoration::NotRestoring,
        )
        .is_capacity());
    }
}
