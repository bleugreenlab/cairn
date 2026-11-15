use super::*;

pub(super) fn substrate_failure_kind(failure: &SubstrateFailure) -> CheckFailureKind {
    // A preempted check is re-armed like one that found no room: the machine
    // was taken for a model, which says nothing against the infrastructure.
    if matches!(
        failure.shape(),
        SubstrateFailureShape::Capacity | SubstrateFailureShape::CommandPreempted
    ) {
        CheckFailureKind::Capacity
    } else {
        CheckFailureKind::Infrastructure
    }
}

/// What Cairn's own machinery failed to do, in the only terms an agent can act
/// on. Every arm answers one question — why is there no verdict? — without
/// naming a slot, a cell, a scratch path, or an executor outcome variant. Those
/// coordinates are real and they matter, but they address a substrate the agent
/// has no standing inside: the operator log is where they belong.
/// The no-start conditions an agent can meet, kept apart because they call for
/// different responses. "It did not start" is not one condition: a machine that
/// is momentarily full will take the same check in a minute, a fleet with no
/// machine for its toolchain never will, and work whose own environment was torn
/// down is not a fault at all. Collapsing them into one lead sentence forced
/// every reader to the operator log to learn which had happened (CAIRN-3345).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubstrateFailureShape {
    /// Cairn never got the check started, for a reason that is Cairn's own.
    Dispatch,
    /// Execution capacity never became available before the deadline. The one
    /// condition time alone relieves, and so the only one worth retrying.
    Capacity,
    /// Contact with the machine that claimed the check was lost. Only said
    /// when a machine claimed it: before a claim there was no contact to lose.
    MachineUnreachable,
    /// The fleet had no reachable machine to hand the check to. What an
    /// executor-unavailable refusal means before any machine claimed the work.
    NoReachableMachine,
    /// The machine is being taken out of service and accepts no new work.
    Draining,
    /// Nothing in the fleet matches what this check requires.
    NoMachine,
    /// A working environment for the check could not be prepared.
    Preparation,
    /// The working environment the check was given could not be made fit for
    /// it, and was taken out of service for that. Distinct from `Preparation`
    /// because the next attempt gets a different environment rather than the
    /// same broken one.
    EnvironmentRetired,
    /// The command ran, but Cairn could not obtain or record its result.
    Result,
    /// The check was handed to a machine that then stopped accounting for it,
    /// so whether any of it ran is not known. Distinct from `Result` and
    /// `Dispatch` because neither "it ran" nor "it never started" can be
    /// backed: the runner sees the machine's account only between polls.
    OutcomeUnknown,
    /// Cairn's own storage for the check failed.
    Storage,
    /// Cairn's runner lost track of the check's result between the execution
    /// and the requester. Never a machine or a link: every path that ends a
    /// wait is meant to answer it, so reaching this is a bug in Cairn.
    RunnerFault,
    /// The check ran, then its process tree stopped making progress — no
    /// output and no meaningful CPU for the cadence's silence bound — and its
    /// executor stopped it. Distinct from `Result` because the outcome is
    /// known (Cairn ended the command), and from a red because nothing about
    /// the tree was decided: a wedged test says where to look, not whether
    /// the change is good.
    CommandStalled,
    /// The check was running on a machine an inference deployment is loading
    /// onto, its measured memory was in the deployment's way once the drain's
    /// bound passed, and Cairn stopped it to make room. A named non-verdict
    /// beside `CommandStalled`: nothing about the tree was decided, and the
    /// check is restartable, so it is presented again at once and placement
    /// keeps it off the machine the deployment holds.
    CommandPreempted,
    /// The kernel's out-of-memory killer killed part of the check's process
    /// tree while it ran. Whatever exit the command chose afterwards is the
    /// consequence of that, so this is the machine's capacity rather than a
    /// verdict on the code (cairn/5135).
    CommandOomKilled,
    /// The machine keeps its cells on a volume that is not mounted, so it took
    /// no work that needs one. Like a drain, a fact about the fleet's shape
    /// right now and not about this head or this machine's health.
    BuildVolumeAbsent,
    /// The volume holding the check's cell went away while the check ran.
    /// Whatever the command reported was read from a tree that vanished, so
    /// it is not a verdict; the machine is the one to avoid until the volume
    /// is back.
    BuildVolumeLost,
    /// Placement found machines that match the check but could not read their
    /// current load: a reading it decides on was stale or missing, so it
    /// refused them. Kept apart from `NoMachine` because the two call for
    /// opposite responses: a fleet with nothing that matches stays that way
    /// until the fleet changes, while a blind machine is usually back within
    /// minutes on its own.
    MeasurementBlind,
}

impl SubstrateFailureShape {
    pub(super) fn lead(self) -> &'static str {
        match self {
            Self::Dispatch => "Cairn could not start this check.",
            Self::Capacity => {
                "Cairn could not obtain the capacity to run this check before its deadline."
            }
            Self::MachineUnreachable => "Cairn lost contact with the machine running this check.",
            Self::NoReachableMachine => "Cairn had no reachable machine to hand this check to.",
            Self::Draining => {
                "The machine that runs checks is being taken out of service, so this check did not start."
            }
            Self::NoMachine => {
                "No machine available to Cairn can run this check's platform or toolchain."
            }
            Self::Preparation => {
                "Cairn could not prepare a working environment for this check."
            }
            Self::EnvironmentRetired => {
                "The working environment this check was given could not be made ready, so Cairn took it out of service."
            }
            Self::Result => "This check ran, but Cairn could not record its result.",
            Self::OutcomeUnknown => {
                "Cairn lost track of this check on the machine it was handed to, so whether it ran is unknown."
            }
            Self::Storage => "Cairn's own storage for this check failed.",
            Self::RunnerFault => {
                "Cairn's runner lost track of this check's result, which is a bug in Cairn rather than a machine failure."
            }
            Self::CommandStalled => {
                "This check stopped making progress, so Cairn stopped it before it produced a verdict."
            }
            Self::CommandPreempted => {
                "Cairn stopped this check before it produced a verdict to make room for an inference model loading on its machine. It is run again elsewhere; this is not a verdict on the code."
            }
            Self::CommandOomKilled => {
                "The machine ran out of memory while this check ran, and the kernel killed part of it. That is the machine's capacity, not a verdict on the code."
            }
            Self::BuildVolumeAbsent => {
                "The machine that would run this check keeps its cells on a volume that is not mounted, so this check did not start there."
            }
            Self::BuildVolumeLost => {
                "The volume holding this check's working tree went away while it ran, so whatever it reported is not a verdict on the code."
            }
            Self::MeasurementBlind => {
                "The machines that can run this check were refusing new work because Cairn could not read their current load, so this check did not start. That clears on its own when they report again."
            }
        }
    }

    /// The token this shape is stored under on a failing observation
    /// (`check_result_observations.substrate_shape`), so a reader of the record
    /// learns what the runner concluded without reading its prose.
    ///
    /// Spelled out per variant rather than derived from the variant's name: rows
    /// already on disk keep their meaning through a rename, and a new shape
    /// cannot compile until it is given a token here and in [`Self::from_stored`].
    pub(crate) fn stored(self) -> &'static str {
        match self {
            Self::Dispatch => "dispatch",
            Self::Capacity => "capacity",
            Self::MachineUnreachable => "machine_unreachable",
            Self::NoReachableMachine => "no_reachable_machine",
            Self::Draining => "draining",
            Self::NoMachine => "no_machine",
            Self::Preparation => "preparation",
            Self::EnvironmentRetired => "environment_retired",
            Self::Result => "result",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::Storage => "storage",
            Self::RunnerFault => "runner_fault",
            Self::CommandStalled => "command_stalled",
            Self::CommandPreempted => "command_preempted",
            Self::CommandOomKilled => "command_oom_killed",
            Self::BuildVolumeAbsent => "build_volume_absent",
            Self::BuildVolumeLost => "build_volume_lost",
            Self::MeasurementBlind => "measurement_blind",
        }
    }

    /// The shape a stored token names; `None` for a token this build does not
    /// know, which only a newer build could have written.
    pub(crate) fn from_stored(token: &str) -> Option<Self> {
        Some(match token {
            "dispatch" => Self::Dispatch,
            "capacity" => Self::Capacity,
            "machine_unreachable" => Self::MachineUnreachable,
            "no_reachable_machine" => Self::NoReachableMachine,
            "draining" => Self::Draining,
            "no_machine" => Self::NoMachine,
            "preparation" => Self::Preparation,
            "environment_retired" => Self::EnvironmentRetired,
            "result" => Self::Result,
            "outcome_unknown" => Self::OutcomeUnknown,
            "storage" => Self::Storage,
            "runner_fault" => Self::RunnerFault,
            "command_stalled" => Self::CommandStalled,
            "command_preempted" => Self::CommandPreempted,
            "command_oom_killed" => Self::CommandOomKilled,
            "build_volume_absent" => Self::BuildVolumeAbsent,
            "build_volume_lost" => Self::BuildVolumeLost,
            "measurement_blind" => Self::MeasurementBlind,
            _ => return None,
        })
    }

    /// Whether presenting this work again could plausibly change the answer.
    ///
    /// Two conditions qualify, and both because something outside the check
    /// changes between attempts. Capacity is relieved by time passing. A retired
    /// environment is relieved by the retirement itself: the environment that
    /// could not take the check is gone, so the next attempt is handed a
    /// different one. A structural refusal (no matching machine, a toolchain
    /// fault, a draining host), a cancellation, and anything that happened after
    /// the command ran are all re-presented unchanged, so retrying them would
    /// only spend the machine twice for the same answer.
    #[cfg(test)]
    pub(super) fn is_transient(self) -> bool {
        matches!(
            self,
            Self::Capacity | Self::EnvironmentRetired | Self::CommandPreempted
        )
    }
}

/// Fit `diagnostic` into `budget` chars while keeping both ends that carry
/// meaning: the leading text that says which failure this is, and the final
/// lines that say how it ended, with the gap between them named.
///
/// Never silent and never head-only. A reader who cannot see the end of a
/// process log cannot tell a killed process from a clean transport exit, which
/// is exactly the inversion a head-only bound produced twice against the same
/// class of executor loss.
pub(super) fn bounded_diagnostic(diagnostic: &str, budget: usize) -> String {
    let total = diagnostic.chars().count();
    if total <= budget {
        return diagnostic.to_string();
    }
    // The marker states how much it dropped, so its own length depends on the
    // split it belongs to. Reserving against the whole diagnostic bounds it
    // from above — the omitted count can only be smaller than the total — so
    // the composition below can never exceed the budget.
    let reserved = omission_marker(total).chars().count();
    let spendable = budget.saturating_sub(reserved);
    if spendable == 0 {
        return elided_to_the_bone(diagnostic, budget);
    }
    let head = head_lines(diagnostic, INLINE_DIAGNOSTIC_HEAD_CHARS.min(spendable / 2));
    let end = tail_lines(diagnostic, spendable.saturating_sub(head.chars().count()));
    let omitted = total.saturating_sub(head.chars().count() + end.chars().count());
    format!("{head}{}{end}", omission_marker(omitted))
}

/// The break a reader must not mistake for the whole log.
pub(super) fn omission_marker(omitted: usize) -> String {
    format!("\n… {omitted} characters omitted …\n")
}

/// What a budget too small to state its own omission count still owes a reader:
/// the mark that says this text is not whole, and whatever end fits behind it.
///
/// Never the bare tail. A fragment with nothing to mark it reads as a complete
/// short diagnostic, which is the exact misreading the whole bound exists to
/// prevent — at budget 1 a silent tail is one character indistinguishable from a
/// one-character failure. No production caller has a budget this small, so this
/// is written to keep the helper's promise true rather than true-where-called;
/// a bound that is honest only at the sizes it currently sees is a bound that
/// breaks the first time it is reused.
pub(super) fn elided_to_the_bone(diagnostic: &str, budget: usize) -> String {
    if budget == 0 {
        return String::new();
    }
    format!("…{}", tail(diagnostic, budget - 1))
}

/// `text` cut to `budget` chars, marked whenever anything was dropped.
///
/// An attribution names the work that held the machine, so its beginning is the
/// part that identifies it and the mark says the name is abbreviated.
pub(super) fn bounded_attribution(text: &str, budget: usize) -> String {
    if text.chars().count() <= budget {
        return text.to_string();
    }
    let kept: String = text.chars().take(budget.saturating_sub(1)).collect();
    format!("{kept}…")
}

/// The leading `budget` chars of `text`, cut back to a line boundary when that
/// still spends most of the budget, so the head reads as whole lines rather
/// than a severed one.
pub(super) fn head_lines(text: &str, budget: usize) -> String {
    let head: String = text.chars().take(budget).collect();
    let Some(cut) = head.rfind('\n') else {
        return head;
    };
    // Measured in characters, like every other quantity in this bound: a byte
    // ratio would trim at a different point than the documented one as soon as
    // the text is multibyte.
    let kept = head[..cut].chars().count();
    if kept * 2 >= head.chars().count() {
        head[..cut].to_string()
    } else {
        head
    }
}

/// The final `budget` chars of `text`, advanced past a severed opening line
/// when that costs little, so the tail reads as whole lines. A last line longer
/// than the budget keeps its severed form: cutting to the next boundary there
/// would throw the decisive line away entirely.
pub(super) fn tail_lines(text: &str, budget: usize) -> String {
    let end = tail(text, budget);
    let Some(cut) = end.find('\n') else {
        return end;
    };
    let dropped = end[..=cut].chars().count();
    let total = end.chars().count();
    if dropped * 2 <= total && dropped < total {
        end[cut + 1..].to_string()
    } else {
        end
    }
}

/// What an executor's refusal to start a cell says about the check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NoStart {
    /// A substrate condition, in the shape an agent is told.
    Substrate(SubstrateFailureShape),
    /// Cairn itself ended the check, so nothing failed.
    Cancelled(crate::fleet::CancelCause),
}

#[cfg(test)]
impl NoStart {
    pub(super) fn is_transient(&self) -> bool {
        matches!(self, Self::Substrate(shape) if shape.is_transient())
    }
}

/// The condition class behind an executor's refusal to start a cell.
///
/// Exhaustive over [`CellUnavailableReason`] on purpose: a new reason cannot be
/// added upstream without landing here, which is what keeps a novel refusal from
/// silently arriving at an agent as the generic "could not start this check".
/// The sibling classifier in [`crate::fleet::placement`] answers a different
/// question about the same input — whether the fleet should keep waiting — and
/// the two are deliberately separate: what the fleet does next and what the
/// agent is told are not the same decision.
pub(super) fn no_start(reason: &cairn_common::executor_protocol::CellUnavailableReason) -> NoStart {
    use cairn_common::executor_protocol::{AdmissionRejectionReason, CellUnavailableReason};
    use NoStart::Substrate;
    use SubstrateFailureShape as Shape;
    match reason {
        CellUnavailableReason::Deadline {
            host_pressure,
            substrate,
        } => Substrate(deadline_shape(host_pressure.as_ref(), substrate.as_ref())),
        CellUnavailableReason::AdmissionRejected { reason } => Substrate(match reason {
            AdmissionRejectionReason::QueueFull => Shape::Capacity,
            AdmissionRejectionReason::Draining => Shape::Draining,
            AdmissionRejectionReason::StorageCleanupFailed => Shape::Storage,
            // A reservation larger than the machine's whole budget is not a
            // wait: no amount of idling makes the host bigger.
            AdmissionRejectionReason::RequestTooLarge => Shape::NoMachine,
        }),
        CellUnavailableReason::ExecutorUnavailable => Substrate(Shape::MachineUnreachable),
        CellUnavailableReason::NoMatchingExecutor { rejected } => {
            if rejected
                .iter()
                .any(|machine| transient_placement_refusal(&machine.reason))
            {
                Substrate(Shape::Capacity)
            } else {
                Substrate(Shape::NoMachine)
            }
        }
        // The pinned machine can never hold the work's measured memory: the
        // pin, not the fleet, is what left no machine to run it on.
        CellUnavailableReason::HomeCannotHold { .. } => Substrate(Shape::NoMachine),
        // The runner withdrew the work because the runner itself was stopping.
        // That is Cairn ending the check, not a machine lost or missing, and
        // not a substrate failure either: charged to the substrate, a runner
        // restart advanced every lane's infrastructure streak.
        CellUnavailableReason::RunnerStopping { .. } => {
            NoStart::Cancelled(crate::fleet::CancelCause::RunnerStopping)
        }
        // What the machine said last cannot settle whether the check ran: the
        // runner reads its account between polls, an item can start and end
        // between two reads, and a completion record covers refusals too.
        CellUnavailableReason::ClaimedAttemptSilent { .. } => Substrate(Shape::OutcomeUnknown),
        CellUnavailableReason::Provisioning
        | CellUnavailableReason::Checkout
        | CellUnavailableReason::Preparation
        | CellUnavailableReason::PreparationCommandNotFound { .. } => Substrate(Shape::Preparation),
        CellUnavailableReason::SlotUnhealthy => Substrate(Shape::EnvironmentRetired),
        CellUnavailableReason::BuildVolumeAbsent { .. } => Substrate(Shape::BuildVolumeAbsent),
        CellUnavailableReason::BuildVolumeLost { .. } => Substrate(Shape::BuildVolumeLost),
        // The environment was ready and the command still could not be launched,
        // which is Cairn's own machinery failing rather than the machine's state.
        CellUnavailableReason::Spawn
        | CellUnavailableReason::RunnerContext
        | CellUnavailableReason::ObjectInfrastructure(_) => Substrate(Shape::Dispatch),
    }
}

/// Which repair domain an infrastructure failure belongs to.
///
/// Decided here, at the source, rather than read back off the stored
/// `failure_kind`: [`substrate_failure_kind`] collapses eleven substrate shapes
/// into `Infrastructure`, and the distinction this needs is exactly the one it
/// throws away.
///
/// Exhaustive over [`SubstrateFailureShape`] with no wildcard, the same
/// discipline [`no_start_shape`] keeps for the same reason: a novel shape must
/// be classified here rather than inheriting whichever answer happens to be
/// nearest.
pub(super) fn infra_repair_domain(
    substrate: Option<SubstrateFailureShape>,
    kind: Option<CheckFailureKind>,
) -> InfraRepairDomain {
    if let Some(shape) = substrate {
        return match shape {
            // Structural placement gaps stay fleet-scoped: a commit cannot
            // repair them. Capability changes and an eligible machine's
            // declared repair move this scope.
            // An unmounted build volume is the same kind of fact: the machine
            // is fine and the head is fine, and the fleet's shape is what
            // changes when the volume returns.
            SubstrateFailureShape::NoMachine
            | SubstrateFailureShape::Draining
            | SubstrateFailureShape::BuildVolumeAbsent => InfraRepairDomain::Fleet,
            // Fleet too, but under an identity that names the blind machines
            // (`Fleet::capability_identity`), so the counter it is charged to
            // exists only while they stay blind and their recovery is the
            // fleet change that lifts it. Retained for historical stored rows;
            // new typed telemetry refusals are Capacity in the Head domain.
            SubstrateFailureShape::MeasurementBlind => InfraRepairDomain::Fleet,
            // Cell races, lost links, capacity, storage, cancellation:
            // conditions local to one attempt at one head, which must stay
            // narrow and remain retryable elsewhere. `Preparation` stays here
            // deliberately — a checkout that could not be prepared is frequently
            // about THIS commit.
            SubstrateFailureShape::Dispatch
            | SubstrateFailureShape::Capacity
            | SubstrateFailureShape::MachineUnreachable
            | SubstrateFailureShape::NoReachableMachine
            | SubstrateFailureShape::Preparation
            | SubstrateFailureShape::EnvironmentRetired
            | SubstrateFailureShape::Result
            | SubstrateFailureShape::OutcomeUnknown
            | SubstrateFailureShape::Storage
            | SubstrateFailureShape::RunnerFault => InfraRepairDomain::Head,
            // A machine taken for an inference load: nothing about the suite,
            // the head or the machine's health, and the very next placement
            // goes elsewhere. Narrow, like capacity.
            SubstrateFailureShape::CommandPreempted => InfraRepairDomain::Head,
            // A stalled command is the suite's own: some test or step in it
            // waited on something that never came. That is a defect in the
            // command at this head, owned by whoever owns the suite, and no
            // machine repair or fleet change touches it.
            SubstrateFailureShape::CommandStalled => InfraRepairDomain::HeadOnPlatform,
            // The machine's capacity: a bigger or less loaded machine runs the
            // same commit. Reaching the bound avoids this machine rather than
            // refusing the check, and no commit is blamed.
            SubstrateFailureShape::CommandOomKilled => InfraRepairDomain::Environment,
            // The machine lost the ground under the work: avoid it, keyed to
            // its environment, and blame no commit.
            SubstrateFailureShape::BuildVolumeLost => InfraRepairDomain::Environment,
        };
    }
    // No substrate failure means the command RAN and its own output named the
    // cause. An evidence-matched host defect — sccache corruption, a missing
    // toolchain, missing build outputs — is a fact about the machine that
    // produced it, and no commit repairs it.
    if kind == Some(CheckFailureKind::Infrastructure) {
        return InfraRepairDomain::Environment;
    }
    InfraRepairDomain::Head
}

/// Whether a placement refusal can change without changing this check or the
/// fleet's structural capabilities. Read the immutable refusal, never a later
/// telemetry sample: a recovered machine cannot rewrite why an attempt died.
pub(super) fn transient_placement_refusal(
    reason: &cairn_common::executor_protocol::PlacementRejectionReason,
) -> bool {
    use cairn_common::executor_protocol::PlacementRejectionReason as Reason;
    match reason {
        Reason::ConnectionClosed
        | Reason::ConnectionStale { .. }
        | Reason::RunnerNotListening { .. }
        | Reason::Reconnecting { .. }
        | Reason::TelemetryGap { .. }
        | Reason::TelemetryStale { .. }
        | Reason::InsufficientMemory { .. }
        | Reason::InsufficientVolume { .. }
        | Reason::ProcessDeferred(_)
        | Reason::OutrankedBy { .. }
        | Reason::NestedSandboxCapableElsewhere { .. }
        | Reason::UpdatePending { .. }
        | Reason::ModelResident { .. }
        | Reason::PipeDegraded { .. }
        | Reason::Held { .. }
        | Reason::ProvisioningTurn { .. }
        | Reason::CadenceTurn { .. } => true,
        Reason::Declined { reason, .. } => {
            use cairn_common::executor_protocol::DeclineReason;
            match reason {
                DeclineReason::Capacity
                | DeclineReason::Pressure
                | DeclineReason::ProjectProvisioning
                | DeclineReason::Disk
                | DeclineReason::Unknown => true,
                DeclineReason::Constraint | DeclineReason::BuildVolumeAbsent => false,
            }
        }
        Reason::SelectorMismatch { .. }
        | Reason::PinMismatch { .. }
        | Reason::ProjectUnavailable { .. }
        | Reason::NotColocated
        | Reason::RepositoryNotTransferable { .. }
        | Reason::ExceedsMemoryCeiling { .. }
        | Reason::MemoryKillExcluded { .. }
        | Reason::ProcessTooLarge(_)
        | Reason::UntrustedVerdictPlatform { .. }
        | Reason::Drained
        | Reason::BuildVolumeAbsent { .. }
        | Reason::RoleExcluded { .. }
        | Reason::NotPublishEligible { .. }
        | Reason::AvoidedInfrastructure
        | Reason::PreparationToolchainMissing { .. }
        | Reason::GithubCredentialMissing { .. }
        | Reason::PreparationRefused { .. }
        | Reason::BuildPrerequisitesMissing { .. }
        | Reason::AbsenceExpired { .. } => false,
    }
}

/// What an elapsed acquisition deadline actually says happened.
///
/// A deadline is not a condition of its own: it is the moment a wait ended, and
/// what the wait was ON is in the executor's evidence. `CapacityBusy` behind a
/// queue is a machine doing its job; `ConnectedStalled`, a draining host, a disk
/// below its floor, or no evidence at all are machines that were never going to
/// get to this request. Telling an agent "could not obtain capacity" for the
/// second group — and, worse, asking twice more before saying it — is the
/// condition-class collapse this whole mapping exists to remove.
///
/// The wait/refuse split itself is NOT re-decided here: it is
/// [`crate::fleet::placement`]'s, read through the same two predicates that
/// module uses, so retry eligibility here and the fleet's own waiting decision
/// cannot drift apart. This function only names which condition a refusal was.
pub(super) fn deadline_shape(
    host_pressure: Option<&cairn_common::executor_protocol::HostPressureEvidence>,
    substrate: Option<&cairn_common::executor_protocol::ExecutorSubstrateEvidence>,
) -> SubstrateFailureShape {
    use crate::fleet::placement::{pressure_relieves_itself, substrate_is_working};
    use cairn_common::executor_protocol::HostPressureCondition;
    let working = substrate.is_some_and(|evidence| substrate_is_working(evidence.state));
    let relieving = host_pressure.is_some_and(pressure_relieves_itself);
    if working || relieving {
        return SubstrateFailureShape::Capacity;
    }
    // The executor's own statement about itself outranks inferred pressure.
    if let Some(evidence) = substrate {
        return stalled_substrate_shape(evidence.state);
    }
    match host_pressure {
        // Disk below its floor is the one hold nothing running will relieve: the
        // bytes come back from an operator, not from the queue draining.
        Some(evidence)
            if evidence
                .conditions
                .iter()
                .any(|condition| matches!(condition, HostPressureCondition::DiskFree { .. })) =>
        {
            SubstrateFailureShape::Storage
        }
        // Silence. The machine said nothing about why the wait ended, which is
        // the same reading placement takes: one that stopped answering.
        _ => SubstrateFailureShape::MachineUnreachable,
    }
}

/// The condition a substrate state describes when it is not one the fleet waits
/// on. Exhaustive so a new executor state must be decided here rather than
/// inheriting whichever answer happens to be nearest.
pub(super) fn stalled_substrate_shape(
    state: cairn_common::executor_protocol::ExecutorSubstrateState,
) -> SubstrateFailureShape {
    use cairn_common::executor_protocol::ExecutorSubstrateState as State;
    match state {
        // Refusing new work on purpose, usually on its way out.
        State::Draining => SubstrateFailureShape::Draining,
        // Connected and no longer reporting: the machine stopped answering.
        State::ConnectedStalled => SubstrateFailureShape::MachineUnreachable,
        // The machine answered explicitly; Cairn cannot start work until its
        // incompatible local executor has been replaced.
        State::PermanentRefusal => SubstrateFailureShape::Dispatch,
        // Another live process holds the local executor's generation, so this
        // runner has no local machine until it exits. A lane retried later can
        // land once it has.
        State::GenerationHeld => SubstrateFailureShape::MachineUnreachable,
        // Running work already admitted, and every state the fleet counts as the
        // machine working. Those were answered as capacity above; naming them
        // keeps this match total.
        State::ExecutionRunning
        | State::SupervisorSpawning
        | State::SupervisorRespawning
        | State::ProtocolAttaching
        | State::InitialStorageSweep
        | State::StorageAccounting
        | State::DispatchPreparing
        | State::SlotAdoption
        | State::CapacityBusy
        | State::KernelWait => SubstrateFailureShape::Capacity,
    }
}

/// The closing half of every agent-facing infrastructure message: what the
/// failure means for the agent's own work. "Runs again" is a fact rather than a
/// hope: an infrastructure verdict is never reusable from the result cache, so
/// the next cadence re-executes the
/// check (see [`crate::execution::cache::get_check_result`]).
pub(super) const SUBSTRATE_FAILURE_CONSEQUENCE: &str = "This is a failure inside Cairn, not a result about your change: no verdict was recorded, and the check runs again the next time checks run.";

/// The bound on the whole composed agent message, diagnostic and all.
///
/// The bound belongs to the composed message rather than to the diagnostic
/// inside it. Bounding only the diagnostic leaves the total free to drift with
/// whichever lead a shape happens to carry and with attribution text of no
/// fixed length, so "the diagnostic is bounded" and "the record is bounded"
/// quietly stop being the same claim: the ten leads differ by tens of
/// characters, and an attributed capacity refusal has no structural limit at
/// all. Deriving the diagnostic's share from what the prefix actually costs is
/// what makes one bound hold for every shape.
///
/// The size is the one the stored records were already observed to hold, kept
/// so this becomes a guarantee of the length the surfaces have lived with
/// rather than a change to it.
pub(super) const SUBSTRATE_AGENT_MESSAGE_CHARS: usize = 1_445;

/// How much of an overflowing inline diagnostic is spent identifying it.
///
/// A diagnostic long enough to overflow always has a captured process log
/// embedded in it, and the log's two ends answer different questions: the head
/// says which failure this is, the end says how the process actually died.
/// Spending the whole budget on the head twice turned a SIGKILLed remote
/// process into an inexplicable clean carrier exit — the machine-parseable
/// `CAIRN_REMOTE_PROCESS_EXIT=9` and the memory-pressure lines were present in
/// the captured log and cut out of the record a reader acts on. So
/// identification gets a small fixed share and the remainder goes to the end.
pub(super) const INLINE_DIAGNOSTIC_HEAD_CHARS: usize = 240;

/// How much of the message a capacity attribution may spend.
///
/// The attribution explains the row; the diagnostic is the evidence for it.
/// Cairn composes the attribution from its own placement records, so it is a
/// short phrase in practice — but nothing structurally bounds it, and a prefix
/// free to grow without limit would squeeze the evidence out of the message
/// whose purpose is to carry it. Bounding the attribution is what keeps a floor
/// under the diagnostic.
pub(super) const CAPACITY_ATTRIBUTION_CHARS: usize = 120;

/// A check-infrastructure failure, composed into its two audiences at the seam
/// where an executor outcome becomes a check verdict.
///
/// The agent-facing half is authored here, at the source, rather than scrubbed
/// downstream: [`Self::agent_message`] is plain language an agent can act on.
/// [`Self::diagnostic`] keeps the complete substrate detail for the operator log.
/// The agent message carries a bounded copy too: a failure row must name its
/// failing step without requiring a second log surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubstrateFailure {
    pub(super) shape: SubstrateFailureShape,
    /// Placement has already exhausted its wait and answered the request.
    /// Retryable on a new placement fact, not by restarting the same wait.
    pub(super) placement_refused: bool,
    pub(super) diagnostic: String,
    pub(super) build_service_implicated: bool,
    /// What the batch was waiting ON, when Cairn's own placement records say.
    /// Agent-facing on purpose: unlike a cell id or a queue position, the
    /// identity of the work holding the machine is a fact about the world that
    /// explains the row and predicts its recovery.
    pub(super) waited_on: Option<String>,
    /// How long the request this failure answers was outstanding, when the
    /// submission measured it. A batched check is handed its outcome only after
    /// the whole batch settles, so the check's own timer reads zero however long
    /// it actually waited or ran; this is the time it really spent.
    pub(super) elapsed_ms: Option<i64>,
    /// The machine that had claimed the check when it failed, when one had.
    pub(super) placement: Option<Box<crate::fleet::CellPlacement>>,
    /// The executor's account of the command this failure ended, when one ran:
    /// a command the executor stopped still ran somewhere, on some platform.
    /// Which platform decides which stall counter it is charged to, and the
    /// machine is what a result names. Boxed: every check result carries this
    /// failure type, and the metadata is larger than all its other fields.
    pub(super) provenance: Option<Box<cairn_common::executor_protocol::CellExecutionMeta>>,
}

impl SubstrateFailure {
    pub(crate) fn new(shape: SubstrateFailureShape, diagnostic: impl Into<String>) -> Self {
        Self {
            shape,
            placement_refused: false,
            diagnostic: diagnostic.into(),
            build_service_implicated: false,
            waited_on: None,
            elapsed_ms: None,
            provenance: None,
            placement: None,
        }
    }

    /// Record the machine that had claimed the check.
    pub(crate) fn on_machine(mut self, placement: crate::fleet::CellPlacement) -> Self {
        self.placement = Some(Box::new(placement));
        self
    }

    pub(crate) fn placement(&self) -> Option<&crate::fleet::CellPlacement> {
        self.placement.as_deref()
    }

    /// The sentence that opens the agent's message: the shape's own, naming
    /// the machine when the failure is a lost link to one.
    pub(super) fn lead(&self) -> std::borrow::Cow<'static, str> {
        match (&self.placement, self.shape) {
            (Some(placement), SubstrateFailureShape::MachineUnreachable) => format!(
                "Cairn lost contact with {}, the machine running this check.",
                placement.name
            )
            .into(),
            _ => self.shape.lead().into(),
        }
    }

    /// Attach the executor's account of the command this failure ended.
    pub(crate) fn ran_as(
        mut self,
        provenance: cairn_common::executor_protocol::CellExecutionMeta,
    ) -> Self {
        self.provenance = Some(Box::new(provenance));
        self
    }

    pub(crate) fn provenance(&self) -> Option<&cairn_common::executor_protocol::CellExecutionMeta> {
        self.provenance.as_deref()
    }

    /// Record how long the request behind this failure was outstanding.
    pub(crate) fn lasted(&mut self, elapsed: std::time::Duration) {
        self.elapsed_ms = Some(i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX));
    }

    pub(crate) fn elapsed_ms(&self) -> Option<i64> {
        self.elapsed_ms
    }

    /// Record what this failure spent its patience waiting on.
    pub(crate) fn name_the_wait(&mut self, waited_on: &str) {
        self.waited_on = Some(waited_on.to_string());
    }

    pub(crate) fn waited_on(&self) -> Option<&str> {
        self.waited_on.as_deref()
    }

    /// Mark this failure as one the shared build-cache service caused, so its
    /// verdict may carry the service's advisory. Applicability is a property of
    /// the failure, established where the failure is composed — never inferred
    /// downstream from whether the daemon happens to be sick at the time.
    pub(crate) fn implicating_build_service(mut self) -> Self {
        self.build_service_implicated = true;
        self
    }

    pub(crate) fn build_service_implicated(&self) -> bool {
        self.build_service_implicated
    }

    /// The authored text an agent reads in place of the substrate diagnostic.
    pub(crate) fn agent_message(&self) -> String {
        let explanation = match &self.waited_on {
            Some(waited_on) => format!(
                "{} {}. {SUBSTRATE_FAILURE_CONSEQUENCE}",
                self.lead(),
                bounded_attribution(waited_on, CAPACITY_ATTRIBUTION_CHARS)
            ),
            None => format!("{} {SUBSTRATE_FAILURE_CONSEQUENCE}", self.lead()),
        };
        let prefix = format!("{explanation}\nFailing step:\n");
        let budget = SUBSTRATE_AGENT_MESSAGE_CHARS.saturating_sub(prefix.chars().count());
        format!("{prefix}{}", bounded_diagnostic(&self.diagnostic, budget))
    }

    /// The unabridged substrate diagnostic retained for the operator log.
    pub(crate) fn diagnostic(&self) -> &str {
        &self.diagnostic
    }

    /// The condition class this failure preserves.
    pub(crate) fn shape(&self) -> SubstrateFailureShape {
        self.shape
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckExecutionFailure {
    /// The runner stopped waiting, but the remote lane remains owned.
    LeftRunning(crate::fleet::LeftRunningAttempt),
    /// The check's own process failed — output the agent owns and can act on.
    Process(String),
    /// Cairn's machinery failed before, around, or after the check.
    Substrate(SubstrateFailure),
    /// Submission declined to launch this command at all: the bounded retries of
    /// some repair domain are spent. This is not a failure of anything — nothing
    /// ran — and it must never be classified, stored, or counted as one. The
    /// evidence the refusal was taken on travels with it, so every surface that
    /// renders the refusal names the real diagnostic.
    Suppressed(Box<SuppressedCheck>),
    /// Submission declined to offer this command to placement at all: no machine
    /// the fleet knows of, attached or enrolled, could ever take it. Like a
    /// suppression, nothing ran and no retry budget was spent; unlike one, no
    /// failure preceded it. The sentence names what was asked for and what the
    /// fleet has instead.
    NotPlaceable(String),
    /// Submission declined to offer this command to placement because the
    /// project's canonical execution policy does not load in this build, which
    /// refuses every execution in the project. Nothing ran; the sentence names
    /// the file and the parser's reason.
    ConfigUnreadable(String),
    /// Cairn cancelled the check before it produced a verdict, for a stated
    /// cause. Not an execution result: nothing about the tree, the machine, or
    /// the substrate was learned, so like a suppression it is never stored or
    /// counted, and it moves no infrastructure streak.
    Cancelled(crate::fleet::CancelCause),
}

impl CheckExecutionFailure {
    pub(crate) fn substrate(shape: SubstrateFailureShape, diagnostic: impl Into<String>) -> Self {
        Self::Substrate(SubstrateFailure::new(shape, diagnostic))
    }
}

impl From<String> for CheckExecutionFailure {
    fn from(error: String) -> Self {
        Self::Process(error)
    }
}

/// Terminal classification refining a failing check's binary `passed = false`
/// verdict. Persisted timeout rows and mixed-version executor results remain
/// readable even though current check batches cannot create duration timeouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckFailureKind {
    /// Historical or mixed-version result killed at a timeout budget.
    TimedOut,
    /// The process could not be spawned (e.g. its cwd vanished mid-run).
    SpawnError,
    /// Died by signal mid-run without hitting the budget (crash / OOM kill).
    Killed,
    /// Positive evidence identifies a host, toolchain, or shared-service failure.
    Infrastructure,
    /// Positive evidence identifies a system dependency absent from the executor.
    Environment,
    /// The producer failed without enough evidence to assign urgency or ownership.
    Unclassified,
    /// A recognized test runner reports that the requested selection matched nothing.
    Selection,
    /// A prerequisite step failed before the suite ran: `ensure-sidecar`, whose
    /// sidecar build compiles crates the suite never compiles, and only on a
    /// cell that lacks them. Still a red about the tree, but never the suite's
    /// reusable verdict, since its key need not cover what the step compiled.
    Prerequisite,
    /// Fleet admission could not obtain capacity before its knowledge-bounded wait expired.
    Capacity,
    /// A recognized test runner exited abnormally without assertion failures.
    RunnerError,
    /// No machine the fleet knows of, attached or enrolled, satisfies the
    /// check's platform or toolchains, so it was never offered to placement.
    /// Nothing ran: this is the absence of a verdict until such a machine
    /// attaches, not a failure of anything.
    NotPlaceable,
    /// The project's `.cairn/config.yaml` does not load in this build, so
    /// placement refuses every execution in the project and the check was
    /// never offered to it. Like [`Self::NotPlaceable`], the absence of a
    /// verdict until the file loads (fixed, or read by a newer build), not a
    /// failure of the code or of any machine.
    ConfigUnreadable,
    /// Cairn cancelled the check before it produced a verdict: its job was
    /// stopped, a newer request replaced it, the runner was stopping. Neither
    /// a verdict about the code nor a fact about the substrate, so it is never
    /// stored and renders as unverified.
    Cancelled,
}

impl CheckFailureKind {
    /// Whether the failure says Cairn could not obtain a verdict, rather than
    /// saying the evaluated change failed. `TimedOut` is intentionally included:
    /// current batches do not impose command-duration timeouts, so persisted
    /// timeout results come from historical or mixed-version fleet budget kills.
    /// A change-owned hang reaches today's build-slot deadline as a substrate
    /// failure instead. Keeping that ownership here makes suppression, merge
    /// gating, publication, and turn-end signaling share one policy.
    pub(crate) fn is_infrastructure(self) -> bool {
        matches!(
            self,
            CheckFailureKind::TimedOut
                | CheckFailureKind::SpawnError
                | CheckFailureKind::Infrastructure
                | CheckFailureKind::Capacity
                | CheckFailureKind::RunnerError
                | CheckFailureKind::NotPlaceable
                | CheckFailureKind::ConfigUnreadable
        )
    }

    /// Whether the check was never offered to placement: it holds no verdict,
    /// opens no infrastructure streak, has no run to time, and runs on the
    /// first wave after its cause clears.
    pub(crate) fn attempted_nothing(self) -> bool {
        matches!(
            self,
            CheckFailureKind::NotPlaceable | CheckFailureKind::ConfigUnreadable
        )
    }

    /// Whether a failure of this kind says something about the evaluated
    /// change — whether it is a red. Distinct from [`Self::is_infrastructure`],
    /// which asks whether the substrate is failing: a cancellation is neither,
    /// so it must stay out of both the streak and the red.
    pub(crate) fn is_verdict_about_the_code(self) -> bool {
        !self.is_infrastructure() && self != CheckFailureKind::Cancelled
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CheckFailureKind::TimedOut => "timed_out",
            CheckFailureKind::SpawnError => "spawn_error",
            CheckFailureKind::Killed => "killed",
            CheckFailureKind::Infrastructure => "infrastructure",
            CheckFailureKind::Environment => "environment",
            CheckFailureKind::Unclassified => "unclassified",
            CheckFailureKind::Selection => "selection",
            CheckFailureKind::Prerequisite => "prerequisite",
            CheckFailureKind::Capacity => "capacity",
            CheckFailureKind::RunnerError => "runner_error",
            CheckFailureKind::NotPlaceable => "not_placeable",
            CheckFailureKind::ConfigUnreadable => "config_unreadable",
            CheckFailureKind::Cancelled => "cancelled",
        }
    }

    /// Parse a persisted `failure_kind` string back into the enum; `None` for a
    /// pass, an ordinary failure, or a legacy `NULL`. (Not `FromStr`: the
    /// absent/unknown case is an ordinary `None`, not a parse error.)
    pub(crate) fn from_stored(s: &str) -> Option<Self> {
        match s {
            "timed_out" => Some(CheckFailureKind::TimedOut),
            "spawn_error" => Some(CheckFailureKind::SpawnError),
            "killed" => Some(CheckFailureKind::Killed),
            "infrastructure" => Some(CheckFailureKind::Infrastructure),
            "environment" => Some(CheckFailureKind::Environment),
            "unclassified" => Some(CheckFailureKind::Unclassified),
            "selection" => Some(CheckFailureKind::Selection),
            "prerequisite" => Some(CheckFailureKind::Prerequisite),
            "capacity" => Some(CheckFailureKind::Capacity),
            "runner_error" => Some(CheckFailureKind::RunnerError),
            "not_placeable" => Some(CheckFailureKind::NotPlaceable),
            "config_unreadable" => Some(CheckFailureKind::ConfigUnreadable),
            "cancelled" => Some(CheckFailureKind::Cancelled),
            _ => None,
        }
    }

    /// The human verdict fragment. Duration is used only when rendering a
    /// historical or mixed-version timeout row.
    pub(crate) fn describe(self, duration_ms: i64) -> String {
        match self {
            CheckFailureKind::TimedOut => {
                format!("timed out after {}", format_timeout_budget(duration_ms))
            }
            CheckFailureKind::SpawnError => "failed to spawn".to_string(),
            CheckFailureKind::Killed => "killed (signal)".to_string(),
            CheckFailureKind::Infrastructure => "infrastructure/toolchain failure".to_string(),
            CheckFailureKind::Environment => {
                "environment cannot build this crate cone on this machine — unverified".to_string()
            }
            CheckFailureKind::Unclassified => {
                "failure could not be classified — needs investigation".to_string()
            }
            CheckFailureKind::Selection => "test selection matched no tests".to_string(),
            CheckFailureKind::Prerequisite => {
                "a prerequisite step failed before the suite ran — not the suite's verdict"
                    .to_string()
            }
            CheckFailureKind::Capacity => "executor capacity unavailable".to_string(),
            CheckFailureKind::RunnerError => "test runner failed".to_string(),
            CheckFailureKind::NotPlaceable => {
                "not placeable — no machine in the fleet can run it".to_string()
            }
            CheckFailureKind::ConfigUnreadable => {
                "not run — this build cannot read the project's .cairn/config.yaml".to_string()
            }
            CheckFailureKind::Cancelled => "cancelled".to_string(),
        }
    }
}

/// Format a timeout budget compactly: whole minutes at or above a minute, else
/// seconds. `600_000` → `10m`, `1_800_000` → `30m`, `45_000` → `45s`.
pub(super) fn format_timeout_budget(duration_ms: i64) -> String {
    if duration_ms >= 60_000 {
        format!("{}m", (duration_ms as f64 / 60_000.0).round() as i64)
    } else {
        format!("{}s", (duration_ms as f64 / 1000.0).round() as i64)
    }
}

/// Chars of combined check output retained in the cache row's `output_tail`.
pub(super) const OUTPUT_TAIL_CHARS: usize = 4_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FailureClassification {
    pub(super) kind: CheckFailureKind,
    pub(super) reason: String,
    pub(super) evidence_line: Option<usize>,
    /// Positive evidence that sccache failed while reading a cached compiler
    /// artifact. This is deliberately separate from the broad infrastructure
    /// bucket so only this reviewed failure shape can trigger an uncached rerun.
    pub(super) cache_corruption: bool,
    /// A capability the failure shows the project's work needs of a machine,
    /// which the runner records so placement requires it from now on.
    pub(super) implied_requirement: Option<ImpliedRequirement>,
}

/// What a classified failure says the project needs of the machines it runs
/// on, beyond what it declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ImpliedRequirement {
    pub(super) capability: ProjectCapability,
    /// The repository the failing fetch named, when it named one.
    pub(super) repository: Option<String>,
}

/// What a failure signature is computed over besides the kind and exit code:
/// the cause (the substrate shape, or the classifier's reason) and the line the
/// classification rests on (a substrate failure's first diagnostic line, which
/// is its `Failing step`, or the classifier's evidence line). Report rows the
/// classifier blanked are never the evidence line, so a test name cannot sign a
/// failure.
pub(super) fn failure_cause_and_evidence(
    substrate: Option<&SubstrateFailure>,
    classification: Option<&FailureClassification>,
    output: &str,
) -> (String, String) {
    if let Some(substrate) = substrate {
        let step = substrate
            .diagnostic()
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or_default();
        return (
            format!("substrate:{:?}", substrate.shape()),
            step.to_string(),
        );
    }
    let Some(classification) = classification else {
        return (String::new(), String::new());
    };
    let evidence = classification
        .evidence_line
        .and_then(|index| output.lines().nth(index))
        .unwrap_or_default();
    (
        without_executor_provenance(&classification.reason).to_string(),
        evidence.to_string(),
    )
}

/// Name the way a failing check DIED when the death is not an ordinary test
/// failure, so a red verdict never sends an agent hunting for a test that never
/// failed. `None` means "this is an ordinary failure": the parsed failures and
/// the exit code already explain it.
pub(super) fn classify_check_failure(
    command: &str,
    exit_code: Option<i32>,
    timed_out: bool,
    spawn_error: bool,
    parsed: Option<&ParsedCheckResult>,
    output: &str,
) -> Option<FailureClassification> {
    if exit_code == Some(0) {
        return None;
    }
    if spawn_error {
        return Some(FailureClassification {
            kind: CheckFailureKind::SpawnError,
            reason: "Cairn: check process failed to spawn".to_string(),
            evidence_line: None,
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    if timed_out {
        return Some(FailureClassification {
            kind: CheckFailureKind::TimedOut,
            reason: "Cairn: check exceeded its timeout budget".to_string(),
            evidence_line: None,
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    if exit_code.is_none() {
        return Some(FailureClassification {
            kind: CheckFailureKind::Killed,
            reason: "Cairn: check process died without an exit code".to_string(),
            evidence_line: None,
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    // Named failure sites — failing assertions, or files that failed to collect
    // — are an ordinary failure the detail path already renders.
    //
    // Not for rust-lint: its named sites can be compiler errors that sccache
    // corruption produced, and the scans below must still see that output so
    // the uncached retry fires. A lint result only adds names to a red; it never
    // decides how the red is classified.
    //
    // So is the plan audit's own red on a clean run: a named finding, not a
    // runner that failed outside any test.
    if parsed.is_some_and(|result| {
        result.parser != RUST_LINT_PARSER
            && (result.failed > 0
                || result.suite_failures > 0
                || result.exit_is_plan_audits(exit_code))
    }) {
        return None;
    }

    // A runner's report rows name tests, and a test's name is free text: the
    // suite that pins these very signatures names its cases after them, so a
    // passing row of it would read as an sccache transport failure. Rows are
    // blanked rather than dropped so evidence indices still address `output`.
    // A row naming a FAILING test is the same positive evidence a parsed
    // failure is, for a run whose structured result never reached Cairn.
    //
    // A structured-result line is data for the same reason, read or not: its
    // test identities are free text, and its real outcomes reach this
    // function through `parsed`, never through a phrase scan. One the parser
    // refused (truncated, an unknown schema) is still the envelope.
    let mut names_a_failing_test = false;
    let lines: Vec<&str> = output
        .lines()
        .map(|line| {
            if line.trim_start().starts_with(RESULT_ARTIFACT_JSON_MARKER) {
                return "";
            }
            match reported_test(line) {
                Some(ReportedTest::Failed) => {
                    names_a_failing_test = true;
                    ""
                }
                Some(ReportedTest::NotFailed) => "",
                None => line,
            }
        })
        .collect();
    if names_a_failing_test && !parsed.is_some_and(|result| result.parser == RUST_LINT_PARSER) {
        return None;
    }
    let cache_corruption = lines.iter().position(|line| {
        line.to_ascii_lowercase()
            .contains("memory map must have a non-zero length")
    });
    // A script's own marker is the exit code it left with and the line it
    // wrote, which opens with the marker; the literal elsewhere is a quotation.
    let explicit_infrastructure = (exit_code == Some(75))
        .then(|| {
            lines
                .iter()
                .position(|line| line.starts_with("CAIRN_INFRASTRUCTURE_EXIT: "))
        })
        .flatten();
    // The run computed a pass and the executor's result destination refused
    // it (`HANDOFF_FAILED_EXIT_CODE` in scripts/lib/rust-process-outcome.ts).
    // The code and the marker are both required, as for exit 75.
    let handoff_refused = (exit_code == Some(74))
        .then(|| {
            lines
                .iter()
                .position(|line| line.starts_with("CAIRN_HANDOFF_FAILED_EXIT: "))
        })
        .flatten();
    let producer_failure = producer_failure_marker(&lines, exit_code);
    let cache_or_sandbox_denial = lines.iter().position(|line| {
        let line = line.replace('\\', "/").to_ascii_lowercase();
        line.contains("sccache: encountered fatal error")
            || line.contains("couldn't create a temp dir")
            || (line.contains("operation not permitted (os error 1)")
                && line.contains("/build-slots/")
                && line.contains("/target/"))
    });
    let transport = lines.iter().position(|line| {
        let line = line.to_ascii_lowercase();
        line.contains("failed to send data to or receive data from server")
            || line.contains("failed client/server communication")
            || line.contains("failed to fill whole buffer")
            || line.contains("server looks like it shut down unexpectedly")
    });
    let abnormal_254 = (exit_code == Some(254))
        .then(|| {
            lines.iter().position(|line| {
                let line = line.to_ascii_lowercase();
                (line.contains("rustc") || line.contains("sccache"))
                    && (line.contains("exit status: 254")
                        || line.contains("exited with status 254"))
                    && (line.contains("process didn't exit successfully")
                        || line.contains("failed to execute compile")
                        || line.contains("compiler process"))
            })
        })
        .flatten();
    let missing_generated = lines.iter().position(|line| {
        let line = line.replace('\\', "/").to_ascii_lowercase();
        line.contains("target/")
            && line.contains("/build/")
            && line.contains("/out/")
            && (line.contains("couldn't read")
                || line.contains("failed to read")
                || line.contains("no such file or directory"))
    });
    let missing_toolchain = lines.iter().position(|line| {
        let line = line.trim().to_ascii_lowercase();
        ["node", "bun", "npm", "cargo"].iter().any(|program| {
            let posix_missing = line.ends_with(&format!("{program}: command not found"))
                || line.ends_with(&format!("command not found: {program}"))
                || line.ends_with(&format!("{program}: not found"));
            let shell_diagnostic = line.starts_with("sh:")
                || line.starts_with("bash:")
                || line.starts_with("zsh:")
                || line.starts_with("/bin/sh:")
                || line.contains(": line ");
            (posix_missing && shell_diagnostic)
                || (line.starts_with("/usr/bin/env:")
                    && line.contains(program)
                    && line.contains("no such file or directory"))
                || (line.starts_with(&format!("'{program}' is not recognized"))
                    && line.contains("internal or external command"))
                || (line.starts_with(&format!("the term '{program}' is not recognized"))
                    && line.contains("name of a cmdlet"))
        })
    });
    if let Some(evidence_line) = cache_corruption {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason: "Cairn: sccache cache-read corruption matched reviewed evidence".to_string(),
            evidence_line: Some(evidence_line),
            cache_corruption: true,
            implied_requirement: None,
        });
    }
    if let Some(evidence_line) = handoff_refused {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason: "Cairn: the run computed its verdict, but the executor refused its structured \
                     result, so no verdict was recorded"
                .to_string(),
            evidence_line: Some(evidence_line),
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    if let Some((evidence_line, kind)) = producer_failure {
        let reason = match kind {
            CheckFailureKind::Environment => {
                "Cairn: the executor environment is missing a required system dependency"
            }
            CheckFailureKind::Unclassified => {
                "Cairn: the Rust process failed without enough evidence to assign cause or ownership"
            }
            CheckFailureKind::Selection => "Cairn: the requested test selection matched no tests",
            CheckFailureKind::Prerequisite => {
                "Cairn: a prerequisite step failed before the suite ran, so this red is not the \
                 suite's verdict and is never reused as one"
            }
            _ => unreachable!("producer failure markers only map to typed Rust process outcomes"),
        };
        return Some(FailureClassification {
            kind,
            reason: reason.to_string(),
            evidence_line: Some(evidence_line),
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    // A git dependency the machine could not authenticate to fetch exits as a
    // build failure, and cargo words it as a bad pin. The cause is the
    // machine's missing credential, which the same tree fetches fine elsewhere.
    if let Some(failure) =
        cairn_common::git_credential::credential_failure_in(lines.iter().copied())
    {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason: credential_fetch_reason(&failure, output),
            evidence_line: Some(failure.line_index),
            cache_corruption: false,
            implied_requirement: failure.is_github().then(|| ImpliedRequirement {
                capability: ProjectCapability::GithubCredential,
                repository: failure.repository.clone(),
            }),
        });
    }
    // A compile the host ended exits as a build failure (cargo's 101), but the
    // host's signal or storage error is the cause, not the tree. Storing it as
    // infrastructure is what charges the death to the machine.
    if let Some(ended) = host_ended_compile(&lines.join("\n")) {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason: format!(
                "Cairn: {}; the host ended the build and no compiler error about the code was \
                 printed, so this run is not a verdict on the tree",
                ended.describe()
            ),
            evidence_line: Some(ended.evidence_line),
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    if let Some(evidence_line) = explicit_infrastructure
        .or(cache_or_sandbox_denial)
        .or(transport)
        .or(abnormal_254)
        .or(missing_generated)
        .or(missing_toolchain)
    {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason:
                "Cairn: infrastructure/toolchain failure matched reviewed abnormal-build evidence"
                    .to_string(),
            evidence_line: Some(evidence_line),
            cache_corruption: false,
            implied_requirement: None,
        });
    }

    // A shell reports a process ended by a signal as 128 + the signal. The
    // termination signals are sent from outside (an OOM killer, a supervisor,
    // a shutdown) and stop the run before it can report, so the exit is the
    // signal's and says nothing about the change. Crash signals (SIGSEGV,
    // SIGABRT) stay ordinary failures: those are the process's own outcome.
    if let Some(signal) = exit_code.and_then(termination_signal) {
        return Some(FailureClassification {
            kind: CheckFailureKind::Killed,
            reason: format!(
                "Cairn: check process was terminated by {signal} (exit {}) before it reported \
                 a verdict",
                exit_code.unwrap_or_default()
            ),
            evidence_line: None,
            cache_corruption: false,
            implied_requirement: None,
        });
    }

    if let Some(result) = parsed.filter(|result| result.parser == "vitest") {
        // Vitest exited nonzero having reported neither a failing test nor a
        // failing suite. The dominant cause is an error that escaped a test file
        // asynchronously: Vitest fails the run over it but attributes it to no
        // test, and its JSON `success` stays true. Point at that line so the tail
        // excerpt carries the stack rather than trailing render noise.
        let unhandled = lines
            .iter()
            .position(|line| line.to_ascii_lowercase().contains("unhandled error"));
        let mut reason = if result.passed == 0 {
            "Cairn: Vitest failed before reporting any test assertions".to_string()
        } else {
            format!(
                "Cairn: Vitest runner failed after {} tests passed with no assertion failures",
                result.passed
            )
        };
        if unhandled.is_some() {
            reason.push_str(
                " \u{2014} an error escaped a test file after it finished; Vitest fails the run \
                 without attributing it to any test",
            );
        }
        if let Some(gap) = result.ingestion.as_ref().and_then(ingestion_gap) {
            reason.push_str(&format!("; its result is incomplete: {gap}"));
        }
        return Some(FailureClassification {
            kind: CheckFailureKind::RunnerError,
            reason,
            evidence_line: unhandled,
            cache_corruption: false,
            implied_requirement: None,
        });
    }

    // A Vitest command that exited nonzero having emitted no report at all never
    // reached a test: its config or its dependencies failed to load. Without this
    // arm the verdict is a bare `exit 1` beside a tail of resolver noise.
    if parsed.is_none() && crate::execution::check_parsers::is_vitest_command(command) {
        return Some(FailureClassification {
            kind: CheckFailureKind::RunnerError,
            reason: "Cairn: Vitest exited without producing a report \u{2014} the run failed \
                     before collecting any test file"
                .to_string(),
            evidence_line: None,
            cache_corruption: false,
            implied_requirement: None,
        });
    }
    None
}

/// The rows a structured result carried that its parse refused, in words, or
/// `None` when it refused none.
pub(super) fn ingestion_gap(
    ingestion: &crate::execution::check_evidence::Ingestion,
) -> Option<String> {
    let mut gaps = Vec::new();
    if ingestion.duplicate_rows > 0 {
        let first = ingestion
            .duplicate_ids
            .first()
            .map(|id| format!(" (first: `{id}`)"))
            .unwrap_or_default();
        gaps.push(format!(
            "{} row(s) repeat an earlier test identity{first}",
            ingestion.duplicate_rows
        ));
    }
    if ingestion.invalid_rows > 0 {
        gaps.push(format!(
            "{} row(s) have no identity or no terminal status",
            ingestion.invalid_rows
        ));
    }
    (!gaps.is_empty()).then(|| gaps.join("; "))
}

/// Why a credential-less dependency fetch is not a verdict on the tree, in
/// words that point the reader at the machine rather than at the pin.
pub(super) fn credential_fetch_reason(
    failure: &cairn_common::git_credential::CredentialFailure,
    output: &str,
) -> String {
    const LINE_CHARS: usize = 200;
    let what = match (&failure.repository, &failure.host) {
        (Some(repository), _) => format!("fetching git dependency `{repository}`"),
        (None, Some(host)) => format!("fetching a git dependency from `{host}`"),
        (None, None) => "fetching a git dependency".to_string(),
    };
    let line: String = failure.line.chars().take(LINE_CHARS).collect();
    let reported_as_missing_revision = output.lines().any(|line| {
        let line = line.trim_start();
        line.starts_with("revision ") && line.ends_with(" not found")
    });
    let disguise = if reported_as_missing_revision {
        "; cargo reports this as 'revision … not found', but the pin was never checked"
    } else {
        ""
    };
    let credential = match failure.host.as_deref() {
        Some("github.com") | None => "GitHub".to_string(),
        Some(host) => format!("`{host}`"),
    };
    format!(
        "Cairn: {what} failed for want of a credential (`{line}`){disguise}. This machine has \
         no usable {credential} credential, so this run is not a verdict on the tree"
    )
}

/// The termination signal a shell exit status of 128 + N reports, for the
/// signals something outside the process sends to stop it.
pub(super) fn termination_signal(exit_code: i32) -> Option<&'static str> {
    exit_code.checked_sub(128).and_then(terminating_signal)
}

/// The name of a signal that is sent from outside a process to end it (an OOM
/// killer, a supervisor, a shutdown), by number. Crash signals (SIGSEGV,
/// SIGABRT) have none: those are the process's own outcome.
pub(super) fn terminating_signal(signal: i32) -> Option<&'static str> {
    match signal {
        1 => Some("SIGHUP"),
        2 => Some("SIGINT"),
        9 => Some("SIGKILL"),
        15 => Some("SIGTERM"),
        _ => None,
    }
}

/// A compile the tree did not fail: cargo reports a crate that could not
/// compile, the same output says the host ended it, and no compiler error
/// about the code appears anywhere. The host ends a compile by signalling its
/// process from outside (a stall stop, a pressure reclaim, a cell operation),
/// by failing its storage (a full disk, an I/O error), or by refusing the
/// compiler's write of its own build output (a sandbox or grant that does not
/// cover the target directory). Each is the execution side of the seam, and
/// there is no rustc signature to match against a build break.
///
/// One predicate serves both halves: the store's classifier reads it over the
/// run's whole output to store the row as `infrastructure`, and the main
/// cadence reads it over a stored row's tail to report the row as a death.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HostEndedCompile {
    pub(crate) by: HostEnding,
    pub(crate) krate: Option<String>,
    /// Index, among the output's lines, of the line that names the ending.
    pub(crate) evidence_line: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HostEnding {
    /// A compiler process ended by this terminating signal.
    Signal(&'static str),
    /// The operating system's storage error, as the OS words it.
    Storage(&'static str),
    /// The operating system refused rustc's write of its own build output
    /// (dep-info, object, metadata), as the OS words the refusal.
    WriteDenied(&'static str),
}

impl HostEndedCompile {
    /// What ended the compile, and which crate it was compiling.
    pub(super) fn describe(&self) -> String {
        let krate = self
            .krate
            .as_ref()
            .map(|krate| format!(" while compiling `{krate}`"))
            .unwrap_or_default();
        match self.by {
            HostEnding::Signal(signal) => {
                format!("a compiler process was killed by {signal}{krate}")
            }
            HostEnding::Storage(error) => format!("the host's storage failed ({error}){krate}"),
            // No crate: cargo fails every crate the refusal reaches, in
            // scheduling order, and the lane breaker signs the reason, so a
            // named crate would make one standing refusal sign differently on
            // each run. The evidence line names the refused path.
            HostEnding::WriteDenied(error) => format!(
                "the operating system refused the compiler's write of its build output \
                 ({error}), a refusal that recurs on this machine until its write access to \
                 that directory changes, not one that clears with time"
            ),
        }
    }
}

pub(crate) fn host_ended_compile(output: &str) -> Option<HostEndedCompile> {
    let mut by = None;
    let mut krate = None;
    let mut compiling = false;
    // Whether the lines being read belong to an uncoded `error:` diagnostic,
    // which is about the code only when it points at a source location.
    // Warnings carry locations too, and say nothing about the verdict.
    let mut in_error = false;
    for (index, line) in output.lines().map(str::trim_start).enumerate() {
        if line.starts_with("error[E") || (in_error && line.starts_with("--> ")) {
            return None;
        }
        if line.starts_with("error") || line.starts_with("warning") {
            in_error = false;
        }
        let subject = line
            .strip_prefix("error: could not compile ")
            .or_else(|| line.strip_prefix("error: failed to run custom build command for "));
        if let Some(subject) = subject {
            compiling = true;
            krate = krate.or_else(|| backticked(subject));
        } else if let Some(storage) = storage_error(line) {
            by = by.or(Some((index, HostEnding::Storage(storage))));
        } else if let Some(denial) = output_write_denial(line) {
            by = by.or(Some((index, HostEnding::WriteDenied(denial))));
        } else if let Some(signal) = process_signal(line) {
            by = by.or(Some((index, HostEnding::Signal(signal))));
        } else if line.starts_with("error: ") {
            in_error = true;
        }
    }
    let (evidence_line, by) = by?;
    compiling.then_some(HostEndedCompile {
        by,
        krate,
        evidence_line,
    })
}

/// The storage error an output line reports, in the operating system's words
/// (Linux and macOS, then Windows). Matched by text: the same `os error` number
/// means different things on different platforms.
pub(super) fn storage_error(line: &str) -> Option<&'static str> {
    let line = line.to_ascii_lowercase();
    [
        ("no space left on device", "No space left on device"),
        ("input/output error", "Input/output error"),
        (
            "there is not enough space on the disk",
            "There is not enough space on the disk",
        ),
    ]
    .into_iter()
    .find_map(|(needle, words)| line.contains(needle).then_some(words))
}

/// The operating system's refusal of a write rustc makes to its own build
/// output, as the OS words it. Only rustc's output-write diagnostics count,
/// each opening its own `error:` line: dep-info (`error writing dependencies
/// to`), codegen output (`could not write output to`), and metadata (`failed
/// to write`). A permission error a program or test prints, or a line that
/// merely contains a target path and `os error 1`, is not one.
pub(super) fn output_write_denial(line: &str) -> Option<&'static str> {
    let line = line.to_ascii_lowercase();
    let diagnostic = line.strip_prefix("error: ")?;
    [
        "error writing dependencies to ",
        "could not write output to ",
        "failed to write ",
    ]
    .iter()
    .any(|write| diagnostic.starts_with(write))
    .then_some(())?;
    [
        ("operation not permitted", "Operation not permitted"),
        ("permission denied", "Permission denied"),
    ]
    .into_iter()
    .find_map(|(needle, words)| diagnostic.contains(needle).then_some(words))
}

pub(super) fn backticked(text: &str) -> Option<String> {
    let (name, _) = text.strip_prefix('`')?.split_once('`')?;
    Some(name.to_string())
}

/// The terminating signal a cargo process line reports, as in
/// ``process didn't exit successfully: `rustc …` (signal: 15, SIGTERM: termination signal)``.
/// The line is often cut at its head in a stored tail, so only its end is read.
pub(super) fn process_signal(line: &str) -> Option<&'static str> {
    let (_, after) = line.rsplit_once("(signal: ")?;
    let number = after.split(|c: char| !c.is_ascii_digit()).next()?;
    terminating_signal(number.parse().ok()?)
}

/// Compose the `output_tail` a check row stores, within [`OUTPUT_TAIL_CHARS`]:
/// the classification and its evidence (when the death was classified), then
/// the check's own `report` (when its result file carried one), then as much of
/// the output's tail as still fits. The tail is [`stream_tail`]: stdout and
/// stderr are bounded separately, so a failure block on one stream survives
/// any amount of chatter on the other, and within each stream a build error
/// printed above a long run of warnings survives the cut.
///
/// The report leads anyway, because a report is the check's own verdict and
/// should not compete with chatter on its own stream for the window.
///
/// The tail never carries the result-file line: it was parsed already, and on
/// a run with little stderr it would fill the window with JSON.
pub(super) fn classified_output_excerpt(
    output: &str,
    classification: Option<&FailureClassification>,
    parsed: Option<&ParsedCheckResult>,
) -> String {
    let chatter = without_result_artifact_lines(output);
    let report = parsed
        .and_then(|result| result.report.as_deref())
        .filter(|report| !report.trim().is_empty());
    if classification.is_none() && report.is_none() {
        return stream_tail(&chatter, OUTPUT_TAIL_CHARS);
    }
    let mut prefix = String::new();
    if let Some(classification) = classification {
        // Evidence indices were computed over the original output.
        let lines: Vec<&str> = output.lines().collect();
        let context = classification.evidence_line.map(|index| {
            let start = index.saturating_sub(2);
            let end = (index + 3).min(lines.len());
            lines[start..end].join("\n")
        });
        prefix.push_str(&classification.reason);
        if let Some(context) = context.filter(|context| !context.is_empty()) {
            prefix.push_str("\nEvidence:\n");
            prefix.push_str(&context);
        }
    }
    if let Some(report) = report {
        if !prefix.is_empty() {
            prefix.push_str("\n\n");
        }
        prefix.push_str(report.trim_end());
    }
    prefix.push_str("\n\nFinal output:\n");
    let remaining = OUTPUT_TAIL_CHARS.saturating_sub(prefix.chars().count());
    prefix.push_str(&stream_tail(&chatter, remaining));
    prefix.chars().take(OUTPUT_TAIL_CHARS).collect()
}

/// `output` without the executor's `CAIRN_CHECK_RESULT_JSON=` line(s).
pub(super) fn without_result_artifact_lines(output: &str) -> std::borrow::Cow<'_, str> {
    if !output.contains(RESULT_ARTIFACT_JSON_MARKER) {
        return std::borrow::Cow::Borrowed(output);
    }
    std::borrow::Cow::Owned(
        output
            .lines()
            .filter(|line| !line.trim_start().starts_with(RESULT_ARTIFACT_JSON_MARKER))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Append the shared build-cache service's advisory to a verdict — but only for
/// a failure the service can plausibly explain.
///
/// Two failure classes qualify. A check whose OWN output looked like an abnormal
/// build reaches [`CheckFailureKind::Infrastructure`] through
/// [`classify_check_failure`]'s evidence arms, which is exactly the shape a sick
/// cache daemon produces; it carries no [`SubstrateFailure`], so `substrate` is
/// `None`. And a dispatch refusal the service itself caused is marked
/// [`SubstrateFailure::implicating_build_service`] where it is composed.
///
/// Every other substrate failure — a cell's storage cleanup, a capacity
/// deadline, a cancellation, a lost result — is unrelated to the cache, and
/// attaching "this commonly causes failures of this kind" to one would assert a
/// cause that is not one, however sick the daemon happens to be at that moment.
/// The snapshot itself is operator vocabulary and rides the log record
/// regardless.
/// What the log says when an infrastructure failure carried no substrate
/// evidence at all — itself the finding, rather than a silently absent key.
pub(super) const NO_SUBSTRATE_DIAGNOSTIC: &str = "no substrate diagnostic recorded";

/// The operator-facing line for a check that failed on infrastructure.
///
/// The cause leads and the environment follows, because the reader's first
/// question is always "what broke" and only their second is "what else was
/// true at the time". Sorted into the environment object, the diagnostic
/// trailed kilobytes of build-service fingerprint and process table — present,
/// unreadable, and so unread: a `bun: command not found` that would have been
/// diagnosed at a glance instead went unnoticed across an evening of failed
/// review checks.
pub(super) fn infrastructure_failure_log_line(
    check: &str,
    substrate_diagnostic: Option<&str>,
    environment: serde_json::Value,
) -> String {
    format!(
        "check infrastructure failure: {check}: {}\nenvironment: {environment}",
        substrate_diagnostic.unwrap_or(NO_SUBSTRATE_DIAGNOSTIC)
    )
}

pub(super) fn with_build_service_advisory(
    output_tail: String,
    build_service: Option<&crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot>,
    substrate: Option<&SubstrateFailure>,
) -> String {
    if substrate.is_some_and(|failure| !failure.build_service_implicated()) {
        return output_tail;
    }
    let Some(advisory) = build_service.and_then(|snapshot| snapshot.agent_advisory()) else {
        return output_tail;
    };
    // The remainder resumes where the head stopped, so a short verdict is not
    // printed twice around the advisory.
    let head: String = output_tail.chars().take(1_500).collect();
    let remainder: String = output_tail.chars().skip(1_500).collect();
    let reserved = head.chars().count() + advisory.chars().count() + 4;
    let rest = tail(&remainder, OUTPUT_TAIL_CHARS.saturating_sub(reserved));
    if rest.is_empty() {
        format!("{head}\n\n{advisory}")
    } else {
        format!("{head}\n\n{advisory}\n\n{rest}")
    }
}

/// The operator-facing diagnostic for a build-service failure that must stop
/// check dispatch, or `None`. The agent-facing half of the same failure is
/// composed by [`SubstrateFailure`] at the call site.
pub(super) fn active_build_service_failure(
    snapshot: &crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot,
) -> Option<String> {
    snapshot
        .current_failure()
        .map(|failure| format!("sccache port conflict: {failure}"))
}

pub(super) fn append_sandbox_denial_evidence(
    output: &mut String,
    denials: &[cairn_common::executor_protocol::SandboxDenialEvidence],
) {
    for evidence in denials {
        if !output.is_empty() {
            output.push_str("\n\n");
        }
        let scope = match &evidence.denial {
            cairn_common::executor_protocol::SandboxDenial::Path(path) => {
                format!("path={path}")
            }
            cairn_common::executor_protocol::SandboxDenial::Command => "scope=command".to_string(),
        };
        output.push_str(&format!(
            "Cairn sandbox denial evidence: operation={}, {scope}, command={}, stream={}",
            evidence.operation.as_deref().unwrap_or("unknown"),
            evidence.command,
            evidence.stream_id,
        ));
    }
}

pub(super) fn append_tracked_modification_evidence(
    output: &mut String,
    evidence: Option<&cairn_common::executor_protocol::TrackedModificationEvidence>,
) {
    let Some(evidence) = evidence else { return };
    if !output.is_empty() {
        output.push_str("\n\n");
    }

    output.push_str(&format!(
        "note: check modified tracked paths: {} ({} files, +{} -{}); changes were discarded",
        evidence.paths.join(", "),
        evidence.files_changed,
        evidence.lines_added,
        evidence.lines_deleted,
    ));
    if let Some(failure) = &evidence.index_restore_failure {
        output.push_str(&format!("\nnote: {failure}"));
    }
}

/// What a check the executor stopped is recorded as: never a red and never a
/// reusable verdict. A stall names the request's own last output and how long
/// it was quiet, so the reader looks at this command rather than at whatever
/// the machine was doing when it was read; a cancellation is a withdrawal.
///
/// The executor's account of the command travels with the failure: the stall
/// is charged to the counter of the platform it happened on, and the result
/// names the machine.
pub(super) fn stopped_command_failure(
    stop: &cairn_common::executor_protocol::CommandStop,
    output: &str,
    provenance: cairn_common::executor_protocol::CellExecutionMeta,
    account: &crate::fleet::CellAccount,
) -> CheckExecutionFailure {
    let failure = match stop {
        cairn_common::executor_protocol::CommandStop::Silent { .. } => SubstrateFailure::new(
            SubstrateFailureShape::CommandStalled,
            format!(
                "stopped by its executor: {}. The process tree it left, if any, is listed under Stopped commands on the executor's page.

{output}",
                stop.describe()
            ),
        ),
        // Cairn ended the command itself: a cancellation, not a substrate
        // failure, so nothing is charged to the platform it ran on. A
        // preemption for an inference load is the one cancellation that is
        // presented again, and it names the machine it was taken from.
        cairn_common::executor_protocol::CommandStop::Cancelled => {
            match cancelled_check(cancel_cause(account)) {
                CheckExecutionFailure::Substrate(failure) => failure,
                cancelled => return cancelled,
            }
        }
        cairn_common::executor_protocol::CommandStop::OomKilled { .. } => SubstrateFailure::new(
            SubstrateFailureShape::CommandOomKilled,
            format!("{}.\n\n{output}", capitalized(&stop.describe())),
        ),
    };
    CheckExecutionFailure::Substrate(failure.ran_as(provenance))
}

pub(super) fn capitalized(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// The cause Cairn recorded for a cancellation the executor reported. The
/// executor's outcome carries none, and one that went unrecorded is still a
/// cancellation rather than a failure.
pub(super) fn cancel_cause(account: &crate::fleet::CellAccount) -> crate::fleet::CancelCause {
    account
        .cancel_cause
        .clone()
        .unwrap_or(crate::fleet::CancelCause::Unrecorded)
}

/// A cancellation the runner recorded, as a check's result. Preempted for an
/// inference load is `CommandPreempted`, naming the model and the measured
/// memory it needed, which the capacity loop presents again at once; every
/// other cause is the cancellation it is, and is never stored.
pub(super) fn cancelled_check(cause: crate::fleet::CancelCause) -> CheckExecutionFailure {
    match cause {
        crate::fleet::CancelCause::Preempted(preemption) => {
            CheckExecutionFailure::Substrate(SubstrateFailure::new(
                SubstrateFailureShape::CommandPreempted,
                format!("{}.", capitalized(&preemption.sentence())),
            ))
        }
        cause => CheckExecutionFailure::Cancelled(cause),
    }
}

/// What a coalesced wait delivered, as a check result.
pub(super) fn check_result_from_delivery(
    delivery: crate::fleet::CoalescedDelivery,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    use crate::fleet::CoalescedWaitError;
    match delivery {
        Ok(submitted) => check_result_from_cell_outcome(
            submitted.outcome,
            &submitted.account,
            Some(submitted.publication),
        ),
        Err(CoalescedWaitError::Refused(outcome)) => {
            check_result_from_cell_outcome(*outcome, &crate::fleet::CellAccount::default(), None)
        }
        Err(CoalescedWaitError::Cancelled(cause)) => Err(cancelled_check(cause)),
        Err(CoalescedWaitError::LeaderVanished) => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::RunnerFault,
            "the shared execution's leader ended without publishing a terminal outcome",
        )),
        Err(CoalescedWaitError::Orphaned) => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::RunnerFault,
            "the coalesced result channel closed with nothing sent",
        )),
    }
}

pub(crate) fn check_result_from_cell_outcome(
    outcome: CellOutcome,
    account: &crate::fleet::CellAccount,
    publication: Option<crate::fleet::PublicationCoordination>,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    match (
        outcome_result(outcome, account, publication),
        &account.placement,
    ) {
        (Err(CheckExecutionFailure::Substrate(failure)), Some(placement)) => Err(
            CheckExecutionFailure::Substrate(failure.on_machine(placement.clone())),
        ),
        (result, _) => result,
    }
}

pub(super) fn outcome_result(
    outcome: CellOutcome,
    account: &crate::fleet::CellAccount,
    publication: Option<crate::fleet::PublicationCoordination>,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    if let Some(attempt) = account.left_running.clone() {
        return Err(CheckExecutionFailure::LeftRunning(attempt));
    }
    match outcome {
        CellOutcome::Completed {
            output, metadata, ..
        } if metadata.stop.is_some() => Err(stopped_command_failure(
            metadata.stop.as_ref().expect("guarded"),
            &output,
            metadata.clone(),
            account,
        )),
        CellOutcome::Completed {
            exit_code,
            mut output,
            timed_out,
            metadata,
            mutation_delta: None,
            sandbox_denials,
            tracked_modifications,
            ..
        } => {
            let duration_ms = metadata
                .duration_ms
                .map(|duration| i64::try_from(duration).unwrap_or(i64::MAX));
            append_sandbox_denial_evidence(&mut output, &sandbox_denials);
            append_tracked_modification_evidence(&mut output, tracked_modifications.as_ref());
            Ok(CheckExecResult {
                exit_code,
                output,
                timed_out,
                duration_ms,
                provenance: Some(metadata),
                publication,
            })
        }
        CellOutcome::Completed {
            mutation_delta: Some(delta),
            ..
        } => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Result,
            format!(
                "cell produced mutation delta {} based on {}",
                delta.delta_commit, delta.base_commit
            ),
        )),
        CellOutcome::FailedAfterExecution { diagnostic, .. } => {
            Err(CheckExecutionFailure::substrate(
                SubstrateFailureShape::Result,
                format!("the command ran, but its result could not be reported: {diagnostic}"),
            ))
        }
        CellOutcome::Unavailable {
            reason:
                cairn_common::executor_protocol::CellUnavailableReason::Deadline {
                    host_pressure,
                    substrate: Some(substrate),
                },
            diagnostic,
        } => {
            let now = unix_time_ms_for_checks();
            let mut facts = vec![format!(
                "substrate={:?}, lastProgressAge={}ms",
                substrate.state,
                now.saturating_sub(substrate.last_progress_unix_ms)
            )];
            if let Some(depth) = substrate.queue_depth {
                facts.push(format!("queueDepth={depth}"));
            }
            if let Some(position) = substrate.queue_position {
                facts.push(format!("queuePosition={position}"));
            }
            if let Some(active) = substrate.active_cell_count {
                facts.push(format!("activeSlots={active}"));
            }
            if let Some(started) = substrate.oldest_running_started_at_unix_ms {
                facts.push(format!(
                    "oldestRunningAge={}ms",
                    now.saturating_sub(started)
                ));
            }
            // The shape is what the evidence says the wait was on, read the way
            // every other deadline is: a working machine is capacity, a
            // stalled or draining one is not, and calling the second
            // "capacity unavailable" sends the reader to placement.
            let shape = deadline_shape(host_pressure.as_ref(), Some(&substrate));
            if let Some(pressure) = host_pressure {
                facts.push(format!("hostPressure={pressure:?}"));
            }
            Err(CheckExecutionFailure::substrate(
                shape,
                format!("build-slot deadline ({}) — {diagnostic}", facts.join(", ")),
            ))
        }
        CellOutcome::Unavailable { reason, diagnostic } => match no_start(&reason) {
            NoStart::Cancelled(cause) => Err(CheckExecutionFailure::Cancelled(cause)),
            NoStart::Substrate(shape) => {
                // "Lost contact" needs a machine that had the work. Before any
                // claim there was no contact to lose: the fleet had nowhere
                // to send it.
                let shape = match (shape, &account.placement) {
                    (SubstrateFailureShape::MachineUnreachable, None) => {
                        SubstrateFailureShape::NoReachableMachine
                    }
                    (shape, _) => shape,
                };
                // A silent claim's diagnostic already names the machine, the
                // silence, and what the machine said last; a `Debug` rendering
                // of the reason would only repeat it less readably.
                let diagnostic = if let cairn_common::executor_protocol::CellUnavailableReason::NoMatchingExecutor { rejected } = &reason {
                    let transient = rejected.iter()
                        .filter(|machine| transient_placement_refusal(&machine.reason))
                        .map(|machine| format!("{} ({})", machine.executor_name, machine.reason.describe()))
                        .collect::<Vec<_>>()
                        .join("; ");
                    if transient.is_empty() {
                        format!("NoMatchingExecutor: {diagnostic}")
                    } else {
                        format!("This check will be retried: temporarily unavailable machines: {transient}.\n{diagnostic}")
                    }
                } else if matches!(
                    reason,
                    cairn_common::executor_protocol::CellUnavailableReason::ClaimedAttemptSilent { .. }
                ) {
                    diagnostic
                } else {
                    format!("{reason:?}: {diagnostic}")
                };
                let mut failure = SubstrateFailure::new(shape, diagnostic);
                failure.placement_refused = matches!(
                    reason,
                    cairn_common::executor_protocol::CellUnavailableReason::NoMatchingExecutor { .. }
                );
                Err(CheckExecutionFailure::Substrate(failure))
            }
        },
        CellOutcome::StorageFailure {
            stage,
            kind,
            diagnostic,
            ..
        } => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Storage,
            format!("storage failure ({stage:?}/{kind:?}): {diagnostic}"),
        )),
        CellOutcome::Cancelled { .. } => Err(cancelled_check(cancel_cause(account))),
    }
}
