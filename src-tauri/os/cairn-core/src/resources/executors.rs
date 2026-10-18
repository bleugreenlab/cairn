//! The `cairn://executors` family: the fleet as an agent reads it.
//!
//! Two representations over one cached projection. The collection answers "what
//! machines exist and what can they do" — the question an agent has before it
//! decides a batch belongs somewhere specific. The item answers "what is that
//! machine doing right now, and is anything wrong with it" — the question behind
//! a placement refusal.
//!
//! Three properties hold across both, and each is load-bearing:
//!
//! - **Names, never identities.** A machine is addressed here by exactly the
//!   name a placement request accepts, so what an agent can read is what it can
//!   target. Opaque executor and device identifiers stay inside the runner; an
//!   address an agent cannot discover is not an address.
//! - **Timestamped readings or named gaps.** Every machine number is a
//!   [`Measurement`], rendered with the instant it was taken or with the reason
//!   there is no reading. A gap is never printed as a zero: "this platform
//!   cannot answer" and "this machine has no memory left" are opposite facts.
//! - **Cached state only.** The read serves what the runner already holds. It
//!   never probes an executor and never provokes fleet-wide sampling, so
//!   inspecting the fleet costs nothing that running work would notice.

use cairn_common::executor_protocol::{
    BindingEvent, DeclineReason, ExecutorHealthStatus, ExecutorInspection, MachineRole,
    Measurement, MeasurementReading, PlacementDecision, PlacementOutcome, PlacementPrediction,
    PlacementReason, PlacementRejectionReason, PlacementSyncCost, ReservationRationale,
    MIN_CONFIDENT_RESERVATION_SAMPLES,
};
use cairn_common::executor_protocol::{
    DiskDecline, DiskReliefOutlook, OccupancyLineageReport, OccupancyRule, OccupiedTreeSkip,
    QuarantineTargetStrip, RetiredHomeAccounting, RetiredHomeState, SkippedEntry,
    StorageReclaimRung, StorageRungReclaim, StorageWithheld, StorageWithheldReason,
};

/// This runner's own private agent-session lanes. Kept at collection scope
/// because sessions run on the runner, not on any executor; rendered from the
/// promoter's last recorded decision, never a fresh probe.
pub(crate) fn render_session_admission(
    lanes: &[cairn_db::storage::LaneBudget],
    now_unix_secs: i64,
) -> String {
    let mut out = String::from(
        "\n## Session admission\n\nPrivate agent sessions on this runner are admitted against measured host memory, keeping free the same reserve the warm-process collector keeps. An operator ceiling, when set, also bounds each lane.\n\n",
    );
    if lanes.is_empty() {
        out.push_str("No session lane holds queued or running work.\n");
        return out;
    }
    for lane in lanes {
        out.push_str(&format!(
            "- {}\n",
            crate::execution::jobs::describe_lane(lane, now_unix_secs)
        ));
        if let Some(shortfall) = &lane.shortfall {
            out.push_str(&format!(
                "  - Queue head waiting because {}.\n",
                crate::execution::jobs::describe_shortfall(shortfall)
            ));
        }
    }
    out
}

/// The identity a bootstrap writes into every resident executor.
///
/// Kept at collection scope because it belongs to the runner, not to any one
/// machine. An operator can compare this value with a remote daemon's refusal
/// without reading runner logs or guessing which configured endpoint won.
pub(crate) fn render_listener_identity(listener: Option<&ExecutorListenerAdvertisement>) -> String {
    match listener {
        Some(listener) => format!(
            "\n## Executor listener\n\n- Endpoints: {}\n- Certificate SHA-256: {}\n",
            listener.endpoints.join(", "),
            listener.cert_sha256.join(", ")
        ),
        None => "\n## Executor listener\n\nNot configured; this runner accepts no resident remote executors.\n".to_string(),
    }
}
// Deliberately its own statement rather than folded into the import above. That
// list is edited by whoever touches the reservation rendering; keeping the
// fleet-visibility types on a separate line means two branches working on this
// file do not collide on one line neither of them cares about.
use cairn_common::executor_protocol::{
    AllocatorState, BuildPrerequisiteStatus, CpuAdmissionHealth, CpuAdmissionState, EnrolledRemote,
    ExecutorCapabilities, ExecutorListenerAdvertisement, GenerationEndMemory, RemoteArtifactFault,
    RemoteAttachAttempt, RemoteConnectionPhase, RemoteConnectionTransition, RemoteLinkState,
    UpdateReport, UpdateState, CPU_ADMISSION_BLIND_AFTER_MS, MACHINE_READING_STALE_AFTER_MS,
};

use cairn_common::abnormal_exit::{crash_report_for, AbnormalExit};
use cairn_common::executor_protocol::{
    ColocatedGenerationHold, ColocatedSupervision, ExecutorRetirementSummary,
    ExecutorSubstrateEvidence, ExecutorSubstrateState, GenerationHoldReason, RetirementOrigin,
    RetirementOutcome, RetirementPhase, WorkloadSurvivor,
};
use cairn_common::executor_protocol::{
    GpuDevice, GpuMemory, MachineMemory, UnifiedAllocationSource, UnifiedPool,
};

use crate::fleet::management::{EnrollmentCleanup, EnrollmentOperation};

/// The runner's own abnormal restart, when it had one.
///
/// Every machine in this resource attaches to one runner, so a runner that
/// killed itself is a fleet-level fact: it is why links reset, why work was
/// interrupted, and why an executor's heartbeat age reads younger than the work
/// it was doing. Rendering it first is the same rule the machines already follow
/// -- a reading or a named gap, never silence. A runner that came up cleanly has
/// nothing to say here and says nothing.
fn runner_restart_section(exit: Option<&AbnormalExit>, captured_at_unix_ms: u64) -> String {
    let Some(exit) = exit else {
        return String::new();
    };
    let restarted_at = crate::clock::stamp_millis_with_seconds(exit.at_unix_ms as i64)
        .unwrap_or_else(|| "an unknown time".to_string());
    let mut out = String::from("## Runner restarted\n\nThis runner replaced one that exited abnormally. Work in flight at that moment was interrupted.\n\n");
    out.push_str(&format!(
        "- Restarted: {restarted_at} ({})\n",
        age(exit.elapsed_ms(captured_at_unix_ms))
    ));
    out.push_str(&format!("- Cause: {}\n", exit.reason));
    out.push_str(&format!("- Predecessor pid: {}\n", exit.pid));
    // The report is resolved now rather than stored, because the OS writes it
    // seconds after the abort -- after the successor has already booted.
    match crash_report_for(exit) {
        Some(path) => out.push_str(&format!("- Crash report: {}\n\n", path.display())),
        None => out.push_str(
            "- Crash report: none found for this exit (the platform may not write one)\n\n",
        ),
    }

    out
}

pub(crate) fn render_attach_log(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    let mut out = format!("# Executor {} attach log\n\n", remote.name);
    let Some(attempt) = &remote.last_attempt else {
        out.push_str("No attach attempt has completed since the runner started.\n");
        return out;
    };
    out.push_str(&format!(
        "- Attempted: {}\n",
        age(captured_at_unix_ms.saturating_sub(attempt.attempted_at_unix_ms))
    ));
    // The same helper the summary line uses. `duration_ms` renders a decimal
    // minute, so one uptime read as "7m" beside the machine and "7.0m" in its
    // own log — two spellings of one quantity, which this test has been failing
    // on. Uptime has one rendering.
    if let Some(uptime) = attempt.held_for_ms {
        out.push_str(&format!("- Link held: {}\n", held(uptime)));
    }
    out.push_str(&format!("- Cause: {}\n", attempt.summary));
    out.push_str(&retirement_lines(&remote.retired));
    out.push('\n');
    match &attempt.detail {
        Some(detail) => out.push_str(&format!("```text\n{detail}\n```\n")),
        None => out.push_str("The runner recorded no session output beyond the cause above.\n"),
    }
    out
}

/// What retiring a machine's pre-inversion executor found and did, as the
/// attach log states it. Empty when no retirement has run.
pub(crate) fn retirement_lines(retired: &[String]) -> String {
    match retired {
        [] => String::new(),
        [only] => format!("- Retired: {only}\n"),
        lines => {
            let mut out = String::from("- Retired:\n");
            for line in lines {
                out.push_str(&format!("  - {line}\n"));
            }
            out
        }
    }
}

fn render_connection_timeline(
    transitions: &[RemoteConnectionTransition],
    captured_at_unix_ms: u64,
) -> String {
    if transitions.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n## Recent connection transitions\n");
    for transition in transitions {
        let mut evidence = Vec::new();
        if let Some(generation) = transition.generation {
            evidence.push(format!("generation {generation}"));
        }
        if let Some(status) = transition.remote_process_exit_status {
            evidence.push(format!("remote process exit {status}"));
        }
        if let Some(reason) = &transition.reason {
            evidence.push(reason.clone());
        }
        // Before the stderr, which can run to many lines: the size a machine
        // died at is the first thing an operator investigating a kill is
        // looking for, and it must not be buried under a log tail.
        if let Some(memory) = &transition.memory_at_generation_end {
            evidence.push(generation_end_memory_summary(
                memory,
                transition.occurred_at_unix_ms,
            ));
        }
        if let Some(stderr) = &transition.last_stderr {
            evidence.push(format!("stderr: {}", stderr.trim()));
        }
        out.push_str(&format!(
            "- {}: {}{}\n",
            age(captured_at_unix_ms.saturating_sub(transition.occurred_at_unix_ms)),
            connection_phase_label(transition.phase),
            if evidence.is_empty() {
                String::new()
            } else {
                format!(" — {}", evidence.join("; "))
            }
        ));
    }
    out.push('\n');
    out
}

/// How big the executor process was when its generation ended.
///
/// The one line a machine killed for its size could not previously produce
/// anywhere: its own telemetry died with it, and the process that replaced it
/// reported a fresh start. Both the size and the allocator split are here,
/// because after the fact they are read together — the size says whether memory
/// was the cause, and the split says where the fix belongs.
///
/// The age stated is the reading's own, from when the executor took it, not
/// from when the runner received the heartbeat carrying it. A sampler starves
/// under the pressure that ends the process, so the last heartbeat can be
/// recent while the reading inside it is minutes old. Aging the reading by the
/// heartbeat printed "measured 1s before it ended" beside a 2 GiB reading of a
/// process the kernel killed at 10 GiB (CAIRN-4627).
///
/// Past the sampler's own bound the reading is not a size at all: it is the
/// last thing the executor measured before it stopped measuring, and it is
/// rendered as that.
fn generation_end_memory_summary(memory: &GenerationEndMemory, occurred_at_unix_ms: u64) -> String {
    let resident = &memory.process.resident_bytes;
    let Some(size) = resident.value() else {
        return "memory at generation end: resident size unmeasured".to_string();
    };
    let reading_age_ms = occurred_at_unix_ms.saturating_sub(resident.measured_at_unix_ms);
    let before_end = |ms: u64| age(ms).trim_end_matches(" ago").to_string();
    if reading_age_ms > MACHINE_READING_STALE_AFTER_MS {
        return format!(
            "memory at generation end: unknown — last reading {} was taken {} before it ended, \
             past the {} sampler bound; the executor had stopped measuring",
            bytes(*size),
            before_end(reading_age_ms),
            before_end(MACHINE_READING_STALE_AFTER_MS),
        );
    }
    // The allocator split is read beside the size, so it has to be as fresh as
    // the size to mean anything next to it.
    let allocator = &memory.process.allocator;
    let allocator = match allocator.value() {
        Some(state)
            if occurred_at_unix_ms.saturating_sub(allocator.measured_at_unix_ms)
                <= MACHINE_READING_STALE_AFTER_MS =>
        {
            format!(", allocator {}", allocator_split(state))
        }
        _ => String::new(),
    };
    format!(
        "memory at generation end: resident {}{allocator} (measured {} before it ended)",
        bytes(*size),
        before_end(reading_age_ms),
    )
}

fn connection_phase_label(phase: RemoteConnectionPhase) -> &'static str {
    match phase {
        RemoteConnectionPhase::Attempting => "attempting connection",
        RemoteConnectionPhase::CredentialRecovered => "credential recovered",
        RemoteConnectionPhase::ProtocolReady => "protocol ready",
        RemoteConnectionPhase::ProtocolReadyTimedOut => "protocol Ready timed out",
        RemoteConnectionPhase::HeartbeatLost => "heartbeat lost",
        RemoteConnectionPhase::TelemetryStalled => "telemetry stalled",
        RemoteConnectionPhase::Disconnected => "protocol disconnected",
        RemoteConnectionPhase::RemoteProcessExited => "remote executor exited",
        RemoteConnectionPhase::RetryScheduled => "retry scheduled",
        RemoteConnectionPhase::EnvironmentEvicted => "execution environment rebuilt elsewhere",
    }
}

/// A machine waiting on a publication is not a machine with a problem, and the
/// two get different registers below.
///
/// Render the fleet collection: one line per machine, plus the toolchains and
/// load an agent weighs before targeting one.
///
/// Enrolled machines that are not attached are listed too, under their own
/// heading. They cannot be targeted, but omitting them is what let three
/// machines disappear from this resource for hours while their enrollments sat
/// intact: an empty fleet and a fleet whose machines are all unreachable are
/// opposite facts, and this rendering has to be able to tell them apart.
pub(crate) fn render_executors(
    executors: &[ExecutorInspection],
    enrolled: &[EnrolledRemote],
    enrolling: &[EnrollmentOperation],
    replaced_abnormal_exit: Option<&AbnormalExit>,
    captured_at_unix_ms: u64,
    warming_up: bool,
) -> String {
    if executors.is_empty() && enrolled.is_empty() && enrolling.is_empty() {
        let mut empty = if warming_up {
            String::from("# Executors\n\nStatus: warming\n\nWaiting quietly for the first executor heartbeat in this runner session.\n")
        } else {
            String::from("# Executors\n\nNo executor is attached to this runner.\n\nThe runner supervises a colocated executor named `local`; if nothing is listed here it is not currently attached. Enrolled machines are added with `cairn executor add <user@host>`.\n")
        };
        // Deliberately rendered even here. An empty fleet right after a runner
        // abort is not a quiet fleet; it is the aftermath, and that is precisely
        // the moment the restart explains what is being read.
        empty.push_str(&runner_restart_section(
            replaced_abnormal_exit,
            captured_at_unix_ms,
        ));
        return empty;
    }
    let mut out = String::from("# Executors\n\n");
    out.push_str(&runner_restart_section(
        replaced_abnormal_exit,
        captured_at_unix_ms,
    ));
    if executors.is_empty() && warming_up {
        out.push_str("Status: warming\n\nWaiting quietly for the first executor heartbeat in this runner session.\n\n");
    } else if executors.is_empty() {
        out.push_str(
            "No executor is attached to this runner, but it is enrolled with machines that are not reporting. Nothing can be placed until one attaches.\n\n",
        );
    } else {
        out.push_str(&format!(
            "{} attached to this runner. These names are what a placement request accepts.\n\n",
            plural(executors.len(), "executor", "executors")
        ));
    }
    for executor in executors {
        out.push_str(&format!("## {}\n", executor.name));
        let capabilities = &executor.health.advertisement.capabilities;
        out.push_str(&format!(
            "- Platform: {} / {} ({} logical cores)\n",
            capabilities.os, capabilities.arch, capabilities.logical_cores
        ));
        out.push_str(&format!(
            "- Toolchains: {}\n",
            toolchain_summary(capabilities)
        ));
        if let Some(summary) = build_prerequisite_summary(capabilities) {
            out.push_str(&format!("- Build prerequisites: {summary}\n"));
        }
        for eligibility in &executor.project_eligibility {
            if let Some(why) = ineligibility_summary(&eligibility.refusals) {
                out.push_str(&format!(
                    "- Not eligible for {}: {why}\n",
                    eligibility.project_id
                ));
            }
        }
        out.push_str(&format!("- Link: {}\n", link_summary(executor)));
        if executor.health.role != MachineRole::General {
            out.push_str(&machine_role_line(executor.health.role));
        }
        if executor.health.drain_mode {
            out.push_str("- Draining: yes (refusing new work)\n");
        }
        out.push_str(&format!(
            "- Running now: {} (queued {})\n",
            executor.occupancy.executing_requests.len(),
            executor.occupancy.queued_requests.len()
        ));
        out.push_str(&format!("- Read: cairn://executors/{}\n\n", executor.name));
    }
    if !enrolled.is_empty() {
        out.push_str(&format!(
            "## Enrolled, not attached ({})\n\nThese machines are enrolled with this runner and are not reporting. They cannot be targeted until they attach.\n\n",
            enrolled.len()
        ));
        for remote in enrolled {
            out.push_str(&format!("### {}\n", remote.name));
            out.push_str(&format!("- Platform: {} / {}\n", remote.os, remote.arch));
            out.push_str(&format!("- Link: {}\n", enrolled_link_summary(remote)));
            if remote.role != MachineRole::General {
                out.push_str(&machine_role_line(remote.role));
            }
            if remote.drained {
                out.push_str("- Draining: yes (held by the runner)\n");
            }
            out.push_str(&format!(
                "- Last seen: {}\n",
                last_seen(remote, captured_at_unix_ms)
            ));
            out.push_str(&format!(
                "- Last attempt: {}\n",
                last_attempt_age(remote, captured_at_unix_ms)
            ));
            if remote
                .last_attempt
                .as_ref()
                .is_some_and(|attempt| attempt.detail.is_some())
            {
                out.push_str(&format!(
                    "- Full attach log: cairn://executors/{}?view=attach-log\n",
                    remote.name
                ));
            }
            out.push_str(&format!("- Read: cairn://executors/{}\n\n", remote.name));
        }
    }

    if !enrolling.is_empty() {
        out.push_str(&format!(
            "## Enrolling now ({})\n\nThese machines are being brought up. They are not targetable until they report ready.\n\n",
            enrolling.len()
        ));
        for operation in enrolling {
            out.push_str(&format!("### {}\n", operation.name));
            out.push_str(&format!(
                "- Phase: {} ({})\n",
                operation.phase.label(),
                age(operation.elapsed_ms(captured_at_unix_ms))
            ));
            out.push_str(&format!("- Read: {}\n\n", operation.uri));
        }
    }
    out.push_str("Target one with `run({executor:{name:\"<name>\"}})`, or any machine on a platform with `run({executor:{os:\"linux\"}})`.\n");
    out
}

/// Render an enrollment that is still running, or the one that most recently
/// failed, for a name that has no machine behind it yet.
///
/// This is what makes an enrollment legible while it happens: the alternative is
/// a name that reads as unknown for the several minutes an SSH bootstrap takes,
/// which is indistinguishable from a name that was never enrolled at all.
pub(crate) fn render_enrollment(
    operation: &EnrollmentOperation,
    captured_at_unix_ms: u64,
) -> String {
    let mut out = format!("# Executor {}\n\n", operation.name);
    out.push_str("## Enrollment\n");
    out.push_str(&format!("- Phase: {}\n", operation.phase.label()));
    out.push_str(&format!(
        "- Elapsed: {}\n",
        age(operation.elapsed_ms(captured_at_unix_ms))
    ));
    out.push_str(&format!("- Operation: {}\n", operation.id));
    if let Some(diagnostic) = &operation.diagnostic {
        out.push_str(&format!("- Diagnostic: {diagnostic}\n"));
    }
    out.push_str(&format!(
        "- Rollback: {}\n\n",
        match operation.cleanup {
            EnrollmentCleanup::NotApplicable => "nothing to roll back",
            EnrollmentCleanup::Complete => "complete — nothing was left on the host",
            EnrollmentCleanup::Incomplete =>
                "incomplete — remove this machine to clear what the rollback could not",
        }
    ));
    out.push_str(if operation.phase.is_terminal() {
        "This enrollment has finished. Nothing is placed here; the machine either attached or did not.\n"
    } else {
        "This machine is being enrolled. No load, admission, or occupancy is reported: those are measured on the machine, and it is not reporting yet.\n"
    });
    out
}

/// Render one enrolled machine that is not attached.
///
/// Deliberately short. Everything the full executor rendering shows — load,
/// admission, queues, the work resident on a machine — comes from the machine
/// itself, and none of it was measured. Printing those sections as zeroes would
/// describe an idle machine rather than an absent one.
/// What a machine is for, and what that means for today's classes of work,
/// derived from the role's own [`MachineRole::accepts`].
fn machine_role_line(role: MachineRole) -> String {
    format!("- Role: {} ({})\n", role.label(), role.describe_accepted())
}

pub(crate) fn render_enrolled_remote(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    let mut out = format!("# Executor {}\n\n", remote.name);
    out.push_str("## Identity\n");
    out.push_str(&format!("- Name: {}\n", remote.name));
    out.push_str("- Attachment: enrolled (attached over the executor protocol)\n");
    out.push_str(&machine_role_line(remote.role));
    out.push_str(&format!("- Platform: {} / {}\n\n", remote.os, remote.arch));

    out.push_str("## Link\n");
    out.push_str(&format!("- Status: {}\n", enrolled_link_summary(remote)));
    // Draining is runner-side state, so it is one of the few things worth
    // saying about a machine that is reporting nothing: it decides what happens
    // when this one comes back.
    if remote.drained {
        out.push_str("- Draining: yes — held by the runner; when this machine attaches it will be told, and no new work is placed here until it is cleared\n");
    }
    out.push_str(&format!(
        "- Last seen: {}\n",
        last_seen(remote, captured_at_unix_ms)
    ));
    out.push_str(&format!(
        "- Last attempt: {}\n",
        last_attempt_age(remote, captured_at_unix_ms)
    ));
    out.push_str(&render_connection_timeline(
        &remote.connection_timeline,
        captured_at_unix_ms,
    ));
    if remote
        .last_attempt
        .as_ref()
        .is_some_and(|attempt| attempt.detail.is_some())
    {
        out.push_str(&format!(
            "- Full attach log: cairn://executors/{}?view=attach-log\n",
            remote.name
        ));
    }
    out.push('\n');

    out.push_str(match &remote.link {
        RemoteLinkState::Bootstrapping { .. } => {
            "This machine is being bootstrapped: the runner is installing the executor and registering it as a service over SSH. That is a one-time ceremony; once it finishes the machine connects on its own and the runner stops reaching into it.\n"
        }
        // Waiting for a publication is not a fault and must not be painted as
        // one: nobody has to do anything, and it clears itself when CI lands
        // the artifact.
        RemoteLinkState::BootstrapFailed { .. }
            if remote
                .last_attempt
                .as_ref()
                .is_some_and(is_artifact_publish_wait) =>
        {
            "This machine is enrolled and waiting for the runner's checksummed executor release. Re-run the enrollment once publication completes; nothing about the host needs fixing.\n"
        }
        RemoteLinkState::BootstrapFailed { .. } => {
            "The bootstrap ceremony did not complete, so this machine has no resident executor to dial in with. The cause above is the runner's account of what stopped it; nothing retries on its own, because nothing is installed to retry. Re-run the enrollment once the cause is fixed.\n"
        }
        RemoteLinkState::BootstrapHeld { .. } => {
            "This machine's bootstrap ceremony is held by the runner, not by the machine. Nothing about this host needs fixing and no attempt has been spent on it: the ceremony runs by itself as soon as the runner's executor listener is serving an endpoint a machine can dial.\n"
        }
        RemoteLinkState::NeverConnected { .. } => {
            "This machine is installed and has not dialled in yet. That is not a failure: the executor service was registered and the machine reaches the runner on its own, so this ordinarily resolves within a minute of the service starting. If it persists, look at the executor's own log on that host — a refused certificate pin or an endpoint it cannot reach is reported there, not here.\n"
        }
        RemoteLinkState::Connected { .. } => {
            "This machine is connected but is not reporting executor facts yet.\n"
        }
        RemoteLinkState::Absent { last_beat_unix_ms, .. } if last_beat_unix_ms.is_none() => {
            "This machine is enrolled and has not been seen since this runner started. That is ordinary: the runner does not reach into it, so nothing has happened yet in either direction. It will appear when its executor dials in.\n"
        }
        RemoteLinkState::Absent { .. } => {
            "This machine is enrolled and is not connected right now. That is ordinary when it is powered off, asleep, or away from the network this runner lives on. Nothing here is waiting to be repaired: its executor reconnects on its own, and the runner takes no action to bring it back.\n"
        }
    });
    out.push_str("\nNo load, admission, or occupancy is reported: those are measured on the machine, and this one is not reporting.\n");
    out
}

/// One machine's link condition in a single line.
fn enrolled_link_summary(remote: &EnrolledRemote) -> String {
    let attempt = remote.last_attempt.as_ref();
    let summary = attempt.map(|attempt| attempt.summary.as_str());
    match &remote.link {
        RemoteLinkState::Bootstrapping { phase } => {
            format!("bootstrapping — {}", bootstrap_phase_label(*phase))
        }
        // An artifact this fleet is merely waiting on speaks for itself; adding
        // "bootstrap failed" in front of it would put a fault register on a
        // condition nobody has to act on.
        RemoteLinkState::BootstrapFailed { .. }
            if attempt.is_some_and(is_artifact_publish_wait) =>
        {
            summary.unwrap_or_default().to_string()
        }
        RemoteLinkState::BootstrapFailed { cause } => {
            let cause = if cause.is_empty() {
                summary.unwrap_or("the ceremony did not complete")
            } else {
                cause.as_str()
            };
            format!("bootstrap failed — {cause}")
        }
        RemoteLinkState::BootstrapHeld { precondition } => {
            format!("held by the runner — {}", precondition.describe())
        }
        RemoteLinkState::NeverConnected { .. } => summary
            .map(|summary| format!("installed; waiting for first dial — {summary}"))
            .unwrap_or_else(|| "installed; waiting for first dial".to_string()),
        RemoteLinkState::Connected { generation, .. } => {
            format!("connected — generation {generation}")
        }
        // A link that HELD and then went away says how long it held. That is
        // the difference between a machine that flapped on arrival and one that
        // worked for hours before its network went, and they are investigated
        // differently.
        RemoteLinkState::Absent { last_reason, .. } => match attempt
            .and_then(|attempt| Some((attempt.held_for_ms?, attempt.summary.as_str())))
        {
            Some((held, summary)) => format!("{} — {summary}", link_loss_phrase(held)),
            None if last_reason.is_empty() => "absent".to_string(),
            None => format!("absent — {last_reason}"),
        },
    }
}

/// An attempt that is waiting on a publication rather than reporting a fault.
fn is_artifact_publish_wait(attempt: &RemoteAttachAttempt) -> bool {
    attempt.artifact_fault == Some(RemoteArtifactFault::Unpublished)
}

const LINK_STABILITY_MS: u64 = 60_000;

fn link_loss_phrase(held_for_ms: u64) -> String {
    if held_for_ms >= LINK_STABILITY_MS {
        format!("link lost after {} of healthy operation", held(held_for_ms))
    } else {
        format!("link lost {} after attaching", held(held_for_ms))
    }
}

fn bootstrap_phase_label(phase: cairn_common::executor_protocol::BootstrapPhase) -> &'static str {
    use cairn_common::executor_protocol::BootstrapPhase;
    match phase {
        BootstrapPhase::Probing => "asking the host what it is",
        BootstrapPhase::Installing => "installing the executor binary",
        BootstrapPhase::VerifyingReach => "verifying it can reach this runner",
        BootstrapPhase::Configuring => "writing its configuration and credential",
        BootstrapPhase::Registering => "registering its service",
        BootstrapPhase::AwaitingFirstDial => "waiting for it to dial in",
    }
}

fn last_seen(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    match remote.last_seen_unix_ms {
        Some(seen) => age(captured_at_unix_ms.saturating_sub(seen)),
        None => "never attached since the runner started".to_string(),
    }
}

fn last_attempt_age(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    match &remote.last_attempt {
        Some(attempt) => age(captured_at_unix_ms.saturating_sub(attempt.attempted_at_unix_ms)),
        None => "none completed yet".to_string(),
    }
}

fn held(ms: u64) -> String {
    age(ms).trim_end_matches(" ago").to_string()
}

/// Render the durable result of the resident executor's most recent update.
///
/// Kept independent of the fleet storage location: the link owns updating the
/// cached report, while every attached and absent-machine view needs one spelling
/// of its operational meaning.
pub(crate) fn render_update_report(
    report: Option<&UpdateReport>,
    captured_at_unix_ms: u64,
) -> String {
    let Some(report) = report else {
        return String::new();
    };
    let state = match report.state {
        UpdateState::Pending => "pending",
        UpdateState::Succeeded => "succeeded",
        UpdateState::Failed => "failed",
    };
    let mut out = format!(
        "- Last update: {state}, {} → {} (build {}), {}\n",
        report.from_build,
        report.to_distribution_id,
        report.to_build,
        age(captured_at_unix_ms.saturating_sub(report.at_unix_ms)),
    );
    if report.state == UpdateState::Failed {
        if let Some(cause) = report.cause.as_deref() {
            out.push_str(&format!("- Update failure: {cause}\n"));
        }
    }
    out
}

/// Render one machine's compact operational status.
pub(crate) fn render_executor(executor: &ExecutorInspection) -> String {
    let health = &executor.health;
    let capabilities = &health.advertisement.capabilities;
    let mut out = format!("# Executor {}\n\n", executor.name);

    out.push_str("## Identity\n");
    out.push_str(&format!("- Name: {}\n", executor.name));
    out.push_str(&format!(
        "- Attachment: {}\n",
        if executor.colocated {
            "colocated (supervised inside the runner's own process tree)"
        } else {
            "enrolled (attached over the executor protocol)"
        }
    ));
    out.push_str(&machine_role_line(health.role));
    out.push_str(&format!(
        "- Platform: {} / {}\n",
        capabilities.os, capabilities.arch
    ));
    out.push_str(&format!(
        "- Logical cores: {}\n",
        capabilities.logical_cores
    ));
    out.push_str(&format!(
        "- Toolchains: {}\n",
        list_or(&capabilities.toolchains, "none advertised")
    ));
    out.push_str(&render_toolchain_probes(capabilities));
    out.push_str(&render_build_prerequisites(capabilities));
    out.push_str(&render_project_eligibility(&executor.project_eligibility));
    out.push_str(&render_containment(capabilities, executor.colocated));
    out.push_str(&format!(
        "- Projects served: {}\n\n",
        list_or(&capabilities.projects_served, "every project")
    ));

    out.push_str("## Link\n");
    out.push_str(&format!("- Status: {}\n", link_summary(executor)));
    // A machine riding a tunnel inherits the tunnel's stalls as link losses,
    // which is invisible from either end without this line.
    if let Some(path) = &executor.link_path {
        out.push_str(&format!(
            "- Path: {} ({} → {})\n",
            if path.through_tunnel() {
                "tailnet"
            } else {
                "direct"
            },
            path.executor.ip(),
            path.runner
        ));
    }
    out.push_str(&format!(
        "- Last heartbeat: {}\n",
        age(health.heartbeat_age_ms)
    ));
    // Link staleness and fact staleness are separate verdicts on purpose: a
    // machine beating on time can still be shipping numbers that stopped
    // moving, and folding the two would report a healthy host as a dead one.
    out.push_str(&format!(
        "- Telemetry measured: {}\n",
        match health.liveness_age_ms {
            Some(value) => format!(
                "{}{}",
                age(value),
                if health.telemetry_stale {
                    " (stale: these numbers are history, the link is not)"
                } else {
                    ""
                }
            ),
            None => "not reported by this executor".to_string(),
        }
    ));
    // A freeze the machine explains and one it does not are different facts,
    // and the difference is why this link is still up: the runner stops reading
    // an explained freeze as a dead transport, so this line is what keeps it
    // from becoming invisible instead.
    if let Some(blocked) = &health.advertisement.facts_stale_refresh_blocked {
        out.push_str(&format!(
            "- Telemetry blocked: the refresh pass has been {} for {} (pass began {} ago) — every number above stands still until it returns\n",
            blocked.phase.label(),
            age(executor
                .captured_at_unix_ms
                .saturating_sub(blocked.phase_entered_unix_ms)),
            age(executor
                .captured_at_unix_ms
                .saturating_sub(blocked.pass_started_unix_ms)),
        ));
    }
    if let Some(refresh) = &health.advertisement.last_refresh {
        out.push_str(&format!(
            "- Last refresh: {}\n",
            render_refresh_timings(refresh)
        ));
    }
    out.push_str(&format!(
        "- Connection generation: {}\n",
        health.connection_generation
    ));
    out.push_str(&render_connection_timeline(
        &executor.connection_timeline,
        executor.captured_at_unix_ms,
    ));
    out.push_str(&render_update_report(
        executor.last_update.as_ref(),
        executor.captured_at_unix_ms,
    ));
    out.push_str(&format!(
        "- Draining: {}\n",
        if health.drain_mode {
            "yes — the runner refuses new work here and leaves resident work alone. It holds across reconnects and runner restarts until it is cleared"
        } else {
            "no"
        }
    ));
    out.push_str(&format!(
        "- Build: {}\n",
        match (&executor.executor_build_id, &health.build_skew) {
            (_, Some(skew)) => format!(
                "{} — SKEWED from the runner's deployed {}",
                skew.executor_build_id, skew.runner_build_id
            ),
            (Some(build), None) => build.clone(),
            (None, None) => "not reported".to_string(),
        }
    ));
    out.push('\n');

    out.push_str("## Placement telemetry\n");
    let machine = &health.machine;
    out.push_str(&format!(
        "- CPU utilisation: {}\n",
        reading(&machine.cpu, executor.captured_at_unix_ms, |cpu| format!(
            "{:.0}% of {} cores ({:.0}% user, {:.0}% system)",
            cpu.utilization * 100.0,
            cpu.logical_cores,
            cpu.user * 100.0,
            cpu.system * 100.0
        ))
    ));
    out.push_str(&format!(
        "- Memory: {}\n",
        reading(&machine.memory, executor.captured_at_unix_ms, |memory| {
            format!(
                "{} available of {}",
                bytes(memory.available_bytes),
                bytes(memory.total_bytes)
            )
        })
    ));
    out.push_str(&format!(
        "- I/O pressure: {}\n",
        reading(
            &health.host.io_pressure,
            executor.captured_at_unix_ms,
            render_io_pressure
        )
    ));
    out.push_str(
        "- Kernel I/O timeouts: not measured; the kernel log is their only source, and whether it is readable varies by host\n",
    );
    out.push('\n');

    out.push_str(&render_gpus(
        &health.host.gpus,
        &machine.memory,
        executor.captured_at_unix_ms,
    ));

    out.push_str(&render_storage(executor));
    // Two verdicts, never one. The physical half is how much room is left; the
    // custodial half is whether the executor still owns bytes it has admitted it
    // cannot reclaim. Flattening them is what let an `Ok` volume hide two
    // unreclaimable trees for sixteen days (CAIRN-4217).
    let reclamation = health.disk.reclamation.as_ref();
    let custodial = match reclamation {
        Some(reclamation) if reclamation.is_outstanding() => "Degraded",
        Some(_) => "Ok",
        None => "Unreported",
    };
    // A sweep that skipped something never reads as a bare `Completed`: a
    // quarantine the janitor could not see once sat behind exactly that word for
    // a week, dropped from every pass while it held 170 GB (CAIRN-4858).
    let skipped = health.disk.cleanup_skipped_entries.unwrap_or(0);
    let partial = match skipped {
        0 => String::new(),
        count => format!(
            ", partial: {count} skipped ({})",
            inline_skip_paths(&health.disk.cleanup_skipped, count)
        ),
    };
    // A latched machine never reads as a sweep that merely completed: every
    // admission is being refused, and the page is where an operator learns
    // which write is holding it (CAIRN-5067).
    let sweep = if health.disk.cleanup_blocked {
        format!(
            "{:?} with error — admission blocked",
            health.disk.sweep_status
        )
    } else {
        format!("{:?}", health.disk.sweep_status)
    };
    out.push_str(&format!(
        "- Verdict: volume {:?}, custody {custodial}, sweep {sweep}{partial}\n",
        health.disk.status
    ));
    if health.disk.cleanup_blocked {
        out.push_str(&format!(
            "- Cleanup blocking admission: {}{}\n",
            health
                .disk
                .cleanup_last_error
                .as_deref()
                .unwrap_or("no error recorded"),
            health
                .disk
                .cleanup_failing_path
                .as_deref()
                .map(|path| format!(" (failing path `{path}`)"))
                .unwrap_or_default()
        ));
    }
    out.push_str(&render_storage_pressure(executor));
    if skipped > 0 {
        out.push_str("- Sweep skipped:\n");
        for entry in &health.disk.cleanup_skipped {
            out.push_str(&format!(
                "  - `{}` ({:?}, {:?})\n",
                entry.path, entry.operation, entry.reason
            ));
        }
        let unlisted = skipped.saturating_sub(health.disk.cleanup_skipped.len());
        if unlisted > 0 {
            out.push_str(&format!("  - and {unlisted} more not listed\n"));
        }
    }
    if let Some(reclamation) = reclamation.filter(|reclamation| reclamation.is_outstanding()) {
        out.push_str(&format!(
            "- Reclamation alarms: {} outstanding\n",
            reclamation.outstanding
        ));
        for alarm in &reclamation.alarms {
            let age = executor
                .captured_at_unix_ms
                .saturating_sub(alarm.first_permanent_unix_ms);
            out.push_str(&format!(
                "  - `{}` — stuck {}, {} after {} attempts, {:?}; last error: {}\n",
                alarm.path,
                duration_ms(age),
                bytes(alarm.bytes),
                alarm.attempts,
                alarm.verification,
                alarm.last_error
            ));
            if !alarm.survivors.is_empty() {
                out.push_str(&format!("    survivors: {}\n", alarm.survivors));
            }
        }
        // Stated every time, because the one thing an operator must not assume
        // is that Cairn has already tried something destructive on their behalf.
        out.push_str(
            "  - Cairn has not modified these paths. Reclaiming them is destructive and requires operator confirmation.\n",
        );
    } else if reclamation.is_none() {
        out.push_str(
            "- Reclamation alarms: not reported by this executor (absence is not a clean bill of health)\n",
        );
    }
    if let Some(cadence) = health.disk.accounting_cadence.as_ref() {
        if let Some(interval_ms) = cadence.interval_ms {
            out.push_str(&format!(
                "- Storage accounting cadence: every {}{}\n",
                duration_ms(interval_ms),
                cadence
                    .last_pass_duration_ms
                    .map(|pass| format!(", last pass {}", duration_ms(pass)))
                    .unwrap_or_default()
            ));
        }
    }
    out.push('\n');

    out.push_str("## Admission\n");
    let admission = &health.admission;
    let reserved = admission.active_reservation.concurrency_units;
    match admission.concurrency_capacity {
        Some(u32::MAX) => out.push_str(&format!("- Concurrency: {reserved} reserved\n")),
        Some(capacity) => out.push_str(&format!(
            "- Concurrency: {reserved} of {capacity} units reserved\n"
        )),
        None => out.push_str(&format!(
            "- Concurrency: {reserved} units reserved (capacity unstated)\n"
        )),
    }
    out.push_str(&format!(
        "- Accepted {} / rejected {} / timed out {}\n",
        admission.accepted_count, admission.rejected_count, admission.timed_out_count
    ));
    // A project provisions one cell at a time, and its other work declines as
    // capacity while it does. Naming the hold, its owner, and its age is what
    // tells that apart from a machine that is actually full.
    for hold in &executor.occupancy.provisioning_holds {
        out.push_str(&format!(
            "- Provisioning hold: project {} held by request `{}`{}, taken {}\n",
            hold.project_id,
            hold.owner_request_id,
            hold.entry_id
                .as_deref()
                .map(|entry_id| format!(" (entry `{entry_id}`)"))
                .unwrap_or_default(),
            age(executor
                .captured_at_unix_ms
                .saturating_sub(hold.since_unix_ms)),
        ));
    }
    if executor.occupancy.orphaned_provisioning_releases > 0 {
        out.push_str(&format!(
            "- Orphaned provisioning holds released: {} (each is an admission exit that failed to release its hold; this is a defect to report)\n",
            executor.occupancy.orphaned_provisioning_releases
        ));
    }
    // The measured-CPU brake is the only thing standing between a saturated
    // machine and more work, so whether it can currently see is a fact the
    // Admission section owes its reader. A machine that stopped measuring reads
    // as idle everywhere else on this page.
    out.push_str(&format!(
        "- Measured CPU brake: {}\n",
        cpu_brake_summary(&health.host.cpu_admission, executor.captured_at_unix_ms)
    ));
    if health.queues.is_empty() {
        out.push_str("- Queues: empty\n");
    } else {
        for queue in &health.queues {
            out.push_str(&format!(
                "- Queue {:?}: depth {}{}\n",
                queue.priority,
                queue.depth,
                queue
                    .oldest_age_ms
                    .map(|value| format!(", oldest waiting {}", age(value)))
                    .unwrap_or_default()
            ));
        }
    }
    out.push('\n');

    out.push_str(&render_executor_memory(executor));

    out.push_str("## Process ownership\n");
    out.push_str(&format!(
        "- Resident ownership: {} tracked, {} stale reaped, {} unverified{}\n",
        health.resident_processes.tracked_live_count,
        health.resident_processes.reaped_stale_count,
        health.resident_processes.unverified_stale_count,
        health
            .resident_processes
            .oldest_stale_age_ms
            .map(|value| format!(", oldest stale {}", age(value)))
            .unwrap_or_default()
    ));
    out.push_str(&format!(
        "- Command ownership: {} tracked, {} stale reaped, {} unverified{}\n",
        health.command_processes.tracked_live_count,
        health.command_processes.reaped_stale_count,
        health.command_processes.unverified_count,
        health
            .command_processes
            .oldest_age_ms
            .map(|value| format!(", oldest {}", age(value)))
            .unwrap_or_default()
    ));

    // Only when there are any: an eviction is the one place the fleet destroys
    // an owner's uncommitted work, and it should be legible here rather than
    // only in the machine's log, which nobody reads until something is wrong.
    if !health.evicted_cells.is_empty() {
        out.push_str("\n## Environments evicted at adoption\n");
        for evicted in &health.evicted_cells {
            out.push_str(&format!(
                "- {} ({}, {}): {}{}\n",
                evicted.cell_id,
                if evicted.holder.is_empty() {
                    "no recorded holder"
                } else {
                    evicted.holder.as_str()
                },
                age(executor
                    .captured_at_unix_ms
                    .saturating_sub(evicted.evicted_at_unix_ms)),
                evicted.reason,
                // The worse condition, so it is never left implied: the machine
                // came up without this cell but did not get rid of it, and every
                // restart from here meets it again.
                if evicted.retirement_failed {
                    " — STILL ON DISK: retirement failed, so this repeats on every restart until an operator clears it"
                } else {
                    ""
                }
            ));
        }
    }
    // Only when there are any. A declined slot is still in the pool and still
    // Idle, so nothing else on this page distinguishes it from a warm one that
    // claims simply have not needed; this is the one place that says claims
    // keep refusing it, and why.
    if !health.busy_declined_cells.is_empty() {
        out.push_str("\n## Slots declined at claim\n");
        for declined in &health.busy_declined_cells {
            let cause = match &declined.lock_path {
                Some(lock) => format!(
                    "Git lock {lock}{}, {}",
                    declined
                        .lock_age_ms
                        .map(|ms| format!(", {} old", duration_ms(ms)))
                        .unwrap_or_default(),
                    declined
                        .holder
                        .as_deref()
                        .map(|holder| format!("held by {holder}"))
                        .unwrap_or_else(|| "no holder could be proven".to_string()),
                ),
                None => declined.diagnostic.clone(),
            };
            out.push_str(&format!(
                "- {} ({}): declined {} claims in a row, the first {}; {cause}. Still in the pool, not quarantined; a lock no process holds on a machine that cannot prove it abandoned is the likely cause, and removing it by hand clears this.\n",
                declined.cell_id,
                declined.project_id,
                declined.consecutive_declines,
                age(executor
                    .captured_at_unix_ms
                    .saturating_sub(declined.first_declined_at_unix_ms)),
            ));
        }
    }
    out.push_str(&render_stopped_commands(
        &health.stopped_commands,
        &capabilities.os,
        executor.captured_at_unix_ms,
    ));
    out.push('\n');

    out.push_str("## Occupancy\n");
    let occupancy = &executor.occupancy;
    // "Materialized" is every cell in the pool. It used to read "checked out",
    // which an operator took for holds and went looking for bytes held cells
    // did not have; holds are `held`, and only idle cells are reclaimable.
    out.push_str(&format!(
        "- Cells: {} materialized ({} held, {} running, {} idle reclaimable)\n",
        health.inventory.checked_out_count,
        health.inventory.held_count,
        health.inventory.transient_occupancy,
        health.inventory.idle_count
    ));
    out.push_str(&format!(
        "- Idle retention guard: up to {} idle per project, {} machine-wide{}\n",
        health.inventory.idle_retention_budget_per_project,
        health.inventory.idle_retention_budget_total,
        if health.inventory.idle_retention_pressured {
            " (volume at its pressure line: reclamation is byte-driven, coldest first)"
        } else {
            ""
        }
    ));
    out.push_str(&format!(
        "- Running: {}\n",
        occupancy.executing_requests.len()
    ));
    out.push_str(&format!("- Queued: {}\n", occupancy.queued_requests.len()));
    out.push_str(&format!(
        "- Resident processes: {}\n",
        occupancy
            .resident_occupancy
            .as_ref()
            .map(|value| value.process_count)
            .unwrap_or(0)
    ));
    out.push('\n');

    out.push_str("## Pool slots\n");
    if occupancy.cells.is_empty() {
        out.push_str("No cached slot state.\n");
    }
    for cell in &occupancy.cells {
        let head = cell
            .last_sealed_commit
            .as_deref()
            .map(short_commit)
            .unwrap_or_else(|| "unmeasured".into());
        let lifecycle = serde_json::to_value(&cell.lifecycle)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| "unknown".into());
        // The walk's measurement when there is one, labelled with its age; the
        // running counter otherwise, labelled as the estimate it is.
        let footprint = executor
            .slots
            .iter()
            .find(|slot| slot.project_id == cell.project_id && slot.cell_id == cell.cell_id)
            .and_then(|slot| Some((slot.measured_bytes?, slot.measured_at_unix_ms?)))
            .map_or_else(
                || format!("~{} estimate", bytes(cell.size_bytes)),
                |(measured, at)| {
                    format!(
                        "{} measured {}",
                        bytes(measured),
                        age(executor.captured_at_unix_ms.saturating_sub(at))
                    )
                },
            );
        out.push_str(&format!(
            "- {}: project {}, head {}, {}, {}, last used {}; last distance: {}\n",
            cell.cell_id,
            slot_project_name(cell),
            head,
            lifecycle,
            footprint,
            age(executor
                .captured_at_unix_ms
                .saturating_sub(cell.last_used_unix_ms)),
            render_cache_distance(cell.last_distance.as_ref(), executor.captured_at_unix_ms)
        ));
        if let Some(reason) = &cell.quarantine_reason {
            out.push_str(&format!("  - Quarantine: {}\n", reason.replace('\n', " ")));
        }
    }
    out.push('\n');

    out.push_str(&format!(
        "Placement history: cairn://executors/{}?view=placements\n\nTarget this machine with `run({{executor:{{name:\"{}\"}}}})`. Renaming it is an operator action: `cairn executor rename {} <new-name>`.\n",
        executor.name, executor.name, executor.name
    ));
    out
}

/// Every recent placement this machine took part in — won or lost.
///
/// This is the answer to the two questions an operator actually has about a
/// fleet: why did that work run there, and why is this machine idle. Both are
/// read off the same record, which is why there is no second placement log.
/// Local execution appears here on the same terms as any other machine: it says
/// what it won on, and "local fallback" is never an unstated state.
pub(crate) fn render_placements(executor: &ExecutorInspection) -> String {
    let mut out = format!("# Executor {} placement decisions\n\n", executor.name);
    if executor.recent_placements.is_empty() {
        out.push_str(
            "- No placement decision in the runner's recent window named this machine.\n\n",
        );
        return out;
    }
    let routine = executor
        .recent_placements
        .iter()
        .filter(|decision| is_routine_placement(decision))
        .count();
    if routine > 0 {
        out.push_str(&format!(
            "- {routine} routine {} (pinned, home, or sole candidate)\n",
            if routine == 1 {
                "placement"
            } else {
                "placements"
            }
        ));
        let requests = executor
            .recent_placements
            .iter()
            .filter(|decision| is_routine_placement(decision))
            .map(|decision| {
                format!(
                    "[`{}`](cairn://executors/{}?view=placement&request={})",
                    decision.request_id, executor.name, decision.request_id
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("  - Requests: {requests}\n"));
    }
    for decision in executor
        .recent_placements
        .iter()
        .filter(|decision| !is_routine_placement(decision))
    {
        let asked = placement_ask(decision);
        // The binding shape when there is one: what happened to the machines
        // that were asked, not only which one placement picked. A decision whose
        // chain holds a decline is never routine, so it always reaches here.
        let outcome = match (&decision.outcome, describe_binding(decision)) {
            (_, Some(binding)) => binding,
            (PlacementOutcome::Selected(selection), None) => format!(
                "chose {} because {}",
                selection.executor_name,
                selection.reason.as_str()
            ),
            (PlacementOutcome::Refused { diagnostic }, None) => format!("refused: {diagnostic}"),
        };
        out.push_str(&format!(
            "- Request `{}` ({}, asked for {asked}): {outcome}\n  - Detail: cairn://executors/{}?view=placement&request={}\n",
            decision.request_id,
            decision.mobility.as_str(),
            executor.name,
            decision.request_id,
        ));
    }
    out.push('\n');
    out
}

fn is_routine_placement(decision: &PlacementDecision) -> bool {
    // A decline is the interesting case by definition: a machine placement
    // chose was asked and said no. Collapsing such a decision into the routine
    // count would hide the one thing the binding chain exists to show.
    if decision
        .binding
        .iter()
        .any(|event| matches!(event, BindingEvent::Declined { .. }))
    {
        return false;
    }
    matches!(
        &decision.outcome,
        PlacementOutcome::Selected(selection)
            if matches!(
                selection.reason,
                PlacementReason::Pinned | PlacementReason::ColocatedHome | PlacementReason::OnlyCandidate
            )
    )
}

/// How a decision actually bound, in one line.
///
/// The list view's job is to make an unusual placement worth opening, so this
/// says what happened to the machines that were asked rather than only which
/// one won: `chose bbs (claimed)`, or `chose bbs (declined: pressure), then
/// fedora (claimed)`.
fn describe_binding(decision: &PlacementDecision) -> Option<String> {
    if decision.binding.is_empty() {
        return None;
    }
    let mut steps: Vec<String> = Vec::new();
    for event in &decision.binding {
        match event {
            BindingEvent::Offered { executor_name, .. } => steps.push(executor_name.clone()),
            BindingEvent::Claimed { .. } => {
                if let Some(last) = steps.last_mut() {
                    last.push_str(" (claimed)");
                }
            }
            BindingEvent::Declined { reason, disk, .. } => {
                if let Some(last) = steps.last_mut() {
                    last.push_str(&format!(
                        " (declined: {}{})",
                        decline_reason_word(*reason),
                        disk.as_ref()
                            .map(|disk| format!(", {}", disk_outlook_word(&disk.outlook)))
                            .unwrap_or_default()
                    ));
                }
            }
            BindingEvent::Lost { .. } => {
                if let Some(last) = steps.last_mut() {
                    last.push_str(" (claim lost)");
                }
            }
            BindingEvent::GenerationEnded { .. } => {
                if let Some(last) = steps.last_mut() {
                    last.push_str(" (link ended)");
                }
            }
        }
    }
    Some(format!("chose {}", steps.join(", then ")))
}

/// The head of the `## Storage` section: the volume against its thresholds,
/// what Cairn holds by category, and what of the volume is not Cairn's.
///
/// A bare `Ok` beside a volume one build from refusing work is the defect this
/// section exists to end: the verdict is always shown with the lines it was
/// computed against, and the category split the executor already walks is
/// shown rather than only carried.
fn render_storage(executor: &ExecutorInspection) -> String {
    let health = &executor.health;
    let disk = &health.disk;
    let machine = &health.machine;
    let captured = executor.captured_at_unix_ms;
    let mut out = String::from("## Storage\n");
    let thresholds = if disk.pressure_free_bytes > 0 {
        format!(
            " (pressure line {}, critical {}, admission floor {})",
            bytes(disk.pressure_free_bytes),
            bytes(disk.critical_free_bytes),
            bytes(disk.watermark_bytes)
        )
    } else {
        " (thresholds not reported by this executor)".to_string()
    };
    out.push_str(&format!(
        "- Volume: {} — {:?}{thresholds}\n",
        reading(&machine.volume, captured, |volume| format!(
            "{} free of {}",
            bytes(volume.free_bytes),
            bytes(volume.total_bytes)
        )),
        disk.status,
    ));
    match machine.disk_accounting.value() {
        Some(accounting) => {
            let categories = &accounting.categories;
            let other_top = if accounting.other_top.is_empty() {
                String::new()
            } else {
                format!(
                    " ({})",
                    accounting
                        .other_top
                        .iter()
                        .map(|entry| format!("{} {}", entry.path, bytes(entry.bytes)))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            let retired_homes = if categories.retired_homes_bytes > 0 {
                format!(
                    " · retired executor homes {}",
                    bytes(categories.retired_homes_bytes)
                )
            } else {
                String::new()
            };
            out.push_str(&format!(
                "- Cairn holds {} (logical bytes, accounting {}): live cells {} · external caches {} · quarantines {} · managed objects {} · warm caches {} · other {}{other_top}{retired_homes}\n",
                bytes(accounting.used_bytes),
                age(captured.saturating_sub(machine.disk_accounting.measured_at_unix_ms)),
                bytes(categories.live_cells_bytes),
                bytes(categories.external_managed_caches_bytes),
                bytes(categories.quarantines_bytes),
                bytes(categories.managed_objects_bytes),
                bytes(categories.warm_caches_bytes),
                bytes(categories.temporary_other_bytes),
            ));
            for home in &accounting.retired_homes {
                out.push_str(&retired_home_line(home));
            }
            // A partial walk names what it could not price, so a directory
            // the executor cannot read is a path on this page rather than a
            // count nobody can act on.
            if accounting.is_partial() {
                out.push_str("- Accounting skipped:\n");
                for entry in &accounting.skipped {
                    out.push_str(&format!(
                        "  - `{}` ({:?}, {:?})\n",
                        entry.path, entry.operation, entry.reason
                    ));
                }
                let unlisted = accounting
                    .skipped_count()
                    .saturating_sub(accounting.skipped.len());
                if unlisted > 0 {
                    out.push_str(&format!("  - and {unlisted} more not listed\n"));
                }
            }
            if let Some(volume) = machine.volume.value() {
                // Unaccounted, never "not Cairn's": the walk covers what
                // Cairn manages, and Cairn's processes write outside it too.
                // A full volume whose remainder read as someone else's was
                // in fact holding a reclaimed target's binaries open.
                if accounting.is_partial() {
                    out.push_str(&format!(
                        "- Outside Cairn's accounting: not inferred — accounting is partial ({} entries skipped)\n",
                        accounting.skipped_count()
                    ));
                } else {
                    let used = volume.total_bytes.saturating_sub(volume.free_bytes);
                    out.push_str(&format!(
                        "- Outside Cairn's accounting (inferred: volume used minus Cairn's accounting): ~{} of the volume's {} used. Not necessarily another owner's: the walk does not count an earlier executor installation's home, deleted files a running process still holds open, toolchain and package caches Cairn does not manage, system temporary directories, or filesystem snapshots\n",
                        bytes(used.saturating_sub(accounting.used_bytes)),
                        bytes(used)
                    ));
                }
            }
        }
        None => out.push_str(&format!(
            "- Cairn holds: {}\n",
            reading(&machine.disk_accounting, captured, |_| String::new())
        )),
    }
    out
}

/// One retired executor home: a named component with its bytes, never folded
/// into the unaccounted remainder, and what the ladder is doing with it.
fn retired_home_line(home: &RetiredHomeAccounting) -> String {
    let at = |unix_ms: u64| {
        crate::clock::stamp_millis_with_seconds(unix_ms as i64)
            .unwrap_or_else(|| "an unknown time".to_string())
    };
    let what = match &home.state {
        RetiredHomeState::Reclaiming => format!("{}, reclaiming", bytes(home.bytes)),
        RetiredHomeState::Held {
            pids,
            unknown,
            detail,
        } => {
            let who = if *unknown {
                "left in place: whether a process uses it could not be determined".to_string()
            } else {
                match pids.as_slice() {
                    [] => "left in place: held by a live process".to_string(),
                    [pid] => format!("held by 1 process (pid {pid})"),
                    pids => format!(
                        "held by {} processes (pids {})",
                        pids.len(),
                        pids.iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                }
            };
            let detail = detail
                .as_deref()
                .map(|detail| format!(" — {detail}"))
                .unwrap_or_default();
            format!(
                "{}, {who}; reclaimed once it is released{detail}",
                bytes(home.bytes)
            )
        }
        RetiredHomeState::RetainedRunnerState {
            bytes_freed,
            at_unix_ms,
        } => format!(
            "reclaimed {} at {}; {} retained: runner state (cairn.db), the operator's to remove",
            bytes(*bytes_freed),
            at(*at_unix_ms),
            bytes(home.bytes)
        ),
        RetiredHomeState::Refused { reason } => {
            format!("{}, left alone: {reason}", bytes(home.bytes))
        }
        RetiredHomeState::Reclaimed {
            bytes_freed,
            at_unix_ms,
        } => format!("reclaimed {} at {}", bytes(*bytes_freed), at(*at_unix_ms)),
    };
    format!(
        "- Retired executor home {} (retired {}): {what}\n",
        home.path,
        at(home.retired_at_unix_ms)
    )
}

/// The pressure tier's own account: what the last pass asked for, what each
/// rung gave, whether that was enough, and which trees it left alone.
fn render_storage_pressure(executor: &ExecutorInspection) -> String {
    let disk = &executor.health.disk;
    let captured = executor.captured_at_unix_ms;
    let mut out = String::new();
    match disk.pressure.as_ref() {
        Some(pressure) => {
            let by_rung = |rung: StorageReclaimRung| {
                pressure
                    .reclaimed_by_rung
                    .iter()
                    .find(|entry| entry.rung == rung)
                    .copied()
                    .unwrap_or(StorageRungReclaim {
                        rung,
                        bytes: 0,
                        count: 0,
                    })
            };
            let total = pressure
                .reclaimed_by_rung
                .iter()
                .fold(0_u64, |sum, entry| sum.saturating_add(entry.bytes));
            let retired = by_rung(StorageReclaimRung::RetiredCells);
            let retired_homes = by_rung(StorageReclaimRung::RetiredHomes);
            out.push_str(&format!(
                "- Last pressure pass {}: band {:?}, asked {}, reclaimed {} ({}retired trees {} · managed caches {} · quarantine targets {} · idle cell scratch {} · idle targets {} · {} cells retired {}) — {:?}{}\n",
                age(captured.saturating_sub(pressure.at_unix_ms)),
                pressure.band,
                bytes(pressure.target_bytes),
                bytes(total),
                if retired_homes.bytes > 0 {
                    format!("retired executor homes {} · ", bytes(retired_homes.bytes))
                } else {
                    String::new()
                },
                bytes(by_rung(StorageReclaimRung::RetiredTrees).bytes),
                bytes(by_rung(StorageReclaimRung::ManagedCaches).bytes),
                bytes(by_rung(StorageReclaimRung::QuarantineTargets).bytes),
                bytes(by_rung(StorageReclaimRung::IdleCellScratch).bytes),
                bytes(by_rung(StorageReclaimRung::IdleTargets).bytes),
                retired.count,
                bytes(retired.bytes),
                pressure.outcome,
                pressure
                    .rearm_below_free_bytes
                    .map(|rearm| format!(
                        "; {} held by policy; next pressure pass when free < {} or after the housekeeping interval",
                        bytes(pressure.held_bytes),
                        bytes(rearm)
                    ))
                    .unwrap_or_default()
            ));
            for withheld in &pressure.withheld {
                out.push_str(&format!("- Not reclaimed: {}\n", withheld_line(withheld)));
            }
            for skip in &pressure.occupied_skips {
                out.push_str(&format!(
                    "- Skipped: `{}` {}\n",
                    skip.path,
                    occupied_skip_reason(skip)
                ));
            }
        }
        None if disk.pressure_free_bytes > 0 => {
            out.push_str("- Last pressure pass: none since this executor started\n")
        }
        None => {}
    }
    if !disk.quarantine_targets_stripped.is_empty() {
        out.push_str("- Quarantine build output stripped under pressure:\n");
        for strip in &disk.quarantine_targets_stripped {
            out.push_str(&format!(
                "  - `{}` {} {}{}\n",
                strip.path,
                bytes(strip.bytes),
                age(captured.saturating_sub(strip.at_unix_ms)),
                not_consulted_clause(strip)
            ));
        }
    }
    if let Some(lineage) = &disk.occupancy_lineage {
        out.push_str(&occupancy_lineage_line(lineage));
    }
    out
}

/// One class of bytes a pressure pass left in place, naming why and what
/// releases it, so the reader can tell relief that is Cairn's to give from
/// relief that is theirs.
fn withheld_line(withheld: &StorageWithheld) -> String {
    let count = |one: &str, many: &str| {
        format!(
            "{} {}",
            withheld.count,
            if withheld.count == 1 { one } else { many }
        )
    };
    let size = if withheld.unmeasured {
        "an unmeasured amount".to_string()
    } else {
        bytes(withheld.bytes)
    };
    match withheld.reason {
        StorageWithheldReason::ForensicWindow => format!(
            "{size} in {} inside the forensic window; released when the window expires",
            count("quarantine", "quarantines")
        ),
        StorageWithheldReason::FencedCells => format!(
            "{size} in {} fenced behind processes from an ended epoch, which the executor will not signal because nothing proves it launched them; released when they exit, so ending them is the operator's (each cell's Quarantine line under Pool slots names them)",
            count("cell", "cells")
        ),
        StorageWithheldReason::CachesInUse => format!(
            "{size} in {} in use (written in the last ten minutes, or held by a build, a reader, or a publication); released when that use ends",
            count("managed cache", "managed caches")
        ),
        StorageWithheldReason::CachesKept => format!(
            "{size} in {} the pass did not take (its ask was met first, or the pressure band's floor kept them); the critical band reaches below the floor",
            count("managed cache", "managed caches")
        ),
        StorageWithheldReason::CachesOwnerRetained => format!(
            "{size} in {} whose writers keep their own retention (CI targets, the asset cache, the sccache store), {}; the executor accounts them but never deletes them, so trimming them is their owner's or the operator's",
            count("managed cache root", "managed cache roots"),
            if withheld.unmeasured {
                "not yet sized by an accounting walk"
            } else {
                "as of the last accounting walk"
            }
        ),
    }
}

/// Why the pressure tier left a tree alone, naming the rule that fired and
/// what would release it.
fn occupied_skip_reason(skip: &OccupiedTreeSkip) -> String {
    let who = |pid: u32| match (&skip.process, &skip.executable) {
        (Some(name), Some(executable)) => format!("pid {pid} ({name}, {executable})"),
        (Some(name), None) => format!("pid {pid} ({name})"),
        (None, Some(executable)) => format!("pid {pid} ({executable})"),
        (None, None) => format!("pid {pid}"),
    };
    let held = skip.held_path.as_deref().unwrap_or("inside it");
    match (skip.rule, skip.pid) {
        (Some(OccupancyRule::Cwd), Some(pid)) => format!(
            "occupied by {}: working directory inside it; released when it leaves or exits",
            who(pid)
        ),
        (Some(OccupancyRule::Executable), Some(pid)) => format!(
            "occupied by {}: running executable {held}; released when it exits",
            who(pid)
        ),
        (Some(OccupancyRule::OpenFile), Some(pid)) => format!(
            "occupied by {}: open file {held}; released when it closes or exits",
            who(pid)
        ),
        (Some(OccupancyRule::WithheldInLineage), Some(pid)) => format!(
            "occupancy unknown: {} withholds its descriptors and runs in the executor's own {}; released when it exits",
            who(pid),
            skip.container.as_deref().unwrap_or("lineage")
        ),
        (Some(OccupancyRule::WithheldContainerUnreadable), Some(pid)) => format!(
            "occupancy unknown: {} withholds its descriptors and its lineage container could not be read; released when it exits",
            who(pid)
        ),
        (Some(OccupancyRule::LineageUnknown), Some(pid)) => format!(
            "occupancy unknown: {} withholds its descriptors and the executor's own lineage could not be read; released when it exits or the executor can read its lineage",
            who(pid)
        ),
        (Some(OccupancyRule::ReadFailed), Some(pid)) => format!(
            "occupancy unknown: the holdings of {} could not be read; released when it exits",
            who(pid)
        ),
        (Some(OccupancyRule::ProcessTableUnreadable), _) => {
            "occupancy unknown: the process table could not be read; released when it can be"
                .into()
        }
        (Some(OccupancyRule::PlatformUnsupported), _) => {
            "occupancy unknown: this platform cannot say which processes hold files, so this rung never strips here"
                .into()
        }
        // A peer that predates the rule.
        (None, Some(pid)) if !skip.unknown => format!("occupied by pid {pid}"),
        (None, Some(pid)) => format!("occupancy unknown: pid {pid}'s holdings could not be read"),
        _ => "occupancy unknown".into(),
    }
}

/// The processes a strip's vacancy did not consult, so the page says what the
/// rung acted without reading.
fn not_consulted_clause(strip: &QuarantineTargetStrip) -> String {
    if strip.withheld_outside_lineage.is_empty() {
        return String::new();
    }
    let named = strip
        .withheld_outside_lineage
        .iter()
        .map(|process| {
            let name = process
                .name
                .as_deref()
                .map(|name| format!(" {name}"))
                .unwrap_or_default();
            match &process.executable {
                Some(executable) => {
                    format!(
                        "pid {}{name} ({executable}, {})",
                        process.pid, process.container
                    )
                }
                None => format!("pid {}{name} ({})", process.pid, process.container),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let more = match strip.withheld_outside_lineage_truncated {
        0 => String::new(),
        more => format!(" and {more} more"),
    };
    format!(
        "; not consulted: {named}{more}, withheld by the kernel and outside the executor's lineage"
    )
}

/// Which kernel containers the executor counts as its own, so a reader can
/// see what a "not consulted" clause was measured against.
fn occupancy_lineage_line(lineage: &OccupancyLineageReport) -> String {
    if let Some(why) = &lineage.unknown {
        return format!(
            "- Occupancy lineage: unknown ({why}); any process whose holdings the kernel withholds makes a quarantine's occupancy unknown\n"
        );
    }
    let earlier = lineage
        .containers
        .iter()
        .filter(|container| Some(*container) != lineage.current.as_ref())
        .map(String::as_str)
        .collect::<Vec<_>>();
    let current = lineage.current.as_deref().unwrap_or("unread");
    let earlier = if earlier.is_empty() {
        String::new()
    } else {
        format!("; earlier launches this boot: {}", earlier.join(", "))
    };
    format!(
        "- Occupancy lineage: {current}{earlier} (a withheld process outside these is not consulted; inside them it makes occupancy unknown)\n"
    )
}

/// Whether waiting can relieve a disk refusal, in a word or two.
fn disk_outlook_word(outlook: &DiskReliefOutlook) -> &'static str {
    match outlook {
        DiskReliefOutlook::InFlight => "sweep in flight",
        DiskReliefOutlook::Relieved => "new growth since a relieved pass",
        DiskReliefOutlook::Unrelieved { .. } => "pressure unrelieved, waiting will not help",
        DiskReliefOutlook::CleanupFailed => "storage cleanup failed",
        DiskReliefOutlook::NotAttempted => "no pressure pass yet",
        DiskReliefOutlook::Deferred => "pressure pass yielded, next one continues",
    }
}

fn disk_decline_line(disk: &DiskDecline) -> String {
    let held = match disk.outlook {
        DiskReliefOutlook::Unrelieved { held_bytes } => {
            format!(" ({} held by policy)", bytes(held_bytes))
        }
        _ => String::new(),
    };
    format!(
        "{} free against a {} floor; {}{held}",
        bytes(disk.free_bytes),
        bytes(disk.floor_bytes),
        disk_outlook_word(&disk.outlook)
    )
}

fn decline_reason_word(reason: DeclineReason) -> &'static str {
    match reason {
        DeclineReason::Capacity => "capacity",
        DeclineReason::Pressure => "host pressure",
        DeclineReason::Disk => "disk",
        DeclineReason::Constraint => "constraint",
        DeclineReason::Unknown => "unstated",
        DeclineReason::ProjectProvisioning => "project provisioning hold",
    }
}

pub(crate) fn render_placement(executor: &ExecutorInspection, request_id: &str) -> String {
    let Some(decision) = executor
        .recent_placements
        .iter()
        .find(|decision| decision.request_id == request_id)
    else {
        return format!(
            "No recent placement decision for request `{request_id}` names executor `{}`. Read cairn://executors/{}?view=placements for the available request ids.\n",
            executor.name, executor.name
        );
    };
    let mut out = format!(
        "# Executor {} placement `{}`\n\n",
        executor.name, decision.request_id
    );
    render_placement_detail(&mut out, decision);
    out
}

/// What a placement request asked for, in the terms a reader can act on. A pin
/// is the runner's own fact, so it never hides the selector the caller wrote.
fn placement_ask(decision: &PlacementDecision) -> String {
    let selector = decision
        .selector
        .as_ref()
        .filter(|selector| !selector.is_empty());
    match (selector, &decision.pinned_executor_id) {
        (Some(selector), Some(_)) => format!(
            "pinned to the machine holding its working tree, and {}",
            selector.describe()
        ),
        (None, Some(_)) => "pinned to the machine holding its working tree".to_string(),
        (Some(selector), None) => selector.describe(),
        (None, None) => "any executor".to_string(),
    }
}

fn render_placement_detail(out: &mut String, decision: &PlacementDecision) {
    let asked = placement_ask(decision);
    out.push_str(&format!(
        "- Request `{}` ({}, asked for {asked})\n",
        decision.request_id,
        decision.mobility.as_str()
    ));
    match &decision.outcome {
        PlacementOutcome::Selected(selection) => {
            out.push_str(&format!(
                "  - Chose {} because {}\n",
                selection.executor_name,
                selection.reason.as_str()
            ));
            out.push_str(&format!(
                "  - Cache distance: {}\n",
                render_cache_distance(
                    selection.cache_distance.as_ref(),
                    decision.decided_at_unix_ms
                )
            ));
            if let Some(tie_break) = &selection.tie_break {
                out.push_str(&format!(
                    "  - Tie-break: {} ({})\n",
                    tie_break.deciding_reason,
                    tie_break.candidates.join(", ")
                ));
            }
            out.push_str(&format!(
                "  - CPU: {}\n",
                reading(
                    &selection.readings.cpu,
                    decision.decided_at_unix_ms,
                    |cpu| {
                        format!(
                            "{:.0}% busy across {} cores",
                            cpu.utilization * 100.0,
                            cpu.logical_cores
                        )
                    }
                )
            ));
            out.push_str(&format!(
                "  - Memory: {}\n",
                reading(
                    &selection.readings.memory,
                    decision.decided_at_unix_ms,
                    |memory| {
                        format!(
                            "{} available of {}",
                            bytes(memory.available_bytes),
                            bytes(memory.total_bytes)
                        )
                    }
                )
            ));
            out.push_str(&format!(
                "  - Volume: {}\n",
                reading(
                    &selection.readings.volume,
                    decision.decided_at_unix_ms,
                    |volume| {
                        format!(
                            "{} free of {}",
                            bytes(volume.free_bytes),
                            bytes(volume.total_bytes)
                        )
                    }
                )
            ));
            out.push_str(&format!(
                "  - Reserved: {} memory, {} disk, {} concurrency ({:?}); {}\n",
                bytes(selection.reservation.memory_bytes),
                bytes(selection.reservation.disk_growth_bytes),
                selection.reservation.concurrency_units,
                selection.reservation.source,
                describe_rationale(&selection.reservation_rationale)
            ));
            out.push_str(&format!(
                "  - Repository sync: {}\n",
                match selection.sync_cost {
                    PlacementSyncCost::Known { bytes: value } =>
                        format!("{} of objects to send", bytes(value)),
                    PlacementSyncCost::Unknown => "not estimable".to_string(),
                }
            ));
            if let Some(coordinate) = &selection.object_transfer {
                out.push_str(&format!(
                    "  - Objects travel as request {} attempt {} on connection generation {}\n",
                    coordinate.request_id, coordinate.attempt_id, coordinate.connection_generation
                ));
            }
            out.push_str(&format!("  - {}\n", selection.observation_reuse.describe()));
            if let Some(prediction) = &selection.prediction {
                render_prediction(out, "Predicted", prediction);
            }
        }
        PlacementOutcome::Refused { diagnostic } => {
            out.push_str(&format!("  - Refused: {diagnostic}\n"));
        }
    }
    if let Some(winner) = &decision.unpinned_winner {
        out.push_str(&format!(
            "  - Without the execution-home constraints: {} because {}\n",
            winner.executor_name,
            winner.reason.as_str()
        ));
        if let Some(prediction) = &winner.prediction {
            render_prediction(out, "    Predicted", prediction);
        }
    }
    // How the decision actually bound. `rejected` below says which machines
    // placement never asked; this says what happened to the ones it did ask,
    // which is the half that used to leave no evidence but a log line.
    if !decision.binding.is_empty() {
        out.push_str("  - Binding:\n");
        for event in &decision.binding {
            let line = match event {
                BindingEvent::Offered {
                    executor_name,
                    generation,
                    at_unix_ms,
                    ..
                } => format!(
                    "    - at {at_unix_ms}: offered to {executor_name} on connection generation {generation}\n"
                ),
                BindingEvent::Claimed {
                    executor_id,
                    at_unix_ms,
                    ..
                } => format!("    - at {at_unix_ms}: claimed by {executor_id}\n"),
                BindingEvent::Declined {
                    executor_id,
                    reason,
                    detail,
                    disk,
                    at_unix_ms,
                } => format!(
                    "    - at {at_unix_ms}: declined by {executor_id} ({}){}{}\n",
                    decline_reason_word(*reason),
                    detail
                        .as_deref()
                        .map(|detail| format!(": {detail}"))
                        .unwrap_or_default(),
                    disk.as_ref()
                        .map(|disk| format!(" — {}", disk_decline_line(disk)))
                        .unwrap_or_default()
                ),
                BindingEvent::Lost {
                    executor_id,
                    at_unix_ms,
                } => format!(
                    "    - at {at_unix_ms}: claim from {executor_id} arrived after the entry moved on\n"
                ),
                BindingEvent::GenerationEnded {
                    executor_id,
                    generation,
                    at_unix_ms,
                } => format!(
                    "    - at {at_unix_ms}: connection generation {generation} on {executor_id} ended\n"
                ),
            };
            out.push_str(&line);
        }
    }
    for rejection in &decision.rejected {
        out.push_str(&format!(
            "  - Passed over {}: {}{}\n",
            rejection.executor_name,
            rejection.reason.describe(),
            rejection
                .prediction
                .as_ref()
                .and_then(|prediction| prediction.reliability.as_ref())
                .map(|reliability| format!(" — Priced: {}", reliability.describe()))
                .unwrap_or_default()
        ));
        // A machine that was actually ranked shows its own numbers here, so
        // "why not that one" is answered by the same record that answered
        // "why this one". A structurally rejected machine was never priced
        // and prints nothing rather than a fabricated total.
        if let Some(prediction) = &rejection.prediction {
            render_prediction(out, "    Predicted", prediction);
        }
    }
    out.push('\n');
}

/// What one machine was predicted to cost, with every component's evidence.
///
/// The total and its legs are rendered together and always in the same order,
/// because a total on its own is indistinguishable from a constant. A leg that
/// is unknown says so in words; nothing here renders an absent measurement as a
/// zero, which is the failure that let an unreadable machine look like an empty
/// one.
fn render_prediction(out: &mut String, label: &str, prediction: &PlacementPrediction) {
    out.push_str(&format!(
        "  - {label}: {} to a verdict\n",
        duration_ms(prediction.predicted_verdict_ms)
    ));
    out.push_str(&format!(
        "    - Queue: {}\n",
        match prediction.queue.predicted_ms() {
            Some(value) => format!("{} · {}", duration_ms(value), prediction.queue.describe()),
            None => prediction.queue.describe(),
        }
    ));
    out.push_str(&format!(
        "    - Run: {} base, {} after contention · {}\n",
        duration_ms(prediction.base_run_ms),
        duration_ms(prediction.adjusted_run_ms),
        prediction.run.describe()
    ));
    out.push_str(&format!(
        "    - Contention: {}{}\n",
        prediction.contention.describe(),
        match prediction.contention.sample_count {
            0 => String::new(),
            1 => " (1 sample)".to_string(),
            count => format!(" ({count} samples)"),
        }
    ));
    out.push_str(&format!("    - Cache: {}\n", prediction.warmth.describe()));
    // A summand like the queue and the run, so it is named wherever the total
    // is: a winner that was penalized and still won says so too.
    if let Some(reliability) = &prediction.reliability {
        out.push_str(&format!("    - Priced: {}\n", reliability.describe()));
    }
    // History, not a reading: the reason this machine ranked behind ones that
    // never refused this entry, whatever its numbers say.
    if let Some(decline) = prediction.declined_here {
        out.push_str(&format!(
            "    - Declined here: {}\n",
            cairn_common::executor_protocol::describe_prior_decline(decline)
        ));
    }
    // Preparation is evidence, never a summand. No transfer history exists to
    // turn missing object bytes into milliseconds, and inventing a rate would be
    // exactly the kind of proxy this ranking replaced.
    out.push_str(&format!(
        "    - Preparation: {}\n",
        prediction.preparation.describe()
    ));
    out.push_str(&format!(
        "    - Profile: `{}` on {}\n",
        prediction
            .run
            .profile_key
            .as_deref()
            .unwrap_or("no command identity"),
        prediction.run.profile_context
    ));
}

/// Milliseconds an operator can read at a glance, without losing the unit.
fn duration_ms(value: u64) -> String {
    if value < 1_000 {
        return format!("{value}ms");
    }
    let seconds = value as f64 / 1_000.0;
    if seconds < 90.0 {
        return format!("{seconds:.1}s");
    }
    format!("{:.1}m", seconds / 60.0)
}

/// How a reservation came to be the number it is, in one line.
///
/// A reservation has two independent halves and they are rendered as two, never
/// as one. Concurrency is stated by the caller and cannot be learned; memory and
/// disk are learned per command identity and cannot be declared. Presenting the
/// learned half's confidence as the explanation for a declared whole-machine
/// charge is what made an over-reserved host read as a profile's conclusion
/// (CAIRN-3345).
fn describe_rationale(rationale: &ReservationRationale) -> String {
    let declared = match rationale.declared_concurrency_units {
        Some(1) => Some("1 concurrency unit declared by the caller".to_string()),
        Some(units) => Some(format!("{units} concurrency units declared by the caller")),
        None => None,
    };
    let evidence = match rationale.sample_count {
        0 => "no observation yet".to_string(),
        1 => "1 observation".to_string(),
        count => format!("{count} observations"),
    };
    let confidence = if rationale.sample_count > 0
        && rationale.sample_count < MIN_CONFIDENT_RESERVATION_SAMPLES
    {
        format!(
            " (low confidence: under {MIN_CONFIDENT_RESERVATION_SAMPLES} observations, the safety prior still governs)"
        )
    } else {
        String::new()
    };
    let key = rationale
        .profile_key
        .as_deref()
        .unwrap_or("no command identity");
    let fallback = rationale
        .fallback
        .map(|fallback| format!(", fell back because {}", fallback.as_str()))
        .unwrap_or_default();
    let learned = format!(
        "memory/disk learned from {evidence}{confidence} of `{key}` on {} with {}% headroom over a {} prior{fallback}",
        rationale.profile_context,
        rationale.headroom_percent,
        bytes(rationale.prior.memory_bytes)
    );
    match declared {
        Some(declared) => format!("{declared}; {learned}"),
        None => learned,
    }
}

#[cfg(test)]
mod rationale_tests {
    use super::*;
    use cairn_common::executor_protocol::{ReservationFallback, ResourceReservation};

    fn rationale(sample_count: u64, declared: Option<u32>) -> ReservationRationale {
        ReservationRationale {
            declared_concurrency_units: declared,
            profile_key: Some("check:rust".into()),
            profile_context: "device:executor on macos/aarch64 with toolchains [rust]".into(),
            sample_count,
            upper_peak_rss_bytes: Some(1_600),
            upper_disk_growth_bytes: Some(3_200),
            upper_duration_ms: Some(5_000),
            prior: ResourceReservation::default(),
            headroom_percent: 25,
            fallback: (sample_count < MIN_CONFIDENT_RESERVATION_SAMPLES)
                .then_some(ReservationFallback::BelowConfidenceFloor),
        }
    }

    /// The specimen this rendering exists for: a whole-machine concurrency charge
    /// the CALLER declared, explained on the record as something a single
    /// observation concluded. One line, two provenances, neither standing in for
    /// the other (CAIRN-3345).
    #[test]
    fn a_declared_charge_is_never_explained_by_a_learned_lookup() {
        let rendered = describe_rationale(&rationale(1, Some(16)));
        assert!(
            rendered.starts_with("16 concurrency units declared by the caller;"),
            "the declared half leads, in the caller's own terms: {rendered}"
        );
        assert!(
            rendered.contains("memory/disk learned from 1 observation"),
            "the learned half names what it actually covers: {rendered}"
        );
        assert!(
            rendered.contains("low confidence"),
            "one observation is not a prediction: {rendered}"
        );
    }

    /// Past the floor the estimate governs, and the qualifier goes away.
    #[test]
    fn a_confident_estimate_is_not_hedged() {
        let rendered = describe_rationale(&rationale(MIN_CONFIDENT_RESERVATION_SAMPLES, Some(1)));
        assert!(rendered.contains("5 observations"), "{rendered}");
        assert!(!rendered.contains("low confidence"), "{rendered}");
    }

    /// Work that declared nothing renders the learned half alone rather than an
    /// invented declaration of one unit.
    #[test]
    fn undeclared_work_renders_only_what_it_learned() {
        let rendered = describe_rationale(&rationale(9, None));
        assert!(!rendered.contains("declared by the caller"), "{rendered}");
        assert!(
            rendered.starts_with("memory/disk learned from"),
            "{rendered}"
        );
    }
}

/// The refusal for a name that addresses nothing, naming every name that does.
///
/// Enrolled-but-unattached names are named too. They address a real machine, so
/// leaving them out of the refusal would tell a caller that a machine it is
/// entitled to ask about does not exist.
pub(crate) fn unknown_executor(
    name: &str,
    executors: &[ExecutorInspection],
    enrolled: &[EnrolledRemote],
) -> String {
    let known = if executors.is_empty() {
        "no executor is currently attached".to_string()
    } else {
        executors
            .iter()
            .map(|executor| executor.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let enrolled = if enrolled.is_empty() {
        String::new()
    } else {
        format!(
            " Enrolled but not attached: {}.",
            enrolled
                .iter()
                .map(|remote| remote.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "No executor is named {name}. Known executors: {known}.{enrolled} Read cairn://executors for live state."
    )
}

/// How big the executor daemon is, and which of its own owners is holding it.
///
/// The two halves answer different questions and the section is only useful
/// with both. Resident size says whether there is a problem; the owner table
/// says where it is. Before this, an operator watching a helper reach a hundred
/// gigabytes could read the first and had nothing at all for the second, and
/// restarting to reclaim the machine destroyed the evidence.
///
/// Peaks are rendered beside live values because a settled snapshot is read
/// after the incident far more often than during it, and "empty now, held two
/// hundred megabytes" is the sentence that identifies a transient owner.
fn render_executor_memory(executor: &ExecutorInspection) -> String {
    let process = &executor.health.machine.process;
    let mut out = String::from("## Executor memory\n");
    out.push_str(&format!(
        "- Resident: {}\n",
        reading(
            &process.resident_bytes,
            executor.captured_at_unix_ms,
            |value| bytes(*value)
        )
    ));
    out.push_str(&format!(
        "- Physical footprint: {}\n",
        reading(
            &process.physical_footprint_bytes,
            executor.captured_at_unix_ms,
            |value| bytes(*value)
        )
    ));
    out.push_str(&format!(
        "- Mapped address space: {}\n",
        reading(
            &process.virtual_bytes,
            executor.captured_at_unix_ms,
            |value| bytes(*value)
        )
    ));
    out.push_str(&format!(
        "- Allocator: {}\n",
        reading(&process.allocator, executor.captured_at_unix_ms, |value| {
            allocator_split(value)
        })
    ));

    let retained = &process.retained;
    if retained.is_unreported() {
        out.push_str(
            "- Retained state: not reported by this executor (it predates retained-state \
             telemetry)\n\n",
        );
        return out;
    }
    out.push_str(&format!(
        "- Retained state measured: {}\n",
        age(executor
            .captured_at_unix_ms
            .saturating_sub(retained.measured_at_unix_ms))
    ));
    out.push_str(&format!(
        "- Attributed total: {} across every owner below (estimated from owned string and \
         vector capacities, so it is a floor rather than a reconciliation of resident size)\n",
        bytes(retained.total_estimated_bytes())
    ));

    let owners: Vec<_> = retained
        .owners()
        .into_iter()
        .filter(|(_, owner)| !owner.is_untouched())
        .collect();
    if owners.is_empty() {
        out.push_str("- Owners: every owner is empty and has never held anything\n\n");
        return out;
    }
    out.push_str("\n| Owner | Now | Bytes now | Peak | Peak bytes |\n");
    out.push_str("| --- | --: | --: | --: | --: |\n");
    for (kind, owner) in owners {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            kind.label(),
            owner.entries,
            bytes(owner.estimated_bytes),
            owner.peak_entries,
            bytes(owner.peak_estimated_bytes),
        ));
    }
    out.push('\n');
    out
}

/// The allocator's split, written so the sentence itself says which half the
/// reader is looking at.
///
/// The owner table above accounts only for memory something is holding, so a
/// resident set it cannot explain reads as a failure of attribution. It is not,
/// when the unexplained bytes are free: the program released them and the
/// allocator kept them, and no owner exists to name however many are added.
/// "Free but retained" is the phrase that closes that gap in one line.
fn allocator_split(state: &AllocatorState) -> String {
    format!(
        "{} in use, {} free but retained (arena {}, mmapped {} across {} chunks)",
        bytes(state.in_use_bytes),
        bytes(state.free_retained_bytes),
        bytes(state.arena_bytes),
        bytes(state.mmapped_bytes),
        state.mmap_chunk_count,
    )
}

/// What the executor's own CPU circuit breaker can currently see.
///
/// Blindness is judged against the clock here, not read off the reported state,
/// for the same reason the executor judges it that way: the state only advances
/// when the sampler runs, and a sampler that has stopped cannot report that it
/// stopped.
fn cpu_brake_summary(cpu: &CpuAdmissionHealth, captured_at_unix_ms: u64) -> String {
    let Some(measured_at) = cpu.measured_at_unix_ms else {
        return "never measured on this machine — the brake cannot engage".to_string();
    };
    let age_ms = captured_at_unix_ms.saturating_sub(measured_at);
    if age_ms > CPU_ADMISSION_BLIND_AFTER_MS {
        return format!(
            "BLIND — last reading {}, past the {}s bound; non-interactive admission is held",
            age(age_ms),
            CPU_ADMISSION_BLIND_AFTER_MS / 1_000
        );
    }
    let utilization = match cpu.utilization {
        Some(value) => format!("{:.0}%", value * 100.0),
        None => "unreported".to_string(),
    };
    match cpu.state {
        CpuAdmissionState::Pressured => format!(
            "pressured at {utilization} — holding new non-interactive work (measured {})",
            age(age_ms)
        ),
        _ => format!("accepting at {utilization} (measured {})", age(age_ms)),
    }
}

/// What the runner's supervisor is doing about the colocated executor, or the
/// empty string when there is nothing to say.
///
/// Rendered beside `local` and, crucially, when `local` is not attached at all:
/// that is when a supervisor waiting on a held generation, or a successor
/// failing to start, is the only account of why (cairn/4569, where `local` read
/// "online, generation 1" for hours while every successor failed).
pub(crate) fn render_colocated_supervision(
    substrate: Option<&ExecutorSubstrateEvidence>,
    supervision: &ColocatedSupervision,
    attached: bool,
    captured_at_unix_ms: u64,
) -> String {
    let since = |at: u64| {
        format!(
            "{} ({})",
            crate::clock::stamp_millis_with_seconds(at as i64)
                .unwrap_or_else(|| "an unknown time".to_string()),
            age(captured_at_unix_ms.saturating_sub(at))
        )
    };
    let mut lines = Vec::new();
    if let Some(hold) = &supervision.generation_hold {
        lines.push(format!(
            "- Supervisor: waiting since {}: {}",
            since(hold.observed_since_unix_ms),
            generation_hold_cause(hold, captured_at_unix_ms)
        ));
    } else if let Some(failing_since) = supervision.failing_since_unix_ms {
        lines.push(format!(
            "- Supervisor: successor failing since {} ({} {}): {}",
            since(failing_since),
            supervision.failed_attempts,
            if supervision.failed_attempts == 1 {
                "attempt"
            } else {
                "attempts"
            },
            supervision
                .last_cause
                .as_deref()
                .unwrap_or("no cause was recorded")
        ));
    } else if let Some(evidence) = substrate.filter(|_| !attached) {
        lines.push(format!(
            "- Supervisor: {} since {}",
            substrate_state_label(evidence.state),
            since(evidence.since_unix_ms)
        ));
    }
    if let Some(retirement) = &supervision.last_retirement {
        lines.push(format!(
            "- Last handoff: {} ({})",
            describe_retirement(retirement),
            age(captured_at_unix_ms.saturating_sub(retirement.completed_at_unix_ms))
        ));
    }
    for survivor in &supervision.workload_survivors {
        lines.push(format!(
            "- Survivor: {}",
            describe_survivor(survivor, captured_at_unix_ms)
        ));
    }
    for cell in &supervision.unresolved_cells {
        lines.push(format!(
            "- Unresolved cell: `{}`{}: what ran there is unknown since {} ago, because {}; it clears on its own at the next boot of this host; {}",
            cell.cell_authority,
            cell.retired_executor_pid
                .map(|pid| format!(" (retired executor pid {pid})"))
                .unwrap_or_default(),
            age(captured_at_unix_ms.saturating_sub(cell.first_seen_unix_ms))
                .trim_end_matches(" ago"),
            cell.reason,
            next_look(cell.next_verification_unix_ms, captured_at_unix_ms)
        ));
    }
    if lines.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n## Local executor supervision\n\n");
    if !attached {
        out.push_str("The colocated executor `local` is not attached to this runner.\n\n");
    }
    for line in lines {
        out.push_str(&line);
        out.push('\n');
    }
    out
}

fn generation_hold_cause(hold: &ColocatedGenerationHold, captured_at_unix_ms: u64) -> String {
    let holder = match hold.holder_pid {
        Some(pid) => format!("pid {pid}"),
        None => "a process that left no record of itself".to_string(),
    };
    let recorded = hold
        .holder_acquired_at_unix_ms
        .map(|at| {
            format!(
                " (recorded {})",
                age(captured_at_unix_ms.saturating_sub(at))
            )
        })
        .unwrap_or_default();
    match hold.reason {
        GenerationHoldReason::Unidentified => format!(
            "generation held by {holder}{recorded}, which this runner did not start and will not signal; `local` starts when it exits"
        ),
        GenerationHoldReason::RetirementStuck {
            phase: RetirementPhase::Verify,
        } => format!(
            "outgoing executor {holder}{recorded} is gone, but work it launched survived SIGKILL (listed below); looking again a few times before starting `local` anyway, since a survivor holds only its own cell"
        ),
        GenerationHoldReason::RetirementStuck { phase } => format!(
            "outgoing executor {holder}{recorded} is still alive after retirement reached {}; retrying, and starting no successor while it holds the generation",
            phase.label()
        ),
    }
}

/// One survivor of a retirement, as the person reading the fleet needs it:
/// which process, where, since when, what has been done about it, and when it
/// will be looked at next.
fn describe_survivor(survivor: &WorkloadSurvivor, captured_at_unix_ms: u64) -> String {
    let mut line = format!(
        "pid {} ({}, start token `{}`) outlived its executor's retirement at verification; cell `{}`, lock {}; first seen {}; {} verified SIGKILL{} sent",
        survivor.pid,
        survivor.kind.label(),
        survivor.start_token,
        survivor.cell_authority,
        if survivor.cell_lock_held { "still held" } else { "free" },
        age(captured_at_unix_ms.saturating_sub(survivor.first_seen_unix_ms)),
        survivor.kill_attempts,
        if survivor.kill_attempts == 1 { "" } else { "s" },
    );
    if let Some(why) = &survivor.unsignalled_because {
        line.push_str(&format!("; not signalled at the last look: {why}"));
    }
    line.push_str("; ");
    line.push_str(&next_look(
        survivor.next_verification_unix_ms,
        captured_at_unix_ms,
    ));
    line
}

fn next_look(next_verification_unix_ms: u64, captured_at_unix_ms: u64) -> String {
    match next_verification_unix_ms.saturating_sub(captured_at_unix_ms) {
        0 => "re-verification due now".to_string(),
        next => format!("next re-verification in {}", duration_ms(next)),
    }
}

fn describe_retirement(retirement: &ExecutorRetirementSummary) -> String {
    let whose = match retirement.origin {
        RetirementOrigin::OwnChild => "this runner's executor",
        RetirementOrigin::PriorRunner => "left running by a previous runner",
    };
    let seconds = |ms: u64| format!("{:.1}s", ms as f64 / 1_000.0);
    let workload = &retirement.workload;
    let mut described = match (retirement.outcome, retirement.escalated_to) {
        (RetirementOutcome::Retired, None) if retirement.graceful_ms == 0 => format!(
            "retired pid {} ({whose}); it had already exited",
            retirement.pid
        ),
        (RetirementOutcome::Retired, None) => format!(
            "retired pid {} ({whose}); it exited {} after being asked",
            retirement.pid,
            seconds(retirement.graceful_ms)
        ),
        (RetirementOutcome::Retired, Some(phase)) => format!(
            "retired pid {} ({whose}); escalated to {} after a {} graceful window",
            retirement.pid,
            phase.label(),
            seconds(retirement.graceful_ms)
        ),
        (
            RetirementOutcome::Stuck {
                phase: RetirementPhase::Verify,
            },
            _,
        ) => {
            let plural = |count: u32, one: &str, many: &str| {
                format!("{count} {}", if count == 1 { one } else { many })
            };
            let mut failures = Vec::new();
            if !workload.surviving_pids.is_empty() {
                failures.push(format!(
                    "workload pids {:?} are still running",
                    workload.surviving_pids
                ));
            }
            if workload.held_cell_locks > 0 {
                failures.push(format!(
                    "{} still held",
                    plural(workload.held_cell_locks, "cell lock is", "cell locks are")
                ));
            }
            if workload.unreadable_cells > 0 {
                failures.push(format!(
                    "{} could not be decoded, so what ran there is unknown",
                    plural(
                        workload.unreadable_cells,
                        "cell state file",
                        "cell state files"
                    )
                ));
            }
            if let Some(why) = &workload.ownership_unconfirmed {
                failures.push(format!("nothing was signalled: {why}"));
            }
            format!(
                "pid {} ({whose}) is gone, but verification failed: {}",
                retirement.pid,
                failures.join("; ")
            )
        }
        (RetirementOutcome::Stuck { phase }, _) => format!(
            "pid {} ({whose}) is still alive after retirement reached {}",
            retirement.pid,
            phase.label()
        ),
    };
    if workload.reaped > 0 {
        described.push_str(&format!(
            "; SIGKILLed {} workload process{} it had left running",
            workload.reaped,
            if workload.reaped == 1 { "" } else { "es" }
        ));
    }
    if let Some(because) = &workload.left_alone_because {
        described.push_str(&format!(
            "; its recorded workload was left alone: {because}"
        ));
    }
    described
}

fn substrate_state_label(state: ExecutorSubstrateState) -> &'static str {
    match state {
        ExecutorSubstrateState::SupervisorSpawning => "starting",
        ExecutorSubstrateState::SupervisorRespawning => "restarting",
        ExecutorSubstrateState::PermanentRefusal => "refused by this runner's executor binary",
        ExecutorSubstrateState::ProtocolAttaching => "attaching",
        ExecutorSubstrateState::InitialStorageSweep => "sweeping storage",
        ExecutorSubstrateState::StorageAccounting => "accounting storage",
        ExecutorSubstrateState::DispatchPreparing => "preparing dispatch",
        ExecutorSubstrateState::SlotAdoption => "adopting cells",
        ExecutorSubstrateState::CapacityBusy => "at capacity",
        ExecutorSubstrateState::ExecutionRunning => "running work",
        ExecutorSubstrateState::ConnectedStalled => "stalled",
        ExecutorSubstrateState::Draining => "draining",
        ExecutorSubstrateState::GenerationHeld => "generation held by another process",
    }
}

fn link_summary(executor: &ExecutorInspection) -> String {
    match executor.health.status {
        // The link being up is not the whole answer when the machine itself
        // says it cannot take work.
        ExecutorHealthStatus::Online => {
            match executor
                .occupancy
                .substrate_state
                .as_ref()
                .filter(|evidence| {
                    matches!(
                        evidence.state,
                        ExecutorSubstrateState::ConnectedStalled
                            | ExecutorSubstrateState::Draining
                            | ExecutorSubstrateState::PermanentRefusal
                            | ExecutorSubstrateState::GenerationHeld
                    )
                }) {
                Some(evidence) => format!(
                    "online, but {}{}",
                    substrate_state_label(evidence.state),
                    evidence
                        .diagnostic
                        .as_deref()
                        .map(|diagnostic| format!(": {diagnostic}"))
                        .unwrap_or_default()
                ),
                None => "online".to_string(),
            }
        }
        ExecutorHealthStatus::Stale => format!(
            "stale — no heartbeat for {}",
            age(executor.health.heartbeat_age_ms)
        ),
    }
}

/// One reading, rendered as its value with an age, or as the named reason there
/// is no value. A gap never renders as a number.
fn reading<T>(
    measurement: &Measurement<T>,
    captured_at_unix_ms: u64,
    render: impl Fn(&T) -> String,
) -> String {
    match &measurement.reading {
        MeasurementReading::Measured { value } => format!(
            "{} (measured {})",
            render(value),
            age(captured_at_unix_ms.saturating_sub(measurement.measured_at_unix_ms))
        ),
        MeasurementReading::Unavailable { reason, detail } => {
            let named = format!("unavailable: {}", reason.describe());
            match detail {
                Some(detail) => format!("{named} ({detail})"),
                None => named.to_string(),
            }
        }
    }
}

fn short_commit(commit: &str) -> String {
    commit.chars().take(8).collect()
}

fn render_cache_distance(
    measurement: Option<&cairn_common::cache_distance::MeasuredDistance>,
    now: u64,
) -> String {
    let Some(measurement) = measurement else {
        return "unmeasured".into();
    };
    let distance = &measurement.distance;
    format!(
        "{} files / {} crates ({} transitive) / {} JS workspaces to {}, {}",
        distance.files,
        distance.crates,
        distance.crates_transitive,
        distance.js_workspaces,
        short_commit(&measurement.target_commit),
        age(now.saturating_sub(measurement.measured_at_unix_ms))
    )
}

/// Public keys come from cached owner metadata or the project-scoped slot path,
/// never from an opaque project UUID or an executor-local absolute path.
fn slot_project_name(cell: &cairn_common::executor_protocol::PersistentCellState) -> String {
    if let Some(key) = cell
        .occupancy
        .command
        .as_ref()
        .and_then(|command| command.owner.as_ref())
        .and_then(|owner| owner.project_key.as_ref())
    {
        return key.clone();
    }
    let path = cell.path.replace('\\', "/");
    let parts = path.split('/').collect::<Vec<_>>();
    parts
        .windows(3)
        .find(|parts| parts[0] == "build-slots" && parts[1] != ".authority")
        .map(|parts| parts[1].to_string())
        .unwrap_or_else(|| "name unavailable".into())
}

/// The commands this machine ended, and what each teardown left alive.
///
/// Said only when there is something to say, except where the machine cannot
/// stop a stalled command at all: there the absence of stops is not evidence
/// that nothing stalled, and the reader is owed that.
fn render_stopped_commands(
    stopped: &[cairn_common::executor_protocol::StoppedCommandReport],
    os: &str,
    captured_at_unix_ms: u64,
) -> String {
    let stall_detection = matches!(os, "macos" | "linux");
    if stopped.is_empty() && stall_detection {
        return String::new();
    }
    let mut out = String::from("\n## Stopped commands\n");
    if !stall_detection {
        out.push_str(
            "- Stall detection: unavailable on this platform (it cannot read a command tree's CPU, and output silence alone is not evidence of a stall); only cancellation stops a command here\n",
        );
    }
    for stop in stopped.iter().rev() {
        let groups = if stop.signalled_groups.is_empty() {
            String::new()
        } else {
            format!(
                "; signalled groups {}",
                stop.signalled_groups
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let survivors = if stop.surviving_pids.is_empty() {
            String::new()
        } else {
            format!(
                " — STILL RUNNING: pids {} (each fences its cell until it exits)",
                stop.surviving_pids
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        out.push_str(&format!(
            "- {} ({}, request {}): {}{groups}{survivors}\n",
            stop.header,
            age(captured_at_unix_ms.saturating_sub(stop.stopped_at_unix_ms)),
            stop.request_id,
            stop.cause.describe(),
        ));
    }
    out
}

/// The last completed refresh pass, and every phase of it long enough to
/// matter, separating time spent queueing for pool state from time spent
/// working: the first is a machine waiting on a holder, the second a phase
/// that is itself slow.
fn render_refresh_timings(
    refresh: &cairn_common::executor_protocol::LivenessRefreshTimings,
) -> String {
    /// Phases shorter than this are noise beside the ones worth reading.
    const SHOWN_FROM_MS: u64 = 100;
    let phases: Vec<String> = refresh
        .phases
        .iter()
        .filter(|timing| timing.elapsed_ms >= SHOWN_FROM_MS)
        .map(|timing| {
            let waited = timing
                .pool_state_wait_ms
                .filter(|waited| *waited >= SHOWN_FROM_MS)
                .map(|waited| {
                    format!(
                        ", of which {} waiting for pool state",
                        refresh_duration(waited)
                    )
                })
                .unwrap_or_default();
            format!(
                "{} {}{waited}",
                timing.phase.label(),
                refresh_duration(timing.elapsed_ms)
            )
        })
        .collect();
    if phases.is_empty() {
        refresh_duration(refresh.pass_ms)
    } else {
        format!(
            "{} ({})",
            refresh_duration(refresh.pass_ms),
            phases.join("; ")
        )
    }
}

fn render_io_pressure(pressure: &cairn_common::executor_protocol::IoPressure) -> String {
    let share = |millis: u32| format!("{:.1}%", f64::from(millis) / 10.0);
    let mut line = format!(
        "{} of time fully stalled (10 s), {} (60 s)",
        share(pressure.full_avg10_millis),
        share(pressure.full_avg60_millis)
    );
    if let Some(device) = &pressure.slowest_device {
        line.push_str(&match device.mean_write_ms {
            Some(mean) => format!(
                "; slowest device {} mean write {}, {} I/Os in flight",
                device.device,
                refresh_duration(mean),
                device.in_flight
            ),
            None => format!(
                "; {} stalled: no I/O completed in {} while busy throughout, {} I/Os in flight",
                device.device,
                refresh_duration(device.window_ms),
                device.in_flight
            ),
        });
    }
    line
}

/// The `## GPU` section: one line per device, or the named reason there are
/// none.
///
/// A unified device's pool is quoted from the machine's own `Memory` reading
/// and from nowhere else, because it is the same memory: printing a second
/// figure for it is how a reader would come to add the two together.
fn render_gpus(
    gpus: &Measurement<Vec<GpuDevice>>,
    memory: &Measurement<MachineMemory>,
    captured_at_unix_ms: u64,
) -> String {
    let mut out = String::from("## GPU\n");
    let devices = match &gpus.reading {
        MeasurementReading::Unavailable { reason, detail } => {
            out.push_str(&format!("- UNMEASURED: {}", reason.describe()));
            if let Some(detail) = detail {
                out.push_str(&format!(" ({detail})"));
            }
            out.push_str("\n\n");
            return out;
        }
        MeasurementReading::Measured { value } => value,
    };
    let reading_age_ms = gpus.age_ms(captured_at_unix_ms);
    let duration = |ms: u64| age(ms).trim_end_matches(" ago").to_string();
    if reading_age_ms > MACHINE_READING_STALE_AFTER_MS {
        out.push_str(&format!(
            "- STALE: last reading {} ago, past the {} sampler bound; these are history, and \
             anything that acts on GPU headroom must treat them as unmeasured\n",
            duration(reading_age_ms),
            duration(MACHINE_READING_STALE_AFTER_MS),
        ));
    }
    let measured = format!("(measured {})", age(reading_age_ms));
    if devices.is_empty() {
        out.push_str(&format!(
            "- No GPU devices: the reader works and found none {measured}\n\n"
        ));
        return out;
    }
    for device in devices {
        let identity = match (&device.uuid, device.cores) {
            (Some(uuid), Some(cores)) => format!(" ({uuid}, {cores} cores)"),
            (Some(uuid), None) => format!(" ({uuid})"),
            (None, Some(cores)) => format!(" ({cores} cores)"),
            (None, None) => String::new(),
        };
        out.push_str(&format!(
            "- GPU {} {}{identity}: {}; {} {measured}\n",
            device.index,
            device.model,
            render_gpu_memory(&device.memory, memory),
            gpu_field("utilisation", &device.utilization_millis, |millis| {
                format!("utilisation {:.0}%", f64::from(*millis) / 10.0)
            }),
        ));
    }
    out.push('\n');
    out
}

fn render_gpu_memory(
    reading: &MeasurementReading<GpuMemory>,
    system_memory: &Measurement<MachineMemory>,
) -> String {
    gpu_field("memory", reading, |memory| match memory {
        GpuMemory::Discrete {
            used_bytes,
            total_bytes,
        } => format!(
            "discrete memory {} used of {}, not part of system memory",
            bytes(*used_bytes),
            bytes(*total_bytes)
        ),
        GpuMemory::Unified {
            gpu_allocated_bytes,
            source,
            pool,
        } => {
            // The Memory reading is quoted as the pool only where the executor
            // says it measures the host. Under a cgroup limit it is the limit,
            // which bounds the executor's processes but not the GPU's
            // allocations, and quoting it would put 100 GiB of GPU memory
            // inside a 32 GiB "pool".
            let pool = match pool {
                MeasurementReading::Measured {
                    value: UnifiedPool::HostMemory,
                } => match system_memory.value() {
                    Some(pool) => format!(
                        "{} available of {}, the Memory reading above, not a second number",
                        bytes(pool.available_bytes),
                        bytes(pool.total_bytes)
                    ),
                    None => "the Memory reading above is unavailable, so the pool's size is too"
                        .to_string(),
                },
                MeasurementReading::Measured {
                    value: UnifiedPool::CgroupLimited { limit_bytes },
                } => format!(
                    "pool UNMEASURED: the Memory reading above is this executor's cgroup limit of {}, which bounds its own processes but not the GPU's allocations",
                    bytes(*limit_bytes)
                ),
                MeasurementReading::Unavailable { reason, detail } => match detail {
                    Some(detail) => format!("pool unavailable: {} ({detail})", reason.describe()),
                    None => format!("pool unavailable: {}", reason.describe()),
                },
            };
            let allocated = match source {
                UnifiedAllocationSource::NvmlComputeProcesses => {
                    gpu_field("GPU-allocated", gpu_allocated_bytes, |allocated| {
                        format!("GPU-allocated {} by compute processes", bytes(*allocated))
                    })
                }
                UnifiedAllocationSource::AppleDriverInUse => {
                    gpu_field("GPU driver in use", gpu_allocated_bytes, |allocated| {
                        format!("GPU driver in use {}", bytes(*allocated))
                    })
                }
            };
            format!("unified memory, one pool with system memory: {pool}; {allocated}")
        }
    })
}

/// One per-device field: its value, or `<label> unavailable: <reason>`.
fn gpu_field<T>(
    label: &str,
    reading: &MeasurementReading<T>,
    render: impl Fn(&T) -> String,
) -> String {
    match reading {
        MeasurementReading::Measured { value } => render(value),
        MeasurementReading::Unavailable { reason, detail } => match detail {
            Some(detail) => format!("{label} unavailable: {} ({detail})", reason.describe()),
            None => format!("{label} unavailable: {}", reason.describe()),
        },
    }
}

fn refresh_duration(ms: u64) -> String {
    if ms >= 1_000 {
        format!("{:.1} s", ms as f64 / 1_000.0)
    } else {
        format!("{ms} ms")
    }
}

fn age(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms}ms ago");
    }
    let seconds = ms / 1_000;
    if seconds < 60 {
        return format!("{seconds}s ago");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{minutes}m ago");
    }
    format!("{}h ago", minutes / 60)
}

fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = value as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

/// The toolchain line for the fleet listing: the advertised set, and when that
/// is empty, the reason the machine itself gave.
///
/// A bare "none advertised" is the fleet fact an operator can act on least,
/// because it reads the same whether the machine has no toolchain or its probe
/// never worked. Those are opposite facts, exactly as a gap and a zero are
/// elsewhere in this file, so the emptiness is reported with the account it was
/// probed as and the failure that produced it.
fn toolchain_summary(capabilities: &ExecutorCapabilities) -> String {
    if !capabilities.toolchains.is_empty() {
        return capabilities.toolchains.join(", ");
    }
    let Some(detection) = &capabilities.toolchain_detection else {
        return "none advertised".to_string();
    };
    let failures = detection
        .probes
        .iter()
        .filter(|probe| !probe.detected)
        .map(|probe| format!("{}: `{}` {}", probe.toolchain, probe.command, probe.detail))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        return format!(
            "none advertised (probed for none, as account {})",
            detection.account
        );
    }
    format!(
        "none advertised (as account {} — {})",
        detection.account,
        failures.join("; ")
    )
}

/// One machine's build-prerequisite verdict for the fleet listing, or nothing
/// when the machine is equipped and there is no news.
///
/// A machine whose executor predates the check is reported rather than passed
/// over in silence, because "nothing is wrong here" and "nobody asked" are
/// exactly the pair this listing must not merge — the same rule the memory and
/// CPU readings follow. Merging them is the whole defect: a Linux machine with
/// no `libwebkit2gtk-4.1-dev` advertises `rust`, is placed on a lint lane, and
/// returns an infrastructure exit that names nothing an operator can repair.
fn build_prerequisite_summary(capabilities: &ExecutorCapabilities) -> Option<String> {
    let Some(check) = &capabilities.build_prerequisites else {
        return Some("not reported (this machine's executor predates the check)".to_string());
    };
    match check.status {
        BuildPrerequisiteStatus::Satisfied => None,
        BuildPrerequisiteStatus::Missing => Some(format!("MISSING — {}", one_line(&check.detail))),
        BuildPrerequisiteStatus::Unchecked => {
            Some(format!("not checked — {}", one_line(&check.detail)))
        }
    }
}

/// The machine's prerequisite verdict in full, in the script's own words.
///
/// The script's text is reproduced rather than summarized because the summary
/// is precisely what makes the failure unactionable: `CAIRN_INFRASTRUCTURE_EXIT`
/// says only that Cairn could not run the check, while the script names the
/// host, the packages it could not find, and the command that installs them.
///
/// The verdict is stamped "at attach" because that is when it was taken and
/// nothing refreshes it: a machine provisioned while it stays attached goes on
/// reporting the old answer until it reattaches. Saying so is the difference
/// between a known seam and a quiet lie.
fn render_build_prerequisites(capabilities: &ExecutorCapabilities) -> String {
    let Some(check) = &capabilities.build_prerequisites else {
        return "- Build prerequisites: not reported by this executor\n".to_string();
    };
    let status = match check.status {
        BuildPrerequisiteStatus::Satisfied => "satisfied",
        BuildPrerequisiteStatus::Missing => "MISSING",
        BuildPrerequisiteStatus::Unchecked => "could not be checked",
    };
    let source = if check.script.is_empty() {
        "no script is declared for this platform".to_string()
    } else {
        format!("`{}`", check.script)
    };
    let mut out =
        format!("- Build prerequisites: {status} — {source}, as of this machine's last attach\n");
    for line in check
        .detail
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        out.push_str(&format!("  - {line}\n"));
    }
    out
}

/// Each project the runner knows, and whether placement lets this machine
/// take its work. The reasons are placement's own, so a machine that reads
/// healthy everywhere else still says here why a project never lands on it.
fn render_project_eligibility(
    eligibility: &[cairn_common::executor_protocol::ProjectEligibility],
) -> String {
    if eligibility.is_empty() {
        return String::new();
    }
    let mut out = String::from("- Project eligibility:\n");
    for project in eligibility {
        if project.refusals.is_empty() {
            out.push_str(&format!("  - {}: eligible\n", project.project_id));
            continue;
        }
        for refusal in &project.refusals {
            let hint = match refusal {
                PlacementRejectionReason::BuildPrerequisitesMissing { .. } => {
                    " (lifted when the machine re-attaches with the prerequisites installed)"
                }
                _ => "",
            };
            out.push_str(&format!(
                "  - {}: not eligible — {}{hint}\n",
                project.project_id,
                one_line(&refusal.describe())
            ));
        }
    }
    out
}

/// A project's refusals as the listing's one line, or `None` when eligible.
fn ineligibility_summary(refusals: &[PlacementRejectionReason]) -> Option<String> {
    let parts: Vec<String> = refusals
        .iter()
        .map(|refusal| match refusal {
            PlacementRejectionReason::PreparationToolchainMissing { missing, .. } => {
                format!("setup needs {}", missing.join(", "))
            }
            PlacementRejectionReason::PreparationRefused { program, .. } => {
                format!("setup needs `{program}`, which is not on its PATH")
            }
            PlacementRejectionReason::BuildPrerequisitesMissing { .. } => {
                "rust work (build prerequisites MISSING)".to_string()
            }
            other => other.describe(),
        })
        .collect();
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// Collapse a multi-line diagnostic onto the listing's single line.
fn one_line(detail: &str) -> String {
    let joined = detail
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" — ");
    if joined.is_empty() {
        "no detail reported".to_string()
    } else {
        joined
    }
}

/// What this machine has actually measured about its own containment, edge by
/// edge.
///
/// The point of the page is that an operator can tell why a machine qualifies
/// for an envelope or why it does not, without reading the code. So every edge
/// prints, an unmeasured one prints its reason verbatim, and the build and boot
/// the measurements are bound to print beside them -- those are what turn "this
/// was measured" into "this is still true".
///
/// Colocated and enrolled rows mean different things and are labelled as such.
/// A colocated machine's probe ran in the process tree that will spawn the
/// batch; an enrolled machine's row is that machine's own authenticated claim,
/// which binding cannot upgrade into proof.
fn render_containment(capabilities: &ExecutorCapabilities, colocated: bool) -> String {
    let Some(containment) = &capabilities.containment else {
        return "- Containment: not reported by this executor (it predates containment reporting, so every envelope is refused)\n"
            .to_string();
    };
    let platform = containment.platform.map_or_else(
        || "no published platform identity".to_string(),
        |platform| platform.target().to_string(),
    );
    let mut out = format!(
        "- Containment: {} measurement, build {}, boot {}, platform {}\n",
        if colocated {
            "colocated (probed inside the process tree that spawns this machine's work)"
        } else {
            "enrolled (this machine's own authenticated claim about itself)"
        },
        containment.executor_build_id,
        containment.boot_generation,
        platform
    );
    if containment.observations.is_empty() {
        out.push_str("  - this executor measured no edge\n");
        return out;
    }
    for observation in &containment.observations {
        let fingerprint = containment
            .current_fingerprints
            .iter()
            .find(|entry| entry.edge == observation.edge);
        let currency = match fingerprint {
            Some(entry) if entry.fingerprint.is_some() => "",
            _ => " [no current fingerprint: an input that could widen this edge cannot be read, so no positive evidence stands]",
        };
        out.push_str(&format!(
            "  - {:?}: {}{currency}\n",
            observation.edge,
            describe_disposition(&observation.disposition)
        ));
    }
    out
}

fn describe_disposition(disposition: &cairn_common::executor_protocol::EdgeDisposition) -> String {
    use cairn_common::executor_protocol::EdgeDisposition;
    match disposition {
        EdgeDisposition::Refused { observed, .. } => {
            format!("Refused — {}", observed.detail)
        }
        EdgeDisposition::Constrained {
            envelope, observed, ..
        } => format!("Constrained to {envelope:?} — {}", observed.detail),
        EdgeDisposition::Ambient { observed, .. } => {
            format!("Ambient — {}", observed.detail)
        }
        EdgeDisposition::Unmeasured { reason } => format!("Unmeasured — {reason}"),
    }
}

/// The full probe record for one machine: what ran, as whom, where the program
/// resolved, and what came back — for toolchains that were found as well as
/// those that were not.
///
/// The account is printed even on success because it is what makes a later
/// absence intelligible: a per-user toolchain install belongs to one account, so
/// the account a probe ran as is frequently the whole explanation for what it
/// could and could not see.
fn render_toolchain_probes(capabilities: &ExecutorCapabilities) -> String {
    let Some(detection) = &capabilities.toolchain_detection else {
        return "- Toolchain probes: not reported by this executor\n".to_string();
    };
    let mut out = format!(
        "- Toolchain probes: run as account {} (home {})\n",
        detection.account, detection.home
    );
    if detection.probes.is_empty() {
        out.push_str("  - this executor probed for no toolchains\n");
        return out;
    }
    for probe in &detection.probes {
        out.push_str(&format!(
            "  - {}: {} — `{}` at {} — {}\n",
            probe.toolchain,
            if probe.detected {
                "detected"
            } else {
                "not detected"
            },
            probe.command,
            probe.resolved_path.as_deref().unwrap_or("not on PATH"),
            probe.detail
        ));
    }
    out
}

#[cfg(test)]
mod storage_section_tests {
    use super::render_executor;
    use super::tests::inspection;
    use cairn_common::executor_protocol::{
        DiskAccounting, DiskCategoryAccounting, DiskHealthStatus, DiskPathUsage, MachineVolume,
        Measurement, OccupiedTreeSkip, PersistentCellLifecycle, QuarantineTargetStrip,
        SkippedEntry, SkippedEntryOperation, SkippedEntryReason, SlotState, StoragePressureBand,
        StoragePressureOutcome, StoragePressureReport, StorageReclaimRung, StorageRungReclaim,
        StorageWithheld, StorageWithheldReason,
    };

    const GIB: u64 = 1024 * 1024 * 1024;

    fn tight_local() -> cairn_common::executor_protocol::ExecutorInspection {
        let mut executor = inspection("local", true);
        let at = executor.captured_at_unix_ms;
        executor.health.disk.status = DiskHealthStatus::Tight;
        executor.health.disk.watermark_bytes = 2 * GIB;
        executor.health.disk.critical_free_bytes = 16 * GIB;
        executor.health.disk.pressure_free_bytes = 64 * GIB;
        executor.health.machine.volume = Measurement::measured(
            at,
            MachineVolume {
                total_bytes: 1_800 * GIB,
                free_bytes: 54 * GIB,
            },
        );
        executor.health.machine.disk_accounting = Measurement::measured(
            at.saturating_sub(120_000),
            DiskAccounting {
                used_bytes: 346 * GIB,
                categories: DiskCategoryAccounting {
                    retired_homes_bytes: 0,
                    managed_objects_bytes: 0,
                    live_cells_bytes: 24 * GIB,
                    warm_caches_bytes: 0,
                    external_managed_caches_bytes: 93 * GIB,
                    quarantines_bytes: 104 * GIB,
                    temporary_other_bytes: 125 * GIB,
                },
                other_top: vec![DiskPathUsage {
                    path: "cairn.db".into(),
                    bytes: 16 * GIB,
                }],
                ..DiskAccounting::default()
            },
        );
        executor
    }

    /// The section the CAIRN-4860 operator had to reconstruct by hand: the
    /// verdict against its thresholds, Cairn's bytes by category, and what of
    /// the volume is not Cairn's.
    #[test]
    fn the_storage_section_names_thresholds_categories_and_what_is_not_cairns() {
        let rendered = render_executor(&tight_local());
        assert!(rendered.contains("## Storage\n"), "{rendered}");
        assert!(
            rendered.contains("54.0 GiB free of 1.8 TiB")
                && rendered.contains(
                    "Tight (pressure line 64.0 GiB, critical 16.0 GiB, admission floor 2.0 GiB)"
                ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "Cairn holds 346.0 GiB (logical bytes, accounting 2m ago): live cells 24.0 GiB"
            ) && rendered.contains("external caches 93.0 GiB")
                && rendered.contains("quarantines 104.0 GiB")
                && rendered.contains("other 125.0 GiB (cairn.db 16.0 GiB)"),
            "{rendered}"
        );
        assert!(
            rendered
                .contains("Outside Cairn's accounting (inferred: volume used minus Cairn's accounting): ~1.4 TiB"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- Verdict: volume Tight, custody"),
            "{rendered}"
        );
        assert!(
            rendered.contains("Last pressure pass: none since this executor started"),
            "{rendered}"
        );
    }

    /// A total short by an unknown amount cannot support a subtraction.
    #[test]
    fn partial_accounting_suppresses_the_not_cairns_inference() {
        let mut executor = tight_local();
        let at = executor.captured_at_unix_ms;
        let mut accounting = executor
            .health
            .machine
            .disk_accounting
            .value()
            .unwrap()
            .clone();
        accounting.skipped = vec![SkippedEntry {
            path: "build-slots/p/slot-3".into(),
            operation: SkippedEntryOperation::ReadMetadata,
            reason: SkippedEntryReason::PermissionDenied,
        }];
        executor.health.machine.disk_accounting = Measurement::measured(at, accounting);
        let rendered = render_executor(&executor);
        assert!(
            rendered
                .contains("Outside Cairn's accounting: not inferred — accounting is partial (1 entries skipped)"),
            "{rendered}"
        );
        assert!(!rendered.contains("inferred: volume used"), "{rendered}");
        assert!(
            rendered.contains(
                "- Accounting skipped:\n  - `build-slots/p/slot-3` (ReadMetadata, PermissionDenied)\n"
            ),
            "{rendered}"
        );
    }

    /// A retired executor home is a named component with its bytes, inside
    /// Cairn's accounting, in whatever state the ladder left it.
    #[test]
    fn each_retired_executor_home_is_named_with_its_bytes_and_state() {
        use cairn_common::executor_protocol::{RetiredHomeAccounting, RetiredHomeState};
        let mut executor = tight_local();
        let at = executor.captured_at_unix_ms;
        let mut accounting = executor
            .health
            .machine
            .disk_accounting
            .value()
            .unwrap()
            .clone();
        let home = |path: &str, bytes: u64, state| RetiredHomeAccounting {
            path: path.into(),
            bytes,
            retired_at_unix_ms: at,
            state,
        };
        accounting.categories.retired_homes_bytes = 225 * GIB;
        accounting.retired_homes = vec![
            home(
                "/home/bleu/.cairn-executor",
                224 * GIB,
                RetiredHomeState::Reclaiming,
            ),
            home(
                "/home/bleu/.cairn-b",
                3 * GIB,
                RetiredHomeState::Held {
                    pids: vec![1, 2, 3],
                    unknown: false,
                    detail: None,
                },
            ),
            home(
                "/home/bleu/.cairn",
                GIB,
                RetiredHomeState::RetainedRunnerState {
                    bytes_freed: 37 * GIB,
                    at_unix_ms: at,
                },
            ),
            home(
                "/Users/dev/.cairn-executor",
                0,
                RetiredHomeState::Reclaimed {
                    bytes_freed: 193 * GIB,
                    at_unix_ms: at,
                },
            ),
            home(
                "/srv/odd",
                GIB,
                RetiredHomeState::Refused {
                    reason: "it is a symbolic link".into(),
                },
            ),
        ];
        executor.health.machine.disk_accounting = Measurement::measured(at, accounting);
        let rendered = render_executor(&executor);
        for expected in [
            "retired executor homes 225.0 GiB",
            "- Retired executor home /home/bleu/.cairn-executor (retired ",
            "): 224.0 GiB, reclaiming",
            "3.0 GiB, held by 3 processes (pids 1, 2, 3); reclaimed once it is released",
            "reclaimed 37.0 GiB at ",
            "1.0 GiB retained: runner state (cairn.db), the operator's to remove",
            "/Users/dev/.cairn-executor (retired ",
            "reclaimed 193.0 GiB at ",
            "1.0 GiB, left alone: it is a symbolic link",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected:?} in {rendered}"
            );
        }
    }

    /// An unrelieved pass says what it asked for, what each rung gave, what it
    /// left alone, and when the next one will run.
    #[test]
    fn an_unrelieved_pressure_pass_renders_its_rungs_skips_and_rearm_line() {
        let mut executor = tight_local();
        let at = executor.captured_at_unix_ms;
        let rung = |rung, bytes, count| StorageRungReclaim { rung, bytes, count };
        executor.health.disk.pressure = Some(StoragePressureReport {
            at_unix_ms: at - 180_000,
            band: StoragePressureBand::Pressure,
            target_bytes: 74 * GIB,
            free_bytes_before: 54 * GIB,
            free_bytes_after: 54 * GIB + 31 * GIB,
            reclaimed_by_rung: vec![
                rung(StorageReclaimRung::QuarantineTargets, 31 * GIB, 2),
                rung(StorageReclaimRung::IdleCellScratch, 3 * GIB, 4),
                rung(StorageReclaimRung::IdleTargets, 0, 0),
            ],
            outcome: StoragePressureOutcome::Unrelieved,
            rearm_below_free_bytes: Some(46 * GIB),
            held_bytes: 5 * GIB,
            withheld: Vec::new(),
            occupied_skips: vec![
                OccupiedTreeSkip {
                    path: "build-slots/cairn/slot-1290.quarantine-1".into(),
                    pid: Some(4411),
                    unknown: false,
                    rule: None,
                    process: None,
                    executable: None,
                    held_path: None,
                    container: None,
                },
                OccupiedTreeSkip {
                    path: "build-slots/cairn/slot-1291.quarantine-1".into(),
                    pid: Some(1653),
                    unknown: true,
                    rule: None,
                    process: None,
                    executable: None,
                    held_path: None,
                    container: None,
                },
            ],
        });
        executor.health.disk.quarantine_targets_stripped = vec![QuarantineTargetStrip {
            path: "build-slots/cairn/slot-1268.quarantine-1".into(),
            bytes: 22 * GIB,
            at_unix_ms: at - 180_000,
            withheld_outside_lineage: Vec::new(),
            withheld_outside_lineage_truncated: 0,
        }];
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "Last pressure pass 3m ago: band Pressure, asked 74.0 GiB, reclaimed 34.0 GiB"
            ) && rendered.contains(
                "quarantine targets 31.0 GiB · idle cell scratch 3.0 GiB · idle targets 0 B"
            ) && rendered.contains(
                "— Unrelieved; 5.0 GiB held by policy; next pressure pass when free < 46.0 GiB"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "Skipped: `build-slots/cairn/slot-1290.quarantine-1` occupied by pid 4411"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("`build-slots/cairn/slot-1268.quarantine-1` 22.0 GiB 3m ago"),
            "{rendered}"
        );
    }

    /// The bgsrv-mac reading of cairn/4998: a critical pass that falls short
    /// names what it left and why, and `held by policy` counts the fenced cell
    /// the page lists under Pool slots, not only in-window quarantines.
    #[test]
    fn an_unrelieved_critical_pass_names_every_class_of_bytes_it_left_in_place() {
        let mut executor = tight_local();
        let at = executor.captured_at_unix_ms;
        let withheld = |reason, bytes, count| StorageWithheld {
            reason,
            bytes,
            count,
            unmeasured: false,
        };
        let withheld = vec![
            withheld(StorageWithheldReason::ForensicWindow, GIB / 2, 2),
            withheld(StorageWithheldReason::FencedCells, 31 * GIB, 1),
            withheld(StorageWithheldReason::CachesInUse, 3 * GIB, 1),
            withheld(StorageWithheldReason::CachesKept, 0, 0),
            withheld(StorageWithheldReason::CachesOwnerRetained, GIB, 1),
        ];
        executor.health.disk.pressure = Some(StoragePressureReport {
            at_unix_ms: at - 60_000,
            band: StoragePressureBand::Critical,
            target_bytes: 127 * GIB,
            free_bytes_before: GIB / 8,
            free_bytes_after: 70 * GIB,
            reclaimed_by_rung: Vec::new(),
            outcome: StoragePressureOutcome::Unrelieved,
            rearm_below_free_bytes: Some(62 * GIB),
            held_bytes: withheld.iter().map(|withheld| withheld.bytes).sum(),
            withheld,
            occupied_skips: Vec::new(),
        });
        let rendered = render_executor(&executor);
        for expected in [
            "Unrelieved; 35.5 GiB held by policy",
            "- Not reclaimed: 1.0 GiB in 1 managed cache root whose writers keep their own retention",
            "- Not reclaimed: 512.0 MiB in 2 quarantines inside the forensic window; released when the window expires",
            "- Not reclaimed: 31.0 GiB in 1 cell fenced behind processes from an ended epoch, which the executor will not signal",
            "- Not reclaimed: 3.0 GiB in 1 managed cache in use",
            "- Not reclaimed: 0 B in 0 managed caches the pass did not take",
        ] {
            assert!(rendered.contains(expected), "missing {expected:?} in {rendered}");
        }
    }

    /// Every skip names the rule that fired and what would release it; a strip
    /// names the withheld processes it did not consult; and the lineage those
    /// were measured against is on the page.
    #[test]
    fn occupancy_skips_name_their_rule_and_strips_name_what_they_did_not_consult() {
        use cairn_common::executor_protocol::{
            OccupancyLineageReport, OccupancyRule, WithheldProcess,
        };
        let mut executor = tight_local();
        let at = executor.captured_at_unix_ms;
        let skip = |rule, pid: Option<u32>, process: Option<&str>| OccupiedTreeSkip {
            path: "build-slots/cairn/slot-1.quarantine-1".into(),
            pid,
            unknown: !matches!(
                rule,
                OccupancyRule::Cwd | OccupancyRule::Executable | OccupancyRule::OpenFile
            ),
            rule: Some(rule),
            process: process.map(Into::into),
            executable: None,
            held_path: None,
            container: None,
        };
        let skips = vec![
            (
                OccupiedTreeSkip {
                    held_path: Some("target/debug/deps/libcairn.rlib".into()),
                    ..skip(OccupancyRule::OpenFile, Some(4411), Some("cargo"))
                },
                "occupied by pid 4411 (cargo): open file target/debug/deps/libcairn.rlib; released when it closes or exits",
            ),
            (
                skip(OccupancyRule::Cwd, Some(4412), Some("fish")),
                "occupied by pid 4412 (fish): working directory inside it; released when it leaves or exits",
            ),
            (
                OccupiedTreeSkip {
                    held_path: Some("target/debug/deps/cairn_core-1".into()),
                    ..skip(OccupancyRule::Executable, Some(4413), Some("cairn_core-1"))
                },
                "occupied by pid 4413 (cairn_core-1): running executable target/debug/deps/cairn_core-1; released when it exits",
            ),
            (
                OccupiedTreeSkip {
                    executable: Some("/usr/bin/sudo".into()),
                    container: Some("coalition 19563".into()),
                    ..skip(OccupancyRule::WithheldInLineage, Some(4414), Some("sudo"))
                },
                "occupancy unknown: pid 4414 (sudo, /usr/bin/sudo) withholds its descriptors and runs in the executor's own coalition 19563; released when it exits",
            ),
            (
                skip(OccupancyRule::WithheldContainerUnreadable, Some(4415), Some("login")),
                "occupancy unknown: pid 4415 (login) withholds its descriptors and its lineage container could not be read; released when it exits",
            ),
            (
                skip(OccupancyRule::LineageUnknown, Some(4416), Some("sd-pam")),
                "occupancy unknown: pid 4416 (sd-pam) withholds its descriptors and the executor's own lineage could not be read",
            ),
            (
                skip(OccupancyRule::ReadFailed, Some(4417), Some("rustc")),
                "occupancy unknown: the holdings of pid 4417 (rustc) could not be read; released when it exits",
            ),
            (
                skip(OccupancyRule::ProcessTableUnreadable, None, None),
                "occupancy unknown: the process table could not be read",
            ),
            (
                skip(OccupancyRule::PlatformUnsupported, None, None),
                "occupancy unknown: this platform cannot say which processes hold files",
            ),
        ];
        executor.health.disk.pressure = Some(StoragePressureReport {
            at_unix_ms: at - 60_000,
            band: StoragePressureBand::Pressure,
            target_bytes: 10 * GIB,
            free_bytes_before: 58 * GIB,
            free_bytes_after: 58 * GIB,
            reclaimed_by_rung: Vec::new(),
            outcome: StoragePressureOutcome::Unrelieved,
            rearm_below_free_bytes: None,
            held_bytes: 0,
            withheld: Vec::new(),
            occupied_skips: skips.iter().map(|(skip, _)| skip.clone()).collect(),
        });
        let login = |pid| WithheldProcess {
            pid,
            name: Some("login".into()),
            executable: Some("/usr/bin/login".into()),
            container: "coalition 803".into(),
        };
        executor.health.disk.quarantine_targets_stripped = vec![QuarantineTargetStrip {
            path: "build-slots/cairn/slot-1268.quarantine-1".into(),
            bytes: 22 * GIB,
            at_unix_ms: at - 60_000,
            withheld_outside_lineage: vec![login(20461), login(6751)],
            withheld_outside_lineage_truncated: 0,
        }];
        executor.health.disk.occupancy_lineage = Some(OccupancyLineageReport {
            current: Some("coalition 19563".into()),
            containers: vec!["coalition 4759".into(), "coalition 19563".into()],
            unknown: None,
        });

        let rendered = render_executor(&executor);
        for (_, line) in &skips {
            assert!(
                rendered.contains(&format!("`build-slots/cairn/slot-1.quarantine-1` {line}")),
                "missing {line:?}:\n{rendered}"
            );
        }
        assert!(
            rendered.contains(
                "`build-slots/cairn/slot-1268.quarantine-1` 22.0 GiB 1m ago; not consulted: pid 20461 login (/usr/bin/login, coalition 803), pid 6751 login (/usr/bin/login, coalition 803), withheld by the kernel and outside the executor's lineage"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "- Occupancy lineage: coalition 19563; earlier launches this boot: coalition 4759"
            ),
            "{rendered}"
        );

        executor.health.disk.occupancy_lineage = Some(OccupancyLineageReport {
            unknown: Some("coalition info: Operation not permitted".into()),
            ..OccupancyLineageReport::default()
        });
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "- Occupancy lineage: unknown (coalition info: Operation not permitted); any process whose holdings the kernel withholds makes a quarantine's occupancy unknown"
            ),
            "{rendered}"
        );
    }

    /// "Materialized" is inventory, not holds; and a slot's footprint is the
    /// walk's measurement when there is one, labelled, and an estimate when not.
    #[test]
    fn occupancy_separates_inventory_from_holds_and_slots_label_their_bytes() {
        let mut executor = tight_local();
        let at = executor.captured_at_unix_ms;
        executor.health.inventory.checked_out_count = 10;
        executor.health.inventory.held_count = 3;
        executor.health.inventory.transient_occupancy = 1;
        executor.health.inventory.idle_count = 3;
        let mut measured = executor.occupancy.cells.first().cloned();
        if measured.is_none() {
            let rendered = render_executor(&executor);
            assert!(
                rendered.contains("Cells: 10 materialized (3 held, 1 running, 3 idle reclaimable)"),
                "{rendered}"
            );
            return;
        }
        let cell = measured.take().unwrap();
        executor.slots = vec![SlotState {
            cell_id: cell.cell_id.clone(),
            project_id: cell.project_id.clone(),
            head_commit: None,
            last_distance: None,
            last_used_unix_ms: 0,
            size_bytes: 0,
            lifecycle: PersistentCellLifecycle::Idle,
            measured_bytes: Some(6 * GIB),
            measured_at_unix_ms: Some(at - 60_000),
        }];
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("Cells: 10 materialized (3 held, 1 running, 3 idle reclaimable)"),
            "{rendered}"
        );
        assert!(rendered.contains("6.0 GiB measured 1m ago"), "{rendered}");
    }
}

#[cfg(test)]
mod storage_verdict_tests {
    use super::render_executor;
    use super::tests::inspection;
    use cairn_common::executor_protocol::{
        DiskHealthStatus, SkippedEntry, SkippedEntryOperation, SkippedEntryReason,
        StorageAlarmVerification, StorageReclamationAlarm, StorageReclamationHealth,
        StorageSweepStatus,
    };

    fn alarm(verification: StorageAlarmVerification) -> StorageReclamationAlarm {
        StorageReclamationAlarm {
            path: "/cairn/build-slots/acme/slot-3.quarantine-1750000000000".into(),
            first_permanent_unix_ms: 100_000,
            attempts: 7,
            bytes: 18 * 1024 * 1024 * 1024,
            last_error: "Permission denied (os error 13)".into(),
            survivors: "target/debug/build, .cargo-lock".into(),
            verification,
        }
    }

    /// The CAIRN-4217 regression, stated as a test: a volume with room to spare
    /// and a janitor that has permanently given up is not `Ok`. Two such trees
    /// sat unreclaimable for sixteen days behind a verdict derived only from
    /// free space.
    #[test]
    fn an_outstanding_alarm_degrades_an_otherwise_healthy_volume() {
        let mut executor = inspection("local", true);
        executor.health.disk.status = DiskHealthStatus::Ok;
        executor.health.disk.reclamation = Some(StorageReclamationHealth {
            outstanding: 2,
            alarms: vec![alarm(StorageAlarmVerification::Observed)],
        });

        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("custody Degraded"),
            "an unreclaimable tree is a degraded executor: {rendered}"
        );
        assert!(
            rendered.contains("volume Ok"),
            "the physical verdict stays visible and separate: {rendered}"
        );
        assert!(rendered.contains("2 outstanding"), "{rendered}");
        assert!(
            rendered.contains("slot-3.quarantine-1750000000000"),
            "the operator needs the path to act on: {rendered}"
        );
        assert!(rendered.contains("7 attempts"), "{rendered}");
        assert!(
            rendered.contains("Permission denied (os error 13)"),
            "the last error is what says why it is stuck: {rendered}"
        );
        assert!(rendered.contains("target/debug/build"), "{rendered}");
        assert!(
            rendered.contains("Cairn has not modified these paths")
                && rendered.contains("requires operator confirmation"),
            "reclaiming is destructive and operator-owned, and must say so: {rendered}"
        );
    }

    /// A restart must not launder an alarm into a clean slate: health is
    /// degraded from the instant the process comes up, before any sweep has had
    /// a chance to retry the path.
    #[test]
    fn a_restored_alarm_degrades_before_any_sweep_has_verified_it() {
        let mut executor = inspection("local", true);
        executor.health.disk.status = DiskHealthStatus::Ok;
        executor.health.disk.reclamation = Some(StorageReclamationHealth {
            outstanding: 1,
            alarms: vec![alarm(StorageAlarmVerification::RestoredUnverified)],
        });

        let rendered = render_executor(&executor);
        assert!(rendered.contains("custody Degraded"), "{rendered}");
        assert!(
            rendered.contains("RestoredUnverified"),
            "the reader is told this has not been retried yet: {rendered}"
        );
    }

    /// Silence is not evidence. A peer that does not report reclamation at all
    /// must not be rendered as one that reported nothing wrong.
    #[test]
    fn an_executor_that_does_not_report_reclamation_is_not_rendered_as_clean() {
        let mut executor = inspection("local", true);
        executor.health.disk.status = DiskHealthStatus::Ok;
        executor.health.disk.reclamation = None;

        let rendered = render_executor(&executor);
        assert!(rendered.contains("custody Unreported"), "{rendered}");
        assert!(
            rendered.contains("absence is not a clean bill of health"),
            "{rendered}"
        );
    }

    /// A completed sweep that found nothing is a positive claim, and reads as
    /// one.
    #[test]
    fn a_sweep_that_found_nothing_reads_as_ok_rather_than_unreported() {
        let mut executor = inspection("local", true);
        executor.health.disk.status = DiskHealthStatus::Ok;
        executor.health.disk.reclamation = Some(StorageReclamationHealth::default());

        let rendered = render_executor(&executor);
        assert!(rendered.contains("custody Ok"), "{rendered}");
        assert!(!rendered.contains("Reclamation alarms"), "{rendered}");
        assert!(!rendered.contains("partial"), "{rendered}");
        assert!(!rendered.contains("Sweep skipped"), "{rendered}");
    }

    /// A sweep that could not reach something says so and says what, so a tree
    /// the janitor cannot see is never hidden behind a bare `Completed`.
    #[test]
    fn a_partial_sweep_names_what_it_skipped() {
        let mut executor = inspection("local", true);
        executor.health.disk.status = DiskHealthStatus::Ok;
        executor.health.disk.sweep_status = StorageSweepStatus::Completed;
        executor.health.disk.reclamation = Some(StorageReclamationHealth::default());
        executor.health.disk.cleanup_skipped_entries = Some(3);
        executor.health.disk.cleanup_skipped = vec![
            SkippedEntry {
                path: "build-slots/p/slot-7.quarantine-1".into(),
                operation: SkippedEntryOperation::Remove,
                reason: SkippedEntryReason::PermissionDenied,
            },
            SkippedEntry {
                path: "build-slots/p/slot-8.quarantine-2".into(),
                operation: SkippedEntryOperation::Measure,
                reason: SkippedEntryReason::Other,
            },
        ];
        executor.health.disk.cleanup_skipped_truncated = 1;

        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "sweep Completed, partial: 3 skipped (`build-slots/p/slot-7.quarantine-1`, `build-slots/p/slot-8.quarantine-2`, …)\n"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("`build-slots/p/slot-7.quarantine-1` (Remove, PermissionDenied)"),
            "{rendered}"
        );
        assert!(
            rendered.contains("`build-slots/p/slot-8.quarantine-2` (Measure, Other)"),
            "{rendered}"
        );
        assert!(rendered.contains("and 1 more not listed"), "{rendered}");
    }

    /// A sweep whose error latched admission says so on the verdict line and
    /// names the error and its path, so a machine refusing every admission
    /// never reads as a sweep that merely completed.
    #[test]
    fn a_latched_sweep_names_what_is_blocking_admission() {
        let mut executor = inspection("local", true);
        executor.health.disk.status = DiskHealthStatus::Ok;
        executor.health.disk.sweep_status = StorageSweepStatus::Completed;
        executor.health.disk.cleanup_blocked = true;
        executor.health.disk.cleanup_last_error = Some(
            "write executor state failed for /h/build-slots/.authority/slot-1/state.json: No space left on device"
                .into(),
        );
        executor.health.disk.cleanup_failing_path =
            Some("/h/build-slots/.authority/slot-1/state.json".into());

        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("sweep Completed with error — admission blocked"),
            "{rendered}"
        );
        assert!(!rendered.contains("sweep Completed\n"), "{rendered}");
        assert!(
            rendered.contains(
                "- Cleanup blocking admission: write executor state failed for /h/build-slots/.authority/slot-1/state.json: No space left on device (failing path `/h/build-slots/.authority/slot-1/state.json`)"
            ),
            "{rendered}"
        );
    }

    /// One skip renders its path inline, with no ellipsis.
    #[test]
    fn a_single_skip_is_named_inline_on_the_verdict() {
        let mut executor = inspection("local", true);
        executor.health.disk.sweep_status = StorageSweepStatus::Completed;
        executor.health.disk.cleanup_skipped_entries = Some(1);
        executor.health.disk.cleanup_skipped = vec![SkippedEntry {
            path: "build-slots/cairn/.authority/slot-42/scratch.discard-1-a".into(),
            operation: SkippedEntryOperation::Remove,
            reason: SkippedEntryReason::PermissionDenied,
        }];

        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "partial: 1 skipped (`build-slots/cairn/.authority/slot-42/scratch.discard-1-a`)\n"
            ),
            "{rendered}"
        );
        assert!(!rendered.contains("admission blocked"), "{rendered}");
    }
}

/// The first few skipped paths, for the verdict line, so the tree a partial
/// sweep could not reach is named where the verdict is read.
fn inline_skip_paths(skipped: &[SkippedEntry], count: usize) -> String {
    const INLINE_SKIP_PATHS: usize = 3;
    let mut paths = skipped
        .iter()
        .take(INLINE_SKIP_PATHS)
        .map(|entry| format!("`{}`", entry.path))
        .collect::<Vec<_>>();
    if count > paths.len() {
        paths.push("…".to_string());
    }
    paths.join(", ")
}

fn list_or(values: &[String], empty: &str) -> String {
    if values.is_empty() {
        empty.to_string()
    } else {
        values.join(", ")
    }
}

fn plural(count: usize, one: &str, many: &str) -> String {
    if count == 1 {
        format!("{count} {one}")
    } else {
        format!("{count} {many}")
    }
}

#[cfg(test)]
mod tests {
    use cairn_common::executor_protocol::MeasurementGap;
    use cairn_common::executor_protocol::{ExecutorRetainedState, ProcessTelemetry, RetainedOwner};

    use super::*;
    use cairn_common::executor_protocol::{
        BuildPrerequisiteCheck, CpuPressure, EvictedCellReport, ExecutorAdvertisement,
        ExecutorCapabilities, ExecutorHealthSnapshot, ExecutorIdentity, ExecutorSubstrateReport,
        FleetSnapshot, MachineMemory, MachineVolume, RemoteAttachAttempt,
        ResidentOccupancyEvidence, ResidentProcessHealth, ResourceReservation, ToolchainDetection,
        ToolchainProbe,
    };

    const CAPTURED_AT: u64 = 1_000_000;

    fn cached_slot() -> cairn_common::executor_protocol::PersistentCellState {
        serde_json::from_value(serde_json::json!({
            "executorId": "executor-private-uuid", "projectId": "project-private-uuid",
            "slotId": "slot-1", "path": "/private/machine/build-slots/CAIRN/slot-1",
            "repository": "/private/repository", "authorityPath": "/private/authority",
            "lifecycle": "idle", "cellEpoch": 2, "lastSealedCommit": "abcdef1234567890",
            "lastUsedUnixMs": CAPTURED_AT - 120_000, "sizeBytes": 2048,
            "occupancy": {}
        }))
        .unwrap()
    }

    #[test]
    fn cache_distance_rendering_distinguishes_unmeasured_from_measured_zero_and_shortens_target() {
        use cairn_common::cache_distance::{CacheDistance, MeasuredDistance};
        assert_eq!(render_cache_distance(None, CAPTURED_AT), "unmeasured");
        let mut measurement = MeasuredDistance {
            target_commit: "123456789abcdef".into(),
            distance: CacheDistance::default(),
            measured_at_unix_ms: CAPTURED_AT - 60_000,
        };
        assert_eq!(
            render_cache_distance(Some(&measurement), CAPTURED_AT),
            "0 files / 0 crates (0 transitive) / 0 JS workspaces to 12345678, 1m ago"
        );
        measurement.distance = CacheDistance {
            files: 9,
            crates: 2,
            crates_transitive: 5,
            js_workspaces: 3,
        };
        measurement.measured_at_unix_ms = CAPTURED_AT + 1;
        assert_eq!(
            render_cache_distance(Some(&measurement), CAPTURED_AT),
            "9 files / 2 crates (5 transitive) / 3 JS workspaces to 12345678, 0ms ago"
        );
    }

    #[test]
    fn pool_slot_rendering_shows_project_head_lifecycle_size_age_and_distance_without_private_identity(
    ) {
        let mut executor = inspection("local", true);
        executor.occupancy.cells = vec![cached_slot()];
        let rendered = render_executor(&executor);
        assert!(rendered.contains("slot-1: project CAIRN, head abcdef12, idle, ~2.0 KiB estimate, last used 2m ago; last distance: unmeasured"), "{rendered}");
        for private in [
            "project-private-uuid",
            "executor-private-uuid",
            "device-9f3c1a",
            "executor-7b21ce",
            "/private/",
            "abcdef1234567890",
        ] {
            assert!(!rendered.contains(private), "leaked {private}: {rendered}");
        }
        executor.occupancy.cells[0].last_sealed_commit = None;
        executor.occupancy.cells[0].last_distance =
            Some(cairn_common::cache_distance::MeasuredDistance {
                target_commit: "fedcba9876543210".into(),
                distance: Default::default(),
                measured_at_unix_ms: CAPTURED_AT - 2000,
            });
        let rendered = render_executor(&executor);
        assert!(rendered.contains("head unmeasured"), "{rendered}");
        assert!(rendered.contains("last distance: 0 files / 0 crates (0 transitive) / 0 JS workspaces to fedcba98, 2s ago"), "{rendered}");
    }

    #[test]
    fn slot_project_name_handles_windows_paths_and_never_falls_back_to_private_project_id() {
        let mut cell = cached_slot();
        assert_eq!(slot_project_name(&cell), "CAIRN");
        cell.occupancy.command = Some(
            serde_json::from_value(serde_json::json!({
                "requestId": "r", "attemptId": "a", "command": "true",
                "priority": "reviewCheck", "queuedAtUnixMs": 1,
                "owner": { "projectId": "private-project-uuid", "projectKey": "OWNER" }
            }))
            .unwrap(),
        );
        assert_eq!(
            slot_project_name(&cell),
            "OWNER",
            "owner metadata precedes the path"
        );
        cell.occupancy
            .command
            .as_mut()
            .unwrap()
            .owner
            .as_mut()
            .unwrap()
            .project_key = None;
        assert_eq!(
            slot_project_name(&cell),
            "CAIRN",
            "absent owner key falls back to the public path key"
        );
        cell.path = r"C:\private\build-slots\PROJECT\slot-1".into();
        assert_eq!(slot_project_name(&cell), "PROJECT");
        for path in [
            "/private/opaque-slot",
            "/private/build-slots/.authority/slot-1",
        ] {
            cell.path = path.into();
            assert_eq!(slot_project_name(&cell), "name unavailable");
        }
    }

    #[test]
    fn a_failed_resident_update_names_the_transition_age_and_exact_cause() {
        let mut executor = inspection("bglab-ub", false);
        executor.last_update = Some(UpdateReport {
            state: UpdateState::Failed,
            from_build: "build-old".into(),
            to_distribution_id: "executor-protocol-v92-linux-x86-64".into(),
            to_build: "build-new".into(),
            cause: Some("digest mismatch: expected abc, observed def".into()),
            at_unix_ms: CAPTURED_AT - 120_000,
        });
        let rendered = render_executor(&executor);
        assert!(rendered.contains("Last update: failed, build-old → executor-protocol-v92-linux-x86-64 (build build-new), 2m ago"), "{rendered}");
        assert!(
            rendered.contains("Update failure: digest mismatch: expected abc, observed def"),
            "{rendered}"
        );
    }

    /// The operator surface is the whole point of measuring: a machine's page
    /// has to explain why it qualifies or why it does not, in words that send
    /// someone somewhere. So the probe's own reason is carried verbatim rather
    /// than summarized — a summary is exactly what makes an unmeasured edge
    /// look like a settled one.
    #[test]
    fn an_unmeasured_edge_explains_itself_on_the_machine_page() {
        let mut executor = inspection("bglab-mac", false);
        executor.health.advertisement.capabilities.containment =
            Some(cairn_sandbox::containment::observations());
        let rendered = render_executor(&executor);

        assert!(rendered.contains("## Identity"));
        assert!(
            rendered.contains("enrolled (this machine's own authenticated claim about itself)"),
            "an enrolled machine's row is a claim, and the page has to say so:\n{rendered}"
        );
        assert!(
            rendered.contains("BatchSpawnFilesystemConfined"),
            "{rendered}"
        );
        assert!(
            rendered.contains("PtyResidentFilesystemConfined: Unmeasured"),
            "an edge nobody probes must say so by name:\n{rendered}"
        );
        assert!(
            rendered.contains("EnvironmentInheritance"),
            "a confirmed crossing with no probe still gets a row:\n{rendered}"
        );
        assert!(
            rendered.contains("no probe attempts"),
            "the probe's own reason travels verbatim:\n{rendered}"
        );
        assert!(
            rendered.contains("no current fingerprint"),
            "an edge whose widening inputs cannot be read must say that too:\n{rendered}"
        );
    }

    /// A machine that reports nothing must not render as a machine that
    /// measured something and found it safe.
    #[test]
    fn an_executor_without_containment_reporting_says_so() {
        let executor = inspection("older", false);
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("Containment: not reported by this executor"),
            "{rendered}"
        );
        assert!(rendered.contains("every envelope is refused"), "{rendered}");
    }

    pub(super) fn inspection(name: &str, colocated: bool) -> ExecutorInspection {
        let identity = ExecutorIdentity {
            device_id: "device-9f3c1a".into(),
            executor_id: "executor-7b21ce".into(),
            display_name: name.into(),
        };
        ExecutorInspection {
            name: name.into(),
            link_path: None,
            recent_placements: Vec::new(),
            colocated,
            health: ExecutorHealthSnapshot {
                identity: identity.clone(),
                public_name: name.into(),
                colocated,
                status: ExecutorHealthStatus::Online,
                heartbeat_age_ms: 4_000,
                liveness_age_ms: Some(5_000),
                telemetry_stale: false,
                advertisement: ExecutorAdvertisement {
                    identity,
                    capabilities: ExecutorCapabilities {
                        os: "linux".into(),
                        arch: "x86_64".into(),
                        logical_cores: 16,
                        concurrency_capacity: None,
                        toolchains: vec!["rust".into(), "bun".into()],
                        projects_served: Vec::new(),
                        disk_budget_bytes: None,
                        memory_budget_bytes: None,
                        sandbox: None,
                        toolchain_detection: None,
                        build_prerequisites: None,
                        containment: None,
                    },
                    current_load: 0,
                    warm_roots: Vec::new(),
                    observed_at_unix_ms: CAPTURED_AT - 4_000,
                    liveness_observed_at_unix_ms: Some(CAPTURED_AT - 5_000),
                    facts_stale_refresh_blocked: None,
                    last_refresh: None,
                },
                admission: ExecutorSubstrateReport::default().admission,
                queues: Vec::new(),
                host: ExecutorSubstrateReport::default().host,
                disk: ExecutorSubstrateReport::default().disk,
                machine: ExecutorSubstrateReport::default().machine,
                inventory: ExecutorSubstrateReport::default().inventory,
                connection_generation: 3,
                applied_policy: ExecutorSubstrateReport::default().applied_policy,
                drain_mode: false,
                role: Default::default(),
                resident_processes: Default::default(),
                command_processes: Default::default(),
                evicted_cells: Vec::new(),
                busy_declined_cells: Vec::new(),
                build_skew: None,
                stopped_commands: Vec::new(),
            },
            executor_build_id: Some("build-abc".into()),
            last_update: None,
            occupancy: FleetSnapshot::default(),
            slots: Vec::new(),
            captured_at_unix_ms: CAPTURED_AT,
            connection_timeline: Vec::new(),
            project_eligibility: Vec::new(),
        }
    }

    #[test]
    fn resident_ownership_health_appears_once_on_the_executor() {
        let mut executor = inspection("local", true);
        executor.health.resident_processes = ResidentProcessHealth {
            tracked_live_count: 2,
            reaped_stale_count: 3,
            unverified_stale_count: 1,
            oldest_stale_age_ms: Some(90_000),
        };

        let rendered = render_executor(&executor);
        let summary =
            "Resident ownership: 2 tracked, 3 stale reaped, 1 unverified, oldest stale 1m ago";
        assert_eq!(rendered.matches(summary).count(), 1, "{rendered}");
    }

    /// Stages the state bglab-win was actually in: a machine whose probe ran
    /// and truthfully found nothing, because the toolchain belongs to a
    /// different OS account.
    fn probed_and_absent() -> ToolchainDetection {
        ToolchainDetection {
            account: "mitch".into(),
            home: "C:\\Users\\mitch".into(),
            probes: vec![ToolchainProbe {
                toolchain: "rust".into(),
                command: "cargo --version".into(),
                detected: false,
                resolved_path: None,
                detail: "could not run: program not found".into(),
            }],
        }
    }

    fn probed_and_present() -> ToolchainDetection {
        ToolchainDetection {
            account: "mitch".into(),
            home: "C:\\Users\\mitch".into(),
            probes: vec![ToolchainProbe {
                toolchain: "rust".into(),
                command: "cargo --version".into(),
                detected: true,
                resolved_path: Some("C:\\Users\\mitch\\.cargo\\bin\\cargo.exe".into()),
                detail: "cargo 1.97.1 (c980f4866 2026-06-30)".into(),
            }],
        }
    }

    fn with_toolchains(
        name: &str,
        toolchains: Vec<String>,
        detection: Option<ToolchainDetection>,
    ) -> ExecutorInspection {
        let mut executor = inspection(name, false);
        let capabilities = &mut executor.health.advertisement.capabilities;
        capabilities.toolchains = toolchains;
        capabilities.toolchain_detection = detection;
        executor
    }

    /// The listing is where an empty toolchain set is first seen, so it is where
    /// the emptiness has to be explainable. A bare "none advertised" reads the
    /// same for a machine without Rust and for a broken probe; naming the
    /// account and the failure is what separates them.
    #[test]
    fn a_machine_advertising_nothing_names_the_account_and_the_failure() {
        let rendered = render_executors(
            &[with_toolchains(
                "bglab-win",
                Vec::new(),
                Some(probed_and_absent()),
            )],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(
            rendered.contains(
                "- Toolchains: none advertised (as account mitch — rust: `cargo --version` could not run: program not found)"
            ),
            "{rendered}"
        );
    }

    /// An executor too old to report probes must say that, not present its
    /// silence as a finding. "Cannot explain itself" and "probed and found
    /// nothing" are opposite facts about a machine.
    #[test]
    fn an_executor_that_reports_no_probes_is_not_read_as_having_probed() {
        let listed = render_executors(
            &[with_toolchains("bglab-win", Vec::new(), None)],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(
            listed.contains("- Toolchains: none advertised\n"),
            "{listed}"
        );
        assert!(!listed.contains("as account"), "{listed}");

        let detailed = render_executor(&with_toolchains("bglab-win", Vec::new(), None));
        assert!(
            detailed.contains("- Toolchain probes: not reported by this executor"),
            "{detailed}"
        );
    }

    /// The machine's own page carries the whole record, including for a probe
    /// that succeeded: which command proved it, where the program resolved, and
    /// the account that could see it.
    #[test]
    fn the_machine_page_records_the_probe_behind_a_detected_toolchain() {
        let rendered = render_executor(&with_toolchains(
            "bglab-win",
            vec!["rust".into()],
            Some(probed_and_present()),
        ));
        assert!(rendered.contains("- Toolchains: rust\n"), "{rendered}");
        assert!(
            rendered.contains("- Toolchain probes: run as account mitch (home C:\\Users\\mitch)"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "  - rust: detected — `cargo --version` at C:\\Users\\mitch\\.cargo\\bin\\cargo.exe — cargo 1.97.1 (c980f4866 2026-06-30)"
            ),
            "{rendered}"
        );
    }

    /// A probe that resolved a program and still failed is the interesting case:
    /// the path is reported alongside the failure rather than suppressed,
    /// because "found it and it does not work" is a different problem from
    /// "could not find it".
    #[test]
    fn a_program_that_resolves_and_then_fails_reports_both_facts() {
        let detection = ToolchainDetection {
            account: "mitch".into(),
            home: "C:\\Users\\mitch".into(),
            probes: vec![ToolchainProbe {
                toolchain: "rust".into(),
                command: "cargo --version".into(),
                detected: false,
                resolved_path: Some("C:\\Users\\other\\.cargo\\bin\\cargo.exe".into()),
                detail: "exited 1: error: rustup could not choose a version of cargo to run".into(),
            }],
        };
        let rendered = render_executor(&with_toolchains("bglab-win", Vec::new(), Some(detection)));
        assert!(
            rendered.contains("C:\\Users\\other\\.cargo\\bin\\cargo.exe"),
            "{rendered}"
        );
        assert!(
            rendered.contains("rustup could not choose a version of cargo to run"),
            "{rendered}"
        );
    }

    /// A machine that can build still reads as one line. The evidence belongs on
    /// the machine's page, not in the scan an agent does before placing work.
    #[test]
    fn an_advertised_toolchain_still_lists_as_a_plain_set() {
        let rendered = render_executors(
            &[with_toolchains(
                "bglab-win",
                vec!["rust".into(), "bun".into()],
                Some(probed_and_present()),
            )],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(rendered.contains("- Toolchains: rust, bun\n"), "{rendered}");
    }

    fn with_prerequisites(name: &str, check: Option<BuildPrerequisiteCheck>) -> ExecutorInspection {
        let mut executor = inspection(name, false);
        executor
            .health
            .advertisement
            .capabilities
            .build_prerequisites = check;
        executor
    }

    /// The measurement this whole facility exists for, staged as it was found on
    /// bglab-sp1: a machine that truthfully advertises `rust`, is eligible for
    /// the Rust lanes, and dies in a `glib-sys` build script before compiling a
    /// single first-party crate.
    fn missing_webkit() -> BuildPrerequisiteCheck {
        BuildPrerequisiteCheck {
            script: "scripts/setup-self-hosted-runner-linux.sh --check".into(),
            status: BuildPrerequisiteStatus::Missing,
            detail: "Missing Linux self-hosted runner prerequisites on bglab-sp1: libwebkit2gtk-4.1-dev libayatana-appindicator3-dev\nProvision bglab-sp1 once with: sudo scripts/setup-self-hosted-runner-linux.sh".into(),
        }
    }

    /// A machine that cannot build has to be legible as that in the listing an
    /// operator scans, naming the package and the command that repairs it — the
    /// two things `CAIRN_INFRASTRUCTURE_EXIT` leaves them without.
    #[test]
    fn a_machine_that_cannot_build_says_so_beside_the_toolchain_it_advertises() {
        let rendered = render_executors(
            &[with_prerequisites("bglab-sp1", Some(missing_webkit()))],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(rendered.contains("- Toolchains: rust, bun\n"), "{rendered}");
        assert!(
            rendered.contains("- Build prerequisites: MISSING — Missing Linux self-hosted runner prerequisites on bglab-sp1: libwebkit2gtk-4.1-dev libayatana-appindicator3-dev — Provision bglab-sp1 once with: sudo scripts/setup-self-hosted-runner-linux.sh\n"),
            "{rendered}"
        );
    }

    /// A machine that cannot prepare a project's cells reads as that, not as a
    /// healthy online peer: in the listing as one line per project, and on its
    /// page with placement's own reason for each.
    #[test]
    fn a_machine_placement_will_not_use_for_a_project_says_so() {
        let mut executor = with_prerequisites("bgsrv-ub2", Some(missing_webkit()));
        executor.project_eligibility = vec![
            cairn_common::executor_protocol::ProjectEligibility {
                project_id: "cairn".into(),
                refusals: vec![
                    PlacementRejectionReason::PreparationToolchainMissing {
                        project_id: "cairn".into(),
                        missing: vec!["bun".into()],
                        advertised: Vec::new(),
                    },
                    PlacementRejectionReason::BuildPrerequisitesMissing {
                        project_id: "cairn".into(),
                        script: "scripts/setup-self-hosted-runner-linux.sh".into(),
                        detail: missing_webkit().detail,
                    },
                ],
            },
            cairn_common::executor_protocol::ProjectEligibility {
                project_id: "other".into(),
                refusals: Vec::new(),
            },
        ];
        let listed = render_executors(
            std::slice::from_ref(&executor),
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(
            listed.contains(
                "- Not eligible for cairn: setup needs bun; rust work (build prerequisites MISSING)\n"
            ),
            "{listed}"
        );
        assert!(!listed.contains("Not eligible for other"), "{listed}");

        let page = render_executor(&executor);
        assert!(page.contains("- Project eligibility:\n"), "{page}");
        assert!(
            page.contains(
                "  - cairn: not eligible — project setup for cairn requires bun; this machine advertises none\n"
            ),
            "{page}"
        );
        assert!(page.contains("Provision bglab-sp1 once with"), "{page}");
        assert!(page.contains("  - other: eligible\n"), "{page}");
    }

    /// The machine's own page keeps the script's diagnostic intact, line by
    /// line, and states when the verdict was taken — nothing refreshes it while
    /// a machine stays attached, so a page that implied otherwise would lie.
    #[test]
    fn the_machine_page_reproduces_the_scripts_own_diagnostic() {
        let rendered = render_executor(&with_prerequisites("bglab-sp1", Some(missing_webkit())));
        assert!(
            rendered.contains("- Build prerequisites: MISSING — `scripts/setup-self-hosted-runner-linux.sh --check`, as of this machine's last attach\n"),
            "{rendered}"
        );
        assert!(
            rendered.contains("  - Missing Linux self-hosted runner prerequisites on bglab-sp1: libwebkit2gtk-4.1-dev libayatana-appindicator3-dev\n"),
            "{rendered}"
        );
        assert!(
            rendered.contains("  - Provision bglab-sp1 once with: sudo scripts/setup-self-hosted-runner-linux.sh\n"),
            "{rendered}"
        );
    }

    /// An equipped machine costs the listing nothing. The evidence stays on the
    /// machine's page, the same division the toolchain probes already keep.
    #[test]
    fn an_equipped_machine_adds_no_line_to_the_listing() {
        let satisfied = BuildPrerequisiteCheck {
            script: "scripts/setup-self-hosted-runner-linux.sh --check".into(),
            status: BuildPrerequisiteStatus::Satisfied,
            detail: "Linux self-hosted runner prerequisites are installed on fedora.".into(),
        };
        let listed = render_executors(
            &[with_prerequisites("fedora", Some(satisfied.clone()))],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(!listed.contains("Build prerequisites"), "{listed}");

        let detailed = render_executor(&with_prerequisites("fedora", Some(satisfied)));
        assert!(
            detailed.contains("- Build prerequisites: satisfied — `scripts/setup-self-hosted-runner-linux.sh --check`, as of this machine's last attach\n"),
            "{detailed}"
        );
        assert!(
            detailed
                .contains("  - Linux self-hosted runner prerequisites are installed on fedora.\n"),
            "{detailed}"
        );
    }

    /// The distinction the defect is made of. A machine nobody could ask must
    /// never read like a machine that answered — in either direction: not as
    /// equipped, which is what silence would imply, and not as missing packages
    /// it was never asked about, which would send an operator to install things
    /// that are already there.
    #[test]
    fn a_machine_that_was_never_asked_reads_as_neither_equipped_nor_missing() {
        let listed = render_executors(
            &[with_prerequisites("bglab-mac", None)],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(
            listed.contains(
                "- Build prerequisites: not reported (this machine's executor predates the check)\n"
            ),
            "{listed}"
        );
        assert!(!listed.contains("MISSING"), "{listed}");

        let unchecked = BuildPrerequisiteCheck {
            script: "scripts/setup-self-hosted-runner-windows.ps1 -Check".into(),
            status: BuildPrerequisiteStatus::Unchecked,
            detail: "could not run pwsh: program not found".into(),
        };
        let listed = render_executors(
            &[with_prerequisites("bglab-win", Some(unchecked.clone()))],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(
            listed.contains(
                "- Build prerequisites: not checked — could not run pwsh: program not found\n"
            ),
            "{listed}"
        );
        assert!(!listed.contains("MISSING"), "{listed}");

        let detailed = render_executor(&with_prerequisites("bglab-mac", None));
        assert!(
            detailed.contains("- Build prerequisites: not reported by this executor\n"),
            "{detailed}"
        );

        let detailed = render_executor(&with_prerequisites("bglab-win", Some(unchecked)));
        assert!(
            detailed.contains("- Build prerequisites: could not be checked — `scripts/setup-self-hosted-runner-windows.ps1 -Check`, as of this machine's last attach\n"),
            "{detailed}"
        );
    }

    fn measured(executor: &mut ExecutorInspection) {
        executor.health.machine.cpu = Measurement::measured(
            CAPTURED_AT - 2_000,
            CpuPressure {
                utilization: 0.42,
                user: 0.30,
                system: 0.12,
                logical_cores: 16,
            },
        );
        executor.health.machine.memory = Measurement::measured(
            CAPTURED_AT - 2_000,
            MachineMemory {
                total_bytes: 64 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
            },
        );
        executor.health.machine.volume = Measurement::measured(
            CAPTURED_AT - 2_000,
            MachineVolume {
                total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
                free_bytes: 512 * 1024 * 1024 * 1024,
            },
        );
    }

    /// The incident this section exists for, rendered.
    ///
    /// The shape is the one a live `vmmap` found on a daemon at 59.6 GiB: an
    /// enormous mapped address space, a much smaller footprint, and owners
    /// holding almost nothing. A reader has to be able to see all three at
    /// once, because it is their *combination* that says the growth is
    /// allocator regions rather than retained data — and that distinction
    /// decides whether the next person looks for a leak or for an allocation
    /// rate.
    #[test]
    fn executor_memory_shows_size_beside_the_owners_that_account_for_it() {
        let mut executor = inspection("local", true);
        executor.health.machine.process.resident_bytes =
            Measurement::measured(CAPTURED_AT - 5_000, 59_632_304 * 1024);
        executor.health.machine.process.physical_footprint_bytes =
            Measurement::measured(CAPTURED_AT - 5_000, 9_448_928_051);
        executor.health.machine.process.virtual_bytes =
            Measurement::measured(CAPTURED_AT - 5_000, 124_998_048_154);
        executor.health.machine.process.retained = ExecutorRetainedState {
            measured_at_unix_ms: CAPTURED_AT - 5_000,
            cells: RetainedOwner {
                entries: 3,
                estimated_bytes: 2_048,
                peak_entries: 6,
                peak_estimated_bytes: 4_096,
            },
            queued_output_chunks: RetainedOwner {
                entries: 0,
                estimated_bytes: 0,
                peak_entries: 64,
                peak_estimated_bytes: 524_288,
            },
            ..ExecutorRetainedState::default()
        };

        let rendered = render_executor(&executor);
        assert!(rendered.contains("## Executor memory"), "{rendered}");
        assert!(rendered.contains("56.9 GiB"), "resident size: {rendered}");
        assert!(rendered.contains("8.8 GiB"), "footprint: {rendered}");
        assert!(
            rendered.contains("Mapped address space: 116.4 GiB"),
            "the reading that distinguishes fragmentation from retention: {rendered}"
        );
        assert!(rendered.contains("cells"), "{rendered}");
        // An owner that is empty now but peaked is exactly the transient this
        // table exists to surface after the fact.
        assert!(
            rendered.contains("queued output chunks"),
            "a drained owner with a peak must still render: {rendered}"
        );
        assert!(rendered.contains("512.0 KiB"), "its peak: {rendered}");
        // Owners that were never touched are noise and stay out.
        assert!(
            !rendered.contains("residency reclaim contention"),
            "an untouched owner must not pad the table: {rendered}"
        );
    }

    /// The other specimen, and the reading that tells the two apart.
    ///
    /// Read down to the owner table, this machine and the one above are the
    /// same incident: an enormous resident set beside owners accounting for a
    /// rounding error of it. They are not the same incident. The daemon above
    /// mapped thirteen times its footprint, which is an allocator abandoning
    /// regions. This one maps 1.13 times its resident set and holds that memory
    /// as ordinary anonymous pages — a shape mapped size cannot distinguish
    /// from a genuine leak, and one the owner table structurally cannot explain
    /// however many owners it grows, because the program has already freed the
    /// bytes and freed bytes have no owner.
    ///
    /// So the assertion that matters is the pair: 33.4 MiB attributed against
    /// 51.8 GiB the allocator says it is holding on nobody's behalf. Without
    /// the second number the first reads as attribution failing. With it, the
    /// first is attribution reporting correctly and the growth has a different
    /// address entirely.
    #[test]
    fn an_allocator_holding_freed_memory_is_named_rather_than_blamed_on_the_owner_table() {
        let mut executor = inspection("bglab-sp1", false);
        executor.health.machine.process.resident_bytes =
            Measurement::measured(CAPTURED_AT - 5_000, 55_996_416 * 1024);
        executor.health.machine.process.virtual_bytes =
            Measurement::measured(CAPTURED_AT - 5_000, 63_438_848 * 1024);
        executor.health.machine.process.allocator = Measurement::measured(
            CAPTURED_AT - 5_000,
            AllocatorState {
                in_use_bytes: 1_288_489_984,
                free_retained_bytes: 55_620_051_763,
                arena_bytes: 56_800_235_356,
                mmapped_bytes: 117_440_512,
                mmap_chunk_count: 4,
            },
        );
        executor.health.machine.process.retained = ExecutorRetainedState {
            measured_at_unix_ms: CAPTURED_AT - 5_000,
            managed_manifests: RetainedOwner {
                entries: 41,
                estimated_bytes: 35_022_438,
                peak_entries: 44,
                peak_estimated_bytes: 35_022_438,
            },
            ..ExecutorRetainedState::default()
        };

        let rendered = render_executor(&executor);
        assert!(rendered.contains("Resident: 53.4 GiB"), "{rendered}");
        assert!(
            rendered.contains("Mapped address space: 60.5 GiB"),
            "mapped barely exceeds resident, which is not the fragmentation \
             shape: {rendered}"
        );
        assert!(
            rendered.contains("Allocator: 1.2 GiB in use, 51.8 GiB free but retained"),
            "the split that decides whether the fix is naming an owner or \
             finding a churn site: {rendered}"
        );
        assert!(
            rendered.contains("arena 52.9 GiB, mmapped 112.0 MiB across 4 chunks"),
            "where the retained bytes live — the arena, not returnable \
             mappings: {rendered}"
        );
        assert!(
            rendered.contains("Attributed total: 33.4 MiB"),
            "attribution reports what it can see, and it is tiny: {rendered}"
        );
    }

    /// An executor too old to publish retained state says so, rather than
    /// rendering as a daemon that measured itself and found nothing. The two
    /// look identical in a defaulted struct and mean opposite things.
    #[test]
    fn an_executor_without_retained_telemetry_is_named_as_unreported() {
        let mut executor = inspection("bglab-ub", false);
        executor.health.machine.process.resident_bytes =
            Measurement::measured(CAPTURED_AT - 1_000, 27 * 1024 * 1024);
        executor.health.machine.process.retained = ExecutorRetainedState::default();

        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("Retained state: not reported"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("Attributed total"),
            "a total of zero would claim a measurement that was never taken: {rendered}"
        );
    }

    /// A reading that was never taken renders as its named reason. A process
    /// size of "0 B" would read as a daemon that is not there.
    ///
    /// The allocator split is the sharpest case. Its zero value is a coherent
    /// and reassuring sentence — nothing in use, nothing retained, the
    /// healthiest heap a process can have — which is the exact opposite of
    /// "this platform's allocator was never asked". The reading is glibc's own
    /// bookkeeping, so every macOS and Windows machine in the fleet answers
    /// this way permanently and by design.
    #[test]
    fn an_unmeasured_process_size_renders_as_its_gap_not_as_zero() {
        let mut executor = inspection("bglab-win", false);
        executor.health.machine.process.resident_bytes =
            Measurement::unavailable(CAPTURED_AT, MeasurementGap::SamplingFailed);
        executor.health.machine.process.virtual_bytes =
            Measurement::unavailable(CAPTURED_AT, MeasurementGap::UnsupportedPlatform);
        executor.health.machine.process.allocator =
            Measurement::unavailable(CAPTURED_AT, MeasurementGap::UnsupportedPlatform);

        let rendered = render_executor(&executor);
        assert!(rendered.contains("Resident: unavailable"), "{rendered}");
        assert!(
            rendered.contains("Mapped address space: unavailable"),
            "{rendered}"
        );
        assert!(rendered.contains("Allocator: unavailable"), "{rendered}");
        assert!(!rendered.contains("Resident: 0 B"), "{rendered}");
        assert!(
            !rendered.contains("0 B in use"),
            "an unread allocator must not render as an empty one: {rendered}"
        );
    }

    #[test]
    fn unbounded_concurrency_capacity_is_named_without_rendering_its_sentinel() {
        let mut executor = inspection("bglab-ub", false);
        executor
            .health
            .admission
            .active_reservation
            .concurrency_units = 0;
        executor.health.admission.concurrency_capacity = Some(u32::MAX);

        let rendered = render_executor(&executor);

        assert!(rendered.contains("Concurrency: 0 reserved"));
        assert!(
            !rendered.contains("Concurrency: 0 reserved ("),
            "{rendered}"
        );
        assert!(!rendered.contains("Concurrency: 0/"), "{rendered}");
        assert!(!rendered.contains("Concurrency: 0 of "), "{rendered}");
        assert!(!rendered.contains('∞'), "{rendered}");
        assert!(!rendered.contains(&u32::MAX.to_string()), "{rendered}");
    }

    /// A machine that stopped measuring its own load reads as idle everywhere
    /// else on this page — low utilisation, units free, queues empty — so the
    /// one line that can contradict that has to be unmistakable.
    #[test]
    fn the_admission_section_says_when_the_cpu_brake_has_gone_blind() {
        let mut executor = inspection("blind", false);
        let went_quiet = executor.captured_at_unix_ms - CPU_ADMISSION_BLIND_AFTER_MS - 1;
        executor.health.host.cpu_admission = CpuAdmissionHealth {
            state: CpuAdmissionState::Accepting,
            utilization: Some(0.99),
            state_since_unix_ms: Some(went_quiet),
            measured_at_unix_ms: Some(went_quiet),
        };

        let rendered = render_executor(&executor);

        assert!(rendered.contains("Measured CPU brake: BLIND"), "{rendered}");
        assert!(
            !rendered.contains("accepting at 99%"),
            "a frozen reading must not be presented as a current verdict: {rendered}"
        );

        // Never measured is a different fact and must not read as blindness.
        executor.health.host.cpu_admission = CpuAdmissionHealth::default();
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("never measured on this machine"),
            "{rendered}"
        );
        assert!(!rendered.contains("BLIND"), "{rendered}");

        // And a fresh reading is reported as the verdict it is.
        executor.health.host.cpu_admission = CpuAdmissionHealth {
            state: CpuAdmissionState::Pressured,
            utilization: Some(0.97),
            state_since_unix_ms: Some(executor.captured_at_unix_ms),
            measured_at_unix_ms: Some(executor.captured_at_unix_ms),
        };
        let rendered = render_executor(&executor);
        assert!(rendered.contains("pressured at 97%"), "{rendered}");
    }

    #[test]
    fn finite_concurrency_capacity_keeps_its_configured_value() {
        let mut executor = inspection("finite", false);
        executor
            .health
            .admission
            .active_reservation
            .concurrency_units = 2;
        executor.health.admission.concurrency_capacity = Some(6);

        let rendered = render_executor(&executor);

        assert!(
            rendered.contains("Concurrency: 2 of 6 units reserved"),
            "{rendered}"
        );
    }

    /// The collection is an address book: every machine listed under the name a
    /// placement request accepts, with the platform and toolchains that decide
    /// whether it is the right one.
    #[test]
    fn the_collection_lists_every_machine_by_the_name_placement_accepts() {
        let rendered = render_executors(
            &[inspection("bglab-ub", false), inspection("local", true)],
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(rendered.contains("## bglab-ub"), "{rendered}");
        assert!(rendered.contains("## local"), "{rendered}");
        assert!(
            rendered.contains("cairn://executors/bglab-ub"),
            "{rendered}"
        );
        assert!(rendered.contains("rust, bun"), "{rendered}");
        assert!(rendered.contains("executor:{name:"), "{rendered}");
    }

    /// An empty fleet says what is true and what to do about it, rather than
    /// rendering an empty list an agent has to interpret.
    #[test]
    fn an_empty_fleet_renders_an_explanation_not_an_empty_list() {
        let rendered = render_executors(&[], &[], &[], None, CAPTURED_AT, false);
        assert!(rendered.contains("No executor is attached"), "{rendered}");
        assert!(rendered.contains("local"), "{rendered}");
    }

    #[test]
    fn a_fresh_session_with_no_readings_renders_warming_not_failure() {
        let rendered = render_executors(&[], &[], &[], None, CAPTURED_AT, true);
        assert!(rendered.contains("Status: warming"), "{rendered}");
        assert!(!rendered.contains("No executor is attached"), "{rendered}");
    }

    /// A runner that came up cleanly says nothing about restarts. The section
    /// has to stay silent in the ordinary case, or the one time it matters it
    /// reads as boilerplate.
    #[test]
    fn a_clean_runner_says_nothing_about_a_restart() {
        let rendered = render_executors(&[], &[], &[], None, CAPTURED_AT, false);
        assert!(!rendered.contains("Runner restarted"), "{rendered}");
    }

    /// The CAIRN-3419 requirement: a deliberate self-abort must still be legible
    /// after the successor is up and healthy. Time, cause, and the pid that died
    /// all have to survive the restart that erased everything else.
    #[test]
    fn a_runner_that_aborted_itself_reports_it_after_the_restart() {
        let exit = AbnormalExit {
            at_unix_ms: CAPTURED_AT - 90_000,
            pid: 74402,
            reason: "the transport liveness watchdog aborted the runner after 16 consecutive failed loopback probes over 62s (the listener was bound but its accept queue never drained)".to_string(),
        };
        let rendered = render_executors(&[], &[], &[], Some(&exit), CAPTURED_AT, false);
        assert!(rendered.contains("Runner restarted"), "{rendered}");
        assert!(
            rendered.contains("transport liveness watchdog"),
            "{rendered}"
        );
        assert!(rendered.contains("74402"), "{rendered}");
        // An age, so the reader knows whether this is now or last week.
        assert!(rendered.contains("1m ago"), "{rendered}");
        // A crash report is a reading or a NAMED gap, never an empty field.
        assert!(rendered.contains("Crash report:"), "{rendered}");
        // Interrupted work is the consequence an operator is actually chasing.
        assert!(rendered.contains("interrupted"), "{rendered}");
    }

    /// The restart is fleet context, so it belongs above the machines it
    /// explains rather than buried under them.
    #[test]
    fn a_restart_is_reported_above_the_machines_it_explains() {
        let exit = AbnormalExit {
            at_unix_ms: CAPTURED_AT - 1_000,
            pid: 1,
            reason: "aborted".to_string(),
        };
        let executor = inspection("local", true);
        let rendered = render_executors(
            std::slice::from_ref(&executor),
            &[],
            &[],
            Some(&exit),
            CAPTURED_AT,
            false,
        );
        let restart = rendered.find("Runner restarted").expect("restart section");
        let machine = rendered.find("## local").expect("machine section");
        assert!(restart < machine, "{rendered}");
    }

    /// Far enough from the epoch that a machine can have been gone for hours,
    /// which is the duration this whole record is about.
    const REMOTE_CAPTURED_AT: u64 = 10_000_000;

    fn enrolled(name: &str, link: RemoteLinkState) -> EnrolledRemote {
        EnrolledRemote {
            executor_id: name.into(),
            name: name.into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            link,
            last_attempt: Some(RemoteAttachAttempt::bootstrap_failure(
                REMOTE_CAPTURED_AT - 120_000,
                "ssh unreachable (ssh: connect to host bglab-ub port 22: No route to host)",
            )),
            last_seen_unix_ms: Some(REMOTE_CAPTURED_AT - 7_200_000),
            retired: Vec::new(),
            connection_timeline: Vec::new(),
            drained: false,
            role: Default::default(),
        }
    }

    /// A machine that was killed for its size still says how big it was.
    ///
    /// This is the page an operator opens after an out-of-memory kill, and
    /// until now it was the page that could say least: the executor's own
    /// memory section is built from what the machine is reporting, and a
    /// machine that was killed reports nothing. Every number it had died with
    /// it. The transition line is where the surviving reading belongs, because
    /// that line is already the record of the moment being investigated.
    ///
    /// The reading's own age is part of it. A sample taken four seconds before
    /// the end is the size at the end; one taken minutes before is a lower
    /// bound on a process that was still growing, and a reader who cannot tell
    /// those apart will draw a slope through the wrong point.
    #[test]
    fn a_machine_that_died_reports_the_size_it_died_at() {
        let mut remote = enrolled(
            "bglab-ub",
            RemoteLinkState::Absent {
                last_beat_unix_ms: None,
                last_reason: String::new(),
            },
        );
        remote.connection_timeline = vec![RemoteConnectionTransition {
            occurred_at_unix_ms: REMOTE_CAPTURED_AT - 3_600_000,
            phase: RemoteConnectionPhase::RemoteProcessExited,
            generation: Some(4),
            reason: Some("the remote executor exited".into()),
            remote_process_exit_status: Some(9),
            last_stderr: None,
            memory_at_generation_end: Some(GenerationEndMemory {
                received_at_unix_ms: REMOTE_CAPTURED_AT - 3_600_000 - 4_000,
                process: ProcessTelemetry {
                    resident_bytes: Measurement::measured(
                        REMOTE_CAPTURED_AT - 3_600_000 - 4_000,
                        10_200_547_328,
                    ),
                    allocator: Measurement::measured(
                        REMOTE_CAPTURED_AT - 3_600_000 - 4_000,
                        AllocatorState {
                            in_use_bytes: 43_000_000,
                            free_retained_bytes: 10_100_000_000,
                            arena_bytes: 10_150_000_000,
                            mmapped_bytes: 1_048_576,
                            mmap_chunk_count: 1,
                        },
                    ),
                    ..ProcessTelemetry::default()
                },
            }),
        }];

        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(
            rendered.contains("memory at generation end: resident 9.5 GiB"),
            "the size the kernel killed it at: {rendered}"
        );
        assert!(
            rendered.contains("allocator 41.0 MiB in use, 9.4 GiB free but retained"),
            "and where those bytes were, which is what decides the fix: \
             {rendered}"
        );
        assert!(
            rendered.contains("(measured 4s before it ended)"),
            "how close to the end the reading was taken: {rendered}"
        );
    }

    /// A reading the executor took long before its generation ended is not
    /// the size at the end, however recently a heartbeat carried it.
    ///
    /// The specimen this encodes: a sampler starved under the very pressure
    /// that killed the daemon, so its last heartbeats kept re-sending a 2 GiB
    /// reading while the kernel killed it at 10 GiB, and the page said
    /// "measured 1s before it ended" because it aged the heartbeat. The age is
    /// the reading's own, and past the sampler's bound no size is claimed.
    #[test]
    fn a_reading_older_than_the_sampler_bound_is_not_rendered_as_the_size_at_the_end() {
        let ended_at = REMOTE_CAPTURED_AT - 3_600_000;
        let mut remote = enrolled(
            "bgsrv-ub",
            RemoteLinkState::Absent {
                last_beat_unix_ms: None,
                last_reason: String::new(),
            },
        );
        let reading_taken_at = ended_at - 9 * 60_000;
        remote.connection_timeline = vec![RemoteConnectionTransition {
            occurred_at_unix_ms: ended_at,
            phase: RemoteConnectionPhase::RemoteProcessExited,
            generation: Some(9),
            reason: Some("the remote executor exited".into()),
            remote_process_exit_status: Some(9),
            last_stderr: None,
            memory_at_generation_end: Some(GenerationEndMemory {
                // The heartbeat arrived a second before the end; the reading
                // inside it is nine minutes old.
                received_at_unix_ms: ended_at - 1_000,
                process: ProcessTelemetry {
                    resident_bytes: Measurement::measured(reading_taken_at, 1_503_238_553),
                    allocator: Measurement::measured(
                        reading_taken_at,
                        AllocatorState {
                            in_use_bytes: 400_000_000,
                            free_retained_bytes: 1_000_000_000,
                            arena_bytes: 1_400_000_000,
                            mmapped_bytes: 0,
                            mmap_chunk_count: 0,
                        },
                    ),
                    ..ProcessTelemetry::default()
                },
            }),
        }];

        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(
            rendered.contains("memory at generation end: unknown"),
            "a stale reading must not be presented as the size at the end: {rendered}"
        );
        assert!(
            rendered.contains("last reading 1.4 GiB was taken 9m before it ended"),
            "its own age, not the heartbeat's: {rendered}"
        );
        assert!(
            rendered.contains("the executor had stopped measuring"),
            "{rendered}"
        );
        assert!(!rendered.contains("resident 1.4 GiB"), "{rendered}");
        assert!(
            !rendered.contains("measured 1s before it ended"),
            "{rendered}"
        );

        // A fresh reading in a stale heartbeat is the other half: the age is
        // still the reading's own.
        let fresh = GenerationEndMemory {
            received_at_unix_ms: ended_at - 60_000,
            process: ProcessTelemetry {
                resident_bytes: Measurement::measured(ended_at - 2_000, 1_503_238_553),
                ..ProcessTelemetry::default()
            },
        };
        let summary = generation_end_memory_summary(&fresh, ended_at);
        assert_eq!(
            summary,
            "memory at generation end: resident 1.4 GiB (measured 2s before it ended)"
        );
    }

    /// A generation with no retained reading says nothing about memory, rather
    /// than saying it had none. The transition itself still renders in full:
    /// the size is additional evidence on a line that stood on its own before
    /// it existed, and every machine that predates this reading has one.
    #[test]
    fn a_generation_without_a_retained_reading_claims_no_size() {
        let mut remote = enrolled(
            "bglab-ub",
            RemoteLinkState::Absent {
                last_beat_unix_ms: None,
                last_reason: String::new(),
            },
        );
        remote.connection_timeline = vec![RemoteConnectionTransition {
            occurred_at_unix_ms: REMOTE_CAPTURED_AT - 3_600_000,
            phase: RemoteConnectionPhase::RemoteProcessExited,
            generation: Some(4),
            reason: Some("the remote executor exited".into()),
            remote_process_exit_status: Some(9),
            last_stderr: None,
            memory_at_generation_end: None,
        }];

        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(rendered.contains("remote executor exited"), "{rendered}");
        assert!(rendered.contains("remote process exit 9"), "{rendered}");
        assert!(
            !rendered.contains("memory at generation end"),
            "a reading that was never retained must not render as one that \
             was: {rendered}"
        );
        assert!(!rendered.contains("resident 0 B"), "{rendered}");
    }

    /// A machine that is not reporting still has one operational fact worth
    /// stating: whether the runner is holding it drained. It decides what
    /// happens the moment the machine comes back.
    #[test]
    fn a_machine_page_states_its_role_and_its_attachment_attached_or_not() {
        let mut executor = inspection("sp1", false);
        let general = render_executor(&executor);
        assert!(
            general.contains("- Attachment: enrolled (attached over the executor protocol)\n"),
            "{general}"
        );
        assert!(
            general.contains("- Role: general (accepts every batch class)\n"),
            "{general}"
        );
        executor.health.role = MachineRole::InferenceOnly;
        let inference = render_executor(&executor);
        assert!(
            inference.contains("- Role: inference only (accepts none of today's batch classes)\n"),
            "{inference}"
        );
        let listed = render_executors(
            std::slice::from_ref(&executor),
            &[],
            &[],
            None,
            CAPTURED_AT,
            false,
        );
        assert!(listed.contains("- Role: inference only"), "{listed}");

        let mut remote = enrolled(
            "sp2",
            RemoteLinkState::Absent {
                last_beat_unix_ms: Some(REMOTE_CAPTURED_AT - 60_000),
                last_reason: String::new(),
            },
        );
        remote.role = MachineRole::InferenceOnly;
        let detached = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(detached.contains("- Attachment: enrolled"), "{detached}");
        assert!(detached.contains("- Role: inference only"), "{detached}");
        let overview = render_executors(&[], &[remote], &[], None, REMOTE_CAPTURED_AT, false);
        assert!(overview.contains("- Role: inference only"), "{overview}");
    }

    #[test]
    fn a_detached_machine_states_the_drain_it_is_under() {
        let mut remote = enrolled(
            "bglab-sp1",
            RemoteLinkState::Absent {
                last_beat_unix_ms: None,
                last_reason: String::new(),
            },
        );
        assert!(
            !render_enrolled_remote(&remote, REMOTE_CAPTURED_AT).contains("Draining"),
            "a machine under no drain says nothing about one"
        );
        remote.drained = true;
        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(rendered.contains("Draining: yes"), "{rendered}");
        assert!(
            rendered.contains("when this machine attaches"),
            "the drain has to say what it will do, not just that it exists: {rendered}"
        );
    }

    fn enrolled_after(name: &str, link: RemoteLinkState, account: &str) -> EnrolledRemote {
        let mut remote = enrolled(name, link);
        remote.last_attempt = Some(RemoteAttachAttempt::bootstrap_failure(
            REMOTE_CAPTURED_AT - 120_000,
            account,
        ));
        remote
    }

    fn lost_link_specimen(held_for_ms: u64) -> EnrolledRemote {
        let account = "the SSH session carrying the link exited after 433s (exit status: 255); composing bootstrap PATH\nprobe 7: toolchain present\nfatal: Not a valid commit name";
        let mut remote = enrolled(
            "bglab-mac",
            RemoteLinkState::Absent {
                last_beat_unix_ms: Some(REMOTE_CAPTURED_AT - 120_000),
                last_reason: "the link went away".to_string(),
            },
        );
        remote.last_attempt = Some(RemoteAttachAttempt::link_lost(
            REMOTE_CAPTURED_AT - 120_000,
            account,
            held_for_ms,
        ));
        remote
    }

    #[test]
    fn lost_link_summary_is_bounded_and_full_log_remains_reachable() {
        let remote = lost_link_specimen(433_000);
        let overview = render_executors(
            &[],
            std::slice::from_ref(&remote),
            &[],
            None,
            REMOTE_CAPTURED_AT,
            false,
        );
        assert!(!overview.contains("probe 7"), "{overview}");
        assert!(overview.contains("view=attach-log"), "{overview}");
        let machine = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(
            machine.contains("link lost after 7m of healthy operation"),
            "{machine}"
        );
        assert!(!machine.contains("bootstrap failed"), "{machine}");
        let log = render_attach_log(&remote, REMOTE_CAPTURED_AT);
        assert!(log.contains("probe 7"), "{log}");
        assert!(log.contains("fatal: Not a valid commit name"), "{log}");
        assert!(log.contains("Link held: 7m"), "{log}");
    }

    /// The retirement account sits beside the attempt's cause, one fact per
    /// line, and is absent when no retirement ran.
    #[test]
    fn an_attach_log_states_what_retiring_the_legacy_executor_did() {
        let mut remote = enrolled_after(
            "bbs",
            RemoteLinkState::NeverConnected {
                since_bootstrap_unix_ms: REMOTE_CAPTURED_AT - 60_000,
            },
            "installed; not dialled in",
        );
        assert!(!render_attach_log(&remote, REMOTE_CAPTURED_AT).contains("Retired"));
        remote.retired = vec![
            "supervisor cairn-runner.service (pid 812) running /usr/bin/cairn-executor: stopped and removed".into(),
            "home /home/bleu/.cairn-executor: generation released".into(),
        ];
        let log = render_attach_log(&remote, REMOTE_CAPTURED_AT);
        let cause = log.find("- Cause:").unwrap();
        let retired = log
            .find("- Retired:\n  - supervisor cairn-runner.service (pid 812)")
            .expect(&log);
        assert!(cause < retired, "{log}");
        assert!(
            log.contains("  - home /home/bleu/.cairn-executor: generation released\n"),
            "{log}"
        );

        remote.retired = vec!["nothing found: no pre-inversion supervisor or executor home".into()];
        assert!(render_attach_log(&remote, REMOTE_CAPTURED_AT)
            .contains("- Retired: nothing found: no pre-inversion supervisor or executor home\n"));
    }

    #[test]
    fn bootstrap_failure_remains_distinct_and_has_no_empty_log_link() {
        let remote = enrolled_after(
            "bglab-mac",
            RemoteLinkState::BootstrapFailed {
                cause: "ssh authentication refused (root@host: Permission denied (publickey).)"
                    .to_string(),
            },
            "ssh authentication refused (root@host: Permission denied (publickey).)",
        );
        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(
            rendered.contains("bootstrap failed — ssh authentication"),
            "{rendered}"
        );
        assert!(!rendered.contains("link lost"), "{rendered}");
        assert!(!rendered.contains("view=attach-log"), "{rendered}");
    }

    /// The defect this whole record exists for: three enrolled machines stopped
    /// reporting and this resource rendered exactly as though they had never
    /// been enrolled. An unattached machine is listed, named, and explained.
    #[test]
    fn an_enrolled_machine_that_is_not_attached_is_still_listed() {
        let rendered = render_executors(
            &[inspection("local", true)],
            &[enrolled(
                "bglab-ub",
                RemoteLinkState::Absent {
                    last_beat_unix_ms: None,
                    last_reason: String::new(),
                },
            )],
            &[],
            None,
            REMOTE_CAPTURED_AT,
            false,
        );
        assert!(rendered.contains("### bglab-ub"), "{rendered}");
        assert!(
            rendered.contains("Enrolled, not attached (1)"),
            "{rendered}"
        );
        assert!(rendered.contains("absent"), "{rendered}");
        assert!(rendered.contains("2h ago"), "{rendered}");
    }

    /// A fleet with no attached machine but live enrollments is NOT the same
    /// fact as a fleet with no machines, and saying "no executor is attached"
    /// alone is what let an outage read as an ordinary empty runner.
    #[test]
    fn every_machine_unattached_is_not_rendered_as_an_empty_fleet() {
        let rendered = render_executors(
            &[],
            &[
                enrolled(
                    "bglab-mac",
                    RemoteLinkState::BootstrapFailed {
                        cause: String::new(),
                    },
                ),
                enrolled(
                    "bglab-ub",
                    RemoteLinkState::Absent {
                        last_beat_unix_ms: None,
                        last_reason: String::new(),
                    },
                ),
            ],
            &[],
            None,
            REMOTE_CAPTURED_AT,
            false,
        );
        assert!(rendered.contains("### bglab-mac"), "{rendered}");
        assert!(rendered.contains("### bglab-ub"), "{rendered}");
        assert!(
            rendered.contains("enrolled with machines that are not reporting"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("The runner supervises a colocated executor"),
            "an outage must not render as the empty-runner explanation: {rendered}"
        );
    }

    /// The two down states mean opposite things about whether a person needs to
    /// act, so each one says which it is and what follows from it.
    #[test]
    fn the_two_down_states_are_told_apart_in_words() {
        // After the inversion the pair is not "unreachable versus attach
        // failed" — the runner does not reach for a machine at all. It is a
        // machine that is simply not here right now, which is ordinary and
        // self-correcting, versus a bootstrap that never completed, which
        // nothing retries because nothing was installed to retry.
        let absent = render_enrolled_remote(
            &enrolled(
                "bglab-ub",
                RemoteLinkState::Absent {
                    last_beat_unix_ms: Some(REMOTE_CAPTURED_AT - 7_200_000),
                    last_reason: "the link went away; the machine reconnects on its own"
                        .to_string(),
                },
            ),
            REMOTE_CAPTURED_AT,
        );
        assert!(absent.contains("absent — the link went away"), "{absent}");
        assert!(
            absent.contains("reconnects on its own"),
            "an ordinary absence must say nothing is waiting to be repaired: {absent}"
        );
        assert!(
            !absent.contains("bootstrap failed"),
            "a machine that is merely away has not failed: {absent}"
        );

        let mut refused = enrolled(
            "bglab-mac",
            RemoteLinkState::BootstrapFailed {
                cause: "ssh authentication refused (root@host: Permission denied (publickey).)"
                    .to_string(),
            },
        );
        refused.last_attempt = Some(RemoteAttachAttempt::bootstrap_failure(
            REMOTE_CAPTURED_AT - 120_000,
            "ssh authentication refused (root@host: Permission denied (publickey).)",
        ));
        let failed = render_enrolled_remote(&refused, REMOTE_CAPTURED_AT);
        assert!(
            failed.contains("bootstrap failed — ssh authentication refused ("),
            "{failed}"
        );
        assert!(
            failed.contains("once the cause is fixed"),
            "a ceremony that failed must say what clears it: {failed}"
        );
        assert!(failed.contains("Permission denied (publickey)"), "{failed}");
        assert!(failed.contains("2m ago"), "{failed}");
    }

    #[test]
    fn an_expected_artifact_publish_wait_uses_the_calm_register() {
        let account =
            "no aarch64-apple-darwin cairn-cmd artifact published for executor protocol v48";
        let mut waiting = enrolled(
            "bglab-mac",
            RemoteLinkState::BootstrapFailed {
                cause: String::new(),
            },
        );
        waiting.last_attempt = Some(
            RemoteAttachAttempt::bootstrap_failure(REMOTE_CAPTURED_AT - 120_000, account)
                .with_artifact_fault(Some(RemoteArtifactFault::Unpublished)),
        );
        let rendered = render_enrolled_remote(&waiting, REMOTE_CAPTURED_AT);

        assert!(
            rendered.contains(&format!("Status: {account}")),
            "{rendered}"
        );
        assert!(!rendered.contains("Status: attach failed"), "{rendered}");
        assert!(!rendered.contains("until the cause is fixed"), "{rendered}");
    }

    /// The calm register is earned by the FAULT, not by the wording. A machine
    /// whose release is broken has a problem someone must fix, and it must not
    /// inherit the reassuring text just because its message also mentions an
    /// artifact — which is exactly what a substring rule could not distinguish.
    #[test]
    fn a_broken_release_is_not_painted_as_a_publication_wait() {
        let account = "cairn-cmd artifact 0.12.32 for x86_64-unknown-linux-gnu size mismatch: manifest declares 11837192 bytes, found 11828496 bytes";
        let mut broken = enrolled(
            "bglab-ub",
            RemoteLinkState::BootstrapFailed {
                cause: String::new(),
            },
        );
        broken.last_attempt = Some(
            RemoteAttachAttempt::bootstrap_failure(REMOTE_CAPTURED_AT - 120_000, account)
                .with_artifact_fault(Some(RemoteArtifactFault::Integrity)),
        );
        let rendered = render_enrolled_remote(&broken, REMOTE_CAPTURED_AT);

        assert!(rendered.contains("once the cause is fixed"), "{rendered}");
        assert!(
            !rendered.contains("waiting for the runner's checksummed executor release"),
            "a broken release must not read as a publication anyone is waiting on: {rendered}"
        );
        // The evidence a person needs survives into the rendered line.
        assert!(rendered.contains("11828496"), "{rendered}");
    }

    /// Nothing on an unattached machine was measured, so nothing about its load
    /// is printed. A zero here would describe an idle machine, not an absent one.
    #[test]
    fn an_unattached_machine_reports_no_load_rather_than_zeroes() {
        let rendered = render_enrolled_remote(
            &enrolled(
                "bglab-ub",
                RemoteLinkState::Absent {
                    last_beat_unix_ms: None,
                    last_reason: String::new(),
                },
            ),
            REMOTE_CAPTURED_AT,
        );
        assert!(
            rendered.contains("No load, admission, or occupancy is reported"),
            "{rendered}"
        );
        assert!(!rendered.contains("0%"), "{rendered}");
        assert!(!rendered.contains("logical cores"), "{rendered}");
    }

    /// A machine that was installed and has not dialled yet is neither present
    /// nor broken, and claiming either would be a verdict nothing supports.
    #[test]
    fn a_machine_that_has_not_dialled_yet_claims_neither_outcome() {
        let mut remote = enrolled(
            "bglab-win",
            RemoteLinkState::NeverConnected {
                since_bootstrap_unix_ms: 0,
            },
        );
        remote.last_attempt = None;
        remote.last_seen_unix_ms = None;
        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(
            rendered.contains("installed; waiting for first dial"),
            "{rendered}"
        );
        assert!(rendered.contains("never attached"), "{rendered}");
        assert!(rendered.contains("none completed yet"), "{rendered}");
        assert!(!rendered.contains("did not answer"), "{rendered}");
        assert!(
            !rendered.contains("bootstrap failed"),
            "a machine that has not dialled has not failed: {rendered}"
        );
    }

    /// A ceremony waiting on the runner's own listener is the runner's fact.
    /// Rendering it as the machine's failure is what sent an operator to six
    /// hosts that had nothing wrong with them.
    #[test]
    fn a_runner_held_bootstrap_names_the_runner_and_never_the_machine() {
        let mut remote = enrolled(
            "bglab-ub",
            RemoteLinkState::BootstrapHeld {
                precondition:
                    cairn_common::executor_protocol::RunnerPrecondition::ListenerNotServing {
                        reason: "bind 0.0.0.0:3846: address in use".into(),
                    },
            },
        );
        remote.last_attempt = None;
        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(rendered.contains("held by the runner"), "{rendered}");
        assert!(rendered.contains("address in use"), "{rendered}");
        assert!(
            rendered.contains("Nothing about this host needs fixing"),
            "{rendered}"
        );
        assert!(!rendered.contains("bootstrap failed"), "{rendered}");
    }

    /// Every machine number carries the instant it was taken. An age computed
    /// from the heartbeat instead would let a fresh beat launder an hour-old
    /// reading.
    #[test]
    fn measured_telemetry_renders_its_value_with_the_age_of_the_reading() {
        let mut executor = inspection("bglab-ub", false);
        measured(&mut executor);
        let rendered = render_executor(&executor);
        assert!(rendered.contains("42% of 16 cores"), "{rendered}");
        assert!(
            rendered.contains("8.0 GiB available of 64.0 GiB"),
            "{rendered}"
        );
        assert!(rendered.contains("512.0 GiB free of 2.0 TiB"), "{rendered}");
        assert!(rendered.contains("(measured 2s ago)"), "{rendered}");
    }

    /// A gap is a named reason, never a zero. "This platform cannot answer" and
    /// "this machine has no memory left" are opposite facts, and rendering the
    /// first as `0 B available` states the second.
    #[test]
    fn an_unavailable_reading_renders_its_named_gap_and_never_a_zero() {
        let mut executor = inspection("bglab-ub", false);
        executor.health.machine.cpu = Measurement {
            measured_at_unix_ms: CAPTURED_AT,
            reading: MeasurementReading::Unavailable {
                reason: MeasurementGap::UnsupportedPlatform,
                detail: None,
            },
        };
        executor.health.machine.memory = Measurement {
            measured_at_unix_ms: CAPTURED_AT,
            reading: MeasurementReading::Unavailable {
                reason: MeasurementGap::SamplingFailed,
                detail: Some("sysinfo refresh returned no rows".into()),
            },
        };
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("CPU utilisation: unavailable: this platform has no reading"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "Memory: unavailable: sampling failed (sysinfo refresh returned no rows)"
            ),
            "{rendered}"
        );
        // The default Measurement is "not sampled yet", which is honest for a
        // freshly started executor and must not read as an empty volume.
        assert!(
            rendered.contains("Volume: unavailable: not sampled yet"),
            "{rendered}"
        );
        assert!(!rendered.contains("0 B available"), "{rendered}");
        assert!(!rendered.contains("0 B free"), "{rendered}");
    }

    /// Link staleness and fact staleness are separate verdicts. A machine
    /// beating on time while its numbers stop moving is a healthy link carrying
    /// history, and folding the two reports a working host as a dead one.
    #[test]
    fn stale_telemetry_is_reported_apart_from_a_healthy_link() {
        let mut executor = inspection("bglab-ub", false);
        executor.health.telemetry_stale = true;
        executor.health.liveness_age_ms = Some(200_000);
        let rendered = render_executor(&executor);
        assert!(rendered.contains("Status: online"), "{rendered}");
        assert!(
            rendered.contains("stale: these numbers are history"),
            "{rendered}"
        );
    }

    /// A freeze the machine explains has to be visible somewhere, or the
    /// runner's decision not to bounce it becomes the same silence reached by a
    /// different route.
    #[test]
    fn a_blocked_refresh_pass_names_its_phase_and_its_age() {
        let mut executor = inspection("fedora", false);
        executor.health.telemetry_stale = true;
        executor.health.liveness_age_ms = Some(400_000);
        executor.health.advertisement.facts_stale_refresh_blocked =
            Some(cairn_common::executor_protocol::LivenessRefreshBlocked {
                phase: cairn_common::executor_protocol::LivenessRefreshPhase::Snapshot,
                pass_started_unix_ms: CAPTURED_AT - 400_000,
                phase_entered_unix_ms: CAPTURED_AT - 400_000,
            });

        let rendered = render_executor(&executor);
        assert!(rendered.contains("Status: online"), "{rendered}");
        assert!(
            rendered.contains("Telemetry blocked: the refresh pass has been reading pool state"),
            "{rendered}"
        );
        assert!(
            rendered.contains("stands still until it returns"),
            "{rendered}"
        );
    }

    /// A slow pass is read off the page rather than the journal, with the time
    /// it spent waiting for pool state told apart from the time it worked.
    #[test]
    fn the_last_refresh_pass_renders_its_slow_phases_and_their_pool_state_waits() {
        use cairn_common::executor_protocol::{
            LivenessRefreshPhase, LivenessRefreshPhaseTiming, LivenessRefreshTimings,
        };
        let mut executor = inspection("bbs", false);
        let timing = |phase, elapsed_ms, pool_state_wait_ms| LivenessRefreshPhaseTiming {
            phase,
            elapsed_ms,
            pool_state_wait_ms,
        };
        executor.health.advertisement.last_refresh = Some(LivenessRefreshTimings {
            pass_started_unix_ms: CAPTURED_AT - 100_000,
            pass_ms: 88_192,
            phases: vec![
                timing(LivenessRefreshPhase::ExpireResidencies, 30_001, None),
                timing(LivenessRefreshPhase::EvictIdleManifests, 2, None),
                timing(LivenessRefreshPhase::Snapshot, 40, Some(38)),
                timing(LivenessRefreshPhase::SubstrateReport, 58_190, Some(58_100)),
                timing(LivenessRefreshPhase::WarmRoots, 1, None),
            ],
        });
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "- Last refresh: 88.2 s (expiring residencies 30.0 s; sampling substrate health 58.2 s, of which 58.1 s waiting for pool state)\n"
            ),
            "{rendered}"
        );

        executor.health.advertisement.last_refresh = Some(LivenessRefreshTimings {
            pass_started_unix_ms: CAPTURED_AT - 30_000,
            pass_ms: 14,
            phases: vec![timing(LivenessRefreshPhase::Snapshot, 3, Some(0))],
        });
        assert!(
            render_executor(&executor).contains("- Last refresh: 14 ms\n"),
            "a quick pass is one number"
        );

        executor.health.advertisement.last_refresh = None;
        assert!(!render_executor(&executor).contains("Last refresh"));
    }

    /// Which route a remote link took reads off the page.
    #[test]
    fn a_remote_link_names_its_path() {
        use cairn_common::executor_protocol::ExecutorLinkPath;
        let mut executor = inspection("bbs", false);
        executor.link_path = Some(ExecutorLinkPath {
            executor: "100.99.188.99:51234".parse().unwrap(),
            runner: "100.75.184.96:3846".parse().unwrap(),
        });
        assert!(
            render_executor(&executor)
                .contains("- Path: tailnet (100.99.188.99 → 100.75.184.96:3846)\n"),
            "{}",
            render_executor(&executor)
        );
        executor.link_path = Some(ExecutorLinkPath {
            executor: "10.10.10.10:51234".parse().unwrap(),
            runner: "10.10.10.51:3846".parse().unwrap(),
        });
        assert!(render_executor(&executor)
            .contains("- Path: direct (10.10.10.10 → 10.10.10.51:3846)\n"));
        executor.link_path = None;
        assert!(!render_executor(&executor).contains("- Path:"));
    }

    /// A drive timing out writes is visible on the page beside CPU and memory,
    /// and a platform without the reading says so rather than showing a quiet
    /// disk.
    #[test]
    fn io_pressure_renders_beside_cpu_and_memory_or_names_its_gap() {
        use cairn_common::executor_protocol::{DeviceWriteLatency, IoPressure, MeasurementGap};
        let mut executor = inspection("bbs", false);
        executor.health.host.io_pressure = Measurement::measured(
            CAPTURED_AT - 3_000,
            IoPressure {
                full_avg10_millis: 230,
                full_avg60_millis: 190,
                slowest_device: Some(DeviceWriteLatency {
                    device: "nvme1n1".into(),
                    mean_write_ms: Some(4_200),
                    writes: 10,
                    in_flight: 4,
                    window_ms: 15_000,
                }),
            },
        );
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "- I/O pressure: 23.0% of time fully stalled (10 s), 19.0% (60 s); slowest device nvme1n1 mean write 4.2 s, 4 I/Os in flight (measured 3s ago)\n"
            ),
            "{rendered}"
        );

        executor.health.host.io_pressure = Measurement::measured(
            CAPTURED_AT - 3_000,
            IoPressure {
                full_avg10_millis: 990,
                full_avg60_millis: 700,
                slowest_device: Some(DeviceWriteLatency {
                    device: "nvme1n1".into(),
                    mean_write_ms: None,
                    writes: 0,
                    in_flight: 4,
                    window_ms: 30_000,
                }),
            },
        );
        assert!(
            render_executor(&executor).contains(
                "; nvme1n1 stalled: no I/O completed in 30.0 s while busy throughout, 4 I/Os in flight (measured 3s ago)\n"
            ),
            "a stalled device is named as stalled"
        );

        executor.health.host.io_pressure =
            Measurement::unavailable(CAPTURED_AT - 3_000, MeasurementGap::UnsupportedPlatform);
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("- I/O pressure: unavailable: this platform has no reading\n"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- Kernel I/O timeouts: not measured;"),
            "{rendered}"
        );
    }

    fn gpu(
        model: &str,
        uuid: Option<&str>,
        cores: Option<u32>,
        memory: GpuMemory,
        utilization_millis: MeasurementReading<u32>,
    ) -> GpuDevice {
        use cairn_common::executor_protocol::GpuVendor;
        GpuDevice {
            index: 0,
            vendor: if model.starts_with("Apple") {
                GpuVendor::Apple
            } else {
                GpuVendor::Nvidia
            },
            model: model.into(),
            uuid: uuid.map(Into::into),
            cores,
            memory: MeasurementReading::Measured { value: memory },
            utilization_millis,
        }
    }

    fn host_pool() -> MeasurementReading<UnifiedPool> {
        MeasurementReading::Measured {
            value: UnifiedPool::HostMemory,
        }
    }

    fn gb10_with_pool(pool: MeasurementReading<UnifiedPool>) -> GpuDevice {
        gpu(
            "NVIDIA GB10",
            Some("GPU-cd34"),
            None,
            GpuMemory::Unified {
                gpu_allocated_bytes: MeasurementReading::Measured {
                    value: 107_632_132_096,
                },
                source: UnifiedAllocationSource::NvmlComputeProcesses,
                pool,
            },
            MeasurementReading::Measured { value: 350 },
        )
    }

    fn gb10() -> GpuDevice {
        gb10_with_pool(host_pool())
    }

    /// bginf-sp1's `/proc/meminfo` while models were resident: MemTotal
    /// 127,600,812 kB, MemAvailable 7,971,312 kB (cairn/5068, 10-17).
    fn spark_memory() -> Measurement<MachineMemory> {
        Measurement::measured(
            CAPTURED_AT - 2_000,
            MachineMemory {
                total_bytes: 127_600_812 * 1024,
                available_bytes: 7_971_312 * 1024,
            },
        )
    }

    /// Each GPU renders as one line under its own heading after placement
    /// telemetry, discrete memory as its own number and unified memory as the
    /// machine's Memory reading, quoted rather than repeated.
    #[test]
    fn gpus_render_discrete_and_unified_memory_per_device() {
        let mut executor = inspection("bbs", false);
        measured(&mut executor);
        executor.health.host.gpus = Measurement::measured(
            CAPTURED_AT - 2_000,
            vec![gpu(
                "NVIDIA GeForce RTX 4090",
                Some("GPU-ab12"),
                None,
                GpuMemory::Discrete {
                    used_bytes: 19_541_000_192,
                    total_bytes: 25_757_220_864,
                },
                MeasurementReading::Measured { value: 870 },
            )],
        );
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "## GPU\n- GPU 0 NVIDIA GeForce RTX 4090 (GPU-ab12): discrete memory 18.2 GiB used of 24.0 GiB, not part of system memory; utilisation 87% (measured 2s ago)\n"
            ),
            "{rendered}"
        );
        assert!(
            rendered.find("## Placement telemetry") < rendered.find("## GPU"),
            "{rendered}"
        );

        let laptop_memory = executor.health.machine.memory.clone();
        executor.health.machine.memory = spark_memory();
        executor.health.host.gpus = Measurement::measured(CAPTURED_AT - 2_000, vec![gb10()]);
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("- Memory: 7.6 GiB available of 121.7 GiB (measured 2s ago)\n"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "- GPU 0 NVIDIA GB10 (GPU-cd34): unified memory, one pool with system memory: 7.6 GiB available of 121.7 GiB, the Memory reading above, not a second number; GPU-allocated 100.2 GiB by compute processes; utilisation 35% (measured 2s ago)\n"
            ),
            "{rendered}"
        );
        executor.health.machine.memory = laptop_memory;

        executor.health.host.gpus = Measurement::measured(
            CAPTURED_AT - 2_000,
            vec![gpu(
                "Apple M4 Pro",
                None,
                Some(16),
                GpuMemory::Unified {
                    gpu_allocated_bytes: MeasurementReading::Measured { value: 172_130_304 },
                    source: UnifiedAllocationSource::AppleDriverInUse,
                    pool: host_pool(),
                },
                MeasurementReading::Unavailable {
                    reason: MeasurementGap::SamplingFailed,
                    detail: Some("PerformanceStatistics has no \"Device Utilization %\"".into()),
                },
            )],
        );
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "- GPU 0 Apple M4 Pro (16 cores): unified memory, one pool with system memory: 8.0 GiB available of 64.0 GiB, the Memory reading above, not a second number; GPU driver in use 164.2 MiB; utilisation unavailable: sampling failed (PerformanceStatistics has no \"Device Utilization %\") (measured 2s ago)\n"
            ),
            "{rendered}"
        );
    }

    /// A unified pool is never sized from anywhere but the Memory reading,
    /// so when that reading is a gap the pool is one too.
    #[test]
    fn a_unified_pool_without_a_memory_reading_says_so() {
        let mut executor = inspection("spark", false);
        executor.health.machine.memory =
            Measurement::unavailable(CAPTURED_AT - 2_000, MeasurementGap::SamplingFailed);
        executor.health.host.gpus = Measurement::measured(CAPTURED_AT - 2_000, vec![gb10()]);
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "unified memory, one pool with system memory: the Memory reading above is unavailable, so the pool's size is too; GPU-allocated 100.2 GiB"
            ),
            "{rendered}"
        );
    }

    /// A cgroup limit is never presented as a unified GPU's pool. The
    /// executor's Memory reading stays exactly what the brake acts on, and the
    /// GPU line says the physical pool is unmeasured rather than placing
    /// 100 GiB of GPU allocations inside a 32 GiB limit.
    #[test]
    fn a_cgroup_limited_executor_does_not_present_its_limit_as_the_gpu_pool() {
        let mut executor = inspection("spark", false);
        let limit_bytes = 32 * 1024 * 1024 * 1024;
        executor.health.machine.memory = Measurement::measured(
            CAPTURED_AT - 2_000,
            MachineMemory {
                total_bytes: limit_bytes,
                available_bytes: 4 * 1024 * 1024 * 1024,
            },
        );
        executor.health.host.gpus = Measurement::measured(
            CAPTURED_AT - 2_000,
            vec![gb10_with_pool(MeasurementReading::Measured {
                value: UnifiedPool::CgroupLimited { limit_bytes },
            })],
        );
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("- Memory: 4.0 GiB available of 32.0 GiB (measured 2s ago)\n"),
            "the brake's reading is unchanged: {rendered}"
        );
        let line = rendered
            .lines()
            .find(|line| line.starts_with("- GPU 0 NVIDIA GB10"))
            .expect("the GB10 line");
        assert_eq!(
            line,
            "- GPU 0 NVIDIA GB10 (GPU-cd34): unified memory, one pool with system memory: pool UNMEASURED: the Memory reading above is this executor's cgroup limit of 32.0 GiB, which bounds its own processes but not the GPU's allocations; GPU-allocated 100.2 GiB by compute processes; utilisation 35% (measured 2s ago)"
        );
        assert!(!line.contains("available of"), "{line}");

        executor.health.host.gpus = Measurement::measured(
            CAPTURED_AT - 2_000,
            vec![gb10_with_pool(MeasurementReading::Unavailable {
                reason: MeasurementGap::SamplingFailed,
                detail: Some("the system memory probe returned no reading".into()),
            })],
        );
        assert!(
            render_executor(&executor).contains(
                "one pool with system memory: pool unavailable: sampling failed (the system memory probe returned no reading); GPU-allocated"
            ),
            "{}",
            render_executor(&executor)
        );
    }

    /// No reading is never "no GPUs": a host without a reader, and an
    /// executor too old to report, name why; a reader that found nothing says
    /// exactly that.
    #[test]
    fn gpu_gaps_render_unmeasured_and_an_empty_list_renders_as_none_found() {
        let mut executor = inspection("bgsrv-ub", false);
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("## GPU\n- UNMEASURED: not sampled yet\n"),
            "an older executor: {rendered}"
        );

        executor.health.host.gpus = Measurement::unavailable_with(
            CAPTURED_AT - 1_000,
            MeasurementGap::UnsupportedPlatform,
            "NVML not loadable: libnvidia-ml.so.1: cannot open shared object file",
        );
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(
                "## GPU\n- UNMEASURED: this platform has no reading (NVML not loadable: libnvidia-ml.so.1: cannot open shared object file)\n"
            ),
            "{rendered}"
        );

        executor.health.host.gpus = Measurement::measured(CAPTURED_AT - 2_000, Vec::new());
        assert!(
            render_executor(&executor).contains(
                "## GPU\n- No GPU devices: the reader works and found none (measured 2s ago)\n"
            ),
            "{}",
            render_executor(&executor)
        );
    }

    /// Past the sampler's bound the device lines are history, and the section
    /// says so before them.
    #[test]
    fn a_gpu_reading_past_the_sampler_bound_renders_stale() {
        let mut executor = inspection("spark", false);
        let bound_s = MACHINE_READING_STALE_AFTER_MS / 1_000;
        executor.health.host.gpus = Measurement::measured(
            CAPTURED_AT - MACHINE_READING_STALE_AFTER_MS - 1_000,
            vec![gb10()],
        );
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains(&format!(
                "## GPU\n- STALE: last reading {}s ago, past the {bound_s}s sampler bound; these are history, and anything that acts on GPU headroom must treat them as unmeasured\n- GPU 0 NVIDIA GB10",
                bound_s + 1
            )),
            "{rendered}"
        );

        executor.health.host.gpus = Measurement::measured(
            CAPTURED_AT - MACHINE_READING_STALE_AFTER_MS + 1_000,
            vec![gb10()],
        );
        let rendered = render_executor(&executor);
        assert!(!rendered.contains("STALE"), "{rendered}");
        assert!(
            rendered.contains("## GPU\n- GPU 0 NVIDIA GB10"),
            "{rendered}"
        );
    }

    /// Nothing internal escapes into either representation: not the opaque
    /// executor or device identity, not a path on the machine's disk. An address
    /// an agent cannot discover is not an address, and one it must not know is
    /// not published beside the ones it must.
    #[test]
    fn neither_representation_leaks_an_opaque_identity() {
        let mut executor = inspection("bglab-ub", false);
        measured(&mut executor);
        executor.occupancy.resident_occupancy = Some(ResidentOccupancyEvidence {
            process_count: 2,
            reservation: ResourceReservation::default(),
        });
        for rendered in [
            render_executor(&executor),
            render_executors(
                std::slice::from_ref(&executor),
                &[],
                &[],
                None,
                CAPTURED_AT,
                false,
            ),
        ] {
            assert!(!rendered.contains("executor-7b21ce"), "{rendered}");
            assert!(!rendered.contains("device-9f3c1a"), "{rendered}");
            assert!(!rendered.contains("/Users/"), "{rendered}");
            assert!(!rendered.contains("credential"), "{rendered}");
        }
        assert!(render_executor(&executor).contains("Resident processes: 2"));
    }

    /// Eviction is the one moment the fleet destroys an owner's uncommitted
    /// work. A machine's own log is not where the owner looks, so the loss is
    /// named here: which environment, whose it was, and why it could not be
    /// taken.
    #[test]
    fn an_evicted_environment_names_its_holder_and_the_reason() {
        let mut executor = inspection("bglab-mac", false);
        assert!(
            !render_executor(&executor).contains("evicted"),
            "a machine that evicted nothing says nothing"
        );

        executor.health.evicted_cells = vec![EvictedCellReport {
            project_id: "cairn".into(),
            cell_id: "slot-42".into(),
            holder: "job:wedged".into(),
            reason: "checkout stands on commit fc46de87, which this machine's object store cannot supply".into(),
            evicted_at_unix_ms: CAPTURED_AT - 60_000,
            retirement_failed: false,
        }];
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("## Environments evicted at adoption"),
            "{rendered}"
        );
        assert!(
            rendered.contains(
                "- slot-42 (job:wedged, 1m ago): checkout stands on commit fc46de87, which this machine's object store cannot supply"
            ),
            "{rendered}"
        );
        assert!(!rendered.contains("STILL ON DISK"), "{rendered}");

        // A cell that was refused but would not retire is the worse of the two
        // and must not read as the ordinary one.
        executor.health.evicted_cells[0].retirement_failed = true;
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("STILL ON DISK: retirement failed"),
            "{rendered}"
        );
    }

    /// A slot claims keep declining is still in the pool, so the page says so
    /// plainly, with the lock, its age, and its holder, or the diagnostic when
    /// no lock was named.
    #[test]
    fn a_slot_declined_at_claim_names_its_lock_and_stays_in_the_pool() {
        let mut executor = inspection("bglab-mac", false);
        assert!(
            !render_executor(&executor).contains("## Slots declined at claim"),
            "a machine that declined nothing says nothing"
        );

        executor.health.busy_declined_cells =
            vec![cairn_common::executor_protocol::BusyDeclinedCellReport {
                project_id: "cairn".into(),
                cell_id: "slot-42".into(),
                consecutive_declines: 5,
                first_declined_at_unix_ms: CAPTURED_AT - 240_000,
                last_declined_at_unix_ms: CAPTURED_AT - 10_000,
                lock_path: Some("/r/.git/worktrees/slot-42/index.lock".into()),
                lock_age_ms: Some(252_000),
                holder: Some("812 git".into()),
                diagnostic: "claim found live Git lock /r/.git/worktrees/slot-42/index.lock (held by 812 git)".into(),
            }];
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("## Slots declined at claim"),
            "{rendered}"
        );
        let line = rendered
            .lines()
            .find(|line| line.starts_with("- slot-42 (cairn)"))
            .unwrap_or_else(|| panic!("{rendered}"));
        assert!(
            line.contains("declined 5 claims in a row, the first 4m ago;"),
            "{line}"
        );
        assert!(
            line.contains("Git lock /r/.git/worktrees/slot-42/index.lock"),
            "{line}"
        );
        assert!(
            line.contains("index.lock, 4.2m old, held by 812 git"),
            "{line}"
        );
        assert!(line.contains("held by 812 git"), "{line}");
        assert!(
            line.contains("Still in the pool, not quarantined"),
            "{line}"
        );

        let declined = &mut executor.health.busy_declined_cells[0];
        declined.holder = None;
        assert!(render_executor(&executor).contains("no holder could be proven"));

        // A decline from a git step's contention may name no lock at all; the
        // diagnostic is then the whole account.
        let declined = &mut executor.health.busy_declined_cells[0];
        declined.lock_path = None;
        declined.lock_age_ms = None;
        declined.diagnostic =
            "switch idle pool slot: The lock for the packed-ref file could not be obtained".into();
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("the first 4m ago; switch idle pool slot: The lock for the packed-ref file could not be obtained."),
            "{rendered}"
        );
    }

    /// An older executor's report carries no declined slots, and reads as none.
    #[test]
    fn a_report_without_declined_slots_decodes_to_none() {
        let mut report = serde_json::to_value(
            cairn_common::executor_protocol::ExecutorSubstrateReport::default(),
        )
        .unwrap();
        assert!(
            report.get("busyDeclinedCells").is_none(),
            "an empty list is not sent"
        );
        report.as_object_mut().unwrap().remove("busyDeclinedCells");
        let decoded: cairn_common::executor_protocol::ExecutorSubstrateReport =
            serde_json::from_value(report).unwrap();
        assert!(decoded.busy_declined_cells.is_empty());
    }

    /// A stop names the command, the evidence that justified it, and anything
    /// its teardown could not end, so a surviving pid is visible here rather
    /// than only in the host's process table.
    #[test]
    fn a_stopped_command_names_its_cause_and_its_survivors() {
        let mut executor = inspection("bglab-mac", false);
        assert!(
            !render_executor(&executor).contains("## Stopped commands"),
            "a machine that stopped nothing says nothing"
        );
        executor.health.stopped_commands =
            vec![cairn_common::executor_protocol::StoppedCommandReport {
                request_id: "req-1".into(),
                header: "rust-tests".into(),
                cause: cairn_common::executor_protocol::CommandStop::Silent {
                    quiet_ms: 900_000,
                    last_output_unix_ms: None,
                    cpu_ms_in_window: Some(40),
                },
                stopped_at_unix_ms: CAPTURED_AT - 120_000,
                signalled_groups: vec![4120, 4133],
                surviving_pids: vec![4133],
            }];
        let rendered = render_executor(&executor);
        assert!(rendered.contains("## Stopped commands"), "{rendered}");
        assert!(rendered.contains("- rust-tests (2m ago, request req-1): no output and no CPU progress across its process tree for 900s"), "{rendered}");
        assert!(
            rendered.contains("signalled groups 4120, 4133"),
            "{rendered}"
        );
        assert!(rendered.contains("STILL RUNNING: pids 4133"), "{rendered}");
    }

    #[test]
    fn a_platform_without_stall_detection_says_so() {
        let rendered = render_stopped_commands(&[], "windows", CAPTURED_AT);
        assert!(
            rendered.contains("Stall detection: unavailable"),
            "{rendered}"
        );
        assert!(render_stopped_commands(&[], "macos", CAPTURED_AT).is_empty());
    }

    #[test]
    fn command_and_resident_ownership_render_as_distinct_health() {
        let mut executor = inspection("bglab-ub", false);
        executor.health.resident_processes.tracked_live_count = 2;
        executor.health.command_processes.tracked_live_count = 3;
        executor.health.command_processes.reaped_stale_count = 4;
        executor.health.command_processes.unverified_count = 1;
        executor.health.command_processes.oldest_age_ms = Some(5_000);
        let rendered = render_executor(&executor);
        assert!(
            rendered.contains("Resident ownership: 2 tracked"),
            "{rendered}"
        );
        assert!(
            rendered
                .contains("Command ownership: 3 tracked, 4 stale reaped, 1 unverified, oldest 5s",),
            "{rendered}"
        );
    }

    /// A name that addresses nothing is answered with every name that does — the
    /// same list the collection renders, from the same cache — so the refusal
    /// and the resource cannot disagree.
    /// The two questions an operator has about a fleet are "why did that run
    /// there" and "why is this machine idle", and both are answered off one
    /// record. Local winning has to read as a measured result, because "local
    /// fallback" being an unstated default is exactly how a broken remote stays
    /// invisible.
    #[test]
    fn a_placement_record_names_the_machine_the_evidence_and_the_passed_over() {
        use cairn_common::executor_protocol::{
            CpuPressure, MachineMeasurement, MachineMemory, MachineVolume, ObservationReuse,
            PlacementDecision, PlacementMobility, PlacementOutcome, PlacementReadings,
            PlacementReason, PlacementRejection, PlacementRejectionReason, PlacementSelection,
            PlacementSyncCost, ReservationRationale, ResourceReservation,
            ResourceReservationSource,
        };

        let mut executor = inspection("bglab-ub", false);
        executor.recent_placements = vec![PlacementDecision {
            request_id: "check-cadence-7".into(),
            attempt_id: "a".into(),
            decided_at_unix_ms: CAPTURED_AT,
            mobility: PlacementMobility::SpillEligible,
            selector: None,
            pinned_executor_id: None,
            unpinned_winner: None,
            binding: Vec::new(),
            policy: None,
            outcome: PlacementOutcome::Selected(Box::new(PlacementSelection {
                cache_distance: None,
                prediction: None,
                memory_ceiling: None,
                executor_name: "bglab-ub".into(),
                executor_id: "executor-7b21ce".into(),
                colocated: false,
                reason: PlacementReason::PredictedEarliestVerdict,
                tie_break: None,
                readings: PlacementReadings {
                    cpu: Measurement::measured(
                        CAPTURED_AT - 2_000,
                        CpuPressure {
                            utilization: 0.03,
                            user: 0.02,
                            system: 0.01,
                            logical_cores: 16,
                        },
                    ),
                    memory: Measurement::measured(
                        CAPTURED_AT - 2_000,
                        MachineMemory {
                            total_bytes: 64 * 1024 * 1024 * 1024,
                            available_bytes: 48 * 1024 * 1024 * 1024,
                        },
                    ),
                    volume: Measurement::measured(
                        CAPTURED_AT - 2_000,
                        MachineVolume {
                            total_bytes: 1024 * 1024 * 1024 * 1024,
                            free_bytes: 900 * 1024 * 1024 * 1024,
                        },
                    ),
                },
                reservation: ResourceReservation {
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_growth_bytes: 4 * 1024 * 1024 * 1024,
                    concurrency_units: 1,
                    source: ResourceReservationSource::Learned,
                },
                reservation_rationale: ReservationRationale {
                    declared_concurrency_units: Some(1),
                    profile_key: Some("check:rust".into()),
                    profile_context: "device:executor on linux/x86_64 with toolchains [rust]"
                        .into(),
                    sample_count: 7,
                    upper_peak_rss_bytes: Some(1_600),
                    upper_disk_growth_bytes: Some(3_200),
                    upper_duration_ms: Some(90_000),
                    prior: ResourceReservation::default(),
                    headroom_percent: 25,
                    fallback: None,
                },
                sync_cost: PlacementSyncCost::Known { bytes: 12_345 },
                object_transfer: None,
                observation_reuse: ObservationReuse::UntrustedRemoteEnvironment,
            })),
            rejected: vec![PlacementRejection {
                prediction: None,
                executor_name: "local".into(),
                executor_id: "colocated".into(),
                reason: PlacementRejectionReason::TelemetryGap {
                    measurement: MachineMeasurement::Volume,
                    gap: MeasurementGap::SamplingFailed,
                },
            }],
        }];

        let status = render_executor(&executor);
        assert!(!status.contains("check-cadence-7"), "{status}");
        assert!(
            status.contains("cairn://executors/bglab-ub?view=placements"),
            "{status}"
        );

        let listed = render_placements(&executor);
        assert!(listed.contains("check-cadence-7"), "{listed}");
        assert!(
            listed.contains("?view=placement&request=check-cadence-7"),
            "{listed}"
        );
        assert!(
            !listed.contains("3% busy across 16 cores"),
            "decision lists must not inline evidence trees: {listed}"
        );
        assert!(
            !listed.contains("Passed over local"),
            "candidate details belong to the single-decision view: {listed}"
        );

        let rendered = render_placement(&executor, "check-cadence-7");
        assert!(rendered.contains("check-cadence-7"), "{rendered}");
        assert!(rendered.contains("spillEligible"), "{rendered}");
        assert!(rendered.contains("predictedEarliestVerdict"), "{rendered}");
        assert!(
            rendered.contains("3% busy across 16 cores"),
            "the reading that decided it is on the record: {rendered}"
        );
        assert!(
            rendered.contains("measured 2s ago"),
            "and so is when it was taken: {rendered}"
        );
        assert!(
            rendered.contains("7 observations of `check:rust`"),
            "the reservation says what it was learned from: {rendered}"
        );
        assert!(
            rendered.contains("1 concurrency unit declared by the caller"),
            "a declared lane count is never presented as something a profile concluded: {rendered}"
        );
        assert!(
            !rendered.contains("low confidence"),
            "seven observations are past the floor: {rendered}"
        );
        assert!(
            rendered.contains("observation non-reusable"),
            "a spilled check seeds no baseline, and that is stated: {rendered}"
        );
        assert!(
            rendered.contains("Passed over local: volume is unavailable: sampling failed"),
            "the idle machine's own reason is readable off the record: {rendered}"
        );

        executor.recent_placements[0]
            .rejected
            .push(PlacementRejection {
                prediction: None,
                executor_name: "bglab-mac".into(),
                executor_id: "bglab-mac".into(),
                reason: PlacementRejectionReason::Held {
                    evidence: cairn_common::executor_protocol::AdmissionHoldEvidence::Disk {
                        free_bytes: 1024 * 1024 * 1024 * 14 / 10,
                        floor_bytes: 2 * 1024 * 1024 * 1024,
                        sweep_in_flight: false,
                    },
                },
            });
        let rendered = render_placement(&executor, "check-cadence-7");
        assert!(
            rendered.contains(
                "Passed over bglab-mac: held: disk free 1.4 GiB (floor 2.0 GiB), no sweep in flight"
            ),
            "a machine demoted by its posted condition names it with its figures: {rendered}"
        );

        let PlacementOutcome::Selected(selection) = &mut executor.recent_placements[0].outcome
        else {
            unreachable!("fixture is selected")
        };
        selection.reason = PlacementReason::Pinned;
        let routine = render_placements(&executor);
        assert!(routine.contains("1 routine placement"), "{routine}");
        assert!(routine.contains("Requests:"), "{routine}");
        assert!(
            routine.contains("?view=placement&request=check-cadence-7"),
            "collapsed routine decisions remain individually drillable: {routine}"
        );
        assert!(!routine.contains("Passed over local"), "{routine}");
    }

    /// A machine that has taken part in no recent decision says that, rather
    /// than rendering an empty heading an operator has to interpret.
    #[test]
    fn a_machine_with_no_recent_placement_says_so() {
        let rendered = render_placements(&inspection("bglab-win", false));
        assert!(
            rendered.contains("No placement decision in the runner's recent window"),
            "{rendered}"
        );
    }

    #[test]
    fn an_unknown_placement_request_points_back_to_the_available_list() {
        let rendered = render_placement(&inspection("bglab-win", false), "missing");
        assert!(rendered.contains("missing"), "{rendered}");
        assert!(rendered.contains("?view=placements"), "{rendered}");
    }

    #[test]
    fn an_unknown_name_is_refused_with_the_names_that_exist() {
        let executors = [inspection("bglab-ub", false), inspection("local", true)];
        let refusal = unknown_executor("bglab-win", &executors, &[]);
        assert!(refusal.contains("bglab-win"), "{refusal}");
        assert!(refusal.contains("bglab-ub, local"), "{refusal}");
        assert!(refusal.contains("cairn://executors"), "{refusal}");

        let empty = unknown_executor("bglab-win", &[], &[]);
        assert!(
            empty.contains("no executor is currently attached"),
            "{empty}"
        );
    }

    /// An enrolled name addresses a real machine. Refusing it as unknown would
    /// tell a caller that a machine it is entitled to ask about does not exist.
    #[test]
    fn a_refusal_names_the_enrolled_machines_too() {
        let refusal = unknown_executor(
            "typo",
            &[inspection("local", true)],
            &[enrolled(
                "bglab-ub",
                RemoteLinkState::Absent {
                    last_beat_unix_ms: None,
                    last_reason: String::new(),
                },
            )],
        );
        assert!(
            refusal.contains("Enrolled but not attached: bglab-ub"),
            "{refusal}"
        );
    }

    /// A build the runner did not deploy is the difference between "this ran
    /// your code" and "this ran something else", so it is stated rather than
    /// left to be inferred from a build id.
    #[test]
    fn a_build_skew_is_named_rather_than_left_to_inference() {
        let mut executor = inspection("bglab-ub", false);
        executor.health.build_skew = Some(cairn_common::executor_protocol::BuildSkew {
            runner_build_id: "build-new".into(),
            executor_build_id: "build-old".into(),
        });
        let rendered = render_executor(&executor);
        assert!(rendered.contains("SKEWED"), "{rendered}");
        assert!(rendered.contains("build-old"), "{rendered}");
        assert!(rendered.contains("build-new"), "{rendered}");
    }
}

#[cfg(test)]
mod supervision_tests {
    use super::*;
    use cairn_common::executor_protocol::RetirementPhase;

    const NOW: u64 = 1_800_000_000_000;

    fn held(reason: GenerationHoldReason) -> ColocatedSupervision {
        ColocatedSupervision {
            generation_hold: Some(ColocatedGenerationHold {
                holder_pid: Some(12184),
                holder_acquired_at_unix_ms: Some(NOW - 21_419_135),
                observed_since_unix_ms: NOW - 192_000,
                reason,
            }),
            ..Default::default()
        }
    }

    /// The view that hid comment #1's 84 hours: with `local` unattached, the
    /// supervisor's own account is what says why.
    #[test]
    fn an_unattached_local_names_the_process_holding_its_generation() {
        let rendered = render_colocated_supervision(
            None,
            &held(GenerationHoldReason::Unidentified),
            false,
            NOW,
        );
        assert!(rendered.contains("`local` is not attached"), "{rendered}");
        assert!(
            rendered.contains("generation held by pid 12184 (recorded 5h ago)"),
            "{rendered}"
        );
        assert!(rendered.contains("3m ago"), "{rendered}");
        assert!(rendered.contains("will not signal"), "{rendered}");
    }

    #[test]
    fn a_stuck_retirement_names_the_phase_it_reached() {
        let rendered = render_colocated_supervision(
            None,
            &held(GenerationHoldReason::RetirementStuck {
                phase: RetirementPhase::Force,
            }),
            false,
            NOW,
        );
        assert!(
            rendered.contains(
                "pid 12184 (recorded 5h ago) is still alive after retirement reached SIGKILL"
            ),
            "{rendered}"
        );
    }

    #[test]
    fn a_failing_successor_reads_its_start_count_and_cause() {
        let supervision = ColocatedSupervision {
            failing_since_unix_ms: Some(NOW - 600_000),
            failed_attempts: 40,
            last_cause: Some("child exited status=1; bootstrap failed".into()),
            ..Default::default()
        };
        let rendered = render_colocated_supervision(None, &supervision, true, NOW);
        assert!(
            rendered.contains("successor failing since") && rendered.contains("(40 attempts)"),
            "{rendered}"
        );
        assert!(rendered.contains("bootstrap failed"), "{rendered}");
        assert!(!rendered.contains("not attached"), "{rendered}");
    }

    #[test]
    fn an_escalated_handoff_stays_visible_after_it_worked() {
        let supervision = ColocatedSupervision {
            last_retirement: Some(ExecutorRetirementSummary {
                pid: 12184,
                origin: RetirementOrigin::PriorRunner,
                escalated_to: Some(RetirementPhase::Force),
                graceful_ms: 7_000,
                force_ms: Some(30),
                total_ms: 7_030,
                outcome: RetirementOutcome::Retired,
                completed_at_unix_ms: NOW - 60_000,
                workload: Default::default(),
            }),
            ..Default::default()
        };
        let rendered = render_colocated_supervision(None, &supervision, true, NOW);
        assert!(
            rendered.contains("Last handoff: retired pid 12184 (left running by a previous runner); escalated to SIGKILL after a 7.0s graceful window (1m ago)"),
            "{rendered}"
        );
    }

    /// A retirement whose workload outlived it must not read as a clean
    /// handoff, and each survivor is named with everything needed to act on
    /// it: which process, which cell, since when, and when it is looked at next.
    #[test]
    fn workload_survivors_are_named_with_their_cell_and_next_look() {
        let supervision = ColocatedSupervision {
            last_retirement: Some(ExecutorRetirementSummary {
                pid: 12184,
                origin: RetirementOrigin::OwnChild,
                escalated_to: Some(RetirementPhase::Force),
                graceful_ms: 7_000,
                force_ms: Some(3_000),
                total_ms: 10_000,
                outcome: RetirementOutcome::Stuck {
                    phase: RetirementPhase::Verify,
                },
                completed_at_unix_ms: NOW - 120_000,
                workload: cairn_common::executor_protocol::RetirementWorkload {
                    reaped: 2,
                    surviving_pids: vec![70565],
                    held_cell_locks: 1,
                    ..Default::default()
                },
            }),
            workload_survivors: vec![WorkloadSurvivor {
                pid: 70565,
                start_token: "1760400000:12".into(),
                leads_group: Some(70565),
                kind: cairn_common::executor_generation::WorkloadKind::Command,
                cell_authority: "/home/build-slots/cairn/.authority/slot-1310".into(),
                cell_epoch: 4,
                cell_lock_held: true,
                first_seen_unix_ms: NOW - 120_000,
                last_verified_unix_ms: NOW - 15_000,
                next_verification_unix_ms: NOW + 45_000,
                kill_attempts: 3,
                unsignalled_because: None,
                group_anchors: Vec::new(),
                retired_executor_pid: Some(12184),
                ownership_unconfirmed: None,
                successor_gated: true,
            }],
            ..Default::default()
        };
        let rendered = render_colocated_supervision(None, &supervision, true, NOW);
        assert!(
            rendered.contains("pid 12184 (this runner's executor) is gone, but verification failed: workload pids [70565] are still running; 1 cell lock is still held; SIGKILLed 2 workload processes it had left running"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- Survivor: pid 70565 (command, start token `1760400000:12`) outlived its executor's retirement at verification; cell `/home/build-slots/cairn/.authority/slot-1310`, lock still held; first seen 2m ago; 3 verified SIGKILLs sent; next re-verification in 45.0s"),
            "{rendered}"
        );
    }

    /// Unknown workload is a verification failure in its own right, and each
    /// cell whose workload is unknown is named with why and when it is looked
    /// at next.
    #[test]
    fn unknown_workload_is_named_as_a_failure_with_each_unresolved_cell() {
        let supervision = ColocatedSupervision {
            last_retirement: Some(ExecutorRetirementSummary {
                pid: 12184,
                origin: RetirementOrigin::PriorRunner,
                escalated_to: None,
                graceful_ms: 0,
                force_ms: None,
                total_ms: 40,
                outcome: RetirementOutcome::Stuck {
                    phase: RetirementPhase::Verify,
                },
                completed_at_unix_ms: NOW - 60_000,
                workload: cairn_common::executor_protocol::RetirementWorkload {
                    unreadable_cells: 1,
                    ..Default::default()
                },
            }),
            unresolved_cells: vec![cairn_common::executor_protocol::UnresolvedCell {
                cell_authority: "/home/build-slots/cairn/.authority/slot-7".into(),
                reason: "its state still does not decode: unparseable persistent slot state".into(),
                retired_executor_pid: Some(12184),
                boot_id: Some("boot".into()),
                first_seen_unix_ms: NOW - 60_000,
                last_verified_unix_ms: NOW,
                next_verification_unix_ms: NOW + 60_000,
            }],
            ..Default::default()
        };
        let rendered = render_colocated_supervision(None, &supervision, true, NOW);
        assert!(
            rendered.contains("pid 12184 (left running by a previous runner) is gone, but verification failed: 1 cell state file could not be decoded, so what ran there is unknown"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- Unresolved cell: `/home/build-slots/cairn/.authority/slot-7` (retired executor pid 12184): what ran there is unknown since 1m ago, because its state still does not decode: unparseable persistent slot state; it clears on its own at the next boot of this host; next re-verification in 60.0s"),
            "{rendered}"
        );
    }

    #[test]
    fn a_quiet_supervisor_renders_nothing() {
        assert_eq!(
            render_colocated_supervision(None, &ColocatedSupervision::default(), true, NOW),
            ""
        );
    }
}
