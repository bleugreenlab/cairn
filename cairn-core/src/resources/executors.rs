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
    ExecutorHealthStatus, ExecutorInspection, Measurement, MeasurementReading, PlacementOutcome,
    PlacementSyncCost, ReservationRationale, MIN_CONFIDENT_RESERVATION_SAMPLES,
};
// Deliberately its own statement rather than folded into the import above. That
// list is edited by whoever touches the reservation rendering; keeping the
// fleet-visibility types on a separate line means two branches working on this
// file do not collide on one line neither of them cares about.
use cairn_common::executor_protocol::{EnrolledRemote, ExecutorCapabilities, RemoteLinkState};

use cairn_common::abnormal_exit::{crash_report_for, AbnormalExit};

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
) -> String {
    if executors.is_empty() && enrolled.is_empty() && enrolling.is_empty() {
        let mut empty = String::from("# Executors\n\nNo executor is attached to this runner.\n\nThe runner supervises a colocated executor named `local`; if nothing is listed here it is not currently attached. Enrolled machines are added with `cairn executor add <user@host>`.\n");
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
    if executors.is_empty() {
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
        out.push_str(&format!("- Link: {}\n", link_summary(executor)));
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
            out.push_str(&format!(
                "- Last seen: {}\n",
                last_seen(remote, captured_at_unix_ms)
            ));
            out.push_str(&format!(
                "- Last attempt: {}\n",
                last_attempt(remote, captured_at_unix_ms)
            ));
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
pub(crate) fn render_enrolled_remote(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    let mut out = format!("# Executor {}\n\n", remote.name);
    out.push_str("## Identity\n");
    out.push_str(&format!("- Name: {}\n", remote.name));
    out.push_str("- Role: enrolled (attached over the executor protocol)\n");
    out.push_str(&format!("- Platform: {} / {}\n\n", remote.os, remote.arch));

    out.push_str("## Link\n");
    out.push_str(&format!("- Status: {}\n", enrolled_link_summary(remote)));
    out.push_str(&format!(
        "- Last seen: {}\n",
        last_seen(remote, captured_at_unix_ms)
    ));
    out.push_str(&format!(
        "- Last attempt: {}\n\n",
        last_attempt(remote, captured_at_unix_ms)
    ));

    out.push_str(match remote.link {
        RemoteLinkState::Unreachable => {
            "This machine is enrolled and did not answer. That is ordinary when it is powered off or this runner is away from the network it lives on; the runner keeps retrying on a backoff and it will attach on its own once the host is reachable.\n"
        }
        RemoteLinkState::AttachFailed => {
            "This machine answered and the runner could not bring an executor up on it. The reason above is the runner's own account of the last attempt; retries continue on a backoff but will keep failing the same way until the cause is fixed.\n"
        }
        RemoteLinkState::Pending => {
            "This machine is enrolled and no attempt has completed since the runner started.\n"
        }
    });
    out.push_str("\nNo load, admission, or occupancy is reported: those are measured on the machine, and this one is not reporting.\n");
    out
}

fn enrolled_link_summary(remote: &EnrolledRemote) -> String {
    match remote.link {
        RemoteLinkState::Unreachable => "unreachable — the host did not answer".to_string(),
        RemoteLinkState::AttachFailed => {
            "attach failed — the host answered and the executor could not be started".to_string()
        }
        RemoteLinkState::Pending => "not yet attempted since the runner started".to_string(),
    }
}

fn last_seen(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    match remote.last_seen_unix_ms {
        Some(seen) => age(captured_at_unix_ms.saturating_sub(seen)),
        None => "never attached since the runner started".to_string(),
    }
}

fn last_attempt(remote: &EnrolledRemote, captured_at_unix_ms: u64) -> String {
    match &remote.last_attempt {
        Some(attempt) => format!(
            "{} — {}",
            age(captured_at_unix_ms.saturating_sub(attempt.attempted_at_unix_ms)),
            attempt.reason
        ),
        None => "none completed yet".to_string(),
    }
}

/// Render one machine in full.
pub(crate) fn render_executor(executor: &ExecutorInspection) -> String {
    let health = &executor.health;
    let capabilities = &health.advertisement.capabilities;
    let mut out = format!("# Executor {}\n\n", executor.name);

    out.push_str("## Identity\n");
    out.push_str(&format!("- Name: {}\n", executor.name));
    out.push_str(&format!(
        "- Role: {}\n",
        if executor.colocated {
            "colocated (supervised inside the runner's own process tree)"
        } else {
            "enrolled (attached over the executor protocol)"
        }
    ));
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
    out.push_str(&format!(
        "- Projects served: {}\n\n",
        list_or(&capabilities.projects_served, "every project")
    ));

    out.push_str("## Link\n");
    out.push_str(&format!("- Status: {}\n", link_summary(executor)));
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
    out.push_str(&format!(
        "- Connection generation: {}\n",
        health.connection_generation
    ));
    out.push_str(&format!(
        "- Draining: {}\n",
        if health.drain_mode {
            "yes (refusing new work)"
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
        "- Volume: {}\n",
        reading(&machine.volume, executor.captured_at_unix_ms, |volume| {
            format!(
                "{} free of {}",
                bytes(volume.free_bytes),
                bytes(volume.total_bytes)
            )
        })
    ));
    out.push_str(&format!(
        "- Storage verdict: {:?} (sweep {:?})\n\n",
        health.disk.status, health.disk.sweep_status
    ));

    out.push_str("## Admission\n");
    let admission = &health.admission;
    out.push_str(&format!(
        "- Concurrency: {} of {} units reserved\n",
        admission.active_reservation.concurrency_units,
        admission
            .concurrency_capacity
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unstated".into())
    ));
    out.push_str(&format!(
        "- Accepted {} / rejected {} / timed out {}\n",
        admission.accepted_count, admission.rejected_count, admission.timed_out_count
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

    out.push_str("## Occupancy\n");
    let occupancy = &executor.occupancy;
    out.push_str(&format!(
        "- Cells: {} ({} checked out, {} idle)\n",
        occupancy.cells.len(),
        health.inventory.checked_out_count,
        health.inventory.idle_count
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

    render_placements(&mut out, executor);

    out.push_str(&format!(
        "Target this machine with `run({{executor:{{name:\"{}\"}}}})`. Renaming it is an operator action: `cairn executor rename {} <new-name>`.\n",
        executor.name, executor.name
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
fn render_placements(out: &mut String, executor: &ExecutorInspection) {
    out.push_str("## Placement\n");
    if executor.recent_placements.is_empty() {
        out.push_str(
            "- No placement decision in the runner's recent window named this machine.\n\n",
        );
        return;
    }
    for decision in &executor.recent_placements {
        let asked = match (&decision.selector, &decision.pinned_executor_id) {
            (_, Some(_)) => "pinned to its execution home".to_string(),
            (Some(selector), None) => selector.describe(),
            (None, None) => "any executor".to_string(),
        };
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
                        coordinate.request_id,
                        coordinate.attempt_id,
                        coordinate.connection_generation
                    ));
                }
                out.push_str(&format!("  - {}\n", selection.observation_reuse.describe()));
            }
            PlacementOutcome::Refused { diagnostic } => {
                out.push_str(&format!("  - Refused: {diagnostic}\n"));
            }
        }
        for rejection in &decision.rejected {
            out.push_str(&format!(
                "  - Passed over {}: {}\n",
                rejection.executor_name,
                rejection.reason.describe()
            ));
        }
    }
    out.push('\n');
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

fn link_summary(executor: &ExecutorInspection) -> String {
    match executor.health.status {
        ExecutorHealthStatus::Online => "online".to_string(),
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

    use super::*;
    use cairn_common::executor_protocol::{
        CpuPressure, ExecutorAdvertisement, ExecutorCapabilities, ExecutorHealthSnapshot,
        ExecutorIdentity, ExecutorSubstrateReport, FleetSnapshot, MachineMemory, MachineVolume,
        RemoteAttachAttempt, ResidentOccupancyEvidence, ResourceReservation, ToolchainDetection,
        ToolchainProbe,
    };

    const CAPTURED_AT: u64 = 1_000_000;

    fn inspection(name: &str, colocated: bool) -> ExecutorInspection {
        let identity = ExecutorIdentity {
            device_id: "device-9f3c1a".into(),
            executor_id: "executor-7b21ce".into(),
            display_name: name.into(),
        };
        ExecutorInspection {
            name: name.into(),
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
                        toolchains: vec!["rust".into(), "bun".into()],
                        projects_served: Vec::new(),
                        disk_budget_bytes: None,
                        memory_budget_bytes: None,
                        toolchain_detection: None,
                    },
                    current_load: 0,
                    warm_roots: Vec::new(),
                    observed_at_unix_ms: CAPTURED_AT - 4_000,
                    liveness_observed_at_unix_ms: Some(CAPTURED_AT - 5_000),
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
                build_skew: None,
            },
            executor_build_id: Some("build-abc".into()),
            occupancy: FleetSnapshot::default(),
            captured_at_unix_ms: CAPTURED_AT,
        }
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
        );
        assert!(rendered.contains("- Toolchains: rust, bun\n"), "{rendered}");
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
        let rendered = render_executors(&[], &[], &[], None, CAPTURED_AT);
        assert!(rendered.contains("No executor is attached"), "{rendered}");
        assert!(rendered.contains("local"), "{rendered}");
    }

    /// A runner that came up cleanly says nothing about restarts. The section
    /// has to stay silent in the ordinary case, or the one time it matters it
    /// reads as boilerplate.
    #[test]
    fn a_clean_runner_says_nothing_about_a_restart() {
        let rendered = render_executors(&[], &[], &[], None, CAPTURED_AT);
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
        let rendered = render_executors(&[], &[], &[], Some(&exit), CAPTURED_AT);
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
            name: name.into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            link,
            last_attempt: Some(RemoteAttachAttempt {
                attempted_at_unix_ms: REMOTE_CAPTURED_AT - 120_000,
                reason: "ssh: connect to host bglab-ub port 22: No route to host".into(),
            }),
            last_seen_unix_ms: Some(REMOTE_CAPTURED_AT - 7_200_000),
        }
    }

    /// The defect this whole record exists for: three enrolled machines stopped
    /// reporting and this resource rendered exactly as though they had never
    /// been enrolled. An unattached machine is listed, named, and explained.
    #[test]
    fn an_enrolled_machine_that_is_not_attached_is_still_listed() {
        let rendered = render_executors(
            &[inspection("local", true)],
            &[enrolled("bglab-ub", RemoteLinkState::Unreachable)],
            &[],
            None,
            REMOTE_CAPTURED_AT,
        );
        assert!(rendered.contains("### bglab-ub"), "{rendered}");
        assert!(
            rendered.contains("Enrolled, not attached (1)"),
            "{rendered}"
        );
        assert!(rendered.contains("unreachable"), "{rendered}");
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
                enrolled("bglab-mac", RemoteLinkState::AttachFailed),
                enrolled("bglab-ub", RemoteLinkState::Unreachable),
            ],
            &[],
            None,
            REMOTE_CAPTURED_AT,
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
        let unreachable = render_enrolled_remote(
            &enrolled("bglab-ub", RemoteLinkState::Unreachable),
            REMOTE_CAPTURED_AT,
        );
        assert!(
            unreachable.contains("the host did not answer"),
            "{unreachable}"
        );
        assert!(
            unreachable.contains("attach on its own once the host is reachable"),
            "{unreachable}"
        );

        let failed = render_enrolled_remote(
            &enrolled("bglab-mac", RemoteLinkState::AttachFailed),
            REMOTE_CAPTURED_AT,
        );
        assert!(failed.contains("the host answered"), "{failed}");
        assert!(failed.contains("until the cause is fixed"), "{failed}");
        assert!(failed.contains("No route to host"), "{failed}");
        assert!(failed.contains("2m ago"), "{failed}");
    }

    /// Nothing on an unattached machine was measured, so nothing about its load
    /// is printed. A zero here would describe an idle machine, not an absent one.
    #[test]
    fn an_unattached_machine_reports_no_load_rather_than_zeroes() {
        let rendered = render_enrolled_remote(
            &enrolled("bglab-ub", RemoteLinkState::Unreachable),
            REMOTE_CAPTURED_AT,
        );
        assert!(
            rendered.contains("No load, admission, or occupancy is reported"),
            "{rendered}"
        );
        assert!(!rendered.contains("0%"), "{rendered}");
        assert!(!rendered.contains("logical cores"), "{rendered}");
    }

    /// A machine the runner has not tried yet is neither reachable nor failing,
    /// and claiming either would be a verdict nothing supports.
    #[test]
    fn a_machine_not_yet_attempted_claims_neither_outcome() {
        let mut remote = enrolled("bglab-win", RemoteLinkState::Pending);
        remote.last_attempt = None;
        remote.last_seen_unix_ms = None;
        let rendered = render_enrolled_remote(&remote, REMOTE_CAPTURED_AT);
        assert!(rendered.contains("not yet attempted"), "{rendered}");
        assert!(rendered.contains("never attached"), "{rendered}");
        assert!(rendered.contains("none completed yet"), "{rendered}");
        assert!(!rendered.contains("did not answer"), "{rendered}");
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
            render_executors(std::slice::from_ref(&executor), &[], &[], None, CAPTURED_AT),
        ] {
            assert!(!rendered.contains("executor-7b21ce"), "{rendered}");
            assert!(!rendered.contains("device-9f3c1a"), "{rendered}");
            assert!(!rendered.contains("/Users/"), "{rendered}");
            assert!(!rendered.contains("credential"), "{rendered}");
        }
        assert!(render_executor(&executor).contains("Resident processes: 2"));
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
            outcome: PlacementOutcome::Selected(Box::new(PlacementSelection {
                executor_name: "bglab-ub".into(),
                executor_id: "executor-7b21ce".into(),
                colocated: false,
                reason: PlacementReason::MeasuredIdle,
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
                executor_name: "local".into(),
                executor_id: "colocated".into(),
                reason: PlacementRejectionReason::TelemetryGap {
                    measurement: MachineMeasurement::Volume,
                    gap: MeasurementGap::SamplingFailed,
                },
            }],
        }];

        let rendered = render_executor(&executor);
        assert!(rendered.contains("## Placement"), "{rendered}");
        assert!(rendered.contains("check-cadence-7"), "{rendered}");
        assert!(rendered.contains("spillEligible"), "{rendered}");
        assert!(rendered.contains("measuredIdle"), "{rendered}");
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
    }

    /// A machine that has taken part in no recent decision says that, rather
    /// than rendering an empty heading an operator has to interpret.
    #[test]
    fn a_machine_with_no_recent_placement_says_so() {
        let rendered = render_executor(&inspection("bglab-win", false));
        assert!(
            rendered.contains("No placement decision in the runner's recent window"),
            "{rendered}"
        );
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
            &[enrolled("bglab-ub", RemoteLinkState::Unreachable)],
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
