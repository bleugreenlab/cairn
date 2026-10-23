//! Whether a check lane on this machine can apply a Seatbelt profile of its own.
//!
//! macOS will not apply a profile inside a process that already has one (see
//! `macos::current_process_is_confined`). The executor wraps a check lane's
//! command before it starts ([`Confinement::for_branch_code`]), so the only
//! honest measurement is the lane's own path: wrap a command exactly as a lane
//! command is wrapped, have it apply a nested profile the way a sandbox test
//! does, and see whether the in-bounds write lands. Probing from the executor
//! process instead measures a context no lane runs in, which is how a machine
//! whose every lane exited 71 advertised itself capable (cairn/5316).
//!
//! A measurement is reused only while its fingerprint holds: the executor's
//! launcher class, the credential floor the lane carries, and the outer
//! profile's bytes. Any of them moving re-probes, and an input that could not
//! be read means no reuse at all.

use crate::{Confinement, NestedSandboxWriteProbe, OperatorCredentialFloor};
use cairn_common::executor_protocol::{
    NestedSandboxProbeContext, SandboxCapabilityEvidence, SandboxLauncherClass,
};
use sha2::{Digest, Sha256};
use std::sync::Mutex;

/// The reason a refusal is given when anything confines the lane's command
/// before it can apply its own profile.
const KERNEL_RULE: &str =
    "outer profile inherited: a confined process cannot apply a second profile";

/// The live inputs that decide a check lane's answer.
#[derive(Debug, Clone)]
struct LaneContext {
    launcher: SandboxLauncherClass,
    confinement: Confinement,
    /// The credential floor's rules when the lane's outer profile carries them.
    credential_floor_rules: Option<String>,
    outer_profile: Option<String>,
    /// Whether every input above was read rather than assumed.
    fully_read: bool,
}

impl LaneContext {
    fn current() -> Self {
        let floor = OperatorCredentialFloor::for_this_machine();
        let confinement = Confinement::for_branch_code_on(&floor);
        let outer_profile = confinement.seatbelt_profile();
        let credential_floor_rules = (matches!(confinement, Confinement::CredentialFloor)
            && outer_profile.is_some())
        .then(|| credential_floor_rules(&floor))
        .flatten();
        let launcher = launcher_class();
        Self {
            launcher,
            confinement,
            credential_floor_rules,
            outer_profile,
            fully_read: launcher != SandboxLauncherClass::Unknown && floor.family_is_listable(),
        }
    }

    fn fingerprint(&self) -> Option<String> {
        self.fully_read.then(|| {
            sha256(&format!(
                "launcher={:?}\u{1f}floor={}\u{1e}outer={}",
                self.launcher,
                self.credential_floor_rules.as_deref().unwrap_or("none"),
                self.outer_profile.as_deref().unwrap_or("none"),
            ))
        })
    }

    /// A refusal, said in terms of what confined the lane's command.
    fn explain(&self, probe: &NestedSandboxWriteProbe) -> String {
        if !probe.apply_refused {
            return probe.detail.clone();
        }
        if self.launcher == SandboxLauncherClass::ConfinedDescendant {
            format!(
                "{KERNEL_RULE} (this executor itself runs under an inherited Seatbelt profile, so every command it spawns does too); {}",
                probe.detail
            )
        } else if self.credential_floor_rules.is_some() {
            format!(
                "{KERNEL_RULE} (this machine holds an operator credential, so its check-lane commands run under the operator credential floor); {}",
                probe.detail
            )
        } else if self.outer_profile.is_some() {
            format!(
                "{KERNEL_RULE} (check-lane commands here are wrapped in a Seatbelt profile); {}",
                probe.detail
            )
        } else {
            probe.detail.clone()
        }
    }
}

/// The floor's Seatbelt rules. Only macOS expresses the floor as a profile, so
/// elsewhere there are none to fingerprint.
#[cfg(target_os = "macos")]
fn credential_floor_rules(floor: &OperatorCredentialFloor) -> Option<String> {
    Some(floor.sbpl_rules())
}

#[cfg(not(target_os = "macos"))]
fn credential_floor_rules(_floor: &OperatorCredentialFloor) -> Option<String> {
    None
}

#[cfg(target_os = "macos")]
fn launcher_class() -> SandboxLauncherClass {
    match crate::macos::process_confinement() {
        Some(true) => SandboxLauncherClass::ConfinedDescendant,
        Some(false) => SandboxLauncherClass::Unconfined,
        None => SandboxLauncherClass::Unknown,
    }
}

#[cfg(not(target_os = "macos"))]
fn launcher_class() -> SandboxLauncherClass {
    SandboxLauncherClass::Unconfined
}

#[cfg(target_os = "macos")]
fn probe_under(outer: &Confinement) -> NestedSandboxWriteProbe {
    crate::macos::probe_nested_sandbox_write_under(outer)
}

#[cfg(not(target_os = "macos"))]
fn probe_under(_outer: &Confinement) -> NestedSandboxWriteProbe {
    crate::nested_probe_not_applicable()
}

fn sha256(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// Whether a check lane on this machine can apply a nested Seatbelt profile,
/// with the context the answer was measured in.
///
/// The inputs are re-read on every call and the probe re-runs whenever their
/// fingerprint differs from the last measurement's, so the executor's liveness
/// pass carries a changed outer profile to the runner on the next heartbeat.
/// One assembly point serves the advertisement and the per-execution record,
/// so the two cannot disagree about what this machine is.
pub fn check_lane_nested_sandbox_evidence() -> SandboxCapabilityEvidence {
    static LAST: Mutex<Option<SandboxCapabilityEvidence>> = Mutex::new(None);
    evidence_for(&LaneContext::current(), &LAST, probe_under, unix_now_ms)
}

fn evidence_for(
    context: &LaneContext,
    last: &Mutex<Option<SandboxCapabilityEvidence>>,
    probe: impl FnOnce(&Confinement) -> NestedSandboxWriteProbe,
    now_ms: impl FnOnce() -> u64,
) -> SandboxCapabilityEvidence {
    let fingerprint = context.fingerprint();
    // Held across the probe so two callers that both see a moved fingerprint
    // measure once rather than racing two probes.
    let mut last = last.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let (Some(fingerprint), Some(previous)) = (&fingerprint, last.as_ref()) {
        let held = previous
            .nested_sandbox_write_context
            .as_ref()
            .and_then(|context| context.fingerprint.as_ref());
        if held == Some(fingerprint) {
            return previous.clone();
        }
    }
    let measured = probe(&context.confinement);
    let evidence = SandboxCapabilityEvidence {
        cairn_sandboxed_marker: std::env::var_os("CAIRN_SANDBOXED").is_some(),
        nested_sandbox_write_available: measured.available,
        nested_sandbox_write_detail: if measured.available {
            measured.detail.clone()
        } else {
            context.explain(&measured)
        },
        nested_sandbox_write_context: Some(NestedSandboxProbeContext {
            launcher: context.launcher,
            credential_floor_sha256: context.credential_floor_rules.as_deref().map(sha256),
            outer_profile_sha256: context.outer_profile.as_deref().map(sha256),
            fingerprint,
            measured_at_unix_ms: now_ms(),
        }),
    };
    *last = Some(evidence.clone());
    evidence
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn context(launcher: SandboxLauncherClass, outer: Option<&str>) -> LaneContext {
        LaneContext {
            launcher,
            confinement: if outer.is_some() {
                Confinement::CredentialFloor
            } else {
                Confinement::Unconfined
            },
            credential_floor_rules: outer.map(|_| "(deny file-read-data ...)".to_string()),
            outer_profile: outer.map(str::to_string),
            fully_read: launcher != SandboxLauncherClass::Unknown,
        }
    }

    fn refused() -> NestedSandboxWriteProbe {
        NestedSandboxWriteProbe {
            available: false,
            apply_refused: true,
            detail: "nested Seatbelt in-bounds temp write refused (exit Some(71)): sandbox-exec: sandbox_apply: Operation not permitted".into(),
        }
    }

    fn written() -> NestedSandboxWriteProbe {
        NestedSandboxWriteProbe {
            available: true,
            apply_refused: false,
            detail: "an in-bounds temp write succeeded under a nested Seatbelt profile".into(),
        }
    }

    /// Measure `context` against `last`, counting how many probes it took.
    fn measure(
        context: &LaneContext,
        last: &Mutex<Option<SandboxCapabilityEvidence>>,
        probes: &Cell<u32>,
        result: NestedSandboxWriteProbe,
    ) -> SandboxCapabilityEvidence {
        evidence_for(
            context,
            last,
            |_| {
                probes.set(probes.get() + 1);
                result
            },
            || 1,
        )
    }

    #[test]
    fn a_measurement_is_reused_only_while_its_fingerprint_holds() {
        let last = Mutex::new(None);
        let probes = Cell::new(0);
        let unconfined = context(SandboxLauncherClass::Unconfined, None);

        let first = measure(&unconfined, &last, &probes, written());
        assert!(first.check_lane_can_nest());
        measure(&unconfined, &last, &probes, refused());
        assert_eq!(probes.get(), 1, "an unchanged context must not re-probe");

        // The outer profile moving under the lane is exactly the change that
        // left an attach-time positive standing while every lane exited 71.
        let floored = context(
            SandboxLauncherClass::Unconfined,
            Some("(version 1) floor v1"),
        );
        let after = measure(&floored, &last, &probes, refused());
        assert_eq!(probes.get(), 2, "a moved outer profile must re-probe");
        assert!(!after.check_lane_can_nest());

        let revised = context(
            SandboxLauncherClass::Unconfined,
            Some("(version 1) floor v2"),
        );
        measure(&revised, &last, &probes, refused());
        assert_eq!(probes.get(), 3, "a revised floor is a new outer profile");

        let descendant = context(
            SandboxLauncherClass::ConfinedDescendant,
            Some("(version 1) floor v2"),
        );
        measure(&descendant, &last, &probes, refused());
        assert_eq!(
            probes.get(),
            4,
            "identical policy text under a different launcher is a different context"
        );
    }

    #[test]
    fn an_input_that_cannot_be_read_re_probes_every_time() {
        let last = Mutex::new(None);
        let probes = Cell::new(0);
        let unknown = context(SandboxLauncherClass::Unknown, None);
        let evidence = measure(&unknown, &last, &probes, written());
        measure(&unknown, &last, &probes, written());
        assert_eq!(probes.get(), 2);
        assert_eq!(
            evidence
                .nested_sandbox_write_context
                .as_ref()
                .and_then(|context| context.fingerprint.as_ref()),
            None,
            "a context with an unread input has no fingerprint to reuse it by"
        );
    }

    #[test]
    fn a_refusal_names_the_kernel_rule_and_what_confined_the_lane() {
        let last = Mutex::new(None);
        let probes = Cell::new(0);
        let floored = context(SandboxLauncherClass::Unconfined, Some("(version 1) floor"));
        let evidence = measure(&floored, &last, &probes, refused());
        assert!(!evidence.nested_sandbox_write_available);
        assert!(
            evidence
                .nested_sandbox_write_detail
                .starts_with(KERNEL_RULE),
            "{}",
            evidence.nested_sandbox_write_detail
        );
        assert!(evidence
            .nested_sandbox_write_detail
            .contains("operator credential floor"));
        let recorded = evidence.nested_sandbox_write_context.unwrap();
        assert_eq!(
            recorded.outer_profile_sha256.as_deref(),
            Some(sha256("(version 1) floor").as_str())
        );
        assert!(recorded.credential_floor_sha256.is_some());

        let descendant = context(SandboxLauncherClass::ConfinedDescendant, None);
        let evidence = measure(&descendant, &last, &probes, refused());
        assert!(evidence
            .nested_sandbox_write_detail
            .contains("this executor itself runs under an inherited Seatbelt profile"));
    }

    #[test]
    fn evidence_without_a_context_does_not_place_a_lane() {
        let mut evidence = measure(
            &context(SandboxLauncherClass::Unconfined, None),
            &Mutex::new(None),
            &Cell::new(0),
            written(),
        );
        assert!(evidence.check_lane_can_nest());
        evidence.nested_sandbox_write_context = None;
        assert!(
            !evidence.check_lane_can_nest(),
            "an executor-process measurement says nothing about a lane"
        );
    }

    /// The real kernel, through the real wrapping: a command spawned under the
    /// credential floor cannot apply a profile of its own. Whether this test
    /// process is itself confined does not change that answer.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_nested_apply_under_the_credential_floor_is_refused_live() {
        let floor = OperatorCredentialFloor::for_this_machine();
        let rules = floor.sbpl_rules();
        if rules.is_empty() {
            eprintln!("no home directory, so the credential floor applies no profile here");
            return;
        }
        let lane = LaneContext {
            launcher: launcher_class(),
            confinement: Confinement::CredentialFloor,
            credential_floor_rules: Some(rules),
            outer_profile: Confinement::CredentialFloor.seatbelt_profile(),
            fully_read: true,
        };
        assert!(lane.outer_profile.is_some());
        let evidence = evidence_for(&lane, &Mutex::new(None), probe_under, || 1);
        assert!(
            !evidence.nested_sandbox_write_available,
            "a nested profile applied under the credential floor: {}",
            evidence.nested_sandbox_write_detail
        );
        assert!(
            evidence
                .nested_sandbox_write_detail
                .starts_with(KERNEL_RULE),
            "{}",
            evidence.nested_sandbox_write_detail
        );
    }

    /// The live context is the one a lane is spawned in: the same
    /// confinement `for_branch_code` picks and the same profile it wraps.
    #[test]
    fn the_live_context_is_the_check_lane_confinement() {
        let lane = LaneContext::current();
        assert_eq!(
            lane.outer_profile,
            Confinement::for_branch_code().seatbelt_profile()
        );
        assert_eq!(
            lane.credential_floor_rules.is_some(),
            matches!(lane.confinement, Confinement::CredentialFloor)
                && lane.outer_profile.is_some()
        );
    }
}
