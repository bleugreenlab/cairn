//! Whether a nested Seatbelt profile can be applied in this process: the one
//! definition every test guard and the lane report consult.
//!
//! A test that applies a profile, itself or through a product spawn that wraps
//! its child, needs this process to apply one inside whatever already confines
//! it. Only this process can answer that, by applying a fence and writing inside
//! it, so that measurement decides. A process already inside a profile fails it
//! at `sandbox_apply`, so the one answer covers both "is this process confined"
//! and "can it nest".
//!
//! The executor asks the same physical question from its own side when it
//! measures a check lane ([`crate::nested`]), and hands its record to every
//! command it spawns in [`LANE_NESTED_SANDBOX_ENV`]. Here that record is a
//! cross-check, compared and never obeyed: a disagreement is printed in the
//! lane's output and written to the skip log, and the in-process measurement
//! still decides. A record is compared only when this process was spawned in
//! the very outer profile the record measured; one from another context (a
//! fenced batch, an unconfined spawn) is reported as not comparable.
//!
//! The `CAIRN_SANDBOXED` marker is deliberately not read. It records a fence
//! decision, and a lane under the credential floor is kernel-confined with the
//! marker unset.

use crate::{Confinement, NestedSandboxWriteProbe};
use cairn_common::executor_protocol::{NestedSandboxProbeContext, SandboxCapabilityEvidence};
use serde_json::Value;
use std::sync::OnceLock;

/// The environment variable the executor hands its lane record down in.
pub const LANE_NESTED_SANDBOX_ENV: &str = "CAIRN_LANE_NESTED_SANDBOX";

/// The skip-log observation kind a cross-check disagreement is written under.
pub const NESTED_SANDBOX_DISAGREEMENT: &str = "nested-sandbox-cross-check-disagreed";

/// The executor's lane measurement as handed to one spawn: the answer, the
/// context it was measured in, and the outer profile this spawn was actually
/// wrapped in, which is what decides whether the two can be compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneNestedSandboxRecord {
    pub available: bool,
    pub detail: String,
    pub context: Option<NestedSandboxProbeContext>,
    /// SHA-256 of the profile the executor wrapped this spawn in; `None` when
    /// it applied nothing.
    pub spawn_outer_profile_sha256: Option<String>,
}

impl LaneNestedSandboxRecord {
    pub fn for_spawn(evidence: &SandboxCapabilityEvidence, spawn: &Confinement) -> Self {
        Self {
            available: evidence.nested_sandbox_write_available,
            detail: evidence.nested_sandbox_write_detail.clone(),
            context: evidence.nested_sandbox_write_context.clone(),
            spawn_outer_profile_sha256: spawn
                .seatbelt_profile()
                .as_deref()
                .map(crate::nested::sha256),
        }
    }

    /// The compact JSON a spawn's environment carries.
    pub fn to_env_value(&self) -> String {
        serde_json::json!({
            "available": self.available,
            "detail": self.detail,
            "context": self.context,
            "spawnOuterProfileSha256": self.spawn_outer_profile_sha256,
        })
        .to_string()
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let value: Value =
            serde_json::from_str(text).map_err(|error| format!("not JSON: {error}"))?;
        let available = value
            .get("available")
            .and_then(Value::as_bool)
            .ok_or("no boolean `available`")?;
        let detail = value
            .get("detail")
            .and_then(Value::as_str)
            .ok_or("no string `detail`")?
            .to_string();
        let context = match value.get("context") {
            None | Some(Value::Null) => None,
            Some(context) => Some(
                serde_json::from_value(context.clone())
                    .map_err(|error| format!("unreadable `context`: {error}"))?,
            ),
        };
        let spawn_outer_profile_sha256 = match value.get("spawnOuterProfileSha256") {
            None | Some(Value::Null) => None,
            Some(Value::String(sha)) => Some(sha.clone()),
            Some(_) => return Err("`spawnOuterProfileSha256` is not a string".into()),
        };
        Ok(Self {
            available,
            detail,
            context,
            spawn_outer_profile_sha256,
        })
    }

    /// Why this record says nothing about the process it was handed to, if it
    /// says nothing: it describes a lane wrapped in some profile, and is a fact
    /// about this process only when this process was wrapped in that profile.
    fn incomparable(&self) -> Option<String> {
        let Some(context) = &self.context else {
            return Some("the record names no context it was measured in".into());
        };
        if context.fingerprint.is_none() {
            return Some("the record has no fingerprint: an input to it could not be read".into());
        }
        if context.outer_profile_sha256 != self.spawn_outer_profile_sha256 {
            return Some(format!(
                "this process was spawned under outer profile {}, and the record measured a lane under {}",
                short_sha(self.spawn_outer_profile_sha256.as_deref()),
                short_sha(context.outer_profile_sha256.as_deref()),
            ));
        }
        None
    }
}

fn short_sha(sha: Option<&str>) -> String {
    sha.map(|sha| sha.chars().take(12).collect())
        .unwrap_or_else(|| "none (nothing applied)".into())
}

/// How the executor's handed-down record compares with this process's own
/// measurement. Reported, never deciding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrossCheck {
    Agrees(LaneNestedSandboxRecord),
    Disagrees(LaneNestedSandboxRecord),
    NotComparable {
        record: LaneNestedSandboxRecord,
        why: String,
    },
    /// Nothing was handed down: a developer shell, or a spawn not made by an
    /// executor.
    NoRecord,
    Unreadable(String),
}

/// The one answer to "can a nested Seatbelt profile be applied here".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NestedSandboxHere {
    /// No Seatbelt on this platform: nothing nests, so nothing declines.
    NotApplicable,
    /// Measured in this process by applying a fence and writing inside it.
    Measured {
        available: bool,
        detail: String,
        cross_check: CrossCheck,
    },
}

impl NestedSandboxHere {
    pub fn available(&self) -> bool {
        matches!(
            self,
            Self::Measured {
                available: true,
                ..
            }
        )
    }

    /// Whether a test that needs a nested profile must skip here.
    pub fn declines(&self) -> bool {
        matches!(
            self,
            Self::Measured {
                available: false,
                ..
            }
        )
    }

    pub fn detail(&self) -> String {
        match self {
            Self::NotApplicable => format!("not applicable on {}", std::env::consts::OS),
            Self::Measured { detail, .. } => detail.clone(),
        }
    }

    /// Both answers, when the executor's comparable record disagrees with this
    /// process's measurement.
    pub fn disagreement(&self) -> Option<String> {
        let Self::Measured {
            available,
            detail,
            cross_check: CrossCheck::Disagrees(record),
        } = self
        else {
            return None;
        };
        Some(format!(
            "in-process {} ({detail}), executor record {} ({})",
            verdict(*available),
            verdict(record.available),
            record.detail
        ))
    }

    /// The lane report's line: the answer, what decided it, and how the
    /// executor's record compares.
    pub fn report(&self, now_ms: u64) -> String {
        let Self::Measured {
            available,
            detail,
            cross_check,
        } = self
        else {
            return self.detail();
        };
        let cross = match cross_check {
            CrossCheck::Agrees(record) => format!(
                "executor record {}{} — agrees",
                verdict(record.available),
                measured_ago(record, now_ms)
            ),
            CrossCheck::Disagrees(record) => format!(
                "executor record DISAGREES (in-process {}, record {}{}: {})",
                verdict(*available),
                verdict(record.available),
                measured_ago(record, now_ms),
                record.detail
            ),
            CrossCheck::NotComparable { record, why } => format!(
                "executor record {}{} not comparable: {why}",
                verdict(record.available),
                measured_ago(record, now_ms)
            ),
            CrossCheck::NoRecord => "no record handed down".into(),
            CrossCheck::Unreadable(why) => format!("executor record unreadable: {why}"),
        };
        format!(
            "{}: {detail}; source: probe (this process); {cross}",
            verdict(*available)
        )
    }
}

fn verdict(available: bool) -> &'static str {
    if available {
        "AVAILABLE"
    } else {
        "UNAVAILABLE"
    }
}

fn measured_ago(record: &LaneNestedSandboxRecord, now_ms: u64) -> String {
    record
        .context
        .as_ref()
        .map(|context| {
            let seconds = now_ms.saturating_sub(context.measured_at_unix_ms) / 1000;
            let age = match seconds {
                s if s < 60 => format!("{s}s"),
                s if s < 3600 => format!("{}m", s / 60),
                s if s < 86_400 => format!("{}h", s / 3600),
                s => format!("{}d", s / 86_400),
            };
            format!(" measured {age} ago")
        })
        .unwrap_or_default()
}

/// This process's measurement, with the handed-down record set beside it.
/// Only macOS measures; elsewhere it is exercised by the tests alone.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn decide(probe: NestedSandboxWriteProbe, handed: Option<&str>) -> NestedSandboxHere {
    let cross_check = match handed {
        None => CrossCheck::NoRecord,
        Some(text) => match LaneNestedSandboxRecord::parse(text) {
            Err(why) => CrossCheck::Unreadable(why),
            Ok(record) => match record.incomparable() {
                Some(why) => CrossCheck::NotComparable { record, why },
                None if record.available == probe.available => CrossCheck::Agrees(record),
                None => CrossCheck::Disagrees(record),
            },
        },
    };
    NestedSandboxHere::Measured {
        available: probe.available,
        detail: probe.detail,
        cross_check,
    }
}

#[cfg(target_os = "macos")]
fn measure_here() -> NestedSandboxHere {
    let handed =
        std::env::var_os(LANE_NESTED_SANDBOX_ENV).map(|value| value.to_string_lossy().into_owned());
    decide(
        crate::macos::probe_nested_sandbox_write_under(&Confinement::Unconfined),
        handed.as_deref(),
    )
}

#[cfg(not(target_os = "macos"))]
fn measure_here() -> NestedSandboxHere {
    NestedSandboxHere::NotApplicable
}

/// Whether this process can apply a nested Seatbelt profile, measured once.
///
/// The first evaluation that finds the executor's record disagreeing prints
/// both answers on stderr, so they land in the lane's output, and writes an
/// observation to the skip log, so the run's skip report names it.
pub fn nested_sandbox_here() -> &'static NestedSandboxHere {
    static HERE: OnceLock<NestedSandboxHere> = OnceLock::new();
    HERE.get_or_init(|| {
        let here = measure_here();
        if let Some(disagreement) = here.disagreement() {
            eprintln!("nested-sandbox cross-check disagreed: {disagreement}");
            cairn_common::skip_ledger::record_observation(
                NESTED_SANDBOX_DISAGREEMENT,
                &disagreement,
            );
        }
        here
    })
}

/// For a test that applies a Seatbelt profile, itself or through a product
/// spawn that wraps its child: whether to skip because this process cannot
/// apply one. Recorded as the declared `nested-sandbox-exec`, never a pass;
/// pass the test's own name as a string literal so the skip census can match
/// its manifest stanza. Never true off macOS.
pub fn skip_without_nested_sandbox(test: &str) -> bool {
    let here = nested_sandbox_here();
    if !here.declines() {
        return false;
    }
    eprintln!(
        "skipping {test}: this process cannot apply a nested Seatbelt profile ({})",
        here.detail()
    );
    cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::SandboxLauncherClass;

    fn probe(available: bool) -> NestedSandboxWriteProbe {
        NestedSandboxWriteProbe {
            available,
            apply_refused: !available,
            detail: if available {
                "an in-bounds temp write succeeded under a nested Seatbelt profile".into()
            } else {
                "nested Seatbelt in-bounds temp write refused (exit Some(71)): sandbox-exec: sandbox_apply: Operation not permitted".into()
            },
        }
    }

    fn record(
        available: bool,
        outer: Option<&str>,
        spawn: Option<&str>,
    ) -> LaneNestedSandboxRecord {
        LaneNestedSandboxRecord {
            available,
            detail: if available {
                "lane nests".into()
            } else {
                "outer profile inherited".into()
            },
            context: Some(NestedSandboxProbeContext {
                launcher: SandboxLauncherClass::Unconfined,
                credential_floor_sha256: outer.map(|_| "cc9f3dca006d".into()),
                outer_profile_sha256: outer.map(str::to_string),
                fingerprint: Some("8638bb2f93da".into()),
                measured_at_unix_ms: 1_000,
            }),
            spawn_outer_profile_sha256: spawn.map(str::to_string),
        }
    }

    fn cross_check(here: &NestedSandboxHere) -> &CrossCheck {
        match here {
            NestedSandboxHere::Measured { cross_check, .. } => cross_check,
            NestedSandboxHere::NotApplicable => panic!("decide always measures"),
        }
    }

    #[test]
    fn a_comparable_record_that_disagrees_is_reported_and_the_process_still_decides() {
        let handed = record(false, Some("bd409cbaa10a"), Some("bd409cbaa10a")).to_env_value();
        let here = decide(probe(true), Some(&handed));
        assert!(
            here.available(),
            "the in-process measurement decides, not the record"
        );
        assert!(!here.declines());
        assert!(matches!(cross_check(&here), CrossCheck::Disagrees(_)));
        let disagreement = here
            .disagreement()
            .expect("a disagreement names both answers");
        assert!(
            disagreement.contains("in-process AVAILABLE")
                && disagreement.contains("executor record UNAVAILABLE"),
            "{disagreement}"
        );
        let report = here.report(61_000);
        assert!(
            report.contains("DISAGREES (in-process AVAILABLE, record UNAVAILABLE measured 1m ago"),
            "{report}"
        );
    }

    #[test]
    fn with_no_record_the_probe_decides_and_says_so() {
        let here = decide(probe(false), None);
        assert!(here.declines());
        assert_eq!(cross_check(&here), &CrossCheck::NoRecord);
        assert_eq!(here.disagreement(), None);
        let report = here.report(0);
        assert!(
            report.starts_with("UNAVAILABLE: nested Seatbelt"),
            "{report}"
        );
        assert!(
            report.contains("source: probe (this process); no record handed down"),
            "{report}"
        );
    }

    #[test]
    fn a_record_from_another_outer_profile_is_not_compared() {
        // The lane record measured the credential floor; this process was a
        // fenced batch, wrapped in a different profile entirely.
        let handed = record(true, Some("bd409cbaa10a"), Some("0fe2c4d1aa77")).to_env_value();
        let here = decide(probe(false), Some(&handed));
        assert!(here.declines(), "the in-process measurement still decides");
        let CrossCheck::NotComparable { why, .. } = cross_check(&here) else {
            panic!("a record from another context is not compared: {here:?}");
        };
        assert!(
            why.contains("0fe2c4d1aa77") && why.contains("bd409cbaa10a"),
            "{why}"
        );
        assert_eq!(
            here.disagreement(),
            None,
            "not comparable is not a disagreement"
        );
        assert!(here
            .report(0)
            .contains("not comparable: this process was spawned under outer profile 0fe2c4d1aa77"));
    }

    #[test]
    fn a_record_without_a_fingerprint_is_not_compared() {
        let mut unfingerprinted = record(true, Some("bd409cbaa10a"), Some("bd409cbaa10a"));
        unfingerprinted.context.as_mut().unwrap().fingerprint = None;
        let here = decide(probe(false), Some(&unfingerprinted.to_env_value()));
        assert!(matches!(
            cross_check(&here),
            CrossCheck::NotComparable { why, .. } if why.contains("no fingerprint")
        ));
    }

    #[test]
    fn a_malformed_record_is_unreadable_and_decides_nothing() {
        for handed in [
            "not json",
            "{\"available\":\"yes\"}",
            "{\"available\":true,\"detail\":\"x\",\"context\":7}",
        ] {
            let here = decide(probe(true), Some(handed));
            assert!(here.available(), "{handed}");
            assert!(
                matches!(cross_check(&here), CrossCheck::Unreadable(_)),
                "{handed}: {here:?}"
            );
            assert!(here.report(0).contains("executor record unreadable"));
        }
    }

    #[test]
    fn a_comparable_record_that_agrees_says_so() {
        let handed = record(false, Some("bd409cbaa10a"), Some("bd409cbaa10a")).to_env_value();
        let here = decide(probe(false), Some(&handed));
        assert!(matches!(cross_check(&here), CrossCheck::Agrees(_)));
        assert_eq!(here.disagreement(), None);
        let report = here.report(7_200_000);
        assert!(
            report.ends_with("executor record UNAVAILABLE measured 1h ago — agrees"),
            "{report}"
        );
    }

    #[test]
    fn an_unconfined_lane_record_compares_with_an_unconfined_spawn() {
        // No profile on either side is the same context, not a missing one.
        let handed = record(true, None, None).to_env_value();
        assert!(matches!(
            cross_check(&decide(probe(true), Some(&handed))),
            CrossCheck::Agrees(_)
        ));
    }

    #[test]
    fn a_record_round_trips_through_the_environment() {
        let original = record(false, Some("bd409cbaa10a"), None);
        assert_eq!(
            LaneNestedSandboxRecord::parse(&original.to_env_value()),
            Ok(original)
        );
    }

    /// The guard skips exactly when the answer declines, so off macOS it never
    /// does: Linux lanes keep running every test it guards.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn off_macos_nothing_nests_and_nothing_declines() {
        assert_eq!(nested_sandbox_here(), &NestedSandboxHere::NotApplicable);
        assert!(!nested_sandbox_here().declines());
        assert!(!nested_sandbox_here().available());
    }
}
