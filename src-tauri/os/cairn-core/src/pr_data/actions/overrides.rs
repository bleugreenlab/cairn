//! The operator's overrides of the landing gates.
//!
//! Two gates refuse a default-branch landing for want of evidence rather than
//! because of evidence: the verdict-existence gate (no complete verdict for an
//! applicable check) and the landing compile gate (no machine could compile the
//! merge result). Either can be overridden, by the operator only, with a
//! reason that is journaled with the merge. Both travel together, because one
//! merge attempt passes both gates: a head landed without its verdicts can
//! still find the compile gate unavailable.
//!
//! A resource write never carries an override. A write carries an agent's
//! session, or a `_cairn_origin` marker any caller can set, and neither is a
//! person's authority. The overrides are reached only through the runner's
//! `merge_pr_as_operator`, which checks the caller's operator credential before
//! it reads anything.

use super::landing_gate::LandingGateSkip;
use super::verdict_gate::VerdictWaiver;

/// What the operator overrides on one merge. The default overrides nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorOverrides {
    pub verdict_waiver: Option<VerdictWaiver>,
    pub landing_gate_skip: Option<LandingGateSkip>,
}

/// The payload keys a resource write is refused for carrying.
const OVERRIDE_KEYS: &[&str] = &["waiveVerdicts", "skipLandingGate", "landingGateSkip"];

fn reason(value: &serde_json::Value, what: &str) -> Result<String, String> {
    match value.as_str().map(str::trim) {
        Some(reason) if !reason.is_empty() => Ok(reason.to_string()),
        _ => Err(format!(
            "{what} must be a non-empty string saying why it is safe to land without it"
        )),
    }
}

/// Read the operator's overrides from `merge_pr_as_operator`'s arguments. Each
/// is the operator's reason; at least one must be given, since an override of
/// nothing is the ordinary `merge_pr`.
pub fn parse_operator_overrides(
    waive_verdicts: Option<&serde_json::Value>,
    skip_landing_gate: Option<&serde_json::Value>,
) -> Result<OperatorOverrides, String> {
    let overrides = OperatorOverrides {
        verdict_waiver: waive_verdicts
            .map(|value| reason(value, "waiveVerdicts"))
            .transpose()?
            .map(|reason| VerdictWaiver { reason }),
        landing_gate_skip: skip_landing_gate
            .map(|value| reason(value, "skipLandingGate"))
            .transpose()?
            .map(|reason| LandingGateSkip { reason }),
    };
    if overrides == OperatorOverrides::default() {
        return Err(
            "merge_pr_as_operator overrides nothing: give `waiveVerdicts` and/or \
             `skipLandingGate`, each with the reason; a merge without an override is `merge_pr`"
                .to_string(),
        );
    }
    Ok(overrides)
}

/// Refuse an override key on a `cairn://` write, whatever its value: the key's
/// presence is the attempt. Called by the artifact merge surface, whose payload
/// keys are open; the node and thread-branch surfaces declare closed key sets,
/// so the write gate refuses these keys there before dispatch.
pub fn refuse_operator_overrides_on_write(payload: &serde_json::Value) -> Result<(), String> {
    let Some(key) = OVERRIDE_KEYS
        .iter()
        .find(|key| payload.get(**key).is_some())
    else {
        return Ok(());
    };
    Err(format!(
        "payload.{key} is not accepted on a resource write: overriding a merge gate is a \
         person's act, and a write carries an agent's session or a `_cairn_origin` marker, never \
         a person's authority. The operator lands without a gate from the desktop app's PR \
         view, or as a signed-in owner or admin through the `merge_pr_as_operator` command."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn each_override_must_carry_a_reason() {
        assert_eq!(
            parse_operator_overrides(Some(&json!("fleet down, ran it by hand")), None),
            Ok(OperatorOverrides {
                verdict_waiver: Some(VerdictWaiver {
                    reason: "fleet down, ran it by hand".into()
                }),
                landing_gate_skip: None,
            })
        );
        assert_eq!(
            parse_operator_overrides(None, Some(&json!("compiled by hand"))),
            Ok(OperatorOverrides {
                verdict_waiver: None,
                landing_gate_skip: Some(LandingGateSkip {
                    reason: "compiled by hand".into()
                }),
            })
        );
        for bad in [json!("  "), json!(null), json!(true)] {
            assert!(parse_operator_overrides(Some(&bad), None).is_err(), "{bad}");
            assert!(parse_operator_overrides(None, Some(&bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn an_override_of_nothing_is_refused() {
        let refusal = parse_operator_overrides(None, None).unwrap_err();
        assert!(refusal.contains("merge_pr"), "{refusal}");
    }

    /// Any value, even `null` or `false`, is refused on a write: the key's
    /// presence is the attempt, and a write never carries the authority for it.
    #[test]
    fn a_write_never_carries_an_override() {
        assert_eq!(
            refuse_operator_overrides_on_write(&json!({ "action": "merge" })),
            Ok(())
        );
        for key in OVERRIDE_KEYS {
            for value in [json!("fleet down"), serde_json::Value::Null, json!(false)] {
                let mut payload = json!({ "action": "merge" });
                payload[*key] = value;
                let refusal = refuse_operator_overrides_on_write(&payload).unwrap_err();
                assert!(
                    refusal.contains(&format!("payload.{key} is not accepted"))
                        && refusal.contains("merge_pr_as_operator"),
                    "{refusal}"
                );
            }
        }
    }
}
