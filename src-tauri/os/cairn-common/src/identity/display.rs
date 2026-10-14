//! One render-time projection of a [`PrincipalRef`] into what a person reads.
//!
//! Stored attribution is immutable ids — a machine IS its `device_id`, an agent
//! IS its node home URI — and nothing here changes a stored byte. Aliases
//! resolve at render time from registries the backend can see, and travel
//! ALONGSIDE the raw ref, so a surface renders a name without ever mapping one
//! itself.
//!
//! This module is the only place the per-kind rules live. The desktop timelines
//! and the `cairn://` resources both consume what it produces; neither prettifies
//! a principal on its own.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{Address, AppearanceSnapshot, PrincipalRef};
use crate::uri::{parse_uri, CairnResource};

/// What a surface shows when attribution is genuinely absent — an issue created
/// before authorship was recorded at all. An id that merely failed to resolve is
/// never this: it renders as itself.
pub const UNATTRIBUTED: &str = "Unattributed";

/// Identifiers longer than this are shortened in `label`. `detail` always keeps
/// the full form, so shortening costs a reader nothing.
const ELIDE_ABOVE: usize = 12;
const ELIDE_TO: usize = 8;

/// What one principal looks like to a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrincipalDisplay {
    /// The readable name a surface shows: an alias when one resolved, an honest
    /// (possibly shortened) rendering of the id when none did.
    pub label: String,
    /// The full canonical identity `label` stands for — the tooltip, and the
    /// form a reader can act on. Never elided, never a guess.
    pub detail: String,
}

impl PrincipalDisplay {
    /// One line for a surface with no tooltip to demote `detail` into — the
    /// `cairn://` resource renderings. The parenthetical is dropped when
    /// `detail` already opens with what `label` says, so a label that is a
    /// prefix or a shortening of the canonical form is not printed twice.
    pub fn inline(&self) -> String {
        let stem = self.label.trim_end_matches('…');
        if !stem.is_empty() && self.detail.starts_with(stem) {
            self.detail.clone()
        } else {
            format!("{} ({})", self.label, self.detail)
        }
    }
}

/// The alias registries one display pass resolves against, read once and held
/// for the pass.
///
/// Only names this installation can PROVE belong here: its own device row, and
/// the executors it enrolled. Authorship replicates to teammates, so a device id
/// this machine has never met stays unresolved and renders as itself — a wrong
/// name is worse than a raw id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrincipalAliases {
    device_names: HashMap<String, String>,
}

impl PrincipalAliases {
    /// Record what a device is called. A later call for the same id wins, which
    /// is how this installation's own presence name takes precedence over an
    /// enrollment name for the same machine.
    pub fn with_device(mut self, device_id: impl Into<String>, name: impl Into<String>) -> Self {
        let name = name.into();
        if !name.trim().is_empty() {
            self.device_names.insert(device_id.into(), name);
        }
        self
    }

    /// How `principal` reads, given what this pass can resolve.
    ///
    /// `appearance` is the evidence recorded with this same principal, when the
    /// caller kept it. It carries the only alias an external correspondent ever
    /// has; every other kind resolves without it.
    pub fn display(
        &self,
        principal: Option<&PrincipalRef>,
        appearance: Option<&AppearanceSnapshot>,
    ) -> PrincipalDisplay {
        let Some(principal) = principal else {
            return PrincipalDisplay {
                label: UNATTRIBUTED.to_string(),
                detail: UNATTRIBUTED.to_string(),
            };
        };
        match principal {
            // A person is their subject. The issuer that vouched for them, and
            // the organization they hold it through, are provenance rather than
            // name, so they ride in `detail`.
            PrincipalRef::Human {
                issuer,
                subject,
                organization,
            } => PrincipalDisplay {
                label: subject.clone(),
                detail: match organization {
                    Some(organization) => format!("{subject} ({issuer}, {organization})"),
                    None => format!("{subject} ({issuer})"),
                },
            },
            PrincipalRef::Agent { node, .. } => PrincipalDisplay {
                label: agent_label(node).unwrap_or_else(|| node.clone()),
                detail: node.clone(),
            },
            PrincipalRef::Machine { device_id } => PrincipalDisplay {
                label: self
                    .device_names
                    .get(device_id)
                    .cloned()
                    .unwrap_or_else(|| elide(device_id)),
                detail: device_id.clone(),
            },
            PrincipalRef::External {
                provider,
                namespace,
                id,
            } => PrincipalDisplay {
                label: observed_alias(principal, appearance)
                    .unwrap_or_else(|| format!("{provider}:{id}")),
                detail: format!("{provider}:{namespace}/{id}"),
            },
        }
    }
}

/// A long opaque id, shortened for a glance. Short ids — a chosen name, a
/// hostname — are already readable and survive whole.
fn elide(id: &str) -> String {
    if id.chars().count() <= ELIDE_ABOVE {
        return id.to_string();
    }
    let head: String = id.chars().take(ELIDE_TO).collect();
    format!("{head}…")
}

/// The alias a channel observed for this correspondent, when the evidence
/// recorded with this very principal carries one.
///
/// An alias is only usable as a name when the snapshot is about the same
/// principal AND names the same sender: an observed alias belongs to the address
/// it was observed at, not to whatever principal a caller pairs it with.
fn observed_alias(
    principal: &PrincipalRef,
    appearance: Option<&AppearanceSnapshot>,
) -> Option<String> {
    let appearance = appearance?;
    if appearance.principal() != principal {
        return None;
    }
    let PrincipalRef::External { provider, id, .. } = principal else {
        return None;
    };
    match &appearance.evidence().address {
        Address::Channel {
            provider: observed_provider,
            sender,
            observed_alias,
            ..
        } if observed_provider == provider && sender == id => observed_alias
            .clone()
            .filter(|alias| !alias.trim().is_empty()),
        _ => None,
    }
}

/// An agent's home URI as a person addresses it — the same shape the app's own
/// resource labels use, so a name in a timeline matches the row it points at.
/// `None` for anything that does not parse as a home, which then renders raw.
fn agent_label(node: &str) -> Option<String> {
    match parse_uri(node)? {
        CairnResource::Node {
            project,
            number,
            node_id,
            ..
        } => Some(format!("{}/{number} / {node_id}", project.to_lowercase())),
        CairnResource::Task {
            project,
            number,
            node_id,
            task_name,
            ..
        } => Some(format!(
            "{}/{number} / {node_id} / {task_name}",
            project.to_lowercase()
        )),
        // A thread has exactly one identifier: its name.
        CairnResource::Thread {
            project,
            name,
            path,
        } => {
            let thread = format!("{}/{name}", project.to_lowercase());
            match path.as_slice() {
                [task, task_name] if task == "task" => Some(format!("{thread} / {task_name}")),
                _ => Some(thread),
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{
        AppearanceEvidence, AppearanceTransport, VerificationMethod, VerificationRecord,
        VerificationStatus, VerificationStrength,
    };

    const DEVICE: &str = "77bcd7b1-b309-45c2-936b-df0fa904379c";

    fn aliases() -> PrincipalAliases {
        PrincipalAliases::default().with_device(DEVICE, "studio (macos)")
    }

    fn external() -> PrincipalRef {
        PrincipalRef::External {
            provider: "discord".into(),
            namespace: "guild-3".into(),
            id: "sender-9".into(),
        }
    }

    fn channel_appearance(alias: Option<&str>, sender: &str) -> AppearanceSnapshot {
        let verification = VerificationRecord::new(
            VerificationMethod::ChannelAllowlist,
            VerificationStatus::Verified,
            None,
            None,
            None,
            None,
            VerificationStrength::new("allowlisted").unwrap(),
            10,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::ChannelReply,
            Address::Channel {
                provider: "discord".into(),
                conversation: "guild-3".into(),
                sender: sender.into(),
                observed_alias: alias.map(str::to_owned),
            },
            verification,
            11,
            None,
        )
        .unwrap();
        AppearanceSnapshot::new(external(), evidence, Vec::new(), None).unwrap()
    }

    #[test]
    fn absent_attribution_is_the_only_unattributed() {
        let display = aliases().display(None, None);
        assert_eq!(display.label, UNATTRIBUTED);
        assert_eq!(display.detail, UNATTRIBUTED);
        assert_eq!(display.inline(), UNATTRIBUTED);
    }

    #[test]
    fn a_known_device_reads_as_its_name_and_keeps_its_id() {
        let display = aliases().display(
            Some(&PrincipalRef::Machine {
                device_id: DEVICE.into(),
            }),
            None,
        );
        assert_eq!(display.label, "studio (macos)");
        assert_eq!(display.detail, DEVICE);
        assert_eq!(display.inline(), format!("studio (macos) ({DEVICE})"));
    }

    // A teammate's machine cannot resolve the creator's device, and must not
    // invent a name for it. The id is shortened for the eye and kept whole for
    // the record.
    #[test]
    fn an_unknown_device_is_elided_never_renamed() {
        let foreign = "c0ffee00-dead-4bee-9999-000000000001";
        let display = PrincipalAliases::default().display(
            Some(&PrincipalRef::Machine {
                device_id: foreign.into(),
            }),
            None,
        );
        assert_eq!(display.label, "c0ffee00…");
        assert_eq!(display.detail, foreign);
        assert_eq!(display.inline(), foreign);
    }

    #[test]
    fn a_short_device_id_survives_whole() {
        let display = PrincipalAliases::default().display(
            Some(&PrincipalRef::Machine {
                device_id: "studio-mac".into(),
            }),
            None,
        );
        assert_eq!(display.label, "studio-mac");
        assert_eq!(display.inline(), "studio-mac");
    }

    #[test]
    fn a_person_is_their_subject_with_provenance_demoted() {
        let display = aliases().display(
            Some(&PrincipalRef::Human {
                issuer: "https://identity.example".into(),
                subject: "user-42".into(),
                organization: Some("org-1".into()),
            }),
            None,
        );
        assert_eq!(display.label, "user-42");
        assert_eq!(display.detail, "user-42 (https://identity.example, org-1)");
        assert_eq!(
            display.inline(),
            "user-42 (https://identity.example, org-1)"
        );

        let without_org = aliases().display(
            Some(&PrincipalRef::Human {
                issuer: "https://identity.example".into(),
                subject: "user-42".into(),
                organization: None,
            }),
            None,
        );
        assert_eq!(without_org.detail, "user-42 (https://identity.example)");
    }

    #[test]
    fn agent_homes_read_as_the_rows_they_address() {
        for (node, label) in [
            ("cairn://p/TEST/12/3/builder", "test/12 / builder"),
            (
                "cairn://p/TEST/12/3/builder/task/explore",
                "test/12 / builder / explore",
            ),
            ("cairn://p/TEST/identity", "test/identity"),
            (
                "cairn://p/TEST/identity/task/probe",
                "test/identity / probe",
            ),
        ] {
            let display = aliases().display(
                Some(&PrincipalRef::Agent {
                    node: node.into(),
                    run_id: Some("run-7".into()),
                }),
                None,
            );
            assert_eq!(display.label, label, "for {node}");
            assert_eq!(display.detail, node);
            assert_eq!(display.inline(), format!("{label} ({node})"));
        }
    }

    #[test]
    fn an_unreadable_agent_home_renders_raw() {
        let display = aliases().display(
            Some(&PrincipalRef::Agent {
                node: "not-a-cairn-uri".into(),
                run_id: None,
            }),
            None,
        );
        assert_eq!(display.label, "not-a-cairn-uri");
        assert_eq!(display.inline(), "not-a-cairn-uri");
    }

    #[test]
    fn an_external_correspondent_uses_the_alias_its_channel_observed() {
        let named = aliases().display(
            Some(&external()),
            Some(&channel_appearance(Some("mitch"), "sender-9")),
        );
        assert_eq!(named.label, "mitch");
        assert_eq!(named.detail, "discord:guild-3/sender-9");

        let unnamed = aliases().display(Some(&external()), None);
        assert_eq!(unnamed.label, "discord:sender-9");
        assert_eq!(unnamed.detail, "discord:guild-3/sender-9");
    }

    // An alias belongs to the address it was observed at. Evidence about some
    // other sender is not a name for this one.
    #[test]
    fn an_alias_observed_for_another_sender_is_not_borrowed() {
        let display = aliases().display(
            Some(&external()),
            Some(&channel_appearance(Some("someone-else"), "sender-11")),
        );
        assert_eq!(display.label, "discord:sender-9");
    }
}
