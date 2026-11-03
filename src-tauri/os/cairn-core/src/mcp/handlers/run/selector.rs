//! Which selector places a tree-bound run batch, and where it came from.
//!
//! Three things can say where a batch runs, and they rank by how much each is
//! a statement of intent. A call-level `executor` is the caller naming the
//! machine for this batch. A node's placement default is the caller having
//! named it once for all of its work. Scratch affinity is a guess that the
//! machine holding the node scratch is a good place to go. So a call beats a
//! default, a default beats affinity, and affinity is consulted only when
//! neither said anything.
//!
//! Only a call-level selector (and, until cairn/5988, an affinity guess)
//! reaches the shell's `CAIRN_BATCH_EXECUTOR`, which a nested `cairn check run`
//! inherits as a narrowing. A default never does: a check result is identified
//! by what was checked and placed by platform (tenet 1233).

use crate::fleet::placement_default::PlacementDefault;
use cairn_common::executor_protocol::ExecutorSelector;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SelectorSource {
    /// The call's own `executor`.
    Call,
    /// The node's declared placement default.
    NodeDefault,
    /// The machine of a live process holding the node scratch.
    ScratchAffinity,
}

/// The selector a batch is placed by, with its provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct EffectiveSelector {
    pub(super) selector: ExecutorSelector,
    pub(super) source: SelectorSource,
    /// The default itself, when it is the source, for the label and refusals.
    pub(super) default: Option<PlacementDefault>,
}

impl EffectiveSelector {
    /// The declared selector: the call's, else the node's default. `None` when
    /// neither says anything, which is the only case affinity may fill.
    pub(super) fn declared(
        call: Option<&ExecutorSelector>,
        default: Option<PlacementDefault>,
    ) -> Option<Self> {
        if let Some(selector) = call.filter(|selector| !selector.is_empty()) {
            return Some(Self {
                selector: selector.clone(),
                source: SelectorSource::Call,
                default: None,
            });
        }
        let default = default.filter(|default| !default.selector.is_empty())?;
        Some(Self {
            selector: default.selector.clone(),
            source: SelectorSource::NodeDefault,
            default: Some(default),
        })
    }

    pub(super) fn from_affinity(selector: ExecutorSelector) -> Self {
        Self {
            selector,
            source: SelectorSource::ScratchAffinity,
            default: None,
        }
    }

    /// The default that placed this batch, when one did.
    pub(super) fn node_default(&self) -> Option<&PlacementDefault> {
        match self.source {
            SelectorSource::NodeDefault => self.default.as_ref(),
            SelectorSource::Call | SelectorSource::ScratchAffinity => None,
        }
    }
}

/// The two selectors a batch carries, from the one that places it.
///
/// `placement` is what residence and the cell request read, so it is every
/// effective selector. `shell_pin` is what the batch's shells receive as
/// `CAIRN_BATCH_EXECUTOR`, which a nested `cairn check run` inherits as a
/// narrowing, so it is never a node default (tenet 1233). Both call sites read
/// this, so neither can take the other's by mistake.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct BatchSelectors {
    pub(super) placement: Option<ExecutorSelector>,
    pub(super) shell_pin: Option<ExecutorSelector>,
}

impl BatchSelectors {
    pub(super) fn of(effective: Option<&EffectiveSelector>) -> Self {
        let Some(effective) = effective else {
            return Self::default();
        };
        Self {
            placement: Some(effective.selector.clone()),
            shell_pin: match effective.source {
                SelectorSource::NodeDefault => None,
                // The affinity guess reaches the shells as it did before node
                // defaults existed; cairn/5988 takes it off.
                SelectorSource::Call | SelectorSource::ScratchAffinity => {
                    Some(effective.selector.clone())
                }
            },
        }
    }
}

/// Whether a node's placement default is consulted for this batch at all: only
/// an agent's own tree-bound batch. A page call runs on no machine its caller
/// names, and gateway, REPL, response and model-door items run on the host.
pub(super) fn default_applies(page_call: bool, has_process: bool, has_run_context: bool) -> bool {
    !page_call && has_process && has_run_context
}

/// `text` with the note that the fleet cleared the node's default ahead of it.
pub(super) fn with_clear_notice(notice: Option<&str>, text: String) -> String {
    match notice {
        Some(notice) => format!("{notice}\n\n{text}"),
        None => text,
    }
}

/// `detail` with the default's refusal suffix after it, when the default
/// placed the batch that was refused.
pub(super) fn with_default_suffix(default: Option<&PlacementDefault>, detail: String) -> String {
    match default {
        Some(default) => format!("{detail}\n\n{}", default.refusal_suffix()),
        None => detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(name: &str) -> ExecutorSelector {
        ExecutorSelector {
            name: Some(name.to_string()),
            ..ExecutorSelector::default()
        }
    }

    fn default_on(name: &str) -> PlacementDefault {
        PlacementDefault {
            selector: named(name),
            executor_id: Some(format!("exec-{name}")),
            set_by: "operator".into(),
            set_at: 100,
            cleared: None,
        }
    }

    #[test]
    fn a_call_beats_a_default_and_a_default_beats_nothing() {
        let call =
            EffectiveSelector::declared(Some(&named("mac")), Some(default_on("bbs"))).unwrap();
        assert_eq!(call.source, SelectorSource::Call);
        assert_eq!(call.selector, named("mac"));
        assert_eq!(call.node_default(), None);

        let defaulted = EffectiveSelector::declared(None, Some(default_on("bbs"))).unwrap();
        assert_eq!(defaulted.source, SelectorSource::NodeDefault);
        assert_eq!(defaulted.selector, named("bbs"));
        assert_eq!(defaulted.node_default(), Some(&default_on("bbs")));

        // An empty call-level selector says nothing, so the default still holds.
        let empty = EffectiveSelector::declared(
            Some(&ExecutorSelector::default()),
            Some(default_on("bbs")),
        )
        .unwrap();
        assert_eq!(empty.source, SelectorSource::NodeDefault);

        // Nothing declared is the only case affinity may fill.
        assert_eq!(EffectiveSelector::declared(None, None), None);
        assert_eq!(
            EffectiveSelector::from_affinity(named("fedora")).node_default(),
            None
        );
    }

    /// The default places the batch and never reaches the shells' pin, which is
    /// the only route from a run selector to a check lane.
    #[test]
    fn a_default_places_the_batch_but_is_never_the_shells_pin() {
        let defaulted = EffectiveSelector::declared(None, Some(default_on("bbs")));
        assert_eq!(
            BatchSelectors::of(defaulted.as_ref()),
            BatchSelectors {
                placement: Some(named("bbs")),
                shell_pin: None,
            }
        );
        let call = EffectiveSelector::declared(Some(&named("mac")), Some(default_on("bbs")));
        assert_eq!(
            BatchSelectors::of(call.as_ref()),
            BatchSelectors {
                placement: Some(named("mac")),
                shell_pin: Some(named("mac")),
            }
        );
        let affinity = EffectiveSelector::from_affinity(named("fedora"));
        assert_eq!(
            BatchSelectors::of(Some(&affinity)),
            BatchSelectors {
                placement: Some(named("fedora")),
                shell_pin: Some(named("fedora")),
            }
        );
        assert_eq!(BatchSelectors::of(None), BatchSelectors::default());
    }

    #[test]
    fn only_an_agents_own_tree_bound_batch_consults_the_default() {
        assert!(default_applies(false, true, true));
        assert!(
            !default_applies(true, true, true),
            "a page call names no machine"
        );
        assert!(
            !default_applies(false, false, true),
            "host items run on the host"
        );
        assert!(
            !default_applies(false, true, false),
            "an ambient batch has no node"
        );
    }

    #[test]
    fn a_refusal_says_the_default_placed_it_and_a_cleared_default_is_announced_first() {
        let refused = with_default_suffix(Some(&default_on("bbs")), "bbs is drained".into());
        assert!(refused.starts_with("bbs is drained\n\nbbs is this node's placement default"));
        assert_eq!(with_default_suffix(None, "x".into()), "x");
        assert_eq!(
            with_clear_notice(Some("Note: cleared"), "Ran on **a**.".into()),
            "Note: cleared\n\nRan on **a**."
        );
        assert_eq!(with_clear_notice(None, "x".into()), "x");
    }
}
