//! Which in-flight pure-verdict execution a new request may share.
//!
//! Coalescing hands one execution's outcome to every request asking the same
//! question. Content identity ([`CheckResultIdentity`]) settles that the
//! question is the same; it says nothing about where the answer may come from.
//! A request that demands a platform or a machine counts only an answer
//! produced under that demand, so it may join an execution only when every
//! placement that execution's leader can take is one the request would itself
//! accept. Otherwise it leads its own execution beside the first, which is why
//! an execution is addressed by its leader as well as its content.

use std::collections::BTreeSet;

use cairn_common::executor_protocol::{executor_names_match, CellRequest, ExecutorSelector};

use super::RequestIdentity;
use crate::execution::cache::CheckResultIdentity;

/// One in-flight execution: the content it answers and the request leading it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CoalescingKey {
    pub result: CheckResultIdentity,
    pub leader: RequestIdentity,
}

impl CoalescingKey {
    pub(crate) fn new(result: CheckResultIdentity, leader: RequestIdentity) -> Self {
        Self { result, leader }
    }
}

/// The placement clauses a request was submitted with, as coalescing reads
/// them. Every clause is one placement treats as hard, so an execution placed
/// under a weaker set may land somewhere the request would have refused.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PlacementDemand {
    /// Lowercased platform families whose answer counts. Empty counts any.
    verdict_platforms: BTreeSet<String>,
    selector: Option<ExecutorSelector>,
    pinned_executor_id: Option<String>,
    avoided_executor_ids: BTreeSet<String>,
}

impl PlacementDemand {
    pub(crate) fn of(request: &CellRequest) -> Self {
        Self {
            verdict_platforms: request
                .verdict_platforms
                .iter()
                .map(|platform| platform.to_ascii_lowercase())
                .collect(),
            selector: request
                .executor
                .clone()
                .filter(|selector| !selector.is_empty()),
            pinned_executor_id: request.pinned_executor_id.clone(),
            avoided_executor_ids: request.avoided_executor_ids.iter().cloned().collect(),
        }
    }

    /// Whether every placement an execution submitted under `self` may take is
    /// one `wanted` also accepts.
    ///
    /// Judged from the leader's demand, never from where it happened to land:
    /// a subscriber may join before its leader is placed, and a leader that
    /// asked for anywhere is not bound to the platform a stricter subscriber
    /// needs. So a request with no demand joins anything, `os=linux` joins a
    /// leader that also demanded linux, and `os=macos` never joins a leader
    /// that may run on linux.
    pub(crate) fn meets(&self, wanted: &PlacementDemand) -> bool {
        let platforms = wanted.verdict_platforms.is_empty()
            || (!self.verdict_platforms.is_empty()
                && self.verdict_platforms.is_subset(&wanted.verdict_platforms));
        let pinned = wanted.pinned_executor_id.is_none()
            || self.pinned_executor_id == wanted.pinned_executor_id;
        let avoided = wanted
            .avoided_executor_ids
            .is_subset(&self.avoided_executor_ids);
        let selector = match (&self.selector, &wanted.selector) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(held), Some(wanted)) => selector_meets(held, wanted),
        };
        platforms && pinned && avoided && selector
    }
}

/// Whether a machine `held` admits is necessarily one `wanted` admits.
///
/// Conservative where the answer needs the fleet: a leader that named a machine
/// does not meet a demand stated as an `os`, even when that machine runs it.
fn selector_meets(held: &ExecutorSelector, wanted: &ExecutorSelector) -> bool {
    let name = wanted.name.as_deref().is_none_or(|wanted| {
        held.name
            .as_deref()
            .is_some_and(|held| executor_names_match(held, wanted))
    });
    let os = wanted.os.as_deref().is_none_or(|wanted| {
        held.os
            .as_deref()
            .is_some_and(|held| held.eq_ignore_ascii_case(wanted))
    });
    let toolchains = wanted
        .required_toolchains
        .iter()
        .all(|toolchain| held.required_toolchains.contains(toolchain));
    let sandbox =
        !wanted.requires_macos_nested_sandbox_write || held.requires_macos_nested_sandbox_write;
    let containment = wanted.containment.is_none() || held.containment == wanted.containment;
    name && os && toolchains && sandbox && containment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demand(platforms: &[&str], selector: Option<ExecutorSelector>) -> PlacementDemand {
        PlacementDemand {
            verdict_platforms: platforms.iter().map(|p| p.to_string()).collect(),
            selector,
            ..PlacementDemand::default()
        }
    }

    fn os(os: &str) -> Option<ExecutorSelector> {
        Some(ExecutorSelector {
            os: Some(os.into()),
            ..ExecutorSelector::default()
        })
    }

    #[test]
    fn no_demand_is_met_by_any_leader() {
        let none = PlacementDemand::default();
        assert!(none.meets(&none));
        assert!(demand(&["linux"], os("linux")).meets(&none));
        assert!(demand(&["macos", "linux"], None).meets(&none));
    }

    #[test]
    fn a_platform_demand_is_met_only_by_a_leader_confined_to_it() {
        let linux = demand(&["linux"], os("linux"));
        let macos = demand(&["macos"], os("macos"));
        assert!(linux.meets(&linux));
        assert!(
            !linux.meets(&macos),
            "a linux lane never answers a macos demand"
        );
        assert!(!macos.meets(&linux));
        assert!(
            !PlacementDemand::default().meets(&linux),
            "a leader that may land anywhere does not answer a platform demand"
        );
        assert!(
            !demand(&["macos", "linux"], None).meets(&macos),
            "a leader trusting two platforms may land on the one macos does not count"
        );
        // A configured trust set is met by a leader narrowed inside it.
        assert!(linux.meets(&demand(&["macos", "linux"], None)));
    }

    #[test]
    fn a_selector_demand_is_met_only_by_a_selector_at_least_as_strict() {
        let configured = Some(ExecutorSelector {
            required_toolchains: vec!["rust".into()],
            ..ExecutorSelector::default()
        });
        let narrowed = Some(ExecutorSelector {
            os: Some("Linux".into()),
            required_toolchains: vec!["rust".into()],
            ..ExecutorSelector::default()
        });
        assert!(demand(&[], narrowed.clone()).meets(&demand(&[], configured.clone())));
        assert!(!demand(&[], configured).meets(&demand(&[], narrowed.clone())));
        assert!(demand(&[], narrowed).meets(&demand(&[], os("linux"))));

        let named = Some(ExecutorSelector {
            name: Some("fedora".into()),
            ..ExecutorSelector::default()
        });
        assert!(demand(&[], named.clone()).meets(&demand(&[], named.clone())));
        assert!(
            !demand(&[], named).meets(&demand(&[], os("linux"))),
            "a named leader's platform is the fleet's to say, so it is not assumed"
        );
    }

    #[test]
    fn avoidance_and_pins_are_hard_clauses() {
        let avoids = PlacementDemand {
            avoided_executor_ids: BTreeSet::from(["broken".to_string()]),
            ..PlacementDemand::default()
        };
        assert!(avoids.meets(&PlacementDemand::default()));
        assert!(!PlacementDemand::default().meets(&avoids));

        let pinned = PlacementDemand {
            pinned_executor_id: Some("home".into()),
            ..PlacementDemand::default()
        };
        assert!(pinned.meets(&pinned));
        assert!(!PlacementDemand::default().meets(&pinned));
    }
}
