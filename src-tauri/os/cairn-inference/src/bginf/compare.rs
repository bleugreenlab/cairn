//! Cairn's resolution beside bginf's own, pool by pool.
//!
//! Everything is taken from one `/state` snapshot: the pin and default (via
//! the import), the powered set, and the node reports, so the two sides are
//! computed from identical inputs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::deployment::DeploymentId;
use crate::inventory::Inventory;
use crate::machine::MachineName;
use crate::pool::{DeploymentSet, PoolId};
use crate::resolve::{self, Blocked, MachineObservation, Reason, Resolution};

use super::import::MachineMap;
use super::wire::{PoolState, State};

/// bginf's name for the empty desired set.
const OFF: &str = "off";

/// Whether bginf counts `machine` as powered: its desired power is `on`, or
/// ctl manages no outlet for it (an unmanaged node is always powered).
pub fn is_powered(state: &State, machine: &MachineName, map: &MachineMap) -> bool {
    state
        .power
        .get(&map.bginf(machine))
        .is_none_or(|power| power.desired == "on")
}

/// The powered machines among the inventory's pools, as bginf counts them.
pub fn powered_set(
    inventory: &Inventory,
    state: &State,
    map: &MachineMap,
) -> BTreeSet<MachineName> {
    inventory
        .pools
        .values()
        .flat_map(|pool| pool.machines.iter())
        .map(|m| m.name.clone())
        .filter(|m| is_powered(state, m, map))
        .collect()
}

/// The node reports as machine observations.
pub fn observations(state: &State, map: &MachineMap) -> BTreeMap<MachineName, MachineObservation> {
    state
        .nodes
        .iter()
        .map(|(node, report)| {
            (
                map.cairn(node),
                MachineObservation {
                    running: report.running.as_deref().map(DeploymentId::from),
                    healthy: report.healthy.unwrap_or(false),
                    stale: report.stale,
                },
            )
        })
        .collect()
}

/// A desired set in bginf's terms: `off` when empty, else the ids joined
/// with `+`.
pub fn desired_string(set: &DeploymentSet) -> String {
    if set.is_empty() {
        OFF.to_string()
    } else {
        set.iter()
            .map(DeploymentId::as_str)
            .collect::<Vec<_>>()
            .join("+")
    }
}

fn blocked_string(blocked: &Blocked, map: &MachineMap) -> String {
    let nodes: Vec<String> = blocked.unpowered.iter().map(|m| map.bginf(m)).collect();
    format!(
        "{} needs {} (powered off)",
        desired_string(&blocked.set),
        nodes.join(",")
    )
}

/// The reason exactly as bginf's `resolve` words it, with bginf's node
/// names. A pin the default passed over is not mentioned, as in bginf.
pub fn reason_string(reason: &Reason, map: &MachineMap) -> String {
    match reason {
        Reason::Pin => "pin".to_string(),
        Reason::Default { .. } => "default".to_string(),
        Reason::Fallback { blocked } => format!("fallback: {}", blocked_string(blocked, map)),
        Reason::Nothing { blocked } => blocked_string(blocked, map),
    }
}

/// One way the two sides differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Difference {
    Desired {
        cairn: String,
        bginf: String,
    },
    Reason {
        cairn: String,
        bginf: String,
    },
    Converged {
        cairn: bool,
        bginf: bool,
    },
    /// bginf reports a pool Cairn has no record of.
    PoolMissingInCairn,
    /// Cairn holds a pool bginf does not report.
    PoolMissingInBginf,
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Desired { cairn, bginf } => {
                write!(f, "desired: Cairn {cairn}, bginf {bginf}")
            }
            Self::Reason { cairn, bginf } => {
                write!(f, "reason: Cairn {cairn:?}, bginf {bginf:?}")
            }
            Self::Converged { cairn, bginf } => {
                write!(f, "converged: Cairn {cairn}, bginf {bginf}")
            }
            Self::PoolMissingInCairn => {
                f.write_str("bginf reports this pool; Cairn has no record of it")
            }
            Self::PoolMissingInBginf => {
                f.write_str("Cairn holds this pool; bginf does not report it")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Agree,
    Disagree(Vec<Difference>),
}

/// Cairn's side of one pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CairnSide {
    pub resolution: Resolution,
    /// Cairn's convergence check over bginf's node reports.
    pub converged: bool,
}

/// One pool, both sides, and the verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolComparison {
    pub pool: PoolId,
    pub cairn: Option<CairnSide>,
    pub bginf: Option<PoolState>,
    pub verdict: Verdict,
}

/// Compare every pool either side knows.
pub fn compare(inventory: &Inventory, state: &State, map: &MachineMap) -> Vec<PoolComparison> {
    let powered = powered_set(inventory, state, map);
    let observed = observations(state, map);
    let ids: BTreeSet<PoolId> = inventory
        .pools
        .keys()
        .cloned()
        .chain(state.pools.keys().map(|p| PoolId::from(p.as_str())))
        .collect();
    ids.into_iter()
        .map(|id| {
            let cairn = inventory.pools.get(&id).map(|pool| {
                let resolution = resolve::resolve(pool, &inventory.deployments, &powered);
                let converged = resolve::converged(
                    pool,
                    &inventory.deployments,
                    &resolution.desired,
                    &observed,
                );
                CairnSide {
                    resolution,
                    converged,
                }
            });
            let bginf = state.pools.get(id.as_str()).cloned();
            let verdict = match (&cairn, &bginf) {
                (None, _) => Verdict::Disagree(vec![Difference::PoolMissingInCairn]),
                (_, None) => Verdict::Disagree(vec![Difference::PoolMissingInBginf]),
                (Some(cairn), Some(bginf)) => {
                    let mut differences = Vec::new();
                    let desired = desired_string(&cairn.resolution.desired);
                    if desired != bginf.desired {
                        differences.push(Difference::Desired {
                            cairn: desired,
                            bginf: bginf.desired.clone(),
                        });
                    }
                    let reason = reason_string(&cairn.resolution.reason, map);
                    if reason != bginf.reason {
                        differences.push(Difference::Reason {
                            cairn: reason,
                            bginf: bginf.reason.clone(),
                        });
                    }
                    if cairn.converged != bginf.converged {
                        differences.push(Difference::Converged {
                            cairn: cairn.converged,
                            bginf: bginf.converged,
                        });
                    }
                    if differences.is_empty() {
                        Verdict::Agree
                    } else {
                        Verdict::Disagree(differences)
                    }
                }
            };
            PoolComparison {
                pool: id,
                cairn,
                bginf,
                verdict,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bginf::import::{import, Topology};
    use crate::bginf::wire::Profile;
    use crate::fixture::{bginf_test_inventory, set};

    fn halo_is_fedora() -> MachineMap {
        MachineMap::new([("halo".to_string(), "fedora".to_string())])
    }

    fn state(value: serde_json::Value) -> State {
        serde_json::from_value(value).unwrap()
    }

    /// bginf `test_power_is_per_pool_and_only_for_managed_nodes`: only managed
    /// nodes appear under `power`, a node without an outlet is always
    /// powered, and an unpowered sp2 gives bginf's exact fallback reason.
    #[test]
    fn power_is_per_pool_and_an_unmanaged_node_is_always_powered() {
        let inventory = bginf_test_inventory();
        let snapshot = state(serde_json::json!({
            "power": {"sp1": {"desired": "on"}, "sp2": {"desired": "off", "phase": "draining"}}
        }));
        let map = MachineMap::default();
        assert_eq!(
            powered_set(&inventory, &snapshot, &map),
            BTreeSet::from(["sp1".into(), "halo".into()])
        );
        let sparks = inventory
            .resolve_pool(&"sparks".into(), &powered_set(&inventory, &snapshot, &map))
            .unwrap();
        assert_eq!(sparks.desired, set(&["qwen36-fast"]));
        assert_eq!(
            reason_string(&sparks.reason, &map),
            "fallback: dsv4f-tp2 needs sp2 (powered off)"
        );
        let halo = inventory
            .resolve_pool(&"halo".into(), &powered_set(&inventory, &snapshot, &map))
            .unwrap();
        assert_eq!(
            (
                desired_string(&halo.desired),
                reason_string(&halo.reason, &map)
            ),
            ("qwen3-embed-4b".to_string(), "default".to_string())
        );
    }

    #[test]
    fn reason_strings_use_bginf_node_names_and_hide_a_blocked_pin() {
        let map = halo_is_fedora();
        let blocked = Blocked {
            slot: resolve::Slot::Pin,
            set: set(&["a", "b"]),
            unpowered: vec!["sp1".into(), "fedora".into()],
        };
        assert_eq!(
            reason_string(
                &Reason::Nothing {
                    blocked: blocked.clone()
                },
                &map
            ),
            "a+b needs sp1,halo (powered off)"
        );
        assert_eq!(
            reason_string(
                &Reason::Default {
                    pin_blocked: Some(blocked)
                },
                &map
            ),
            "default"
        );
    }

    fn profile(nodes: &[&str], host: &str, port: u16) -> Profile {
        Profile {
            nodes: nodes.iter().map(|n| n.to_string()).collect(),
            head: Some(nodes[0].to_string()),
            start: Some("./start.sh".into()),
            stop: Some("./stop.sh".into()),
            health: Some(format!("http://{host}:{port}/v1/models")),
            api_base: Some(format!("http://{host}:{port}/v1")),
            ..Profile::default()
        }
    }

    fn live_profiles() -> crate::bginf::Profiles {
        let mut profiles = crate::bginf::Profiles::new();
        profiles.insert(
            "dsv4f-tp2".into(),
            profile(&["sp1", "sp2"], "192.0.2.40", 8888),
        );
        profiles.insert("qwen36-fast".into(), profile(&["sp1"], "192.0.2.40", 8001));
        profiles.insert(
            "qwen3-embed-4b".into(),
            profile(&["halo"], "192.0.2.43", 8090),
        );
        profiles.insert(
            "off".into(),
            Profile {
                id: Some("off".into()),
                ..Profile::default()
            },
        );
        profiles
    }

    /// A `/state` as bginf would serve it with sp2 powered off, the Sparks on
    /// their qwen36 fallback and halo serving its default.
    fn live_state(sparks_converged: bool) -> State {
        state(serde_json::json!({
            "pools": {
                "sparks": {"desired": "qwen36-fast", "reason": "fallback: dsv4f-tp2 needs sp2 (powered off)",
                           "pin": null, "default": "dsv4f-tp2", "running": "qwen36-fast",
                           "converged": sparks_converged, "nodes": ["sp1", "sp2"]},
                "halo": {"desired": "qwen3-embed-4b", "reason": "default", "pin": null,
                         "default": "qwen3-embed-4b", "running": "qwen3-embed-4b", "converged": true,
                         "nodes": ["halo"]}
            },
            "power": {"sp1": {"desired": "on", "phase": "on"}, "sp2": {"desired": "off", "phase": "off"}},
            "nodes": {
                "sp1": {"running": "qwen36-fast", "healthy": true, "stale": false, "age_s": 4},
                "halo": {"running": "qwen3-embed-4b", "healthy": true, "stale": false, "age_s": 9}
            }
        }))
    }

    #[test]
    fn a_live_snapshot_agrees_pool_by_pool() {
        let map = halo_is_fedora();
        let snapshot = live_state(true);
        let imported = import(&live_profiles(), Topology::Live(&snapshot), &map);
        assert_eq!(imported.warnings, Vec::<String>::new());
        let comparisons = compare(&imported.inventory, &snapshot, &map);
        assert_eq!(
            comparisons
                .iter()
                .map(|c| (c.pool.as_str(), c.verdict.clone()))
                .collect::<Vec<_>>(),
            vec![("halo", Verdict::Agree), ("sparks", Verdict::Agree)]
        );
    }

    #[test]
    fn a_disagreement_names_each_difference() {
        let map = halo_is_fedora();
        let snapshot = live_state(false);
        let imported = import(&live_profiles(), Topology::Live(&snapshot), &map);
        let mut snapshot = snapshot;
        snapshot.pools.get_mut("halo").unwrap().reason = "pin".into();
        let comparisons = compare(&imported.inventory, &snapshot, &map);
        assert_eq!(
            comparisons[0].verdict,
            Verdict::Disagree(vec![Difference::Reason {
                cairn: "default".into(),
                bginf: "pin".into()
            }])
        );
        assert_eq!(
            comparisons[1].verdict,
            Verdict::Disagree(vec![Difference::Converged {
                cairn: true,
                bginf: false
            }])
        );
    }

    #[test]
    fn a_pool_only_one_side_knows_disagrees() {
        let inventory = bginf_test_inventory();
        let snapshot = state(serde_json::json!({"pools": {"extra": {"desired": "off"}}}));
        let comparisons = compare(&inventory, &snapshot, &MachineMap::default());
        let verdicts: BTreeMap<&str, &Verdict> = comparisons
            .iter()
            .map(|c| (c.pool.as_str(), &c.verdict))
            .collect();
        assert_eq!(
            verdicts["extra"],
            &Verdict::Disagree(vec![Difference::PoolMissingInCairn])
        );
        assert_eq!(
            verdicts["sparks"],
            &Verdict::Disagree(vec![Difference::PoolMissingInBginf])
        );
    }
}
