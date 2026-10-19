//! The pin-over-default resolver and the convergence check, ported from
//! bginf's `ctl.py` (`resolve` and `converged`) and generalised from one
//! profile per pool to sets of deployments.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Serialize;

use crate::deployment::{Deployment, DeploymentId};
use crate::machine::MachineName;
use crate::pool::{DeploymentSet, Pool};

/// One level of a pool's priority stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Slot {
    Pin,
    Default,
}

impl Slot {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Default => "default",
        }
    }
}

/// The priority stack, highest first. bginf reserves a demand slot and a
/// bench slot between the pin and the default (pin > demand > bench >
/// default); they go here, in that order, when they are built. The default is
/// always last and always present: an unset default is the empty set.
pub const SLOTS: [Slot; 2] = [Slot::Pin, Slot::Default];

/// The set a slot holds in `pool`, or `None` when the slot is empty (no pin).
pub fn slot_set(pool: &Pool, slot: Slot) -> Option<&DeploymentSet> {
    match slot {
        Slot::Pin => pool.pin.as_ref(),
        Slot::Default => Some(&pool.default),
    }
}

/// A slot whose set could not run because some machine it needs is
/// unpowered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Blocked {
    pub slot: Slot,
    pub set: DeploymentSet,
    /// The set's unpowered machines, in deployment-id then placement order.
    pub unpowered: Vec<MachineName>,
}

/// Why a pool resolved the way it did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Reason {
    /// The pin fits.
    Pin,
    /// The default fits. bginf reports a blocked pin here as plain
    /// `default`; Cairn keeps the pin it passed over.
    Default { pin_blocked: Option<Blocked> },
    /// Neither fits; the largest single deployment that does won.
    Fallback { blocked: Blocked },
    /// Neither fits and no deployment of the pool does either.
    Nothing { blocked: Blocked },
}

/// A pool's desired deployments and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resolution {
    pub desired: DeploymentSet,
    pub reason: Reason,
}

fn set_label(set: &DeploymentSet) -> String {
    if set.is_empty() {
        "nothing".to_string()
    } else {
        set.iter()
            .map(DeploymentId::as_str)
            .collect::<Vec<_>>()
            .join("+")
    }
}

fn machine_list(machines: &[MachineName]) -> String {
    machines
        .iter()
        .map(MachineName::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

impl fmt::Display for Blocked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the {} {} needs {} (powered off)",
            self.slot.as_str(),
            set_label(&self.set),
            machine_list(&self.unpowered)
        )
    }
}

/// Cairn's own explanation, which also names a pin the default passed over.
impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pin => f.write_str("pin"),
            Self::Default { pin_blocked: None } => f.write_str("default"),
            Self::Default {
                pin_blocked: Some(blocked),
            } => write!(f, "default, because {blocked}"),
            Self::Fallback { blocked } => write!(f, "fallback, because {blocked}"),
            Self::Nothing { blocked } => write!(f, "nothing fits, because {blocked}"),
        }
    }
}

/// The machines `set` occupies, in deployment-id order and then each
/// deployment's placement order, without repeats.
pub fn machines_of(
    set: &DeploymentSet,
    deployments: &BTreeMap<DeploymentId, Deployment>,
) -> Vec<MachineName> {
    let mut seen = BTreeSet::new();
    set.iter()
        .filter_map(|id| deployments.get(id))
        .flat_map(|d| d.machines.iter())
        .filter(|m| seen.insert(*m))
        .cloned()
        .collect()
}

/// Resolve one pool.
///
/// The pin, then the default, wins if every machine its set needs is
/// powered; an empty set always fits. Only the first blocked slot is
/// recorded, as bginf does. When neither fits, each deployment of the pool is
/// a candidate on its own (so a member of a blocked set can still run alone),
/// and the largest that fits wins, where **largest means the most machines
/// occupied, ties broken by deployment id ascending**: bginf's sort key
/// `(-len(nodes), id)`. When none fits, the pool runs nothing.
///
/// Packing several fallback deployments onto disjoint machines would be a
/// refinement; bginf picks one, and so does this.
pub fn resolve(
    pool: &Pool,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    powered: &BTreeSet<MachineName>,
) -> Resolution {
    let mut first_blocked: Option<Blocked> = None;
    for slot in SLOTS {
        let Some(set) = slot_set(pool, slot) else {
            continue;
        };
        let unpowered: Vec<MachineName> = machines_of(set, deployments)
            .into_iter()
            .filter(|m| !powered.contains(m))
            .collect();
        if unpowered.is_empty() {
            let reason = match slot {
                Slot::Pin => Reason::Pin,
                Slot::Default => Reason::Default {
                    pin_blocked: first_blocked,
                },
            };
            return Resolution {
                desired: set.clone(),
                reason,
            };
        }
        first_blocked.get_or_insert(Blocked {
            slot,
            set: set.clone(),
            unpowered,
        });
    }
    let blocked = first_blocked.expect("the default slot is always present, so it fit or blocked");

    let mut candidates: Vec<&Deployment> = deployments
        .values()
        .filter(|d| !d.machines.is_empty() && d.machines.iter().all(|m| pool.has_machine(m)))
        .filter(|d| d.machines.iter().all(|m| powered.contains(m)))
        .collect();
    candidates.sort_by(|a, b| {
        b.machines
            .len()
            .cmp(&a.machines.len())
            .then_with(|| a.id.cmp(&b.id))
    });
    match candidates.first() {
        Some(winner) => Resolution {
            desired: BTreeSet::from([winner.id.clone()]),
            reason: Reason::Fallback { blocked },
        },
        None => Resolution {
            desired: DeploymentSet::new(),
            reason: Reason::Nothing { blocked },
        },
    }
}

/// What one machine last reported.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineObservation {
    /// The deployment answering its health check on this machine, if any.
    pub running: Option<DeploymentId>,
    pub healthy: bool,
    /// The report is too old to trust.
    pub stale: bool,
}

/// Whether `pool` is running exactly `desired`: every machine of every
/// desired deployment runs that deployment, healthy and not stale, and every
/// other machine of the pool that reported runs nothing or a desired
/// deployment. As in bginf, staleness is not checked for those other
/// machines, and a machine that never reported does not hold the pool back
/// unless a desired deployment needs it.
pub fn converged(
    pool: &Pool,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    desired: &DeploymentSet,
    observations: &BTreeMap<MachineName, MachineObservation>,
) -> bool {
    let mut wanted = BTreeSet::new();
    for id in desired {
        let Some(deployment) = deployments.get(id) else {
            return false;
        };
        for machine in &deployment.machines {
            let running_it = observations
                .get(machine)
                .is_some_and(|o| o.running.as_ref() == Some(id) && o.healthy && !o.stale);
            if !running_it {
                return false;
            }
            wanted.insert(machine);
        }
    }
    pool.machines
        .iter()
        .filter(|m| !wanted.contains(&m.name))
        .filter_map(|m| observations.get(&m.name))
        .all(|o| o.running.as_ref().is_none_or(|r| desired.contains(r)))
}

/// Whether `deployment` is loaded: desired in a pool that has converged.
pub fn loaded(deployment: &DeploymentId, desired: &DeploymentSet, converged: bool) -> bool {
    converged && desired.contains(deployment)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{bginf_test_inventory, command_recipe, deployment, machines, pool, set};
    use crate::inventory::{Inventory, UnknownPool};
    use crate::pool::PoolId;

    fn all_powered() -> BTreeSet<MachineName> {
        machines(&["sp1", "sp2", "halo"])
    }

    fn resolve_in(
        inventory: &Inventory,
        pool: &str,
        powered: &BTreeSet<MachineName>,
    ) -> Resolution {
        inventory.resolve_pool(&pool.into(), powered).unwrap()
    }

    fn pool_mut<'a>(inventory: &'a mut Inventory, pool: &str) -> &'a mut Pool {
        inventory.pools.get_mut(&PoolId::from(pool)).unwrap()
    }

    fn default_reason() -> Reason {
        Reason::Default { pin_blocked: None }
    }

    /// bginf `test_pools_resolve_independently`.
    #[test]
    fn pools_resolve_independently() {
        let inventory = bginf_test_inventory();
        assert_eq!(
            resolve_in(&inventory, "sparks", &all_powered()),
            Resolution {
                desired: set(&["dsv4f-tp2"]),
                reason: default_reason()
            }
        );
        assert_eq!(
            resolve_in(&inventory, "halo", &all_powered()),
            Resolution {
                desired: set(&["qwen3-embed-4b"]),
                reason: default_reason()
            }
        );
        assert_eq!(
            inventory.deployments[&DeploymentId::from("qwen3-embed-4b")].head,
            MachineName::from("halo")
        );
    }

    fn observe(running: Option<&str>, healthy: bool) -> MachineObservation {
        MachineObservation {
            running: running.map(DeploymentId::from),
            healthy,
            stale: false,
        }
    }

    fn state_of(
        inventory: &Inventory,
        pool: &str,
        observations: &BTreeMap<MachineName, MachineObservation>,
    ) -> (DeploymentSet, bool) {
        let resolution = resolve_in(inventory, pool, &all_powered());
        let pool = &inventory.pools[&PoolId::from(pool)];
        let done = converged(
            pool,
            &inventory.deployments,
            &resolution.desired,
            observations,
        );
        (resolution.desired, done)
    }

    /// bginf `test_each_pool_converges_on_its_own`.
    #[test]
    fn each_pool_converges_on_its_own() {
        let inventory = bginf_test_inventory();
        let mut observations = BTreeMap::new();
        for node in ["sp1", "sp2"] {
            observations.insert(MachineName::from(node), observe(Some("dsv4f-tp2"), true));
        }
        let (sparks, sparks_done) = state_of(&inventory, "sparks", &observations);
        let (halo, halo_done) = state_of(&inventory, "halo", &observations);
        assert!(sparks_done && !halo_done);
        assert!(
            loaded(&"dsv4f-tp2".into(), &sparks, sparks_done),
            "halo running nothing yet doesn't hold the Sparks back"
        );
        assert!(!loaded(&"qwen3-embed-4b".into(), &halo, halo_done));

        observations.insert("halo".into(), observe(Some("qwen3-embed-4b"), true));
        let (sparks, sparks_done) = state_of(&inventory, "sparks", &observations);
        let (halo, halo_done) = state_of(&inventory, "halo", &observations);
        assert!(loaded(&"dsv4f-tp2".into(), &sparks, sparks_done));
        assert!(loaded(&"qwen3-embed-4b".into(), &halo, halo_done));

        // A Spark drops out: only its pool unconverges.
        observations.insert("sp2".into(), observe(None, false));
        let (sparks, sparks_done) = state_of(&inventory, "sparks", &observations);
        let (halo, halo_done) = state_of(&inventory, "halo", &observations);
        assert!(!loaded(&"dsv4f-tp2".into(), &sparks, sparks_done));
        assert!(loaded(&"qwen3-embed-4b".into(), &halo, halo_done));
    }

    #[test]
    fn a_stale_wanted_machine_is_not_converged_but_a_stale_bystander_is_ignored() {
        let inventory = bginf_test_inventory();
        let mut inventory = inventory;
        pool_mut(&mut inventory, "sparks").default = set(&["qwen36-fast"]);
        let mut observations = BTreeMap::new();
        observations.insert(
            MachineName::from("sp1"),
            MachineObservation {
                stale: true,
                ..observe(Some("qwen36-fast"), true)
            },
        );
        assert!(!state_of(&inventory, "sparks", &observations).1);
        observations.insert("sp1".into(), observe(Some("qwen36-fast"), true));
        observations.insert(
            "sp2".into(),
            MachineObservation {
                stale: true,
                ..observe(None, false)
            },
        );
        assert!(state_of(&inventory, "sparks", &observations).1);
        observations.insert("sp2".into(), observe(Some("dsv4f-tp2"), true));
        assert!(!state_of(&inventory, "sparks", &observations).1);
    }

    /// bginf `test_pin_targets_the_profiles_pool_and_off_every_pool`.
    #[test]
    fn pins_target_their_pool_and_the_empty_pin_fits_every_pool() {
        let mut inventory = bginf_test_inventory();
        pool_mut(&mut inventory, "sparks").pin = Some(set(&["qwen36-fast"]));
        assert_eq!(
            resolve_in(&inventory, "sparks", &all_powered()),
            Resolution {
                desired: set(&["qwen36-fast"]),
                reason: Reason::Pin
            }
        );
        assert_eq!(
            resolve_in(&inventory, "halo", &all_powered()).reason,
            default_reason()
        );

        for pool in ["sparks", "halo"] {
            pool_mut(&mut inventory, pool).pin = Some(set(&[]));
        }
        for pool in ["sparks", "halo"] {
            assert_eq!(
                resolve_in(&inventory, pool, &all_powered()),
                Resolution {
                    desired: set(&[]),
                    reason: Reason::Pin
                }
            );
        }

        pool_mut(&mut inventory, "halo").pin = None;
        assert_eq!(
            resolve_in(&inventory, "halo", &all_powered()).desired,
            set(&["qwen3-embed-4b"])
        );
        assert_eq!(
            resolve_in(&inventory, "sparks", &all_powered()).desired,
            set(&[])
        );

        pool_mut(&mut inventory, "sparks").pin = None;
        for pool in ["sparks", "halo"] {
            assert_eq!(
                resolve_in(&inventory, pool, &all_powered()).reason,
                default_reason()
            );
        }

        assert_eq!(
            inventory.resolve_pool(&"nope".into(), &all_powered()),
            Err(UnknownPool("nope".into()))
        );
    }

    /// bginf `test_power_is_per_pool_and_only_for_managed_nodes`, resolver half.
    #[test]
    fn an_unpowered_spark_falls_back_in_its_own_pool_only() {
        let inventory = bginf_test_inventory();
        let powered = machines(&["sp1", "halo"]);
        assert_eq!(
            resolve_in(&inventory, "sparks", &powered),
            Resolution {
                desired: set(&["qwen36-fast"]),
                reason: Reason::Fallback {
                    blocked: Blocked {
                        slot: Slot::Default,
                        set: set(&["dsv4f-tp2"]),
                        unpowered: vec!["sp2".into()],
                    }
                }
            }
        );
        assert_eq!(
            resolve_in(&inventory, "halo", &powered),
            Resolution {
                desired: set(&["qwen3-embed-4b"]),
                reason: default_reason()
            }
        );
    }

    #[test]
    fn a_blocked_pin_with_a_fitting_default_records_the_pin() {
        let mut inventory = bginf_test_inventory();
        let sparks = pool_mut(&mut inventory, "sparks");
        sparks.pin = Some(set(&["dsv4f-tp2"]));
        sparks.default = set(&["qwen36-fast"]);
        let resolution = resolve_in(&inventory, "sparks", &machines(&["sp1", "halo"]));
        assert_eq!(resolution.desired, set(&["qwen36-fast"]));
        assert_eq!(
            resolution.reason,
            Reason::Default {
                pin_blocked: Some(Blocked {
                    slot: Slot::Pin,
                    set: set(&["dsv4f-tp2"]),
                    unpowered: vec!["sp2".into()],
                })
            }
        );
        assert_eq!(
            resolution.reason.to_string(),
            "default, because the pin dsv4f-tp2 needs sp2 (powered off)"
        );
    }

    #[test]
    fn when_nothing_fits_the_pool_runs_nothing() {
        let inventory = bginf_test_inventory();
        let resolution = resolve_in(&inventory, "sparks", &machines(&["halo"]));
        assert_eq!(
            resolution,
            Resolution {
                desired: set(&[]),
                reason: Reason::Nothing {
                    blocked: Blocked {
                        slot: Slot::Default,
                        set: set(&["dsv4f-tp2"]),
                        unpowered: vec!["sp1".into(), "sp2".into()],
                    }
                }
            }
        );
    }

    #[test]
    fn the_fallback_prefers_more_machines_then_the_lower_id() {
        let mut inventory = Inventory::default();
        for (id, nodes) in [
            ("wide", &["a", "b", "c"][..]),
            ("pair-b", &["a", "b"][..]),
            ("pair-a", &["b", "a"][..]),
            ("solo", &["a"][..]),
        ] {
            inventory.insert_recipe(command_recipe(id, nodes.len() as u32));
            inventory.insert_deployment(deployment(id, nodes));
        }
        inventory.insert_pool(pool("p", &["a", "b", "c"], &["wide"]));
        assert_eq!(inventory.validate(), Ok(()));
        let resolution = resolve_in(&inventory, "p", &machines(&["a", "b"]));
        assert_eq!(resolution.desired, set(&["pair-a"]));
        assert!(matches!(resolution.reason, Reason::Fallback { .. }));
        let resolution = resolve_in(&inventory, "p", &machines(&["a"]));
        assert_eq!(resolution.desired, set(&["solo"]));
    }

    /// The sets case: qwen36-fast on sp1 together with another model on sp2.
    fn sets_inventory() -> Inventory {
        let mut inventory = bginf_test_inventory();
        inventory.insert_recipe(command_recipe("other", 1));
        inventory.insert_deployment(deployment("other", &["sp2"]));
        pool_mut(&mut inventory, "sparks").default = set(&["qwen36-fast", "other"]);
        assert_eq!(inventory.validate(), Ok(()));
        inventory
    }

    #[test]
    fn a_default_set_runs_every_member() {
        let inventory = sets_inventory();
        assert_eq!(
            resolve_in(&inventory, "sparks", &all_powered()),
            Resolution {
                desired: set(&["qwen36-fast", "other"]),
                reason: default_reason()
            }
        );
    }

    #[test]
    fn a_member_of_a_blocked_set_still_runs_alone() {
        let inventory = sets_inventory();
        let blocked = |unpowered: &str| Blocked {
            slot: Slot::Default,
            set: set(&["qwen36-fast", "other"]),
            unpowered: vec![unpowered.into()],
        };
        assert_eq!(
            resolve_in(&inventory, "sparks", &machines(&["sp1", "halo"])),
            Resolution {
                desired: set(&["qwen36-fast"]),
                reason: Reason::Fallback {
                    blocked: blocked("sp2")
                }
            }
        );
        assert_eq!(
            resolve_in(&inventory, "sparks", &machines(&["sp2", "halo"])),
            Resolution {
                desired: set(&["other"]),
                reason: Reason::Fallback {
                    blocked: blocked("sp1")
                }
            }
        );
    }

    #[test]
    fn a_two_deployment_pin_wins_over_the_default() {
        let mut inventory = sets_inventory();
        let sparks = pool_mut(&mut inventory, "sparks");
        sparks.default = set(&["dsv4f-tp2"]);
        sparks.pin = Some(set(&["qwen36-fast", "other"]));
        assert_eq!(inventory.validate(), Ok(()));
        assert_eq!(
            resolve_in(&inventory, "sparks", &all_powered()),
            Resolution {
                desired: set(&["qwen36-fast", "other"]),
                reason: Reason::Pin
            }
        );
    }

    #[test]
    fn a_set_converges_only_when_every_member_runs() {
        let inventory = sets_inventory();
        let mut observations = BTreeMap::new();
        observations.insert(MachineName::from("sp1"), observe(Some("qwen36-fast"), true));
        assert!(!state_of(&inventory, "sparks", &observations).1);
        observations.insert("sp2".into(), observe(Some("other"), true));
        assert!(state_of(&inventory, "sparks", &observations).1);
    }
}
