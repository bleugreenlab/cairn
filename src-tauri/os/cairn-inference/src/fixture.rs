//! bginf's own test fixture (`ctl/test_ctl.py`) as Cairn records: sp1 and
//! sp2 in pool `sparks`, halo in pool `halo`, the defaults its `api` fixture
//! sets (`dsv4f-tp2` and `qwen3-embed-4b`), and no pins.

use std::collections::BTreeSet;

use crate::deployment::Deployment;
use crate::inventory::Inventory;
use crate::machine::MachineName;
use crate::pool::{DeploymentSet, Pool, PoolMachine, RecordSource};
use crate::recipe::{Engine, ModelRef, Recipe, Requirements};

pub(crate) fn command_recipe(id: &str, machines: u32) -> Recipe {
    Recipe {
        id: id.into(),
        source: RecordSource::Bginf,
        model: ModelRef {
            name: id.into(),
            weights: Vec::new(),
        },
        served_name: id.into(),
        upstream_id: None,
        verbs: None,
        engine: Engine::Command {
            start: "./start.sh".into(),
            stop: Some("./stop.sh".into()),
            description: None,
        },
        requirements: Requirements {
            machines,
            accelerator: None,
            interconnect: None,
            memory: None,
        },
        aliases: Vec::new(),
        notes: None,
    }
}

/// A deployment of the recipe with the same id, headed on its first machine.
pub(crate) fn deployment(id: &str, machines: &[&str]) -> Deployment {
    Deployment {
        id: id.into(),
        source: RecordSource::Bginf,
        recipe: id.into(),
        machines: machines.iter().map(|m| MachineName::from(*m)).collect(),
        head: machines[0].into(),
        api_port: 8000,
        api_path: "/v1".into(),
        health_path: "/v1/models".into(),
    }
}

pub(crate) fn set(ids: &[&str]) -> DeploymentSet {
    ids.iter().map(|id| (*id).into()).collect()
}

pub(crate) fn machines(names: &[&str]) -> BTreeSet<MachineName> {
    names.iter().map(|m| (*m).into()).collect()
}

pub(crate) fn pool(id: &str, machines: &[&str], default: &[&str]) -> Pool {
    Pool {
        id: id.into(),
        source: RecordSource::Bginf,
        machines: machines
            .iter()
            .map(|m| PoolMachine {
                name: (*m).into(),
                address: None,
            })
            .collect(),
        default: set(default),
        pin: None,
    }
}

pub(crate) fn bginf_test_inventory() -> Inventory {
    let mut inventory = Inventory::default();
    for (id, nodes) in [
        ("dsv4f-tp2", &["sp1", "sp2"][..]),
        ("qwen36-fast", &["sp1"][..]),
        ("qwen3-embed-4b", &["halo"][..]),
    ] {
        inventory.insert_recipe(command_recipe(id, nodes.len() as u32));
        inventory.insert_deployment(deployment(id, nodes));
    }
    inventory.insert_pool(pool("sparks", &["sp1", "sp2"], &["dsv4f-tp2"]));
    inventory.insert_pool(pool("halo", &["halo"], &["qwen3-embed-4b"]));
    inventory
}
