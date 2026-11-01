//! The test inventory, bginf's own test fixture (`ctl/test_ctl.py`) as Cairn
//! records: sp1 and sp2 in pool `sparks`, halo in pool `halo`, the defaults
//! `dsv4f-tp2` and `qwen3-embed-4b`, and no pins. Each recipe's model is
//! registered under the recipe's id, every fact unknown.

use std::collections::BTreeSet;

use crate::deployment::Deployment;
use crate::inventory::Inventory;
use crate::machine::MachineName;
use crate::model::Model;
use crate::pool::{DeploymentSet, Pool, PoolMachine, RecordSource};
use crate::recipe::{Engine, ModelRef, Recipe, RecipeOrigin, Requirements};

pub(crate) fn command_recipe(id: &str, machines: u32) -> Recipe {
    Recipe {
        id: id.into(),
        source: RecordSource::Cairn,
        model: ModelRef {
            name: id.into(),
            weights: Vec::new(),
        },
        served_name: id.into(),
        upstream_id: None,
        verbs: None,
        modalities: None,
        quantization: None,
        origin: RecipeOrigin::Authored,
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
        notes: None,
    }
}

/// A deployment of the recipe with the same id, headed on its first machine.
pub(crate) fn deployment(id: &str, machines: &[&str]) -> Deployment {
    Deployment {
        id: id.into(),
        source: RecordSource::Cairn,
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
        source: RecordSource::Cairn,
        machines: machines
            .iter()
            .map(|m| PoolMachine {
                name: (*m).into(),
                address: None,
                link: None,
            })
            .collect(),
        default: set(default),
        pin: None,
    }
}

/// A recipe and its model, registered together.
pub(crate) fn insert_command_recipe(inventory: &mut Inventory, id: &str, machines: u32) {
    inventory.insert_model(Model::named(id));
    inventory.insert_recipe(command_recipe(id, machines));
}

pub(crate) fn test_inventory() -> Inventory {
    let mut inventory = Inventory::default();
    for (id, nodes) in [
        ("dsv4f-tp2", &["sp1", "sp2"][..]),
        ("qwen36-fast", &["sp1"][..]),
        ("qwen3-embed-4b", &["halo"][..]),
    ] {
        insert_command_recipe(&mut inventory, id, nodes.len() as u32);
        inventory.insert_deployment(deployment(id, nodes));
    }
    inventory.insert_pool(pool("sparks", &["sp1", "sp2"], &["dsv4f-tp2"]));
    inventory.insert_pool(pool("halo", &["halo"], &["qwen3-embed-4b"]));
    inventory
}
