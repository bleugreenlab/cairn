//! Acceptance: bginf's configuration files (a scrubbed copy, same structure)
//! import as three command recipes, their deployments, and two pools with
//! bginf's `halo` mapped to the executor `fedora`.
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cairn_inference::bginf::{import, BginfFiles, MachineMap, PoolSlots, Topology};
use cairn_inference::{Engine, Inventory, MachineName, PoolId};

fn files() -> BginfFiles {
    BginfFiles::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bginf")).unwrap()
}

fn import_with(slots: &BTreeMap<String, PoolSlots>) -> (Inventory, Vec<String>) {
    let files = files();
    let map = MachineMap::new([("halo".to_string(), "fedora".to_string())]);
    let imported = import(
        &files.profiles,
        Topology::Files {
            files: &files,
            slots,
        },
        &map,
    );
    (imported.inventory, imported.warnings)
}

fn names(machines: &[&str]) -> Vec<MachineName> {
    machines.iter().map(|m| MachineName::from(*m)).collect()
}

#[test]
fn bginf_files_import_as_three_command_recipes_and_two_pools() {
    let (inventory, warnings) = import_with(&BTreeMap::new());
    assert_eq!(warnings, Vec::<String>::new());
    assert_eq!(inventory.validate(), Ok(()));

    let recipes: Vec<&str> = inventory.recipes.keys().map(|r| r.as_str()).collect();
    assert_eq!(recipes, vec!["dsv4f-tp2", "qwen3-embed-4b", "qwen36-fast"]);
    assert!(inventory
        .recipes
        .values()
        .all(|r| matches!(r.engine, Engine::Command { .. }) && r.verbs.is_none()));
    let deployments: Vec<&str> = inventory.deployments.keys().map(|d| d.as_str()).collect();
    assert_eq!(deployments, recipes, "off becomes no record");

    let pool_machines = |id: &str| -> Vec<MachineName> {
        inventory.pools[&PoolId::from(id)]
            .machines
            .iter()
            .map(|m| m.name.clone())
            .collect()
    };
    assert_eq!(inventory.pools.len(), 2);
    assert_eq!(pool_machines("sparks"), names(&["sp1", "sp2"]));
    assert_eq!(pool_machines("halo"), names(&["fedora"]));

    let tp2 = &inventory.deployments[&"dsv4f-tp2".into()];
    assert_eq!(tp2.machines, names(&["sp1", "sp2"]));
    assert_eq!(tp2.head, MachineName::from("sp1"));
    assert_eq!(
        tp2.health_url(inventory.address_of(&tp2.head).unwrap()),
        "http://192.0.2.40:8888/v1/models"
    );
    let embed = &inventory.deployments[&"qwen3-embed-4b".into()];
    assert_eq!(embed.head, MachineName::from("fedora"));
    assert_eq!(
        embed.health_url(inventory.address_of(&embed.head).unwrap()),
        "http://192.0.2.43:8090/health"
    );
    let embed_recipe = &inventory.recipes[&"qwen3-embed-4b".into()];
    assert_eq!(embed_recipe.served_name, "qwen3-embedding-4b");
    assert_eq!(
        embed_recipe.upstream_id.as_deref(),
        Some("qwen/qwen3-embedding-4b")
    );
    assert_eq!(embed_recipe.requirements.machines, 1);
    assert_eq!(
        inventory.recipes[&"dsv4f-tp2".into()].requirements.machines,
        2
    );
}

#[test]
fn a_supplied_off_default_is_the_empty_set() {
    let slots = BTreeMap::from([
        (
            "sparks".to_string(),
            PoolSlots {
                pin: None,
                default: Some("off".to_string()),
            },
        ),
        (
            "halo".to_string(),
            PoolSlots {
                pin: None,
                default: Some("qwen3-embed-4b".to_string()),
            },
        ),
    ]);
    let (inventory, warnings) = import_with(&slots);
    assert_eq!(warnings, Vec::<String>::new());
    assert_eq!(
        inventory.pools[&PoolId::from("sparks")].default,
        BTreeSet::new()
    );
    assert_eq!(
        inventory.pools[&PoolId::from("halo")].default,
        BTreeSet::from(["qwen3-embed-4b".into()])
    );
}

#[test]
fn the_imported_inventory_round_trips_through_cairn_yaml() {
    let (inventory, _) = import_with(&BTreeMap::new());
    assert_eq!(Inventory::from_yaml(&inventory.to_yaml()), Ok(inventory));
}
