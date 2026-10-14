//! Provider adapters and their tests must build without the application runtime.
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[test]
fn provider_and_test_graph_do_not_depend_on_runtime() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--offline",
            "--locked",
            "--all-features",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.toml"))
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let graph: Value = serde_json::from_slice(&output.stdout).unwrap();
    let core = graph["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|package| package["name"] == "cairn-core")
        .unwrap();
    for dependency in core["dependencies"].as_array().unwrap() {
        assert!(
            !matches!(dependency["name"].as_str(), Some("serenity" | "teloxide")),
            "provider SDKs belong behind cairn-channels, not directly in cairn-core"
        );
    }
    let packages: HashMap<&str, &str> = graph["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|package| {
            (
                package["id"].as_str().unwrap(),
                package["name"].as_str().unwrap(),
            )
        })
        .collect();
    let nodes: HashMap<&str, &Value> = graph["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| (node["id"].as_str().unwrap(), node))
        .collect();
    for root in ["cairn-channels"] {
        let root_id = *packages.iter().find(|(_, name)| **name == root).unwrap().0;
        let mut pending = vec![root_id];
        let mut visited = HashSet::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            let name = packages[id];
            assert!(
                !matches!(
                    name,
                    "cairn-core" | "cairn-db" | "cairn-transport" | "cairn-checkenv" | "turso"
                ),
                "{root} must not reach runtime dependency {name}"
            );
            for dependency in nodes[id]["deps"].as_array().unwrap() {
                // Include normal/build edges everywhere and dev edges at the
                // root. Dependency crates' tests are not part of this build.
                if dependency["dep_kinds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|kind| id == root_id || kind["kind"] != "dev")
                {
                    pending.push(dependency["pkg"].as_str().unwrap());
                }
            }
        }
    }
}
