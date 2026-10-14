//! A workflow engine must build and test without importing its application host.
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[test]
fn workflow_runtime_and_tests_do_not_depend_on_application_hosts() {
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
    for root in ["cairn-workflow"] {
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
                    "cairn-core"
                        | "cairn"
                        | "cairn-transport"
                        | "cairn-orch"
                        | "cairn-server"
                        | "cairn-checkenv"
                        | "cairn-session"
                ),
                "{root} must not reach application host {name}"
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
