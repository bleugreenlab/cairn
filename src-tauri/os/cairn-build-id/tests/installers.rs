//! Every binary whose dependency closure reaches `cairn-checkenv` installs the
//! build's implementation identity. A binary that forgot would panic on its
//! first identity read, which for a rarely used path can be long after start.
use serde_json::Value;
use std::collections::{HashMap, HashSet};

const INSTALL: &str = "cairn_build_id::install();";

#[test]
fn every_binary_that_reaches_the_identity_installs_it() {
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
    let packages: HashMap<&str, &Value> = graph["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|package| (package["id"].as_str().unwrap(), package))
        .collect();
    let nodes: HashMap<&str, &Value> = graph["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| (node["id"].as_str().unwrap(), node))
        .collect();

    // Whether a target of `root` links cairn-checkenv: normal edges everywhere,
    // plus the root's dev edges for an example, which links them.
    let reaches_checkenv = |root: &str, with_dev: bool| {
        let mut pending = vec![root];
        let mut visited = HashSet::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            if packages[id]["name"] == "cairn-checkenv" {
                return true;
            }
            for dependency in nodes[id]["deps"].as_array().unwrap() {
                let followed = dependency["dep_kinds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|kind| {
                        kind["kind"].is_null() || (with_dev && id == root && kind["kind"] == "dev")
                    });
                if followed {
                    pending.push(dependency["pkg"].as_str().unwrap());
                }
            }
        }
        false
    };

    let mut checked = Vec::new();
    let mut missing = Vec::new();
    for member in graph["workspace_members"].as_array().unwrap() {
        let id = member.as_str().unwrap();
        let package = packages[id];
        for target in package["targets"].as_array().unwrap() {
            let kinds = target["kind"].as_array().unwrap();
            let example = kinds.iter().any(|kind| kind == "example");
            if !example && !kinds.iter().any(|kind| kind == "bin") {
                continue;
            }
            if !reaches_checkenv(id, example) {
                continue;
            }
            let name = format!(
                "{} {}",
                package["name"].as_str().unwrap(),
                target["name"].as_str().unwrap()
            );
            let source = std::fs::read_to_string(target["src_path"].as_str().unwrap()).unwrap();
            if !source.contains(INSTALL) {
                missing.push(name.clone());
            }
            checked.push(name);
        }
    }
    assert!(
        missing.is_empty(),
        "these binaries link cairn-checkenv but never call `{INSTALL}` in main: {missing:?}"
    );
    for expected in [
        "cairn-orch cairn-orch",
        "cairn-server cairn-server",
        "cairn-exec cairn-exec",
    ] {
        assert!(
            checked.iter().any(|name| name == expected),
            "{expected} no longer reaches cairn-checkenv; checked {checked:?}"
        );
    }
}
