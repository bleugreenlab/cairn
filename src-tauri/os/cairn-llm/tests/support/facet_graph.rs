//! The cairn-llm facet's dependency boundary, checked against the Cargo graph.
//!
//! One implementation, included by `#[path]` from each crate that runs it: the
//! four public facet members, the two `os/` crates most likely to grow a
//! forbidden edge (`cairn-core`, `cairn-session`), the private
//! `cairn-llm-subscriptions`, and the external host in `examples/llm-host`. A
//! manifest-only change to any of them therefore runs the check under the
//! scoped test runner.
//!
//! The workspace is located through `cargo locate-project --workspace`, not a
//! relative path, and a forbidden crate absent from the workspace counts as
//! unreachable, so the same check runs unchanged in the public mirror where
//! the runtime and the private crate do not exist.
#![allow(dead_code)]

use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// The public facet crates. Everything they reach is publishable.
pub const PUBLIC_ROOTS: &[&str] = &[
    "cairn-llm",
    "cairn-llm-wire",
    "cairn-llm-types",
    "cairn-credentials",
];

/// Public members that must reach no workspace crate at all.
pub const LEAF_ROOTS: &[&str] = &["cairn-llm-wire", "cairn-llm-types"];

/// The runtime, the work layer, heavy configuration, and the private crate.
pub const PUBLIC_FORBIDDEN: &[&str] = &[
    "cairn-core",
    "cairn-db",
    "cairn-transport",
    "cairn-session",
    "cairn-analytics",
    "cairn-config",
    "cairn-checkenv",
    "cairn-orch",
    "cairn-server",
    "cairn-exec",
    // Tokenizers belong to the context-accounting host, not the facet: the
    // turn driver fits requests by `CHARS_PER_TOKEN`, never a tokenizer.
    "cairn-tokenize",
    "turso",
    PRIVATE_CRATE,
];

/// Subscription mechanics: outside `os/`, never mirrored.
pub const PRIVATE_CRATE: &str = "cairn-llm-subscriptions";

/// The private crate talks to the runtime only through the public seam.
pub const PRIVATE_FORBIDDEN: &[&str] = &[
    "cairn-core",
    "cairn-db",
    "cairn-transport",
    "cairn-session",
    "cairn-analytics",
    "turso",
];

struct Package {
    name: String,
    manifest: PathBuf,
}

pub struct Graph {
    packages: HashMap<String, Package>,
    members: HashSet<String>,
    /// Package id -> (dependency id, whether any edge kind is non-dev).
    edges: HashMap<String, Vec<(String, bool)>>,
}

fn metadata(workspace_manifest: &Path) -> Result<Value, String> {
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
        .arg(workspace_manifest)
        .output()
        .map_err(|err| format!("run cargo metadata: {err}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    serde_json::from_slice(&output.stdout).map_err(|err| err.to_string())
}

impl Graph {
    /// Load the resolved graph of the workspace that builds `manifest_dir`.
    ///
    /// That is the nearest ancestor workspace listing this crate as a member.
    /// In the repository that is `src-tauri/Cargo.toml`; in the public mirror
    /// it is the root `scripts/mirror-os.ts` writes at the same path; for a
    /// host outside both it is the host's own workspace. Walking up rather than asking
    /// `cargo locate-project` keeps all three layouts on one path, and skips a
    /// nearer `[workspace]` that does not build this crate.
    pub fn load(manifest_dir: &str) -> Graph {
        let crate_manifest = Path::new(manifest_dir).join("Cargo.toml");
        let mut failures = Vec::new();
        for dir in Path::new(manifest_dir).ancestors() {
            let candidate = dir.join("Cargo.toml");
            let is_workspace = std::fs::read_to_string(&candidate)
                .is_ok_and(|text| text.lines().any(|line| line.trim() == "[workspace]"));
            if !is_workspace {
                continue;
            }
            match metadata(&candidate) {
                Ok(graph) => {
                    let graph = Graph::from_metadata(&graph);
                    if graph
                        .members
                        .iter()
                        .any(|id| graph.packages[id].manifest == crate_manifest)
                    {
                        return graph;
                    }
                    failures.push(format!("{}: not a member", candidate.display()));
                }
                Err(err) => failures.push(format!("{}: {err}", candidate.display())),
            }
        }
        panic!(
            "no workspace above {manifest_dir} builds it:\n{}",
            failures.join("\n")
        );
    }

    /// Build from `cargo metadata --format-version 1` output.
    pub fn from_metadata(graph: &Value) -> Graph {
        let packages = graph["packages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|package| {
                (
                    package["id"].as_str().unwrap().to_string(),
                    Package {
                        name: package["name"].as_str().unwrap().to_string(),
                        manifest: PathBuf::from(package["manifest_path"].as_str().unwrap()),
                    },
                )
            })
            .collect();
        let members = graph["workspace_members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_string())
            .collect();
        let edges = graph["resolve"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| {
                let deps = node["deps"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|dep| {
                        let non_dev = dep["dep_kinds"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|kind| kind["kind"] != "dev");
                        (dep["pkg"].as_str().unwrap().to_string(), non_dev)
                    })
                    .collect();
                (node["id"].as_str().unwrap().to_string(), deps)
            })
            .collect();
        Graph {
            packages,
            members,
            edges,
        }
    }

    fn id_of(&self, name: &str) -> Option<&str> {
        self.members
            .iter()
            .find(|id| self.packages[id.as_str()].name == name)
            .or_else(|| {
                self.packages
                    .iter()
                    .find(|(_, package)| package.name == name)
                    .map(|(id, _)| id)
            })
            .map(String::as_str)
    }

    pub fn exists(&self, name: &str) -> bool {
        self.id_of(name).is_some()
    }

    /// The public source root: the directory holding the public facet crates,
    /// `src-tauri/os/` both in the monorepo and in the public mirror, which
    /// preserves the repository's paths. Anchored on where `cairn-llm` lives
    /// rather than on a directory name, so a host that vendors the crates
    /// elsewhere classifies alike.
    fn public_root(&self) -> Option<&Path> {
        let id = self.id_of("cairn-llm")?;
        self.packages[id].manifest.parent()?.parent()
    }

    /// Whether the package lives under the public source root.
    fn in_os(&self, id: &str) -> bool {
        self.public_root()
            .is_some_and(|root| self.packages[id].manifest.starts_with(root))
    }

    /// Everything `root` builds against: normal and build edges everywhere,
    /// dev edges at the root only (a dependency's own tests are not part of
    /// this build). Maps each reached id to the id that first reached it.
    fn closure(&self, root_id: &str) -> HashMap<String, Option<String>> {
        let mut reached: HashMap<String, Option<String>> = HashMap::new();
        reached.insert(root_id.to_string(), None);
        let mut pending = vec![root_id.to_string()];
        while let Some(id) = pending.pop() {
            for (dep, non_dev) in self.edges.get(&id).into_iter().flatten() {
                if (id == root_id || *non_dev) && !reached.contains_key(dep) {
                    reached.insert(dep.clone(), Some(id.clone()));
                    pending.push(dep.clone());
                }
            }
        }
        reached
    }

    fn chain(&self, reached: &HashMap<String, Option<String>>, id: &str) -> String {
        let mut names = vec![self.packages[id].name.clone()];
        let mut cursor = reached[id].clone();
        while let Some(parent) = cursor {
            names.push(self.packages[&parent].name.clone());
            cursor = reached[&parent].clone();
        }
        names.reverse();
        names.join(" -> ")
    }

    /// Violations of `root` reaching any of `forbidden`. A root or forbidden
    /// name missing from the workspace yields none.
    fn reaches(&self, root: &str, forbidden: &[&str]) -> Vec<String> {
        let Some(root_id) = self.id_of(root) else {
            return Vec::new();
        };
        let reached = self.closure(root_id);
        let mut violations: Vec<String> = reached
            .keys()
            .filter(|id| id.as_str() != root_id)
            .filter(|id| forbidden.contains(&self.packages[id.as_str()].name.as_str()))
            .map(|id| {
                format!(
                    "{root} reaches {}: {}",
                    self.packages[id.as_str()].name,
                    self.chain(&reached, id)
                )
            })
            .collect();
        violations.sort();
        violations
    }

    /// The public facet's rules. Returns every violation found.
    pub fn public_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        for root in PUBLIC_ROOTS {
            let Some(root_id) = self.id_of(root) else {
                violations.push(format!("public facet crate {root} is not in the workspace"));
                continue;
            };
            // 1. No runtime, work layer, configuration, or private crate.
            violations.extend(self.reaches(root, PUBLIC_FORBIDDEN));
            let reached = self.closure(root_id);
            for id in reached.keys().filter(|id| id.as_str() != root_id) {
                if !self.members.contains(id) {
                    continue;
                }
                let name = &self.packages[id.as_str()].name;
                // 2. The leaves reach no workspace crate at all.
                if LEAF_ROOTS.contains(root) {
                    violations.push(format!(
                        "{root} must reach no workspace crate, but reaches {name}: {}",
                        self.chain(&reached, id)
                    ));
                }
                // 3. The public closure is publishable: all of it lives in os/.
                if !self.in_os(id) {
                    violations.push(format!(
                        "{root} reaches {name}, which lives outside os/: {}",
                        self.chain(&reached, id)
                    ));
                }
            }
        }
        // 4. Nothing under os/, tests included, reaches the private crate.
        let mut os_members: Vec<&String> =
            self.members.iter().filter(|id| self.in_os(id)).collect();
        os_members.sort();
        for id in os_members {
            violations.extend(self.reaches(&self.packages[id.as_str()].name, &[PRIVATE_CRATE]));
        }
        violations.sort();
        violations.dedup();
        violations
    }

    /// The private crate's rules, on top of the public ones.
    pub fn private_violations(&self) -> Vec<String> {
        let mut violations = self.public_violations();
        violations.extend(self.reaches(PRIVATE_CRATE, PRIVATE_FORBIDDEN));
        // Not vacuous: in the full workspace every name either check relies on
        // must exist, so a rename cannot quietly turn a rule into a no-op.
        for name in PUBLIC_ROOTS
            .iter()
            .chain(PUBLIC_FORBIDDEN)
            .chain(PRIVATE_FORBIDDEN)
            .chain(std::iter::once(&PRIVATE_CRATE))
        {
            if !self.exists(name) {
                violations.push(format!(
                    "{name} is named by the facet boundary but absent from the workspace"
                ));
            }
        }
        violations.sort();
        violations.dedup();
        violations
    }
}

impl Graph {
    /// A host outside Cairn's workspace (the example in `examples/llm-host`,
    /// or an application built from Cairn's parts): its closure may hold the
    /// public facet and the private subscriptions crate, and nothing else of
    /// Cairn's runtime, database, session layer, or tokenizers.
    pub fn external_host_violations(&self, manifest_dir: &str) -> Vec<String> {
        let manifest = Path::new(manifest_dir).join("Cargo.toml");
        let Some(host) = self
            .members
            .iter()
            .find(|id| self.packages[id.as_str()].manifest == manifest)
            .map(|id| self.packages[id.as_str()].name.clone())
        else {
            return vec![format!(
                "{manifest_dir} is not a member of its own workspace"
            )];
        };
        let forbidden: Vec<&str> = PUBLIC_FORBIDDEN
            .iter()
            .copied()
            .filter(|name| *name != PRIVATE_CRATE)
            .collect();
        let mut violations = self.reaches(&host, &forbidden);
        // Not vacuous: an external host is one that uses the facet.
        if self.reaches(&host, &["cairn-llm"]).is_empty() {
            violations.push(format!("{host} does not reach cairn-llm"));
        }
        violations
    }
}

pub fn assert_external_host(manifest_dir: &str) {
    let violations = Graph::load(manifest_dir).external_host_violations(manifest_dir);
    assert!(
        violations.is_empty(),
        "external host boundary violated:\n  {}",
        violations.join("\n  ")
    );
}

pub fn assert_public_facet(manifest_dir: &str) {
    let violations = Graph::load(manifest_dir).public_violations();
    assert!(
        violations.is_empty(),
        "cairn-llm facet boundary violated:\n  {}",
        violations.join("\n  ")
    );
}

pub fn assert_private_facet(manifest_dir: &str) {
    let violations = Graph::load(manifest_dir).private_violations();
    assert!(
        violations.is_empty(),
        "cairn-llm facet boundary violated:\n  {}",
        violations.join("\n  ")
    );
}
