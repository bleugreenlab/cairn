//! Conservative test-target selection planner.
//!
//! Given the set of changed files and a project's `checks` contract, this module
//! decides, for each check, whether it applies and exactly what command to run.
//! It is the input the runner slice consumes; it executes nothing, touches no
//! cache, and does no streaming. Keeping that boundary is deliberate: a pure
//! planner is cheap to unit-test and keeps the runner clean.
//!
//! ## Conservatism is the load-bearing rule
//!
//! Over-including targets only wastes work; UNDER-including risks skipping a
//! check that should have run. So whenever `{targets}` resolution is uncertain
//! (a changed file maps to no known crate, or `cargo metadata` fails or won't
//! parse), the planner degrades the placeholder to an empty string — running the
//! command as a whole-suite full run — rather than guessing a narrow set.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::config::project_settings::CheckCommand;
use crate::jj::GraphFileChange;

/// Placeholder in a `related`-mode command, substituted with the relevant
/// changed files.
const CHANGED_FILES_PLACEHOLDER: &str = "{changedFiles}";
/// Placeholder substituted with the resolved crate-graph target arguments
/// (e.g. `-p crateA -p crateB`).
const TARGETS_PLACEHOLDER: &str = "{targets}";

/// Whether a planned run covers the whole check or a selected subset.
///
/// The inheritance step needs this distinction: a `Full` run establishes the
/// authoritative status for every target, while a `Partial` run only refreshes
/// the selected subset and leaves the rest to inherit from a baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CheckScope {
    Full,
    Partial,
}

/// Whether any path intersects a check's impact set. Invalid globs are treated
/// conservatively as a match so callers never reuse evidence on uncertainty.
pub(crate) fn paths_match_impact(globs: &[String], paths: &[String]) -> bool {
    let changes: Vec<GraphFileChange> = paths
        .iter()
        .map(|path| GraphFileChange {
            path: path.clone(),
            previous_path: None,
            status: "modified".to_string(),
            additions: 0,
            deletions: 0,
        })
        .collect();
    !matches!(matched_paths(globs, &changes), MatchOutcome::Matched(paths) if paths.is_empty())
}

/// The decision for a single check: whether it applies to this change set and,
/// if so, the concrete command to run and whether that run is full or partial.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckPlan {
    /// The check's name (its key in the `checks` map).
    pub(crate) name: String,
    /// Whether this check applies to the change set. A check that does not apply
    /// is skipped by the runner; its `command`/`scope` are populated with the
    /// full-run defaults but unused.
    pub(crate) applies: bool,
    /// The concrete command to run, with any `{changedFiles}`/`{targets}`
    /// placeholder already substituted.
    pub(crate) command: String,
    /// Whether `command` runs the whole check or a selected subset.
    pub(crate) scope: CheckScope,
    /// Process-wide admission class used immediately before process spawn.
    pub(crate) resource_class: crate::config::project_settings::CheckResourceClass,
    /// Whether the command executes locally and therefore needs the runner-local semaphore.
    pub(crate) requires_runner_local_admission: bool,
}

/// Plan every check against a change set.
///
/// Returns one [`CheckPlan`] per check, ordered by check name for determinism
/// (the input is an unordered map). `repo_root` is the worktree root the
/// `changed_files` paths are relative to; it anchors crate-graph resolution.
pub(crate) fn plan_checks(
    checks: &HashMap<String, CheckCommand>,
    changed_files: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    let mut plans: Vec<CheckPlan> = checks
        .iter()
        .map(|(name, check)| plan_one(name, check, changed_files, repo_root))
        .collect();
    plans.sort_by(|a, b| a.name.cmp(&b.name));
    plans
}

/// Plan a single check. Split out so the per-check rules read top-to-bottom.
fn plan_one(
    name: &str,
    check: &CheckCommand,
    changed_files: &[GraphFileChange],
    repo_root: &Path,
) -> CheckPlan {
    let full_plan = |scope: CheckScope, applies: bool| CheckPlan {
        name: name.to_string(),
        applies,
        command: check.command.clone(),
        scope,
        resource_class: check.resource_class,
        requires_runner_local_admission: true,
    };

    // Coarse gate: does this check apply at all? With no `impact`, any change
    // triggers it; with `impact` globs, only an intersecting change does. An
    // invalid glob is treated conservatively as a match (apply, run full).
    let matched: Vec<String> = match &check.impact {
        None => {
            if changed_files.is_empty() {
                return full_plan(CheckScope::Full, false);
            }
            all_candidate_paths(changed_files)
        }
        Some(globs) => match matched_paths(globs, changed_files) {
            MatchOutcome::Matched(paths) if !paths.is_empty() => paths,
            MatchOutcome::Matched(_) => return full_plan(CheckScope::Full, false),
            // Glob compilation failed: we cannot prove non-application, so apply
            // and run the full command conservatively.
            MatchOutcome::GlobError => return full_plan(CheckScope::Full, true),
        },
    };

    // The check applies. Selectivity is expressed by a placeholder inside the
    // command — the placeholder *is* the selector, and its resolver is implied.
    if check.command.contains(CHANGED_FILES_PLACEHOLDER) {
        // `{changedFiles}` → the impact-matched changed files.
        let command = substitute(&check.command, CHANGED_FILES_PLACEHOLDER, &join(&matched));
        CheckPlan {
            name: name.to_string(),
            applies: true,
            command,
            scope: CheckScope::Partial,
            resource_class: check.resource_class,
            requires_runner_local_admission: true,
        }
    } else if check.command.contains(TARGETS_PLACEHOLDER) {
        // `{targets}` → crate-graph targets resolved from the matched files. On
        // uncertain resolution the placeholder degrades to an empty string, which
        // naturally runs the whole suite (a conservative full run).
        match resolve_crate_targets(&matched, repo_root) {
            Some(targets) if !targets.is_empty() => {
                let args = targets
                    .iter()
                    .map(|c| format!("-p {c}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                let command = substitute(&check.command, TARGETS_PLACEHOLDER, &args);
                CheckPlan {
                    name: name.to_string(),
                    applies: true,
                    command,
                    scope: CheckScope::Partial,
                    resource_class: check.resource_class,
                    requires_runner_local_admission: true,
                }
            }
            _ => {
                let command = substitute(&check.command, TARGETS_PLACEHOLDER, "")
                    .trim()
                    .to_string();
                CheckPlan {
                    name: name.to_string(),
                    applies: true,
                    command,
                    scope: CheckScope::Full,
                    resource_class: check.resource_class,
                    requires_runner_local_admission: true,
                }
            }
        }
    } else {
        // No placeholder → run the command as-is (a full run).
        full_plan(CheckScope::Full, true)
    }
}

/// Outcome of matching changed files against a check's impact globs.
enum MatchOutcome {
    /// Globs compiled; carries the changed paths that matched (possibly empty).
    Matched(Vec<String>),
    /// A glob pattern failed to compile.
    GlobError,
}

/// Every candidate path across the change set, deduped and sorted. A rename
/// contributes both its new and previous path so a file moved out of a crate
/// still counts against that crate.
fn all_candidate_paths(changed_files: &[GraphFileChange]) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for change in changed_files {
        set.insert(change.path.clone());
        if let Some(prev) = &change.previous_path {
            set.insert(prev.clone());
        }
    }
    set.into_iter().collect()
}

/// The candidate paths that match any of the impact globs, deduped and sorted.
/// Reuses the same globset matcher the worktree-populate rules use rather than
/// adding a second glob semantics. `literal_separator(false)` mirrors populate,
/// so `**`/`*` cross path separators the same way.
fn matched_paths(globs: &[String], changed_files: &[GraphFileChange]) -> MatchOutcome {
    let set = match build_glob_set(globs) {
        Ok(set) => set,
        Err(_) => return MatchOutcome::GlobError,
    };
    let matched = all_candidate_paths(changed_files)
        .into_iter()
        .filter(|p| set.is_match(p))
        .collect();
    MatchOutcome::Matched(matched)
}

/// The content identity of the tree entries a check's impact globs select — its
/// "input hash". `entries` are `(path, blob_id)` pairs from the sealed tree; the
/// matching subset is hashed (sorted for order-independence) so the value changes
/// iff a matching file's content changes or the matched path set changes. Reuses
/// [`build_glob_set`], so glob semantics are byte-identical to the application
/// gate above (`literal_separator(false)`) — one glob semantics in the codebase.
/// A glob-compile error conservatively includes every entry (over-invalidate,
/// never a false reuse).
pub(crate) fn check_input_hash(entries: &[(String, String)], globs: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let matcher = build_glob_set(globs).ok();
    let mut matched: Vec<&(String, String)> = entries
        .iter()
        .filter(|(path, _)| {
            matcher
                .as_ref()
                .map(|set| set.is_match(path))
                .unwrap_or(true)
        })
        .collect();
    matched.sort();
    let mut hasher = Sha256::new();
    for (path, blob) in matched {
        hasher.update(path.as_bytes());
        hasher.update([0u8]);
        hasher.update(blob.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

/// Build a [`globset::GlobSet`] from pattern strings, matching the worktree
/// populate matcher's `literal_separator(false)` semantics.
pub(crate) fn build_glob_set(patterns: &[String]) -> Result<globset::GlobSet, String> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        let glob = globset::GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|e| format!("Invalid glob pattern '{pattern}': {e}"))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|e| format!("Failed to build glob set: {e}"))
}

fn join(paths: &[String]) -> String {
    paths.join(" ")
}

fn substitute(template: &str, placeholder: &str, value: &str) -> String {
    template.replace(placeholder, value)
}

// ---------------------------------------------------------------------------
// Crate-graph resolver (`targets_from: crate-graph`)
// ---------------------------------------------------------------------------

/// Resolve the affected cargo workspace members for a set of changed files.
///
/// Runs `cargo metadata` over the Rust workspace at `repo_root/src-tauri`, maps
/// each changed file to its owning member, then expands to the transitive
/// reverse-dependency closure within the workspace (a change in crate X affects
/// X and every member that transitively depends on X).
///
/// Returns `None` whenever resolution is uncertain — metadata fails or won't
/// parse, or a changed file maps to no member — so the caller falls back to a
/// full run rather than under-selecting.
fn resolve_crate_targets(changed_files: &[String], repo_root: &Path) -> Option<Vec<String>> {
    let rust_root = repo_root.join("src-tauri");
    let metadata_json = run_cargo_metadata(&rust_root)?;
    resolve_crate_targets_from_metadata(&metadata_json, changed_files, repo_root)
}

/// Run `cargo metadata --format-version 1 --no-deps` in `rust_root`, returning
/// its stdout JSON, or `None` on any spawn/exit failure. `--no-deps` restricts
/// `packages` to workspace members, which is all the reverse-dependency closure
/// needs and keeps the call fast and hermetic.
fn run_cargo_metadata(rust_root: &Path) -> Option<String> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(rust_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

/// Minimal projection of `cargo metadata` output. Unknown fields are ignored.
#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    name: String,
    manifest_path: String,
    #[serde(default)]
    dependencies: Vec<CargoDependency>,
}

#[derive(Debug, Deserialize)]
struct CargoDependency {
    name: String,
}

/// Pure core of the crate-graph resolver: parse metadata JSON, map changed files
/// to members, and expand to the reverse-dependency closure. Split from the
/// `cargo` invocation so it can be unit-tested with a fixture without shelling
/// out. Returns `None` on the same uncertainty conditions as
/// [`resolve_crate_targets`].
fn resolve_crate_targets_from_metadata(
    metadata_json: &str,
    changed_files: &[String],
    repo_root: &Path,
) -> Option<Vec<String>> {
    let metadata: CargoMetadata = serde_json::from_str(metadata_json).ok()?;

    // Member name -> package directory (the manifest's parent).
    let member_dirs: Vec<(String, PathBuf)> = metadata
        .packages
        .iter()
        .filter_map(|pkg| {
            Path::new(&pkg.manifest_path)
                .parent()
                .map(|dir| (pkg.name.clone(), dir.to_path_buf()))
        })
        .collect();
    let member_names: BTreeSet<&str> = metadata.packages.iter().map(|p| p.name.as_str()).collect();

    // Map each changed file to its owning member by longest package-dir prefix
    // (deepest wins, so a file in a nested member is attributed to that member,
    // not an ancestor workspace member). Any unmappable file is uncertain.
    let mut seeds: BTreeSet<String> = BTreeSet::new();
    for file in changed_files {
        let file_abs = repo_root.join(file);
        let owner = member_dirs
            .iter()
            .filter(|(_, dir)| file_abs.starts_with(dir))
            .max_by_key(|(_, dir)| dir.components().count());
        match owner {
            Some((name, _)) => {
                seeds.insert(name.clone());
            }
            None => return None,
        }
    }

    // Reverse edges: member -> members that depend on it (workspace-internal
    // only). Built from each package's declared dependencies filtered to members.
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for pkg in &metadata.packages {
        for dep in &pkg.dependencies {
            if member_names.contains(dep.name.as_str()) {
                dependents
                    .entry(dep.name.as_str())
                    .or_default()
                    .push(pkg.name.as_str());
            }
        }
    }

    // Transitive reverse-dependency closure over the seeds.
    let mut affected: BTreeSet<String> = BTreeSet::new();
    let mut stack: Vec<String> = seeds.into_iter().collect();
    while let Some(crate_name) = stack.pop() {
        if !affected.insert(crate_name.clone()) {
            continue;
        }
        if let Some(rdeps) = dependents.get(crate_name.as_str()) {
            for rdep in rdeps {
                if !affected.contains(*rdep) {
                    stack.push((*rdep).to_string());
                }
            }
        }
    }

    Some(affected.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(path: &str, status: &str) -> GraphFileChange {
        GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: status.to_string(),
            additions: 1,
            deletions: 0,
        }
    }

    fn rename(from: &str, to: &str) -> GraphFileChange {
        GraphFileChange {
            path: to.to_string(),
            previous_path: Some(from.to_string()),
            status: "renamed".to_string(),
            additions: 0,
            deletions: 0,
        }
    }

    fn check(command: &str, impact: Option<&[&str]>) -> CheckCommand {
        CheckCommand {
            command: command.to_string(),
            impact: impact.map(|globs| globs.iter().map(|s| s.to_string()).collect()),
            policy: crate::config::project_settings::CheckPolicy::Advisory,
            when: crate::config::project_settings::CheckWhen::Write,
            resource_class: crate::config::project_settings::CheckResourceClass::Shared,
            timeout: None,
            constraints: None,
        }
    }

    fn plan_for(check: CheckCommand, changed: &[GraphFileChange]) -> CheckPlan {
        let mut map = HashMap::new();
        map.insert("check".to_string(), check);
        plan_checks(&map, changed, Path::new("/repo"))
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn manual_cache_dirt_only_invalidates_matching_impact() {
        let globs = vec!["src-tauri/**/*.rs".to_string(), "package.json".to_string()];
        assert!(paths_match_impact(
            &globs,
            &["src-tauri/os/cairn-core/src/lib.rs".to_string()]
        ));
        assert!(!paths_match_impact(&globs, &["docs/checks.md".to_string()]));
        assert!(paths_match_impact(
            &["[invalid".to_string()],
            &["docs/checks.md".to_string()]
        ));
    }

    // --- coarse gate: applies / does-not-apply -----------------------------

    #[test]
    fn impact_glob_applies_when_a_changed_file_matches() {
        let c = check("cargo test", Some(&["src-tauri/**/*.rs"]));
        let plan = plan_for(
            c,
            &[change("src-tauri/os/cairn-core/src/lib.rs", "modified")],
        );
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "cargo test");
    }

    #[test]
    fn impact_glob_does_not_apply_when_no_changed_file_matches() {
        let c = check("cargo test", Some(&["src-tauri/**/*.rs"]));
        let plan = plan_for(c, &[change("web/src/App.tsx", "modified")]);
        assert!(!plan.applies);
    }

    #[test]
    fn no_impact_applies_to_any_change() {
        let c = check("cargo test", None);
        let plan = plan_for(c, &[change("anything.md", "modified")]);
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
    }

    #[test]
    fn no_impact_does_not_apply_to_empty_change_set() {
        let c = check("cargo test", None);
        let plan = plan_for(c, &[]);
        assert!(!plan.applies);
    }

    #[test]
    fn invalid_glob_falls_back_to_full_and_applies() {
        // An unclosed bracket is an invalid glob.
        let c = check("cargo test", Some(&["src-tauri/["]));
        let plan = plan_for(c, &[change("src-tauri/x.rs", "modified")]);
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "cargo test");
    }

    // --- no placeholder -> full --------------------------------------------

    #[test]
    fn no_placeholder_runs_full() {
        let c = check("bun run check", None);
        let plan = plan_for(c, &[change("src/App.tsx", "modified")]);
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "bun run check");
    }

    // --- {changedFiles} substitution ---------------------------------------

    #[test]
    fn related_substitutes_matched_changed_files() {
        let c = check(
            "vitest related {changedFiles}",
            Some(&["**/*.ts", "**/*.tsx"]),
        );
        let plan = plan_for(
            c,
            &[
                change("src/a.ts", "modified"),
                change("src/b.tsx", "added"),
                change("README.md", "modified"),
            ],
        );
        assert!(plan.applies);
        assert_eq!(plan.scope, CheckScope::Partial);
        // README.md is excluded; matched paths are sorted.
        assert_eq!(plan.command, "vitest related src/a.ts src/b.tsx");
    }

    #[test]
    fn related_with_no_impact_uses_all_changed_files() {
        let c = check("vitest related {changedFiles}", None);
        let plan = plan_for(
            c,
            &[change("src/b.ts", "modified"), change("src/a.ts", "added")],
        );
        assert_eq!(plan.scope, CheckScope::Partial);
        assert_eq!(plan.command, "vitest related src/a.ts src/b.ts");
    }

    #[test]
    fn related_rename_includes_previous_path() {
        let c = check("vitest related {changedFiles}", Some(&["src/**/*.ts"]));
        let plan = plan_for(c, &[rename("src/old.ts", "src/new.ts")]);
        assert_eq!(plan.command, "vitest related src/new.ts src/old.ts");
    }

    // --- targets: uncertain resolution degrades to a full run --------------

    #[test]
    fn targets_uncertain_resolution_degrades_to_full_suite() {
        // repo_root has no cargo workspace here, so crate-graph resolution is
        // uncertain; the {targets} placeholder degrades to empty and the command
        // runs as the whole suite (a conservative full run).
        let c = check("bun run test:rust {targets}", Some(&["src-tauri/**/*.rs"]));
        let plan = plan_for(c, &[change("src-tauri/x.rs", "modified")]);
        assert_eq!(plan.scope, CheckScope::Full);
        assert_eq!(plan.command, "bun run test:rust");
    }

    // --- deterministic ordering of multiple checks -------------------------

    // --- per-check input hash ---------------------------------------------

    #[test]
    fn check_input_hash_ignores_non_matching_files() {
        let globs = vec!["src-tauri/**".to_string()];
        let base = vec![
            ("src-tauri/a.rs".to_string(), "blobA".to_string()),
            ("docs/x.md".to_string(), "blobX".to_string()),
        ];
        // Changing a NON-matching (doc) blob leaves the input hash unchanged.
        let doc_changed = vec![
            ("src-tauri/a.rs".to_string(), "blobA".to_string()),
            ("docs/x.md".to_string(), "blobY".to_string()),
        ];
        assert_eq!(
            check_input_hash(&base, &globs),
            check_input_hash(&doc_changed, &globs),
            "a doc-only change must not alter a src-tauri check's input hash"
        );
        // Changing a MATCHING blob changes it.
        let src_changed = vec![
            ("src-tauri/a.rs".to_string(), "blobB".to_string()),
            ("docs/x.md".to_string(), "blobX".to_string()),
        ];
        assert_ne!(
            check_input_hash(&base, &globs),
            check_input_hash(&src_changed, &globs)
        );
    }

    #[test]
    fn check_input_hash_is_order_independent() {
        let globs = vec!["src/**".to_string()];
        let a = vec![
            ("src/a.ts".to_string(), "1".to_string()),
            ("src/b.ts".to_string(), "2".to_string()),
        ];
        let b = vec![
            ("src/b.ts".to_string(), "2".to_string()),
            ("src/a.ts".to_string(), "1".to_string()),
        ];
        assert_eq!(check_input_hash(&a, &globs), check_input_hash(&b, &globs));
    }

    #[test]
    fn plans_are_sorted_by_name() {
        let mut map = HashMap::new();
        map.insert("zebra".to_string(), check("z", None));
        map.insert("alpha".to_string(), check("a", None));
        map.insert("mike".to_string(), check("m", None));
        let plans = plan_checks(&map, &[change("x", "modified")], Path::new("/repo"));
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mike", "zebra"]);
    }

    // --- crate-graph resolver (hermetic, fixture metadata) -----------------

    /// A 4-member workspace mirroring this repo's shape: cairn-common is a leaf
    /// dependency; cairn-core and cairn-cmd depend on it; cairn (the app)
    /// depends on cairn-core. Manifest paths are anchored under `/repo`.
    fn fixture_metadata() -> String {
        serde_json::json!({
            "packages": [
                {
                    "name": "cairn-common",
                    "manifest_path": "/repo/src-tauri/os/cairn-common/Cargo.toml",
                    "dependencies": []
                },
                {
                    "name": "cairn-core",
                    "manifest_path": "/repo/src-tauri/os/cairn-core/Cargo.toml",
                    "dependencies": [{ "name": "cairn-common" }]
                },
                {
                    "name": "cairn-cmd",
                    "manifest_path": "/repo/src-tauri/os/cairn-cmd/Cargo.toml",
                    "dependencies": [{ "name": "cairn-common" }]
                },
                {
                    "name": "cairn",
                    "manifest_path": "/repo/src-tauri/Cargo.toml",
                    "dependencies": [
                        { "name": "cairn-core" },
                        { "name": "serde" }
                    ]
                }
            ]
        })
        .to_string()
    }

    fn resolve(files: &[&str]) -> Option<Vec<String>> {
        let owned: Vec<String> = files.iter().map(|s| s.to_string()).collect();
        resolve_crate_targets_from_metadata(&fixture_metadata(), &owned, Path::new("/repo"))
    }

    #[test]
    fn crate_graph_leaf_crate_selects_only_itself() {
        // cairn-cmd has no workspace dependents.
        let targets = resolve(&["src-tauri/os/cairn-cmd/src/main.rs"]).unwrap();
        assert_eq!(targets, vec!["cairn-cmd".to_string()]);
    }

    #[test]
    fn crate_graph_depended_on_crate_selects_transitive_dependents() {
        // cairn-common is depended on by cairn-core and cairn-cmd; cairn-core is
        // depended on by the app. So a change in cairn-common affects all four.
        let targets = resolve(&["src-tauri/os/cairn-common/src/uri.rs"]).unwrap();
        assert_eq!(
            targets,
            vec![
                "cairn".to_string(),
                "cairn-cmd".to_string(),
                "cairn-common".to_string(),
                "cairn-core".to_string(),
            ]
        );
    }

    #[test]
    fn crate_graph_nested_member_wins_over_ancestor_workspace_member() {
        // src-tauri/os/cairn-core/... is under both the cairn app dir (src-tauri)
        // and the cairn-core dir; the deeper member must win.
        let targets = resolve(&["src-tauri/os/cairn-core/src/lib.rs"]).unwrap();
        // cairn-core plus the app that depends on it.
        assert_eq!(targets, vec!["cairn".to_string(), "cairn-core".to_string()]);
    }

    #[test]
    fn crate_graph_app_crate_file_selects_only_app() {
        let targets = resolve(&["src-tauri/src/main.rs"]).unwrap();
        assert_eq!(targets, vec!["cairn".to_string()]);
    }

    #[test]
    fn crate_graph_unmappable_file_returns_none() {
        // A file outside every member directory cannot be attributed.
        assert!(resolve(&["web/src/App.tsx"]).is_none());
    }

    #[test]
    fn crate_graph_mixed_mappable_and_unmappable_returns_none() {
        assert!(resolve(&["src-tauri/os/cairn-cmd/src/main.rs", "docs/x.md"]).is_none());
    }

    #[test]
    fn crate_graph_malformed_metadata_returns_none() {
        let owned = vec!["src-tauri/os/cairn-cmd/src/main.rs".to_string()];
        assert!(
            resolve_crate_targets_from_metadata("not json", &owned, Path::new("/repo")).is_none()
        );
    }

    #[test]
    fn crate_graph_multiple_seeds_union_their_closures() {
        // A change in both cairn-cmd (leaf) and cairn-core (depended on by app).
        let targets = resolve(&[
            "src-tauri/os/cairn-cmd/src/main.rs",
            "src-tauri/os/cairn-core/src/lib.rs",
        ])
        .unwrap();
        assert_eq!(
            targets,
            vec![
                "cairn".to_string(),
                "cairn-cmd".to_string(),
                "cairn-core".to_string(),
            ]
        );
    }

    // --- end-to-end: targets mode through the planner ----------------------

    #[test]
    fn targets_crate_graph_substitutes_resolved_targets() {
        // Drive the full planner with a crate-graph targets check. Resolution
        // shells out to cargo metadata against repo_root/src-tauri; in the test
        // worktree that path is real, but to stay hermetic we instead assert the
        // pure resolver+substitution wiring via a direct command build.
        let resolved = resolve(&["src-tauri/os/cairn-cmd/src/main.rs"]).unwrap();
        let args = resolved
            .iter()
            .map(|c| format!("-p {c}"))
            .collect::<Vec<_>>()
            .join(" ");
        let command = substitute("cargo test {targets}", TARGETS_PLACEHOLDER, &args);
        assert_eq!(command, "cargo test -p cairn-cmd");
    }
}
