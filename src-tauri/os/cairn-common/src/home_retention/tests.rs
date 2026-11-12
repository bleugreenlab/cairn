use super::*;

fn all_entries() -> impl Iterator<Item = (&'static str, &'static HomeEntry)> {
    HOME_RETENTION.iter().flat_map(|top| {
        std::iter::once(("", top)).chain(top.children.iter().map(move |child| (top.name, child)))
    })
}

#[test]
fn the_registry_is_well_formed() {
    let issue = regex::Regex::new(r"^cairn://p/[a-z0-9-]+/[0-9]+$").unwrap();
    let mut seen = std::collections::HashSet::new();
    for (parent, entry) in all_entries() {
        assert!(
            seen.insert((parent, entry.name)),
            "{parent}/{} is registered twice",
            entry.name
        );
        assert!(!entry.writer.is_empty(), "{} names no writer", entry.name);
        assert!(!entry.owner.is_empty(), "{} names no owner", entry.name);
        if let Some(named) = entry.rule.issue() {
            assert!(
                issue.is_match(named),
                "{parent}/{} is Undecided but names {named:?}, not a cairn://p/<project>/<number> issue",
                entry.name
            );
        }
    }
    // Every name the executor layout spells is a line here.
    for name in [
        crate::runtime::BUILD_SLOTS_DIR,
        crate::executor_paths::MANAGED_OBJECTS_DIR,
        crate::executor_paths::DEPENDENCY_CATALOG_DIR,
        crate::executor_paths::TOOL_CACHE_DIR,
        crate::executor_paths::SCRATCH_TREES_DIR,
        crate::executor_paths::CACHE_HOME_DIR,
        crate::executor_paths::BUILD_ROOT_MARKER,
        crate::imports::IMPORTS_ROOT_DIR,
    ] {
        assert!(classify(name, None).is_some(), "{name} is not registered");
    }
    assert_eq!(
        classify(
            crate::runtime::BUILD_SLOTS_DIR,
            Some(crate::executor_paths::AUTHORITY_DIR)
        )
        .map(|entry| entry.name),
        Some(crate::executor_paths::AUTHORITY_DIR)
    );
    for name in FORMER_BUILD_ROOT_ENTRIES {
        assert!(classify(name, None).is_some(), "{name} is not registered");
    }
}

#[test]
fn entries_match_exact_names_and_children_refine_their_parent() {
    assert_eq!(classify("orch.lock", None).unwrap().name, "orch.lock");
    assert_eq!(classify("orch.err.log", None).unwrap().name, "orch.err.log");
    assert_eq!(classify("cairn.db-wal", None).unwrap().name, "cairn.db-wal");
    assert_eq!(classify("cairn.search", None).unwrap().name, "cairn.search");
    // A name that merely shares a registered entry's prefix is nothing that
    // entry's enforcer reaches: an unrelated backup or directory stays
    // unclaimed, so the walk names it once it is large.
    for unrelated in [
        "cairn.db.backup",
        "cairn.db-old",
        "orch-old-builds",
        "orch.err.log.2",
        "logs-archive",
    ] {
        assert_eq!(classify(unrelated, None), None, "{unrelated}");
    }
    assert_eq!(
        classify("claude-profiles", Some("_transcripts"))
            .unwrap()
            .rule
            .issue(),
        Some("cairn://p/cairn/6759")
    );
    assert_eq!(
        classify("claude-profiles", Some("acc_1")).unwrap().name,
        "claude-profiles"
    );
    assert_eq!(classify("mystery", None), None);
}

/// The production home's top-level listing: the 11-09 census (performance
/// journal 53, cairn://p/cairn/performance/db/journal/53) and the local
/// executor's home as listed on 11-12, less the names no current code writes
/// (below), plus the executor-only names a remote executor's home holds.
const PRODUCTION_HOME: &[&str] = &[
    ".cairn-build-root",
    ".cairn-runtime.lock",
    ".git",
    ".gitignore",
    ".storage-reserve",
    "AGENTS.md",
    "agents",
    "archive",
    "backups",
    "bin",
    "build-slot-residency-routes.json",
    "build-slots",
    "cache-home",
    "cairn-exec.replaced",
    "cairn.db",
    "cairn.db-log",
    "cairn.db-wal",
    "cairn.search",
    "check-slices",
    "claude-profiles",
    "codex",
    "credential",
    "credential-leases",
    "credentials.yaml",
    "dependency-catalog",
    "executor-artifacts",
    "executor-listener",
    "executor.json",
    "executor_id",
    "hook-settings.json",
    "imports",
    "inference",
    "jj",
    "jj-stores",
    "keybinds.json",
    "keychain-read-health.json",
    "library",
    "logs",
    "managed-objects",
    "mcp_auth_secret",
    "mcp_tools.json",
    "models",
    "node-scratch-store",
    "object-upload-staging",
    "operator_auth_secret",
    "orch.err.log",
    "orch.err.log.1",
    "orch.lock",
    "orch.out.log",
    "packs",
    "plugin-data",
    "rebuilds",
    "recipes",
    "references",
    "responses",
    "routes",
    "runner.lock",
    "runtime",
    "scratch",
    "served-api",
    "services",
    "settings.yaml",
    "shell_integration",
    "skills",
    "sock",
    "state",
    "teams",
    "tier-grid.retired.yaml",
    "tmp",
    "tool-cache",
    "tools",
    "turn-checks",
    "update-attempt.json",
    "uv-cache",
    "voice",
    "voice-components.json",
    "warm-caches",
    "workflows",
    "worktrees",
];

/// Names in the local executor's home on 11-12 that no current code writes:
/// retired writers' leftovers and the operator's own files. They stay
/// unclaimed on purpose, so the walk names any of them that grows past the
/// threshold instead of the registry vouching for bytes nothing bounds.
const WRITERLESS_LEFTOVERS: &[&str] = &[
    "cairn-dev.db",
    "cairn.db.db-log",
    "cairn.turso.db",
    "cargo-bin-watch.log",
    "claude-usage-probe",
    "mcp-config.json",
    "runner.json",
    "runner.out.log",
    "self-rebuild.log",
    "settings.yaml.bak-20261013-1640",
    "shims",
    "turn-check-clones",
    "warm-search-bin",
];

fn home_with(names: &[&str]) -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    for name in names {
        let path = home.path().join(name);
        if name.contains('.') && !name.starts_with('.') || name.ends_with(".lock") {
            std::fs::write(path, b"").unwrap();
        } else {
            std::fs::create_dir(path).unwrap();
        }
    }
    home
}

#[test]
fn a_production_shaped_home_is_fully_claimed() {
    let home = home_with(PRODUCTION_HOME);
    assert_eq!(
        unclaimed_entries(home.path()).unwrap(),
        Vec::<String>::new(),
        "every top-level entry of a production home has a registered writer and rule"
    );

    std::fs::create_dir(home.path().join("mystery")).unwrap();
    assert_eq!(
        unclaimed_entries(home.path()).unwrap(),
        vec!["mystery".to_string()],
        "an entry no writer registered is named"
    );
}

#[test]
fn writerless_leftovers_stay_unclaimed() {
    let home = home_with(WRITERLESS_LEFTOVERS);
    let mut expected = WRITERLESS_LEFTOVERS
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(unclaimed_entries(home.path()).unwrap(), expected);
}
