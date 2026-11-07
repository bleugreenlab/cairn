//! The migration's ownership table, row by row, on a v2 home.

use super::*;
use crate::{install, lock as pack_lock, manifest};
use cairn_platform::services::{RealFileSystem, RealGitClient};
use std::path::PathBuf;

fn write(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn sha(text: &str) -> String {
    crate::sha256_bytes(text.as_bytes())
}

const SHIPPED: &str = "shipped\n";

struct Home {
    _temp: tempfile::TempDir,
    resources: PathBuf,
    config: PathBuf,
}

/// A bundle with pack `core` shipping five agents and a skill, and a v2 home
/// holding their flat copies in every pre-upgrade state.
fn v2_home() -> Home {
    let temp = tempfile::tempdir().unwrap();
    let resources = temp.path().join("res");
    let config = temp.path().join("home");
    let core = resources.join("packs/core");
    write(
        &core.join("cairn-pack.yaml"),
        "id: core\nname: Core\nversion: 2.0.0\ndefault: true\n",
    );
    for agent in ["forked", "owned", "edited", "legacy", "gone", "removed"] {
        write(&core.join(format!("agents/{agent}.md")), SHIPPED);
    }
    write(&core.join("skills/notes/SKILL.md"), SHIPPED);

    // Flat copies as a v2 sync left them.
    write(&config.join("agents/forked.md"), "my fork\n");
    write(&config.join("agents/owned.md"), SHIPPED);
    write(&config.join("agents/edited.md"), "edited since sync\n");
    write(&config.join("agents/legacy.md"), SHIPPED);
    write(&config.join("agents/removed.md"), "my own after removing\n");
    write(
        &config.join("agents/retired.md"),
        "a fork of a retired item\n",
    );
    write(&config.join("skills/notes/SKILL.md"), SHIPPED);
    write(&config.join("skills/notes/.DS_Store"), "finder");
    write(
        &config.join("agent-pins.yaml"),
        "agents:\n  forked:\n    icon: hammer\n",
    );

    let legacy_tree = crate::hash_tree_present(&config.join("skills/notes"), true).unwrap();
    let lock = format!(
        "cairnVersion: 2\nid: core\nname: Core\nversion: 1.0.0\ninstalledAt: now\ncontentHash: old\nsource:\n  kind: bundled\n  format: cairn\nitems:\n\
         - kind: agent\n  id: edited\n  path: agents/edited.md\n  contentHash: {shipped}\n\
         - kind: agent\n  id: forked\n  path: agents/forked.md\n  contentHash: {shipped}\n  forked: true\n\
         - kind: agent\n  id: gone\n  path: agents/gone.md\n  contentHash: {shipped}\n\
         - kind: agent\n  id: legacy\n  path: agents/legacy.md\n\
         - kind: agent\n  id: owned\n  path: agents/owned.md\n  contentHash: {shipped}\n\
         - kind: skill\n  id: notes\n  path: skills/notes\n  contentHash: {legacy_tree}\n\
         removed:\n- kind: agent\n  id: removed\n  path: agents/removed.md\n",
        shipped = sha(SHIPPED),
    );
    write(&pack_lock::lock_path(&config, "core"), &lock);
    Home {
        _temp: temp,
        resources,
        config,
    }
}

#[test]
fn every_row_of_the_ownership_table() {
    let home = v2_home();
    let manifest = manifest::load(&home.resources.join("packs/core")).unwrap();
    let migration = migrate_pack(&RealGitClient, &RealFileSystem, &home.config, &manifest).unwrap();
    assert_eq!(
        migration.deleted,
        vec![
            "agents/legacy.md".to_string(),
            "agents/owned.md".to_string(),
            "skills/notes".to_string(),
        ],
        "only copies proven to be the pack's go: a baseline match (the skill through its pre-exclusion tree hash) or, with no baseline, the shipped bytes"
    );
    for kept in [
        "agents/forked.md",
        "agents/edited.md",
        "agents/removed.md",
        "agents/retired.md",
        "agent-pins.yaml",
    ] {
        assert!(home.config.join(kept).exists(), "{kept} is the user's");
    }
}

#[test]
fn the_first_v3_sync_migrates_stages_and_rewrites_the_lock() {
    let home = v2_home();
    let result = install::sync_workspace_packs(
        &RealGitClient,
        &RealFileSystem,
        &home.resources,
        &home.config,
        "2.0.0",
    )
    .unwrap();
    assert!(result
        .changed_paths
        .contains(&"agents/owned.md".to_string()));
    assert!(!home.config.join("agents/owned.md").exists());
    assert!(home.config.join("packs/core/agents/owned.md").is_file());
    let lock = pack_lock::read_lock(&home.config, "core").unwrap();
    assert_eq!(lock.cairn_version, pack_lock::LOCK_VERSION);
    assert!(lock.is_removed(PackItemKind::Agent, "removed"));
    let text = std::fs::read_to_string(pack_lock::lock_path(&home.config, "core")).unwrap();
    assert!(!text.contains("forked: true"), "{text}");

    // Migration runs once: the next sync finds a v3 lock and leaves the
    // user's files alone.
    std::fs::write(home.config.join("agents/legacy.md"), SHIPPED).unwrap();
    install::sync_workspace_packs(
        &RealGitClient,
        &RealFileSystem,
        &home.resources,
        &home.config,
        "2.0.0",
    )
    .unwrap();
    assert!(home.config.join("agents/legacy.md").exists());
}

#[test]
fn a_path_last_committed_by_a_sync_is_the_packs_without_a_baseline() {
    for subject in BUNDLE_COMMIT_SUBJECTS {
        assert!(is_pack_authored_subject(subject));
    }
    assert!(is_pack_authored_subject("Sync pack resources: core"));
    assert!(is_pack_authored_subject("Update bundled defaults to 0.8.7"));
    assert!(!is_pack_authored_subject("Snapshot workspace config"));
    assert!(!is_pack_authored_subject("Merge branch 'bundle'"));
    assert!(!is_pack_authored_subject("User edits explore"));
}

/// A machine service's mirror goes when it is the pack's and is kept, with a
/// note, when it was edited, since a service never runs from it.
#[test]
fn a_service_mirror_is_deleted_or_kept_and_noted() {
    let temp = tempfile::tempdir().unwrap();
    let resources = temp.path().join("res");
    let config = temp.path().join("home");
    let axon = resources.join("packs/axon");
    write(
        &axon.join("cairn-pack.yaml"),
        "id: axon\nname: Axon\nversion: 1.0.0\n",
    );
    write(&axon.join("services/axon/service.yaml"), SHIPPED);
    write(&axon.join("services/probe/service.yaml"), SHIPPED);
    write(&config.join("services/axon/service.yaml"), SHIPPED);
    write(&config.join("services/probe/service.yaml"), "edited\n");
    let tree = crate::hash_tree_present(&axon.join("services/axon"), false).unwrap();
    write(
        &pack_lock::lock_path(&config, "axon"),
        &format!(
            "cairnVersion: 2\nid: axon\nname: Axon\nversion: 1.0.0\ninstalledAt: now\ncontentHash: old\nsource:\n  kind: bundled\nitems:\n- kind: service\n  id: axon\n  path: services/axon\n  contentHash: {tree}\n- kind: service\n  id: probe\n  path: services/probe\n  contentHash: {tree}\n"
        ),
    );
    let manifest = manifest::load(&axon).unwrap();
    let migration = migrate_pack(&RealGitClient, &RealFileSystem, &config, &manifest).unwrap();
    assert_eq!(migration.deleted, vec!["services/axon".to_string()]);
    assert!(config.join("services/probe/service.yaml").exists());
    assert!(
        migration.notes[0].contains("never run"),
        "{:?}",
        migration.notes
    );
}
