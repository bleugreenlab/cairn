//! The one-time move from packs copied out into the workspace (lock v1 and v2)
//! to packs read in place (v3, tenet 1434).
//!
//! Before v3 a pack's items were materialized into the flat workspace
//! (`<home>/agents/build.md`) and owned by a per-item hash baseline; an edit
//! froze the item as the user's fork. Now the pack's items are read from
//! `packs/<id>/`, so a flat copy is either the user's own definition, which
//! stays and shadows the pack's, or a pack-owned copy, which is deleted so the
//! name tracks the pack. This module is the only home of the ownership rules
//! that decision needs, and it never deletes bytes it cannot prove are the
//! pack's:
//!
//! | Pre-upgrade state | Action |
//! |---|---|
//! | `forked: true` | keep |
//! | baseline hash, bytes equal it | delete the copy |
//! | baseline hash, bytes differ (edited since the last sync) | keep |
//! | no baseline (pre-v2 lock) | delete if the bytes equal what ships, or the path's last commit is pack-authored and it has no uncommitted change; else keep |
//! | file missing | nothing |
//! | listed in `removed` | untouched |
//! | a retired fork (no longer in the lock) | untouched |
//!
//! A machine service's flat mirror (`services/<id>/`) follows the same rule; an
//! edited one is kept and noted, since a service always runs from the app
//! bundle's own declaration.

use std::path::Path;

use serde::Deserialize;

use crate::{self as pack, lock as pack_lock, ContentHash, PackItemKind, PackManifest};
use cairn_platform::services::{guarded_resolve_file, FileSystem, GitClient};

/// The commit the migration makes in the workspace repository, once.
pub const MIGRATION_COMMIT: &str = "Packs read in place: remove materialized copies";

const BUNDLE_COMMIT_SUBJECTS: &[&str] = &[
    "Initialize Cairn workspace config",
    "Add missing bundled workspace defaults",
    "Sync bundled workspace defaults",
];
const PACK_COMMIT_PREFIX: &str = "Sync pack resources: ";
/// The June bundle-branch sync committed shipped resources, and nothing else,
/// in a scratch worktree of a dedicated `bundle` branch under this subject. A
/// path whose last commit carries it holds exactly what that build shipped.
///
/// Its two siblings from the same era are deliberately NOT ownership evidence:
/// `Merge branch 'bundle'` is only ever a path's last commit when the merge
/// combined the user's side with the bundle's, and `Snapshot workspace config`
/// committed the user's own pending edits before a sync.
const BUNDLE_BRANCH_COMMIT_PREFIX: &str = "Update bundled defaults to ";

fn is_pack_authored_subject(subject: &str) -> bool {
    BUNDLE_COMMIT_SUBJECTS.contains(&subject)
        || subject.starts_with(PACK_COMMIT_PREFIX)
        || subject.starts_with(BUNDLE_BRANCH_COMMIT_PREFIX)
}

/// A v1/v2 lock item, with the ownership fields v3 dropped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyItem {
    kind: PackItemKind,
    id: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    content_hash: Option<String>,
    #[serde(default)]
    forked: bool,
}

#[derive(Debug, Deserialize)]
struct LegacyLock {
    #[serde(default)]
    items: Vec<LegacyItem>,
}

/// What migrating one pack did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackMigration {
    /// Workspace-relative flat copies deleted because they were the pack's.
    pub deleted: Vec<String>,
    /// Things kept that the user should know about.
    pub notes: Vec<String>,
}

/// Migrate one pack whose lock predates v3, before its first v3 sync. With
/// no lock file at all (a workspace adopted from before locks existed), the
/// shipped items stand in with no baselines.
pub fn migrate_pack(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifest: &PackManifest,
) -> Result<PackMigration, String> {
    let lock_path = pack_lock::lock_path(config_dir, &manifest.id);
    let items = if fs.exists(&lock_path) {
        let text = fs.read_to_string(&lock_path)?;
        serde_yaml::from_str::<LegacyLock>(&text)
            .map_err(|e| format!("Unreadable pack lock {lock_path:?}: {e}"))?
            .items
    } else {
        manifest
            .items()
            .into_iter()
            .map(|item| LegacyItem {
                kind: item.kind,
                id: item.id,
                path: item.path,
                content_hash: None,
                forked: false,
            })
            .collect()
    };

    let mut migration = PackMigration::default();
    for item in items {
        // MCP servers, credentials and providers always lived in the pack's
        // own layer; a sync rewrites them.
        if matches!(
            item.kind,
            PackItemKind::Mcp | PackItemKind::Credential | PackItemKind::Provider
        ) {
            continue;
        }
        let Some(rel) = item.path.clone() else {
            continue;
        };
        let flat = config_dir.join(&rel);
        if !fs.exists(&flat) || item.forked {
            continue;
        }
        let owned = match &item.content_hash {
            Some(baseline) => matches_baseline(item.kind, config_dir, &flat, baseline)?,
            None => {
                shipped_matches(manifest, item.kind, &rel, config_dir, &flat)?
                    || legacy_path_is_pack_owned(git, config_dir, &rel)
            }
        };
        if owned {
            if is_tree(item.kind) {
                fs.remove_dir_all(&flat)?;
            } else {
                fs.remove_file(&flat)?;
            }
            migration.deleted.push(rel);
        } else if item.kind == PackItemKind::Service {
            migration.notes.push(format!(
                "`{rel}` was edited and is kept, but it is never run: machine service `{}` always runs from the app bundle's declaration",
                item.id
            ));
        }
    }
    migration.deleted.sort();
    Ok(migration)
}

/// Commit what the migration changed in the workspace repository, once.
pub fn commit_migration(git: &dyn GitClient, config_dir: &Path, paths: &[String]) {
    if paths.is_empty() || !git.is_repo(config_dir).unwrap_or(false) {
        return;
    }
    let mut add = vec!["add".to_string(), "-A".to_string(), "--".to_string()];
    add.extend(paths.iter().cloned());
    if let Err(error) = git.run(config_dir, add) {
        log::warn!("Could not stage the pack migration: {error}");
        return;
    }
    let mut commit = vec![
        "-c".to_string(),
        "user.name=Cairn".to_string(),
        "-c".to_string(),
        "user.email=cairn@local.invalid".to_string(),
        "commit".to_string(),
        "--only".to_string(),
        "-m".to_string(),
        MIGRATION_COMMIT.to_string(),
        "--".to_string(),
    ];
    commit.extend(paths.iter().cloned());
    match git.run(config_dir, commit) {
        Ok(output) if output.success => {}
        Ok(output) => log::warn!(
            "Could not commit the pack migration: {}",
            output.stdout.trim()
        ),
        Err(error) => log::warn!("Could not commit the pack migration: {error}"),
    }
}

fn is_tree(kind: PackItemKind) -> bool {
    matches!(
        kind,
        PackItemKind::Service | PackItemKind::Skill | PackItemKind::Workflow
    )
}

fn current_hash(kind: PackItemKind, config_dir: &Path, flat: &Path) -> Result<ContentHash, String> {
    pack::hash_item_path(kind, config_dir, flat)
}

/// Whether a flat copy still holds the bytes the old sync last wrote. A tree
/// baseline recorded before OS metadata left the tree hash still matches when
/// the tree, metadata included, hashes to it.
fn matches_baseline(
    kind: PackItemKind,
    config_dir: &Path,
    flat: &Path,
    baseline: &str,
) -> Result<bool, String> {
    if current_hash(kind, config_dir, flat)? == ContentHash::Present(baseline.to_string()) {
        return Ok(true);
    }
    Ok(is_tree(kind) && pack::hash_tree_present(flat, true)? == baseline)
}

/// Whether a flat copy is byte-identical to what this build ships for it.
fn shipped_matches(
    manifest: &PackManifest,
    kind: PackItemKind,
    rel: &str,
    config_dir: &Path,
    flat: &Path,
) -> Result<bool, String> {
    let source = manifest.root.join(rel);
    let shipped = if is_tree(kind) {
        pack::hash_tree(&source)?
    } else if source.exists() {
        pack::hash_file(&guarded_resolve_file(&manifest.root, &source)?)?
    } else {
        ContentHash::Missing
    };
    Ok(matches!(shipped, ContentHash::Present(_))
        && shipped == current_hash(kind, config_dir, flat)?)
}

/// Whether a path's bytes are what a sync last committed: its last commit
/// carries a sync's subject and the working tree holds no change to it since.
/// An uncommitted edit is the user's even under a sync-authored commit.
fn legacy_path_is_pack_owned(git: &dyn GitClient, config_dir: &Path, rel_path: &str) -> bool {
    let clean = git
        .run(
            config_dir,
            vec![
                "status".into(),
                "--porcelain".into(),
                "--".into(),
                rel_path.into(),
            ],
        )
        .is_ok_and(|output| output.success && output.stdout.trim().is_empty());
    if !clean {
        return false;
    }
    let output = git.run(
        config_dir,
        vec![
            "log".into(),
            "-1".into(),
            "--format=%s".into(),
            "--".into(),
            rel_path.into(),
        ],
    );
    match output {
        Ok(output) if output.success => {
            let subject = output.stdout.trim();
            !subject.is_empty() && is_pack_authored_subject(subject)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests;
