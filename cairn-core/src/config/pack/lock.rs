//! Install locks: the destination-side record of which pack owns what.
//!
//! Ownership splits into two questions with two mechanisms.
//!
//! *"Has the user edited this file since we wrote it?"* is answered by git
//! history — the last commit subject on a path (see
//! `workspace::bundle::pack_file_is_user_owned`). That oracle predates packs,
//! is already correct, and stays exactly where it is.
//!
//! *"Which pack owns this path, at what version, from where?"* is a question git
//! history answers only by walking, so it gets a lock file at
//! `~/.cairn/packs/<id>/pack.yaml`. One read of that file answers everything the
//! catalog resource, an update, and an uninstall need. Provenance for a
//! URL-installed pack lives here and deliberately NOT in a commit subject: a URL
//! is not a stable git-subject identity, and cramming it in would make the
//! ownership oracle stringly and fragile.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::manifest::{PackAuthor, PackFormat, PackItem, PackItemKind, PackManifest};
use crate::services::FileSystem;

/// Subdirectory of the workspace config home holding per-pack locks and MCP
/// definitions. Tracked by the workspace repo (see `workspace::repo`) so lock
/// files are ownership-classifiable like any other managed content.
pub const PACKS_DIR: &str = "packs";
/// The lock filename inside `packs/<id>/`.
pub const LOCK_FILE: &str = "pack.yaml";

/// Where an installed pack came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PackSourceKind {
    /// Shipped in the app's resource directory.
    Bundled,
    /// Fetched from a URL.
    Url,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackSource {
    pub kind: PackSourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The requested revision (branch or tag), when installed from a URL.
    #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    /// The RESOLVED commit. Without this an "update" has nothing to compare
    /// against, so the fetch paths record it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<PackFormat>,
}

impl PackSource {
    pub fn bundled(format: PackFormat) -> Self {
        PackSource {
            kind: PackSourceKind::Bundled,
            url: None,
            git_ref: None,
            sha: None,
            format: Some(format),
        }
    }
}

fn default_cairn_version() -> u32 {
    1
}

/// What one installed pack put in the workspace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackLock {
    #[serde(default = "default_cairn_version")]
    pub cairn_version: u32,
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<PackAuthor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
    pub installed_at: String,
    /// Content hash of the pack's source tree at install time. Replaces the old
    /// single global `.bundle-sync` marker: the same short-circuit, per pack.
    pub content_hash: String,
    pub source: PackSource,
    #[serde(default)]
    pub items: Vec<PackItem>,
    /// Items the user removed from this pack locally.
    ///
    /// Removing one item must not mean uninstalling the pack that ships it: a
    /// user who wants MATLAB's skill but not its MCP server should be able to
    /// say so. The pack stays installed and keeps updating; these items are
    /// simply not materialized, not re-copied by the next sync, and not offered.
    /// Without this record, deleting a pack item is silently undone on the next
    /// launch by the same copy-when-missing that seeds a fresh workspace.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<PackItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl PackLock {
    /// Build a lock recording `manifest` as installed with `content_hash`.
    pub fn new(
        manifest: &PackManifest,
        content_hash: String,
        source: PackSource,
        items: Vec<PackItem>,
    ) -> Self {
        PackLock {
            cairn_version: default_cairn_version(),
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            author: manifest.author.clone(),
            homepage: manifest.homepage.clone(),
            keywords: manifest.keywords.clone(),
            installed_at: chrono::Utc::now().to_rfc3339(),
            content_hash,
            source,
            items,
            removed: Vec::new(),
            notes: manifest.notes.clone(),
        }
    }

    /// Workspace-relative destination paths this pack installed.
    pub fn item_paths(&self) -> Vec<String> {
        self.items
            .iter()
            .filter_map(|item| item.path.clone())
            .collect()
    }

    /// Workspace-relative paths the user removed from this pack.
    pub fn removed_paths(&self) -> Vec<String> {
        self.removed
            .iter()
            .filter_map(|item| item.path.clone())
            .collect()
    }

    pub fn is_removed(&self, kind: PackItemKind, id: &str) -> bool {
        self.removed
            .iter()
            .any(|item| item.kind == kind && item.id == id)
    }
}

/// Record that the user removed one item from whichever installed pack ships it.
///
/// Returns the owning pack's id, or `None` when no installed pack claims the
/// item — in which case the deletion is an ordinary one against the user's own
/// content and needs no record.
pub fn record_item_removal(
    fs: &dyn FileSystem,
    config_dir: &Path,
    kind: PackItemKind,
    id: &str,
) -> Option<String> {
    for mut lock in installed_packs(config_dir) {
        let position = lock
            .items
            .iter()
            .position(|item| item.kind == kind && item.id == id);
        let item = match position {
            Some(position) => lock.items.remove(position),
            // A lock written by an older build may not list the item even though
            // its pack supplies it. Fall back to what the pack actually ships,
            // so a removal is never silently dropped.
            None if kind == PackItemKind::Mcp && pack_supplies_mcp(config_dir, &lock.id, id) => {
                PackItem::mcp(id)
            }
            None => continue,
        };
        if lock.is_removed(kind, id) {
            return Some(lock.id);
        }
        // The file this record describes is already gone from the tree, and has
        // to land in the SAME commit as the record: one user action, one commit.
        // Split apart, a `git checkout` reverts the deletion while the record
        // persists, leaving a resurrected item that no pack's `items` claims and
        // that no future sync will ever update.
        let removed_path: Vec<String> = item.path.clone().into_iter().collect();
        lock.removed.push(item);
        lock.removed
            .sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
        lock.removed.dedup();
        let pack_id = lock.id.clone();
        if let Err(error) = write_lock(fs, config_dir, &lock) {
            log::warn!(
                "Failed to record removal of {kind:?} `{id}` from pack `{pack_id}`: {error}"
            );
            return None;
        }
        commit_lock(config_dir, &pack_id, &removed_path);
        return Some(pack_id);
    }
    None
}

/// Commit a lock the moment it changes, in both directions, together with any
/// workspace paths the change describes.
///
/// This is a workspace whose premise is that history IS the record. An
/// uncommitted lock is reverted by an ordinary `git checkout` in `~/.cairn`,
/// leaving the record and the tree disagreeing about what is installed — and a
/// later sync resolves that disagreement in favor of the stale record.
fn commit_lock(config_dir: &Path, pack_id: &str, workspace_paths: &[String]) {
    let mut paths = vec![lock_path(config_dir, pack_id)];
    paths.extend(workspace_paths.iter().map(|rel| config_dir.join(rel)));
    crate::config::commit_config_paths(&paths, &format!("Sync pack resources: {pack_id}"));
}

fn pack_supplies_mcp(config_dir: &Path, pack_id: &str, server: &str) -> bool {
    let path = mcp_path(config_dir, pack_id);
    path.is_file()
        && super::mcp::parse_pack_mcp_file(&path)
            .map(|servers| servers.contains_key(server))
            .unwrap_or(false)
}

/// Undo every recorded removal for a pack, so the next sync materializes its
/// full item set again. Returns the items that came back.
pub fn restore_removed_items(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
) -> Result<Vec<PackItem>, String> {
    let Some(mut lock) = read_lock(config_dir, pack_id) else {
        return Err(format!("Pack `{pack_id}` is not installed"));
    };
    if lock.removed.is_empty() {
        return Ok(Vec::new());
    }
    let restored = lock.removed.clone();
    lock.items.append(&mut lock.removed);
    lock.items
        .sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    lock.items.dedup();
    write_lock(fs, config_dir, &lock)?;
    // The restored files are materialized by the sync that follows and are
    // committed there; only the record changes here.
    commit_lock(config_dir, pack_id, &[]);
    Ok(restored)
}

/// Put a lock back exactly as it was, committing the reversal the same way the
/// change it undoes was committed.
///
/// A recorded removal is the only trace of a decision the user made, and
/// [`restore_removed_items`] discards it before the sync that materializes the
/// items has run. If that sync then fails, the items are still absent and the
/// record that would bring them back is gone — a state the user cannot see and
/// cannot undo. This is how that window is closed.
pub fn rewrite_lock(fs: &dyn FileSystem, config_dir: &Path, lock: &PackLock) -> Result<(), String> {
    write_lock(fs, config_dir, lock)?;
    commit_lock(config_dir, &lock.id, &[]);
    Ok(())
}

/// `<config_dir>/packs/<id>`.
pub fn pack_dir(config_dir: &Path, id: &str) -> PathBuf {
    config_dir.join(PACKS_DIR).join(id)
}

/// `<config_dir>/packs/<id>/pack.yaml`.
pub fn lock_path(config_dir: &Path, id: &str) -> PathBuf {
    pack_dir(config_dir, id).join(LOCK_FILE)
}

/// `<config_dir>/packs/<id>/mcp.yaml` — an installed pack's MCP layer.
pub fn mcp_path(config_dir: &Path, id: &str) -> PathBuf {
    pack_dir(config_dir, id).join(super::manifest::PACK_MCP_FILE)
}

pub fn read_lock(config_dir: &Path, id: &str) -> Option<PackLock> {
    let path = lock_path(config_dir, id);
    let text = std::fs::read_to_string(&path).ok()?;
    match serde_yaml::from_str::<PackLock>(&text) {
        Ok(lock) => Some(lock),
        Err(error) => {
            log::warn!("Unreadable pack lock at {path:?}: {error}");
            None
        }
    }
}

/// Write an install lock. Routed through [`FileSystem`] rather than `std::fs`
/// because the workspace sync that calls it is itself filesystem-injected.
pub fn write_lock(
    fs: &dyn FileSystem,
    config_dir: &Path,
    lock: &PackLock,
) -> Result<PathBuf, String> {
    let path = lock_path(config_dir, &lock.id);
    let parent = path
        .parent()
        .ok_or_else(|| format!("Pack lock path {path:?} has no parent"))?;
    fs.create_dir_all(parent)?;
    let body = serde_yaml::to_string(lock)
        .map_err(|e| format!("Failed to serialize pack lock for `{}`: {e}", lock.id))?;
    fs.write_str(&path, &body)?;
    Ok(path)
}

/// Remove an installed pack's whole `packs/<id>/` directory — lock and MCP
/// layer alike. Idempotent.
pub fn remove_pack_dir(fs: &dyn FileSystem, config_dir: &Path, id: &str) -> Result<(), String> {
    let dir = pack_dir(config_dir, id);
    if fs.exists(&dir) {
        fs.remove_dir_all(&dir)?;
    }
    Ok(())
}

/// Filename recording that the user explicitly uninstalled a pack.
///
/// An uninstall keeps items the user edited, and adoption reads a surviving
/// item as "already installed" — so without a record of the decision, the very
/// next startup would re-adopt the pack it was just removed from, and a pack
/// marked `default: true` would reinstall wholesale. This marker is what makes
/// the catalog's delete hold: an explicit choice outranks both.
pub const UNINSTALL_MARKER: &str = "uninstalled";

pub fn uninstall_marker_path(config_dir: &Path, id: &str) -> PathBuf {
    pack_dir(config_dir, id).join(UNINSTALL_MARKER)
}

/// Whether the user has uninstalled this pack and not since reinstalled it.
pub fn is_uninstalled(config_dir: &Path, id: &str) -> bool {
    uninstall_marker_path(config_dir, id).is_file()
}

pub fn record_uninstall(
    fs: &dyn FileSystem,
    config_dir: &Path,
    id: &str,
) -> Result<PathBuf, String> {
    let path = uninstall_marker_path(config_dir, id);
    if let Some(parent) = path.parent() {
        fs.create_dir_all(parent)?;
    }
    fs.write_str(
        &path,
        &format!(
            "# Uninstalled from this workspace at {}.\n\
             # Delete this file, or install the pack again, to bring it back.\n",
            chrono::Utc::now().to_rfc3339()
        ),
    )?;
    Ok(path)
}

/// Revoke an uninstall. Installing a pack again is an explicit choice that
/// supersedes the earlier one. Idempotent.
pub fn clear_uninstall(fs: &dyn FileSystem, config_dir: &Path, id: &str) -> Result<(), String> {
    let path = uninstall_marker_path(config_dir, id);
    if fs.exists(&path) {
        fs.remove_file(&path)?;
    }
    Ok(())
}

/// Every pack installed in `config_dir`, sorted by id.
pub fn installed_packs(config_dir: &Path) -> Vec<PackLock> {
    let root = config_dir.join(PACKS_DIR);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut ids: Vec<String> = entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().to_str().map(|s| s.to_string()))
        .collect();
    ids.sort();
    ids.iter()
        .filter_map(|id| read_lock(config_dir, id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pack::manifest::{PackItemKind, PackManifest};
    use tempfile::TempDir;

    fn manifest(id: &str) -> PackManifest {
        PackManifest {
            id: id.to_string(),
            name: id.to_string(),
            version: "1.0.0".into(),
            description: "d".into(),
            author: None,
            homepage: None,
            license: None,
            keywords: vec![],
            default: false,
            retired: vec![],
            format: PackFormat::Cairn,
            notes: vec![],
            root: PathBuf::from("/src"),
        }
    }

    #[test]
    fn a_lock_round_trips_through_yaml() {
        let temp = TempDir::new().unwrap();
        let lock = PackLock::new(
            &manifest("matlab"),
            "sha256:abc".into(),
            PackSource::bundled(PackFormat::Cairn),
            vec![
                PackItem::file(PackItemKind::Skill, "matlab", "skills/matlab"),
                PackItem::mcp("matlab"),
            ],
        );
        write_lock(&crate::services::RealFileSystem, temp.path(), &lock).unwrap();

        let read = read_lock(temp.path(), "matlab").unwrap();
        assert_eq!(read, lock);
        assert_eq!(read.item_paths(), vec!["skills/matlab".to_string()]);
        assert_eq!(installed_packs(temp.path()), vec![lock]);
    }

    #[test]
    fn a_url_source_carries_a_resolved_sha() {
        let temp = TempDir::new().unwrap();
        let mut lock = PackLock::new(
            &manifest("acme"),
            "sha256:def".into(),
            PackSource {
                kind: PackSourceKind::Url,
                url: Some("https://github.com/acme/pack".into()),
                git_ref: Some("main".into()),
                sha: Some("9f2c".into()),
                format: Some(PackFormat::ClaudeCode),
            },
            vec![],
        );
        lock.notes
            .push("ignored claude-code component: hooks".into());
        write_lock(&crate::services::RealFileSystem, temp.path(), &lock).unwrap();

        let read = read_lock(temp.path(), "acme").unwrap();
        assert_eq!(read.source.sha.as_deref(), Some("9f2c"));
        assert_eq!(read.source.kind, PackSourceKind::Url);
        assert_eq!(read.notes.len(), 1);
    }

    #[test]
    fn a_missing_lock_reads_as_not_installed() {
        let temp = TempDir::new().unwrap();
        assert!(read_lock(temp.path(), "nope").is_none());
        assert!(installed_packs(temp.path()).is_empty());
    }
}
