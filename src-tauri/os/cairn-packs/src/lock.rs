//! Install locks: what an installed pack is, where it came from, and what it
//! holds.
//!
//! A pack's items are read where the pack installed them
//! (`packs/<id>/<item path>`), so the lock carries an inventory rather than
//! ownership: the items, the items the user removed, the hash of the pack's
//! source and the hash of the tree staged from it. Locks written before items
//! were read in place (cairnVersion 1 and 2) are read once by
//! [`crate::migrate_v3`], which owns their per-item ownership fields.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::manifest::{PackAuthor, PackFormat, PackItem, PackItemKind, PackManifest};
use cairn_platform::services::FileSystem;

/// Subdirectory of the workspace config home holding per-pack locks and MCP
/// definitions. The lock is the live ownership authority; workspace Git may
/// retain these files only as passive audit history.
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
    /// Imported from a local Agent Plugin directory and installed from a managed snapshot.
    Local,
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
    /// Canonical original directory for a local import. Runtime reads use the managed snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The id of the trusted key whose signature over the library index vouched
    /// for this install. Absent for anything not installed from a signed index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer: Option<String>,
    /// For content a library index references from another repository: where
    /// the bytes were actually fetched from. `url`/`sha` above stay the
    /// library's, because that is the index that vouched for them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<UpstreamRef>,
}

/// A repository and pinned commit a library entry's content was fetched from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamRef {
    pub url: String,
    pub sha: String,
}

impl PackSource {
    pub fn bundled(format: PackFormat) -> Self {
        PackSource {
            kind: PackSourceKind::Bundled,
            url: None,
            git_ref: None,
            sha: None,
            format: Some(format),
            path: None,
            signer: None,
            upstream: None,
        }
    }

    pub fn local(path: String) -> Self {
        PackSource {
            kind: PackSourceKind::Local,
            url: None,
            git_ref: None,
            sha: None,
            format: Some(PackFormat::AgentPlugin),
            path: Some(path),
            signer: None,
            upstream: None,
        }
    }

    /// A pack installed from a library source's signed index.
    pub fn library(
        url: String,
        git_ref: String,
        sha: String,
        signer: String,
        upstream: Option<UpstreamRef>,
        format: PackFormat,
    ) -> Self {
        PackSource {
            kind: PackSourceKind::Url,
            url: Some(url),
            git_ref: Some(git_ref),
            sha: Some(sha),
            format: Some(format),
            path: None,
            signer: Some(signer),
            upstream,
        }
    }
}

/// The lock format this build writes: items read in place.
pub const LOCK_VERSION: u32 = 3;

/// A lock with no `cairnVersion` predates the field, which arrived with v2.
fn default_cairn_version() -> u32 {
    1
}

/// One active item of an installed pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackLockItem {
    pub kind: PackItemKind,
    pub id: String,
    /// The item's path inside its pack, which is also where it is installed
    /// under `packs/<id>/`. Absent for an MCP server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl From<PackItem> for PackLockItem {
    fn from(item: PackItem) -> Self {
        Self {
            kind: item.kind,
            id: item.id,
            path: item.path,
        }
    }
}

impl PackLockItem {
    pub fn manifest_item(&self) -> PackItem {
        PackItem {
            kind: self.kind,
            id: self.id.clone(),
            path: self.path.clone(),
        }
    }
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
    /// Content hash of the pack's source tree at install time: the version
    /// installed, compared with what a source now offers.
    pub content_hash: String,
    /// Hash of the tree staged under `packs/<id>/` from that source. A tree
    /// that no longer hashes to it was changed by something other than a sync,
    /// and the next sync stages the same version again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_hash: Option<String>,
    pub source: PackSource,
    #[serde(default)]
    pub items: Vec<PackLockItem>,
    /// Items the user removed from this pack (tenet 1121).
    ///
    /// Removing one item must not mean uninstalling the pack that ships it: a
    /// user who wants MATLAB's skill but not its MCP server should be able to
    /// say so. The pack stays installed and keeps updating. A removed library
    /// item stays staged in place, so restoring it is clearing this record, but
    /// resolution skips it; a removed MCP server is left out of the pack's MCP
    /// layer.
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
            cairn_version: LOCK_VERSION,
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            author: manifest.author.clone(),
            homepage: manifest.homepage.clone(),
            keywords: manifest.keywords.clone(),
            installed_at: chrono::Utc::now().to_rfc3339(),
            content_hash,
            staged_hash: None,
            source,
            items: items.into_iter().map(PackLockItem::from).collect(),
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

    pub fn item(&self, kind: PackItemKind, id: &str) -> Option<&PackLockItem> {
        self.items
            .iter()
            .find(|item| item.kind == kind && item.id == id)
    }

    pub fn item_mut(&mut self, kind: PackItemKind, id: &str) -> Option<&mut PackLockItem> {
        self.items
            .iter_mut()
            .find(|item| item.kind == kind && item.id == id)
    }

    pub fn sort_items(&mut self) {
        self.items
            .sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
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
            Some(position) => lock.items.remove(position).manifest_item(),
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
        return Some(pack_id);
    }
    None
}

/// Record that the user removed `kind` `id` from the installed pack `pack_id`,
/// returning the lock's path. Idempotent for an item already removed.
pub fn record_removal_from(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
    kind: PackItemKind,
    id: &str,
) -> Result<PathBuf, String> {
    let mut lock = read_lock(config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    if lock.is_removed(kind, id) {
        return Ok(lock_path(config_dir, pack_id));
    }
    let position = lock
        .items
        .iter()
        .position(|item| item.kind == kind && item.id == id)
        .ok_or_else(|| format!("Pack `{pack_id}` does not carry {} `{id}`", kind.as_str()))?;
    let item = lock.items.remove(position).manifest_item();
    lock.removed.push(item);
    lock.removed
        .sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    lock.removed.dedup();
    write_lock(fs, config_dir, &lock)
}

fn pack_supplies_mcp(config_dir: &Path, pack_id: &str, server: &str) -> bool {
    let path = mcp_path(config_dir, pack_id);
    path.is_file()
        && super::mcp::parse_pack_mcp_file(&path)
            .map(|file| file.servers.contains_key(server))
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
    lock.items.extend(
        std::mem::take(&mut lock.removed)
            .into_iter()
            .map(PackLockItem::from),
    );
    lock.items
        .sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    lock.items.dedup();
    write_lock(fs, config_dir, &lock)?;
    Ok(restored)
}

/// Put a lock back exactly as it was.
///
/// A recorded removal is the only trace of a decision the user made, and
/// [`restore_removed_items`] discards it before the sync that materializes the
/// items has run. If that sync then fails, the items are still absent and the
/// record that would bring them back is gone — a state the user cannot see and
/// cannot undo. This is how that window is closed.
pub fn rewrite_lock(fs: &dyn FileSystem, config_dir: &Path, lock: &PackLock) -> Result<(), String> {
    write_lock(fs, config_dir, lock)?;
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
/// Without a record of the decision, a pack marked `default: true` would
/// reinstall wholesale at the very next startup. This marker is what makes
/// the catalog's delete hold: an explicit choice outranks the default.
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
    use crate::manifest::{PackItemKind, PackManifest};
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
            services: vec![],
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
        write_lock(
            &cairn_platform::services::RealFileSystem,
            temp.path(),
            &lock,
        )
        .unwrap();

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
                path: None,
                signer: Some("cairn-library-2026".into()),
                upstream: Some(UpstreamRef {
                    url: "https://github.com/acme/upstream".into(),
                    sha: "0123456789abcdef0123456789abcdef01234567".into(),
                }),
            },
            vec![],
        );
        lock.notes
            .push("ignored claude-code component: hooks".into());
        write_lock(
            &cairn_platform::services::RealFileSystem,
            temp.path(),
            &lock,
        )
        .unwrap();

        let read = read_lock(temp.path(), "acme").unwrap();
        assert_eq!(read.source.sha.as_deref(), Some("9f2c"));
        assert_eq!(read.source.kind, PackSourceKind::Url);
        assert_eq!(read.source.signer.as_deref(), Some("cairn-library-2026"));
        assert_eq!(read.source, lock.source, "provenance round-trips whole");
        assert_eq!(read.notes.len(), 1);
    }

    #[test]
    fn a_missing_lock_reads_as_not_installed() {
        let temp = TempDir::new().unwrap();
        assert!(read_lock(temp.path(), "nope").is_none());
        assert!(installed_packs(temp.path()).is_empty());
    }

    /// A v2 lock's per-item ownership fields are read by nothing but the
    /// migration; the v3 lock reads such a file as its plain inventory.
    #[test]
    fn a_v2_lock_reads_as_its_inventory() {
        let yaml = "cairnVersion: 2\nid: old\nname: Old\nversion: 1.0.0\ninstalledAt: now\ncontentHash: old\nsource:\n  kind: bundled\nitems:\n  - kind: agent\n    id: build\n    path: agents/build.md\n    contentHash: abc\n    forked: true\n";
        let lock: PackLock = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(lock.cairn_version, 2);
        assert_eq!(
            lock.item(PackItemKind::Agent, "build")
                .unwrap()
                .path
                .as_deref(),
            Some("agents/build.md")
        );
        let written = serde_yaml::to_string(&lock).unwrap();
        assert!(
            !written.contains("forked") && !written.contains("abc"),
            "{written}"
        );
    }

    #[test]
    fn a_removal_is_recorded_against_its_pack_and_restored_whole() {
        let temp = TempDir::new().unwrap();
        let fs = cairn_platform::services::RealFileSystem;
        let lock = PackLock::new(
            &manifest("core"),
            "0".repeat(64),
            PackSource::bundled(PackFormat::Cairn),
            vec![PackItem::file(
                PackItemKind::Agent,
                "build",
                "agents/build.md",
            )],
        );
        assert_eq!(lock.cairn_version, LOCK_VERSION);
        write_lock(&fs, temp.path(), &lock).unwrap();
        record_removal_from(&fs, temp.path(), "core", PackItemKind::Agent, "build").unwrap();
        let removed = read_lock(temp.path(), "core").unwrap();
        assert!(removed.is_removed(PackItemKind::Agent, "build"));
        assert!(removed.item(PackItemKind::Agent, "build").is_none());
        assert!(
            record_removal_from(&fs, temp.path(), "core", PackItemKind::Skill, "none").is_err()
        );

        restore_removed_items(&fs, temp.path(), "core").unwrap();
        let restored = read_lock(temp.path(), "core").unwrap();
        assert!(!restored.is_removed(PackItemKind::Agent, "build"));
        assert!(restored.item(PackItemKind::Agent, "build").is_some());
    }
}
