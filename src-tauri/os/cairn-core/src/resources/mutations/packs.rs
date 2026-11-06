//! Resource-pack mutations: install, update, restore, uninstall, and removing
//! one item without the pack around it.
//!
//! All of them run the same machinery the startup sync runs, so a pack
//! installed from the catalog and a pack installed on a fresh run are the same
//! thing afterwards — same destination layout, same ownership rules, same lock.
//! Both callers reach these functions: the `cairn://packs/{id}` writes an agent
//! makes, and the typed settings commands. That shared entry is what makes the
//! registry event below unmissable rather than something each surface remembers
//! to send.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::config::pack::{self, ContentHash, PackItem, PackItemKind};
use crate::library::{PackAuthority, OPERATOR_ONLY_PACK_ACT};
use crate::orchestrator::Orchestrator;
use crate::services::{RealFileSystem, RealGitClient};
use crate::workspace::bundle::{sync_one_pack, sync_resolved_pack, uninstall_pack};

/// What one pack mutation did, in the terms the catalog itself uses.
///
/// The human `summary` is what an agent's write reports; every other field is
/// the same facts in a form a settings screen can act on, so the two surfaces
/// cannot describe one action differently.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PackMutationResult {
    /// `install`, `update`, `restore`, `uninstall`, `remove-item`, or `reset-item`.
    pub action: String,
    pub pack_id: String,
    /// Workspace-relative paths a sync wrote or retired.
    pub changed_paths: Vec<String>,
    /// Paths an uninstall deleted.
    pub removed_paths: Vec<String>,
    /// Paths an uninstall left in place because the user had edited them.
    pub kept_paths: Vec<String>,
    /// Items a restore brought back.
    pub restored_items: Vec<PackItem>,
    /// The single item a removal took out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_item: Option<PackItem>,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<String>,
}

fn commit_plugin_snapshot(
    stage: tempfile::TempDir,
    destination: &Path,
) -> Result<Option<std::path::PathBuf>, String> {
    commit_snapshot(&stage.keep(), destination)
}

/// Move a staged pack root into place as `destination`, keeping any previous
/// snapshot aside as `source.previous` until the sync that follows succeeds.
fn commit_snapshot(
    staged: &Path,
    destination: &Path,
) -> Result<Option<std::path::PathBuf>, String> {
    let parent = destination
        .parent()
        .ok_or("Managed snapshot has no parent")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let backup = parent.join("source.previous");
    if backup.exists() {
        return Err(format!(
            "Cannot commit managed plugin snapshot while stale backup exists at {}",
            backup.display()
        ));
    }
    let had_snapshot = destination.exists();
    if had_snapshot {
        std::fs::rename(destination, &backup)
            .map_err(|e| format!("Failed to preserve managed plugin snapshot: {e}"))?;
    }
    if let Err(error) = std::fs::rename(staged, destination) {
        if had_snapshot {
            let _ = std::fs::rename(&backup, destination);
        }
        return Err(format!("Failed to retain managed plugin snapshot: {error}"));
    }
    Ok(had_snapshot.then_some(backup))
}

fn rollback_plugin_snapshot(destination: &Path, backup: Option<&Path>) {
    let _ = std::fs::remove_dir_all(destination);
    if let Some(backup) = backup {
        let _ = std::fs::rename(backup, destination);
    }
}

struct PackMutationSnapshot {
    _temp: tempfile::TempDir,
    entries: Vec<(PathBuf, Option<(PathBuf, bool)>)>,
}

impl PackMutationSnapshot {
    fn capture(
        config_dir: &Path,
        pack_id: &str,
        old_lock: Option<&pack::PackLock>,
        new_manifest: &pack::PackManifest,
    ) -> Result<Self, String> {
        let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
        let mut paths = BTreeSet::from([
            pack::lock::lock_path(config_dir, pack_id),
            pack::lock::mcp_path(config_dir, pack_id),
        ]);
        for item in old_lock
            .into_iter()
            .flat_map(|lock| lock.items.iter().map(pack::PackLockItem::manifest_item))
            .chain(new_manifest.items())
        {
            if let Some(path) = item.path {
                paths.insert(pack::item_destination(
                    config_dir, pack_id, item.kind, &path,
                ));
            }
        }

        let mut entries = Vec::with_capacity(paths.len());
        for (index, path) in paths.into_iter().enumerate() {
            let backup = temp.path().join(index.to_string());
            let saved = if path.is_dir() {
                crate::services::guarded_copy_tree(&path, &backup)?;
                Some((backup, true))
            } else if path.is_file() {
                let parent = path
                    .parent()
                    .ok_or_else(|| format!("Snapshot path has no parent: {path:?}"))?;
                crate::services::guarded_copy_file(parent, &path, &backup)?;
                Some((backup, false))
            } else {
                None
            };
            entries.push((path, saved));
        }
        Ok(Self {
            _temp: temp,
            entries,
        })
    }

    fn restore(&self) -> Result<(), String> {
        for (path, saved) in &self.entries {
            if path.is_dir() {
                std::fs::remove_dir_all(path)
                    .map_err(|e| format!("Failed to clear rollback path {path:?}: {e}"))?;
            } else if path.exists() {
                std::fs::remove_file(path)
                    .map_err(|e| format!("Failed to clear rollback path {path:?}: {e}"))?;
            }
            if let Some((backup, is_dir)) = saved {
                if *is_dir {
                    crate::services::guarded_copy_tree(backup, path)?;
                } else {
                    let parent = backup.parent().expect("temporary backup has a parent");
                    crate::services::guarded_copy_file(parent, backup, path)?;
                }
            }
        }
        Ok(())
    }
}

fn stage_plugin_snapshot(
    config_dir: &Path,
    original: &Path,
) -> Result<(tempfile::TempDir, pack::agent_plugin::AgentPlugin), String> {
    let canonical = original
        .canonicalize()
        .map_err(|e| format!("Failed to resolve plugin path {original:?}: {e}"))?;
    let plugin = pack::agent_plugin::load(&canonical)?;
    let packs = config_dir.join(pack::lock::PACKS_DIR);
    std::fs::create_dir_all(&packs).map_err(|e| e.to_string())?;
    let stage = tempfile::Builder::new()
        .prefix(".plugin-snapshot-")
        .tempdir_in(&packs)
        .map_err(|e| e.to_string())?;
    crate::services::guarded_copy_tree(&canonical, stage.path())?;

    // Project the extension into the canonical flat source layout consumed by
    // the existing synchronizer. The original portable tree remains intact.
    for dir in ["agents", "recipes", "responses", "routes", "workflows"] {
        let source = stage
            .path()
            .join(pack::agent_plugin::CAIRN_EXTENSION)
            .join(dir);
        if source.is_dir() {
            crate::services::guarded_copy_tree(&source, &stage.path().join(dir))?;
        }
    }
    if !plugin.mcp_servers.is_empty() {
        let servers = plugin
            .mcp_servers
            .iter()
            .map(|(name, server)| (name.clone(), server.config.clone()))
            .collect::<BTreeMap<_, _>>();
        let text = serde_yaml::to_string(&serde_json::json!({"mcpServers": servers}))
            .map_err(|e| e.to_string())?;
        std::fs::write(stage.path().join(pack::manifest::PACK_MCP_FILE), text)
            .map_err(|e| e.to_string())?;
    }
    let mut staged = pack::agent_plugin::load(stage.path())?;
    staged.manifest.root = stage.path().to_path_buf();
    Ok((stage, staged))
}

pub fn import_agent_plugin(orch: &Orchestrator, path: &Path) -> Result<PackMutationResult, String> {
    let canonical = path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve plugin path {path:?}: {e}"))?;
    let (stage, plugin) = stage_plugin_snapshot(&orch.config_dir, &canonical)?;
    let id = plugin.manifest.id.clone();
    let source = pack::PackSource::local(canonical.to_string_lossy().into_owned());
    // A re-import of the same directory is an update, which has its own path.
    if pack::lock::read_lock(&orch.config_dir, &id).is_some() {
        return Err(format!(
            "Pack `{id}` is already installed; update it with patch {{action:\"update\"}}"
        ));
    }
    pack::check_install_collisions(
        &orch.config_dir,
        &plugin.manifest,
        &source,
        &shipped_pack_ids(orch),
    )?;
    let destination = pack::lock::pack_dir(&orch.config_dir, &id).join("source");
    let transaction = PackMutationSnapshot::capture(&orch.config_dir, &id, None, &plugin.manifest)?;
    let backup = commit_plugin_snapshot(stage, &destination)?;
    let mut manifest = plugin.manifest.clone();
    manifest.root = destination.clone();
    let result = sync_resolved_pack(
        &RealGitClient,
        &RealFileSystem,
        &orch.config_dir,
        manifest,
        source,
    )
    .map_err(|error| {
        rollback_plugin_snapshot(&destination, backup.as_deref());
        match transaction.restore() {
            Ok(()) => error,
            Err(rollback) => format!("{error}; rollback failed: {rollback}"),
        }
    })?;
    if let Some(backup) = backup {
        let _ = std::fs::remove_dir_all(backup);
    }
    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "import".into(),
        pack_id: id.clone(),
        changed_paths: result.changed_paths,
        diagnostics: plugin.diagnostics,
        summary: format!("Imported Agent Plugin '{id}'"),
        ..Default::default()
    })
}

fn update_local_plugin(
    orch: &Orchestrator,
    lock: &pack::PackLock,
) -> Result<PackMutationResult, String> {
    let original = lock
        .source
        .path
        .as_deref()
        .ok_or("Local plugin source has no provenance path")?;
    let (stage, plugin) = stage_plugin_snapshot(&orch.config_dir, Path::new(original))?;
    if plugin.manifest.id != lock.id {
        return Err(format!(
            "Updated plugin id `{}` does not match installed id `{}`",
            plugin.manifest.id, lock.id
        ));
    }
    let destination = pack::lock::pack_dir(&orch.config_dir, &lock.id).join("source");
    let transaction =
        PackMutationSnapshot::capture(&orch.config_dir, &lock.id, Some(lock), &plugin.manifest)?;
    let backup = commit_plugin_snapshot(stage, &destination)?;
    let mut manifest = plugin.manifest.clone();
    manifest.root = destination.clone();
    let result = sync_resolved_pack(
        &RealGitClient,
        &RealFileSystem,
        &orch.config_dir,
        manifest,
        lock.source.clone(),
    )
    .map_err(|error| {
        rollback_plugin_snapshot(&destination, backup.as_deref());
        match transaction.restore() {
            Ok(()) => error,
            Err(rollback) => format!("{error}; rollback failed: {rollback}"),
        }
    })?;
    if let Some(backup) = backup {
        let _ = std::fs::remove_dir_all(backup);
    }
    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "update".into(),
        pack_id: lock.id.clone(),
        changed_paths: result.changed_paths,
        diagnostics: plugin.diagnostics,
        summary: format!("Updated pack '{}'", lock.id),
        ..Default::default()
    })
}

pub fn export_agent_plugin(
    orch: &Orchestrator,
    pack_id: &str,
    destination: &Path,
) -> Result<PackMutationResult, String> {
    let lock = pack::lock::read_lock(&orch.config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    let manifest = pack::PackManifest {
        id: lock.id.clone(),
        name: lock.name.clone(),
        version: lock.version.clone(),
        description: lock.description.clone(),
        author: lock.author.clone(),
        homepage: lock.homepage.clone(),
        license: None,
        keywords: lock.keywords.clone(),
        default: false,
        retired: vec![],
        format: pack::PackFormat::AgentPlugin,
        notes: lock.notes.clone(),
        root: orch.config_dir.clone(),
    };
    let items = lock
        .items
        .iter()
        .map(pack::PackLockItem::manifest_item)
        .collect::<Vec<_>>();
    let servers = pack::mcp::parse_pack_mcp_file(&pack::lock::mcp_path(&orch.config_dir, pack_id))
        .unwrap_or_default()
        .servers
        .into_iter()
        .map(|(name, config)| (name, pack::agent_plugin::AgentPluginServer { config }))
        .collect();
    pack::agent_plugin::export(destination, &manifest, &items, &servers)?;
    Ok(PackMutationResult {
        action: "export".into(),
        pack_id: pack_id.into(),
        summary: format!("Exported pack '{pack_id}' to {}", destination.display()),
        ..Default::default()
    })
}

/// Replace one live local copy with the pack's currently shipped item.
///
/// Reset is deliberately distinct from restoring a removed item: it requires a
/// live fork, and it is the only operation that may remove a workspace MCP
/// shadow in favor of the pack definition. Resetting a machine service puts the
/// shipped declaration back in force, so it is the operator's, like installing
/// one.
pub fn reset_pack_item(
    orch: &Orchestrator,
    pack_id: &str,
    kind: PackItemKind,
    item_id: &str,
    authority: PackAuthority,
) -> Result<PackMutationResult, String> {
    let mut lock = pack::lock::read_lock(&orch.config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    let manifest = shipped_manifest(orch, &lock)?;
    if !manifest
        .items()
        .iter()
        .any(|item| item.kind == kind && item.id == item_id)
    {
        return Err(format!(
            "Pack `{pack_id}` does not currently ship {} `{item_id}`",
            kind.as_str()
        ));
    }
    if kind == PackItemKind::Service && authority != PackAuthority::Operator {
        return Err(machine_service_refusal(
            &format!("Resetting machine service `{pack_id}/{item_id}` to the shipped version"),
            pack_id,
        ));
    }
    let item = lock.item(kind, item_id).cloned().ok_or_else(|| {
        format!(
            "Pack `{pack_id}` does not carry {} `{item_id}`",
            kind.as_str()
        )
    })?;
    // A fork the lock has not recorded yet (an edit since the last sync) is
    // just as much the user's copy, and the catalog already offers its reset.
    let unrecorded_edit = kind != PackItemKind::Mcp
        && item.path.as_deref().is_some_and(|path| {
            pack::hash_item_path(
                kind,
                &orch.config_dir,
                &pack::item_destination(&orch.config_dir, pack_id, kind, path),
            )
            .and_then(|current| pack::matches_baseline(&orch.config_dir, &item, &current))
            .is_ok_and(|matches| !matches)
        });
    if !item.forked && !unrecorded_edit {
        return Err(format!(
            "{} `{item_id}` is already using pack `{pack_id}`'s version",
            kind.as_str()
        ));
    }

    let baseline = match kind {
        PackItemKind::Mcp => {
            let definitions =
                pack::mcp::parse_pack_mcp_file(&pack::lock::mcp_path(&orch.config_dir, pack_id))?
                    .servers;
            let definition = definitions.get(item_id).ok_or_else(|| {
                format!("Installed pack `{pack_id}` has no MCP definition `{item_id}`")
            })?;
            pack::hash_mcp_definition(definition)?
        }
        _ => {
            let path = item
                .path
                .as_deref()
                .ok_or_else(|| format!("{} `{item_id}` has no materialized path", kind.as_str()))?;
            match pack::hash_item_path(
                kind,
                &orch.config_dir,
                &pack::item_destination(&orch.config_dir, pack_id, kind, path),
            )? {
                ContentHash::Present(hash) => hash,
                ContentHash::Missing => {
                    return Err(format!(
                        "{} `{item_id}` is missing; restore it instead",
                        kind.as_str()
                    ))
                }
            }
        }
    };

    if kind == PackItemKind::Mcp {
        crate::config::mcp_tools::remove_server(
            &orch.config_dir,
            crate::config::mcp_tools::ToolScope::Workspace,
            item_id,
        )?;
    }
    let previous_lock = lock.clone();
    lock.reset_item_baseline(kind, item_id, baseline);
    pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &lock)?;
    if kind == PackItemKind::Mcp {
        // The shadow only: deleting the server would record it as removed from
        // the pack, and the re-sync below would then withhold the very
        // definition this reset restores.
        if let Err(error) =
            crate::config::mcp_servers::delete_workspace_mcp_shadow(&orch.config_dir, item_id)
        {
            let _ = pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &previous_lock);
            return Err(error);
        }
    }

    let result = resync_from_shipped(orch, &lock, manifest).inspect_err(|_| {
        let _ = pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &previous_lock);
    })?;

    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "reset-item".to_string(),
        pack_id: pack_id.to_string(),
        changed_paths: result.changed_paths,
        summary: format!(
            "Reset {} `{item_id}` to pack '{pack_id}'s version",
            kind.as_str()
        ),
        ..Default::default()
    })
}

/// Ids of every pack the app bundle ships, for collision refusals.
fn shipped_pack_ids(orch: &Orchestrator) -> Vec<String> {
    crate::library::shipped_ids(&orch.config_dir).unwrap_or_default()
}

/// What an installed pack shipped: the app bundle's copy for a bundled pack,
/// the verified snapshot for a local or library one.
fn shipped_manifest(
    orch: &Orchestrator,
    lock: &pack::PackLock,
) -> Result<pack::PackManifest, String> {
    match lock.source.kind {
        pack::PackSourceKind::Bundled => {
            let resource_dir = source_dir(orch)?;
            pack::available_pack(&resource_dir, &lock.id)
                .ok_or_else(|| format!("No pack `{}` is shipped in {resource_dir:?}", lock.id))
        }
        _ => crate::library::snapshot_manifest(&orch.config_dir, lock),
    }
}

/// Sync an installed pack again from what it shipped, without fetching.
fn resync_from_shipped(
    orch: &Orchestrator,
    lock: &pack::PackLock,
    manifest: pack::PackManifest,
) -> Result<pack::PackSyncResult, String> {
    match lock.source.kind {
        pack::PackSourceKind::Bundled => sync_one_pack(
            &RealGitClient,
            &RealFileSystem,
            &source_dir(orch)?,
            &orch.config_dir,
            &lock.id,
        ),
        _ => sync_resolved_pack(
            &RealGitClient,
            &RealFileSystem,
            &orch.config_dir,
            manifest,
            lock.source.clone(),
        ),
    }
}

/// Install or update one entry of a verified library index.
///
/// Fetches the entry's origin at the commit the index pins, stages it under
/// `packs/.library-stage-*`, and refuses unless every byte matches the index
/// and nothing it installs collides with what is already here. Only then does
/// the staged root become the pack's snapshot and run through the same sync as
/// every other pack; a failed sync puts the snapshot, lock, and items back.
pub(crate) fn install_library_entry(
    orch: &Orchestrator,
    source: &pack::library::LibrarySource,
    verified: &pack::library::VerifiedIndex,
    entry_id: &str,
    existing: Option<&pack::PackLock>,
) -> Result<PackMutationResult, String> {
    let config_dir = &orch.config_dir;
    let packs = config_dir.join(pack::lock::PACKS_DIR);
    std::fs::create_dir_all(&packs).map_err(|e| e.to_string())?;
    let stage = tempfile::Builder::new()
        .prefix(".library-stage-")
        .tempdir_in(&packs)
        .map_err(|e| e.to_string())?;
    let staged = pack::library::stage::stage_verified_entry(
        source,
        verified,
        entry_id,
        stage.path(),
        &pack::library::derive::PolicyFetch(orch.library.policy),
    )?;
    pack::check_install_collisions(
        config_dir,
        &staged.manifest,
        &staged.source,
        &shipped_pack_ids(orch),
    )?;

    let id = staged.manifest.id.clone();
    let destination = crate::library::snapshot_root(config_dir, &id);
    let transaction = PackMutationSnapshot::capture(config_dir, &id, existing, &staged.manifest)?;
    let backup = commit_snapshot(&staged.manifest.root, &destination)?;
    let mut manifest = staged.manifest.clone();
    manifest.root = destination.clone();
    let result = sync_resolved_pack(
        &RealGitClient,
        &RealFileSystem,
        config_dir,
        manifest,
        staged.source.clone(),
    )
    .map_err(|error| {
        rollback_plugin_snapshot(&destination, backup.as_deref());
        match transaction.restore() {
            Ok(()) => error,
            Err(rollback) => format!("{error}; rollback failed: {rollback}"),
        }
    })?;
    if let Some(backup) = backup {
        let _ = std::fs::remove_dir_all(backup);
    }
    drop(stage);
    orch.emit_pack_registry_change();
    let (action, verb) = match existing {
        Some(_) => ("update", "Updated"),
        None => ("install", "Installed"),
    };
    Ok(PackMutationResult {
        action: action.into(),
        pack_id: id.clone(),
        changed_paths: result.changed_paths,
        diagnostics: staged.manifest.notes_from(staged.source.kind),
        summary: format!("{verb} pack '{id}' from the {}", source.label),
        ..Default::default()
    })
}

/// The configured source whose verified cache offers `pack_id`, if any.
fn library_offer(
    orch: &Orchestrator,
    pack_id: &str,
) -> Result<Option<(pack::library::LibrarySource, pack::library::VerifiedIndex)>, String> {
    for source in &orch.library.sources {
        let cached = pack::library::cache::read_cached_index(&orch.config_dir, source);
        let Some(verified) = cached.verified() else {
            continue;
        };
        if let Some(refusal) = verified.refused.iter().find(|r| r.id == pack_id) {
            return Err(format!(
                "The {} offers `{pack_id}`, but it cannot be installed: {}",
                source.label, refusal.reason
            ));
        }
        if verified.entry(pack_id).is_some() {
            return Ok(Some((source.clone(), verified.clone())));
        }
    }
    Ok(None)
}

/// Update a pack installed from a library source.
///
/// An update that changes what runs on this machine (an endpoint or a stdio
/// server, a workflow) is the operator's to apply; for anyone else it is
/// refused, naming the Library page. With nothing new in the index, the pack
/// re-syncs from its snapshot without fetching.
fn update_library_pack(
    orch: &Orchestrator,
    lock: &pack::PackLock,
    authority: PackAuthority,
) -> Result<PackMutationResult, String> {
    let url = lock.source.url.as_deref().unwrap_or_default();
    let source = orch.library.source_for_url(url).cloned().ok_or_else(|| {
        format!(
            "Pack `{}` came from {url}, which this build does not read; it keeps working, but \
             its updates are frozen",
            lock.id
        )
    })?;
    let verified = match pack::library::cache::read_cached_index(&orch.config_dir, &source) {
        pack::library::cache::CachedIndex::Verified(verified) => *verified,
        pack::library::cache::CachedIndex::Refused(reason) => {
            return Err(format!(
                "Pack `{}` was not updated: the {} index cannot be used ({reason})",
                lock.id, source.label
            ))
        }
        pack::library::cache::CachedIndex::Absent => {
            return Err(format!(
                "Pack `{}` was not updated: the {} has not been fetched on this machine yet; \
                 open the Library to refresh it",
                lock.id, source.label
            ))
        }
    };
    let entry = verified.entry(&lock.id).ok_or_else(|| {
        format!(
            "The {} no longer offers `{}`; the installed copy keeps working",
            source.label, lock.id
        )
    })?;
    match crate::library::pending_update(&orch.config_dir, lock, entry) {
        crate::library::PendingUpdate::NeedsReview(changes)
            if authority != PackAuthority::Operator =>
        {
            let what = changes
                .iter()
                .map(|change| format!("{} `{}` {}", change.kind, change.id, change.change))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "Pack `{}`'s update {} ({what}), so only the operator may apply it, from the \
                 Library page in Settings. Updates that change only content apply on their own.",
                lock.id, OPERATOR_ONLY_PACK_ACT
            ));
        }
        crate::library::PendingUpdate::UpToDate => {
            if let Ok(manifest) = crate::library::snapshot_manifest(&orch.config_dir, lock) {
                let result = resync_from_shipped(orch, lock, manifest)?;
                orch.emit_pack_registry_change();
                return Ok(PackMutationResult {
                    action: "update".into(),
                    pack_id: lock.id.clone(),
                    changed_paths: result.changed_paths,
                    summary: format!("Pack '{}' is up to date with the {}", lock.id, source.label),
                    ..Default::default()
                });
            }
        }
        _ => {}
    }
    install_library_entry(orch, &source, &verified, &lock.id, Some(lock))
}

/// Restore removed items of a local or library pack from its snapshot.
fn restore_snapshot_pack(
    orch: &Orchestrator,
    lock: &pack::PackLock,
) -> Result<PackMutationResult, String> {
    // Resolve the snapshot before the removal records are discarded, so a
    // pack that could never be restored keeps them.
    let manifest = crate::library::snapshot_manifest(&orch.config_dir, lock)?;
    let restored = pack::lock::restore_removed_items(&RealFileSystem, &orch.config_dir, &lock.id)?;
    let result = resync_from_shipped(orch, lock, manifest).inspect_err(|_| {
        let _ = pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, lock);
    })?;
    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "restore".into(),
        pack_id: lock.id.clone(),
        changed_paths: result.changed_paths,
        summary: format!(
            "Restored {} removed item(s) to pack '{}'",
            restored.len(),
            lock.id
        ),
        restored_items: restored,
        ..Default::default()
    })
}

/// The app resource directory this workspace syncs from, recorded by the last
/// startup sync. Without it there is nothing to install FROM.
fn source_dir(orch: &Orchestrator) -> Result<std::path::PathBuf, String> {
    pack::source_dir(&orch.config_dir).ok_or_else(|| {
        "This workspace has no recorded app resource directory yet, so no pack can be installed \
         from it. It is recorded on the next app startup."
            .to_string()
    })
}

/// The refusal for an agent's act that would put a native machine service's
/// declaration in force; `act` is the sentence's subject.
fn machine_service_refusal(act: &str, pack_id: &str) -> String {
    format!(
        "{act} {OPERATOR_ONLY_PACK_ACT}: a machine service runs on a selected machine as a \
         resident, outside any fence, so only the operator may put it in place, from the \
         Library page in Settings. Nothing in pack `{pack_id}` was changed."
    )
}

/// Refuse an install, update, or restore of a pack that ships a native machine
/// service unless the operator asked. Only bundled packs can carry one (every
/// other source drops services when its manifest resolves), so this asks what
/// the pack would materialize from the bundle.
fn require_operator_for_machine_services(
    manifest: &pack::PackManifest,
    action: &str,
    authority: PackAuthority,
) -> Result<(), String> {
    if authority == PackAuthority::Operator {
        return Ok(());
    }
    let services: Vec<String> = manifest
        .items()
        .into_iter()
        .filter(|item| item.kind == PackItemKind::Service)
        .map(|item| format!("{}/{}", manifest.id, item.id))
        .collect();
    if services.is_empty() {
        return Ok(());
    }
    let verb = match action {
        "install" => "Installing",
        "restore" => "Restoring",
        _ => "Updating",
    };
    let noun = if services.len() == 1 {
        "machine service"
    } else {
        "machine services"
    };
    Err(machine_service_refusal(
        &format!(
            "{verb} pack `{}`, which carries the native {noun} {},",
            manifest.id,
            services.join(", ")
        ),
        &manifest.id,
    ))
}

/// Install, update, or restore one pack, as an agent.
///
/// Emits the registry change only after the filesystem, lock, and Git work has
/// succeeded, so a failed action never invalidates a caller's view of a
/// workspace that did not move.
pub fn apply_pack_action(
    orch: &Orchestrator,
    action: &str,
    pack_id: &str,
) -> Result<PackMutationResult, String> {
    apply_pack_action_as(orch, action, pack_id, PackAuthority::Agent)
}

/// [`apply_pack_action`] for a caller whose authority the transport has
/// established. A library update that changes what runs, and any act on a
/// pack that ships a native machine service, needs the operator.
pub fn apply_pack_action_as(
    orch: &Orchestrator,
    action: &str,
    pack_id: &str,
    authority: PackAuthority,
) -> Result<PackMutationResult, String> {
    let lock = pack::lock::read_lock(&orch.config_dir, pack_id);
    match (action, &lock) {
        ("update", Some(lock)) if lock.source.kind == pack::PackSourceKind::Local => {
            return update_local_plugin(orch, lock);
        }
        ("update", Some(lock)) if lock.source.kind == pack::PackSourceKind::Url => {
            return update_library_pack(orch, lock, authority);
        }
        ("restore", Some(lock)) if lock.source.kind != pack::PackSourceKind::Bundled => {
            return restore_snapshot_pack(orch, lock);
        }
        ("install", None) => {
            let shipped = pack::source_dir(&orch.config_dir)
                .and_then(|root| pack::available_pack(&root, pack_id))
                .is_some();
            if !shipped {
                if let Some((source, verified)) = library_offer(orch, pack_id)? {
                    return install_library_entry(orch, &source, &verified, pack_id, None);
                }
            }
        }
        _ => {}
    }
    // Resolve what this action will sync FROM before touching any lock. A
    // restore records its intent by rewriting the pack lock, so a source that
    // could never have been synced from must be refused while that record is
    // still intact — otherwise the reachable "installed pack whose shipped
    // source is gone" case clears the user's removal markers and then fails.
    let resource_dir = source_dir(orch)?;
    let Some(shipped) = pack::available_pack(&resource_dir, pack_id) else {
        return Err(format!(
            "No pack `{pack_id}` is shipped in {resource_dir:?}"
        ));
    };
    // Ahead of the restore below for the same reason: a refusal must leave the
    // lock, and the removal records in it, untouched.
    if matches!(action, "install" | "update" | "restore") {
        require_operator_for_machine_services(&shipped, action, authority)?;
    }

    let previous_lock = pack::lock::read_lock(&orch.config_dir, pack_id);
    let installed = previous_lock.is_some();
    let mut restored_items: Vec<PackItem> = Vec::new();
    match action {
        "install" if installed => {
            return Err(format!(
                "Pack `{pack_id}` is already installed. Use action:\"update\" to re-sync it."
            ))
        }
        "update" | "restore" if !installed => {
            return Err(format!(
                "Pack `{pack_id}` is not installed. Use action:\"install\" first."
            ))
        }
        "restore" => {
            restored_items =
                pack::lock::restore_removed_items(&RealFileSystem, &orch.config_dir, pack_id)?;
        }
        "install" | "update" => {}
        other => {
            return Err(format!(
                "Unknown pack action `{other}` (install | update | restore)"
            ))
        }
    }

    let result = match sync_one_pack(
        &RealGitClient,
        &RealFileSystem,
        &resource_dir,
        &orch.config_dir,
        pack_id,
    ) {
        Ok(result) => result,
        Err(error) => {
            // The restore above already discarded the removal records; the sync
            // that was to materialize those items did not run. Put the records
            // back, so the failure leaves the workspace exactly as it was and
            // the user keeps the way to try again.
            if let (false, Some(previous)) = (restored_items.is_empty(), previous_lock) {
                if let Err(rollback) =
                    pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &previous)
                {
                    log::error!(
                        "Pack `{pack_id}`: restore failed ({error}) and its removal records could \
                         not be put back ({rollback}); {} item(s) are now neither installed nor \
                         recorded as removed",
                        restored_items.len()
                    );
                }
            }
            return Err(error);
        }
    };

    let summary = match action {
        "install" => format!("Installed pack '{pack_id}'"),
        "restore" => format!(
            "Restored {} removed item(s) to pack '{pack_id}'",
            restored_items.len()
        ),
        _ => format!("Updated pack '{pack_id}'"),
    };
    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: action.to_string(),
        pack_id: pack_id.to_string(),
        changed_paths: result.changed_paths,
        restored_items,
        summary,
        ..Default::default()
    })
}

pub fn apply_pack_delete(orch: &Orchestrator, pack_id: &str) -> Result<PackMutationResult, String> {
    let lock = pack::lock::read_lock(&orch.config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    // Evict before discarding the lock: a failed sidecar write must leave the
    // names available for retry. A surviving user fork or another pack may
    // need to refetch, but neither its configuration nor credentials are ours
    // to delete. Project catalogs belong to a separate scope.
    let servers: BTreeSet<_> = lock
        .items
        .iter()
        .filter(|item| item.kind == PackItemKind::Mcp)
        .map(|item| item.id.as_str())
        .chain(
            lock.removed
                .iter()
                .filter(|item| item.kind == PackItemKind::Mcp)
                .map(|item| item.id.as_str()),
        )
        .collect();
    for server in &servers {
        crate::config::mcp_tools::remove_server(
            &orch.config_dir,
            crate::config::mcp_tools::ToolScope::Workspace,
            server,
        )?;
    }
    let result = uninstall_pack(&RealGitClient, &RealFileSystem, &orch.config_dir, pack_id)?;
    crate::config::machine_services::clear_selections(
        orch,
        crate::config::machine_services::service_keys(&lock),
    )?;
    // Again after the registry no longer offers them: a capture published
    // between the eviction above and the uninstall would otherwise survive it.
    for server in &servers {
        crate::config::mcp_tools::remove_server(
            &orch.config_dir,
            crate::config::mcp_tools::ToolScope::Workspace,
            server,
        )?;
    }
    let mut summary = format!(
        "Uninstalled pack '{pack_id}' — removed {} item(s)",
        result.removed.len()
    );
    if !result.kept.is_empty() {
        summary.push_str(&format!(
            "; kept your edited copies of {}",
            result.kept.join(", ")
        ));
    }

    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "uninstall".to_string(),
        pack_id: pack_id.to_string(),
        removed_paths: result.removed,
        kept_paths: result.kept,
        summary,
        ..Default::default()
    })
}

/// Take ONE item out of an installed pack, leaving the pack installed and still
/// updating around the gap.
///
/// The deletion itself runs through the same kind-specific function an ordinary
/// "delete this agent" performs, which is what records the removal against the
/// owning pack (`pack::note_removed_item`). Doing it any other way would give
/// the settings screen a second definition of what deleting a skill means — and
/// would, for an MCP server, be the difference between dropping a connector and
/// discarding the credential behind it.
pub fn apply_pack_item_removal(
    orch: &Orchestrator,
    pack_id: &str,
    kind: PackItemKind,
    item_id: &str,
) -> Result<PackMutationResult, String> {
    let lock = pack::lock::read_lock(&orch.config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    if kind == PackItemKind::Mcp
        && (lock.is_removed(kind, item_id) || lock.item(kind, item_id).is_some())
    {
        crate::config::mcp_tools::remove_server(
            &orch.config_dir,
            crate::config::mcp_tools::ToolScope::Workspace,
            item_id,
        )?;
    }
    if lock.is_removed(kind, item_id) {
        return Ok(PackMutationResult {
            action: "remove-item".to_string(),
            pack_id: pack_id.to_string(),
            removed_item: Some(PackItem {
                kind,
                id: item_id.to_string(),
                path: None,
            }),
            summary: format!(
                "{} `{item_id}` was already removed from pack '{pack_id}'",
                kind.as_str()
            ),
            ..Default::default()
        });
    }
    if !lock
        .items
        .iter()
        .any(|item| item.kind == kind && item.id == item_id)
    {
        return Err(format!(
            "Pack `{pack_id}` does not carry {} `{item_id}`",
            kind.as_str()
        ));
    }

    let config_dir = orch.config_dir.as_path();
    match kind {
        PackItemKind::Agent => crate::config::agents::delete_agent(config_dir, item_id, None)?,
        PackItemKind::Skill => crate::config::skills::delete_skill(config_dir, item_id, None)?,
        PackItemKind::Recipe => crate::config::recipes::delete_recipe(config_dir, item_id, None)?,
        PackItemKind::Response => {
            crate::config::responses::delete_response(config_dir, item_id, None)?;
        }
        PackItemKind::Route => {
            crate::config::routes::delete_route(config_dir, item_id, None)?;
        }
        PackItemKind::Workflow => {
            crate::config::workflows::delete_workflow(config_dir, item_id, None)?
        }
        PackItemKind::Service => {
            crate::config::machine_services::delete_pack_service(orch, pack_id, item_id)?;
        }
        // A provider and the credential it reads live in the pack's own layer
        // and are one capability: they leave by uninstalling the pack.
        PackItemKind::Credential | PackItemKind::Provider => {
            return Err(format!(
                "{} `{item_id}` cannot be removed on its own; uninstall pack '{pack_id}' to \
                 remove it",
                kind.as_str()
            ))
        }
        PackItemKind::Mcp => crate::config::mcp_tools::withdraw_and_unregister(
            config_dir,
            crate::config::mcp_tools::ToolScope::Workspace,
            item_id,
            || crate::config::mcp_servers::delete_workspace_mcp_server(config_dir, item_id),
        )?,
    }

    // The delete above records the removal against whichever installed pack
    // claims the item. Re-reading the lock is how this reports the item as the
    // catalog will show it, and how a silently dropped record becomes an error
    // instead of a removal the next sync undoes.
    let removed = pack::lock::read_lock(config_dir, pack_id)
        .and_then(|lock| {
            lock.removed
                .into_iter()
                .find(|item| item.kind == kind && item.id == item_id)
        })
        .ok_or_else(|| {
            format!(
                "Removed {} `{item_id}`, but pack '{pack_id}' did not record it; the next sync \
                 would restore it",
                kind.as_str()
            )
        })?;

    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "remove-item".to_string(),
        pack_id: pack_id.to_string(),
        removed_paths: removed.path.clone().into_iter().collect(),
        summary: format!(
            "Removed {} `{item_id}` from pack '{pack_id}' — the pack stays installed",
            kind.as_str()
        ),
        removed_item: Some(removed),
        ..Default::default()
    })
}

/// The `cairn://packs/{id}` patch arm: read the action off the payload and run
/// the same function the settings command runs. A resource write carries no
/// operator credential, so it always acts as an agent; an act that needs the
/// operator is refused naming the Library page, where the operator makes it.
pub(super) fn dispatch_pack_action(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    pack_id: &str,
) -> Result<PackMutationResult, String> {
    let action = super::payload_trimmed_non_empty_str(payload, "action", &[])
        .ok_or("payload.action is required (install | update | restore | reset-item)")?;
    if action == "export" {
        let path = super::payload_trimmed_non_empty_str(payload, "path", &[])
            .ok_or("payload.path is required for export")?;
        export_agent_plugin(orch, pack_id, Path::new(path))
    } else if action == "reset-item" {
        let kind = super::payload_trimmed_non_empty_str(payload, "kind", &[])
            .ok_or("payload.kind is required for reset-item")?;
        let item_id = super::payload_trimmed_non_empty_str(payload, "itemId", &[])
            .ok_or("payload.itemId is required for reset-item")?;
        let kind =
            PackItemKind::parse(kind).ok_or_else(|| format!("Unknown pack item kind `{kind}`"))?;
        reset_pack_item(orch, pack_id, kind, item_id, PackAuthority::Agent)
    } else {
        apply_pack_action(orch, action, pack_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::resources::packs::{pack_catalog, PackItemState, PackState};
    use crate::services::testing::TestServicesBuilder;
    use crate::services::EventEmitter;
    use crate::storage::SearchIndex;
    use crate::workspace::bundle::sync_workspace_packs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    /// A capturing emitter the test can keep a handle on after handing it to
    /// the orchestrator.
    #[derive(Clone, Default)]
    struct RecordingEmitter {
        events: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    }

    impl EventEmitter for RecordingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push((event.to_string(), payload));
            Ok(())
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.emit(event, serde_json::Value::Null)
        }
    }

    struct Workspace {
        orch: Orchestrator,
        emitter: RecordingEmitter,
        home: PathBuf,
        resources: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn source_pack(resources: &Path, id: &str) -> PathBuf {
        resources.join("packs").join(id)
    }

    /// A loadable, enabled route, so the routing worker's cached route set
    /// actually holds it.
    const PING_ROUTE: &str = "name: Ping\ndescription: fixture route\nenabled: true\nnodes:\n  - id: in\n    type: trigger\n    when: { fact: github_comment }\n  - id: out\n    type: sink\n    sink: { kind: issue, labels: [x] }\nedges:\n  - { from: in, to: out }\n";

    const PROBE_SERVICE: &str = "name: Probe\nruntime: native\nplacement: per-machine\ntransport: mcp-stdio\nargs: [serve]\nartifacts:\n  version: 1.2.3\n  prove: [--version]\n  targets:\n    x86_64-unknown-linux-gnu:\n      url: https://example.invalid/probe\n      sha256: eb5765e715747caccda94ba30266d6d8616861bc9893ca152700546db938df54\n      size: 10\n      executable: probe\nprobe:\n  args: [status]\n  every: 5m\n  ready: /ready\n";

    /// A `core` pack carrying one of every file-backed kind, plus an optional
    /// `matlab` pack whose only content is a connector — between them every
    /// `PackItemKind` is represented.
    fn write_resources(resources: &Path) {
        write(
            &source_pack(resources, "core").join("cairn-pack.yaml"),
            "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
        );
        let core = source_pack(resources, "core");
        write(&core.join("agents/explore.md"), "bundle agent\n");
        write(&core.join("recipes/default.yaml"), "name: default\n");
        write(&core.join("responses/conveyor.md"), "response\n");
        write(&core.join("routes/ping.yaml"), PING_ROUTE);
        write(
            &core.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\n",
        );
        write(&core.join("workflows/flow/workflow.yaml"), "name: Flow\n");
        write(&core.join("services/probe/service.yaml"), PROBE_SERVICE);

        write(
            &source_pack(resources, "matlab").join("cairn-pack.yaml"),
            "id: matlab\nname: MATLAB\nversion: 1.0.0\ndefault: false\n",
        );
        write(
            &source_pack(resources, "matlab").join("mcp.yaml"),
            "mcpServers:\n  matlab:\n    type: stdio\n    command: ${MATLAB_BIN}\n",
        );
    }

    async fn workspace() -> Workspace {
        workspace_with(None).await
    }

    async fn workspace_with(library: Option<crate::library::LibraryContext>) -> Workspace {
        crate::config::secrets::mock_keychain::install();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_resources(&resources);
        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &home, "1.0.0").unwrap();

        let db = crate::storage::migrated_test_db("pack-mutations.db").await;
        let index = SearchIndex::open_or_create(temp.path().join("index")).unwrap();
        let db_state = Arc::new(DbState::new(Arc::new(db), Arc::new(index)));
        let emitter = RecordingEmitter::default();
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_emitter(emitter.clone())
                .build(),
        );
        let mut builder = Orchestrator::builder(db_state, services, home.clone());
        if let Some(library) = library {
            builder = builder.library_context(library);
        }
        let orch = builder.build();
        Workspace {
            orch,
            emitter,
            home,
            resources,
            _temp: temp,
        }
    }

    // Legacy rows exercise physical removal without depending on the current
    // binding schema or treating an unverifiable record as a cache hit.
    fn seed_catalogs(ws: &Workspace) -> serde_json::Value {
        let fixture = serde_json::json!({
            "workspace": {"matlab": [], "unrelated": []},
            "projects": {"/repo/project": {"matlab": []}}
        });
        write(
            &ws.home.join("mcp_tools.json"),
            &serde_json::to_string(&fixture).unwrap(),
        );
        fixture
    }

    fn assert_workspace_catalog_withdrawn(ws: &Workspace, before: &serde_json::Value) {
        let after: serde_json::Value =
            serde_json::from_slice(&std::fs::read(ws.home.join("mcp_tools.json")).unwrap())
                .unwrap();
        assert!(after["workspace"].get("matlab").is_none());
        assert_eq!(
            after["workspace"]["unrelated"],
            before["workspace"]["unrelated"]
        );
        assert_eq!(after["projects"], before["projects"]);
    }

    #[tokio::test]
    async fn uninstall_withdraws_catalog_without_deleting_a_user_fork() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        write(
            &ws.home.join("settings.yaml"),
            "mcpServers:\n  matlab:\n    type: stdio\n    command: user-matlab\n",
        );
        let settings = std::fs::read(ws.home.join("settings.yaml")).unwrap();
        let before = seed_catalogs(&ws);

        apply_pack_delete(&ws.orch, "matlab").unwrap();

        assert_workspace_catalog_withdrawn(&ws, &before);
        assert_eq!(
            std::fs::read(ws.home.join("settings.yaml")).unwrap(),
            settings
        );
        assert!(pack::lock::read_lock(&ws.home, "matlab").is_none());
        assert_eq!(
            crate::config::mcp_servers::load_workspace_mcp_servers(&ws.home)["matlab"]
                .command
                .as_deref(),
            Some("user-matlab")
        );
    }

    #[tokio::test]
    async fn item_removal_and_retry_withdraw_only_workspace_catalog() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        for _ in 0..2 {
            let before = seed_catalogs(&ws);
            apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
            assert_workspace_catalog_withdrawn(&ws, &before);
            assert!(pack::lock::read_lock(&ws.home, "matlab")
                .unwrap()
                .is_removed(PackItemKind::Mcp, "matlab"));
            assert!(
                !crate::config::mcp_servers::load_workspace_mcp_servers(&ws.home)
                    .contains_key("matlab")
            );
        }
    }

    #[tokio::test]
    async fn uninstall_withdraws_orphan_catalog_for_a_previously_removed_item() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
        let before = seed_catalogs(&ws);

        apply_pack_delete(&ws.orch, "matlab").unwrap();

        assert_workspace_catalog_withdrawn(&ws, &before);
    }

    #[tokio::test]
    async fn reset_withdraws_the_fork_catalog_before_restoring_pack_definition() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        // An MCP fork is an edit to the pack's installed layer; a settings entry
        // only shadows it. Make both, so the reset must undo both.
        write(
            &pack::lock::mcp_path(&ws.home, "matlab"),
            "mcpServers:\n  matlab:\n    type: stdio\n    command: forked-matlab\n",
        );
        write(
            &ws.home.join("settings.yaml"),
            "mcpServers:\n  matlab:\n    type: stdio\n    command: user-matlab\n",
        );
        apply_pack_action(&ws.orch, "update", "matlab").unwrap();
        assert!(
            pack::lock::read_lock(&ws.home, "matlab")
                .unwrap()
                .item(PackItemKind::Mcp, "matlab")
                .unwrap()
                .forked
        );
        let before = seed_catalogs(&ws);

        reset_pack_item(
            &ws.orch,
            "matlab",
            PackItemKind::Mcp,
            "matlab",
            PackAuthority::Agent,
        )
        .unwrap();

        assert_workspace_catalog_withdrawn(&ws, &before);
        assert!(
            !crate::config::mcp_servers::load_settings_mcp_servers(&ws.home).contains_key("matlab")
        );
        assert!(pack::mcp::load_pack_mcp_servers(&ws.home).contains_key("matlab"));
    }

    /// The fixture's `core` ships the probe service, so acting on it is the
    /// operator's.
    fn as_operator(
        ws: &Workspace,
        action: &str,
        pack_id: &str,
    ) -> Result<PackMutationResult, String> {
        apply_pack_action_as(&ws.orch, action, pack_id, PackAuthority::Operator)
    }

    fn pack_events(ws: &Workspace) -> usize {
        ws.emitter
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, payload)| name == "config-changed" && payload["entity_type"] == "pack")
            .count()
    }

    fn item_state(ws: &Workspace, pack_id: &str, kind: &str, id: &str) -> Option<PackItemState> {
        pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        )
        .find(pack_id)?
        .items
        .iter()
        .find(|item| item.kind == kind && item.id == id)
        .map(|item| item.state)
    }

    /// Removing one item is a first-class operation for every kind a pack can
    /// carry — not just the file-backed ones, and never at the cost of the pack
    /// around it. The MCP arm is the one that would silently regress: its
    /// deletion path is the only one that also touches a settings file.
    fn select_probe(ws: &Workspace, key: &cairn_config::fleet::MachineServiceKey) {
        crate::fleet::management::mutate_fleet(&ws.orch, |config| {
            config.machine_services.select(
                key.clone(),
                "alpha",
                cairn_config::fleet::MachineSelection {
                    override_: Some(cairn_config::fleet::CommandOverride {
                        command: "/opt/probe".to_string(),
                    }),
                },
            )
        })
        .unwrap();
    }

    fn selected_on(ws: &Workspace, key: &cairn_config::fleet::MachineServiceKey) -> Vec<String> {
        crate::config::settings::load_fleet(&ws.home)
            .machine_services
            .machines_for(key)
            .into_iter()
            .map(|(machine, _)| machine.to_string())
            .collect()
    }

    /// Which machines run a service is fleet state, but a removed service has
    /// no business keeping it: the row must not outlive what it selects, and
    /// bringing the service back starts with no machine selected.
    #[tokio::test]
    async fn removing_a_service_or_uninstalling_its_pack_clears_its_selections() {
        let ws = workspace().await;
        let key: cairn_config::fleet::MachineServiceKey = "core/probe".parse().unwrap();

        select_probe(&ws, &key);
        assert_eq!(selected_on(&ws, &key), ["alpha"]);
        apply_pack_item_removal(&ws.orch, "core", PackItemKind::Service, "probe").unwrap();
        assert!(selected_on(&ws, &key).is_empty(), "overrides included");

        as_operator(&ws, "restore", "core").unwrap();
        assert!(ws.home.join("services/probe/service.yaml").exists());
        assert!(selected_on(&ws, &key).is_empty(), "restore selects nothing");

        select_probe(&ws, &key);
        apply_pack_delete(&ws.orch, "core").unwrap();
        assert!(selected_on(&ws, &key).is_empty());

        as_operator(&ws, "install", "core").unwrap();
        assert!(
            selected_on(&ws, &key).is_empty(),
            "reinstall selects nothing"
        );
    }

    /// A manifest shipping one service is that service: it lists under Machine
    /// services, with a row for each machine it ships an artifact for and each
    /// machine that selected it, and the agent's page says the same. An empty
    /// section does not render at all.
    #[tokio::test]
    async fn a_single_service_manifest_lists_under_machine_services_with_its_machines() {
        use crate::config::machine_services::{
            InstanceView, MachineCandidate, MachineServiceSnapshot,
        };
        use crate::config::mcp_servers::MachineStateTag;
        use crate::project_services::supervisor::machine::InstanceState;
        use crate::resources::packs::{read_pack, read_packs};

        let ws = workspace().await;
        let none = MachineServiceSnapshot::empty();
        assert!(
            !read_packs(&ws.home, &ws.orch.library, &none, None).contains("## Machine services"),
            "a pack's service does not make the section, and an empty one does not render"
        );

        let solo = source_pack(&ws.resources, "solo");
        write(
            &solo.join("cairn-pack.yaml"),
            "id: solo\nname: Solo\nversion: 1.0.0\n",
        );
        write(&solo.join("services/solo/service.yaml"), PROBE_SERVICE);
        as_operator(&ws, "install", "solo").unwrap();

        let candidate = |name: &str, target: &str| MachineCandidate {
            name: name.to_string(),
            target: Some(target.to_string()),
            attached: true,
        };
        let machines = MachineServiceSnapshot {
            instances: std::collections::BTreeMap::from([(
                "solo/solo".parse().unwrap(),
                vec![InstanceView {
                    machine: "selected-anyway".to_string(),
                    state: InstanceState::WaitingForOperator {
                        reason: "permission-denied".to_string(),
                    },
                    installed_version: None,
                    override_command: Some("/opt/solo".to_string()),
                    provenance: "override /opt/solo (someone)".to_string(),
                    serves_mcp: true,
                }],
            )]),
            machines: vec![
                candidate("shipped-for", "x86_64-unknown-linux-gnu"),
                candidate("not-shipped-for", "aarch64-apple-darwin"),
                candidate("selected-anyway", "aarch64-apple-darwin"),
            ],
        };

        let catalog = pack_catalog(&ws.home, &ws.orch.library, &machines);
        let entry = catalog.find("solo").unwrap();
        assert_eq!(entry.entry_kind, "service");
        assert_eq!(entry.runs, pack::ItemRuns::Here);
        let service = entry.items[0].machine_service.as_ref().unwrap();
        assert_eq!(service.key, "solo/solo");
        assert_eq!(service.pinned_version.as_deref(), Some("1.2.3"));
        assert_eq!(service.transport.as_deref(), Some("mcp-stdio"));
        let names: Vec<&str> = service.machines.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["shipped-for", "selected-anyway"]);
        assert!(!service.machines[0].selected && service.machines[0].state.is_none());
        let selected = &service.machines[1];
        assert!(selected.selected);
        assert_eq!(selected.state, Some(MachineStateTag::WaitingForOperator));
        assert_eq!(selected.override_command.as_deref(), Some("/opt/solo"));
        assert_eq!(
            selected.provenance.as_deref(),
            Some("override /opt/solo (someone)"),
            "who set it comes from the data, never a hardcoded word"
        );

        let page = read_packs(&ws.home, &ws.orch.library, &machines, None);
        assert!(page.contains("## Machine services (1)"), "{page}");
        let detail = read_pack(&ws.home, &ws.orch.library, &machines, "solo");
        assert!(
            detail.contains("selected-anyway: waiting for the operator"),
            "{detail}"
        );
        assert!(detail.contains("override /opt/solo (someone)"), "{detail}");
        assert!(detail.contains("machineServices"), "{detail}");
    }

    /// A local or remote source's service never reaches the workspace, cannot be
    /// selected because nothing declares it, and the pack's page says why.
    #[tokio::test]
    async fn a_local_pack_lists_no_service_and_carries_the_sentence() {
        use crate::config::machine_services::MachineServiceSnapshot;
        use crate::resources::packs::read_pack;

        let ws = workspace().await;
        let source = ws._temp.path().join("plugin-source");
        write(
            &source.join("cairn-pack.yaml"),
            "id: local-kit\nname: Local Kit\n",
        );
        write(
            &source.join("skills/local-kit/SKILL.md"),
            "---\nname: local-kit\ndescription: Useful.\n---\n",
        );
        write(
            &source.join("services/local-probe/service.yaml"),
            PROBE_SERVICE,
        );
        sync_resolved_pack(
            &RealGitClient,
            &RealFileSystem,
            &ws.home,
            pack::manifest::load(&source).unwrap(),
            pack::PackSource::local(source.to_string_lossy().into_owned()),
        )
        .unwrap();

        let none = MachineServiceSnapshot::empty();
        let catalog = pack_catalog(&ws.home, &ws.orch.library, &none);
        let entry = catalog.find("local-kit").unwrap();
        assert!(entry.items.iter().all(|item| item.kind != "service"));
        assert!(!ws.home.join("services/local-probe").exists());
        let sentence = pack::manifest::NON_BUNDLED_SERVICES_NOTE;
        assert!(entry.notes.iter().any(|note| note.contains(sentence)));
        assert!(read_pack(&ws.home, &ws.orch.library, &none, "local-kit").contains(sentence));
        assert!(
            crate::project_services::pack_source::declared_machine_service(
                &ws.home,
                &"local-kit/local-probe".parse().unwrap(),
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn every_item_kind_can_be_removed_and_restored_without_uninstalling_its_pack() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();

        let cases: [(&str, PackItemKind, &str, &str); 8] = [
            ("core", PackItemKind::Agent, "explore", "agents/explore.md"),
            (
                "core",
                PackItemKind::Recipe,
                "default",
                "recipes/default.yaml",
            ),
            (
                "core",
                PackItemKind::Response,
                "conveyor",
                "responses/conveyor.md",
            ),
            ("core", PackItemKind::Route, "ping", "routes/ping.yaml"),
            ("core", PackItemKind::Service, "probe", "services/probe"),
            ("core", PackItemKind::Skill, "example", "skills/example"),
            ("core", PackItemKind::Workflow, "flow", "workflows/flow"),
            ("matlab", PackItemKind::Mcp, "matlab", ""),
        ];

        for (pack_id, kind, id, path) in cases {
            let result = apply_pack_item_removal(&ws.orch, pack_id, kind, id).unwrap();
            assert_eq!(result.action, "remove-item");
            assert_eq!(result.removed_item.as_ref().unwrap().kind, kind);
            if !path.is_empty() {
                assert!(
                    !ws.home.join(path).exists(),
                    "{path} should be gone after removing {kind:?} `{id}`"
                );
            }
            assert_eq!(
                item_state(&ws, pack_id, kind.as_str(), id),
                Some(PackItemState::RemovedByUser),
                "the catalog must report {kind:?} `{id}` as removed by the user"
            );
            assert!(
                pack::lock::read_lock(&ws.home, pack_id).is_some(),
                "removing one item must leave pack `{pack_id}` installed"
            );
        }

        assert!(
            !crate::config::pack::mcp::load_pack_mcp_servers(&ws.home).contains_key("matlab"),
            "a removed connector stops being offered"
        );

        // A restore per pack brings every one of them back, materialized.
        // `core` ships the probe service, so its restore is the operator's.
        for restored in [
            as_operator(&ws, "restore", "core").unwrap(),
            apply_pack_action(&ws.orch, "restore", "matlab").unwrap(),
        ] {
            assert!(!restored.restored_items.is_empty());
        }
        for path in [
            "agents/explore.md",
            "recipes/default.yaml",
            "responses/conveyor.md",
            "routes/ping.yaml",
            "services/probe/service.yaml",
            "skills/example/SKILL.md",
            "workflows/flow/workflow.yaml",
        ] {
            assert!(
                ws.home.join(path).exists(),
                "restore must bring back {path}"
            );
        }
        assert!(crate::config::pack::mcp::load_pack_mcp_servers(&ws.home).contains_key("matlab"));
        for (pack_id, kind, id, _) in cases {
            assert_eq!(
                item_state(&ws, pack_id, kind.as_str(), id),
                Some(PackItemState::PackOwned),
                "restored {kind:?} `{id}` in pack `{pack_id}` must return to pack ownership"
            );
        }
    }

    /// An edit saved between syncs is the user's copy at once: the catalog
    /// says so, says the app ships something different, shows both sides,
    /// A provider pack shipped beside `core`, installed into `ws`.
    fn install_provider_fixture(ws: &Workspace) -> PathBuf {
        let root = source_pack(&ws.resources, "search-fixture");
        write(
            &root.join("cairn-pack.yaml"),
            "id: search-fixture\nname: Search\nversion: 1.0.0\ndefault: false\n",
        );
        write(
            &root.join("credentials/api-key.yaml"),
            "label: Key\nkind: secret\nvar: SEARCH_FIXTURE_KEY\nhosts: [api.search.test]\naccessors: [provider:web-search]\n",
        );
        write(
            &root.join("providers/web-search.yaml"),
            "label: Search\nprovides: web-search\ncredential: api-key\nadapter:\n  http:\n    request:\n      method: GET\n      url: https://api.search.test/?q=${query|query}\n      headers: { X-Key: \"${credential}\" }\n    response:\n      results: results\n      fields: { url: [url] }\n",
        );
        apply_pack_action(&ws.orch, "install", "search-fixture").unwrap();
        root
    }

    /// Reset reads a provider's and a credential's real home, the pack layer.
    #[tokio::test]
    async fn reset_restores_edited_provider_and_credential_declarations() {
        let ws = workspace().await;
        let shipped = install_provider_fixture(&ws);
        for (kind, id, rel) in [
            (
                PackItemKind::Provider,
                "web-search",
                "providers/web-search.yaml",
            ),
            (
                PackItemKind::Credential,
                "api-key",
                "credentials/api-key.yaml",
            ),
        ] {
            let installed = ws.home.join("packs/search-fixture").join(rel);
            let original = std::fs::read_to_string(shipped.join(rel)).unwrap();
            std::fs::write(&installed, format!("{original}# edited\n")).unwrap();
            assert_eq!(
                item_state(&ws, "search-fixture", kind.as_str(), id),
                Some(PackItemState::EditedByUser)
            );
            reset_pack_item(&ws.orch, "search-fixture", kind, id, PackAuthority::Agent).unwrap();
            assert_eq!(std::fs::read_to_string(&installed).unwrap(), original);
            assert_eq!(
                item_state(&ws, "search-fixture", kind.as_str(), id),
                Some(PackItemState::PackOwned)
            );
        }
    }

    /// A failed mutation's rollback covers the declarations in the pack layer,
    /// not just the lock.
    #[tokio::test]
    async fn a_rollback_restores_installed_provider_declarations() {
        let ws = workspace().await;
        install_provider_fixture(&ws);
        let lock = pack::lock::read_lock(&ws.home, "search-fixture").unwrap();
        let manifest = pack::available_pack(&ws.resources, "search-fixture").unwrap();
        let provider = ws
            .home
            .join("packs/search-fixture/providers/web-search.yaml");
        let credential = ws
            .home
            .join("packs/search-fixture/credentials/api-key.yaml");
        let before = (
            std::fs::read(&provider).unwrap(),
            std::fs::read(&credential).unwrap(),
            std::fs::read(pack::lock::lock_path(&ws.home, "search-fixture")).unwrap(),
        );

        let snapshot =
            PackMutationSnapshot::capture(&ws.home, "search-fixture", Some(&lock), &manifest)
                .unwrap();
        // What a half-applied update leaves behind.
        std::fs::write(&provider, "label: half-written\n").unwrap();
        std::fs::remove_file(&credential).unwrap();
        std::fs::write(pack::lock::lock_path(&ws.home, "search-fixture"), "garbage").unwrap();
        snapshot.restore().unwrap();

        assert_eq!(std::fs::read(&provider).unwrap(), before.0);
        assert_eq!(std::fs::read(&credential).unwrap(), before.1);
        assert_eq!(
            std::fs::read(pack::lock::lock_path(&ws.home, "search-fixture")).unwrap(),
            before.2
        );
    }

    /// and its reset works before any sync has recorded the fork.
    #[tokio::test]
    async fn an_unsynced_edit_reads_as_a_copy_behind_shipped_and_resets() {
        let ws = workspace().await;
        write(&ws.home.join("agents/explore.md"), "my agent\n");

        let entry = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        )
        .find("core")
        .unwrap()
        .clone();
        let item = entry
            .items
            .iter()
            .find(|item| item.kind == "agent" && item.id == "explore")
            .unwrap();
        assert_eq!(item.state, PackItemState::EditedByUser);
        assert!(item.behind_shipped);
        assert_eq!((entry.edited_count, entry.behind_count), (1, 1));

        let diff = crate::resources::packs::pack_item_diff(
            &ws.home,
            "core",
            PackItemKind::Agent,
            "explore",
        )
        .unwrap();
        assert_eq!(diff.files.len(), 1);
        assert_eq!(diff.files[0].shipped.as_deref(), Some("bundle agent\n"));
        assert_eq!(diff.files[0].yours.as_deref(), Some("my agent\n"));

        // A skill differs only in the files that differ; Finder metadata is no file.
        write(&ws.home.join("skills/example/.DS_Store"), "finder");
        write(&ws.home.join("skills/example/notes.md"), "mine\n");
        let skill = crate::resources::packs::pack_item_diff(
            &ws.home,
            "core",
            PackItemKind::Skill,
            "example",
        )
        .unwrap();
        assert_eq!(skill.files.len(), 1);
        assert_eq!(skill.files[0].path, "skills/example/notes.md");
        assert_eq!(skill.files[0].shipped, None);

        reset_pack_item(
            &ws.orch,
            "core",
            PackItemKind::Agent,
            "explore",
            PackAuthority::Agent,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(ws.home.join("agents/explore.md")).unwrap(),
            "bundle agent\n"
        );
        assert_eq!(
            item_state(&ws, "core", "agent", "explore"),
            Some(PackItemState::PackOwned)
        );
    }

    /// The routing worker caches each scope's route set. A pack mutation must
    /// drop that cache when it lands, not when the cache ages out: a removed
    /// route that kept firing would still run its sinks, and a restored one
    /// would leave facts unmatched in between.
    #[tokio::test]
    async fn pack_mutations_invalidate_the_cached_route_set_immediately() {
        let ws = workspace().await;
        let cached = |ws: &Workspace| {
            ws.orch
                .fact_log
                .index
                .routes(&ws.orch.config_dir, None, ws.orch.services.clock.now())
                .unwrap()
                .routes_for("github_comment")
                .count()
        };
        assert_eq!(cached(&ws), 1, "the shipped route is loaded and cached");

        apply_pack_item_removal(&ws.orch, "core", PackItemKind::Route, "ping").unwrap();
        assert_eq!(cached(&ws), 0, "a removed route must stop matching at once");

        as_operator(&ws, "restore", "core").unwrap();
        assert_eq!(cached(&ws), 1, "a restored route must match at once");
    }

    /// Deleting a shipped route or response through its `cairn://` resource is a
    /// user removal like any other: the next pack sync must not copy it back.
    #[tokio::test]
    async fn a_resource_delete_of_a_shipped_item_survives_the_next_sync() {
        let ws = workspace().await;
        let request = crate::mcp::types::McpCallbackRequest {
            answer_bound_ms: None,
            cwd: "/empty/scratch".to_string(),
            run_id: None,
            tool: "write".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            thread_id: None,
            origin: None,
        };
        let events_before = pack_events(&ws);
        super::super::routes::delete(&ws.orch, &request, "ping", None, false)
            .await
            .unwrap();
        super::super::responses::delete(&ws.orch, &request, "conveyor", None)
            .await
            .unwrap();
        assert_eq!(pack_events(&ws), events_before + 2);

        sync_workspace_packs(
            &RealGitClient,
            &RealFileSystem,
            &ws.resources,
            &ws.home,
            "1.0.1",
        )
        .unwrap();
        for (kind, id, path) in [
            ("route", "ping", "routes/ping.yaml"),
            ("response", "conveyor", "responses/conveyor.md"),
        ] {
            assert!(
                !ws.home.join(path).exists(),
                "the sync must not copy back {path}"
            );
            assert_eq!(
                item_state(&ws, "core", kind, id),
                Some(PackItemState::RemovedByUser)
            );
        }
    }

    /// An update must name the files it refused to overwrite. Flattened into
    /// prose those names can only be re-parsed; the settings screen reports them
    /// from the structured field.
    #[tokio::test]
    async fn an_update_preserves_a_local_copy_and_the_catalog_marks_it() {
        let ws = workspace().await;
        write(&ws.home.join("agents/explore.md"), "mine now\n");

        write(
            &source_pack(&ws.resources, "core").join("agents/explore.md"),
            "shipped v2\n",
        );
        let result = as_operator(&ws, "update", "core").unwrap();

        assert_eq!(result.action, "update");
        assert_eq!(
            item_state(&ws, "core", "agent", "explore"),
            Some(PackItemState::EditedByUser)
        );
        assert_eq!(
            std::fs::read_to_string(ws.home.join("agents/explore.md")).unwrap(),
            "mine now\n",
            "an update must not overwrite what the user made theirs"
        );
    }

    /// An uninstall's two lists are different promises: what it deleted, and
    /// what it deliberately left behind.
    #[tokio::test]
    async fn an_uninstall_separates_what_it_removed_from_what_it_kept() {
        let ws = workspace().await;
        write(&ws.home.join("agents/explore.md"), "mine now\n");

        let result = apply_pack_delete(&ws.orch, "core").unwrap();
        assert_eq!(result.action, "uninstall");
        assert_eq!(result.kept_paths, vec!["agents/explore.md".to_string()]);
        assert!(result
            .removed_paths
            .contains(&"recipes/default.yaml".to_string()));
        assert!(ws.home.join("agents/explore.md").exists());
        assert!(!ws.home.join("recipes/default.yaml").exists());

        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        let core = catalog.find("core").unwrap();
        assert_eq!(core.state, PackState::Available);
        assert!(core.uninstalled_by_user);
        assert!(
            core.installs_by_default,
            "an uninstall outranks the default set"
        );
    }

    /// The event is the whole point of the shared entry: it must fire exactly
    /// once per successful mutation, and never for one that failed — an event
    /// for a workspace that did not move is a refetch that can only confuse.
    #[tokio::test]
    async fn a_successful_mutation_emits_one_pack_change_and_a_failure_emits_none() {
        let ws = workspace().await;

        // `matlab` ships no machine service, so an agent installs, updates,
        // and restores it exactly as the operator would.
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        assert_eq!(pack_events(&ws), 1);
        apply_pack_action(&ws.orch, "update", "matlab").unwrap();
        apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
        apply_pack_action(&ws.orch, "restore", "matlab").unwrap();
        assert_eq!(pack_events(&ws), 4);

        assert!(
            apply_pack_action(&ws.orch, "install", "matlab").is_err(),
            "installing an installed pack is an error"
        );
        assert!(apply_pack_action(&ws.orch, "install", "nonexistent").is_err());
        assert!(apply_pack_delete(&ws.orch, "nonexistent").is_err());
        assert!(
            apply_pack_item_removal(&ws.orch, "core", PackItemKind::Agent, "nonexistent").is_err()
        );
        assert_eq!(pack_events(&ws), 4, "a failed mutation emits nothing");

        apply_pack_delete(&ws.orch, "matlab").unwrap();
        assert_eq!(pack_events(&ws), 5);
    }

    /// A machine service runs on a selected machine as a resident outside any
    /// fence, so putting its declaration in place is the operator's: an agent's
    /// install, restore, update, or reset of it is refused with nothing
    /// written and no event, and the operator's goes through.
    #[tokio::test]
    async fn a_pack_that_ships_a_machine_service_is_the_operators_to_put_in_place() {
        let ws = workspace().await;
        let solo = source_pack(&ws.resources, "solo");
        write(
            &solo.join("cairn-pack.yaml"),
            "id: solo\nname: Solo\nversion: 1.0.0\n",
        );
        write(
            &solo.join("skills/solo/SKILL.md"),
            "---\nname: solo\ndescription: Solo.\n---\n",
        );
        write(&solo.join("services/solo/service.yaml"), PROBE_SERVICE);
        let refused = |error: String| {
            assert!(error.contains(OPERATOR_ONLY_PACK_ACT), "{error}");
            assert!(error.contains("solo/solo"), "{error}");
            assert!(error.contains("outside any fence"), "{error}");
            assert!(
                error.contains("from the Library page in Settings"),
                "{error}"
            );
        };
        let events = pack_events(&ws);

        refused(apply_pack_action(&ws.orch, "install", "solo").unwrap_err());
        assert!(pack::lock::read_lock(&ws.home, "solo").is_none());
        assert!(!ws.home.join("services/solo").exists());
        assert!(!ws.home.join("skills/solo").exists());
        assert_eq!(pack_events(&ws), events, "a refusal emits nothing");

        as_operator(&ws, "install", "solo").unwrap();
        assert!(ws.home.join("services/solo/service.yaml").is_file());

        refused(apply_pack_action(&ws.orch, "update", "solo").unwrap_err());
        as_operator(&ws, "update", "solo").unwrap();

        apply_pack_item_removal(&ws.orch, "solo", PackItemKind::Service, "solo").unwrap();
        let lock = std::fs::read(pack::lock::lock_path(&ws.home, "solo")).unwrap();
        let events = pack_events(&ws);
        refused(apply_pack_action(&ws.orch, "restore", "solo").unwrap_err());
        assert_eq!(
            std::fs::read(pack::lock::lock_path(&ws.home, "solo")).unwrap(),
            lock,
            "a refused restore leaves the removal recorded"
        );
        assert!(!ws.home.join("services/solo").exists());
        assert_eq!(pack_events(&ws), events);
        as_operator(&ws, "restore", "solo").unwrap();
        assert!(ws.home.join("services/solo/service.yaml").is_file());

        let declaration = ws.home.join("services/solo/service.yaml");
        write(&declaration, "name: Mine\n");
        refused(
            reset_pack_item(
                &ws.orch,
                "solo",
                PackItemKind::Service,
                "solo",
                PackAuthority::Agent,
            )
            .unwrap_err(),
        );
        assert_eq!(
            std::fs::read_to_string(&declaration).unwrap(),
            "name: Mine\n"
        );
        reset_pack_item(
            &ws.orch,
            "solo",
            PackItemKind::Service,
            "solo",
            PackAuthority::Operator,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(&declaration).unwrap(),
            PROBE_SERVICE
        );
    }

    /// A restore discards the removal records BEFORE the sync that materializes
    /// the items runs. If that sync fails and the records stay discarded, the
    /// items are absent, the catalog no longer reports them as removed, and the
    /// only trace of a decision the user made is gone — with nothing on screen
    /// saying so. The failure must leave the workspace exactly as it was.
    #[tokio::test]
    async fn a_restore_that_cannot_materialize_leaves_the_removal_recorded() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
        let events_before = pack_events(&ws);

        // Force the materializing sync to fail after the restore has run: every
        // sync asserts the workspace's managed content directories exist, and a
        // plain file cannot be created as a directory.
        std::fs::remove_dir_all(ws.home.join("skills")).unwrap();
        std::fs::write(ws.home.join("skills"), "not a directory\n").unwrap();

        apply_pack_action(&ws.orch, "restore", "matlab")
            .expect_err("the sync cannot materialize into a file");

        assert_eq!(
            pack_events(&ws),
            events_before,
            "a failed restore emits nothing"
        );
        assert!(
            pack::lock::read_lock(&ws.home, "matlab")
                .expect("the pack is still installed")
                .is_removed(PackItemKind::Mcp, "matlab"),
            "a restore that could not materialize must leave the removal recorded, so the user \
             can try again"
        );
    }

    /// The same window, reached the way a real workspace reaches it: an
    /// installed pack whose shipped source is no longer on this machine. The
    /// catalog represents that state, so Restore is offered for it.
    #[tokio::test]
    async fn a_restore_with_no_shipped_source_is_refused_before_it_records_anything() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
        let events_before = pack_events(&ws);

        std::fs::remove_dir_all(source_pack(&ws.resources, "matlab")).unwrap();

        apply_pack_action(&ws.orch, "restore", "matlab")
            .expect_err("nothing ships this pack any more");

        assert_eq!(pack_events(&ws), events_before);
        assert!(pack::lock::read_lock(&ws.home, "matlab")
            .expect("the pack is still installed")
            .is_removed(PackItemKind::Mcp, "matlab"));
    }

    /// The catalog is the one model both surfaces read, so it has to carry
    /// every state a picker distinguishes on.
    fn write_failure_plugin(root: &Path, agent: &str, command: &str) {
        write(
            &root.join("plugin.json"),
            &format!(
                r#"{{"$schema":"{}","name":"rollback-plugin","version":"1.0.0","extensions":{{"dev.cairn":{{"version":1}}}}}}"#,
                pack::agent_plugin::PLUGIN_SCHEMA_URI
            ),
        );
        write(&root.join("dev.cairn/responses/first.md"), agent);
        write(
            &root.join("dev.cairn/workflows/later/workflow.yaml"),
            "name: later\n",
        );
        write(
            &root.join("mcp.json"),
            &format!(
                r#"{{"$schema":"{}","mcpServers":{{"rollback":{{"type":"stdio","command":"{command}"}}}}}}"#,
                pack::agent_plugin::MCP_SCHEMA_URI
            ),
        );
    }

    #[tokio::test]
    async fn failed_local_sync_restores_source_lock_items_and_mcp() {
        let ws = workspace().await;
        let plugin = ws._temp.path().join("plugin");
        write_failure_plugin(&plugin, "first import\n", "old-command");

        std::fs::remove_dir_all(ws.home.join("skills")).unwrap();
        std::fs::write(ws.home.join("skills"), "blocks skill directory\n").unwrap();
        import_agent_plugin(&ws.orch, &plugin).expect_err("later item must fail");
        assert!(!ws.home.join("agents/first.md").exists());
        assert!(pack::lock::read_lock(&ws.home, "rollback-plugin").is_none());
        assert!(!pack::lock::mcp_path(&ws.home, "rollback-plugin").exists());
        assert!(!pack::lock::pack_dir(&ws.home, "rollback-plugin")
            .join("source")
            .exists());

        std::fs::remove_file(ws.home.join("skills")).unwrap();
        std::fs::create_dir_all(ws.home.join("skills")).unwrap();
        import_agent_plugin(&ws.orch, &plugin).unwrap();
        let source = pack::lock::pack_dir(&ws.home, "rollback-plugin").join("source");
        let lock_before =
            std::fs::read(pack::lock::lock_path(&ws.home, "rollback-plugin")).unwrap();
        let mcp_before = std::fs::read(pack::lock::mcp_path(&ws.home, "rollback-plugin")).unwrap();
        let source_before = std::fs::read(source.join("plugin.json")).unwrap();
        let installed_lock = pack::lock::read_lock(&ws.home, "rollback-plugin").unwrap();
        let agent_path = installed_lock
            .items
            .iter()
            .find(|item| item.kind == PackItemKind::Response)
            .and_then(|item| item.path.as_deref())
            .unwrap();
        let agent_before = std::fs::read(ws.home.join(agent_path)).unwrap();
        let skill_path = installed_lock
            .items
            .iter()
            .find(|item| item.kind == PackItemKind::Workflow)
            .and_then(|item| item.path.as_deref())
            .unwrap();
        write_failure_plugin(&plugin, "updated before failure\n", "new-command");
        std::fs::remove_dir_all(ws.home.join(skill_path)).unwrap();
        std::fs::write(ws.home.join(skill_path), "blocks replacement\n").unwrap();
        let skill_before = std::fs::read(ws.home.join(skill_path)).unwrap();
        let error = apply_pack_action(&ws.orch, "update", "rollback-plugin")
            .expect_err("later item must fail");
        assert!(!error.contains("rollback failed"), "{error}");

        assert_eq!(
            std::fs::read(pack::lock::lock_path(&ws.home, "rollback-plugin")).unwrap(),
            lock_before
        );
        assert_eq!(
            std::fs::read(pack::lock::mcp_path(&ws.home, "rollback-plugin")).unwrap(),
            mcp_before
        );
        assert_eq!(
            std::fs::read(source.join("plugin.json")).unwrap(),
            source_before
        );
        assert_eq!(
            std::fs::read(ws.home.join(agent_path)).unwrap(),
            agent_before
        );
        assert_eq!(
            std::fs::read(ws.home.join(skill_path)).unwrap(),
            skill_before
        );
    }

    #[tokio::test]
    async fn importing_a_plugin_with_a_rejected_skill_installs_nothing() {
        let ws = workspace().await;
        let plugin = ws._temp.path().join("plugin");
        write(
            &plugin.join("plugin.json"),
            &format!(
                r#"{{"$schema":"{}","name":"skill-check"}}"#,
                pack::agent_plugin::PLUGIN_SCHEMA_URI
            ),
        );
        write(
            &plugin.join("skills/good/SKILL.md"),
            "---\nname: good\ndescription: Useful.\n---\n",
        );
        write(&plugin.join("skills/bad/SKILL.md"), "---\nname: bad\n---\n");

        let error = import_agent_plugin(&ws.orch, &plugin).expect_err("a rejected skill refuses");
        assert!(error.contains("skill `bad`"), "{error}");
        assert!(error.contains("requires string 'description'"), "{error}");
        assert!(!ws.home.join("skills/bad").exists());
        assert!(!ws.home.join("skills/good").exists());
        assert!(pack::lock::read_lock(&ws.home, "skill-check").is_none());
        assert!(!pack::lock::pack_dir(&ws.home, "skill-check")
            .join("source")
            .exists());

        std::fs::remove_dir_all(plugin.join("skills/bad")).unwrap();
        import_agent_plugin(&ws.orch, &plugin).unwrap();
        let lock = pack::lock::read_lock(&ws.home, "skill-check").unwrap();
        assert!(lock.item(PackItemKind::Skill, "good").is_some());
        assert!(ws.home.join("skills/good/SKILL.md").is_file());
    }

    #[tokio::test]
    async fn the_catalog_reports_installed_available_default_source_and_readiness() {
        let ws = workspace().await;
        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert!(catalog.source_recorded);

        let core = catalog.find("core").unwrap();
        assert_eq!(core.state, PackState::Installed);
        assert!(core.installs_by_default);
        assert!(!core.update_available);
        assert_eq!(core.installed_version.as_deref(), Some("1.0.0"));
        assert_eq!(core.shipped_version.as_deref(), Some("1.0.0"));
        assert_eq!(core.source.kind, "bundled");
        assert_eq!(
            core.counts.iter().map(|c| c.count).sum::<usize>(),
            7,
            "one of every file-backed kind"
        );

        let matlab = catalog.find("matlab").unwrap();
        assert_eq!(matlab.state, PackState::Available);
        assert!(!matlab.installs_by_default);
        assert!(matlab.installed_version.is_none());

        // Installing the connector surfaces the reason it is inert: its command
        // references a `${VAR}` with no value anywhere.
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        let installed = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        let connector = installed
            .find("matlab")
            .unwrap()
            .items
            .iter()
            .find(|item| item.kind == "mcp")
            .expect("the connector is an item");
        assert!(
            matches!(
                connector.not_ready.as_ref(),
                Some(crate::config::mcp_servers::NotReady::MissingVars { vars })
                    if vars == &["MATLAB_BIN".to_string()]
            ),
            "got {:?}",
            connector.not_ready
        );

        // A shipped change is what "update available" means.
        write(
            &source_pack(&ws.resources, "core").join("agents/explore.md"),
            "shipped v2\n",
        );
        assert!(
            pack_catalog(
                &ws.home,
                &ws.orch.library,
                &crate::config::machine_services::MachineServiceSnapshot::empty()
            )
            .find("core")
            .unwrap()
            .update_available
        );
    }

    // ── Library sources ──
    //
    // A real git repository in marketplace layout, its signed index published
    // on `cairn-index`, fetched over `file://` under the test git policy, and
    // verified by the production verifier against a fixture key.

    use crate::config::mcp_servers::NotReady;
    use crate::library::refresh_library_sources;
    use crate::library::testing::LibraryFixture;

    fn stdio_connector(fixture: &LibraryFixture, id: &str, command: &str) {
        fixture.write(
            id,
            "cairn-pack.yaml",
            &format!("id: {id}\nname: {id}\nversion: 1.0.0\n"),
        );
        fixture.write(
            id,
            "mcp.yaml",
            &format!(
                "mcpServers:\n  {id}:\n    command: {command}\n    args: [\"${{FIXTURE_TOKEN_FILE}}\"]\n\
                 setup:\n  FIXTURE_TOKEN_FILE:\n    kind: path\n    label: Token file\n"
            ),
        );
    }

    fn guides_pack(fixture: &LibraryFixture, notes: &str, tips: &str) {
        fixture.write(
            "guides",
            "cairn-pack.yaml",
            "id: guides\nname: Guides\nversion: 1.0.0\n",
        );
        fixture.write(
            "guides",
            "skills/notes/SKILL.md",
            &format!("---\nname: notes\ndescription: Notes\n---\n{notes}\n"),
        );
        fixture.write(
            "guides",
            "skills/tips/SKILL.md",
            &format!("---\nname: tips\ndescription: Tips\n---\n{tips}\n"),
        );
    }

    async fn library_workspace(fixture: &LibraryFixture) -> Workspace {
        workspace_with(Some(fixture.context())).await
    }

    fn mcp_layer(ws: &Workspace, id: &str) -> String {
        std::fs::read_to_string(pack::lock::mcp_path(&ws.home, id)).unwrap()
    }

    fn stage_dirs(ws: &Workspace) -> Vec<String> {
        std::fs::read_dir(ws.home.join("packs"))
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.file_name().to_string_lossy().to_string())
                    .filter(|name| name.starts_with(".library-stage-"))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn a_library_connector_installs_with_its_provenance_and_waits_for_setup() {
        let mut fixture = LibraryFixture::new();
        stdio_connector(&fixture, "acme", "acme-mcp");
        fixture.publish();
        let ws = library_workspace(&fixture).await;

        let before = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert_eq!(before.sources[0].state, "not-fetched");
        assert!(
            before.find("acme").is_none(),
            "catalog reads never fetch, so nothing is offered yet"
        );

        let report = refresh_library_sources(&ws.orch, true);
        assert_eq!(report.sources[0].outcome, "updated", "{report:?}");
        let offered = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        )
        .find("acme")
        .cloned()
        .expect("the library's entry is offered");
        assert_eq!(offered.state, PackState::Available);
        assert_eq!(offered.source.label, "Cairn library");
        assert_eq!(offered.source.signer.as_deref(), Some("cairn-library-test"));
        assert_eq!(
            offered.runs,
            pack::ItemRuns::Here,
            "a stdio server runs here"
        );

        let result = apply_pack_action(&ws.orch, "install", "acme").unwrap();
        assert_eq!(
            result.summary,
            "Installed pack 'acme' from the Cairn library"
        );
        let lock = pack::lock::read_lock(&ws.home, "acme").unwrap();
        assert_eq!(lock.source.kind, pack::PackSourceKind::Url);
        assert_eq!(
            lock.source.url.as_deref(),
            Some(fixture.source().url.as_str())
        );
        assert!(lock
            .source
            .sha
            .as_deref()
            .is_some_and(pack::library::is_full_commit));
        assert_eq!(lock.source.signer.as_deref(), Some("cairn-library-test"));
        assert!(stage_dirs(&ws).is_empty(), "the stage is dropped");

        // The same readiness gate as a bundled pack server.
        let entry = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        )
        .find("acme")
        .cloned()
        .unwrap();
        let server = entry.items.iter().find(|item| item.kind == "mcp").unwrap();
        assert!(
            matches!(&server.not_ready, Some(NotReady::MissingVars { vars }) if vars == &["FIXTURE_TOKEN_FILE".to_string()]),
            "{:?}",
            server.not_ready
        );
        assert_eq!(server.setup[0].setup.label.as_deref(), Some("Token file"));
        assert!(
            !crate::config::mcp_servers::resolve_mcp_servers(
                &ws.home,
                None,
                &crate::config::machine_services::MachineServiceSnapshot::empty()
            )
            .contains_key("acme"),
            "an ingested server with an unset variable is withheld from agents"
        );
    }

    #[tokio::test]
    async fn an_update_that_changes_what_runs_waits_for_the_operator() {
        let mut fixture = LibraryFixture::new();
        stdio_connector(&fixture, "acme", "acme-mcp");
        fixture.publish();
        let ws = library_workspace(&fixture).await;
        refresh_library_sources(&ws.orch, true);
        apply_pack_action(&ws.orch, "install", "acme").unwrap();
        let layer = mcp_layer(&ws, "acme");

        stdio_connector(&fixture, "acme", "acme-mcp-v2");
        fixture.publish();
        let report = refresh_library_sources(&ws.orch, true);
        assert_eq!(report.sources[0].awaiting_review, vec!["acme".to_string()]);
        assert!(report.sources[0].applied.is_empty());
        assert_eq!(
            mcp_layer(&ws, "acme"),
            layer,
            "a refresh never applies a change to what runs"
        );

        let entry = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        )
        .find("acme")
        .cloned()
        .unwrap();
        assert!(entry.update_available && entry.update_needs_review);
        let change = &entry.runtime_changes[0];
        assert_eq!(
            (change.kind.as_str(), change.change.as_str()),
            ("mcp", "changed")
        );
        assert_eq!(
            change.before.as_ref().and_then(|d| d.command.as_deref()),
            Some("acme-mcp")
        );
        assert_eq!(
            change.after.as_ref().and_then(|d| d.command.as_deref()),
            Some("acme-mcp-v2")
        );

        let error = apply_pack_action(&ws.orch, "update", "acme").unwrap_err();
        assert!(error.contains(OPERATOR_ONLY_PACK_ACT), "{error}");
        assert!(
            error.contains("from the Library page in Settings"),
            "{error}"
        );
        assert_eq!(
            mcp_layer(&ws, "acme"),
            layer,
            "a refused update writes nothing"
        );

        apply_pack_action_as(&ws.orch, "update", "acme", PackAuthority::Operator).unwrap();
        assert!(mcp_layer(&ws, "acme").contains("acme-mcp-v2"));
        let after = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        let entry = after.find("acme").unwrap();
        assert!(!entry.update_available && !entry.update_needs_review);
    }

    #[tokio::test]
    async fn a_content_update_applies_on_refresh_and_leaves_your_copy_alone() {
        let mut fixture = LibraryFixture::new();
        guides_pack(&fixture, "v1", "v1");
        fixture.publish();
        let ws = library_workspace(&fixture).await;
        refresh_library_sources(&ws.orch, true);
        apply_pack_action(&ws.orch, "install", "guides").unwrap();
        write(&ws.home.join("skills/tips/SKILL.md"), "mine\n");

        guides_pack(&fixture, "v2", "v2");
        fixture.publish();
        let report = refresh_library_sources(&ws.orch, true);
        assert_eq!(
            report.sources[0].applied,
            vec!["guides".to_string()],
            "{report:?}"
        );
        assert!(
            std::fs::read_to_string(ws.home.join("skills/notes/SKILL.md"))
                .unwrap()
                .contains("v2")
        );
        assert_eq!(
            std::fs::read_to_string(ws.home.join("skills/tips/SKILL.md")).unwrap(),
            "mine\n",
            "latest is always latest, except for your copy"
        );
        assert_eq!(
            item_state(&ws, "guides", "skill", "tips"),
            Some(PackItemState::EditedByUser)
        );
        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        let tips = catalog
            .find("guides")
            .unwrap()
            .items
            .iter()
            .find(|item| item.id == "tips")
            .unwrap();
        assert!(tips.behind_shipped, "your copy, behind shipped");

        // Remove and restore come back from the snapshot, without a fetch.
        apply_pack_item_removal(&ws.orch, "guides", PackItemKind::Skill, "notes").unwrap();
        assert!(!ws.home.join("skills/notes").exists());
        let restored = apply_pack_action(&ws.orch, "restore", "guides").unwrap();
        assert_eq!(restored.restored_items.len(), 1);
        assert!(
            std::fs::read_to_string(ws.home.join("skills/notes/SKILL.md"))
                .unwrap()
                .contains("v2")
        );
    }

    #[tokio::test]
    async fn a_bad_signature_keeps_the_last_verified_index_and_a_tampered_cache_is_dropped() {
        let mut fixture = LibraryFixture::new();
        stdio_connector(&fixture, "acme", "acme-mcp");
        let bytes = fixture.publish();
        let ws = library_workspace(&fixture).await;
        refresh_library_sources(&ws.orch, true);

        fixture.publish_tampered(&bytes.replace("acme-mcp", "evil-mcp"));
        let report = refresh_library_sources(&ws.orch, true);
        assert_eq!(report.sources[0].outcome, "failed");
        assert_eq!(
            report.sources[0].error.as_deref(),
            Some("index signature does not verify against the Cairn library's trusted keys")
        );
        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert_eq!(
            catalog.sources[0].state, "verified",
            "the previous index stays"
        );
        assert!(catalog.sources[0]
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("does not verify")));
        assert!(catalog.find("acme").is_some());
        assert!(
            !ws.home.join("packs/acme").exists(),
            "nothing was installed"
        );

        // The cache lives where agents can write; a cache that no longer
        // verifies is not believed.
        let cached = fixture.cache_dir(&ws.home).join("index.json");
        let text = std::fs::read_to_string(&cached).unwrap();
        std::fs::write(&cached, text.replace("acme-mcp", "evil-mcp")).unwrap();
        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert_eq!(catalog.sources[0].state, "unverified");
        assert!(catalog.sources[0]
            .refusal
            .as_deref()
            .is_some_and(|r| r.contains("does not verify")));
        assert!(catalog.find("acme").is_none(), "its entries are dropped");
        let page = crate::resources::packs::read_packs(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
            None,
        );
        assert!(page.contains("the cached index is not trusted"), "{page}");
    }

    #[tokio::test]
    async fn content_the_signed_index_does_not_vouch_for_is_refused_and_nothing_is_written() {
        let mut fixture = LibraryFixture::new();
        guides_pack(&fixture, "v1", "v1");
        let bytes = fixture.publish();
        let mut index: serde_json::Value = serde_json::from_str(&bytes).unwrap();
        let forged = pack::sha256_bytes(b"forged");
        for item in index["entries"][0]["items"].as_array_mut().unwrap() {
            if item["id"] == "notes" {
                item["hash"] = serde_json::json!(forged);
            }
        }
        fixture.publish_signed(&serde_json::to_string_pretty(&index).unwrap());
        let ws = library_workspace(&fixture).await;
        refresh_library_sources(&ws.orch, true);

        let error = apply_pack_action(&ws.orch, "install", "guides").unwrap_err();
        assert!(error.starts_with("skill `notes` from "), "{error}");
        assert!(
            error.contains(&format!("but the signed index says {forged}")),
            "{error}"
        );
        assert!(pack::lock::read_lock(&ws.home, "guides").is_none());
        assert!(!ws.home.join("skills/notes").exists());
        assert!(!ws.home.join("packs/guides").exists());
        assert!(
            stage_dirs(&ws).is_empty(),
            "the stage is dropped on failure"
        );
    }

    #[tokio::test]
    async fn installing_over_your_own_skill_is_refused_naming_both_sides() {
        let mut fixture = LibraryFixture::new();
        guides_pack(&fixture, "v1", "v1");
        fixture.publish();
        let ws = library_workspace(&fixture).await;
        refresh_library_sources(&ws.orch, true);
        write(
            &ws.home.join("skills/notes/SKILL.md"),
            "---\nname: notes\n---\nmine\n",
        );
        let error = apply_pack_action(&ws.orch, "install", "guides").unwrap_err();
        assert!(
            error.starts_with(
                "you already have your own skill `notes` at `skills/notes`, which no pack installed"
            ),
            "{error}"
        );
        assert!(error.contains(&fixture.source().url), "{error}");
        assert!(pack::lock::read_lock(&ws.home, "guides").is_none());
        assert!(stage_dirs(&ws).is_empty());
    }

    #[tokio::test]
    async fn a_bundled_connector_that_moved_to_the_library_keeps_working_under_its_new_provenance()
    {
        let mut fixture = LibraryFixture::new();
        // Byte-identical to the bundled pack this workspace installs.
        fixture.write(
            "matlab",
            "cairn-pack.yaml",
            "id: matlab\nname: MATLAB\nversion: 1.0.0\ndefault: false\n",
        );
        fixture.write(
            "matlab",
            "mcp.yaml",
            "mcpServers:\n  matlab:\n    type: stdio\n    command: ${MATLAB_BIN}\n",
        );
        fixture.publish();
        let ws = library_workspace(&fixture).await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        let layer = mcp_layer(&ws, "matlab");
        let before = pack::lock::read_lock(&ws.home, "matlab").unwrap();
        assert_eq!(before.source.kind, pack::PackSourceKind::Bundled);

        // While this build still ships it, the bundled pack wins.
        refresh_library_sources(&ws.orch, true);
        assert_eq!(
            pack::lock::read_lock(&ws.home, "matlab")
                .unwrap()
                .source
                .kind,
            pack::PackSourceKind::Bundled
        );
        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert!(catalog.sources[0].refused_entries.iter().any(
            |refusal| refusal.id == "matlab" && refusal.reason.contains("ships with this app")
        ));

        // The next build no longer ships it.
        std::fs::remove_dir_all(source_pack(&ws.resources, "matlab")).unwrap();
        let report = refresh_library_sources(&ws.orch, true);
        assert_eq!(report.sources[0].migrated, vec!["matlab".to_string()]);
        let after = pack::lock::read_lock(&ws.home, "matlab").unwrap();
        assert_eq!(after.source.kind, pack::PackSourceKind::Url);
        assert!(after.source.signer.is_some() && after.source.sha.is_some());
        assert_eq!(
            after.items, before.items,
            "items and baselines are untouched"
        );
        assert_eq!(after.content_hash, before.content_hash);
        assert_eq!(mcp_layer(&ws, "matlab"), layer, "so readiness is unchanged");
        let entry = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        )
        .find("matlab")
        .cloned()
        .unwrap();
        assert_eq!(entry.source.label, "Cairn library");
        assert!(
            !entry.update_available,
            "a byte-identical move is not an update"
        );
        assert!(entry
            .items
            .iter()
            .any(|item| matches!(item.not_ready, Some(NotReady::MissingVars { .. }))));
    }

    #[tokio::test]
    async fn an_unreachable_library_leaves_the_bundled_core_and_says_why() {
        let fixture = LibraryFixture::new();
        let ws = library_workspace(&fixture).await;
        let report = refresh_library_sources(&ws.orch, true);
        assert_eq!(report.sources[0].outcome, "failed");
        assert!(report.sources[0]
            .error
            .as_deref()
            .is_some_and(|e| e.contains("has no `cairn-index` branch")));
        let catalog = pack_catalog(
            &ws.home,
            &ws.orch.library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert_eq!(catalog.sources[0].state, "unreachable");
        let ids: Vec<&str> = catalog
            .packs
            .iter()
            .map(|entry| entry.id.as_str())
            .collect();
        assert_eq!(ids, vec!["core", "matlab"], "only what the app ships");
        // Within the TTL a refresh is not attempted again unless forced; a
        // failure is never cached as fresh.
        assert_eq!(
            refresh_library_sources(&ws.orch, false).sources[0].outcome,
            "failed"
        );
    }
}
