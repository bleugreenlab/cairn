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
    if let Err(error) = std::fs::rename(stage.keep(), destination) {
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
                paths.insert(config_dir.join(path));
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
    for dir in ["agents", "recipes", "responses", "workflows"] {
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
    if pack::lock::read_lock(&orch.config_dir, &id).is_some()
        || source_dir(orch)
            .ok()
            .and_then(|root| pack::available_pack(&root, &id))
            .is_some()
    {
        return Err(format!("Pack id `{id}` collides with an existing pack"));
    }
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
        pack::PackSource::local(canonical.to_string_lossy().into_owned()),
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
/// shadow in favor of the pack definition.
pub fn reset_pack_item(
    orch: &Orchestrator,
    pack_id: &str,
    kind: PackItemKind,
    item_id: &str,
) -> Result<PackMutationResult, String> {
    let resource_dir = source_dir(orch)?;
    let manifest = pack::available_pack(&resource_dir, pack_id)
        .ok_or_else(|| format!("No pack `{pack_id}` is shipped in {resource_dir:?}"))?;
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

    let mut lock = pack::lock::read_lock(&orch.config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    let item = lock.item(kind, item_id).cloned().ok_or_else(|| {
        format!(
            "Pack `{pack_id}` does not carry {} `{item_id}`",
            kind.as_str()
        )
    })?;
    if !item.forked {
        return Err(format!(
            "{} `{item_id}` is already using pack `{pack_id}`'s version",
            kind.as_str()
        ));
    }

    let baseline = match kind {
        PackItemKind::Mcp => {
            let definitions =
                pack::mcp::parse_pack_mcp_file(&pack::lock::mcp_path(&orch.config_dir, pack_id))?;
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
            match pack::hash_item_path(kind, &orch.config_dir, &orch.config_dir.join(path))? {
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

    let previous_lock = lock.clone();
    lock.reset_item_baseline(kind, item_id, baseline);
    pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &lock)?;
    if kind == PackItemKind::Mcp {
        if let Err(error) =
            crate::config::mcp_servers::delete_workspace_mcp_server(&orch.config_dir, item_id)
        {
            let _ = pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &previous_lock);
            return Err(error);
        }
    }

    let result = sync_one_pack(
        &RealGitClient,
        &RealFileSystem,
        &resource_dir,
        &orch.config_dir,
        pack_id,
    )
    .inspect_err(|_| {
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

/// The app resource directory this workspace syncs from, recorded by the last
/// startup sync. Without it there is nothing to install FROM.
fn source_dir(orch: &Orchestrator) -> Result<std::path::PathBuf, String> {
    pack::source_dir(&orch.config_dir).ok_or_else(|| {
        "This workspace has no recorded app resource directory yet, so no pack can be installed \
         from it. It is recorded on the next app startup."
            .to_string()
    })
}

/// Install, update, or restore one pack.
///
/// Emits the registry change only after the filesystem, lock, and Git work has
/// succeeded, so a failed action never invalidates a caller's view of a
/// workspace that did not move.
pub fn apply_pack_action(
    orch: &Orchestrator,
    action: &str,
    pack_id: &str,
) -> Result<PackMutationResult, String> {
    if action == "update" {
        if let Some(lock) = pack::lock::read_lock(&orch.config_dir, pack_id) {
            if lock.source.kind == pack::PackSourceKind::Local {
                return update_local_plugin(orch, &lock);
            }
        }
    }
    // Resolve what this action will sync FROM before touching any lock. A
    // restore records its intent by rewriting the pack lock, so a source that
    // could never have been synced from must be refused while that record is
    // still intact — otherwise the reachable "installed pack whose shipped
    // source is gone" case clears the user's removal markers and then fails.
    let resource_dir = source_dir(orch)?;
    if pack::available_pack(&resource_dir, pack_id).is_none() {
        return Err(format!(
            "No pack `{pack_id}` is shipped in {resource_dir:?}"
        ));
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
    let result = uninstall_pack(&RealGitClient, &RealFileSystem, &orch.config_dir, pack_id)?;
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
        PackItemKind::Workflow => {
            crate::config::workflows::delete_workflow(config_dir, item_id, None)?
        }
        PackItemKind::Mcp => {
            crate::config::mcp_servers::delete_workspace_mcp_server(config_dir, item_id)?
        }
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
/// the same function the settings command runs.
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
        reset_pack_item(orch, pack_id, kind, item_id)
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
        write(
            &core.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\n",
        );
        write(&core.join("workflows/flow/workflow.yaml"), "name: Flow\n");

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
        let orch = Orchestrator::builder(db_state, services, home.clone()).build();
        Workspace {
            orch,
            emitter,
            home,
            resources,
            _temp: temp,
        }
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
        pack_catalog(&ws.home)
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
    #[tokio::test]
    async fn every_item_kind_can_be_removed_and_restored_without_uninstalling_its_pack() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();

        let cases: [(&str, PackItemKind, &str, &str); 6] = [
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
        for pack_id in ["core", "matlab"] {
            let restored = apply_pack_action(&ws.orch, "restore", pack_id).unwrap();
            assert!(!restored.restored_items.is_empty());
        }
        for path in [
            "agents/explore.md",
            "recipes/default.yaml",
            "responses/conveyor.md",
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
        let result = apply_pack_action(&ws.orch, "update", "core").unwrap();

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

        let catalog = pack_catalog(&ws.home);
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

        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        assert_eq!(pack_events(&ws), 1);

        assert!(
            apply_pack_action(&ws.orch, "install", "matlab").is_err(),
            "installing an installed pack is an error"
        );
        assert!(apply_pack_action(&ws.orch, "install", "nonexistent").is_err());
        assert!(apply_pack_delete(&ws.orch, "nonexistent").is_err());
        assert!(
            apply_pack_item_removal(&ws.orch, "core", PackItemKind::Agent, "nonexistent").is_err()
        );
        assert_eq!(pack_events(&ws), 1, "a failed mutation emits nothing");

        apply_pack_delete(&ws.orch, "matlab").unwrap();
        assert_eq!(pack_events(&ws), 2);
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
    async fn the_catalog_reports_installed_available_default_source_and_readiness() {
        let ws = workspace().await;
        let catalog = pack_catalog(&ws.home);
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
            5,
            "one of every file-backed kind"
        );

        let matlab = catalog.find("matlab").unwrap();
        assert_eq!(matlab.state, PackState::Available);
        assert!(!matlab.installs_by_default);
        assert!(matlab.installed_version.is_none());

        // Installing the connector surfaces the reason it is inert: its command
        // references a `${VAR}` with no value anywhere.
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        let installed = pack_catalog(&ws.home);
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
            pack_catalog(&ws.home)
                .find("core")
                .unwrap()
                .update_available
        );
    }
}
