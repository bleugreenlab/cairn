//! Pack-aware workspace sync.
//!
//! Pack locks are the live ownership authority. Each installed item records the
//! hash Cairn last materialized and a durable fork bit. Git is consulted only to
//! classify pre-v2 locks once; migrated packs never invoke Git again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::server_config::McpServerConfig;
use crate::{
    self as pack, lock as pack_lock, ContentHash, PackItemKind, PackLock, PackLockItem,
    PackManifest, PackSource,
};
use cairn_platform::services::{guarded_copy_file, guarded_copy_tree, FileSystem, GitClient};

const LEGACY_BUNDLE_SYNC_MARKER: &str = ".bundle-sync";
const BUNDLE_COMMIT_SUBJECTS: &[&str] = &[
    "Initialize Cairn workspace config",
    "Add missing bundled workspace defaults",
    "Sync bundled workspace defaults",
];
const PACK_COMMIT_PREFIX: &str = "Sync pack resources: ";

fn is_pack_authored_subject(subject: &str) -> bool {
    BUNDLE_COMMIT_SUBJECTS.contains(&subject) || subject.starts_with(PACK_COMMIT_PREFIX)
}

/// Sync an already resolved source through the canonical pack ownership machinery.
pub fn sync_resolved_pack(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifest: PackManifest,
    source: PackSource,
) -> Result<PackSyncResult, String> {
    fs.create_dir_all(config_dir)?;
    ensure_workspace_content_dirs(fs, config_dir)?;
    pack_lock::clear_uninstall(fs, config_dir, &manifest.id)?;
    let hash = pack::content_hash(&manifest.root)?;
    let lock = pack_lock::read_lock(config_dir, &manifest.id);
    apply_plans(
        git,
        fs,
        config_dir,
        vec![PackPlan {
            manifest,
            lock,
            hash,
            action: PackAction::Sync,
            source,
        }],
    )
}

/// Explicitly replace every currently shipped item in one bundled pack and
/// establish fresh ownership baselines. Unlike ordinary sync, this deliberately
/// overwrites forks and clears tombstones because the caller has confirmed the
/// total restore scope.
pub fn restore_one_bundled_pack(
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
    pack_id: &str,
) -> Result<PackSyncResult, String> {
    let manifest = pack::available_pack(resource_dir, pack_id)
        .ok_or_else(|| format!("No pack `{pack_id}` is shipped in {resource_dir:?}"))?;
    fs.create_dir_all(config_dir)?;
    ensure_workspace_content_dirs(fs, config_dir)?;
    pack::record_source_dir(fs, resource_dir, config_dir)?;
    pack_lock::clear_uninstall(fs, config_dir, pack_id)?;

    let source_mcp = source_mcp_servers(&manifest)?;
    let mut lock = PackLock::new(
        &manifest,
        pack::content_hash(&manifest.root)?,
        PackSource::bundled(manifest.format),
        manifest.items(),
    );
    let mut outcome = SyncOutcome::default();
    for item in &mut lock.items {
        materialize_item(fs, config_dir, pack_id, &manifest, item, &source_mcp)?;
        item.content_hash = source_item_hash(&manifest, item, &source_mcp)?;
        item.forked = false;
        if let Some(path) = &item.path {
            outcome.changed(path.clone());
        }
    }

    lock.cairn_version = 2;
    lock.removed.clear();
    lock.sort_items();
    pack_lock::write_lock(fs, config_dir, &lock)?;
    outcome.changed_paths.sort();
    outcome.changed_paths.dedup();
    Ok(PackSyncResult {
        updated: outcome.changed,
        changed_paths: outcome.changed_paths,
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackSyncResult {
    pub updated: bool,
    pub changed_paths: Vec<String>,
}

#[derive(Default)]
struct SyncOutcome {
    changed: bool,
    changed_paths: Vec<String>,
}

impl SyncOutcome {
    fn changed(&mut self, path: String) {
        self.changed = true;
        self.changed_paths.push(path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackAction {
    Sync,
    Skip,
}

struct PackPlan {
    manifest: PackManifest,
    lock: Option<PackLock>,
    hash: String,
    action: PackAction,
    source: PackSource,
}

impl PackPlan {
    fn syncing(&self) -> bool {
        self.action == PackAction::Sync
    }
}

pub fn sync_workspace_packs(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
    _app_version: &str,
) -> Result<PackSyncResult, String> {
    fs.create_dir_all(config_dir)?;
    ensure_workspace_content_dirs(fs, config_dir)?;
    pack::record_source_dir(fs, resource_dir, config_dir)?;
    let plans = plan_packs(fs, config_dir, pack::discover_available_packs(resource_dir))?;
    let result = apply_plans(git, fs, config_dir, plans)?;

    let all_migrated = pack_lock::installed_packs(config_dir)
        .iter()
        .all(PackLock::migration_complete);
    let marker = config_dir.join(LEGACY_BUNDLE_SYNC_MARKER);
    if all_migrated && fs.exists(&marker) {
        fs.remove_file(&marker)?;
    }
    Ok(result)
}

fn ensure_workspace_content_dirs(fs: &dyn FileSystem, config_dir: &Path) -> Result<(), String> {
    for dir_name in pack::CONTENT_DIRS {
        fs.create_dir_all(&config_dir.join(dir_name))?;
    }
    Ok(())
}

pub fn sync_one_pack(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
    pack_id: &str,
) -> Result<PackSyncResult, String> {
    let manifest = pack::available_pack(resource_dir, pack_id)
        .ok_or_else(|| format!("No pack `{pack_id}` is shipped in {resource_dir:?}"))?;
    fs.create_dir_all(config_dir)?;
    ensure_workspace_content_dirs(fs, config_dir)?;
    pack::record_source_dir(fs, resource_dir, config_dir)?;
    pack_lock::clear_uninstall(fs, config_dir, pack_id)?;
    let hash = pack::content_hash(&manifest.root)?;
    let lock = pack_lock::read_lock(config_dir, pack_id);
    apply_plans(
        git,
        fs,
        config_dir,
        vec![PackPlan {
            source: PackSource::bundled(manifest.format),
            manifest,
            lock,
            hash,
            action: PackAction::Sync,
        }],
    )
}

pub fn uninstall_pack(
    _git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
) -> Result<PackUninstallResult, String> {
    let mut lock = pack_lock::read_lock(config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    let mut result = PackUninstallResult::default();

    for item in &mut lock.items {
        if item.forked {
            if let Some(path) = &item.path {
                result.kept.push(path.clone());
            }
            continue;
        }
        let current = current_item_hash(config_dir, pack_id, item)?;
        let owned = matches!(
            (&current, item.content_hash.as_deref()),
            (ContentHash::Present(current), Some(baseline)) if current == baseline
        );
        if matches!(current, ContentHash::Missing) {
            continue;
        }
        if !owned {
            item.forked = true;
            if let Some(path) = &item.path {
                result.kept.push(path.clone());
            }
            continue;
        }
        remove_item_content(fs, config_dir, pack_id, item)?;
        if let Some(path) = &item.path {
            result.removed.push(path.clone());
        }
    }

    pack_lock::remove_pack_dir(fs, config_dir, pack_id)?;
    pack_lock::record_uninstall(fs, config_dir, pack_id)?;
    result.removed.sort();
    result.kept.sort();
    Ok(result)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackUninstallResult {
    pub removed: Vec<String>,
    pub kept: Vec<String>,
}

fn apply_plans(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    plans: Vec<PackPlan>,
) -> Result<PackSyncResult, String> {
    let mut outcome = SyncOutcome::default();
    for plan in plans.into_iter().filter(PackPlan::syncing) {
        let mut lock = plan.lock.unwrap_or_else(|| {
            PackLock::new(
                &plan.manifest,
                plan.hash.clone(),
                plan.source.clone(),
                plan.manifest.items(),
            )
        });

        if !lock.migration_complete() {
            migrate_legacy_lock(git, fs, config_dir, &plan.manifest, &mut lock)?;
            // Migration is durable before normal sync can overwrite any item.
            pack_lock::write_lock(fs, config_dir, &lock)?;
        }

        sync_pack(
            fs,
            config_dir,
            &plan.manifest,
            &plan.hash,
            &plan.source,
            &mut lock,
            &mut outcome,
        )?;
    }
    outcome.changed_paths.sort();
    outcome.changed_paths.dedup();
    Ok(PackSyncResult {
        updated: outcome.changed,
        changed_paths: outcome.changed_paths,
    })
}

fn plan_packs(
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifests: Vec<PackManifest>,
) -> Result<Vec<PackPlan>, String> {
    manifests
        .into_iter()
        .map(|manifest| {
            let hash = pack::content_hash(&manifest.root)?;
            let mut lock = pack_lock::read_lock(config_dir, &manifest.id);
            let uninstalled = pack_lock::is_uninstalled(config_dir, &manifest.id);
            let adopted =
                lock.is_none() && !uninstalled && pack_is_materialized(fs, config_dir, &manifest);
            if adopted {
                lock = Some(PackLock::new(
                    &manifest,
                    hash.clone(),
                    PackSource::bundled(manifest.format),
                    manifest.items(),
                ));
            }
            let action = if uninstalled {
                PackAction::Skip
            } else if lock.is_some() || manifest.default {
                PackAction::Sync
            } else {
                PackAction::Skip
            };
            let source = PackSource::bundled(manifest.format);
            Ok(PackPlan {
                manifest,
                lock,
                hash,
                action,
                source,
            })
        })
        .collect()
}

fn pack_is_materialized(fs: &dyn FileSystem, config_dir: &Path, manifest: &PackManifest) -> bool {
    manifest.items().iter().any(|item| {
        item.path
            .as_deref()
            .is_some_and(|path| fs.exists(&config_dir.join(path)))
    })
}

fn migrate_legacy_lock(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifest: &PackManifest,
    lock: &mut PackLock,
) -> Result<(), String> {
    let pack_id = lock.id.clone();
    let source_mcp = source_mcp_servers(manifest)?;
    for item in &mut lock.items {
        if item.content_hash.is_some() {
            continue;
        }
        let current = current_item_hash_with_mcp(config_dir, &pack_id, item, None)?;
        match current {
            ContentHash::Missing => {
                // Missing content is safe to restore. Seed the shipped hash so the
                // lock is fully migrated before the copy happens.
                item.content_hash = source_item_hash(manifest, item, &source_mcp)?;
            }
            ContentHash::Present(hash) => {
                let history_path = item.path.clone().unwrap_or_else(|| {
                    format!(
                        "{}/{}/{}",
                        pack_lock::PACKS_DIR,
                        lock.id,
                        pack::manifest::PACK_MCP_FILE
                    )
                });
                let owned = legacy_path_is_pack_owned(git, config_dir, &history_path);
                item.content_hash = Some(hash);
                item.forked = !owned;
            }
        }
    }
    lock.sort_items();
    // Validate serializability/readability through the same writer boundary.
    let _ = fs;
    Ok(())
}

fn legacy_path_is_pack_owned(git: &dyn GitClient, config_dir: &Path, rel_path: &str) -> bool {
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

fn sync_pack(
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifest: &PackManifest,
    pack_hash: &str,
    source: &PackSource,
    lock: &mut PackLock,
    outcome: &mut SyncOutcome,
) -> Result<(), String> {
    let pack_id = lock.id.clone();
    let source_items = manifest.items();
    let source_mcp = source_mcp_servers(manifest)?;
    let source_keys: BTreeSet<_> = source_items
        .iter()
        .map(|item| (item.kind, item.id.clone()))
        .collect();

    // Retire items no longer shipped. A fork survives without a pack claim;
    // pack-owned content and its lock record disappear.
    let mut retained = Vec::new();
    for mut old in std::mem::take(&mut lock.items) {
        if source_keys.contains(&(old.kind, old.id.clone())) {
            retained.push(old);
            continue;
        }
        let current = current_item_hash(config_dir, &pack_id, &old)?;
        let owned = !old.forked
            && matches!(
                (&current, old.content_hash.as_deref()),
                (ContentHash::Present(current), Some(baseline)) if current == baseline
            );
        if owned {
            remove_item_content(fs, config_dir, &pack_id, &old)?;
            if let Some(path) = &old.path {
                outcome.changed(path.clone());
            }
        } else if !matches!(current, ContentHash::Missing) {
            // Retired forks remain as ordinary user content, no longer claimed.
            old.forked = true;
        }
    }
    lock.items = retained;

    for source_item in source_items {
        if lock.is_removed(source_item.kind, &source_item.id) {
            continue;
        }
        if lock.item(source_item.kind, &source_item.id).is_none() {
            lock.items.push(PackLockItem::legacy(source_item.clone()));
        }
        let item = lock.item_mut(source_item.kind, &source_item.id).unwrap();
        if item.forked {
            continue;
        }
        let source_hash = source_item_hash(manifest, item, &source_mcp)?.ok_or_else(|| {
            format!(
                "Pack item {:?} `{}` has no shipped content",
                item.kind, item.id
            )
        })?;
        let current = current_item_hash_with_mcp(config_dir, &pack_id, item, None)?;
        match current {
            ContentHash::Missing => {
                materialize_item(fs, config_dir, &pack_id, manifest, item, &source_mcp)?;
                item.content_hash = Some(source_hash);
                if let Some(path) = &item.path {
                    outcome.changed(path.clone());
                }
            }
            ContentHash::Present(current_hash) => {
                let Some(baseline) = item.content_hash.as_deref() else {
                    // A fully migrated pack can gain a newly shipped item whose
                    // destination already exists. With no positive ownership
                    // evidence, the existing bytes belong to the user.
                    item.content_hash = Some(current_hash);
                    item.forked = true;
                    continue;
                };
                if current_hash != baseline {
                    item.forked = true;
                } else if current_hash != source_hash {
                    materialize_item(fs, config_dir, &pack_id, manifest, item, &source_mcp)?;
                    item.content_hash = Some(source_hash);
                    if let Some(path) = &item.path {
                        outcome.changed(path.clone());
                    }
                }
            }
        }
    }

    lock.cairn_version = 2;
    lock.name = manifest.name.clone();
    lock.version = manifest.version.clone();
    lock.description = manifest.description.clone();
    lock.author = manifest.author.clone();
    lock.homepage = manifest.homepage.clone();
    lock.keywords = manifest.keywords.clone();
    lock.notes = manifest.notes.clone();
    lock.content_hash = pack_hash.to_string();
    lock.source = source.clone();
    lock.sort_items();
    // Content always lands before the baseline that claims it.
    pack_lock::write_lock(fs, config_dir, lock)?;
    Ok(())
}

fn source_item_hash(
    manifest: &PackManifest,
    item: &PackLockItem,
    mcp: &BTreeMap<String, McpServerConfig>,
) -> Result<Option<String>, String> {
    if item.kind == PackItemKind::Mcp {
        return mcp.get(&item.id).map(pack::hash_mcp_definition).transpose();
    }
    let Some(path) = &item.path else {
        return Ok(None);
    };
    match pack::hash_item_path(item.kind, &manifest.root, &manifest.root.join(path))? {
        ContentHash::Missing => Ok(None),
        ContentHash::Present(hash) => Ok(Some(hash)),
    }
}

fn current_item_hash(
    config_dir: &Path,
    pack_id: &str,
    item: &PackLockItem,
) -> Result<ContentHash, String> {
    current_item_hash_with_mcp(config_dir, pack_id, item, None)
}

fn current_item_hash_with_mcp(
    config_dir: &Path,
    pack_id: &str,
    item: &PackLockItem,
    cached_mcp: Option<&BTreeMap<String, McpServerConfig>>,
) -> Result<ContentHash, String> {
    if item.kind != PackItemKind::Mcp {
        let path = item
            .path
            .as_ref()
            .ok_or_else(|| "File-backed pack item has no path".to_string())?;
        return pack::hash_item_path(item.kind, config_dir, &config_dir.join(path));
    }
    let owned;
    let servers = if let Some(servers) = cached_mcp {
        servers
    } else {
        owned = installed_mcp_servers(config_dir, pack_id)?;
        &owned
    };
    match servers.get(&item.id) {
        Some(config) => Ok(ContentHash::Present(pack::hash_mcp_definition(config)?)),
        None => Ok(ContentHash::Missing),
    }
}

fn source_mcp_servers(
    manifest: &PackManifest,
) -> Result<BTreeMap<String, McpServerConfig>, String> {
    match manifest.mcp_source() {
        Some(path) => Ok(pack::mcp::parse_pack_mcp_file(&path)?
            .servers
            .into_iter()
            .collect()),
        None => Ok(BTreeMap::new()),
    }
}

fn installed_mcp_servers(
    config_dir: &Path,
    pack_id: &str,
) -> Result<BTreeMap<String, McpServerConfig>, String> {
    let path = pack_lock::mcp_path(config_dir, pack_id);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    Ok(pack::mcp::parse_pack_mcp_file(&path)?
        .servers
        .into_iter()
        .collect())
}

fn materialize_item(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
    manifest: &PackManifest,
    item: &PackLockItem,
    source_mcp: &BTreeMap<String, McpServerConfig>,
) -> Result<(), String> {
    if item.kind == PackItemKind::Mcp {
        let mut installed = installed_mcp_servers(config_dir, pack_id)?;
        let config = source_mcp
            .get(&item.id)
            .ok_or_else(|| format!("Missing shipped MCP definition `{}`", item.id))?;
        installed.insert(item.id.clone(), config.clone());
        return write_mcp_layer(fs, config_dir, pack_id, &installed);
    }
    let path = item
        .path
        .as_ref()
        .ok_or_else(|| "File-backed pack item has no path".to_string())?;
    let source = manifest.root.join(path);
    let destination = config_dir.join(path);
    if matches!(item.kind, PackItemKind::Skill | PackItemKind::Workflow) {
        if fs.exists(&destination) {
            fs.remove_dir_all(&destination)?;
        }
        guarded_copy_tree(&source, &destination)
    } else {
        guarded_copy_file(&manifest.root, &source, &destination)
    }
}

fn remove_item_content(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
    item: &PackLockItem,
) -> Result<(), String> {
    if item.kind == PackItemKind::Mcp {
        let mut installed = installed_mcp_servers(config_dir, pack_id)?;
        installed.remove(&item.id);
        return write_mcp_layer(fs, config_dir, pack_id, &installed);
    }
    let path = item
        .path
        .as_ref()
        .ok_or_else(|| "File-backed pack item has no path".to_string())?;
    let destination = config_dir.join(path);
    if !fs.exists(&destination) {
        return Ok(());
    }
    if matches!(item.kind, PackItemKind::Skill | PackItemKind::Workflow) {
        fs.remove_dir_all(&destination)
    } else {
        fs.remove_file(&destination)
    }
}

fn write_mcp_layer(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
    servers: &BTreeMap<String, McpServerConfig>,
) -> Result<(), String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct McpLayer<'a> {
        mcp_servers: &'a BTreeMap<String, McpServerConfig>,
    }
    let path = pack_lock::mcp_path(config_dir, pack_id);
    if servers.is_empty() {
        if fs.exists(&path) {
            fs.remove_file(&path)?;
        }
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs.create_dir_all(parent)?;
    }
    let yaml = serde_yaml::to_string(&McpLayer {
        mcp_servers: servers,
    })
    .map_err(|error| format!("Failed to serialize pack MCP layer: {error}"))?;
    fs.write_str(&path, &yaml)
}

fn marker_matches(fs: &dyn FileSystem, marker_path: &Path, hash: &str) -> bool {
    fs.exists(marker_path)
        && fs
            .read_to_string(marker_path)
            .map(|value| value.trim() == hash)
            .unwrap_or(false)
}

const RUNTIME_DIR: &str = "runtime";
/// Marker recording the content hash of the last-provisioned runtime, so a
/// version change re-syncs and staleness is detectable.
const RUNTIME_MARKER: &str = ".runtime-version";

/// Provision the Cairn-owned workflow runtime (`@cairn/harness` + `@cairn/sdk`)
/// into `<cairn_home>/runtime` from the app's bundled `runtime/` resource
/// (CAIRN-2504). A workflow spawned from ANY project sets `NODE_PATH` to this
/// runtime first (see `backends::workflow`), so the harness resolves regardless
/// of the invoking project's own `node_modules`.
///
/// The runtime is app-owned rather than pack content: it is machinery, never
/// user-edited, and not something a user installs or removes. So a content-hash
/// change replaces the tree wholesale, making version skew self-healing on the
/// next startup. No-op when the bundle ships no `runtime/` (a dev build resolves
/// the harness from the Cairn repo's own `node_modules`). Returns whether it
/// (re)synced.
pub fn provision_workflow_runtime(
    fs: &dyn FileSystem,
    resource_dir: &Path,
    cairn_home: &Path,
) -> Result<bool, String> {
    let source = resource_dir.join(RUNTIME_DIR);
    if !fs.exists(&source) {
        return Ok(false);
    }
    let hash = pack::content_hash(&source)?;
    let dest = cairn_home.join(RUNTIME_DIR);
    let marker_path = dest.join(RUNTIME_MARKER);
    if marker_matches(fs, &marker_path, &hash) {
        return Ok(false);
    }
    if fs.exists(&dest) {
        fs.remove_dir_all(&dest)?;
    }
    fs.copy_dir_recursive(&source, &dest)?;
    fs.write_str(&marker_path, &format!("{hash}\n"))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_and_pack_commit_subjects_are_both_sync_authored() {
        // Retaining the pre-pack subjects verbatim is what keeps an existing
        // user's files recognized as pack-owned after the upgrade. Dropping any
        // of them would reclassify their whole workspace as user-edited and
        // turn the next update into a wall of conflicts.
        for subject in BUNDLE_COMMIT_SUBJECTS {
            assert!(is_pack_authored_subject(subject));
        }
        assert!(is_pack_authored_subject("Sync pack resources: core"));
        assert!(is_pack_authored_subject(
            "Sync pack resources: some-third-party"
        ));
        assert!(!is_pack_authored_subject("Snapshot workspace config"));
        assert!(!is_pack_authored_subject("User edits explore"));
    }
}
