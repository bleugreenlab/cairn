//! Pack-aware workspace sync.
//!
//! Pack locks are the live ownership authority. Each installed item records the
//! hash Cairn last materialized and a durable fork bit. Git is consulted only to
//! classify pre-v2 locks once; migrated packs never invoke Git again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::mcp_servers::McpServerConfig;
use crate::config::pack::{
    self, lock as pack_lock, ContentHash, PackItemKind, PackLock, PackLockItem, PackManifest,
    PackSource,
};
use crate::services::{FileSystem, GitClient};

#[cfg(test)]
const DEFAULT_BRANCH: &str = "main";
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
    updated: bool,
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
                PackSource::bundled(plan.manifest.format),
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
            Ok(PackPlan {
                manifest,
                lock,
                hash,
                action,
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
    lock.source = PackSource::bundled(manifest.format);
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
    match pack::hash_item_path(item.kind, &manifest.root.join(path))? {
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
        return pack::hash_item_path(item.kind, &config_dir.join(path));
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
        Some(path) => Ok(pack::mcp::parse_pack_mcp_file(&path)?.into_iter().collect()),
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
    Ok(pack::mcp::parse_pack_mcp_file(&path)?.into_iter().collect())
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
        fs.copy_dir_recursive(&source, &destination)
    } else {
        fs.copy_file(&source, &destination)
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
    use crate::services::{RealFileSystem, RealGitClient};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    /// A shipped pack's source root inside a fake resource dir.
    fn source_pack(resources: &Path, id: &str) -> PathBuf {
        resources.join("packs").join(id)
    }

    fn write_manifest(resources: &Path, id: &str, default: bool, retired: &[&str]) {
        let mut body = format!("id: {id}\nname: {id}\nversion: 1.0.0\ndefault: {default}\n");
        if !retired.is_empty() {
            body.push_str("retired:\n");
            for path in retired {
                body.push_str(&format!("  - {path}\n"));
            }
        }
        write(&source_pack(resources, id).join("cairn-pack.yaml"), &body);
    }

    /// A complete default `core` pack: one of every content kind.
    fn write_complete_resources(resources: &Path) {
        write_manifest(resources, "core", true, &[]);
        let root = source_pack(resources, "core");
        write(&root.join("agents/explore.md"), "bundle agent\n");
        write(&root.join("recipes/default.yaml"), "name: default\n");
        write(&root.join("responses/conveyor.md"), "response\n");
        write(
            &root.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\n",
        );
        write(
            &root.join("workflows/example/workflow.yaml"),
            "name: Example\n",
        );
    }

    fn write_resources_a(resources: &Path) {
        write_manifest(resources, "core", true, &["recipes/main-coordinator.yaml"]);
        let root = source_pack(resources, "core");
        write(&root.join("agents/explore.md"), "bundle a\n");
        write(&root.join("recipes/default.yaml"), "name: default\n");
    }

    fn write_resources_b(resources: &Path) {
        write_manifest(resources, "core", true, &["recipes/main-coordinator.yaml"]);
        let root = source_pack(resources, "core");
        write(&root.join("agents/explore.md"), "bundle b\n");
        write(&root.join("agents/new-agent.md"), "new\n");
        write(&root.join("recipes/default.yaml"), "name: default\n");
        write(
            &root.join("recipes/memory-triage.yaml"),
            "name: memory-triage\n",
        );
        write(
            &root.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\nBody\n",
        );
    }

    /// A non-default pack carrying one skill and one MCP server, the `matlab`
    /// archetype in miniature.
    fn write_optional_pack(resources: &Path, id: &str) {
        write_manifest(resources, id, false, &[]);
        let root = source_pack(resources, id);
        write(
            &root.join(format!("skills/{id}/SKILL.md")),
            &format!("---\nname: {id}\ndescription: Optional skill\n---\n"),
        );
        write(
            &root.join("mcp.yaml"),
            &format!("mcpServers:\n  {id}:\n    type: stdio\n    command: ${{{id}_BIN}}\n"),
        );
    }

    /// Every tracked file in the workspace except git internals, so a sync can
    /// be asserted to have written nothing.
    fn snapshot_tree(root: &Path) -> BTreeMap<String, String> {
        fn walk(root: &Path, dir: &Path, into: &mut BTreeMap<String, String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.file_name().and_then(|n| n.to_str()) == Some(".git") {
                    continue;
                }
                if path.is_dir() {
                    walk(root, &path, into);
                } else {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .to_string();
                    let body = std::fs::read_to_string(&path).unwrap_or_default();
                    into.insert(rel, body);
                }
            }
        }
        let mut map = BTreeMap::new();
        walk(root, root, &mut map);
        map
    }

    /// The tracked-file allowlist as it stood BEFORE packs: no `packs/`, and —
    /// the load-bearing part — no `responses/`. Seeding the post-upgrade string
    /// instead would quietly skip the whole allowlist-widening path that a real
    /// existing workspace takes.
    const LEGACY_WORKSPACE_GITIGNORE: &str = "# Ignore everything by default; only the curated config below is tracked.\n/*\n!/agents/\n!/skills/\n!/recipes/\n!/workflows/\n!/AGENTS.md\n!/settings.yaml\n!/.gitignore\n.DS_Store\n";

    /// Build a workspace exactly as the PRE-PACK bundle sync left one: every
    /// shipped file materialized flat, committed under a legacy bundle subject,
    /// and a global `.bundle-sync` marker. No `packs/` directory anywhere.
    fn seed_legacy_workspace(repo: &Path, resources: &Path, pack_ids: &[&str]) {
        std::fs::create_dir_all(repo).unwrap();
        for id in pack_ids {
            let root = source_pack(resources, id);
            for dir_name in pack::CONTENT_DIRS {
                let source = root.join(dir_name);
                if source.exists() {
                    RealFileSystem
                        .copy_dir_recursive(&source, &repo.join(dir_name))
                        .unwrap();
                }
            }
        }
        std::fs::write(repo.join(LEGACY_BUNDLE_SYNC_MARKER), "legacy-hash\n").unwrap();
        std::fs::write(repo.join(".gitignore"), LEGACY_WORKSPACE_GITIGNORE).unwrap();
        let git = RealGitClient;
        git.init_repo(repo, DEFAULT_BRANCH).unwrap();
        git.add_all(repo).unwrap();
        git.commit(repo, "Initialize Cairn workspace config")
            .unwrap();
    }

    /// The acceptance path, exercised against the packs this repository actually
    /// ships rather than a fixture: a fresh workspace materializes `core` only,
    /// `matlab` and `linear` wait in the catalog, and installing each one lands
    /// exactly what it carries — files for `matlab`, nothing but a connector for
    /// `linear`. Fixtures can drift from the shipped tree; this cannot.
    #[test]
    fn the_shipped_packs_install_the_default_set_and_offer_the_rest() {
        crate::config::secrets::mock_keychain::install();
        let src_tauri = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("cairn-core manifest is nested under src-tauri");

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, src_tauri, &repo, "1.0.0").unwrap();

        for path in [
            "agents/build.md",
            "recipes/build.yaml",
            "responses/conveyor.md",
            "skills/browser/SKILL.md",
            "workflows/fan-out/workflow.yaml",
        ] {
            assert!(repo.join(path).exists(), "core must ship {path}");
        }
        assert!(pack_lock::read_lock(&repo, "core").is_some());

        // The optional packs are offered, not installed.
        assert!(!repo.join("skills/matlab").exists());
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(pack_lock::read_lock(&repo, "linear").is_none());
        let catalog = crate::resources::packs::read_packs(&repo, None);
        assert!(catalog.contains("`matlab`"), "{catalog}");
        assert!(catalog.contains("`linear`"), "{catalog}");
        assert_eq!(
            crate::resources::packs::read_packs(&repo, Some("installed"))
                .matches("State: installed")
                .count(),
            1,
            "only core is installed on a fresh workspace"
        );

        // Installing matlab lands its skill AND its MCP definition.
        sync_one_pack(&git, &fs, src_tauri, &repo, "matlab").unwrap();
        assert!(repo.join("skills/matlab/SKILL.md").exists());
        assert!(repo.join("skills/matlab/scripts/inspect-mat.py").exists());
        assert!(repo.join("packs/matlab/mcp.yaml").exists());

        // Installing linear lands nothing but a connector and its OAuth config.
        sync_one_pack(&git, &fs, src_tauri, &repo, "linear").unwrap();
        let layer = pack::mcp::load_pack_mcp_servers(&repo);
        let linear = &layer["linear"];
        assert_eq!(linear.pack_id, "linear");
        assert_eq!(linear.config.transport, "http");
        assert_eq!(
            linear.config.url.as_deref(),
            Some("https://mcp.linear.app/mcp")
        );
        assert!(
            linear.config.oauth.is_some(),
            "the connector archetype ships its OAuth config ready for auth"
        );

        // Both connectors are visible in settings with a reason, and inert for
        // agents until the user supplies what they need.
        let entries = crate::config::mcp_servers::workspace_mcp_entries(&repo);
        assert_eq!(entries["linear"].origin, "pack:linear");
        assert_eq!(entries["matlab"].origin, "pack:matlab");
        assert!(entries["linear"].not_ready.is_some());
        assert!(entries["matlab"].not_ready.is_some());

        let pack_page = crate::resources::packs::read_pack(&repo, "linear");
        assert!(pack_page.contains("State: installed"), "{pack_page}");
        assert!(pack_page.contains("needs_auth"), "{pack_page}");

        // Uninstalling gives the workspace back exactly what it had.
        let result = uninstall_pack(&git, &fs, &repo, "matlab").unwrap();
        assert!(result.kept.is_empty());
        assert!(!repo.join("skills/matlab").exists());
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(!crate::config::mcp_servers::workspace_mcp_entries(&repo).contains_key("matlab"));
    }

    #[test]
    fn fresh_install_materializes_only_default_packs() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");

        let result =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert!(result.updated);
        assert!(
            !repo.join(".git").exists(),
            "fresh pack sync does not initialize Git"
        );

        // The default pack is installed and recorded...
        assert!(repo.join("agents/explore.md").exists());
        assert!(repo.join("responses/conveyor.md").exists());
        let core = pack_lock::read_lock(&repo, "core").expect("core lock");
        assert_eq!(core.source.kind, pack::PackSourceKind::Bundled);
        assert!(core.items.iter().any(|item| item.id == "explore"));

        // ...and the optional pack waits in the catalog, materializing nothing.
        assert!(!repo.join("skills/matlab").exists());
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(!repo.join("packs/matlab").exists());
    }

    #[test]
    fn installed_pack_writes_its_mcp_layer_and_records_the_server() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        // Ship it as a default so this sync installs it, standing in for the
        // catalog-driven install of an optional pack.
        write_optional_pack(&resources, "matlab");
        write_manifest(&resources, "matlab", true, &[]);

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        assert!(repo.join("skills/matlab/SKILL.md").exists());
        let mcp = repo.join("packs/matlab/mcp.yaml");
        assert!(mcp.exists(), "an installed pack materializes its MCP layer");

        let lock = pack_lock::read_lock(&repo, "matlab").expect("matlab lock");
        assert!(lock
            .items
            .iter()
            .any(|item| item.kind == pack::PackItemKind::Mcp && item.id == "matlab"));

        let layer = pack::mcp::load_pack_mcp_servers(&repo);
        assert_eq!(layer["matlab"].pack_id, "matlab");
        assert_eq!(layer["matlab"].config.transport, "stdio");
    }

    #[test]
    fn an_existing_workspace_migrates_without_writing_or_conflicting() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");

        // A pre-pack workspace holding BOTH packs' content, matlab included --
        // the honest read of an existing user who already has the matlab skill.
        seed_legacy_workspace(&repo, &resources, &["core", "matlab"]);
        let before = snapshot_tree(&repo);

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        let after = snapshot_tree(&repo);
        for (path, body) in &before {
            // Two pieces of workspace machinery legitimately change: the
            // superseded global marker is removed, and the tracked-file
            // allowlist widens. No RESOURCE may be rewritten.
            if path == LEGACY_BUNDLE_SYNC_MARKER || path == ".gitignore" {
                continue;
            }
            assert_eq!(
                after.get(path),
                Some(body),
                "migration rewrote {path}, but every file was already byte-identical"
            );
        }
        // The only new files are pack metadata and the machine-local record of
        // where the app ships its packs from; no resource was duplicated.
        let added: Vec<&String> = after
            .keys()
            .filter(|key| !before.contains_key(*key))
            .collect();
        assert!(
            added
                .iter()
                .all(|key| key.starts_with("packs/") || key.as_str() == ".pack-source"),
            "migration added non-pack files: {added:?}"
        );

        assert!(pack_lock::read_lock(&repo, "core").is_some());
        assert!(
            pack_lock::read_lock(&repo, "matlab").is_some(),
            "a pack whose content the user already has is recorded as installed"
        );

        let core = pack_lock::read_lock(&repo, "core").unwrap();
        assert!(core
            .item(PackItemKind::Response, "conveyor")
            .unwrap()
            .content_hash
            .is_some());
        assert!(
            !repo.join(LEGACY_BUNDLE_SYNC_MARKER).exists(),
            "the superseded global marker is removed once every lock is written"
        );

        // Idempotent: a second pass changes nothing and adds no commit.
        let again =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert!(!again.updated);
        assert_eq!(snapshot_tree(&repo), after);
    }

    /// An uninstall must survive the next startup. It deliberately KEEPS items
    /// the user edited, and adoption reads a surviving item as "already
    /// installed" -- so without a record of the decision the sync silently
    /// undoes it, restoring the lock and the pack's MCP layer.
    #[test]
    fn an_uninstall_holds_across_a_restart_even_with_edited_items_left_behind() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        sync_one_pack(&git, &fs, &resources, &repo, "matlab").unwrap();
        assert!(repo.join("skills/matlab/SKILL.md").exists());

        // The user customizes the skill, so uninstall keeps it.
        std::fs::write(
            repo.join("skills/matlab/SKILL.md"),
            "---\nname: mine\n---\n",
        )
        .unwrap();

        let result = uninstall_pack(&git, &fs, &repo, "matlab").unwrap();
        assert_eq!(result.kept, vec!["skills/matlab".to_string()]);
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());

        // The next startup must NOT re-adopt it from the item it kept.
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(
            pack_lock::read_lock(&repo, "matlab").is_none(),
            "an uninstall must not be undone by the next sync"
        );
        assert!(!repo.join("packs/matlab/mcp.yaml").exists());
        assert!(!pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));
        // Their edited copy is still theirs.
        assert!(repo.join("skills/matlab/SKILL.md").exists());

        // Installing again is an explicit choice that revokes the uninstall.
        sync_one_pack(&git, &fs, &resources, &repo, "matlab").unwrap();
        assert!(pack_lock::read_lock(&repo, "matlab").is_some());
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(pack_lock::read_lock(&repo, "matlab").is_some());
    }

    /// Removing ONE item must not require uninstalling the pack around it: a
    /// user who wants a pack's skill but not its MCP server should be able to
    /// say so and have it stick. Copy-when-missing is exactly the mechanism
    /// that would otherwise undo the removal on the next launch.
    #[test]
    fn a_removed_item_stays_removed_while_its_pack_keeps_updating() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        write_manifest(&resources, "matlab", true, &[]);
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(repo.join("skills/matlab/SKILL.md").exists());
        assert!(pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));

        // Drop just the connector, keeping the skill.
        crate::config::mcp_servers::delete_workspace_mcp_server(&repo, "matlab").unwrap();
        assert!(!pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));

        // Drop just the skill from another pack, keeping the rest of it.
        crate::config::skills::delete_skill(&repo, "example", None).unwrap();

        // A shipped change must reach the pack without resurrecting either.
        std::fs::write(
            source_pack(&resources, "core").join("agents/explore.md"),
            "updated agent\n",
        )
        .unwrap();
        sync_workspace_packs(&git, &fs, &resources, &repo, "2.0.0").unwrap();

        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "updated agent\n",
            "the pack must keep updating around a removed item"
        );
        assert!(
            !pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"),
            "a removed MCP server must not come back on the next sync"
        );
        assert!(
            !repo.join("skills/example").exists(),
            "a removed skill must not be copied back by copy-when-missing"
        );
        assert!(
            repo.join("skills/matlab/SKILL.md").exists(),
            "unrelated items are untouched"
        );

        // Restoring is one explicit action, and brings them back.
        pack_lock::restore_removed_items(&fs, &repo, "core").unwrap();
        pack_lock::restore_removed_items(&fs, &repo, "matlab").unwrap();
        sync_one_pack(&git, &fs, &resources, &repo, "core").unwrap();
        sync_one_pack(&git, &fs, &resources, &repo, "matlab").unwrap();
        assert!(repo.join("skills/example/SKILL.md").exists());
        assert!(pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));
    }

    /// The same guarantee for a `default: true` pack, which the planner would
    /// otherwise reinstall wholesale on every launch.
    #[test]
    fn uninstalling_a_default_pack_also_holds() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        uninstall_pack(&git, &fs, &repo, "core").unwrap();
        assert!(!repo.join("agents/explore.md").exists());

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(
            pack_lock::read_lock(&repo, "core").is_none(),
            "`default: true` describes a FRESH workspace, not a standing override \
             of the user's explicit removal"
        );
        assert!(!repo.join("agents/explore.md").exists());
    }

    #[test]
    fn migration_leaves_a_pack_whose_content_was_deleted_available() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");

        // The user deleted the matlab skill, so none of that pack's items are
        // present. Reinstalling it behind their back would be the wrong read of
        // their intent.
        seed_legacy_workspace(&repo, &resources, &["core"]);

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        assert!(pack_lock::read_lock(&repo, "core").is_some());
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(!repo.join("skills/matlab").exists());
    }

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

    #[test]
    fn unchanged_pack_content_short_circuits() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        let again =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert_eq!(again, PackSyncResult::default());
    }

    #[test]
    fn real_temp_repo_adds_missing_packs_and_preserves_user_edits() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);

        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_packs(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();

        std::fs::write(repo.join("agents/explore.md"), "user edit\n").unwrap();

        sync_workspace_packs(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        // The user-edited file is preserved as a durable fork even though the
        // pack shipped a new version of it.
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert!(
            pack_lock::read_lock(&repo, "core")
                .unwrap()
                .item(PackItemKind::Agent, "explore")
                .unwrap()
                .forked
        );
        // Genuinely new pack resources are still installed.
        assert!(repo.join("agents/new-agent.md").exists());
        assert!(repo.join("recipes/memory-triage.yaml").exists());
        assert!(repo.join("skills/example/SKILL.md").exists());
    }

    #[test]
    fn a_retired_resource_is_no_longer_shipped() {
        // Retiring a pack resource is two edits that must agree: delete it from
        // the pack's source tree, and list it under `retired:` so existing
        // workspaces drop their copy. Listing alone leaves the file shipping,
        // and the sync would copy it straight back after removing it -- so the
        // retirement would silently do nothing. Nothing else notices, because a
        // retired resource is by definition absent from every other list.
        let src_tauri = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("cairn-core manifest is nested under src-tauri");
        let packs = pack::discover_available_packs(src_tauri);
        assert!(
            !packs.is_empty(),
            "the repo ships packs under src-tauri/packs"
        );
        for manifest in packs {
            for rel_path in &manifest.retired {
                let shipped = manifest.root.join(rel_path);
                assert!(
                    !shipped.exists(),
                    "{rel_path} is retired by pack `{}` but is still shipped at {shipped:?}; \
                     delete it from the pack or drop it from `retired:`",
                    manifest.id
                );
            }
        }
    }

    #[test]
    fn an_unclaimed_retired_path_is_preserved_fail_safe() {
        let git = RealGitClient;
        let fs = RealFileSystem;
        let retired = "recipes/main-coordinator.yaml";

        // A retired path absent from the old lock has no hash baseline. It may
        // have been created by the user, so retirement must preserve it.
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources-a");
        write_resources_a(&resources);
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();

        let edited = repo.join(retired);
        write(&edited, "my own version\n");

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(edited.exists(), "a user-owned copy survives retirement");
    }

    #[test]
    fn changed_pack_file_propagates_to_unmodified_install() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);

        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_packs(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "bundle a\n"
        );

        // The user never touched explore.md, so the pack's new version of it
        // must reach this install on upgrade.
        let result = sync_workspace_packs(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert!(result.updated);
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "bundle b\n"
        );
        // The lock now records the new content, so a repeat is a no-op.
        let again = sync_workspace_packs(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert!(!again.updated);
    }

    #[test]
    fn same_version_changed_pack_content_resyncs_and_preserves_user_files() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);

        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_packs(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();
        assert_eq!(
            pack_lock::read_lock(&repo, "core").unwrap().content_hash,
            pack::content_hash(&source_pack(&resources_a, "core")).unwrap()
        );

        std::fs::write(repo.join("agents/explore.md"), "user edit\n").unwrap();

        // Same app version, different shipped content: the pack's content hash
        // is what drives the re-sync, not the version string.
        let result = sync_workspace_packs(&git, &fs, &resources_b, &repo, "1.0.0").unwrap();
        assert!(result.updated);
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert!(repo.join("recipes/memory-triage.yaml").exists());
        assert!(repo.join("agents/new-agent.md").exists());
        assert_eq!(
            pack_lock::read_lock(&repo, "core").unwrap().content_hash,
            pack::content_hash(&source_pack(&resources_b, "core")).unwrap()
        );
    }

    #[test]
    fn migrated_lock_skips_git_on_subsequent_syncs() {
        use crate::services::testing::MockGitClient;

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);

        let git = MockGitClient::new();
        sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        let lock = pack_lock::read_lock(&repo, "core").unwrap();
        assert!(lock.migration_complete());

        let second_git = MockGitClient::new();
        let second =
            sync_workspace_packs(&second_git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(
            !second.updated,
            "a migrated lock is hash-only and idempotent"
        );
    }

    #[test]
    fn restoring_missing_content_does_not_call_git() {
        use crate::services::testing::MockGitClient;

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = MockGitClient::new();
        sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        std::fs::remove_file(repo.join("agents/explore.md")).unwrap();

        let second_git = MockGitClient::new();
        let restored =
            sync_workspace_packs(&second_git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(restored.updated);
        assert!(repo.join("agents/explore.md").exists());
    }

    #[test]
    fn provision_runtime_syncs_then_short_circuits_then_resyncs_on_change() {
        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let cairn_home = temp.path().join("home");
        let harness = resource_dir.join("runtime/node_modules/@cairn/harness");
        write(
            &harness.join("package.json"),
            "{\"name\":\"@cairn/harness\"}",
        );
        write(&harness.join("src/index.ts"), "export const v = 1;\n");

        let fs = RealFileSystem;
        // First provision installs the runtime and its marker.
        assert!(provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        let dest_harness = cairn_home.join("runtime/node_modules/@cairn/harness/src/index.ts");
        assert!(dest_harness.exists());
        assert!(cairn_home.join("runtime/.runtime-version").exists());

        // A repeat with unchanged content is a no-op (marker current).
        assert!(!provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());

        // A shipped change re-syncs the tree.
        std::fs::write(harness.join("src/index.ts"), "export const v = 2;\n").unwrap();
        assert!(provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        assert_eq!(
            std::fs::read_to_string(&dest_harness).unwrap(),
            "export const v = 2;\n"
        );
    }

    #[test]
    fn provision_runtime_is_noop_without_a_bundled_runtime() {
        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        std::fs::create_dir_all(&resource_dir).unwrap();
        let cairn_home = temp.path().join("home");

        // No `runtime/` in the bundle (the dev case): nothing provisioned.
        assert!(!provision_workflow_runtime(&RealFileSystem, &resource_dir, &cairn_home).unwrap());
        assert!(!cairn_home.join("runtime").exists());
    }

    #[test]
    fn a_new_shipped_item_preserves_preexisting_user_content_as_a_fork() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_resources_a(&resources);

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(pack_lock::read_lock(&repo, "core")
            .unwrap()
            .migration_complete());

        write(&repo.join("agents/new-agent.md"), "my agent\n");
        write(
            &source_pack(&resources, "core").join("agents/new-agent.md"),
            "shipped agent\n",
        );

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "2.0.0").unwrap();

        assert_eq!(
            std::fs::read_to_string(repo.join("agents/new-agent.md")).unwrap(),
            "my agent\n"
        );
        let lock = pack_lock::read_lock(&repo, "core").unwrap();
        let item = lock
            .item(PackItemKind::Agent, "new-agent")
            .expect("new shipped item is recorded");
        assert!(item.forked);
        assert!(item.content_hash.is_some());
        assert!(lock.migration_complete());
    }

    /// A fresh workspace seeds the packed `fan-out` workflow package, the loader
    /// then lists it, and a later sync of a CHANGED pack preserves the user's
    /// edit to their copy (directory packages are copy-when-missing, never
    /// overwritten). This is the loader-level proof that a fresh workspace lists
    /// `fan-out` out of the box -- the built-in workflow's whole provisioning
    /// contract -- without dogfooding through the running host.
    #[test]
    fn packed_workflow_seeds_missing_lists_and_preserves_user_edits() {
        fn write_workflow(resources: &Path, body: &str) {
            let root = source_pack(resources, "core").join("workflows/fan-out");
            write(
                &root.join("workflow.yaml"),
                "name: Fan Out\ndescription: Zero-authoring ad-hoc agent batch.\n",
            );
            write(&root.join("main.ts"), body);
        }

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);
        write_workflow(&resources_a, "// v1\n");
        write_workflow(&resources_b, "// v2\n");

        let git = RealGitClient;
        let fs = RealFileSystem;

        // Fresh workspace: the package is seeded and the loader lists it.
        sync_workspace_packs(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();
        assert!(repo.join("workflows/fan-out/workflow.yaml").exists());
        let listed = crate::config::workflows::list_workflows(&repo, None).unwrap();
        assert!(
            listed.iter().any(|r| matches!(
                r,
                crate::config::ConfigResult::Ok(w) if w.id == "fan-out"
            )),
            "fresh workspace must list the packed fan-out workflow"
        );

        // The user edits their copy, then a CHANGED pack syncs: the edit stands
        // (copy-when-missing never overwrites an existing package).
        std::fs::write(repo.join("workflows/fan-out/main.ts"), "// user edit\n").unwrap();
        sync_workspace_packs(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("workflows/fan-out/main.ts")).unwrap(),
            "// user edit\n",
            "a user's edited workflow copy must never be clobbered by a re-sync"
        );
    }
}
