//! Pack-aware workspace sync.
//!
//! Pack locks are the live ownership authority. Each installed item records the
//! hash Cairn last materialized and a durable fork bit. Git is consulted only to
//! classify pre-v2 locks once; migrated packs never invoke Git again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::mcp::McpVarSetup;
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

/// Sync an already resolved source through the canonical pack ownership machinery.
pub fn sync_resolved_pack(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifest: PackManifest,
    source: PackSource,
) -> Result<PackSyncResult, String> {
    refuse_invalid_skills(&manifest)?;
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

/// Refuse a non-bundled install whose pack id, destination path, or MCP server
/// name already belongs to something else, naming both sides.
///
/// Packs do not namespace what they install, so two sources offering
/// `skills/pdf` are a real conflict. The outcome is defined and never silent:
/// no overwrite, no rename. Runs before anything is written. An update of the
/// same pack from the same origin skips what that pack already holds.
/// Bundled sync does not call this; it keeps its adopt-as-fork migration rule.
pub fn check_install_collisions(
    config_dir: &Path,
    manifest: &PackManifest,
    incoming: &PackSource,
    shipped_ids: &[String],
) -> Result<(), String> {
    let id = &manifest.id;
    let incoming_label = describe_source(incoming);
    let refuse = |reason: String| {
        Err(format!(
            "{reason}; refusing to install pack `{id}` from {incoming_label}"
        ))
    };
    let own = pack_lock::read_lock(config_dir, id);
    match &own {
        Some(lock) if !same_origin(&lock.source, incoming) => {
            return refuse(format!(
                "pack id `{id}` is already installed from {}",
                describe_source(&lock.source)
            ));
        }
        None if shipped_ids.iter().any(|shipped| shipped == id) => {
            return refuse(format!("pack id `{id}` ships with the app"));
        }
        _ => {}
    }
    let own_items: BTreeSet<(PackItemKind, String)> = own
        .iter()
        .flat_map(|lock| {
            lock.items
                .iter()
                .map(PackLockItem::manifest_item)
                .chain(lock.removed.iter().cloned())
        })
        .map(|item| (item.kind, item.id))
        .collect();
    let others: Vec<PackLock> = pack_lock::installed_packs(config_dir)
        .into_iter()
        .filter(|lock| lock.id != *id)
        .collect();
    let pack_servers = pack::mcp::load_pack_mcp_servers(config_dir);
    let settings_servers = settings_mcp_server_names(config_dir);

    for item in manifest.items() {
        if own_items.contains(&(item.kind, item.id.clone())) {
            continue;
        }
        if item.kind == PackItemKind::Mcp {
            if let Some(owner) = pack_servers
                .get(&item.id)
                .filter(|server| server.pack_id != *id)
            {
                let source = others
                    .iter()
                    .find(|lock| lock.id == owner.pack_id)
                    .map(|lock| describe_source(&lock.source))
                    .unwrap_or_else(|| "an installed pack".to_string());
                return refuse(format!(
                    "MCP server `{}` is already defined by pack `{}` (from {source})",
                    item.id, owner.pack_id
                ));
            }
            if settings_servers.contains(&item.id) {
                return refuse(format!(
                    "MCP server `{}` is already defined in your settings.yaml",
                    item.id
                ));
            }
            continue;
        }
        let Some(path) = &item.path else { continue };
        if let Some(owner) = others.iter().find(|lock| {
            lock.items
                .iter()
                .filter_map(|claimed| claimed.path.as_deref())
                .chain(lock.removed.iter().filter_map(|gone| gone.path.as_deref()))
                .any(|claimed| claimed == path)
        }) {
            return refuse(format!(
                "`{path}` is installed from pack `{}` (from {})",
                owner.id,
                describe_source(&owner.source)
            ));
        }
        if config_dir.join(path).exists() {
            return refuse(format!(
                "you already have your own {} `{}` at `{path}`, which no pack installed",
                item.kind.as_str(),
                item.id
            ));
        }
    }
    Ok(())
}

/// Where a pack came from, in the words a refusal uses.
pub fn describe_source(source: &PackSource) -> String {
    match source.kind {
        pack::PackSourceKind::Bundled => "the app bundle".to_string(),
        pack::PackSourceKind::Local => format!(
            "a local import of {}",
            source.path.as_deref().unwrap_or("an unknown directory")
        ),
        pack::PackSourceKind::Url => {
            let url = source.url.as_deref().unwrap_or("an unknown URL");
            match &source.upstream {
                Some(upstream) => format!("{url} (upstream {})", upstream.url),
                None => url.to_string(),
            }
        }
    }
}

fn same_origin(left: &PackSource, right: &PackSource) -> bool {
    left.kind == right.kind
        && match left.kind {
            pack::PackSourceKind::Bundled => true,
            pack::PackSourceKind::Local => left.path == right.path,
            pack::PackSourceKind::Url => match (&left.url, &right.url) {
                (Some(a), Some(b)) => pack::library::source::same_repository(a, b),
                _ => false,
            },
        }
}

/// Server names the user's own `settings.yaml` defines.
fn settings_mcp_server_names(config_dir: &Path) -> BTreeSet<String> {
    std::fs::read_to_string(config_dir.join("settings.yaml"))
        .ok()
        .and_then(|text| serde_yaml::from_str::<serde_yaml::Value>(&text).ok())
        .and_then(|value| value.get("mcpServers").cloned())
        .and_then(|servers| servers.as_mapping().cloned())
        .map(|servers| {
            servers
                .keys()
                .filter_map(|key| key.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Refuse a whole pack when any skill its source ships fails the skill-package
/// rules, before anything is written.
///
/// The sync re-derives items from the source tree by convention at every step
/// (lock, retirement, collisions), so a skill the plugin loader skipped would
/// still be installed, and skipping it here alone would let a later
/// re-derivation bring it back. Refusing keeps one item set throughout.
fn refuse_invalid_skills(manifest: &PackManifest) -> Result<(), String> {
    let skills: Vec<_> = manifest
        .items()
        .into_iter()
        .filter(|item| item.kind == PackItemKind::Skill)
        .collect();
    if skills.is_empty() {
        return Ok(());
    }
    let root = manifest.root.canonicalize().map_err(|e| {
        format!(
            "Failed to resolve pack `{}` source {:?}: {e}",
            manifest.id, manifest.root
        )
    })?;
    let rejected: Vec<String> = skills
        .iter()
        .filter_map(|item| {
            let dir = root.join(item.path.as_deref().unwrap_or_default());
            pack::agent_plugin::validate_skill(&root, &item.id, &dir)
                .err()
                .map(|reason| format!("`{}` ({reason})", item.id))
        })
        .collect();
    if rejected.is_empty() {
        return Ok(());
    }
    Err(format!(
        "Pack `{}` was refused: {} {} failed skill validation. Nothing was written; fix or \
         remove {} in the pack's source and try again.",
        manifest.id,
        if rejected.len() == 1 {
            "skill"
        } else {
            "skills"
        },
        rejected.join(", "),
        if rejected.len() == 1 { "it" } else { "them" },
    ))
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
        if matches!(current, ContentHash::Missing) {
            continue;
        }
        let owned = matches_baseline(config_dir, item, &current)?;
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
                // Bytes identical to what this build ships cannot hold a user
                // edit worth keeping, so they need no history to be owned.
                let shipped = source_item_hash(manifest, item, &source_mcp)?;
                let owned = shipped.as_deref() == Some(hash.as_str()) || {
                    let history_path = item.path.clone().unwrap_or_else(|| {
                        format!(
                            "{}/{}/{}",
                            pack_lock::PACKS_DIR,
                            lock.id,
                            pack::manifest::PACK_MCP_FILE
                        )
                    });
                    legacy_path_is_pack_owned(git, config_dir, &history_path)
                };
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
        let owned = !old.forked && matches_baseline(config_dir, &old, &current)?;
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
                    if !matches_legacy_tree_baseline(config_dir, item, baseline)? {
                        item.forked = true;
                        continue;
                    }
                    // Same content under the pre-exclusion hash: re-record the
                    // baseline in today's terms so the fallback is paid once.
                    item.content_hash = Some(current_hash.clone());
                }
                if current_hash != source_hash {
                    materialize_item(fs, config_dir, &pack_id, manifest, item, &source_mcp)?;
                    item.content_hash = Some(source_hash);
                    if let Some(path) = &item.path {
                        outcome.changed(path.clone());
                    }
                }
            }
        }
    }

    refresh_mcp_setup(fs, config_dir, &pack_id, manifest)?;

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

/// Whether an item's current content is still the bytes Cairn last materialized.
pub fn matches_baseline(
    config_dir: &Path,
    item: &PackLockItem,
    current: &ContentHash,
) -> Result<bool, String> {
    let (ContentHash::Present(current), Some(baseline)) = (current, item.content_hash.as_deref())
    else {
        return Ok(false);
    };
    Ok(current == baseline || matches_legacy_tree_baseline(config_dir, item, baseline)?)
}

/// A skill or workflow baseline recorded before OS metadata left the tree hash
/// matches when the tree, metadata included, still hashes to it.
fn matches_legacy_tree_baseline(
    config_dir: &Path,
    item: &PackLockItem,
    baseline: &str,
) -> Result<bool, String> {
    if !matches!(item.kind, PackItemKind::Skill | PackItemKind::Workflow) {
        return Ok(false);
    }
    let Some(path) = &item.path else {
        return Ok(false);
    };
    Ok(
        pack::hash_tree_including_os_metadata(&config_dir.join(path))?
            == ContentHash::Present(baseline.to_string()),
    )
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
        return write_mcp_layer(
            fs,
            config_dir,
            pack_id,
            &installed,
            source_mcp_setup(manifest)?,
        );
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
        let setup = installed_mcp_setup(config_dir, pack_id)?;
        return write_mcp_layer(fs, config_dir, pack_id, &installed, setup);
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

/// The `setup` copy a pack ships for its servers' `${VAR}`s.
fn source_mcp_setup(manifest: &PackManifest) -> Result<BTreeMap<String, McpVarSetup>, String> {
    match manifest.mcp_source() {
        Some(path) => Ok(pack::mcp::parse_pack_mcp_file(&path)?.setup),
        None => Ok(BTreeMap::new()),
    }
}

fn installed_mcp_setup(
    config_dir: &Path,
    pack_id: &str,
) -> Result<BTreeMap<String, McpVarSetup>, String> {
    let path = pack_lock::mcp_path(config_dir, pack_id);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    Ok(pack::mcp::parse_pack_mcp_file(&path)?.setup)
}

/// Carry the shipped `setup` copy into an installed layer that lacks it.
///
/// The copy is what turns "needs setup" into a labelled field a user can act
/// on. Layers written before it was carried hold the servers alone, and the
/// item sync rewrites a layer only when a server changes, so this is what
/// brings an existing install up to date. Server definitions are rewritten
/// exactly as installed.
fn refresh_mcp_setup(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
    manifest: &PackManifest,
) -> Result<(), String> {
    let path = pack_lock::mcp_path(config_dir, pack_id);
    if !path.is_file() {
        return Ok(());
    }
    let shipped = source_mcp_setup(manifest)?;
    if installed_mcp_setup(config_dir, pack_id)? == shipped {
        return Ok(());
    }
    let servers = installed_mcp_servers(config_dir, pack_id)?;
    write_mcp_layer(fs, config_dir, pack_id, &servers, shipped)
}

fn write_mcp_layer(
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
    servers: &BTreeMap<String, McpServerConfig>,
    setup: BTreeMap<String, McpVarSetup>,
) -> Result<(), String> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct McpLayer<'a> {
        mcp_servers: &'a BTreeMap<String, McpServerConfig>,
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        setup: BTreeMap<String, McpVarSetup>,
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
        setup,
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
    use crate::manifest;
    use cairn_platform::services::RealFileSystem;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    /// A pack source with one skill and one MCP server.
    fn incoming(root: &Path, id: &str, skill: &str, server: &str) -> PackManifest {
        let dir = root.join(id);
        write(&dir.join("cairn-pack.yaml"), &format!("id: {id}\n"));
        write(
            &dir.join(format!("skills/{skill}/SKILL.md")),
            "---\nname: x\n---\n",
        );
        write(
            &dir.join("mcp.yaml"),
            &format!("mcpServers:\n  {server}:\n    type: http\n    url: https://x.test/mcp\n"),
        );
        manifest::load(&dir).unwrap()
    }

    fn library_source() -> PackSource {
        PackSource::library(
            "https://github.com/bleugreenlab/cairn-library".into(),
            "main".into(),
            "1111111111111111111111111111111111111111".into(),
            "k".into(),
            None,
            pack::PackFormat::Cairn,
        )
    }

    fn install(config: &Path, manifest: &PackManifest, source: PackSource) {
        let lock = PackLock::new(manifest, "h".into(), source, manifest.items());
        pack_lock::write_lock(&RealFileSystem, config, &lock).unwrap();
    }

    #[test]
    fn your_own_content_at_a_destination_is_refused_by_name() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("home");
        write(&config.join("skills/pdf/SKILL.md"), "mine\n");
        let manifest = incoming(temp.path(), "document-skills", "pdf", "docs");
        let error =
            check_install_collisions(&config, &manifest, &library_source(), &[]).unwrap_err();
        assert_eq!(
            error,
            "you already have your own skill `pdf` at `skills/pdf`, which no pack installed; \
             refusing to install pack `document-skills` from \
             https://github.com/bleugreenlab/cairn-library"
        );
    }

    #[test]
    fn a_path_or_server_another_pack_holds_is_refused_naming_that_pack() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("home");
        let first = incoming(&temp.path().join("a"), "first", "pdf", "shared");
        install(&config, &first, PackSource::local("/plugins/first".into()));
        write(
            &pack_lock::mcp_path(&config, "first"),
            "mcpServers:\n  shared:\n    type: http\n    url: https://x.test/mcp\n",
        );

        let same_path = incoming(&temp.path().join("b"), "second", "pdf", "other");
        let error =
            check_install_collisions(&config, &same_path, &library_source(), &[]).unwrap_err();
        assert!(
            error.starts_with(
                "`skills/pdf` is installed from pack `first` (from a local import of /plugins/first)"
            ),
            "{error}"
        );

        let same_server = incoming(&temp.path().join("c"), "third", "other", "shared");
        let error =
            check_install_collisions(&config, &same_server, &library_source(), &[]).unwrap_err();
        assert!(
            error.starts_with("MCP server `shared` is already defined by pack `first`"),
            "{error}"
        );
    }

    #[test]
    fn a_server_your_settings_define_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("home");
        write(
            &config.join("settings.yaml"),
            "mcpServers:\n  linear:\n    type: http\n    url: https://mine.test/mcp\n",
        );
        let manifest = incoming(temp.path(), "linear", "linear", "linear");
        let error =
            check_install_collisions(&config, &manifest, &library_source(), &[]).unwrap_err();
        assert!(
            error.starts_with("MCP server `linear` is already defined in your settings.yaml"),
            "{error}"
        );
    }

    #[test]
    fn a_taken_or_shipped_id_is_refused_and_a_same_origin_update_is_not() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("home");
        let manifest = incoming(temp.path(), "linear", "linear", "linear");

        let error = check_install_collisions(
            &config,
            &manifest,
            &library_source(),
            &["linear".to_string()],
        )
        .unwrap_err();
        assert!(
            error.starts_with("pack id `linear` ships with the app"),
            "{error}"
        );

        install(
            &config,
            &manifest,
            PackSource::local("/plugins/linear".into()),
        );
        let error =
            check_install_collisions(&config, &manifest, &library_source(), &[]).unwrap_err();
        assert!(
            error.starts_with(
                "pack id `linear` is already installed from a local import of /plugins/linear"
            ),
            "{error}"
        );

        install(&config, &manifest, library_source());
        write(&config.join("skills/linear/SKILL.md"), "materialized\n");
        assert!(
            check_install_collisions(&config, &manifest, &library_source(), &[]).is_ok(),
            "an update of the same pack from the same source holds its own paths"
        );
    }

    /// The installed layer keeps the pack's `setup` copy, so a gated server
    /// explains what it needs after install rather than only in the source.
    #[test]
    fn an_installed_mcp_layer_keeps_the_setup_copy() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("home");
        let source = temp.path().join("src/tool");
        write(&source.join("cairn-pack.yaml"), "id: tool\n");
        write(
            &source.join("mcp.yaml"),
            "mcpServers:\n  tool:\n    command: ${TOOL_BIN}\nsetup:\n  TOOL_BIN:\n    kind: path\n    label: Tool executable\n",
        );
        let manifest = manifest::load(&source).unwrap();
        sync_resolved_pack(
            &cairn_platform::services::RealGitClient,
            &RealFileSystem,
            &config,
            manifest,
            PackSource::local("/src/tool".into()),
        )
        .unwrap();
        let installed =
            pack::mcp::parse_pack_mcp_file(&pack_lock::mcp_path(&config, "tool")).unwrap();
        assert_eq!(
            installed.setup["TOOL_BIN"].label.as_deref(),
            Some("Tool executable")
        );

        // A layer written before the copy was carried is brought up to date.
        write(
            &pack_lock::mcp_path(&config, "tool"),
            "mcpServers:\n  tool:\n    command: ${TOOL_BIN}\n",
        );
        sync_resolved_pack(
            &cairn_platform::services::RealGitClient,
            &RealFileSystem,
            &config,
            manifest::load(&source).unwrap(),
            PackSource::local("/src/tool".into()),
        )
        .unwrap();
        let refreshed =
            pack::mcp::parse_pack_mcp_file(&pack_lock::mcp_path(&config, "tool")).unwrap();
        assert!(refreshed.setup.contains_key("TOOL_BIN"));
        assert_eq!(refreshed.servers, installed.servers);
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
        assert!(is_pack_authored_subject("Update bundled defaults to 0.8.7"));
        // Both carry the user's own content, so neither may prove ownership.
        assert!(!is_pack_authored_subject("Snapshot workspace config"));
        assert!(!is_pack_authored_subject("Merge branch 'bundle'"));
        assert!(!is_pack_authored_subject("User edits explore"));
    }

    #[test]
    fn a_source_skill_that_fails_validation_refuses_the_whole_pack() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source/notes-pack");
        let config = temp.path().join("home");
        write(
            &source.join("skills/good/SKILL.md"),
            "---\nname: good\ndescription: Useful.\n---\n",
        );
        write(
            &source.join("skills/bad/SKILL.md"),
            "---\nname: other\ndescription: Broken.\n---\n",
        );
        let manifest = pack::manifest::load(&source).unwrap();

        let error = sync_resolved_pack(
            &cairn_platform::services::RealGitClient,
            &cairn_platform::services::RealFileSystem,
            &config,
            manifest,
            PackSource::local("source".to_string()),
        )
        .unwrap_err();

        assert!(error.contains("skill `bad`"), "{error}");
        assert!(error.contains("does not match directory 'bad'"), "{error}");
        assert!(!error.contains("`good`"), "{error}");
        assert!(!config.exists(), "nothing is written");
    }
}
