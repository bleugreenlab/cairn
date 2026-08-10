//! Pack-aware workspace sync.
//!
//! `~/.cairn` is a git repository, and that history is the ownership oracle: a
//! managed file whose most recent commit carries a subject the sync itself
//! authored has not been touched by the user, so a newer shipped version may
//! overwrite it in place. Any other last commit marks the file the user's, and
//! it is preserved and reported as a skipped conflict.
//!
//! Packs make the SOURCE side plural while leaving the destination identical.
//! Each shipped pack owns a subtree of `resource_dir/packs/<id>/` whose layout
//! mirrors the flat workspace layout exactly, so installing a pack is the same
//! copy the single monolithic bundle always performed -- just scoped to one
//! pack's contents and recorded in that pack's install lock.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::pack::{self, lock as pack_lock, PackLock, PackManifest, PackSource};
use crate::services::{FileSystem, GitClient, GitOutput};

use super::repo::ensure_workspace_repo;

const DEFAULT_BRANCH: &str = "main";

/// The pre-pack global sync marker: one content hash for the entire shipped
/// tree. Superseded by each pack's own `contentHash` in its install lock, and
/// removed once those locks exist -- its absence is what makes the migration to
/// packs idempotent.
const LEGACY_BUNDLE_SYNC_MARKER: &str = ".bundle-sync";

/// Commit subjects the workspace sync authored before packs existed. They are
/// retained verbatim so an existing workspace's history keeps meaning after the
/// upgrade: every file such a user holds is still recognized as pack-owned,
/// which is precisely what keeps the first pack-aware sync conflict-free.
const BUNDLE_COMMIT_SUBJECTS: &[&str] = &[
    "Initialize Cairn workspace config",
    "Add missing bundled workspace defaults",
    "Sync bundled workspace defaults",
];

/// Subject prefix for a pack-authored commit: `Sync pack resources: <id>`.
const PACK_COMMIT_PREFIX: &str = "Sync pack resources: ";

fn pack_commit_subject(pack_id: &str) -> String {
    format!("{PACK_COMMIT_PREFIX}{pack_id}")
}

/// Whether `subject` was authored by the workspace sync rather than by the user.
fn is_pack_authored_subject(subject: &str) -> bool {
    BUNDLE_COMMIT_SUBJECTS.contains(&subject) || subject.starts_with(PACK_COMMIT_PREFIX)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackSyncResult {
    updated: bool,
    /// Workspace-relative paths this sync wrote or retired. Reported so a
    /// catalog action can say what it actually changed rather than only that
    /// something did.
    pub changed_paths: Vec<String>,
    pub skipped_conflicts: Vec<String>,
}

#[derive(Default)]
struct SyncOutcome {
    changed: bool,
    /// Workspace-relative paths written or removed by this sync. Both are
    /// committed the same way: `git add --` records a deletion as readily as an
    /// addition, so a retirement lands in the pack-owned commit.
    copied_paths: Vec<String>,
    skipped_conflicts: Vec<String>,
}

impl SyncOutcome {
    fn merge(&mut self, other: SyncOutcome) {
        self.changed |= other.changed;
        self.copied_paths.extend(other.copied_paths);
        for conflict in other.skipped_conflicts {
            if !self.skipped_conflicts.contains(&conflict) {
                self.skipped_conflicts.push(conflict);
            }
        }
    }
}

/// What this sync does with one shipped pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackAction {
    /// Materialize this pack's contents and keep them current.
    Sync,
    /// Available but not installed: leave it for the catalog to offer.
    Skip,
}

struct PackPlan {
    manifest: PackManifest,
    /// Install state recorded before this sync, if any.
    lock: Option<PackLock>,
    /// Content hash of the pack's shipped source tree.
    hash: String,
    action: PackAction,
}

impl PackPlan {
    /// Whether the shipped pack differs from what the workspace recorded, so an
    /// in-place update of pack-owned files may be needed. An unrecorded pack is
    /// stale by definition.
    fn is_stale(&self) -> bool {
        match &self.lock {
            Some(lock) => lock.content_hash != self.hash,
            None => true,
        }
    }

    fn syncing(&self) -> bool {
        self.action == PackAction::Sync
    }
}

/// Sync every shipped pack into the workspace config tree.
///
/// Replaces the single-bundle sync: the destination layout, the ownership rules,
/// and the conflict reporting are unchanged, but the source is now a set of
/// packs and installation is per-pack and recorded.
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
    let result = apply_plans(git, fs, config_dir, &plans)?;

    // The pre-pack global marker is superseded by the per-pack locks. Removing
    // it only after a FULL sync has written every lock keeps the migration
    // idempotent: an interrupted run still finds the marker and re-derives the
    // same plan.
    if plans.iter().any(|plan| plan.syncing()) {
        let marker = config_dir.join(LEGACY_BUNDLE_SYNC_MARKER);
        if fs.exists(&marker) {
            fs.remove_file(&marker)?;
        }
    }

    Ok(result)
}

/// Every managed content directory exists in a synced workspace, whether or not
/// any installed pack ships into it.
///
/// This is a property of the WORKSPACE, not of a pack. Code that writes a user's
/// own agent, recipe, or workflow targets `<workspace>/<kind>/` directly and
/// does not create the directory first, so a missing one surfaces as a bare
/// `NotFound` from an ordinary "create a recipe" action. Before packs the
/// invariant fell out of the sync walking one fixed directory list; now that the
/// source side is plural and the destination is shared, no single pack implies
/// the workspace's shape, so it is asserted here instead.
fn ensure_workspace_content_dirs(fs: &dyn FileSystem, config_dir: &Path) -> Result<(), String> {
    for dir_name in pack::CONTENT_DIRS {
        fs.create_dir_all(&config_dir.join(dir_name))?;
    }
    Ok(())
}

/// Install, or re-sync, ONE shipped pack by id.
///
/// This is the catalog's install/update action. It runs exactly the machinery
/// the startup sync runs, with one difference: an explicit choice overrides the
/// default-set question that decides participation on startup, so a pack the app
/// ships as optional installs when asked for.
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

    // Installing is an explicit choice that supersedes an earlier uninstall.
    pack_lock::clear_uninstall(fs, config_dir, pack_id)?;

    let mut plans = plan_packs(fs, config_dir, vec![manifest])?;
    for plan in &mut plans {
        plan.action = PackAction::Sync;
    }
    apply_plans(git, fs, config_dir, &plans)
}

/// Remove an installed pack's contents, keeping anything the user made theirs.
///
/// Ownership is the same git-subject question an update asks, so an item the
/// user edited survives an uninstall and is reported rather than deleted. The
/// pack's own `packs/<id>/` directory — its lock and MCP layer — always goes,
/// which is what makes the pack "not installed" again.
pub fn uninstall_pack(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    pack_id: &str,
) -> Result<PackUninstallResult, String> {
    let lock = pack_lock::read_lock(config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;

    let mut result = PackUninstallResult::default();
    for rel_path in lock.item_paths() {
        let dest = config_dir.join(&rel_path);
        if !fs.exists(&dest) {
            continue;
        }
        if pack_file_is_user_owned(git, config_dir, &rel_path).unwrap_or(true) {
            result.kept.push(rel_path);
            continue;
        }
        if dest.is_dir() {
            fs.remove_dir_all(&dest)?;
        } else {
            fs.remove_file(&dest)?;
        }
        result.removed.push(rel_path);
    }

    // Replace the pack's whole directory with a record of the decision, so the
    // next startup does not re-adopt it from an item the uninstall preserved.
    pack_lock::remove_pack_dir(fs, config_dir, pack_id)?;
    pack_lock::record_uninstall(fs, config_dir, pack_id)?;

    let mut paths = result.removed.clone();
    paths.push(format!("{}/{pack_id}", pack_lock::PACKS_DIR));
    stage_and_commit(
        git,
        config_dir,
        &format!("Uninstall pack: {pack_id}"),
        &paths,
    )?;

    Ok(result)
}

/// What an uninstall did: paths it removed, and paths it left alone because the
/// user had made them theirs.
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
    plans: &[PackPlan],
) -> Result<PackSyncResult, String> {
    // Materializing missing defaults is deliberately independent of Git. A
    // missing or undiscoverable Git binary may prevent history management, but
    // it must not leave a fresh workspace without usable resources.
    let mut outcome = SyncOutcome::default();
    for plan in plans.iter().filter(|plan| plan.syncing()) {
        outcome.merge(sync_pack_resources(git, fs, config_dir, plan, false)?);
    }

    let repo_exists = git.is_repo(config_dir)?;

    if !repo_exists {
        git.init_repo(config_dir, DEFAULT_BRANCH)?;
        ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;
        initialize_pack_history(git, fs, config_dir, plans)?;
        record_pack_state(git, fs, config_dir, plans)?;
        return Ok(sync_result(true, outcome));
    }

    ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;

    if git.root_commit(config_dir, DEFAULT_BRANCH).is_err() {
        // Recovery may have been interrupted after repository initialization.
        // Rebuild the unborn repository's history with the same ownership
        // classification used on the first recovery attempt.
        initialize_pack_history(git, fs, config_dir, plans)?;
        record_pack_state(git, fs, config_dir, plans)?;
        return Ok(sync_result(true, outcome));
    }

    // Keep newly materialized defaults pack-owned without folding unrelated
    // pending user edits into the pack commit.
    commit_copied_defaults(git, config_dir, &outcome.copied_paths)?;

    // `ensure_workspace_repo` above may have widened the tracked-file allowlist
    // (a release that starts managing a new directory). Files it newly exposes
    // have no history at all, so the ownership oracle fails safe to user-owned
    // -- and the snapshot below would commit them under "Snapshot workspace
    // config", making that permanent and freezing them against every future
    // update. Claim the ones whose content still matches their shipped source
    // FIRST, which is the same classification the unborn-repo path uses.
    adopt_newly_tracked_pack_files(git, fs, config_dir, plans)?;

    let stale: Vec<&PackPlan> = plans
        .iter()
        .filter(|plan| plan.syncing() && plan.is_stale())
        .collect();

    if !stale.is_empty() {
        // Some pack's content changed since it was recorded, so an in-place
        // update may overwrite managed files. Snapshot any uncommitted user
        // edits first so an overwrite can never lose unsaved work, and so an
        // edited file is committed under a non-pack subject that marks it
        // user-owned.
        snapshot_pending_user_edits(git, config_dir)?;
    }

    for plan in stale {
        let updated = sync_pack_resources(git, fs, config_dir, plan, true)?;
        let changed = updated.changed;
        let paths = updated.copied_paths.clone();
        outcome.merge(updated);
        if changed {
            stage_and_commit(
                git,
                config_dir,
                &pack_commit_subject(&plan.manifest.id),
                &paths,
            )?;
        }
    }

    let recorded = record_pack_state(git, fs, config_dir, plans)?;
    let updated = outcome.changed || recorded;

    Ok(sync_result(updated, outcome))
}

fn sync_result(updated: bool, outcome: SyncOutcome) -> PackSyncResult {
    let mut changed_paths = outcome.copied_paths;
    changed_paths.sort();
    changed_paths.dedup();
    PackSyncResult {
        updated,
        changed_paths,
        skipped_conflicts: outcome.skipped_conflicts,
    }
}

/// Decide what happens to each shipped pack, before any writes, so adoption can
/// observe the pre-sync workspace.
fn plan_packs(
    fs: &dyn FileSystem,
    config_dir: &Path,
    manifests: Vec<PackManifest>,
) -> Result<Vec<PackPlan>, String> {
    let mut plans = Vec::new();
    for manifest in manifests {
        let hash = pack::content_hash(&manifest.root)?;
        let lock = pack_lock::read_lock(config_dir, &manifest.id);
        let recorded = lock.is_some();
        let uninstalled = pack_lock::is_uninstalled(config_dir, &manifest.id);
        let adopted = !recorded && !uninstalled && pack_is_materialized(fs, config_dir, &manifest);
        if adopted {
            log::info!(
                "Pack `{}` is already materialized in {config_dir:?}; recording it as installed",
                manifest.id
            );
        }
        // An explicit uninstall outranks BOTH adoption and the default set. An
        // uninstall deliberately keeps items the user edited, and adoption reads
        // a surviving item as "already installed" — so without this the next
        // startup would silently undo the removal, and a `default: true` pack
        // would reinstall wholesale.
        let action = if uninstalled {
            PackAction::Skip
        } else if recorded || adopted || manifest.default {
            PackAction::Sync
        } else {
            PackAction::Skip
        };
        plans.push(PackPlan {
            manifest,
            lock,
            hash,
            action,
        });
    }
    Ok(plans)
}

/// Whether any of this pack's items already sit where an install would write
/// them.
///
/// True for a workspace seeded by the pre-pack bundle sync, whose files are
/// byte-identical and at exactly those paths. Adopting such a pack is what makes
/// the migration a lock write with zero resource writes and zero conflicts. A
/// user who deleted a pack's content entirely reads as false, so that pack
/// correctly shows as available rather than being reinstalled behind their back.
fn pack_is_materialized(fs: &dyn FileSystem, config_dir: &Path, manifest: &PackManifest) -> bool {
    manifest.items().iter().any(|item| {
        item.path
            .as_deref()
            .is_some_and(|path| fs.exists(&config_dir.join(path)))
    })
}

/// Copy one pack's resources into the workspace config tree. Always copies files
/// whose destination is missing. When `allow_update` is set (an established repo
/// whose pack content changed), a file whose content differs from the shipped
/// source is overwritten **only if it is still pack-owned** (see
/// [`pack_file_is_user_owned`]); a user-customized file is left untouched and
/// reported as a skipped conflict. Skill and workflow packages are multi-file
/// directories and are only ever copy-when-missing.
fn sync_pack_resources(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    plan: &PackPlan,
    allow_update: bool,
) -> Result<SyncOutcome, String> {
    let mut outcome = SyncOutcome::default();
    let source_root = &plan.manifest.root;
    // Items the user removed from this pack. They stay out: copy-when-missing
    // is exactly the mechanism that would otherwise undo the removal on the
    // next launch, which would make a per-item delete meaningless.
    let removed: std::collections::BTreeSet<String> = plan
        .lock
        .as_ref()
        .map(|lock| lock.removed_paths().into_iter().collect())
        .unwrap_or_default();

    for dir_name in pack::CONTENT_DIRS {
        let source_dir = source_root.join(dir_name);
        if !source_dir.exists() {
            continue;
        }
        let dest_dir = config_dir.join(dir_name);
        fs.create_dir_all(&dest_dir)?;

        let mut entries = std::fs::read_dir(&source_dir)
            .map_err(|e| format!("Failed to read pack {dir_name} directory {source_dir:?}: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to read pack {dir_name} entry: {e}"))?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let source = entry.path();
            let file_name = entry.file_name();
            let dest = dest_dir.join(&file_name);
            let rel_path = format!("{dir_name}/{}", file_name.to_string_lossy());
            if removed.contains(&rel_path) {
                continue;
            }

            // Skill and workflow packages are versioned directory trees keyed by
            // a manifest file. In-place update of a multi-file package is out of
            // scope, so they are copy-when-missing only -- which also means a
            // user's edited copy is never overwritten on a later sync.
            if let Some(manifest) = package_manifest_file(dir_name) {
                if source.is_dir() && source.join(manifest).exists() && !fs.exists(&dest) {
                    fs.copy_dir_recursive(&source, &dest)?;
                    outcome.changed = true;
                    outcome.copied_paths.push(rel_path);
                }
                continue;
            }

            if !(source.is_file() && pack_file_matches_dir(dir_name, &source)) {
                continue;
            }

            if !fs.exists(&dest) {
                fs.copy_file(&source, &dest)?;
                outcome.changed = true;
                outcome.copied_paths.push(rel_path);
                continue;
            }

            if !allow_update {
                continue;
            }

            // Dest exists and the pack changed. Overwrite only an unmodified
            // (pack-owned) file whose content actually differs from the source.
            if fs.read_to_string(&source)? == fs.read_to_string(&dest)? {
                continue;
            }

            if pack_file_is_user_owned(git, config_dir, &rel_path)? {
                outcome.skipped_conflicts.push(rel_path);
            } else {
                fs.copy_file(&source, &dest)?;
                outcome.changed = true;
                outcome.copied_paths.push(rel_path);
            }
        }
    }

    for rel_path in &plan.manifest.retired {
        let dest = config_dir.join(rel_path);
        if !fs.exists(&dest) {
            continue;
        }
        // A Git failure means ownership cannot be established, and materializing
        // defaults stays independent of Git -- so keep the file rather than
        // deleting something that might be the user's.
        if pack_file_is_user_owned(git, config_dir, rel_path).unwrap_or(true) {
            outcome.skipped_conflicts.push(rel_path.clone());
            continue;
        }
        fs.remove_file(&dest)?;
        outcome.changed = true;
        outcome.copied_paths.push(rel_path.clone());
    }

    Ok(outcome)
}

/// Write each synced pack's `packs/<id>/` state -- its MCP definitions and its
/// install lock -- and commit them as pack-owned. The lock is what makes a pack
/// "installed", so for an adopted pack this write IS the whole migration.
/// Returns whether anything was written.
fn record_pack_state(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    plans: &[PackPlan],
) -> Result<bool, String> {
    let mut wrote_any = false;

    for plan in plans.iter().filter(|plan| plan.syncing()) {
        let id = &plan.manifest.id;
        let mut paths = Vec::new();

        if let Some(source) = plan.manifest.mcp_source() {
            let dest = pack_lock::mcp_path(config_dir, id);
            // Copied verbatim rather than re-serialized: YAML is a superset of
            // JSON, so an ingested `.mcp.json` stays byte-faithful to its
            // source and a diff against the shipped file stays meaningful.
            let differs =
                !fs.exists(&dest) || fs.read_to_string(&source)? != fs.read_to_string(&dest)?;
            if differs {
                if let Some(parent) = dest.parent() {
                    fs.create_dir_all(parent)?;
                }
                fs.copy_file(&source, &dest)?;
                paths.push(format!(
                    "{}/{id}/{}",
                    pack_lock::PACKS_DIR,
                    pack::manifest::PACK_MCP_FILE
                ));
            }
        }

        if plan.is_stale() {
            // A pack update must not resurrect what the user removed, so the
            // previous lock's removals carry forward and its items are held
            // back from the freshly discovered set.
            let removed = plan
                .lock
                .as_ref()
                .map(|lock| lock.removed.clone())
                .unwrap_or_default();
            let items = plan
                .manifest
                .items()
                .into_iter()
                .filter(|item| {
                    !removed
                        .iter()
                        .any(|gone| gone.kind == item.kind && gone.id == item.id)
                })
                .collect();
            let mut lock = PackLock::new(
                &plan.manifest,
                plan.hash.clone(),
                PackSource::bundled(plan.manifest.format),
                items,
            );
            lock.removed = removed;
            pack_lock::write_lock(fs, config_dir, &lock)?;
            paths.push(format!(
                "{}/{id}/{}",
                pack_lock::PACKS_DIR,
                pack_lock::LOCK_FILE
            ));
        }

        if paths.is_empty() {
            continue;
        }
        wrote_any = true;
        stage_and_commit(git, config_dir, &pack_commit_subject(id), &paths)?;
    }

    Ok(wrote_any)
}

fn commit_copied_defaults(
    git: &dyn GitClient,
    config_dir: &Path,
    copied_paths: &[String],
) -> Result<(), String> {
    if copied_paths.is_empty() {
        return Ok(());
    }
    let mut paths = vec![".gitignore".to_string()];
    paths.extend(copied_paths.iter().cloned());
    stage_and_commit(
        git,
        config_dir,
        "Add missing bundled workspace defaults",
        &paths,
    )
}

fn stage_and_commit(
    git: &dyn GitClient,
    config_dir: &Path,
    subject: &str,
    paths: &[String],
) -> Result<(), String> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut add_args = vec!["add".to_string(), "--".to_string()];
    add_args.extend(paths.iter().cloned());
    let output = git.run(config_dir, add_args)?;
    if !output.success {
        return Err(format!(
            "Failed to stage pack resources for `{subject}`: {}",
            output.stderr
        ));
    }
    commit_only_paths(git, config_dir, subject, paths)
}

fn commit_only_paths(
    git: &dyn GitClient,
    config_dir: &Path,
    subject: &str,
    paths: &[String],
) -> Result<(), String> {
    let mut args = vec![
        "-c".to_string(),
        "user.name=Cairn".to_string(),
        "-c".to_string(),
        "user.email=cairn@local.invalid".to_string(),
        "commit".to_string(),
        "--only".to_string(),
        "-m".to_string(),
        subject.to_string(),
        "--".to_string(),
    ];
    args.extend(paths.iter().cloned());
    let output = git.run(config_dir, args)?;
    // A path-scoped commit that finds nothing to commit is a legitimate no-op:
    // restoring a byte-identical default (a working-tree deletion reverted with
    // identical content) leaves the named paths already matching HEAD. Git
    // reports this with one of several phrasings depending on the surrounding
    // tree state, all of which must succeed so a no-op restore never fails the
    // whole sync -- and, with `--only`, an unrelated staged change is left
    // untouched.
    if output.success || is_nothing_to_commit(&output) {
        Ok(())
    } else {
        Err(format!("git commit failed: {}", output.stderr))
    }
}

/// Whether a failed `git commit` failed only because the requested scope had
/// nothing to commit. Git emits one of several phrasings depending on tree
/// state; all mean the working tree already matches HEAD for that scope.
fn is_nothing_to_commit(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr);
    combined.contains("nothing to commit")
        || combined.contains("nothing added to commit")
        || combined.contains("no changes added to commit")
}

fn initialize_pack_history(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    plans: &[PackPlan],
) -> Result<(), String> {
    // Defaults may have been edited after an earlier Git-less provisioning
    // pass. Build the baseline without those paths, then snapshot everything
    // else under a user-owned subject so future upgrades cannot overwrite it.
    let pack_owned_paths = classify_untracked_pack_paths(fs, config_dir, plans)?;
    git.add_all(config_dir)?;
    commit_only_paths(
        git,
        config_dir,
        "Initialize Cairn workspace config",
        &pack_owned_paths,
    )?;
    snapshot_pending_user_edits(git, config_dir)
}

/// Workspace paths that still match their shipped source byte-for-byte, and so
/// belong in the pack-owned baseline commit. A path whose content already
/// diverges is left out, to be snapshotted as the user's.
fn classify_untracked_pack_paths(
    fs: &dyn FileSystem,
    config_dir: &Path,
    plans: &[PackPlan],
) -> Result<Vec<String>, String> {
    let mut pack_owned = vec![".gitignore".to_string()];
    for plan in plans.iter().filter(|plan| plan.syncing()) {
        pack_owned.extend(pack_owned_paths(fs, config_dir, plan)?);
    }
    Ok(pack_owned)
}

/// One pack's workspace paths whose content still matches what it ships.
fn pack_owned_paths(
    fs: &dyn FileSystem,
    config_dir: &Path,
    plan: &PackPlan,
) -> Result<Vec<String>, String> {
    let mut pack_owned = Vec::new();

    for dir_name in pack::CONTENT_DIRS {
        let source_dir = plan.manifest.root.join(dir_name);
        if !source_dir.exists() {
            continue;
        }
        let mut entries = std::fs::read_dir(&source_dir)
            .map_err(|error| format!("Failed to read pack {dir_name} directory: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("Failed to read pack {dir_name} entry: {error}"))?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let source = entry.path();
            let file_name = entry.file_name();
            let relative = format!("{dir_name}/{}", file_name.to_string_lossy());
            let destination = config_dir.join(&relative);
            if !fs.exists(&destination) {
                continue;
            }
            if let Some(manifest) = package_manifest_file(dir_name) {
                if source.is_dir() && source.join(manifest).exists() {
                    pack_owned.push(relative);
                }
                continue;
            }
            if source.is_file()
                && pack_file_matches_dir(dir_name, &source)
                && fs.read_to_string(&source)? == fs.read_to_string(&destination)?
            {
                pack_owned.push(relative);
            }
        }
    }

    Ok(pack_owned)
}

/// Commit, under each pack's own subject, the workspace files that are still
/// untracked but now inside the allowlist and still identical to what that pack
/// ships.
///
/// This is the seam a widening allowlist passes through. Without it such a file
/// is first seen by `snapshot_pending_user_edits`, which commits it under a
/// user-owned subject -- permanently, since the oracle only ever reads the most
/// recent commit. The next shipped change to it would then be reported as a
/// conflict the user never caused.
fn adopt_newly_tracked_pack_files(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    plans: &[PackPlan],
) -> Result<(), String> {
    let untracked = untracked_paths(git, config_dir)?;
    if untracked.is_empty() {
        return Ok(());
    }

    for plan in plans.iter().filter(|plan| plan.syncing()) {
        let claimable: Vec<String> = pack_owned_paths(fs, config_dir, plan)?
            .into_iter()
            // A package item is a directory, and `ls-files` reports files, so a
            // directory counts as untracked when anything under it is.
            .filter(|path| {
                untracked.contains(path)
                    || untracked
                        .iter()
                        .any(|entry| entry.starts_with(&format!("{path}/")))
            })
            .collect();
        if claimable.is_empty() {
            continue;
        }
        log::info!(
            "Claiming newly tracked pack `{}` files as pack-owned: {}",
            plan.manifest.id,
            claimable.join(", ")
        );
        stage_and_commit(
            git,
            config_dir,
            &pack_commit_subject(&plan.manifest.id),
            &claimable,
        )?;
    }

    Ok(())
}

/// Workspace-relative paths git currently considers untracked and not ignored.
/// A git failure yields an empty set, which degrades to today's behavior rather
/// than claiming files whose state could not be established.
fn untracked_paths(
    git: &dyn GitClient,
    config_dir: &Path,
) -> Result<std::collections::BTreeSet<String>, String> {
    let output = git.run(
        config_dir,
        vec![
            "ls-files".to_string(),
            "--others".to_string(),
            "--exclude-standard".to_string(),
        ],
    )?;
    if !output.success {
        return Ok(std::collections::BTreeSet::new());
    }
    Ok(output
        .stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

/// Whether a tracked managed file has been edited by the user since it was last
/// shipped. True when the most recent commit touching `rel_path` carries a
/// subject the sync did not author. An untracked file (no commit history) or an
/// unreadable log is treated as user-owned so local work is never lost.
pub(crate) fn pack_file_is_user_owned(
    git: &dyn GitClient,
    config_dir: &Path,
    rel_path: &str,
) -> Result<bool, String> {
    let output = git.run(
        config_dir,
        vec![
            "log".to_string(),
            "-1".to_string(),
            "--format=%s".to_string(),
            "--".to_string(),
            rel_path.to_string(),
        ],
    )?;
    if !output.success {
        return Ok(true);
    }
    let subject = output.stdout.trim();
    if subject.is_empty() {
        return Ok(true);
    }
    Ok(!is_pack_authored_subject(subject))
}

/// The manifest filename that marks a directory-package for `dir_name`, or
/// `None` for a flat (single-file) resource dir. A package dir is copied whole
/// when missing and never updated in place, so an edited copy is preserved.
fn package_manifest_file(dir_name: &str) -> Option<&'static str> {
    match dir_name {
        "skills" => Some("SKILL.md"),
        "workflows" => Some("workflow.yaml"),
        _ => None,
    }
}

fn pack_file_matches_dir(dir_name: &str, path: &Path) -> bool {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match dir_name {
        "agents" | "responses" => ext == "md",
        "recipes" => ext == "yaml" || ext == "yml",
        _ => false,
    }
}

fn marker_matches(fs: &dyn FileSystem, marker_path: &Path, hash: &str) -> bool {
    fs.exists(marker_path)
        && fs
            .read_to_string(marker_path)
            .map(|value| value.trim() == hash)
            .unwrap_or(false)
}

/// Subdirectory of `CAIRN_HOME` (and of the app's bundled resources) holding the
/// Cairn-owned workflow runtime: `runtime/node_modules/@cairn/{harness,sdk}`.
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

fn snapshot_pending_user_edits(git: &dyn GitClient, config_dir: &Path) -> Result<(), String> {
    if !git.status(config_dir)?.trim().is_empty() {
        git.add_all(config_dir)?;
        git.commit(config_dir, "Snapshot workspace config")?;
    }
    Ok(())
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

    fn last_commit_subject(repo: &Path, rel_path: &str) -> String {
        RealGitClient
            .run(
                repo,
                vec![
                    "log".into(),
                    "-1".into(),
                    "--format=%s".into(),
                    "--".into(),
                    rel_path.into(),
                ],
            )
            .unwrap()
            .stdout
            .trim()
            .to_string()
    }

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

    fn commit_count(repo: &Path) -> usize {
        RealGitClient
            .run(
                repo,
                vec!["rev-list".into(), "--count".into(), "HEAD".into()],
            )
            .unwrap()
            .stdout
            .trim()
            .parse()
            .unwrap()
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
        assert!(result.skipped_conflicts.is_empty());
        assert!(repo.join(".git").exists());

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

        let result =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();

        assert!(
            result.skipped_conflicts.is_empty(),
            "an upgrade must produce no conflict noise: {:?}",
            result.skipped_conflicts
        );

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

        // `responses/` was outside the pre-pack allowlist, so this upgrade is
        // the first time git can see it. It must land pack-owned: snapshotted
        // as the user's instead, it would freeze against every future update
        // and report the next shipped change as a conflict nobody caused.
        let subject = last_commit_subject(&repo, "responses/conveyor.md");
        assert!(
            is_pack_authored_subject(&subject),
            "a newly allowlisted file must be claimed as pack-owned, got {subject:?}"
        );
        assert!(!pack_file_is_user_owned(&RealGitClient, &repo, "responses/conveyor.md").unwrap());
        assert!(
            !repo.join(LEGACY_BUNDLE_SYNC_MARKER).exists(),
            "the superseded global marker is removed once every lock is written"
        );

        // Idempotent: a second pass changes nothing and adds no commit.
        let commits = commit_count(&repo);
        let again =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert!(!again.updated);
        assert_eq!(commit_count(&repo), commits);
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
        git.add_all(&repo).unwrap();
        git.commit(&repo, "My matlab tweaks").unwrap();

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
        assert!(is_pack_authored_subject(&pack_commit_subject("core")));
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
        let commits = commit_count(&repo);

        let again =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert_eq!(again, PackSyncResult::default());
        assert_eq!(commit_count(&repo), commits);
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
        git.add_all(&repo).unwrap();
        git.commit(&repo, "User edits explore").unwrap();

        let result = sync_workspace_packs(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        // The user-edited file is preserved and surfaced as a skipped conflict
        // even though the pack shipped a new version of it.
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert_eq!(
            result.skipped_conflicts,
            vec!["agents/explore.md".to_string()]
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
    fn retired_pack_file_is_removed_unless_the_user_edited_it() {
        let git = RealGitClient;
        let fs = RealFileSystem;
        let retired = "recipes/main-coordinator.yaml";

        // An install that still carries the shipped copy: the sync removes it.
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources-a");
        write_resources_a(&resources);
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();

        let stale = repo.join(retired);
        write(&stale, "shipped copy\n");
        git.add_all(&repo).unwrap();
        git.commit(&repo, "Sync bundled workspace defaults")
            .unwrap();

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(
            !stale.exists(),
            "an untouched shipped copy of a retired resource is removed"
        );

        // An install whose copy the user edited: the sync keeps it and reports
        // the conflict rather than deleting their work.
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources-a");
        write_resources_a(&resources);
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();

        let edited = repo.join(retired);
        write(&edited, "my own version\n");
        git.add_all(&repo).unwrap();
        git.commit(&repo, "Keep my coordinator").unwrap();

        let result = sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(edited.exists(), "a user-owned copy survives retirement");
        assert!(result.skipped_conflicts.contains(&retired.to_string()));
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
        assert!(result.skipped_conflicts.is_empty());
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
        git.add_all(&repo).unwrap();
        git.commit(&repo, "User edits explore").unwrap();

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
    fn missing_git_still_provisions_all_defaults_and_recovers_later() {
        use crate::services::testing::MockGitClient;
        use mockall::predicate::eq;

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);

        let mut unavailable_git = MockGitClient::new();
        unavailable_git
            .expect_is_repo()
            .with(eq(repo.clone()))
            .times(1)
            .returning(|_| Err("Failed to run git: No such file or directory".to_string()));

        let error = sync_workspace_packs(
            &unavailable_git,
            &RealFileSystem,
            &resources,
            &repo,
            "1.0.0",
        )
        .unwrap_err();
        assert!(error.contains("No such file or directory"));
        assert!(repo.join("agents/explore.md").exists());
        assert!(repo.join("recipes/default.yaml").exists());
        assert!(repo.join("skills/example/SKILL.md").exists());
        assert!(repo.join("workflows/example/workflow.yaml").exists());
        assert!(pack_lock::read_lock(&repo, "core").is_none());
        assert!(!repo.join(".git").exists());

        // A user edit made while Git is unavailable survives recovery,
        // including when a previous recovery stopped after repository init.
        std::fs::write(repo.join("agents/explore.md"), "user edit\n").unwrap();

        let git = RealGitClient;
        git.init_repo(&repo, DEFAULT_BRANCH).unwrap();
        assert!(git.root_commit(&repo, DEFAULT_BRANCH).is_err());

        let recovered =
            sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(recovered.updated);
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert!(repo.join(".git").exists());
        assert!(pack_lock::read_lock(&repo, "core").is_some());
        assert!(git.root_commit(&repo, DEFAULT_BRANCH).is_ok());
        let tracked = git.run(&repo, vec!["ls-files".to_string()]).unwrap();
        for path in [
            "agents/explore.md",
            "recipes/default.yaml",
            "responses/conveyor.md",
            "skills/example/SKILL.md",
            "workflows/example/workflow.yaml",
            "packs/core/pack.yaml",
        ] {
            assert!(
                tracked.stdout.lines().any(|tracked| tracked == path),
                "{path} must be tracked, or its ownership can never be established"
            );
        }

        let idempotent =
            sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(!idempotent.updated);

        std::fs::write(
            source_pack(&resources, "core").join("agents/explore.md"),
            "pack upgrade\n",
        )
        .unwrap();
        let upgraded =
            sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert_eq!(
            upgraded.skipped_conflicts,
            vec!["agents/explore.md".to_string()]
        );
    }

    #[test]
    fn restoring_defaults_does_not_consume_an_unrelated_staged_change() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = RealGitClient;
        sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        std::fs::write(repo.join("AGENTS.md"), "staged user change\n").unwrap();
        git.run(
            &repo,
            vec!["add".to_string(), "--".to_string(), "AGENTS.md".to_string()],
        )
        .unwrap();
        std::fs::remove_file(repo.join("agents/explore.md")).unwrap();

        let restored =
            sync_workspace_packs(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(restored.updated);
        let staged = git
            .run(
                &repo,
                vec![
                    "diff".to_string(),
                    "--cached".to_string(),
                    "--name-only".to_string(),
                ],
            )
            .unwrap();
        assert_eq!(staged.stdout.trim(), "AGENTS.md");
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
        git.add_all(&repo).unwrap();
        git.commit(&repo, "User edits fan-out").unwrap();
        sync_workspace_packs(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("workflows/fan-out/main.ts")).unwrap(),
            "// user edit\n",
            "a user's edited workflow copy must never be clobbered by a re-sync"
        );
    }
}
