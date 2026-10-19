//! Bounding the ref store a project's jj store shares with its Git checkout.
//!
//! jj-lib protects every commit it writes with a `refs/jj/keep/<commit>` ref in
//! the backing Git repository, and records every operation as a full view
//! snapshot in its operation log. Neither is ever pruned unless someone runs a
//! garbage collection, so both grow with every publication, rebase, and import
//! for the life of the project. The keep refs land in the same `packed-refs` the
//! operator's checkout, every check ref, and every export contend for, and that
//! file's size is what each rewrite of it holds the lock across.
//!
//! This module is the store half of the maintenance pass that bounds them. The
//! orchestrator decides when it runs, under which locks, and what it records;
//! everything here takes paths and returns counts.
//!
//! The pass mirrors `jj util gc` from the pinned jj 0.42 except for its last
//! step. jj finishes with a bare `git gc --prune=<keep_newer>`, which also
//! expires reflogs and prunes worktree records in what is here the operator's
//! own working checkout, and which repacks the whole object database into one
//! pack (28 minutes on the Cairn project). The object database is maintained by
//! two `git repack` steps instead:
//!
//! - [`compact_object_store`] rolls small packs up geometrically and writes a
//!   multi-pack index. It never deletes an object, so it runs on its own cadence
//!   whenever git's own thresholds trip.
//! - [`reclaim_unreachable_objects`] is the only step that drops objects: a cruft
//!   repack, which drops packed unreferenced objects older than the window, then
//!   `git prune`, which drops loose ones. Geometric compaction packs every loose
//!   object, referenced or not, and `git prune` only ever looks at loose
//!   objects, so without the cruft repack packed garbage would never be
//!   reclaimed at all.
//!
//! An unreferenced object's age is its loose file's mtime, or, once packed, the
//! mtime of the pack holding it; a cruft pack then records that age per object
//! and carries it forward. Compaction writes fresh packs, so it restamps every
//! unreferenced object it rolls up. Retention is therefore at least the window
//! from an object's last repack outside a cruft pack, not from when it was
//! written: garbage that compaction packed survives the next reclaim (into a
//! cruft pack, which compaction leaves alone) and is dropped by the one after.
//!
//! Neither runs `reflog expire` or prunes worktrees, and both walk from every
//! ref, reflog, and index, so `refs/stash`, every `refs/heads/*`, and their
//! reflogs keep everything they reach regardless of age. Commits older than the
//! window that nothing references any more (rewritten or abandoned history)
//! stop resolving by design. The reclaim must run after
//! [`collect_store_garbage`] has recreated the keep refs, so that every hidden
//! commit inside the window is reachable when it walks.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt as _;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::op_walk;
use jj_lib::operation::Operation;
use jj_lib::repo::{ReadonlyRepo, Repo as _};
use jj_lib::stacked_table::TableStore;

use super::packed_refs::{with_packed_refs_patience, PackedRefs};

const NO_GC_REF_PREFIX: &str = "refs/jj/keep/";

/// The commit-id length jj's git backend keys its extra-metadata table by.
const COMMIT_ID_LENGTH: usize = 20;

/// Namespaces counted separately in a census, as paths under `refs/`. A ref in
/// none of them is counted as `other`.
pub const REF_NAMESPACES: [&str; 7] = [
    "jj/keep",
    "heads",
    "remotes",
    "tags",
    "cairn/checks",
    "cairn/stranded",
    "cairn/rescued",
];

/// The census namespace a full ref name belongs to.
pub fn ref_namespace(refname: &str) -> &'static str {
    let rest = refname.strip_prefix("refs/").unwrap_or(refname);
    REF_NAMESPACES
        .into_iter()
        .find(|namespace| {
            rest.strip_prefix(namespace)
                .is_some_and(|tail| tail.starts_with('/'))
        })
        .unwrap_or("other")
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NamespaceRefs {
    pub packed: u64,
    pub loose: u64,
}

/// The size of a repository's ref store, taken without any lock.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefStoreCensus {
    pub packed_bytes: u64,
    pub packed_entries: u64,
    pub loose_refs: u64,
    /// Leftover `<ref>.lock` files under `refs/`. Each one blocks every update
    /// of its ref, and a ref transaction naming it fails whole.
    pub ref_locks: u64,
    pub namespaces: BTreeMap<String, NamespaceRefs>,
}

/// The Git directory whose `packed-refs` a jj store's commits contend for: the
/// one [`PackedRefs::JjStore`] waits on.
pub fn store_git_dir(store: &Path) -> Option<PathBuf> {
    super::packed_refs::git_common_dir(&super::export::resolve_backing_checkout(store)?)
}

/// Count a repository's refs by namespace.
///
/// Reads `packed-refs` as a stream and walks the loose ref directories. It never
/// takes `packed-refs.lock` or spawns git, so it is safe to run as often as a
/// measurement wants, beside any writer.
pub fn census_ref_store(git_dir: &Path) -> Result<RefStoreCensus, String> {
    let mut census = RefStoreCensus::default();
    let packed = git_dir.join("packed-refs");
    match std::fs::File::open(&packed) {
        Ok(file) => {
            census.packed_bytes = file
                .metadata()
                .map_err(|error| format!("inspect {}: {error}", packed.display()))?
                .len();
            for line in BufReader::new(file).split(b'\n') {
                let line = line.map_err(|error| format!("read {}: {error}", packed.display()))?;
                if line
                    .first()
                    .is_none_or(|first| *first == b'#' || *first == b'^')
                {
                    continue;
                }
                let Some(space) = line.iter().position(|byte| *byte == b' ') else {
                    continue;
                };
                let name = String::from_utf8_lossy(&line[space + 1..]);
                census.packed_entries += 1;
                census
                    .namespaces
                    .entry(ref_namespace(&name).to_string())
                    .or_default()
                    .packed += 1;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("open {}: {error}", packed.display())),
    }
    count_loose_refs(&git_dir.join("refs"), "refs", &mut census)?;
    Ok(census)
}

fn count_loose_refs(dir: &Path, prefix: &str, census: &mut RefStoreCensus) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("list {}: {error}", dir.display())),
    };
    for entry in entries {
        let entry = entry.map_err(|error| format!("list {}: {error}", dir.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // Finder litters `.DS_Store` here; git never names a ref with a dot.
        if name.starts_with('.') {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|error| format!("inspect {}: {error}", entry.path().display()))?;
        let refname = format!("{prefix}/{name}");
        if file_type.is_dir() {
            count_loose_refs(&entry.path(), &refname, census)?;
        } else if name.ends_with(".lock") {
            census.ref_locks += 1;
        } else {
            census.loose_refs += 1;
            census
                .namespaces
                .entry(ref_namespace(&refname).to_string())
                .or_default()
                .loose += 1;
        }
    }
    Ok(())
}

/// The shape of a repository's object database, taken without any lock and
/// without spawning git.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectStoreCensus {
    /// `.pack` files under `objects/pack`, kept and cruft packs included.
    pub packs: u64,
    /// Packs with a `.keep` beside them. Cairn's own are the delta pins a
    /// publication holds until its tree is committed into jj; no repack rewrites
    /// or deletes a kept pack.
    pub kept_packs: u64,
    /// Packs with a `.mtimes` beside them: cruft packs, which hold unreferenced
    /// objects with each object's own modification time.
    pub cruft_packs: u64,
    /// Bytes of the `.pack` files themselves; their indexes are not counted.
    pub pack_bytes: u64,
    /// Loose objects, estimated the way `git gc --auto` estimates them: the
    /// entries of `objects/17` times 256. Listing every loose object directory
    /// of a store holding 668k of them is what this census avoids.
    pub loose_objects: u64,
    /// Whether `objects/pack/multi-pack-index` exists.
    pub multi_pack_index: bool,
}

/// Measure a repository's object database from one listing of `objects/pack`
/// and one of `objects/17`.
pub fn census_object_store(git_dir: &Path) -> Result<ObjectStoreCensus, String> {
    let objects = git_dir.join("objects");
    let pack_dir = objects.join("pack");
    let mut census = ObjectStoreCensus::default();
    let mut names = HashSet::new();
    let mut packs = Vec::new();
    match std::fs::read_dir(&pack_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry =
                    entry.map_err(|error| format!("list {}: {error}", pack_dir.display()))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(stem) = name.strip_suffix(".pack") {
                    // A pack another repack unlinked between the listing and the
                    // stat is simply gone; it is not an error in the census.
                    let bytes = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
                    packs.push((stem.to_string(), bytes));
                }
                names.insert(name);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("list {}: {error}", pack_dir.display())),
    }
    for (stem, bytes) in packs {
        census.packs += 1;
        census.pack_bytes += bytes;
        if names.contains(&format!("{stem}.keep")) {
            census.kept_packs += 1;
        }
        if names.contains(&format!("{stem}.mtimes")) {
            census.cruft_packs += 1;
        }
    }
    census.multi_pack_index = names.contains("multi-pack-index");

    let sample = objects.join("17");
    match std::fs::read_dir(&sample) {
        Ok(entries) => {
            let loose = entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    name.len() == 38 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                .count() as u64;
            census.loose_objects = loose * 256;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("list {}: {error}", sample.display())),
    }
    Ok(census)
}

/// What one repack of the object database did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectStoreCompaction {
    pub before: ObjectStoreCensus,
    pub after: ObjectStoreCensus,
    pub duration: Duration,
}

/// Roll the repository's small packs up into a geometric progression and write
/// a multi-pack index: `git repack --geometric=2 -d -n --write-midx`.
///
/// The large base pack is left alone: a pack is rewritten only when it is less
/// than half the size of the packs below it combined, so the work is
/// proportional to what was written since the last pass, not to the store.
///
/// It never drops an object, which is what lets it run beside every other
/// reader and writer without a lock or a confirmation. Packs it replaces are
/// deleted only after the new pack and index are in place; git and gix (jj)
/// both re-scan the pack directory when an object they look up is missing, and
/// a deleted pack a reader already has mapped stays readable on Unix until it
/// is unmapped. Kept packs are neither rolled up nor deleted (git's default
/// without `--pack-kept-objects`), so a publication's in-flight delta pin is
/// safe; its pack is rolled up on a later pass, once the pin is released.
///
/// `-n` skips `update-server-info`, which would otherwise rewrite `info/refs`
/// naming every ref in the store; bitmaps are off because nothing here serves
/// fetches.
///
/// One consequence callers must know: `--geometric` packs every loose object,
/// whether anything references it or not. Unreferenced objects therefore move
/// into packs before they age out of the retention window, after which
/// `git prune` never finds them; only [`reclaim_unreachable_objects`] drops
/// them.
pub fn compact_object_store(git_dir: &Path) -> Result<ObjectStoreCompaction, String> {
    require_git(
        git_dir,
        (2, 34),
        "geometric compaction with a multi-pack index",
    )?;
    run_repack(
        git_dir,
        &["--geometric=2".to_string()],
        "git repack --geometric",
    )
}

/// Repack the whole object database, dropping unreferenced objects older than
/// `keep_newer`: `git repack --cruft --cruft-expiration=<keep_newer> -d -n
/// --write-midx`, then `git prune --expire=<keep_newer>`, the two steps
/// `git gc --cruft` runs.
///
/// Every object reachable from any ref, any reflog, or any worktree's index goes
/// into the new main pack, so the operator's branches, stash, and reflogs keep
/// everything they reach regardless of age. Unreferenced objects younger than
/// the cutoff go into a cruft pack that records each one's age (see the module
/// doc for what restamps it); older packed ones are dropped. The repack leaves
/// expired unreferenced *loose* objects where they are, so `git prune`, which
/// walks the same refs, reflogs, and index, deletes those. Neither expires a
/// reflog or prunes a worktree, so neither needs configuration to leave them
/// alone. Kept packs are left in place.
///
/// This rewrites the base pack, so it takes as long as a full `git gc`. It is
/// the only step that removes objects, and the caller gates and schedules it
/// accordingly.
pub fn reclaim_unreachable_objects(
    git_dir: &Path,
    keep_newer: SystemTime,
) -> Result<ObjectStoreCompaction, String> {
    require_git(git_dir, (2, 37), "a cruft reclaim of unreferenced objects")?;
    let keep_newer = keep_newer
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut step = run_repack(
        git_dir,
        &[
            "--cruft".to_string(),
            format!("--cruft-expiration=@{keep_newer} +0000"),
        ],
        "git repack --cruft",
    )?;
    let started = Instant::now();
    git_capture(
        git_dir,
        &["prune", &format!("--expire=@{keep_newer} +0000")],
        "git prune",
    )?;
    step.duration += started.elapsed();
    step.after = census_object_store(git_dir)?;
    Ok(step)
}

fn run_repack(
    git_dir: &Path,
    mode: &[String],
    context: &str,
) -> Result<ObjectStoreCompaction, String> {
    let before = census_object_store(git_dir)?;
    let started = Instant::now();
    let output = cairn_platform::env::git()
        .args(["--git-dir=.", "-c", "repack.writeBitmaps=false", "repack"])
        .args(mode)
        .args(["-d", "-n", "--write-midx", "--quiet"])
        .current_dir(git_dir)
        .output()
        .map_err(|error| format!("start {context}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let duration = started.elapsed();
    Ok(ObjectStoreCompaction {
        before,
        after: census_object_store(git_dir)?,
        duration,
    })
}

/// Refuse, naming the version found, when the git these steps spawn is older
/// than the one that introduced the options they pass.
fn require_git(git_dir: &Path, minimum: (u32, u32), what: &str) -> Result<(), String> {
    let reported = git_capture(git_dir, &["version"], "read the git version")?;
    match parse_git_version(&reported) {
        Some(found) if found >= minimum => Ok(()),
        Some((major, minor)) => Err(format!(
            "{what} needs git {}.{} or later, and the git on this machine's PATH is {major}.{minor}",
            minimum.0, minimum.1
        )),
        None => Err(format!(
            "{what}: could not read a version from `git version` (`{}`)",
            reported.trim()
        )),
    }
}

/// `(major, minor)` from `git version 2.39.5 (Apple Git-154)` and its kin.
fn parse_git_version(reported: &str) -> Option<(u32, u32)> {
    let version = reported.trim().strip_prefix("git version ")?;
    let mut parts = version.split(|c: char| !c.is_ascii_digit());
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// How many operations the store's operation log holds, by counting its files.
pub fn operation_count(store: &Path) -> Result<u64, String> {
    let dir = store
        .join(".jj")
        .join("repo")
        .join("op_store")
        .join("operations");
    let entries =
        std::fs::read_dir(&dir).map_err(|error| format!("list {}: {error}", dir.display()))?;
    Ok(entries.filter_map(Result::ok).count() as u64)
}

/// What truncating the operation log did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperationLogTruncation {
    /// Retained operations rewritten onto the root operation, each with a new id.
    pub reparented: u64,
    pub truncate: Duration,
    /// Rebuilding the commit index for the rewritten operation ids, paid here so
    /// the next publication does not.
    pub reindex: Duration,
}

/// Drop every operation that ended before `older_than` from the store's
/// operation log, the in-process equivalent of `jj op abandon ..<op>`.
///
/// Every retained operation is rewritten with a new id, so the caller must hold
/// the store lock: anything that snapshotted an operation id for rollback would
/// otherwise restore to an id that no longer exists.
///
/// The store's own working copy records an operation id too, and jj treats an
/// id it cannot load as a hard error that stale-workspace recovery does not
/// handle. That record is normally stale, because every store write except
/// `jj workspace add` passes `--ignore-working-copy`, so it is first brought to
/// the head operation (the files in the store's checkout are moved to the
/// view's working-copy commit, the direction `jj workspace update-stale`
/// takes), and then remapped to the head's new id. Clamping the cut to the
/// stale record instead would pin the retention boundary wherever the store was
/// last refreshed, so a store only ever written by publications would keep its
/// whole history. Only a view that names no working-copy commit for the store
/// falls back to that clamp. Any other workspace registered in the store records
/// an id the rewrite cannot reach, so the truncation refuses while one of them
/// still exists on disk; a registration whose directory is gone is a leftover
/// nothing will load again. Nothing the rewrite writes is visible until the
/// operation heads move, and that is the last step.
///
/// How many operations became unreachable is not counted here (counting means
/// loading every one of them); the garbage collection that deletes them reports
/// it.
pub fn truncate_operation_log(
    store: &Path,
    older_than: SystemTime,
) -> Result<OperationLogTruncation, String> {
    block_on(truncate_operation_log_inner(store, older_than))
}

async fn truncate_operation_log_inner(
    store: &Path,
    older_than: SystemTime,
) -> Result<OperationLogTruncation, String> {
    let started = Instant::now();
    let mut workspace = crate::load_workspace(store)?;
    let loader = workspace.repo_loader().clone();
    let op_store = loader.op_store().clone();
    // Loading at head merges divergent operation heads, so the walk below
    // starts from exactly one.
    let repo = loader
        .load_at_head()
        .await
        .map_err(|error| format!("load the store at its operation head: {error}"))?;
    let head = repo.operation().clone();
    let mut wc_op_id = workspace.working_copy().operation_id().clone();
    refuse_live_foreign_workspaces(&workspace, &repo)?;

    let mut cutoff_ms = millis_since_epoch(older_than);
    if wc_op_id != *head.id() {
        let own = workspace.workspace_name().to_owned();
        match repo.view().get_wc_commit_id(&own).cloned() {
            Some(wc_commit_id) => {
                let wc_commit = repo
                    .store()
                    .get_commit(&wc_commit_id)
                    .map_err(|error| format!("read the store's working-copy commit: {error}"))?;
                workspace
                    .check_out(head.id().clone(), None, &wc_commit)
                    .await
                    .map_err(|error| {
                        format!("bring the store's working copy to the head operation: {error}")
                    })?;
                wc_op_id = head.id().clone();
            }
            None => {
                let wc_op = loader
                    .load_operation(&wc_op_id)
                    .await
                    .map_err(|error| format!("load the store working copy's operation: {error}"))?;
                cutoff_ms = cutoff_ms.min(wc_op.metadata().time.end.timestamp.0);
            }
        }
    }

    let mut newest_old: Option<Operation> = None;
    {
        let mut walk = std::pin::pin!(op_walk::walk_ancestors(std::slice::from_ref(&head)));
        while let Some(op) = walk
            .try_next()
            .await
            .map_err(|error| format!("walk the operation log: {error}"))?
        {
            if op.id() == op_store.root_operation_id() {
                break;
            }
            if op.metadata().time.end.timestamp.0 < cutoff_ms {
                newest_old = Some(op);
                break;
            }
        }
    }
    let Some(newest_old) = newest_old else {
        return Ok(OperationLogTruncation {
            truncate: started.elapsed(),
            ..Default::default()
        });
    };
    if newest_old.id() == head.id() {
        // The store has been idle for the whole window; the head itself cannot
        // be abandoned, and there is nothing older worth the rewrite.
        return Ok(OperationLogTruncation {
            truncate: started.elapsed(),
            ..Default::default()
        });
    }

    let root = loader.root_operation().await;
    let rewritten = reparent_onto(op_store.as_ref(), &newest_old, &head, &root).await?;
    let Some(new_head) = rewritten.ids.get(head.id()).cloned() else {
        return Err("operation-log truncation did not rewrite the head operation".to_string());
    };
    let new_wc_op = if wc_op_id == *head.id() {
        new_head.clone()
    } else {
        rewritten.ids.get(&wc_op_id).cloned().ok_or_else(|| {
            format!(
                "operation-log truncation would abandon the store working copy's operation {}; \
                 nothing was changed",
                wc_op_id.hex()
            )
        })?
    };

    loader
        .op_heads_store()
        .update_op_heads(std::slice::from_ref(head.id()), &new_head)
        .await
        .map_err(|error| format!("move the operation head to the truncated log: {error}"))?;
    let locked = workspace
        .start_working_copy_mutation()
        .await
        .map_err(|error| format!("lock the store working copy: {error}"))?;
    locked
        .finish(new_wc_op)
        .await
        .map_err(|error| format!("remap the store working copy's operation: {error}"))?;
    let truncate = started.elapsed();

    let reindex_started = Instant::now();
    loader
        .load_at_head()
        .await
        .map_err(|error| format!("reindex the truncated store: {error}"))?;
    Ok(OperationLogTruncation {
        reparented: rewritten.ids.len() as u64,
        truncate,
        reindex: reindex_started.elapsed(),
    })
}

/// A workspace other than the store's own whose directory still exists would
/// be left recording an operation id that no longer exists.
fn refuse_live_foreign_workspaces(
    workspace: &jj_lib::workspace::Workspace,
    repo: &ReadonlyRepo,
) -> Result<(), String> {
    use jj_lib::workspace_store::{SimpleWorkspaceStore, WorkspaceStore as _};
    let registry = SimpleWorkspaceStore::load(workspace.repo_path())
        .map_err(|error| format!("read the store's workspace registry: {error}"))?;
    let own = workspace.workspace_name();
    let mut live = Vec::new();
    for name in repo.view().wc_commit_ids().keys() {
        if name.as_str() == own.as_str() {
            continue;
        }
        let Some(path) = registry
            .get_workspace_path(name)
            .map_err(|error| format!("read the store's workspace registry: {error}"))?
        else {
            continue;
        };
        let path = workspace.repo_path().join(path);
        if path.join(".jj").exists() {
            live.push(format!("{} ({})", name.as_str(), path.display()));
        }
    }
    if live.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "operation-log truncation refused: workspaces other than the store's own still \
             exist and would be left recording rewritten operation ids: {}; nothing was changed",
            live.join(", ")
        ))
    }
}

struct Reparented {
    ids: HashMap<OperationId, OperationId>,
}

/// `op_walk::reparent_range` for a single cut, keeping the whole old-to-new id
/// map instead of only the new heads: the store's working copy may record any
/// retained operation, not just the head.
async fn reparent_onto(
    op_store: &dyn jj_lib::op_store::OpStore,
    cut: &Operation,
    head: &Operation,
    dest: &Operation,
) -> Result<Reparented, String> {
    let to_rewrite: Vec<Operation> =
        op_walk::walk_ancestors_range(std::slice::from_ref(head), std::slice::from_ref(cut))
            .try_collect()
            .await
            .map_err(|error| format!("walk the operation log: {error}"))?;
    let mut ids = HashMap::new();
    for old in to_rewrite.into_iter().rev() {
        let mut data = old.store_operation().clone();
        let mut dest_once = Some(dest.id());
        data.parents = data
            .parents
            .iter()
            .filter_map(|id| ids.get(id).or_else(|| dest_once.take()))
            .cloned()
            .collect();
        let new_id = op_store
            .write_operation(&data)
            .await
            .map_err(|error| format!("write a reparented operation: {error}"))?;
        ids.insert(old.id().clone(), new_id);
    }
    Ok(Reparented { ids })
}

/// What a garbage collection removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreGarbageCollection {
    pub operations_before: u64,
    pub operations_after: u64,
    /// Visible and hidden heads the retained operation log still names: the
    /// commits that keep a `refs/jj/keep` ref.
    pub index_heads: u64,
    pub keep_refs_before: u64,
    pub keep_refs_removed: u64,
    pub keep_refs_created: u64,
    /// Keep refs left alone because a stale `<ref>.lock` blocks them.
    pub keep_refs_locked: u64,
    pub op_store_gc: Duration,
    pub ref_phase: Duration,
}

/// Garbage-collect the store, keeping every operation newer than `keep_newer`.
///
/// The first two of jj 0.42's `jj util gc` phases: unreachable operations and
/// views, then the keep refs (recreated for the index heads only) and the
/// commit metadata table. Each protects what was written recently by mtime,
/// which is what lets it run beside other writers without the store lock. The
/// third phase, Git objects, is [`reclaim_unreachable_objects`], which the
/// caller runs after this so every hidden commit inside the window has its
/// keep ref when the repack walks.
///
/// Keep refs get their own, much shorter, grace (`keep_refs_newer`). jj spares a
/// young loose keep ref because the commit it protects may belong to an
/// operation that has not committed yet, and that race lasts seconds. The
/// retention window is already carried by the operation log, whose index heads
/// name every hidden commit inside it, and by the object prune date; sparing
/// keep refs for the whole window measured as 15,920 surplus loose refs on the
/// first pass and would hold a month of them in steady state.
pub fn collect_store_garbage(
    store: &Path,
    keep_newer: SystemTime,
    keep_refs_newer: SystemTime,
) -> Result<StoreGarbageCollection, String> {
    block_on(collect_store_garbage_inner(
        store,
        keep_newer,
        keep_refs_newer,
    ))
}

async fn collect_store_garbage_inner(
    store: &Path,
    keep_newer: SystemTime,
    keep_refs_newer: SystemTime,
) -> Result<StoreGarbageCollection, String> {
    let git_dir = store_git_dir(store)
        .ok_or_else(|| format!("resolve the Git repository behind {}", store.display()))?;
    let mut report = StoreGarbageCollection {
        operations_before: operation_count(store)?,
        ..Default::default()
    };

    let started = Instant::now();
    let repo = load_repo(store).await?;
    repo.op_store()
        .gc(std::slice::from_ref(repo.op_id()), keep_newer)
        .await
        .map_err(|error| format!("collect unreachable operations: {error}"))?;
    report.op_store_gc = started.elapsed();
    report.operations_after = operation_count(store)?;

    let started = Instant::now();
    let root_commit = repo.store().root_commit_id().hex();
    let heads: HashSet<String> = repo
        .index()
        .all_heads_for_gc()
        .map_err(|error| format!("read the index heads: {error}"))?
        .map(|id| id.hex())
        .filter(|id| *id != root_commit)
        .collect();
    report.index_heads = heads.len() as u64;
    recreate_keep_refs(store, &git_dir, &heads, keep_refs_newer, &mut report)?;
    let extras = TableStore::load(
        store.join(".jj").join("repo").join("store").join("extra"),
        COMMIT_ID_LENGTH,
    );
    let extras_head = extras
        .get_head()
        .map_err(|error| format!("read the commit metadata table: {error}"))?;
    extras
        .gc(&extras_head, keep_newer)
        .map_err(|error| format!("collect the commit metadata table: {error}"))?;
    report.ref_phase = started.elapsed();
    Ok(report)
}

/// Keep exactly one `refs/jj/keep` ref per index head, plus any loose keep ref
/// written after `keep_newer` (a commit a concurrent writer is landing right
/// now).
fn recreate_keep_refs(
    store: &Path,
    git_dir: &Path,
    heads: &HashSet<String>,
    keep_newer: SystemTime,
    report: &mut StoreGarbageCollection,
) -> Result<(), String> {
    let listing = git_capture(
        git_dir,
        &[
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            NO_GC_REF_PREFIX,
        ],
        "list keep refs",
    )?;
    let mut covered = HashSet::new();
    let mut retire = Vec::new();
    for line in listing.lines() {
        let Some((oid, name)) = line.split_once(' ') else {
            continue;
        };
        report.keep_refs_before += 1;
        let named_for_target = name.strip_prefix(NO_GC_REF_PREFIX) == Some(oid);
        if named_for_target && heads.contains(oid) {
            covered.insert(oid.to_string());
            continue;
        }
        let loose = git_dir.join(name);
        if loose
            .metadata()
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|modified| modified > keep_newer)
        {
            continue;
        }
        retire.push(RefTarget {
            name: name.to_string(),
            oid: oid.to_string(),
        });
    }
    // Recreate before retiring, so no head is ever without its keep ref.
    let missing: Vec<String> = heads
        .difference(&covered)
        .map(|head| format!("update {NO_GC_REF_PREFIX}{head} {head}\n"))
        .collect();
    for chunk in missing.chunks(REF_TRANSACTION_CHUNK) {
        let transaction = chunk.concat();
        with_packed_refs_patience(PackedRefs::JjStore(store), "recreate keep refs", || {
            update_refs(git_dir, &transaction)
        })?;
        report.keep_refs_created += chunk.len() as u64;
    }
    let retired = retire_refs(PackedRefs::JjStore(store), git_dir, &retire)?;
    report.keep_refs_removed = retired.removed;
    report.keep_refs_locked = retired.locked;
    Ok(())
}

/// How many refs one `update-ref` transaction carries. Every transaction that
/// touches a packed ref holds `packed-refs.lock` until it commits; deleting
/// 192,364 keep refs as one transaction held it for over two minutes, far past
/// the 30 s every other writer waits. A thousand refs commit in under a second.
const REF_TRANSACTION_CHUNK: usize = 1000;

/// A ref and the object it must still name when it is deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefTarget {
    pub name: String,
    pub oid: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetiredRefs {
    pub removed: u64,
    /// Refs left in place because a leftover `<ref>.lock` blocks every update of
    /// them; naming one would fail the whole transaction it rode in.
    pub locked: u64,
}

/// Delete refs in bounded transactions, each waiting out `packed-refs.lock`
/// contention.
///
/// Each deletion names the object the ref must still hold, so a ref some writer
/// moved since it was listed fails its transaction rather than being deleted
/// out from under that writer. Such a failure surfaces; the refs retired in
/// earlier transactions stay retired.
pub fn retire_refs(
    scope: PackedRefs<'_>,
    git_dir: &Path,
    refs: &[RefTarget],
) -> Result<RetiredRefs, String> {
    let mut retired = RetiredRefs::default();
    let mut deletable = Vec::with_capacity(refs.len());
    for target in refs {
        if git_dir.join(format!("{}.lock", target.name)).exists() {
            retired.locked += 1;
        } else {
            deletable.push(format!("delete {} {}\n", target.name, target.oid));
        }
    }
    for chunk in deletable.chunks(REF_TRANSACTION_CHUNK) {
        let transaction = chunk.concat();
        with_packed_refs_patience(scope, "retire refs", || update_refs(git_dir, &transaction))?;
        retired.removed += chunk.len() as u64;
    }
    Ok(retired)
}

/// List refs under `prefix` with the object each names.
pub fn list_refs(git_dir: &Path, prefix: &str) -> Result<Vec<RefTarget>, String> {
    Ok(git_capture(
        git_dir,
        &["for-each-ref", "--format=%(objectname) %(refname)", prefix],
        "list refs",
    )?
    .lines()
    .filter_map(|line| line.split_once(' '))
    .map(|(oid, name)| RefTarget {
        name: name.to_string(),
        oid: oid.to_string(),
    })
    .collect())
}

/// Apply a `git update-ref --stdin` transaction: every line lands or none do.
fn update_refs(git_dir: &Path, transaction: &str) -> Result<(), String> {
    let mut child = cairn_platform::env::git()
        .arg("--git-dir=.")
        .args(["update-ref", "--stdin"])
        .current_dir(git_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("start git update-ref: {error}"))?;
    child
        .stdin
        .take()
        .ok_or_else(|| "git update-ref has no stdin".to_string())?
        .write_all(transaction.as_bytes())
        .map_err(|error| format!("feed git update-ref: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for git update-ref: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git update-ref: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn git_capture(git_dir: &Path, args: &[&str], context: &str) -> Result<String, String> {
    let output = cairn_platform::env::git()
        .arg("--git-dir=.")
        .args(args)
        .current_dir(git_dir)
        .output()
        .map_err(|error| format!("{context}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

async fn load_repo(store: &Path) -> Result<std::sync::Arc<ReadonlyRepo>, String> {
    crate::load_workspace(store)?
        .repo_loader()
        .load_at_head()
        .await
        .map_err(|error| format!("load the store at its operation head: {error}"))
}

fn millis_since_epoch(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// jj-lib futures are not `Send`; each maintenance step runs to completion on
/// its own current-thread runtime, the way the publication transactions do.
fn block_on<T>(future: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("start the store maintenance runtime: {error}"))?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::super::*;
    use jj_lib::repo::Repo as _;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    #[test]
    fn refs_fall_into_their_census_namespace() {
        assert_eq!(ref_namespace("refs/jj/keep/abc"), "jj/keep");
        assert_eq!(ref_namespace("refs/cairn/checks/req"), "cairn/checks");
        assert_eq!(ref_namespace("refs/cairn/rescued/x/1"), "cairn/rescued");
        assert_eq!(ref_namespace("refs/heads/agent/x"), "heads");
        assert_eq!(ref_namespace("refs/headsx/y"), "other");
        assert_eq!(ref_namespace("refs/codex/snapshots/z"), "other");
    }

    #[test]
    fn census_counts_packed_and_loose_refs_without_git() {
        let temp = TempDir::new().unwrap();
        let git_dir = temp.path();
        std::fs::write(
            git_dir.join("packed-refs"),
            "# pack-refs with: peeled fully-peeled sorted \n\
             1111111111111111111111111111111111111111 refs/heads/main\n\
             2222222222222222222222222222222222222222 refs/jj/keep/2222222222222222222222222222222222222222\n\
             3333333333333333333333333333333333333333 refs/tags/v1\n\
             ^4444444444444444444444444444444444444444\n",
        )
        .unwrap();
        let keep = git_dir.join("refs/jj/keep");
        let checks = git_dir.join("refs/cairn/checks");
        std::fs::create_dir_all(&keep).unwrap();
        std::fs::create_dir_all(&checks).unwrap();
        std::fs::write(keep.join("aaaa"), "aaaa\n").unwrap();
        std::fs::write(checks.join("req-1"), "bbbb\n").unwrap();
        std::fs::write(checks.join("req-2.lock"), "").unwrap();
        std::fs::write(git_dir.join("refs/jj/.DS_Store"), "").unwrap();

        let census = census_ref_store(git_dir).unwrap();
        assert_eq!(census.packed_entries, 3, "peeled lines are not refs");
        assert_eq!(census.loose_refs, 2);
        assert_eq!(census.ref_locks, 1);
        assert_eq!(
            census.namespaces["jj/keep"],
            NamespaceRefs {
                packed: 1,
                loose: 1
            }
        );
        assert_eq!(
            census.namespaces["cairn/checks"],
            NamespaceRefs {
                packed: 0,
                loose: 1
            }
        );
        assert_eq!(census.namespaces["heads"].packed, 1);
        assert!(census.packed_bytes > 0);
    }

    #[test]
    fn retiring_refs_skips_a_locked_ref_and_refuses_a_moved_one() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path();
        init_project(repo);
        let head = git_stdout(repo, &["rev-parse", "HEAD"]);
        for name in ["a", "b", "c"] {
            git(
                repo,
                &["update-ref", &format!("refs/cairn/checks/{name}"), &head],
            );
        }
        git(repo, &["pack-refs", "--all"]);
        let git_dir = repo.join(".git");
        // Packing emptied the loose directory; the leftover lock of an
        // interrupted writer is all that remains in it.
        std::fs::create_dir_all(git_dir.join("refs/cairn/checks")).unwrap();
        std::fs::write(git_dir.join("refs/cairn/checks/b.lock"), "").unwrap();
        let listed = list_refs(&git_dir, "refs/cairn/checks/").unwrap();
        assert_eq!(listed.len(), 3);

        let first_two: Vec<RefTarget> = listed
            .iter()
            .filter(|target| !target.name.ends_with("/c"))
            .cloned()
            .collect();
        let retired = retire_refs(PackedRefs::Checkout(repo), &git_dir, &first_two).unwrap();
        assert_eq!(
            retired,
            RetiredRefs {
                removed: 1,
                locked: 1
            }
        );
        let remaining: Vec<String> = list_refs(&git_dir, "refs/cairn/checks/")
            .unwrap()
            .into_iter()
            .map(|target| target.name)
            .collect();
        assert_eq!(remaining, ["refs/cairn/checks/b", "refs/cairn/checks/c"]);

        // A writer moved `c` after it was listed: the stale expectation must
        // refuse rather than delete the writer's new value.
        std::fs::write(repo.join("moved.rs"), "moved\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "moved"]);
        let moved = git_stdout(repo, &["rev-parse", "HEAD"]);
        git(repo, &["update-ref", "refs/cairn/checks/c", &moved]);
        let stale: Vec<RefTarget> = listed
            .into_iter()
            .filter(|target| target.name.ends_with("/c"))
            .collect();
        assert!(retire_refs(PackedRefs::Checkout(repo), &git_dir, &stale).is_err());
        assert_eq!(
            git_stdout(repo, &["rev-parse", "refs/cairn/checks/c"]),
            moved
        );
    }

    struct Fixture {
        _home: TempDir,
        proj: TempDir,
        jj: JjEnv,
        store: PathBuf,
    }

    impl Fixture {
        fn new() -> Option<Self> {
            let bin = jj_bin()?;
            let home = TempDir::new().unwrap();
            let proj = TempDir::new().unwrap();
            init_project(proj.path());
            let jj = JjEnv::resolve(&bin, home.path());
            let store = home.path().join("jj-stores").join("proj");
            ensure_project_store(&jj, &store, proj.path()).unwrap();
            Some(Self {
                _home: home,
                proj,
                jj,
                store,
            })
        }

        fn jj(&self, args: &[&str]) -> String {
            let mut args = args.to_vec();
            args.push("--ignore-working-copy");
            self.jj
                .run(&self.store, &args, "maintenance fixture")
                .unwrap()
        }

        /// A branch whose commits were each described twice, so every rewrite
        /// leaves a hidden predecessor head behind in the operation log.
        fn rewritten_history(&self, branch: &str) {
            let mut parent = "main".to_string();
            for round in 0..3 {
                self.jj(&["new", &parent, "--no-edit", "-m", &format!("draft {round}")]);
                let commit = self.jj(&[
                    "log",
                    "-r",
                    &format!("latest(children({parent}))"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ]);
                self.jj(&["describe", "-r", &commit, "-m", &format!("final {round}")]);
                parent = self.jj(&[
                    "log",
                    "-r",
                    &format!("latest(children({parent}))"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ]);
            }
            create_bookmark_at(&self.jj, &self.store, branch, &parent).unwrap();
        }

        fn git_dir(&self) -> PathBuf {
            store_git_dir(&self.store).expect("the fixture store resolves its git dir")
        }

        fn branch_tips(&self) -> String {
            git_stdout(
                self.proj.path(),
                &[
                    "for-each-ref",
                    "--format=%(refname) %(objectname)",
                    "refs/heads",
                    "refs/stash",
                ],
            )
        }
    }

    /// History before the cut, a marker operation after it, and the store's
    /// working copy at the head: the shape of a live store a day into the pass.
    fn cut_after_rewrites(fx: &Fixture) -> SystemTime {
        fx.rewritten_history("agent/rewritten");
        std::thread::sleep(Duration::from_millis(1100));
        let cutoff = SystemTime::now();
        std::thread::sleep(Duration::from_millis(50));
        fx.jj(&["bookmark", "create", "after-cut", "-r", "main"]);
        cutoff
    }

    /// The end time of the oldest operation the head still reaches, other than
    /// the root.
    fn oldest_retained_end_ms(store: &Path) -> i64 {
        super::block_on(async {
            let repo = super::load_repo(store).await?;
            let root = repo.op_store().root_operation_id().clone();
            let mut oldest = i64::MAX;
            let mut walk = std::pin::pin!(jj_lib::op_walk::walk_ancestors(std::slice::from_ref(
                repo.operation()
            )));
            while let Some(op) = futures::TryStreamExt::try_next(&mut walk)
                .await
                .map_err(|error| error.to_string())?
            {
                if *op.id() != root {
                    oldest = oldest.min(op.metadata().time.end.timestamp.0);
                }
            }
            Ok(oldest)
        })
        .unwrap()
    }

    fn millis(time: SystemTime) -> i64 {
        super::millis_since_epoch(time)
    }

    /// The steady state, not one clean cut: a store written only by ordinary
    /// publications, whose own working copy nothing ever refreshes, must still
    /// drop every operation older than each pass's boundary as the boundary
    /// moves forward.
    #[test]
    #[serial_test::serial(jj)]
    fn repeated_passes_move_the_boundary_with_only_publications_writing() {
        let Some(fx) = Fixture::new() else {
            eprintln!("skipping ref-store steady-state test: jj not resolvable");
            return;
        };
        let branch = "agent/steady-state";
        let mut head = bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
        create_bookmark_at(&fx.jj, &fx.store, branch, &head).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let publish = |round: u32, expected: &str| -> String {
            let published = runtime
                .block_on(publish_logical_head_exported(
                    &fx.jj,
                    &fx.store,
                    branch,
                    expected,
                    ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
                        path: format!("round-{round}.rs"),
                        content: Some(format!("{round}\n").into_bytes()),
                    }]),
                    None,
                    crate::PublicationMode::Child {
                        description: format!("round {round}"),
                    },
                ))
                .unwrap();
            published.export.unwrap();
            published.landed.head
        };
        let mut round = 0;
        let mut publish_rounds = |count: u32, head: &mut String| {
            for _ in 0..count {
                *head = publish(round, head);
                round += 1;
            }
        };

        for _pass in 0..2 {
            publish_rounds(2, &mut head);
            std::thread::sleep(Duration::from_millis(1100));
            let cutoff = SystemTime::now();
            std::thread::sleep(Duration::from_millis(50));
            publish_rounds(2, &mut head);

            let truncation = truncate_operation_log(&fx.store, cutoff).unwrap();
            assert!(truncation.reparented > 0, "each pass cuts something");
            assert!(
                oldest_retained_end_ms(&fx.store) >= millis(cutoff),
                "nothing older than this pass's boundary survives, whatever the store's own \
                 working copy last saw"
            );
            let future = SystemTime::now() + Duration::from_secs(60);
            collect_store_garbage(&fx.store, future, future).unwrap();
            reclaim_unreachable_objects(&fx.git_dir(), future).unwrap();
        }
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
            Some(head.as_str())
        );
        fx.jj
            .run(
                &fx.store,
                &["status"],
                "store working copy after two passes",
            )
            .expect("the store's working copy names an operation that still exists");
    }

    #[test]
    #[serial_test::serial(jj)]
    fn the_pass_bounds_keep_refs_to_index_heads_and_keeps_every_branch() {
        let Some(fx) = Fixture::new() else {
            eprintln!("skipping ref-store maintenance test: jj not resolvable");
            return;
        };
        let cutoff = cut_after_rewrites(&fx);
        let tips = fx.branch_tips();
        let bookmark = bookmark_commit(&fx.jj, &fx.store, "agent/rewritten").unwrap();

        let truncation = truncate_operation_log(&fx.store, cutoff).unwrap();
        assert!(
            truncation.reparented > 0,
            "operations after the cut were rewritten onto the root"
        );
        let future = SystemTime::now() + Duration::from_secs(60);
        let gc = collect_store_garbage(&fx.store, future, future).unwrap();
        assert!(
            gc.operations_after < gc.operations_before,
            "the abandoned operations were collected: {gc:?}"
        );
        assert!(
            gc.keep_refs_removed > 0,
            "pre-cut rewrites lost their keep refs: {gc:?}"
        );
        // Every hidden head kept its keep ref, so a reclaim that drops every
        // unreferenced object, whatever its age, loses nothing jj can name.
        reclaim_unreachable_objects(&fx.git_dir(), future).unwrap();

        let keep = list_refs(&fx.git_dir(), "refs/jj/keep/").unwrap();
        assert_eq!(
            keep.len() as u64,
            gc.index_heads,
            "one keep ref per index head"
        );
        assert!(keep.iter().all(|target| target.name.ends_with(&target.oid)));

        assert_eq!(fx.branch_tips(), tips, "no branch or stash moved");
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, "agent/rewritten").as_deref(),
            Some(bookmark.as_str())
        );
        fx.jj(&["log", "-r", "all()", "--no-graph"]);

        // The truncated store still publishes, and its working copy was
        // remapped rather than left naming a deleted operation.
        let published = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(publish_logical_head_exported(
                &fx.jj,
                &fx.store,
                "agent/rewritten",
                &bookmark,
                ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
                    path: "after-maintenance.rs".into(),
                    content: Some(b"after\n".to_vec()),
                }]),
                None,
                crate::PublicationMode::Child {
                    description: "after maintenance".into(),
                },
            ))
            .expect("a truncated store still publishes");
        published.export.unwrap();
        fx.jj
            .run(
                &fx.store,
                &["status"],
                "store working copy after truncation",
            )
            .expect("the store's working copy names an operation that still exists");
    }

    fn object_exists(repo: &Path, object: &str) -> bool {
        cairn_platform::env::git()
            .args(["cat-file", "-e", object])
            .current_dir(repo)
            .status()
            .unwrap()
            .success()
    }

    fn loose_object_path(git_dir: &Path, object: &str) -> PathBuf {
        git_dir
            .join("objects")
            .join(&object[..2])
            .join(&object[2..])
    }

    const DAY: Duration = Duration::from_secs(24 * 60 * 60);

    /// Write a blob nothing references, its loose file dated `age` ago.
    fn aged_dangling_blob(repo: &Path, git_dir: &Path, content: &str, age: Duration) -> String {
        std::fs::write(repo.join("aged.txt"), content).unwrap();
        let blob = git_stdout(repo, &["hash-object", "-w", "aged.txt"]);
        std::fs::remove_file(repo.join("aged.txt")).unwrap();
        // git writes loose objects read-only; setting an explicit time needs
        // only ownership, not a writable handle.
        std::fs::File::open(loose_object_path(git_dir, &blob))
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
        blob
    }

    fn cruft_packs(git_dir: &Path) -> Vec<String> {
        let mut stems: Vec<String> = std::fs::read_dir(git_dir.join("objects/pack"))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                name.strip_suffix(".mtimes").map(str::to_string)
            })
            .collect();
        stems.sort();
        stems
    }

    /// What an unreferenced object's age is, and so when the reclaim drops it:
    /// an expired loose object goes at once, a young one moves into a cruft
    /// pack, and one compaction already packed was restamped with the new
    /// pack's date, so it outlives the next reclaim. A cruft pack is never
    /// rolled up by compaction, so the age it records carries to the reclaim
    /// after.
    #[test]
    #[serial_test::serial(jj)]
    fn the_reclaim_ages_unreferenced_objects_by_their_loose_file_or_last_pack() {
        let Some(fx) = Fixture::new() else {
            eprintln!("skipping object reclaim age test: jj not resolvable");
            return;
        };
        let repo = fx.proj.path();
        let git_dir = fx.git_dir();
        let window = SystemTime::now() - 30 * DAY;

        let expired_loose = aged_dangling_blob(repo, &git_dir, "expired\n", 60 * DAY);
        let young_loose = aged_dangling_blob(repo, &git_dir, "young\n", DAY);
        reclaim_unreachable_objects(&git_dir, window).unwrap();
        assert!(
            !object_exists(repo, &expired_loose),
            "an expired loose object is dropped, not left loose"
        );
        assert!(object_exists(repo, &young_loose));
        assert!(!loose_object_path(&git_dir, &young_loose).exists());
        let cruft = cruft_packs(&git_dir);
        assert_eq!(cruft.len(), 1, "the young object is in a cruft pack");

        let restamped = aged_dangling_blob(repo, &git_dir, "restamped\n", 60 * DAY);
        for round in 0..4 {
            std::fs::write(repo.join(format!("round-{round}.rs")), format!("{round}\n")).unwrap();
            git(repo, &["add", "-A"]);
            git(repo, &["commit", "-q", "-m", &format!("round {round}")]);
            git(repo, &["repack", "-q", "-d"]);
        }
        compact_object_store(&git_dir).unwrap();
        assert!(!loose_object_path(&git_dir, &restamped).exists());
        assert_eq!(
            cruft_packs(&git_dir),
            cruft,
            "compaction leaves the cruft pack and the ages it records alone"
        );

        reclaim_unreachable_objects(&git_dir, window).unwrap();
        assert!(
            object_exists(repo, &restamped),
            "compaction restamped the object with its new pack's date, so this reclaim keeps it"
        );
        assert!(object_exists(repo, &young_loose));
    }

    /// Every loose object file, counted exactly rather than estimated.
    fn loose_object_files(git_dir: &Path) -> usize {
        std::fs::read_dir(git_dir.join("objects"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.len() == 2 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            .map(|entry| std::fs::read_dir(entry.path()).unwrap().count())
            .sum()
    }

    /// `(stem, objects, kept)` for every pack, the object count read from the
    /// last fanout entry of its version-2 index.
    fn packs(git_dir: &Path) -> Vec<(String, u64, bool)> {
        let dir = git_dir.join("objects/pack");
        let mut packs: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let stem = name.strip_suffix(".pack")?.to_string();
                let index = std::fs::read(dir.join(format!("{stem}.idx"))).unwrap();
                let last_fanout = 8 + 255 * 4;
                let objects =
                    u32::from_be_bytes(index[last_fanout..last_fanout + 4].try_into().unwrap());
                let kept = dir.join(format!("{stem}.keep")).exists();
                Some((stem, u64::from(objects), kept))
            })
            .collect();
        packs.sort_by_key(|(_, objects, _)| *objects);
        packs
    }

    fn all_objects(repo: &Path) -> Vec<String> {
        git_stdout(
            repo,
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname)",
            ],
        )
        .lines()
        .map(str::to_string)
        .collect()
    }

    #[test]
    fn git_versions_parse_with_and_without_a_vendor_suffix() {
        assert_eq!(
            super::parse_git_version("git version 2.52.0\n"),
            Some((2, 52))
        );
        assert_eq!(
            super::parse_git_version("git version 2.39.5 (Apple Git-154)"),
            Some((2, 39))
        );
        assert_eq!(
            super::parse_git_version("git version 2.45.1.windows.1"),
            Some((2, 45))
        );
        assert_eq!(super::parse_git_version("not git"), None);
        assert!((2, 33) < (2, 34) && (3, 0) > (2, 37));
    }

    #[test]
    fn the_object_census_counts_packs_and_estimates_loose_objects_without_git() {
        let temp = TempDir::new().unwrap();
        let git_dir = temp.path();
        let pack = git_dir.join("objects/pack");
        std::fs::create_dir_all(&pack).unwrap();
        for stem in ["pack-a", "pack-b", "cairn-delta-pin-c", "pack-cruft"] {
            std::fs::write(pack.join(format!("{stem}.pack")), [0u8; 100]).unwrap();
            std::fs::write(pack.join(format!("{stem}.idx")), [0u8; 10]).unwrap();
        }
        std::fs::write(pack.join("cairn-delta-pin-c.keep"), "").unwrap();
        std::fs::write(pack.join("pack-cruft.mtimes"), "").unwrap();
        std::fs::write(pack.join("tmp_pack_xyz"), "").unwrap();
        let sample = git_dir.join("objects/17");
        std::fs::create_dir_all(&sample).unwrap();
        std::fs::write(sample.join("a".repeat(38)), "").unwrap();
        std::fs::write(sample.join("b".repeat(38)), "").unwrap();
        std::fs::write(sample.join("tmp_obj_xyz"), "").unwrap();

        let census = census_object_store(git_dir).unwrap();
        assert_eq!(
            census,
            ObjectStoreCensus {
                packs: 4,
                kept_packs: 1,
                cruft_packs: 1,
                pack_bytes: 400,
                loose_objects: 512,
                multi_pack_index: false,
            }
        );
        std::fs::write(pack.join("multi-pack-index"), "").unwrap();
        assert!(census_object_store(git_dir).unwrap().multi_pack_index);
        assert_eq!(
            census_object_store(&temp.path().join("missing")).unwrap(),
            ObjectStoreCensus::default(),
            "a repository with no objects yet is empty, not an error"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn compaction_rolls_small_packs_into_a_geometric_progression_with_a_multi_pack_index() {
        let Some(fx) = Fixture::new() else {
            eprintln!("skipping object-store compaction test: jj not resolvable");
            return;
        };
        let repo = fx.proj.path();
        let git_dir = fx.git_dir();
        for round in 0..10 {
            std::fs::write(repo.join(format!("round-{round}.rs")), format!("{round}\n")).unwrap();
            git(repo, &["add", "-A"]);
            git(repo, &["commit", "-q", "-m", &format!("round {round}")]);
            git(repo, &["repack", "-q", "-d"]);
        }
        // A publication's delta pin, held while its tree is committed into jj.
        let (pinned, _, _) = packs(&git_dir).pop().unwrap();
        let keep = git_dir.join(format!("objects/pack/{pinned}.keep"));
        std::fs::write(&keep, "Cairn validated delta publication\n").unwrap();
        // Loose objects: one commit after the last repack, and a blob nothing
        // references.
        std::fs::write(repo.join("loose.rs"), "loose\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "loose"]);
        std::fs::write(repo.join("dangling.txt"), "dangling\n").unwrap();
        let dangling = git_stdout(repo, &["hash-object", "-w", "dangling.txt"]);
        std::fs::remove_file(repo.join("dangling.txt")).unwrap();
        let objects = all_objects(repo);
        assert!(loose_object_files(&git_dir) > 0);

        let compaction = compact_object_store(&git_dir).unwrap();

        assert_eq!(
            compaction.before.packs, 10,
            "one pack per round: {compaction:?}"
        );
        assert_eq!(compaction.before.kept_packs, 1);
        assert!(!compaction.before.multi_pack_index);
        let after = packs(&git_dir);
        let unkept: Vec<u64> = after
            .iter()
            .filter(|(_, _, kept)| !kept)
            .map(|(_, objects, _)| *objects)
            .collect();
        assert!(
            (unkept.len() as u64) < compaction.before.packs - 1,
            "small packs were rolled up: {after:?}"
        );
        assert!(
            unkept.windows(2).all(|pair| pair[1] >= 2 * pair[0]),
            "the unkept packs form a factor-2 progression: {after:?}"
        );
        assert_eq!(compaction.after.packs, after.len() as u64);
        assert!(compaction.after.multi_pack_index);
        assert!(git_dir.join("objects/pack/multi-pack-index").exists());
        assert!(
            after.iter().any(|(stem, _, kept)| *stem == pinned && *kept),
            "the kept pack and its .keep are untouched"
        );
        assert!(keep.exists());
        assert_eq!(
            loose_object_files(&git_dir),
            0,
            "every loose object was packed"
        );
        assert_eq!(compaction.after.loose_objects, 0);
        assert!(
            object_exists(repo, &dangling),
            "compaction never drops an object, referenced or not"
        );
        let mut check = cairn_platform::env::git()
            .args(["cat-file", "--batch-check"])
            .current_dir(repo)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        std::io::Write::write_all(
            &mut check.stdin.take().unwrap(),
            format!("{}\n", objects.join("\n")).as_bytes(),
        )
        .unwrap();
        let checked = String::from_utf8(check.wait_with_output().unwrap().stdout).unwrap();
        assert_eq!(checked.lines().count(), objects.len());
        assert!(
            !checked.contains("missing"),
            "every object that existed before still resolves:\n{checked}"
        );
        // jj reads the store through gix, which must read the new layout.
        fx.jj(&["log", "-r", "all()", "--no-graph"]);
    }

    /// The backing repository is the operator's own checkout. The reclaim must
    /// leave reflogs, the stash, and every branch exactly as it found them,
    /// keep unreferenced objects inside the window, and drop the older ones,
    /// including those compaction has already moved into a pack.
    #[test]
    #[serial_test::serial(jj)]
    fn reclaim_honours_reflogs_the_stash_and_branches() {
        let Some(fx) = Fixture::new() else {
            eprintln!("skipping object reclaim reflog test: jj not resolvable");
            return;
        };
        let repo = fx.proj.path();
        let git_dir = fx.git_dir();
        // A commit only the reflog still reaches.
        std::fs::write(repo.join("reflogged.rs"), "reflogged\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "reflogged only"]);
        let reflogged = git_stdout(repo, &["rev-parse", "HEAD"]);
        git(repo, &["reset", "-q", "--hard", "HEAD~1"]);
        // A stash.
        std::fs::write(repo.join("shared.rs"), "stashed\n").unwrap();
        git(repo, &["stash", "-q"]);
        let stash = git_stdout(repo, &["rev-parse", "refs/stash"]);
        // An object nothing reaches at all.
        std::fs::write(repo.join("dangling.txt"), "dangling\n").unwrap();
        let dangling = git_stdout(repo, &["hash-object", "-w", "dangling.txt"]);
        std::fs::remove_file(repo.join("dangling.txt")).unwrap();

        let tips = fx.branch_tips();
        let head_reflog = git_stdout(repo, &["reflog", "show", "HEAD"]);
        let main_reflog = git_stdout(repo, &["reflog", "show", "main"]);

        // The catch the reclaim exists for: compaction packs the dangling
        // object, so `git prune` would never see it again.
        compact_object_store(&git_dir).unwrap();
        assert!(object_exists(repo, &dangling));
        assert!(!loose_object_path(&git_dir, &dangling).exists());

        let future = SystemTime::now() + Duration::from_secs(60);
        collect_store_garbage(&fx.store, future, future).unwrap();

        // Inside the window, the unreferenced object survives in a cruft pack.
        let past = SystemTime::now() - DAY;
        let kept = reclaim_unreachable_objects(&git_dir, past).unwrap();
        assert!(object_exists(repo, &dangling), "younger than the cutoff");
        assert!(!loose_object_path(&git_dir, &dangling).exists());
        assert!(kept.after.cruft_packs >= 1, "{kept:?}");
        assert!(kept.after.multi_pack_index);

        // Past the window, it is dropped; everything a ref or reflog reaches
        // is not.
        let reclaimed = reclaim_unreachable_objects(&git_dir, future).unwrap();
        assert!(
            !object_exists(repo, &dangling),
            "the reclaim dropped what nothing reaches: {reclaimed:?}"
        );
        assert!(
            object_exists(repo, &reflogged),
            "a reflogged commit survives"
        );
        assert_eq!(git_stdout(repo, &["rev-parse", "refs/stash"]), stash);
        assert_eq!(fx.branch_tips(), tips, "no branch moved");
        assert_eq!(git_stdout(repo, &["reflog", "show", "HEAD"]), head_reflog);
        assert_eq!(git_stdout(repo, &["reflog", "show", "main"]), main_reflog);
        fx.jj(&["log", "-r", "all()", "--no-graph"]);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn truncation_refuses_while_another_workspace_still_exists() {
        let Some(fx) = Fixture::new() else {
            eprintln!("skipping ref-store workspace refusal test: jj not resolvable");
            return;
        };
        let workspaces = TempDir::new().unwrap();
        let ws = workspaces.path().join("job");
        add_workspace(&fx.jj, &fx.store, &ws, "agent/job", "main", None).unwrap();
        let cutoff = cut_after_rewrites(&fx);
        let operations = operation_count(&fx.store).unwrap();

        let error = truncate_operation_log(&fx.store, cutoff).unwrap_err();
        assert!(error.contains("refused"), "{error}");
        assert!(error.contains("agent"), "names the live workspace: {error}");
        assert_eq!(
            operation_count(&fx.store).unwrap(),
            operations,
            "a refused truncation writes nothing"
        );

        // A registration whose directory is gone is a leftover, not a reader.
        std::fs::remove_dir_all(&ws).unwrap();
        assert!(
            truncate_operation_log(&fx.store, cutoff)
                .unwrap()
                .reparented
                > 0
        );
    }
}
