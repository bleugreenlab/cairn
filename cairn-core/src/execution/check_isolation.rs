//! Per-check copy-on-write worktree isolation for both project-check cadences.
//!
//! Both the `when:write` cadence ([`crate::execution::checks`]) and the
//! `when:review` turn-end cadence ([`crate::execution::checks_turn_end`]) use this
//! module to run their cache-miss checks concurrently, each in its own COW clone of
//! the sealed worktree, falling back to sequential in-place execution in the shared
//! checkout when a cheap clone is unavailable ([`decide_exec_mode`]). The
//! mutation-capture and fold pieces ([`detect_mutations`] / [`apply_mutations`])
//! are write-cadence-only: the review cadence is a pure verify and discards its
//! clones, so a stray tracked write lands in a disposable clone rather than
//! dirtying `@`.
//!
//! A cache-missing `when:write` check may rewrite tracked files (a formatter, a
//! `--fix` lint). To run the affected checks CONCURRENTLY without a formatter's
//! half-written tree leaking into another check's view, each cache-miss check runs
//! against its own **copy-on-write clone of the sealed worktree** instead of the
//! one shared checkout. Isolation is universal (there is no read-only/mutating
//! declaration in the check schema): a check physically cannot see another
//! check's writes because it never shares their filesystem.
//!
//! Why a COW clone and not a fresh `jj workspace add`: a new workspace
//! materializes only tracked files — no `node_modules`, no seeded
//! `src-tauri/target` — so `bunx vitest` / `cargo` checks would break or rebuild
//! from scratch. An APFS `clonefile` of the worktree directory brings the
//! untracked build caches along at near-zero cost. Stripping `.jj` from the clone
//! removes the one hazard of copying a worktree (a stale workspace pointer into
//! the shared jj store): the clone becomes a plain directory, and check commands
//! never run jj.
//!
//! ## Lifecycle
//!
//! 1. [`prepare_clone`] COW-clones the sealed worktree per cache-miss check and
//!    strips `.jj`. If ANY clone fails (non-APFS volume, clone error, disk full)
//!    the caller cleans up and falls back to sequential in-place execution for the
//!    whole batch — the mode is decided once, up front, never per check.
//! 2. [`baseline_index`] snapshots each clone's stat identity right after the
//!    clone (before its check runs). `clonefile` preserves mtimes, so this equals
//!    the sealed worktree's state.
//! 3. The check runs in its clone, UNCONFINED: the clone is disposable and
//!    isolated, so it needs no sandbox — the isolation replaces the confinement.
//! 4. After ALL checks join, [`detect_mutations`] rescans each clone, and
//!    [`apply_mutations`] copies the check-made changes back into the real
//!    worktree in plan order. The existing `fold_worktree_after_checks` path then
//!    folds them into the sealed commit, exactly as it folds an in-place
//!    formatter's edits today.
//! 5. [`CloneGuard`] best-effort removes the job's whole clone root on every exit
//!    path (success or failure).
//!
//! Mutation capture works from a possibly-dirty baseline too: when a path-scoped
//! seal left pre-existing dirt, the clone carries that dirt and the baseline index
//! records it, so the set-difference still isolates only the check-made changes.
//!
//! ## Determinism note (deliberate)
//!
//! Today's sequential in-place loop hands each check whatever tree the previous
//! check left, in arbitrary plan order. Under isolation every check validates
//! exactly the SEALED tree — one well-defined input — and formatter edits fold in
//! afterward. Formatting is semantics-preserving, so the sealed-tree verdicts
//! carry over to the folded commit; this is strictly more deterministic than the
//! in-place ordering it replaces.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::services::FileSystem;

/// How the affected cache-miss `when:write` checks execute for one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckExecMode {
    /// Each cache-miss check runs concurrently in its own COW clone of the sealed
    /// worktree; check-made mutations are copied back afterward.
    Isolated,
    /// Fallback: all checks run sequentially in the one shared sealed worktree
    /// (used when a cheap COW clone is unavailable). This is the pre-isolation
    /// behavior, preserved verbatim.
    Shared,
}

/// One tracked change a check made in its clone, relative to the clone root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckMutation {
    /// The check wrote (created or modified) this file; carries the clone's bytes.
    Write { rel: PathBuf, bytes: Vec<u8> },
    /// The check deleted this file.
    Delete { rel: PathBuf },
}

/// Stat identity of a file: `(mtime_ns, size)`. Cheap to capture and compare;
/// used only to NARROW the byte-comparison candidates, never as the final word.
type StatIdentity = (u64, u64);

fn stat_identity(meta: &std::fs::Metadata) -> StatIdentity {
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    (mtime, meta.len())
}

/// Best-effort removal of a job's clone root on drop, so a crash or early return
/// never strands COW clones under the config dir.
pub(crate) struct CloneGuard {
    root: PathBuf,
}

impl CloneGuard {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl Drop for CloneGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The per-job clone root under the config dir: `<config_dir>/check-clones/<job_id>`.
/// A sibling of the worktrees root under the same home volume, so `clonefile`
/// (which cannot cross volumes) can reach it from the worktree.
pub(crate) fn clone_root_for_job(config_dir: &Path, job_id: &str) -> PathBuf {
    config_dir.join("check-clones").join(job_id)
}

/// The per-job clone root for the REVIEW (turn-end) cadence:
/// `<config_dir>/turn-check-clones/<job_id>`. A DISTINCT namespace from the write
/// cadence's `check-clones/<job_id>` so a review [`CloneGuard`] can never remove a
/// write-cadence clone — defense-in-depth even though the two cadences never
/// overlap in time.
pub(crate) fn turn_end_clone_root_for_job(config_dir: &Path, job_id: &str) -> PathBuf {
    config_dir.join("turn-check-clones").join(job_id)
}

/// Decide, once and up front, how a batch of cache-miss checks executes, COW-
/// cloning the sealed worktree for each miss when isolation is available. Shared by
/// both cadences so the try-clone-or-fall-back boundary lives in exactly one place.
///
/// - No misses => `(Shared, empty)`: nothing runs, so no clone is needed.
/// - Every miss clones => `(Isolated, clones)`: each miss runs in its own clone,
///   keyed by plan index.
/// - ANY clone fails (non-APFS volume, disk full, cross-volume) => the partial
///   clones are removed and the WHOLE batch falls back to `(Shared, empty)`,
///   running sequentially in the one shared worktree.
///
/// `misses` pairs each cache-miss check's plan index with its name (for the clone
/// directory). The write cadence computes per-clone baselines from the returned map
/// afterward (for its fold); the review cadence discards the clones unchanged
/// (fold-free).
pub(crate) fn decide_exec_mode(
    fs: &dyn FileSystem,
    worktree: &Path,
    clone_root: &Path,
    misses: &[(usize, &str)],
) -> (CheckExecMode, BTreeMap<usize, PathBuf>) {
    if misses.is_empty() {
        return (CheckExecMode::Shared, BTreeMap::new());
    }
    let mut clones: BTreeMap<usize, PathBuf> = BTreeMap::new();
    for &(index, name) in misses {
        match prepare_clone(fs, worktree, clone_root, index, name) {
            Ok(dir) => {
                clones.insert(index, dir);
            }
            Err(e) => {
                log::warn!(
                    "checks: COW clone unavailable ({e}); falling back to sequential \
                     in-place execution for this batch"
                );
                // Discard any partial clones so a half-cloned batch never lingers;
                // the caller's CloneGuard is the backstop for the same root.
                let _ = std::fs::remove_dir_all(clone_root);
                return (CheckExecMode::Shared, BTreeMap::new());
            }
        }
    }
    (CheckExecMode::Isolated, clones)
}

/// COW-clone the sealed worktree for one cache-miss check into
/// `<clone_root>/<index>-<name>`, then strip `.jj` so the clone is a plain
/// directory. Nukes any leftover destination first (idempotent after a crash).
/// Returns the clone path, or `Err` on the first failure (which routes the whole
/// batch to the sequential fallback).
pub(crate) fn prepare_clone(
    fs: &dyn FileSystem,
    worktree: &Path,
    clone_root: &Path,
    index: usize,
    name: &str,
) -> Result<PathBuf, String> {
    let dest = clone_root.join(format!("{index}-{}", sanitize_name(name)));
    // A COW clone requires a non-existent destination; clear any crash leftover.
    if fs.exists(&dest) || fs.is_symlink(&dest) {
        fs.remove_dir_all(&dest)?;
    }
    fs.try_clone_dir_cow(worktree, &dest)?;
    // Strip `.jj`: the clone must be a plain directory, never a jj workspace with
    // a stale pointer into the shared store. A failure here would leave that
    // hazard, so it routes to the fallback like any other clone failure.
    let jj = dest.join(".jj");
    if fs.exists(&jj) {
        fs.remove_dir_all(&jj)?;
    }
    Ok(dest)
}

/// Sanitize a check name into a single safe path segment.
fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Snapshot the stat identity of every non-ignored file in a clone, keyed by
/// clone-relative path. Uses gitignore semantics (via the `ignore` crate) so
/// ignored build trees (`target/`, `node_modules/`) are skipped — the walk is
/// stat-only over roughly the tracked-file count. `require_git(false)` makes
/// `.gitignore` files apply even though a jj worktree has no `.git`.
pub(crate) fn baseline_index(clone: &Path) -> BTreeMap<PathBuf, StatIdentity> {
    let mut map = BTreeMap::new();
    for entry in ignore::WalkBuilder::new(clone)
        .hidden(false)
        .parents(false)
        .git_global(false)
        .git_exclude(false)
        .require_git(false)
        .build()
        .flatten()
    {
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let path = entry.path();
        if let Ok(meta) = path.metadata() {
            if let Ok(rel) = path.strip_prefix(clone) {
                map.insert(rel.to_path_buf(), stat_identity(&meta));
            }
        }
    }
    map
}

/// Detect the tracked changes a check made in its clone, relative to `baseline`.
/// Candidates are paths whose stat identity changed, plus paths added or removed
/// versus the baseline. Modified/added candidates are CONFIRMED by byte-comparing
/// the clone file against the real-worktree counterpart — filtering
/// touch-but-identical rewrites (a formatter that rewrote a file to the same
/// bytes) so they never dirty the fold.
pub(crate) fn detect_mutations(
    clone: &Path,
    baseline: &BTreeMap<PathBuf, StatIdentity>,
    real_worktree: &Path,
) -> Vec<CheckMutation> {
    let current = baseline_index(clone);
    let mut muts = Vec::new();

    for (rel, ident) in &current {
        let changed = match baseline.get(rel) {
            None => true,                    // added
            Some(before) => before != ident, // stat moved
        };
        if !changed {
            continue;
        }
        let clone_bytes = match std::fs::read(clone.join(rel)) {
            Ok(b) => b,
            Err(_) => continue,
        };
        // Confirm against the real worktree: a touch-but-identical rewrite (same
        // bytes as the sealed tree) is not a mutation.
        let real_bytes = std::fs::read(real_worktree.join(rel)).ok();
        if real_bytes.as_deref() == Some(clone_bytes.as_slice()) {
            continue;
        }
        muts.push(CheckMutation::Write {
            rel: rel.clone(),
            bytes: clone_bytes,
        });
    }

    for rel in baseline.keys() {
        if !current.contains_key(rel) {
            muts.push(CheckMutation::Delete { rel: rel.clone() });
        }
    }

    muts
}

/// Copy every check's mutations back into the real worktree, applied
/// SEQUENTIALLY in plan order after all checks have joined. Conflict policy: if
/// two checks wrote the same path with different content, warn (naming both) and
/// let the later-in-plan-order write win — formatters are idempotent and
/// effectively disjoint in practice, and the fold plus the next commit's checks
/// re-validate regardless. Returns the union of touched paths for logging.
pub(crate) fn apply_mutations(
    real_worktree: &Path,
    per_check: &[(String, Vec<CheckMutation>)],
) -> Vec<String> {
    // Records which check last wrote each path (and with what bytes) so a later
    // conflicting write can be named in the warning.
    let mut writers: BTreeMap<PathBuf, (String, Vec<u8>)> = BTreeMap::new();
    let mut touched: BTreeSet<PathBuf> = BTreeSet::new();

    for (name, muts) in per_check {
        for m in muts {
            match m {
                CheckMutation::Write { rel, bytes } => {
                    if let Some((prev, prev_bytes)) = writers.get(rel) {
                        if prev_bytes != bytes {
                            log::warn!(
                                "when:write checks: {name} and {prev} both wrote {rel:?} with \
                                 different content; {name} (later in plan order) wins"
                            );
                        }
                    }
                    let dest = real_worktree.join(rel);
                    if let Some(parent) = dest.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    match std::fs::write(&dest, bytes) {
                        Ok(()) => {
                            writers.insert(rel.clone(), (name.clone(), bytes.clone()));
                            touched.insert(rel.clone());
                        }
                        Err(e) => log::warn!(
                            "when:write checks: failed to copy {name}'s mutation back to \
                             {dest:?}: {e}"
                        ),
                    }
                }
                CheckMutation::Delete { rel } => {
                    let dest = real_worktree.join(rel);
                    let _ = std::fs::remove_file(&dest);
                    writers.remove(rel);
                    touched.insert(rel.clone());
                }
            }
        }
    }

    touched.iter().map(|p| p.display().to_string()).collect()
}

/// Resolve the working directory and sandbox flag for one check by plan index.
/// A miss with a clone (Isolated mode) runs in that clone, UNCONFINED — the clone
/// is a disposable, isolated copy, so there is no shared checkout to protect and
/// the sandbox is unnecessary (declared checks already run unconfined via the
/// check-command exemption). A check with no clone (Shared mode, or the initial
/// hit resolution) runs in the real sealed worktree, sandboxed exactly as before.
pub(crate) fn resolve_check_exec(
    clones: &BTreeMap<usize, PathBuf>,
    index: usize,
    real_worktree: &str,
) -> (String, bool) {
    match clones.get(&index) {
        Some(dir) => (
            dir.to_str().unwrap_or(real_worktree).to_string(),
            false, // unconfined: the clone is the isolation
        ),
        None => (real_worktree.to_string(), true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::RealFileSystem;

    fn write_file(root: &Path, rel: &str, contents: &str) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn prepare_clone_strips_jj_and_copies_content() {
        let tmp = tempfile::tempdir().unwrap();
        let worktree = tmp.path().join("wt");
        write_file(&worktree, "src/lib.rs", "fn main() {}");
        write_file(&worktree, ".jj/repo", "../store/.jj/repo");
        let clone_root = tmp.path().join("clones");

        let fs = RealFileSystem;
        let clone = prepare_clone(&fs, &worktree, &clone_root, 0, "rust-fmt").unwrap();

        assert!(clone.ends_with("0-rust-fmt"));
        assert_eq!(
            std::fs::read_to_string(clone.join("src/lib.rs")).unwrap(),
            "fn main() {}"
        );
        assert!(
            !clone.join(".jj").exists(),
            ".jj must be stripped from the clone"
        );
    }

    #[test]
    fn detect_finds_modified_added_and_deleted_ignoring_gitignored() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        let clone = tmp.path().join("clone");
        // The sealed worktree state, mirrored into a hand-built clone.
        write_file(&real, ".gitignore", "ignored/\n");
        write_file(&real, "keep.rs", "original");
        write_file(&real, "gone.rs", "to be deleted");
        write_file(&clone, ".gitignore", "ignored/\n");
        write_file(&clone, "keep.rs", "original");
        write_file(&clone, "gone.rs", "to be deleted");

        let baseline = baseline_index(&clone);

        // A check reformats keep.rs, adds new.rs, deletes gone.rs, and writes into
        // a gitignored dir (which must be invisible to the scan).
        std::thread::sleep(std::time::Duration::from_millis(10));
        write_file(&clone, "keep.rs", "reformatted");
        write_file(&clone, "new.rs", "brand new");
        std::fs::remove_file(clone.join("gone.rs")).unwrap();
        write_file(&clone, "ignored/build.log", "noise");

        let muts = detect_mutations(&clone, &baseline, &real);

        assert!(muts.contains(&CheckMutation::Write {
            rel: PathBuf::from("keep.rs"),
            bytes: b"reformatted".to_vec()
        }));
        assert!(muts.contains(&CheckMutation::Write {
            rel: PathBuf::from("new.rs"),
            bytes: b"brand new".to_vec()
        }));
        assert!(muts.contains(&CheckMutation::Delete {
            rel: PathBuf::from("gone.rs")
        }));
        assert!(
            !muts.iter().any(
                |m| matches!(m, CheckMutation::Write { rel, .. } if rel.starts_with("ignored"))
            ),
            "writes under a gitignored dir must be invisible: {muts:?}"
        );
    }

    #[test]
    fn touch_but_identical_rewrite_is_not_a_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        let clone = tmp.path().join("clone");
        write_file(&real, "a.rs", "same bytes");
        write_file(&clone, "a.rs", "same bytes");

        let baseline = baseline_index(&clone);
        std::thread::sleep(std::time::Duration::from_millis(10));
        // Rewrite with identical content — the mtime moves but the bytes match the
        // real worktree, so it is not a mutation.
        write_file(&clone, "a.rs", "same bytes");

        let muts = detect_mutations(&clone, &baseline, &real);
        assert!(
            muts.is_empty(),
            "identical rewrite must not be captured: {muts:?}"
        );
    }

    #[test]
    fn apply_lands_bytes_and_deletes_in_the_real_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        write_file(&real, "keep.rs", "old");
        write_file(&real, "gone.rs", "bye");

        let per_check = vec![(
            "rust-fmt".to_string(),
            vec![
                CheckMutation::Write {
                    rel: PathBuf::from("keep.rs"),
                    bytes: b"new".to_vec(),
                },
                CheckMutation::Delete {
                    rel: PathBuf::from("gone.rs"),
                },
            ],
        )];
        let touched = apply_mutations(&real, &per_check);

        assert_eq!(
            std::fs::read_to_string(real.join("keep.rs")).unwrap(),
            "new"
        );
        assert!(!real.join("gone.rs").exists());
        assert!(touched.contains(&"keep.rs".to_string()));
        assert!(touched.contains(&"gone.rs".to_string()));
    }

    #[test]
    fn conflicting_writes_resolve_later_in_plan_order() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        write_file(&real, "x.rs", "seed");

        // Two checks wrote the same path with different content; the later one wins.
        let per_check = vec![
            (
                "first".to_string(),
                vec![CheckMutation::Write {
                    rel: PathBuf::from("x.rs"),
                    bytes: b"from-first".to_vec(),
                }],
            ),
            (
                "second".to_string(),
                vec![CheckMutation::Write {
                    rel: PathBuf::from("x.rs"),
                    bytes: b"from-second".to_vec(),
                }],
            ),
        ];
        apply_mutations(&real, &per_check);
        assert_eq!(
            std::fs::read_to_string(real.join("x.rs")).unwrap(),
            "from-second"
        );
    }

    #[test]
    fn resolve_check_exec_isolates_distinct_cwds_unconfined() {
        let mut clones = BTreeMap::new();
        clones.insert(0usize, PathBuf::from("/clones/0-a"));
        clones.insert(1usize, PathBuf::from("/clones/1-b"));

        let (cwd0, sandbox0) = resolve_check_exec(&clones, 0, "/real/wt");
        let (cwd1, sandbox1) = resolve_check_exec(&clones, 1, "/real/wt");
        assert_ne!(cwd0, cwd1, "each isolated check gets a distinct cwd");
        assert_ne!(cwd0, "/real/wt");
        assert_ne!(cwd1, "/real/wt");
        assert!(
            !sandbox0 && !sandbox1,
            "isolated clone checks run unconfined"
        );

        // Shared mode (no clones) resolves to the real worktree, sandboxed.
        let empty = BTreeMap::new();
        let (cwd, sandbox) = resolve_check_exec(&empty, 0, "/real/wt");
        assert_eq!(cwd, "/real/wt");
        assert!(
            sandbox,
            "shared-mode checks stay sandboxed in the real worktree"
        );
    }

    #[test]
    fn decide_exec_mode_isolated_on_success_shared_on_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let worktree = tmp.path().join("wt");
        write_file(&worktree, "src/lib.rs", "fn main() {}");
        let clone_root = tmp.path().join("clones");
        let fs = RealFileSystem;

        // No misses => Shared, no clones.
        let (mode, clones) = decide_exec_mode(&fs, &worktree, &clone_root, &[]);
        assert_eq!(mode, CheckExecMode::Shared);
        assert!(clones.is_empty());

        // Misses that clone successfully => Isolated, one clone per miss.
        let misses = [(0usize, "rust-fmt"), (1usize, "frontend")];
        let (mode, clones) = decide_exec_mode(&fs, &worktree, &clone_root, &misses);
        assert_eq!(mode, CheckExecMode::Isolated);
        assert_eq!(clones.len(), 2);
        assert!(clones.contains_key(&0) && clones.contains_key(&1));
        for dir in clones.values() {
            assert!(
                dir.join("src/lib.rs").exists(),
                "each clone carries the tree"
            );
        }
    }

    #[test]
    fn decide_exec_mode_falls_back_to_shared_on_clone_failure() {
        let tmp = tempfile::tempdir().unwrap();
        // A non-existent source makes the COW clone fail, routing the whole batch to
        // the sequential in-place fallback.
        let missing = tmp.path().join("does-not-exist");
        let clone_root = tmp.path().join("clones");
        let fs = RealFileSystem;
        let misses = [(0usize, "rust-fmt")];
        let (mode, clones) = decide_exec_mode(&fs, &missing, &clone_root, &misses);
        assert_eq!(mode, CheckExecMode::Shared);
        assert!(clones.is_empty(), "a clone failure yields no clones");
    }
}
