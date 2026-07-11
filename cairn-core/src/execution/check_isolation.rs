//! Per-check copy-on-write worktree isolation for both project-check cadences.
//!
//! The asynchronous `when:review` cadence uses this module to run cache-miss
//! checks concurrently, each in its own copy-on-write clone of the sealed worktree.
//! The synchronous `when:write` cadence deliberately does not use this machinery:
//! it runs sequentially in the existing checkout so formatter/fixer mutations are
//! visible to later checks and can be folded directly into the sealed commit.
//!
//! Why a COW clone and not a fresh `jj workspace add`: a new workspace materializes
//! only tracked files, so checks would lose seeded dependency and build caches. An
//! APFS `clonefile` carries those caches cheaply. Clone preparation strips `.jj` and
//! the copied Cargo target: the clone must be a plain disposable directory, and a
//! potentially live build directory is not a consistent snapshot. JavaScript caches
//! and generated sidecar binaries remain available while sccache safely reuses Rust
//! compilation output.
//!
//! [`decide_exec_mode`] prepares one clone per review cache miss. If any clone is
//! unavailable, it preserves the established review fallback: clean partial clones
//! and run the batch sequentially in the shared checkout. [`CloneGuard`] removes a
//! suite root on completion, failure, timeout, or cancellation; startup cleanup
//! removes abandoned historical roots.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::services::FileSystem;

/// How a cache-miss check batch executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckExecMode {
    /// Each cache-miss review check runs concurrently in its own disposable COW
    /// clone of the sealed worktree.
    Isolated,
    /// All checks run sequentially in one shared sealed worktree. This is canonical
    /// for write checks and the preserved fallback when review clones are unavailable.
    Shared,
}

/// Removal guard for one generated check-suite directory. It never owns the job
/// parent, so overlapping suites for the same job cannot delete one another.
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
        if let Err(error) = std::fs::remove_dir_all(&self.root) {
            if error.kind() != std::io::ErrorKind::NotFound {
                log::warn!(
                    "failed to remove check clone suite {}: {error}",
                    self.root.display()
                );
            }
        }
        if let Some(job_parent) = self.root.parent() {
            if let Err(error) = std::fs::remove_dir(job_parent) {
                if !matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) {
                    log::warn!(
                        "failed to remove empty check clone job parent {}: {error}",
                        job_parent.display()
                    );
                }
            }
        }
    }
}

pub(crate) fn new_suite_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// One review-cadence suite root:
/// `<config_dir>/turn-check-clones/<job_id>/<suite_id>`.
pub(crate) fn turn_end_clone_root_for_suite(
    config_dir: &Path,
    job_id: &str,
    suite_id: &str,
) -> PathBuf {
    config_dir
        .join("turn-check-clones")
        .join(job_id)
        .join(suite_id)
}

/// Remove abandoned suite directories at host startup. Cleanup is restricted to
/// the two dedicated namespaces and uses `symlink_metadata` so an unexpected
/// symlink is unlinked rather than traversed.
pub(crate) fn cleanup_abandoned_clone_roots(config_dir: &Path) {
    for namespace in ["check-clones", "turn-check-clones"] {
        let root = config_dir.join(namespace);
        let Ok(metadata) = std::fs::symlink_metadata(&root) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            if let Err(error) = std::fs::remove_file(&root) {
                log::warn!(
                    "failed to unlink abandoned check clone root {}: {error}",
                    root.display()
                );
            }
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&root) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let result = match std::fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_symlink() => std::fs::remove_file(&path),
                Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(&path),
                Ok(_) => std::fs::remove_file(&path),
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                if error.kind() != std::io::ErrorKind::NotFound {
                    log::warn!(
                        "failed to clean abandoned check clone {}: {error}",
                        path.display()
                    );
                }
            }
        }
        let _ = std::fs::remove_dir(&root);
    }
}

/// Decide, once and up front, how a review batch of cache-miss checks executes,
/// COW-cloning the sealed worktree for each miss when isolation is available. This
/// keeps review's try-clone-or-fall-back boundary in exactly one place.
///
/// - No misses => `(Shared, empty)`: nothing runs, so no clone is needed.
/// - Every miss clones => `(Isolated, clones)`: each miss runs in its own clone,
///   keyed by plan index.
/// - ANY clone fails (non-APFS volume, disk full, cross-volume) => the partial
///   clones are removed and the WHOLE batch falls back to `(Shared, empty)`,
///   running sequentially in the one shared worktree.
///
/// `misses` pairs each cache-miss review check's plan index with its name. Review
/// discards every clone unchanged after the fold-free suite completes.
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
/// `<clone_root>/<index>-<name>`, then sanitize clone-local generated state. Nukes
/// any leftover destination first (idempotent after a crash).
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
    // A disposable review clone must never inherit Cargo target state from a
    // potentially live source checkout. clonefile is not a transactional snapshot,
    // so copied fingerprints and build-script output can disagree. Keep the rest of
    // the COW clone intact; supervised sccache provides safe Rust dependency reuse.
    let cargo_target = dest.join("src-tauri/target");
    if fs.exists(&cargo_target) {
        fs.remove_dir_all(&cargo_target)?;
    }
    Ok(dest)
}

/// Sanitize a check name into a single safe path segment.
fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
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
    fn prepare_clone_sanitizes_generated_state_and_keeps_reusable_content() {
        let tmp = tempfile::tempdir().unwrap();
        let worktree = tmp.path().join("wt");
        write_file(&worktree, "src/lib.rs", "fn main() {}");
        write_file(&worktree, ".jj/repo", "../store/.jj/repo");
        write_file(
            &worktree,
            "src-tauri/target/debug/build/example/out/poisoned.txt",
            "incomplete build output",
        );
        write_file(
            &worktree,
            "node_modules/example/package.json",
            r#"{"name":"example"}"#,
        );
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
        assert!(
            !clone.join("src-tauri/target").exists(),
            "Cargo target state must be stripped from the clone"
        );
        assert_eq!(
            std::fs::read_to_string(clone.join("node_modules/example/package.json")).unwrap(),
            r#"{"name":"example"}"#,
            "non-Cargo dependency caches must remain available"
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
    fn suite_roots_and_guards_are_independently_owned() {
        let tmp = tempfile::tempdir().unwrap();
        let first = turn_end_clone_root_for_suite(tmp.path(), "job", &new_suite_id());
        let second = turn_end_clone_root_for_suite(tmp.path(), "job", &new_suite_id());
        assert_ne!(first, second);
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        drop(CloneGuard::new(first.clone()));
        assert!(!first.exists());
        assert!(
            second.exists(),
            "one suite guard must not remove another suite"
        );
    }

    #[cfg(unix)]
    #[test]
    fn startup_cleanup_does_not_follow_external_symlinks() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let marker = external.path().join("keep");
        std::fs::write(&marker, "safe").unwrap();
        let job_root = tmp.path().join("check-clones").join("job");
        std::fs::create_dir_all(&job_root).unwrap();
        symlink(external.path(), job_root.join("outside")).unwrap();
        cleanup_abandoned_clone_roots(tmp.path());
        assert!(
            marker.exists(),
            "cleanup must never traverse an external symlink"
        );
        assert!(!tmp.path().join("check-clones").exists());
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
