//! Deterministic extension routing and indexing-root resolution.
//!
//! Phase 1's `config::language_servers::server_for_extension` routed via
//! `HashMap::iter().find`, whose iteration order is nondeterministic when two
//! enabled servers claim the same extension. [`route_extension`] fixes that by
//! choosing a stable winner (lowest language id). [`resolve_root`] walks up
//! from a target file to the nearest ancestor holding a configured root marker,
//! clamped to the worktree boundary.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::language_servers::LanguageServerConfig;
use crate::services::GitClient;

/// Route a file extension to its language server **deterministically**: among
/// enabled entries whose `extensions` contain `ext`, the one with the
/// lexicographically smallest language id wins. Returns `None` when no enabled
/// entry claims the extension.
pub fn route_extension<'a>(
    map: &'a HashMap<String, LanguageServerConfig>,
    ext: &str,
) -> Option<(&'a String, &'a LanguageServerConfig)> {
    let mut matches: Vec<(&'a String, &'a LanguageServerConfig)> = map
        .iter()
        .filter(|(_, cfg)| cfg.enabled && cfg.extensions.iter().any(|e| e == ext))
        .collect();
    matches.sort_by(|a, b| a.0.cmp(b.0));
    matches.into_iter().next()
}

/// Resolve the indexing root for `file`: the nearest ancestor directory (walking
/// up from the file's own directory) that contains one of `root_markers`,
/// clamped to `worktree`. Falls back to `worktree` when no marker is found
/// within it. The walk never escapes the worktree, so a marker living above the
/// worktree boundary is ignored.
pub fn resolve_root(file: &Path, worktree: &Path, root_markers: &[String]) -> PathBuf {
    let start = file.parent().unwrap_or(file);
    let mut cur = Some(start);
    while let Some(dir) = cur {
        // Clamp to the worktree: once we step outside it, stop.
        if !dir.starts_with(worktree) {
            break;
        }
        if root_markers.iter().any(|m| dir.join(m).exists()) {
            return dir.to_path_buf();
        }
        if dir == worktree {
            break;
        }
        cur = dir.parent();
    }
    worktree.to_path_buf()
}

/// Directories never descended into while scanning for root markers: build
/// output, dependency caches, and VCS metadata that can be huge and never hold a
/// meaningful workspace root.
fn is_ignored_scan_dir(name: &str) -> bool {
    matches!(
        name,
        ".git"
            | "node_modules"
            | "target"
            | "dist"
            | "build"
            | "out"
            | "vendor"
            | "coverage"
            | ".next"
            | ".turbo"
            | ".venv"
            | "__pycache__"
    )
}

/// Find the shallowest directories within `worktree` (bounded to `max_depth`
/// levels below it) that contain one of `markers`.
///
/// Fan-out discovery for the read surface: a worktree's primary language often
/// lives in a subdirectory (e.g. `src-tauri/Cargo.toml` in a JS-rooted repo), so
/// checking only the worktree root would miss it entirely. This walks down,
/// skipping build/vendor/VCS dirs and symlinks, and stops descending a branch as
/// soon as it finds a marker — one server rooted at the workspace root is enough,
/// so nested member crates under it are not separately reported. Returns one
/// entry per independent sub-root (usually exactly one).
pub fn find_marker_roots(worktree: &Path, markers: &[String], max_depth: usize) -> Vec<PathBuf> {
    if markers.is_empty() {
        return Vec::new();
    }
    let mut roots = Vec::new();
    let mut stack = vec![(worktree.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        if markers.iter().any(|marker| dir.join(marker).exists()) {
            roots.push(dir);
            // A marker here is the workspace root for this branch; do not descend
            // into nested member roots of the same language.
            continue;
        }
        if depth >= max_depth {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            // `file_type` from `read_dir` does not follow symlinks, so a symlinked
            // directory reports as a symlink and is skipped — no cycle risk.
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            if !is_dir {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if is_ignored_scan_dir(&name) {
                continue;
            }
            stack.push((entry.path(), depth + 1));
        }
    }
    roots
}

/// The git tree SHA naming the content of `relpath` under `rev` in `repo`. A
/// subtree's tree SHA is its content identity: two checkouts whose subroots
/// resolve to the same tree SHA hold byte-identical trees there. `relpath` is
/// repo-relative with forward slashes; an empty `relpath` names the whole repo
/// root (`<rev>^{tree}`). Returns `None` on any git error or empty output, so a
/// failed lookup degrades to "not equivalent" rather than a false match.
pub fn tree_sha(git: &dyn GitClient, repo: &Path, rev: &str, relpath: &str) -> Option<String> {
    let spec = if relpath.is_empty() {
        format!("{rev}^{{tree}}")
    } else {
        format!("{rev}:{relpath}")
    };
    let sha = git.rev_parse(repo, vec![spec]).ok()?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
}

/// Whether the working tree at `relpath` in `repo` is clean — no modified *or*
/// untracked files — per `git status --porcelain -- <relpath>`. An empty
/// `relpath` checks the whole repo. `--porcelain` lists untracked entries
/// (`??`) too, so a clean result means on-disk content equals HEAD for that
/// subroot. Any git error is treated as "not clean" (conservative).
pub fn subroot_clean(git: &dyn GitClient, repo: &Path, relpath: &str) -> bool {
    let mut args = vec!["status".to_string(), "--porcelain".to_string()];
    if !relpath.is_empty() {
        args.push("--".to_string());
        args.push(relpath.to_string());
    }
    match git.run(repo, args) {
        Ok(output) => output.success && output.stdout.trim().is_empty(),
        Err(_) => false,
    }
}

/// Whether a worktree subroot and a main-checkout subroot are interchangeable as
/// an indexing root: both clean on disk and byte-identical at HEAD. All three
/// conditions are required —
///
/// 1. the main subroot is clean (so its HEAD content is what's on disk),
/// 2. the worktree subroot is clean (airtight even without the worktree==HEAD
///    invariant), and
/// 3. the two HEAD tree SHAs are equal (identical source content).
///
/// When this holds, a language server rooted at either side indexes the same
/// tree, so equivalent worktrees can collapse onto the main-checkout instance.
/// The test never produces a false positive: any divergence, dirt, or git error
/// returns `false`, degrading to a per-worktree server.
pub fn subroot_equivalent(
    git: &dyn GitClient,
    worktree: &Path,
    worktree_relpath: &str,
    repo_path: &Path,
    base_relpath: &str,
) -> bool {
    if !subroot_clean(git, repo_path, base_relpath) {
        return false;
    }
    if !subroot_clean(git, worktree, worktree_relpath) {
        return false;
    }
    match (
        tree_sha(git, worktree, "HEAD", worktree_relpath),
        tree_sha(git, repo_path, "HEAD", base_relpath),
    ) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::language_servers::LanguageServerConfig;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn find_marker_roots_discovers_nested_workspace_root() {
        // A JS-rooted repo whose Rust workspace lives one level down, mirroring
        // Cairn (package.json at root, Cargo.toml under src-tauri/).
        let wt = tempdir().unwrap();
        let root = wt.path();
        fs::write(root.join("package.json"), "{}").unwrap();
        let rust = root.join("src-tauri");
        fs::create_dir_all(rust.join("os/cairn-core")).unwrap();
        fs::write(rust.join("Cargo.toml"), "").unwrap();
        // A nested member crate must NOT produce a second root.
        fs::write(rust.join("os/cairn-core/Cargo.toml"), "").unwrap();

        let ts = find_marker_roots(root, &["package.json".to_string()], 4);
        assert_eq!(ts, vec![root.to_path_buf()]);

        let rs = find_marker_roots(root, &["Cargo.toml".to_string()], 4);
        assert_eq!(rs, vec![rust.clone()], "should stop at the workspace root");
    }

    #[test]
    fn find_marker_roots_skips_build_dirs_and_respects_depth() {
        let wt = tempdir().unwrap();
        let root = wt.path();
        // A marker buried inside an ignored build dir must not be found.
        fs::create_dir_all(root.join("target/debug")).unwrap();
        fs::write(root.join("target/debug/Cargo.toml"), "").unwrap();
        assert!(find_marker_roots(root, &["Cargo.toml".to_string()], 4).is_empty());

        // A marker deeper than max_depth is not found.
        let deep = root.join("a/b/c/d");
        fs::create_dir_all(&deep).unwrap();
        fs::write(deep.join("go.mod"), "").unwrap();
        assert!(find_marker_roots(root, &["go.mod".to_string()], 2).is_empty());
        assert_eq!(
            find_marker_roots(root, &["go.mod".to_string()], 4),
            vec![deep]
        );
    }

    fn entry(enabled: bool, exts: &[&str], markers: &[&str]) -> LanguageServerConfig {
        LanguageServerConfig {
            enabled,
            command: vec!["server".to_string()],
            extensions: exts.iter().map(|s| s.to_string()).collect(),
            root_markers: markers.iter().map(|s| s.to_string()).collect(),
            container_separator: "::".to_string(),
            initialization_options: None,
            env: Default::default(),
        }
    }

    #[test]
    fn route_extension_is_deterministic_across_competing_servers() {
        // Two enabled servers both claim "q". The lexicographically smallest
        // language id must win every time, regardless of map iteration order.
        let mut map = HashMap::new();
        map.insert("zeta".to_string(), entry(true, &["q"], &[]));
        map.insert("alpha".to_string(), entry(true, &["q"], &[]));
        for _ in 0..50 {
            assert_eq!(
                route_extension(&map, "q").map(|(id, _)| id.as_str()),
                Some("alpha")
            );
        }
    }

    #[test]
    fn route_extension_skips_disabled_and_unmatched() {
        let mut map = HashMap::new();
        map.insert("alpha".to_string(), entry(false, &["q"], &[]));
        map.insert("beta".to_string(), entry(true, &["r"], &[]));
        assert!(route_extension(&map, "q").is_none());
        assert_eq!(
            route_extension(&map, "r").map(|(id, _)| id.as_str()),
            Some("beta")
        );
    }

    #[test]
    fn resolve_root_picks_nearest_ancestor_marker() {
        let wt = tempdir().unwrap();
        let outer = wt.path();
        let inner = outer.join("crates").join("leaf");
        fs::create_dir_all(inner.join("src")).unwrap();
        // Markers at both the worktree root and the inner crate.
        fs::write(outer.join("Cargo.toml"), "").unwrap();
        fs::write(inner.join("Cargo.toml"), "").unwrap();
        let file = inner.join("src").join("lib.rs");
        fs::write(&file, "").unwrap();

        let markers = vec!["Cargo.toml".to_string()];
        // Nearest ancestor with a marker is the inner crate, not the worktree.
        assert_eq!(resolve_root(&file, outer, &markers), inner);
    }

    #[test]
    fn resolve_root_falls_back_to_worktree_when_no_marker() {
        let wt = tempdir().unwrap();
        let outer = wt.path();
        let file = outer.join("src").join("main.rs");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, "").unwrap();
        let markers = vec!["Cargo.toml".to_string()];
        assert_eq!(resolve_root(&file, outer, &markers), outer);
    }

    #[test]
    fn resolve_root_clamps_to_worktree_boundary() {
        // A marker living ABOVE the worktree must not be selected; resolution
        // clamps at the worktree even when the file's ancestors hold a marker
        // outside it.
        let base = tempdir().unwrap();
        fs::write(base.path().join("Cargo.toml"), "").unwrap(); // above the worktree
        let worktree = base.path().join("wt");
        let file = worktree.join("src").join("main.rs");
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(&file, "").unwrap();
        let markers = vec!["Cargo.toml".to_string()];
        assert_eq!(resolve_root(&file, &worktree, &markers), worktree);
    }

    use crate::services::RealGitClient;

    /// Build a git repo with a `crate/` subroot, one commit, and return the
    /// `RealGitClient` to drive it. Uses the inline-identity `commit`, so no
    /// global git user config is required.
    fn init_repo_with_subroot(root: &Path) -> RealGitClient {
        let git = RealGitClient;
        git.init_repo(root, "main").unwrap();
        fs::create_dir_all(root.join("crate/src")).unwrap();
        fs::write(root.join("crate/Cargo.toml"), "[package]\nname=\"x\"\n").unwrap();
        fs::write(root.join("crate/src/lib.rs"), "pub fn a() {}\n").unwrap();
        git.add_all(root).unwrap();
        git.commit(root, "init").unwrap();
        git
    }

    /// A linked worktree on a fresh branch shares the main repo's object store,
    /// so subtree tree SHAs resolve identically across the two checkouts.
    fn add_worktree(git: &RealGitClient, main: &Path, dest: &Path) {
        git.worktree_add_new_branch(main, dest, "feature", "main")
            .unwrap();
    }

    #[test]
    fn tree_sha_matches_for_identical_subtree_and_diverges_after_edit() {
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo_with_subroot(main);
        let wt_parent = tempdir().unwrap();
        let wt = wt_parent.path().join("wt");
        add_worktree(&git, main, &wt);

        let base = tree_sha(&git, main, "HEAD", "crate").unwrap();
        let mirror = tree_sha(&git, &wt, "HEAD", "crate").unwrap();
        assert_eq!(base, mirror, "identical subtrees share a tree SHA");
        // The whole-repo root form resolves too.
        assert!(tree_sha(&git, main, "HEAD", "").is_some());
        // A nonexistent subroot has no tree SHA.
        assert!(tree_sha(&git, main, "HEAD", "nope").is_none());

        // Commit an edit in the worktree: its subtree SHA diverges from base.
        fs::write(wt.join("crate/src/lib.rs"), "pub fn a() { let _ = 1; }\n").unwrap();
        git.add_all(&wt).unwrap();
        git.commit(&wt, "edit").unwrap();
        assert_ne!(tree_sha(&git, &wt, "HEAD", "crate").unwrap(), base);
    }

    #[test]
    fn subroot_equivalent_true_when_identical_and_both_clean() {
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo_with_subroot(main);
        let wt_parent = tempdir().unwrap();
        let wt = wt_parent.path().join("wt");
        add_worktree(&git, main, &wt);
        assert!(subroot_equivalent(&git, &wt, "crate", main, "crate"));
        // The whole-repo root (empty relpath) is equivalent too.
        assert!(subroot_equivalent(&git, &wt, "", main, ""));
    }

    #[test]
    fn subroot_equivalent_false_when_worktree_subroot_dirty_or_diverged() {
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo_with_subroot(main);
        let wt_parent = tempdir().unwrap();
        let wt = wt_parent.path().join("wt");
        add_worktree(&git, main, &wt);

        // Uncommitted (dirty) edit in the worktree subroot: not equivalent.
        fs::write(wt.join("crate/src/lib.rs"), "pub fn a() { let _ = 2; }\n").unwrap();
        assert!(!subroot_clean(&git, &wt, "crate"));
        assert!(!subroot_equivalent(&git, &wt, "crate", main, "crate"));

        // Commit it: clean again, but the tree SHA now diverges from base.
        git.add_all(&wt).unwrap();
        git.commit(&wt, "edit").unwrap();
        assert!(subroot_clean(&git, &wt, "crate"));
        assert!(!subroot_equivalent(&git, &wt, "crate", main, "crate"));
    }

    #[test]
    fn subroot_equivalent_false_when_base_subroot_dirty() {
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo_with_subroot(main);
        let wt_parent = tempdir().unwrap();
        let wt = wt_parent.path().join("wt");
        add_worktree(&git, main, &wt);

        // Modified-but-uncommitted file in the main checkout subroot.
        fs::write(main.join("crate/src/lib.rs"), "pub fn a() { let _ = 3; }\n").unwrap();
        assert!(!subroot_clean(&git, main, "crate"));
        assert!(!subroot_equivalent(&git, &wt, "crate", main, "crate"));

        // Restore, then add an *untracked* file: --porcelain still flags it.
        fs::write(main.join("crate/src/lib.rs"), "pub fn a() {}\n").unwrap();
        assert!(subroot_clean(&git, main, "crate"));
        fs::write(main.join("crate/src/extra.rs"), "// new\n").unwrap();
        assert!(!subroot_clean(&git, main, "crate"));
        assert!(!subroot_equivalent(&git, &wt, "crate", main, "crate"));
    }

    #[test]
    fn subroot_equivalent_false_when_relpath_absent_in_base() {
        let main_dir = tempdir().unwrap();
        let main = main_dir.path();
        let git = init_repo_with_subroot(main);
        let wt_parent = tempdir().unwrap();
        let wt = wt_parent.path().join("wt");
        add_worktree(&git, main, &wt);
        // Commit a subroot that exists only in the worktree.
        fs::create_dir_all(wt.join("only/src")).unwrap();
        fs::write(wt.join("only/Cargo.toml"), "").unwrap();
        fs::write(wt.join("only/src/lib.rs"), "pub fn b() {}\n").unwrap();
        git.add_all(&wt).unwrap();
        git.commit(&wt, "add only").unwrap();
        assert!(!subroot_equivalent(&git, &wt, "only", main, "only"));
    }
}
