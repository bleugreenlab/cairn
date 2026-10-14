//! Configuration file paths and versioned writes.
use std::path::{Path, PathBuf};

fn git_commit_paths_with_identity(
    repo_root: &Path,
    msg: &str,
    paths: &[PathBuf],
) -> Option<String> {
    let mut command = cairn_platform::env::git();
    command.args([
        "-c",
        "user.name=Cairn",
        "-c",
        "user.email=cairn@local.invalid",
        "commit",
        "--only",
        "-m",
        msg,
        "--",
    ]);
    command.args(paths);
    match command.current_dir(repo_root).output() {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            if !combined.contains("nothing to commit") {
                log::warn!(
                    "Failed to commit scoped config paths in {:?}: {}",
                    repo_root,
                    combined.trim()
                );
            }
            return None;
        }
        Err(error) => {
            log::warn!(
                "Failed to commit scoped config paths in {:?}: {error}",
                repo_root
            );
            return None;
        }
    }
    let head = cairn_platform::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_root)
        .output()
        .ok()?;
    head.status
        .success()
        .then(|| String::from_utf8_lossy(&head.stdout).trim().to_string())
}

/// Commit one project config path and require that its containing repository
/// actually received a commit. Routine project-setting writes use this stricter
/// boundary so a successful response cannot leave config bytes uncommitted.
pub fn commit_config_path_required(path: &Path, msg: &str) -> Result<(), String> {
    let Some(root) = path.parent().and_then(git_work_tree_root) else {
        return Ok(());
    };
    let commits = commit_config_paths(&[path.to_path_buf()], msg);
    if commits
        .iter()
        .any(|(committed_root, _)| *committed_root == root)
    {
        std::thread::spawn(move || push_workspace_repo_best_effort(&root));
        Ok(())
    } else {
        Err(format!(
            "failed to commit project config {} after writing it",
            path.display()
        ))
    }
}

// Git reports a canonical root, while callers may reach the checkout through
// a symlink (including macOS /tmp). Resolve only the parent: deleted paths have
// no canonical leaf, and a symlink leaf is the Git entry, not its target. Index
// capture and commits must use the same spelling to preserve staged user work.
pub(crate) fn git_relative_path(path: &Path, root: &Path) -> Option<PathBuf> {
    let parent = path.parent()?.canonicalize().ok()?;
    let resolved = parent.join(path.file_name()?);
    resolved.strip_prefix(root).ok().map(Path::to_path_buf)
}

/// Commit specific config file/directory paths to whichever git work tree
/// contains them, staging ONLY those paths so unrelated dirty state in the
/// repository is left untouched.
///
/// Both the shared `ConfigResource` mutation path (the Settings UI) and the MCP
/// resource mutation handlers funnel their post-write commits through here, for
/// workspace scope (`~/.cairn`, a repo since CAIRN-1468) and project scope (the
/// project checkout) alike. Paths are grouped by their containing repository, so
/// a scope change that moves a config from one repo to another commits each side
/// once.
///
/// Best effort: the file write has already succeeded and is never rolled back,
/// so a git failure is logged rather than surfaced as a mutation error. A path
/// not inside any git work tree (an older install where `~/.cairn` is not a
/// repo) is silently skipped. Returns the (work-tree root, sha) of each commit
/// actually made, so callers can tell which repository committed.
pub fn commit_config_paths(paths: &[PathBuf], msg: &str) -> Vec<(PathBuf, String)> {
    use std::collections::BTreeMap;

    // Explicit commit pathspecs keep unrelated pre-staged user work out of the
    // app-authored commit. New files need intent-to-add so a path-limited commit
    // accepts them without staging their contents in the caller's index.
    let mut repos: BTreeMap<PathBuf, Vec<PathBuf>> = BTreeMap::new();
    for path in paths {
        let Some(parent) = path.parent() else {
            continue;
        };
        if !parent.exists() {
            continue;
        }
        let Some(root) = git_work_tree_root(parent) else {
            continue;
        };
        let Some(relative) = git_relative_path(path, &root) else {
            continue;
        };
        let tracked = cairn_platform::env::git()
            .args(["ls-files", "--error-unmatch", "--"])
            .arg(&relative)
            .current_dir(&root)
            .output()
            .is_ok_and(|output| output.status.success());
        if !tracked && path.exists() {
            let staged = cairn_platform::env::git()
                .args(["add", "--intent-to-add", "--"])
                .arg(&relative)
                .current_dir(&root)
                .output();
            if !staged.is_ok_and(|output| output.status.success()) {
                log::warn!("Failed to mark new config path {:?} intent-to-add", path);
                continue;
            }
        }
        repos.entry(root).or_default().push(relative);
    }

    let mut commits = Vec::new();
    for (root, paths) in repos {
        if let Some(sha) = git_commit_paths_with_identity(&root, msg, &paths) {
            commits.push((root, sha));
        }
    }
    commits
}

/// Commit `paths` (via [`commit_config_paths`]) and, when `push_root` names the
/// workspace repo and that repo just received one of those commits, push it to
/// `origin` in the background.
///
/// This is the single funnel every settings/config writer uses, so no write path
/// can drift back to leaving `settings.yaml` / `config.yaml` dirty. Callers offer
/// the repo whose commit should be pushed as `push_root`, and the committed-repo
/// check above decides whether it actually pushes: the fixed-scope `settings.yaml`
/// writers pass `Some(config_dir)`, the scope-flexible config-resource path passes
/// the workspace repo unconditionally so a workspace->project move still pushes
/// the workspace-side deletion, and the project `config.yaml` writer passes
/// `Some(project_path)` so its commit reaches `origin`. Pushing project config is
/// deliberate: `.cairn/config.yaml` is versioned on the project's shared branch,
/// and a diverged local main (committed-but-unpushed config) breaks issue PR
/// merges until a manual push. A caller with nothing to push may pass `None`.
///
/// The push is fire-and-forget on a detached thread so a slow network round-trip
/// never blocks the UI save, and is skipped entirely when nothing committed.
pub fn commit_and_maybe_push(paths: &[PathBuf], msg: &str, push_root: Option<&Path>) {
    let commits = commit_config_paths(paths, msg);
    if commits.is_empty() {
        return;
    }
    let Some(push_root) = push_root else {
        return;
    };
    // Resolve through git's own toplevel so a symlinked temp dir matches the
    // canonical roots `commit_config_paths` already returns.
    let Some(canonical) = git_work_tree_root(push_root) else {
        return;
    };
    if commits.iter().any(|(root, _)| *root == canonical) {
        std::thread::spawn(move || push_workspace_repo_best_effort(&canonical));
    }
}

/// Push the current branch of `repo_root` to its `origin` remote, best effort.
///
/// Returns silently when `repo_root` has no `origin` (nothing to push), when
/// HEAD is detached, or when the push fails (offline, auth, non-fast-forward) —
/// the local commit is authoritative and the remote is a sync convenience, so a
/// push failure must never surface as an error or roll anything back.
///
/// Always a plain `git push`, **never** a force: a non-fast-forward means the
/// remote advanced elsewhere, and force-pushing would destroy config history.
/// Such divergence is logged and left for manual reconciliation.
///
/// Standalone and synchronous so it is unit-testable directly against a temp
/// repo with a bare origin; production reaches it through the detached thread
/// spawned by [`commit_and_maybe_push`].
pub fn push_workspace_repo_best_effort(repo_root: &Path) {
    // No origin configured -> nothing to push.
    match cairn_platform::env::git()
        .args(["remote", "get-url", "origin"])
        .current_dir(repo_root)
        .output()
    {
        Ok(out) if out.status.success() => {
            if String::from_utf8_lossy(&out.stdout).trim().is_empty() {
                return;
            }
        }
        _ => return,
    }

    // Resolve the current branch; bail on detached HEAD.
    let branch = match cairn_platform::env::git()
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(repo_root)
        .output()
    {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => return,
    };
    if branch.is_empty() || branch == "HEAD" {
        return;
    }

    // Plain push, never force.
    match cairn_platform::env::git()
        .args(["push", "origin", &branch])
        .current_dir(repo_root)
        .output()
    {
        Ok(out) if out.status.success() => {}
        Ok(out) => log::warn!(
            "Best-effort push of workspace config to origin/{branch} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ),
        Err(e) => log::warn!("Best-effort push of workspace config failed: {e}"),
    }
}

/// Resolve the git work-tree root that contains `dir`, or None when it is not
/// inside one (an older install where `~/.cairn` is not a git repo).
pub fn git_work_tree_root(dir: &Path) -> Option<PathBuf> {
    let out = cairn_platform::env::git()
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let root = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!root.is_empty()).then(|| PathBuf::from(root))
}

/// Configuration search roots, in project-then-workspace precedence order.
pub fn config_root_subdirs(
    config_dir: &Path,
    project_path: Option<&Path>,
    subdir: &str,
) -> Vec<(PathBuf, bool)> {
    let mut roots = Vec::new();
    if let Some(project_path) = project_path {
        roots.push((project_path.join(".cairn").join(subdir), true));
    }
    roots.push((config_dir.join(subdir), false));
    roots
}

/// Config file loading outcome, including parse errors for presentation.
#[derive(Debug, Clone)]
pub enum ConfigResult<T> {
    /// Successfully parsed config
    Ok(T),
    /// Failed to parse - shows error in UI
    Err { path: PathBuf, error: String },
}

/// Extract ID from a file path (filename without extension)
pub fn id_from_path(path: &std::path::Path) -> Option<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
}
