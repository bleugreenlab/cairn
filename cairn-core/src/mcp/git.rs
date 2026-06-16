//! Git helper functions for MCP handlers.

use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};

/// The `file:` URI scheme prefix for worktree and global filesystem targets.
pub const FILE_URI_SCHEME: &str = "file:";

/// Cap on "did you mean" path suggestions surfaced for a missing `file:` target.
const MAX_PATH_SUGGESTIONS: usize = 5;

/// Cap on candidates collected before ranking, bounding work for a basename
/// (e.g. `mod.rs`) that recurs throughout the tree.
const MAX_PATH_CANDIDATES: usize = 64;

/// Safety net so a suggestion walk on a huge tree can't stall an error path.
const SUGGEST_WALK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitAuthor {
    pub name: String,
    pub email: String,
}

impl GitAuthor {
    pub fn new(name: impl Into<String>, email: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            email: email.into(),
        }
    }
}

fn authored_git(author: Option<&GitAuthor>) -> Command {
    let mut cmd = crate::env::git();
    if let Some(author) = author {
        cmd.env("GIT_AUTHOR_NAME", &author.name)
            .env("GIT_AUTHOR_EMAIL", &author.email)
            .env("GIT_COMMITTER_NAME", &author.name)
            .env("GIT_COMMITTER_EMAIL", &author.email);
    }
    cmd
}

fn run_git_output(
    worktree_path: &Path,
    args: &[&str],
    failure_context: &str,
) -> Result<Output, String> {
    crate::env::git()
        .args(args)
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("{failure_context}: {e}"))
}

fn git_stdout(
    worktree_path: &Path,
    args: &[&str],
    failure_context: &str,
    command_name: &str,
) -> Result<String, String> {
    let output = run_git_output(worktree_path, args, failure_context)?;
    if !output.status.success() {
        return Err(format!(
            "{command_name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFileTarget {
    pub uri: String,
    pub relative_path: String,
    pub full_path: PathBuf,
}

/// Classification of a `file:` target under shell-style path rules.
///
/// The discriminator is the shell rule: a leading `/` after the scheme means
/// absolute; anything else is worktree-relative. Bare `file:` is the worktree
/// root. There are no URI-authority / triple-slash semantics.
enum FileTargetKind {
    /// Bare `file:` — the worktree root.
    Root,
    /// Worktree-relative path. May contain `..`; escape enforcement is
    /// per-operation (`read` permits escapes, `write` rejects them).
    Relative(String),
    /// Absolute path (leading `/` after the scheme).
    Absolute(PathBuf),
}

fn invalid_file_target_message(target: &str) -> String {
    format!(
        "Invalid file target '{target}': expected file: (worktree root), file:relative/path, or file:/absolute/path"
    )
}

fn worktree_only_message(target: &str) -> String {
    format!("change is worktree-only; use a relative path like file:src/x (got '{target}')")
}

fn legacy_tilde_message(target: &str) -> String {
    format!(
        "the file:~ worktree convention was removed; use bare file: for the worktree root or file:<relative/path> for a worktree file (got '{target}')"
    )
}

/// Normalize the worktree-relative portion of a `file:` target.
///
/// Drops `.` and empty segments and preserves `..` — escape enforcement is the
/// caller's job, per operation.
fn normalize_relative_components(path: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::ParentDir => parts.push("..".to_string()),
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
        }
    }
    parts.join("/")
}

fn classify_file_target(target: &str) -> Result<FileTargetKind, String> {
    let rest = target
        .strip_prefix(FILE_URI_SCHEME)
        .ok_or_else(|| invalid_file_target_message(target))?;
    if rest.is_empty() {
        return Ok(FileTargetKind::Root);
    }
    if rest.starts_with('/') {
        return Ok(FileTargetKind::Absolute(PathBuf::from(rest)));
    }
    // Hard cut: the old `file:~`/`file:~/...` worktree convention is gone.
    if rest == "~" || rest.starts_with("~/") {
        return Err(legacy_tilde_message(target));
    }
    let normalized = normalize_relative_components(rest);
    if normalized.is_empty() {
        Ok(FileTargetKind::Root)
    } else {
        Ok(FileTargetKind::Relative(normalized))
    }
}

/// Normalize a `file:` target to its canonical URI string.
///
/// Worktree-relative only: rejects absolute targets, since the only caller
/// (`write`) is worktree-jailed.
pub fn normalize_file_uri(target: &str) -> Result<String, String> {
    match classify_file_target(target)? {
        FileTargetKind::Root => Ok(FILE_URI_SCHEME.to_string()),
        FileTargetKind::Relative(rel) => Ok(format!("{FILE_URI_SCHEME}{rel}")),
        FileTargetKind::Absolute(_) => Err(worktree_only_message(target)),
    }
}

/// Resolve a `file:` target to a concrete path.
///
/// `allow_absolute` co-varies with the jail: when true (read), absolute targets
/// are permitted and the worktree-escape check is skipped (global reads,
/// including `..`, are allowed); when false (change), absolute targets are
/// rejected and the resolved path must stay within the worktree root.
fn resolve_file_target_internal(
    worktree_path: &Path,
    target: &str,
    create_missing_dirs: bool,
    require_exists: bool,
    allow_absolute: bool,
) -> Result<ResolvedFileTarget, String> {
    let (uri, relative_path, joined_path) = match classify_file_target(target)? {
        FileTargetKind::Root => (
            FILE_URI_SCHEME.to_string(),
            String::new(),
            worktree_path.to_path_buf(),
        ),
        FileTargetKind::Relative(rel) => {
            let uri = format!("{FILE_URI_SCHEME}{rel}");
            let joined = worktree_path.join(&rel);
            (uri, rel, joined)
        }
        FileTargetKind::Absolute(abs) => {
            if !allow_absolute {
                return Err(worktree_only_message(target));
            }
            let uri = format!("{FILE_URI_SCHEME}{}", abs.display());
            (uri, abs.display().to_string(), abs)
        }
    };

    let worktree_canonical = worktree_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve worktree path: {e}"))?;

    if require_exists && !joined_path.exists() {
        return Err(format!(
            "Entered path does not exist: {uri}{}",
            did_you_mean_block(worktree_path, &uri)
        ));
    }

    let full_path = if joined_path.exists() {
        let canonical = joined_path
            .canonicalize()
            .map_err(|e| format!("Failed to resolve path: {e}"))?;

        if !allow_absolute && !canonical.starts_with(&worktree_canonical) {
            return Err(format!("Path escapes the worktree root: {uri}"));
        }

        canonical
    } else {
        let parent = joined_path
            .parent()
            .ok_or_else(|| "Invalid file path: no parent directory".to_string())?;

        if create_missing_dirs && !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directories: {e}"))?;
        }

        let existing_ancestor = parent
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .ok_or_else(|| "No existing ancestor directory found".to_string())?;

        let canonical_ancestor = existing_ancestor
            .canonicalize()
            .map_err(|e| format!("Failed to resolve ancestor path: {e}"))?;

        if !allow_absolute && !canonical_ancestor.starts_with(&worktree_canonical) {
            return Err(format!("Path escapes the worktree root: {uri}"));
        }

        joined_path
    };

    Ok(ResolvedFileTarget {
        uri,
        relative_path,
        full_path,
    })
}

/// Search the worktree for files sharing the missing target's basename and
/// return up to [`MAX_PATH_SUGGESTIONS`] `file:`-prefixed relative paths,
/// best-effort. Handles the common "right filename, wrong directory" typo by
/// ranking paths that are a suffix of the entered path first, then preferring
/// shallower paths. Walks with the same `.gitignore`-aware walker the glob/grep
/// handlers use, so heavy build dirs (`target/`, `node_modules/`) are skipped.
/// Returns empty on any error, when the basename can't be determined, or when
/// nothing matches.
pub fn suggest_similar_paths(worktree_path: &Path, missing_uri: &str) -> Vec<String> {
    let entered = missing_uri
        .strip_prefix(FILE_URI_SCHEME)
        .unwrap_or(missing_uri);
    let target_name = match Path::new(entered)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => return Vec::new(),
    };

    let walker = ignore::WalkBuilder::new(worktree_path)
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .build();

    let deadline = std::time::Instant::now() + SUGGEST_WALK_TIMEOUT;
    let mut candidates: Vec<String> = Vec::new();
    for entry in walker {
        if std::time::Instant::now() > deadline {
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if entry.file_type().is_none_or(|ft| ft.is_dir()) {
            continue;
        }
        if entry.file_name().to_str() != Some(target_name.as_str()) {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(worktree_path).unwrap_or(path);
        candidates.push(relative.to_string_lossy().replace('\\', "/"));
        if candidates.len() >= MAX_PATH_CANDIDATES {
            break;
        }
    }

    // Rank: a candidate that is a path-suffix of the entered path is the
    // "missing a directory prefix" case and almost always the intended file, so
    // surface those first; then prefer shallower, shorter paths.
    candidates.sort_by_key(|rel| {
        let is_suffix = rel == entered || rel.ends_with(&format!("/{entered}"));
        (!is_suffix, rel.matches('/').count(), rel.len())
    });
    candidates.truncate(MAX_PATH_SUGGESTIONS);
    candidates
        .into_iter()
        .map(|rel| format!("{FILE_URI_SCHEME}{rel}"))
        .collect()
}

/// Format a "Did you mean:" block (leading newline included) for a missing
/// `file:` target, or an empty string when there are no close matches. Callers
/// append it directly to a "does not exist" error message.
pub fn did_you_mean_block(worktree_path: &Path, missing_uri: &str) -> String {
    let suggestions = suggest_similar_paths(worktree_path, missing_uri);
    if suggestions.is_empty() {
        return String::new();
    }
    let mut block = String::from("\nDid you mean:");
    for suggestion in suggestions {
        block.push_str("\n  ");
        block.push_str(&suggestion);
    }
    block
}

/// Result from a git commit operation
#[derive(Debug)]
pub struct CommitResult {
    pub sha: String,
    pub pr_number: Option<i32>,
}

/// Check if a PR exists for the current branch. Returns PR number if found.
pub fn get_pr_for_branch(worktree_path: &Path) -> Option<i32> {
    let output = crate::env::gh()
        .args(["pr", "view", "--json", "number"])
        .current_dir(worktree_path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    json.get("number")?.as_i64().map(|n| n as i32)
}

/// Validate that a file URI stays within the worktree (no path traversal).
pub fn validate_file_path(
    worktree_path: &Path,
    file_uri: &str,
) -> Result<ResolvedFileTarget, String> {
    resolve_file_target_internal(worktree_path, file_uri, true, false, false)
}

/// Validate that a file path stays within the worktree WITHOUT creating directories.
///
/// Like `validate_file_path` but non-mutating: for new files in missing directories,
/// walks up to find the nearest existing ancestor and verifies it's inside the worktree.
/// Used by filechange Phase 1 validation to avoid side effects before all changes are validated.
pub fn validate_file_path_dry(worktree_path: &Path, file_uri: &str) -> Result<PathBuf, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, false, false)
        .map(|target| target.full_path)
}

/// Validate file URI for read operations.
pub fn validate_read_path(
    worktree_path: &Path,
    file_uri: &str,
) -> Result<ResolvedFileTarget, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, true, true)
}

/// True if `full_path` resolves outside `worktree_path`. For a not-yet-existing
/// path the nearest existing ancestor is checked, matching how new-file writes
/// are jailed. Permissive on resolution failure: returns false (no fence) when
/// neither the worktree nor any ancestor can be canonicalized.
///
/// This is the single prefix-comparison the worktree fence uses for reads and
/// writes, so the escape rule lives in one place.
pub fn path_escapes_worktree(worktree_path: &Path, full_path: &Path) -> bool {
    let Ok(worktree_canonical) = worktree_path.canonicalize() else {
        return false;
    };
    let resolved = if full_path.exists() {
        full_path.canonicalize().ok()
    } else {
        full_path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .and_then(|ancestor| ancestor.canonicalize().ok())
    };
    match resolved {
        Some(path) => !path.starts_with(&worktree_canonical),
        None => false,
    }
}

/// Whether `full_path` resolves into any of `dirs` (i.e. equals or is nested
/// under one). Mirrors [`path_escapes_worktree`]'s resolution (canonicalize, or
/// fall back to the nearest existing ancestor) so a not-yet-existing path is
/// judged by where it would live. Used both for the read denylist (gate reads of
/// credential stores/keys) and the write writable-extra set (temp/toolchain
/// writes are in-sandbox).
pub fn path_within_any(full_path: &Path, dirs: &[PathBuf]) -> bool {
    if dirs.is_empty() {
        return false;
    }
    let resolved = if full_path.exists() {
        full_path.canonicalize().ok()
    } else {
        full_path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .and_then(|ancestor| ancestor.canonicalize().ok())
    };
    let Some(resolved) = resolved else {
        return false;
    };
    dirs.iter().any(|dir| {
        let dir_canon = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        resolved.starts_with(&dir_canon)
    })
}

/// Resolve a `file:` read target to an absolute path **without** requiring it to
/// exist. Lets the read denylist gate run before existence validation, so a
/// denylisted path is denied uniformly whether or not it exists (no
/// deny-vs-"does not exist" existence enumeration).
pub fn resolve_file_path_lenient(worktree_path: &Path, target: &str) -> Result<PathBuf, String> {
    resolve_file_target_internal(worktree_path, target, false, false, true).map(|t| t.full_path)
}

/// Normalize a `file:` change target, permitting absolute paths when
/// `allow_escape` (the worktree fence adjudicates the crossing). With
/// `allow_escape` false this is exactly [`normalize_file_uri`] (worktree-only).
pub fn normalize_change_target(target: &str, allow_escape: bool) -> Result<String, String> {
    match classify_file_target(target)? {
        FileTargetKind::Root => Ok(FILE_URI_SCHEME.to_string()),
        FileTargetKind::Relative(rel) => Ok(format!("{FILE_URI_SCHEME}{rel}")),
        FileTargetKind::Absolute(abs) => {
            if allow_escape {
                Ok(format!("{FILE_URI_SCHEME}{}", abs.display()))
            } else {
                Err(worktree_only_message(target))
            }
        }
    }
}

/// Resolve a `file:` change target to a concrete path, permitting absolute and
/// escaping targets when `allow_escape`. Used by `write` so the worktree fence
/// can adjudicate an out-of-worktree write instead of the resolver hard-
/// rejecting it. Never creates directories or requires existence (the caller
/// checks existence per mutation mode).
pub fn resolve_change_target(
    worktree_path: &Path,
    file_uri: &str,
    allow_escape: bool,
) -> Result<PathBuf, String> {
    resolve_file_target_internal(worktree_path, file_uri, false, false, allow_escape)
        .map(|target| target.full_path)
}

pub fn current_commit(worktree_path: &Path) -> Result<String, String> {
    git_stdout(
        worktree_path,
        &["rev-parse", "HEAD"],
        "Failed to run git rev-parse HEAD",
        "git rev-parse HEAD",
    )
}

pub fn current_branch(worktree_path: &Path) -> Result<String, String> {
    git_stdout(
        worktree_path,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        "Failed to run git rev-parse --abbrev-ref HEAD",
        "git rev-parse --abbrev-ref HEAD",
    )
}

fn run_authored_commit(
    worktree_path: &Path,
    args: &[&str],
    author: Option<&GitAuthor>,
    failure_context: &str,
) -> Result<Output, String> {
    authored_git(author)
        .args(args)
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("{failure_context}: {e}"))
}

fn has_previous_commit(worktree_path: &Path) -> Result<bool, String> {
    let output = run_git_output(
        worktree_path,
        &["log", "-1", "--format=%H"],
        "Failed to check git log",
    )?;

    Ok(output.status.success() && !output.stdout.is_empty())
}

fn commit_staged_changes(
    worktree_path: &Path,
    commit_msg: &str,
    author: Option<&GitAuthor>,
) -> Result<(), String> {
    let output = if commit_msg == "^" {
        if has_previous_commit(worktree_path)? {
            run_authored_commit(
                worktree_path,
                &["commit", "--amend", "--no-edit"],
                author,
                "Failed to run git commit --amend",
            )?
        } else {
            run_authored_commit(
                worktree_path,
                &["commit", "-m", "Initial changes"],
                author,
                "Failed to run git commit",
            )?
        }
    } else {
        run_authored_commit(
            worktree_path,
            &["commit", "-m", commit_msg],
            author,
            "Failed to run git commit",
        )?
    };

    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git commit failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn finalize_commit_result(worktree_path: &Path) -> CommitResult {
    let sha = git_stdout(
        worktree_path,
        &["rev-parse", "--short", "HEAD"],
        "Failed to run git rev-parse --short HEAD",
        "git rev-parse --short HEAD",
    )
    .unwrap_or_else(|_| "unknown".to_string());

    // Push to origin (don't fail if push fails - commit succeeded locally)
    push_to_origin(worktree_path);

    // Check if a PR exists for this branch
    let pr_number = get_pr_for_branch(worktree_path);

    CommitResult { sha, pr_number }
}

fn commit_staged_and_finalize(
    worktree_path: &Path,
    commit_msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    commit_staged_changes(worktree_path, commit_msg, author)?;
    Ok(finalize_commit_result(worktree_path))
}

/// Hard-reset the worktree to HEAD and remove untracked files and directories.
///
/// Enforces the worktree==HEAD invariant the session-archival scheme rests on:
/// after an MCP mutation handler fails or declines to commit, the agent
/// worktree must again exactly equal HEAD so the archival git coordinates stay
/// sound. Runs `git reset --hard HEAD` followed by `git clean -fd`.
pub fn restore_worktree_to_head(worktree_path: &Path) -> Result<(), String> {
    let reset = run_git_output(
        worktree_path,
        &["reset", "--hard", "HEAD"],
        "Failed to run git reset --hard HEAD",
    )?;
    if !reset.status.success() {
        return Err(format!(
            "git reset --hard HEAD failed: {}",
            String::from_utf8_lossy(&reset.stderr).trim()
        ));
    }

    let clean = run_git_output(
        worktree_path,
        &["clean", "-fd"],
        "Failed to run git clean -fd",
    )?;
    if !clean.status.success() {
        return Err(format!(
            "git clean -fd failed: {}",
            String::from_utf8_lossy(&clean.stderr).trim()
        ));
    }

    Ok(())
}

/// Whether the repo backing `worktree_path` is mid-merge or mid-rebase.
///
/// Gates the `NO_COMMIT` escape: leaving the worktree dirty is only legitimate
/// while one of these multi-step operations is in flight (so the agent can
/// resolve conflicts across several tool calls). Merge/rebase state files live
/// in the *per-worktree* git dir, not the shared common dir, so for a linked
/// worktree (Cairn's normal case) `--git-dir` is the authoritative location;
/// the common dir is also checked so the main worktree and any edge cases are
/// covered. Marker files: `MERGE_HEAD` (merge), `rebase-merge/` (interactive or
/// merge-based rebase), `rebase-apply/` (am/patch-based rebase).
pub fn is_repo_mid_transition(worktree_path: &Path) -> bool {
    let mut git_dirs: Vec<PathBuf> = Vec::new();
    for flag in ["--git-dir", "--git-common-dir"] {
        if let Ok(dir) = git_stdout(
            worktree_path,
            &["rev-parse", flag],
            "Failed to run git rev-parse",
            "git rev-parse",
        ) {
            // `--git-dir`/`--git-common-dir` may be relative to the worktree.
            let path = Path::new(&dir);
            let resolved = if path.is_absolute() {
                path.to_path_buf()
            } else {
                worktree_path.join(path)
            };
            if !git_dirs.contains(&resolved) {
                git_dirs.push(resolved);
            }
        }
    }

    git_dirs.iter().any(|dir| {
        dir.join("MERGE_HEAD").exists()
            || dir.join("rebase-merge").exists()
            || dir.join("rebase-apply").exists()
    })
}

/// Git add and commit (or amend) all changes in the worktree.
pub fn git_commit_all(
    worktree_path: &Path,
    commit_msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    // Stage all changes
    let add_output = run_git_output(worktree_path, &["add", "-A"], "Failed to run git add")?;

    if !add_output.status.success() {
        let stderr = String::from_utf8_lossy(&add_output.stderr);
        return Err(format!("git add failed: {}", stderr));
    }

    // Check if there are changes to commit
    let status_output = run_git_output(
        worktree_path,
        &["diff", "--cached", "--quiet"],
        "Failed to check git status",
    )?;

    // If exit code is 0, there are no changes
    if status_output.status.success() {
        return Err("nothing to commit, working tree clean".to_string());
    }

    commit_staged_and_finalize(worktree_path, commit_msg, author)
}

/// Git add and commit (or amend) a file.
pub fn git_commit_file(
    worktree_path: &Path,
    file_path: &str,
    commit_msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    // Stage the file
    let add_output = run_git_output(worktree_path, &["add", file_path], "Failed to run git add")?;

    if !add_output.status.success() {
        let stderr = String::from_utf8_lossy(&add_output.stderr);
        return Err(format!("git add failed: {}", stderr));
    }

    commit_staged_and_finalize(worktree_path, commit_msg, author)
}

/// Git add and commit (or amend) multiple files atomically.
pub fn git_commit_files(
    worktree_path: &Path,
    file_paths: &[&str],
    commit_msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    // Stage all specified files
    for file_path in file_paths {
        let add_output =
            run_git_output(worktree_path, &["add", file_path], "Failed to run git add")?;

        if !add_output.status.success() {
            let stderr = String::from_utf8_lossy(&add_output.stderr);
            // git add of deleted files: use git add -u instead
            if stderr.contains("did not match any files") {
                let _ = crate::env::git()
                    .args(["add", "-u", file_path])
                    .current_dir(worktree_path)
                    .output();
            } else {
                return Err(format!("git add failed for {}: {}", file_path, stderr));
            }
        }
    }

    commit_staged_and_finalize(worktree_path, commit_msg, author)
}

/// Return true when the worktree has a configured, non-empty `origin` remote.
pub fn has_remote(worktree_path: &Path) -> bool {
    crate::env::git()
        .args(["remote", "get-url", "origin"])
        .current_dir(worktree_path)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| !String::from_utf8_lossy(&output.stdout).trim().is_empty())
        .unwrap_or(false)
}

/// Push current branch to origin. Logs errors but doesn't fail.
pub fn push_to_origin(worktree_path: &Path) {
    // Get current branch name
    let branch = match git_stdout(
        worktree_path,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        "Failed to determine branch name for push",
        "git rev-parse --abbrev-ref HEAD",
    ) {
        Ok(branch) => branch,
        Err(err) => {
            log::warn!("Could not determine branch name for push: {}", err);
            return;
        }
    };

    // Skip push for detached HEAD or main/master branches
    if branch == "HEAD" || branch == "main" || branch == "master" {
        log::debug!("Skipping push for branch: {}", branch);
        return;
    }

    // Push to origin (force-with-lease handles amended commits safely)
    let push_output = run_git_output(
        worktree_path,
        &["push", "--force-with-lease", "origin", &branch],
        "Failed to run git push",
    );

    match push_output {
        Ok(o) if o.status.success() => {
            log::info!("Pushed to origin/{}", branch);
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            log::warn!("Push failed (commit succeeded locally): {}", stderr);
        }
        Err(err) => {
            log::warn!("Push failed (commit succeeded locally): {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn git_output(worktree: &Path, args: &[&str]) -> Output {
        crate::env::git()
            .args(args)
            .current_dir(worktree)
            .output()
            .unwrap()
    }

    fn init_git_repo_with_user(worktree: &Path, email: &str, name: &str) {
        assert!(git_output(worktree, &["init"]).status.success());
        assert!(git_output(worktree, &["config", "user.email", email])
            .status
            .success());
        assert!(git_output(worktree, &["config", "user.name", name])
            .status
            .success());
    }

    fn porcelain(worktree: &Path) -> String {
        String::from_utf8_lossy(&git_output(worktree, &["status", "--porcelain"]).stdout)
            .to_string()
    }

    fn current_branch(worktree: &Path) -> String {
        String::from_utf8_lossy(
            &git_output(worktree, &["rev-parse", "--abbrev-ref", "HEAD"]).stdout,
        )
        .trim()
        .to_string()
    }

    fn commit_base(worktree: &Path) {
        std::fs::write(worktree.join("f.txt"), "base\n").unwrap();
        assert!(git_output(worktree, &["add", "-A"]).status.success());
        assert!(git_output(worktree, &["commit", "-m", "base"])
            .status
            .success());
    }

    /// Build a repo whose `feature` branch conflicts with the base branch on
    /// `f.txt`, leaving HEAD on the base branch. Returns the base branch name.
    fn setup_conflicting_branches(worktree: &Path) -> String {
        init_git_repo_with_user(worktree, "t@e.com", "Tester");
        commit_base(worktree);
        let base = current_branch(worktree);
        assert!(git_output(worktree, &["checkout", "-b", "feature"])
            .status
            .success());
        std::fs::write(worktree.join("f.txt"), "feature\n").unwrap();
        git_output(worktree, &["add", "-A"]);
        git_output(worktree, &["commit", "-m", "feature"]);
        assert!(git_output(worktree, &["checkout", &base]).status.success());
        std::fs::write(worktree.join("f.txt"), "mainline\n").unwrap();
        git_output(worktree, &["add", "-A"]);
        git_output(worktree, &["commit", "-m", "mainline"]);
        base
    }

    #[test]
    fn restore_worktree_to_head_resets_tracked_and_removes_untracked() {
        let temp = tempdir().unwrap();
        let wt = temp.path();
        init_git_repo_with_user(wt, "t@e.com", "Tester");
        std::fs::write(wt.join("tracked.txt"), "v1").unwrap();
        git_output(wt, &["add", "-A"]);
        git_output(wt, &["commit", "-m", "init"]);

        std::fs::write(wt.join("tracked.txt"), "v2").unwrap();
        std::fs::write(wt.join("untracked.txt"), "new").unwrap();
        assert!(!porcelain(wt).is_empty());

        restore_worktree_to_head(wt).unwrap();

        assert_eq!(
            std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
            "v1"
        );
        assert!(!wt.join("untracked.txt").exists());
        assert!(porcelain(wt).is_empty(), "worktree should equal HEAD");
    }

    #[test]
    fn is_repo_mid_transition_false_for_clean_or_merely_dirty_repo() {
        let temp = tempdir().unwrap();
        let wt = temp.path();
        init_git_repo_with_user(wt, "t@e.com", "Tester");
        commit_base(wt);
        assert!(!is_repo_mid_transition(wt));
        // A dirty-but-not-transitioning worktree is still not a transition.
        std::fs::write(wt.join("f.txt"), "dirty\n").unwrap();
        std::fs::write(wt.join("stray.txt"), "x\n").unwrap();
        assert!(!is_repo_mid_transition(wt));
    }

    #[test]
    fn is_repo_mid_transition_true_during_merge_conflict() {
        let temp = tempdir().unwrap();
        let wt = temp.path();
        setup_conflicting_branches(wt);
        let merge = git_output(wt, &["merge", "feature"]);
        assert!(!merge.status.success(), "merge should conflict");
        assert!(is_repo_mid_transition(wt));
        assert!(git_output(wt, &["merge", "--abort"]).status.success());
        assert!(!is_repo_mid_transition(wt));
    }

    #[test]
    fn is_repo_mid_transition_true_during_rebase_merge_flavor() {
        let temp = tempdir().unwrap();
        let wt = temp.path();
        let base = setup_conflicting_branches(wt);
        assert!(git_output(wt, &["checkout", "feature"]).status.success());
        let rebase = git_output(wt, &["rebase", "--merge", &base]);
        assert!(!rebase.status.success(), "rebase should conflict");
        assert!(is_repo_mid_transition(wt));
        git_output(wt, &["rebase", "--abort"]);
        assert!(!is_repo_mid_transition(wt));
    }

    #[test]
    fn is_repo_mid_transition_true_during_rebase_apply_flavor() {
        let temp = tempdir().unwrap();
        let wt = temp.path();
        let base = setup_conflicting_branches(wt);
        assert!(git_output(wt, &["checkout", "feature"]).status.success());
        let rebase = git_output(wt, &["rebase", "--apply", &base]);
        assert!(!rebase.status.success(), "rebase should conflict");
        assert!(is_repo_mid_transition(wt));
        git_output(wt, &["rebase", "--abort"]);
        assert!(!is_repo_mid_transition(wt));
    }

    #[test]
    fn test_normalize_file_uri_root_and_relative() {
        assert_eq!(normalize_file_uri("file:").unwrap(), "file:");
        assert_eq!(
            normalize_file_uri("file:src/lib/mod.rs").unwrap(),
            "file:src/lib/mod.rs"
        );
        // A leading "./" normalizes away.
        assert_eq!(
            normalize_file_uri("file:./src/lib.rs").unwrap(),
            "file:src/lib.rs"
        );
    }

    #[test]
    fn test_normalize_file_uri_rejects_absolute_and_non_scheme() {
        // change is worktree-only: absolute targets are rejected.
        assert!(normalize_file_uri("file:/tmp/lib.rs").is_err());
        // Bare paths without the scheme are not file targets.
        assert!(normalize_file_uri("src/lib.rs").is_err());
        assert!(normalize_file_uri("/tmp/lib.rs").is_err());
    }

    #[test]
    fn test_legacy_tilde_targets_are_rejected() {
        for legacy in ["file:~", "file:~/", "file:~/src/lib.rs"] {
            let err = normalize_file_uri(legacy).unwrap_err();
            assert!(
                err.contains("file:~ worktree convention was removed"),
                "expected hard-cut message for {legacy}, got: {err}"
            );
        }
        let temp = tempdir().unwrap();
        let err = validate_read_path(temp.path(), "file:~/Cargo.toml").unwrap_err();
        assert!(err.contains("file:~ worktree convention was removed"));
    }

    #[test]
    fn path_escapes_worktree_detects_inside_and_outside() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(worktree.join("src")).unwrap();
        std::fs::write(worktree.join("src/lib.rs"), "x").unwrap();
        std::fs::write(temp.path().join("outside.txt"), "y").unwrap();

        // Existing in-worktree paths (incl. nested) and the root do not escape.
        assert!(!path_escapes_worktree(&worktree, &worktree));
        assert!(!path_escapes_worktree(
            &worktree,
            &worktree.join("src/lib.rs")
        ));
        // Existing paths outside the worktree escape.
        assert!(path_escapes_worktree(
            &worktree,
            &temp.path().join("outside.txt")
        ));
        assert!(path_escapes_worktree(&worktree, Path::new("/etc")));
    }

    #[test]
    fn path_within_any_matches_only_listed_subtrees() {
        let temp = tempdir().unwrap();
        let secrets = temp.path().join("secrets");
        std::fs::create_dir_all(&secrets).unwrap();
        std::fs::write(secrets.join("creds"), "x").unwrap();
        let public = temp.path().join("public.txt");
        std::fs::write(&public, "y").unwrap();

        let dirs = vec![secrets.clone()];
        // A file inside a listed subtree matches.
        assert!(path_within_any(&secrets.join("creds"), &dirs));
        // A file outside any listed subtree does not.
        assert!(!path_within_any(&public, &dirs));
        // A not-yet-existing file under a listed dir still matches (via ancestor).
        assert!(path_within_any(&secrets.join("new/deep"), &dirs));
        // An empty list never matches.
        assert!(!path_within_any(&public, &[]));
    }

    #[test]
    fn path_escapes_worktree_handles_nonexistent_via_ancestor() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(worktree.join("src")).unwrap();

        // A not-yet-existing file resolves via its nearest existing ancestor:
        // an in-worktree ancestor means the new file is in-worktree.
        assert!(!path_escapes_worktree(
            &worktree,
            &worktree.join("src/new.rs")
        ));
        assert!(!path_escapes_worktree(
            &worktree,
            &worktree.join("brand/new/deep.rs")
        ));
        // A not-yet-existing file whose nearest existing ancestor is outside the
        // worktree escapes.
        assert!(path_escapes_worktree(
            &worktree,
            &temp.path().join("sibling/new.rs")
        ));
    }

    #[test]
    fn test_validate_read_path_resolves_root() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let root = validate_read_path(worktree, "file:").unwrap();
        assert_eq!(root.uri, "file:");
        assert_eq!(root.relative_path, "");
        assert_eq!(root.full_path, worktree.canonicalize().unwrap());
    }

    #[test]
    fn test_validate_read_path_resolves_relative() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src/lib")).unwrap();
        std::fs::write(worktree.join("src/lib/mod.rs"), "content").unwrap();

        let result = validate_read_path(worktree, "file:src/lib/mod.rs").unwrap();
        assert_eq!(result.uri, "file:src/lib/mod.rs");
        assert_eq!(result.relative_path, "src/lib/mod.rs");
        assert!(result.full_path.ends_with("src/lib/mod.rs"));
    }

    #[test]
    fn test_validate_read_path_resolves_absolute_global() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        // A global file outside the worktree resolves for read.
        let outside = tempdir().unwrap();
        let global = outside.path().join("global.txt");
        std::fs::write(&global, "global").unwrap();

        let uri = format!("file:{}", global.display());
        let result = validate_read_path(worktree, &uri).unwrap();
        assert_eq!(result.full_path, global.canonicalize().unwrap());
    }

    #[test]
    fn test_validate_read_path_allows_parent_escape() {
        let temp = tempdir().unwrap();
        let worktree = temp.path().join("wt");
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::write(temp.path().join("outside.txt"), "content").unwrap();

        // read permits ".." escapes (global reach).
        let result = validate_read_path(&worktree, "file:../outside.txt").unwrap();
        assert!(result.full_path.ends_with("outside.txt"));
    }

    #[test]
    fn test_validate_read_path_rejects_nonexistent() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_read_path(worktree, "file:missing.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_suggest_similar_paths_finds_prefixed_match() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src-tauri/turso_migrations")).unwrap();
        std::fs::write(
            worktree.join("src-tauri/turso_migrations/0007_add_uri.sql"),
            "sql",
        )
        .unwrap();

        // Entered with the directory prefix missing — the deeper file is the
        // intended one and should be the top suggestion.
        let suggestions = suggest_similar_paths(worktree, "file:turso_migrations/0007_add_uri.sql");
        assert_eq!(
            suggestions,
            vec!["file:src-tauri/turso_migrations/0007_add_uri.sql".to_string()]
        );
    }

    #[test]
    fn test_suggest_similar_paths_ranks_suffix_match_first() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("a/turso_migrations")).unwrap();
        std::fs::create_dir_all(worktree.join("unrelated")).unwrap();
        // A suffix match (right tail, wrong prefix) and an unrelated same-name file.
        std::fs::write(worktree.join("a/turso_migrations/0007.sql"), "x").unwrap();
        std::fs::write(worktree.join("unrelated/0007.sql"), "x").unwrap();

        let suggestions = suggest_similar_paths(worktree, "file:turso_migrations/0007.sql");
        assert_eq!(
            suggestions.first().map(String::as_str),
            Some("file:a/turso_migrations/0007.sql"),
            "the path-suffix match should rank first, got: {suggestions:?}"
        );
        assert!(suggestions.contains(&"file:unrelated/0007.sql".to_string()));
    }

    #[test]
    fn test_suggest_similar_paths_returns_empty_when_no_basename_match() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::write(worktree.join("present.txt"), "x").unwrap();

        assert!(suggest_similar_paths(worktree, "file:absent.txt").is_empty());
    }

    #[test]
    fn test_validate_read_path_nonexistent_suggests_correct_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src-tauri/turso_migrations")).unwrap();
        std::fs::write(
            worktree.join("src-tauri/turso_migrations/0007_add_uri.sql"),
            "sql",
        )
        .unwrap();

        let err =
            validate_read_path(worktree, "file:turso_migrations/0007_add_uri.sql").unwrap_err();
        assert!(err.contains("Entered path does not exist"), "got: {err}");
        assert!(err.contains("Did you mean:"), "got: {err}");
        assert!(
            err.contains("file:src-tauri/turso_migrations/0007_add_uri.sql"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validate_file_path_returns_canonical_uri_and_relative_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        std::fs::create_dir_all(worktree.join("src/lib")).unwrap();
        std::fs::write(worktree.join("src/lib/mod.rs"), "content").unwrap();

        let result = validate_file_path(worktree, "file:src/lib/mod.rs").unwrap();
        assert_eq!(result.uri, "file:src/lib/mod.rs");
        assert_eq!(result.relative_path, "src/lib/mod.rs");
        assert!(result.full_path.ends_with("src/lib/mod.rs"));
    }

    #[test]
    fn test_validate_file_path_creates_parent_dirs_for_new_file() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_file_path(worktree, "file:new/nested/dir/file.txt").unwrap();
        assert_eq!(result.uri, "file:new/nested/dir/file.txt");
        assert_eq!(result.relative_path, "new/nested/dir/file.txt");
        assert!(worktree.join("new/nested/dir").exists());
    }

    #[test]
    fn test_validate_file_path_blocks_relative_traversal() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_file_path(worktree, "file:src/../../outside.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("worktree"));
    }

    #[test]
    fn test_validate_file_path_rejects_absolute() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_file_path(worktree, "file:/tmp/outside.txt");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("worktree-only"));
    }

    #[test]
    fn test_git_commit_all_commits_changes() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        init_git_repo_with_user(worktree, "test@test.com", "Test User");

        // Create and modify a file
        std::fs::write(worktree.join("test.txt"), "content").unwrap();

        // Commit all changes
        let result = git_commit_all(worktree, "Test commit", None);
        assert!(result.is_ok(), "Should successfully commit changes");

        // Verify commit was created
        let log_output = git_output(worktree, &["log", "--oneline"]);
        assert!(log_output.status.success());
        let log_str = String::from_utf8_lossy(&log_output.stdout);
        assert!(log_str.contains("Test commit"));
    }

    #[test]
    fn test_git_commit_files_uses_explicit_author() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        init_git_repo_with_user(worktree, "ambient@example.com", "Ambient User");

        std::fs::write(worktree.join("authored.txt"), "content").unwrap();
        let author = GitAuthor::new("Project User", "project@example.com");
        git_commit_files(
            worktree,
            &["authored.txt"],
            "Authored commit",
            Some(&author),
        )
        .unwrap();

        let log_output = git_output(worktree, &["log", "-1", "--format=%an <%ae>"]);
        assert!(log_output.status.success());
        assert_eq!(
            String::from_utf8_lossy(&log_output.stdout).trim(),
            "Project User <project@example.com>"
        );
    }

    #[test]
    fn has_remote_detects_origin_presence() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        crate::env::git()
            .args(["init"])
            .current_dir(worktree)
            .output()
            .unwrap();

        assert!(!has_remote(worktree));

        crate::env::git()
            .args(["remote", "add", "origin", "https://example.com/repo.git"])
            .current_dir(worktree)
            .output()
            .unwrap();

        assert!(has_remote(worktree));
    }

    #[test]
    fn test_git_commit_file_commits_only_requested_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        init_git_repo_with_user(worktree, "test@test.com", "Test User");

        std::fs::write(worktree.join("selected.txt"), "selected").unwrap();
        std::fs::write(worktree.join("other.txt"), "other").unwrap();

        git_commit_file(worktree, "selected.txt", "Selected commit", None).unwrap();

        let show_output = git_output(worktree, &["show", "--name-only", "--format=", "HEAD"]);
        assert!(show_output.status.success());
        let committed_paths = String::from_utf8_lossy(&show_output.stdout);
        assert!(committed_paths.lines().any(|line| line == "selected.txt"));
        assert!(!committed_paths.lines().any(|line| line == "other.txt"));

        let status_output = git_output(worktree, &["status", "--porcelain"]);
        assert!(status_output.status.success());
        let status = String::from_utf8_lossy(&status_output.stdout);
        assert!(
            status.lines().any(|line| line == "?? other.txt"),
            "unrequested file should remain untracked, got: {status}"
        );
    }

    #[test]
    fn test_git_commit_all_fails_when_nothing_to_commit() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        init_git_repo_with_user(worktree, "test@test.com", "Test User");

        // Try to commit with no changes
        let result = git_commit_all(worktree, "Test commit", None);
        assert!(result.is_err(), "Should fail when nothing to commit");
        assert!(
            result.unwrap_err().contains("nothing to commit"),
            "Error should mention nothing to commit"
        );
    }

    #[test]
    fn test_git_commit_all_amends_previous_commit() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        init_git_repo_with_user(worktree, "test@test.com", "Test User");

        // Create initial commit
        std::fs::write(worktree.join("test.txt"), "content").unwrap();
        let _ = git_commit_all(worktree, "Initial commit", None).unwrap();

        // Get commit count
        let log_output = git_output(worktree, &["rev-list", "--count", "HEAD"]);
        let initial_count = String::from_utf8_lossy(&log_output.stdout)
            .trim()
            .parse::<u32>()
            .unwrap();

        // Make another change and amend
        std::fs::write(worktree.join("test2.txt"), "more content").unwrap();
        let result = git_commit_all(worktree, "^", None);
        assert!(result.is_ok(), "Should successfully amend commit");

        // Verify commit count is still the same (amend doesn't create new commit)
        let log_output = git_output(worktree, &["rev-list", "--count", "HEAD"]);
        let final_count = String::from_utf8_lossy(&log_output.stdout)
            .trim()
            .parse::<u32>()
            .unwrap();
        assert_eq!(
            initial_count, final_count,
            "Amend should not increase commit count"
        );
    }
}
