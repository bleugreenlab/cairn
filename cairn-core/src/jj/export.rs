//! HEAD-preserving `jj git export`.
//!
//! The shared jj store's git backend IS the project's `.git`, so any
//! `jj git export` that moves the branch the project checkout's HEAD is a symref
//! to (normally `refs/heads/main`) DETACHES that HEAD: jj cannot leave HEAD a
//! symref to a branch it is moving, so it pins HEAD at the pre-move commit (which
//! keeps the working tree clean). Left alone, the user's checkout sits in
//! detached HEAD after the export.
//!
//! [`export_git_preserving_checkout`] wraps the export and, when it can PROVE the
//! export itself caused the detach — HEAD was attached to `B` before, `B` moved
//! during the export, and HEAD is detached after — re-attaches HEAD to `B` and
//! fast-forwards the clean working tree. This is the one canonical repair,
//! invoked synchronously at the export choke point rather than in a deferred,
//! success-path-only cleanup. The pre/post attribution is what makes it safe to
//! run on EVERY export: it only ever repairs a detach the export caused, works
//! for non-default branches, and never touches a user-deliberate detached HEAD.
use super::JjEnv;
use crate::pr_data::helpers::reattach_checkout_head;
use crate::services::{GitClient, RealGitClient};
use std::path::{Path, PathBuf};

/// The project checkout's HEAD attachment, snapshotted before an export. Only
/// captured when HEAD was ATTACHED (a pre-existing detached HEAD is the user's
/// own choice and is never repaired), so `branch` is always non-empty.
struct CheckoutHeadBefore {
    branch: String,
    branch_commit: String,
}

/// Run `jj git export` in `export_cwd` and repair the project checkout's HEAD if
/// the export detached it by moving the branch HEAD was attached to.
///
/// The export's own result semantics are preserved verbatim (callers propagate or
/// swallow it exactly as before); the HEAD repair is a best-effort side effect
/// that logs a warning on failure and never changes the returned result.
pub fn export_git_preserving_checkout(
    jj: &JjEnv,
    export_cwd: &Path,
    ignore_working_copy: bool,
    ctx: &str,
) -> Result<(), String> {
    let checkout = resolve_backing_checkout(export_cwd);
    let git = RealGitClient;
    let before = checkout
        .as_deref()
        .and_then(|repo| snapshot_checkout_head(&git, repo));

    let args: &[&str] = if ignore_working_copy {
        &["git", "export", "--ignore-working-copy"]
    } else {
        &["git", "export"]
    };
    let result = jj.run(export_cwd, args, ctx).map(|_| ());

    if let (Some(repo), Some(before)) = (checkout.as_deref(), before.as_ref()) {
        repair_export_detach(&git, repo, before);
    }
    result
}

/// Resolve the primary project checkout backing the jj store/workspace at
/// `export_cwd`, or `None` when the topology cannot be resolved (best-effort:
/// an unresolvable checkout simply skips the HEAD repair).
///
/// A jj store's `.jj/repo` is the repo directory itself; a workspace's `.jj/repo`
/// is a file naming the shared store's repo directory (relative to that file).
/// The store's git backend records the project's `.git` in `store/git_target`,
/// and Cairn always inits the store against the project's MAIN checkout, so the
/// checkout is the worktree that owns that `.git` — its parent directory.
fn resolve_backing_checkout(export_cwd: &Path) -> Option<PathBuf> {
    let repo_pointer = export_cwd.join(".jj").join("repo");
    let store_repo = if repo_pointer.is_dir() {
        repo_pointer
    } else {
        let target = std::fs::read_to_string(&repo_pointer).ok()?;
        let target = PathBuf::from(target.trim());
        if target.is_absolute() {
            target
        } else {
            repo_pointer.parent()?.join(target)
        }
    };

    let git_target_file = store_repo.join("store").join("git_target");
    let raw = std::fs::read_to_string(&git_target_file).ok()?;
    let git_dir = PathBuf::from(raw.trim());
    let git_dir = if git_dir.is_absolute() {
        git_dir
    } else {
        git_target_file.parent()?.join(git_dir)
    };

    let checkout = git_dir.parent()?.to_path_buf();
    // Guard against a topology that isn't a `<worktree>/.git` layout (e.g. a bare
    // backend): only a real worktree can carry a detached HEAD to repair.
    git_dir.exists().then_some(checkout)
}

/// Snapshot the checkout's HEAD attachment before the export. Returns `None` when
/// HEAD is already detached (the user's own choice, never repaired) or when the
/// probe fails (best-effort: skip the repair).
fn snapshot_checkout_head(git: &dyn GitClient, repo: &Path) -> Option<CheckoutHeadBefore> {
    // `git branch --show-current` is empty exactly when HEAD is detached.
    let branch = git.current_branch(repo).ok()?;
    if branch.is_empty() {
        return None;
    }
    let branch_commit = git
        .rev_parse(repo, vec![format!("refs/heads/{branch}")])
        .ok()?;
    Some(CheckoutHeadBefore {
        branch,
        branch_commit,
    })
}

/// Repair a detach the export caused, and ONLY that: HEAD must be detached now,
/// and the branch it was attached to must have actually moved during the export.
/// Any other post-state (still attached, or the branch did not move) is left
/// untouched. Best-effort throughout — a probe failure logs and returns.
fn repair_export_detach(git: &dyn GitClient, repo: &Path, before: &CheckoutHeadBefore) {
    let now = match git.current_branch(repo) {
        Ok(branch) => branch,
        Err(e) => {
            log::warn!(
                "post-export HEAD check failed for checkout {}: {e}",
                repo.display()
            );
            return;
        }
    };
    if !now.is_empty() {
        // Still attached: the export did not detach HEAD.
        return;
    }

    let after_commit = match git.rev_parse(repo, vec![format!("refs/heads/{}", before.branch)]) {
        Ok(commit) => commit,
        Err(e) => {
            log::warn!(
                "post-export ref read failed for checkout {}: {e}",
                repo.display()
            );
            return;
        }
    };
    if after_commit == before.branch_commit {
        // The branch did not move, so this export did not cause the detach; leave
        // HEAD alone rather than attribute an unrelated detached state to it.
        return;
    }

    if let Err(e) = reattach_checkout_head(git, repo, &before.branch) {
        log::warn!("failed to re-attach checkout HEAD after export detached it: {e}");
    }
}
