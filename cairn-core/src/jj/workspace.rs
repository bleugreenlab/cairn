//! Job workspace lifecycle over the shared store and the non-snapshotted
//! `.jj` marker files (branch, base, project-root).
use super::*;
use std::path::Path;

use crate::mcp::git::GitAuthor;

/// Filename of the non-snapshotted branch marker inside a workspace's `.jj` dir.
/// See [`read_branch_marker`] for what its presence and absence mean.
pub(crate) const BRANCH_MARKER: &str = "cairn-branch";

/// Filename of the non-snapshotted base marker inside a workspace's `.jj` dir.
/// Records the integration base (branch name + resolved SHA) so in-fence check
/// tooling can diff the agent's own commits against the base it branched from —
/// the worktree otherwise has no on-disk record of its base (jj ancestry cannot
/// tell the base apart from siblings that coincide at the branch point). See
/// `scripts/lib/check-base.ts` and `docs/check-harness.md`.
const BASE_MARKER: &str = "cairn-base";

/// Filename of the non-snapshotted project-root marker inside a workspace's
/// `.jj` dir. Records the project's primary local checkout path so in-worktree
/// dev tooling can borrow machine-local artifacts from it (sidecar binaries,
/// warm caches). A jj workspace is `.jj`-only — `git rev-parse` cannot find
/// the checkout the way it can from a linked git worktree — so without the
/// marker there is no on-disk route back. See `scripts/main-checkout.ts`.
const PROJECT_ROOT_MARKER: &str = "cairn-project-root";

/// jj workspace names cannot contain `/`; map a git branch to a stable name.
pub fn workspace_name_for_branch(branch: &str) -> String {
    branch.replace('/', "-")
}

/// Add a job workspace off the shared store at `ws_path`, basing its working
/// copy on `base_rev`, and record the real branch in the marker.
pub fn add_workspace(
    jj: &JjEnv,
    store_dir: &Path,
    ws_path: &Path,
    branch: &str,
    base_rev: &str,
    author: Option<&GitAuthor>,
) -> Result<(), String> {
    add_workspace_with_marker_writer(
        jj,
        store_dir,
        ws_path,
        branch,
        base_rev,
        author,
        write_branch_marker,
    )
}

fn rollback_created_workspace(
    jj: &JjEnv,
    store_dir: &Path,
    ws_path: &Path,
    name: &str,
    branch: &str,
    delete_created_bookmark: bool,
) {
    let _ = forget_workspace_name(jj, store_dir, name);
    if delete_created_bookmark {
        let _ = jj.run(
            store_dir,
            &["bookmark", "delete", branch, "--ignore-working-copy"],
            "jj bookmark delete",
        );
    }
    let _ = std::fs::remove_dir_all(ws_path);
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn add_workspace_with_marker_writer(
    jj: &JjEnv,
    store_dir: &Path,
    ws_path: &Path,
    branch: &str,
    base_rev: &str,
    author: Option<&GitAuthor>,
    marker_writer: impl FnOnce(&Path, &str) -> Result<(), String>,
) -> Result<(), String> {
    let name = workspace_name_for_branch(branch);

    // Inspection and destructive retry cleanup are deliberately separate. The
    // orchestration layer must prove this exact job owns both the registration
    // and path before calling `cleanup_workspace_retry`; this low-level add never
    // forgets or removes pre-existing state on its own.
    let mut args: Vec<String> = JjEnv::author_args(author);
    args.extend([
        "workspace".into(),
        "add".into(),
        "--name".into(),
        name,
        "-r".into(),
        base_rev.into(),
        ws_path.to_string_lossy().to_string(),
    ]);
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    // The one store operation jj will not let Cairn perform without the store's
    // default workspace — it rejects `--ignore-working-copy` here outright — so
    // it carries the repair instead. See [`run_needing_store_workspace`].
    run_needing_store_workspace(jj, store_dir, &argref, "jj workspace add")?;
    if let Err(error) = marker_writer(ws_path, branch) {
        // Bookmark creation occurs only after the marker succeeds, so this
        // failure owns only the new workspace registration and path. A branch
        // bookmark may have predated this general low-level call.
        rollback_created_workspace(
            jj,
            store_dir,
            ws_path,
            &workspace_name_for_branch(branch),
            branch,
            false,
        );
        return Err(error);
    }

    // Ensure the workspace's branch is a resolvable, pushable bookmark from
    // creation — git parity, where a worktree's branch ref exists immediately.
    // A Coordinator never seals (seal is the only other place a bookmark is
    // created), so without this its integration bookmark would never exist and a
    // child's `jj workspace add -r <integration-branch>` could not resolve the
    // revision (it also leaves `ensure_bookmark_on_origin` nothing to publish).
    // Create only if absent: `bookmark create` errors when the name already
    // exists and a retried job must not fail on that, while `bookmark set` is
    // wrong here because it refuses backwards/sideways moves.
    if bookmark_commit(jj, store_dir, branch).is_none() {
        if let Err(error) = jj.run(
            store_dir,
            &[
                "bookmark",
                "create",
                branch,
                "-r",
                base_rev,
                "--ignore-working-copy",
            ],
            "jj bookmark create",
        ) {
            // This branch is reached only after proving the bookmark absent;
            // a partially-applied create is therefore owned by this invocation.
            rollback_created_workspace(
                jj,
                store_dir,
                ws_path,
                &workspace_name_for_branch(branch),
                branch,
                true,
            );
            return Err(error);
        }
    }
    Ok(())
}

/// Whether `rev` resolves to a commit in the shared store (any revset: a
/// bookmark, commit id, or `root()`). Lets a base ref that is not a project git
/// ref (an unsealed coordinator bookmark, which lives only in the shared store)
/// still be handed to `jj workspace add`.
///
/// `--ignore-working-copy` is load-bearing rather than conventional here: this
/// probe answers `false` on ANY jj error, and [`resolve_base_rev`] reads that
/// `false` as "not a store revset" and falls through to `HEAD` or `root()`. A
/// stale store default workspace would therefore not fail provisioning — it
/// would silently provision the job off the wrong base.
pub fn revset_resolves(jj: &JjEnv, store: &Path, rev: &str) -> bool {
    jj.run(
        store,
        &[
            "log",
            "-r",
            rev,
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
        "jj log resolve",
    )
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// Resolve a base ref to a revision `jj workspace add -r` / `bookmark create -r`
/// can always resolve in the shared store, so provisioning never fails with
/// `Revision <x> doesn't exist`. The ladder, in order:
///
/// 1. `git_rev_parse(base_ref)` -> commit SHA (the common path; the store's git
///    backend is the project `.git`, so the SHA resolves directly in the store).
/// 2. Else, if `base_ref` already resolves in the store as a revset (an unsealed
///    coordinator bookmark is a store bookmark, not a project git ref) -> keep
///    it literal. This probe MUST come before the HEAD fallback, or a
///    coordinator branch would be silently re-based onto the default tip.
/// 3. Else, `git_rev_parse("HEAD")` -> the repo's current tip (a local-only repo
///    whose configured default branch name has no matching ref, but which has
///    commits, bases off its real tip — git parity).
/// 4. Else (unborn / empty repo, no `HEAD`) -> `root()`, jj's always-present
///    root commit.
///
/// `git_rev_parse` returns the trimmed SHA for a ref the project git resolves,
/// or `None`. Kept as a closure so the orchestration layer owns the git service
/// and this stays unit-testable with the jj test harness.
pub(crate) fn resolve_base_rev<F>(
    jj: &JjEnv,
    store: &Path,
    base_ref: &str,
    git_rev_parse: F,
) -> String
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(sha) = git_rev_parse(base_ref).filter(|s| !s.trim().is_empty()) {
        return sha.trim().to_string();
    }
    if revset_resolves(jj, store, base_ref) {
        return base_ref.to_string();
    }
    if let Some(sha) = git_rev_parse("HEAD").filter(|s| !s.trim().is_empty()) {
        return sha.trim().to_string();
    }
    "root()".to_string()
}

/// Cleanup for a retry whose exact registration/path ownership was proven by the
/// orchestration layer. Never call this as collision recovery.
pub fn cleanup_workspace_retry(
    jj: &JjEnv,
    store_dir: &Path,
    ws_path: &Path,
    workspace_name: &str,
) -> Result<(), String> {
    let _ = forget_workspace_name(jj, store_dir, workspace_name);
    if ws_path.exists() {
        std::fs::remove_dir_all(ws_path)
            .map_err(|e| format!("clear proven retry workspace dir: {e}"))?;
    }
    Ok(())
}

/// Forget a persisted jj workspace registration name. The directory itself is
/// removed by the caller.
pub(crate) fn forget_workspace_name(
    jj: &JjEnv,
    store_dir: &Path,
    workspace_name: &str,
) -> Result<(), String> {
    jj.run(
        store_dir,
        &[
            "workspace",
            "forget",
            workspace_name,
            "--ignore-working-copy",
        ],
        "jj workspace forget",
    )
    .map(|_| ())
}

/// Compatibility helper for callers that still key teardown by branch.
pub fn forget_workspace(jj: &JjEnv, store_dir: &Path, branch: &str) -> Result<(), String> {
    forget_workspace_name(jj, store_dir, &workspace_name_for_branch(branch))
}

/// Record the real git branch in the workspace's non-snapshotted marker — the
/// act that makes the workspace Cairn-owned. Written once, by
/// [`add_workspace_with_marker_writer`], as part of provisioning.
pub fn write_branch_marker(ws_path: &Path, branch: &str) -> Result<(), String> {
    let p = ws_path.join(".jj").join(BRANCH_MARKER);
    std::fs::write(&p, format!("{branch}\n")).map_err(|e| format!("write branch marker: {e}"))
}

/// The OWNERSHIP PREDICATE for a checkout: did Cairn provision it, and does
/// Cairn own a branch here?
///
/// `Some(branch)` means yes, so publishing to that branch and rolling the
/// working copy back are both this system's to perform. `None` means the
/// checkout is somebody else's — a user-colocated jj repo is the standing case —
/// and Cairn must touch nothing in it that publishes or destroys. A caller
/// therefore WITHHOLDS on `None`; it must not refuse, because absence is not a
/// fault.
///
/// Absence is in fact the only answer production gives today: nothing outside
/// tests calls [`add_workspace`], so no checkout a run can reach carries a
/// marker. The predicate survives because it is what keeps an ambient run in a
/// user's own jj repo from publishing into it or reverting it, and because the
/// jj suite provisions real marked workspaces to exercise the machinery — seal,
/// merge, reconcile, base advance — that is still very much live.
pub fn read_branch_marker(ws_path: &Path) -> Option<String> {
    std::fs::read_to_string(ws_path.join(".jj").join(BRANCH_MARKER))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Record the integration base in the workspace's non-snapshotted marker: the
/// base branch name on line 1 (it auto-advances with the integration tip, so a
/// branch-keyed changed-file diff stays correct as the base moves) and the
/// resolved base SHA on line 2 (a stable cache key for a future baseline). The
/// `.jj` dir is never snapshotted, so the marker is invisible to the working
/// copy commit — like [`write_branch_marker`].
pub fn write_base_marker(ws_path: &Path, base_branch: &str, base_rev: &str) -> Result<(), String> {
    let p = ws_path.join(".jj").join(BASE_MARKER);
    std::fs::write(&p, format!("{base_branch}\n{base_rev}\n"))
        .map_err(|e| format!("write base marker: {e}"))
}

/// Read the workspace's base marker as `(branch, rev)`, if present. Returns
/// `None` when the marker is absent or its branch line is empty.
pub(crate) fn read_base_marker(ws_path: &Path) -> Option<(String, String)> {
    let content = std::fs::read_to_string(ws_path.join(".jj").join(BASE_MARKER)).ok()?;
    let mut lines = content.lines();
    let branch = lines.next().map(str::trim).filter(|s| !s.is_empty())?;
    let rev = lines.next().map(str::trim).unwrap_or("");
    Some((branch.to_string(), rev.to_string()))
}

/// Record the project's primary checkout path in the workspace's
/// non-snapshotted marker — like [`write_branch_marker`], invisible to the
/// working-copy commit.
pub fn write_project_root_marker(ws_path: &Path, repo_path: &Path) -> Result<(), String> {
    let p = ws_path.join(".jj").join(PROJECT_ROOT_MARKER);
    std::fs::write(&p, format!("{}\n", repo_path.display()))
        .map_err(|e| format!("write project root marker: {e}"))
}
