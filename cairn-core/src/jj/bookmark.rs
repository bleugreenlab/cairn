//! Bookmark / git-ref resolution, export, and publishing to origin.
use super::*;
use std::path::Path;

/// Export jj's state to the workspace's backing git refs (`jj git export`), so a
/// git-level read of HEAD/refs reflects jj's current (post-rebase) state. Used by
/// archival before it packs the worktree history: an out-of-workspace auto-rebase
/// (an orchestration merge) may not have exported to this workspace's git yet, so
/// without the refresh the pack could be built from stale refs. Best-effort.
pub fn export_git(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    jj.run(ws, &["git", "export"], "jj git export").map(|_| ())
}

/// Forward-map a possibly-rewritten commit to its current commit-id and stable
/// change-id. jj's headline auto-rebase rewrites a commit's commit-id while its
/// change-id stays stable, so a coordinate recorded before a rebase points at a
/// now-hidden commit; this resolves it forward to the commit that actually lands
/// in the archival pack, with the durable change-id as provenance.
///
/// Returns `None` when `ws` is not a jj workspace or jj cannot resolve the commit.
/// Both yield identity/skip semantics at the call site: plain-git worktrees and
/// unresolvable ids keep today's behavior.
pub fn forward_resolve_commit(
    jj: &JjEnv,
    ws: &Path,
    commit: &str,
) -> Option<(
    String, /* change_id */
    String, /* current_commit */
)> {
    if !is_jj_dir(ws) {
        return None;
    }
    // Step 1: resolve the (possibly hidden/rewritten) commit to its stable
    // change-id. `jj log -r <commit_id>` resolves even a commit jj has since
    // rewritten, as long as its object still exists — which it does at teardown,
    // before any git gc.
    let change_id = jj
        .run(
            ws,
            &["log", "-r", commit, "--no-graph", "-T", "change_id"],
            "jj forward-resolve change_id",
        )
        .ok()
        .filter(|s| !s.is_empty())?;

    // Step 2: resolve the change-id forward to its current visible commit-id. A
    // divergent change resolves to several visible commits; prefer the one
    // reachable from the worktree tip (`@`), which is the commit the archival pack
    // (built over `pack_anchor..tip`) actually contains. The `change_id(...)`
    // function form is divergence-safe (a bare change-id symbol errors when the
    // change is divergent), and `latest(...)` collapses an empty or multi-commit
    // result to a single line. Fall back to the change-id's latest visible commit
    // when nothing is tip-reachable.
    let tip_reachable = format!("latest(change_id({change_id}) & ::@)");
    let current = jj
        .run(
            ws,
            &["log", "-r", &tip_reachable, "--no-graph", "-T", "commit_id"],
            "jj forward-resolve commit_id (tip-reachable)",
        )
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            jj.run(
                ws,
                &[
                    "log",
                    "-r",
                    &format!("latest(change_id({change_id}))"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                ],
                "jj forward-resolve commit_id",
            )
            .ok()
            .filter(|s| !s.is_empty())
        })?;

    Some((change_id, current))
}

/// Push the workspace's bookmark to origin. Best-effort: logs and never fails,
/// mirroring `mcp::git::push_to_origin`'s contract so a local/remoteless jj
/// project never fails a seal. Skips empty/`main`/`master` branches (the same
/// guard the git path uses). jj 0.42 auto-tracks a new bookmark on push, so the
/// removed `--allow-new` flag is not passed; seals only advance the bookmark, so
/// the push is a fast-forward and needs no force.
///
/// `--ignore-working-copy`: a publish must never SNAPSHOT the live `@`. The
/// bookmark already points at the sealed `@-`, so pushing needs no fresh
/// snapshot — and snapshotting here would fold whatever transient dirt sits in
/// `@` (e.g. a `when:write` check's caches, since the post-seal push runs from
/// the workspace) into the working-copy commit, exactly the kind of working-copy
/// mutation a concurrent store op can then wedge a later seal on. Matches
/// `advance_workspace_onto` / `node_changed_files`, which pass it deliberately.
pub fn push_to_origin(jj: &JjEnv, ws: &Path, branch: &str) {
    if branch.is_empty() || branch == "main" || branch == "master" {
        log::debug!("Skipping jj push for branch: {branch}");
        return;
    }
    match jj.run(
        ws,
        &[
            "git",
            "push",
            "--remote",
            "origin",
            "--bookmark",
            branch,
            "--ignore-working-copy",
        ],
        "jj git push",
    ) {
        Ok(_) => log::info!("Pushed bookmark {branch} to origin (jj)"),
        Err(e) => log::warn!("jj push failed (seal succeeded locally): {e}"),
    }
}

/// Resolve a bookmark name to a commit id over the shared store, or `None` when
/// the bookmark does not exist. `bookmarks(exact:"…")` matches the literal name
/// (bookmark names carry `/`, which a bare revset symbol also accepts but the
/// exact form is unambiguous), and an empty revset exits 0 with empty output.
pub fn bookmark_commit(jj: &JjEnv, store: &Path, branch: &str) -> Option<String> {
    let revset = format!("bookmarks(exact:{:?})", branch);
    revset_commit(jj, store, &revset)
}

/// Whether the `src` bookmark's tip has already landed in `dst` — its commit is
/// an ancestor of (or equal to) the `dst` bookmark's tip in the shared store.
///
/// `bookmarks(exact:SRC) & ::bookmarks(exact:DST)` intersects SRC's target commit
/// with DST's ancestor set (inclusive); a non-empty result means SRC's tip lies
/// on DST's history, i.e. a fold already carried SRC into DST. Returns `false`
/// when either bookmark is missing or the revset is empty — a landed check fails
/// closed ("cannot prove landed" is treated as "not landed"), so a caller that
/// deletes only landed branches preserves anything it cannot verify.
///
/// Note this is a *lineage* test: a squash landing rewrites SRC onto DST before
/// the fold, so the rewritten SRC bookmark is an ancestor of DST and this holds;
/// but an out-of-band squash that discards SRC's commits (e.g. GitHub's own
/// squash-merge) leaves SRC off DST's history and returns `false`. Use it only
/// where the store owns the fold (the local jj merge path and its teardown).
pub fn bookmark_landed_in(jj: &JjEnv, store: &Path, src: &str, dst: &str) -> bool {
    if src.is_empty() || dst.is_empty() {
        return false;
    }
    let revset = format!("bookmarks(exact:{src:?}) & ::bookmarks(exact:{dst:?})");
    revset_commit(jj, store, &revset).is_some()
}

/// Local bookmarks pointing exactly at `rev` in this workspace's view of the
/// store. The single-commit analogue of [`local_bookmarks_in_range`]: the amend
/// guard in [`seal_paths`] uses it to detect whether `@-` (the commit a `^` amend
/// would rewrite) is SHARED with a sibling bookmark, in which case the amend is
/// converted to a child commit rather than rewriting shared history.
/// `--ignore-working-copy` keeps the read from snapshotting `@` before the seal
/// deliberately does so.
pub fn local_bookmarks_at(jj: &JjEnv, ws: &Path, rev: &str) -> Result<Vec<String>, String> {
    let out = jj.run(
        ws,
        &[
            "log",
            "-r",
            rev,
            "--no-graph",
            "-T",
            "local_bookmarks.map(|b| b.name()).join(\"\\n\") ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (local bookmarks at rev)",
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// Resolve a single revset to a commit id over the shared store, or `None` when
/// it does not resolve. Used for both exact local bookmarks and remote-tracking
/// bookmarks such as `main@origin`.
pub fn revset_commit(jj: &JjEnv, store: &Path, revset: &str) -> Option<String> {
    jj.run(
        store,
        &["log", "-r", revset, "--no-graph", "-T", "commit_id"],
        "jj log revset commit",
    )
    .ok()
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
}

/// The commit id of an active workspace's working-copy commit (`<name>@`),
/// resolved over the shared store. Used to detect whether
/// [`advance_workspace_onto`] actually moved the `@` (a real advance) versus an
/// idempotent no-op, so the on-branch advance only notifies on a genuine move.
pub fn workspace_head_commit(jj: &JjEnv, store: &Path, ws_branch: &str) -> Option<String> {
    let source = format!("{}@", workspace_name_for_branch(ws_branch));
    jj.run(
        store,
        &["log", "-r", &source, "--no-graph", "-T", "commit_id"],
        "jj log workspace head commit",
    )
    .ok()
    .filter(|s| !s.is_empty())
}

/// Publish a bookmark that already lives in the shared store to origin. Used to
/// put a Coordinator integration-branch base on origin from the store, where it
/// exists as a bookmark even though the project checkout carries no local ref
/// for it (so the git `push origin <base>` the git path uses cannot find it).
///
/// No-op when the bookmark does not resolve in the store (base not sealed yet)
/// or already matches origin (`jj git push` reports "Nothing changed"). jj 0.42
/// auto-tracks a new bookmark on push, so no `--allow-new` is passed.
pub fn ensure_bookmark_on_origin(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    if branch.is_empty() {
        return Ok(());
    }
    if bookmark_commit(jj, store, branch).is_none() {
        log::debug!("jj base bookmark {branch} absent from store; nothing to publish");
        return Ok(());
    }
    jj.run(
        store,
        &["git", "push", "--remote", "origin", "--bookmark", branch],
        "jj git push base bookmark",
    )
    .map(|_| ())
}
