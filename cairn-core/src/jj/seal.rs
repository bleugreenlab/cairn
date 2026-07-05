//! Sealing the working copy into addressable commits, folding check edits,
//! and discarding working-copy changes.
use super::*;
use std::path::Path;

use crate::mcp::git::{CommitResult, GitAuthor};

/// Whether the working copy (`@`) carries changes versus its parent. Never
/// consults `git status` (non-empty mid-work under jj because the change lives
/// in `@`, not git's HEAD).
pub fn is_working_copy_dirty(jj: &JjEnv, ws: &Path) -> Result<bool, String> {
    Ok(!jj
        .run(ws, &["diff", "--summary"], "jj diff --summary")?
        .is_empty())
}

/// The change id of `@` (stable across the working copy's content amendments).
pub fn snapshot_change_id(jj: &JjEnv, ws: &Path) -> Result<String, String> {
    jj.run(
        ws,
        &["log", "-r", "@", "--no-graph", "-T", "change_id.short()"],
        "jj log -r @",
    )
}

/// Whether the seal's scoped paths carry uncommitted changes in `@`. A whole-`@`
/// seal (empty `paths`) reuses [`is_working_copy_dirty`]; a path-scoped seal
/// diffs only those filesets, because [`seal_paths`] deliberately leaves
/// unrelated un-sealed dirt in `@`, so the empty-seal expectation must be measured
/// against the scoped paths only — otherwise a legitimately no-op scoped write
/// (whose unrelated dirt makes the whole `@` look dirty) would false-positive.
pub(crate) fn scoped_dirty(jj: &JjEnv, ws: &Path, paths: &[&str]) -> Result<bool, String> {
    if paths.is_empty() {
        return is_working_copy_dirty(jj, ws);
    }
    let mut args: Vec<String> = vec!["diff".into(), "-r".into(), "@".into(), "--summary".into()];
    for path in paths {
        args.push(quote_fileset(path));
    }
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    Ok(!jj
        .run(ws, &argref, "jj diff -r @ --summary (scoped)")?
        .is_empty())
}

/// Whether the just-sealed `@-` commit is the empty/divergent data-loss shape: a
/// `jj commit` that returned a real sha but silently captured nothing because a
/// concurrent op reset `@` out from under it. `pre_dirty` is the seal's measured
/// pre-commit dirt over the same scoped paths. Returns `true` when either:
///
/// - `pre_dirty && empty`: the working copy had scoped changes to seal, but `@-`
///   has no diff vs its parent — the dirt was reset away before the commit
///   captured it (jj's `empty` keyword, correct for both seal modes since only
///   the scoped paths were committed into `@-`); or
/// - divergent: the sealed change resolves to more than one visible commit
///   (`<id>/0../n`), the shape a concurrent-op merge leaves when both forked
///   rewrites are kept.
///
/// Two cheap `jj log` reads on the just-sealed commit; runs only on the seal path.
pub(crate) fn sealed_commit_is_lost(
    jj: &JjEnv,
    ws: &Path,
    pre_dirty: bool,
) -> Result<bool, String> {
    let empty = jj
        .run(
            ws,
            &["log", "-r", "@-", "--no-graph", "-T", "empty"],
            "jj seal empty check",
        )?
        .contains("true");
    if pre_dirty && empty {
        return Ok(true);
    }
    let cid = jj.run(
        ws,
        &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
        "jj seal change id",
    )?;
    let cid = cid.trim();
    if cid.is_empty() {
        return Ok(false);
    }
    let twins = jj.run(
        ws,
        &[
            "log",
            "-r",
            &format!("change_id({cid})"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
        ],
        "jj seal divergence check",
    )?;
    Ok(twins.lines().filter(|l| !l.trim().is_empty()).count() > 1)
}

/// Seal the whole `@` into one addressable commit (the run-path seal: seals the
/// entire working copy). See [`seal_paths`].
pub fn seal(
    jj: &JjEnv,
    ws: &Path,
    msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    seal_paths(jj, ws, msg, author, &[])
}

/// Seal `@` into one addressable commit and open a fresh empty `@`. When `paths`
/// is non-empty the seal is **path-scoped**: only those paths leave `@`, so
/// unrelated un-sealed dirt (e.g. a prior failed or full-sandbox run's side
/// effects) stays in the working copy and is NOT folded into this commit: a
/// file-scoped seal touches only those paths. An empty slice seals the whole `@`.
/// `^` folds the scoped paths into the prior sealed commit (git `--amend`
/// equivalent). Advances the workspace's git bookmark to the sealed commit and
/// exports it to the project's git (best-effort). Returns the sealed commit id.
pub fn seal_paths(
    jj: &JjEnv,
    ws: &Path,
    msg: &str,
    author: Option<&GitAuthor>,
    paths: &[&str],
) -> Result<CommitResult, String> {
    // Read the workspace's own branch up front: it drives both the amend-share
    // guard here and the fast-forward guard / bookmark advance further down.
    let branch = read_branch_marker(ws);

    let mut args: Vec<String> = JjEnv::author_args(author);
    // Set when a `^` amend is CONVERTED to a child commit because `@-` is shared.
    let mut amend_note: Option<String> = None;
    if msg == "^" {
        // A `^` amend rewrites `@-` in place. If `@-` carries a bookmark OTHER than
        // this workspace's own branch, that commit is SHARED — a sibling or
        // integration bookmark is parked on it — and squash-rewriting it would
        // break the sibling (the incident: an amend rewrote a shared integration
        // commit while the builder's bookmark sat on the tip). Convert to a regular
        // child commit reusing `@-`'s description; the post-seal `bookmark set
        // <own-branch> -r @-` then advances only THIS branch and the shared commit
        // is never rewritten.
        let foreign: Vec<String> = local_bookmarks_at(jj, ws, "@-")
            .unwrap_or_default()
            .into_iter()
            .filter(|b| branch.as_deref() != Some(b.as_str()))
            .collect();
        if foreign.is_empty() {
            args.extend(["squash".into(), "--use-destination-message".into()]);
        } else {
            let desc = jj
                .run(
                    ws,
                    &[
                        "log",
                        "-r",
                        "@-",
                        "--no-graph",
                        "-T",
                        "description",
                        "--ignore-working-copy",
                    ],
                    "jj amend-convert description",
                )
                .map(|s| s.trim().to_string())
                .unwrap_or_default();
            let desc = if desc.is_empty() {
                "amend".to_string()
            } else {
                desc
            };
            args.extend(["commit".into(), "-m".into(), desc]);
            amend_note = Some(format!(
                "amend converted to a new commit: the previous commit is shared with {}",
                foreign.join(", ")
            ));
        }
    } else {
        args.extend(["commit".into(), "-m".into(), msg.into()]);
    }
    // Path-scope so only these paths leave `@`; empty = whole working copy.
    // jj parses positional path args as fileset expressions, so each path is
    // wrapped as a quoted string literal to match a path with fileset
    // metacharacters (e.g. a Next.js `(app)` route group) literally.
    for path in paths {
        args.push(quote_fileset(path));
    }
    let argref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    // Pre-commit backstop: refuse a stale-workspace seal BEFORE creating the
    // commit, so no orphan is ever produced. If the branch bookmark has advanced
    // PAST this workspace's head `@-` (a Coordinator whose integration bookmark a
    // child fold moved out from under its stale `@`), the commit would descend
    // from the stale `@-` and land OFF the branch; the bookmark advance would then
    // be refused as non-fast-forward, leaving an orphaned commit the generic
    // discard (`jj restore`, which only resets `@` to its parent) cannot recover.
    // Checking here — before `jj commit` — keeps `@` clean and on the stale line so
    // a follow-up advance can fix it. The healthy case (bookmark == `@-`) and an
    // amend (the bookmark follows the rewrite) both fast-forward. With the
    // post-fold workspace advance in place this is unreachable on the happy path.
    if let Some(branch) = branch.as_deref() {
        if !seal_is_fast_forward(jj, ws, branch)? {
            // The fast-forward guard refused: `@` does not descend from the branch
            // bookmark. Two structurally different causes need OPPOSITE handling,
            // and ancestry alone cannot separate them (in both, `@-` is an ancestor
            // of the bookmark). The distinguisher is whether the bookmark tip
            // carries a recorded CONFLICT:
            //
            // - Conflicted tip → a deliberate resolve-at-base FLATTEN. `@` is a
            //   fresh resolved tree on the current base while the bookmark still
            //   points at the conflicted intermediate stack tip the agent is
            //   escaping. Discarding `@` would destroy the resolved work and
            //   advancing would land back on the conflict, so this returns a
            //   DISTINCT error routed to a non-destructive preserve-and-instruct
            //   path (see [`is_conflicted_branch_seal_error`]).
            // - Clean tip → a genuine STALE / coordinator-advance: the bookmark
            //   advanced onto a clean tip and `@` is a stale shell. The existing
            //   "behind its branch tip" message and its stale-family recovery
            //   (discard, self-healing via update-stale) stay unchanged.
            if branch_has_conflict(jj, ws, branch).unwrap_or(false) {
                return Err(CONFLICTED_BRANCH_SEAL_MSG.to_string());
            }
            return Err(format!(
                "seal refused: workspace `{branch}` is behind its branch tip — the branch \
                 advanced past this workspace's head, so sealing would create a commit off \
                 `{branch}`. The workspace must be advanced onto the branch tip before sealing."
            ));
        }
    }

    // Measure the scoped dirt BEFORE committing so an EMPTY seal (the working copy
    // reset out from under the commit) can be told apart from a legitimately no-op
    // scoped write. Best-effort: if the probe can't run we conservatively skip the
    // empty-anomaly arm (divergence is still checked) rather than fail a good seal.
    // Skipped for an amend (`^`): its emptiness semantics differ and it is not the
    // observed failure mode.
    let pre_dirty = if msg == "^" {
        false
    } else {
        scoped_dirty(jj, ws, paths).unwrap_or(false)
    };

    jj.run(ws, &argref, "jj commit")?;
    let sha = jj.run(
        ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
        "jj log -r @-",
    )?;

    // Detection backstop: a concurrent store advance can reset `@` out from under
    // the commit so `jj commit` succeeds but seals an EMPTY or DIVERGENT commit —
    // silent data loss otherwise reported as a real sha. Check only on a real
    // commit (the amend path is excluded above via `pre_dirty`/`msg`). On the
    // anomaly, back the bad commit out so `@` returns to its pre-seal parent and a
    // retry lands cleanly, then return the typed, recoverable lost-seal error. The
    // bookmark has NOT moved yet (that runs only on the clean path below), so
    // `jj abandon @-` reparents `@` onto the original parent and drops the commit
    // without stranding the bookmark on a twin.
    if msg != "^" && sealed_commit_is_lost(jj, ws, pre_dirty).unwrap_or(false) {
        if let Err(e) = jj.run(ws, &["abandon", "@-"], "jj abandon lost seal") {
            log::warn!("failed to back out lost-seal commit (still reporting the loss): {e}");
        }
        return Err(LOST_SEAL_MSG.to_string());
    }
    // Advance the project's git branch ref to the sealed commit so push and
    // git-side reads stay current. The pre-commit fast-forward check above
    // guarantees this is a forward move, so it stays best-effort: a transient ref
    // failure never fails an otherwise-good seal (a stale ref self-heals next
    // seal).
    if let Some(branch) = branch.as_deref() {
        if let Err(e) = jj.run(
            ws,
            &["bookmark", "set", branch, "-r", "@-"],
            "jj bookmark set",
        ) {
            log::warn!("jj bookmark set after seal (best-effort, continuing): {e}");
        }
        let _ = jj.run(ws, &["git", "export"], "jj git export");
    }
    Ok(CommitResult {
        sha,
        pr_number: None,
        amend_note,
    })
}

/// Whether sealing this workspace would FAST-FORWARD its branch bookmark: the
/// bookmark must be an ancestor of (or equal to) the workspace head `@-`, so a new
/// commit descending from `@-` advances the bookmark forward. `false` means the
/// branch advanced PAST this workspace (a Coordinator whose integration bookmark a
/// child fold moved out from under its stale `@`); sealing then would create an
/// off-branch commit whose bookmark advance jj refuses as non-fast-forward.
/// [`seal_paths`] checks this BEFORE `jj commit` so a stale seal is refused
/// without ever creating the orphan. A bookmark that does not resolve yet (never
/// created) is treated as fast-forwardable — the post-commit `bookmark set` will
/// create it. The revset `(<bookmark>) & ::@` is non-empty iff the bookmark
/// commit is an ancestor-or-self of `@` (the working copy) — i.e. `@` descends
/// from the bookmark, so sealing fast-forwards it.
///
/// `::@` (not `::@-`) is deliberate: it also accepts the bookmark sitting ON `@`
/// itself — the legitimate state when the worktree's working-copy commit IS the
/// branch tip (e.g. an agent's last commit is the working copy, or any worktree
/// where the bookmark was set to `@`). Sealing there is a clean fast-forward (the
/// edit commits into `@` and the bookmark advances), so it must not be refused.
/// A genuinely-ahead bookmark on a divergent line (the Coordinator-fold case) is
/// still rejected, because it is not an ancestor of `@`.
pub fn seal_is_fast_forward(jj: &JjEnv, ws: &Path, branch: &str) -> Result<bool, String> {
    let Some(bookmark) = bookmark_commit(jj, ws, branch) else {
        return Ok(true);
    };
    let hit = jj.run(
        ws,
        &[
            "log",
            "-r",
            &format!("({bookmark}) & ::@"),
            "--no-graph",
            "-T",
            "commit_id",
        ],
        "jj seal fast-forward precheck",
    )?;
    Ok(!hit.is_empty())
}

/// Outcome of folding a `when:write` check's tracked changes into the sealed
/// commit: the repo-relative paths the check modified (also the inline summary's
/// content). `fold_worktree_into_seal` returns `None` instead of an empty list
/// when there was nothing to fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldOutcome {
    pub folded_files: Vec<String>,
}

/// Fold a `when:write` check's tracked working-copy changes into the just-sealed
/// commit (`jj squash` of `@` into `@-`), leaving `@` clean == the amended sealed
/// tip.
///
/// A check is an observer of the sealed commit, but its command may legitimately
/// rewrite tracked files (a formatter, `lint --fix`, regenerated snapshots).
/// Folding does two jobs at once: it delivers those edits into the commit AND
/// restores the seal-clean invariant, so a concurrent base-advance / reconcile in
/// the lock-free check window can never snapshot or rebase a dirty `@` into the
/// stale / divergent / behind-tip tangle that wedges the next seal (CAIRN-2260).
///
/// Only TRACKED changes fold: gitignored writes (vitest/tsc caches) are excluded
/// from the working-copy snapshot (gitignore + `snapshot.auto-track`), so they
/// never enter `@` and are never committed — they stay as ignored files on disk.
/// `jj squash` keeps the sealed commit's message and author, so the folded edits
/// ride the agent's original commit. The bookmark follows the rewrite onto the
/// amended commit; the git ref and origin are re-published (best-effort) so they
/// reflect the new tree (an amend-push jj tracks via the remote bookmark).
///
/// Returns `Ok(None)` when `@` carried no tracked change (a pure verify check) —
/// the amend is then a no-op and `@` was already clean.
pub fn fold_worktree_into_seal(jj: &JjEnv, ws: &Path) -> Result<Option<FoldOutcome>, String> {
    // The tracked files the check changed. Empty => pure verify check: nothing to
    // fold, `@` already clean. (A stale `@` makes this error and propagate, so the
    // caller falls back to the next seal's stale recovery rather than amending
    // blindly.)
    let changed = jj.run(ws, &["diff", "-r", "@", "--name-only"], "jj fold diff")?;
    let folded_files: Vec<String> = changed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    if folded_files.is_empty() {
        return Ok(None);
    }
    // Fold `@`'s tracked changes into the sealed parent and open a fresh empty `@`.
    jj.run(ws, &["squash"], "jj squash (fold check changes)")?;
    // Re-establish bookmark / git ref / origin at the amended commit, mirroring a
    // seal. The bookmark auto-follows the rewrite; `bookmark set` is idempotent
    // belt-and-braces, and the export/push propagate the amended tree.
    if let Some(branch) = read_branch_marker(ws) {
        if let Err(e) = jj.run(
            ws,
            &["bookmark", "set", &branch, "-r", "@-"],
            "jj bookmark set (fold)",
        ) {
            log::warn!("fold: bookmark set after squash (best-effort): {e}");
        }
        let _ = jj.run(ws, &["git", "export"], "jj git export (fold)");
        push_to_origin(jj, ws, &branch);
    }
    Ok(Some(FoldOutcome { folded_files }))
}

/// Discard working-copy changes by resetting `@` to its parent. Reversible via
/// the operation log — replacing git's destructive `reset --hard`.
///
/// Self-heals a STALE working copy. `jj restore` is itself blocked on a stale
/// `@` (a sibling workspace rewrote it over the shared store) — the same refusal
/// that blocks the seal — so a naive `restore` would dead-end and strand the
/// loose edits uncommitted, exactly the data-loss path the commit barrier must
/// not have. `update-stale` is the one op staleness does not block: it refreshes
/// `@` onto the rewritten/advanced commit and overwrites the loose
/// (unsnapshotted) batch edits, leaving the worktree == fresh `@`. So when
/// `restore` reports staleness, recover through `update-stale` instead of
/// failing, and the rollback no longer shares the seal's single point of
/// failure. See [`is_stale_error`].
pub fn discard(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    match jj.run(ws, &["restore"], "jj restore") {
        Ok(_) => Ok(()),
        Err(e) if is_stale_error(&e) => {
            // update-stale advances `@` and discards the loose edits → clean.
            update_stale(jj, ws)?;
            // Belt-and-braces: a now-unblocked restore guarantees `@` == parent.
            let _ = jj.run(ws, &["restore"], "jj restore (post update-stale)");
            Ok(())
        }
        Err(e) => Err(e),
    }
}
