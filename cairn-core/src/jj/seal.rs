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

#[cfg(test)]
pub(crate) fn sealed_commit_is_lost(
    jj: &JjEnv,
    ws: &Path,
    pre_dirty: bool,
) -> Result<bool, String> {
    sealed_commit_probe(jj, ws, pre_dirty).map(|(_, lost)| lost)
}

/// The change id of `@` (stable across the working copy's content amendments).
pub(crate) fn snapshot_change_id(jj: &JjEnv, ws: &Path) -> Result<String, String> {
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
fn sealed_commit_probe(jj: &JjEnv, ws: &Path, pre_dirty: bool) -> Result<(String, bool), String> {
    let probe = jj.run(
        ws,
        &[
            "log",
            "-r",
            "@-",
            "--no-graph",
            "-T",
            "commit_id.short() ++ \"|\" ++ if(empty, \"true\", \"false\") ++ \"|\" ++ change_id.short()",
        ],
        "jj sealed commit probe",
    )?;
    let (sha, empty, cid) = parse_sealed_commit_probe(&probe)?;
    if pre_dirty && empty {
        return Ok((sha, true));
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
    Ok((
        sha,
        twins.lines().filter(|l| !l.trim().is_empty()).count() > 1,
    ))
}

fn parse_sealed_commit_probe(probe: &str) -> Result<(String, bool, String), String> {
    let mut fields = probe.trim().split('|');
    let sha = fields.next().unwrap_or_default().trim();
    let empty = match fields.next().map(str::trim) {
        Some("true") => true,
        Some("false") => false,
        other => {
            return Err(format!(
                "malformed jj sealed commit probe: invalid empty field {other:?}"
            ))
        }
    };
    let cid = fields.next().unwrap_or_default().trim();
    if sha.is_empty() || cid.is_empty() || fields.next().is_some() {
        return Err(format!("malformed jj sealed commit probe: {probe:?}"));
    }
    Ok((sha.to_string(), empty, cid.to_string()))
}

/// Seal the whole `@` into one addressable commit, resolving this workspace's
/// branch ownership from its marker AT CALL TIME.
///
/// That resolution is only sound when nothing has run between the read and the
/// seal, so the production barrier path deliberately does not use this: it
/// captures ownership during request preflight and passes it to [`seal_paths`],
/// because the marker lives in a checkout the batch can write and a batch must
/// not be able to redirect its own publication — or silence it and still be told
/// the commit landed. This wrapper serves fixtures that provision a workspace and
/// seal it in the same breath, which is every remaining caller.
pub fn seal(
    jj: &JjEnv,
    ws: &Path,
    msg: &str,
    author: Option<&GitAuthor>,
) -> Result<CommitResult, String> {
    let branch = read_branch_marker(ws);
    seal_paths(jj, ws, msg, author, &[], branch.as_deref())
}

/// Seal `@` into one addressable commit and open a fresh empty `@`. When `paths`
/// is non-empty the seal is **path-scoped**: only those paths leave `@`, so
/// unrelated un-sealed dirt (e.g. a prior failed or full-sandbox run's side
/// effects) stays in the working copy and is NOT folded into this commit: a
/// file-scoped seal touches only those paths. An empty slice seals the whole `@`.
/// `^` folds the scoped paths into the prior sealed commit (git `--amend`
/// equivalent).
///
/// `branch` is the branch Cairn owns in this workspace, and it is a PARAMETER
/// rather than a marker read because ownership has to be settled before whatever
/// produced these changes ran. `Some` means Cairn owns it, so advancing that
/// branch's bookmark to the sealed commit and exporting it to the project's git
/// are part of this operation's contract, and a failure at either step rolls the
/// seal back rather than reporting a commit the branch does not carry. `None`
/// means the checkout is not Cairn's, so the seal commits locally and publishes
/// nothing. Returns the sealed commit id.
///
/// Both answers are load-bearing and neither is safe to re-derive here. Deciding
/// `None` late would report a successful commit while silently skipping the
/// publication its caller was told happened; deciding `Some` late would publish
/// to a branch the caller never owned. [`seal`] resolves it from the marker for
/// fixtures; the barrier path passes what it captured at preflight.
pub(crate) fn seal_paths(
    jj: &JjEnv,
    ws: &Path,
    msg: &str,
    author: Option<&GitAuthor>,
    paths: &[&str],
    branch: Option<&str>,
) -> Result<CommitResult, String> {
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
            .filter(|b| branch != Some(b.as_str()))
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
    if let Some(branch) = branch {
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
            if branch_has_conflict(jj, ws, branch)? {
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
        scoped_dirty(jj, ws, paths)?
    };

    // A post-commit integrity probe is necessarily fallible after mutation.
    // Snapshot the operation first so any command/parsing failure can restore the
    // exact pre-seal graph (including amend/squash), not merely report an error
    // while leaving a hidden commit behind.
    let pre_seal_operation = operation_id(jj, ws)?;
    jj.run(ws, &argref, "jj commit")?;
    let (sha, lost) = match sealed_commit_probe(jj, ws, pre_dirty) {
        Ok(probe) => probe,
        Err(error) => {
            restore_operation(jj, ws, &pre_seal_operation).map_err(|restore_error| {
                format!(
                    "sealed commit integrity probe failed ({error}); restoring the pre-seal operation also failed ({restore_error})"
                )
            })?;
            return Err(format!(
                "sealed commit integrity probe failed; the pre-seal operation was restored: {error}"
            ));
        }
    };

    // Detection backstop: a concurrent store advance can reset `@` out from under
    // the commit so `jj commit` succeeds but seals an EMPTY or DIVERGENT commit —
    // silent data loss otherwise reported as a real sha. Check only on a real
    // commit (the amend path is excluded above via `pre_dirty`/`msg`). On the
    // anomaly, back the bad commit out so `@` returns to its pre-seal parent and a
    // retry lands cleanly, then return the typed, recoverable lost-seal error. The
    // bookmark has NOT moved yet (that runs only on the clean path below), so
    // `jj abandon @-` reparents `@` onto the original parent and drops the commit
    // without stranding the bookmark on a twin.
    if msg != "^" && lost {
        if let Err(e) = jj.run(ws, &["abandon", "@-"], "jj abandon lost seal") {
            log::warn!("failed to back out lost-seal commit (still reporting the loss): {e}");
        }
        return Err(LOST_SEAL_MSG.to_string());
    }
    // Cairn owns `branch` in this checkout, so landing the sealed commit ON that
    // branch is what the caller asked for, not incidental cleanup. Both steps
    // therefore FAIL CLOSED, and a failure rolls the seal back to the pre-seal
    // operation — the same invariant the integrity probe above keeps, that a
    // failed seal leaves no orphan commit behind.
    if let Some(branch) = branch {
        if let Err(error) = publish_sealed_commit(jj, ws, branch) {
            restore_operation(jj, ws, &pre_seal_operation).map_err(|restore_error| format!(
                "commit {sha} was sealed locally but remains unpublished on `{branch}` ({error}); \
                 restoring the pre-seal operation also failed ({restore_error}) — this workspace now \
                 carries a commit its branch does not"
            ))?;
            return Err(format!(
                "commit {sha} was sealed locally but remains unpublished on `{branch}`, so the seal \
                 was rolled back: {error}"
            ));
        }
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
fn seal_is_fast_forward(jj: &JjEnv, ws: &Path, branch: &str) -> Result<bool, String> {
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

/// Land a sealed commit on the branch this workspace owns: advance the branch's
/// bookmark to `@-`, then PROVE the advance reached the project's git ref.
///
/// Both steps publish: external state reads the git ref (a child workspace cut
/// from it, a push, GitHub's view of a PR head), so neither is best-effort
/// cleanup. The export in particular cannot be discarded — jj reports a refused
/// ref on stderr and still exits 0, and the refusal does NOT self-heal on the
/// next seal, so a swallowed failure is a branch that silently stops carrying
/// the work every later caller is told it carries.
fn publish_sealed_commit(jj: &JjEnv, ws: &Path, branch: &str) -> Result<(), String> {
    jj.run(
        ws,
        &["bookmark", "set", branch, "-r", "@-"],
        "jj bookmark set",
    )?;
    export_bookmark_advance(jj, ws, false, branch, "jj git export")
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
pub(crate) fn discard(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    match jj.run(ws, &["restore"], "jj restore") {
        Ok(_) => Ok(()),
        Err(stale) if is_stale_error(&stale) => {
            // update-stale advances `@` and discards the loose edits → clean.
            update_stale(jj, ws)?;
            // The now-unblocked restore guarantees `@` == parent, and its result
            // is the verdict rather than a belt-and-braces afterthought. It was
            // discarded with `let _`, so this arm returned `Ok` unconditionally:
            // if the recovery did not take, the commit barrier went on to tell
            // the agent its worktree had been restored to HEAD when it had not.
            // The arm is reachable without genuine jj-staleness — `is_stale_error`
            // also matches the seal path's own "behind its branch tip" — and in
            // that state `update-stale` correctly reports the working copy is not
            // stale and changes nothing, which is precisely when the unchecked
            // restore mattered.
            jj.run(ws, &["restore"], "jj restore (post update-stale)")
                .map(|_| ())
                .map_err(|retry| {
                    format!(
                        "the working copy was stale ({stale}) and remained unreconciled after \
                         `jj workspace update-stale`: {retry}"
                    )
                })
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod probe_tests {
    use super::parse_sealed_commit_probe;

    #[test]
    fn malformed_sealed_commit_probe_is_rejected() {
        for malformed in [
            "",
            "sha|maybe|change",
            "sha|true|",
            "|false|change",
            "sha|false|change|extra",
        ] {
            assert!(
                parse_sealed_commit_probe(malformed).is_err(),
                "accepted {malformed:?}"
            );
        }
        assert_eq!(
            parse_sealed_commit_probe("abc|false|def").unwrap(),
            ("abc".into(), false, "def".into())
        );
    }
}
