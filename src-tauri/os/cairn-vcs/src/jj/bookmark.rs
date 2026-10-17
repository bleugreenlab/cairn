//! Bookmark / git-ref resolution, export, and publishing to origin.
use super::*;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(test)]
static FAIL_NEXT_LOCAL_BOOKMARKS_AT: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn fail_next_local_bookmarks_at() {
    FAIL_NEXT_LOCAL_BOOKMARKS_AT.store(true, Ordering::SeqCst);
}

/// Delete a provisional local bookmark without snapshotting any workspace.
pub fn delete_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    jj.run(
        store,
        &["bookmark", "delete", branch, "--ignore-working-copy"],
        "jj bookmark delete",
    )
    .map(|_| ())
}
/// Query only candidate bookmark names in one structured jj invocation. An
/// empty candidate set is resolved without spawning jj.
pub fn query_local_bookmarks(
    jj: &JjEnv,
    store: &Path,
    candidates: &[String],
) -> Result<std::collections::HashSet<String>, String> {
    if candidates.is_empty() {
        return Ok(std::collections::HashSet::new());
    }
    let revset = candidates
        .iter()
        .map(|name| format!("bookmarks(exact:{name:?})"))
        .collect::<Vec<_>>()
        .join(" | ");
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            &revset,
            "--no-graph",
            "-T",
            "local_bookmarks.map(|b| b.name()).join(\"\\n\") ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (candidate local bookmarks)",
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// Create a new local bookmark at an exact revision without snapshotting any
/// workspace. Fails if the bookmark already exists.
pub fn create_bookmark_at(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    revision: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "create",
            branch,
            "-r",
            revision,
            "--ignore-working-copy",
        ],
        "jj bookmark create",
    )
    .map(|_| ())
}

/// Move an existing bookmark forward to an exact revision without snapshotting
/// a workspace. Normal jj fast-forward safeguards remain in force.
pub fn set_bookmark_at(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    revision: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            revision,
            "--ignore-working-copy",
        ],
        "jj bookmark set",
    )
    .map(|_| ())
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
    #[cfg(test)]
    if FAIL_NEXT_LOCAL_BOOKMARKS_AT.swap(false, Ordering::SeqCst) {
        return Err("injected local bookmark read failure".to_string());
    }
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

/// Resolve a revset that must name at most one commit, keeping the three
/// outcomes DISTINCT.
///
/// - `Ok(Some(commit))` — it resolved to exactly one commit.
/// - `Ok(None)` — it resolved to nothing. jj exits 0 with empty output here, so
///   this is a real answer: the bookmark is absent.
/// - `Err(_)` — either jj failed (a nonexistent `<branch>@origin` exits 1, for
///   instance), or the revset named MORE than one commit, which is the
///   conflicted-bookmark-name state.
///
/// [`revset_commit`] collapses all three into an `Option`, which is right for a
/// caller asking "is this bookmark here?" and wrong for one asserting a
/// postcondition it just established. A site that has moved a bookmark and must
/// prove the move reached git needs to tell "absent" from "could not tell" — only
/// one of those means there is nothing to publish, and treating the other as
/// benign is how an unverified publication passes for a verified one.
///
/// The template emits one id PER LINE, and that separator is load-bearing rather
/// than cosmetic: a conflicted bookmark name resolves to several commits, and a
/// bare `commit_id` template CONCATENATES them, so a resolver that promises one
/// commit would otherwise hand back an 80-character string that is not a commit
/// id and let a caller pass it to jj as a revision.
pub fn revset_commit_checked(
    jj: &JjEnv,
    store: &Path,
    revset: &str,
) -> Result<Option<String>, String> {
    // Resolving a bookmark is a pure read of the branch graph, so it never wants
    // the working copy. Passing `--ignore-working-copy` unconditionally is both
    // the store-wide convention and the only way this stays truthful on a stale
    // workspace. It replaces a resolve-then-retry-on-`is_stale_error` dance that
    // paid for two subprocesses to reach the same answer, and whose failure mode
    // was worse than its cost: any error other than staleness degraded into
    // `None`, so a conflicted `main` read as "the bookmark does not exist".
    // As a bonus, a read no longer mints a snapshot of an agent's workspace as a
    // side effect at the call sites that pass one as `store`.
    let output = jj.run(
        store,
        &[
            "log",
            "-r",
            revset,
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log revset commit",
    )?;
    let mut commits = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let Some(first) = commits.next() else {
        return Ok(None);
    };
    if commits.next().is_some() {
        return Err(format!(
            "jj revset `{revset}` resolved to more than one commit, so it does not name a single \
             revision (a conflicted bookmark name is the usual cause)"
        ));
    }
    Ok(Some(first.to_string()))
}

/// Resolve a bookmark to exactly one commit, keeping "absent" distinct from
/// "could not tell". See [`revset_commit_checked`].
pub fn bookmark_commit_checked(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<Option<String>, String> {
    revset_commit_checked(jj, store, &format!("bookmarks(exact:{branch:?})"))
}

/// Resolve a single revset to a commit id over the shared store, or `None` when
/// it does not resolve. Used for both exact local bookmarks and remote-tracking
/// bookmarks such as `main@origin`.
///
/// The permissive form, for callers legitimately ASKING whether something is
/// there — a `<branch>@origin` that does not exist is the ordinary shape of a
/// local-only project, not an anomaly, so this stays quiet. A caller that has
/// just established a postcondition and needs it PROVEN must use
/// [`revset_commit_checked`] or [`bookmark_commit_checked`] instead, so a
/// resolver failure cannot masquerade as an absent bookmark.
pub fn revset_commit(jj: &JjEnv, store: &Path, revset: &str) -> Option<String> {
    match revset_commit_checked(jj, store, revset) {
        Ok(commit) => commit,
        Err(error) => {
            log::debug!("jj revset `{revset}` did not resolve to a single commit: {error}");
            None
        }
    }
}

/// Whether the LOCAL bookmark named `branch` is in jj's conflicted-name state:
/// several competing targets recorded for one name, because the local side and a
/// tracked remote (or the backing git ref) both moved from a common base.
///
/// This state is not a variant of divergent change-ids (which
/// [`collapse_divergent_bookmark`] handles) and it is not detectable from a
/// commit id, because the name resolves to several. It IS the state that makes
/// every `main`-resolving verb fail with `Name \`main\` is conflicted` — job
/// spawn included — so it is probed structurally rather than inferred from an
/// error string. `remote` scopes the template to the local entry; jj lists the
/// remote-tracking entries under the same name.
pub(crate) fn bookmark_name_is_conflicted(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<bool, String> {
    if branch.is_empty() {
        return Ok(false);
    }
    let out = jj.run(
        store,
        &[
            "bookmark",
            "list",
            branch,
            "--ignore-working-copy",
            "-T",
            "if(!remote && self.conflict(), name ++ \"\\n\", \"\")",
        ],
        "jj bookmark list (conflicted-name probe)",
    )?;
    Ok(out.lines().any(|line| line.trim() == branch))
}
/// What one pass of [`reconcile_tracked_bookmark`] found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookmarkReconciliation {
    /// No `<branch>@origin` in the store, so origin is not an authority to
    /// reconcile against (a local-only project, or a branch never pushed).
    NoRemote,
    /// The local bookmark already equals origin's tip. The common case.
    Unchanged { commit: String },
    /// The local bookmark strictly DESCENDS from origin's tip: it holds work
    /// origin has not seen. Never moved — see [`reconcile_tracked_bookmark`].
    AheadOfOrigin { local: String, remote: String },
    /// The local bookmark disagreed with origin and was moved onto origin's tip.
    /// `from` is `None` when the bookmark was absent or its name was conflicted,
    /// in which case it had no single prior target to report.
    Repaired { from: Option<String>, to: String },
}

/// Bring the store's local bookmark for a REMOTE-AUTHORITATIVE branch back into
/// agreement with origin, unconditionally and without a human in the loop.
///
/// This is the step Cairn did not have. `jj git import` existed only inside
/// store provisioning, reached from the base-advance paths only after they had
/// already returned early when no sibling needed rebasing — so the overwhelmingly
/// common case (a PR merges, nothing else is in flight) imported nothing and
/// reconciled nothing. The tracked bookmark was left holding a target that no
/// longer agreed with its remote, and the next operation touching it failed with
/// `Name \`main\` is conflicted`, which kills every verb that resolves that name
/// — including provisioning a new job. That is why a bare `jj git import` at the
/// console repaired it by hand, over and over.
///
/// The sequence is the one measured to work on jj 0.42, in this order:
///
/// 1. `jj git import`, so the store observes where the backing git refs really are.
///    This import names `branch` as origin-authoritative, so a backward move of
///    origin's copy is observed rather than held off as a stale observation.
/// 2. Compare the local bookmark against `<branch>@origin`.
/// 3. `jj bookmark set -r <branch>@origin --allow-backwards` when they disagree.
/// 4. `jj git export`, VERIFIED, so the backing git ref follows the repair.
///
/// Step 4 is not optional. Leaving the git ref where it was recreates precisely
/// the local-versus-`@git` divergence that the next import turns back into a
/// conflicted name — the cycle this function exists to break.
///
/// # Only for a remote-authoritative branch
///
/// Step 3 can move a bookmark BACKWARDS, which is only ever correct where origin
/// is the sole authority: the project's configured default or integration
/// branch. An agent branch legitimately holds sealed work origin has not seen,
/// so callers must pass a branch they know to be remote-authoritative rather
/// than letting this infer it. Even then, a local bookmark that strictly
/// descends from origin is reported as [`BookmarkReconciliation::AheadOfOrigin`]
/// and left alone: for the default branch that should not happen (Cairn's own
/// merges push), so it is a signal to surface, not a state to overwrite.
///
/// The caller MUST hold the per-store lock — this reads and then writes a
/// bookmark, and an interleaved writer between the two would be repaired away.
pub fn reconcile_tracked_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<BookmarkReconciliation, String> {
    if branch.is_empty() {
        return Ok(BookmarkReconciliation::NoRemote);
    }
    import_git_with_remote_authority(jj, store, branch)?;

    let remote_ref = format!("{branch}@origin");
    let Some(remote) = revset_commit(jj, store, &remote_ref) else {
        log::debug!(
            "jj reconcile: `{remote_ref}` does not resolve in the store; nothing to reconcile"
        );
        return Ok(BookmarkReconciliation::NoRemote);
    };

    let conflicted = bookmark_name_is_conflicted(jj, store, branch)?;
    let local = (!conflicted)
        .then(|| bookmark_commit(jj, store, branch))
        .flatten();

    if local.as_deref() == Some(remote.as_str()) {
        // Agreement on the jj side is not the whole answer: the import may have
        // fast-forwarded the bookmark onto origin's tip while the backing git ref
        // stayed where it was. Leaving that gap open is what lets the two views
        // drift apart again, so the export is owed here exactly as it is after a
        // repair.
        hold_git_ref_to_bookmark(jj, store, branch, &remote)?;
        return Ok(BookmarkReconciliation::Unchanged { commit: remote });
    }
    if let Some(local) = local.as_deref() {
        if revset_descends_from(jj, store, local, &remote) {
            log::warn!(
                "jj reconcile: store bookmark `{branch}` ({local}) is AHEAD of {remote_ref} \
                 ({remote}); leaving it alone. The default branch holding unpushed local commits \
                 is unexpected — Cairn's own merges push."
            );
            // Not moved, but still held to the same rule: git must agree with
            // whatever the store says this bookmark is.
            hold_git_ref_to_bookmark(jj, store, branch, local)?;
            return Ok(BookmarkReconciliation::AheadOfOrigin {
                local: local.to_string(),
                remote,
            });
        }
        report_bookmark_misposition(jj, store, branch, local, &remote);
    } else if conflicted {
        log::warn!(
            "jj reconcile: store bookmark `{branch}` name is CONFLICTED; repairing onto \
             {remote_ref} ({remote})"
        );
    }

    set_bookmark_backwards(
        jj,
        store,
        branch,
        &remote_ref,
        "jj reconcile: repair onto origin",
    )?;
    hold_git_ref_to_bookmark(jj, store, branch, &remote)?;

    // Re-verify from scratch: the repair must leave a single, unconflicted target
    // equal to origin, or the store is still in a state that fails a verb.
    if bookmark_name_is_conflicted(jj, store, branch)? {
        import_git_with_remote_authority(jj, store, branch)?;
        set_bookmark_backwards(
            jj,
            store,
            branch,
            &remote_ref,
            "jj reconcile: second repair onto origin",
        )?;
    }
    let after = bookmark_commit(jj, store, branch);
    if after.as_deref() != Some(remote.as_str()) {
        return Err(format!(
            "jj reconcile: bookmark `{branch}` did not converge on {remote_ref}: it is at {} after \
             repair, expected {remote}",
            after.as_deref().unwrap_or("<unresolved/conflicted>")
        ));
    }
    log::info!(
        "jj reconcile: store bookmark `{branch}` reconciled onto {remote_ref} ({remote}) from {}",
        local.as_deref().unwrap_or("<unresolved/conflicted>")
    );
    Ok(BookmarkReconciliation::Repaired {
        from: local,
        to: remote,
    })
}

/// Outcome of converging a store-authoritative managed branch after origin was
/// rewritten. The caller must have fetched origin and must hold the store lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedBranchConvergence {
    Unchanged {
        commit: String,
    },
    AheadOfOrigin {
        local: String,
        remote: String,
    },
    Rebased {
        from: String,
        to: String,
        frontier: String,
    },
}

/// Converge a managed branch with a freshly fetched origin copy, ending with
/// that copy TRACKED so the next push is accepted.
///
/// Equal tips and a local tip that descends from origin's are already converged.
/// A rewritten origin is handled by rebasing only the unpublished suffix onto
/// it; the frontier is selected by jj change identity, not description or tree,
/// and must be unique in the local ancestry. Only once one of those establishes
/// that origin's copy is this branch's own history is it tracked: every refusal
/// (no change-id twin, an ambiguous frontier, external descendants, a conflict)
/// returns first, so a remote with foreign history is never tracked and never
/// overwritten. Tracking is what heals a copy the fetch imported untracked — the
/// state in which jj refuses every push of the branch. This is intentionally
/// separate from `reconcile_tracked_bookmark`: origin does not own an agent branch.
pub fn converge_managed_branch_after_remote_rewrite(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<ManagedBranchConvergence, String> {
    import_git(jj, store)?;
    // The shape a stale origin observation leaves behind settles without
    // choosing, so it is tried before falling back to the exported target.
    if bookmark_name_is_conflicted(jj, store, branch)? {
        match settle_rewritten_base_with_child(jj, store, branch)? {
            RewrittenBaseSettlement::Settled {
                tip,
                rewrite,
                child,
            } => {
                log::info!(
                    "stale publication recovery: settled conflicted `{branch}` on {tip} before \
                     converging"
                );
                // Origin holding exactly the child just replayed is the
                // settled shape itself: `tip` is that child's own commits on
                // the rewrite, so it supersedes origin's copy rather than
                // racing it. Track that copy so the next push is a sideways
                // move leased on it.
                let remote = revset_commit_checked(jj, store, &format!("{branch}@origin"))?;
                if remote.as_deref() == Some(child.as_str()) {
                    track_bookmark(jj, store, branch)?;
                    hold_git_ref_to_bookmark(jj, store, branch, &tip)?;
                    return Ok(ManagedBranchConvergence::Rebased {
                        from: child,
                        to: tip,
                        frontier: rewrite,
                    });
                }
            }
            RewrittenBaseSettlement::Declined { reason } => log::debug!(
                "stale publication recovery: `{branch}` is conflicted but not a rewritten base \
                 with a child ({reason}); falling back to its exported target"
            ),
        }
    }
    if bookmark_name_is_conflicted(jj, store, branch)? {
        let checkout = super::export::resolve_backing_checkout(store).ok_or_else(|| {
            format!("stale publication recovery refused: bookmark `{branch}` is conflicted and its backing checkout cannot be resolved")
        })?;
        let output = cairn_platform::env::git()
            .args(["rev-parse", &format!("refs/heads/{branch}")])
            .current_dir(checkout)
            .output()
            .map_err(|error| {
                format!("stale publication recovery: resolve exported `{branch}` ref: {error}")
            })?;
        if !output.status.success() {
            return Err(format!("stale publication recovery refused: bookmark `{branch}` is conflicted and its exported ref is unreadable: {}", String::from_utf8_lossy(&output.stderr).trim()));
        }
        let exported = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let targets = conflicted_bookmark_targets(jj, store, branch)?;
        if targets.iter().filter(|target| *target == &exported).count() != 1 {
            return Err(format!("stale publication recovery refused: bookmark `{branch}` is conflicted and exported commit {exported} is not a unique recorded local target ({})", targets.join(", ")));
        }
        set_bookmark_backwards(
            jj,
            store,
            branch,
            &exported,
            "stale publication recovery: retain verified exported local target",
        )?;
    }
    let local = bookmark_commit_checked(jj, store, branch)?.ok_or_else(|| {
        format!("stale publication recovery refused: local bookmark `{branch}` is missing")
    })?;
    let remote_ref = format!("{branch}@origin");
    let remote = revset_commit_checked(jj, store, &remote_ref)?.ok_or_else(|| {
        format!("stale publication recovery refused: remote bookmark `{remote_ref}` is missing")
    })?;
    if local == remote {
        track_bookmark(jj, store, branch)?;
        hold_git_ref_to_bookmark(jj, store, branch, &local)?;
        return Ok(ManagedBranchConvergence::Unchanged { commit: local });
    }
    if revset_descends_from(jj, store, &local, &remote) {
        track_bookmark(jj, store, branch)?;
        hold_git_ref_to_bookmark(jj, store, branch, &local)?;
        return Ok(ManagedBranchConvergence::AheadOfOrigin { local, remote });
    }

    let remote_change = jj
        .run(
            store,
            &[
                "log",
                "-r",
                &remote,
                "--no-graph",
                "-T",
                "change_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "jj log (rewritten remote change id)",
        )?
        .trim()
        .to_string();
    let candidates = jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("::{local} & change_id({remote_change:?})"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (rewritten remote frontier twins)",
    )?;
    let twins = candidates
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let frontier = match twins.as_slice() {
        [frontier] => (*frontier).to_string(),
        [] => return Err(format!("stale publication recovery refused: rewritten remote `{remote_ref}` ({remote}) has no change-id twin in `{branch}` ancestry")),
        _ => return Err(format!("stale publication recovery refused: rewritten remote `{remote_ref}` has ambiguous change-id frontier `{remote_change}` ({} local twins: {})", twins.len(), twins.join(", "))),
    };
    if frontier == local {
        return Err("stale publication recovery refused: rewritten origin replaced the local tip and there is no unpublished suffix to preserve".to_string());
    }
    let suffix_roots = jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("children({frontier}) & ::{local}"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (unpublished suffix root)",
    )?;
    let roots = suffix_roots
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let suffix_root = match roots.as_slice() {
        [root] => *root,
        _ => return Err(format!("stale publication recovery refused: expected one unpublished suffix above frontier {frontier}, found {}", roots.len())),
    };
    // `jj rebase -s` rewrites the whole descendant subtree. In a shared store a
    // delegated child can fork from an unpublished commit, and rewriting it here
    // would move that child's bookmark without re-exporting or verifying its Git
    // ref. Keep this transaction branch-local: any bookmarked descendant outside
    // the publishing branch's own ancestry blocks recovery.
    let external_descendants = jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("({suffix_root}:: ~ ::{local}) & bookmarks()"),
            "--no-graph",
            "-T",
            "local_bookmarks.map(|bookmark| bookmark.name()).join(\",\") ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (stale publication external descendants)",
    )?;
    let external_bookmarks = external_descendants
        .lines()
        .flat_map(|line| line.split(','))
        .map(str::trim)
        .filter(|name| !name.is_empty() && *name != branch)
        .collect::<std::collections::BTreeSet<_>>();
    if !external_bookmarks.is_empty() {
        return Err(format!(
            "stale publication recovery refused: unpublished suffix of `{branch}` has managed descendant bookmark(s) outside its branch ancestry: {}",
            external_bookmarks.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }

    let suffix_revset = format!("{frontier}..{local}");
    let suffix_change_ids = jj.run(
        store,
        &[
            "log",
            "-r",
            &suffix_revset,
            "--reversed",
            "--no-graph",
            "-T",
            "change_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (unpublished suffix identities before recovery)",
    )?;
    let pre_tree = sealed_tree_hash_via_git(jj, store, &local)?;
    let pre_op = operation_id(jj, store)?;
    if let Err(error) = jj.run(
        store,
        &[
            "rebase",
            "-s",
            suffix_root,
            "-d",
            &remote,
            "--ignore-working-copy",
        ],
        "jj rebase (stale publication suffix)",
    ) {
        restore_operation(jj, store, &pre_op)?;
        return Err(format!(
            "stale publication recovery failed while rebasing unpublished suffix: {error}"
        ));
    }
    match branch_has_conflict(jj, store, branch) {
        Ok(false) => {}
        Ok(true) => {
            let paths = branch_conflicted_paths(jj, store, branch);
            restore_operation(jj, store, &pre_op).map_err(|restore| {
                format!("stale publication recovery found a conflict, then failed to restore operation {pre_op}: {restore}. The branch may still be rewritten; do not publish it")
            })?;
            hold_git_ref_to_bookmark(jj, store, branch, &local)?;
            return Err(format!("stale publication recovery refused: unpublished suffix conflicts with rewritten origin ({})", paths.join(", ")));
        }
        Err(error) => {
            restore_operation(jj, store, &pre_op).map_err(|restore| {
                format!("stale publication recovery could not inspect the rebased branch ({error}), then failed to restore operation {pre_op}: {restore}. The branch may still be rewritten; do not publish it")
            })?;
            hold_git_ref_to_bookmark(jj, store, branch, &local)?;
            return Err(format!("stale publication recovery could not verify the rebased branch and was rolled back: {error}"));
        }
    }
    let verify = || -> Result<String, String> {
        let to = bookmark_commit_checked(jj, store, branch)?.ok_or_else(|| {
            format!("stale publication recovery lost bookmark `{branch}` after rebase")
        })?;
        if !revset_descends_from(jj, store, &to, &remote) {
            return Err(format!("stale publication recovery verification failed: `{branch}` does not descend from fetched origin after rebase"));
        }
        let post_tree = sealed_tree_hash_via_git(jj, store, &to)?;
        if post_tree != pre_tree {
            return Err(format!("stale publication recovery verification failed: final tree changed from {pre_tree} to {post_tree}"));
        }
        let post_change_ids = jj.run(
            store,
            &[
                "log",
                "-r",
                &format!("{remote}..{to}"),
                "--reversed",
                "--no-graph",
                "-T",
                "change_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "jj log (unpublished suffix identities after recovery)",
        )?;
        if post_change_ids != suffix_change_ids {
            return Err("stale publication recovery verification failed: unpublished suffix change identities or order changed".to_string());
        }
        track_bookmark(jj, store, branch)?;
        hold_git_ref_to_bookmark(jj, store, branch, &to)?;
        Ok(to)
    };
    let to = match verify() {
        Ok(to) => to,
        Err(error) => {
            restore_operation(jj, store, &pre_op)?;
            hold_git_ref_to_bookmark(jj, store, branch, &local)?;
            return Err(error);
        }
    };
    Ok(ManagedBranchConvergence::Rebased {
        from: local,
        to,
        frontier,
    })
}

/// Who owns the truth for a branch, which decides how its name is repaired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchAuthority {
    /// The project's default/integration branch: origin is the sole authority,
    /// so the repair is a reconcile onto origin's tip and may move backwards.
    Origin,
    /// An agent branch: it legitimately holds sealed work origin has never seen,
    /// so the repair settles the name locally and never consults origin.
    Store,
}

/// Clear a conflicted branch NAME so the verbs that resolve it work again.
///
/// This is the single entry point the read, write, and run paths call when a
/// coordinate turns out to be conflicted, and it exists because that state used
/// to be terminal from inside a job: no run could hydrate a checkout, so an agent
/// could not even attempt the repair from where it stood. Nothing about a
/// conflicted name is unrepairable; it was only unreachable.
///
/// Returns the commit the name settled on, or `None` when there was nothing to
/// repair. The caller MUST hold the per-store lock.
pub fn repair_conflicted_branch_name(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    authority: BranchAuthority,
) -> Result<Option<String>, NameRepairError> {
    match authority {
        BranchAuthority::Origin => match reconcile_tracked_bookmark(jj, store, branch)? {
            BookmarkReconciliation::Unchanged { commit } => Ok(Some(commit)),
            BookmarkReconciliation::Repaired { to, .. } => Ok(Some(to)),
            BookmarkReconciliation::AheadOfOrigin { local, .. } => Ok(Some(local)),
            // Origin is not an authority here after all, so fall back to the
            // local settlement rather than leaving the name conflicted.
            BookmarkReconciliation::NoRemote => repair_conflicted_bookmark_name(jj, store, branch),
        },
        BranchAuthority::Store => repair_conflicted_bookmark_name(jj, store, branch),
    }
}

/// One target recorded in a conflicted bookmark name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookmarkTarget {
    pub commit: String,
    pub change: String,
    /// The first line of the commit's description.
    pub description: String,
}

/// A conflicted bookmark name as jj records it: the target both sides moved
/// away from (`removed`) and where each side moved it (`added`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictedBookmark {
    pub branch: String,
    pub removed: Vec<BookmarkTarget>,
    pub added: Vec<BookmarkTarget>,
}

/// Read a conflicted bookmark name's removed and added targets without touching
/// the store. `None` when the name is not conflicted (or does not exist).
pub fn describe_conflicted_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<Option<ConflictedBookmark>, String> {
    if branch.is_empty() {
        return Ok(None);
    }
    let target = "\"\\t\" ++ c.commit_id() ++ \"\\t\" ++ c.change_id() ++ \"\\t\" ++ \
                  c.description().first_line() ++ \"\\n\"";
    let template = format!(
        "if(!remote && name == {branch:?} && self.conflict(), \
         removed_targets.map(|c| \"-\" ++ {target}).join(\"\") ++ \
         added_targets.map(|c| \"+\" ++ {target}).join(\"\"), \"\")"
    );
    let out = jj.run(
        store,
        &[
            "bookmark",
            "list",
            branch,
            "--ignore-working-copy",
            "-T",
            &template,
        ],
        "jj bookmark list (conflicted bookmark targets)",
    )?;
    let mut conflict = ConflictedBookmark {
        branch: branch.to_string(),
        removed: Vec::new(),
        added: Vec::new(),
    };
    for line in out.lines() {
        let mut fields = line.splitn(4, '\t');
        let (Some(side), Some(commit), Some(change)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let target = BookmarkTarget {
            commit: commit.trim().to_string(),
            change: change.trim().to_string(),
            description: fields.next().unwrap_or_default().trim().to_string(),
        };
        match side.trim() {
            "-" => conflict.removed.push(target),
            "+" => conflict.added.push(target),
            _ => {}
        }
    }
    Ok((!conflict.added.is_empty()).then_some(conflict))
}

/// The phrase every conflicted-name refusal carries, so the reconcile failure
/// classifier matches the refusal itself rather than whatever jj said later.
pub const CONFLICTED_NAME_REFUSAL: &str = "name is conflicted";

/// Why a conflicted branch name was left conflicted, and the exact commands a
/// maintainer can run against the store to combine its versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairRefusal {
    pub branch: String,
    pub conflict: ConflictedBookmark,
    /// Why automatic settlement declined, in a sentence.
    pub reason: String,
    /// Shell lines for a maintainer, in order. Lines starting `# ` annotate the
    /// command that follows them.
    pub commands: Vec<String>,
}

impl std::fmt::Display for RepairRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "branch `{}` {CONFLICTED_NAME_REFUSAL}: it has unrelated versions of its history in \
             the store ({}), so there is no single tip to settle it on ({})",
            self.branch,
            self.conflict
                .added
                .iter()
                .map(|target| target.commit.as_str())
                .collect::<Vec<_>>()
                .join(" and "),
            self.reason
        )
    }
}

/// A conflicted-name repair that did not settle the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameRepairError {
    /// The store declined to choose between histories. Never retried into a
    /// different answer by the store itself; a maintainer has to combine them.
    Refused(Box<RepairRefusal>),
    /// jj or git failed while the repair was running.
    Failed(String),
}

impl NameRepairError {
    pub fn refusal(&self) -> Option<&RepairRefusal> {
        match self {
            Self::Refused(refusal) => Some(refusal),
            Self::Failed(_) => None,
        }
    }
}

impl std::fmt::Display for NameRepairError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => refusal.fmt(f),
            Self::Failed(error) => f.write_str(error),
        }
    }
}

impl From<String> for NameRepairError {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

/// What the next repair of a conflicted branch name will do, judged without
/// writing anything. Rendered on the branch's rebase page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictedNameOutlook {
    /// The next replay or branch read will try to settle the name; `how` says
    /// how. Not a promise: a replay that conflicts is still refused, so
    /// `commands` carries the by-hand combination for that case (empty where
    /// the settlement cannot fail, because one version contains the rest).
    Settleable { how: String, commands: Vec<String> },
    /// Automatic settlement will refuse; the refusal carries the commands.
    Refused(Box<RepairRefusal>),
}

/// [`describe_conflicted_bookmark`] plus what the repair would make of it. Reads
/// only; never imports, rebases, or moves a bookmark.
pub fn assess_conflicted_bookmark_name(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<Option<(ConflictedBookmark, ConflictedNameOutlook)>, String> {
    let Some(conflict) = describe_conflicted_bookmark(jj, store, branch)? else {
        return Ok(None);
    };
    let added = conflict
        .added
        .iter()
        .map(|target| target.commit.clone())
        .collect::<Vec<_>>();
    if let Some(winner) = target_containing_the_rest(jj, store, &added) {
        let how = format!("settle on `{winner}`, which contains every other version");
        let commands = Vec::new();
        return Ok(Some((
            conflict,
            ConflictedNameOutlook::Settleable { how, commands },
        )));
    }
    let outlook = match rewritten_base_with_child(jj, store, &conflict)? {
        // A replay git already expects to conflict will be refused when it is
        // tried, so it is reported as the refusal it will be.
        Shape::Matches(shape)
            if predict_replay_conflict(store, &shape.removed, &shape.rewrite, &shape.child)
                == Some(true) =>
        {
            ConflictedNameOutlook::Refused(Box::new(repair_refusal(
                jj,
                store,
                conflict.clone(),
                format!(
                    "replaying `{}`'s commits onto `{}` is expected to conflict",
                    shape.child, shape.rewrite
                ),
            )))
        }
        Shape::Matches(shape) => ConflictedNameOutlook::Settleable {
            how: format!(
                "replay {} commit(s) of `{}` onto `{}`, the rewrite of their old parent `{}`",
                shape.suffix.len(),
                shape.child,
                shape.rewrite,
                shape.removed
            ),
            commands: repair_refusal(jj, store, conflict.clone(), String::new()).commands,
        },
        Shape::Declined(reason) => ConflictedNameOutlook::Refused(Box::new(repair_refusal(
            jj,
            store,
            conflict.clone(),
            reason,
        ))),
    };
    Ok(Some((conflict, outlook)))
}

/// The first of `targets` that descends from every other, if any.
fn target_containing_the_rest(jj: &JjEnv, store: &Path, targets: &[String]) -> Option<String> {
    targets
        .iter()
        .find(|candidate| {
            targets.iter().all(|other| {
                other == *candidate || revset_descends_from(jj, store, candidate, other)
            })
        })
        .cloned()
}

/// A conflicted name holding a rewritten base `rewrite` (the removed target's
/// own rewrite, same change id) beside `child`, which was built on the removed
/// target before it was rewritten.
struct RewrittenBaseWithChild {
    removed: String,
    rewrite: String,
    child: String,
    child_change: String,
    /// `removed..child`, the commits a settlement replays.
    suffix: Vec<String>,
}

enum Shape {
    Matches(RewrittenBaseWithChild),
    Declined(String),
}

/// Prove the rewritten-base-plus-child shape, reading only.
///
/// That shape arises when one side rewrote the branch's commits (a base-advance
/// replay) while the other side still holds a commit built on their pre-rewrite
/// form. Replaying the child's own commits onto the rewrite keeps both: nothing
/// either side holds is left behind, so it is a convergence rather than a choice.
/// Every precondition that would make it a choice declines instead.
fn rewritten_base_with_child(
    jj: &JjEnv,
    store: &Path,
    conflict: &ConflictedBookmark,
) -> Result<Shape, String> {
    let ([removed], [first, second]) = (conflict.removed.as_slice(), conflict.added.as_slice())
    else {
        return Ok(Shape::Declined(format!(
            "its versions are not one rewritten commit and one child of that commit's earlier \
             form ({} removed, {} added)",
            conflict.removed.len(),
            conflict.added.len()
        )));
    };
    let is_rewrite = |target: &BookmarkTarget| {
        target.change == removed.change && target.commit != removed.commit
    };
    let (rewrite, child) = match (is_rewrite(first), is_rewrite(second)) {
        (true, false) => (first, second),
        (false, true) => (second, first),
        _ => {
            return Ok(Shape::Declined(
                "neither version is a rewrite of the commit both moved away from".to_string(),
            ))
        }
    };
    if !revset_descends_from(jj, store, &child.commit, &removed.commit) {
        return Ok(Shape::Declined(format!(
            "`{}` was not built on `{}`, the commit both versions moved away from",
            child.commit, removed.commit
        )));
    }
    if revset_descends_from(jj, store, &child.commit, &rewrite.commit)
        || revset_descends_from(jj, store, &rewrite.commit, &child.commit)
    {
        return Ok(Shape::Declined(format!(
            "`{}` and `{}` are already in one line of history",
            child.commit, rewrite.commit
        )));
    }
    let suffix_revset = format!("{}..{}", removed.commit, child.commit);
    let suffix = log_lines(jj, store, &suffix_revset, "commit_id")?;
    if suffix.is_empty() {
        return Ok(Shape::Declined(format!(
            "`{}` adds no commits over `{}`",
            child.commit, removed.commit
        )));
    }
    if super::conflict::revset_has_conflict(jj, store, &suffix_revset)? {
        return Ok(Shape::Declined(format!(
            "commits between `{}` and `{}` already carry a conflict",
            removed.commit, child.commit
        )));
    }
    let suffix_changes = log_lines(jj, store, &suffix_revset, "change_id")?;
    let carried = suffix_changes
        .iter()
        .map(|change| format!("change_id({change})"))
        .collect::<Vec<_>>()
        .join(" | ");
    if !log_lines(
        jj,
        store,
        &format!("::{} & ({carried})", rewrite.commit),
        "commit_id",
    )?
    .is_empty()
    {
        return Ok(Shape::Declined(format!(
            "`{}` already carries some of `{}`'s commits",
            rewrite.commit, child.commit
        )));
    }
    let built_on = log_lines(
        jj,
        store,
        &format!("descendants({suffix_revset}) ~ ({suffix_revset})"),
        "commit_id",
    )?;
    if !built_on.is_empty() {
        return Ok(Shape::Declined(format!(
            "other work is built on `{}`'s commits ({}), and replaying them would move it",
            child.commit,
            built_on.join(", ")
        )));
    }
    Ok(Shape::Matches(RewrittenBaseWithChild {
        removed: removed.commit.clone(),
        rewrite: rewrite.commit.clone(),
        child: child.commit.clone(),
        child_change: child.change.clone(),
        suffix,
    }))
}

fn log_lines(jj: &JjEnv, store: &Path, revset: &str, field: &str) -> Result<Vec<String>, String> {
    Ok(jj
        .run(
            store,
            &[
                "log",
                "-r",
                revset,
                "--no-graph",
                "-T",
                &format!("{field} ++ \"\\n\""),
                "--ignore-working-copy",
            ],
            "jj log (conflicted name settlement)",
        )?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// What [`settle_rewritten_base_with_child`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewrittenBaseSettlement {
    /// The name now holds `tip`: `child`'s own commits replayed onto
    /// `rewrite`, the rewritten base.
    Settled {
        tip: String,
        rewrite: String,
        child: String,
    },
    /// Nothing was changed (or the trial replay was rolled back), for `reason`.
    Declined { reason: String },
}

/// Settle a conflicted name holding a rewritten base and a child of its
/// pre-rewrite twin by replaying the child's own commits onto the rewrite.
///
/// The shape is the one a stale origin observation leaves behind when a
/// base-advance replay lands in between: the replay rewrote the commit the
/// bookmark held (`R` to `A`) while the branch's newest commit `C`, built on
/// `R`, came back from origin. Both are real work, and `A` plus `C`'s own commits
/// is the history that keeps both. A replay that conflicts is rolled back and
/// declined: resolving it is a choice, and this never makes one.
///
/// The caller MUST hold the per-store lock.
pub fn settle_rewritten_base_with_child(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<RewrittenBaseSettlement, String> {
    let Some(conflict) = describe_conflicted_bookmark(jj, store, branch)? else {
        return Ok(RewrittenBaseSettlement::Declined {
            reason: "the name is not conflicted".to_string(),
        });
    };
    let shape = match rewritten_base_with_child(jj, store, &conflict)? {
        Shape::Matches(shape) => shape,
        Shape::Declined(reason) => return Ok(RewrittenBaseSettlement::Declined { reason }),
    };
    let RewrittenBaseWithChild {
        removed,
        rewrite,
        child,
        child_change,
        suffix,
    } = shape;

    let pre_op = operation_id(jj, store)?;
    if let Err(error) = jj.run(
        store,
        &[
            "rebase",
            "-r",
            &format!("{removed}..{child}"),
            "-d",
            &rewrite,
            "--ignore-working-copy",
        ],
        "jj rebase (settle rewritten base with child)",
    ) {
        restore_operation(jj, store, &pre_op)?;
        return Err(error);
    }
    let replayed = format!("{rewrite}:: & change_id({child_change})");
    let tip = match revset_commit_checked(jj, store, &replayed) {
        Ok(Some(tip)) => tip,
        Ok(None) => {
            restore_operation(jj, store, &pre_op)?;
            return Err(format!(
                "replaying `{child}` onto `{rewrite}` left no single commit at `{replayed}`"
            ));
        }
        Err(error) => {
            restore_operation(jj, store, &pre_op)?;
            return Err(error);
        }
    };
    match super::conflict::revset_has_conflict(jj, store, &format!("{rewrite}..{tip}")) {
        Ok(false) => {}
        Ok(true) => {
            restore_operation(jj, store, &pre_op)?;
            return Ok(RewrittenBaseSettlement::Declined {
                reason: format!("replaying `{child}`'s commits onto `{rewrite}` conflicts"),
            });
        }
        Err(error) => {
            restore_operation(jj, store, &pre_op)?;
            return Err(error);
        }
    }
    set_bookmark_backwards(
        jj,
        store,
        branch,
        &tip,
        "jj repair: settle rewritten base with child",
    )?;
    hold_git_ref_to_bookmark(jj, store, branch, &tip)?;
    if bookmark_name_is_conflicted(jj, store, branch)? {
        return Err(format!(
            "branch `{branch}` is still conflicted after replaying `{child}` onto `{rewrite}` as \
             `{tip}`"
        ));
    }
    log::info!(
        "jj repair: bookmark `{branch}` held a rewritten base ({removed}→{rewrite}) and a child of \
         its pre-rewrite twin ({child}); replayed {} commit(s) onto {rewrite} as {tip}",
        suffix.len()
    );
    Ok(RewrittenBaseSettlement::Settled {
        tip,
        rewrite,
        child,
    })
}

/// Build the refusal for a conflicted name, with a maintainer's commands.
///
/// The commands only ever COMBINE versions: for each ordering, keep one as the
/// base and replay the other's own commits onto it. Nothing here keeps one side
/// and drops the other, because that is exactly the silent loss a refusal
/// exists to prevent.
fn repair_refusal(
    jj: &JjEnv,
    store: &Path,
    conflict: ConflictedBookmark,
    reason: String,
) -> RepairRefusal {
    let commands = maintainer_commands(store, &conflict, |keep, replay, fork| {
        let fork = match fork {
            Fork::Commit(commit) => Some(commit.to_string()),
            Fork::Point => revset_commit(jj, store, &format!("fork_point({keep}|{replay})")),
        }?;
        predict_replay_conflict(store, &fork, keep, replay)
    });
    RepairRefusal {
        branch: conflict.branch.clone(),
        conflict,
        reason,
        commands,
    }
}

/// Where a by-hand combination replays a version's own commits from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fork<'a> {
    /// From this commit: the removed target, when the kept version is its
    /// rewrite, so only the child's own commits move.
    Commit(&'a str),
    /// From the two versions' fork point.
    Point,
}

/// The maintainer's commands for a conflicted name: one inspection, then the
/// orderings that combine its versions without leaving any out.
///
/// Only ever COMBINES: keep one version as the base and replay the other's own
/// commits onto it, so nothing either side holds is left behind. There is no
/// keep-one-side command, because that is exactly the silent loss a refusal
/// exists to prevent. For the same reason a name with more than two versions
/// gets no mutating commands at all: a pairwise replay followed by a bookmark
/// move would drop every version outside the pair.
///
/// `predict(keep, replay, fork)` says whether that replay is expected to
/// conflict, for the annotation.
fn maintainer_commands(
    store: &Path,
    conflict: &ConflictedBookmark,
    predict: impl Fn(&str, &str, Fork<'_>) -> Option<bool>,
) -> Vec<String> {
    let jj_cmd = format!(
        "jj -R {} --ignore-working-copy",
        shell_quote(&store.to_string_lossy())
    );
    let branch = &conflict.branch;
    let removed = conflict
        .removed
        .iter()
        .map(|target| format!(" | {}", target.commit))
        .collect::<String>();
    let mut commands = vec![
        "# Inspect every version of the branch and the commit they moved away from".to_string(),
        format!(
            "{jj_cmd} log -r {}",
            shell_quote(&format!("bookmarks(exact:{branch:?}){removed}"))
        ),
    ];
    let [first, second] = conflict.added.as_slice() else {
        commands.push(format!(
            "# {} versions: combining them means replaying each onto the result of the last, \
             which no fixed command list here can describe safely. Combine them by hand, one at \
             a time, and move the branch only once every version is in its history.",
            conflict.added.len()
        ));
        return commands;
    };
    // When one version is the removed target's own rewrite, the other's
    // commits since the removed target are exactly its own work; replaying the
    // rewrite's ancestry instead would drag whatever it was rewritten onto.
    let rewritten = |target: &BookmarkTarget| match conflict.removed.as_slice() {
        [removed] => target.change == removed.change && target.commit != removed.commit,
        _ => false,
    };
    let orderings = match (
        rewritten(first),
        rewritten(second),
        conflict.removed.as_slice(),
    ) {
        (true, false, [removed]) => vec![(first, second, Fork::Commit(&removed.commit))],
        (false, true, [removed]) => vec![(second, first, Fork::Commit(&removed.commit))],
        _ => vec![(first, second, Fork::Point), (second, first, Fork::Point)],
    };
    for (keep, replay, fork) in orderings {
        let expectation = match predict(&keep.commit, &replay.commit, fork) {
            Some(true) => "expected to conflict",
            Some(false) => "expected to replay cleanly",
            None => "not predicted",
        };
        let from = match fork {
            Fork::Commit(commit) => commit.to_string(),
            Fork::Point => format!("fork_point({}|{})", keep.commit, replay.commit),
        };
        commands.push(format!(
            "# Keep {} as the base and replay {}'s own commits onto it ({expectation})",
            keep.commit, replay.commit
        ));
        commands.push(format!(
            "{jj_cmd} rebase -r {} -d {}",
            shell_quote(&format!("{from}..{}", replay.commit)),
            keep.commit
        ));
        commands.push(format!(
            "{jj_cmd} bookmark set {} --allow-backwards -r {}",
            shell_quote(branch),
            shell_quote(&format!("{}:: & change_id({})", keep.commit, replay.change))
        ));
    }
    commands
}

/// Whether replaying `tip`'s commits from `fork` onto `onto` is expected to
/// conflict, from git's three-way tree merge of the same three commits. A
/// prediction, not a proof: a multi-commit replay can conflict partway through
/// even where the end states merge. `None` when git could not say.
fn predict_replay_conflict(store: &Path, fork: &str, onto: &str, tip: &str) -> Option<bool> {
    let checkout = super::export::resolve_backing_checkout(store)?;
    let status = cairn_platform::env::git()
        .args([
            "merge-tree",
            "--write-tree",
            &format!("--merge-base={fork}"),
            onto,
            tip,
        ])
        .current_dir(checkout)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()?;
    match status.code() {
        Some(0) => Some(false),
        Some(1) => Some(true),
        _ => None,
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// The competing targets of a conflicted bookmark name, in store order.
///
/// `bookmarks(exact:...)` resolves to EVERY positive target of the conflicted
/// name, which is why the single-commit resolver returns nothing for it. Here
/// that plurality is the answer rather than the problem.
fn conflicted_bookmark_targets(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<Vec<String>, String> {
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("bookmarks(exact:{branch:?})"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (conflicted bookmark targets)",
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// Clear jj's conflicted-name state on a branch whose authority is LOCAL — an
/// agent branch, which legitimately holds sealed work origin has never seen and
/// so cannot be reconciled onto origin the way
/// [`reconcile_tracked_bookmark`] reconciles the default branch.
///
/// Returns the commit the name settled on, or `None` when it was never
/// conflicted.
///
/// # Why this exists
///
/// A conflicted name is not a variant of "the branch is broken": it is the same
/// local-versus-`@git` divergence the export freeze produces, seen from the
/// other side. Both terms are real commits, and in every observed occurrence one
/// of them contains the other — the git ref froze while the store kept
/// advancing, or a ref moved outside jj while the store had not yet exported.
/// Settling on the target that DESCENDS from every other is therefore a
/// convergence, not a choice: no commit on any side is left behind.
///
/// When no target contains the rest, one shape still has an answer that keeps
/// every commit: a rewritten base beside a child of its pre-rewrite twin, which
/// [`settle_rewritten_base_with_child`] replays onto the rewrite.
///
/// # What it will not do
///
/// Otherwise the sides hold genuinely different histories and there is no
/// answer that keeps both. Picking one would discard the other's commits
/// silently, which is the single failure mode this whole arc exists to prevent.
/// It refuses with a [`RepairRefusal`] naming every target and the exact
/// commands a maintainer can run to combine them.
///
/// The caller MUST hold the per-store lock: this reads targets and then writes
/// the bookmark.
pub fn repair_conflicted_bookmark_name(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<Option<String>, NameRepairError> {
    if branch.is_empty() || !bookmark_name_is_conflicted(jj, store, branch)? {
        return Ok(None);
    }
    // Step one of the canonical sequence, and on its own sometimes the whole
    // repair: once the store observes where the backing refs really are, a name
    // conflicted only by a missed import converges without being written to.
    import_git(jj, store)?;
    if !bookmark_name_is_conflicted(jj, store, branch)? {
        let settled = bookmark_commit(jj, store, branch);
        if let Some(commit) = settled.as_deref() {
            log::info!(
                "jj repair: bookmark `{branch}` name converged on {commit} from the import alone"
            );
            hold_git_ref_to_bookmark(jj, store, branch, commit)?;
        }
        return Ok(settled);
    }

    let targets = conflicted_bookmark_targets(jj, store, branch)?;
    let Some(winner) = target_containing_the_rest(jj, store, &targets) else {
        let reason = match settle_rewritten_base_with_child(jj, store, branch)? {
            RewrittenBaseSettlement::Settled { tip, .. } => return Ok(Some(tip)),
            RewrittenBaseSettlement::Declined { reason } => reason,
        };
        let conflict = describe_conflicted_bookmark(jj, store, branch)?.ok_or_else(|| {
            format!("branch `{branch}` stopped being conflicted while its repair was refusing")
        })?;
        log::error!(
            "jj repair: bookmark `{branch}` holds genuinely divergent targets, none descending \
             from the rest: {:?}. Descriptions: {:?}. Recent store operations: {:?}. Refusing to \
             choose — settling on one would discard the other's commits: {reason}.",
            targets,
            targets
                .iter()
                .map(|commit| commit_summary(jj, store, commit))
                .collect::<Vec<_>>(),
            recent_operation_ids(jj, store),
        );
        return Err(NameRepairError::Refused(Box::new(repair_refusal(
            jj, store, conflict, reason,
        ))));
    };

    log::warn!(
        "jj repair: bookmark `{branch}` name is CONFLICTED across {targets:?}; settling on \
         {winner}, which contains every other target"
    );
    set_bookmark_backwards(
        jj,
        store,
        branch,
        &winner,
        "jj repair: settle conflicted bookmark name",
    )?;
    // Without this the git ref keeps its own target and the next import
    // re-conflicts the name — the cycle, not a repair.
    hold_git_ref_to_bookmark(jj, store, branch, &winner)?;

    if bookmark_name_is_conflicted(jj, store, branch)? {
        return Err(format!(
            "branch `{branch}` still has competing versions of its history after being settled on \
             {winner}"
        )
        .into());
    }
    let after = bookmark_commit(jj, store, branch);
    if after.as_deref() != Some(winner.as_str()) {
        return Err(format!(
            "branch `{branch}` did not settle on {winner}: it is at {} instead",
            after.as_deref().unwrap_or("no single commit")
        )
        .into());
    }
    Ok(Some(winner))
}

/// Hold the backing git ref to the store's answer for `branch`.
///
/// One rule with no exceptions: whatever commit the store says a bookmark is,
/// `refs/heads/<branch>` must agree. Reconciling the jj side while leaving the
/// git side stale is how the two views drift apart, and their disagreement is
/// what the next import turns into a conflicted name.
fn hold_git_ref_to_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    commit: &str,
) -> Result<(), String> {
    export_git_verified(
        jj,
        store,
        true,
        "jj reconcile: export reconciled bookmark",
        &[(branch, commit)],
    )
}

fn set_bookmark_backwards(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    revision: &str,
    ctx: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            revision,
            "--allow-backwards",
            "--ignore-working-copy",
        ],
        ctx,
    )
    .map(|_| ())
}

/// Name a default bookmark found somewhere it has no business being.
///
/// The default branch has twice been observed sitting on an unrelated agent build
/// commit, with no forensic trail left by the time it was noticed. The cause is
/// not pinnable from the code, so instead the invariant is asserted at the one
/// place the bookmark is now read and repaired, and a violation is recorded with
/// everything the next diagnosis needs: the commit, what it says, every bookmark
/// riding it, and the store operations that led here. The repair still proceeds
/// — this reports, it does not gate.
fn report_bookmark_misposition(jj: &JjEnv, store: &Path, branch: &str, local: &str, remote: &str) {
    let riders = local_bookmarks_at(jj, store, local).unwrap_or_default();
    let on_agent_branch = riders.iter().any(|name| name.starts_with("agent/"));
    // A bookmark strictly BEHIND origin is the ordinary case this function
    // repairs; only an unrelated position, or one riding an agent branch, is the
    // anomaly worth a forensic record.
    let behind = revset_descends_from(jj, store, remote, local);
    if behind && !on_agent_branch {
        return;
    }
    log::error!(
        "jj reconcile: default bookmark `{branch}` is MISPOSITIONED at {local} — {}. \
         description: {:?}; bookmarks at that commit: {:?}; recent store operations: {:?}. \
         Repairing onto {remote}.",
        if on_agent_branch {
            "it rides an agent branch"
        } else {
            "it is neither an ancestor nor a descendant of origin's tip"
        },
        commit_summary(jj, store, local),
        riders,
        recent_operation_ids(jj, store),
    );
}

/// The first line of a commit's description, for a diagnostic. Empty when it does
/// not resolve; this is never load-bearing.
fn commit_summary(jj: &JjEnv, store: &Path, commit: &str) -> String {
    jj.run(
        store,
        &[
            "log",
            "-r",
            commit,
            "--no-graph",
            "-T",
            "description.first_line()",
            "--ignore-working-copy",
        ],
        "jj log (commit summary)",
    )
    .unwrap_or_default()
}

/// The most recent store operation ids, so an anomaly report points at the ops
/// that produced it rather than requiring the op log to still be there later.
fn recent_operation_ids(jj: &JjEnv, store: &Path) -> Vec<String> {
    jj.run(
        store,
        &[
            "op",
            "log",
            "--no-graph",
            "-n",
            "5",
            "-T",
            "id.short() ++ \" \" ++ description ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj op log (misposition forensics)",
    )
    .unwrap_or_default()
    .lines()
    .map(str::trim)
    .filter(|line| !line.is_empty())
    .map(ToOwned::to_owned)
    .collect()
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
    let Some(published) = verified_publish_target(jj, store, branch)? else {
        log::debug!("jj base bookmark {branch} absent from store; nothing to publish");
        return Ok(());
    };
    jj.run(
        store,
        &[
            "git",
            "push",
            "--remote",
            "origin",
            "--bookmark",
            branch,
            "--ignore-working-copy",
        ],
        "jj git push base bookmark",
    )?;
    confirm_origin_tip(store, branch, &published)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(commit: &str, change: &str) -> BookmarkTarget {
        BookmarkTarget {
            commit: commit.to_string(),
            change: change.to_string(),
            description: String::new(),
        }
    }

    fn conflict(removed: Vec<BookmarkTarget>, added: Vec<BookmarkTarget>) -> ConflictedBookmark {
        ConflictedBookmark {
            branch: "agent/x".into(),
            removed,
            added,
        }
    }

    fn commands(conflict: &ConflictedBookmark) -> Vec<String> {
        maintainer_commands(Path::new("/store"), conflict, |_, _, _| None)
    }

    /// A pairwise replay plus a bookmark move would drop every version outside
    /// the pair, so three or more versions get the inspection and nothing that
    /// mutates.
    #[test]
    fn three_versions_get_no_command_that_would_drop_one() {
        let lines = commands(&conflict(
            vec![target("0000", "zzzz")],
            vec![
                target("aaaa", "kkkk"),
                target("bbbb", "llll"),
                target("cccc", "mmmm"),
            ],
        ));
        assert!(
            lines.iter().any(|line| line.contains(" log -r ")),
            "{lines:?}"
        );
        assert!(
            !lines
                .iter()
                .any(|line| line.contains(" rebase ") || line.contains(" bookmark set ")),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|line| line.starts_with("# 3 versions")),
            "{lines:?}"
        );
    }

    #[test]
    fn two_unrelated_versions_get_both_orderings_from_their_fork_point() {
        let lines = commands(&conflict(
            vec![target("0000", "zzzz")],
            vec![target("aaaa", "kkkk"), target("bbbb", "llll")],
        ));
        assert!(lines.contains(
            &"jj -R '/store' --ignore-working-copy rebase -r 'fork_point(aaaa|bbbb)..bbbb' -d aaaa"
                .to_string()
        ));
        assert!(lines.contains(
            &"jj -R '/store' --ignore-working-copy rebase -r 'fork_point(bbbb|aaaa)..aaaa' -d bbbb"
                .to_string()
        ));
        assert!(lines.contains(
            &"jj -R '/store' --ignore-working-copy bookmark set 'agent/x' --allow-backwards -r 'aaaa:: & change_id(llll)'"
                .to_string()
        ));
    }

    /// Where one version is the removed target's rewrite, only the child's own
    /// commits move: replaying the rewrite's ancestry would drag whatever it
    /// was rewritten onto.
    #[test]
    fn a_rewritten_base_gets_only_the_childs_own_commits_replayed() {
        let lines = commands(&conflict(
            vec![target("rrrr", "same")],
            vec![target("cccc", "kids"), target("aaaa", "same")],
        ));
        let rebases = lines
            .iter()
            .filter(|line| line.contains(" rebase "))
            .collect::<Vec<_>>();
        assert_eq!(
            rebases,
            vec!["jj -R '/store' --ignore-working-copy rebase -r 'rrrr..cccc' -d aaaa"],
            "{lines:?}"
        );
    }
}
