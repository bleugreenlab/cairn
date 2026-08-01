//! Bookmark / git-ref resolution, export, and publishing to origin.
use super::*;
use std::path::Path;
/// Query only candidate bookmark names in one structured jj invocation. An
/// empty candidate set is resolved without spawning jj.
pub(crate) fn query_local_bookmarks(
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
/// Push the workspace's bookmark to origin. Callers choose whether publication
/// is strict or best-effort by propagating or logging the returned error. Skips
/// empty/`main`/`master` branches (the same guard the git path uses). jj 0.42
/// auto-tracks a new bookmark on push, so the removed `--allow-new` flag is not
/// passed; seals only advance the bookmark, so the push is a fast-forward and
/// needs no force.
///
/// `--ignore-working-copy`: a publish must never SNAPSHOT the live `@`. The
/// bookmark already points at the sealed `@-`, so pushing needs no fresh
/// snapshot — and snapshotting here would fold whatever transient dirt sits in
/// `@` (e.g. a `when:write` check's caches, since the post-seal push runs from
/// the workspace) into the working-copy commit, exactly the kind of working-copy
/// mutation a concurrent store op can then wedge a later seal on. Matches
/// `advance_workspace_onto` / `node_changed_files`, which pass it deliberately.
///
/// The push is bracketed by verification: the backing git ref must equal the
/// bookmark commit BEFORE the push (or the push carries a stale tree — the
/// half-landed-PR mode), and origin's tip must equal it AFTER (or the push
/// reported a success that origin did not record — the phantom-PR mode).
pub(crate) fn push_to_origin(jj: &JjEnv, ws: &Path, branch: &str) -> Result<(), String> {
    if branch.is_empty() || branch == "main" || branch == "master" {
        log::debug!("Skipping jj push for branch: {branch}");
        return Ok(());
    }
    let published = verified_publish_target(jj, ws, branch)?;
    jj.run(
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
    )?;
    if let Some(published) = published.as_deref() {
        confirm_origin_tip(ws, branch, published)?;
    }
    log::info!("Pushed bookmark {branch} to origin (jj)");
    Ok(())
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
pub(crate) fn bookmark_landed_in(jj: &JjEnv, store: &Path, src: &str, dst: &str) -> bool {
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
pub(crate) fn local_bookmarks_at(jj: &JjEnv, ws: &Path, rev: &str) -> Result<Vec<String>, String> {
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
pub(crate) fn revset_commit_checked(
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
pub(crate) fn bookmark_commit_checked(
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
pub(crate) fn revset_commit(jj: &JjEnv, store: &Path, revset: &str) -> Option<String> {
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
    import_git(jj, store)?;

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
        import_git(jj, store)?;
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
pub(crate) enum ManagedBranchConvergence {
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

/// Rebase only the unpublished suffix of a managed branch onto a rewritten
/// origin tip. The frontier is selected by jj change identity, not description or
/// tree, and must be unique in the local ancestry. This is intentionally separate
/// from `reconcile_tracked_bookmark`: origin does not own an agent branch.
pub(crate) fn converge_managed_branch_after_remote_rewrite(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<ManagedBranchConvergence, String> {
    import_git(jj, store)?;
    if bookmark_name_is_conflicted(jj, store, branch)? {
        let checkout = super::export::resolve_backing_checkout(store).ok_or_else(|| {
            format!("stale publication recovery refused: bookmark `{branch}` is conflicted and its backing checkout cannot be resolved")
        })?;
        let output = crate::env::git()
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
        hold_git_ref_to_bookmark(jj, store, branch, &local)?;
        return Ok(ManagedBranchConvergence::Unchanged { commit: local });
    }
    if revset_descends_from(jj, store, &local, &remote) {
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
) -> Result<Option<String>, String> {
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
/// # What it will not do
///
/// When no target descends from all the others, the two sides hold genuinely
/// different histories and there is no answer that keeps both. Picking one would
/// discard the other's commits silently, which is the single failure mode this
/// whole arc exists to prevent. It reports every target and refuses instead.
///
/// The caller MUST hold the per-store lock: this reads targets and then writes
/// the bookmark.
pub fn repair_conflicted_bookmark_name(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<Option<String>, String> {
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
    let Some(winner) = targets
        .iter()
        .find(|candidate| {
            targets.iter().all(|other| {
                other == *candidate || revset_descends_from(jj, store, candidate, other)
            })
        })
        .cloned()
    else {
        log::error!(
            "jj repair: bookmark `{branch}` holds genuinely divergent targets, none descending \
             from the rest: {:?}. Descriptions: {:?}. Recent store operations: {:?}. Refusing to \
             choose — settling on one would discard the other's commits.",
            targets,
            targets
                .iter()
                .map(|commit| commit_summary(jj, store, commit))
                .collect::<Vec<_>>(),
            recent_operation_ids(jj, store),
        );
        return Err(format!(
            "branch `{branch}` has two unrelated versions of its history in the store ({}), so \
             there is no single tip to settle it on",
            targets.join(" and ")
        ));
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
        ));
    }
    let after = bookmark_commit(jj, store, branch);
    if after.as_deref() != Some(winner.as_str()) {
        return Err(format!(
            "branch `{branch}` did not settle on {winner}: it is at {} instead",
            after.as_deref().unwrap_or("no single commit")
        ));
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
pub(crate) fn ensure_bookmark_on_origin(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
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
