//! Store-side merge folds, rebases, squashes, and bookmark advancement
//! primitives the sibling reconcile builds on.
use super::*;
use std::path::Path;

// ── Sibling reconcile (auto-rebase onto an advanced integration tip) ─────────

/// Outcome of reconciling in-flight siblings onto an advanced integration tip:
/// which sibling bookmarks rebased cleanly, which recorded a conflict, and which
/// were held back untouched. A recorded conflict is STOP-THE-LINE, not a
/// convenience item: jj refuses to push or merge a conflicted commit, so a
/// conflicted branch destined for GitHub is wedged until the agent resolves the
/// markers and re-seals. The reconcile also never hands a conflicted base down to
/// clean siblings — when the rebase dest itself carries a conflict, every sibling
/// is `held` on its prior clean commit rather than rebased onto the conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileFailure {
    pub(crate) branch: String,
    pub(crate) error: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Sibling bookmarks that rebased with no conflict.
    pub(crate) rebased_clean: Vec<String>,
    /// Sibling bookmarks whose rebase recorded a conflict.
    pub(crate) conflicted: Vec<String>,
    /// Trivial bookmark advances that are current but intentionally
    /// silent because the sibling had no branch work to announce.
    pub(crate) silent: Vec<String>,
    /// Sibling bookmarks held UNrebased because the rebase dest itself carries
    /// a recorded conflict — never handed a conflicted base. Cleared on the next
    /// reconcile once the base re-seals conflict-free.
    pub(crate) held: Vec<String>,
    /// Exact per-branch failures from graph movement, flatten recovery, or publication.
    pub(crate) failed: Vec<ReconcileFailure>,
    /// The full three-way conflict diagnostic per branch in `conflicted`,
    /// captured INSIDE the rebase before it was rolled back. That capture is the
    /// only window in which any of it exists: afterwards the branch is clean
    /// again, so there is nothing left to enumerate and a later probe would
    /// report none.
    pub(crate) conflict_diagnostics: std::collections::HashMap<String, ConflictDiagnostic>,
}

/// The refusal an agent receives when its branch cannot be rebased onto an
/// advanced base.
///
/// Written for the world Cairn actually runs: agents hold detached git
/// worktrees, not jj workspaces, so there are no conflict markers to resolve and
/// no seal to re-take. The rebase was rolled back, so the branch is untouched —
/// which is the fact that makes this actionable: the agent merges the new base
/// into their own branch with ordinary writes and commits the result.
pub(crate) fn base_conflict_refusal(
    target_branch: &str,
    source_branch: &str,
    paths: &[String],
) -> String {
    let files = if paths.is_empty() {
        String::new()
    } else {
        format!("\nConflicting file(s): {}.", paths.join(", "))
    };
    format!(
        "Refusing to merge: `{target_branch}` advanced, and `{source_branch}`'s content conflicts \
         with the new tip. The automatic rebase was rolled back, so `{source_branch}` is exactly \
         where it was and no work was lost.{files}\nBring the new `{target_branch}` into \
         `{source_branch}` yourself: read both versions of each file above, write the merged \
         result with ordinary edits, commit it on `{source_branch}`, then merge again."
    )
}

/// The refusal for a branch whose TIP is clean but whose own history carries a
/// conflict-flagged commit.
///
/// Folding it would carry that commit onto the target as ordinary history, after
/// which jj refuses to push the target at all — the wedge this arc has hit
/// repeatedly. The branch itself is intact and its tip tree is fine; what it
/// needs is to be rebuilt on clean content, which for most paths is the guarded
/// flatten and for a commit-preserving fold is the agent's own re-commit.
pub(crate) fn conflicted_ancestry_refusal(
    target_branch: &str,
    source_branch: &str,
    paths: &[String],
) -> String {
    let files = if paths.is_empty() {
        String::new()
    } else {
        format!(
            "\nFile(s) recorded as conflicted in that history: {}.",
            paths.join(", ")
        )
    };
    format!(
        "Refusing to merge: `{source_branch}` rebases cleanly onto `{target_branch}`, but its own \
         history still contains commit(s) recorded as conflicted, and this merge preserves every \
         commit — so those would land on `{target_branch}` and make it unpushable.{files}\nRebuild \
         `{source_branch}` on clean content (one commit carrying your current tree on top of the \
         live `{target_branch}`), then merge again."
    )
}

/// Fold a child's real commit into the integration bookmark over the shared
/// store — the local "merge" of a child PR. `jj bookmark set` is forward-only (it
/// refuses a backwards/sideways move), so the child must already sit on the
/// current integration tip; callers establish that by rebasing the source onto
/// the current tip before folding (`store_merge_child`, `rebase_then_fold_into`).
/// A refusal here means that rebase did not run or did not take — surface it
/// loudly rather than silently regressing the tip.
/// `--ignore-working-copy` because the fold is driven from the store, not a
/// workspace (Gotcha A: the store's default `@` may be stale after a prior
/// `--ignore-working-copy` rebase).
///
/// A backwards/sideways refusal is mapped to a safe, actionable message: jj's
/// raw stderr hints `--allow-backwards`, which would move the bookmark BACKWARD
/// and clobber the commits that advanced it. That hint must never reach an
/// agent, so it is never echoed. For a fold whose target advances out of band
/// (the project default branch), callers use `rebase_then_fold_into`, which
/// rebases first so this path is never reached.
pub(crate) fn merge_into_bookmark(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    child_branch: &str,
) -> Result<(), String> {
    let child_rev = format!("bookmarks(exact:{child_branch:?})");
    if let Err(e) = jj.run(
        store,
        &[
            "bookmark",
            "set",
            integration_branch,
            "-r",
            &child_rev,
            "--ignore-working-copy",
        ],
        "jj bookmark set (merge fold)",
    ) {
        // Sanitize jj's raw backwards/sideways refusal: its stderr hints
        // `--allow-backwards`, which would move the bookmark BACKWARD and clobber
        // the commits that advanced it. Map it to a message that names the real
        // cause (the source is not a descendant of the target) and the safe
        // remedy (rebase first), and NEVER echo the dangerous hint.
        let lowered = e.to_lowercase();
        if lowered.contains("backwards") || lowered.contains("sideways") {
            return Err(format!(
                "Refusing to fold `{child_branch}` into `{integration_branch}`: the source is not a descendant of the target (the target advanced past the source's fork point). Rebase the source onto the current target tip and let it re-seal, then merge again."
            ));
        }
        return Err(e);
    }
    // Export the advanced bookmark to the backing git repo so the project's
    // `refs/heads/<integration>` tracks the fold (as `seal` does after a sealed
    // commit). Without this the store bookmark is advanced but the project git
    // ref lags, and a later child provisioned off the integration branch
    // resolves its base via that stale ref (`execution/jobs/worktrees.rs`) and
    // would start from the pre-merge tip — breaking the store-owns-merge
    // invariant. Load-bearing, so it fails the fold rather than silently leaving
    // a stale ref — which requires VERIFYING the ref, because a refused export
    // exits 0 and would otherwise satisfy that contract while leaving exactly the
    // stale ref it promises never to leave.
    export_bookmark_advance(
        jj,
        store,
        true,
        integration_branch,
        "jj git export (merge fold)",
    )
}

/// Merge a source bookmark into a target whose tip may have advanced out of band
/// (the project default branch). Unlike `merge_into_bookmark`'s forward-only fold
/// — which assumes Cairn's reconcile keeps the source on an integration tip — the
/// default branch advances OUTSIDE the fold chain (another PR merged, or an
/// external push), so the source is first rebased onto the current target tip,
/// exactly as `reconcile_siblings` rebases siblings, then the target FFs to it.
/// A recorded conflict returns a safe, actionable error and NEVER the
/// `--allow-backwards` hint (which would move the default branch backward and
/// clobber it). `dest` is the resolved live target tip (`<target>@origin` for a
/// remote project after a fetch, else the local bookmark). Idempotent when the
/// source already sits on `dest` (the rebase is a `jj rebase` no-op).
pub fn rebase_then_fold_into(
    jj: &JjEnv,
    store: &Path,
    target_branch: &str,
    source_branch: &str,
    dest: &str,
) -> Result<(), String> {
    // This method folds the source's real commits onto the target, so the whole
    // rebased range has to be conflict-free — a clean tip over a conflict-flagged
    // ancestor would land that ancestor on the target as ordinary history, and
    // the target then cannot be pushed at all.
    match rebase_branch_onto(jj, store, source_branch, dest)? {
        RebaseOutcome::Rebased => {}
        RebaseOutcome::Conflicted { diagnostic } => {
            return Err(base_conflict_refusal(
                target_branch,
                source_branch,
                &diagnostic.conflicting_paths(),
            ))
        }
        RebaseOutcome::RebasedOverConflictedAncestry { paths } => {
            return Err(conflicted_ancestry_refusal(
                target_branch,
                source_branch,
                &paths,
            ))
        }
    }
    // The source is now a descendant of `dest` (and thus of the local target
    // bookmark, which `dest` advanced from), so this FF can never go backwards.
    merge_into_bookmark(jj, store, target_branch, source_branch)
}

/// Collapse a (possibly multi-commit) branch into a single commit on top of
/// `base_rev`, preserving its current tree. This restores the squash *shape* at
/// a default-branch landing: after the source is rebased onto the live default
/// tip, this rewrites the source bookmark to one commit whose parent is that tip
/// and whose tree equals the rebased source tree, so the FF fold lands exactly
/// one commit on the default branch instead of every per-change commit the agent
/// sealed. `message` becomes that commit's description (the PR title).
///
/// Operates entirely over the shared store with `--ignore-working-copy`
/// discipline (the store's `@` is a scratch working copy that must never be
/// snapshotted — Gotcha A, matching `merge_into_bookmark`/`rebase_branch_onto`).
/// Crucially the store's `@` is also never *moved*: `jj new --no-edit` creates
/// the squashed commit WITHOUT checking it out, so the working copy stays on its
/// scratch commit and a later plain (non-`--ignore-working-copy`) read — e.g.
/// `bookmark_commit` at the end of the fold — does not trip jj's stale-working-
/// copy guard.
///
/// Steps: capture the rebased tip (it carries the full source tree); create an
/// empty commit as a child of `base_rev`, addressing it by the set difference of
/// `base_rev`'s children before and after (`jj new` prints no machine-readable
/// id); repoint the bookmark to that empty commit; then `restore` the captured
/// tree INTO the bookmark. The restore mints a fresh commit id, so the bookmark
/// is moved FIRST and the restore targets the bookmark revset so it follows the
/// rewrite. The repoint is a deliberate sideways move — the squashed commit is
/// NOT a descendant of the old branch tip — so it passes `--allow-backwards`;
/// that hint is legitimate here (we are replacing the branch's own history with
/// an equivalent-tree single commit), unlike `merge_into_bookmark`, where the
/// same hint would clobber commits that advanced a shared target.
pub(crate) fn squash_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    base_rev: &str,
    message: &str,
) -> Result<(), String> {
    // The rebased tip still carries the complete source tree; capture it before
    // the bookmark is moved off it.
    let source_tree_rev = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("squash: branch `{branch}` did not resolve"))?;

    // Create an empty commit as a child of the live default tip, WITHOUT moving
    // `@`. `jj new` emits no machine-readable id, so address the new commit by
    // the set difference of `base_rev`'s children before and after.
    let before = base_children(jj, store, base_rev)?;
    jj.run(
        store,
        &[
            "new",
            "--no-edit",
            "-r",
            base_rev,
            "-m",
            message,
            "--ignore-working-copy",
        ],
        "jj new (squash base)",
    )?;
    let after = base_children(jj, store, base_rev)?;
    let mut added: Vec<String> = after.difference(&before).cloned().collect();
    let squashed = match added.len() {
        1 => added.remove(0),
        n => {
            return Err(format!(
                "squash: expected exactly one new commit on `{base_rev}`, found {n}"
            ))
        }
    };

    // Repoint the branch at the empty commit FIRST, then restore the source tree
    // INTO the bookmark so it follows the rewrite (`restore` mints a new id).
    // The repoint is a deliberate sideways move, so `--allow-backwards` is
    // correct here.
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &squashed,
            "--ignore-working-copy",
            "--allow-backwards",
        ],
        "jj bookmark set (squash)",
    )?;
    let branch_rev = format!("bookmarks(exact:{branch:?})");
    jj.run(
        store,
        &[
            "restore",
            "--from",
            &source_tree_rev,
            "--into",
            &branch_rev,
            "--ignore-working-copy",
        ],
        "jj restore (squash tree)",
    )?;
    // Export the rewritten bookmark to the backing git, as the fold path does,
    // so the project's `refs/heads/<branch>` tracks the squashed commit. The
    // restore minted a fresh commit id, so the expectation is read back from the
    // bookmark rather than assumed to be `squashed`.
    export_bookmark_advance(jj, store, true, branch, "jj git export (squash)")
}

/// Commit ids of the direct children of `rev` in the shared store. Used to
/// address a freshly-created `jj new --no-edit` commit by set difference, since
/// `jj new` emits no machine-readable id.
fn base_children(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
) -> Result<std::collections::HashSet<String>, String> {
    let revset = format!("children({rev})");
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            &revset,
            "--no-graph",
            "--ignore-working-copy",
            "-T",
            "commit_id ++ \"\\n\"",
        ],
        "jj log (base children)",
    )?;
    Ok(out
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

/// Idempotently mark a remote bookmark as jj-tracked so a local push of it is
/// accepted. jj refuses to push a local bookmark whose `@origin` counterpart is
/// untracked ("Non-tracking remote bookmark … exists"), which happens when
/// origin's ref was created outside this store's jj. A no-op when already
/// tracked; errors (best-effort for the caller) when there is no such remote
/// bookmark, e.g. a no-remote project.
pub(crate) fn track_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    let remote_ref = format!("{branch}@origin");
    jj.run(
        store,
        &["bookmark", "track", &remote_ref, "--ignore-working-copy"],
        "jj bookmark track",
    )
    .map(|_| ())
}

/// Push an already-advanced store bookmark to origin with `--ignore-working-copy`
/// (Gotcha A: the store's default `@` may be stale after a fold/rebase). Used to
/// advance both the integration tip after a fold and a cleanly-rebased sibling's
/// PR head; jj's remote-tracking model accepts a rewritten bookmark without a
/// force-push.
///
/// A verified store-bookmark push failure. Only jj's explicit stale-remote
/// rejection is recoverable; all other failures retain their original diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreBookmarkPushError {
    StaleRemote(String),
    Failed(String),
}

impl std::fmt::Display for StoreBookmarkPushError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleRemote(error) | Self::Failed(error) => formatter.write_str(error),
        }
    }
}

/// Bracketed by the same publication verification as [`push_to_origin`]: a
/// reconciled sibling whose git ref had frozen behind its bookmark would
/// otherwise advance its PR head to a commit nobody produced.
pub(crate) fn push_store_bookmark_classified(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<(), StoreBookmarkPushError> {
    let published =
        verified_publish_target(jj, store, branch).map_err(StoreBookmarkPushError::Failed)?;
    if let Err(error) = jj.run_with_timeout(
        store,
        &[
            "git",
            "push",
            "--ignore-working-copy",
            "--remote",
            "origin",
            "--bookmark",
            branch,
        ],
        "jj git push store bookmark",
        JJ_NETWORK_TIMEOUT,
    ) {
        let lowered = error.to_ascii_lowercase();
        return Err(
            if lowered.contains("unexpectedly moved on the remote")
                && lowered.contains("stale info")
            {
                StoreBookmarkPushError::StaleRemote(error)
            } else {
                StoreBookmarkPushError::Failed(error)
            },
        );
    }
    match published.as_deref() {
        Some(published) => {
            confirm_origin_tip(store, branch, published).map_err(StoreBookmarkPushError::Failed)
        }
        None => Ok(()),
    }
}

/// Compatibility boundary for merge publication paths that do not recover stale
/// remotes themselves. Managed agent publication uses the classified form.
pub(crate) fn push_store_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    push_store_bookmark_classified(jj, store, branch).map_err(|error| error.to_string())
}

/// Which of two very different situations a rolled-back rebase actually found.
///
/// Both arrive as "jj recorded a conflict", and until they were told apart every
/// agent that hit either was given the same advice — which is why two builders
/// (CAIRN-3327, CAIRN-3328) each burned a round doing content work on a branch
/// whose content was already finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictCondition {
    /// The two sides genuinely disagree about content. There is real merging to
    /// do, and it is the agent's to do with ordinary file writes.
    ///
    /// The default, deliberately: drift is only ever concluded from positive
    /// evidence that the sides agree, so every degraded or unreadable case falls
    /// toward asking for a human read rather than claiming work is already done.
    #[default]
    ContentConflict,
    /// Every conflicting path is byte-identical between the branch and the
    /// destination: the disagreement is about ANCESTRY, not content. The agent's
    /// work may well be complete, and no amount of editing will clear it — agent
    /// slots are plain git worktrees whose refs are downstream exports of the
    /// runner's private jj store, so no agent-side operation can move a branch's
    /// ancestry. The remedy is store-side.
    BaseDrift,
}

impl ConflictCondition {
    /// The stable wire/storage name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContentConflict => "content_conflict",
            Self::BaseDrift => "base_drift",
        }
    }
}

/// How an incoming file relates to the branch that could not absorb it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncomingClassification {
    /// jj recorded a conflict on this path. The agent must merge it by hand.
    Conflicting,
    /// The incoming change touches this path and the branch does not conflict on
    /// it, so a later retry absorbs it untouched.
    ///
    /// This is the classification the whole diagnostic exists for. A merged PR
    /// is one coordinated change across many files; reporting only the
    /// conflicting subset lets an agent resolve the named file, stop compiling,
    /// and have no idea why. Naming these makes it obvious the tree is
    /// mid-change and which siblings travel with the conflict.
    CleanOnRetry,
}

impl IncomingClassification {
    /// The stable wire/storage name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conflicting => "conflicting",
            Self::CleanOnRetry => "clean_on_retry",
        }
    }
}

/// One file the incoming change touches, with the status it carries and how it
/// lands on the branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingFile {
    pub path: String,
    /// jj's own name-status word for `base..theirs` (`M`, `A`, `D`, `R`, …), or
    /// `"C"` for a path jj reported as conflicting that the inventory did not
    /// otherwise name.
    pub status: String,
    pub classification: IncomingClassification,
}

/// The complete, immutable three-way coordinates of a conflict, captured INSIDE
/// the rebase that produced it.
///
/// That capture window is the only one there is. The rebase is rolled back
/// before it returns, so afterwards the branch is clean again and every one of
/// these facts is unrecoverable by probing — which is exactly how conflict
/// reporting came to hand agents a clean tree and a filename.
///
/// The commit ids are immutable objects, so a reader can recompute either side
/// of the merge later without storing a single patch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConflictDiagnostic {
    /// Merge base of `ours` and `theirs` — the fork point the branch left.
    pub base: Option<String>,
    /// The branch tip before the rebase: the agent's own content.
    pub ours: Option<String>,
    /// The pinned destination the rebase targeted: the incoming content.
    pub theirs: Option<String>,
    /// The conflict-flagged tip that briefly existed, before rollback. Recorded
    /// for forensics; it is never exported and may be garbage-collected.
    pub conflicted_tip: Option<String>,
    /// Content conflict, or ancestry drift.
    pub condition: ConflictCondition,
    /// The incoming change's COMPLETE file set, each classified.
    pub incoming: Vec<IncomingFile>,
}

impl ConflictDiagnostic {
    /// A diagnostic carrying only the conflicting paths, for the paths that
    /// cannot reach a real rebase (a synthesized refusal, or a legacy caller).
    pub fn from_paths(paths: Vec<String>) -> Self {
        Self {
            incoming: paths
                .into_iter()
                .map(|path| IncomingFile {
                    path,
                    status: "C".to_string(),
                    classification: IncomingClassification::Conflicting,
                })
                .collect(),
            ..Self::default()
        }
    }

    /// The paths jj recorded as conflicting.
    pub fn conflicting_paths(&self) -> Vec<String> {
        self.filter_paths(IncomingClassification::Conflicting)
    }

    /// The paths the incoming change carries that this branch does NOT conflict
    /// on — the siblings that arrive on the next retry.
    pub fn clean_on_retry_paths(&self) -> Vec<String> {
        self.filter_paths(IncomingClassification::CleanOnRetry)
    }

    fn filter_paths(&self, wanted: IncomingClassification) -> Vec<String> {
        self.incoming
            .iter()
            .filter(|file| file.classification == wanted)
            .map(|file| file.path.clone())
            .collect()
    }

    /// A stable identity for this exact conflict, so a delivery can be
    /// deduplicated against a repeat of the SAME base advance while a genuinely
    /// new one still gets through. Built from the three coordinates that define
    /// the merge; the conflicted tip is deliberately excluded, because it is a
    /// fresh object on every attempt at the same merge.
    pub fn fingerprint(&self) -> String {
        let field = |value: &Option<String>| value.clone().unwrap_or_else(|| "?".to_string());
        format!(
            "{}:{}:{}",
            field(&self.base),
            field(&self.ours),
            field(&self.theirs)
        )
    }
}

/// What a rebase did to a branch. Callers MUST branch on this rather than
/// re-probing [`branch_has_conflict`] afterwards: a conflicting rebase is undone
/// before this returns, so a post-hoc probe reports a clean branch and would read
/// a refusal as a success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseOutcome {
    /// Nothing in `dest..branch` carries a recorded conflict. The bookmark moved
    /// and the backing git ref was exported and verified onto it.
    Rebased,
    /// The tip is clean, but an ANCESTOR in the rebased range carries a recorded
    /// conflict.
    ///
    /// This is not something a rebase creates from a clean branch — jj propagates
    /// a conflict to every descendant until something resolves it, so a fresh
    /// conflict lands on the tip and is rolled back below. It is what a branch
    /// that ALREADY carried conflicted history looks like after being re-applied:
    /// a store predating this guard, `jj` run outside Cairn, or the older
    /// resolve-on-top-and-re-seal sequence, whose resolving commit keeps the tip
    /// clean while the conflicted commit beneath it survives every later rebase.
    ///
    /// The branch DID move and WAS exported, deliberately. Its tip tree is clean,
    /// so it is safe to materialize; and refusing to move it would strand it
    /// forever, because the flatten that heals this shape can only run on a
    /// branch that has been rebased onto its dest.
    ///
    /// It is NOT foldable. A fold carries the whole ancestry onto the target, so
    /// every fold path must either heal it first (`flatten_branch_recovery`) or
    /// refuse. `paths` names the files recorded as conflicted in that ancestry.
    RebasedOverConflictedAncestry { paths: Vec<String> },
    /// The rebase recorded a conflict on the tip, so it was ROLLED BACK: the
    /// branch sits on exactly the commit it held before, and nothing
    /// conflict-flagged was ever exported.
    ///
    /// The diagnostic carries everything the rollback is about to make
    /// unobservable: the immutable three-way coordinates, the incoming change's
    /// whole file set, and which condition this is.
    Conflicted { diagnostic: ConflictDiagnostic },
}

/// Rebase a whole branch onto a destination over the shared store, non-blocking,
/// and never leave a conflict where anything can check it out.
///
/// `--ignore-working-copy` because this is driven from the store, not a
/// workspace. `jj rebase` SUCCEEDS while recording a conflict inside the rebased
/// commit, and exporting that commit is the single most destructive thing this
/// system can do: jj's git representation of a conflict-flagged commit is the
/// DESTINATION side of every conflicted file at the top level with no markers,
/// plus `.jjconflict-*` sidecars carrying the real content. Every agent cell is a
/// plain `git worktree add --detach` of that ref, so the branch's own work simply
/// vanishes from the tree the agent then builds and tests against — which is how
/// fifty commits came to describe work absent from their own trees, and how half
/// a design landed on the default branch.
///
/// So a conflict newly recorded on the branch TIP never reaches git. The store
/// operation id is captured immediately before the rebase (exact under the
/// per-store lock every caller holds, per [`restore_operation`]), and on a
/// recorded tip conflict the conflicting paths are read off the branch, the
/// operation is restored, and the export runs against the RESTORED bookmark. The
/// branch is then bit-identical to its pre-rebase self and the caller is told so.
///
/// That is the whole of what the rollback covers, and the scope is deliberate.
/// Conflict-flagged commits a branch was ALREADY carrying are re-applied by the
/// rebase and survive it; rolling back would not remove them and would strand the
/// branch, so they are reported instead — see
/// [`RebaseOutcome::RebasedOverConflictedAncestry`], which every caller must
/// either heal or refuse.
///
/// On the clean path the export still runs and is still VERIFIED: jj moves the
/// local bookmark during the rebase, and leaving the backing git ref at the old
/// commit produces a local-vs-`@git` conflicted bookmark, after which idempotent
/// descendant checks stop being reliable. Since a refused export reports success,
/// only the verification actually keeps the two ref views in lockstep.
///
/// The decision is RANGE-aware, not tip-aware. A clean tip does not mean a clean
/// branch: a branch that already carried a conflict-flagged commit keeps it
/// through every later rebase, and folding such a branch would carry that
/// ancestry onto the target. That case is reported as
/// [`RebaseOutcome::RebasedOverConflictedAncestry`] so no caller can mistake it
/// for clean — see that variant for why it is exported anyway.
pub fn rebase_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
) -> Result<RebaseOutcome, String> {
    // Captured immediately before the rebase, so the restore window covers
    // exactly this one rebase and no sibling's.
    let pre_op = operation_id(jj, store)?;
    let pre_tip = bookmark_commit(jj, store, branch);

    jj.run(
        store,
        &["rebase", "-b", branch, "-o", dest, "--ignore-working-copy"],
        "jj rebase",
    )?;

    if branch_has_conflict(jj, store, branch)? {
        // Everything the rollback is about to erase is read HERE, in the only
        // window where it exists. Afterwards the branch is clean again, so a
        // later probe reports no conflict, no conflicting paths, and no
        // conflicted tip — the shape that used to leave an agent with a clean
        // tree and a filename.
        let paths = branch_conflicted_paths(jj, store, branch);
        let diagnostic =
            capture_conflict_diagnostic(jj, store, branch, dest, pre_tip.as_deref(), paths.clone());
        restore_operation(jj, store, &pre_op).map_err(|error| {
            format!(
                "jj rebase of `{branch}` onto `{dest}` recorded a conflict, and the store could \
                 not be rolled back to its pre-rebase operation {pre_op}: {error}. The branch may \
                 be sitting on a conflict-flagged commit; do NOT export or publish it."
            )
        })?;
        // The bookmark is back where it started, so this export asserts the
        // pre-rebase commit rather than publishing anything new. It is what
        // proves the git ref was never dragged onto the conflict.
        export_bookmark_advance(
            jj,
            store,
            true,
            branch,
            "jj git export (conflicting rebase rolled back)",
        )?;
        log::info!(
            "jj rebase: `{branch}` onto `{dest}` recorded a {} and was rolled back to {} — nothing \
             conflict-flagged was exported. Conflicting file(s): {}. Incoming change also carries \
             {} file(s) that arrive cleanly on retry.",
            diagnostic.condition.as_str(),
            pre_tip.as_deref().unwrap_or("<unresolved>"),
            if paths.is_empty() {
                "<none enumerated>".to_string()
            } else {
                paths.join(", ")
            },
            diagnostic.clean_on_retry_paths().len()
        );
        return Ok(RebaseOutcome::Conflicted { diagnostic });
    }

    export_bookmark_advance(jj, store, true, branch, "jj git export (rebase)")?;

    // A clean TIP is not a clean branch. Enumerate the whole rebased range: a
    // conflict-flagged ancestor is invisible to `branch_has_conflict` and would
    // otherwise be folded onto a target as ordinary history.
    let ancestry_conflicts =
        conflicted_commits(jj, store, &format!("({dest})..bookmarks(exact:{branch:?})"));
    if !ancestry_conflicts.is_empty() {
        let paths: Vec<String> = ancestry_conflicts
            .iter()
            .flat_map(|commit| commit.files.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        log::warn!(
            "jj rebase: `{branch}` rebased onto `{dest}` with a clean tip, but {} commit(s) in its \
             own history carry a recorded conflict ({}). The branch is materializable but NOT \
             foldable — it must be flattened onto its base before it can merge. Conflicted \
             file(s): {}",
            ancestry_conflicts.len(),
            ancestry_conflicts
                .iter()
                .map(|commit| commit.commit_id.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            if paths.is_empty() {
                "<none enumerated>".to_string()
            } else {
                paths.join(", ")
            }
        );
        return Ok(RebaseOutcome::RebasedOverConflictedAncestry { paths });
    }

    // A clean rebase is where the UNFLAGGED loss lives: when both sides edited
    // the same region jj can pick a winner and record no conflict at all, and the
    // branch's version is simply gone from a tree that looks perfectly ordinary.
    // Prevention is not proven here; detection in the log is.
    if let (Some(pre_tip), Some(post_tip)) = (
        pre_tip.as_deref(),
        bookmark_commit(jj, store, branch).as_deref(),
    ) {
        if pre_tip != post_tip {
            let selection = rebase_side_selection(jj, store, dest, pre_tip, post_tip);
            log_rebase_side_selection(branch, dest, pre_tip, post_tip, &selection);
        }
    }
    Ok(RebaseOutcome::Rebased)
}

/// The fork point of `pre_tip` and `dest` — the base of the three-way merge that
/// just failed. `None` when it does not resolve; the diagnostic degrades to
/// missing coordinates rather than failing the rebase that produced it.
fn merge_base_of(jj: &JjEnv, store: &Path, pre_tip: &str, dest: &str) -> Option<String> {
    revset_commit(jj, store, &format!("heads(::{pre_tip} & ::({dest}))"))
}

/// Whether any of `paths` differs in content between two revisions.
///
/// This is the whole of the base-drift test. jj records a conflict from the
/// SHAPE of the history — three-way merge over a stale base — independently of
/// whether the two sides actually disagree about bytes. When they do not, the
/// agent has nothing to edit and telling them to merge by hand sends them
/// looking for a difference that is not there.
///
/// Advisory: an unresolvable diff answers `true`, which classifies the conflict
/// as a content conflict. That is the safe direction — it asks for a human read
/// of both sides rather than claiming work is already done.
pub(crate) fn paths_differ_between(
    jj: &JjEnv,
    store: &Path,
    from: &str,
    to: &str,
    paths: &[String],
) -> bool {
    if paths.is_empty() {
        return true;
    }
    let mut args = vec![
        "diff",
        "--ignore-working-copy",
        "--summary",
        "--from",
        from,
        "--to",
        to,
        "--",
    ];
    args.extend(paths.iter().map(String::as_str));
    match jj.run(store, &args, "jj diff --summary (base-drift probe)") {
        Ok(output) => !output.trim().is_empty(),
        Err(error) => {
            log::warn!("conflict diagnostic: base-drift probe failed, assuming a content conflict: {error}");
            true
        }
    }
}

/// One side of a recorded conflict, recomputed from immutable commits.
///
/// A conflict diagnostic stores coordinates, not patches, precisely so this can
/// be asked later and answered from the objects themselves. When an object is
/// gone — garbage-collected, or never local on this machine — that is reported as
/// an error. It is never quietly answered from the current tree, which describes
/// a different range and would read as an authoritative answer to the question
/// actually asked.
pub(crate) fn merge_side_patch(
    jj: &JjEnv,
    store: &Path,
    from: &str,
    to: &str,
    file: Option<&str>,
) -> Result<String, String> {
    for revision in [from, to] {
        if revset_commit_checked(jj, store, revision)
            .map_err(|error| format!("resolve {revision}: {error}"))?
            .is_none()
        {
            return Err(format!(
                "commit {revision} is no longer available in this store"
            ));
        }
    }
    let mut args = vec![
        "diff",
        "--ignore-working-copy",
        "--git",
        "--from",
        from,
        "--to",
        to,
    ];
    if let Some(file) = file {
        args.push("--");
        args.push(file);
    }
    jj.run(store, &args, "jj diff --git (conflict merge side)")
}

/// Assemble the complete conflict diagnostic while the conflict still exists.
///
/// Called from inside [`rebase_branch_onto`] between the recorded conflict and
/// the rollback, under the per-store lock every caller holds. Every step is
/// advisory in the sense that it degrades to a missing field rather than failing
/// the rebase: an instrument must never be able to break the machinery it
/// measures. What it must NOT do is guess — conflict membership comes from jj's
/// real rebase result (`conflicting`), never from path overlap.
fn capture_conflict_diagnostic(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
    pre_tip: Option<&str>,
    conflicting: Vec<String>,
) -> ConflictDiagnostic {
    let theirs = revset_commit(jj, store, dest);
    let conflicted_tip = bookmark_commit(jj, store, branch);
    let base = match (pre_tip, theirs.as_deref()) {
        (Some(ours), Some(theirs)) => merge_base_of(jj, store, ours, theirs),
        _ => None,
    };

    // The incoming change's COMPLETE file set: everything `base..theirs` touches,
    // with jj's own status word so an add, a delete, and a rename are
    // distinguishable rather than flattened to "changed".
    let mut incoming: Vec<IncomingFile> = match (base.as_deref(), theirs.as_deref()) {
        (Some(base), Some(theirs)) => diff_name_status(jj, store, base, theirs)
            .into_iter()
            .map(|(status, path)| {
                let classification = if conflicting.contains(&path) {
                    IncomingClassification::Conflicting
                } else {
                    IncomingClassification::CleanOnRetry
                };
                IncomingFile {
                    path,
                    status,
                    classification,
                }
            })
            .collect(),
        _ => Vec::new(),
    };
    // A conflicting path the inventory did not name is still a conflicting path.
    // jj's authority over conflict membership is absolute here; the inventory is
    // only how the SIBLINGS are discovered.
    for path in &conflicting {
        if !incoming.iter().any(|file| &file.path == path) {
            incoming.push(IncomingFile {
                path: path.clone(),
                status: "C".to_string(),
                classification: IncomingClassification::Conflicting,
            });
        }
    }
    incoming.sort_by(|a, b| a.path.cmp(&b.path));

    let condition = match (pre_tip, theirs.as_deref()) {
        (Some(ours), Some(theirs))
            if !paths_differ_between(jj, store, ours, theirs, &conflicting) =>
        {
            ConflictCondition::BaseDrift
        }
        _ => ConflictCondition::ContentConflict,
    };

    ConflictDiagnostic {
        base,
        ours: pre_tip.map(ToOwned::to_owned),
        theirs,
        conflicted_tip,
        condition,
        incoming,
    }
}

/// What a clean rebase changed on a branch's tip, and which of those files the
/// branch had itself modified against its old base.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebaseSideSelection {
    /// Name-status of `pre_tip..post_tip` as `(status, path)`.
    pub changed: Vec<(String, String)>,
    /// Paths in `changed` that the branch had ALSO modified against its old base.
    /// Both sides edited these, and the rebase produced a result for each without
    /// recording a conflict — so a side was chosen and nothing said so.
    pub overlapping: Vec<String>,
}

/// Name-status of a store-side diff as `(status, path)` pairs. Advisory: any jj
/// error yields an empty list rather than failing the rebase that produced it.
fn diff_name_status(jj: &JjEnv, store: &Path, from: &str, to: &str) -> Vec<(String, String)> {
    jj.run(
        store,
        &[
            "diff",
            "--ignore-working-copy",
            "--summary",
            "--from",
            from,
            "--to",
            to,
        ],
        "jj diff --summary (rebase side selection)",
    )
    .unwrap_or_default()
    .lines()
    .filter_map(|line| {
        let (status, path) = line.trim_end().split_once(' ')?;
        (!path.is_empty()).then(|| (status.to_string(), path.to_string()))
    })
    .collect()
}

/// Compute what a completed rebase of `branch` from `pre_tip` to `post_tip` onto
/// `dest` changed, and where that intersects the branch's own edits.
///
/// The branch's own footprint is measured against its OLD base — the fork point
/// of `pre_tip` and `dest`, i.e. `heads(::pre_tip & ::dest)` — so the intersection
/// names exactly the files both sides touched. An unresolvable fork point yields
/// an empty `overlapping` rather than a failure: this is an instrument, and it
/// must never be able to fail a reconcile.
pub(crate) fn rebase_side_selection(
    jj: &JjEnv,
    store: &Path,
    dest: &str,
    pre_tip: &str,
    post_tip: &str,
) -> RebaseSideSelection {
    let changed = diff_name_status(jj, store, pre_tip, post_tip);
    let fork_point = jj
        .run(
            store,
            &[
                "log",
                "-r",
                &format!("heads(::{pre_tip} & ::({dest}))"),
                "--no-graph",
                "--ignore-working-copy",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
            "jj log (rebase fork point)",
        )
        .ok()
        .and_then(|out| out.lines().next().map(str::trim).map(ToOwned::to_owned))
        .filter(|commit| !commit.is_empty());
    let Some(fork_point) = fork_point else {
        return RebaseSideSelection {
            changed,
            overlapping: Vec::new(),
        };
    };
    let own: std::collections::BTreeSet<String> = diff_name_status(jj, store, &fork_point, pre_tip)
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    let overlapping = changed
        .iter()
        .map(|(_, path)| path)
        .filter(|path| own.contains(*path))
        .cloned()
        .collect();
    RebaseSideSelection {
        changed,
        overlapping,
    }
}

/// Emit the side-selection record: the whole file-level name-status at info, and
/// the both-sides-edited subset at warn. A base advance can legitimately change
/// hundreds of files on a branch tip, so the info line is capped; the warn line
/// is the one that matters and is never truncated.
fn log_rebase_side_selection(
    branch: &str,
    dest: &str,
    pre_tip: &str,
    post_tip: &str,
    selection: &RebaseSideSelection,
) {
    const MAX_LISTED: usize = 60;
    let listed: Vec<String> = selection
        .changed
        .iter()
        .take(MAX_LISTED)
        .map(|(status, path)| format!("{status} {path}"))
        .collect();
    let elided = selection.changed.len().saturating_sub(listed.len());
    log::info!(
        "jj rebase: `{branch}` moved {pre_tip} -> {post_tip} onto `{dest}`; {} file(s) differ: {}{}",
        selection.changed.len(),
        listed.join(", "),
        if elided > 0 {
            format!(" (and {elided} more)")
        } else {
            String::new()
        }
    );
    if selection.overlapping.is_empty() {
        return;
    }
    log::warn!(
        "jj rebase: `{branch}` — the rebase changed {} file(s) that the branch had itself modified \
         against its old base, WITHOUT recording a conflict: {}. jj chose a side for each; compare \
         them against the pre-rebase tip {pre_tip} before trusting the result.",
        selection.overlapping.len(),
        selection.overlapping.join(", ")
    );
}

/// Fast-forward a branch bookmark to a concrete destination commit over the shared
/// store, then export the move to git immediately so jj's bookmark and backing git
/// ref stay in lockstep. This is the no-work sibling analogue of
/// [`rebase_branch_onto`]: there is no branch commit to rebase, only an idle
/// bookmark to move onto the advanced base.
pub(crate) fn fast_forward_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            dest,
            "--ignore-working-copy",
        ],
        "jj bookmark fast-forward",
    )?;
    // `dest` is the exact commit this was asked to land on, so assert that rather
    // than whatever the bookmark reports afterwards.
    export_git_verified(
        jj,
        store,
        true,
        "jj git export (bookmark fast-forward)",
        &[(branch, dest)],
    )
}
