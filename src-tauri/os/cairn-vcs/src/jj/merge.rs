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
    pub branch: String,
    pub error: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Sibling bookmarks that rebased with no conflict.
    pub rebased_clean: Vec<String>,
    /// Sibling bookmarks whose rebase recorded a conflict.
    pub conflicted: Vec<String>,
    /// Trivial bookmark advances that are current but intentionally
    /// silent because the sibling had no branch work to announce.
    pub silent: Vec<String>,
    /// Sibling bookmarks held UNrebased because the rebase dest itself carries
    /// a recorded conflict — never handed a conflicted base. Cleared on the next
    /// reconcile once the base re-seals conflict-free.
    pub held: Vec<String>,
    /// Exact per-branch failures from graph movement, flatten recovery, or publication.
    pub failed: Vec<ReconcileFailure>,
    /// The full three-way conflict diagnostic per branch in `conflicted`,
    /// captured INSIDE the rebase before it was rolled back. That capture is the
    /// only window in which any of it exists: afterwards the branch is clean
    /// again, so there is nothing left to enumerate and a later probe would
    /// report none.
    pub conflict_diagnostics: std::collections::HashMap<String, ConflictDiagnostic>,
    /// Branches in merge catch-up mode that were brought current by one merge
    /// commit. Each is also in `rebased_clean`, so every downstream consumer
    /// treats it as a clean catch-up; this carries what the merge brought in.
    pub merged_clean: std::collections::HashMap<String, MergedCatchUp>,
    /// Branches in merge catch-up mode whose merge was refused untouched (an
    /// ambiguous merge base). Each is also in `failed`.
    pub merge_refused: std::collections::HashMap<String, String>,
}

/// One clean merge catch-up: the merge commit and the base range it brought in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedCatchUp {
    pub merge: String,
    pub old_base: String,
    pub new_base: String,
    /// Whether a committed resolution was carried into the merge, which
    /// changes the branch's own patch where a plain clean merge does not.
    pub resolved: bool,
}

/// The refusal an agent receives when its branch cannot be rebased onto an
/// advanced base.
///
/// Written for the world Cairn actually runs: agents hold detached git
/// worktrees, not jj workspaces, so there are no conflict markers to resolve and
/// no seal to re-take. The rebase was rolled back, so the branch is untouched —
/// which is the fact that makes this actionable: the agent merges the new base
/// into their own branch with ordinary writes and commits the result.
pub fn base_conflict_refusal(target_branch: &str, source_branch: &str, paths: &[String]) -> String {
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
pub fn conflicted_ancestry_refusal(
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
///
/// This is the one primitive every store fold moves a target through, so it is
/// where the landing parse gate lives: a fold whose tree would leave a file
/// unparseable that parses on the target is refused before the bookmark moves
/// (see [`landing_parse_refusal`]).
pub fn merge_into_bookmark(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    child_branch: &str,
) -> Result<(), String> {
    if let Some(refusal) = landing_parse_refusal(jj, store, integration_branch, child_branch) {
        return Err(refusal);
    }
    fold_into_bookmark(jj, store, integration_branch, child_branch)
}

/// [`merge_into_bookmark`] for a tree that was already judged off the store
/// lock: the parse gate (and the landing's identifier invariants) ran against
/// a prospective commit with tree `expected_tree`, so under the lock the fold
/// only proves it is folding that very tree and moves the bookmark. A tree that
/// differs is refused unmoved, and the caller judges again.
pub fn fold_validated_into_bookmark(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    child_branch: &str,
    expected_tree: &str,
) -> Result<(), String> {
    let child = bookmark_commit(jj, store, child_branch)
        .ok_or_else(|| format!("bookmark `{child_branch}` did not resolve to one commit"))?;
    let tree = sealed_tree_hash_via_git(jj, store, &child)?;
    if tree != expected_tree {
        return Err(format!(
            "Refusing to fold `{child_branch}` into `{integration_branch}`: its tree {tree} is not \
             the tree {expected_tree} that was judged for this landing. Merge again so the tree \
             that lands is the one judged."
        ));
    }
    fold_into_bookmark(jj, store, integration_branch, child_branch)
}

fn fold_into_bookmark(
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
            if let Some(refusal) = &diagnostic.validation {
                return Err(unparseable_landing_refusal(
                    target_branch,
                    source_branch,
                    &refusal.failures,
                ));
            }
            return Err(base_conflict_refusal(
                target_branch,
                source_branch,
                &diagnostic.conflicting_paths(),
            ));
        }
    }
    // The source is now a descendant of `dest` (and thus of the local target
    // bookmark, which `dest` advanced from), so this FF can never go backwards.
    merge_into_bookmark(jj, store, target_branch, source_branch)
}

/// The exact branch-only squash that was accepted and exported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SquashReport {
    pub squashed_commit: String,
    pub collapsed_conflicted_commits: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SquashError {
    CoordinatesChanged,
    TimedOut,
    EvidenceLimit { limit: usize },
    Repository(String),
}

impl std::fmt::Display for SquashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CoordinatesChanged => {
                f.write_str("squash source or base moved; prepare the landing again")
            }
            Self::TimedOut => f.write_str("squash timed out: operation budget exhausted"),
            Self::EvidenceLimit { limit } => write!(
                f,
                "squash requires more than {limit} exact predecessor receipts; rebuild the branch from its current base while preserving its own changes"
            ),
            Self::Repository(error) => f.write_str(error),
        }
    }
}
impl std::error::Error for SquashError {}

impl From<super::AdvanceComputeError> for SquashError {
    fn from(error: super::AdvanceComputeError) -> Self {
        match error {
            super::AdvanceComputeError::CoordinatesChanged => Self::CoordinatesChanged,
            super::AdvanceComputeError::TimedOut => Self::TimedOut,
            super::AdvanceComputeError::EvidenceLimit { limit } => Self::EvidenceLimit { limit },
            other => Self::Repository(other.to_string()),
        }
    }
}
impl From<super::AdvanceInstallError> for SquashError {
    fn from(error: super::AdvanceInstallError) -> Self {
        match error {
            super::AdvanceInstallError::CoordinatesChanged => Self::CoordinatesChanged,
            super::AdvanceInstallError::Expired => Self::TimedOut,
            super::AdvanceInstallError::Repository(error) => Self::Repository(error),
        }
    }
}

/// Squash only this branch onto the exact base, retaining its complete tree.
/// The caller holds the canonical store writer guard and validates the declared
/// target. No descendant, workspace, rider or unrelated bookmark is rewritten.
/// The same detached candidate installer performs the fresh source/base CAS and
/// verified export; a refused export restores only this exact accepted source.
pub fn squash_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    expected_source: &str,
    base_rev: &str,
    message: &str,
) -> Result<SquashReport, SquashError> {
    let remaining = jj
        .operation_deadline
        .map(|deadline| deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or(JJ_DEFAULT_TIMEOUT);
    if remaining.is_zero() {
        return Err(SquashError::TimedOut);
    }
    let bounded = jj.with_operation_budget(remaining);
    // base_rev may be an immutable T, not a declared bookmark. Do not manufacture
    // a bookmark from a SHA: the core independently guards the actual target.
    let snapshot = super::advance::capture_replay(store, branch, base_rev, false)
        .map_err(SquashError::Repository)?;
    if !snapshot
        .coordinate()
        .head
        .eq_ignore_ascii_case(expected_source)
    {
        return Err(SquashError::CoordinatesChanged);
    }
    let (candidate, collapsed_conflicted_commits) =
        super::advance::compute_branch_squash(&bounded, snapshot, message, remaining)?;
    let installed = super::advance::install_branch_advance_exported(&bounded, candidate)?;
    if let Err(error) = installed.export {
        let cleanup = bounded.cleanup_env();
        if let Err(restore) = super::reconcile::restore_bookmark(
            &cleanup,
            store,
            branch,
            &installed.head,
            &installed.coordinate.head,
        ) {
            return Err(SquashError::Repository(format!(
                "{error}\n{}",
                rollback_incomplete(&format!("source-only squash rollback: {restore}")),
            )));
        }
        return Err(SquashError::Repository(error));
    }
    Ok(SquashReport {
        squashed_commit: installed.head,
        collapsed_conflicted_commits,
    })
}

/// Idempotently mark a remote bookmark as jj-tracked so a local push of it is
/// accepted. jj refuses to push a local bookmark whose `@origin` counterpart is
/// untracked ("Non-tracking remote bookmark … exists"), which happens when
/// origin's ref was imported rather than pushed by this store's jj. A no-op when
/// already tracked; errors when there is no such remote bookmark, e.g. a
/// no-remote project. Tracking merges the remote target into the local bookmark,
/// so callers track only a remote the local bookmark equals or descends from.
pub fn track_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "track",
            branch,
            "--remote=origin",
            "--ignore-working-copy",
        ],
        "jj bookmark track",
    )
    .map(|_| ())
}

/// Track `branch@origin` when it is untracked AND carries this branch's own
/// history: the local bookmark equals it or descends from it. Returns whether it
/// tracked anything.
///
/// An untracked remote copy of a managed branch is the residue of an import
/// (a stale-push recovery's fetch, or a ref another clone pushed). It is doubly
/// harmful: jj refuses to push over it, and jj's default `immutable_heads()`
/// includes `untracked_remote_bookmarks()`, so every commit it reaches becomes
/// immutable and a base-advance rebase of the branch is refused. When the lineage
/// matches, tracking is exactly the state a push of our own would have recorded.
/// A remote whose history is not an ancestor of the local branch is left
/// untracked: tracking it would fold a foreign target into the local bookmark.
/// The caller holds the per-store lock.
pub fn track_own_remote_of_same_lineage(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<bool, String> {
    let untracked =
        format!("untracked_remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")");
    let Some(remote) = revset_commit_checked(jj, store, &untracked)? else {
        return Ok(false);
    };
    let Some(local) = bookmark_commit_checked(jj, store, branch)? else {
        return Ok(false);
    };
    if local != remote && !revset_descends_from(jj, store, &local, &remote) {
        return Ok(false);
    }
    track_bookmark(jj, store, branch)?;
    log::info!(
        "jj: tracked `{branch}@origin` ({remote}); the local bookmark ({local}) carries its history"
    );
    Ok(true)
}

/// A verified store-bookmark push failure. The two remote-state refusals are
/// recoverable by fetch-and-converge; every other failure keeps its original
/// diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreBookmarkPushError {
    /// jj's recorded view of `branch@origin` no longer matches origin.
    StaleRemote(String),
    /// `branch@origin` exists but is not tracked, so jj will not push over it.
    UntrackedRemote(String),
    Failed(String),
}

impl std::fmt::Display for StoreBookmarkPushError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleRemote(error) | Self::UntrackedRemote(error) | Self::Failed(error) => {
                formatter.write_str(error)
            }
        }
    }
}

/// Push an already-advanced store bookmark to origin with `--ignore-working-copy`
/// (the store's default `@` may be stale after a fold/rebase). Used to advance
/// the integration tip after a fold, a cleanly-rebased sibling's PR head, and a
/// managed branch's PR head; jj's remote-tracking model accepts a rewritten
/// bookmark without a force-push.
///
/// The push is bracketed by verification: the backing git ref must equal the
/// bookmark commit BEFORE the push (or the push carries a stale tree — the
/// half-landed-PR mode), and origin's tip must equal it AFTER (or the push
/// reported a success that origin did not record — the phantom-PR mode).
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
            } else if lowered.contains("non-tracking remote bookmark") {
                StoreBookmarkPushError::UntrackedRemote(error)
            } else {
                StoreBookmarkPushError::Failed(error)
            },
        );
    }
    match published.as_deref() {
        Some(published) => {
            confirm_origin_tip(jj, store, branch, published).map_err(StoreBookmarkPushError::Failed)
        }
        None => Ok(()),
    }
}

/// Unclassified form for merge publication paths that do not recover stale or
/// untracked remotes themselves. Managed agent publication goes through
/// `publish_managed_branch`, which does.
pub fn push_store_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
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
    /// jj merged the two sides without recording a conflict, and the result
    /// does not parse: a file both sides changed parsed on each of them and not
    /// in the merge. Line merges cannot see syntax, so two appends to one list
    /// can merge "cleanly" into a list closed twice. The remedy is the content
    /// one, guided by the parser's first error rather than by markers.
    Unparseable,
}

impl ConflictCondition {
    /// The stable wire/storage name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContentConflict => "content_conflict",
            Self::BaseDrift => "base_drift",
            Self::Unparseable => "unparseable",
        }
    }

    /// The condition a stored name means. A name this build does not know
    /// reads as a content conflict, the condition whose remedy is always safe.
    pub fn from_stored(name: &str) -> Self {
        match name {
            "base_drift" => Self::BaseDrift,
            "unparseable" => Self::Unparseable,
            _ => Self::ContentConflict,
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
    /// Set when jj recorded NO conflict but the rebased tip's tree root carried
    /// jj conflict scaffolding, so the verified export refused it. Then
    /// `conflicted_tip` is that refused commit and the conflicting paths are the
    /// files both sides changed, since jj named none.
    pub scaffolding: Option<ScaffoldingRefusal>,
    /// Set when jj recorded NO conflict but the rebased tree does not parse at
    /// a path both sides changed (see [`ConflictCondition::Unparseable`]). Then
    /// `conflicted_tip` is that refused commit and the conflicting paths are
    /// exactly the files that failed to parse.
    pub validation: Option<ValidationRefusal>,
}

/// A rebased tip refused at export because its tree root carried jj conflict
/// scaffolding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaffoldingRefusal {
    /// The commit the bookmark would have been exported to.
    pub commit: String,
    /// The scaffolding entries at its tree root, e.g. `JJ-CONFLICT-README`.
    pub entries: Vec<String>,
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
    /// The rebase recorded a conflict on the tip, so it was ROLLED BACK: the
    /// branch sits on exactly the commit it held before, and nothing
    /// conflict-flagged was ever exported.
    ///
    /// The diagnostic carries everything the rollback is about to make
    /// unobservable: the immutable three-way coordinates, the incoming change's
    /// whole file set, and which condition this is.
    Conflicted { diagnostic: Box<ConflictDiagnostic> },
}

/// Replay and heal a branch through the canonical detached engine, then install
/// and verify its export under the caller-held canonical writer lock. Conflicts
/// never publish a live bookmark or an intermediate conflict-flagged history.
pub fn rebase_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
) -> Result<RebaseOutcome, String> {
    rebase_branch_onto_with_resolution(jj, store, branch, dest, None)
}

/// What a committed resolution puts at one path of the rebased tip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CarriedContent {
    /// The pre-rebase tip's file, whole. Exact only when the destination still
    /// holds what the resolution was made against.
    FromTip,
    /// This content: the resolution carried onto the destination.
    Text(String),
}

/// An explicitly requested replay's committed resolution, already carried onto
/// the destination it will land on.
///
/// Built only by the durable conflict-session replay. An automatic rebase
/// passes none and keeps the fail-closed rollback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionCarry {
    /// The tip the carry was computed from. A branch found anywhere else is
    /// refused before the graph moves: content computed from one tip, laid over
    /// another, is exactly the silent overwrite this exists to prevent.
    pub expected_pre_tip: String,
    /// Each carried path and what lands there. A session path absent from this
    /// list is deliberately left to jj's own merge, so a conflict there rolls
    /// the replay back.
    pub paths: Vec<(String, CarriedContent)>,
}

impl ResolutionCarry {
    /// Every path restored whole from the expected tip.
    pub fn from_tip(expected_pre_tip: &str, paths: &[String]) -> Self {
        Self {
            expected_pre_tip: expected_pre_tip.to_string(),
            paths: paths
                .iter()
                .map(|path| (path.clone(), CarriedContent::FromTip))
                .collect(),
        }
    }
}

/// Undo a rebase that landed in the store but whose follow-through failed, and
/// say truthfully which of the two outcomes the store is now in.
///
/// Only a proven rollback earns "unchanged": the store back at `pre_op` AND
/// the backing git ref verified back with it. Anything less is written as
/// [`rollback_incomplete`], which the reconcile classifier ranks above every
/// other cause — a lost lock nested inside it included — so the branch is
/// reported as needing repair rather than quietly retried.
pub(crate) fn roll_back_rebase(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    pre_op: &str,
    failure: String,
) -> String {
    let cleanup = jj.cleanup_env();
    let jj = &cleanup;
    let undone = super::reconcile::restore_operation_and_export(
        jj,
        store,
        pre_op,
        branch,
        "jj git export (failed rebase rolled back)",
    );
    match undone {
        Ok(()) => format!(
            "{failure}\nThe rebase was rolled back to operation {pre_op}, so the branch is unchanged."
        ),
        Err(rollback) => format!(
            "{failure}\n{}",
            rollback_incomplete(&format!(
                "rolling the rebase back to operation {pre_op} also failed: {rollback}. The branch \
                 may sit on the rebased tip in the store and not in git; do not publish it."
            ))
        ),
    }
}

/// Rebase a branch while honoring an explicitly identified committed resolution.
///
/// `resolution` is supplied only by the explicit durable conflict-session
/// replay operation. A normal automatic rebase passes none and retains the
/// fail-closed rollback behavior.
pub(crate) fn rebase_branch_onto_with_resolution(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
    resolution: Option<&ResolutionCarry>,
) -> Result<RebaseOutcome, String> {
    // Remote-tracking repair is a deliberate locked mutation, before capture.
    // The detached engine itself never tracks, imports or repairs live names.
    if let Err(error) = track_own_remote_of_same_lineage(jj, store, branch) {
        log::warn!("jj replay: could not inspect or track own remote for {branch}: {error}");
    }
    let snapshot = super::advance::capture_replay(store, branch, dest, false)?;
    let coordinate = snapshot.coordinate().clone();
    let budget = jj
        .operation_deadline
        .map(|deadline| deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or(super::env::JJ_DEFAULT_TIMEOUT);
    let candidate = match super::advance::compute_legacy_replay(jj, snapshot, resolution, budget) {
        Ok(candidate) => candidate,
        Err(AdvanceComputeError::Conflict(diagnostic)) => {
            return Ok(RebaseOutcome::Conflicted { diagnostic })
        }
        Err(error) => return Err(error.to_string()),
    };
    let installed =
        super::advance::install_legacy_replay(jj, candidate).map_err(|error| error.to_string())?;
    if let Err(error) = installed.export {
        return Err(roll_back_rebase(
            jj,
            store,
            branch,
            &coordinate.operation,
            error,
        ));
    }
    let selection = rebase_side_selection(
        jj,
        store,
        &coordinate.base,
        &coordinate.head,
        &installed.head,
    );
    log_rebase_side_selection(branch, dest, &coordinate.head, &installed.head, &selection);
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
pub fn merge_side_patch(
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
/// Called from inside [`rebase_branch_onto_with_resolution`] between the recorded conflict and
/// the rollback, under the per-store lock every caller holds. Every step is
/// advisory in the sense that it degrades to a missing field rather than failing
/// the rebase: an instrument must never be able to break the machinery it
/// measures. What it must NOT do is guess — conflict membership comes from jj's
/// real rebase result (`conflicting`), never from path overlap.
pub(crate) fn capture_conflict_diagnostic(
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
        scaffolding: None,
        validation: None,
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
pub fn diff_name_status(jj: &JjEnv, store: &Path, from: &str, to: &str) -> Vec<(String, String)> {
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
    let before = bookmark_commit(jj, store, branch);
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
    let Err(error) = export_git_verified(
        jj,
        store,
        true,
        "jj git export (bookmark fast-forward)",
        &[(branch, dest)],
    ) else {
        return Ok(());
    };
    // A refused export (a frozen ref, or a tree carrying conflict scaffolding)
    // leaves git where it was, so the store goes back too: a bookmark ahead of
    // its git ref is the drift every later import turns into a conflicted name.
    let Some(before) = before else {
        return Err(error);
    };
    // Rolled back only from `dest`: a bookmark another writer has moved since is
    // theirs, and moving it back would drop what they published.
    let restored = crate::repoint_bookmark_if_unchanged(
        store,
        branch,
        &[dest.to_string()],
        &before,
        &format!("roll back fast-forward of `{branch}` to {before}"),
    )
    .and_then(|()| {
        export_git_verified(
            jj,
            store,
            true,
            "jj git export (fast-forward rolled back)",
            &[(branch, before.as_str())],
        )
    });
    Err(match restored {
        Ok(()) => format!("{error}\nThe fast-forward was rolled back to {before}."),
        Err(rollback) => format!(
            "{error}\n{}",
            rollback_incomplete(&format!(
                "rolling the fast-forward of `{branch}` back to {before} also failed: {rollback}"
            ))
        ),
    })
}
