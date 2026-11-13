//! Captured coordinates and object lifetime for detached branch advancement.
//!
//! Captures never reconcile operation heads or change bookmarks. The snapshot
//! owns a reclamation lease until computation and installation have finished.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{AsyncReadExt as _, StreamExt as _};
use jj_lib::backend::{CommitId, CopyId, TreeValue};
use jj_lib::commit::Commit;
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::RefName;
use jj_lib::repo::{MutableRepo, ReadonlyRepo, Repo as _};
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::revset::ResolvedRevsetExpression;
use jj_lib::rewrite::CommitRewriter;

#[cfg(test)]
thread_local! {
    static REMOTE_PROOF_ANCESTOR_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(4095) };
}

/// The result of explicit, writer-locked operation-head repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledOperations {
    pub before: Vec<String>,
    pub after: String,
}

/// Explicitly reconcile operation heads under the caller's canonical store
/// writer lock. Unlike literal loading, this function may publish a merged view.
/// A singleton is returned without loading, rewriting or publishing anything.
pub fn settle_operation_heads(
    jj: &super::JjEnv,
    store: &Path,
) -> Result<SettledOperations, String> {
    jj.check_operation_budget("settle operation heads")?;
    let workspace = crate::load_workspace(store)?;
    let loader = workspace.repo_loader();
    block_on(async {
        let mut before: Vec<_> = loader
            .op_heads_store()
            .get_op_heads()
            .await
            .map_err(|error| format!("read operation heads: {error}"))?
            .into_iter()
            .map(|id| id.hex())
            .collect();
        before.sort();
        if before.is_empty() {
            return Err("cannot repair a store with no operation heads".to_string());
        }
        if before.len() == 1 {
            return Ok(SettledOperations {
                after: before[0].clone(),
                before,
            });
        }
        let repo = loader
            .load_at_head()
            .await
            .map_err(|error| format!("settle operation heads: {error}"))?;
        Ok(SettledOperations {
            before,
            after: repo.op_id().hex(),
        })
    })
}

/// Both bookmark coordinates captured from one exact operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvanceCoordinate {
    pub branch: String,
    pub head: String,
    pub base_branch: String,
    pub base: String,
    pub operation: String,
}

/// How a snapshot's base was named, and so how installation re-checks it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseRef {
    /// The base branch's local bookmark target.
    Bookmark,
    /// Any revision symbol the store resolves.
    Symbol,
    /// One exact commit on the base branch's history. `base_branch` keeps the
    /// relationship's name; a commit never moves, so only the branch is
    /// compared again.
    Exact,
}

/// An immutable repository snapshot, kept alive independently of bookmark
/// movement. Core must separately validate its declared integration relationship
/// under the canonical store lock before installing any computation from it.
pub struct BranchAdvanceSnapshot {
    store: PathBuf,
    repo: Arc<ReadonlyRepo>,
    coordinate: AdvanceCoordinate,
    base_ref: BaseRef,
    dependent_bookmarks: Option<HashSet<String>>,
    cost: Option<ReplayCostProbe>,
    regenerable: Option<RegenerablePaths>,
    _lease: super::object_lease::ObjectLease,
}

impl BranchAdvanceSnapshot {
    /// Let computation from this snapshot settle a conflict whose every path
    /// `paths` covers by taking the branch's side of each (see
    /// [`RegenerablePaths`]). Without it every conflict refuses.
    pub fn with_regenerable_paths(mut self, paths: RegenerablePaths) -> Self {
        self.regenerable = Some(paths);
        self
    }

    /// Report what computation from this snapshot costs into `probe`, phase by
    /// phase as it runs, so a computation that runs out of budget still leaves
    /// how far it got.
    pub fn with_cost_probe(mut self, probe: ReplayCostProbe) -> Self {
        self.cost = Some(probe);
        self
    }

    fn note(&self, update: impl FnOnce(&mut ReplayCostObservation)) {
        if let Some(cost) = &self.cost {
            cost.record(update);
        }
    }

    /// Supply the complete transitive declared-dependent closure captured under
    /// the same store guard. These names are not independent ownership anchors.
    /// They still protect older versions during literal divergence inspection.
    ///
    /// This is source-operation evidence, not another install CAS: a dependent
    /// created or retargeted later keeps its old content and its next preparation
    /// uses predecessor proof to catch up. No dependent is advanced here.
    pub fn with_dependent_bookmarks(mut self, names: Vec<String>) -> Self {
        self.dependent_bookmarks = Some(names.into_iter().collect());
        self
    }

    pub fn coordinate(&self) -> &AdvanceCoordinate {
        &self.coordinate
    }

    /// Return H's exact tree when the captured base is already an ancestor and
    /// neither H nor its branch-owned history needs conflict healing. This is a
    /// pure index/object query: no CLI, name repair or view writes. `budget`
    /// starts at entry and is checked around every index, object, tree and
    /// range step; expiry is [`AdvanceComputeError::TimedOut`], never `None`.
    pub fn already_prepared(
        &self,
        budget: Duration,
    ) -> Result<Option<String>, AdvanceComputeError> {
        let deadline = Deadline(Instant::now() + budget);
        deadline.check()?;
        futures::executor::block_on(self.already_prepared_within(&deadline))
    }

    async fn already_prepared_within(
        &self,
        deadline: &Deadline,
    ) -> Result<Option<String>, AdvanceComputeError> {
        // Every answer, `None` included, is given only inside the budget: a
        // probe that overran is a timeout, never "needs a replay".
        deadline.check()?;
        let head = CommitId::try_from_hex(&self.coordinate.head)
            .ok_or_else(|| compute_error("invalid captured head"))?;
        let base = CommitId::try_from_hex(&self.coordinate.base)
            .ok_or_else(|| compute_error("invalid captured base"))?;
        let ancestor = self
            .repo
            .index()
            .is_ancestor(&base, &head)
            .map_err(compute_error)?;
        deadline.check()?;
        if !ancestor {
            return Ok(None);
        }
        let tip = self.repo.store().get_commit(&head).map_err(compute_error)?;
        let base_commit = self.repo.store().get_commit(&base).map_err(compute_error)?;
        deadline.check()?;
        if tip.has_conflict() || base_commit.has_conflict() {
            return Ok(None);
        }
        let roots = tip.tree().trees().await.map_err(compute_error)?;
        deadline.check()?;
        let Some(root) = roots.as_resolved() else {
            return Ok(None);
        };
        let mut names = Vec::new();
        for entry in root.entries_non_recursive() {
            deadline.check()?;
            names.push(entry.name().as_internal_str());
        }
        let scaffolded = !cairn_common::conflict_scaffolding::conflict_scaffolding_in_root_listing(
            &names.join("\n"),
        )
        .is_empty();
        deadline.check()?;
        if scaffolded {
            return Ok(None);
        }
        let commits = branch_commits(self.repo.as_ref(), &head, &base, deadline, None).await?;
        for commit in &commits {
            deadline.check()?;
            if commit.has_conflict() {
                return Ok(None);
            }
        }
        deadline.check()?;
        Ok(tip.tree_ids().as_resolved().map(|id| id.hex()))
    }

    /// Return exact input commits reachable from the captured head, without
    /// loading objects or mutating the view. The caller supplies the remaining
    /// absolute-operation budget; expiry is never a partial subset. Well-formed
    /// unindexed IDs are excluded: every ancestor of indexed H is indexed.
    /// Malformed IDs refuse the whole query. Consumers must treat exclusion as
    /// withholding evidence, never as permission to discard or overwrite work.
    pub fn reachable_from_head(
        &self,
        commits: &[String],
        budget: Duration,
    ) -> Result<Vec<String>, AdvanceComputeError> {
        let deadline = Deadline(Instant::now() + budget);
        deadline.check()?;
        let head = CommitId::try_from_hex(&self.coordinate.head)
            .ok_or_else(|| compute_error("invalid captured head"))?;
        let mut reachable = Vec::new();
        for commit in commits {
            deadline.check()?;
            let id = parsed_evidence(self.repo.as_ref(), commit)?;
            if !self.repo.index().has_id(&id).map_err(compute_error)? {
                continue;
            }
            if self
                .repo
                .index()
                .is_ancestor(&id, &head)
                .map_err(compute_error)?
            {
                reachable.push(commit.clone());
            }
            deadline.check()?;
        }
        deadline.check()?;
        Ok(reachable)
    }

    /// Compare BOTH bookmarks in a freshly loaded, settled repository.
    ///
    /// This is a preflight, not an installer or a replacement for the caller's
    /// canonical writer lock. Installation must perform this comparison in its
    /// own transaction immediately before publication.
    pub fn coordinates_match(&self) -> Result<bool, String> {
        block_on(async {
            let current = crate::load_repo_at_head(&self.store)
                .await
                .map_err(|error| error.to_string())?;
            Ok(self.matches_repo(&current))
        })
    }

    fn matches_repo(&self, current: &ReadonlyRepo) -> bool {
        let expected = &self.coordinate;
        normal_target(current, &expected.branch).is_ok_and(|id| id.hex() == expected.head)
            && match self.base_ref {
                BaseRef::Bookmark => normal_target(current, &expected.base_branch)
                    .is_ok_and(|id| id.hex() == expected.base),
                BaseRef::Symbol => crate::resolve_symbol_at(current, &expected.base_branch)
                    .is_ok_and(|id| id.hex() == expected.base),
                BaseRef::Exact => true,
            }
    }

    /// The captured operation, never re-resolved through its former head name.
    pub fn operation(&self) -> String {
        self.repo.op_id().hex()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvanceCaptureError {
    Busy(String),
    UnsettledOperations {
        heads: usize,
    },
    UnresolvedCoordinate {
        name: String,
    },
    /// An exact destination that is not in the base branch's history.
    DestinationOffBase {
        destination: String,
        base_branch: String,
    },
    Repository(String),
}

impl std::fmt::Display for AdvanceCaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(reason) | Self::Repository(reason) => f.write_str(reason),
            Self::UnsettledOperations { heads } => write!(f, "the store has {heads} operation heads; literal inspection requires one settled operation"),
            Self::UnresolvedCoordinate { name } => write!(f, "advance coordinate `{name}` is missing or conflicted"),
            Self::DestinationOffBase { destination, base_branch } => write!(f, "`{destination}` is no longer in `{base_branch}`'s history"),
        }
    }
}

impl std::error::Error for AdvanceCaptureError {}

/// Capture a branch and its integration target from a single operation.
///
/// Runs synchronously on the caller's blocking worker. The caller captures its
/// declared base relationship under the canonical store lock; VCS knows only
/// bookmarks, not the host's relationship database. Garbage collection refuses
/// while a snapshot is alive, including after either bookmark moves.
pub fn capture_branch_advance(
    store: &Path,
    branch: &str,
    base_branch: &str,
) -> Result<BranchAdvanceSnapshot, AdvanceCaptureError> {
    if branch.is_empty() || base_branch.is_empty() || branch == base_branch {
        return Err(AdvanceCaptureError::UnresolvedCoordinate {
            name: if branch.is_empty() {
                branch.into()
            } else {
                base_branch.into()
            },
        });
    }
    capture_replay_typed(store, branch, base_branch, true)
}

/// Capture a branch against `destination`, one exact commit of `base_branch`'s
/// history, instead of the base's live target: an open conflict session's
/// recorded base, which stays the branch's next catch-up step until it lands.
///
/// The coordinate keeps `base_branch` as its base name, so every outcome is
/// keyed by the relationship (`main` at `destination`), never by a commit
/// standing in for a branch name. Only the source bookmark is mutable here:
/// later movement of the live base does not retarget the capture. The
/// destination must be a full commit id (symbols and abbreviations are
/// refused even when they resolve) and an ancestor of, or equal to, the base's
/// local target.
pub fn capture_branch_onto(
    store: &Path,
    branch: &str,
    base_branch: &str,
    destination: &str,
) -> Result<BranchAdvanceSnapshot, AdvanceCaptureError> {
    let unresolved = |name: &str| AdvanceCaptureError::UnresolvedCoordinate { name: name.into() };
    let Some(destination_id) = CommitId::try_from_hex(destination) else {
        return Err(unresolved(destination));
    };
    if branch.is_empty() || base_branch.is_empty() || branch == base_branch {
        return Err(unresolved(if branch.is_empty() {
            branch
        } else {
            base_branch
        }));
    }
    let mut snapshot = capture_replay_typed(store, branch, destination, false)?;
    if destination != snapshot.coordinate.base {
        return Err(unresolved(destination));
    }
    let live = normal_target(&snapshot.repo, base_branch).map_err(|_| unresolved(base_branch))?;
    let on_base = snapshot
        .repo
        .index()
        .is_ancestor(&destination_id, &live)
        .map_err(|error| AdvanceCaptureError::Repository(error.to_string()))?;
    if !on_base {
        return Err(AdvanceCaptureError::DestinationOffBase {
            destination: destination.to_string(),
            base_branch: base_branch.to_string(),
        });
    }
    snapshot.coordinate.base_branch = base_branch.to_string();
    snapshot.base_ref = BaseRef::Exact;
    Ok(snapshot)
}

pub(super) fn capture_replay(
    store: &Path,
    branch: &str,
    base_branch: &str,
    local_base: bool,
) -> Result<BranchAdvanceSnapshot, String> {
    capture_replay_typed(store, branch, base_branch, local_base).map_err(|error| error.to_string())
}

fn capture_replay_typed(
    store: &Path,
    branch: &str,
    base_branch: &str,
    local_base: bool,
) -> Result<BranchAdvanceSnapshot, AdvanceCaptureError> {
    let git_dir = super::store_git_dir(store).ok_or_else(|| {
        AdvanceCaptureError::Repository(format!(
            "resolve backing Git directory for {}",
            store.display()
        ))
    })?;
    let lease =
        super::object_lease::ObjectLease::acquire(&git_dir).map_err(|error| match error {
            super::object_lease::LeaseError::Busy => {
                AdvanceCaptureError::Busy("store maintenance is active".into())
            }
            super::object_lease::LeaseError::Repository(error) => {
                AdvanceCaptureError::Repository(error)
            }
        })?;
    let workspace = crate::load_workspace(store).map_err(AdvanceCaptureError::Repository)?;
    let loader = workspace.repo_loader();
    let repo = futures::executor::block_on(async {
        let heads = loader
            .op_heads_store()
            .get_op_heads()
            .await
            .map_err(|error| AdvanceCaptureError::Repository(error.to_string()))?;
        let [head] = heads.as_slice() else {
            return Err(AdvanceCaptureError::UnsettledOperations { heads: heads.len() });
        };
        let operation = loader
            .load_operation(head)
            .await
            .map_err(|error| AdvanceCaptureError::Repository(error.to_string()))?;
        loader
            .load_at(&operation)
            .await
            .map_err(|error| AdvanceCaptureError::Repository(error.to_string()))
    })?;
    let unresolved = |name: &str| AdvanceCaptureError::UnresolvedCoordinate { name: name.into() };
    let coordinate = AdvanceCoordinate {
        branch: branch.to_string(),
        head: normal_target(&repo, branch)
            .map_err(|_| unresolved(branch))?
            .hex(),
        base_branch: base_branch.to_string(),
        base: if local_base {
            normal_target(&repo, base_branch).map_err(|_| unresolved(base_branch))?
        } else {
            crate::resolve_symbol_at(&repo, base_branch).map_err(|_| unresolved(base_branch))?
        }
        .hex(),
        operation: repo.op_id().hex(),
    };
    Ok(BranchAdvanceSnapshot {
        store: store.to_path_buf(),
        repo,
        coordinate,
        base_ref: if local_base {
            BaseRef::Bookmark
        } else {
            BaseRef::Symbol
        },
        dependent_bookmarks: None,
        cost: None,
        regenerable: None,
        _lease: lease,
    })
}

/// A validated detached advancement. Construction is private to the VCS engines:
/// callers cannot turn an arbitrary commit into permission to advance a branch.
pub struct BranchAdvanceCandidate {
    snapshot: BranchAdvanceSnapshot,
    head: Commit,
    healing: super::AncestryHeal,
    dropped_commit_ids: Vec<String>,
    stale_regenerable: Vec<StaleRegenerable>,
    expires_at: Instant,
}

impl BranchAdvanceCandidate {
    pub fn coordinate(&self) -> &AdvanceCoordinate {
        self.snapshot.coordinate()
    }

    /// The regenerable paths this candidate carries with the branch's bytes
    /// in place of a conflict, each stale until it is regenerated. Empty for
    /// a candidate that settled no conflict that way.
    pub fn stale_regenerable(&self) -> &[StaleRegenerable] {
        &self.stale_regenerable
    }

    pub fn head(&self) -> String {
        self.head.id().hex()
    }

    /// Exact accepted removed-from-this-branch claims: actual drops and source
    /// identities superseded by proven base predecessors. The historical field
    /// name does not imply all their content was discarded.
    /// Available before install for a pending witness; publication authority only
    /// begins when this complete candidate is durably accepted.
    pub fn dropped_commit_ids(&self) -> &[String] {
        &self.dropped_commit_ids
    }

    pub fn tree(&self) -> Result<String, String> {
        self.head
            .tree_ids()
            .as_resolved()
            .map(|tree| tree.hex())
            .ok_or_else(|| "detached advance candidate has unresolved trees".to_string())
    }
}

/// A refusal from detached computation. Conflict diagnostics refer only to the
/// captured immutable coordinates; the caller must revalidate before retaining
/// them or opening a conflict session.
#[derive(Debug, Clone)]
pub enum AdvanceComputeError {
    Conflict(Box<super::ConflictDiagnostic>),
    BaseConflicted { base: String },
    TimedOut,
    CoordinatesChanged,
    AmbiguousOwnership { commit: String },
    UnknownEvidence { commit: String },
    Busy,
    UnaccountedRemoteCommits { commits: Vec<String> },
    EvidenceLimit { limit: usize },
    Repository(String),
}

impl std::fmt::Display for AdvanceComputeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(diagnostic) => write!(
                f,
                "advance conflicts with base: {}",
                diagnostic.conflicting_paths().join(", ")
            ),
            Self::BaseConflicted { base } => {
                write!(f, "advance base `{base}` carries recorded conflicts")
            }
            Self::TimedOut => {
                f.write_str("detached branch advance timed out: operation budget exhausted")
            }
            Self::CoordinatesChanged => f.write_str("detached replay inputs changed"),
            Self::AmbiguousOwnership { commit } => write!(
                f,
                "merge `{commit}` requires complete declared-dependent ownership context"
            ),
            Self::EvidenceLimit { limit } => write!(
                f,
                "branch history/proof exceeds the {limit}-entry bound; rebuild the branch from its current base while preserving its own changes"
            ),
            Self::Busy => f.write_str("store maintenance is active"),
            Self::UnaccountedRemoteCommits { commits } => {
                write!(f, "origin has unaccounted commits: {}", commits.join(", "))
            }
            Self::UnknownEvidence { commit } => write!(
                f,
                "advance evidence `{commit}` is not a full indexed commit"
            ),
            Self::Repository(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for AdvanceComputeError {}

fn compute_error(error: impl std::fmt::Display) -> AdvanceComputeError {
    let error = error.to_string();
    if super::is_jj_timeout_error(&error) {
        AdvanceComputeError::TimedOut
    } else {
        AdvanceComputeError::Repository(error)
    }
}

/// Removed-from-this-branch evidence from an accepted installation of this branch.
/// The caller must exclude pending/unaccepted records and validate project/store
/// provenance. Head ancestry is independently checked against the exact local T.
#[derive(Debug, Clone)]
pub struct AcceptedAdvanceDrops {
    pub head_after: String,
    pub dropped_commit_ids: Vec<String>,
}

/// Prove every origin-only commit using hidden status, exact predecessor
/// ancestry, or accepted drop evidence. This is not permission to rewrite an
/// unmanaged/default branch, nor a remote lease: those policies belong to the
/// publisher. Unknown evidence and exhausted budgets refuse the whole proof.
pub fn origin_copy_is_superseded_with_accepted(
    jj: &super::JjEnv,
    store: &Path,
    branch: &str,
    local: &str,
    remote: &str,
    accepted: &[AcceptedAdvanceDrops],
    budget: Duration,
) -> Result<bool, AdvanceComputeError> {
    let limit = Instant::now() + budget;
    let deadline = Deadline(
        jj.operation_deadline
            .map_or(limit, |outer| outer.min(limit)),
    );
    deadline.check()?;
    let git_dir =
        super::store_git_dir(store).ok_or_else(|| compute_error("resolve proof store"))?;
    let _lease =
        super::object_lease::ObjectLease::acquire(&git_dir).map_err(|error| match error {
            super::object_lease::LeaseError::Busy => AdvanceComputeError::Busy,
            super::object_lease::LeaseError::Repository(error) => compute_error(error),
        })?;
    futures::executor::block_on(async {
        deadline.check()?;
        let repo = crate::load_repo_at_head(store)
            .await
            .map_err(compute_error)?;
        deadline.check()?;
        let local = indexed_evidence(repo.as_ref(), local)?;
        let tracked = repo.view().get_remote_bookmark(
            RefName::new(branch).to_remote_symbol(jj_lib::ref_name::RemoteName::new("origin")),
        );
        if !tracked.is_tracked()
            || tracked
                .target
                .as_normal()
                .is_none_or(|id| id.hex() != remote)
        {
            return Ok(false);
        }
        let remote = indexed_evidence(repo.as_ref(), remote)?;
        if !normal_target(&repo, branch).is_ok_and(|target| target == local) {
            return Err(AdvanceComputeError::CoordinatesChanged);
        }
        // View visibility is independent of a ref's target: an earlier
        // branch-local operation may have removed an obsolete view head
        // while another bookmark still names its hidden ancestor. Cairn is
        // the sole view owner: hiding origin-only work is only a shortcut
        // over its own rewrite/drop evidence, never authority to abandon
        // arbitrary external commits.
        let origin_only = ResolvedRevsetExpression::commit(remote)
            .ancestors()
            .minus(&ResolvedRevsetExpression::commit(local.clone()).ancestors())
            .intersection(&ResolvedRevsetExpression::visible_heads().ancestors());
        let visible = origin_only.evaluate(repo.as_ref()).map_err(compute_error)?;
        let mut stream = visible.stream();
        let mut missing = HashSet::new();
        while let Some(id) = stream.next().await {
            deadline.check()?;
            missing.insert(id.map_err(compute_error)?);
        }
        for claim in accepted {
            deadline.check()?;
            let head_after = parsed_evidence(repo.as_ref(), &claim.head_after)?;
            // A reclaimed historical head cannot be an ancestor of this
            // indexed T. Ignore the old row; inherited evidence lives on a
            // newer accepted row. Malformed IDs still refuse.
            if !repo.index().has_id(&head_after).map_err(compute_error)? {
                continue;
            }
            if !repo
                .index()
                .is_ancestor(&head_after, &local)
                .map_err(compute_error)?
            {
                continue;
            }
            for dropped in &claim.dropped_commit_ids {
                deadline.check()?;
                if dropped.len() != repo.store().commit_id_length() * 2 {
                    return Err(AdvanceComputeError::UnknownEvidence {
                        commit: dropped.clone(),
                    });
                }
                let id = CommitId::try_from_hex(dropped).ok_or_else(|| {
                    AdvanceComputeError::UnknownEvidence {
                        commit: dropped.clone(),
                    }
                })?;
                missing.remove(&id);
            }
        }
        // The direct flattened metadata is the common path. Ordinary jj
        // amendments may retain only one predecessor, so follow exact
        // predecessor edges too. Missing metadata never becomes evidence.
        let ancestors = ResolvedRevsetExpression::commit(local.clone())
            .ancestors()
            .minus(&ResolvedRevsetExpression::commit(local.clone()))
            .evaluate(repo.as_ref())
            .map_err(compute_error)?;
        let mut stream = ancestors.stream();
        let mut direct_head = Some(repo.store().get_commit(&local).map_err(compute_error)?);
        #[cfg(not(test))]
        let ancestor_limit = 4095;
        #[cfg(test)]
        let ancestor_limit = REMOTE_PROOF_ANCESTOR_LIMIT.with(std::cell::Cell::get);
        let mut visited = HashSet::new();
        let mut edges = 0usize;
        let mut inspected = 0usize;
        while !missing.is_empty() {
            deadline.check()?;
            let commit = if let Some(head) = direct_head.take() {
                head
            } else {
                if inspected >= ancestor_limit {
                    break;
                }
                let Some(id) = stream.next().await else { break };
                inspected += 1;
                repo.store()
                    .get_commit(&id.map_err(compute_error)?)
                    .map_err(compute_error)?
            };
            let mut pending = Vec::new();
            for id in &commit.store_commit().predecessors {
                deadline.check()?;
                missing.remove(id);
                if pending.len() < 16_384 {
                    pending.push((id.clone(), 1usize));
                }
            }
            while let Some((id, depth)) = pending.pop() {
                deadline.check()?;
                missing.remove(&id);
                if missing.is_empty() {
                    break;
                }
                if depth >= 64
                    || visited.len() >= 4096
                    || edges >= 16_384
                    || !visited.insert(id.clone())
                {
                    continue;
                }
                edges += 1;
                let Ok(predecessor) = repo.store().get_commit(&id) else {
                    continue;
                };
                for older in &predecessor.store_commit().predecessors {
                    deadline.check()?;
                    missing.remove(older);
                    if pending.len() < 16_384 {
                        pending.push((older.clone(), depth + 1));
                    }
                }
            }
        }
        deadline.check()?;
        if missing.is_empty() {
            Ok(true)
        } else {
            let mut commits: Vec<_> = missing.into_iter().map(|id| id.hex()).collect();
            commits.sort();
            Err(AdvanceComputeError::UnaccountedRemoteCommits { commits })
        }
    })
}

fn indexed_evidence(
    repo: &dyn jj_lib::repo::Repo,
    hex: &str,
) -> Result<CommitId, AdvanceComputeError> {
    let id = parsed_evidence(repo, hex)?;
    if !repo.index().has_id(&id).map_err(compute_error)? {
        return Err(AdvanceComputeError::UnknownEvidence {
            commit: hex.to_string(),
        });
    }
    Ok(id)
}

fn parsed_evidence(
    repo: &dyn jj_lib::repo::Repo,
    hex: &str,
) -> Result<CommitId, AdvanceComputeError> {
    let unknown = || AdvanceComputeError::UnknownEvidence {
        commit: hex.to_string(),
    };
    if hex.len() != repo.store().commit_id_length() * 2 {
        return Err(unknown());
    }
    CommitId::try_from_hex(hex).ok_or_else(unknown)
}

struct Deadline(Instant);

impl Deadline {
    fn check(&self) -> Result<(), AdvanceComputeError> {
        if Instant::now() >= self.0 {
            Err(AdvanceComputeError::TimedOut)
        } else {
            Ok(())
        }
    }
}

/// The version of the native replay and ancestry-heal algorithm a cost
/// measurement or a timeout describes.
///
/// Bump it whenever a change alters how much work the engine does for the same
/// branch and base: measurements and timeout evidence recorded under one
/// version are never compared with, or allowed to block, another. A rebuild
/// that leaves the algorithm alone keeps the version.
pub const REPLAY_ENGINE_VERSION: u32 = 1;

/// The step a detached computation last entered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputePhase {
    /// Reading the captured branch, its replay boundaries and their mutability.
    Scope,
    /// Rewriting the branch's own commits onto the base.
    Rebase,
    /// Carrying a resolution and passing the declared-merge, parse and
    /// conflict-scaffolding gates.
    Validate,
    /// Resolving the branch's conflict-flagged intermediate commits in place.
    Heal,
    /// Proving which source commits the candidate removed.
    Claims,
    /// Building a merge-mode catch-up commit.
    Merge,
    /// Computation finished.
    Done,
}

impl ComputePhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Scope => "scope",
            Self::Rebase => "rebase",
            Self::Validate => "validation",
            Self::Heal => "ancestry heal",
            Self::Claims => "drop claims",
            Self::Merge => "catch-up merge",
            Self::Done => "done",
        }
    }
}

/// What one detached computation observed about its own cost. Every count is
/// what this computation saw at its captured coordinate; `None` is a step it
/// never reached, never zero.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplayCostObservation {
    /// The step entered last: where a computation that ran out of budget was.
    pub phase: Option<ComputePhase>,
    /// The branch's own commits (head's ancestors not in the base's).
    pub branch_commits: Option<u64>,
    /// Of those, the commits already conflict-flagged in the captured history.
    pub flagged_in_history: Option<u64>,
    /// Commits conflict-flagged after the rebase onto this base, as the heal
    /// found them: the history's own flags plus any this base introduced. A
    /// different base may introduce more or fewer.
    pub flagged_after_rebase: Option<u64>,
    /// Commits whose conflicted paths the heal restored so far.
    pub restored_commits: u64,
    /// Distinct paths the heal restored in previously conflicted commits.
    pub affected_paths: Option<u64>,
    pub rebase_ms: Option<u64>,
    pub heal_ms: Option<u64>,
}

/// A shared sink a computation reports its [`ReplayCostObservation`] into as
/// it goes. The caller keeps a clone and reads it whatever the outcome.
#[derive(Debug, Clone, Default)]
pub struct ReplayCostProbe(Arc<std::sync::Mutex<ReplayCostObservation>>);

impl ReplayCostProbe {
    pub fn observation(&self) -> ReplayCostObservation {
        self.0
            .lock()
            .map(|observed| observed.clone())
            .unwrap_or_default()
    }

    fn record(&self, update: impl FnOnce(&mut ReplayCostObservation)) {
        if let Ok(mut observed) = self.0.lock() {
            update(&mut observed);
        }
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Text already proven by a destination's declared structural merge rules.
/// This is distinct from a human conflict-session resolution carry. The host
/// validates rule authority; VCS validates coordinates, affected paths and trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredMergeEdits {
    pub expected_head: String,
    pub expected_base: String,
    pub edits: Vec<(String, String)>,
}

/// The paths a destination declares regenerable: committed outputs a command
/// derives from sources, whose bytes no line merge can settle. The host reads
/// the declaration at the destination and hands only this predicate down, so
/// the engine decides nothing about commands or configuration.
///
/// A conflict whose EVERY path this covers settles by taking the branch's side
/// of each; one path outside it leaves the whole conflict standing.
#[derive(Clone)]
pub struct RegenerablePaths(Arc<dyn Fn(&str) -> bool + Send + Sync>);

impl RegenerablePaths {
    pub fn new(covers: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(covers))
    }

    pub fn covers(&self, path: &str) -> bool {
        (self.0)(path)
    }
}

impl std::fmt::Debug for RegenerablePaths {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RegenerablePaths(..)")
    }
}

/// One regenerable path a candidate carries with the branch's bytes in place
/// of a conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleRegenerable {
    pub path: String,
    /// The branch's blob that was taken; `None` when the branch deleted the
    /// path. A later head holding a different blob was changed by someone, so
    /// a regeneration computed for this one no longer applies.
    pub blob: Option<String>,
}

/// The trailer a catch-up merge that took regenerable paths carries, so the
/// stale fact is in history as well as on the branch's record.
pub const STALE_REGENERATION_TRAILER: &str = "Stale (regeneration pending):";

/// When `regenerable` covers every conflicted path of `tree`, the tree with
/// each of them set to `ours`' value, and what was taken. `None` when nothing
/// is declared, nothing conflicts, or any conflict falls outside the
/// declaration: that conflict stands exactly as it would have.
async fn take_regenerable_side(
    regenerable: Option<&RegenerablePaths>,
    tree: &jj_lib::merged_tree::MergedTree,
    ours: &Commit,
    deadline: &Deadline,
) -> Result<Option<(jj_lib::merged_tree::MergedTree, Vec<StaleRegenerable>)>, AdvanceComputeError> {
    let Some(regenerable) = regenerable else {
        return Ok(None);
    };
    let mut paths = Vec::new();
    for (path, value) in tree.conflicts() {
        deadline.check()?;
        value.map_err(compute_error)?;
        let path = path.as_internal_file_string().to_string();
        if !regenerable.covers(&path) {
            return Ok(None);
        }
        paths.push(path);
    }
    if paths.is_empty() {
        return Ok(None);
    }
    let mut builder = MergedTreeBuilder::new(tree.clone());
    let mut taken = Vec::with_capacity(paths.len());
    for path in paths {
        deadline.check()?;
        let repo_path = RepoPathBuf::from_internal_string(path.clone()).map_err(compute_error)?;
        let value = ours
            .tree()
            .path_value(&repo_path)
            .await
            .map_err(compute_error)?;
        let blob = match value.as_resolved() {
            Some(Some(TreeValue::File { id, .. })) => Some(id.hex()),
            Some(None) => None,
            Some(Some(_)) => {
                return Err(compute_error(format!(
                    "regenerable path `{path}` is not a file on the branch"
                )))
            }
            None => {
                return Err(compute_error(format!(
                    "regenerable path `{path}` is itself conflicted on the branch"
                )))
            }
        };
        builder.set_or_remove(repo_path, value);
        taken.push(StaleRegenerable { path, blob });
    }
    let tree = builder.write_tree().await.map_err(compute_error)?;
    deadline.check()?;
    if tree.has_conflict() {
        return Err(compute_error(
            "taking the branch's side of every regenerable path left a conflict",
        ));
    }
    Ok(Some((tree, taken)))
}

/// Compute a branch-local replay without publishing any operation or bookmark.
///
/// Run on a blocking worker and await that worker before releasing its lifetime.
/// The absolute deadline is checked between graph, merge and file operations;
/// no timed-out worker retains permission to install. `resolution` is an
/// already-authorized carry, not a request to compute a resolution basis.
pub fn compute_branch_advance(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    budget: Duration,
) -> Result<BranchAdvanceCandidate, AdvanceComputeError> {
    compute_branch_advance_with_declared(jj, snapshot, resolution, None, budget)
}

/// Replay with optional pre-proven declared structural merges. Repeating a
/// refused computation with these edits still publishes no live view; a fresh
/// capture is accepted only when the plan's exact H/B pair still matches.
pub fn compute_branch_advance_with_declared(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    declared: Option<&DeclaredMergeEdits>,
    budget: Duration,
) -> Result<BranchAdvanceCandidate, AdvanceComputeError> {
    compute_with_recovery(jj, snapshot, resolution, declared, budget, false)
}

/// Compute merge-mode catch-up without publishing an operation or bookmark.
/// Existing source commits are never rewritten, replayed, healed or dropped.
/// A divergent candidate has exact parents H then B; installation uses the same
/// snapshot lease, absolute deadline and two-coordinate CAS as replay candidates.
pub fn compute_branch_merge_advance(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    budget: Duration,
) -> Result<BranchAdvanceCandidate, AdvanceComputeError> {
    let message = super::catch_up_merge_message(
        &snapshot.coordinate.base_branch,
        &snapshot.coordinate.base,
        &snapshot.coordinate.branch,
    );
    compute_branch_merge_with_message(jj, snapshot, resolution, &message, budget)
}

pub(super) fn compute_branch_merge_with_message(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    message: &str,
    budget: Duration,
) -> Result<BranchAdvanceCandidate, AdvanceComputeError> {
    let limit = Instant::now() + budget;
    let deadline = Deadline(
        jj.operation_deadline
            .map_or(limit, |outer| outer.min(limit)),
    );
    deadline.check()?;
    let bounded = jj.with_operation_budget(deadline.0.saturating_duration_since(Instant::now()));
    let head = super::packed_refs::with_packed_refs_patience_for(
        &bounded,
        super::PackedRefs::JjStore(&snapshot.store),
        "compute detached catch-up merge",
        || {
            let result = futures::executor::block_on(compute_merge_candidate(
                &snapshot, resolution, message, &deadline,
            ));
            match result {
                Err(AdvanceComputeError::Repository(error))
                    if super::packed_refs_lock_error(&error) =>
                {
                    Err(error)
                }
                other => Ok(other),
            }
        },
    )
    .map_err(compute_error)??;
    let (head, stale_regenerable) = head;
    deadline.check()?;
    snapshot.note(|observed| observed.phase = Some(ComputePhase::Done));
    Ok(BranchAdvanceCandidate {
        snapshot,
        healing: super::AncestryHeal {
            tip: head.id().hex(),
            ..Default::default()
        },
        head,
        dropped_commit_ids: Vec::new(),
        stale_regenerable,
        expires_at: deadline.0,
    })
}

/// The canonical merge kernel of detached preparation. Only fresh objects are
/// written; its transaction is never committed.
async fn compute_merge_candidate(
    snapshot: &BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    message: &str,
    deadline: &Deadline,
) -> Result<(Commit, Vec<StaleRegenerable>), AdvanceComputeError> {
    deadline.check()?;
    snapshot.note(|observed| observed.phase = Some(ComputePhase::Merge));
    let repo = &snapshot.repo;
    let ours = repo
        .store()
        .get_commit(&indexed_evidence(repo.as_ref(), &snapshot.coordinate.head)?)
        .map_err(compute_error)?;
    let base = repo
        .store()
        .get_commit(&indexed_evidence(repo.as_ref(), &snapshot.coordinate.base)?)
        .map_err(compute_error)?;
    if let Some(carry) = resolution {
        if carry.expected_pre_tip != snapshot.coordinate.head {
            return Err(AdvanceComputeError::CoordinatesChanged);
        }
        if carry
            .paths
            .iter()
            .any(|(_, content)| *content != super::CarriedContent::FromTip)
        {
            return Err(compute_error(
                "catch-up refused: merge resolution must take whole paths from the committed tip",
            ));
        }
    }
    if base.has_conflict() {
        return Err(AdvanceComputeError::BaseConflicted {
            base: base.id().hex(),
        });
    }
    deadline.check()?;
    let already_current = repo
        .index()
        .is_ancestor(base.id(), ours.id())
        .map_err(compute_error)?;
    let fast_forward = repo
        .index()
        .is_ancestor(ours.id(), base.id())
        .map_err(compute_error)?;
    deadline.check()?;
    let mut overlapping = Vec::new();
    let mut stale = Vec::new();
    let candidate = if already_current {
        ours.clone()
    } else if fast_forward {
        base.clone()
    } else {
        let bases = repo
            .index()
            .common_ancestors(
                std::slice::from_ref(ours.id()),
                std::slice::from_ref(base.id()),
            )
            .map_err(compute_error)?;
        deadline.check()?;
        if bases.len() != 1 {
            let ids: Vec<_> = bases.iter().map(|id| id.hex()).collect();
            return Err(compute_error(super::ambiguous_merge_base_refusal(
                &snapshot.coordinate.branch,
                &snapshot.coordinate.base,
                &ids,
            )));
        }
        let fork = repo.store().get_commit(&bases[0]).map_err(compute_error)?;
        let own = changed(&fork, &ours, deadline).await?;
        overlapping = changed(&fork, &base, deadline)
            .await?
            .into_iter()
            .map(|(_, path)| path)
            .filter(|path| own.iter().any(|(_, own)| own == path))
            .collect();
        let mut tx = repo.start_transaction();
        let tree = jj_lib::rewrite::merge_commit_trees(tx.repo(), &[ours.clone(), base.clone()])
            .await
            .map_err(compute_error)?;
        deadline.check()?;
        let mut tree = MergedTreeBuilder::new(tree);
        if let Some(carry) = resolution {
            for (path, _) in &carry.paths {
                deadline.check()?;
                let path =
                    RepoPathBuf::from_internal_string(path.clone()).map_err(compute_error)?;
                let value = ours.tree().path_value(&path).await.map_err(compute_error)?;
                if !value.is_resolved() {
                    return Err(compute_error(
                        "catch-up refused: committed resolution path is unresolved",
                    ));
                }
                tree.set_or_remove(path, value);
            }
        }
        let mut tree = tree.write_tree().await.map_err(compute_error)?;
        deadline.check()?;
        let mut message = message.to_string();
        if let Some((taken, paths)) =
            take_regenerable_side(snapshot.regenerable.as_ref(), &tree, &ours, deadline).await?
        {
            message = format!(
                "{}\n\n{STALE_REGENERATION_TRAILER} {}",
                message.trim_end(),
                paths
                    .iter()
                    .map(|stale| stale.path.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            overlapping.retain(|path| !paths.iter().any(|stale| stale.path == *path));
            tree = taken;
            stale = paths;
        }
        let commit = tx
            .repo_mut()
            .new_commit(vec![ours.id().clone(), base.id().clone()], tree)
            .set_description(message)
            .detach()
            .write_hidden()
            .await
            .map_err(compute_error)?;
        deadline.check()?;
        commit
    };
    if candidate.has_conflict() {
        let paths = conflict_paths(&candidate)?;
        return Err(AdvanceComputeError::Conflict(Box::new(
            diagnostic(snapshot, &ours, &base, &candidate, &paths, deadline).await?,
        )));
    }
    let failures =
        candidate_parse_failures(&ours, &base, &candidate, &overlapping, deadline).await?;
    if !failures.is_empty() {
        let paths: Vec<_> = failures
            .iter()
            .map(|failure| failure.path.clone())
            .collect();
        let mut refusal = diagnostic(snapshot, &ours, &base, &candidate, &paths, deadline).await?;
        refusal.condition = super::ConflictCondition::Unparseable;
        refusal.validation = Some(super::ValidationRefusal {
            commit: candidate.id().hex(),
            failures,
        });
        return Err(AdvanceComputeError::Conflict(Box::new(refusal)));
    }
    let roots = candidate.tree().trees().await.map_err(compute_error)?;
    deadline.check()?;
    let root = roots
        .as_resolved()
        .ok_or_else(|| compute_error("merge candidate has unresolved trees"))?;
    let mut names = Vec::new();
    for entry in root.entries_non_recursive() {
        deadline.check()?;
        names.push(entry.name().as_internal_str());
    }
    let entries =
        cairn_common::conflict_scaffolding::conflict_scaffolding_in_root_listing(&names.join("\n"));
    if !entries.is_empty() {
        let mut refusal =
            diagnostic(snapshot, &ours, &base, &candidate, &overlapping, deadline).await?;
        refusal.condition = super::ConflictCondition::ContentConflict;
        refusal.scaffolding = Some(super::ScaffoldingRefusal {
            commit: candidate.id().hex(),
            entries,
        });
        return Err(AdvanceComputeError::Conflict(Box::new(refusal)));
    }
    deadline.check()?;
    Ok((candidate, stale))
}

const SQUASH_RECEIPT_LIMIT: usize = 4096;

/// Construct a fresh squash object without rewriting any existing commit or view.
/// A real merge would make every exact source-range commit an ancestor; these
/// predecessor receipts preserve that history under the squash's fresh change ID.
/// Ownership remains enforced by replay's pinning policy where receipts are used.
pub(super) fn compute_branch_squash(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    message: &str,
    budget: Duration,
) -> Result<(BranchAdvanceCandidate, usize), AdvanceComputeError> {
    let limit = Instant::now() + budget;
    let deadline = Deadline(
        jj.operation_deadline
            .map_or(limit, |outer| outer.min(limit)),
    );
    deadline.check()?;
    let bounded = jj.with_operation_budget(deadline.0.saturating_duration_since(Instant::now()));
    let (head, collapsed) = super::packed_refs::with_packed_refs_patience_for(
        &bounded,
        super::PackedRefs::JjStore(&snapshot.store),
        "construct branch-only squash",
        || {
            let result = futures::executor::block_on(async {
                deadline.check()?;
                let source = snapshot.repo.store()
                    .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head)
                        .ok_or_else(|| compute_error("invalid squash source"))?)
                    .map_err(compute_error)?;
                let base = snapshot.repo.store()
                    .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.base)
                        .ok_or_else(|| compute_error("invalid squash base"))?)
                    .map_err(compute_error)?;
                if !snapshot.repo.index().is_ancestor(base.id(), source.id()).map_err(compute_error)? {
                    return Err(compute_error(format!(
                        "flatten guard: branch `{}` does not descend from dest `{}`; refusing to re-parent a stale tree",
                        snapshot.coordinate.branch, snapshot.coordinate.base,
                    )));
                }
                if base.has_conflict() || source.has_conflict() {
                    return Err(compute_error("squash guard: source and base tips must be conflict-free"));
                }
                let roots = source.tree().trees().await.map_err(compute_error)?;
                deadline.check()?;
                let root = roots.as_resolved()
                    .ok_or_else(|| compute_error("squash guard: source tree is unresolved"))?;
                let mut listing = String::new();
                for entry in root.entries_non_recursive() {
                    deadline.check()?;
                    listing.push_str(entry.name().as_internal_str());
                    listing.push('\n');
                }
                if !cairn_common::conflict_scaffolding::conflict_scaffolding_in_root_listing(&listing).is_empty() {
                    return Err(compute_error("squash guard: source tree contains conflict scaffolding"));
                }
                let commits = branch_commits(snapshot.repo.as_ref(), source.id(), base.id(), &deadline, Some(SQUASH_RECEIPT_LIMIT)).await?;
                let collapsed = commits.iter().filter(|commit| commit.has_conflict()).count();
                let mut predecessors = Vec::new();
                let mut seen = HashSet::new();
                for commit in commits.iter().rev() {
                    deadline.check()?;
                    if seen.insert(commit.id().clone()) {
                        predecessors.push(commit.id().clone());
                    }
                }
                for commit in commits.iter().rev() {
                    for predecessor in &commit.store_commit().predecessors {
                        deadline.check()?;
                        if predecessor.hex().len() != snapshot.repo.store().commit_id_length() * 2 {
                            return Err(AdvanceComputeError::UnknownEvidence { commit: predecessor.hex() });
                        }
                        if snapshot.repo.index().has_id(predecessor).map_err(compute_error)?
                            && snapshot.repo.index().is_ancestor(predecessor, base.id()).map_err(compute_error)?
                        {
                            continue;
                        }
                        if seen.insert(predecessor.clone()) {
                            predecessors.push(predecessor.clone());
                        }
                        if predecessors.len() > SQUASH_RECEIPT_LIMIT {
                            return Err(AdvanceComputeError::EvidenceLimit { limit: SQUASH_RECEIPT_LIMIT });
                        }
                    }
                }
                if predecessors.len() > SQUASH_RECEIPT_LIMIT {
                    return Err(AdvanceComputeError::EvidenceLimit { limit: SQUASH_RECEIPT_LIMIT });
                }
                let mut tx = snapshot.repo.start_transaction();
                let head = tx.repo_mut()
                    .new_commit(vec![base.id().clone()], source.tree())
                    .set_predecessors(predecessors)
                    .set_description(message)
                    .detach().write_hidden().await.map_err(compute_error)?;
                // With the same exact parent, tree identity proves the complete
                // pre/post footprint, including every deletion, is identical.
                if head.tree_ids() != source.tree_ids() || head.parent_ids() != [base.id().clone()] {
                    return Err(compute_error("flatten footprint guard: squash tree or exact parent changed"));
                }
                deadline.check()?;
                Ok((head, collapsed))
            });
            match result {
                Err(AdvanceComputeError::Repository(error)) if super::packed_refs_lock_error(&error) => Err(error),
                other => Ok(other),
            }
        },
    ).map_err(compute_error)??;
    deadline.check()?;
    Ok((
        BranchAdvanceCandidate {
            snapshot,
            healing: super::AncestryHeal {
                tip: head.id().hex(),
                ..Default::default()
            },
            head,
            dropped_commit_ids: Vec::new(),
            stale_regenerable: Vec::new(),
            expires_at: deadline.0,
        },
        collapsed,
    ))
}

pub(super) fn compute_legacy_replay(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    budget: Duration,
) -> Result<BranchAdvanceCandidate, AdvanceComputeError> {
    compute_with_recovery(jj, snapshot, resolution, None, budget, true)
}

fn compute_with_recovery(
    jj: &super::JjEnv,
    snapshot: BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    declared: Option<&DeclaredMergeEdits>,
    budget: Duration,
    legacy: bool,
) -> Result<BranchAdvanceCandidate, AdvanceComputeError> {
    if declared.is_some_and(|plan| {
        plan.expected_head != snapshot.coordinate.head
            || plan.expected_base != snapshot.coordinate.base
    }) {
        return Err(AdvanceComputeError::CoordinatesChanged);
    }
    let limit = Instant::now() + budget;
    let deadline = Deadline(
        jj.operation_deadline
            .map_or(limit, |outer| outer.min(limit)),
    );
    // Legacy callers without an operation budget retain the canonical
    // maintenance-owned stale-lock proof. Detached callers always have an
    // absolute bound and never enter that unbounded external proof.
    let patience = if legacy {
        jj.clone()
    } else {
        jj.with_operation_budget(deadline.0.saturating_duration_since(Instant::now()))
    };
    let (head, healing, dropped_commit_ids, stale_regenerable) =
        super::packed_refs::with_packed_refs_patience_for(
            &patience,
            super::PackedRefs::JjStore(&snapshot.store),
            "compute detached replay",
            || {
                let result = futures::executor::block_on(compute_candidate(
                    jj, &snapshot, resolution, declared, &deadline,
                ));
                match result {
                    Err(AdvanceComputeError::Repository(error))
                        if super::packed_refs_lock_error(&error) =>
                    {
                        Err(error)
                    }
                    other => Ok(other),
                }
            },
        )
        .map_err(compute_error)??;
    deadline.check()?;
    Ok(BranchAdvanceCandidate {
        snapshot,
        head,
        healing,
        dropped_commit_ids,
        stale_regenerable,
        expires_at: deadline.0,
    })
}

async fn branch_commits(
    repo: &dyn jj_lib::repo::Repo,
    head: &CommitId,
    base: &CommitId,
    deadline: &Deadline,
    limit: Option<usize>,
) -> Result<Vec<Commit>, AdvanceComputeError> {
    let expression = ResolvedRevsetExpression::commit(head.clone())
        .ancestors()
        .minus(&ResolvedRevsetExpression::commit(base.clone()).ancestors());
    let revset = expression.evaluate(repo).map_err(compute_error)?;
    let mut stream = revset.stream();
    let mut commits = Vec::new();
    while let Some(id) = stream.next().await {
        deadline.check()?;
        if let Some(limit) = limit {
            if commits.len() >= limit {
                return Err(AdvanceComputeError::EvidenceLimit { limit });
            }
        }
        commits.push(
            repo.store()
                .get_commit(&id.map_err(compute_error)?)
                .map_err(compute_error)?,
        );
    }
    // Revsets stream reverse topologically. Every parent is processed before
    // its child, including both arms of branch-owned merges.
    commits.reverse();
    Ok(commits)
}

// Structural squash receipts retain all exact source IDs; ordinary temporal
// versions remain capped. Missing objects consume temporal room, never become
// permanent structural receipts. Proof consumers still compare IDs directly.
fn rewrite_predecessors(
    old: &Commit,
    deadline: &Deadline,
) -> Result<Vec<CommitId>, AdvanceComputeError> {
    let mut seen = HashSet::new();
    let mut structural = Vec::new();
    let mut temporal = vec![old.id().clone()];
    seen.insert(old.id().clone());
    for id in &old.store_commit().predecessors {
        deadline.check()?;
        if !seen.insert(id.clone()) {
            continue;
        }
        if id.hex().len() != old.store().commit_id_length() * 2 {
            return Err(AdvanceComputeError::UnknownEvidence { commit: id.hex() });
        }
        let loaded = old.store().get_commit(id);
        deadline.check()?;
        let differs = match loaded {
            Ok(commit) => commit.change_id() != old.change_id(),
            Err(jj_lib::backend::BackendError::ObjectNotFound { .. }) => false,
            Err(error) => return Err(compute_error(error)),
        };
        if differs {
            structural.push(id.clone());
            if structural.len() > SQUASH_RECEIPT_LIMIT {
                return Err(AdvanceComputeError::EvidenceLimit {
                    limit: SQUASH_RECEIPT_LIMIT,
                });
            }
        } else if temporal.len() < 64 {
            temporal.push(id.clone());
        }
    }
    structural.extend(temporal);
    Ok(structural)
}

/// Source-operation ownership is deliberate: a later foreign bookmark move
/// cannot invalidate candidate content and is not an extra installation CAS.
fn foreign_heads(
    repo: &ReadonlyRepo,
    branch: &str,
    dependents: Option<&HashSet<String>>,
) -> Vec<CommitId> {
    repo.view()
        .local_bookmarks()
        .filter(|(name, _)| {
            *name != RefName::new(branch)
                && !dependents.is_some_and(|names| names.contains(name.as_str()))
        })
        .flat_map(|(_, target)| target.added_ids().cloned())
        .chain(
            repo.view()
                .local_tags()
                .flat_map(|(_, target)| target.added_ids().cloned()),
        )
        // A working-copy commit has no branch-ownership identity in jj's view.
        // It can be this branch's own or stale checkout, not a foreign owner.
        // Installation still preserves workspace heads and their ancestry.
        .collect()
}

/// A literal branch target remains authoritative when another branch retains
/// an older version of the same change. Only unowned visible residue is repair.
pub(crate) fn branch_tip_divergent(
    repo: &ReadonlyRepo,
    branch: &str,
    tip: &CommitId,
) -> Result<bool, String> {
    let limit = Instant::now() + Duration::from_secs(10);
    let commit = repo
        .store()
        .get_commit(tip)
        .map_err(|error| error.to_string())?;
    let targets = repo
        .resolve_change_id(commit.change_id())
        .map_err(|error| error.to_string())?
        .ok_or("change ID has no visible targets")?;
    let foreign = foreign_heads(repo, branch, None);
    for (_, other) in targets.visible_with_offsets() {
        if other == tip {
            continue;
        }
        let mut owned = false;
        for anchor in &foreign {
            if Instant::now() >= limit {
                return Err("branch ownership inspection timed out".into());
            }
            if repo
                .index()
                .is_ancestor(other, anchor)
                .map_err(|error| error.to_string())?
            {
                owned = true;
                break;
            }
        }
        if !owned {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn replay_boundaries(
    snapshot: &BranchAdvanceSnapshot,
    commits: &[Commit],
    head: &Commit,
    base: &Commit,
    deadline: &Deadline,
) -> Result<(HashSet<CommitId>, HashMap<CommitId, CommitId>), AdvanceComputeError> {
    let repo = snapshot.repo.as_ref();
    let foreign = foreign_heads(
        repo,
        &snapshot.coordinate.branch,
        snapshot.dependent_bookmarks.as_ref(),
    );
    // Filter descendants of the demanded head once. They retain this branch's
    // old graph; they are not independent owners of its merge arms.
    let mut independent = Vec::new();
    for anchor in foreign {
        deadline.check()?;
        if !repo
            .index()
            .is_ancestor(head.id(), &anchor)
            .map_err(compute_error)?
        {
            independent.push(anchor);
        }
    }
    // One indexed reachability union supplies the SAME ownership predicate to
    // primary-line selection and side pinning. No per-commit × anchor walks.
    let considered: HashSet<_> = commits
        .iter()
        .flat_map(|commit| {
            std::iter::once(commit.id().clone()).chain(commit.parent_ids().iter().cloned())
        })
        .collect();
    let expression = ResolvedRevsetExpression::commits(independent)
        .ancestors()
        .intersection(&ResolvedRevsetExpression::commits(
            considered.into_iter().collect(),
        ));
    let owners = expression.evaluate(repo).map_err(compute_error)?;
    let mut stream = owners.stream();
    let mut foreign_owned = HashSet::new();
    while let Some(id) = stream.next().await {
        deadline.check()?;
        foreign_owned.insert(id.map_err(compute_error)?);
    }
    let range: HashSet<_> = commits.iter().map(|commit| commit.id().clone()).collect();
    let mut primary = HashSet::new();
    let mut next = head.clone();
    while range.contains(next.id()) {
        deadline.check()?;
        primary.insert(next.id().clone());
        let Some(first) = next.parent_ids().first() else {
            break;
        };
        let owned = &foreign_owned;
        let unowned: Vec<_> = next
            .parent_ids()
            .iter()
            .filter(|id| !owned.contains(id))
            .collect();
        let base_arm = next
            .parent_ids()
            .iter()
            .skip(1)
            .map(|id| {
                repo.index()
                    .is_ancestor(id, base.id())
                    .map_err(compute_error)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .any(|is_base| is_base);
        let parent = if next.parent_ids().len() > 1 && owned.contains(first) && !base_arm {
            if snapshot.dependent_bookmarks.is_none() {
                return Err(AdvanceComputeError::AmbiguousOwnership {
                    commit: next.id().hex(),
                });
            }
            if unowned.len() != 1 {
                return Err(AdvanceComputeError::AmbiguousOwnership {
                    commit: next.id().hex(),
                });
            }
            // Parent order does not transfer ownership. In a reversed merge
            // (foreign side, own line), follow the uniquely unanchored own line.
            unowned[0]
        } else {
            first
        };
        next = repo.store().get_commit(parent).map_err(compute_error)?;
    }
    let mut pinned = HashSet::new();
    for commit in commits {
        deadline.check()?;
        if primary.contains(commit.id()) {
            continue;
        }
        if foreign_owned.contains(commit.id()) {
            pinned.insert(commit.id().clone());
        }
    }
    // Change-ID indexing is cheap, but only exact directional predecessors prove
    // incorporation. Fresh-identity squashes need the same proof via a receipt
    // index; build that only for identities not already proven by this pass.
    let eligible: Vec<_> = commits.iter().collect();
    let mut mapping = HashMap::new();
    let mut by_change: HashMap<CommitId, HashSet<CommitId>> = HashMap::new();
    for old in &eligible {
        deadline.check()?;
        let targets: HashSet<_> = repo
            .resolve_change_id(old.change_id())
            .map_err(compute_error)?
            .map(|changes| changes.targets.into_iter().map(|(id, _)| id).collect())
            .unwrap_or_default();
        if let Some(id) = exact_base_successor(
            repo,
            old,
            base,
            &targets,
            !pinned.contains(old.id()),
            deadline,
        )? {
            mapping.insert(old.id().clone(), id);
        } else {
            by_change.insert(old.id().clone(), targets);
        }
    }
    if by_change.is_empty() {
        return retain_replay_boundaries(repo, commits, pinned, mapping, deadline).await;
    }
    // Unlanded own commits also need this walk: absent a reverse predecessor
    // index, they cannot be distinguished from identities collapsed by a squash.
    // Plain history growth consumes the deadline, not the evidence quota.
    super::reached_step("advance: scan squash receipts");
    let expression = ResolvedRevsetExpression::commit(base.id().clone())
        .ancestors()
        .minus(&ResolvedRevsetExpression::commit(head.id().clone()).ancestors());
    let indexed = expression.evaluate(repo).map_err(compute_error)?;
    let mut stream = indexed.stream();
    let mut receipt_candidates: HashMap<CommitId, HashSet<CommitId>> = HashMap::new();
    let mut receipt_count = 0usize;
    let mut edge_count = 0usize;
    while let Some(id) = stream.next().await {
        deadline.check()?;
        let id = id.map_err(compute_error)?;
        let newer = match repo.store().get_commit(&id) {
            Ok(commit) => commit,
            Err(jj_lib::backend::BackendError::ObjectNotFound { .. }) => continue,
            Err(error) => return Err(compute_error(error)),
        };
        if !newer.store_commit().predecessors.is_empty() {
            receipt_count += 1;
            if receipt_count > 65_536 {
                return Err(AdvanceComputeError::EvidenceLimit { limit: 65_536 });
            }
        }
        for predecessor in &newer.store_commit().predecessors {
            deadline.check()?;
            edge_count += 1;
            if edge_count > 1_048_576 {
                return Err(AdvanceComputeError::EvidenceLimit { limit: 1_048_576 });
            }
            if predecessor.hex().len() != repo.store().commit_id_length() * 2 {
                return Err(AdvanceComputeError::UnknownEvidence {
                    commit: predecessor.hex(),
                });
            }
            if by_change.contains_key(predecessor) {
                receipt_candidates
                    .entry(predecessor.clone())
                    .or_default()
                    .insert(id.clone());
            }
        }
    }
    for old in eligible {
        let Some(mut targets) = by_change.remove(old.id()) else {
            continue;
        };
        targets.extend(receipt_candidates.remove(old.id()).unwrap_or_default());
        if let Some(id) = exact_base_successor(
            repo,
            old,
            base,
            &targets,
            !pinned.contains(old.id()),
            deadline,
        )? {
            mapping.insert(old.id().clone(), id);
        }
    }
    retain_replay_boundaries(repo, commits, pinned, mapping, deadline).await
}

async fn retain_replay_boundaries(
    repo: &dyn jj_lib::repo::Repo,
    commits: &[Commit],
    potential_pins: HashSet<CommitId>,
    mut mapping: HashMap<CommitId, CommitId>,
    deadline: &Deadline,
) -> Result<(HashSet<CommitId>, HashMap<CommitId, CommitId>), AdvanceComputeError> {
    let range: HashSet<_> = commits.iter().map(|commit| commit.id().clone()).collect();
    let mut retained = HashSet::new();
    // Mapping only shrinks. Newly replayed own merges can require additional
    // literal foreign arms, whose entire in-range ancestry must remain intact.
    // Retained arms satisfy prefix closure; an unproven own ancestor does not.
    loop {
        deadline.check()?;
        let previous = mapping.len();
        let mut roots = HashSet::new();
        for commit in commits {
            deadline.check()?;
            if potential_pins.contains(commit.id()) || mapping.contains_key(commit.id()) {
                continue;
            }
            for parent in commit.parent_ids() {
                deadline.check()?;
                if potential_pins.contains(parent) && !mapping.contains_key(parent) {
                    roots.insert(parent.clone());
                }
            }
        }
        if !roots.is_empty() {
            let expression = ResolvedRevsetExpression::commits(roots.into_iter().collect())
                .ancestors()
                .intersection(&ResolvedRevsetExpression::commits(
                    range.iter().cloned().collect(),
                ));
            let ancestors = expression.evaluate(repo).map_err(compute_error)?;
            let mut stream = ancestors.stream();
            while let Some(id) = stream.next().await {
                deadline.check()?;
                retained.insert(id.map_err(compute_error)?);
            }
        }
        mapping.retain(|id, _| !retained.contains(id));
        // branch_commits is oldest-first: removing one unproven prefix member
        // invalidates all dependent mappings in this pass, without guessing that
        // a descendant receipt also proves its missing ancestors.
        for commit in commits {
            deadline.check()?;
            if !mapping.contains_key(commit.id()) {
                continue;
            }
            let mut complete = true;
            for parent in commit.parent_ids() {
                deadline.check()?;
                if range.contains(parent)
                    && !mapping.contains_key(parent)
                    && !retained.contains(parent)
                {
                    complete = false;
                    break;
                }
            }
            if !complete {
                mapping.remove(commit.id());
            }
        }
        if mapping.len() == previous {
            break;
        }
    }
    // A foreign object keeps its literal ancestors reachable. An own-line
    // ancestor may still replay onto the new base; treating that own version as
    // a replay pin could prevent the candidate from including the new base.
    let pinned = potential_pins
        .into_iter()
        .filter(|id| !mapping.contains_key(id))
        .collect();
    Ok((pinned, mapping))
}

fn exact_base_successor(
    repo: &dyn jj_lib::repo::Repo,
    old: &Commit,
    base: &Commit,
    targets: &HashSet<CommitId>,
    allow_temporal: bool,
    deadline: &Deadline,
) -> Result<Option<CommitId>, AdvanceComputeError> {
    let mut successor: Option<CommitId> = None;
    let mut ambiguous = false;
    for id in targets {
        deadline.check()?;
        if !repo
            .index()
            .is_ancestor(id, base.id())
            .map_err(compute_error)?
        {
            continue;
        }
        let newer = match repo.store().get_commit(id) {
            Ok(commit) => commit,
            Err(jj_lib::backend::BackendError::ObjectNotFound { .. }) => continue,
            Err(error) => return Err(compute_error(error)),
        };
        if !newer.store_commit().predecessors.contains(old.id()) {
            continue;
        }
        if let Some(chosen) = &successor {
            if repo
                .index()
                .is_ancestor(id, chosen)
                .map_err(compute_error)?
            {
                successor = Some(id.clone());
            } else if !repo
                .index()
                .is_ancestor(chosen, id)
                .map_err(compute_error)?
            {
                ambiguous = true;
            }
        } else {
            successor = Some(id.clone());
        }
    }
    let successor = if ambiguous { None } else { successor };
    // A pin only withholds the already-judged answer. Filtering temporal facts
    // before ambiguity would let the pin turn an ambiguous proof into authority.
    if !allow_temporal {
        if let Some(id) = &successor {
            let newer = repo.store().get_commit(id).map_err(compute_error)?;
            if newer.change_id() == old.change_id() {
                return Ok(None);
            }
        }
    }
    Ok(successor)
}

async fn rewrite_hidden(
    repo: &mut MutableRepo,
    old: &Commit,
    parents: Vec<CommitId>,
    deadline: &Deadline,
) -> Result<Commit, AdvanceComputeError> {
    if parents == old.parent_ids() {
        return Ok(old.clone());
    }
    let commit = CommitRewriter::new(repo, old.clone(), parents)
        .rebase()
        .await
        .map_err(compute_error)?
        .set_predecessors(rewrite_predecessors(old, deadline)?)
        .detach()
        .write_hidden()
        .await
        .map_err(compute_error)?;
    repo.add_head(&commit).await.map_err(compute_error)?;
    Ok(commit)
}

fn conflict_paths(commit: &Commit) -> Result<Vec<String>, AdvanceComputeError> {
    commit
        .tree()
        .conflicts()
        .map(|(path, value)| {
            value.map_err(compute_error)?;
            Ok(path.as_internal_file_string().to_string())
        })
        .collect()
}

async fn restore_paths(
    repo: &mut MutableRepo,
    commit: &Commit,
    source: &Commit,
    paths: &[String],
    deadline: &Deadline,
) -> Result<Commit, AdvanceComputeError> {
    let mut tree = MergedTreeBuilder::new(commit.tree());
    for path in paths {
        deadline.check()?;
        let path = RepoPathBuf::from_internal_string(path.clone()).map_err(compute_error)?;
        let value = source
            .tree()
            .path_value(&path)
            .await
            .map_err(compute_error)?;
        if !value.is_resolved() {
            return Err(compute_error("ancestry heal source has unresolved paths"));
        }
        tree.set_or_remove(path, value);
    }
    let tree = tree.write_tree().await.map_err(compute_error)?;
    let restored = repo
        .rewrite_commit(commit)
        .set_predecessors(rewrite_predecessors(commit, deadline)?)
        .set_tree(tree)
        .detach()
        .write_hidden()
        .await
        .map_err(compute_error)?;
    repo.add_head(&restored).await.map_err(compute_error)?;
    Ok(restored)
}

struct ProvenMergeCollapse {
    source: CommitId,
    parent: CommitId,
}

fn prove_merge_collapse(
    repo: &dyn jj_lib::repo::Repo,
    old: &Commit,
    parents: &[CommitId],
    base: &Commit,
    proven: &HashSet<CommitId>,
    dropped: &HashSet<CommitId>,
    deadline: &Deadline,
) -> Result<ProvenMergeCollapse, AdvanceComputeError> {
    let refusal = || {
        compute_error(
            "replay would collapse distinct merge parents without individual integration proof",
        )
    };
    if old.parent_ids().len() <= 1 || parents != [base.id().clone()] {
        return Err(refusal());
    }
    for parent in old.parent_ids() {
        deadline.check()?;
        if !proven.contains(parent)
            && !dropped.contains(parent)
            && !repo
                .index()
                .is_ancestor(parent, base.id())
                .map_err(compute_error)?
        {
            return Err(refusal());
        }
    }
    Ok(ProvenMergeCollapse {
        source: old.id().clone(),
        parent: base.id().clone(),
    })
}

async fn newly_empty(
    repo: &MutableRepo,
    before: &Commit,
    after: &Commit,
    collapse: Option<&ProvenMergeCollapse>,
) -> Result<Option<CommitId>, AdvanceComputeError> {
    if after.parent_ids().len() != 1 {
        return Ok(None);
    }
    let collapsed_merge = before.parent_ids().len() > 1;
    if collapsed_merge {
        if !collapse.is_some_and(|proof| {
            proof.source == *before.id() && after.parent_ids() == [proof.parent.clone()]
        }) {
            return Err(compute_error(
                "[ADVANCE-MERGE-INVARIANT] unproven merge collapse cannot be emptied",
            ));
        }
    } else if before.parent_ids().len() != 1 {
        return Ok(None);
    }
    let parent = repo
        .store()
        .get_commit(&after.parent_ids()[0])
        .map_err(compute_error)?;
    if after.tree_ids() != parent.tree_ids() {
        return Ok(None);
    }
    // The guarded rebase has already integrated every old parent. A collapsed
    // merge with no remaining own change is now a newly empty linear commit.
    if collapsed_merge {
        return Ok(Some(parent.id().clone()));
    }
    let old_parent = repo
        .store()
        .get_commit(&before.parent_ids()[0])
        .map_err(compute_error)?;
    Ok((before.tree_ids() != old_parent.tree_ids()).then(|| parent.id().clone()))
}

async fn heal_branch(
    repo: &mut MutableRepo,
    head: &Commit,
    base: &Commit,
    pinned: &HashSet<CommitId>,
    dropped_ids: &mut HashSet<CommitId>,
    deadline: &Deadline,
    cost: Option<&ReplayCostProbe>,
) -> Result<(Commit, super::AncestryHeal), AdvanceComputeError> {
    let started = Instant::now();
    let note = |update: &dyn Fn(&mut ReplayCostObservation)| {
        if let Some(cost) = cost {
            cost.record(|observed| {
                update(observed);
                observed.heal_ms = Some(elapsed_ms(started));
            });
        }
    };
    note(&|observed| observed.phase = Some(ComputePhase::Heal));
    let before = branch_commits(repo, head.id(), base.id(), deadline, None).await?;
    let flagged = before.iter().filter(|commit| commit.has_conflict()).count() as u64;
    note(&|observed| {
        observed.flagged_after_rebase = Some(flagged);
        observed.affected_paths = Some(0);
    });
    if flagged == 0 {
        return Ok((
            head.clone(),
            super::AncestryHeal {
                tip: head.id().hex(),
                ..Default::default()
            },
        ));
    }
    let mut mapping: HashMap<CommitId, CommitId> = HashMap::new();
    let mut healed = Vec::new();
    let mut dropped = Vec::new();
    let mut affected: HashSet<String> = HashSet::new();
    for old in &before {
        let affected_now = affected.len() as u64;
        note(&|observed| observed.affected_paths = Some(affected_now));
        deadline.check()?;
        if pinned.contains(old.id()) {
            if old.has_conflict() {
                return Err(compute_error(
                    "cannot heal conflicted history owned by a foreign branch",
                ));
            }
            mapping.insert(old.id().clone(), old.id().clone());
            continue;
        }
        let parents = old
            .parent_ids()
            .iter()
            .map(|id| mapping.get(id).unwrap_or(id).clone())
            .collect();
        let mut current = rewrite_hidden(repo, old, parents, deadline).await?;
        let mut resolved_paths = None;
        if current.has_conflict() {
            let paths = conflict_paths(&current)?;
            if paths.is_empty() {
                return Err(compute_error(format!(
                    "{} no conflicted paths named",
                    super::ANCESTRY_HEAL_GUARD
                )));
            }
            // An existing conflict takes the committed clean-tip resolution.
            // A newly conflicted descendant takes its own pre-heal tree.
            let source = if old.has_conflict() { head } else { old };
            current = restore_paths(repo, &current, source, &paths, deadline).await?;
            super::reached_step("ancestry heal: commit resolved");
            note(&|observed| observed.restored_commits += 1);
            if old.has_conflict() {
                affected.extend(paths.iter().cloned());
                resolved_paths = Some(paths);
            }
        }
        if current.has_conflict() {
            return Err(compute_error(format!(
                "{} restored commit remains conflicted",
                super::ANCESTRY_HEAL_GUARD
            )));
        }
        if let Some(parent) = newly_empty(repo, old, &current, None).await? {
            dropped_ids.insert(old.id().clone());
            dropped_ids.extend(old.store_commit().predecessors.iter().cloned());
            dropped.push(super::emptied::dropped_label(
                &old.id().hex(),
                old.description().lines().next().unwrap_or(""),
            ));
            mapping.insert(old.id().clone(), parent);
        } else {
            if let Some(paths) = resolved_paths {
                healed.push((old.id().clone(), paths));
            }
            mapping.insert(old.id().clone(), current.id().clone());
        }
    }
    let affected_now = affected.len() as u64;
    note(&|observed| observed.affected_paths = Some(affected_now));
    let final_id = mapping.get(head.id()).unwrap_or(head.id()).clone();
    let final_head = repo.store().get_commit(&final_id).map_err(compute_error)?;
    if final_head.tree_ids() != head.tree_ids() {
        return Err(compute_error(format!(
            "{} healed tip tree changed",
            super::ANCESTRY_HEAL_GUARD
        )));
    }
    // Add provenance only to surviving healed commits, then rebase their
    // branch-owned descendants. Metadata-only rewrites cannot alter the tree.
    let final_commits = branch_commits(repo, final_head.id(), base.id(), deadline, None).await?;
    let healed_report = healed
        .iter()
        .map(|(id, paths)| {
            let commit = repo.store().get_commit(id).map_err(compute_error)?;
            Ok(super::HealedCommit {
                change_id: commit.change_id().reverse_hex(),
                paths: paths.clone(),
            })
        })
        .collect::<Result<Vec<_>, AdvanceComputeError>>()?;
    let notes: HashMap<CommitId, Vec<String>> = healed
        .into_iter()
        .filter_map(|(old, paths)| mapping.get(&old).cloned().map(|id| (id, paths)))
        .filter(|(id, _)| final_commits.iter().any(|commit| commit.id() == id))
        .collect();
    let mut annotated = HashMap::new();
    let mut first = true;
    for old in final_commits {
        note(&|_| {});
        deadline.check()?;
        if pinned.contains(old.id()) {
            annotated.insert(old.id().clone(), old.id().clone());
            continue;
        }
        let parents = old
            .parent_ids()
            .iter()
            .map(|id| annotated.get(id).unwrap_or(id).clone())
            .collect();
        let mut current = rewrite_hidden(repo, &old, parents, deadline).await?;
        if let Some(paths) = notes.get(old.id()) {
            let note = super::heal::heal_note(
                &base.id().hex(),
                &head.id().hex(),
                head.description().lines().next().unwrap_or(""),
                paths,
                if first { &dropped } else { &[] },
            );
            first = false;
            current = repo
                .rewrite_commit(&current)
                .set_predecessors(rewrite_predecessors(&current, deadline)?)
                .set_description(format!("{}\n\n{note}", old.description().trim_end()))
                .detach()
                .write_hidden()
                .await
                .map_err(compute_error)?;
            repo.add_head(&current).await.map_err(compute_error)?;
        }
        annotated.insert(old.id().clone(), current.id().clone());
    }
    let id = annotated.get(final_head.id()).unwrap_or(final_head.id());
    let result = repo.store().get_commit(id).map_err(compute_error)?;
    if result.tree_ids() != head.tree_ids() {
        return Err(compute_error(format!(
            "{} annotated tip tree changed",
            super::ANCESTRY_HEAL_GUARD
        )));
    }
    let report = super::AncestryHeal {
        tip: result.id().hex(),
        healed: healed_report,
        dropped,
    };
    note(&|_| {});
    Ok((result, report))
}

// Drop events during healing name hidden intermediate commits. Only original
// source commits absent from both ancestry and exact predecessor evidence become
// accepted-drop evidence; labels and tree equality are never publication proof.
async fn exact_source_drops(
    repo: &MutableRepo,
    source: &[Commit],
    head: &Commit,
    base: &Commit,
    proven_base_versions: &HashSet<CommitId>,
    dropped_ids: &HashSet<CommitId>,
    deadline: &Deadline,
) -> Result<Vec<String>, AdvanceComputeError> {
    let mut predecessors = HashSet::new();
    for commit in branch_commits(repo, head.id(), base.id(), deadline, None).await? {
        deadline.check()?;
        predecessors.extend(commit.store_commit().predecessors.iter().cloned());
    }
    let mut dropped = Vec::new();
    for old in source {
        deadline.check()?;
        if proven_base_versions.contains(old.id())
            || predecessors.contains(old.id())
            || repo
                .index()
                .is_ancestor(old.id(), head.id())
                .map_err(compute_error)?
        {
            continue;
        }
        if !dropped_ids.contains(old.id()) {
            return Err(compute_error(format!(
                "source commit {} has neither a successor nor an actual drop",
                old.id().hex()
            )));
        }
        dropped.push(old.id().hex());
    }
    Ok(dropped)
}

const REMOVED_CLAIM_INVARIANT: &str = "[ADVANCE-CLAIM-INVARIANT]";

fn validate_removed_claims(
    repo: &dyn jj_lib::repo::Repo,
    head: &Commit,
    claims: &[String],
    deadline: &Deadline,
) -> Result<(), AdvanceComputeError> {
    for claim in claims {
        deadline.check()?;
        let id = indexed_evidence(repo, claim)?;
        if repo
            .index()
            .is_ancestor(&id, head.id())
            .map_err(compute_error)?
        {
            return Err(AdvanceComputeError::Repository(format!(
                "{REMOVED_CLAIM_INVARIANT} retained source {claim} cannot be claimed as removed"
            )));
        }
    }
    Ok(())
}

fn accepted_source_claims(
    mut actual: Vec<String>,
    proven: &HashSet<CommitId>,
    deadline: &Deadline,
) -> Result<Vec<String>, AdvanceComputeError> {
    for id in proven {
        deadline.check()?;
        actual.push(id.hex());
    }
    actual.sort();
    actual.dedup();
    if actual.len() > SQUASH_RECEIPT_LIMIT {
        return Err(AdvanceComputeError::EvidenceLimit {
            limit: SQUASH_RECEIPT_LIMIT,
        });
    }
    Ok(actual)
}

async fn changed(
    from: &Commit,
    to: &Commit,
    deadline: &Deadline,
) -> Result<Vec<(String, String)>, AdvanceComputeError> {
    let mut result = Vec::new();
    let mut stream = from.tree().diff_stream(&to.tree(), &EverythingMatcher);
    while let Some(entry) = stream.next().await {
        deadline.check()?;
        let values = entry.values.map_err(compute_error)?;
        let status = if values.before.as_resolved().is_some_and(Option::is_none) {
            "A"
        } else if values.after.as_resolved().is_some_and(Option::is_none) {
            "D"
        } else {
            "M"
        };
        result.push((
            status.to_string(),
            entry.path.as_internal_file_string().to_string(),
        ));
    }
    Ok(result)
}

async fn file_content(
    commit: &Commit,
    path: &str,
    deadline: &Deadline,
) -> Result<super::FileContent, String> {
    let path = RepoPathBuf::from_internal_string(path).map_err(|error| error.to_string())?;
    let value = commit
        .tree()
        .path_value(&path)
        .await
        .map_err(|error| error.to_string())?;
    match value.as_resolved() {
        Some(None) => Ok(super::FileContent::Absent),
        Some(Some(TreeValue::File { id, .. })) => {
            let mut reader = commit
                .store()
                .read_file(&path, id)
                .await
                .map_err(|error| error.to_string())?;
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 65536];
            loop {
                deadline
                    .check()
                    .map_err(|_| "candidate read deadline exhausted".to_string())?;
                let count = reader
                    .read(&mut chunk)
                    .await
                    .map_err(|error| error.to_string())?;
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..count]);
            }
            Ok(match String::from_utf8(bytes) {
                Ok(text) => super::FileContent::Text(text),
                Err(_) => super::FileContent::Binary,
            })
        }
        Some(Some(_)) => Ok(super::FileContent::Binary),
        None => Err("path contains a recorded conflict".to_string()),
    }
}

async fn diagnostic(
    snapshot: &BranchAdvanceSnapshot,
    ours: &Commit,
    base: &Commit,
    candidate: &Commit,
    paths: &[String],
    deadline: &Deadline,
) -> Result<super::ConflictDiagnostic, AdvanceComputeError> {
    let fork = crate::best_common_ancestor(snapshot.repo.as_ref(), ours.id(), base.id())
        .map_err(compute_error)?;
    let mut incoming = BTreeMap::new();
    if let Some(fork) = &fork {
        let fork = snapshot
            .repo
            .store()
            .get_commit(fork)
            .map_err(compute_error)?;
        for (status, path) in changed(&fork, base, deadline).await? {
            incoming.insert(path, status);
        }
    }
    for path in paths {
        incoming
            .entry(path.clone())
            .or_insert_with(|| "C".to_string());
    }
    let mut equal = !paths.is_empty();
    for path in paths {
        deadline.check()?;
        let path = RepoPathBuf::from_internal_string(path.clone()).map_err(compute_error)?;
        equal &= ours.tree().path_value(&path).await.map_err(compute_error)?
            == base.tree().path_value(&path).await.map_err(compute_error)?;
    }
    Ok(super::ConflictDiagnostic {
        base: fork.map(|id| id.hex()),
        ours: Some(ours.id().hex()),
        theirs: Some(base.id().hex()),
        conflicted_tip: Some(candidate.id().hex()),
        condition: if equal {
            super::ConflictCondition::BaseDrift
        } else {
            super::ConflictCondition::ContentConflict
        },
        incoming: incoming
            .into_iter()
            .map(|(path, status)| {
                let classification = if paths.contains(&path) {
                    super::IncomingClassification::Conflicting
                } else {
                    super::IncomingClassification::CleanOnRetry
                };
                super::IncomingFile {
                    path,
                    status,
                    classification,
                }
            })
            .collect(),
        scaffolding: None,
        validation: None,
    })
}

async fn apply_declared_edits(
    repo: &mut MutableRepo,
    candidate: &Commit,
    plan: &DeclaredMergeEdits,
    refused_paths: &[String],
    deadline: &Deadline,
) -> Result<Commit, AdvanceComputeError> {
    let paths: std::collections::BTreeSet<_> =
        plan.edits.iter().map(|(path, _)| path.as_str()).collect();
    let refused: std::collections::BTreeSet<_> = refused_paths.iter().map(String::as_str).collect();
    if paths.is_empty() || paths.len() != plan.edits.len() || paths != refused {
        return Err(compute_error(
            "declared structural merge does not exactly cover refused paths",
        ));
    }
    let mut tree = MergedTreeBuilder::new(candidate.tree());
    for (path, content) in &plan.edits {
        deadline.check()?;
        if let Some(failure) = crate::parse_gate::parse_failure(path, content) {
            return Err(compute_error(format!(
                "declared structural merge does not parse: {failure}"
            )));
        }
        let path = RepoPathBuf::from_internal_string(path.clone()).map_err(compute_error)?;
        let current = candidate
            .tree()
            .path_value(&path)
            .await
            .map_err(compute_error)?;
        let (executable, copy_id) = match current.as_resolved() {
            Some(Some(TreeValue::File {
                executable,
                copy_id,
                ..
            })) => (*executable, copy_id.clone()),
            _ => (false, CopyId::placeholder()),
        };
        let mut reader = futures::io::Cursor::new(content.as_bytes());
        let id = repo
            .store()
            .write_file(&path, &mut reader)
            .await
            .map_err(compute_error)?;
        tree.set_or_remove(
            path,
            Merge::resolved(Some(TreeValue::File {
                id,
                executable,
                copy_id,
            })),
        );
    }
    let tree = tree.write_tree().await.map_err(compute_error)?;
    let resolved = repo
        .new_commit(vec![candidate.id().clone()], tree)
        .set_description("Merge declared registry lists element-wise after base advance")
        .detach()
        .write_hidden()
        .await
        .map_err(compute_error)?;
    repo.add_head(&resolved).await.map_err(compute_error)?;
    Ok(resolved)
}

async fn candidate_parse_failures(
    ours: &Commit,
    base: &Commit,
    candidate: &Commit,
    paths: &[String],
    deadline: &Deadline,
) -> Result<Vec<super::ParseFailure>, AdvanceComputeError> {
    let mut contents = HashMap::new();
    for commit in [ours, base, candidate] {
        for path in paths {
            deadline.check()?;
            contents.insert(
                (commit.id().hex(), path.clone()),
                file_content(commit, path, deadline).await,
            );
        }
    }
    deadline.check()?;
    Ok(super::landing_gate::introduced_parse_failures_with(
        &candidate.id().hex(),
        &[&ours.id().hex(), &base.id().hex()],
        paths,
        |revision, path| {
            contents
                .get(&(revision.to_string(), path.to_string()))
                .cloned()
                .unwrap_or_else(|| Err("unreadable candidate path".to_string()))
        },
    ))
}

async fn compute_candidate(
    jj: &super::JjEnv,
    snapshot: &BranchAdvanceSnapshot,
    resolution: Option<&super::ResolutionCarry>,
    declared: Option<&DeclaredMergeEdits>,
    deadline: &Deadline,
) -> Result<
    (
        Commit,
        super::AncestryHeal,
        Vec<String>,
        Vec<StaleRegenerable>,
    ),
    AdvanceComputeError,
> {
    deadline.check()?;
    snapshot.note(|observed| observed.phase = Some(ComputePhase::Scope));
    let repo = &snapshot.repo;
    let ours = repo
        .store()
        .get_commit(
            &CommitId::try_from_hex(&snapshot.coordinate.head)
                .ok_or_else(|| compute_error("invalid captured head"))?,
        )
        .map_err(compute_error)?;
    let base = repo
        .store()
        .get_commit(
            &CommitId::try_from_hex(&snapshot.coordinate.base)
                .ok_or_else(|| compute_error("invalid captured base"))?,
        )
        .map_err(compute_error)?;
    if base.has_conflict() {
        return Err(AdvanceComputeError::BaseConflicted {
            base: base.id().hex(),
        });
    }
    if resolution.is_some_and(|carry| carry.expected_pre_tip != snapshot.coordinate.head) {
        return Err(AdvanceComputeError::CoordinatesChanged);
    }
    // Ask jj's configured immutable policy at the captured operation, never at
    // the live operation head. This read cannot auto-reconcile concurrent ops.
    let needs_rebase = !repo
        .index()
        .is_ancestor(base.id(), ours.id())
        .map_err(compute_error)?
        && !repo
            .index()
            .is_ancestor(ours.id(), base.id())
            .map_err(compute_error)?;
    let commits = branch_commits(repo.as_ref(), ours.id(), base.id(), deadline, None).await?;
    let flagged_in_history = commits
        .iter()
        .filter(|commit| commit.has_conflict())
        .count() as u64;
    snapshot.note(|observed| {
        observed.branch_commits = Some(commits.len() as u64);
        observed.flagged_in_history = Some(flagged_in_history);
    });
    let (pinned, mut mapping) =
        replay_boundaries(snapshot, &commits, &ours, &base, deadline).await?;
    let proven_base_versions: HashSet<_> = mapping.keys().cloned().collect();
    let mut dropped_ids = HashSet::new();
    let range = format!("({}..{})", base.id().hex(), ours.id().hex());
    let rewrite_scope = if needs_rebase {
        range.clone()
    } else {
        format!("(({range} & conflicts()):: & {range})")
    };
    let excluded: Vec<_> = pinned
        .iter()
        .chain(mapping.keys())
        .map(|id| id.hex())
        .collect();
    let rewrite_scope = if excluded.is_empty() {
        rewrite_scope
    } else {
        format!("({rewrite_scope}) ~ ({})", excluded.join(" | "))
    };
    let bounded = jj.with_operation_budget(deadline.0.saturating_duration_since(Instant::now()));
    let query = |expression: &str| {
        bounded.run(
            &snapshot.store,
            &[
                "log",
                "--at-op",
                &snapshot.coordinate.operation,
                "--ignore-working-copy",
                "-r",
                expression,
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "inspect immutable detached replay scope",
        )
    };
    let immutable = query(&format!("{rewrite_scope} & immutable()")).map_err(compute_error)?;
    if !immutable.trim().is_empty() {
        let on_trunk = !query(&format!("{rewrite_scope} & ::trunk()"))
            .map_err(compute_error)?
            .trim()
            .is_empty();
        let cause = if on_trunk {
            "Some commits are already on the default branch; this branch has at least partly landed. Refusing to replay already-landed history."
        } else if !query(&format!("{rewrite_scope} & ::untracked_remote_bookmarks()"))
            .map_err(compute_error)?
            .trim()
            .is_empty()
        {
            "None of this branch's commits are on the default branch; nothing has landed. They are immutable because origin holds them under a remote bookmark this store does not track."
        } else {
            "Some of this branch's commits are immutable under the repository's configured policy."
        };
        return Err(compute_error(format!(
            "immutable commit in detached replay scope. {cause}"
        )));
    }
    let mut transaction = repo.start_transaction();
    let mut candidate = ours.clone();
    let mut replay_dropped = Vec::new();
    let rebase_started = Instant::now();
    snapshot.note(|observed| {
        observed.phase = Some(ComputePhase::Rebase);
        observed.rebase_ms = Some(0);
    });
    if !repo
        .index()
        .is_ancestor(base.id(), ours.id())
        .map_err(compute_error)?
    {
        if repo
            .index()
            .is_ancestor(ours.id(), base.id())
            .map_err(compute_error)?
        {
            candidate = base.clone();
        } else {
            for old in &commits {
                if let Some(id) = mapping.get(old.id()) {
                    candidate = repo.store().get_commit(id).map_err(compute_error)?;
                    continue;
                }
                if pinned.contains(old.id()) {
                    candidate = old.clone();
                    mapping.insert(old.id().clone(), old.id().clone());
                    continue;
                }
                super::reached_step("advance: before hidden rewrite");
                snapshot.note(|observed| observed.rebase_ms = Some(elapsed_ms(rebase_started)));
                deadline.check()?;
                let mut parents = Vec::new();
                for parent in old.parent_ids() {
                    let mut parent = mapping
                        .get(parent)
                        .cloned()
                        .unwrap_or_else(|| base.id().clone());
                    // A proven landed prefix is already supplied by the whole
                    // base, including its later commits, not just its successor.
                    if transaction
                        .repo()
                        .index()
                        .is_ancestor(&parent, base.id())
                        .map_err(compute_error)?
                    {
                        parent = base.id().clone();
                    }
                    if !parents.contains(&parent) {
                        parents.push(parent);
                    }
                }
                let collapse =
                    if old.parent_ids().len() > 1 && parents.len() != old.parent_ids().len() {
                        Some(prove_merge_collapse(
                            transaction.repo(),
                            old,
                            &parents,
                            &base,
                            &proven_base_versions,
                            &dropped_ids,
                            deadline,
                        )?)
                    } else {
                        None
                    };
                // Healing can absorb a resolution-only commit into its older
                // conflicted parent (a different change ID). When the old clean
                // tree is exactly the base and its sole parent was mapped, that
                // resolution is newly empty. Do not replay conflict bookkeeping
                // as a fresh patch. Originally empty commits are still retained.
                if parents == [base.id().clone()]
                    && old.parent_ids().len() == 1
                    && mapping.contains_key(&old.parent_ids()[0])
                    && old.tree_ids() == base.tree_ids()
                    && repo
                        .store()
                        .get_commit(&old.parent_ids()[0])
                        .map_err(compute_error)?
                        .tree_ids()
                        != old.tree_ids()
                {
                    dropped_ids.insert(old.id().clone());
                    replay_dropped.push(super::emptied::dropped_label(
                        &old.id().hex(),
                        old.description().lines().next().unwrap_or(""),
                    ));
                    candidate = base.clone();
                    mapping.insert(old.id().clone(), base.id().clone());
                    continue;
                }
                let current =
                    rewrite_hidden(transaction.repo_mut(), old, parents, deadline).await?;
                super::reached_step("advance: hidden commit written");
                deadline.check()?;
                candidate = if let Some(parent) =
                    newly_empty(transaction.repo(), old, &current, collapse.as_ref()).await?
                {
                    dropped_ids.insert(old.id().clone());
                    replay_dropped.push(super::emptied::dropped_label(
                        &old.id().hex(),
                        old.description().lines().next().unwrap_or(""),
                    ));
                    repo.store().get_commit(&parent).map_err(compute_error)?
                } else {
                    current
                };
                mapping.insert(old.id().clone(), candidate.id().clone());
            }
        }
    }
    snapshot.note(|observed| {
        observed.rebase_ms = Some(elapsed_ms(rebase_started));
        observed.phase = Some(ComputePhase::Validate);
    });
    if transaction
        .repo()
        .index()
        .is_ancestor(candidate.id(), base.id())
        .map_err(compute_error)?
    {
        candidate = base.clone();
    }
    if candidate.has_conflict() {
        if let Some(carry) = resolution.filter(|carry| !carry.paths.is_empty()) {
            let mut tree = MergedTreeBuilder::new(candidate.tree());
            for (path, content) in &carry.paths {
                deadline.check()?;
                let path =
                    RepoPathBuf::from_internal_string(path.clone()).map_err(compute_error)?;
                let value = match content {
                    super::CarriedContent::FromTip => {
                        ours.tree().path_value(&path).await.map_err(compute_error)?
                    }
                    super::CarriedContent::Text(text) => {
                        let old_value =
                            ours.tree().path_value(&path).await.map_err(compute_error)?;
                        let (executable, copy_id) = match old_value.as_resolved() {
                            Some(Some(TreeValue::File {
                                executable,
                                copy_id,
                                ..
                            })) => (*executable, copy_id.clone()),
                            _ => (false, CopyId::placeholder()),
                        };
                        let mut reader = futures::io::Cursor::new(text.as_bytes());
                        let id = repo
                            .store()
                            .write_file(&path, &mut reader)
                            .await
                            .map_err(compute_error)?;
                        Merge::resolved(Some(TreeValue::File {
                            id,
                            executable,
                            copy_id,
                        }))
                    }
                };
                tree.set_or_remove(path, value);
            }
            let tree = tree.write_tree().await.map_err(compute_error)?;
            candidate = transaction
                .repo_mut()
                .rewrite_commit(&candidate)
                .set_predecessors(rewrite_predecessors(&candidate, deadline)?)
                .set_tree(tree)
                .detach()
                .write_hidden()
                .await
                .map_err(compute_error)?;
            transaction
                .repo_mut()
                .add_head(&candidate)
                .await
                .map_err(compute_error)?;
        }
    }
    if candidate.has_conflict() {
        if let Some(plan) = declared {
            // Regenerable paths are settled by taking a side below, never by
            // a declared merge, so the plan covers exactly the rest.
            let paths: Vec<String> = conflict_paths(&candidate)?
                .into_iter()
                .filter(|path| {
                    !snapshot
                        .regenerable
                        .as_ref()
                        .is_some_and(|regenerable| regenerable.covers(path))
                })
                .collect();
            candidate =
                apply_declared_edits(transaction.repo_mut(), &candidate, plan, &paths, deadline)
                    .await?;
        }
    }
    let mut stale = Vec::new();
    if candidate.has_conflict() {
        if let Some((tree, taken)) = take_regenerable_side(
            snapshot.regenerable.as_ref(),
            &candidate.tree(),
            &ours,
            deadline,
        )
        .await?
        {
            candidate = transaction
                .repo_mut()
                .rewrite_commit(&candidate)
                .set_predecessors(rewrite_predecessors(&candidate, deadline)?)
                .set_tree(tree)
                .detach()
                .write_hidden()
                .await
                .map_err(compute_error)?;
            transaction
                .repo_mut()
                .add_head(&candidate)
                .await
                .map_err(compute_error)?;
            stale = taken;
        }
    }
    if candidate.has_conflict() {
        let paths = conflict_paths(&candidate)?;
        return Err(AdvanceComputeError::Conflict(Box::new(
            diagnostic(snapshot, &ours, &base, &candidate, &paths, deadline).await?,
        )));
    }
    // Reuse the merge parse-blame policy with an unpublished object reader.
    let fork =
        crate::best_common_ancestor(repo.as_ref(), ours.id(), base.id()).map_err(compute_error)?;
    let mut overlapping = Vec::new();
    if let Some(fork) = fork {
        let fork = repo.store().get_commit(&fork).map_err(compute_error)?;
        let own = changed(&fork, &ours, deadline).await?;
        overlapping = changed(&ours, &candidate, deadline)
            .await?
            .into_iter()
            .map(|(_, path)| path)
            .filter(|path| own.iter().any(|(_, own)| own == path))
            .filter(|path| !stale.iter().any(|taken| taken.path == *path))
            .collect();
    }
    let mut failures =
        candidate_parse_failures(&ours, &base, &candidate, &overlapping, deadline).await?;
    if !failures.is_empty() {
        if let Some(plan) = declared {
            let paths: Vec<_> = failures
                .iter()
                .map(|failure| failure.path.clone())
                .collect();
            candidate =
                apply_declared_edits(transaction.repo_mut(), &candidate, plan, &paths, deadline)
                    .await?;
            failures =
                candidate_parse_failures(&ours, &base, &candidate, &overlapping, deadline).await?;
        }
    }
    if !failures.is_empty() {
        let paths: Vec<_> = failures
            .iter()
            .map(|failure| failure.path.clone())
            .collect();
        let mut refusal = diagnostic(snapshot, &ours, &base, &candidate, &paths, deadline).await?;
        refusal.condition = super::ConflictCondition::Unparseable;
        refusal.validation = Some(super::ValidationRefusal {
            commit: candidate.id().hex(),
            failures,
        });
        return Err(AdvanceComputeError::Conflict(Box::new(refusal)));
    }
    let backing = super::export::resolve_backing_checkout(&snapshot.store)
        .ok_or_else(|| compute_error("detached advance has no backing Git repository"))?;
    let bounded_jj = jj.with_operation_budget(deadline.0.saturating_duration_since(Instant::now()));
    let entries = super::export::root_conflict_scaffolding(
        Some(&bounded_jj),
        &backing,
        &candidate.id().hex(),
    )
    .map_err(compute_error)?;
    if !entries.is_empty() {
        let mut refusal =
            diagnostic(snapshot, &ours, &base, &candidate, &overlapping, deadline).await?;
        refusal.condition = super::ConflictCondition::ContentConflict;
        refusal.scaffolding = Some(super::ScaffoldingRefusal {
            commit: candidate.id().hex(),
            entries,
        });
        return Err(AdvanceComputeError::Conflict(Box::new(refusal)));
    }
    let (head, mut healing) = heal_branch(
        transaction.repo_mut(),
        &candidate,
        &base,
        &pinned,
        &mut dropped_ids,
        deadline,
        snapshot.cost.as_ref(),
    )
    .await?;
    snapshot.note(|observed| observed.phase = Some(ComputePhase::Claims));
    if !transaction
        .repo()
        .index()
        .is_ancestor(base.id(), head.id())
        .map_err(compute_error)?
    {
        return Err(compute_error(
            "detached candidate does not include its captured base",
        ));
    }
    replay_dropped.append(&mut healing.dropped);
    healing.dropped = replay_dropped;
    let actual_drops = exact_source_drops(
        transaction.repo(),
        &commits,
        &head,
        &base,
        &proven_base_versions,
        &dropped_ids,
        deadline,
    )
    .await?;
    // Replay/heal decisions retain their actual-drop set. Only the accepted
    // candidate's emitted evidence also retains its exact proven base mappings.
    let dropped_commit_ids = accepted_source_claims(actual_drops, &proven_base_versions, deadline)?;
    validate_removed_claims(transaction.repo(), &head, &dropped_commit_ids, deadline)?;
    deadline.check()?;
    snapshot.note(|observed| observed.phase = Some(ComputePhase::Done));
    Ok((head, healing, dropped_commit_ids, stale))
}

/// A successful local installation, with export failure kept distinct from
/// durable acceptance. A failed export must be recovered, never retried as a
/// second replay or reported as a fully published branch.
#[derive(Debug)]
pub struct InstalledBranchAdvance {
    pub coordinate: AdvanceCoordinate,
    pub head: String,
    pub tree: String,
    pub export: Result<(), String>,
    pub healing: super::AncestryHeal,
    pub dropped_commit_ids: Vec<String>,
    pub accepted_operation: Option<String>,
    /// What the export's budgeted transaction could not verify (a killed
    /// writer not proven gone). Its environment ends with this call, so the
    /// caller, still holding the store guard, persists these as a fence.
    pub recoveries: Vec<super::RecoveryCoordinates>,
}

/// Durable jj acceptance before backing-Git export. The candidate's object
/// lease remains held while the caller records acceptance under its store lock.
pub struct LocallyInstalledBranchAdvance {
    pub coordinate: AdvanceCoordinate,
    pub head: String,
    pub tree: String,
    pub healing: super::AncestryHeal,
    pub dropped_commit_ids: Vec<String>,
    pub accepted_operation: Option<String>,
    candidate: BranchAdvanceCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvanceInstallError {
    CoordinatesChanged,
    Expired,
    Repository(String),
}

impl std::fmt::Display for AdvanceInstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CoordinatesChanged => {
                f.write_str("branch or base changed before detached advance installation")
            }
            Self::Expired => f.write_str(
                "detached branch advance installation timed out: operation budget exhausted",
            ),
            Self::Repository(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for AdvanceInstallError {}

/// The legacy explicit heal adapter, sharing the detached engine. Its caller
/// holds the canonical writer lock throughout capture, compute and installation.
pub(super) fn heal_branch_locked(
    jj: &super::JjEnv,
    store: &Path,
    branch: &str,
    base: &str,
) -> Result<super::AncestryHeal, String> {
    let snapshot = capture_replay(store, branch, base, false)?;
    let head = CommitId::try_from_hex(&snapshot.coordinate.head).ok_or("invalid heal head")?;
    let base = CommitId::try_from_hex(&snapshot.coordinate.base).ok_or("invalid heal base")?;
    if !snapshot
        .repo
        .index()
        .is_ancestor(&base, &head)
        .map_err(|error| error.to_string())?
    {
        return Err(format!(
            "{} branch does not descend from its heal base",
            super::ANCESTRY_HEAL_GUARD
        ));
    }
    if snapshot
        .repo
        .store()
        .get_commit(&head)
        .map_err(|error| error.to_string())?
        .has_conflict()
    {
        return Err(format!(
            "{} tip is itself conflict-flagged",
            super::ANCESTRY_HEAL_GUARD
        ));
    }
    let pre_op = snapshot.coordinate.operation.clone();
    let remaining = jj
        .operation_deadline
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .unwrap_or(super::env::JJ_DEFAULT_TIMEOUT);
    let candidate =
        compute_legacy_replay(jj, snapshot, None, remaining).map_err(|error| error.to_string())?;
    let installed = install_legacy_replay(jj, candidate).map_err(|error| error.to_string())?;
    if let Err(error) = installed.export {
        return Err(super::merge::roll_back_rebase(
            jj, store, branch, &pre_op, error,
        ));
    }
    Ok(installed.healing)
}

/// Install only the candidate's branch into a freshly loaded transaction.
///
/// The caller MUST hold the canonical store writer lock through this function
/// and independently revalidate the declared base relationship under that lock.
/// There is no network I/O here. No operation or view from computation is
/// published; unrelated branch, workspace and remote updates survive.
pub fn install_branch_advance_exported(
    jj: &super::JjEnv,
    candidate: BranchAdvanceCandidate,
) -> Result<InstalledBranchAdvance, AdvanceInstallError> {
    install_branch_advance_local(jj, candidate)
        .map(|local| export_installed_branch_advance(jj, local))
}

pub(super) fn install_legacy_replay(
    jj: &super::JjEnv,
    candidate: BranchAdvanceCandidate,
) -> Result<InstalledBranchAdvance, AdvanceInstallError> {
    install_branch_advance_local(jj, candidate).map(|local| export_installed(jj, local, true))
}

/// Accept a detached candidate locally, without exporting any Git ref.
/// The caller holds the canonical writer lock and revalidates the relationship.
/// Record a durable acceptance witness before calling the export half.
pub fn install_branch_advance_local(
    _jj: &super::JjEnv,
    candidate: BranchAdvanceCandidate,
) -> Result<LocallyInstalledBranchAdvance, AdvanceInstallError> {
    let remaining = candidate
        .expires_at
        .saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(AdvanceInstallError::Expired);
    }
    let tree = candidate.tree().map_err(AdvanceInstallError::Repository)?;
    let head = candidate.head();
    let accepted_operation = block_on(async {
        let snapshot = &candidate.snapshot;
        let repo = crate::load_repo_at_head(&snapshot.store)
            .await
            .map_err(|error| format!("reload detached advance store: {error}"))?;
        if !snapshot.matches_repo(&repo) {
            return Ok(None);
        }
        // Indexing is ephemeral even for an unchanged head. It validates the
        // base-inclusion invariant; only a changed branch commits this transaction.
        let mut transaction = repo.start_transaction();
        transaction
            .repo_mut()
            .add_head(&candidate.head)
            .await
            .map_err(|error| format!("index detached advance: {error}"))?;
        let base =
            CommitId::try_from_hex(&snapshot.coordinate.base).ok_or("invalid captured base")?;
        if !transaction
            .repo()
            .index()
            .is_ancestor(&base, candidate.head.id())
            .map_err(|error| error.to_string())?
        {
            return Err("detached candidate does not include its captured base".to_string());
        }
        if head != snapshot.coordinate.head {
            transaction.repo_mut().set_local_bookmark_target(
                RefName::new(&snapshot.coordinate.branch),
                RefTarget::normal(candidate.head.id().clone()),
            );
            let old =
                CommitId::try_from_hex(&snapshot.coordinate.head).ok_or("invalid captured head")?;
            let old_commit = snapshot
                .repo
                .store()
                .get_commit(&old)
                .map_err(|error| error.to_string())?;
            if candidate.head.change_id() == old_commit.change_id() {
                let anchored = repo
                    .view()
                    .local_bookmarks_for_commit(&old)
                    .any(|(name, _)| name != RefName::new(&snapshot.coordinate.branch))
                    || repo
                        .view()
                        .local_tags()
                        .any(|(_, target)| target.added_ids().any(|id| id == &old))
                    || repo.view().wc_commit_ids().values().any(|id| id == &old);
                if !anchored {
                    transaction.repo_mut().remove_head(&old);
                }
            }
            // A fresh-identity squash retains the old graph without inspecting
            // or changing riders. Maintenance can reclaim unowned residue later.
            // The only publication point. All expensive candidate work happened
            // before this fresh transaction, not in a previously captured view.
            super::reached_step("advance: before install commit");
            if Instant::now() >= candidate.expires_at {
                return Ok(Some(None));
            }
            let accepted = transaction
                .commit(format!(
                    "advance `{}` onto `{}`",
                    snapshot.coordinate.branch, snapshot.coordinate.base_branch
                ))
                .await
                .map_err(|error| format!("install detached advance: {error}"))?;
            return Ok(Some(Some(Some(accepted.op_id().hex()))));
        }
        if Instant::now() >= candidate.expires_at {
            return Ok(Some(None));
        }
        Ok(Some(Some(None)))
    })
    .map_err(AdvanceInstallError::Repository)?
    .ok_or(AdvanceInstallError::CoordinatesChanged)
    .and_then(|accepted| accepted.ok_or(AdvanceInstallError::Expired))?;
    Ok(LocallyInstalledBranchAdvance {
        coordinate: candidate.snapshot.coordinate.clone(),
        head,
        tree,
        healing: candidate.healing.clone(),
        dropped_commit_ids: candidate.dropped_commit_ids.clone(),
        accepted_operation,
        candidate,
    })
}

/// Complete verified export after the caller has durably recorded acceptance.
/// The caller must retain the same canonical writer guard across both halves.
pub fn export_installed_branch_advance(
    jj: &super::JjEnv,
    local: LocallyInstalledBranchAdvance,
) -> InstalledBranchAdvance {
    export_installed(jj, local, false)
}

fn export_installed(
    jj: &super::JjEnv,
    local: LocallyInstalledBranchAdvance,
    legacy: bool,
) -> InstalledBranchAdvance {
    let candidate = &local.candidate;
    let snapshot = &candidate.snapshot;
    // The lease remains alive through verified export and all its repair
    // paths, even if the candidate expires after durable jj acceptance.
    super::reached_step("rebase: before export");
    let export_env = if legacy {
        jj.clone()
    } else {
        jj.with_operation_budget(
            candidate
                .expires_at
                .saturating_duration_since(Instant::now()),
        )
        .cleanup_env()
    };
    let export = export_accepted_branch(
        &export_env,
        &snapshot.store,
        &snapshot.coordinate.branch,
        &local.head,
    );
    if !candidate.healing.dropped.is_empty() {
        log::info!(
            "accepted advance of `{}` dropped newly empty commits: {}",
            snapshot.coordinate.branch,
            candidate.healing.dropped.join(", ")
        );
    }
    // A legacy export ran in the caller's own environment, whose ledger the
    // caller drains itself.
    let recoveries = if legacy {
        Vec::new()
    } else {
        export_env.take_recoveries()
    };
    InstalledBranchAdvance {
        coordinate: local.coordinate,
        head: local.head,
        tree: local.tree,
        export,
        healing: local.healing,
        dropped_commit_ids: local.dropped_commit_ids,
        accepted_operation: local.accepted_operation,
        recoveries,
    }
}

/// Re-export one already accepted exact head, including after process restart.
/// The caller MUST hold the canonical writer lock through the check and export.
/// This neither replays nor grants authority to a pending acceptance record.
pub fn export_accepted_branch(
    jj: &super::JjEnv,
    store: &Path,
    branch: &str,
    expected_head: &str,
) -> Result<(), String> {
    block_on(async {
        let repo = crate::load_repo_at_head(store)
            .await
            .map_err(|error| error.to_string())?;
        let expected =
            indexed_evidence(repo.as_ref(), expected_head).map_err(|error| error.to_string())?;
        if normal_target(&repo, branch)? != expected {
            return Err("accepted advance head changed before export".to_string());
        }
        Ok(())
    })?;
    super::export::export_bookmark_advance(
        jj,
        store,
        true,
        branch,
        "export accepted branch advance",
    )
}

fn normal_target(repo: &ReadonlyRepo, branch: &str) -> Result<CommitId, String> {
    repo.view()
        .get_local_bookmark(RefName::new(branch))
        .as_normal()
        .cloned()
        .ok_or_else(|| format!("advance bookmark `{branch}` is missing or conflicted"))
}

fn block_on<T>(future: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
    // Local jj futures need no Tokio reactor. A blocking worker may still carry
    // an entered Tokio handle, where starting another Runtime would panic.
    futures::executor::block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jj::testing::*;
    use crate::jj::*;
    use jj_lib::op_store::RefTarget;
    use std::time::SystemTime;
    use tempfile::TempDir;

    async fn fixture_commit(
        repo: &mut MutableRepo,
        parent: &Commit,
        path: &str,
        content: &str,
    ) -> Commit {
        let path = RepoPathBuf::from_internal_string(path).unwrap();
        let mut reader = futures::io::Cursor::new(content.as_bytes());
        let id = repo.store().write_file(&path, &mut reader).await.unwrap();
        let mut tree = MergedTreeBuilder::new(parent.tree());
        tree.set_or_remove(
            path,
            Merge::resolved(Some(TreeValue::File {
                id,
                executable: false,
                copy_id: CopyId::placeholder(),
            })),
        );
        let tree = tree.write_tree().await.unwrap();
        repo.new_commit(vec![parent.id().clone()], tree)
            .set_description(content)
            .detach()
            .write_hidden()
            .await
            .unwrap()
    }

    #[test]
    fn accepted_source_claims_are_exact_sorted_complete_or_typed_refusal() {
        let deadline = Deadline(Instant::now() + Duration::from_secs(30));
        let proven = (1..=SQUASH_RECEIPT_LIMIT)
            .map(|number| CommitId::try_from_hex(format!("{number:040x}")).unwrap())
            .collect::<HashSet<_>>();
        let first = format!("{:040x}", 1);
        let claims = accepted_source_claims(vec![first.clone()], &proven, &deadline).unwrap();
        assert_eq!(claims.len(), SQUASH_RECEIPT_LIMIT);
        assert_eq!(claims.first(), Some(&first));
        assert!(claims.windows(2).all(|pair| pair[0] < pair[1]));
        let overflow = format!("{:040x}", SQUASH_RECEIPT_LIMIT + 1);
        assert!(matches!(
            accepted_source_claims(vec![overflow], &proven, &deadline),
            Err(AdvanceComputeError::EvidenceLimit {
                limit: SQUASH_RECEIPT_LIMIT
            })
        ));
        assert!(matches!(
            accepted_source_claims(Vec::new(), &proven, &Deadline(Instant::now())),
            Err(AdvanceComputeError::TimedOut)
        ));
    }

    #[test]
    #[serial_test::serial(jj)]
    fn removed_claim_guard_checks_both_actual_drops_and_proven_mappings() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let snapshot = fixture.snapshot();
        let head = snapshot
            .repo
            .store()
            .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head).unwrap())
            .unwrap();
        let deadline = Deadline(Instant::now() + Duration::from_secs(30));
        for claims in [
            accepted_source_claims(vec![head.id().hex()], &HashSet::new(), &deadline).unwrap(),
            accepted_source_claims(
                Vec::new(),
                &HashSet::from([head.parent_ids()[0].clone()]),
                &deadline,
            )
            .unwrap(),
        ] {
            assert!(
                matches!(validate_removed_claims(snapshot.repo.as_ref(), &head, &claims, &deadline),
                Err(AdvanceComputeError::Repository(ref message)) if message.starts_with(REMOVED_CLAIM_INVARIANT))
            );
        }
        assert!(matches!(
            validate_removed_claims(
                snapshot.repo.as_ref(),
                &head,
                &[head.id().hex()[..12].to_string()],
                &deadline
            ),
            Err(AdvanceComputeError::UnknownEvidence { .. })
        ));
    }

    #[test]
    fn native_squash_publication_has_no_unnamed_branch_writer() {
        fn body<'a>(source: &'a str, signature: &str) -> &'a str {
            let start = source
                .lines()
                .scan(0usize, |offset, line| {
                    let at = *offset;
                    *offset += line.len() + 1;
                    Some((at, line))
                })
                .find(|(_, line)| line.starts_with(signature))
                .unwrap()
                .0;
            let end = source[start..].find("\n}\n").unwrap() + start;
            &source[start..end]
        }
        let advance = include_str!("advance.rs");
        let merge = include_str!("merge.rs");
        for text in [
            body(advance, "pub(super) fn compute_branch_squash("),
            body(merge, "pub fn squash_branch_onto("),
            body(advance, "pub fn install_branch_advance_local("),
        ] {
            for forbidden in [
                "rider",
                "\"abandon\"",
                "rebase_descendants",
                "\"rebase\"",
                "bookmarks()",
                "visible_heads",
                ".rewrite_commit(",
                "op_restore",
            ] {
                // Descriptive comments are not executable writers.
                let code = text
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("//"))
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(!code.contains(forbidden), "{forbidden} in {text}");
            }
        }
        let factory = body(advance, "pub(super) fn compute_branch_squash(");
        assert!(factory.contains(".new_commit("));
        assert!(factory.contains(".set_predecessors("));
        assert!(factory.contains(".write_hidden()"));
        assert!(!factory.contains(".commit("));
        let install = body(advance, "pub fn install_branch_advance_local(");
        assert!(install.contains("snapshot.matches_repo(&repo)"));
        assert_eq!(install.matches(".set_local_bookmark_target(").count(), 1);
        assert!(install.contains("candidate.head.change_id() == old_commit.change_id()"));
    }

    #[test]
    #[serial_test::serial(jj)]
    fn branch_only_squash_refuses_stale_source_and_expired_entry_without_writes() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let snapshot = fixture.snapshot();
        let source = snapshot.coordinate.head.clone();
        let base = snapshot.coordinate.base.clone();
        let before = operation_id(&fixture.jj, &fixture.store).unwrap();
        assert!(matches!(
            squash_branch_onto(
                &fixture.jj,
                &fixture.store,
                "feature",
                &base,
                &base,
                "stale"
            ),
            Err(SquashError::CoordinatesChanged)
        ));
        assert!(matches!(
            squash_branch_onto(
                &fixture.jj.with_operation_budget(Duration::ZERO),
                &fixture.store,
                "feature",
                &source,
                &base,
                "expired"
            ),
            Err(SquashError::TimedOut)
        ));
        assert_eq!(operation_id(&fixture.jj, &fixture.store).unwrap(), before);
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap(),
            source
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_pin_can_only_withhold_an_exact_base_successor() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        futures::executor::block_on(async {
            let repo = crate::load_repo_at_head(&fixture.store).await.unwrap();
            let old = repo
                .store()
                .get_commit(&normal_target(&repo, "feature").unwrap())
                .unwrap();
            let mut tx = repo.start_transaction();
            let temporal = tx
                .repo_mut()
                .rewrite_commit(&old)
                .set_predecessors(vec![old.id().clone()])
                .set_description("temporal proof")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            let structural = tx
                .repo_mut()
                .new_commit(old.parent_ids().to_vec(), old.tree())
                .set_predecessors(vec![old.id().clone()])
                .set_description("structural proof")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            let base = tx
                .repo_mut()
                .new_commit(
                    vec![temporal.id().clone(), structural.id().clone()],
                    old.tree(),
                )
                .set_description("base containing incomparable proofs")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            tx.repo_mut().add_head(&base).await.unwrap();
            let deadline = Deadline(Instant::now() + Duration::from_secs(30));
            for mask in 0..4 {
                let mut targets = HashSet::new();
                if mask & 1 != 0 {
                    targets.insert(temporal.id().clone());
                }
                if mask & 2 != 0 {
                    targets.insert(structural.id().clone());
                }
                let unpinned =
                    exact_base_successor(tx.repo(), &old, &base, &targets, true, &deadline)
                        .unwrap();
                let pinned =
                    exact_base_successor(tx.repo(), &old, &base, &targets, false, &deadline)
                        .unwrap();
                assert!(
                    pinned.is_none() || pinned == unpinned,
                    "a pin cannot resolve ambiguity or add authority"
                );
                match mask {
                    0 | 3 => {
                        assert!(unpinned.is_none());
                        assert!(pinned.is_none());
                    }
                    1 => {
                        assert_eq!(unpinned, Some(temporal.id().clone()));
                        assert!(pinned.is_none());
                    }
                    2 => {
                        assert_eq!(unpinned, Some(structural.id().clone()));
                        assert_eq!(pinned, unpinned);
                    }
                    _ => unreachable!(),
                }
            }
        });
    }

    #[test]
    #[serial_test::serial(jj)]
    fn branch_only_squash_receipts_keep_foreign_reverts_but_never_absorb_an_amendment() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let (base, source, foreign, foreign_owner) = futures::executor::block_on(async {
            let repo = crate::load_repo_at_head(&fixture.store).await.unwrap();
            let base = repo
                .store()
                .get_commit(&normal_target(&repo, "main").unwrap())
                .unwrap();
            let mut tx = repo.start_transaction();
            let foreign =
                fixture_commit(tx.repo_mut(), &base, "foreign.txt", "old foreign effect\n").await;
            let owner = fixture_commit(
                tx.repo_mut(),
                &foreign,
                "foreign-owner.txt",
                "new owner delta\n",
            )
            .await;
            let ours = fixture_commit(tx.repo_mut(), &base, "source.txt", "source delta\n").await;
            let combined =
                fixture_commit(tx.repo_mut(), &ours, "foreign.txt", "old foreign effect\n").await;
            let merged = tx
                .repo_mut()
                .new_commit(
                    vec![ours.id().clone(), foreign.id().clone()],
                    combined.tree(),
                )
                .set_description("merge exact foreign identity")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            let mut tree = MergedTreeBuilder::new(merged.tree());
            tree.set_or_remove(
                RepoPathBuf::from_internal_string("foreign.txt").unwrap(),
                Merge::resolved(None),
            );
            let tree = tree.write_tree().await.unwrap();
            let source = tx
                .repo_mut()
                .new_commit(vec![merged.id().clone()], tree)
                .set_description("revert foreign effect")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            for (name, commit) in [("feature", &source), ("foreign-owner", &owner)] {
                tx.repo_mut().add_head(commit).await.unwrap();
                tx.repo_mut().set_local_bookmark_target(
                    RefName::new(name),
                    RefTarget::normal(commit.id().clone()),
                );
            }
            tx.repo_mut().set_remote_bookmark(
                RefName::new("feature")
                    .to_remote_symbol(jj_lib::ref_name::RemoteName::new("origin")),
                jj_lib::op_store::RemoteRef {
                    target: RefTarget::normal(source.id().clone()),
                    state: jj_lib::op_store::RemoteRefState::Tracked,
                },
            );
            tx.commit("seed source merge and later revert with an independent owner")
                .await
                .unwrap();
            (base, source, foreign, owner)
        });
        let report = squash_branch_onto(
            &fixture.jj,
            &fixture.store,
            "feature",
            &source.id().hex(),
            &base.id().hex(),
            "land net source tree",
        )
        .unwrap();
        origin_copy_is_superseded_with_accepted(
            &fixture.jj,
            &fixture.store,
            "feature",
            &report.squashed_commit,
            &source.id().hex(),
            &[],
            Duration::from_secs(60),
        )
        .unwrap();
        let snapshot = capture_branch_advance(&fixture.store, "foreign-owner", "feature").unwrap();
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(60)).unwrap();
        assert_eq!(candidate.dropped_commit_ids(), &[foreign.id().hex()]);
        assert_eq!(candidate.head.change_id(), foreign_owner.change_id());
        let path = RepoPathBuf::from_internal_string("foreign.txt").unwrap();
        assert!(
            futures::executor::block_on(candidate.head.tree().path_value(&path))
                .unwrap()
                .as_resolved()
                .is_some_and(Option::is_none)
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:foreign-owner.txt", candidate.head())]
            ),
            "new owner delta"
        );
        let foreign_candidate = candidate;
        let amended = futures::executor::block_on(async {
            let repo = crate::load_repo_at_head(&fixture.store).await.unwrap();
            let mut tx = repo.start_transaction();
            let tree =
                fixture_commit(tx.repo_mut(), &base, "foreign.txt", "new owner amendment\n").await;
            let amended = tx
                .repo_mut()
                .rewrite_commit(&foreign)
                .set_predecessors(vec![foreign.id().clone()])
                .set_tree(tree.tree())
                .set_description("amend old foreign identity")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            tx.repo_mut().add_head(&amended).await.unwrap();
            tx.repo_mut().set_local_bookmark_target(
                RefName::new("amended-owner"),
                RefTarget::normal(amended.id().clone()),
            );
            tx.commit("owner authors a new version absent from the squash receipt")
                .await
                .unwrap();
            amended
        });
        let snapshot = capture_branch_advance(&fixture.store, "amended-owner", "feature").unwrap();
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(60)).unwrap();
        assert_eq!(candidate.head.change_id(), amended.change_id());
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:foreign.txt", candidate.head())]
            ),
            "new owner amendment"
        );
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "foreign-owner").unwrap(),
            foreign_owner.id().hex()
        );
        futures::executor::block_on(async {
            let repo = crate::load_repo_at_head(&fixture.store).await.unwrap();
            let mut tx = repo.start_transaction();
            let own = fixture_commit(
                tx.repo_mut(),
                &source,
                "child.txt",
                "child-owned merge work\n",
            )
            .await;
            let child = tx
                .repo_mut()
                .new_commit(vec![own.id().clone(), foreign.id().clone()], own.tree())
                .set_description("child retains foreign merge arm")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            tx.repo_mut().add_head(&child).await.unwrap();
            tx.repo_mut().set_local_bookmark_target(
                RefName::new("source-child"),
                RefTarget::normal(child.id().clone()),
            );
            tx.repo_mut().set_remote_bookmark(
                RefName::new("feature")
                    .to_remote_symbol(jj_lib::ref_name::RemoteName::new("origin")),
                jj_lib::op_store::RemoteRef {
                    target: RefTarget::normal(amended.id().clone()),
                    state: jj_lib::op_store::RemoteRefState::Tracked,
                },
            );
            tx.commit("retain a foreign arm and observe outside origin amendment")
                .await
                .unwrap();
        });
        let snapshot = capture_branch_advance(&fixture.store, "source-child", "feature").unwrap();
        futures::executor::block_on(async {
            let deadline = Deadline(Instant::now() + Duration::from_secs(60));
            let head = snapshot
                .repo
                .store()
                .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head).unwrap())
                .unwrap();
            let base = snapshot
                .repo
                .store()
                .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.base).unwrap())
                .unwrap();
            let commits = branch_commits(
                snapshot.repo.as_ref(),
                head.id(),
                base.id(),
                &deadline,
                None,
            )
            .await
            .unwrap();
            let (pinned, mapping) = replay_boundaries(&snapshot, &commits, &head, &base, &deadline)
                .await
                .unwrap();
            assert!(!pinned.contains(foreign.id()));
            assert!(
                mapping.contains_key(foreign.id()),
                "the exact receipt governs this merge parent"
            );
        });
        let error = origin_copy_is_superseded_with_accepted(
            &fixture.jj,
            &fixture.store,
            "feature",
            &report.squashed_commit,
            &amended.id().hex(),
            &[],
            Duration::from_secs(60),
        )
        .unwrap_err();
        let AdvanceComputeError::UnaccountedRemoteCommits { commits } = error else {
            panic!("outside owner amendment must have a named refusal: {error}");
        };
        assert!(commits.contains(&amended.id().hex()));

        // The child's publication is proven by L's structural receipts and
        // the rewritten child's own predecessor. Accepted mappings retain proof
        // when the bounded ancestry walk cannot reach L.
        let origin = TempDir::new().unwrap();
        git(origin.path(), &["init", "--bare", "-q", "-b", "main"]);
        git(
            fixture.project.path(),
            &["remote", "add", "origin", origin.path().to_str().unwrap()],
        );
        git(
            fixture.project.path(),
            &[
                "push",
                "--quiet",
                "origin",
                &format!("{}:refs/heads/foreign-owner", foreign_owner.id().hex()),
            ],
        );
        futures::executor::block_on(async {
            let repo = crate::load_repo_at_head(&fixture.store).await.unwrap();
            let mut tx = repo.start_transaction();
            tx.repo_mut().set_remote_bookmark(
                RefName::new("foreign-owner")
                    .to_remote_symbol(jj_lib::ref_name::RemoteName::new("origin")),
                jj_lib::op_store::RemoteRef {
                    target: RefTarget::normal(foreign_owner.id().clone()),
                    state: jj_lib::op_store::RemoteRefState::Tracked,
                },
            );
            tx.commit("record the child's exact old origin coordinate")
                .await
                .unwrap();
        });
        let unchanged_child = bookmark_commit(&fixture.jj, &fixture.store, "source-child").unwrap();
        let installed = install_branch_advance_exported(&fixture.jj, foreign_candidate).unwrap();
        installed.export.unwrap();
        assert_eq!(installed.dropped_commit_ids, [foreign.id().hex()]);
        origin_copy_is_superseded_with_accepted(
            &fixture.jj,
            &fixture.store,
            "foreign-owner",
            &installed.head,
            &foreign_owner.id().hex(),
            &[],
            Duration::from_secs(60),
        )
        .unwrap();
        let claims = [AcceptedAdvanceDrops {
            head_after: installed.head.clone(),
            dropped_commit_ids: installed.dropped_commit_ids.clone(),
        }];
        struct RestoreAncestorLimit(usize);
        impl Drop for RestoreAncestorLimit {
            fn drop(&mut self) {
                REMOTE_PROOF_ANCESTOR_LIMIT.with(|limit| limit.set(self.0));
            }
        }
        let limit_scope =
            RestoreAncestorLimit(REMOTE_PROOF_ANCESTOR_LIMIT.with(|limit| limit.replace(0)));
        let missing = origin_copy_is_superseded_with_accepted(
            &fixture.jj,
            &fixture.store,
            "foreign-owner",
            &installed.head,
            &foreign_owner.id().hex(),
            &[],
            Duration::from_secs(60),
        )
        .unwrap_err();
        assert!(
            matches!(missing, AdvanceComputeError::UnaccountedRemoteCommits { ref commits }
            if commits == &[foreign.id().hex()])
        );
        origin_copy_is_superseded_with_accepted(
            &fixture.jj,
            &fixture.store,
            "foreign-owner",
            &installed.head,
            &foreign_owner.id().hex(),
            &claims,
            Duration::from_secs(60),
        )
        .unwrap();
        let accepted = push_managed_branch_unrecorded(
            &fixture.jj,
            &fixture.store,
            "foreign-owner",
            Some(&foreign_owner.id().hex()),
            &installed.head,
            Some(&claims),
        )
        .unwrap();
        drop(limit_scope);
        readback_leased_push(&fixture.jj, &fixture.store, &accepted).unwrap();
        record_leased_push(&fixture.jj, &fixture.store, &accepted).unwrap();
        assert_eq!(
            git_stdout(origin.path(), &["rev-parse", "refs/heads/foreign-owner"]),
            installed.head
        );
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "source-child").unwrap(),
            unchanged_child
        );
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "amended-owner").unwrap(),
            amended.id().hex()
        );
        git(
            fixture.project.path(),
            &[
                "push",
                "--quiet",
                "--force",
                "origin",
                &format!("{}:refs/heads/foreign-owner", amended.id().hex()),
            ],
        );
        futures::executor::block_on(async {
            let repo = crate::load_repo_at_head(&fixture.store).await.unwrap();
            let mut tx = repo.start_transaction();
            tx.repo_mut().set_remote_bookmark(
                RefName::new("foreign-owner")
                    .to_remote_symbol(jj_lib::ref_name::RemoteName::new("origin")),
                jj_lib::op_store::RemoteRef {
                    target: RefTarget::normal(amended.id().clone()),
                    state: jj_lib::op_store::RemoteRefState::Tracked,
                },
            );
            tx.commit("observe an outside owner's exact amendment on origin")
                .await
                .unwrap();
        });
        let outside = origin_copy_is_superseded_with_accepted(
            &fixture.jj,
            &fixture.store,
            "foreign-owner",
            &installed.head,
            &amended.id().hex(),
            &claims,
            Duration::from_secs(60),
        )
        .unwrap_err();
        assert!(
            matches!(outside, AdvanceComputeError::UnaccountedRemoteCommits { ref commits }
            if commits.contains(&amended.id().hex()))
        );
        let refused = push_managed_branch_unrecorded(
            &fixture.jj,
            &fixture.store,
            "foreign-owner",
            Some(&amended.id().hex()),
            &installed.head,
            Some(&claims),
        )
        .unwrap_err();
        assert!(matches!(
            refused,
            LeasedPushError::RemoteRewriteUnproven { .. }
        ));
        assert_eq!(
            git_stdout(origin.path(), &["rev-parse", "refs/heads/foreign-owner"]),
            amended.id().hex()
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn branch_only_squash_export_failure_cannot_rollback_over_a_new_owner_commit() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        // First make the source descend from the exact current base.
        let candidate = compute_branch_advance(
            &fixture.jj,
            fixture.snapshot(),
            None,
            Duration::from_secs(60),
        )
        .unwrap();
        let prepared = install_branch_advance_exported(&fixture.jj, candidate).unwrap();
        prepared.export.unwrap();
        let source = prepared.head;
        let base = bookmark_commit(&fixture.jj, &fixture.store, "main").unwrap();
        let store = fixture.store.clone();
        let owner_head = std::rc::Rc::new(std::cell::RefCell::new(None));
        let recorded = owner_head.clone();
        at_step("rebase: before export", move || {
            let head = futures::executor::block_on(async {
                let repo = crate::load_repo_at_head(&store).await.unwrap();
                let parent = repo
                    .store()
                    .get_commit(&normal_target(&repo, "feature").unwrap())
                    .unwrap();
                let mut tx = repo.start_transaction();
                let own = fixture_commit(
                    tx.repo_mut(),
                    &parent,
                    "owner-new.txt",
                    "sealed during export\n",
                )
                .await;
                tx.repo_mut().add_head(&own).await.unwrap();
                tx.repo_mut().set_local_bookmark_target(
                    RefName::new("feature"),
                    RefTarget::normal(own.id().clone()),
                );
                tx.commit("owner seals after squash acceptance")
                    .await
                    .unwrap();
                own.id().hex()
            });
            *recorded.borrow_mut() = Some(head);
        });
        let error = squash_branch_onto(
            &fixture.jj,
            &fixture.store,
            "feature",
            &source,
            &base,
            "landing",
        )
        .unwrap_err();
        assert!(error.to_string().contains("rollback incomplete"), "{error}");
        let own = owner_head.borrow().clone().unwrap();
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap(),
            own
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{own}:owner-new.txt")]
            ),
            "sealed during export"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_conflict_wholly_inside_regenerable_paths_takes_the_branch_side() {
        let Some(bin) = jj_bin() else { return };
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let write = |path: &str, text: &str| {
            let path = project.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write("gen/out", "derived from nothing\n");
        write("src/shared", "shared\n");
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "ancestor"]);
        git(project.path(), &["checkout", "-qb", "feature"]);
        write("src/b", "b\n");
        write("gen/out", "derived from b\n");
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "owner regenerates"]);
        git(project.path(), &["checkout", "-qb", "feature-shared"]);
        write("src/shared", "owner's shared\n");
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "owner edits shared"]);
        git(project.path(), &["checkout", "-q", "main"]);
        write("src/a", "a\n");
        write("gen/out", "derived from a\n");
        write("src/shared", "main's shared\n");
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "incoming regenerates"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("store");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        let regenerable = || RegenerablePaths::new(|path| path.starts_with("gen/"));
        let capture = |branch: &str| {
            capture_branch_advance(&store, branch, "main")
                .unwrap()
                .with_regenerable_paths(regenerable())
        };
        let show = |commit: &str, path: &str| {
            git_stdout(project.path(), &["show", &format!("{commit}:{path}")])
        };
        let before = operation_id(&jj, &store).unwrap();

        // Undeclared, the same replay is an ordinary conflict.
        let undeclared = capture_branch_advance(&store, "feature", "main").unwrap();
        let Err(AdvanceComputeError::Conflict(diagnostic)) =
            compute_branch_advance(&jj, undeclared, None, Duration::from_secs(30))
        else {
            panic!("an undeclared regenerable conflict is a conflict")
        };
        assert_eq!(diagnostic.conflicting_paths(), ["gen/out"]);

        let candidate =
            compute_branch_advance(&jj, capture("feature"), None, Duration::from_secs(30))
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(show(&candidate.head(), "gen/out"), "derived from b");
        assert_eq!(show(&candidate.head(), "src/a"), "a");
        assert_eq!(show(&candidate.head(), "src/b"), "b");
        let [stale] = candidate.stale_regenerable() else {
            panic!("one stale path: {:?}", candidate.stale_regenerable())
        };
        assert_eq!(stale.path, "gen/out");
        assert_eq!(
            stale.blob.as_deref(),
            Some(git_stdout(project.path(), &["rev-parse", "feature:gen/out"]).as_str())
        );

        let merged =
            compute_branch_merge_advance(&jj, capture("feature"), None, Duration::from_secs(30))
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(show(&merged.head(), "gen/out"), "derived from b");
        assert_eq!(merged.stale_regenerable().len(), 1);
        let message = git_stdout(
            project.path(),
            &["log", "-1", "--format=%B", &merged.head()],
        );
        assert!(
            message.ends_with(&format!("{STALE_REGENERATION_TRAILER} gen/out")),
            "{message}"
        );

        // One conflict outside the declaration leaves the whole conflict.
        for compute in [compute_branch_advance, compute_branch_merge_advance] {
            let Err(AdvanceComputeError::Conflict(diagnostic)) = compute(
                &jj,
                capture("feature-shared"),
                None,
                Duration::from_secs(30),
            ) else {
                panic!("a conflict outside the globs stands")
            };
            let mut paths = diagnostic.conflicting_paths();
            paths.sort();
            assert_eq!(paths, ["gen/out", "src/shared"]);
        }
        assert_eq!(operation_id(&jj, &store).unwrap(), before);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn declared_registry_merge_is_detached_and_coordinate_bound() {
        let Some(bin) = jj_bin() else { return };
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let original = "pub const ITEMS: &[&str] = &[\n    \"base\",\n];\n";
        let ours = "pub const ITEMS: &[&str] = &[\n    \"base\",\n    \"owner\",\n];\n";
        let theirs = "pub const ITEMS: &[&str] = &[\n    \"base\",\n    \"incoming\",\n];\n";
        let combined =
            "pub const ITEMS: &[&str] = &[\n    \"base\",\n    \"owner\",\n    \"incoming\",\n];\n";
        std::fs::write(project.path().join("registry.rs"), original).unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "registry ancestor"]);
        git(project.path(), &["checkout", "-qb", "feature"]);
        std::fs::write(project.path().join("registry.rs"), ours).unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "owner registry"]);
        git(project.path(), &["checkout", "-q", "main"]);
        std::fs::write(project.path().join("registry.rs"), theirs).unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-qm", "incoming registry"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("store");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        let capture = || capture_branch_advance(&store, "feature", "main").unwrap();
        let snapshot = capture();
        let coordinate = snapshot.coordinate().clone();
        let before = operation_id(&jj, &store).unwrap();
        let outcome = compute_branch_advance(&jj, snapshot, None, Duration::from_secs(30));
        let Err(AdvanceComputeError::Conflict(diagnostic)) = outcome else {
            panic!("expected list conflict")
        };
        assert_eq!(diagnostic.conflicting_paths(), ["registry.rs"]);
        let plan = DeclaredMergeEdits {
            expected_head: coordinate.head.clone(),
            expected_base: coordinate.base.clone(),
            edits: vec![("registry.rs".into(), combined.into())],
        };
        let candidate = compute_branch_advance_with_declared(
            &jj,
            capture(),
            None,
            Some(&plan),
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(operation_id(&jj, &store).unwrap(), before);
        assert_eq!(
            bookmark_commit(&jj, &store, "feature").unwrap(),
            coordinate.head
        );
        assert_eq!(
            git_stdout(
                project.path(),
                &["show", &format!("{}:registry.rs", candidate.head())]
            ),
            combined.trim_end()
        );
        let mut stale = plan.clone();
        stale.expected_base = "0".repeat(coordinate.base.len());
        assert!(matches!(
            compute_branch_advance_with_declared(
                &jj,
                capture(),
                None,
                Some(&stale),
                Duration::from_secs(30)
            ),
            Err(AdvanceComputeError::CoordinatesChanged)
        ));
        assert_eq!(operation_id(&jj, &store).unwrap(), before);
        let mut malformed = plan;
        malformed.edits[0].1 = "pub const ITEMS: &[&str] = &[".into();
        assert!(compute_branch_advance_with_declared(
            &jj,
            capture(),
            None,
            Some(&malformed),
            Duration::from_secs(30)
        )
        .is_err());
        assert_eq!(operation_id(&jj, &store).unwrap(), before);
        let installed = install_branch_advance_exported(&jj, candidate).unwrap();
        installed.export.unwrap();
        assert_eq!(
            git_stdout(project.path(), &["show", "feature:registry.rs"]),
            combined.trim_end()
        );
        assert_eq!(
            bookmark_commit(&jj, &store, "main").unwrap(),
            coordinate.base
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn prepared_detection_and_operation_repair_are_explicit() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let snapshot = fixture.snapshot();
        let malformed = snapshot.coordinate.head[..12].to_string();
        assert!(matches!(
            snapshot.reachable_from_head(std::slice::from_ref(&malformed), Duration::from_secs(10)),
            Err(AdvanceComputeError::UnknownEvidence { commit }) if commit == malformed
        ));
        assert!(snapshot
            .reachable_from_head(&["a".repeat(40)], Duration::from_secs(10))
            .unwrap()
            .is_empty());
        let queries = vec![
            snapshot.coordinate.head.clone(),
            snapshot.coordinate.base.clone(),
            "a".repeat(40),
        ];
        assert_eq!(
            snapshot
                .reachable_from_head(&queries, Duration::from_secs(10))
                .unwrap(),
            std::slice::from_ref(&snapshot.coordinate.head)
        );
        assert!(matches!(
            snapshot.reachable_from_head(&queries, Duration::ZERO),
            Err(AdvanceComputeError::TimedOut)
        ));
        assert!(
            matches!(
                snapshot.already_prepared(Duration::ZERO),
                Err(AdvanceComputeError::TimedOut)
            ),
            "an expired budget is a timeout even when the base is not an ancestor"
        );
        assert_eq!(
            snapshot.already_prepared(Duration::from_secs(10)).unwrap(),
            None
        );
        fixture.run(&[
            "bookmark",
            "set",
            "feature",
            "-r",
            "main",
            "--allow-backwards",
        ]);
        let snapshot = fixture.snapshot();
        let before = operation_id(&fixture.jj, &fixture.store).unwrap();
        assert!(
            matches!(
                snapshot.already_prepared(Duration::ZERO),
                Err(AdvanceComputeError::TimedOut)
            ),
            "an expired budget is a timeout, not an answer"
        );
        assert!(snapshot
            .already_prepared(Duration::from_secs(10))
            .unwrap()
            .is_some());
        assert_eq!(operation_id(&fixture.jj, &fixture.store).unwrap(), before);
        block_on(async {
            for branch in ["one", "two"] {
                let mut transaction = snapshot.repo.start_transaction();
                transaction.repo_mut().set_local_bookmark_target(
                    RefName::new(branch),
                    jj_lib::op_store::RefTarget::normal(
                        CommitId::try_from_hex(&snapshot.coordinate.head).unwrap(),
                    ),
                );
                transaction
                    .commit("divergent snapshot fixture")
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        })
        .unwrap();
        let error = match capture_branch_advance(&fixture.store, "feature", "main") {
            Ok(_) => panic!("literal capture must refuse split operations"),
            Err(error) => error,
        };
        assert!(
            matches!(error, AdvanceCaptureError::UnsettledOperations { heads: 2 }),
            "{error}"
        );
        let settled = settle_operation_heads(&fixture.jj, &fixture.store).unwrap();
        assert_eq!(settled.before.len(), 2);
        let again = settle_operation_heads(&fixture.jj, &fixture.store).unwrap();
        assert_eq!(
            again.before.as_slice(),
            std::slice::from_ref(&settled.after)
        );
        assert_eq!(again.after, settled.after);
        let snapshot = fixture.snapshot();
        assert!(normal_target(&snapshot.repo, "one").is_ok());
        assert!(normal_target(&snapshot.repo, "two").is_ok());
    }

    #[test]
    #[serial_test::serial(jj)]
    fn replay_names_newly_empty_commits_but_retains_intentional_empty_commits() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let first = git_stdout(fixture.project.path(), &["rev-parse", "feature^"]);
        git(fixture.project.path(), &["cherry-pick", &first]);
        import_git(&fixture.jj, &fixture.store).unwrap();
        fixture.run(&["new", "feature", "--no-edit", "-m", "intentional empty"]);
        let empty = fixture.run(&[
            "log",
            "-r",
            "latest(children(feature))",
            "--no-graph",
            "-T",
            "commit_id",
        ]);
        fixture.run(&["bookmark", "set", "feature", "-r", &empty]);
        let snapshot = fixture.snapshot();
        let empty_commit = snapshot
            .repo
            .store()
            .get_commit(&CommitId::try_from_hex(&empty).unwrap())
            .unwrap();
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(30)).unwrap();
        assert_eq!(candidate.head.change_id(), empty_commit.change_id());
        assert_eq!(candidate.dropped_commit_ids(), std::slice::from_ref(&first));
        assert!(!candidate.dropped_commit_ids().contains(&empty));
        assert_eq!(
            candidate.healing.dropped,
            [super::super::emptied::dropped_label(&first, "owner one")]
        );
        assert_eq!(
            candidate.head.tree_ids(),
            candidate
                .head
                .store()
                .get_commit(&candidate.head.parent_ids()[0])
                .unwrap()
                .tree_ids()
        );
        let installed = install_branch_advance_exported(&fixture.jj, candidate).unwrap();
        installed.export.unwrap();
        assert_eq!(installed.healing.dropped.len(), 1);
        assert_eq!(installed.dropped_commit_ids, [first]);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn an_older_same_change_base_does_not_discard_an_owner_amendment() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let snapshot = fixture.snapshot();
        let old_tip = snapshot
            .repo
            .store()
            .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head).unwrap())
            .unwrap();
        let older = snapshot
            .repo
            .store()
            .get_commit(&old_tip.parent_ids()[0])
            .unwrap();
        block_on(async {
            let mut tx = snapshot.repo.start_transaction();
            let path = RepoPathBuf::from_internal_string("amendment.txt").unwrap();
            let mut reader = futures::io::Cursor::new(b"owner amendment\n".as_slice());
            let id = tx
                .repo()
                .store()
                .write_file(&path, &mut reader)
                .await
                .unwrap();
            let mut tree = MergedTreeBuilder::new(older.tree());
            tree.set_or_remove(
                path,
                Merge::resolved(Some(TreeValue::File {
                    id,
                    executable: false,
                    copy_id: CopyId::placeholder(),
                })),
            );
            let tree = tree.write_tree().await.unwrap();
            let amended = tx
                .repo_mut()
                .rewrite_commit(&older)
                .set_predecessors(
                    rewrite_predecessors(
                        &older,
                        &Deadline(Instant::now() + Duration::from_secs(60)),
                    )
                    .unwrap(),
                )
                .set_tree(tree)
                .detach()
                .write_hidden()
                .await
                .unwrap();
            let child = tx
                .repo_mut()
                .new_commit(vec![amended.id().clone()], amended.tree())
                .set_description("owner's intentional empty child")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            tx.repo_mut().add_head(&child).await.unwrap();
            tx.repo_mut().set_local_bookmark_target(
                RefName::new("main"),
                jj_lib::op_store::RefTarget::normal(older.id().clone()),
            );
            tx.repo_mut().set_local_bookmark_target(
                RefName::new("feature"),
                jj_lib::op_store::RefTarget::normal(child.id().clone()),
            );
            tx.commit("older base with owner amendment").await.unwrap();
            Ok(())
        })
        .unwrap();
        let snapshot = fixture.snapshot();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let head = snapshot
                    .repo
                    .store()
                    .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head).unwrap())
                    .unwrap();
                let commits = branch_commits(
                    snapshot.repo.as_ref(),
                    head.id(),
                    older.id(),
                    &Deadline(Instant::now() + Duration::from_secs(30)),
                    None,
                )
                .await
                .unwrap();
                let (_, mapping) = replay_boundaries(
                    &snapshot,
                    &commits,
                    &head,
                    &older,
                    &Deadline(Instant::now() + Duration::from_secs(30)),
                )
                .await
                .unwrap();
                assert!(
                    mapping.is_empty(),
                    "an old version cannot prove a newer amendment was incorporated"
                );
            });
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(30)).unwrap();
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:amendment.txt", candidate.head())]
            ),
            "owner amendment"
        );
        install_branch_advance_exported(&fixture.jj, candidate)
            .unwrap()
            .export
            .unwrap();
    }

    #[test]
    #[serial_test::serial(jj)]
    fn missing_predecessor_evidence_replays_instead_of_skipping() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let source = fixture.snapshot();
        let original = source
            .repo
            .store()
            .get_commit(&CommitId::try_from_hex(&source.coordinate.head).unwrap())
            .unwrap();
        fixture.run(&["bookmark", "set", "old-feature", "-r", &original.id().hex()]);
        let mut candidate = compute_branch_advance(
            &fixture.jj,
            fixture.snapshot(),
            None,
            Duration::from_secs(30),
        )
        .unwrap();
        candidate.head = block_on(async {
            let mut tx = candidate.snapshot.repo.start_transaction();
            tx.repo_mut()
                .rewrite_commit(&candidate.head)
                .set_predecessors(Vec::new())
                .set_description(candidate.head.description())
                .detach()
                .write_hidden()
                .await
                .map_err(|error| error.to_string())
        })
        .unwrap();
        install_branch_advance_exported(&fixture.jj, candidate)
            .unwrap()
            .export
            .unwrap();
        let snapshot = capture_branch_advance(&fixture.store, "old-feature", "feature").unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let base = snapshot
                    .repo
                    .store()
                    .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.base).unwrap())
                    .unwrap();
                let deadline = Deadline(Instant::now() + Duration::from_secs(30));
                let commits = branch_commits(
                    snapshot.repo.as_ref(),
                    original.id(),
                    base.id(),
                    &deadline,
                    None,
                )
                .await
                .unwrap();
                let (_, mapping) =
                    replay_boundaries(&snapshot, &commits, &original, &base, &deadline)
                        .await
                        .unwrap();
                assert!(!mapping.contains_key(original.id()));
            });
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(30)).unwrap();
        assert!(
            candidate
                .healing
                .dropped
                .iter()
                .any(|label| label.contains("owner two")),
            "missing proof executes replay and canonical empty-drop, not a provenance skip"
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:owner.txt", candidate.head())]
            ),
            "owner two"
        );
        install_branch_advance_exported(&fixture.jj, candidate)
            .unwrap()
            .export
            .unwrap();
    }

    #[test]
    #[serial_test::serial(jj)]
    fn flattened_hidden_predecessors_are_capped_and_keep_newest_versions() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let snapshot = fixture.snapshot();
        block_on(async {
            let mut tx = snapshot.repo.start_transaction();
            let original = snapshot
                .repo
                .store()
                .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head).unwrap())
                .unwrap();
            let mut current = original.clone();
            let mut previous = current.id().clone();
            for number in 0..70 {
                previous = current.id().clone();
                current = tx
                    .repo_mut()
                    .rewrite_commit(&current)
                    .set_predecessors(
                        rewrite_predecessors(
                            &current,
                            &Deadline(Instant::now() + Duration::from_secs(60)),
                        )
                        .unwrap(),
                    )
                    .set_description(format!("hidden version {number}"))
                    .detach()
                    .write_hidden()
                    .await
                    .unwrap();
            }
            assert_eq!(current.store_commit().predecessors.len(), 64);
            assert_eq!(current.store_commit().predecessors[0], previous);
            assert!(
                !current.store_commit().predecessors.contains(original.id()),
                "expired evidence falls back to replay instead of growing forever"
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial(jj)]
    fn transitive_dependents_do_not_pin_the_parents_merged_lineage() {
        let Some(fixture) = ReplayFixture::new(true) else {
            return;
        };
        let old = bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap();
        fixture.run(&["rebase", "-b", "feature", "-o", "main"]);
        fixture.run(&[
            "restore",
            "--from",
            &old,
            "--into",
            "feature",
            "--",
            "common.txt",
        ]);
        let snapshot = fixture.snapshot();
        let (child, grandchild) = block_on(async {
            let head = snapshot
                .repo
                .store()
                .get_commit(&CommitId::try_from_hex(&snapshot.coordinate.head).unwrap())
                .unwrap();
            let fork = snapshot
                .repo
                .store()
                .get_commit(&head.parent_ids()[0])
                .unwrap();
            let mut tx = snapshot.repo.start_transaction();
            let child = tx
                .repo_mut()
                .new_commit(vec![fork.id().clone()], fork.tree())
                .set_description("child forked before parent tip")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            let grandchild = tx
                .repo_mut()
                .new_commit(vec![child.id().clone()], child.tree())
                .set_description("grandchild forked from child")
                .detach()
                .write_hidden()
                .await
                .unwrap();
            tx.repo_mut().add_head(&grandchild).await.unwrap();
            tx.repo_mut().set_local_bookmark_target(
                RefName::new("child"),
                jj_lib::op_store::RefTarget::normal(child.id().clone()),
            );
            tx.repo_mut().set_local_bookmark_target(
                RefName::new("grandchild"),
                jj_lib::op_store::RefTarget::normal(grandchild.id().clone()),
            );
            tx.commit("declare dependent fixture heads").await.unwrap();
            Ok((child.id().hex(), grandchild.id().hex()))
        })
        .unwrap();
        fixture.run(&[
            "new",
            "feature",
            "main",
            "--no-edit",
            "-m",
            "owner merges its base",
        ]);
        let merged = fixture.run(&[
            "log",
            "-r",
            "latest(children(feature) & children(main))",
            "--no-graph",
            "-T",
            "commit_id",
        ]);
        fixture.run(&["bookmark", "set", "feature", "-r", &merged]);
        std::fs::write(
            fixture.project.path().join("later.txt"),
            "later base work\n",
        )
        .unwrap();
        git(fixture.project.path(), &["add", "-A"]);
        git(
            fixture.project.path(),
            &["commit", "-qm", "base advances again"],
        );
        import_git(&fixture.jj, &fixture.store).unwrap();
        let snapshot = fixture
            .snapshot()
            .with_dependent_bookmarks(vec!["child".into(), "grandchild".into()]);
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(30)).unwrap();
        assert!(
            !candidate.healing.healed.is_empty(),
            "the owner's conflicted lineage must be healed, not pinned"
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:common.txt", candidate.head())]
            ),
            "owner two"
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:later.txt", candidate.head())]
            ),
            "later base work"
        );
        let installed = install_branch_advance_exported(&fixture.jj, candidate).unwrap();
        installed.export.unwrap();
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "child").unwrap(),
            child
        );
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "grandchild").unwrap(),
            grandchild
        );
        assert!(conflicted_commits(&fixture.jj, &fixture.store, "main..feature").is_empty());
    }

    struct ReplayFixture {
        _home: TempDir,
        project: TempDir,
        jj: JjEnv,
        store: PathBuf,
    }

    impl ReplayFixture {
        fn new(conflict: bool) -> Option<Self> {
            let bin = jj_bin()?;
            let home = TempDir::new().unwrap();
            let project = TempDir::new().unwrap();
            init_project(project.path());
            std::fs::write(project.path().join("common.txt"), "original\n").unwrap();
            git(project.path(), &["add", "-A"]);
            git(project.path(), &["commit", "-qm", "common ancestor"]);
            git(project.path(), &["checkout", "-qb", "feature"]);
            let own_path = if conflict { "common.txt" } else { "owner.txt" };
            for (content, message) in [("owner one\n", "owner one"), ("owner two\n", "owner two")] {
                std::fs::write(project.path().join(own_path), content).unwrap();
                git(project.path(), &["add", "-A"]);
                git(project.path(), &["commit", "-qm", message]);
            }
            git(project.path(), &["checkout", "-q", "main"]);
            let base_path = if conflict { "common.txt" } else { "base.txt" };
            std::fs::write(project.path().join(base_path), "incoming\n").unwrap();
            git(project.path(), &["add", "-A"]);
            git(project.path(), &["commit", "-qm", "advanced base"]);
            let jj = JjEnv::resolve(&bin, home.path());
            let store = home.path().join("store");
            ensure_project_store(&jj, &store, project.path()).unwrap();
            Some(Self {
                _home: home,
                project,
                jj,
                store,
            })
        }

        fn run(&self, args: &[&str]) -> String {
            let mut args = args.to_vec();
            args.push("--ignore-working-copy");
            self.jj
                .run(&self.store, &args, "detached replay fixture")
                .unwrap()
        }

        fn snapshot(&self) -> BranchAdvanceSnapshot {
            capture_branch_advance(&self.store, "feature", "main").unwrap()
        }
    }

    #[test]
    #[serial_test::serial(jj)]
    fn detached_replay_preserves_stack_and_never_publishes_during_compute() {
        let Some(fixture) = ReplayFixture::new(false) else {
            return;
        };
        let snapshot = fixture.snapshot();
        let head_before = snapshot.coordinate.head.clone();
        let base = snapshot.coordinate.base.clone();
        let old_tip = snapshot
            .repo
            .store()
            .get_commit(&CommitId::try_from_hex(&head_before).unwrap())
            .unwrap();
        let old_parent = snapshot
            .repo
            .store()
            .get_commit(&old_tip.parent_ids()[0])
            .unwrap();
        let before = operation_id(&fixture.jj, &fixture.store).unwrap();
        let candidate =
            compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(60))
                .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(operation_id(&fixture.jj, &fixture.store).unwrap(), before);
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap(),
            head_before
        );
        assert_eq!(
            git_stdout(fixture.project.path(), &["rev-parse", "feature"]),
            head_before
        );
        assert_eq!(candidate.head.change_id(), old_tip.change_id());
        // Exercise Cairn's real settings loader, not jj-lib's test defaults.
        assert!(candidate
            .head
            .store_commit()
            .predecessors
            .contains(old_tip.id()));
        let mut transaction = candidate.snapshot.repo.start_transaction();
        let twice = block_on(async {
            transaction
                .repo_mut()
                .rewrite_commit(&candidate.head)
                .set_predecessors(
                    rewrite_predecessors(
                        &candidate.head,
                        &Deadline(Instant::now() + Duration::from_secs(60)),
                    )
                    .unwrap(),
                )
                .set_description("second hidden version")
                .detach()
                .write_hidden()
                .await
                .map_err(|error| error.to_string())
        })
        .unwrap();
        assert!(twice.store_commit().predecessors.contains(old_tip.id()));
        assert!(twice
            .store_commit()
            .predecessors
            .contains(candidate.head.id()));
        assert!(twice.store_commit().predecessors.len() <= 64);
        let parent = candidate
            .head
            .store()
            .get_commit(&candidate.head.parent_ids()[0])
            .unwrap();
        assert_eq!(parent.change_id(), old_parent.change_id());
        assert_eq!(
            parent.parent_ids(),
            &[CommitId::try_from_hex(&base).unwrap()]
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:owner.txt", candidate.head())]
            ),
            "owner two"
        );
        assert_eq!(
            git_stdout(
                fixture.project.path(),
                &["show", &format!("{}:base.txt", candidate.head())]
            ),
            "incoming"
        );
        let local = install_branch_advance_local(&fixture.jj, candidate).unwrap();
        assert_eq!(
            operation_id(&fixture.jj, &fixture.store).unwrap(),
            local.accepted_operation.clone().unwrap()
        );
        assert_ne!(local.accepted_operation.as_deref(), Some(before.as_str()));
        assert_eq!(
            git_stdout(fixture.project.path(), &["rev-parse", "feature"]),
            head_before,
            "local acceptance must not export before the durable witness is recorded"
        );
        assert!(super::super::object_lease::Reclamation::for_store(&fixture.store).is_err());
        let installed = export_installed_branch_advance(&fixture.jj, local);
        installed.export.unwrap();
        assert_ne!(installed.head, head_before);
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "main").unwrap(),
            base
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn detached_conflict_and_timeout_leave_live_views_unchanged() {
        let Some(fixture) = ReplayFixture::new(true) else {
            return;
        };
        let before = operation_id(&fixture.jj, &fixture.store).unwrap();
        let snapshot = fixture.snapshot();
        let coordinate = snapshot.coordinate().clone();
        let outcome = compute_branch_advance(&fixture.jj, snapshot, None, Duration::from_secs(60));
        let Err(AdvanceComputeError::Conflict(diagnostic)) = outcome else {
            panic!("expected exact conflict refusal")
        };
        assert_eq!(diagnostic.ours.as_deref(), Some(coordinate.head.as_str()));
        assert_eq!(diagnostic.theirs.as_deref(), Some(coordinate.base.as_str()));
        assert_eq!(diagnostic.conflicting_paths(), vec!["common.txt"]);
        assert_eq!(operation_id(&fixture.jj, &fixture.store).unwrap(), before);
        assert_eq!(
            bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap(),
            coordinate.head
        );
        assert_eq!(
            git_stdout(fixture.project.path(), &["rev-parse", "feature"]),
            coordinate.head
        );
        assert!(matches!(
            compute_branch_advance(&fixture.jj, fixture.snapshot(), None, Duration::ZERO),
            Err(AdvanceComputeError::TimedOut)
        ));
        assert_eq!(operation_id(&fixture.jj, &fixture.store).unwrap(), before);
    }

    /// A branch of `prefix` commits touching only their own files, then `hot`
    /// commits that each rewrite one of `hot_paths` paths the base also changes
    /// AND a file of their own, then `tail` commits touching only their own
    /// files. Every commit keeps a change of its own, so a healed commit is never
    /// emptied away and retained history is paid for in full.
    ///
    /// The store holds the branch rebased onto `prior` (the base that changed the
    /// hot paths) with the original tip's resolution restored into the last hot
    /// commit: `hot - 1` conflict-flagged intermediate commits under a clean tip,
    /// which is how an owner's resolved branch is left. `main` is one unrelated
    /// commit past `prior`, so a replay onto `main` is a divergent-base replay
    /// that rebases every commit and then heals.
    struct HealFixture {
        _home: TempDir,
        project: TempDir,
        jj: JjEnv,
        store: PathBuf,
        total: usize,
        flagged: usize,
        hot_paths: usize,
    }

    impl HealFixture {
        fn new(prefix: usize, hot: usize, tail: usize, hot_paths: usize) -> Option<Self> {
            assert!(hot >= 2 && hot_paths >= 1);
            let bin = jj_bin()?;
            let home = TempDir::new().unwrap();
            let project = TempDir::new().unwrap();
            let root = project.path();
            init_project(root);
            let hot_path = |index: usize| format!("hot{}.txt", index % hot_paths);
            for path in 0..hot_paths {
                std::fs::write(root.join(hot_path(path)), "original\n").unwrap();
            }
            git(root, &["add", "-A"]);
            git(root, &["commit", "-qm", "common ancestor"]);
            git(root, &["checkout", "-qb", "feature"]);
            let total = prefix + hot + tail;
            for index in 0..total {
                let hot_index = index.checked_sub(prefix).filter(|at| *at < hot);
                if let Some(at) = hot_index {
                    std::fs::write(root.join(hot_path(at)), format!("owner {at}\n")).unwrap();
                }
                std::fs::write(
                    root.join(format!("own{index}.txt")),
                    format!("own {index}\n"),
                )
                .unwrap();
                git(root, &["add", "-A"]);
                git(root, &["commit", "-qm", &format!("feature {index}")]);
            }
            git(root, &["checkout", "-q", "main"]);
            for path in 0..hot_paths {
                std::fs::write(root.join(hot_path(path)), "incoming\n").unwrap();
            }
            git(root, &["add", "-A"]);
            git(root, &["commit", "-qm", "advanced base"]);
            git(root, &["branch", "prior"]);
            std::fs::write(root.join("later.txt"), "later\n").unwrap();
            git(root, &["add", "-A"]);
            git(root, &["commit", "-qm", "unrelated later base"]);
            let jj = JjEnv::resolve(&bin, home.path());
            let store = home.path().join("store");
            ensure_project_store(&jj, &store, root).unwrap();
            let fixture = Self {
                _home: home,
                project,
                jj,
                store,
                total,
                flagged: hot - 1,
                hot_paths,
            };
            let original = bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap();
            fixture.run(&["rebase", "-b", "feature", "-o", "prior"]);
            // jj's revset parent operator, never Git's `~N`.
            let last_hot = format!("feature{}", "-".repeat(tail));
            let mut restore = vec![
                "restore".to_string(),
                "--from".to_string(),
                original,
                "--into".to_string(),
                last_hot,
                "--".to_string(),
            ];
            restore.extend((0..hot_paths).map(hot_path));
            fixture.run(&restore.iter().map(String::as_str).collect::<Vec<_>>());
            assert_eq!(
                conflicted_commits(&fixture.jj, &fixture.store, "prior..feature").len(),
                fixture.flagged,
                "the fixture leaves exactly the intermediate hot commits conflict-flagged"
            );
            Some(fixture)
        }

        fn run(&self, args: &[&str]) -> String {
            let mut args = args.to_vec();
            args.push("--ignore-working-copy");
            self.jj.run(&self.store, &args, "heal fixture").unwrap()
        }

        fn commands_built() -> usize {
            super::super::env::COMMANDS_BUILT.with(std::cell::Cell::get)
        }

        /// The heal alone on the already-rebased branch, as the engine runs it,
        /// asserting the heal contract. (milliseconds, jj commands, observation)
        fn direct_heal(&self) -> (f64, usize, ReplayCostObservation) {
            let snapshot = capture_branch_advance(&self.store, "feature", "prior").unwrap();
            let repo = snapshot.repo.clone();
            let commit = |hex: &str| {
                repo.store()
                    .get_commit(&CommitId::try_from_hex(hex).unwrap())
                    .unwrap()
            };
            let head = commit(&snapshot.coordinate.head);
            let base = commit(&snapshot.coordinate.base);
            let deadline = Deadline(Instant::now() + Duration::from_secs(60));
            let before = futures::executor::block_on(branch_commits(
                repo.as_ref(),
                head.id(),
                base.id(),
                &deadline,
                None,
            ))
            .unwrap();
            let clean_prefix: Vec<_> = before
                .iter()
                .take_while(|commit| !commit.has_conflict())
                .map(|commit| commit.id().clone())
                .collect();
            let mut transaction = repo.start_transaction();
            let probe = ReplayCostProbe::default();
            let commands = Self::commands_built();
            let started = Instant::now();
            let (healed, report) = futures::executor::block_on(heal_branch(
                transaction.repo_mut(),
                &head,
                &base,
                &HashSet::new(),
                &mut HashSet::new(),
                &deadline,
                Some(&probe),
            ))
            .unwrap();
            let millis = started.elapsed().as_secs_f64() * 1000.0;
            let commands = Self::commands_built() - commands;
            assert_eq!(healed.tree_ids(), head.tree_ids(), "the tip tree is exact");
            let after = futures::executor::block_on(branch_commits(
                transaction.repo(),
                healed.id(),
                base.id(),
                &deadline,
                None,
            ))
            .unwrap();
            assert!(after.iter().all(|commit| !commit.has_conflict()));
            assert_eq!(
                after.len(),
                self.total,
                "no commit with its own change is dropped"
            );
            assert_eq!(
                after[..clean_prefix.len()]
                    .iter()
                    .map(|commit| commit.id().clone())
                    .collect::<Vec<_>>(),
                clean_prefix,
                "the clean prefix keeps its commit ids"
            );
            // A commit whose conflict was only inherited is cured by rewriting
            // it onto its healed parent and needs no restore of its own.
            assert!(
                (1..=self.flagged).contains(&report.healed.len()),
                "{} healed of {} flagged",
                report.healed.len(),
                self.flagged
            );
            assert!(report.dropped.is_empty(), "{:?}", report.dropped);
            (millis, commands, probe.observation())
        }

        /// A divergent-base replay onto `main` under `budget`, from a fresh
        /// capture: (milliseconds, jj commands, observation, candidate).
        fn divergent_compute(
            &self,
            budget: Duration,
        ) -> (
            f64,
            usize,
            ReplayCostObservation,
            Result<BranchAdvanceCandidate, AdvanceComputeError>,
        ) {
            let probe = ReplayCostProbe::default();
            let snapshot = capture_branch_advance(&self.store, "feature", "main")
                .unwrap()
                .with_cost_probe(probe.clone());
            let commands = Self::commands_built();
            let started = Instant::now();
            let candidate = compute_branch_advance(&self.jj, snapshot, None, budget);
            let millis = started.elapsed().as_secs_f64() * 1000.0;
            (
                millis,
                Self::commands_built() - commands,
                probe.observation(),
                candidate,
            )
        }

        /// Install and export `candidate`, asserting the published branch:
        /// (milliseconds, jj commands).
        fn install_and_verify(&self, candidate: BranchAdvanceCandidate) -> (f64, usize) {
            let commands = Self::commands_built();
            let started = Instant::now();
            let local = install_branch_advance_local(&self.jj, candidate).unwrap();
            let installed = export_installed_branch_advance(&self.jj, local);
            let millis = started.elapsed().as_secs_f64() * 1000.0;
            let commands = Self::commands_built() - commands;
            installed.export.unwrap();
            let root = self.project.path();
            assert_eq!(
                git_stdout(root, &["rev-parse", "feature"]),
                installed.head,
                "the exported Git ref is the installed head"
            );
            assert!(
                conflicted_commits(&self.jj, &self.store, "main..feature").is_empty(),
                "no conflict-flagged commit remains in the published ancestry"
            );
            assert_eq!(
                self.run(&[
                    "log",
                    "-r",
                    "main..feature & empty()",
                    "--no-graph",
                    "-T",
                    "commit_id"
                ])
                .trim(),
                "",
                "no retained commit was emptied"
            );
            for index in 0..self.total {
                let back = self.total - 1 - index;
                assert_eq!(
                    git_stdout(root, &["show", &format!("feature~{back}:own{index}.txt")]),
                    format!("own {index}"),
                    "commit {index} keeps its unrelated change"
                );
            }
            assert_eq!(git_stdout(root, &["show", "feature:later.txt"]), "later");
            (millis, commands)
        }
    }

    /// The native heal on a realistic shape: conflict-flagged commits that each
    /// keep a change of their own, a clean prefix, two hot paths, and a base
    /// that diverged again since. It never spawns a jj command per commit, keeps
    /// the exact tip tree and the clean prefix's ids, preserves every
    /// intermediate commit's unrelated change, reports each healed commit, and
    /// publishes a conflict-free ancestry whose Git ref is the installed head.
    #[test]
    #[serial_test::serial(jj)]
    fn native_heal_keeps_retained_history_exact_and_spawns_no_cli_per_commit() {
        let Some(fixture) = HealFixture::new(2, 6, 2, 2) else {
            return;
        };
        let (_, heal_commands, healed) = fixture.direct_heal();
        assert_eq!(heal_commands, 0, "the heal is native: no jj command at all");
        assert_eq!(healed.flagged_after_rebase, Some(5));
        assert_eq!(healed.affected_paths, Some(2));
        assert!((1..=5).contains(&healed.restored_commits), "{healed:?}");
        assert_eq!(healed.phase, Some(ComputePhase::Heal));

        let (_, compute_commands, observed, candidate) =
            fixture.divergent_compute(Duration::from_secs(60));
        let candidate = candidate.unwrap_or_else(|error| panic!("{error:?}"));
        assert!(
            compute_commands <= 2,
            "computation spawns a constant handful of jj commands, not one per commit: \
             {compute_commands}"
        );
        assert_eq!(observed.phase, Some(ComputePhase::Done));
        assert_eq!(observed.branch_commits, Some(10));
        assert_eq!(observed.flagged_in_history, Some(5));
        assert_eq!(observed.flagged_after_rebase, Some(5));
        assert_eq!(observed.affected_paths, Some(2));
        assert!(observed.rebase_ms.is_some() && observed.heal_ms.is_some());
        assert!((1..=5).contains(&candidate.healing.healed.len()));
        assert!(candidate.healing.dropped.is_empty());
        fixture.install_and_verify(candidate);
    }

    /// A computation that runs out of budget still reports where it was.
    #[test]
    #[serial_test::serial(jj)]
    fn an_exhausted_computation_reports_the_phase_it_reached() {
        let Some(fixture) = HealFixture::new(0, 3, 0, 1) else {
            return;
        };
        let (_, _, observed, outcome) = fixture.divergent_compute(Duration::ZERO);
        assert!(matches!(outcome, Err(AdvanceComputeError::TimedOut)));
        assert_eq!(
            observed.branch_commits, None,
            "nothing past entry was reached"
        );
        let live = bookmark_commit(&fixture.jj, &fixture.store, "feature").unwrap();
        assert_eq!(
            conflicted_commits(&fixture.jj, &fixture.store, "prior..feature").len(),
            2,
            "the live branch at {live} is untouched"
        );
    }

    /// Opt-in measurement of the native engine (cairn/6456). Set
    /// `CAIRN_REPLAY_BENCH=1` (and optionally `CAIRN_REPLAY_BENCH_SAMPLES`) and
    /// run `bun run test:rust -p cairn-vcs --lib benchmark_current_native_heal
    /// -- --nocapture`. Each sample builds a fresh fixture, so samples are
    /// independent; within a sample the order is heal alone, the divergent
    /// replay under a real 10 s deadline, then under a real 60 s deadline with
    /// install and verified export (the later phases run warm). Prints one
    /// `BENCH` line per phase; it fits nothing and asserts no threshold.
    #[test]
    #[serial_test::serial(jj)]
    fn benchmark_current_native_heal() {
        if std::env::var_os("CAIRN_REPLAY_BENCH").is_none() {
            return;
        }
        let samples: usize = std::env::var("CAIRN_REPLAY_BENCH_SAMPLES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3);
        let host = std::process::Command::new("hostname")
            .output()
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .unwrap_or_default();
        let load = || {
            let mut averages = [0f64; 3];
            // SAFETY: getloadavg writes at most the three doubles it is given.
            let read = unsafe { libc::getloadavg(averages.as_mut_ptr(), 3) };
            if read == 3 {
                format!("{:.2}", averages[0])
            } else {
                "unavailable".to_string()
            }
        };
        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        println!(
            "BENCH env host={host} profile={profile} engine={REPLAY_ENGINE_VERSION} samples={samples}"
        );
        // (prefix, hot, tail, hot paths): hot - 1 commits end conflict-flagged.
        let shapes = [
            (0, 17, 0, 1),
            (0, 51, 0, 1),
            (0, 101, 0, 1),
            (0, 15, 235, 1),
            (0, 51, 0, 4),
        ];
        for (prefix, hot, tail, paths) in shapes {
            for sample in 1..=samples {
                let built = Instant::now();
                let Some(fixture) = HealFixture::new(prefix, hot, tail, paths) else {
                    return;
                };
                let shape = format!(
                    "total={} flagged={} hot_paths={} sample={sample}",
                    fixture.total, fixture.flagged, fixture.hot_paths
                );
                println!(
                    "BENCH fixture {shape} build_ms={:.0} load1={}",
                    built.elapsed().as_secs_f64() * 1000.0,
                    load()
                );
                let (ms, jj, observed) = fixture.direct_heal();
                println!(
                    "BENCH heal {shape} order=1 ms={ms:.1} jj={jj} restored={} affected_paths={:?} \
                     load1={} tree_equal=true ancestry_clean=true",
                    observed.restored_commits,
                    observed.affected_paths,
                    load()
                );
                for (order, budget) in [(2, 10), (3, 60)] {
                    let (ms, jj, observed, outcome) =
                        fixture.divergent_compute(Duration::from_secs(budget));
                    let verdict = match &outcome {
                        Ok(_) => "completed".to_string(),
                        Err(AdvanceComputeError::TimedOut) => "timed_out".to_string(),
                        Err(other) => format!("refused:{other}"),
                    };
                    let mut line = format!(
                        "BENCH divergent {shape} order={order} budget_s={budget} outcome={verdict} \
                         compute_ms={ms:.1} jj={jj} phase={:?} rebase_ms={:?} heal_ms={:?} \
                         flagged_after_rebase={:?} restored={} load1={}",
                        observed.phase.map(ComputePhase::label),
                        observed.rebase_ms,
                        observed.heal_ms,
                        observed.flagged_after_rebase,
                        observed.restored_commits,
                        load()
                    );
                    if let (Ok(candidate), 60) = (outcome, budget) {
                        let (install_ms, install_jj) = fixture.install_and_verify(candidate);
                        line.push_str(&format!(
                            " install_export_ms={install_ms:.1} install_export_jj={install_jj} \
                             total_ms={:.1} exported=true ancestry_clean=true",
                            ms + install_ms
                        ));
                    }
                    // One write per line: nextest's progress notices must not
                    // split a sample.
                    println!("{line}");
                }
            }
        }
    }

    /// Construct a hidden object without publishing a transaction. Production
    /// candidates will additionally run replay, heal and publication guards.
    fn hidden_candidate(snapshot: BranchAdvanceSnapshot) -> BranchAdvanceCandidate {
        let head = block_on(async {
            let id = CommitId::try_from_hex(&snapshot.coordinate.head).unwrap();
            let old = snapshot.repo.store().get_commit(&id).unwrap();
            let mut transaction = snapshot.repo.start_transaction();
            transaction
                .repo_mut()
                .new_commit(vec![id], old.tree())
                .set_description("detached candidate fixture")
                .detach()
                .write_hidden()
                .await
                .map_err(|error| error.to_string())
        })
        .unwrap();
        BranchAdvanceCandidate {
            snapshot,
            head,
            healing: Default::default(),
            dropped_commit_ids: Vec::new(),
            stale_regenerable: Vec::new(),
            expires_at: Instant::now() + std::time::Duration::from_secs(60),
        }
    }

    #[test]
    #[serial_test::serial(jj)]
    fn installer_rejects_owner_and_base_races_and_preserves_other_branches() {
        let Some(bin) = jj_bin() else { return };
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("store");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        let run = |args: &[&str]| {
            let mut args = args.to_vec();
            args.push("--ignore-working-copy");
            jj.run(&store, &args, "advance installer fixture").unwrap()
        };
        run(&["bookmark", "set", "feature", "-r", "main"]);
        let before = operation_id(&jj, &store).unwrap();
        let candidate =
            hidden_candidate(capture_branch_advance(&store, "feature", "main").unwrap());
        assert_eq!(
            operation_id(&jj, &store).unwrap(),
            before,
            "hidden compute must not publish"
        );
        run(&["new", "main", "--no-edit", "-m", "owner commit"]);
        let owner = run(&[
            "log",
            "-r",
            "latest(children(main))",
            "--no-graph",
            "-T",
            "commit_id",
        ]);
        run(&["bookmark", "set", "feature", "-r", &owner]);
        assert!(matches!(
            install_branch_advance_exported(&jj, candidate),
            Err(AdvanceInstallError::CoordinatesChanged)
        ));
        assert_eq!(bookmark_commit(&jj, &store, "feature").unwrap(), owner);

        let candidate =
            hidden_candidate(capture_branch_advance(&store, "feature", "main").unwrap());
        run(&["bookmark", "set", "main", "-r", &owner]);
        assert!(matches!(
            install_branch_advance_exported(&jj, candidate),
            Err(AdvanceInstallError::CoordinatesChanged)
        ));
        assert_eq!(bookmark_commit(&jj, &store, "feature").unwrap(), owner);

        let candidate =
            hidden_candidate(capture_branch_advance(&store, "feature", "main").unwrap());
        let head = candidate.head();
        run(&["bookmark", "set", "other", "-r", &owner]);
        let installed = install_branch_advance_exported(&jj, candidate).unwrap();
        assert_eq!(installed.head, head);
        installed.export.unwrap();
        assert_eq!(bookmark_commit(&jj, &store, "feature").unwrap(), head);
        assert_eq!(bookmark_commit(&jj, &store, "other").unwrap(), owner);
        assert_eq!(bookmark_commit(&jj, &store, "main").unwrap(), owner);
        assert_eq!(
            git_stdout(project.path(), &["rev-parse", "refs/heads/feature"]),
            head
        );

        let mut invalid =
            hidden_candidate(capture_branch_advance(&store, "feature", "main").unwrap());
        let base = invalid
            .snapshot
            .repo
            .store()
            .get_commit(&CommitId::try_from_hex(&invalid.coordinate().base).unwrap())
            .unwrap();
        invalid.head = invalid
            .snapshot
            .repo
            .store()
            .get_commit(&base.parent_ids()[0])
            .unwrap();
        let before_invalid = operation_id(&jj, &store).unwrap();
        assert!(matches!(install_branch_advance_exported(&jj, invalid),
            Err(AdvanceInstallError::Repository(error)) if error.contains("does not include its captured base")));
        assert_eq!(operation_id(&jj, &store).unwrap(), before_invalid);

        let mut candidate =
            hidden_candidate(capture_branch_advance(&store, "feature", "main").unwrap());
        candidate.expires_at = Instant::now();
        let before = operation_id(&jj, &store).unwrap();
        assert!(matches!(
            install_branch_advance_exported(&jj, candidate),
            Err(AdvanceInstallError::Expired)
        ));
        assert_eq!(operation_id(&jj, &store).unwrap(), before);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn snapshot_is_literal_and_compares_both_coordinates() {
        let Some(bin) = jj_bin() else { return };
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("store");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        let run = |args: &[&str]| {
            let mut args = args.to_vec();
            args.push("--ignore-working-copy");
            jj.run(&store, &args, "advance snapshot fixture").unwrap()
        };
        run(&["bookmark", "set", "feature", "-r", "main"]);
        let before = operation_id(&jj, &store).unwrap();
        let snapshot = capture_branch_advance(&store, "feature", "main").unwrap();
        assert_eq!(snapshot.operation(), before);
        assert_eq!(operation_id(&jj, &store).unwrap(), before);
        assert!(snapshot.coordinates_match().unwrap());
        let old_head = snapshot.coordinate().head.clone();
        run(&["new", "main", "--no-edit", "-m", "owner commit"]);
        let owner = run(&[
            "log",
            "-r",
            "latest(children(main))",
            "--no-graph",
            "-T",
            "commit_id",
        ]);
        run(&["bookmark", "set", "feature", "-r", &owner]);
        assert!(!snapshot.coordinates_match().unwrap());
        assert_eq!(snapshot.coordinate().head, old_head);
        drop(snapshot);

        let snapshot = capture_branch_advance(&store, "feature", "main").unwrap();
        run(&["bookmark", "set", "main", "-r", &owner]);
        assert!(!snapshot.coordinates_match().unwrap());
        assert_eq!(bookmark_commit(&jj, &store, "feature").unwrap(), owner);
        let error = collect_store_garbage(
            &store,
            SystemTime::now(),
            SystemTime::now(),
            SystemTime::now(),
        )
        .unwrap_err();
        assert!(error.contains("detached advance"), "{error}");
        let error = truncate_operation_log(
            &store,
            SystemTime::now(),
            std::time::Instant::now() + std::time::Duration::from_secs(600),
        )
        .unwrap_err();
        assert!(error.contains("detached advance"), "{error}");
        let error = reclaim_unreachable_objects(&store_git_dir(&store).unwrap(), SystemTime::now())
            .unwrap_err();
        assert!(error.contains("detached advance"), "{error}");
        drop(snapshot);
        // Releasing a snapshot permits the same maintenance gate again.
        let _maintenance =
            super::super::object_lease::Reclamation::acquire(&store_git_dir(&store).unwrap())
                .unwrap();
        assert!(capture_branch_advance(&store, "feature", "main").is_err());
    }
}
