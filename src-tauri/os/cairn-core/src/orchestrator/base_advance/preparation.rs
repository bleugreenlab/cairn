//! Demand-driven, branch-addressed base preparation.
//!
//! A branch is carried onto its base only when work needs the current base: a
//! commit, a requested check or run capture, or a merge. Preparing one branch
//! never enumerates, replays, notifies or resumes any other branch.
//!
//! # Protocol
//!
//! Each attempt runs three phases. The canonical store writer lock is held only
//! for the first and the last, and never across network I/O or a wait for
//! anything except the lock itself:
//!
//! 1. **Snapshot** (under the lock): read the declared base relationship from
//!    the database, refuse a branch held by an in-flight run batch, recover an
//!    accepted install whose Git export never completed, and capture head `H`
//!    and base `B` from one exact operation. The relationship is read under the
//!    lock because every writer of `jobs.base_branch` and of the project default
//!    holds the same lock ([`crate::projects::default_branch`]).
//! 2. **Compute** (no lock): the detached replay runs on a blocking worker under
//!    an absolute cooperative deadline and publishes nothing.
//! 3. **Install** (under the lock): re-read the relationship and compare it with
//!    the snapshot's, then install, which compares both `H` and `B` in a fresh
//!    transaction. Any movement discards the candidate and the attempt restarts
//!    from a new snapshot, up to [`PREPARATION_ATTEMPTS`]. An owner's concurrent
//!    commit therefore always wins over a candidate computed before it.
//!
//! Every store mutation runs on a blocking worker that OWNS the writer guard, so
//! a caller that stops awaiting (a timeout, a cancelled request) cannot release
//! the lock while the mutation is still running.
//!
//! # Outcomes and their records
//!
//! A clean advance sends one passive note per base coordinate to the jobs on the
//! branch; it never resumes anyone. A conflict leaves the bookmark unchanged and
//! is recorded according to [`PreparationPurpose`]:
//!
//! - `Check`/`Run` persist non-actionable evidence on the branch's item and send
//!   nothing. They never open a conflict session or materialize markers.
//! - `OwnerCommit`/`OwnerMerge` open (or keep) the existing conflict session and
//!   steer the owner once per diagnostic fingerprint. The delivery keys live in
//!   their own namespace, so no check-only record can suppress a later owner
//!   wake.
//!
//! A branch whose immediate parent has an open conflict session of its own is
//! held wherever it would move, for every purpose
//! ([`PreparationRefusal::HeldByParentConflict`]): the refusal names the parent
//! and its session, nothing is recorded on the branch and nobody is woken. The
//! hold is read from the parent's session ([`held_by_parent_conflict`]) and ends
//! with it.
//!
//! Durable records reuse the reconcile tables: an outcome is keyed on the intent
//! row for the exact `(base branch, base commit)` coordinate plus this branch's
//! item. A demand-minted intent row is created `completed`, so the legacy sweep
//! (which claims only `pending` and lease-expired `running` rows) never expands
//! it into siblings. An existing intent row is reused only when it is already
//! `completed` or `superseded`; one that the legacy worker may still own
//! (`pending`/`running`) refuses the preparation as busy rather than risk
//! completing or editing accepted legacy work. Recovery is always by the exact
//! branch and coordinate.
//!
//! A timeout records the existing coordinate-keyed cooldown, which later
//! demands at the same head respect under the explicit-replay policy.
//!
//! # Recovery fences
//!
//! Every locked boundary that may mutate (the snapshot's repairs and export
//! recovery, the install, a conflict's record, a name repair, operation-head
//! settlement) first refuses a branch or store that a durable recovery fence
//! covers ([`crate::orchestrator::branch_recovery`]), as
//! [`PreparationRefusal::Fenced`]. Each budgeted transaction the preparer runs
//! hands its ledger's recoveries to [`persist_preparation_recoveries`] before
//! its guard is released; a fence recorded there means the branch is fenced and
//! not known to be unchanged, and its notices go out once every guard is.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cairn_db::turso::params;

use super::{
    advance_sibling_durable_base, append_conflict_diagnostic, branch_owners, load_on_branch_jobs,
    load_project_default_branch, load_project_repo_path, load_reconcile_quarantine,
    mark_publication_unconfirmed, materialize_markers_for_branch, persist_reconcile_item,
    quarantine_blocks_automatic_replay, record_reconcile_timeout, refresh_residencies_for_branch,
    release_reconcile_quarantine, replay_kept_patch, ReconcileItemUpdate, DELEGATOR_ON_BRANCH_JOIN,
    LIVE_JOB_PREDICATE, OWNS_BRANCH,
};
use crate::messages::delivery::{latest_run_for_job, queue_system_direct_once};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::conflict_session::{
    close_open_sessions_for_branch, load_active_session, record_conflict_session,
    record_marker_state, supersede_stale_sessions, ConflictSession, IncomingIdentity,
};
use crate::orchestrator::{JjStoreGuard, Orchestrator, StoreLockClass};
use crate::storage::{LocalDb, RowExt};

/// How many snapshot/compute/install rounds one preparation may take before a
/// branch or base that keeps moving is reported instead of chased.
pub(crate) const PREPARATION_ATTEMPTS: u32 = 3;

/// The detached replay's budget, the same bound the background reconcile uses.
pub(crate) const DEFAULT_COMPUTE_BUDGET: Duration = crate::jj::BACKGROUND_RECONCILE_BUDGET;

/// How long one phase waits for the store writer lock before refusing as busy.
pub(crate) const DEFAULT_LOCK_WAIT: Duration = crate::mcp::vcs::STORE_LOCK_TIMEOUT;

/// The bound on re-exporting an accepted install whose export failed. The jj
/// side is already durable; this only brings the Git ref to it.
const EXPORT_RECOVERY_BUDGET: Duration = Duration::from_secs(30);

/// Bounds on the follow-up work after an accepted install. Exceeding one is a
/// warning on the accepted coordinate, never a refusal.
const RESIDENCY_REFRESH_BUDGET: Duration = Duration::from_secs(30);
const PATCH_COMPARE_BUDGET: Duration = Duration::from_secs(30);

pub(super) const OUTCOME_EXPORT_PENDING: &str = "export_pending";
const OUTCOME_EXPORTED: &str = "exported";
pub(super) const OUTCOME_EXPORT_SUPERSEDED: &str = "export_superseded";
const OUTCOME_CHECK_CONFLICT: &str = "check_conflict";
const OUTCOME_CONFLICTED: &str = "conflicted";

/// Why a branch is being prepared. Only an owner's own commit or merge is
/// actionable: it may open a conflict session and wake the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparationPurpose {
    /// A committing write or a run batch's sealed publication.
    OwnerCommit,
    /// Landing the branch.
    OwnerMerge,
    /// A requested check capture, on this owner's branch or another's.
    Check,
    /// A verdict-only `run branch:`.
    Run,
}

impl PreparationPurpose {
    pub(crate) fn actionable(self) -> bool {
        matches!(self, Self::OwnerCommit | Self::OwnerMerge)
    }

    fn label(self) -> &'static str {
        match self {
            Self::OwnerCommit => "commit",
            Self::OwnerMerge => "merge",
            Self::Check => "check",
            Self::Run => "branch run",
        }
    }
}

/// One demand for a managed branch's current base.
#[derive(Clone)]
pub(crate) struct PreparationRequest {
    pub(crate) project_id: String,
    pub(crate) branch: String,
    /// The job whose commit, merge or capture asks for this preparation; its
    /// declaration governs the relationship. It may be a task sharing its
    /// parent's branch, which a conflict tells that the owner resolves. `None`
    /// resolves the branch's live owner; a terminal job's stale declaration
    /// never decides.
    pub(crate) owner_job_id: Option<String>,
    /// Prepare against this branch instead of the declared base: the actual
    /// landing target. The declaration is still captured and revalidated.
    pub(crate) target_override: Option<String>,
    /// The invoking run batch's own sealed result, which must not hold its own
    /// preparation; minted only by the run module from its proofs, and exempt
    /// only on the project, store and branch it was sealed for. Every other
    /// batch still does.
    pub(crate) sealed: Option<crate::mcp::handlers::run::SealedPublication>,
    pub(crate) purpose: PreparationPurpose,
    pub(crate) compute_budget: Duration,
    pub(crate) lock_wait: Duration,
    /// An already-authorized resolution carry for the captured head. This
    /// module never computes one.
    pub(crate) carry: Option<Arc<crate::jj::ResolutionCarry>>,
    /// The job whose turn-end review wave is capturing this preparation. An
    /// install keeps that wave (it is the one judging the prepared head);
    /// every other wave on the branch follows the patch rule.
    pub(crate) capturing_review_of: Option<String>,
    /// The owner explicitly requested this replay (cairn/6197). An automatic
    /// timeout cooldown never blocks it, and its own timeout is reported to
    /// the requester instead of cooling the branch down. The caller sizes
    /// `compute_budget` for it; demand preparation keeps the automatic bound.
    pub(crate) explicit_replay: bool,
    /// The exact base commit `carry` was planned onto.
    pub(crate) carry_base: Option<String>,
    /// Prepare onto this exact commit of the base's history instead of the
    /// base's live target, in either catch-up mode: a committed resolution
    /// lands on the session base it resolves, never a newer base it has not
    /// seen. Only an owner's commit may set it, and a commit that is not in
    /// the live base's history is refused at capture. An owner's commit with
    /// an open session behind the live base is pinned to that session's base
    /// even without it (see [`Context::pin_session_base`]).
    pub(crate) exact_destination: Option<String>,
    /// A reservation the caller already holds (a landing reserving source and
    /// target). It must cover this branch, and preparation never re-acquires.
    /// Without one, preparation reserves the branch itself before any store
    /// guard under which it would mutate: bounded for an owner's commit or
    /// merge, a single try for a check or run (busy, never a wait or a wake).
    pub(crate) reservation: Option<Arc<crate::orchestrator::BranchReservation>>,
    /// The write whose replay this preparation serves. Checked under every
    /// mutating guard before any mutation: an already-published write
    /// installs, exports and repairs nothing.
    pub(crate) write_replay: Option<crate::mcp::handlers::write::replay::WriteReplayProbe>,
    /// An absolute bound on the whole preparation. Every reservation wait,
    /// store-lock wait and compute phase takes the smaller of its own bound
    /// and the time left; it bounds acquisition and the cooperative compute
    /// deadline only, so a held guard is never dropped mid-mutation.
    pub(crate) deadline: Option<std::time::Instant>,
    /// The exact head this preparation is for. A capture at any other head is
    /// refused as moved under the snapshot guard, before anything else answers:
    /// a request accepted at one tip never applies to commits made after it.
    pub(crate) expected_head: Option<String>,
    /// The explicit request this preparation serves, for its cost evidence and
    /// its timeout key: when its budget started, how large it is, and the
    /// resolution semantics the carry alone does not show.
    pub(crate) explicit_cost: Option<super::replay_cost::ExplicitCost>,
}

/// `bound`, clamped to what is left of the request's deadline.
fn within_deadline(request: &PreparationRequest, bound: Duration) -> Duration {
    match request.deadline {
        Some(deadline) => bound.min(deadline.saturating_duration_since(std::time::Instant::now())),
        None => bound,
    }
}

impl PreparationRequest {
    pub(crate) fn new(
        project_id: impl Into<String>,
        branch: impl Into<String>,
        owner_job_id: Option<String>,
        purpose: PreparationPurpose,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            branch: branch.into(),
            owner_job_id,
            target_override: None,
            sealed: None,
            purpose,
            compute_budget: DEFAULT_COMPUTE_BUDGET,
            lock_wait: DEFAULT_LOCK_WAIT,
            carry: None,
            capturing_review_of: None,
            explicit_replay: false,
            carry_base: None,
            exact_destination: None,
            reservation: None,
            write_replay: None,
            deadline: None,
            expected_head: None,
            explicit_cost: None,
        }
    }

    pub(crate) fn for_check(
        project_id: impl Into<String>,
        branch: impl Into<String>,
        owner_job_id: Option<String>,
    ) -> Self {
        Self::new(project_id, branch, owner_job_id, PreparationPurpose::Check)
    }

    pub(crate) fn for_run(
        project_id: impl Into<String>,
        branch: impl Into<String>,
        owner_job_id: Option<String>,
    ) -> Self {
        Self::new(project_id, branch, owner_job_id, PreparationPurpose::Run)
    }
}

/// The integration relationship a preparation was performed against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BaseRelationship {
    pub(crate) owner_job_id: String,
    /// The owner's `jobs.base_branch`, when it declares one.
    pub(crate) declared: Option<String>,
    /// The project's effective default, which an undeclared base follows.
    pub(crate) project_default: String,
    pub(crate) target_override: Option<String>,
    /// The branch actually prepared onto.
    pub(crate) base_branch: String,
}

impl BaseRelationship {
    /// A branchless job's own integration target: the branch is its base.
    pub(crate) fn is_direct_target(&self, branch: &str) -> bool {
        self.base_branch == branch
    }

    /// Equal for installation purposes. Owner identity may change (another live
    /// job on the same branch) without changing what the base is.
    fn same_base(&self, other: &Self) -> bool {
        self.declared == other.declared
            && self.project_default == other.project_default
            && self.target_override == other.target_override
            && self.base_branch == other.base_branch
    }
}

/// Whether the Git side of the store reflects the prepared head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedExport {
    /// Nothing was installed and no earlier export was pending.
    NotNeeded,
    Exported,
    /// The install is durable in jj but its Git export failed. `recorded` names
    /// the intent row holding the durable pending-export record the next
    /// preparation of this branch recovers, or `None` when no record could be
    /// kept; the pull request's cached verdict was downgraded either way.
    Failed {
        error: String,
        recorded: Option<String>,
    },
}

/// An immutable prepared coordinate.
#[derive(Debug, Clone)]
pub(crate) struct PreparedBranch {
    pub(crate) project_id: String,
    pub(crate) store: PathBuf,
    pub(crate) branch: String,
    pub(crate) head_before: String,
    pub(crate) head_after: String,
    /// The jj/Git tree object id of `head_after`.
    pub(crate) tree: String,
    pub(crate) relationship: BaseRelationship,
    pub(crate) base_commit: String,
    /// `base_commit` is an open session's recorded base, deliberately behind
    /// the live base: revalidation then compares only the branch, since a
    /// newer base does not change what this coordinate was prepared against.
    pub(crate) pinned_base: bool,
    pub(crate) advanced: bool,
    pub(crate) export: PreparedExport,
    pub(crate) attempts: u32,
    /// The open conflict session an owner's resolution commit is bound to,
    /// when the branch was deliberately NOT carried onto the base because the
    /// commit resolves that recorded conflict.
    pub(crate) session: Option<BoundSession>,
    /// Follow-up work after an accepted install that did not complete. The
    /// coordinate above is accepted regardless.
    pub(crate) warnings: Vec<String>,
}

/// The recorded conflict a resolution commit proceeds under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundSession {
    pub(crate) intent_id: String,
    pub(crate) diagnostic_version: i64,
    pub(crate) fingerprint: String,
    /// The tip the conflict was recorded at; the head descends from it.
    pub(crate) ours: String,
}

impl PreparedBranch {
    /// Whether the bound session is still the branch's open session. Part of
    /// every revalidation; a coordinate with no bound session passes.
    async fn revalidate_session(&self, db: &LocalDb) -> Result<(), PreparationRefusal> {
        let Some(bound) = &self.session else {
            return Ok(());
        };
        let open = load_active_session(db, &self.branch)
            .await
            .map_err(PreparationRefusal::Repository)?;
        match open {
            Some(session)
                if session.intent_id == bound.intent_id
                    && session.diagnostic_version == bound.diagnostic_version
                    && session.fingerprint() == bound.fingerprint =>
            {
                Ok(())
            }
            _ => Err(PreparationRefusal::Moved {
                what: format!(
                    "the conflict session `{}` this commit resolves is no longer open",
                    bound.intent_id
                ),
            }),
        }
    }
}

impl PreparedBranch {
    /// Compare this coordinate with the store and database under the caller's
    /// own writer guard: the final check before a seal, publication or landing
    /// that was interpreted against it.
    pub(crate) async fn revalidate(
        &self,
        db: &LocalDb,
        guard: &JjStoreGuard,
    ) -> Result<(), PreparationRefusal> {
        self.revalidate_for_publication(db, guard, &self.head_after)
            .await
    }

    /// Revalidate after this writer has integrated held deltas under the same
    /// guard. The prepared capture remains immutable; `expected_head` is the
    /// exact head that writer produced, not permission to follow a moving name.
    /// Callers must revalidate the original capture before the first mutation
    /// and retain their guard through the sequence of delta publications.
    pub(crate) async fn revalidate_for_publication(
        &self,
        db: &LocalDb,
        guard: &JjStoreGuard,
        expected_head: &str,
    ) -> Result<(), PreparationRefusal> {
        if !guard.covers(&self.store) {
            return Err(PreparationRefusal::Repository(format!(
                "revalidating `{}` requires the writer lock of {}",
                self.branch,
                self.store.display()
            )));
        }
        let relationship = load_relationship(
            db,
            &self.project_id,
            &self.branch,
            Some(&self.relationship.owner_job_id),
            self.relationship.target_override.as_deref(),
        )
        .await?;
        if !relationship.same_base(&self.relationship) {
            return Err(PreparationRefusal::Moved {
                what: relationship_change(&self.relationship, &relationship),
            });
        }
        let (head, base) =
            literal_coordinates(&self.store, &self.branch, &self.relationship.base_branch).await?;
        if head != expected_head {
            return Err(PreparationRefusal::Moved {
                what: format!("`{}` moved from {expected_head} to {head}", self.branch),
            });
        }
        // A direct target is its own base: the writer's own publications
        // legitimately advance both, and the head check above covers it.
        if !self.relationship.is_direct_target(&self.branch)
            && !self.pinned_base
            && base != self.base_commit
        {
            return Err(PreparationRefusal::Moved {
                what: format!(
                    "base `{}` moved from {} to {base}",
                    self.relationship.base_branch, self.base_commit
                ),
            });
        }
        self.revalidate_session(db).await
    }
}

/// What a conflict refusal left on the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConflictRecord {
    /// Non-actionable evidence on this branch's item. Nothing woke.
    CheckEvidence { intent_id: String },
    /// The owner's conflict session. `reopened` is false when the same
    /// conflict was already open and was left as it was.
    Session { intent_id: String, reopened: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BusyReason {
    StoreLock {
        holder: Option<String>,
    },
    RunBatch(String),
    /// The legacy sweep may still own this coordinate's intent row.
    LegacyIntent {
        intent_id: String,
        status: String,
    },
    /// Another operation reserves the branch (or, for a store-wide
    /// mutation, a branch of the store).
    Reservation(String),
    /// Store maintenance (reclamation, garbage collection) holds the objects
    /// a capture needs.
    StoreMaintenance(String),
    /// An install's acceptance is not settled: the branch's head is not
    /// exactly what the journal row can prove. No writer may move the branch
    /// until it is (a repair when `status` is `unknown`).
    AcceptanceDebt {
        acceptance_id: String,
        status: String,
    },
    /// Internal: the attempt needs to mutate and holds no reservation yet.
    /// [`prepare_branch`] reserves and retries; it never escapes.
    ReservationRequired,
}

/// Where a branch conflicts with its base, and what preparation recorded of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BaseConflict {
    pub(crate) branch: String,
    pub(crate) head: String,
    pub(crate) base_branch: String,
    pub(crate) base_commit: String,
    pub(crate) paths: Vec<String>,
    pub(crate) record: ConflictRecord,
}

/// A preparation that installed nothing. The branch bookmark is unchanged by
/// this preparation in every case but a [`Self::Fenced`] this preparation
/// recorded itself, which says the branch is not known to be unchanged.
#[derive(Debug, Clone)]
pub(crate) enum PreparationRefusal {
    /// No live job manages this branch (or the named owner is not on it).
    Unmanaged {
        branch: String,
    },
    /// A coordinate or relationship that cannot be read.
    Unresolved {
        coordinate: String,
        reason: String,
    },
    /// A named bookmark does not exist in the project store. Typed apart from
    /// [`Self::Unresolved`] because absence is terminal: a caller whose own
    /// minted branch is the absent name answers with the lost-branch refusal
    /// its reads give, not a resolution failure that reads like a retryable typo.
    Absent {
        coordinate: String,
    },
    /// Live jobs on the branch declare different bases.
    RelationshipAmbiguous {
        branch: String,
        bases: Vec<String>,
    },
    Busy(BusyReason),
    /// An earlier attempt at this exact head timed out and its cooldown holds.
    Cooldown {
        fingerprint: String,
        retry_at: i64,
    },
    /// The branch conflicts with its base. Boxed: it is by far the largest
    /// refusal, and every preparation result carries the refusal's size.
    Conflict(Box<BaseConflict>),
    BaseConflicted {
        base_branch: String,
        base: String,
    },
    /// The branch's immediate parent has an open conflict session with its
    /// own base: the branch waits for that resolution instead of being carried
    /// onto the parent's unresolved tip. Nothing was recorded or woken.
    HeldByParentConflict(Box<ParentConflictHold>),
    TimedOut {
        head: String,
        base_branch: String,
        base_commit: String,
        budget: Duration,
        /// The blocker this timeout recorded, when it was an explicit replay's
        /// compute exhaustion: what an identical repeat now meets.
        blocker: Option<Box<super::replay_cost::TimeoutBlocker>>,
    },
    /// An explicit replay identical to one whose detached computation already
    /// exhausted its compute budget (cairn/6456). Refused before computing;
    /// nothing was recorded or changed. Boxed for the refusal's size.
    RepeatTimeout(Box<super::replay_cost::TimeoutBlocker>),
    RetriesExhausted {
        attempts: u32,
        last: String,
    },
    /// A prepared coordinate no longer holds (from [`PreparedBranch::revalidate`]).
    Moved {
        what: String,
    },
    /// The write this preparation serves was already published; nothing was
    /// done, and the caller answers from its recorded result.
    AlreadyPublished,
    /// A merge in the branch's history cannot be attributed to the branch or
    /// to one of its dependents. Nothing is installed or repaired.
    AmbiguousOwnership {
        branch: String,
        commit: String,
    },
    /// A durable recovery fence covers the branch or its store, or a budgeted
    /// step of this preparation left one: the refusal names the recovery, the
    /// budget class and the one supported route.
    Fenced {
        refusal: String,
    },
    Repository(String),
}

impl PreparationRefusal {
    /// A short, stable name for the outcome, for result rows and logs.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Unmanaged { .. } => "unmanaged",
            Self::Unresolved { .. } => "unresolved",
            Self::Absent { .. } => "absent",
            Self::RelationshipAmbiguous { .. } => "relationship ambiguous",
            Self::Busy(_) => "busy",
            Self::Cooldown { .. } => "cooldown",
            Self::Conflict(_) => "conflicts with base",
            Self::BaseConflicted { .. } => "base conflicted",
            Self::HeldByParentConflict(_) => "held by parent conflict",
            Self::TimedOut { .. } => "timed out",
            Self::RepeatTimeout(_) => "identical repeat of a timeout",
            Self::RetriesExhausted { .. } => "retries exhausted",
            Self::Moved { .. } => "moved",
            Self::AlreadyPublished => "already published",
            Self::AmbiguousOwnership { .. } => "ambiguous ownership",
            Self::Fenced { .. } => "fenced",
            Self::Repository(_) => "repository error",
        }
    }
}

impl std::fmt::Display for PreparationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unmanaged { branch } => write!(
                f,
                "`{branch}` is not a managed branch with a live owner, so it was not prepared"
            ),
            Self::Unresolved { coordinate, reason } => {
                write!(f, "`{coordinate}` could not be resolved: {reason}")
            }
            Self::Absent { coordinate } => write!(
                f,
                "`{coordinate}` could not be resolved: no such bookmark in the project store"
            ),
            Self::RelationshipAmbiguous { branch, bases } => write!(
                f,
                "the live jobs on `{branch}` declare different bases ({}), so which base it \
                 carries is ambiguous",
                bases.join(", ")
            ),
            Self::Busy(BusyReason::StoreLock { holder }) => write!(
                f,
                "the project's version-control store is busy (held by {})",
                holder.as_deref().unwrap_or("another operation")
            ),
            Self::Busy(BusyReason::RunBatch(batch)) => write!(
                f,
                "{batch} may still commit to this branch, so it is not rewritten under it"
            ),
            Self::Busy(BusyReason::LegacyIntent { intent_id, status }) => write!(
                f,
                "the background reconcile still owns this base coordinate (intent {intent_id} is \
                 {status})"
            ),
            Self::Busy(BusyReason::Reservation(holder)) => write!(f, "{holder}"),
            Self::Busy(BusyReason::AcceptanceDebt {
                acceptance_id,
                status,
            }) => write!(
                f,
                "an earlier install of this branch is not settled (acceptance {acceptance_id} is \
                 {status}); nothing moves the branch until it is"
            ),
            Self::Busy(BusyReason::StoreMaintenance(reason)) => {
                write!(f, "the project store is under maintenance: {reason}")
            }
            Self::Busy(BusyReason::ReservationRequired) => {
                write!(f, "the branch must be reserved before it is prepared")
            }
            Self::AlreadyPublished => write!(f, "this write was already published"),
            Self::Cooldown {
                fingerprint,
                retry_at,
            } => write!(
                f,
                "preparing this unchanged head timed out before at {fingerprint}; it is cooling \
                 down until Unix time {retry_at} (a new commit on the branch releases it)"
            ),
            Self::Conflict(conflict) => {
                let BaseConflict {
                    branch,
                    base_branch,
                    base_commit,
                    paths,
                    ..
                } = &**conflict;
                write!(
                    f,
                    "`{branch}` conflicts with base `{base_branch}` at {base_commit}{}; the \
                     branch was left unchanged",
                    if paths.is_empty() {
                        String::new()
                    } else {
                        format!(" in {}", paths.join(", "))
                    }
                )
            }
            Self::BaseConflicted { base_branch, base } => write!(
                f,
                "base `{base_branch}` is itself conflicted at {base}; nothing can be prepared onto it"
            ),
            Self::HeldByParentConflict(hold) => write!(
                f,
                "`{}` is held: its parent `{}` has an open conflict session (session {}) with \
                 `{}` at {}; the branch is left unchanged and is prepared at its next demand \
                 once that session is resolved",
                hold.branch,
                hold.parent,
                hold.session,
                hold.parent_base,
                hold.parent_destination
            ),
            Self::TimedOut {
                head,
                base_branch,
                base_commit,
                budget,
                ..
            } => write!(
                f,
                "preparing {head} onto `{base_branch}` at {base_commit} exceeded its {}s budget; \
                 nothing was installed",
                budget.as_secs()
            ),
            Self::RepeatTimeout(blocker) => write!(
                f,
                "an identical explicit replay of {} onto `{}` at {} already exhausted its \
                 compute budget, so this one was refused before computing; nothing changed",
                blocker.key.head, blocker.key.base_branch, blocker.key.base
            ),
            Self::RetriesExhausted { attempts, last } => write!(
                f,
                "the branch or its base kept moving across {attempts} preparation attempts \
                 (last: {last})"
            ),
            Self::Moved { what } => write!(f, "the prepared coordinate moved: {what}"),
            Self::AmbiguousOwnership { branch, commit } => write!(
                f,
                "whether merge {commit} in `{branch}`'s history belongs to it or to a branch \
                 built on it cannot be told, so nothing was replayed"
            ),
            Self::Fenced { refusal } => write!(f, "{refusal}"),
            Self::Repository(error) => write!(f, "preparation failed: {error}"),
        }
    }
}

/// A branch held behind its immediate parent's open conflict session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParentConflictHold {
    pub(crate) branch: String,
    pub(crate) parent: String,
    /// The parent's session: its reconcile intent id.
    pub(crate) session: String,
    /// What the parent conflicts with: its own base and that base's commit.
    pub(crate) parent_base: String,
    pub(crate) parent_destination: String,
}

/// Whether `branch` is held because `parent`, its immediate base, has an open
/// conflict session in `store`.
///
/// This is the one source of the held-descendant rule. The hold is derived
/// from the parent's live session row on every read, never from a recorded
/// per-descendant intent, so resolving or superseding the parent's session
/// releases every descendant at its next demand with nothing to clean up.
/// The preparer consults it wherever it would move a branch, for every
/// purpose. Any other surface that would move or rebuild a descendant, such
/// as a rebase resource `rebuild` action, reads this same function rather
/// than keeping a hold of its own.
pub(crate) async fn held_by_parent_conflict(
    db: &LocalDb,
    store: &Path,
    branch: &str,
    parent: &str,
) -> Result<Option<ParentConflictHold>, String> {
    if parent == branch {
        return Ok(None);
    }
    let (store_path, parent_name) = (store.to_string_lossy().into_owned(), parent.to_string());
    let found = db
        .read(move |conn| {
            let (store_path, parent) = (store_path.clone(), parent_name.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.intent_id, n.target_branch, n.destination_commit
                           FROM jj_reconcile_items i
                           JOIN jj_reconcile_intents n ON n.id = i.intent_id
                          WHERE i.bookmark = ?1 AND n.store_path = ?2
                            AND i.resolution_state = 'open'
                            AND i.diagnostic_version IS NOT NULL
                          ORDER BY i.updated_at DESC LIMIT 1",
                        params![parent.as_str(), store_path.as_str()],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.text(1)?, row.text(2)?)),
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| format!("read the parent's conflict session: {error}"))?;
    Ok(found.map(
        |(session, parent_base, parent_destination)| ParentConflictHold {
            branch: branch.to_string(),
            parent: parent.to_string(),
            session,
            parent_base,
            parent_destination,
        },
    ))
}

/// Prepare one managed branch onto its current base. See the module docs.
///
/// Every preparation is reported to the merge attempt whose task it runs on,
/// if any (`pr_data::actions::merge_metrics`); elsewhere the report is a no-op.
pub(crate) async fn prepare_branch(
    orch: &Orchestrator,
    request: PreparationRequest,
) -> Result<PreparedBranch, PreparationRefusal> {
    use crate::pr_data::actions::merge_metrics::{note_preparation, PreparationNote};
    let branch = request.branch.clone();
    let started = std::time::Instant::now();
    let result = prepare_branch_unobserved(orch, request).await;
    let note = match &result {
        Ok(prepared) => PreparationNote::Prepared {
            head_before: &prepared.head_before,
            head_after: &prepared.head_after,
            base_commit: &prepared.base_commit,
            advanced: prepared.advanced,
        },
        Err(refusal) => PreparationNote::Refused {
            kind: refusal.kind(),
        },
    };
    note_preparation(&branch, started.elapsed(), note);
    result
}

async fn prepare_branch_unobserved(
    orch: &Orchestrator,
    request: PreparationRequest,
) -> Result<PreparedBranch, PreparationRefusal> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &request.project_id)
        .await
        .map_err(|error| PreparationRefusal::Repository(error.to_string()))?;
    let repo_path = load_project_repo_path(&orch.db, &request.project_id)
        .await
        .map_err(PreparationRefusal::Repository)?
        .filter(|path| !path.is_empty())
        .ok_or_else(|| PreparationRefusal::Unmanaged {
            branch: request.branch.clone(),
        })?;
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    if let Some(inherited) = &request.reservation {
        if !inherited.covers(&store, &request.branch) {
            return Err(PreparationRefusal::Repository(format!(
                "the inherited reservation does not cover `{}` of {}",
                request.branch,
                store.display()
            )));
        }
    }
    let context = Context {
        orch,
        db: &db,
        shared_db: db.clone(),
        jj: crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        store,
        repo_path,
        request: &request,
        owned: std::sync::Mutex::new(request.reservation.clone()),
        pinned: std::sync::Mutex::new(None),
        recorded: std::sync::atomic::AtomicBool::new(false),
        fenced_here: std::sync::Mutex::new(None),
        started: std::time::Instant::now(),
        cost: std::sync::Mutex::new(AttemptCost::default()),
    };
    let result = prepare_in(&context).await;
    // Durable before the result is served to anyone.
    context.record_cost_sample(&result).await;
    // Every guard is released by now; each fence was durable before its was.
    if context.recorded.load(std::sync::atomic::Ordering::SeqCst) {
        crate::orchestrator::branch_recovery::deliver_pending_notices(orch, &db).await;
    }
    result
}

async fn prepare_in(context: &Context<'_>) -> Result<PreparedBranch, PreparationRefusal> {
    let request = context.request;
    if !crate::jj::is_jj_dir(&context.store) {
        // Only an owner's own commit or merge may create the store; a check,
        // a run or a read of a project with no store has nothing to prepare.
        if !request.purpose.actionable() {
            return Err(PreparationRefusal::Unmanaged {
                branch: request.branch.clone(),
            });
        }
        context.ensure_reserved().await?;
        context.initialize_store().await?;
    }
    let mut last = String::from("no attempt completed");
    let mut declared: Option<Arc<crate::jj::DeclaredMergeEdits>> = None;
    let mut declared_tried = false;
    let (mut attempt, mut races) = (0, 0);
    while races < PREPARATION_ATTEMPTS {
        // A round that stopped only to reserve is not an attempt: the next
        // round reuses its number.
        let outcome = match context.attempt(attempt + 1, declared.clone()).await {
            // The attempt stopped before mutating, its guards released, so
            // reserving now never happens under the store lock.
            Err(PreparationRefusal::Busy(BusyReason::ReservationRequired))
                if context.reserved().is_none() =>
            {
                context.ensure_reserved().await?;
                continue;
            }
            Err(PreparationRefusal::Busy(BusyReason::ReservationRequired)) => {
                return Err(PreparationRefusal::Repository(
                    "preparation asked for a reservation it already holds".to_string(),
                ));
            }
            other => other?,
        };
        attempt += 1;
        match outcome {
            Attempt::Prepared(prepared) => return Ok(*prepared),
            Attempt::Raced(why) => {
                log::info!(
                    "branch preparation of `{}` (attempt {attempt}) restarts: {why}",
                    request.branch
                );
                races += 1;
                last = why;
                // A plan proven at the old coordinate proves nothing at a new
                // one; the next conflict may prove a fresh one.
                declared = None;
                declared_tried = false;
            }
            Attempt::Declared(edits) if !declared_tried => {
                declared = Some(Arc::new(edits));
                declared_tried = true;
            }
            Attempt::Declared(_) => {
                return Err(PreparationRefusal::Repository(
                    "a declared merge plan was proposed twice at one coordinate".to_string(),
                ));
            }
        }
    }
    Err(PreparationRefusal::RetriesExhausted {
        attempts: PREPARATION_ATTEMPTS,
        last,
    })
}

/// Persist what one budgeted transaction of `branch`'s preparation could not
/// verify, while the caller still holds the store guard, and say what it means
/// for the branch. `None` when every rollback in it was verified, or none ran,
/// and no writer it killed may survive.
///
/// A recorded fence is reported as [`crate::jj::ROLLBACK_INCOMPLETE`]: the
/// branch is fenced, never "unchanged". A recovery the database refuses is
/// reported as such, and the store stays refused for this process (see
/// [`crate::orchestrator::branch_recovery::record_recoveries`]).
pub(super) async fn persist_preparation_recoveries(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    explicit_replay: bool,
    operation: &str,
    recoveries: Vec<crate::jj::RecoveryCoordinates>,
) -> Option<String> {
    use crate::orchestrator::branch_recovery;
    if recoveries.is_empty() {
        return None;
    }
    let class = if explicit_replay {
        branch_recovery::BUDGET_EXPLICIT
    } else {
        branch_recovery::BUDGET_AUTOMATIC
    };
    let note = match branch_recovery::record_recoveries(
        db,
        project_id,
        store,
        Some(branch),
        class,
        operation,
        recoveries,
    )
    .await
    {
        Ok(records) => records
            .iter()
            .map(|record| record.refusal(Some(branch), "Publishing or rewriting this branch"))
            .collect::<Vec<_>>()
            .join("\n"),
        Err(error) => error,
    };
    Some(format!(
        "{} the {operation} could not be verified, so `{branch}` is fenced, not known to be \
         unchanged.\n{note}",
        crate::jj::ROLLBACK_INCOMPLETE
    ))
}

/// The part of a conflict session row a basis assessment depends on.
#[derive(PartialEq)]
struct SessionProof {
    intent_id: String,
    fingerprint: String,
    diagnostic_version: i64,
    condition: String,
    resolution_state: Option<String>,
    updated_at: i64,
    files: Vec<crate::orchestrator::conflict_session::SessionFile>,
    basis: crate::orchestrator::conflict_session::ResolutionBasis,
    replay_requested_at: Option<i64>,
    replay_requested_tip: Option<String>,
    replay_requested_destination: Option<String>,
}

impl SessionProof {
    fn of(session: &crate::orchestrator::conflict_session::ConflictSession) -> Self {
        Self {
            intent_id: session.intent_id.clone(),
            fingerprint: session.fingerprint(),
            diagnostic_version: session.diagnostic_version,
            condition: session.condition.clone(),
            resolution_state: session.resolution_state.clone(),
            updated_at: session.updated_at,
            files: session.files.clone(),
            basis: session.resolution_basis.clone(),
            replay_requested_at: session.replay_requested_at,
            replay_requested_tip: session.replay_requested_tip.clone(),
            replay_requested_destination: session.replay_requested_destination.clone(),
        }
    }
}

/// How many removed-from-branch claims (its own and inherited) one row may
/// carry.
const INHERITED_DROP_CAP: usize = 4096;

/// How many of the newest accepted anchors inheritance considers, and how
/// much claim text one row may hold before it fails by name.
const ACCEPTED_ROWS_READ: i64 = 64;
const ACCEPTED_CLAIM_BYTES: i64 = 4 * 1024 * 1024;

enum Inherited {
    /// The snapshot is boxed: it dwarfs the timeout variant.
    Claims(
        JjStoreGuard,
        Box<crate::jj::BranchAdvanceSnapshot>,
        Vec<String>,
    ),
    TimedOut,
}

/// An acceptance journal row still owed: `pending` or `unknown`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct AcceptanceDebt {
    pub(crate) id: String,
    pub(crate) status: String,
    pub(crate) head_before: String,
    pub(crate) head_after: String,
    /// A store-wide mutation's fence, if one is set: its id and the bookmark
    /// and Git ref it read before mutating (`None` = absent).
    pub(crate) bracket_id: Option<String>,
    pub(crate) bracket_bookmark: Option<String>,
    pub(crate) bracket_git_ref: Option<String>,
}

/// One owed row a store-wide mutation fences before it mutates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptanceFence {
    pub(crate) id: String,
    pub(crate) bracket_id: String,
    pub(crate) bookmark: Option<String>,
    pub(crate) git_ref: Option<String>,
}

/// Fence `fences` in one transaction, only on rows still pending or unknown.
/// An error means nothing was fenced and the caller must not mutate.
pub(crate) async fn fence_acceptances(
    db: &LocalDb,
    fences: &[AcceptanceFence],
) -> Result<(), String> {
    #[cfg(test)]
    if tests::fence_writes_fail() {
        return Err("injected acceptance fence failure".to_string());
    }
    let fences = fences.to_vec();
    db.write(move |conn| {
        let fences = fences.clone();
        Box::pin(async move {
            for fence in &fences {
                conn.execute(
                    "UPDATE branch_advance_acceptances
                        SET bracket_id = ?2, bracket_bookmark = ?3, bracket_git_ref = ?4
                      WHERE id = ?1 AND status IN ('pending', 'unknown')",
                    params![
                        fence.id.as_str(),
                        fence.bracket_id.as_str(),
                        fence.bookmark.as_deref(),
                        fence.git_ref.as_deref()
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("fence owed acceptances: {error}"))
}

/// Finish a bracket in one transaction: the rows in `moved_ids` become
/// `unknown` (when pending), and every row of `bracket_id` loses its fence.
/// If this fails the fence stays, and settlement resolves it from the copies.
pub(crate) async fn resolve_acceptance_fences(
    db: &LocalDb,
    bracket_id: &str,
    moved_ids: &[String],
) -> Result<(), String> {
    #[cfg(test)]
    if tests::fence_writes_fail() {
        return Err("injected acceptance fence failure".to_string());
    }
    let (bracket, moved) = (bracket_id.to_string(), moved_ids.to_vec());
    db.write(move |conn| {
        let (bracket, moved) = (bracket.clone(), moved.clone());
        Box::pin(async move {
            for id in &moved {
                conn.execute(
                    "UPDATE branch_advance_acceptances SET status = 'unknown'
                      WHERE id = ?1 AND bracket_id = ?2 AND status = 'pending'",
                    params![id.as_str(), bracket.as_str()],
                )
                .await?;
            }
            conn.execute(
                "UPDATE branch_advance_acceptances
                    SET bracket_id = NULL, bracket_bookmark = NULL, bracket_git_ref = NULL
                  WHERE bracket_id = ?1",
                params![bracket.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("finish the acceptance fence {bracket_id}: {error}"))
}

/// An operator's explicit dismissal of an `unknown` row, the only way one
/// leaves `unknown`. Under the store guard and the branch's reservation, it
/// requires the row's exact identity, an `unknown` status and the branch's
/// literal head equal to `expected_head`; it records that head and `reason`
/// and never moves, pushes, exports or wakes anything. The row stays as
/// history and never authorizes a claim. Repeating the same dismissal (same
/// head and reason) is a no-op; anything else is refused.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dismiss_unknown_acceptance(
    db: &LocalDb,
    guard: &JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
    project_id: &str,
    store: &Path,
    branch: &str,
    acceptance_id: &str,
    expected_head: &str,
    reason: &str,
) -> Result<AcceptanceDismissal, String> {
    if !guard.covers(store) {
        return Err(format!(
            "dismissing requires the writer lock of {}",
            store.display()
        ));
    }
    if !reservation.covers(store, branch) {
        return Err(format!("dismissing requires `{branch}` reserved"));
    }
    if reason.trim().is_empty() {
        return Err("a dismissal states its reason".to_string());
    }
    let store_key = journal_store_key(store)?;
    let (id, project, key, name) = (
        acceptance_id.to_string(),
        project_id.to_string(),
        store_key.clone(),
        branch.to_string(),
    );
    let row: Option<(String, Option<String>, Option<String>)> = db
        .read(move |conn| {
            let (id, project, key, name) = (id.clone(), project.clone(), key.clone(), name.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, repair_head, repair_reason FROM branch_advance_acceptances
                          WHERE id = ?1 AND project_id = ?2 AND store_path = ?3 AND branch = ?4",
                        params![id.as_str(), project.as_str(), key.as_str(), name.as_str()],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| format!("read acceptance {acceptance_id}: {error}"))?;
    let Some((status, repair_head, repair_reason)) = row else {
        return Err(format!(
            "no acceptance {acceptance_id} on `{branch}` of this store"
        ));
    };
    if status == "dismissed" {
        return if repair_head.as_deref() == Some(expected_head)
            && repair_reason.as_deref() == Some(reason)
        {
            Ok(AcceptanceDismissal::AlreadyDismissed)
        } else {
            Err(format!(
                "acceptance {acceptance_id} was already dismissed differently"
            ))
        };
    }
    if status != "unknown" {
        return Err(format!(
            "acceptance {acceptance_id} is {status}; only an unknown acceptance can be dismissed"
        ));
    }
    let head = cairn_vcs::resolve_coordinate(store, branch)
        .await
        .map_err(|error| format!("`{branch}`'s literal head is unreadable: {error}"))?;
    if head != expected_head {
        return Err(format!(
            "`{branch}` is at {head}, not the {expected_head} the dismissal names"
        ));
    }
    let changed = db
        .execute(
            "UPDATE branch_advance_acceptances
                SET status = 'dismissed', dismissed_at = ?2, repair_head = ?3, repair_reason = ?4
              WHERE id = ?1 AND status = 'unknown' AND bracket_id IS NULL",
            params![
                acceptance_id,
                chrono::Utc::now().timestamp(),
                expected_head,
                reason
            ],
        )
        .await
        .map_err(|error| format!("dismiss acceptance {acceptance_id}: {error}"))?;
    if changed != 1 {
        return Err(format!(
            "acceptance {acceptance_id} changed, or is fenced by a store-wide mutation, while \
             it was dismissed"
        ));
    }
    Ok(AcceptanceDismissal::Dismissed)
}

/// What [`dismiss_unknown_acceptance`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceptanceDismissal {
    Dismissed,
    AlreadyDismissed,
}

/// The one store identity the acceptance journal is written and read under:
/// the canonical path, as the store-lock and reservation registries key it,
/// so an alias, `..` or symlink spelling of a store finds the same rows. An
/// existing store that cannot be resolved fails closed rather than fall back
/// to the raw spelling, which could miss outstanding debt.
pub(crate) fn journal_store_key(store: &Path) -> Result<String, String> {
    std::fs::canonicalize(store)
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| {
            format!(
                "the store {} cannot be resolved to its canonical identity: {error}",
                store.display()
            )
        })
}

/// The branch's unsettled acceptance rows. A writer of the branch's bookmark
/// must not move it while any exists (except the preparation that settles it).
pub(crate) async fn outstanding_acceptances(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Vec<AcceptanceDebt>, String> {
    let (project, store, branch) = (
        project_id.to_string(),
        journal_store_key(store)?,
        branch.to_string(),
    );
    db.read(move |conn| {
        let (project, store, branch) = (project.clone(), store.clone(), branch.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, status, head_before, head_after,
                            bracket_id, bracket_bookmark, bracket_git_ref
                       FROM branch_advance_acceptances
                      WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                        AND status IN ('pending', 'unknown')
                      ORDER BY created_at, id",
                    params![project.as_str(), store.as_str(), branch.as_str()],
                )
                .await?;
            let mut owed = Vec::new();
            while let Some(row) = rows.next().await? {
                owed.push(AcceptanceDebt {
                    id: row.text(0)?,
                    status: row.text(1)?,
                    head_before: row.text(2)?,
                    head_after: row.text(3)?,
                    bracket_id: row.opt_text(4)?,
                    bracket_bookmark: row.opt_text(5)?,
                    bracket_git_ref: row.opt_text(6)?,
                });
            }
            Ok(owed)
        })
    })
    .await
    .map_err(|error| format!("read the acceptance journal: {error}"))
}

/// The branch's newest accepted install as `(head_after, dropped ∪
/// inherited)`, or nothing: the claims a publication's supersession proof
/// may use when that `head_after` is in the published tip's history. Never
/// pending, unknown or dismissed rows.
///
/// One row's claim text is read, bounded by [`ACCEPTED_CLAIM_BYTES`]. Reading
/// only the newest is sound because each accepted row inherits the claims of
/// the newest accepted row its starting head descends from, so the newest row
/// carries the chain. A branch rewound behind it finds no anchor and its proof
/// refuses; nothing passes on a partial set.
pub(crate) async fn accepted_drop_claims(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Vec<(String, Vec<String>)>, String> {
    let Some((id, head_after)) = accepted_anchors(db, project_id, store, branch)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(Vec::new());
    };
    Ok(vec![(head_after, acceptance_claims(db, &id).await?)])
}

/// The newest accepted rows' `(id, head_after)`, newest first, at most
/// [`ACCEPTED_ROWS_READ`]: identities only, never claim text.
async fn accepted_anchors(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Vec<(String, String)>, String> {
    let (project, store, branch) = (
        project_id.to_string(),
        journal_store_key(store)?,
        branch.to_string(),
    );
    db.read(move |conn| {
        let (project, store, branch) = (project.clone(), store.clone(), branch.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, head_after FROM branch_advance_acceptances
                      WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                        AND status = 'accepted'
                      ORDER BY created_at DESC, id DESC
                      LIMIT ?4",
                    params![
                        project.as_str(),
                        store.as_str(),
                        branch.as_str(),
                        ACCEPTED_ROWS_READ
                    ],
                )
                .await?;
            let mut anchors = Vec::new();
            while let Some(row) = rows.next().await? {
                anchors.push((row.text(0)?, row.text(1)?));
            }
            Ok(anchors)
        })
    })
    .await
    .map_err(|error| format!("read accepted installs: {error}"))
}

/// One accepted row's claims, `dropped ∪ inherited`, bounded by
/// [`ACCEPTED_CLAIM_BYTES`] before it is parsed.
async fn acceptance_claims(db: &LocalDb, id: &str) -> Result<Vec<String>, String> {
    let id = id.to_string();
    let (dropped, inherited, bytes): (String, String, i64) = db
        .read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT dropped_commits_json, inherited_drops_json,
                                length(dropped_commits_json) + length(inherited_drops_json)
                           FROM branch_advance_acceptances
                          WHERE id = ?1 AND status = 'accepted'",
                        params![id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row("the accepted row vanished".to_string())
                })?;
                Ok((row.text(0)?, row.text(1)?, row.i64(2)?))
            })
        })
        .await
        .map_err(|error| format!("read an accepted install's claims: {error}"))?;
    if bytes > ACCEPTED_CLAIM_BYTES {
        return Err(format!(
            "an accepted install carries more than {ACCEPTED_CLAIM_BYTES} bytes of \
             removed-from-branch claims"
        ));
    }
    let mut claims: std::collections::BTreeSet<String> =
        serde_json::from_str::<Vec<String>>(&dropped)
            .map_err(|error| format!("unreadable dropped claims: {error}"))?
            .into_iter()
            .collect();
    claims.extend(
        serde_json::from_str::<Vec<String>>(&inherited)
            .map_err(|error| format!("unreadable inherited claims: {error}"))?,
    );
    Ok(claims.into_iter().collect())
}

/// Settle the branch's outstanding acceptance rows before a first write, for
/// every bookmark writer that does not go through [`prepare_branch`] (landing
/// fold, retirement, origin fast-forward, collapse). `guard` witnesses the
/// store writer lock; the branch's literal head is read once under it. Returns
/// the row still owed, if any: the caller must then not move the branch.
pub(crate) async fn settle_branch_acceptances(
    db: &LocalDb,
    guard: &JjStoreGuard,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Option<AcceptanceDebt>, String> {
    if !guard.covers(store) {
        return Err(format!(
            "settling `{branch}`'s acceptances requires the writer lock of {}",
            store.display()
        ));
    }
    let owed = outstanding_acceptances(db, project_id, store, branch).await?;
    let Some(first) = owed.first().cloned() else {
        return Ok(None);
    };
    match cairn_vcs::resolve_coordinate(store, branch).await {
        Ok(head) => settle_acceptances_at(db, project_id, store, branch, &head).await,
        // An unreadable head proves nothing: the debt stands as recorded.
        Err(error) => {
            log::warn!("settle `{branch}`'s acceptances: its head is unreadable: {error}");
            Ok(Some(first))
        }
    }
}

/// Record a confirmed publication of `branch` at exactly `published` as the
/// claims reset point, on the newest accepted row, once. The caller holds the
/// store guard and the branch's reservation and must have proven that origin's
/// copy is exactly `published` (a read-back of the accepted lease, not a
/// fetch or a webhook); without that proof it must not call this, and the
/// publication is still accepted. `published` must be the branch's literal
/// head. Never for an integration base such as the project default.
/// Repeating it is a no-op; a row already checkpointed keeps its first one.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_publication_checkpoint(
    db: &LocalDb,
    guard: &JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
    project_id: &str,
    store: &Path,
    branch: &str,
    published: &str,
) -> Result<(), String> {
    if !guard.covers(store) || !reservation.covers(store, branch) {
        return Err(format!(
            "recording `{branch}`'s publication requires its writer lock and reservation"
        ));
    }
    // Enforced, not left to callers: an integration base never resets
    // claims, and only a managed branch (one with a live owner) has them.
    if load_project_default_branch(db, project_id)
        .await?
        .as_deref()
        == Some(branch)
    {
        return Err(format!(
            "`{branch}` is the project default; its publication records no checkpoint"
        ));
    }
    if live_branch_owner(db, project_id, branch).await?.is_none() {
        return Err(format!(
            "`{branch}` is not a managed branch; its publication records no checkpoint"
        ));
    }
    let head = cairn_vcs::resolve_coordinate(store, branch)
        .await
        .map_err(|error| format!("`{branch}`'s literal head is unreadable: {error}"))?;
    if head != published {
        return Err(format!(
            "`{branch}` is at {head}, not the published {published}; no checkpoint recorded"
        ));
    }
    let Some((id, _)) = accepted_anchors(db, project_id, store, branch)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
    db.execute(
        "UPDATE branch_advance_acceptances
            SET published_checkpoint = ?2, published_at = ?3
          WHERE id = ?1 AND status = 'accepted' AND published_checkpoint IS NULL",
        params![id.as_str(), published, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record `{branch}`'s publication checkpoint: {error}"))
}

async fn acceptance_checkpoint(db: &LocalDb, id: &str) -> Result<Option<String>, String> {
    let id = id.to_string();
    db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT published_checkpoint FROM branch_advance_acceptances WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            })
        })
    })
    .await
    .map_err(|error| format!("read a publication checkpoint: {error}"))
}

/// The single settlement rule for a `pending` row, against a head read once
/// under the lock: == `head_after` accepts, == `head_before` abandons,
/// anything else is unknown. Never decided by ancestry; `unknown` is final
/// until repaired.
async fn settle_acceptances_at(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    head: &str,
) -> Result<Option<AcceptanceDebt>, String> {
    let mut owed = None;
    let rows = outstanding_acceptances(db, project_id, store, branch).await?;
    // A fence left by a store-wide mutation that never finished is resolved
    // first, from its stored copies against the bookmark and Git ref now.
    let state = if rows.iter().any(|row| row.bracket_id.is_some()) {
        let (store, branch) = (store.to_path_buf(), branch.to_string());
        tokio::task::spawn_blocking(move || crate::jj::read_local_branch_state(&store, &branch))
            .await
            .map_err(|error| error.to_string())
            .and_then(|state| state)
            .ok()
    } else {
        None
    };
    for mut row in rows {
        if let Some(bracket) = row.bracket_id.clone() {
            let unmoved = state.as_ref().is_some_and(|state| {
                state.bookmark == row.bracket_bookmark && state.git_ref == row.bracket_git_ref
            });
            let moved = if unmoved {
                Vec::new()
            } else {
                vec![row.id.clone()]
            };
            match resolve_acceptance_fences(db, &bracket, &moved).await {
                Ok(()) if unmoved => row.bracket_id = None,
                Ok(()) => {
                    owed.get_or_insert(AcceptanceDebt {
                        status: "unknown".to_string(),
                        bracket_id: None,
                        ..row
                    });
                    continue;
                }
                Err(error) => {
                    log::warn!("resolve the fence on acceptance {}: {error}", row.id);
                    owed.get_or_insert(row);
                    continue;
                }
            }
        }
        // An unknown row was seen somewhere neither of its heads names, or
        // was moved by a store-wide mutation: no later head is evidence. It
        // leaves only by an explicit dismissal.
        if row.status == "unknown" {
            owed.get_or_insert(row);
            continue;
        }
        let settled = if head == row.head_after {
            "accepted"
        } else if head == row.head_before {
            "abandoned"
        } else {
            "unknown"
        };
        #[cfg(test)]
        let injected = tests::acceptance_write_fails(store);
        #[cfg(not(test))]
        let injected = false;
        let written = if injected {
            Err("injected acceptance write failure".to_string())
        } else {
            set_acceptance_status(db, &row.id, settled, None).await
        };
        match (written, settled) {
            (Ok(()), "accepted" | "abandoned") => {}
            (Ok(()), status) => {
                owed.get_or_insert(AcceptanceDebt {
                    status: status.to_string(),
                    ..row
                });
            }
            (Err(error), _) => {
                log::warn!("settle acceptance {}: {error}", row.id);
                owed.get_or_insert(row);
            }
        }
    }
    Ok(owed)
}

/// The branch's accepted installs whose export is still owed, oldest first,
/// bounded.
async fn accepted_unexported(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Vec<(String, String)>, String> {
    let (project, store, branch) = (
        project_id.to_string(),
        journal_store_key(store)?,
        branch.to_string(),
    );
    db.read(move |conn| {
        let (project, store, branch) = (project.clone(), store.clone(), branch.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, head_after FROM branch_advance_acceptances
                      WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                        AND status = 'accepted' AND export_state = 'pending'
                      ORDER BY created_at, id
                      LIMIT ?4",
                    params![
                        project.as_str(),
                        store.as_str(),
                        branch.as_str(),
                        ACCEPTED_ROWS_READ
                    ],
                )
                .await?;
            let mut owed = Vec::new();
            while let Some(row) = rows.next().await? {
                owed.push((row.text(0)?, row.text(1)?));
            }
            Ok(owed)
        })
    })
    .await
    .map_err(|error| format!("read owed exports: {error}"))
}

/// The export transition, once, out of `pending`.
async fn set_export_state(db: &LocalDb, id: &str, state: &str) -> Result<(), String> {
    db.execute(
        "UPDATE branch_advance_acceptances
            SET export_state = ?2, exported_at = CASE WHEN ?2 = 'exported' THEN ?3 END
          WHERE id = ?1 AND export_state = 'pending'",
        params![id, state, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record the export state of acceptance {id}: {error}"))
}

/// The one settlement transition, and only out of `pending`: an `unknown` row
/// can never become accepted or abandoned, whatever a future caller asks, and
/// a fenced row cannot be accepted or abandoned until its fence is resolved.
async fn set_acceptance_status(
    db: &LocalDb,
    id: &str,
    status: &str,
    operation: Option<&str>,
) -> Result<(), String> {
    db.execute(
        "UPDATE branch_advance_acceptances
            SET status = ?2, accepted_operation = COALESCE(?3, accepted_operation),
                settled_at = CASE WHEN ?2 IN ('accepted', 'abandoned') THEN ?4 END
          WHERE id = ?1 AND status = 'pending' AND (?2 = 'unknown' OR bracket_id IS NULL)",
        params![id, status, operation, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("settle acceptance {id}: {error}"))
}

/// The immutable conflict inventory survives a quarantined retry without
/// constructing a second merge object. A newer destination bypasses it.
#[derive(serde::Serialize, serde::Deserialize)]
struct CachedMergeConflict {
    base: Option<String>,
    ours: Option<String>,
    theirs: Option<String>,
    conflicted_tip: Option<String>,
    condition: String,
    incoming: Vec<(String, String, bool)>,
    scaffolding: Option<(String, Vec<String>)>,
    validation: Option<CachedMergeValidation>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CachedMergeValidation {
    commit: String,
    failures: Vec<(String, usize, usize, String)>,
}

impl CachedMergeConflict {
    fn of(value: &crate::jj::ConflictDiagnostic) -> Self {
        Self {
            base: value.base.clone(),
            ours: value.ours.clone(),
            theirs: value.theirs.clone(),
            conflicted_tip: value.conflicted_tip.clone(),
            condition: value.condition.as_str().to_string(),
            scaffolding: value
                .scaffolding
                .as_ref()
                .map(|value| (value.commit.clone(), value.entries.clone())),
            validation: value
                .validation
                .as_ref()
                .map(|value| CachedMergeValidation {
                    commit: value.commit.clone(),
                    failures: value
                        .failures
                        .iter()
                        .map(|failure| {
                            (
                                failure.path.clone(),
                                failure.line,
                                failure.column,
                                failure.message.clone(),
                            )
                        })
                        .collect(),
                }),
            incoming: value
                .incoming
                .iter()
                .map(|file| {
                    (
                        file.path.clone(),
                        file.status.clone(),
                        file.classification == crate::jj::IncomingClassification::Conflicting,
                    )
                })
                .collect(),
        }
    }

    fn diagnostic(self) -> crate::jj::ConflictDiagnostic {
        crate::jj::ConflictDiagnostic {
            base: self.base,
            ours: self.ours,
            theirs: self.theirs,
            conflicted_tip: self.conflicted_tip,
            condition: crate::jj::ConflictCondition::from_stored(&self.condition),
            scaffolding: self
                .scaffolding
                .map(|(commit, entries)| crate::jj::ScaffoldingRefusal { commit, entries }),
            validation: self.validation.map(|value| crate::jj::ValidationRefusal {
                commit: value.commit,
                failures: value
                    .failures
                    .into_iter()
                    .map(
                        |(path, line, column, message)| cairn_vcs::parse_gate::ParseFailure {
                            path,
                            line,
                            column,
                            message,
                        },
                    )
                    .collect(),
            }),
            incoming: self
                .incoming
                .into_iter()
                .map(|(path, status, conflict)| crate::jj::IncomingFile {
                    path,
                    status,
                    classification: if conflict {
                        crate::jj::IncomingClassification::Conflicting
                    } else {
                        crate::jj::IncomingClassification::CleanOnRetry
                    },
                })
                .collect(),
        }
    }
}

/// What the base's declared merge rules make of a conflict.
enum DeclaredOutcome {
    Plan(crate::jj::DeclaredMergeEdits),
    /// The rules do not settle it.
    None,
    /// Proving the plan exceeded the request's budget.
    TimedOut,
}

enum Attempt {
    Prepared(Box<PreparedBranch>),
    Raced(String),
    /// The conflict is settled by the base's declared merge rules; recompute
    /// from a fresh capture with these pinned edits.
    Declared(crate::jj::DeclaredMergeEdits),
}

struct Context<'a> {
    orch: &'a Orchestrator,
    db: &'a LocalDb,
    /// The same database, owned, for a store-wide mutation's bracket.
    shared_db: Arc<LocalDb>,
    jj: crate::jj::JjEnv,
    store: PathBuf,
    repo_path: String,
    request: &'a PreparationRequest,
    /// The reservation this preparation holds: inherited, or taken by
    /// [`Context::ensure_reserved`]. Released when preparation returns.
    owned: std::sync::Mutex<Option<Arc<crate::orchestrator::BranchReservation>>>,
    /// The exact base commit the current attempt captured against instead of
    /// the live base, if any: the request's own or an open session's.
    pinned: std::sync::Mutex<Option<String>>,
    /// Set once a recovery this preparation took was recorded, or failed to
    /// be, so its notices are delivered after every guard is released.
    recorded: std::sync::atomic::AtomicBool,
    /// The fence this preparation recorded itself. Nothing more is spawned
    /// against the store through it: a writer it killed may still run.
    fenced_here: std::sync::Mutex<Option<String>>,
    started: std::time::Instant,
    /// What the latest attempt cost, for its replay cost sample.
    cost: std::sync::Mutex<AttemptCost>,
}

/// What one attempt has cost so far. `coordinate` is set only once the
/// attempt reaches detached computation; an attempt that never computes
/// records no sample.
#[derive(Default)]
struct AttemptCost {
    attempt_started: Option<std::time::Instant>,
    coordinate: Option<crate::jj::AdvanceCoordinate>,
    merge_mode: bool,
    allowance: Option<Duration>,
    probe: Option<crate::jj::ReplayCostProbe>,
    phases: super::replay_cost::PhaseTimings,
    compute_exhausted: bool,
    failure_phase: Option<String>,
}

/// Where a preparation ran out of time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutPhase {
    /// The already-current probe under the snapshot guard.
    Probe,
    /// Reading earlier installs' drop claims.
    InheritedDrops,
    /// Detached computation: the only exhaustion that may block a repeat.
    Compute,
    /// The candidate expired before its install, behind lock or reservation
    /// waits.
    InstallExpiry,
}

impl TimeoutPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Probe => "the already-current probe",
            Self::InheritedDrops => "reading earlier installs' drop claims",
            Self::Compute => "detached computation",
            Self::InstallExpiry => "waiting to install",
        }
    }
}

impl Context<'_> {
    fn with_cost(&self, update: impl FnOnce(&mut AttemptCost)) {
        if let Ok(mut cost) = self.cost.lock() {
            update(&mut cost);
        }
    }

    /// The effective resolution of this preparation, as a timeout key holds it.
    fn resolution(&self) -> String {
        let (take, drop) = self
            .request
            .explicit_cost
            .as_ref()
            .map_or((false, false), |cost| {
                (cost.take_committed_tip, cost.drop_incoming)
            });
        super::replay_cost::resolution_fingerprint(take, drop, self.request.carry.as_deref())
    }

    fn timeout_key(
        &self,
        coordinate: &crate::jj::AdvanceCoordinate,
        merge_mode: bool,
    ) -> super::replay_cost::TimeoutKey {
        super::replay_cost::TimeoutKey {
            project_id: self.request.project_id.clone(),
            store_path: self.store.to_string_lossy().into_owned(),
            branch: coordinate.branch.clone(),
            head: coordinate.head.clone(),
            base_branch: coordinate.base_branch.clone(),
            base: coordinate.base.clone(),
            mode: super::replay_cost::ReplayMode::of(merge_mode),
            resolution: self.resolution(),
            engine_version: crate::jj::REPLAY_ENGINE_VERSION,
        }
    }

    /// The budget the whole request was given: the explicit request's, or this
    /// preparation's own compute budget.
    fn request_budget(&self) -> Duration {
        self.request
            .explicit_cost
            .as_ref()
            .map_or(self.request.compute_budget, |cost| cost.budget)
    }

    /// Record that an explicit replay's detached computation exhausted the
    /// allowance it had at `coordinate`. The blocker refuses only an attempt
    /// that could not compute materially longer, so time this attempt lost to
    /// waits never refuses a later one with that time back.
    async fn record_timeout_blocker(
        &self,
        coordinate: &crate::jj::AdvanceCoordinate,
    ) -> Result<Option<super::replay_cost::TimeoutBlocker>, String> {
        use super::replay_cost;
        let (allowance, compute_ms, observed, merge_mode) = {
            let cost = self.cost.lock().map_err(|error| error.to_string())?;
            (
                cost.allowance,
                cost.phases.compute_ms,
                cost.probe
                    .as_ref()
                    .map(crate::jj::ReplayCostProbe::observation)
                    .unwrap_or_default(),
                cost.merge_mode,
            )
        };
        let Some(allowance) = allowance else {
            return Ok(None);
        };
        let budget = self.request_budget();
        let (host, _) = replay_cost::host_context();
        let blocker = replay_cost::TimeoutBlocker {
            key: self.timeout_key(coordinate, merge_mode),
            phase: observed.phase,
            compute_ms: compute_ms.unwrap_or_default(),
            allowance_ms: replay_cost::millis(allowance),
            budget_ms: replay_cost::millis(budget),
            observed,
            build: replay_cost::build_identity().await,
            host,
            recorded_at: chrono::Utc::now().timestamp(),
        };
        replay_cost::record_blocker(self.db, &blocker).await?;
        Ok(Some(blocker))
    }

    /// Persist what the last attempt that reached detached computation cost.
    /// A failure is logged: a sample is evidence, never a gate.
    async fn record_cost_sample(&self, result: &Result<PreparedBranch, PreparationRefusal>) {
        use super::replay_cost::{self, BudgetClass, CostOutcome, ReplayCostEvidence};
        let Ok(mut cost) = self.cost.lock().map(|mut cost| std::mem::take(&mut *cost)) else {
            return;
        };
        let Some(coordinate) = cost.coordinate.take() else {
            return;
        };
        let request = self.request;
        let started = request
            .explicit_cost
            .as_ref()
            .map_or(self.started, |explicit| explicit.started);
        cost.phases.proof_ms = request.explicit_cost.as_ref().map(|explicit| {
            replay_cost::millis(self.started.saturating_duration_since(explicit.started))
        });
        cost.phases.total_ms = replay_cost::millis(started.elapsed());
        let outcome = match result {
            Ok(prepared) if prepared.advanced => CostOutcome::Installed,
            Ok(_) => CostOutcome::AlreadyCurrent,
            Err(PreparationRefusal::Conflict(_)) => CostOutcome::Conflicted,
            Err(PreparationRefusal::TimedOut { .. }) if cost.compute_exhausted => {
                CostOutcome::ComputeExhausted
            }
            Err(PreparationRefusal::TimedOut { .. }) => CostOutcome::TimedOut,
            Err(other) => CostOutcome::Refused(other.kind().to_string()),
        };
        let (host, load1) = replay_cost::host_context();
        let evidence = ReplayCostEvidence {
            version: replay_cost::EVIDENCE_VERSION,
            engine_version: crate::jj::REPLAY_ENGINE_VERSION,
            build: replay_cost::build_identity().await,
            recorded_at: chrono::Utc::now().timestamp(),
            host,
            load1,
            branch: coordinate.branch.clone(),
            head: coordinate.head.clone(),
            base_branch: coordinate.base_branch.clone(),
            base: coordinate.base.clone(),
            mode: replay_cost::ReplayMode::of(cost.merge_mode),
            budget_class: if request.explicit_replay {
                BudgetClass::Explicit
            } else {
                BudgetClass::Background
            },
            resolution: self.resolution(),
            observed: cost
                .probe
                .as_ref()
                .map(crate::jj::ReplayCostProbe::observation)
                .unwrap_or_default(),
            phases: cost.phases,
            budget_ms: replay_cost::millis(self.request_budget()),
            compute_allowance_ms: cost.allowance.map(replay_cost::millis),
            outcome,
            failure_phase: cost.failure_phase,
        };
        if let Err(error) =
            replay_cost::record_sample(self.db, &request.project_id, &self.store, &evidence).await
        {
            log::warn!("replay cost of `{}`: {error}", request.branch);
        }
    }

    fn fence_action(&self) -> String {
        if self.request.explicit_replay {
            format!("Replaying `{}`", self.request.branch)
        } else {
            format!(
                "Preparing `{}` for a {}",
                self.request.branch,
                self.request.purpose.label()
            )
        }
    }

    /// Refuse under `_guard`, before any mutation, when a recovery fence
    /// covers any of `bookmarks` or the whole store. Checked under the lock,
    /// so a fence the previous holder recorded is seen; fails closed.
    async fn refuse_fenced(
        &self,
        _guard: &JjStoreGuard,
        bookmarks: &[&str],
    ) -> Result<(), PreparationRefusal> {
        crate::orchestrator::branch_recovery::refuse_if_fenced(
            self.db,
            &self.store,
            bookmarks,
            &self.fence_action(),
        )
        .await
        .map_err(|refusal| PreparationRefusal::Fenced { refusal })
    }

    /// [`Self::refuse_fenced`] for a store-wide mutation, which may move any
    /// bookmark: every fence on the store refuses it.
    async fn refuse_any_fence(&self, guard: &JjStoreGuard) -> Result<(), PreparationRefusal> {
        let fences = crate::orchestrator::branch_recovery::fences(self.db, &self.store)
            .await
            .map_err(|error| PreparationRefusal::Fenced {
                refusal: format!(
                    "{} refused: the branch recovery fences could not be read ({error})",
                    self.fence_action()
                ),
            })?;
        let fenced: Vec<&str> = fences
            .iter()
            .flat_map(|fence| fence.affected.iter().map(String::as_str))
            .collect();
        self.refuse_fenced(guard, &fenced).await
    }

    /// Persist one budgeted transaction's `recoveries` under `_guard`; see
    /// [`persist_preparation_recoveries`].
    async fn persist_recoveries(
        &self,
        _guard: &JjStoreGuard,
        operation: &str,
        recoveries: Vec<crate::jj::RecoveryCoordinates>,
    ) -> Option<String> {
        let fenced = persist_preparation_recoveries(
            self.db,
            &self.request.project_id,
            &self.store,
            &self.request.branch,
            self.request.explicit_replay,
            operation,
            recoveries,
        )
        .await?;
        self.recorded
            .store(true, std::sync::atomic::Ordering::SeqCst);
        *self.fenced_here.lock().unwrap() = Some(fenced.clone());
        Some(fenced)
    }

    fn take_fenced_here(&self) -> Option<String> {
        self.fenced_here.lock().unwrap().take()
    }

    fn reserved(&self) -> Option<Arc<crate::orchestrator::BranchReservation>> {
        self.owned.lock().unwrap().clone()
    }

    /// Refuse a mutation of this branch that has no reservation yet; the
    /// attempt loop reserves and retries. Call under a guard, before mutating.
    fn require_reserved(&self) -> Result<(), PreparationRefusal> {
        match self.reserved() {
            Some(_) => Ok(()),
            None => Err(PreparationRefusal::Busy(BusyReason::ReservationRequired)),
        }
    }

    /// Admission for a store-wide mutation (operation-head settlement, a base
    /// name repair, store creation): no foreign reservation may be held.
    fn refuse_foreign(&self, guard: &JjStoreGuard) -> Result<(), PreparationRefusal> {
        let owned = self.reserved();
        self.orch
            .refuse_foreign_reservations(guard, owned.as_deref())
            .map_err(|holder| PreparationRefusal::Busy(BusyReason::Reservation(holder)))
    }

    /// Refuse under `guard` when the write this preparation serves is
    /// already published.
    async fn refuse_published(&self, guard: &JjStoreGuard) -> Result<(), PreparationRefusal> {
        let Some(probe) = &self.request.write_replay else {
            return Ok(());
        };
        match probe.published_under_guard(guard).await {
            Ok(false) => Ok(()),
            Ok(true) => Err(PreparationRefusal::AlreadyPublished),
            Err(error) => Err(PreparationRefusal::Repository(format!(
                "whether this write was already published is unknown: {error}"
            ))),
        }
    }

    /// Reserve this branch, never while holding a store guard. Every purpose
    /// waits within the same bound it waits for the store lock: a reservation
    /// is held only across one preparation or publication, and the suites of
    /// one `cairn check run` prepare the same branch concurrently, so a check
    /// or run that refused at once would fail on its own sibling.
    async fn ensure_reserved(
        &self,
    ) -> Result<Arc<crate::orchestrator::BranchReservation>, PreparationRefusal> {
        if let Some(held) = self.reserved() {
            return Ok(held);
        }
        let wait = within_deadline(self.request, self.request.lock_wait);
        let purpose = format!(
            "branch preparation of `{}` for {}",
            self.request.branch,
            self.request.purpose.label()
        );
        let reservation = Arc::new(
            self.orch
                .reserve_branches(&self.store, &[self.request.branch.as_str()], &purpose, wait)
                .await
                .map_err(|holder| PreparationRefusal::Busy(BusyReason::Reservation(holder)))?,
        );
        *self.owned.lock().unwrap() = Some(reservation.clone());
        Ok(reservation)
    }

    async fn attempt(
        &self,
        attempt: u32,
        declared: Option<Arc<crate::jj::DeclaredMergeEdits>>,
    ) -> Result<Attempt, PreparationRefusal> {
        let request = self.request;
        let branch = request.branch.as_str();
        self.with_cost(|cost| {
            *cost = AttemptCost {
                attempt_started: Some(std::time::Instant::now()),
                ..AttemptCost::default()
            }
        });

        *self.pinned.lock().unwrap() = request.exact_destination.clone();
        // Phase 1: snapshot under the writer lock.
        let guard = self.lock("snapshot").await?;
        self.refuse_published(&guard).await?;
        // Resolve mode even on the already-current and bound-session paths.
        // This read owns no connection once it returns; snapshot and jj work
        // below never run inside a database closure.
        let merge_mode = crate::orchestrator::catch_up::mode_for(
            self.db,
            &request.project_id,
            &self.store,
            branch,
        )
        .await
        .map_err(PreparationRefusal::Repository)?
        .is_some();
        // Before every repair and export recovery this phase may make.
        self.refuse_fenced(&guard, &[branch]).await?;
        let relationship = self.relationship().await?;
        // Under the snapshot guard, before any path can answer: the install
        // re-checks under its own guard, since compute runs without one.
        self.refuse_held_branch(&relationship).await?;
        if relationship.is_direct_target(branch) {
            return self.direct_target(guard, relationship, attempt).await;
        }
        let (guard, snapshot) = self.capture(guard, &relationship).await?;
        let (guard, snapshot) = self
            .pin_session_base(guard, &relationship, snapshot)
            .await?;
        let coordinate = snapshot.coordinate().clone();
        if let Some(expected) = &request.expected_head {
            if *expected != coordinate.head {
                drop(guard);
                return Err(PreparationRefusal::Moved {
                    what: format!(
                        "`{branch}` is at {}, not the {expected} this preparation is for",
                        coordinate.head
                    ),
                });
            }
        }
        // Settlement first: an unsettled earlier install blocks every writer of
        // this branch. An owner's commit or merge is refused; a check or run
        // may still judge an up-to-date coordinate, flagged, but never installs.
        let debt = self.settle_acceptance_debt(&coordinate.head).await?;
        if let Some(debt) = &debt {
            if request.purpose.actionable() {
                return Err(PreparationRefusal::Busy(BusyReason::AcceptanceDebt {
                    acceptance_id: debt.id.clone(),
                    status: debt.status.clone(),
                }));
            }
        }
        // Then the exports accepted installs still owe, before the fast path
        // can answer: a just-settled row's export is covered too. Only on a
        // debt-free branch; with a row owed they are reported, not attempted.
        let (guard, recovered_export) = self
            .recover_pending_export(guard, &coordinate.head, debt.is_none())
            .await?;
        if let Some(refusal) = self.take_fenced_here() {
            drop(guard);
            return Err(PreparationRefusal::Fenced { refusal });
        }
        // The hot path: already on its base with nothing to heal. Answered from
        // the captured index under this same guard, before any repair,
        // cooldown, session or compute work.
        let (guard, (snapshot, prepared)) = {
            let budget = within_deadline(request, request.compute_budget);
            under_guard(guard, "check already prepared", move || {
                let prepared = snapshot.already_prepared(budget);
                (snapshot, prepared)
            })
            .await?
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            // The probe ran out of the request's budget: the same typed timeout
            // and coordinate-keyed cooldown as a timed-out replay.
            Err(crate::jj::AdvanceComputeError::TimedOut) => {
                drop(guard);
                return self
                    .timed_out(&relationship, &coordinate, TimeoutPhase::Probe)
                    .await;
            }
            Err(crate::jj::AdvanceComputeError::Repository(error)) => {
                return Err(PreparationRefusal::Repository(error));
            }
            Err(other) => {
                return Err(PreparationRefusal::Repository(format!(
                    "the already-prepared probe refused: {other:?}"
                )));
            }
        };
        if let Some(tree) = prepared {
            drop(guard);
            // Reported only: the branch write permit is the one gate, and it
            // refuses every writer while the row is owed.
            let warnings = match &debt {
                Some(debt) => vec![format!(
                    "acceptance {} of this branch is {}; no writer may publish onto it until it \
                     is settled",
                    debt.id, debt.status
                )],
                None => Vec::new(),
            };
            return Ok(Attempt::Prepared(Box::new(PreparedBranch {
                warnings,
                project_id: request.project_id.clone(),
                store: self.store.clone(),
                branch: branch.to_string(),
                head_before: coordinate.head.clone(),
                pinned_base: self.is_pinned(&coordinate.base),
                head_after: coordinate.head,
                tree,
                relationship,
                base_commit: coordinate.base,
                advanced: false,
                export: recovered_export,
                attempts: attempt,
                session: None,
            })));
        }
        if let Some(debt) = debt {
            return Err(PreparationRefusal::Busy(BusyReason::AcceptanceDebt {
                acceptance_id: debt.id,
                status: debt.status,
            }));
        }
        // A replay is needed. First the same safe repair the locked replay
        // always made: track origin's copy of this branch when it shares the
        // branch's lineage. It tracks only a remote the local bookmark equals or
        // descends from, so the bookmark's own target never moves (a check may
        // do it); it is metadata on one branch, not a store-wide import, and it
        // runs only once the branch owes no acceptance (refused above). Detached computation never touches tracking, so a
        // repair that changed anything is followed by a fresh capture.
        self.require_reserved()?;
        let (jj, store, own) = (self.jj.clone(), self.store.clone(), branch.to_string());
        let (guard, tracked) = under_guard(guard, "track own remote", move || {
            crate::jj::track_own_remote_of_same_lineage(&jj, &store, &own)
        })
        .await?;
        let (guard, snapshot) = match tracked {
            Ok(true) => {
                drop(snapshot);
                let (guard, snapshot) = self.capture(guard, &relationship).await?;
                if snapshot.coordinate() != &coordinate {
                    return Ok(Attempt::Raced(
                        "tracking origin's copy moved the captured coordinate".to_string(),
                    ));
                }
                (guard, snapshot)
            }
            Ok(false) => (guard, snapshot),
            Err(error) => {
                log::warn!("branch preparation could not track origin's `{branch}`: {error}");
                (guard, snapshot)
            }
        };
        if let Some(quarantine) =
            load_reconcile_quarantine(self.db, &request.project_id, &self.store, branch)
                .await
                .map_err(PreparationRefusal::Repository)?
        {
            // Demand preparation respects the coordinate-keyed timeout
            // cooldown; an owner's explicit replay request is never held by it.
            if !request.explicit_replay
                && quarantine.failure_kind == crate::jj::RECONCILE_TIMEOUT
                && quarantine_blocks_automatic_replay(
                    &quarantine,
                    Some(&coordinate.head),
                    // `replay_is_explicit` keys on this label's prefix, so it
                    // must not read as an owner's request.
                    &format!("automatic preparation of {branch}"),
                )
            {
                let cooldown = PreparationRefusal::Cooldown {
                    fingerprint: quarantine.fingerprint.clone(),
                    retry_at: quarantine.retry_at(),
                };
                drop(guard);
                return self
                    .with_catch_up_pending(
                        merge_mode,
                        &relationship,
                        &coordinate,
                        attempt,
                        recovered_export,
                        Err(cooldown),
                    )
                    .await;
            }
        }
        // An ordinary resolution commit proceeds under its recorded session;
        // an explicit replay request is asking for the catch-up itself.
        if request.purpose == PreparationPurpose::OwnerCommit && !request.explicit_replay {
            let (returned, bound) = self.bound_session(guard, &coordinate, false).await?;
            if let Some(bound) = bound {
                let (returned, tree) = self.tree_of(returned, &coordinate.head).await?;
                drop(returned);
                return Ok(Attempt::Prepared(Box::new(PreparedBranch {
                    project_id: request.project_id.clone(),
                    store: self.store.clone(),
                    branch: branch.to_string(),
                    head_before: coordinate.head.clone(),
                    pinned_base: self.is_pinned(&coordinate.base),
                    head_after: coordinate.head,
                    tree,
                    relationship,
                    base_commit: coordinate.base,
                    advanced: false,
                    export: recovered_export,
                    attempts: attempt,
                    session: Some(bound),
                    warnings: Vec::new(),
                })));
            }
            return self
                .continue_attempt(
                    attempt,
                    returned,
                    relationship,
                    snapshot,
                    recovered_export,
                    declared,
                    merge_mode,
                )
                .await;
        }
        self.continue_attempt(
            attempt,
            guard,
            relationship,
            snapshot,
            recovered_export,
            declared,
            merge_mode,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // One captured phase state, carried unchanged into compute/install.
    async fn continue_attempt(
        &self,
        attempt: u32,
        guard: JjStoreGuard,
        relationship: BaseRelationship,
        snapshot: crate::jj::BranchAdvanceSnapshot,
        recovered_export: PreparedExport,
        declared: Option<Arc<crate::jj::DeclaredMergeEdits>>,
        merge_mode: bool,
    ) -> Result<Attempt, PreparationRefusal> {
        let request = self.request;
        let branch = request.branch.as_str();
        let coordinate = snapshot.coordinate().clone();
        // The branch would move from here on: before any cached conflict, a
        // session, a wake or compute, a parent mid-conflict holds it.
        self.refuse_parent_conflict(&relationship).await?;
        if merge_mode {
            if let Some(quarantine) =
                load_reconcile_quarantine(self.db, &request.project_id, &self.store, branch)
                    .await
                    .map_err(PreparationRefusal::Repository)?
            {
                if quarantine.observed_tip() == coordinate.head
                    && quarantine.observed_dest() == Some(coordinate.base.as_str())
                    && (!request.explicit_replay
                        || quarantine.failure_kind == crate::jj::MERGE_CATCH_UP_REFUSED)
                {
                    let cached = self.merge_quarantine_payload().await?;
                    if let Ok(cached) = serde_json::from_str::<CachedMergeConflict>(&cached) {
                        drop(guard);
                        let conflict = self
                            .conflicted(&relationship, &coordinate, cached.diagnostic(), true)
                            .await;
                        return self
                            .with_catch_up_pending(
                                merge_mode,
                                &relationship,
                                &coordinate,
                                attempt,
                                recovered_export,
                                conflict,
                            )
                            .await;
                    }
                    return Err(PreparationRefusal::Repository(cached));
                }
            }
        }
        // A plan is pinned to the coordinate it was proven at; the engine also
        // refuses a mismatch, but a moved coordinate is a race, not an error.
        if let Some(edits) = &declared {
            if edits.expected_head != coordinate.head || edits.expected_base != coordinate.base {
                return Ok(Attempt::Raced(format!(
                    "the declared merge plan was proven at {} onto {}, but the branch is at {} \
                     onto {}",
                    edits.expected_head, edits.expected_base, coordinate.head, coordinate.base
                )));
            }
        }
        let declared_applied = declared.is_some();
        if let Some(carry) = &request.carry {
            if carry.expected_pre_tip != coordinate.head {
                return Err(PreparationRefusal::Moved {
                    what: format!(
                        "the resolution carry was computed for {}, but `{branch}` is at {}",
                        carry.expected_pre_tip, coordinate.head
                    ),
                });
            }
        }
        // A carry planned onto one base can drop another base's incoming
        // hunks; the caller re-plans at the new exact coordinate.
        if let Some(planned) = &request.carry_base {
            if *planned != coordinate.base {
                return Err(PreparationRefusal::Moved {
                    what: format!(
                        "the resolution carry was planned onto {planned}, but base `{}` is at {}",
                        coordinate.base_branch, coordinate.base
                    ),
                });
            }
        }
        // An owner's explicit replay identical to one whose computation
        // already exhausted its budget would do the same work under the same
        // budget: refused here, before the dependent traversal and every
        // expensive step after it. Recovery and pending exports were handled
        // above, so they still take precedence.
        if request.explicit_replay {
            let key = self.timeout_key(&coordinate, merge_mode);
            let allowance = within_deadline(request, request.compute_budget);
            if let Some(blocker) = super::replay_cost::matching_blocker(self.db, &key, allowance)
                .await
                .map_err(PreparationRefusal::Repository)?
            {
                drop(guard);
                return Err(PreparationRefusal::RepeatTimeout(Box::new(blocker)));
            }
        }
        // Only now is a replay certain: the fast path and the bound-session
        // shortcut have both passed, so an up-to-date branch never pays for,
        // or is refused by, the dependent traversal.
        let snapshot = snapshot.with_dependent_bookmarks(self.dependents().await?);
        // Drop claims of earlier accepted installs this head descends from
        // travel into this attempt's row, so a chain of local advances keeps
        // every claim until the branch publishes.
        let (guard, snapshot, inherited) = match self.inherited_drops(guard, snapshot).await? {
            Inherited::Claims(guard, snapshot, claims) => (guard, *snapshot, claims),
            Inherited::TimedOut => {
                return self
                    .timed_out(&relationship, &coordinate, TimeoutPhase::InheritedDrops)
                    .await
            }
        };
        drop(guard);

        // Phase 2: detached computation, no lock held. The worker is awaited
        // to completion; its deadline is cooperative and it publishes nothing.
        let jj = self.jj.clone();
        let carry = request.carry.clone();
        let budget = within_deadline(request, request.compute_budget);
        let probe = crate::jj::ReplayCostProbe::default();
        let snapshot = snapshot.with_cost_probe(probe.clone());
        self.with_cost(|cost| {
            cost.phases.capture_ms = cost
                .attempt_started
                .map(|started| super::replay_cost::millis(started.elapsed()));
            cost.coordinate = Some(coordinate.clone());
            cost.merge_mode = merge_mode;
            cost.allowance = Some(budget);
            cost.probe = Some(probe);
        });
        // Fault injection: the computation itself gets no time, while the
        // allowance recorded above stays the real one.
        #[cfg(test)]
        let budget = if tests::compute_starved(&self.store) {
            Duration::ZERO
        } else {
            budget
        };
        let compute_started = std::time::Instant::now();
        #[cfg(test)]
        let hook_store = self.store.clone();
        let computed = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            tests::during_compute(&hook_store);
            if merge_mode {
                crate::jj::compute_branch_merge_advance(&jj, snapshot, carry.as_deref(), budget)
            } else {
                crate::jj::compute_branch_advance_with_declared(
                    &jj,
                    snapshot,
                    carry.as_deref(),
                    declared.as_deref(),
                    budget,
                )
            }
        })
        .await
        .map_err(|error| PreparationRefusal::Repository(format!("preparation worker: {error}")))?;
        self.with_cost(|cost| {
            cost.phases.compute_ms = Some(super::replay_cost::millis(compute_started.elapsed()));
        });

        let candidate = match computed {
            Ok(candidate) => candidate,
            Err(crate::jj::AdvanceComputeError::BaseConflicted { base }) => {
                return Err(PreparationRefusal::BaseConflicted {
                    base_branch: coordinate.base_branch,
                    base,
                })
            }
            Err(crate::jj::AdvanceComputeError::Repository(error)) => {
                if merge_mode && error.contains(crate::jj::MERGE_CATCH_UP_REFUSAL) {
                    let guard = self.lock("record merge refusal").await?;
                    if let Some(raced) = self.moved_since(&relationship, &coordinate).await? {
                        return Ok(Attempt::Raced(raced));
                    }
                    self.refuse_fenced(&guard, &[branch]).await?;
                    self.record_merge_quarantine(
                        &coordinate,
                        crate::jj::MERGE_CATCH_UP_REFUSED,
                        &error,
                    )
                    .await?;
                    drop(guard);
                    let mut report = crate::jj::ReconcileReport::default();
                    report
                        .merge_refused
                        .insert(branch.to_string(), error.clone());
                    if request.purpose.actionable() {
                        crate::orchestrator::catch_up::announce_reconcile_outcome(
                            self.orch,
                            self.db,
                            &request.project_id,
                            &self.store,
                            branch,
                            &relationship.base_branch,
                            &report,
                        )
                        .await;
                    }
                }
                return Err(PreparationRefusal::Repository(error));
            }
            Err(crate::jj::AdvanceComputeError::TimedOut) => {
                self.with_cost(|cost| cost.compute_exhausted = true);
                let timed_out = self
                    .timed_out(&relationship, &coordinate, TimeoutPhase::Compute)
                    .await;
                return self
                    .with_catch_up_pending(
                        merge_mode,
                        &relationship,
                        &coordinate,
                        attempt,
                        recovered_export,
                        timed_out,
                    )
                    .await;
            }
            Err(crate::jj::AdvanceComputeError::AmbiguousOwnership { commit }) => {
                return Err(PreparationRefusal::AmbiguousOwnership {
                    branch: branch.to_string(),
                    commit,
                });
            }
            Err(crate::jj::AdvanceComputeError::CoordinatesChanged) => {
                return Ok(Attempt::Raced(
                    "the declared plan or carry no longer matches the captured head and base"
                        .to_string(),
                ));
            }
            Err(crate::jj::AdvanceComputeError::Busy) => {
                return Err(PreparationRefusal::Busy(BusyReason::StoreMaintenance(
                    "store maintenance holds the objects the replay needs".to_string(),
                )));
            }
            // Evidence the engine could not account for refuses; it is never
            // read as an answer.
            Err(
                error @ (crate::jj::AdvanceComputeError::UnknownEvidence { .. }
                | crate::jj::AdvanceComputeError::UnaccountedRemoteCommits { .. }
                | crate::jj::AdvanceComputeError::EvidenceLimit { .. }),
            ) => {
                return Err(PreparationRefusal::Repository(error.to_string()));
            }
            Err(crate::jj::AdvanceComputeError::Conflict(diagnostic)) => {
                let diagnostic = *diagnostic;
                let mut assess_basis = true;
                if !merge_mode && !declared_applied {
                    match self.declared_plan(&coordinate, &diagnostic).await? {
                        DeclaredOutcome::Plan(edits) => return Ok(Attempt::Declared(edits)),
                        // The plan only saves the owner a resolution; failing to
                        // prove one in time leaves an ordinary conflict, never a
                        // stranded commit. The session-basis assessment would
                        // re-read the basis this probe just ran out of time on,
                        // so the reopened session keeps the earlier basis.
                        DeclaredOutcome::TimedOut => {
                            log::warn!(
                                "declared merge plan for `{branch}` timed out at {}:{}; \
                                 recording the conflict with the earlier resolution basis",
                                coordinate.head,
                                coordinate.base
                            );
                            assess_basis = false;
                        }
                        DeclaredOutcome::None => {}
                    }
                }
                let conflict = self
                    .conflicted(&relationship, &coordinate, diagnostic, assess_basis)
                    .await;
                return self
                    .with_catch_up_pending(
                        merge_mode,
                        &relationship,
                        &coordinate,
                        attempt,
                        recovered_export,
                        conflict,
                    )
                    .await;
            }
        };
        let tree = candidate.tree().map_err(PreparationRefusal::Repository)?;
        if candidate.head() == coordinate.head {
            // Already on its base with nothing to heal: the snapshot taken
            // under the lock is the coherent coordinate. Nothing replays.
            return Ok(Attempt::Prepared(Box::new(PreparedBranch {
                project_id: request.project_id.clone(),
                store: self.store.clone(),
                branch: branch.to_string(),
                head_before: coordinate.head.clone(),
                pinned_base: self.is_pinned(&coordinate.base),
                head_after: coordinate.head,
                tree,
                relationship,
                base_commit: coordinate.base,
                advanced: false,
                export: recovered_export,
                attempts: attempt,
                session: None,
                warnings: Vec::new(),
            })));
        }

        #[cfg(test)]
        tests::before_install(&self.store, attempt);

        // Phase 3: install under the writer lock, holding the branch's
        // reservation (taken before the lock).
        let install_started = std::time::Instant::now();
        self.ensure_reserved().await?;
        let guard = self.lock("install").await?;
        self.refuse_published(&guard).await?;
        let current = self.relationship().await?;
        if !current.same_base(&relationship) {
            return Ok(Attempt::Raced(relationship_change(&relationship, &current)));
        }
        self.refuse_held_branch(&current).await?;
        self.refuse_parent_conflict(&current).await?;
        self.refuse_fenced(&guard, &[branch]).await?;
        let current_merge_mode = crate::orchestrator::catch_up::mode_for(
            self.db,
            &request.project_id,
            &self.store,
            branch,
        )
        .await
        .map_err(PreparationRefusal::Repository)?
        .is_some();
        if current_merge_mode != merge_mode {
            return Ok(Attempt::Raced(
                "the branch catch-up mode changed during computation".to_string(),
            ));
        }
        // The journal row is written before the local install, so an install
        // that lands and a crash before it is recorded still leaves a row the
        // next preparation settles from the literal head.
        let acceptance = self
            .record_pending_acceptance(&candidate, &inherited)
            .await?;
        let jj = self.jj.clone();
        let (guard, local) = under_guard(guard, "install prepared branch", move || {
            crate::jj::install_branch_advance_local(&jj, candidate)
        })
        .await?;
        let local = match local {
            Ok(local) => local,
            Err(crate::jj::AdvanceInstallError::CoordinatesChanged) => {
                self.abandon_acceptance(&acceptance).await;
                return Ok(Attempt::Raced(format!(
                    "`{branch}` or `{}` moved during computation",
                    coordinate.base_branch
                )));
            }
            Err(crate::jj::AdvanceInstallError::Expired) => {
                self.abandon_acceptance(&acceptance).await;
                drop(guard);
                return self
                    .timed_out(&relationship, &coordinate, TimeoutPhase::InstallExpiry)
                    .await;
            }
            // Whether the install committed is unknown: the row stays pending
            // and the next preparation settles it from the literal head.
            Err(crate::jj::AdvanceInstallError::Repository(error)) => {
                return Err(PreparationRefusal::Repository(error));
            }
        };
        // From here the install is accepted: nothing below may turn it into a
        // refusal. Bookkeeping that fails is reported on the coordinate.
        self.with_cost(|cost| {
            cost.phases.install_ms = Some(super::replay_cost::millis(install_started.elapsed()));
        });
        let export_started = std::time::Instant::now();
        let mut warnings = Vec::new();
        // A changed candidate always commits an operation; none means the
        // install did not happen as journaled, so the row stays pending and
        // the literal head settles it.
        let accepted = match local.accepted_operation.as_deref() {
            Some(operation) => self.accept_acceptance(&acceptance, operation).await,
            None => Err("the install reported no operation".to_string()),
        };
        if let Err(error) = accepted {
            warnings.push(format!(
                "the install is accepted but its journal row {acceptance} is still pending \
                 ({error}); no writer may publish onto the branch until it is settled"
            ));
        }
        // The lease stays alive through the journal transition and the export.
        let jj = self.jj.clone();
        #[cfg(test)]
        let inject_export_failure = tests::export_fails(&self.store);
        #[cfg(not(test))]
        let inject_export_failure = false;
        let (guard, installed) = under_guard(guard, "export prepared branch", move || {
            if inject_export_failure {
                // A crash between acceptance and export: jj holds the
                // install and Git never saw it.
                return crate::jj::InstalledBranchAdvance {
                    coordinate: local.coordinate,
                    head: local.head,
                    tree: local.tree,
                    export: Err("injected export failure".to_string()),
                    healing: local.healing,
                    dropped_commit_ids: local.dropped_commit_ids,
                    accepted_operation: local.accepted_operation,
                    recoveries: Vec::new(),
                };
            }
            crate::jj::export_installed_branch_advance(&jj, local)
        })
        .await?;
        let mut installed = installed;
        let recoveries = std::mem::take(&mut installed.recoveries);
        if let Some(fenced) = self
            .persist_recoveries(&guard, "export of the installed advance", recoveries)
            .await
        {
            installed.export = Err(fenced);
        }
        let (guard, export) = self.settle_export(guard, &installed, &acceptance).await;
        self.with_cost(|cost| {
            cost.phases.export_ms = Some(super::replay_cost::millis(export_started.elapsed()));
        });
        // The install stands; a fence left by its export is reported on it,
        // and every publication of the branch refuses until it is repaired.
        if let Some(fenced) = self.take_fenced_here() {
            warnings.push(fenced);
        }
        // The branch absorbed this base: whatever session it had is finished,
        // and a timeout cooldown at the old head no longer describes it.
        keep(
            &mut warnings,
            "close the absorbed conflict session",
            close_open_sessions_for_branch(self.db, branch).await,
        );
        keep(
            &mut warnings,
            "release the timeout cooldown",
            release_reconcile_quarantine(self.db, &request.project_id, &self.store, branch).await,
        );
        keep(
            &mut warnings,
            "release the explicit replay timeout blockers",
            super::replay_cost::clear_blockers(self.db, &request.project_id, &self.store, branch)
                .await,
        );
        let jobs = match load_on_branch_jobs(self.db, &request.project_id, branch).await {
            Ok(jobs) => jobs,
            Err(error) => {
                keep(&mut warnings, "load the branch's jobs", Err(error));
                Vec::new()
            }
        };
        for job in &jobs {
            keep(
                &mut warnings,
                "record the durable base",
                advance_sibling_durable_base(self.db, job, &installed.coordinate.base).await,
            );
        }
        drop(guard);

        self.after_install(&jobs, &relationship, &installed, &mut warnings)
            .await;
        Ok(Attempt::Prepared(Box::new(PreparedBranch {
            project_id: request.project_id.clone(),
            store: self.store.clone(),
            branch: branch.to_string(),
            head_before: installed.coordinate.head.clone(),
            head_after: installed.head.clone(),
            tree: installed.tree.clone(),
            relationship,
            pinned_base: self.is_pinned(&installed.coordinate.base),
            base_commit: installed.coordinate.base.clone(),
            advanced: true,
            export,
            attempts: attempt,
            session: None,
            warnings,
        })))
    }

    /// Lock-free follow-up of an accepted install, each step bounded: refresh
    /// held checkouts to the captured head (never re-resolving the moving
    /// name), keep or withdraw review waves by the patch rule, and leave the
    /// owner one passive note.
    async fn after_install(
        &self,
        jobs: &[super::SiblingJob],
        relationship: &BaseRelationship,
        installed: &crate::jj::InstalledBranchAdvance,
        warnings: &mut Vec<String>,
    ) {
        let request = self.request;
        let branch = request.branch.as_str();
        match tokio::time::timeout(
            RESIDENCY_REFRESH_BUDGET,
            refresh_residencies_for_branch(
                self.orch,
                self.db,
                &request.project_id,
                branch,
                &installed.head,
            ),
        )
        .await
        {
            Ok(0) => {}
            Ok(failed) => warnings.push(format!(
                "{failed} held checkout(s) of `{branch}` were not refreshed to {}",
                installed.head
            )),
            Err(_) => warnings.push(format!(
                "refreshing held checkouts of `{branch}` exceeded {}s",
                RESIDENCY_REFRESH_BUDGET.as_secs()
            )),
        }

        // A replay that provably kept the branch's own patch keeps a running
        // review wave; a changed or unreadable comparison withdraws it.
        let jj = self.jj.with_operation_budget(PATCH_COMPARE_BUDGET);
        let (store, before, after, base) = (
            self.store.clone(),
            installed.coordinate.head.clone(),
            installed.head.clone(),
            installed.coordinate.base.clone(),
        );
        let merge_mode = crate::orchestrator::catch_up::mode_for(
            self.db,
            &request.project_id,
            &self.store,
            branch,
        )
        .await
        .ok()
        .flatten()
        .is_some();
        let patch_unchanged = (merge_mode && request.carry.is_none())
            || tokio::time::timeout(
                PATCH_COMPARE_BUDGET,
                tokio::task::spawn_blocking(move || {
                    replay_kept_patch(&jj, &store, Some(&before), Some(&after), &base)
                }),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false);
        crate::execution::checks::cancel_stale_review_on_branch_advance_except(
            self.orch,
            &relationship.owner_job_id,
            crate::execution::checks::BranchAdvance::BaseReplay { patch_unchanged },
            request.capturing_review_of.as_deref(),
        )
        .await;

        if merge_mode && installed.head != installed.coordinate.base {
            let mut report = crate::jj::ReconcileReport::default();
            let jj = self.jj.with_operation_budget(PATCH_COMPARE_BUDGET);
            let (store, before, base) = (
                self.store.clone(),
                installed.coordinate.head.clone(),
                installed.coordinate.base.clone(),
            );
            let old_base = tokio::task::spawn_blocking(move || {
                let jj = jj.pin_immutable_read(&store, &[before.as_str(), base.as_str()])?;
                crate::jj::merge_bases(&jj, &store, &before, &base)
            })
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(|bases| bases.into_iter().next());
            if let Some(old_base) = old_base {
                report.merged_clean.insert(
                    branch.to_string(),
                    crate::jj::MergedCatchUp {
                        merge: installed.head.clone(),
                        old_base,
                        new_base: installed.coordinate.base.clone(),
                        resolved: request.carry.is_some(),
                    },
                );
                crate::orchestrator::catch_up::announce_reconcile_outcome(
                    self.orch,
                    self.db,
                    &request.project_id,
                    &self.store,
                    branch,
                    &relationship.base_branch,
                    &report,
                )
                .await;
            }
        } else {
            self.notify_clean(jobs, relationship, installed);
        }
    }

    /// The exact coordinate of a branchless job's integration target: its
    /// literal head, captured under the lock, with the target as its own base.
    async fn direct_target(
        &self,
        guard: JjStoreGuard,
        relationship: BaseRelationship,
        attempt: u32,
    ) -> Result<Attempt, PreparationRefusal> {
        let branch = self.request.branch.as_str();
        let head = cairn_vcs::resolve_coordinate(&self.store, branch)
            .await
            .map_err(|error| PreparationRefusal::Unresolved {
                coordinate: branch.to_string(),
                reason: error.to_string(),
            })?;
        if let Some(expected) = &self.request.expected_head {
            if *expected != head {
                drop(guard);
                return Err(PreparationRefusal::Moved {
                    what: format!(
                        "`{branch}` is at {head}, not the {expected} this preparation is for"
                    ),
                });
            }
        }
        let (guard, tree) = self.tree_of(guard, &head).await?;
        drop(guard);
        Ok(Attempt::Prepared(Box::new(PreparedBranch {
            project_id: self.request.project_id.clone(),
            store: self.store.clone(),
            branch: branch.to_string(),
            head_before: head.clone(),
            head_after: head.clone(),
            tree,
            relationship,
            base_commit: head,
            pinned_base: false,
            advanced: false,
            export: PreparedExport::NotNeeded,
            attempts: attempt,
            session: None,
            warnings: Vec::new(),
        })))
    }

    /// Create the project store under the writer lock, for an owner's demand.
    async fn initialize_store(&self) -> Result<(), PreparationRefusal> {
        let guard = self.lock("initialize store").await?;
        self.refuse_published(&guard).await?;
        self.refuse_foreign(&guard)?;
        let (jj, store, repo) = (
            self.jj.clone(),
            self.store.clone(),
            PathBuf::from(&self.repo_path),
        );
        // Store-wide: the import can move other branches' bookmarks, so it
        // runs inside the bracket that marks any moved owed branch unknown.
        let owned = self.reserved();
        let (guard, ()) = self
            .orch
            .bracket_store_mutation(
                guard,
                owned.as_deref(),
                self.shared_db.clone(),
                &self.request.project_id,
                move || {
                    if crate::jj::is_jj_dir(&store) {
                        return Ok(());
                    }
                    crate::jj::ensure_project_store(&jj, &store, &repo).map(|_| ())
                },
            )
            .await
            .map_err(|error| PreparationRefusal::Unresolved {
                coordinate: self.repo_path.clone(),
                reason: format!("the project store could not be created: {error}"),
            })?;
        drop(guard);
        Ok(())
    }

    /// The open session an owner's commit at `coordinate` resolves: recorded
    /// for this project, store and exact base coordinate, with the head at or
    /// past the tip it was recorded at. A session recorded against an older
    /// base binds nothing; the commit then prepares, and a conflict reopens
    /// the session at the current base. `any_base` drops only the exact-base
    /// requirement, for a merge-mode commit whose catch-up stays pending (see
    /// [`Self::with_catch_up_pending`]).
    async fn bound_session(
        &self,
        guard: JjStoreGuard,
        coordinate: &crate::jj::AdvanceCoordinate,
        any_base: bool,
    ) -> Result<(JjStoreGuard, Option<BoundSession>), PreparationRefusal> {
        let Some(session) = load_active_session(self.db, &self.request.branch)
            .await
            .map_err(PreparationRefusal::Repository)?
        else {
            return Ok((guard, None));
        };
        let store_path = self.store.to_string_lossy();
        let Some(ours) = session.ours.clone() else {
            return Ok((guard, None));
        };
        if session.project_id != self.request.project_id
            || session.store_path != store_path
            || session.target_branch != coordinate.base_branch
            || (!any_base && session.destination_commit != coordinate.base)
            || !session.version_is_current()
        {
            return Ok((guard, None));
        }
        let bound = BoundSession {
            intent_id: session.intent_id.clone(),
            diagnostic_version: session.diagnostic_version,
            fingerprint: session.fingerprint(),
            ours: ours.clone(),
        };
        if ours == coordinate.head {
            return Ok((guard, Some(bound)));
        }
        let (jj, store, head) = (self.jj.clone(), self.store.clone(), coordinate.head.clone());
        let (guard, descends) = under_guard(guard, "bind resolution commit", move || {
            crate::jj::revset_descends_from(&jj, &store, &head, &ours)
        })
        .await?;
        Ok((guard, descends.then_some(bound)))
    }

    /// Whether this attempt captured against `base` as an exact commit rather
    /// than the base's live target.
    fn is_pinned(&self, base: &str) -> bool {
        self.pinned.lock().unwrap().as_deref() == Some(base)
    }

    /// The open session's recorded base, when it is still this branch's next
    /// catch-up step: recorded for this project, store and base, an ancestor
    /// of the live base, and not yet in the branch's ancestry. A commit the
    /// store no longer knows (a rewritten base) is no step at all, and the
    /// branch is judged against the live base as before.
    async fn session_base_ahead(
        &self,
        coordinate: &crate::jj::AdvanceCoordinate,
    ) -> Result<Option<ConflictSession>, PreparationRefusal> {
        let Some(session) = load_active_session(self.db, &self.request.branch)
            .await
            .map_err(PreparationRefusal::Repository)?
        else {
            return Ok(None);
        };
        if session.project_id != self.request.project_id
            || session.store_path != self.store.to_string_lossy()
            || session.target_branch != coordinate.base_branch
            || session.destination_commit == coordinate.base
        {
            return Ok(None);
        }
        let recorded = session.destination_commit.as_str();
        let on_base = cairn_vcs::history::contains(&self.store, recorded, &coordinate.base).await;
        let landed = cairn_vcs::history::contains(&self.store, recorded, &coordinate.head).await;
        match (on_base, landed) {
            (Ok(true), Ok(false)) => Ok(Some(session)),
            (Ok(_), Ok(_)) => Ok(None),
            (Err(error), _) | (_, Err(error)) => {
                log::warn!(
                    "branch preparation of `{}`: whether session base {recorded} is still ahead \
                     could not be read ({error}); preparing against the live base",
                    self.request.branch
                );
                Ok(None)
            }
        }
    }

    /// An open conflict session's recorded base X is the branch's next
    /// catch-up step until it lands, so an owner's commit is judged against X
    /// even after the base moved on. A resolution then binds the session at X
    /// (or lands on X), and a conflict refreshes that same `(base, X)` session
    /// instead of recording one against the newer base and superseding the
    /// resolution's own. The newer base's commits are a later round.
    ///
    /// A landing (`OwnerMerge`) still needs the live base, so with an older
    /// session open it is refused naming that session and records nothing:
    /// at most one session is open per branch, the oldest that has not
    /// landed. Checks and runs judge the live base untouched.
    async fn pin_session_base(
        &self,
        guard: JjStoreGuard,
        relationship: &BaseRelationship,
        snapshot: crate::jj::BranchAdvanceSnapshot,
    ) -> Result<(JjStoreGuard, crate::jj::BranchAdvanceSnapshot), PreparationRefusal> {
        let request = self.request;
        if request.exact_destination.is_some() || !request.purpose.actionable() {
            return Ok((guard, snapshot));
        }
        let coordinate = snapshot.coordinate().clone();
        let Some(session) = self.session_base_ahead(&coordinate).await? else {
            return Ok((guard, snapshot));
        };
        let recorded = session.destination_commit.clone();
        if request.purpose == PreparationPurpose::OwnerMerge {
            drop(snapshot);
            drop(guard);
            return Err(PreparationRefusal::Conflict(Box::new(BaseConflict {
                branch: request.branch.clone(),
                head: coordinate.head,
                base_branch: coordinate.base_branch,
                base_commit: recorded,
                paths: session
                    .conflicting()
                    .map(|file| file.path.clone())
                    .collect(),
                record: ConflictRecord::Session {
                    intent_id: session.intent_id,
                    reopened: false,
                },
            })));
        }
        drop(snapshot);
        log::info!(
            "branch preparation of `{}`: judged against its open session's base {recorded}, \
             not `{}` at {}",
            request.branch,
            coordinate.base_branch,
            coordinate.base
        );
        *self.pinned.lock().unwrap() = Some(recorded);
        let (guard, captured) = self
            .capture_once(guard, &request.branch, &relationship.base_branch)
            .await?;
        let snapshot = captured.map_err(|error| capture_refusal(error, &self.store))?;
        Ok((guard, snapshot))
    }

    /// An owner's ordinary commit on a merge-mode branch whose catch-up could
    /// not be installed: published on the branch's own head under its open
    /// conflict session, with the catch-up merge left pending.
    ///
    /// In merge mode the catch-up is one extra merge commit on top of the
    /// owner's history and never rewrites it, so the owner's commit does not
    /// depend on the merge at all: holding it until the merge succeeds only
    /// stops the owner (and every child merged into an integration branch)
    /// while the base keeps moving. A refusal is kept when it is not about the
    /// catch-up (a fence, a busy store, a moved branch), when this is not an
    /// owner's ordinary commit, or when no open session the head descends from
    /// records that the catch-up is outstanding; a merge-mode branch with no
    /// session has nothing telling its owner the merge is pending. The
    /// refusal's cooldown or session record stands as written, and the next
    /// preparation retries the merge as before.
    async fn with_catch_up_pending(
        &self,
        merge_mode: bool,
        relationship: &BaseRelationship,
        coordinate: &crate::jj::AdvanceCoordinate,
        attempt: u32,
        export: PreparedExport,
        outcome: Result<Attempt, PreparationRefusal>,
    ) -> Result<Attempt, PreparationRefusal> {
        let refusal = match outcome {
            Ok(attempt) => return Ok(attempt),
            Err(refusal) => refusal,
        };
        let request = self.request;
        let about_the_catch_up = match &refusal {
            PreparationRefusal::Conflict(conflict) => {
                matches!(conflict.record, ConflictRecord::Session { .. })
            }
            PreparationRefusal::TimedOut { .. } | PreparationRefusal::Cooldown { .. } => true,
            _ => false,
        };
        if !merge_mode
            || request.purpose != PreparationPurpose::OwnerCommit
            || request.explicit_replay
            || !about_the_catch_up
        {
            return Err(refusal);
        }
        let branch = request.branch.as_str();
        let guard = self.lock("publish with the catch-up pending").await?;
        if let Some(raced) = self.moved_since(relationship, coordinate).await? {
            return Ok(Attempt::Raced(raced));
        }
        self.refuse_fenced(&guard, &[branch]).await?;
        let (guard, bound) = self.bound_session(guard, coordinate, true).await?;
        let Some(bound) = bound else {
            return Err(refusal);
        };
        let (guard, tree) = self.tree_of(guard, &coordinate.head).await?;
        drop(guard);
        let pending = format!(
            "`{branch}` catches up by merge: this commit lands on its head under conflict \
             session {}, and the merge onto `{}` at {} is still pending ({refusal})",
            bound.intent_id, coordinate.base_branch, coordinate.base
        );
        log::info!("catch-up[{branch}] owner commit published with the merge pending: {pending}");
        Ok(Attempt::Prepared(Box::new(PreparedBranch {
            project_id: request.project_id.clone(),
            store: self.store.clone(),
            branch: branch.to_string(),
            head_before: coordinate.head.clone(),
            head_after: coordinate.head.clone(),
            tree,
            relationship: relationship.clone(),
            base_commit: coordinate.base.clone(),
            pinned_base: self.is_pinned(&coordinate.base),
            advanced: false,
            export,
            attempts: attempt,
            session: Some(bound),
            warnings: vec![pending],
        })))
    }

    async fn tree_of(
        &self,
        guard: JjStoreGuard,
        commit: &str,
    ) -> Result<(JjStoreGuard, String), PreparationRefusal> {
        let git_dir = crate::jj::store_git_dir(&self.store).ok_or_else(|| {
            PreparationRefusal::Repository(format!(
                "no backing Git directory for {}",
                self.store.display()
            ))
        })?;
        let spec = format!("{commit}^{{tree}}");
        let (guard, output) = under_guard(guard, "read prepared tree", move || {
            crate::env::git()
                .arg("--git-dir")
                .arg(&git_dir)
                .args(["rev-parse", "--verify", &spec])
                .output()
        })
        .await?;
        let output = output.map_err(|error| PreparationRefusal::Repository(error.to_string()))?;
        if !output.status.success() {
            return Err(PreparationRefusal::Repository(format!(
                "read the tree of {commit}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok((
            guard,
            String::from_utf8_lossy(&output.stdout).trim().to_string(),
        ))
    }

    async fn lock(&self, phase: &str) -> Result<JjStoreGuard, PreparationRefusal> {
        // An explicit replay's locked phases (snapshot, install, export) run
        // under its larger budget; its detached computation holds no lock. The
        // hold line names the explicit replay and the budget it ran under.
        let purpose = if self.request.explicit_replay {
            format!(
                "explicit replay, budget={}s",
                self.request.compute_budget.as_secs()
            )
        } else {
            self.request.purpose.label().to_string()
        };
        let operation = format!(
            "branch preparation ({phase}) of `{}` for {purpose}",
            self.request.branch
        );
        self.orch
            .acquire_jj_store_lock_with_timeout(
                &self.store,
                StoreLockClass::Foreground,
                operation,
                Some(within_deadline(self.request, self.request.lock_wait)),
            )
            .await
            .map_err(|()| {
                PreparationRefusal::Busy(BusyReason::StoreLock {
                    holder: self
                        .orch
                        .store_lock_status(&self.store)
                        .map(|status| status.describe_holder()),
                })
            })
    }

    async fn relationship(&self) -> Result<BaseRelationship, PreparationRefusal> {
        load_relationship(
            self.db,
            &self.request.project_id,
            &self.request.branch,
            self.request.owner_job_id.as_deref(),
            self.request.target_override.as_deref(),
        )
        .await
    }

    /// An owner's commit or merge is excluded by in-flight run work on its
    /// branch on every path that hands back a coordinate it will write onto,
    /// the up-to-date and session-bound answers included. A check or run is a
    /// snapshot, never a writer: the writer's branch permit decides later.
    /// The invoking batch's own verified sealed publication is the single
    /// exemption.
    async fn refuse_held_branch(
        &self,
        relationship: &BaseRelationship,
    ) -> Result<(), PreparationRefusal> {
        if !self.request.purpose.actionable() {
            return Ok(());
        }
        let request = self.request;
        let exempt = request
            .sealed
            .as_ref()
            .filter(|sealed| sealed.is_for(&request.project_id, &self.store, &request.branch))
            .map(|sealed| (sealed.job_id(), sealed.request_id()));
        match super::inflight_run_batch_except(
            self.db,
            &request.project_id,
            &request.branch,
            Some(&relationship.owner_job_id),
            exempt,
        )
        .await
        {
            Ok(None) => Ok(()),
            Ok(Some(batch)) => Err(PreparationRefusal::Busy(BusyReason::RunBatch(
                batch.describe(),
            ))),
            Err(error) => Err(PreparationRefusal::Repository(error)),
        }
    }

    /// Every purpose that would move the branch is held while its immediate
    /// parent's own conflict session is open ([`held_by_parent_conflict`]).
    /// Checked where a move becomes certain and again under the install guard,
    /// since a parent's session can open during computation.
    async fn refuse_parent_conflict(
        &self,
        relationship: &BaseRelationship,
    ) -> Result<(), PreparationRefusal> {
        match held_by_parent_conflict(
            self.db,
            &self.store,
            &self.request.branch,
            &relationship.base_branch,
        )
        .await
        {
            Ok(None) => Ok(()),
            Ok(Some(hold)) => Err(PreparationRefusal::HeldByParentConflict(Box::new(hold))),
            Err(error) => Err(PreparationRefusal::Repository(error)),
        }
    }

    /// Capture both coordinates, repairing a conflicted branch or base NAME
    /// once under the lock this phase already holds. Pure reads never repair;
    /// this explicit demand applies the same authority the mutating resolver
    /// always has: the store for a managed branch, origin for the default.
    /// Settle this branch's unsettled acceptance rows against its literal head,
    /// under the caller's guard. Exact head comparisons only: `head_after`
    /// may be a commit that already existed (the base itself), so reachability
    /// is never acceptance proof. Returns the row still owed, if any.
    async fn settle_acceptance_debt(
        &self,
        head: &str,
    ) -> Result<Option<AcceptanceDebt>, PreparationRefusal> {
        settle_acceptances_at(
            self.db,
            &self.request.project_id,
            &self.store,
            &self.request.branch,
            head,
        )
        .await
        .map_err(PreparationRefusal::Repository)
    }

    /// The union of drop claims of accepted rows whose head this attempt's
    /// starting head descends from, read from the captured snapshot.
    async fn inherited_drops(
        &self,
        guard: JjStoreGuard,
        snapshot: crate::jj::BranchAdvanceSnapshot,
    ) -> Result<Inherited, PreparationRefusal> {
        // Anchors only (ids and heads, no claim text); the newest reachable
        // one's union already carries every earlier ancestor's claims, so
        // its claims are the only ones read.
        let anchors = accepted_anchors(
            self.db,
            &self.request.project_id,
            &self.store,
            &self.request.branch,
        )
        .await
        .map_err(PreparationRefusal::Repository)?;
        if anchors.is_empty() {
            return Ok(Inherited::Claims(guard, Box::new(snapshot), Vec::new()));
        }
        let heads: Vec<String> = anchors.iter().map(|(_, head)| head.clone()).collect();
        let budget = within_deadline(self.request, DEPENDENT_LOOKUP_BUDGET);
        let (guard, (snapshot, reachable)) = under_guard(guard, "inherit drop claims", move || {
            let reachable = snapshot.reachable_from_head(&heads, budget);
            (snapshot, reachable)
        })
        .await?;
        let reachable = match reachable {
            Ok(reachable) => reachable,
            Err(crate::jj::AdvanceComputeError::TimedOut) => return Ok(Inherited::TimedOut),
            Err(other) => {
                return Err(PreparationRefusal::Repository(format!(
                    "read which accepted installs this head carries: {other:?}"
                )))
            }
        };
        // Newest first: the first reachable anchor is the one to inherit.
        let Some((anchor, _)) = anchors
            .into_iter()
            .find(|(_, head_after)| reachable.contains(head_after))
        else {
            return Ok(Inherited::Claims(guard, Box::new(snapshot), Vec::new()));
        };
        // The reset: a confirmed publication recorded on the anchor that this
        // head descends from means origin holds none of what earlier claims
        // covered, so nothing is inherited. Not descending from it (origin was
        // rewound or moved externally) keeps the full claims.
        let checkpoint = acceptance_checkpoint(self.db, &anchor)
            .await
            .map_err(PreparationRefusal::Repository)?;
        let (guard, snapshot, reset) = match checkpoint {
            None => (guard, snapshot, false),
            Some(published) => {
                let budget = within_deadline(self.request, DEPENDENT_LOOKUP_BUDGET);
                let probe = vec![published.clone()];
                let (guard, (snapshot, reachable)) =
                    under_guard(guard, "read the publication checkpoint", move || {
                        let reachable = snapshot.reachable_from_head(&probe, budget);
                        (snapshot, reachable)
                    })
                    .await?;
                match reachable {
                    Ok(reachable) => (guard, snapshot, reachable.contains(&published)),
                    Err(crate::jj::AdvanceComputeError::TimedOut) => {
                        return Ok(Inherited::TimedOut)
                    }
                    Err(other) => {
                        return Err(PreparationRefusal::Repository(format!(
                            "read the publication checkpoint: {other:?}"
                        )))
                    }
                }
            }
        };
        if reset {
            return Ok(Inherited::Claims(guard, Box::new(snapshot), Vec::new()));
        }
        let claims = acceptance_claims(self.db, &anchor)
            .await
            .map_err(PreparationRefusal::Repository)?;
        if claims.len() > INHERITED_DROP_CAP {
            // Refused before install: a row that silently lost claims would
            // let a later publication fail to prove what it replaced. There
            // is no claims reset yet, so this bound is the outcome.
            return Err(PreparationRefusal::Repository(format!(
                "`{}` has accumulated more than {INHERITED_DROP_CAP} removed-from-branch claims \
                 since its last confirmed publication; publish it (a confirmed publication \
                 resets the claims), then prepare it again",
                self.request.branch
            )));
        }
        Ok(Inherited::Claims(guard, Box::new(snapshot), claims))
    }

    async fn record_pending_acceptance(
        &self,
        candidate: &crate::jj::BranchAdvanceCandidate,
        inherited: &[String],
    ) -> Result<String, PreparationRefusal> {
        let id = format!("acceptance-{}", uuid::Uuid::new_v4());
        let coordinate = candidate.coordinate();
        // The row's own claims (newly empty commits and commits a squash
        // receipt represents) count with what it inherits: the next install
        // inherits all of them, and the reader stops by name past the byte
        // bound. Refused here, before the install, so nothing lands with a
        // claim set a later publication could not read back whole.
        let claimed = candidate.dropped_commit_ids().len() + inherited.len();
        if claimed > INHERITED_DROP_CAP {
            return Err(PreparationRefusal::Repository(format!(
                "replaying `{}` would leave {claimed} removed-from-branch claims on it, more than \
                 {INHERITED_DROP_CAP}; publish it (a confirmed publication resets the claims), \
                 then prepare it again",
                self.request.branch
            )));
        }
        let dropped = serde_json::to_string(candidate.dropped_commit_ids())
            .map_err(|error| PreparationRefusal::Repository(error.to_string()))?;
        let inherited = serde_json::to_string(inherited)
            .map_err(|error| PreparationRefusal::Repository(error.to_string()))?;
        if (dropped.len() + inherited.len()) as i64 > ACCEPTED_CLAIM_BYTES {
            return Err(PreparationRefusal::Repository(format!(
                "replaying `{}` would record more than {ACCEPTED_CLAIM_BYTES} bytes of \
                 removed-from-branch claims; publish it, then prepare it again",
                self.request.branch
            )));
        }
        let store_key = journal_store_key(&self.store).map_err(PreparationRefusal::Repository)?;
        self.db
            .execute(
                "INSERT INTO branch_advance_acceptances
                   (id, project_id, store_path, branch, head_before, head_after, base_branch,
                    base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
                    status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'pending', ?12)",
                params![
                    id.as_str(),
                    self.request.project_id.as_str(),
                    store_key.as_str(),
                    self.request.branch.as_str(),
                    coordinate.head.as_str(),
                    candidate.head(),
                    coordinate.base_branch.as_str(),
                    coordinate.base.as_str(),
                    coordinate.operation.as_str(),
                    dropped,
                    inherited,
                    chrono::Utc::now().timestamp()
                ],
            )
            .await
            .map_err(|error| {
                PreparationRefusal::Repository(format!(
                    "record the pending acceptance; nothing was installed: {error}"
                ))
            })?;
        Ok(id)
    }

    async fn accept_acceptance(&self, id: &str, operation: &str) -> Result<(), String> {
        #[cfg(test)]
        if tests::acceptance_write_fails(&self.store) {
            return Err("injected acceptance write failure".to_string());
        }
        set_acceptance_status(self.db, id, "accepted", Some(operation)).await
    }

    /// A refused install leaves the branch at its starting head; a failure to
    /// record that leaves the row pending, which settles the same way later.
    async fn abandon_acceptance(&self, id: &str) {
        if let Err(error) = set_acceptance_status(self.db, id, "abandoned", None).await {
            log::warn!("abandon acceptance {id}: {error}");
        }
    }

    /// The branches that declare this one as their base (transitively), read
    /// under the caller's guard. Ownership evidence for a replay only: never
    /// replayed, enqueued or notified, and never part of the install
    /// comparison. Read only once a replay will actually run.
    async fn dependents(&self) -> Result<Vec<String>, PreparationRefusal> {
        let deadline =
            std::time::Instant::now() + within_deadline(self.request, DEPENDENT_LOOKUP_BUDGET);
        dependent_branches(
            self.db,
            &self.request.project_id,
            &self.request.branch,
            deadline,
        )
        .await
        .map_err(PreparationRefusal::Repository)
    }

    async fn capture(
        &self,
        guard: JjStoreGuard,
        relationship: &BaseRelationship,
    ) -> Result<(JjStoreGuard, crate::jj::BranchAdvanceSnapshot), PreparationRefusal> {
        let branch = self.request.branch.clone();
        let base = relationship.base_branch.clone();
        let (mut guard, captured) = self.capture_once(guard, &branch, &base).await?;
        let mut error = match captured {
            Ok(snapshot) => return Ok((guard, snapshot)),
            Err(error) => error,
        };
        if let crate::jj::AdvanceCaptureError::UnsettledOperations { heads } = &error {
            // Concurrent operations left more than one head. Settling them is
            // the same merge jj's own loader performs, done here explicitly
            // under the lock rather than by any read. Settlement unions the
            // heads' targets (a bookmark set differently by each becomes a
            // conflicted name) and never chooses one, so a check may settle.
            let heads = *heads;
            self.refuse_any_fence(&guard).await?;
            self.refuse_foreign(&guard)?;
            self.require_reserved()?;
            let (jj, store) = (self.jj.clone(), self.store.clone());
            // Store-wide: settlement can move other branches' bookmarks, so it
            // runs inside the bracket that marks any moved owed branch unknown.
            let owned = self.reserved();
            let (returned, settled) = self
                .orch
                .bracket_store_mutation(
                    guard,
                    owned.as_deref(),
                    self.shared_db.clone(),
                    &self.request.project_id,
                    move || crate::jj::settle_operation_heads(&jj, &store),
                )
                .await
                .map_err(|reason| PreparationRefusal::Unresolved {
                    coordinate: self.store.display().to_string(),
                    reason: format!(
                        "its {heads} concurrent operations could not be settled: {reason}"
                    ),
                })?;
            guard = returned;
            log::warn!(
                "branch preparation of `{branch}` settled {} operation heads into {}",
                settled.before.len(),
                settled.after
            );
            let (returned, captured) = self.capture_once(guard, &branch, &base).await?;
            guard = returned;
            error = match captured {
                Ok(snapshot) => return Ok((guard, snapshot)),
                Err(error) => error,
            };
        }
        let name = match error {
            crate::jj::AdvanceCaptureError::UnresolvedCoordinate { name } => name,
            other => return Err(capture_refusal(other, &self.store)),
        };
        // One named bookmark did not resolve to a single commit. Only a
        // conflicted NAME is repairable; absence is an answer.
        match cairn_vcs::resolve_coordinate(&self.store, &name).await {
            Err(cairn_vcs::CoordinateResolutionError::Conflicted { .. }) => {}
            Err(cairn_vcs::CoordinateResolutionError::Absent { .. }) => {
                return Err(PreparationRefusal::Absent { coordinate: name });
            }
            Err(other) => {
                return Err(PreparationRefusal::Unresolved {
                    coordinate: name,
                    reason: other.to_string(),
                });
            }
            Ok(commit) => {
                return Err(PreparationRefusal::Unresolved {
                    coordinate: name,
                    reason: format!("capture refused it, though it now resolves to {commit}"),
                });
            }
        }
        // Repairing a name chooses one of its targets, which changes the
        // bookmark, so a check or run refuses before it, for every name
        // including the default (whose repair belongs to origin sync); an
        // owner's commit or merge repairs.
        if !self.request.purpose.actionable() {
            return Err(PreparationRefusal::Unresolved {
                coordinate: name,
                reason: format!(
                    "its name has competing targets; a {} never repairs it, the owner's next \
                     commit or merge does",
                    self.request.purpose.label()
                ),
            });
        }
        // A repair chooses a target, so it must never run on a name with an
        // outstanding acceptance: choosing that row's candidate head would
        // let settlement accept what nothing proved. Refused before moving.
        if let Some(debt) =
            outstanding_acceptances(self.db, &self.request.project_id, &self.store, &name)
                .await
                .map_err(PreparationRefusal::Repository)?
                .into_iter()
                .next()
        {
            return Err(PreparationRefusal::Busy(BusyReason::AcceptanceDebt {
                acceptance_id: debt.id,
                status: debt.status,
            }));
        }
        // Repairing this branch's own name needs its reservation; repairing
        // the base's moves a branch it does not hold.
        if name == branch {
            self.require_reserved()?;
        } else {
            self.refuse_foreign(&guard)?;
        }
        self.refuse_fenced(&guard, &[name.as_str()]).await?;
        let authority = if name == relationship.project_default {
            crate::jj::BranchAuthority::Origin
        } else {
            crate::jj::BranchAuthority::Store
        };
        let (jj, store, target) = (self.jj.clone(), self.store.clone(), name.clone());
        let (guard, repair) = under_guard(guard, "conflicted branch name repair", move || {
            crate::jj::repair_conflicted_branch_name(&jj, &store, &target, authority)
                .map_err(|error| error.to_string())
        })
        .await?;
        repair.map_err(|reason| PreparationRefusal::Unresolved {
            coordinate: name.clone(),
            reason: format!("its competing targets could not be repaired: {reason}"),
        })?;
        // A repaired branch name may leave the base conflicted too; the next
        // attempt repairs that one, bounded by the attempt loop.
        let (guard, captured) = self.capture_once(guard, &branch, &base).await?;
        match captured {
            Ok(snapshot) => Ok((guard, snapshot)),
            Err(error) => Err(capture_refusal(error, &self.store)),
        }
    }

    async fn capture_once(
        &self,
        guard: JjStoreGuard,
        branch: &str,
        base: &str,
    ) -> Result<
        (
            JjStoreGuard,
            Result<crate::jj::BranchAdvanceSnapshot, crate::jj::AdvanceCaptureError>,
        ),
        PreparationRefusal,
    > {
        let destination = self.pinned.lock().unwrap().clone();
        // Only an owner's commit catches up onto a session's recorded base; a
        // check, a run or a landing judges the live base.
        if destination.is_some() && self.request.purpose != PreparationPurpose::OwnerCommit {
            return Err(PreparationRefusal::Repository(
                "an exact destination is only for an owner's commit".to_string(),
            ));
        }
        let (store, branch, base) = (self.store.clone(), branch.to_string(), base.to_string());
        under_guard(
            guard,
            "capture branch and base",
            move || match destination {
                Some(destination) => {
                    crate::jj::capture_branch_onto(&store, &branch, &base, &destination)
                }
                None => crate::jj::capture_branch_advance(&store, &branch, &base),
            },
        )
        .await
    }

    /// Bring owed exports up to date: the journal's accepted installs, then
    /// any legacy reconcile-item record retirement left for the branch.
    async fn recover_pending_export(
        &self,
        guard: JjStoreGuard,
        head: &str,
        debt_free: bool,
    ) -> Result<(JjStoreGuard, PreparedExport), PreparationRefusal> {
        let (guard, journal) = self.recover_journal_exports(guard, head, debt_free).await?;
        let (guard, legacy) = self.recover_legacy_export(guard, head).await?;
        // The worse outcome is reported: a failure anywhere is a failure.
        let outcome = match (journal, legacy) {
            (failed @ PreparedExport::Failed { .. }, _)
            | (_, failed @ PreparedExport::Failed { .. }) => failed,
            (PreparedExport::Exported, _) | (_, PreparedExport::Exported) => {
                PreparedExport::Exported
            }
            _ => PreparedExport::NotNeeded,
        };
        Ok((guard, outcome))
    }

    /// Legacy: an export retirement recorded as a reconcile item for an
    /// old-worker install. No new install writes one; this drains them.
    async fn recover_legacy_export(
        &self,
        guard: JjStoreGuard,
        head: &str,
    ) -> Result<(JjStoreGuard, PreparedExport), PreparationRefusal> {
        let pending = match pending_exports(
            self.db,
            &self.request.project_id,
            &self.store,
            &self.request.branch,
        )
        .await
        {
            Ok(pending) => pending,
            // Unknown is not "none": an accepted install may still be waiting
            // for its export, so the coordinate says so.
            Err(error) => {
                log::warn!(
                    "branch preparation: pending exports of `{}` unreadable: {error}",
                    self.request.branch
                );
                return Ok((
                    guard,
                    PreparedExport::Failed {
                        error: format!("whether an earlier export is pending is unknown: {error}"),
                        recorded: None,
                    },
                ));
            }
        };
        if pending.is_empty() {
            return Ok((guard, PreparedExport::NotNeeded));
        }
        // Re-exporting moves this branch's Git ref.
        self.require_reserved()?;
        let mut guard = guard;
        let mut outcome = PreparedExport::NotNeeded;
        for (intent_id, observed) in pending {
            if observed != head {
                // A later operation moved the bookmark and owns its own
                // export; this record no longer describes the branch.
                let _ = set_item_outcome(
                    self.db,
                    &intent_id,
                    &self.request.branch,
                    OUTCOME_EXPORT_SUPERSEDED,
                )
                .await;
                continue;
            }
            let (returned, exported) = self.export(guard, head).await;
            guard = returned;
            outcome = match exported {
                Ok(()) => {
                    let _ = set_item_outcome(
                        self.db,
                        &intent_id,
                        &self.request.branch,
                        OUTCOME_EXPORTED,
                    )
                    .await;
                    PreparedExport::Exported
                }
                Err(error) => PreparedExport::Failed {
                    error,
                    recorded: Some(intent_id),
                },
            };
        }
        Ok((guard, outcome))
    }

    async fn export(&self, guard: JjStoreGuard, head: &str) -> (JjStoreGuard, Result<(), String>) {
        // A writer an earlier export killed may still run: spawn nothing more.
        if let Some(fenced) = self.fenced_here.lock().unwrap().clone() {
            return (guard, Err(fenced));
        }
        #[cfg(test)]
        if tests::export_fails(&self.store) {
            return (guard, Err("injected export failure".to_string()));
        }
        let jj = self.jj.with_operation_budget(EXPORT_RECOVERY_BUDGET);
        let ledger = jj.clone();
        let (store, branch, head) = (
            self.store.clone(),
            self.request.branch.clone(),
            head.to_string(),
        );
        // Construct the result outside so a join failure still returns a guard
        // the caller can drop deliberately.
        let outcome = under_guard(guard, "recover prepared branch export", move || {
            // Exports only while the bookmark is exactly this head.
            crate::jj::export_accepted_branch(&jj, &store, &branch, &head)
        })
        .await;
        let (guard, result) = match outcome {
            Ok((guard, result)) => (guard, result),
            Err(error) => {
                // The worker panicked and released the guard with it; take the
                // lock again so the caller's phase keeps its exclusion.
                let guard = self
                    .lock("export recovery")
                    .await
                    .expect("re-acquire the writer lock after a failed export worker");
                (guard, Err(error.to_string()))
            }
        };
        match self
            .persist_recoveries(&guard, "export recovery", ledger.take_recoveries())
            .await
        {
            Some(fenced) => (guard, Err(fenced)),
            None => (guard, result),
        }
    }

    /// Retry a failed export once under the same guard, then keep a durable
    /// pending-export record the next preparation recovers.
    /// Record the export of this install in its journal row. The row has said
    /// `pending` since before the install, so a failure here, or a crash
    /// before it, leaves a durable record the next preparation recovers.
    async fn settle_export(
        &self,
        guard: JjStoreGuard,
        installed: &crate::jj::InstalledBranchAdvance,
        acceptance: &str,
    ) -> (JjStoreGuard, PreparedExport) {
        let branch = &self.request.branch;
        let (guard, outcome) = match &installed.export {
            Ok(()) => (guard, Ok(())),
            // A fenced export is not retried: its writer may still run.
            Err(fenced) if self.fenced_here.lock().unwrap().is_some() => {
                (guard, Err(fenced.clone()))
            }
            Err(first) => {
                let (guard, retried) = self.export(guard, &installed.head).await;
                (
                    guard,
                    retried.map_err(|error| format!("{first}; retry: {error}")),
                )
            }
        };
        match outcome {
            Ok(()) => {
                if let Err(error) = set_export_state(self.db, acceptance, "exported").await {
                    // The export landed; the row still says pending, so the
                    // next preparation re-verifies it, which is harmless.
                    log::warn!("record the export of acceptance {acceptance}: {error}");
                }
                (guard, PreparedExport::Exported)
            }
            Err(error) => {
                log::error!(
                    "branch preparation installed `{branch}` at {} but could not export it: \
                     {error}",
                    installed.head
                );
                if let Err(error) = mark_publication_unconfirmed(
                    self.orch,
                    self.db,
                    &self.request.project_id,
                    branch,
                )
                .await
                {
                    log::error!("downgrade the cached verdict of `{branch}`: {error}");
                }
                (
                    guard,
                    PreparedExport::Failed {
                        error,
                        recorded: Some(acceptance.to_string()),
                    },
                )
            }
        }
    }

    /// Export every accepted install of this branch whose export is still
    /// owed, while the literal head is exactly its `head_after`; a row whose
    /// branch has since moved is superseded (that writer owns its export).
    async fn recover_journal_exports(
        &self,
        guard: JjStoreGuard,
        head: &str,
        debt_free: bool,
    ) -> Result<(JjStoreGuard, PreparedExport), PreparationRefusal> {
        let owed = match accepted_unexported(
            self.db,
            &self.request.project_id,
            &self.store,
            &self.request.branch,
        )
        .await
        {
            Ok(owed) => owed,
            // Unknown is not "none".
            Err(error) => {
                return Ok((
                    guard,
                    PreparedExport::Failed {
                        error: format!("whether an accepted export is owed is unknown: {error}"),
                        recorded: None,
                    },
                ))
            }
        };
        if !debt_free {
            if let Some((id, _)) = owed.into_iter().next() {
                return Ok((
                    guard,
                    PreparedExport::Failed {
                        error: "an accepted export is owed but waits for the branch's acceptance \
                                to settle"
                            .to_string(),
                        recorded: Some(id),
                    },
                ));
            }
            return Ok((guard, PreparedExport::NotNeeded));
        }
        let mut guard = guard;
        let mut outcome = PreparedExport::NotNeeded;
        for (id, head_after) in owed {
            if head_after != head {
                if let Err(error) = set_export_state(self.db, &id, "superseded").await {
                    log::warn!("mark the export of acceptance {id} superseded: {error}");
                }
                continue;
            }
            // A check or run never waits for the branch's reservation: the
            // owed export is reported and left for the next owner demand.
            if self.reserved().is_none() && !self.request.purpose.actionable() {
                outcome = PreparedExport::Failed {
                    error: "an accepted export is owed; a check or run does not hold the \
                            branch's reservation to perform it"
                        .to_string(),
                    recorded: Some(id),
                };
                continue;
            }
            self.require_reserved()?;
            let (returned, exported) = self.export(guard, head).await;
            guard = returned;
            outcome = match exported {
                Ok(()) => {
                    if let Err(error) = set_export_state(self.db, &id, "exported").await {
                        log::warn!("record the export of acceptance {id}: {error}");
                    }
                    PreparedExport::Exported
                }
                Err(error) => PreparedExport::Failed {
                    error,
                    recorded: Some(id),
                },
            };
        }
        Ok((guard, outcome))
    }

    async fn merge_quarantine_payload(&self) -> Result<String, PreparationRefusal> {
        let (project, store, branch) = (
            self.request.project_id.clone(),
            self.store.to_string_lossy().into_owned(),
            self.request.branch.clone(),
        );
        self.db.read(move |conn| Box::pin(async move {
            let mut rows = conn.query("SELECT last_diagnostic FROM jj_reconcile_quarantines WHERE project_id=?1 AND store_path=?2 AND bookmark=?3", params![project, store, branch]).await?;
            Ok(match rows.next().await? { Some(row) => row.text(0)?, None => String::new() })
        })).await.map_err(|error| PreparationRefusal::Repository(error.to_string()))
    }

    async fn record_merge_quarantine(
        &self,
        coordinate: &crate::jj::AdvanceCoordinate,
        kind: &str,
        diagnostic: &str,
    ) -> Result<(), PreparationRefusal> {
        let now = chrono::Utc::now().timestamp();
        self.db.execute(
            "INSERT INTO jj_reconcile_quarantines (project_id,store_path,bookmark,failure_kind,fingerprint,last_diagnostic,strike_count,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,1,?7,?7)
             ON CONFLICT(project_id,store_path,bookmark) DO UPDATE SET failure_kind=excluded.failure_kind,fingerprint=excluded.fingerprint,last_diagnostic=excluded.last_diagnostic,updated_at=excluded.updated_at",
            params![self.request.project_id.as_str(), self.store.to_string_lossy().as_ref(), self.request.branch.as_str(), kind,
                format!("{}:{}", coordinate.head, coordinate.base), diagnostic, now],
        ).await.map_err(|error| PreparationRefusal::Repository(error.to_string()))?;
        Ok(())
    }

    async fn timed_out(
        &self,
        relationship: &BaseRelationship,
        coordinate: &crate::jj::AdvanceCoordinate,
        phase: TimeoutPhase,
    ) -> Result<Attempt, PreparationRefusal> {
        let guard = self.lock("record timeout").await?;
        if let Some(raced) = self.moved_since(relationship, coordinate).await? {
            return Ok(Attempt::Raced(raced));
        }
        let budget = self.request.compute_budget;
        let error = format!(
            "{}: detached preparation exceeded its {}s budget in {}",
            crate::jj::RECONCILE_TIMEOUT,
            budget.as_secs(),
            phase.label()
        );
        self.with_cost(|cost| {
            let reached = cost
                .probe
                .as_ref()
                .and_then(|probe| probe.observation().phase)
                .filter(|_| phase == TimeoutPhase::Compute);
            cost.failure_phase = Some(match reached {
                Some(reached) => format!("{} ({})", phase.label(), reached.label()),
                None => phase.label().to_string(),
            });
        });
        let mut blocker = None;
        if self.request.explicit_replay {
            // Only the explicit budget's own compute exhaustion blocks an
            // identical explicit repeat; the automatic cooldown never does.
            let blocked = if phase == TimeoutPhase::Compute {
                self.record_timeout_blocker(coordinate).await
            } else {
                Ok(None)
            };
            match blocked {
                Ok(Some(recorded)) => {
                    log::warn!(
                        "explicit replay of `{}` exhausted its compute budget; an identical \
                         repeat is now refused: {error}",
                        self.request.branch
                    );
                    blocker = Some(Box::new(recorded));
                }
                Ok(None) => log::warn!(
                    "explicit replay of `{}` timed out without a cooldown or blocker: {error}",
                    self.request.branch
                ),
                Err(record) => log::warn!(
                    "explicit replay of `{}` timed out ({error}); its blocker could not be \
                     recorded: {record}",
                    self.request.branch
                ),
            }
        } else {
            record_reconcile_timeout(
                self.db,
                &self.request.project_id,
                &self.store,
                &self.request.branch,
                &coordinate.head,
                &coordinate.base,
                &error,
            )
            .await
            .map_err(PreparationRefusal::Repository)?;
        }
        drop(guard);
        Err(PreparationRefusal::TimedOut {
            head: coordinate.head.clone(),
            base_branch: coordinate.base_branch.clone(),
            base_commit: coordinate.base.clone(),
            budget,
            blocker,
        })
    }

    /// The base's declared element-wise merges, proven off-lock from the
    /// pinned sides of `diagnostic`: the same proof the locked replay used. A
    /// plan exists only when every conflicting path is declared and proven.
    async fn declared_plan(
        &self,
        coordinate: &crate::jj::AdvanceCoordinate,
        diagnostic: &crate::jj::ConflictDiagnostic,
    ) -> Result<DeclaredOutcome, PreparationRefusal> {
        use crate::orchestrator::conflict_session::{bounded_store_assessment, AssessmentScope};
        // An unreadable session must not silently drop the basis the plan
        // would be proven against.
        let session = load_active_session(self.db, &self.request.branch)
            .await
            .map_err(PreparationRefusal::Repository)?;
        let Some(fork) = diagnostic.base.clone() else {
            return Ok(DeclaredOutcome::None);
        };
        let paths = diagnostic.conflicting_paths().len();
        let (store, branch, diagnostic_owned, head, base) = (
            self.store.clone(),
            self.request.branch.clone(),
            diagnostic.clone(),
            coordinate.head.clone(),
            coordinate.base.clone(),
        );
        // The same bounded, off-runtime basis assessment the replay uses, at
        // the pinned head and base, under what is left of this request's
        // budget. Expiry is a timeout, never a stored-basis fallback.
        // Pinned to the captured operation: an unpinned read could settle
        // divergent operation heads outside the store guard.
        let planned = bounded_store_assessment(
            &self.jj.at_operation(&coordinate.operation),
            within_deadline(self.request, self.request.compute_budget),
            AssessmentScope {
                phase: "declared merge plan",
                branch: &self.request.branch,
                candidate_paths: paths,
            },
            move |jj| {
                let baseline =
                    super::replay_baseline_at(jj, &store, &branch, session.as_ref(), &head, &base)?
                        .unwrap_or_else(|| super::ReplayBaseline::fork_only(&fork));
                let rules = crate::orchestrator::landing::merge_rules_at(jj, &store, &base);
                super::prepare_structural_merge(jj, &store, &diagnostic_owned, &baseline, &rules)
            },
        )
        .await;
        let plan = match planned {
            Ok(Some(plan)) => plan,
            Ok(None) => return Ok(DeclaredOutcome::None),
            Err(error) if crate::jj::is_jj_timeout_error(&error) => {
                return Ok(DeclaredOutcome::TimedOut);
            }
            // A plan that cannot be proven leaves the conflict a conflict:
            // nothing is installed, and the session or evidence says so.
            Err(error) => {
                log::warn!(
                    "declared merge plan for `{}` unreadable: {error}",
                    self.request.branch
                );
                return Ok(DeclaredOutcome::None);
            }
        };
        if plan.tip != coordinate.head || plan.destination != coordinate.base {
            return Ok(DeclaredOutcome::None);
        }
        let edits = plan
            .edits
            .into_iter()
            .filter_map(|edit| match edit {
                crate::orchestrator::landing::bind::FileEdit::Rewrite { path, content } => {
                    Some((path, content))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if edits.len() != plan.paths.len() {
            return Ok(DeclaredOutcome::None);
        }
        Ok(DeclaredOutcome::Plan(crate::jj::DeclaredMergeEdits {
            expected_head: coordinate.head.clone(),
            expected_base: coordinate.base.clone(),
            edits,
        }))
    }

    /// The resolution basis a session reopened at `diagnostic` inherits from
    /// `previous`, assessed off-lock under the request's remaining budget.
    ///
    /// Any failure, a timeout included, keeps the earlier basis. The
    /// assessment only ever adds entries to it (`basis_after`), so the earlier
    /// basis is a subset of the assessed one: it can make a later
    /// take-committed-tip replay refuse a path, never lose one. A timeout must
    /// not end the preparation: the assessment shells out per path and side
    /// and can be irreducibly longer than any budget on a large store, and
    /// refusing here strands the owner's commit at every base move.
    async fn bounded_inherited_basis(
        &self,
        coordinate: &crate::jj::AdvanceCoordinate,
        previous: Option<&crate::orchestrator::conflict_session::ConflictSession>,
        diagnostic: &crate::jj::ConflictDiagnostic,
    ) -> crate::orchestrator::conflict_session::ResolutionBasis {
        use crate::orchestrator::conflict_session::{bounded_store_assessment, AssessmentScope};
        let (Some(previous), Some(ours)) = (previous, diagnostic.ours.clone()) else {
            return previous
                .map(|previous| previous.resolution_basis.clone())
                .unwrap_or_default();
        };
        let (session, store) = (previous.clone(), self.store.clone());
        // Read on the previous session's sides, then narrowed to what the new
        // session's base has not superseded, so the stored row stays as small
        // as what it still claims.
        let mut reopened = previous.clone();
        reopened.base = diagnostic.base.clone();
        let assessed = bounded_store_assessment(
            &self.jj.at_operation(&coordinate.operation),
            within_deadline(self.request, self.request.compute_budget),
            AssessmentScope {
                phase: "conflict session basis",
                branch: &self.request.branch,
                candidate_paths: previous.conflicting().count(),
            },
            move |jj| {
                let basis = session.resolution_basis_at(jj, &store, &ours)?;
                reopened.without_superseded(jj, &store, basis)
            },
        )
        .await;
        match assessed {
            Ok(basis) => basis,
            Err(error) => {
                log::warn!(
                    "conflict session for `{}`: could not tell which files changed since `{}` \
                     ({error}); keeping the earlier resolution basis",
                    previous.bookmark,
                    previous.ours.as_deref().unwrap_or("?")
                );
                previous.resolution_basis.clone()
            }
        }
    }

    /// Record a conflict only once the coordinates it describes are proved
    /// current, under the lock. `assess_basis` false keeps the open session's
    /// basis without reading it again, after a probe of the same basis has
    /// already run out of time.
    async fn conflicted(
        &self,
        relationship: &BaseRelationship,
        coordinate: &crate::jj::AdvanceCoordinate,
        diagnostic: crate::jj::ConflictDiagnostic,
        assess_basis: bool,
    ) -> Result<Attempt, PreparationRefusal> {
        let branch = self.request.branch.as_str();
        // What still conflicts is the answer; what the session records is that
        // plus every path the carry placed, so the next request carries them
        // again instead of conflicting on them.
        let paths = diagnostic.conflicting_paths();
        let diagnostic = match self.request.carry.as_deref() {
            Some(carry) => with_carried_paths(diagnostic, carry),
            None => diagnostic,
        };
        // An owner's session inherits a basis; assess it before the lock,
        // bounded, so no expensive read runs under the store guard.
        let (open, basis) = if self.request.purpose.actionable() {
            let open = load_active_session(self.db, branch)
                .await
                .map_err(PreparationRefusal::Repository)?;
            let basis = if assess_basis {
                self.bounded_inherited_basis(coordinate, open.as_ref(), &diagnostic)
                    .await
            } else {
                open.as_ref()
                    .map(|session| session.resolution_basis.clone())
                    .unwrap_or_default()
            };
            (open, basis)
        } else {
            (None, Default::default())
        };
        #[cfg(test)]
        tests::after_basis_assessment(&self.store);
        let guard = self.lock("record conflict").await?;
        if let Some(raced) = self.moved_since(relationship, coordinate).await? {
            return Ok(Attempt::Raced(raced));
        }
        self.refuse_fenced(&guard, &[branch]).await?;
        let intent_id = self
            .outcome_intent(&coordinate.base_branch, &coordinate.base)
            .await?;
        let fingerprint = diagnostic.fingerprint();
        if crate::orchestrator::catch_up::mode_for(
            self.db,
            &self.request.project_id,
            &self.store,
            branch,
        )
        .await
        .map_err(PreparationRefusal::Repository)?
        .is_some()
        {
            let cached = serde_json::to_string(&CachedMergeConflict::of(&diagnostic))
                .map_err(|error| PreparationRefusal::Repository(error.to_string()))?;
            self.record_merge_quarantine(
                coordinate,
                crate::orchestrator::catch_up::MERGE_CONFLICT_QUARANTINE,
                &cached,
            )
            .await?;
        }
        let record = if self.request.purpose.actionable() {
            // The basis was read from `open`; a session that changed since is
            // a race, not something to record over.
            let now_open = load_active_session(self.db, branch)
                .await
                .map_err(PreparationRefusal::Repository)?;
            // Everything the basis proof read, not only the coordinates: an
            // attestation or a replay request recorded on the same session
            // while it was assessed must not be overwritten by the older read.
            if now_open.as_ref().map(SessionProof::of) != open.as_ref().map(SessionProof::of) {
                return Ok(Attempt::Raced(
                    "the open conflict session changed while its basis was assessed".to_string(),
                ));
            }
            // The same conflict, unless this attempt found a path the open
            // session does not list yet.
            let already_open = open.as_ref().is_some_and(|session| {
                let listed: std::collections::BTreeSet<&str> = session
                    .conflicting()
                    .map(|file| file.path.as_str())
                    .collect();
                session.intent_id == intent_id
                    && session.fingerprint() == fingerprint
                    && diagnostic
                        .conflicting_paths()
                        .iter()
                        .all(|path| listed.contains(path.as_str()))
            });
            persist_reconcile_item(
                self.db,
                ReconcileItemUpdate {
                    intent_id: &intent_id,
                    bookmark: branch,
                    observed_tip: Some(&coordinate.head),
                    status: "completed",
                    failure_kind: Some("conflict"),
                    outcome_kind: Some(OUTCOME_CONFLICTED),
                    fingerprint: Some(&fingerprint),
                    diagnostic: None,
                },
            )
            .await
            .map_err(PreparationRefusal::Repository)?;
            if !already_open {
                let incoming = IncomingIdentity {
                    base_branch: coordinate.base_branch.clone(),
                    pr_number: None,
                    issue: None,
                };
                record_conflict_session(
                    self.db,
                    &intent_id,
                    branch,
                    &diagnostic,
                    &incoming,
                    &basis,
                )
                .await
                .map_err(PreparationRefusal::Repository)?;
                supersede_stale_sessions(self.db, branch, &intent_id)
                    .await
                    .map_err(PreparationRefusal::Repository)?;
            }
            drop(guard);
            self.steer_owner(&intent_id, coordinate, &diagnostic, already_open)
                .await?;
            if !already_open {
                let mut report = crate::jj::ReconcileReport::default();
                report
                    .conflict_diagnostics
                    .insert(branch.to_string(), diagnostic.clone());
                crate::orchestrator::catch_up::announce_reconcile_outcome(
                    self.orch,
                    self.db,
                    &self.request.project_id,
                    &self.store,
                    branch,
                    &relationship.base_branch,
                    &report,
                )
                .await;
            }
            ConflictRecord::Session {
                intent_id,
                reopened: !already_open,
            }
        } else {
            self.record_check_evidence(&intent_id, coordinate, &fingerprint, &paths)
                .await?;
            drop(guard);
            ConflictRecord::CheckEvidence { intent_id }
        };
        Err(PreparationRefusal::Conflict(Box::new(BaseConflict {
            branch: branch.to_string(),
            head: coordinate.head.clone(),
            base_branch: coordinate.base_branch.clone(),
            base_commit: coordinate.base.clone(),
            paths,
            record,
        })))
    }

    /// Non-actionable evidence: never a session, never markers, never a wake.
    /// An item that already carries an owner's session keeps it untouched.
    async fn record_check_evidence(
        &self,
        intent_id: &str,
        coordinate: &crate::jj::AdvanceCoordinate,
        fingerprint: &str,
        paths: &[String],
    ) -> Result<(), PreparationRefusal> {
        let branch = self.request.branch.as_str();
        if item_has_session(self.db, intent_id, branch)
            .await
            .map_err(PreparationRefusal::Repository)?
        {
            return Ok(());
        }
        let evidence = format!(
            "{} preparation of `{branch}` at {} conflicts with base `{}` at {}{}",
            self.request.purpose.label(),
            coordinate.head,
            coordinate.base_branch,
            coordinate.base,
            if paths.is_empty() {
                String::new()
            } else {
                format!(": {}", paths.join(", "))
            }
        );
        persist_reconcile_item(
            self.db,
            ReconcileItemUpdate {
                intent_id,
                bookmark: branch,
                observed_tip: Some(&coordinate.head),
                status: "completed",
                failure_kind: Some("conflict"),
                outcome_kind: Some(OUTCOME_CHECK_CONFLICT),
                fingerprint: Some(fingerprint),
                diagnostic: Some(&evidence),
            },
        )
        .await
        .map_err(PreparationRefusal::Repository)
    }

    /// Project markers and steer the owner, once per diagnostic fingerprint.
    /// Runs without the store lock: materialization talks to the executor.
    async fn steer_owner(
        &self,
        intent_id: &str,
        coordinate: &crate::jj::AdvanceCoordinate,
        diagnostic: &crate::jj::ConflictDiagnostic,
        already_open: bool,
    ) -> Result<(), PreparationRefusal> {
        let request = self.request;
        let branch = request.branch.as_str();
        let marker_state = if already_open {
            load_active_session(self.db, branch)
                .await
                .ok()
                .flatten()
                .map(|session| session.marker_state)
                .unwrap_or_default()
        } else {
            let (state, marker_diagnostic, dispositions) = materialize_markers_for_branch(
                self.orch,
                self.db,
                &request.project_id,
                branch,
                diagnostic,
            )
            .await;
            record_marker_state(
                self.db,
                intent_id,
                branch,
                state,
                marker_diagnostic.as_deref(),
                &dispositions,
            )
            .await
            .map_err(PreparationRefusal::Repository)?;
            state
        };
        let note = build_preparation_conflict_note(
            branch,
            &coordinate.base_branch,
            &coordinate.base,
            request.purpose,
        );
        let message = append_conflict_diagnostic(&note, Some(diagnostic), marker_state);
        let fingerprint = diagnostic.fingerprint();
        let jobs = load_on_branch_jobs(self.db, &request.project_id, branch)
            .await
            .map_err(PreparationRefusal::Repository)?;
        let owners = branch_owners(&jobs);
        for job in &owners {
            let Some(run_id) = latest_run_for_job(self.db, &job.id) else {
                continue;
            };
            let key = conflict_delivery_key(&request.project_id, branch, &job.id, &fingerprint);
            if let Err(error) =
                queue_system_direct_once(self.orch, &run_id, &message, DeliveryUrgency::Steer, &key)
            {
                log::warn!("steer job {} about `{branch}`'s conflict: {error}", job.id);
            }
        }
        // A task sharing the branch whose own commit met the conflict learns
        // that it did not land and who resolves it. Its tool result already
        // carries the refusal, so this rides along rather than waking it.
        let Some(requester) = request.owner_job_id.as_deref() else {
            return Ok(());
        };
        let shares = jobs.iter().any(|job| job.id == requester)
            && !owners.iter().any(|job| job.id == requester);
        if !shares {
            return Ok(());
        }
        let Some(run_id) = latest_run_for_job(self.db, requester) else {
            return Ok(());
        };
        let mut named = Vec::with_capacity(owners.len());
        for owner in &owners {
            named.push(
                crate::jobs::queries::home_uri_for_job(self.db, &owner.id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| format!("job `{}`", owner.id)),
            );
        }
        let note = build_preparation_conflict_sharer_note(
            branch,
            &coordinate.base_branch,
            &coordinate.base,
            request.purpose,
            &named.join(", "),
        );
        let key = conflict_delivery_key(&request.project_id, branch, requester, &fingerprint);
        if let Err(error) =
            queue_system_direct_once(self.orch, &run_id, &note, DeliveryUrgency::Passive, &key)
        {
            log::warn!("tell job {requester} its commit to `{branch}` conflicted: {error}");
        }
        Ok(())
    }

    fn notify_clean(
        &self,
        jobs: &[super::SiblingJob],
        relationship: &BaseRelationship,
        installed: &crate::jj::InstalledBranchAdvance,
    ) {
        let request = self.request;
        let note = build_preparation_clean_note(
            &request.branch,
            &relationship.base_branch,
            &installed.coordinate.base,
            &installed.coordinate.head,
            &installed.head,
            request.purpose,
        );
        for job in branch_owners(jobs) {
            let Some(run_id) = latest_run_for_job(self.db, &job.id) else {
                continue;
            };
            let key = clean_delivery_key(
                &request.project_id,
                &request.branch,
                &job.id,
                &installed.coordinate.base,
            );
            if let Err(error) =
                queue_system_direct_once(self.orch, &run_id, &note, DeliveryUrgency::Passive, &key)
            {
                log::warn!("note the prepared base to job {}: {error}", job.id);
            }
        }
    }

    /// What moved since `coordinate` was captured, read under the caller's lock.
    async fn moved_since(
        &self,
        relationship: &BaseRelationship,
        coordinate: &crate::jj::AdvanceCoordinate,
    ) -> Result<Option<String>, PreparationRefusal> {
        let current = self.relationship().await?;
        if !current.same_base(relationship) {
            return Ok(Some(relationship_change(relationship, &current)));
        }
        // A pinned coordinate is against one commit, which never moves: only
        // the branch can.
        let observed = if self.is_pinned(&coordinate.base) {
            cairn_vcs::resolve_coordinate(&self.store, &coordinate.branch)
                .await
                .map(|head| (head, coordinate.base.clone()))
                .map_err(|error| PreparationRefusal::Repository(error.to_string()))
        } else {
            literal_coordinates(&self.store, &coordinate.branch, &coordinate.base_branch).await
        };
        match observed {
            Ok((head, base)) if head == coordinate.head && base == coordinate.base => Ok(None),
            Ok((head, base)) => Ok(Some(format!(
                "`{}` is at {head} and `{}` at {base}, not the computed {} onto {}",
                coordinate.branch, coordinate.base_branch, coordinate.head, coordinate.base
            ))),
            Err(refusal) => Ok(Some(refusal.to_string())),
        }
    }

    /// The intent row that keys this exact base coordinate's outcomes.
    async fn outcome_intent(
        &self,
        base_branch: &str,
        base_commit: &str,
    ) -> Result<String, PreparationRefusal> {
        demand_intent(
            self.db,
            &self.request.project_id,
            &self.store,
            base_branch,
            base_commit,
            self.request.purpose,
        )
        .await
    }
}

/// Run blocking store work on a worker that owns the writer guard, returning
/// the guard with the result. If the awaiting future is dropped, the worker
/// still finishes its mutation before the guard is released.
async fn under_guard<T: Send + 'static>(
    guard: JjStoreGuard,
    phase: &'static str,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<(JjStoreGuard, T), PreparationRefusal> {
    tokio::task::spawn_blocking(move || {
        let output = guard.blocking_phase(phase, work);
        (guard, output)
    })
    .await
    .map_err(|error| PreparationRefusal::Repository(format!("{phase} worker: {error}")))
}

async fn literal_coordinates(
    store: &Path,
    branch: &str,
    base_branch: &str,
) -> Result<(String, String), PreparationRefusal> {
    let resolve = |name: &str| {
        let name = name.to_string();
        async move {
            cairn_vcs::resolve_coordinate(store, &name)
                .await
                .map_err(|error| PreparationRefusal::Unresolved {
                    coordinate: name.clone(),
                    reason: error.to_string(),
                })
        }
    };
    Ok((resolve(branch).await?, resolve(base_branch).await?))
}

fn relationship_change(before: &BaseRelationship, after: &BaseRelationship) -> String {
    format!(
        "the base relationship moved from `{}` (declared {:?}, default `{}`) to `{}` (declared \
         {:?}, default `{}`)",
        before.base_branch,
        before.declared,
        before.project_default,
        after.base_branch,
        after.declared,
        after.project_default
    )
}

/// The live job that owns `branch`, by the same predicate preparation uses to
/// pick an owner when none is named. `None` means the name is not a managed
/// branch for preparation: at most an exact, unmanaged snapshot.
pub(crate) async fn live_branch_owner(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Option<String>, String> {
    Ok(live_owners(db, project_id, branch, None)
        .await?
        .into_iter()
        .next()
        .map(|owner| owner.job_id))
}

/// How deep, how many, and for how long declared dependents are followed
/// before the traversal fails closed rather than hand the engine incomplete
/// evidence. It runs under the snapshot guard, so every bound is hard.
const DEPENDENT_DEPTH_CAP: usize = 32;
const DEPENDENT_COUNT_CAP: usize = 512;
const DEPENDENT_LOOKUP_BUDGET: Duration = Duration::from_secs(2);

/// One parent's direct dependents. Seeks `idx_jobs_project_base_branch`
/// (migration 0267, in both the private and team lineages); the plan is pinned
/// by `the_dependent_lookup_seeks_its_index_in_both_lineages`.
pub(super) const DEPENDENT_BRANCHES_SQL: &str = "SELECT DISTINCT TRIM(j.branch) FROM jobs j
      WHERE j.project_id = ?1 AND j.base_branch = ?2
        AND TRIM(COALESCE(j.branch, '')) <> ''
      LIMIT ?3";

/// Every branch whose job declares `branch` as its base, and theirs in turn:
/// one `jobs(project_id, base_branch)`-indexed lookup per parent, never an
/// enumeration of open branches. Finished jobs count too: their bookmarks can
/// still anchor history, and the engine ignores names with no bookmark.
/// Sorted and distinct, excluding `branch` itself.
pub(super) async fn dependent_branches(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    deadline: std::time::Instant,
) -> Result<Vec<String>, String> {
    let incomplete =
        |why: String| format!("{why}; the ownership evidence for `{branch}` is not complete");
    let mut seen = std::collections::BTreeSet::new();
    let mut frontier = vec![branch.to_string()];
    for _ in 0..DEPENDENT_DEPTH_CAP {
        if frontier.is_empty() {
            return Ok(seen.into_iter().collect());
        }
        let mut next = Vec::new();
        for parent in frontier {
            if std::time::Instant::now() >= deadline {
                return Err(incomplete(
                    "reading the branches built on it ran out of time".into(),
                ));
            }
            // Never materialize more rows than could still fit under the cap.
            let remaining = DEPENDENT_COUNT_CAP.saturating_sub(seen.len()) + 1;
            let (project, parent_owned) = (project_id.to_string(), parent.clone());
            let children: Vec<String> = db
                .read(move |conn| {
                    let (project, parent) = (project.clone(), parent_owned.clone());
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                DEPENDENT_BRANCHES_SQL,
                                params![project.as_str(), parent.as_str(), remaining as i64],
                            )
                            .await?;
                        let mut children = Vec::new();
                        while let Some(row) = rows.next().await? {
                            children.push(row.text(0)?);
                        }
                        Ok(children)
                    })
                })
                .await
                .map_err(|error| format!("read the branches built on `{parent}`: {error}"))?;
            for child in children {
                if child != branch && seen.insert(child.clone()) {
                    next.push(child);
                }
            }
            if seen.len() > DEPENDENT_COUNT_CAP {
                return Err(incomplete(format!(
                    "more than {DEPENDENT_COUNT_CAP} branches are built on it"
                )));
            }
        }
        frontier = next;
    }
    Err(format!(
        "branches built on `{branch}` nest deeper than {DEPENDENT_DEPTH_CAP}; their ownership \
         evidence is not complete"
    ))
}

#[derive(Clone)]
struct Owner {
    job_id: String,
    declared: Option<String>,
    branchless: bool,
}

/// The live jobs on branch `?2` in project `?1`, owners first ([`OWNS_BRANCH`]:
/// a task on its parent's branch never displaces the parent), then newest
/// first: who a preparation that names no owner acts for. Its plan is pinned
/// by `live_job_lookups_by_branch_seek_project_and_branch` (migration
/// `0403_index_jobs_project_branch`).
pub(super) fn live_owners_on_branch_sql() -> String {
    format!(
        "SELECT j.id, j.base_branch, 0 FROM jobs j {DELEGATOR_ON_BRANCH_JOIN}
          WHERE j.project_id = ?1 AND j.branch = ?2
            AND {LIVE_JOB_PREDICATE}
          ORDER BY {OWNS_BRANCH} DESC, j.created_at DESC, j.id"
    )
}

async fn live_owners(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    owner_job_id: Option<&str>,
) -> Result<Vec<Owner>, String> {
    let (project, branch_owned) = (project_id.to_string(), branch.to_string());
    let owner = owner_job_id.map(str::to_string);
    db.read(move |conn| {
        let (project, branch, owner) = (project.clone(), branch_owned.clone(), owner.clone());
        Box::pin(async move {
            let mut rows = match &owner {
                // A named owner may be branchless: it works directly on its
                // recorded integration target.
                Some(owner) => {
                    conn.query(
                        // A whitespace-only branch is no branch, as the write
                        // path's own resolution treats it.
                        "SELECT id, base_branch, TRIM(COALESCE(branch, '')) = '' FROM jobs
                          WHERE id = ?1 AND project_id = ?2
                            AND (branch = ?3 OR TRIM(COALESCE(branch, '')) = '')",
                        params![owner.as_str(), project.as_str(), branch.as_str()],
                    )
                    .await?
                }
                None => {
                    conn.query(
                        &live_owners_on_branch_sql(),
                        params![project.as_str(), branch.as_str()],
                    )
                    .await?
                }
            };
            let mut owners = Vec::new();
            while let Some(row) = rows.next().await? {
                owners.push(Owner {
                    job_id: row.text(0)?,
                    declared: normalized(row.opt_text(1)?),
                    branchless: row.i64(2)? != 0,
                });
            }
            Ok(owners)
        })
    })
    .await
    .map_err(|error| format!("read the owners of `{branch}`: {error}"))
}

fn normalized(name: Option<String>) -> Option<String> {
    name.map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

/// Read the relationship from the owner's declaration and the project default.
pub(super) async fn load_relationship(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    owner_job_id: Option<&str>,
    target_override: Option<&str>,
) -> Result<BaseRelationship, PreparationRefusal> {
    let owners = live_owners(db, project_id, branch, owner_job_id)
        .await
        .map_err(PreparationRefusal::Repository)?;
    let Some(Owner {
        job_id: owner_job_id,
        declared,
        branchless,
    }) = owners.first().cloned()
    else {
        return Err(PreparationRefusal::Unmanaged {
            branch: branch.to_string(),
        });
    };
    let mut bases: Vec<String> = owners
        .iter()
        .map(|owner| {
            owner
                .declared
                .clone()
                .unwrap_or_else(|| "(project default)".into())
        })
        .collect();
    bases.sort();
    bases.dedup();
    if bases.len() > 1 {
        return Err(PreparationRefusal::RelationshipAmbiguous {
            branch: branch.to_string(),
            bases,
        });
    }
    let project_default = load_project_default_branch(db, project_id)
        .await
        .map_err(PreparationRefusal::Repository)?
        .ok_or_else(|| PreparationRefusal::Unresolved {
            coordinate: "project default branch".to_string(),
            reason: "the project records no default branch".to_string(),
        })?;
    if branchless {
        // A branchless job's coordinate is its target itself: the exact head,
        // never a replay of the target onto itself. Only its own target.
        let target = declared.clone().unwrap_or_else(|| project_default.clone());
        if target != branch {
            return Err(PreparationRefusal::Unmanaged {
                branch: branch.to_string(),
            });
        }
        return Ok(BaseRelationship {
            owner_job_id,
            declared,
            project_default,
            target_override: None,
            base_branch: target,
        });
    }
    let target_override = normalized(target_override.map(str::to_string));
    let base_branch = target_override
        .clone()
        .or_else(|| declared.clone())
        .unwrap_or_else(|| project_default.clone());
    if base_branch == branch {
        return Err(PreparationRefusal::Unresolved {
            coordinate: branch.to_string(),
            reason: "the branch is its own base".to_string(),
        });
    }
    Ok(BaseRelationship {
        owner_job_id,
        declared,
        project_default,
        target_override,
        base_branch,
    })
}

/// The intent row for one exact base coordinate, minted `completed` when
/// absent so no legacy sweep ever claims or expands it.
pub(super) async fn demand_intent(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    base_branch: &str,
    base_commit: &str,
    purpose: PreparationPurpose,
) -> Result<String, PreparationRefusal> {
    let store_path = store.to_string_lossy().into_owned();
    let lookup = || {
        let (project, store, target, destination) = (
            project_id.to_string(),
            store_path.clone(),
            base_branch.to_string(),
            base_commit.to_string(),
        );
        db.read(move |conn| {
            let (project, store, target, destination) = (
                project.clone(),
                store.clone(),
                target.clone(),
                destination.clone(),
            );
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, status FROM jj_reconcile_intents
                          WHERE project_id = ?1 AND store_path = ?2
                            AND target_branch = ?3 AND destination_commit = ?4",
                        params![
                            project.as_str(),
                            store.as_str(),
                            target.as_str(),
                            destination.as_str()
                        ],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.text(1)?)),
                    None => None,
                })
            })
        })
    };
    let classify = |found: (String, String)| match found.1.as_str() {
        "completed" | "superseded" => Ok(found.0),
        _ => Err(PreparationRefusal::Busy(BusyReason::LegacyIntent {
            intent_id: found.0,
            status: found.1,
        })),
    };
    if let Some(found) = lookup()
        .await
        .map_err(|error| PreparationRefusal::Repository(error.to_string()))?
    {
        return classify(found);
    }
    let now = chrono::Utc::now().timestamp();
    let sources = serde_json::json!([format!("demand:{}", purpose.label())]).to_string();
    db.execute(
        "INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit,
            trigger_sources_json, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'completed', ?7, ?7)
         ON CONFLICT(project_id, store_path, target_branch, destination_commit) DO NOTHING",
        params![
            format!("demand-{}", uuid::Uuid::new_v4()),
            project_id,
            store_path.as_str(),
            base_branch,
            base_commit,
            sources,
            now
        ],
    )
    .await
    .map_err(|error| PreparationRefusal::Repository(format!("mint outcome intent: {error}")))?;
    let found = lookup()
        .await
        .map_err(|error| PreparationRefusal::Repository(error.to_string()))?
        .ok_or_else(|| PreparationRefusal::Repository("outcome intent vanished".to_string()))?;
    classify(found)
}

async fn item_has_session(db: &LocalDb, intent_id: &str, branch: &str) -> Result<bool, String> {
    let (intent, branch) = (intent_id.to_string(), branch.to_string());
    db.read(move |conn| {
        let (intent, branch) = (intent.clone(), branch.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT resolution_state FROM jj_reconcile_items
                      WHERE intent_id = ?1 AND bookmark = ?2",
                    params![intent.as_str(), branch.as_str()],
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.opt_text(0)?.is_some(),
                None => false,
            })
        })
    })
    .await
    .map_err(|error| format!("inspect branch item: {error}"))
}

async fn pending_exports(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<Vec<(String, String)>, String> {
    let (project, store, branch) = (
        project_id.to_string(),
        store.to_string_lossy().into_owned(),
        branch.to_string(),
    );
    db.read(move |conn| {
        let (project, store, branch) = (project.clone(), store.clone(), branch.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.intent_id, i.observed_tip FROM jj_reconcile_items i
                       JOIN jj_reconcile_intents n ON n.id = i.intent_id
                      WHERE n.project_id = ?1 AND n.store_path = ?2 AND i.bookmark = ?3
                        AND i.outcome_kind = ?4 AND i.observed_tip IS NOT NULL
                      ORDER BY i.updated_at",
                    params![
                        project.as_str(),
                        store.as_str(),
                        branch.as_str(),
                        OUTCOME_EXPORT_PENDING
                    ],
                )
                .await?;
            let mut pending = Vec::new();
            while let Some(row) = rows.next().await? {
                pending.push((row.text(0)?, row.text(1)?));
            }
            Ok(pending)
        })
    })
    .await
    .map_err(|error| format!("read pending exports: {error}"))
}

async fn set_item_outcome(
    db: &LocalDb,
    intent_id: &str,
    branch: &str,
    outcome: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items SET outcome_kind = ?3, updated_at = ?4
          WHERE intent_id = ?1 AND bookmark = ?2",
        params![intent_id, branch, outcome, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("update branch item outcome: {error}"))
}

fn capture_refusal(error: crate::jj::AdvanceCaptureError, store: &Path) -> PreparationRefusal {
    match error {
        crate::jj::AdvanceCaptureError::Busy(reason) => {
            PreparationRefusal::Busy(BusyReason::StoreMaintenance(reason))
        }
        crate::jj::AdvanceCaptureError::UnsettledOperations { heads } => {
            PreparationRefusal::Unresolved {
                coordinate: store.display().to_string(),
                reason: format!("{heads} operation heads remain unsettled"),
            }
        }
        crate::jj::AdvanceCaptureError::UnresolvedCoordinate { name } => {
            PreparationRefusal::Unresolved {
                coordinate: name,
                reason: "it does not resolve to a single commit".to_string(),
            }
        }
        crate::jj::AdvanceCaptureError::DestinationOffBase {
            destination,
            base_branch,
        } => PreparationRefusal::Unresolved {
            coordinate: destination,
            reason: format!("it is no longer in `{base_branch}`'s history"),
        },
        crate::jj::AdvanceCaptureError::Repository(error) => PreparationRefusal::Repository(error),
    }
}

/// `diagnostic` with every path `carry` placed recorded as conflicting.
///
/// A carried attempt that still conflicts reports only the residual paths,
/// because the carried ones merged. Recording only those would make the next
/// request carry only the residue, so the paths this attempt carried would
/// conflict again, and the session's list would oscillate between the two
/// sets without ever landing.
fn with_carried_paths(
    mut diagnostic: crate::jj::ConflictDiagnostic,
    carry: &crate::jj::ResolutionCarry,
) -> crate::jj::ConflictDiagnostic {
    for (path, _) in &carry.paths {
        match diagnostic
            .incoming
            .iter_mut()
            .find(|file| &file.path == path)
        {
            Some(file) => file.classification = crate::jj::IncomingClassification::Conflicting,
            None => diagnostic.incoming.push(crate::jj::IncomingFile {
                path: path.clone(),
                status: "C".to_string(),
                classification: crate::jj::IncomingClassification::Conflicting,
            }),
        }
    }
    diagnostic
}

fn keep(warnings: &mut Vec<String>, what: &str, result: Result<(), String>) {
    if let Err(error) = result {
        log::error!("branch preparation accepted its install but could not {what}: {error}");
        warnings.push(format!("could not {what}: {error}"));
    }
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

// The key becomes a globally unique message id, so it names the recipient job:
// each recipient hears each note once, not just the first.
fn clean_delivery_key(project_id: &str, branch: &str, job_id: &str, base_commit: &str) -> String {
    format!("prepare:{project_id}:{branch}:{job_id}:clean:{base_commit}")
}

fn conflict_delivery_key(
    project_id: &str,
    branch: &str,
    job_id: &str,
    fingerprint: &str,
) -> String {
    format!("prepare:{project_id}:{branch}:{job_id}:conflicted:{fingerprint}")
}

fn build_preparation_clean_note(
    branch: &str,
    base_branch: &str,
    base_commit: &str,
    head_before: &str,
    head_after: &str,
    purpose: PreparationPurpose,
) -> String {
    format!(
        "[Base branch update] Your branch `{branch}` was carried onto base `{base_branch}` at {} \
         when a {} needed the current base (head {} -> {}). Nothing to resolve, and no manual \
         rebase or force-push is needed. A clean advance is not a green build: rebuild and \
         re-run the checks that cover what the base changed.",
        short(base_commit),
        purpose.label(),
        short(head_before),
        short(head_after),
    )
}

fn build_preparation_conflict_note(
    branch: &str,
    base_branch: &str,
    base_commit: &str,
    purpose: PreparationPurpose,
) -> String {
    format!(
        "⛔ BLOCKING [Base branch update] Your {} needed `{branch}` on its current base \
         `{base_branch}` at {}, and carrying it there conflicts. Nothing was installed: your \
         branch is unchanged. Resolve the conflict through `cairn:~/rebase`; your work cannot \
         land until it carries the base.",
        purpose.label(),
        short(base_commit),
    )
}

/// The conflict as told to a task whose own commit met it on the branch it
/// shares with `owners`: never a request to resolve what the owner resolves.
fn build_preparation_conflict_sharer_note(
    branch: &str,
    base_branch: &str,
    base_commit: &str,
    purpose: PreparationPurpose,
    owners: &str,
) -> String {
    format!(
        "[Base branch update] Your {} to `{branch}` did not land: the branch needed its current \
         base `{base_branch}` at {}, carrying it there conflicts, and nothing was installed. \
         `{branch}` belongs to {owners}, which has been told and resolves the conflict; it is not \
         yours to resolve. Another commit to `{branch}` meets the same conflict until it is \
         resolved, so report what you were committing to the owner rather than retrying.",
        purpose.label(),
        short(base_commit),
    )
}

#[cfg(test)]
#[path = "preparation_tests.rs"]
pub(super) mod tests;
