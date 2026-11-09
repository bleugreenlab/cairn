//! Landing a pull request on its project's default branch.
//!
//! Every default-branch landing runs one loop, and the store lock is held only
//! where something must not move:
//!
//! 1. **Prepare**: the canonical branch preparer carries the source onto the
//!    target it actually lands on, as its owner's merge
//!    (`store_merge::prepare_landing_source`). The result is one exact
//!    coordinate: the source head, its tree, the target commit it descends
//!    from, and the declared relationship. For a project with a remote, origin's
//!    copy of the target is fetched into the store first, and the coordinate
//!    holds only while the store's target and origin's are the same commit.
//! 2. **Prospect**, under the lock, then undone: the merge result as it would
//!    land on exactly that coordinate. A branch that changed a declared
//!    identifier site (`landing.identifiers`) is bound there
//!    (`orchestrator::landing::bind`); any other branch is folded prospectively.
//!    Nothing here replays the source: a coordinate that no longer holds sends
//!    the attempt back to preparation.
//! 3. **Validate**, off the lock: every file the landing changes still parses,
//!    and every bound identifier holds its invariant (no NEXT, the next numbers
//!    and no others, at most one new protocol version).
//! 4. **Land if unchanged**, under the lock: the prepared coordinate is
//!    revalidated (source, target and relationship), with origin's target
//!    fetched again. Any movement restarts preparation, binding and validation;
//!    otherwise the bind is redone for real, its tree is proven to be the one
//!    validated, and it lands.
//!
//! Holding the lock from the recheck through the landing is what serializes
//! binding: a second landing that bound against the same tip finds it moved
//! and binds again against the first one's result.
//!
//! Publication enforces both expected coordinates. The validated commit is
//! folded in the store onto exactly the prepared target, and for a project with
//! a remote it reaches origin by one push of the target ref alone
//! ([`publish_target`]): leased on the prepared target as its exact expected
//! old value and refused unless the landing descends from it. The source ref is
//! not pushed, so there is no second ref whose publication could diverge.
//! GitHub's merge API is not used: it can require the pull request's head but
//! not the base, so it could land a tree nothing validated. Each publication
//! try journals its intent before the push (`landing_record`), so a landing
//! origin accepted is never lost to a crash or reported as unpublished.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::project_settings::IdentifierRule;
use crate::github::api;
use crate::github::credentials::get_owner_repo;
use crate::orchestrator::base_advance::preparation::PreparedBranch;
use crate::orchestrator::landing::{self, bind};
use crate::orchestrator::{BranchReservation, JjStoreGuard, Orchestrator};
use crate::security::broker::github::installation_authority;
use crate::storage::LocalDb;

use super::context::MergeMrContext;
use super::human_approval::{
    self, approval_strategy, gate_checks, Ceremony, Execution, InFlight, LandedShape,
    LandingEvidence, LandingWorld, Observation, SystemClock,
};
use super::landing_record::{
    journal_publication_intent, journal_publication_outcome, Publication, PublicationOutcome,
    PublicationShape,
};
use super::merge::MergeAttemptJournal;
use super::store_merge::{
    fold_validated_default, prepare_landing_source, prepared_landing_holds,
    prospective_default_landing, ExactTarget, ProspectiveMerge,
};

/// How long a landing waits for its source and target to be free of another
/// publication before it refuses.
pub(super) const LANDING_RESERVATION_WAIT: std::time::Duration =
    crate::mcp::vcs::STORE_LOCK_TIMEOUT;

/// How many times the loop builds and validates a merge result because a tip
/// moved under it, before it gives the merge back to its caller.
pub(super) const MAX_LANDING_ATTEMPTS: usize = 4;

/// What landed, and what it allocated.
#[derive(Debug)]
pub(super) struct DefaultLanding {
    /// The exact landing commit the store built on the target and origin
    /// accepted. The GitHub merge API is no longer a transport, so no answer
    /// from GitHub supplies it. It stays an `Option` only until the field's
    /// owners can change its type.
    pub landed_commit: Option<String>,
    /// The identifiers the landing bound.
    pub plan: bind::BindPlan,
    /// What the running build says of the `.cairn/config.yaml` this landing
    /// puts on the default branch, judged on the exact merge result.
    pub config_notice: Option<String>,
    /// The publication to origin's target that landed it, with the journal
    /// attempt holding its intent; `None` for a project with no remote.
    pub published: Option<PublishedLanding>,
    /// The landing's reservation of its source and target, handed to the
    /// caller so it is still held while the merge is recorded. Until the
    /// source's pull request reads merged, a child could otherwise land into
    /// the source after it landed. The caller releases it once the merge is
    /// recorded.
    pub reservation: Option<Arc<BranchReservation>>,
}

/// A landing origin accepted, and the journal attempt that recorded its
/// intent. Its outcome is journaled once the landing is recorded.
#[derive(Debug, Clone)]
pub(super) struct PublishedLanding {
    pub publication: Publication,
    pub attempt_id: String,
    /// Keeps the attempt in flight until the last copy is dropped, which the
    /// merge command does only after it has journaled the outcome.
    pub driving: PublicationInFlight,
}

/// A publication attempt this process is driving, from its intent to its
/// outcome: landing recovery leaves it alone (`Busy`) for as long as any copy
/// is held. Without it, recovery reads a live publication's missing outcome as
/// a lost one and settles it under the command that is still recording it.
#[derive(Clone)]
pub(super) struct PublicationInFlight(Arc<InFlight>);

impl PublicationInFlight {
    /// Refused when something in this process already drives the attempt: a
    /// publication's attempt id is minted fresh, so that is a defect, and
    /// nothing has been pushed yet.
    pub(super) fn enter(attempt_id: &str) -> Result<Self, String> {
        InFlight::try_enter(attempt_id)
            .map(|driving| Self(Arc::new(driving)))
            .ok_or_else(|| {
                format!("publication attempt {attempt_id} is already being driven in this process")
            })
    }
}

impl std::fmt::Debug for PublicationInFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublicationInFlight")
    }
}

/// What one landing attempt put on its target.
pub(super) struct Landed {
    pub commit: Option<String>,
    pub published: Option<PublishedLanding>,
}

/// What pushing a folded landing to origin's target did.
enum Publish {
    /// Origin accepted it. `unconfirmed` carries why the read-back did not
    /// complete; the publication happened regardless.
    Accepted { unconfirmed: Option<String> },
    /// Origin's target was not the expected old commit. Nothing was published.
    TargetMoved {
        observed: Option<String>,
        error: String,
    },
    /// Refused definitively before or by the receiver. Nothing was published.
    Refused(String),
    /// Origin may or may not hold it.
    Unknown(String),
}

/// Defer the store's object reclamation for as long as a landing holds it.
/// The landing builds commits that nothing references while they are set
/// aside (a prospect's fold-and-undo; the landing commit between its undo and
/// origin's acceptance). The named `refs/cairn/landing` pin is the durable
/// root once created; this covers the jj metadata and the window before the
/// pin exists. Taken before the store lock, never under it; refused while
/// maintenance is reclaiming, so the landing retries rather than racing it.
fn object_lease(
    store: &Store,
    merge_context: &MergeMrContext,
) -> Result<crate::jj::StoreObjectLease, String> {
    crate::jj::StoreObjectLease::for_store(&store.path).map_err(|error| {
        format!(
            "Refusing to merge `{}` into `{}`: the store is busy with maintenance ({error}). \
             Nothing changed; merge again shortly.",
            merge_context.source_branch, merge_context.target_branch
        )
    })
}

/// Whether `reservation` holds both of the landing's bookmarks, which is what
/// lets it publish with the store lock released.
pub(super) fn reservation_covers(
    reservation: &BranchReservation,
    store: &Store,
    merge_context: &MergeMrContext,
) -> Result<(), String> {
    for branch in [&merge_context.source_branch, &merge_context.target_branch] {
        if !reservation.covers(&store.path, branch) {
            return Err(format!(
                "the landing does not hold `{branch}` reserved, so it cannot publish with the \
                 store lock released"
            ));
        }
    }
    Ok(())
}

/// Admit the landing's writers of its source and its target under `guard`:
/// neither may be under a branch recovery fence (nor the store), and each must
/// be reserved by the landing and owe no acceptance
/// (`JjStoreGuard::branch_write_permit`). Required before the landing moves
/// either bookmark or publishes, and again under every re-taken guard, so a
/// fence laid after the landing prepared or validated still refuses it.
pub(super) async fn admit_landing_writers(
    orch: &Orchestrator,
    guard: &JjStoreGuard,
    reservation: &BranchReservation,
    merge_context: &MergeMrContext,
) -> Result<(), String> {
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    let fence_db = crate::orchestrator::base_advance::recovery_fence_db(orch, &store).await;
    crate::orchestrator::branch_recovery::refuse_if_fenced(
        &fence_db,
        &store,
        &[
            merge_context.source_branch.as_str(),
            merge_context.target_branch.as_str(),
        ],
        "Merging this pull request",
    )
    .await?;
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &merge_context.project_id)
        .await
        .map_err(|error| format!("the project's database is unavailable: {error}"))?;
    for branch in [&merge_context.source_branch, &merge_context.target_branch] {
        guard
            .branch_write_permit(
                &db,
                reservation,
                &merge_context.project_id,
                branch,
                crate::orchestrator::BatchAdmission::None,
            )
            .await?;
    }
    Ok(())
}

/// Which refs a landing's publication moves on origin.
pub(super) enum PublishRefs {
    /// The target alone: a default landing does not push its source ref, so
    /// there is no second ref whose publication could diverge.
    Target,
    /// Source and target together in one atomic push: a child landing's pull
    /// request head moves with the integration branch it lands in.
    /// `source_origin` is the source ref's exact lease, the store's recorded
    /// copy of origin's source (`None`: origin must not have the branch).
    /// `remote_rewrite` is `Some` only when origin's target is not an ancestor
    /// of the landing and the target is a managed integration branch: the
    /// accepted installs' drop claims the transport's supersession proof may
    /// use before it rewrites origin's target (refusing when it cannot prove
    /// it). `None` only ever fast-forwards origin.
    SourceAndTarget {
        source_origin: Option<String>,
        remote_rewrite: Option<Vec<crate::jj::AcceptedAdvanceDrops>>,
    },
}

/// Publish a landing commit to origin with the store lock released, then
/// re-take the lock and record the outcome.
///
/// The landing commit is immutable and the store's bookmarks still hold the
/// prepared target and source: nothing local names a landing origin has not
/// accepted, so no other branch can prepare onto it. The caller's reservation
/// of the source and the target (checked by [`reservation_covers`]) keeps both
/// still while the lock is free. The push moves the refs `refs` names, each
/// leased on its exact expected old value; the target's is the prepared
/// target, and the landing must descend from it. Only an accepted push moves
/// the store: both bookmarks forward to the landing, each only from its
/// expected value ([`advance_landing_bookmarks`]), then origin's new value is
/// recorded. A refusal leaves the store as it was; so does an unknown outcome,
/// for recovery.
async fn publish_released(
    orch: &Orchestrator,
    store: &Store,
    merge_context: &MergeMrContext,
    guard: JjStoreGuard,
    reservation: &BranchReservation,
    publication: &Publication,
    refs: PublishRefs,
) -> (JjStoreGuard, Publish) {
    let source = merge_context.source_branch.as_str();
    drop(guard);
    let (jj, path, pushed) = (store.jj.clone(), store.path.clone(), publication.clone());
    let publish = super::merge_metrics::time(super::merge_metrics::Phase::Publish);
    let outcome = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        publication_barrier::reached(&path);
        let heads = match refs {
            PublishRefs::Target => crate::jj::push_commit_expecting_unrecorded(
                &jj,
                &path,
                &pushed.target_branch,
                &pushed.expected_old,
                &pushed.landing_commit,
            ),
            PublishRefs::SourceAndTarget {
                source_origin,
                remote_rewrite,
            } => crate::jj::publish_landing_atomic_unrecorded(
                &jj,
                &path,
                &crate::jj::LandingPublication {
                    source_branch: pushed.source_branch.clone(),
                    source_expected_old: source_origin,
                    target_branch: pushed.target_branch.clone(),
                    target_expected_old: pushed.expected_old.clone(),
                    landing: pushed.landing_commit.clone(),
                    proof: crate::jj::TargetProof::Descends,
                    remote_rewrite,
                },
                &pushed.source_before,
                &pushed.target_before,
            ),
        }?;
        // Reading origin back is network I/O too, so it stays off the lock.
        let readback = crate::jj::readback_leased_push_exact(&jj, &path, &heads);
        Ok((heads, readback))
    })
    .await;
    drop(publish);
    let wait = super::merge_metrics::time(super::merge_metrics::Phase::LockWait);
    let guard = orch
        .acquire_jj_store_lock(
            &store.path,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("record the landing publication of {source}"),
        )
        .await;
    drop(wait);
    let publish = match outcome {
        Err(join) => Publish::Unknown(format!("the push worker did not finish: {join}")),
        Ok(Ok((heads, readback))) => {
            // Only a read-back of exactly the landing proves origin holds it;
            // a descendant still confirms the landing, but proves nothing a
            // checkpoint could name.
            let exact = matches!(readback, Ok(true));
            let readback = readback.err();
            // Origin said yes: the store moves to the landing whether or not
            // the read-back completed, and records origin's value locally.
            // Only this landing's own bookmarks move, and only once both
            // writers are admitted again under this guard: the lock was free
            // for the push. A refusal leaves the store unmoved; origin took the
            // landing regardless, and recovery records it.
            let admitted = match reservation_covers(reservation, store, merge_context) {
                Ok(()) => admit_landing_writers(orch, &guard, reservation, merge_context).await,
                Err(error) => Err(error),
            };
            let advanced = admitted.and_then(|()| {
                guard.blocking_phase("install accepted landing", || {
                    advance_landing_bookmarks(
                        store,
                        &[
                            (
                                publication.target_branch.as_str(),
                                publication.target_before.as_str(),
                                publication.landing_commit.as_str(),
                            ),
                            (
                                source,
                                publication.source_before.as_str(),
                                publication.landing_commit.as_str(),
                            ),
                        ],
                    )
                })
            });
            let repointed = match &advanced {
                Ok(()) => repoint_landed_children(orch, merge_context, &guard)
                    .await
                    .err(),
                Err(_) => None,
            };
            // Recording origin's value moves exactly the accepted refs' tracked
            // copies (no import), which the landing's own permits cover, so
            // another branch's reservation does not defer it.
            let recorded = advanced.and_then(|()| {
                guard.blocking_phase("record landing publication", || {
                    crate::jj::in_place_blocking(|| {
                        crate::jj::record_leased_push(&store.jj, &store.path, &heads)
                    })
                })
            });
            // The store holds the landing, records origin at it, and origin
            // holds exactly it: the confirmed publication is each managed
            // branch's claims reset point. Never on anything less.
            let checkpointed = if exact && recorded.is_ok() {
                checkpoint_publication(
                    orch,
                    &guard,
                    reservation,
                    store,
                    merge_context,
                    &publication.landing_commit,
                )
                .await
            } else {
                None
            };
            Publish::Accepted {
                unconfirmed: match (readback, recorded, repointed, checkpointed) {
                    (None, Ok(()), None, None) => None,
                    (readback, recorded, repointed, checkpointed) => Some(
                        [readback, recorded.err(), repointed, checkpointed]
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>()
                            .join("; "),
                    ),
                },
            }
        }
        Ok(Err(crate::jj::LeasedPushError::Unknown(error))) => Publish::Unknown(error),
        Ok(Err(crate::jj::LeasedPushError::TargetMoved { observed, .. })) => Publish::TargetMoved {
            error: format!(
                "target moved: origin's `{}` is {}, not the prepared {}; nothing was pushed",
                publication.target_branch,
                observed.as_deref().unwrap_or("<absent>"),
                publication.expected_old
            ),
            observed,
        },
        Ok(Err(error)) => Publish::Refused(error.to_string()),
    };
    (guard, publish)
}

/// Record a confirmed publication at exactly `landing` as the claims reset
/// point of each of the landing's managed branches (`preparation::
/// record_publication_checkpoint`), under the re-taken `guard` and the
/// landing's reservation. The project default and a branch with no live owner
/// carry no claims and are skipped. Returns why a checkpoint that applies was
/// not recorded; that never undoes the landing, it only leaves the claims for
/// the next confirmed publication to reset.
async fn checkpoint_publication(
    orch: &Orchestrator,
    guard: &JjStoreGuard,
    reservation: &BranchReservation,
    store: &Store,
    merge_context: &MergeMrContext,
    landing: &str,
) -> Option<String> {
    use crate::orchestrator::base_advance::preparation::{
        live_branch_owner, record_publication_checkpoint,
    };
    let project = merge_context.project_id.as_str();
    let db = match crate::execution::routing::owning_db_for_project(&orch.db, project).await {
        Ok(db) => db,
        Err(error) => {
            return Some(format!(
                "no publication checkpoint was recorded: the project's database is unavailable \
                 ({error})"
            ))
        }
    };
    let mut missed = Vec::new();
    for branch in [&merge_context.target_branch, &merge_context.source_branch] {
        if *branch == merge_context.default_branch {
            continue;
        }
        match live_branch_owner(&db, project, branch).await {
            Ok(Some(_)) => {}
            Ok(None) => continue,
            Err(error) => {
                missed.push(format!("`{branch}`'s owner is unreadable: {error}"));
                continue;
            }
        }
        if let Err(error) = record_publication_checkpoint(
            &db,
            guard,
            reservation,
            project,
            &store.path,
            branch,
            landing,
        )
        .await
        {
            missed.push(error);
        }
    }
    (!missed.is_empty()).then(|| {
        format!(
            "the publication checkpoint was not recorded for every managed branch: {}",
            missed.join("; ")
        )
    })
}

/// A pause between releasing the store lock and pushing a landing, so tests can
/// move origin, write other branches, or try the reserved ones while the push
/// is pending. Runs on the push's blocking worker.
#[cfg(test)]
pub(in crate::pr_data::actions) mod publication_barrier {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    type Hook = Box<dyn FnMut() + Send>;

    fn hooks() -> &'static Mutex<HashMap<PathBuf, Hook>> {
        static HOOKS: OnceLock<Mutex<HashMap<PathBuf, Hook>>> = OnceLock::new();
        HOOKS.get_or_init(Default::default)
    }

    /// Run `hook` before each landing push from the store at `store`.
    pub(in crate::pr_data::actions) fn set(store: &Path, hook: impl FnMut() + Send + 'static) {
        hooks()
            .lock()
            .unwrap()
            .insert(store.to_path_buf(), Box::new(hook));
    }

    pub(in crate::pr_data::actions) fn clear(store: &Path) {
        hooks().lock().unwrap().remove(store);
    }

    pub(super) fn reached(store: &Path) {
        let hook = hooks().lock().unwrap().remove(store);
        if let Some(mut hook) = hook {
            hook();
            hooks().lock().unwrap().insert(store.to_path_buf(), hook);
        }
    }
}

/// A pause inside a landing's origin take, on the worker that owns the store
/// guard, so tests can cancel the landing mid-take. Same contract as
/// [`publication_barrier`]; a separate point so publication tests never stop
/// here.
#[cfg(test)]
pub(in crate::pr_data::actions) mod origin_take_barrier {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    type Hook = Box<dyn FnMut() + Send>;

    fn hooks() -> &'static Mutex<HashMap<PathBuf, Hook>> {
        static HOOKS: OnceLock<Mutex<HashMap<PathBuf, Hook>>> = OnceLock::new();
        HOOKS.get_or_init(Default::default)
    }

    /// Run `hook` inside each origin take of the store at `store`.
    pub(in crate::pr_data::actions) fn set(store: &Path, hook: impl FnMut() + Send + 'static) {
        hooks()
            .lock()
            .unwrap()
            .insert(store.to_path_buf(), Box::new(hook));
    }

    pub(in crate::pr_data::actions) fn clear(store: &Path) {
        hooks().lock().unwrap().remove(store);
    }

    pub(super) fn reached(store: &Path) {
        let hook = hooks().lock().unwrap().remove(store);
        if let Some(mut hook) = hook {
            hook();
            hooks().lock().unwrap().insert(store.to_path_buf(), hook);
        }
    }
}

/// Point everything cut from the landed source (child jobs' declared base and
/// their open pull requests' target) at the branch it landed on, as part of
/// the landing itself: under the store lock, while the landing still holds
/// the source reserved, so no child can prepare onto or land into the landed
/// branch in between. A failure leaves the children naming the landed branch;
/// the landing's final check then refuses them (`prepared_landing_holds`)
/// until they are corrected, rather than landing them into it.
pub(super) async fn repoint_landed_children(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    guard: &JjStoreGuard,
) -> Result<(), String> {
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &merge_context.project_id)
        .await
        .map_err(|error| format!("the project's database is unavailable: {error}"))?;
    crate::orchestrator::base_advance::repoint_children_of_merged_branch(
        &db,
        guard,
        &merge_context.project_id,
        source,
        target,
    )
    .await
    .map_err(|error| {
        format!(
            "the children of `{source}` were not re-pointed at `{target}` ({error}); they are \
             refused from landing on `{source}` until they are"
        )
    })
}

/// The ref that keeps a landing commit's objects alive from the moment it is
/// built until its publication has a terminal outcome. Between the fold being
/// set aside and origin accepting it, nothing else references the commit, so
/// reclamation could prune it; and after an unknown outcome the commit must
/// outlive this process for recovery to install it. `refs/cairn/` is the
/// namespace compaction keeps. Named by the publication's journal attempt, so
/// recovery finds it from the journal and releases it once the attempt is
/// settled.
pub(crate) fn landing_pin_ref(attempt_id: &str) -> String {
    format!("refs/cairn/landing/{attempt_id}")
}

/// Create the landing pin in the store's backing repository (`repo_path`,
/// the project checkout the store exports to). Creation only: an existing pin
/// for the same attempt is a reused attempt id and refused.
pub(super) fn pin_landing(repo_path: &str, attempt_id: &str, commit: &str) -> Result<(), String> {
    let pin = landing_pin_ref(attempt_id);
    let output = crate::env::git()
        .args(["update-ref", &pin, commit, ""])
        .current_dir(repo_path)
        .output()
        .map_err(|error| format!("pin the landing commit as {pin}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "pin the landing commit as {pin}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// Release a landing pin once its publication is settled: the store's
/// bookmarks reference the commit, or origin refused it. Best effort: a pin
/// left behind only retains objects.
fn unpin_landing(repo_path: &str, attempt_id: &str) {
    let pin = landing_pin_ref(attempt_id);
    match crate::env::git()
        .args(["update-ref", "-d", &pin])
        .current_dir(repo_path)
        .output()
    {
        Ok(output) if output.status.success() => {}
        Ok(output) => log::warn!(
            "landing: {pin} was not released: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(error) => log::warn!("landing: {pin} was not released: {error}"),
    }
}

/// Move a landing's bookmarks forward once origin accepted it, as `(bookmark,
/// expected value, landing commit)`: only when every one of them still holds
/// its expected value, so nothing that moved one since is overwritten. The
/// caller holds the store lock and the bookmarks' reservation.
pub(super) fn advance_landing_bookmarks(
    store: &Store,
    moves: &[(&str, &str, &str)],
) -> Result<(), String> {
    for (name, expected, _) in moves {
        let now = crate::jj::bookmark_commit(&store.jj, &store.path, name);
        if now.as_deref() != Some(*expected) {
            return Err(format!(
                "`{name}` is at {now:?}, not the expected {expected}, so the store was not \
                 moved to the landing; recovery records it"
            ));
        }
    }
    let moved = (|| -> Result<(), String> {
        for (name, _, landing) in moves {
            store.jj.run(
                &store.path,
                &[
                    "bookmark",
                    "set",
                    name,
                    "-r",
                    landing,
                    "--allow-backwards",
                    "--ignore-working-copy",
                ],
                "advance landing bookmark",
            )?;
        }
        let exported: Vec<(&str, &str)> = moves
            .iter()
            .map(|(name, _, landing)| (*name, *landing))
            .collect();
        crate::jj::export_git_verified(
            &store.jj,
            &store.path,
            true,
            "advance landing bookmarks",
            &exported,
        )
    })();
    // Reported on the landing's own task: every caller runs this inline under
    // `blocking_phase`. Past the precheck a failure may have moved some of
    // them, so it is reported unobserved, never as unmoved.
    match &moved {
        Ok(()) => super::merge_metrics::note_bookmark_writes(moves),
        Err(_) => {
            let names: Vec<&str> = moves.iter().map(|(name, _, _)| *name).collect();
            super::merge_metrics::note_unobserved_writes(&names);
        }
    }
    moved
}

/// One merge result as it would land on one prepared coordinate, and what it
/// took to build it.
struct Prospect {
    prepared: PreparedBranch,
    merge: ProspectiveMerge,
    /// Whether the landing replays and binds the branch.
    binds: bool,
    rules: Vec<(String, IdentifierRule)>,
    plan: bind::BindPlan,
}

/// A prospect, or why the prepared coordinate stopped holding before one
/// could be built on it.
enum Built {
    Ready(Box<Prospect>),
    Moved(String),
}

pub(super) struct Store {
    pub(super) jj: crate::jj::JjEnv,
    pub(super) path: PathBuf,
}

pub(super) fn store_of(orch: &Orchestrator, merge_context: &MergeMrContext) -> Store {
    Store {
        jj: crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        path: crate::jj::project_store_dir(
            &orch.config_dir,
            Path::new(&merge_context.mr.repo_path),
        ),
    }
}

/// Land `merge_context`'s branch on the default branch. `method` is the
/// landing method as the route resolves it. `verified_head` is a pin bump's
/// verified head, which must be exactly what lands. `journal` is the merge
/// call's attempt; each publication try to origin journals under its own
/// attempt id derived from it.
pub(super) async fn land_on_default(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
    method: &str,
    verified_head: Option<&str>,
    journal: &MergeAttemptJournal<'_>,
) -> Result<DefaultLanding, String> {
    let source = merge_context.source_branch.as_str();
    let target = merge_context.target_branch.as_str();
    let store = store_of(orch, merge_context);
    // Held from preparation through publication: neither branch moves while
    // the store lock is free for the network push.
    let reservation = orch
        .reserve_branches(
            &store.path,
            &[source, target],
            &format!("landing {source} on {target}"),
            LANDING_RESERVATION_WAIT,
        )
        .await
        .map_err(|error| {
            format!("Refusing to merge `{source}` into `{target}`: {error}. Nothing changed.")
        })?;
    // Shared with the preparation, which installs under it.
    let reservation = Arc::new(reservation);
    let _lease = object_lease(&store, merge_context)?;
    for attempt in 0..MAX_LANDING_ATTEMPTS {
        super::merge_metrics::landing_attempt();
        let prospect =
            match prepare_prospect(orch, job_id, merge_context, method, &store, &reservation)
                .await?
            {
                Built::Ready(prospect) => prospect,
                Built::Moved(moved) => {
                    log::info!(
                    "landing[{job_id}]: the prepared coordinate of `{source}` moved before its \
                     merge result was built (attempt {}): {moved}",
                    attempt + 1
                );
                    continue;
                }
            };
        if let Some(verified) = verified_head {
            // A pin bump lands exactly its verified head: one the preparation
            // carried onto a newer target is a different input, never reused.
            if prospect.binds || prospect.prepared.head_after != verified {
                return Err(format!(
                    "Refusing to merge `{source}` as a pin bump: the head that would land is not \
                     the verified head {verified} (the branch moved, or it changes an identifier \
                     the landing binds, which a pin bump never does). Merge again so the new head \
                     is verified."
                ));
            }
        }

        validate_prospect(&store, merge_context, &prospect).await?;
        // The running build judges the configuration this exact merge result
        // puts on the default branch: unloadable is refused, set-aside checks
        // land with a notice.
        let config_notice = super::config_preflight::preflight_landing_config(
            merge_context,
            &prospect.prepared.base_commit,
            &prospect.merge.commit_id,
        )
        .await?;

        let landed = {
            let wait = super::merge_metrics::time(super::merge_metrics::Phase::LockWait);
            let guard = orch
                .acquire_jj_store_lock(
                    &store.path,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("landing {source} on {target}"),
                )
                .await;
            drop(wait);
            let attempt_id = format!("{}-publication-{}", journal.attempt_id, attempt + 1);
            let publication_journal = MergeAttemptJournal {
                attempt_id: &attempt_id,
                ..*journal
            };
            land_if_unchanged(
                orch,
                merge_context,
                method,
                &store,
                &prospect,
                guard,
                &reservation,
                &publication_journal,
            )
            .await?
        };
        match landed {
            Some(Landed { commit, published }) => {
                return Ok(DefaultLanding {
                    landed_commit: commit,
                    plan: prospect.plan,
                    config_notice,
                    published,
                    reservation: Some(reservation),
                })
            }
            None => log::info!(
                "landing[{job_id}]: `{source}` or `{target}` moved while the landing was \
                 validated (attempt {}); building and validating the new merge result",
                attempt + 1
            ),
        }
    }
    Err(format!(
        "Refusing to merge `{source}` into `{target}`: `{target}` or the branch kept moving while \
         the landing was validated, {MAX_LANDING_ATTEMPTS} times. `{target}` was not advanced; merge \
         again."
    ))
}

/// Validate a prospect off the store lock: every changed file parses, and
/// every bound identifier holds its invariant.
async fn validate_prospect(
    store: &Store,
    merge_context: &MergeMrContext,
    prospect: &Prospect,
) -> Result<(), String> {
    let _validate = super::merge_metrics::time(super::merge_metrics::Phase::Validate);
    let (head, tip) = (
        prospect.merge.commit_id.clone(),
        prospect.merge.target_tip.clone(),
    );
    // Off the store lock, so every read is pinned to an operation that indexes
    // both commits: an unpinned jj read would merge a split operation log behind
    // the writer. The undone prospect stays indexed, and the caller's object
    // lease keeps it from reclamation.
    let jj = store
        .jj
        .pin_immutable_read(&store.path, &[head.as_str(), tip.as_str()])?;
    let path = store.path.clone();
    let (target, source) = (
        merge_context.target_branch.clone(),
        merge_context.source_branch.clone(),
    );
    let rules = if prospect.binds {
        prospect.rules.clone()
    } else {
        Vec::new()
    };
    let expected = prospect.merge.tree_hash.clone();
    tokio::task::spawn_blocking(move || {
        let tree = crate::jj::sealed_tree_hash_via_git(&jj, &path, &head)?;
        if tree != expected {
            return Err(format!(
                "Refusing to merge: judged head {head} has tree {tree}, not {expected}"
            ));
        }
        if !crate::jj::revset_descends_from(&jj, &path, &head, &tip) {
            return Err(format!(
                "Refusing to merge: judged head {head} does not contain prepared target {tip}"
            ));
        }
        bind::validate_landing(&jj, &path, &head, &tip, &target, &source, &rules)
    })
    .await
    .map_err(|error| format!("landing validation did not finish: {error}"))?
}

/// Prepare the source onto the landing target, then build the merge result on
/// exactly the prepared coordinate.
async fn prepare_prospect(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
    method: &str,
    store: &Store,
    reservation: &Arc<BranchReservation>,
) -> Result<Built, String> {
    let source = merge_context.source_branch.as_str();
    reservation_covers(reservation, store, merge_context)?;
    if !merge_context.mr.is_local {
        take_origin_target(orch, store, merge_context, reservation).await?;
    }
    let prepared = prepare_landing_source(orch, job_id, merge_context, reservation).await?;
    let wait = super::merge_metrics::time(super::merge_metrics::Phase::LockWait);
    let guard = orch
        .acquire_jj_store_lock(
            &store.path,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("landing prospect for {source}"),
        )
        .await;
    drop(wait);
    let _build = super::merge_metrics::time(super::merge_metrics::Phase::Build);
    build_prospect(
        orch,
        merge_context,
        method,
        store,
        prepared,
        &guard,
        reservation,
    )
    .await
}

/// Bring origin's copy of the default branch into the store, so the source is
/// prepared onto the target the landing publishes onto. A store copy that is
/// not exactly origin's afterwards (diverged, or ahead with commits origin
/// never took) is refused: the default branch's landing is leased on, and
/// built onto, one exact commit both hold.
pub(super) async fn take_origin_target(
    orch: &Orchestrator,
    store: &Store,
    merge_context: &MergeMrContext,
    reservation: &BranchReservation,
) -> Result<(), String> {
    let taken = judge_origin_target(orch, store, merge_context, reservation).await?;
    if let crate::jj::OriginFastForward::Diverged { local, remote } = &taken.judged {
        return Err(origin_target_refusal(
            merge_context,
            format!(
                "the store's `{}` ({local}) and origin's ({remote}) have diverged",
                merge_context.target_branch
            ),
        ));
    }
    if taken.local.as_deref() != Some(taken.origin.as_str()) {
        return Err(origin_target_refusal(
            merge_context,
            format!(
                "the store's `{}` is at {:?}, not origin's {}",
                merge_context.target_branch, taken.local, taken.origin
            ),
        ));
    }
    Ok(())
}

/// Origin's copy of a landing target, and what the store made of it.
pub(super) struct OriginTarget {
    /// The commit origin's copy names.
    pub(super) origin: String,
    /// The store's judgement of it. Each route decides which shapes it can
    /// land on: the default branch only the store's own copy, a managed
    /// integration branch also one the transport's supersession proof must
    /// settle (`Superseded` and `Diverged` by the hidden-commit test, which
    /// a dormant child's visible old commit alone can turn into `Diverged`).
    pub(super) judged: crate::jj::OriginFastForward,
    /// The store's target afterwards, read under the same lock.
    pub(super) local: Option<String>,
}

/// The refusal of a landing because of what origin's copy of its target is.
pub(super) fn origin_target_refusal(merge_context: &MergeMrContext, why: String) -> String {
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    format!("Refusing to merge `{source}` into `{target}`: {why}. `{target}` was not advanced.")
}

/// Read origin's copy of the landing target with no store lock held, then
/// judge it under the lock, taking it into the store when it is a pure
/// fast-forward of the store's copy, which the landing's reservation of the
/// target entitles it to move. A copy that diverged from the store's is
/// refused: neither could be leased on the other. Each route decides which of
/// the remaining shapes it can land on.
pub(super) async fn judge_origin_target(
    orch: &Orchestrator,
    store: &Store,
    merge_context: &MergeMrContext,
    reservation: &BranchReservation,
) -> Result<OriginTarget, String> {
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    let refuse = |why: String| origin_target_refusal(merge_context, why);
    let (path, branch) = (store.path.clone(), target.to_string());
    let fetch = super::merge_metrics::time(super::merge_metrics::Phase::Fetch);
    let observed =
        crate::orchestrator::base_branch_sync::with_fetch_lock(orch, &store.path, || {
            let (path, branch) = (path.clone(), branch.clone());
            async move {
                tokio::task::spawn_blocking(move || {
                    crate::jj::fetch_origin_observation(&path, &branch)
                })
                .await
                .map_err(|error| format!("the fetch worker did not finish: {error}"))
                .and_then(|fetched| fetched)
            }
        })
        .await
        .map_err(|error| refuse(format!("origin's `{target}` could not be read ({error})")))?
        .ok_or_else(|| refuse(format!("origin has no `{target}`")))?;
    drop(fetch);
    let wait = super::merge_metrics::time(super::merge_metrics::Phase::LockWait);
    let guard = orch
        .acquire_jj_store_lock(
            &store.path,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("take origin's {target} for landing {source}"),
        )
        .await;
    drop(wait);
    // Under the lock, so a fence the previous holder recorded is seen. A fence
    // on the target or source, in any open database, refuses the take; every
    // other fenced bookmark is named so the import leaves it where its recorded
    // repair expects it.
    let preserved = crate::orchestrator::branch_recovery::preserved_import_bookmarks(
        orch,
        &store.path,
        &[target, source],
        &format!("Taking origin's `{target}` before this merge"),
    )
    .await
    .map_err(refuse)?;
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &merge_context.project_id)
        .await
        .map_err(|error| refuse(format!("the project's database is unavailable: {error}")))?;
    // The take moves only the target (its local bookmark, tracked remote and
    // export, each by compare-and-swap), so the target's permit admits it. It
    // runs the supersession proof, so it runs on a blocking worker, and that
    // worker owns the guard by value from the permit until the take returns,
    // even if this caller stops awaiting meanwhile.
    let (jj, path, branch, seen) = (
        store.jj.preserving_import_bookmarks(preserved),
        store.path.clone(),
        target.to_string(),
        observed.clone(),
    );
    guard
        .branch_write_permit(
            &db,
            reservation,
            &merge_context.project_id,
            target,
            crate::orchestrator::BatchAdmission::None,
        )
        .await
        .map_err(|refusal| refuse(refusal.to_string()))?;
    let joined = tokio::task::spawn_blocking(move || {
        let taken = guard.blocking_phase("take origin's landing target", || {
            #[cfg(test)]
            origin_take_barrier::reached(&path);
            let judged = crate::jj::fast_forward_branch_from_origin(&jj, &path, &branch, &seen)?;
            Ok::<_, String>((judged, crate::jj::bookmark_commit(&jj, &path, &branch)))
        });
        (guard, taken)
    })
    .await;
    // Reported here, on the landing's task: the worker cannot see the merge
    // attempt's collector.
    if !matches!(&joined, Ok((_, Ok(_)))) {
        super::merge_metrics::note_unobserved_writes(&[target]);
    }
    let (guard, taken) = joined
        .map_err(|error| refuse(format!("the origin take worker did not finish: {error}")))?;
    let (judged, local) = taken.map_err(refuse)?;
    if let crate::jj::OriginFastForward::Advanced { from, to } = &judged {
        super::merge_metrics::note_bookmark_writes(&[(target, from.as_str(), to.as_str())]);
    }
    drop(guard);
    Ok(OriginTarget {
        origin: observed,
        judged,
        local,
    })
}

/// Why the store no longer holds `prepared`, if it does not: the source
/// bookmark at the prepared head and the target bookmark at the prepared target
/// commit. Origin's target is not compared here: the publication's lease on
/// the exact prepared target is the authority on that. The caller holds the
/// store lock.
pub(super) fn coordinate_moved(
    store: &Store,
    merge_context: &MergeMrContext,
    prepared: &PreparedBranch,
) -> Option<String> {
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    let source_now = crate::jj::bookmark_commit(&store.jj, &store.path, source);
    if source_now.as_deref() != Some(prepared.head_after.as_str()) {
        return Some(format!(
            "`{source}` is at {source_now:?}, not the prepared {}",
            prepared.head_after
        ));
    }
    let target_now = crate::jj::bookmark_commit(&store.jj, &store.path, target);
    if target_now.as_deref() != Some(prepared.base_commit.as_str()) {
        return Some(format!(
            "`{target}` is at {target_now:?}, not the prepared {}",
            prepared.base_commit
        ));
    }
    None
}

/// Bind the prepared source against its prepared target. Preparation already
/// carried it onto that target, so nothing is replayed here: a source that does
/// not descend from it is refused. The caller holds the store lock and has
/// checked that the source bookmark is at the prepared head.
fn bind_prepared(
    store: &Store,
    merge_context: &MergeMrContext,
    prepared: &PreparedBranch,
    rules: &[(String, IdentifierRule)],
) -> Result<bind::BoundLanding, String> {
    let descends = crate::jj::revset_commit(
        &store.jj,
        &store.path,
        &format!("{} & ::{}", prepared.base_commit, prepared.head_after),
    )
    .is_some();
    if !descends {
        return Err(format!(
            "Refusing to merge `{}`: its prepared head {} does not descend from `{}` at {}.",
            merge_context.source_branch,
            prepared.head_after,
            merge_context.target_branch,
            prepared.base_commit
        ));
    }
    bind::bind_onto(
        &store.jj,
        &store.path,
        &merge_context.source_branch,
        &prepared.head_after,
        &prepared.base_commit,
        &merge_context.target_branch,
        rules,
    )
}

/// The merge result as it would land on exactly `prepared`, built in the store
/// and undone. The caller holds the store lock.
async fn build_prospect(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    store: &Store,
    prepared: PreparedBranch,
    guard: &JjStoreGuard,
    reservation: &BranchReservation,
) -> Result<Built, String> {
    if let Some(moved) = coordinate_moved(store, merge_context, &prepared) {
        return Ok(Built::Moved(moved));
    }
    let (source_tip, target_tip) = (prepared.head_after.clone(), prepared.base_commit.clone());
    let mode = crate::orchestrator::catch_up::mode_for_project(
        orch,
        &merge_context.project_id,
        &store.path,
        &merge_context.source_branch,
    )
    .await?;
    if let Some(mode) = mode.as_ref() {
        crate::orchestrator::catch_up::landing_review_gate(
            orch,
            mode,
            &store.jj,
            &store.path,
            &merge_context.target_branch,
            &target_tip,
        )
        .await?;
    }
    let rules = landing::identifier_rules(&landing::landing_settings_at(
        &store.jj,
        &store.path,
        &target_tip,
    ))?;
    let binds = mode.is_some()
        || bind::touches_identifiers(&store.jj, &store.path, &source_tip, &target_tip, &rules)?;
    if !binds {
        let merge = prospective_default_landing(
            orch,
            merge_context,
            method,
            &target_tip,
            &source_tip,
            guard,
            reservation,
        )
        .await?;
        if merge.source_tip != source_tip || merge.target_tip != target_tip {
            return Ok(Built::Moved(format!(
                "the merge result was built from {} onto {}, not the prepared {source_tip} onto \
                 {target_tip}",
                merge.source_tip, merge.target_tip
            )));
        }
        return Ok(Built::Ready(Box::new(Prospect {
            prepared,
            merge,
            binds,
            rules,
            plan: bind::BindPlan::default(),
        })));
    }

    let operation = guard.capture_operation(&store.jj)?;
    let built = (|| -> Result<(ProspectiveMerge, bind::BindPlan), String> {
        let bound = bind_prepared(store, merge_context, &prepared, &rules)?;
        let tree_hash = crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, &bound.head)?;
        let patch = store.jj.run(
            &store.path,
            &[
                "diff",
                "--ignore-working-copy",
                "--git",
                "--from",
                &target_tip,
                "--to",
                &bound.head,
            ],
            "jj diff bound landing",
        )?;
        Ok((
            ProspectiveMerge {
                source_tip: source_tip.clone(),
                target_tip: target_tip.clone(),
                tree_entries: crate::jj::tree_entries(&store.jj, &store.path, &bound.head)?,
                commit_id: bound.head,
                tree_hash,
                changed_files: crate::jj::parse_git_diff(&patch),
            },
            bound.plan,
        ))
    })();
    let restored = guard.undo_own_operations(&store.jj, &operation);
    match (built, restored) {
        (Ok((merge, plan)), Ok(())) => Ok(Built::Ready(Box::new(Prospect {
            prepared,
            merge,
            binds,
            rules,
            plan,
        }))),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!(
            "failed to restore the prospective landing operation: {error}"
        )),
        (Err(error), Err(restore)) => Err(format!(
            "{error} (also failed to restore the prospective landing operation: {restore})"
        )),
    }
}

/// Land the validated prospect if its prepared coordinate still holds: `Some`
/// with what landed, `None` when the source, the target or the declared
/// relationship moved, or origin's target was no longer the prepared one,
/// which restarts preparation. `guard` is the store lock for the local recheck,
/// bind and fold; it is released for the network push and re-taken to record
/// it, while `reservation` keeps the source and target still.
#[allow(clippy::too_many_arguments)]
/// The shape a default landing publishes, as its fold produces it: `squash`
/// mints one commit on the target; any other method (a workspace pull
/// request's forced `merge`) fast-forwards the target to the prepared head,
/// keeping the owner's commits. Recovery and refresh judge origin's history by
/// the journaled shape, so it must name what was actually published.
fn publication_shape(method: &str) -> PublicationShape {
    if method == "squash" {
        PublicationShape::Squash
    } else {
        PublicationShape::FastForward
    }
}

#[allow(clippy::too_many_arguments)] // the eight are one landing's coordinate set; bundling them would churn the import's hottest file
async fn land_if_unchanged(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    store: &Store,
    prospect: &Prospect,
    guard: JjStoreGuard,
    reservation: &BranchReservation,
    journal: &MergeAttemptJournal<'_>,
) -> Result<Option<Landed>, String> {
    reservation_covers(reservation, store, merge_context)?;
    if let Some(moved) = coordinate_moved(store, merge_context, &prospect.prepared) {
        log::info!("landing: {moved}; preparing again");
        return Ok(None);
    }
    if !prepared_landing_holds(orch, merge_context, &prospect.prepared, &guard).await? {
        return Ok(None);
    }
    // Both branches are admitted before anything is folded or pushed; the
    // reservation keeps the admission true while the guard is free.
    admit_landing_writers(orch, &guard, reservation, merge_context).await?;
    let operation = guard.capture_operation(&store.jj)?;
    let rollback = |error: String| -> String {
        match guard.undo_own_operations(&store.jj, &operation) {
            Ok(()) => error,
            Err(restore) => format!("{error} (also failed to roll the landing back: {restore})"),
        }
    };
    // The bind commit lands on the source bookmark, which the fold reads.
    let source_head = if prospect.binds {
        let bound = bind_prepared(store, merge_context, &prospect.prepared, &prospect.rules)
            .map_err(&rollback)?;
        let tree = crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, &bound.head)
            .map_err(&rollback)?;
        if tree != prospect.merge.tree_hash {
            log::warn!(
                "landing: binding `{}` again produced tree {tree}, not the judged {}; judging again",
                merge_context.source_branch,
                prospect.merge.tree_hash
            );
            guard
                .undo_own_operations(&store.jj, &operation)
                .map_err(|error| {
                    format!("failed to roll back a landing whose tree changed: {error}")
                })?;
            return Ok(None);
        }
        bound.head
    } else {
        prospect.prepared.head_after.clone()
    };

    let exact = ExactTarget {
        target: &prospect.prepared.base_commit,
        source: &source_head,
        tree: Some(&prospect.merge.tree_hash),
    };
    let fold = super::merge_metrics::time(super::merge_metrics::Phase::Fold);
    let commit = fold_validated_default(orch, merge_context, method, exact, &guard, reservation)
        .await
        .map_err(&rollback)?;
    drop(fold);
    if merge_context.mr.is_local {
        // Only the two bookmarks move. The fold rewrote the source's commits
        // and jj rebased everything descending from them, another branch cut
        // from the source included; so the fold is set aside and the landing
        // installed by compare-and-swap, exactly as an accepted remote one is.
        guard
            .undo_own_operations(&store.jj, &operation)
            .map_err(|error| format!("the folded landing could not be set aside: {error}"))?;
        guard.blocking_phase("install local landing", || {
            advance_landing_bookmarks(
                store,
                &[
                    (
                        merge_context.target_branch.as_str(),
                        prospect.prepared.base_commit.as_str(),
                        commit.as_str(),
                    ),
                    (
                        merge_context.source_branch.as_str(),
                        prospect.prepared.head_after.as_str(),
                        commit.as_str(),
                    ),
                ],
            )
        })?;
        if let Err(error) = repoint_landed_children(orch, merge_context, &guard).await {
            log::warn!("landing: {error}");
        }
        return Ok(Some(Landed {
            commit: Some(commit),
            published: None,
        }));
    }

    // The landing commit exists; pin it, then the store goes back to the
    // prepared target and source until origin accepts it.
    let repo_path = merge_context.mr.repo_path.as_str();
    pin_landing(repo_path, journal.attempt_id, &commit).map_err(&rollback)?;
    if let Err(error) = guard.undo_own_operations(&store.jj, &operation) {
        unpin_landing(repo_path, journal.attempt_id);
        return Err(format!(
            "the folded landing could not be set aside: {error}"
        ));
    }
    let publication = Publication {
        target_branch: merge_context.target_branch.clone(),
        expected_old: prospect.prepared.base_commit.clone(),
        // The default branch's store target is origin's: they are one commit.
        target_before: prospect.prepared.base_commit.clone(),
        landing_commit: commit.clone(),
        landing_tree: prospect.merge.tree_hash.clone(),
        source_branch: merge_context.source_branch.clone(),
        source_before: prospect.prepared.head_after.clone(),
        source_after: commit,
        shape: publication_shape(method),
    };
    publish_pinned(
        orch,
        store,
        merge_context,
        guard,
        reservation,
        journal,
        publication,
        PublishRefs::Target,
    )
    .await
}

/// Publish a pinned landing to origin with the store lock released, and settle
/// the store and the journal by origin's answer: the one publication path of
/// every ungated landing. The caller holds `guard`, has revalidated the
/// prepared coordinate under it, and has pinned `publication.landing_commit` as
/// [`landing_pin_ref`]`(journal.attempt_id)`; the store's bookmarks still name
/// the prepared target and source. The intent is journaled before anything is
/// sent, and the pin is released unless the outcome is unknown. `Ok(None)` when
/// origin's target was no longer the prepared one, which restarts the landing
/// from preparation.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_pinned(
    orch: &Orchestrator,
    store: &Store,
    merge_context: &MergeMrContext,
    guard: JjStoreGuard,
    reservation: &BranchReservation,
    journal: &MergeAttemptJournal<'_>,
    publication: Publication,
    refs: PublishRefs,
) -> Result<Option<Landed>, String> {
    let source = merge_context.source_branch.as_str();
    let repo_path = merge_context.mr.repo_path.as_str();
    let commit = publication.landing_commit.clone();
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &merge_context.project_id)
        .await
        .map_err(|error| {
            unpin_landing(repo_path, journal.attempt_id);
            format!("the project's database is unavailable: {error}")
        })?;
    // In flight before its intent is visible, so no recovery pass can find
    // the intent without also finding it driven.
    let driving = PublicationInFlight::enter(journal.attempt_id).map_err(|error| {
        unpin_landing(repo_path, journal.attempt_id);
        format!("Refusing to merge `{source}`: {error}. Nothing was pushed.")
    })?;
    // A publication no recovery could find is never pushed.
    journal_publication_intent(&db, journal, &publication)
        .await
        .map_err(|error| {
            unpin_landing(repo_path, journal.attempt_id);
            format!(
                "Refusing to merge `{source}`: the landing's publication intent could not be \
                 recorded ({error}). Nothing was pushed."
            )
        })?;
    let outcome = |outcome: PublicationOutcome| {
        let (db, publication) = (db.clone(), publication.clone());
        async move {
            if let Err(error) =
                journal_publication_outcome(&db, journal, &publication, &outcome).await
            {
                log::warn!(
                    "landing: the publication outcome of {} was not journaled: {error}",
                    publication.landing_commit
                );
            }
        }
    };
    let (guard, publish) = publish_released(
        orch,
        store,
        merge_context,
        guard,
        reservation,
        &publication,
        refs,
    )
    .await;
    // From here the store already holds the outcome: nothing below rolls the
    // operation log back. Only an unknown outcome keeps the pin, for recovery.
    if !matches!(publish, Publish::Unknown(_)) {
        unpin_landing(repo_path, journal.attempt_id);
    }
    let landed = match publish {
        Publish::Accepted { unconfirmed } => {
            if let Some(error) = unconfirmed {
                log::warn!(
                    "landing: origin accepted {commit} for `{}`, but recording it in the store \
                     did not complete: {error}",
                    merge_context.target_branch
                );
            }
            Ok(Some(Landed {
                commit: Some(commit),
                // The caller journals the outcome once the landing is
                // recorded; the attempt stays driven until then.
                published: Some(PublishedLanding {
                    publication,
                    attempt_id: journal.attempt_id.to_string(),
                    driving,
                }),
            }))
        }
        Publish::TargetMoved { observed, error } => {
            outcome(PublicationOutcome::TargetMoved { observed }).await;
            log::info!("landing: {error}; preparing again");
            Ok(None)
        }
        Publish::Refused(error) => {
            // Definitive: origin is unchanged. Recovery proves that from
            // origin and closes the attempt.
            outcome(PublicationOutcome::Unknown(format!("refused: {error}"))).await;
            Err(format!(
                "Refusing to merge `{source}` into `{}`: {error}. Nothing was published.",
                merge_context.target_branch
            ))
        }
        Publish::Unknown(error) => {
            // Origin may hold the landing, so the store keeps it: rolling
            // back would report as unpublished a commit origin may have taken.
            outcome(PublicationOutcome::Unknown(error.clone())).await;
            Err(format!(
                "The landing of `{source}` as {commit} on `{}` has an unknown outcome ({error}). \
                 Origin may hold it; Cairn records it once recovery proves it. Do not merge \
                 again until then.",
                merge_context.target_branch
            ))
        }
    };
    drop(guard);
    landed
}

// ─── The approval-gated landing ────────────────────────────────────────────────────────

/// What an approval-gated landing needs beyond an ordinary one.
pub(super) struct ApprovalGate<'a> {
    pub db: &'a LocalDb,
    pub enrollment_id: &'a str,
    pub description: Option<String>,
    pub attempt: MergeAttemptJournal<'a>,
}

/// Land on the default branch only on the selected phone's approval of exactly
/// this act. The full prepare-and-prospect loop runs, but the landing executes
/// once: an approval binds one act, so a moved coordinate ends the attempt
/// rather than retrying it. Returns a notice when the landing could not yet be
/// proven to the approvals service.
pub(super) async fn land_on_default_with_approval(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
    method: &str,
    gate: ApprovalGate<'_>,
) -> Result<(DefaultLanding, Option<String>), String> {
    let journal = human_approval::AttemptJournal::new(gate.db, gate.attempt, 1);
    let credential = match crate::api::runner_credential() {
        Ok(credential) => credential,
        Err(error) => {
            journal
                .append(
                    cairn_common::approvals::ApprovalOutcome::PolicyUnavailable,
                    super::merge::JournalEntry {
                        detail: Some(error.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            return Err(format!(
                "Refusing to merge into the project base branch: the approval phone cannot be \
                 asked because this runner has no approvals credential ({error})."
            ));
        }
    };
    let client = crate::approvals_client::ApprovalsClient::new(
        &*orch.services.http,
        orch.approval_keys.clone(),
        orch.runtime_admission_api_config(),
        credential,
    );
    let mut world =
        match GatedLanding::new(orch, job_id, merge_context, method, gate.attempt.attempt_id) {
            Ok(world) => world,
            Err(refusal) => {
                journal
                    .append(
                        cairn_common::approvals::ApprovalOutcome::Failed,
                        super::merge::JournalEntry {
                            detail: Some(refusal.clone()),
                            ..Default::default()
                        },
                    )
                    .await?;
                return Err(refusal);
            }
        };
    let ceremony = Ceremony {
        client: &client,
        journal: &journal,
        clock: &SystemClock,
        enrollment_id: gate.enrollment_id,
        project_id: &merge_context.project_id,
        attempt_id: gate.attempt.attempt_id,
        job_id,
        title: Some(merge_context.title.clone()),
        description: gate.description,
    };
    let approved = ceremony.run(&mut world).await?;
    let notice = (!approved.proven).then(|| {
        "The merge landed, but whether it is exactly the approved merge is not yet proven; the \
         approval stays unsettled until Cairn reconciles it."
            .to_string()
    });
    Ok((approved.landing, notice))
}

/// The default-branch landing as the approval ceremony drives it.
struct GatedLanding<'a> {
    orch: &'a Orchestrator,
    job_id: &'a str,
    merge_context: &'a MergeMrContext,
    method: &'a str,
    store: Store,
    owner: String,
    repo: String,
    prospect: Option<Prospect>,
    /// The configuration notice judged on the observed merge result.
    config_notice: Option<String>,
    /// The approval attempt, which names the landing pin.
    attempt_id: &'a str,
}

impl<'a> GatedLanding<'a> {
    fn new(
        orch: &'a Orchestrator,
        job_id: &'a str,
        merge_context: &'a MergeMrContext,
        method: &'a str,
        attempt_id: &'a str,
    ) -> Result<Self, String> {
        let (owner, repo) = get_owner_repo(&merge_context.mr.repo_path).map_err(|error| {
            format!(
                "Refusing to merge into the project base branch: approval needs the project's \
                 GitHub repository, and it could not be resolved ({error})."
            )
        })?;
        Ok(Self {
            orch,
            job_id,
            merge_context,
            method,
            store: store_of(orch, merge_context),
            owner,
            repo,
            prospect: None,
            config_notice: None,
            attempt_id,
        })
    }

    fn remote(&self) -> bool {
        !self.merge_context.mr.is_local
    }
}

fn jj_log(store: &Store, revset: &str, template: &str, label: &str) -> Result<String, String> {
    store.jj.run(
        &store.path,
        &[
            "log",
            "-r",
            revset,
            "--no-graph",
            "-T",
            template,
            "--ignore-working-copy",
        ],
        label,
    )
}

fn store_commit_shape(store: &Store, commit: &str) -> Result<LandedShape, String> {
    let parents = jj_log(
        store,
        commit,
        "parents.map(|c| c.commit_id()).join(\" \")",
        "jj log parents",
    )?;
    Ok(LandedShape {
        parents: parents.split_whitespace().map(str::to_string).collect(),
        tree: crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, commit)?,
    })
}

/// The approval route of a landing published to origin by a leased
/// fast-forward of its target, whose evidence is origin's target history.
const ORIGIN_ROUTE: &str = "origin";

impl LandingWorld for GatedLanding<'_> {
    type Landing = DefaultLanding;

    fn route(&self) -> &'static str {
        if self.remote() {
            ORIGIN_ROUTE
        } else {
            "store"
        }
    }

    async fn observe(&mut self) -> Result<Observation, String> {
        let source = self.merge_context.source_branch.as_str();
        let target = self.merge_context.target_branch.as_str();
        // Held only while preparing: the approval wait that follows may take
        // minutes, and `execute` reserves again and compares.
        let reservation = self
            .orch
            .reserve_branches(
                &self.store.path,
                &[source, target],
                &format!("approval prospect for {source}"),
                LANDING_RESERVATION_WAIT,
            )
            .await?;
        let reservation = Arc::new(reservation);
        // For the prospect's own fold-and-undo; execute builds the landing
        // again under a lease of its own.
        let _lease = object_lease(&self.store, self.merge_context)?;
        let mut prospect = None;
        for attempt in 0..MAX_LANDING_ATTEMPTS {
            super::merge_metrics::landing_attempt();
            match prepare_prospect(
                self.orch,
                self.job_id,
                self.merge_context,
                self.method,
                &self.store,
                &reservation,
            )
            .await?
            {
                Built::Ready(ready) => {
                    prospect = Some(*ready);
                    break;
                }
                Built::Moved(moved) => log::info!(
                    "approval landing: the prepared coordinate of `{source}` moved (attempt {}): \
                     {moved}",
                    attempt + 1
                ),
            }
        }
        drop(reservation);
        let prospect = prospect.ok_or_else(|| {
            format!(
                "`{source}` or its target kept moving while the landing was prepared, \
                 {MAX_LANDING_ATTEMPTS} times; nothing was asked or landed"
            )
        })?;
        validate_prospect(&self.store, self.merge_context, &prospect).await?;
        // Judged on the merge result the approval binds; execute lands exactly
        // that tree or refuses.
        self.config_notice = super::config_preflight::preflight_landing_config(
            self.merge_context,
            &prospect.prepared.base_commit,
            &prospect.merge.commit_id,
        )
        .await?;
        let strategy = approval_strategy(self.method, false)?;
        // Everything below runs with no store lock held.
        let auth = installation_authority(&self.orch.db.local, &self.owner).await?;
        let repository =
            api::fetch_repository(&*self.orch.services.http, &auth, &self.owner, &self.repo)
                .await
                .map_err(|error| format!("the repository could not be read: {error}"))?;
        let (source_tip, target_tip) = (
            prospect.prepared.head_after.clone(),
            prospect.prepared.base_commit.clone(),
        );
        let source_message = jj_log(
            &self.store,
            &source_tip,
            "description",
            "jj log description",
        )?;
        let count = jj_log(
            &self.store,
            &format!("{target_tip}..{source_tip}"),
            "commit_id ++ \"\\n\"",
            "jj log source range",
        )?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
        let source_commit_count = u32::try_from(count)
            .ok()
            .filter(|count| *count >= 1)
            .ok_or_else(|| {
                format!("`{source}` has no commits to merge that the base does not already hold")
            })?;
        let merge_base_oid = crate::jj::revset_commit_checked(
            &self.store.jj,
            &self.store.path,
            &format!("heads(::{target_tip} & ::{source_tip})"),
        )?;
        let statuses =
            crate::execution::checks_status::node_check_statuses(self.orch, self.job_id).await;
        let observation = Observation {
            repository_id: repository.id.to_string(),
            repository_name: repository.full_name,
            target_ref: format!("refs/heads/{}", self.merge_context.target_branch),
            target_oid: target_tip,
            source_ref: format!("refs/heads/{source}"),
            source_oid: source_tip.clone(),
            source_message,
            source_commit_count,
            merge_base_oid,
            // The landing commit is minted at landing, with fresh dates.
            candidate_oid: None,
            result_tree_oid: prospect.merge.tree_hash.clone(),
            strategy,
            // Nothing deletes the source branch: worktree teardown owns it.
            deletes_branch: false,
            checks: gate_checks(statuses.as_deref(), &source_tip),
        };
        self.prospect = Some(prospect);
        Ok(observation)
    }

    async fn execute(&mut self, approved: &Observation) -> Execution<DefaultLanding> {
        let config_notice = self.config_notice.take();
        let Some(prospect) = self.prospect.take() else {
            return Execution::Refused("the landing was never observed".to_string());
        };
        let source = self.merge_context.source_branch.as_str();
        let target = self.merge_context.target_branch.as_str();
        let store = &self.store;
        let reservation = match self
            .orch
            .reserve_branches(
                &store.path,
                &[source, target],
                &format!("approved landing {source} on {target}"),
                LANDING_RESERVATION_WAIT,
            )
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => return Execution::Refused(error),
        };
        let reservation = Arc::new(reservation);
        let _lease = match object_lease(store, self.merge_context) {
            Ok(lease) => lease,
            Err(error) => return Execution::Refused(error),
        };
        // ── The final comparison and the local landing, under the store lock ──
        let wait = super::merge_metrics::time(super::merge_metrics::Phase::LockWait);
        let guard = self
            .orch
            .acquire_jj_store_lock(
                &store.path,
                crate::orchestrator::StoreLockClass::Foreground,
                format!("approved landing {source} on {target}"),
            )
            .await;
        drop(wait);
        // The approval binds the prepared coordinate: it must still hold in
        // full (source, target and the relationship). Origin's target is held
        // to it by the publication's lease.
        let moved = coordinate_moved(store, self.merge_context, &prospect.prepared);
        let holds =
            match prepared_landing_holds(self.orch, self.merge_context, &prospect.prepared, &guard)
                .await
            {
                Ok(holds) => holds,
                Err(error) => return Execution::Refused(error),
            };
        if moved.is_some()
            || !holds
            || approved.source_oid != prospect.prepared.head_after
            || approved.target_oid != prospect.prepared.base_commit
        {
            return Execution::BoundStateChanged {
                observed: serde_json::json!({
                    "sourceOid": crate::jj::bookmark_commit(&store.jj, &store.path, source),
                    "targetOid": crate::jj::bookmark_commit(&store.jj, &store.path, target),
                    "moved": moved,
                }),
            };
        }
        // Both branches are admitted before anything is folded or pushed.
        if let Err(error) =
            admit_landing_writers(self.orch, &guard, &reservation, self.merge_context).await
        {
            return Execution::Refused(error);
        }
        let operation = match guard.capture_operation(&store.jj) {
            Ok(operation) => operation,
            Err(error) => return Execution::Refused(error),
        };
        let restore = || guard.undo_own_operations(&store.jj, &operation);
        let head = if prospect.binds {
            let bound = match bind_prepared(
                store,
                self.merge_context,
                &prospect.prepared,
                &prospect.rules,
            ) {
                Ok(bound) => bound,
                Err(error) => {
                    let _ = restore();
                    return Execution::Refused(error);
                }
            };
            let tree = crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, &bound.head);
            if tree.as_deref() != Ok(approved.result_tree_oid.as_str()) {
                let _ = restore();
                return Execution::BoundStateChanged {
                    observed: serde_json::json!({ "resultTreeOid": tree.ok() }),
                };
            }
            bound.head
        } else {
            approved.source_oid.clone()
        };
        let exact = ExactTarget {
            target: &approved.target_oid,
            source: &head,
            tree: Some(&approved.result_tree_oid),
        };
        let fold = super::merge_metrics::time(super::merge_metrics::Phase::Fold);
        let commit = match fold_validated_default(
            self.orch,
            self.merge_context,
            self.method,
            exact,
            &guard,
            &reservation,
        )
        .await
        {
            Ok(commit) => commit,
            Err(error) => {
                let _ = restore();
                return Execution::Refused(error);
            }
        };
        drop(fold);
        let landed = |commit: String, reservation: Arc<BranchReservation>| Execution::Landed {
            commit: commit.clone(),
            approved_head: head.clone(),
            landing: DefaultLanding {
                landed_commit: Some(commit),
                plan: prospect.plan.clone(),
                config_notice: config_notice.clone(),
                published: None,
                reservation: Some(reservation),
            },
        };
        if !self.remote() {
            // Only the two bookmarks move (see land_if_unchanged): the fold
            // is set aside and the landing installed by compare-and-swap, so
            // no branch descending from the source is rebased.
            if let Err(error) = restore() {
                return Execution::Refused(format!(
                    "the folded landing could not be set aside: {error}"
                ));
            }
            let installed = guard.blocking_phase("install local landing", || {
                advance_landing_bookmarks(
                    store,
                    &[
                        (target, approved.target_oid.as_str(), commit.as_str()),
                        (
                            source,
                            prospect.prepared.head_after.as_str(),
                            commit.as_str(),
                        ),
                    ],
                )
            });
            if let Err(error) = installed {
                return Execution::Refused(error);
            }
            if let Err(error) = repoint_landed_children(self.orch, self.merge_context, &guard).await
            {
                log::warn!("approval landing: {error}");
            }
            return landed(commit, reservation);
        }
        // The landing commit exists; pin it, then the store goes back to the
        // prepared target and source until origin accepts it.
        let repo_path = self.merge_context.mr.repo_path.as_str();
        if let Err(error) = pin_landing(repo_path, self.attempt_id, &commit) {
            let _ = restore();
            return Execution::Refused(error);
        }
        if let Err(error) = restore() {
            unpin_landing(repo_path, self.attempt_id);
            return Execution::Refused(format!(
                "the folded landing could not be set aside: {error}"
            ));
        }
        // The ceremony's own `executing` event is this publication's intent.
        let publication = Publication {
            target_branch: target.to_string(),
            expected_old: approved.target_oid.clone(),
            target_before: approved.target_oid.clone(),
            landing_commit: commit.clone(),
            landing_tree: approved.result_tree_oid.clone(),
            source_branch: source.to_string(),
            source_before: prospect.prepared.head_after.clone(),
            source_after: commit.clone(),
            shape: publication_shape(self.method),
        };
        if let Err(error) = reservation_covers(&reservation, store, self.merge_context) {
            unpin_landing(repo_path, self.attempt_id);
            return Execution::Refused(error);
        }
        let (guard, publish) = publish_released(
            self.orch,
            store,
            self.merge_context,
            guard,
            &reservation,
            &publication,
            PublishRefs::Target,
        )
        .await;
        // The store holds the outcome now; nothing below restores operations.
        // Only an unknown outcome keeps the pin, for recovery.
        if !matches!(publish, Publish::Unknown(_)) {
            unpin_landing(repo_path, self.attempt_id);
        }
        let execution = match publish {
            Publish::Accepted { unconfirmed } => {
                if let Some(error) = unconfirmed {
                    log::warn!(
                        "approval landing: origin accepted {commit} for `{target}`, but \
                         recording it in the store did not complete: {error}"
                    );
                }
                landed(commit, reservation)
            }
            Publish::TargetMoved { observed, error } => {
                log::info!("approval landing: {error}");
                Execution::BoundStateChanged {
                    observed: serde_json::json!({ "targetOid": observed }),
                }
            }
            Publish::Refused(error) => Execution::Refused(error),
            Publish::Unknown(reason) => Execution::Unknown {
                reason,
                approved_head: Some(head),
            },
        };
        drop(guard);
        execution
    }

    async fn landed_shape(&self, commit: &str) -> Result<LandedShape, String> {
        if !self.remote() {
            return store_commit_shape(&self.store, commit);
        }
        let auth = installation_authority(&self.orch.db.local, &self.owner).await?;
        let shape = api::fetch_commit_shape(
            &*self.orch.services.http,
            &auth,
            &self.owner,
            &self.repo,
            commit,
        )
        .await?;
        Ok(LandedShape {
            parents: shape.parents,
            tree: shape.tree,
        })
    }
}

/// Reconcile every approved merge whose outcome was never settled: prove it
/// landed, find definitive evidence it did not, or leave it `indeterminate`.
/// Runs at startup and on a slow timer; an attempt still running in this
/// process is left to the call driving it.
pub async fn reconcile_approved_merges(orch: &Orchestrator) {
    let db = &orch.db.local;
    let attempts = match human_approval::unsettled_attempts(db).await {
        Ok(attempts) => attempts,
        Err(error) => {
            log::warn!("approval reconciliation: {error}");
            return;
        }
    };
    if attempts.is_empty() {
        return;
    }
    let credential = match crate::api::runner_credential() {
        Ok(credential) => credential,
        Err(error) => {
            log::warn!(
                "approval reconciliation: {} unsettled approved merge(s) wait: {error}",
                attempts.len()
            );
            return;
        }
    };
    let client = crate::approvals_client::ApprovalsClient::new(
        &*orch.services.http,
        orch.approval_keys.clone(),
        orch.runtime_admission_api_config(),
        credential,
    );
    for attempt in attempts {
        let evidence = landing_evidence(orch, &attempt).await;
        match human_approval::settle_attempt(db, &client, &attempt, &evidence).await {
            Ok(Some(outcome)) => log::info!(
                "approval reconciliation: merge attempt {} settled as {outcome:?}",
                attempt.attempt_id
            ),
            Ok(None) => {}
            Err(error) => log::warn!(
                "approval reconciliation: merge attempt {}: {error}",
                attempt.attempt_id
            ),
        }
    }
}

async fn landing_evidence(
    orch: &Orchestrator,
    attempt: &human_approval::UnsettledAttempt,
) -> LandingEvidence {
    if attempt.mutation_issued == Some(false) {
        return LandingEvidence::DefinitelyNotLanded(
            "the attempt recorded that no mutation was issued".to_string(),
        );
    }
    let context =
        match super::context::resolve_merge_mr_context_for_job(&orch.db.local, &attempt.job_id)
            .await
        {
            Ok(context) => context,
            Err(error) => return LandingEvidence::Unknown(error),
        };
    if attempt.route == ORIGIN_ROUTE {
        // Published by a leased fast-forward: GitHub's merged flag says
        // nothing about it. Origin's target history is the evidence.
        return super::landing_record::remote_target_evidence(
            orch,
            &context.mr.repo_path,
            &context.target_branch,
            &attempt.intent.target_oid,
            attempt.intent.result_tree_oid.as_deref(),
        )
        .await;
    }
    // Attempts made before landings left GitHub's merge API.
    if attempt.route == "github" {
        let Some(pr_number) = context.mr.github_pr_number else {
            return LandingEvidence::Unknown("the pull request has no number".to_string());
        };
        let Ok((owner, repo)) = get_owner_repo(&context.mr.repo_path) else {
            return LandingEvidence::Unknown("the repository is unresolvable".to_string());
        };
        let auth = match installation_authority(&orch.db.local, &owner).await {
            Ok(auth) => auth,
            Err(error) => return LandingEvidence::Unknown(error),
        };
        let http = &*orch.services.http;
        let state = match api::fetch_pr_merge_state(http, &auth, &owner, &repo, pr_number).await {
            Ok(state) => state,
            Err(error) => return LandingEvidence::Unknown(error),
        };
        let pr = human_approval::PullRequestState {
            merged: state.merged,
            closed: state.state == "closed",
            head: Some(state.head.sha),
            merge_commit: state.merge_commit_sha,
        };
        let commit = match human_approval::github_attribution(&pr, attempt.bound_head()) {
            Ok(commit) => commit,
            Err(evidence) => return evidence,
        };
        return match api::fetch_commit_shape(http, &auth, &owner, &repo, &commit).await {
            Ok(shape) => LandingEvidence::Landed {
                commit,
                shape: LandedShape {
                    parents: shape.parents,
                    tree: shape.tree,
                },
            },
            Err(error) => LandingEvidence::Unknown(error),
        };
    }
    // The store fold runs in this process under the store lock and its jj
    // operation is atomic, so once no call is driving it the target either
    // advanced with this attempt's fold or did not.
    let store = store_of(orch, &context);
    let Some(tip) = crate::jj::bookmark_commit(&store.jj, &store.path, &context.target_branch)
    else {
        return LandingEvidence::Unknown("the target bookmark is unreadable".to_string());
    };
    human_approval::store_evidence(&tip, &attempt.intent, |commit| {
        store_commit_shape(&store, commit)
    })
}

/// Record what a landing bound: one `landing_allocations` row per value, and a
/// comment on the issue saying what the branch landed as. Written only after
/// the landing succeeded; a failure is logged and never undoes the landing,
/// whose commit already says what it bound.
pub(super) async fn record_allocations(
    orch: &Orchestrator,
    db: &LocalDb,
    merge_context: &MergeMrContext,
    landed: &DefaultLanding,
) {
    if landed.plan.allocations.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp();
    for allocation in &landed.plan.allocations {
        let written = orch
            .db
            .local
            .execute(
                "INSERT INTO landing_allocations
                   (id, project_id, kind, value, name, provisional, issue_id, branch,
                    landed_commit, allocated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                cairn_db::turso::params![
                    uuid::Uuid::new_v4().to_string(),
                    merge_context.project_id.as_str(),
                    allocation.kind.as_str(),
                    i64::try_from(allocation.value).unwrap_or(i64::MAX),
                    allocation.name.as_str(),
                    allocation.provisional.as_str(),
                    merge_context.issue_id.as_deref(),
                    merge_context.source_branch.as_str(),
                    landed.landed_commit.as_deref(),
                    now,
                ],
            )
            .await;
        if let Err(error) = written {
            log::warn!(
                "landing: `{}` landed as {} {}, but the allocation was not recorded: {error}",
                merge_context.source_branch,
                allocation.kind,
                allocation.name
            );
        }
    }
    let Some(issue_id) = merge_context.issue_id.clone() else {
        return;
    };
    let content = format!("Landed as {}.", landed.plan.summary());
    let commented = db
        .write(move |conn| {
            let (issue_id, content) = (issue_id.clone(), content.clone());
            Box::pin(async move {
                crate::issues::comments::insert_conn(
                    conn,
                    &cairn_common::ids::mint_child(&issue_id),
                    &issue_id,
                    &content,
                    "agent",
                    now,
                )
                .await
                .map(|_| ())
            })
        })
        .await;
    match commented {
        Ok(()) => {
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "comments", "action": "insert"}),
            );
        }
        Err(error) => log::warn!(
            "landing: `{}` landed as {}, but the issue comment was not written: {error}",
            merge_context.source_branch,
            landed.plan.summary()
        ),
    }
}
